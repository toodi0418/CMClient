use argon2::{Argon2, MIN_SALT_LEN, Params, PasswordHash, PasswordVerifier};
use std::{
    collections::{BTreeSet, VecDeque},
    fmt,
    sync::{Arc, Mutex},
};
use tokio::sync::Semaphore;
use zeroize::Zeroizing;

const MAX_AUDIT_CAPACITY: usize = 4_096;
const MAX_CONCURRENT_PASSWORD_VERIFICATIONS: usize = 2;
const MIN_ARGON2_MEMORY_KIB: u32 = 19_456;
const MAX_ARGON2_MEMORY_KIB: u32 = 65_536;
const MIN_ARGON2_ITERATIONS: u32 = 2;
const MAX_ARGON2_ITERATIONS: u32 = 6;
const MIN_ARGON2_LANES: u32 = 1;
const MAX_ARGON2_LANES: u32 = 4;
const MIN_ARGON2_OUTPUT_BYTES: usize = 16;
const MAX_ARGON2_OUTPUT_BYTES: usize = 64;

#[derive(Clone, PartialEq, Eq)]
pub struct LanAccessConfig {
    pub password_hash: String,
    pub allowed_origins: BTreeSet<String>,
    pub session_ttl_seconds: u64,
    pub audit_capacity: usize,
}

impl fmt::Debug for LanAccessConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LanAccessConfig")
            .field("password_hash", &"[REDACTED]")
            .field("allowed_origins", &self.allowed_origins)
            .field("session_ttl_seconds", &self.session_ttl_seconds)
            .field("audit_capacity", &self.audit_capacity)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagementAuditEntry {
    pub occurred_at_unix_seconds: u64,
    pub action: &'static str,
    pub outcome: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagementAccessError {
    InvalidConfiguration,
    OriginDenied,
    LoginRateLimited,
    CredentialsInvalid,
    SessionInvalid,
    SessionExpired,
    CsrfInvalid,
}

impl ManagementAccessError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidConfiguration => "MANAGEMENT_LAN_AUTH_CONFIGURATION_INVALID",
            Self::OriginDenied => "MANAGEMENT_ORIGIN_DENIED",
            Self::LoginRateLimited => "MANAGEMENT_LOGIN_RATE_LIMITED",
            Self::CredentialsInvalid => "MANAGEMENT_CREDENTIALS_INVALID",
            Self::SessionInvalid => "MANAGEMENT_SESSION_INVALID",
            Self::SessionExpired => "MANAGEMENT_SESSION_EXPIRED",
            Self::CsrfInvalid => "MANAGEMENT_CSRF_INVALID",
        }
    }
}

/// Password verification, exact Origin policy, and a bounded redacted audit projection.
///
/// Browser sessions and request-rate accounting deliberately live in Tower middleware. This
/// controller owns only CMClient policy that those generic layers cannot express.
pub struct ManagementAccessController {
    password_hash: Arc<str>,
    allowed_origins: BTreeSet<String>,
    session_ttl_seconds: u64,
    audit_capacity: usize,
    password_verifications: Arc<Semaphore>,
    audit: Mutex<VecDeque<ManagementAuditEntry>>,
}

impl fmt::Debug for ManagementAccessController {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ManagementAccessController")
            .field("credential_hash", &"[REDACTED]")
            .field("allowed_origins", &self.allowed_origins)
            .field("session_ttl_seconds", &self.session_ttl_seconds)
            .field("audit_capacity", &self.audit_capacity)
            .finish_non_exhaustive()
    }
}

impl ManagementAccessController {
    pub fn new(config: LanAccessConfig) -> Result<Self, ManagementAccessError> {
        if config.password_hash.is_empty()
            || config.allowed_origins.is_empty()
            || config.session_ttl_seconds == 0
            || config.session_ttl_seconds > 86_400
            || config.audit_capacity == 0
            || config.audit_capacity > MAX_AUDIT_CAPACITY
            || config
                .allowed_origins
                .iter()
                .any(|origin| !is_exact_http_origin(origin))
        {
            return Err(ManagementAccessError::InvalidConfiguration);
        }
        validate_password_hash(&config.password_hash)?;
        Ok(Self {
            password_hash: Arc::from(config.password_hash),
            allowed_origins: config.allowed_origins,
            session_ttl_seconds: config.session_ttl_seconds,
            audit_capacity: config.audit_capacity,
            password_verifications: Arc::new(Semaphore::new(MAX_CONCURRENT_PASSWORD_VERIFICATIONS)),
            audit: Mutex::new(VecDeque::new()),
        })
    }

    pub const fn session_ttl_seconds(&self) -> u64 {
        self.session_ttl_seconds
    }

    pub fn allowed_origins(&self) -> &BTreeSet<String> {
        &self.allowed_origins
    }

    pub fn require_origin(
        &self,
        origin: &str,
        now_unix_seconds: u64,
    ) -> Result<(), ManagementAccessError> {
        if self.allowed_origins.contains(origin) {
            return Ok(());
        }
        self.audit(now_unix_seconds, "request", "origin_denied");
        Err(ManagementAccessError::OriginDenied)
    }

    /// Runs bounded Argon2 work without blocking an Axum executor worker.
    pub async fn verify_password(
        &self,
        origin: &str,
        password: &str,
        now_unix_seconds: u64,
    ) -> Result<(), ManagementAccessError> {
        self.require_origin(origin, now_unix_seconds)?;
        let permit = Arc::clone(&self.password_verifications)
            .try_acquire_owned()
            .map_err(|_| {
                self.audit(now_unix_seconds, "login", "verification_capacity");
                ManagementAccessError::LoginRateLimited
            })?;
        let password_hash = Arc::clone(&self.password_hash);
        let candidate = Zeroizing::new(password.to_owned());
        let verified = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            PasswordHash::new(&password_hash)
                .ok()
                .and_then(|hash| {
                    Argon2::default()
                        .verify_password(candidate.as_bytes(), &hash)
                        .ok()
                })
                .is_some()
        })
        .await
        .unwrap_or(false);
        if !verified {
            self.audit(now_unix_seconds, "login", "denied");
            return Err(ManagementAccessError::CredentialsInvalid);
        }
        self.audit(now_unix_seconds, "login", "allowed");
        Ok(())
    }

    pub fn audit_snapshot(&self) -> Vec<ManagementAuditEntry> {
        self.audit
            .lock()
            .map_or_else(|_| Vec::new(), |entries| entries.iter().cloned().collect())
    }

    pub fn audit(
        &self,
        occurred_at_unix_seconds: u64,
        action: &'static str,
        outcome: &'static str,
    ) {
        if let Ok(mut entries) = self.audit.lock() {
            entries.push_back(ManagementAuditEntry {
                occurred_at_unix_seconds,
                action,
                outcome,
            });
            while entries.len() > self.audit_capacity {
                entries.pop_front();
            }
        }
    }
}

fn validate_password_hash(value: &str) -> Result<(), ManagementAccessError> {
    let hash = PasswordHash::new(value).map_err(|_| ManagementAccessError::InvalidConfiguration)?;
    let params =
        Params::try_from(&hash).map_err(|_| ManagementAccessError::InvalidConfiguration)?;
    let salt = hash
        .salt
        .ok_or(ManagementAccessError::InvalidConfiguration)?;
    let mut decoded_salt = [0_u8; 64];
    let salt_length = salt
        .decode_b64(&mut decoded_salt)
        .map_err(|_| ManagementAccessError::InvalidConfiguration)?
        .len();
    let output_length = hash
        .hash
        .as_ref()
        .ok_or(ManagementAccessError::InvalidConfiguration)?
        .len();
    if hash.algorithm.as_str() != "argon2id"
        || hash.version != Some(19)
        || hash.params.iter().count() != 3
        || !(MIN_ARGON2_MEMORY_KIB..=MAX_ARGON2_MEMORY_KIB).contains(&params.m_cost())
        || !(MIN_ARGON2_ITERATIONS..=MAX_ARGON2_ITERATIONS).contains(&params.t_cost())
        || !(MIN_ARGON2_LANES..=MAX_ARGON2_LANES).contains(&params.p_cost())
        || salt_length < MIN_SALT_LEN
        || !(MIN_ARGON2_OUTPUT_BYTES..=MAX_ARGON2_OUTPUT_BYTES).contains(&output_length)
    {
        return Err(ManagementAccessError::InvalidConfiguration);
    }
    Ok(())
}

fn is_exact_http_origin(value: &str) -> bool {
    let authority = value
        .strip_prefix("https://")
        .or_else(|| value.strip_prefix("http://"));
    authority.is_some_and(|authority| {
        !authority.is_empty()
            && !authority.contains(['/', '?', '#', '@'])
            && !authority.contains(char::is_whitespace)
    })
}

#[cfg(test)]
mod tests {
    use super::{LanAccessConfig, ManagementAccessController, ManagementAccessError};
    use argon2::{Argon2, PasswordHasher, password_hash::SaltString};
    use std::collections::BTreeSet;

    fn password_hash() -> String {
        let salt =
            SaltString::encode_b64(b"cmclient-access-fixture").expect("fixture salt should encode");
        Argon2::default()
            .hash_password(b"password", &salt)
            .expect("fixture password should hash")
            .to_string()
    }

    fn controller(audit_capacity: usize) -> ManagementAccessController {
        ManagementAccessController::new(LanAccessConfig {
            password_hash: password_hash(),
            allowed_origins: BTreeSet::from([
                String::from("https://cmclient.example"),
                String::from("http://192.0.2.10:7080"),
            ]),
            session_ttl_seconds: 60,
            audit_capacity,
        })
        .expect("configuration should be valid")
    }

    #[tokio::test]
    async fn verifies_argon2_and_exact_origin_without_retaining_session_state() {
        let controller = controller(8);
        assert!(
            controller
                .verify_password("https://cmclient.example", "password", 100)
                .await
                .is_ok()
        );
        assert_eq!(
            controller
                .verify_password("https://cmclient.example", "wrong", 101)
                .await,
            Err(ManagementAccessError::CredentialsInvalid)
        );
        assert_eq!(
            controller
                .verify_password("https://CMCLIENT.example", "password", 102)
                .await,
            Err(ManagementAccessError::OriginDenied)
        );
        assert_eq!(controller.session_ttl_seconds(), 60);
    }

    #[test]
    fn accepts_explicit_http_origin_and_bounds_redacted_audit() {
        let controller = controller(2);
        assert!(
            controller
                .require_origin("http://192.0.2.10:7080", 1)
                .is_ok()
        );
        controller.audit(2, "listener", "http_lan_warning");
        controller.audit(3, "request", "allowed");
        let audit = controller.audit_snapshot();
        assert_eq!(audit.len(), 2);
        assert_eq!(audit[0].outcome, "http_lan_warning");
        assert_eq!(audit[1].outcome, "allowed");
        let debug = format!("{controller:?}");
        assert!(!debug.contains("$argon2"));
        assert!(!debug.contains("password"));
    }

    #[test]
    fn rejects_weak_hashes_and_non_origin_urls() {
        let valid = password_hash();
        for password_hash in [
            valid.replacen("$argon2id$", "$argon2i$", 1),
            valid.replacen("v=19$", "v=16$", 1),
            valid.replacen("m=19456", "m=8", 1),
        ] {
            assert!(matches!(
                ManagementAccessController::new(LanAccessConfig {
                    password_hash,
                    allowed_origins: BTreeSet::from([String::from("https://cmclient.example")]),
                    session_ttl_seconds: 60,
                    audit_capacity: 8,
                }),
                Err(ManagementAccessError::InvalidConfiguration)
            ));
        }
        for origin in [
            "cmclient.example",
            "https://cmclient.example/path",
            "https://user@cmclient.example",
            "https://cmclient.example?query",
        ] {
            assert!(matches!(
                ManagementAccessController::new(LanAccessConfig {
                    password_hash: valid.clone(),
                    allowed_origins: BTreeSet::from([origin.to_owned()]),
                    session_ttl_seconds: 60,
                    audit_capacity: 8,
                }),
                Err(ManagementAccessError::InvalidConfiguration)
            ));
        }
    }
}
