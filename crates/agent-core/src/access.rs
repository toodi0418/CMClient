use argon2::{Argon2, PasswordHash, PasswordVerifier};
use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    sync::Mutex,
};
use uuid::Uuid;

const MAX_AUDIT_CAPACITY: usize = 4_096;
const MAX_LOGIN_FAILURES: u8 = 5;
const LOGIN_WINDOW_SECONDS: u64 = 60;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LanAccessConfig {
    pub password_hash: String,
    pub allowed_origins: BTreeSet<String>,
    pub session_ttl_seconds: u64,
    pub audit_capacity: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagementSession {
    pub id: String,
    pub csrf_token: String,
    pub expires_at_unix_seconds: u64,
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

struct SessionRecord {
    csrf_token: String,
    expires_at_unix_seconds: u64,
}

struct FailureWindow {
    started_at_unix_seconds: u64,
    failures: u8,
}

pub struct ManagementAccessController {
    password_hash: String,
    allowed_origins: BTreeSet<String>,
    session_ttl_seconds: u64,
    audit_capacity: usize,
    sessions: Mutex<HashMap<String, SessionRecord>>,
    failures: Mutex<HashMap<String, FailureWindow>>,
    audit: Mutex<VecDeque<ManagementAuditEntry>>,
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
                .any(|origin| !is_https_origin(origin))
        {
            return Err(ManagementAccessError::InvalidConfiguration);
        }
        PasswordHash::new(&config.password_hash)
            .map_err(|_| ManagementAccessError::InvalidConfiguration)?;
        Ok(Self {
            password_hash: config.password_hash,
            allowed_origins: config.allowed_origins,
            session_ttl_seconds: config.session_ttl_seconds,
            audit_capacity: config.audit_capacity,
            sessions: Mutex::new(HashMap::new()),
            failures: Mutex::new(HashMap::new()),
            audit: Mutex::new(VecDeque::new()),
        })
    }

    pub fn login(
        &self,
        remote_key: &str,
        origin: &str,
        password: &str,
        now_unix_seconds: u64,
    ) -> Result<ManagementSession, ManagementAccessError> {
        self.require_origin(origin, now_unix_seconds)?;
        {
            let mut failures = self
                .failures
                .lock()
                .map_err(|_| ManagementAccessError::CredentialsInvalid)?;
            let window = failures
                .entry(remote_key.to_owned())
                .or_insert(FailureWindow {
                    started_at_unix_seconds: now_unix_seconds,
                    failures: 0,
                });
            if now_unix_seconds.saturating_sub(window.started_at_unix_seconds)
                >= LOGIN_WINDOW_SECONDS
            {
                *window = FailureWindow {
                    started_at_unix_seconds: now_unix_seconds,
                    failures: 0,
                };
            }
            if window.failures >= MAX_LOGIN_FAILURES {
                self.audit(now_unix_seconds, "login", "rate_limited");
                return Err(ManagementAccessError::LoginRateLimited);
            }
        }
        if PasswordHash::new(&self.password_hash)
            .ok()
            .and_then(|hash| {
                Argon2::default()
                    .verify_password(password.as_bytes(), &hash)
                    .ok()
            })
            .is_none()
        {
            if let Ok(mut failures) = self.failures.lock() {
                if let Some(window) = failures.get_mut(remote_key) {
                    window.failures = window.failures.saturating_add(1);
                }
            }
            self.audit(now_unix_seconds, "login", "denied");
            return Err(ManagementAccessError::CredentialsInvalid);
        }
        if let Ok(mut failures) = self.failures.lock() {
            failures.remove(remote_key);
        }
        let session = ManagementSession {
            id: random_token(),
            csrf_token: random_token(),
            expires_at_unix_seconds: now_unix_seconds.saturating_add(self.session_ttl_seconds),
        };
        self.sessions
            .lock()
            .map_err(|_| ManagementAccessError::SessionInvalid)?
            .insert(
                session.id.clone(),
                SessionRecord {
                    csrf_token: session.csrf_token.clone(),
                    expires_at_unix_seconds: session.expires_at_unix_seconds,
                },
            );
        self.audit(now_unix_seconds, "login", "allowed");
        Ok(session)
    }

    pub fn authorize(
        &self,
        origin: Option<&str>,
        session_id: &str,
        csrf_token: Option<&str>,
        write: bool,
        now_unix_seconds: u64,
    ) -> Result<(), ManagementAccessError> {
        if write {
            self.require_origin(origin.unwrap_or_default(), now_unix_seconds)?;
        } else if let Some(origin) = origin {
            self.require_origin(origin, now_unix_seconds)?;
        }
        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| ManagementAccessError::SessionInvalid)?;
        let Some(session) = sessions.get(session_id) else {
            self.audit(now_unix_seconds, "request", "session_denied");
            return Err(ManagementAccessError::SessionInvalid);
        };
        if session.expires_at_unix_seconds <= now_unix_seconds {
            sessions.remove(session_id);
            self.audit(now_unix_seconds, "request", "session_expired");
            return Err(ManagementAccessError::SessionExpired);
        }
        if write && csrf_token != Some(session.csrf_token.as_str()) {
            self.audit(now_unix_seconds, "request", "csrf_denied");
            return Err(ManagementAccessError::CsrfInvalid);
        }
        self.audit(now_unix_seconds, "request", "allowed");
        Ok(())
    }

    pub fn audit_snapshot(&self) -> Vec<ManagementAuditEntry> {
        self.audit
            .lock()
            .map_or_else(|_| Vec::new(), |entries| entries.iter().cloned().collect())
    }

    fn require_origin(
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

    fn audit(&self, occurred_at_unix_seconds: u64, action: &'static str, outcome: &'static str) {
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

fn random_token() -> String {
    Uuid::new_v4().simple().to_string()
}

fn is_https_origin(value: &str) -> bool {
    let Some(authority) = value.strip_prefix("https://") else {
        return false;
    };
    !authority.is_empty()
        && !authority.contains('/')
        && !authority.contains('@')
        && !authority.contains(char::is_whitespace)
}

#[cfg(test)]
mod tests {
    use super::{LanAccessConfig, ManagementAccessController, ManagementAccessError};
    use argon2::{Argon2, PasswordHasher, password_hash::SaltString};
    use std::collections::BTreeSet;

    fn controller() -> ManagementAccessController {
        let salt =
            SaltString::encode_b64(b"cmclient-access-fixture").expect("fixture salt should encode");
        let password_hash = Argon2::default()
            .hash_password(b"password", &salt)
            .expect("fixture password should hash")
            .to_string();
        ManagementAccessController::new(LanAccessConfig {
            password_hash,
            allowed_origins: BTreeSet::from([String::from("https://cmclient.example")]),
            session_ttl_seconds: 60,
            audit_capacity: 512,
        })
        .expect("configuration should be valid")
    }

    #[test]
    fn validates_login_session_origin_and_csrf_without_audit_secrets() {
        let controller = controller();
        let session = controller
            .login("client-a", "https://cmclient.example", "password", 100)
            .expect("password should authenticate");
        assert!(
            controller
                .authorize(
                    Some("https://cmclient.example"),
                    &session.id,
                    Some(&session.csrf_token),
                    true,
                    101,
                )
                .is_ok()
        );
        assert_eq!(
            controller.authorize(
                Some("https://cmclient.example"),
                &session.id,
                Some("wrong"),
                true,
                101,
            ),
            Err(ManagementAccessError::CsrfInvalid)
        );
        let audit = controller.audit_snapshot();
        assert!(audit.iter().any(|entry| entry.outcome == "allowed"));
        assert!(audit.iter().any(|entry| entry.outcome == "csrf_denied"));
    }

    #[test]
    fn fails_closed_for_invalid_origin_expired_sessions_and_bounded_login_attempts() {
        let controller = controller();
        assert_eq!(
            controller.login("client-a", "http://cmclient.example", "password", 100),
            Err(ManagementAccessError::OriginDenied)
        );
        for _ in 0..5 {
            assert_eq!(
                controller.login("client-a", "https://cmclient.example", "wrong", 100),
                Err(ManagementAccessError::CredentialsInvalid)
            );
        }
        assert_eq!(
            controller.login("client-a", "https://cmclient.example", "password", 100),
            Err(ManagementAccessError::LoginRateLimited)
        );
        let session = controller
            .login("client-b", "https://cmclient.example", "password", 100)
            .expect("different client should authenticate");
        assert_eq!(
            controller.authorize(
                Some("https://cmclient.example"),
                &session.id,
                None,
                false,
                160,
            ),
            Err(ManagementAccessError::SessionExpired)
        );
    }
}
