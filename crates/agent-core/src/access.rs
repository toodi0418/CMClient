use argon2::{Argon2, MIN_SALT_LEN, Params, PasswordHash, PasswordVerifier};
use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
use uuid::Uuid;

const MAX_AUDIT_CAPACITY: usize = 4_096;
const MAX_LOGIN_FAILURES: u8 = 5;
const LOGIN_WINDOW_SECONDS: u64 = 60;
const MAX_LOGIN_SOURCE_WINDOWS: usize = 4_096;
const MAX_MANAGEMENT_SESSIONS: usize = 1_024;
const MAX_CONCURRENT_PASSWORD_VERIFICATIONS: usize = 2;
const MIN_ARGON2_MEMORY_KIB: u32 = 19_456;
const MAX_ARGON2_MEMORY_KIB: u32 = 65_536;
const MIN_ARGON2_ITERATIONS: u32 = 2;
const MAX_ARGON2_ITERATIONS: u32 = 6;
const MIN_ARGON2_LANES: u32 = 1;
const MAX_ARGON2_LANES: u32 = 4;
const MIN_ARGON2_OUTPUT_BYTES: usize = 16;
const MAX_ARGON2_OUTPUT_BYTES: usize = 64;

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

struct PasswordVerificationLimiter {
    active: AtomicUsize,
}

impl PasswordVerificationLimiter {
    const fn new() -> Self {
        Self {
            active: AtomicUsize::new(0),
        }
    }

    fn try_acquire(&self) -> Option<PasswordVerificationPermit<'_>> {
        self.active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                (active < MAX_CONCURRENT_PASSWORD_VERIFICATIONS).then_some(active + 1)
            })
            .ok()
            .map(|_| PasswordVerificationPermit {
                active: &self.active,
            })
    }

    #[cfg(test)]
    fn active(&self) -> usize {
        self.active.load(Ordering::Acquire)
    }
}

struct PasswordVerificationPermit<'a> {
    active: &'a AtomicUsize,
}

impl Drop for PasswordVerificationPermit<'_> {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::AcqRel);
    }
}

pub struct ManagementAccessController {
    password_hash: String,
    allowed_origins: BTreeSet<String>,
    session_ttl_seconds: u64,
    audit_capacity: usize,
    sessions: Mutex<HashMap<String, SessionRecord>>,
    failures: Mutex<HashMap<String, FailureWindow>>,
    password_verifications: PasswordVerificationLimiter,
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
        validate_password_hash(&config.password_hash)?;
        Ok(Self {
            password_hash: config.password_hash,
            allowed_origins: config.allowed_origins,
            session_ttl_seconds: config.session_ttl_seconds,
            audit_capacity: config.audit_capacity,
            sessions: Mutex::new(HashMap::new()),
            failures: Mutex::new(HashMap::new()),
            password_verifications: PasswordVerificationLimiter::new(),
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
        self.login_with_verifier(
            remote_key,
            origin,
            password,
            now_unix_seconds,
            |password_hash, candidate| {
                PasswordHash::new(password_hash)
                    .ok()
                    .and_then(|hash| {
                        Argon2::default()
                            .verify_password(candidate.as_bytes(), &hash)
                            .ok()
                    })
                    .is_some()
            },
        )
    }

    fn login_with_verifier(
        &self,
        remote_key: &str,
        origin: &str,
        password: &str,
        now_unix_seconds: u64,
        verify: impl FnOnce(&str, &str) -> bool,
    ) -> Result<ManagementSession, ManagementAccessError> {
        self.require_origin(origin, now_unix_seconds)?;
        let Some(_verification_permit) = self.password_verifications.try_acquire() else {
            self.audit(now_unix_seconds, "login", "verification_capacity");
            return Err(ManagementAccessError::LoginRateLimited);
        };
        self.reserve_login_attempt(remote_key, now_unix_seconds)?;

        if !verify(&self.password_hash, password) {
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
        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| ManagementAccessError::SessionInvalid)?;
        sessions.retain(|_, record| record.expires_at_unix_seconds > now_unix_seconds);
        if sessions.len() >= MAX_MANAGEMENT_SESSIONS {
            self.audit(now_unix_seconds, "login", "session_capacity");
            return Err(ManagementAccessError::LoginRateLimited);
        }
        sessions.insert(
            session.id.clone(),
            SessionRecord {
                csrf_token: session.csrf_token.clone(),
                expires_at_unix_seconds: session.expires_at_unix_seconds,
            },
        );
        drop(sessions);
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
        let requested_session_expired = sessions
            .get(session_id)
            .is_some_and(|session| session.expires_at_unix_seconds <= now_unix_seconds);
        sessions.retain(|_, session| session.expires_at_unix_seconds > now_unix_seconds);
        if requested_session_expired {
            self.audit(now_unix_seconds, "request", "session_expired");
            return Err(ManagementAccessError::SessionExpired);
        }
        let Some(session) = sessions.get(session_id) else {
            self.audit(now_unix_seconds, "request", "session_denied");
            return Err(ManagementAccessError::SessionInvalid);
        };
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

    fn reserve_login_attempt(
        &self,
        remote_key: &str,
        now_unix_seconds: u64,
    ) -> Result<(), ManagementAccessError> {
        let mut failures = self
            .failures
            .lock()
            .map_err(|_| ManagementAccessError::CredentialsInvalid)?;
        failures.retain(|_, window| {
            now_unix_seconds.saturating_sub(window.started_at_unix_seconds) < LOGIN_WINDOW_SECONDS
        });
        if !failures.contains_key(remote_key) && failures.len() >= MAX_LOGIN_SOURCE_WINDOWS {
            drop(failures);
            self.audit(now_unix_seconds, "login", "source_capacity");
            return Err(ManagementAccessError::LoginRateLimited);
        }
        let window = failures
            .entry(remote_key.to_owned())
            .or_insert(FailureWindow {
                started_at_unix_seconds: now_unix_seconds,
                failures: 0,
            });
        if window.failures >= MAX_LOGIN_FAILURES {
            drop(failures);
            self.audit(now_unix_seconds, "login", "rate_limited");
            return Err(ManagementAccessError::LoginRateLimited);
        }
        window.failures = window.failures.saturating_add(1);
        Ok(())
    }

    #[cfg(test)]
    fn session_count(&self) -> usize {
        self.sessions.lock().map_or(0, |sessions| sessions.len())
    }

    #[cfg(test)]
    fn failure_source_count(&self) -> usize {
        self.failures.lock().map_or(0, |failures| failures.len())
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
    use super::{
        LanAccessConfig, MAX_LOGIN_SOURCE_WINDOWS, MAX_MANAGEMENT_SESSIONS,
        ManagementAccessController, ManagementAccessError,
    };
    use argon2::{Argon2, PasswordHasher, password_hash::SaltString};
    use std::{
        collections::BTreeSet,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
            mpsc,
        },
        thread,
        time::Duration,
    };

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

    fn config(password_hash: String) -> LanAccessConfig {
        LanAccessConfig {
            password_hash,
            allowed_origins: BTreeSet::from([String::from("https://cmclient.example")]),
            session_ttl_seconds: 60,
            audit_capacity: 512,
        }
    }

    struct VerificationRelease(Arc<AtomicBool>);

    impl Drop for VerificationRelease {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
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

    #[test]
    fn rejects_weak_or_unbounded_password_hash_parameters() {
        let salt =
            SaltString::encode_b64(b"cmclient-access-fixture").expect("fixture salt should encode");
        let valid = Argon2::default()
            .hash_password(b"password", &salt)
            .expect("fixture password should hash")
            .to_string();
        for invalid in [
            valid.replacen("$argon2id$", "$argon2i$", 1),
            valid.replacen("v=19$", "v=16$", 1),
            valid.replacen("m=19456", "m=8", 1),
            valid.replacen("m=19456", "m=1048576", 1),
            valid.replacen("t=2", "t=1", 1),
            valid.replacen("t=2", "t=7", 1),
            valid.replacen("p=1", "p=8", 1),
            valid.replacen("p=1", "p=1,x=1", 1),
            valid.replacen("Y21jbGllbnQtYWNjZXNzLWZpeHR1cmU", "YWJj", 1),
            valid.rsplit_once('$').map_or_else(
                || String::from("invalid"),
                |(prefix, _)| format!("{prefix}$YWJjZA"),
            ),
        ] {
            assert!(matches!(
                ManagementAccessController::new(config(invalid)),
                Err(ManagementAccessError::InvalidConfiguration)
            ));
        }
    }

    #[test]
    fn bounds_concurrent_password_verification_before_argon2_work() {
        const THREADS: usize = 12;
        let controller = Arc::new(controller());
        let start = Arc::new(std::sync::Barrier::new(THREADS + 1));
        let release = Arc::new(AtomicBool::new(false));
        let release_on_drop = VerificationRelease(Arc::clone(&release));
        let (entered_sender, entered_receiver) = mpsc::channel();
        let (result_sender, result_receiver) = mpsc::channel();
        let mut workers = Vec::new();
        for index in 0..THREADS {
            let controller = Arc::clone(&controller);
            let start = Arc::clone(&start);
            let release = Arc::clone(&release);
            let entered_sender = entered_sender.clone();
            let result_sender = result_sender.clone();
            workers.push(thread::spawn(move || {
                start.wait();
                let result = controller.login_with_verifier(
                    &format!("client-{index}"),
                    "https://cmclient.example",
                    "wrong",
                    100,
                    |_, _| {
                        entered_sender
                            .send(())
                            .expect("entered receiver should remain available");
                        while !release.load(Ordering::Acquire) {
                            thread::yield_now();
                        }
                        false
                    },
                );
                result_sender
                    .send(result)
                    .expect("result receiver should remain available");
            }));
        }
        drop(entered_sender);
        drop(result_sender);
        start.wait();
        for _ in 0..2 {
            entered_receiver
                .recv_timeout(Duration::from_secs(2))
                .expect("both bounded verifiers should enter");
        }
        let early_results = (0..THREADS - 2)
            .map(|_| {
                result_receiver
                    .recv_timeout(Duration::from_secs(2))
                    .expect("capacity rejections should complete before verifier release")
            })
            .collect::<Vec<_>>();
        assert!(
            early_results
                .iter()
                .all(|result| *result == Err(ManagementAccessError::LoginRateLimited))
        );
        assert_eq!(controller.password_verifications.active(), 2);
        release.store(true, Ordering::Release);
        drop(release_on_drop);

        let late_results = (0..2)
            .map(|_| {
                result_receiver
                    .recv_timeout(Duration::from_secs(2))
                    .expect("active verifiers should complete after release")
            })
            .collect::<Vec<_>>();
        assert!(
            late_results
                .iter()
                .all(|result| *result == Err(ManagementAccessError::CredentialsInvalid))
        );
        for worker in workers {
            worker.join().expect("worker should join");
        }
        assert_eq!(controller.password_verifications.active(), 0);
    }

    #[test]
    fn bounds_and_prunes_failure_sources_and_sessions() {
        let failure_controller = controller();
        for index in 0..MAX_LOGIN_SOURCE_WINDOWS {
            assert_eq!(
                failure_controller.login_with_verifier(
                    &format!("source-{index}"),
                    "https://cmclient.example",
                    "wrong",
                    100,
                    |_, _| false,
                ),
                Err(ManagementAccessError::CredentialsInvalid)
            );
        }
        assert_eq!(
            failure_controller.failure_source_count(),
            MAX_LOGIN_SOURCE_WINDOWS
        );
        assert_eq!(
            failure_controller.login_with_verifier(
                "overflow",
                "https://cmclient.example",
                "wrong",
                100,
                |_, _| false,
            ),
            Err(ManagementAccessError::LoginRateLimited)
        );
        assert_eq!(
            failure_controller.login_with_verifier(
                "fresh",
                "https://cmclient.example",
                "wrong",
                160,
                |_, _| false,
            ),
            Err(ManagementAccessError::CredentialsInvalid)
        );
        assert_eq!(failure_controller.failure_source_count(), 1);

        let session_controller = controller();
        let first = session_controller
            .login_with_verifier(
                "client",
                "https://cmclient.example",
                "password",
                100,
                |_, _| true,
            )
            .expect("first session should issue");
        for _ in 1..MAX_MANAGEMENT_SESSIONS {
            session_controller
                .login_with_verifier(
                    "client",
                    "https://cmclient.example",
                    "password",
                    100,
                    |_, _| true,
                )
                .expect("bounded session should issue");
        }
        assert_eq!(session_controller.session_count(), MAX_MANAGEMENT_SESSIONS);
        assert_eq!(
            session_controller.login_with_verifier(
                "client",
                "https://cmclient.example",
                "password",
                100,
                |_, _| true,
            ),
            Err(ManagementAccessError::LoginRateLimited)
        );
        assert_eq!(
            session_controller.authorize(
                Some("https://cmclient.example"),
                &first.id,
                None,
                false,
                160,
            ),
            Err(ManagementAccessError::SessionExpired)
        );
        assert_eq!(session_controller.session_count(), 0);
    }
}
