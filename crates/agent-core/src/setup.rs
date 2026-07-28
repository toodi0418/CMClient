//! Agent-owned setup state, generation fencing, and passive Meshtastic discovery.
//!
//! Setup is persisted below the one runtime root and is deliberately separate
//! from the Gateway database.  The Agent can therefore keep the Web/control
//! surfaces available while credentials and transport configuration are still
//! incomplete.  Discovery is bounded and observation-only: it never scans a
//! subnet and the setup validator accepts only the configuration handshake.

use crate::RuntimePaths;
use cmclient_runtime_primitives::{DocumentError, DocumentFormat, DurableDocument, TypedDocument};
use mdns_sd::{DaemonStatus, ServiceDaemon, ServiceEvent};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    net::{IpAddr, Ipv4Addr},
    sync::{Mutex, MutexGuard},
    time::{Duration, Instant},
};
use uuid::Uuid;

pub const SETUP_SCHEMA_VERSION: u8 = 1;
pub const CURRENT_TERMS_VERSION: &str = "cmclient-2.0-terms-v1";
pub const MESHTASTIC_SERVICE_TYPE: &str = "_meshtastic._tcp.local.";
pub const MESHTASTIC_TCP_PORT: u16 = 4_403;
pub const MAX_DISCOVERY_CANDIDATES: usize = 16;
pub const MAX_MDNS_EVENTS: usize = 64;
pub const MAX_MDNS_TIMEOUT: Duration = Duration::from_secs(5);
pub const MAX_SETUP_GENERATION: u64 = 9_007_199_254_740_991;
const RECOVERY_GENERATION_HIGH_BIT: u64 = 1 << 52;
const RECOVERY_GENERATION_RANDOM_MASK: u64 = (1 << 51) - 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SetupPhase {
    Uninitialized,
    TermsRequired,
    CredentialsRequired,
    Validating,
    Ready,
    RecoveryRequired,
}

impl SetupPhase {
    pub const fn setup_required(self) -> bool {
        !matches!(self, Self::Ready)
    }

    pub const fn reason_code(self) -> &'static str {
        match self {
            Self::Uninitialized | Self::TermsRequired => "SETUP_TERMS_REQUIRED",
            Self::CredentialsRequired => "SETUP_CREDENTIALS_REQUIRED",
            Self::Validating => "SETUP_VALIDATING",
            Self::Ready => "SETUP_READY",
            Self::RecoveryRequired => "SETUP_RECOVERY_REQUIRED",
        }
    }
}

/// Durable state.  It contains no credential, endpoint, identity, or packet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SetupState {
    pub schema_version: u8,
    pub setup_generation: u64,
    pub phase: SetupPhase,
    pub terms_version: Option<String>,
}

impl SetupState {
    pub fn initial() -> Self {
        Self {
            schema_version: SETUP_SCHEMA_VERSION,
            setup_generation: 1,
            phase: SetupPhase::TermsRequired,
            terms_version: None,
        }
    }

    fn recovery() -> Self {
        let random = Uuid::new_v4().as_u128() as u64;
        Self {
            schema_version: SETUP_SCHEMA_VERSION,
            setup_generation: RECOVERY_GENERATION_HIGH_BIT
                | (random & RECOVERY_GENERATION_RANDOM_MASK),
            phase: SetupPhase::RecoveryRequired,
            terms_version: None,
        }
    }

    fn validate_terms_version(version: &str) -> bool {
        !version.is_empty()
            && version.len() <= 128
            && version
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
    }
}

impl DurableDocument for SetupState {
    const FORMAT: DocumentFormat = DocumentFormat::Json;
    const MAX_BYTES: usize = 16 * 1024;

    fn validate(&self) -> bool {
        self.schema_version == SETUP_SCHEMA_VERSION
            && (1..=MAX_SETUP_GENERATION).contains(&self.setup_generation)
            && match self.phase {
                SetupPhase::Uninitialized
                | SetupPhase::TermsRequired
                | SetupPhase::RecoveryRequired => self.terms_version.is_none(),
                SetupPhase::CredentialsRequired | SetupPhase::Validating | SetupPhase::Ready => {
                    self.terms_version
                        .as_deref()
                        .is_some_and(Self::validate_terms_version)
                }
            }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SetupStatus {
    pub schema_version: u8,
    pub current_terms_version: String,
    pub phase: SetupPhase,
    pub setup_required: bool,
    pub terms_required: bool,
    pub credentials_required: bool,
    pub validating: bool,
    pub ready: bool,
    pub recovery_required: bool,
    pub reason_code: String,
}

impl From<&SetupState> for SetupStatus {
    fn from(state: &SetupState) -> Self {
        Self {
            schema_version: SETUP_SCHEMA_VERSION,
            current_terms_version: String::from(CURRENT_TERMS_VERSION),
            phase: state.phase,
            setup_required: state.phase.setup_required(),
            terms_required: matches!(
                state.phase,
                SetupPhase::Uninitialized | SetupPhase::TermsRequired
            ),
            credentials_required: matches!(state.phase, SetupPhase::CredentialsRequired),
            validating: matches!(state.phase, SetupPhase::Validating),
            ready: matches!(state.phase, SetupPhase::Ready),
            recovery_required: matches!(state.phase, SetupPhase::RecoveryRequired),
            reason_code: String::from(state.phase.reason_code()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GenerationFence {
    generation: u64,
}

impl GenerationFence {
    pub const fn generation(self) -> u64 {
        self.generation
    }

    pub fn is_current(self, state: &SetupState) -> bool {
        state.setup_generation == self.generation
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetupError {
    PathInvalid,
    ReadFailed,
    Invalid,
    WriteFailed,
    TransitionInvalid,
    StaleGeneration,
    GenerationExhausted,
}

impl SetupError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::PathInvalid => "SETUP_STATE_PATH_INVALID",
            Self::ReadFailed => "SETUP_STATE_READ_FAILED",
            Self::Invalid => "SETUP_STATE_INVALID",
            Self::WriteFailed => "SETUP_STATE_WRITE_FAILED",
            Self::TransitionInvalid => "SETUP_STATE_TRANSITION_INVALID",
            Self::StaleGeneration => "SETUP_GENERATION_STALE",
            Self::GenerationExhausted => "SETUP_GENERATION_EXHAUSTED",
        }
    }
}

/// Serialized mutations of `setup.json` are guarded in-process.  The Agent's
/// process lock provides the cross-process boundary; this lock protects Web,
/// Control, and supervisor threads inside that process.
pub struct SetupStore {
    document: TypedDocument<SetupState>,
    state: Mutex<SetupState>,
}

impl SetupStore {
    pub fn open(paths: &RuntimePaths) -> Result<Self, SetupError> {
        Self::open_file(paths.setup_state_file())
    }

    pub fn open_file(path: impl Into<std::path::PathBuf>) -> Result<Self, SetupError> {
        let document = TypedDocument::<SetupState>::new(path).map_err(map_document_error)?;
        let mut state = match document.load_optional() {
            Ok(Some(state)) => state,
            Ok(None) => {
                let state = SetupState::initial();
                document.store(&state).map_err(map_document_error)?;
                state
            }
            Err(
                DocumentError::Malformed | DocumentError::SchemaInvalid | DocumentError::TooLarge,
            ) => {
                let state = SetupState::recovery();
                document.store(&state).map_err(map_document_error)?;
                state
            }
            Err(error) => return Err(map_document_error(error)),
        };
        if !state.validate() {
            return Err(SetupError::Invalid);
        }
        let terms_changed = !matches!(
            state.phase,
            SetupPhase::TermsRequired | SetupPhase::Uninitialized | SetupPhase::RecoveryRequired
        ) && state.terms_version.as_deref() != Some(CURRENT_TERMS_VERSION);
        if matches!(state.phase, SetupPhase::Uninitialized) || terms_changed {
            if terms_changed {
                state.setup_generation = next_generation(state.setup_generation)?;
            }
            state.phase = SetupPhase::TermsRequired;
            state.terms_version = None;
            document.store(&state).map_err(map_document_error)?;
        }
        Ok(Self {
            document,
            state: Mutex::new(state),
        })
    }

    pub fn snapshot(&self) -> Result<SetupState, SetupError> {
        self.lock_state().map(|state| state.clone())
    }

    pub fn status(&self) -> Result<SetupStatus, SetupError> {
        self.lock_state().map(|state| SetupStatus::from(&*state))
    }

    pub fn generation(&self) -> Result<GenerationFence, SetupError> {
        self.lock_state().map(|state| GenerationFence {
            generation: state.setup_generation,
        })
    }

    pub fn accept_terms(&self, terms_version: &str) -> Result<SetupStatus, SetupError> {
        if terms_version != CURRENT_TERMS_VERSION {
            return Err(SetupError::TransitionInvalid);
        }
        self.mutate(|state| {
            if !matches!(
                state.phase,
                SetupPhase::TermsRequired | SetupPhase::Uninitialized
            ) {
                return Err(SetupError::TransitionInvalid);
            }
            state.phase = SetupPhase::CredentialsRequired;
            state.terms_version = Some(String::from(terms_version));
            Ok(())
        })
    }

    pub fn begin_validation(&self) -> Result<GenerationFence, SetupError> {
        let mut state = self.lock_state()?;
        let mut next = state.clone();
        if !matches!(next.phase, SetupPhase::CredentialsRequired) {
            return Err(SetupError::TransitionInvalid);
        }
        next.phase = SetupPhase::Validating;
        if !next.validate() {
            return Err(SetupError::Invalid);
        }
        self.document.store(&next).map_err(map_document_error)?;
        *state = next;
        Ok(GenerationFence {
            generation: state.setup_generation,
        })
    }

    pub fn mark_ready(&self, fence: GenerationFence) -> Result<SetupStatus, SetupError> {
        self.mutate(|state| {
            if !fence.is_current(state) || !matches!(state.phase, SetupPhase::Validating) {
                return Err(SetupError::StaleGeneration);
            }
            state.phase = SetupPhase::Ready;
            Ok(())
        })
    }

    pub fn require_credentials(&self) -> Result<SetupStatus, SetupError> {
        self.mutate(|state| {
            if matches!(
                state.phase,
                SetupPhase::TermsRequired | SetupPhase::Uninitialized
            ) {
                return Ok(());
            }
            state.phase = SetupPhase::CredentialsRequired;
            Ok(())
        })
    }

    pub fn reset(&self) -> Result<SetupStatus, SetupError> {
        self.mutate(|state| {
            state.setup_generation = next_generation(state.setup_generation)?;
            state.phase = SetupPhase::TermsRequired;
            state.terms_version = None;
            Ok(())
        })
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, SetupState>, SetupError> {
        self.state.lock().map_err(|_| SetupError::ReadFailed)
    }

    fn mutate<F>(&self, mutation: F) -> Result<SetupStatus, SetupError>
    where
        F: FnOnce(&mut SetupState) -> Result<(), SetupError>,
    {
        let mut state = self.lock_state()?;
        let mut next = state.clone();
        mutation(&mut next)?;
        if !next.validate() {
            return Err(SetupError::Invalid);
        }
        self.document.store(&next).map_err(map_document_error)?;
        *state = next;
        Ok(SetupStatus::from(&*state))
    }
}

fn next_generation(current: u64) -> Result<u64, SetupError> {
    current
        .checked_add(1)
        .filter(|generation| *generation <= MAX_SETUP_GENERATION)
        .ok_or(SetupError::GenerationExhausted)
}

fn map_document_error(error: DocumentError) -> SetupError {
    match error {
        DocumentError::PathInvalid => SetupError::PathInvalid,
        DocumentError::Malformed | DocumentError::SchemaInvalid | DocumentError::TooLarge => {
            SetupError::Invalid
        }
        DocumentError::ReadFailed => SetupError::ReadFailed,
        DocumentError::EncodeFailed
        | DocumentError::WriteFailed
        | DocumentError::CommitFailed
        | DocumentError::DiscardFailed
        | DocumentError::PolicyInvalid => SetupError::WriteFailed,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscoverySource {
    Migrated,
    Loopback,
    Mdns,
    Manual,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MeshtasticCandidate {
    pub host: String,
    pub port: u16,
    pub source: DiscoverySource,
}

impl MeshtasticCandidate {
    pub fn new(host: impl Into<String>, port: u16, source: DiscoverySource) -> Option<Self> {
        let host = host.into();
        if !valid_endpoint_host(&host) || port != MESHTASTIC_TCP_PORT {
            return None;
        }
        Some(Self { host, port, source })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscoveryError {
    Busy,
    ConfigurationInvalid,
    MdnsUnavailable,
    MdnsFailed,
}

impl DiscoveryError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::Busy => "SETUP_DISCOVERY_BUSY",
            Self::ConfigurationInvalid => "SETUP_DISCOVERY_CONFIGURATION_INVALID",
            Self::MdnsUnavailable => "SETUP_MDNS_UNAVAILABLE",
            Self::MdnsFailed => "SETUP_MDNS_FAILED",
        }
    }
}

/// Return candidates in the user-visible, bounded order. The first matching
/// endpoint wins; later discovery sources cannot reorder or duplicate it.
pub fn ordered_candidates(
    migrated: Option<MeshtasticCandidate>,
    mdns: impl IntoIterator<Item = MeshtasticCandidate>,
    manual: Option<MeshtasticCandidate>,
) -> Vec<MeshtasticCandidate> {
    let mut candidates = Vec::with_capacity(MAX_DISCOVERY_CANDIDATES);
    let mut seen = BTreeSet::new();
    let loopback = [Ipv4Addr::LOCALHOST.to_string(), String::from("::1")];
    let mut push = |candidate: MeshtasticCandidate| {
        if candidates.len() >= MAX_DISCOVERY_CANDIDATES || !valid_endpoint_host(&candidate.host) {
            return;
        }
        let key = format!("{}:{}", candidate.host.to_ascii_lowercase(), candidate.port);
        if seen.insert(key) {
            candidates.push(candidate);
        }
    };
    if let Some(candidate) =
        migrated.filter(|candidate| candidate.source == DiscoverySource::Migrated)
    {
        push(candidate);
    }
    for host in loopback {
        if let Some(candidate) =
            MeshtasticCandidate::new(host, MESHTASTIC_TCP_PORT, DiscoverySource::Loopback)
        {
            push(candidate);
        }
    }
    for candidate in mdns {
        if candidate.source == DiscoverySource::Mdns
            && candidate
                .host
                .parse::<IpAddr>()
                .is_ok_and(valid_discovered_address)
        {
            push(candidate);
        }
    }
    if let Some(candidate) = manual.filter(|candidate| candidate.source == DiscoverySource::Manual)
    {
        push(candidate);
    }
    candidates
}

/// Browse only the standard Meshtastic service.  The daemon is stopped before
/// returning, and both the event count and wall-clock window are bounded.
pub fn discover_mdns(
    timeout: Duration,
    maximum: usize,
) -> Result<Vec<MeshtasticCandidate>, DiscoveryError> {
    if timeout.is_zero()
        || timeout > MAX_MDNS_TIMEOUT
        || maximum == 0
        || maximum > MAX_DISCOVERY_CANDIDATES
    {
        return Err(DiscoveryError::ConfigurationInvalid);
    }
    let daemon = ServiceDaemon::new().map_err(|_| DiscoveryError::MdnsUnavailable)?;
    let receiver = daemon
        .browse(MESHTASTIC_SERVICE_TYPE)
        .map_err(|_| DiscoveryError::MdnsFailed)?;
    let deadline = Instant::now() + timeout;
    let mut events = 0;
    let mut candidates = Vec::new();
    let mut seen = BTreeSet::new();
    while events < MAX_MDNS_EVENTS && candidates.len() < maximum {
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            break;
        };
        let Ok(event) = receiver.recv_timeout(remaining) else {
            break;
        };
        events += 1;
        let ServiceEvent::ServiceResolved(service) = event else {
            continue;
        };
        if !service.is_valid() || service.get_port() != MESHTASTIC_TCP_PORT {
            continue;
        }
        let mut addresses = service
            .get_addresses()
            .iter()
            .filter(|address| valid_discovered_address(address.to_ip_addr()))
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        addresses.sort();
        for host in addresses {
            if candidates.len() >= maximum {
                continue;
            }
            let key = format!("{host}:{}", service.get_port());
            if seen.insert(key) {
                if let Some(candidate) =
                    MeshtasticCandidate::new(host, service.get_port(), DiscoverySource::Mdns)
                {
                    candidates.push(candidate);
                }
            }
        }
    }
    let browse_stopped = daemon.stop_browse(MESHTASTIC_SERVICE_TYPE).is_ok();
    let daemon_stopped = daemon
        .shutdown()
        .ok()
        .and_then(|status| status.recv_timeout(Duration::from_secs(1)).ok())
        .is_some_and(|status| status == DaemonStatus::Shutdown);
    if !browse_stopped || !daemon_stopped {
        return Err(DiscoveryError::MdnsFailed);
    }
    Ok(candidates)
}

fn valid_discovered_address(address: IpAddr) -> bool {
    !address.is_unspecified() && !address.is_multicast() && !address.is_loopback()
}

fn valid_endpoint_host(host: &str) -> bool {
    !host.is_empty()
        && host.len() <= 255
        && !host
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
        && !host.contains('/')
        && !host.contains("://")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetupWireAction {
    WantConfigId,
    ConfigurationResponse,
    MeshPacket,
    AdminCommand,
    ConfigurationMutation,
    TextMessage,
    Position,
    Reboot,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SetupValidationPolicy;

impl SetupValidationPolicy {
    pub const fn allow(action: SetupWireAction) -> Result<(), SetupValidationError> {
        match action {
            SetupWireAction::WantConfigId | SetupWireAction::ConfigurationResponse => Ok(()),
            _ => Err(SetupValidationError::ForbiddenAction),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetupValidationError {
    ForbiddenAction,
}

impl SetupValidationError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::ForbiddenAction => "SETUP_VALIDATION_ACTION_FORBIDDEN",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, path::PathBuf};
    use uuid::Uuid;

    fn fixture_path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!("cmclient-setup-{label}-{}", Uuid::new_v4()))
    }

    #[test]
    fn missing_state_is_persisted_as_terms_required_and_public_status_is_redacted() {
        let root = fixture_path("initial");
        let paths = RuntimePaths {
            data_dir: root.clone(),
            config_dir: root.clone(),
            cache_dir: root.join("cache"),
            log_dir: root.join("logs"),
        };
        let store = SetupStore::open(&paths).expect("setup state should initialize");
        assert_eq!(
            store.snapshot().expect("state should load").phase,
            SetupPhase::TermsRequired
        );
        assert!(paths.setup_state_file().is_file());
        let status = store.status().expect("public status should load");
        assert!(status.setup_required && status.terms_required);
        let serialized = serde_json::to_string(&status).expect("status should serialize");
        assert!(serialized.contains("\"phase\":\"terms_required\""));
        assert!(serialized.contains("\"recoveryRequired\":false"));
        assert!(!serialized.contains("generation"));
        assert!(!serialized.contains("127.0.0.1"));
        fs::remove_dir_all(root).expect("fixture should clean up");
    }

    #[test]
    fn recovery_status_is_explicit_and_remains_redacted() {
        let root = fixture_path("recovery");
        let path = root.join("state/setup.json");
        let document = TypedDocument::<SetupState>::new(&path).expect("document should create");
        document
            .store(&SetupState {
                schema_version: SETUP_SCHEMA_VERSION,
                setup_generation: 4,
                phase: SetupPhase::RecoveryRequired,
                terms_version: None,
            })
            .expect("fixture should persist");

        let store = SetupStore::open_file(&path).expect("recovery state should reopen");
        let status = store.status().expect("recovery status should load");
        assert_eq!(status.phase, SetupPhase::RecoveryRequired);
        assert!(status.setup_required && status.recovery_required);
        assert_eq!(status.reason_code, "SETUP_RECOVERY_REQUIRED");
        let serialized = serde_json::to_string(&status).expect("status should serialize");
        assert!(!serialized.contains("setupGeneration"));
        fs::remove_dir_all(root).expect("fixture should clean up");
    }

    #[test]
    fn invalid_documents_are_replaced_by_a_redacted_fresh_recovery_generation() {
        let oversized = vec![b'x'; SetupState::MAX_BYTES + 1];
        for (label, bytes) in [
            ("malformed", b"{credential-marker".to_vec()),
            (
                "unknown-field",
                br#"{"schemaVersion":1,"setupGeneration":9,"phase":"ready","termsVersion":"cmclient-2.0-terms-v1","credentialMarker":"must-not-survive"}"#.to_vec(),
            ),
            (
                "zero-generation",
                br#"{"schemaVersion":1,"setupGeneration":0,"phase":"terms_required","termsVersion":null}"#.to_vec(),
            ),
            ("oversized", oversized),
        ] {
            let root = fixture_path(label);
            let path = root.join("state/setup.json");
            fs::create_dir_all(path.parent().expect("state parent should exist"))
                .expect("state directory should create");
            fs::write(&path, bytes).expect("invalid fixture should write");

            let store = SetupStore::open_file(&path).expect("invalid state should enter recovery");
            let state = store.snapshot().expect("recovery state should load");
            assert_eq!(state.phase, SetupPhase::RecoveryRequired);
            assert!((RECOVERY_GENERATION_HIGH_BIT..=MAX_SETUP_GENERATION)
                .contains(&state.setup_generation));
            let status = store.status().expect("recovery status should project");
            assert!(status.setup_required && status.recovery_required);
            let persisted = fs::read_to_string(&path).expect("replacement state should read");
            assert!(!persisted.contains("credential"));
            assert!(!persisted.contains("must-not-survive"));
            assert!(persisted.len() < 512);
            fs::remove_dir_all(root).expect("fixture should clean up");
        }
    }

    #[test]
    fn an_uninitialized_document_is_normalized_before_it_is_exposed() {
        let root = fixture_path("uninitialized");
        let path = root.join("state/setup.json");
        let document = TypedDocument::<SetupState>::new(&path).expect("document should create");
        document
            .store(&SetupState {
                schema_version: SETUP_SCHEMA_VERSION,
                setup_generation: 1,
                phase: SetupPhase::Uninitialized,
                terms_version: None,
            })
            .expect("fixture should persist");
        let store = SetupStore::open_file(&path).expect("state should normalize");
        assert_eq!(
            store.snapshot().expect("state should load").phase,
            SetupPhase::TermsRequired
        );
        fs::remove_dir_all(root).expect("fixture should clean up");
    }

    #[test]
    fn generation_fence_rejects_stale_validation_after_reset() {
        let root = fixture_path("generation");
        let store =
            SetupStore::open_file(root.join("state/setup.json")).expect("state should initialize");
        store
            .accept_terms(CURRENT_TERMS_VERSION)
            .expect("terms should be accepted");
        let fence = store.begin_validation().expect("validation should begin");
        let status = store.reset().expect("reset should succeed");
        assert!(status.terms_required && status.setup_required);
        assert!(matches!(
            store.mark_ready(fence),
            Err(SetupError::StaleGeneration)
        ));
        assert_eq!(
            store
                .generation()
                .expect("generation should load")
                .generation(),
            fence.generation() + 1
        );
        fs::remove_dir_all(root).expect("fixture should clean up");
    }

    #[test]
    fn state_survives_restart_and_only_valid_transitions_reach_ready() {
        let root = fixture_path("restart");
        let path = root.join("state/setup.json");
        let store = SetupStore::open_file(&path).expect("state should initialize");
        assert!(matches!(
            store.mark_ready(GenerationFence { generation: 1 }),
            Err(SetupError::StaleGeneration)
        ));
        store
            .accept_terms(CURRENT_TERMS_VERSION)
            .expect("terms should be accepted");
        let fence = store.begin_validation().expect("validation should begin");
        store
            .mark_ready(fence)
            .expect("ready transition should succeed");
        drop(store);
        let restarted = SetupStore::open_file(&path).expect("state should survive restart");
        assert!(restarted.status().expect("status should load").ready);
        fs::remove_dir_all(root).expect("fixture should clean up");
    }

    #[test]
    fn a_terms_version_change_reopens_setup_and_advances_generation() {
        let root = fixture_path("terms-version");
        let path = root.join("state/setup.json");
        let store = SetupStore::open_file(&path).expect("state should initialize");
        store
            .accept_terms(CURRENT_TERMS_VERSION)
            .expect("terms should be accepted");
        let fence = store.begin_validation().expect("validation should begin");
        store
            .mark_ready(fence)
            .expect("ready transition should succeed");
        let mut raw: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).expect("state file should read"))
                .expect("state should be JSON");
        raw["termsVersion"] = serde_json::Value::String(String::from("cmclient-1.0-terms-v0"));
        fs::write(
            &path,
            serde_json::to_vec(&raw).expect("fixture should encode"),
        )
        .expect("fixture should write");
        let reopened = SetupStore::open_file(&path).expect("state should reopen");
        let status = reopened.status().expect("status should load");
        assert!(status.terms_required && status.setup_required);
        assert_eq!(
            reopened
                .generation()
                .expect("generation should load")
                .generation(),
            fence.generation() + 1
        );
        fs::remove_dir_all(root).expect("fixture should clean up");
    }

    #[test]
    fn candidate_order_is_migrated_then_loopback_then_mdns_then_manual_without_duplicates() {
        let migrated =
            MeshtasticCandidate::new("192.0.2.10", MESHTASTIC_TCP_PORT, DiscoverySource::Migrated);
        let mdns = vec![
            MeshtasticCandidate::new("192.0.2.10", MESHTASTIC_TCP_PORT, DiscoverySource::Mdns)
                .unwrap(),
            MeshtasticCandidate::new("192.0.2.11", MESHTASTIC_TCP_PORT, DiscoverySource::Mdns)
                .unwrap(),
        ];
        let manual =
            MeshtasticCandidate::new("192.0.2.12", MESHTASTIC_TCP_PORT, DiscoverySource::Manual);
        let candidates = ordered_candidates(migrated, mdns, manual);
        assert_eq!(candidates[0].source, DiscoverySource::Migrated);
        assert_eq!(candidates[1].host, "127.0.0.1");
        assert_eq!(candidates[1].source, DiscoverySource::Loopback);
        assert_eq!(candidates[2].host, "::1");
        assert_eq!(candidates[3].host, "192.0.2.11");
        assert_eq!(candidates[4].source, DiscoverySource::Manual);
        assert_eq!(candidates.len(), 5);
    }

    #[test]
    fn candidates_are_bounded_and_reject_non_meshtastic_ports_or_untrusted_hosts() {
        assert!(MeshtasticCandidate::new("192.0.2.1", 80, DiscoverySource::Manual).is_none());
        assert!(
            MeshtasticCandidate::new(
                "http://192.0.2.1",
                MESHTASTIC_TCP_PORT,
                DiscoverySource::Manual
            )
            .is_none()
        );
        let candidates = ordered_candidates(
            None,
            (1..=MAX_DISCOVERY_CANDIDATES + 8).filter_map(|index| {
                MeshtasticCandidate::new(
                    format!("192.0.2.{index}"),
                    MESHTASTIC_TCP_PORT,
                    DiscoverySource::Mdns,
                )
            }),
            None,
        );
        assert_eq!(candidates.len(), MAX_DISCOVERY_CANDIDATES);
    }

    #[test]
    fn setup_validation_allows_only_configuration_handshake_actions() {
        assert!(SetupValidationPolicy::allow(SetupWireAction::WantConfigId).is_ok());
        assert!(SetupValidationPolicy::allow(SetupWireAction::ConfigurationResponse).is_ok());
        for action in [
            SetupWireAction::MeshPacket,
            SetupWireAction::AdminCommand,
            SetupWireAction::ConfigurationMutation,
            SetupWireAction::TextMessage,
            SetupWireAction::Position,
            SetupWireAction::Reboot,
            SetupWireAction::Unknown,
        ] {
            assert_eq!(
                SetupValidationPolicy::allow(action),
                Err(SetupValidationError::ForbiddenAction)
            );
        }
    }

    #[test]
    fn mdns_configuration_is_bounded_before_any_socket_is_created() {
        assert_eq!(
            discover_mdns(Duration::from_secs(6), 1),
            Err(DiscoveryError::ConfigurationInvalid)
        );
        assert_eq!(
            discover_mdns(Duration::from_secs(1), 0),
            Err(DiscoveryError::ConfigurationInvalid)
        );
    }

    #[test]
    fn mdns_results_reject_loopback_and_non_unicast_addresses() {
        assert!(!valid_discovered_address(IpAddr::V4(
            std::net::Ipv4Addr::LOCALHOST
        )));
        assert!(!valid_discovered_address(IpAddr::V6(
            std::net::Ipv6Addr::LOCALHOST
        )));
        assert!(!valid_discovered_address(IpAddr::V4(
            std::net::Ipv4Addr::UNSPECIFIED
        )));
        assert!(valid_discovered_address(IpAddr::V4(
            std::net::Ipv4Addr::new(192, 0, 2, 10)
        )));

        let candidates = ordered_candidates(
            None,
            [
                MeshtasticCandidate::new("127.0.0.1", MESHTASTIC_TCP_PORT, DiscoverySource::Mdns)
                    .expect("synthetic candidate should parse"),
                MeshtasticCandidate::new("192.0.2.10", MESHTASTIC_TCP_PORT, DiscoverySource::Mdns)
                    .expect("LAN candidate should parse"),
            ],
            None,
        );
        assert_eq!(candidates.len(), 3);
        assert!(candidates.iter().any(|candidate| {
            candidate.source == DiscoverySource::Mdns && candidate.host == "192.0.2.10"
        }));
        assert!(!candidates.iter().any(|candidate| {
            candidate.source == DiscoverySource::Mdns && candidate.host == "127.0.0.1"
        }));
    }
}
