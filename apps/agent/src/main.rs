use atomic_write_file::AtomicWriteFile;
use axum::{
    Json, Router,
    extract::{State, rejection::JsonRejection},
    http::{HeaderMap, StatusCode},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{any, get, post},
};
use chrono::{SecondsFormat, Utc};
use cmclient_agent_core::access::{ManagementAccessController, ManagementAuditEntry};
use cmclient_agent_core::secrets::{
    AgentSecretStore, CMCloudActiveDeviceCredential, CallMeshSetupSecretState, SecretKind,
    SecretStoreError,
};
use cmclient_agent_core::setup::{
    DiscoveryError, DiscoverySource, MAX_DISCOVERY_CANDIDATES, MAX_MDNS_TIMEOUT,
    MeshtasticCandidate, SetupError, SetupPhase, SetupStatus, SetupStore, discover_mdns,
    ordered_candidates,
};
use cmclient_agent_core::web::{
    ActiveGatewayRoute, GATEWAY_CAPABILITY_HEADER, GatewayRoute, GatewaySessionHandle,
    ManagementSetupState, ManagementTlsConfig, ManagementWebConfig, ManagementWebError,
    ManagementWebProfile, ManagementWebService,
};
use cmclient_agent_core::{
    AgentConfig, AgentLease, AgentRuntimeProfile, AprsConfig, MeshtasticConnectionConfig,
    RuntimePaths, ensure_runtime_directories,
};
use cmclient_control_api::{
    ControlClient, ControlCommand, ControlEndpoint, ControlError, ControlHandler,
    ControlSecretKind, ControlServer, ControlStatus, ControlUpdateEvent, DiagnosticsControlBundle,
    GatewayControlStatus, GatewayProjection, InternalComponent, ManagementWebControlStatus,
    UpdateControlJob, UpdateControlStatus, compiled_component_identity, default_local_endpoint,
};
use cmclient_legacy_migration::{
    ChildGatewayMaintenanceRunner, GatewayMaintenanceRunner, ProductMigrationSourceSet,
    migrate_detected_product_source_sets,
};
use cmclient_runtime_logging::{LogLevel, LogPolicy, StructuredLogSink};
use cmclient_supervisor::{
    BackoffPolicy, GatewayCommand, GatewayLogHealthUpdate, GatewayReady, GatewayStatus,
    GatewaySupervisor, SupervisorEvent,
};
use cmclient_updater::{PersistentUpdateJob, UpdateJournalStore, recover_interrupted_update};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, VecDeque},
    env,
    ffi::OsString,
    fs,
    io::{BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    process::ExitCode,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc::{self, SyncSender, TrySendError},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use tokio_stream::{StreamExt, wrappers::BroadcastStream};
use zeroize::Zeroize;

mod cmcloud_enrollment;
mod tray;

const EX_USAGE: u8 = 2;
const EX_CONFIG: u8 = 5;
const UPDATE_EVENT_BUFFER: usize = 64;
const AGENT_EVENT_REPLAY_BUFFER: usize = 64;
const AGENT_EVENT_SUBSCRIBER_LIMIT: usize = 32;
const MAX_SSE_EVENT_BYTES: usize = 60 * 1024;
const SUPERVISOR_POLL_INTERVAL: Duration = Duration::from_millis(100);
const LIFECYCLE_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
const AGENT_AUDIT_CAPACITY: usize = 512;
const GATEWAY_SSE_READ_POLL_INTERVAL: Duration = Duration::from_secs(1);
const GATEWAY_SSE_RECONNECT_DELAY: Duration = Duration::from_secs(2);
const AGENT_LOG_FILE: &str = "agent.jsonl";
const GATEWAY_LOG_FILE: &str = "gateway.jsonl";
const MAX_GATEWAY_IDENTITY_BYTES: u64 = 64 * 1024;
const SETUP_VALIDATION_ONLY_ENVIRONMENT_NAME: &str = "CMCLIENT_SETUP_VALIDATION_ONLY";
const SETUP_COMMIT_START_ENVIRONMENT_NAME: &str = "CMCLIENT_SETUP_COMMIT_START";
const PROXY_ENABLED_ENVIRONMENT_NAME: &str = "CMCLIENT_PROXY_ENABLED";
const CMCLOUD_MODE_ENVIRONMENT_NAME: &str = "CMCLIENT_CMCLOUD_MODE";
const CMCLOUD_URL_ENVIRONMENT_NAME: &str = "CMCLIENT_CMCLOUD_URL";
const CMCLOUD_INSTALLATION_ID_ENVIRONMENT_NAME: &str = "CMCLIENT_CMCLOUD_INSTALLATION_ID";
const CMCLOUD_INSTALLATION_GENERATION_ENVIRONMENT_NAME: &str =
    "CMCLIENT_CMCLOUD_INSTALLATION_GENERATION";
const CMCLOUD_CREDENTIAL_VERSION_ENVIRONMENT_NAME: &str = "CMCLIENT_CMCLOUD_CREDENTIAL_VERSION";
const SETUP_TRANSACTION_FILE_NAME: &str = "setup-transaction.json";
const SETUP_TRANSACTION_VERSION: u8 = 1;
const RESET_TRANSACTION_FILE_NAME: &str = "reset-transaction.json";
const RESET_TRANSACTION_VERSION: u8 = 1;
const MAX_SETUP_CONFIGURATION_BYTES: usize = 1024 * 1024;
#[cfg(test)]
const FACTORY_RESET_FIXTURE_VERSION: u8 = 1;
#[cfg(test)]
const FACTORY_RESET_FIXTURE_MARKER_FILE_NAME: &str = ".factory-reset-fixture.json";
#[cfg(test)]
const FACTORY_RESET_FIXTURE_JOURNAL_FILE_NAME: &str = ".factory-reset-journal.json";
#[cfg(test)]
const FACTORY_RESET_FIXTURE_COMPLETION_FILE_NAME: &str = ".factory-reset-completed.json";

#[derive(Clone)]
struct AgentWebEvent {
    id: String,
    event: String,
    data: Vec<u8>,
}

struct AgentEventJournal {
    retained: VecDeque<AgentWebEvent>,
}

struct AgentEventHub {
    stream: &'static str,
    epoch: u64,
    next_sequence: AtomicU64,
    journal: Mutex<AgentEventJournal>,
    live: tokio::sync::broadcast::Sender<AgentWebEvent>,
    subscribers: Arc<AtomicUsize>,
    runtime_log: Option<StructuredLogSink>,
}

struct AgentEventSubscription {
    replay: Vec<AgentWebEvent>,
    live: tokio::sync::broadcast::Receiver<AgentWebEvent>,
    _permit: AgentEventSubscriberPermit,
}

struct AgentEventSubscriberPermit {
    subscribers: Arc<AtomicUsize>,
}

impl Drop for AgentEventSubscriberPermit {
    fn drop(&mut self) {
        self.subscribers.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AgentEventHubError {
    SubscriberLimit,
    Serialization,
    State,
}

impl AgentEventHub {
    fn new_with_log(stream: &'static str, runtime_log: Option<StructuredLogSink>) -> Self {
        let epoch = Utc::now()
            .timestamp_micros()
            .unsigned_abs()
            .saturating_add(u64::from(std::process::id()));
        Self::new_with_epoch_and_log(stream, epoch, runtime_log)
    }

    #[cfg(test)]
    fn new_with_epoch(stream: &'static str, epoch: u64) -> Self {
        Self::new_with_epoch_and_log(stream, epoch, None)
    }

    fn new_with_epoch_and_log(
        stream: &'static str,
        epoch: u64,
        runtime_log: Option<StructuredLogSink>,
    ) -> Self {
        let (live, _) = tokio::sync::broadcast::channel(AGENT_EVENT_REPLAY_BUFFER);
        Self {
            stream,
            epoch,
            next_sequence: AtomicU64::new(1),
            journal: Mutex::new(AgentEventJournal {
                retained: VecDeque::with_capacity(AGENT_EVENT_REPLAY_BUFFER),
            }),
            live,
            subscribers: Arc::new(AtomicUsize::new(0)),
            runtime_log,
        }
    }

    fn publish<T: Serialize>(
        &self,
        event_type: &'static str,
        payload: &T,
    ) -> Result<AgentWebEvent, AgentEventHubError> {
        let sequence = self.next_sequence.fetch_add(1, Ordering::AcqRel);
        let id = format!("agent:{}:{}-{sequence}", self.stream, self.epoch);
        let envelope = serde_json::json!({
            "eventId": id,
            "schemaVersion": 1,
            "stream": self.stream,
            "type": event_type,
            "occurredAt": utc_now(),
            "source": "agent",
            "payload": payload,
        });
        let data = serde_json::to_vec(&envelope).map_err(|_| AgentEventHubError::Serialization)?;
        if data.len() > MAX_SSE_EVENT_BYTES
            || data.iter().any(|byte| matches!(*byte, b'\r' | b'\n'))
        {
            return Err(AgentEventHubError::Serialization);
        }
        let event = AgentWebEvent {
            id,
            event: String::from(event_type),
            data,
        };
        let mut journal = self.journal.lock().map_err(|_| AgentEventHubError::State)?;
        if journal.retained.len() == AGENT_EVENT_REPLAY_BUFFER {
            journal.retained.pop_front();
        }
        journal.retained.push_back(event.clone());
        let _ = self.live.send(event.clone());
        Ok(event)
    }

    fn subscribe(
        &self,
        last_event_id: Option<&str>,
    ) -> Result<AgentEventSubscription, AgentEventHubError> {
        self.acquire_subscriber()?;
        let permit = AgentEventSubscriberPermit {
            subscribers: Arc::clone(&self.subscribers),
        };
        let journal = self.journal.lock().map_err(|_| AgentEventHubError::State)?;
        let live = self.live.subscribe();
        let replay = match last_event_id {
            Some(cursor) => journal
                .retained
                .iter()
                .position(|event| event.id == cursor)
                .map_or_else(
                    || journal.retained.back().cloned().into_iter().collect(),
                    |position| {
                        journal
                            .retained
                            .iter()
                            .skip(position + 1)
                            .cloned()
                            .collect()
                    },
                ),
            None => journal.retained.back().cloned().into_iter().collect(),
        };
        Ok(AgentEventSubscription {
            replay,
            live,
            _permit: permit,
        })
    }

    fn acquire_subscriber(&self) -> Result<(), AgentEventHubError> {
        let mut current = self.subscribers.load(Ordering::Acquire);
        loop {
            if current >= AGENT_EVENT_SUBSCRIBER_LIMIT {
                return Err(AgentEventHubError::SubscriberLimit);
            }
            match self.subscribers.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(observed) => current = observed,
            }
        }
    }
}

struct AgentUpdateService {
    journal: UpdateJournalStore,
    subscribers: Mutex<Vec<SyncSender<ControlUpdateEvent>>>,
    web_events: Arc<AgentEventHub>,
    next_event_id: AtomicU64,
}

impl AgentUpdateService {
    #[cfg(test)]
    fn new(data_dir: &Path) -> Result<Self, ControlError> {
        Self::new_with_log(data_dir, None)
    }

    fn new_with_log(
        data_dir: &Path,
        runtime_log: Option<StructuredLogSink>,
    ) -> Result<Self, ControlError> {
        let service = Self {
            journal: UpdateJournalStore::new(data_dir).map_err(|_| ControlError::CommandFailed)?,
            subscribers: Mutex::new(Vec::new()),
            web_events: Arc::new(AgentEventHub::new_with_log("update", runtime_log)),
            next_event_id: AtomicU64::new(1),
        };
        service.publish_web_status(&service.status()?)?;
        Ok(service)
    }

    fn recover(&self) -> Result<(), ControlError> {
        let recovered = recover_interrupted_update(&self.journal, utc_now())
            .map_err(|_| ControlError::CommandFailed)?;
        if let Some(job) = recovered {
            self.persist(&job)?;
        }
        Ok(())
    }

    fn persist(&self, job: &PersistentUpdateJob) -> Result<(), ControlError> {
        self.journal
            .persist(job)
            .map_err(|_| ControlError::CommandFailed)?;
        self.publish(job)
    }

    fn status(&self) -> Result<UpdateControlStatus, ControlError> {
        let job = self
            .journal
            .load()
            .map_err(|_| ControlError::CommandFailed)?
            .map(update_control_job);
        Ok(UpdateControlStatus {
            schema_version: 1,
            job,
        })
    }

    fn subscribe(&self) -> Result<mpsc::Receiver<ControlUpdateEvent>, ControlError> {
        let (sender, receiver) = mpsc::sync_channel(UPDATE_EVENT_BUFFER);
        sender
            .try_send(self.event_for(&self.status()?)?)
            .map_err(|_| ControlError::CommandFailed)?;
        self.subscribers
            .lock()
            .map_err(|_| ControlError::CommandFailed)?
            .push(sender);
        Ok(receiver)
    }

    fn publish(&self, job: &PersistentUpdateJob) -> Result<(), ControlError> {
        let status = UpdateControlStatus {
            schema_version: 1,
            job: Some(update_control_job(job.clone())),
        };
        let mut subscribers = self
            .subscribers
            .lock()
            .map_err(|_| ControlError::CommandFailed)?;
        let event = self.event_for(&status)?;
        subscribers.retain(|sender| sender.try_send(event.clone()).is_ok());
        self.publish_web_status(&status)?;
        Ok(())
    }

    fn publish_web_status(&self, status: &UpdateControlStatus) -> Result<(), ControlError> {
        self.web_events
            .publish("update.status", status)
            .map(|_| ())
            .map_err(|_| ControlError::CommandFailed)
    }

    fn event_for(&self, status: &UpdateControlStatus) -> Result<ControlUpdateEvent, ControlError> {
        let sequence = self.next_event_id.fetch_add(1, Ordering::Relaxed);
        Ok(ControlUpdateEvent {
            id: format!("update-{sequence}"),
            event: String::from("update.status_changed"),
            data: serde_json::to_vec(status).map_err(|_| ControlError::CommandFailed)?,
        })
    }
}

fn update_control_job(job: PersistentUpdateJob) -> UpdateControlJob {
    let (bytes_downloaded, bytes_total, bytes_per_second) =
        job.progress.map_or((None, None, None), |progress| {
            (
                Some(progress.bytes_downloaded),
                progress.bytes_total,
                progress.bytes_per_second,
            )
        });
    UpdateControlJob {
        id: job.id,
        phase: job.phase.as_str().to_owned(),
        updated_at: job.updated_at,
        error_code: job.error_code,
        bytes_downloaded,
        bytes_total,
        bytes_per_second,
        recent_log_codes: job
            .recent_logs
            .into_iter()
            .map(|entry| entry.code)
            .collect(),
    }
}

fn utc_now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct AgentLifecycleStatus {
    schema_version: u8,
    agent: String,
    gateway: String,
    management_web: String,
    management_web_url: Option<String>,
    uptime_seconds: u64,
    latest_error_code: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SetupTermsRequest {
    terms_version: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SetupResetRequest {
    confirmation: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ResetKind {
    Operational,
    Factory,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ResetTransactionJournal {
    version: u8,
    kind: ResetKind,
    target_generation: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ResetCompletionMarker {
    version: u8,
    kind: ResetKind,
}

// This worker is test-only by design. It provides the destructive-reset
// recovery contract without creating a production path to a user's root.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum FactoryResetFixturePhase {
    Prepared,
    Quiesced,
    MutableStateCleared,
    RootRecreated,
    Completed,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum FactoryResetBackupBehavior {
    RetainExisting,
    EraseAll,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
struct FactoryResetFixtureConfirmation {
    backup_behavior: FactoryResetBackupBehavior,
    first_confirmation: &'static str,
    final_confirmation: &'static str,
}

#[cfg(test)]
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FactoryResetFixtureMarker {
    version: u8,
    nonce: String,
}

#[cfg(test)]
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FactoryResetFixtureJournal {
    version: u8,
    nonce: String,
    backup_behavior: FactoryResetBackupBehavior,
    phase: FactoryResetFixturePhase,
}

#[cfg(test)]
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FactoryResetFixtureCompletion {
    version: u8,
    nonce: String,
    backup_behavior: FactoryResetBackupBehavior,
}

#[cfg(test)]
struct FactoryResetFixtureJob {
    paths: RuntimePaths,
    nonce: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SetupConfigureRequest {
    meshtastic_host: String,
    meshtastic_port: u16,
    mesh_network_id: Option<String>,
    gateway_id: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SetupTransactionJournal {
    version: u8,
    generation: u64,
    previous_configuration: Option<Vec<u8>>,
}

/// A management-only pairing code. This request is consumed by the Agent and
/// zeroized before it can cross the Gateway supervisor boundary.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CMCloudEnrollmentRequest {
    pairing_code: String,
}

impl Drop for CMCloudEnrollmentRequest {
    fn drop(&mut self) {
        self.pairing_code.zeroize();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum CMCloudEnrollmentState {
    NotConfigured,
    CredentialsRequired,
    PendingEnrollment,
    Active,
}

/// Redacted enrollment state exposed to the authenticated management UI.
///
/// Device credentials, pairing codes, installation IDs, boot IDs, and session
/// epochs deliberately have no representation in this response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct CMCloudEnrollmentStatus {
    schema_version: u8,
    state: CMCloudEnrollmentState,
    endpoint: Option<String>,
    installation_generation: Option<u64>,
    credential_version: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
enum CMCloudAccountRole {
    Member,
    Operator,
    Admin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
enum CMCloudAccountState {
    Pending,
    Approved,
    Suspended,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
enum CMCloudStationKind {
    #[serde(rename = "cmclient")]
    CmClient,
    #[serde(rename = "mqtt_only")]
    MqttOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
enum CMCloudStationState {
    Online,
    Offline,
    Pending,
    Suspended,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CMCloudAccountProjectionTenant {
    id: String,
    name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CMCloudAccountProjectionAccount {
    issuer: String,
    subject: String,
    display_name: String,
    #[serde(default)]
    email: Option<String>,
    role: CMCloudAccountRole,
    state: CMCloudAccountState,
    mapping_freeze_epoch: u64,
    #[serde(default)]
    mapping_frozen_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CMCloudAccountProjectionStation {
    id: String,
    label: String,
    kind: CMCloudStationKind,
    state: CMCloudStationState,
    #[serde(default)]
    callsign: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CMCloudAccountProjectionAuthority {
    cmcloud: bool,
    epoch: u64,
    revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CMCloudAccountProjectionFreshness {
    projected_at: String,
    stale_after_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CMCloudAccountProjectionErrorState {
    code: String,
    since: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CMCloudAccountProjection {
    #[serde(rename = "type")]
    projection_type: String,
    schema_version: u8,
    revision: u64,
    generation: u64,
    tenant: CMCloudAccountProjectionTenant,
    account: CMCloudAccountProjectionAccount,
    stations: Vec<CMCloudAccountProjectionStation>,
    authority: CMCloudAccountProjectionAuthority,
    freshness: CMCloudAccountProjectionFreshness,
    #[serde(default)]
    error_state: Option<CMCloudAccountProjectionErrorState>,
}

impl CMCloudAccountProjection {
    fn validate(&self) -> Result<(), CMCloudAccountProjectionControlError> {
        if self.projection_type != "account_projection"
            || self.schema_version != 1
            || self.tenant.id.trim().is_empty()
            || self.tenant.name.trim().is_empty()
            || self.account.issuer.trim().is_empty()
            || self.account.subject.trim().is_empty()
            || self.account.display_name.trim().is_empty()
            || !self.authority.cmcloud
            || self.authority.revision != self.revision
            || self.authority.epoch != self.account.mapping_freeze_epoch
            || self.freshness.stale_after_ms == 0
            || self
                .freshness
                .projected_at
                .parse::<chrono::DateTime<chrono::Utc>>()
                .is_err()
            || self.error_state.is_some()
        {
            return Err(CMCloudAccountProjectionControlError::Unavailable);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CMCloudEnrollmentControlError {
    InvalidInput,
    NotConfigured,
    SetupRequired,
    InProgress,
    Unavailable,
    Enrollment(cmcloud_enrollment::CMCloudEnrollmentError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CMCloudAccountProjectionControlError {
    Unavailable,
    Ambiguous,
    Stale,
}

impl CMCloudAccountProjectionControlError {
    const fn code(self) -> &'static str {
        match self {
            Self::Unavailable => "ACCOUNT_PROJECTION_UNAVAILABLE",
            Self::Ambiguous => "ACCOUNT_PROJECTION_AMBIGUOUS",
            Self::Stale => "ACCOUNT_PROJECTION_STALE",
        }
    }

    const fn status_code(self) -> StatusCode {
        match self {
            Self::Unavailable | Self::Stale => StatusCode::SERVICE_UNAVAILABLE,
            Self::Ambiguous => StatusCode::CONFLICT,
        }
    }
}

impl CMCloudEnrollmentControlError {
    const fn code(self) -> &'static str {
        match self {
            Self::InvalidInput => "CMCLOUD_ENROLLMENT_REQUEST_INVALID",
            Self::NotConfigured => "CMCLOUD_ENROLLMENT_NOT_CONFIGURED",
            Self::SetupRequired => "CMCLOUD_ENROLLMENT_SETUP_REQUIRED",
            Self::InProgress => "CMCLOUD_ENROLLMENT_IN_PROGRESS",
            Self::Unavailable => "CMCLOUD_ENROLLMENT_UNAVAILABLE",
            Self::Enrollment(error) => error.code(),
        }
    }

    const fn status_code(self) -> StatusCode {
        match self {
            Self::InvalidInput => StatusCode::BAD_REQUEST,
            Self::NotConfigured | Self::SetupRequired | Self::InProgress => StatusCode::CONFLICT,
            Self::Unavailable
            | Self::Enrollment(cmcloud_enrollment::CMCloudEnrollmentError::SecretStore)
            | Self::Enrollment(cmcloud_enrollment::CMCloudEnrollmentError::Transport) => {
                StatusCode::SERVICE_UNAVAILABLE
            }
            Self::Enrollment(
                cmcloud_enrollment::CMCloudEnrollmentError::Protocol
                | cmcloud_enrollment::CMCloudEnrollmentError::Rejected
                | cmcloud_enrollment::CMCloudEnrollmentError::StaleEnrollment,
            ) => StatusCode::BAD_GATEWAY,
            Self::Enrollment(cmcloud_enrollment::CMCloudEnrollmentError::RecoveryRequired) => {
                StatusCode::CONFLICT
            }
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SetupDiscoveryResponse {
    schema_version: u8,
    candidates: Vec<MeshtasticCandidate>,
    cmcloud_url: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SetupApplyError {
    Setup(SetupError),
    InvalidInput,
    Cancelled,
    EndpointUnreachable,
    CallMeshCredentialRejected,
    CallMeshUnavailable,
    SupervisorUnavailable,
    ConfigWriteFailed,
}

impl From<SetupError> for SetupApplyError {
    fn from(error: SetupError) -> Self {
        Self::Setup(error)
    }
}

#[derive(Clone, Default)]
struct SetupCancellationToken {
    cancelled: Arc<AtomicBool>,
}

impl SetupCancellationToken {
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

struct SetupRequestCancellationGuard {
    cancellation: SetupCancellationToken,
    armed: bool,
}

impl SetupRequestCancellationGuard {
    fn new(cancellation: SetupCancellationToken) -> Self {
        Self {
            cancellation,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for SetupRequestCancellationGuard {
    fn drop(&mut self) {
        if self.armed {
            self.cancellation.cancel();
        }
    }
}

#[derive(Clone, Copy)]
enum SetupRollbackState {
    None,
    JournalOnly,
    Validating,
    Persistent,
}

struct SetupPendingCommit {
    controller: Arc<AgentController>,
    status: SetupStatus,
    previous_configuration: Option<Vec<u8>>,
    committed: bool,
}

impl SetupPendingCommit {
    fn commit(self) -> Result<SetupStatus, SetupApplyError> {
        self.commit_with(|controller, status| controller.finish_setup_commit(status))
    }

    fn commit_with(
        mut self,
        finish: impl FnOnce(&AgentController, &SetupStatus) -> Result<(), SetupApplyError>,
    ) -> Result<SetupStatus, SetupApplyError> {
        if let Err(error) = finish(&self.controller, &self.status) {
            let error = self.controller.rollback_setup_error(
                self.previous_configuration.as_deref(),
                true,
                error,
            );
            self.committed = true;
            return Err(error);
        }
        self.committed = true;
        Ok(self.status.clone())
    }
}

impl Drop for SetupPendingCommit {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        if self.controller.rollback_setup_error(
            self.previous_configuration.as_deref(),
            true,
            SetupApplyError::Cancelled,
        ) != SetupApplyError::Cancelled
        {
            self.controller
                .remember_error_code("SETUP_TRANSACTION_ROLLBACK_FAILED");
        }
    }
}

type SetupApplyHandler = Arc<
    dyn Fn(
            SetupConfigureRequest,
            SetupCancellationToken,
        ) -> Result<SetupPendingCommit, SetupApplyError>
        + Send
        + Sync,
>;

type SetupResetHandler = Arc<dyn Fn() -> Result<SetupStatus, SetupApplyError> + Send + Sync>;

type CMCloudEnrollmentHandler = Arc<
    dyn Fn(
            CMCloudEnrollmentRequest,
        ) -> Result<CMCloudEnrollmentStatus, CMCloudEnrollmentControlError>
        + Send
        + Sync,
>;

type CMCloudEnrollmentStatusHandler =
    Arc<dyn Fn() -> Result<CMCloudEnrollmentStatus, CMCloudEnrollmentControlError> + Send + Sync>;

type CMCloudAccountProjectionHandler = Arc<
    dyn Fn() -> Result<CMCloudAccountProjection, CMCloudAccountProjectionControlError>
        + Send
        + Sync,
>;

const CMCLOUD_PRODUCTION_URL: &str = "wss://cmcloud.tmmarc.org/agent/v1";

struct AgentWebState {
    updates: Arc<AgentUpdateService>,
    setup: Arc<SetupStore>,
    setup_gate_required: bool,
    setup_events: Arc<AgentEventHub>,
    lifecycle: Mutex<AgentLifecycleStatus>,
    lifecycle_events: Arc<AgentEventHub>,
    management_setup_state: Mutex<Option<ManagementSetupState>>,
    management_access: Option<Arc<ManagementAccessController>>,
    setup_apply: Mutex<Option<SetupApplyHandler>>,
    operational_reset: Mutex<Option<SetupResetHandler>>,
    cmcloud_enrollment: Mutex<Option<CMCloudEnrollmentHandler>>,
    cmcloud_enrollment_status: Mutex<Option<CMCloudEnrollmentStatusHandler>>,
    cmcloud_account_projection: Mutex<Option<CMCloudAccountProjectionHandler>>,
    migrated_meshtastic: Option<MeshtasticCandidate>,
    discovery_gate: Mutex<()>,
    audit: Mutex<VecDeque<ManagementAuditEntry>>,
    runtime_log: Option<StructuredLogSink>,
}

impl AgentWebState {
    #[cfg(test)]
    fn new(
        updates: Arc<AgentUpdateService>,
        setup: Arc<SetupStore>,
        setup_gate_required: bool,
        lifecycle: AgentLifecycleStatus,
    ) -> Result<Self, ControlError> {
        Self::new_with_log(
            updates,
            setup,
            setup_gate_required,
            lifecycle,
            None,
            None,
            None,
        )
    }

    fn new_with_log(
        updates: Arc<AgentUpdateService>,
        setup: Arc<SetupStore>,
        setup_gate_required: bool,
        lifecycle: AgentLifecycleStatus,
        runtime_log: Option<StructuredLogSink>,
        management_access: Option<Arc<ManagementAccessController>>,
        migrated_meshtastic: Option<MeshtasticCandidate>,
    ) -> Result<Self, ControlError> {
        let setup_events = Arc::new(AgentEventHub::new_with_log("setup", runtime_log.clone()));
        setup_events
            .publish(
                "setup.status",
                &setup.status().map_err(|_| ControlError::CommandFailed)?,
            )
            .map_err(|_| ControlError::CommandFailed)?;
        let lifecycle_events = Arc::new(AgentEventHub::new_with_log(
            "lifecycle",
            runtime_log.clone(),
        ));
        lifecycle_events
            .publish("lifecycle.status", &lifecycle)
            .map_err(|_| ControlError::CommandFailed)?;
        Ok(Self {
            updates,
            setup,
            setup_gate_required,
            setup_events,
            lifecycle: Mutex::new(lifecycle),
            lifecycle_events,
            management_setup_state: Mutex::new(None),
            management_access,
            setup_apply: Mutex::new(None),
            operational_reset: Mutex::new(None),
            cmcloud_enrollment: Mutex::new(None),
            cmcloud_enrollment_status: Mutex::new(None),
            cmcloud_account_projection: Mutex::new(None),
            migrated_meshtastic,
            discovery_gate: Mutex::new(()),
            audit: Mutex::new(VecDeque::new()),
            runtime_log,
        })
    }

    fn install_setup_apply(&self, handler: SetupApplyHandler) -> Result<(), ControlError> {
        *self
            .setup_apply
            .lock()
            .map_err(|_| ControlError::CommandFailed)? = Some(handler);
        Ok(())
    }

    fn install_operational_reset(&self, handler: SetupResetHandler) -> Result<(), ControlError> {
        *self
            .operational_reset
            .lock()
            .map_err(|_| ControlError::CommandFailed)? = Some(handler);
        Ok(())
    }

    fn install_cmcloud_enrollment(
        &self,
        enrollment: CMCloudEnrollmentHandler,
        status: CMCloudEnrollmentStatusHandler,
    ) -> Result<(), ControlError> {
        *self
            .cmcloud_enrollment
            .lock()
            .map_err(|_| ControlError::CommandFailed)? = Some(enrollment);
        *self
            .cmcloud_enrollment_status
            .lock()
            .map_err(|_| ControlError::CommandFailed)? = Some(status);
        Ok(())
    }

    fn install_cmcloud_account_projection(
        &self,
        handler: CMCloudAccountProjectionHandler,
    ) -> Result<(), ControlError> {
        *self
            .cmcloud_account_projection
            .lock()
            .map_err(|_| ControlError::CommandFailed)? = Some(handler);
        Ok(())
    }

    fn reset_setup(&self) -> Result<SetupStatus, SetupApplyError> {
        let current = self.setup.status()?;
        if !self.setup_gate_required
            || !current.setup_required
            || matches!(current.phase, SetupPhase::Validating)
        {
            return Err(SetupError::TransitionInvalid.into());
        }
        let status = self.setup.reset()?;
        self.publish_setup_status(&status)
            .map_err(|_| SetupApplyError::ConfigWriteFailed)?;
        self.record_audit("setup_reset", "allowed", "SETUP_RESET_COMPLETED");
        self.record_audit("setup_generation", "changed", "SETUP_GENERATION_CHANGED");
        Ok(status)
    }

    fn reset_operational(&self) -> Result<SetupStatus, SetupApplyError> {
        let handler = self
            .operational_reset
            .lock()
            .map_err(|_| SetupApplyError::SupervisorUnavailable)?
            .clone()
            .ok_or(SetupApplyError::SupervisorUnavailable)?;
        handler()
    }

    fn attach_management_setup_state(
        &self,
        state: ManagementSetupState,
    ) -> Result<(), ControlError> {
        let status = self
            .setup
            .status()
            .map_err(|_| ControlError::CommandFailed)?;
        let generation = self
            .setup
            .generation()
            .map_err(|_| ControlError::CommandFailed)?
            .generation();
        state.set(
            generation,
            self.setup_gate_required && status.setup_required,
        );
        *self
            .management_setup_state
            .lock()
            .map_err(|_| ControlError::CommandFailed)? = Some(state);
        Ok(())
    }

    fn publish_setup_status(
        &self,
        status: &cmclient_agent_core::setup::SetupStatus,
    ) -> Result<(), ControlError> {
        let generation = self
            .setup
            .generation()
            .map_err(|_| ControlError::CommandFailed)?
            .generation();
        if let Some(state) = self
            .management_setup_state
            .lock()
            .map_err(|_| ControlError::CommandFailed)?
            .as_ref()
        {
            state.set(
                generation,
                self.setup_gate_required && status.setup_required,
            );
        }
        self.setup_events
            .publish("setup.status", status)
            .map(|_| ())
            .map_err(|_| ControlError::CommandFailed)
    }

    fn update_lifecycle(&self, next: AgentLifecycleStatus) -> Result<(), ControlError> {
        let mut current = self
            .lifecycle
            .lock()
            .map_err(|_| ControlError::CommandFailed)?;
        if *current == next {
            return Ok(());
        }
        let publish = lifecycle_event_changed(&current, &next);
        *current = next.clone();
        drop(current);
        if publish {
            self.lifecycle_events
                .publish("lifecycle.status", &next)
                .map(|_| ())
                .map_err(|_| ControlError::CommandFailed)?;
        }
        Ok(())
    }

    fn record_audit(&self, action: &'static str, outcome: &'static str, log_code: &'static str) {
        let occurred_at_unix_seconds = Utc::now().timestamp().max(0) as u64;
        if let Ok(mut audit) = self.audit.lock() {
            audit.push_back(ManagementAuditEntry {
                occurred_at_unix_seconds,
                action,
                outcome,
            });
            while audit.len() > AGENT_AUDIT_CAPACITY {
                audit.pop_front();
            }
        }
        if let Some(access) = &self.management_access {
            access.audit(occurred_at_unix_seconds, action, outcome);
        }
        if let Some(runtime_log) = &self.runtime_log {
            let _ = runtime_log.write_code(LogLevel::Info, log_code);
        }
    }

    #[cfg(test)]
    fn audit_snapshot(&self) -> Vec<ManagementAuditEntry> {
        self.audit
            .lock()
            .map_or_else(|_| Vec::new(), |audit| audit.iter().cloned().collect())
    }
}

fn lifecycle_event_changed(current: &AgentLifecycleStatus, next: &AgentLifecycleStatus) -> bool {
    current.schema_version != next.schema_version
        || current.agent != next.agent
        || current.gateway != next.gateway
        || current.management_web != next.management_web
        || current.management_web_url != next.management_web_url
        || current.latest_error_code != next.latest_error_code
}

fn agent_web_router(state: Arc<AgentWebState>) -> Router {
    Router::new()
        .route("/api/v1/setup/status", get(management_setup_status))
        .route("/api/v1/setup/discovery", get(management_setup_discovery))
        .route("/api/v1/setup/configure", post(management_setup_configure))
        .route("/api/v1/setup/terms", post(management_setup_terms))
        .route("/api/v1/setup/reset", post(management_setup_reset))
        .route(
            "/api/v1/reset/operational",
            post(management_operational_reset),
        )
        .route("/api/v1/setup/events", get(management_setup_events))
        .route(
            "/api/v1/cmcloud/enrollment",
            get(management_cmcloud_enrollment_status).post(management_cmcloud_enrollment),
        )
        .route(
            "/api/v1/cmcloud/account-projection",
            get(management_cmcloud_account_projection),
        )
        .route("/api/v1/lifecycle/status", get(management_lifecycle_status))
        .route("/api/v1/lifecycle/events", get(management_lifecycle_events))
        .route("/api/v1/updates", get(management_update_status))
        .route("/api/v1/updates/events", get(management_update_events))
        .route(
            "/api/v1/control/{*path}",
            any(management_control_route_not_found),
        )
        .route("/api/v1/control", any(management_control_route_not_found))
        .with_state(state)
}

async fn management_setup_discovery(State(state): State<Arc<AgentWebState>>) -> Response {
    let status = match state.setup.status() {
        Ok(status) => status,
        Err(error) => return setup_error_response(error),
    };
    if !state.setup_gate_required || !matches!(status.phase, SetupPhase::CredentialsRequired) {
        return setup_error_response(SetupError::TransitionInvalid);
    }
    let migrated = state.migrated_meshtastic.clone();
    let operation = tokio::task::spawn_blocking(move || {
        let _guard = state
            .discovery_gate
            .try_lock()
            .map_err(|_| DiscoveryError::Busy)?;
        let mdns = discover_mdns(MAX_MDNS_TIMEOUT, MAX_DISCOVERY_CANDIDATES)?;
        Ok::<_, DiscoveryError>(ordered_candidates(migrated, mdns, None))
    })
    .await;
    match operation {
        Ok(Ok(candidates)) => (
            StatusCode::OK,
            Json(SetupDiscoveryResponse {
                schema_version: 1,
                candidates,
                cmcloud_url: CMCLOUD_PRODUCTION_URL,
            }),
        )
            .into_response(),
        Ok(Err(error)) => setup_discovery_error_response(error),
        Err(_) => setup_discovery_error_response(DiscoveryError::MdnsFailed),
    }
}

async fn management_setup_configure(
    State(state): State<Arc<AgentWebState>>,
    request: Result<Json<SetupConfigureRequest>, JsonRejection>,
) -> Response {
    let Json(request) = match request {
        Ok(request) => request,
        Err(_) => return setup_request_invalid_response(),
    };
    let handler = match state.setup_apply.lock() {
        Ok(handler) => handler.clone(),
        Err(_) => return management_control_failed_response(),
    };
    let Some(handler) = handler else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"code": "SETUP_TRANSACTION_UNAVAILABLE"})),
        )
            .into_response();
    };
    let cancellation = SetupCancellationToken::default();
    let mut cancellation_guard = SetupRequestCancellationGuard::new(cancellation.clone());
    let operation = tokio::task::spawn_blocking(move || handler(request, cancellation)).await;
    match operation {
        Ok(Ok(pending)) => match pending.commit() {
            Ok(status) => {
                cancellation_guard.disarm();
                (StatusCode::OK, Json(status)).into_response()
            }
            Err(error) => setup_apply_error_response(error),
        },
        Ok(Err(error)) => setup_apply_error_response(error),
        Err(_) => management_control_failed_response(),
    }
}

async fn management_cmcloud_enrollment_status(State(state): State<Arc<AgentWebState>>) -> Response {
    let handler = match state.cmcloud_enrollment_status.lock() {
        Ok(handler) => handler.clone(),
        Err(_) => return management_control_failed_response(),
    };
    let Some(handler) = handler else {
        return cmcloud_enrollment_error_response(CMCloudEnrollmentControlError::Unavailable);
    };
    match tokio::task::spawn_blocking(move || handler()).await {
        Ok(Ok(status)) => (StatusCode::OK, Json(status)).into_response(),
        Ok(Err(error)) => cmcloud_enrollment_error_response(error),
        Err(_) => cmcloud_enrollment_error_response(CMCloudEnrollmentControlError::Unavailable),
    }
}

async fn management_cmcloud_enrollment(
    State(state): State<Arc<AgentWebState>>,
    request: Result<Json<CMCloudEnrollmentRequest>, JsonRejection>,
) -> Response {
    let Json(request) = match request {
        Ok(request) if valid_cmcloud_pairing_code(&request.pairing_code) => request,
        Ok(_) | Err(_) => {
            return cmcloud_enrollment_error_response(CMCloudEnrollmentControlError::InvalidInput);
        }
    };
    let handler = match state.cmcloud_enrollment.lock() {
        Ok(handler) => handler.clone(),
        Err(_) => return management_control_failed_response(),
    };
    let Some(handler) = handler else {
        return cmcloud_enrollment_error_response(CMCloudEnrollmentControlError::Unavailable);
    };
    match tokio::task::spawn_blocking(move || handler(request)).await {
        Ok(Ok(status)) => (StatusCode::OK, Json(status)).into_response(),
        Ok(Err(error)) => cmcloud_enrollment_error_response(error),
        Err(_) => cmcloud_enrollment_error_response(CMCloudEnrollmentControlError::Unavailable),
    }
}

async fn management_cmcloud_account_projection(
    State(state): State<Arc<AgentWebState>>,
) -> Response {
    let handler = match state.cmcloud_account_projection.lock() {
        Ok(handler) => handler.clone(),
        Err(_) => return management_control_failed_response(),
    };
    let Some(handler) = handler else {
        return cmcloud_account_projection_error_response(
            CMCloudAccountProjectionControlError::Unavailable,
        );
    };
    match tokio::task::spawn_blocking(move || handler()).await {
        Ok(Ok(projection)) => (StatusCode::OK, Json(projection)).into_response(),
        Ok(Err(error)) => cmcloud_account_projection_error_response(error),
        Err(_) => cmcloud_account_projection_error_response(
            CMCloudAccountProjectionControlError::Unavailable,
        ),
    }
}

fn valid_cmcloud_pairing_code(value: &str) -> bool {
    (16..=512).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn cmcloud_gateway_environment(
    endpoint: &str,
    credential: &CMCloudActiveDeviceCredential,
) -> BTreeMap<String, String> {
    debug_assert_eq!(credential.identity().endpoint(), endpoint);
    BTreeMap::from([
        (
            String::from(CMCLOUD_MODE_ENVIRONMENT_NAME),
            String::from("required"),
        ),
        (
            String::from(CMCLOUD_URL_ENVIRONMENT_NAME),
            endpoint.to_owned(),
        ),
        (
            String::from(CMCLOUD_INSTALLATION_ID_ENVIRONMENT_NAME),
            credential.identity().installation_id().to_owned(),
        ),
        (
            String::from(CMCLOUD_INSTALLATION_GENERATION_ENVIRONMENT_NAME),
            credential.identity().installation_generation().to_string(),
        ),
        (
            String::from(CMCLOUD_CREDENTIAL_VERSION_ENVIRONMENT_NAME),
            credential.identity().credential_version().to_string(),
        ),
        (String::from("CMCLIENT_APRS_ENABLED"), String::from("false")),
        (
            String::from(PROXY_ENABLED_ENVIRONMENT_NAME),
            String::from("false"),
        ),
    ])
}

fn cmcloud_enrollment_error_response(error: CMCloudEnrollmentControlError) -> Response {
    (
        error.status_code(),
        Json(serde_json::json!({"code": error.code()})),
    )
        .into_response()
}

fn cmcloud_account_projection_error_response(
    error: CMCloudAccountProjectionControlError,
) -> Response {
    (
        error.status_code(),
        Json(serde_json::json!({"code": error.code()})),
    )
        .into_response()
}

fn validate_setup_request(request: &SetupConfigureRequest) -> Result<(), SetupApplyError> {
    let host = request.meshtastic_host.trim();
    if host.is_empty()
        || request.meshtastic_host != host
        || host.len() > 255
        || host.contains(char::is_whitespace)
        || host.contains('/')
        || host.contains(['"', '\\', '\r', '\n'])
        || request.meshtastic_port != 4_403
    {
        return Err(SetupApplyError::InvalidInput);
    }
    for value in [
        request.mesh_network_id.as_deref(),
        request.gateway_id.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        let value = value.trim();
        if value.is_empty()
            || value.len() > 128
            || value.chars().any(char::is_control)
            || value.contains(['"', '\\'])
        {
            return Err(SetupApplyError::InvalidInput);
        }
    }
    Ok(())
}

fn write_setup_configuration(
    path: &Path,
    host: &str,
    port: u16,
    mesh_network_id: &str,
    gateway_id: &str,
) -> Result<(), SetupApplyError> {
    let contents = format!(
        "[agent]\nmanagement_web_enabled = true\n\n[cmcloud]\nagent_websocket_url = \"{CMCLOUD_PRODUCTION_URL}\"\n\n[meshtastic]\ntransport = \"tcp\"\nmesh_network_id = \"{mesh_network_id}\"\ngateway_id = \"{gateway_id}\"\ntcp_host = \"{host}\"\ntcp_port = {port}\n"
    );
    write_atomic_configuration(path, contents.as_bytes())
}

fn write_atomic_configuration(path: &Path, contents: &[u8]) -> Result<(), SetupApplyError> {
    let parent = path.parent().ok_or(SetupApplyError::ConfigWriteFailed)?;
    fs::create_dir_all(parent).map_err(|_| SetupApplyError::ConfigWriteFailed)?;
    let mut output = AtomicWriteFile::open(path).map_err(|_| SetupApplyError::ConfigWriteFailed)?;
    output
        .write_all(contents)
        .map_err(|_| SetupApplyError::ConfigWriteFailed)?;
    output
        .commit()
        .map_err(|_| SetupApplyError::ConfigWriteFailed)
}

fn capture_setup_configuration(path: &Path) -> Result<Option<Vec<u8>>, SetupApplyError> {
    match fs::read(path) {
        Ok(contents) if contents.len() <= MAX_SETUP_CONFIGURATION_BYTES => Ok(Some(contents)),
        Ok(_) => Err(SetupApplyError::ConfigWriteFailed),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(_) => Err(SetupApplyError::ConfigWriteFailed),
    }
}

fn restore_setup_configuration(
    path: &Path,
    previous: Option<&[u8]>,
) -> Result<(), SetupApplyError> {
    match previous {
        Some(contents) => write_atomic_configuration(path, contents),
        None => match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(_) => Err(SetupApplyError::ConfigWriteFailed),
        },
    }
}

fn setup_transaction_file(paths: &RuntimePaths) -> PathBuf {
    paths.state_dir().join(SETUP_TRANSACTION_FILE_NAME)
}

fn write_setup_transaction(
    path: &Path,
    generation: u64,
    previous_configuration: Option<&[u8]>,
) -> Result<(), SetupApplyError> {
    if generation == 0
        || previous_configuration
            .is_some_and(|contents| contents.len() > MAX_SETUP_CONFIGURATION_BYTES)
    {
        return Err(SetupApplyError::ConfigWriteFailed);
    }
    let document = SetupTransactionJournal {
        version: SETUP_TRANSACTION_VERSION,
        generation,
        previous_configuration: previous_configuration.map(<[u8]>::to_vec),
    };
    let bytes = serde_json::to_vec(&document).map_err(|_| SetupApplyError::ConfigWriteFailed)?;
    write_atomic_configuration(path, &bytes)
}

fn read_setup_transaction(path: &Path) -> Result<Option<SetupTransactionJournal>, SetupApplyError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(SetupApplyError::ConfigWriteFailed),
    };
    if bytes.len() > MAX_SETUP_CONFIGURATION_BYTES.saturating_mul(4) {
        return Err(SetupApplyError::ConfigWriteFailed);
    }
    let document: SetupTransactionJournal =
        serde_json::from_slice(&bytes).map_err(|_| SetupApplyError::ConfigWriteFailed)?;
    if document.version != SETUP_TRANSACTION_VERSION
        || document.generation == 0
        || document
            .previous_configuration
            .as_ref()
            .is_some_and(|contents| contents.len() > MAX_SETUP_CONFIGURATION_BYTES)
    {
        return Err(SetupApplyError::ConfigWriteFailed);
    }
    Ok(Some(document))
}

fn remove_setup_transaction(path: &Path) -> Result<(), SetupApplyError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(SetupApplyError::ConfigWriteFailed),
    }
}

fn reset_transaction_file(paths: &RuntimePaths) -> PathBuf {
    paths.state_dir().join(RESET_TRANSACTION_FILE_NAME)
}

fn reset_completion_file(paths: &RuntimePaths) -> PathBuf {
    paths.state_dir().join("reset-completed.json")
}

fn write_reset_transaction(
    paths: &RuntimePaths,
    kind: ResetKind,
    target_generation: u64,
) -> Result<(), SetupApplyError> {
    if target_generation == 0 {
        return Err(SetupApplyError::ConfigWriteFailed);
    }
    let bytes = serde_json::to_vec(&ResetTransactionJournal {
        version: RESET_TRANSACTION_VERSION,
        kind,
        target_generation,
    })
    .map_err(|_| SetupApplyError::ConfigWriteFailed)?;
    write_atomic_configuration(&reset_transaction_file(paths), &bytes)
}

fn read_reset_transaction(
    paths: &RuntimePaths,
) -> Result<Option<ResetTransactionJournal>, SetupApplyError> {
    let path = reset_transaction_file(paths);
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(SetupApplyError::ConfigWriteFailed),
    };
    if bytes.len() > MAX_SETUP_CONFIGURATION_BYTES {
        return Err(SetupApplyError::ConfigWriteFailed);
    }
    let journal: ResetTransactionJournal =
        serde_json::from_slice(&bytes).map_err(|_| SetupApplyError::ConfigWriteFailed)?;
    if journal.version != RESET_TRANSACTION_VERSION || journal.target_generation == 0 {
        return Err(SetupApplyError::ConfigWriteFailed);
    }
    Ok(Some(journal))
}

fn remove_reset_transaction(paths: &RuntimePaths) -> Result<(), SetupApplyError> {
    match fs::remove_file(reset_transaction_file(paths)) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(SetupApplyError::ConfigWriteFailed),
    }
}

fn write_reset_completion_marker(
    paths: &RuntimePaths,
    kind: ResetKind,
) -> Result<(), SetupApplyError> {
    let bytes = serde_json::to_vec(&ResetCompletionMarker {
        version: RESET_TRANSACTION_VERSION,
        kind,
    })
    .map_err(|_| SetupApplyError::ConfigWriteFailed)?;
    write_atomic_configuration(&reset_completion_file(paths), &bytes)
}

fn reset_completion_marker_exists(paths: &RuntimePaths) -> Result<bool, SetupApplyError> {
    let bytes = match fs::read(reset_completion_file(paths)) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(_) => return Err(SetupApplyError::ConfigWriteFailed),
    };
    if bytes.len() > MAX_SETUP_CONFIGURATION_BYTES {
        return Err(SetupApplyError::ConfigWriteFailed);
    }
    let marker: ResetCompletionMarker =
        serde_json::from_slice(&bytes).map_err(|_| SetupApplyError::ConfigWriteFailed)?;
    if marker.version != RESET_TRANSACTION_VERSION {
        return Err(SetupApplyError::ConfigWriteFailed);
    }
    Ok(true)
}

fn validate_reset_paths(paths: &RuntimePaths, config_file: &Path) -> Result<(), SetupApplyError> {
    if !paths.root_dir().is_absolute()
        || paths
            .root_dir()
            .file_name()
            .is_none_or(|name| name != ".cmclient")
        || config_file != paths.config_file()
    {
        return Err(SetupApplyError::ConfigWriteFailed);
    }
    Ok(())
}

fn complete_operational_reset(
    paths: &RuntimePaths,
    config_file: &Path,
    secrets: &AgentSecretStore,
    setup: &SetupStore,
    target_generation: u64,
) -> Result<SetupStatus, SetupApplyError> {
    validate_reset_paths(paths, config_file)?;
    let status = setup.reset_to_generation(target_generation)?;
    secrets
        .clear_for_reset()
        .map_err(|_| SetupApplyError::ConfigWriteFailed)?;
    restore_setup_configuration(config_file, None)?;
    remove_setup_transaction(&setup_transaction_file(paths))?;
    write_reset_completion_marker(paths, ResetKind::Operational)?;
    Ok(status)
}

fn recover_interrupted_reset(paths: &RuntimePaths) -> Result<bool, SetupApplyError> {
    let Some(journal) = read_reset_transaction(paths)? else {
        return reset_completion_marker_exists(paths);
    };
    if journal.kind != ResetKind::Operational {
        return Err(SetupApplyError::ConfigWriteFailed);
    }
    let secrets = AgentSecretStore::runtime(paths.root_dir())
        .map_err(|_| SetupApplyError::ConfigWriteFailed)?;
    let setup = SetupStore::open(paths)?;
    complete_operational_reset(
        paths,
        &paths.config_file(),
        &secrets,
        &setup,
        journal.target_generation,
    )?;
    remove_reset_transaction(paths)?;
    Ok(true)
}

#[cfg(test)]
impl FactoryResetFixtureConfirmation {
    fn for_backup_behavior(backup_behavior: FactoryResetBackupBehavior) -> Self {
        let final_confirmation = match backup_behavior {
            FactoryResetBackupBehavior::RetainExisting => "retain-existing-backups",
            FactoryResetBackupBehavior::EraseAll => "erase-all-backups",
        };
        Self {
            backup_behavior,
            first_confirmation: "factory-reset-fixture",
            final_confirmation,
        }
    }

    fn validate(self) -> Result<(), SetupApplyError> {
        let expected_final_confirmation = match self.backup_behavior {
            FactoryResetBackupBehavior::RetainExisting => "retain-existing-backups",
            FactoryResetBackupBehavior::EraseAll => "erase-all-backups",
        };
        if self.first_confirmation != "factory-reset-fixture"
            || self.final_confirmation != expected_final_confirmation
        {
            return Err(SetupApplyError::InvalidInput);
        }
        Ok(())
    }
}

#[cfg(test)]
impl FactoryResetFixtureJob {
    fn create(paths: RuntimePaths) -> Result<Self, SetupApplyError> {
        validate_factory_reset_fixture_paths(&paths)?;
        ensure_runtime_directories(&paths).map_err(|_| SetupApplyError::ConfigWriteFailed)?;
        let nonce = format!(
            "fixture-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|_| SetupApplyError::ConfigWriteFailed)?
                .as_nanos()
        );
        let job = Self { paths, nonce };
        job.write_marker()?;
        job.validate()?;
        Ok(job)
    }

    fn run<F>(
        &self,
        confirmation: FactoryResetFixtureConfirmation,
        quiesce: F,
    ) -> Result<(), SetupApplyError>
    where
        F: FnMut() -> Result<(), SetupApplyError>,
    {
        confirmation.validate()?;
        self.validate()?;
        let journal = match self.read_journal()? {
            Some(journal) => {
                if journal.backup_behavior != confirmation.backup_behavior {
                    return Err(SetupApplyError::InvalidInput);
                }
                journal
            }
            None if self.completion_matches(confirmation.backup_behavior)? => return Ok(()),
            None => {
                let journal = FactoryResetFixtureJournal {
                    version: FACTORY_RESET_FIXTURE_VERSION,
                    nonce: self.nonce.clone(),
                    backup_behavior: confirmation.backup_behavior,
                    phase: FactoryResetFixturePhase::Prepared,
                };
                self.write_journal(&journal)?;
                journal
            }
        };
        self.run_journal(journal, quiesce, None)
    }

    fn recover(&self) -> Result<(), SetupApplyError> {
        self.validate()?;
        let Some(journal) = self.read_journal()? else {
            return if self.completion_matches_any()? {
                Ok(())
            } else {
                Err(SetupApplyError::ConfigWriteFailed)
            };
        };
        self.run_journal(journal, || Ok(()), None)
    }

    fn run_until<F>(
        &self,
        confirmation: FactoryResetFixtureConfirmation,
        quiesce: F,
        interrupt_after: FactoryResetFixturePhase,
    ) -> Result<(), SetupApplyError>
    where
        F: FnMut() -> Result<(), SetupApplyError>,
    {
        confirmation.validate()?;
        self.validate()?;
        let journal = FactoryResetFixtureJournal {
            version: FACTORY_RESET_FIXTURE_VERSION,
            nonce: self.nonce.clone(),
            backup_behavior: confirmation.backup_behavior,
            phase: FactoryResetFixturePhase::Prepared,
        };
        self.write_journal(&journal)?;
        self.run_journal(journal, quiesce, Some(interrupt_after))
    }

    fn run_journal<F>(
        &self,
        mut journal: FactoryResetFixtureJournal,
        mut quiesce: F,
        interrupt_after: Option<FactoryResetFixturePhase>,
    ) -> Result<(), SetupApplyError>
    where
        F: FnMut() -> Result<(), SetupApplyError>,
    {
        loop {
            if interrupt_after == Some(journal.phase) {
                return Err(SetupApplyError::Cancelled);
            }
            match journal.phase {
                FactoryResetFixturePhase::Prepared => {
                    quiesce()?;
                    journal.phase = FactoryResetFixturePhase::Quiesced;
                    self.write_journal(&journal)?;
                }
                FactoryResetFixturePhase::Quiesced => {
                    clear_factory_reset_fixture_state(&self.paths, journal.backup_behavior)?;
                    journal.phase = FactoryResetFixturePhase::MutableStateCleared;
                    self.write_journal(&journal)?;
                }
                FactoryResetFixturePhase::MutableStateCleared => {
                    ensure_runtime_directories(&self.paths)
                        .map_err(|_| SetupApplyError::ConfigWriteFailed)?;
                    let setup = SetupStore::open(&self.paths)?;
                    if setup.status()?.phase != SetupPhase::TermsRequired {
                        return Err(SetupApplyError::ConfigWriteFailed);
                    }
                    journal.phase = FactoryResetFixturePhase::RootRecreated;
                    self.write_journal(&journal)?;
                }
                FactoryResetFixturePhase::RootRecreated => {
                    self.write_completion(journal.backup_behavior)?;
                    journal.phase = FactoryResetFixturePhase::Completed;
                    self.write_journal(&journal)?;
                }
                FactoryResetFixturePhase::Completed => {
                    self.write_completion(journal.backup_behavior)?;
                    remove_factory_reset_fixture_file(&self.journal_file())?;
                    return Ok(());
                }
            }
        }
    }

    fn marker_file(&self) -> PathBuf {
        self.paths
            .root_dir()
            .join(FACTORY_RESET_FIXTURE_MARKER_FILE_NAME)
    }

    fn journal_file(&self) -> PathBuf {
        self.paths
            .root_dir()
            .join(FACTORY_RESET_FIXTURE_JOURNAL_FILE_NAME)
    }

    fn completion_file(&self) -> PathBuf {
        self.paths
            .root_dir()
            .join(FACTORY_RESET_FIXTURE_COMPLETION_FILE_NAME)
    }

    fn write_marker(&self) -> Result<(), SetupApplyError> {
        validate_factory_reset_fixture_output_file(&self.marker_file())?;
        let bytes = serde_json::to_vec(&FactoryResetFixtureMarker {
            version: FACTORY_RESET_FIXTURE_VERSION,
            nonce: self.nonce.clone(),
        })
        .map_err(|_| SetupApplyError::ConfigWriteFailed)?;
        write_atomic_configuration(&self.marker_file(), &bytes)
    }

    fn write_journal(&self, journal: &FactoryResetFixtureJournal) -> Result<(), SetupApplyError> {
        validate_factory_reset_fixture_output_file(&self.journal_file())?;
        let bytes = serde_json::to_vec(journal).map_err(|_| SetupApplyError::ConfigWriteFailed)?;
        write_atomic_configuration(&self.journal_file(), &bytes)
    }

    fn read_journal(&self) -> Result<Option<FactoryResetFixtureJournal>, SetupApplyError> {
        read_factory_reset_fixture_document(&self.journal_file()).and_then(|document| {
            let Some(bytes) = document else {
                return Ok(None);
            };
            let journal = serde_json::from_slice::<FactoryResetFixtureJournal>(&bytes)
                .map_err(|_| SetupApplyError::ConfigWriteFailed)?;
            if journal.version != FACTORY_RESET_FIXTURE_VERSION || journal.nonce != self.nonce {
                return Err(SetupApplyError::ConfigWriteFailed);
            }
            Ok(Some(journal))
        })
    }

    fn write_completion(
        &self,
        backup_behavior: FactoryResetBackupBehavior,
    ) -> Result<(), SetupApplyError> {
        validate_factory_reset_fixture_output_file(&self.completion_file())?;
        let bytes = serde_json::to_vec(&FactoryResetFixtureCompletion {
            version: FACTORY_RESET_FIXTURE_VERSION,
            nonce: self.nonce.clone(),
            backup_behavior,
        })
        .map_err(|_| SetupApplyError::ConfigWriteFailed)?;
        write_atomic_configuration(&self.completion_file(), &bytes)
    }

    fn completion_matches(
        &self,
        backup_behavior: FactoryResetBackupBehavior,
    ) -> Result<bool, SetupApplyError> {
        let Some(bytes) = read_factory_reset_fixture_document(&self.completion_file())? else {
            return Ok(false);
        };
        let completion = serde_json::from_slice::<FactoryResetFixtureCompletion>(&bytes)
            .map_err(|_| SetupApplyError::ConfigWriteFailed)?;
        Ok(completion.version == FACTORY_RESET_FIXTURE_VERSION
            && completion.nonce == self.nonce
            && completion.backup_behavior == backup_behavior)
    }

    fn completion_matches_any(&self) -> Result<bool, SetupApplyError> {
        let Some(bytes) = read_factory_reset_fixture_document(&self.completion_file())? else {
            return Ok(false);
        };
        let completion = serde_json::from_slice::<FactoryResetFixtureCompletion>(&bytes)
            .map_err(|_| SetupApplyError::ConfigWriteFailed)?;
        Ok(completion.version == FACTORY_RESET_FIXTURE_VERSION && completion.nonce == self.nonce)
    }

    fn validate(&self) -> Result<(), SetupApplyError> {
        validate_factory_reset_fixture_paths(&self.paths)?;
        let marker_path = self.marker_file();
        let Some(bytes) = read_factory_reset_fixture_document(&marker_path)? else {
            return Err(SetupApplyError::ConfigWriteFailed);
        };
        let marker = serde_json::from_slice::<FactoryResetFixtureMarker>(&bytes)
            .map_err(|_| SetupApplyError::ConfigWriteFailed)?;
        if marker.version != FACTORY_RESET_FIXTURE_VERSION || marker.nonce != self.nonce {
            return Err(SetupApplyError::ConfigWriteFailed);
        }
        Ok(())
    }
}

#[cfg(test)]
fn validate_factory_reset_fixture_paths(paths: &RuntimePaths) -> Result<(), SetupApplyError> {
    let root = paths.root_dir();
    if !root.is_absolute()
        || root.file_name().is_none_or(|name| name != ".cmclient")
        || paths.config_dir != *root
        || paths.cache_dir != root.join("cache")
        || paths.log_dir != root.join("logs")
    {
        return Err(SetupApplyError::ConfigWriteFailed);
    }
    let parent = root.parent().ok_or(SetupApplyError::ConfigWriteFailed)?;
    let parent_name = parent
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(SetupApplyError::ConfigWriteFailed)?;
    if !parent_name.starts_with("cmclient-factory-reset-fixture-") {
        return Err(SetupApplyError::ConfigWriteFailed);
    }
    let metadata = fs::symlink_metadata(root).map_err(|_| SetupApplyError::ConfigWriteFailed)?;
    if !metadata.file_type().is_dir() || factory_reset_fixture_is_reparse_point(&metadata) {
        return Err(SetupApplyError::ConfigWriteFailed);
    }
    let canonical_root = fs::canonicalize(root).map_err(|_| SetupApplyError::ConfigWriteFailed)?;
    let canonical_parent =
        fs::canonicalize(parent).map_err(|_| SetupApplyError::ConfigWriteFailed)?;
    let canonical_temp =
        fs::canonicalize(std::env::temp_dir()).map_err(|_| SetupApplyError::ConfigWriteFailed)?;
    if canonical_root.parent() != Some(canonical_parent.as_path())
        || !canonical_parent.starts_with(canonical_temp)
    {
        return Err(SetupApplyError::ConfigWriteFailed);
    }
    Ok(())
}

#[cfg(all(test, target_os = "windows"))]
fn factory_reset_fixture_is_reparse_point(metadata: &fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;

    metadata.file_attributes() & 0x0400 != 0
}

#[cfg(all(test, not(target_os = "windows")))]
fn factory_reset_fixture_is_reparse_point(metadata: &fs::Metadata) -> bool {
    metadata.file_type().is_symlink()
}

#[cfg(test)]
fn read_factory_reset_fixture_document(path: &Path) -> Result<Option<Vec<u8>>, SetupApplyError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(SetupApplyError::ConfigWriteFailed),
    };
    if !metadata.file_type().is_file() || factory_reset_fixture_is_reparse_point(&metadata) {
        return Err(SetupApplyError::ConfigWriteFailed);
    }
    let bytes = fs::read(path).map_err(|_| SetupApplyError::ConfigWriteFailed)?;
    if bytes.len() > MAX_SETUP_CONFIGURATION_BYTES {
        return Err(SetupApplyError::ConfigWriteFailed);
    }
    Ok(Some(bytes))
}

#[cfg(test)]
fn validate_factory_reset_fixture_output_file(path: &Path) -> Result<(), SetupApplyError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(_) => return Err(SetupApplyError::ConfigWriteFailed),
    };
    if !metadata.file_type().is_file() || factory_reset_fixture_is_reparse_point(&metadata) {
        return Err(SetupApplyError::ConfigWriteFailed);
    }
    Ok(())
}

#[cfg(test)]
fn clear_factory_reset_fixture_state(
    paths: &RuntimePaths,
    backup_behavior: FactoryResetBackupBehavior,
) -> Result<(), SetupApplyError> {
    validate_factory_reset_fixture_paths(paths)?;
    for name in [
        "config.toml",
        "secrets.json",
        "cmclient.db",
        "cmclient.db-shm",
        "cmclient.db-wal",
    ] {
        remove_factory_reset_fixture_file(&paths.root_dir().join(name))?;
    }
    for name in ["cache", "logs", "run", "state", "updates"] {
        remove_factory_reset_fixture_directory(&paths.root_dir().join(name))?;
    }
    if backup_behavior == FactoryResetBackupBehavior::EraseAll {
        remove_factory_reset_fixture_directory(&paths.backups_dir())?;
    }
    Ok(())
}

#[cfg(test)]
fn remove_factory_reset_fixture_file(path: &Path) -> Result<(), SetupApplyError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(_) => return Err(SetupApplyError::ConfigWriteFailed),
    };
    if !metadata.file_type().is_file() || factory_reset_fixture_is_reparse_point(&metadata) {
        return Err(SetupApplyError::ConfigWriteFailed);
    }
    fs::remove_file(path).map_err(|_| SetupApplyError::ConfigWriteFailed)
}

#[cfg(test)]
fn remove_factory_reset_fixture_directory(path: &Path) -> Result<(), SetupApplyError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(_) => return Err(SetupApplyError::ConfigWriteFailed),
    };
    if !metadata.file_type().is_dir() || factory_reset_fixture_is_reparse_point(&metadata) {
        return Err(SetupApplyError::ConfigWriteFailed);
    }
    validate_factory_reset_fixture_tree(path)?;
    fs::remove_dir_all(path).map_err(|_| SetupApplyError::ConfigWriteFailed)
}

#[cfg(test)]
fn validate_factory_reset_fixture_tree(path: &Path) -> Result<(), SetupApplyError> {
    let metadata = fs::symlink_metadata(path).map_err(|_| SetupApplyError::ConfigWriteFailed)?;
    if factory_reset_fixture_is_reparse_point(&metadata) {
        return Err(SetupApplyError::ConfigWriteFailed);
    }
    if metadata.file_type().is_file() {
        return Ok(());
    }
    if !metadata.file_type().is_dir() {
        return Err(SetupApplyError::ConfigWriteFailed);
    }
    for entry in fs::read_dir(path).map_err(|_| SetupApplyError::ConfigWriteFailed)? {
        let entry = entry.map_err(|_| SetupApplyError::ConfigWriteFailed)?;
        validate_factory_reset_fixture_tree(&entry.path())?;
    }
    Ok(())
}

fn recover_interrupted_setup(
    paths: &RuntimePaths,
    config_file: &Path,
    secrets: &AgentSecretStore,
) -> Result<(), SetupApplyError> {
    let transaction_file = setup_transaction_file(paths);
    let Some(transaction) = read_setup_transaction(&transaction_file)? else {
        return match secrets
            .callmesh_setup_state()
            .map_err(|_| SetupApplyError::ConfigWriteFailed)?
        {
            CallMeshSetupSecretState::None => Ok(()),
            CallMeshSetupSecretState::Promoted => secrets
                .finalize_callmesh_setup()
                .map(|_| ())
                .map_err(|_| SetupApplyError::ConfigWriteFailed),
            CallMeshSetupSecretState::Staged => Err(SetupApplyError::ConfigWriteFailed),
        };
    };
    let setup = SetupStore::open(paths).map_err(SetupApplyError::Setup)?;
    if setup
        .generation()
        .map_err(SetupApplyError::Setup)?
        .generation()
        != transaction.generation
    {
        restore_setup_configuration(config_file, transaction.previous_configuration.as_deref())?;
        secrets
            .rollback_callmesh_setup()
            .map_err(|_| SetupApplyError::ConfigWriteFailed)?;
        return remove_setup_transaction(&transaction_file);
    }
    let secret_state = secrets
        .callmesh_setup_state()
        .map_err(|_| SetupApplyError::ConfigWriteFailed)?;
    let setup_status = setup.status().map_err(SetupApplyError::Setup)?;
    if matches!(setup_status.phase, SetupPhase::Ready) {
        if secret_state == CallMeshSetupSecretState::Staged {
            secrets
                .promote_callmesh_setup()
                .map_err(|_| SetupApplyError::ConfigWriteFailed)?;
        }
        if secret_state != CallMeshSetupSecretState::None {
            secrets
                .finalize_callmesh_setup()
                .map_err(|_| SetupApplyError::ConfigWriteFailed)?;
        }
        return remove_setup_transaction(&transaction_file);
    }

    restore_setup_configuration(config_file, transaction.previous_configuration.as_deref())?;
    secrets
        .rollback_callmesh_setup()
        .map_err(|_| SetupApplyError::ConfigWriteFailed)?;
    setup
        .require_credentials()
        .map_err(SetupApplyError::Setup)?;
    remove_setup_transaction(&transaction_file)
}

async fn management_setup_status(State(state): State<Arc<AgentWebState>>) -> Response {
    match tokio::task::spawn_blocking(move || state.setup.status()).await {
        Ok(Ok(status)) => (StatusCode::OK, Json(status)).into_response(),
        Ok(Err(_)) | Err(_) => management_control_failed_response(),
    }
}

async fn management_setup_terms(
    State(state): State<Arc<AgentWebState>>,
    request: Result<Json<SetupTermsRequest>, JsonRejection>,
) -> Response {
    let Json(request) = match request {
        Ok(request) => request,
        Err(_) => return setup_request_invalid_response(),
    };
    let operation = tokio::task::spawn_blocking(move || {
        let status = state.setup.accept_terms(&request.terms_version)?;
        state
            .publish_setup_status(&status)
            .map_err(|_| cmclient_agent_core::setup::SetupError::WriteFailed)?;
        Ok::<_, cmclient_agent_core::setup::SetupError>(status)
    })
    .await;
    match operation {
        Ok(Ok(status)) => (StatusCode::OK, Json(status)).into_response(),
        Ok(Err(error)) => setup_error_response(error),
        Err(_) => management_control_failed_response(),
    }
}

async fn management_setup_reset(
    State(state): State<Arc<AgentWebState>>,
    request: Result<Json<SetupResetRequest>, JsonRejection>,
) -> Response {
    let Json(request) = match request {
        Ok(request) => request,
        Err(_) => return setup_request_invalid_response(),
    };
    if request.confirmation != "operational_reset" {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"code": "SETUP_RESET_CONFIRMATION_INVALID"})),
        )
            .into_response();
    }
    let operation = tokio::task::spawn_blocking(move || state.reset_setup()).await;
    match operation {
        Ok(Ok(status)) => (StatusCode::OK, Json(status)).into_response(),
        Ok(Err(error)) => setup_apply_error_response(error),
        Err(_) => management_control_failed_response(),
    }
}

async fn management_operational_reset(
    State(state): State<Arc<AgentWebState>>,
    request: Result<Json<SetupResetRequest>, JsonRejection>,
) -> Response {
    let Json(request) = match request {
        Ok(request) => request,
        Err(_) => return setup_request_invalid_response(),
    };
    if request.confirmation != "operational_reset" {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"code": "SETUP_RESET_CONFIRMATION_INVALID"})),
        )
            .into_response();
    }
    let operation = tokio::task::spawn_blocking(move || state.reset_operational()).await;
    match operation {
        Ok(Ok(status)) => (StatusCode::OK, Json(status)).into_response(),
        Ok(Err(error)) => setup_apply_error_response(error),
        Err(_) => management_control_failed_response(),
    }
}

async fn management_lifecycle_status(State(state): State<Arc<AgentWebState>>) -> Response {
    match state.lifecycle.lock() {
        Ok(status) => (StatusCode::OK, Json(status.clone())).into_response(),
        Err(_) => management_control_failed_response(),
    }
}

async fn management_setup_events(
    State(state): State<Arc<AgentWebState>>,
    headers: HeaderMap,
) -> Response {
    management_agent_events(&state.setup_events, &headers)
}

async fn management_lifecycle_events(
    State(state): State<Arc<AgentWebState>>,
    headers: HeaderMap,
) -> Response {
    management_agent_events(&state.lifecycle_events, &headers)
}

async fn management_update_status(State(state): State<Arc<AgentWebState>>) -> Response {
    let updates = Arc::clone(&state.updates);
    match tokio::task::spawn_blocking(move || updates.status()).await {
        Ok(Ok(status)) => (StatusCode::OK, Json(status)).into_response(),
        Ok(Err(_)) | Err(_) => management_control_failed_response(),
    }
}

async fn management_update_events(
    State(state): State<Arc<AgentWebState>>,
    headers: HeaderMap,
) -> Response {
    management_agent_events(&state.updates.web_events, &headers)
}

fn management_agent_events(hub: &AgentEventHub, headers: &HeaderMap) -> Response {
    let last_event_id = match parse_last_event_id(headers) {
        Ok(value) => value,
        Err(()) => return sse_cursor_error(),
    };
    let subscription = match hub.subscribe(last_event_id.as_deref()) {
        Ok(subscription) => subscription,
        Err(AgentEventHubError::SubscriberLimit) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"code": "SSE_SUBSCRIBER_LIMIT_REACHED"})),
            )
                .into_response();
        }
        Err(_) => return management_control_failed_response(),
    };
    let AgentEventSubscription {
        replay,
        live,
        _permit: permit,
    } = subscription;
    let replay = tokio_stream::iter(replay).map(agent_web_sse_event);
    let runtime_log = hub.runtime_log.clone();
    let live = BroadcastStream::new(live).map(move |event| {
        let _keep_permit = &permit;
        match event {
            Ok(event) => agent_web_sse_event(event),
            Err(_) => {
                if let Some(runtime_log) = &runtime_log {
                    let _ = runtime_log.write_code(LogLevel::Warn, "AGENT_SSE_SLOW_CONSUMER");
                }
                Err(std::io::Error::other("AGENT_SSE_SLOW_CONSUMER"))
            }
        }
    });
    let events = replay.chain(live);
    Sse::new(events)
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("heartbeat"),
        )
        .into_response()
}

fn parse_last_event_id(headers: &HeaderMap) -> Result<Option<String>, ()> {
    let mut values = headers.get_all("last-event-id").iter();
    let first = values.next();
    if values.next().is_some() {
        return Err(());
    }
    let Some(value) = first else {
        return Ok(None);
    };
    let value = value.to_str().map_err(|_| ())?;
    if !is_safe_sse_token(value) {
        return Err(());
    }
    Ok(Some(value.to_owned()))
}

fn sse_cursor_error() -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({"code": "SSE_CURSOR_INVALID"})),
    )
        .into_response()
}

fn setup_error_response(error: SetupError) -> Response {
    let status = match error {
        SetupError::TransitionInvalid | SetupError::StaleGeneration => StatusCode::CONFLICT,
        SetupError::PathInvalid
        | SetupError::ReadFailed
        | SetupError::Invalid
        | SetupError::WriteFailed
        | SetupError::GenerationExhausted => StatusCode::SERVICE_UNAVAILABLE,
    };
    (status, Json(serde_json::json!({"code": error.code()}))).into_response()
}

fn setup_discovery_error_response(error: DiscoveryError) -> Response {
    let status = match error {
        DiscoveryError::Busy => StatusCode::TOO_MANY_REQUESTS,
        DiscoveryError::ConfigurationInvalid => StatusCode::BAD_REQUEST,
        DiscoveryError::MdnsUnavailable | DiscoveryError::MdnsFailed => {
            StatusCode::SERVICE_UNAVAILABLE
        }
    };
    (status, Json(serde_json::json!({"code": error.code()}))).into_response()
}

fn setup_apply_error_response(error: SetupApplyError) -> Response {
    match error {
        SetupApplyError::Setup(error) => setup_error_response(error),
        SetupApplyError::InvalidInput => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"code": "SETUP_CONFIGURATION_INVALID"})),
        )
            .into_response(),
        SetupApplyError::Cancelled => (
            StatusCode::REQUEST_TIMEOUT,
            Json(serde_json::json!({"code": "SETUP_TRANSACTION_CANCELLED"})),
        )
            .into_response(),
        SetupApplyError::EndpointUnreachable => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"code": "SETUP_MESHTASTIC_UNREACHABLE"})),
        )
            .into_response(),
        SetupApplyError::CallMeshCredentialRejected => (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"code": "CALLMESH_CREDENTIAL_REJECTED"})),
        )
            .into_response(),
        SetupApplyError::CallMeshUnavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"code": "CALLMESH_UNAVAILABLE"})),
        )
            .into_response(),
        SetupApplyError::SupervisorUnavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"code": "SETUP_SUPERVISOR_UNAVAILABLE"})),
        )
            .into_response(),
        SetupApplyError::ConfigWriteFailed => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"code": "SETUP_CONFIGURATION_WRITE_FAILED"})),
        )
            .into_response(),
    }
}

fn setup_request_invalid_response() -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({"code": "SETUP_REQUEST_INVALID"})),
    )
        .into_response()
}

async fn management_control_route_not_found() -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({"code": "CONTROL_ROUTE_NOT_FOUND"})),
    )
        .into_response()
}

fn management_control_failed_response() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(serde_json::json!({"code": "CONTROL_COMMAND_FAILED"})),
    )
        .into_response()
}

fn agent_web_sse_event(event: AgentWebEvent) -> Result<Event, std::io::Error> {
    if !is_safe_sse_token(&event.id)
        || !is_safe_sse_token(&event.event)
        || event.data.len() > MAX_SSE_EVENT_BYTES
        || event.data.iter().any(|byte| matches!(*byte, b'\r' | b'\n'))
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Agent event is not valid SSE data",
        ));
    }
    let data = String::from_utf8(event.data).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Agent event data is not UTF-8",
        )
    })?;
    Ok(Event::default().id(event.id).event(event.event).data(data))
}

fn is_safe_sse_token(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_' | b':'))
}

fn agent_lifecycle_status(status: &ControlStatus) -> AgentLifecycleStatus {
    let gateway = match status.gateway {
        GatewayControlStatus::Stopped => "stopped",
        GatewayControlStatus::Starting => "starting",
        GatewayControlStatus::Running => "running",
        GatewayControlStatus::Backoff => "backoff",
        GatewayControlStatus::Degraded => "degraded",
    };
    let management_web = match status.management_web {
        ManagementWebControlStatus::Disabled => "disabled",
        ManagementWebControlStatus::Running => "running",
    };
    AgentLifecycleStatus {
        schema_version: 1,
        agent: String::from("running"),
        gateway: String::from(gateway),
        management_web: String::from(management_web),
        management_web_url: status.management_web_url.clone(),
        uptime_seconds: status.uptime_seconds,
        latest_error_code: status.latest_error_code.clone(),
    }
}

struct AgentController {
    identity: cmclient_control_api::ComponentIdentityReport,
    supervisor: Mutex<Option<GatewaySupervisor>>,
    gateway_transition: Mutex<()>,
    setup_transaction: Mutex<()>,
    cmcloud_enrollment_transition: Mutex<()>,
    private_gateway_bootstrap: bool,
    runtime_log: Option<StructuredLogSink>,
    agent_log_error_code: Mutex<Option<String>>,
    gateway_log_health: Mutex<GatewayRuntimeLogHealth>,
    gateway_session: GatewaySessionHandle,
    management_web: Mutex<Option<ManagementWebService>>,
    management_web_shutdown: Mutex<Option<JoinHandle<Result<(), ManagementWebError>>>>,
    management_web_config: ManagementWebConfig,
    management_access: Option<Arc<ManagementAccessController>>,
    control_endpoint: ControlEndpoint,
    paths: RuntimePaths,
    config_file: PathBuf,
    setup_transaction_file: PathBuf,
    cmcloud_endpoint: Option<String>,
    secrets: AgentSecretStore,
    setup: Arc<SetupStore>,
    setup_gate_required: bool,
    updates: Arc<AgentUpdateService>,
    web_state: Arc<AgentWebState>,
    shutdown_requested: AtomicBool,
    started_at: Instant,
    latest_error_code: Mutex<Option<String>>,
}

#[derive(Default)]
struct GatewayRuntimeLogHealth {
    capture_error_code: Option<String>,
    write_error_code: Option<String>,
}

impl GatewayRuntimeLogHealth {
    fn current_error_code(&self) -> Option<String> {
        self.capture_error_code
            .clone()
            .or_else(|| self.write_error_code.clone())
    }
}

struct SupervisorWorker {
    shutdown: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl SupervisorWorker {
    fn start(controller: Arc<AgentController>) -> Result<Self, ControlError> {
        let shutdown = Arc::new(AtomicBool::new(false));
        let worker_shutdown = Arc::clone(&shutdown);
        let worker = thread::Builder::new()
            .name(String::from("cmclient-gateway-supervisor"))
            .spawn(move || {
                let mut next_lifecycle_refresh = Instant::now();
                while !worker_shutdown.load(Ordering::Acquire) {
                    let lifecycle_changed = controller.tick_supervisor().unwrap_or(true);
                    let now = Instant::now();
                    if lifecycle_changed || now >= next_lifecycle_refresh {
                        let _ = controller.publish_lifecycle_snapshot();
                        next_lifecycle_refresh = now + LIFECYCLE_REFRESH_INTERVAL;
                    }
                    thread::sleep(SUPERVISOR_POLL_INTERVAL);
                }
            })
            .map_err(|_| ControlError::CommandFailed)?;
        Ok(Self {
            shutdown,
            worker: Some(worker),
        })
    }

    fn stop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl Drop for SupervisorWorker {
    fn drop(&mut self) {
        self.stop();
    }
}

fn apply_aprs_environment(environment: &mut BTreeMap<String, String>, aprs: Option<&AprsConfig>) {
    let Some(aprs) = aprs else {
        return;
    };
    environment.insert(String::from("CMCLIENT_APRS_ENABLED"), String::from("true"));
    if let Some(host) = &aprs.host {
        environment.insert(String::from("CMCLIENT_APRS_HOST"), host.clone());
    }
    if let Some(port) = aprs.port {
        environment.insert(String::from("CMCLIENT_APRS_PORT"), port.to_string());
    }
    if let Some(destination) = &aprs.destination {
        environment.insert(
            String::from("CMCLIENT_APRS_DESTINATION"),
            destination.clone(),
        );
    }
}

fn disable_proxy_for_setup(environment: &mut BTreeMap<String, String>) {
    environment.insert(
        String::from(PROXY_ENABLED_ENVIRONMENT_NAME),
        String::from("false"),
    );
}

fn apply_physical_qualification_environment(
    environment: &mut BTreeMap<String, String>,
    configured: Option<&str>,
    stage: Option<&str>,
) -> Result<(), ControlError> {
    let Some(configured) = configured.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(());
    };
    if ["0", "false", "no", "off"]
        .iter()
        .any(|value| configured.eq_ignore_ascii_case(value))
    {
        return Ok(());
    }
    if !["1", "true", "yes", "on"]
        .iter()
        .any(|value| configured.eq_ignore_ascii_case(value))
    {
        return Err(ControlError::CommandFailed);
    }
    let stage = stage
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("windows-source-smoke");
    if stage.len() > 128
        || !stage
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
    {
        return Err(ControlError::CommandFailed);
    }
    environment.insert(
        String::from("CMCLIENT_MESHTASTIC_PHYSICAL_PROFILE"),
        String::from("true"),
    );
    environment.insert(
        String::from("CMCLIENT_QUALIFICATION_STAGE"),
        stage.to_owned(),
    );
    Ok(())
}

fn physical_source_smoke_enabled() -> bool {
    std::env::var("CMCLIENT_MESHTASTIC_PHYSICAL_PROFILE")
        .ok()
        .is_some_and(|value| {
            ["1", "true", "yes", "on"]
                .iter()
                .any(|enabled| value.trim().eq_ignore_ascii_case(enabled))
        })
}

fn setup_gate_required(config: &AgentConfig) -> bool {
    setup_gate_required_with_profile(config, physical_source_smoke_enabled())
}

fn setup_gate_required_with_profile(config: &AgentConfig, physical_profile: bool) -> bool {
    // The physical source-smoke profile is a campaign-only observation path.
    // It is valid only when CallMesh/APRS/Proxy are absent; P18's product
    // guard then owns the one Meshtastic socket and its allowlisted write.
    if physical_profile
        && config.meshtastic.is_some()
        && config.callmesh.is_none()
        && config.aprs.is_none()
        && config.proxy.is_none()
    {
        return false;
    }
    true
}

const fn management_web_profile(profile: AgentRuntimeProfile) -> ManagementWebProfile {
    match profile {
        AgentRuntimeProfile::Native => ManagementWebProfile::Native,
        AgentRuntimeProfile::Docker => ManagementWebProfile::Docker,
    }
}

impl AgentController {
    fn install_setup_apply(self: &Arc<Self>) -> Result<(), ControlError> {
        let weak = Arc::downgrade(self);
        self.web_state
            .install_setup_apply(Arc::new(move |request, cancellation| {
                let controller = weak
                    .upgrade()
                    .ok_or(SetupApplyError::SupervisorUnavailable)?;
                controller.apply_setup(request, cancellation)
            }))
    }

    fn install_operational_reset(self: &Arc<Self>) -> Result<(), ControlError> {
        let weak = Arc::downgrade(self);
        self.web_state.install_operational_reset(Arc::new(move || {
            let controller = weak
                .upgrade()
                .ok_or(SetupApplyError::SupervisorUnavailable)?;
            controller.operational_reset()
        }))
    }

    fn install_cmcloud_enrollment(self: &Arc<Self>) -> Result<(), ControlError> {
        let enrollment_controller = Arc::downgrade(self);
        let status_controller = Arc::downgrade(self);
        self.web_state.install_cmcloud_enrollment(
            Arc::new(move |request| {
                let controller = enrollment_controller
                    .upgrade()
                    .ok_or(CMCloudEnrollmentControlError::Unavailable)?;
                controller.enroll_cmcloud(request)
            }),
            Arc::new(move || {
                let controller = status_controller
                    .upgrade()
                    .ok_or(CMCloudEnrollmentControlError::Unavailable)?;
                controller.cmcloud_enrollment_status()
            }),
        )?;
        let projection_controller = Arc::downgrade(self);
        self.web_state
            .install_cmcloud_account_projection(Arc::new(move || {
                let controller = projection_controller
                    .upgrade()
                    .ok_or(CMCloudAccountProjectionControlError::Unavailable)?;
                controller.cmcloud_account_projection()
            }))
    }

    fn cmcloud_enrollment_status(
        &self,
    ) -> Result<CMCloudEnrollmentStatus, CMCloudEnrollmentControlError> {
        let Some(endpoint) = self.cmcloud_endpoint.as_ref() else {
            return Ok(CMCloudEnrollmentStatus {
                schema_version: 1,
                state: CMCloudEnrollmentState::NotConfigured,
                endpoint: None,
                installation_generation: None,
                credential_version: None,
            });
        };
        let pending = self
            .secrets
            .cmcloud_enrollment_attempt()
            .map_err(|_| CMCloudEnrollmentControlError::Unavailable)?;
        if pending.is_some_and(|attempt| attempt.endpoint() == endpoint) {
            return Ok(CMCloudEnrollmentStatus {
                schema_version: 1,
                state: CMCloudEnrollmentState::PendingEnrollment,
                endpoint: Some(endpoint.clone()),
                installation_generation: None,
                credential_version: None,
            });
        }
        let active = self
            .secrets
            .cmcloud_active_device_credential()
            .map_err(|_| CMCloudEnrollmentControlError::Unavailable)?
            .filter(|credential| credential.identity().endpoint() == endpoint);
        match active {
            Some(credential) => Ok(CMCloudEnrollmentStatus {
                schema_version: 1,
                state: CMCloudEnrollmentState::Active,
                endpoint: Some(endpoint.clone()),
                installation_generation: Some(credential.identity().installation_generation()),
                credential_version: Some(credential.identity().credential_version()),
            }),
            None => Ok(CMCloudEnrollmentStatus {
                schema_version: 1,
                state: CMCloudEnrollmentState::CredentialsRequired,
                endpoint: Some(endpoint.clone()),
                installation_generation: None,
                credential_version: None,
            }),
        }
    }

    fn cmcloud_account_projection(
        &self,
    ) -> Result<CMCloudAccountProjection, CMCloudAccountProjectionControlError> {
        let status = self
            .cmcloud_enrollment_status()
            .map_err(|_| CMCloudAccountProjectionControlError::Unavailable)?;
        if status.state != CMCloudEnrollmentState::Active {
            return Err(CMCloudAccountProjectionControlError::Unavailable);
        }
        let route = self
            .gateway_session
            .snapshot()
            .ok_or(CMCloudAccountProjectionControlError::Unavailable)?;
        let projection = gateway_cmcloud_account_projection(&route)?;
        projection.validate()?;
        if status.installation_generation != Some(projection.generation) {
            return Err(CMCloudAccountProjectionControlError::Stale);
        }
        Ok(projection)
    }

    fn enroll_cmcloud(
        &self,
        request: CMCloudEnrollmentRequest,
    ) -> Result<CMCloudEnrollmentStatus, CMCloudEnrollmentControlError> {
        let Some(endpoint) = self.cmcloud_endpoint.as_deref() else {
            return Err(CMCloudEnrollmentControlError::NotConfigured);
        };
        if !valid_cmcloud_pairing_code(&request.pairing_code) {
            return Err(CMCloudEnrollmentControlError::InvalidInput);
        }
        self.ensure_resource_start_allowed()
            .map_err(|_| CMCloudEnrollmentControlError::Unavailable)?;
        let _enrollment = match self.cmcloud_enrollment_transition.try_lock() {
            Ok(guard) => guard,
            Err(std::sync::TryLockError::WouldBlock) => {
                return Err(CMCloudEnrollmentControlError::InProgress);
            }
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return Err(CMCloudEnrollmentControlError::Unavailable);
            }
        };
        self.ensure_cmcloud_enrollment_setup_allowed()?;
        // This only writes or resumes Agent-owned recovery material. A local
        // preflight rejection must not interrupt a working CMCloud Gateway.
        cmcloud_enrollment::prepare_cmcloud_enrollment(
            &self.secrets,
            endpoint,
            &request.pairing_code,
            &self.identity.identity.version,
        )
        .map_err(CMCloudEnrollmentControlError::Enrollment)?;
        // Keep an established Gateway running while the Agent obtains and
        // acknowledges a provisional credential. CMCloud does not revoke the
        // active credential/session until that ACK commits, so transport or
        // pairing failures here cannot interrupt the current station.
        cmcloud_enrollment::enroll_cmcloud_blocking(
            &self.secrets,
            endpoint,
            &request.pairing_code,
            &self.identity.identity.version,
        )
        .map_err(CMCloudEnrollmentControlError::Enrollment)?;
        self.configure_active_cmcloud_gateway_credential()?;
        self.mark_cmcloud_enrollment_ready()?;
        self.start_supervisor()
            .map_err(|_| CMCloudEnrollmentControlError::Unavailable)?;
        self.web_state.record_audit(
            "cmcloud_enrollment",
            "allowed",
            "CMCLOUD_ENROLLMENT_COMPLETED",
        );
        self.log_agent_code(LogLevel::Info, "CMCLOUD_ENROLLMENT_COMPLETED");
        self.cmcloud_enrollment_status()
    }

    fn ensure_cmcloud_enrollment_setup_allowed(&self) -> Result<(), CMCloudEnrollmentControlError> {
        if !self.setup_gate_required {
            return Ok(());
        }
        let status = self
            .setup
            .status()
            .map_err(|_| CMCloudEnrollmentControlError::Unavailable)?;
        if matches!(
            status.phase,
            SetupPhase::CredentialsRequired | SetupPhase::Ready
        ) {
            Ok(())
        } else {
            Err(CMCloudEnrollmentControlError::SetupRequired)
        }
    }

    fn configure_active_cmcloud_gateway_credential(
        &self,
    ) -> Result<(), CMCloudEnrollmentControlError> {
        if !self.private_gateway_bootstrap {
            return Err(CMCloudEnrollmentControlError::Unavailable);
        }
        let Some(endpoint) = self.cmcloud_endpoint.as_deref() else {
            return Err(CMCloudEnrollmentControlError::NotConfigured);
        };
        let credential = self
            .secrets
            .cmcloud_active_device_credential()
            .map_err(|_| CMCloudEnrollmentControlError::Unavailable)?
            .filter(|credential| credential.identity().endpoint() == endpoint)
            .ok_or(CMCloudEnrollmentControlError::Unavailable)?;
        let _gateway_transition = self
            .gateway_transition
            .lock()
            .map_err(|_| CMCloudEnrollmentControlError::Unavailable)?;
        self.stop_supervisor_locked()
            .map_err(|_| CMCloudEnrollmentControlError::Unavailable)?;
        let mut supervisor = self
            .supervisor
            .lock()
            .map_err(|_| CMCloudEnrollmentControlError::Unavailable)?;
        let Some(supervisor) = supervisor.as_mut() else {
            return Ok(());
        };
        supervisor.set_environment(cmcloud_gateway_environment(endpoint, &credential));
        supervisor
            .set_cmcloud_device_credential(
                credential.identity().endpoint(),
                credential.identity().installation_id(),
                credential.identity().installation_generation(),
                credential.identity().credential_version(),
                credential.device_credential().expose_secret(),
            )
            .map_err(|_| CMCloudEnrollmentControlError::Unavailable)
    }

    fn mark_cmcloud_enrollment_ready(&self) -> Result<(), CMCloudEnrollmentControlError> {
        if !self.setup_gate_required {
            return Ok(());
        }
        let phase = self
            .setup
            .status()
            .map_err(|_| CMCloudEnrollmentControlError::Unavailable)?
            .phase;
        if matches!(phase, SetupPhase::Ready) {
            return Ok(());
        }
        if !matches!(phase, SetupPhase::CredentialsRequired) {
            return Err(CMCloudEnrollmentControlError::SetupRequired);
        }
        let fence = self
            .setup
            .begin_validation()
            .map_err(|_| CMCloudEnrollmentControlError::Unavailable)?;
        let status = self
            .setup
            .mark_ready(fence)
            .map_err(|_| CMCloudEnrollmentControlError::Unavailable)?;
        self.web_state
            .publish_setup_status(&status)
            .map_err(|_| CMCloudEnrollmentControlError::Unavailable)
    }

    fn apply_setup(
        self: &Arc<Self>,
        request: SetupConfigureRequest,
        cancellation: SetupCancellationToken,
    ) -> Result<SetupPendingCommit, SetupApplyError> {
        let _transaction = self
            .setup_transaction
            .lock()
            .map_err(|_| SetupApplyError::SupervisorUnavailable)?;
        validate_setup_request(&request)?;
        let current = self.setup.status()?;
        if !self.setup_gate_required || !matches!(current.phase, SetupPhase::CredentialsRequired) {
            return Err(SetupApplyError::Setup(SetupError::TransitionInvalid));
        }
        self.check_setup_cancellation(&cancellation, None, SetupRollbackState::None)?;

        let previous_configuration = capture_setup_configuration(&self.config_file)?;
        let generation = self.setup.generation()?.generation();
        let mesh_network_id = request
            .mesh_network_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or("default");
        let gateway_id = request
            .gateway_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or("cmclient-gateway");
        self.check_setup_cancellation(
            &cancellation,
            previous_configuration.as_deref(),
            SetupRollbackState::None,
        )?;
        write_setup_transaction(
            &self.setup_transaction_file,
            generation,
            previous_configuration.as_deref(),
        )?;
        self.check_setup_cancellation(
            &cancellation,
            previous_configuration.as_deref(),
            SetupRollbackState::JournalOnly,
        )?;
        let fence = match self.setup.begin_validation() {
            Ok(fence) => fence,
            Err(error) => {
                let _ = remove_setup_transaction(&self.setup_transaction_file);
                return Err(error.into());
            }
        };
        self.check_setup_cancellation(
            &cancellation,
            previous_configuration.as_deref(),
            SetupRollbackState::Validating,
        )?;
        if let Err(error) = self.configure_supervisor_for_setup(
            fence.generation(),
            &request,
            mesh_network_id,
            gateway_id,
        ) {
            return Err(self.rollback_setup_error(previous_configuration.as_deref(), false, error));
        }
        self.check_setup_cancellation(
            &cancellation,
            previous_configuration.as_deref(),
            SetupRollbackState::Validating,
        )?;

        match self.start_supervisor_for_setup() {
            Ok(true) => {}
            Ok(false) | Err(_) => {
                let error = self.setup_supervisor_failure();
                return Err(self.rollback_setup_error(
                    previous_configuration.as_deref(),
                    false,
                    error,
                ));
            }
        }
        self.check_setup_cancellation(
            &cancellation,
            previous_configuration.as_deref(),
            SetupRollbackState::Validating,
        )?;
        if self.prepare_supervisor_for_committed_setup().is_err() {
            return Err(self.rollback_setup_error(
                previous_configuration.as_deref(),
                false,
                SetupApplyError::SupervisorUnavailable,
            ));
        }
        self.check_setup_cancellation(
            &cancellation,
            previous_configuration.as_deref(),
            SetupRollbackState::Validating,
        )?;

        if let Err(error) = write_setup_configuration(
            &self.config_file,
            request.meshtastic_host.trim(),
            request.meshtastic_port,
            mesh_network_id,
            gateway_id,
        ) {
            return Err(self.rollback_setup_error(previous_configuration.as_deref(), true, error));
        }
        self.check_setup_cancellation(
            &cancellation,
            previous_configuration.as_deref(),
            SetupRollbackState::Persistent,
        )?;
        let status = match self.setup.mark_ready(fence) {
            Ok(status) => status,
            Err(error) => {
                return Err(self.rollback_setup_error(
                    previous_configuration.as_deref(),
                    true,
                    error.into(),
                ));
            }
        };
        match self.start_supervisor() {
            Ok(true) => {}
            Ok(false) | Err(_) => {
                let error = self.setup_supervisor_failure();
                return Err(self.rollback_setup_error(
                    previous_configuration.as_deref(),
                    true,
                    error,
                ));
            }
        }
        if self.complete_supervisor_setup_commit_start().is_err() {
            return Err(self.rollback_setup_error(
                previous_configuration.as_deref(),
                true,
                SetupApplyError::SupervisorUnavailable,
            ));
        }
        self.check_setup_cancellation(
            &cancellation,
            previous_configuration.as_deref(),
            SetupRollbackState::Persistent,
        )?;
        Ok(SetupPendingCommit {
            controller: Arc::clone(self),
            status,
            previous_configuration,
            committed: false,
        })
    }

    fn check_setup_cancellation(
        &self,
        cancellation: &SetupCancellationToken,
        previous_configuration: Option<&[u8]>,
        state: SetupRollbackState,
    ) -> Result<(), SetupApplyError> {
        if !cancellation.is_cancelled() {
            return Ok(());
        }
        match state {
            SetupRollbackState::None => Err(SetupApplyError::Cancelled),
            SetupRollbackState::JournalOnly => {
                remove_setup_transaction(&self.setup_transaction_file)?;
                Err(SetupApplyError::Cancelled)
            }
            SetupRollbackState::Validating => Err(self.rollback_setup_error(
                previous_configuration,
                false,
                SetupApplyError::Cancelled,
            )),
            SetupRollbackState::Persistent => Err(self.rollback_setup_error(
                previous_configuration,
                true,
                SetupApplyError::Cancelled,
            )),
        }
    }

    fn finish_setup_commit(&self, status: &SetupStatus) -> Result<(), SetupApplyError> {
        // Keep the promoted secret transaction rollback-capable until every
        // other fallible commit action has completed. A crash after removing
        // the journal is still recoverable: Ready plus a promoted secret is
        // finalized by startup recovery.
        remove_setup_transaction(&self.setup_transaction_file)?;
        // Finalize any legacy transaction left by an interrupted pre-CMCloud
        // setup, but never create or promote a CallMesh credential here.
        self.secrets
            .finalize_callmesh_setup()
            .map_err(|_| SetupApplyError::ConfigWriteFailed)?;
        if self.web_state.publish_setup_status(status).is_err() {
            self.remember_error_code("SETUP_STATUS_PUBLISH_FAILED");
        } else {
            self.clear_error();
        }
        Ok(())
    }

    fn setup_supervisor_failure(&self) -> SetupApplyError {
        let code = self
            .latest_error_code
            .lock()
            .ok()
            .and_then(|value| value.clone());
        match code.as_deref() {
            Some("CALLMESH_CREDENTIAL_REJECTED") => SetupApplyError::CallMeshCredentialRejected,
            Some("CALLMESH_UNAVAILABLE") => SetupApplyError::CallMeshUnavailable,
            Some("SETUP_MESHTASTIC_UNREACHABLE") => SetupApplyError::EndpointUnreachable,
            _ => SetupApplyError::SupervisorUnavailable,
        }
    }

    fn prepare_supervisor_for_committed_setup(&self) -> Result<(), SetupApplyError> {
        if !self
            .stop_supervisor()
            .map_err(|_| SetupApplyError::SupervisorUnavailable)?
        {
            return Err(SetupApplyError::SupervisorUnavailable);
        }
        let _transition = self
            .gateway_transition
            .lock()
            .map_err(|_| SetupApplyError::SupervisorUnavailable)?;
        let mut supervisor = self
            .supervisor
            .lock()
            .map_err(|_| SetupApplyError::SupervisorUnavailable)?;
        let supervisor = supervisor
            .as_mut()
            .ok_or(SetupApplyError::SupervisorUnavailable)?;
        if !matches!(supervisor.status(), GatewayStatus::Stopped) {
            return Err(SetupApplyError::SupervisorUnavailable);
        }
        supervisor.set_environment(BTreeMap::from([
            (
                String::from(SETUP_VALIDATION_ONLY_ENVIRONMENT_NAME),
                String::from("false"),
            ),
            (
                String::from(SETUP_COMMIT_START_ENVIRONMENT_NAME),
                String::from("true"),
            ),
            (
                String::from("CMCLIENT_MESHTASTIC_PHYSICAL_PROFILE"),
                String::from("false"),
            ),
            (
                String::from(PROXY_ENABLED_ENVIRONMENT_NAME),
                String::from("false"),
            ),
        ]));
        Ok(())
    }

    fn complete_supervisor_setup_commit_start(&self) -> Result<(), SetupApplyError> {
        let mut supervisor = self
            .supervisor
            .lock()
            .map_err(|_| SetupApplyError::SupervisorUnavailable)?;
        let supervisor = supervisor
            .as_mut()
            .ok_or(SetupApplyError::SupervisorUnavailable)?;
        supervisor.set_environment(BTreeMap::from([(
            String::from(SETUP_COMMIT_START_ENVIRONMENT_NAME),
            String::from("false"),
        )]));
        Ok(())
    }

    fn rollback_setup_error(
        &self,
        previous_configuration: Option<&[u8]>,
        restore_persistent_state: bool,
        original: SetupApplyError,
    ) -> SetupApplyError {
        self.rollback_setup_attempt(previous_configuration, restore_persistent_state)
            .err()
            .unwrap_or(original)
    }

    fn rollback_setup_attempt(
        &self,
        previous_configuration: Option<&[u8]>,
        restore_persistent_state: bool,
    ) -> Result<(), SetupApplyError> {
        let mut rollback_failed = self.stop_supervisor().is_err();
        if let Ok(mut supervisor) = self.supervisor.lock() {
            if let Some(supervisor) = supervisor.as_mut() {
                supervisor.clear_callmesh_api_key();
            }
        } else {
            rollback_failed = true;
        }
        if self.secrets.rollback_callmesh_setup().is_err() {
            rollback_failed = true;
        }
        if restore_persistent_state
            && restore_setup_configuration(&self.config_file, previous_configuration).is_err()
        {
            rollback_failed = true;
        }
        match self.setup.require_credentials() {
            Ok(status) => {
                if self.web_state.publish_setup_status(&status).is_err() {
                    rollback_failed = true;
                }
            }
            Err(_) => rollback_failed = true,
        }
        if !rollback_failed && remove_setup_transaction(&self.setup_transaction_file).is_err() {
            rollback_failed = true;
        }
        if rollback_failed {
            Err(SetupApplyError::ConfigWriteFailed)
        } else {
            Ok(())
        }
    }

    fn configure_supervisor_for_setup(
        &self,
        generation: u64,
        request: &SetupConfigureRequest,
        mesh_network_id: &str,
        gateway_id: &str,
    ) -> Result<(), SetupApplyError> {
        let _transition = self
            .gateway_transition
            .lock()
            .map_err(|_| SetupApplyError::SupervisorUnavailable)?;
        let mut supervisor = self
            .supervisor
            .lock()
            .map_err(|_| SetupApplyError::SupervisorUnavailable)?;
        let Some(supervisor) = supervisor.as_mut() else {
            return Err(SetupApplyError::SupervisorUnavailable);
        };
        if !matches!(supervisor.status(), GatewayStatus::Stopped) {
            supervisor
                .stop()
                .map_err(|_| SetupApplyError::SupervisorUnavailable)?;
        }
        let mut environment = BTreeMap::new();
        environment.insert(
            String::from(CMCLOUD_MODE_ENVIRONMENT_NAME),
            String::from("required"),
        );
        environment.insert(
            String::from(CMCLOUD_URL_ENVIRONMENT_NAME),
            String::from(CMCLOUD_PRODUCTION_URL),
        );
        environment.insert(
            String::from("CMCLIENT_MESHTASTIC_TRANSPORT"),
            String::from("tcp"),
        );
        environment.insert(
            String::from("CMCLIENT_MESHTASTIC_TCP_HOST"),
            request.meshtastic_host.trim().to_owned(),
        );
        environment.insert(
            String::from("CMCLIENT_MESHTASTIC_TCP_PORT"),
            request.meshtastic_port.to_string(),
        );
        environment.insert(
            String::from("CMCLIENT_MESH_NETWORK_ID"),
            mesh_network_id.into(),
        );
        environment.insert(String::from("CMCLIENT_GATEWAY_ID"), gateway_id.into());
        environment.insert(String::from("CMCLIENT_APRS_ENABLED"), String::from("false"));
        environment.insert(
            String::from(SETUP_VALIDATION_ONLY_ENVIRONMENT_NAME),
            String::from("true"),
        );
        environment.insert(
            String::from(SETUP_COMMIT_START_ENVIRONMENT_NAME),
            String::from("false"),
        );
        environment.insert(
            String::from("CMCLIENT_MESHTASTIC_PHYSICAL_PROFILE"),
            String::from("true"),
        );
        disable_proxy_for_setup(&mut environment);
        environment.insert(
            String::from("CMCLIENT_QUALIFICATION_STAGE"),
            format!("setup-generation-{generation}"),
        );
        supervisor.set_environment(environment);
        supervisor
            .set_setup_generation(generation)
            .map_err(|_| SetupApplyError::SupervisorUnavailable)
    }

    fn from_config(config: &AgentConfig) -> Result<Self, ControlError> {
        let secrets =
            AgentSecretStore::runtime(config.paths.root_dir()).map_err(control_secret_error)?;
        Self::from_config_with_secrets(config, secrets)
    }

    fn from_config_with_secrets(
        config: &AgentConfig,
        secrets: AgentSecretStore,
    ) -> Result<Self, ControlError> {
        Self::from_config_with_secrets_internal(config, secrets, true)
    }

    #[cfg(test)]
    fn from_config_with_secrets_without_private_bootstrap(
        config: &AgentConfig,
        secrets: AgentSecretStore,
    ) -> Result<Self, ControlError> {
        Self::from_config_with_secrets_internal(config, secrets, false)
    }

    fn from_config_with_secrets_internal(
        config: &AgentConfig,
        secrets: AgentSecretStore,
        private_bootstrap: bool,
    ) -> Result<Self, ControlError> {
        if config.cmcloud.is_some()
            && (config.callmesh.is_some() || config.aprs.is_some() || config.proxy.is_some())
        {
            return Err(ControlError::CommandFailed);
        }
        let identity = compiled_component_identity(InternalComponent::Agent)
            .map_err(|_| ControlError::CommandFailed)?;
        let setup =
            Arc::new(SetupStore::open(&config.paths).map_err(|_| ControlError::CommandFailed)?);
        let setup_gate_required = setup_gate_required(config);
        let cmcloud_active_credential = if let Some(cmcloud) = config.cmcloud.as_ref() {
            secrets
                .cmcloud_active_device_credential()
                .map_err(control_secret_error)?
                .filter(|credential| {
                    credential.identity().endpoint() == cmcloud.agent_websocket_url
                })
        } else {
            None
        };
        let credentials_ready = if config.cmcloud.is_some() {
            cmcloud_active_credential.is_some()
        } else {
            secrets
                .read(SecretKind::CallMeshApiKey)
                .map_err(control_secret_error)?
                .is_some()
        };
        if setup_gate_required
            && !credentials_ready
            && matches!(
                setup
                    .snapshot()
                    .map_err(|_| ControlError::CommandFailed)?
                    .phase,
                SetupPhase::Ready
            )
        {
            setup
                .require_credentials()
                .map_err(|_| ControlError::CommandFailed)?;
        }
        let setup_generation = setup
            .generation()
            .map_err(|_| ControlError::CommandFailed)?
            .generation();
        let mut initial_log_error_code = None;
        let mut initial_agent_log_error_code = None;
        let mut initial_gateway_log_error_code = None;
        let log_policy = match LogPolicy::from_environment() {
            Ok(policy) => Some(policy),
            Err(error) => {
                initial_log_error_code = Some(String::from(error.code()));
                initial_agent_log_error_code = Some(String::from(error.code()));
                None
            }
        };
        let runtime_log = log_policy.and_then(|policy| {
            match StructuredLogSink::open(&config.paths.log_dir, AGENT_LOG_FILE, "agent", policy) {
                Ok(sink) => Some(sink),
                Err(error) => {
                    remember_initial_log_error(&mut initial_log_error_code, error.code());
                    initial_agent_log_error_code = Some(String::from(error.code()));
                    None
                }
            }
        });
        let control_endpoint = default_local_endpoint(config.paths.root_dir())?;
        let gateway_command = resolve_gateway_command(config);
        let supervisor = gateway_command
            .as_ref()
            .map(|command| {
                let mut supervisor = GatewaySupervisor::new(
                    GatewayCommand {
                        program: command.first().cloned().unwrap_or_default(),
                        arguments: command.iter().skip(1).cloned().collect(),
                    },
                    BackoffPolicy::default(),
                )
                .map_err(|_| ControlError::CommandFailed)?;
                let mut environment = BTreeMap::from([
                    (String::from("CMCLIENT_SUPERVISED"), String::from("1")),
                    (
                        String::from("CMCLIENT_BUILD_VERSION"),
                        identity.identity.version.clone(),
                    ),
                    (
                        String::from("CMCLIENT_BUILD_COMMIT"),
                        identity.identity.source_commit.clone(),
                    ),
                    (
                        String::from("CMCLIENT_BUILD_TREE"),
                        identity.identity.source_tree.clone(),
                    ),
                    (
                        String::from("CMCLIENT_BUILD_CHANNEL"),
                        String::from(identity.identity.channel.as_str()),
                    ),
                    (
                        String::from("CMCLIENT_RUNTIME_PROFILE"),
                        String::from(identity.identity.target.profile.as_str()),
                    ),
                    (
                        String::from("CMCLIENT_PACKAGE_PROFILE"),
                        String::from(identity.identity.target.package_profile.as_str()),
                    ),
                    (
                        String::from("CMCLIENT_TARGET_OS"),
                        String::from(identity.identity.target.os.as_str()),
                    ),
                    (
                        String::from("CMCLIENT_TARGET_ARCHITECTURE"),
                        String::from(identity.identity.target.architecture.as_str()),
                    ),
                    (
                        String::from("CMCLIENT_RUNTIME_ROOT"),
                        config.paths.root_dir().to_string_lossy().into_owned(),
                    ),
                    (
                        String::from("CMCLIENT_DB_PATH"),
                        config.paths.database_file().to_string_lossy().into_owned(),
                    ),
                    (
                        String::from("CMCLIENT_BACKUP_DIR"),
                        config.paths.backups_dir().to_string_lossy().into_owned(),
                    ),
                ]);
                let physical_profile = std::env::var("CMCLIENT_MESHTASTIC_PHYSICAL_PROFILE").ok();
                let qualification_stage = std::env::var("CMCLIENT_QUALIFICATION_STAGE").ok();
                apply_physical_qualification_environment(
                    &mut environment,
                    physical_profile.as_deref(),
                    qualification_stage.as_deref(),
                )?;
                if let Some(callmesh) = &config.callmesh {
                    environment.insert(String::from("CMCLIENT_CALLMESH_URL"), callmesh.url.clone());
                }
                if let Some(cmcloud) = config.cmcloud.as_ref() {
                    environment.insert(
                        String::from(CMCLOUD_MODE_ENVIRONMENT_NAME),
                        String::from("required"),
                    );
                    environment.insert(
                        String::from(CMCLOUD_URL_ENVIRONMENT_NAME),
                        cmcloud.agent_websocket_url.clone(),
                    );
                    environment
                        .insert(String::from("CMCLIENT_APRS_ENABLED"), String::from("false"));
                    if let Some(credential) = cmcloud_active_credential.as_ref() {
                        environment.insert(
                            String::from(CMCLOUD_INSTALLATION_ID_ENVIRONMENT_NAME),
                            credential.identity().installation_id().to_owned(),
                        );
                        environment.insert(
                            String::from(CMCLOUD_INSTALLATION_GENERATION_ENVIRONMENT_NAME),
                            credential.identity().installation_generation().to_string(),
                        );
                        environment.insert(
                            String::from(CMCLOUD_CREDENTIAL_VERSION_ENVIRONMENT_NAME),
                            credential.identity().credential_version().to_string(),
                        );
                    }
                }
                if let Some(meshtastic) = &config.meshtastic {
                    environment.insert(
                        String::from("CMCLIENT_MESH_NETWORK_ID"),
                        meshtastic.mesh_network_id.clone(),
                    );
                    environment.insert(
                        String::from("CMCLIENT_GATEWAY_ID"),
                        meshtastic.gateway_id.clone(),
                    );
                    match &meshtastic.connection {
                        MeshtasticConnectionConfig::Tcp { host, port } => {
                            environment.insert(
                                String::from("CMCLIENT_MESHTASTIC_TRANSPORT"),
                                String::from("tcp"),
                            );
                            environment
                                .insert(String::from("CMCLIENT_MESHTASTIC_TCP_HOST"), host.clone());
                            environment.insert(
                                String::from("CMCLIENT_MESHTASTIC_TCP_PORT"),
                                port.to_string(),
                            );
                        }
                        MeshtasticConnectionConfig::Serial { path, baud_rate } => {
                            environment.insert(
                                String::from("CMCLIENT_MESHTASTIC_TRANSPORT"),
                                String::from("serial"),
                            );
                            environment.insert(
                                String::from("CMCLIENT_MESHTASTIC_SERIAL_PATH"),
                                path.clone(),
                            );
                            environment.insert(
                                String::from("CMCLIENT_MESHTASTIC_SERIAL_BAUD"),
                                baud_rate.to_string(),
                            );
                        }
                    }
                }
                if config.cmcloud.is_none() {
                    apply_aprs_environment(&mut environment, config.aprs.as_ref());
                }
                environment.insert(
                    String::from(PROXY_ENABLED_ENVIRONMENT_NAME),
                    String::from("false"),
                );
                if config.cmcloud.is_none() {
                    if let Some(proxy) = &config.proxy {
                        environment.insert(
                            String::from(PROXY_ENABLED_ENVIRONMENT_NAME),
                            String::from("true"),
                        );
                        environment.insert(String::from("CMCLIENT_PROXY_HOST"), proxy.host.clone());
                        environment
                            .insert(String::from("CMCLIENT_PROXY_PORT"), proxy.port.to_string());
                        environment.insert(
                            String::from("CMCLIENT_PROXY_UPSTREAM_HOST"),
                            proxy.upstream_host.clone(),
                        );
                        environment.insert(
                            String::from("CMCLIENT_PROXY_UPSTREAM_PORT"),
                            proxy.upstream_port.to_string(),
                        );
                        environment.insert(String::from("CMCLIENT_PROXY_MODE"), proxy.mode.clone());
                        environment.insert(
                            String::from("CMCLIENT_PROXY_ALLOW_LAN"),
                            proxy.allow_lan.to_string(),
                        );
                        if !proxy.allowlist.is_empty() {
                            environment.insert(
                                String::from("CMCLIENT_PROXY_ALLOWLIST"),
                                proxy.allowlist.join(","),
                            );
                        }
                    }
                }
                supervisor.set_environment(environment);
                if private_bootstrap {
                    supervisor
                        .set_setup_generation(setup_generation)
                        .map_err(|_| ControlError::CommandFailed)?;
                    supervisor
                        .enable_private_bootstrap()
                        .map_err(|_| ControlError::CommandFailed)?;
                    if config.callmesh.is_some() {
                        if let Some(api_key) = secrets
                            .read(SecretKind::CallMeshApiKey)
                            .map_err(control_secret_error)?
                        {
                            supervisor
                                .set_callmesh_api_key(api_key.expose_secret())
                                .map_err(|_| ControlError::CommandFailed)?;
                        }
                    }
                    if let Some(credential) = cmcloud_active_credential.as_ref() {
                        supervisor
                            .set_cmcloud_device_credential(
                                credential.identity().endpoint(),
                                credential.identity().installation_id(),
                                credential.identity().installation_generation(),
                                credential.identity().credential_version(),
                                credential.device_credential().expose_secret(),
                            )
                            .map_err(|_| ControlError::CommandFailed)?;
                    }
                }
                if let Some(policy) = log_policy {
                    match StructuredLogSink::open(
                        &config.paths.log_dir,
                        GATEWAY_LOG_FILE,
                        "gateway",
                        policy,
                    ) {
                        Ok(sink) => supervisor
                            .set_log_sink(sink)
                            .map_err(|_| ControlError::CommandFailed)?,
                        Err(error) => {
                            remember_initial_log_error(&mut initial_log_error_code, error.code());
                            initial_gateway_log_error_code = Some(String::from(error.code()));
                        }
                    }
                }
                Ok(supervisor)
            })
            .transpose()?;
        let management_access = config
            .management_lan
            .as_ref()
            .map(|lan| {
                ManagementAccessController::new(lan.access.clone())
                    .map(Arc::new)
                    .map_err(|_| ControlError::CommandFailed)
            })
            .transpose()?;
        let management_tls = match config.management_lan.as_ref() {
            Some(lan) => match (&lan.certificate_path, &lan.private_key_path) {
                (Some(certificate_path), Some(private_key_path)) => Some(ManagementTlsConfig {
                    certificate_path: certificate_path.clone(),
                    private_key_path: private_key_path.clone(),
                }),
                (None, None) => None,
                _ => return Err(ControlError::CommandFailed),
            },
            None => None,
        };
        let management_web_config = ManagementWebConfig {
            enabled: true,
            port: config.management_lan.as_ref().map_or(7080, |lan| lan.port),
            profile: management_web_profile(config.runtime_profile),
            setup_generation,
            setup_required: setup_gate_required
                && setup
                    .status()
                    .map_err(|_| ControlError::CommandFailed)?
                    .setup_required,
            allow_lan: management_access.is_some(),
            allowed_cidrs: config
                .management_lan
                .as_ref()
                .map_or_else(Vec::new, |lan| lan.allowed_cidrs.clone()),
            allowed_hosts: config
                .management_lan
                .as_ref()
                .map_or_else(Default::default, |lan| lan.allowed_hosts.clone()),
            tls: management_tls,
            static_web_root: Some(resolve_static_web_root()),
        };
        let gateway_session = GatewaySessionHandle::new();
        let updates = Arc::new(AgentUpdateService::new_with_log(
            config.paths.root_dir(),
            runtime_log.clone(),
        )?);
        updates.recover()?;
        let started_at = Instant::now();
        let migrated_meshtastic =
            config
                .meshtastic
                .as_ref()
                .and_then(|meshtastic| match &meshtastic.connection {
                    MeshtasticConnectionConfig::Tcp { host, port } => {
                        MeshtasticCandidate::new(host.clone(), *port, DiscoverySource::Migrated)
                    }
                    MeshtasticConnectionConfig::Serial { .. } => None,
                });
        let web_state = Arc::new(AgentWebState::new_with_log(
            Arc::clone(&updates),
            Arc::clone(&setup),
            setup_gate_required,
            AgentLifecycleStatus {
                schema_version: 1,
                agent: String::from("running"),
                gateway: String::from("stopped"),
                management_web: if config.management_web_enabled {
                    String::from("running")
                } else {
                    String::from("disabled")
                },
                management_web_url: None,
                uptime_seconds: 0,
                latest_error_code: management_web_config
                    .setup_required
                    .then(|| String::from("SETUP_REQUIRED")),
            },
            runtime_log.clone(),
            management_access.clone(),
            migrated_meshtastic,
        )?);
        let management_web = if config.management_web_enabled {
            Some(
                ManagementWebService::start(
                    &management_web_config,
                    agent_web_router(Arc::clone(&web_state)),
                    management_access.clone(),
                    gateway_session.clone(),
                )
                .map_err(|_| ControlError::CommandFailed)?,
            )
        } else {
            None
        };
        if let Some(service) = management_web.as_ref() {
            web_state.attach_management_setup_state(service.setup_state())?;
            web_state.update_lifecycle(AgentLifecycleStatus {
                schema_version: 1,
                agent: String::from("running"),
                gateway: String::from("stopped"),
                management_web: String::from("running"),
                management_web_url: Some(service.advertised_url().trim_end_matches('/').to_owned()),
                uptime_seconds: 0,
                latest_error_code: management_web_config
                    .setup_required
                    .then(|| String::from("SETUP_REQUIRED")),
            })?;
        }
        let controller = Self {
            identity,
            supervisor: Mutex::new(supervisor),
            gateway_transition: Mutex::new(()),
            setup_transaction: Mutex::new(()),
            cmcloud_enrollment_transition: Mutex::new(()),
            private_gateway_bootstrap: private_bootstrap,
            runtime_log,
            agent_log_error_code: Mutex::new(initial_agent_log_error_code),
            gateway_log_health: Mutex::new(GatewayRuntimeLogHealth {
                capture_error_code: None,
                write_error_code: initial_gateway_log_error_code,
            }),
            gateway_session,
            management_web: Mutex::new(management_web),
            management_web_shutdown: Mutex::new(None),
            management_web_config,
            management_access,
            control_endpoint,
            paths: config.paths.clone(),
            config_file: config.config_file.clone(),
            setup_transaction_file: setup_transaction_file(&config.paths),
            cmcloud_endpoint: Some(config.cmcloud.as_ref().map_or_else(
                || CMCLOUD_PRODUCTION_URL.to_owned(),
                |cmcloud| cmcloud.agent_websocket_url.clone(),
            )),
            secrets,
            setup,
            setup_gate_required,
            updates,
            web_state,
            shutdown_requested: AtomicBool::new(false),
            started_at,
            latest_error_code: Mutex::new(initial_log_error_code),
        };
        controller.log_agent_code(LogLevel::Info, "AGENT_RUNTIME_READY");
        Ok(controller)
    }

    fn status(&self) -> Result<ControlStatus, ControlError> {
        let _ = self.tick_supervisor()?;
        let status = self.control_status_snapshot()?;
        self.web_state
            .update_lifecycle(agent_lifecycle_status(&status))?;
        Ok(status)
    }

    fn publish_lifecycle_snapshot(&self) -> Result<(), ControlError> {
        let status = self.control_status_snapshot()?;
        self.web_state
            .update_lifecycle(agent_lifecycle_status(&status))
    }

    fn control_status_snapshot(&self) -> Result<ControlStatus, ControlError> {
        let setup_blocked = self.setup_blocked()?;
        let mut supervisor = self
            .supervisor
            .lock()
            .map_err(|_| ControlError::CommandFailed)?;
        let lifecycle = match supervisor.as_mut() {
            Some(supervisor) => match supervisor.status() {
                GatewayStatus::Stopped => GatewayControlStatus::Stopped,
                GatewayStatus::Running { .. } => GatewayControlStatus::Running,
                GatewayStatus::Backoff { .. } => GatewayControlStatus::Backoff,
            },
            None => GatewayControlStatus::Stopped,
        };
        drop(supervisor);
        let gateway_route = self.gateway_session.snapshot();
        let gateway = match lifecycle {
            GatewayControlStatus::Running
                if gateway_route
                    .as_ref()
                    .is_some_and(gateway_health_with_route) =>
            {
                GatewayControlStatus::Running
            }
            GatewayControlStatus::Running => GatewayControlStatus::Degraded,
            status => status,
        };
        let (management_web, management_web_url) = self
            .management_web
            .lock()
            .map_err(|_| ControlError::CommandFailed)?
            .as_ref()
            .map_or((ManagementWebControlStatus::Disabled, None), |service| {
                (
                    ManagementWebControlStatus::Running,
                    Some(service.advertised_url().trim_end_matches('/').to_owned()),
                )
            });
        let remembered_error_code = self
            .latest_error_code
            .lock()
            .map_err(|_| ControlError::CommandFailed)?
            .clone();
        let persistent_log_error_code = self
            .agent_log_error_code
            .lock()
            .map_err(|_| ControlError::CommandFailed)?
            .clone()
            .or(self.current_gateway_log_error_code()?);
        let latest_error_code = if setup_blocked {
            Some(String::from("SETUP_REQUIRED"))
        } else if persistent_log_error_code.is_some() {
            persistent_log_error_code
        } else {
            match gateway {
                GatewayControlStatus::Backoff => Some(String::from("GATEWAY_RESTART_BACKOFF")),
                GatewayControlStatus::Degraded => Some(String::from("GATEWAY_HEALTH_DEGRADED")),
                _ => remembered_error_code,
            }
        };
        Ok(ControlStatus {
            schema_version: 3,
            agent: String::from("running"),
            identity: self.identity.clone(),
            gateway,
            management_web,
            management_web_url,
            uptime_seconds: self.started_at.elapsed().as_secs(),
            latest_error_code,
        })
    }

    fn setup_blocked(&self) -> Result<bool, ControlError> {
        if !self.setup_gate_required {
            return Ok(false);
        }
        Ok(self
            .setup
            .status()
            .map_err(|_| ControlError::CommandFailed)?
            .setup_required)
    }

    fn enable_management_web(&self) -> Result<ControlStatus, ControlError> {
        self.ensure_resource_start_allowed()?;
        if !self.reap_management_web_shutdown()? {
            return Err(ControlError::ResourceExhausted);
        }
        let mut management_web = self
            .management_web
            .lock()
            .map_err(|_| ControlError::CommandFailed)?;
        self.ensure_resource_start_allowed()?;
        if management_web.is_none() {
            let service = ManagementWebService::start(
                &self.management_web_config,
                agent_web_router(Arc::clone(&self.web_state)),
                self.management_access.clone(),
                self.gateway_session.clone(),
            )
            .map_err(|_| ControlError::CommandFailed)?;
            self.web_state
                .attach_management_setup_state(service.setup_state())?;
            *management_web = Some(service);
        }
        drop(management_web);
        self.status()
    }

    fn disable_management_web(&self) -> Result<ControlStatus, ControlError> {
        let shutdown_ready = self.reap_management_web_shutdown()?;
        if !shutdown_ready {
            return self.status();
        }
        let service = self
            .management_web
            .lock()
            .map_err(|_| ControlError::CommandFailed)?
            .take();
        if let Some(service) = service {
            let worker = thread::Builder::new()
                .name(String::from("cmclient-management-web-shutdown"))
                .spawn(move || {
                    let mut service = service;
                    service.stop()
                })
                .map_err(|_| ControlError::CommandFailed)?;
            *self
                .management_web_shutdown
                .lock()
                .map_err(|_| ControlError::CommandFailed)? = Some(worker);
        }
        self.status()
    }

    fn reap_management_web_shutdown(&self) -> Result<bool, ControlError> {
        let worker = {
            let mut shutdown = self
                .management_web_shutdown
                .lock()
                .map_err(|_| ControlError::CommandFailed)?;
            if shutdown
                .as_ref()
                .is_some_and(|worker| !worker.is_finished())
            {
                return Ok(false);
            }
            shutdown.take()
        };
        match worker {
            Some(worker) => worker
                .join()
                .map_err(|_| ControlError::CommandFailed)?
                .map_err(|_| ControlError::CommandFailed),
            None => Ok(()),
        }?;
        Ok(true)
    }

    fn stop_management_web(&self) -> Result<(), ControlError> {
        let service = self
            .management_web
            .lock()
            .map_err(|_| ControlError::CommandFailed)?
            .take();
        let service_result = service.map_or(Ok(()), |mut service| {
            service.stop().map_err(|_| ControlError::CommandFailed)
        });
        let worker = self
            .management_web_shutdown
            .lock()
            .map_err(|_| ControlError::CommandFailed)?
            .take();
        let worker_result = worker.map_or(Ok(()), |worker| {
            worker
                .join()
                .map_err(|_| ControlError::CommandFailed)?
                .map_err(|_| ControlError::CommandFailed)
        });
        service_result.and(worker_result)
    }

    fn request_shutdown(&self) {
        if self.shutdown_requested.swap(true, Ordering::AcqRel) {
            return;
        }
        self.log_agent_code(LogLevel::Info, "AGENT_SHUTDOWN_REQUESTED");
        let endpoint = self.control_endpoint.clone();
        let _ = thread::Builder::new()
            .name(String::from("cmclient-control-shutdown-wake"))
            .spawn(move || {
                let _ = ControlClient::new_with_timeout(endpoint, Duration::from_secs(2))
                    .and_then(|client| client.status());
            });
    }

    fn is_shutdown_requested(&self) -> bool {
        self.shutdown_requested.load(Ordering::Acquire)
    }

    fn ensure_resource_start_allowed(&self) -> Result<(), ControlError> {
        if self.is_shutdown_requested() {
            Err(ControlError::CommandFailed)
        } else {
            Ok(())
        }
    }

    fn remember_error(&self, error: &ControlError) {
        self.remember_error_code(error.code());
    }

    fn remember_error_code(&self, code: &str) {
        let sink_available = self.runtime_log.is_some();
        let log_error_code = self.write_agent_code(LogLevel::Error, code);
        if let Ok(mut latest_error_code) = self.latest_error_code.lock() {
            if let Ok(mut agent_log_error_code) = self.agent_log_error_code.lock() {
                if let Some(log_error_code) = log_error_code {
                    let log_error_code = String::from(log_error_code);
                    *latest_error_code = Some(log_error_code.clone());
                    *agent_log_error_code = Some(log_error_code);
                } else {
                    if sink_available {
                        *agent_log_error_code = None;
                    }
                    *latest_error_code = Some(String::from(code));
                }
            } else {
                *latest_error_code = Some(String::from(log_error_code.unwrap_or(code)));
            }
        }
    }

    fn apply_gateway_log_health(&self, update: GatewayLogHealthUpdate) {
        let (reported_error_code, recovered_error_code, current_gateway_error_code) = {
            let Ok(mut health) = self.gateway_log_health.lock() else {
                return;
            };
            if let Some(code) = update.capture_error_code {
                health.capture_error_code = Some(String::from(code));
            }
            if let Some(code) = update.write_error_code {
                health.write_error_code = Some(String::from(code));
            }
            let recovered_error_code = update.write_recovered_code.and_then(|recovered| {
                if health.write_error_code.as_deref() == Some(recovered) {
                    health.write_error_code.take()
                } else {
                    None
                }
            });
            let reported_error_code = update.capture_error_code.map(String::from).or_else(|| {
                update.write_error_code.and_then(|code| {
                    (health.write_error_code.as_deref() == Some(code)).then(|| String::from(code))
                })
            });
            (
                reported_error_code,
                recovered_error_code,
                health.current_error_code(),
            )
        };

        if let Some(code) = reported_error_code {
            self.remember_error_code(&code);
            return;
        }
        let Some(recovered_error_code) = recovered_error_code else {
            return;
        };
        let agent_log_error_code = self
            .agent_log_error_code
            .lock()
            .ok()
            .and_then(|code| code.clone());
        if let Ok(mut latest_error_code) = self.latest_error_code.lock() {
            if latest_error_code.as_deref() == Some(recovered_error_code.as_str()) {
                *latest_error_code = agent_log_error_code.or(current_gateway_error_code);
            }
        }
    }

    fn current_gateway_log_error_code(&self) -> Result<Option<String>, ControlError> {
        self.gateway_log_health
            .lock()
            .map_err(|_| ControlError::CommandFailed)
            .map(|health| health.current_error_code())
    }

    fn clear_error(&self) {
        if let Ok(mut latest_error_code) = self.latest_error_code.lock() {
            if latest_error_code
                .as_deref()
                .is_none_or(|code| !is_runtime_log_error(code))
            {
                *latest_error_code = None;
            }
        }
    }

    fn log_agent_code(&self, level: LogLevel, code: &str) {
        let sink_available = self.runtime_log.is_some();
        let log_error_code = self.write_agent_code(level, code);
        let gateway_log_error_code = self.current_gateway_log_error_code().ok().flatten();
        if let Ok(mut latest_error_code) = self.latest_error_code.lock() {
            if let Ok(mut agent_log_error_code) = self.agent_log_error_code.lock() {
                if let Some(error_code) = log_error_code {
                    let error_code = String::from(error_code);
                    *latest_error_code = Some(error_code.clone());
                    *agent_log_error_code = Some(error_code);
                } else if sink_available {
                    let owns_latest =
                        latest_error_code.as_deref() == agent_log_error_code.as_deref();
                    let recovered = if owns_latest {
                        *latest_error_code = gateway_log_error_code;
                        true
                    } else {
                        true
                    };
                    if recovered {
                        *agent_log_error_code = None;
                    }
                }
            } else if let Some(error_code) = log_error_code {
                *latest_error_code = Some(String::from(error_code));
            }
        }
    }

    fn write_agent_code(&self, level: LogLevel, code: &str) -> Option<&'static str> {
        let sink = self.runtime_log.as_ref()?;
        let write_error = sink.write_code(level, code).err().map(|error| error.code());
        let latched_error = sink.take_error_code();
        write_error.or(latched_error)
    }

    fn has_precise_supervisor_error(&self) -> bool {
        self.latest_error_code
            .lock()
            .is_ok_and(|latest_error_code| {
                latest_error_code.as_deref().is_some_and(|code| {
                    code.starts_with("GATEWAY_SUPERVISOR_") || is_runtime_log_error(code)
                })
            })
    }

    fn has_setup_required_error(&self) -> bool {
        self.latest_error_code
            .lock()
            .is_ok_and(|latest_error_code| latest_error_code.as_deref() == Some("SETUP_REQUIRED"))
    }

    fn invalidate_persisted_callmesh_credential(&self) {
        let Ok(_transaction) = self.setup_transaction.try_lock() else {
            return;
        };
        self.gateway_session.clear();
        let mut failed = self.secrets.remove(SecretKind::CallMeshApiKey).is_err();
        match self.supervisor.lock() {
            Ok(mut supervisor) => {
                if let Some(supervisor) = supervisor.as_mut() {
                    supervisor.clear_callmesh_api_key();
                }
            }
            Err(_) => failed = true,
        }
        match self.setup.require_credentials() {
            Ok(status) => {
                if self.web_state.publish_setup_status(&status).is_err() {
                    failed = true;
                }
            }
            Err(_) => failed = true,
        }
        if failed {
            self.remember_error_code("SETUP_CREDENTIAL_INVALIDATION_FAILED");
        }
    }

    fn operational_reset(&self) -> Result<SetupStatus, SetupApplyError> {
        let _reset_transaction = self
            .setup_transaction
            .lock()
            .map_err(|_| SetupApplyError::SupervisorUnavailable)?;
        if !self.setup_gate_required {
            return Err(SetupError::TransitionInvalid.into());
        }
        let current = self.setup.status()?;
        if matches!(current.phase, SetupPhase::Validating) {
            return Err(SetupError::TransitionInvalid.into());
        }
        validate_reset_paths(&self.paths, &self.config_file)?;
        let target_generation = self.setup.next_reset_generation()?;
        write_reset_transaction(&self.paths, ResetKind::Operational, target_generation)?;

        // Hold the transition lock across the child stop and generation change
        // so the supervisor worker cannot revive the old external owner.
        let _gateway_transition = self
            .gateway_transition
            .lock()
            .map_err(|_| SetupApplyError::SupervisorUnavailable)?;
        self.stop_supervisor_locked()
            .map_err(|_| SetupApplyError::SupervisorUnavailable)?;
        if let Some(supervisor) = self
            .supervisor
            .lock()
            .map_err(|_| SetupApplyError::SupervisorUnavailable)?
            .as_mut()
        {
            supervisor.clear_callmesh_api_key();
        }

        let result = complete_operational_reset(
            &self.paths,
            &self.config_file,
            &self.secrets,
            &self.setup,
            target_generation,
        );
        let committed_status = self.setup.status().ok().filter(|status| {
            status.terms_required
                && self
                    .setup
                    .generation()
                    .ok()
                    .is_some_and(|generation| generation.generation() == target_generation)
        });
        if let Some(status) = committed_status.as_ref() {
            self.web_state
                .publish_setup_status(status)
                .map_err(|_| SetupApplyError::ConfigWriteFailed)?;
        }
        let status = result?;
        remove_reset_transaction(&self.paths)?;
        self.web_state.record_audit(
            "operational_reset",
            "allowed",
            "OPERATIONAL_RESET_COMPLETED",
        );
        self.web_state
            .record_audit("setup_generation", "changed", "SETUP_GENERATION_CHANGED");
        Ok(status)
    }

    fn stop_supervisor(&self) -> Result<bool, ControlError> {
        let _transition = self
            .gateway_transition
            .lock()
            .map_err(|_| ControlError::CommandFailed)?;
        self.stop_supervisor_locked()
    }

    fn stop_supervisor_locked(&self) -> Result<bool, ControlError> {
        self.gateway_session.clear();
        let (result, log_health) = {
            let mut supervisor = self
                .supervisor
                .lock()
                .map_err(|_| ControlError::CommandFailed)?;
            let Some(supervisor) = supervisor.as_mut() else {
                return Ok(false);
            };
            let result = supervisor.stop();
            let log_health = supervisor.take_log_health_update();
            (result, log_health)
        };
        self.apply_gateway_log_health(log_health);
        result.map_err(|error| {
            self.remember_error_code(error.code());
            ControlError::CommandFailed
        })?;
        self.log_agent_code(LogLevel::Info, "GATEWAY_SUPERVISOR_STOPPED");
        Ok(true)
    }

    /// Caller holds `gateway_transition`, preventing a setup-state change from
    /// racing a new Gateway spawn while the existing owner is being drained.
    fn stop_supervisor_for_setup_block_locked(&self) -> Result<bool, ControlError> {
        let route_was_active = self.gateway_session.snapshot().is_some();
        self.gateway_session.clear();
        let (result, log_health) = {
            let mut supervisor = self
                .supervisor
                .lock()
                .map_err(|_| ControlError::CommandFailed)?;
            let Some(supervisor) = supervisor.as_mut() else {
                return Ok(route_was_active);
            };
            if matches!(supervisor.status(), GatewayStatus::Stopped) {
                (Ok(false), supervisor.take_log_health_update())
            } else {
                (
                    supervisor.stop().map(|_| true),
                    supervisor.take_log_health_update(),
                )
            }
        };
        self.apply_gateway_log_health(log_health);
        let stopped = result.map_err(|error| {
            self.remember_error_code(error.code());
            ControlError::CommandFailed
        })?;
        if stopped {
            self.log_agent_code(LogLevel::Info, "GATEWAY_SUPERVISOR_STOPPED");
        }
        Ok(route_was_active || stopped)
    }

    fn start_supervisor(&self) -> Result<bool, ControlError> {
        self.start_supervisor_inner(false)
    }

    fn start_supervisor_for_setup(&self) -> Result<bool, ControlError> {
        self.start_supervisor_inner(true)
    }

    fn start_supervisor_inner(&self, allow_validating: bool) -> Result<bool, ControlError> {
        self.ensure_setup_start_allowed(allow_validating)?;
        let _transition = self
            .gateway_transition
            .lock()
            .map_err(|_| ControlError::CommandFailed)?;
        self.ensure_setup_start_allowed(allow_validating)?;
        self.ensure_resource_start_allowed()?;
        let (result, ready, log_health) = {
            let mut supervisor = self
                .supervisor
                .lock()
                .map_err(|_| ControlError::CommandFailed)?;
            self.ensure_resource_start_allowed()?;
            let Some(supervisor) = supervisor.as_mut() else {
                return Ok(false);
            };
            self.synchronize_supervisor_setup_generation(supervisor)?;
            // A manual Start must not turn a crash-loop backoff into an
            // unbounded restart source. `tick` resumes only after its owned
            // deadline, while stopped and already-running supervisors retain
            // the normal Start semantics.
            let result = match supervisor.status() {
                GatewayStatus::Backoff { .. } => supervisor.tick(),
                GatewayStatus::Stopped | GatewayStatus::Running { .. } => supervisor.start(),
            };
            let ready = supervisor.gateway_ready().cloned();
            let log_health = supervisor.take_log_health_update();
            (result, ready, log_health)
        };
        self.apply_gateway_log_health(log_health);
        let event = result.map_err(|error| {
            self.gateway_session.clear();
            let code = error.code();
            self.remember_error_code(code);
            if !allow_validating && code == "CALLMESH_CREDENTIAL_REJECTED" {
                self.invalidate_persisted_callmesh_credential();
            }
            if allow_validating
                && matches!(
                    code,
                    "CALLMESH_CREDENTIAL_REJECTED"
                        | "CALLMESH_UNAVAILABLE"
                        | "SETUP_MESHTASTIC_UNREACHABLE"
                )
            {
                // Setup classification must not be masked by a secondary log
                // sink failure while the private bootstrap is unwinding.
                if let Ok(mut latest_error_code) = self.latest_error_code.lock() {
                    *latest_error_code = Some(String::from(code));
                }
            }
            ControlError::CommandFailed
        })?;
        if self.private_gateway_bootstrap
            && (matches!(event, SupervisorEvent::Started { .. })
                || (matches!(event, SupervisorEvent::Heartbeat { .. })
                    && self.gateway_session.snapshot().is_none()))
        {
            self.publish_verified_gateway(ready)?;
        }
        if matches!(event, SupervisorEvent::Started { .. }) {
            self.log_agent_code(LogLevel::Info, "GATEWAY_SUPERVISOR_STARTED");
        }
        Ok(true)
    }

    fn start_resident_supervisor(&self) -> Result<bool, ControlError> {
        if self.setup_blocked()? {
            return Ok(false);
        }
        self.start_supervisor()
    }

    fn tick_supervisor(&self) -> Result<bool, ControlError> {
        let _transition = self
            .gateway_transition
            .lock()
            .map_err(|_| ControlError::CommandFailed)?;
        if self.setup_blocked()? {
            return self.stop_supervisor_for_setup_block_locked();
        }
        if self.is_shutdown_requested() {
            return Ok(false);
        }
        let (result, ready, log_health) = {
            let mut supervisor = self
                .supervisor
                .lock()
                .map_err(|_| ControlError::CommandFailed)?;
            if self.is_shutdown_requested() {
                return Ok(false);
            }
            match supervisor.as_mut() {
                Some(supervisor) => {
                    self.synchronize_supervisor_setup_generation(supervisor)?;
                    let result = supervisor.tick().map(Some);
                    let ready = supervisor.gateway_ready().cloned();
                    let log_health = supervisor.take_log_health_update();
                    (result, ready, log_health)
                }
                None => (Ok(None), None, GatewayLogHealthUpdate::default()),
            }
        };
        self.apply_gateway_log_health(log_health);
        match result {
            Ok(Some(SupervisorEvent::Started { .. })) => {
                if self.private_gateway_bootstrap {
                    self.publish_verified_gateway(ready)?;
                }
                if let Ok(mut latest_error_code) = self.latest_error_code.lock() {
                    if latest_error_code
                        .as_deref()
                        .is_some_and(is_transient_supervisor_error)
                    {
                        *latest_error_code = None;
                    }
                }
                self.log_agent_code(LogLevel::Info, "GATEWAY_SUPERVISOR_STARTED");
                Ok(true)
            }
            Ok(Some(SupervisorEvent::Heartbeat { .. })) => {
                let route_was_missing =
                    self.private_gateway_bootstrap && self.gateway_session.snapshot().is_none();
                if route_was_missing {
                    self.publish_verified_gateway(ready)?;
                }
                if let Ok(mut latest_error_code) = self.latest_error_code.lock() {
                    if latest_error_code
                        .as_deref()
                        .is_some_and(is_transient_supervisor_error)
                    {
                        *latest_error_code = None;
                    }
                }
                Ok(route_was_missing)
            }
            Ok(Some(SupervisorEvent::Exited { .. })) => {
                self.gateway_session.clear();
                self.log_agent_code(LogLevel::Warn, "GATEWAY_SUPERVISOR_EXITED");
                Ok(true)
            }
            Ok(Some(SupervisorEvent::Backoff { .. } | SupervisorEvent::Stopped)) => {
                self.gateway_session.clear();
                Ok(true)
            }
            Ok(None) => {
                let changed = self.gateway_session.snapshot().is_some();
                self.gateway_session.clear();
                Ok(changed)
            }
            Err(error) => {
                self.gateway_session.clear();
                self.remember_error_code(error.code());
                Err(ControlError::CommandFailed)
            }
        }
    }

    fn synchronize_supervisor_setup_generation(
        &self,
        supervisor: &mut GatewaySupervisor,
    ) -> Result<(), ControlError> {
        if !self.private_gateway_bootstrap
            || matches!(supervisor.status(), GatewayStatus::Running { .. })
        {
            return Ok(());
        }
        let generation = self
            .setup
            .generation()
            .map_err(|_| ControlError::CommandFailed)?
            .generation();
        supervisor
            .set_setup_generation(generation)
            .map_err(|_| ControlError::CommandFailed)
    }

    fn ensure_setup_start_allowed(&self, allow_validating: bool) -> Result<(), ControlError> {
        if !self.setup_gate_required {
            return Ok(());
        }
        let status = self
            .setup
            .status()
            .map_err(|_| ControlError::CommandFailed)?;
        if !status.setup_required
            || (allow_validating && matches!(status.phase, SetupPhase::Validating))
        {
            return Ok(());
        }
        self.remember_error_code("SETUP_REQUIRED");
        Err(ControlError::CommandFailed)
    }

    fn publish_verified_gateway(&self, ready: Option<GatewayReady>) -> Result<(), ControlError> {
        let result = ready
            .ok_or(ControlError::CommandFailed)
            .and_then(|ready| verified_gateway_route(&ready, &self.identity))
            .map_err(|_| ControlError::CommandFailed);
        match result {
            Ok(route) => {
                self.gateway_session.set(route);
                Ok(())
            }
            Err(error) => {
                self.gateway_session.clear();
                // Start and tick both hold `gateway_transition` while calling
                // this method, so the verified route and child teardown share
                // one ownership transition. Never leave a failed identity
                // check with a live, un-routable Gateway.
                let cleanup = self.stop_supervisor_locked();
                self.remember_error_code("GATEWAY_SUPERVISOR_IDENTITY_VERIFICATION_FAILED");
                cleanup?;
                Err(error)
            }
        }
    }
}

fn shutdown_agent_runtime(
    controller: &AgentController,
    supervisor_worker: &mut SupervisorWorker,
) -> Result<(), ControlError> {
    supervisor_worker.stop();
    let supervisor_result = controller.stop_supervisor().map(|_| ());
    let management_web_result = controller.stop_management_web();
    let result = supervisor_result.and(management_web_result);
    if result.is_ok() {
        controller.log_agent_code(LogLevel::Info, "AGENT_RUNTIME_STOPPED");
    }
    result
}

fn remember_initial_log_error(current: &mut Option<String>, code: &'static str) {
    if current.is_none() {
        *current = Some(String::from(code));
    }
}

fn is_runtime_log_error(code: &str) -> bool {
    code.starts_with("RUNTIME_LOG_")
}

fn is_transient_supervisor_error(code: &str) -> bool {
    matches!(
        code,
        "GATEWAY_SUPERVISOR_SPAWN_FAILED" | "GATEWAY_SUPERVISOR_PROCESS_IO_FAILED"
    )
}

impl ControlHandler for AgentController {
    fn handle(&self, command: ControlCommand) -> Result<ControlStatus, ControlError> {
        let result = match command {
            ControlCommand::Status => self.status(),
            ControlCommand::Start => {
                self.start_supervisor()?
                    .then_some(())
                    .ok_or(ControlError::CommandFailed)?;
                self.status()
            }
            ControlCommand::Stop => {
                self.stop_supervisor()?
                    .then_some(())
                    .ok_or(ControlError::CommandFailed)?;
                self.status()
            }
            ControlCommand::Restart => {
                self.ensure_resource_start_allowed()?;
                self.stop_supervisor()?
                    .then_some(())
                    .ok_or(ControlError::CommandFailed)?;
                self.start_supervisor()?
                    .then_some(())
                    .ok_or(ControlError::CommandFailed)?;
                self.status()
            }
            ControlCommand::OperationalReset => {
                self.operational_reset()
                    .map_err(|_| ControlError::CommandFailed)?;
                self.status()
            }
            ControlCommand::ShutdownAgent => {
                self.request_shutdown();
                self.status()
            }
            ControlCommand::EnableManagementWeb => self.enable_management_web(),
            ControlCommand::DisableManagementWeb => self.disable_management_web(),
        };
        match &result {
            Ok(_) if !matches!(command, ControlCommand::Status) => self.clear_error(),
            Ok(_) => {}
            Err(ControlError::CommandFailed) if self.has_setup_required_error() => {}
            Err(ControlError::CommandFailed) if self.has_precise_supervisor_error() => {}
            Err(error) => self.remember_error(error),
        }
        result
    }

    fn prepare_command(&self, command: ControlCommand) -> Result<ControlStatus, ControlError> {
        if command != ControlCommand::ShutdownAgent {
            return self.handle(command);
        }
        let result = self.status();
        match &result {
            Ok(_) => self.clear_error(),
            Err(ControlError::CommandFailed) if self.has_precise_supervisor_error() => {}
            Err(error) => self.remember_error(error),
        }
        result
    }

    fn command_response_sent(&self, command: ControlCommand) {
        if command == ControlCommand::ShutdownAgent {
            self.request_shutdown();
        }
    }

    fn update_status(&self) -> Result<UpdateControlStatus, ControlError> {
        self.updates.status()
    }

    fn subscribe_update_events(&self) -> Result<mpsc::Receiver<ControlUpdateEvent>, ControlError> {
        self.updates.subscribe()
    }

    fn diagnostics_bundle(&self) -> Result<DiagnosticsControlBundle, ControlError> {
        let status = self.status()?;
        let update = self.updates.status()?;
        Ok(DiagnosticsControlBundle {
            schema_version: 2,
            identity: status.identity,
            gateway: status.gateway,
            management_web: status.management_web,
            latest_error_code: status.latest_error_code,
            update_error_code: update.job.as_ref().and_then(|job| job.error_code.clone()),
            update_log_codes: update.job.map_or_else(Vec::new, |job| job.recent_log_codes),
        })
    }

    fn gateway_projection(
        &self,
        projection: GatewayProjection,
    ) -> Result<serde_json::Value, ControlError> {
        let route = self
            .gateway_session
            .snapshot()
            .ok_or(ControlError::CommandFailed)?;
        gateway_json_projection(&route, projection)
    }

    fn subscribe_gateway_events(&self) -> Result<mpsc::Receiver<ControlUpdateEvent>, ControlError> {
        Ok(bridge_gateway_events(self.gateway_session.clone()))
    }

    fn store_secret(&self, kind: ControlSecretKind, value: &str) -> Result<(), ControlError> {
        if matches!(
            kind,
            ControlSecretKind::AprsPasscode | ControlSecretKind::ManagementAdminToken
        ) {
            return Err(ControlError::SecretKindDeprecated);
        }
        let _setup_transaction = (kind == ControlSecretKind::CallMeshApiKey)
            .then(|| self.setup_transaction.try_lock())
            .transpose()
            .map_err(|_| ControlError::ResourceExhausted)?;
        self.secrets
            .store(secret_kind(kind), value)
            .map_err(control_secret_error)
    }

    fn remove_secret(&self, kind: ControlSecretKind) -> Result<bool, ControlError> {
        let _setup_transaction = (kind == ControlSecretKind::CallMeshApiKey)
            .then(|| self.setup_transaction.try_lock())
            .transpose()
            .map_err(|_| ControlError::ResourceExhausted)?;
        self.secrets
            .remove(secret_kind(kind))
            .map_err(control_secret_error)
    }

    fn cmcloud_enrollment_status(&self) -> Result<serde_json::Value, ControlError> {
        let status =
            AgentController::cmcloud_enrollment_status(self).map_err(cmcloud_control_error)?;
        serde_json::to_value(status).map_err(|_| ControlError::InvalidEnvelope)
    }

    fn enroll_cmcloud(&self, pairing_code: &str) -> Result<serde_json::Value, ControlError> {
        let status = AgentController::enroll_cmcloud(
            self,
            CMCloudEnrollmentRequest {
                pairing_code: pairing_code.to_owned(),
            },
        )
        .map_err(cmcloud_control_error)?;
        serde_json::to_value(status).map_err(|_| ControlError::InvalidEnvelope)
    }
}

fn cmcloud_control_error(error: CMCloudEnrollmentControlError) -> ControlError {
    ControlError::Application(error.code().to_owned())
}

fn verified_gateway_route(
    ready: &GatewayReady,
    expected: &cmclient_control_api::ComponentIdentityReport,
) -> Result<GatewayRoute, ControlError> {
    let route = GatewayRoute::new(ready.address, ready.capability.clone())
        .map_err(|_| ControlError::CommandFailed)?;
    let client = reqwest::blocking::Client::builder()
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(3))
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .build()
        .map_err(|_| ControlError::CommandFailed)?;
    let active_route = route.active().ok_or(ControlError::CommandFailed)?;
    let response = client
        .get(format!(
            "http://{}/api/v1/system/version",
            active_route.address()
        ))
        .header("accept", "application/json")
        .header(GATEWAY_CAPABILITY_HEADER, active_route.capability())
        .send()
        .map_err(map_gateway_request_error)?;
    if !response.status().is_success()
        || response
            .content_length()
            .is_some_and(|length| length > MAX_GATEWAY_IDENTITY_BYTES)
    {
        return Err(ControlError::CommandFailed);
    }
    let mut bytes = Vec::new();
    response
        .take(MAX_GATEWAY_IDENTITY_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(map_gateway_io_error)?;
    if !route.is_active() {
        return Err(ControlError::CommandFailed);
    }
    if bytes.len() as u64 > MAX_GATEWAY_IDENTITY_BYTES {
        return Err(ControlError::ResponseTooLarge);
    }
    let actual: cmclient_control_api::ComponentIdentityReport =
        serde_json::from_slice(&bytes).map_err(|_| ControlError::InvalidEnvelope)?;
    if !route.is_active()
        || actual.validate().is_err()
        || actual.component != InternalComponent::Gateway
        || actual.identity != expected.identity
    {
        return Err(ControlError::CommandFailed);
    }
    drop(active_route);
    Ok(route)
}

fn gateway_json_projection(
    route: &GatewayRoute,
    projection: GatewayProjection,
) -> Result<serde_json::Value, ControlError> {
    const MAX_GATEWAY_PROJECTION_BYTES: u64 = 2 * 1024 * 1024;
    let (method, path) = match projection {
        GatewayProjection::Meshtastic => (reqwest::Method::GET, "/api/v1/meshtastic"),
        GatewayProjection::Nodes => (reqwest::Method::GET, "/api/v1/nodes"),
        GatewayProjection::Positions => (reqwest::Method::GET, "/api/v1/positions"),
        GatewayProjection::Aprs => (reqwest::Method::GET, "/api/v1/aprs"),
        GatewayProjection::CallMesh => (reqwest::Method::GET, "/api/v1/callmesh"),
        GatewayProjection::Proxy => (reqwest::Method::GET, "/api/v1/proxy"),
        GatewayProjection::RecentEvents => (reqwest::Method::GET, "/api/v1/events/recent"),
        GatewayProjection::DatabaseIntegrity => {
            (reqwest::Method::POST, "/api/v1/diagnostics/integrity-check")
        }
        GatewayProjection::Backup => (reqwest::Method::POST, "/api/v1/backups"),
    };
    let client = reqwest::blocking::Client::builder()
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .build()
        .map_err(|_| ControlError::CommandFailed)?;
    let active_route = route.active().ok_or(ControlError::CommandFailed)?;
    let response = client
        .request(method, format!("http://{}{path}", active_route.address()))
        .header("accept", "application/json")
        .header(GATEWAY_CAPABILITY_HEADER, active_route.capability())
        .send()
        .map_err(map_gateway_request_error)?;
    if matches!(response.status().as_u16(), 408 | 504) {
        return Err(ControlError::Timeout);
    }
    if !response.status().is_success() {
        return Err(ControlError::CommandFailed);
    }
    if response
        .content_length()
        .is_some_and(|length| length > MAX_GATEWAY_PROJECTION_BYTES)
    {
        return Err(ControlError::ResponseTooLarge);
    }
    let mut bytes = Vec::new();
    response
        .take(MAX_GATEWAY_PROJECTION_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(map_gateway_io_error)?;
    if !route.is_active() {
        return Err(ControlError::CommandFailed);
    }
    if bytes.len() as u64 > MAX_GATEWAY_PROJECTION_BYTES {
        return Err(ControlError::ResponseTooLarge);
    }
    let projection = serde_json::from_slice(&bytes).map_err(|_| ControlError::InvalidEnvelope)?;
    if !route.is_active() {
        return Err(ControlError::CommandFailed);
    }
    drop(active_route);
    Ok(projection)
}

fn gateway_cmcloud_account_projection(
    route: &GatewayRoute,
) -> Result<CMCloudAccountProjection, CMCloudAccountProjectionControlError> {
    const MAX_PROJECTION_BYTES: u64 = 256 * 1024;
    let client = reqwest::blocking::Client::builder()
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(5))
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .build()
        .map_err(|_| CMCloudAccountProjectionControlError::Unavailable)?;
    let active_route = route
        .active()
        .ok_or(CMCloudAccountProjectionControlError::Unavailable)?;
    let response = client
        .get(format!(
            "http://{}/api/v1/cmcloud/account-projection",
            active_route.address()
        ))
        .header("accept", "application/json")
        .header(GATEWAY_CAPABILITY_HEADER, active_route.capability())
        .send()
        .map_err(|_| CMCloudAccountProjectionControlError::Unavailable)?;
    if !route.is_active() {
        return Err(CMCloudAccountProjectionControlError::Unavailable);
    }
    if response
        .content_length()
        .is_some_and(|length| length > MAX_PROJECTION_BYTES)
    {
        return Err(CMCloudAccountProjectionControlError::Unavailable);
    }
    let response_status = response.status();
    let mut bytes = Vec::new();
    response
        .take(MAX_PROJECTION_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| CMCloudAccountProjectionControlError::Unavailable)?;
    if bytes.len() as u64 > MAX_PROJECTION_BYTES || !route.is_active() {
        return Err(CMCloudAccountProjectionControlError::Unavailable);
    }
    if !response_status.is_success() {
        let code = serde_json::from_slice::<serde_json::Value>(&bytes)
            .ok()
            .and_then(|value| {
                value
                    .get("code")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            });
        return Err(match code {
            Some(code) if code == "ACCOUNT_PROJECTION_AMBIGUOUS" => {
                CMCloudAccountProjectionControlError::Ambiguous
            }
            Some(code) if code == "ACCOUNT_PROJECTION_STALE" => {
                CMCloudAccountProjectionControlError::Stale
            }
            _ => CMCloudAccountProjectionControlError::Unavailable,
        });
    }
    serde_json::from_slice(&bytes).map_err(|_| CMCloudAccountProjectionControlError::Unavailable)
}

fn gateway_health_with_route(route: &GatewayRoute) -> bool {
    const MAX_GATEWAY_HEALTH_BYTES: u64 = 4 * 1024;
    let Some(active_route) = route.active() else {
        return false;
    };
    let result = (|| {
        let client = reqwest::blocking::Client::builder()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(2))
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .build()
            .ok()?;
        let response = client
            .get(format!(
                "http://{}/api/v1/system/health",
                active_route.address()
            ))
            .header("accept", "application/json")
            .header(GATEWAY_CAPABILITY_HEADER, active_route.capability())
            .send()
            .ok()?;
        if !response.status().is_success()
            || response
                .content_length()
                .is_some_and(|length| length > MAX_GATEWAY_HEALTH_BYTES)
        {
            return None;
        }
        let mut body = Vec::new();
        response
            .take(MAX_GATEWAY_HEALTH_BYTES + 1)
            .read_to_end(&mut body)
            .ok()?;
        if !route.is_active() {
            return None;
        }
        if body.len() as u64 > MAX_GATEWAY_HEALTH_BYTES {
            return None;
        }
        let body = serde_json::from_slice::<serde_json::Value>(&body).ok()?;
        (route.is_active() && body.get("status").and_then(serde_json::Value::as_str) == Some("ok"))
            .then_some(())
    })();
    drop(active_route);
    result.is_some()
}

fn map_gateway_request_error(error: reqwest::Error) -> ControlError {
    if error.is_timeout() {
        ControlError::Timeout
    } else {
        ControlError::CommandFailed
    }
}

fn map_gateway_io_error(error: std::io::Error) -> ControlError {
    if gateway_io_is_timeout(&error) {
        ControlError::Timeout
    } else {
        ControlError::CommandFailed
    }
}

fn gateway_io_is_timeout(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
    ) || error
        .get_ref()
        .and_then(|source| source.downcast_ref::<reqwest::Error>())
        .is_some_and(reqwest::Error::is_timeout)
}

fn bridge_gateway_events(
    gateway_session: GatewaySessionHandle,
) -> mpsc::Receiver<ControlUpdateEvent> {
    bridge_gateway_events_with_read_poll(gateway_session, GATEWAY_SSE_READ_POLL_INTERVAL)
}

fn bridge_gateway_events_with_read_poll(
    gateway_session: GatewaySessionHandle,
    read_poll: Duration,
) -> mpsc::Receiver<ControlUpdateEvent> {
    debug_assert!(!read_poll.is_zero());
    let (sender, receiver) = mpsc::sync_channel(64);
    let _ = thread::Builder::new()
        .name(String::from("cmclient-gateway-event-bridge"))
        .spawn(move || {
            #[cfg(test)]
            let _bridge_guard = GatewayEventBridgeTestGuard::new();
            let mut last_event_id = None;
            loop {
                if !probe_gateway_event_receiver(&sender) {
                    break;
                }
                let Some(route) = gateway_session.snapshot() else {
                    if !wait_for_gateway_event_retry(&sender, GATEWAY_SSE_RECONNECT_DELAY) {
                        break;
                    }
                    continue;
                };
                if !bridge_gateway_event_stream(&route, &sender, read_poll, &mut last_event_id) {
                    break;
                }
                thread::sleep(Duration::from_millis(250));
            }
        });
    receiver
}

fn bridge_gateway_event_stream(
    route: &GatewayRoute,
    sender: &SyncSender<ControlUpdateEvent>,
    read_poll: Duration,
    last_event_id: &mut Option<String>,
) -> bool {
    let mut reader = match open_gateway_event_stream(route, read_poll, last_event_id.as_deref()) {
        Ok(reader) => reader,
        Err(()) => return wait_for_gateway_event_retry(sender, GATEWAY_SSE_RECONNECT_DELAY),
    };
    let mut id: Option<String> = None;
    let mut event: Option<String> = None;
    let mut data: Option<Vec<u8>> = None;
    let mut next_receiver_probe = Instant::now() + read_poll;
    loop {
        if !reader.is_current() {
            return true;
        }
        if Instant::now() >= next_receiver_probe {
            if !probe_gateway_event_receiver(sender) {
                return false;
            }
            next_receiver_probe = Instant::now() + read_poll;
        }
        let mut line = Vec::new();
        let count = loop {
            match reader.read_line(&mut line) {
                Ok(count) => break count,
                Err(GatewaySseLineReadError::Poll) => {
                    if !reader.is_current() {
                        return true;
                    }
                    if !probe_gateway_event_receiver(sender) {
                        return false;
                    }
                    next_receiver_probe = Instant::now() + read_poll;
                }
                Err(GatewaySseLineReadError::Invalid) => {
                    return probe_gateway_event_receiver(sender);
                }
                Err(GatewaySseLineReadError::Stale) => return true,
            }
        };
        if !reader.is_current() {
            return true;
        }
        if count == 0 {
            break;
        }
        if line.last() == Some(&b'\n') {
            line.pop();
        }
        if line.last() == Some(&b'\r') {
            line.pop();
        }
        let Ok(line) = std::str::from_utf8(&line) else {
            return probe_gateway_event_receiver(sender);
        };
        if line.starts_with(':') {
            if !reader.is_current() {
                return true;
            }
            if !try_forward_gateway_event(sender, gateway_heartbeat()) {
                return false;
            }
            continue;
        }
        if line.is_empty() {
            let completed = (id.take(), event.take(), data.take());
            if let (Some(id), Some(event), Some(data)) = completed {
                if !reader.is_current() {
                    return true;
                }
                if !try_forward_gateway_event(
                    sender,
                    ControlUpdateEvent {
                        id: id.clone(),
                        event,
                        data,
                    },
                ) {
                    return false;
                }
                *last_event_id = Some(id);
            }
            continue;
        }
        let Some((field, value)) = line.split_once(':') else {
            continue;
        };
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "id" if is_safe_sse_token(value) => id = Some(value.to_owned()),
            "event" if is_safe_sse_token(value) => event = Some(value.to_owned()),
            "data"
                if !value.contains('\r')
                    && !value.contains('\n')
                    && value.len() <= MAX_SSE_EVENT_BYTES =>
            {
                data = Some(value.as_bytes().to_vec());
            }
            _ => {}
        }
    }
    if reader.is_current() {
        probe_gateway_event_receiver(sender)
    } else {
        true
    }
}

struct GatewayEventStreamReader {
    reader: BufReader<reqwest::blocking::Response>,
    route: GatewayRoute,
    _active_route: ActiveGatewayRoute,
}

impl GatewayEventStreamReader {
    fn is_current(&self) -> bool {
        self.route.is_active()
    }

    fn read_line(&mut self, output: &mut Vec<u8>) -> Result<usize, GatewaySseLineReadError> {
        if !self.is_current() {
            return Err(GatewaySseLineReadError::Stale);
        }
        let result = read_bounded_gateway_sse_line(&mut self.reader, output);
        if !self.is_current() {
            return Err(GatewaySseLineReadError::Stale);
        }
        result
    }
}

fn open_gateway_event_stream(
    route: &GatewayRoute,
    read_poll: Duration,
    last_event_id: Option<&str>,
) -> Result<GatewayEventStreamReader, ()> {
    let client = reqwest::blocking::Client::builder()
        .connect_timeout(Duration::from_secs(3))
        .timeout(read_poll)
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .build()
        .map_err(|_| ())?;
    let active_route = route.active().ok_or(())?;
    let mut request = client
        .get(format!("http://{}/api/v1/events", active_route.address()))
        .header("accept", "text/event-stream")
        .header(GATEWAY_CAPABILITY_HEADER, active_route.capability());
    if let Some(last_event_id) = last_event_id.filter(|value| is_safe_sse_token(value)) {
        request = request.header("last-event-id", last_event_id);
    }
    let response = request.send().map_err(|_| ())?;
    let content_type_valid = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("text/event-stream"));
    if response.status() != reqwest::StatusCode::OK || !content_type_valid || !route.is_active() {
        return Err(());
    }
    Ok(GatewayEventStreamReader {
        reader: BufReader::new(response),
        route: route.clone(),
        _active_route: active_route,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GatewaySseLineReadError {
    Poll,
    Invalid,
    Stale,
}

fn read_bounded_gateway_sse_line(
    reader: &mut impl BufRead,
    output: &mut Vec<u8>,
) -> Result<usize, GatewaySseLineReadError> {
    const MAX_GATEWAY_SSE_LINE_BYTES: usize = 64 * 1024;
    loop {
        let available = reader.fill_buf().map_err(|error| {
            if gateway_io_is_timeout(&error) {
                GatewaySseLineReadError::Poll
            } else {
                GatewaySseLineReadError::Invalid
            }
        })?;
        if available.is_empty() {
            return Ok(output.len());
        }
        let count = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |index| index + 1);
        if output.len().saturating_add(count) > MAX_GATEWAY_SSE_LINE_BYTES {
            return Err(GatewaySseLineReadError::Invalid);
        }
        let ended = available.get(count.saturating_sub(1)) == Some(&b'\n');
        output.extend_from_slice(&available[..count]);
        reader.consume(count);
        if ended {
            return Ok(output.len());
        }
    }
}

fn probe_gateway_event_receiver(sender: &SyncSender<ControlUpdateEvent>) -> bool {
    try_forward_gateway_event(sender, gateway_heartbeat())
}

fn try_forward_gateway_event(
    sender: &SyncSender<ControlUpdateEvent>,
    event: ControlUpdateEvent,
) -> bool {
    match sender.try_send(event) {
        Ok(()) => true,
        Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => false,
    }
}

fn wait_for_gateway_event_retry(sender: &SyncSender<ControlUpdateEvent>, delay: Duration) -> bool {
    let deadline = Instant::now() + delay;
    loop {
        if !probe_gateway_event_receiver(sender) {
            return false;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return true;
        }
        thread::sleep(remaining.min(Duration::from_secs(1)));
    }
}

#[cfg(test)]
static ACTIVE_GATEWAY_EVENT_BRIDGES: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
struct GatewayEventBridgeTestGuard;

#[cfg(test)]
impl GatewayEventBridgeTestGuard {
    fn new() -> Self {
        ACTIVE_GATEWAY_EVENT_BRIDGES.fetch_add(1, Ordering::AcqRel);
        Self
    }
}

#[cfg(test)]
impl Drop for GatewayEventBridgeTestGuard {
    fn drop(&mut self) {
        ACTIVE_GATEWAY_EVENT_BRIDGES.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(all(test, not(target_os = "windows")))]
fn active_gateway_event_bridge_count() -> usize {
    ACTIVE_GATEWAY_EVENT_BRIDGES.load(Ordering::Acquire)
}

fn gateway_heartbeat() -> ControlUpdateEvent {
    ControlUpdateEvent {
        id: format!("heartbeat-{}", Utc::now().timestamp_millis()),
        event: String::from("gateway.heartbeat"),
        data: br#"{"schemaVersion":1}"#.to_vec(),
    }
}

const fn secret_kind(kind: ControlSecretKind) -> SecretKind {
    match kind {
        ControlSecretKind::CallMeshApiKey => SecretKind::CallMeshApiKey,
        ControlSecretKind::AprsPasscode => SecretKind::AprsPasscode,
        ControlSecretKind::ManagementAdminToken => SecretKind::ManagementAdminToken,
    }
}

const fn control_secret_error(error: SecretStoreError) -> ControlError {
    match error {
        SecretStoreError::InvalidValue => ControlError::SecretValueInvalid,
        SecretStoreError::Unavailable => ControlError::SecretStoreUnavailable,
    }
}

fn load_agent_config_read_only() -> Result<AgentConfig, String> {
    let environment = env::vars().collect::<BTreeMap<_, _>>();
    AgentConfig::from_environment(&environment).map_err(|error| String::from(error.code()))
}

fn load_agent_config_after_migration_with(
    environment: &BTreeMap<String, String>,
    paths: &RuntimePaths,
    candidates: &[ProductMigrationSourceSet],
    maintenance: &dyn GatewayMaintenanceRunner,
) -> Result<AgentConfig, String> {
    let reset_completed = recover_interrupted_reset(paths)
        .map_err(|_| String::from("SETUP_CONFIGURATION_WRITE_FAILED"))?;
    if !reset_completed {
        migrate_detected_product_source_sets(paths.root_dir(), candidates, maintenance)
            .map_err(|error| String::from(error.code()))?;
    }
    let secrets =
        AgentSecretStore::runtime(paths.root_dir()).map_err(|error| String::from(error.code()))?;
    recover_interrupted_setup(paths, &paths.config_file(), &secrets)
        .map_err(|_| String::from("SETUP_CONFIGURATION_WRITE_FAILED"))?;
    AgentConfig::from_environment(environment).map_err(|error| String::from(error.code()))
}

fn legacy_state_candidates(
    environment: &BTreeMap<String, String>,
) -> Vec<ProductMigrationSourceSet> {
    let mut candidates = Vec::new();
    #[cfg(target_os = "windows")]
    {
        push_single_root_legacy_candidate(
            &mut candidates,
            environment_value(environment, "APPDATA")
                .map(|root| PathBuf::from(root).join("CMClient")),
        );
        push_single_root_legacy_candidate(
            &mut candidates,
            environment_value(environment, "PROGRAMDATA")
                .map(|root| PathBuf::from(root).join("CMClient")),
        );
    }
    #[cfg(target_os = "macos")]
    push_single_root_legacy_candidate(
        &mut candidates,
        environment_value(environment, "HOME")
            .map(|root| PathBuf::from(root).join("Library/Application Support/CMClient")),
    );
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        let home = environment_value(environment, "HOME").map(PathBuf::from);
        let config_root = environment_value(environment, "XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .or_else(|| home.as_ref().map(|root| root.join(".config")))
            .map(|root| root.join("cmclient"));
        let data_root = environment_value(environment, "XDG_DATA_HOME")
            .map(PathBuf::from)
            .or_else(|| home.as_ref().map(|root| root.join(".local/share")))
            .map(|root| root.join("cmclient"));
        push_legacy_source_candidate(&mut candidates, config_root, data_root);
    }
    candidates
}

#[cfg(any(target_os = "windows", target_os = "macos"))]
fn push_single_root_legacy_candidate(
    candidates: &mut Vec<ProductMigrationSourceSet>,
    candidate: Option<PathBuf>,
) {
    push_legacy_source_candidate(candidates, candidate.clone(), candidate);
}

fn push_legacy_source_candidate(
    candidates: &mut Vec<ProductMigrationSourceSet>,
    config_root: Option<PathBuf>,
    data_root: Option<PathBuf>,
) {
    let config_root = config_root.filter(|path| path.is_absolute());
    let data_root = data_root.filter(|path| path.is_absolute());
    let (config_root, data_root) = match (config_root, data_root) {
        (Some(config_root), Some(data_root)) => (config_root, data_root),
        (Some(root), None) | (None, Some(root)) => (root.clone(), root),
        (None, None) => return,
    };
    let candidate = ProductMigrationSourceSet {
        config_root,
        data_root,
    };
    if candidates.iter().any(|existing| {
        paths_equal(&existing.config_root, &candidate.config_root)
            && paths_equal(&existing.data_root, &candidate.data_root)
    }) {
        return;
    }
    candidates.push(candidate);
}

fn paths_equal(left: &Path, right: &Path) -> bool {
    #[cfg(target_os = "windows")]
    return left
        .to_string_lossy()
        .eq_ignore_ascii_case(&right.to_string_lossy());
    #[cfg(not(target_os = "windows"))]
    return left == right;
}

fn environment_value<'a>(environment: &'a BTreeMap<String, String>, name: &str) -> Option<&'a str> {
    environment
        .get(name)
        .or_else(|| {
            environment
                .iter()
                .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
                .map(|(_, value)| value)
        })
        .map(String::as_str)
}

fn resolve_gateway_maintenance_program(
    environment: &BTreeMap<String, String>,
) -> Result<(PathBuf, PathBuf), String> {
    let bundle = bundled_root();
    let development_entrypoint =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../gateway/dist/main.js");
    let gateway_entrypoint = explicit_runtime_file(
        environment,
        "CMCLIENT_GATEWAY_ENTRYPOINT",
        "AGENT_GATEWAY_ENTRYPOINT_INVALID",
    )?
    .or_else(|| {
        bundle
            .as_ref()
            .map(|root| root.join("gateway/dist/main.js"))
            .filter(|path| path.is_file())
    })
    .or_else(|| {
        (cfg!(debug_assertions) && development_entrypoint.is_file())
            .then_some(development_entrypoint)
    })
    .ok_or_else(|| String::from("AGENT_GATEWAY_ENTRYPOINT_INVALID"))?;
    let bundled_node = bundle
        .as_ref()
        .map(|root| root.join(private_node_relative_path()));
    let mut private_node = explicit_runtime_file(
        environment,
        "CMCLIENT_PRIVATE_NODE",
        "AGENT_PRIVATE_NODE_INVALID",
    )?
    .or_else(|| bundled_node.filter(|path| path.is_file()));
    if private_node.is_none() && cfg!(debug_assertions) {
        private_node = find_node_on_path(environment);
    }
    let private_node = private_node.ok_or_else(|| String::from("AGENT_PRIVATE_NODE_INVALID"))?;
    Ok((
        canonical_file_or_path(private_node),
        canonical_file_or_path(gateway_entrypoint),
    ))
}

fn explicit_runtime_file(
    environment: &BTreeMap<String, String>,
    name: &str,
    error_code: &str,
) -> Result<Option<PathBuf>, String> {
    let Some(value) = environment_value(environment, name) else {
        return Ok(None);
    };
    let path = PathBuf::from(value);
    if !path.is_absolute() || !path.is_file() {
        return Err(String::from(error_code));
    }
    let metadata = fs::symlink_metadata(&path).map_err(|_| String::from(error_code))?;
    if metadata.file_type().is_symlink() {
        return Err(String::from(error_code));
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        if metadata.file_attributes() & 0x0400 != 0 {
            return Err(String::from(error_code));
        }
    }
    fs::canonicalize(path)
        .map(Some)
        .map_err(|_| String::from(error_code))
}

fn find_node_on_path(environment: &BTreeMap<String, String>) -> Option<PathBuf> {
    let path = environment_value(environment, "PATH")?;
    let executable = if cfg!(target_os = "windows") {
        "node.exe"
    } else {
        "node"
    };
    env::split_paths(&OsString::from(path))
        .map(|directory| directory.join(executable))
        .find(|candidate| candidate.is_absolute() && candidate.is_file())
        .map(canonical_file_or_path)
}

fn canonical_file_or_path(path: PathBuf) -> PathBuf {
    let canonical = if path.is_file() {
        fs::canonicalize(&path).unwrap_or(path)
    } else {
        path
    };
    normalize_runtime_process_path(canonical)
}

#[cfg(target_os = "windows")]
fn normalize_runtime_process_path(path: PathBuf) -> PathBuf {
    let value = path.to_string_lossy();
    if let Some(unc_path) = value.strip_prefix(r"\\?\UNC\") {
        return PathBuf::from(format!(r"\\{unc_path}"));
    }
    if let Some(drive_path) = value.strip_prefix(r"\\?\") {
        return PathBuf::from(drive_path);
    }
    path
}

#[cfg(not(target_os = "windows"))]
fn normalize_runtime_process_path(path: PathBuf) -> PathBuf {
    path
}

const fn private_node_relative_path() -> &'static str {
    if cfg!(target_os = "windows") {
        "runtime/node/node.exe"
    } else {
        "runtime/node/bin/node"
    }
}

fn main() -> ExitCode {
    let mut arguments = std::env::args().skip(1);
    match arguments.next().as_deref() {
        Some("--serve") => serve(),
        None | Some("--check-config") | Some("--check-instance") => {
            match load_agent_config_read_only() {
                Ok(config) => {
                    if let Err(error) = ensure_runtime_directories(&config.paths) {
                        eprintln!("{}", error.code());
                        return ExitCode::from(EX_CONFIG);
                    }
                    if std::env::args()
                        .skip(1)
                        .any(|argument| argument == "--check-instance")
                    {
                        match AgentLease::acquire(&config.paths) {
                            Ok((_lease, _state)) => println!("agent instance lock valid"),
                            Err(error) => {
                                eprintln!("{}", error.code());
                                return ExitCode::from(EX_CONFIG);
                            }
                        }
                    } else {
                        println!("agent configuration valid");
                    }
                    ExitCode::SUCCESS
                }
                Err(error) => {
                    eprintln!("{error}");
                    ExitCode::from(EX_CONFIG)
                }
            }
        }
        Some("--version") => {
            println!("cmclient-agent {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        Some(_) => {
            eprintln!("AGENT_USAGE_INVALID_ARGUMENT");
            ExitCode::from(EX_USAGE)
        }
    }
}

fn install_shutdown_signal_handler(controller: Arc<AgentController>) -> Result<(), ControlError> {
    ctrlc::set_handler(move || controller.request_shutdown())
        .map_err(|_| ControlError::CommandFailed)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ShutdownSignalRegistration {
    Registered,
    Unavailable,
}

fn classify_shutdown_signal_registration(
    registration: Result<(), ControlError>,
) -> ShutdownSignalRegistration {
    match registration {
        Ok(()) => ShutdownSignalRegistration::Registered,
        Err(_) => ShutdownSignalRegistration::Unavailable,
    }
}

fn serve() -> ExitCode {
    let environment = env::vars().collect::<BTreeMap<_, _>>();
    let paths = match RuntimePaths::from_environment(&environment) {
        Ok(paths) => paths,
        Err(error) => {
            eprintln!("{}", error.code());
            return ExitCode::from(EX_CONFIG);
        }
    };
    if let Err(error) = ensure_runtime_directories(&paths) {
        eprintln!("{}", error.code());
        return ExitCode::from(EX_CONFIG);
    }
    let _lease = match AgentLease::acquire(&paths) {
        Ok((lease, _)) => lease,
        Err(error) => {
            eprintln!("{}", error.code());
            return ExitCode::from(EX_CONFIG);
        }
    };
    let candidates = legacy_state_candidates(&environment);
    let (program, gateway_entrypoint) = match resolve_gateway_maintenance_program(&environment) {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::from(EX_CONFIG);
        }
    };
    let maintenance = match ChildGatewayMaintenanceRunner::new(program, gateway_entrypoint) {
        Ok(maintenance) => maintenance,
        Err(error) => {
            eprintln!("{}", error.code());
            return ExitCode::from(EX_CONFIG);
        }
    };
    let config = match load_agent_config_after_migration_with(
        &environment,
        &paths,
        &candidates,
        &maintenance,
    ) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::from(EX_CONFIG);
        }
    };
    let controller = match AgentController::from_config(&config) {
        Ok(controller) => Arc::new(controller),
        Err(error) => {
            eprintln!("{}", error.code());
            return ExitCode::from(EX_CONFIG);
        }
    };
    if let Err(error) = controller.install_setup_apply() {
        controller.remember_error(&error);
        eprintln!("{}", error.code());
        return ExitCode::from(EX_CONFIG);
    }
    if let Err(error) = controller.install_operational_reset() {
        controller.remember_error(&error);
        eprintln!("{}", error.code());
        return ExitCode::from(EX_CONFIG);
    }
    if let Err(error) = controller.install_cmcloud_enrollment() {
        controller.remember_error(&error);
        eprintln!("{}", error.code());
        return ExitCode::from(EX_CONFIG);
    }
    let endpoint = match default_local_endpoint(config.paths.root_dir()) {
        Ok(endpoint) => endpoint,
        Err(error) => {
            controller.remember_error(&error);
            eprintln!("{}", error.code());
            return ExitCode::from(EX_CONFIG);
        }
    };
    let control_handler: Arc<dyn ControlHandler> = controller.clone();
    let server = match ControlServer::bind(endpoint.clone(), control_handler) {
        Ok(server) => server,
        Err(error) => {
            controller.remember_error(&error);
            eprintln!("{}", error.code());
            return ExitCode::from(EX_CONFIG);
        }
    };
    // Control IPC remains the authoritative shutdown mechanism. A headless
    // Windows parent can legitimately reject console Ctrl-C registration, so
    // that optional integration must not prevent the resident Agent from
    // serving its control plane and Gateway.
    if classify_shutdown_signal_registration(install_shutdown_signal_handler(Arc::clone(
        &controller,
    ))) == ShutdownSignalRegistration::Unavailable
    {
        eprintln!("AGENT_SHUTDOWN_SIGNAL_UNAVAILABLE");
    }
    let mut supervisor_worker = match SupervisorWorker::start(Arc::clone(&controller)) {
        Ok(worker) => worker,
        Err(error) => {
            controller.remember_error(&error);
            eprintln!("{}", error.code());
            return ExitCode::from(EX_CONFIG);
        }
    };
    // The resident Agent owns the private Gateway whenever setup is ready.
    // A startup failure remains supervised by the worker's bounded backoff;
    // it must not turn the control plane itself into a one-shot process.
    if let Err(error) = controller.start_resident_supervisor() {
        controller.remember_error(&error);
    }
    let mut agent_tray = tray::AgentTray::start(endpoint, config.paths.desktop_process_file());
    let serve_error = loop {
        if controller.is_shutdown_requested() {
            break None;
        }
        match server.poll_once() {
            Ok(_) if controller.is_shutdown_requested() => break None,
            Ok(_) => {}
            Err(error) => break Some(error),
        }
    };
    drop(server);
    agent_tray.stop();
    let shutdown_error = shutdown_agent_runtime(&controller, &mut supervisor_worker).err();
    if let Some(error) = &serve_error {
        controller.remember_error(error);
        eprintln!("{}", error.code());
    }
    if let Some(error) = shutdown_error {
        controller.remember_error(&error);
        eprintln!("{}", error.code());
        return ExitCode::from(EX_CONFIG);
    }
    if serve_error.is_some() {
        ExitCode::from(EX_CONFIG)
    } else {
        ExitCode::SUCCESS
    }
}

fn resolve_static_web_root() -> PathBuf {
    if let Some(path) = std::env::var_os("CMCLIENT_WEB_ROOT") {
        return PathBuf::from(path);
    }
    let bundled = bundled_root().map(|root| root.join("web"));
    let development = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../web/dist");
    if development.join("index.html").is_file() {
        return development;
    }
    bundled.unwrap_or_else(|| PathBuf::from("web"))
}

fn resolve_gateway_command(_config: &AgentConfig) -> Option<Vec<String>> {
    #[cfg(test)]
    if let Some(command) = &_config.gateway_command {
        return Some(command.clone());
    }
    let environment = env::vars().collect::<BTreeMap<_, _>>();
    let (private_node, gateway_entrypoint) =
        resolve_gateway_maintenance_program(&environment).ok()?;
    Some(vec![
        private_node.to_string_lossy().into_owned(),
        gateway_entrypoint.to_string_lossy().into_owned(),
    ])
}

fn bundled_root() -> Option<PathBuf> {
    std::env::current_exe().ok().and_then(|executable| {
        executable
            .parent()
            .and_then(Path::parent)
            .map(Path::to_path_buf)
    })
}

#[cfg(test)]
mod tests {
    use super::{
        AGENT_EVENT_REPLAY_BUFFER, AGENT_EVENT_SUBSCRIBER_LIMIT, AgentConfig, AgentController,
        AgentEventHub, AgentEventHubError, AgentEventSubscription, AgentLifecycleStatus,
        AgentRuntimeProfile, AgentSecretStore, AgentUpdateService, AgentWebState,
        CMCloudAccountProjection, CMCloudAccountProjectionAccount,
        CMCloudAccountProjectionAuthority, CMCloudAccountProjectionControlError,
        CMCloudAccountProjectionFreshness, CMCloudAccountProjectionStation,
        CMCloudAccountProjectionTenant, CMCloudAccountRole, CMCloudAccountState,
        CMCloudEnrollmentControlError, CMCloudEnrollmentState, CMCloudEnrollmentStatus,
        CMCloudStationKind, CMCloudStationState, ControlCommand, ControlHandler,
        FactoryResetBackupBehavior, FactoryResetFixtureConfirmation, FactoryResetFixtureJob,
        FactoryResetFixturePhase, GatewayLogHealthUpdate, GatewayRoute, GatewaySessionHandle,
        GatewayStatus, InternalComponent, LogLevel, LogPolicy, ManagementWebConfig,
        ManagementWebError, ManagementWebService, ResetKind, SecretKind, SetupApplyError,
        SetupCancellationToken, SetupConfigureRequest, SetupError, SetupPhase, SetupRollbackState,
        SetupStore, ShutdownSignalRegistration, StructuredLogSink, SupervisorWorker,
        agent_web_router, apply_aprs_environment, apply_physical_qualification_environment,
        bridge_gateway_event_stream, classify_shutdown_signal_registration, cmcloud_enrollment,
        cmcloud_gateway_environment, compiled_component_identity, disable_proxy_for_setup,
        ensure_runtime_directories, gateway_json_projection, legacy_state_candidates,
        load_agent_config_after_migration_with, management_agent_events, management_web_profile,
        push_legacy_source_candidate, recover_interrupted_reset, recover_interrupted_setup,
        remove_setup_transaction, reset_completion_file, reset_transaction_file,
        resolve_gateway_maintenance_program, setup_apply_error_response, setup_error_response,
        setup_gate_required_with_profile, setup_transaction_file, valid_cmcloud_pairing_code,
        validate_setup_request, verified_gateway_route, write_reset_transaction,
        write_setup_configuration, write_setup_transaction,
    };
    #[cfg(not(target_os = "windows"))]
    use super::{
        GatewayControlStatus, ManagementWebControlStatus, active_gateway_event_bridge_count,
        bridge_gateway_events_with_read_poll, gateway_heartbeat, read_bounded_gateway_sse_line,
        shutdown_agent_runtime, try_forward_gateway_event,
    };
    use axum::{
        Router,
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    #[cfg(not(target_os = "windows"))]
    use cmclient_agent_core::CallMeshConfig;
    use cmclient_agent_core::{
        AprsConfig, CMCloudConfig, MeshtasticConfig, MeshtasticConnectionConfig, RuntimePaths,
    };
    #[cfg(not(target_os = "windows"))]
    use cmclient_control_api::UpdateControlStatus;
    use cmclient_control_api::{
        ControlClient, ControlError, ControlSecretKind, ControlServer, default_local_endpoint,
    };
    use cmclient_legacy_migration::ProductMigrationSourceSet;
    use cmclient_legacy_migration::{
        GatewayMaintenanceReport, GatewayMaintenanceRunner, MigrationError,
    };
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    #[cfg(not(target_os = "windows"))]
    use cmclient_supervisor::{BackoffPolicy, GatewayCommand, GatewaySupervisor};
    #[cfg(not(target_os = "windows"))]
    use cmclient_updater::{PersistentUpdateJob, UpdatePhase};
    #[cfg(target_os = "windows")]
    use std::net::{TcpListener, TcpStream};
    use std::{
        collections::BTreeMap,
        io::{Read, Write},
        path::{Path, PathBuf},
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        thread,
        time::{Duration, Instant},
    };
    #[cfg(not(target_os = "windows"))]
    use std::{io::Cursor, net::TcpListener, sync::mpsc};

    #[test]
    #[ignore = "child-process fixture"]
    fn long_running_gateway_fixture() {
        thread::sleep(Duration::from_secs(10));
    }

    #[test]
    #[ignore = "child-process fixture"]
    fn agent_gateway_fixture() {
        let mode = std::env::var("CMCLIENT_AGENT_TEST_MODE")
            .expect("Gateway fixture mode should be configured");
        let marker = std::env::var("CMCLIENT_AGENT_TEST_MARKER")
            .expect("Gateway fixture marker should be configured");
        let mut marker = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(marker)
            .expect("Gateway fixture marker should open");
        marker
            .write_all(b"x")
            .expect("Gateway fixture marker should write");
        drop(marker);
        match mode.as_str() {
            "wait" => {
                let mut stdin = std::io::stdin();
                let _ = stdin.read(&mut [0_u8; 1]);
            }
            "crash" => {
                let executable =
                    std::env::current_exe().expect("Gateway fixture executable should resolve");
                let keeper = std::process::Command::new(executable)
                    .args([
                        "--ignored",
                        "--exact",
                        "tests::long_running_gateway_fixture",
                    ])
                    .spawn()
                    .expect("Gateway fixture keeper should start");
                std::mem::forget(keeper);
                thread::sleep(Duration::from_millis(50));
            }
            _ => panic!("unsupported Gateway fixture mode"),
        }
    }

    #[test]
    fn setup_callmesh_failure_responses_use_stable_retry_semantics() {
        let rejected = setup_apply_error_response(SetupApplyError::CallMeshCredentialRejected);
        assert_eq!(rejected.status(), StatusCode::UNAUTHORIZED);
        let unavailable = setup_apply_error_response(SetupApplyError::CallMeshUnavailable);
        assert_eq!(unavailable.status(), StatusCode::SERVICE_UNAVAILABLE);
        let cancelled = setup_apply_error_response(SetupApplyError::Cancelled);
        assert_eq!(cancelled.status(), StatusCode::REQUEST_TIMEOUT);
    }

    fn wait_for_fixture_marker(marker: &Path, expected: &str, timeout: Duration) -> String {
        wait_for_fixture_marker_with(expected, timeout, || std::fs::read_to_string(marker))
    }

    fn ready_supervised_controller(
        name: &str,
        gateway_command: Vec<String>,
    ) -> (PathBuf, Arc<AgentController>) {
        let sequence = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock should follow epoch")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "cmclient-agent-{name}-{}-{sequence}",
            std::process::id()
        ));
        std::fs::create_dir_all(&directory).expect("test directory should exist");
        let paths = RuntimePaths {
            data_dir: directory.clone(),
            config_dir: directory.clone(),
            cache_dir: directory.join("cache"),
            log_dir: directory.join("logs"),
        };
        let setup = SetupStore::open(&paths).expect("setup state should initialize");
        setup
            .accept_terms(cmclient_agent_core::setup::CURRENT_TERMS_VERSION)
            .expect("terms should be accepted");
        let fence = setup.begin_validation().expect("validation should begin");
        setup.mark_ready(fence).expect("setup should become ready");
        drop(setup);
        let (secrets, cmcloud) = cmcloud_fixture(&paths);
        let config = AgentConfig {
            paths,
            config_file: directory.join("agent.toml"),
            runtime_profile: AgentRuntimeProfile::Native,
            gateway_command: Some(gateway_command),
            callmesh: None,
            cmcloud: Some(cmcloud),
            meshtastic: None,
            aprs: None,
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };
        let controller = Arc::new(
            AgentController::from_config_with_secrets_without_private_bootstrap(&config, secrets)
                .expect("controller should initialize"),
        );
        (directory, controller)
    }

    fn cmcloud_fixture(paths: &RuntimePaths) -> (AgentSecretStore, CMCloudConfig) {
        let setup = SetupStore::open(paths).expect("CMCloud setup state should initialize");
        if setup
            .status()
            .expect("CMCloud setup status should load")
            .setup_required
        {
            setup
                .accept_terms(cmclient_agent_core::setup::CURRENT_TERMS_VERSION)
                .expect("CMCloud terms should be accepted");
            let fence = setup
                .begin_validation()
                .expect("CMCloud validation should begin");
            setup
                .mark_ready(fence)
                .expect("CMCloud setup should become ready");
        }
        let endpoint = "wss://cmcloud.example.invalid/agent/v1";
        let secrets = AgentSecretStore::memory();
        secrets
            .begin_cmcloud_enrollment(endpoint, "fixture-pairing-code-0123456789", "2.0.0-rc.1")
            .expect("CMCloud pairing fixture should begin");
        secrets
            .record_cmcloud_issued_credential(0, 1, 1, "fixture-device-credential-0123456789")
            .expect("CMCloud credential fixture should persist");
        secrets
            .activate_cmcloud_credential(0, 1, 1)
            .expect("CMCloud credential fixture should activate");
        (
            secrets,
            CMCloudConfig {
                agent_websocket_url: String::from(endpoint),
            },
        )
    }

    fn gateway_fixture_command() -> Vec<String> {
        let executable = std::env::current_exe().expect("test executable should resolve");
        vec![
            executable.to_string_lossy().into_owned(),
            String::from("--ignored"),
            String::from("--exact"),
            String::from("tests::agent_gateway_fixture"),
        ]
    }

    fn configure_gateway_fixture(controller: &AgentController, mode: &str, marker: &Path) {
        controller
            .supervisor
            .lock()
            .expect("supervisor should lock")
            .as_mut()
            .expect("supervisor should exist")
            .set_environment(BTreeMap::from([
                (String::from("CMCLIENT_AGENT_TEST_MODE"), String::from(mode)),
                (
                    String::from("CMCLIENT_AGENT_TEST_MARKER"),
                    marker.to_string_lossy().into_owned(),
                ),
            ]));
    }

    fn wait_for_fixture_marker_with(
        expected: &str,
        timeout: Duration,
        mut read_marker: impl FnMut() -> std::io::Result<String>,
    ) -> String {
        let deadline = Instant::now() + timeout;
        loop {
            match read_marker() {
                Ok(contents) if contents == expected || contents == "rejected" => {
                    return contents;
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => panic!("marker should read: {error}"),
            }
            assert!(Instant::now() < deadline, "gateway fixture did not report");
            thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn setup_configuration_validation_rejects_injection_and_non_meshtastic_ports() {
        let request = |host: &str, port: u16| SetupConfigureRequest {
            meshtastic_host: String::from(host),
            meshtastic_port: port,
            mesh_network_id: Some(String::from("default")),
            gateway_id: Some(String::from("cmclient-gateway")),
        };
        let valid = request("172.16.8.88", 4_403);
        assert!(validate_setup_request(&valid).is_ok());

        for invalid in [
            request("172.16.8.88\n", 4_403),
            request("172.16.8.88\"", 4_403),
            request("172.16.8.88", 80),
        ] {
            assert_eq!(
                validate_setup_request(&invalid),
                Err(SetupApplyError::InvalidInput)
            );
        }
    }

    #[test]
    fn setup_request_rejects_legacy_callmesh_credentials() {
        let legacy = serde_json::json!({
            "meshtasticHost": "172.16.8.88",
            "meshtasticPort": 4403,
            "meshNetworkId": "default",
            "gatewayId": "cmclient-gateway",
            "callmeshApiKey": "must-not-be-accepted"
        });
        assert!(serde_json::from_value::<SetupConfigureRequest>(legacy).is_err());
    }

    #[test]
    fn setup_configuration_file_is_atomic_and_contains_no_secret_or_destination_override() {
        let root = std::env::temp_dir().join(format!(
            "cmclient-agent-setup-config-{}",
            std::process::id(),
        ));
        let _ = std::fs::remove_dir_all(&root);
        let path = root.join("config/agent.toml");
        write_setup_configuration(&path, "172.16.8.88", 4_403, "default", "cmclient-gateway")
            .expect("setup config should commit");
        let contents = std::fs::read_to_string(&path).expect("setup config should read");
        assert!(contents.contains("tcp_host = \"172.16.8.88\""));
        assert!(contents.contains("agent_websocket_url = \"wss://cmcloud.tmmarc.org/agent/v1\""));
        assert!(contents.contains("[cmcloud]"));
        assert!(!contents.contains("[callmesh]"));
        assert!(!contents.contains("[aprs]"));
        assert!(!contents.contains("callmesh.tmmarc.org"));
        assert!(!contents.contains("CMCLIENT_APRS_DESTINATION"));
        assert!(!contents.contains("APCM20"));
        std::fs::remove_dir_all(root).expect("setup config fixture should clean up");
    }

    fn setup_rollback_fixture(
        name: &str,
        secrets: AgentSecretStore,
    ) -> (std::path::PathBuf, AgentController) {
        let sequence = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock should follow epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "cmclient-agent-{name}-{}-{sequence}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).expect("setup rollback root should create");
        std::fs::create_dir_all(root.join("config"))
            .expect("setup rollback config directory should create");
        let config = AgentConfig {
            paths: RuntimePaths {
                data_dir: root.clone(),
                config_dir: root.join("config"),
                cache_dir: root.join("cache"),
                log_dir: root.join("logs"),
            },
            config_file: root.join("config/agent.toml"),
            runtime_profile: AgentRuntimeProfile::Native,
            gateway_command: None,
            callmesh: None,
            cmcloud: None,
            meshtastic: None,
            aprs: None,
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };
        let setup = SetupStore::open(&config.paths).expect("setup store should initialize");
        setup
            .accept_terms(cmclient_agent_core::setup::CURRENT_TERMS_VERSION)
            .expect("terms should be accepted");
        drop(setup);
        let controller =
            AgentController::from_config_with_secrets_without_private_bootstrap(&config, secrets)
                .expect("controller should initialize");
        (root, controller)
    }

    #[test]
    fn setup_cancellation_before_first_durable_write_leaves_no_transaction_state() {
        let secrets = AgentSecretStore::memory();
        secrets
            .store(SecretKind::CallMeshApiKey, "fixture-previous-key")
            .expect("previous secret should store");
        let (root, controller) =
            setup_rollback_fixture("setup-cancel-pre-durable", secrets.clone());
        let config_file = root.join("config/agent.toml");
        let previous_configuration = b"[agent]\nmanagement_web_enabled = false\n";
        std::fs::write(&config_file, previous_configuration).expect("previous config should store");
        let cancellation = SetupCancellationToken::default();
        cancellation.cancel();

        assert_eq!(
            controller.check_setup_cancellation(
                &cancellation,
                Some(previous_configuration),
                SetupRollbackState::None,
            ),
            Err(SetupApplyError::Cancelled),
        );
        assert_eq!(
            std::fs::read(&config_file).expect("previous config should remain readable"),
            previous_configuration,
        );
        assert_eq!(
            secrets
                .read(SecretKind::CallMeshApiKey)
                .expect("previous secret should remain readable")
                .expect("previous secret should remain present")
                .expose_secret(),
            "fixture-previous-key",
        );
        assert_eq!(
            controller
                .setup
                .status()
                .expect("setup status should read")
                .phase,
            SetupPhase::CredentialsRequired,
        );
        assert!(!controller.setup_transaction_file.exists());

        drop(controller);
        std::fs::remove_dir_all(root).expect("setup cancellation fixture should clean up");
    }

    #[test]
    fn setup_cancellation_after_persistent_mutation_rolls_back_config_secret_and_ready() {
        let secrets = AgentSecretStore::memory();
        secrets
            .store(SecretKind::CallMeshApiKey, "fixture-previous-key")
            .expect("previous secret should store");
        let (root, controller) = setup_rollback_fixture("setup-cancel-persistent", secrets.clone());
        let config_file = root.join("config/agent.toml");
        let previous_configuration = b"[agent]\nmanagement_web_enabled = false\n";
        std::fs::write(&config_file, previous_configuration).expect("previous config should store");
        let generation = controller
            .setup
            .generation()
            .expect("setup generation should read")
            .generation();
        write_setup_transaction(
            &controller.setup_transaction_file,
            generation,
            Some(previous_configuration),
        )
        .expect("setup transaction should store");
        let fence = controller
            .setup
            .begin_validation()
            .expect("validation should begin");
        secrets
            .stage_callmesh_setup("fixture-replacement-key")
            .expect("replacement secret should stage");
        write_setup_configuration(
            &config_file,
            "172.16.8.88",
            4_403,
            "replacement-network",
            "replacement-gateway",
        )
        .expect("replacement config should store");
        secrets
            .promote_callmesh_setup()
            .expect("replacement secret should promote");
        controller
            .setup
            .mark_ready(fence)
            .expect("ready marker should store");
        let cancellation = SetupCancellationToken::default();
        cancellation.cancel();

        assert_eq!(
            controller.check_setup_cancellation(
                &cancellation,
                Some(previous_configuration),
                SetupRollbackState::Persistent,
            ),
            Err(SetupApplyError::Cancelled),
        );
        assert_eq!(
            std::fs::read(&config_file).expect("restored config should read"),
            previous_configuration,
        );
        assert_eq!(
            secrets
                .read(SecretKind::CallMeshApiKey)
                .expect("restored secret should read")
                .expect("restored secret should be present")
                .expose_secret(),
            "fixture-previous-key",
        );
        let status = controller.setup.status().expect("setup status should read");
        assert_eq!(status.phase, SetupPhase::CredentialsRequired);
        assert!(status.setup_required);
        assert!(!controller.setup_transaction_file.exists());

        drop(controller);
        std::fs::remove_dir_all(root).expect("setup cancellation fixture should clean up");
    }

    fn prepare_pending_setup_commit(
        name: &str,
        secrets: AgentSecretStore,
    ) -> (
        std::path::PathBuf,
        Arc<AgentController>,
        super::SetupPendingCommit,
        Vec<u8>,
    ) {
        secrets
            .store(SecretKind::CallMeshApiKey, "fixture-previous-key")
            .expect("previous secret should store");
        let (root, controller) = setup_rollback_fixture(name, secrets.clone());
        let controller = Arc::new(controller);
        let previous_configuration = b"[agent]\nmanagement_web_enabled = false\n".to_vec();
        std::fs::write(&controller.config_file, &previous_configuration)
            .expect("previous config should store");
        let generation = controller
            .setup
            .generation()
            .expect("setup generation should read")
            .generation();
        write_setup_transaction(
            &controller.setup_transaction_file,
            generation,
            Some(&previous_configuration),
        )
        .expect("setup transaction should store");
        let fence = controller
            .setup
            .begin_validation()
            .expect("validation should begin");
        secrets
            .stage_callmesh_setup("fixture-replacement-key")
            .expect("replacement secret should stage");
        write_setup_configuration(
            &controller.config_file,
            "172.16.8.88",
            4_403,
            "replacement-network",
            "replacement-gateway",
        )
        .expect("replacement config should store");
        secrets
            .promote_callmesh_setup()
            .expect("replacement secret should promote");
        let status = controller
            .setup
            .mark_ready(fence)
            .expect("ready marker should store");
        let pending = super::SetupPendingCommit {
            controller: Arc::clone(&controller),
            status,
            previous_configuration: Some(previous_configuration.clone()),
            committed: false,
        };
        (root, controller, pending, previous_configuration)
    }

    fn assert_pending_setup_rolled_back(
        controller: &AgentController,
        secrets: &AgentSecretStore,
        previous_configuration: &[u8],
    ) {
        assert_eq!(
            std::fs::read(&controller.config_file).expect("restored config should read"),
            previous_configuration,
        );
        assert_eq!(
            secrets
                .read(SecretKind::CallMeshApiKey)
                .expect("restored secret should read")
                .expect("restored secret should exist")
                .expose_secret(),
            "fixture-previous-key",
        );
        let status = controller.setup.status().expect("setup status should read");
        assert_eq!(status.phase, SetupPhase::CredentialsRequired);
        assert!(status.setup_required);
    }

    #[test]
    fn setup_finalize_failure_rolls_back_before_ready_response() {
        let secrets = AgentSecretStore::memory_with_finalize_failure_once();
        let (root, controller, pending, previous_configuration) =
            prepare_pending_setup_commit("setup-finalize-failure", secrets.clone());

        assert_eq!(pending.commit(), Err(SetupApplyError::ConfigWriteFailed),);
        assert_pending_setup_rolled_back(&controller, &secrets, &previous_configuration);
        assert!(!controller.setup_transaction_file.exists());

        drop(controller);
        std::fs::remove_dir_all(root).expect("finalize failure fixture should clean up");
    }

    #[test]
    fn setup_journal_cleanup_failure_rolls_back_before_ready_response() {
        let secrets = AgentSecretStore::memory();
        let (root, controller, pending, previous_configuration) =
            prepare_pending_setup_commit("setup-journal-cleanup-failure", secrets.clone());
        std::fs::remove_file(&controller.setup_transaction_file)
            .expect("journal fixture file should remove");
        std::fs::create_dir(&controller.setup_transaction_file)
            .expect("journal fixture directory should create");

        assert_eq!(pending.commit(), Err(SetupApplyError::ConfigWriteFailed),);
        assert_pending_setup_rolled_back(&controller, &secrets, &previous_configuration);

        std::fs::remove_dir(&controller.setup_transaction_file)
            .expect("failed journal fixture should remove");
        let generation = controller
            .setup
            .generation()
            .expect("retry generation should read")
            .generation();
        write_setup_transaction(
            &controller.setup_transaction_file,
            generation,
            Some(&previous_configuration),
        )
        .expect("setup should be retryable after storage recovers");
        remove_setup_transaction(&controller.setup_transaction_file)
            .expect("retry journal should clean up");

        drop(controller);
        std::fs::remove_dir_all(root).expect("journal failure fixture should clean up");
    }

    #[test]
    fn setup_auth_failures_do_not_mutate_persistent_state_or_reach_ready() {
        let secrets = AgentSecretStore::memory();
        secrets
            .store(SecretKind::CallMeshApiKey, "fixture-previous-key")
            .expect("previous secret should store");
        let (root, controller) = setup_rollback_fixture("setup-auth-rollback", secrets.clone());
        let config_file = root.join("config/agent.toml");
        let previous_configuration = b"[agent]\nmanagement_web_enabled = false\n";
        std::fs::write(&config_file, previous_configuration).expect("previous config should store");
        for failure in [
            SetupApplyError::CallMeshCredentialRejected,
            SetupApplyError::CallMeshUnavailable,
        ] {
            controller
                .setup
                .begin_validation()
                .expect("validation should begin");
            assert_eq!(
                controller.rollback_setup_error(Some(previous_configuration), false, failure,),
                failure,
                "successful pre-commit rollback must preserve the authentication classification",
            );
            assert_eq!(
                std::fs::read(&config_file).expect("previous config should remain readable"),
                previous_configuration,
            );
            assert_eq!(
                secrets
                    .read(SecretKind::CallMeshApiKey)
                    .expect("previous secret should remain readable")
                    .expect("previous secret should remain present")
                    .expose_secret(),
                "fixture-previous-key",
            );
            let status = controller.setup.status().expect("setup status should read");
            assert_eq!(status.phase, SetupPhase::CredentialsRequired);
            assert!(status.setup_required);
        }

        drop(controller);
        std::fs::remove_dir_all(root).expect("setup rollback fixture should clean up");
    }

    #[test]
    fn setup_post_validation_failure_restores_config_and_secret_before_retry() {
        let secrets = AgentSecretStore::memory();
        secrets
            .store(SecretKind::CallMeshApiKey, "fixture-previous-key")
            .expect("previous secret should store");
        let (root, controller) =
            setup_rollback_fixture("setup-post-validation-rollback", secrets.clone());
        let config_file = root.join("config/agent.toml");
        let previous_configuration = b"[agent]\nmanagement_web_enabled = false\n";
        std::fs::write(&config_file, previous_configuration).expect("previous config should store");
        controller
            .setup
            .begin_validation()
            .expect("validation should begin");

        write_setup_configuration(
            &config_file,
            "172.16.8.88",
            4_403,
            "replacement-network",
            "replacement-gateway",
        )
        .expect("replacement config should store");
        secrets
            .stage_callmesh_setup("fixture-replacement-key")
            .expect("replacement secret should stage");
        secrets
            .promote_callmesh_setup()
            .expect("replacement secret should promote");

        assert_eq!(
            controller.rollback_setup_error(
                Some(previous_configuration),
                true,
                SetupApplyError::SupervisorUnavailable,
            ),
            SetupApplyError::SupervisorUnavailable,
        );
        assert_eq!(
            std::fs::read(&config_file).expect("restored config should read"),
            previous_configuration,
        );
        assert_eq!(
            secrets
                .read(SecretKind::CallMeshApiKey)
                .expect("restored secret should read")
                .expect("restored secret should be present")
                .expose_secret(),
            "fixture-previous-key",
        );
        let status = controller.setup.status().expect("setup status should read");
        assert_eq!(status.phase, SetupPhase::CredentialsRequired);
        assert!(status.setup_required);

        drop(controller);
        std::fs::remove_dir_all(root).expect("setup rollback fixture should clean up");
    }

    #[test]
    fn setup_post_validation_failure_removes_new_config_and_secret() {
        let secrets = AgentSecretStore::memory();
        let (root, controller) =
            setup_rollback_fixture("setup-post-validation-cleanup", secrets.clone());
        let config_file = root.join("config/agent.toml");
        controller
            .setup
            .begin_validation()
            .expect("validation should begin");
        write_setup_configuration(
            &config_file,
            "172.16.8.88",
            4_403,
            "default",
            "cmclient-gateway",
        )
        .expect("new config should store");
        secrets
            .stage_callmesh_setup("fixture-new-key")
            .expect("new secret should stage");
        secrets
            .promote_callmesh_setup()
            .expect("new secret should promote");

        assert_eq!(
            controller.rollback_setup_error(None, true, SetupApplyError::SupervisorUnavailable,),
            SetupApplyError::SupervisorUnavailable,
        );
        assert!(
            !config_file.exists(),
            "new config must be removed on rollback"
        );
        assert!(
            secrets
                .read(SecretKind::CallMeshApiKey)
                .expect("secret backend should remain readable")
                .is_none(),
            "new secret must be removed on rollback",
        );
        let status = controller.setup.status().expect("setup status should read");
        assert_eq!(status.phase, SetupPhase::CredentialsRequired);
        assert!(status.setup_required);

        drop(controller);
        std::fs::remove_dir_all(root).expect("setup rollback fixture should clean up");
    }

    fn setup_crash_fixture(name: &str) -> (std::path::PathBuf, RuntimePaths) {
        let sequence = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock should follow epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "cmclient-agent-{name}-{}-{sequence}",
            std::process::id()
        ));
        let paths = RuntimePaths {
            data_dir: root.clone(),
            config_dir: root.clone(),
            cache_dir: root.join("cache"),
            log_dir: root.join("logs"),
        };
        (root, paths)
    }

    #[test]
    fn setup_crash_during_validating_reopens_and_restores_prior_state() {
        let (root, paths) = setup_crash_fixture("setup-validating-crash");
        let config_file = paths.config_file();
        let previous_configuration = b"[agent]\nmanagement_web_enabled = false\n";
        std::fs::create_dir_all(&root).expect("crash fixture root should create");
        std::fs::write(&config_file, previous_configuration).expect("previous config should store");
        let setup = SetupStore::open(&paths).expect("setup store should initialize");
        setup
            .accept_terms(cmclient_agent_core::setup::CURRENT_TERMS_VERSION)
            .expect("terms should be accepted");
        let generation = setup
            .generation()
            .expect("setup generation should read")
            .generation();
        let transaction_file = setup_transaction_file(&paths);
        write_setup_transaction(&transaction_file, generation, Some(previous_configuration))
            .expect("setup transaction should store");
        setup.begin_validation().expect("validation should begin");
        let secrets = AgentSecretStore::runtime(&root).expect("secret store should initialize");
        secrets
            .store(SecretKind::CallMeshApiKey, "fixture-previous-key")
            .expect("previous key should store");
        secrets
            .stage_callmesh_setup("fixture-candidate-key")
            .expect("candidate key should stage");
        secrets
            .promote_callmesh_setup()
            .expect("candidate key should promote before crash");
        write_setup_configuration(
            &config_file,
            "172.16.8.88",
            4_403,
            "replacement-network",
            "replacement-gateway",
        )
        .expect("replacement config should store");
        let journal = std::fs::read_to_string(&transaction_file)
            .expect("non-secret setup journal should read");
        assert!(!journal.contains("fixture-previous-key"));
        assert!(!journal.contains("fixture-candidate-key"));
        drop(setup);
        drop(secrets);

        let reopened_secrets =
            AgentSecretStore::runtime(&root).expect("secret store should reopen");
        recover_interrupted_setup(&paths, &config_file, &reopened_secrets)
            .expect("interrupted validation should recover");

        assert_eq!(
            std::fs::read(&config_file).expect("restored config should read"),
            previous_configuration,
        );
        assert_eq!(
            reopened_secrets
                .read(SecretKind::CallMeshApiKey)
                .expect("restored key should read")
                .expect("restored key should exist")
                .expose_secret(),
            "fixture-previous-key",
        );
        let reopened_setup = SetupStore::open(&paths).expect("setup store should reopen");
        let status = reopened_setup.status().expect("setup status should read");
        assert_eq!(status.phase, SetupPhase::CredentialsRequired);
        assert!(status.setup_required);
        assert!(!transaction_file.exists());
        std::fs::remove_dir_all(root).expect("crash fixture should clean up");
    }

    #[test]
    fn setup_crash_before_secret_stage_reopens_as_credentials_required() {
        let (root, paths) = setup_crash_fixture("setup-pre-secret-crash");
        let config_file = paths.config_file();
        let setup = SetupStore::open(&paths).expect("setup store should initialize");
        setup
            .accept_terms(cmclient_agent_core::setup::CURRENT_TERMS_VERSION)
            .expect("terms should be accepted");
        let generation = setup
            .generation()
            .expect("setup generation should read")
            .generation();
        let transaction_file = setup_transaction_file(&paths);
        write_setup_transaction(&transaction_file, generation, None)
            .expect("setup transaction should store");
        setup.begin_validation().expect("validation should begin");
        drop(setup);

        let reopened_secrets =
            AgentSecretStore::runtime(&root).expect("secret store should reopen");
        recover_interrupted_setup(&paths, &config_file, &reopened_secrets)
            .expect("pre-secret crash should recover");
        let reopened_setup = SetupStore::open(&paths).expect("setup store should reopen");
        assert_eq!(
            reopened_setup
                .status()
                .expect("setup status should read")
                .phase,
            SetupPhase::CredentialsRequired,
        );
        assert!(!config_file.exists());
        assert!(!transaction_file.exists());
        std::fs::remove_dir_all(root).expect("crash fixture should clean up");
    }

    #[test]
    fn setup_ready_commit_marker_finishes_staged_secret_after_restart() {
        let (root, paths) = setup_crash_fixture("setup-ready-staged-crash");
        let config_file = paths.config_file();
        let setup = SetupStore::open(&paths).expect("setup store should initialize");
        setup
            .accept_terms(cmclient_agent_core::setup::CURRENT_TERMS_VERSION)
            .expect("terms should be accepted");
        let generation = setup
            .generation()
            .expect("setup generation should read")
            .generation();
        let transaction_file = setup_transaction_file(&paths);
        write_setup_transaction(&transaction_file, generation, None)
            .expect("setup transaction should store");
        let fence = setup.begin_validation().expect("validation should begin");
        let secrets = AgentSecretStore::runtime(&root).expect("secret store should initialize");
        secrets
            .stage_callmesh_setup("fixture-committed-key")
            .expect("candidate key should stage");
        write_setup_configuration(
            &config_file,
            "172.16.8.88",
            4_403,
            "committed-network",
            "committed-gateway",
        )
        .expect("committed config should store");
        setup.mark_ready(fence).expect("ready marker should commit");
        drop(setup);
        drop(secrets);

        let reopened_secrets =
            AgentSecretStore::runtime(&root).expect("secret store should reopen");
        recover_interrupted_setup(&paths, &config_file, &reopened_secrets)
            .expect("ready setup should finish committing");
        assert_eq!(
            reopened_secrets
                .read(SecretKind::CallMeshApiKey)
                .expect("committed key should read")
                .expect("committed key should exist")
                .expose_secret(),
            "fixture-committed-key",
        );
        assert_eq!(
            SetupStore::open(&paths)
                .expect("setup store should reopen")
                .status()
                .expect("setup status should read")
                .phase,
            SetupPhase::Ready,
        );
        assert!(!transaction_file.exists());
        std::fs::remove_dir_all(root).expect("crash fixture should clean up");
    }

    #[test]
    fn newer_reset_generation_wins_while_old_transaction_rolls_back() {
        let (root, paths) = setup_crash_fixture("setup-reset-generation-crash");
        let config_file = paths.config_file();
        let previous_configuration = b"[agent]\nmanagement_web_enabled = false\n";
        std::fs::create_dir_all(&root).expect("crash fixture root should create");
        std::fs::write(&config_file, previous_configuration).expect("previous config should store");
        let setup = SetupStore::open(&paths).expect("setup store should initialize");
        setup
            .accept_terms(cmclient_agent_core::setup::CURRENT_TERMS_VERSION)
            .expect("terms should be accepted");
        let generation = setup
            .generation()
            .expect("setup generation should read")
            .generation();
        let transaction_file = setup_transaction_file(&paths);
        write_setup_transaction(&transaction_file, generation, Some(previous_configuration))
            .expect("setup transaction should store");
        setup.begin_validation().expect("validation should begin");
        let secrets = AgentSecretStore::runtime(&root).expect("secret store should initialize");
        secrets
            .store(SecretKind::CallMeshApiKey, "fixture-previous-key")
            .expect("previous key should store");
        secrets
            .stage_callmesh_setup("fixture-candidate-key")
            .expect("candidate key should stage");
        write_setup_configuration(
            &config_file,
            "172.16.8.88",
            4_403,
            "replacement-network",
            "replacement-gateway",
        )
        .expect("replacement config should store");
        setup
            .reset()
            .expect("newer reset should advance generation");
        drop(setup);
        drop(secrets);

        let reopened_secrets =
            AgentSecretStore::runtime(&root).expect("secret store should reopen");
        recover_interrupted_setup(&paths, &config_file, &reopened_secrets)
            .expect("older transaction should rollback without undoing reset");
        assert_eq!(
            std::fs::read(&config_file).expect("restored config should read"),
            previous_configuration,
        );
        assert_eq!(
            reopened_secrets
                .read(SecretKind::CallMeshApiKey)
                .expect("restored key should read")
                .expect("restored key should exist")
                .expose_secret(),
            "fixture-previous-key",
        );
        assert_eq!(
            SetupStore::open(&paths)
                .expect("setup store should reopen")
                .status()
                .expect("setup status should read")
                .phase,
            SetupPhase::TermsRequired,
        );
        assert!(!transaction_file.exists());
        std::fs::remove_dir_all(root).expect("crash fixture should clean up");
    }

    #[test]
    fn setup_crash_after_secret_finalize_reopens_as_committed_ready() {
        let (root, paths) = setup_crash_fixture("setup-committed-crash");
        let config_file = paths.config_file();
        let previous_configuration = b"[agent]\nmanagement_web_enabled = false\n";
        std::fs::create_dir_all(&root).expect("crash fixture root should create");
        std::fs::write(&config_file, previous_configuration).expect("previous config should store");
        let setup = SetupStore::open(&paths).expect("setup store should initialize");
        setup
            .accept_terms(cmclient_agent_core::setup::CURRENT_TERMS_VERSION)
            .expect("terms should be accepted");
        let generation = setup
            .generation()
            .expect("setup generation should read")
            .generation();
        let transaction_file = setup_transaction_file(&paths);
        write_setup_transaction(&transaction_file, generation, Some(previous_configuration))
            .expect("setup transaction should store");
        let fence = setup.begin_validation().expect("validation should begin");
        let secrets = AgentSecretStore::runtime(&root).expect("secret store should initialize");
        secrets
            .store(SecretKind::CallMeshApiKey, "fixture-previous-key")
            .expect("previous key should store");
        secrets
            .stage_callmesh_setup("fixture-committed-key")
            .expect("candidate key should stage");
        write_setup_configuration(
            &config_file,
            "172.16.8.88",
            4_403,
            "committed-network",
            "committed-gateway",
        )
        .expect("committed config should store");
        secrets
            .promote_callmesh_setup()
            .expect("candidate key should promote");
        setup.mark_ready(fence).expect("setup should become ready");
        secrets
            .finalize_callmesh_setup()
            .expect("candidate key should finalize");
        let committed_configuration =
            std::fs::read(&config_file).expect("committed config should read");
        drop(setup);
        drop(secrets);

        let reopened_secrets =
            AgentSecretStore::runtime(&root).expect("secret store should reopen");
        recover_interrupted_setup(&paths, &config_file, &reopened_secrets)
            .expect("committed setup cleanup should recover");
        assert_eq!(
            std::fs::read(&config_file).expect("committed config should remain readable"),
            committed_configuration,
        );
        assert_eq!(
            reopened_secrets
                .read(SecretKind::CallMeshApiKey)
                .expect("committed key should read")
                .expect("committed key should exist")
                .expose_secret(),
            "fixture-committed-key",
        );
        assert_eq!(
            SetupStore::open(&paths)
                .expect("setup store should reopen")
                .status()
                .expect("setup status should read")
                .phase,
            SetupPhase::Ready,
        );
        assert!(!transaction_file.exists());
        std::fs::remove_dir_all(root).expect("crash fixture should clean up");
    }

    fn test_agent_web_state(directory: &Path) -> Arc<AgentWebState> {
        let paths = RuntimePaths {
            data_dir: directory.to_path_buf(),
            config_dir: directory.join("config"),
            cache_dir: directory.join("cache"),
            log_dir: directory.join("logs"),
        };
        let setup = Arc::new(SetupStore::open(&paths).expect("setup store should initialize"));
        let updates =
            Arc::new(AgentUpdateService::new(directory).expect("update service should initialize"));
        Arc::new(
            AgentWebState::new(
                updates,
                setup,
                true,
                AgentLifecycleStatus {
                    schema_version: 1,
                    agent: String::from("running"),
                    gateway: String::from("stopped"),
                    management_web: String::from("running"),
                    management_web_url: Some(String::from("http://127.0.0.1:7080")),
                    uptime_seconds: 1,
                    latest_error_code: Some(String::from("SETUP_REQUIRED")),
                },
            )
            .expect("Agent Web state should initialize"),
        )
    }

    #[test]
    fn cmcloud_gateway_environment_keeps_pairing_and_device_credentials_off_environment() {
        let endpoint = "wss://cmcloud.example.invalid/agent/v1";
        let pairing_code = "pairing-code-fixture-0123456789";
        let device_credential = "device-credential-fixture-0123456789";
        let secrets = AgentSecretStore::memory();
        secrets
            .begin_cmcloud_enrollment(endpoint, pairing_code, "2.0.0-rc.1")
            .expect("pairing transaction should persist");
        secrets
            .record_cmcloud_issued_credential(0, 1, 4, device_credential)
            .expect("issued credential should persist");
        secrets
            .activate_cmcloud_credential(0, 1, 4)
            .expect("credential should activate");
        let credential = secrets
            .cmcloud_active_device_credential()
            .expect("active credential lookup should work")
            .expect("active credential should exist");

        let environment = cmcloud_gateway_environment(endpoint, &credential);
        assert_eq!(
            environment.get("CMCLIENT_CMCLOUD_MODE"),
            Some(&String::from("required"))
        );
        assert_eq!(
            environment.get("CMCLIENT_CMCLOUD_URL"),
            Some(&endpoint.to_owned())
        );
        assert!(!environment.contains_key("CMCLIENT_CMCLOUD_DEVICE_CREDENTIAL"));
        assert!(!environment.keys().any(|key| key.contains("PAIRING")));
        assert!(!environment.values().any(|value| value == pairing_code));
        assert!(!environment.values().any(|value| value == device_credential));
    }

    #[tokio::test]
    async fn cmcloud_enrollment_control_validates_input_and_returns_only_redacted_state() {
        let directory = std::env::temp_dir().join(format!(
            "cmclient-agent-cmcloud-enrollment-control-{}",
            std::process::id(),
        ));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).expect("temporary directory should exist");
        let state = test_agent_web_state(&directory);
        let calls = Arc::new(AtomicUsize::new(0));
        let status = CMCloudEnrollmentStatus {
            schema_version: 1,
            state: CMCloudEnrollmentState::Active,
            endpoint: Some(String::from("wss://cmcloud.example.invalid/agent/v1")),
            installation_generation: Some(7),
            credential_version: Some(3),
        };
        let enrollment_calls = Arc::clone(&calls);
        let enrollment_status = status.clone();
        let status_handler = status.clone();
        state
            .install_cmcloud_enrollment(
                Arc::new(move |request| {
                    assert!(valid_cmcloud_pairing_code(&request.pairing_code));
                    enrollment_calls.fetch_add(1, Ordering::AcqRel);
                    Ok(enrollment_status.clone())
                }),
                Arc::new(move || Ok(status_handler.clone())),
            )
            .expect("enrollment handler should install");
        let router = agent_web_router(Arc::clone(&state));

        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/cmcloud/enrollment")
                    .body(Body::empty())
                    .expect("status request should build"),
            )
            .await
            .expect("status request should respond");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1_024)
            .await
            .expect("status body should read");
        let value: serde_json::Value =
            serde_json::from_slice(&body).expect("status response should be JSON");
        assert_eq!(
            value,
            serde_json::json!({
                "schemaVersion": 1,
                "state": "active",
                "endpoint": "wss://cmcloud.example.invalid/agent/v1",
                "installationGeneration": 7,
                "credentialVersion": 3,
            }),
        );
        let status_text = String::from_utf8(body.to_vec()).expect("status should be UTF-8");
        for forbidden in [
            "pairingCode",
            "deviceCredential",
            "installationId",
            "bootId",
        ] {
            assert!(
                !status_text.contains(forbidden),
                "redacted status must not expose {forbidden}",
            );
        }

        for invalid_body in [
            r#"{"pairingCode":"short"}"#,
            r#"{"pairingCode":"pairing-code-fixture-0123456789/"}"#,
            r#"{"pairingCode":"pairing-code-fixture-0123456789","extra":true}"#,
        ] {
            let response = router
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/api/v1/cmcloud/enrollment")
                        .header("content-type", "application/json")
                        .body(Body::from(invalid_body))
                        .expect("invalid enrollment request should build"),
                )
                .await
                .expect("invalid enrollment request should respond");
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(
                    &to_bytes(response.into_body(), 1_024)
                        .await
                        .expect("invalid response body should read"),
                )
                .expect("invalid response should be JSON"),
                serde_json::json!({"code": "CMCLOUD_ENROLLMENT_REQUEST_INVALID"}),
            );
        }
        assert_eq!(calls.load(Ordering::Acquire), 0);

        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/cmcloud/enrollment")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"pairingCode":"pairing-code-fixture-0123456789"}"#,
                    ))
                    .expect("valid enrollment request should build"),
            )
            .await
            .expect("valid enrollment request should respond");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(calls.load(Ordering::Acquire), 1);

        drop(state);
        std::fs::remove_dir_all(&directory).expect("temporary directory should be removed");
    }

    #[tokio::test]
    async fn active_cmcloud_enrollment_exposes_account_projection() {
        let directory = std::env::temp_dir().join(format!(
            "cmclient-agent-cmcloud-account-projection-control-{}",
            std::process::id(),
        ));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).expect("temporary directory should exist");
        let state = test_agent_web_state(&directory);
        let status = CMCloudEnrollmentStatus {
            schema_version: 1,
            state: CMCloudEnrollmentState::Active,
            endpoint: Some(String::from("wss://cmcloud.example.invalid/agent/v1")),
            installation_generation: Some(7),
            credential_version: Some(3),
        };
        state
            .install_cmcloud_enrollment(
                Arc::new(move |_request| Ok(status.clone())),
                Arc::new(|| {
                    Ok(CMCloudEnrollmentStatus {
                        schema_version: 1,
                        state: CMCloudEnrollmentState::Active,
                        endpoint: Some(String::from("wss://cmcloud.example.invalid/agent/v1")),
                        installation_generation: Some(7),
                        credential_version: Some(3),
                    })
                }),
            )
            .expect("enrollment handler should install");
        let projection = CMCloudAccountProjection {
            projection_type: String::from("account_projection"),
            schema_version: 1,
            revision: 4,
            generation: 7,
            tenant: CMCloudAccountProjectionTenant {
                id: String::from("9660bc4b-bc0a-4d6f-b1a6-2278630b1a4b"),
                name: String::from("Operations"),
            },
            account: CMCloudAccountProjectionAccount {
                issuer: String::from("https://callmesh.example/oidc"),
                subject: String::from("subject-1"),
                display_name: String::from("Operator"),
                email: Some(String::from("operator@example.test")),
                role: CMCloudAccountRole::Operator,
                state: CMCloudAccountState::Approved,
                mapping_freeze_epoch: 1,
                mapping_frozen_at: Some(String::from("2026-08-20T12:00:00Z")),
            },
            stations: vec![CMCloudAccountProjectionStation {
                id: String::from("e83d098c-67f7-4e06-a502-6848c8e6ed65"),
                label: String::from("Mesh gateway"),
                kind: CMCloudStationKind::CmClient,
                state: CMCloudStationState::Online,
                callsign: Some(String::from("BM5GSV-5")),
            }],
            authority: CMCloudAccountProjectionAuthority {
                cmcloud: true,
                epoch: 1,
                revision: 4,
            },
            freshness: CMCloudAccountProjectionFreshness {
                projected_at: String::from("2026-08-20T12:00:00Z"),
                stale_after_ms: 120_000,
            },
            error_state: None,
        };
        assert_eq!(projection.validate(), Ok(()));
        let mut stale_epoch = projection.clone();
        stale_epoch.authority.epoch = 0;
        assert_eq!(
            stale_epoch.validate(),
            Err(CMCloudAccountProjectionControlError::Unavailable)
        );
        state
            .install_cmcloud_account_projection(Arc::new(move || Ok(projection.clone())))
            .expect("account projection handler should install");
        let router = agent_web_router(Arc::clone(&state));

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/api/v1/cmcloud/account-projection")
                    .body(Body::empty())
                    .expect("projection request should build"),
            )
            .await
            .expect("projection request should respond");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 8 * 1024)
            .await
            .expect("projection body should read");
        let value: serde_json::Value =
            serde_json::from_slice(&body).expect("projection response should be JSON");
        assert_eq!(value["account"]["displayName"], "Operator");
        assert!(
            !value["stations"]
                .as_array()
                .unwrap_or(&Vec::new())
                .is_empty()
        );

        drop(state);
        std::fs::remove_dir_all(&directory).expect("temporary directory should be removed");
    }

    #[test]
    fn cmcloud_enrollment_recovery_conflict_has_a_stable_client_status() {
        let error = CMCloudEnrollmentControlError::Enrollment(
            cmcloud_enrollment::CMCloudEnrollmentError::RecoveryRequired,
        );
        assert_eq!(error.status_code(), StatusCode::CONFLICT);
        assert_eq!(error.code(), "CMCLOUD_ENROLLMENT_RECOVERY_REQUIRED");
    }

    #[tokio::test]
    async fn aborted_setup_http_request_cancels_spawn_blocking_transaction() {
        let directory = std::env::temp_dir().join(format!(
            "cmclient-agent-setup-http-cancel-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&directory);
        let state = test_agent_web_state(&directory);
        let started = Arc::new(AtomicBool::new(false));
        let cancellation_observed = Arc::new(AtomicBool::new(false));
        let handler_started = Arc::clone(&started);
        let handler_observed = Arc::clone(&cancellation_observed);
        state
            .install_setup_apply(Arc::new(move |_request, cancellation| {
                handler_started.store(true, Ordering::Release);
                let deadline = Instant::now() + Duration::from_secs(5);
                while !cancellation.is_cancelled() && Instant::now() < deadline {
                    thread::sleep(Duration::from_millis(5));
                }
                handler_observed.store(cancellation.is_cancelled(), Ordering::Release);
                Err(SetupApplyError::Cancelled)
            }))
            .expect("setup apply handler should install");
        let router = agent_web_router(Arc::clone(&state));
        let request = tokio::spawn(async move {
            router
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/api/v1/setup/configure")
                        .header("content-type", "application/json")
                        .body(Body::from(
                            r#"{"meshtasticHost":"127.0.0.1","meshtasticPort":4403,"meshNetworkId":"default","gatewayId":"cmclient-gateway"}"#,
                        ))
                        .expect("setup request should build"),
                )
                .await
        });
        let start_deadline = Instant::now() + Duration::from_secs(2);
        while !started.load(Ordering::Acquire) {
            assert!(
                Instant::now() < start_deadline,
                "spawn-blocking setup handler should start",
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        request.abort();
        let _ = request.await;

        let cancellation_deadline = Instant::now() + Duration::from_secs(2);
        while !cancellation_observed.load(Ordering::Acquire) {
            assert!(
                Instant::now() < cancellation_deadline,
                "aborted HTTP handler should cancel its blocking setup transaction",
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(
            state
                .setup
                .status()
                .expect("setup status should read")
                .phase,
            SetupPhase::TermsRequired,
        );
        drop(state);
        std::fs::remove_dir_all(directory).expect("setup HTTP fixture should clean up");
    }

    struct NoDatabaseMaintenance;

    impl GatewayMaintenanceRunner for NoDatabaseMaintenance {
        fn migrate_database(
            &self,
            _source_database: &Path,
            _staged_database: &Path,
        ) -> Result<GatewayMaintenanceReport, MigrationError> {
            panic!("a configuration-only migration must not start database maintenance")
        }
    }

    #[test]
    fn setup_required_blocks_external_gateway_start_and_physical_smoke_is_explicitly_scoped() {
        let root =
            std::env::temp_dir().join(format!("cmclient-agent-setup-gate-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("fixture root should create");
        let config = AgentConfig {
            paths: RuntimePaths {
                data_dir: root.clone(),
                config_dir: root.clone(),
                cache_dir: root.join("cache"),
                log_dir: root.join("logs"),
            },
            config_file: root.join("config.toml"),
            runtime_profile: AgentRuntimeProfile::Native,
            gateway_command: Some(vec![String::from("cmclient-gateway-fixture")]),
            callmesh: None,
            cmcloud: None,
            meshtastic: Some(MeshtasticConfig {
                mesh_network_id: String::from("fixture"),
                gateway_id: String::from("fixture-gateway"),
                connection: MeshtasticConnectionConfig::Tcp {
                    host: String::from("192.0.2.10"),
                    port: 4_403,
                },
            }),
            aprs: None,
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };
        assert!(setup_gate_required_with_profile(&config, false));
        assert!(!setup_gate_required_with_profile(&config, true));

        let controller =
            AgentController::from_config_with_secrets(&config, AgentSecretStore::memory())
                .expect("controller should initialize in setup state");
        let status = controller.status().expect("status should be available");
        assert_eq!(
            status.gateway,
            cmclient_control_api::GatewayControlStatus::Stopped
        );
        assert_eq!(status.latest_error_code.as_deref(), Some("SETUP_REQUIRED"));
        assert_eq!(
            controller.handle(cmclient_control_api::ControlCommand::Start),
            Err(cmclient_control_api::ControlError::CommandFailed)
        );
        assert_eq!(
            controller
                .status()
                .expect("status should remain available")
                .latest_error_code
                .as_deref(),
            Some("SETUP_REQUIRED")
        );
        drop(controller);
        std::fs::remove_dir_all(root).expect("fixture root should clean up");
    }

    #[test]
    fn management_web_profile_mapping_keeps_docker_fail_closed() {
        let mut native_service = ManagementWebService::start(
            &ManagementWebConfig {
                port: 0,
                profile: management_web_profile(AgentRuntimeProfile::Native),
                ..ManagementWebConfig::default()
            },
            axum::Router::new(),
            None,
            GatewaySessionHandle::new(),
        )
        .expect("native loopback management must not require LAN credentials");
        native_service
            .stop()
            .expect("native management service should stop");

        let config = ManagementWebConfig {
            profile: management_web_profile(AgentRuntimeProfile::Docker),
            ..ManagementWebConfig::default()
        };
        assert!(matches!(
            ManagementWebService::start(
                &config,
                axum::Router::new(),
                None,
                GatewaySessionHandle::new(),
            ),
            Err(ManagementWebError::InvalidConfiguration)
        ));
    }

    #[test]
    fn startup_migrates_product_state_before_loading_the_canonical_config() {
        let sequence = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock should follow epoch")
            .as_nanos();
        let fixture = std::env::temp_dir().join(format!(
            "cmclient-agent-startup-migration-{}-{sequence}",
            std::process::id()
        ));
        let home = fixture.join("home");
        let source = fixture.join("legacy");
        std::fs::create_dir_all(&home).expect("fixture home should exist");
        std::fs::create_dir_all(&source).expect("legacy source should exist");
        let source_config = b"[agent]\nmanagement_web_enabled = false\n";
        std::fs::write(source.join("config.toml"), source_config)
            .expect("legacy configuration should write");
        let mut environment = BTreeMap::from([
            (String::from("HOME"), home.to_string_lossy().into_owned()),
            (
                String::from("USERPROFILE"),
                home.to_string_lossy().into_owned(),
            ),
        ]);
        if cfg!(target_os = "windows") {
            environment.insert(
                String::from("APPDATA"),
                home.join("AppData/Roaming").to_string_lossy().into_owned(),
            );
        }
        let paths = RuntimePaths::from_environment(&environment)
            .expect("runtime paths should derive from the immutable snapshot");
        let source_set = ProductMigrationSourceSet {
            config_root: source.clone(),
            data_root: source.clone(),
        };

        let config = load_agent_config_after_migration_with(
            &environment,
            &paths,
            std::slice::from_ref(&source_set),
            &NoDatabaseMaintenance,
        )
        .expect("startup migration should finish before config parsing");

        assert!(!config.management_web_enabled);
        assert_eq!(config.runtime_profile, AgentRuntimeProfile::Native);
        assert_eq!(
            std::fs::read(paths.config_file()).expect("canonical config should activate"),
            source_config
        );
        assert_eq!(
            std::fs::read(source.join("config.toml")).expect("source should remain"),
            source_config
        );
        let _ = std::fs::remove_dir_all(fixture);
    }

    #[test]
    fn platform_legacy_candidates_are_absolute_and_exclude_the_new_root() {
        let root = std::env::temp_dir().join("cmclient-agent-legacy-candidates");
        let environment = BTreeMap::from([
            (
                String::from("HOME"),
                root.join("home").to_string_lossy().into_owned(),
            ),
            (
                String::from("USERPROFILE"),
                root.join("home").to_string_lossy().into_owned(),
            ),
            (
                String::from("APPDATA"),
                root.join("roaming").to_string_lossy().into_owned(),
            ),
            (
                String::from("PROGRAMDATA"),
                root.join("program-data").to_string_lossy().into_owned(),
            ),
        ]);
        let paths =
            RuntimePaths::from_environment(&environment).expect("runtime paths should derive");
        let candidates = legacy_state_candidates(&environment);

        assert!(!candidates.is_empty());
        assert!(candidates.iter().all(|candidate| {
            candidate.config_root.is_absolute() && candidate.data_root.is_absolute()
        }));
        assert!(
            candidates
                .iter()
                .all(|candidate| candidate.config_root != paths.root_dir()
                    && candidate.data_root != paths.root_dir())
        );
    }

    #[test]
    fn split_legacy_roots_form_one_logical_candidate() {
        let root = std::env::temp_dir().join("cmclient-agent-split-legacy-candidate");
        let config_root = root.join("config/cmclient");
        let data_root = root.join("data/cmclient");
        let mut candidates = Vec::new();

        push_legacy_source_candidate(
            &mut candidates,
            Some(config_root.clone()),
            Some(data_root.clone()),
        );
        push_legacy_source_candidate(
            &mut candidates,
            Some(config_root.clone()),
            Some(data_root.clone()),
        );

        assert_eq!(
            candidates,
            vec![ProductMigrationSourceSet {
                config_root,
                data_root: data_root.clone(),
            }]
        );

        let mut data_only = Vec::new();
        push_legacy_source_candidate(&mut data_only, None, Some(data_root.clone()));
        assert_eq!(
            data_only,
            vec![ProductMigrationSourceSet {
                config_root: data_root.clone(),
                data_root,
            }]
        );
    }

    #[test]
    fn explicit_private_runtime_paths_fail_closed() {
        let root = std::env::temp_dir().join(format!(
            "cmclient-agent-private-runtime-paths-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).expect("private runtime fixture should exist");
        let entrypoint = root.join("main.js");
        std::fs::write(&entrypoint, b"").expect("entrypoint fixture should write");
        let executable = std::env::current_exe().expect("test executable should resolve");

        let invalid_node = BTreeMap::from([
            (
                String::from("CMCLIENT_GATEWAY_ENTRYPOINT"),
                entrypoint.to_string_lossy().into_owned(),
            ),
            (String::from("CMCLIENT_PRIVATE_NODE"), String::from("node")),
        ]);
        assert_eq!(
            resolve_gateway_maintenance_program(&invalid_node),
            Err(String::from("AGENT_PRIVATE_NODE_INVALID"))
        );

        let invalid_entrypoint = BTreeMap::from([
            (
                String::from("CMCLIENT_GATEWAY_ENTRYPOINT"),
                String::from("main.js"),
            ),
            (
                String::from("CMCLIENT_PRIVATE_NODE"),
                executable.to_string_lossy().into_owned(),
            ),
        ]);
        assert_eq!(
            resolve_gateway_maintenance_program(&invalid_entrypoint),
            Err(String::from("AGENT_GATEWAY_ENTRYPOINT_INVALID"))
        );
        std::fs::remove_dir_all(root).expect("private runtime fixture should clean up");
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn private_runtime_process_paths_use_node_compatible_win32_prefixes() {
        assert_eq!(
            normalize_runtime_process_path(std::path::PathBuf::from(
                r"\\?\C:\Program Files\CMClient\runtime\node.exe",
            )),
            std::path::PathBuf::from(r"C:\Program Files\CMClient\runtime\node.exe"),
        );
        assert_eq!(
            normalize_runtime_process_path(std::path::PathBuf::from(
                r"\\?\UNC\server\share\CMClient\gateway\main.js",
            )),
            std::path::PathBuf::from(r"\\server\share\CMClient\gateway\main.js"),
        );
    }

    #[test]
    fn agent_log_recovery_restores_a_persistent_gateway_log_error() {
        let sequence = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock should follow epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "cmclient-agent-log-recovery-{}-{sequence}",
            std::process::id()
        ));
        let make_config = |data_dir: &Path, log_dir: &Path| AgentConfig {
            paths: RuntimePaths {
                data_dir: data_dir.to_path_buf(),
                config_dir: data_dir.join("config"),
                cache_dir: data_dir.join("cache"),
                log_dir: log_dir.to_path_buf(),
            },
            config_file: data_dir.join("config").join("agent.toml"),
            runtime_profile: AgentRuntimeProfile::Native,
            gateway_command: None,
            callmesh: None,
            cmcloud: None,
            meshtastic: None,
            aprs: None,
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };

        let healthy_root = root.join("healthy");
        let healthy_log_dir = healthy_root.join("logs");
        std::fs::create_dir_all(&healthy_root).expect("healthy root should create");
        let healthy = AgentController::from_config_with_secrets(
            &make_config(&healthy_root, &healthy_log_dir),
            AgentSecretStore::memory(),
        )
        .expect("healthy controller should initialize");
        healthy.apply_gateway_log_health(GatewayLogHealthUpdate {
            capture_error_code: Some("RUNTIME_LOG_CAPTURE_READ_FAILED"),
            write_error_code: None,
            write_recovered_code: None,
        });
        assert_eq!(
            healthy
                .latest_error_code
                .lock()
                .expect("latest error should lock")
                .as_deref(),
            Some("RUNTIME_LOG_CAPTURE_READ_FAILED")
        );
        assert_eq!(
            healthy
                .gateway_log_health
                .lock()
                .expect("Gateway log error should lock")
                .capture_error_code
                .as_deref(),
            Some("RUNTIME_LOG_CAPTURE_READ_FAILED")
        );

        let agent_failure = healthy_log_dir.join("agent.jsonl.1");
        std::fs::create_dir(&agent_failure).expect("Agent log failure fixture should create");
        healthy.log_agent_code(LogLevel::Info, "AGENT_HEARTBEAT");
        assert_eq!(
            healthy
                .latest_error_code
                .lock()
                .expect("latest error should lock")
                .as_deref(),
            Some("RUNTIME_LOG_FILE_UNAVAILABLE")
        );
        assert_eq!(
            healthy
                .agent_log_error_code
                .lock()
                .expect("Agent log error should lock")
                .as_deref(),
            Some("RUNTIME_LOG_FILE_UNAVAILABLE")
        );

        std::fs::remove_dir(agent_failure).expect("Agent log failure fixture should remove");
        healthy.log_agent_code(LogLevel::Info, "AGENT_HEARTBEAT");
        assert_eq!(
            healthy
                .agent_log_error_code
                .lock()
                .expect("Agent log error should lock")
                .as_deref(),
            None
        );
        assert_eq!(
            healthy
                .latest_error_code
                .lock()
                .expect("latest error should lock")
                .as_deref(),
            Some("RUNTIME_LOG_CAPTURE_READ_FAILED"),
            "Agent recovery must restore the independent Gateway log failure"
        );
        assert_eq!(
            healthy
                .gateway_log_health
                .lock()
                .expect("Gateway log error should lock")
                .capture_error_code
                .as_deref(),
            Some("RUNTIME_LOG_CAPTURE_READ_FAILED")
        );
        assert_eq!(
            healthy
                .latest_error_code
                .lock()
                .expect("latest error should lock")
                .as_deref(),
            Some("RUNTIME_LOG_CAPTURE_READ_FAILED"),
            "Agent writes must not clear an independent Gateway capture failure"
        );
        drop(healthy);

        let missing_root = root.join("missing");
        let missing_log_dir = missing_root.join("logs");
        std::fs::create_dir_all(missing_log_dir.join("agent.jsonl"))
            .expect("unsafe active log fixture should create");
        let missing = AgentController::from_config_with_secrets(
            &make_config(&missing_root, &missing_log_dir),
            AgentSecretStore::memory(),
        )
        .expect("controller should tolerate an unavailable runtime log");
        assert_eq!(
            missing
                .latest_error_code
                .lock()
                .expect("latest error should lock")
                .as_deref(),
            Some("RUNTIME_LOG_FILE_UNAVAILABLE")
        );
        assert_eq!(
            missing
                .agent_log_error_code
                .lock()
                .expect("Agent log error should lock")
                .as_deref(),
            Some("RUNTIME_LOG_FILE_UNAVAILABLE")
        );
        missing.log_agent_code(LogLevel::Info, "AGENT_HEARTBEAT");
        assert_eq!(
            missing
                .latest_error_code
                .lock()
                .expect("latest error should lock")
                .as_deref(),
            Some("RUNTIME_LOG_FILE_UNAVAILABLE")
        );
        drop(missing);
        std::fs::remove_dir_all(root).expect("test root should remove");
    }

    #[test]
    fn overlapping_gateway_capture_error_and_write_recovery_preserves_capture_health() {
        let sequence = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock should follow epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "cmclient-agent-overlapping-log-health-{}-{sequence}",
            std::process::id()
        ));
        let config = AgentConfig {
            paths: RuntimePaths {
                data_dir: root.clone(),
                config_dir: root.join("config"),
                cache_dir: root.join("cache"),
                log_dir: root.join("logs"),
            },
            config_file: root.join("config").join("agent.toml"),
            runtime_profile: AgentRuntimeProfile::Native,
            gateway_command: None,
            callmesh: None,
            cmcloud: None,
            meshtastic: None,
            aprs: None,
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };
        std::fs::create_dir_all(&root).expect("test root should create");
        let setup = SetupStore::open(&config.paths).expect("setup state should initialize");
        setup
            .accept_terms(cmclient_agent_core::setup::CURRENT_TERMS_VERSION)
            .expect("terms should be accepted");
        let fence = setup.begin_validation().expect("validation should begin");
        setup.mark_ready(fence).expect("setup should become ready");
        let secrets = AgentSecretStore::memory();
        secrets
            .store(SecretKind::CallMeshApiKey, "fixture-callmesh-api-key")
            .expect("fixture secret should store");
        let controller = AgentController::from_config_with_secrets(&config, secrets)
            .expect("controller should initialize");
        controller.apply_gateway_log_health(GatewayLogHealthUpdate {
            capture_error_code: Some("RUNTIME_LOG_CAPTURE_READ_FAILED"),
            write_error_code: Some("RUNTIME_LOG_FILE_UNAVAILABLE"),
            write_recovered_code: Some("RUNTIME_LOG_FILE_UNAVAILABLE"),
        });

        let health = controller
            .gateway_log_health
            .lock()
            .expect("Gateway log health should lock");
        assert_eq!(
            health.capture_error_code.as_deref(),
            Some("RUNTIME_LOG_CAPTURE_READ_FAILED")
        );
        assert_eq!(health.write_error_code, None);
        drop(health);
        assert_eq!(
            controller
                .status()
                .expect("status should derive persistent log health")
                .latest_error_code
                .as_deref(),
            Some("RUNTIME_LOG_CAPTURE_READ_FAILED"),
            "matching write recovery must not clear the overlapping capture failure"
        );

        drop(controller);
        std::fs::remove_dir_all(root).expect("test root should remove");
    }

    #[cfg(not(target_os = "windows"))]
    fn read_runtime_log_family(directory: &Path, file_name: &str) -> String {
        let mut paths = std::fs::read_dir(directory)
            .expect("runtime log directory should read")
            .map(|entry| entry.expect("runtime log entry should read").path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| {
                        name == file_name || name.starts_with(&format!("{file_name}."))
                    })
            })
            .collect::<Vec<_>>();
        paths.sort();
        paths
            .iter()
            .map(|path| std::fs::read_to_string(path).expect("runtime log should read"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn gateway_fixture_marker_waits_for_committed_content() {
        let mut observed_contents = [String::new(), String::from("ok")].into_iter();

        assert_eq!(
            wait_for_fixture_marker_with("ok", Duration::from_secs(2), || {
                Ok(observed_contents
                    .next()
                    .expect("marker should be accepted after the committed observation"))
            }),
            "ok",
        );
        assert!(observed_contents.next().is_none());
    }

    #[test]
    fn rejects_a_gateway_with_a_different_product_identity() {
        let capability = "d".repeat(64);
        let listener = TcpListener::bind("127.0.0.1:0").expect("identity fixture should bind");
        let address = listener.local_addr().expect("fixture address should load");
        let expected = compiled_component_identity(InternalComponent::Agent)
            .expect("compiled Agent identity should load");
        let mut response_identity = expected.clone();
        response_identity.component = InternalComponent::Gateway;
        response_identity.identity.source_commit = "e".repeat(40);
        if response_identity.identity.source_commit == expected.identity.source_commit {
            response_identity.identity.source_commit = "f".repeat(40);
        }
        let body = serde_json::to_vec(&response_identity).expect("identity should serialize");
        let expected_capability = capability.clone();
        let fixture = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("identity request should connect");
            use std::io::{Read as _, Write as _};
            let mut request = [0_u8; 4096];
            let count = stream
                .read(&mut request)
                .expect("identity request should read");
            let request = std::str::from_utf8(&request[..count]).expect("request should be UTF-8");
            assert!(request.contains(&format!(
                "{}: {expected_capability}\r\n",
                super::GATEWAY_CAPABILITY_HEADER,
            )));
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len(),
            )
            .and_then(|()| stream.write_all(&body))
            .expect("identity response should write");
        });
        let ready = cmclient_supervisor::GatewayReady {
            pid: std::process::id(),
            address,
            startup_nonce: "c".repeat(32),
            capability,
        };

        assert_eq!(
            verified_gateway_route(&ready, &expected),
            Err(ControlError::CommandFailed)
        );
        fixture.join().expect("identity fixture should join");
    }

    #[cfg(target_os = "windows")]
    #[test]
    #[ignore = "requires a built production Gateway and Node; run by Windows source qualification"]
    fn supervised_real_gateway_uses_private_dynamic_session() {
        let private_node = std::env::var_os("CMCLIENT_PRIVATE_NODE")
            .map(std::path::PathBuf::from)
            .expect("Windows source qualification requires CMCLIENT_PRIVATE_NODE");
        assert!(private_node.is_absolute(), "private Node must be absolute");
        let private_node_metadata =
            std::fs::symlink_metadata(&private_node).expect("private Node metadata should load");
        assert!(
            private_node_metadata.is_file() && !private_node_metadata.file_type().is_symlink(),
            "private Node must be a regular non-link file"
        );
        use std::os::windows::fs::MetadataExt;
        assert_eq!(
            private_node_metadata.file_attributes() & 0x0400,
            0,
            "private Node must not be a reparse point"
        );
        let private_node =
            std::fs::canonicalize(private_node).expect("private Node should canonicalize");
        let version = std::process::Command::new(&private_node)
            .arg("--version")
            .env_clear()
            .output()
            .expect("private Node should start without PATH");
        assert!(version.status.success(), "private Node version should run");
        assert_eq!(
            String::from_utf8(version.stdout)
                .expect("private Node version should be UTF-8")
                .trim(),
            "v24.18.0"
        );
        let data_dir = std::env::temp_dir().join(format!(
            "cmclient-agent-private-gateway-{}",
            std::process::id(),
        ));
        let _ = std::fs::remove_dir_all(&data_dir);
        std::fs::create_dir_all(&data_dir).expect("test directory should exist");
        let gateway_entry = Path::new(env!("CARGO_MANIFEST_DIR")).join("../gateway/dist/main.js");
        assert!(
            gateway_entry.is_file(),
            "Gateway production entrypoint should be built"
        );
        let reserved =
            TcpListener::bind(("127.0.0.1", 0)).expect("fixed-port sentinel should bind");
        let fixed_port = reserved
            .local_addr()
            .expect("fixed-port sentinel address should load")
            .port();
        let config = AgentConfig {
            paths: RuntimePaths {
                data_dir: data_dir.clone(),
                config_dir: data_dir.join("config"),
                cache_dir: data_dir.join("cache"),
                log_dir: data_dir.join("logs"),
            },
            config_file: data_dir.join("agent.toml"),
            runtime_profile: AgentRuntimeProfile::Native,
            gateway_command: Some(vec![
                private_node.to_string_lossy().into_owned(),
                gateway_entry.to_string_lossy().into_owned(),
            ]),
            callmesh: None,
            cmcloud: None,
            meshtastic: None,
            aprs: None,
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };
        let controller = AgentController::from_config(&config).expect("controller should build");

        let started = controller
            .handle(super::ControlCommand::Start)
            .expect("real supervised Gateway should start");
        let (ready, supervised_pid) = {
            let supervisor = controller
                .supervisor
                .lock()
                .expect("supervisor lock should open");
            let supervisor = supervisor.as_ref().expect("supervisor should exist");
            let ready = supervisor
                .gateway_ready()
                .cloned()
                .expect("private Gateway session should be published");
            let supervised_pid = match supervisor.status() {
                cmclient_supervisor::GatewayStatus::Running { pid } => pid,
                status => panic!("Gateway should be running, got {status:?}"),
            };
            (ready, supervised_pid)
        };
        let route = controller
            .gateway_session
            .snapshot()
            .expect("Agent route should be available");
        let (route_address, route_capability) = {
            let active_route = route.active().expect("Agent route should be active");
            (active_route.address(), active_route.capability().to_owned())
        };
        assert_eq!(route_address, ready.address);
        assert!(
            route_capability == ready.capability,
            "Gateway session capability did not match bootstrap"
        );
        assert_eq!(ready.pid, supervised_pid);
        assert_ne!(route_address.port(), fixed_port);
        assert_eq!(route_address.ip().to_string(), "127.0.0.1");
        assert_eq!(ready.startup_nonce.len(), 32);
        assert!(
            ready
                .startup_nonce
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        );
        assert_eq!(route_capability.len(), 64);
        assert!(!format!("{route:?}").contains(&route_capability));
        assert!(!format!("{:?}", controller.gateway_session).contains(&route_capability));

        let direct = reqwest::blocking::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .build()
            .expect("test client should build");
        let health_url = format!("http://{route_address}/api/v1/system/health");
        assert_eq!(
            direct
                .get(&health_url)
                .send()
                .expect("direct request should complete")
                .status()
                .as_u16(),
            403
        );
        let wrong_capability = if route_capability.starts_with('f') {
            "e".repeat(64)
        } else {
            "f".repeat(64)
        };
        assert_eq!(
            direct
                .get(&health_url)
                .header(super::GATEWAY_CAPABILITY_HEADER, wrong_capability)
                .send()
                .expect("wrong-capability request should complete")
                .status()
                .as_u16(),
            403
        );
        assert_eq!(
            controller
                .gateway_projection(cmclient_control_api::GatewayProjection::Meshtastic)
                .expect("Agent should authenticate the Meshtastic projection"),
            serde_json::json!({ "configured": false })
        );
        assert_eq!(
            TcpListener::bind(route_address)
                .expect_err("private Gateway address should already be owned")
                .kind(),
            std::io::ErrorKind::AddrInUse
        );

        let stopped = controller
            .handle(super::ControlCommand::Stop)
            .expect("real supervised Gateway should stop");
        assert!(controller.gateway_session.snapshot().is_none());
        assert!(!route.is_active(), "stale route clone should be revoked");
        assert!(
            TcpStream::connect_timeout(&route_address, Duration::from_millis(250)).is_err(),
            "stopped private Gateway address should close"
        );
        drop(reserved);
        assert_eq!(started.gateway, super::GatewayControlStatus::Running);
        assert_eq!(stopped.gateway, super::GatewayControlStatus::Stopped);

        let startup_nonce = ready.startup_nonce;
        let capability = ready.capability;
        drop(controller);
        assert!(!directory_contains_bytes(
            &data_dir,
            startup_nonce.as_bytes()
        ));
        assert!(!directory_contains_bytes(&data_dir, capability.as_bytes()));
        std::fs::remove_dir_all(&data_dir).expect("test directory should remove");
    }

    #[cfg(target_os = "windows")]
    fn directory_contains_bytes(root: &Path, needle: &[u8]) -> bool {
        std::fs::read_dir(root)
            .expect("test root should be readable")
            .filter_map(Result::ok)
            .any(|entry| {
                let path = entry.path();
                if path.is_dir() {
                    directory_contains_bytes(&path, needle)
                } else {
                    std::fs::read(path).is_ok_and(|bytes| {
                        bytes.windows(needle.len()).any(|window| window == needle)
                    })
                }
            })
    }

    #[test]
    fn derives_exact_workspace_identity_for_the_gateway_child() {
        let report = compiled_component_identity(InternalComponent::Agent).unwrap();
        assert_eq!(report.identity.product, "CMClient");
        assert_eq!(report.identity.channel.as_str(), "dev");
        report.validate().unwrap();
        assert_eq!(report.identity.source_commit.len(), 40);
        assert!(
            report.identity.source_tree.len() == 40
                || (report.identity.source_tree.len() == 71
                    && report.identity.source_tree.starts_with("sha256:"))
        );
    }

    #[test]
    fn aprs_environment_contains_only_enablement_and_operator_overrides() {
        let mut environment = BTreeMap::new();
        apply_aprs_environment(
            &mut environment,
            Some(&AprsConfig {
                host: None,
                port: None,
                destination: None,
            }),
        );
        assert_eq!(
            environment,
            BTreeMap::from([(String::from("CMCLIENT_APRS_ENABLED"), String::from("true"),)])
        );

        apply_aprs_environment(
            &mut environment,
            Some(&AprsConfig {
                host: Some(String::from("operator.aprs.example")),
                port: Some(14_580),
                destination: Some(String::from("APCM20")),
            }),
        );
        assert_eq!(
            environment.get("CMCLIENT_APRS_HOST").map(String::as_str),
            Some("operator.aprs.example")
        );
        assert_eq!(
            environment.get("CMCLIENT_APRS_PORT").map(String::as_str),
            Some("14580")
        );
        assert_eq!(
            environment
                .get("CMCLIENT_APRS_DESTINATION")
                .map(String::as_str),
            Some("APCM20")
        );
        for forbidden in [
            "CMCLIENT_APRS_LOGIN_CALLSIGN",
            "CMCLIENT_APRS_PASSCODE",
            "CMCLIENT_APRS_SYMBOL_TABLE",
            "CMCLIENT_APRS_SYMBOL_CODE",
            "CMCLIENT_APRS_COMMENT",
        ] {
            assert!(
                !environment.contains_key(forbidden),
                "Agent must not inject {forbidden}",
            );
        }
    }

    #[test]
    fn setup_environment_disables_a_stale_proxy_flag() {
        let mut environment =
            BTreeMap::from([(String::from("CMCLIENT_PROXY_ENABLED"), String::from("true"))]);

        disable_proxy_for_setup(&mut environment);

        assert_eq!(
            environment
                .get("CMCLIENT_PROXY_ENABLED")
                .map(String::as_str),
            Some("false")
        );
    }

    #[test]
    fn physical_qualification_environment_is_explicit_and_bounded() {
        let mut environment = BTreeMap::new();
        apply_physical_qualification_environment(
            &mut environment,
            Some("true"),
            Some("windows-source-smoke"),
        )
        .expect("physical qualification environment should validate");
        assert_eq!(
            environment
                .get("CMCLIENT_MESHTASTIC_PHYSICAL_PROFILE")
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            environment
                .get("CMCLIENT_QUALIFICATION_STAGE")
                .map(String::as_str),
            Some("windows-source-smoke")
        );

        assert!(
            apply_physical_qualification_environment(
                &mut BTreeMap::new(),
                Some("true"),
                Some("unsafe stage"),
            )
            .is_err()
        );
        let mut disabled = BTreeMap::new();
        apply_physical_qualification_environment(&mut disabled, Some("false"), None)
            .expect("disabled physical qualification should be accepted");
        assert!(disabled.is_empty());
    }

    #[test]
    fn controller_rejects_new_aprs_passcodes_but_removes_a_legacy_value() {
        let directory = std::env::temp_dir().join(format!(
            "cmclient-agent-legacy-aprs-cleanup-{}",
            std::process::id(),
        ));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).expect("temporary directory should exist");
        let secrets = AgentSecretStore::memory();
        secrets
            .store(SecretKind::AprsPasscode, "synthetic-legacy-passcode")
            .expect("legacy fixture should be seeded directly");
        let config = AgentConfig {
            paths: RuntimePaths {
                data_dir: directory.clone(),
                config_dir: directory.join("config"),
                cache_dir: directory.join("cache"),
                log_dir: directory.join("logs"),
            },
            config_file: directory.join("agent.toml"),
            runtime_profile: AgentRuntimeProfile::Native,
            gateway_command: None,
            callmesh: None,
            cmcloud: None,
            meshtastic: None,
            aprs: None,
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };
        let controller = AgentController::from_config_with_secrets(&config, secrets.clone())
            .expect("controller should initialize");

        assert_eq!(
            controller.store_secret(ControlSecretKind::AprsPasscode, "replacement"),
            Err(ControlError::SecretKindDeprecated),
        );
        assert!(
            controller
                .remove_secret(ControlSecretKind::AprsPasscode)
                .expect("legacy value should be removable")
        );
        assert!(
            secrets
                .read(SecretKind::AprsPasscode)
                .expect("secret backend should remain readable")
                .is_none()
        );

        drop(controller);
        std::fs::remove_dir_all(directory).expect("temporary directory should be removed");
    }

    #[test]
    fn setup_transaction_fences_concurrent_callmesh_secret_mutation() {
        let directory = std::env::temp_dir().join(format!(
            "cmclient-agent-setup-secret-fence-{}",
            std::process::id(),
        ));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).expect("temporary directory should exist");
        let secrets = AgentSecretStore::memory();
        secrets
            .store(SecretKind::CallMeshApiKey, "original-fixture-key")
            .expect("fixture key should store");
        let config = AgentConfig {
            paths: RuntimePaths {
                data_dir: directory.clone(),
                config_dir: directory.clone(),
                cache_dir: directory.join("cache"),
                log_dir: directory.join("logs"),
            },
            config_file: directory.join("agent.toml"),
            runtime_profile: AgentRuntimeProfile::Native,
            gateway_command: None,
            callmesh: None,
            cmcloud: None,
            meshtastic: None,
            aprs: None,
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };
        let controller = AgentController::from_config_with_secrets(&config, secrets.clone())
            .expect("controller should initialize");

        let setup_guard = controller
            .setup_transaction
            .lock()
            .expect("setup transaction should lock");
        assert_eq!(
            controller.store_secret(ControlSecretKind::CallMeshApiKey, "racing-key"),
            Err(ControlError::ResourceExhausted),
        );
        assert_eq!(
            controller.remove_secret(ControlSecretKind::CallMeshApiKey),
            Err(ControlError::ResourceExhausted),
        );
        let original = secrets
            .read(SecretKind::CallMeshApiKey)
            .expect("fixture key should remain readable")
            .expect("fixture key should remain present");
        assert_eq!(original.expose_secret(), "original-fixture-key");
        drop(setup_guard);

        controller
            .store_secret(ControlSecretKind::CallMeshApiKey, "replacement-fixture-key")
            .expect("secret mutation should resume after setup transaction");
        let replacement = secrets
            .read(SecretKind::CallMeshApiKey)
            .expect("replacement key should remain readable")
            .expect("replacement key should remain present");
        assert_eq!(replacement.expose_secret(), "replacement-fixture-key");

        drop(controller);
        std::fs::remove_dir_all(directory).expect("temporary directory should be removed");
    }

    #[tokio::test]
    async fn management_web_never_exposes_the_local_control_protocol() {
        let directory = std::env::temp_dir().join(format!(
            "cmclient-agent-no-network-control-{}",
            std::process::id(),
        ));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).expect("temporary directory should exist");
        let router = agent_web_router(test_agent_web_state(&directory));
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/control/status")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let response = to_bytes(response.into_body(), 1_024)
            .await
            .expect("response body should read");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&response)
                .expect("response should be JSON"),
            serde_json::json!({"code": "CONTROL_ROUTE_NOT_FOUND"}),
        );
        let login = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/auth/login")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(
            login.status(),
            StatusCode::NOT_FOUND,
            "login authority belongs only to agent-core middleware",
        );

        std::fs::remove_dir_all(directory).expect("temporary directory should be removed");
    }

    #[tokio::test]
    async fn management_update_routes_are_axum_json_and_sse() {
        let directory = std::env::temp_dir().join(format!(
            "cmclient-agent-axum-updates-{}",
            std::process::id(),
        ));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).expect("temporary directory should exist");
        let router = agent_web_router(test_agent_web_state(&directory));

        let status = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/updates")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(status.status(), StatusCode::OK);
        let body = to_bytes(status.into_body(), 4_096)
            .await
            .expect("status body should read");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).expect("status should be JSON"),
            serde_json::json!({"schemaVersion": 1, "job": null}),
        );

        let events = router
            .oneshot(
                Request::builder()
                    .uri("/api/v1/updates/events")
                    .header("last-event-id", "gateway-999")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(events.status(), StatusCode::OK);
        assert_eq!(
            events
                .headers()
                .get("content-type")
                .and_then(|value| value.to_str().ok()),
            Some("text/event-stream"),
        );
        let mut body = events.into_body();
        let first_event = tokio::time::timeout(Duration::from_secs(1), async {
            let mut encoded = Vec::new();
            loop {
                let frame = body
                    .frame()
                    .await
                    .expect("SSE stream should yield an initial snapshot")
                    .expect("SSE frame should encode");
                if let Ok(data) = frame.into_data() {
                    encoded.extend_from_slice(&data);
                    if encoded.windows(2).any(|window| window == b"\n\n") {
                        return encoded;
                    }
                }
            }
        })
        .await
        .expect("initial SSE snapshot should not block");
        let first_event =
            String::from_utf8(first_event).expect("initial SSE snapshot should be UTF-8");
        assert!(first_event.contains("id: agent:update:"));
        assert!(first_event.contains("event: update.status"));
        assert!(!first_event.contains("gateway-999"));
        drop(body);

        std::fs::remove_dir_all(directory).expect("temporary directory should be removed");
    }

    #[tokio::test]
    async fn management_setup_and_lifecycle_routes_use_the_shared_redacted_contract() {
        let directory =
            std::env::temp_dir().join(format!("cmclient-agent-axum-setup-{}", std::process::id(),));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).expect("temporary directory should exist");
        let state = test_agent_web_state(&directory);
        let initial_generation = state
            .setup
            .generation()
            .expect("generation should load")
            .generation();
        let mut management = ManagementWebService::start(
            &ManagementWebConfig {
                port: 0,
                setup_generation: initial_generation,
                setup_required: true,
                ..ManagementWebConfig::default()
            },
            Router::new(),
            None,
            GatewaySessionHandle::new(),
        )
        .expect("management policy should start");
        let management_setup = management.setup_state();
        state
            .attach_management_setup_state(management_setup.clone())
            .expect("management setup state should attach");
        let router = agent_web_router(Arc::clone(&state));

        let status = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/setup/status")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("setup status should respond");
        assert_eq!(status.status(), StatusCode::OK);
        let status = to_bytes(status.into_body(), 4_096)
            .await
            .expect("setup status should read");
        let status: serde_json::Value =
            serde_json::from_slice(&status).expect("setup status should be JSON");
        assert_eq!(status["phase"], "terms_required");
        assert_eq!(status["recoveryRequired"], false);
        assert!(status.get("setupGeneration").is_none());

        let terms = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/setup/terms")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"termsVersion":"cmclient-2.0-terms-v1"}"#))
                    .expect("request should build"),
            )
            .await
            .expect("terms route should respond");
        assert_eq!(terms.status(), StatusCode::OK);
        let terms: serde_json::Value = serde_json::from_slice(
            &to_bytes(terms.into_body(), 4_096)
                .await
                .expect("terms response should read"),
        )
        .expect("terms response should be JSON");
        assert_eq!(terms["phase"], "credentials_required");

        for invalid_body in [
            r#"{"termsVersion":"cmclient-2.0-terms-v1","extra":true}"#,
            r#"{"termsVersion":42}"#,
            r#"{"termsVersion":"unterminated""#,
        ] {
            let invalid = router
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/api/v1/setup/terms")
                        .header("content-type", "application/json")
                        .body(Body::from(invalid_body))
                        .expect("invalid request should build"),
                )
                .await
                .expect("invalid setup request should respond");
            assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(
                    &to_bytes(invalid.into_body(), 4_096)
                        .await
                        .expect("invalid setup response should read"),
                )
                .expect("invalid setup response should be JSON"),
                serde_json::json!({"code": "SETUP_REQUEST_INVALID"}),
            );
        }

        let lifecycle = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/lifecycle/status")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("lifecycle status should respond");
        assert_eq!(lifecycle.status(), StatusCode::OK);
        let lifecycle: serde_json::Value = serde_json::from_slice(
            &to_bytes(lifecycle.into_body(), 4_096)
                .await
                .expect("lifecycle response should read"),
        )
        .expect("lifecycle response should be JSON");
        assert_eq!(lifecycle["gateway"], "stopped");
        assert!(lifecycle.get("identity").is_none());

        let reset = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/setup/reset")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"confirmation":"operational_reset"}"#))
                    .expect("request should build"),
            )
            .await
            .expect("setup reset should respond");
        assert_eq!(reset.status(), StatusCode::OK);
        assert_eq!(
            state
                .setup
                .generation()
                .expect("generation should load")
                .generation(),
            initial_generation + 1,
        );
        assert_eq!(
            management_setup.snapshot(),
            (initial_generation + 1, true),
            "setup reset must update the live Management Web generation fence before returning",
        );
        let reset_audit = state.audit_snapshot();
        assert_eq!(
            reset_audit,
            vec![
                cmclient_agent_core::access::ManagementAuditEntry {
                    occurred_at_unix_seconds: reset_audit[0].occurred_at_unix_seconds,
                    action: "setup_reset",
                    outcome: "allowed",
                },
                cmclient_agent_core::access::ManagementAuditEntry {
                    occurred_at_unix_seconds: reset_audit[1].occurred_at_unix_seconds,
                    action: "setup_generation",
                    outcome: "changed",
                },
            ],
            "reset audit must contain only stable code-like fields",
        );

        let events = router
            .oneshot(
                Request::builder()
                    .uri("/api/v1/setup/events")
                    .header("last-event-id", "agent:lifecycle:foreign-1")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("setup events should respond");
        assert_eq!(events.status(), StatusCode::OK);
        let mut body = events.into_body();
        let first_event = tokio::time::timeout(Duration::from_secs(1), async {
            let mut encoded = Vec::new();
            loop {
                let frame = body
                    .frame()
                    .await
                    .expect("setup SSE should yield a snapshot")
                    .expect("setup SSE frame should encode");
                if let Ok(data) = frame.into_data() {
                    encoded.extend_from_slice(&data);
                    if encoded.windows(2).any(|window| window == b"\n\n") {
                        return encoded;
                    }
                }
            }
        })
        .await
        .expect("setup snapshot should not block");
        let first_event = String::from_utf8(first_event).expect("setup SSE should be UTF-8");
        assert!(first_event.contains("id: agent:setup:"));
        assert!(first_event.contains("event: setup.status"));
        assert!(!first_event.contains("setupGeneration"));

        management.stop().expect("management policy should stop");
        std::fs::remove_dir_all(directory).expect("temporary directory should be removed");
    }

    #[tokio::test]
    async fn operational_reset_route_requires_confirmation_and_dispatches_the_agent_owner() {
        let directory = std::env::temp_dir().join(format!(
            "cmclient-agent-operational-reset-route-{}",
            std::process::id(),
        ));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).expect("temporary directory should exist");
        let state = test_agent_web_state(&directory);
        let calls = Arc::new(AtomicUsize::new(0));
        let handler_setup = Arc::clone(&state.setup);
        let handler_calls = Arc::clone(&calls);
        state
            .install_operational_reset(Arc::new(move || {
                handler_calls.fetch_add(1, Ordering::AcqRel);
                handler_setup.reset().map_err(Into::into)
            }))
            .expect("operational reset handler should install");
        let router = agent_web_router(Arc::clone(&state));

        let invalid = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/reset/operational")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"confirmation":"wrong"}"#))
                    .expect("invalid request should build"),
            )
            .await
            .expect("invalid reset should respond");
        assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
        assert_eq!(calls.load(Ordering::Acquire), 0);

        let reset = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/reset/operational")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"confirmation":"operational_reset"}"#))
                    .expect("reset request should build"),
            )
            .await
            .expect("operational reset should respond");
        assert_eq!(reset.status(), StatusCode::OK);
        assert_eq!(calls.load(Ordering::Acquire), 1);

        std::fs::remove_dir_all(directory).expect("temporary directory should be removed");
    }

    #[tokio::test]
    async fn agent_event_namespaces_bound_replay_subscribers_and_slow_consumers() {
        for (stream, event_type, epoch, foreign_cursor) in [
            ("setup", "setup.status", 11, "agent:lifecycle:12-1"),
            ("lifecycle", "lifecycle.status", 12, "agent:update:13-1"),
            ("update", "update.status", 13, "agent:setup:11-1"),
        ] {
            let hub = AgentEventHub::new_with_epoch(stream, epoch);
            let first = hub
                .publish(event_type, &serde_json::json!({"sequence": 1}))
                .expect("first event should publish");
            let second = hub
                .publish(event_type, &serde_json::json!({"sequence": 2}))
                .expect("second event should publish");

            let replay = hub
                .subscribe(Some(&first.id))
                .expect("known cursor should subscribe");
            assert_eq!(
                replay
                    .replay
                    .iter()
                    .map(|event| event.id.as_str())
                    .collect::<Vec<_>>(),
                vec![second.id.as_str()],
            );
            drop(replay);
            let foreign = hub
                .subscribe(Some(foreign_cursor))
                .expect("foreign cursor should recover with a snapshot");
            assert_eq!(foreign.replay.len(), 1);
            assert_eq!(foreign.replay[0].id, second.id);
            drop(foreign);

            let slow = hub.subscribe(None).expect("slow subscriber should open");
            for sequence in 0..=AGENT_EVENT_REPLAY_BUFFER {
                hub.publish(event_type, &serde_json::json!({"sequence": sequence + 10}))
                    .expect("event should publish");
            }
            let latest = hub
                .journal
                .lock()
                .expect("journal should lock")
                .retained
                .back()
                .expect("latest event should exist")
                .id
                .clone();
            let expired = hub
                .subscribe(Some(&first.id))
                .expect("expired cursor should recover with a snapshot");
            assert_eq!(expired.replay.len(), 1);
            assert_eq!(expired.replay[0].id, latest);
            drop(expired);
            let AgentEventSubscription {
                mut live,
                _permit: permit,
                ..
            } = slow;
            assert!(matches!(
                live.recv().await,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_))
            ));
            drop(live);
            drop(permit);

            let mut subscribers = Vec::new();
            for _ in 0..AGENT_EVENT_SUBSCRIBER_LIMIT {
                subscribers.push(hub.subscribe(None).expect("subscriber should fit"));
            }
            assert!(matches!(
                hub.subscribe(None),
                Err(AgentEventHubError::SubscriberLimit)
            ));
            subscribers.pop();
            subscribers.push(
                hub.subscribe(None)
                    .expect("released subscriber slot should be reusable"),
            );
            drop(subscribers);

            let restarted = AgentEventHub::new_with_epoch(stream, epoch + 100);
            let restarted_event = restarted
                .publish(event_type, &serde_json::json!({"sequence": 1}))
                .expect("restart snapshot should publish");
            let recovery = restarted
                .subscribe(Some(&first.id))
                .expect("old process cursor should recover");
            assert_eq!(recovery.replay[0].id, restarted_event.id);
            assert_ne!(first.id, restarted_event.id);
        }
    }

    #[tokio::test]
    async fn agent_sse_slow_consumer_closes_with_a_stable_observable_reason() {
        let directory = std::env::temp_dir().join(format!(
            "cmclient-agent-sse-slow-consumer-{}",
            std::process::id(),
        ));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).expect("log directory should exist");
        let sink =
            StructuredLogSink::open(&directory, "agent.jsonl", "agent", LogPolicy::default())
                .expect("runtime log should open");
        let hub = AgentEventHub::new_with_epoch_and_log("setup", 77, Some(sink));
        hub.publish("setup.status", &serde_json::json!({"sequence": 0}))
            .expect("snapshot should publish");
        let response = management_agent_events(&hub, &axum::http::HeaderMap::new());
        let mut body = response.into_body();

        for sequence in 1..=AGENT_EVENT_REPLAY_BUFFER + 1 {
            hub.publish("setup.status", &serde_json::json!({"sequence": sequence}))
                .expect("live event should publish");
        }
        body.frame()
            .await
            .expect("snapshot frame should exist")
            .expect("snapshot frame should encode");
        let error = body
            .frame()
            .await
            .expect("lagged stream should yield a terminal error")
            .expect_err("lagged stream must close instead of continuing silently");
        assert!(error.to_string().contains("AGENT_SSE_SLOW_CONSUMER"));
        drop(body);
        assert_eq!(
            hub.subscribers.load(std::sync::atomic::Ordering::Acquire),
            0,
        );

        let logs = std::fs::read_dir(&directory)
            .expect("log directory should read")
            .map(|entry| {
                std::fs::read_to_string(entry.expect("log entry should read").path())
                    .expect("log file should read")
            })
            .collect::<String>();
        assert!(logs.contains("AGENT_SSE_SLOW_CONSUMER"));
        std::fs::remove_dir_all(directory).expect("log directory should remove");
    }

    #[tokio::test]
    async fn every_agent_sse_route_enforces_and_releases_its_subscriber_cap() {
        let directory = std::env::temp_dir().join(format!(
            "cmclient-agent-sse-route-cap-{}",
            std::process::id(),
        ));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).expect("temporary directory should exist");
        let router = agent_web_router(test_agent_web_state(&directory));

        for path in [
            "/api/v1/setup/events",
            "/api/v1/lifecycle/events",
            "/api/v1/updates/events",
        ] {
            let mut subscribers = Vec::new();
            for _ in 0..AGENT_EVENT_SUBSCRIBER_LIMIT {
                let response = router
                    .clone()
                    .oneshot(
                        Request::builder()
                            .uri(path)
                            .body(Body::empty())
                            .expect("subscriber request should build"),
                    )
                    .await
                    .expect("subscriber request should respond");
                assert_eq!(response.status(), StatusCode::OK);
                subscribers.push(response);
            }
            let rejected = router
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(path)
                        .body(Body::empty())
                        .expect("overflow request should build"),
                )
                .await
                .expect("overflow request should respond");
            assert_eq!(rejected.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(
                    &to_bytes(rejected.into_body(), 1_024)
                        .await
                        .expect("overflow response should read"),
                )
                .expect("overflow response should be JSON"),
                serde_json::json!({"code": "SSE_SUBSCRIBER_LIMIT_REACHED"}),
            );

            drop(subscribers);
            let recovered = router
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(path)
                        .body(Body::empty())
                        .expect("recovery request should build"),
                )
                .await
                .expect("recovery request should respond");
            assert_eq!(recovered.status(), StatusCode::OK);
            drop(recovered);
        }

        std::fs::remove_dir_all(directory).expect("temporary directory should remove");
    }

    #[test]
    fn setup_storage_failures_are_not_reported_as_client_conflicts() {
        for error in [
            SetupError::PathInvalid,
            SetupError::ReadFailed,
            SetupError::Invalid,
            SetupError::WriteFailed,
            SetupError::GenerationExhausted,
        ] {
            assert_eq!(
                setup_error_response(error).status(),
                StatusCode::SERVICE_UNAVAILABLE,
            );
        }
        for error in [SetupError::TransitionInvalid, SetupError::StaleGeneration] {
            assert_eq!(setup_error_response(error).status(), StatusCode::CONFLICT);
        }
    }

    #[test]
    fn reset_generation_reaches_the_next_private_gateway_bootstrap() {
        let directory = std::env::temp_dir().join(format!(
            "cmclient-agent-generation-bootstrap-{}",
            std::process::id(),
        ));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).expect("temporary directory should exist");
        let paths = RuntimePaths {
            data_dir: directory.clone(),
            config_dir: directory.clone(),
            cache_dir: directory.join("cache"),
            log_dir: directory.join("logs"),
        };
        let setup = SetupStore::open(&paths).expect("setup state should initialize");
        setup
            .accept_terms(cmclient_agent_core::setup::CURRENT_TERMS_VERSION)
            .expect("terms should be accepted");
        let fence = setup.begin_validation().expect("validation should begin");
        setup.mark_ready(fence).expect("setup should become ready");
        drop(setup);
        let secrets = AgentSecretStore::memory();
        secrets
            .store(SecretKind::CallMeshApiKey, "fixture-callmesh-api-key")
            .expect("fixture secret should store");
        let missing_gateway = directory.join("missing-private-gateway");
        let config = AgentConfig {
            paths,
            config_file: directory.join("agent.toml"),
            runtime_profile: AgentRuntimeProfile::Native,
            gateway_command: Some(vec![missing_gateway.to_string_lossy().into_owned()]),
            callmesh: None,
            cmcloud: None,
            meshtastic: None,
            aprs: None,
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };
        let controller = AgentController::from_config_with_secrets(&config, secrets)
            .expect("controller should initialize");
        controller.setup.reset().expect("setup should reset");
        controller
            .setup
            .accept_terms(cmclient_agent_core::setup::CURRENT_TERMS_VERSION)
            .expect("terms should be reaccepted");
        let fence = controller
            .setup
            .begin_validation()
            .expect("validation should restart");
        controller
            .setup
            .mark_ready(fence)
            .expect("setup should become ready again");
        let expected_generation = controller
            .setup
            .generation()
            .expect("generation should load")
            .generation();
        assert_eq!(
            controller.start_supervisor(),
            Err(ControlError::CommandFailed),
        );
        assert_eq!(
            controller
                .supervisor
                .lock()
                .expect("supervisor should lock")
                .as_ref()
                .expect("supervisor should exist")
                .configured_setup_generation(),
            expected_generation,
            "the live supervisor must refresh its private bootstrap fence before spawning",
        );
        let _ = controller.stop_supervisor();
        std::fs::remove_dir_all(directory).expect("temporary directory should remove");
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn operational_reset_from_ready_stops_gateway_and_preserves_user_history() {
        let fixture = std::env::temp_dir().join(format!(
            "cmclient-agent-operational-reset-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock should follow epoch")
                .as_nanos(),
        ));
        let home = fixture.join("home");
        let root = home.join(".cmclient");
        let _ = std::fs::remove_dir_all(&fixture);
        std::fs::create_dir_all(&root).expect("fixture root should create");
        let paths = RuntimePaths {
            data_dir: root.clone(),
            config_dir: root.clone(),
            cache_dir: root.join("cache"),
            log_dir: root.join("logs"),
        };
        ensure_runtime_directories(&paths).expect("runtime directories should create");
        let config_file = paths.config_file();
        write_setup_configuration(
            &config_file,
            "127.0.0.1",
            4_403,
            "fixture-network",
            "fixture-gateway",
        )
        .expect("setup configuration should write");
        std::fs::write(paths.database_file(), b"database-canary")
            .expect("database canary should write");
        std::fs::write(paths.backups_dir().join("backup-canary"), b"backup-canary")
            .expect("backup canary should write");
        std::fs::write(paths.updates_dir().join("update-canary"), b"update-canary")
            .expect("update canary should write");
        std::fs::write(paths.log_dir.join("log-canary"), b"log-canary")
            .expect("log canary should write");

        let setup = SetupStore::open(&paths).expect("setup should initialize");
        setup
            .accept_terms(cmclient_agent_core::setup::CURRENT_TERMS_VERSION)
            .expect("terms should be accepted");
        let fence = setup.begin_validation().expect("validation should begin");
        setup.mark_ready(fence).expect("setup should become ready");
        let initial_generation = setup
            .generation()
            .expect("generation should load")
            .generation();
        drop(setup);

        let secrets = AgentSecretStore::memory();
        secrets
            .store(SecretKind::CallMeshApiKey, "fixture-callmesh-api-key")
            .expect("CallMesh key should store");
        secrets
            .store(SecretKind::AprsPasscode, "fixture-aprs-passcode")
            .expect("APRS passcode should store");
        let powershell = std::path::PathBuf::from(
            std::env::var_os("SystemRoot").expect("Windows SystemRoot should exist"),
        )
        .join("System32/WindowsPowerShell/v1.0/powershell.exe");
        let config = AgentConfig {
            paths: paths.clone(),
            config_file: config_file.clone(),
            runtime_profile: AgentRuntimeProfile::Native,
            gateway_command: Some(vec![
                powershell.to_string_lossy().into_owned(),
                String::from("-NoLogo"),
                String::from("-NoProfile"),
                String::from("-NonInteractive"),
                String::from("-Command"),
                String::from("Start-Sleep -Seconds 30"),
            ]),
            callmesh: None,
            cmcloud: None,
            meshtastic: None,
            aprs: None,
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };
        let controller = AgentController::from_config_with_secrets_without_private_bootstrap(
            &config,
            secrets.clone(),
        )
        .expect("controller should initialize");
        controller
            .handle(ControlCommand::Start)
            .expect("Gateway fixture should start");
        controller.gateway_session.set(
            GatewayRoute::new(
                "127.0.0.1:44031"
                    .parse()
                    .expect("route address should parse"),
                "a".repeat(64),
            )
            .expect("route should initialize"),
        );
        assert!(matches!(
            controller
                .supervisor
                .lock()
                .expect("supervisor should lock")
                .as_ref()
                .expect("supervisor should exist")
                .status(),
            cmclient_supervisor::GatewayStatus::Running { .. }
        ));

        let reset = controller
            .operational_reset()
            .expect("operational reset should complete");
        assert!(reset.terms_required && reset.setup_required);
        assert_eq!(
            controller
                .setup
                .generation()
                .expect("generation should load")
                .generation(),
            initial_generation + 1,
        );
        assert!(controller.gateway_session.snapshot().is_none());
        assert!(matches!(
            controller
                .supervisor
                .lock()
                .expect("supervisor should lock")
                .as_ref()
                .expect("supervisor should exist")
                .status(),
            cmclient_supervisor::GatewayStatus::Stopped
        ));
        assert!(!config_file.exists(), "operational config must be removed");
        for kind in SecretKind::ALL {
            assert!(
                secrets
                    .read(kind)
                    .expect("secret state should read")
                    .is_none(),
                "operational reset must clear {kind:?}",
            );
        }
        assert_eq!(
            std::fs::read(paths.database_file()).expect("database should be retained"),
            b"database-canary",
        );
        assert_eq!(
            std::fs::read(paths.backups_dir().join("backup-canary"))
                .expect("backup should be retained"),
            b"backup-canary",
        );
        assert_eq!(
            std::fs::read(paths.updates_dir().join("update-canary"))
                .expect("update state should be retained"),
            b"update-canary",
        );
        assert_eq!(
            std::fs::read(paths.log_dir.join("log-canary")).expect("log should be retained"),
            b"log-canary",
        );
        assert!(reset_completion_file(&paths).is_file());
        assert!(!reset_transaction_file(&paths).exists());
        assert_eq!(
            controller.start_supervisor(),
            Err(ControlError::CommandFailed),
            "setup-required state must fence the old supervisor from restarting",
        );
        let audit = controller.web_state.audit_snapshot();
        assert!(
            audit
                .iter()
                .any(|entry| entry.action == "operational_reset")
        );
        assert!(audit.iter().any(|entry| entry.action == "setup_generation"));

        drop(controller);
        std::fs::remove_dir_all(fixture).expect("fixture should clean up");
    }

    #[test]
    fn interrupted_operational_reset_replays_to_the_same_fence_without_restoring_secrets() {
        let fixture = std::env::temp_dir().join(format!(
            "cmclient-agent-reset-recovery-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock should follow epoch")
                .as_nanos(),
        ));
        let root = fixture.join(".cmclient");
        std::fs::create_dir_all(&root).expect("fixture root should create");
        let paths = RuntimePaths {
            data_dir: root.clone(),
            config_dir: root.clone(),
            cache_dir: root.join("cache"),
            log_dir: root.join("logs"),
        };
        ensure_runtime_directories(&paths).expect("runtime directories should create");
        let config_file = paths.config_file();
        write_setup_configuration(
            &config_file,
            "127.0.0.1",
            4_403,
            "recovery-network",
            "recovery-gateway",
        )
        .expect("configuration should write");
        std::fs::write(paths.database_file(), b"database-canary")
            .expect("database canary should write");

        let setup = SetupStore::open(&paths).expect("setup should initialize");
        setup
            .accept_terms(cmclient_agent_core::setup::CURRENT_TERMS_VERSION)
            .expect("terms should be accepted");
        let fence = setup.begin_validation().expect("validation should begin");
        setup.mark_ready(fence).expect("setup should become ready");
        let target_generation = setup
            .next_reset_generation()
            .expect("reset generation should allocate");
        write_setup_transaction(
            &setup_transaction_file(&paths),
            setup
                .generation()
                .expect("generation should load")
                .generation(),
            Some(&std::fs::read(&config_file).expect("configuration snapshot should read")),
        )
        .expect("stale setup transaction should write");
        write_reset_transaction(&paths, ResetKind::Operational, target_generation)
            .expect("reset journal should write");
        drop(setup);

        let secrets = AgentSecretStore::runtime(&root).expect("secret store should initialize");
        secrets
            .store(SecretKind::CallMeshApiKey, "fixture-prior-key")
            .expect("prior key should store");
        secrets
            .stage_callmesh_setup("fixture-staged-key")
            .expect("staged key should store");
        drop(secrets);

        assert!(recover_interrupted_reset(&paths).expect("reset recovery should finish"));
        let recovered_setup = SetupStore::open(&paths).expect("setup should reopen");
        assert_eq!(
            recovered_setup
                .generation()
                .expect("generation should load")
                .generation(),
            target_generation,
        );
        assert_eq!(
            recovered_setup
                .status()
                .expect("setup status should load")
                .phase,
            SetupPhase::TermsRequired,
        );
        let recovered_secrets =
            AgentSecretStore::runtime(&root).expect("secret store should reopen");
        for kind in SecretKind::ALL {
            assert!(
                recovered_secrets
                    .read(kind)
                    .expect("secret should remain readable")
                    .is_none(),
                "recovery must not restore {kind:?}",
            );
        }
        assert!(!config_file.exists());
        assert!(!setup_transaction_file(&paths).exists());
        assert!(!reset_transaction_file(&paths).exists());
        assert!(reset_completion_file(&paths).is_file());
        assert_eq!(
            std::fs::read(paths.database_file()).expect("database should remain retained"),
            b"database-canary",
        );

        std::fs::remove_dir_all(fixture).expect("fixture should clean up");
    }

    #[test]
    fn factory_reset_fixture_is_confirmed_allowlisted_and_recovers_every_stage() {
        let phases = [
            FactoryResetFixturePhase::Prepared,
            FactoryResetFixturePhase::Quiesced,
            FactoryResetFixturePhase::MutableStateCleared,
            FactoryResetFixturePhase::RootRecreated,
            FactoryResetFixturePhase::Completed,
        ];
        for (index, phase) in phases.into_iter().enumerate() {
            let base = std::env::temp_dir().join(format!(
                "cmclient-factory-reset-fixture-{}-{}-{index}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("clock should follow epoch")
                    .as_nanos(),
            ));
            let root = base.join(".cmclient");
            std::fs::create_dir_all(&root).expect("fixture root should create");
            let paths = RuntimePaths {
                data_dir: root.clone(),
                config_dir: root.clone(),
                cache_dir: root.join("cache"),
                log_dir: root.join("logs"),
            };
            let job = FactoryResetFixtureJob::create(paths.clone())
                .expect("validated fixture job should initialize");
            write_setup_configuration(
                &paths.config_file(),
                "127.0.0.1",
                4_403,
                "fixture-network",
                "fixture-gateway",
            )
            .expect("fixture configuration should write");
            std::fs::write(paths.secrets_file(), b"fixture-secret-document")
                .expect("fixture secret should write");
            std::fs::write(paths.database_file(), b"fixture-database")
                .expect("fixture database should write");
            std::fs::write(paths.cache_dir.join("cache-canary"), b"cache")
                .expect("fixture cache should write");
            std::fs::write(paths.log_dir.join("log-canary"), b"log")
                .expect("fixture log should write");
            std::fs::write(paths.backups_dir().join("backup-canary"), b"backup")
                .expect("fixture backup should write");
            std::fs::write(root.join("unmanaged-canary"), b"unmanaged")
                .expect("unmanaged canary should write");
            let sibling = base.join("sibling-canary");
            std::fs::write(&sibling, b"sibling").expect("sibling canary should write");

            let quiesced = Arc::new(AtomicBool::new(false));
            let quiesced_for_job = quiesced.clone();
            let confirmation = FactoryResetFixtureConfirmation::for_backup_behavior(
                FactoryResetBackupBehavior::RetainExisting,
            );
            assert_eq!(
                job.run_until(
                    confirmation,
                    move || {
                        quiesced_for_job.store(true, Ordering::Release);
                        Ok(())
                    },
                    phase,
                ),
                Err(SetupApplyError::Cancelled),
            );
            assert_eq!(
                quiesced.load(Ordering::Acquire),
                phase != FactoryResetFixturePhase::Prepared,
                "the phase journal must precede any destructive work",
            );
            assert!(sibling.is_file());
            assert!(root.join("unmanaged-canary").is_file());

            job.recover()
                .expect("interrupted factory reset should recover");
            assert!(root.join(".factory-reset-completed.json").is_file());
            assert!(!root.join(".factory-reset-journal.json").exists());
            assert!(!paths.config_file().exists());
            assert!(!paths.secrets_file().exists());
            assert!(!paths.database_file().exists());
            assert!(!paths.cache_dir.join("cache-canary").exists());
            assert!(!paths.log_dir.join("log-canary").exists());
            assert!(
                paths.backups_dir().join("backup-canary").is_file(),
                "the first confirmation explicitly retained backups",
            );
            assert!(root.join("unmanaged-canary").is_file());
            assert!(sibling.is_file());
            assert_eq!(
                SetupStore::open(&paths)
                    .expect("fresh setup should open")
                    .status()
                    .expect("fresh setup status should load")
                    .phase,
                SetupPhase::TermsRequired,
            );
            std::fs::remove_dir_all(base).expect("fixture should clean up");
        }

        let erase_base = std::env::temp_dir().join(format!(
            "cmclient-factory-reset-fixture-{}-erase-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock should follow epoch")
                .as_nanos(),
        ));
        let erase_root = erase_base.join(".cmclient");
        std::fs::create_dir_all(&erase_root).expect("erase fixture root should create");
        let erase_paths = RuntimePaths {
            data_dir: erase_root.clone(),
            config_dir: erase_root.clone(),
            cache_dir: erase_root.join("cache"),
            log_dir: erase_root.join("logs"),
        };
        let erase_job = FactoryResetFixtureJob::create(erase_paths.clone())
            .expect("erase fixture job should initialize");
        std::fs::write(erase_paths.backups_dir().join("backup-canary"), b"backup")
            .expect("erase backup should write");
        assert_eq!(
            erase_job.run(
                FactoryResetFixtureConfirmation {
                    backup_behavior: FactoryResetBackupBehavior::EraseAll,
                    first_confirmation: "not-the-factory-confirmation",
                    final_confirmation: "erase-all-backups",
                },
                || Ok(()),
            ),
            Err(SetupApplyError::InvalidInput),
        );
        assert!(erase_paths.backups_dir().join("backup-canary").is_file());
        erase_job
            .run(
                FactoryResetFixtureConfirmation::for_backup_behavior(
                    FactoryResetBackupBehavior::EraseAll,
                ),
                || Ok(()),
            )
            .expect("explicit erase-all reset should complete");
        assert!(!erase_paths.backups_dir().join("backup-canary").exists());
        assert!(erase_root.join(".factory-reset-completed.json").is_file());
        std::fs::remove_dir_all(erase_base).expect("erase fixture should clean up");

        let real_root = std::env::temp_dir().join(format!(
            "cmclient-reset-real-root-canary-{}",
            std::process::id(),
        ));
        let real_data_root = real_root.join(".cmclient");
        std::fs::create_dir_all(&real_data_root).expect("real-root canary should create");
        let real_canary = real_data_root.join("must-not-delete");
        std::fs::write(&real_canary, b"canary").expect("real-root canary should write");
        let real_paths = RuntimePaths {
            data_dir: real_data_root.clone(),
            config_dir: real_data_root.clone(),
            cache_dir: real_data_root.join("cache"),
            log_dir: real_data_root.join("logs"),
        };
        assert!(
            matches!(
                FactoryResetFixtureJob::create(real_paths),
                Err(SetupApplyError::ConfigWriteFailed)
            ),
            "a normal-looking root must not become a factory target",
        );
        assert!(real_canary.is_file());
        std::fs::remove_dir_all(real_root).expect("real-root canary should clean up");
    }

    #[test]
    fn resident_start_starts_ready_gateway_once_while_worker_is_running() {
        let (directory, controller) =
            ready_supervised_controller("resident-start", gateway_fixture_command());
        let marker = directory.join("gateway-start");
        configure_gateway_fixture(&controller, "wait", &marker);
        let mut worker =
            SupervisorWorker::start(Arc::clone(&controller)).expect("worker should start");

        assert!(
            controller
                .start_resident_supervisor()
                .expect("ready setup should start the resident Gateway")
        );
        assert_eq!(
            wait_for_fixture_marker(&marker, "x", Duration::from_secs(2)),
            "x"
        );
        let first_pid = match controller
            .supervisor
            .lock()
            .expect("supervisor should lock")
            .as_ref()
            .expect("supervisor should exist")
            .status()
        {
            GatewayStatus::Running { pid } => pid,
            status => panic!("resident Gateway should be running, got {status:?}"),
        };

        assert!(
            controller
                .start_resident_supervisor()
                .expect("a repeated resident start should be a heartbeat")
        );
        thread::sleep(Duration::from_millis(100));
        assert_eq!(
            std::fs::read_to_string(&marker).expect("start marker should read"),
            "x",
            "the resident worker must retain one Gateway owner"
        );
        assert_eq!(
            controller
                .supervisor
                .lock()
                .expect("supervisor should lock")
                .as_ref()
                .expect("supervisor should exist")
                .status(),
            GatewayStatus::Running { pid: first_pid }
        );

        worker.stop();
        controller
            .stop_supervisor()
            .expect("resident Gateway should stop");
        drop(controller);
        std::fs::remove_file(marker).expect("start marker should remove");
        std::fs::remove_dir_all(directory).expect("test directory should remove");
    }

    #[test]
    fn setup_block_tick_stops_existing_gateway_and_revokes_its_route() {
        let (directory, controller) =
            ready_supervised_controller("setup-block", gateway_fixture_command());
        let marker = directory.join("gateway-start");
        configure_gateway_fixture(&controller, "wait", &marker);
        controller
            .start_resident_supervisor()
            .expect("ready setup should start the Gateway");
        assert_eq!(
            wait_for_fixture_marker(&marker, "x", Duration::from_secs(2)),
            "x"
        );
        let route_listener = TcpListener::bind("127.0.0.1:0").expect("route fixture should bind");
        controller.gateway_session.set(
            GatewayRoute::new(
                route_listener
                    .local_addr()
                    .expect("route address should resolve"),
                "a".repeat(64),
            )
            .expect("route should be valid"),
        );
        drop(route_listener);

        controller
            .setup
            .require_credentials()
            .expect("setup should become blocked");
        assert!(
            controller
                .tick_supervisor()
                .expect("setup block should drain the existing child")
        );
        assert!(controller.gateway_session.snapshot().is_none());
        assert!(matches!(
            controller
                .supervisor
                .lock()
                .expect("supervisor should lock")
                .as_ref()
                .expect("supervisor should exist")
                .status(),
            GatewayStatus::Stopped
        ));

        drop(controller);
        std::fs::remove_file(marker).expect("start marker should remove");
        std::fs::remove_dir_all(directory).expect("test directory should remove");
    }

    #[test]
    fn failed_gateway_identity_verification_stops_the_unroutable_child() {
        let (directory, controller) =
            ready_supervised_controller("identity-stop", gateway_fixture_command());
        let marker = directory.join("gateway-start");
        configure_gateway_fixture(&controller, "wait", &marker);
        controller
            .start_resident_supervisor()
            .expect("ready setup should start the Gateway");
        assert_eq!(
            wait_for_fixture_marker(&marker, "x", Duration::from_secs(2)),
            "x"
        );
        let route_listener = TcpListener::bind("127.0.0.1:0").expect("route fixture should bind");
        controller.gateway_session.set(
            GatewayRoute::new(
                route_listener
                    .local_addr()
                    .expect("route address should resolve"),
                "b".repeat(64),
            )
            .expect("route should be valid"),
        );
        drop(route_listener);

        let transition = controller
            .gateway_transition
            .lock()
            .expect("transition should lock");
        assert_eq!(
            controller.publish_verified_gateway(None),
            Err(ControlError::CommandFailed)
        );
        drop(transition);

        assert!(controller.gateway_session.snapshot().is_none());
        assert!(matches!(
            controller
                .supervisor
                .lock()
                .expect("supervisor should lock")
                .as_ref()
                .expect("supervisor should exist")
                .status(),
            GatewayStatus::Stopped
        ));
        assert_eq!(
            controller
                .latest_error_code
                .lock()
                .expect("identity failure should be recorded")
                .as_deref(),
            Some("GATEWAY_SUPERVISOR_IDENTITY_VERIFICATION_FAILED")
        );

        drop(controller);
        std::fs::remove_file(marker).expect("start marker should remove");
        std::fs::remove_dir_all(directory).expect("test directory should remove");
    }

    #[test]
    fn explicit_start_respects_an_active_gateway_crash_backoff() {
        let (directory, controller) =
            ready_supervised_controller("start-backoff", gateway_fixture_command());
        let marker = directory.join("gateway-start");
        configure_gateway_fixture(&controller, "crash", &marker);
        controller
            .start_resident_supervisor()
            .expect("first Gateway start should succeed");
        assert_eq!(
            wait_for_fixture_marker(&marker, "x", Duration::from_secs(2)),
            "x"
        );
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            controller
                .tick_supervisor()
                .expect("Gateway exit should enter backoff");
            if matches!(
                controller
                    .supervisor
                    .lock()
                    .expect("supervisor should lock")
                    .as_ref()
                    .expect("supervisor should exist")
                    .status(),
                GatewayStatus::Backoff { attempt: 1, .. }
            ) {
                break;
            }
            assert!(Instant::now() < deadline, "Gateway did not enter backoff");
            thread::sleep(Duration::from_millis(10));
        }

        controller
            .start_resident_supervisor()
            .expect("manual start must retain the active backoff deadline");
        thread::sleep(Duration::from_millis(100));
        assert_eq!(
            std::fs::read_to_string(&marker).expect("start marker should read"),
            "x",
            "manual Start bypassed the supervisor crash backoff"
        );
        assert!(matches!(
            controller
                .supervisor
                .lock()
                .expect("supervisor should lock")
                .as_ref()
                .expect("supervisor should exist")
                .status(),
            GatewayStatus::Backoff { attempt: 1, .. }
        ));

        controller
            .stop_supervisor()
            .expect("backoff supervisor should reset during cleanup");
        drop(controller);
        std::fs::remove_file(marker).expect("start marker should remove");
        std::fs::remove_dir_all(directory).expect("test directory should remove");
    }

    #[tokio::test]
    async fn lifecycle_worker_refreshes_uptime_and_detects_failed_gateway_health() {
        let directory = std::env::temp_dir().join(format!(
            "cmclient-agent-lifecycle-health-{}",
            std::process::id(),
        ));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).expect("temporary directory should exist");
        let paths = RuntimePaths {
            data_dir: directory.clone(),
            config_dir: directory.clone(),
            cache_dir: directory.join("cache"),
            log_dir: directory.join("logs"),
        };
        let setup = SetupStore::open(&paths).expect("setup state should initialize");
        setup
            .accept_terms(cmclient_agent_core::setup::CURRENT_TERMS_VERSION)
            .expect("terms should be accepted");
        let fence = setup.begin_validation().expect("validation should begin");
        setup.mark_ready(fence).expect("setup should become ready");
        drop(setup);
        let secrets = AgentSecretStore::memory();
        secrets
            .store(SecretKind::CallMeshApiKey, "fixture-callmesh-api-key")
            .expect("fixture secret should store");
        let executable = std::env::current_exe().expect("test executable should resolve");
        let config = AgentConfig {
            paths,
            config_file: directory.join("agent.toml"),
            runtime_profile: AgentRuntimeProfile::Native,
            gateway_command: Some(vec![
                executable.to_string_lossy().into_owned(),
                String::from("--ignored"),
                String::from("--exact"),
                String::from("tests::long_running_gateway_fixture"),
            ]),
            callmesh: None,
            cmcloud: None,
            meshtastic: None,
            aprs: None,
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };
        let controller = Arc::new(
            AgentController::from_config_with_secrets_without_private_bootstrap(&config, secrets)
                .expect("controller should initialize"),
        );
        controller
            .handle(ControlCommand::Start)
            .expect("Gateway fixture should start");

        let gateway = TcpListener::bind("127.0.0.1:0").expect("health fixture should bind");
        let gateway_address = gateway.local_addr().expect("health address should load");
        let gateway_thread = thread::spawn(move || {
            let (mut stream, _) = gateway.accept().expect("health request should accept");
            let mut request = [0_u8; 4096];
            let _ = stream
                .read(&mut request)
                .expect("health request should read");
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 15\r\nconnection: close\r\n\r\n{\"status\":\"ok\"}",
                )
                .expect("health response should write");
        });
        controller.gateway_session.set(
            GatewayRoute::new(gateway_address, "c".repeat(64))
                .expect("health route should be valid"),
        );
        let initial_controller = Arc::clone(&controller);
        tokio::task::spawn_blocking(move || initial_controller.publish_lifecycle_snapshot())
            .await
            .expect("health snapshot task should join")
            .expect("healthy lifecycle should publish");
        gateway_thread.join().expect("health fixture should join");

        let router = agent_web_router(Arc::clone(&controller.web_state));
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/lifecycle/events")
                    .body(Body::empty())
                    .expect("lifecycle request should build"),
            )
            .await
            .expect("lifecycle request should respond");
        let mut body = response.into_body();
        let mut worker =
            SupervisorWorker::start(Arc::clone(&controller)).expect("worker should start");

        let observed = tokio::time::timeout(Duration::from_secs(5), async {
            let mut encoded = String::new();
            loop {
                let frame = body
                    .frame()
                    .await
                    .expect("lifecycle stream should remain open")
                    .expect("lifecycle frame should encode");
                if let Ok(data) = frame.into_data() {
                    encoded.push_str(
                        std::str::from_utf8(&data).expect("lifecycle frame should be UTF-8"),
                    );
                    if encoded.contains("\"gateway\":\"degraded\"") {
                        break encoded;
                    }
                }
            }
        })
        .await
        .expect("failed health should publish degraded lifecycle");
        assert!(observed.contains("GATEWAY_HEALTH_DEGRADED"));

        tokio::time::sleep(Duration::from_millis(1_200)).await;
        let status = router
            .oneshot(
                Request::builder()
                    .uri("/api/v1/lifecycle/status")
                    .body(Body::empty())
                    .expect("status request should build"),
            )
            .await
            .expect("status request should respond");
        let status: serde_json::Value = serde_json::from_slice(
            &to_bytes(status.into_body(), 4_096)
                .await
                .expect("status body should read"),
        )
        .expect("status body should be JSON");
        assert!(
            status["uptimeSeconds"]
                .as_u64()
                .is_some_and(|value| value >= 1)
        );

        drop(body);
        worker.stop();
        controller.gateway_session.clear();
        controller
            .handle(ControlCommand::Stop)
            .expect("Gateway fixture should stop");
        drop(controller);
        std::fs::remove_dir_all(directory).expect("temporary directory should remove");
    }

    #[cfg(target_os = "windows")]
    #[tokio::test]
    async fn background_supervisor_publishes_crash_lifecycle_without_a_status_request() {
        let directory = std::env::temp_dir().join(format!(
            "cmclient-agent-lifecycle-worker-{}",
            std::process::id(),
        ));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).expect("temporary directory should exist");
        let paths = RuntimePaths {
            data_dir: directory.clone(),
            config_dir: directory.clone(),
            cache_dir: directory.join("cache"),
            log_dir: directory.join("logs"),
        };
        let setup = SetupStore::open(&paths).expect("setup state should initialize");
        setup
            .accept_terms(cmclient_agent_core::setup::CURRENT_TERMS_VERSION)
            .expect("terms should be accepted");
        let fence = setup.begin_validation().expect("validation should begin");
        setup.mark_ready(fence).expect("setup should become ready");
        let secrets = AgentSecretStore::memory();
        secrets
            .store(SecretKind::CallMeshApiKey, "fixture-callmesh-api-key")
            .expect("fixture secret should store");
        let powershell = std::path::PathBuf::from(
            std::env::var_os("SystemRoot").expect("Windows SystemRoot should exist"),
        )
        .join("System32/WindowsPowerShell/v1.0/powershell.exe");
        let config = AgentConfig {
            paths,
            config_file: directory.join("agent.toml"),
            runtime_profile: AgentRuntimeProfile::Native,
            gateway_command: Some(vec![
                powershell.to_string_lossy().into_owned(),
                String::from("-NoLogo"),
                String::from("-NoProfile"),
                String::from("-NonInteractive"),
                String::from("-Command"),
                String::from("Start-Sleep -Milliseconds 750; exit 7"),
            ]),
            callmesh: None,
            cmcloud: None,
            meshtastic: None,
            aprs: None,
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };
        let controller = Arc::new(
            AgentController::from_config_with_secrets_without_private_bootstrap(&config, secrets)
                .expect("controller should initialize"),
        );
        let response = agent_web_router(Arc::clone(&controller.web_state))
            .oneshot(
                Request::builder()
                    .uri("/api/v1/lifecycle/events")
                    .body(Body::empty())
                    .expect("lifecycle request should build"),
            )
            .await
            .expect("lifecycle request should respond");
        let mut body = response.into_body();
        let mut worker =
            SupervisorWorker::start(Arc::clone(&controller)).expect("worker should start");
        controller
            .handle(ControlCommand::Start)
            .expect("Gateway fixture should start");

        let observed = tokio::time::timeout(Duration::from_secs(5), async {
            let mut encoded = String::new();
            loop {
                let frame = body
                    .frame()
                    .await
                    .expect("lifecycle stream should remain open")
                    .expect("lifecycle frame should encode");
                if let Ok(data) = frame.into_data() {
                    encoded.push_str(
                        std::str::from_utf8(&data).expect("lifecycle frame should be UTF-8"),
                    );
                    if encoded.contains("\"gateway\":\"backoff\"") {
                        break encoded;
                    }
                }
            }
        })
        .await
        .expect("background crash should publish lifecycle promptly");
        assert!(observed.contains("event: lifecycle.status"));
        assert!(observed.contains("GATEWAY_RESTART_BACKOFF"));

        drop(body);
        worker.stop();
        controller
            .handle(ControlCommand::Stop)
            .expect("supervisor should stop");
        drop(controller);
        std::fs::remove_dir_all(directory).expect("temporary directory should remove");
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn configured_callmesh_secret_is_never_added_to_the_gateway_environment() {
        let data_dir = std::env::temp_dir().join(format!(
            "cmclient-agent-plaintext-boundary-{}",
            std::process::id(),
        ));
        let marker = data_dir.join("gateway-environment");
        let _ = std::fs::remove_dir_all(&data_dir);
        std::fs::create_dir_all(&data_dir).expect("test directory should exist");
        let secrets = AgentSecretStore::memory();
        secrets
            .store(SecretKind::CallMeshApiKey, "fixture-callmesh-value")
            .expect("fixture secret should store");
        let config = AgentConfig {
            paths: RuntimePaths {
                data_dir: data_dir.clone(),
                config_dir: data_dir.clone(),
                cache_dir: data_dir.join("cache"),
                log_dir: data_dir.join("logs"),
            },
            config_file: data_dir.join("agent.toml"),
            runtime_profile: AgentRuntimeProfile::Native,
            gateway_command: Some(vec![
                String::from("sh"),
                String::from("-c"),
                format!(
                    "if [ \"$CMCLIENT_CALLMESH_URL\" = \"https://callmesh.example.invalid\" ] && [ -z \"${{CMCLIENT_CALLMESH_API_KEY+x}}\" ] && [ -z \"${{CMCLIENT_PLAINTEXT_SECRET_FILE+x}}\" ]; then printf ok > '{}'; else printf rejected > '{}'; fi; read _",
                    marker.display(),
                    marker.display(),
                ),
            ]),
            callmesh: Some(CallMeshConfig {
                url: String::from("https://callmesh.example.invalid"),
            }),
            cmcloud: None,
            meshtastic: None,
            aprs: None,
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };
        let controller =
            AgentController::from_config_with_secrets_without_private_bootstrap(&config, secrets)
                .expect("controller should initialize");

        controller
            .supervisor
            .lock()
            .expect("supervisor should lock")
            .as_mut()
            .expect("supervisor should be configured")
            .start()
            .expect("gateway fixture should start");
        assert_eq!(
            wait_for_fixture_marker(&marker, "ok", Duration::from_secs(2)),
            "ok",
        );
        controller
            .supervisor
            .lock()
            .expect("supervisor should lock")
            .as_mut()
            .expect("supervisor should be configured")
            .stop()
            .expect("gateway fixture should stop");
        std::fs::remove_dir_all(data_dir).expect("test directory should remove");
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn configured_aprs_forwards_only_operator_endpoint_overrides() {
        let data_dir = std::env::temp_dir().join(format!(
            "cmclient-agent-aprs-boundary-{}",
            std::process::id(),
        ));
        let marker = data_dir.join("gateway-environment");
        let _ = std::fs::remove_dir_all(&data_dir);
        std::fs::create_dir_all(&data_dir).expect("test directory should exist");
        let secrets = AgentSecretStore::memory();
        let config = AgentConfig {
            paths: RuntimePaths {
                data_dir: data_dir.clone(),
                config_dir: data_dir.clone(),
                cache_dir: data_dir.join("cache"),
                log_dir: data_dir.join("logs"),
            },
            config_file: data_dir.join("agent.toml"),
            runtime_profile: AgentRuntimeProfile::Native,
            gateway_command: Some(vec![
                String::from("sh"),
                String::from("-c"),
                format!(
                    "if [ \"$CMCLIENT_APRS_ENABLED\" = \"true\" ] && [ \"$CMCLIENT_APRS_HOST\" = \"asia.aprs2.net\" ] && [ \"$CMCLIENT_APRS_PORT\" = \"14580\" ] && [ \"$CMCLIENT_APRS_DESTINATION\" = \"APCM20\" ] && [ \"${{CMCLIENT_APRS_LOGIN_CALLSIGN+x}}\" != x ] && [ \"${{CMCLIENT_APRS_PASSCODE+x}}\" != x ] && [ \"${{CMCLIENT_APRS_SYMBOL_TABLE+x}}\" != x ] && [ \"${{CMCLIENT_APRS_SYMBOL_CODE+x}}\" != x ] && [ \"${{CMCLIENT_APRS_COMMENT+x}}\" != x ]; then printf ok > '{}'; else printf rejected > '{}'; fi; read _",
                    marker.display(),
                    marker.display(),
                ),
            ]),
            callmesh: None,
            cmcloud: None,
            meshtastic: None,
            aprs: Some(AprsConfig {
                host: Some(String::from("asia.aprs2.net")),
                port: Some(14_580),
                destination: Some(String::from("APCM20")),
            }),
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };
        let controller =
            AgentController::from_config_with_secrets_without_private_bootstrap(&config, secrets)
                .expect("controller should initialize");

        controller
            .supervisor
            .lock()
            .expect("supervisor should lock")
            .as_mut()
            .expect("supervisor should be configured")
            .start()
            .expect("gateway fixture should start");
        assert_eq!(
            wait_for_fixture_marker(&marker, "ok", Duration::from_secs(2)),
            "ok",
        );
        controller
            .supervisor
            .lock()
            .expect("supervisor should lock")
            .as_mut()
            .expect("supervisor should be configured")
            .stop()
            .expect("gateway fixture should stop");
        std::fs::remove_dir_all(data_dir).expect("test directory should remove");
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn reports_running_only_after_gateway_health_succeeds() {
        let gateway = TcpListener::bind("127.0.0.1:0").expect("gateway should bind");
        let gateway_address = gateway.local_addr().expect("gateway address should load");
        let gateway_thread = thread::spawn(move || {
            let (mut stream, _) = gateway.accept().expect("gateway should accept");
            let mut request = [0_u8; 4096];
            let _ = stream
                .read(&mut request)
                .expect("health request should read");
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 15\r\nconnection: close\r\n\r\n{\"status\":\"ok\"}",
                )
                .expect("health response should write");
        });
        let paths = RuntimePaths {
            data_dir: PathBuf::from("/tmp/cmclient-agent-health"),
            config_dir: PathBuf::from("/tmp/cmclient-agent-health"),
            cache_dir: PathBuf::from("/tmp/cmclient-agent-health/cache"),
            log_dir: PathBuf::from("/tmp/cmclient-agent-health/logs"),
        };
        let (secrets, cmcloud) = cmcloud_fixture(&paths);
        let config = AgentConfig {
            paths,
            config_file: PathBuf::from("/tmp/cmclient-agent-health/agent.toml"),
            runtime_profile: AgentRuntimeProfile::Native,
            gateway_command: Some(vec![
                String::from("sh"),
                String::from("-c"),
                String::from("read _"),
            ]),
            callmesh: None,
            cmcloud: Some(cmcloud),
            meshtastic: None,
            aprs: None,
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };
        let controller =
            AgentController::from_config_with_secrets_without_private_bootstrap(&config, secrets)
                .expect("controller should build");
        let diagnostics = controller
            .diagnostics_bundle()
            .expect("sanitized diagnostics should build");
        let serialized =
            serde_json::to_string(&diagnostics).expect("sanitized diagnostics should serialize");
        assert_eq!(diagnostics.schema_version, 2);
        assert!(!serialized.contains("/tmp/cmclient-agent-health"));
        assert!(!serialized.contains("gateway_command"));

        controller.gateway_session.set(
            cmclient_agent_core::web::GatewayRoute::new(gateway_address, "a".repeat(64))
                .expect("test route should be valid"),
        );

        let status = controller
            .handle(ControlCommand::Start)
            .expect("gateway should start");
        assert_eq!(status.gateway, GatewayControlStatus::Running);
        assert_eq!(status.management_web, ManagementWebControlStatus::Disabled);
        controller
            .handle(ControlCommand::Stop)
            .expect("gateway should stop");
        gateway_thread.join().expect("gateway should join");
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn restarts_a_crashed_gateway_without_control_requests() {
        let marker =
            std::env::temp_dir().join(format!("cmclient-agent-supervisor-{}", std::process::id()));
        let _ = std::fs::remove_file(&marker);
        let paths = RuntimePaths {
            data_dir: PathBuf::from("/tmp/cmclient-agent-supervisor"),
            config_dir: PathBuf::from("/tmp/cmclient-agent-supervisor"),
            cache_dir: PathBuf::from("/tmp/cmclient-agent-supervisor/cache"),
            log_dir: PathBuf::from("/tmp/cmclient-agent-supervisor/logs"),
        };
        let (secrets, cmcloud) = cmcloud_fixture(&paths);
        let config = AgentConfig {
            paths,
            config_file: PathBuf::from("/tmp/cmclient-agent-supervisor/agent.toml"),
            runtime_profile: AgentRuntimeProfile::Native,
            gateway_command: Some(vec![
                String::from("sh"),
                String::from("-c"),
                format!("printf x >> '{}'; (sleep 10) & exit 7", marker.display()),
            ]),
            callmesh: None,
            cmcloud: Some(cmcloud),
            meshtastic: None,
            aprs: None,
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };
        let controller = Arc::new(
            AgentController::from_config_with_secrets_without_private_bootstrap(&config, secrets)
                .expect("controller should initialize"),
        );
        let mut worker =
            SupervisorWorker::start(Arc::clone(&controller)).expect("worker should start");

        controller
            .handle(ControlCommand::Start)
            .expect("first gateway process should start");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(4);
        loop {
            let starts = std::fs::read(&marker).map_or(0, |contents| contents.len());
            if starts >= 2 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "background supervisor did not restart the gateway"
            );
            std::thread::sleep(std::time::Duration::from_millis(25));
        }

        worker.stop();
        controller
            .handle(ControlCommand::Stop)
            .expect("supervisor should stop");
        std::fs::remove_file(marker).expect("marker should remove");
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn terminal_shutdown_rejects_resource_starting_commands() {
        let data_dir = std::env::temp_dir().join(format!(
            "cmclient-agent-shutdown-fence-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&data_dir);
        std::fs::create_dir_all(&data_dir).expect("test directory should exist");
        let config = AgentConfig {
            paths: RuntimePaths {
                data_dir: data_dir.clone(),
                config_dir: data_dir.clone(),
                cache_dir: data_dir.join("cache"),
                log_dir: data_dir.join("logs"),
            },
            config_file: data_dir.join("agent.toml"),
            runtime_profile: AgentRuntimeProfile::Native,
            gateway_command: Some(vec![
                String::from("sh"),
                String::from("-c"),
                String::from("exit 0"),
            ]),
            callmesh: None,
            cmcloud: None,
            meshtastic: None,
            aprs: None,
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };
        let controller =
            AgentController::from_config(&config).expect("controller should initialize");

        controller.request_shutdown();

        for command in [
            ControlCommand::Start,
            ControlCommand::Restart,
            ControlCommand::EnableManagementWeb,
        ] {
            assert_eq!(
                controller.handle(command),
                Err(cmclient_control_api::ControlError::CommandFailed)
            );
        }
        for command in [
            ControlCommand::Status,
            ControlCommand::Stop,
            ControlCommand::DisableManagementWeb,
            ControlCommand::ShutdownAgent,
        ] {
            controller
                .handle(command)
                .expect("non-starting command should remain safe during teardown");
        }
        assert!(
            controller
                .management_web
                .lock()
                .expect("management service should lock")
                .is_none()
        );

        std::fs::remove_dir_all(data_dir).expect("test directory should remove");
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn queued_start_rechecks_shutdown_after_acquiring_the_supervisor_lock() {
        let data_dir = std::env::temp_dir().join(format!(
            "cmclient-agent-queued-start-fence-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&data_dir);
        std::fs::create_dir_all(&data_dir).expect("test directory should exist");
        let paths = RuntimePaths {
            data_dir: data_dir.clone(),
            config_dir: data_dir.clone(),
            cache_dir: data_dir.join("cache"),
            log_dir: data_dir.join("logs"),
        };
        let (secrets, cmcloud) = cmcloud_fixture(&paths);
        let config = AgentConfig {
            paths,
            config_file: data_dir.join("agent.toml"),
            runtime_profile: AgentRuntimeProfile::Native,
            gateway_command: Some(vec![
                String::from("sh"),
                String::from("-c"),
                String::from("sleep 5"),
            ]),
            callmesh: None,
            cmcloud: Some(cmcloud),
            meshtastic: None,
            aprs: None,
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };
        let controller = Arc::new(
            AgentController::from_config_with_secrets_without_private_bootstrap(&config, secrets)
                .expect("controller should initialize"),
        );
        let supervisor = controller
            .supervisor
            .lock()
            .expect("supervisor should lock");
        let (ready_sender, ready_receiver) = mpsc::sync_channel(1);
        let start_controller = Arc::clone(&controller);
        let start_thread = thread::spawn(move || {
            ready_sender
                .send(())
                .expect("queued start should announce itself");
            start_controller.handle(ControlCommand::Start)
        });
        ready_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("queued start should begin");
        thread::sleep(Duration::from_millis(25));
        assert!(!start_thread.is_finished());

        controller.request_shutdown();
        drop(supervisor);

        let start_result = start_thread.join().expect("queued start should join");
        let gateway_status = controller
            .supervisor
            .lock()
            .expect("supervisor should lock")
            .as_ref()
            .expect("supervisor should exist")
            .status();
        let _ = controller.stop_supervisor();
        assert_eq!(
            start_result,
            Err(cmclient_control_api::ControlError::CommandFailed)
        );
        assert!(matches!(
            gateway_status,
            cmclient_supervisor::GatewayStatus::Stopped
        ));

        std::fs::remove_dir_all(data_dir).expect("test directory should remove");
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn restart_rechecks_shutdown_after_draining_the_old_child() {
        let data_dir = std::env::temp_dir().join(format!(
            "cmclient-agent-restart-fence-{}",
            std::process::id()
        ));
        let marker = data_dir.join("shutdown-observed");
        let _ = std::fs::remove_dir_all(&data_dir);
        std::fs::create_dir_all(&data_dir).expect("test directory should exist");
        let config = AgentConfig {
            paths: RuntimePaths {
                data_dir: data_dir.clone(),
                config_dir: data_dir.clone(),
                cache_dir: data_dir.join("cache"),
                log_dir: data_dir.join("logs"),
            },
            config_file: data_dir.join("agent.toml"),
            runtime_profile: AgentRuntimeProfile::Native,
            gateway_command: Some(vec![
                String::from("sh"),
                String::from("-c"),
                format!("read _; printf x > '{}'; exec sleep 5", marker.display()),
            ]),
            callmesh: None,
            cmcloud: None,
            meshtastic: None,
            aprs: None,
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };
        let controller = Arc::new(
            AgentController::from_config_with_secrets_without_private_bootstrap(
                &config,
                AgentSecretStore::memory(),
            )
            .expect("controller should initialize"),
        );
        controller
            .supervisor
            .lock()
            .expect("supervisor should lock")
            .as_mut()
            .expect("supervisor should exist")
            .start()
            .expect("initial Gateway should start");
        let restart_controller = Arc::clone(&controller);
        let restart_thread =
            thread::spawn(move || restart_controller.handle(ControlCommand::Restart));
        wait_for_resource_count(
            || usize::from(marker.exists()),
            1,
            "restart did not begin draining the old Gateway",
        );

        controller
            .handle(ControlCommand::ShutdownAgent)
            .expect("shutdown command should latch before waiting for restart");
        assert!(controller.is_shutdown_requested());

        let restart_result = restart_thread.join().expect("restart should join");
        let gateway_status = controller
            .supervisor
            .lock()
            .expect("supervisor should lock")
            .as_ref()
            .expect("supervisor should exist")
            .status();
        let _ = controller.stop_supervisor();
        assert_eq!(
            restart_result,
            Err(cmclient_control_api::ControlError::CommandFailed)
        );
        assert!(matches!(
            gateway_status,
            cmclient_supervisor::GatewayStatus::Stopped
        ));

        std::fs::remove_dir_all(data_dir).expect("test directory should remove");
    }

    #[test]
    fn local_shutdown_ack_precedes_agent_control_teardown() {
        let data_dir = std::env::temp_dir().join(format!(
            "cmclient-agent-shutdown-ack-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&data_dir);
        std::fs::create_dir_all(&data_dir).expect("test directory should exist");
        let config = AgentConfig {
            paths: RuntimePaths {
                data_dir: data_dir.clone(),
                config_dir: data_dir.clone(),
                cache_dir: data_dir.join("cache"),
                log_dir: data_dir.join("logs"),
            },
            config_file: data_dir.join("agent.toml"),
            runtime_profile: AgentRuntimeProfile::Native,
            gateway_command: None,
            callmesh: None,
            cmcloud: None,
            meshtastic: None,
            aprs: None,
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };
        let controller = Arc::new(
            AgentController::from_config_with_secrets_without_private_bootstrap(
                &config,
                AgentSecretStore::memory(),
            )
            .expect("controller should initialize"),
        );
        let endpoint = default_local_endpoint(&data_dir).expect("endpoint should derive");
        let handler: Arc<dyn ControlHandler> = controller.clone();
        let server = ControlServer::bind(endpoint.clone(), handler).expect("server should bind");
        let server_controller = Arc::clone(&controller);
        let server_thread = thread::spawn(move || {
            while !server.poll_once().expect("server poll should succeed") {}
            let deadline = Instant::now() + Duration::from_secs(1);
            while !server_controller.is_shutdown_requested() {
                assert!(
                    Instant::now() < deadline,
                    "shutdown should commit after its response is sent"
                );
                thread::sleep(Duration::from_millis(2));
            }
            drop(server);
        });

        let shutdown_status = ControlClient::new(endpoint)
            .expect("client should initialize")
            .shutdown_agent()
            .expect("shutdown response should arrive before Agent teardown");
        assert_eq!(shutdown_status.agent, "running");
        server_thread.join().expect("server thread should join");
        assert!(controller.is_shutdown_requested());

        drop(controller);
        std::fs::remove_dir_all(data_dir).expect("test directory should remove");
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn teardown_stops_the_gateway_while_a_control_event_stream_retains_the_controller() {
        let data_dir =
            std::env::temp_dir().join(format!("cmclient-agent-teardown-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&data_dir);
        std::fs::create_dir_all(&data_dir).expect("test directory should exist");
        let paths = RuntimePaths {
            data_dir: data_dir.clone(),
            config_dir: data_dir.clone(),
            cache_dir: data_dir.join("cache"),
            log_dir: data_dir.join("logs"),
        };
        let (secrets, cmcloud) = cmcloud_fixture(&paths);
        let config = AgentConfig {
            paths,
            config_file: data_dir.join("agent.toml"),
            runtime_profile: AgentRuntimeProfile::Native,
            gateway_command: Some(vec![
                String::from("sh"),
                String::from("-c"),
                String::from("read _"),
            ]),
            callmesh: None,
            cmcloud: Some(cmcloud),
            meshtastic: None,
            aprs: None,
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };
        let controller = Arc::new(
            AgentController::from_config_with_secrets_without_private_bootstrap(&config, secrets)
                .expect("controller should initialize"),
        );
        let mut worker =
            SupervisorWorker::start(Arc::clone(&controller)).expect("worker should start");
        controller
            .handle(ControlCommand::Start)
            .expect("gateway should start");

        let endpoint = default_local_endpoint(&data_dir).expect("endpoint should derive");
        let handler: Arc<dyn ControlHandler> = controller.clone();
        let server =
            Arc::new(ControlServer::bind(endpoint.clone(), handler).expect("server should bind"));
        let polling_server = Arc::clone(&server);
        let server_thread = thread::spawn(move || polling_server.serve_once());
        let mut events = ControlClient::new(endpoint.clone())
            .expect("client should initialize")
            .subscribe_update_events()
            .expect("control event stream should connect");
        server_thread
            .join()
            .expect("server should join")
            .expect("server should accept the event subscription");
        let initial_event = events
            .next_event()
            .expect("control event should decode")
            .expect("initial control event should arrive");
        assert_eq!(initial_event.event, "update.status_changed");
        assert!(Arc::strong_count(&controller) >= 3);
        let shutdown_server = Arc::clone(&server);
        let shutdown_thread = thread::spawn(move || shutdown_server.serve_once());
        ControlClient::new(endpoint)
            .expect("shutdown client should initialize")
            .shutdown_agent()
            .expect("shutdown response should arrive before Agent teardown");
        shutdown_thread
            .join()
            .expect("shutdown server should join")
            .expect("shutdown server should accept the request");
        assert!(controller.is_shutdown_requested());

        drop(server);
        assert!(
            events
                .next_event()
                .expect("server shutdown should close the control event stream")
                .is_none()
        );
        shutdown_agent_runtime(&controller, &mut worker).expect("Agent should tear down");
        assert!(matches!(
            controller
                .supervisor
                .lock()
                .expect("supervisor should lock")
                .as_ref()
                .expect("supervisor should exist")
                .status(),
            cmclient_supervisor::GatewayStatus::Stopped
        ));
        drop(events);
        std::fs::remove_dir_all(data_dir).expect("test directory should remove");
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn status_tick_preserves_the_precise_supervisor_error_code() {
        let data_dir = std::env::temp_dir().join(format!(
            "cmclient-agent-supervisor-error-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&data_dir);
        std::fs::create_dir_all(&data_dir).expect("test directory should exist");
        let missing_program = data_dir.join("missing-gateway");
        let paths = RuntimePaths {
            data_dir: data_dir.clone(),
            config_dir: data_dir.clone(),
            cache_dir: data_dir.join("cache"),
            log_dir: data_dir.join("logs"),
        };
        let (secrets, cmcloud) = cmcloud_fixture(&paths);
        let config = AgentConfig {
            paths,
            config_file: data_dir.join("agent.toml"),
            runtime_profile: AgentRuntimeProfile::Native,
            gateway_command: Some(vec![missing_program.to_string_lossy().into_owned()]),
            callmesh: None,
            cmcloud: Some(cmcloud),
            meshtastic: None,
            aprs: None,
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };
        let controller =
            AgentController::from_config_with_secrets_without_private_bootstrap(&config, secrets)
                .expect("controller should initialize");
        let mut supervisor = GatewaySupervisor::new_with_stable_window(
            GatewayCommand {
                program: missing_program.to_string_lossy().into_owned(),
                arguments: Vec::new(),
            },
            BackoffPolicy {
                initial_delay: std::time::Duration::from_millis(10),
                maximum_delay: std::time::Duration::from_millis(10),
            },
            std::time::Duration::from_secs(1),
        )
        .expect("supervisor should initialize");
        assert!(supervisor.start().is_err());
        *controller
            .supervisor
            .lock()
            .expect("supervisor should lock") = Some(supervisor);
        std::thread::sleep(std::time::Duration::from_millis(20));

        assert_eq!(
            controller.handle(ControlCommand::Status),
            Err(cmclient_control_api::ControlError::CommandFailed)
        );
        assert_eq!(
            controller
                .latest_error_code
                .lock()
                .expect("error code should lock")
                .as_deref(),
            Some("GATEWAY_SUPERVISOR_SPAWN_FAILED")
        );
        std::fs::remove_dir_all(data_dir).expect("test directory should remove");
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn service_logs_agent_lifecycle_and_preserves_gateway_log_failures() {
        let sequence = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock should follow epoch")
            .as_nanos();
        let data_dir = std::env::temp_dir().join(format!(
            "cmclient-agent-runtime-logging-{}-{sequence}",
            std::process::id()
        ));
        let log_dir = data_dir.join("logs");
        std::fs::create_dir_all(log_dir.join("gateway.jsonl"))
            .expect("unsafe gateway log fixture should create");
        let paths = RuntimePaths {
            data_dir: data_dir.clone(),
            config_dir: data_dir.clone(),
            cache_dir: data_dir.join("cache"),
            log_dir: log_dir.clone(),
        };
        let (secrets, cmcloud) = cmcloud_fixture(&paths);
        let config = AgentConfig {
            paths,
            config_file: data_dir.join("agent.toml"),
            runtime_profile: AgentRuntimeProfile::Native,
            gateway_command: Some(vec![
                String::from("sh"),
                String::from("-c"),
                String::from("read _"),
            ]),
            callmesh: None,
            cmcloud: Some(cmcloud),
            meshtastic: None,
            aprs: None,
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };
        let controller =
            AgentController::from_config_with_secrets_without_private_bootstrap(&config, secrets)
                .expect("controller should initialize");

        let started = controller
            .handle(ControlCommand::Start)
            .expect("gateway should start without a logging sink");
        assert_eq!(
            started.latest_error_code.as_deref(),
            Some("RUNTIME_LOG_FILE_UNAVAILABLE")
        );
        let heartbeat = controller
            .handle(ControlCommand::Status)
            .expect("healthy supervisor tick should succeed");
        assert_eq!(
            heartbeat.latest_error_code.as_deref(),
            Some("RUNTIME_LOG_FILE_UNAVAILABLE")
        );
        let stopped = controller
            .handle(ControlCommand::Stop)
            .expect("gateway should stop");
        assert_eq!(
            stopped.latest_error_code.as_deref(),
            Some("RUNTIME_LOG_FILE_UNAVAILABLE")
        );

        let agent_log = read_runtime_log_family(&log_dir, "agent.jsonl");
        assert!(agent_log.contains("AGENT_RUNTIME_READY"));
        assert!(agent_log.contains("GATEWAY_SUPERVISOR_STARTED"));
        assert!(agent_log.contains("GATEWAY_SUPERVISOR_STOPPED"));
        assert!(!agent_log.contains(data_dir.to_string_lossy().as_ref()));
        for line in agent_log.lines() {
            serde_json::from_str::<serde_json::Value>(line)
                .expect("agent log lines should be structured JSON");
        }

        drop(controller);
        std::fs::remove_dir_all(data_dir).expect("test directory should remove");
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn persists_update_jobs_and_emits_follow_up_sse_status() {
        let data_dir =
            std::env::temp_dir().join(format!("cmclient-agent-updates-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&data_dir);
        let service = AgentUpdateService::new(&data_dir).expect("update service should initialize");
        let events = service.subscribe().expect("subscription should initialize");
        let initial = events
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("initial snapshot should arrive");
        let initial: UpdateControlStatus =
            serde_json::from_slice(&initial.data).expect("initial status should deserialize");
        assert!(initial.job.is_none());
        let job = PersistentUpdateJob {
            schema_version: 1,
            id: String::from("update-1"),
            phase: UpdatePhase::Downloading,
            created_at: String::from("2026-07-18T03:00:00.000Z"),
            updated_at: String::from("2026-07-18T03:01:00.000Z"),
            error_code: None,
            progress: None,
            recent_logs: Vec::new(),
            rollback_plan: None,
        };

        service.persist(&job).expect("job should persist");

        let event = events
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("status transition should arrive");
        let status: UpdateControlStatus =
            serde_json::from_slice(&event.data).expect("status should deserialize");
        assert_eq!(
            status.job.as_ref().map(|job| job.phase.as_str()),
            Some("downloading")
        );
        assert_eq!(service.status().expect("status should load"), status);
        let _ = std::fs::remove_dir_all(data_dir);
    }

    #[test]
    fn gateway_projection_rejects_a_body_completed_after_route_rotation() {
        use std::io::{Read as _, Write as _};

        let listener = TcpListener::bind("127.0.0.1:0").expect("Gateway fixture should bind");
        let address = listener.local_addr().expect("fixture address should load");
        let (partial_body_sent, partial_body_observed) = std::sync::mpsc::sync_channel(1);
        let (finish_body, finish_body_requested) = std::sync::mpsc::sync_channel(1);
        let fixture = thread::spawn(move || {
            let (mut stream, _) = listener
                .accept()
                .expect("projection request should connect");
            let mut request = Vec::new();
            let mut chunk = [0_u8; 2_048];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let count = stream.read(&mut chunk).expect("request should read");
                assert!(count > 0, "projection request ended before headers");
                request.extend_from_slice(&chunk[..count]);
            }
            let request = String::from_utf8(request)
                .expect("request should be UTF-8")
                .to_ascii_lowercase();
            assert!(request.contains(&format!(
                "{}: {}\r\n",
                super::GATEWAY_CAPABILITY_HEADER,
                "e".repeat(64)
            )));

            let body = br#"{"generation":"old"}"#;
            let midpoint = body.len() / 2;
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len(),
            )
            .and_then(|()| stream.write_all(&body[..midpoint]))
            .and_then(|()| stream.flush())
            .expect("partial projection should write");
            partial_body_sent
                .send(())
                .expect("partial body marker should send");
            finish_body_requested
                .recv_timeout(Duration::from_secs(2))
                .expect("rotation should release the fixture body");
            let _ = stream.write_all(&body[midpoint..]);
            let _ = stream.flush();
        });

        let route = cmclient_agent_core::web::GatewayRoute::new(address, "e".repeat(64))
            .expect("Gateway route should be valid");
        let session = GatewaySessionHandle::with_route(route.clone());
        let projection_route = route.clone();
        let projection = thread::spawn(move || {
            gateway_json_projection(
                &projection_route,
                cmclient_control_api::GatewayProjection::Nodes,
            )
        });
        partial_body_observed
            .recv_timeout(Duration::from_secs(2))
            .expect("projection should start reading the old body");

        let replacement = cmclient_agent_core::web::GatewayRoute::new(address, "f".repeat(64))
            .expect("replacement route should be valid");
        let rotation = thread::spawn(move || session.set(replacement));
        let deadline = Instant::now() + Duration::from_secs(2);
        while route.is_active() {
            assert!(
                Instant::now() < deadline,
                "route rotation did not revoke the in-flight generation"
            );
            thread::yield_now();
        }
        finish_body
            .send(())
            .expect("fixture should finish the old body");

        assert_eq!(
            projection.join().expect("projection worker should join"),
            Err(cmclient_control_api::ControlError::CommandFailed),
            "a complete stale projection must fail closed",
        );
        rotation.join().expect("route rotation should drain");
        fixture.join().expect("Gateway fixture should join");
    }

    #[test]
    fn route_rotation_prevents_old_gateway_events_from_reaching_control() {
        use std::io::{Read as _, Write as _};

        let listener = TcpListener::bind("127.0.0.1:0").expect("Gateway fixture should bind");
        let address = listener.local_addr().expect("fixture address should load");
        let (write_stale_event, stale_event_requested) = std::sync::mpsc::sync_channel(1);
        let fixture = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("event request should connect");
            let mut request = Vec::new();
            let mut chunk = [0_u8; 2_048];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let count = stream.read(&mut chunk).expect("request should read");
                assert!(count > 0, "event request ended before headers");
                request.extend_from_slice(&chunk[..count]);
            }
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream; charset=utf-8\r\nconnection: close\r\n\r\n: ready\n\n",
                )
                .and_then(|()| stream.flush())
                .expect("event response should start");
            stale_event_requested
                .recv_timeout(Duration::from_secs(2))
                .expect("rotation should release the stale event");
            let _ = stream.write_all(
                b"id: stale-1\nevent: mesh.position\ndata: {\"generation\":\"old\"}\n\n",
            );
            let _ = stream.flush();
        });

        let route = cmclient_agent_core::web::GatewayRoute::new(address, "1".repeat(64))
            .expect("Gateway route should be valid");
        let session = GatewaySessionHandle::with_route(route.clone());
        let (sender, receiver) = std::sync::mpsc::sync_channel(16);
        let bridge_route = route.clone();
        let bridge = thread::spawn(move || {
            let mut last_event_id = None;
            bridge_gateway_event_stream(
                &bridge_route,
                &sender,
                Duration::from_millis(100),
                &mut last_event_id,
            )
        });
        assert_eq!(
            receiver
                .recv_timeout(Duration::from_secs(2))
                .expect("ready heartbeat should prove the stream is active")
                .event,
            "gateway.heartbeat"
        );

        let replacement = cmclient_agent_core::web::GatewayRoute::new(address, "2".repeat(64))
            .expect("replacement route should be valid");
        let rotation = thread::spawn(move || session.set(replacement));
        let deadline = Instant::now() + Duration::from_secs(2);
        while route.is_active() {
            assert!(
                Instant::now() < deadline,
                "route rotation did not revoke the event generation"
            );
            thread::yield_now();
        }
        write_stale_event
            .send(())
            .expect("fixture should attempt the stale event");

        assert!(bridge.join().expect("event bridge should join"));
        rotation.join().expect("route rotation should drain");
        fixture.join().expect("Gateway fixture should join");
        assert!(
            receiver
                .try_iter()
                .all(|event| event.event != "mesh.position"),
            "an event from the revoked generation reached Control subscribers",
        );
    }

    #[test]
    fn incomplete_gateway_sse_fields_do_not_leak_across_event_boundaries() {
        use std::io::{Read as _, Write as _};

        let listener = TcpListener::bind("127.0.0.1:0").expect("Gateway fixture should bind");
        let address = listener.local_addr().expect("fixture address should load");
        let fixture = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("event request should connect");
            let mut request = Vec::new();
            let mut chunk = [0_u8; 2_048];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let count = stream.read(&mut chunk).expect("request should read");
                assert!(count > 0, "event request ended before headers");
                request.extend_from_slice(&chunk[..count]);
            }
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\nid: incomplete-1\nevent: mesh.position\n\ndata: {\"mustNotForward\":true}\n\n",
                )
                .expect("incomplete events should write");
        });

        let route = cmclient_agent_core::web::GatewayRoute::new(address, "3".repeat(64))
            .expect("Gateway route should be valid");
        let (sender, receiver) = std::sync::mpsc::sync_channel(8);
        let mut last_event_id = None;
        assert!(bridge_gateway_event_stream(
            &route,
            &sender,
            Duration::from_millis(100),
            &mut last_event_id,
        ));

        fixture.join().expect("Gateway fixture should join");
        assert_eq!(last_event_id, None);
        assert!(
            receiver
                .try_iter()
                .all(|event| { event.event != "mesh.position" && event.id != "incomplete-1" }),
            "fields from two incomplete SSE records were combined into an event",
        );
    }

    #[test]
    fn handwritten_gateway_outbound_http_does_not_return() {
        let source = include_str!("main.rs");
        let outbound = source
            .split_once("fn open_gateway_event_stream(")
            .and_then(|(_, source)| source.split_once("fn probe_gateway_event_receiver("))
            .map(|(source, _)| source)
            .expect("Gateway event client source should be bounded by stable functions");
        for forbidden in [
            "TcpStream::",
            "write_all(",
            "HTTP/1.",
            "MAX_RESPONSE_HEADER_BYTES",
            "trim_http_line(",
        ] {
            assert!(
                !outbound.contains(forbidden),
                "Gateway outbound HTTP must stay owned by reqwest: {forbidden}",
            );
        }
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn preserves_gateway_timeout_for_local_control_clients() {
        let gateway = TcpListener::bind("127.0.0.1:0").expect("gateway should bind");
        let gateway_address = gateway.local_addr().expect("gateway address should load");
        let gateway_thread = thread::spawn(move || {
            let (mut stream, _) = gateway.accept().expect("gateway should accept");
            let mut request = [0_u8; 4_096];
            let count = stream.read(&mut request).expect("request should read");
            let request = std::str::from_utf8(&request[..count]).expect("request should be UTF-8");
            assert!(request.contains(&format!(
                "{}: {}\r\n",
                super::GATEWAY_CAPABILITY_HEADER,
                "a".repeat(64)
            )));
            stream
                .write_all(
                    b"HTTP/1.1 504 Gateway Timeout\r\ncontent-type: application/json\r\ncontent-length: 26\r\nconnection: close\r\n\r\n{\"code\":\"GATEWAY_TIMEOUT\"}",
                )
                .expect("timeout response should write");
        });
        let route = cmclient_agent_core::web::GatewayRoute::new(gateway_address, "a".repeat(64))
            .expect("Gateway route should be valid");

        assert_eq!(
            gateway_json_projection(&route, cmclient_control_api::GatewayProjection::Nodes),
            Err(cmclient_control_api::ControlError::Timeout)
        );
        gateway_thread.join().expect("gateway should join");
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn bounds_gateway_sse_lines_before_allocating_an_event() {
        let mut oversized = Cursor::new(vec![b'a'; 64 * 1024 + 1]);
        assert!(read_bounded_gateway_sse_line(&mut oversized, &mut Vec::new()).is_err());
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn closes_a_gateway_event_bridge_instead_of_blocking_on_a_full_queue() {
        let (sender, _receiver) = mpsc::sync_channel(1);
        assert!(try_forward_gateway_event(&sender, gateway_heartbeat()));
        assert!(!try_forward_gateway_event(&sender, gateway_heartbeat()));
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn reconnects_the_gateway_event_bridge_with_the_last_forwarded_event_id() {
        let gateway = TcpListener::bind("127.0.0.1:0").expect("gateway should bind");
        let gateway_address = gateway.local_addr().expect("gateway address should load");
        let gateway_thread = thread::spawn(move || {
            let mut requests = Vec::new();
            for index in 1..=2 {
                let (mut stream, _) = gateway.accept().expect("gateway should accept");
                let mut request = Vec::new();
                let mut chunk = [0_u8; 2_048];
                while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                    let count = stream.read(&mut chunk).expect("request should read");
                    assert!(count > 0, "request ended before headers");
                    request.extend_from_slice(&chunk[..count]);
                }
                requests.push(String::from_utf8(request).expect("request should be UTF-8"));
                stream
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\nid: gateway-{index}\nevent: mesh.position\ndata: {{\"index\":{index}}}\n\n"
                        )
                        .as_bytes(),
                    )
                    .expect("event should write");
            }
            requests
        });
        let (sender, _receiver) = mpsc::sync_channel(8);
        let mut last_event_id = None;
        let route = cmclient_agent_core::web::GatewayRoute::new(gateway_address, "b".repeat(64))
            .expect("Gateway route should be valid");

        assert!(bridge_gateway_event_stream(
            &route,
            &sender,
            Duration::from_millis(100),
            &mut last_event_id,
        ));
        assert_eq!(last_event_id.as_deref(), Some("gateway-1"));
        assert!(bridge_gateway_event_stream(
            &route,
            &sender,
            Duration::from_millis(100),
            &mut last_event_id,
        ));
        assert_eq!(last_event_id.as_deref(), Some("gateway-2"));

        let requests = gateway_thread.join().expect("gateway should join");
        for request in &requests {
            assert!(request.contains(&format!(
                "{}: {}\r\n",
                super::GATEWAY_CAPABILITY_HEADER,
                "b".repeat(64)
            )));
        }
        assert!(!requests[0].to_ascii_lowercase().contains("last-event-id:"));
        assert!(
            requests[1]
                .to_ascii_lowercase()
                .contains("last-event-id: gateway-1\r\n")
        );
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn releases_repeated_gateway_event_bridges_when_a_hung_upstream_outlives_receivers() {
        const SUBSCRIPTIONS: usize = 12;
        assert_eq!(active_gateway_event_bridge_count(), 0);

        let gateway = TcpListener::bind("127.0.0.1:0").expect("gateway should bind");
        let gateway_address = gateway.local_addr().expect("gateway address should load");
        let connected = Arc::new(AtomicUsize::new(0));
        let closed = Arc::new(AtomicUsize::new(0));
        let gateway_thread = {
            let connected = Arc::clone(&connected);
            let closed = Arc::clone(&closed);
            thread::spawn(move || {
                let mut workers = Vec::new();
                for _ in 0..SUBSCRIPTIONS {
                    let (mut stream, _) = gateway.accept().expect("gateway should accept");
                    let connected = Arc::clone(&connected);
                    let closed = Arc::clone(&closed);
                    workers.push(thread::spawn(move || {
                        stream
                            .set_read_timeout(Some(Duration::from_secs(5)))
                            .expect("gateway read timeout should configure");
                        let mut request = Vec::new();
                        let mut chunk = [0_u8; 2_048];
                        while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                            let count = stream.read(&mut chunk).expect("request should read");
                            assert!(count > 0, "bridge request ended before its headers");
                            request.extend_from_slice(&chunk[..count]);
                        }
                        stream
                            .write_all(
                                b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream; charset=utf-8\r\ncache-control: no-cache\r\nconnection: keep-alive\r\n\r\n",
                            )
                            .expect("hung Gateway headers should write");
                        connected.fetch_add(1, Ordering::AcqRel);

                        let mut byte = [0_u8; 1];
                        assert_eq!(
                            stream.read(&mut byte).expect("bridge socket should close"),
                            0,
                            "bridge unexpectedly wrote after the SSE request"
                        );
                        closed.fetch_add(1, Ordering::AcqRel);
                    }));
                }
                for worker in workers {
                    worker.join().expect("Gateway connection should join");
                }
            })
        };

        let receivers = (0..SUBSCRIPTIONS)
            .map(|_| {
                bridge_gateway_events_with_read_poll(
                    cmclient_agent_core::web::GatewaySessionHandle::with_route(
                        cmclient_agent_core::web::GatewayRoute::new(
                            gateway_address,
                            "c".repeat(64),
                        )
                        .expect("Gateway route should be valid"),
                    ),
                    Duration::from_millis(100),
                )
            })
            .collect::<Vec<_>>();
        wait_for_resource_count(
            || connected.load(Ordering::Acquire),
            SUBSCRIPTIONS,
            "Gateway SSE bridges did not connect",
        );
        wait_for_resource_count(
            active_gateway_event_bridge_count,
            SUBSCRIPTIONS,
            "Gateway SSE bridge threads did not start",
        );

        drop(receivers);

        wait_for_resource_count(
            active_gateway_event_bridge_count,
            0,
            "Gateway SSE bridge threads survived receiver drop",
        );
        wait_for_resource_count(
            || closed.load(Ordering::Acquire),
            SUBSCRIPTIONS,
            "Gateway SSE bridge sockets survived receiver drop",
        );
        gateway_thread.join().expect("hung Gateway should join");
    }

    #[cfg(not(target_os = "windows"))]
    fn wait_for_resource_count(current: impl Fn() -> usize, expected: usize, message: &str) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while current() != expected {
            assert!(Instant::now() < deadline, "{message}");
            thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn shutdown_signal_registration_failure_is_nonfatal() {
        assert_eq!(
            classify_shutdown_signal_registration(Err(ControlError::CommandFailed)),
            ShutdownSignalRegistration::Unavailable,
        );
    }
}
