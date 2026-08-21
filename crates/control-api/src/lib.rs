//! Typed, bounded local Agent control over Unix sockets or Windows named pipes.

pub use cmclient_product_identity::{
    ComponentIdentityReport, IdentityError, InternalComponent, PackageProfile, ProductIdentity,
    ProductTarget, ReleaseChannel, ReleaseIdentity, RuntimeProfile, TargetArchitecture,
    TargetOperatingSystem, compiled_component_identity,
};

use bytes::{Bytes, BytesMut};
use interprocess::local_socket::{
    Listener, ListenerNonblockingMode, ListenerOptions, Name, Stream, prelude::*,
};
#[cfg(unix)]
use interprocess::{
    ConnectWaitMode,
    local_socket::{ConnectOptions, GenericFilePath},
    os::unix::local_socket::ListenerOptionsExt as _,
};
#[cfg(windows)]
use interprocess::{
    ConnectWaitMode,
    os::windows::{
        local_socket::NamedPipe,
        named_pipe::{
            DuplexPipeStream, local_socket::Stream as WindowsLocalStream,
            pipe_mode::Bytes as PipeBytes,
        },
    },
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
#[cfg(windows)]
use sha2::{Digest, Sha256};
use std::{
    fmt::{Display, Formatter},
    fs,
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};
use zeroize::{Zeroize, Zeroizing};

/// Stable workspace identity for the control API boundary.
pub const COMPONENT: &str = "control-api";
pub const CONTROL_PROTOCOL_VERSION: u16 = 1;
pub const MAX_CONTROL_FRAME_BYTES: usize = 2 * 1024 * 1024;
const MAX_CONTROL_EVENT_DATA_BYTES: usize = 60 * 1024;
const MAX_CONTROL_CONNECTIONS: usize = 64;
const CONTROL_SERVER_IO_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(windows)]
const CONTROL_BIND_RETRY_TIMEOUT: Duration = Duration::from_millis(250);
#[cfg(unix)]
const CONTROL_ENDPOINT_PROBE_TIMEOUT: Duration = Duration::from_millis(250);
const CONTROL_EVENT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
const CONTROL_IO_POLL_INTERVAL: Duration = Duration::from_millis(2);
const CONTROL_SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(25);
const CONTROL_WRITE_CHUNK_BYTES: usize = 512;
#[cfg(windows)]
const WINDOWS_PIPE_PREFIX: &str = r"\\.\pipe\";
#[cfg(windows)]
const WINDOWS_PIPE_NAME_DOMAIN: &[u8] = b"cmclient-control-v1\0";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlEndpoint {
    UnixSocket(PathBuf),
    NamedPipe(String),
}

impl ControlEndpoint {
    pub fn unix(path: impl Into<PathBuf>) -> Self {
        Self::UnixSocket(path.into())
    }

    pub fn named_pipe(name: impl Into<String>) -> Self {
        Self::NamedPipe(name.into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ControlStatus {
    pub schema_version: u8,
    pub agent: String,
    pub identity: ComponentIdentityReport,
    pub gateway: GatewayControlStatus,
    pub management_web: ManagementWebControlStatus,
    pub management_web_url: Option<String>,
    pub uptime_seconds: u64,
    pub latest_error_code: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GatewayControlStatus {
    Stopped,
    Starting,
    Running,
    Backoff,
    Degraded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManagementWebControlStatus {
    Disabled,
    Running,
}

/// Read-only projection of the Agent-owned persistent update job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UpdateControlStatus {
    pub schema_version: u8,
    pub job: Option<UpdateControlJob>,
}

/// Sanitized Agent diagnostic bundle exposed only through local Control.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DiagnosticsControlBundle {
    pub schema_version: u8,
    pub identity: ComponentIdentityReport,
    pub gateway: GatewayControlStatus,
    pub management_web: ManagementWebControlStatus,
    pub latest_error_code: Option<String>,
    pub update_error_code: Option<String>,
    pub update_log_codes: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ControlSecretKind {
    CallMeshApiKey,
    AprsPasscode,
    ManagementAdminToken,
}

impl ControlSecretKind {
    pub const fn path_segment(self) -> &'static str {
        match self {
            Self::CallMeshApiKey => "callmesh-api-key",
            Self::AprsPasscode => "aprs-passcode",
            Self::ManagementAdminToken => "management-admin-token",
        }
    }
}

/// Confirmation for a local secret mutation. It never contains the value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ControlSecretReceipt {
    pub stored: bool,
}

/// Safe update fields exposed through local Control events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UpdateControlJob {
    pub id: String,
    pub phase: String,
    pub updated_at: String,
    pub error_code: Option<String>,
    pub bytes_downloaded: Option<u64>,
    pub bytes_total: Option<u64>,
    pub bytes_per_second: Option<u64>,
    pub recent_log_codes: Vec<String>,
}

/// One Agent update or Gateway event delivered through framed local Control.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlUpdateEvent {
    pub id: String,
    pub event: String,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GatewayProjection {
    Meshtastic,
    Nodes,
    Positions,
    Aprs,
    CallMesh,
    Proxy,
    RecentEvents,
    DatabaseIntegrity,
    Backup,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlCommand {
    Status,
    Start,
    OpenDesktop,
    Stop,
    Restart,
    OperationalReset,
    ShutdownAgent,
    EnableManagementWeb,
    DisableManagementWeb,
}

pub trait ControlHandler: Send + Sync {
    fn handle(&self, command: ControlCommand) -> Result<ControlStatus, ControlError>;

    fn prepare_command(&self, command: ControlCommand) -> Result<ControlStatus, ControlError> {
        self.handle(command)
    }

    fn command_response_sent(&self, _command: ControlCommand) {}

    fn update_status(&self) -> Result<UpdateControlStatus, ControlError> {
        Err(ControlError::CommandFailed)
    }

    fn subscribe_update_events(&self) -> Result<mpsc::Receiver<ControlUpdateEvent>, ControlError> {
        Err(ControlError::CommandFailed)
    }

    fn diagnostics_bundle(&self) -> Result<DiagnosticsControlBundle, ControlError> {
        Err(ControlError::CommandFailed)
    }

    fn gateway_projection(
        &self,
        _projection: GatewayProjection,
    ) -> Result<serde_json::Value, ControlError> {
        Err(ControlError::CommandFailed)
    }

    fn subscribe_gateway_events(&self) -> Result<mpsc::Receiver<ControlUpdateEvent>, ControlError> {
        Err(ControlError::CommandFailed)
    }

    fn store_secret(&self, _kind: ControlSecretKind, _value: &str) -> Result<(), ControlError> {
        Err(ControlError::CommandFailed)
    }

    fn remove_secret(&self, _kind: ControlSecretKind) -> Result<bool, ControlError> {
        Err(ControlError::CommandFailed)
    }

    fn cmcloud_enrollment_status(&self) -> Result<serde_json::Value, ControlError> {
        Err(ControlError::CommandFailed)
    }

    fn enroll_cmcloud(&self, _pairing_code: &str) -> Result<serde_json::Value, ControlError> {
        Err(ControlError::CommandFailed)
    }
}

#[derive(Debug, Clone)]
pub struct StaticControlHandler {
    status: ControlStatus,
}

impl StaticControlHandler {
    pub fn new(status: ControlStatus) -> Self {
        Self { status }
    }
}

impl ControlHandler for StaticControlHandler {
    fn handle(&self, _command: ControlCommand) -> Result<ControlStatus, ControlError> {
        Ok(self.status.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlError {
    EndpointAlreadyInUse,
    UnsupportedEndpoint,
    Io,
    InvalidEnvelope,
    ProtocolVersionUnsupported,
    ResponseTooLarge,
    Timeout,
    ResourceExhausted,
    Authentication,
    SecretKindDeprecated,
    SecretStoreUnavailable,
    SecretValueInvalid,
    CommandFailed,
    Application(String),
}

impl ControlError {
    pub fn from_code(code: &str) -> Option<Self> {
        match code {
            "CONTROL_ENDPOINT_ALREADY_IN_USE" => Some(Self::EndpointAlreadyInUse),
            "CONTROL_ENDPOINT_UNSUPPORTED" => Some(Self::UnsupportedEndpoint),
            "CONTROL_IO_FAILED" => Some(Self::Io),
            "CONTROL_ENVELOPE_INVALID" => Some(Self::InvalidEnvelope),
            "CONTROL_PROTOCOL_VERSION_UNSUPPORTED" => Some(Self::ProtocolVersionUnsupported),
            "CONTROL_RESPONSE_TOO_LARGE" => Some(Self::ResponseTooLarge),
            "CONTROL_TIMEOUT" => Some(Self::Timeout),
            "CONTROL_RESOURCE_EXHAUSTED" => Some(Self::ResourceExhausted),
            "CONTROL_AUTHENTICATION_FAILED" => Some(Self::Authentication),
            "CONTROL_SECRET_KIND_DEPRECATED" => Some(Self::SecretKindDeprecated),
            "AGENT_SECRET_STORE_UNAVAILABLE" => Some(Self::SecretStoreUnavailable),
            "AGENT_SECRET_VALUE_INVALID" => Some(Self::SecretValueInvalid),
            "CONTROL_COMMAND_FAILED" => Some(Self::CommandFailed),
            _ if valid_error_code(code) => Some(Self::Application(code.to_owned())),
            _ => None,
        }
    }

    pub fn code(&self) -> &str {
        match self {
            Self::EndpointAlreadyInUse => "CONTROL_ENDPOINT_ALREADY_IN_USE",
            Self::UnsupportedEndpoint => "CONTROL_ENDPOINT_UNSUPPORTED",
            Self::Io => "CONTROL_IO_FAILED",
            Self::InvalidEnvelope => "CONTROL_ENVELOPE_INVALID",
            Self::ProtocolVersionUnsupported => "CONTROL_PROTOCOL_VERSION_UNSUPPORTED",
            Self::ResponseTooLarge => "CONTROL_RESPONSE_TOO_LARGE",
            Self::Timeout => "CONTROL_TIMEOUT",
            Self::ResourceExhausted => "CONTROL_RESOURCE_EXHAUSTED",
            Self::Authentication => "CONTROL_AUTHENTICATION_FAILED",
            Self::SecretKindDeprecated => "CONTROL_SECRET_KIND_DEPRECATED",
            Self::SecretStoreUnavailable => "AGENT_SECRET_STORE_UNAVAILABLE",
            Self::SecretValueInvalid => "AGENT_SECRET_VALUE_INVALID",
            Self::CommandFailed => "CONTROL_COMMAND_FAILED",
            Self::Application(code) => code.as_str(),
        }
    }
}

impl Display for ControlError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.code())
    }
}

impl std::error::Error for ControlError {}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RequestEnvelope {
    version: u16,
    request_id: String,
    request: WireRequest,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
enum WireRequest {
    Command(ControlCommand),
    UpdateStatus,
    DiagnosticsBundle,
    GatewayProjection(GatewayProjection),
    StoreSecret {
        kind: ControlSecretKind,
        value: String,
    },
    RemoveSecret(ControlSecretKind),
    CmCloudEnrollmentStatus,
    EnrollCmCloud {
        pairing_code: String,
    },
    SubscribeUpdateEvents,
    SubscribeGatewayEvents,
}

impl WireRequest {
    fn zeroize_sensitive(&mut self) {
        match self {
            Self::StoreSecret { value, .. }
            | Self::EnrollCmCloud {
                pairing_code: value,
            } => {
                value.zeroize();
            }
            _ => {}
        }
    }
}

impl Drop for WireRequest {
    fn drop(&mut self) {
        self.zeroize_sensitive();
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ServerEnvelope {
    Response {
        version: u16,
        request_id: String,
        payload: WireResponse,
    },
    Error {
        version: u16,
        request_id: String,
        code: String,
    },
    Event {
        version: u16,
        request_id: String,
        event: WireEvent,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
enum WireResponse {
    Status(ControlStatus),
    UpdateStatus(UpdateControlStatus),
    DiagnosticsBundle(DiagnosticsControlBundle),
    GatewayProjection(serde_json::Value),
    SecretReceipt(ControlSecretReceipt),
    CmCloudEnrollmentStatus(serde_json::Value),
    SubscriptionAccepted,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WireEvent {
    Data {
        id: String,
        event: String,
        data: serde_json::Value,
    },
    Heartbeat,
}

enum DispatchOutcome {
    Response {
        payload: WireResponse,
        command: Option<ControlCommand>,
    },
    Events(mpsc::Receiver<ControlUpdateEvent>),
}

impl DispatchOutcome {
    fn response(payload: WireResponse) -> Self {
        Self::Response {
            payload,
            command: None,
        }
    }
}

fn dispatch_request(
    mut request: WireRequest,
    handler: &dyn ControlHandler,
) -> Result<DispatchOutcome, ControlError> {
    let outcome = match &mut request {
        WireRequest::Command(command) => {
            let command = *command;
            handler
                .prepare_command(command)
                .map(WireResponse::Status)
                .map(|payload| DispatchOutcome::Response {
                    payload,
                    command: Some(command),
                })
        }
        WireRequest::UpdateStatus => handler
            .update_status()
            .map(WireResponse::UpdateStatus)
            .map(DispatchOutcome::response),
        WireRequest::DiagnosticsBundle => handler
            .diagnostics_bundle()
            .map(WireResponse::DiagnosticsBundle)
            .map(DispatchOutcome::response),
        WireRequest::GatewayProjection(projection) => handler
            .gateway_projection(*projection)
            .map(WireResponse::GatewayProjection)
            .map(DispatchOutcome::response),
        WireRequest::StoreSecret { kind, value } => {
            if *kind != ControlSecretKind::CallMeshApiKey {
                Err(ControlError::SecretKindDeprecated)
            } else {
                handler
                    .store_secret(*kind, value)
                    .map(|()| WireResponse::SecretReceipt(ControlSecretReceipt { stored: true }))
                    .map(DispatchOutcome::response)
            }
        }
        WireRequest::RemoveSecret(kind) => handler
            .remove_secret(*kind)
            .map(|stored| WireResponse::SecretReceipt(ControlSecretReceipt { stored }))
            .map(DispatchOutcome::response),
        WireRequest::CmCloudEnrollmentStatus => handler
            .cmcloud_enrollment_status()
            .map(WireResponse::CmCloudEnrollmentStatus)
            .map(DispatchOutcome::response),
        WireRequest::EnrollCmCloud { pairing_code } => handler
            .enroll_cmcloud(pairing_code)
            .map(WireResponse::CmCloudEnrollmentStatus)
            .map(DispatchOutcome::response),
        WireRequest::SubscribeUpdateEvents => handler
            .subscribe_update_events()
            .map(DispatchOutcome::Events),
        WireRequest::SubscribeGatewayEvents => handler
            .subscribe_gateway_events()
            .map(DispatchOutcome::Events),
    };
    request.zeroize_sensitive();
    outcome
}

#[derive(Clone)]
struct ConnectionLimiter {
    active: Arc<AtomicUsize>,
    maximum: usize,
}

impl ConnectionLimiter {
    fn new(maximum: usize) -> Self {
        debug_assert!(maximum > 0);
        Self {
            active: Arc::new(AtomicUsize::new(0)),
            maximum,
        }
    }

    fn try_acquire(&self) -> Option<ConnectionPermit> {
        self.active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                (active < self.maximum).then_some(active + 1)
            })
            .ok()
            .map(|_| ConnectionPermit {
                active: Arc::clone(&self.active),
            })
    }

    #[cfg(test)]
    fn active(&self) -> usize {
        self.active.load(Ordering::Acquire)
    }
}

struct ConnectionPermit {
    active: Arc<AtomicUsize>,
}

impl Drop for ConnectionPermit {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::AcqRel);
    }
}

pub struct ControlServer {
    listener: Listener,
    handler: Arc<dyn ControlHandler>,
    connections: ConnectionLimiter,
    io_timeout: Duration,
    shutdown: Arc<AtomicBool>,
    workers: Mutex<Vec<thread::JoinHandle<()>>>,
}

impl ControlServer {
    pub fn bind(
        endpoint: ControlEndpoint,
        handler: Arc<dyn ControlHandler>,
    ) -> Result<Self, ControlError> {
        Self::bind_with_timeout(endpoint, handler, CONTROL_SERVER_IO_TIMEOUT)
    }

    fn bind_with_timeout(
        endpoint: ControlEndpoint,
        handler: Arc<dyn ControlHandler>,
        io_timeout: Duration,
    ) -> Result<Self, ControlError> {
        if io_timeout.is_zero() || !is_local_endpoint(&endpoint) {
            return Err(ControlError::UnsupportedEndpoint);
        }
        prepare_endpoint(&endpoint)?;
        let listener = bind_listener(&endpoint)?;
        secure_bound_endpoint(&endpoint)?;
        Ok(Self {
            listener,
            handler,
            connections: ConnectionLimiter::new(MAX_CONTROL_CONNECTIONS),
            io_timeout,
            shutdown: Arc::new(AtomicBool::new(false)),
            workers: Mutex::new(Vec::new()),
        })
    }

    /// Polls once for a client and starts one managed worker when a connection
    /// is available. `false` means the nonblocking listener was idle.
    pub fn poll_once(&self) -> Result<bool, ControlError> {
        self.reap_workers()?;
        let stream = match self.listener.accept() {
            Ok(stream) => stream,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(CONTROL_IO_POLL_INTERVAL);
                return Ok(false);
            }
            Err(error) => return Err(map_io_error(error)),
        };
        let Some(permit) = self.connections.try_acquire() else {
            return Ok(true);
        };
        configure_stream(&stream)?;
        let handler = Arc::clone(&self.handler);
        let shutdown = Arc::clone(&self.shutdown);
        let timeout = self.io_timeout;
        let worker = thread::Builder::new()
            .name(String::from("cmclient-control-client"))
            .spawn(move || {
                let _permit = permit;
                let _ = serve_connection(stream, handler.as_ref(), timeout, shutdown.as_ref());
            })
            .map_err(|_| ControlError::Io)?;
        self.workers
            .lock()
            .map_err(|_| ControlError::CommandFailed)?
            .push(worker);
        Ok(true)
    }

    pub fn serve_once(&self) -> Result<(), ControlError> {
        let deadline = deadline_after(self.io_timeout)?;
        while !self.poll_once()? {
            ensure_before_deadline(deadline)?;
        }
        Ok(())
    }

    #[cfg(test)]
    fn serve_once_inline(&self) -> Result<(), ControlError> {
        let deadline = deadline_after(self.io_timeout)?;
        let stream = loop {
            match self.listener.accept() {
                Ok(stream) => break stream,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    wait_for_io(deadline)?;
                }
                Err(error) => return Err(map_io_error(error)),
            }
        };
        let permit = self
            .connections
            .try_acquire()
            .ok_or(ControlError::ResourceExhausted)?;
        configure_stream(&stream)?;
        let result = serve_connection(
            stream,
            self.handler.as_ref(),
            self.io_timeout,
            self.shutdown.as_ref(),
        );
        drop(permit);
        result
    }

    fn reap_workers(&self) -> Result<(), ControlError> {
        let mut workers = self
            .workers
            .lock()
            .map_err(|_| ControlError::CommandFailed)?;
        let mut index = 0;
        while index < workers.len() {
            if workers[index].is_finished() {
                let worker = workers.swap_remove(index);
                if worker.join().is_err() {
                    return Err(ControlError::CommandFailed);
                }
            } else {
                index += 1;
            }
        }
        Ok(())
    }
}

impl Drop for ControlServer {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        let workers = self
            .workers
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for worker in workers.drain(..) {
            let _ = worker.join();
        }
    }
}

#[cfg(unix)]
fn bind_listener(endpoint: &ControlEndpoint) -> Result<Listener, ControlError> {
    let name = endpoint_name(endpoint)?;
    ListenerOptions::new()
        .name(name)
        .mode(0o600)
        .nonblocking(ListenerNonblockingMode::Accept)
        .create_sync()
        .map_err(map_bind_error)
}

#[cfg(windows)]
fn bind_listener(endpoint: &ControlEndpoint) -> Result<Listener, ControlError> {
    let deadline = deadline_after(CONTROL_BIND_RETRY_TIMEOUT)?;
    loop {
        let name = endpoint_name(endpoint)?;
        match ListenerOptions::new()
            .name(name)
            .nonblocking(ListenerNonblockingMode::Accept)
            .create_sync()
        {
            Ok(listener) => return Ok(listener),
            Err(error) => {
                let error = map_bind_error(error);
                if error != ControlError::EndpointAlreadyInUse || Instant::now() >= deadline {
                    return Err(error);
                }
                thread::sleep(CONTROL_IO_POLL_INTERVAL);
            }
        }
    }
}

fn serve_connection(
    mut stream: Stream,
    handler: &dyn ControlHandler,
    timeout: Duration,
    cancellation: &AtomicBool,
) -> Result<(), ControlError> {
    let deadline = deadline_after(timeout)?;
    let mut reader = FrameReader::new();
    let mut envelope: RequestEnvelope = reader
        .read_json_cancellable(&mut stream, deadline, Some(cancellation))?
        .ok_or(ControlError::InvalidEnvelope)?;
    reader.reject_available_trailing_data(&mut stream, deadline, Some(cancellation))?;
    if !valid_request_id(&envelope.request_id) {
        envelope.request.zeroize_sensitive();
        return Err(ControlError::InvalidEnvelope);
    }
    let request_id = envelope.request_id.clone();
    if envelope.version != CONTROL_PROTOCOL_VERSION {
        envelope.request.zeroize_sensitive();
        return write_server_error(
            &mut stream,
            &request_id,
            ControlError::ProtocolVersionUnsupported,
            deadline_after(timeout)?,
            cancellation,
        );
    }
    ensure_not_cancelled(cancellation)?;
    let outcome = dispatch_request(envelope.request, handler);
    match outcome {
        Ok(DispatchOutcome::Response { payload, command }) => {
            let result = write_json_frame_cancellable(
                &mut stream,
                &ServerEnvelope::Response {
                    version: CONTROL_PROTOCOL_VERSION,
                    request_id,
                    payload,
                },
                deadline_after(timeout)?,
                Some(cancellation),
            );
            if result.is_ok() {
                if let Some(command) = command {
                    handler.command_response_sent(command);
                }
            }
            result
        }
        Ok(DispatchOutcome::Events(events)) => {
            write_json_frame_cancellable(
                &mut stream,
                &ServerEnvelope::Response {
                    version: CONTROL_PROTOCOL_VERSION,
                    request_id: request_id.clone(),
                    payload: WireResponse::SubscriptionAccepted,
                },
                deadline_after(timeout)?,
                Some(cancellation),
            )?;
            serve_events(&mut stream, &request_id, events, timeout, cancellation)
        }
        Err(error) => write_server_error(
            &mut stream,
            &request_id,
            error,
            deadline_after(timeout)?,
            cancellation,
        ),
    }
}

fn write_server_error(
    stream: &mut Stream,
    request_id: &str,
    error: ControlError,
    deadline: Instant,
    cancellation: &AtomicBool,
) -> Result<(), ControlError> {
    write_json_frame_cancellable(
        stream,
        &ServerEnvelope::Error {
            version: CONTROL_PROTOCOL_VERSION,
            request_id: request_id.to_owned(),
            code: error.code().to_owned(),
        },
        deadline,
        Some(cancellation),
    )
}

fn serve_events(
    stream: &mut Stream,
    request_id: &str,
    events: mpsc::Receiver<ControlUpdateEvent>,
    timeout: Duration,
    cancellation: &AtomicBool,
) -> Result<(), ControlError> {
    let mut heartbeat_deadline = deadline_after(CONTROL_EVENT_HEARTBEAT_INTERVAL)?;
    loop {
        ensure_not_cancelled(cancellation)?;
        let now = Instant::now();
        let wait = heartbeat_deadline
            .saturating_duration_since(now)
            .min(CONTROL_SHUTDOWN_POLL_INTERVAL);
        let event = match events.recv_timeout(wait) {
            Ok(event) => wire_event(event)?,
            Err(mpsc::RecvTimeoutError::Timeout) if Instant::now() < heartbeat_deadline => {
                continue;
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                heartbeat_deadline = deadline_after(CONTROL_EVENT_HEARTBEAT_INTERVAL)?;
                WireEvent::Heartbeat
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
        };
        write_json_frame_cancellable(
            stream,
            &ServerEnvelope::Event {
                version: CONTROL_PROTOCOL_VERSION,
                request_id: request_id.to_owned(),
                event,
            },
            deadline_after(timeout)?,
            Some(cancellation),
        )?;
    }
}

fn wire_event(mut event: ControlUpdateEvent) -> Result<WireEvent, ControlError> {
    if !valid_event_token(&event.id)
        || !valid_event_token(&event.event)
        || event.data.len() > MAX_CONTROL_EVENT_DATA_BYTES
    {
        event.data.zeroize();
        return Err(ControlError::InvalidEnvelope);
    }
    let data = serde_json::from_slice(&event.data).map_err(|_| ControlError::InvalidEnvelope);
    event.data.zeroize();
    data.map(|data| WireEvent::Data {
        id: event.id,
        event: event.event,
        data,
    })
}

pub struct ControlClient {
    endpoint: ControlEndpoint,
    timeout: Duration,
}

impl ControlClient {
    pub fn new(endpoint: ControlEndpoint) -> Result<Self, ControlError> {
        Self::new_with_timeout(endpoint, CONTROL_SERVER_IO_TIMEOUT)
    }

    pub fn new_with_timeout(
        endpoint: ControlEndpoint,
        timeout: Duration,
    ) -> Result<Self, ControlError> {
        if timeout.is_zero() || !is_local_endpoint(&endpoint) {
            return Err(ControlError::UnsupportedEndpoint);
        }
        endpoint_name(&endpoint)?;
        Ok(Self { endpoint, timeout })
    }

    pub fn status(&self) -> Result<ControlStatus, ControlError> {
        self.command(ControlCommand::Status)
    }

    pub fn start(&self) -> Result<ControlStatus, ControlError> {
        self.command(ControlCommand::Start)
    }

    pub fn open_desktop(&self) -> Result<ControlStatus, ControlError> {
        self.command(ControlCommand::OpenDesktop)
    }

    pub fn stop(&self) -> Result<ControlStatus, ControlError> {
        self.command(ControlCommand::Stop)
    }

    pub fn restart(&self) -> Result<ControlStatus, ControlError> {
        self.command(ControlCommand::Restart)
    }

    pub fn operational_reset(&self) -> Result<ControlStatus, ControlError> {
        self.command(ControlCommand::OperationalReset)
    }

    pub fn shutdown_agent(&self) -> Result<ControlStatus, ControlError> {
        self.command(ControlCommand::ShutdownAgent)
    }

    pub fn enable_management_web(&self) -> Result<ControlStatus, ControlError> {
        self.command(ControlCommand::EnableManagementWeb)
    }

    pub fn disable_management_web(&self) -> Result<ControlStatus, ControlError> {
        self.command(ControlCommand::DisableManagementWeb)
    }

    pub fn update_status(&self) -> Result<UpdateControlStatus, ControlError> {
        match self.exchange(WireRequest::UpdateStatus)? {
            WireResponse::UpdateStatus(status) => Ok(status),
            _ => Err(ControlError::InvalidEnvelope),
        }
    }

    pub fn diagnostics_bundle(&self) -> Result<DiagnosticsControlBundle, ControlError> {
        match self.exchange(WireRequest::DiagnosticsBundle)? {
            WireResponse::DiagnosticsBundle(bundle) => Ok(bundle),
            _ => Err(ControlError::InvalidEnvelope),
        }
    }

    pub fn gateway_projection(
        &self,
        projection: GatewayProjection,
    ) -> Result<serde_json::Value, ControlError> {
        match self.exchange(WireRequest::GatewayProjection(projection))? {
            WireResponse::GatewayProjection(value) => Ok(value),
            _ => Err(ControlError::InvalidEnvelope),
        }
    }

    pub fn store_secret(
        &self,
        kind: ControlSecretKind,
        value: &str,
    ) -> Result<ControlSecretReceipt, ControlError> {
        match self.exchange(WireRequest::StoreSecret {
            kind,
            value: value.to_owned(),
        })? {
            WireResponse::SecretReceipt(receipt) => Ok(receipt),
            _ => Err(ControlError::InvalidEnvelope),
        }
    }

    pub fn remove_secret(
        &self,
        kind: ControlSecretKind,
    ) -> Result<ControlSecretReceipt, ControlError> {
        match self.exchange(WireRequest::RemoveSecret(kind))? {
            WireResponse::SecretReceipt(receipt) => Ok(receipt),
            _ => Err(ControlError::InvalidEnvelope),
        }
    }

    pub fn subscribe_update_events(&self) -> Result<ControlUpdateEventStream, ControlError> {
        self.subscribe(WireRequest::SubscribeUpdateEvents)
    }

    pub fn subscribe_gateway_events(&self) -> Result<ControlUpdateEventStream, ControlError> {
        self.subscribe(WireRequest::SubscribeGatewayEvents)
    }

    pub fn cmcloud_enrollment_status(&self) -> Result<serde_json::Value, ControlError> {
        match self.exchange(WireRequest::CmCloudEnrollmentStatus)? {
            WireResponse::CmCloudEnrollmentStatus(value) => Ok(value),
            _ => Err(ControlError::InvalidEnvelope),
        }
    }

    pub fn enroll_cmcloud(&self, pairing_code: &str) -> Result<serde_json::Value, ControlError> {
        match self.exchange(WireRequest::EnrollCmCloud {
            pairing_code: pairing_code.to_owned(),
        })? {
            WireResponse::CmCloudEnrollmentStatus(value) => Ok(value),
            _ => Err(ControlError::InvalidEnvelope),
        }
    }

    fn command(&self, command: ControlCommand) -> Result<ControlStatus, ControlError> {
        match self.exchange(WireRequest::Command(command))? {
            WireResponse::Status(status) => Ok(status),
            _ => Err(ControlError::InvalidEnvelope),
        }
    }

    fn exchange(&self, request: WireRequest) -> Result<WireResponse, ControlError> {
        let deadline = deadline_after(self.timeout)?;
        let (mut stream, request_id, mut reader) = self.send_request(request, deadline)?;
        match reader
            .read_json::<ServerEnvelope>(&mut stream, deadline)?
            .ok_or(ControlError::InvalidEnvelope)?
        {
            ServerEnvelope::Response {
                version,
                request_id: response_id,
                payload,
            } if valid_server_envelope(version, &request_id, &response_id) => Ok(payload),
            ServerEnvelope::Error {
                version,
                request_id: response_id,
                code,
            } if valid_server_envelope(version, &request_id, &response_id) => {
                Err(ControlError::from_code(&code).unwrap_or(ControlError::CommandFailed))
            }
            _ => Err(ControlError::InvalidEnvelope),
        }
    }

    fn subscribe(&self, request: WireRequest) -> Result<ControlUpdateEventStream, ControlError> {
        let deadline = deadline_after(self.timeout)?;
        let (mut stream, request_id, mut reader) = self.send_request(request, deadline)?;
        match reader
            .read_json::<ServerEnvelope>(&mut stream, deadline)?
            .ok_or(ControlError::InvalidEnvelope)?
        {
            ServerEnvelope::Response {
                version,
                request_id: response_id,
                payload: WireResponse::SubscriptionAccepted,
            } if valid_server_envelope(version, &request_id, &response_id) => {
                Ok(ControlUpdateEventStream {
                    stream,
                    reader,
                    request_id,
                    timeout: self.timeout,
                })
            }
            ServerEnvelope::Error {
                version,
                request_id: response_id,
                code,
            } if valid_server_envelope(version, &request_id, &response_id) => {
                Err(ControlError::from_code(&code).unwrap_or(ControlError::CommandFailed))
            }
            _ => Err(ControlError::InvalidEnvelope),
        }
    }

    fn send_request(
        &self,
        request: WireRequest,
        deadline: Instant,
    ) -> Result<(Stream, String, FrameReader), ControlError> {
        let request_id = uuid::Uuid::new_v4().simple().to_string();
        let mut envelope = RequestEnvelope {
            version: CONTROL_PROTOCOL_VERSION,
            request_id: request_id.clone(),
            request,
        };
        let result = (|| {
            let mut stream = connect_endpoint(&self.endpoint, deadline)?;
            write_json_frame(&mut stream, &envelope, deadline)?;
            Ok((stream, request_id, FrameReader::new()))
        })();
        envelope.request.zeroize_sensitive();
        result
    }
}

pub struct ControlUpdateEventStream {
    stream: Stream,
    reader: FrameReader,
    request_id: String,
    timeout: Duration,
}

impl ControlUpdateEventStream {
    /// Reads one typed event. `None` means the peer closed cleanly.
    pub fn next_event(&mut self) -> Result<Option<ControlUpdateEvent>, ControlError> {
        loop {
            let Some(envelope) = self
                .reader
                .read_json::<ServerEnvelope>(&mut self.stream, deadline_after(self.timeout)?)?
            else {
                return Ok(None);
            };
            match envelope {
                ServerEnvelope::Event {
                    version,
                    request_id,
                    event,
                } if valid_server_envelope(version, &self.request_id, &request_id) => match event {
                    WireEvent::Heartbeat => continue,
                    WireEvent::Data { id, event, data } => {
                        if !valid_event_token(&id) || !valid_event_token(&event) {
                            return Err(ControlError::InvalidEnvelope);
                        }
                        let data =
                            serde_json::to_vec(&data).map_err(|_| ControlError::InvalidEnvelope)?;
                        if data.len() > MAX_CONTROL_EVENT_DATA_BYTES {
                            return Err(ControlError::ResponseTooLarge);
                        }
                        return Ok(Some(ControlUpdateEvent { id, event, data }));
                    }
                },
                ServerEnvelope::Error {
                    version,
                    request_id,
                    code,
                } if valid_server_envelope(version, &self.request_id, &request_id) => {
                    return Err(
                        ControlError::from_code(&code).unwrap_or(ControlError::CommandFailed)
                    );
                }
                _ => return Err(ControlError::InvalidEnvelope),
            }
        }
    }
}

struct FrameReader {
    codec: LengthDelimitedCodec,
    buffer: BytesMut,
}

impl FrameReader {
    fn new() -> Self {
        Self {
            codec: control_codec(),
            buffer: BytesMut::with_capacity(4 * 1024),
        }
    }

    fn read_json<T: DeserializeOwned>(
        &mut self,
        stream: &mut Stream,
        deadline: Instant,
    ) -> Result<Option<T>, ControlError> {
        self.read_json_cancellable(stream, deadline, None)
    }

    fn read_json_cancellable<T: DeserializeOwned>(
        &mut self,
        stream: &mut Stream,
        deadline: Instant,
        cancellation: Option<&AtomicBool>,
    ) -> Result<Option<T>, ControlError> {
        let Some(mut frame) = self.read_frame_cancellable(stream, deadline, cancellation)? else {
            return Ok(None);
        };
        let decoded =
            serde_json::from_slice(frame.as_ref()).map_err(|_| ControlError::InvalidEnvelope);
        frame.as_mut().zeroize();
        decoded.map(Some)
    }

    fn read_frame_cancellable(
        &mut self,
        stream: &mut Stream,
        deadline: Instant,
        cancellation: Option<&AtomicBool>,
    ) -> Result<Option<BytesMut>, ControlError> {
        let mut chunk = Zeroizing::new([0_u8; 4096]);
        loop {
            match self.codec.decode(&mut self.buffer) {
                Ok(Some(frame)) => return Ok(Some(frame)),
                Ok(None) => {}
                Err(_) => {
                    self.buffer.as_mut().zeroize();
                    self.buffer.clear();
                    return Err(ControlError::ResponseTooLarge);
                }
            }
            ensure_io_active(deadline, cancellation)?;
            match stream.read(chunk.as_mut()) {
                Ok(0) => {
                    #[cfg(unix)]
                    {
                        if self.buffer.is_empty() {
                            return Ok(None);
                        }
                        self.buffer.as_mut().zeroize();
                        self.buffer.clear();
                        return Err(ControlError::InvalidEnvelope);
                    }
                    #[cfg(windows)]
                    {
                        // On nonblocking Windows byte-mode pipes, both an idle read and a
                        // disconnected peer are reported as zero bytes. A zero-length pipe write
                        // probes the connection without adding protocol bytes.
                        if !windows_pipe_has_peer(stream)? {
                            if self.buffer.is_empty() {
                                return Ok(None);
                            }
                            self.buffer.as_mut().zeroize();
                            self.buffer.clear();
                            return Err(ControlError::InvalidEnvelope);
                        }
                        wait_for_io_cancellable(deadline, cancellation)?;
                    }
                }
                Ok(count) => self.buffer.extend_from_slice(&chunk[..count]),
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock
                            | std::io::ErrorKind::TimedOut
                            | std::io::ErrorKind::Interrupted
                    ) =>
                {
                    wait_for_io_cancellable(deadline, cancellation)?;
                }
                Err(_) => return Err(ControlError::Io),
            }
        }
    }

    fn reject_available_trailing_data(
        &mut self,
        stream: &mut Stream,
        deadline: Instant,
        cancellation: Option<&AtomicBool>,
    ) -> Result<(), ControlError> {
        ensure_io_active(deadline, cancellation)?;
        if !self.buffer.is_empty() {
            self.buffer.as_mut().zeroize();
            self.buffer.clear();
            return Err(ControlError::InvalidEnvelope);
        }

        let mut byte = [0_u8; 1];
        match stream.read(&mut byte) {
            Ok(0) => Ok(()),
            Ok(_) => {
                byte.zeroize();
                Err(ControlError::InvalidEnvelope)
            }
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock
                        | io::ErrorKind::TimedOut
                        | io::ErrorKind::Interrupted
                ) =>
            {
                Ok(())
            }
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::BrokenPipe
                        | io::ErrorKind::ConnectionAborted
                        | io::ErrorKind::ConnectionReset
                        | io::ErrorKind::NotConnected
                        | io::ErrorKind::UnexpectedEof
                ) =>
            {
                Ok(())
            }
            Err(_) => Err(ControlError::Io),
        }
    }
}

#[cfg(windows)]
fn windows_pipe_has_peer(stream: &Stream) -> Result<bool, ControlError> {
    let Stream::NamedPipe(stream) = stream;
    let pipe = stream.inner();
    let mut writer = pipe;
    let result = writer.write(&[]);
    pipe.assume_flushed();
    match result {
        Ok(0) => Ok(true),
        Ok(_) => Err(ControlError::Io),
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::BrokenPipe
                    | io::ErrorKind::ConnectionAborted
                    | io::ErrorKind::ConnectionReset
                    | io::ErrorKind::NotConnected
                    | io::ErrorKind::UnexpectedEof
            ) =>
        {
            Ok(false)
        }
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(true),
        Err(_) => Err(ControlError::Io),
    }
}

impl Drop for FrameReader {
    fn drop(&mut self) {
        self.buffer.as_mut().zeroize();
    }
}

fn write_json_frame<T: Serialize>(
    stream: &mut Stream,
    value: &T,
    deadline: Instant,
) -> Result<(), ControlError> {
    write_json_frame_cancellable(stream, value, deadline, None)
}

fn write_json_frame_cancellable<T: Serialize>(
    stream: &mut Stream,
    value: &T,
    deadline: Instant,
    cancellation: Option<&AtomicBool>,
) -> Result<(), ControlError> {
    let serialized = serde_json::to_vec(value).map_err(|_| ControlError::InvalidEnvelope)?;
    if serialized.len() > MAX_CONTROL_FRAME_BYTES {
        let mut serialized = serialized;
        serialized.zeroize();
        return Err(ControlError::ResponseTooLarge);
    }
    let mut codec = control_codec();
    let mut encoded = BytesMut::with_capacity(serialized.len().saturating_add(4));
    let owned = Bytes::from_owner(Zeroizing::new(serialized));
    if codec.encode(owned, &mut encoded).is_err() {
        encoded.as_mut().zeroize();
        return Err(ControlError::ResponseTooLarge);
    }
    let result = write_all_until(stream, encoded.as_ref(), deadline, cancellation);
    encoded.as_mut().zeroize();
    result
}

fn control_codec() -> LengthDelimitedCodec {
    let mut builder = LengthDelimitedCodec::builder();
    builder
        .max_frame_length(MAX_CONTROL_FRAME_BYTES)
        .length_field_type::<u32>()
        .big_endian();
    builder.new_codec()
}

fn write_all_until(
    stream: &mut Stream,
    mut bytes: &[u8],
    deadline: Instant,
    cancellation: Option<&AtomicBool>,
) -> Result<(), ControlError> {
    while !bytes.is_empty() {
        ensure_io_active(deadline, cancellation)?;
        let chunk = &bytes[..bytes.len().min(CONTROL_WRITE_CHUNK_BYTES)];
        match stream.write(chunk) {
            Ok(0) => wait_for_io_cancellable(deadline, cancellation)?,
            Ok(count) => bytes = &bytes[count..],
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock
                        | std::io::ErrorKind::TimedOut
                        | std::io::ErrorKind::Interrupted
                ) =>
            {
                wait_for_io_cancellable(deadline, cancellation)?;
            }
            Err(_) => return Err(ControlError::Io),
        }
    }
    ensure_io_active(deadline, cancellation)?;
    stream.flush().map_err(map_io_error)
}

fn configure_stream(stream: &Stream) -> Result<(), ControlError> {
    stream.set_nonblocking(true).map_err(map_io_error)
}

fn connect_endpoint(endpoint: &ControlEndpoint, deadline: Instant) -> Result<Stream, ControlError> {
    ensure_before_deadline(deadline)?;
    let remaining = deadline.saturating_duration_since(Instant::now());
    let stream = connect_endpoint_io(endpoint, remaining).map_err(map_io_error)?;
    ensure_before_deadline(deadline)?;
    configure_stream(&stream)?;
    Ok(stream)
}

#[cfg(unix)]
fn connect_endpoint_io(endpoint: &ControlEndpoint, timeout: Duration) -> io::Result<Stream> {
    let name = endpoint_name(endpoint)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "unsupported control endpoint"))?;
    ConnectOptions::new()
        .name(name)
        .wait_mode(ConnectWaitMode::Timeout(timeout))
        .nonblocking_stream(true)
        .connect_sync()
}

#[cfg(windows)]
fn connect_endpoint_io(endpoint: &ControlEndpoint, timeout: Duration) -> io::Result<Stream> {
    let ControlEndpoint::NamedPipe(path) = endpoint else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "unsupported control endpoint",
        ));
    };
    if !valid_local_pipe_name(path) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "unsupported control endpoint",
        ));
    }
    let pipe = DuplexPipeStream::<PipeBytes>::connect_by_path_with_wait_mode(
        path.as_str(),
        ConnectWaitMode::Timeout(timeout),
    )?;
    let stream = WindowsLocalStream::from(pipe);
    Ok(stream.into())
}

#[cfg(unix)]
fn endpoint_name(endpoint: &ControlEndpoint) -> Result<Name<'_>, ControlError> {
    let ControlEndpoint::UnixSocket(path) = endpoint else {
        return Err(ControlError::UnsupportedEndpoint);
    };
    path.as_os_str()
        .to_fs_name::<GenericFilePath>()
        .map_err(|_| ControlError::UnsupportedEndpoint)
}

#[cfg(windows)]
fn endpoint_name(endpoint: &ControlEndpoint) -> Result<Name<'_>, ControlError> {
    let ControlEndpoint::NamedPipe(name) = endpoint else {
        return Err(ControlError::UnsupportedEndpoint);
    };
    if !valid_local_pipe_name(name) {
        return Err(ControlError::UnsupportedEndpoint);
    }
    name.as_str()
        .to_fs_name::<NamedPipe>()
        .map_err(|_| ControlError::UnsupportedEndpoint)
}

#[cfg(unix)]
fn prepare_endpoint(endpoint: &ControlEndpoint) -> Result<(), ControlError> {
    use std::os::unix::fs::{FileTypeExt as _, PermissionsExt as _};

    let ControlEndpoint::UnixSocket(path) = endpoint else {
        return Err(ControlError::UnsupportedEndpoint);
    };
    let parent = path.parent().ok_or(ControlError::UnsupportedEndpoint)?;
    fs::create_dir_all(parent).map_err(|_| ControlError::Io)?;
    let metadata = fs::symlink_metadata(parent).map_err(|_| ControlError::Io)?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(ControlError::UnsupportedEndpoint);
    }
    fs::set_permissions(parent, fs::Permissions::from_mode(0o700)).map_err(|_| ControlError::Io)?;

    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_socket() => {
            let deadline = deadline_after(CONTROL_ENDPOINT_PROBE_TIMEOUT)?;
            let remaining = deadline.saturating_duration_since(Instant::now());
            match connect_endpoint_io(endpoint, remaining) {
                Ok(_) => return Err(ControlError::EndpointAlreadyInUse),
                Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => {}
                Err(_) => return Err(ControlError::EndpointAlreadyInUse),
            }
            fs::remove_file(path).map_err(|_| ControlError::EndpointAlreadyInUse)
        }
        Ok(_) => Err(ControlError::EndpointAlreadyInUse),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(ControlError::Io),
    }
}

#[cfg(windows)]
fn prepare_endpoint(endpoint: &ControlEndpoint) -> Result<(), ControlError> {
    endpoint_name(endpoint).map(|_| ())
}

#[cfg(unix)]
fn secure_bound_endpoint(endpoint: &ControlEndpoint) -> Result<(), ControlError> {
    use std::os::unix::fs::PermissionsExt as _;

    let ControlEndpoint::UnixSocket(path) = endpoint else {
        return Err(ControlError::UnsupportedEndpoint);
    };
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(|_| ControlError::Io)
}

#[cfg(windows)]
fn secure_bound_endpoint(_endpoint: &ControlEndpoint) -> Result<(), ControlError> {
    // interprocess 2.4.2 creates local-socket pipe instances with
    // accept_remote=false, which applies PIPE_REJECT_REMOTE_CLIENTS.
    Ok(())
}

fn map_bind_error(error: std::io::Error) -> ControlError {
    if matches!(
        error.kind(),
        std::io::ErrorKind::AddrInUse
            | std::io::ErrorKind::AlreadyExists
            | std::io::ErrorKind::PermissionDenied
    ) {
        ControlError::EndpointAlreadyInUse
    } else {
        map_io_error(error)
    }
}

fn map_io_error(error: std::io::Error) -> ControlError {
    if matches!(
        error.kind(),
        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
    ) {
        ControlError::Timeout
    } else {
        ControlError::Io
    }
}

fn deadline_after(timeout: Duration) -> Result<Instant, ControlError> {
    Instant::now()
        .checked_add(timeout)
        .ok_or(ControlError::Timeout)
}

fn ensure_before_deadline(deadline: Instant) -> Result<(), ControlError> {
    (Instant::now() < deadline)
        .then_some(())
        .ok_or(ControlError::Timeout)
}

fn ensure_not_cancelled(cancellation: &AtomicBool) -> Result<(), ControlError> {
    (!cancellation.load(Ordering::Acquire))
        .then_some(())
        .ok_or(ControlError::Io)
}

fn ensure_io_active(
    deadline: Instant,
    cancellation: Option<&AtomicBool>,
) -> Result<(), ControlError> {
    ensure_before_deadline(deadline)?;
    if let Some(cancellation) = cancellation {
        ensure_not_cancelled(cancellation)?;
    }
    Ok(())
}

#[cfg(test)]
fn wait_for_io(deadline: Instant) -> Result<(), ControlError> {
    wait_for_io_cancellable(deadline, None)
}

fn wait_for_io_cancellable(
    deadline: Instant,
    cancellation: Option<&AtomicBool>,
) -> Result<(), ControlError> {
    ensure_io_active(deadline, cancellation)?;
    let remaining = deadline.saturating_duration_since(Instant::now());
    thread::sleep(remaining.min(CONTROL_IO_POLL_INTERVAL));
    ensure_io_active(deadline, cancellation)
}

fn valid_request_id(value: &str) -> bool {
    value.len() == 32 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn valid_server_envelope(version: u16, expected: &str, actual: &str) -> bool {
    version == CONTROL_PROTOCOL_VERSION && expected == actual && valid_request_id(actual)
}

fn valid_event_token(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
}

fn valid_error_code(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}

pub fn is_local_endpoint(endpoint: &ControlEndpoint) -> bool {
    match endpoint {
        #[cfg(unix)]
        ControlEndpoint::UnixSocket(path) => path.is_absolute(),
        #[cfg(unix)]
        ControlEndpoint::NamedPipe(_) => false,
        #[cfg(windows)]
        ControlEndpoint::NamedPipe(name) => valid_local_pipe_name(name),
        #[cfg(windows)]
        ControlEndpoint::UnixSocket(_) => false,
        #[cfg(all(not(unix), not(windows)))]
        _ => false,
    }
}

#[cfg(windows)]
fn valid_local_pipe_name(value: &str) -> bool {
    let Some(name) = value.strip_prefix(WINDOWS_PIPE_PREFIX) else {
        return false;
    };
    !name.is_empty()
        && value.len() <= 256
        && !name.contains(['\\', '/'])
        && !name.bytes().any(|byte| byte.is_ascii_control())
}

pub fn default_unix_socket(state_root: &Path) -> ControlEndpoint {
    ControlEndpoint::UnixSocket(state_root.join("run").join("control.sock"))
}

/// Derives the private Control endpoint from the canonical unified state root.
pub fn default_local_endpoint(state_root: &Path) -> Result<ControlEndpoint, ControlError> {
    if !state_root.is_absolute() {
        return Err(ControlError::UnsupportedEndpoint);
    }
    let canonical_root = fs::canonicalize(state_root).map_err(|_| ControlError::Io)?;
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt as _;

        let mut hasher = Sha256::new();
        hasher.update(WINDOWS_PIPE_NAME_DOMAIN);
        for code_unit in canonical_root.as_os_str().encode_wide() {
            hasher.update(code_unit.to_le_bytes());
        }
        let digest = hasher.finalize();
        Ok(ControlEndpoint::NamedPipe(format!(
            r"\\.\pipe\cmclient-control-v1-{}",
            encode_hex(&digest)
        )))
    }
    #[cfg(unix)]
    {
        Ok(default_unix_socket(&canonical_root))
    }
    #[cfg(all(not(unix), not(windows)))]
    {
        let _ = canonical_root;
        Err(ControlError::UnsupportedEndpoint)
    }
}

#[cfg(windows)]
fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    fn status() -> ControlStatus {
        ControlStatus {
            schema_version: 3,
            agent: String::from("running"),
            identity: compiled_component_identity(InternalComponent::Agent).unwrap(),
            gateway: GatewayControlStatus::Running,
            management_web: ManagementWebControlStatus::Running,
            management_web_url: Some(String::from("http://127.0.0.1:7080")),
            uptime_seconds: 1,
            latest_error_code: None,
        }
    }

    struct TestRoot {
        path: PathBuf,
    }

    impl TestRoot {
        fn new(label: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "cmclient-control-{label}-{}",
                uuid::Uuid::new_v4().simple()
            ));
            fs::create_dir_all(&path).expect("test state root should exist");
            Self { path }
        }

        fn endpoint(&self) -> ControlEndpoint {
            default_local_endpoint(&self.path).expect("endpoint should derive")
        }
    }

    impl Drop for TestRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn connect_for_test(endpoint: &ControlEndpoint) -> Stream {
        connect_endpoint(
            endpoint,
            deadline_after(Duration::from_secs(2)).expect("test deadline should be valid"),
        )
        .expect("test client should connect")
    }

    #[test]
    fn control_status_requires_the_v3_component_identity_wire() {
        let current = status();
        let encoded = serde_json::to_value(&current).unwrap();
        assert_eq!(encoded["schemaVersion"], 3);
        assert_eq!(encoded["identity"]["component"], "agent");
        assert!(encoded.get("agentVersion").is_none());
        assert_eq!(
            serde_json::from_value::<ControlStatus>(encoded).unwrap(),
            current
        );
    }

    #[test]
    fn bounds_and_releases_control_connection_slots() {
        let limiter = ConnectionLimiter::new(2);
        let first = limiter.try_acquire().unwrap();
        let second = limiter.try_acquire().unwrap();
        assert!(limiter.try_acquire().is_none());
        assert_eq!(limiter.active(), 2);
        drop(first);
        let replacement = limiter.try_acquire().unwrap();
        drop(second);
        drop(replacement);
        assert_eq!(limiter.active(), 0);
    }

    #[test]
    fn dropped_clients_release_all_server_slots_before_the_io_deadline() {
        let root = TestRoot::new("dropped-slots");
        let endpoint = root.endpoint();
        let server = Arc::new(
            ControlServer::bind_with_timeout(
                endpoint.clone(),
                Arc::new(StaticControlHandler::new(status())),
                Duration::from_secs(2),
            )
            .unwrap(),
        );

        for _ in 0..MAX_CONTROL_CONNECTIONS {
            let client = connect_for_test(&endpoint);
            assert!(server.poll_once().unwrap());
            drop(client);
        }

        let release_deadline = deadline_after(Duration::from_secs(1)).unwrap();
        while server.connections.active() != 0 {
            server.reap_workers().unwrap();
            ensure_before_deadline(release_deadline).unwrap();
            thread::sleep(CONTROL_IO_POLL_INTERVAL);
        }

        let polling_server = Arc::clone(&server);
        let server_thread = thread::spawn(move || polling_server.serve_once());
        assert_eq!(
            ControlClient::new(endpoint).unwrap().status().unwrap(),
            status()
        );
        server_thread.join().unwrap().unwrap();
        drop(server);
    }

    struct RecordingHandler {
        commands: Mutex<Vec<ControlCommand>>,
    }

    impl RecordingHandler {
        fn new() -> Self {
            Self {
                commands: Mutex::new(Vec::new()),
            }
        }
    }

    impl ControlHandler for RecordingHandler {
        fn handle(&self, command: ControlCommand) -> Result<ControlStatus, ControlError> {
            self.commands.lock().unwrap().push(command);
            Ok(status())
        }
    }

    #[test]
    fn lifecycle_commands_round_trip_over_typed_frames() {
        let root = TestRoot::new("lifecycle");
        let endpoint = root.endpoint();
        let handler = Arc::new(RecordingHandler::new());
        let server = ControlServer::bind(endpoint.clone(), handler.clone()).unwrap();
        let server_thread = thread::spawn(move || {
            for _ in 0..6 {
                server.serve_once_inline().unwrap();
            }
        });
        let client = ControlClient::new(endpoint).unwrap();
        assert_eq!(client.status().unwrap(), status());
        assert_eq!(client.start().unwrap(), status());
        assert_eq!(client.open_desktop().unwrap(), status());
        assert_eq!(client.stop().unwrap(), status());
        assert_eq!(client.restart().unwrap(), status());
        assert_eq!(client.operational_reset().unwrap(), status());
        server_thread.join().unwrap();
        assert_eq!(
            *handler.commands.lock().unwrap(),
            vec![
                ControlCommand::Status,
                ControlCommand::Start,
                ControlCommand::OpenDesktop,
                ControlCommand::Stop,
                ControlCommand::Restart,
                ControlCommand::OperationalReset,
            ]
        );
    }

    struct DeferredShutdownHandler {
        committed: AtomicBool,
    }

    impl ControlHandler for DeferredShutdownHandler {
        fn handle(&self, _command: ControlCommand) -> Result<ControlStatus, ControlError> {
            Ok(status())
        }

        fn prepare_command(&self, command: ControlCommand) -> Result<ControlStatus, ControlError> {
            assert_eq!(command, ControlCommand::ShutdownAgent);
            assert!(!self.committed.load(Ordering::Acquire));
            Ok(status())
        }

        fn command_response_sent(&self, command: ControlCommand) {
            assert_eq!(command, ControlCommand::ShutdownAgent);
            self.committed.store(true, Ordering::Release);
        }
    }

    #[test]
    fn shutdown_is_committed_only_after_its_response_is_written() {
        let root = TestRoot::new("shutdown-ack");
        let endpoint = root.endpoint();
        let handler = Arc::new(DeferredShutdownHandler {
            committed: AtomicBool::new(false),
        });
        let server = ControlServer::bind(endpoint.clone(), handler.clone()).unwrap();
        let server_handler = handler.clone();
        let server_thread = thread::spawn(move || {
            while !server.poll_once().unwrap() {}
            let deadline = deadline_after(Duration::from_secs(1)).unwrap();
            while !server_handler.committed.load(Ordering::Acquire) {
                ensure_before_deadline(deadline).unwrap();
                thread::sleep(CONTROL_IO_POLL_INTERVAL);
            }
            drop(server);
        });

        assert_eq!(
            ControlClient::new(endpoint)
                .unwrap()
                .shutdown_agent()
                .unwrap(),
            status()
        );
        server_thread.join().unwrap();
        assert!(handler.committed.load(Ordering::Acquire));
    }

    #[test]
    fn unsupported_protocol_version_returns_typed_error() {
        let root = TestRoot::new("version");
        let endpoint = root.endpoint();
        let server = ControlServer::bind(
            endpoint.clone(),
            Arc::new(StaticControlHandler::new(status())),
        )
        .unwrap();
        let server_thread = thread::spawn(move || server.serve_once_inline().unwrap());
        let mut stream = connect_for_test(&endpoint);
        let request_id = uuid::Uuid::new_v4().simple().to_string();
        write_json_frame(
            &mut stream,
            &RequestEnvelope {
                version: CONTROL_PROTOCOL_VERSION + 1,
                request_id: request_id.clone(),
                request: WireRequest::Command(ControlCommand::Status),
            },
            deadline_after(Duration::from_secs(2)).unwrap(),
        )
        .unwrap();
        let mut reader = FrameReader::new();
        let response = reader
            .read_json::<ServerEnvelope>(
                &mut stream,
                deadline_after(Duration::from_secs(2)).unwrap(),
            )
            .unwrap()
            .unwrap();
        assert!(matches!(
            response,
            ServerEnvelope::Error { request_id: id, code, .. }
                if id == request_id && code == ControlError::ProtocolVersionUnsupported.code()
        ));
        server_thread.join().unwrap();
    }

    fn assert_bad_frame_does_not_poison_listener(bytes: Vec<u8>, label: &str) {
        let root = TestRoot::new(label);
        let endpoint = root.endpoint();
        let server = ControlServer::bind_with_timeout(
            endpoint.clone(),
            Arc::new(StaticControlHandler::new(status())),
            Duration::from_millis(200),
        )
        .unwrap();
        let server_thread = thread::spawn(move || {
            assert!(matches!(
                server.serve_once_inline(),
                Err(ControlError::InvalidEnvelope | ControlError::ResponseTooLarge)
            ));
            server.serve_once_inline().unwrap();
        });
        let mut stream = connect_for_test(&endpoint);
        stream.write_all(&bytes).unwrap();
        drop(stream);
        assert_eq!(
            ControlClient::new(endpoint).unwrap().status().unwrap(),
            status()
        );
        server_thread.join().unwrap();
    }

    #[test]
    fn trailing_frame_is_rejected_before_dispatch() {
        let root = TestRoot::new("trailing-frame");
        let endpoint = root.endpoint();
        let handler = Arc::new(RecordingHandler::new());
        let server = ControlServer::bind(endpoint.clone(), handler.clone()).unwrap();
        let server_thread = thread::spawn(move || server.serve_once_inline());
        let mut stream = connect_for_test(&endpoint);
        let mut bytes = BytesMut::new();
        let mut codec = control_codec();
        for command in [ControlCommand::Start, ControlCommand::Stop] {
            let encoded = serde_json::to_vec(&RequestEnvelope {
                version: CONTROL_PROTOCOL_VERSION,
                request_id: uuid::Uuid::new_v4().simple().to_string(),
                request: WireRequest::Command(command),
            })
            .unwrap();
            codec.encode(Bytes::from(encoded), &mut bytes).unwrap();
        }
        stream.write_all(bytes.as_ref()).unwrap();
        drop(stream);
        bytes.as_mut().zeroize();

        assert_eq!(
            server_thread.join().unwrap(),
            Err(ControlError::InvalidEnvelope)
        );
        assert!(handler.commands.lock().unwrap().is_empty());
    }

    #[test]
    fn malformed_envelope_is_rejected_without_poisoning_listener() {
        let mut bytes = Vec::from((8_u32).to_be_bytes());
        bytes.extend_from_slice(b"not-json");
        assert_bad_frame_does_not_poison_listener(bytes, "malformed");
    }

    #[test]
    fn oversized_frame_prefix_is_rejected_before_body() {
        let size = u32::try_from(MAX_CONTROL_FRAME_BYTES + 1).unwrap();
        assert_bad_frame_does_not_poison_listener(Vec::from(size.to_be_bytes()), "oversized");
    }

    #[test]
    fn slow_partial_frame_hits_the_bounded_deadline() {
        let root = TestRoot::new("slow");
        let endpoint = root.endpoint();
        let server = ControlServer::bind_with_timeout(
            endpoint.clone(),
            Arc::new(StaticControlHandler::new(status())),
            Duration::from_millis(75),
        )
        .unwrap();
        let server_thread = thread::spawn(move || server.serve_once_inline());
        let mut stream = connect_for_test(&endpoint);
        stream.write_all(&20_u32.to_be_bytes()).unwrap();
        stream.write_all(b"{").unwrap();
        thread::sleep(Duration::from_millis(125));
        drop(stream);
        assert_eq!(server_thread.join().unwrap(), Err(ControlError::Timeout));
    }

    #[test]
    fn mid_frame_disconnect_is_bounded() {
        let root = TestRoot::new("disconnect");
        let endpoint = root.endpoint();
        let server = ControlServer::bind_with_timeout(
            endpoint.clone(),
            Arc::new(StaticControlHandler::new(status())),
            Duration::from_millis(75),
        )
        .unwrap();
        let server_thread = thread::spawn(move || server.serve_once_inline());
        let mut stream = connect_for_test(&endpoint);
        stream.write_all(&20_u32.to_be_bytes()).unwrap();
        stream.write_all(b"{").unwrap();
        drop(stream);
        assert_eq!(
            server_thread.join().unwrap(),
            Err(ControlError::InvalidEnvelope)
        );
    }

    struct RejectingSecretHandler;

    impl ControlHandler for RejectingSecretHandler {
        fn handle(&self, _command: ControlCommand) -> Result<ControlStatus, ControlError> {
            Ok(status())
        }

        fn store_secret(&self, kind: ControlSecretKind, value: &str) -> Result<(), ControlError> {
            assert_eq!(kind, ControlSecretKind::CallMeshApiKey);
            assert_eq!(value, "secret-marker-must-not-return");
            Err(ControlError::SecretValueInvalid)
        }
    }

    #[test]
    fn secret_failures_return_only_a_stable_code() {
        let root = TestRoot::new("secret-error");
        let endpoint = root.endpoint();
        let server =
            ControlServer::bind(endpoint.clone(), Arc::new(RejectingSecretHandler)).unwrap();
        let server_thread = thread::spawn(move || server.serve_once_inline().unwrap());
        let mut stream = connect_for_test(&endpoint);
        let request_id = uuid::Uuid::new_v4().simple().to_string();
        write_json_frame(
            &mut stream,
            &RequestEnvelope {
                version: CONTROL_PROTOCOL_VERSION,
                request_id: request_id.clone(),
                request: WireRequest::StoreSecret {
                    kind: ControlSecretKind::CallMeshApiKey,
                    value: String::from("secret-marker-must-not-return"),
                },
            },
            deadline_after(Duration::from_secs(2)).unwrap(),
        )
        .unwrap();
        let mut reader = FrameReader::new();
        let response = reader
            .read_json::<ServerEnvelope>(
                &mut stream,
                deadline_after(Duration::from_secs(2)).unwrap(),
            )
            .unwrap()
            .unwrap();
        let encoded = serde_json::to_vec(&response).unwrap();
        assert!(matches!(
            response,
            ServerEnvelope::Error { request_id: id, code, .. }
                if id == request_id && code == ControlError::SecretValueInvalid.code()
        ));
        assert!(
            !encoded
                .windows("secret-marker-must-not-return".len())
                .any(|window| window == b"secret-marker-must-not-return")
        );
        server_thread.join().unwrap();
    }

    struct RejectingCMCloudEnrollmentHandler;

    impl ControlHandler for RejectingCMCloudEnrollmentHandler {
        fn handle(&self, _command: ControlCommand) -> Result<ControlStatus, ControlError> {
            Ok(status())
        }

        fn enroll_cmcloud(&self, pairing_code: &str) -> Result<serde_json::Value, ControlError> {
            assert_eq!(pairing_code, "pairing-marker-must-not-return");
            Err(ControlError::Application(String::from(
                "CMCLOUD_ENROLLMENT_REJECTED",
            )))
        }
    }

    #[test]
    fn cmcloud_enrollment_failures_preserve_stable_codes_without_pairing_material() {
        let root = TestRoot::new("cmcloud-enrollment-error");
        let endpoint = root.endpoint();
        let server = ControlServer::bind(
            endpoint.clone(),
            Arc::new(RejectingCMCloudEnrollmentHandler),
        )
        .unwrap();
        let server_thread = thread::spawn(move || server.serve_once_inline().unwrap());
        let mut stream = connect_for_test(&endpoint);
        let request_id = uuid::Uuid::new_v4().simple().to_string();
        write_json_frame(
            &mut stream,
            &RequestEnvelope {
                version: CONTROL_PROTOCOL_VERSION,
                request_id: request_id.clone(),
                request: WireRequest::EnrollCmCloud {
                    pairing_code: String::from("pairing-marker-must-not-return"),
                },
            },
            deadline_after(Duration::from_secs(2)).unwrap(),
        )
        .unwrap();
        let mut reader = FrameReader::new();
        let response = reader
            .read_json::<ServerEnvelope>(
                &mut stream,
                deadline_after(Duration::from_secs(2)).unwrap(),
            )
            .unwrap()
            .unwrap();
        let encoded = serde_json::to_vec(&response).unwrap();
        assert!(matches!(
            response,
            ServerEnvelope::Error { request_id: id, code, .. }
                if id == request_id && code == "CMCLOUD_ENROLLMENT_REJECTED"
        ));
        assert!(
            !encoded
                .windows("pairing-marker-must-not-return".len())
                .any(|window| window == b"pairing-marker-must-not-return")
        );
        server_thread.join().unwrap();
    }

    struct EventHandler;

    impl ControlHandler for EventHandler {
        fn handle(&self, _command: ControlCommand) -> Result<ControlStatus, ControlError> {
            Ok(status())
        }

        fn subscribe_update_events(
            &self,
        ) -> Result<mpsc::Receiver<ControlUpdateEvent>, ControlError> {
            let (sender, receiver) = mpsc::sync_channel(1);
            sender
                .send(ControlUpdateEvent {
                    id: String::from("update-1"),
                    event: String::from("update.status_changed"),
                    data: br#"{"schemaVersion":1}"#.to_vec(),
                })
                .unwrap();
            Ok(receiver)
        }
    }

    #[test]
    fn event_subscription_uses_ack_and_typed_event_envelopes() {
        let root = TestRoot::new("events");
        let endpoint = root.endpoint();
        let server = ControlServer::bind(endpoint.clone(), Arc::new(EventHandler)).unwrap();
        let server_thread = thread::spawn(move || server.serve_once_inline().unwrap());
        let mut events = ControlClient::new_with_timeout(endpoint, Duration::from_secs(2))
            .unwrap()
            .subscribe_update_events()
            .unwrap();
        let event = events.next_event().unwrap().unwrap();
        assert_eq!(event.id, "update-1");
        assert_eq!(event.event, "update.status_changed");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&event.data).unwrap(),
            serde_json::json!({ "schemaVersion": 1 })
        );
        server_thread.join().unwrap();
    }

    struct PersistentEventHandler {
        senders: Mutex<Vec<mpsc::SyncSender<ControlUpdateEvent>>>,
    }

    impl PersistentEventHandler {
        fn new() -> Self {
            Self {
                senders: Mutex::new(Vec::new()),
            }
        }
    }

    impl ControlHandler for PersistentEventHandler {
        fn handle(&self, _command: ControlCommand) -> Result<ControlStatus, ControlError> {
            Ok(status())
        }

        fn subscribe_update_events(
            &self,
        ) -> Result<mpsc::Receiver<ControlUpdateEvent>, ControlError> {
            let (sender, receiver) = mpsc::sync_channel(1);
            self.senders.lock().unwrap().push(sender);
            Ok(receiver)
        }
    }

    #[test]
    fn dropping_server_cancels_active_event_worker_and_allows_rebind() {
        let root = TestRoot::new("event-shutdown");
        let endpoint = root.endpoint();
        let server = Arc::new(
            ControlServer::bind(endpoint.clone(), Arc::new(PersistentEventHandler::new())).unwrap(),
        );
        let polling_server = Arc::clone(&server);
        let poll_thread = thread::spawn(move || while !polling_server.poll_once().unwrap() {});
        let mut events =
            ControlClient::new_with_timeout(endpoint.clone(), Duration::from_millis(500))
                .unwrap()
                .subscribe_update_events()
                .unwrap();
        poll_thread.join().unwrap();

        let started = Instant::now();
        drop(server);
        assert!(started.elapsed() < Duration::from_secs(1));
        let event_result = events.next_event();
        assert!(matches!(event_result, Ok(None) | Err(ControlError::Io)));
        drop(events);

        let replacement =
            ControlServer::bind(endpoint, Arc::new(StaticControlHandler::new(status()))).unwrap();
        drop(replacement);
    }

    #[test]
    fn canonical_roots_have_deterministic_non_colliding_endpoints() {
        let first = TestRoot::new("root-a");
        let second = TestRoot::new("root-b");
        assert_eq!(first.endpoint(), first.endpoint());
        assert_ne!(first.endpoint(), second.endpoint());
    }

    #[test]
    fn server_can_restart_on_the_same_endpoint() {
        let root = TestRoot::new("restart");
        let endpoint = root.endpoint();
        for _ in 0..2 {
            let server = ControlServer::bind(
                endpoint.clone(),
                Arc::new(StaticControlHandler::new(status())),
            )
            .unwrap();
            let server_thread = thread::spawn(move || server.serve_once_inline().unwrap());
            assert_eq!(
                ControlClient::new(endpoint.clone())
                    .unwrap()
                    .status()
                    .unwrap(),
                status()
            );
            server_thread.join().unwrap();
        }
    }

    #[test]
    fn only_one_listener_can_own_an_endpoint() {
        let root = TestRoot::new("exclusive-listener");
        let endpoint = root.endpoint();
        let _server = ControlServer::bind(
            endpoint.clone(),
            Arc::new(StaticControlHandler::new(status())),
        )
        .unwrap();
        assert!(matches!(
            ControlServer::bind(endpoint, Arc::new(StaticControlHandler::new(status()))),
            Err(ControlError::EndpointAlreadyInUse)
        ));
    }

    #[cfg(windows)]
    #[test]
    fn windows_busy_pipe_connect_respects_client_timeout() {
        let root = TestRoot::new("connect-timeout");
        let endpoint = root.endpoint();
        let server = ControlServer::bind(
            endpoint.clone(),
            Arc::new(StaticControlHandler::new(status())),
        )
        .unwrap();
        let first = connect_for_test(&endpoint);
        let client = ControlClient::new_with_timeout(endpoint, Duration::from_millis(50)).unwrap();
        let started = Instant::now();
        assert_eq!(client.status(), Err(ControlError::Timeout));
        assert!(started.elapsed() < Duration::from_secs(1));
        drop(first);
        drop(server);
    }

    #[cfg(windows)]
    #[test]
    fn windows_named_pipes_reject_remote_clients_and_malformed_forms() {
        use interprocess::os::windows::named_pipe::PipeListenerOptions;

        assert!(!PipeListenerOptions::new().accept_remote);
        for name in [
            r"\\127.0.0.1\pipe\cmclient-control",
            r"\\server\pipe\cmclient-control",
            r"\\.\pipe\nested\cmclient-control",
            r"\\.\pipe\",
        ] {
            assert!(matches!(
                ControlClient::new(ControlEndpoint::named_pipe(name)),
                Err(ControlError::UnsupportedEndpoint)
            ));
        }
    }

    #[cfg(unix)]
    #[test]
    fn unix_endpoint_uses_private_directory_and_socket_modes() {
        use std::os::unix::fs::PermissionsExt as _;

        let root = TestRoot::new("permissions");
        let endpoint = root.endpoint();
        let server = ControlServer::bind(
            endpoint.clone(),
            Arc::new(StaticControlHandler::new(status())),
        )
        .unwrap();
        let ControlEndpoint::UnixSocket(path) = endpoint else {
            unreachable!();
        };
        assert_eq!(
            fs::metadata(path.parent().unwrap())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        drop(server);
    }

    #[cfg(unix)]
    #[test]
    fn unix_stale_socket_is_removed_only_after_connection_refusal() {
        use std::os::unix::net::UnixListener;

        let root = TestRoot::new("stale-socket");
        let endpoint = root.endpoint();
        let ControlEndpoint::UnixSocket(path) = &endpoint else {
            unreachable!();
        };
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let stale = UnixListener::bind(path).unwrap();
        drop(stale);

        let server =
            ControlServer::bind(endpoint, Arc::new(StaticControlHandler::new(status()))).unwrap();
        drop(server);
    }
}
