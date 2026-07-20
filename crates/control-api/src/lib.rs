//! Local Agent control API over Unix sockets or Windows named pipes.

pub use cmclient_product_identity::{
    ComponentIdentityReport, IdentityError, InternalComponent, PackageProfile, ProductIdentity,
    ProductTarget, ReleaseChannel, ReleaseIdentity, RuntimeProfile, TargetArchitecture,
    TargetOperatingSystem, compiled_component_identity,
};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fmt::{Display, Formatter},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
};
use subtle::ConstantTimeEq;
use zeroize::Zeroize;

/// Stable workspace identity for the control API boundary.
pub const COMPONENT: &str = "control-api";
pub const REMOTE_CONTROL_SCOPE: &str = "control:admin";
pub const REMOTE_CONTROL_AUTH_SCHEME: &str = "CMClient-HMAC";
pub const REMOTE_CONTROL_TIMESTAMP_HEADER: &str = "x-cmclient-timestamp";
pub const REMOTE_CONTROL_NONCE_HEADER: &str = "x-cmclient-nonce";
pub const REMOTE_CONTROL_SCOPE_HEADER: &str = "x-cmclient-scope";
const REMOTE_CONTROL_WINDOW_SECONDS: u64 = 30;
const MAX_CONTROL_RESPONSE_BYTES: usize = 2 * 1024 * 1024;
const MAX_CONTROL_RESPONSE_WIRE_BYTES: usize = MAX_CONTROL_RESPONSE_BYTES + 8 * 1024;
const MAX_CONTROL_SSE_EVENT_BYTES: usize = 60 * 1024;
const MAX_CONTROL_CONNECTIONS: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteControlAuth {
    pub timestamp: String,
    pub nonce: String,
    pub scope: String,
    pub authorization: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteControlAuthError {
    Missing,
    Invalid,
    Expired,
    Replay,
}

impl RemoteControlAuthError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::Missing => "REMOTE_CONTROL_AUTH_MISSING",
            Self::Invalid => "REMOTE_CONTROL_AUTH_INVALID",
            Self::Expired => "REMOTE_CONTROL_AUTH_EXPIRED",
            Self::Replay => "REMOTE_CONTROL_REPLAY_REJECTED",
        }
    }
}

pub struct RemoteControlReplayGuard {
    nonces: Mutex<BTreeMap<String, u64>>,
}

impl Default for RemoteControlReplayGuard {
    fn default() -> Self {
        Self {
            nonces: Mutex::new(BTreeMap::new()),
        }
    }
}

impl RemoteControlReplayGuard {
    pub fn verify_and_record(
        &self,
        token: &str,
        method: &str,
        path: &str,
        body: &[u8],
        auth: &RemoteControlAuth,
        now_unix_seconds: u64,
    ) -> Result<(), RemoteControlAuthError> {
        validate_remote_auth_fields(method, path, auth)?;
        let timestamp = auth
            .timestamp
            .parse::<u64>()
            .map_err(|_| RemoteControlAuthError::Invalid)?;
        if now_unix_seconds.abs_diff(timestamp) > REMOTE_CONTROL_WINDOW_SECONDS {
            return Err(RemoteControlAuthError::Expired);
        }
        let expected = remote_control_signature(token, method, path, body, timestamp, &auth.nonce)?;
        let provided = auth
            .authorization
            .strip_prefix(REMOTE_CONTROL_AUTH_SCHEME)
            .and_then(|value| value.strip_prefix(' '))
            .ok_or(RemoteControlAuthError::Invalid)?;
        if provided.len() != expected.len()
            || !bool::from(provided.as_bytes().ct_eq(expected.as_bytes()))
        {
            return Err(RemoteControlAuthError::Invalid);
        }
        let mut nonces = self
            .nonces
            .lock()
            .map_err(|_| RemoteControlAuthError::Invalid)?;
        nonces.retain(|_, seen_at| {
            now_unix_seconds.abs_diff(*seen_at) <= REMOTE_CONTROL_WINDOW_SECONDS
        });
        if nonces.contains_key(&auth.nonce) {
            return Err(RemoteControlAuthError::Replay);
        }
        nonces.insert(auth.nonce.clone(), timestamp);
        Ok(())
    }
}

pub fn sign_remote_control_request(
    token: &str,
    method: &str,
    path: &str,
    body: &[u8],
    timestamp: u64,
) -> Result<RemoteControlAuth, RemoteControlAuthError> {
    let nonce = uuid::Uuid::new_v4().simple().to_string();
    let signature = remote_control_signature(token, method, path, body, timestamp, &nonce)?;
    Ok(RemoteControlAuth {
        timestamp: timestamp.to_string(),
        nonce,
        scope: String::from(REMOTE_CONTROL_SCOPE),
        authorization: format!("{REMOTE_CONTROL_AUTH_SCHEME} {signature}"),
    })
}

fn remote_control_signature(
    token: &str,
    method: &str,
    path: &str,
    body: &[u8],
    timestamp: u64,
    nonce: &str,
) -> Result<String, RemoteControlAuthError> {
    if token.len() < 32
        || token.len() > 4_096
        || token.bytes().any(|byte| byte.is_ascii_control())
        || !valid_remote_method(method)
        || !valid_remote_path(path)
        || !valid_remote_nonce(nonce)
    {
        return Err(RemoteControlAuthError::Invalid);
    }
    let body_digest = Sha256::digest(body);
    let canonical = format!(
        "v1\n{REMOTE_CONTROL_SCOPE}\n{timestamp}\n{nonce}\n{method}\n{path}\n{}",
        encode_hex(&body_digest)
    );
    let mut mac = Hmac::<Sha256>::new_from_slice(token.as_bytes())
        .map_err(|_| RemoteControlAuthError::Invalid)?;
    mac.update(canonical.as_bytes());
    let mut bytes = mac.finalize().into_bytes();
    let encoded = encode_hex(&bytes);
    bytes.zeroize();
    Ok(encoded)
}

fn validate_remote_auth_fields(
    method: &str,
    path: &str,
    auth: &RemoteControlAuth,
) -> Result<(), RemoteControlAuthError> {
    if auth.scope != REMOTE_CONTROL_SCOPE
        || !valid_remote_method(method)
        || !valid_remote_path(path)
        || !valid_remote_nonce(&auth.nonce)
        || auth.timestamp.is_empty()
        || auth.timestamp.len() > 20
        || !auth.timestamp.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(RemoteControlAuthError::Invalid);
    }
    Ok(())
}

fn valid_remote_method(value: &str) -> bool {
    matches!(value, "GET" | "POST" | "PUT" | "DELETE")
}

fn valid_remote_path(value: &str) -> bool {
    value.starts_with("/api/v1/control/")
        && value.len() <= 256
        && !value
            .chars()
            .any(|character| matches!(character, '?' | '#' | '\r' | '\n'))
}

fn valid_remote_nonce(value: &str) -> bool {
    value.len() == 32 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn error_from_http_response(head: &[u8], body: &[u8]) -> ControlError {
    let code = serde_json::from_slice::<serde_json::Value>(body)
        .ok()
        .and_then(|value| value.get("code")?.as_str().map(str::to_owned));
    if let Some(error) = code.as_deref().and_then(ControlError::from_code) {
        return error;
    }
    if head.starts_with(b"HTTP/1.1 401") || head.starts_with(b"HTTP/1.1 403") {
        ControlError::Authentication
    } else if head.starts_with(b"HTTP/1.1 408") || head.starts_with(b"HTTP/1.1 504") {
        ControlError::Timeout
    } else if head.starts_with(b"HTTP/1.1 413") {
        ControlError::ResponseTooLarge
    } else {
        ControlError::CommandFailed
    }
}

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

/// Sanitized Agent diagnostic bundle exposed only through the local Control API.
/// It intentionally has no file paths, configuration values, log records, or secrets.
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlSecretKind {
    CallMeshApiKey,
    /// Legacy APRS passcodes remain addressable only for removal. New values
    /// are rejected by the Control router with `CONTROL_SECRET_KIND_DEPRECATED`.
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

    fn from_path_segment(value: &str) -> Option<Self> {
        match value {
            "callmesh-api-key" => Some(Self::CallMeshApiKey),
            "aprs-passcode" => Some(Self::AprsPasscode),
            "management-admin-token" => Some(Self::ManagementAdminToken),
            _ => None,
        }
    }
}

/// Confirmation for a local secret mutation. It never contains the value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ControlSecretReceipt {
    pub stored: bool,
}

/// Safe update fields exposed through local control and SSE.
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

/// One Agent update state transition delivered through the local SSE stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlUpdateEvent {
    pub id: String,
    pub event: String,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

impl GatewayProjection {
    const fn control_path(self) -> &'static str {
        match self {
            Self::Meshtastic => "/api/v1/control/gateway/meshtastic",
            Self::Nodes => "/api/v1/control/gateway/nodes",
            Self::Positions => "/api/v1/control/gateway/positions",
            Self::Aprs => "/api/v1/control/gateway/aprs",
            Self::CallMesh => "/api/v1/control/gateway/callmesh",
            Self::Proxy => "/api/v1/control/gateway/proxy",
            Self::RecentEvents => "/api/v1/control/events/recent",
            Self::DatabaseIntegrity => "/api/v1/control/database/integrity-check",
            Self::Backup => "/api/v1/control/backups",
        }
    }

    const fn method(self) -> &'static str {
        match self {
            Self::DatabaseIntegrity | Self::Backup => "POST",
            _ => "GET",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlCommand {
    Status,
    Start,
    Stop,
    Restart,
    ShutdownAgent,
    EnableManagementWeb,
    DisableManagementWeb,
}

pub trait ControlHandler: Send + Sync {
    fn handle(&self, command: ControlCommand) -> Result<ControlStatus, ControlError>;

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
    InvalidHttp,
    ResponseTooLarge,
    Timeout,
    ResourceExhausted,
    Authentication,
    SecretKindDeprecated,
    SecretStoreUnavailable,
    SecretValueInvalid,
    CommandFailed,
}

impl ControlError {
    pub fn from_code(code: &str) -> Option<Self> {
        match code {
            "CONTROL_ENDPOINT_ALREADY_IN_USE" => Some(Self::EndpointAlreadyInUse),
            "CONTROL_ENDPOINT_UNSUPPORTED" => Some(Self::UnsupportedEndpoint),
            "CONTROL_IO_FAILED" => Some(Self::Io),
            "CONTROL_HTTP_INVALID" => Some(Self::InvalidHttp),
            "CONTROL_RESPONSE_TOO_LARGE" => Some(Self::ResponseTooLarge),
            "CONTROL_TIMEOUT" => Some(Self::Timeout),
            "CONTROL_RESOURCE_EXHAUSTED" => Some(Self::ResourceExhausted),
            "CONTROL_AUTHENTICATION_FAILED" => Some(Self::Authentication),
            "CONTROL_SECRET_KIND_DEPRECATED" => Some(Self::SecretKindDeprecated),
            "AGENT_SECRET_STORE_UNAVAILABLE" => Some(Self::SecretStoreUnavailable),
            "AGENT_SECRET_VALUE_INVALID" => Some(Self::SecretValueInvalid),
            "CONTROL_COMMAND_FAILED" => Some(Self::CommandFailed),
            _ => None,
        }
    }

    pub const fn code(&self) -> &'static str {
        match self {
            Self::EndpointAlreadyInUse => "CONTROL_ENDPOINT_ALREADY_IN_USE",
            Self::UnsupportedEndpoint => "CONTROL_ENDPOINT_UNSUPPORTED",
            Self::Io => "CONTROL_IO_FAILED",
            Self::InvalidHttp => "CONTROL_HTTP_INVALID",
            Self::ResponseTooLarge => "CONTROL_RESPONSE_TOO_LARGE",
            Self::Timeout => "CONTROL_TIMEOUT",
            Self::ResourceExhausted => "CONTROL_RESOURCE_EXHAUSTED",
            Self::Authentication => "CONTROL_AUTHENTICATION_FAILED",
            Self::SecretKindDeprecated => "CONTROL_SECRET_KIND_DEPRECATED",
            Self::SecretStoreUnavailable => "AGENT_SECRET_STORE_UNAVAILABLE",
            Self::SecretValueInvalid => "AGENT_SECRET_VALUE_INVALID",
            Self::CommandFailed => "CONTROL_COMMAND_FAILED",
        }
    }
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

impl Display for ControlError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.code())
    }
}

impl std::error::Error for ControlError {}

#[derive(Clone)]
pub struct ControlRouter {
    handler: Arc<dyn ControlHandler>,
}

enum ControlResponse {
    Json { status: u16, body: Vec<u8> },
    EventStream(mpsc::Receiver<ControlUpdateEvent>),
}

impl ControlRouter {
    pub fn new(handler: Arc<dyn ControlHandler>) -> Self {
        Self { handler }
    }

    fn route(&self, request: &str) -> Result<ControlResponse, ControlError> {
        let (head, body) = request
            .split_once("\r\n\r\n")
            .ok_or(ControlError::InvalidHttp)?;
        let request_line = head.lines().next().ok_or(ControlError::InvalidHttp)?;
        let route = match request_line
            .split_whitespace()
            .collect::<Vec<_>>()
            .as_slice()
        {
            ["GET", "/api/v1/control/status", "HTTP/1.1"]
            | ["GET", "/api/v1/control/status", "HTTP/1.0"] => {
                ControlRoute::Command(ControlCommand::Status)
            }
            ["POST", "/api/v1/control/start", "HTTP/1.1"]
            | ["POST", "/api/v1/control/start", "HTTP/1.0"] => {
                ControlRoute::Command(ControlCommand::Start)
            }
            ["POST", "/api/v1/control/stop", "HTTP/1.1"]
            | ["POST", "/api/v1/control/stop", "HTTP/1.0"] => {
                ControlRoute::Command(ControlCommand::Stop)
            }
            ["POST", "/api/v1/control/restart", "HTTP/1.1"]
            | ["POST", "/api/v1/control/restart", "HTTP/1.0"] => {
                ControlRoute::Command(ControlCommand::Restart)
            }
            ["POST", "/api/v1/control/agent/shutdown", "HTTP/1.1"]
            | ["POST", "/api/v1/control/agent/shutdown", "HTTP/1.0"] => {
                ControlRoute::Command(ControlCommand::ShutdownAgent)
            }
            ["POST", "/api/v1/control/web/enable", "HTTP/1.1"]
            | ["POST", "/api/v1/control/web/enable", "HTTP/1.0"] => {
                ControlRoute::Command(ControlCommand::EnableManagementWeb)
            }
            ["POST", "/api/v1/control/web/disable", "HTTP/1.1"]
            | ["POST", "/api/v1/control/web/disable", "HTTP/1.0"] => {
                ControlRoute::Command(ControlCommand::DisableManagementWeb)
            }
            ["GET", "/api/v1/control/updates", "HTTP/1.1"]
            | ["GET", "/api/v1/control/updates", "HTTP/1.0"] => ControlRoute::UpdateStatus,
            ["GET", "/api/v1/control/updates/events", "HTTP/1.1"]
            | ["GET", "/api/v1/control/updates/events", "HTTP/1.0"] => ControlRoute::UpdateEvents,
            ["GET", "/api/v1/control/diagnostics/bundle", "HTTP/1.1"]
            | ["GET", "/api/v1/control/diagnostics/bundle", "HTTP/1.0"] => {
                ControlRoute::DiagnosticsBundle
            }
            ["GET", "/api/v1/control/gateway/meshtastic", "HTTP/1.1"]
            | ["GET", "/api/v1/control/gateway/meshtastic", "HTTP/1.0"] => {
                ControlRoute::GatewayProjection(GatewayProjection::Meshtastic)
            }
            ["GET", "/api/v1/control/gateway/nodes", "HTTP/1.1"]
            | ["GET", "/api/v1/control/gateway/nodes", "HTTP/1.0"] => {
                ControlRoute::GatewayProjection(GatewayProjection::Nodes)
            }
            ["GET", "/api/v1/control/gateway/positions", "HTTP/1.1"]
            | ["GET", "/api/v1/control/gateway/positions", "HTTP/1.0"] => {
                ControlRoute::GatewayProjection(GatewayProjection::Positions)
            }
            ["GET", "/api/v1/control/gateway/aprs", "HTTP/1.1"]
            | ["GET", "/api/v1/control/gateway/aprs", "HTTP/1.0"] => {
                ControlRoute::GatewayProjection(GatewayProjection::Aprs)
            }
            ["GET", "/api/v1/control/gateway/callmesh", "HTTP/1.1"]
            | ["GET", "/api/v1/control/gateway/callmesh", "HTTP/1.0"] => {
                ControlRoute::GatewayProjection(GatewayProjection::CallMesh)
            }
            ["GET", "/api/v1/control/gateway/proxy", "HTTP/1.1"]
            | ["GET", "/api/v1/control/gateway/proxy", "HTTP/1.0"] => {
                ControlRoute::GatewayProjection(GatewayProjection::Proxy)
            }
            ["GET", "/api/v1/control/events/recent", "HTTP/1.1"]
            | ["GET", "/api/v1/control/events/recent", "HTTP/1.0"] => {
                ControlRoute::GatewayProjection(GatewayProjection::RecentEvents)
            }
            [
                "POST",
                "/api/v1/control/database/integrity-check",
                "HTTP/1.1",
            ]
            | [
                "POST",
                "/api/v1/control/database/integrity-check",
                "HTTP/1.0",
            ] => ControlRoute::GatewayProjection(GatewayProjection::DatabaseIntegrity),
            ["POST", "/api/v1/control/backups", "HTTP/1.1"]
            | ["POST", "/api/v1/control/backups", "HTTP/1.0"] => {
                ControlRoute::GatewayProjection(GatewayProjection::Backup)
            }
            ["GET", "/api/v1/control/events", "HTTP/1.1"]
            | ["GET", "/api/v1/control/events", "HTTP/1.0"] => ControlRoute::GatewayEvents,
            ["PUT", path, "HTTP/1.1"] | ["PUT", path, "HTTP/1.0"]
                if path.starts_with("/api/v1/control/secrets/") =>
            {
                if is_legacy_aprs_passcode_path(path) {
                    return error_response(ControlError::SecretKindDeprecated);
                }
                let kind = path
                    .strip_prefix("/api/v1/control/secrets/")
                    .and_then(ControlSecretKind::from_path_segment)
                    .ok_or(ControlError::InvalidHttp)?;
                ControlRoute::StoreSecret(kind)
            }
            ["DELETE", path, "HTTP/1.1"] | ["DELETE", path, "HTTP/1.0"]
                if path.starts_with("/api/v1/control/secrets/") =>
            {
                let kind = path
                    .strip_prefix("/api/v1/control/secrets/")
                    .and_then(ControlSecretKind::from_path_segment)
                    .ok_or(ControlError::InvalidHttp)?;
                ControlRoute::RemoveSecret(kind)
            }
            [_, _, _] => {
                return Ok(ControlResponse::Json {
                    status: 404,
                    body: br#"{"code":"CONTROL_ROUTE_NOT_FOUND"}"#.to_vec(),
                });
            }
            _ => return Err(ControlError::InvalidHttp),
        };
        let response = match route {
            ControlRoute::Command(command) => self.handler.handle(command).and_then(json_response),
            ControlRoute::UpdateStatus => self.handler.update_status().and_then(json_response),
            ControlRoute::UpdateEvents => self
                .handler
                .subscribe_update_events()
                .map(ControlResponse::EventStream),
            ControlRoute::DiagnosticsBundle => {
                self.handler.diagnostics_bundle().and_then(json_response)
            }
            ControlRoute::GatewayProjection(projection) => self
                .handler
                .gateway_projection(projection)
                .and_then(json_response),
            ControlRoute::GatewayEvents => self
                .handler
                .subscribe_gateway_events()
                .map(ControlResponse::EventStream),
            ControlRoute::StoreSecret(kind) => validated_body(head, body)
                .and_then(|value| self.handler.store_secret(kind, value))
                .and_then(|()| json_response(ControlSecretReceipt { stored: true })),
            ControlRoute::RemoveSecret(kind) => validated_body(head, body)
                .and_then(|value| {
                    if !value.is_empty() {
                        return Err(ControlError::InvalidHttp);
                    }
                    self.handler.remove_secret(kind)
                })
                .and_then(|stored| json_response(ControlSecretReceipt { stored })),
        };
        response.or_else(error_response)
    }
}

enum ControlRoute {
    Command(ControlCommand),
    UpdateStatus,
    UpdateEvents,
    DiagnosticsBundle,
    GatewayProjection(GatewayProjection),
    GatewayEvents,
    StoreSecret(ControlSecretKind),
    RemoveSecret(ControlSecretKind),
}

fn validated_body<'a>(head: &str, body: &'a str) -> Result<&'a str, ControlError> {
    let mut content_length = None;
    for line in head.lines().skip(1) {
        let (name, value) = line.split_once(':').ok_or(ControlError::InvalidHttp)?;
        if name.eq_ignore_ascii_case("content-length") {
            let length = value
                .trim()
                .parse::<usize>()
                .map_err(|_| ControlError::InvalidHttp)?;
            if content_length.replace(length).is_some() {
                return Err(ControlError::InvalidHttp);
            }
        }
    }
    if content_length != Some(body.len()) {
        return Err(ControlError::InvalidHttp);
    }
    Ok(body)
}

fn is_legacy_aprs_passcode_path(path: &str) -> bool {
    path.strip_prefix("/api/v1/control/secrets/")
        .is_some_and(|kind| kind == "aprs-passcode")
}

fn json_response<T: Serialize>(value: T) -> Result<ControlResponse, ControlError> {
    serde_json::to_vec(&value)
        .map(|body| ControlResponse::Json { status: 200, body })
        .map_err(|_| ControlError::Io)
}

fn error_response(error: ControlError) -> Result<ControlResponse, ControlError> {
    let status = match &error {
        ControlError::InvalidHttp | ControlError::SecretValueInvalid => 400,
        ControlError::Authentication => 401,
        ControlError::SecretKindDeprecated => 410,
        ControlError::ResponseTooLarge => 413,
        ControlError::Timeout => 504,
        ControlError::ResourceExhausted | ControlError::SecretStoreUnavailable => 503,
        _ => 500,
    };
    serde_json::to_vec(&serde_json::json!({ "code": error.code() }))
        .map(|body| ControlResponse::Json { status, body })
        .map_err(|_| ControlError::Io)
}

#[cfg(unix)]
mod unix {
    use super::{
        ConnectionLimiter, ControlEndpoint, ControlError, ControlHandler, ControlResponse,
        ControlRouter, ControlStatus, ControlUpdateEvent, MAX_CONTROL_CONNECTIONS,
        MAX_CONTROL_RESPONSE_BYTES, MAX_CONTROL_RESPONSE_WIRE_BYTES, MAX_CONTROL_SSE_EVENT_BYTES,
        error_from_http_response,
    };
    use std::{
        fs,
        io::{Read, Write},
        os::unix::fs::PermissionsExt,
        os::unix::net::{UnixListener, UnixStream},
        path::PathBuf,
        sync::mpsc::{Receiver, RecvTimeoutError},
        thread,
        time::Duration,
    };
    use zeroize::Zeroize;

    const MAX_REQUEST_BYTES: usize = 8 * 1024;
    const CONTROL_SERVER_IO_TIMEOUT: Duration = Duration::from_secs(30);

    pub struct ControlServer {
        endpoint: PathBuf,
        listener: UnixListener,
        router: ControlRouter,
        connections: ConnectionLimiter,
    }

    impl ControlServer {
        pub fn bind(
            endpoint: ControlEndpoint,
            handler: std::sync::Arc<dyn ControlHandler>,
        ) -> Result<Self, ControlError> {
            let ControlEndpoint::UnixSocket(path) = endpoint else {
                return Err(ControlError::UnsupportedEndpoint);
            };
            if path.exists() {
                match UnixStream::connect(&path) {
                    Ok(_) => return Err(ControlError::EndpointAlreadyInUse),
                    Err(_) => fs::remove_file(&path).map_err(|_| ControlError::Io)?,
                }
            }
            let listener = UnixListener::bind(&path).map_err(|_| ControlError::Io)?;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
                .map_err(|_| ControlError::Io)?;
            Ok(Self {
                endpoint: path,
                listener,
                router: ControlRouter::new(handler),
                connections: ConnectionLimiter::new(MAX_CONTROL_CONNECTIONS),
            })
        }

        pub fn serve_once(&self) -> Result<(), ControlError> {
            let (mut stream, _) = self.listener.accept().map_err(|_| ControlError::Io)?;
            if configure_stream(&stream).is_err() {
                return Ok(());
            }
            let Some(permit) = self.connections.try_acquire() else {
                let body = br#"{"code":"CONTROL_RESOURCE_EXHAUSTED"}"#;
                let _ = write_json_response(&mut stream, 503, body);
                return Ok(());
            };
            let router = self.router.clone();
            thread::spawn(move || {
                let _permit = permit;
                let _ = serve_connection(stream, router);
            });
            Ok(())
        }

        pub fn endpoint(&self) -> &std::path::Path {
            &self.endpoint
        }
    }

    fn configure_stream(stream: &UnixStream) -> Result<(), ControlError> {
        stream
            .set_read_timeout(Some(CONTROL_SERVER_IO_TIMEOUT))
            .and_then(|_| stream.set_write_timeout(Some(CONTROL_SERVER_IO_TIMEOUT)))
            .map_err(|_| ControlError::Io)
    }

    fn serve_connection(mut stream: UnixStream, router: ControlRouter) -> Result<(), ControlError> {
        let mut request = read_request(&mut stream)?;
        let response = std::str::from_utf8(&request)
            .map_err(|_| ControlError::InvalidHttp)
            .and_then(|request| router.route(request));
        request.zeroize();
        match response? {
            ControlResponse::Json { status, body } => {
                write_json_response(&mut stream, status, &body)
            }
            ControlResponse::EventStream(events) => write_update_event_stream(&mut stream, events),
        }
    }

    fn read_request(stream: &mut UnixStream) -> Result<Vec<u8>, ControlError> {
        let mut request = Vec::new();
        let mut chunk = [0_u8; 2048];
        loop {
            let count = stream.read(&mut chunk).map_err(|_| ControlError::Io)?;
            if count == 0 {
                return Err(ControlError::InvalidHttp);
            }
            request.extend_from_slice(&chunk[..count]);
            if request.len() > MAX_REQUEST_BYTES {
                return Err(ControlError::ResponseTooLarge);
            }
            let Some(header_end) = request
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .map(|index| index + 4)
            else {
                continue;
            };
            let header = std::str::from_utf8(&request[..header_end])
                .map_err(|_| ControlError::InvalidHttp)?;
            let content_length = content_length(header)?;
            let request_length = header_end.saturating_add(content_length);
            if request_length > MAX_REQUEST_BYTES {
                return Err(ControlError::ResponseTooLarge);
            }
            if request.len() >= request_length {
                request.truncate(request_length);
                return Ok(request);
            }
        }
    }

    fn content_length(header: &str) -> Result<usize, ControlError> {
        let mut length = None;
        for line in header.lines().skip(1) {
            if line.is_empty() {
                continue;
            }
            let (name, value) = line.split_once(':').ok_or(ControlError::InvalidHttp)?;
            if name.eq_ignore_ascii_case("content-length") {
                let parsed = value
                    .trim()
                    .parse::<usize>()
                    .map_err(|_| ControlError::InvalidHttp)?;
                if length.replace(parsed).is_some() {
                    return Err(ControlError::InvalidHttp);
                }
            }
        }
        length.ok_or(ControlError::InvalidHttp)
    }

    fn write_json_response(
        stream: &mut UnixStream,
        status: u16,
        body: &[u8],
    ) -> Result<(), ControlError> {
        let status_text = match status {
            200 => "OK",
            400 => "Bad Request",
            401 => "Unauthorized",
            410 => "Gone",
            413 => "Content Too Large",
            503 => "Service Unavailable",
            404 => "Not Found",
            504 => "Gateway Timeout",
            _ => "Internal Server Error",
        };
        let header = format!(
            "HTTP/1.1 {status} {status_text}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            body.len()
        );
        stream
            .write_all(header.as_bytes())
            .map_err(|_| ControlError::Io)?;
        stream.write_all(body).map_err(|_| ControlError::Io)
    }

    fn write_update_event_stream(
        stream: &mut UnixStream,
        events: Receiver<ControlUpdateEvent>,
    ) -> Result<(), ControlError> {
        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream; charset=utf-8\r\ncache-control: no-cache\r\nconnection: close\r\n\r\n",
            )
            .map_err(|_| ControlError::Io)?;
        loop {
            match events.recv_timeout(Duration::from_secs(15)) {
                Ok(event) => write_sse_event(stream, &event)?,
                Err(RecvTimeoutError::Timeout) => stream
                    .write_all(b": heartbeat\n\n")
                    .map_err(|_| ControlError::Io)?,
                Err(RecvTimeoutError::Disconnected) => return Ok(()),
            }
        }
    }

    fn write_sse_event(
        stream: &mut UnixStream,
        event: &ControlUpdateEvent,
    ) -> Result<(), ControlError> {
        if !is_safe_sse_token(&event.id)
            || !is_safe_sse_token(&event.event)
            || event.data.len() > MAX_CONTROL_SSE_EVENT_BYTES
            || event.data.iter().any(|byte| matches!(*byte, b'\r' | b'\n'))
        {
            return Err(ControlError::InvalidHttp);
        }
        stream
            .write_all(format!("id: {}\nevent: {}\ndata: ", event.id, event.event).as_bytes())
            .and_then(|_| stream.write_all(&event.data))
            .and_then(|_| stream.write_all(b"\n\n"))
            .map_err(|_| ControlError::Io)
    }

    fn is_safe_sse_token(value: &str) -> bool {
        !value.is_empty()
            && value.len() <= 128
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
    }

    impl Drop for ControlServer {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.endpoint);
        }
    }

    pub struct ControlClient {
        endpoint: PathBuf,
        timeout: Duration,
    }

    /// Blocking reader for the Agent-owned update event stream.
    pub struct ControlUpdateEventStream {
        stream: UnixStream,
        buffer: Vec<u8>,
    }

    impl ControlClient {
        pub fn new(endpoint: ControlEndpoint) -> Result<Self, ControlError> {
            Self::new_with_timeout(endpoint, Duration::from_secs(30))
        }

        pub fn new_with_timeout(
            endpoint: ControlEndpoint,
            timeout: Duration,
        ) -> Result<Self, ControlError> {
            let ControlEndpoint::UnixSocket(endpoint) = endpoint else {
                return Err(ControlError::UnsupportedEndpoint);
            };
            if timeout.is_zero() {
                return Err(ControlError::Timeout);
            }
            Ok(Self { endpoint, timeout })
        }

        pub fn status(&self) -> Result<ControlStatus, ControlError> {
            self.request("GET", "/api/v1/control/status")
        }

        pub fn start(&self) -> Result<ControlStatus, ControlError> {
            self.request("POST", "/api/v1/control/start")
        }

        pub fn stop(&self) -> Result<ControlStatus, ControlError> {
            self.request("POST", "/api/v1/control/stop")
        }

        pub fn restart(&self) -> Result<ControlStatus, ControlError> {
            self.request("POST", "/api/v1/control/restart")
        }

        pub fn shutdown_agent(&self) -> Result<ControlStatus, ControlError> {
            self.request("POST", "/api/v1/control/agent/shutdown")
        }

        pub fn enable_management_web(&self) -> Result<ControlStatus, ControlError> {
            self.request("POST", "/api/v1/control/web/enable")
        }

        pub fn disable_management_web(&self) -> Result<ControlStatus, ControlError> {
            self.request("POST", "/api/v1/control/web/disable")
        }

        pub fn update_status(&self) -> Result<super::UpdateControlStatus, ControlError> {
            self.request_update("GET", "/api/v1/control/updates")
        }

        pub fn diagnostics_bundle(&self) -> Result<super::DiagnosticsControlBundle, ControlError> {
            self.request_json("GET", "/api/v1/control/diagnostics/bundle", "")
        }

        pub fn gateway_projection(
            &self,
            projection: super::GatewayProjection,
        ) -> Result<serde_json::Value, ControlError> {
            self.request_json(projection.method(), projection.control_path(), "")
        }

        pub fn store_secret(
            &self,
            kind: super::ControlSecretKind,
            value: &str,
        ) -> Result<super::ControlSecretReceipt, ControlError> {
            self.request_json(
                "PUT",
                &format!("/api/v1/control/secrets/{}", kind.path_segment()),
                value,
            )
        }

        pub fn remove_secret(
            &self,
            kind: super::ControlSecretKind,
        ) -> Result<super::ControlSecretReceipt, ControlError> {
            self.request_json(
                "DELETE",
                &format!("/api/v1/control/secrets/{}", kind.path_segment()),
                "",
            )
        }

        pub fn subscribe_update_events(&self) -> Result<ControlUpdateEventStream, ControlError> {
            self.subscribe_event_stream("/api/v1/control/updates/events")
        }

        pub fn subscribe_gateway_events(&self) -> Result<ControlUpdateEventStream, ControlError> {
            self.subscribe_event_stream("/api/v1/control/events")
        }

        fn subscribe_event_stream(
            &self,
            path: &str,
        ) -> Result<ControlUpdateEventStream, ControlError> {
            let mut stream = self.connect()?;
            let request = format!(
                "GET {path} HTTP/1.1\r\nhost: localhost\r\naccept: text/event-stream\r\ncontent-length: 0\r\n\r\n"
            );
            stream.write_all(request.as_bytes()).map_err(map_io_error)?;
            let response = read_sse_response_head(&mut stream)?;
            let separator = response
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .ok_or(ControlError::InvalidHttp)?;
            let (head, body) = response.split_at(separator + 4);
            if !head.starts_with(b"HTTP/1.1 200")
                || !String::from_utf8_lossy(head)
                    .to_ascii_lowercase()
                    .contains("content-type: text/event-stream")
            {
                return Err(error_from_http_response(head, body));
            }
            Ok(ControlUpdateEventStream {
                stream,
                buffer: body.to_vec(),
            })
        }

        fn request(&self, method: &str, path: &str) -> Result<ControlStatus, ControlError> {
            self.request_json(method, path, "")
        }

        fn request_update(
            &self,
            method: &str,
            path: &str,
        ) -> Result<super::UpdateControlStatus, ControlError> {
            self.request_json(method, path, "")
        }

        fn request_json<T: serde::de::DeserializeOwned>(
            &self,
            method: &str,
            path: &str,
            body: &str,
        ) -> Result<T, ControlError> {
            let mut stream = self.connect()?;
            let mut request = format!(
                "{method} {path} HTTP/1.1\r\nhost: localhost\r\ncontent-length: {}\r\n\r\n{body}",
                body.len()
            );
            let write_result = stream.write_all(request.as_bytes());
            request.zeroize();
            write_result.map_err(map_io_error)?;
            let mut response = Vec::new();
            (&mut stream)
                .take((MAX_CONTROL_RESPONSE_WIRE_BYTES + 1) as u64)
                .read_to_end(&mut response)
                .map_err(map_io_error)?;
            if response.len() > MAX_CONTROL_RESPONSE_WIRE_BYTES {
                return Err(ControlError::ResponseTooLarge);
            }
            let separator = response
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .ok_or(ControlError::InvalidHttp)?;
            let (head, body) = response.split_at(separator + 4);
            if !head.starts_with(b"HTTP/1.1 200") {
                return Err(error_from_http_response(head, body));
            }
            if body.len() > MAX_CONTROL_RESPONSE_BYTES {
                return Err(ControlError::ResponseTooLarge);
            }
            serde_json::from_slice(body).map_err(|_| ControlError::InvalidHttp)
        }

        fn connect(&self) -> Result<UnixStream, ControlError> {
            let stream = UnixStream::connect(&self.endpoint).map_err(map_io_error)?;
            stream
                .set_read_timeout(Some(self.timeout))
                .map_err(map_io_error)?;
            stream
                .set_write_timeout(Some(self.timeout))
                .map_err(map_io_error)?;
            Ok(stream)
        }
    }

    impl ControlUpdateEventStream {
        /// Reads one update state transition. `None` means the stream closed cleanly.
        pub fn next_event(&mut self) -> Result<Option<ControlUpdateEvent>, ControlError> {
            loop {
                if let Some((index, boundary_length)) = sse_boundary(&self.buffer) {
                    let block = self.buffer[..index].to_vec();
                    self.buffer.drain(..index + boundary_length);
                    if let Some(event) = parse_sse_event(&block)? {
                        return Ok(Some(event));
                    }
                    continue;
                }
                if self.buffer.len() > 64 * 1024 {
                    return Err(ControlError::ResponseTooLarge);
                }
                let mut chunk = [0_u8; 4096];
                let count = self.stream.read(&mut chunk).map_err(map_io_error)?;
                if count == 0 {
                    return if self.buffer.is_empty() {
                        Ok(None)
                    } else {
                        Err(ControlError::InvalidHttp)
                    };
                }
                self.buffer.extend_from_slice(&chunk[..count]);
            }
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

    fn read_sse_response_head(stream: &mut UnixStream) -> Result<Vec<u8>, ControlError> {
        let mut response = Vec::new();
        let mut chunk = [0_u8; 4096];
        loop {
            let count = stream.read(&mut chunk).map_err(map_io_error)?;
            if count == 0 {
                return Err(ControlError::InvalidHttp);
            }
            response.extend_from_slice(&chunk[..count]);
            if response.windows(4).any(|window| window == b"\r\n\r\n") {
                return Ok(response);
            }
            if response.len() > 8 * 1024 {
                return Err(ControlError::ResponseTooLarge);
            }
        }
    }

    fn sse_boundary(buffer: &[u8]) -> Option<(usize, usize)> {
        buffer
            .windows(2)
            .position(|window| window == b"\n\n")
            .map(|index| (index, 2))
            .or_else(|| {
                buffer
                    .windows(4)
                    .position(|window| window == b"\r\n\r\n")
                    .map(|index| (index, 4))
            })
    }

    fn parse_sse_event(block: &[u8]) -> Result<Option<ControlUpdateEvent>, ControlError> {
        let block = std::str::from_utf8(block).map_err(|_| ControlError::InvalidHttp)?;
        let mut id = None;
        let mut event = None;
        let mut data = None;
        for line in block.lines() {
            if line.is_empty() || line.starts_with(':') {
                continue;
            }
            let (field, value) = line.split_once(':').ok_or(ControlError::InvalidHttp)?;
            let value = value.strip_prefix(' ').unwrap_or(value);
            match field {
                "id" => id = Some(value.to_owned()),
                "event" => event = Some(value.to_owned()),
                "data" => data = Some(value.as_bytes().to_vec()),
                _ => return Err(ControlError::InvalidHttp),
            }
        }
        match (id, event, data) {
            (None, None, None) => Ok(None),
            (Some(id), Some(event), Some(data))
                if is_safe_sse_token(&id)
                    && is_safe_sse_token(&event)
                    && !data.contains(&b'\n') =>
            {
                Ok(Some(ControlUpdateEvent { id, event, data }))
            }
            _ => Err(ControlError::InvalidHttp),
        }
    }
}

#[cfg(unix)]
pub use unix::{ControlClient, ControlServer, ControlUpdateEventStream};

#[cfg(windows)]
mod windows {
    use super::{
        ConnectionLimiter, ControlEndpoint, ControlError, ControlHandler, ControlResponse,
        ControlRouter, ControlStatus, ControlUpdateEvent, MAX_CONTROL_CONNECTIONS,
        MAX_CONTROL_RESPONSE_BYTES, MAX_CONTROL_RESPONSE_WIRE_BYTES, MAX_CONTROL_SSE_EVENT_BYTES,
        error_from_http_response,
    };
    use interprocess::{
        local_socket::{Listener, ListenerOptions, Stream, prelude::*},
        os::windows::local_socket::NamedPipe,
    };
    use std::{
        io::{Read, Write},
        sync::mpsc::{Receiver, RecvTimeoutError},
        thread,
        time::{Duration, Instant},
    };
    use zeroize::Zeroize;

    const MAX_REQUEST_BYTES: usize = 8 * 1024;
    const CONTROL_SERVER_IO_TIMEOUT: Duration = Duration::from_secs(30);
    const CONTROL_PIPE_WRITE_CHUNK_BYTES: usize = 256;

    pub struct ControlServer {
        listener: Listener,
        router: ControlRouter,
        connections: ConnectionLimiter,
    }

    impl ControlServer {
        pub fn bind(
            endpoint: ControlEndpoint,
            handler: std::sync::Arc<dyn ControlHandler>,
        ) -> Result<Self, ControlError> {
            let ControlEndpoint::NamedPipe(path) = endpoint else {
                return Err(ControlError::UnsupportedEndpoint);
            };
            let name = path
                .to_fs_name::<NamedPipe>()
                .map_err(|_| ControlError::UnsupportedEndpoint)?;
            let listener = ListenerOptions::new()
                .name(name)
                .create_sync()
                .map_err(|error| {
                    if error.kind() == std::io::ErrorKind::AddrInUse {
                        ControlError::EndpointAlreadyInUse
                    } else {
                        ControlError::Io
                    }
                })?;
            Ok(Self {
                listener,
                router: ControlRouter::new(handler),
                connections: ConnectionLimiter::new(MAX_CONTROL_CONNECTIONS),
            })
        }

        pub fn serve_once(&self) -> Result<(), ControlError> {
            let mut stream = self.listener.accept().map_err(|_| ControlError::Io)?;
            if configure_stream(&stream).is_err() {
                return Ok(());
            }
            let Some(permit) = self.connections.try_acquire() else {
                let body = br#"{"code":"CONTROL_RESOURCE_EXHAUSTED"}"#;
                let _ = write_json_response(&mut stream, 503, body);
                return Ok(());
            };
            let router = self.router.clone();
            thread::spawn(move || {
                let _permit = permit;
                let _ = serve_connection(stream, router);
            });
            Ok(())
        }
    }

    fn configure_stream(stream: &Stream) -> Result<(), ControlError> {
        stream.set_nonblocking(true).map_err(|_| ControlError::Io)
    }

    fn serve_connection(mut stream: Stream, router: ControlRouter) -> Result<(), ControlError> {
        let mut request = read_request(&mut stream)?;
        let response = std::str::from_utf8(&request)
            .map_err(|_| ControlError::InvalidHttp)
            .and_then(|request| router.route(request));
        request.zeroize();
        match response? {
            ControlResponse::Json { status, body } => {
                write_json_response(&mut stream, status, &body)
            }
            ControlResponse::EventStream(events) => write_update_event_stream(&mut stream, events),
        }
    }

    fn read_request(stream: &mut Stream) -> Result<Vec<u8>, ControlError> {
        let mut request = Vec::new();
        let mut chunk = [0_u8; 2048];
        let deadline = deadline_after(CONTROL_SERVER_IO_TIMEOUT)?;
        loop {
            let count = read_until(stream, &mut chunk, deadline)?;
            if count == 0 {
                return Err(ControlError::InvalidHttp);
            }
            request.extend_from_slice(&chunk[..count]);
            if request.len() > MAX_REQUEST_BYTES {
                return Err(ControlError::ResponseTooLarge);
            }
            let Some(header_end) = request
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .map(|index| index + 4)
            else {
                continue;
            };
            let header = std::str::from_utf8(&request[..header_end])
                .map_err(|_| ControlError::InvalidHttp)?;
            let content_length = content_length(header)?;
            let request_length = header_end.saturating_add(content_length);
            if request_length > MAX_REQUEST_BYTES {
                return Err(ControlError::ResponseTooLarge);
            }
            if request.len() >= request_length {
                request.truncate(request_length);
                return Ok(request);
            }
        }
    }

    fn content_length(header: &str) -> Result<usize, ControlError> {
        let mut length = None;
        for line in header.lines().skip(1) {
            if line.is_empty() {
                continue;
            }
            let (name, value) = line.split_once(':').ok_or(ControlError::InvalidHttp)?;
            if name.eq_ignore_ascii_case("content-length") {
                let parsed = value
                    .trim()
                    .parse::<usize>()
                    .map_err(|_| ControlError::InvalidHttp)?;
                if length.replace(parsed).is_some() {
                    return Err(ControlError::InvalidHttp);
                }
            }
        }
        length.ok_or(ControlError::InvalidHttp)
    }

    fn write_json_response(
        stream: &mut Stream,
        status: u16,
        body: &[u8],
    ) -> Result<(), ControlError> {
        let status_text = match status {
            200 => "OK",
            400 => "Bad Request",
            401 => "Unauthorized",
            410 => "Gone",
            413 => "Content Too Large",
            503 => "Service Unavailable",
            404 => "Not Found",
            504 => "Gateway Timeout",
            _ => "Internal Server Error",
        };
        let header = format!(
            "HTTP/1.1 {status} {status_text}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            body.len()
        );
        let deadline = deadline_after(CONTROL_SERVER_IO_TIMEOUT)?;
        write_all_until(stream, header.as_bytes(), deadline)?;
        write_all_until(stream, body, deadline)
    }

    fn write_update_event_stream(
        stream: &mut Stream,
        events: Receiver<ControlUpdateEvent>,
    ) -> Result<(), ControlError> {
        write_all_until(
            stream,
            b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream; charset=utf-8\r\ncache-control: no-cache\r\nconnection: close\r\n\r\n",
            deadline_after(CONTROL_SERVER_IO_TIMEOUT)?,
        )?;
        loop {
            match events.recv_timeout(Duration::from_secs(15)) {
                Ok(event) => write_sse_event(stream, &event)?,
                Err(RecvTimeoutError::Timeout) => write_all_until(
                    stream,
                    b": heartbeat\n\n",
                    deadline_after(CONTROL_SERVER_IO_TIMEOUT)?,
                )?,
                Err(RecvTimeoutError::Disconnected) => return Ok(()),
            }
        }
    }

    fn write_sse_event(
        stream: &mut Stream,
        event: &ControlUpdateEvent,
    ) -> Result<(), ControlError> {
        if !is_safe_sse_token(&event.id)
            || !is_safe_sse_token(&event.event)
            || event.data.len() > MAX_CONTROL_SSE_EVENT_BYTES
            || event.data.iter().any(|byte| matches!(*byte, b'\r' | b'\n'))
        {
            return Err(ControlError::InvalidHttp);
        }
        let deadline = deadline_after(CONTROL_SERVER_IO_TIMEOUT)?;
        write_all_until(
            stream,
            format!("id: {}\nevent: {}\ndata: ", event.id, event.event).as_bytes(),
            deadline,
        )?;
        write_all_until(stream, &event.data, deadline)?;
        write_all_until(stream, b"\n\n", deadline)
    }

    fn is_safe_sse_token(value: &str) -> bool {
        !value.is_empty()
            && value.len() <= 128
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
    }

    pub struct ControlClient {
        endpoint: String,
        timeout: Duration,
    }

    /// Blocking reader for the Agent-owned update event stream.
    pub struct ControlUpdateEventStream {
        stream: Stream,
        buffer: Vec<u8>,
        timeout: Duration,
    }

    impl ControlClient {
        pub fn new(endpoint: ControlEndpoint) -> Result<Self, ControlError> {
            Self::new_with_timeout(endpoint, Duration::from_secs(30))
        }

        pub fn new_with_timeout(
            endpoint: ControlEndpoint,
            timeout: Duration,
        ) -> Result<Self, ControlError> {
            let ControlEndpoint::NamedPipe(endpoint) = endpoint else {
                return Err(ControlError::UnsupportedEndpoint);
            };
            if timeout.is_zero() {
                return Err(ControlError::Timeout);
            }
            pipe_name(&endpoint)?;
            Ok(Self { endpoint, timeout })
        }

        pub fn status(&self) -> Result<ControlStatus, ControlError> {
            self.request("GET", "/api/v1/control/status")
        }

        pub fn start(&self) -> Result<ControlStatus, ControlError> {
            self.request("POST", "/api/v1/control/start")
        }

        pub fn stop(&self) -> Result<ControlStatus, ControlError> {
            self.request("POST", "/api/v1/control/stop")
        }

        pub fn restart(&self) -> Result<ControlStatus, ControlError> {
            self.request("POST", "/api/v1/control/restart")
        }

        pub fn shutdown_agent(&self) -> Result<ControlStatus, ControlError> {
            self.request("POST", "/api/v1/control/agent/shutdown")
        }

        pub fn enable_management_web(&self) -> Result<ControlStatus, ControlError> {
            self.request("POST", "/api/v1/control/web/enable")
        }

        pub fn disable_management_web(&self) -> Result<ControlStatus, ControlError> {
            self.request("POST", "/api/v1/control/web/disable")
        }

        pub fn update_status(&self) -> Result<super::UpdateControlStatus, ControlError> {
            self.request_update("GET", "/api/v1/control/updates")
        }

        pub fn diagnostics_bundle(&self) -> Result<super::DiagnosticsControlBundle, ControlError> {
            self.request_json("GET", "/api/v1/control/diagnostics/bundle", "")
        }

        pub fn gateway_projection(
            &self,
            projection: super::GatewayProjection,
        ) -> Result<serde_json::Value, ControlError> {
            self.request_json(projection.method(), projection.control_path(), "")
        }

        pub fn store_secret(
            &self,
            kind: super::ControlSecretKind,
            value: &str,
        ) -> Result<super::ControlSecretReceipt, ControlError> {
            self.request_json(
                "PUT",
                &format!("/api/v1/control/secrets/{}", kind.path_segment()),
                value,
            )
        }

        pub fn remove_secret(
            &self,
            kind: super::ControlSecretKind,
        ) -> Result<super::ControlSecretReceipt, ControlError> {
            self.request_json(
                "DELETE",
                &format!("/api/v1/control/secrets/{}", kind.path_segment()),
                "",
            )
        }

        pub fn subscribe_update_events(&self) -> Result<ControlUpdateEventStream, ControlError> {
            self.subscribe_event_stream("/api/v1/control/updates/events")
        }

        pub fn subscribe_gateway_events(&self) -> Result<ControlUpdateEventStream, ControlError> {
            self.subscribe_event_stream("/api/v1/control/events")
        }

        fn subscribe_event_stream(
            &self,
            path: &str,
        ) -> Result<ControlUpdateEventStream, ControlError> {
            let mut stream = self.connect()?;
            let request = format!(
                "GET {path} HTTP/1.1\r\nhost: localhost\r\naccept: text/event-stream\r\ncontent-length: 0\r\n\r\n"
            );
            write_all_until(
                &mut stream,
                request.as_bytes(),
                deadline_after(self.timeout)?,
            )?;
            let response = read_sse_response_head(&mut stream, self.timeout)?;
            let separator = response
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .ok_or(ControlError::InvalidHttp)?;
            let (head, body) = response.split_at(separator + 4);
            if !head.starts_with(b"HTTP/1.1 200")
                || !String::from_utf8_lossy(head)
                    .to_ascii_lowercase()
                    .contains("content-type: text/event-stream")
            {
                return Err(error_from_http_response(head, body));
            }
            Ok(ControlUpdateEventStream {
                stream,
                buffer: body.to_vec(),
                timeout: self.timeout,
            })
        }

        fn connect(&self) -> Result<Stream, ControlError> {
            let stream = Stream::connect(pipe_name(&self.endpoint)?).map_err(map_io_error)?;
            stream.set_nonblocking(true).map_err(map_io_error)?;
            Ok(stream)
        }

        fn request(&self, method: &str, path: &str) -> Result<ControlStatus, ControlError> {
            self.request_json(method, path, "")
        }

        fn request_update(
            &self,
            method: &str,
            path: &str,
        ) -> Result<super::UpdateControlStatus, ControlError> {
            self.request_json(method, path, "")
        }

        fn request_json<T: serde::de::DeserializeOwned>(
            &self,
            method: &str,
            path: &str,
            body: &str,
        ) -> Result<T, ControlError> {
            let mut stream = self.connect()?;
            let mut request = format!(
                "{method} {path} HTTP/1.1\r\nhost: localhost\r\ncontent-length: {}\r\n\r\n{body}",
                body.len()
            );
            let write_result = write_all_until(
                &mut stream,
                request.as_bytes(),
                deadline_after(self.timeout)?,
            );
            request.zeroize();
            write_result?;
            let response = read_http_response(&mut stream, deadline_after(self.timeout)?)?;
            let separator = response
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .ok_or(ControlError::InvalidHttp)?;
            let (head, body) = response.split_at(separator + 4);
            if !head.starts_with(b"HTTP/1.1 200") {
                return Err(error_from_http_response(head, body));
            }
            if body.len() > MAX_CONTROL_RESPONSE_BYTES {
                return Err(ControlError::ResponseTooLarge);
            }
            serde_json::from_slice(body).map_err(|_| ControlError::InvalidHttp)
        }
    }

    impl ControlUpdateEventStream {
        /// Reads one update state transition. `None` means the stream closed cleanly.
        pub fn next_event(&mut self) -> Result<Option<ControlUpdateEvent>, ControlError> {
            loop {
                if let Some((index, boundary_length)) = sse_boundary(&self.buffer) {
                    let block = self.buffer[..index].to_vec();
                    self.buffer.drain(..index + boundary_length);
                    if let Some(event) = parse_sse_event(&block)? {
                        return Ok(Some(event));
                    }
                    continue;
                }
                if self.buffer.len() > 64 * 1024 {
                    return Err(ControlError::ResponseTooLarge);
                }
                let mut chunk = [0_u8; 4096];
                let count =
                    read_until(&mut self.stream, &mut chunk, deadline_after(self.timeout)?)?;
                if count == 0 {
                    return if self.buffer.is_empty() {
                        Ok(None)
                    } else {
                        Err(ControlError::InvalidHttp)
                    };
                }
                self.buffer.extend_from_slice(&chunk[..count]);
            }
        }
    }

    fn pipe_name(value: &str) -> Result<interprocess::local_socket::Name<'_>, ControlError> {
        value
            .to_fs_name::<NamedPipe>()
            .map_err(|_| ControlError::UnsupportedEndpoint)
    }

    fn read_sse_response_head(
        stream: &mut Stream,
        timeout: Duration,
    ) -> Result<Vec<u8>, ControlError> {
        let mut response = Vec::new();
        let mut chunk = [0_u8; 4096];
        let deadline = deadline_after(timeout)?;
        loop {
            let count = read_until(stream, &mut chunk, deadline)?;
            if count == 0 {
                return Err(ControlError::InvalidHttp);
            }
            response.extend_from_slice(&chunk[..count]);
            if let Some(header_end) = response
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .map(|index| index + 4)
            {
                if header_end > 8 * 1024 {
                    return Err(ControlError::ResponseTooLarge);
                }
                return Ok(response);
            }
            if response.len() > 8 * 1024 {
                return Err(ControlError::ResponseTooLarge);
            }
        }
    }

    fn read_until(
        stream: &mut Stream,
        buffer: &mut [u8],
        deadline: Instant,
    ) -> Result<usize, ControlError> {
        loop {
            match stream.read(buffer) {
                Ok(0) => {
                    if Instant::now() >= deadline {
                        return Err(ControlError::Timeout);
                    }
                    // PIPE_NOWAIT exposes both temporary no-data and peer closure as zero-byte
                    // reads through interprocess, so only protocol progress and the deadline are
                    // reliable at this layer.
                    thread::sleep(Duration::from_millis(5));
                }
                Ok(count) => return Ok(count),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if Instant::now() >= deadline {
                        return Err(ControlError::Timeout);
                    }
                    thread::sleep(Duration::from_millis(5));
                }
                Err(error) => return Err(map_io_error(error)),
            }
        }
    }

    fn write_all_until(
        stream: &mut Stream,
        mut bytes: &[u8],
        deadline: Instant,
    ) -> Result<(), ControlError> {
        while !bytes.is_empty() {
            let chunk_length = bytes.len().min(CONTROL_PIPE_WRITE_CHUNK_BYTES);
            match stream.write(&bytes[..chunk_length]) {
                Ok(0) => {
                    if Instant::now() >= deadline {
                        return Err(ControlError::Timeout);
                    }
                    thread::sleep(Duration::from_millis(5));
                }
                Ok(count) => bytes = &bytes[count..],
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if Instant::now() >= deadline {
                        return Err(ControlError::Timeout);
                    }
                    thread::sleep(Duration::from_millis(5));
                }
                Err(error) => return Err(map_io_error(error)),
            }
        }
        Ok(())
    }

    fn read_http_response(stream: &mut Stream, deadline: Instant) -> Result<Vec<u8>, ControlError> {
        const MAX_RESPONSE_HEADER_BYTES: usize = 8 * 1024;
        let mut response = Vec::new();
        let mut chunk = [0_u8; 4096];
        loop {
            let count = read_until(stream, &mut chunk, deadline)?;
            if count == 0 {
                return Err(ControlError::InvalidHttp);
            }
            response.extend_from_slice(&chunk[..count]);
            if response.len() > MAX_CONTROL_RESPONSE_WIRE_BYTES {
                return Err(ControlError::ResponseTooLarge);
            }
            let Some(header_end) = response
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .map(|index| index + 4)
            else {
                if response.len() > MAX_RESPONSE_HEADER_BYTES {
                    return Err(ControlError::ResponseTooLarge);
                }
                continue;
            };
            if header_end > MAX_RESPONSE_HEADER_BYTES {
                return Err(ControlError::ResponseTooLarge);
            }
            let header = std::str::from_utf8(&response[..header_end])
                .map_err(|_| ControlError::InvalidHttp)?;
            let response_length = header_end
                .checked_add(content_length(header)?)
                .ok_or(ControlError::ResponseTooLarge)?;
            if response_length > MAX_CONTROL_RESPONSE_WIRE_BYTES {
                return Err(ControlError::ResponseTooLarge);
            }
            while response.len() < response_length {
                let count = read_until(stream, &mut chunk, deadline)?;
                if count == 0 {
                    return Err(ControlError::InvalidHttp);
                }
                let remaining = response_length - response.len();
                response.extend_from_slice(&chunk[..count.min(remaining)]);
            }
            response.truncate(response_length);
            return Ok(response);
        }
    }

    fn deadline_after(timeout: Duration) -> Result<Instant, ControlError> {
        Instant::now()
            .checked_add(timeout)
            .ok_or(ControlError::Timeout)
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

    fn sse_boundary(buffer: &[u8]) -> Option<(usize, usize)> {
        buffer
            .windows(2)
            .position(|window| window == b"\n\n")
            .map(|index| (index, 2))
            .or_else(|| {
                buffer
                    .windows(4)
                    .position(|window| window == b"\r\n\r\n")
                    .map(|index| (index, 4))
            })
    }

    fn parse_sse_event(block: &[u8]) -> Result<Option<ControlUpdateEvent>, ControlError> {
        let block = std::str::from_utf8(block).map_err(|_| ControlError::InvalidHttp)?;
        let mut id = None;
        let mut event = None;
        let mut data = None;
        for line in block.lines() {
            if line.is_empty() || line.starts_with(':') {
                continue;
            }
            let (field, value) = line.split_once(':').ok_or(ControlError::InvalidHttp)?;
            let value = value.strip_prefix(' ').unwrap_or(value);
            match field {
                "id" => id = Some(value.to_owned()),
                "event" => event = Some(value.to_owned()),
                "data" => data = Some(value.as_bytes().to_vec()),
                _ => return Err(ControlError::InvalidHttp),
            }
        }
        match (id, event, data) {
            (None, None, None) => Ok(None),
            (Some(id), Some(event), Some(data))
                if is_safe_sse_token(&id)
                    && is_safe_sse_token(&event)
                    && !data.contains(&b'\n') =>
            {
                Ok(Some(ControlUpdateEvent { id, event, data }))
            }
            _ => Err(ControlError::InvalidHttp),
        }
    }

    #[cfg(test)]
    mod tests {
        use super::{ControlClient, ControlServer};
        use crate::{
            ControlEndpoint, ControlError, ControlStatus, GatewayControlStatus, InternalComponent,
            ManagementWebControlStatus, StaticControlHandler, compiled_component_identity,
        };
        use interprocess::local_socket::{Listener, ListenerOptions, Stream, prelude::*};
        use std::{
            io::{Read, Write},
            sync::Arc,
            time::Duration,
        };

        fn status() -> ControlStatus {
            ControlStatus {
                schema_version: 3,
                agent: String::from("running"),
                identity: compiled_component_identity(InternalComponent::Agent).unwrap(),
                gateway: GatewayControlStatus::Running,
                management_web: ManagementWebControlStatus::Disabled,
                management_web_url: None,
                uptime_seconds: 1,
                latest_error_code: None,
            }
        }

        fn endpoint(label: &str) -> ControlEndpoint {
            ControlEndpoint::named_pipe(format!(
                r"\\.\pipe\cmclient-control-{label}-{}",
                uuid::Uuid::new_v4().simple()
            ))
        }

        fn listener(endpoint: &ControlEndpoint) -> Listener {
            let ControlEndpoint::NamedPipe(path) = endpoint else {
                panic!("test endpoint must be a named pipe");
            };
            ListenerOptions::new()
                .name(super::pipe_name(path).expect("pipe name should be valid"))
                .create_sync()
                .expect("named pipe should bind")
        }

        fn read_request(stream: &mut Stream) {
            let mut request = Vec::new();
            let mut chunk = [0_u8; 2_048];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let count = stream.read(&mut chunk).expect("request should read");
                assert_ne!(count, 0, "client should send a complete request");
                request.extend_from_slice(&chunk[..count]);
            }
        }

        fn write_status(stream: &mut Stream, value: &ControlStatus) {
            let body = serde_json::to_vec(value).expect("status should serialize");
            let header = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len()
            );
            stream
                .write_all(header.as_bytes())
                .expect("response header should write");
            stream.write_all(&body).expect("response body should write");
        }

        #[test]
        fn serves_status_over_a_private_named_pipe() {
            let endpoint = endpoint("roundtrip");
            let server = ControlServer::bind(
                endpoint.clone(),
                Arc::new(StaticControlHandler::new(status())),
            )
            .expect("named pipe should bind");
            let server_thread = std::thread::spawn(move || server.serve_once());
            let client = ControlClient::new(endpoint).expect("client should initialize");
            assert_eq!(client.status().expect("status should load"), status());
            server_thread
                .join()
                .expect("server thread should join")
                .expect("server should serve request");
        }

        #[test]
        fn transfers_more_than_one_named_pipe_buffer() {
            let endpoint = endpoint("multi-buffer");
            let listener = listener(&endpoint);
            let expected = vec![b'x'; 2 * 1024];
            let server_payload = expected.clone();
            let server_thread = std::thread::spawn(move || {
                let mut stream = listener.accept().expect("client should connect");
                stream
                    .set_nonblocking(true)
                    .expect("server stream should become nonblocking");
                super::write_all_until(
                    &mut stream,
                    &server_payload,
                    super::deadline_after(Duration::from_secs(2)).unwrap(),
                )
            });

            let ControlEndpoint::NamedPipe(path) = &endpoint else {
                panic!("test endpoint must be a named pipe");
            };
            let mut stream =
                Stream::connect(super::pipe_name(path).unwrap()).expect("client should connect");
            stream
                .set_nonblocking(true)
                .expect("client stream should become nonblocking");
            let deadline = super::deadline_after(Duration::from_secs(2)).unwrap();
            let mut received = Vec::new();
            let mut chunk = [0_u8; 256];
            while received.len() < expected.len() {
                let count = super::read_until(&mut stream, &mut chunk, deadline)
                    .expect("payload should arrive before the deadline");
                received.extend_from_slice(&chunk[..count]);
            }

            assert_eq!(received, expected);
            server_thread
                .join()
                .expect("server thread should join")
                .expect("server should write the complete payload");
        }

        #[test]
        fn waits_for_a_delayed_named_pipe_response_without_treating_zero_as_eof() {
            let endpoint = endpoint("delayed");
            let listener = listener(&endpoint);
            let expected = status();
            let response = expected.clone();
            let server_thread = std::thread::spawn(move || {
                let mut stream = listener.accept().expect("client should connect");
                read_request(&mut stream);
                std::thread::sleep(Duration::from_millis(75));
                write_status(&mut stream, &response);
            });
            let client = ControlClient::new_with_timeout(endpoint, Duration::from_secs(2))
                .expect("client should initialize");
            assert_eq!(
                client.status().expect("delayed status should load"),
                expected
            );
            server_thread.join().expect("server thread should join");
        }

        #[test]
        fn times_out_while_a_named_pipe_peer_remains_connected_without_a_response() {
            let endpoint = endpoint("timeout");
            let listener = listener(&endpoint);
            let server_thread = std::thread::spawn(move || {
                let mut stream = listener.accept().expect("client should connect");
                read_request(&mut stream);
                std::thread::sleep(Duration::from_millis(250));
            });
            let client = ControlClient::new_with_timeout(endpoint, Duration::from_millis(50))
                .expect("client should initialize");
            assert_eq!(client.status(), Err(ControlError::Timeout));
            server_thread.join().expect("server thread should join");
        }

        #[test]
        fn times_out_when_a_named_pipe_peer_closes_without_a_response() {
            let endpoint = endpoint("closed");
            let listener = listener(&endpoint);
            let server_thread = std::thread::spawn(move || {
                let mut stream = listener.accept().expect("client should connect");
                read_request(&mut stream);
            });
            let client = ControlClient::new_with_timeout(endpoint, Duration::from_millis(50))
                .expect("client should initialize");
            assert_eq!(client.status(), Err(ControlError::Timeout));
            server_thread.join().expect("server thread should join");
        }

        #[test]
        fn times_out_for_a_truncated_named_pipe_http_response() {
            let endpoint = endpoint("truncated");
            let listener = listener(&endpoint);
            let server_thread = std::thread::spawn(move || {
                let mut stream = listener.accept().expect("client should connect");
                read_request(&mut stream);
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 4\r\n\r\n{")
                    .expect("partial response should write");
            });
            let client = ControlClient::new_with_timeout(endpoint, Duration::from_millis(50))
                .expect("client should initialize");
            assert_eq!(client.status(), Err(ControlError::Timeout));
            server_thread.join().expect("server thread should join");
        }

        #[test]
        fn times_out_for_a_truncated_named_pipe_sse_response_head() {
            let endpoint = endpoint("truncated-sse-head");
            let listener = listener(&endpoint);
            let server_thread = std::thread::spawn(move || {
                let mut stream = listener.accept().expect("client should connect");
                read_request(&mut stream);
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\n")
                    .expect("partial response head should write");
            });
            let client = ControlClient::new_with_timeout(endpoint, Duration::from_millis(50))
                .expect("client should initialize");
            assert!(matches!(
                client.subscribe_update_events(),
                Err(ControlError::Timeout)
            ));
            server_thread.join().expect("server thread should join");
        }

        #[test]
        fn times_out_for_a_truncated_named_pipe_sse_event() {
            let endpoint = endpoint("truncated-sse-event");
            let listener = listener(&endpoint);
            let server_thread = std::thread::spawn(move || {
                let mut stream = listener.accept().expect("client should connect");
                read_request(&mut stream);
                stream
                    .write_all(
                        b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\nid: partial\n",
                    )
                    .expect("partial event should write");
            });
            let client = ControlClient::new_with_timeout(endpoint, Duration::from_millis(50))
                .expect("client should initialize");
            let mut events = client
                .subscribe_update_events()
                .expect("response head should load");
            assert_eq!(events.next_event(), Err(ControlError::Timeout));
            server_thread.join().expect("server thread should join");
        }
    }
}

#[cfg(windows)]
pub use windows::{ControlClient, ControlServer, ControlUpdateEventStream};

#[cfg(all(not(unix), not(windows)))]
pub struct ControlServer;

#[cfg(all(not(unix), not(windows)))]
impl ControlServer {
    pub fn bind(
        _endpoint: ControlEndpoint,
        _handler: std::sync::Arc<dyn ControlHandler>,
    ) -> Result<Self, ControlError> {
        Err(ControlError::UnsupportedEndpoint)
    }

    pub fn serve_once(&self) -> Result<(), ControlError> {
        Err(ControlError::UnsupportedEndpoint)
    }
}

#[cfg(all(not(unix), not(windows)))]
pub struct ControlClient;

#[cfg(all(not(unix), not(windows)))]
pub struct ControlUpdateEventStream;

#[cfg(all(not(unix), not(windows)))]
impl ControlUpdateEventStream {
    pub fn next_event(&mut self) -> Result<Option<ControlUpdateEvent>, ControlError> {
        Err(ControlError::UnsupportedEndpoint)
    }
}

#[cfg(all(not(unix), not(windows)))]
impl ControlClient {
    pub fn new(_endpoint: ControlEndpoint) -> Result<Self, ControlError> {
        Err(ControlError::UnsupportedEndpoint)
    }

    pub fn new_with_timeout(
        _endpoint: ControlEndpoint,
        _timeout: std::time::Duration,
    ) -> Result<Self, ControlError> {
        Err(ControlError::UnsupportedEndpoint)
    }

    pub fn status(&self) -> Result<ControlStatus, ControlError> {
        Err(ControlError::UnsupportedEndpoint)
    }

    pub fn start(&self) -> Result<ControlStatus, ControlError> {
        Err(ControlError::UnsupportedEndpoint)
    }

    pub fn stop(&self) -> Result<ControlStatus, ControlError> {
        Err(ControlError::UnsupportedEndpoint)
    }

    pub fn restart(&self) -> Result<ControlStatus, ControlError> {
        Err(ControlError::UnsupportedEndpoint)
    }

    pub fn enable_management_web(&self) -> Result<ControlStatus, ControlError> {
        Err(ControlError::UnsupportedEndpoint)
    }

    pub fn disable_management_web(&self) -> Result<ControlStatus, ControlError> {
        Err(ControlError::UnsupportedEndpoint)
    }

    pub fn update_status(&self) -> Result<UpdateControlStatus, ControlError> {
        Err(ControlError::UnsupportedEndpoint)
    }

    pub fn diagnostics_bundle(&self) -> Result<DiagnosticsControlBundle, ControlError> {
        Err(ControlError::UnsupportedEndpoint)
    }

    pub fn gateway_projection(
        &self,
        _projection: GatewayProjection,
    ) -> Result<serde_json::Value, ControlError> {
        Err(ControlError::UnsupportedEndpoint)
    }

    pub fn store_secret(
        &self,
        _kind: ControlSecretKind,
        _value: &str,
    ) -> Result<ControlSecretReceipt, ControlError> {
        Err(ControlError::UnsupportedEndpoint)
    }

    pub fn remove_secret(
        &self,
        _kind: ControlSecretKind,
    ) -> Result<ControlSecretReceipt, ControlError> {
        Err(ControlError::UnsupportedEndpoint)
    }

    pub fn subscribe_update_events(&self) -> Result<ControlUpdateEventStream, ControlError> {
        Err(ControlError::UnsupportedEndpoint)
    }

    pub fn subscribe_gateway_events(&self) -> Result<ControlUpdateEventStream, ControlError> {
        Err(ControlError::UnsupportedEndpoint)
    }
}

pub fn is_local_endpoint(endpoint: &ControlEndpoint) -> bool {
    match endpoint {
        ControlEndpoint::UnixSocket(path) => path.is_absolute(),
        ControlEndpoint::NamedPipe(name) => name.starts_with(r"\\.\pipe\"),
    }
}

pub fn default_unix_socket(data_dir: &Path) -> ControlEndpoint {
    ControlEndpoint::UnixSocket(data_dir.join("control.sock"))
}

/// Default private Agent control endpoint for the current platform.
pub fn default_local_endpoint(data_dir: &Path) -> ControlEndpoint {
    #[cfg(windows)]
    {
        let _ = data_dir;
        ControlEndpoint::NamedPipe(String::from(r"\\.\pipe\cmclient-control"))
    }
    #[cfg(not(windows))]
    {
        default_unix_socket(data_dir)
    }
}

#[cfg(test)]
mod tests {
    #[cfg(any(unix, windows))]
    use super::ControlEndpoint;
    #[cfg(unix)]
    use super::default_unix_socket;
    #[cfg(any(unix, windows))]
    use super::{ControlClient, ControlServer};
    use super::{
        ControlError, ControlHandler, ControlSecretKind, ControlStatus, GatewayControlStatus,
        InternalComponent, ManagementWebControlStatus, RemoteControlAuthError,
        RemoteControlReplayGuard, StaticControlHandler, compiled_component_identity,
        is_local_endpoint, sign_remote_control_request,
    };
    #[cfg(unix)]
    use super::{
        ControlUpdateEvent, DiagnosticsControlBundle, GatewayProjection, UpdateControlJob,
        UpdateControlStatus,
    };
    use std::sync::Arc;
    use std::sync::Mutex;
    #[cfg(unix)]
    use std::{collections::BTreeMap, path::PathBuf, sync::mpsc};

    #[test]
    fn bounds_and_releases_control_connection_slots() {
        let limiter = super::ConnectionLimiter::new(2);
        let first = limiter.try_acquire().expect("first slot should acquire");
        let second = limiter.try_acquire().expect("second slot should acquire");

        assert!(limiter.try_acquire().is_none());
        assert_eq!(limiter.active(), 2);

        drop(first);
        let replacement = limiter
            .try_acquire()
            .expect("released slot should be reusable");
        assert_eq!(limiter.active(), 2);

        drop(second);
        drop(replacement);
        assert_eq!(limiter.active(), 0);
    }

    #[test]
    fn preserves_stable_secret_store_errors_across_http_control() {
        assert_eq!(
            super::error_from_http_response(
                b"HTTP/1.1 503 Service Unavailable\r\n\r\n",
                br#"{"code":"AGENT_SECRET_STORE_UNAVAILABLE"}"#,
            ),
            super::ControlError::SecretStoreUnavailable
        );
        assert_eq!(
            super::error_from_http_response(
                b"HTTP/1.1 400 Bad Request\r\n\r\n",
                br#"{"code":"AGENT_SECRET_VALUE_INVALID"}"#,
            ),
            super::ControlError::SecretValueInvalid
        );
    }
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

        let legacy = serde_json::json!({
            "schema_version": 2,
            "agent": "running",
            "agent_version": "2.0.0-rc.1",
            "gateway": "running",
            "management_web": "running",
            "management_web_url": null,
            "uptime_seconds": 1,
            "latest_error_code": null
        });
        assert!(serde_json::from_value::<ControlStatus>(legacy).is_err());
    }

    struct LegacyAprsSecretHandler {
        present: Mutex<bool>,
        store_attempts: Mutex<usize>,
    }

    impl ControlHandler for LegacyAprsSecretHandler {
        fn handle(&self, _command: super::ControlCommand) -> Result<ControlStatus, ControlError> {
            Ok(status())
        }

        fn store_secret(&self, _kind: ControlSecretKind, _value: &str) -> Result<(), ControlError> {
            *self
                .store_attempts
                .lock()
                .map_err(|_| ControlError::CommandFailed)? += 1;
            Ok(())
        }

        fn remove_secret(&self, kind: ControlSecretKind) -> Result<bool, ControlError> {
            assert_eq!(kind, ControlSecretKind::AprsPasscode);
            let mut present = self
                .present
                .lock()
                .map_err(|_| ControlError::CommandFailed)?;
            let was_present = *present;
            *present = false;
            Ok(was_present)
        }
    }

    #[test]
    fn legacy_aprs_passcode_can_only_be_removed_through_local_control() {
        let handler = Arc::new(LegacyAprsSecretHandler {
            present: Mutex::new(true),
            store_attempts: Mutex::new(0),
        });
        let router = super::ControlRouter::new(handler.clone());

        let put = router
            .route(
                "PUT /api/v1/control/secrets/aprs-passcode HTTP/1.1\r\nhost: localhost\r\ncontent-length: 8\r\n\r\nignored",
            )
            .expect("deprecated set should produce a response");
        assert!(matches!(
            put,
            super::ControlResponse::Json {
                status: 410,
                body
            } if body == br#"{"code":"CONTROL_SECRET_KIND_DEPRECATED"}"#
        ));
        assert_eq!(
            *handler
                .store_attempts
                .lock()
                .expect("store counter should lock"),
            0
        );

        let delete = router
            .route(
                "DELETE /api/v1/control/secrets/aprs-passcode HTTP/1.1\r\nhost: localhost\r\ncontent-length: 0\r\n\r\n",
            )
            .expect("deprecated remove should dispatch");
        assert!(matches!(
            delete,
            super::ControlResponse::Json {
                status: 200,
                body
            } if body == br#"{"stored":true}"#
        ));
        assert!(!*handler.present.lock().expect("secret state should lock"));
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn deprecated_aprs_passcode_round_trips_through_the_platform_control_transport() {
        let handler = Arc::new(LegacyAprsSecretHandler {
            present: Mutex::new(true),
            store_attempts: Mutex::new(0),
        });
        let (endpoint, cleanup_directory) = deprecated_secret_endpoint();
        let server = ControlServer::bind(endpoint.clone(), handler.clone())
            .expect("platform Control server should bind");
        let server_thread = std::thread::spawn(move || server.serve_once());
        let client =
            ControlClient::new(endpoint).expect("platform Control client should initialize");

        assert_eq!(
            client.store_secret(ControlSecretKind::AprsPasscode, "ignored"),
            Err(ControlError::SecretKindDeprecated)
        );
        server_thread
            .join()
            .expect("Control server thread should join")
            .expect("Control server should write the deprecated response");
        assert_eq!(
            *handler
                .store_attempts
                .lock()
                .expect("store counter should lock"),
            0
        );
        if let Some(directory) = cleanup_directory {
            std::fs::remove_dir_all(directory).expect("temporary directory should be removed");
        }
    }

    #[cfg(any(unix, windows))]
    fn deprecated_secret_endpoint() -> (ControlEndpoint, Option<std::path::PathBuf>) {
        #[cfg(unix)]
        {
            let directory = std::env::temp_dir().join(format!(
                "cmclient-control-deprecated-{}-{}",
                std::process::id(),
                uuid::Uuid::new_v4().simple()
            ));
            std::fs::create_dir_all(&directory).expect("temporary directory should exist");
            return (default_unix_socket(&directory), Some(directory));
        }
        #[cfg(windows)]
        {
            (
                ControlEndpoint::named_pipe(format!(
                    r"\\.\pipe\cmclient-control-deprecated-{}",
                    uuid::Uuid::new_v4().simple()
                )),
                None,
            )
        }
    }

    #[cfg(unix)]
    struct UpdateHandler {
        update: UpdateControlStatus,
        events: Mutex<Option<mpsc::Receiver<ControlUpdateEvent>>>,
    }

    #[cfg(unix)]
    struct DiagnosticsAndSecretHandler {
        values: Mutex<BTreeMap<&'static str, String>>,
    }

    #[cfg(unix)]
    struct GatewayProjectionHandler {
        events: Mutex<Option<mpsc::Receiver<ControlUpdateEvent>>>,
    }

    #[cfg(unix)]
    struct SlowHandler;

    #[cfg(unix)]
    struct FailClosedProjectionHandler;

    #[cfg(unix)]
    impl ControlHandler for DiagnosticsAndSecretHandler {
        fn handle(&self, _command: super::ControlCommand) -> Result<ControlStatus, ControlError> {
            Ok(status())
        }

        fn diagnostics_bundle(&self) -> Result<DiagnosticsControlBundle, ControlError> {
            Ok(DiagnosticsControlBundle {
                schema_version: 2,
                identity: compiled_component_identity(InternalComponent::Agent).unwrap(),
                gateway: GatewayControlStatus::Running,
                management_web: ManagementWebControlStatus::Running,
                latest_error_code: Some(String::from("GATEWAY_HEALTH_DEGRADED")),
                update_error_code: None,
                update_log_codes: vec![String::from("UPDATE_SIGNATURE_VERIFIED")],
            })
        }

        fn store_secret(&self, kind: ControlSecretKind, value: &str) -> Result<(), ControlError> {
            self.values
                .lock()
                .map_err(|_| ControlError::CommandFailed)?
                .insert(kind.path_segment(), value.to_owned());
            Ok(())
        }

        fn remove_secret(&self, kind: ControlSecretKind) -> Result<bool, ControlError> {
            Ok(self
                .values
                .lock()
                .map_err(|_| ControlError::CommandFailed)?
                .remove(kind.path_segment())
                .is_some())
        }
    }

    #[cfg(unix)]
    impl ControlHandler for UpdateHandler {
        fn handle(&self, _command: super::ControlCommand) -> Result<ControlStatus, ControlError> {
            Ok(status())
        }

        fn update_status(&self) -> Result<UpdateControlStatus, ControlError> {
            Ok(self.update.clone())
        }

        fn subscribe_update_events(
            &self,
        ) -> Result<mpsc::Receiver<ControlUpdateEvent>, ControlError> {
            self.events
                .lock()
                .map_err(|_| ControlError::CommandFailed)?
                .take()
                .ok_or(ControlError::CommandFailed)
        }
    }

    #[cfg(unix)]
    impl ControlHandler for GatewayProjectionHandler {
        fn handle(&self, _command: super::ControlCommand) -> Result<ControlStatus, ControlError> {
            Ok(status())
        }

        fn gateway_projection(
            &self,
            projection: GatewayProjection,
        ) -> Result<serde_json::Value, ControlError> {
            Ok(serde_json::json!({ "projection": format!("{projection:?}") }))
        }

        fn subscribe_gateway_events(
            &self,
        ) -> Result<mpsc::Receiver<ControlUpdateEvent>, ControlError> {
            self.events
                .lock()
                .map_err(|_| ControlError::CommandFailed)?
                .take()
                .ok_or(ControlError::CommandFailed)
        }
    }

    #[cfg(unix)]
    impl ControlHandler for SlowHandler {
        fn handle(&self, _command: super::ControlCommand) -> Result<ControlStatus, ControlError> {
            std::thread::sleep(std::time::Duration::from_millis(200));
            Ok(status())
        }
    }

    #[cfg(unix)]
    impl ControlHandler for FailClosedProjectionHandler {
        fn handle(&self, _command: super::ControlCommand) -> Result<ControlStatus, ControlError> {
            Ok(status())
        }

        fn gateway_projection(
            &self,
            projection: GatewayProjection,
        ) -> Result<serde_json::Value, ControlError> {
            match projection {
                GatewayProjection::Nodes => Err(ControlError::Timeout),
                GatewayProjection::Positions => Ok(serde_json::json!({
                    "items": ["x".repeat(super::MAX_CONTROL_RESPONSE_BYTES + 1)]
                })),
                _ => Err(ControlError::CommandFailed),
            }
        }
    }

    #[test]
    fn recognizes_local_endpoint_forms() {
        #[cfg(unix)]
        assert!(is_local_endpoint(&default_unix_socket(
            PathBuf::from("/tmp/cmclient").as_path()
        )));
        #[cfg(windows)]
        assert!(is_local_endpoint(&ControlEndpoint::named_pipe(
            r"\\.\pipe\cmclient-control"
        )));
    }

    #[test]
    fn signs_scoped_remote_requests_and_rejects_replay_or_expiry() {
        let token = "test-token-test-token-test-token-0";
        let auth =
            sign_remote_control_request(token, "POST", "/api/v1/control/restart", b"", 1_000)
                .expect("request should sign");
        let guard = RemoteControlReplayGuard::default();
        assert_eq!(
            guard.verify_and_record(token, "POST", "/api/v1/control/restart", b"", &auth, 1_001,),
            Ok(())
        );
        assert_eq!(
            guard.verify_and_record(token, "POST", "/api/v1/control/restart", b"", &auth, 1_001,),
            Err(RemoteControlAuthError::Replay)
        );

        let expired = sign_remote_control_request(token, "GET", "/api/v1/control/status", b"", 900)
            .expect("request should sign");
        assert_eq!(
            RemoteControlReplayGuard::default().verify_and_record(
                token,
                "GET",
                "/api/v1/control/status",
                b"",
                &expired,
                1_001,
            ),
            Err(RemoteControlAuthError::Expired)
        );
    }

    #[test]
    fn binds_remote_signatures_to_method_path_body_scope_and_token() {
        let auth = sign_remote_control_request(
            "test-token-test-token-test-token-0",
            "PUT",
            "/api/v1/control/secrets/management-admin-token",
            b"secret-value",
            1_000,
        )
        .expect("request should sign");
        let guard = RemoteControlReplayGuard::default();
        for (token, method, path, body) in [
            (
                "other-token-other-token-other-00",
                "PUT",
                "/api/v1/control/secrets/management-admin-token",
                b"secret-value".as_slice(),
            ),
            (
                "test-token-test-token-test-token-0",
                "POST",
                "/api/v1/control/secrets/management-admin-token",
                b"secret-value".as_slice(),
            ),
            (
                "test-token-test-token-test-token-0",
                "PUT",
                "/api/v1/control/secrets/callmesh-api-key",
                b"secret-value".as_slice(),
            ),
            (
                "test-token-test-token-test-token-0",
                "PUT",
                "/api/v1/control/secrets/management-admin-token",
                b"changed".as_slice(),
            ),
        ] {
            assert_eq!(
                guard.verify_and_record(token, method, path, body, &auth, 1_000),
                Err(RemoteControlAuthError::Invalid)
            );
        }
    }

    #[test]
    fn routes_zero_length_control_requests_before_dispatching_them() {
        let router = super::ControlRouter::new(Arc::new(StaticControlHandler::new(status())));
        assert!(matches!(
            router.route(
                "GET /api/v1/control/status HTTP/1.1\r\nhost: localhost\r\ncontent-length: 0\r\n\r\n"
            ),
            Ok(super::ControlResponse::Json { status: 200, .. })
        ));
    }

    #[cfg(unix)]
    #[test]
    fn serves_status_over_a_private_unix_socket() {
        let directory =
            std::env::temp_dir().join(format!("cmclient-control-{}", std::process::id()));
        std::fs::create_dir_all(&directory).expect("temporary directory should exist");
        let endpoint = default_unix_socket(&directory);
        let server = ControlServer::bind(
            endpoint.clone(),
            Arc::new(StaticControlHandler::new(status())),
        )
        .expect("server should bind");
        let server_thread = std::thread::spawn(move || {
            server.serve_once()?;
            server.serve_once()?;
            server.serve_once()?;
            server.serve_once()?;
            server.serve_once()
        });
        let client = ControlClient::new(endpoint).expect("client should initialize");
        assert_eq!(client.status().expect("status should load"), status());
        assert_eq!(client.start().expect("start should load"), status());
        assert_eq!(
            client.shutdown_agent().expect("Agent shutdown should load"),
            status()
        );
        assert_eq!(
            client
                .enable_management_web()
                .expect("web enable should load"),
            status()
        );
        assert_eq!(
            client
                .disable_management_web()
                .expect("web disable should load"),
            status()
        );
        server_thread
            .join()
            .expect("server thread should join")
            .expect("server should respond");
        std::fs::remove_dir_all(directory).expect("temporary directory should be removed");
    }

    #[cfg(unix)]
    #[test]
    fn serves_update_status_and_a_bounded_sse_event_over_the_private_socket() {
        let directory =
            std::env::temp_dir().join(format!("cmclient-control-update-{}", std::process::id()));
        std::fs::create_dir_all(&directory).expect("temporary directory should exist");
        let endpoint = default_unix_socket(&directory);
        let update = UpdateControlStatus {
            schema_version: 1,
            job: Some(UpdateControlJob {
                id: String::from("update-1"),
                phase: String::from("health_checking"),
                updated_at: String::from("2026-07-18T03:00:00.000Z"),
                error_code: None,
                bytes_downloaded: None,
                bytes_total: None,
                bytes_per_second: None,
                recent_log_codes: Vec::new(),
            }),
        };
        let (sender, receiver) = mpsc::sync_channel(1);
        sender
            .send(ControlUpdateEvent {
                id: String::from("update-1"),
                event: String::from("update.status_changed"),
                data: serde_json::to_vec(&update).expect("event should serialize"),
            })
            .expect("event should queue");
        drop(sender);
        let server = ControlServer::bind(
            endpoint.clone(),
            Arc::new(UpdateHandler {
                update: update.clone(),
                events: Mutex::new(Some(receiver)),
            }),
        )
        .expect("server should bind");
        let server_thread = std::thread::spawn(move || {
            server.serve_once()?;
            server.serve_once()
        });

        let client = ControlClient::new(endpoint.clone()).expect("client should initialize");
        assert_eq!(
            client.update_status().expect("update status should load"),
            update
        );

        let mut events = client
            .subscribe_update_events()
            .expect("SSE client should connect");
        let event = events
            .next_event()
            .expect("SSE event should be valid")
            .expect("SSE event should arrive");
        assert_eq!(event.id, "update-1");
        assert_eq!(event.event, "update.status_changed");
        assert_eq!(
            serde_json::from_slice::<UpdateControlStatus>(&event.data)
                .expect("event data should deserialize"),
            update
        );
        server_thread
            .join()
            .expect("server thread should join")
            .expect("server should accept requests");
        std::fs::remove_dir_all(directory).expect("temporary directory should be removed");
    }

    #[cfg(unix)]
    #[test]
    fn accepts_secret_bodies_but_exposes_only_sanitized_diagnostics() {
        let directory =
            std::env::temp_dir().join(format!("cmclient-control-secrets-{}", std::process::id()));
        std::fs::create_dir_all(&directory).expect("temporary directory should exist");
        let endpoint = default_unix_socket(&directory);
        let handler = Arc::new(DiagnosticsAndSecretHandler {
            values: Mutex::new(BTreeMap::new()),
        });
        let server =
            ControlServer::bind(endpoint.clone(), handler.clone()).expect("server should bind");
        let server_thread = std::thread::spawn(move || {
            server.serve_once()?;
            server.serve_once()?;
            server.serve_once()
        });
        let client = ControlClient::new(endpoint).expect("client should initialize");

        let stored = client
            .store_secret(ControlSecretKind::CallMeshApiKey, "credential-value")
            .expect("secret should store");
        assert_eq!(stored, super::ControlSecretReceipt { stored: true });
        let diagnostics = client
            .diagnostics_bundle()
            .expect("diagnostics should load");
        let serialized = serde_json::to_string(&diagnostics).expect("diagnostics should serialize");
        assert!(serialized.contains("UPDATE_SIGNATURE_VERIFIED"));
        assert!(!serialized.contains("credential-value"));
        assert_eq!(
            client
                .remove_secret(ControlSecretKind::CallMeshApiKey)
                .expect("secret should remove"),
            super::ControlSecretReceipt { stored: true }
        );
        assert!(
            handler
                .values
                .lock()
                .expect("test values should lock")
                .is_empty()
        );
        server_thread
            .join()
            .expect("server thread should join")
            .expect("server should respond");
        std::fs::remove_dir_all(directory).expect("temporary directory should be removed");
    }

    #[cfg(unix)]
    #[test]
    fn serves_gateway_projections_jobs_and_events_over_the_private_socket() {
        let directory = std::env::temp_dir().join(format!("cmc-gw-proj-{}", std::process::id()));
        std::fs::create_dir_all(&directory).expect("temporary directory should exist");
        let endpoint = default_unix_socket(&directory);
        let (sender, receiver) = mpsc::sync_channel(1);
        sender
            .send(ControlUpdateEvent {
                id: String::from("event-1"),
                event: String::from("mesh.position.accepted"),
                data: br#"{"schemaVersion":1,"nodeId":"node-1"}"#.to_vec(),
            })
            .expect("event should queue");
        drop(sender);
        let server = ControlServer::bind(
            endpoint.clone(),
            Arc::new(GatewayProjectionHandler {
                events: Mutex::new(Some(receiver)),
            }),
        )
        .expect("server should bind");
        let server_thread = std::thread::spawn(move || {
            server.serve_once()?;
            server.serve_once()?;
            server.serve_once()
        });
        let client = ControlClient::new(endpoint).expect("client should initialize");

        assert_eq!(
            client
                .gateway_projection(GatewayProjection::Nodes)
                .expect("nodes projection should load"),
            serde_json::json!({ "projection": "Nodes" })
        );
        assert_eq!(
            client
                .gateway_projection(GatewayProjection::Backup)
                .expect("backup job should submit"),
            serde_json::json!({ "projection": "Backup" })
        );
        let mut events = client
            .subscribe_gateway_events()
            .expect("gateway SSE should connect");
        let event = events
            .next_event()
            .expect("gateway event should be valid")
            .expect("gateway event should arrive");
        assert_eq!(event.id, "event-1");
        assert_eq!(event.event, "mesh.position.accepted");

        server_thread
            .join()
            .expect("server thread should join")
            .expect("server should respond");
        std::fs::remove_dir_all(directory).expect("temporary directory should be removed");
    }

    #[cfg(unix)]
    #[test]
    fn maps_control_response_deadlines_to_the_stable_timeout_error() {
        let directory =
            std::env::temp_dir().join(format!("cmclient-control-timeout-{}", std::process::id()));
        std::fs::create_dir_all(&directory).expect("temporary directory should exist");
        let endpoint = default_unix_socket(&directory);
        let server = ControlServer::bind(endpoint.clone(), Arc::new(SlowHandler))
            .expect("server should bind");
        let server_thread = std::thread::spawn(move || server.serve_once());
        let client =
            ControlClient::new_with_timeout(endpoint, std::time::Duration::from_millis(25))
                .expect("client should initialize");

        assert_eq!(client.status(), Err(ControlError::Timeout));

        server_thread
            .join()
            .expect("server thread should join")
            .expect("server should accept request");
        std::thread::sleep(std::time::Duration::from_millis(225));
        std::fs::remove_dir_all(directory).expect("temporary directory should be removed");
    }

    #[cfg(unix)]
    #[test]
    fn preserves_handler_timeouts_and_bounds_local_json_responses() {
        let directory =
            std::env::temp_dir().join(format!("cmclient-control-bounds-{}", std::process::id()));
        std::fs::create_dir_all(&directory).expect("temporary directory should exist");
        let endpoint = default_unix_socket(&directory);
        let server = ControlServer::bind(endpoint.clone(), Arc::new(FailClosedProjectionHandler))
            .expect("server should bind");
        let server_thread = std::thread::spawn(move || {
            server.serve_once()?;
            server.serve_once()
        });
        let client = ControlClient::new(endpoint).expect("client should initialize");

        assert_eq!(
            client.gateway_projection(GatewayProjection::Nodes),
            Err(ControlError::Timeout)
        );
        assert_eq!(
            client.gateway_projection(GatewayProjection::Positions),
            Err(ControlError::ResponseTooLarge)
        );

        server_thread
            .join()
            .expect("server thread should join")
            .expect("server should accept requests");
        std::fs::remove_dir_all(directory).expect("temporary directory should be removed");
    }
}
