//! Local Agent control API over Unix sockets or Windows named pipes.

use serde::{Deserialize, Serialize};
use std::{
    fmt::{Display, Formatter},
    path::{Path, PathBuf},
    sync::{Arc, mpsc},
};

/// Stable workspace identity for the control API boundary.
pub const COMPONENT: &str = "control-api";

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
pub struct ControlStatus {
    pub schema_version: u8,
    pub agent: String,
    pub agent_version: String,
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
    pub agent_version: String,
    pub gateway: GatewayControlStatus,
    pub management_web: ManagementWebControlStatus,
    pub latest_error_code: Option<String>,
    pub update_error_code: Option<String>,
    pub update_log_codes: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
pub enum ControlCommand {
    Status,
    Start,
    Stop,
    Restart,
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
    CommandFailed,
}

impl ControlError {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::EndpointAlreadyInUse => "CONTROL_ENDPOINT_ALREADY_IN_USE",
            Self::UnsupportedEndpoint => "CONTROL_ENDPOINT_UNSUPPORTED",
            Self::Io => "CONTROL_IO_FAILED",
            Self::InvalidHttp => "CONTROL_HTTP_INVALID",
            Self::ResponseTooLarge => "CONTROL_RESPONSE_TOO_LARGE",
            Self::CommandFailed => "CONTROL_COMMAND_FAILED",
        }
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
            ["PUT", path, "HTTP/1.1"] | ["PUT", path, "HTTP/1.0"]
                if path.starts_with("/api/v1/control/secrets/") =>
            {
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

fn json_response<T: Serialize>(value: T) -> Result<ControlResponse, ControlError> {
    serde_json::to_vec(&value)
        .map(|body| ControlResponse::Json { status: 200, body })
        .map_err(|_| ControlError::Io)
}

fn error_response(error: ControlError) -> Result<ControlResponse, ControlError> {
    serde_json::to_vec(&serde_json::json!({ "code": error.code() }))
        .map(|body| ControlResponse::Json { status: 500, body })
        .map_err(|_| ControlError::Io)
}

#[cfg(unix)]
mod unix {
    use super::{
        ControlEndpoint, ControlError, ControlHandler, ControlResponse, ControlRouter,
        ControlStatus, ControlUpdateEvent,
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

    pub struct ControlServer {
        endpoint: PathBuf,
        listener: UnixListener,
        router: ControlRouter,
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
            })
        }

        pub fn serve_once(&self) -> Result<(), ControlError> {
            let (stream, _) = self.listener.accept().map_err(|_| ControlError::Io)?;
            let router = self.router.clone();
            thread::spawn(move || {
                let _ = serve_connection(stream, router);
            });
            Ok(())
        }

        pub fn endpoint(&self) -> &std::path::Path {
            &self.endpoint
        }
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
            404 => "Not Found",
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
            || event.data.contains(&b'\n')
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
    }

    /// Blocking reader for the Agent-owned update event stream.
    pub struct ControlUpdateEventStream {
        stream: UnixStream,
        buffer: Vec<u8>,
    }

    impl ControlClient {
        pub fn new(endpoint: ControlEndpoint) -> Result<Self, ControlError> {
            let ControlEndpoint::UnixSocket(endpoint) = endpoint else {
                return Err(ControlError::UnsupportedEndpoint);
            };
            Ok(Self { endpoint })
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
            let mut stream = UnixStream::connect(&self.endpoint).map_err(|_| ControlError::Io)?;
            stream
                .write_all(
                    b"GET /api/v1/control/updates/events HTTP/1.1\r\nhost: localhost\r\naccept: text/event-stream\r\ncontent-length: 0\r\n\r\n",
                )
                .map_err(|_| ControlError::Io)?;
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
                return Err(ControlError::CommandFailed);
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
            let mut stream = UnixStream::connect(&self.endpoint).map_err(|_| ControlError::Io)?;
            let mut request = format!(
                "{method} {path} HTTP/1.1\r\nhost: localhost\r\ncontent-length: {}\r\n\r\n{body}",
                body.len()
            );
            let write_result = stream.write_all(request.as_bytes());
            request.zeroize();
            write_result.map_err(|_| ControlError::Io)?;
            let mut response = Vec::new();
            stream
                .read_to_end(&mut response)
                .map_err(|_| ControlError::Io)?;
            let separator = response
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .ok_or(ControlError::InvalidHttp)?;
            let (head, body) = response.split_at(separator + 4);
            if !head.starts_with(b"HTTP/1.1 200") {
                return Err(ControlError::CommandFailed);
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
                let count = self.stream.read(&mut chunk).map_err(|_| ControlError::Io)?;
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

    fn read_sse_response_head(stream: &mut UnixStream) -> Result<Vec<u8>, ControlError> {
        let mut response = Vec::new();
        let mut chunk = [0_u8; 4096];
        loop {
            let count = stream.read(&mut chunk).map_err(|_| ControlError::Io)?;
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
        ControlEndpoint, ControlError, ControlHandler, ControlResponse, ControlRouter,
        ControlStatus, ControlUpdateEvent,
    };
    use interprocess::{
        local_socket::{Listener, ListenerOptions, Stream, prelude::*},
        os::windows::local_socket::NamedPipe,
    };
    use std::{
        io::{Read, Write},
        sync::mpsc::{Receiver, RecvTimeoutError},
        thread,
        time::Duration,
    };
    use zeroize::Zeroize;

    const MAX_REQUEST_BYTES: usize = 8 * 1024;

    pub struct ControlServer {
        listener: Listener,
        router: ControlRouter,
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
            })
        }

        pub fn serve_once(&self) -> Result<(), ControlError> {
            let stream = self.listener.accept().map_err(|_| ControlError::Io)?;
            let router = self.router.clone();
            thread::spawn(move || {
                let _ = serve_connection(stream, router);
            });
            Ok(())
        }
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
        stream: &mut Stream,
        status: u16,
        body: &[u8],
    ) -> Result<(), ControlError> {
        let status_text = match status {
            200 => "OK",
            404 => "Not Found",
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
        stream: &mut Stream,
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
        stream: &mut Stream,
        event: &ControlUpdateEvent,
    ) -> Result<(), ControlError> {
        if !is_safe_sse_token(&event.id)
            || !is_safe_sse_token(&event.event)
            || event.data.contains(&b'\n')
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

    pub struct ControlClient {
        endpoint: String,
    }

    /// Blocking reader for the Agent-owned update event stream.
    pub struct ControlUpdateEventStream {
        stream: Stream,
        buffer: Vec<u8>,
    }

    impl ControlClient {
        pub fn new(endpoint: ControlEndpoint) -> Result<Self, ControlError> {
            let ControlEndpoint::NamedPipe(endpoint) = endpoint else {
                return Err(ControlError::UnsupportedEndpoint);
            };
            pipe_name(&endpoint)?;
            Ok(Self { endpoint })
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
            let mut stream = self.connect()?;
            stream
                .write_all(
                    b"GET /api/v1/control/updates/events HTTP/1.1\r\nhost: localhost\r\naccept: text/event-stream\r\ncontent-length: 0\r\n\r\n",
                )
                .map_err(|_| ControlError::Io)?;
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
                return Err(ControlError::CommandFailed);
            }
            Ok(ControlUpdateEventStream {
                stream,
                buffer: body.to_vec(),
            })
        }

        fn connect(&self) -> Result<Stream, ControlError> {
            Stream::connect(pipe_name(&self.endpoint)?).map_err(|_| ControlError::Io)
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
            write_result.map_err(|_| ControlError::Io)?;
            let mut response = Vec::new();
            stream
                .read_to_end(&mut response)
                .map_err(|_| ControlError::Io)?;
            let separator = response
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .ok_or(ControlError::InvalidHttp)?;
            let (head, body) = response.split_at(separator + 4);
            if !head.starts_with(b"HTTP/1.1 200") {
                return Err(ControlError::CommandFailed);
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
                let count = self.stream.read(&mut chunk).map_err(|_| ControlError::Io)?;
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

    fn read_sse_response_head(stream: &mut Stream) -> Result<Vec<u8>, ControlError> {
        let mut response = Vec::new();
        let mut chunk = [0_u8; 4096];
        loop {
            let count = stream.read(&mut chunk).map_err(|_| ControlError::Io)?;
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

    #[cfg(test)]
    mod tests {
        use super::{ControlClient, ControlServer};
        use crate::{
            ControlEndpoint, ControlStatus, GatewayControlStatus, ManagementWebControlStatus,
            StaticControlHandler,
        };
        use std::sync::Arc;

        fn status() -> ControlStatus {
            ControlStatus {
                schema_version: 2,
                agent: String::from("running"),
                agent_version: String::from("2.0.0-dev.0"),
                gateway: GatewayControlStatus::Running,
                management_web: ManagementWebControlStatus::Disabled,
                management_web_url: None,
                uptime_seconds: 1,
                latest_error_code: None,
            }
        }

        #[test]
        fn serves_status_over_a_private_named_pipe() {
            let endpoint = ControlEndpoint::named_pipe(format!(
                r"\\.\pipe\cmclient-control-test-{}",
                std::process::id()
            ));
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
    #[cfg(unix)]
    use super::{
        ControlClient, ControlError, ControlHandler, ControlSecretKind, ControlServer,
        ControlUpdateEvent, DiagnosticsControlBundle, UpdateControlJob, UpdateControlStatus,
    };
    use super::{
        ControlEndpoint, ControlStatus, GatewayControlStatus, ManagementWebControlStatus,
        StaticControlHandler, default_unix_socket, is_local_endpoint,
    };
    #[cfg(unix)]
    use std::{
        collections::BTreeMap,
        sync::{Mutex, mpsc},
    };
    use std::{path::PathBuf, sync::Arc};

    fn status() -> ControlStatus {
        ControlStatus {
            schema_version: 2,
            agent: String::from("running"),
            agent_version: String::from("2.0.0-dev.0"),
            gateway: GatewayControlStatus::Running,
            management_web: ManagementWebControlStatus::Running,
            management_web_url: Some(String::from("http://127.0.0.1:7080")),
            uptime_seconds: 1,
            latest_error_code: None,
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
    impl ControlHandler for DiagnosticsAndSecretHandler {
        fn handle(&self, _command: super::ControlCommand) -> Result<ControlStatus, ControlError> {
            Ok(status())
        }

        fn diagnostics_bundle(&self) -> Result<DiagnosticsControlBundle, ControlError> {
            Ok(DiagnosticsControlBundle {
                schema_version: 1,
                agent_version: String::from("2.0.0-dev.0"),
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

    #[test]
    fn recognizes_local_endpoint_forms() {
        assert!(is_local_endpoint(&default_unix_socket(
            PathBuf::from("/tmp/cmclient").as_path()
        )));
        assert!(is_local_endpoint(&ControlEndpoint::named_pipe(
            r"\\.\pipe\cmclient-control"
        )));
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
            server.serve_once()
        });
        let client = ControlClient::new(endpoint).expect("client should initialize");
        assert_eq!(client.status().expect("status should load"), status());
        assert_eq!(client.start().expect("start should load"), status());
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
}
