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
        let request_line = request.lines().next().ok_or(ControlError::InvalidHttp)?;
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
        };
        response.or_else(error_response)
    }
}

enum ControlRoute {
    Command(ControlCommand),
    UpdateStatus,
    UpdateEvents,
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
        let mut request = [0_u8; 8_192];
        let bytes = stream.read(&mut request).map_err(|_| ControlError::Io)?;
        let request =
            std::str::from_utf8(&request[..bytes]).map_err(|_| ControlError::InvalidHttp)?;
        match router.route(request)? {
            ControlResponse::Json { status, body } => {
                write_json_response(&mut stream, status, &body)
            }
            ControlResponse::EventStream(events) => write_update_event_stream(&mut stream, events),
        }
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

        pub fn subscribe_update_events(&self) -> Result<ControlUpdateEventStream, ControlError> {
            let mut stream = UnixStream::connect(&self.endpoint).map_err(|_| ControlError::Io)?;
            stream
                .write_all(
                    b"GET /api/v1/control/updates/events HTTP/1.1\r\nhost: localhost\r\naccept: text/event-stream\r\n\r\n",
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
            self.request_json(method, path)
        }

        fn request_update(
            &self,
            method: &str,
            path: &str,
        ) -> Result<super::UpdateControlStatus, ControlError> {
            self.request_json(method, path)
        }

        fn request_json<T: serde::de::DeserializeOwned>(
            &self,
            method: &str,
            path: &str,
        ) -> Result<T, ControlError> {
            let mut stream = UnixStream::connect(&self.endpoint).map_err(|_| ControlError::Io)?;
            let request =
                format!("{method} {path} HTTP/1.1\r\nhost: localhost\r\ncontent-length: 0\r\n\r\n");
            stream
                .write_all(request.as_bytes())
                .map_err(|_| ControlError::Io)?;
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

#[cfg(not(unix))]
pub struct ControlServer;

#[cfg(not(unix))]
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

#[cfg(not(unix))]
pub struct ControlClient;

#[cfg(not(unix))]
pub struct ControlUpdateEventStream;

#[cfg(not(unix))]
impl ControlUpdateEventStream {
    pub fn next_event(&mut self) -> Result<Option<ControlUpdateEvent>, ControlError> {
        Err(ControlError::UnsupportedEndpoint)
    }
}

#[cfg(not(unix))]
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

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use super::{ControlClient, ControlServer};
    use super::{
        ControlEndpoint, ControlError, ControlHandler, ControlStatus, ControlUpdateEvent,
        GatewayControlStatus, ManagementWebControlStatus, StaticControlHandler, UpdateControlJob,
        UpdateControlStatus, default_unix_socket, is_local_endpoint,
    };
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex, mpsc};

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

    struct UpdateHandler {
        update: UpdateControlStatus,
        events: Mutex<Option<mpsc::Receiver<ControlUpdateEvent>>>,
    }

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
}
