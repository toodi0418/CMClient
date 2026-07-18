//! Local Agent control API over Unix sockets or Windows named pipes.

use serde::{Deserialize, Serialize};
use std::{
    fmt::{Display, Formatter},
    path::{Path, PathBuf},
    sync::Arc,
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

pub struct ControlRouter {
    handler: Arc<dyn ControlHandler>,
}

impl ControlRouter {
    pub fn new(handler: Arc<dyn ControlHandler>) -> Self {
        Self { handler }
    }

    fn route(&self, request: &str) -> Result<(u16, Vec<u8>), ControlError> {
        let request_line = request.lines().next().ok_or(ControlError::InvalidHttp)?;
        let command = match request_line
            .split_whitespace()
            .collect::<Vec<_>>()
            .as_slice()
        {
            ["GET", "/api/v1/control/status", "HTTP/1.1"]
            | ["GET", "/api/v1/control/status", "HTTP/1.0"] => ControlCommand::Status,
            ["POST", "/api/v1/control/start", "HTTP/1.1"]
            | ["POST", "/api/v1/control/start", "HTTP/1.0"] => ControlCommand::Start,
            ["POST", "/api/v1/control/stop", "HTTP/1.1"]
            | ["POST", "/api/v1/control/stop", "HTTP/1.0"] => ControlCommand::Stop,
            ["POST", "/api/v1/control/restart", "HTTP/1.1"]
            | ["POST", "/api/v1/control/restart", "HTTP/1.0"] => ControlCommand::Restart,
            ["POST", "/api/v1/control/web/enable", "HTTP/1.1"]
            | ["POST", "/api/v1/control/web/enable", "HTTP/1.0"] => {
                ControlCommand::EnableManagementWeb
            }
            ["POST", "/api/v1/control/web/disable", "HTTP/1.1"]
            | ["POST", "/api/v1/control/web/disable", "HTTP/1.0"] => {
                ControlCommand::DisableManagementWeb
            }
            [_, _, _] => return Ok((404, br#"{"code":"CONTROL_ROUTE_NOT_FOUND"}"#.to_vec())),
            _ => return Err(ControlError::InvalidHttp),
        };
        match self.handler.handle(command) {
            Ok(status) => serde_json::to_vec(&status)
                .map(|body| (200, body))
                .map_err(|_| ControlError::Io),
            Err(error) => serde_json::to_vec(&serde_json::json!({ "code": error.code() }))
                .map(|body| (500, body))
                .map_err(|_| ControlError::Io),
        }
    }
}

#[cfg(unix)]
mod unix {
    use super::{ControlEndpoint, ControlError, ControlHandler, ControlRouter, ControlStatus};
    use std::{
        fs,
        io::{Read, Write},
        os::unix::fs::PermissionsExt,
        os::unix::net::{UnixListener, UnixStream},
        path::PathBuf,
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
            let (mut stream, _) = self.listener.accept().map_err(|_| ControlError::Io)?;
            let mut request = [0_u8; 8_192];
            let bytes = stream.read(&mut request).map_err(|_| ControlError::Io)?;
            let request =
                std::str::from_utf8(&request[..bytes]).map_err(|_| ControlError::InvalidHttp)?;
            let (status, body) = self.router.route(request)?;
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
            stream.write_all(&body).map_err(|_| ControlError::Io)
        }

        pub fn endpoint(&self) -> &std::path::Path {
            &self.endpoint
        }
    }

    impl Drop for ControlServer {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.endpoint);
        }
    }

    pub struct ControlClient {
        endpoint: PathBuf,
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

        fn request(&self, method: &str, path: &str) -> Result<ControlStatus, ControlError> {
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
}

#[cfg(unix)]
pub use unix::{ControlClient, ControlServer};

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
        ControlEndpoint, ControlStatus, GatewayControlStatus, ManagementWebControlStatus,
        StaticControlHandler, default_unix_socket, is_local_endpoint,
    };
    use std::path::PathBuf;
    use std::sync::Arc;

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
}
