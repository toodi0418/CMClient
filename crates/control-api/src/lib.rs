//! Local Agent control API over Unix sockets or Windows named pipes.

use serde::{Deserialize, Serialize};
use std::{
    fmt::{Display, Formatter},
    path::{Path, PathBuf},
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
    pub gateway: GatewayControlStatus,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlError {
    EndpointAlreadyInUse,
    UnsupportedEndpoint,
    Io,
    InvalidHttp,
    ResponseTooLarge,
}

impl ControlError {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::EndpointAlreadyInUse => "CONTROL_ENDPOINT_ALREADY_IN_USE",
            Self::UnsupportedEndpoint => "CONTROL_ENDPOINT_UNSUPPORTED",
            Self::Io => "CONTROL_IO_FAILED",
            Self::InvalidHttp => "CONTROL_HTTP_INVALID",
            Self::ResponseTooLarge => "CONTROL_RESPONSE_TOO_LARGE",
        }
    }
}

impl Display for ControlError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.code())
    }
}

impl std::error::Error for ControlError {}

#[derive(Debug, Clone)]
pub struct ControlRouter {
    status: ControlStatus,
}

impl ControlRouter {
    pub fn new(status: ControlStatus) -> Self {
        Self { status }
    }

    fn route(&self, request: &str) -> Result<(u16, Vec<u8>), ControlError> {
        let request_line = request.lines().next().ok_or(ControlError::InvalidHttp)?;
        match request_line
            .split_whitespace()
            .collect::<Vec<_>>()
            .as_slice()
        {
            ["GET", "/api/v1/control/status", "HTTP/1.1"]
            | ["GET", "/api/v1/control/status", "HTTP/1.0"] => serde_json::to_vec(&self.status)
                .map(|body| (200, body))
                .map_err(|_| ControlError::Io),
            [_, _, _] => Ok((404, br#"{"code":"CONTROL_ROUTE_NOT_FOUND"}"#.to_vec())),
            _ => Err(ControlError::InvalidHttp),
        }
    }
}

#[cfg(unix)]
mod unix {
    use super::{ControlEndpoint, ControlError, ControlRouter, ControlStatus};
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
            status: ControlStatus,
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
                router: ControlRouter::new(status),
            })
        }

        pub fn serve_once(&self) -> Result<(), ControlError> {
            let (mut stream, _) = self.listener.accept().map_err(|_| ControlError::Io)?;
            let mut request = [0_u8; 8_192];
            let bytes = stream.read(&mut request).map_err(|_| ControlError::Io)?;
            let request =
                std::str::from_utf8(&request[..bytes]).map_err(|_| ControlError::InvalidHttp)?;
            let (status, body) = self.router.route(request)?;
            let status_text = if status == 200 { "OK" } else { "Not Found" };
            write!(
                stream,
                "HTTP/1.1 {status} {status_text}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len()
            )
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
            let mut stream = UnixStream::connect(&self.endpoint).map_err(|_| ControlError::Io)?;
            stream
                .write_all(b"GET /api/v1/control/status HTTP/1.1\r\nhost: localhost\r\n\r\n")
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
                return Err(ControlError::InvalidHttp);
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
    pub fn bind(_endpoint: ControlEndpoint, _status: ControlStatus) -> Result<Self, ControlError> {
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
        ControlEndpoint, ControlStatus, GatewayControlStatus, default_unix_socket,
        is_local_endpoint,
    };
    use std::path::PathBuf;

    fn status() -> ControlStatus {
        ControlStatus {
            schema_version: 1,
            agent: String::from("running"),
            gateway: GatewayControlStatus::Running,
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
        let server = ControlServer::bind(endpoint.clone(), status()).expect("server should bind");
        let server_thread = std::thread::spawn(move || server.serve_once());
        let client = ControlClient::new(endpoint).expect("client should initialize");
        assert_eq!(client.status().expect("status should load"), status());
        server_thread
            .join()
            .expect("server thread should join")
            .expect("server should respond");
        std::fs::remove_dir_all(directory).expect("temporary directory should be removed");
    }
}
