use rustls::{ServerConfig, ServerConnection, StreamOwned};
use std::{
    collections::BTreeMap,
    io::{self, BufReader, Read, Write},
    net::{IpAddr, SocketAddr, TcpListener, TcpStream},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

const MAX_REQUEST_BYTES: usize = 1024 * 1024;
const GATEWAY_TIMEOUT: Duration = Duration::from_secs(2);
const SERVICE_POLL_INTERVAL: Duration = Duration::from_millis(20);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagementWebConfig {
    pub enabled: bool,
    pub bind: IpAddr,
    pub port: u16,
    pub gateway: SocketAddr,
    pub allow_lan: bool,
    pub tls: Option<ManagementTlsConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagementTlsConfig {
    pub certificate_path: PathBuf,
    pub private_key_path: PathBuf,
}

impl Default for ManagementWebConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            bind: IpAddr::from([127, 0, 0, 1]),
            port: 7080,
            gateway: SocketAddr::from(([127, 0, 0, 1], 4810)),
            allow_lan: false,
            tls: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManagementWebError {
    Disabled,
    NonLoopbackBind,
    Io,
    InvalidHttp,
    RequestTooLarge,
    TlsConfiguration,
}

impl ManagementWebError {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Disabled => "MANAGEMENT_WEB_DISABLED",
            Self::NonLoopbackBind => "MANAGEMENT_WEB_NON_LOOPBACK_DENIED",
            Self::Io => "MANAGEMENT_WEB_IO_FAILED",
            Self::InvalidHttp => "MANAGEMENT_WEB_HTTP_INVALID",
            Self::RequestTooLarge => "MANAGEMENT_WEB_REQUEST_TOO_LARGE",
            Self::TlsConfiguration => "MANAGEMENT_WEB_TLS_CONFIGURATION_INVALID",
        }
    }
}

/// Handles a small Agent-owned API surface before a request is forwarded to Gateway.
///
/// The management listener remains transport-only: ownership of the actual state stays with
/// the Agent application that installs this handler.
pub trait ManagementWebApiHandler: Send + Sync {
    /// Returns `true` after writing a complete response to `client`.
    fn handle(
        &self,
        client: &mut dyn ManagementWebStream,
        request: &ManagementWebRequest,
    ) -> Result<bool, ManagementWebError>;
}

pub trait ManagementWebStream: Read + Write {}

impl<T: Read + Write> ManagementWebStream for T {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagementWebRequest {
    pub method: String,
    pub path: String,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
    pub remote_addr: SocketAddr,
}

impl ManagementWebRequest {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .get(&name.to_ascii_lowercase())
            .map(String::as_str)
    }
}

pub struct ManagementWebListener {
    listener: TcpListener,
    gateway: SocketAddr,
    api_handler: Option<Arc<dyn ManagementWebApiHandler>>,
    tls: Option<Arc<ServerConfig>>,
}

pub struct ManagementWebService {
    address: SocketAddr,
    shutdown: Arc<AtomicBool>,
    worker: Option<JoinHandle<Result<(), ManagementWebError>>>,
}

impl ManagementWebService {
    pub fn start(config: &ManagementWebConfig) -> Result<Self, ManagementWebError> {
        let listener = ManagementWebListener::bind(config)?;
        Self::start_listener(listener)
    }

    pub fn start_with_api_handler(
        config: &ManagementWebConfig,
        api_handler: Arc<dyn ManagementWebApiHandler>,
    ) -> Result<Self, ManagementWebError> {
        let listener = ManagementWebListener::bind_with_api_handler(config, api_handler)?;
        Self::start_listener(listener)
    }

    fn start_listener(listener: ManagementWebListener) -> Result<Self, ManagementWebError> {
        let address = listener.local_addr()?;
        let shutdown = Arc::new(AtomicBool::new(true));
        let worker_shutdown = Arc::clone(&shutdown);
        let worker = thread::Builder::new()
            .name(String::from("cmclient-management-web"))
            .spawn(move || listener.serve_until(&worker_shutdown))
            .map_err(|_| ManagementWebError::Io)?;
        Ok(Self {
            address,
            shutdown,
            worker: Some(worker),
        })
    }

    pub const fn local_addr(&self) -> SocketAddr {
        self.address
    }

    pub fn stop(mut self) -> Result<(), ManagementWebError> {
        self.shutdown.store(false, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            worker.join().map_err(|_| ManagementWebError::Io)??;
        }
        Ok(())
    }
}

impl Drop for ManagementWebService {
    fn drop(&mut self) {
        self.shutdown.store(false, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl ManagementWebListener {
    pub fn bind(config: &ManagementWebConfig) -> Result<Self, ManagementWebError> {
        Self::bind_internal(config, None)
    }

    pub fn bind_with_api_handler(
        config: &ManagementWebConfig,
        api_handler: Arc<dyn ManagementWebApiHandler>,
    ) -> Result<Self, ManagementWebError> {
        Self::bind_internal(config, Some(api_handler))
    }

    fn bind_internal(
        config: &ManagementWebConfig,
        api_handler: Option<Arc<dyn ManagementWebApiHandler>>,
    ) -> Result<Self, ManagementWebError> {
        if !config.enabled {
            return Err(ManagementWebError::Disabled);
        }
        if !config.gateway.ip().is_loopback()
            || (!config.bind.is_loopback()
                && (!config.allow_lan || api_handler.is_none() || config.tls.is_none()))
        {
            return Err(ManagementWebError::NonLoopbackBind);
        }
        let listener = TcpListener::bind(SocketAddr::new(config.bind, config.port))
            .map_err(|_| ManagementWebError::Io)?;
        let tls = config.tls.as_ref().map(load_tls_config).transpose()?;
        Ok(Self {
            listener,
            gateway: config.gateway,
            api_handler,
            tls,
        })
    }

    pub fn local_addr(&self) -> Result<SocketAddr, ManagementWebError> {
        self.listener
            .local_addr()
            .map_err(|_| ManagementWebError::Io)
    }

    pub fn serve(self) -> Result<(), ManagementWebError> {
        loop {
            let (stream, remote_addr) =
                self.listener.accept().map_err(|_| ManagementWebError::Io)?;
            let gateway = self.gateway;
            let api_handler = self.api_handler.clone();
            let tls = self.tls.clone();
            thread::spawn(move || {
                let _ = serve_connection(stream, remote_addr, gateway, api_handler, tls);
            });
        }
    }

    pub fn serve_once(&self) -> Result<(), ManagementWebError> {
        let (stream, remote_addr) = self.listener.accept().map_err(|_| ManagementWebError::Io)?;
        serve_connection(
            stream,
            remote_addr,
            self.gateway,
            self.api_handler.clone(),
            self.tls.clone(),
        )
    }

    fn serve_until(self, shutdown: &AtomicBool) -> Result<(), ManagementWebError> {
        self.listener
            .set_nonblocking(true)
            .map_err(|_| ManagementWebError::Io)?;
        while shutdown.load(Ordering::Acquire) {
            match self.listener.accept() {
                Ok((stream, remote_addr)) => {
                    let gateway = self.gateway;
                    let api_handler = self.api_handler.clone();
                    let tls = self.tls.clone();
                    thread::spawn(move || {
                        let _ = serve_connection(stream, remote_addr, gateway, api_handler, tls);
                    });
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(SERVICE_POLL_INTERVAL);
                }
                Err(_) => return Err(ManagementWebError::Io),
            }
        }
        Ok(())
    }
}

pub fn gateway_health(gateway: SocketAddr) -> bool {
    let Ok(mut stream) = TcpStream::connect_timeout(&gateway, GATEWAY_TIMEOUT) else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(GATEWAY_TIMEOUT));
    let _ = stream.set_write_timeout(Some(GATEWAY_TIMEOUT));
    if stream
        .write_all(
            b"GET /api/v1/system/health HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n",
        )
        .is_err()
    {
        return false;
    }
    let mut response = Vec::new();
    stream.read_to_end(&mut response).is_ok()
        && response.starts_with(b"HTTP/1.1 200")
        && response
            .windows(br#"{"status":"ok"}"#.len())
            .any(|window| window == br#"{"status":"ok"}"#)
}

fn serve_connection(
    mut client: TcpStream,
    remote_addr: SocketAddr,
    gateway: SocketAddr,
    api_handler: Option<Arc<dyn ManagementWebApiHandler>>,
    tls: Option<Arc<ServerConfig>>,
) -> Result<(), ManagementWebError> {
    client
        .set_read_timeout(Some(GATEWAY_TIMEOUT))
        .map_err(|_| ManagementWebError::Io)?;
    client
        .set_write_timeout(Some(GATEWAY_TIMEOUT))
        .map_err(|_| ManagementWebError::Io)?;
    if let Some(tls) = tls {
        let connection =
            ServerConnection::new(tls).map_err(|_| ManagementWebError::TlsConfiguration)?;
        let mut stream = StreamOwned::new(connection, client);
        return serve_http_connection(&mut stream, remote_addr, gateway, api_handler);
    }
    serve_http_connection(&mut client, remote_addr, gateway, api_handler)
}

fn serve_http_connection(
    client: &mut dyn ManagementWebStream,
    remote_addr: SocketAddr,
    gateway: SocketAddr,
    api_handler: Option<Arc<dyn ManagementWebApiHandler>>,
) -> Result<(), ManagementWebError> {
    let request = read_request(client)?;
    let request_context = parse_request(&request, remote_addr)?;
    if let Some(api_handler) = api_handler
        && api_handler.handle(client, &request_context)?
    {
        return Ok(());
    }
    match (
        request_context.method.as_str(),
        request_context.path.as_str(),
    ) {
        ("GET", "/") | ("GET", "/index.html") => write_response(
            client,
            "200 OK",
            "text/html; charset=utf-8",
            "<!doctype html><title>CMClient</title><main id=app>CMClient management web</main>",
        ),
        (_, path) if path.starts_with("/api/") => proxy_api(client, gateway, &request),
        (_, _) => write_response(
            client,
            "404 Not Found",
            "application/json",
            r#"{"code":"WEB_ROUTE_NOT_FOUND"}"#,
        ),
    }
}

fn proxy_api(
    client: &mut dyn ManagementWebStream,
    gateway: SocketAddr,
    request: &[u8],
) -> Result<(), ManagementWebError> {
    let mut upstream = match TcpStream::connect_timeout(&gateway, GATEWAY_TIMEOUT) {
        Ok(stream) => stream,
        Err(_) => {
            return write_response(
                client,
                "503 Service Unavailable",
                "application/json",
                r#"{"code":"GATEWAY_PROXY_UNAVAILABLE"}"#,
            );
        }
    };
    upstream
        .set_read_timeout(None)
        .map_err(|_| ManagementWebError::Io)?;
    upstream
        .set_write_timeout(Some(GATEWAY_TIMEOUT))
        .map_err(|_| ManagementWebError::Io)?;
    let request = with_connection_close(request)?;
    if upstream.write_all(&request).is_err() {
        return write_response(
            client,
            "503 Service Unavailable",
            "application/json",
            r#"{"code":"GATEWAY_PROXY_UNAVAILABLE"}"#,
        );
    }
    io::copy(&mut upstream, client).map_err(|_| ManagementWebError::Io)?;
    Ok(())
}

fn read_request(stream: &mut dyn ManagementWebStream) -> Result<Vec<u8>, ManagementWebError> {
    let mut request = Vec::new();
    let mut buffer = [0_u8; 8192];
    loop {
        let count = stream
            .read(&mut buffer)
            .map_err(|_| ManagementWebError::Io)?;
        if count == 0 {
            return Err(ManagementWebError::InvalidHttp);
        }
        request.extend_from_slice(&buffer[..count]);
        if request.len() > MAX_REQUEST_BYTES {
            return Err(ManagementWebError::RequestTooLarge);
        }
        let Some(header_end) = header_end(&request) else {
            continue;
        };
        let content_length = content_length(&request[..header_end])?;
        let request_length = header_end + content_length;
        if request_length > MAX_REQUEST_BYTES {
            return Err(ManagementWebError::RequestTooLarge);
        }
        if request.len() >= request_length {
            request.truncate(request_length);
            return Ok(request);
        }
    }
}

fn parse_request(
    request: &[u8],
    remote_addr: SocketAddr,
) -> Result<ManagementWebRequest, ManagementWebError> {
    let header_end = header_end(request).ok_or(ManagementWebError::InvalidHttp)?;
    let header =
        std::str::from_utf8(&request[..header_end]).map_err(|_| ManagementWebError::InvalidHttp)?;
    let header = header
        .strip_suffix("\r\n\r\n")
        .ok_or(ManagementWebError::InvalidHttp)?;
    let line = header
        .lines()
        .next()
        .ok_or(ManagementWebError::InvalidHttp)?;
    let mut parts = line.split_whitespace();
    let method = parts.next().ok_or(ManagementWebError::InvalidHttp)?;
    let path = parts.next().ok_or(ManagementWebError::InvalidHttp)?;
    let version = parts.next().ok_or(ManagementWebError::InvalidHttp)?;
    if parts.next().is_some() || !version.starts_with("HTTP/") || !path.starts_with('/') {
        return Err(ManagementWebError::InvalidHttp);
    }
    let mut headers = BTreeMap::new();
    for line in header.lines().skip(1) {
        let Some((name, value)) = line.split_once(':') else {
            return Err(ManagementWebError::InvalidHttp);
        };
        let name = name.trim();
        if name.is_empty()
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            return Err(ManagementWebError::InvalidHttp);
        }
        let key = name.to_ascii_lowercase();
        if headers.insert(key, value.trim().to_owned()).is_some() {
            return Err(ManagementWebError::InvalidHttp);
        }
    }
    Ok(ManagementWebRequest {
        method: String::from(method),
        path: String::from(path),
        headers,
        body: request[header_end..].to_vec(),
        remote_addr,
    })
}

fn content_length(header: &[u8]) -> Result<usize, ManagementWebError> {
    let header = std::str::from_utf8(header).map_err(|_| ManagementWebError::InvalidHttp)?;
    let mut length = None;
    for line in header.lines().skip(1) {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.eq_ignore_ascii_case("transfer-encoding") {
            return Err(ManagementWebError::InvalidHttp);
        }
        if name.eq_ignore_ascii_case("content-length") {
            let parsed = value
                .trim()
                .parse::<usize>()
                .map_err(|_| ManagementWebError::InvalidHttp);
            if length.replace(parsed?).is_some() {
                return Err(ManagementWebError::InvalidHttp);
            }
        }
    }
    Ok(length.unwrap_or(0))
}

fn with_connection_close(request: &[u8]) -> Result<Vec<u8>, ManagementWebError> {
    let header_end = header_end(request).ok_or(ManagementWebError::InvalidHttp)?;
    let header =
        std::str::from_utf8(&request[..header_end]).map_err(|_| ManagementWebError::InvalidHttp)?;
    let header = header
        .strip_suffix("\r\n\r\n")
        .ok_or(ManagementWebError::InvalidHttp)?;
    let mut rewritten = String::new();
    for (index, line) in header.split("\r\n").enumerate() {
        if index > 0
            && (line
                .split_once(':')
                .is_some_and(|(name, _)| name.eq_ignore_ascii_case("connection"))
                || line
                    .split_once(':')
                    .is_some_and(|(name, _)| name.eq_ignore_ascii_case("proxy-connection")))
        {
            continue;
        }
        rewritten.push_str(line);
        rewritten.push_str("\r\n");
    }
    rewritten.push_str("connection: close\r\n\r\n");
    let mut rewritten = rewritten.into_bytes();
    rewritten.extend_from_slice(&request[header_end..]);
    Ok(rewritten)
}

fn header_end(request: &[u8]) -> Option<usize> {
    request
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| index + 4)
}

fn write_response(
    stream: &mut dyn ManagementWebStream,
    status: &str,
    content_type: &str,
    body: &str,
) -> Result<(), ManagementWebError> {
    let header = format!(
        "HTTP/1.1 {status}\r\ncontent-type: {content_type}\r\ncache-control: no-store\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    stream
        .write_all(header.as_bytes())
        .and_then(|_| stream.write_all(body.as_bytes()))
        .map_err(|_| ManagementWebError::Io)
}

fn load_tls_config(config: &ManagementTlsConfig) -> Result<Arc<ServerConfig>, ManagementWebError> {
    let certificate_file = std::fs::File::open(&config.certificate_path)
        .map_err(|_| ManagementWebError::TlsConfiguration)?;
    let mut certificate_reader = BufReader::new(certificate_file);
    let certificates = rustls_pemfile::certs(&mut certificate_reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| ManagementWebError::TlsConfiguration)?;
    if certificates.is_empty() {
        return Err(ManagementWebError::TlsConfiguration);
    }
    let key_file = std::fs::File::open(&config.private_key_path)
        .map_err(|_| ManagementWebError::TlsConfiguration)?;
    let mut key_reader = BufReader::new(key_file);
    let key = rustls_pemfile::private_key(&mut key_reader)
        .map_err(|_| ManagementWebError::TlsConfiguration)?
        .ok_or(ManagementWebError::TlsConfiguration)?;
    ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certificates, key)
        .map(Arc::new)
        .map_err(|_| ManagementWebError::TlsConfiguration)
}

#[cfg(test)]
mod tests {
    use super::{
        ManagementWebApiHandler, ManagementWebConfig, ManagementWebError, ManagementWebListener,
        ManagementWebService, ManagementWebStream, gateway_health,
    };
    use std::{
        io::{Read, Write},
        net::{SocketAddr, TcpListener, TcpStream},
        sync::Arc,
        thread,
    };

    struct AgentRoute;

    impl ManagementWebApiHandler for AgentRoute {
        fn handle(
            &self,
            client: &mut dyn ManagementWebStream,
            request: &super::ManagementWebRequest,
        ) -> Result<bool, ManagementWebError> {
            if request.method == "GET" && request.path == "/api/v1/updates" {
                client
                    .write_all(
                        b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 16\r\nconnection: close\r\n\r\n{\"schemaVersion\":1}",
                    )
                    .map_err(|_| ManagementWebError::Io)?;
                return Ok(true);
            }
            Ok(false)
        }
    }

    #[test]
    fn rejects_lan_bind_without_the_security_layer() {
        let config = ManagementWebConfig {
            bind: "0.0.0.0".parse().expect("IP should parse"),
            ..Default::default()
        };
        assert!(matches!(
            ManagementWebListener::bind(&config),
            Err(ManagementWebError::NonLoopbackBind)
        ));
    }

    #[test]
    fn rejects_lan_bind_when_tls_files_are_missing() {
        let missing = std::env::temp_dir().join(format!(
            "cmclient-management-tls-missing-{}",
            std::process::id()
        ));
        let config = ManagementWebConfig {
            bind: "0.0.0.0".parse().expect("IP should parse"),
            port: 0,
            allow_lan: true,
            tls: Some(super::ManagementTlsConfig {
                certificate_path: missing.join("certificate.pem"),
                private_key_path: missing.join("private-key.pem"),
            }),
            ..Default::default()
        };

        assert!(matches!(
            ManagementWebListener::bind_with_api_handler(&config, Arc::new(AgentRoute)),
            Err(ManagementWebError::TlsConfiguration)
        ));
    }

    #[test]
    fn rejects_chunked_requests_before_they_reach_the_gateway() {
        assert!(matches!(
            super::content_length(
                b"GET /api/v1/system/health HTTP/1.1\r\nhost: localhost\r\ntransfer-encoding: chunked\r\n\r\n"
            ),
            Err(ManagementWebError::InvalidHttp)
        ));
    }

    #[test]
    fn proxies_gateway_sse_streams_and_health_checks() {
        let gateway = TcpListener::bind("127.0.0.1:0").expect("gateway should bind");
        let gateway_address = gateway.local_addr().expect("gateway address should load");
        let gateway_thread = thread::spawn(move || {
            for _ in 0..2 {
                let (mut stream, _) = gateway.accept().expect("gateway should accept");
                let mut request = [0_u8; 4096];
                let count = stream.read(&mut request).expect("request should read");
                if request[..count].starts_with(b"GET /api/v1/events ") {
                    let body = ": heartbeat\n\nevent: gateway.ready\ndata: {}\n\n";
                    let response = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    stream
                        .write_all(response.as_bytes())
                        .expect("SSE response should write");
                } else {
                    stream
                        .write_all(
                            b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 15\r\nconnection: close\r\n\r\n{\"status\":\"ok\"}",
                        )
                        .expect("health response should write");
                }
            }
        });
        let config = ManagementWebConfig {
            port: 0,
            gateway: gateway_address,
            ..Default::default()
        };
        let listener = ManagementWebListener::bind(&config).expect("listener should bind");
        let address = listener.local_addr().expect("address should load");
        let server = thread::spawn(move || listener.serve_once());
        let mut client = TcpStream::connect(address).expect("client should connect");
        client
            .write_all(b"GET /api/v1/events HTTP/1.1\r\nhost: localhost\r\n\r\n")
            .expect("request should write");
        let mut response = String::new();
        client
            .read_to_string(&mut response)
            .expect("response should read");
        assert!(response.starts_with("HTTP/1.1 200"));
        assert!(response.contains("event: gateway.ready"));
        assert!(gateway_health(gateway_address));
        server
            .join()
            .expect("server should join")
            .expect("server should respond");
        gateway_thread.join().expect("gateway should join");
    }

    #[test]
    fn reports_gateway_unavailable_without_exposing_transport_errors() {
        let reserved = TcpListener::bind("127.0.0.1:0").expect("port should bind");
        let gateway: SocketAddr = reserved.local_addr().expect("address should load");
        drop(reserved);
        let config = ManagementWebConfig {
            port: 0,
            gateway,
            ..Default::default()
        };
        let listener = ManagementWebListener::bind(&config).expect("listener should bind");
        let address = listener.local_addr().expect("address should load");
        let server = thread::spawn(move || listener.serve_once());
        let mut client = TcpStream::connect(address).expect("client should connect");
        client
            .write_all(b"GET /api/v1/system/health HTTP/1.1\r\nhost: localhost\r\n\r\n")
            .expect("request should write");
        let mut response = String::new();
        client
            .read_to_string(&mut response)
            .expect("response should read");
        assert!(response.starts_with("HTTP/1.1 503"));
        assert!(response.contains("GATEWAY_PROXY_UNAVAILABLE"));
        server
            .join()
            .expect("server should join")
            .expect("server should respond");
    }

    #[test]
    fn serves_agent_owned_routes_when_gateway_is_unavailable() {
        let reserved = TcpListener::bind("127.0.0.1:0").expect("port should bind");
        let gateway = reserved.local_addr().expect("gateway address should load");
        drop(reserved);
        let listener = ManagementWebListener::bind_with_api_handler(
            &ManagementWebConfig {
                port: 0,
                gateway,
                ..Default::default()
            },
            Arc::new(AgentRoute),
        )
        .expect("listener should bind");
        let address = listener.local_addr().expect("address should load");
        let server = thread::spawn(move || listener.serve_once());
        let mut client = TcpStream::connect(address).expect("client should connect");
        client
            .write_all(b"GET /api/v1/updates HTTP/1.1\r\nhost: localhost\r\n\r\n")
            .expect("request should write");
        let mut response = String::new();
        client
            .read_to_string(&mut response)
            .expect("response should read");
        assert!(response.starts_with("HTTP/1.1 200"));
        assert!(response.contains("{\"schemaVersion\":1}"));
        server
            .join()
            .expect("server should join")
            .expect("server should respond");
    }

    #[test]
    fn service_stops_and_releases_its_listener() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("port should bind");
        let gateway = listener.local_addr().expect("gateway address should load");
        let service = ManagementWebService::start(&ManagementWebConfig {
            port: 0,
            gateway,
            ..Default::default()
        })
        .expect("service should start");
        let address = service.local_addr();
        service.stop().expect("service should stop");
        TcpListener::bind(address).expect("service should release listener");
    }
}
