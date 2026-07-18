use std::{
    io::{self, Read, Write},
    net::{IpAddr, SocketAddr, TcpListener, TcpStream},
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
}

impl Default for ManagementWebConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            bind: IpAddr::from([127, 0, 0, 1]),
            port: 7080,
            gateway: SocketAddr::from(([127, 0, 0, 1], 4810)),
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
}

impl ManagementWebError {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Disabled => "MANAGEMENT_WEB_DISABLED",
            Self::NonLoopbackBind => "MANAGEMENT_WEB_NON_LOOPBACK_DENIED",
            Self::Io => "MANAGEMENT_WEB_IO_FAILED",
            Self::InvalidHttp => "MANAGEMENT_WEB_HTTP_INVALID",
            Self::RequestTooLarge => "MANAGEMENT_WEB_REQUEST_TOO_LARGE",
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
        client: &mut TcpStream,
        method: &str,
        path: &str,
    ) -> Result<bool, ManagementWebError>;
}

pub struct ManagementWebListener {
    listener: TcpListener,
    gateway: SocketAddr,
    api_handler: Option<Arc<dyn ManagementWebApiHandler>>,
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
        if !config.bind.is_loopback() || !config.gateway.ip().is_loopback() {
            return Err(ManagementWebError::NonLoopbackBind);
        }
        let listener = TcpListener::bind(SocketAddr::new(config.bind, config.port))
            .map_err(|_| ManagementWebError::Io)?;
        Ok(Self {
            listener,
            gateway: config.gateway,
            api_handler,
        })
    }

    pub fn local_addr(&self) -> Result<SocketAddr, ManagementWebError> {
        self.listener
            .local_addr()
            .map_err(|_| ManagementWebError::Io)
    }

    pub fn serve(self) -> Result<(), ManagementWebError> {
        loop {
            let (stream, _) = self.listener.accept().map_err(|_| ManagementWebError::Io)?;
            let gateway = self.gateway;
            let api_handler = self.api_handler.clone();
            thread::spawn(move || {
                let _ = serve_connection(stream, gateway, api_handler);
            });
        }
    }

    pub fn serve_once(&self) -> Result<(), ManagementWebError> {
        let (stream, _) = self.listener.accept().map_err(|_| ManagementWebError::Io)?;
        serve_connection(stream, self.gateway, self.api_handler.clone())
    }

    fn serve_until(self, shutdown: &AtomicBool) -> Result<(), ManagementWebError> {
        self.listener
            .set_nonblocking(true)
            .map_err(|_| ManagementWebError::Io)?;
        while shutdown.load(Ordering::Acquire) {
            match self.listener.accept() {
                Ok((stream, _)) => {
                    let gateway = self.gateway;
                    let api_handler = self.api_handler.clone();
                    thread::spawn(move || {
                        let _ = serve_connection(stream, gateway, api_handler);
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
    gateway: SocketAddr,
    api_handler: Option<Arc<dyn ManagementWebApiHandler>>,
) -> Result<(), ManagementWebError> {
    client
        .set_read_timeout(Some(GATEWAY_TIMEOUT))
        .map_err(|_| ManagementWebError::Io)?;
    let request = read_request(&mut client)?;
    let (method, path) = request_target(&request)?;
    if let Some(api_handler) = api_handler
        && api_handler.handle(&mut client, &method, &path)?
    {
        return Ok(());
    }
    match (method.as_str(), path.as_str()) {
        ("GET", "/") | ("GET", "/index.html") => write_response(
            &mut client,
            "200 OK",
            "text/html; charset=utf-8",
            "<!doctype html><title>CMClient</title><main id=app>CMClient management web</main>",
        ),
        (_, path) if path.starts_with("/api/") => proxy_api(&mut client, gateway, &request),
        (_, _) => write_response(
            &mut client,
            "404 Not Found",
            "application/json",
            r#"{"code":"WEB_ROUTE_NOT_FOUND"}"#,
        ),
    }
}

fn proxy_api(
    client: &mut TcpStream,
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

fn read_request(stream: &mut TcpStream) -> Result<Vec<u8>, ManagementWebError> {
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

fn request_target(request: &[u8]) -> Result<(String, String), ManagementWebError> {
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
    Ok((String::from(method), String::from(path)))
}

fn content_length(header: &[u8]) -> Result<usize, ManagementWebError> {
    let header = std::str::from_utf8(header).map_err(|_| ManagementWebError::InvalidHttp)?;
    for line in header.lines().skip(1) {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.eq_ignore_ascii_case("content-length") {
            return value
                .trim()
                .parse::<usize>()
                .map_err(|_| ManagementWebError::InvalidHttp);
        }
    }
    Ok(0)
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
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &str,
) -> Result<(), ManagementWebError> {
    let header = format!(
        "HTTP/1.1 {status}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    stream
        .write_all(header.as_bytes())
        .and_then(|_| stream.write_all(body.as_bytes()))
        .map_err(|_| ManagementWebError::Io)
}

#[cfg(test)]
mod tests {
    use super::{
        ManagementWebApiHandler, ManagementWebConfig, ManagementWebError, ManagementWebListener,
        ManagementWebService, gateway_health,
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
            client: &mut TcpStream,
            method: &str,
            path: &str,
        ) -> Result<bool, ManagementWebError> {
            if method == "GET" && path == "/api/v1/updates" {
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
