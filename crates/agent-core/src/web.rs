use rustls::{
    ServerConfig, ServerConnection, StreamOwned,
    pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
};
use std::{
    collections::BTreeMap,
    fmt,
    io::{self, Read, Write},
    net::{IpAddr, Shutdown, SocketAddr, TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, RwLock, RwLockReadGuard,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use zeroize::Zeroizing;

const MAX_REQUEST_BYTES: usize = 1024 * 1024;
const MAX_GATEWAY_HEALTH_RESPONSE_BYTES: usize = 4096;
const MAX_ACTIVE_CONNECTIONS: usize = 64;
const GATEWAY_TIMEOUT: Duration = Duration::from_secs(2);
const SERVICE_POLL_INTERVAL: Duration = Duration::from_millis(20);
const PROXY_READ_POLL_INTERVAL: Duration = Duration::from_millis(100);
const CONNECTION_LIMIT_BODY: &str = r#"{"code":"MANAGEMENT_WEB_CONNECTION_LIMIT_REACHED"}"#;
pub const GATEWAY_CAPABILITY_HEADER: &str = "x-cmclient-gateway-capability";

#[derive(Debug)]
struct GatewayRouteLease {
    active: RwLock<bool>,
}

impl GatewayRouteLease {
    fn new() -> Self {
        Self {
            active: RwLock::new(true),
        }
    }

    fn acquire(&self) -> Option<RwLockReadGuard<'_, bool>> {
        let guard = self
            .active
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (*guard).then_some(guard)
    }

    fn revoke(&self) {
        *self
            .active
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = false;
    }
}

#[derive(Clone)]
pub struct GatewayRoute {
    address: SocketAddr,
    capability: Arc<Zeroizing<String>>,
    lease: Arc<GatewayRouteLease>,
}

impl GatewayRoute {
    pub fn new(
        address: SocketAddr,
        capability: impl Into<String>,
    ) -> Result<Self, ManagementWebError> {
        let capability = Zeroizing::new(capability.into());
        if address.ip() != IpAddr::from([127, 0, 0, 1])
            || address.port() == 0
            || !valid_gateway_capability(&capability)
        {
            return Err(ManagementWebError::InvalidHttp);
        }
        Ok(Self {
            address,
            capability: Arc::new(capability),
            lease: Arc::new(GatewayRouteLease::new()),
        })
    }

    pub const fn address(&self) -> SocketAddr {
        self.address
    }

    /// Reports whether this route still belongs to the current Gateway session.
    ///
    /// Callers that send the capability should use [`GatewayRoute::active`] so the session
    /// cannot be revoked between checking this value and writing the secret to the socket.
    pub fn is_active(&self) -> bool {
        self.lease.acquire().is_some()
    }

    pub fn active(&self) -> Option<ActiveGatewayRoute<'_>> {
        self.lease.acquire().map(|lease| ActiveGatewayRoute {
            route: self,
            _lease: lease,
        })
    }

    fn revoke(&self) {
        self.lease.revoke();
    }
}

impl PartialEq for GatewayRoute {
    fn eq(&self, other: &Self) -> bool {
        self.address == other.address
            && self.capability == other.capability
            && Arc::ptr_eq(&self.lease, &other.lease)
    }
}

impl Eq for GatewayRoute {}

pub struct ActiveGatewayRoute<'a> {
    route: &'a GatewayRoute,
    _lease: RwLockReadGuard<'a, bool>,
}

impl ActiveGatewayRoute<'_> {
    pub const fn address(&self) -> SocketAddr {
        self.route.address
    }

    pub fn capability(&self) -> &str {
        self.route.capability.as_str()
    }
}

impl fmt::Debug for ActiveGatewayRoute<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActiveGatewayRoute")
            .field("address", &self.route.address)
            .field("capability", &"[REDACTED]")
            .finish()
    }
}

impl fmt::Debug for GatewayRoute {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GatewayRoute")
            .field("address", &self.address)
            .field("capability", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Default)]
pub struct GatewaySessionHandle {
    route: Arc<RwLock<Option<GatewayRoute>>>,
}

impl GatewaySessionHandle {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_route(route: GatewayRoute) -> Self {
        Self {
            route: Arc::new(RwLock::new(Some(route))),
        }
    }

    pub fn set(&self, route: GatewayRoute) {
        let mut current = self
            .route
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(previous) = current.take() {
            previous.revoke();
        }
        *current = route.is_active().then_some(route);
    }

    pub fn clear(&self) {
        let mut current = self
            .route
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(previous) = current.take() {
            previous.revoke();
        }
    }

    pub fn snapshot(&self) -> Option<GatewayRoute> {
        self.route
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .filter(GatewayRoute::is_active)
    }
}

impl fmt::Debug for GatewaySessionHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GatewaySessionHandle")
            .field("available", &self.snapshot().is_some())
            .finish()
    }
}

#[derive(Clone)]
enum GatewayRouteSource {
    Static {
        address: SocketAddr,
        capability: Option<Arc<Zeroizing<String>>>,
    },
    Dynamic(GatewaySessionHandle),
}

#[derive(Clone)]
struct ResolvedGatewayRoute {
    address: SocketAddr,
    capability: Option<Arc<Zeroizing<String>>>,
    lease: Option<Arc<GatewayRouteLease>>,
}

impl ResolvedGatewayRoute {
    fn acquire(&self) -> Option<Option<RwLockReadGuard<'_, bool>>> {
        self.lease
            .as_ref()
            .map_or(Some(None), |lease| lease.acquire().map(Some))
    }
}

impl GatewayRouteSource {
    fn dynamic(session: GatewaySessionHandle) -> Self {
        Self::Dynamic(session)
    }

    fn snapshot(&self) -> Option<ResolvedGatewayRoute> {
        match self {
            Self::Static {
                address,
                capability,
            } => Some(ResolvedGatewayRoute {
                address: *address,
                capability: capability.clone(),
                lease: None,
            }),
            Self::Dynamic(session) => session.snapshot().map(|route| ResolvedGatewayRoute {
                address: route.address,
                capability: Some(Arc::clone(&route.capability)),
                lease: Some(Arc::clone(&route.lease)),
            }),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ManagementWebConfig {
    pub enabled: bool,
    pub bind: IpAddr,
    pub port: u16,
    pub gateway: SocketAddr,
    pub gateway_capability: Option<String>,
    pub allow_lan: bool,
    pub tls: Option<ManagementTlsConfig>,
    pub static_web_root: Option<PathBuf>,
}

impl fmt::Debug for ManagementWebConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ManagementWebConfig")
            .field("enabled", &self.enabled)
            .field("bind", &self.bind)
            .field("port", &self.port)
            .field("gateway", &self.gateway)
            .field(
                "gateway_capability",
                &self.gateway_capability.as_ref().map(|_| "[REDACTED]"),
            )
            .field("allow_lan", &self.allow_lan)
            .field("tls", &self.tls)
            .field("static_web_root", &self.static_web_root)
            .finish()
    }
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
            gateway_capability: None,
            allow_lan: false,
            tls: None,
            static_web_root: None,
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
    gateway_route: GatewayRouteSource,
    api_handler: Option<Arc<dyn ManagementWebApiHandler>>,
    tls: Option<Arc<ServerConfig>>,
    static_web_root: Option<PathBuf>,
}

pub struct ManagementWebService {
    address: SocketAddr,
    shutdown: Arc<AtomicBool>,
    connections: Arc<ActiveConnectionRegistry>,
    connection_workers: Arc<ConnectionWorkerRegistry>,
    worker: Option<JoinHandle<Result<(), ManagementWebError>>>,
}

#[derive(Debug)]
struct ActiveConnectionRegistry {
    limit: usize,
    next_id: AtomicU64,
    streams: Mutex<BTreeMap<u64, TcpStream>>,
}

#[derive(Debug)]
struct ActiveConnectionSlot {
    id: u64,
    registry: Arc<ActiveConnectionRegistry>,
}

#[derive(Debug, Default)]
struct ConnectionWorkerRegistry {
    workers: Mutex<Vec<JoinHandle<()>>>,
}

#[derive(Debug)]
enum ConnectionRegistrationError {
    Full,
    Io,
}

impl ActiveConnectionRegistry {
    fn new(limit: usize) -> Self {
        debug_assert!(limit > 0);
        Self {
            limit,
            next_id: AtomicU64::new(1),
            streams: Mutex::new(BTreeMap::new()),
        }
    }

    fn try_register(
        self: &Arc<Self>,
        stream: &TcpStream,
    ) -> Result<ActiveConnectionSlot, ConnectionRegistrationError> {
        let mut streams = self
            .streams
            .lock()
            .map_err(|_| ConnectionRegistrationError::Io)?;
        if streams.len() >= self.limit {
            return Err(ConnectionRegistrationError::Full);
        }
        let shutdown_handle = stream
            .try_clone()
            .map_err(|_| ConnectionRegistrationError::Io)?;
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        streams.insert(id, shutdown_handle);
        Ok(ActiveConnectionSlot {
            id,
            registry: Arc::clone(self),
        })
    }

    fn shutdown_all(&self) {
        let streams = self
            .streams
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for stream in streams.values() {
            let _ = stream.shutdown(Shutdown::Both);
        }
    }

    #[cfg(test)]
    fn active_count(&self) -> usize {
        self.streams
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }
}

impl Drop for ActiveConnectionSlot {
    fn drop(&mut self) {
        self.registry
            .streams
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.id);
    }
}

impl ConnectionWorkerRegistry {
    fn track(&self, worker: JoinHandle<()>) {
        self.workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(worker);
    }

    fn reap_finished(&self) -> Result<(), ManagementWebError> {
        let finished = {
            let mut workers = self
                .workers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut finished = Vec::new();
            let mut index = 0;
            while index < workers.len() {
                if workers[index].is_finished() {
                    finished.push(workers.swap_remove(index));
                } else {
                    index += 1;
                }
            }
            finished
        };
        join_connection_workers(finished)
    }

    fn join_all(&self) -> Result<(), ManagementWebError> {
        let workers = std::mem::take(
            &mut *self
                .workers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        join_connection_workers(workers)
    }

    #[cfg(test)]
    fn tracked_count(&self) -> usize {
        self.workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }
}

fn join_connection_workers(workers: Vec<JoinHandle<()>>) -> Result<(), ManagementWebError> {
    let mut failed = false;
    for worker in workers {
        failed |= worker.join().is_err();
    }
    if failed {
        Err(ManagementWebError::Io)
    } else {
        Ok(())
    }
}

impl ManagementWebService {
    pub fn start(config: &ManagementWebConfig) -> Result<Self, ManagementWebError> {
        let listener = ManagementWebListener::bind(config)?;
        Self::start_listener(listener)
    }

    pub fn start_with_gateway_session(
        config: &ManagementWebConfig,
        gateway_session: GatewaySessionHandle,
    ) -> Result<Self, ManagementWebError> {
        let listener = ManagementWebListener::bind_with_gateway_session(config, gateway_session)?;
        Self::start_listener(listener)
    }

    pub fn start_with_api_handler(
        config: &ManagementWebConfig,
        api_handler: Arc<dyn ManagementWebApiHandler>,
    ) -> Result<Self, ManagementWebError> {
        let listener = ManagementWebListener::bind_with_api_handler(config, api_handler)?;
        Self::start_listener(listener)
    }

    pub fn start_with_api_handler_and_gateway_session(
        config: &ManagementWebConfig,
        api_handler: Arc<dyn ManagementWebApiHandler>,
        gateway_session: GatewaySessionHandle,
    ) -> Result<Self, ManagementWebError> {
        let listener = ManagementWebListener::bind_with_api_handler_and_gateway_session(
            config,
            api_handler,
            gateway_session,
        )?;
        Self::start_listener(listener)
    }

    fn start_listener(listener: ManagementWebListener) -> Result<Self, ManagementWebError> {
        Self::start_listener_with_connection_limit(listener, MAX_ACTIVE_CONNECTIONS)
    }

    fn start_listener_with_connection_limit(
        listener: ManagementWebListener,
        connection_limit: usize,
    ) -> Result<Self, ManagementWebError> {
        let address = listener.local_addr()?;
        let shutdown = Arc::new(AtomicBool::new(true));
        let worker_shutdown = Arc::clone(&shutdown);
        let connections = Arc::new(ActiveConnectionRegistry::new(connection_limit));
        let worker_connections = Arc::clone(&connections);
        let connection_workers = Arc::new(ConnectionWorkerRegistry::default());
        let listener_workers = Arc::clone(&connection_workers);
        let worker = thread::Builder::new()
            .name(String::from("cmclient-management-web"))
            .spawn(move || {
                listener.serve_until(worker_shutdown, worker_connections, listener_workers)
            })
            .map_err(|_| ManagementWebError::Io)?;
        Ok(Self {
            address,
            shutdown,
            connections,
            connection_workers,
            worker: Some(worker),
        })
    }

    pub const fn local_addr(&self) -> SocketAddr {
        self.address
    }

    pub fn stop(mut self) -> Result<(), ManagementWebError> {
        self.shutdown_and_join()
    }

    fn shutdown_and_join(&mut self) -> Result<(), ManagementWebError> {
        self.shutdown.store(false, Ordering::Release);
        let listener_result = self.worker.take().map_or(Ok(()), |worker| {
            worker.join().map_err(|_| ManagementWebError::Io)?
        });
        self.connections.shutdown_all();
        let connections_result = self.connection_workers.join_all();
        listener_result.and(connections_result)
    }
}

impl Drop for ManagementWebService {
    fn drop(&mut self) {
        let _ = self.shutdown_and_join();
    }
}

fn static_gateway_route(
    config: &ManagementWebConfig,
) -> Result<GatewayRouteSource, ManagementWebError> {
    if !config.gateway.ip().is_loopback() {
        return Err(ManagementWebError::NonLoopbackBind);
    }
    if config
        .gateway_capability
        .as_deref()
        .is_some_and(|value| !valid_gateway_capability(value))
    {
        return Err(ManagementWebError::InvalidHttp);
    }
    Ok(GatewayRouteSource::Static {
        address: config.gateway,
        capability: config
            .gateway_capability
            .clone()
            .map(Zeroizing::new)
            .map(Arc::new),
    })
}

impl ManagementWebListener {
    pub fn bind(config: &ManagementWebConfig) -> Result<Self, ManagementWebError> {
        if !config.enabled {
            return Err(ManagementWebError::Disabled);
        }
        Self::bind_internal(config, None, static_gateway_route(config)?)
    }

    pub fn bind_with_gateway_session(
        config: &ManagementWebConfig,
        gateway_session: GatewaySessionHandle,
    ) -> Result<Self, ManagementWebError> {
        Self::bind_internal(config, None, GatewayRouteSource::dynamic(gateway_session))
    }

    pub fn bind_with_api_handler(
        config: &ManagementWebConfig,
        api_handler: Arc<dyn ManagementWebApiHandler>,
    ) -> Result<Self, ManagementWebError> {
        if !config.enabled {
            return Err(ManagementWebError::Disabled);
        }
        Self::bind_internal(config, Some(api_handler), static_gateway_route(config)?)
    }

    pub fn bind_with_api_handler_and_gateway_session(
        config: &ManagementWebConfig,
        api_handler: Arc<dyn ManagementWebApiHandler>,
        gateway_session: GatewaySessionHandle,
    ) -> Result<Self, ManagementWebError> {
        Self::bind_internal(
            config,
            Some(api_handler),
            GatewayRouteSource::dynamic(gateway_session),
        )
    }

    fn bind_internal(
        config: &ManagementWebConfig,
        api_handler: Option<Arc<dyn ManagementWebApiHandler>>,
        gateway_route: GatewayRouteSource,
    ) -> Result<Self, ManagementWebError> {
        if !config.enabled {
            return Err(ManagementWebError::Disabled);
        }
        if !config.bind.is_loopback()
            && (!config.allow_lan || api_handler.is_none() || config.tls.is_none())
        {
            return Err(ManagementWebError::NonLoopbackBind);
        }
        let listener = TcpListener::bind(SocketAddr::new(config.bind, config.port))
            .map_err(|_| ManagementWebError::Io)?;
        let tls = config.tls.as_ref().map(load_tls_config).transpose()?;
        let static_web_root = config
            .static_web_root
            .as_ref()
            .map(|root| {
                let root = std::fs::canonicalize(root).map_err(|_| ManagementWebError::Io)?;
                if !root.is_dir() {
                    return Err(ManagementWebError::Io);
                }
                Ok(root)
            })
            .transpose()?;
        Ok(Self {
            listener,
            gateway_route,
            api_handler,
            tls,
            static_web_root,
        })
    }

    pub fn local_addr(&self) -> Result<SocketAddr, ManagementWebError> {
        self.listener
            .local_addr()
            .map_err(|_| ManagementWebError::Io)
    }

    pub fn serve(self) -> Result<(), ManagementWebError> {
        let running = Arc::new(AtomicBool::new(true));
        let connections = Arc::new(ActiveConnectionRegistry::new(MAX_ACTIVE_CONNECTIONS));
        let connection_workers = Arc::new(ConnectionWorkerRegistry::default());
        let serve_result: Result<(), ManagementWebError> = (|| {
            loop {
                connection_workers.reap_finished()?;
                let (stream, remote_addr) =
                    self.listener.accept().map_err(|_| ManagementWebError::Io)?;
                self.dispatch_connection(
                    stream,
                    remote_addr,
                    Arc::clone(&connections),
                    Arc::clone(&connection_workers),
                    Arc::clone(&running),
                )?;
            }
        })();
        running.store(false, Ordering::Release);
        connections.shutdown_all();
        serve_result.and(connection_workers.join_all())
    }

    pub fn serve_once(&self) -> Result<(), ManagementWebError> {
        let (stream, remote_addr) = self.listener.accept().map_err(|_| ManagementWebError::Io)?;
        serve_connection(
            stream,
            remote_addr,
            self.gateway_route.clone(),
            self.api_handler.clone(),
            self.tls.clone(),
            self.static_web_root.clone(),
            Arc::new(AtomicBool::new(true)),
        )
    }

    fn serve_until(
        self,
        shutdown: Arc<AtomicBool>,
        connections: Arc<ActiveConnectionRegistry>,
        connection_workers: Arc<ConnectionWorkerRegistry>,
    ) -> Result<(), ManagementWebError> {
        self.listener
            .set_nonblocking(true)
            .map_err(|_| ManagementWebError::Io)?;
        while shutdown.load(Ordering::Acquire) {
            connection_workers.reap_finished()?;
            match self.listener.accept() {
                Ok((stream, remote_addr)) => {
                    self.dispatch_connection(
                        stream,
                        remote_addr,
                        Arc::clone(&connections),
                        Arc::clone(&connection_workers),
                        Arc::clone(&shutdown),
                    )?;
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(SERVICE_POLL_INTERVAL);
                }
                Err(_) => return Err(ManagementWebError::Io),
            }
        }
        Ok(())
    }

    fn dispatch_connection(
        &self,
        mut stream: TcpStream,
        remote_addr: SocketAddr,
        connections: Arc<ActiveConnectionRegistry>,
        connection_workers: Arc<ConnectionWorkerRegistry>,
        running: Arc<AtomicBool>,
    ) -> Result<(), ManagementWebError> {
        stream
            .set_nonblocking(false)
            .map_err(|_| ManagementWebError::Io)?;
        let slot = match connections.try_register(&stream) {
            Ok(slot) => slot,
            Err(ConnectionRegistrationError::Full) => {
                let _ = stream.set_write_timeout(Some(GATEWAY_TIMEOUT));
                let _ = write_response(
                    &mut stream,
                    "503 Service Unavailable",
                    "application/json",
                    CONNECTION_LIMIT_BODY,
                );
                return Ok(());
            }
            Err(ConnectionRegistrationError::Io) => return Err(ManagementWebError::Io),
        };
        let gateway_route = self.gateway_route.clone();
        let api_handler = self.api_handler.clone();
        let tls = self.tls.clone();
        let static_web_root = self.static_web_root.clone();
        let worker = thread::Builder::new()
            .name(String::from("cmclient-management-web-connection"))
            .spawn(move || {
                let _slot = slot;
                let _ = serve_connection(
                    stream,
                    remote_addr,
                    gateway_route,
                    api_handler,
                    tls,
                    static_web_root,
                    running,
                );
            })
            .map_err(|_| ManagementWebError::Io)?;
        connection_workers.track(worker);
        Ok(())
    }
}

pub fn gateway_health(gateway: SocketAddr) -> bool {
    gateway_health_request(gateway, None, GATEWAY_TIMEOUT)
}

pub fn gateway_health_with_route(route: &GatewayRoute) -> bool {
    let Some(route) = route.active() else {
        return false;
    };
    gateway_health_request(route.address(), Some(route.capability()), GATEWAY_TIMEOUT)
}

fn gateway_health_request(
    gateway: SocketAddr,
    capability: Option<&str>,
    timeout: Duration,
) -> bool {
    let Some(deadline) = Instant::now().checked_add(timeout) else {
        return false;
    };
    let Some(connect_timeout) = remaining_until(deadline) else {
        return false;
    };
    let Ok(mut stream) = TcpStream::connect_timeout(&gateway, connect_timeout) else {
        return false;
    };
    let request = Zeroizing::new(if let Some(capability) = capability {
        if !valid_gateway_capability(capability) {
            return false;
        }
        format!(
            "GET /api/v1/system/health HTTP/1.1\r\nhost: localhost\r\n{GATEWAY_CAPABILITY_HEADER}: {capability}\r\nconnection: close\r\n\r\n"
        )
        .into_bytes()
    } else {
        b"GET /api/v1/system/health HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n"
            .to_vec()
    });
    if !write_all_until(&mut stream, request.as_slice(), deadline) {
        return false;
    }
    let mut response = Vec::with_capacity(512);
    let mut buffer = [0_u8; 512];
    loop {
        match validate_gateway_health_response(&response) {
            Ok(true) => return true,
            Ok(false) => {}
            Err(()) => return false,
        }
        let remaining_capacity = MAX_GATEWAY_HEALTH_RESPONSE_BYTES.saturating_sub(response.len());
        if remaining_capacity == 0 {
            return false;
        }
        let Some(read_timeout) = remaining_until(deadline) else {
            return false;
        };
        if stream.set_read_timeout(Some(read_timeout)).is_err() {
            return false;
        }
        let read_length = remaining_capacity.min(buffer.len());
        let count = match stream.read(&mut buffer[..read_length]) {
            Ok(0) | Err(_) => return false,
            Ok(count) => count,
        };
        response.extend_from_slice(&buffer[..count]);
    }
}

fn remaining_until(deadline: Instant) -> Option<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|remaining| !remaining.is_zero())
}

fn write_all_until(stream: &mut TcpStream, mut bytes: &[u8], deadline: Instant) -> bool {
    while !bytes.is_empty() {
        let Some(write_timeout) = remaining_until(deadline) else {
            return false;
        };
        if stream.set_write_timeout(Some(write_timeout)).is_err() {
            return false;
        }
        match stream.write(bytes) {
            Ok(0) | Err(_) => return false,
            Ok(count) => bytes = &bytes[count..],
        }
    }
    true
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct GatewayHealthResponse {
    status: String,
}

fn validate_gateway_health_response(response: &[u8]) -> Result<bool, ()> {
    let Some(header_end) = response.windows(4).position(|window| window == b"\r\n\r\n") else {
        return Ok(false);
    };
    let header = std::str::from_utf8(&response[..header_end]).map_err(|_| ())?;
    let mut lines = header.split("\r\n");
    if lines.next() != Some("HTTP/1.1 200 OK") {
        return Err(());
    }
    let mut headers = BTreeMap::new();
    for line in lines {
        let (name, value) = parse_header_field(line).map_err(|_| ())?;
        if headers.insert(name, value).is_some() {
            return Err(());
        }
    }
    if headers.contains_key("transfer-encoding") {
        return Err(());
    }
    let content_type = headers.get("content-type").ok_or(())?;
    if !matches!(
        content_type.to_ascii_lowercase().as_str(),
        "application/json" | "application/json; charset=utf-8"
    ) {
        return Err(());
    }
    let content_length = headers.get("content-length").ok_or(())?;
    if content_length.is_empty() || !content_length.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(());
    }
    let content_length = content_length.parse::<usize>().map_err(|_| ())?;
    let body_start = header_end.checked_add(4).ok_or(())?;
    let response_length = body_start
        .checked_add(content_length)
        .filter(|length| *length <= MAX_GATEWAY_HEALTH_RESPONSE_BYTES)
        .ok_or(())?;
    if response.len() < response_length {
        return Ok(false);
    }
    if response.len() != response_length {
        return Err(());
    }
    let health: GatewayHealthResponse =
        serde_json::from_slice(&response[body_start..]).map_err(|_| ())?;
    (health.status == "ok").then_some(true).ok_or(())
}

fn serve_connection(
    mut client: TcpStream,
    remote_addr: SocketAddr,
    gateway_route: GatewayRouteSource,
    api_handler: Option<Arc<dyn ManagementWebApiHandler>>,
    tls: Option<Arc<ServerConfig>>,
    static_web_root: Option<PathBuf>,
    running: Arc<AtomicBool>,
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
        return serve_http_connection(
            &mut stream,
            remote_addr,
            &gateway_route,
            api_handler,
            static_web_root.as_deref(),
            &running,
        );
    }
    serve_http_connection(
        &mut client,
        remote_addr,
        &gateway_route,
        api_handler,
        static_web_root.as_deref(),
        &running,
    )
}

fn serve_http_connection(
    client: &mut dyn ManagementWebStream,
    remote_addr: SocketAddr,
    gateway_route: &GatewayRouteSource,
    api_handler: Option<Arc<dyn ManagementWebApiHandler>>,
    static_web_root: Option<&Path>,
    running: &AtomicBool,
) -> Result<(), ManagementWebError> {
    let request = match read_request(client) {
        Ok(request) => request,
        Err(ManagementWebError::InvalidHttp) => {
            return write_response(
                client,
                "400 Bad Request",
                "application/json",
                r#"{"code":"MANAGEMENT_WEB_HTTP_INVALID"}"#,
            );
        }
        Err(ManagementWebError::RequestTooLarge) => {
            return write_response(
                client,
                "413 Content Too Large",
                "application/json",
                r#"{"code":"MANAGEMENT_WEB_REQUEST_TOO_LARGE"}"#,
            );
        }
        Err(error) => return Err(error),
    };
    let request_context = match parse_request(&request, remote_addr) {
        Ok(request) => request,
        Err(ManagementWebError::InvalidHttp) => {
            return write_response(
                client,
                "400 Bad Request",
                "application/json",
                r#"{"code":"MANAGEMENT_WEB_HTTP_INVALID"}"#,
            );
        }
        Err(error) => return Err(error),
    };
    if let Some(api_handler) = api_handler {
        if api_handler.handle(client, &request_context)? {
            return Ok(());
        }
    }
    let routed_path = request_path(&request_context.path);
    if routed_path == "/api" || routed_path.starts_with("/api/") {
        let Some(gateway_route) = gateway_route.snapshot() else {
            return write_gateway_unavailable(client);
        };
        return proxy_api(client, &gateway_route, &request, running);
    }
    if let Some(static_web_root) = static_web_root {
        return serve_static_web(client, &request_context, static_web_root);
    }
    match (
        request_context.method.as_str(),
        request_path(&request_context.path),
    ) {
        ("GET" | "HEAD", "/" | "/index.html") => write_bytes_response(
            client,
            "200 OK",
            "text/html; charset=utf-8",
            "no-cache",
            b"<!doctype html><title>CMClient</title><main id=app>CMClient management web</main>",
            request_context.method == "HEAD",
        ),
        (_, _) => write_response(
            client,
            "404 Not Found",
            "application/json",
            r#"{"code":"WEB_ROUTE_NOT_FOUND"}"#,
        ),
    }
}

fn serve_static_web(
    client: &mut dyn ManagementWebStream,
    request: &ManagementWebRequest,
    root: &Path,
) -> Result<(), ManagementWebError> {
    if request.method != "GET" && request.method != "HEAD" {
        return write_bytes_response(
            client,
            "405 Method Not Allowed",
            "application/json",
            "no-store",
            br#"{"code":"WEB_METHOD_NOT_ALLOWED"}"#,
            false,
        );
    }

    let path = request_path(&request.path);
    let relative_path = path.strip_prefix('/').unwrap_or(path);
    let requested_file = if relative_path.is_empty() {
        root.join("index.html")
    } else {
        root.join(relative_path)
    };
    if let Some(file) = readable_static_file(root, &requested_file) {
        return write_static_file(client, request, &file, path == "/index.html" || path == "/");
    }

    if looks_like_asset_path(path) {
        return write_bytes_response(
            client,
            "404 Not Found",
            "application/json",
            "no-store",
            br#"{"code":"WEB_ASSET_NOT_FOUND"}"#,
            request.method == "HEAD",
        );
    }

    let index = root.join("index.html");
    let Some(index) = readable_static_file(root, &index) else {
        return write_bytes_response(
            client,
            "404 Not Found",
            "application/json",
            "no-store",
            br#"{"code":"WEB_ROUTE_NOT_FOUND"}"#,
            request.method == "HEAD",
        );
    };
    write_static_file(client, request, &index, true)
}

fn readable_static_file(root: &Path, candidate: &Path) -> Option<PathBuf> {
    let candidate = std::fs::canonicalize(candidate).ok()?;
    (candidate.starts_with(root) && candidate.is_file()).then_some(candidate)
}

fn write_static_file(
    client: &mut dyn ManagementWebStream,
    request: &ManagementWebRequest,
    file: &Path,
    is_index: bool,
) -> Result<(), ManagementWebError> {
    let mut body = std::fs::File::open(file).map_err(|_| ManagementWebError::Io)?;
    let content_length = body.metadata().map_err(|_| ManagementWebError::Io)?.len();
    let content_type = static_content_type(file);
    let cache_control = if is_index {
        "no-cache"
    } else if has_vite_content_hash(file) {
        "public, max-age=31536000, immutable"
    } else {
        "no-cache"
    };
    write_response_header(
        client,
        "200 OK",
        content_type,
        cache_control,
        content_length,
    )?;
    if request.method != "HEAD" {
        io::copy(&mut body, client).map_err(|_| ManagementWebError::Io)?;
    }
    Ok(())
}

fn request_path(target: &str) -> &str {
    target.split_once('?').map_or(target, |(path, _)| path)
}

fn looks_like_asset_path(path: &str) -> bool {
    path == "/assets"
        || path.starts_with("/assets/")
        || Path::new(path)
            .file_name()
            .and_then(|name| Path::new(name).extension())
            .is_some()
}

fn has_vite_content_hash(path: &Path) -> bool {
    let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
        return false;
    };
    let bytes = stem.as_bytes();
    (8..=64.min(bytes.len().saturating_sub(1))).any(|hash_length| {
        let separator = bytes.len() - hash_length - 1;
        matches!(bytes[separator], b'-' | b'.')
            && bytes[separator + 1..]
                .iter()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(*byte, b'_' | b'-'))
    })
}

fn static_content_type(path: &Path) -> &'static str {
    match path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("html") => "text/html; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("js" | "mjs") => "text/javascript; charset=utf-8",
        Some("json" | "map") => "application/json; charset=utf-8",
        Some("webmanifest") => "application/manifest+json",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("jpg" | "jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("webp") => "image/webp",
        Some("avif") => "image/avif",
        Some("ico") => "image/x-icon",
        Some("woff") => "font/woff",
        Some("woff2") => "font/woff2",
        Some("ttf") => "font/ttf",
        Some("otf") => "font/otf",
        Some("wasm") => "application/wasm",
        Some("txt") => "text/plain; charset=utf-8",
        Some("xml") => "application/xml; charset=utf-8",
        _ => "application/octet-stream",
    }
}

fn proxy_api(
    client: &mut dyn ManagementWebStream,
    gateway: &ResolvedGatewayRoute,
    request: &[u8],
    running: &AtomicBool,
) -> Result<(), ManagementWebError> {
    // A dynamic session cannot be revoked while its capability is in flight. `clear` and `set`
    // take the lease's write lock, so after either returns every stale snapshot fails here.
    let Some(lease) = gateway.acquire() else {
        return write_gateway_unavailable(client);
    };
    let Some(deadline) = Instant::now().checked_add(GATEWAY_TIMEOUT) else {
        return write_gateway_unavailable(client);
    };
    let Some(connect_timeout) = remaining_until(deadline) else {
        return write_gateway_unavailable(client);
    };
    let mut upstream = match TcpStream::connect_timeout(&gateway.address, connect_timeout) {
        Ok(stream) => stream,
        Err(_) => return write_gateway_unavailable(client),
    };
    upstream
        .set_read_timeout(Some(PROXY_READ_POLL_INTERVAL))
        .map_err(|_| ManagementWebError::Io)?;
    let request = Zeroizing::new(with_gateway_headers(
        request,
        gateway
            .capability
            .as_ref()
            .map(|capability| capability.as_str()),
    )?);
    if !write_all_until(&mut upstream, request.as_slice(), deadline) {
        return write_gateway_unavailable(client);
    }
    drop(lease);
    copy_proxy_response(&mut upstream, client, running)
}

fn write_gateway_unavailable(
    client: &mut dyn ManagementWebStream,
) -> Result<(), ManagementWebError> {
    write_response(
        client,
        "503 Service Unavailable",
        "application/json",
        r#"{"code":"GATEWAY_PROXY_UNAVAILABLE"}"#,
    )
}

fn copy_proxy_response(
    upstream: &mut TcpStream,
    client: &mut dyn ManagementWebStream,
    running: &AtomicBool,
) -> Result<(), ManagementWebError> {
    let mut buffer = [0_u8; 16 * 1024];
    while running.load(Ordering::Acquire) {
        match upstream.read(&mut buffer) {
            Ok(0) => return Ok(()),
            Ok(count) => client
                .write_all(&buffer[..count])
                .map_err(|_| ManagementWebError::Io)?,
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
                ) => {}
            Err(_) => return Err(ManagementWebError::Io),
        }
    }
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
        .split("\r\n")
        .next()
        .ok_or(ManagementWebError::InvalidHttp)?;
    let mut parts = line.split_whitespace();
    let method = parts.next().ok_or(ManagementWebError::InvalidHttp)?;
    let path = parts.next().ok_or(ManagementWebError::InvalidHttp)?;
    let version = parts.next().ok_or(ManagementWebError::InvalidHttp)?;
    if parts.next().is_some() || !version.starts_with("HTTP/") || !path.starts_with('/') {
        return Err(ManagementWebError::InvalidHttp);
    }
    validate_request_target(path)?;
    let mut headers = BTreeMap::new();
    for line in header.split("\r\n").skip(1) {
        let (name, value) = parse_header_field(line)?;
        if headers.insert(name, value.to_owned()).is_some() {
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

fn validate_request_target(target: &str) -> Result<(), ManagementWebError> {
    if target.contains('#') {
        return Err(ManagementWebError::InvalidHttp);
    }
    let (path, query) = target
        .split_once('?')
        .map_or((target, None), |(path, query)| (path, Some(query)));
    if path.is_empty() || path.contains('\\') {
        return Err(ManagementWebError::InvalidHttp);
    }
    let decoded_path = percent_decode_component(path, true)?;
    let decoded_path =
        std::str::from_utf8(&decoded_path).map_err(|_| ManagementWebError::InvalidHttp)?;
    if decoded_path.starts_with("//")
        || decoded_path
            .split('/')
            .any(|segment| segment == "." || segment == "..")
    {
        return Err(ManagementWebError::InvalidHttp);
    }
    if let Some(query) = query {
        percent_decode_component(query, false)?;
    }
    Ok(())
}

fn percent_decode_component(
    component: &str,
    reject_encoded_separator: bool,
) -> Result<Vec<u8>, ManagementWebError> {
    let bytes = component.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        let decoded_byte = if byte == b'%' {
            let high = bytes
                .get(index + 1)
                .and_then(|byte| hex_value(*byte))
                .ok_or(ManagementWebError::InvalidHttp)?;
            let low = bytes
                .get(index + 2)
                .and_then(|byte| hex_value(*byte))
                .ok_or(ManagementWebError::InvalidHttp)?;
            index += 3;
            let decoded = high * 16 + low;
            if reject_encoded_separator && matches!(decoded, b'/' | b'\\') {
                return Err(ManagementWebError::InvalidHttp);
            }
            decoded
        } else {
            index += 1;
            byte
        };
        if decoded_byte == 0 || decoded_byte.is_ascii_control() {
            return Err(ManagementWebError::InvalidHttp);
        }
        decoded.push(decoded_byte);
    }
    Ok(decoded)
}

const fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn content_length(header: &[u8]) -> Result<usize, ManagementWebError> {
    let header = std::str::from_utf8(header).map_err(|_| ManagementWebError::InvalidHttp)?;
    let header = header
        .strip_suffix("\r\n\r\n")
        .ok_or(ManagementWebError::InvalidHttp)?;
    let mut length = None;
    for line in header.split("\r\n").skip(1) {
        let (name, value) = parse_header_field(line)?;
        if name == "transfer-encoding" {
            return Err(ManagementWebError::InvalidHttp);
        }
        if name == "content-length" {
            if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(ManagementWebError::InvalidHttp);
            }
            let parsed = value
                .parse::<usize>()
                .map_err(|_| ManagementWebError::InvalidHttp);
            if length.replace(parsed?).is_some() {
                return Err(ManagementWebError::InvalidHttp);
            }
        }
    }
    Ok(length.unwrap_or(0))
}

fn with_gateway_headers(
    request: &[u8],
    gateway_capability: Option<&str>,
) -> Result<Vec<u8>, ManagementWebError> {
    let header_end = header_end(request).ok_or(ManagementWebError::InvalidHttp)?;
    let header =
        std::str::from_utf8(&request[..header_end]).map_err(|_| ManagementWebError::InvalidHttp)?;
    let header = header
        .strip_suffix("\r\n\r\n")
        .ok_or(ManagementWebError::InvalidHttp)?;
    let mut rewritten = String::new();
    for (index, line) in header.split("\r\n").enumerate() {
        if index > 0 {
            let (name, _) = parse_header_field(line)?;
            if matches!(
                name.as_str(),
                "connection" | "proxy-connection" | GATEWAY_CAPABILITY_HEADER
            ) {
                continue;
            }
        }
        rewritten.push_str(line);
        rewritten.push_str("\r\n");
    }
    rewritten.push_str("connection: close\r\n\r\n");
    if let Some(capability) = gateway_capability {
        if !valid_gateway_capability(capability) {
            return Err(ManagementWebError::InvalidHttp);
        }
        let insertion = rewritten.len() - 2;
        rewritten.insert_str(
            insertion,
            &format!("{GATEWAY_CAPABILITY_HEADER}: {capability}\r\n"),
        );
    }
    let mut rewritten = rewritten.into_bytes();
    rewritten.extend_from_slice(&request[header_end..]);
    Ok(rewritten)
}

fn valid_gateway_capability(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn canonical_header_name(name: &str) -> Result<String, ManagementWebError> {
    if name.is_empty()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return Err(ManagementWebError::InvalidHttp);
    }
    Ok(name.to_ascii_lowercase())
}

fn parse_header_field(line: &str) -> Result<(String, &str), ManagementWebError> {
    let (name, value) = line
        .split_once(':')
        .ok_or(ManagementWebError::InvalidHttp)?;
    let name = canonical_header_name(name)?;
    if value
        .bytes()
        .any(|byte| byte == b'\r' || byte == b'\n' || (byte.is_ascii_control() && byte != b'\t'))
    {
        return Err(ManagementWebError::InvalidHttp);
    }
    Ok((name, value.trim()))
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
    write_bytes_response(
        stream,
        status,
        content_type,
        "no-store",
        body.as_bytes(),
        false,
    )
}

fn write_bytes_response(
    stream: &mut dyn ManagementWebStream,
    status: &str,
    content_type: &str,
    cache_control: &str,
    body: &[u8],
    head_only: bool,
) -> Result<(), ManagementWebError> {
    write_response_header(
        stream,
        status,
        content_type,
        cache_control,
        body.len() as u64,
    )?;
    if !head_only {
        stream.write_all(body).map_err(|_| ManagementWebError::Io)?;
    }
    Ok(())
}

fn write_response_header(
    stream: &mut dyn ManagementWebStream,
    status: &str,
    content_type: &str,
    cache_control: &str,
    content_length: u64,
) -> Result<(), ManagementWebError> {
    let header = format!(
        "HTTP/1.1 {status}\r\ncontent-type: {content_type}\r\ncache-control: {cache_control}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        content_length
    );
    stream
        .write_all(header.as_bytes())
        .map_err(|_| ManagementWebError::Io)
}

fn load_tls_config(config: &ManagementTlsConfig) -> Result<Arc<ServerConfig>, ManagementWebError> {
    let certificates = CertificateDer::pem_file_iter(&config.certificate_path)
        .map_err(|_| ManagementWebError::TlsConfiguration)?;
    let certificates = certificates
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| ManagementWebError::TlsConfiguration)?;
    if certificates.is_empty() {
        return Err(ManagementWebError::TlsConfiguration);
    }
    let key = PrivateKeyDer::from_pem_file(&config.private_key_path)
        .map_err(|_| ManagementWebError::TlsConfiguration)?;
    ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certificates, key)
        .map(Arc::new)
        .map_err(|_| ManagementWebError::TlsConfiguration)
}

#[cfg(test)]
mod tests {
    use super::{
        ActiveConnectionRegistry, ConnectionRegistrationError, GatewayRoute, GatewaySessionHandle,
        ManagementWebApiHandler, ManagementWebConfig, ManagementWebError, ManagementWebListener,
        ManagementWebService, ManagementWebStream, gateway_health, gateway_health_with_route,
        with_gateway_headers,
    };

    #[test]
    fn strips_client_gateway_capabilities_and_injects_one_agent_value() {
        let capability = "a".repeat(64);
        let request = b"POST /api/v1/jobs HTTP/1.1\r\nhost: localhost\r\nX-CMClient-Gateway-Capability: spoofed\r\nconnection: keep-alive\r\ncontent-length: 4\r\n\r\nbody";
        let rewritten =
            with_gateway_headers(request, Some(&capability)).expect("request should rewrite");
        let rewritten = String::from_utf8(rewritten).expect("request should remain UTF-8");

        assert!(!rewritten.contains("spoofed"));
        assert!(!rewritten.contains("keep-alive"));
        assert_eq!(
            rewritten.matches("x-cmclient-gateway-capability:").count(),
            1
        );
        assert!(rewritten.contains(&format!("x-cmclient-gateway-capability: {capability}\r\n")));
        assert!(rewritten.ends_with("\r\n\r\nbody"));
        assert!(with_gateway_headers(request, Some(&"A".repeat(64))).is_err());
    }

    #[test]
    fn rejects_whitespace_obfuscated_gateway_capability_headers_consistently() {
        let remote = "127.0.0.1:12345"
            .parse()
            .expect("remote address should parse");
        for request in [
            b"GET /api/v1/system/health HTTP/1.1\r\n X-CMClient-Gateway-Capability: spoofed\r\nhost: localhost\r\n\r\n".as_slice(),
            b"GET /api/v1/system/health HTTP/1.1\r\nX-CMClient-Gateway-Capability : spoofed\r\nhost: localhost\r\n\r\n".as_slice(),
            b"POST /api/v1/jobs HTTP/1.1\r\nhost: localhost\r\n transfer-encoding: chunked\r\n\r\n".as_slice(),
            b"GET /api/v1/system/health HTTP/1.1\r\nhost: localhost\nX-CMClient-Gateway-Capability: spoofed\r\n\r\n".as_slice(),
        ] {
            assert!(matches!(
                super::parse_request(request, remote),
                Err(ManagementWebError::InvalidHttp)
            ));
            assert!(matches!(
                with_gateway_headers(request, Some(&"a".repeat(64))),
                Err(ManagementWebError::InvalidHttp)
            ));
            let header_end = super::header_end(request).expect("request should have a header");
            assert!(matches!(
                super::content_length(&request[..header_end]),
                Err(ManagementWebError::InvalidHttp)
            ));
        }
    }
    use std::{
        fs,
        io::{Cursor, Read, Write},
        net::{SocketAddr, TcpListener, TcpStream},
        path::{Path, PathBuf},
        sync::{Arc, atomic::AtomicBool, mpsc},
        thread,
        time::{Duration, Instant},
    };

    struct AgentRoute;

    struct RejectEveryRequest;

    struct StaticWebFixture {
        root: PathBuf,
    }

    impl StaticWebFixture {
        fn new() -> Self {
            let root = std::env::temp_dir()
                .join(format!("cmclient-management-web-{}", uuid::Uuid::new_v4()));
            fs::create_dir_all(root.join("assets")).expect("fixture directories should exist");
            fs::write(
                root.join("index.html"),
                b"<!doctype html><main id=app>production bundle</main>",
            )
            .expect("index should write");
            fs::write(
                root.join("assets/app-BWWK_6zJ.js"),
                b"globalThis.cmclient=true;",
            )
            .expect("JavaScript asset should write");
            fs::write(root.join("assets/app-a1b2c3d4.css"), b"#app{display:block}")
                .expect("CSS asset should write");
            fs::write(root.join("favicon.ico"), [0_u8, 1, 2, 3]).expect("icon should write");
            Self { root }
        }
    }

    impl Drop for StaticWebFixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    fn static_response(root: &Path, request: &[u8]) -> Vec<u8> {
        static_response_with_handler(root, request, None)
    }

    fn static_response_with_handler(
        root: &Path,
        request: &[u8],
        handler: Option<Arc<dyn ManagementWebApiHandler>>,
    ) -> Vec<u8> {
        let config = ManagementWebConfig {
            port: 0,
            static_web_root: Some(root.to_owned()),
            ..Default::default()
        };
        let listener = match handler {
            Some(handler) => ManagementWebListener::bind_with_api_handler(&config, handler),
            None => ManagementWebListener::bind(&config),
        }
        .expect("management listener should bind");
        let address = listener.local_addr().expect("listener address should load");
        let server = thread::spawn(move || listener.serve_once());
        let mut client = TcpStream::connect(address).expect("client should connect");
        client.write_all(request).expect("request should write");
        let mut response = Vec::new();
        client
            .read_to_end(&mut response)
            .expect("response should read");
        server
            .join()
            .expect("server should join")
            .expect("server should respond");
        response
    }

    fn response_body(response: &[u8]) -> &[u8] {
        let body_start = response
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .expect("response should have a header")
            + 4;
        &response[body_start..]
    }

    fn send_request(address: SocketAddr, request: &[u8]) -> String {
        let mut client = TcpStream::connect(address).expect("client should connect");
        client.write_all(request).expect("request should write");
        let mut response = String::new();
        client
            .read_to_string(&mut response)
            .expect("response should read");
        response
    }

    fn spawn_gateway(
        expected_capability: String,
        response_body: &'static str,
    ) -> (SocketAddr, thread::JoinHandle<()>) {
        let gateway = TcpListener::bind("127.0.0.1:0").expect("gateway should bind");
        let address = gateway.local_addr().expect("gateway address should load");
        let worker = thread::spawn(move || {
            let (mut stream, _) = gateway.accept().expect("gateway should accept");
            let mut request = [0_u8; 4096];
            let count = stream.read(&mut request).expect("request should read");
            let request = String::from_utf8(request[..count].to_vec())
                .expect("gateway request should be UTF-8");
            assert!(!request.contains("spoofed"));
            assert_eq!(request.matches("x-cmclient-gateway-capability:").count(), 1);
            assert!(request.contains(&format!(
                "x-cmclient-gateway-capability: {expected_capability}\r\n"
            )));
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{response_body}",
                response_body.len()
            );
            stream
                .write_all(response.as_bytes())
                .expect("gateway response should write");
        });
        (address, worker)
    }

    fn tcp_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("pair listener should bind");
        let client = TcpStream::connect(listener.local_addr().expect("pair address should load"))
            .expect("pair client should connect");
        let (server, _) = listener.accept().expect("pair server should accept");
        (client, server)
    }

    fn wait_for_active_count(registry: &ActiveConnectionRegistry, expected: usize) {
        let deadline = Instant::now() + Duration::from_secs(1);
        while registry.active_count() != expected {
            assert!(
                Instant::now() < deadline,
                "active connection count did not become {expected}"
            );
            thread::yield_now();
        }
    }

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

    impl ManagementWebApiHandler for RejectEveryRequest {
        fn handle(
            &self,
            client: &mut dyn ManagementWebStream,
            _request: &super::ManagementWebRequest,
        ) -> Result<bool, ManagementWebError> {
            super::write_response(
                client,
                "401 Unauthorized",
                "application/json",
                r#"{"code":"AUTH_REQUIRED"}"#,
            )?;
            Ok(true)
        }
    }

    #[test]
    fn gateway_routes_are_loopback_capability_bound_and_redacted() {
        let capability = "a".repeat(64);
        let route = GatewayRoute::new(
            "127.0.0.1:4810".parse().expect("address should parse"),
            capability.clone(),
        )
        .expect("valid route should construct");
        assert_eq!(route.address().to_string(), "127.0.0.1:4810");
        let active = route.active().expect("route should be active");
        assert_eq!(active.capability(), capability);
        drop(active);
        assert!(!format!("{route:?}").contains(&capability));

        let session = GatewaySessionHandle::with_route(route.clone());
        assert_eq!(session.snapshot(), Some(route));
        assert!(!format!("{session:?}").contains(&capability));
        let stale = session.snapshot().expect("route should snapshot");
        session.clear();
        assert!(session.snapshot().is_none());
        assert!(!stale.is_active());
        assert!(stale.active().is_none());

        assert!(matches!(
            GatewayRoute::new(
                "192.0.2.1:4810".parse().expect("address should parse"),
                "b".repeat(64),
            ),
            Err(ManagementWebError::InvalidHttp)
        ));
        assert!(matches!(
            GatewayRoute::new(
                "127.0.0.1:4810".parse().expect("address should parse"),
                "B".repeat(64),
            ),
            Err(ManagementWebError::InvalidHttp)
        ));
        assert!(matches!(
            GatewayRoute::new(
                "127.0.0.1:0".parse().expect("address should parse"),
                "c".repeat(64),
            ),
            Err(ManagementWebError::InvalidHttp)
        ));
        assert!(matches!(
            GatewayRoute::new(
                "[::1]:4810".parse().expect("address should parse"),
                "d".repeat(64),
            ),
            Err(ManagementWebError::InvalidHttp)
        ));
    }

    #[test]
    fn session_rotation_revokes_old_snapshots_and_waits_for_capability_users() {
        let first = GatewayRoute::new(
            "127.0.0.1:4810".parse().expect("address should parse"),
            "1".repeat(64),
        )
        .expect("first route should construct");
        let session = GatewaySessionHandle::with_route(first);
        let stale = session.snapshot().expect("first route should snapshot");
        let active = stale.active().expect("first route should be active");
        let (started_sender, started_receiver) = mpsc::sync_channel(1);
        let (cleared_sender, cleared_receiver) = mpsc::sync_channel(1);
        let clear_session = session.clone();
        let clearer = thread::spawn(move || {
            started_sender.send(()).expect("start signal should send");
            clear_session.clear();
            cleared_sender.send(()).expect("clear signal should send");
        });
        started_receiver.recv().expect("clear should start");
        assert!(
            cleared_receiver
                .recv_timeout(Duration::from_millis(50))
                .is_err(),
            "clear must wait until the capability write lease is released"
        );
        drop(active);
        cleared_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("clear should finish after the lease is released");
        clearer.join().expect("clear thread should join");
        assert!(!stale.is_active());

        let second = GatewayRoute::new(
            "127.0.0.1:4811".parse().expect("address should parse"),
            "2".repeat(64),
        )
        .expect("second route should construct");
        session.set(second);
        let rotated = session.snapshot().expect("second route should snapshot");
        assert_eq!(rotated.address().port(), 4811);
        assert!(rotated.is_active());
        session.set(
            GatewayRoute::new(
                "127.0.0.1:4812".parse().expect("address should parse"),
                "4".repeat(64),
            )
            .expect("third route should construct"),
        );
        assert!(!rotated.is_active());
        assert!(
            session
                .snapshot()
                .is_some_and(|route| route.address().port() == 4812 && route.is_active())
        );
    }

    #[test]
    fn stale_dynamic_proxy_snapshot_never_sends_capability_to_reused_port() {
        let original = TcpListener::bind("127.0.0.1:0").expect("original port should bind");
        let address = original.local_addr().expect("original address should load");
        let route = GatewayRoute::new(address, "3".repeat(64)).expect("route should construct");
        let session = GatewaySessionHandle::with_route(route.clone());
        let stale = super::GatewayRouteSource::dynamic(session.clone())
            .snapshot()
            .expect("route should resolve before clear");

        drop(original);
        session.clear();
        let reused = TcpListener::bind(address).expect("cleared port should be reusable");
        reused
            .set_nonblocking(true)
            .expect("reused listener should be nonblocking");
        assert!(!gateway_health_with_route(&route));
        let mut response = Cursor::new(Vec::new());
        super::proxy_api(
            &mut response,
            &stale,
            b"GET /api/v1/system/health HTTP/1.1\r\nhost: localhost\r\n\r\n",
            &AtomicBool::new(true),
        )
        .expect("stale proxy should fail closed");
        assert!(
            String::from_utf8(response.into_inner())
                .expect("response should be UTF-8")
                .starts_with("HTTP/1.1 503 Service Unavailable")
        );
        assert!(matches!(
            reused.accept(),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock
        ));
    }

    #[test]
    fn dynamic_gateway_session_rotates_without_restarting_management_web() {
        let session = GatewaySessionHandle::new();
        let service = ManagementWebService::start_with_api_handler_and_gateway_session(
            &ManagementWebConfig {
                port: 0,
                gateway: "192.0.2.1:9".parse().expect("address should parse"),
                gateway_capability: Some(String::from("ignored-static-value")),
                ..Default::default()
            },
            Arc::new(AgentRoute),
            session.clone(),
        )
        .expect("dynamic management service should start");
        let management_address = service.local_addr();

        let agent_response = send_request(
            management_address,
            b"GET /api/v1/updates HTTP/1.1\r\nhost: localhost\r\n\r\n",
        );
        assert!(agent_response.starts_with("HTTP/1.1 200 OK"));
        assert!(agent_response.contains("{\"schemaVersion\":1}"));

        let unavailable = send_request(
            management_address,
            b"GET /api/v1/system/health HTTP/1.1\r\nhost: localhost\r\n\r\n",
        );
        assert!(unavailable.starts_with("HTTP/1.1 503 Service Unavailable"));
        assert_eq!(
            response_body(unavailable.as_bytes()),
            br#"{"code":"GATEWAY_PROXY_UNAVAILABLE"}"#
        );

        let first_capability = "1".repeat(64);
        let (first_address, first_gateway) =
            spawn_gateway(first_capability.clone(), r#"{"generation":1}"#);
        session.set(
            GatewayRoute::new(first_address, first_capability)
                .expect("first route should construct"),
        );
        let first = send_request(
            management_address,
            b"GET /api/v1/system/health HTTP/1.1\r\nhost: localhost\r\nX-CMClient-Gateway-Capability: spoofed\r\n\r\n",
        );
        assert!(first.starts_with("HTTP/1.1 200 OK"));
        assert!(first.ends_with(r#"{"generation":1}"#));
        first_gateway.join().expect("first gateway should join");

        session.clear();
        let unavailable = send_request(
            management_address,
            b"GET /api/v1/system/health HTTP/1.1\r\nhost: localhost\r\n\r\n",
        );
        assert!(unavailable.starts_with("HTTP/1.1 503 Service Unavailable"));

        let second_capability = "2".repeat(64);
        let (second_address, second_gateway) =
            spawn_gateway(second_capability.clone(), r#"{"generation":2}"#);
        session.set(
            GatewayRoute::new(second_address, second_capability)
                .expect("second route should construct"),
        );
        let second = send_request(
            management_address,
            b"GET /api/v1/system/health HTTP/1.1\r\nhost: localhost\r\nX-CMClient-Gateway-Capability: spoofed\r\n\r\n",
        );
        assert!(second.starts_with("HTTP/1.1 200 OK"));
        assert!(second.ends_with(r#"{"generation":2}"#));
        assert_eq!(service.local_addr(), management_address);
        second_gateway.join().expect("second gateway should join");
        service.stop().expect("management service should stop");
    }

    #[test]
    fn gateway_health_with_route_authenticates_with_the_exact_capability() {
        let capability = "3".repeat(64);
        let (address, gateway) = spawn_gateway(capability.clone(), r#"{"status":"ok"}"#);
        let route = GatewayRoute::new(address, capability).expect("route should construct");

        assert!(gateway_health_with_route(&route));
        gateway.join().expect("gateway should join");
    }

    #[test]
    fn gateway_health_requires_an_exact_bounded_http_json_response() {
        let valid = b"HTTP/1.1 200 OK\r\ncontent-type: application/json; charset=utf-8\r\ncontent-length: 15\r\nconnection: close\r\n\r\n{\"status\":\"ok\"}";
        assert_eq!(super::validate_gateway_health_response(valid), Ok(true));

        for body in [
            r#"{"status":"bad","message":"{\"status\":\"ok\"}"}"#,
            r#"{"status":"ok","extra":true}"#,
        ] {
            let invalid = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
                body.len()
            );
            assert_eq!(
                super::validate_gateway_health_response(invalid.as_bytes()),
                Err(())
            );
        }
        for invalid in [
            b"HTTP/1.0 200 OK\r\ncontent-type: application/json\r\ncontent-length: 15\r\n\r\n{\"status\":\"ok\"}".as_slice(),
            b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ntransfer-encoding: chunked\r\n\r\nf\r\n{\"status\":\"ok\"}\r\n0\r\n\r\n".as_slice(),
            b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 5000\r\n\r\n".as_slice(),
            b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 15\r\n\r\n{\"status\":\"ok\"}trailing".as_slice(),
        ] {
            assert_eq!(super::validate_gateway_health_response(invalid), Err(()));
        }
        assert_eq!(
            super::validate_gateway_health_response(
                b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 15\r\n\r\n{\"status\":"
            ),
            Ok(false)
        );
    }

    #[test]
    fn gateway_health_uses_one_total_deadline_against_drip_responses() {
        let gateway = TcpListener::bind("127.0.0.1:0").expect("gateway should bind");
        let address = gateway.local_addr().expect("gateway address should load");
        let worker = thread::spawn(move || {
            let (mut stream, _) = gateway.accept().expect("gateway should accept");
            let mut request = [0_u8; 1024];
            let _ = stream
                .read(&mut request)
                .expect("health request should read");
            for _ in 0..30 {
                if stream.write_all(b"H").is_err() {
                    break;
                }
                thread::sleep(Duration::from_millis(10));
            }
        });
        let started = Instant::now();
        assert!(!super::gateway_health_request(
            address,
            None,
            Duration::from_millis(80)
        ));
        assert!(
            started.elapsed() < Duration::from_millis(250),
            "health probe exceeded its single total deadline"
        );
        worker.join().expect("gateway should join");
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
    fn serves_vite_bundle_files_with_mime_and_cache_headers() {
        let fixture = StaticWebFixture::new();
        let index = static_response(&fixture.root, b"GET / HTTP/1.1\r\nhost: localhost\r\n\r\n");
        let index = String::from_utf8(index).expect("index response should be UTF-8");
        assert!(index.starts_with("HTTP/1.1 200 OK"));
        assert!(index.contains("content-type: text/html; charset=utf-8\r\n"));
        assert!(index.contains("cache-control: no-cache\r\n"));
        assert!(index.ends_with("<!doctype html><main id=app>production bundle</main>"));

        let javascript = static_response(
            &fixture.root,
            b"GET /assets/app-BWWK_6zJ.js?v=1 HTTP/1.1\r\nhost: localhost\r\n\r\n",
        );
        let javascript =
            String::from_utf8(javascript).expect("JavaScript response should be UTF-8");
        assert!(javascript.starts_with("HTTP/1.1 200 OK"));
        assert!(javascript.contains("content-type: text/javascript; charset=utf-8\r\n"));
        assert!(javascript.contains("cache-control: public, max-age=31536000, immutable\r\n"));
        assert!(javascript.ends_with("globalThis.cmclient=true;"));

        let css = static_response(
            &fixture.root,
            b"GET /assets/app-a1b2c3d4.css HTTP/1.1\r\nhost: localhost\r\n\r\n",
        );
        let css = String::from_utf8(css).expect("CSS response should be UTF-8");
        assert!(css.contains("content-type: text/css; charset=utf-8\r\n"));
        assert!(css.contains("cache-control: public, max-age=31536000, immutable\r\n"));

        let icon = static_response(
            &fixture.root,
            b"GET /favicon.ico HTTP/1.1\r\nhost: localhost\r\n\r\n",
        );
        assert!(icon.starts_with(b"HTTP/1.1 200 OK"));
        assert!(
            icon.windows(b"content-type: image/x-icon\r\n".len())
                .any(|window| window == b"content-type: image/x-icon\r\n")
        );
        assert!(
            icon.windows(b"cache-control: no-cache\r\n".len())
                .any(|window| window == b"cache-control: no-cache\r\n")
        );
        assert_eq!(response_body(&icon), [0_u8, 1, 2, 3]);
    }

    #[test]
    fn recognizes_common_vite_asset_content_types() {
        for (file, expected) in [
            ("app.mjs", "text/javascript; charset=utf-8"),
            ("data.json", "application/json; charset=utf-8"),
            ("logo.svg", "image/svg+xml"),
            ("logo.png", "image/png"),
            ("photo.webp", "image/webp"),
            ("font.woff2", "font/woff2"),
            ("module.wasm", "application/wasm"),
            ("site.webmanifest", "application/manifest+json"),
            ("opaque.bin", "application/octet-stream"),
        ] {
            assert_eq!(super::static_content_type(Path::new(file)), expected);
        }
    }

    #[test]
    fn rejects_a_configured_static_root_that_does_not_exist() {
        let missing =
            std::env::temp_dir().join(format!("cmclient-web-missing-{}", uuid::Uuid::new_v4()));
        let result = ManagementWebListener::bind(&ManagementWebConfig {
            port: 0,
            static_web_root: Some(missing),
            ..Default::default()
        });
        assert!(matches!(result, Err(ManagementWebError::Io)));
    }

    #[test]
    fn head_reports_static_length_without_sending_a_body() {
        let fixture = StaticWebFixture::new();
        let response = static_response(
            &fixture.root,
            b"HEAD /assets/app-BWWK_6zJ.js HTTP/1.1\r\nhost: localhost\r\n\r\n",
        );
        let header = String::from_utf8(response.clone()).expect("HEAD response should be UTF-8");
        assert!(header.starts_with("HTTP/1.1 200 OK"));
        assert!(header.contains("content-length: 25\r\n"));
        assert!(response_body(&response).is_empty());
    }

    #[test]
    fn falls_back_to_index_for_spa_routes_but_not_missing_assets() {
        let fixture = StaticWebFixture::new();
        let route = static_response(
            &fixture.root,
            b"GET /nodes/meshtastic-1?tab=telemetry HTTP/1.1\r\nhost: localhost\r\n\r\n",
        );
        let route = String::from_utf8(route).expect("route response should be UTF-8");
        assert!(route.starts_with("HTTP/1.1 200 OK"));
        assert!(route.contains("cache-control: no-cache\r\n"));
        assert!(route.ends_with("<!doctype html><main id=app>production bundle</main>"));

        for missing in [
            b"GET /assets/missing.js HTTP/1.1\r\nhost: localhost\r\n\r\n".as_slice(),
            b"GET /missing.css HTTP/1.1\r\nhost: localhost\r\n\r\n".as_slice(),
        ] {
            let response = static_response(&fixture.root, missing);
            let response = String::from_utf8(response).expect("error response should be UTF-8");
            assert!(response.starts_with("HTTP/1.1 404 Not Found"));
            assert!(response.contains("WEB_ASSET_NOT_FOUND"));
            assert!(!response.contains("production bundle"));
        }
    }

    #[test]
    fn rejects_malformed_and_traversal_request_targets() {
        let fixture = StaticWebFixture::new();
        for request in [
            b"GET /../secret HTTP/1.1\r\nhost: localhost\r\n\r\n".as_slice(),
            b"GET /%2e%2e/secret HTTP/1.1\r\nhost: localhost\r\n\r\n".as_slice(),
            b"GET /assets%2fapp-BWWK_6zJ.js HTTP/1.1\r\nhost: localhost\r\n\r\n".as_slice(),
            b"GET /assets/app.js%00 HTTP/1.1\r\nhost: localhost\r\n\r\n".as_slice(),
            b"GET /assets/app.js?value=%ZZ HTTP/1.1\r\nhost: localhost\r\n\r\n".as_slice(),
            b"GET //outside HTTP/1.1\r\nhost: localhost\r\n\r\n".as_slice(),
            b"GET /assets\\outside.js HTTP/1.1\r\nhost: localhost\r\n\r\n".as_slice(),
        ] {
            let response = static_response(&fixture.root, request);
            let response = String::from_utf8(response).expect("error response should be UTF-8");
            assert!(response.starts_with("HTTP/1.1 400 Bad Request"));
            assert!(response.contains("MANAGEMENT_WEB_HTTP_INVALID"));
            assert!(!response.contains("production bundle"));
        }
    }

    #[cfg(unix)]
    #[test]
    fn refuses_static_symlinks_that_escape_the_bundle_root() {
        use std::os::unix::fs::symlink;

        let fixture = StaticWebFixture::new();
        let outside = fixture.root.with_extension("secret.txt");
        fs::write(&outside, b"must not escape").expect("outside file should write");
        symlink(&outside, fixture.root.join("assets/leak.txt")).expect("symlink should create");

        let response = static_response(
            &fixture.root,
            b"GET /assets/leak.txt HTTP/1.1\r\nhost: localhost\r\n\r\n",
        );
        let response = String::from_utf8(response).expect("error response should be UTF-8");
        assert!(response.starts_with("HTTP/1.1 404 Not Found"));
        assert!(!response.contains("must not escape"));
        fs::remove_file(outside).expect("outside file should remove");
    }

    #[test]
    fn runs_access_handler_before_static_file_serving() {
        let fixture = StaticWebFixture::new();
        let response = static_response_with_handler(
            &fixture.root,
            b"GET / HTTP/1.1\r\nhost: localhost\r\n\r\n",
            Some(Arc::new(RejectEveryRequest)),
        );
        let response = String::from_utf8(response).expect("auth response should be UTF-8");
        assert!(response.starts_with("HTTP/1.1 401 Unauthorized"));
        assert!(response.contains("AUTH_REQUIRED"));
        assert!(!response.contains("production bundle"));
    }

    #[test]
    fn retains_minimal_shell_when_static_root_is_not_configured() {
        let listener = ManagementWebListener::bind(&ManagementWebConfig {
            port: 0,
            ..Default::default()
        })
        .expect("listener should bind");
        let address = listener.local_addr().expect("address should load");
        let server = thread::spawn(move || listener.serve_once());
        let mut client = TcpStream::connect(address).expect("client should connect");
        client
            .write_all(b"GET / HTTP/1.1\r\nhost: localhost\r\n\r\n")
            .expect("request should write");
        let mut response = String::new();
        client
            .read_to_string(&mut response)
            .expect("response should read");
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("CMClient management web"));
        server
            .join()
            .expect("server should join")
            .expect("server should respond");
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
    fn proxies_api_requests_before_considering_spa_fallback() {
        let fixture = StaticWebFixture::new();
        let gateway = TcpListener::bind("127.0.0.1:0").expect("gateway should bind");
        let gateway_address = gateway.local_addr().expect("gateway address should load");
        let gateway_thread = thread::spawn(move || {
            let (mut stream, _) = gateway.accept().expect("gateway should accept");
            let mut request = [0_u8; 4096];
            let count = stream
                .read(&mut request)
                .expect("gateway request should read");
            let request = &request[..count];
            assert!(request.starts_with(b"GET /api/v1/nodes?limit=1 HTTP/1.1\r\n"));
            assert!(
                request
                    .windows(b"connection: close\r\n".len())
                    .any(|window| window == b"connection: close\r\n")
            );
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 12\r\nconnection: close\r\n\r\n{\"nodes\":[]}",
                )
                .expect("gateway response should write");
        });
        let listener = ManagementWebListener::bind(&ManagementWebConfig {
            port: 0,
            gateway: gateway_address,
            static_web_root: Some(fixture.root.clone()),
            ..Default::default()
        })
        .expect("listener should bind");
        let address = listener.local_addr().expect("address should load");
        let server = thread::spawn(move || listener.serve_once());
        let mut client = TcpStream::connect(address).expect("client should connect");
        client
            .write_all(b"GET /api/v1/nodes?limit=1 HTTP/1.1\r\nhost: localhost\r\n\r\n")
            .expect("request should write");
        let mut response = String::new();
        client
            .read_to_string(&mut response)
            .expect("response should read");
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.ends_with("{\"nodes\":[]}"));
        assert!(!response.contains("production bundle"));
        server
            .join()
            .expect("server should join")
            .expect("server should proxy");
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
    fn active_connection_registry_enforces_and_releases_slots() {
        let registry = Arc::new(ActiveConnectionRegistry::new(1));
        let (_first_client, first_server) = tcp_pair();
        let first_slot = registry
            .try_register(&first_server)
            .expect("first connection should reserve the only slot");
        assert_eq!(registry.active_count(), 1);

        let (_second_client, second_server) = tcp_pair();
        assert!(matches!(
            registry.try_register(&second_server),
            Err(ConnectionRegistrationError::Full)
        ));
        assert_eq!(registry.active_count(), 1);

        drop(first_slot);
        assert_eq!(registry.active_count(), 0);
        let second_slot = registry
            .try_register(&second_server)
            .expect("released capacity should be reusable");
        assert_eq!(registry.active_count(), 1);
        drop(second_slot);
        assert_eq!(registry.active_count(), 0);
    }

    #[test]
    fn service_rejects_excess_connections_with_a_stable_response() {
        let listener = ManagementWebListener::bind(&ManagementWebConfig {
            port: 0,
            ..Default::default()
        })
        .expect("listener should bind");
        let service = ManagementWebService::start_listener_with_connection_limit(listener, 1)
            .expect("service should start");
        let address = service.local_addr();

        let stalled = TcpStream::connect(address).expect("first connection should connect");
        wait_for_active_count(&service.connections, 1);

        let mut rejected = TcpStream::connect(address).expect("excess connection should connect");
        rejected
            .set_read_timeout(Some(Duration::from_secs(1)))
            .expect("read timeout should configure");
        let mut response = String::new();
        rejected
            .read_to_string(&mut response)
            .expect("capacity response should read");
        assert!(
            response.starts_with("HTTP/1.1 503 Service Unavailable\r\n"),
            "unexpected capacity response: {response:?}"
        );
        assert_eq!(
            response_body(response.as_bytes()),
            br#"{"code":"MANAGEMENT_WEB_CONNECTION_LIMIT_REACHED"}"#
        );
        assert_eq!(service.connections.active_count(), 1);

        drop(stalled);
        wait_for_active_count(&service.connections, 0);
        let mut recovered = TcpStream::connect(address).expect("released slot should accept");
        recovered
            .write_all(b"GET / HTTP/1.1\r\nhost: localhost\r\n\r\n")
            .expect("request should write");
        let mut response = String::new();
        recovered
            .read_to_string(&mut response)
            .expect("response should read");
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        service.stop().expect("service should stop");
    }

    #[test]
    fn stop_and_drop_shutdown_stalled_connections() {
        for explicit_stop in [true, false] {
            let service = ManagementWebService::start(&ManagementWebConfig {
                port: 0,
                ..Default::default()
            })
            .expect("service should start");
            let registry = Arc::clone(&service.connections);
            let mut stalled = TcpStream::connect(service.local_addr())
                .expect("stalled connection should connect");
            stalled
                .set_read_timeout(Some(Duration::from_secs(1)))
                .expect("read timeout should configure");
            wait_for_active_count(&registry, 1);

            if explicit_stop {
                service.stop().expect("service should stop");
            } else {
                drop(service);
            }

            let mut byte = [0_u8; 1];
            match stalled.read(&mut byte) {
                Ok(0) => {}
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::ConnectionReset
                            | std::io::ErrorKind::ConnectionAborted
                            | std::io::ErrorKind::BrokenPipe
                            | std::io::ErrorKind::NotConnected
                    ) => {}
                result => panic!("stalled connection was not shut down: {result:?}"),
            }
            wait_for_active_count(&registry, 0);
        }
    }

    #[test]
    fn service_stop_drains_a_connection_blocked_on_the_gateway() {
        let gateway = TcpListener::bind("127.0.0.1:0").expect("gateway should bind");
        let gateway_address = gateway.local_addr().expect("gateway address should load");
        let (accepted_sender, accepted_receiver) = mpsc::sync_channel(1);
        let (release_sender, release_receiver) = mpsc::sync_channel(1);
        let gateway_thread = thread::spawn(move || {
            let (mut stream, _) = gateway.accept().expect("gateway should accept");
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request).expect("request should read");
            accepted_sender
                .send(())
                .expect("accepted signal should send");
            let _ = release_receiver.recv_timeout(Duration::from_secs(2));
        });
        let service = ManagementWebService::start(&ManagementWebConfig {
            port: 0,
            gateway: gateway_address,
            ..Default::default()
        })
        .expect("service should start");
        let connections = Arc::clone(&service.connections);
        let workers = Arc::clone(&service.connection_workers);
        let mut client = TcpStream::connect(service.local_addr()).expect("client should connect");
        client
            .write_all(b"GET /api/v1/events HTTP/1.1\r\nhost: localhost\r\n\r\n")
            .expect("proxy request should write");
        accepted_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("gateway should receive the request");
        wait_for_active_count(&connections, 1);
        assert_eq!(workers.tracked_count(), 1);

        let (stopped_sender, stopped_receiver) = mpsc::sync_channel(1);
        let stop_thread = thread::spawn(move || {
            let _ = stopped_sender.send(service.stop());
        });
        let stopped = match stopped_receiver.recv_timeout(Duration::from_secs(1)) {
            Ok(stopped) => stopped,
            Err(error) => {
                let _ = release_sender.send(());
                let _ = stop_thread.join();
                panic!("service did not drain the blocked proxy: {error}");
            }
        };
        stopped.expect("service should stop cleanly");
        stop_thread.join().expect("stop thread should join");
        assert_eq!(connections.active_count(), 0);
        assert_eq!(workers.tracked_count(), 0);

        let _ = release_sender.send(());
        gateway_thread.join().expect("gateway should join");
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
