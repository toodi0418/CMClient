use crate::access::{ManagementAccessController, ManagementAccessError};
use axum::{
    Json, Router,
    body::Body,
    error_handling::HandleErrorLayer,
    extract::{ConnectInfo, Extension, Request, rejection::JsonRejection},
    http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{any, get, post},
};
use bytes::Bytes;
use hyper::body::{Body as HttpBody, Frame};
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::{TokioExecutor, TokioTimer},
};
use ipnet::IpNet;
use serde::{Deserialize, Serialize};
use socket2::{Domain, Protocol, Socket, Type};
use std::{
    collections::BTreeSet,
    fmt,
    future::Future,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener},
    path::{Path, PathBuf},
    pin::Pin,
    str::FromStr,
    sync::{
        Arc, Condvar, Mutex, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
    thread::{self, JoinHandle},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use time::Duration as CookieDuration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use tower::{
    BoxError, ServiceBuilder, ServiceExt, limit::ConcurrencyLimitLayer, load_shed::LoadShedLayer,
};
use tower_governor::{
    GovernorLayer, governor::GovernorConfigBuilder, key_extractor::PeerIpKeyExtractor,
};
use tower_http::{
    limit::RequestBodyLimitLayer,
    request_id::{MakeRequestId, PropagateRequestIdLayer, RequestId, SetRequestIdLayer},
    services::{ServeDir, ServeFile},
    timeout::{RequestBodyTimeoutLayer, TimeoutError},
    trace::TraceLayer,
};
use tower_sessions::{Expiry, MemoryStore, Session, SessionManagerLayer, cookie::SameSite};
use uuid::Uuid;
use zeroize::Zeroizing;

const MAX_REQUEST_BYTES: usize = 1024 * 1024;
const GATEWAY_RESPONSE_TIMEOUT: Duration = Duration::from_secs(2);
const GATEWAY_LEASE_DRAIN_TIMEOUT: Duration = Duration::from_millis(250);
const WEB_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const SESSION_AUTHENTICATED: &str = "management.authenticated";
const SESSION_CSRF: &str = "management.csrf";
const SESSION_EXPIRES_AT: &str = "management.expires_at";
const SESSION_GENERATION: &str = "management.generation";
const SESSION_ROLE: &str = "management.role";
const SESSION_SETUP_GENERATION: &str = "management.setup_generation";
const SESSION_COOKIE_NAME: &str = "cmclient.sid";
const CSRF_HEADER_NAME: &str = "x-csrf-token";
const LOGIN_MAX_PASSWORD_BYTES: usize = 1024;
const REQUEST_HEADER_TIMEOUT: Duration = Duration::from_secs(5);
#[cfg(not(test))]
const REQUEST_BODY_TIMEOUT: Duration = Duration::from_secs(10);
#[cfg(test)]
const REQUEST_BODY_TIMEOUT: Duration = Duration::from_millis(50);
const LOGIN_TIMEOUT: Duration = Duration::from_secs(15);
const RATE_LIMIT_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);
const MAX_REQUEST_HEADERS: usize = 64;
const MAX_HTTP1_BUFFER_BYTES: usize = 64 * 1024;
const MAX_CONCURRENT_REQUESTS: usize = 64;
const MAX_RESPONSE_BODIES: usize = 64;
pub const GATEWAY_CAPABILITY_HEADER: &str = "x-cmclient-gateway-capability";
const AUTH_CACHE_CONTROL: &str = "no-store";
const SHELL_CACHE_CONTROL: &str = "no-cache";
const IMMUTABLE_ASSET_CACHE_CONTROL: &str = "public, max-age=31536000, immutable";
const MANAGEMENT_CONTENT_SECURITY_POLICY: &str = "default-src 'self'; style-src-elem 'self' 'unsafe-inline'; \
     style-src-attr 'unsafe-inline'; object-src 'none'; frame-ancestors 'none'; base-uri 'self'";

const GATEWAY_REQUEST_HEADER_ALLOWLIST: &[&str] = &[
    "accept",
    "content-type",
    "content-length",
    "last-event-id",
    "x-trace-id",
    "x-correlation-id",
    "idempotency-key",
    "range",
    "if-match",
    "if-none-match",
    "if-modified-since",
    "if-unmodified-since",
    "if-range",
];
const GATEWAY_RESPONSE_HEADER_ALLOWLIST: &[&str] = &[
    "content-type",
    "content-length",
    "content-encoding",
    "content-disposition",
    "cache-control",
    "etag",
    "last-modified",
    "content-range",
    "accept-ranges",
    "retry-after",
    "x-trace-id",
];

static NEXT_WEB_GENERATION: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
struct GatewayLeaseState {
    active: bool,
    readers: usize,
}

#[derive(Debug)]
struct GatewayRouteLease {
    state: Mutex<GatewayLeaseState>,
    drained: Condvar,
    cancelled: CancellationToken,
}

impl GatewayRouteLease {
    fn new() -> Self {
        Self {
            state: Mutex::new(GatewayLeaseState {
                active: true,
                readers: 0,
            }),
            drained: Condvar::new(),
            cancelled: CancellationToken::new(),
        }
    }

    fn acquire(self: &Arc<Self>) -> Option<GatewayLeaseToken> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.active {
            return None;
        }
        state.readers = state.readers.saturating_add(1);
        Some(GatewayLeaseToken {
            lease: Arc::clone(self),
        })
    }

    fn is_active(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active
    }

    fn deactivate(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active = false;
        self.cancelled.cancel();
    }

    fn drain(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let deadline = Instant::now() + GATEWAY_LEASE_DRAIN_TIMEOUT;
        while state.readers != 0 {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            let (next, wait_result) = self
                .drained
                .wait_timeout(state, remaining)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state = next;
            if wait_result.timed_out() {
                break;
            }
        }
    }
}

#[derive(Debug)]
struct GatewayLeaseToken {
    lease: Arc<GatewayRouteLease>,
}

impl Drop for GatewayLeaseToken {
    fn drop(&mut self) {
        let mut state = self
            .lease
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.readers = state.readers.saturating_sub(1);
        if state.readers == 0 {
            self.lease.drained.notify_all();
        }
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
        if address.ip() != IpAddr::V4(Ipv4Addr::LOCALHOST)
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

    pub fn is_active(&self) -> bool {
        self.lease.is_active()
    }

    pub fn active(&self) -> Option<ActiveGatewayRoute> {
        self.lease.acquire().map(|lease| ActiveGatewayRoute {
            address: self.address,
            capability: Arc::clone(&self.capability),
            cancellation: self.lease.cancelled.clone(),
            _lease: lease,
        })
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

impl fmt::Debug for GatewayRoute {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GatewayRoute")
            .field("address", &self.address)
            .field("capability", &"[REDACTED]")
            .field("active", &self.is_active())
            .finish()
    }
}

pub struct ActiveGatewayRoute {
    address: SocketAddr,
    capability: Arc<Zeroizing<String>>,
    cancellation: CancellationToken,
    _lease: GatewayLeaseToken,
}

impl ActiveGatewayRoute {
    pub const fn address(&self) -> SocketAddr {
        self.address
    }

    pub fn capability(&self) -> &str {
        self.capability.as_str()
    }

    fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }
}

impl fmt::Debug for ActiveGatewayRoute {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActiveGatewayRoute")
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
        let previous = {
            let mut current = self
                .route
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(previous) = current.as_ref() {
                previous.lease.deactivate();
            }
            current.replace(route)
        };
        if let Some(previous) = previous {
            previous.lease.drain();
        }
    }

    pub fn clear(&self) {
        let previous = {
            let mut current = self
                .route
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(previous) = current.as_ref() {
                previous.lease.deactivate();
            }
            current.take()
        };
        if let Some(previous) = previous {
            previous.lease.drain();
        }
    }

    pub fn snapshot(&self) -> Option<GatewayRoute> {
        self.route
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .filter(GatewayRoute::is_active)
    }

    fn active(&self) -> Option<ActiveGatewayRoute> {
        self.snapshot()?.active()
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ManagementWebProfile {
    #[default]
    Native,
    Docker,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ManagementWebConfig {
    pub enabled: bool,
    pub port: u16,
    pub profile: ManagementWebProfile,
    pub setup_generation: u64,
    pub allow_lan: bool,
    pub allowed_cidrs: Vec<String>,
    pub allowed_hosts: BTreeSet<String>,
    pub tls: Option<ManagementTlsConfig>,
    pub static_web_root: Option<PathBuf>,
}

impl fmt::Debug for ManagementWebConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ManagementWebConfig")
            .field("enabled", &self.enabled)
            .field("port", &self.port)
            .field("profile", &self.profile)
            .field("setup_generation", &self.setup_generation)
            .field("allow_lan", &self.allow_lan)
            .field("allowed_cidrs", &self.allowed_cidrs)
            .field("allowed_hosts", &self.allowed_hosts)
            .field("tls", &self.tls)
            .field("static_web_root", &self.static_web_root)
            .finish()
    }
}

impl Default for ManagementWebConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            port: 7080,
            profile: ManagementWebProfile::Native,
            setup_generation: 1,
            allow_lan: false,
            allowed_cidrs: Vec::new(),
            allowed_hosts: BTreeSet::new(),
            tls: None,
            static_web_root: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagementTlsConfig {
    pub certificate_path: PathBuf,
    pub private_key_path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManagementWebError {
    Disabled,
    NonLoopbackBind,
    InvalidConfiguration,
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
            Self::InvalidConfiguration => "MANAGEMENT_WEB_CONFIGURATION_INVALID",
            Self::Io => "MANAGEMENT_WEB_IO_FAILED",
            Self::InvalidHttp => "MANAGEMENT_WEB_HTTP_INVALID",
            Self::RequestTooLarge => "MANAGEMENT_WEB_REQUEST_TOO_LARGE",
            Self::TlsConfiguration => "MANAGEMENT_WEB_TLS_CONFIGURATION_INVALID",
        }
    }
}

impl fmt::Display for ManagementWebError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}

impl std::error::Error for ManagementWebError {}

type GatewayClient = Client<HttpConnector, Body>;

struct WebPolicy {
    profile: ManagementWebProfile,
    allow_lan: bool,
    allowed_cidrs: Vec<IpNet>,
    allowed_hosts: BTreeSet<String>,
    local_hosts: BTreeSet<String>,
    allowed_local_origins: BTreeSet<String>,
    http_lan_warning: bool,
    static_web_root: Option<PathBuf>,
    access: Option<Arc<ManagementAccessController>>,
    generation: Arc<AtomicU64>,
    setup_generation: Arc<AtomicU64>,
    response_slots: Arc<Semaphore>,
    gateway_session: GatewaySessionHandle,
    gateway_client: GatewayClient,
}

impl fmt::Debug for WebPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WebPolicy")
            .field("profile", &self.profile)
            .field("allow_lan", &self.allow_lan)
            .field("allowed_cidrs", &self.allowed_cidrs)
            .field("allowed_hosts", &self.allowed_hosts)
            .field("local_hosts", &self.local_hosts)
            .field("http_lan_warning", &self.http_lan_warning)
            .field("access", &self.access.is_some())
            .field("generation", &self.generation.load(Ordering::Acquire))
            .field(
                "setup_generation",
                &self.setup_generation.load(Ordering::Acquire),
            )
            .field(
                "available_response_slots",
                &self.response_slots.available_permits(),
            )
            .field("gateway_session", &self.gateway_session)
            .finish_non_exhaustive()
    }
}

pub struct ManagementWebService {
    addresses: Vec<SocketAddr>,
    advertised_url: String,
    generation: Arc<AtomicU64>,
    setup_generation: Arc<AtomicU64>,
    rate_limit_cleanup: CancellationToken,
    handles: Vec<axum_server::Handle<SocketAddr>>,
    worker: Option<JoinHandle<Result<(), ManagementWebError>>>,
}

impl ManagementWebService {
    pub fn start(
        config: &ManagementWebConfig,
        agent_routes: Router,
        access: Option<Arc<ManagementAccessController>>,
        gateway_session: GatewaySessionHandle,
    ) -> Result<Self, ManagementWebError> {
        if !config.enabled {
            return Err(ManagementWebError::Disabled);
        }
        validate_access_configuration(config, access.as_deref())?;
        let allowed_cidrs = config
            .allowed_cidrs
            .iter()
            .map(|cidr| IpNet::from_str(cidr).map_err(|_| ManagementWebError::InvalidConfiguration))
            .collect::<Result<Vec<_>, _>>()?;
        let static_web_root = canonical_static_root(config.static_web_root.as_deref())?;

        let (ipv4, ipv6, addresses) = bind_dual_stack(config.port)?;
        let port = addresses[0].port();
        let local_hosts = local_hosts(port);
        let allowed_hosts = allowed_hosts(config, port)?;
        let scheme = if config.tls.is_some() {
            "https"
        } else {
            "http"
        };
        let allowed_local_origins = local_hosts
            .iter()
            .map(|host| format!("{scheme}://{host}"))
            .collect();
        let advertised_url = format!("{scheme}://127.0.0.1:{port}/");
        let generation = Arc::new(AtomicU64::new(
            NEXT_WEB_GENERATION.fetch_add(1, Ordering::AcqRel),
        ));
        let setup_generation = Arc::new(AtomicU64::new(config.setup_generation));
        let http_lan_warning = config.allow_lan && config.tls.is_none();
        if http_lan_warning {
            if let Some(access) = &access {
                access.audit(unix_time(), "listener", "http_lan_warning");
            }
        }
        let policy = Arc::new(WebPolicy {
            profile: config.profile,
            allow_lan: config.allow_lan,
            allowed_cidrs,
            allowed_hosts,
            local_hosts,
            allowed_local_origins,
            http_lan_warning,
            static_web_root,
            access,
            generation: Arc::clone(&generation),
            setup_generation: Arc::clone(&setup_generation),
            response_slots: Arc::new(Semaphore::new(MAX_RESPONSE_BODIES)),
            gateway_session,
            gateway_client: Client::builder(TokioExecutor::new()).build_http(),
        });
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|_| ManagementWebError::Io)?;
        let rate_limit_cleanup = CancellationToken::new();
        let app = {
            let _runtime_guard = runtime.enter();
            management_router(
                agent_routes,
                Arc::clone(&policy),
                config.tls.is_some(),
                rate_limit_cleanup.clone(),
            )
        };
        let tls = config
            .tls
            .as_ref()
            .map(|tls| {
                runtime.block_on(axum_server::tls_rustls::RustlsConfig::from_pem_file(
                    &tls.certificate_path,
                    &tls.private_key_path,
                ))
            })
            .transpose()
            .map_err(|_| ManagementWebError::TlsConfiguration)?;
        let handles = vec![
            axum_server::Handle::<SocketAddr>::new(),
            axum_server::Handle::<SocketAddr>::new(),
        ];
        let worker_handles = handles.clone();
        let worker = thread::Builder::new()
            .name(String::from("cmclient-management-web"))
            .spawn(move || runtime.block_on(run_servers(ipv4, ipv6, app, tls, worker_handles)))
            .map_err(|_| ManagementWebError::Io)?;

        Ok(Self {
            addresses,
            advertised_url,
            generation,
            setup_generation,
            rate_limit_cleanup,
            handles,
            worker: Some(worker),
        })
    }

    pub fn local_addr(&self) -> Result<SocketAddr, ManagementWebError> {
        self.addresses
            .first()
            .copied()
            .ok_or(ManagementWebError::Io)
    }

    pub fn addresses(&self) -> &[SocketAddr] {
        &self.addresses
    }

    pub fn advertised_url(&self) -> &str {
        &self.advertised_url
    }

    pub fn revoke_sessions(&self) {
        self.generation.fetch_add(1, Ordering::AcqRel);
    }

    pub fn set_setup_generation(&self, generation: u64) {
        if self.setup_generation.swap(generation, Ordering::AcqRel) != generation {
            self.revoke_sessions();
        }
    }

    pub fn stop(&mut self) -> Result<(), ManagementWebError> {
        let Some(worker) = self.worker.take() else {
            return Ok(());
        };
        self.revoke_sessions();
        self.rate_limit_cleanup.cancel();
        for handle in &self.handles {
            handle.graceful_shutdown(Some(WEB_SHUTDOWN_TIMEOUT));
        }
        worker.join().map_err(|_| ManagementWebError::Io)??;
        Ok(())
    }
}

impl Drop for ManagementWebService {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

async fn run_servers(
    ipv4: TcpListener,
    ipv6: TcpListener,
    app: Router,
    tls: Option<axum_server::tls_rustls::RustlsConfig>,
    handles: Vec<axum_server::Handle<SocketAddr>>,
) -> Result<(), ManagementWebError> {
    let make_ipv4 = app
        .clone()
        .into_make_service_with_connect_info::<SocketAddr>();
    let make_ipv6 = app.into_make_service_with_connect_info::<SocketAddr>();
    let result = if let Some(tls) = tls {
        let mut ipv4_server = axum_server::from_tcp_rustls(ipv4, tls.clone())
            .map_err(|_| ManagementWebError::Io)?
            .http1_only()
            .handle(handles[0].clone());
        configure_http1(&mut ipv4_server);
        let mut ipv6_server = axum_server::from_tcp_rustls(ipv6, tls)
            .map_err(|_| ManagementWebError::Io)?
            .http1_only()
            .handle(handles[1].clone());
        configure_http1(&mut ipv6_server);
        tokio::try_join!(ipv4_server.serve(make_ipv4), ipv6_server.serve(make_ipv6)).map(|_| ())
    } else {
        let mut ipv4_server = axum_server::from_tcp(ipv4)
            .map_err(|_| ManagementWebError::Io)?
            .http1_only()
            .handle(handles[0].clone());
        configure_http1(&mut ipv4_server);
        let mut ipv6_server = axum_server::from_tcp(ipv6)
            .map_err(|_| ManagementWebError::Io)?
            .http1_only()
            .handle(handles[1].clone());
        configure_http1(&mut ipv6_server);
        tokio::try_join!(ipv4_server.serve(make_ipv4), ipv6_server.serve(make_ipv6)).map(|_| ())
    };
    result.map_err(|_| ManagementWebError::Io)
}

fn configure_http1<A>(server: &mut axum_server::Server<SocketAddr, A>) {
    server
        .http_builder()
        .http1()
        .timer(TokioTimer::new())
        .max_headers(MAX_REQUEST_HEADERS)
        .header_read_timeout(REQUEST_HEADER_TIMEOUT)
        .max_buf_size(MAX_HTTP1_BUFFER_BYTES);
}

fn management_router(
    agent_routes: Router,
    policy: Arc<WebPolicy>,
    secure_cookie: bool,
    cleanup: CancellationToken,
) -> Router {
    let mut login_governor = GovernorConfigBuilder::default().key_extractor(PeerIpKeyExtractor);
    login_governor.per_second(12).burst_size(5);
    let login_governor = Arc::new(
        login_governor
            .finish()
            .expect("non-zero login governor configuration"),
    );
    let login = Router::new()
        .route("/api/v1/auth/login", post(login))
        .route("/api/v1/auth/session", get(local_session))
        .layer(middleware::from_fn(login_timeout))
        .layer(
            GovernorLayer::new(Arc::clone(&login_governor)).error_handler(|_| {
                stable_error(
                    StatusCode::TOO_MANY_REQUESTS,
                    ManagementAccessError::LoginRateLimited.code(),
                )
            }),
        );

    let mut api_governor = GovernorConfigBuilder::default().key_extractor(PeerIpKeyExtractor);
    api_governor.per_second(1).burst_size(120);
    let api_governor = Arc::new(
        api_governor
            .finish()
            .expect("non-zero API governor configuration"),
    );
    let login_limiter = Arc::clone(&login_governor);
    let api_limiter = Arc::clone(&api_governor);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(RATE_LIMIT_CLEANUP_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                () = cleanup.cancelled() => break,
                _ = interval.tick() => {
                    login_limiter.limiter().retain_recent();
                    api_limiter.limiter().retain_recent();
                }
            }
        }
    });
    let api = Router::new()
        .merge(agent_routes)
        .merge(login)
        .route("/api/v1/{*path}", any(proxy_gateway))
        .method_not_allowed_fallback(method_not_allowed)
        .layer(
            GovernorLayer::new(Arc::clone(&api_governor)).error_handler(|_| {
                stable_error(
                    StatusCode::TOO_MANY_REQUESTS,
                    "MANAGEMENT_REQUEST_RATE_LIMITED",
                )
            }),
        )
        .layer(RequestBodyLimitLayer::new(MAX_REQUEST_BYTES))
        .layer(RequestBodyTimeoutLayer::new(REQUEST_BODY_TIMEOUT))
        .layer(middleware::from_fn(validate_request_body_headers))
        .layer(middleware::from_fn(auth_cache_control));

    let app = api.fallback(static_fallback);

    let ttl = policy
        .access
        .as_ref()
        .map_or(3_600, |access| access.session_ttl_seconds());
    let session_layer = SessionManagerLayer::new(MemoryStore::default())
        .with_name(SESSION_COOKIE_NAME)
        .with_http_only(true)
        .with_same_site(SameSite::Strict)
        .with_secure(secure_cookie)
        .with_expiry(Expiry::OnInactivity(CookieDuration::seconds(
            i64::try_from(ttl).unwrap_or(3_600),
        )));

    let app = app
        .layer(middleware::from_fn(authorize_request))
        .layer(session_layer);

    let capacity_limited = ServiceBuilder::new()
        .layer(HandleErrorLayer::new(handle_capacity_error))
        .layer(LoadShedLayer::new())
        .layer(ConcurrencyLimitLayer::new(MAX_CONCURRENT_REQUESTS))
        .service(app);
    Router::new()
        .fallback_service(capacity_limited)
        .layer(middleware::from_fn(response_lifetime_limit))
        .layer(middleware::from_fn(admit_peer_and_host))
        .layer(middleware::from_fn(security_headers))
        .layer(Extension(policy))
        .layer(PropagateRequestIdLayer::x_request_id())
        .layer(TraceLayer::new_for_http().make_span_with(
            |request: &axum::http::Request<Body>| {
                tracing::info_span!("management_web_request", method = %request.method())
            },
        ))
        .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
        .layer(middleware::from_fn(scrub_client_headers))
}

async fn handle_capacity_error(_error: BoxError) -> Response {
    stable_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "MANAGEMENT_WEB_CONNECTION_LIMIT_REACHED",
    )
}

async fn response_lifetime_limit(
    Extension(policy): Extension<Arc<WebPolicy>>,
    request: Request,
    next: Next,
) -> Response {
    let Ok(permit) = Arc::clone(&policy.response_slots).try_acquire_owned() else {
        audit_policy(&policy, "request", "response_capacity_denied");
        return stable_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "MANAGEMENT_WEB_CONNECTION_LIMIT_REACHED",
        );
    };
    let response = next.run(request).await;
    let (parts, body) = response.into_parts();
    Response::from_parts(parts, Body::new(ResponsePermitBody::new(body, permit)))
}

async fn admit_peer_and_host(
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Extension(policy): Extension<Arc<WebPolicy>>,
    request: Request,
    next: Next,
) -> Response {
    if !host_allowed(request.headers(), &policy.allowed_hosts) {
        audit_policy(&policy, "request", "host_denied");
        return stable_error(StatusCode::FORBIDDEN, "MANAGEMENT_HOST_DENIED");
    }
    let peer_allowed = match policy.profile {
        ManagementWebProfile::Native if peer.ip().is_loopback() => true,
        ManagementWebProfile::Native => {
            policy.allow_lan
                && policy.access.is_some()
                && (policy.allowed_cidrs.is_empty()
                    || policy
                        .allowed_cidrs
                        .iter()
                        .any(|network| network.contains(&peer.ip())))
        }
        ManagementWebProfile::Docker => {
            policy.access.is_some()
                && (policy.allowed_cidrs.is_empty()
                    || policy
                        .allowed_cidrs
                        .iter()
                        .any(|network| network.contains(&peer.ip())))
        }
    };
    if !peer_allowed {
        audit_policy(&policy, "request", "peer_denied");
        return stable_error(StatusCode::FORBIDDEN, "MANAGEMENT_PEER_DENIED");
    }
    next.run(request).await
}

async fn authorize_request(
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Extension(policy): Extension<Arc<WebPolicy>>,
    session: Session,
    request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path();
    let is_api = path == "/api" || path.starts_with("/api/");
    if !is_api || path == "/api/v1/auth/login" || path == "/api/v1/auth/session" {
        return next.run(request).await;
    }
    let local_host = exact_header(request.headers(), header::HOST)
        .is_some_and(|host| policy.local_hosts.contains(&host.to_ascii_lowercase()));
    let is_loopback =
        peer.ip().is_loopback() && local_host && policy.profile == ManagementWebProfile::Native;
    let write = is_write_method(request.method());
    if is_loopback && !write {
        return next.run(request).await;
    }
    let now = unix_time();
    let origin = exact_header(request.headers(), header::ORIGIN);
    if is_loopback {
        if origin.is_none_or(|origin| !policy.allowed_local_origins.contains(origin)) {
            audit_policy(&policy, "request", "origin_denied");
            return access_error(ManagementAccessError::OriginDenied);
        }
    } else {
        let Some(access) = &policy.access else {
            return access_error(ManagementAccessError::SessionInvalid);
        };
        if let Some(origin) = origin {
            if let Err(error) = access.require_origin(origin, now) {
                return access_error(error);
            }
        } else if write {
            access.audit(now, "request", "origin_denied");
            return access_error(ManagementAccessError::OriginDenied);
        }
    }
    let authenticated = session
        .get::<bool>(SESSION_AUTHENTICATED)
        .await
        .ok()
        .flatten()
        .unwrap_or(false);
    let generation = session.get::<u64>(SESSION_GENERATION).await.ok().flatten();
    let setup_generation = session
        .get::<u64>(SESSION_SETUP_GENERATION)
        .await
        .ok()
        .flatten();
    let role = session.get::<String>(SESSION_ROLE).await.ok().flatten();
    let expires_at = session.get::<u64>(SESSION_EXPIRES_AT).await.ok().flatten();
    if expires_at.is_some_and(|expires_at| expires_at <= now) {
        let _ = session.delete().await;
        audit_policy(&policy, "request", "session_expired");
        return access_error(ManagementAccessError::SessionExpired);
    }
    let role_allowed = matches!(role.as_deref(), Some("admin"))
        || (is_loopback && matches!(role.as_deref(), Some("local")));
    if !authenticated
        || !role_allowed
        || generation != Some(policy.generation.load(Ordering::Acquire))
        || setup_generation != Some(policy.setup_generation.load(Ordering::Acquire))
        || expires_at.is_none_or(|expires_at| expires_at <= now)
    {
        let _ = session.delete().await;
        audit_policy(&policy, "request", "session_denied");
        return access_error(ManagementAccessError::SessionInvalid);
    }
    if write {
        let expected = session.get::<String>(SESSION_CSRF).await.ok().flatten();
        let supplied = exact_header(request.headers(), HeaderName::from_static(CSRF_HEADER_NAME));
        if !matches!((expected.as_deref(), supplied), (Some(expected), Some(supplied)) if expected == supplied)
        {
            audit_policy(&policy, "request", "csrf_denied");
            return access_error(ManagementAccessError::CsrfInvalid);
        }
    }
    audit_policy(&policy, "request", "allowed");
    next.run(request).await
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LoginBody {
    password: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct LoginResponse {
    schema_version: u8,
    csrf_token: String,
    expires_at: u64,
}

async fn login(
    Extension(policy): Extension<Arc<WebPolicy>>,
    session: Session,
    headers: HeaderMap,
    body: Result<Json<LoginBody>, JsonRejection>,
) -> Response {
    let Json(body) = match body {
        Ok(body) => body,
        Err(error) => return login_body_error(&error),
    };
    let Some(access) = &policy.access else {
        return stable_error(StatusCode::NOT_FOUND, "MANAGEMENT_AUTH_NOT_CONFIGURED");
    };
    if body.password.is_empty() || body.password.len() > LOGIN_MAX_PASSWORD_BYTES {
        return access_error(ManagementAccessError::CredentialsInvalid);
    }
    let Some(origin) = exact_header(&headers, header::ORIGIN) else {
        return access_error(ManagementAccessError::OriginDenied);
    };
    let now = unix_time();
    if let Err(error) = access.verify_password(origin, &body.password, now).await {
        return access_error(error);
    }
    issue_session(
        &session,
        &policy,
        "admin",
        access.session_ttl_seconds(),
        now,
    )
    .await
}

async fn local_session(
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Extension(policy): Extension<Arc<WebPolicy>>,
    session: Session,
    headers: HeaderMap,
) -> Response {
    let local_host = exact_header(&headers, header::HOST)
        .is_some_and(|host| policy.local_hosts.contains(&host.to_ascii_lowercase()));
    if policy.profile != ManagementWebProfile::Native || !peer.ip().is_loopback() || !local_host {
        return stable_error(StatusCode::FORBIDDEN, "MANAGEMENT_LOCAL_SESSION_DENIED");
    }
    let ttl = policy
        .access
        .as_ref()
        .map_or(3_600, |access| access.session_ttl_seconds());
    issue_session(&session, &policy, "local", ttl, unix_time()).await
}

async fn issue_session(
    session: &Session,
    policy: &WebPolicy,
    role: &'static str,
    ttl_seconds: u64,
    now: u64,
) -> Response {
    let csrf_token = Uuid::new_v4().simple().to_string();
    let expires_at = now.saturating_add(ttl_seconds);
    let stored = session.cycle_id().await.is_ok()
        && session.insert(SESSION_AUTHENTICATED, true).await.is_ok()
        && session.insert(SESSION_ROLE, role).await.is_ok()
        && session
            .insert(
                SESSION_GENERATION,
                policy.generation.load(Ordering::Acquire),
            )
            .await
            .is_ok()
        && session
            .insert(
                SESSION_SETUP_GENERATION,
                policy.setup_generation.load(Ordering::Acquire),
            )
            .await
            .is_ok()
        && session.insert(SESSION_CSRF, &csrf_token).await.is_ok()
        && session.insert(SESSION_EXPIRES_AT, expires_at).await.is_ok();
    if !stored {
        let _ = session.delete().await;
        return stable_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "MANAGEMENT_SESSION_STORE_FAILED",
        );
    }
    Json(LoginResponse {
        schema_version: 1,
        csrf_token,
        expires_at,
    })
    .into_response()
}

async fn proxy_gateway(Extension(policy): Extension<Arc<WebPolicy>>, request: Request) -> Response {
    let Some(active) = policy.gateway_session.active() else {
        return stable_error(StatusCode::SERVICE_UNAVAILABLE, "GATEWAY_PROXY_UNAVAILABLE");
    };
    let cancellation = active.cancellation_token();
    let outbound = match gateway_request(request, &active) {
        Ok(request) => request,
        Err(error) => return stable_error(StatusCode::BAD_REQUEST, error.code()),
    };
    let response = tokio::select! {
        () = cancellation.cancelled() => None,
        response = tokio::time::timeout(
            GATEWAY_RESPONSE_TIMEOUT,
            policy.gateway_client.request(outbound),
        ) => response.ok().and_then(Result::ok),
    };
    let Some(response) = response else {
        return stable_error(StatusCode::SERVICE_UNAVAILABLE, "GATEWAY_PROXY_UNAVAILABLE");
    };
    let (mut parts, body) = response.into_parts();
    strip_hop_headers(&mut parts.headers);
    parts.headers = allowlisted_headers(&parts.headers, GATEWAY_RESPONSE_HEADER_ALLOWLIST);
    parts.extensions.clear();
    axum::http::Response::from_parts(
        parts,
        Body::new(RevocableBody::new(body, cancellation, active)),
    )
}

fn gateway_request(
    request: Request,
    active: &ActiveGatewayRoute,
) -> Result<axum::http::Request<Body>, ManagementWebError> {
    let (mut parts, body) = request.into_parts();
    let path_and_query = parts
        .uri
        .path_and_query()
        .map_or("/", axum::http::uri::PathAndQuery::as_str);
    parts.uri = Uri::from_str(&format!("http://{}{path_and_query}", active.address()))
        .map_err(|_| ManagementWebError::InvalidHttp)?;
    parts.extensions.clear();
    strip_hop_headers(&mut parts.headers);
    parts.headers = allowlisted_headers(&parts.headers, GATEWAY_REQUEST_HEADER_ALLOWLIST);
    parts.headers.insert(
        header::HOST,
        HeaderValue::from_str(&active.address().to_string())
            .map_err(|_| ManagementWebError::InvalidHttp)?,
    );
    parts.headers.insert(
        HeaderName::from_static(GATEWAY_CAPABILITY_HEADER),
        HeaderValue::from_str(active.capability()).map_err(|_| ManagementWebError::InvalidHttp)?,
    );
    let body = Body::new(RevocableBody::new(body, active.cancellation_token(), ()));
    Ok(axum::http::Request::from_parts(parts, body))
}

type CancellationWait = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

struct RevocableBody<B, H> {
    body: B,
    cancellation: CancellationToken,
    cancelled: CancellationWait,
    hold: Option<H>,
    ended: bool,
}

impl<B, H> RevocableBody<B, H> {
    fn new(body: B, cancellation: CancellationToken, hold: H) -> Self {
        let cancelled = Box::pin(cancellation.clone().cancelled_owned());
        Self {
            body,
            cancellation,
            cancelled,
            hold: Some(hold),
            ended: false,
        }
    }
}

impl<B, H> HttpBody for RevocableBody<B, H>
where
    B: HttpBody + Unpin,
    H: Unpin,
{
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        if this.ended {
            return Poll::Ready(None);
        }
        if this.cancelled.as_mut().poll(context).is_ready() {
            this.ended = true;
            this.hold.take();
            return Poll::Ready(None);
        }
        match Pin::new(&mut this.body).poll_frame(context) {
            Poll::Ready(None) => {
                this.ended = true;
                this.hold.take();
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(error))) => {
                this.ended = true;
                this.hold.take();
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(Some(Ok(frame))) => {
                if frame.is_trailers() {
                    // Trailers are terminal metadata here; ending the body avoids both leakage and
                    // returning Pending after consuming a ready frame without registering a waker.
                    this.ended = true;
                    this.hold.take();
                    return Poll::Ready(None);
                }
                if this.body.is_end_stream() {
                    this.ended = true;
                    this.hold.take();
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.ended || self.cancellation.is_cancelled() || self.body.is_end_stream()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        self.body.size_hint()
    }
}

struct ResponsePermitBody {
    body: Body,
    permit: Option<OwnedSemaphorePermit>,
}

impl ResponsePermitBody {
    fn new(body: Body, permit: OwnedSemaphorePermit) -> Self {
        Self {
            body,
            permit: Some(permit),
        }
    }
}

impl HttpBody for ResponsePermitBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        match Pin::new(&mut this.body).poll_frame(context) {
            Poll::Ready(None) => {
                this.permit.take();
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(error))) => {
                this.permit.take();
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(Some(Ok(frame))) => {
                if this.body.is_end_stream() {
                    this.permit.take();
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        self.body.size_hint()
    }
}

async fn static_fallback(
    Extension(policy): Extension<Arc<WebPolicy>>,
    request: Request,
) -> Response {
    let path = request.uri().path().to_owned();
    if path == "/api" || path.starts_with("/api/") {
        stable_error(StatusCode::NOT_FOUND, "API_ROUTE_NOT_FOUND")
    } else if !matches!(*request.method(), Method::GET | Method::HEAD) {
        stable_error(StatusCode::METHOD_NOT_ALLOWED, "WEB_METHOD_NOT_ALLOWED")
    } else {
        let Some(root) = &policy.static_web_root else {
            return stable_error(StatusCode::NOT_FOUND, "WEB_ASSET_NOT_FOUND");
        };
        let method = request.method().clone();
        let asset_like = path
            .rsplit('/')
            .next()
            .is_some_and(|segment| segment.contains('.'));
        let response = match ServeDir::new(root).oneshot(request).await {
            Ok(response) => response,
            Err(error) => match error {},
        };
        if response.status() != StatusCode::NOT_FOUND {
            return static_cache_response(response.map(Body::new), &path);
        }
        if asset_like {
            return stable_error(StatusCode::NOT_FOUND, "WEB_ASSET_NOT_FOUND");
        }
        let index_request = axum::http::Request::builder()
            .method(method)
            .uri("/")
            .body(Body::empty());
        let Ok(index_request) = index_request else {
            return stable_error(StatusCode::INTERNAL_SERVER_ERROR, "WEB_ASSET_READ_FAILED");
        };
        match ServeFile::new(root.join("index.html"))
            .oneshot(index_request)
            .await
        {
            Ok(response) => static_cache_response(response.map(Body::new), &path),
            Err(error) => match error {},
        }
    }
}

fn static_cache_response(mut response: Response, request_path: &str) -> Response {
    let policy = if is_clearly_hashed_vite_asset(request_path) {
        IMMUTABLE_ASSET_CACHE_CONTROL
    } else {
        SHELL_CACHE_CONTROL
    };
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static(policy));
    response
}

fn is_clearly_hashed_vite_asset(path: &str) -> bool {
    let Some(file_name) = path.strip_prefix("/assets/") else {
        return false;
    };
    if file_name.contains('/') {
        return false;
    }
    let Some((stem, extension)) = file_name.rsplit_once('.') else {
        return false;
    };
    let Some((logical_name, hash)) = stem.rsplit_once('-') else {
        return false;
    };
    !logical_name.is_empty()
        && !extension.is_empty()
        && (8..=64).contains(&hash.len())
        && hash
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

async fn security_headers(
    Extension(policy): Extension<Arc<WebPolicy>>,
    request: Request,
    next: Next,
) -> Response {
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(MANAGEMENT_CONTENT_SECURITY_POLICY),
    );
    headers.remove(header::ACCESS_CONTROL_ALLOW_ORIGIN);
    if policy.http_lan_warning {
        headers.insert(
            header::WARNING,
            HeaderValue::from_static("299 CMClient \"MANAGEMENT_HTTP_LAN_WARNING\""),
        );
        headers.insert(
            HeaderName::from_static("x-cmclient-management-warning"),
            HeaderValue::from_static("MANAGEMENT_HTTP_LAN_WARNING"),
        );
    }
    response
}

async fn auth_cache_control(request: Request, next: Next) -> Response {
    let auth_response = matches!(
        request.uri().path(),
        "/api/v1/auth/login" | "/api/v1/auth/session"
    );
    let mut response = next.run(request).await;
    if auth_response {
        response.headers_mut().insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static(AUTH_CACHE_CONTROL),
        );
    }
    response
}

async fn login_timeout(request: Request, next: Next) -> Response {
    complete_login_request(next.run(request), LOGIN_TIMEOUT).await
}

async fn complete_login_request(
    request: impl Future<Output = Response>,
    timeout: Duration,
) -> Response {
    match tokio::time::timeout(timeout, request).await {
        Ok(response) => response,
        Err(_) => stable_error(StatusCode::REQUEST_TIMEOUT, "MANAGEMENT_WEB_LOGIN_TIMEOUT"),
    }
}

async fn validate_request_body_headers(request: Request, next: Next) -> Response {
    let content_lengths = request.headers().get_all(header::CONTENT_LENGTH);
    let mut values = content_lengths.iter();
    let Some(value) = values.next() else {
        return next.run(request).await;
    };
    if values.next().is_some() || request.headers().contains_key(header::TRANSFER_ENCODING) {
        return stable_error(
            StatusCode::BAD_REQUEST,
            ManagementWebError::InvalidHttp.code(),
        );
    }
    let Ok(value) = value.to_str() else {
        return stable_error(
            StatusCode::BAD_REQUEST,
            ManagementWebError::InvalidHttp.code(),
        );
    };
    let Ok(length) = value.parse::<u64>() else {
        return stable_error(
            StatusCode::BAD_REQUEST,
            ManagementWebError::InvalidHttp.code(),
        );
    };
    if length > MAX_REQUEST_BYTES as u64 {
        return stable_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            ManagementWebError::RequestTooLarge.code(),
        );
    }
    next.run(request).await
}

async fn method_not_allowed() -> Response {
    stable_error(
        StatusCode::METHOD_NOT_ALLOWED,
        "MANAGEMENT_WEB_METHOD_NOT_ALLOWED",
    )
}

#[derive(Clone, Copy, Debug)]
struct MakeRequestUuid;

impl MakeRequestId for MakeRequestUuid {
    fn make_request_id<B>(&mut self, _request: &axum::http::Request<B>) -> Option<RequestId> {
        HeaderValue::from_str(&Uuid::new_v4().simple().to_string())
            .ok()
            .map(RequestId::new)
    }
}

async fn scrub_client_headers(mut request: Request, next: Next) -> Response {
    for name in [
        GATEWAY_CAPABILITY_HEADER,
        "forwarded",
        "x-forwarded-for",
        "x-forwarded-host",
        "x-forwarded-proto",
        "x-real-ip",
        "x-request-id",
    ] {
        request.headers_mut().remove(name);
    }
    next.run(request).await
}

fn validate_access_configuration(
    config: &ManagementWebConfig,
    access: Option<&ManagementAccessController>,
) -> Result<(), ManagementWebError> {
    if config.profile == ManagementWebProfile::Docker && access.is_none() {
        return Err(ManagementWebError::InvalidConfiguration);
    }
    if config.profile == ManagementWebProfile::Native {
        if config.allow_lan && access.is_none() {
            return Err(ManagementWebError::InvalidConfiguration);
        }
        if !config.allow_lan && !config.allowed_cidrs.is_empty() {
            return Err(ManagementWebError::InvalidConfiguration);
        }
    }
    if let Some(tls) = &config.tls {
        if !tls.certificate_path.is_absolute() || !tls.private_key_path.is_absolute() {
            return Err(ManagementWebError::TlsConfiguration);
        }
    }
    Ok(())
}

fn canonical_static_root(root: Option<&Path>) -> Result<Option<PathBuf>, ManagementWebError> {
    let Some(root) = root else {
        return Ok(None);
    };
    validate_static_tree(root)?;
    let root = std::fs::canonicalize(root).map_err(|_| ManagementWebError::Io)?;
    if !root.is_dir() || !root.join("index.html").is_file() {
        return Err(ManagementWebError::Io);
    }
    Ok(Some(root))
}

fn validate_static_tree(path: &Path) -> Result<(), ManagementWebError> {
    let metadata = std::fs::symlink_metadata(path).map_err(|_| ManagementWebError::Io)?;
    if metadata.file_type().is_symlink() || is_windows_reparse_point(&metadata) {
        return Err(ManagementWebError::InvalidConfiguration);
    }
    if metadata.is_dir() {
        for entry in std::fs::read_dir(path).map_err(|_| ManagementWebError::Io)? {
            let entry = entry.map_err(|_| ManagementWebError::Io)?;
            validate_static_tree(&entry.path())?;
        }
    }
    Ok(())
}

#[cfg(windows)]
fn is_windows_reparse_point(metadata: &std::fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;

    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;
    metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
}

#[cfg(not(windows))]
const fn is_windows_reparse_point(_metadata: &std::fs::Metadata) -> bool {
    false
}

fn allowed_hosts(
    config: &ManagementWebConfig,
    port: u16,
) -> Result<BTreeSet<String>, ManagementWebError> {
    let mut hosts = local_hosts(port);
    for host in &config.allowed_hosts {
        let host = host.to_ascii_lowercase();
        if host.is_empty()
            || host == "*"
            || host.contains(['/', '\\', '@'])
            || host.contains(char::is_whitespace)
        {
            return Err(ManagementWebError::InvalidConfiguration);
        }
        hosts.insert(host);
    }
    Ok(hosts)
}

fn local_hosts(port: u16) -> BTreeSet<String> {
    BTreeSet::from([
        format!("localhost:{port}"),
        format!("127.0.0.1:{port}"),
        format!("[::1]:{port}"),
    ])
}

fn host_allowed(headers: &HeaderMap, allowed_hosts: &BTreeSet<String>) -> bool {
    let mut values = headers.get_all(header::HOST).iter();
    let Some(host) = values.next().and_then(|value| value.to_str().ok()) else {
        return false;
    };
    values.next().is_none() && allowed_hosts.contains(&host.to_ascii_lowercase())
}

fn exact_header<N>(headers: &HeaderMap, name: N) -> Option<&str>
where
    N: axum::http::header::AsHeaderName,
{
    let mut values = headers.get_all(name).iter();
    let value = values.next()?.to_str().ok()?;
    values.next().is_none().then_some(value)
}

fn allowlisted_headers(headers: &HeaderMap, allowlist: &[&'static str]) -> HeaderMap {
    let mut allowed = HeaderMap::new();
    for &name in allowlist {
        let name = HeaderName::from_static(name);
        for value in headers.get_all(&name) {
            allowed.append(name.clone(), value.clone());
        }
    }
    allowed
}

fn strip_hop_headers(headers: &mut HeaderMap) {
    let connection_headers = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|name| HeaderName::from_bytes(name.trim().as_bytes()).ok())
        .collect::<Vec<_>>();
    for name in connection_headers {
        headers.remove(name);
    }
    for name in [
        "connection",
        "proxy-connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    ] {
        headers.remove(name);
    }
}

fn bind_dual_stack(
    requested_port: u16,
) -> Result<(TcpListener, TcpListener, Vec<SocketAddr>), ManagementWebError> {
    let ipv4 = bind_socket(
        Domain::IPV4,
        SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), requested_port),
        false,
    )?;
    let port = ipv4
        .local_addr()
        .map_err(|_| ManagementWebError::Io)?
        .port();
    let ipv6 = bind_socket(
        Domain::IPV6,
        SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), port),
        true,
    )?;
    let addresses = vec![
        ipv4.local_addr().map_err(|_| ManagementWebError::Io)?,
        ipv6.local_addr().map_err(|_| ManagementWebError::Io)?,
    ];
    Ok((ipv4, ipv6, addresses))
}

fn bind_socket(
    domain: Domain,
    address: SocketAddr,
    ipv6_only: bool,
) -> Result<TcpListener, ManagementWebError> {
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))
        .map_err(|_| ManagementWebError::Io)?;
    socket
        .set_reuse_address(false)
        .map_err(|_| ManagementWebError::Io)?;
    if domain == Domain::IPV6 {
        socket
            .set_only_v6(ipv6_only)
            .map_err(|_| ManagementWebError::Io)?;
    }
    socket
        .bind(&address.into())
        .map_err(|_| ManagementWebError::Io)?;
    socket.listen(128).map_err(|_| ManagementWebError::Io)?;
    let listener: TcpListener = socket.into();
    listener
        .set_nonblocking(true)
        .map_err(|_| ManagementWebError::Io)?;
    Ok(listener)
}

fn valid_gateway_capability(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn is_write_method(method: &Method) -> bool {
    !matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS)
}

fn unix_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn audit_policy(policy: &WebPolicy, action: &'static str, outcome: &'static str) {
    if let Some(access) = &policy.access {
        access.audit(unix_time(), action, outcome);
    }
}

fn access_error(error: ManagementAccessError) -> Response {
    let status = match error {
        ManagementAccessError::CredentialsInvalid
        | ManagementAccessError::SessionInvalid
        | ManagementAccessError::SessionExpired => StatusCode::UNAUTHORIZED,
        ManagementAccessError::LoginRateLimited => StatusCode::TOO_MANY_REQUESTS,
        ManagementAccessError::OriginDenied | ManagementAccessError::CsrfInvalid => {
            StatusCode::FORBIDDEN
        }
        ManagementAccessError::InvalidConfiguration => StatusCode::INTERNAL_SERVER_ERROR,
    };
    stable_error(status, error.code())
}

fn login_body_error(error: &JsonRejection) -> Response {
    if error_chain_contains::<TimeoutError>(error) {
        return stable_error(
            StatusCode::REQUEST_TIMEOUT,
            "MANAGEMENT_WEB_REQUEST_BODY_TIMEOUT",
        );
    }
    match error {
        JsonRejection::MissingJsonContentType(_) => stable_error(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "MANAGEMENT_WEB_JSON_CONTENT_TYPE_REQUIRED",
        ),
        JsonRejection::JsonSyntaxError(_) => {
            stable_error(StatusCode::BAD_REQUEST, "MANAGEMENT_WEB_JSON_INVALID")
        }
        JsonRejection::JsonDataError(_) => stable_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "MANAGEMENT_WEB_JSON_SCHEMA_INVALID",
        ),
        JsonRejection::BytesRejection(_) if error.status() == StatusCode::PAYLOAD_TOO_LARGE => {
            stable_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                ManagementWebError::RequestTooLarge.code(),
            )
        }
        JsonRejection::BytesRejection(_) => stable_error(
            StatusCode::BAD_REQUEST,
            "MANAGEMENT_WEB_REQUEST_BODY_INVALID",
        ),
        _ => stable_error(
            StatusCode::BAD_REQUEST,
            "MANAGEMENT_WEB_REQUEST_BODY_INVALID",
        ),
    }
}

fn error_chain_contains<T>(error: &(dyn std::error::Error + 'static)) -> bool
where
    T: std::error::Error + 'static,
{
    let mut current = Some(error);
    while let Some(error) = current {
        if error.is::<T>() {
            return true;
        }
        current = error.source();
    }
    false
}

#[derive(Serialize)]
struct StableErrorBody {
    code: &'static str,
}

fn stable_error(status: StatusCode, code: &'static str) -> Response {
    (status, Json(StableErrorBody { code })).into_response()
}

#[cfg(test)]
mod tests {
    use super::{
        AUTH_CACHE_CONTROL, CSRF_HEADER_NAME, GATEWAY_CAPABILITY_HEADER,
        GATEWAY_LEASE_DRAIN_TIMEOUT, GATEWAY_RESPONSE_HEADER_ALLOWLIST, GatewayRoute,
        GatewaySessionHandle, IMMUTABLE_ASSET_CACHE_CONTROL, MANAGEMENT_CONTENT_SECURITY_POLICY,
        MAX_REQUEST_BYTES, MAX_RESPONSE_BODIES, ManagementWebConfig, ManagementWebError,
        ManagementWebProfile, ManagementWebService, RevocableBody, SHELL_CACHE_CONTROL, WebPolicy,
        allowlisted_headers, canonical_static_root, complete_login_request, gateway_request,
        local_hosts, management_router, strip_hop_headers,
    };
    use crate::access::{LanAccessConfig, ManagementAccessController};
    use argon2::{Argon2, PasswordHasher, password_hash::SaltString};
    use axum::{
        Router,
        body::{Body, to_bytes},
        extract::ConnectInfo,
        http::{HeaderMap, HeaderName, HeaderValue, Method, Request, StatusCode, header},
        response::Response,
        routing::{get, post},
    };
    use bytes::Bytes;
    use http_body_util::BodyExt;
    use hyper::body::{Body as HttpBody, Frame};
    use hyper_util::{client::legacy::Client, rt::TokioExecutor};
    use ipnet::IpNet;
    use serde_json::Value;
    use std::{
        collections::BTreeSet,
        convert::Infallible,
        net::SocketAddr,
        path::{Path, PathBuf},
        pin::Pin,
        str::FromStr,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicU64, Ordering},
            mpsc,
        },
        task::{Context, Poll},
        thread,
        time::{Duration, Instant},
    };
    use tokio::sync::Semaphore;
    use tokio_util::sync::CancellationToken;
    use tower::ServiceExt;
    use uuid::Uuid;

    const TEST_PORT: u16 = 7_080;

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new(label: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "cmclient-agent-core-{label}-{}-{}",
                std::process::id(),
                Uuid::new_v4().simple()
            ));
            std::fs::create_dir_all(&path).expect("test directory should be created");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn test_access(origin: &str) -> Arc<ManagementAccessController> {
        let salt =
            SaltString::encode_b64(b"cmclient-web-fixture").expect("fixture salt should encode");
        let password_hash = Argon2::default()
            .hash_password(b"password", &salt)
            .expect("fixture password should hash")
            .to_string();
        Arc::new(
            ManagementAccessController::new(LanAccessConfig {
                password_hash,
                allowed_origins: BTreeSet::from([origin.to_owned()]),
                session_ttl_seconds: 600,
                audit_capacity: 64,
            })
            .expect("fixture access policy should be valid"),
        )
    }

    fn test_policy(
        profile: ManagementWebProfile,
        allow_lan: bool,
        allowed_cidrs: Vec<IpNet>,
        extra_hosts: &[&str],
        access: Option<Arc<ManagementAccessController>>,
        static_web_root: Option<PathBuf>,
        http_lan_warning: bool,
    ) -> Arc<WebPolicy> {
        let local_hosts = local_hosts(TEST_PORT);
        let mut allowed_hosts = local_hosts.clone();
        allowed_hosts.extend(extra_hosts.iter().map(|host| (*host).to_owned()));
        Arc::new(WebPolicy {
            profile,
            allow_lan,
            allowed_cidrs,
            allowed_hosts,
            local_hosts: local_hosts.clone(),
            allowed_local_origins: local_hosts
                .iter()
                .map(|host| format!("http://{host}"))
                .collect(),
            http_lan_warning,
            static_web_root,
            access,
            generation: Arc::new(AtomicU64::new(1)),
            setup_generation: Arc::new(AtomicU64::new(1)),
            response_slots: Arc::new(Semaphore::new(MAX_RESPONSE_BODIES)),
            gateway_session: GatewaySessionHandle::new(),
            gateway_client: Client::builder(TokioExecutor::new()).build_http(),
        })
    }

    fn test_router(agent_routes: Router, policy: Arc<WebPolicy>) -> (Router, CancellationToken) {
        let cleanup = CancellationToken::new();
        let router = management_router(agent_routes, policy, false, cleanup.clone());
        (router, cleanup)
    }

    fn test_request(
        method: Method,
        uri: &str,
        host: &str,
        peer: SocketAddr,
        body: Body,
    ) -> Request<Body> {
        let mut request = Request::builder()
            .method(method)
            .uri(uri)
            .header(header::HOST, host)
            .body(body)
            .expect("request should build");
        request.extensions_mut().insert(ConnectInfo(peer));
        request
    }

    fn response_cookie(response: &Response) -> String {
        response
            .headers()
            .get_all(header::SET_COOKIE)
            .iter()
            .filter_map(|value| value.to_str().ok())
            .find(|value| value.starts_with("cmclient.sid="))
            .and_then(|value| value.split(';').next())
            .expect("session response should set the management cookie")
            .to_owned()
    }

    async fn response_json(response: Response) -> Value {
        let body = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("response body should read");
        serde_json::from_slice(&body).expect("response should contain JSON")
    }

    async fn assert_error(response: Response, status: StatusCode, code: &str) {
        assert_eq!(response.status(), status);
        assert_eq!(
            response_json(response).await,
            serde_json::json!({"code": code})
        );
    }

    fn bounded_sync_operation(operation: impl FnOnce() + Send + 'static) -> Duration {
        let (finished, completion) = mpsc::sync_channel(1);
        let worker = thread::spawn(move || {
            let started = Instant::now();
            operation();
            finished
                .send(started.elapsed())
                .expect("completion receiver should remain available");
        });
        let elapsed = completion
            .recv_timeout(Duration::from_secs(1))
            .expect("lease revocation must have a deterministic upper bound");
        worker.join().expect("revocation worker should not panic");
        elapsed
    }

    struct PendingBody {
        polled: Option<Arc<AtomicBool>>,
    }

    impl PendingBody {
        fn idle() -> Self {
            Self { polled: None }
        }

        fn observed(polled: Arc<AtomicBool>) -> Self {
            Self {
                polled: Some(polled),
            }
        }
    }

    impl HttpBody for PendingBody {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            if let Some(polled) = &self.get_mut().polled {
                polled.store(true, Ordering::Release);
            }
            Poll::Pending
        }
    }

    struct TrailerBody {
        trailers: Option<HeaderMap>,
    }

    impl TrailerBody {
        fn new(trailers: HeaderMap) -> Self {
            Self {
                trailers: Some(trailers),
            }
        }
    }

    impl HttpBody for TrailerBody {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            match self.get_mut().trailers.take() {
                Some(trailers) => Poll::Ready(Some(Ok(Frame::trailers(trailers)))),
                None => Poll::Ready(None),
            }
        }

        fn is_end_stream(&self) -> bool {
            self.trailers.is_none()
        }
    }

    fn forbidden_trailers() -> HeaderMap {
        let mut trailers = HeaderMap::new();
        for (name, value) in [
            (GATEWAY_CAPABILITY_HEADER, "capability-must-not-cross"),
            ("cookie", "cmclient.sid=must-not-cross"),
            (CSRF_HEADER_NAME, "csrf-must-not-cross"),
            ("origin", "https://must-not-cross.example"),
            ("access-control-allow-origin", "*"),
            ("x-private-trailer", "must-not-cross"),
        ] {
            trailers.insert(
                HeaderName::from_static(name),
                HeaderValue::from_static(value),
            );
        }
        trailers
    }

    #[tokio::test]
    async fn local_session_uses_the_existing_payload_and_csrf_contract_and_revokes() {
        let policy = test_policy(
            ManagementWebProfile::Native,
            false,
            Vec::new(),
            &[],
            None,
            None,
            false,
        );
        let routes = Router::new().route(
            "/api/v1/test-write",
            post(|| async { StatusCode::NO_CONTENT }),
        );
        let (router, cleanup) = test_router(routes, Arc::clone(&policy));
        let peer = SocketAddr::from(([127, 0, 0, 1], 49_100));

        let session = router
            .clone()
            .oneshot(test_request(
                Method::GET,
                "/api/v1/auth/session",
                "localhost:7080",
                peer,
                Body::empty(),
            ))
            .await
            .expect("router should respond");
        assert_eq!(session.status(), StatusCode::OK);
        assert_eq!(
            session
                .headers()
                .get(header::CACHE_CONTROL)
                .and_then(|value| value.to_str().ok()),
            Some(AUTH_CACHE_CONTROL)
        );
        let cookie = response_cookie(&session);
        let payload = response_json(session).await;
        assert_eq!(payload["schemaVersion"], 1);
        assert_eq!(payload.as_object().map(serde_json::Map::len), Some(3));
        let csrf = payload["csrfToken"]
            .as_str()
            .expect("session should contain CSRF")
            .to_owned();
        assert_eq!(csrf.len(), 32);
        assert!(payload["expiresAt"].as_u64().is_some_and(|value| value > 0));

        let mut legacy_header = test_request(
            Method::POST,
            "/api/v1/test-write",
            "localhost:7080",
            peer,
            Body::empty(),
        );
        legacy_header.headers_mut().insert(
            header::ORIGIN,
            HeaderValue::from_static("http://localhost:7080"),
        );
        legacy_header.headers_mut().insert(
            header::COOKIE,
            HeaderValue::from_str(&cookie).expect("cookie should encode"),
        );
        legacy_header.headers_mut().insert(
            "x-cmclient-csrf",
            HeaderValue::from_str(&csrf).expect("CSRF should encode"),
        );
        assert_error(
            router
                .clone()
                .oneshot(legacy_header)
                .await
                .expect("router should respond"),
            StatusCode::FORBIDDEN,
            "MANAGEMENT_CSRF_INVALID",
        )
        .await;

        let mut protected = test_request(
            Method::POST,
            "/api/v1/test-write",
            "localhost:7080",
            peer,
            Body::empty(),
        );
        protected.headers_mut().insert(
            header::ORIGIN,
            HeaderValue::from_static("http://localhost:7080"),
        );
        protected.headers_mut().insert(
            header::COOKIE,
            HeaderValue::from_str(&cookie).expect("cookie should encode"),
        );
        protected.headers_mut().insert(
            CSRF_HEADER_NAME,
            HeaderValue::from_str(&csrf).expect("CSRF should encode"),
        );
        assert_eq!(
            router
                .clone()
                .oneshot(protected)
                .await
                .expect("router should respond")
                .status(),
            StatusCode::NO_CONTENT
        );

        policy.generation.fetch_add(1, Ordering::AcqRel);
        let mut revoked = test_request(
            Method::POST,
            "/api/v1/test-write",
            "localhost:7080",
            peer,
            Body::empty(),
        );
        revoked.headers_mut().insert(
            header::ORIGIN,
            HeaderValue::from_static("http://localhost:7080"),
        );
        revoked.headers_mut().insert(
            header::COOKIE,
            HeaderValue::from_str(&cookie).expect("cookie should encode"),
        );
        revoked.headers_mut().insert(
            CSRF_HEADER_NAME,
            HeaderValue::from_str(&csrf).expect("CSRF should encode"),
        );
        assert_error(
            router
                .clone()
                .oneshot(revoked)
                .await
                .expect("router should respond"),
            StatusCode::UNAUTHORIZED,
            "MANAGEMENT_SESSION_INVALID",
        )
        .await;
        cleanup.cancel();
    }

    #[tokio::test]
    async fn host_admission_does_not_turn_loopback_reverse_proxies_into_local_clients() {
        let policy = test_policy(
            ManagementWebProfile::Native,
            false,
            Vec::new(),
            &["proxy.example:7080"],
            None,
            None,
            false,
        );
        let routes = Router::new().route("/api/v1/test-read", get(|| async { "ok" }));
        let (router, cleanup) = test_router(routes, policy);
        let peer = SocketAddr::from(([127, 0, 0, 1], 49_101));

        let mut spoofed = test_request(
            Method::GET,
            "/api/v1/test-read",
            "evil.example:7080",
            peer,
            Body::empty(),
        );
        spoofed.headers_mut().insert(
            "x-forwarded-host",
            HeaderValue::from_static("localhost:7080"),
        );
        let denied = router
            .clone()
            .oneshot(spoofed)
            .await
            .expect("router should respond");
        assert_eq!(
            denied
                .headers()
                .get(header::X_CONTENT_TYPE_OPTIONS)
                .and_then(|value| value.to_str().ok()),
            Some("nosniff")
        );
        assert!(denied.headers().contains_key("x-request-id"));
        assert_eq!(
            denied
                .headers()
                .get(header::CONTENT_SECURITY_POLICY)
                .and_then(|value| value.to_str().ok()),
            Some(MANAGEMENT_CONTENT_SECURITY_POLICY)
        );
        assert_error(denied, StatusCode::FORBIDDEN, "MANAGEMENT_HOST_DENIED").await;

        assert_error(
            router
                .clone()
                .oneshot(test_request(
                    Method::GET,
                    "/api/v1/test-read",
                    "proxy.example:7080",
                    peer,
                    Body::empty(),
                ))
                .await
                .expect("router should respond"),
            StatusCode::UNAUTHORIZED,
            "MANAGEMENT_SESSION_INVALID",
        )
        .await;

        let mut duplicate_host = test_request(
            Method::GET,
            "/api/v1/test-read",
            "localhost:7080",
            peer,
            Body::empty(),
        );
        duplicate_host
            .headers_mut()
            .append(header::HOST, HeaderValue::from_static("localhost:7080"));
        assert_error(
            router
                .oneshot(duplicate_host)
                .await
                .expect("router should respond"),
            StatusCode::FORBIDDEN,
            "MANAGEMENT_HOST_DENIED",
        )
        .await;
        cleanup.cancel();
    }

    #[tokio::test]
    async fn login_framework_rejections_use_stable_json_errors() {
        let origin = "http://localhost:7080";
        let policy = test_policy(
            ManagementWebProfile::Native,
            false,
            Vec::new(),
            &[],
            Some(test_access(origin)),
            None,
            false,
        );
        let (router, cleanup) = test_router(Router::new(), policy);
        let peer = SocketAddr::from(([127, 0, 0, 1], 49_108));

        let mut malformed = test_request(
            Method::POST,
            "/api/v1/auth/login",
            "localhost:7080",
            peer,
            Body::from(r#"{"password":"#),
        );
        malformed
            .headers_mut()
            .insert(header::ORIGIN, HeaderValue::from_static(origin));
        malformed.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        assert_error(
            router
                .clone()
                .oneshot(malformed)
                .await
                .expect("router should respond"),
            StatusCode::BAD_REQUEST,
            "MANAGEMENT_WEB_JSON_INVALID",
        )
        .await;

        let mut wrong_content_type = test_request(
            Method::POST,
            "/api/v1/auth/login",
            "localhost:7080",
            peer,
            Body::from(r#"{"password":"password"}"#),
        );
        wrong_content_type
            .headers_mut()
            .insert(header::ORIGIN, HeaderValue::from_static(origin));
        assert_error(
            router
                .clone()
                .oneshot(wrong_content_type)
                .await
                .expect("router should respond"),
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "MANAGEMENT_WEB_JSON_CONTENT_TYPE_REQUIRED",
        )
        .await;

        let mut oversized = test_request(
            Method::POST,
            "/api/v1/auth/login",
            "localhost:7080",
            peer,
            Body::empty(),
        );
        oversized
            .headers_mut()
            .insert(header::ORIGIN, HeaderValue::from_static(origin));
        oversized.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        oversized.headers_mut().insert(
            header::CONTENT_LENGTH,
            HeaderValue::from_str(&(MAX_REQUEST_BYTES + 1).to_string())
                .expect("content length should encode"),
        );
        let oversized = router
            .clone()
            .oneshot(oversized)
            .await
            .expect("router should respond");
        assert_eq!(
            oversized
                .headers()
                .get(header::CACHE_CONTROL)
                .and_then(|value| value.to_str().ok()),
            Some(AUTH_CACHE_CONTROL),
        );
        assert_error(
            oversized,
            StatusCode::PAYLOAD_TOO_LARGE,
            "MANAGEMENT_WEB_REQUEST_TOO_LARGE",
        )
        .await;

        let mut slow = test_request(
            Method::POST,
            "/api/v1/auth/login",
            "localhost:7080",
            peer,
            Body::new(PendingBody::idle()),
        );
        slow.headers_mut()
            .insert(header::ORIGIN, HeaderValue::from_static(origin));
        slow.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        assert_error(
            router
                .clone()
                .oneshot(slow)
                .await
                .expect("router should respond"),
            StatusCode::REQUEST_TIMEOUT,
            "MANAGEMENT_WEB_REQUEST_BODY_TIMEOUT",
        )
        .await;

        let method = router
            .oneshot(test_request(
                Method::DELETE,
                "/api/v1/auth/login",
                "localhost:7080",
                peer,
                Body::empty(),
            ))
            .await
            .expect("router should respond");
        assert_eq!(
            method
                .headers()
                .get(header::CACHE_CONTROL)
                .and_then(|value| value.to_str().ok()),
            Some(AUTH_CACHE_CONTROL),
        );
        assert_error(
            method,
            StatusCode::METHOD_NOT_ALLOWED,
            "MANAGEMENT_WEB_METHOD_NOT_ALLOWED",
        )
        .await;
        cleanup.cancel();
    }

    #[tokio::test]
    async fn login_total_timeout_has_a_distinct_stable_error() {
        let response =
            complete_login_request(std::future::pending::<Response>(), Duration::from_millis(1))
                .await;
        assert_error(
            response,
            StatusCode::REQUEST_TIMEOUT,
            "MANAGEMENT_WEB_LOGIN_TIMEOUT",
        )
        .await;
    }

    #[tokio::test]
    async fn authenticated_lan_allows_optional_cidr_and_exact_https_origin_on_http() {
        let origin = "https://cmclient.example";
        let access = test_access(origin);
        let policy = test_policy(
            ManagementWebProfile::Native,
            true,
            Vec::new(),
            &["cmclient.example"],
            Some(Arc::clone(&access)),
            None,
            true,
        );
        let routes = Router::new().route(
            "/api/v1/test-write",
            post(|| async { StatusCode::NO_CONTENT }),
        );
        let (router, cleanup) = test_router(routes, policy);
        let peer = SocketAddr::from(([192, 0, 2, 10], 49_102));
        let mut login = test_request(
            Method::POST,
            "/api/v1/auth/login",
            "cmclient.example",
            peer,
            Body::from(r#"{"password":"password"}"#),
        );
        login
            .headers_mut()
            .insert(header::ORIGIN, HeaderValue::from_static(origin));
        login.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        login
            .headers_mut()
            .insert("x-forwarded-for", HeaderValue::from_static("127.0.0.1"));
        let response = router
            .clone()
            .oneshot(login)
            .await
            .expect("router should respond");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(header::CACHE_CONTROL)
                .and_then(|value| value.to_str().ok()),
            Some(AUTH_CACHE_CONTROL)
        );
        assert_eq!(
            response
                .headers()
                .get("x-cmclient-management-warning")
                .and_then(|value| value.to_str().ok()),
            Some("MANAGEMENT_HTTP_LAN_WARNING")
        );
        let cookie = response_cookie(&response);
        let payload = response_json(response).await;
        assert_eq!(payload["schemaVersion"], 1);
        let csrf = payload["csrfToken"]
            .as_str()
            .expect("login should contain CSRF");

        let mut protected = test_request(
            Method::POST,
            "/api/v1/test-write",
            "cmclient.example",
            peer,
            Body::empty(),
        );
        protected
            .headers_mut()
            .insert(header::ORIGIN, HeaderValue::from_static(origin));
        protected.headers_mut().insert(
            header::COOKIE,
            HeaderValue::from_str(&cookie).expect("cookie should encode"),
        );
        protected.headers_mut().insert(
            CSRF_HEADER_NAME,
            HeaderValue::from_str(csrf).expect("CSRF should encode"),
        );
        assert_eq!(
            router
                .clone()
                .oneshot(protected)
                .await
                .expect("router should respond")
                .status(),
            StatusCode::NO_CONTENT
        );
        assert!(
            access
                .audit_snapshot()
                .iter()
                .any(|entry| entry.action == "login" && entry.outcome == "allowed")
        );

        let restricted = test_policy(
            ManagementWebProfile::Native,
            true,
            vec![IpNet::from_str("198.51.100.0/24").expect("CIDR should parse")],
            &["cmclient.example"],
            Some(access),
            None,
            false,
        );
        let (restricted, restricted_cleanup) = test_router(Router::new(), restricted);
        assert_error(
            restricted
                .oneshot(test_request(
                    Method::POST,
                    "/api/v1/auth/login",
                    "cmclient.example",
                    peer,
                    Body::from(r#"{"password":"password"}"#),
                ))
                .await
                .expect("router should respond"),
            StatusCode::FORBIDDEN,
            "MANAGEMENT_PEER_DENIED",
        )
        .await;
        cleanup.cancel();
        restricted_cleanup.cancel();
    }

    #[tokio::test]
    async fn forwarded_addresses_cannot_evade_the_login_rate_limit() {
        let policy = test_policy(
            ManagementWebProfile::Native,
            false,
            Vec::new(),
            &[],
            None,
            None,
            false,
        );
        let (router, cleanup) = test_router(Router::new(), policy);
        let peer = SocketAddr::from(([127, 0, 0, 1], 49_103));
        let mut requests = Vec::new();
        for index in 0..6 {
            let mut request = test_request(
                Method::GET,
                "/api/v1/auth/session",
                "localhost:7080",
                peer,
                Body::empty(),
            );
            request.headers_mut().insert(
                "x-forwarded-for",
                HeaderValue::from_str(&format!("198.51.100.{index}"))
                    .expect("forwarded address should encode"),
            );
            let router = router.clone();
            requests.push(tokio::spawn(async move {
                router
                    .oneshot(request)
                    .await
                    .expect("router should respond")
            }));
        }
        let mut responses = Vec::new();
        for request in requests {
            responses.push(request.await.expect("request task should complete"));
        }
        assert_eq!(
            responses
                .iter()
                .filter(|response| response.status() == StatusCode::OK)
                .count(),
            5
        );
        let limited = responses
            .into_iter()
            .find(|response| response.status() == StatusCode::TOO_MANY_REQUESTS)
            .expect("sixth request should be rate limited by peer IP");
        assert_eq!(
            limited
                .headers()
                .get(header::CACHE_CONTROL)
                .and_then(|value| value.to_str().ok()),
            Some(AUTH_CACHE_CONTROL)
        );
        assert_error(
            limited,
            StatusCode::TOO_MANY_REQUESTS,
            "MANAGEMENT_LOGIN_RATE_LIMITED",
        )
        .await;
        cleanup.cancel();
    }

    #[tokio::test]
    async fn static_fallback_preserves_api_and_asset_misses_and_serves_spa_routes() {
        let directory = TestDirectory::new("static");
        std::fs::write(directory.path().join("index.html"), "<main>CMClient</main>")
            .expect("index should write");
        let assets = directory.path().join("assets");
        std::fs::create_dir_all(&assets).expect("asset directory should be created");
        std::fs::write(assets.join("index-BRHyKwF0.js"), "console.log('hashed')")
            .expect("hashed asset should write");
        std::fs::write(assets.join("app.js"), "console.log('plain')")
            .expect("non-hashed asset should write");
        let root = canonical_static_root(Some(directory.path()))
            .expect("static root should validate")
            .expect("static root should be configured");
        let policy = test_policy(
            ManagementWebProfile::Native,
            false,
            Vec::new(),
            &[],
            None,
            Some(root),
            false,
        );
        let (router, cleanup) = test_router(Router::new(), policy);
        let peer = SocketAddr::from(([127, 0, 0, 1], 49_104));

        let direct_index = router
            .clone()
            .oneshot(test_request(
                Method::GET,
                "/index.html",
                "localhost:7080",
                peer,
                Body::empty(),
            ))
            .await
            .expect("router should respond");
        assert_eq!(direct_index.status(), StatusCode::OK);
        assert_eq!(
            direct_index
                .headers()
                .get(header::CACHE_CONTROL)
                .and_then(|value| value.to_str().ok()),
            Some(SHELL_CACHE_CONTROL)
        );
        drop(
            to_bytes(direct_index.into_body(), 1_024)
                .await
                .expect("index response should read"),
        );

        let hashed = router
            .clone()
            .oneshot(test_request(
                Method::GET,
                "/assets/index-BRHyKwF0.js",
                "localhost:7080",
                peer,
                Body::empty(),
            ))
            .await
            .expect("router should respond");
        assert_eq!(hashed.status(), StatusCode::OK);
        assert_eq!(
            hashed
                .headers()
                .get(header::CACHE_CONTROL)
                .and_then(|value| value.to_str().ok()),
            Some(IMMUTABLE_ASSET_CACHE_CONTROL)
        );
        drop(
            to_bytes(hashed.into_body(), 1_024)
                .await
                .expect("hashed response should read"),
        );

        let non_hashed = router
            .clone()
            .oneshot(test_request(
                Method::GET,
                "/assets/app.js",
                "localhost:7080",
                peer,
                Body::empty(),
            ))
            .await
            .expect("router should respond");
        assert_eq!(non_hashed.status(), StatusCode::OK);
        assert_eq!(
            non_hashed
                .headers()
                .get(header::CACHE_CONTROL)
                .and_then(|value| value.to_str().ok()),
            Some(SHELL_CACHE_CONTROL)
        );
        drop(
            to_bytes(non_hashed.into_body(), 1_024)
                .await
                .expect("non-hashed response should read"),
        );

        assert_error(
            router
                .clone()
                .oneshot(test_request(
                    Method::GET,
                    "/api",
                    "localhost:7080",
                    peer,
                    Body::empty(),
                ))
                .await
                .expect("router should respond"),
            StatusCode::NOT_FOUND,
            "API_ROUTE_NOT_FOUND",
        )
        .await;
        assert_error(
            router
                .clone()
                .oneshot(test_request(
                    Method::GET,
                    "/missing.js",
                    "localhost:7080",
                    peer,
                    Body::empty(),
                ))
                .await
                .expect("router should respond"),
            StatusCode::NOT_FOUND,
            "WEB_ASSET_NOT_FOUND",
        )
        .await;
        let spa = router
            .oneshot(test_request(
                Method::GET,
                "/settings/advanced",
                "localhost:7080",
                peer,
                Body::empty(),
            ))
            .await
            .expect("router should respond");
        assert_eq!(spa.status(), StatusCode::OK);
        assert_eq!(
            spa.headers()
                .get(header::CACHE_CONTROL)
                .and_then(|value| value.to_str().ok()),
            Some(SHELL_CACHE_CONTROL)
        );
        assert_eq!(
            to_bytes(spa.into_body(), 1_024)
                .await
                .expect("SPA response should read"),
            "<main>CMClient</main>"
        );
        cleanup.cancel();
    }

    #[test]
    fn static_root_rejects_links_and_windows_reparse_points() {
        let directory = TestDirectory::new("static-link");
        let root = directory.path().join("web");
        let target = directory.path().join("outside");
        std::fs::create_dir_all(&root).expect("web root should be created");
        std::fs::create_dir_all(&target).expect("link target should be created");
        std::fs::write(root.join("index.html"), "index").expect("index should write");
        std::fs::write(target.join("secret.txt"), "secret").expect("target should write");
        let link = root.join("linked");
        create_directory_link(&target, &link);

        assert!(matches!(
            canonical_static_root(Some(&root)),
            Err(ManagementWebError::InvalidConfiguration)
        ));
        remove_directory_link(&link);
    }

    #[tokio::test]
    async fn response_body_lifetime_limit_returns_the_stable_capacity_error() {
        let routes = Router::new().route(
            "/api/v1/stream",
            get(|| async { Response::new(Body::new(PendingBody::idle())) }),
        );
        let policy = test_policy(
            ManagementWebProfile::Native,
            false,
            Vec::new(),
            &[],
            None,
            None,
            false,
        );
        let (router, cleanup) = test_router(routes, policy);
        let peer = SocketAddr::from(([127, 0, 0, 1], 49_105));
        let mut active = Vec::new();
        for _ in 0..MAX_RESPONSE_BODIES {
            let response = tokio::time::timeout(
                Duration::from_secs(1),
                router.clone().oneshot(test_request(
                    Method::GET,
                    "/api/v1/stream",
                    "localhost:7080",
                    peer,
                    Body::empty(),
                )),
            )
            .await
            .expect("stream response headers should not wait")
            .expect("router should respond");
            assert_eq!(response.status(), StatusCode::OK);
            active.push(response);
        }

        let overloaded = tokio::time::timeout(
            Duration::from_secs(1),
            router.oneshot(test_request(
                Method::GET,
                "/api/v1/stream",
                "localhost:7080",
                peer,
                Body::empty(),
            )),
        )
        .await
        .expect("overloaded request must not wait for a response body slot")
        .expect("router should return capacity response");
        assert_eq!(
            overloaded
                .headers()
                .get(header::X_CONTENT_TYPE_OPTIONS)
                .and_then(|value| value.to_str().ok()),
            Some("nosniff")
        );
        assert!(overloaded.headers().contains_key("x-request-id"));
        assert_error(
            overloaded,
            StatusCode::SERVICE_UNAVAILABLE,
            "MANAGEMENT_WEB_CONNECTION_LIMIT_REACHED",
        )
        .await;
        drop(active);
        cleanup.cancel();
    }

    #[cfg(unix)]
    fn create_directory_link(target: &Path, link: &Path) {
        std::os::unix::fs::symlink(target, link).expect("directory symlink should be created");
    }

    #[cfg(unix)]
    fn remove_directory_link(link: &Path) {
        std::fs::remove_file(link).expect("directory symlink should be removed");
    }

    #[cfg(windows)]
    fn create_directory_link(target: &Path, link: &Path) {
        let status = std::process::Command::new("cmd")
            .args(["/c", "mklink", "/J"])
            .arg(link)
            .arg(target)
            .status()
            .expect("junction command should run");
        assert!(status.success(), "directory junction should be created");
    }

    #[cfg(windows)]
    fn remove_directory_link(link: &Path) {
        std::fs::remove_dir(link).expect("directory junction should be removed");
    }

    #[test]
    fn route_capability_is_redacted_and_rotation_revokes_owned_lease() {
        let first = GatewayRoute::new(SocketAddr::from(([127, 0, 0, 1], 4810)), "a".repeat(64))
            .expect("route should construct");
        assert!(!format!("{first:?}").contains(&"a".repeat(64)));
        let session = GatewaySessionHandle::with_route(first.clone());
        let active = first.active().expect("route should be active");
        let session_for_rotation = session.clone();
        let rotation = thread::spawn(move || {
            session_for_rotation.set(
                GatewayRoute::new(SocketAddr::from(([127, 0, 0, 1], 4811)), "b".repeat(64))
                    .expect("replacement route should construct"),
            );
        });
        thread::sleep(Duration::from_millis(20));
        assert!(!first.is_active());
        assert!(!rotation.is_finished());
        drop(active);
        rotation.join().expect("rotation should finish");
        assert_eq!(
            session.snapshot().expect("new route").address().port(),
            4811
        );
    }

    #[test]
    fn replacement_is_not_snapshot_visible_until_previous_route_is_inactive() {
        let previous = GatewayRoute::new(SocketAddr::from(([127, 0, 0, 1], 4_810)), "a".repeat(64))
            .expect("previous route should construct");
        let session = GatewaySessionHandle::with_route(previous.clone());
        let replacement =
            GatewayRoute::new(SocketAddr::from(([127, 0, 0, 1], 4_811)), "b".repeat(64))
                .expect("replacement route should construct");

        let previous_state = previous
            .lease
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let session_for_set = session.clone();
        let set_worker = thread::spawn(move || session_for_set.set(replacement));

        let observation_deadline = Instant::now() + Duration::from_secs(2);
        let replacement_published_before_deactivation = loop {
            match session.route.try_read() {
                Ok(current) => {
                    if current
                        .as_ref()
                        .is_some_and(|route| route.address().port() == 4_811)
                    {
                        break true;
                    }
                    assert!(
                        Instant::now() < observation_deadline,
                        "route replacement did not enter its cutover"
                    );
                    thread::yield_now();
                }
                Err(std::sync::TryLockError::WouldBlock) => break false,
                Err(std::sync::TryLockError::Poisoned(error)) => {
                    panic!("route lock was poisoned: {error}")
                }
            }
        };
        let session_for_snapshot = session.clone();
        let (snapshot_sender, snapshot_receiver) = std::sync::mpsc::sync_channel(1);
        let snapshot_worker = thread::spawn(move || {
            snapshot_sender
                .send(
                    session_for_snapshot
                        .snapshot()
                        .expect("replacement route should become visible"),
                )
                .expect("snapshot receiver should remain available");
        });

        let visible_before_deactivation = snapshot_receiver
            .recv_timeout(Duration::from_millis(50))
            .ok();
        assert!(previous_state.active);
        drop(previous_state);

        let published = visible_before_deactivation.clone().unwrap_or_else(|| {
            snapshot_receiver
                .recv_timeout(Duration::from_secs(2))
                .expect("replacement snapshot should publish after deactivation")
        });
        set_worker.join().expect("route replacement should finish");
        snapshot_worker
            .join()
            .expect("snapshot observer should finish");

        assert!(
            !replacement_published_before_deactivation,
            "replacement route was published while the previous route was still active"
        );
        assert!(
            visible_before_deactivation.is_none(),
            "replacement became snapshot-visible before the previous route was deactivated"
        );
        assert!(!previous.is_active());
        assert_eq!(published.address().port(), 4_811);
    }

    #[test]
    fn clear_and_set_are_bounded_when_a_stale_lease_is_never_dropped() {
        assert!(GATEWAY_LEASE_DRAIN_TIMEOUT <= Duration::from_secs(2));

        let cleared_route =
            GatewayRoute::new(SocketAddr::from(([127, 0, 0, 1], 4_810)), "e".repeat(64))
                .expect("route should construct");
        let cleared_session = GatewaySessionHandle::with_route(cleared_route.clone());
        let stale_clear_lease = cleared_route.active().expect("route should be active");
        let clear_cancellation = stale_clear_lease.cancellation_token();
        let session_to_clear = cleared_session.clone();
        let clear_elapsed = bounded_sync_operation(move || session_to_clear.clear());
        assert!(clear_elapsed < Duration::from_secs(1));
        assert!(clear_cancellation.is_cancelled());
        assert!(!cleared_route.is_active());
        assert!(cleared_session.snapshot().is_none());

        let replaced_route =
            GatewayRoute::new(SocketAddr::from(([127, 0, 0, 1], 4_811)), "f".repeat(64))
                .expect("route should construct");
        let replaced_session = GatewaySessionHandle::with_route(replaced_route.clone());
        let stale_set_lease = replaced_route.active().expect("route should be active");
        let set_cancellation = stale_set_lease.cancellation_token();
        let replacement =
            GatewayRoute::new(SocketAddr::from(([127, 0, 0, 1], 4_812)), "0".repeat(64))
                .expect("replacement should construct");
        let session_to_set = replaced_session.clone();
        let set_elapsed = bounded_sync_operation(move || session_to_set.set(replacement));
        assert!(set_elapsed < Duration::from_secs(1));
        assert!(set_cancellation.is_cancelled());
        assert!(!replaced_route.is_active());
        assert_eq!(
            replaced_session
                .snapshot()
                .expect("replacement should be installed")
                .address()
                .port(),
            4_812
        );

        drop(stale_clear_lease);
        drop(stale_set_lease);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn route_rotation_wakes_an_idle_body_and_releases_its_owned_lease() {
        let route = GatewayRoute::new(SocketAddr::from(([127, 0, 0, 1], 4_810)), "d".repeat(64))
            .expect("route should construct");
        let session = GatewaySessionHandle::with_route(route);
        let active = session.active().expect("route should be active");
        let cancellation = active.cancellation_token();
        let polled = Arc::new(AtomicBool::new(false));
        let mut body = RevocableBody::new(
            PendingBody::observed(Arc::clone(&polled)),
            cancellation,
            active,
        );
        let body_task = tokio::spawn(async move {
            let frame = tokio::time::timeout(Duration::from_secs(2), body.frame())
                .await
                .expect("rotation should wake an idle body");
            assert!(frame.is_none(), "rotation should terminate the body");
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !polled.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("body should register its cancellation waker");

        let session_to_clear = session.clone();
        tokio::time::timeout(
            Duration::from_secs(2),
            tokio::task::spawn_blocking(move || session_to_clear.clear()),
        )
        .await
        .expect("clear should finish after the idle body wakes")
        .expect("clear task should not panic");
        body_task.await.expect("body task should not panic");
        assert!(session.snapshot().is_none());
    }

    #[tokio::test]
    async fn request_and_response_trailers_cannot_bypass_gateway_header_allowlists() {
        let route = GatewayRoute::new(SocketAddr::from(([127, 0, 0, 1], 4_810)), "1".repeat(64))
            .expect("route should construct");
        let active = route.active().expect("route should be active");
        let request = Request::builder()
            .uri("/api/v1/events")
            .body(Body::new(TrailerBody::new(forbidden_trailers())))
            .expect("request should construct");
        let outbound = gateway_request(request, &active).expect("request should sanitize");
        let mut request_body = outbound.into_body();
        assert!(
            tokio::time::timeout(Duration::from_secs(1), request_body.frame())
                .await
                .expect("request trailer filtering should make bounded progress")
                .is_none(),
            "request trailer frame must be dropped entirely"
        );

        let response_active = route.active().expect("route should remain active");
        let cancellation = response_active.cancellation_token();
        let mut response_body = RevocableBody::new(
            TrailerBody::new(forbidden_trailers()),
            cancellation,
            response_active,
        );
        assert!(
            tokio::time::timeout(Duration::from_secs(1), response_body.frame())
                .await
                .expect("response trailer filtering should make bounded progress")
                .is_none(),
            "response trailer frame must be dropped entirely"
        );
    }

    #[test]
    fn gateway_request_strips_spoofed_and_hop_headers_but_preserves_event_cursor() {
        let route = GatewayRoute::new(SocketAddr::from(([127, 0, 0, 1], 4810)), "c".repeat(64))
            .expect("route should construct");
        let active = route.active().expect("route should be active");
        let request = Request::builder()
            .uri("/api/v1/events?after=9")
            .header(header::HOST, "localhost:7080")
            .header(GATEWAY_CAPABILITY_HEADER, "spoofed")
            .header(header::CONNECTION, "x-private, keep-alive")
            .header("x-private", "remove-me")
            .header("last-event-id", "domain:9")
            .header("x-trace-id", "trace-9")
            .header(header::RANGE, "bytes=0-99")
            .header(header::IF_NONE_MATCH, "\"event-9\"")
            .header(header::COOKIE, "cmclient.sid=must-not-cross")
            .header(header::AUTHORIZATION, "Bearer must-not-cross")
            .header(header::ORIGIN, "https://must-not-cross.example")
            .header(header::REFERER, "https://must-not-cross.example/page")
            .header(CSRF_HEADER_NAME, "must-not-cross")
            .body(Body::empty())
            .expect("request should construct");
        let outbound = gateway_request(request, &active).expect("request should sanitize");
        assert_eq!(
            outbound.headers()[GATEWAY_CAPABILITY_HEADER],
            HeaderValue::from_str(&"c".repeat(64)).expect("capability header")
        );
        assert_eq!(outbound.headers()["last-event-id"], "domain:9");
        assert_eq!(outbound.headers()["x-trace-id"], "trace-9");
        assert_eq!(outbound.headers()[header::RANGE], "bytes=0-99");
        assert_eq!(outbound.headers()[header::IF_NONE_MATCH], "\"event-9\"");
        assert!(!outbound.headers().contains_key("x-private"));
        assert!(!outbound.headers().contains_key(header::CONNECTION));
        assert!(!outbound.headers().contains_key(header::COOKIE));
        assert!(!outbound.headers().contains_key(header::AUTHORIZATION));
        assert!(!outbound.headers().contains_key(header::ORIGIN));
        assert!(!outbound.headers().contains_key(header::REFERER));
        assert!(!outbound.headers().contains_key(CSRF_HEADER_NAME));
        assert_eq!(outbound.uri().path(), "/api/v1/events");
        assert_eq!(outbound.uri().query(), Some("after=9"));

        let mut response_headers = axum::http::HeaderMap::new();
        response_headers.insert(
            GATEWAY_CAPABILITY_HEADER,
            HeaderValue::from_static("must-not-reflect"),
        );
        response_headers.insert(header::CONNECTION, HeaderValue::from_static("upgrade"));
        response_headers.insert(header::UPGRADE, HeaderValue::from_static("websocket"));
        response_headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/event-stream"),
        );
        response_headers.insert(header::ETAG, HeaderValue::from_static("\"event-9\""));
        response_headers.insert(
            header::SET_COOKIE,
            HeaderValue::from_static("gateway=denied"),
        );
        response_headers.insert(
            header::ACCESS_CONTROL_ALLOW_ORIGIN,
            HeaderValue::from_static("*"),
        );
        response_headers.insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        strip_hop_headers(&mut response_headers);
        let response_headers =
            allowlisted_headers(&response_headers, GATEWAY_RESPONSE_HEADER_ALLOWLIST);
        assert_eq!(response_headers[header::CONTENT_TYPE], "text/event-stream");
        assert_eq!(response_headers[header::ETAG], "\"event-9\"");
        assert!(!response_headers.contains_key(GATEWAY_CAPABILITY_HEADER));
        assert!(!response_headers.contains_key(header::CONNECTION));
        assert!(!response_headers.contains_key(header::SET_COOKIE));
        assert!(!response_headers.contains_key(header::ACCESS_CONTROL_ALLOW_ORIGIN));
        assert!(!response_headers.contains_key(header::WWW_AUTHENTICATE));
    }

    #[test]
    fn binds_both_wildcard_families_and_stops_without_custom_socket_workers() {
        let mut service = ManagementWebService::start(
            &ManagementWebConfig {
                port: 0,
                ..ManagementWebConfig::default()
            },
            Router::new().route("/health", get(|| async { "ok" })),
            None,
            GatewaySessionHandle::new(),
        )
        .expect("dual-stack service should start");
        assert_eq!(service.addresses().len(), 2);
        assert!(service.addresses()[0].ip().is_unspecified());
        assert!(service.addresses()[1].ip().is_unspecified());
        assert_eq!(service.addresses()[0].port(), service.addresses()[1].port());
        assert!(service.advertised_url().starts_with("http://127.0.0.1:"));
        service.stop().expect("service should stop");
    }

    #[test]
    fn docker_and_native_lan_fail_closed_without_auth_and_cidr() {
        let docker = ManagementWebService::start(
            &ManagementWebConfig {
                port: 0,
                profile: ManagementWebProfile::Docker,
                ..ManagementWebConfig::default()
            },
            Router::new(),
            None,
            GatewaySessionHandle::new(),
        );
        assert!(matches!(
            docker,
            Err(ManagementWebError::InvalidConfiguration)
        ));
        let native_lan = ManagementWebService::start(
            &ManagementWebConfig {
                port: 0,
                allow_lan: true,
                ..ManagementWebConfig::default()
            },
            Router::new(),
            None,
            GatewaySessionHandle::new(),
        );
        assert!(matches!(
            native_lan,
            Err(ManagementWebError::InvalidConfiguration)
        ));
    }

    #[test]
    fn forbidden_handwritten_web_and_access_machinery_does_not_return() {
        let web_source = include_str!("web.rs");
        for forbidden in [
            concat!("ManagementWeb", "ApiHandler"),
            concat!("ManagementWeb", "Listener"),
            concat!("ManagementWeb", "Request"),
            concat!("ManagementWeb", "Stream"),
            concat!("Server", "Connection"),
            concat!("Stream", "Owned"),
            concat!("read", "_request"),
            concat!("parse", "_request"),
            concat!("write", "_response"),
        ] {
            assert!(
                !web_source.contains(forbidden),
                "forbidden token: {forbidden}"
            );
        }
        let access_source = include_str!("access.rs");
        for forbidden in [
            concat!("Session", "Record"),
            concat!("Failure", "Window"),
            concat!("PasswordVerification", "Limiter"),
        ] {
            assert!(
                !access_source.contains(forbidden),
                "forbidden token: {forbidden}"
            );
        }
    }
}
