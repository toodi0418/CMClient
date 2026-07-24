use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{any, get},
};
use chrono::{SecondsFormat, Utc};
use cmclient_agent_core::access::ManagementAccessController;
use cmclient_agent_core::secrets::{AgentSecretStore, SecretKind, SecretStoreError};
use cmclient_agent_core::setup::{SetupPhase, SetupStore};
use cmclient_agent_core::web::{
    ActiveGatewayRoute, GATEWAY_CAPABILITY_HEADER, GatewayRoute, GatewaySessionHandle,
    ManagementTlsConfig, ManagementWebConfig, ManagementWebError, ManagementWebProfile,
    ManagementWebService,
};
use cmclient_agent_core::{
    AgentConfig, AgentLease, AgentRuntimeProfile, AprsConfig, MeshtasticConnectionConfig,
    RuntimePaths, ensure_runtime_directories,
};
use cmclient_control_api::{
    ControlClient, ControlCommand, ControlEndpoint, ControlError, ControlHandler,
    ControlSecretKind, ControlServer, ControlStatus, ControlUpdateEvent, DiagnosticsControlBundle,
    GatewayControlStatus, GatewayProjection, InternalComponent, ManagementWebControlStatus,
    UpdateControlJob, UpdateControlStatus, compiled_component_identity, default_local_endpoint,
};
use cmclient_legacy_migration::{
    ChildGatewayMaintenanceRunner, GatewayMaintenanceRunner, ProductMigrationSourceSet,
    migrate_detected_product_source_sets,
};
use cmclient_runtime_logging::{LogLevel, LogPolicy, StructuredLogSink};
use cmclient_supervisor::{
    BackoffPolicy, GatewayCommand, GatewayLogHealthUpdate, GatewayReady, GatewayStatus,
    GatewaySupervisor, SupervisorEvent,
};
use cmclient_updater::{PersistentUpdateJob, UpdateJournalStore, recover_interrupted_update};
use std::{
    collections::BTreeMap,
    env,
    ffi::OsString,
    fs,
    io::{BufRead, BufReader, Read},
    path::{Path, PathBuf},
    process::ExitCode,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{self, SyncSender, TrySendError},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use tokio_stream::{StreamExt, wrappers::BroadcastStream};

#[cfg(test)]
use std::sync::atomic::AtomicUsize;

const EX_USAGE: u8 = 2;
const EX_CONFIG: u8 = 5;
const UPDATE_EVENT_BUFFER: usize = 64;
const MAX_SSE_EVENT_BYTES: usize = 60 * 1024;
const SUPERVISOR_POLL_INTERVAL: Duration = Duration::from_millis(100);
const GATEWAY_SSE_READ_POLL_INTERVAL: Duration = Duration::from_secs(1);
const GATEWAY_SSE_RECONNECT_DELAY: Duration = Duration::from_secs(2);
const AGENT_LOG_FILE: &str = "agent.jsonl";
const GATEWAY_LOG_FILE: &str = "gateway.jsonl";
const MAX_GATEWAY_IDENTITY_BYTES: u64 = 64 * 1024;

struct AgentUpdateService {
    journal: UpdateJournalStore,
    subscribers: Mutex<Vec<SyncSender<ControlUpdateEvent>>>,
    web_events: tokio::sync::broadcast::Sender<ControlUpdateEvent>,
    next_event_id: AtomicU64,
}

impl AgentUpdateService {
    fn new(data_dir: &Path) -> Result<Self, ControlError> {
        let (web_events, _) = tokio::sync::broadcast::channel(UPDATE_EVENT_BUFFER);
        Ok(Self {
            journal: UpdateJournalStore::new(data_dir).map_err(|_| ControlError::CommandFailed)?,
            subscribers: Mutex::new(Vec::new()),
            web_events,
            next_event_id: AtomicU64::new(1),
        })
    }

    fn recover(&self) -> Result<(), ControlError> {
        let recovered = recover_interrupted_update(&self.journal, utc_now())
            .map_err(|_| ControlError::CommandFailed)?;
        if let Some(job) = recovered {
            self.persist(&job)?;
        }
        Ok(())
    }

    fn persist(&self, job: &PersistentUpdateJob) -> Result<(), ControlError> {
        self.journal
            .persist(job)
            .map_err(|_| ControlError::CommandFailed)?;
        self.publish(job)
    }

    fn status(&self) -> Result<UpdateControlStatus, ControlError> {
        let job = self
            .journal
            .load()
            .map_err(|_| ControlError::CommandFailed)?
            .map(update_control_job);
        Ok(UpdateControlStatus {
            schema_version: 1,
            job,
        })
    }

    fn subscribe(&self) -> Result<mpsc::Receiver<ControlUpdateEvent>, ControlError> {
        let (sender, receiver) = mpsc::sync_channel(UPDATE_EVENT_BUFFER);
        sender
            .try_send(self.event_for(&self.status()?)?)
            .map_err(|_| ControlError::CommandFailed)?;
        self.subscribers
            .lock()
            .map_err(|_| ControlError::CommandFailed)?
            .push(sender);
        Ok(receiver)
    }

    fn subscribe_web(
        &self,
    ) -> Result<
        (
            ControlUpdateEvent,
            tokio::sync::broadcast::Receiver<ControlUpdateEvent>,
        ),
        ControlError,
    > {
        let _event_order = self
            .subscribers
            .lock()
            .map_err(|_| ControlError::CommandFailed)?;
        let receiver = self.web_events.subscribe();
        Ok((self.web_snapshot()?, receiver))
    }

    fn web_snapshot(&self) -> Result<ControlUpdateEvent, ControlError> {
        self.event_for(&self.status()?)
    }

    fn publish(&self, job: &PersistentUpdateJob) -> Result<(), ControlError> {
        let status = UpdateControlStatus {
            schema_version: 1,
            job: Some(update_control_job(job.clone())),
        };
        let mut subscribers = self
            .subscribers
            .lock()
            .map_err(|_| ControlError::CommandFailed)?;
        let event = self.event_for(&status)?;
        subscribers.retain(|sender| sender.try_send(event.clone()).is_ok());
        let _ = self.web_events.send(event);
        Ok(())
    }

    fn event_for(&self, status: &UpdateControlStatus) -> Result<ControlUpdateEvent, ControlError> {
        let sequence = self.next_event_id.fetch_add(1, Ordering::Relaxed);
        Ok(ControlUpdateEvent {
            id: format!("update-{sequence}"),
            event: String::from("update.status_changed"),
            data: serde_json::to_vec(status).map_err(|_| ControlError::CommandFailed)?,
        })
    }
}

fn update_control_job(job: PersistentUpdateJob) -> UpdateControlJob {
    let (bytes_downloaded, bytes_total, bytes_per_second) =
        job.progress.map_or((None, None, None), |progress| {
            (
                Some(progress.bytes_downloaded),
                progress.bytes_total,
                progress.bytes_per_second,
            )
        });
    UpdateControlJob {
        id: job.id,
        phase: job.phase.as_str().to_owned(),
        updated_at: job.updated_at,
        error_code: job.error_code,
        bytes_downloaded,
        bytes_total,
        bytes_per_second,
        recent_log_codes: job
            .recent_logs
            .into_iter()
            .map(|entry| entry.code)
            .collect(),
    }
}

fn utc_now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn agent_web_router(updates: Arc<AgentUpdateService>) -> Router {
    Router::new()
        .route("/api/v1/updates", get(management_update_status))
        .route("/api/v1/updates/events", get(management_update_events))
        .route(
            "/api/v1/control/{*path}",
            any(management_control_route_not_found),
        )
        .route("/api/v1/control", any(management_control_route_not_found))
        .with_state(updates)
}

async fn management_update_status(State(updates): State<Arc<AgentUpdateService>>) -> Response {
    match tokio::task::spawn_blocking(move || updates.status()).await {
        Ok(Ok(status)) => (StatusCode::OK, Json(status)).into_response(),
        Ok(Err(_)) | Err(_) => management_control_failed_response(),
    }
}

async fn management_update_events(State(updates): State<Arc<AgentUpdateService>>) -> Response {
    let subscription = tokio::task::spawn_blocking(move || updates.subscribe_web()).await;
    let (snapshot, receiver) = match subscription {
        Ok(Ok(subscription)) => subscription,
        Ok(Err(_)) | Err(_) => return management_control_failed_response(),
    };
    let snapshot = match update_web_sse_event(snapshot) {
        Ok(event) => event,
        Err(_) => return management_control_failed_response(),
    };
    let events = tokio_stream::once(Ok::<_, std::io::Error>(snapshot)).chain(
        BroadcastStream::new(receiver).map(|event| match event {
            Ok(event) => update_web_sse_event(event),
            Err(_) => Err(std::io::Error::other("update event subscriber lagged")),
        }),
    );
    Sse::new(events)
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("heartbeat"),
        )
        .into_response()
}

async fn management_control_route_not_found() -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({"code": "CONTROL_ROUTE_NOT_FOUND"})),
    )
        .into_response()
}

fn management_control_failed_response() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(serde_json::json!({"code": "CONTROL_COMMAND_FAILED"})),
    )
        .into_response()
}

fn update_web_sse_event(event: ControlUpdateEvent) -> Result<Event, std::io::Error> {
    if !is_safe_sse_token(&event.id)
        || !is_safe_sse_token(&event.event)
        || event.data.len() > MAX_SSE_EVENT_BYTES
        || event.data.iter().any(|byte| matches!(*byte, b'\r' | b'\n'))
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "update event is not valid SSE data",
        ));
    }
    let data = String::from_utf8(event.data).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "update event data is not UTF-8",
        )
    })?;
    Ok(Event::default().id(event.id).event(event.event).data(data))
}

fn is_safe_sse_token(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
}

struct AgentController {
    identity: cmclient_control_api::ComponentIdentityReport,
    supervisor: Mutex<Option<GatewaySupervisor>>,
    gateway_transition: Mutex<()>,
    private_gateway_bootstrap: bool,
    runtime_log: Option<StructuredLogSink>,
    agent_log_error_code: Mutex<Option<String>>,
    gateway_log_health: Mutex<GatewayRuntimeLogHealth>,
    gateway_session: GatewaySessionHandle,
    management_web: Mutex<Option<ManagementWebService>>,
    management_web_shutdown: Mutex<Option<JoinHandle<Result<(), ManagementWebError>>>>,
    management_web_config: ManagementWebConfig,
    management_access: Option<Arc<ManagementAccessController>>,
    control_endpoint: ControlEndpoint,
    secrets: AgentSecretStore,
    setup: Arc<SetupStore>,
    setup_gate_required: bool,
    updates: Arc<AgentUpdateService>,
    shutdown_requested: AtomicBool,
    started_at: Instant,
    latest_error_code: Mutex<Option<String>>,
}

#[derive(Default)]
struct GatewayRuntimeLogHealth {
    capture_error_code: Option<String>,
    write_error_code: Option<String>,
}

impl GatewayRuntimeLogHealth {
    fn current_error_code(&self) -> Option<String> {
        self.capture_error_code
            .clone()
            .or_else(|| self.write_error_code.clone())
    }
}

struct SupervisorWorker {
    shutdown: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl SupervisorWorker {
    fn start(controller: Arc<AgentController>) -> Result<Self, ControlError> {
        let shutdown = Arc::new(AtomicBool::new(false));
        let worker_shutdown = Arc::clone(&shutdown);
        let worker = thread::Builder::new()
            .name(String::from("cmclient-gateway-supervisor"))
            .spawn(move || {
                while !worker_shutdown.load(Ordering::Acquire) {
                    let _ = controller.tick_supervisor();
                    thread::sleep(SUPERVISOR_POLL_INTERVAL);
                }
            })
            .map_err(|_| ControlError::CommandFailed)?;
        Ok(Self {
            shutdown,
            worker: Some(worker),
        })
    }

    fn stop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl Drop for SupervisorWorker {
    fn drop(&mut self) {
        self.stop();
    }
}

fn apply_aprs_environment(environment: &mut BTreeMap<String, String>, aprs: Option<&AprsConfig>) {
    let Some(aprs) = aprs else {
        return;
    };
    environment.insert(String::from("CMCLIENT_APRS_ENABLED"), String::from("true"));
    if let Some(host) = &aprs.host {
        environment.insert(String::from("CMCLIENT_APRS_HOST"), host.clone());
    }
    if let Some(port) = aprs.port {
        environment.insert(String::from("CMCLIENT_APRS_PORT"), port.to_string());
    }
    if let Some(destination) = &aprs.destination {
        environment.insert(
            String::from("CMCLIENT_APRS_DESTINATION"),
            destination.clone(),
        );
    }
}

fn apply_physical_qualification_environment(
    environment: &mut BTreeMap<String, String>,
    configured: Option<&str>,
    stage: Option<&str>,
) -> Result<(), ControlError> {
    let Some(configured) = configured.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(());
    };
    if ["0", "false", "no", "off"]
        .iter()
        .any(|value| configured.eq_ignore_ascii_case(value))
    {
        return Ok(());
    }
    if !["1", "true", "yes", "on"]
        .iter()
        .any(|value| configured.eq_ignore_ascii_case(value))
    {
        return Err(ControlError::CommandFailed);
    }
    let stage = stage
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("windows-source-smoke");
    if stage.len() > 128
        || !stage
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
    {
        return Err(ControlError::CommandFailed);
    }
    environment.insert(
        String::from("CMCLIENT_MESHTASTIC_PHYSICAL_PROFILE"),
        String::from("true"),
    );
    environment.insert(
        String::from("CMCLIENT_QUALIFICATION_STAGE"),
        stage.to_owned(),
    );
    Ok(())
}

fn physical_source_smoke_enabled() -> bool {
    std::env::var("CMCLIENT_MESHTASTIC_PHYSICAL_PROFILE")
        .ok()
        .is_some_and(|value| {
            ["1", "true", "yes", "on"]
                .iter()
                .any(|enabled| value.trim().eq_ignore_ascii_case(enabled))
        })
}

fn setup_gate_required(config: &AgentConfig) -> bool {
    setup_gate_required_with_profile(config, physical_source_smoke_enabled())
}

fn setup_gate_required_with_profile(config: &AgentConfig, physical_profile: bool) -> bool {
    let has_external_configuration = config.callmesh.is_some()
        || config.meshtastic.is_some()
        || config.aprs.is_some()
        || config.proxy.is_some();
    if !has_external_configuration {
        return false;
    }
    // The physical source-smoke profile is a campaign-only observation path.
    // It is valid only when CallMesh/APRS/Proxy are absent; P18's product
    // guard then owns the one Meshtastic socket and its allowlisted write.
    if physical_profile
        && config.callmesh.is_none()
        && config.aprs.is_none()
        && config.proxy.is_none()
    {
        return false;
    }
    true
}

const fn management_web_profile(profile: AgentRuntimeProfile) -> ManagementWebProfile {
    match profile {
        AgentRuntimeProfile::Native => ManagementWebProfile::Native,
        AgentRuntimeProfile::Docker => ManagementWebProfile::Docker,
    }
}

impl AgentController {
    fn from_config(config: &AgentConfig) -> Result<Self, ControlError> {
        let secrets =
            AgentSecretStore::runtime(config.paths.root_dir()).map_err(control_secret_error)?;
        Self::from_config_with_secrets(config, secrets)
    }

    fn from_config_with_secrets(
        config: &AgentConfig,
        secrets: AgentSecretStore,
    ) -> Result<Self, ControlError> {
        Self::from_config_with_secrets_internal(config, secrets, true)
    }

    #[cfg(test)]
    fn from_config_with_secrets_without_private_bootstrap(
        config: &AgentConfig,
        secrets: AgentSecretStore,
    ) -> Result<Self, ControlError> {
        Self::from_config_with_secrets_internal(config, secrets, false)
    }

    fn from_config_with_secrets_internal(
        config: &AgentConfig,
        secrets: AgentSecretStore,
        private_bootstrap: bool,
    ) -> Result<Self, ControlError> {
        let identity = compiled_component_identity(InternalComponent::Agent)
            .map_err(|_| ControlError::CommandFailed)?;
        let setup =
            Arc::new(SetupStore::open(&config.paths).map_err(|_| ControlError::CommandFailed)?);
        let setup_gate_required = setup_gate_required(config);
        if setup_gate_required
            && secrets
                .read(SecretKind::CallMeshApiKey)
                .map_err(control_secret_error)?
                .is_none()
            && matches!(
                setup
                    .snapshot()
                    .map_err(|_| ControlError::CommandFailed)?
                    .phase,
                SetupPhase::Ready
            )
        {
            setup
                .require_credentials()
                .map_err(|_| ControlError::CommandFailed)?;
        }
        let setup_generation = setup
            .generation()
            .map_err(|_| ControlError::CommandFailed)?
            .generation();
        let mut initial_log_error_code = None;
        let mut initial_agent_log_error_code = None;
        let mut initial_gateway_log_error_code = None;
        let log_policy = match LogPolicy::from_environment() {
            Ok(policy) => Some(policy),
            Err(error) => {
                initial_log_error_code = Some(String::from(error.code()));
                initial_agent_log_error_code = Some(String::from(error.code()));
                None
            }
        };
        let runtime_log = log_policy.and_then(|policy| {
            match StructuredLogSink::open(&config.paths.log_dir, AGENT_LOG_FILE, "agent", policy) {
                Ok(sink) => Some(sink),
                Err(error) => {
                    remember_initial_log_error(&mut initial_log_error_code, error.code());
                    initial_agent_log_error_code = Some(String::from(error.code()));
                    None
                }
            }
        });
        let control_endpoint = default_local_endpoint(config.paths.root_dir())?;
        let gateway_command = resolve_gateway_command(config);
        let supervisor = gateway_command
            .as_ref()
            .map(|command| {
                let mut supervisor = GatewaySupervisor::new(
                    GatewayCommand {
                        program: command.first().cloned().unwrap_or_default(),
                        arguments: command.iter().skip(1).cloned().collect(),
                    },
                    BackoffPolicy::default(),
                )
                .map_err(|_| ControlError::CommandFailed)?;
                let mut environment = BTreeMap::from([
                    (String::from("CMCLIENT_SUPERVISED"), String::from("1")),
                    (
                        String::from("CMCLIENT_BUILD_VERSION"),
                        identity.identity.version.clone(),
                    ),
                    (
                        String::from("CMCLIENT_BUILD_COMMIT"),
                        identity.identity.source_commit.clone(),
                    ),
                    (
                        String::from("CMCLIENT_BUILD_TREE"),
                        identity.identity.source_tree.clone(),
                    ),
                    (
                        String::from("CMCLIENT_BUILD_CHANNEL"),
                        String::from(identity.identity.channel.as_str()),
                    ),
                    (
                        String::from("CMCLIENT_RUNTIME_PROFILE"),
                        String::from(identity.identity.target.profile.as_str()),
                    ),
                    (
                        String::from("CMCLIENT_PACKAGE_PROFILE"),
                        String::from(identity.identity.target.package_profile.as_str()),
                    ),
                    (
                        String::from("CMCLIENT_TARGET_OS"),
                        String::from(identity.identity.target.os.as_str()),
                    ),
                    (
                        String::from("CMCLIENT_TARGET_ARCHITECTURE"),
                        String::from(identity.identity.target.architecture.as_str()),
                    ),
                    (
                        String::from("CMCLIENT_RUNTIME_ROOT"),
                        config.paths.root_dir().to_string_lossy().into_owned(),
                    ),
                    (
                        String::from("CMCLIENT_DB_PATH"),
                        config.paths.database_file().to_string_lossy().into_owned(),
                    ),
                    (
                        String::from("CMCLIENT_BACKUP_DIR"),
                        config.paths.backups_dir().to_string_lossy().into_owned(),
                    ),
                ]);
                let physical_profile = std::env::var("CMCLIENT_MESHTASTIC_PHYSICAL_PROFILE").ok();
                let qualification_stage = std::env::var("CMCLIENT_QUALIFICATION_STAGE").ok();
                apply_physical_qualification_environment(
                    &mut environment,
                    physical_profile.as_deref(),
                    qualification_stage.as_deref(),
                )?;
                if let Some(callmesh) = &config.callmesh {
                    environment.insert(String::from("CMCLIENT_CALLMESH_URL"), callmesh.url.clone());
                }
                if let Some(meshtastic) = &config.meshtastic {
                    environment.insert(
                        String::from("CMCLIENT_MESH_NETWORK_ID"),
                        meshtastic.mesh_network_id.clone(),
                    );
                    environment.insert(
                        String::from("CMCLIENT_GATEWAY_ID"),
                        meshtastic.gateway_id.clone(),
                    );
                    match &meshtastic.connection {
                        MeshtasticConnectionConfig::Tcp { host, port } => {
                            environment.insert(
                                String::from("CMCLIENT_MESHTASTIC_TRANSPORT"),
                                String::from("tcp"),
                            );
                            environment
                                .insert(String::from("CMCLIENT_MESHTASTIC_TCP_HOST"), host.clone());
                            environment.insert(
                                String::from("CMCLIENT_MESHTASTIC_TCP_PORT"),
                                port.to_string(),
                            );
                        }
                        MeshtasticConnectionConfig::Serial { path, baud_rate } => {
                            environment.insert(
                                String::from("CMCLIENT_MESHTASTIC_TRANSPORT"),
                                String::from("serial"),
                            );
                            environment.insert(
                                String::from("CMCLIENT_MESHTASTIC_SERIAL_PATH"),
                                path.clone(),
                            );
                            environment.insert(
                                String::from("CMCLIENT_MESHTASTIC_SERIAL_BAUD"),
                                baud_rate.to_string(),
                            );
                        }
                    }
                }
                apply_aprs_environment(&mut environment, config.aprs.as_ref());
                if let Some(proxy) = &config.proxy {
                    environment
                        .insert(String::from("CMCLIENT_PROXY_ENABLED"), String::from("true"));
                    environment.insert(String::from("CMCLIENT_PROXY_HOST"), proxy.host.clone());
                    environment.insert(String::from("CMCLIENT_PROXY_PORT"), proxy.port.to_string());
                    environment.insert(
                        String::from("CMCLIENT_PROXY_UPSTREAM_HOST"),
                        proxy.upstream_host.clone(),
                    );
                    environment.insert(
                        String::from("CMCLIENT_PROXY_UPSTREAM_PORT"),
                        proxy.upstream_port.to_string(),
                    );
                    environment.insert(String::from("CMCLIENT_PROXY_MODE"), proxy.mode.clone());
                    environment.insert(
                        String::from("CMCLIENT_PROXY_ALLOW_LAN"),
                        proxy.allow_lan.to_string(),
                    );
                    if !proxy.allowlist.is_empty() {
                        environment.insert(
                            String::from("CMCLIENT_PROXY_ALLOWLIST"),
                            proxy.allowlist.join(","),
                        );
                    }
                }
                supervisor.set_environment(environment);
                if private_bootstrap {
                    supervisor
                        .enable_private_bootstrap()
                        .map_err(|_| ControlError::CommandFailed)?;
                    if let Some(api_key) = secrets
                        .read(SecretKind::CallMeshApiKey)
                        .map_err(control_secret_error)?
                    {
                        supervisor
                            .set_callmesh_api_key(api_key.expose_secret())
                            .map_err(|_| ControlError::CommandFailed)?;
                    }
                }
                if let Some(policy) = log_policy {
                    match StructuredLogSink::open(
                        &config.paths.log_dir,
                        GATEWAY_LOG_FILE,
                        "gateway",
                        policy,
                    ) {
                        Ok(sink) => supervisor
                            .set_log_sink(sink)
                            .map_err(|_| ControlError::CommandFailed)?,
                        Err(error) => {
                            remember_initial_log_error(&mut initial_log_error_code, error.code());
                            initial_gateway_log_error_code = Some(String::from(error.code()));
                        }
                    }
                }
                Ok(supervisor)
            })
            .transpose()?;
        let management_access = config
            .management_lan
            .as_ref()
            .map(|lan| {
                ManagementAccessController::new(lan.access.clone())
                    .map(Arc::new)
                    .map_err(|_| ControlError::CommandFailed)
            })
            .transpose()?;
        let management_tls = match config.management_lan.as_ref() {
            Some(lan) => match (&lan.certificate_path, &lan.private_key_path) {
                (Some(certificate_path), Some(private_key_path)) => Some(ManagementTlsConfig {
                    certificate_path: certificate_path.clone(),
                    private_key_path: private_key_path.clone(),
                }),
                (None, None) => None,
                _ => return Err(ControlError::CommandFailed),
            },
            None => None,
        };
        let management_web_config = ManagementWebConfig {
            enabled: true,
            port: config.management_lan.as_ref().map_or(7080, |lan| lan.port),
            profile: management_web_profile(config.runtime_profile),
            setup_generation,
            allow_lan: management_access.is_some(),
            allowed_cidrs: config
                .management_lan
                .as_ref()
                .map_or_else(Vec::new, |lan| lan.allowed_cidrs.clone()),
            allowed_hosts: config
                .management_lan
                .as_ref()
                .map_or_else(Default::default, |lan| lan.allowed_hosts.clone()),
            tls: management_tls,
            static_web_root: Some(resolve_static_web_root()),
        };
        let gateway_session = GatewaySessionHandle::new();
        let updates = Arc::new(AgentUpdateService::new(config.paths.root_dir())?);
        updates.recover()?;
        let management_web = if config.management_web_enabled {
            Some(
                ManagementWebService::start(
                    &management_web_config,
                    agent_web_router(Arc::clone(&updates)),
                    management_access.clone(),
                    gateway_session.clone(),
                )
                .map_err(|_| ControlError::CommandFailed)?,
            )
        } else {
            None
        };
        let controller = Self {
            identity,
            supervisor: Mutex::new(supervisor),
            gateway_transition: Mutex::new(()),
            private_gateway_bootstrap: private_bootstrap,
            runtime_log,
            agent_log_error_code: Mutex::new(initial_agent_log_error_code),
            gateway_log_health: Mutex::new(GatewayRuntimeLogHealth {
                capture_error_code: None,
                write_error_code: initial_gateway_log_error_code,
            }),
            gateway_session,
            management_web: Mutex::new(management_web),
            management_web_shutdown: Mutex::new(None),
            management_web_config,
            management_access,
            control_endpoint,
            secrets,
            setup,
            setup_gate_required,
            updates,
            shutdown_requested: AtomicBool::new(false),
            started_at: Instant::now(),
            latest_error_code: Mutex::new(initial_log_error_code),
        };
        controller.log_agent_code(LogLevel::Info, "AGENT_RUNTIME_READY");
        Ok(controller)
    }

    fn status(&self) -> Result<ControlStatus, ControlError> {
        self.tick_supervisor()?;
        let setup_blocked = self.setup_blocked()?;
        let mut supervisor = self
            .supervisor
            .lock()
            .map_err(|_| ControlError::CommandFailed)?;
        let lifecycle = match supervisor.as_mut() {
            Some(supervisor) => match supervisor.status() {
                GatewayStatus::Stopped => GatewayControlStatus::Stopped,
                GatewayStatus::Running { .. } => GatewayControlStatus::Running,
                GatewayStatus::Backoff { .. } => GatewayControlStatus::Backoff,
            },
            None => GatewayControlStatus::Stopped,
        };
        drop(supervisor);
        let gateway_route = self.gateway_session.snapshot();
        let gateway = match lifecycle {
            GatewayControlStatus::Running
                if gateway_route
                    .as_ref()
                    .is_some_and(gateway_health_with_route) =>
            {
                GatewayControlStatus::Running
            }
            GatewayControlStatus::Running => GatewayControlStatus::Degraded,
            status => status,
        };
        let (management_web, management_web_url) = self
            .management_web
            .lock()
            .map_err(|_| ControlError::CommandFailed)?
            .as_ref()
            .map_or((ManagementWebControlStatus::Disabled, None), |service| {
                (
                    ManagementWebControlStatus::Running,
                    Some(service.advertised_url().trim_end_matches('/').to_owned()),
                )
            });
        let remembered_error_code = self
            .latest_error_code
            .lock()
            .map_err(|_| ControlError::CommandFailed)?
            .clone();
        let persistent_log_error_code = self
            .agent_log_error_code
            .lock()
            .map_err(|_| ControlError::CommandFailed)?
            .clone()
            .or(self.current_gateway_log_error_code()?);
        let latest_error_code = if setup_blocked {
            Some(String::from("SETUP_REQUIRED"))
        } else if persistent_log_error_code.is_some() {
            persistent_log_error_code
        } else {
            match gateway {
                GatewayControlStatus::Backoff => Some(String::from("GATEWAY_RESTART_BACKOFF")),
                GatewayControlStatus::Degraded => Some(String::from("GATEWAY_HEALTH_DEGRADED")),
                _ => remembered_error_code,
            }
        };
        Ok(ControlStatus {
            schema_version: 3,
            agent: String::from("running"),
            identity: self.identity.clone(),
            gateway,
            management_web,
            management_web_url,
            uptime_seconds: self.started_at.elapsed().as_secs(),
            latest_error_code,
        })
    }

    fn setup_blocked(&self) -> Result<bool, ControlError> {
        if !self.setup_gate_required {
            return Ok(false);
        }
        Ok(self
            .setup
            .status()
            .map_err(|_| ControlError::CommandFailed)?
            .setup_required)
    }

    fn enable_management_web(&self) -> Result<ControlStatus, ControlError> {
        self.ensure_resource_start_allowed()?;
        if !self.reap_management_web_shutdown()? {
            return Err(ControlError::ResourceExhausted);
        }
        let mut management_web = self
            .management_web
            .lock()
            .map_err(|_| ControlError::CommandFailed)?;
        self.ensure_resource_start_allowed()?;
        if management_web.is_none() {
            *management_web = Some(
                ManagementWebService::start(
                    &self.management_web_config,
                    agent_web_router(Arc::clone(&self.updates)),
                    self.management_access.clone(),
                    self.gateway_session.clone(),
                )
                .map_err(|_| ControlError::CommandFailed)?,
            );
        }
        drop(management_web);
        self.status()
    }

    fn disable_management_web(&self) -> Result<ControlStatus, ControlError> {
        let shutdown_ready = self.reap_management_web_shutdown()?;
        if !shutdown_ready {
            return self.status();
        }
        let service = self
            .management_web
            .lock()
            .map_err(|_| ControlError::CommandFailed)?
            .take();
        if let Some(service) = service {
            let worker = thread::Builder::new()
                .name(String::from("cmclient-management-web-shutdown"))
                .spawn(move || {
                    let mut service = service;
                    service.stop()
                })
                .map_err(|_| ControlError::CommandFailed)?;
            *self
                .management_web_shutdown
                .lock()
                .map_err(|_| ControlError::CommandFailed)? = Some(worker);
        }
        self.status()
    }

    fn reap_management_web_shutdown(&self) -> Result<bool, ControlError> {
        let worker = {
            let mut shutdown = self
                .management_web_shutdown
                .lock()
                .map_err(|_| ControlError::CommandFailed)?;
            if shutdown
                .as_ref()
                .is_some_and(|worker| !worker.is_finished())
            {
                return Ok(false);
            }
            shutdown.take()
        };
        match worker {
            Some(worker) => worker
                .join()
                .map_err(|_| ControlError::CommandFailed)?
                .map_err(|_| ControlError::CommandFailed),
            None => Ok(()),
        }?;
        Ok(true)
    }

    fn stop_management_web(&self) -> Result<(), ControlError> {
        let service = self
            .management_web
            .lock()
            .map_err(|_| ControlError::CommandFailed)?
            .take();
        let service_result = service.map_or(Ok(()), |mut service| {
            service.stop().map_err(|_| ControlError::CommandFailed)
        });
        let worker = self
            .management_web_shutdown
            .lock()
            .map_err(|_| ControlError::CommandFailed)?
            .take();
        let worker_result = worker.map_or(Ok(()), |worker| {
            worker
                .join()
                .map_err(|_| ControlError::CommandFailed)?
                .map_err(|_| ControlError::CommandFailed)
        });
        service_result.and(worker_result)
    }

    fn request_shutdown(&self) {
        if self.shutdown_requested.swap(true, Ordering::AcqRel) {
            return;
        }
        self.log_agent_code(LogLevel::Info, "AGENT_SHUTDOWN_REQUESTED");
        let endpoint = self.control_endpoint.clone();
        let _ = thread::Builder::new()
            .name(String::from("cmclient-control-shutdown-wake"))
            .spawn(move || {
                let _ = ControlClient::new_with_timeout(endpoint, Duration::from_secs(2))
                    .and_then(|client| client.status());
            });
    }

    fn is_shutdown_requested(&self) -> bool {
        self.shutdown_requested.load(Ordering::Acquire)
    }

    fn ensure_resource_start_allowed(&self) -> Result<(), ControlError> {
        if self.is_shutdown_requested() {
            Err(ControlError::CommandFailed)
        } else {
            Ok(())
        }
    }

    fn remember_error(&self, error: &ControlError) {
        self.remember_error_code(error.code());
    }

    fn remember_error_code(&self, code: &str) {
        let sink_available = self.runtime_log.is_some();
        let log_error_code = self.write_agent_code(LogLevel::Error, code);
        if let Ok(mut latest_error_code) = self.latest_error_code.lock() {
            if let Ok(mut agent_log_error_code) = self.agent_log_error_code.lock() {
                if let Some(log_error_code) = log_error_code {
                    let log_error_code = String::from(log_error_code);
                    *latest_error_code = Some(log_error_code.clone());
                    *agent_log_error_code = Some(log_error_code);
                } else {
                    if sink_available {
                        *agent_log_error_code = None;
                    }
                    *latest_error_code = Some(String::from(code));
                }
            } else {
                *latest_error_code = Some(String::from(log_error_code.unwrap_or(code)));
            }
        }
    }

    fn apply_gateway_log_health(&self, update: GatewayLogHealthUpdate) {
        let (reported_error_code, recovered_error_code, current_gateway_error_code) = {
            let Ok(mut health) = self.gateway_log_health.lock() else {
                return;
            };
            if let Some(code) = update.capture_error_code {
                health.capture_error_code = Some(String::from(code));
            }
            if let Some(code) = update.write_error_code {
                health.write_error_code = Some(String::from(code));
            }
            let recovered_error_code = update.write_recovered_code.and_then(|recovered| {
                if health.write_error_code.as_deref() == Some(recovered) {
                    health.write_error_code.take()
                } else {
                    None
                }
            });
            let reported_error_code = update.capture_error_code.map(String::from).or_else(|| {
                update.write_error_code.and_then(|code| {
                    (health.write_error_code.as_deref() == Some(code)).then(|| String::from(code))
                })
            });
            (
                reported_error_code,
                recovered_error_code,
                health.current_error_code(),
            )
        };

        if let Some(code) = reported_error_code {
            self.remember_error_code(&code);
            return;
        }
        let Some(recovered_error_code) = recovered_error_code else {
            return;
        };
        let agent_log_error_code = self
            .agent_log_error_code
            .lock()
            .ok()
            .and_then(|code| code.clone());
        if let Ok(mut latest_error_code) = self.latest_error_code.lock() {
            if latest_error_code.as_deref() == Some(recovered_error_code.as_str()) {
                *latest_error_code = agent_log_error_code.or(current_gateway_error_code);
            }
        }
    }

    fn current_gateway_log_error_code(&self) -> Result<Option<String>, ControlError> {
        self.gateway_log_health
            .lock()
            .map_err(|_| ControlError::CommandFailed)
            .map(|health| health.current_error_code())
    }

    fn clear_error(&self) {
        if let Ok(mut latest_error_code) = self.latest_error_code.lock() {
            if latest_error_code
                .as_deref()
                .is_none_or(|code| !is_runtime_log_error(code))
            {
                *latest_error_code = None;
            }
        }
    }

    fn log_agent_code(&self, level: LogLevel, code: &str) {
        let sink_available = self.runtime_log.is_some();
        let log_error_code = self.write_agent_code(level, code);
        let gateway_log_error_code = self.current_gateway_log_error_code().ok().flatten();
        if let Ok(mut latest_error_code) = self.latest_error_code.lock() {
            if let Ok(mut agent_log_error_code) = self.agent_log_error_code.lock() {
                if let Some(error_code) = log_error_code {
                    let error_code = String::from(error_code);
                    *latest_error_code = Some(error_code.clone());
                    *agent_log_error_code = Some(error_code);
                } else if sink_available {
                    let owns_latest =
                        latest_error_code.as_deref() == agent_log_error_code.as_deref();
                    let recovered = if owns_latest {
                        *latest_error_code = gateway_log_error_code;
                        true
                    } else {
                        true
                    };
                    if recovered {
                        *agent_log_error_code = None;
                    }
                }
            } else if let Some(error_code) = log_error_code {
                *latest_error_code = Some(String::from(error_code));
            }
        }
    }

    fn write_agent_code(&self, level: LogLevel, code: &str) -> Option<&'static str> {
        let sink = self.runtime_log.as_ref()?;
        let write_error = sink.write_code(level, code).err().map(|error| error.code());
        let latched_error = sink.take_error_code();
        write_error.or(latched_error)
    }

    fn has_precise_supervisor_error(&self) -> bool {
        self.latest_error_code
            .lock()
            .is_ok_and(|latest_error_code| {
                latest_error_code.as_deref().is_some_and(|code| {
                    code.starts_with("GATEWAY_SUPERVISOR_") || is_runtime_log_error(code)
                })
            })
    }

    fn has_setup_required_error(&self) -> bool {
        self.latest_error_code
            .lock()
            .is_ok_and(|latest_error_code| latest_error_code.as_deref() == Some("SETUP_REQUIRED"))
    }

    fn stop_supervisor(&self) -> Result<bool, ControlError> {
        let _transition = self
            .gateway_transition
            .lock()
            .map_err(|_| ControlError::CommandFailed)?;
        self.gateway_session.clear();
        let (result, log_health) = {
            let mut supervisor = self
                .supervisor
                .lock()
                .map_err(|_| ControlError::CommandFailed)?;
            let Some(supervisor) = supervisor.as_mut() else {
                return Ok(false);
            };
            let result = supervisor.stop();
            let log_health = supervisor.take_log_health_update();
            (result, log_health)
        };
        self.apply_gateway_log_health(log_health);
        result.map_err(|error| {
            self.remember_error_code(error.code());
            ControlError::CommandFailed
        })?;
        self.log_agent_code(LogLevel::Info, "GATEWAY_SUPERVISOR_STOPPED");
        Ok(true)
    }

    fn start_supervisor(&self) -> Result<bool, ControlError> {
        if self.setup_blocked()? {
            self.remember_error_code("SETUP_REQUIRED");
            return Err(ControlError::CommandFailed);
        }
        let _transition = self
            .gateway_transition
            .lock()
            .map_err(|_| ControlError::CommandFailed)?;
        self.ensure_resource_start_allowed()?;
        let (result, ready, log_health) = {
            let mut supervisor = self
                .supervisor
                .lock()
                .map_err(|_| ControlError::CommandFailed)?;
            self.ensure_resource_start_allowed()?;
            let Some(supervisor) = supervisor.as_mut() else {
                return Ok(false);
            };
            let result = supervisor.start();
            let ready = supervisor.gateway_ready().cloned();
            let log_health = supervisor.take_log_health_update();
            (result, ready, log_health)
        };
        self.apply_gateway_log_health(log_health);
        let event = result.map_err(|error| {
            self.gateway_session.clear();
            self.remember_error_code(error.code());
            ControlError::CommandFailed
        })?;
        if self.private_gateway_bootstrap
            && (matches!(event, SupervisorEvent::Started { .. })
                || (matches!(event, SupervisorEvent::Heartbeat { .. })
                    && self.gateway_session.snapshot().is_none()))
        {
            self.publish_verified_gateway(ready)?;
        }
        if matches!(event, SupervisorEvent::Started { .. }) {
            self.log_agent_code(LogLevel::Info, "GATEWAY_SUPERVISOR_STARTED");
        }
        Ok(true)
    }

    fn tick_supervisor(&self) -> Result<(), ControlError> {
        if self.setup_blocked()? {
            self.gateway_session.clear();
            return Ok(());
        }
        let _transition = self
            .gateway_transition
            .lock()
            .map_err(|_| ControlError::CommandFailed)?;
        if self.is_shutdown_requested() {
            return Ok(());
        }
        let (result, ready, log_health) = {
            let mut supervisor = self
                .supervisor
                .lock()
                .map_err(|_| ControlError::CommandFailed)?;
            if self.is_shutdown_requested() {
                return Ok(());
            }
            match supervisor.as_mut() {
                Some(supervisor) => {
                    let result = supervisor.tick().map(Some);
                    let ready = supervisor.gateway_ready().cloned();
                    let log_health = supervisor.take_log_health_update();
                    (result, ready, log_health)
                }
                None => (Ok(None), None, GatewayLogHealthUpdate::default()),
            }
        };
        self.apply_gateway_log_health(log_health);
        match result {
            Ok(Some(SupervisorEvent::Started { .. })) => {
                if self.private_gateway_bootstrap {
                    self.publish_verified_gateway(ready)?;
                }
                if let Ok(mut latest_error_code) = self.latest_error_code.lock() {
                    if latest_error_code
                        .as_deref()
                        .is_some_and(is_transient_supervisor_error)
                    {
                        *latest_error_code = None;
                    }
                }
                self.log_agent_code(LogLevel::Info, "GATEWAY_SUPERVISOR_STARTED");
                Ok(())
            }
            Ok(Some(SupervisorEvent::Heartbeat { .. })) => {
                if self.private_gateway_bootstrap && self.gateway_session.snapshot().is_none() {
                    self.publish_verified_gateway(ready)?;
                }
                if let Ok(mut latest_error_code) = self.latest_error_code.lock() {
                    if latest_error_code
                        .as_deref()
                        .is_some_and(is_transient_supervisor_error)
                    {
                        *latest_error_code = None;
                    }
                }
                Ok(())
            }
            Ok(Some(SupervisorEvent::Exited { .. })) => {
                self.gateway_session.clear();
                self.log_agent_code(LogLevel::Warn, "GATEWAY_SUPERVISOR_EXITED");
                Ok(())
            }
            Ok(Some(SupervisorEvent::Backoff { .. } | SupervisorEvent::Stopped)) | Ok(None) => {
                self.gateway_session.clear();
                Ok(())
            }
            Err(error) => {
                self.gateway_session.clear();
                self.remember_error_code(error.code());
                Err(ControlError::CommandFailed)
            }
        }
    }

    fn publish_verified_gateway(&self, ready: Option<GatewayReady>) -> Result<(), ControlError> {
        let result = ready
            .ok_or(ControlError::CommandFailed)
            .and_then(|ready| verified_gateway_route(&ready, &self.identity))
            .map_err(|_| ControlError::CommandFailed);
        match result {
            Ok(route) => {
                self.gateway_session.set(route);
                Ok(())
            }
            Err(error) => {
                self.gateway_session.clear();
                if let Ok(mut supervisor) = self.supervisor.lock() {
                    if let Some(supervisor) = supervisor.as_mut() {
                        let _ = supervisor.stop();
                    }
                }
                self.remember_error_code("GATEWAY_SUPERVISOR_IDENTITY_VERIFICATION_FAILED");
                Err(error)
            }
        }
    }
}

fn shutdown_agent_runtime(
    controller: &AgentController,
    supervisor_worker: &mut SupervisorWorker,
) -> Result<(), ControlError> {
    supervisor_worker.stop();
    let supervisor_result = controller.stop_supervisor().map(|_| ());
    let management_web_result = controller.stop_management_web();
    let result = supervisor_result.and(management_web_result);
    if result.is_ok() {
        controller.log_agent_code(LogLevel::Info, "AGENT_RUNTIME_STOPPED");
    }
    result
}

fn remember_initial_log_error(current: &mut Option<String>, code: &'static str) {
    if current.is_none() {
        *current = Some(String::from(code));
    }
}

fn is_runtime_log_error(code: &str) -> bool {
    code.starts_with("RUNTIME_LOG_")
}

fn is_transient_supervisor_error(code: &str) -> bool {
    matches!(
        code,
        "GATEWAY_SUPERVISOR_SPAWN_FAILED" | "GATEWAY_SUPERVISOR_PROCESS_IO_FAILED"
    )
}

impl ControlHandler for AgentController {
    fn handle(&self, command: ControlCommand) -> Result<ControlStatus, ControlError> {
        let result = match command {
            ControlCommand::Status => self.status(),
            ControlCommand::Start => {
                self.start_supervisor()?
                    .then_some(())
                    .ok_or(ControlError::CommandFailed)?;
                self.status()
            }
            ControlCommand::Stop => {
                self.stop_supervisor()?
                    .then_some(())
                    .ok_or(ControlError::CommandFailed)?;
                self.status()
            }
            ControlCommand::Restart => {
                self.ensure_resource_start_allowed()?;
                self.stop_supervisor()?
                    .then_some(())
                    .ok_or(ControlError::CommandFailed)?;
                self.start_supervisor()?
                    .then_some(())
                    .ok_or(ControlError::CommandFailed)?;
                self.status()
            }
            ControlCommand::ShutdownAgent => {
                self.request_shutdown();
                self.status()
            }
            ControlCommand::EnableManagementWeb => self.enable_management_web(),
            ControlCommand::DisableManagementWeb => self.disable_management_web(),
        };
        match &result {
            Ok(_) if !matches!(command, ControlCommand::Status) => self.clear_error(),
            Ok(_) => {}
            Err(ControlError::CommandFailed) if self.has_setup_required_error() => {}
            Err(ControlError::CommandFailed) if self.has_precise_supervisor_error() => {}
            Err(error) => self.remember_error(error),
        }
        result
    }

    fn prepare_command(&self, command: ControlCommand) -> Result<ControlStatus, ControlError> {
        if command != ControlCommand::ShutdownAgent {
            return self.handle(command);
        }
        let result = self.status();
        match &result {
            Ok(_) => self.clear_error(),
            Err(ControlError::CommandFailed) if self.has_precise_supervisor_error() => {}
            Err(error) => self.remember_error(error),
        }
        result
    }

    fn command_response_sent(&self, command: ControlCommand) {
        if command == ControlCommand::ShutdownAgent {
            self.request_shutdown();
        }
    }

    fn update_status(&self) -> Result<UpdateControlStatus, ControlError> {
        self.updates.status()
    }

    fn subscribe_update_events(&self) -> Result<mpsc::Receiver<ControlUpdateEvent>, ControlError> {
        self.updates.subscribe()
    }

    fn diagnostics_bundle(&self) -> Result<DiagnosticsControlBundle, ControlError> {
        let status = self.status()?;
        let update = self.updates.status()?;
        Ok(DiagnosticsControlBundle {
            schema_version: 2,
            identity: status.identity,
            gateway: status.gateway,
            management_web: status.management_web,
            latest_error_code: status.latest_error_code,
            update_error_code: update.job.as_ref().and_then(|job| job.error_code.clone()),
            update_log_codes: update.job.map_or_else(Vec::new, |job| job.recent_log_codes),
        })
    }

    fn gateway_projection(
        &self,
        projection: GatewayProjection,
    ) -> Result<serde_json::Value, ControlError> {
        let route = self
            .gateway_session
            .snapshot()
            .ok_or(ControlError::CommandFailed)?;
        gateway_json_projection(&route, projection)
    }

    fn subscribe_gateway_events(&self) -> Result<mpsc::Receiver<ControlUpdateEvent>, ControlError> {
        Ok(bridge_gateway_events(self.gateway_session.clone()))
    }

    fn store_secret(&self, kind: ControlSecretKind, value: &str) -> Result<(), ControlError> {
        if matches!(
            kind,
            ControlSecretKind::AprsPasscode | ControlSecretKind::ManagementAdminToken
        ) {
            return Err(ControlError::SecretKindDeprecated);
        }
        self.secrets
            .store(secret_kind(kind), value)
            .map_err(control_secret_error)
    }

    fn remove_secret(&self, kind: ControlSecretKind) -> Result<bool, ControlError> {
        self.secrets
            .remove(secret_kind(kind))
            .map_err(control_secret_error)
    }
}

fn verified_gateway_route(
    ready: &GatewayReady,
    expected: &cmclient_control_api::ComponentIdentityReport,
) -> Result<GatewayRoute, ControlError> {
    let route = GatewayRoute::new(ready.address, ready.capability.clone())
        .map_err(|_| ControlError::CommandFailed)?;
    let client = reqwest::blocking::Client::builder()
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(3))
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .build()
        .map_err(|_| ControlError::CommandFailed)?;
    let active_route = route.active().ok_or(ControlError::CommandFailed)?;
    let response = client
        .get(format!(
            "http://{}/api/v1/system/version",
            active_route.address()
        ))
        .header("accept", "application/json")
        .header(GATEWAY_CAPABILITY_HEADER, active_route.capability())
        .send()
        .map_err(map_gateway_request_error)?;
    if !response.status().is_success()
        || response
            .content_length()
            .is_some_and(|length| length > MAX_GATEWAY_IDENTITY_BYTES)
    {
        return Err(ControlError::CommandFailed);
    }
    let mut bytes = Vec::new();
    response
        .take(MAX_GATEWAY_IDENTITY_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(map_gateway_io_error)?;
    if !route.is_active() {
        return Err(ControlError::CommandFailed);
    }
    if bytes.len() as u64 > MAX_GATEWAY_IDENTITY_BYTES {
        return Err(ControlError::ResponseTooLarge);
    }
    let actual: cmclient_control_api::ComponentIdentityReport =
        serde_json::from_slice(&bytes).map_err(|_| ControlError::InvalidEnvelope)?;
    if !route.is_active()
        || actual.validate().is_err()
        || actual.component != InternalComponent::Gateway
        || actual.identity != expected.identity
    {
        return Err(ControlError::CommandFailed);
    }
    drop(active_route);
    Ok(route)
}

fn gateway_json_projection(
    route: &GatewayRoute,
    projection: GatewayProjection,
) -> Result<serde_json::Value, ControlError> {
    const MAX_GATEWAY_PROJECTION_BYTES: u64 = 2 * 1024 * 1024;
    let (method, path) = match projection {
        GatewayProjection::Meshtastic => (reqwest::Method::GET, "/api/v1/meshtastic"),
        GatewayProjection::Nodes => (reqwest::Method::GET, "/api/v1/nodes"),
        GatewayProjection::Positions => (reqwest::Method::GET, "/api/v1/positions"),
        GatewayProjection::Aprs => (reqwest::Method::GET, "/api/v1/aprs"),
        GatewayProjection::CallMesh => (reqwest::Method::GET, "/api/v1/callmesh"),
        GatewayProjection::Proxy => (reqwest::Method::GET, "/api/v1/proxy"),
        GatewayProjection::RecentEvents => (reqwest::Method::GET, "/api/v1/events/recent"),
        GatewayProjection::DatabaseIntegrity => {
            (reqwest::Method::POST, "/api/v1/diagnostics/integrity-check")
        }
        GatewayProjection::Backup => (reqwest::Method::POST, "/api/v1/backups"),
    };
    let client = reqwest::blocking::Client::builder()
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .build()
        .map_err(|_| ControlError::CommandFailed)?;
    let active_route = route.active().ok_or(ControlError::CommandFailed)?;
    let response = client
        .request(method, format!("http://{}{path}", active_route.address()))
        .header("accept", "application/json")
        .header(GATEWAY_CAPABILITY_HEADER, active_route.capability())
        .send()
        .map_err(map_gateway_request_error)?;
    if matches!(response.status().as_u16(), 408 | 504) {
        return Err(ControlError::Timeout);
    }
    if !response.status().is_success() {
        return Err(ControlError::CommandFailed);
    }
    if response
        .content_length()
        .is_some_and(|length| length > MAX_GATEWAY_PROJECTION_BYTES)
    {
        return Err(ControlError::ResponseTooLarge);
    }
    let mut bytes = Vec::new();
    response
        .take(MAX_GATEWAY_PROJECTION_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(map_gateway_io_error)?;
    if !route.is_active() {
        return Err(ControlError::CommandFailed);
    }
    if bytes.len() as u64 > MAX_GATEWAY_PROJECTION_BYTES {
        return Err(ControlError::ResponseTooLarge);
    }
    let projection = serde_json::from_slice(&bytes).map_err(|_| ControlError::InvalidEnvelope)?;
    if !route.is_active() {
        return Err(ControlError::CommandFailed);
    }
    drop(active_route);
    Ok(projection)
}

fn gateway_health_with_route(route: &GatewayRoute) -> bool {
    const MAX_GATEWAY_HEALTH_BYTES: u64 = 4 * 1024;
    let Some(active_route) = route.active() else {
        return false;
    };
    let result = (|| {
        let client = reqwest::blocking::Client::builder()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(2))
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .build()
            .ok()?;
        let response = client
            .get(format!(
                "http://{}/api/v1/system/health",
                active_route.address()
            ))
            .header("accept", "application/json")
            .header(GATEWAY_CAPABILITY_HEADER, active_route.capability())
            .send()
            .ok()?;
        if !response.status().is_success()
            || response
                .content_length()
                .is_some_and(|length| length > MAX_GATEWAY_HEALTH_BYTES)
        {
            return None;
        }
        let mut body = Vec::new();
        response
            .take(MAX_GATEWAY_HEALTH_BYTES + 1)
            .read_to_end(&mut body)
            .ok()?;
        if !route.is_active() {
            return None;
        }
        if body.len() as u64 > MAX_GATEWAY_HEALTH_BYTES {
            return None;
        }
        let body = serde_json::from_slice::<serde_json::Value>(&body).ok()?;
        (route.is_active() && body.get("status").and_then(serde_json::Value::as_str) == Some("ok"))
            .then_some(())
    })();
    drop(active_route);
    result.is_some()
}

fn map_gateway_request_error(error: reqwest::Error) -> ControlError {
    if error.is_timeout() {
        ControlError::Timeout
    } else {
        ControlError::CommandFailed
    }
}

fn map_gateway_io_error(error: std::io::Error) -> ControlError {
    if gateway_io_is_timeout(&error) {
        ControlError::Timeout
    } else {
        ControlError::CommandFailed
    }
}

fn gateway_io_is_timeout(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
    ) || error
        .get_ref()
        .and_then(|source| source.downcast_ref::<reqwest::Error>())
        .is_some_and(reqwest::Error::is_timeout)
}

fn bridge_gateway_events(
    gateway_session: GatewaySessionHandle,
) -> mpsc::Receiver<ControlUpdateEvent> {
    bridge_gateway_events_with_read_poll(gateway_session, GATEWAY_SSE_READ_POLL_INTERVAL)
}

fn bridge_gateway_events_with_read_poll(
    gateway_session: GatewaySessionHandle,
    read_poll: Duration,
) -> mpsc::Receiver<ControlUpdateEvent> {
    debug_assert!(!read_poll.is_zero());
    let (sender, receiver) = mpsc::sync_channel(64);
    let _ = thread::Builder::new()
        .name(String::from("cmclient-gateway-event-bridge"))
        .spawn(move || {
            #[cfg(test)]
            let _bridge_guard = GatewayEventBridgeTestGuard::new();
            let mut last_event_id = None;
            loop {
                if !probe_gateway_event_receiver(&sender) {
                    break;
                }
                let Some(route) = gateway_session.snapshot() else {
                    if !wait_for_gateway_event_retry(&sender, GATEWAY_SSE_RECONNECT_DELAY) {
                        break;
                    }
                    continue;
                };
                if !bridge_gateway_event_stream(&route, &sender, read_poll, &mut last_event_id) {
                    break;
                }
                thread::sleep(Duration::from_millis(250));
            }
        });
    receiver
}

fn bridge_gateway_event_stream(
    route: &GatewayRoute,
    sender: &SyncSender<ControlUpdateEvent>,
    read_poll: Duration,
    last_event_id: &mut Option<String>,
) -> bool {
    let mut reader = match open_gateway_event_stream(route, read_poll, last_event_id.as_deref()) {
        Ok(reader) => reader,
        Err(()) => return wait_for_gateway_event_retry(sender, GATEWAY_SSE_RECONNECT_DELAY),
    };
    let mut id: Option<String> = None;
    let mut event: Option<String> = None;
    let mut data: Option<Vec<u8>> = None;
    let mut next_receiver_probe = Instant::now() + read_poll;
    loop {
        if !reader.is_current() {
            return true;
        }
        if Instant::now() >= next_receiver_probe {
            if !probe_gateway_event_receiver(sender) {
                return false;
            }
            next_receiver_probe = Instant::now() + read_poll;
        }
        let mut line = Vec::new();
        let count = loop {
            match reader.read_line(&mut line) {
                Ok(count) => break count,
                Err(GatewaySseLineReadError::Poll) => {
                    if !reader.is_current() {
                        return true;
                    }
                    if !probe_gateway_event_receiver(sender) {
                        return false;
                    }
                    next_receiver_probe = Instant::now() + read_poll;
                }
                Err(GatewaySseLineReadError::Invalid) => {
                    return probe_gateway_event_receiver(sender);
                }
                Err(GatewaySseLineReadError::Stale) => return true,
            }
        };
        if !reader.is_current() {
            return true;
        }
        if count == 0 {
            break;
        }
        if line.last() == Some(&b'\n') {
            line.pop();
        }
        if line.last() == Some(&b'\r') {
            line.pop();
        }
        let Ok(line) = std::str::from_utf8(&line) else {
            return probe_gateway_event_receiver(sender);
        };
        if line.starts_with(':') {
            if !reader.is_current() {
                return true;
            }
            if !try_forward_gateway_event(sender, gateway_heartbeat()) {
                return false;
            }
            continue;
        }
        if line.is_empty() {
            let completed = (id.take(), event.take(), data.take());
            if let (Some(id), Some(event), Some(data)) = completed {
                if !reader.is_current() {
                    return true;
                }
                if !try_forward_gateway_event(
                    sender,
                    ControlUpdateEvent {
                        id: id.clone(),
                        event,
                        data,
                    },
                ) {
                    return false;
                }
                *last_event_id = Some(id);
            }
            continue;
        }
        let Some((field, value)) = line.split_once(':') else {
            continue;
        };
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "id" if is_safe_sse_token(value) => id = Some(value.to_owned()),
            "event" if is_safe_sse_token(value) => event = Some(value.to_owned()),
            "data"
                if !value.contains('\r')
                    && !value.contains('\n')
                    && value.len() <= MAX_SSE_EVENT_BYTES =>
            {
                data = Some(value.as_bytes().to_vec());
            }
            _ => {}
        }
    }
    if reader.is_current() {
        probe_gateway_event_receiver(sender)
    } else {
        true
    }
}

struct GatewayEventStreamReader {
    reader: BufReader<reqwest::blocking::Response>,
    route: GatewayRoute,
    _active_route: ActiveGatewayRoute,
}

impl GatewayEventStreamReader {
    fn is_current(&self) -> bool {
        self.route.is_active()
    }

    fn read_line(&mut self, output: &mut Vec<u8>) -> Result<usize, GatewaySseLineReadError> {
        if !self.is_current() {
            return Err(GatewaySseLineReadError::Stale);
        }
        let result = read_bounded_gateway_sse_line(&mut self.reader, output);
        if !self.is_current() {
            return Err(GatewaySseLineReadError::Stale);
        }
        result
    }
}

fn open_gateway_event_stream(
    route: &GatewayRoute,
    read_poll: Duration,
    last_event_id: Option<&str>,
) -> Result<GatewayEventStreamReader, ()> {
    let client = reqwest::blocking::Client::builder()
        .connect_timeout(Duration::from_secs(3))
        .timeout(read_poll)
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .build()
        .map_err(|_| ())?;
    let active_route = route.active().ok_or(())?;
    let mut request = client
        .get(format!("http://{}/api/v1/events", active_route.address()))
        .header("accept", "text/event-stream")
        .header(GATEWAY_CAPABILITY_HEADER, active_route.capability());
    if let Some(last_event_id) = last_event_id.filter(|value| is_safe_sse_token(value)) {
        request = request.header("last-event-id", last_event_id);
    }
    let response = request.send().map_err(|_| ())?;
    let content_type_valid = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("text/event-stream"));
    if response.status() != reqwest::StatusCode::OK || !content_type_valid || !route.is_active() {
        return Err(());
    }
    Ok(GatewayEventStreamReader {
        reader: BufReader::new(response),
        route: route.clone(),
        _active_route: active_route,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GatewaySseLineReadError {
    Poll,
    Invalid,
    Stale,
}

fn read_bounded_gateway_sse_line(
    reader: &mut impl BufRead,
    output: &mut Vec<u8>,
) -> Result<usize, GatewaySseLineReadError> {
    const MAX_GATEWAY_SSE_LINE_BYTES: usize = 64 * 1024;
    loop {
        let available = reader.fill_buf().map_err(|error| {
            if gateway_io_is_timeout(&error) {
                GatewaySseLineReadError::Poll
            } else {
                GatewaySseLineReadError::Invalid
            }
        })?;
        if available.is_empty() {
            return Ok(output.len());
        }
        let count = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |index| index + 1);
        if output.len().saturating_add(count) > MAX_GATEWAY_SSE_LINE_BYTES {
            return Err(GatewaySseLineReadError::Invalid);
        }
        let ended = available.get(count.saturating_sub(1)) == Some(&b'\n');
        output.extend_from_slice(&available[..count]);
        reader.consume(count);
        if ended {
            return Ok(output.len());
        }
    }
}

fn probe_gateway_event_receiver(sender: &SyncSender<ControlUpdateEvent>) -> bool {
    try_forward_gateway_event(sender, gateway_heartbeat())
}

fn try_forward_gateway_event(
    sender: &SyncSender<ControlUpdateEvent>,
    event: ControlUpdateEvent,
) -> bool {
    match sender.try_send(event) {
        Ok(()) => true,
        Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => false,
    }
}

fn wait_for_gateway_event_retry(sender: &SyncSender<ControlUpdateEvent>, delay: Duration) -> bool {
    let deadline = Instant::now() + delay;
    loop {
        if !probe_gateway_event_receiver(sender) {
            return false;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return true;
        }
        thread::sleep(remaining.min(Duration::from_secs(1)));
    }
}

#[cfg(test)]
static ACTIVE_GATEWAY_EVENT_BRIDGES: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
struct GatewayEventBridgeTestGuard;

#[cfg(test)]
impl GatewayEventBridgeTestGuard {
    fn new() -> Self {
        ACTIVE_GATEWAY_EVENT_BRIDGES.fetch_add(1, Ordering::AcqRel);
        Self
    }
}

#[cfg(test)]
impl Drop for GatewayEventBridgeTestGuard {
    fn drop(&mut self) {
        ACTIVE_GATEWAY_EVENT_BRIDGES.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(all(test, not(target_os = "windows")))]
fn active_gateway_event_bridge_count() -> usize {
    ACTIVE_GATEWAY_EVENT_BRIDGES.load(Ordering::Acquire)
}

fn gateway_heartbeat() -> ControlUpdateEvent {
    ControlUpdateEvent {
        id: format!("heartbeat-{}", Utc::now().timestamp_millis()),
        event: String::from("gateway.heartbeat"),
        data: br#"{"schemaVersion":1}"#.to_vec(),
    }
}

const fn secret_kind(kind: ControlSecretKind) -> SecretKind {
    match kind {
        ControlSecretKind::CallMeshApiKey => SecretKind::CallMeshApiKey,
        ControlSecretKind::AprsPasscode => SecretKind::AprsPasscode,
        ControlSecretKind::ManagementAdminToken => SecretKind::ManagementAdminToken,
    }
}

const fn control_secret_error(error: SecretStoreError) -> ControlError {
    match error {
        SecretStoreError::InvalidValue => ControlError::SecretValueInvalid,
        SecretStoreError::Unavailable => ControlError::SecretStoreUnavailable,
    }
}

fn load_agent_config_after_migration() -> Result<AgentConfig, String> {
    let environment = env::vars().collect::<BTreeMap<_, _>>();
    let paths =
        RuntimePaths::from_environment(&environment).map_err(|error| String::from(error.code()))?;
    let candidates = legacy_state_candidates(&environment);
    let (program, gateway_entrypoint) = resolve_gateway_maintenance_program(&environment)?;
    let maintenance = ChildGatewayMaintenanceRunner::new(program, gateway_entrypoint)
        .map_err(|error| String::from(error.code()))?;
    load_agent_config_after_migration_with(&environment, &paths, &candidates, &maintenance)
}

fn load_agent_config_after_migration_with(
    environment: &BTreeMap<String, String>,
    paths: &RuntimePaths,
    candidates: &[ProductMigrationSourceSet],
    maintenance: &dyn GatewayMaintenanceRunner,
) -> Result<AgentConfig, String> {
    migrate_detected_product_source_sets(paths.root_dir(), candidates, maintenance)
        .map_err(|error| String::from(error.code()))?;
    AgentConfig::from_environment(environment).map_err(|error| String::from(error.code()))
}

fn legacy_state_candidates(
    environment: &BTreeMap<String, String>,
) -> Vec<ProductMigrationSourceSet> {
    let mut candidates = Vec::new();
    #[cfg(target_os = "windows")]
    {
        push_single_root_legacy_candidate(
            &mut candidates,
            environment_value(environment, "APPDATA")
                .map(|root| PathBuf::from(root).join("CMClient")),
        );
        push_single_root_legacy_candidate(
            &mut candidates,
            environment_value(environment, "PROGRAMDATA")
                .map(|root| PathBuf::from(root).join("CMClient")),
        );
    }
    #[cfg(target_os = "macos")]
    push_single_root_legacy_candidate(
        &mut candidates,
        environment_value(environment, "HOME")
            .map(|root| PathBuf::from(root).join("Library/Application Support/CMClient")),
    );
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        let home = environment_value(environment, "HOME").map(PathBuf::from);
        let config_root = environment_value(environment, "XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .or_else(|| home.as_ref().map(|root| root.join(".config")))
            .map(|root| root.join("cmclient"));
        let data_root = environment_value(environment, "XDG_DATA_HOME")
            .map(PathBuf::from)
            .or_else(|| home.as_ref().map(|root| root.join(".local/share")))
            .map(|root| root.join("cmclient"));
        push_legacy_source_candidate(&mut candidates, config_root, data_root);
    }
    candidates
}

fn push_single_root_legacy_candidate(
    candidates: &mut Vec<ProductMigrationSourceSet>,
    candidate: Option<PathBuf>,
) {
    push_legacy_source_candidate(candidates, candidate.clone(), candidate);
}

fn push_legacy_source_candidate(
    candidates: &mut Vec<ProductMigrationSourceSet>,
    config_root: Option<PathBuf>,
    data_root: Option<PathBuf>,
) {
    let config_root = config_root.filter(|path| path.is_absolute());
    let data_root = data_root.filter(|path| path.is_absolute());
    let (config_root, data_root) = match (config_root, data_root) {
        (Some(config_root), Some(data_root)) => (config_root, data_root),
        (Some(root), None) | (None, Some(root)) => (root.clone(), root),
        (None, None) => return,
    };
    let candidate = ProductMigrationSourceSet {
        config_root,
        data_root,
    };
    if candidates.iter().any(|existing| {
        paths_equal(&existing.config_root, &candidate.config_root)
            && paths_equal(&existing.data_root, &candidate.data_root)
    }) {
        return;
    }
    candidates.push(candidate);
}

fn paths_equal(left: &Path, right: &Path) -> bool {
    #[cfg(target_os = "windows")]
    return left
        .to_string_lossy()
        .eq_ignore_ascii_case(&right.to_string_lossy());
    #[cfg(not(target_os = "windows"))]
    return left == right;
}

fn environment_value<'a>(environment: &'a BTreeMap<String, String>, name: &str) -> Option<&'a str> {
    environment
        .get(name)
        .or_else(|| {
            environment
                .iter()
                .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
                .map(|(_, value)| value)
        })
        .map(String::as_str)
}

fn resolve_gateway_maintenance_program(
    environment: &BTreeMap<String, String>,
) -> Result<(PathBuf, PathBuf), String> {
    let bundle = bundled_root();
    let development_entrypoint =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../gateway/dist/main.js");
    let gateway_entrypoint = explicit_runtime_file(
        environment,
        "CMCLIENT_GATEWAY_ENTRYPOINT",
        "AGENT_GATEWAY_ENTRYPOINT_INVALID",
    )?
    .or_else(|| {
        bundle
            .as_ref()
            .map(|root| root.join("gateway/dist/main.js"))
            .filter(|path| path.is_file())
    })
    .or_else(|| {
        (cfg!(debug_assertions) && development_entrypoint.is_file())
            .then_some(development_entrypoint)
    })
    .ok_or_else(|| String::from("AGENT_GATEWAY_ENTRYPOINT_INVALID"))?;
    let bundled_node = bundle
        .as_ref()
        .map(|root| root.join(private_node_relative_path()));
    let mut private_node = explicit_runtime_file(
        environment,
        "CMCLIENT_PRIVATE_NODE",
        "AGENT_PRIVATE_NODE_INVALID",
    )?
    .or_else(|| bundled_node.filter(|path| path.is_file()));
    if private_node.is_none() && cfg!(debug_assertions) {
        private_node = find_node_on_path(environment);
    }
    let private_node = private_node.ok_or_else(|| String::from("AGENT_PRIVATE_NODE_INVALID"))?;
    Ok((
        canonical_file_or_path(private_node),
        canonical_file_or_path(gateway_entrypoint),
    ))
}

fn explicit_runtime_file(
    environment: &BTreeMap<String, String>,
    name: &str,
    error_code: &str,
) -> Result<Option<PathBuf>, String> {
    let Some(value) = environment_value(environment, name) else {
        return Ok(None);
    };
    let path = PathBuf::from(value);
    if !path.is_absolute() || !path.is_file() {
        return Err(String::from(error_code));
    }
    let metadata = fs::symlink_metadata(&path).map_err(|_| String::from(error_code))?;
    if metadata.file_type().is_symlink() {
        return Err(String::from(error_code));
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        if metadata.file_attributes() & 0x0400 != 0 {
            return Err(String::from(error_code));
        }
    }
    fs::canonicalize(path)
        .map(Some)
        .map_err(|_| String::from(error_code))
}

fn find_node_on_path(environment: &BTreeMap<String, String>) -> Option<PathBuf> {
    let path = environment_value(environment, "PATH")?;
    let executable = if cfg!(target_os = "windows") {
        "node.exe"
    } else {
        "node"
    };
    env::split_paths(&OsString::from(path))
        .map(|directory| directory.join(executable))
        .find(|candidate| candidate.is_absolute() && candidate.is_file())
        .map(canonical_file_or_path)
}

fn canonical_file_or_path(path: PathBuf) -> PathBuf {
    let canonical = if path.is_file() {
        fs::canonicalize(&path).unwrap_or(path)
    } else {
        path
    };
    normalize_runtime_process_path(canonical)
}

#[cfg(target_os = "windows")]
fn normalize_runtime_process_path(path: PathBuf) -> PathBuf {
    let value = path.to_string_lossy();
    if let Some(unc_path) = value.strip_prefix(r"\\?\UNC\") {
        return PathBuf::from(format!(r"\\{unc_path}"));
    }
    if let Some(drive_path) = value.strip_prefix(r"\\?\") {
        return PathBuf::from(drive_path);
    }
    path
}

#[cfg(not(target_os = "windows"))]
fn normalize_runtime_process_path(path: PathBuf) -> PathBuf {
    path
}

const fn private_node_relative_path() -> &'static str {
    if cfg!(target_os = "windows") {
        "runtime/node/node.exe"
    } else {
        "runtime/node/bin/node"
    }
}

fn main() -> ExitCode {
    let mut arguments = std::env::args().skip(1);
    match arguments.next().as_deref() {
        Some("--serve") => serve(),
        None | Some("--check-config") | Some("--check-instance") => {
            match load_agent_config_after_migration() {
                Ok(config) => {
                    if let Err(error) = ensure_runtime_directories(&config.paths) {
                        eprintln!("{}", error.code());
                        return ExitCode::from(EX_CONFIG);
                    }
                    if std::env::args()
                        .skip(1)
                        .any(|argument| argument == "--check-instance")
                    {
                        match AgentLease::acquire(&config.paths) {
                            Ok((_lease, _state)) => println!("agent instance lock valid"),
                            Err(error) => {
                                eprintln!("{}", error.code());
                                return ExitCode::from(EX_CONFIG);
                            }
                        }
                    } else {
                        println!("agent configuration valid");
                    }
                    ExitCode::SUCCESS
                }
                Err(error) => {
                    eprintln!("{error}");
                    ExitCode::from(EX_CONFIG)
                }
            }
        }
        Some("--version") => {
            println!("cmclient-agent {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        Some(_) => {
            eprintln!("AGENT_USAGE_INVALID_ARGUMENT");
            ExitCode::from(EX_USAGE)
        }
    }
}

fn install_shutdown_signal_handler(controller: Arc<AgentController>) -> Result<(), ControlError> {
    ctrlc::set_handler(move || controller.request_shutdown())
        .map_err(|_| ControlError::CommandFailed)
}

fn serve() -> ExitCode {
    let config = match load_agent_config_after_migration() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::from(EX_CONFIG);
        }
    };
    if let Err(error) = ensure_runtime_directories(&config.paths) {
        eprintln!("{}", error.code());
        return ExitCode::from(EX_CONFIG);
    }
    let _lease = match AgentLease::acquire(&config.paths) {
        Ok((lease, _)) => lease,
        Err(error) => {
            eprintln!("{}", error.code());
            return ExitCode::from(EX_CONFIG);
        }
    };
    let controller = match AgentController::from_config(&config) {
        Ok(controller) => Arc::new(controller),
        Err(error) => {
            eprintln!("{}", error.code());
            return ExitCode::from(EX_CONFIG);
        }
    };
    let endpoint = match default_local_endpoint(config.paths.root_dir()) {
        Ok(endpoint) => endpoint,
        Err(error) => {
            controller.remember_error(&error);
            eprintln!("{}", error.code());
            return ExitCode::from(EX_CONFIG);
        }
    };
    let control_handler: Arc<dyn ControlHandler> = controller.clone();
    let server = match ControlServer::bind(endpoint, control_handler) {
        Ok(server) => server,
        Err(error) => {
            controller.remember_error(&error);
            eprintln!("{}", error.code());
            return ExitCode::from(EX_CONFIG);
        }
    };
    if let Err(error) = install_shutdown_signal_handler(Arc::clone(&controller)) {
        controller.remember_error(&error);
        eprintln!("{}", error.code());
        return ExitCode::from(EX_CONFIG);
    }
    let mut supervisor_worker = match SupervisorWorker::start(Arc::clone(&controller)) {
        Ok(worker) => worker,
        Err(error) => {
            controller.remember_error(&error);
            eprintln!("{}", error.code());
            return ExitCode::from(EX_CONFIG);
        }
    };
    let serve_error = loop {
        if controller.is_shutdown_requested() {
            break None;
        }
        match server.poll_once() {
            Ok(_) if controller.is_shutdown_requested() => break None,
            Ok(_) => {}
            Err(error) => break Some(error),
        }
    };
    drop(server);
    let shutdown_error = shutdown_agent_runtime(&controller, &mut supervisor_worker).err();
    if let Some(error) = &serve_error {
        controller.remember_error(error);
        eprintln!("{}", error.code());
    }
    if let Some(error) = shutdown_error {
        controller.remember_error(&error);
        eprintln!("{}", error.code());
        return ExitCode::from(EX_CONFIG);
    }
    if serve_error.is_some() {
        ExitCode::from(EX_CONFIG)
    } else {
        ExitCode::SUCCESS
    }
}

fn resolve_static_web_root() -> PathBuf {
    if let Some(path) = std::env::var_os("CMCLIENT_WEB_ROOT") {
        return PathBuf::from(path);
    }
    let bundled = bundled_root().map(|root| root.join("web"));
    let development = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../web/dist");
    if development.join("index.html").is_file() {
        return development;
    }
    bundled.unwrap_or_else(|| PathBuf::from("web"))
}

fn resolve_gateway_command(_config: &AgentConfig) -> Option<Vec<String>> {
    #[cfg(test)]
    if let Some(command) = &_config.gateway_command {
        return Some(command.clone());
    }
    let environment = env::vars().collect::<BTreeMap<_, _>>();
    let (private_node, gateway_entrypoint) =
        resolve_gateway_maintenance_program(&environment).ok()?;
    Some(vec![
        private_node.to_string_lossy().into_owned(),
        gateway_entrypoint.to_string_lossy().into_owned(),
    ])
}

fn bundled_root() -> Option<PathBuf> {
    std::env::current_exe().ok().and_then(|executable| {
        executable
            .parent()
            .and_then(Path::parent)
            .map(Path::to_path_buf)
    })
}

#[cfg(test)]
mod tests {
    use super::{
        AgentConfig, AgentController, AgentRuntimeProfile, AgentSecretStore, AgentUpdateService,
        ControlHandler, GatewayLogHealthUpdate, GatewaySessionHandle, InternalComponent, LogLevel,
        ManagementWebConfig, ManagementWebError, ManagementWebService, SecretKind,
        agent_web_router, apply_aprs_environment, apply_physical_qualification_environment,
        bridge_gateway_event_stream, compiled_component_identity, gateway_json_projection,
        legacy_state_candidates, load_agent_config_after_migration_with, management_web_profile,
        normalize_runtime_process_path, push_legacy_source_candidate,
        resolve_gateway_maintenance_program, setup_gate_required_with_profile,
        verified_gateway_route,
    };
    #[cfg(not(target_os = "windows"))]
    use super::{
        ControlCommand, GatewayControlStatus, ManagementWebControlStatus, SupervisorWorker,
        active_gateway_event_bridge_count, bridge_gateway_events_with_read_poll, gateway_heartbeat,
        read_bounded_gateway_sse_line, shutdown_agent_runtime, try_forward_gateway_event,
    };
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    #[cfg(not(target_os = "windows"))]
    use cmclient_agent_core::CallMeshConfig;
    use cmclient_agent_core::{
        AprsConfig, MeshtasticConfig, MeshtasticConnectionConfig, RuntimePaths,
    };
    #[cfg(not(target_os = "windows"))]
    use cmclient_control_api::UpdateControlStatus;
    use cmclient_control_api::{
        ControlClient, ControlError, ControlSecretKind, ControlServer, default_local_endpoint,
    };
    use cmclient_legacy_migration::ProductMigrationSourceSet;
    use cmclient_legacy_migration::{
        GatewayMaintenanceReport, GatewayMaintenanceRunner, MigrationError,
    };
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    #[cfg(not(target_os = "windows"))]
    use cmclient_supervisor::{BackoffPolicy, GatewayCommand, GatewaySupervisor};
    #[cfg(not(target_os = "windows"))]
    use cmclient_updater::{PersistentUpdateJob, UpdatePhase};
    #[cfg(target_os = "windows")]
    use std::net::{TcpListener, TcpStream};
    use std::{
        collections::BTreeMap,
        path::Path,
        sync::Arc,
        thread,
        time::{Duration, Instant},
    };
    #[cfg(not(target_os = "windows"))]
    use std::{
        io::{Cursor, Read, Write},
        net::{TcpListener, TcpStream},
        path::PathBuf,
        sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
            mpsc,
        },
    };

    #[cfg(not(target_os = "windows"))]
    fn wait_for_fixture_marker(marker: &Path, expected: &str, timeout: Duration) -> String {
        wait_for_fixture_marker_with(expected, timeout, || std::fs::read_to_string(marker))
    }

    fn wait_for_fixture_marker_with(
        expected: &str,
        timeout: Duration,
        mut read_marker: impl FnMut() -> std::io::Result<String>,
    ) -> String {
        let deadline = Instant::now() + timeout;
        loop {
            match read_marker() {
                Ok(contents) if contents == expected || contents == "rejected" => {
                    return contents;
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => panic!("marker should read: {error}"),
            }
            assert!(Instant::now() < deadline, "gateway fixture did not report");
            thread::sleep(Duration::from_millis(10));
        }
    }

    struct NoDatabaseMaintenance;

    impl GatewayMaintenanceRunner for NoDatabaseMaintenance {
        fn migrate_database(
            &self,
            _source_database: &Path,
            _staged_database: &Path,
        ) -> Result<GatewayMaintenanceReport, MigrationError> {
            panic!("a configuration-only migration must not start database maintenance")
        }
    }

    #[test]
    fn setup_required_blocks_external_gateway_start_and_physical_smoke_is_explicitly_scoped() {
        let root =
            std::env::temp_dir().join(format!("cmclient-agent-setup-gate-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("fixture root should create");
        let config = AgentConfig {
            paths: RuntimePaths {
                data_dir: root.clone(),
                config_dir: root.clone(),
                cache_dir: root.join("cache"),
                log_dir: root.join("logs"),
            },
            config_file: root.join("config.toml"),
            runtime_profile: AgentRuntimeProfile::Native,
            gateway_command: Some(vec![String::from("cmclient-gateway-fixture")]),
            callmesh: None,
            meshtastic: Some(MeshtasticConfig {
                mesh_network_id: String::from("fixture"),
                gateway_id: String::from("fixture-gateway"),
                connection: MeshtasticConnectionConfig::Tcp {
                    host: String::from("192.0.2.10"),
                    port: 4_403,
                },
            }),
            aprs: None,
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };
        assert!(setup_gate_required_with_profile(&config, false));
        assert!(!setup_gate_required_with_profile(&config, true));

        let controller =
            AgentController::from_config_with_secrets(&config, AgentSecretStore::memory())
                .expect("controller should initialize in setup state");
        let status = controller.status().expect("status should be available");
        assert_eq!(
            status.gateway,
            cmclient_control_api::GatewayControlStatus::Stopped
        );
        assert_eq!(status.latest_error_code.as_deref(), Some("SETUP_REQUIRED"));
        assert_eq!(
            controller.handle(cmclient_control_api::ControlCommand::Start),
            Err(cmclient_control_api::ControlError::CommandFailed)
        );
        assert_eq!(
            controller
                .status()
                .expect("status should remain available")
                .latest_error_code
                .as_deref(),
            Some("SETUP_REQUIRED")
        );
        drop(controller);
        std::fs::remove_dir_all(root).expect("fixture root should clean up");
    }

    #[test]
    fn management_web_profile_mapping_keeps_docker_fail_closed() {
        let mut native_service = ManagementWebService::start(
            &ManagementWebConfig {
                port: 0,
                profile: management_web_profile(AgentRuntimeProfile::Native),
                ..ManagementWebConfig::default()
            },
            axum::Router::new(),
            None,
            GatewaySessionHandle::new(),
        )
        .expect("native loopback management must not require LAN credentials");
        native_service
            .stop()
            .expect("native management service should stop");

        let config = ManagementWebConfig {
            profile: management_web_profile(AgentRuntimeProfile::Docker),
            ..ManagementWebConfig::default()
        };
        assert!(matches!(
            ManagementWebService::start(
                &config,
                axum::Router::new(),
                None,
                GatewaySessionHandle::new(),
            ),
            Err(ManagementWebError::InvalidConfiguration)
        ));
    }

    #[test]
    fn startup_migrates_product_state_before_loading_the_canonical_config() {
        let sequence = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock should follow epoch")
            .as_nanos();
        let fixture = std::env::temp_dir().join(format!(
            "cmclient-agent-startup-migration-{}-{sequence}",
            std::process::id()
        ));
        let home = fixture.join("home");
        let source = fixture.join("legacy");
        std::fs::create_dir_all(&home).expect("fixture home should exist");
        std::fs::create_dir_all(&source).expect("legacy source should exist");
        let source_config = b"[agent]\nmanagement_web_enabled = false\n";
        std::fs::write(source.join("config.toml"), source_config)
            .expect("legacy configuration should write");
        let mut environment = BTreeMap::from([
            (String::from("HOME"), home.to_string_lossy().into_owned()),
            (
                String::from("USERPROFILE"),
                home.to_string_lossy().into_owned(),
            ),
        ]);
        if cfg!(target_os = "windows") {
            environment.insert(
                String::from("APPDATA"),
                home.join("AppData/Roaming").to_string_lossy().into_owned(),
            );
        }
        let paths = RuntimePaths::from_environment(&environment)
            .expect("runtime paths should derive from the immutable snapshot");
        let source_set = ProductMigrationSourceSet {
            config_root: source.clone(),
            data_root: source.clone(),
        };

        let config = load_agent_config_after_migration_with(
            &environment,
            &paths,
            std::slice::from_ref(&source_set),
            &NoDatabaseMaintenance,
        )
        .expect("startup migration should finish before config parsing");

        assert!(!config.management_web_enabled);
        assert_eq!(config.runtime_profile, AgentRuntimeProfile::Native);
        assert_eq!(
            std::fs::read(paths.config_file()).expect("canonical config should activate"),
            source_config
        );
        assert_eq!(
            std::fs::read(source.join("config.toml")).expect("source should remain"),
            source_config
        );
        let _ = std::fs::remove_dir_all(fixture);
    }

    #[test]
    fn platform_legacy_candidates_are_absolute_and_exclude_the_new_root() {
        let root = std::env::temp_dir().join("cmclient-agent-legacy-candidates");
        let environment = BTreeMap::from([
            (
                String::from("HOME"),
                root.join("home").to_string_lossy().into_owned(),
            ),
            (
                String::from("USERPROFILE"),
                root.join("home").to_string_lossy().into_owned(),
            ),
            (
                String::from("APPDATA"),
                root.join("roaming").to_string_lossy().into_owned(),
            ),
            (
                String::from("PROGRAMDATA"),
                root.join("program-data").to_string_lossy().into_owned(),
            ),
        ]);
        let paths =
            RuntimePaths::from_environment(&environment).expect("runtime paths should derive");
        let candidates = legacy_state_candidates(&environment);

        assert!(!candidates.is_empty());
        assert!(candidates.iter().all(|candidate| {
            candidate.config_root.is_absolute() && candidate.data_root.is_absolute()
        }));
        assert!(
            candidates
                .iter()
                .all(|candidate| candidate.config_root != paths.root_dir()
                    && candidate.data_root != paths.root_dir())
        );
    }

    #[test]
    fn split_legacy_roots_form_one_logical_candidate() {
        let root = std::env::temp_dir().join("cmclient-agent-split-legacy-candidate");
        let config_root = root.join("config/cmclient");
        let data_root = root.join("data/cmclient");
        let mut candidates = Vec::new();

        push_legacy_source_candidate(
            &mut candidates,
            Some(config_root.clone()),
            Some(data_root.clone()),
        );
        push_legacy_source_candidate(
            &mut candidates,
            Some(config_root.clone()),
            Some(data_root.clone()),
        );

        assert_eq!(
            candidates,
            vec![ProductMigrationSourceSet {
                config_root,
                data_root: data_root.clone(),
            }]
        );

        let mut data_only = Vec::new();
        push_legacy_source_candidate(&mut data_only, None, Some(data_root.clone()));
        assert_eq!(
            data_only,
            vec![ProductMigrationSourceSet {
                config_root: data_root.clone(),
                data_root,
            }]
        );
    }

    #[test]
    fn explicit_private_runtime_paths_fail_closed() {
        let root = std::env::temp_dir().join(format!(
            "cmclient-agent-private-runtime-paths-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).expect("private runtime fixture should exist");
        let entrypoint = root.join("main.js");
        std::fs::write(&entrypoint, b"").expect("entrypoint fixture should write");
        let executable = std::env::current_exe().expect("test executable should resolve");

        let invalid_node = BTreeMap::from([
            (
                String::from("CMCLIENT_GATEWAY_ENTRYPOINT"),
                entrypoint.to_string_lossy().into_owned(),
            ),
            (String::from("CMCLIENT_PRIVATE_NODE"), String::from("node")),
        ]);
        assert_eq!(
            resolve_gateway_maintenance_program(&invalid_node),
            Err(String::from("AGENT_PRIVATE_NODE_INVALID"))
        );

        let invalid_entrypoint = BTreeMap::from([
            (
                String::from("CMCLIENT_GATEWAY_ENTRYPOINT"),
                String::from("main.js"),
            ),
            (
                String::from("CMCLIENT_PRIVATE_NODE"),
                executable.to_string_lossy().into_owned(),
            ),
        ]);
        assert_eq!(
            resolve_gateway_maintenance_program(&invalid_entrypoint),
            Err(String::from("AGENT_GATEWAY_ENTRYPOINT_INVALID"))
        );
        std::fs::remove_dir_all(root).expect("private runtime fixture should clean up");
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn private_runtime_process_paths_use_node_compatible_win32_prefixes() {
        assert_eq!(
            normalize_runtime_process_path(std::path::PathBuf::from(
                r"\\?\C:\Program Files\CMClient\runtime\node.exe",
            )),
            std::path::PathBuf::from(r"C:\Program Files\CMClient\runtime\node.exe"),
        );
        assert_eq!(
            normalize_runtime_process_path(std::path::PathBuf::from(
                r"\\?\UNC\server\share\CMClient\gateway\main.js",
            )),
            std::path::PathBuf::from(r"\\server\share\CMClient\gateway\main.js"),
        );
    }

    #[test]
    fn agent_log_recovery_restores_a_persistent_gateway_log_error() {
        let sequence = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock should follow epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "cmclient-agent-log-recovery-{}-{sequence}",
            std::process::id()
        ));
        let make_config = |data_dir: &Path, log_dir: &Path| AgentConfig {
            paths: RuntimePaths {
                data_dir: data_dir.to_path_buf(),
                config_dir: data_dir.join("config"),
                cache_dir: data_dir.join("cache"),
                log_dir: log_dir.to_path_buf(),
            },
            config_file: data_dir.join("config").join("agent.toml"),
            runtime_profile: AgentRuntimeProfile::Native,
            gateway_command: None,
            callmesh: None,
            meshtastic: None,
            aprs: None,
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };

        let healthy_root = root.join("healthy");
        let healthy_log_dir = healthy_root.join("logs");
        std::fs::create_dir_all(&healthy_root).expect("healthy root should create");
        let healthy = AgentController::from_config_with_secrets(
            &make_config(&healthy_root, &healthy_log_dir),
            AgentSecretStore::memory(),
        )
        .expect("healthy controller should initialize");
        healthy.apply_gateway_log_health(GatewayLogHealthUpdate {
            capture_error_code: Some("RUNTIME_LOG_CAPTURE_READ_FAILED"),
            write_error_code: None,
            write_recovered_code: None,
        });
        assert_eq!(
            healthy
                .latest_error_code
                .lock()
                .expect("latest error should lock")
                .as_deref(),
            Some("RUNTIME_LOG_CAPTURE_READ_FAILED")
        );
        assert_eq!(
            healthy
                .gateway_log_health
                .lock()
                .expect("Gateway log error should lock")
                .capture_error_code
                .as_deref(),
            Some("RUNTIME_LOG_CAPTURE_READ_FAILED")
        );

        let agent_failure = healthy_log_dir.join("agent.jsonl.1");
        std::fs::create_dir(&agent_failure).expect("Agent log failure fixture should create");
        healthy.log_agent_code(LogLevel::Info, "AGENT_HEARTBEAT");
        assert_eq!(
            healthy
                .latest_error_code
                .lock()
                .expect("latest error should lock")
                .as_deref(),
            Some("RUNTIME_LOG_FILE_UNAVAILABLE")
        );
        assert_eq!(
            healthy
                .agent_log_error_code
                .lock()
                .expect("Agent log error should lock")
                .as_deref(),
            Some("RUNTIME_LOG_FILE_UNAVAILABLE")
        );

        std::fs::remove_dir(agent_failure).expect("Agent log failure fixture should remove");
        healthy.log_agent_code(LogLevel::Info, "AGENT_HEARTBEAT");
        assert_eq!(
            healthy
                .agent_log_error_code
                .lock()
                .expect("Agent log error should lock")
                .as_deref(),
            None
        );
        assert_eq!(
            healthy
                .latest_error_code
                .lock()
                .expect("latest error should lock")
                .as_deref(),
            Some("RUNTIME_LOG_CAPTURE_READ_FAILED"),
            "Agent recovery must restore the independent Gateway log failure"
        );
        assert_eq!(
            healthy
                .gateway_log_health
                .lock()
                .expect("Gateway log error should lock")
                .capture_error_code
                .as_deref(),
            Some("RUNTIME_LOG_CAPTURE_READ_FAILED")
        );
        assert_eq!(
            healthy
                .latest_error_code
                .lock()
                .expect("latest error should lock")
                .as_deref(),
            Some("RUNTIME_LOG_CAPTURE_READ_FAILED"),
            "Agent writes must not clear an independent Gateway capture failure"
        );
        drop(healthy);

        let missing_root = root.join("missing");
        let missing_log_dir = missing_root.join("logs");
        std::fs::create_dir_all(missing_log_dir.join("agent.jsonl"))
            .expect("unsafe active log fixture should create");
        let missing = AgentController::from_config_with_secrets(
            &make_config(&missing_root, &missing_log_dir),
            AgentSecretStore::memory(),
        )
        .expect("controller should tolerate an unavailable runtime log");
        assert_eq!(
            missing
                .latest_error_code
                .lock()
                .expect("latest error should lock")
                .as_deref(),
            Some("RUNTIME_LOG_FILE_UNAVAILABLE")
        );
        assert_eq!(
            missing
                .agent_log_error_code
                .lock()
                .expect("Agent log error should lock")
                .as_deref(),
            Some("RUNTIME_LOG_FILE_UNAVAILABLE")
        );
        missing.log_agent_code(LogLevel::Info, "AGENT_HEARTBEAT");
        assert_eq!(
            missing
                .latest_error_code
                .lock()
                .expect("latest error should lock")
                .as_deref(),
            Some("RUNTIME_LOG_FILE_UNAVAILABLE")
        );
        drop(missing);
        std::fs::remove_dir_all(root).expect("test root should remove");
    }

    #[test]
    fn overlapping_gateway_capture_error_and_write_recovery_preserves_capture_health() {
        let sequence = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock should follow epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "cmclient-agent-overlapping-log-health-{}-{sequence}",
            std::process::id()
        ));
        let config = AgentConfig {
            paths: RuntimePaths {
                data_dir: root.clone(),
                config_dir: root.join("config"),
                cache_dir: root.join("cache"),
                log_dir: root.join("logs"),
            },
            config_file: root.join("config").join("agent.toml"),
            runtime_profile: AgentRuntimeProfile::Native,
            gateway_command: None,
            callmesh: None,
            meshtastic: None,
            aprs: None,
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };
        std::fs::create_dir_all(&root).expect("test root should create");
        let controller =
            AgentController::from_config_with_secrets(&config, AgentSecretStore::memory())
                .expect("controller should initialize");
        controller.apply_gateway_log_health(GatewayLogHealthUpdate {
            capture_error_code: Some("RUNTIME_LOG_CAPTURE_READ_FAILED"),
            write_error_code: Some("RUNTIME_LOG_FILE_UNAVAILABLE"),
            write_recovered_code: Some("RUNTIME_LOG_FILE_UNAVAILABLE"),
        });

        let health = controller
            .gateway_log_health
            .lock()
            .expect("Gateway log health should lock");
        assert_eq!(
            health.capture_error_code.as_deref(),
            Some("RUNTIME_LOG_CAPTURE_READ_FAILED")
        );
        assert_eq!(health.write_error_code, None);
        drop(health);
        assert_eq!(
            controller
                .status()
                .expect("status should derive persistent log health")
                .latest_error_code
                .as_deref(),
            Some("RUNTIME_LOG_CAPTURE_READ_FAILED"),
            "matching write recovery must not clear the overlapping capture failure"
        );

        drop(controller);
        std::fs::remove_dir_all(root).expect("test root should remove");
    }

    #[cfg(not(target_os = "windows"))]
    fn read_runtime_log_family(directory: &Path, file_name: &str) -> String {
        let mut paths = std::fs::read_dir(directory)
            .expect("runtime log directory should read")
            .map(|entry| entry.expect("runtime log entry should read").path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| {
                        name == file_name || name.starts_with(&format!("{file_name}."))
                    })
            })
            .collect::<Vec<_>>();
        paths.sort();
        paths
            .iter()
            .map(|path| std::fs::read_to_string(path).expect("runtime log should read"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn gateway_fixture_marker_waits_for_committed_content() {
        let mut observed_contents = [String::new(), String::from("ok")].into_iter();

        assert_eq!(
            wait_for_fixture_marker_with("ok", Duration::from_secs(2), || {
                Ok(observed_contents
                    .next()
                    .expect("marker should be accepted after the committed observation"))
            }),
            "ok",
        );
        assert!(observed_contents.next().is_none());
    }

    #[test]
    fn rejects_a_gateway_with_a_different_product_identity() {
        let capability = "d".repeat(64);
        let listener = TcpListener::bind("127.0.0.1:0").expect("identity fixture should bind");
        let address = listener.local_addr().expect("fixture address should load");
        let expected = compiled_component_identity(InternalComponent::Agent)
            .expect("compiled Agent identity should load");
        let mut response_identity = expected.clone();
        response_identity.component = InternalComponent::Gateway;
        response_identity.identity.source_commit = "e".repeat(40);
        if response_identity.identity.source_commit == expected.identity.source_commit {
            response_identity.identity.source_commit = "f".repeat(40);
        }
        let body = serde_json::to_vec(&response_identity).expect("identity should serialize");
        let expected_capability = capability.clone();
        let fixture = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("identity request should connect");
            use std::io::{Read as _, Write as _};
            let mut request = [0_u8; 4096];
            let count = stream
                .read(&mut request)
                .expect("identity request should read");
            let request = std::str::from_utf8(&request[..count]).expect("request should be UTF-8");
            assert!(request.contains(&format!(
                "{}: {expected_capability}\r\n",
                super::GATEWAY_CAPABILITY_HEADER,
            )));
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len(),
            )
            .and_then(|()| stream.write_all(&body))
            .expect("identity response should write");
        });
        let ready = cmclient_supervisor::GatewayReady {
            pid: std::process::id(),
            address,
            startup_nonce: "c".repeat(32),
            capability,
        };

        assert_eq!(
            verified_gateway_route(&ready, &expected),
            Err(ControlError::CommandFailed)
        );
        fixture.join().expect("identity fixture should join");
    }

    #[cfg(target_os = "windows")]
    #[test]
    #[ignore = "requires a built production Gateway and Node; run by Windows source qualification"]
    fn supervised_real_gateway_uses_private_dynamic_session() {
        let private_node = std::env::var_os("CMCLIENT_PRIVATE_NODE")
            .map(std::path::PathBuf::from)
            .expect("Windows source qualification requires CMCLIENT_PRIVATE_NODE");
        assert!(private_node.is_absolute(), "private Node must be absolute");
        let private_node_metadata =
            std::fs::symlink_metadata(&private_node).expect("private Node metadata should load");
        assert!(
            private_node_metadata.is_file() && !private_node_metadata.file_type().is_symlink(),
            "private Node must be a regular non-link file"
        );
        use std::os::windows::fs::MetadataExt;
        assert_eq!(
            private_node_metadata.file_attributes() & 0x0400,
            0,
            "private Node must not be a reparse point"
        );
        let private_node =
            std::fs::canonicalize(private_node).expect("private Node should canonicalize");
        let version = std::process::Command::new(&private_node)
            .arg("--version")
            .env_clear()
            .output()
            .expect("private Node should start without PATH");
        assert!(version.status.success(), "private Node version should run");
        assert_eq!(
            String::from_utf8(version.stdout)
                .expect("private Node version should be UTF-8")
                .trim(),
            "v24.18.0"
        );
        let data_dir = std::env::temp_dir().join(format!(
            "cmclient-agent-private-gateway-{}",
            std::process::id(),
        ));
        let _ = std::fs::remove_dir_all(&data_dir);
        std::fs::create_dir_all(&data_dir).expect("test directory should exist");
        let gateway_entry = Path::new(env!("CARGO_MANIFEST_DIR")).join("../gateway/dist/main.js");
        assert!(
            gateway_entry.is_file(),
            "Gateway production entrypoint should be built"
        );
        let reserved =
            TcpListener::bind(("127.0.0.1", 0)).expect("fixed-port sentinel should bind");
        let fixed_port = reserved
            .local_addr()
            .expect("fixed-port sentinel address should load")
            .port();
        let config = AgentConfig {
            paths: RuntimePaths {
                data_dir: data_dir.clone(),
                config_dir: data_dir.join("config"),
                cache_dir: data_dir.join("cache"),
                log_dir: data_dir.join("logs"),
            },
            config_file: data_dir.join("agent.toml"),
            runtime_profile: AgentRuntimeProfile::Native,
            gateway_command: Some(vec![
                private_node.to_string_lossy().into_owned(),
                gateway_entry.to_string_lossy().into_owned(),
            ]),
            callmesh: None,
            meshtastic: None,
            aprs: None,
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };
        let controller = AgentController::from_config(&config).expect("controller should build");

        let started = controller
            .handle(super::ControlCommand::Start)
            .expect("real supervised Gateway should start");
        let (ready, supervised_pid) = {
            let supervisor = controller
                .supervisor
                .lock()
                .expect("supervisor lock should open");
            let supervisor = supervisor.as_ref().expect("supervisor should exist");
            let ready = supervisor
                .gateway_ready()
                .cloned()
                .expect("private Gateway session should be published");
            let supervised_pid = match supervisor.status() {
                cmclient_supervisor::GatewayStatus::Running { pid } => pid,
                status => panic!("Gateway should be running, got {status:?}"),
            };
            (ready, supervised_pid)
        };
        let route = controller
            .gateway_session
            .snapshot()
            .expect("Agent route should be available");
        let (route_address, route_capability) = {
            let active_route = route.active().expect("Agent route should be active");
            (active_route.address(), active_route.capability().to_owned())
        };
        assert_eq!(route_address, ready.address);
        assert!(
            route_capability == ready.capability,
            "Gateway session capability did not match bootstrap"
        );
        assert_eq!(ready.pid, supervised_pid);
        assert_ne!(route_address.port(), fixed_port);
        assert_eq!(route_address.ip().to_string(), "127.0.0.1");
        assert_eq!(ready.startup_nonce.len(), 32);
        assert!(
            ready
                .startup_nonce
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        );
        assert_eq!(route_capability.len(), 64);
        assert!(!format!("{route:?}").contains(&route_capability));
        assert!(!format!("{:?}", controller.gateway_session).contains(&route_capability));

        let direct = reqwest::blocking::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .build()
            .expect("test client should build");
        let health_url = format!("http://{route_address}/api/v1/system/health");
        assert_eq!(
            direct
                .get(&health_url)
                .send()
                .expect("direct request should complete")
                .status()
                .as_u16(),
            403
        );
        let wrong_capability = if route_capability.starts_with('f') {
            "e".repeat(64)
        } else {
            "f".repeat(64)
        };
        assert_eq!(
            direct
                .get(&health_url)
                .header(super::GATEWAY_CAPABILITY_HEADER, wrong_capability)
                .send()
                .expect("wrong-capability request should complete")
                .status()
                .as_u16(),
            403
        );
        assert_eq!(
            controller
                .gateway_projection(cmclient_control_api::GatewayProjection::Meshtastic)
                .expect("Agent should authenticate the Meshtastic projection"),
            serde_json::json!({ "configured": false })
        );
        assert_eq!(
            TcpListener::bind(route_address)
                .expect_err("private Gateway address should already be owned")
                .kind(),
            std::io::ErrorKind::AddrInUse
        );

        let stopped = controller
            .handle(super::ControlCommand::Stop)
            .expect("real supervised Gateway should stop");
        assert!(controller.gateway_session.snapshot().is_none());
        assert!(!route.is_active(), "stale route clone should be revoked");
        assert!(
            TcpStream::connect_timeout(&route_address, Duration::from_millis(250)).is_err(),
            "stopped private Gateway address should close"
        );
        drop(reserved);
        assert_eq!(started.gateway, super::GatewayControlStatus::Running);
        assert_eq!(stopped.gateway, super::GatewayControlStatus::Stopped);

        let startup_nonce = ready.startup_nonce;
        let capability = ready.capability;
        drop(controller);
        assert!(!directory_contains_bytes(
            &data_dir,
            startup_nonce.as_bytes()
        ));
        assert!(!directory_contains_bytes(&data_dir, capability.as_bytes()));
        std::fs::remove_dir_all(&data_dir).expect("test directory should remove");
    }

    #[cfg(target_os = "windows")]
    fn directory_contains_bytes(root: &Path, needle: &[u8]) -> bool {
        std::fs::read_dir(root)
            .expect("test root should be readable")
            .filter_map(Result::ok)
            .any(|entry| {
                let path = entry.path();
                if path.is_dir() {
                    directory_contains_bytes(&path, needle)
                } else {
                    std::fs::read(path).is_ok_and(|bytes| {
                        bytes.windows(needle.len()).any(|window| window == needle)
                    })
                }
            })
    }

    #[test]
    fn derives_exact_workspace_identity_for_the_gateway_child() {
        let report = compiled_component_identity(InternalComponent::Agent).unwrap();
        assert_eq!(report.identity.product, "CMClient");
        assert_eq!(report.identity.channel.as_str(), "dev");
        report.validate().unwrap();
        assert_eq!(report.identity.source_commit.len(), 40);
        assert!(
            report.identity.source_tree.len() == 40
                || (report.identity.source_tree.len() == 71
                    && report.identity.source_tree.starts_with("sha256:"))
        );
    }

    #[test]
    fn aprs_environment_contains_only_enablement_and_operator_overrides() {
        let mut environment = BTreeMap::new();
        apply_aprs_environment(
            &mut environment,
            Some(&AprsConfig {
                host: None,
                port: None,
                destination: None,
            }),
        );
        assert_eq!(
            environment,
            BTreeMap::from([(String::from("CMCLIENT_APRS_ENABLED"), String::from("true"),)])
        );

        apply_aprs_environment(
            &mut environment,
            Some(&AprsConfig {
                host: Some(String::from("operator.aprs.example")),
                port: Some(14_580),
                destination: Some(String::from("APCM20")),
            }),
        );
        assert_eq!(
            environment.get("CMCLIENT_APRS_HOST").map(String::as_str),
            Some("operator.aprs.example")
        );
        assert_eq!(
            environment.get("CMCLIENT_APRS_PORT").map(String::as_str),
            Some("14580")
        );
        assert_eq!(
            environment
                .get("CMCLIENT_APRS_DESTINATION")
                .map(String::as_str),
            Some("APCM20")
        );
        for forbidden in [
            "CMCLIENT_APRS_LOGIN_CALLSIGN",
            "CMCLIENT_APRS_PASSCODE",
            "CMCLIENT_APRS_SYMBOL_TABLE",
            "CMCLIENT_APRS_SYMBOL_CODE",
            "CMCLIENT_APRS_COMMENT",
        ] {
            assert!(
                !environment.contains_key(forbidden),
                "Agent must not inject {forbidden}",
            );
        }
    }

    #[test]
    fn physical_qualification_environment_is_explicit_and_bounded() {
        let mut environment = BTreeMap::new();
        apply_physical_qualification_environment(
            &mut environment,
            Some("true"),
            Some("windows-source-smoke"),
        )
        .expect("physical qualification environment should validate");
        assert_eq!(
            environment
                .get("CMCLIENT_MESHTASTIC_PHYSICAL_PROFILE")
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            environment
                .get("CMCLIENT_QUALIFICATION_STAGE")
                .map(String::as_str),
            Some("windows-source-smoke")
        );

        assert!(
            apply_physical_qualification_environment(
                &mut BTreeMap::new(),
                Some("true"),
                Some("unsafe stage"),
            )
            .is_err()
        );
        let mut disabled = BTreeMap::new();
        apply_physical_qualification_environment(&mut disabled, Some("false"), None)
            .expect("disabled physical qualification should be accepted");
        assert!(disabled.is_empty());
    }

    #[test]
    fn controller_rejects_new_aprs_passcodes_but_removes_a_legacy_value() {
        let directory = std::env::temp_dir().join(format!(
            "cmclient-agent-legacy-aprs-cleanup-{}",
            std::process::id(),
        ));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).expect("temporary directory should exist");
        let secrets = AgentSecretStore::memory();
        secrets
            .store(SecretKind::AprsPasscode, "synthetic-legacy-passcode")
            .expect("legacy fixture should be seeded directly");
        let config = AgentConfig {
            paths: RuntimePaths {
                data_dir: directory.clone(),
                config_dir: directory.join("config"),
                cache_dir: directory.join("cache"),
                log_dir: directory.join("logs"),
            },
            config_file: directory.join("agent.toml"),
            runtime_profile: AgentRuntimeProfile::Native,
            gateway_command: None,
            callmesh: None,
            meshtastic: None,
            aprs: None,
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };
        let controller = AgentController::from_config_with_secrets(&config, secrets.clone())
            .expect("controller should initialize");

        assert_eq!(
            controller.store_secret(ControlSecretKind::AprsPasscode, "replacement"),
            Err(ControlError::SecretKindDeprecated),
        );
        assert!(
            controller
                .remove_secret(ControlSecretKind::AprsPasscode)
                .expect("legacy value should be removable")
        );
        assert!(
            secrets
                .read(SecretKind::AprsPasscode)
                .expect("secret backend should remain readable")
                .is_none()
        );

        drop(controller);
        std::fs::remove_dir_all(directory).expect("temporary directory should be removed");
    }

    #[tokio::test]
    async fn management_web_never_exposes_the_local_control_protocol() {
        let directory = std::env::temp_dir().join(format!(
            "cmclient-agent-no-network-control-{}",
            std::process::id(),
        ));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).expect("temporary directory should exist");
        let router = agent_web_router(Arc::new(
            AgentUpdateService::new(&directory).expect("update service should initialize"),
        ));
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/control/status")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let response = to_bytes(response.into_body(), 1_024)
            .await
            .expect("response body should read");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&response)
                .expect("response should be JSON"),
            serde_json::json!({"code": "CONTROL_ROUTE_NOT_FOUND"}),
        );
        let login = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/auth/login")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(
            login.status(),
            StatusCode::NOT_FOUND,
            "login authority belongs only to agent-core middleware",
        );

        std::fs::remove_dir_all(directory).expect("temporary directory should be removed");
    }

    #[tokio::test]
    async fn management_update_routes_are_axum_json_and_sse() {
        let directory = std::env::temp_dir().join(format!(
            "cmclient-agent-axum-updates-{}",
            std::process::id(),
        ));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).expect("temporary directory should exist");
        let router = agent_web_router(Arc::new(
            AgentUpdateService::new(&directory).expect("update service should initialize"),
        ));

        let status = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/updates")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(status.status(), StatusCode::OK);
        let body = to_bytes(status.into_body(), 4_096)
            .await
            .expect("status body should read");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).expect("status should be JSON"),
            serde_json::json!({"schemaVersion": 1, "job": null}),
        );

        let events = router
            .oneshot(
                Request::builder()
                    .uri("/api/v1/updates/events")
                    .header("last-event-id", "gateway-999")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(events.status(), StatusCode::OK);
        assert_eq!(
            events
                .headers()
                .get("content-type")
                .and_then(|value| value.to_str().ok()),
            Some("text/event-stream"),
        );
        let mut body = events.into_body();
        let first_event = tokio::time::timeout(Duration::from_secs(1), async {
            let mut encoded = Vec::new();
            loop {
                let frame = body
                    .frame()
                    .await
                    .expect("SSE stream should yield an initial snapshot")
                    .expect("SSE frame should encode");
                if let Ok(data) = frame.into_data() {
                    encoded.extend_from_slice(&data);
                    if encoded.windows(2).any(|window| window == b"\n\n") {
                        return encoded;
                    }
                }
            }
        })
        .await
        .expect("initial SSE snapshot should not block");
        let first_event =
            String::from_utf8(first_event).expect("initial SSE snapshot should be UTF-8");
        assert!(first_event.contains("id: update-"));
        assert!(first_event.contains("event: update.status_changed"));
        assert!(!first_event.contains("gateway-999"));
        drop(body);

        std::fs::remove_dir_all(directory).expect("temporary directory should be removed");
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn configured_callmesh_secret_is_never_added_to_the_gateway_environment() {
        let data_dir = std::env::temp_dir().join(format!(
            "cmclient-agent-plaintext-boundary-{}",
            std::process::id(),
        ));
        let marker = data_dir.join("gateway-environment");
        let _ = std::fs::remove_dir_all(&data_dir);
        std::fs::create_dir_all(&data_dir).expect("test directory should exist");
        let secrets = AgentSecretStore::memory();
        secrets
            .store(SecretKind::CallMeshApiKey, "fixture-callmesh-value")
            .expect("fixture secret should store");
        let config = AgentConfig {
            paths: RuntimePaths {
                data_dir: data_dir.clone(),
                config_dir: data_dir.clone(),
                cache_dir: data_dir.join("cache"),
                log_dir: data_dir.join("logs"),
            },
            config_file: data_dir.join("agent.toml"),
            runtime_profile: AgentRuntimeProfile::Native,
            gateway_command: Some(vec![
                String::from("sh"),
                String::from("-c"),
                format!(
                    "if [ \"$CMCLIENT_CALLMESH_URL\" = \"https://callmesh.example.invalid\" ] && [ -z \"${{CMCLIENT_CALLMESH_API_KEY+x}}\" ] && [ -z \"${{CMCLIENT_PLAINTEXT_SECRET_FILE+x}}\" ]; then printf ok > '{}'; else printf rejected > '{}'; fi; read _",
                    marker.display(),
                    marker.display(),
                ),
            ]),
            callmesh: Some(CallMeshConfig {
                url: String::from("https://callmesh.example.invalid"),
            }),
            meshtastic: None,
            aprs: None,
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };
        let controller =
            AgentController::from_config_with_secrets_without_private_bootstrap(&config, secrets)
                .expect("controller should initialize");

        controller
            .supervisor
            .lock()
            .expect("supervisor should lock")
            .as_mut()
            .expect("supervisor should be configured")
            .start()
            .expect("gateway fixture should start");
        assert_eq!(
            wait_for_fixture_marker(&marker, "ok", Duration::from_secs(2)),
            "ok",
        );
        controller
            .supervisor
            .lock()
            .expect("supervisor should lock")
            .as_mut()
            .expect("supervisor should be configured")
            .stop()
            .expect("gateway fixture should stop");
        std::fs::remove_dir_all(data_dir).expect("test directory should remove");
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn configured_aprs_forwards_only_operator_endpoint_overrides() {
        let data_dir = std::env::temp_dir().join(format!(
            "cmclient-agent-aprs-boundary-{}",
            std::process::id(),
        ));
        let marker = data_dir.join("gateway-environment");
        let _ = std::fs::remove_dir_all(&data_dir);
        std::fs::create_dir_all(&data_dir).expect("test directory should exist");
        let secrets = AgentSecretStore::memory();
        let config = AgentConfig {
            paths: RuntimePaths {
                data_dir: data_dir.clone(),
                config_dir: data_dir.clone(),
                cache_dir: data_dir.join("cache"),
                log_dir: data_dir.join("logs"),
            },
            config_file: data_dir.join("agent.toml"),
            runtime_profile: AgentRuntimeProfile::Native,
            gateway_command: Some(vec![
                String::from("sh"),
                String::from("-c"),
                format!(
                    "if [ \"$CMCLIENT_APRS_ENABLED\" = \"true\" ] && [ \"$CMCLIENT_APRS_HOST\" = \"asia.aprs2.net\" ] && [ \"$CMCLIENT_APRS_PORT\" = \"14580\" ] && [ \"$CMCLIENT_APRS_DESTINATION\" = \"APCM20\" ] && [ \"${{CMCLIENT_APRS_LOGIN_CALLSIGN+x}}\" != x ] && [ \"${{CMCLIENT_APRS_PASSCODE+x}}\" != x ] && [ \"${{CMCLIENT_APRS_SYMBOL_TABLE+x}}\" != x ] && [ \"${{CMCLIENT_APRS_SYMBOL_CODE+x}}\" != x ] && [ \"${{CMCLIENT_APRS_COMMENT+x}}\" != x ]; then printf ok > '{}'; else printf rejected > '{}'; fi; read _",
                    marker.display(),
                    marker.display(),
                ),
            ]),
            callmesh: None,
            meshtastic: None,
            aprs: Some(AprsConfig {
                host: Some(String::from("asia.aprs2.net")),
                port: Some(14_580),
                destination: Some(String::from("APCM20")),
            }),
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };
        let controller =
            AgentController::from_config_with_secrets_without_private_bootstrap(&config, secrets)
                .expect("controller should initialize");

        controller
            .supervisor
            .lock()
            .expect("supervisor should lock")
            .as_mut()
            .expect("supervisor should be configured")
            .start()
            .expect("gateway fixture should start");
        assert_eq!(
            wait_for_fixture_marker(&marker, "ok", Duration::from_secs(2)),
            "ok",
        );
        controller
            .supervisor
            .lock()
            .expect("supervisor should lock")
            .as_mut()
            .expect("supervisor should be configured")
            .stop()
            .expect("gateway fixture should stop");
        std::fs::remove_dir_all(data_dir).expect("test directory should remove");
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn reports_running_only_after_gateway_health_succeeds() {
        let gateway = TcpListener::bind("127.0.0.1:0").expect("gateway should bind");
        let gateway_address = gateway.local_addr().expect("gateway address should load");
        let gateway_thread = thread::spawn(move || {
            let (mut stream, _) = gateway.accept().expect("gateway should accept");
            let mut request = [0_u8; 4096];
            let _ = stream
                .read(&mut request)
                .expect("health request should read");
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 15\r\nconnection: close\r\n\r\n{\"status\":\"ok\"}",
                )
                .expect("health response should write");
        });
        let config = AgentConfig {
            paths: RuntimePaths {
                data_dir: PathBuf::from("/tmp/cmclient-agent-health"),
                config_dir: PathBuf::from("/tmp/cmclient-agent-health"),
                cache_dir: PathBuf::from("/tmp/cmclient-agent-health/cache"),
                log_dir: PathBuf::from("/tmp/cmclient-agent-health/logs"),
            },
            config_file: PathBuf::from("/tmp/cmclient-agent-health/agent.toml"),
            runtime_profile: AgentRuntimeProfile::Native,
            gateway_command: Some(vec![
                String::from("sh"),
                String::from("-c"),
                String::from("read _"),
            ]),
            callmesh: None,
            meshtastic: None,
            aprs: None,
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };
        let controller = AgentController::from_config_with_secrets_without_private_bootstrap(
            &config,
            AgentSecretStore::memory(),
        )
        .expect("controller should build");
        controller.gateway_session.set(
            cmclient_agent_core::web::GatewayRoute::new(gateway_address, "a".repeat(64))
                .expect("test route should be valid"),
        );

        let diagnostics = controller
            .diagnostics_bundle()
            .expect("sanitized diagnostics should build");
        let serialized =
            serde_json::to_string(&diagnostics).expect("sanitized diagnostics should serialize");
        assert_eq!(diagnostics.schema_version, 2);
        assert!(!serialized.contains("/tmp/cmclient-agent-health"));
        assert!(!serialized.contains("gateway_command"));

        let status = controller
            .handle(ControlCommand::Start)
            .expect("gateway should start");
        assert_eq!(status.gateway, GatewayControlStatus::Running);
        assert_eq!(status.management_web, ManagementWebControlStatus::Disabled);
        controller
            .handle(ControlCommand::Stop)
            .expect("gateway should stop");
        gateway_thread.join().expect("gateway should join");
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn restarts_a_crashed_gateway_without_control_requests() {
        let marker =
            std::env::temp_dir().join(format!("cmclient-agent-supervisor-{}", std::process::id()));
        let _ = std::fs::remove_file(&marker);
        let config = AgentConfig {
            paths: RuntimePaths {
                data_dir: PathBuf::from("/tmp/cmclient-agent-supervisor"),
                config_dir: PathBuf::from("/tmp/cmclient-agent-supervisor"),
                cache_dir: PathBuf::from("/tmp/cmclient-agent-supervisor/cache"),
                log_dir: PathBuf::from("/tmp/cmclient-agent-supervisor/logs"),
            },
            config_file: PathBuf::from("/tmp/cmclient-agent-supervisor/agent.toml"),
            runtime_profile: AgentRuntimeProfile::Native,
            gateway_command: Some(vec![
                String::from("sh"),
                String::from("-c"),
                format!("printf x >> '{}'; exit 7", marker.display()),
            ]),
            callmesh: None,
            meshtastic: None,
            aprs: None,
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };
        let controller = Arc::new(
            AgentController::from_config_with_secrets_without_private_bootstrap(
                &config,
                AgentSecretStore::memory(),
            )
            .expect("controller should initialize"),
        );
        let mut worker =
            SupervisorWorker::start(Arc::clone(&controller)).expect("worker should start");

        controller
            .handle(ControlCommand::Start)
            .expect("first gateway process should start");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(4);
        loop {
            let starts = std::fs::read(&marker).map_or(0, |contents| contents.len());
            if starts >= 2 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "background supervisor did not restart the gateway"
            );
            std::thread::sleep(std::time::Duration::from_millis(25));
        }

        worker.stop();
        controller
            .handle(ControlCommand::Stop)
            .expect("supervisor should stop");
        std::fs::remove_file(marker).expect("marker should remove");
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn terminal_shutdown_rejects_resource_starting_commands() {
        let data_dir = std::env::temp_dir().join(format!(
            "cmclient-agent-shutdown-fence-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&data_dir);
        std::fs::create_dir_all(&data_dir).expect("test directory should exist");
        let config = AgentConfig {
            paths: RuntimePaths {
                data_dir: data_dir.clone(),
                config_dir: data_dir.clone(),
                cache_dir: data_dir.join("cache"),
                log_dir: data_dir.join("logs"),
            },
            config_file: data_dir.join("agent.toml"),
            runtime_profile: AgentRuntimeProfile::Native,
            gateway_command: Some(vec![
                String::from("sh"),
                String::from("-c"),
                String::from("exit 0"),
            ]),
            callmesh: None,
            meshtastic: None,
            aprs: None,
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };
        let controller =
            AgentController::from_config(&config).expect("controller should initialize");

        controller.request_shutdown();

        for command in [
            ControlCommand::Start,
            ControlCommand::Restart,
            ControlCommand::EnableManagementWeb,
        ] {
            assert_eq!(
                controller.handle(command),
                Err(cmclient_control_api::ControlError::CommandFailed)
            );
        }
        for command in [
            ControlCommand::Status,
            ControlCommand::Stop,
            ControlCommand::DisableManagementWeb,
            ControlCommand::ShutdownAgent,
        ] {
            controller
                .handle(command)
                .expect("non-starting command should remain safe during teardown");
        }
        assert!(
            controller
                .management_web
                .lock()
                .expect("management service should lock")
                .is_none()
        );

        std::fs::remove_dir_all(data_dir).expect("test directory should remove");
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn queued_start_rechecks_shutdown_after_acquiring_the_supervisor_lock() {
        let data_dir = std::env::temp_dir().join(format!(
            "cmclient-agent-queued-start-fence-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&data_dir);
        std::fs::create_dir_all(&data_dir).expect("test directory should exist");
        let config = AgentConfig {
            paths: RuntimePaths {
                data_dir: data_dir.clone(),
                config_dir: data_dir.clone(),
                cache_dir: data_dir.join("cache"),
                log_dir: data_dir.join("logs"),
            },
            config_file: data_dir.join("agent.toml"),
            runtime_profile: AgentRuntimeProfile::Native,
            gateway_command: Some(vec![
                String::from("sh"),
                String::from("-c"),
                String::from("sleep 5"),
            ]),
            callmesh: None,
            meshtastic: None,
            aprs: None,
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };
        let controller =
            Arc::new(AgentController::from_config(&config).expect("controller should initialize"));
        let supervisor = controller
            .supervisor
            .lock()
            .expect("supervisor should lock");
        let (ready_sender, ready_receiver) = mpsc::sync_channel(1);
        let start_controller = Arc::clone(&controller);
        let start_thread = thread::spawn(move || {
            ready_sender
                .send(())
                .expect("queued start should announce itself");
            start_controller.handle(ControlCommand::Start)
        });
        ready_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("queued start should begin");
        thread::sleep(Duration::from_millis(25));
        assert!(!start_thread.is_finished());

        controller.request_shutdown();
        drop(supervisor);

        let start_result = start_thread.join().expect("queued start should join");
        let gateway_status = controller
            .supervisor
            .lock()
            .expect("supervisor should lock")
            .as_ref()
            .expect("supervisor should exist")
            .status();
        let _ = controller.stop_supervisor();
        assert_eq!(
            start_result,
            Err(cmclient_control_api::ControlError::CommandFailed)
        );
        assert!(matches!(
            gateway_status,
            cmclient_supervisor::GatewayStatus::Stopped
        ));

        std::fs::remove_dir_all(data_dir).expect("test directory should remove");
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn restart_rechecks_shutdown_after_draining_the_old_child() {
        let data_dir = std::env::temp_dir().join(format!(
            "cmclient-agent-restart-fence-{}",
            std::process::id()
        ));
        let marker = data_dir.join("shutdown-observed");
        let _ = std::fs::remove_dir_all(&data_dir);
        std::fs::create_dir_all(&data_dir).expect("test directory should exist");
        let config = AgentConfig {
            paths: RuntimePaths {
                data_dir: data_dir.clone(),
                config_dir: data_dir.clone(),
                cache_dir: data_dir.join("cache"),
                log_dir: data_dir.join("logs"),
            },
            config_file: data_dir.join("agent.toml"),
            runtime_profile: AgentRuntimeProfile::Native,
            gateway_command: Some(vec![
                String::from("sh"),
                String::from("-c"),
                format!("read _; printf x > '{}'; exec sleep 5", marker.display()),
            ]),
            callmesh: None,
            meshtastic: None,
            aprs: None,
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };
        let controller = Arc::new(
            AgentController::from_config_with_secrets_without_private_bootstrap(
                &config,
                AgentSecretStore::memory(),
            )
            .expect("controller should initialize"),
        );
        controller
            .supervisor
            .lock()
            .expect("supervisor should lock")
            .as_mut()
            .expect("supervisor should exist")
            .start()
            .expect("initial Gateway should start");
        let restart_controller = Arc::clone(&controller);
        let restart_thread =
            thread::spawn(move || restart_controller.handle(ControlCommand::Restart));
        wait_for_resource_count(
            || usize::from(marker.exists()),
            1,
            "restart did not begin draining the old Gateway",
        );

        controller
            .handle(ControlCommand::ShutdownAgent)
            .expect("shutdown command should latch before waiting for restart");
        assert!(controller.is_shutdown_requested());

        let restart_result = restart_thread.join().expect("restart should join");
        let gateway_status = controller
            .supervisor
            .lock()
            .expect("supervisor should lock")
            .as_ref()
            .expect("supervisor should exist")
            .status();
        let _ = controller.stop_supervisor();
        assert_eq!(
            restart_result,
            Err(cmclient_control_api::ControlError::CommandFailed)
        );
        assert!(matches!(
            gateway_status,
            cmclient_supervisor::GatewayStatus::Stopped
        ));

        std::fs::remove_dir_all(data_dir).expect("test directory should remove");
    }

    #[test]
    fn local_shutdown_ack_precedes_agent_control_teardown() {
        let data_dir = std::env::temp_dir().join(format!(
            "cmclient-agent-shutdown-ack-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&data_dir);
        std::fs::create_dir_all(&data_dir).expect("test directory should exist");
        let config = AgentConfig {
            paths: RuntimePaths {
                data_dir: data_dir.clone(),
                config_dir: data_dir.clone(),
                cache_dir: data_dir.join("cache"),
                log_dir: data_dir.join("logs"),
            },
            config_file: data_dir.join("agent.toml"),
            runtime_profile: AgentRuntimeProfile::Native,
            gateway_command: None,
            callmesh: None,
            meshtastic: None,
            aprs: None,
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };
        let controller = Arc::new(
            AgentController::from_config_with_secrets_without_private_bootstrap(
                &config,
                AgentSecretStore::memory(),
            )
            .expect("controller should initialize"),
        );
        let endpoint = default_local_endpoint(&data_dir).expect("endpoint should derive");
        let handler: Arc<dyn ControlHandler> = controller.clone();
        let server = ControlServer::bind(endpoint.clone(), handler).expect("server should bind");
        let server_controller = Arc::clone(&controller);
        let server_thread = thread::spawn(move || {
            while !server.poll_once().expect("server poll should succeed") {}
            let deadline = Instant::now() + Duration::from_secs(1);
            while !server_controller.is_shutdown_requested() {
                assert!(
                    Instant::now() < deadline,
                    "shutdown should commit after its response is sent"
                );
                thread::sleep(Duration::from_millis(2));
            }
            drop(server);
        });

        let shutdown_status = ControlClient::new(endpoint)
            .expect("client should initialize")
            .shutdown_agent()
            .expect("shutdown response should arrive before Agent teardown");
        assert_eq!(shutdown_status.agent, "running");
        server_thread.join().expect("server thread should join");
        assert!(controller.is_shutdown_requested());

        drop(controller);
        std::fs::remove_dir_all(data_dir).expect("test directory should remove");
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn teardown_stops_the_gateway_while_a_control_event_stream_retains_the_controller() {
        let data_dir =
            std::env::temp_dir().join(format!("cmclient-agent-teardown-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&data_dir);
        std::fs::create_dir_all(&data_dir).expect("test directory should exist");
        let config = AgentConfig {
            paths: RuntimePaths {
                data_dir: data_dir.clone(),
                config_dir: data_dir.clone(),
                cache_dir: data_dir.join("cache"),
                log_dir: data_dir.join("logs"),
            },
            config_file: data_dir.join("agent.toml"),
            runtime_profile: AgentRuntimeProfile::Native,
            gateway_command: Some(vec![
                String::from("sh"),
                String::from("-c"),
                String::from("read _"),
            ]),
            callmesh: None,
            meshtastic: None,
            aprs: None,
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };
        let controller = Arc::new(
            AgentController::from_config_with_secrets_without_private_bootstrap(
                &config,
                AgentSecretStore::memory(),
            )
            .expect("controller should initialize"),
        );
        let mut worker =
            SupervisorWorker::start(Arc::clone(&controller)).expect("worker should start");
        controller
            .handle(ControlCommand::Start)
            .expect("gateway should start");

        let endpoint = default_local_endpoint(&data_dir).expect("endpoint should derive");
        let handler: Arc<dyn ControlHandler> = controller.clone();
        let server =
            Arc::new(ControlServer::bind(endpoint.clone(), handler).expect("server should bind"));
        let polling_server = Arc::clone(&server);
        let server_thread = thread::spawn(move || polling_server.serve_once());
        let mut events = ControlClient::new(endpoint.clone())
            .expect("client should initialize")
            .subscribe_update_events()
            .expect("control event stream should connect");
        server_thread
            .join()
            .expect("server should join")
            .expect("server should accept the event subscription");
        let initial_event = events
            .next_event()
            .expect("control event should decode")
            .expect("initial control event should arrive");
        assert_eq!(initial_event.event, "update.status_changed");
        assert!(Arc::strong_count(&controller) >= 3);
        let shutdown_server = Arc::clone(&server);
        let shutdown_thread = thread::spawn(move || shutdown_server.serve_once());
        ControlClient::new(endpoint)
            .expect("shutdown client should initialize")
            .shutdown_agent()
            .expect("shutdown response should arrive before Agent teardown");
        shutdown_thread
            .join()
            .expect("shutdown server should join")
            .expect("shutdown server should accept the request");
        assert!(controller.is_shutdown_requested());

        drop(server);
        assert!(
            events
                .next_event()
                .expect("server shutdown should close the control event stream")
                .is_none()
        );
        shutdown_agent_runtime(&controller, &mut worker).expect("Agent should tear down");
        assert!(matches!(
            controller
                .supervisor
                .lock()
                .expect("supervisor should lock")
                .as_ref()
                .expect("supervisor should exist")
                .status(),
            cmclient_supervisor::GatewayStatus::Stopped
        ));
        drop(events);
        std::fs::remove_dir_all(data_dir).expect("test directory should remove");
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn status_tick_preserves_the_precise_supervisor_error_code() {
        let data_dir = std::env::temp_dir().join(format!(
            "cmclient-agent-supervisor-error-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&data_dir);
        std::fs::create_dir_all(&data_dir).expect("test directory should exist");
        let missing_program = data_dir.join("missing-gateway");
        let config = AgentConfig {
            paths: RuntimePaths {
                data_dir: data_dir.clone(),
                config_dir: data_dir.clone(),
                cache_dir: data_dir.join("cache"),
                log_dir: data_dir.join("logs"),
            },
            config_file: data_dir.join("agent.toml"),
            runtime_profile: AgentRuntimeProfile::Native,
            gateway_command: Some(vec![missing_program.to_string_lossy().into_owned()]),
            callmesh: None,
            meshtastic: None,
            aprs: None,
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };
        let controller =
            AgentController::from_config(&config).expect("controller should initialize");
        let mut supervisor = GatewaySupervisor::new_with_stable_window(
            GatewayCommand {
                program: missing_program.to_string_lossy().into_owned(),
                arguments: Vec::new(),
            },
            BackoffPolicy {
                initial_delay: std::time::Duration::from_millis(10),
                maximum_delay: std::time::Duration::from_millis(10),
            },
            std::time::Duration::from_secs(1),
        )
        .expect("supervisor should initialize");
        assert!(supervisor.start().is_err());
        *controller
            .supervisor
            .lock()
            .expect("supervisor should lock") = Some(supervisor);
        std::thread::sleep(std::time::Duration::from_millis(20));

        assert_eq!(
            controller.handle(ControlCommand::Status),
            Err(cmclient_control_api::ControlError::CommandFailed)
        );
        assert_eq!(
            controller
                .latest_error_code
                .lock()
                .expect("error code should lock")
                .as_deref(),
            Some("GATEWAY_SUPERVISOR_SPAWN_FAILED")
        );
        std::fs::remove_dir_all(data_dir).expect("test directory should remove");
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn service_logs_agent_lifecycle_and_preserves_gateway_log_failures() {
        let sequence = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock should follow epoch")
            .as_nanos();
        let data_dir = std::env::temp_dir().join(format!(
            "cmclient-agent-runtime-logging-{}-{sequence}",
            std::process::id()
        ));
        let log_dir = data_dir.join("logs");
        std::fs::create_dir_all(log_dir.join("gateway.jsonl"))
            .expect("unsafe gateway log fixture should create");
        let config = AgentConfig {
            paths: RuntimePaths {
                data_dir: data_dir.clone(),
                config_dir: data_dir.clone(),
                cache_dir: data_dir.join("cache"),
                log_dir: log_dir.clone(),
            },
            config_file: data_dir.join("agent.toml"),
            runtime_profile: AgentRuntimeProfile::Native,
            gateway_command: Some(vec![
                String::from("sh"),
                String::from("-c"),
                String::from("read _"),
            ]),
            callmesh: None,
            meshtastic: None,
            aprs: None,
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };
        let controller = AgentController::from_config_with_secrets_without_private_bootstrap(
            &config,
            AgentSecretStore::memory(),
        )
        .expect("controller should initialize");

        let started = controller
            .handle(ControlCommand::Start)
            .expect("gateway should start without a logging sink");
        assert_eq!(
            started.latest_error_code.as_deref(),
            Some("RUNTIME_LOG_FILE_UNAVAILABLE")
        );
        let heartbeat = controller
            .handle(ControlCommand::Status)
            .expect("healthy supervisor tick should succeed");
        assert_eq!(
            heartbeat.latest_error_code.as_deref(),
            Some("RUNTIME_LOG_FILE_UNAVAILABLE")
        );
        let stopped = controller
            .handle(ControlCommand::Stop)
            .expect("gateway should stop");
        assert_eq!(
            stopped.latest_error_code.as_deref(),
            Some("RUNTIME_LOG_FILE_UNAVAILABLE")
        );

        let agent_log = read_runtime_log_family(&log_dir, "agent.jsonl");
        assert!(agent_log.contains("AGENT_RUNTIME_READY"));
        assert!(agent_log.contains("GATEWAY_SUPERVISOR_STARTED"));
        assert!(agent_log.contains("GATEWAY_SUPERVISOR_STOPPED"));
        assert!(!agent_log.contains(data_dir.to_string_lossy().as_ref()));
        for line in agent_log.lines() {
            serde_json::from_str::<serde_json::Value>(line)
                .expect("agent log lines should be structured JSON");
        }

        drop(controller);
        std::fs::remove_dir_all(data_dir).expect("test directory should remove");
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn persists_update_jobs_and_emits_follow_up_sse_status() {
        let data_dir =
            std::env::temp_dir().join(format!("cmclient-agent-updates-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&data_dir);
        let service = AgentUpdateService::new(&data_dir).expect("update service should initialize");
        let events = service.subscribe().expect("subscription should initialize");
        let initial = events
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("initial snapshot should arrive");
        let initial: UpdateControlStatus =
            serde_json::from_slice(&initial.data).expect("initial status should deserialize");
        assert!(initial.job.is_none());
        let job = PersistentUpdateJob {
            schema_version: 1,
            id: String::from("update-1"),
            phase: UpdatePhase::Downloading,
            created_at: String::from("2026-07-18T03:00:00.000Z"),
            updated_at: String::from("2026-07-18T03:01:00.000Z"),
            error_code: None,
            progress: None,
            recent_logs: Vec::new(),
            rollback_plan: None,
        };

        service.persist(&job).expect("job should persist");

        let event = events
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("status transition should arrive");
        let status: UpdateControlStatus =
            serde_json::from_slice(&event.data).expect("status should deserialize");
        assert_eq!(
            status.job.as_ref().map(|job| job.phase.as_str()),
            Some("downloading")
        );
        assert_eq!(service.status().expect("status should load"), status);
        let _ = std::fs::remove_dir_all(data_dir);
    }

    #[test]
    fn gateway_projection_rejects_a_body_completed_after_route_rotation() {
        use std::io::{Read as _, Write as _};

        let listener = TcpListener::bind("127.0.0.1:0").expect("Gateway fixture should bind");
        let address = listener.local_addr().expect("fixture address should load");
        let (partial_body_sent, partial_body_observed) = std::sync::mpsc::sync_channel(1);
        let (finish_body, finish_body_requested) = std::sync::mpsc::sync_channel(1);
        let fixture = thread::spawn(move || {
            let (mut stream, _) = listener
                .accept()
                .expect("projection request should connect");
            let mut request = Vec::new();
            let mut chunk = [0_u8; 2_048];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let count = stream.read(&mut chunk).expect("request should read");
                assert!(count > 0, "projection request ended before headers");
                request.extend_from_slice(&chunk[..count]);
            }
            let request = String::from_utf8(request)
                .expect("request should be UTF-8")
                .to_ascii_lowercase();
            assert!(request.contains(&format!(
                "{}: {}\r\n",
                super::GATEWAY_CAPABILITY_HEADER,
                "e".repeat(64)
            )));

            let body = br#"{"generation":"old"}"#;
            let midpoint = body.len() / 2;
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len(),
            )
            .and_then(|()| stream.write_all(&body[..midpoint]))
            .and_then(|()| stream.flush())
            .expect("partial projection should write");
            partial_body_sent
                .send(())
                .expect("partial body marker should send");
            finish_body_requested
                .recv_timeout(Duration::from_secs(2))
                .expect("rotation should release the fixture body");
            let _ = stream.write_all(&body[midpoint..]);
            let _ = stream.flush();
        });

        let route = cmclient_agent_core::web::GatewayRoute::new(address, "e".repeat(64))
            .expect("Gateway route should be valid");
        let session = GatewaySessionHandle::with_route(route.clone());
        let projection_route = route.clone();
        let projection = thread::spawn(move || {
            gateway_json_projection(
                &projection_route,
                cmclient_control_api::GatewayProjection::Nodes,
            )
        });
        partial_body_observed
            .recv_timeout(Duration::from_secs(2))
            .expect("projection should start reading the old body");

        let replacement = cmclient_agent_core::web::GatewayRoute::new(address, "f".repeat(64))
            .expect("replacement route should be valid");
        let rotation = thread::spawn(move || session.set(replacement));
        let deadline = Instant::now() + Duration::from_secs(2);
        while route.is_active() {
            assert!(
                Instant::now() < deadline,
                "route rotation did not revoke the in-flight generation"
            );
            thread::yield_now();
        }
        finish_body
            .send(())
            .expect("fixture should finish the old body");

        assert_eq!(
            projection.join().expect("projection worker should join"),
            Err(cmclient_control_api::ControlError::CommandFailed),
            "a complete stale projection must fail closed",
        );
        rotation.join().expect("route rotation should drain");
        fixture.join().expect("Gateway fixture should join");
    }

    #[test]
    fn route_rotation_prevents_old_gateway_events_from_reaching_control() {
        use std::io::{Read as _, Write as _};

        let listener = TcpListener::bind("127.0.0.1:0").expect("Gateway fixture should bind");
        let address = listener.local_addr().expect("fixture address should load");
        let (write_stale_event, stale_event_requested) = std::sync::mpsc::sync_channel(1);
        let fixture = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("event request should connect");
            let mut request = Vec::new();
            let mut chunk = [0_u8; 2_048];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let count = stream.read(&mut chunk).expect("request should read");
                assert!(count > 0, "event request ended before headers");
                request.extend_from_slice(&chunk[..count]);
            }
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream; charset=utf-8\r\nconnection: close\r\n\r\n: ready\n\n",
                )
                .and_then(|()| stream.flush())
                .expect("event response should start");
            stale_event_requested
                .recv_timeout(Duration::from_secs(2))
                .expect("rotation should release the stale event");
            let _ = stream.write_all(
                b"id: stale-1\nevent: mesh.position\ndata: {\"generation\":\"old\"}\n\n",
            );
            let _ = stream.flush();
        });

        let route = cmclient_agent_core::web::GatewayRoute::new(address, "1".repeat(64))
            .expect("Gateway route should be valid");
        let session = GatewaySessionHandle::with_route(route.clone());
        let (sender, receiver) = std::sync::mpsc::sync_channel(16);
        let bridge_route = route.clone();
        let bridge = thread::spawn(move || {
            let mut last_event_id = None;
            bridge_gateway_event_stream(
                &bridge_route,
                &sender,
                Duration::from_millis(100),
                &mut last_event_id,
            )
        });
        assert_eq!(
            receiver
                .recv_timeout(Duration::from_secs(2))
                .expect("ready heartbeat should prove the stream is active")
                .event,
            "gateway.heartbeat"
        );

        let replacement = cmclient_agent_core::web::GatewayRoute::new(address, "2".repeat(64))
            .expect("replacement route should be valid");
        let rotation = thread::spawn(move || session.set(replacement));
        let deadline = Instant::now() + Duration::from_secs(2);
        while route.is_active() {
            assert!(
                Instant::now() < deadline,
                "route rotation did not revoke the event generation"
            );
            thread::yield_now();
        }
        write_stale_event
            .send(())
            .expect("fixture should attempt the stale event");

        assert!(bridge.join().expect("event bridge should join"));
        rotation.join().expect("route rotation should drain");
        fixture.join().expect("Gateway fixture should join");
        assert!(
            receiver
                .try_iter()
                .all(|event| event.event != "mesh.position"),
            "an event from the revoked generation reached Control subscribers",
        );
    }

    #[test]
    fn incomplete_gateway_sse_fields_do_not_leak_across_event_boundaries() {
        use std::io::{Read as _, Write as _};

        let listener = TcpListener::bind("127.0.0.1:0").expect("Gateway fixture should bind");
        let address = listener.local_addr().expect("fixture address should load");
        let fixture = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("event request should connect");
            let mut request = Vec::new();
            let mut chunk = [0_u8; 2_048];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let count = stream.read(&mut chunk).expect("request should read");
                assert!(count > 0, "event request ended before headers");
                request.extend_from_slice(&chunk[..count]);
            }
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\nid: incomplete-1\nevent: mesh.position\n\ndata: {\"mustNotForward\":true}\n\n",
                )
                .expect("incomplete events should write");
        });

        let route = cmclient_agent_core::web::GatewayRoute::new(address, "3".repeat(64))
            .expect("Gateway route should be valid");
        let (sender, receiver) = std::sync::mpsc::sync_channel(8);
        let mut last_event_id = None;
        assert!(bridge_gateway_event_stream(
            &route,
            &sender,
            Duration::from_millis(100),
            &mut last_event_id,
        ));

        fixture.join().expect("Gateway fixture should join");
        assert_eq!(last_event_id, None);
        assert!(
            receiver
                .try_iter()
                .all(|event| { event.event != "mesh.position" && event.id != "incomplete-1" }),
            "fields from two incomplete SSE records were combined into an event",
        );
    }

    #[test]
    fn handwritten_gateway_outbound_http_does_not_return() {
        let source = include_str!("main.rs");
        let outbound = source
            .split_once("fn open_gateway_event_stream(")
            .and_then(|(_, source)| source.split_once("fn probe_gateway_event_receiver("))
            .map(|(source, _)| source)
            .expect("Gateway event client source should be bounded by stable functions");
        for forbidden in [
            "TcpStream::",
            "write_all(",
            "HTTP/1.",
            "MAX_RESPONSE_HEADER_BYTES",
            "trim_http_line(",
        ] {
            assert!(
                !outbound.contains(forbidden),
                "Gateway outbound HTTP must stay owned by reqwest: {forbidden}",
            );
        }
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn preserves_gateway_timeout_for_local_control_clients() {
        let gateway = TcpListener::bind("127.0.0.1:0").expect("gateway should bind");
        let gateway_address = gateway.local_addr().expect("gateway address should load");
        let gateway_thread = thread::spawn(move || {
            let (mut stream, _) = gateway.accept().expect("gateway should accept");
            let mut request = [0_u8; 4_096];
            let count = stream.read(&mut request).expect("request should read");
            let request = std::str::from_utf8(&request[..count]).expect("request should be UTF-8");
            assert!(request.contains(&format!(
                "{}: {}\r\n",
                super::GATEWAY_CAPABILITY_HEADER,
                "a".repeat(64)
            )));
            stream
                .write_all(
                    b"HTTP/1.1 504 Gateway Timeout\r\ncontent-type: application/json\r\ncontent-length: 26\r\nconnection: close\r\n\r\n{\"code\":\"GATEWAY_TIMEOUT\"}",
                )
                .expect("timeout response should write");
        });
        let route = cmclient_agent_core::web::GatewayRoute::new(gateway_address, "a".repeat(64))
            .expect("Gateway route should be valid");

        assert_eq!(
            gateway_json_projection(&route, cmclient_control_api::GatewayProjection::Nodes),
            Err(cmclient_control_api::ControlError::Timeout)
        );
        gateway_thread.join().expect("gateway should join");
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn bounds_gateway_sse_lines_before_allocating_an_event() {
        let mut oversized = Cursor::new(vec![b'a'; 64 * 1024 + 1]);
        assert!(read_bounded_gateway_sse_line(&mut oversized, &mut Vec::new()).is_err());
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn closes_a_gateway_event_bridge_instead_of_blocking_on_a_full_queue() {
        let (sender, _receiver) = mpsc::sync_channel(1);
        assert!(try_forward_gateway_event(&sender, gateway_heartbeat()));
        assert!(!try_forward_gateway_event(&sender, gateway_heartbeat()));
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn reconnects_the_gateway_event_bridge_with_the_last_forwarded_event_id() {
        let gateway = TcpListener::bind("127.0.0.1:0").expect("gateway should bind");
        let gateway_address = gateway.local_addr().expect("gateway address should load");
        let gateway_thread = thread::spawn(move || {
            let mut requests = Vec::new();
            for index in 1..=2 {
                let (mut stream, _) = gateway.accept().expect("gateway should accept");
                let mut request = Vec::new();
                let mut chunk = [0_u8; 2_048];
                while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                    let count = stream.read(&mut chunk).expect("request should read");
                    assert!(count > 0, "request ended before headers");
                    request.extend_from_slice(&chunk[..count]);
                }
                requests.push(String::from_utf8(request).expect("request should be UTF-8"));
                stream
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\nid: gateway-{index}\nevent: mesh.position\ndata: {{\"index\":{index}}}\n\n"
                        )
                        .as_bytes(),
                    )
                    .expect("event should write");
            }
            requests
        });
        let (sender, _receiver) = mpsc::sync_channel(8);
        let mut last_event_id = None;
        let route = cmclient_agent_core::web::GatewayRoute::new(gateway_address, "b".repeat(64))
            .expect("Gateway route should be valid");

        assert!(bridge_gateway_event_stream(
            &route,
            &sender,
            Duration::from_millis(100),
            &mut last_event_id,
        ));
        assert_eq!(last_event_id.as_deref(), Some("gateway-1"));
        assert!(bridge_gateway_event_stream(
            &route,
            &sender,
            Duration::from_millis(100),
            &mut last_event_id,
        ));
        assert_eq!(last_event_id.as_deref(), Some("gateway-2"));

        let requests = gateway_thread.join().expect("gateway should join");
        for request in &requests {
            assert!(request.contains(&format!(
                "{}: {}\r\n",
                super::GATEWAY_CAPABILITY_HEADER,
                "b".repeat(64)
            )));
        }
        assert!(!requests[0].to_ascii_lowercase().contains("last-event-id:"));
        assert!(
            requests[1]
                .to_ascii_lowercase()
                .contains("last-event-id: gateway-1\r\n")
        );
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn releases_repeated_gateway_event_bridges_when_a_hung_upstream_outlives_receivers() {
        const SUBSCRIPTIONS: usize = 12;
        assert_eq!(active_gateway_event_bridge_count(), 0);

        let gateway = TcpListener::bind("127.0.0.1:0").expect("gateway should bind");
        let gateway_address = gateway.local_addr().expect("gateway address should load");
        let connected = Arc::new(AtomicUsize::new(0));
        let closed = Arc::new(AtomicUsize::new(0));
        let gateway_thread = {
            let connected = Arc::clone(&connected);
            let closed = Arc::clone(&closed);
            thread::spawn(move || {
                let mut workers = Vec::new();
                for _ in 0..SUBSCRIPTIONS {
                    let (mut stream, _) = gateway.accept().expect("gateway should accept");
                    let connected = Arc::clone(&connected);
                    let closed = Arc::clone(&closed);
                    workers.push(thread::spawn(move || {
                        stream
                            .set_read_timeout(Some(Duration::from_secs(5)))
                            .expect("gateway read timeout should configure");
                        let mut request = Vec::new();
                        let mut chunk = [0_u8; 2_048];
                        while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                            let count = stream.read(&mut chunk).expect("request should read");
                            assert!(count > 0, "bridge request ended before its headers");
                            request.extend_from_slice(&chunk[..count]);
                        }
                        stream
                            .write_all(
                                b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream; charset=utf-8\r\ncache-control: no-cache\r\nconnection: keep-alive\r\n\r\n",
                            )
                            .expect("hung Gateway headers should write");
                        connected.fetch_add(1, Ordering::AcqRel);

                        let mut byte = [0_u8; 1];
                        assert_eq!(
                            stream.read(&mut byte).expect("bridge socket should close"),
                            0,
                            "bridge unexpectedly wrote after the SSE request"
                        );
                        closed.fetch_add(1, Ordering::AcqRel);
                    }));
                }
                for worker in workers {
                    worker.join().expect("Gateway connection should join");
                }
            })
        };

        let receivers = (0..SUBSCRIPTIONS)
            .map(|_| {
                bridge_gateway_events_with_read_poll(
                    cmclient_agent_core::web::GatewaySessionHandle::with_route(
                        cmclient_agent_core::web::GatewayRoute::new(
                            gateway_address,
                            "c".repeat(64),
                        )
                        .expect("Gateway route should be valid"),
                    ),
                    Duration::from_millis(100),
                )
            })
            .collect::<Vec<_>>();
        wait_for_resource_count(
            || connected.load(Ordering::Acquire),
            SUBSCRIPTIONS,
            "Gateway SSE bridges did not connect",
        );
        wait_for_resource_count(
            active_gateway_event_bridge_count,
            SUBSCRIPTIONS,
            "Gateway SSE bridge threads did not start",
        );

        drop(receivers);

        wait_for_resource_count(
            active_gateway_event_bridge_count,
            0,
            "Gateway SSE bridge threads survived receiver drop",
        );
        wait_for_resource_count(
            || closed.load(Ordering::Acquire),
            SUBSCRIPTIONS,
            "Gateway SSE bridge sockets survived receiver drop",
        );
        gateway_thread.join().expect("hung Gateway should join");
    }

    #[cfg(not(target_os = "windows"))]
    fn wait_for_resource_count(current: impl Fn() -> usize, expected: usize, message: &str) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while current() != expected {
            assert!(Instant::now() < deadline, "{message}");
            thread::sleep(Duration::from_millis(10));
        }
    }
}
