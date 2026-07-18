use chrono::{SecondsFormat, Utc};
use cmclient_agent_core::access::{ManagementAccessController, ManagementAccessError};
use cmclient_agent_core::secrets::{AgentSecretStore, SecretKind};
use cmclient_agent_core::web::{
    ManagementTlsConfig, ManagementWebApiHandler, ManagementWebConfig, ManagementWebError,
    ManagementWebListener, ManagementWebRequest, ManagementWebService, ManagementWebStream,
    gateway_health,
};
use cmclient_agent_core::{AgentConfig, AgentLease, ensure_runtime_directories};
use cmclient_control_api::{
    ControlCommand, ControlEndpoint, ControlError, ControlHandler, ControlSecretKind,
    ControlServer, ControlStatus, ControlUpdateEvent, DiagnosticsControlBundle,
    GatewayControlStatus, ManagementWebControlStatus, UpdateControlJob, UpdateControlStatus,
    default_local_endpoint,
};
use cmclient_supervisor::{BackoffPolicy, GatewayCommand, GatewayStatus, GatewaySupervisor};
use cmclient_updater::{PersistentUpdateJob, UpdateJournalStore, recover_interrupted_update};
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    path::Path,
    process::ExitCode,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
        mpsc::{self, RecvTimeoutError, SyncSender},
    },
    time::{Duration, Instant},
};

const EX_USAGE: u8 = 2;
const EX_CONFIG: u8 = 5;
const UPDATE_EVENT_BUFFER: usize = 64;

struct AgentUpdateService {
    journal: UpdateJournalStore,
    subscribers: Mutex<Vec<SyncSender<ControlUpdateEvent>>>,
    next_event_id: AtomicU64,
}

impl AgentUpdateService {
    fn new(data_dir: &Path) -> Result<Self, ControlError> {
        Ok(Self {
            journal: UpdateJournalStore::new(data_dir).map_err(|_| ControlError::CommandFailed)?,
            subscribers: Mutex::new(Vec::new()),
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

    fn publish(&self, job: &PersistentUpdateJob) -> Result<(), ControlError> {
        let status = UpdateControlStatus {
            schema_version: 1,
            job: Some(update_control_job(job.clone())),
        };
        let event = self.event_for(&status)?;
        self.subscribers
            .lock()
            .map_err(|_| ControlError::CommandFailed)?
            .retain(|sender| sender.try_send(event.clone()).is_ok());
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

struct AgentManagementWebApi {
    updates: Arc<AgentUpdateService>,
    access: Option<Arc<ManagementAccessController>>,
}

impl ManagementWebApiHandler for AgentManagementWebApi {
    fn handle(
        &self,
        client: &mut dyn ManagementWebStream,
        request: &ManagementWebRequest,
    ) -> Result<bool, ManagementWebError> {
        if let Some(access) = &self.access {
            if request.method == "POST" && request.path == "/api/v1/auth/login" {
                return handle_management_login(client, request, access);
            }
            if !matches!(
                (request.method.as_str(), request.path.as_str()),
                ("GET", "/") | ("GET", "/index.html")
            ) {
                let write = !matches!(request.method.as_str(), "GET" | "HEAD" | "OPTIONS");
                let session = request.header("cookie").and_then(management_session_cookie);
                let result = session
                    .ok_or(ManagementAccessError::SessionInvalid)
                    .and_then(|session| {
                        access.authorize(
                            request.header("origin"),
                            session,
                            request.header("x-csrf-token"),
                            write,
                            unix_now_seconds(),
                        )
                    });
                if let Err(error) = result {
                    write_management_access_error(client, error)?;
                    return Ok(true);
                }
            }
        }
        match (request.method.as_str(), request.path.as_str()) {
            ("GET", "/api/v1/updates") => {
                let status = match self.updates.status() {
                    Ok(status) => status,
                    Err(_) => {
                        write_management_error(client, "CONTROL_COMMAND_FAILED")?;
                        return Ok(true);
                    }
                };
                let body = serde_json::to_vec(&status).map_err(|_| ManagementWebError::Io)?;
                write_management_json(client, "200 OK", &body)?;
                Ok(true)
            }
            ("GET", "/api/v1/updates/events") => {
                let events = match self.updates.subscribe() {
                    Ok(events) => events,
                    Err(_) => {
                        write_management_error(client, "CONTROL_COMMAND_FAILED")?;
                        return Ok(true);
                    }
                };
                write_management_update_events(client, events)?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }
}

fn handle_management_login(
    client: &mut dyn ManagementWebStream,
    request: &ManagementWebRequest,
    access: &ManagementAccessController,
) -> Result<bool, ManagementWebError> {
    let password = serde_json::from_slice::<serde_json::Value>(&request.body)
        .ok()
        .and_then(|value| {
            let object = value.as_object()?;
            if object.len() != 1 {
                return None;
            }
            object.get("password")?.as_str().map(str::to_owned)
        })
        .filter(|password| !password.is_empty() && password.len() <= 1024);
    let result = password
        .ok_or(ManagementAccessError::CredentialsInvalid)
        .and_then(|password| {
            access.login(
                &request.remote_addr.ip().to_string(),
                request.header("origin").unwrap_or_default(),
                &password,
                unix_now_seconds(),
            )
        });
    match result {
        Ok(session) => {
            let body = serde_json::json!({
                "schemaVersion": 1,
                "csrfToken": session.csrf_token,
                "expiresAt": session.expires_at_unix_seconds,
            });
            let body = serde_json::to_vec(&body).map_err(|_| ManagementWebError::Io)?;
            let max_age = session
                .expires_at_unix_seconds
                .saturating_sub(unix_now_seconds());
            write_management_json_with_headers(
                client,
                "200 OK",
                &body,
                &[format!(
                    "set-cookie: cmclient_session={}; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age={max_age}",
                    session.id
                )],
            )?;
            Ok(true)
        }
        Err(error) => {
            write_management_access_error(client, error)?;
            Ok(true)
        }
    }
}

fn management_session_cookie(value: &str) -> Option<&str> {
    value.split(';').find_map(|part| {
        let (name, value) = part.trim().split_once('=')?;
        (name == "cmclient_session"
            && value.len() == 32
            && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .then_some(value)
    })
}

fn unix_now_seconds() -> u64 {
    Utc::now().timestamp().max(0) as u64
}

fn write_management_access_error(
    client: &mut dyn ManagementWebStream,
    error: ManagementAccessError,
) -> Result<(), ManagementWebError> {
    let status = match error {
        ManagementAccessError::CredentialsInvalid
        | ManagementAccessError::SessionInvalid
        | ManagementAccessError::SessionExpired => "401 Unauthorized",
        ManagementAccessError::LoginRateLimited => "429 Too Many Requests",
        ManagementAccessError::OriginDenied | ManagementAccessError::CsrfInvalid => "403 Forbidden",
        ManagementAccessError::InvalidConfiguration => "500 Internal Server Error",
    };
    let body = format!(r#"{{"code":"{}"}}"#, error.code());
    write_management_json(client, status, body.as_bytes())
}

fn write_management_json(
    client: &mut dyn ManagementWebStream,
    status: &str,
    body: &[u8],
) -> Result<(), ManagementWebError> {
    write_management_json_with_headers(client, status, body, &[])
}

fn write_management_json_with_headers(
    client: &mut dyn ManagementWebStream,
    status: &str,
    body: &[u8],
    headers: &[String],
) -> Result<(), ManagementWebError> {
    let header = format!(
        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncache-control: no-store\r\n{}content-length: {}\r\nconnection: close\r\n\r\n",
        headers
            .iter()
            .map(|header| format!("{header}\r\n"))
            .collect::<String>(),
        body.len(),
    );
    client
        .write_all(header.as_bytes())
        .and_then(|_| client.write_all(body))
        .map_err(|_| ManagementWebError::Io)
}

fn write_management_error(
    client: &mut dyn ManagementWebStream,
    code: &str,
) -> Result<(), ManagementWebError> {
    let body = format!(r#"{{"code":"{code}"}}"#);
    write_management_json(client, "503 Service Unavailable", body.as_bytes())
}

fn write_management_update_events(
    client: &mut dyn ManagementWebStream,
    events: mpsc::Receiver<ControlUpdateEvent>,
) -> Result<(), ManagementWebError> {
    client
        .write_all(
            b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream; charset=utf-8\r\ncache-control: no-cache\r\nconnection: close\r\n\r\n",
        )
        .map_err(|_| ManagementWebError::Io)?;
    loop {
        match events.recv_timeout(Duration::from_secs(15)) {
            Ok(event) => {
                if !is_safe_sse_token(&event.id)
                    || !is_safe_sse_token(&event.event)
                    || event.data.contains(&b'\n')
                {
                    return Err(ManagementWebError::InvalidHttp);
                }
                client
                    .write_all(
                        format!("id: {}\nevent: {}\ndata: ", event.id, event.event).as_bytes(),
                    )
                    .and_then(|_| client.write_all(&event.data))
                    .and_then(|_| client.write_all(b"\n\n"))
                    .map_err(|_| ManagementWebError::Io)?;
            }
            Err(RecvTimeoutError::Timeout) => client
                .write_all(b": heartbeat\n\n")
                .map_err(|_| ManagementWebError::Io)?,
            Err(RecvTimeoutError::Disconnected) => return Ok(()),
        }
    }
}

fn is_safe_sse_token(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
}

struct AgentController {
    supervisor: Mutex<Option<GatewaySupervisor>>,
    gateway: SocketAddr,
    management_web: Mutex<Option<ManagementWebService>>,
    management_web_config: ManagementWebConfig,
    management_access: Option<Arc<ManagementAccessController>>,
    secrets: AgentSecretStore,
    updates: Arc<AgentUpdateService>,
    started_at: Instant,
    latest_error_code: Mutex<Option<String>>,
}

impl AgentController {
    fn from_config(config: &AgentConfig) -> Result<Self, ControlError> {
        let secrets = AgentSecretStore::platform();
        let supervisor = config
            .gateway_command
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
                    (
                        String::from("CMCLIENT_GATEWAY_HOST"),
                        String::from("127.0.0.1"),
                    ),
                    (
                        String::from("CMCLIENT_GATEWAY_PORT"),
                        config.gateway_port.to_string(),
                    ),
                    (
                        String::from("CMCLIENT_DATA_DIR"),
                        config.paths.data_dir.to_string_lossy().into_owned(),
                    ),
                ]);
                if let Some(callmesh) = &config.callmesh {
                    environment.insert(String::from("CMCLIENT_CALLMESH_URL"), callmesh.url.clone());
                    if let Some(api_key) = secrets
                        .read(SecretKind::CallMeshApiKey)
                        .map_err(|_| ControlError::CommandFailed)?
                    {
                        environment.insert(
                            String::from("CMCLIENT_CALLMESH_API_KEY"),
                            api_key.expose_secret().to_owned(),
                        );
                    }
                }
                supervisor.set_environment(environment);
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
        let management_web_config = ManagementWebConfig {
            enabled: true,
            gateway: gateway_address(config.gateway_port),
            bind: config
                .management_lan
                .as_ref()
                .map_or_else(|| std::net::IpAddr::from([127, 0, 0, 1]), |lan| lan.bind),
            port: config.management_lan.as_ref().map_or(7080, |lan| lan.port),
            allow_lan: management_access.is_some(),
            tls: config
                .management_lan
                .as_ref()
                .map(|lan| ManagementTlsConfig {
                    certificate_path: lan.certificate_path.clone(),
                    private_key_path: lan.private_key_path.clone(),
                }),
        };
        let updates = Arc::new(AgentUpdateService::new(&config.paths.data_dir)?);
        updates.recover()?;
        let management_web = if config.management_web_enabled {
            Some(
                ManagementWebService::start_with_api_handler(
                    &management_web_config,
                    Arc::new(AgentManagementWebApi {
                        updates: Arc::clone(&updates),
                        access: management_access.clone(),
                    }),
                )
                .map_err(|_| ControlError::CommandFailed)?,
            )
        } else {
            None
        };
        Ok(Self {
            supervisor: Mutex::new(supervisor),
            gateway: gateway_address(config.gateway_port),
            management_web: Mutex::new(management_web),
            management_web_config,
            management_access,
            secrets,
            updates,
            started_at: Instant::now(),
            latest_error_code: Mutex::new(None),
        })
    }

    fn status(&self) -> Result<ControlStatus, ControlError> {
        let mut supervisor = self
            .supervisor
            .lock()
            .map_err(|_| ControlError::CommandFailed)?;
        let lifecycle = match supervisor.as_mut() {
            Some(supervisor) => {
                let _ = supervisor
                    .poll_heartbeat()
                    .map_err(|_| ControlError::CommandFailed)?;
                match supervisor.status() {
                    GatewayStatus::Stopped => GatewayControlStatus::Stopped,
                    GatewayStatus::Running { .. } => GatewayControlStatus::Running,
                    GatewayStatus::Backoff { .. } => GatewayControlStatus::Backoff,
                }
            }
            None => GatewayControlStatus::Stopped,
        };
        drop(supervisor);
        let gateway = match lifecycle {
            GatewayControlStatus::Running if gateway_health(self.gateway) => {
                GatewayControlStatus::Running
            }
            GatewayControlStatus::Running => GatewayControlStatus::Degraded,
            status => status,
        };
        let management_web_scheme = if self.management_web_config.tls.is_some() {
            "https"
        } else {
            "http"
        };
        let (management_web, management_web_url) = self
            .management_web
            .lock()
            .map_err(|_| ControlError::CommandFailed)?
            .as_ref()
            .map_or((ManagementWebControlStatus::Disabled, None), |service| {
                (
                    ManagementWebControlStatus::Running,
                    Some(format!(
                        "{management_web_scheme}://{}",
                        service.local_addr()
                    )),
                )
            });
        let latest_error_code = match gateway {
            GatewayControlStatus::Backoff => Some(String::from("GATEWAY_RESTART_BACKOFF")),
            GatewayControlStatus::Degraded => Some(String::from("GATEWAY_HEALTH_DEGRADED")),
            _ => self
                .latest_error_code
                .lock()
                .map_err(|_| ControlError::CommandFailed)?
                .clone(),
        };
        Ok(ControlStatus {
            schema_version: 2,
            agent: String::from("running"),
            agent_version: String::from(env!("CARGO_PKG_VERSION")),
            gateway,
            management_web,
            management_web_url,
            uptime_seconds: self.started_at.elapsed().as_secs(),
            latest_error_code,
        })
    }

    fn enable_management_web(&self) -> Result<ControlStatus, ControlError> {
        let mut management_web = self
            .management_web
            .lock()
            .map_err(|_| ControlError::CommandFailed)?;
        if management_web.is_none() {
            *management_web = Some(
                ManagementWebService::start_with_api_handler(
                    &self.management_web_config,
                    Arc::new(AgentManagementWebApi {
                        updates: Arc::clone(&self.updates),
                        access: self.management_access.clone(),
                    }),
                )
                .map_err(|_| ControlError::CommandFailed)?,
            );
        }
        drop(management_web);
        self.status()
    }

    fn disable_management_web(&self) -> Result<ControlStatus, ControlError> {
        let service = self
            .management_web
            .lock()
            .map_err(|_| ControlError::CommandFailed)?
            .take();
        if let Some(service) = service {
            service.stop().map_err(|_| ControlError::CommandFailed)?;
        }
        self.status()
    }

    fn remember_error(&self, error: &ControlError) {
        if let Ok(mut latest_error_code) = self.latest_error_code.lock() {
            *latest_error_code = Some(String::from(error.code()));
        }
    }

    fn clear_error(&self) {
        if let Ok(mut latest_error_code) = self.latest_error_code.lock() {
            *latest_error_code = None;
        }
    }
}

impl ControlHandler for AgentController {
    fn handle(&self, command: ControlCommand) -> Result<ControlStatus, ControlError> {
        let result = match command {
            ControlCommand::Status => self.status(),
            ControlCommand::Start => {
                let mut supervisor = self
                    .supervisor
                    .lock()
                    .map_err(|_| ControlError::CommandFailed)?;
                supervisor
                    .as_mut()
                    .ok_or(ControlError::CommandFailed)?
                    .start()
                    .map_err(|_| ControlError::CommandFailed)?;
                drop(supervisor);
                self.status()
            }
            ControlCommand::Stop => {
                let mut supervisor = self
                    .supervisor
                    .lock()
                    .map_err(|_| ControlError::CommandFailed)?;
                supervisor
                    .as_mut()
                    .ok_or(ControlError::CommandFailed)?
                    .stop()
                    .map_err(|_| ControlError::CommandFailed)?;
                drop(supervisor);
                self.status()
            }
            ControlCommand::Restart => {
                {
                    let mut supervisor = self
                        .supervisor
                        .lock()
                        .map_err(|_| ControlError::CommandFailed)?;
                    let supervisor = supervisor.as_mut().ok_or(ControlError::CommandFailed)?;
                    supervisor.stop().map_err(|_| ControlError::CommandFailed)?;
                    supervisor
                        .start()
                        .map_err(|_| ControlError::CommandFailed)?;
                }
                self.status()
            }
            ControlCommand::EnableManagementWeb => self.enable_management_web(),
            ControlCommand::DisableManagementWeb => self.disable_management_web(),
        };
        match &result {
            Ok(_) if !matches!(command, ControlCommand::Status) => self.clear_error(),
            Ok(_) => {}
            Err(error) => self.remember_error(error),
        }
        result
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
            schema_version: 1,
            agent_version: status.agent_version,
            gateway: status.gateway,
            management_web: status.management_web,
            latest_error_code: status.latest_error_code,
            update_error_code: update.job.as_ref().and_then(|job| job.error_code.clone()),
            update_log_codes: update.job.map_or_else(Vec::new, |job| job.recent_log_codes),
        })
    }

    fn store_secret(&self, kind: ControlSecretKind, value: &str) -> Result<(), ControlError> {
        self.secrets
            .store(secret_kind(kind), value)
            .map_err(|_| ControlError::CommandFailed)
    }

    fn remove_secret(&self, kind: ControlSecretKind) -> Result<bool, ControlError> {
        self.secrets
            .remove(secret_kind(kind))
            .map_err(|_| ControlError::CommandFailed)
    }
}

const fn secret_kind(kind: ControlSecretKind) -> SecretKind {
    match kind {
        ControlSecretKind::CallMeshApiKey => SecretKind::CallMeshApiKey,
        ControlSecretKind::AprsPasscode => SecretKind::AprsPasscode,
        ControlSecretKind::ManagementAdminToken => SecretKind::ManagementAdminToken,
    }
}

fn main() -> ExitCode {
    let mut arguments = std::env::args().skip(1);
    match arguments.next().as_deref() {
        Some("--serve") => serve(),
        Some("--serve-web-once") => serve_web_once(),
        None | Some("--check-config") | Some("--check-instance") => match AgentConfig::load() {
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
                eprintln!("{}", error.code());
                ExitCode::from(EX_CONFIG)
            }
        },
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

fn serve_web_once() -> ExitCode {
    let config = match AgentConfig::load() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{}", error.code());
            return ExitCode::from(EX_CONFIG);
        }
    };
    let web_config = ManagementWebConfig {
        enabled: config.management_web_enabled,
        gateway: gateway_address(config.gateway_port),
        bind: config
            .management_lan
            .as_ref()
            .map_or_else(|| std::net::IpAddr::from([127, 0, 0, 1]), |lan| lan.bind),
        port: config.management_lan.as_ref().map_or(7080, |lan| lan.port),
        allow_lan: config.management_lan.is_some(),
        tls: config
            .management_lan
            .as_ref()
            .map(|lan| ManagementTlsConfig {
                certificate_path: lan.certificate_path.clone(),
                private_key_path: lan.private_key_path.clone(),
            }),
    };
    let management_access = match config.management_lan.as_ref() {
        Some(lan) => match ManagementAccessController::new(lan.access.clone()) {
            Ok(access) => Some(Arc::new(access)),
            Err(_) => {
                eprintln!("MANAGEMENT_LAN_AUTH_CONFIGURATION_INVALID");
                return ExitCode::from(EX_CONFIG);
            }
        },
        None => None,
    };
    let updates = match AgentUpdateService::new(&config.paths.data_dir) {
        Ok(updates) => Arc::new(updates),
        Err(error) => {
            eprintln!("{}", error.code());
            return ExitCode::from(EX_CONFIG);
        }
    };
    if let Err(error) = updates.recover() {
        eprintln!("{}", error.code());
        return ExitCode::from(EX_CONFIG);
    }
    let listener = match ManagementWebListener::bind_with_api_handler(
        &web_config,
        Arc::new(AgentManagementWebApi {
            updates,
            access: management_access,
        }),
    ) {
        Ok(listener) => listener,
        Err(error) => {
            eprintln!("{}", error.code());
            return ExitCode::from(EX_CONFIG);
        }
    };
    listener.serve_once().map_or_else(
        |error| {
            eprintln!("{}", error.code());
            ExitCode::from(EX_CONFIG)
        },
        |_| ExitCode::SUCCESS,
    )
}

fn serve() -> ExitCode {
    let config = match AgentConfig::load() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{}", error.code());
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
    let endpoint = match default_local_endpoint(&config.paths.data_dir) {
        ControlEndpoint::UnixSocket(path) => ControlEndpoint::unix(path),
        endpoint => endpoint,
    };
    let server = match ControlServer::bind(endpoint, controller) {
        Ok(server) => server,
        Err(error) => {
            eprintln!("{}", error.code());
            return ExitCode::from(EX_CONFIG);
        }
    };
    loop {
        if let Err(error) = server.serve_once() {
            eprintln!("{}", error.code());
            return ExitCode::from(EX_CONFIG);
        }
    }
}

fn gateway_address(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

#[cfg(test)]
mod tests {
    #[cfg(not(target_os = "windows"))]
    use super::{
        AgentConfig, AgentController, AgentManagementWebApi, AgentUpdateService, ControlCommand,
        ControlHandler, GatewayControlStatus, ManagementWebApiHandler, ManagementWebControlStatus,
        ManagementWebRequest,
    };
    #[cfg(not(target_os = "windows"))]
    use argon2::{Argon2, PasswordHasher, password_hash::SaltString};
    #[cfg(not(target_os = "windows"))]
    use cmclient_agent_core::{
        RuntimePaths,
        access::{LanAccessConfig, ManagementAccessController},
    };
    #[cfg(not(target_os = "windows"))]
    use cmclient_control_api::UpdateControlStatus;
    #[cfg(not(target_os = "windows"))]
    use cmclient_updater::{PersistentUpdateJob, UpdatePhase};
    #[cfg(not(target_os = "windows"))]
    use std::{
        collections::BTreeMap,
        io::{Read, Write},
        net::{SocketAddr, TcpListener, TcpStream},
        path::PathBuf,
        sync::Arc,
        thread,
    };

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn reports_running_only_after_gateway_health_succeeds() {
        let gateway = TcpListener::bind("127.0.0.1:0").expect("gateway should bind");
        let port = gateway
            .local_addr()
            .expect("gateway address should load")
            .port();
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
            gateway_command: Some(vec![
                String::from("sh"),
                String::from("-c"),
                String::from("sleep 30"),
            ]),
            gateway_port: port,
            callmesh: None,
            management_web_enabled: false,
            management_lan: None,
        };
        let controller = AgentController::from_config(&config).expect("controller should build");

        let diagnostics = controller
            .diagnostics_bundle()
            .expect("sanitized diagnostics should build");
        let serialized =
            serde_json::to_string(&diagnostics).expect("sanitized diagnostics should serialize");
        assert_eq!(diagnostics.schema_version, 1);
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

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn lan_api_requires_a_session_and_csrf_before_gateway_proxying() {
        let data_dir =
            std::env::temp_dir().join(format!("cmclient-agent-access-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&data_dir);
        let salt = SaltString::encode_b64(b"cmclient-agent-access-test")
            .expect("fixture salt should encode");
        let password_hash = Argon2::default()
            .hash_password(b"password", &salt)
            .expect("fixture password should hash")
            .to_string();
        let access = Arc::new(
            ManagementAccessController::new(LanAccessConfig {
                password_hash,
                allowed_origins: std::collections::BTreeSet::from([String::from(
                    "https://cmclient.example",
                )]),
                session_ttl_seconds: 60,
                audit_capacity: 32,
            })
            .expect("access configuration should load"),
        );
        let api = Arc::new(AgentManagementWebApi {
            updates: Arc::new(
                AgentUpdateService::new(&data_dir).expect("update service should initialize"),
            ),
            access: Some(access),
        });
        let remote_addr: SocketAddr = "192.168.1.20:54000"
            .parse()
            .expect("fixture address should parse");

        let denied = invoke_management_api(
            Arc::clone(&api),
            ManagementWebRequest {
                method: String::from("GET"),
                path: String::from("/api/v1/updates"),
                headers: BTreeMap::new(),
                body: Vec::new(),
                remote_addr,
            },
        );
        assert!(denied.starts_with("HTTP/1.1 401 Unauthorized"));
        assert!(denied.contains("MANAGEMENT_SESSION_INVALID"));

        let login = invoke_management_api(
            Arc::clone(&api),
            ManagementWebRequest {
                method: String::from("POST"),
                path: String::from("/api/v1/auth/login"),
                headers: BTreeMap::from([(
                    String::from("origin"),
                    String::from("https://cmclient.example"),
                )]),
                body: br#"{"password":"password"}"#.to_vec(),
                remote_addr,
            },
        );
        assert!(login.starts_with("HTTP/1.1 200 OK"));
        assert!(login.contains("HttpOnly; Secure; SameSite=Strict"));
        let cookie = login
            .lines()
            .find(|line| line.starts_with("set-cookie:"))
            .and_then(|line| {
                line.split_once('=')
                    .map(|(_, value)| value.split(';').next().unwrap_or_default())
            })
            .expect("login should issue a session cookie");

        let allowed = invoke_management_api(
            Arc::clone(&api),
            ManagementWebRequest {
                method: String::from("GET"),
                path: String::from("/api/v1/updates"),
                headers: BTreeMap::from([(
                    String::from("cookie"),
                    format!("cmclient_session={cookie}"),
                )]),
                body: Vec::new(),
                remote_addr,
            },
        );
        assert!(allowed.starts_with("HTTP/1.1 200 OK"));
        assert!(allowed.contains(r#"{"schemaVersion":1,"job":null}"#));

        let csrf_denied = invoke_management_api(
            api,
            ManagementWebRequest {
                method: String::from("POST"),
                path: String::from("/api/v1/updates"),
                headers: BTreeMap::from([
                    (String::from("cookie"), format!("cmclient_session={cookie}")),
                    (
                        String::from("origin"),
                        String::from("https://cmclient.example"),
                    ),
                ]),
                body: Vec::new(),
                remote_addr,
            },
        );
        assert!(csrf_denied.starts_with("HTTP/1.1 403 Forbidden"));
        assert!(csrf_denied.contains("MANAGEMENT_CSRF_INVALID"));
        let _ = std::fs::remove_dir_all(data_dir);
    }

    #[cfg(not(target_os = "windows"))]
    fn invoke_management_api(
        api: Arc<AgentManagementWebApi>,
        request: ManagementWebRequest,
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
        let address = listener.local_addr().expect("address should load");
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("listener should accept");
            api.handle(&mut stream, &request)
                .expect("handler should respond");
        });
        let mut client = TcpStream::connect(address).expect("client should connect");
        let mut response = String::new();
        client
            .read_to_string(&mut response)
            .expect("response should read");
        server.join().expect("server should join");
        response
    }
}
