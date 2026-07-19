use chrono::{SecondsFormat, Utc};
use cmclient_agent_core::access::{ManagementAccessController, ManagementAccessError};
use cmclient_agent_core::secrets::{AgentSecretStore, SecretKind};
use cmclient_agent_core::web::{
    ManagementTlsConfig, ManagementWebApiHandler, ManagementWebConfig, ManagementWebError,
    ManagementWebListener, ManagementWebRequest, ManagementWebService, ManagementWebStream,
    gateway_health,
};
use cmclient_agent_core::{
    AgentConfig, AgentLease, MeshtasticConnectionConfig, ensure_runtime_directories,
};
use cmclient_control_api::{
    ControlClient, ControlCommand, ControlEndpoint, ControlError, ControlHandler,
    ControlSecretKind, ControlServer, ControlStatus, ControlUpdateEvent, ControlUpdateEventStream,
    DiagnosticsControlBundle, GatewayControlStatus, GatewayProjection, ManagementWebControlStatus,
    REMOTE_CONTROL_NONCE_HEADER, REMOTE_CONTROL_SCOPE_HEADER, REMOTE_CONTROL_TIMESTAMP_HEADER,
    RemoteControlAuth, RemoteControlAuthError, RemoteControlReplayGuard, UpdateControlJob,
    UpdateControlStatus, default_local_endpoint,
};
use cmclient_supervisor::{
    BackoffPolicy, GatewayCommand, GatewayStatus, GatewaySupervisor, SupervisorEvent,
};
use cmclient_updater::{PersistentUpdateJob, UpdateJournalStore, recover_interrupted_update};
use std::{
    collections::BTreeMap,
    io::{BufRead, BufReader, Read, Write},
    net::{SocketAddr, TcpStream},
    path::{Path, PathBuf},
    process::ExitCode,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{self, RecvTimeoutError, SyncSender, TrySendError},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

#[cfg(test)]
use std::sync::atomic::AtomicUsize;

const EX_USAGE: u8 = 2;
const EX_CONFIG: u8 = 5;
const UPDATE_EVENT_BUFFER: usize = 64;
const MAX_SSE_EVENT_BYTES: usize = 60 * 1024;
const SUPERVISOR_POLL_INTERVAL: Duration = Duration::from_millis(100);
const GATEWAY_SSE_READ_POLL_INTERVAL: Duration = Duration::from_secs(1);
const GATEWAY_SSE_RECONNECT_DELAY: Duration = Duration::from_secs(2);

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
    control_endpoint: ControlEndpoint,
    secrets: AgentSecretStore,
    remote_replay: Arc<RemoteControlReplayGuard>,
}

impl ManagementWebApiHandler for AgentManagementWebApi {
    fn handle(
        &self,
        client: &mut dyn ManagementWebStream,
        request: &ManagementWebRequest,
    ) -> Result<bool, ManagementWebError> {
        if request.path.starts_with("/api/v1/control/") {
            return self.handle_remote_control(client, request);
        }
        if let Some(access) = &self.access {
            if request.method == "POST" && request.path == "/api/v1/auth/login" {
                return handle_management_login(client, request, access);
            }
            let public_static = matches!(request.method.as_str(), "GET" | "HEAD")
                && !request.path.starts_with("/api/");
            if !public_static {
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

impl AgentManagementWebApi {
    fn handle_remote_control(
        &self,
        client: &mut dyn ManagementWebStream,
        request: &ManagementWebRequest,
    ) -> Result<bool, ManagementWebError> {
        let token = match self.secrets.read(SecretKind::ManagementAdminToken) {
            Ok(Some(token)) => token,
            _ => {
                write_remote_control_auth_error(client, RemoteControlAuthError::Missing)?;
                return Ok(true);
            }
        };
        let auth = match remote_control_auth(request) {
            Ok(auth) => auth,
            Err(error) => {
                write_remote_control_auth_error(client, error)?;
                return Ok(true);
            }
        };
        if let Err(error) = self.remote_replay.verify_and_record(
            token.expose_secret(),
            &request.method,
            &request.path,
            &request.body,
            &auth,
            unix_now_seconds(),
        ) {
            write_remote_control_auth_error(client, error)?;
            return Ok(true);
        }
        let control = match ControlClient::new_with_timeout(
            self.control_endpoint.clone(),
            Duration::from_secs(30),
        ) {
            Ok(control) => control,
            Err(error) => {
                write_remote_control_error(client, &error)?;
                return Ok(true);
            }
        };
        dispatch_remote_control(client, request, &control)?;
        Ok(true)
    }
}

fn remote_control_auth(
    request: &ManagementWebRequest,
) -> Result<RemoteControlAuth, RemoteControlAuthError> {
    Ok(RemoteControlAuth {
        timestamp: request
            .header(REMOTE_CONTROL_TIMESTAMP_HEADER)
            .ok_or(RemoteControlAuthError::Missing)?
            .to_owned(),
        nonce: request
            .header(REMOTE_CONTROL_NONCE_HEADER)
            .ok_or(RemoteControlAuthError::Missing)?
            .to_owned(),
        scope: request
            .header(REMOTE_CONTROL_SCOPE_HEADER)
            .ok_or(RemoteControlAuthError::Missing)?
            .to_owned(),
        authorization: request
            .header("authorization")
            .ok_or(RemoteControlAuthError::Missing)?
            .to_owned(),
    })
}

fn dispatch_remote_control(
    client: &mut dyn ManagementWebStream,
    request: &ManagementWebRequest,
    control: &ControlClient,
) -> Result<(), ManagementWebError> {
    if request.method == "GET" && request.path == "/api/v1/control/updates/events" {
        return match control.subscribe_update_events() {
            Ok(events) => write_remote_control_event_stream(client, events),
            Err(error) => write_remote_control_error(client, &error),
        };
    }
    if request.method == "GET" && request.path == "/api/v1/control/events" {
        return match control.subscribe_gateway_events() {
            Ok(events) => write_remote_control_event_stream(client, events),
            Err(error) => write_remote_control_error(client, &error),
        };
    }
    let result = match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/api/v1/control/status") => control.status().and_then(to_json_value),
        ("POST", "/api/v1/control/start") => control.start().and_then(to_json_value),
        ("POST", "/api/v1/control/stop") => control.stop().and_then(to_json_value),
        ("POST", "/api/v1/control/restart") => control.restart().and_then(to_json_value),
        ("POST", "/api/v1/control/web/enable") => {
            control.enable_management_web().and_then(to_json_value)
        }
        ("POST", "/api/v1/control/web/disable") => {
            control.disable_management_web().and_then(to_json_value)
        }
        ("GET", "/api/v1/control/updates") => control.update_status().and_then(to_json_value),
        ("GET", "/api/v1/control/diagnostics/bundle") => {
            control.diagnostics_bundle().and_then(to_json_value)
        }
        ("GET", "/api/v1/control/gateway/meshtastic") => {
            control.gateway_projection(GatewayProjection::Meshtastic)
        }
        ("GET", "/api/v1/control/gateway/nodes") => {
            control.gateway_projection(GatewayProjection::Nodes)
        }
        ("GET", "/api/v1/control/gateway/positions") => {
            control.gateway_projection(GatewayProjection::Positions)
        }
        ("GET", "/api/v1/control/gateway/aprs") => {
            control.gateway_projection(GatewayProjection::Aprs)
        }
        ("GET", "/api/v1/control/gateway/callmesh") => {
            control.gateway_projection(GatewayProjection::CallMesh)
        }
        ("GET", "/api/v1/control/gateway/proxy") => {
            control.gateway_projection(GatewayProjection::Proxy)
        }
        ("GET", "/api/v1/control/events/recent") => {
            control.gateway_projection(GatewayProjection::RecentEvents)
        }
        ("POST", "/api/v1/control/database/integrity-check") => {
            control.gateway_projection(GatewayProjection::DatabaseIntegrity)
        }
        ("POST", "/api/v1/control/backups") => {
            control.gateway_projection(GatewayProjection::Backup)
        }
        ("PUT", path) if path.starts_with("/api/v1/control/secrets/") => {
            let Some(kind) = remote_secret_kind(path) else {
                write_management_json(
                    client,
                    "404 Not Found",
                    br#"{"code":"CONTROL_ROUTE_NOT_FOUND"}"#,
                )?;
                return Ok(());
            };
            let value =
                std::str::from_utf8(&request.body).map_err(|_| ManagementWebError::InvalidHttp)?;
            control.store_secret(kind, value).and_then(to_json_value)
        }
        ("DELETE", path) if path.starts_with("/api/v1/control/secrets/") => {
            let Some(kind) = remote_secret_kind(path) else {
                write_management_json(
                    client,
                    "404 Not Found",
                    br#"{"code":"CONTROL_ROUTE_NOT_FOUND"}"#,
                )?;
                return Ok(());
            };
            if !request.body.is_empty() {
                Err(ControlError::InvalidHttp)
            } else {
                control.remove_secret(kind).and_then(to_json_value)
            }
        }
        _ => {
            write_management_json(
                client,
                "404 Not Found",
                br#"{"code":"CONTROL_ROUTE_NOT_FOUND"}"#,
            )?;
            return Ok(());
        }
    };
    match result {
        Ok(value) => {
            let body = serde_json::to_vec(&value).map_err(|_| ManagementWebError::Io)?;
            write_management_json(client, "200 OK", &body)
        }
        Err(error) => write_remote_control_error(client, &error),
    }
}

fn to_json_value(value: impl serde::Serialize) -> Result<serde_json::Value, ControlError> {
    serde_json::to_value(value).map_err(|_| ControlError::InvalidHttp)
}

fn remote_secret_kind(path: &str) -> Option<ControlSecretKind> {
    match path.strip_prefix("/api/v1/control/secrets/")? {
        "callmesh-api-key" => Some(ControlSecretKind::CallMeshApiKey),
        "aprs-passcode" => Some(ControlSecretKind::AprsPasscode),
        "management-admin-token" => Some(ControlSecretKind::ManagementAdminToken),
        _ => None,
    }
}

fn write_remote_control_event_stream(
    client: &mut dyn ManagementWebStream,
    mut events: ControlUpdateEventStream,
) -> Result<(), ManagementWebError> {
    client
        .write_all(
            b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream; charset=utf-8\r\ncache-control: no-cache\r\nconnection: close\r\n\r\n",
        )
        .map_err(|_| ManagementWebError::Io)?;
    loop {
        match events.next_event() {
            Ok(Some(event)) => {
                if !is_safe_sse_token(&event.id)
                    || !is_safe_sse_token(&event.event)
                    || event.data.len() > MAX_SSE_EVENT_BYTES
                    || event.data.iter().any(|byte| matches!(*byte, b'\r' | b'\n'))
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
            Ok(None) => return Ok(()),
            Err(ControlError::Timeout) => client
                .write_all(b": heartbeat\n\n")
                .map_err(|_| ManagementWebError::Io)?,
            Err(_) => return Err(ManagementWebError::Io),
        }
    }
}

fn write_remote_control_auth_error(
    client: &mut dyn ManagementWebStream,
    error: RemoteControlAuthError,
) -> Result<(), ManagementWebError> {
    let status = match error {
        RemoteControlAuthError::Missing | RemoteControlAuthError::Invalid => "401 Unauthorized",
        RemoteControlAuthError::Expired | RemoteControlAuthError::Replay => "403 Forbidden",
    };
    let body = format!(r#"{{"code":"{}"}}"#, error.code());
    write_management_json(client, status, body.as_bytes())
}

fn write_remote_control_error(
    client: &mut dyn ManagementWebStream,
    error: &ControlError,
) -> Result<(), ManagementWebError> {
    let body = format!(r#"{{"code":"{}"}}"#, error.code());
    write_management_json(client, remote_control_error_status(error), body.as_bytes())
}

const fn remote_control_error_status(error: &ControlError) -> &'static str {
    match error {
        ControlError::Authentication => "401 Unauthorized",
        ControlError::Timeout => "504 Gateway Timeout",
        ControlError::InvalidHttp | ControlError::ResponseTooLarge => "502 Bad Gateway",
        _ => "503 Service Unavailable",
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
                    || event.data.len() > MAX_SSE_EVENT_BYTES
                    || event.data.iter().any(|byte| matches!(*byte, b'\r' | b'\n'))
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
    management_web_shutdown: Mutex<Option<JoinHandle<Result<(), ManagementWebError>>>>,
    management_web_config: ManagementWebConfig,
    management_access: Option<Arc<ManagementAccessController>>,
    control_endpoint: ControlEndpoint,
    remote_replay: Arc<RemoteControlReplayGuard>,
    secrets: AgentSecretStore,
    updates: Arc<AgentUpdateService>,
    shutdown_requested: AtomicBool,
    started_at: Instant,
    latest_error_code: Mutex<Option<String>>,
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

impl AgentController {
    fn from_config(config: &AgentConfig) -> Result<Self, ControlError> {
        let secrets = AgentSecretStore::platform();
        let control_endpoint = default_local_endpoint(&config.paths.data_dir);
        let remote_replay = Arc::new(RemoteControlReplayGuard::default());
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
                if let Some(aprs) = &config.aprs {
                    let passcode = secrets
                        .read(SecretKind::AprsPasscode)
                        .map_err(|_| ControlError::CommandFailed)?;
                    environment.insert(
                        String::from("CMCLIENT_APRS_ENABLED"),
                        passcode.is_some().to_string(),
                    );
                    if let Some(passcode) = passcode {
                        environment.insert(
                            String::from("CMCLIENT_APRS_LOGIN_CALLSIGN"),
                            aprs.login_callsign.clone(),
                        );
                        environment.insert(
                            String::from("CMCLIENT_APRS_PASSCODE"),
                            passcode.expose_secret().to_owned(),
                        );
                        environment.insert(String::from("CMCLIENT_APRS_HOST"), aprs.host.clone());
                        environment
                            .insert(String::from("CMCLIENT_APRS_PORT"), aprs.port.to_string());
                        environment.insert(
                            String::from("CMCLIENT_APRS_DESTINATION"),
                            aprs.destination.clone(),
                        );
                        environment.insert(
                            String::from("CMCLIENT_APRS_SYMBOL_TABLE"),
                            aprs.symbol_table.to_string(),
                        );
                        environment.insert(
                            String::from("CMCLIENT_APRS_SYMBOL_CODE"),
                            aprs.symbol_code.to_string(),
                        );
                        if let Some(comment) = &aprs.comment {
                            environment
                                .insert(String::from("CMCLIENT_APRS_COMMENT"), comment.clone());
                        }
                    }
                }
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
            static_web_root: Some(resolve_static_web_root()),
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
                        control_endpoint: control_endpoint.clone(),
                        secrets: secrets.clone(),
                        remote_replay: Arc::clone(&remote_replay),
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
            management_web_shutdown: Mutex::new(None),
            management_web_config,
            management_access,
            control_endpoint,
            remote_replay,
            secrets,
            updates,
            shutdown_requested: AtomicBool::new(false),
            started_at: Instant::now(),
            latest_error_code: Mutex::new(None),
        })
    }

    fn status(&self) -> Result<ControlStatus, ControlError> {
        self.tick_supervisor()?;
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
                ManagementWebService::start_with_api_handler(
                    &self.management_web_config,
                    Arc::new(AgentManagementWebApi {
                        updates: Arc::clone(&self.updates),
                        access: self.management_access.clone(),
                        control_endpoint: self.control_endpoint.clone(),
                        secrets: self.secrets.clone(),
                        remote_replay: Arc::clone(&self.remote_replay),
                    }),
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
                .spawn(move || service.stop())
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
        let service_result = service.map_or(Ok(()), |service| {
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
        if let Ok(mut latest_error_code) = self.latest_error_code.lock() {
            *latest_error_code = Some(String::from(code));
        }
    }

    fn clear_error(&self) {
        if let Ok(mut latest_error_code) = self.latest_error_code.lock() {
            *latest_error_code = None;
        }
    }

    fn has_precise_supervisor_error(&self) -> bool {
        self.latest_error_code
            .lock()
            .is_ok_and(|latest_error_code| {
                latest_error_code
                    .as_deref()
                    .is_some_and(|code| code.starts_with("GATEWAY_SUPERVISOR_"))
            })
    }

    fn stop_supervisor(&self) -> Result<bool, ControlError> {
        let mut supervisor = self
            .supervisor
            .lock()
            .map_err(|_| ControlError::CommandFailed)?;
        if let Some(supervisor) = supervisor.as_mut() {
            supervisor.stop().map_err(|_| ControlError::CommandFailed)?;
            return Ok(true);
        }
        Ok(false)
    }

    fn tick_supervisor(&self) -> Result<(), ControlError> {
        if self.is_shutdown_requested() {
            return Ok(());
        }
        let result = {
            let mut supervisor = self
                .supervisor
                .lock()
                .map_err(|_| ControlError::CommandFailed)?;
            if self.is_shutdown_requested() {
                return Ok(());
            }
            match supervisor.as_mut() {
                Some(supervisor) => supervisor.tick().map(Some),
                None => Ok(None),
            }
        };
        match result {
            Ok(Some(SupervisorEvent::Started { .. } | SupervisorEvent::Heartbeat { .. })) => {
                if let Ok(mut latest_error_code) = self.latest_error_code.lock()
                    && latest_error_code
                        .as_deref()
                        .is_some_and(|code| code.starts_with("GATEWAY_SUPERVISOR_"))
                {
                    *latest_error_code = None;
                }
                Ok(())
            }
            Ok(_) => Ok(()),
            Err(error) => {
                self.remember_error_code(error.code());
                Err(ControlError::CommandFailed)
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
    supervisor_result.and(management_web_result)
}

impl ControlHandler for AgentController {
    fn handle(&self, command: ControlCommand) -> Result<ControlStatus, ControlError> {
        let result = match command {
            ControlCommand::Status => self.status(),
            ControlCommand::Start => {
                self.ensure_resource_start_allowed()?;
                let mut supervisor = self
                    .supervisor
                    .lock()
                    .map_err(|_| ControlError::CommandFailed)?;
                self.ensure_resource_start_allowed()?;
                supervisor
                    .as_mut()
                    .ok_or(ControlError::CommandFailed)?
                    .start()
                    .map_err(|_| ControlError::CommandFailed)?;
                drop(supervisor);
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
                {
                    let mut supervisor = self
                        .supervisor
                        .lock()
                        .map_err(|_| ControlError::CommandFailed)?;
                    self.ensure_resource_start_allowed()?;
                    let supervisor = supervisor.as_mut().ok_or(ControlError::CommandFailed)?;
                    supervisor.stop().map_err(|_| ControlError::CommandFailed)?;
                    self.ensure_resource_start_allowed()?;
                    supervisor
                        .start()
                        .map_err(|_| ControlError::CommandFailed)?;
                }
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
            Err(ControlError::CommandFailed)
                if matches!(command, ControlCommand::Status)
                    && self.has_precise_supervisor_error() => {}
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

    fn gateway_projection(
        &self,
        projection: GatewayProjection,
    ) -> Result<serde_json::Value, ControlError> {
        gateway_json_projection(self.gateway, projection)
    }

    fn subscribe_gateway_events(&self) -> Result<mpsc::Receiver<ControlUpdateEvent>, ControlError> {
        Ok(bridge_gateway_events(self.gateway))
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

fn gateway_json_projection(
    gateway: SocketAddr,
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
        .no_proxy()
        .build()
        .map_err(|_| ControlError::CommandFailed)?;
    let response = client
        .request(method, format!("http://{gateway}{path}"))
        .header("accept", "application/json")
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
    if bytes.len() as u64 > MAX_GATEWAY_PROJECTION_BYTES {
        return Err(ControlError::ResponseTooLarge);
    }
    serde_json::from_slice(&bytes).map_err(|_| ControlError::InvalidHttp)
}

fn map_gateway_request_error(error: reqwest::Error) -> ControlError {
    if error.is_timeout() {
        ControlError::Timeout
    } else {
        ControlError::CommandFailed
    }
}

fn map_gateway_io_error(error: std::io::Error) -> ControlError {
    if matches!(
        error.kind(),
        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
    ) {
        ControlError::Timeout
    } else {
        ControlError::CommandFailed
    }
}

fn bridge_gateway_events(gateway: SocketAddr) -> mpsc::Receiver<ControlUpdateEvent> {
    bridge_gateway_events_with_read_poll(gateway, GATEWAY_SSE_READ_POLL_INTERVAL)
}

fn bridge_gateway_events_with_read_poll(
    gateway: SocketAddr,
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
                if !probe_gateway_event_receiver(&sender)
                    || !bridge_gateway_event_stream(gateway, &sender, read_poll, &mut last_event_id)
                {
                    break;
                }
                thread::sleep(Duration::from_millis(250));
            }
        });
    receiver
}

fn bridge_gateway_event_stream(
    gateway: SocketAddr,
    sender: &SyncSender<ControlUpdateEvent>,
    read_poll: Duration,
    last_event_id: &mut Option<String>,
) -> bool {
    let mut reader = match open_gateway_event_stream(gateway, read_poll, last_event_id.as_deref()) {
        Ok(reader) => reader,
        Err(()) => return wait_for_gateway_event_retry(sender, GATEWAY_SSE_RECONNECT_DELAY),
    };
    let mut id: Option<String> = None;
    let mut event: Option<String> = None;
    let mut data: Option<Vec<u8>> = None;
    let mut next_receiver_probe = Instant::now() + read_poll;
    loop {
        if Instant::now() >= next_receiver_probe {
            if !probe_gateway_event_receiver(sender) {
                return false;
            }
            next_receiver_probe = Instant::now() + read_poll;
        }
        let mut line = Vec::new();
        let count = loop {
            match read_bounded_gateway_sse_line(&mut reader, &mut line) {
                Ok(count) => break count,
                Err(GatewaySseLineReadError::Poll) => {
                    if !probe_gateway_event_receiver(sender) {
                        return false;
                    }
                    next_receiver_probe = Instant::now() + read_poll;
                }
                Err(GatewaySseLineReadError::Invalid) => {
                    return probe_gateway_event_receiver(sender);
                }
            }
        };
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
            if !try_forward_gateway_event(sender, gateway_heartbeat()) {
                return false;
            }
            continue;
        }
        if line.is_empty() {
            if let (Some(id), Some(event), Some(data)) = (id.take(), event.take(), data.take()) {
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
    probe_gateway_event_receiver(sender)
}

fn open_gateway_event_stream(
    gateway: SocketAddr,
    read_poll: Duration,
    last_event_id: Option<&str>,
) -> Result<BufReader<TcpStream>, ()> {
    const MAX_RESPONSE_HEADER_BYTES: usize = 8 * 1024;
    let mut stream =
        TcpStream::connect_timeout(&gateway, Duration::from_secs(3)).map_err(|_| ())?;
    stream
        .set_read_timeout(Some(read_poll))
        .and_then(|_| stream.set_write_timeout(Some(Duration::from_secs(3))))
        .map_err(|_| ())?;
    let replay_header = last_event_id
        .filter(|value| is_safe_sse_token(value))
        .map_or_else(String::new, |value| format!("last-event-id: {value}\r\n"));
    stream
        .write_all(
            format!(
                "GET /api/v1/events HTTP/1.0\r\nhost: {gateway}\r\naccept: text/event-stream\r\n{replay_header}connection: close\r\n\r\n"
            )
            .as_bytes(),
        )
        .map_err(|_| ())?;

    let mut reader = BufReader::new(stream);
    let mut header_bytes = 0_usize;
    let mut content_type_valid = false;
    let mut transfer_encoding_present = false;
    for index in 0..128 {
        let mut line = Vec::new();
        let count = read_bounded_gateway_sse_line(&mut reader, &mut line).map_err(|_| ())?;
        if count == 0 {
            return Err(());
        }
        header_bytes = header_bytes.checked_add(count).ok_or(())?;
        if header_bytes > MAX_RESPONSE_HEADER_BYTES {
            return Err(());
        }
        trim_http_line(&mut line);
        if index == 0 {
            if line != b"HTTP/1.1 200 OK" && line != b"HTTP/1.0 200 OK" {
                return Err(());
            }
            continue;
        }
        if line.is_empty() {
            return (content_type_valid && !transfer_encoding_present)
                .then_some(reader)
                .ok_or(());
        }
        let Some(separator) = line.iter().position(|byte| *byte == b':') else {
            return Err(());
        };
        let name = &line[..separator];
        let value = line[separator + 1..]
            .iter()
            .copied()
            .skip_while(|byte| byte.is_ascii_whitespace())
            .collect::<Vec<_>>();
        if name.eq_ignore_ascii_case(b"content-type") {
            content_type_valid = value
                .split(|byte| *byte == b';')
                .next()
                .is_some_and(|media_type| media_type.eq_ignore_ascii_case(b"text/event-stream"));
        } else if name.eq_ignore_ascii_case(b"transfer-encoding") {
            transfer_encoding_present = true;
        }
    }
    Err(())
}

fn trim_http_line(line: &mut Vec<u8>) {
    if line.last() == Some(&b'\n') {
        line.pop();
    }
    if line.last() == Some(&b'\r') {
        line.pop();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GatewaySseLineReadError {
    Poll,
    Invalid,
}

fn read_bounded_gateway_sse_line(
    reader: &mut impl BufRead,
    output: &mut Vec<u8>,
) -> Result<usize, GatewaySseLineReadError> {
    const MAX_GATEWAY_SSE_LINE_BYTES: usize = 64 * 1024;
    loop {
        let available = reader.fill_buf().map_err(|error| {
            if matches!(
                error.kind(),
                std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
            ) {
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

#[cfg(test)]
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
        static_web_root: Some(resolve_static_web_root()),
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
            control_endpoint: default_local_endpoint(&config.paths.data_dir),
            secrets: AgentSecretStore::platform(),
            remote_replay: Arc::new(RemoteControlReplayGuard::default()),
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

fn install_shutdown_signal_handler(controller: Arc<AgentController>) -> Result<(), ControlError> {
    ctrlc::set_handler(move || controller.request_shutdown())
        .map_err(|_| ControlError::CommandFailed)
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
    let control_handler: Arc<dyn ControlHandler> = controller.clone();
    let server = match ControlServer::bind(endpoint, control_handler) {
        Ok(server) => server,
        Err(error) => {
            eprintln!("{}", error.code());
            return ExitCode::from(EX_CONFIG);
        }
    };
    if let Err(error) = install_shutdown_signal_handler(Arc::clone(&controller)) {
        eprintln!("{}", error.code());
        return ExitCode::from(EX_CONFIG);
    }
    let mut supervisor_worker = match SupervisorWorker::start(Arc::clone(&controller)) {
        Ok(worker) => worker,
        Err(error) => {
            eprintln!("{}", error.code());
            return ExitCode::from(EX_CONFIG);
        }
    };
    let serve_error = loop {
        if controller.is_shutdown_requested() {
            break None;
        }
        match server.serve_once() {
            Ok(()) if controller.is_shutdown_requested() => break None,
            Ok(()) => {}
            Err(error) => break Some(error),
        }
    };
    let shutdown_error = shutdown_agent_runtime(&controller, &mut supervisor_worker).err();
    if let Some(error) = &serve_error {
        eprintln!("{}", error.code());
    }
    if let Some(error) = shutdown_error {
        eprintln!("{}", error.code());
        return ExitCode::from(EX_CONFIG);
    }
    if serve_error.is_some() {
        ExitCode::from(EX_CONFIG)
    } else {
        ExitCode::SUCCESS
    }
}

fn gateway_address(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
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

fn resolve_gateway_command(config: &AgentConfig) -> Option<Vec<String>> {
    if let Some(command) = &config.gateway_command {
        return Some(command.clone());
    }
    let configured = std::env::var_os("CMCLIENT_GATEWAY_ENTRYPOINT")
        .map(PathBuf::from)
        .filter(|path| path.is_absolute() && path.is_file());
    let bundled = bundled_root().map(|root| root.join("gateway/dist/main.js"));
    let development = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../gateway/dist/main.js");
    [configured, bundled, Some(development)]
        .into_iter()
        .flatten()
        .find(|path| path.is_file())
        .map(|entrypoint| {
            vec![
                String::from("node"),
                entrypoint.to_string_lossy().into_owned(),
            ]
        })
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
    #[cfg(not(target_os = "windows"))]
    use super::{
        AgentConfig, AgentController, AgentManagementWebApi, AgentSecretStore, AgentUpdateService,
        ControlCommand, ControlHandler, GatewayControlStatus, ManagementWebApiHandler,
        ManagementWebControlStatus, ManagementWebRequest, ManagementWebService,
        RemoteControlReplayGuard, SecretKind, SupervisorWorker, active_gateway_event_bridge_count,
        bridge_gateway_event_stream, bridge_gateway_events_with_read_poll, default_local_endpoint,
        gateway_heartbeat, gateway_json_projection, read_bounded_gateway_sse_line,
        remote_control_error_status, shutdown_agent_runtime, try_forward_gateway_event,
        unix_now_seconds,
    };
    #[cfg(not(target_os = "windows"))]
    use argon2::{Argon2, PasswordHasher, password_hash::SaltString};
    #[cfg(not(target_os = "windows"))]
    use cmclient_agent_core::{
        RuntimePaths,
        access::{LanAccessConfig, ManagementAccessController},
    };
    #[cfg(not(target_os = "windows"))]
    use cmclient_control_api::{
        ControlClient, ControlServer, ControlStatus, REMOTE_CONTROL_NONCE_HEADER,
        REMOTE_CONTROL_SCOPE_HEADER, REMOTE_CONTROL_TIMESTAMP_HEADER, StaticControlHandler,
        UpdateControlStatus, sign_remote_control_request,
    };
    #[cfg(not(target_os = "windows"))]
    use cmclient_supervisor::{BackoffPolicy, GatewayCommand, GatewaySupervisor};
    #[cfg(not(target_os = "windows"))]
    use cmclient_updater::{PersistentUpdateJob, UpdatePhase};
    #[cfg(not(target_os = "windows"))]
    use std::{
        collections::BTreeMap,
        io::{Cursor, Read, Write},
        net::{SocketAddr, TcpListener, TcpStream},
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
            mpsc,
        },
        thread,
        time::{Duration, Instant},
    };

    #[cfg(not(target_os = "windows"))]
    struct DisableManagementWebRoute {
        controller: Arc<AgentController>,
        completed: mpsc::SyncSender<()>,
    }

    #[cfg(not(target_os = "windows"))]
    impl ManagementWebApiHandler for DisableManagementWebRoute {
        fn handle(
            &self,
            client: &mut dyn super::ManagementWebStream,
            _request: &ManagementWebRequest,
        ) -> Result<bool, super::ManagementWebError> {
            self.controller
                .disable_management_web()
                .map_err(|_| super::ManagementWebError::Io)?;
            self.completed
                .send(())
                .map_err(|_| super::ManagementWebError::Io)?;
            client
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 2\r\nconnection: close\r\n\r\n{}",
                )
                .map_err(|_| super::ManagementWebError::Io)?;
            Ok(true)
        }
    }

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
                String::from("read _"),
            ]),
            gateway_port: port,
            callmesh: None,
            meshtastic: None,
            aprs: None,
            proxy: None,
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
            gateway_command: Some(vec![
                String::from("sh"),
                String::from("-c"),
                format!("printf x >> '{}'; exit 7", marker.display()),
            ]),
            gateway_port: 48_109,
            callmesh: None,
            meshtastic: None,
            aprs: None,
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };
        let controller =
            Arc::new(AgentController::from_config(&config).expect("controller should initialize"));
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
            gateway_command: Some(vec![
                String::from("sh"),
                String::from("-c"),
                String::from("exit 0"),
            ]),
            gateway_port: 48_113,
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
            gateway_command: Some(vec![
                String::from("sh"),
                String::from("-c"),
                String::from("sleep 5"),
            ]),
            gateway_port: 48_114,
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
            gateway_command: Some(vec![
                String::from("sh"),
                String::from("-c"),
                format!("read _; printf x > '{}'; exec sleep 5", marker.display()),
            ]),
            gateway_port: 48_115,
            callmesh: None,
            meshtastic: None,
            aprs: None,
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };
        let controller =
            Arc::new(AgentController::from_config(&config).expect("controller should initialize"));
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

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn teardown_stops_the_gateway_while_a_control_sse_retains_the_controller() {
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
            gateway_command: Some(vec![
                String::from("sh"),
                String::from("-c"),
                String::from("read _"),
            ]),
            gateway_port: 48_110,
            callmesh: None,
            meshtastic: None,
            aprs: None,
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };
        let controller =
            Arc::new(AgentController::from_config(&config).expect("controller should initialize"));
        let mut worker =
            SupervisorWorker::start(Arc::clone(&controller)).expect("worker should start");
        controller
            .handle(ControlCommand::Start)
            .expect("gateway should start");

        let endpoint = default_local_endpoint(&data_dir);
        let handler: Arc<dyn ControlHandler> = controller.clone();
        let server = ControlServer::bind(endpoint.clone(), handler).expect("server should bind");
        let server_thread = thread::spawn(move || server.serve_once());
        let events = ControlClient::new(endpoint)
            .expect("client should initialize")
            .subscribe_update_events()
            .expect("control SSE should connect");
        server_thread
            .join()
            .expect("server should join")
            .expect("server should accept SSE");
        assert!(Arc::strong_count(&controller) >= 3);
        controller
            .handle(ControlCommand::ShutdownAgent)
            .expect("local shutdown command should be accepted");
        assert!(controller.is_shutdown_requested());

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
        assert!(Arc::strong_count(&controller) >= 2);

        drop(events);
        std::fs::remove_dir_all(data_dir).expect("test directory should remove");
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn management_web_can_disable_itself_without_a_worker_join_deadlock() {
        let data_dir =
            std::env::temp_dir().join(format!("cmclient-agent-web-disable-{}", std::process::id()));
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
            gateway_command: None,
            gateway_port: 48_112,
            callmesh: None,
            meshtastic: None,
            aprs: None,
            proxy: None,
            management_web_enabled: false,
            management_lan: None,
        };
        let controller =
            Arc::new(AgentController::from_config(&config).expect("controller should initialize"));
        let (completed_sender, completed_receiver) = mpsc::sync_channel(1);
        let service = ManagementWebService::start_with_api_handler(
            &cmclient_agent_core::web::ManagementWebConfig {
                port: 0,
                static_web_root: None,
                ..Default::default()
            },
            Arc::new(DisableManagementWebRoute {
                controller: Arc::clone(&controller),
                completed: completed_sender,
            }),
        )
        .expect("management service should start");
        let address = service.local_addr();
        *controller
            .management_web
            .lock()
            .expect("management service should lock") = Some(service);

        let mut client = TcpStream::connect(address).expect("client should connect");
        client
            .write_all(b"GET / HTTP/1.1\r\nhost: localhost\r\n\r\n")
            .expect("request should write");
        completed_receiver
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("disable handler should return");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
        while !controller
            .reap_management_web_shutdown()
            .expect("shutdown should reap")
        {
            assert!(
                std::time::Instant::now() < deadline,
                "management shutdown did not drain its request worker"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert!(
            controller
                .management_web
                .lock()
                .expect("management service should lock")
                .is_none()
        );

        drop(client);
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
            gateway_command: Some(vec![missing_program.to_string_lossy().into_owned()]),
            gateway_port: 48_111,
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
            control_endpoint: default_local_endpoint(&data_dir),
            secrets: AgentSecretStore::platform(),
            remote_replay: Arc::new(RemoteControlReplayGuard::default()),
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
    #[test]
    fn remote_control_requires_a_scoped_signature_and_rejects_replay() {
        let data_dir =
            std::env::temp_dir().join(format!("cmc-agent-remote-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&data_dir);
        std::fs::create_dir_all(&data_dir).expect("test directory should exist");
        let endpoint = default_local_endpoint(&data_dir);
        let status = ControlStatus {
            schema_version: 2,
            agent: String::from("running"),
            agent_version: String::from("2.0.0-test"),
            gateway: GatewayControlStatus::Running,
            management_web: ManagementWebControlStatus::Running,
            management_web_url: Some(String::from("https://cmclient.example")),
            uptime_seconds: 10,
            latest_error_code: None,
        };
        let server = ControlServer::bind(
            endpoint.clone(),
            Arc::new(StaticControlHandler::new(status)),
        )
        .expect("control server should bind");
        let server_thread = thread::spawn(move || server.serve_once());
        let token = "test-token-test-token-test-token-0";
        let secrets = AgentSecretStore::memory();
        secrets
            .store(SecretKind::ManagementAdminToken, token)
            .expect("remote token should store");
        let api = Arc::new(AgentManagementWebApi {
            updates: Arc::new(
                AgentUpdateService::new(&data_dir).expect("update service should initialize"),
            ),
            access: None,
            control_endpoint: endpoint,
            secrets,
            remote_replay: Arc::new(RemoteControlReplayGuard::default()),
        });
        let path = "/api/v1/control/status";
        let auth = sign_remote_control_request(token, "GET", path, b"", unix_now_seconds())
            .expect("remote request should sign");
        let request = ManagementWebRequest {
            method: String::from("GET"),
            path: String::from(path),
            headers: BTreeMap::from([
                (String::from("authorization"), auth.authorization),
                (
                    String::from(REMOTE_CONTROL_TIMESTAMP_HEADER),
                    auth.timestamp,
                ),
                (String::from(REMOTE_CONTROL_NONCE_HEADER), auth.nonce),
                (String::from(REMOTE_CONTROL_SCOPE_HEADER), auth.scope),
            ]),
            body: Vec::new(),
            remote_addr: "192.168.1.20:54000"
                .parse()
                .expect("fixture address should parse"),
        };

        let allowed = invoke_management_api(Arc::clone(&api), request.clone());
        assert!(allowed.starts_with("HTTP/1.1 200 OK"));
        assert!(allowed.contains(r#""agent_version":"2.0.0-test""#));
        server_thread
            .join()
            .expect("control server thread should join")
            .expect("control server should respond");

        let replayed = invoke_management_api(Arc::clone(&api), request);
        assert!(replayed.starts_with("HTTP/1.1 403 Forbidden"));
        assert!(replayed.contains("REMOTE_CONTROL_REPLAY_REJECTED"));

        let missing = invoke_management_api(
            api,
            ManagementWebRequest {
                method: String::from("GET"),
                path: String::from(path),
                headers: BTreeMap::new(),
                body: Vec::new(),
                remote_addr: "192.168.1.20:54000"
                    .parse()
                    .expect("fixture address should parse"),
            },
        );
        assert!(missing.starts_with("HTTP/1.1 401 Unauthorized"));
        assert!(missing.contains("REMOTE_CONTROL_AUTH_MISSING"));
        let _ = std::fs::remove_dir_all(data_dir);
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn preserves_gateway_timeout_for_local_and_remote_control_clients() {
        let gateway = TcpListener::bind("127.0.0.1:0").expect("gateway should bind");
        let gateway_address = gateway.local_addr().expect("gateway address should load");
        let gateway_thread = thread::spawn(move || {
            let (mut stream, _) = gateway.accept().expect("gateway should accept");
            let mut request = [0_u8; 4_096];
            let _ = stream.read(&mut request).expect("request should read");
            stream
                .write_all(
                    b"HTTP/1.1 504 Gateway Timeout\r\ncontent-type: application/json\r\ncontent-length: 26\r\nconnection: close\r\n\r\n{\"code\":\"GATEWAY_TIMEOUT\"}",
                )
                .expect("timeout response should write");
        });

        assert_eq!(
            gateway_json_projection(
                gateway_address,
                cmclient_control_api::GatewayProjection::Nodes
            ),
            Err(cmclient_control_api::ControlError::Timeout)
        );
        assert_eq!(
            remote_control_error_status(&cmclient_control_api::ControlError::Timeout),
            "504 Gateway Timeout"
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

        assert!(bridge_gateway_event_stream(
            gateway_address,
            &sender,
            Duration::from_millis(100),
            &mut last_event_id,
        ));
        assert_eq!(last_event_id.as_deref(), Some("gateway-1"));
        assert!(bridge_gateway_event_stream(
            gateway_address,
            &sender,
            Duration::from_millis(100),
            &mut last_event_id,
        ));
        assert_eq!(last_event_id.as_deref(), Some("gateway-2"));

        let requests = gateway_thread.join().expect("gateway should join");
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
                bridge_gateway_events_with_read_poll(gateway_address, Duration::from_millis(100))
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
