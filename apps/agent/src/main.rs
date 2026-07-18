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
use cmclient_supervisor::{BackoffPolicy, GatewayCommand, GatewayStatus, GatewaySupervisor};
use cmclient_updater::{PersistentUpdateJob, UpdateJournalStore, recover_interrupted_update};
use std::{
    collections::BTreeMap,
    io::{BufRead, BufReader, Read},
    net::SocketAddr,
    path::{Path, PathBuf},
    process::ExitCode,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
        mpsc::{self, RecvTimeoutError, SyncSender},
    },
    thread,
    time::{Duration, Instant},
};

const EX_USAGE: u8 = 2;
const EX_CONFIG: u8 = 5;
const UPDATE_EVENT_BUFFER: usize = 64;
const MAX_SSE_EVENT_BYTES: usize = 60 * 1024;

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
    management_web_config: ManagementWebConfig,
    management_access: Option<Arc<ManagementAccessController>>,
    control_endpoint: ControlEndpoint,
    remote_replay: Arc<RemoteControlReplayGuard>,
    secrets: AgentSecretStore,
    updates: Arc<AgentUpdateService>,
    started_at: Instant,
    latest_error_code: Mutex<Option<String>>,
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
            management_web_config,
            management_access,
            control_endpoint,
            remote_replay,
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
    let (sender, receiver) = mpsc::sync_channel(64);
    thread::spawn(move || {
        loop {
            if !bridge_gateway_event_stream(gateway, &sender) {
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
) -> bool {
    let client = match reqwest::blocking::Client::builder()
        .connect_timeout(Duration::from_secs(3))
        .no_proxy()
        .build()
    {
        Ok(client) => client,
        Err(_) => return false,
    };
    let response = match client
        .get(format!("http://{gateway}/api/v1/events"))
        .header("accept", "text/event-stream")
        .send()
    {
        Ok(response) if response.status().is_success() => response,
        _ => {
            thread::sleep(Duration::from_secs(2));
            return sender.send(gateway_heartbeat()).is_ok();
        }
    };
    let mut reader = BufReader::new(response);
    let mut id = None;
    let mut event = None;
    let mut data = None;
    loop {
        let mut line = Vec::new();
        let count = match read_bounded_gateway_sse_line(&mut reader, &mut line) {
            Ok(count) => count,
            Err(()) => return sender.send(gateway_heartbeat()).is_ok(),
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
            return sender.send(gateway_heartbeat()).is_ok();
        };
        if line.starts_with(':') {
            if sender.send(gateway_heartbeat()).is_err() {
                return false;
            }
            continue;
        }
        if line.is_empty() {
            if let (Some(id), Some(event), Some(data)) = (id.take(), event.take(), data.take())
                && sender.send(ControlUpdateEvent { id, event, data }).is_err()
            {
                return false;
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
    sender.send(gateway_heartbeat()).is_ok()
}

fn read_bounded_gateway_sse_line(
    reader: &mut impl BufRead,
    output: &mut Vec<u8>,
) -> Result<usize, ()> {
    const MAX_GATEWAY_SSE_LINE_BYTES: usize = 64 * 1024;
    output.clear();
    loop {
        let available = reader.fill_buf().map_err(|_| ())?;
        if available.is_empty() {
            return Ok(output.len());
        }
        let count = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |index| index + 1);
        if output.len().saturating_add(count) > MAX_GATEWAY_SSE_LINE_BYTES {
            return Err(());
        }
        let ended = available.get(count.saturating_sub(1)) == Some(&b'\n');
        output.extend_from_slice(&available[..count]);
        reader.consume(count);
        if ended {
            return Ok(output.len());
        }
    }
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
        ManagementWebControlStatus, ManagementWebRequest, RemoteControlReplayGuard, SecretKind,
        default_local_endpoint, gateway_json_projection, read_bounded_gateway_sse_line,
        remote_control_error_status, unix_now_seconds,
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
        ControlServer, ControlStatus, REMOTE_CONTROL_NONCE_HEADER, REMOTE_CONTROL_SCOPE_HEADER,
        REMOTE_CONTROL_TIMESTAMP_HEADER, StaticControlHandler, UpdateControlStatus,
        sign_remote_control_request,
    };
    #[cfg(not(target_os = "windows"))]
    use cmclient_updater::{PersistentUpdateJob, UpdatePhase};
    #[cfg(not(target_os = "windows"))]
    use std::{
        collections::BTreeMap,
        io::{Cursor, Read, Write},
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
        let token = "0123456789abcdef0123456789abcdef";
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
        assert_eq!(
            read_bounded_gateway_sse_line(&mut oversized, &mut Vec::new()),
            Err(())
        );
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
