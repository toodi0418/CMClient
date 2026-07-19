use clap::{Parser, Subcommand};
use cmclient_agent_core::AgentConfig;
use cmclient_cli_client::{ExitCode, parse_endpoint};
use cmclient_control_api::{
    ControlClient, ControlEndpoint, ControlError, ControlSecretKind, ControlSecretReceipt,
    ControlStatus, ControlUpdateEvent, ControlUpdateEventStream, DiagnosticsControlBundle,
    GatewayControlStatus, GatewayProjection, REMOTE_CONTROL_NONCE_HEADER,
    REMOTE_CONTROL_SCOPE_HEADER, REMOTE_CONTROL_TIMESTAMP_HEADER, UpdateControlStatus,
    default_local_endpoint, sign_remote_control_request,
};
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader, IsTerminal, Read},
    process::ExitCode as ProcessExitCode,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, RecvTimeoutError},
    },
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use zeroize::{Zeroize, Zeroizing};

#[derive(Debug, Parser)]
#[command(name = "cmclient", version, about = "CMClient Agent control client")]
struct Cli {
    #[arg(long, global = true)]
    json: bool,
    #[arg(long, global = true)]
    quiet: bool,
    #[arg(long, global = true)]
    no_color: bool,
    #[arg(long, global = true, default_value_t = 30, value_parser = clap::value_parser!(u64).range(1..=86_400))]
    timeout: u64,
    #[arg(long, global = true, default_value = "local")]
    endpoint: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Status,
    Start,
    Stop,
    Restart,
    Version,
    Logs {
        #[arg(long)]
        follow: bool,
    },
    Events {
        #[arg(long)]
        follow: bool,
    },
    Doctor,
    Web,
    Meshtastic,
    Nodes,
    Positions,
    Aprs,
    Proxy,
    Update {
        #[arg(long)]
        follow: bool,
    },
    Backup,
    Diagnostics,
    Secret {
        #[command(subcommand)]
        command: SecretCommand,
    },
    Database,
}

#[derive(Debug, Subcommand)]
enum SecretCommand {
    /// Reads one secret value from standard input and stores it in the Agent-selected backend.
    Set {
        kind: String,
    },
    Remove {
        kind: String,
    },
}

enum CliControlClient {
    Local(ControlClient),
    Remote(RemoteControlClient),
}

enum CliEventStream {
    Local(ControlUpdateEventStream),
    Remote(mpsc::Receiver<Result<ControlUpdateEvent, ControlError>>),
}

struct RemoteControlClient {
    base_url: String,
    token: Zeroizing<String>,
    timeout: Duration,
    client: reqwest::blocking::Client,
}

enum ClientSetupError {
    Exit(ExitCode),
    Control(ControlError),
}

impl CliControlClient {
    fn status(&self) -> Result<ControlStatus, ControlError> {
        match self {
            Self::Local(client) => client.status(),
            Self::Remote(client) => client.request_json("GET", "/api/v1/control/status", ""),
        }
    }

    fn start(&self) -> Result<ControlStatus, ControlError> {
        self.control_command("POST", "/api/v1/control/start")
    }

    fn stop(&self) -> Result<ControlStatus, ControlError> {
        self.control_command("POST", "/api/v1/control/stop")
    }

    fn restart(&self) -> Result<ControlStatus, ControlError> {
        self.control_command("POST", "/api/v1/control/restart")
    }

    fn enable_management_web(&self) -> Result<ControlStatus, ControlError> {
        match self {
            Self::Local(client) => client.enable_management_web(),
            Self::Remote(client) => client.request_json("POST", "/api/v1/control/web/enable", ""),
        }
    }

    fn update_status(&self) -> Result<UpdateControlStatus, ControlError> {
        match self {
            Self::Local(client) => client.update_status(),
            Self::Remote(client) => client.request_json("GET", "/api/v1/control/updates", ""),
        }
    }

    fn diagnostics_bundle(&self) -> Result<DiagnosticsControlBundle, ControlError> {
        match self {
            Self::Local(client) => client.diagnostics_bundle(),
            Self::Remote(client) => {
                client.request_json("GET", "/api/v1/control/diagnostics/bundle", "")
            }
        }
    }

    fn gateway_projection(&self, projection: GatewayProjection) -> Result<Value, ControlError> {
        match self {
            Self::Local(client) => client.gateway_projection(projection),
            Self::Remote(client) => client.request_json(
                projection_method(projection),
                projection_control_path(projection),
                "",
            ),
        }
    }

    fn store_secret(
        &self,
        kind: ControlSecretKind,
        value: &str,
    ) -> Result<ControlSecretReceipt, ControlError> {
        match self {
            Self::Local(client) => client.store_secret(kind, value),
            Self::Remote(client) => client.request_json(
                "PUT",
                &format!("/api/v1/control/secrets/{}", kind.path_segment()),
                value,
            ),
        }
    }

    fn remove_secret(&self, kind: ControlSecretKind) -> Result<ControlSecretReceipt, ControlError> {
        match self {
            Self::Local(client) => client.remove_secret(kind),
            Self::Remote(client) => client.request_json(
                "DELETE",
                &format!("/api/v1/control/secrets/{}", kind.path_segment()),
                "",
            ),
        }
    }

    fn subscribe_update_events(&self) -> Result<CliEventStream, ControlError> {
        match self {
            Self::Local(client) => client.subscribe_update_events().map(CliEventStream::Local),
            Self::Remote(client) => client
                .subscribe("/api/v1/control/updates/events")
                .map(CliEventStream::Remote),
        }
    }

    fn subscribe_gateway_events(&self) -> Result<CliEventStream, ControlError> {
        match self {
            Self::Local(client) => client.subscribe_gateway_events().map(CliEventStream::Local),
            Self::Remote(client) => client
                .subscribe("/api/v1/control/events")
                .map(CliEventStream::Remote),
        }
    }

    fn control_command(&self, method: &str, path: &str) -> Result<ControlStatus, ControlError> {
        match (self, path) {
            (Self::Local(client), "/api/v1/control/start") => client.start(),
            (Self::Local(client), "/api/v1/control/stop") => client.stop(),
            (Self::Local(client), "/api/v1/control/restart") => client.restart(),
            (Self::Local(_), _) => Err(ControlError::CommandFailed),
            (Self::Remote(client), _) => client.request_json(method, path, ""),
        }
    }
}

impl CliEventStream {
    fn next_event(&mut self) -> Result<Option<ControlUpdateEvent>, ControlError> {
        match self {
            Self::Local(events) => events.next_event(),
            Self::Remote(events) => match events.recv_timeout(Duration::from_millis(250)) {
                Ok(event) => event.map(Some),
                Err(RecvTimeoutError::Timeout) => Err(ControlError::Timeout),
                Err(RecvTimeoutError::Disconnected) => Ok(None),
            },
        }
    }
}

impl RemoteControlClient {
    fn new(base_url: String, token: String, timeout: Duration) -> Result<Self, ControlError> {
        if timeout.is_zero() {
            return Err(ControlError::Timeout);
        }
        let token = Zeroizing::new(token);
        let parsed =
            reqwest::Url::parse(&base_url).map_err(|_| ControlError::UnsupportedEndpoint)?;
        if parsed.scheme() != "https"
            || parsed.host_str().is_none()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
            || !matches!(parsed.path(), "" | "/")
        {
            return Err(ControlError::UnsupportedEndpoint);
        }
        if token.len() < 32
            || token.len() > 4_096
            || token.bytes().any(|byte| byte.is_ascii_control())
        {
            return Err(ControlError::Authentication);
        }
        let client = reqwest::blocking::Client::builder()
            .connect_timeout(timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| ControlError::Io)?;
        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_owned(),
            token,
            timeout,
            client,
        })
    }

    fn request_json<T: serde::de::DeserializeOwned>(
        &self,
        method: &str,
        path: &str,
        body: &str,
    ) -> Result<T, ControlError> {
        let response = self.send(method, path, body, true)?;
        read_remote_json(response)
    }

    fn subscribe(
        &self,
        path: &str,
    ) -> Result<mpsc::Receiver<Result<ControlUpdateEvent, ControlError>>, ControlError> {
        let response = self.send("GET", path, "", false)?;
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        if !content_type.starts_with("text/event-stream") {
            return Err(ControlError::InvalidHttp);
        }
        let (sender, receiver) = mpsc::sync_channel(64);
        thread::spawn(move || read_remote_event_stream(response, sender));
        Ok(receiver)
    }

    fn send(
        &self,
        method: &str,
        path: &str,
        body: &str,
        bounded: bool,
    ) -> Result<reqwest::blocking::Response, ControlError> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| ControlError::Authentication)?
            .as_secs();
        let auth = sign_remote_control_request(
            self.token.as_str(),
            method,
            path,
            body.as_bytes(),
            timestamp,
        )
        .map_err(|_| ControlError::Authentication)?;
        let method = reqwest::Method::from_bytes(method.as_bytes())
            .map_err(|_| ControlError::InvalidHttp)?;
        let mut request = self
            .client
            .request(method, format!("{}{path}", self.base_url))
            .header(reqwest::header::AUTHORIZATION, auth.authorization)
            .header(REMOTE_CONTROL_TIMESTAMP_HEADER, auth.timestamp)
            .header(REMOTE_CONTROL_NONCE_HEADER, auth.nonce)
            .header(REMOTE_CONTROL_SCOPE_HEADER, auth.scope)
            .header(reqwest::header::ACCEPT, "application/json");
        if bounded {
            request = request.timeout(self.timeout);
        } else {
            request = request.header(reqwest::header::ACCEPT, "text/event-stream");
        }
        if !body.is_empty() {
            request = request.body(body.to_owned());
        }
        let response = request.send().map_err(map_remote_error)?;
        match response.status().as_u16() {
            200 | 202 => Ok(response),
            _ => Err(read_remote_control_error(response)),
        }
    }
}

fn read_remote_control_error(response: reqwest::blocking::Response) -> ControlError {
    const MAX_REMOTE_ERROR_BYTES: u64 = 8 * 1024;
    let status = response.status().as_u16();
    if response
        .content_length()
        .is_some_and(|length| length > MAX_REMOTE_ERROR_BYTES)
    {
        return ControlError::ResponseTooLarge;
    }
    let mut body = Vec::new();
    if response
        .take(MAX_REMOTE_ERROR_BYTES + 1)
        .read_to_end(&mut body)
        .is_err()
    {
        return ControlError::Io;
    }
    if body.len() as u64 > MAX_REMOTE_ERROR_BYTES {
        return ControlError::ResponseTooLarge;
    }
    remote_control_error(status, &body)
}

fn remote_control_error(status: u16, body: &[u8]) -> ControlError {
    let code = serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|value| value.get("code")?.as_str().map(str::to_owned));
    if let Some(error) = code.as_deref().and_then(ControlError::from_code) {
        return error;
    }
    match status {
        401 | 403 => ControlError::Authentication,
        408 | 504 => ControlError::Timeout,
        413 => ControlError::ResponseTooLarge,
        _ => ControlError::CommandFailed,
    }
}

fn read_remote_json<T: serde::de::DeserializeOwned>(
    response: reqwest::blocking::Response,
) -> Result<T, ControlError> {
    const MAX_RESPONSE_BYTES: u64 = 2 * 1024 * 1024;
    if response
        .content_length()
        .is_some_and(|length| length > MAX_RESPONSE_BYTES)
    {
        return Err(ControlError::ResponseTooLarge);
    }
    let mut bytes = Vec::new();
    response
        .take(MAX_RESPONSE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(map_io_error)?;
    if bytes.len() as u64 > MAX_RESPONSE_BYTES {
        return Err(ControlError::ResponseTooLarge);
    }
    serde_json::from_slice(&bytes).map_err(|_| ControlError::InvalidHttp)
}

fn read_remote_event_stream(
    response: reqwest::blocking::Response,
    sender: mpsc::SyncSender<Result<ControlUpdateEvent, ControlError>>,
) {
    const MAX_SSE_BLOCK_BYTES: usize = 64 * 1024;
    let mut reader = BufReader::new(response);
    let mut block = Vec::new();
    let mut block_bytes = 0_usize;
    loop {
        let mut line = Vec::new();
        match read_bounded_sse_line(&mut reader, &mut line) {
            Ok(0) if block.is_empty() => return,
            Ok(0) => {
                let _ = sender.send(Err(ControlError::InvalidHttp));
                return;
            }
            Ok(_) => {}
            Err(error) => {
                let _ = sender.send(Err(error));
                return;
            }
        }
        if line.last() == Some(&b'\n') {
            line.pop();
        }
        if line.last() == Some(&b'\r') {
            line.pop();
        }
        let line = match std::str::from_utf8(&line) {
            Ok(line) => line,
            Err(_) => {
                let _ = sender.send(Err(ControlError::InvalidHttp));
                return;
            }
        };
        if line.is_empty() {
            if let Some(event) = parse_remote_sse_block(&block) {
                if sender.send(event).is_err() {
                    return;
                }
            }
            block.clear();
            block_bytes = 0;
        } else if !line.starts_with(':') {
            block_bytes = match block_bytes.checked_add(line.len() + 1) {
                Some(length) if length <= MAX_SSE_BLOCK_BYTES => length,
                _ => {
                    let _ = sender.send(Err(ControlError::ResponseTooLarge));
                    return;
                }
            };
            block.push(line.to_owned());
        }
    }
}

fn read_bounded_sse_line(
    reader: &mut impl BufRead,
    output: &mut Vec<u8>,
) -> Result<usize, ControlError> {
    const MAX_SSE_LINE_BYTES: usize = 64 * 1024;
    output.clear();
    loop {
        let available = reader.fill_buf().map_err(map_io_error)?;
        if available.is_empty() {
            return Ok(output.len());
        }
        let count = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |index| index + 1);
        if output.len().saturating_add(count) > MAX_SSE_LINE_BYTES {
            return Err(ControlError::ResponseTooLarge);
        }
        let ended = available.get(count.saturating_sub(1)) == Some(&b'\n');
        output.extend_from_slice(&available[..count]);
        reader.consume(count);
        if ended {
            return Ok(output.len());
        }
    }
}

fn parse_remote_sse_block(lines: &[String]) -> Option<Result<ControlUpdateEvent, ControlError>> {
    if lines.is_empty() {
        return None;
    }
    let mut id = None;
    let mut event = None;
    let mut data = None;
    for line in lines {
        let Some((field, value)) = line.split_once(':') else {
            return Some(Err(ControlError::InvalidHttp));
        };
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "id" if id.is_none() => id = Some(value.to_owned()),
            "event" if event.is_none() => event = Some(value.to_owned()),
            "data" if data.is_none() => data = Some(value.as_bytes().to_vec()),
            _ => return Some(Err(ControlError::InvalidHttp)),
        }
    }
    match (id, event, data) {
        (Some(id), Some(event), Some(data))
            if is_safe_remote_sse_token(&id)
                && is_safe_remote_sse_token(&event)
                && !data.contains(&b'\r')
                && !data.contains(&b'\n') =>
        {
            Some(Ok(ControlUpdateEvent { id, event, data }))
        }
        _ => Some(Err(ControlError::InvalidHttp)),
    }
}

fn is_safe_remote_sse_token(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
}

fn map_remote_error(error: reqwest::Error) -> ControlError {
    if error.is_timeout() {
        ControlError::Timeout
    } else {
        ControlError::Io
    }
}

fn map_io_error(error: std::io::Error) -> ControlError {
    if matches!(
        error.kind(),
        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
    ) {
        ControlError::Timeout
    } else {
        ControlError::Io
    }
}

const fn projection_method(projection: GatewayProjection) -> &'static str {
    match projection {
        GatewayProjection::DatabaseIntegrity | GatewayProjection::Backup => "POST",
        _ => "GET",
    }
}

const fn projection_control_path(projection: GatewayProjection) -> &'static str {
    match projection {
        GatewayProjection::Meshtastic => "/api/v1/control/gateway/meshtastic",
        GatewayProjection::Nodes => "/api/v1/control/gateway/nodes",
        GatewayProjection::Positions => "/api/v1/control/gateway/positions",
        GatewayProjection::Aprs => "/api/v1/control/gateway/aprs",
        GatewayProjection::CallMesh => "/api/v1/control/gateway/callmesh",
        GatewayProjection::Proxy => "/api/v1/control/gateway/proxy",
        GatewayProjection::RecentEvents => "/api/v1/control/events/recent",
        GatewayProjection::DatabaseIntegrity => "/api/v1/control/database/integrity-check",
        GatewayProjection::Backup => "/api/v1/control/backups",
    }
}

fn main() -> ProcessExitCode {
    let cli = Cli::parse();
    match parse_endpoint(&cli.endpoint) {
        Ok(_) => run(cli),
        Err(code) => {
            eprintln!("CLI_ENDPOINT_INVALID");
            ProcessExitCode::from(code.as_u8())
        }
    }
}

fn run(cli: Cli) -> ProcessExitCode {
    let Cli {
        json,
        quiet,
        no_color,
        timeout,
        endpoint,
        command,
    } = cli;
    let style = OutputStyle::new(no_color);
    if matches!(&command, Command::Version) {
        if json {
            println!(r#"{{"version":"{}"}}"#, env!("CARGO_PKG_VERSION"));
        } else if !quiet {
            println!(
                "{} {}",
                style.heading("cmclient"),
                env!("CARGO_PKG_VERSION")
            );
        }
        return ProcessExitCode::SUCCESS;
    }

    let control_timeout = if command_follows_events(&command) {
        Duration::from_secs(timeout.min(1))
    } else {
        Duration::from_secs(timeout)
    };
    let client = match control_client(&endpoint, control_timeout) {
        Ok(client) => client,
        Err(ClientSetupError::Exit(code)) => return ProcessExitCode::from(code.as_u8()),
        Err(ClientSetupError::Control(error)) => return control_error_exit(error),
    };
    match command {
        Command::Status => print_control_result(client.status(), json, quiet, style),
        Command::Start => print_control_result(client.start(), json, quiet, style),
        Command::Stop => print_control_result(client.stop(), json, quiet, style),
        Command::Restart => print_control_result(client.restart(), json, quiet, style),
        Command::Logs { follow } => {
            events_command(&client, follow, EventOutput::Logs, json, quiet, style)
        }
        Command::Events { follow } => {
            events_command(&client, follow, EventOutput::Events, json, quiet, style)
        }
        Command::Doctor => doctor(&client, json, quiet, style),
        Command::Web => web(&client, json, quiet, style),
        Command::Meshtastic => print_gateway_projection(
            client.gateway_projection(GatewayProjection::Meshtastic),
            "meshtastic",
            json,
            quiet,
            style,
        ),
        Command::Nodes => print_gateway_projection(
            client.gateway_projection(GatewayProjection::Nodes),
            "nodes",
            json,
            quiet,
            style,
        ),
        Command::Positions => print_gateway_projection(
            client.gateway_projection(GatewayProjection::Positions),
            "positions",
            json,
            quiet,
            style,
        ),
        Command::Aprs => print_gateway_projection(
            client.gateway_projection(GatewayProjection::Aprs),
            "aprs",
            json,
            quiet,
            style,
        ),
        Command::Proxy => print_gateway_projection(
            client.gateway_projection(GatewayProjection::Proxy),
            "proxy",
            json,
            quiet,
            style,
        ),
        Command::Update { follow: true } => follow_update_events(&client, json, quiet, style),
        Command::Update { follow: false } => match client.update_status() {
            Ok(status) => print_update_status(&status, json, quiet, style),
            Err(error) => control_error_exit(error),
        },
        Command::Backup => print_job_submission(
            client.gateway_projection(GatewayProjection::Backup),
            "backup",
            json,
            quiet,
            style,
        ),
        Command::Diagnostics => match client.diagnostics_bundle() {
            Ok(bundle) => print_diagnostics_bundle(&bundle, json, quiet, style),
            Err(error) => control_error_exit(error),
        },
        Command::Secret { command } => manage_secret(&client, command, json, quiet),
        Command::Database => print_job_submission(
            client.gateway_projection(GatewayProjection::DatabaseIntegrity),
            "database integrity check",
            json,
            quiet,
            style,
        ),
        Command::Version => ProcessExitCode::SUCCESS,
    }
}

#[derive(Clone, Copy)]
struct OutputStyle {
    color: bool,
}

impl OutputStyle {
    fn new(no_color: bool) -> Self {
        Self {
            color: !no_color && std::io::stdout().is_terminal(),
        }
    }

    fn heading(self, value: &str) -> String {
        if self.color {
            format!("\u{1b}[1;36m{value}\u{1b}[0m")
        } else {
            value.to_owned()
        }
    }

    fn healthy(self, value: &str) -> String {
        if self.color {
            format!("\u{1b}[32m{value}\u{1b}[0m")
        } else {
            value.to_owned()
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EventOutput {
    Logs,
    Events,
}

fn command_follows_events(command: &Command) -> bool {
    matches!(
        command,
        Command::Logs { follow: true }
            | Command::Events { follow: true }
            | Command::Update { follow: true }
    )
}

fn manage_secret(
    client: &CliControlClient,
    command: SecretCommand,
    json: bool,
    quiet: bool,
) -> ProcessExitCode {
    let (kind, result) = match command {
        SecretCommand::Set { kind } => {
            let kind = match parse_secret_kind(&kind) {
                Some(kind) => kind,
                None => {
                    eprintln!("CLI_SECRET_KIND_INVALID");
                    return ProcessExitCode::from(ExitCode::Validation.as_u8());
                }
            };
            let mut value = match read_secret_from_standard_input() {
                Some(value) => value,
                None => {
                    eprintln!("CLI_SECRET_INPUT_INVALID");
                    return ProcessExitCode::from(ExitCode::Validation.as_u8());
                }
            };
            let result = client.store_secret(kind, &value);
            value.zeroize();
            (kind, result)
        }
        SecretCommand::Remove { kind } => {
            let kind = match parse_secret_kind(&kind) {
                Some(kind) => kind,
                None => {
                    eprintln!("CLI_SECRET_KIND_INVALID");
                    return ProcessExitCode::from(ExitCode::Validation.as_u8());
                }
            };
            (kind, client.remove_secret(kind))
        }
    };
    match result {
        Ok(receipt) => {
            if json {
                println!(
                    r#"{{"kind":"{}","stored":{}}}"#,
                    kind.path_segment(),
                    receipt.stored
                );
            } else if !quiet {
                println!(
                    "secret {}",
                    if receipt.stored {
                        "stored"
                    } else {
                        "not_found"
                    }
                );
            }
            ProcessExitCode::SUCCESS
        }
        Err(error) => control_error_exit(error),
    }
}

fn parse_secret_kind(value: &str) -> Option<ControlSecretKind> {
    match value {
        "callmesh-api-key" => Some(ControlSecretKind::CallMeshApiKey),
        "aprs-passcode" => Some(ControlSecretKind::AprsPasscode),
        "management-admin-token" => Some(ControlSecretKind::ManagementAdminToken),
        _ => None,
    }
}

fn read_secret_from_standard_input() -> Option<String> {
    const MAX_SECRET_INPUT_BYTES: u64 = 4_099;
    let mut value = String::new();
    std::io::stdin()
        .take(MAX_SECRET_INPUT_BYTES)
        .read_to_string(&mut value)
        .ok()?;
    if value.len() > 4_098 {
        return None;
    }
    normalize_secret_input(value)
}

fn normalize_secret_input(mut value: String) -> Option<String> {
    while value.ends_with('\n') || value.ends_with('\r') {
        value.pop();
    }
    (!value.is_empty()
        && value.len() <= 4_096
        && !value.bytes().any(|byte| byte.is_ascii_control()))
    .then_some(value)
}

fn print_control_result(
    result: Result<ControlStatus, ControlError>,
    json: bool,
    quiet: bool,
    style: OutputStyle,
) -> ProcessExitCode {
    match result {
        Ok(status) => print_status(&status, json, quiet, style),
        Err(error) => control_error_exit(error),
    }
}

fn control_client(value: &str, timeout: Duration) -> Result<CliControlClient, ClientSetupError> {
    match parse_endpoint(value).map_err(ClientSetupError::Exit)? {
        cmclient_cli_client::ControlEndpointSpec::Local => {
            let config =
                AgentConfig::load().map_err(|_| ClientSetupError::Exit(ExitCode::Validation))?;
            ControlClient::new_with_timeout(default_local_endpoint(&config.paths.data_dir), timeout)
                .map(CliControlClient::Local)
                .map_err(ClientSetupError::Control)
        }
        cmclient_cli_client::ControlEndpointSpec::UnixSocket(path) => {
            ControlClient::new_with_timeout(ControlEndpoint::unix(path), timeout)
                .map(CliControlClient::Local)
                .map_err(ClientSetupError::Control)
        }
        cmclient_cli_client::ControlEndpointSpec::NamedPipe(name) => {
            ControlClient::new_with_timeout(ControlEndpoint::named_pipe(name), timeout)
                .map(CliControlClient::Local)
                .map_err(ClientSetupError::Control)
        }
        cmclient_cli_client::ControlEndpointSpec::Https(url) => {
            let token = std::env::var("CMCLIENT_CONTROL_TOKEN")
                .map_err(|_| ClientSetupError::Control(ControlError::Authentication))?;
            RemoteControlClient::new(url, token, timeout)
                .map(CliControlClient::Remote)
                .map_err(ClientSetupError::Control)
        }
    }
}

fn events_command(
    client: &CliControlClient,
    follow: bool,
    output: EventOutput,
    json: bool,
    quiet: bool,
    style: OutputStyle,
) -> ProcessExitCode {
    if follow {
        return follow_gateway_events(client, output, json, quiet, style);
    }
    match client.gateway_projection(GatewayProjection::RecentEvents) {
        Ok(value) => print_recent_events(&value, output, json, quiet, style),
        Err(error) => control_error_exit(error),
    }
}

fn print_recent_events(
    value: &Value,
    output: EventOutput,
    json: bool,
    quiet: bool,
    style: OutputStyle,
) -> ProcessExitCode {
    let Some(items) = value.get("items").and_then(Value::as_array) else {
        eprintln!("CLI_EVENT_PROJECTION_INVALID");
        return ProcessExitCode::from(ExitCode::OperationFailed.as_u8());
    };
    let items = match items
        .iter()
        .map(|event| event_matches_output(event, output).map(|visible| (event, visible)))
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(items) => items
            .into_iter()
            .filter_map(|(event, visible)| visible.then_some(event))
            .collect::<Vec<_>>(),
        Err(()) => {
            eprintln!("CLI_EVENT_PROJECTION_INVALID");
            return ProcessExitCode::from(ExitCode::OperationFailed.as_u8());
        }
    };
    if json {
        let mut filtered = value.clone();
        filtered["items"] = Value::Array(items.iter().map(|event| (*event).clone()).collect());
        return print_json(&filtered);
    }
    if !quiet {
        if items.is_empty() {
            println!("{}: none", style.heading(event_output_name(output)));
        } else {
            for event in items {
                if print_domain_event(event, output, false, style).is_err() {
                    eprintln!("CLI_EVENT_PROJECTION_INVALID");
                    return ProcessExitCode::from(ExitCode::OperationFailed.as_u8());
                }
            }
        }
    }
    ProcessExitCode::SUCCESS
}

fn follow_gateway_events(
    client: &CliControlClient,
    output: EventOutput,
    json: bool,
    quiet: bool,
    style: OutputStyle,
) -> ProcessExitCode {
    let running = match shutdown_flag() {
        Ok(running) => running,
        Err(code) => {
            eprintln!("{code}");
            return ProcessExitCode::from(ExitCode::OperationFailed.as_u8());
        }
    };
    while running.load(Ordering::Relaxed) {
        let mut events = match client.subscribe_gateway_events() {
            Ok(events) => events,
            Err(
                ControlError::Io
                | ControlError::Timeout
                | ControlError::CommandFailed
                | ControlError::ResourceExhausted,
            ) => {
                reconnect_delay(&running);
                continue;
            }
            Err(error) => return control_error_exit(error),
        };
        while running.load(Ordering::Relaxed) {
            match events.next_event() {
                Ok(Some(event)) if event.event == "gateway.heartbeat" => {}
                Ok(Some(event)) => {
                    let value: Value = match serde_json::from_slice(&event.data) {
                        Ok(value) => value,
                        Err(_) => {
                            eprintln!("CLI_EVENT_PROJECTION_INVALID");
                            return ProcessExitCode::from(ExitCode::OperationFailed.as_u8());
                        }
                    };
                    let visible = match event_matches_output(&value, output) {
                        Ok(visible) => visible,
                        Err(()) => {
                            eprintln!("CLI_EVENT_PROJECTION_INVALID");
                            return ProcessExitCode::from(ExitCode::OperationFailed.as_u8());
                        }
                    };
                    if visible && !quiet && print_domain_event(&value, output, json, style).is_err()
                    {
                        eprintln!("CLI_EVENT_PROJECTION_INVALID");
                        return ProcessExitCode::from(ExitCode::OperationFailed.as_u8());
                    }
                }
                Ok(None)
                | Err(
                    ControlError::Io
                    | ControlError::CommandFailed
                    | ControlError::ResourceExhausted,
                ) => break,
                Err(ControlError::Timeout) => continue,
                Err(error) => return control_error_exit(error),
            }
        }
        reconnect_delay(&running);
    }
    ProcessExitCode::SUCCESS
}

fn event_matches_output(value: &Value, output: EventOutput) -> Result<bool, ()> {
    let event_type = value.get("type").and_then(Value::as_str).ok_or(())?;
    Ok(output == EventOutput::Events || event_type == "log.entry")
}

fn shutdown_flag() -> Result<Arc<AtomicBool>, &'static str> {
    let running = Arc::new(AtomicBool::new(true));
    let signal = Arc::clone(&running);
    ctrlc::set_handler(move || signal.store(false, Ordering::Relaxed))
        .map_err(|_| "CLI_SIGNAL_HANDLER_FAILED")?;
    Ok(running)
}

fn reconnect_delay(running: &AtomicBool) {
    for _ in 0..5 {
        if !running.load(Ordering::Relaxed) {
            return;
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn print_domain_event(
    value: &Value,
    output: EventOutput,
    json: bool,
    style: OutputStyle,
) -> Result<(), ()> {
    if json {
        println!("{}", serde_json::to_string(value).map_err(|_| ())?);
        return Ok(());
    }
    let event_type = value.get("type").and_then(Value::as_str).ok_or(())?;
    let occurred_at = value.get("occurredAt").and_then(Value::as_str).ok_or(())?;
    match output {
        EventOutput::Events => {
            let event_id = value.get("eventId").and_then(Value::as_str).ok_or(())?;
            let source = value.get("source").and_then(Value::as_str).ok_or(())?;
            println!(
                "{occurred_at} {} source={source} id={event_id}",
                style.heading(event_type)
            );
        }
        EventOutput::Logs => {
            let payload = value.get("payload").ok_or(())?;
            println!(
                "{occurred_at} {} {}",
                style.heading(event_type),
                serde_json::to_string(payload).map_err(|_| ())?
            );
        }
    }
    Ok(())
}

const fn event_output_name(output: EventOutput) -> &'static str {
    match output {
        EventOutput::Logs => "logs",
        EventOutput::Events => "events",
    }
}

fn print_gateway_projection(
    result: Result<Value, ControlError>,
    name: &str,
    json: bool,
    quiet: bool,
    style: OutputStyle,
) -> ProcessExitCode {
    let value = match result {
        Ok(value) => value,
        Err(error) => return control_error_exit(error),
    };
    if json {
        return print_json(&value);
    }
    if !quiet {
        println!("{}: {}", style.heading(name), projection_summary(&value));
    }
    ProcessExitCode::SUCCESS
}

fn projection_summary(value: &Value) -> String {
    if let Some(items) = value.get("items").and_then(Value::as_array) {
        return format!("{} item(s)", items.len());
    }
    if let Some(configured) = value.get("configured").and_then(Value::as_bool) {
        let connection = value
            .pointer("/connection/status")
            .and_then(Value::as_str)
            .unwrap_or("disconnected");
        return format!("configured={configured}; connection={connection}");
    }
    serde_json::to_string(value).unwrap_or_else(|_| String::from("unavailable"))
}

fn print_job_submission(
    result: Result<Value, ControlError>,
    name: &str,
    json: bool,
    quiet: bool,
    style: OutputStyle,
) -> ProcessExitCode {
    let value = match result {
        Ok(value) => value,
        Err(error) => return control_error_exit(error),
    };
    let Some(job_id) = value.get("jobId").and_then(Value::as_str) else {
        eprintln!("CLI_JOB_RESPONSE_INVALID");
        return ProcessExitCode::from(ExitCode::OperationFailed.as_u8());
    };
    if json {
        return print_json(&value);
    }
    if !quiet {
        let reused = value
            .get("reused")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        println!(
            "{}: job {job_id}{}",
            style.heading(name),
            if reused { " (reused)" } else { "" }
        );
    }
    ProcessExitCode::SUCCESS
}

fn doctor(
    client: &CliControlClient,
    json_output: bool,
    quiet: bool,
    style: OutputStyle,
) -> ProcessExitCode {
    let status = match client.status() {
        Ok(status) => status,
        Err(error) => return control_error_exit(error),
    };
    let diagnostics = match client.diagnostics_bundle() {
        Ok(diagnostics) => diagnostics,
        Err(error) => return control_error_exit(error),
    };
    let degraded = doctor_is_degraded(&status, &diagnostics);
    if json_output {
        let value = json!({
            "schemaVersion": 1,
            "degraded": degraded,
            "status": status,
            "diagnostics": diagnostics,
        });
        if print_json(&value) != ProcessExitCode::SUCCESS {
            return ProcessExitCode::from(ExitCode::OperationFailed.as_u8());
        }
    } else if !quiet {
        let state = if degraded {
            String::from("degraded")
        } else {
            style.healthy("healthy")
        };
        println!("{}: {}", style.heading("doctor"), state);
        if let Some(code) = status
            .latest_error_code
            .as_deref()
            .or(diagnostics.latest_error_code.as_deref())
            .or(diagnostics.update_error_code.as_deref())
        {
            println!("error: {code}");
        }
    }
    if degraded {
        ProcessExitCode::from(ExitCode::PartialOrDegraded.as_u8())
    } else {
        ProcessExitCode::SUCCESS
    }
}

fn doctor_is_degraded(status: &ControlStatus, diagnostics: &DiagnosticsControlBundle) -> bool {
    status.gateway != GatewayControlStatus::Running
        || diagnostics.gateway != GatewayControlStatus::Running
        || status.latest_error_code.is_some()
        || diagnostics.latest_error_code.is_some()
        || diagnostics.update_error_code.is_some()
}

fn web(client: &CliControlClient, json: bool, quiet: bool, style: OutputStyle) -> ProcessExitCode {
    let status = match client.enable_management_web() {
        Ok(status) => status,
        Err(error) => return control_error_exit(error),
    };
    if json {
        return print_json_serializable(&status);
    }
    if !quiet {
        println!(
            "{}: {}",
            style.heading("web"),
            status
                .management_web_url
                .as_deref()
                .unwrap_or("unavailable")
        );
    }
    ProcessExitCode::SUCCESS
}

fn print_json(value: &Value) -> ProcessExitCode {
    match serde_json::to_string(value) {
        Ok(serialized) => {
            println!("{serialized}");
            ProcessExitCode::SUCCESS
        }
        Err(_) => ProcessExitCode::from(ExitCode::OperationFailed.as_u8()),
    }
}

fn print_json_serializable(value: &impl serde::Serialize) -> ProcessExitCode {
    match serde_json::to_string(value) {
        Ok(serialized) => {
            println!("{serialized}");
            ProcessExitCode::SUCCESS
        }
        Err(_) => ProcessExitCode::from(ExitCode::OperationFailed.as_u8()),
    }
}

fn print_status(
    status: &ControlStatus,
    json: bool,
    quiet: bool,
    style: OutputStyle,
) -> ProcessExitCode {
    if json {
        return print_json_serializable(status);
    } else if !quiet {
        println!(
            "{}: {}; gateway: {:?}",
            style.heading("agent"),
            status.agent,
            status.gateway
        );
    }
    ProcessExitCode::SUCCESS
}

fn print_update_status(
    status: &UpdateControlStatus,
    json: bool,
    quiet: bool,
    style: OutputStyle,
) -> ProcessExitCode {
    if json {
        return print_json_serializable(status);
    } else if !quiet {
        println!("{}", style.heading(&update_summary(status)));
    }
    ProcessExitCode::SUCCESS
}

fn print_diagnostics_bundle(
    bundle: &DiagnosticsControlBundle,
    json: bool,
    quiet: bool,
    style: OutputStyle,
) -> ProcessExitCode {
    if json {
        return print_json_serializable(bundle);
    } else if !quiet {
        println!(
            "{}: gateway: {:?}; management_web: {:?}",
            style.heading("diagnostics"),
            bundle.gateway,
            bundle.management_web
        );
    }
    ProcessExitCode::SUCCESS
}

fn update_summary(status: &UpdateControlStatus) -> String {
    let Some(job) = &status.job else {
        return String::from("update: idle");
    };
    let transfer = job.bytes_downloaded.map_or_else(
        || String::from("--"),
        |downloaded| {
            let total = job
                .bytes_total
                .map_or_else(|| String::from("--"), format_bytes);
            format!("{} / {total}", format_bytes(downloaded))
        },
    );
    let speed = job.bytes_per_second.map_or_else(
        || String::from("--"),
        |bytes| format!("{}/s", format_bytes(bytes)),
    );
    let error = job
        .error_code
        .as_ref()
        .map_or_else(String::new, |code| format!("; error: {code}"));
    format!(
        "update: {}; phase: {}; transfer: {transfer}; speed: {speed}{error}",
        job.id, job.phase
    )
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 4] = ["B", "KiB", "MiB", "GiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} {}", UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

fn follow_update_events(
    client: &CliControlClient,
    json: bool,
    quiet: bool,
    style: OutputStyle,
) -> ProcessExitCode {
    let running = match shutdown_flag() {
        Ok(running) => running,
        Err(code) => {
            eprintln!("{code}");
            return ProcessExitCode::from(ExitCode::OperationFailed.as_u8());
        }
    };
    while running.load(Ordering::Relaxed) {
        let mut events = match client.subscribe_update_events() {
            Ok(events) => events,
            Err(
                ControlError::Io
                | ControlError::Timeout
                | ControlError::CommandFailed
                | ControlError::ResourceExhausted,
            ) => {
                reconnect_delay(&running);
                continue;
            }
            Err(error) => return control_error_exit(error),
        };
        while running.load(Ordering::Relaxed) {
            match events.next_event() {
                Ok(Some(event)) => {
                    let status: UpdateControlStatus = match serde_json::from_slice(&event.data) {
                        Ok(status) => status,
                        Err(_) => {
                            eprintln!("CLI_UPDATE_EVENT_INVALID");
                            return ProcessExitCode::from(ExitCode::OperationFailed.as_u8());
                        }
                    };
                    let exit = print_update_status(&status, json, quiet, style);
                    if exit != ProcessExitCode::SUCCESS {
                        return exit;
                    }
                }
                Ok(None)
                | Err(
                    ControlError::Io
                    | ControlError::CommandFailed
                    | ControlError::ResourceExhausted,
                ) => break,
                Err(ControlError::Timeout) => continue,
                Err(error) => return control_error_exit(error),
            }
        }
        reconnect_delay(&running);
    }
    ProcessExitCode::SUCCESS
}

fn control_error_exit(error: ControlError) -> ProcessExitCode {
    eprintln!("{}", error.code());
    let code = match error {
        ControlError::Io
        | ControlError::UnsupportedEndpoint
        | ControlError::EndpointAlreadyInUse => ExitCode::Connection,
        ControlError::Timeout => ExitCode::Timeout,
        ControlError::Authentication => ExitCode::Authentication,
        ControlError::CommandFailed
        | ControlError::ResourceExhausted
        | ControlError::SecretStoreUnavailable
        | ControlError::SecretValueInvalid => ExitCode::OperationFailed,
        ControlError::InvalidHttp | ControlError::ResponseTooLarge => ExitCode::OperationFailed,
    };
    ProcessExitCode::from(code.as_u8())
}

#[cfg(test)]
mod tests {
    use super::{
        Cli, EventOutput, RemoteControlClient, control_error_exit, doctor_is_degraded,
        event_matches_output, normalize_secret_input, parse_remote_sse_block, parse_secret_kind,
        projection_control_path, read_bounded_sse_line, remote_control_error, update_summary,
    };
    use clap::CommandFactory;
    use cmclient_cli_client::ExitCode;
    use cmclient_control_api::{
        ControlError, ControlStatus, DiagnosticsControlBundle, GatewayControlStatus,
        GatewayProjection, ManagementWebControlStatus, UpdateControlJob, UpdateControlStatus,
    };
    use serde_json::json;
    use std::{io::Cursor, process::ExitCode as ProcessExitCode, time::Duration};

    #[test]
    fn renders_an_idle_update_status_without_a_gateway_projection() {
        assert_eq!(
            update_summary(&UpdateControlStatus {
                schema_version: 1,
                job: None,
            }),
            "update: idle"
        );
    }

    #[test]
    fn renders_phase_transfer_speed_and_stable_failure_code() {
        assert_eq!(
            update_summary(&UpdateControlStatus {
                schema_version: 1,
                job: Some(UpdateControlJob {
                    id: String::from("update-1"),
                    phase: String::from("rolling_back"),
                    updated_at: String::from("2026-07-18T06:00:00.000Z"),
                    error_code: Some(String::from("UPDATE_HEALTH_CHECK_FAILED")),
                    bytes_downloaded: Some(524_288),
                    bytes_total: Some(1_048_576),
                    bytes_per_second: Some(262_144),
                    recent_log_codes: Vec::new(),
                }),
            }),
            "update: update-1; phase: rolling_back; transfer: 512.0 KiB / 1.0 MiB; speed: 256.0 KiB/s; error: UPDATE_HEALTH_CHECK_FAILED"
        );
    }

    #[test]
    fn accepts_only_named_secret_kinds_and_single_line_standard_input() {
        assert!(parse_secret_kind("callmesh-api-key").is_some());
        assert!(parse_secret_kind("private-signing-key").is_none());
        assert_eq!(
            normalize_secret_input(String::from("secret-from-stdin\r\n")),
            Some(String::from("secret-from-stdin"))
        );
        assert!(normalize_secret_input(String::from("two\nlines")).is_none());
        assert!(normalize_secret_input("a".repeat(4_096)).is_some());
        assert!(normalize_secret_input("a".repeat(4_097)).is_none());
    }

    #[test]
    fn remote_endpoint_requires_https_root_and_a_nontrivial_token() {
        let token = String::from("0123456789abcdef0123456789abcdef");
        assert!(
            RemoteControlClient::new(
                String::from("https://cmclient.example"),
                token.clone(),
                Duration::from_secs(1),
            )
            .is_ok()
        );
        for endpoint in [
            "http://cmclient.example",
            "https://user@cmclient.example",
            "https://cmclient.example/control",
            "https://cmclient.example?token=forbidden",
        ] {
            assert!(
                RemoteControlClient::new(
                    String::from(endpoint),
                    token.clone(),
                    Duration::from_secs(1),
                )
                .is_err()
            );
        }
        assert!(matches!(
            RemoteControlClient::new(
                String::from("https://cmclient.example"),
                String::from("short"),
                Duration::from_secs(1),
            ),
            Err(ControlError::Authentication)
        ));
        assert!(matches!(
            RemoteControlClient::new(
                String::from("https://cmclient.example"),
                token,
                Duration::ZERO,
            ),
            Err(ControlError::Timeout)
        ));
        let help = Cli::command().render_long_help().to_string();
        assert!(!help.to_ascii_lowercase().contains("token"));
    }

    #[test]
    fn remote_sse_and_callmesh_projection_use_stable_control_contracts() {
        let event = parse_remote_sse_block(&[
            String::from("id: event-1"),
            String::from("event: mesh.position.accepted"),
            String::from("data: {\"schemaVersion\":1}"),
        ])
        .expect("event block should produce a result")
        .expect("event block should be valid");
        assert_eq!(event.id, "event-1");
        assert_eq!(event.event, "mesh.position.accepted");
        assert_eq!(
            projection_control_path(GatewayProjection::CallMesh),
            "/api/v1/control/gateway/callmesh"
        );
        assert_eq!(
            remote_control_error(503, br#"{"code":"AGENT_SECRET_STORE_UNAVAILABLE"}"#),
            ControlError::SecretStoreUnavailable
        );
        assert_eq!(
            remote_control_error(400, br#"{"code":"AGENT_SECRET_VALUE_INVALID"}"#),
            ControlError::SecretValueInvalid
        );
        assert_eq!(
            remote_control_error(401, br#"{"code":"UNKNOWN"}"#),
            ControlError::Authentication
        );
        assert_eq!(
            parse_remote_sse_block(&[
                String::from("id: event-1"),
                String::from("id: event-2"),
                String::from("event: log.entry"),
                String::from("data: {}"),
            ]),
            Some(Err(ControlError::InvalidHttp))
        );
    }

    #[test]
    fn bounds_remote_sse_lines_and_filters_log_output() {
        let mut oversized = Cursor::new(vec![b'a'; 64 * 1024 + 1]);
        assert_eq!(
            read_bounded_sse_line(&mut oversized, &mut Vec::new()),
            Err(ControlError::ResponseTooLarge)
        );
        let log = json!({ "type": "log.entry" });
        let domain = json!({ "type": "position.accepted" });
        assert_eq!(event_matches_output(&log, EventOutput::Logs), Ok(true));
        assert_eq!(event_matches_output(&domain, EventOutput::Logs), Ok(false));
        assert_eq!(event_matches_output(&domain, EventOutput::Events), Ok(true));
        assert!(event_matches_output(&json!({}), EventOutput::Events).is_err());
    }

    #[test]
    fn maps_timeout_and_stopped_gateway_to_stable_cli_exit_codes() {
        assert_eq!(
            control_error_exit(ControlError::Timeout),
            ProcessExitCode::from(ExitCode::Timeout.as_u8())
        );
        assert_eq!(
            control_error_exit(ControlError::Authentication),
            ProcessExitCode::from(ExitCode::Authentication.as_u8())
        );
        let status = ControlStatus {
            schema_version: 2,
            agent: String::from("running"),
            agent_version: String::from("2.0.0-test"),
            gateway: GatewayControlStatus::Stopped,
            management_web: ManagementWebControlStatus::Running,
            management_web_url: Some(String::from("http://127.0.0.1:7080")),
            uptime_seconds: 1,
            latest_error_code: None,
        };
        let mut diagnostics = DiagnosticsControlBundle {
            schema_version: 1,
            agent_version: String::from("2.0.0-test"),
            gateway: GatewayControlStatus::Stopped,
            management_web: ManagementWebControlStatus::Running,
            latest_error_code: None,
            update_error_code: None,
            update_log_codes: Vec::new(),
        };
        assert!(doctor_is_degraded(&status, &diagnostics));

        let mut running = status;
        running.gateway = GatewayControlStatus::Running;
        diagnostics.gateway = GatewayControlStatus::Running;
        assert!(!doctor_is_degraded(&running, &diagnostics));
    }
}
