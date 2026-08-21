use clap::{Parser, Subcommand};
use cmclient_agent_core::AgentConfig;
use cmclient_cli_client::{ExitCode, parse_endpoint};
use cmclient_control_api::{
    ControlClient, ControlEndpoint, ControlError, ControlSecretKind, ControlStatus,
    DiagnosticsControlBundle, GatewayControlStatus, GatewayProjection, InternalComponent,
    UpdateControlStatus, compiled_component_identity, default_local_endpoint,
};
use serde_json::{Value, json};
use std::{
    io::{IsTerminal, Read},
    process::{Command as ProcessCommand, ExitCode as ProcessExitCode, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};
use zeroize::Zeroize;

#[derive(Debug, Parser)]
#[command(name = "cmclient", version, about = "CMClient Agent control client")]
struct Cli {
    #[arg(long, global = true)]
    json: bool,
    #[arg(long, global = true)]
    quiet: bool,
    #[arg(long, global = true)]
    no_color: bool,
    #[arg(long, global = true)]
    background: bool,
    #[arg(long, global = true, default_value_t = 30, value_parser = clap::value_parser!(u64).range(1..=86_400))]
    timeout: u64,
    #[arg(long, global = true, default_value = "local")]
    endpoint: String,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    Status,
    Start,
    Stop,
    Restart,
    Shutdown,
    Reset {
        #[arg(long)]
        confirm: String,
    },
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
    Cmcloud {
        #[command(subcommand)]
        command: CmCloudCommand,
    },
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

#[derive(Debug, Subcommand)]
enum CmCloudCommand {
    Status,
    Enroll {
        #[arg(value_name = "PAIRING_CODE")]
        pairing_code: String,
    },
}

impl Drop for CmCloudCommand {
    fn drop(&mut self) {
        if let Self::Enroll { pairing_code } = self {
            pairing_code.zeroize();
        }
    }
}

enum ClientSetupError {
    Exit(ExitCode),
    Control(ControlError),
}

fn main() -> ProcessExitCode {
    let cli = Cli::parse();
    match parse_endpoint(&cli.endpoint) {
        Ok(_) => match launcher_mode(cli.command.as_ref(), cli.background) {
            Ok(LauncherMode::Command) => run(cli),
            Ok(mode) => run_launcher(cli, mode),
            Err(code) => {
                eprintln!("{code}");
                ProcessExitCode::from(ExitCode::Validation.as_u8())
            }
        },
        Err(code) => {
            eprintln!("CLI_ENDPOINT_INVALID");
            ProcessExitCode::from(code.as_u8())
        }
    }
}

fn run(cli: Cli) -> ProcessExitCode {
    let Some(command) = cli.command else {
        eprintln!("CLI_COMMAND_REQUIRED");
        return ProcessExitCode::from(ExitCode::Validation.as_u8());
    };
    let Cli {
        json,
        quiet,
        no_color,
        background: _,
        timeout,
        endpoint,
        command: _,
    } = cli;
    let style = OutputStyle::new(no_color);
    if matches!(&command, Command::Version) {
        let identity = match compiled_component_identity(InternalComponent::CommandMode) {
            Ok(identity) => identity,
            Err(_) => {
                eprintln!("BUILD_IDENTITY_INVALID");
                return ProcessExitCode::from(ExitCode::OperationFailed.as_u8());
            }
        };
        if json {
            match serde_json::to_string(&identity) {
                Ok(value) => println!("{value}"),
                Err(_) => {
                    return ProcessExitCode::from(ExitCode::OperationFailed.as_u8());
                }
            }
        } else if !quiet {
            println!(
                "{} {} ({}, {}/{}, {}/{})",
                style.heading(&identity.identity.product),
                identity.identity.version,
                identity.identity.channel.as_str(),
                identity.identity.target.os.as_str(),
                identity.identity.target.architecture.as_str(),
                identity.identity.target.profile.as_str(),
                identity.identity.target.package_profile.as_str(),
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
        Command::Shutdown => print_control_result(client.shutdown_agent(), json, quiet, style),
        Command::Reset { confirm } => {
            if confirm != "operational-reset" {
                if json {
                    let _ = print_json(&json!({"code": "CLI_RESET_CONFIRMATION_INVALID"}));
                    return ProcessExitCode::from(ExitCode::Validation.as_u8());
                }
                eprintln!("CLI_RESET_CONFIRMATION_INVALID");
                return ProcessExitCode::from(ExitCode::Validation.as_u8());
            }
            print_control_result(client.operational_reset(), json, quiet, style)
        }
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
        Command::Cmcloud { command } => cmcloud(&client, command, json, quiet, style),
        Command::Version => ProcessExitCode::SUCCESS,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LauncherMode {
    Command,
    Gui,
    Background,
}

fn launcher_mode(
    command: Option<&Command>,
    background: bool,
) -> Result<LauncherMode, &'static str> {
    if background && command.is_some() {
        return Err("CLI_BACKGROUND_COMMAND_CONFLICT");
    }
    if background {
        Ok(LauncherMode::Background)
    } else if command.is_some() {
        Ok(LauncherMode::Command)
    } else {
        Ok(LauncherMode::Gui)
    }
}

fn run_launcher(cli: Cli, mode: LauncherMode) -> ProcessExitCode {
    let style = OutputStyle::new(cli.no_color);
    let timeout = Duration::from_secs(cli.timeout);
    let client = match ensure_agent_client(&cli.endpoint, timeout) {
        Ok(client) => client,
        Err(error) => return control_error_exit(error),
    };
    let result = match mode {
        LauncherMode::Background => client.status(),
        LauncherMode::Gui => client.open_desktop(),
        LauncherMode::Command => unreachable!(),
    };
    print_control_result(result, cli.json, cli.quiet, style)
}

fn ensure_agent_client(value: &str, timeout: Duration) -> Result<ControlClient, ControlError> {
    if !matches!(
        parse_endpoint(value),
        Ok(cmclient_cli_client::ControlEndpointSpec::Local)
    ) {
        return Err(ControlError::UnsupportedEndpoint);
    }
    let mut last_error = match control_client(value, timeout) {
        Ok(client) => return Ok(client),
        Err(ClientSetupError::Control(error)) => error,
        Err(ClientSetupError::Exit(_)) => return Err(ControlError::UnsupportedEndpoint),
    };
    let executable = std::env::current_exe()
        .ok()
        .and_then(|path| path.parent().map(|parent| parent.to_path_buf()))
        .map(|parent| {
            let name = if cfg!(target_os = "windows") {
                "cmclient-agent.exe"
            } else {
                "cmclient-agent"
            };
            parent.join(name)
        })
        .filter(|path| path.is_file())
        .ok_or(ControlError::CommandFailed)?;
    let mut command = ProcessCommand::new(executable);
    command
        .arg("--serve")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    command.spawn().map_err(|_| ControlError::CommandFailed)?;
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        match control_client(value, Duration::from_millis(250)) {
            Ok(client) => return Ok(client),
            Err(ClientSetupError::Control(error)) => last_error = error,
            Err(ClientSetupError::Exit(_)) => break,
        }
        thread::sleep(Duration::from_millis(50));
    }
    Err(last_error)
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
    client: &ControlClient,
    command: SecretCommand,
    json: bool,
    quiet: bool,
) -> ProcessExitCode {
    let (kind, result) = match command {
        SecretCommand::Set { kind } => {
            let kind = match parse_secret_kind_for_set(&kind) {
                Some(kind) => kind,
                None => {
                    eprintln!("{}", secret_kind_error(&kind));
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
                    eprintln!("{}", secret_kind_error(&kind));
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

fn parse_secret_kind_for_set(value: &str) -> Option<ControlSecretKind> {
    (!matches!(value, "aprs-passcode" | "management-admin-token"))
        .then(|| parse_secret_kind(value))?
}

fn secret_kind_error(value: &str) -> &'static str {
    if matches!(value, "aprs-passcode" | "management-admin-token") {
        "CLI_SECRET_KIND_DEPRECATED"
    } else {
        "CLI_SECRET_KIND_INVALID"
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

fn control_client(value: &str, timeout: Duration) -> Result<ControlClient, ClientSetupError> {
    match parse_endpoint(value).map_err(ClientSetupError::Exit)? {
        cmclient_cli_client::ControlEndpointSpec::Local => {
            let config =
                AgentConfig::load().map_err(|_| ClientSetupError::Exit(ExitCode::Validation))?;
            let endpoint = default_local_endpoint(config.paths.root_dir())
                .map_err(ClientSetupError::Control)?;
            ControlClient::new_with_timeout(endpoint, timeout).map_err(ClientSetupError::Control)
        }
        cmclient_cli_client::ControlEndpointSpec::UnixSocket(path) => {
            ControlClient::new_with_timeout(ControlEndpoint::unix(path), timeout)
                .map_err(ClientSetupError::Control)
        }
        cmclient_cli_client::ControlEndpointSpec::NamedPipe(name) => {
            ControlClient::new_with_timeout(ControlEndpoint::named_pipe(name), timeout)
                .map_err(ClientSetupError::Control)
        }
    }
}

fn events_command(
    client: &ControlClient,
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
    client: &ControlClient,
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
    client: &ControlClient,
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

fn web(client: &ControlClient, json: bool, quiet: bool, style: OutputStyle) -> ProcessExitCode {
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
    client: &ControlClient,
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

fn cmcloud(
    client: &ControlClient,
    command: CmCloudCommand,
    json_output: bool,
    quiet: bool,
    style: OutputStyle,
) -> ProcessExitCode {
    let result = match command {
        CmCloudCommand::Status => client.cmcloud_enrollment_status(),
        CmCloudCommand::Enroll { ref pairing_code } => {
            if !valid_cmcloud_pairing_code(pairing_code) {
                eprintln!("CMCLOUD_ENROLLMENT_REQUEST_INVALID");
                return ProcessExitCode::from(ExitCode::Validation.as_u8());
            }
            client.enroll_cmcloud(pairing_code)
        }
    };
    let value = match result {
        Ok(value) => value,
        Err(error) => return control_error_exit(error),
    };
    if json_output {
        return print_json(&value);
    }
    if !quiet {
        println!("{}: {}", style.heading("cmcloud"), cmcloud_summary(&value));
    }
    ProcessExitCode::SUCCESS
}

fn cmcloud_summary(value: &Value) -> String {
    let state = value
        .get("state")
        .and_then(Value::as_str)
        .unwrap_or("unavailable");
    let endpoint = value
        .get("endpoint")
        .and_then(Value::as_str)
        .unwrap_or("unconfigured");
    let generation = value
        .get("installationGeneration")
        .and_then(Value::as_u64)
        .map_or_else(|| "--".to_owned(), |value| value.to_string());
    let credential_version = value
        .get("credentialVersion")
        .and_then(Value::as_u64)
        .map_or_else(|| "--".to_owned(), |value| value.to_string());
    format!(
        "state={state}; endpoint={endpoint}; generation={generation}; credentialVersion={credential_version}"
    )
}

fn valid_cmcloud_pairing_code(value: &str) -> bool {
    value.len() >= 16
        && value.len() <= 512
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn control_error_exit(error: ControlError) -> ProcessExitCode {
    eprintln!("{}", error.code());
    let code = match error {
        ControlError::Io
        | ControlError::UnsupportedEndpoint
        | ControlError::EndpointAlreadyInUse => ExitCode::Connection,
        ControlError::Timeout => ExitCode::Timeout,
        ControlError::Authentication => ExitCode::Authentication,
        ControlError::SecretKindDeprecated => ExitCode::Validation,
        ControlError::CommandFailed
        | ControlError::ResourceExhausted
        | ControlError::SecretStoreUnavailable
        | ControlError::SecretValueInvalid
        | ControlError::Application(_) => ExitCode::OperationFailed,
        ControlError::InvalidEnvelope
        | ControlError::ProtocolVersionUnsupported
        | ControlError::ResponseTooLarge => ExitCode::OperationFailed,
    };
    ProcessExitCode::from(code.as_u8())
}

#[cfg(test)]
mod tests {
    use super::{
        Cli, Command, EventOutput, LauncherMode, control_error_exit, doctor_is_degraded,
        event_matches_output, launcher_mode, normalize_secret_input, parse_secret_kind,
        parse_secret_kind_for_set, run, secret_kind_error, update_summary,
    };
    use clap::{CommandFactory, Parser};
    use cmclient_cli_client::ExitCode;
    use cmclient_control_api::{
        ControlCommand, ControlEndpoint, ControlError, ControlHandler, ControlServer,
        ControlStatus, DiagnosticsControlBundle, GatewayControlStatus, InternalComponent,
        ManagementWebControlStatus, UpdateControlJob, UpdateControlStatus,
        compiled_component_identity, default_local_endpoint,
    };
    use serde_json::json;
    use std::{
        process::ExitCode as ProcessExitCode,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        },
        thread,
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    };

    struct ShutdownRecordingHandler {
        commands: Mutex<Vec<ControlCommand>>,
        response_sent: AtomicBool,
    }

    impl ShutdownRecordingHandler {
        fn status() -> ControlStatus {
            ControlStatus {
                schema_version: 3,
                agent: String::from("running"),
                identity: compiled_component_identity(InternalComponent::Agent).unwrap(),
                gateway: GatewayControlStatus::Running,
                management_web: ManagementWebControlStatus::Disabled,
                management_web_url: None,
                uptime_seconds: 1,
                latest_error_code: None,
            }
        }
    }

    impl ControlHandler for ShutdownRecordingHandler {
        fn handle(&self, command: ControlCommand) -> Result<ControlStatus, ControlError> {
            self.commands.lock().unwrap().push(command);
            Ok(Self::status())
        }

        fn prepare_command(&self, command: ControlCommand) -> Result<ControlStatus, ControlError> {
            self.commands.lock().unwrap().push(command);
            Ok(Self::status())
        }

        fn command_response_sent(&self, command: ControlCommand) {
            if command == ControlCommand::ShutdownAgent {
                self.response_sent.store(true, Ordering::Release);
            }
        }
    }

    #[test]
    fn parses_shutdown_with_global_json_output() {
        let cli = Cli::try_parse_from(["cmclient", "--json", "shutdown"])
            .expect("shutdown command should parse");

        assert!(cli.json);
        assert!(matches!(cli.command, Some(Command::Shutdown)));
    }

    #[test]
    fn classifies_public_launcher_modes_without_reinterpreting_commands() {
        assert_eq!(launcher_mode(None, false), Ok(LauncherMode::Gui));
        assert_eq!(launcher_mode(None, true), Ok(LauncherMode::Background));
        assert_eq!(
            launcher_mode(Some(&Command::Status), false),
            Ok(LauncherMode::Command)
        );
        assert_eq!(
            launcher_mode(Some(&Command::Status), true),
            Err("CLI_BACKGROUND_COMMAND_CONFLICT")
        );
    }

    #[test]
    fn public_launcher_arguments_parse_without_a_subcommand() {
        let gui = Cli::try_parse_from(["cmclient"]).expect("no-argument launcher should parse");
        assert_eq!(
            launcher_mode(gui.command.as_ref(), gui.background),
            Ok(LauncherMode::Gui)
        );

        let background = Cli::try_parse_from(["cmclient", "--background"])
            .expect("background launcher should parse");
        assert_eq!(
            launcher_mode(background.command.as_ref(), background.background),
            Ok(LauncherMode::Background)
        );
    }

    #[test]
    fn shutdown_command_dispatches_the_agent_shutdown_control_request() {
        let root = std::env::temp_dir().join(format!(
            "cmclient-cli-shutdown-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock should be after the Unix epoch")
                .as_nanos()
        ));
        std::fs::create_dir(&root).expect("test state root should create");
        let endpoint = default_local_endpoint(&root).expect("endpoint should derive");
        let endpoint_argument = match &endpoint {
            ControlEndpoint::UnixSocket(path) => format!("unix://{}", path.display()),
            ControlEndpoint::NamedPipe(name) => name.clone(),
        };
        let handler = Arc::new(ShutdownRecordingHandler {
            commands: Mutex::new(Vec::new()),
            response_sent: AtomicBool::new(false),
        });
        let server =
            ControlServer::bind(endpoint, handler.clone()).expect("control server should bind");
        let server_handler = handler.clone();
        let server_thread = thread::spawn(move || {
            while !server.poll_once().expect("server should poll") {}
            let deadline = Instant::now() + Duration::from_secs(2);
            while !server_handler.response_sent.load(Ordering::Acquire) {
                assert!(
                    Instant::now() < deadline,
                    "shutdown response was not acknowledged"
                );
                thread::sleep(Duration::from_millis(2));
            }
        });

        let exit = run(Cli {
            json: false,
            quiet: true,
            no_color: true,
            background: false,
            timeout: 2,
            endpoint: endpoint_argument,
            command: Some(Command::Shutdown),
        });

        assert_eq!(exit, ProcessExitCode::SUCCESS);
        server_thread
            .join()
            .expect("control server should finish after shutdown response");
        assert_eq!(
            *handler.commands.lock().unwrap(),
            vec![ControlCommand::ShutdownAgent]
        );
        std::fs::remove_dir_all(root).expect("test state root should remove");
    }

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
        assert!(parse_secret_kind("aprs-passcode").is_some());
        assert!(parse_secret_kind_for_set("aprs-passcode").is_none());
        assert!(parse_secret_kind("management-admin-token").is_some());
        assert!(parse_secret_kind_for_set("management-admin-token").is_none());
        assert!(parse_secret_kind("private-signing-key").is_none());
        assert_eq!(
            secret_kind_error("aprs-passcode"),
            "CLI_SECRET_KIND_DEPRECATED"
        );
        assert_eq!(secret_kind_error("unknown"), "CLI_SECRET_KIND_INVALID");
        assert_eq!(
            normalize_secret_input(String::from("secret-from-stdin\r\n")),
            Some(String::from("secret-from-stdin"))
        );
        assert!(normalize_secret_input(String::from("two\nlines")).is_none());
        assert!(normalize_secret_input("a".repeat(4_096)).is_some());
        assert!(normalize_secret_input("a".repeat(4_097)).is_none());
    }

    #[test]
    fn help_does_not_advertise_a_control_token() {
        let help = Cli::command().render_long_help().to_string();
        assert!(!help.to_ascii_lowercase().contains("token"));
    }

    #[test]
    fn filters_log_output() {
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
            schema_version: 3,
            agent: String::from("running"),
            identity: compiled_component_identity(InternalComponent::Agent).unwrap(),
            gateway: GatewayControlStatus::Stopped,
            management_web: ManagementWebControlStatus::Running,
            management_web_url: Some(String::from("http://127.0.0.1:7080")),
            uptime_seconds: 1,
            latest_error_code: None,
        };
        let mut diagnostics = DiagnosticsControlBundle {
            schema_version: 2,
            identity: compiled_component_identity(InternalComponent::Agent).unwrap(),
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
