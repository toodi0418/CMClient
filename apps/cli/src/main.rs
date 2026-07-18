use clap::{Parser, Subcommand};
use cmclient_agent_core::AgentConfig;
use cmclient_cli_client::{ExitCode, parse_endpoint};
use cmclient_control_api::{
    ControlClient, ControlEndpoint, ControlError, ControlSecretKind, ControlStatus,
    DiagnosticsControlBundle, UpdateControlStatus, default_unix_socket,
};
use std::{io::Read, process::ExitCode as ProcessExitCode, thread, time::Duration};
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
    #[arg(long, global = true, default_value_t = 30, value_parser = clap::value_parser!(u64).range(1..))]
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
    /// Reads one secret value from standard input and stores it in the OS credential store.
    Set {
        kind: String,
    },
    Remove {
        kind: String,
    },
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
        endpoint,
        command,
        ..
    } = cli;
    if matches!(&command, Command::Version) {
        if json {
            println!(r#"{{"version":"{}"}}"#, env!("CARGO_PKG_VERSION"));
        } else if !quiet {
            println!("cmclient {}", env!("CARGO_PKG_VERSION"));
        }
        return ProcessExitCode::SUCCESS;
    }

    let endpoint = match control_endpoint(&endpoint) {
        Ok(endpoint) => endpoint,
        Err(code) => return ProcessExitCode::from(code.as_u8()),
    };
    let client = match ControlClient::new(endpoint) {
        Ok(client) => client,
        Err(error) => return control_error_exit(error),
    };
    match command {
        Command::Status => print_control_result(client.status(), json, quiet),
        Command::Start => print_control_result(client.start(), json, quiet),
        Command::Stop => print_control_result(client.stop(), json, quiet),
        Command::Restart => print_control_result(client.restart(), json, quiet),
        Command::Update { follow } if follow => follow_update_events(&client, json, quiet),
        Command::Update { follow: false } => match client.update_status() {
            Ok(status) => print_update_status(&status, json, quiet),
            Err(error) => control_error_exit(error),
        },
        Command::Diagnostics => match client.diagnostics_bundle() {
            Ok(bundle) => print_diagnostics_bundle(&bundle, json, quiet),
            Err(error) => control_error_exit(error),
        },
        Command::Secret { command } => manage_secret(&client, command, json, quiet),
        _ => {
            eprintln!("CLI_COMMAND_NOT_IMPLEMENTED");
            ProcessExitCode::from(ExitCode::OperationFailed.as_u8())
        }
    }
}

fn manage_secret(
    client: &ControlClient,
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
    let mut value = String::new();
    std::io::stdin().read_to_string(&mut value).ok()?;
    normalize_secret_input(value)
}

fn normalize_secret_input(mut value: String) -> Option<String> {
    while value.ends_with('\n') || value.ends_with('\r') {
        value.pop();
    }
    (!value.is_empty() && !value.bytes().any(|byte| byte.is_ascii_control())).then_some(value)
}

fn print_control_result(
    result: Result<ControlStatus, ControlError>,
    json: bool,
    quiet: bool,
) -> ProcessExitCode {
    match result {
        Ok(status) => print_status(&status, json, quiet),
        Err(error) => control_error_exit(error),
    }
}

fn control_endpoint(value: &str) -> Result<ControlEndpoint, ExitCode> {
    match parse_endpoint(value)? {
        cmclient_cli_client::ControlEndpointSpec::Local => {
            let config = AgentConfig::load().map_err(|_| ExitCode::Validation)?;
            Ok(default_unix_socket(&config.paths.data_dir))
        }
        cmclient_cli_client::ControlEndpointSpec::UnixSocket(path) => {
            Ok(ControlEndpoint::unix(path))
        }
        cmclient_cli_client::ControlEndpointSpec::NamedPipe(name) => {
            Ok(ControlEndpoint::named_pipe(name))
        }
        cmclient_cli_client::ControlEndpointSpec::Https(_) => Err(ExitCode::Connection),
    }
}

fn print_status(status: &ControlStatus, json: bool, quiet: bool) -> ProcessExitCode {
    if json {
        match serde_json::to_string(status) {
            Ok(serialized) => println!("{serialized}"),
            Err(_) => return ProcessExitCode::from(ExitCode::OperationFailed.as_u8()),
        }
    } else if !quiet {
        println!("agent: {}; gateway: {:?}", status.agent, status.gateway);
    }
    ProcessExitCode::SUCCESS
}

fn print_update_status(status: &UpdateControlStatus, json: bool, quiet: bool) -> ProcessExitCode {
    if json {
        match serde_json::to_string(status) {
            Ok(serialized) => println!("{serialized}"),
            Err(_) => return ProcessExitCode::from(ExitCode::OperationFailed.as_u8()),
        }
    } else if !quiet {
        println!("{}", update_summary(status));
    }
    ProcessExitCode::SUCCESS
}

fn print_diagnostics_bundle(
    bundle: &DiagnosticsControlBundle,
    json: bool,
    quiet: bool,
) -> ProcessExitCode {
    if json {
        match serde_json::to_string(bundle) {
            Ok(serialized) => println!("{serialized}"),
            Err(_) => return ProcessExitCode::from(ExitCode::OperationFailed.as_u8()),
        }
    } else if !quiet {
        println!(
            "diagnostics: gateway: {:?}; management_web: {:?}",
            bundle.gateway, bundle.management_web
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

fn follow_update_events(client: &ControlClient, json: bool, quiet: bool) -> ProcessExitCode {
    loop {
        let mut events = match client.subscribe_update_events() {
            Ok(events) => events,
            Err(_) => {
                thread::sleep(Duration::from_millis(250));
                continue;
            }
        };
        while let Ok(Some(event)) = events.next_event() {
            let status: UpdateControlStatus = match serde_json::from_slice(&event.data) {
                Ok(status) => status,
                Err(_) => {
                    eprintln!("CLI_UPDATE_EVENT_INVALID");
                    return ProcessExitCode::from(ExitCode::OperationFailed.as_u8());
                }
            };
            let exit = print_update_status(&status, json, quiet);
            if exit != ProcessExitCode::SUCCESS {
                return exit;
            }
        }
        thread::sleep(Duration::from_millis(250));
    }
}

fn control_error_exit(error: ControlError) -> ProcessExitCode {
    eprintln!("{}", error.code());
    let code = match error {
        ControlError::Io
        | ControlError::UnsupportedEndpoint
        | ControlError::EndpointAlreadyInUse => ExitCode::Connection,
        ControlError::CommandFailed => ExitCode::OperationFailed,
        ControlError::InvalidHttp | ControlError::ResponseTooLarge => ExitCode::OperationFailed,
    };
    ProcessExitCode::from(code.as_u8())
}

#[cfg(test)]
mod tests {
    use super::{normalize_secret_input, parse_secret_kind, update_summary};
    use cmclient_control_api::{UpdateControlJob, UpdateControlStatus};

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
    }
}
