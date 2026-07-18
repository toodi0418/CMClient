use clap::{Parser, Subcommand};
use cmclient_agent_core::AgentConfig;
use cmclient_cli_client::{ExitCode, parse_endpoint};
use cmclient_control_api::{
    ControlClient, ControlEndpoint, ControlError, ControlStatus, UpdateControlStatus,
    default_unix_socket,
};
use std::{process::ExitCode as ProcessExitCode, thread, time::Duration};

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
    Database,
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
        _ => {
            eprintln!("CLI_COMMAND_NOT_IMPLEMENTED");
            ProcessExitCode::from(ExitCode::OperationFailed.as_u8())
        }
    }
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
    use super::update_summary;
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
}
