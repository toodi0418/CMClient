use clap::{Parser, Subcommand};
use cmclient_agent_core::AgentConfig;
use cmclient_cli_client::{ExitCode, parse_endpoint};
use cmclient_control_api::{
    ControlClient, ControlEndpoint, ControlError, ControlStatus, default_unix_socket,
};
use std::process::ExitCode as ProcessExitCode;

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
    Update,
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
    let result = match command {
        Command::Status => client.status(),
        Command::Start => client.start(),
        Command::Stop => client.stop(),
        Command::Restart => client.restart(),
        _ => {
            eprintln!("CLI_COMMAND_NOT_IMPLEMENTED");
            return ProcessExitCode::from(ExitCode::OperationFailed.as_u8());
        }
    };
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
