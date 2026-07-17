use clap::{Parser, Subcommand};
use cmclient_cli_client::{ExitCode, parse_endpoint};
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
    if matches!(cli.command, Command::Version) {
        if cli.json {
            println!(r#"{{"version":"{}"}}"#, env!("CARGO_PKG_VERSION"));
        } else if !cli.quiet {
            println!("cmclient {}", env!("CARGO_PKG_VERSION"));
        }
        return ProcessExitCode::SUCCESS;
    }

    eprintln!("CLI_COMMAND_NOT_IMPLEMENTED");
    ProcessExitCode::from(ExitCode::OperationFailed.as_u8())
}
