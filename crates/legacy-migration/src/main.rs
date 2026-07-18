use clap::{Parser, Subcommand};
use cmclient_legacy_migration::{inspect_legacy_settings, write_new_agent_config};
use serde::Serialize;
use std::{fs, path::PathBuf, process::ExitCode};

#[derive(Debug, Parser)]
#[command(name = "cmclient-migrate", about = "CMClient Legacy migration tool")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Inspect a Legacy client-preferences JSON file and optionally create a new Agent config.
    Settings {
        #[arg(long)]
        source: PathBuf,
        #[arg(long, conflicts_with = "write_agent_config")]
        dry_run: bool,
        #[arg(long)]
        write_agent_config: Option<PathBuf>,
    },
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CommandReport {
    dry_run: bool,
    applied: bool,
    report: cmclient_legacy_migration::LegacySettingsReport,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Command::Settings {
            source,
            dry_run: _,
            write_agent_config,
        } => import_settings(source, write_agent_config),
    }
}

fn import_settings(source: PathBuf, write_agent_config: Option<PathBuf>) -> ExitCode {
    if !source.is_absolute() {
        eprintln!("LEGACY_SETTINGS_SOURCE_NOT_ABSOLUTE");
        return ExitCode::from(2);
    }
    let bytes = match fs::read(source) {
        Ok(bytes) => bytes,
        Err(_) => {
            eprintln!("LEGACY_SETTINGS_SOURCE_READ_FAILED");
            return ExitCode::from(1);
        }
    };
    let report = match inspect_legacy_settings(&bytes) {
        Ok(report) => report,
        Err(error) => {
            eprintln!("{}", error.code());
            return ExitCode::from(2);
        }
    };
    let applied = match write_agent_config {
        Some(target) => match write_new_agent_config(&target, &report) {
            Ok(()) => true,
            Err(error) => {
                eprintln!("{}", error.code());
                return ExitCode::from(2);
            }
        },
        None => false,
    };
    let command_report = CommandReport {
        dry_run: !applied,
        applied,
        report,
    };
    match serde_json::to_string(&command_report) {
        Ok(json) => {
            println!("{json}");
            ExitCode::SUCCESS
        }
        Err(_) => {
            eprintln!("LEGACY_SETTINGS_REPORT_SERIALIZATION_FAILED");
            ExitCode::from(1)
        }
    }
}
