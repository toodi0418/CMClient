use clap::{Parser, Subcommand};
use cmclient_legacy_migration::{
    data::{
        LegacyDataError, LegacyDataImportRequest, LegacyDataRollbackRequest, apply_legacy_data,
        inspect_legacy_data, rollback_legacy_data,
    },
    inspect_legacy_settings, write_new_agent_config,
};
use serde::Serialize;
use std::{fs, path::PathBuf, process::ExitCode};

#[derive(Debug, Parser)]
#[command(
    name = "cmclient-migrate",
    version,
    about = "CMClient Legacy migration tool"
)]
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
    /// Inspect, import, verify, and roll back Legacy historical data offline.
    Data {
        #[command(subcommand)]
        command: DataCommand,
    },
}

#[derive(Debug, Subcommand)]
enum DataCommand {
    /// Inspect Legacy history. Use --apply only after the Gateway is stopped.
    Import {
        #[arg(long)]
        source_dir: PathBuf,
        #[arg(long)]
        target_database: PathBuf,
        #[arg(long)]
        mesh_network_id: String,
        #[arg(long)]
        backup_dir: PathBuf,
        #[arg(long)]
        apply: bool,
        #[arg(long)]
        confirm_gateway_stopped: bool,
    },
    /// Restore the snapshot emitted by a successful data import.
    Rollback {
        #[arg(long)]
        target_database: PathBuf,
        #[arg(long)]
        backup_database: PathBuf,
        #[arg(long)]
        confirm_gateway_stopped: bool,
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
        Command::Data { command } => import_data(command),
    }
}

fn import_data(command: DataCommand) -> ExitCode {
    match command {
        DataCommand::Import {
            source_dir,
            target_database,
            mesh_network_id,
            backup_dir,
            apply,
            confirm_gateway_stopped,
        } => {
            if apply && !confirm_gateway_stopped {
                return data_error(LegacyDataError::GatewayStopConfirmationRequired);
            }
            let request = LegacyDataImportRequest {
                source_dir,
                target_database,
                mesh_network_id,
                backup_dir,
            };
            let result = if apply {
                apply_legacy_data(&request)
            } else {
                inspect_legacy_data(&request)
            };
            match result {
                Ok(report) => print_json(&report),
                Err(error) => data_error(error),
            }
        }
        DataCommand::Rollback {
            target_database,
            backup_database,
            confirm_gateway_stopped,
        } => {
            if !confirm_gateway_stopped {
                return data_error(LegacyDataError::GatewayStopConfirmationRequired);
            }
            match rollback_legacy_data(&LegacyDataRollbackRequest {
                target_database,
                backup_database,
            }) {
                Ok(report) => print_json(&report),
                Err(error) => data_error(error),
            }
        }
    }
}

fn data_error(error: LegacyDataError) -> ExitCode {
    eprintln!("{}", error.code());
    ExitCode::from(2)
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
    print_json(&command_report)
}

fn print_json(value: &impl Serialize) -> ExitCode {
    match serde_json::to_string(value) {
        Ok(json) => {
            println!("{json}");
            ExitCode::SUCCESS
        }
        Err(_) => {
            eprintln!("LEGACY_MIGRATION_REPORT_SERIALIZATION_FAILED");
            ExitCode::from(1)
        }
    }
}
