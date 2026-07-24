use clap::{Parser, Subcommand, ValueEnum};
use cmclient_legacy_migration::{
    ChildGatewayMaintenanceRunner, GatewayMaintenanceReport, GatewayMaintenanceRequest,
    MaintenanceSchemaHistory, MigrationError, MigrationPhase, ProductMigrationRequest,
    run_or_resume_product_migration, run_or_resume_product_migration_with_phase_hook,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{Read, Write},
    path::{Path, PathBuf},
    process::ExitCode,
    thread,
    time::Duration,
};

const MAX_TEST_MAINTENANCE_REQUEST_BYTES: u64 = 16 * 1024;
const MAX_TEST_DATABASE_BYTES: u64 = 1024 * 1024 * 1024;

#[derive(Debug, Parser)]
#[command(
    name = "cmclient-migrate",
    version,
    about = "CMClient product state migration tool"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run or resume the bounded product migration transaction.
    Product {
        #[arg(long)]
        source_root: PathBuf,
        #[arg(long)]
        target_root: PathBuf,
        #[arg(long)]
        gateway_program: PathBuf,
        #[arg(long)]
        gateway_entrypoint: Option<PathBuf>,
        #[arg(long, hide = true)]
        test_maintenance_helper: bool,
        #[arg(long, hide = true)]
        test_pause_after: Option<PhaseArgument>,
        #[arg(long, hide = true)]
        test_pause_file: Option<PathBuf>,
    },
    #[command(name = "__test-maintenance", hide = true)]
    TestMaintenance {
        #[arg(long)]
        offline_maintenance: bool,
        #[arg(long, hide = true, default_value = "success")]
        test_mode: TestMaintenanceMode,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum PhaseArgument {
    Detected,
    Staged,
    Verified,
    Activated,
    Complete,
}

impl From<PhaseArgument> for MigrationPhase {
    fn from(value: PhaseArgument) -> Self {
        match value {
            PhaseArgument::Detected => Self::Detected,
            PhaseArgument::Staged => Self::Staged,
            PhaseArgument::Verified => Self::Verified,
            PhaseArgument::Activated => Self::Activated,
            PhaseArgument::Complete => Self::Complete,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum TestMaintenanceMode {
    Success,
    Retryable,
    Failure,
    Malformed,
    Oversize,
    Timeout,
}

fn main() -> ExitCode {
    match Cli::parse().command {
        Command::Product {
            source_root,
            target_root,
            gateway_program,
            gateway_entrypoint,
            test_maintenance_helper,
            test_pause_after,
            test_pause_file,
        } => migrate_product(
            source_root,
            target_root,
            gateway_program,
            gateway_entrypoint,
            test_maintenance_helper,
            test_pause_after,
            test_pause_file,
        ),
        Command::TestMaintenance {
            offline_maintenance,
            test_mode,
        } => test_maintenance(offline_maintenance, test_mode),
    }
}

#[allow(clippy::too_many_arguments)]
fn migrate_product(
    source_root: PathBuf,
    target_root: PathBuf,
    gateway_program: PathBuf,
    gateway_entrypoint: Option<PathBuf>,
    test_maintenance_helper: bool,
    test_pause_after: Option<PhaseArgument>,
    test_pause_file: Option<PathBuf>,
) -> ExitCode {
    if !source_root.is_absolute()
        || !target_root.is_absolute()
        || !gateway_program.is_absolute()
        || test_pause_file
            .as_ref()
            .is_some_and(|path| !path.is_absolute())
        || test_pause_after.is_some() != test_pause_file.is_some()
    {
        return migration_error(MigrationError::PathInvalid);
    }

    let runner = if test_maintenance_helper {
        if !cfg!(debug_assertions) || gateway_entrypoint.is_some() {
            return migration_error(MigrationError::MaintenanceCommandInvalid);
        }
        match ChildGatewayMaintenanceRunner::with_prefix(
            gateway_program,
            vec!["__test-maintenance".into()],
        ) {
            Ok(runner) => runner,
            Err(error) => return migration_error(error),
        }
    } else {
        let Some(gateway_entrypoint) = gateway_entrypoint else {
            return migration_error(MigrationError::MaintenanceCommandInvalid);
        };
        match ChildGatewayMaintenanceRunner::new(gateway_program, gateway_entrypoint) {
            Ok(runner) => runner,
            Err(error) => return migration_error(error),
        }
    };

    let request = ProductMigrationRequest {
        source_root,
        target_root,
    };
    let result = match (test_pause_after, test_pause_file) {
        (Some(phase), Some(marker)) if cfg!(debug_assertions) => {
            let phase = MigrationPhase::from(phase);
            run_or_resume_product_migration_with_phase_hook(&request, &runner, &mut |current| {
                if current == phase {
                    let _ = fs::write(&marker, current_name(current));
                    loop {
                        thread::sleep(Duration::from_secs(1));
                    }
                }
            })
        }
        (None, None) => run_or_resume_product_migration(&request, &runner),
        _ => return migration_error(MigrationError::PathInvalid),
    };

    match result {
        Ok(outcome) => print_json(&outcome),
        Err(error) => migration_error(error),
    }
}

fn test_maintenance(offline_maintenance: bool, mode: TestMaintenanceMode) -> ExitCode {
    if !cfg!(debug_assertions) || !offline_maintenance {
        return migration_error(MigrationError::MaintenanceCommandInvalid);
    }
    match mode {
        TestMaintenanceMode::Retryable => return ExitCode::from(75),
        TestMaintenanceMode::Failure => return ExitCode::from(2),
        TestMaintenanceMode::Malformed => {
            println!("not-json");
            return ExitCode::SUCCESS;
        }
        TestMaintenanceMode::Oversize => {
            let mut stdout = std::io::stdout().lock();
            let _ = stdout.write_all(&vec![b'x'; 64 * 1024 + 1]);
            return ExitCode::SUCCESS;
        }
        TestMaintenanceMode::Timeout => loop {
            thread::sleep(Duration::from_secs(1));
        },
        TestMaintenanceMode::Success => {}
    }

    let request = match read_test_maintenance_request() {
        Ok(request) => request,
        Err(error) => return migration_error(error),
    };
    if fs::create_dir_all(
        request
            .staged_database_path
            .parent()
            .unwrap_or_else(|| Path::new("")),
    )
    .is_err()
        || fs::copy(&request.source_database_path, &request.staged_database_path).is_err()
    {
        return migration_error(MigrationError::MaintenanceFailed);
    }
    let source = match hash_file(&request.source_database_path) {
        Ok(value) => value,
        Err(error) => return migration_error(error),
    };
    let staged = match hash_file(&request.staged_database_path) {
        Ok(value) => value,
        Err(error) => return migration_error(error),
    };
    print_json(&GatewayMaintenanceReport {
        schema_version: 1,
        message_type: String::from("gateway.offline-maintenance-report"),
        operation: String::from("backup_migrate_verify"),
        source_database_sha256: source.1,
        staged_database_sha256: staged.1,
        staged_database_bytes: staged.0,
        integrity: String::from("ok"),
        foreign_key_violations: 0,
        schema_history: vec![MaintenanceSchemaHistory {
            version: 1,
            name: String::from("fixture"),
            sha256: "0".repeat(64),
        }],
        domain_counts: BTreeMap::new(),
    })
}

fn read_test_maintenance_request() -> Result<GatewayMaintenanceRequest, MigrationError> {
    let mut bytes = Vec::new();
    std::io::stdin()
        .lock()
        .take(MAX_TEST_MAINTENANCE_REQUEST_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| MigrationError::MaintenanceCommandInvalid)?;
    if bytes.is_empty() || bytes.len() as u64 > MAX_TEST_MAINTENANCE_REQUEST_BYTES {
        return Err(MigrationError::MaintenanceCommandInvalid);
    }
    let request: GatewayMaintenanceRequest =
        serde_json::from_slice(&bytes).map_err(|_| MigrationError::MaintenanceCommandInvalid)?;
    if !request.is_valid() {
        return Err(MigrationError::MaintenanceCommandInvalid);
    }
    Ok(request)
}

fn hash_file(path: &Path) -> Result<(u64, String), MigrationError> {
    let mut file = File::open(path).map_err(|_| MigrationError::MaintenanceFailed)?;
    let mut digest = Sha256::new();
    let mut size = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|_| MigrationError::MaintenanceFailed)?;
        if read == 0 {
            break;
        }
        size = size
            .checked_add(read as u64)
            .ok_or(MigrationError::MaintenanceFailed)?;
        if size > MAX_TEST_DATABASE_BYTES {
            return Err(MigrationError::MaintenanceFailed);
        }
        digest.update(&buffer[..read]);
    }
    Ok((size, hex_digest(digest.finalize())))
}

fn hex_digest(bytes: impl AsRef<[u8]>) -> String {
    let mut output = String::with_capacity(bytes.as_ref().len() * 2);
    for byte in bytes.as_ref() {
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn current_name(phase: MigrationPhase) -> &'static [u8] {
    match phase {
        MigrationPhase::Detected => b"detected",
        MigrationPhase::Staged => b"staged",
        MigrationPhase::Verified => b"verified",
        MigrationPhase::Activated => b"activated",
        MigrationPhase::Complete => b"complete",
    }
}

fn migration_error(error: MigrationError) -> ExitCode {
    eprintln!("{}", error.code());
    ExitCode::from(2)
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
