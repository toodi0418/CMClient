use atomic_write_file::AtomicWriteFile;
use cmclient_agent_core::{
    secrets::validate_migrated_plaintext_secrets, validate_migrated_agent_config,
};
use cmclient_runtime_primitives::{
    DocumentFormat, DurableDocument, ExclusiveFileLock, LockError, TypedDocument,
};
use same_file::Handle;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::{OsStr, OsString},
    fmt::{Display, Formatter},
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

const JOURNAL_SCHEMA_VERSION: u8 = 1;
const MAINTENANCE_REPORT_SCHEMA_VERSION: u8 = 1;
const MAX_JOURNAL_BYTES: usize = 512 * 1024;
const MAX_MAINTENANCE_REPORT_BYTES: usize = 64 * 1024;
const MAX_MAINTENANCE_REQUEST_BYTES: usize = 16 * 1024;
const MAX_LEGACY_ROOT_CANDIDATES: usize = 8;
const MAX_MIGRATION_FILES: usize = 64;
const MAX_BACKUP_FILES: usize = MAX_MIGRATION_FILES - 3;
const MAX_BACKUP_DEPTH: usize = 8;
const MAX_BACKUP_SCAN_ENTRIES: usize = 512;
const MAX_CONFIG_BYTES: u64 = 1024 * 1024;
const MAX_SECRETS_BYTES: u64 = 64 * 1024;
const MAX_DATABASE_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_DATABASE_WAL_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_DATABASE_SHM_BYTES: u64 = 64 * 1024 * 1024;
const MAX_BACKUP_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_TOTAL_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const MAX_PATH_BYTES: usize = 4096;
const MAX_SCHEMA_HISTORY: usize = 256;
const MAX_DOMAIN_COUNTS: usize = 128;
const MAX_ATOMIC_TEMP_SCAN_ENTRIES: usize = 512;
const GATEWAY_MAINTENANCE_WORK_DIRECTORY: &str = ".cmclient.db.maintenance.maintenance-work";
const DEFAULT_MAINTENANCE_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const MAX_MAINTENANCE_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const WINDOWS_REPARSE_POINT: u32 = 0x0400;
#[cfg(windows)]
const WINDOWS_OPEN_REPARSE_POINT: u32 = 0x0020_0000;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationPhase {
    Detected,
    Staged,
    Verified,
    Activated,
    Complete,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProductMigrationRequest {
    pub source_root: PathBuf,
    pub target_root: PathBuf,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ProductMigrationSourceSet {
    pub config_root: PathBuf,
    pub data_root: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GatewayMaintenanceRequest {
    pub schema_version: u8,
    #[serde(rename = "type")]
    pub message_type: String,
    pub source_database_path: PathBuf,
    pub staged_database_path: PathBuf,
}

impl GatewayMaintenanceRequest {
    fn new(source_database: &Path, staged_database: &Path) -> Result<Self, MigrationError> {
        let request = Self {
            schema_version: 1,
            message_type: String::from("gateway.offline-maintenance"),
            source_database_path: source_database.to_path_buf(),
            staged_database_path: staged_database.to_path_buf(),
        };
        if !request.is_valid() {
            return Err(MigrationError::MaintenanceCommandInvalid);
        }
        Ok(request)
    }

    pub fn is_valid(&self) -> bool {
        self.schema_version == 1
            && self.message_type == "gateway.offline-maintenance"
            && self.source_database_path.is_absolute()
            && self.staged_database_path.is_absolute()
            && self.source_database_path != self.staged_database_path
            && path_text(&self.source_database_path).is_ok()
            && path_text(&self.staged_database_path).is_ok()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MigrationOutcome {
    pub schema_version: u8,
    pub phase: MigrationPhase,
    pub migrated: bool,
    pub file_count: usize,
    pub database_migrated: bool,
    pub plaintext_secrets_migrated: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MaintenanceSchemaHistory {
    pub version: u32,
    pub name: String,
    pub sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GatewayMaintenanceReport {
    pub schema_version: u8,
    #[serde(rename = "type")]
    pub message_type: String,
    pub operation: String,
    pub source_database_sha256: String,
    pub staged_database_sha256: String,
    pub staged_database_bytes: u64,
    pub integrity: String,
    pub foreign_key_violations: u64,
    pub schema_history: Vec<MaintenanceSchemaHistory>,
    pub domain_counts: BTreeMap<String, u64>,
}

impl GatewayMaintenanceReport {
    fn is_valid(&self) -> bool {
        if self.schema_version != MAINTENANCE_REPORT_SCHEMA_VERSION
            || self.message_type != "gateway.offline-maintenance-report"
            || self.operation != "backup_migrate_verify"
            || !is_sha256(&self.source_database_sha256)
            || !is_sha256(&self.staged_database_sha256)
            || self.staged_database_bytes == 0
            || self.staged_database_bytes > MAX_DATABASE_BYTES
            || self.integrity != "ok"
            || self.foreign_key_violations != 0
            || self.schema_history.is_empty()
            || self.schema_history.len() > MAX_SCHEMA_HISTORY
            || self.domain_counts.len() > MAX_DOMAIN_COUNTS
        {
            return false;
        }
        let mut previous = 0;
        for record in &self.schema_history {
            if record.version <= previous
                || !is_bounded_identifier(&record.name, 128)
                || !is_sha256(&record.sha256)
            {
                return false;
            }
            previous = record.version;
        }
        self.domain_counts
            .keys()
            .all(|name| is_bounded_identifier(name, 128))
    }
}

impl DurableDocument for GatewayMaintenanceReport {
    const FORMAT: DocumentFormat = DocumentFormat::Json;
    const MAX_BYTES: usize = MAX_MAINTENANCE_REPORT_BYTES;

    fn validate(&self) -> bool {
        self.is_valid()
    }
}

pub trait GatewayMaintenanceRunner {
    fn migrate_database(
        &self,
        source_database: &Path,
        staged_database: &Path,
    ) -> Result<GatewayMaintenanceReport, MigrationError>;
}

#[derive(Clone, Debug)]
pub struct ChildGatewayMaintenanceRunner {
    program: PathBuf,
    prefix_arguments: Vec<OsString>,
    timeout: Duration,
}

impl ChildGatewayMaintenanceRunner {
    pub fn new(program: PathBuf, gateway_entrypoint: PathBuf) -> Result<Self, MigrationError> {
        if !program.is_absolute() || !gateway_entrypoint.is_absolute() {
            return Err(MigrationError::MaintenanceCommandInvalid);
        }
        Self::with_prefix(program, vec![gateway_entrypoint.into_os_string()])
    }

    #[doc(hidden)]
    pub fn with_prefix(
        program: PathBuf,
        prefix_arguments: Vec<OsString>,
    ) -> Result<Self, MigrationError> {
        if !program.is_absolute()
            || prefix_arguments.len() > 8
            || prefix_arguments.iter().any(|argument| {
                argument.to_string_lossy().len() > MAX_PATH_BYTES
                    || argument.to_string_lossy().contains(char::is_control)
            })
        {
            return Err(MigrationError::MaintenanceCommandInvalid);
        }
        Ok(Self {
            program,
            prefix_arguments,
            timeout: DEFAULT_MAINTENANCE_TIMEOUT,
        })
    }

    #[doc(hidden)]
    pub fn with_prefix_and_timeout(
        program: PathBuf,
        prefix_arguments: Vec<OsString>,
        timeout: Duration,
    ) -> Result<Self, MigrationError> {
        let mut runner = Self::with_prefix(program, prefix_arguments)?;
        if timeout.is_zero() || timeout > MAX_MAINTENANCE_TIMEOUT {
            return Err(MigrationError::MaintenanceCommandInvalid);
        }
        runner.timeout = timeout;
        Ok(runner)
    }
}

impl GatewayMaintenanceRunner for ChildGatewayMaintenanceRunner {
    fn migrate_database(
        &self,
        source_database: &Path,
        staged_database: &Path,
    ) -> Result<GatewayMaintenanceReport, MigrationError> {
        let request = GatewayMaintenanceRequest::new(source_database, staged_database)?;
        let request_bytes =
            serde_json::to_vec(&request).map_err(|_| MigrationError::MaintenanceCommandInvalid)?;
        if request_bytes.len() > MAX_MAINTENANCE_REQUEST_BYTES {
            return Err(MigrationError::MaintenanceCommandInvalid);
        }
        let mut child = Command::new(&self.program)
            .args(&self.prefix_arguments)
            .arg("--offline-maintenance")
            .env_clear()
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|_| MigrationError::MaintenanceFailed)?;
        let mut stdin = child
            .stdin
            .take()
            .ok_or(MigrationError::MaintenanceFailed)?;
        let writer = thread::spawn(move || {
            stdin
                .write_all(&request_bytes)
                .and_then(|_| stdin.flush())
                .map_err(|_| MigrationError::MaintenanceFailed)
        });
        let stdout = child
            .stdout
            .take()
            .ok_or(MigrationError::MaintenanceFailed)?;
        let reader = thread::spawn(move || {
            let mut stdout = stdout;
            let mut bytes = Vec::new();
            let mut buffer = [0_u8; 4096];
            loop {
                let read = stdout
                    .read(&mut buffer)
                    .map_err(|_| MigrationError::MaintenanceFailed)?;
                if read == 0 {
                    break;
                }
                if bytes.len() + read > MAX_MAINTENANCE_REPORT_BYTES {
                    return Err(MigrationError::MaintenanceReportInvalid);
                }
                bytes.extend_from_slice(&buffer[..read]);
            }
            Ok(bytes)
        });

        let deadline = Instant::now() + self.timeout;
        let status = loop {
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) if Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(20));
                }
                Ok(None) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    let _ = writer.join();
                    let _ = reader.join();
                    return Err(MigrationError::MaintenanceTimedOut);
                }
                Err(_) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    let _ = writer.join();
                    let _ = reader.join();
                    return Err(MigrationError::MaintenanceFailed);
                }
            }
        };
        let write_result = writer
            .join()
            .map_err(|_| MigrationError::MaintenanceFailed)?;
        let bytes_result = reader
            .join()
            .map_err(|_| MigrationError::MaintenanceFailed)?;
        if matches!(&bytes_result, Err(MigrationError::MaintenanceReportInvalid)) {
            return Err(MigrationError::MaintenanceReportInvalid);
        }
        if status.code() == Some(75) {
            return Err(MigrationError::MaintenanceRetryable);
        }
        if !status.success() {
            return Err(MigrationError::MaintenanceFailed);
        }
        write_result?;
        let bytes = bytes_result?;
        let report: GatewayMaintenanceReport =
            serde_json::from_slice(&bytes).map_err(|_| MigrationError::MaintenanceReportInvalid)?;
        if !report.is_valid() {
            return Err(MigrationError::MaintenanceReportInvalid);
        }
        Ok(report)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MigrationError {
    PathInvalid,
    SourceMissing,
    SourceUnsafe,
    SourceAmbiguous,
    SourceEmpty,
    SourceTooManyFiles,
    SourceFileTooLarge,
    SourceTooLarge,
    SourceReadFailed,
    SourceChanged,
    SourceInUse,
    TargetUnsafe,
    TargetPopulated,
    MigrationInProgress,
    JournalInvalid,
    JournalWriteFailed,
    CleanupFailed,
    StageFailed,
    StageDigestMismatch,
    ConfigInvalid,
    SecretsInvalid,
    MaintenanceCommandInvalid,
    MaintenanceRetryable,
    MaintenanceTimedOut,
    MaintenanceFailed,
    MaintenanceReportInvalid,
    ActivationFailed,
}

impl MigrationError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::PathInvalid => "LEGACY_MIGRATION_PATH_INVALID",
            Self::SourceMissing => "LEGACY_MIGRATION_SOURCE_MISSING",
            Self::SourceUnsafe => "LEGACY_MIGRATION_SOURCE_UNSAFE",
            Self::SourceAmbiguous => "LEGACY_MIGRATION_SOURCE_AMBIGUOUS",
            Self::SourceEmpty => "LEGACY_MIGRATION_SOURCE_EMPTY",
            Self::SourceTooManyFiles => "LEGACY_MIGRATION_SOURCE_FILE_COUNT_EXCEEDED",
            Self::SourceFileTooLarge => "LEGACY_MIGRATION_SOURCE_FILE_TOO_LARGE",
            Self::SourceTooLarge => "LEGACY_MIGRATION_SOURCE_TOTAL_SIZE_EXCEEDED",
            Self::SourceReadFailed => "LEGACY_MIGRATION_SOURCE_READ_FAILED",
            Self::SourceChanged => "LEGACY_MIGRATION_SOURCE_CHANGED",
            Self::SourceInUse => "LEGACY_MIGRATION_SOURCE_IN_USE",
            Self::TargetUnsafe => "LEGACY_MIGRATION_TARGET_UNSAFE",
            Self::TargetPopulated => "LEGACY_MIGRATION_TARGET_POPULATED",
            Self::MigrationInProgress => "LEGACY_MIGRATION_IN_PROGRESS",
            Self::JournalInvalid => "LEGACY_MIGRATION_JOURNAL_INVALID",
            Self::JournalWriteFailed => "LEGACY_MIGRATION_JOURNAL_WRITE_FAILED",
            Self::CleanupFailed => "LEGACY_MIGRATION_CLEANUP_FAILED",
            Self::StageFailed => "LEGACY_MIGRATION_STAGE_FAILED",
            Self::StageDigestMismatch => "LEGACY_MIGRATION_STAGE_DIGEST_MISMATCH",
            Self::ConfigInvalid => "LEGACY_MIGRATION_CONFIG_INVALID",
            Self::SecretsInvalid => "LEGACY_MIGRATION_SECRETS_INVALID",
            Self::MaintenanceCommandInvalid => "LEGACY_MIGRATION_MAINTENANCE_COMMAND_INVALID",
            Self::MaintenanceRetryable => "LEGACY_MIGRATION_MAINTENANCE_RETRYABLE",
            Self::MaintenanceTimedOut => "LEGACY_MIGRATION_MAINTENANCE_TIMEOUT",
            Self::MaintenanceFailed => "LEGACY_MIGRATION_MAINTENANCE_FAILED",
            Self::MaintenanceReportInvalid => "LEGACY_MIGRATION_MAINTENANCE_REPORT_INVALID",
            Self::ActivationFailed => "LEGACY_MIGRATION_ACTIVATION_FAILED",
        }
    }
}

impl Display for MigrationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.code())
    }
}

impl std::error::Error for MigrationError {}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum MigrationFileKind {
    Config,
    Secrets,
    Database,
    Backup,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum MigrationSourceKind {
    #[default]
    Config,
    Data,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MigrationEntry {
    kind: MigrationFileKind,
    #[serde(default, skip_serializing_if = "is_config_source")]
    source_kind: MigrationSourceKind,
    source_relative: String,
    target_relative: String,
    size_bytes: u64,
    sha256: String,
    identity: SourceFileIdentity,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    database_wal: Option<SourceFileEvidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    database_shm: Option<SourceTopologyEvidence>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SourceFileIdentity {
    device_id: u64,
    file_id: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SourceFileEvidence {
    source_relative: String,
    size_bytes: u64,
    sha256: String,
    identity: SourceFileIdentity,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SourceTopologyEvidence {
    source_relative: String,
    identity: SourceFileIdentity,
}

struct SourceLockGuard {
    held: Vec<HeldSourceLock>,
    absent: Vec<PathBuf>,
}

struct HeldSourceLock {
    path: PathBuf,
    identity: Handle,
    inspection: File,
    _lock: ExclusiveFileLock,
}

impl SourceLockGuard {
    fn verify(&self) -> Result<(), MigrationError> {
        for held in &self.held {
            let path_metadata =
                fs::symlink_metadata(&held.path).map_err(|_| MigrationError::SourceChanged)?;
            let opened_metadata = held
                .inspection
                .metadata()
                .map_err(|_| MigrationError::SourceChanged)?;
            if file_metadata_is_unsafe_link(&path_metadata)
                || !path_metadata.file_type().is_file()
                || path_metadata.len() != 0
                || !opened_metadata.file_type().is_file()
                || opened_metadata.len() != 0
                || opened_file_has_unexpected_link_count(&held.inspection)
                    .map_err(|_| MigrationError::SourceChanged)?
                || file_handle_nofollow(&held.path).map_err(|_| MigrationError::SourceChanged)?
                    != held.identity
            {
                return Err(MigrationError::SourceChanged);
            }
        }
        for path in &self.absent {
            match fs::symlink_metadata(path) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                _ => return Err(MigrationError::SourceChanged),
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StagedDatabaseEvidence {
    size_bytes: u64,
    sha256: String,
    report_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MigrationJournal {
    schema_version: u8,
    transaction_id: String,
    phase: MigrationPhase,
    source_root: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    data_source_root: Option<String>,
    entries: Vec<MigrationEntry>,
    staged_database: Option<StagedDatabaseEvidence>,
    recovery_code: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MigrationSourceRoots {
    config: PathBuf,
    data: PathBuf,
}

impl MigrationSourceRoots {
    fn source_for(&self, entry: &MigrationEntry) -> &Path {
        match entry.source_kind {
            MigrationSourceKind::Config => &self.config,
            MigrationSourceKind::Data => &self.data,
        }
    }

    fn data_journal_value(&self) -> Result<Option<String>, MigrationError> {
        if self.config == self.data {
            Ok(None)
        } else {
            path_text(&self.data).map(Some)
        }
    }
}

impl DurableDocument for MigrationJournal {
    const FORMAT: DocumentFormat = DocumentFormat::Json;
    const MAX_BYTES: usize = MAX_JOURNAL_BYTES;

    fn validate(&self) -> bool {
        if self.schema_version != JOURNAL_SCHEMA_VERSION
            || !is_hex(&self.transaction_id, 24)
            || self.source_root.is_empty()
            || self.source_root.len() > MAX_PATH_BYTES
            || !Path::new(&self.source_root).is_absolute()
            || self.data_source_root.as_ref().is_some_and(|root| {
                root.is_empty()
                    || root.len() > MAX_PATH_BYTES
                    || !Path::new(root).is_absolute()
                    || root == &self.source_root
            })
            || self.entries.is_empty()
            || self.entries.len() > MAX_MIGRATION_FILES
            || self
                .recovery_code
                .as_ref()
                .is_some_and(|code| !is_bounded_error_code(code))
        {
            return false;
        }
        let mut targets = BTreeSet::new();
        let mut total = 0_u64;
        let mut database_count = 0;
        for entry in &self.entries {
            if !valid_entry_paths(entry)
                || !matches!(
                    (
                        self.data_source_root.is_some(),
                        entry.kind,
                        entry.source_kind,
                    ),
                    (false, _, MigrationSourceKind::Config)
                        | (
                            true,
                            MigrationFileKind::Config | MigrationFileKind::Secrets,
                            MigrationSourceKind::Config,
                        )
                        | (
                            true,
                            MigrationFileKind::Database | MigrationFileKind::Backup,
                            MigrationSourceKind::Data,
                        )
                )
                || entry.size_bytes == 0
                || entry.size_bytes > max_file_bytes(entry.kind)
                || !is_sha256(&entry.sha256)
                || !targets.insert(entry.target_relative.to_ascii_lowercase())
            {
                return false;
            }
            total = match total.checked_add(entry.size_bytes) {
                Some(total) => total,
                None => return false,
            };
            if let Some(wal) = entry.database_wal.as_ref() {
                if entry.kind != MigrationFileKind::Database
                    || wal.source_relative != format!("{}-wal", entry.source_relative)
                    || wal.source_relative.len() > MAX_PATH_BYTES
                    || wal.size_bytes > MAX_DATABASE_WAL_BYTES
                    || !is_sha256(&wal.sha256)
                {
                    return false;
                }
                total = match total.checked_add(wal.size_bytes) {
                    Some(total) => total,
                    None => return false,
                };
            }
            if let Some(shared_memory) = entry.database_shm.as_ref() {
                if entry.kind != MigrationFileKind::Database
                    || shared_memory.source_relative != format!("{}-shm", entry.source_relative)
                    || shared_memory.source_relative.len() > MAX_PATH_BYTES
                {
                    return false;
                }
            }
            if entry.kind == MigrationFileKind::Database {
                database_count += 1;
            } else if entry.database_wal.is_some() || entry.database_shm.is_some() {
                return false;
            }
        }
        if total > MAX_TOTAL_BYTES || database_count > 1 {
            return false;
        }
        match (database_count, self.phase >= MigrationPhase::Staged) {
            (0, _) => self.staged_database.is_none(),
            (1, false) => self.staged_database.is_none(),
            (1, true) => self.staged_database.as_ref().is_some_and(|evidence| {
                evidence.size_bytes > 0
                    && evidence.size_bytes <= MAX_DATABASE_BYTES
                    && is_sha256(&evidence.sha256)
                    && is_sha256(&evidence.report_sha256)
            }),
            _ => false,
        }
    }
}

/// Run a new migration or resume the durable transaction already recorded at the target.
pub fn run_or_resume_product_migration(
    request: &ProductMigrationRequest,
    maintenance: &dyn GatewayMaintenanceRunner,
) -> Result<MigrationOutcome, MigrationError> {
    run_or_resume_product_migration_with_phase_hook(request, maintenance, &mut |_| {})
}

pub fn run_or_resume_product_migration_source_set(
    source: &ProductMigrationSourceSet,
    target_root: &Path,
    maintenance: &dyn GatewayMaintenanceRunner,
) -> Result<MigrationOutcome, MigrationError> {
    run_or_resume_product_migration_roots_with_phase_hook(
        source,
        target_root,
        maintenance,
        &mut |_| {},
    )
}

#[doc(hidden)]
pub fn run_or_resume_product_migration_with_phase_hook(
    request: &ProductMigrationRequest,
    maintenance: &dyn GatewayMaintenanceRunner,
    phase_hook: &mut dyn FnMut(MigrationPhase),
) -> Result<MigrationOutcome, MigrationError> {
    run_or_resume_product_migration_roots_with_phase_hook(
        &ProductMigrationSourceSet {
            config_root: request.source_root.clone(),
            data_root: request.source_root.clone(),
        },
        &request.target_root,
        maintenance,
        phase_hook,
    )
}

fn run_or_resume_product_migration_roots_with_phase_hook(
    source: &ProductMigrationSourceSet,
    requested_target_root: &Path,
    maintenance: &dyn GatewayMaintenanceRunner,
    phase_hook: &mut dyn FnMut(MigrationPhase),
) -> Result<MigrationOutcome, MigrationError> {
    if !source.config_root.is_absolute()
        || !source.data_root.is_absolute()
        || !requested_target_root.is_absolute()
        || source.config_root == requested_target_root
        || source.data_root == requested_target_root
    {
        return Err(MigrationError::PathInvalid);
    }
    let existing_journal = load_existing_journal(requested_target_root)?;
    preflight_existing_target(requested_target_root, existing_journal.as_ref())?;
    let roots = if existing_journal
        .as_ref()
        .is_some_and(|journal| journal.phase == MigrationPhase::Complete)
    {
        None
    } else {
        let roots = resolve_source_roots(source)?.ok_or(MigrationError::SourceMissing)?;
        reject_target_overlap(&roots.config, requested_target_root)?;
        if roots.data != roots.config {
            reject_target_overlap(&roots.data, requested_target_root)?;
        }
        Some(roots)
    };
    prepare_target_root(requested_target_root)?;
    let target_root =
        fs::canonicalize(requested_target_root).map_err(|_| MigrationError::TargetUnsafe)?;
    let target_lock_path = target_root.join("run/migration.lock");
    let _target_lock =
        ExclusiveFileLock::try_acquire(&target_lock_path).map_err(map_target_lock_error)?;
    let journal_document =
        TypedDocument::<MigrationJournal>::new(target_root.join("state/migration.json"))
            .map_err(|_| MigrationError::JournalInvalid)?;
    cleanup_atomic_write_temps_for_destination(
        &target_root,
        journal_document.path(),
        MAX_JOURNAL_BYTES as u64,
        MigrationError::JournalInvalid,
    )?;
    let mut journal = load_json_document_nofollow::<MigrationJournal>(
        journal_document.path(),
        MigrationError::JournalInvalid,
    )?;
    cleanup_transaction_atomic_temps(&target_root, journal.as_ref())?;

    if let Some(existing) = journal.as_mut() {
        if existing.phase == MigrationPhase::Complete {
            if let Err(error) = cleanup_complete_stage(&target_root, existing) {
                return Err(persist_recovery_error(&journal_document, existing, error));
            }
            if existing.recovery_code.take().is_some() {
                store_journal(&journal_document, existing)?;
            }
            return Ok(outcome(existing));
        }
        let roots = roots.as_ref().ok_or(MigrationError::SourceMissing)?;
        if existing.source_root != path_text(&roots.config)?
            || existing.data_source_root != roots.data_journal_value()?
        {
            return Err(MigrationError::TargetPopulated);
        }
    }
    validate_target_shape(&target_root, journal.as_ref())?;

    let roots = roots.as_ref().ok_or(MigrationError::SourceMissing)?;
    let source_locks = acquire_source_lock_set(roots)?;
    verify_source_lock_set(&source_locks)?;

    if journal.is_none() {
        let entries = inventory_source(roots)?;
        verify_source_lock_set(&source_locks)?;
        let source_root_text = path_text(&roots.config)?;
        let data_source_root = roots.data_journal_value()?;
        let transaction_id =
            transaction_id(&source_root_text, data_source_root.as_deref(), &entries);
        let detected = MigrationJournal {
            schema_version: JOURNAL_SCHEMA_VERSION,
            transaction_id,
            phase: MigrationPhase::Detected,
            source_root: source_root_text,
            data_source_root,
            entries,
            staged_database: None,
            recovery_code: None,
        };
        store_journal(&journal_document, &detected)?;
        phase_hook(MigrationPhase::Detected);
        journal = Some(detected);
    }

    let mut journal = journal.expect("migration journal initialized");
    if let Err(error) = advance_transaction(
        roots,
        &target_root,
        &journal_document,
        &mut journal,
        &source_locks,
        maintenance,
        phase_hook,
    ) {
        let error = if journal.phase != MigrationPhase::Complete
            && ensure_source_unchanged(roots, &journal, &source_locks).is_err()
        {
            MigrationError::SourceChanged
        } else {
            error
        };
        // Stage evidence becomes durable only with the Staged checkpoint.
        // Keeping it on a Detected failure would make the recovery journal
        // structurally invalid and prevent the original error from persisting.
        if journal.phase == MigrationPhase::Detected {
            journal.staged_database = None;
        }
        return Err(persist_recovery_error(
            &journal_document,
            &mut journal,
            error,
        ));
    }
    Ok(outcome(&journal))
}

/// Inspect a candidate without following unknown leaves or creating anything.
pub fn source_contains_known_state(path: &Path) -> Result<bool, MigrationError> {
    if !path.is_absolute() {
        return Err(MigrationError::PathInvalid);
    }
    match fs::symlink_metadata(path) {
        Ok(metadata) if path_metadata_is_link(&metadata) || !metadata.file_type().is_dir() => {
            return Err(MigrationError::SourceUnsafe);
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(_) => return Err(MigrationError::SourceReadFailed),
    }
    let source_root = fs::canonicalize(path).map_err(|_| MigrationError::SourceReadFailed)?;
    let roots = MigrationSourceRoots {
        config: source_root.clone(),
        data: source_root,
    };
    match inventory_source(&roots) {
        Ok(_) => Ok(true),
        Err(MigrationError::SourceEmpty) => Ok(false),
        Err(error) => Err(error),
    }
}

/// Return the source recorded by an existing valid journal so callers resume before discovery.
pub fn pending_migration_source(target_root: &Path) -> Result<Option<PathBuf>, MigrationError> {
    let journal = load_existing_journal(target_root)?;
    if journal
        .as_ref()
        .is_some_and(|journal| journal.phase == MigrationPhase::Complete)
    {
        return Ok(None);
    }
    journal
        .map(|journal| PathBuf::from(journal.source_root))
        .map(|source| {
            if source.is_absolute() {
                Ok(source)
            } else {
                Err(MigrationError::JournalInvalid)
            }
        })
        .transpose()
}

fn load_existing_journal(target_root: &Path) -> Result<Option<MigrationJournal>, MigrationError> {
    if !target_root.is_absolute() {
        return Err(MigrationError::PathInvalid);
    }
    match fs::symlink_metadata(target_root) {
        Ok(metadata) if path_metadata_is_link(&metadata) || !metadata.file_type().is_dir() => {
            return Err(MigrationError::TargetUnsafe);
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(MigrationError::TargetUnsafe),
    }
    let state = target_root.join("state");
    match fs::symlink_metadata(&state) {
        Ok(metadata) if path_metadata_is_link(&metadata) || !metadata.file_type().is_dir() => {
            return Err(MigrationError::JournalInvalid);
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(MigrationError::JournalInvalid),
    }
    load_json_document_nofollow(
        &state.join("migration.json"),
        MigrationError::JournalInvalid,
    )
}

/// Resume a journaled migration or select exactly one populated platform candidate.
pub fn migrate_detected_product(
    target_root: &Path,
    legacy_candidates: &[PathBuf],
    maintenance: &dyn GatewayMaintenanceRunner,
) -> Result<Option<MigrationOutcome>, MigrationError> {
    let source_sets = legacy_candidates
        .iter()
        .map(|root| ProductMigrationSourceSet {
            config_root: root.clone(),
            data_root: root.clone(),
        })
        .collect::<Vec<_>>();
    migrate_detected_product_source_sets(target_root, &source_sets, maintenance)
}

pub fn migrate_detected_product_source_sets(
    target_root: &Path,
    legacy_candidates: &[ProductMigrationSourceSet],
    maintenance: &dyn GatewayMaintenanceRunner,
) -> Result<Option<MigrationOutcome>, MigrationError> {
    if !target_root.is_absolute() || legacy_candidates.len() > MAX_LEGACY_ROOT_CANDIDATES {
        return Err(MigrationError::PathInvalid);
    }
    if let Some(journal) = load_existing_journal(target_root)? {
        let config_root = PathBuf::from(journal.source_root);
        let data_root = journal
            .data_source_root
            .map(PathBuf::from)
            .unwrap_or_else(|| config_root.clone());
        if !config_root.is_absolute() || !data_root.is_absolute() {
            return Err(MigrationError::JournalInvalid);
        }
        return run_or_resume_product_migration_source_set(
            &ProductMigrationSourceSet {
                config_root,
                data_root,
            },
            target_root,
            maintenance,
        )
        .map(Some);
    }

    let mut populated = BTreeSet::new();
    for candidate in legacy_candidates {
        if source_set_contains_known_state(candidate)? {
            let roots = resolve_source_roots(candidate)?.ok_or(MigrationError::SourceMissing)?;
            populated.insert(ProductMigrationSourceSet {
                config_root: roots.config,
                data_root: roots.data,
            });
        }
    }
    let mut populated = populated.into_iter();
    let Some(source) = populated.next() else {
        return Ok(None);
    };
    if populated.next().is_some() {
        return Err(MigrationError::SourceAmbiguous);
    }
    run_or_resume_product_migration_source_set(&source, target_root, maintenance).map(Some)
}

fn source_set_contains_known_state(
    source: &ProductMigrationSourceSet,
) -> Result<bool, MigrationError> {
    if !source.config_root.is_absolute() || !source.data_root.is_absolute() {
        return Err(MigrationError::PathInvalid);
    }
    let Some(roots) = resolve_source_roots(source)? else {
        return Ok(false);
    };
    match inventory_source(&roots) {
        Ok(_) => Ok(true),
        Err(MigrationError::SourceEmpty) => Ok(false),
        Err(error) => Err(error),
    }
}

fn advance_transaction(
    source_roots: &MigrationSourceRoots,
    target_root: &Path,
    journal_document: &TypedDocument<MigrationJournal>,
    journal: &mut MigrationJournal,
    source_locks: &[SourceLockGuard],
    maintenance: &dyn GatewayMaintenanceRunner,
    phase_hook: &mut dyn FnMut(MigrationPhase),
) -> Result<(), MigrationError> {
    loop {
        match journal.phase {
            MigrationPhase::Detected => {
                ensure_source_unchanged(source_roots, journal, source_locks)?;
                validate_target_shape(target_root, Some(journal))?;
                stage_entries(source_roots, target_root, journal, maintenance)?;
                ensure_source_unchanged(source_roots, journal, source_locks)?;
                journal.phase = MigrationPhase::Staged;
                journal.recovery_code = None;
                store_journal(journal_document, journal)?;
                phase_hook(MigrationPhase::Staged);
            }
            MigrationPhase::Staged => {
                ensure_source_unchanged(source_roots, journal, source_locks)?;
                verify_stage(target_root, journal)?;
                ensure_source_unchanged(source_roots, journal, source_locks)?;
                journal.phase = MigrationPhase::Verified;
                journal.recovery_code = None;
                store_journal(journal_document, journal)?;
                phase_hook(MigrationPhase::Verified);
            }
            MigrationPhase::Verified => {
                ensure_source_unchanged(source_roots, journal, source_locks)?;
                validate_target_shape(target_root, Some(journal))?;
                activate_entries(target_root, journal)?;
                ensure_source_unchanged(source_roots, journal, source_locks)?;
                journal.phase = MigrationPhase::Activated;
                journal.recovery_code = None;
                store_journal(journal_document, journal)?;
                phase_hook(MigrationPhase::Activated);
            }
            MigrationPhase::Activated => {
                ensure_source_unchanged(source_roots, journal, source_locks)?;
                verify_activated_targets(target_root, journal)?;
                ensure_source_unchanged(source_roots, journal, source_locks)?;
                journal.phase = MigrationPhase::Complete;
                journal.recovery_code = None;
                store_journal(journal_document, journal)?;
                phase_hook(MigrationPhase::Complete);
            }
            MigrationPhase::Complete => {
                cleanup_complete_stage(target_root, journal)?;
                return Ok(());
            }
        }
    }
}

fn persist_recovery_error(
    journal_document: &TypedDocument<MigrationJournal>,
    journal: &mut MigrationJournal,
    error: MigrationError,
) -> MigrationError {
    journal.recovery_code = Some(String::from(error.code()));
    match store_journal(journal_document, journal) {
        Ok(()) => error,
        Err(write_error) => write_error,
    }
}

fn resolve_source_roots(
    source: &ProductMigrationSourceSet,
) -> Result<Option<MigrationSourceRoots>, MigrationError> {
    let config = canonical_optional_source_root(&source.config_root)?;
    let data = canonical_optional_source_root(&source.data_root)?;
    Ok(match (config, data) {
        (Some(config), Some(data)) => Some(MigrationSourceRoots { config, data }),
        (Some(root), None) | (None, Some(root)) => Some(MigrationSourceRoots {
            config: root.clone(),
            data: root,
        }),
        (None, None) => None,
    })
}

fn canonical_optional_source_root(path: &Path) -> Result<Option<PathBuf>, MigrationError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if path_metadata_is_link(&metadata) || !metadata.file_type().is_dir() => {
            Err(MigrationError::SourceUnsafe)
        }
        Ok(_) => fs::canonicalize(path)
            .map(Some)
            .map_err(|_| MigrationError::SourceReadFailed),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(_) => Err(MigrationError::SourceReadFailed),
    }
}

fn reject_target_overlap(source_root: &Path, target: &Path) -> Result<(), MigrationError> {
    let target_identity = match fs::symlink_metadata(target) {
        Ok(metadata) => {
            if path_metadata_is_link(&metadata) || !metadata.file_type().is_dir() {
                return Err(MigrationError::TargetUnsafe);
            }
            fs::canonicalize(target).map_err(|_| MigrationError::TargetUnsafe)?
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let parent = target.parent().ok_or(MigrationError::PathInvalid)?;
            let name = target.file_name().ok_or(MigrationError::PathInvalid)?;
            fs::canonicalize(parent)
                .map_err(|_| MigrationError::TargetUnsafe)?
                .join(name)
        }
        Err(_) => return Err(MigrationError::TargetUnsafe),
    };
    if target_identity.starts_with(source_root) || source_root.starts_with(&target_identity) {
        Err(MigrationError::PathInvalid)
    } else {
        Ok(())
    }
}

fn preflight_existing_target(
    target_root: &Path,
    journal: Option<&MigrationJournal>,
) -> Result<(), MigrationError> {
    let metadata = match fs::symlink_metadata(target_root) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(_) => return Err(MigrationError::TargetUnsafe),
    };
    if path_metadata_is_link(&metadata) || !metadata.file_type().is_dir() {
        return Err(MigrationError::TargetUnsafe);
    }
    let canonical_directories = ["run", "state", "cache", "backups", "logs", "updates"];
    let expected_root_files = journal
        .filter(|journal| journal.phase >= MigrationPhase::Verified)
        .map(|journal| {
            journal
                .entries
                .iter()
                .filter(|entry| entry.kind != MigrationFileKind::Backup)
                .map(|entry| entry.target_relative.as_str())
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    for entry in fs::read_dir(target_root).map_err(|_| MigrationError::TargetUnsafe)? {
        let entry = entry.map_err(|_| MigrationError::TargetUnsafe)?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path).map_err(|_| MigrationError::TargetUnsafe)?;
        if path_metadata_is_link(&metadata) {
            return Err(MigrationError::TargetUnsafe);
        }
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| MigrationError::TargetUnsafe)?;
        let allowed_root_atomic_temp =
            match atomic_temp_destination(&name).and_then(|destination| {
                journal
                    .filter(|journal| journal.phase < MigrationPhase::Activated)
                    .and_then(|journal| {
                        journal.entries.iter().find(|migration_entry| {
                            migration_entry.kind != MigrationFileKind::Backup
                                && migration_entry.target_relative == destination
                        })
                    })
            }) {
                Some(migration_entry) => {
                    inspect_bounded_single_link_file(
                        &path,
                        max_file_bytes(migration_entry.kind),
                        true,
                        MigrationError::TargetUnsafe,
                    )?;
                    true
                }
                None => false,
            };
        if canonical_directories.contains(&name.as_str()) {
            if !metadata.file_type().is_dir() {
                return Err(MigrationError::TargetPopulated);
            }
            if journal.is_none() {
                for child in fs::read_dir(&path).map_err(|_| MigrationError::TargetUnsafe)? {
                    let child = child.map_err(|_| MigrationError::TargetUnsafe)?;
                    let child_metadata = fs::symlink_metadata(child.path())
                        .map_err(|_| MigrationError::TargetUnsafe)?;
                    let allowed_stale_lock = name == "run"
                        && child.file_name() == OsStr::new("migration.lock")
                        && !file_metadata_is_unsafe_link(&child_metadata)
                        && child_metadata.file_type().is_file()
                        && child_metadata.len() == 0;
                    let allowed_empty_stage = name == "cache"
                        && child.file_name() == OsStr::new("migration-stage")
                        && !path_metadata_is_link(&child_metadata)
                        && child_metadata.file_type().is_dir()
                        && fs::read_dir(child.path())
                            .map_err(|_| MigrationError::TargetUnsafe)?
                            .next()
                            .is_none();
                    let allowed_stale_journal_temp = name == "state"
                        && child.file_name().to_str().is_some_and(|child_name| {
                            atomic_temp_destination(child_name) == Some("migration.json")
                        })
                        && inspect_bounded_single_link_file(
                            &child.path(),
                            MAX_JOURNAL_BYTES as u64,
                            true,
                            MigrationError::TargetUnsafe,
                        )
                        .is_ok();
                    if !allowed_stale_lock && !allowed_empty_stage && !allowed_stale_journal_temp {
                        return Err(MigrationError::TargetPopulated);
                    }
                }
            }
        } else if !allowed_root_atomic_temp
            && (!metadata.file_type().is_file() || !expected_root_files.contains(name.as_str()))
        {
            return Err(MigrationError::TargetPopulated);
        }
    }
    Ok(())
}

fn prepare_target_root(target_root: &Path) -> Result<(), MigrationError> {
    ensure_private_directory(target_root, MigrationError::TargetUnsafe)?;
    for relative in [
        "run",
        "state",
        "cache",
        "cache/migration-stage",
        "backups",
        "logs",
        "updates",
    ] {
        ensure_private_directory(&target_root.join(relative), MigrationError::TargetUnsafe)?;
    }
    Ok(())
}

fn ensure_private_directory(path: &Path, error: MigrationError) -> Result<(), MigrationError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if !path_metadata_is_link(&metadata) && metadata.file_type().is_dir() => {}
        Ok(_) => return Err(error),
        Err(io_error) if io_error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir(path).map_err(|_| error)?;
        }
        Err(_) => return Err(error),
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(|_| error)?;
    }
    Ok(())
}

fn acquire_source_locks(source_root: &Path) -> Result<SourceLockGuard, MigrationError> {
    let mut existing = Vec::new();
    let mut absent = Vec::new();
    collect_source_lock_path(&source_root.join("agent.lock"), &mut existing, &mut absent)?;

    let run_directory = source_root.join("run");
    match fs::symlink_metadata(&run_directory) {
        Ok(metadata) if path_metadata_is_link(&metadata) || !metadata.file_type().is_dir() => {
            return Err(MigrationError::SourceUnsafe);
        }
        Ok(_) => collect_source_lock_path(
            &run_directory.join("agent.lock"),
            &mut existing,
            &mut absent,
        )?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            absent.push(run_directory.join("agent.lock"));
        }
        Err(_) => return Err(MigrationError::SourceReadFailed),
    }

    if existing.is_empty() {
        let path = source_root.join("agent.lock");
        let file = match OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)
        {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                collect_source_lock_path(&path, &mut existing, &mut absent)?;
                return lock_existing_source_paths(existing, absent);
            }
            Err(_) => return Err(MigrationError::SourceUnsafe),
        };
        absent.retain(|candidate| candidate != &path);
        let guard = SourceLockGuard {
            held: vec![lock_opened_source_file(path, file)?],
            absent,
        };
        guard.verify()?;
        return Ok(guard);
    }

    lock_existing_source_paths(existing, absent)
}

fn acquire_source_lock_set(
    roots: &MigrationSourceRoots,
) -> Result<Vec<SourceLockGuard>, MigrationError> {
    let paths = BTreeSet::from([roots.config.clone(), roots.data.clone()]);
    let mut locks = Vec::with_capacity(paths.len());
    for path in paths {
        locks.push(acquire_source_locks(&path)?);
    }
    Ok(locks)
}

fn verify_source_lock_set(locks: &[SourceLockGuard]) -> Result<(), MigrationError> {
    for lock in locks {
        lock.verify()?;
    }
    Ok(())
}

fn lock_existing_source_paths(
    existing: Vec<PathBuf>,
    absent: Vec<PathBuf>,
) -> Result<SourceLockGuard, MigrationError> {
    let mut held = Vec::with_capacity(existing.len());
    for path in existing {
        let metadata = fs::symlink_metadata(&path).map_err(|_| MigrationError::SourceChanged)?;
        if file_metadata_is_unsafe_link(&metadata)
            || !metadata.file_type().is_file()
            || metadata.len() != 0
        {
            return Err(MigrationError::SourceUnsafe);
        }
        let file =
            open_existing_file_nofollow(&path, true).map_err(|_| MigrationError::SourceUnsafe)?;
        held.push(lock_opened_source_file(path, file)?);
    }
    let guard = SourceLockGuard { held, absent };
    guard.verify()?;
    Ok(guard)
}

fn lock_opened_source_file(path: PathBuf, file: File) -> Result<HeldSourceLock, MigrationError> {
    let metadata = file.metadata().map_err(|_| MigrationError::SourceUnsafe)?;
    if file_metadata_is_unsafe_link(&metadata)
        || !metadata.file_type().is_file()
        || metadata.len() != 0
        || opened_file_has_unexpected_link_count(&file).map_err(|_| MigrationError::SourceUnsafe)?
    {
        return Err(MigrationError::SourceUnsafe);
    }
    let inspection = file.try_clone().map_err(|_| MigrationError::SourceUnsafe)?;
    let identity = Handle::from_file(
        inspection
            .try_clone()
            .map_err(|_| MigrationError::SourceUnsafe)?,
    )
    .map_err(|_| MigrationError::SourceUnsafe)?;
    let lock = ExclusiveFileLock::try_acquire_opened(&path, file).map_err(map_source_lock_error)?;
    Ok(HeldSourceLock {
        path,
        identity,
        inspection,
        _lock: lock,
    })
}

fn collect_source_lock_path(
    path: &Path,
    existing: &mut Vec<PathBuf>,
    absent: &mut Vec<PathBuf>,
) -> Result<(), MigrationError> {
    match fs::symlink_metadata(path) {
        Ok(metadata)
            if file_metadata_is_unsafe_link(&metadata)
                || !metadata.file_type().is_file()
                || metadata.len() != 0 =>
        {
            Err(MigrationError::SourceUnsafe)
        }
        Ok(_) => {
            absent.retain(|candidate| candidate != path);
            existing.push(path.to_path_buf());
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            absent.push(path.to_path_buf());
            Ok(())
        }
        Err(_) => Err(MigrationError::SourceReadFailed),
    }
}

fn inventory_source(roots: &MigrationSourceRoots) -> Result<Vec<MigrationEntry>, MigrationError> {
    let mut entries = Vec::new();
    select_alias(
        &roots.config,
        &["config.toml", "agent.toml"],
        "config.toml",
        MigrationFileKind::Config,
        MigrationSourceKind::Config,
        &mut entries,
    )?;
    add_known_file(
        &roots.config,
        "secrets.json",
        "secrets.json",
        MigrationFileKind::Secrets,
        MigrationSourceKind::Config,
        &mut entries,
    )?;
    let data_source_kind = if roots.config == roots.data {
        MigrationSourceKind::Config
    } else {
        MigrationSourceKind::Data
    };
    select_alias(
        &roots.data,
        &["cmclient.db", "gateway.sqlite"],
        "cmclient.db",
        MigrationFileKind::Database,
        data_source_kind,
        &mut entries,
    )?;
    inventory_backups(&roots.data, data_source_kind, &mut entries)?;
    if entries.is_empty() {
        return Err(MigrationError::SourceEmpty);
    }
    if entries.len() > MAX_MIGRATION_FILES {
        return Err(MigrationError::SourceTooManyFiles);
    }
    entries.sort_by(|left, right| left.target_relative.cmp(&right.target_relative));
    let total = entries.iter().try_fold(0_u64, |total, entry| {
        let total = total
            .checked_add(entry.size_bytes)
            .ok_or(MigrationError::SourceTooLarge)?;
        entry.database_wal.as_ref().map_or(Ok(total), |wal| {
            total
                .checked_add(wal.size_bytes)
                .ok_or(MigrationError::SourceTooLarge)
        })
    })?;
    if total > MAX_TOTAL_BYTES {
        return Err(MigrationError::SourceTooLarge);
    }
    Ok(entries)
}

fn select_alias(
    source_root: &Path,
    names: &[&str],
    target_relative: &str,
    kind: MigrationFileKind,
    source_kind: MigrationSourceKind,
    entries: &mut Vec<MigrationEntry>,
) -> Result<(), MigrationError> {
    let present = names
        .iter()
        .filter_map(|name| match known_path_exists(&source_root.join(name)) {
            Ok(true) => Some(Ok(*name)),
            Ok(false) => None,
            Err(error) => Some(Err(error)),
        })
        .collect::<Result<Vec<_>, _>>()?;
    if present.len() > 1 {
        return Err(MigrationError::SourceAmbiguous);
    }
    if let Some(name) = present.first() {
        add_known_file(
            source_root,
            name,
            target_relative,
            kind,
            source_kind,
            entries,
        )?;
    }
    Ok(())
}

fn add_known_file(
    source_root: &Path,
    source_relative: &str,
    target_relative: &str,
    kind: MigrationFileKind,
    source_kind: MigrationSourceKind,
    entries: &mut Vec<MigrationEntry>,
) -> Result<(), MigrationError> {
    let path = source_root.join(source_relative);
    if !known_path_exists(&path)? {
        return Ok(());
    }
    let (size_bytes, sha256, identity) = fingerprint_file(&path, kind)?;
    let (database_wal, database_shm) = if kind == MigrationFileKind::Database {
        inventory_database_sidecars(source_root, source_relative)?
    } else {
        (None, None)
    };
    entries.push(MigrationEntry {
        kind,
        source_kind,
        source_relative: String::from(source_relative),
        target_relative: String::from(target_relative),
        size_bytes,
        sha256,
        identity,
        database_wal,
        database_shm,
    });
    Ok(())
}

fn inventory_database_sidecars(
    source_root: &Path,
    database_relative: &str,
) -> Result<(Option<SourceFileEvidence>, Option<SourceTopologyEvidence>), MigrationError> {
    let wal_relative = format!("{database_relative}-wal");
    let wal_path = source_root.join(&wal_relative);
    let wal = match fs::symlink_metadata(&wal_path) {
        Ok(metadata) => {
            if file_metadata_is_unsafe_link(&metadata) || !metadata.file_type().is_file() {
                return Err(MigrationError::SourceUnsafe);
            }
            let (size_bytes, sha256, identity) =
                fingerprint_source_file_bounded(&wal_path, MAX_DATABASE_WAL_BYTES, true)?;
            Some(SourceFileEvidence {
                source_relative: wal_relative,
                size_bytes,
                sha256,
                identity,
            })
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(_) => return Err(MigrationError::SourceReadFailed),
    };

    let shared_memory_relative = format!("{database_relative}-shm");
    let shared_memory_path = source_root.join(&shared_memory_relative);
    let shared_memory = match fs::symlink_metadata(&shared_memory_path) {
        Ok(metadata) => {
            if file_metadata_is_unsafe_link(&metadata)
                || !metadata.file_type().is_file()
                || metadata.len() == 0
                || metadata.len() > MAX_DATABASE_SHM_BYTES
            {
                return Err(if metadata.len() > MAX_DATABASE_SHM_BYTES {
                    MigrationError::SourceFileTooLarge
                } else {
                    MigrationError::SourceUnsafe
                });
            }
            let identity = source_topology_identity(&shared_memory_path, MAX_DATABASE_SHM_BYTES)?;
            Some(SourceTopologyEvidence {
                source_relative: shared_memory_relative,
                identity,
            })
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(_) => return Err(MigrationError::SourceReadFailed),
    };
    Ok((wal, shared_memory))
}

fn inventory_backups(
    source_root: &Path,
    source_kind: MigrationSourceKind,
    entries: &mut Vec<MigrationEntry>,
) -> Result<(), MigrationError> {
    let backups = source_root.join("backups");
    let metadata = match fs::symlink_metadata(&backups) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(_) => return Err(MigrationError::SourceReadFailed),
    };
    if path_metadata_is_link(&metadata) || !metadata.file_type().is_dir() {
        return Err(MigrationError::SourceUnsafe);
    }
    let mut names = Vec::new();
    let mut scanned = 0_usize;
    let mut directories = vec![(backups, String::new(), 0_usize)];
    while let Some((directory, prefix, depth)) = directories.pop() {
        for directory_entry in
            fs::read_dir(&directory).map_err(|_| MigrationError::SourceReadFailed)?
        {
            scanned += 1;
            if scanned > MAX_BACKUP_SCAN_ENTRIES {
                return Err(MigrationError::SourceTooManyFiles);
            }
            let directory_entry = directory_entry.map_err(|_| MigrationError::SourceReadFailed)?;
            let path = directory_entry.path();
            let metadata =
                fs::symlink_metadata(&path).map_err(|_| MigrationError::SourceReadFailed)?;
            if path_metadata_is_link(&metadata)
                || (metadata.file_type().is_file()
                    && (metadata_has_unexpected_link_count(&metadata)))
            {
                return Err(MigrationError::SourceUnsafe);
            }
            let name = directory_entry
                .file_name()
                .into_string()
                .map_err(|_| MigrationError::SourceUnsafe)?;
            if !is_safe_backup_segment(&name) {
                return Err(MigrationError::SourceUnsafe);
            }
            let relative = if prefix.is_empty() {
                name
            } else {
                format!("{prefix}/{name}")
            };
            if metadata.file_type().is_dir() {
                if depth >= MAX_BACKUP_DEPTH {
                    return Err(MigrationError::SourceTooManyFiles);
                }
                directories.push((path, relative, depth + 1));
            } else if metadata.file_type().is_file() {
                if is_backup_file_extension(&relative) {
                    if !is_safe_backup_relative(&relative) {
                        return Err(MigrationError::SourceUnsafe);
                    }
                    names.push(relative);
                    if names.len() > MAX_BACKUP_FILES {
                        return Err(MigrationError::SourceTooManyFiles);
                    }
                }
            } else {
                return Err(MigrationError::SourceUnsafe);
            }
        }
    }
    names.sort_by_key(|name| name.to_ascii_lowercase());
    let mut normalized = BTreeSet::new();
    for name in names {
        if !normalized.insert(name.to_ascii_lowercase()) {
            return Err(MigrationError::SourceAmbiguous);
        }
        add_known_file(
            source_root,
            &format!("backups/{name}"),
            &format!("backups/{name}"),
            MigrationFileKind::Backup,
            source_kind,
            entries,
        )?;
    }
    Ok(())
}

fn known_path_exists(path: &Path) -> Result<bool, MigrationError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if file_metadata_is_unsafe_link(&metadata) => {
            Err(MigrationError::SourceUnsafe)
        }
        Ok(metadata) if metadata.file_type().is_file() => Ok(true),
        Ok(_) => Err(MigrationError::SourceUnsafe),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(_) => Err(MigrationError::SourceReadFailed),
    }
}

fn fingerprint_file(
    path: &Path,
    kind: MigrationFileKind,
) -> Result<(u64, String, SourceFileIdentity), MigrationError> {
    fingerprint_source_file_bounded(path, max_file_bytes(kind), false)
}

fn fingerprint_source_file_bounded(
    path: &Path,
    max_bytes: u64,
    allow_empty: bool,
) -> Result<(u64, String, SourceFileIdentity), MigrationError> {
    let metadata = fs::symlink_metadata(path).map_err(|_| MigrationError::SourceReadFailed)?;
    if file_metadata_is_unsafe_link(&metadata) || !metadata.file_type().is_file() {
        return Err(MigrationError::SourceUnsafe);
    }
    if (!allow_empty && metadata.len() == 0) || metadata.len() > max_bytes {
        return Err(MigrationError::SourceFileTooLarge);
    }
    let mut file =
        open_existing_file_nofollow(path, false).map_err(|_| MigrationError::SourceReadFailed)?;
    if opened_file_has_unexpected_link_count(&file).map_err(|_| MigrationError::SourceUnsafe)? {
        return Err(MigrationError::SourceUnsafe);
    }
    let opened = Handle::from_file(
        file.try_clone()
            .map_err(|_| MigrationError::SourceReadFailed)?,
    )
    .map_err(|_| MigrationError::SourceReadFailed)?;
    let current = file_handle_nofollow(path).map_err(|_| MigrationError::SourceReadFailed)?;
    if opened != current {
        return Err(MigrationError::SourceChanged);
    }
    let identity = source_file_identity(&file)?;
    let mut digest = Sha256::new();
    let mut bytes = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|_| MigrationError::SourceReadFailed)?;
        if read == 0 {
            break;
        }
        bytes = bytes
            .checked_add(read as u64)
            .ok_or(MigrationError::SourceFileTooLarge)?;
        if bytes > max_bytes {
            return Err(MigrationError::SourceFileTooLarge);
        }
        digest.update(&buffer[..read]);
    }
    let final_metadata = file.metadata().map_err(|_| MigrationError::SourceChanged)?;
    if file_metadata_is_unsafe_link(&final_metadata)
        || opened_file_has_unexpected_link_count(&file)
            .map_err(|_| MigrationError::SourceChanged)?
        || bytes != metadata.len()
        || final_metadata.len() != metadata.len()
        || file_handle_nofollow(path).map_err(|_| MigrationError::SourceChanged)? != opened
    {
        return Err(MigrationError::SourceChanged);
    }
    if source_file_identity(&file)? != identity {
        return Err(MigrationError::SourceChanged);
    }
    Ok((bytes, hex_digest(digest.finalize()), identity))
}

#[cfg(unix)]
fn source_file_identity(file: &File) -> Result<SourceFileIdentity, MigrationError> {
    use std::os::unix::fs::MetadataExt;

    let metadata = file
        .metadata()
        .map_err(|_| MigrationError::SourceReadFailed)?;
    Ok(SourceFileIdentity {
        device_id: metadata.dev(),
        file_id: metadata.ino(),
    })
}

#[cfg(windows)]
fn source_file_identity(file: &File) -> Result<SourceFileIdentity, MigrationError> {
    let information =
        winapi_util::file::information(file).map_err(|_| MigrationError::SourceReadFailed)?;
    Ok(SourceFileIdentity {
        device_id: information.volume_serial_number(),
        file_id: information.file_index(),
    })
}

#[cfg(not(any(unix, windows)))]
fn source_file_identity(_file: &File) -> Result<SourceFileIdentity, MigrationError> {
    Err(MigrationError::SourceReadFailed)
}

fn source_topology_identity(
    path: &Path,
    max_bytes: u64,
) -> Result<SourceFileIdentity, MigrationError> {
    let metadata = fs::symlink_metadata(path).map_err(|_| MigrationError::SourceReadFailed)?;
    if file_metadata_is_unsafe_link(&metadata)
        || !metadata.file_type().is_file()
        || metadata.len() == 0
        || metadata.len() > max_bytes
    {
        return Err(MigrationError::SourceUnsafe);
    }
    let file =
        open_existing_file_nofollow(path, false).map_err(|_| MigrationError::SourceReadFailed)?;
    if opened_file_has_unexpected_link_count(&file).map_err(|_| MigrationError::SourceUnsafe)? {
        return Err(MigrationError::SourceUnsafe);
    }
    let opened = Handle::from_file(
        file.try_clone()
            .map_err(|_| MigrationError::SourceReadFailed)?,
    )
    .map_err(|_| MigrationError::SourceReadFailed)?;
    if file_handle_nofollow(path).map_err(|_| MigrationError::SourceChanged)? != opened {
        return Err(MigrationError::SourceChanged);
    }
    let identity = source_file_identity(&file)?;
    let final_metadata = file.metadata().map_err(|_| MigrationError::SourceChanged)?;
    if file_metadata_is_unsafe_link(&final_metadata)
        || final_metadata.len() == 0
        || final_metadata.len() > max_bytes
        || opened_file_has_unexpected_link_count(&file)
            .map_err(|_| MigrationError::SourceChanged)?
        || file_handle_nofollow(path).map_err(|_| MigrationError::SourceChanged)? != opened
        || source_file_identity(&file)? != identity
    {
        return Err(MigrationError::SourceChanged);
    }
    Ok(identity)
}

fn ensure_source_unchanged(
    source_roots: &MigrationSourceRoots,
    journal: &MigrationJournal,
    source_locks: &[SourceLockGuard],
) -> Result<(), MigrationError> {
    verify_source_lock_set(source_locks)?;
    let unchanged =
        matches!(inventory_source(source_roots), Ok(entries) if entries == journal.entries);
    verify_source_lock_set(source_locks)?;
    if unchanged {
        Ok(())
    } else {
        Err(MigrationError::SourceChanged)
    }
}

fn stage_entries(
    source_roots: &MigrationSourceRoots,
    target_root: &Path,
    journal: &mut MigrationJournal,
    maintenance: &dyn GatewayMaintenanceRunner,
) -> Result<(), MigrationError> {
    let stage = stage_directory(target_root, journal);
    ensure_private_directory(&stage, MigrationError::StageFailed)?;
    validate_stage_shape(&stage, journal, true)?;
    journal.staged_database = None;
    for entry in &journal.entries {
        if entry.kind == MigrationFileKind::Database {
            continue;
        }
        let source = source_roots.source_for(entry).join(&entry.source_relative);
        let destination = stage.join(&entry.target_relative);
        ensure_stage_parent(&stage, &destination)?;
        copy_file_atomic(
            &source,
            &destination,
            entry.size_bytes,
            &entry.sha256,
            MigrationError::StageFailed,
        )?;
    }
    if let Some(database) = journal
        .entries
        .iter()
        .find(|entry| entry.kind == MigrationFileKind::Database)
    {
        let source = source_roots
            .source_for(database)
            .join(&database.source_relative);
        let work = stage.join(".cmclient.db.maintenance");
        remove_stale_work_file(&work)?;
        let report = maintenance.migrate_database(&source, &work)?;
        if !report.is_valid() || report.source_database_sha256 != database.sha256 {
            return Err(MigrationError::MaintenanceReportInvalid);
        }
        let (work_size, work_sha256) = fingerprint_staged_file(&work, MAX_DATABASE_BYTES)?;
        if report.staged_database_bytes != work_size || report.staged_database_sha256 != work_sha256
        {
            return Err(MigrationError::MaintenanceReportInvalid);
        }
        let destination = stage.join("cmclient.db");
        copy_file_atomic(
            &work,
            &destination,
            work_size,
            &work_sha256,
            MigrationError::StageFailed,
        )?;
        fs::remove_file(&work).map_err(|_| MigrationError::StageFailed)?;
        let report_document =
            TypedDocument::<GatewayMaintenanceReport>::new(stage.join("maintenance-report.json"))
                .map_err(|_| MigrationError::MaintenanceReportInvalid)?;
        report_document
            .store(&report)
            .map_err(|_| MigrationError::MaintenanceReportInvalid)?;
        let report_sha256 = hash_file_bounded(
            report_document.path(),
            MAX_MAINTENANCE_REPORT_BYTES as u64,
            MigrationError::MaintenanceReportInvalid,
        )?
        .1;
        journal.staged_database = Some(StagedDatabaseEvidence {
            size_bytes: work_size,
            sha256: work_sha256,
            report_sha256,
        });
    }
    Ok(())
}

fn ensure_stage_parent(stage: &Path, destination: &Path) -> Result<(), MigrationError> {
    let parent = destination.parent().ok_or(MigrationError::StageFailed)?;
    ensure_nested_parent(stage, parent, MigrationError::StageFailed)
}

fn ensure_nested_parent(
    root: &Path,
    parent: &Path,
    error: MigrationError,
) -> Result<(), MigrationError> {
    if !parent.starts_with(root) {
        return Err(error);
    }
    let relative = parent.strip_prefix(root).map_err(|_| error)?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        let std::path::Component::Normal(segment) = component else {
            return Err(error);
        };
        current.push(segment);
        ensure_private_directory(&current, error)?;
    }
    Ok(())
}

fn remove_stale_work_file(path: &Path) -> Result<(), MigrationError> {
    match fs::symlink_metadata(path) {
        Ok(metadata)
            if !file_metadata_is_unsafe_link(&metadata) && metadata.file_type().is_file() =>
        {
            fs::remove_file(path).map_err(|_| MigrationError::StageFailed)
        }
        Ok(_) => Err(MigrationError::StageFailed),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(MigrationError::StageFailed),
    }
}

fn verify_stage(target_root: &Path, journal: &MigrationJournal) -> Result<(), MigrationError> {
    let stage = stage_directory(target_root, journal);
    validate_stage_shape(&stage, journal, false)?;
    for entry in &journal.entries {
        let (expected_size, expected_sha256) = staged_expectation(entry, journal)?;
        let (size, sha256) = fingerprint_staged_file(
            &stage.join(&entry.target_relative),
            max_file_bytes(entry.kind),
        )?;
        if size != expected_size || sha256 != expected_sha256 {
            return Err(MigrationError::StageDigestMismatch);
        }
        match entry.kind {
            MigrationFileKind::Config => validate_config(&stage.join(&entry.target_relative))?,
            MigrationFileKind::Secrets => validate_secrets(&stage.join(&entry.target_relative))?,
            MigrationFileKind::Database | MigrationFileKind::Backup => {}
        }
    }
    if let Some(evidence) = journal.staged_database.as_ref() {
        let report_path = stage.join("maintenance-report.json");
        let report_sha256 = hash_file_bounded(
            &report_path,
            MAX_MAINTENANCE_REPORT_BYTES as u64,
            MigrationError::StageDigestMismatch,
        )?
        .1;
        if report_sha256 != evidence.report_sha256 {
            return Err(MigrationError::StageDigestMismatch);
        }
        load_json_document_nofollow::<GatewayMaintenanceReport>(
            &report_path,
            MigrationError::MaintenanceReportInvalid,
        )?
        .ok_or(MigrationError::MaintenanceReportInvalid)?;
    }
    Ok(())
}

fn validate_stage_shape(
    stage: &Path,
    journal: &MigrationJournal,
    allow_partial: bool,
) -> Result<(), MigrationError> {
    assert_safe_directory(stage, MigrationError::StageDigestMismatch)?;
    let mut expected = journal
        .entries
        .iter()
        .map(|entry| entry.target_relative.clone())
        .collect::<BTreeSet<_>>();
    if journal
        .entries
        .iter()
        .any(|entry| entry.kind == MigrationFileKind::Database)
    {
        expected.insert(String::from("maintenance-report.json"));
        if allow_partial {
            expected.insert(String::from(".cmclient.db.maintenance"));
        }
    }
    let maintenance_work = stage.join(GATEWAY_MAINTENANCE_WORK_DIRECTORY);
    let ignore = validate_gateway_maintenance_work_directory(
        &maintenance_work,
        allow_partial
            && journal
                .entries
                .iter()
                .any(|entry| entry.kind == MigrationFileKind::Database),
    )?
    .then_some(maintenance_work.as_path());
    let actual = collect_relative_files(stage, ignore)?;
    if actual.iter().any(|path| !expected.contains(path)) || (!allow_partial && actual != expected)
    {
        return Err(MigrationError::StageDigestMismatch);
    }
    Ok(())
}

fn collect_relative_files(
    root: &Path,
    ignored_directory: Option<&Path>,
) -> Result<BTreeSet<String>, MigrationError> {
    let mut files = BTreeSet::new();
    let mut directories = vec![(root.to_path_buf(), String::new())];
    while let Some((directory, prefix)) = directories.pop() {
        for entry in fs::read_dir(&directory).map_err(|_| MigrationError::StageDigestMismatch)? {
            let entry = entry.map_err(|_| MigrationError::StageDigestMismatch)?;
            if ignored_directory.is_some_and(|ignored| entry.path() == ignored) {
                continue;
            }
            let metadata = fs::symlink_metadata(entry.path())
                .map_err(|_| MigrationError::StageDigestMismatch)?;
            if path_metadata_is_link(&metadata)
                || (metadata.file_type().is_file() && metadata_has_unexpected_link_count(&metadata))
            {
                return Err(MigrationError::StageDigestMismatch);
            }
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| MigrationError::StageDigestMismatch)?;
            let relative = if prefix.is_empty() {
                name
            } else {
                format!("{prefix}/{name}")
            };
            if metadata.file_type().is_dir() {
                if !is_safe_backup_directory_relative(&relative) {
                    return Err(MigrationError::StageDigestMismatch);
                }
                directories.push((entry.path(), relative));
            } else if metadata.file_type().is_file() {
                files.insert(relative);
            } else {
                return Err(MigrationError::StageDigestMismatch);
            }
        }
    }
    Ok(files)
}

fn validate_gateway_maintenance_work_directory(
    work: &Path,
    allowed: bool,
) -> Result<bool, MigrationError> {
    let metadata = match fs::symlink_metadata(work) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(_) => return Err(MigrationError::StageDigestMismatch),
    };
    if !allowed || path_metadata_is_link(&metadata) || !metadata.file_type().is_dir() {
        return Err(MigrationError::StageDigestMismatch);
    }
    let work_identity = Handle::from_path(work).map_err(|_| MigrationError::StageDigestMismatch)?;
    let expected = BTreeMap::from([
        ("source.sqlite", MAX_DATABASE_BYTES),
        ("source.sqlite-wal", MAX_DATABASE_WAL_BYTES),
        ("source.sqlite-shm", MAX_DATABASE_SHM_BYTES),
        ("source.sqlite-journal", MAX_DATABASE_BYTES),
        ("staged.sqlite", MAX_DATABASE_BYTES),
        ("staged.sqlite-wal", MAX_DATABASE_WAL_BYTES),
        ("staged.sqlite-shm", MAX_DATABASE_SHM_BYTES),
        ("staged.sqlite-journal", MAX_DATABASE_BYTES),
    ]);
    let mut count = 0_usize;
    for entry in fs::read_dir(work).map_err(|_| MigrationError::StageDigestMismatch)? {
        count += 1;
        if count > expected.len() {
            return Err(MigrationError::StageDigestMismatch);
        }
        let entry = entry.map_err(|_| MigrationError::StageDigestMismatch)?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| MigrationError::StageDigestMismatch)?;
        let max = expected
            .get(name.as_str())
            .ok_or(MigrationError::StageDigestMismatch)?;
        inspect_bounded_single_link_file(
            &entry.path(),
            *max,
            true,
            MigrationError::StageDigestMismatch,
        )?;
    }
    if Handle::from_path(work).map_err(|_| MigrationError::StageDigestMismatch)? != work_identity {
        return Err(MigrationError::StageDigestMismatch);
    }
    Ok(true)
}

fn cleanup_complete_stage(
    target_root: &Path,
    journal: &MigrationJournal,
) -> Result<(), MigrationError> {
    let stage = stage_directory(target_root, journal);
    let Some(stage_chain) =
        verify_directory_chain(target_root, &stage, MigrationError::CleanupFailed)?
    else {
        return Ok(());
    };
    let stage_identity = Handle::from_path(&stage).map_err(|_| MigrationError::CleanupFailed)?;
    let expected = journal
        .entries
        .iter()
        .map(|entry| (entry.target_relative.as_str(), entry))
        .collect::<BTreeMap<_, _>>();
    let actual = collect_relative_files(&stage, None).map_err(|_| MigrationError::CleanupFailed)?;
    if actual.iter().any(|relative| {
        relative != "maintenance-report.json" && !expected.contains_key(relative.as_str())
    }) || (actual.contains("maintenance-report.json") && journal.staged_database.is_none())
    {
        return Err(MigrationError::CleanupFailed);
    }

    let mut actual = actual.into_iter().collect::<Vec<_>>();
    actual.sort_by_key(|relative| relative != "secrets.json");
    for relative in actual {
        let path = stage.join(&relative);
        if relative == "maintenance-report.json" {
            let evidence = journal
                .staged_database
                .as_ref()
                .ok_or(MigrationError::CleanupFailed)?;
            remove_verified_owned_file(
                &stage,
                &path,
                MAX_MAINTENANCE_REPORT_BYTES as u64,
                None,
                &evidence.report_sha256,
            )?;
        } else {
            let entry = expected
                .get(relative.as_str())
                .ok_or(MigrationError::CleanupFailed)?;
            let (size, sha256) =
                staged_expectation(entry, journal).map_err(|_| MigrationError::CleanupFailed)?;
            remove_verified_owned_file(
                &stage,
                &path,
                max_file_bytes(entry.kind),
                Some(size),
                &sha256,
            )?;
        }
    }

    let mut directories = BTreeSet::new();
    for entry in &journal.entries {
        let mut parent = Path::new(&entry.target_relative).parent();
        while let Some(relative) = parent {
            if relative.as_os_str().is_empty() {
                break;
            }
            directories.insert(stage.join(relative));
            parent = relative.parent();
        }
    }
    let mut directories = directories.into_iter().collect::<Vec<_>>();
    directories.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    for directory in directories {
        remove_verified_empty_directory(&stage, &directory)?;
    }

    verify_directory_chain_unchanged(&stage_chain, MigrationError::CleanupFailed)?;
    let current = Handle::from_path(&stage).map_err(|_| MigrationError::CleanupFailed)?;
    if current != stage_identity {
        return Err(MigrationError::CleanupFailed);
    }
    drop(current);
    drop(stage_identity);
    drop(stage_chain);
    fs::remove_dir(stage).map_err(|_| MigrationError::CleanupFailed)
}

fn remove_verified_owned_file(
    trusted_root: &Path,
    path: &Path,
    max: u64,
    expected_size: Option<u64>,
    expected_sha256: &str,
) -> Result<(), MigrationError> {
    let parent = path.parent().ok_or(MigrationError::CleanupFailed)?;
    let directory_chain =
        verify_directory_chain(trusted_root, parent, MigrationError::CleanupFailed)?
            .ok_or(MigrationError::CleanupFailed)?;
    let (size, sha256, identity) =
        hash_file_bounded_with_identity(path, max, MigrationError::CleanupFailed)?;
    if expected_size.is_some_and(|expected| expected != size) || sha256 != expected_sha256 {
        return Err(MigrationError::CleanupFailed);
    }
    verify_directory_chain_unchanged(&directory_chain, MigrationError::CleanupFailed)?;
    let current = file_handle_nofollow(path).map_err(|_| MigrationError::CleanupFailed)?;
    if current != identity {
        return Err(MigrationError::CleanupFailed);
    }
    verify_directory_chain_unchanged(&directory_chain, MigrationError::CleanupFailed)?;
    drop(current);
    drop(identity);
    fs::remove_file(path).map_err(|_| MigrationError::CleanupFailed)?;
    verify_directory_chain_unchanged(&directory_chain, MigrationError::CleanupFailed)
}

fn remove_verified_empty_directory(trusted_root: &Path, path: &Path) -> Result<(), MigrationError> {
    let parent = path.parent().ok_or(MigrationError::CleanupFailed)?;
    let directory_chain =
        verify_directory_chain(trusted_root, parent, MigrationError::CleanupFailed)?
            .ok_or(MigrationError::CleanupFailed)?;
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(_) => return Err(MigrationError::CleanupFailed),
    };
    if path_metadata_is_link(&metadata) || !metadata.file_type().is_dir() {
        return Err(MigrationError::CleanupFailed);
    }
    let identity = Handle::from_path(path).map_err(|_| MigrationError::CleanupFailed)?;
    let current = Handle::from_path(path).map_err(|_| MigrationError::CleanupFailed)?;
    if current != identity {
        return Err(MigrationError::CleanupFailed);
    }
    verify_directory_chain_unchanged(&directory_chain, MigrationError::CleanupFailed)?;
    drop(current);
    drop(identity);
    fs::remove_dir(path).map_err(|_| MigrationError::CleanupFailed)?;
    verify_directory_chain_unchanged(&directory_chain, MigrationError::CleanupFailed)
}

fn validate_config(path: &Path) -> Result<(), MigrationError> {
    validate_migrated_agent_config(path).map_err(|_| MigrationError::ConfigInvalid)
}

fn validate_secrets(path: &Path) -> Result<(), MigrationError> {
    validate_migrated_plaintext_secrets(path).map_err(|_| MigrationError::SecretsInvalid)
}

fn activate_entries(target_root: &Path, journal: &MigrationJournal) -> Result<(), MigrationError> {
    verify_stage(target_root, journal)?;
    let stage = stage_directory(target_root, journal);
    for entry in &journal.entries {
        let source = stage.join(&entry.target_relative);
        let destination = target_root.join(&entry.target_relative);
        let (size, sha256) = staged_expectation(entry, journal)?;
        if destination
            .try_exists()
            .map_err(|_| MigrationError::TargetPopulated)?
        {
            let actual = fingerprint_target_file(&destination, max_file_bytes(entry.kind))?;
            if actual != (size, sha256.clone()) {
                return Err(MigrationError::TargetPopulated);
            }
            continue;
        }
        let parent = destination
            .parent()
            .ok_or(MigrationError::ActivationFailed)?;
        ensure_nested_parent(target_root, parent, MigrationError::ActivationFailed)?;
        copy_file_atomic(
            &source,
            &destination,
            size,
            &sha256,
            MigrationError::ActivationFailed,
        )?;
    }
    verify_activated_targets(target_root, journal)
}

fn verify_activated_targets(
    target_root: &Path,
    journal: &MigrationJournal,
) -> Result<(), MigrationError> {
    for entry in &journal.entries {
        let expected = staged_expectation(entry, journal)?;
        let actual = fingerprint_target_file(
            &target_root.join(&entry.target_relative),
            max_file_bytes(entry.kind),
        )?;
        if actual != expected {
            return Err(MigrationError::ActivationFailed);
        }
    }
    Ok(())
}

fn staged_expectation(
    entry: &MigrationEntry,
    journal: &MigrationJournal,
) -> Result<(u64, String), MigrationError> {
    if entry.kind == MigrationFileKind::Database {
        let evidence = journal
            .staged_database
            .as_ref()
            .ok_or(MigrationError::MaintenanceReportInvalid)?;
        Ok((evidence.size_bytes, evidence.sha256.clone()))
    } else {
        Ok((entry.size_bytes, entry.sha256.clone()))
    }
}

fn copy_file_atomic(
    source: &Path,
    destination: &Path,
    expected_size: u64,
    expected_sha256: &str,
    error: MigrationError,
) -> Result<(), MigrationError> {
    let metadata = fs::symlink_metadata(source).map_err(|_| error)?;
    if file_metadata_is_unsafe_link(&metadata) || !metadata.file_type().is_file() {
        return Err(error);
    }
    let mut source_file = open_existing_file_nofollow(source, false).map_err(|_| error)?;
    if opened_file_has_unexpected_link_count(&source_file).map_err(|_| error)? {
        return Err(error);
    }
    let opened =
        Handle::from_file(source_file.try_clone().map_err(|_| error)?).map_err(|_| error)?;
    if file_handle_nofollow(source).map_err(|_| error)? != opened {
        return Err(error);
    }
    let mut destination_file = AtomicWriteFile::open(destination).map_err(|_| error)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        destination_file
            .as_file()
            .set_permissions(fs::Permissions::from_mode(0o600))
            .map_err(|_| error)?;
    }
    let mut digest = Sha256::new();
    let mut size = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = source_file.read(&mut buffer).map_err(|_| error)?;
        if read == 0 {
            break;
        }
        size = size.checked_add(read as u64).ok_or(error)?;
        if size > expected_size {
            return Err(error);
        }
        digest.update(&buffer[..read]);
        destination_file
            .write_all(&buffer[..read])
            .map_err(|_| error)?;
    }
    let final_metadata = source_file.metadata().map_err(|_| error)?;
    if file_metadata_is_unsafe_link(&final_metadata)
        || opened_file_has_unexpected_link_count(&source_file).map_err(|_| error)?
        || size != expected_size
        || hex_digest(digest.finalize()) != expected_sha256
        || file_handle_nofollow(source).map_err(|_| error)? != opened
    {
        return Err(error);
    }
    destination_file.commit().map_err(|_| error)
}

fn validate_target_shape(
    target_root: &Path,
    journal: Option<&MigrationJournal>,
) -> Result<(), MigrationError> {
    assert_safe_directory(target_root, MigrationError::TargetUnsafe)?;
    let expected_root_files = journal
        .filter(|journal| journal.phase >= MigrationPhase::Verified)
        .map(|journal| {
            journal
                .entries
                .iter()
                .filter(|entry| entry.kind != MigrationFileKind::Backup)
                .map(|entry| entry.target_relative.as_str())
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    for entry in fs::read_dir(target_root).map_err(|_| MigrationError::TargetUnsafe)? {
        let entry = entry.map_err(|_| MigrationError::TargetUnsafe)?;
        let metadata =
            fs::symlink_metadata(entry.path()).map_err(|_| MigrationError::TargetUnsafe)?;
        if path_metadata_is_link(&metadata)
            || (metadata.file_type().is_file() && metadata_has_unexpected_link_count(&metadata))
        {
            return Err(MigrationError::TargetUnsafe);
        }
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| MigrationError::TargetUnsafe)?;
        if ["run", "state", "cache", "backups", "logs", "updates"].contains(&name.as_str()) {
            if !metadata.file_type().is_dir() {
                return Err(MigrationError::TargetUnsafe);
            }
        } else if !metadata.file_type().is_file() || !expected_root_files.contains(name.as_str()) {
            return Err(MigrationError::TargetPopulated);
        }
    }
    assert_named_directory_entries(&target_root.join("run"), &["migration.lock"])?;
    let state_allowed = if journal.is_some() {
        vec!["migration.json"]
    } else {
        Vec::new()
    };
    assert_named_directory_entries(&target_root.join("state"), &state_allowed)?;
    assert_named_directory_entries(&target_root.join("logs"), &[])?;
    assert_named_directory_entries(&target_root.join("updates"), &[])?;
    assert_cache_shape(target_root, journal)?;
    assert_backup_targets(target_root, journal)?;

    if let Some(journal) = journal.filter(|journal| journal.phase >= MigrationPhase::Verified) {
        for entry in journal
            .entries
            .iter()
            .filter(|entry| entry.kind != MigrationFileKind::Backup)
        {
            let target = target_root.join(&entry.target_relative);
            if target
                .try_exists()
                .map_err(|_| MigrationError::TargetPopulated)?
            {
                let expected = staged_expectation(entry, journal)?;
                if fingerprint_target_file(&target, max_file_bytes(entry.kind))? != expected {
                    return Err(MigrationError::TargetPopulated);
                }
            } else if journal.phase >= MigrationPhase::Activated {
                return Err(MigrationError::ActivationFailed);
            }
        }
    }
    Ok(())
}

fn assert_named_directory_entries(
    directory: &Path,
    allowed: &[&str],
) -> Result<(), MigrationError> {
    assert_safe_directory(directory, MigrationError::TargetUnsafe)?;
    for entry in fs::read_dir(directory).map_err(|_| MigrationError::TargetUnsafe)? {
        let entry = entry.map_err(|_| MigrationError::TargetUnsafe)?;
        let metadata =
            fs::symlink_metadata(entry.path()).map_err(|_| MigrationError::TargetUnsafe)?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| MigrationError::TargetUnsafe)?;
        if path_metadata_is_link(&metadata)
            || (metadata.file_type().is_file() && metadata_has_unexpected_link_count(&metadata))
            || !metadata.file_type().is_file()
            || !allowed.contains(&name.as_str())
        {
            return Err(MigrationError::TargetPopulated);
        }
    }
    Ok(())
}

fn assert_cache_shape(
    target_root: &Path,
    journal: Option<&MigrationJournal>,
) -> Result<(), MigrationError> {
    let cache = target_root.join("cache");
    assert_safe_directory(&cache, MigrationError::TargetUnsafe)?;
    for entry in fs::read_dir(&cache).map_err(|_| MigrationError::TargetUnsafe)? {
        let entry = entry.map_err(|_| MigrationError::TargetUnsafe)?;
        let metadata =
            fs::symlink_metadata(entry.path()).map_err(|_| MigrationError::TargetUnsafe)?;
        if entry.file_name() != OsStr::new("migration-stage")
            || path_metadata_is_link(&metadata)
            || !metadata.file_type().is_dir()
        {
            return Err(MigrationError::TargetPopulated);
        }
    }
    let stage_root = cache.join("migration-stage");
    assert_safe_directory(&stage_root, MigrationError::TargetUnsafe)?;
    for entry in fs::read_dir(&stage_root).map_err(|_| MigrationError::TargetUnsafe)? {
        let entry = entry.map_err(|_| MigrationError::TargetUnsafe)?;
        let metadata =
            fs::symlink_metadata(entry.path()).map_err(|_| MigrationError::TargetUnsafe)?;
        let expected = journal.map(|journal| journal.transaction_id.as_str());
        if expected != entry.file_name().to_str()
            || path_metadata_is_link(&metadata)
            || !metadata.file_type().is_dir()
        {
            return Err(MigrationError::TargetPopulated);
        }
    }
    Ok(())
}

fn assert_backup_targets(
    target_root: &Path,
    journal: Option<&MigrationJournal>,
) -> Result<(), MigrationError> {
    let directory = target_root.join("backups");
    assert_safe_directory(&directory, MigrationError::TargetUnsafe)?;
    let expected = journal
        .filter(|journal| journal.phase >= MigrationPhase::Verified)
        .map(|journal| {
            journal
                .entries
                .iter()
                .filter(|entry| entry.kind == MigrationFileKind::Backup)
                .map(|entry| (entry.target_relative.clone(), entry))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();
    let mut scanned = 0_usize;
    let mut directories = vec![(directory, String::from("backups"), 0_usize)];
    while let Some((directory, prefix, depth)) = directories.pop() {
        for entry in fs::read_dir(&directory).map_err(|_| MigrationError::TargetUnsafe)? {
            scanned += 1;
            if scanned > MAX_BACKUP_SCAN_ENTRIES {
                return Err(MigrationError::TargetPopulated);
            }
            let entry = entry.map_err(|_| MigrationError::TargetUnsafe)?;
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path).map_err(|_| MigrationError::TargetUnsafe)?;
            if path_metadata_is_link(&metadata)
                || (metadata.file_type().is_file() && metadata_has_unexpected_link_count(&metadata))
            {
                return Err(MigrationError::TargetPopulated);
            }
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| MigrationError::TargetUnsafe)?;
            if !is_safe_backup_segment(&name) {
                return Err(MigrationError::TargetPopulated);
            }
            let relative = format!("{prefix}/{name}");
            if metadata.file_type().is_dir() {
                if depth >= MAX_BACKUP_DEPTH
                    || !expected.keys().any(|file| {
                        file.strip_prefix(&relative)
                            .is_some_and(|suffix| suffix.starts_with('/'))
                    })
                {
                    return Err(MigrationError::TargetPopulated);
                }
                directories.push((path, relative, depth + 1));
            } else if metadata.file_type().is_file() {
                let source = expected
                    .get(&relative)
                    .ok_or(MigrationError::TargetPopulated)?;
                let owner = journal.expect("backup expectation requires journal");
                if fingerprint_target_file(&path, MAX_BACKUP_BYTES)?
                    != staged_expectation(source, owner)?
                {
                    return Err(MigrationError::TargetPopulated);
                }
            } else {
                return Err(MigrationError::TargetPopulated);
            }
        }
    }
    if let Some(journal) = journal.filter(|journal| journal.phase >= MigrationPhase::Activated) {
        for entry in journal
            .entries
            .iter()
            .filter(|entry| entry.kind == MigrationFileKind::Backup)
        {
            if !target_root
                .join(&entry.target_relative)
                .try_exists()
                .map_err(|_| MigrationError::ActivationFailed)?
            {
                return Err(MigrationError::ActivationFailed);
            }
        }
    }
    Ok(())
}

fn stage_directory(target_root: &Path, journal: &MigrationJournal) -> PathBuf {
    target_root
        .join("cache/migration-stage")
        .join(&journal.transaction_id)
}

fn fingerprint_staged_file(path: &Path, max: u64) -> Result<(u64, String), MigrationError> {
    hash_file_bounded(path, max, MigrationError::StageDigestMismatch)
}

fn fingerprint_target_file(path: &Path, max: u64) -> Result<(u64, String), MigrationError> {
    hash_file_bounded(path, max, MigrationError::TargetPopulated)
}

fn hash_file_bounded(
    path: &Path,
    max: u64,
    error: MigrationError,
) -> Result<(u64, String), MigrationError> {
    let (size, sha256, _) = hash_file_bounded_with_identity(path, max, error)?;
    Ok((size, sha256))
}

fn hash_file_bounded_with_identity(
    path: &Path,
    max: u64,
    error: MigrationError,
) -> Result<(u64, String, Handle), MigrationError> {
    let metadata = fs::symlink_metadata(path).map_err(|_| error)?;
    if file_metadata_is_unsafe_link(&metadata)
        || !metadata.file_type().is_file()
        || metadata.len() == 0
        || metadata.len() > max
    {
        return Err(error);
    }
    let mut file = open_existing_file_nofollow(path, false).map_err(|_| error)?;
    if opened_file_has_unexpected_link_count(&file).map_err(|_| error)? {
        return Err(error);
    }
    let opened = Handle::from_file(file.try_clone().map_err(|_| error)?).map_err(|_| error)?;
    if file_handle_nofollow(path).map_err(|_| error)? != opened {
        return Err(error);
    }
    let mut digest = Sha256::new();
    let mut size = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(|_| error)?;
        if read == 0 {
            break;
        }
        size = size.checked_add(read as u64).ok_or(error)?;
        if size > max {
            return Err(error);
        }
        digest.update(&buffer[..read]);
    }
    let final_metadata = file.metadata().map_err(|_| error)?;
    if file_metadata_is_unsafe_link(&final_metadata)
        || opened_file_has_unexpected_link_count(&file).map_err(|_| error)?
        || size != metadata.len()
        || final_metadata.len() != metadata.len()
        || file_handle_nofollow(path).map_err(|_| error)? != opened
    {
        return Err(error);
    }
    Ok((size, hex_digest(digest.finalize()), opened))
}

fn inspect_bounded_single_link_file(
    path: &Path,
    max: u64,
    allow_empty: bool,
    error: MigrationError,
) -> Result<Handle, MigrationError> {
    let metadata = fs::symlink_metadata(path).map_err(|_| error)?;
    if file_metadata_is_unsafe_link(&metadata)
        || !metadata.file_type().is_file()
        || (!allow_empty && metadata.len() == 0)
        || metadata.len() > max
    {
        return Err(error);
    }
    let file = open_existing_file_nofollow(path, false).map_err(|_| error)?;
    if opened_file_has_unexpected_link_count(&file).map_err(|_| error)? {
        return Err(error);
    }
    let opened = Handle::from_file(file.try_clone().map_err(|_| error)?).map_err(|_| error)?;
    let final_metadata = file.metadata().map_err(|_| error)?;
    if file_metadata_is_unsafe_link(&final_metadata)
        || (!allow_empty && final_metadata.len() == 0)
        || final_metadata.len() > max
        || final_metadata.len() != metadata.len()
        || opened_file_has_unexpected_link_count(&file).map_err(|_| error)?
        || file_handle_nofollow(path).map_err(|_| error)? != opened
    {
        return Err(error);
    }
    Ok(opened)
}

fn cleanup_atomic_write_temps_for_destination(
    trusted_root: &Path,
    destination: &Path,
    max: u64,
    error: MigrationError,
) -> Result<(), MigrationError> {
    let parent = destination.parent().ok_or(error)?;
    let Some(directory_chain) = verify_directory_chain(trusted_root, parent, error)? else {
        return Ok(());
    };
    let destination_name = destination
        .file_name()
        .and_then(OsStr::to_str)
        .ok_or(error)?;
    let mut scanned = 0_usize;
    for entry in fs::read_dir(parent).map_err(|_| error)? {
        scanned += 1;
        if scanned > MAX_ATOMIC_TEMP_SCAN_ENTRIES {
            return Err(error);
        }
        let entry = entry.map_err(|_| error)?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if atomic_temp_destination(&name) != Some(destination_name) {
            continue;
        }
        verify_directory_chain_unchanged(&directory_chain, error)?;
        let path = entry.path();
        let identity = inspect_bounded_single_link_file(&path, max, true, error)?;
        let current = file_handle_nofollow(&path).map_err(|_| error)?;
        if current != identity {
            return Err(error);
        }
        verify_directory_chain_unchanged(&directory_chain, error)?;
        drop(current);
        drop(identity);
        fs::remove_file(path).map_err(|_| error)?;
        verify_directory_chain_unchanged(&directory_chain, error)?;
    }
    verify_directory_chain_unchanged(&directory_chain, error)?;
    Ok(())
}

fn verify_directory_chain(
    trusted_root: &Path,
    directory: &Path,
    error: MigrationError,
) -> Result<Option<Vec<(PathBuf, Handle)>>, MigrationError> {
    if !directory.starts_with(trusted_root) {
        return Err(error);
    }
    let relative = directory.strip_prefix(trusted_root).map_err(|_| error)?;
    let mut current = trusted_root.to_path_buf();
    let mut chain = Vec::new();
    for component in std::iter::once(None).chain(relative.components().map(Some)) {
        if let Some(component) = component {
            let std::path::Component::Normal(segment) = component else {
                return Err(error);
            };
            current.push(segment);
        }
        let metadata = match fs::symlink_metadata(&current) {
            Ok(metadata) => metadata,
            Err(io_error) if io_error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(error),
        };
        if path_metadata_is_link(&metadata) || !metadata.file_type().is_dir() {
            return Err(error);
        }
        let identity = Handle::from_path(&current).map_err(|_| error)?;
        chain.push((current.clone(), identity));
    }
    verify_directory_chain_unchanged(&chain, error)?;
    Ok(Some(chain))
}

fn verify_directory_chain_unchanged(
    chain: &[(PathBuf, Handle)],
    error: MigrationError,
) -> Result<(), MigrationError> {
    for (path, identity) in chain {
        let metadata = fs::symlink_metadata(path).map_err(|_| error)?;
        let current = Handle::from_path(path).map_err(|_| error)?;
        if path_metadata_is_link(&metadata)
            || !metadata.file_type().is_dir()
            || &current != identity
        {
            return Err(error);
        }
    }
    Ok(())
}

fn atomic_temp_destination(name: &str) -> Option<&str> {
    if !name.starts_with('.') || name.len() < 9 {
        return None;
    }
    let suffix_start = name.len().checked_sub(6)?;
    if name.as_bytes().get(suffix_start.checked_sub(1)?) != Some(&b'.')
        || !name[suffix_start..]
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric())
    {
        return None;
    }
    let destination = &name[1..suffix_start - 1];
    (!destination.is_empty()).then_some(destination)
}

fn cleanup_transaction_atomic_temps(
    target_root: &Path,
    journal: Option<&MigrationJournal>,
) -> Result<(), MigrationError> {
    let Some(journal) = journal else {
        return Ok(());
    };
    let stage = stage_directory(target_root, journal);
    for entry in &journal.entries {
        cleanup_atomic_write_temps_for_destination(
            &stage,
            &stage.join(&entry.target_relative),
            max_file_bytes(entry.kind),
            MigrationError::StageDigestMismatch,
        )?;
        if journal.phase < MigrationPhase::Activated {
            cleanup_atomic_write_temps_for_destination(
                target_root,
                &target_root.join(&entry.target_relative),
                max_file_bytes(entry.kind),
                MigrationError::TargetUnsafe,
            )?;
        }
    }
    if journal
        .entries
        .iter()
        .any(|entry| entry.kind == MigrationFileKind::Database)
    {
        cleanup_atomic_write_temps_for_destination(
            &stage,
            &stage.join("maintenance-report.json"),
            MAX_MAINTENANCE_REPORT_BYTES as u64,
            MigrationError::StageDigestMismatch,
        )?;
    }
    Ok(())
}

fn load_json_document_nofollow<T: DurableDocument>(
    path: &Path,
    error: MigrationError,
) -> Result<Option<T>, MigrationError> {
    if T::FORMAT != DocumentFormat::Json {
        return Err(error);
    }
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(io_error) if io_error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(error),
    };
    if file_metadata_is_unsafe_link(&metadata)
        || !metadata.file_type().is_file()
        || metadata.len() > T::MAX_BYTES as u64
    {
        return Err(error);
    }
    let mut file = open_existing_file_nofollow(path, false).map_err(|_| error)?;
    if opened_file_has_unexpected_link_count(&file).map_err(|_| error)? {
        return Err(error);
    }
    let opened = Handle::from_file(file.try_clone().map_err(|_| error)?).map_err(|_| error)?;
    if file_handle_nofollow(path).map_err(|_| error)? != opened {
        return Err(error);
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    Read::by_ref(&mut file)
        .take(T::MAX_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| error)?;
    let final_metadata = file.metadata().map_err(|_| error)?;
    let final_path_metadata = fs::symlink_metadata(path).map_err(|_| error)?;
    if bytes.len() > T::MAX_BYTES
        || bytes.len() as u64 != metadata.len()
        || final_metadata.len() != metadata.len()
        || file_metadata_is_unsafe_link(&final_metadata)
        || file_metadata_is_unsafe_link(&final_path_metadata)
        || opened_file_has_unexpected_link_count(&file).map_err(|_| error)?
        || file_handle_nofollow(path).map_err(|_| error)? != opened
    {
        return Err(error);
    }
    let value: T = serde_json::from_slice(&bytes).map_err(|_| error)?;
    if !value.validate() {
        return Err(error);
    }
    Ok(Some(value))
}

fn transaction_id(
    source_root: &str,
    data_source_root: Option<&str>,
    entries: &[MigrationEntry],
) -> String {
    let mut digest = Sha256::new();
    digest.update(source_root.as_bytes());
    digest.update([0]);
    if let Some(data_source_root) = data_source_root {
        digest.update(data_source_root.as_bytes());
    }
    for entry in entries {
        digest.update([entry.kind as u8]);
        digest.update([entry.source_kind as u8]);
        digest.update(entry.source_relative.as_bytes());
        digest.update([0]);
        digest.update(entry.target_relative.as_bytes());
        digest.update(entry.size_bytes.to_be_bytes());
        digest.update(entry.sha256.as_bytes());
        digest.update(entry.identity.device_id.to_be_bytes());
        digest.update(entry.identity.file_id.to_be_bytes());
        if let Some(wal) = entry.database_wal.as_ref() {
            digest.update([1]);
            digest.update(wal.source_relative.as_bytes());
            digest.update(wal.size_bytes.to_be_bytes());
            digest.update(wal.sha256.as_bytes());
            digest.update(wal.identity.device_id.to_be_bytes());
            digest.update(wal.identity.file_id.to_be_bytes());
        } else {
            digest.update([0]);
        }
        if let Some(shared_memory) = entry.database_shm.as_ref() {
            digest.update([1]);
            digest.update(shared_memory.source_relative.as_bytes());
            digest.update(shared_memory.identity.device_id.to_be_bytes());
            digest.update(shared_memory.identity.file_id.to_be_bytes());
        } else {
            digest.update([0]);
        }
    }
    hex_digest(digest.finalize())[..24].to_owned()
}

fn outcome(journal: &MigrationJournal) -> MigrationOutcome {
    MigrationOutcome {
        schema_version: JOURNAL_SCHEMA_VERSION,
        phase: journal.phase,
        migrated: journal.phase == MigrationPhase::Complete,
        file_count: journal.entries.len(),
        database_migrated: journal
            .entries
            .iter()
            .any(|entry| entry.kind == MigrationFileKind::Database),
        plaintext_secrets_migrated: journal
            .entries
            .iter()
            .any(|entry| entry.kind == MigrationFileKind::Secrets),
    }
}

fn store_journal(
    document: &TypedDocument<MigrationJournal>,
    journal: &MigrationJournal,
) -> Result<(), MigrationError> {
    document
        .store(journal)
        .map_err(|_| MigrationError::JournalWriteFailed)
}

fn map_target_lock_error(error: LockError) -> MigrationError {
    match error {
        LockError::Contended => MigrationError::MigrationInProgress,
        _ => MigrationError::TargetUnsafe,
    }
}

fn map_source_lock_error(error: LockError) -> MigrationError {
    match error {
        LockError::Contended => MigrationError::SourceInUse,
        _ => MigrationError::SourceUnsafe,
    }
}

fn assert_safe_directory(path: &Path, error: MigrationError) -> Result<(), MigrationError> {
    let metadata = fs::symlink_metadata(path).map_err(|_| error)?;
    if path_metadata_is_link(&metadata) || !metadata.file_type().is_dir() {
        Err(error)
    } else {
        Ok(())
    }
}

fn open_existing_file_nofollow(path: &Path, write: bool) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(write);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;

        options.custom_flags(WINDOWS_OPEN_REPARSE_POINT);
    }
    let file = options.open(path)?;
    let metadata = file.metadata()?;
    if file_metadata_is_unsafe_link(&metadata) || !metadata.file_type().is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "path did not resolve to a safe regular file",
        ));
    }
    Ok(file)
}

fn file_handle_nofollow(path: &Path) -> std::io::Result<Handle> {
    Handle::from_file(open_existing_file_nofollow(path, false)?)
}

fn path_metadata_is_link(metadata: &fs::Metadata) -> bool {
    if metadata.file_type().is_symlink() {
        return true;
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        metadata.file_attributes() & WINDOWS_REPARSE_POINT != 0
    }
    #[cfg(not(windows))]
    {
        let _ = WINDOWS_REPARSE_POINT;
        false
    }
}

fn file_metadata_is_unsafe_link(metadata: &fs::Metadata) -> bool {
    path_metadata_is_link(metadata)
        || (metadata.file_type().is_file() && metadata_has_unexpected_link_count(metadata))
}

#[cfg(windows)]
fn opened_file_has_unexpected_link_count(file: &File) -> std::io::Result<bool> {
    winapi_util::file::information(file).map(|information| information.number_of_links() != 1)
}

#[cfg(unix)]
fn opened_file_has_unexpected_link_count(file: &File) -> std::io::Result<bool> {
    use std::os::unix::fs::MetadataExt;
    file.metadata().map(|metadata| metadata.nlink() != 1)
}

#[cfg(not(any(unix, windows)))]
fn opened_file_has_unexpected_link_count(_file: &File) -> std::io::Result<bool> {
    Ok(false)
}

#[cfg(unix)]
fn metadata_has_unexpected_link_count(metadata: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    metadata.nlink() != 1
}

#[cfg(windows)]
fn metadata_has_unexpected_link_count(_metadata: &fs::Metadata) -> bool {
    false
}

#[cfg(not(any(unix, windows)))]
fn metadata_has_unexpected_link_count(_metadata: &fs::Metadata) -> bool {
    false
}

fn valid_entry_paths(entry: &MigrationEntry) -> bool {
    if entry.source_relative.len() > MAX_PATH_BYTES
        || entry.target_relative.len() > MAX_PATH_BYTES
        || Path::new(&entry.source_relative).is_absolute()
        || Path::new(&entry.target_relative).is_absolute()
        || entry.source_relative.contains("..")
        || entry.target_relative.contains("..")
    {
        return false;
    }
    match entry.kind {
        MigrationFileKind::Config => {
            matches!(entry.source_relative.as_str(), "config.toml" | "agent.toml")
                && entry.target_relative == "config.toml"
        }
        MigrationFileKind::Secrets => {
            entry.source_relative == "secrets.json" && entry.target_relative == "secrets.json"
        }
        MigrationFileKind::Database => {
            matches!(
                entry.source_relative.as_str(),
                "cmclient.db" | "gateway.sqlite"
            ) && entry.target_relative == "cmclient.db"
        }
        MigrationFileKind::Backup => {
            entry.source_relative == entry.target_relative
                && entry.target_relative.starts_with("backups/")
                && is_safe_backup_relative(&entry.target_relative["backups/".len()..])
        }
    }
}

fn is_config_source(source: &MigrationSourceKind) -> bool {
    *source == MigrationSourceKind::Config
}

fn max_file_bytes(kind: MigrationFileKind) -> u64 {
    match kind {
        MigrationFileKind::Config => MAX_CONFIG_BYTES,
        MigrationFileKind::Secrets => MAX_SECRETS_BYTES,
        MigrationFileKind::Database => MAX_DATABASE_BYTES,
        MigrationFileKind::Backup => MAX_BACKUP_BYTES,
    }
}

fn is_safe_backup_relative(relative: &str) -> bool {
    !relative.is_empty()
        && relative.len() <= MAX_PATH_BYTES - "backups/".len()
        && is_backup_file_extension(relative)
        && relative.split('/').all(is_safe_backup_segment)
        && relative.split('/').count() <= MAX_BACKUP_DEPTH + 1
}

fn is_safe_backup_directory_relative(relative: &str) -> bool {
    relative == "backups"
        || relative.strip_prefix("backups/").is_some_and(|suffix| {
            !suffix.is_empty()
                && suffix.split('/').all(is_safe_backup_segment)
                && suffix.split('/').count() <= MAX_BACKUP_DEPTH
        })
}

fn is_safe_backup_segment(segment: &str) -> bool {
    !segment.is_empty()
        && segment.len() <= 255
        && !matches!(segment, "." | "..")
        && segment
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn is_backup_file_extension(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    lower.ends_with(".sqlite") || lower.ends_with(".db")
}

fn is_bounded_identifier(value: &str, max: usize) -> bool {
    !value.is_empty()
        && value.len() <= max
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

fn is_bounded_error_code(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}

fn is_sha256(value: &str) -> bool {
    is_hex(value, 64)
}

fn is_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn hex_digest(bytes: impl AsRef<[u8]>) -> String {
    let bytes = bytes.as_ref();
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn path_text(path: &Path) -> Result<String, MigrationError> {
    let value = path.to_str().ok_or(MigrationError::PathInvalid)?;
    if value.is_empty() || value.len() > MAX_PATH_BYTES || value.contains(char::is_control) {
        return Err(MigrationError::PathInvalid);
    }
    Ok(String::from(value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    struct CopyMaintenance {
        calls: AtomicUsize,
        fail: bool,
    }

    impl CopyMaintenance {
        fn working() -> Self {
            Self {
                calls: AtomicUsize::new(0),
                fail: false,
            }
        }
    }

    impl GatewayMaintenanceRunner for CopyMaintenance {
        fn migrate_database(
            &self,
            source_database: &Path,
            staged_database: &Path,
        ) -> Result<GatewayMaintenanceReport, MigrationError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                return Err(MigrationError::MaintenanceFailed);
            }
            fs::copy(source_database, staged_database)
                .map_err(|_| MigrationError::MaintenanceFailed)?;
            let source = hash_file_bounded(
                source_database,
                MAX_DATABASE_BYTES,
                MigrationError::MaintenanceFailed,
            )?;
            let staged = hash_file_bounded(
                staged_database,
                MAX_DATABASE_BYTES,
                MigrationError::MaintenanceFailed,
            )?;
            Ok(GatewayMaintenanceReport {
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
    }

    struct WalMutatingMaintenance {
        wal: PathBuf,
    }

    impl GatewayMaintenanceRunner for WalMutatingMaintenance {
        fn migrate_database(
            &self,
            source_database: &Path,
            staged_database: &Path,
        ) -> Result<GatewayMaintenanceReport, MigrationError> {
            fs::write(&self.wal, b"changed-wal").map_err(|_| MigrationError::MaintenanceFailed)?;
            CopyMaintenance::working().migrate_database(source_database, staged_database)
        }
    }

    struct ReconcilingMaintenanceWork;

    impl GatewayMaintenanceRunner for ReconcilingMaintenanceWork {
        fn migrate_database(
            &self,
            source_database: &Path,
            staged_database: &Path,
        ) -> Result<GatewayMaintenanceReport, MigrationError> {
            let work = staged_database
                .parent()
                .ok_or(MigrationError::MaintenanceFailed)?
                .join(GATEWAY_MAINTENANCE_WORK_DIRECTORY);
            if work.exists() {
                for entry in fs::read_dir(&work).map_err(|_| MigrationError::MaintenanceFailed)? {
                    fs::remove_file(entry.map_err(|_| MigrationError::MaintenanceFailed)?.path())
                        .map_err(|_| MigrationError::MaintenanceFailed)?;
                }
                fs::remove_dir(work).map_err(|_| MigrationError::MaintenanceFailed)?;
            }
            CopyMaintenance::working().migrate_database(source_database, staged_database)
        }
    }

    fn temporary_directory(label: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let test_root = option_env!("CARGO_TARGET_TMPDIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                std::env::current_exe()
                    .unwrap()
                    .parent()
                    .unwrap()
                    .parent()
                    .unwrap()
                    .join("cmclient-test-tmp")
            });
        test_root.join(format!(
            "cmclient-product-migration-{label}-{}-{suffix}",
            std::process::id()
        ))
    }

    fn fixture(label: &str) -> (PathBuf, ProductMigrationRequest) {
        let directory = temporary_directory(label);
        let source = directory.join("legacy");
        let target = directory.join("home/.cmclient");
        fs::create_dir_all(source.join("backups")).unwrap();
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        fs::write(
            source.join("agent.toml"),
            b"[agent]\nmanagement_web_enabled = true\n",
        )
        .unwrap();
        fs::write(
            source.join("secrets.json"),
            br#"{"version":1,"callmesh-api-key":"fixture-private"}"#,
        )
        .unwrap();
        fs::write(source.join("gateway.sqlite"), b"fixture-database").unwrap();
        fs::write(source.join("backups/one.sqlite"), b"fixture-backup").unwrap();
        fs::write(source.join("ignored.log"), b"ignored").unwrap();
        let request = ProductMigrationRequest {
            source_root: source,
            target_root: target,
        };
        (directory, request)
    }

    fn source_snapshot(root: &Path) -> BTreeMap<String, Vec<u8>> {
        let mut snapshot = BTreeMap::new();
        let mut directories = vec![(root.to_path_buf(), String::new())];
        while let Some((directory, prefix)) = directories.pop() {
            for entry in fs::read_dir(directory).unwrap() {
                let entry = entry.unwrap();
                let name = entry.file_name().into_string().unwrap();
                let relative = if prefix.is_empty() {
                    name
                } else {
                    format!("{prefix}/{name}")
                };
                if entry.file_type().unwrap().is_dir() {
                    directories.push((entry.path(), relative));
                } else {
                    snapshot.insert(relative, fs::read(entry.path()).unwrap());
                }
            }
        }
        snapshot
    }

    fn replace_file_identity(path: &Path) {
        let contents = fs::read(path).unwrap();
        let replacement = path.with_extension("identity-replacement");
        fs::write(&replacement, contents).unwrap();
        fs::remove_file(path).unwrap();
        fs::rename(replacement, path).unwrap();
    }

    #[cfg(unix)]
    fn create_directory_link(target: &Path, link: &Path) -> bool {
        std::os::unix::fs::symlink(target, link).unwrap();
        true
    }

    #[cfg(windows)]
    fn create_directory_link(target: &Path, link: &Path) -> bool {
        Command::new("powershell.exe")
            .args([
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "New-Item -ItemType Junction -Path $env:CMCLIENT_TEST_LINK -Target $env:CMCLIENT_TEST_TARGET | Out-Null",
            ])
            .env("CMCLIENT_TEST_LINK", link)
            .env("CMCLIENT_TEST_TARGET", target)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success())
    }

    #[cfg(unix)]
    fn create_file_link(target: &Path, link: &Path) -> bool {
        std::os::unix::fs::symlink(target, link).unwrap();
        true
    }

    #[cfg(windows)]
    fn create_file_link(target: &Path, link: &Path) -> bool {
        std::os::windows::fs::symlink_file(target, link).is_ok()
    }

    fn assert_migration_stage_empty(target_root: &Path) {
        assert!(
            fs::read_dir(target_root.join("cache/migration-stage"))
                .unwrap()
                .next()
                .is_none(),
            "completed migration must not retain plaintext stage data"
        );
    }

    fn interrupt_after_detected(request: &ProductMigrationRequest) {
        use std::panic::{AssertUnwindSafe, catch_unwind};

        let interrupted = catch_unwind(AssertUnwindSafe(|| {
            let _ = run_or_resume_product_migration_with_phase_hook(
                request,
                &CopyMaintenance::working(),
                &mut |phase| {
                    if phase == MigrationPhase::Detected {
                        panic!("simulate hard kill after durable Detected");
                    }
                },
            );
        }));
        assert!(interrupted.is_err());
    }

    fn transaction_stage_from_journal(target_root: &Path) -> PathBuf {
        let journal: serde_json::Value =
            serde_json::from_slice(&fs::read(target_root.join("state/migration.json")).unwrap())
                .unwrap();
        target_root
            .join("cache/migration-stage")
            .join(journal["transactionId"].as_str().unwrap())
    }

    #[test]
    fn migrates_only_known_files_and_is_idempotent_without_touching_source() {
        let (directory, request) = fixture("happy");
        let before = source_snapshot(&request.source_root);
        let maintenance = CopyMaintenance::working();
        let first = run_or_resume_product_migration(&request, &maintenance).unwrap();
        assert_eq!(first.phase, MigrationPhase::Complete);
        assert_eq!(first.file_count, 4);
        assert_eq!(
            fs::read_to_string(request.target_root.join("config.toml")).unwrap(),
            "[agent]\nmanagement_web_enabled = true\n"
        );
        assert_eq!(
            fs::read(request.target_root.join("cmclient.db")).unwrap(),
            b"fixture-database"
        );
        assert!(!request.target_root.join("ignored.log").exists());
        let mut after = source_snapshot(&request.source_root);
        assert_eq!(after.remove("agent.lock"), Some(Vec::new()));
        assert_eq!(before, after);
        assert_migration_stage_empty(&request.target_root);

        let second = run_or_resume_product_migration(&request, &maintenance).unwrap();
        assert_eq!(second, first);
        assert_eq!(maintenance.calls.load(Ordering::SeqCst), 1);
        let mut after_resume = source_snapshot(&request.source_root);
        assert_eq!(after_resume.remove("agent.lock"), Some(Vec::new()));
        assert_eq!(before, after_resume);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn complete_resume_cleans_stage_without_requiring_the_source() {
        use std::panic::{AssertUnwindSafe, catch_unwind};

        let (directory, request) = fixture("complete-cleanup-resume");
        let target = request.target_root.clone();
        let interrupted = catch_unwind(AssertUnwindSafe(|| {
            let _ = run_or_resume_product_migration_with_phase_hook(
                &request,
                &CopyMaintenance::working(),
                &mut |phase| {
                    if phase == MigrationPhase::Complete {
                        panic!("simulate hard kill after durable Complete");
                    }
                },
            );
        }));
        assert!(interrupted.is_err());
        let stage_root = target.join("cache/migration-stage");
        assert!(fs::read_dir(&stage_root).unwrap().next().is_some());
        fs::remove_dir_all(&request.source_root).unwrap();

        let outcome =
            run_or_resume_product_migration(&request, &CopyMaintenance::working()).unwrap();
        assert_eq!(outcome.phase, MigrationPhase::Complete);
        assert_migration_stage_empty(&target);
        let journal: serde_json::Value =
            serde_json::from_slice(&fs::read(target.join("state/migration.json")).unwrap())
                .unwrap();
        assert_eq!(journal["phase"], "complete");
        assert!(journal["recoveryCode"].is_null());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn complete_cleanup_failure_keeps_the_durable_complete_phase() {
        let (directory, request) = fixture("complete-cleanup-failure");
        let target = request.target_root.clone();
        let mut unknown = None;
        let result = run_or_resume_product_migration_with_phase_hook(
            &request,
            &CopyMaintenance::working(),
            &mut |phase| {
                if phase == MigrationPhase::Complete {
                    let path = transaction_stage_from_journal(&target).join("unowned");
                    fs::write(&path, b"do-not-delete").unwrap();
                    unknown = Some(path);
                }
            },
        );
        assert_eq!(result, Err(MigrationError::CleanupFailed));
        let journal: serde_json::Value = serde_json::from_slice(
            &fs::read(request.target_root.join("state/migration.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(journal["phase"], "complete");
        assert_eq!(journal["recoveryCode"], "LEGACY_MIGRATION_CLEANUP_FAILED");
        let unknown = unknown.unwrap();
        assert_eq!(fs::read(&unknown).unwrap(), b"do-not-delete");

        fs::remove_file(unknown).unwrap();
        let resumed =
            run_or_resume_product_migration(&request, &CopyMaintenance::working()).unwrap();
        assert_eq!(resumed.phase, MigrationPhase::Complete);
        assert_migration_stage_empty(&request.target_root);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn reserved_atomic_write_temps_are_reconciled_before_resume() {
        let (directory, request) = fixture("atomic-temp-resume");
        interrupt_after_detected(&request);
        let stage = transaction_stage_from_journal(&request.target_root);
        fs::create_dir(&stage).unwrap();
        let journal_temp = request.target_root.join("state/.migration.json.A1b2C3");
        let stage_temp = stage.join(".config.toml.D4e5F6");
        let activation_temp = request.target_root.join(".config.toml.G7h8I9");
        fs::write(&journal_temp, b"partial-journal").unwrap();
        fs::write(&stage_temp, b"partial-stage").unwrap();
        fs::write(&activation_temp, b"partial-activation").unwrap();

        let outcome =
            run_or_resume_product_migration(&request, &CopyMaintenance::working()).unwrap();
        assert_eq!(outcome.phase, MigrationPhase::Complete);
        assert!(!journal_temp.exists());
        assert!(!stage_temp.exists());
        assert!(!activation_temp.exists());
        assert_migration_stage_empty(&request.target_root);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn initial_journal_atomic_temp_without_canonical_journal_is_reconciled() {
        let (directory, request) = fixture("initial-journal-temp");
        fs::create_dir(&request.target_root).unwrap();
        for relative in [
            "run",
            "state",
            "cache",
            "cache/migration-stage",
            "backups",
            "logs",
            "updates",
        ] {
            fs::create_dir(request.target_root.join(relative)).unwrap();
        }
        let journal_temp = request.target_root.join("state/.migration.json.A1b2C3");
        fs::write(&journal_temp, b"partial-initial-journal").unwrap();

        let outcome =
            run_or_resume_product_migration(&request, &CopyMaintenance::working()).unwrap();
        assert_eq!(outcome.phase, MigrationPhase::Complete);
        assert!(!journal_temp.exists());
        assert_migration_stage_empty(&request.target_root);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn unsafe_exact_atomic_temp_is_not_deleted() {
        let (directory, request) = fixture("unsafe-atomic-temp");
        interrupt_after_detected(&request);
        let owner = directory.join("hardlink-owner");
        let atomic_temp = request.target_root.join("state/.migration.json.J1k2L3");
        fs::write(&owner, b"not-owned-by-migration").unwrap();
        fs::hard_link(&owner, &atomic_temp).unwrap();

        assert_eq!(
            run_or_resume_product_migration(&request, &CopyMaintenance::working()),
            Err(MigrationError::JournalInvalid)
        );
        assert_eq!(fs::read(&owner).unwrap(), b"not-owned-by-migration");
        assert_eq!(fs::read(&atomic_temp).unwrap(), b"not-owned-by-migration");
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn nested_directory_link_cannot_redirect_atomic_temp_cleanup() {
        let (directory, request) = fixture("atomic-temp-directory-link");
        let nested_source = request.source_root.join("backups/nested");
        fs::create_dir(&nested_source).unwrap();
        fs::write(nested_source.join("two.sqlite"), b"nested-backup").unwrap();
        interrupt_after_detected(&request);

        let external = directory.join("external");
        fs::create_dir(&external).unwrap();
        let external_temp = external.join(".two.sqlite.A1b2C3");
        fs::write(&external_temp, b"must-not-delete").unwrap();
        let nested_target = request.target_root.join("backups/nested");
        assert!(
            create_directory_link(&external, &nested_target),
            "junction fixture must be created without elevation"
        );

        assert_eq!(
            run_or_resume_product_migration(&request, &CopyMaintenance::working()),
            Err(MigrationError::TargetUnsafe)
        );
        assert_eq!(fs::read(&external_temp).unwrap(), b"must-not-delete");
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn existing_file_open_does_not_follow_symbolic_or_reparse_leaves() {
        let directory = temporary_directory("nofollow-open");
        fs::create_dir_all(&directory).unwrap();
        let target = directory.join("external-secret");
        let link = directory.join("source-link");
        fs::write(&target, b"must-not-read-through-link").unwrap();
        if !create_file_link(&target, &link) {
            fs::remove_dir_all(directory).unwrap();
            return;
        }
        assert!(open_existing_file_nofollow(&link, false).is_err());
        assert_eq!(fs::read(&target).unwrap(), b"must-not-read-through-link");
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn reserved_gateway_work_directory_can_be_reconciled_on_detected_resume() {
        let (directory, request) = fixture("gateway-work-resume");
        interrupt_after_detected(&request);
        let stage = transaction_stage_from_journal(&request.target_root);
        let work = stage.join(GATEWAY_MAINTENANCE_WORK_DIRECTORY);
        fs::create_dir(&stage).unwrap();
        fs::create_dir(&work).unwrap();
        fs::write(work.join("source.sqlite"), b"partial-source-snapshot").unwrap();

        let outcome = run_or_resume_product_migration(&request, &ReconcilingMaintenanceWork)
            .expect("reserved Gateway work should be retryable");
        assert_eq!(outcome.phase, MigrationPhase::Complete);
        assert_migration_stage_empty(&request.target_root);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn unsafe_gateway_work_directory_fails_closed_without_deletion() {
        let (directory, request) = fixture("gateway-work-unsafe");
        interrupt_after_detected(&request);
        let stage = transaction_stage_from_journal(&request.target_root);
        let work = stage.join(GATEWAY_MAINTENANCE_WORK_DIRECTORY);
        fs::create_dir(&stage).unwrap();
        fs::create_dir(&work).unwrap();
        let unknown = work.join("unknown.sqlite");
        fs::write(&unknown, b"unowned").unwrap();

        assert_eq!(
            run_or_resume_product_migration(&request, &ReconcilingMaintenanceWork),
            Err(MigrationError::StageDigestMismatch)
        );
        assert_eq!(fs::read(&unknown).unwrap(), b"unowned");
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn recovery_journal_write_failure_is_not_swallowed() {
        let (directory, request) = fixture("recovery-write-failure");
        let state = request.target_root.join("state");
        let source_config = request.source_root.join("agent.toml");
        let result = run_or_resume_product_migration_with_phase_hook(
            &request,
            &CopyMaintenance::working(),
            &mut |phase| {
                if phase == MigrationPhase::Detected {
                    fs::remove_file(state.join("migration.json")).unwrap();
                    fs::remove_dir(&state).unwrap();
                    fs::write(&state, b"block recovery journal parent").unwrap();
                    fs::write(&source_config, b"[agent]\nmanagement_web_enabled = false\n")
                        .unwrap();
                }
            },
        );
        assert_eq!(result, Err(MigrationError::JournalWriteFailed));
        assert_eq!(fs::read(&state).unwrap(), b"block recovery journal parent");
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn completed_transaction_ignores_later_runtime_mutation_and_missing_source() {
        let (directory, request) = fixture("complete-terminal");
        let maintenance = CopyMaintenance::working();
        let first = run_or_resume_product_migration(&request, &maintenance).unwrap();
        assert_eq!(first.phase, MigrationPhase::Complete);
        assert_eq!(maintenance.calls.load(Ordering::SeqCst), 1);

        fs::write(request.target_root.join("config.toml"), b"runtime-config").unwrap();
        fs::write(request.target_root.join("cmclient.db"), b"runtime-database").unwrap();
        fs::write(request.target_root.join("state/runtime.json"), b"{}").unwrap();
        fs::write(request.target_root.join("logs/runtime.jsonl"), b"{}\n").unwrap();
        fs::remove_dir_all(&request.source_root).unwrap();

        assert_eq!(
            pending_migration_source(&request.target_root).unwrap(),
            None
        );
        assert_eq!(
            run_or_resume_product_migration(&request, &maintenance).unwrap(),
            first
        );
        assert_eq!(
            migrate_detected_product(
                &request.target_root,
                std::slice::from_ref(&request.source_root),
                &maintenance,
            )
            .unwrap(),
            Some(first)
        );
        assert_eq!(maintenance.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            fs::read(request.target_root.join("config.toml")).unwrap(),
            b"runtime-config"
        );
        assert_eq!(
            fs::read(request.target_root.join("cmclient.db")).unwrap(),
            b"runtime-database"
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn rejects_populated_target_without_merging() {
        let (directory, request) = fixture("target-conflict");
        fs::create_dir_all(&request.target_root).unwrap();
        fs::write(request.target_root.join("config.toml"), b"existing").unwrap();
        assert_eq!(
            run_or_resume_product_migration(&request, &CopyMaintenance::working()),
            Err(MigrationError::TargetPopulated)
        );
        assert_eq!(
            fs::read(request.target_root.join("config.toml")).unwrap(),
            b"existing"
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn source_mutation_after_detected_is_durable_recovery_state() {
        let (directory, request) = fixture("source-change");
        let source_config = request.source_root.join("agent.toml");
        let mut changed = false;
        let result = run_or_resume_product_migration_with_phase_hook(
            &request,
            &CopyMaintenance::working(),
            &mut |phase| {
                if phase == MigrationPhase::Detected && !changed {
                    fs::write(&source_config, b"[agent]\nmanagement_web_enabled = false\n")
                        .unwrap();
                    changed = true;
                }
            },
        );
        assert_eq!(result, Err(MigrationError::SourceChanged));
        let journal = fs::read_to_string(request.target_root.join("state/migration.json")).unwrap();
        assert!(journal.contains("LEGACY_MIGRATION_SOURCE_CHANGED"));
        assert!(!request.target_root.join("config.toml").exists());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn byte_identical_source_replacements_are_detected_by_persistent_identity() {
        for relative in ["agent.toml", "gateway.sqlite", "gateway.sqlite-wal"] {
            let (directory, request) = fixture(&format!(
                "identity-replacement-{}",
                relative.replace('.', "-")
            ));
            let source = request.source_root.join(relative);
            if relative.ends_with("-wal") {
                fs::write(&source, b"fixture-wal").unwrap();
            }
            let result = run_or_resume_product_migration_with_phase_hook(
                &request,
                &CopyMaintenance::working(),
                &mut |phase| {
                    if phase == MigrationPhase::Detected {
                        replace_file_identity(&source);
                    }
                },
            );
            assert_eq!(result, Err(MigrationError::SourceChanged), "{relative}");
            let journal: serde_json::Value = serde_json::from_slice(
                &fs::read(request.target_root.join("state/migration.json")).unwrap(),
            )
            .unwrap();
            assert_eq!(journal["recoveryCode"], "LEGACY_MIGRATION_SOURCE_CHANGED");
            fs::remove_dir_all(directory).unwrap();
        }
    }

    #[test]
    fn shared_memory_topology_changes_fail_closed_without_hashing_volatile_content() {
        for scenario in ["appeared", "disappeared", "replaced"] {
            let (directory, request) = fixture(&format!("shm-{scenario}"));
            let shared_memory = request.source_root.join("gateway.sqlite-shm");
            if scenario != "appeared" {
                fs::write(&shared_memory, b"sqlite-shm-topology").unwrap();
            }
            let result = run_or_resume_product_migration_with_phase_hook(
                &request,
                &CopyMaintenance::working(),
                &mut |phase| {
                    if phase != MigrationPhase::Detected {
                        return;
                    }
                    match scenario {
                        "appeared" => fs::write(&shared_memory, b"sqlite-shm-topology").unwrap(),
                        "disappeared" => fs::remove_file(&shared_memory).unwrap(),
                        "replaced" => replace_file_identity(&shared_memory),
                        _ => unreachable!(),
                    }
                },
            );
            assert_eq!(result, Err(MigrationError::SourceChanged), "{scenario}");
            fs::remove_dir_all(directory).unwrap();
        }
    }

    #[test]
    fn source_is_rechecked_after_activation_before_complete() {
        let (directory, request) = fixture("source-change-activated");
        let source_config = request.source_root.join("agent.toml");
        let result = run_or_resume_product_migration_with_phase_hook(
            &request,
            &CopyMaintenance::working(),
            &mut |phase| {
                if phase == MigrationPhase::Activated {
                    fs::write(&source_config, b"[agent]\nmanagement_web_enabled = false\n")
                        .unwrap();
                }
            },
        );
        assert_eq!(result, Err(MigrationError::SourceChanged));
        let journal: serde_json::Value = serde_json::from_slice(
            &fs::read(request.target_root.join("state/migration.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(journal["phase"], "activated");
        assert_eq!(journal["recoveryCode"], "LEGACY_MIGRATION_SOURCE_CHANGED");
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn staged_digest_tampering_fails_before_activation() {
        let (directory, request) = fixture("stage-tamper");
        let target = request.target_root.clone();
        let mut tampered = false;
        let result = run_or_resume_product_migration_with_phase_hook(
            &request,
            &CopyMaintenance::working(),
            &mut |phase| {
                if phase == MigrationPhase::Staged && !tampered {
                    let journal: serde_json::Value = serde_json::from_slice(
                        &fs::read(target.join("state/migration.json")).unwrap(),
                    )
                    .unwrap();
                    let id = journal["transactionId"].as_str().unwrap();
                    fs::write(
                        target
                            .join("cache/migration-stage")
                            .join(id)
                            .join("config.toml"),
                        b"tampered",
                    )
                    .unwrap();
                    tampered = true;
                }
            },
        );
        assert_eq!(result, Err(MigrationError::StageDigestMismatch));
        assert!(!request.target_root.join("config.toml").exists());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn maintenance_failure_is_reported_without_rust_database_fallback() {
        let (directory, request) = fixture("maintenance-failure");
        let maintenance = CopyMaintenance {
            calls: AtomicUsize::new(0),
            fail: true,
        };
        assert_eq!(
            run_or_resume_product_migration(&request, &maintenance),
            Err(MigrationError::MaintenanceFailed)
        );
        assert!(!request.target_root.join("cmclient.db").exists());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn rejects_alias_conflicts_and_file_count_overflow() {
        let (directory, request) = fixture("bounds");
        fs::write(request.source_root.join("config.toml"), b"[agent]\n").unwrap();
        assert_eq!(
            run_or_resume_product_migration(&request, &CopyMaintenance::working()),
            Err(MigrationError::SourceAmbiguous)
        );
        fs::remove_file(request.source_root.join("config.toml")).unwrap();
        for index in 0..=MAX_BACKUP_FILES {
            fs::write(
                request
                    .source_root
                    .join("backups")
                    .join(format!("overflow-{index}.sqlite")),
                b"x",
            )
            .unwrap();
        }
        assert_eq!(
            run_or_resume_product_migration(&request, &CopyMaintenance::working()),
            Err(MigrationError::SourceTooManyFiles)
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn exact_partial_activation_is_reconciled() {
        let (directory, request) = fixture("activation-reconcile");
        let target = request.target_root.clone();
        let mut injected = false;
        let result = run_or_resume_product_migration_with_phase_hook(
            &request,
            &CopyMaintenance::working(),
            &mut |phase| {
                if phase == MigrationPhase::Verified && !injected {
                    let journal: serde_json::Value = serde_json::from_slice(
                        &fs::read(target.join("state/migration.json")).unwrap(),
                    )
                    .unwrap();
                    let id = journal["transactionId"].as_str().unwrap();
                    fs::copy(
                        target
                            .join("cache/migration-stage")
                            .join(id)
                            .join("config.toml"),
                        target.join("config.toml"),
                    )
                    .unwrap();
                    injected = true;
                }
            },
        )
        .unwrap();
        assert_eq!(result.phase, MigrationPhase::Complete);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn production_invalid_config_and_secrets_fail_from_staged_recovery() {
        for (label, relative, bytes, expected) in [
            (
                "invalid-config",
                "agent.toml",
                b"[unknown]\nenabled = true\n".as_slice(),
                MigrationError::ConfigInvalid,
            ),
            (
                "invalid-secrets",
                "secrets.json",
                br#"{"version":1,"unknown":"value"}"#.as_slice(),
                MigrationError::SecretsInvalid,
            ),
        ] {
            let (directory, request) = fixture(label);
            fs::write(request.source_root.join(relative), bytes).unwrap();
            assert_eq!(
                run_or_resume_product_migration(&request, &CopyMaintenance::working()),
                Err(expected)
            );
            let journal: serde_json::Value = serde_json::from_slice(
                &fs::read(request.target_root.join("state/migration.json")).unwrap(),
            )
            .unwrap();
            assert_eq!(journal["phase"], "staged");
            assert_eq!(journal["recoveryCode"], expected.code());
            assert!(!request.target_root.join(relative).exists());
            fs::remove_dir_all(directory).unwrap();
        }
    }

    #[test]
    fn sqlite_wal_is_journaled_and_mutation_during_maintenance_fails_closed() {
        let (directory, request) = fixture("wal-change");
        let wal = request.source_root.join("gateway.sqlite-wal");
        fs::write(&wal, b"initial-wal").unwrap();
        let result =
            run_or_resume_product_migration(&request, &WalMutatingMaintenance { wal: wal.clone() });
        assert_eq!(result, Err(MigrationError::SourceChanged));
        let journal: serde_json::Value = serde_json::from_slice(
            &fs::read(request.target_root.join("state/migration.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(journal["phase"], "detected");
        assert_eq!(journal["recoveryCode"], "LEGACY_MIGRATION_SOURCE_CHANGED");
        let database = journal["entries"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["kind"] == "database")
            .unwrap();
        assert_eq!(
            database["databaseWal"]["sourceRelative"],
            "gateway.sqlite-wal"
        );
        assert!(!request.target_root.join("cmclient.db").exists());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn zero_length_wal_is_valid_but_zero_length_shared_memory_is_not() {
        let (directory, request) = fixture("zero-sidecars");
        fs::write(request.source_root.join("gateway.sqlite-wal"), b"").unwrap();
        let migrated =
            run_or_resume_product_migration(&request, &CopyMaintenance::working()).unwrap();
        assert_eq!(migrated.phase, MigrationPhase::Complete);
        let journal: serde_json::Value = serde_json::from_slice(
            &fs::read(request.target_root.join("state/migration.json")).unwrap(),
        )
        .unwrap();
        let database = journal["entries"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["kind"] == "database")
            .unwrap();
        assert_eq!(database["databaseWal"]["sizeBytes"], 0);
        fs::remove_dir_all(directory).unwrap();

        let (directory, request) = fixture("zero-shm");
        fs::write(request.source_root.join("gateway.sqlite-shm"), b"").unwrap();
        assert_eq!(
            run_or_resume_product_migration(&request, &CopyMaintenance::working()),
            Err(MigrationError::SourceUnsafe)
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn nested_sqlite_and_db_backups_preserve_relative_paths() {
        let (directory, request) = fixture("nested-backups");
        let nested = request.source_root.join("backups/2026/july");
        fs::create_dir_all(&nested).unwrap();
        fs::write(nested.join("two.db"), b"nested-db-backup").unwrap();
        let outcome =
            run_or_resume_product_migration(&request, &CopyMaintenance::working()).unwrap();
        assert_eq!(outcome.file_count, 5);
        assert_eq!(
            fs::read(request.target_root.join("backups/2026/july/two.db")).unwrap(),
            b"nested-db-backup"
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn split_and_single_existing_source_sets_are_one_logical_candidate() {
        let directory = temporary_directory("split-roots");
        let config = directory.join("xdg-config/cmclient");
        let data = directory.join("xdg-data/cmclient");
        let target = directory.join("home/.cmclient");
        fs::create_dir_all(&config).unwrap();
        fs::create_dir_all(data.join("backups/archive")).unwrap();
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        fs::write(
            config.join("agent.toml"),
            b"[agent]\nmanagement_web_enabled = false\n",
        )
        .unwrap();
        fs::write(data.join("gateway.sqlite"), b"split-database").unwrap();
        fs::write(data.join("backups/archive/old.db"), b"split-backup").unwrap();
        let source = ProductMigrationSourceSet {
            config_root: config.clone(),
            data_root: data.clone(),
        };
        let outcome = migrate_detected_product_source_sets(
            &target,
            std::slice::from_ref(&source),
            &CopyMaintenance::working(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(outcome.file_count, 3);
        assert_eq!(
            fs::read(target.join("cmclient.db")).unwrap(),
            b"split-database"
        );
        assert_eq!(
            fs::read(target.join("backups/archive/old.db")).unwrap(),
            b"split-backup"
        );
        assert_eq!(fs::read(config.join("agent.lock")).unwrap(), b"");
        assert_eq!(fs::read(data.join("agent.lock")).unwrap(), b"");

        let data_only = directory.join("data-only");
        let missing_config = directory.join("missing-config");
        let second_target = directory.join("second-home/.cmclient");
        fs::create_dir_all(&data_only).unwrap();
        fs::create_dir_all(second_target.parent().unwrap()).unwrap();
        fs::write(data_only.join("gateway.sqlite"), b"data-only-db").unwrap();
        let data_only_outcome = migrate_detected_product_source_sets(
            &second_target,
            &[ProductMigrationSourceSet {
                config_root: missing_config,
                data_root: data_only,
            }],
            &CopyMaintenance::working(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(data_only_outcome.file_count, 1);
        assert_eq!(
            fs::read(second_target.join("cmclient.db")).unwrap(),
            b"data-only-db"
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn foreign_target_is_rejected_without_creating_scaffold_or_source_lock() {
        let (directory, request) = fixture("foreign-target-preflight");
        fs::create_dir_all(request.target_root.join("foreign/subtree")).unwrap();
        fs::write(
            request.target_root.join("foreign/subtree/state.bin"),
            b"foreign-state",
        )
        .unwrap();
        let before = source_snapshot(&request.target_root);
        assert_eq!(
            run_or_resume_product_migration(&request, &CopyMaintenance::working()),
            Err(MigrationError::TargetPopulated)
        );
        assert_eq!(source_snapshot(&request.target_root), before);
        assert!(!request.source_root.join("agent.lock").exists());
        assert!(!request.target_root.join("run").exists());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn hardlinked_known_source_and_journal_are_rejected_across_directories() {
        let (directory, request) = fixture("hardlink-source");
        let alias_directory = directory.join("alias");
        fs::create_dir_all(&alias_directory).unwrap();
        fs::hard_link(
            request.source_root.join("agent.toml"),
            alias_directory.join("config-alias.toml"),
        )
        .unwrap();
        assert_eq!(
            run_or_resume_product_migration(&request, &CopyMaintenance::working()),
            Err(MigrationError::SourceUnsafe)
        );
        fs::remove_dir_all(directory).unwrap();

        let (directory, request) = fixture("hardlink-journal");
        run_or_resume_product_migration(&request, &CopyMaintenance::working()).unwrap();
        let alias_directory = directory.join("journal-alias");
        fs::create_dir_all(&alias_directory).unwrap();
        fs::hard_link(
            request.target_root.join("state/migration.json"),
            alias_directory.join("migration.json"),
        )
        .unwrap();
        assert_eq!(
            pending_migration_source(&request.target_root),
            Err(MigrationError::JournalInvalid)
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn source_lock_is_empty_and_lock_topology_cannot_change_mid_transaction() {
        let (directory, request) = fixture("nonempty-source-lock");
        fs::write(request.source_root.join("agent.lock"), b"occupied").unwrap();
        assert_eq!(
            run_or_resume_product_migration(&request, &CopyMaintenance::working()),
            Err(MigrationError::SourceUnsafe)
        );
        assert_eq!(
            fs::read(request.source_root.join("agent.lock")).unwrap(),
            b"occupied"
        );
        fs::remove_dir_all(directory).unwrap();

        let (directory, request) = fixture("lock-appeared");
        let run = request.source_root.join("run");
        let result = run_or_resume_product_migration_with_phase_hook(
            &request,
            &CopyMaintenance::working(),
            &mut |phase| {
                if phase == MigrationPhase::Detected {
                    fs::create_dir_all(&run).unwrap();
                    fs::write(run.join("agent.lock"), b"").unwrap();
                }
            },
        );
        assert_eq!(result, Err(MigrationError::SourceChanged));
        assert_eq!(
            fs::read(request.source_root.join("agent.lock")).unwrap(),
            b""
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn complete_journal_symbolic_link_is_rejected_without_following() {
        use std::os::unix::fs::symlink;
        let (directory, request) = fixture("journal-link");
        run_or_resume_product_migration(&request, &CopyMaintenance::working()).unwrap();
        let journal = request.target_root.join("state/migration.json");
        let moved = request.target_root.join("state/migration.real.json");
        fs::rename(&journal, &moved).unwrap();
        symlink("migration.real.json", &journal).unwrap();
        assert_eq!(
            pending_migration_source(&request.target_root),
            Err(MigrationError::JournalInvalid)
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symbolic_known_sources() {
        use std::os::unix::fs::symlink;
        let (directory, request) = fixture("source-link");
        fs::remove_file(request.source_root.join("agent.toml")).unwrap();
        symlink("ignored.log", request.source_root.join("agent.toml")).unwrap();
        assert_eq!(
            run_or_resume_product_migration(&request, &CopyMaintenance::working()),
            Err(MigrationError::SourceUnsafe)
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn report_and_error_validation_are_bounded() {
        let mut report = GatewayMaintenanceReport {
            schema_version: 1,
            message_type: String::from("gateway.offline-maintenance-report"),
            operation: String::from("backup_migrate_verify"),
            source_database_sha256: "a".repeat(64),
            staged_database_sha256: "b".repeat(64),
            staged_database_bytes: 1,
            integrity: String::from("ok"),
            foreign_key_violations: 0,
            schema_history: vec![MaintenanceSchemaHistory {
                version: 1,
                name: String::from("one"),
                sha256: "c".repeat(64),
            }],
            domain_counts: BTreeMap::new(),
        };
        assert!(report.is_valid());
        report.foreign_key_violations = 1;
        assert!(!report.is_valid());
        for error in [
            MigrationError::PathInvalid,
            MigrationError::SourceChanged,
            MigrationError::TargetPopulated,
            MigrationError::MaintenanceFailed,
            MigrationError::ActivationFailed,
        ] {
            assert_eq!(error.to_string(), error.code());
        }
    }
}
