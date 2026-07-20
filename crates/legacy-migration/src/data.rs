//! Explicit, offline migration of safe Legacy history into a prepared Gateway database.

use chrono::{DateTime, SecondsFormat, Utc};
use fs2::FileExt;
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt::{Display, Formatter},
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    time::Duration,
};

const LEGACY_DATA_SCHEMA_VERSION: u8 = 1;
const BACKUP_MANIFEST_SCHEMA_VERSION: u8 = 1;
const SUPPORTED_GATEWAY_SCHEMA_VERSION: i64 = 8;
const MAX_JSON_INPUT_BYTES: u64 = 16 * 1024 * 1024;
const MAX_BACKUP_MANIFEST_BYTES: u64 = 64 * 1024;
const LEGACY_OBSERVATION_JSON: &str = r#"{"schemaVersion":1,"kind":"other"}"#;

struct MigrationLock {
    _file: File,
}

#[derive(Clone, Debug)]
pub struct LegacyDataImportRequest {
    pub source_dir: PathBuf,
    pub target_database: PathBuf,
    pub mesh_network_id: String,
    pub backup_dir: PathBuf,
}

#[derive(Clone, Debug)]
pub struct LegacyDataRollbackRequest {
    pub target_database: PathBuf,
    pub backup_database: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LegacyDataImportReport {
    pub schema_version: u8,
    pub migration_id: String,
    pub dry_run: bool,
    pub mesh_network_id: String,
    pub sources: Vec<LegacyDataSource>,
    pub records: LegacyDataRecordCounts,
    pub skipped: Vec<LegacyDataSkippedCount>,
    pub backup_file: Option<String>,
    pub backup_manifest_file: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LegacyDataRollbackReport {
    pub schema_version: u8,
    pub restored: bool,
    pub backup_file: String,
    pub backup_manifest_file: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LegacyDataSource {
    pub name: String,
    pub sha256: String,
    pub used: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LegacyDataRecordCounts {
    pub nodes: u64,
    pub messages: u64,
    pub telemetry: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LegacyDataSkippedCount {
    pub code: String,
    pub count: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LegacyDataError {
    SourceNotAbsolute,
    TargetNotAbsolute,
    BackupNotAbsolute,
    InvalidNetworkId,
    SourceEmpty,
    SourceReadFailed,
    SourceTooLarge,
    SourceChanged,
    SourceTargetOverlap,
    SourceDatabaseInvalid,
    SourceJsonInvalid,
    NothingImportable,
    TargetMissing,
    TargetDatabaseInvalid,
    TargetSchemaUnsupported,
    TargetIntegrityFailed,
    GatewayStopConfirmationRequired,
    ImportInProgress,
    TargetAlreadyImported,
    BackupAlreadyExists,
    BackupFailed,
    ImportFailed,
    ImportRecoveryRequired,
    ImportVerificationFailed,
    ImportRolledBack,
    RollbackFailed,
}

impl LegacyDataError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::SourceNotAbsolute => "LEGACY_DATA_SOURCE_NOT_ABSOLUTE",
            Self::TargetNotAbsolute => "LEGACY_DATA_TARGET_NOT_ABSOLUTE",
            Self::BackupNotAbsolute => "LEGACY_DATA_BACKUP_NOT_ABSOLUTE",
            Self::InvalidNetworkId => "LEGACY_DATA_NETWORK_ID_INVALID",
            Self::SourceEmpty => "LEGACY_DATA_SOURCE_EMPTY",
            Self::SourceReadFailed => "LEGACY_DATA_SOURCE_READ_FAILED",
            Self::SourceTooLarge => "LEGACY_DATA_SOURCE_TOO_LARGE",
            Self::SourceChanged => "LEGACY_DATA_SOURCE_CHANGED",
            Self::SourceTargetOverlap => "LEGACY_DATA_SOURCE_TARGET_OVERLAP",
            Self::SourceDatabaseInvalid => "LEGACY_DATA_SOURCE_DATABASE_INVALID",
            Self::SourceJsonInvalid => "LEGACY_DATA_SOURCE_JSON_INVALID",
            Self::NothingImportable => "LEGACY_DATA_NOTHING_IMPORTABLE",
            Self::TargetMissing => "LEGACY_DATA_TARGET_MISSING",
            Self::TargetDatabaseInvalid => "LEGACY_DATA_TARGET_DATABASE_INVALID",
            Self::TargetSchemaUnsupported => "LEGACY_DATA_TARGET_SCHEMA_UNSUPPORTED",
            Self::TargetIntegrityFailed => "LEGACY_DATA_TARGET_INTEGRITY_FAILED",
            Self::GatewayStopConfirmationRequired => {
                "LEGACY_DATA_GATEWAY_STOP_CONFIRMATION_REQUIRED"
            }
            Self::ImportInProgress => "LEGACY_DATA_IMPORT_IN_PROGRESS",
            Self::TargetAlreadyImported => "LEGACY_DATA_TARGET_ALREADY_IMPORTED",
            Self::BackupAlreadyExists => "LEGACY_DATA_BACKUP_ALREADY_EXISTS",
            Self::BackupFailed => "LEGACY_DATA_BACKUP_FAILED",
            Self::ImportFailed => "LEGACY_DATA_IMPORT_FAILED",
            Self::ImportRecoveryRequired => "LEGACY_DATA_IMPORT_RECOVERY_REQUIRED",
            Self::ImportVerificationFailed => "LEGACY_DATA_IMPORT_VERIFICATION_FAILED",
            Self::ImportRolledBack => "LEGACY_DATA_IMPORT_ROLLED_BACK",
            Self::RollbackFailed => "LEGACY_DATA_ROLLBACK_FAILED",
        }
    }
}

impl Display for LegacyDataError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.code())
    }
}

impl std::error::Error for LegacyDataError {}

#[derive(Clone, Debug)]
struct ImportPlan {
    migration_id: String,
    mesh_network_id: String,
    sources: Vec<LegacyDataSource>,
    nodes: BTreeMap<u32, LegacyNode>,
    messages: Vec<LegacyMessage>,
    telemetry: Vec<LegacyTelemetry>,
    skipped: BTreeMap<String, u64>,
}

#[derive(Clone, Debug)]
struct LegacyNode {
    node_num: u32,
    user_id: Option<String>,
    long_name: Option<String>,
    short_name: Option<String>,
    hardware_model: Option<String>,
    role: Option<String>,
    seen_at: String,
}

#[derive(Clone, Debug)]
struct LegacyMessage {
    stable_key: String,
    sender: u32,
    destination: Option<u32>,
    packet_id: Option<u32>,
    channel: Option<u8>,
    text: String,
    observed_at: String,
}

#[derive(Clone, Debug)]
struct LegacyTelemetry {
    stable_key: String,
    node_num: u32,
    packet_id: Option<u32>,
    metric_kind: String,
    metrics: BTreeMap<String, serde_json::Value>,
    observed_at: String,
    telemetry_time_seconds: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BackupManifest {
    schema_version: u8,
    migration_id: String,
    mesh_network_id: String,
    target_identity_sha256: String,
    gateway_schema_version: i64,
    backup_file: String,
    backup_sha256: String,
}

/// Reads known Legacy artifacts without modifying either the source or target.
pub fn inspect_legacy_data(
    request: &LegacyDataImportRequest,
) -> Result<LegacyDataImportReport, LegacyDataError> {
    let plan = build_plan(request)?;
    preflight_target(request)?;
    Ok(plan.report(true, None, None))
}

/// Creates a target snapshot, imports a verified historical projection, and then
/// checks the persisted rows. Any failed mutation is restored from the snapshot.
pub fn apply_legacy_data(
    request: &LegacyDataImportRequest,
) -> Result<LegacyDataImportReport, LegacyDataError> {
    let plan = build_plan(request)?;
    let _lock = acquire_migration_lock(&request.target_database)?;
    ensure_sources_unchanged(&plan, &request.source_dir)?;
    ensure_private_directory(&request.backup_dir)?;
    let backup = request
        .backup_dir
        .join(format!("legacy-data-{}.sqlite", plan.migration_id));
    let manifest_path = backup_manifest_path(&backup)?;
    if reconcile_existing_journal(&plan, request, &backup, &manifest_path)? {
        return successful_import_report(&plan, &backup, &manifest_path);
    }
    preflight_target(request)?;
    reserve_backup_path(&backup)?;
    let manifest = create_backup_and_apply(&plan, request, &backup, &manifest_path)?;
    if verify_import(&plan, &manifest, &request.target_database).is_err() {
        let restored = restore_database(&request.target_database, &backup).is_ok();
        return Err(if restored {
            LegacyDataError::ImportRolledBack
        } else {
            LegacyDataError::ImportRecoveryRequired
        });
    }

    successful_import_report(&plan, &backup, &manifest_path)
}

fn successful_import_report(
    plan: &ImportPlan,
    backup: &Path,
    manifest_path: &Path,
) -> Result<LegacyDataImportReport, LegacyDataError> {
    let backup_file = backup
        .file_name()
        .and_then(|name| name.to_str())
        .map(String::from)
        .ok_or(LegacyDataError::BackupFailed)?;
    let backup_manifest_file = manifest_path
        .file_name()
        .and_then(|name| name.to_str())
        .map(String::from)
        .ok_or(LegacyDataError::BackupFailed)?;
    Ok(plan.report(false, Some(backup_file), Some(backup_manifest_file)))
}

fn preflight_target(request: &LegacyDataImportRequest) -> Result<Connection, LegacyDataError> {
    let target = open_target_database(&request.target_database)?;
    validate_target_database(&target)?;
    if has_prior_import(&target, &request.mesh_network_id)? {
        return Err(LegacyDataError::TargetAlreadyImported);
    }
    Ok(target)
}

fn acquire_migration_lock(target: &Path) -> Result<MigrationLock, LegacyDataError> {
    let parent = target.parent().ok_or(LegacyDataError::TargetMissing)?;
    let name = target
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(LegacyDataError::TargetMissing)?;
    let path = parent.join(format!(".{name}.legacy-migration.lock"));
    let file = private_open_options()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(path)
        .map_err(|_| LegacyDataError::TargetMissing)?;
    verify_migration_lock(&file)?;
    file.try_lock_exclusive()
        .map_err(|_| LegacyDataError::ImportInProgress)?;
    Ok(MigrationLock { _file: file })
}

fn reconcile_existing_journal(
    plan: &ImportPlan,
    request: &LegacyDataImportRequest,
    backup: &Path,
    manifest_path: &Path,
) -> Result<bool, LegacyDataError> {
    let backup_exists = backup
        .try_exists()
        .map_err(|_| LegacyDataError::ImportRecoveryRequired)?;
    let manifest_exists = manifest_path
        .try_exists()
        .map_err(|_| LegacyDataError::ImportRecoveryRequired)?;
    match (backup_exists, manifest_exists) {
        (false, false) => return Ok(false),
        (false, true) => return Err(LegacyDataError::ImportRecoveryRequired),
        (true, false) => {
            let target =
                preflight_target(request).map_err(|_| LegacyDataError::ImportRecoveryRequired)?;
            drop(target);
            fs::remove_file(backup).map_err(|_| LegacyDataError::ImportRecoveryRequired)?;
            sync_parent_directory(backup).map_err(|_| LegacyDataError::ImportRecoveryRequired)?;
            return Ok(false);
        }
        (true, true) => {}
    }

    verify_private_path_file(backup, LegacyDataError::ImportRecoveryRequired)?;
    verify_private_path_file(manifest_path, LegacyDataError::ImportRecoveryRequired)?;
    let manifest =
        read_backup_manifest(manifest_path).map_err(|_| LegacyDataError::ImportRecoveryRequired)?;
    verify_manifest_metadata(plan, request, backup, &manifest)?;
    let actual_backup_sha256 =
        sha256_file_with_error(backup, LegacyDataError::ImportRecoveryRequired)?;
    if actual_backup_sha256 != manifest.backup_sha256
        || verify_gateway_database_file(backup, LegacyDataError::ImportRecoveryRequired).is_err()
    {
        return Err(LegacyDataError::ImportRecoveryRequired);
    }

    let target = open_target_database(&request.target_database)
        .map_err(|_| LegacyDataError::ImportRecoveryRequired)?;
    validate_target_database(&target).map_err(|_| LegacyDataError::ImportRecoveryRequired)?;
    match import_marker_value(&target, &manifest)
        .map_err(|_| LegacyDataError::ImportRecoveryRequired)?
    {
        Some(marker) if marker == manifest => {
            verify_import(plan, &manifest, &request.target_database)
                .map_err(|_| LegacyDataError::ImportRecoveryRequired)?;
            Ok(true)
        }
        Some(_) => Err(LegacyDataError::ImportRecoveryRequired),
        None => {
            if has_prior_import(&target, &plan.mesh_network_id)
                .map_err(|_| LegacyDataError::ImportRecoveryRequired)?
            {
                return Err(LegacyDataError::ImportRecoveryRequired);
            }
            drop(target);
            fs::remove_file(manifest_path).map_err(|_| LegacyDataError::ImportRecoveryRequired)?;
            fs::remove_file(backup).map_err(|_| LegacyDataError::ImportRecoveryRequired)?;
            sync_parent_directory(backup).map_err(|_| LegacyDataError::ImportRecoveryRequired)?;
            Ok(false)
        }
    }
}

/// Restores the pre-import target snapshot. The caller must stop the Gateway
/// before invoking this offline operation.
pub fn rollback_legacy_data(
    request: &LegacyDataRollbackRequest,
) -> Result<LegacyDataRollbackReport, LegacyDataError> {
    if !request.target_database.is_absolute() {
        return Err(LegacyDataError::TargetNotAbsolute);
    }
    if !request.backup_database.is_absolute() {
        return Err(LegacyDataError::BackupNotAbsolute);
    }
    let _lock = acquire_migration_lock(&request.target_database)?;
    let manifest_path = backup_manifest_path(&request.backup_database)?;
    let manifest = read_backup_manifest(&manifest_path)?;
    verify_rollback_proof(request, &manifest)?;
    restore_database(&request.target_database, &request.backup_database)?;
    let restored = open_target_database(&request.target_database)
        .map_err(|_| LegacyDataError::RollbackFailed)?;
    validate_target_database(&restored).map_err(|_| LegacyDataError::RollbackFailed)?;
    if import_marker_value(&restored, &manifest)?.is_some() {
        return Err(LegacyDataError::RollbackFailed);
    }
    let backup_file = request
        .backup_database
        .file_name()
        .and_then(|name| name.to_str())
        .map(String::from)
        .ok_or(LegacyDataError::RollbackFailed)?;
    let backup_manifest_file = manifest_path
        .file_name()
        .and_then(|name| name.to_str())
        .map(String::from)
        .ok_or(LegacyDataError::RollbackFailed)?;
    Ok(LegacyDataRollbackReport {
        schema_version: LEGACY_DATA_SCHEMA_VERSION,
        restored: true,
        backup_file,
        backup_manifest_file,
    })
}

impl ImportPlan {
    fn report(
        &self,
        dry_run: bool,
        backup_file: Option<String>,
        backup_manifest_file: Option<String>,
    ) -> LegacyDataImportReport {
        LegacyDataImportReport {
            schema_version: LEGACY_DATA_SCHEMA_VERSION,
            migration_id: self.migration_id.clone(),
            dry_run,
            mesh_network_id: self.mesh_network_id.clone(),
            sources: self.sources.clone(),
            records: LegacyDataRecordCounts {
                nodes: self.nodes.len() as u64,
                messages: self.messages.len() as u64,
                telemetry: self.telemetry.len() as u64,
            },
            skipped: self
                .skipped
                .iter()
                .map(|(code, count)| LegacyDataSkippedCount {
                    code: code.clone(),
                    count: *count,
                })
                .collect(),
            backup_file,
            backup_manifest_file,
        }
    }
}

fn build_plan(request: &LegacyDataImportRequest) -> Result<ImportPlan, LegacyDataError> {
    validate_request(request)?;
    let mut source_paths = source_paths(&request.source_dir)?;
    if source_paths.is_empty() {
        return Err(LegacyDataError::SourceEmpty);
    }
    source_paths.sort_by(|left, right| left.0.cmp(&right.0));
    let mut sources = Vec::with_capacity(source_paths.len());
    for (name, path) in &source_paths {
        sources.push(LegacyDataSource {
            name: name.clone(),
            sha256: sha256_file(path)?,
            used: false,
        });
    }

    let migration_id = migration_id(&request.mesh_network_id, &sources);
    let mut plan = ImportPlan {
        migration_id,
        mesh_network_id: request.mesh_network_id.clone(),
        sources,
        nodes: BTreeMap::new(),
        messages: Vec::new(),
        telemetry: Vec::new(),
        skipped: BTreeMap::new(),
    };

    let callmesh_path = request.source_dir.join("callmesh-data.sqlite");
    let mut callmesh_nodes = false;
    let mut callmesh_messages = false;
    if callmesh_path.exists() {
        let connection = open_source_database(&callmesh_path)?;
        callmesh_nodes = read_callmesh_nodes(&connection, &mut plan)?;
        callmesh_messages = read_callmesh_messages(&connection, &mut plan)?;
        if callmesh_nodes || callmesh_messages {
            mark_source_used(&mut plan.sources, "callmesh-data.sqlite");
            mark_source_used(&mut plan.sources, "callmesh-data.sqlite-wal");
        }
    }

    if !callmesh_nodes {
        let nodes_path = request.source_dir.join("node-database.json");
        if nodes_path.exists() {
            read_node_json(&nodes_path, &mut plan)?;
            mark_source_used(&mut plan.sources, "node-database.json");
        }
    }
    if !callmesh_messages {
        read_message_jsonl_fallbacks(&request.source_dir, &mut plan)?;
    }

    let telemetry_path = request.source_dir.join("telemetry-records.sqlite");
    let mut telemetry_loaded = false;
    if telemetry_path.exists() {
        let connection = open_source_database(&telemetry_path)?;
        telemetry_loaded = read_telemetry_database(&connection, &mut plan)?;
        if telemetry_loaded {
            mark_source_used(&mut plan.sources, "telemetry-records.sqlite");
            mark_source_used(&mut plan.sources, "telemetry-records.sqlite-wal");
        }
    }
    if !telemetry_loaded {
        read_telemetry_jsonl_fallbacks(&request.source_dir, &mut plan)?;
    }

    plan.messages
        .sort_by(|left, right| left.stable_key.cmp(&right.stable_key));
    let message_count = plan.messages.len();
    plan.messages
        .dedup_by(|left, right| left.stable_key == right.stable_key);
    add_skipped_count(
        &mut plan.skipped,
        "LEGACY_DATA_MESSAGE_DUPLICATE_SKIPPED",
        message_count - plan.messages.len(),
    );
    plan.telemetry
        .sort_by(|left, right| left.stable_key.cmp(&right.stable_key));
    let telemetry_count = plan.telemetry.len();
    plan.telemetry
        .dedup_by(|left, right| left.stable_key == right.stable_key);
    add_skipped_count(
        &mut plan.skipped,
        "LEGACY_DATA_TELEMETRY_DUPLICATE_SKIPPED",
        telemetry_count - plan.telemetry.len(),
    );
    if plan.nodes.is_empty() && plan.messages.is_empty() && plan.telemetry.is_empty() {
        return Err(LegacyDataError::NothingImportable);
    }
    Ok(plan)
}

fn validate_request(request: &LegacyDataImportRequest) -> Result<(), LegacyDataError> {
    if !request.source_dir.is_absolute() {
        return Err(LegacyDataError::SourceNotAbsolute);
    }
    if !request.target_database.is_absolute() {
        return Err(LegacyDataError::TargetNotAbsolute);
    }
    if !request.backup_dir.is_absolute() {
        return Err(LegacyDataError::BackupNotAbsolute);
    }
    if request.mesh_network_id.is_empty()
        || request.mesh_network_id.len() > 128
        || request.mesh_network_id.chars().any(char::is_control)
    {
        return Err(LegacyDataError::InvalidNetworkId);
    }
    if !request.source_dir.is_dir() {
        return Err(LegacyDataError::SourceReadFailed);
    }
    if !request.target_database.is_file() {
        return Err(LegacyDataError::TargetMissing);
    }
    for (_, source) in source_paths(&request.source_dir)? {
        if same_file::is_same_file(source, &request.target_database)
            .map_err(|_| LegacyDataError::SourceReadFailed)?
        {
            return Err(LegacyDataError::SourceTargetOverlap);
        }
    }
    Ok(())
}

fn source_paths(source_dir: &Path) -> Result<Vec<(String, PathBuf)>, LegacyDataError> {
    let mut paths = Vec::new();
    for name in [
        "callmesh-data.sqlite",
        "callmesh-data.sqlite-wal",
        "node-database.json",
        "message-log.jsonl",
        "message-log.jsonl.migrated",
        "telemetry-records.sqlite",
        "telemetry-records.sqlite-wal",
        "telemetry-records.jsonl",
        "telemetry-records.jsonl.migrated",
    ] {
        let path = source_dir.join(name);
        match fs::metadata(&path) {
            Ok(metadata) if metadata.is_file() => paths.push((String::from(name), path)),
            Ok(_) => return Err(LegacyDataError::SourceReadFailed),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(LegacyDataError::SourceReadFailed),
        }
    }
    Ok(paths)
}

fn source_manifest(source_dir: &Path) -> Result<Vec<LegacyDataSource>, LegacyDataError> {
    let mut paths = source_paths(source_dir)?;
    paths.sort_by(|left, right| left.0.cmp(&right.0));
    paths
        .into_iter()
        .map(|(name, path)| {
            Ok(LegacyDataSource {
                name,
                sha256: sha256_file(&path)?,
                used: false,
            })
        })
        .collect()
}

fn ensure_sources_unchanged(plan: &ImportPlan, source_dir: &Path) -> Result<(), LegacyDataError> {
    let current = source_manifest(source_dir)?;
    let expected = plan
        .sources
        .iter()
        .map(|source| (&source.name, &source.sha256))
        .collect::<Vec<_>>();
    let actual = current
        .iter()
        .map(|source| (&source.name, &source.sha256))
        .collect::<Vec<_>>();
    if expected == actual {
        Ok(())
    } else {
        Err(LegacyDataError::SourceChanged)
    }
}

fn reserve_backup_path(path: &Path) -> Result<(), LegacyDataError> {
    let file = create_private_file(path, LegacyDataError::BackupFailed)?;
    verify_private_file(&file, LegacyDataError::BackupFailed)
}

#[cfg(unix)]
fn private_open_options() -> OpenOptions {
    use std::os::unix::fs::OpenOptionsExt;

    let mut options = OpenOptions::new();
    options.mode(0o600);
    options
}

#[cfg(unix)]
fn verify_migration_lock(file: &File) -> Result<(), LegacyDataError> {
    verify_private_file(file, LegacyDataError::TargetMissing)
}

#[cfg(windows)]
fn verify_migration_lock(_file: &File) -> Result<(), LegacyDataError> {
    Ok(())
}

#[cfg(windows)]
fn private_open_options() -> OpenOptions {
    OpenOptions::new()
}

#[cfg(unix)]
fn create_private_file(path: &Path, error: LegacyDataError) -> Result<File, LegacyDataError> {
    use std::os::unix::fs::OpenOptionsExt;

    private_open_options()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(path)
        .map_err(|io_error| {
            if io_error.kind() == std::io::ErrorKind::AlreadyExists {
                LegacyDataError::BackupAlreadyExists
            } else {
                error
            }
        })
}

#[cfg(not(unix))]
fn create_private_file(path: &Path, error: LegacyDataError) -> Result<File, LegacyDataError> {
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|io_error| {
            if io_error.kind() == std::io::ErrorKind::AlreadyExists {
                LegacyDataError::BackupAlreadyExists
            } else {
                error
            }
        })?;
    if verify_private_file(&file, error).is_err() {
        drop(file);
        let _ = fs::remove_file(path);
        return Err(error);
    }
    Ok(file)
}

fn sha256_file(path: &Path) -> Result<String, LegacyDataError> {
    sha256_file_with_error(path, LegacyDataError::SourceReadFailed)
}

fn sha256_file_with_error(path: &Path, error: LegacyDataError) -> Result<String, LegacyDataError> {
    let mut file = File::open(path).map_err(|_| error)?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 32 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(|_| error)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(hex_digest(digest.finalize()))
}

fn ensure_private_directory(path: &Path) -> Result<(), LegacyDataError> {
    match fs::metadata(path) {
        Ok(metadata) if metadata.is_dir() => verify_private_directory(path, &metadata),
        Ok(_) => Err(LegacyDataError::BackupFailed),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            create_private_directory(path)?;
            let metadata = fs::metadata(path).map_err(|_| LegacyDataError::BackupFailed)?;
            if verify_private_directory(path, &metadata).is_err() {
                let _ = fs::remove_dir(path);
                return Err(LegacyDataError::BackupFailed);
            }
            Ok(())
        }
        Err(_) => Err(LegacyDataError::BackupFailed),
    }
}

#[cfg(unix)]
fn create_private_directory(path: &Path) -> Result<(), LegacyDataError> {
    use std::os::unix::fs::DirBuilderExt;

    let mut builder = fs::DirBuilder::new();
    builder.mode(0o700);
    builder
        .create(path)
        .map_err(|_| LegacyDataError::BackupFailed)
}

#[cfg(not(unix))]
fn create_private_directory(path: &Path) -> Result<(), LegacyDataError> {
    fs::create_dir(path).map_err(|_| LegacyDataError::BackupFailed)
}

#[cfg(unix)]
fn verify_private_directory(_path: &Path, metadata: &fs::Metadata) -> Result<(), LegacyDataError> {
    use std::os::unix::fs::MetadataExt;

    if metadata.uid() == rustix::process::geteuid().as_raw() && metadata.mode() & 0o077 == 0 {
        Ok(())
    } else {
        Err(LegacyDataError::BackupFailed)
    }
}

#[cfg(windows)]
fn verify_private_directory(path: &Path, _metadata: &fs::Metadata) -> Result<(), LegacyDataError> {
    verify_private_windows_acl(path)
}

#[cfg(unix)]
fn verify_private_file(file: &File, error: LegacyDataError) -> Result<(), LegacyDataError> {
    use std::os::unix::fs::MetadataExt;

    let metadata = file.metadata().map_err(|_| error)?;
    if metadata.uid() == rustix::process::geteuid().as_raw() && metadata.mode() & 0o077 == 0 {
        Ok(())
    } else {
        Err(error)
    }
}

fn verify_private_path_file(path: &Path, error: LegacyDataError) -> Result<(), LegacyDataError> {
    let file = File::open(path).map_err(|_| error)?;
    verify_private_file(&file, error)
}

#[cfg(windows)]
fn verify_private_file(file: &File, error: LegacyDataError) -> Result<(), LegacyDataError> {
    verify_private_windows_acl_handle(file).map_err(|_| error)
}

#[cfg(windows)]
fn verify_private_windows_acl(path: &Path) -> Result<(), LegacyDataError> {
    use windows_acl::{
        acl::{ACL, AceType},
        helper::{current_user, name_to_sid, sid_to_string},
    };

    let path = path.to_str().ok_or(LegacyDataError::BackupFailed)?;
    let acl = ACL::from_file_path(path, false).map_err(|_| LegacyDataError::BackupFailed)?;
    let user = current_user().ok_or(LegacyDataError::BackupFailed)?;
    let user_sid = name_to_sid(&user, None).map_err(|_| LegacyDataError::BackupFailed)?;
    let user_sid = sid_to_string(user_sid.as_ptr().cast_mut().cast())
        .map_err(|_| LegacyDataError::BackupFailed)?;
    let allowed = [user_sid.as_str(), "S-1-5-18", "S-1-5-32-544"];
    let entries = acl.all().map_err(|_| LegacyDataError::BackupFailed)?;
    if !entries.is_empty()
        && entries.iter().any(|entry| entry.string_sid == user_sid)
        && entries.iter().all(|entry| {
            entry.entry_type == AceType::AccessAllow && allowed.contains(&entry.string_sid.as_str())
        })
    {
        Ok(())
    } else {
        Err(LegacyDataError::BackupFailed)
    }
}

#[cfg(windows)]
fn verify_private_windows_acl_handle(file: &File) -> Result<(), LegacyDataError> {
    use std::os::windows::io::AsRawHandle;
    use windows_acl::{
        acl::{ACL, AceType},
        helper::{current_user, name_to_sid, sid_to_string},
    };

    let acl = ACL::from_file_handle(file.as_raw_handle().cast(), false)
        .map_err(|_| LegacyDataError::BackupFailed)?;
    let user = current_user().ok_or(LegacyDataError::BackupFailed)?;
    let user_sid = name_to_sid(&user, None).map_err(|_| LegacyDataError::BackupFailed)?;
    let user_sid = sid_to_string(user_sid.as_ptr().cast_mut().cast())
        .map_err(|_| LegacyDataError::BackupFailed)?;
    let allowed = [user_sid.as_str(), "S-1-5-18", "S-1-5-32-544"];
    let entries = acl.all().map_err(|_| LegacyDataError::BackupFailed)?;
    if !entries.is_empty()
        && entries.iter().any(|entry| entry.string_sid == user_sid)
        && entries.iter().all(|entry| {
            entry.entry_type == AceType::AccessAllow && allowed.contains(&entry.string_sid.as_str())
        })
    {
        Ok(())
    } else {
        Err(LegacyDataError::BackupFailed)
    }
}

fn migration_id(mesh_network_id: &str, sources: &[LegacyDataSource]) -> String {
    let mut digest = Sha256::new();
    digest.update(mesh_network_id.as_bytes());
    for source in sources {
        digest.update([0]);
        digest.update(source.name.as_bytes());
        digest.update([0]);
        digest.update(source.sha256.as_bytes());
    }
    hex_digest(digest.finalize())[..24].to_owned()
}

fn hex_digest(bytes: impl AsRef<[u8]>) -> String {
    bytes
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn mark_source_used(sources: &mut [LegacyDataSource], name: &str) {
    if let Some(source) = sources.iter_mut().find(|source| source.name == name) {
        source.used = true;
    }
}

fn open_source_database(path: &Path) -> Result<Connection, LegacyDataError> {
    Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|_| LegacyDataError::SourceDatabaseInvalid)
}

fn read_callmesh_nodes(
    connection: &Connection,
    plan: &mut ImportPlan,
) -> Result<bool, LegacyDataError> {
    if !table_has_columns(connection, "nodes", &["mesh_id", "last_seen_at"])? {
        return Ok(false);
    }
    let columns = table_columns(connection, "nodes")?;
    let query = format!(
        "SELECT mesh_id, {}, {}, {}, {}, {}, last_seen_at FROM nodes",
        select_or_null(&columns, "mesh_id_original"),
        select_or_null(&columns, "long_name"),
        select_or_null(&columns, "short_name"),
        select_or_null(&columns, "hw_model"),
        select_or_null(&columns, "role"),
    );
    let mut statement = connection
        .prepare(&query)
        .map_err(|_| LegacyDataError::SourceDatabaseInvalid)?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, Option<i64>>(6)?,
            ))
        })
        .map_err(|_| LegacyDataError::SourceDatabaseInvalid)?;
    let before = plan.nodes.len();
    for row in rows {
        let (mesh_id, original, long_name, short_name, model, role, seen_at) =
            row.map_err(|_| LegacyDataError::SourceDatabaseInvalid)?;
        add_node(
            plan,
            mesh_id.as_deref(),
            original.or_else(|| mesh_id.clone()),
            long_name,
            short_name,
            model,
            role,
            seen_at.and_then(iso_from_millis),
        );
    }
    Ok(plan.nodes.len() > before)
}

fn read_callmesh_messages(
    connection: &Connection,
    plan: &mut ImportPlan,
) -> Result<bool, LegacyDataError> {
    let log_columns = table_columns(connection, "message_log")?;
    if log_columns.contains("data") {
        return read_inline_message_database(connection, plan);
    }
    if !table_has_columns(
        connection,
        "message_log",
        &["flow_id", "timestamp_ms", "type", "detail"],
    )? || !table_has_columns(connection, "message_nodes", &["flow_id", "role"])?
    {
        return Ok(false);
    }
    let node_columns = table_columns(connection, "message_nodes")?;
    let sender = ["mesh_id_normalized", "mesh_id", "mesh_id_original"]
        .into_iter()
        .filter(|column| node_columns.contains(*column))
        .map(|column| format!("from_node.{column}"))
        .collect::<Vec<_>>();
    if sender.is_empty() {
        return Ok(false);
    }
    let sender = if sender.len() == 1 {
        sender[0].clone()
    } else {
        format!("COALESCE({})", sender.join(", "))
    };
    let query = format!(
        "SELECT log.flow_id, log.timestamp_ms, log.type, log.detail, {}, {}, {sender} \
         FROM message_log log JOIN message_nodes from_node ON from_node.flow_id = log.flow_id AND from_node.role = 'from'",
        select_or_null_prefixed(&log_columns, "log", "mesh_packet_id"),
        select_or_null_prefixed(&log_columns, "log", "channel"),
    );
    let mut statement = connection
        .prepare(&query)
        .map_err(|_| LegacyDataError::SourceDatabaseInvalid)?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, Option<i64>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<i64>>(4)?,
                row.get::<_, Option<i64>>(5)?,
                row.get::<_, Option<String>>(6)?,
            ))
        })
        .map_err(|_| LegacyDataError::SourceDatabaseInvalid)?;
    let before = plan.messages.len();
    for row in rows {
        let (flow_id, timestamp, kind, detail, packet_id, channel, sender) =
            row.map_err(|_| LegacyDataError::SourceDatabaseInvalid)?;
        add_message(
            plan,
            flow_id.as_deref(),
            kind.as_deref(),
            detail,
            sender.as_deref(),
            None,
            packet_id,
            channel,
            timestamp.and_then(iso_from_millis),
            "callmesh-message",
        );
    }
    Ok(plan.messages.len() > before)
}

fn read_inline_message_database(
    connection: &Connection,
    plan: &mut ImportPlan,
) -> Result<bool, LegacyDataError> {
    if !table_has_columns(
        connection,
        "message_log",
        &["flow_id", "channel", "timestamp_ms", "data"],
    )? {
        return Ok(false);
    }
    let mut statement = connection
        .prepare("SELECT flow_id, channel, timestamp_ms, data FROM message_log")
        .map_err(|_| LegacyDataError::SourceDatabaseInvalid)?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, Option<i64>>(1)?,
                row.get::<_, Option<i64>>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })
        .map_err(|_| LegacyDataError::SourceDatabaseInvalid)?;
    let before = plan.messages.len();
    for row in rows {
        let (flow_id, channel, timestamp, data) =
            row.map_err(|_| LegacyDataError::SourceDatabaseInvalid)?;
        let Some(data) = data else {
            increment_skip(plan, "LEGACY_DATA_MESSAGE_INVALID");
            continue;
        };
        let Ok(value) = serde_json::from_str::<serde_json::Value>(&data) else {
            increment_skip(plan, "LEGACY_DATA_MESSAGE_INVALID");
            continue;
        };
        add_message_from_value(
            plan,
            flow_id.as_deref(),
            channel,
            timestamp.and_then(iso_from_millis),
            &value,
            "message-inline",
        );
    }
    Ok(plan.messages.len() > before)
}

fn read_telemetry_database(
    connection: &Connection,
    plan: &mut ImportPlan,
) -> Result<bool, LegacyDataError> {
    if !table_has_columns(
        connection,
        "telemetry_records",
        &["id", "mesh_id", "timestamp_ms"],
    )? {
        return Ok(false);
    }
    let columns = table_columns(connection, "telemetry_records")?;
    if columns.contains("data") {
        return read_inline_telemetry_database(connection, plan);
    }
    if !table_has_columns(
        connection,
        "telemetry_metrics",
        &[
            "record_id",
            "metric_key",
            "number_value",
            "text_value",
            "json_value",
        ],
    )? {
        return Ok(false);
    }
    let query = format!(
        "SELECT id, mesh_id, {}, timestamp_ms, {}, {} FROM telemetry_records",
        select_or_null(&columns, "node_mesh_id"),
        select_or_null(&columns, "telemetry_kind"),
        select_or_null(&columns, "telemetry_time_seconds"),
    );
    let mut metrics = HashMap::<String, BTreeMap<String, serde_json::Value>>::new();
    let mut metric_statement = connection
        .prepare("SELECT record_id, metric_key, number_value, text_value, json_value FROM telemetry_metrics")
        .map_err(|_| LegacyDataError::SourceDatabaseInvalid)?;
    let metric_rows = metric_statement
        .query_map([], |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<f64>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
            ))
        })
        .map_err(|_| LegacyDataError::SourceDatabaseInvalid)?;
    for row in metric_rows {
        let (record_id, key, number, text, json) =
            row.map_err(|_| LegacyDataError::SourceDatabaseInvalid)?;
        let Some(record_id) = bounded_text(record_id, 256) else {
            increment_skip(plan, "LEGACY_DATA_TELEMETRY_METRIC_INVALID");
            continue;
        };
        let Some(key) = bounded_text(key, 96) else {
            increment_skip(plan, "LEGACY_DATA_TELEMETRY_METRIC_INVALID");
            continue;
        };
        let value = match number {
            Some(value) if value.is_finite() => Some(serde_json::json!(value)),
            _ => text
                .and_then(|value| bounded_payload_text(Some(value), 512))
                .map(serde_json::Value::String)
                .or_else(|| {
                    json.and_then(|value| serde_json::from_str::<serde_json::Value>(&value).ok())
                        .and_then(|value| value.as_bool().map(serde_json::Value::Bool))
                }),
        };
        if let Some(value) = value {
            metrics.entry(record_id).or_default().insert(key, value);
        } else {
            increment_skip(plan, "LEGACY_DATA_TELEMETRY_METRIC_INVALID");
        }
    }

    let mut statement = connection
        .prepare(&query)
        .map_err(|_| LegacyDataError::SourceDatabaseInvalid)?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<i64>>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<i64>>(5)?,
            ))
        })
        .map_err(|_| LegacyDataError::SourceDatabaseInvalid)?;
    let before = plan.telemetry.len();
    for row in rows {
        let (id, mesh_id, node_mesh_id, timestamp, kind, telemetry_time) =
            row.map_err(|_| LegacyDataError::SourceDatabaseInvalid)?;
        let metrics = id
            .as_ref()
            .and_then(|id| metrics.remove(id))
            .unwrap_or_default();
        add_telemetry(
            plan,
            id.as_deref(),
            node_mesh_id.as_deref().or(mesh_id.as_deref()),
            kind,
            metrics,
            timestamp.and_then(iso_from_millis),
            telemetry_time.and_then(positive_u32),
            "telemetry-record",
        );
    }
    Ok(plan.telemetry.len() > before)
}

fn read_inline_telemetry_database(
    connection: &Connection,
    plan: &mut ImportPlan,
) -> Result<bool, LegacyDataError> {
    let mut statement = connection
        .prepare("SELECT id, mesh_id, timestamp_ms, data FROM telemetry_records")
        .map_err(|_| LegacyDataError::SourceDatabaseInvalid)?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<i64>>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })
        .map_err(|_| LegacyDataError::SourceDatabaseInvalid)?;
    let before = plan.telemetry.len();
    for row in rows {
        let (id, mesh_id, timestamp, data) =
            row.map_err(|_| LegacyDataError::SourceDatabaseInvalid)?;
        let Some(data) = data else {
            increment_skip(plan, "LEGACY_DATA_TELEMETRY_INVALID");
            continue;
        };
        let Ok(value) = serde_json::from_str::<serde_json::Value>(&data) else {
            increment_skip(plan, "LEGACY_DATA_TELEMETRY_INVALID");
            continue;
        };
        add_telemetry_from_value(
            plan,
            id.as_deref(),
            mesh_id.as_deref(),
            timestamp.and_then(iso_from_millis),
            &value,
            "telemetry-inline",
        );
    }
    Ok(plan.telemetry.len() > before)
}

fn read_node_json(path: &Path, plan: &mut ImportPlan) -> Result<(), LegacyDataError> {
    let value = read_json_file(path)?;
    let entries = value
        .as_array()
        .or_else(|| value.get("nodes").and_then(serde_json::Value::as_array))
        .ok_or(LegacyDataError::SourceJsonInvalid)?;
    for entry in entries {
        let Some(entry) = entry.as_object() else {
            increment_skip(plan, "LEGACY_DATA_NODE_INVALID");
            continue;
        };
        add_node(
            plan,
            value_string(entry.get("meshId")).as_deref(),
            value_string(entry.get("meshIdOriginal")).or_else(|| value_string(entry.get("meshId"))),
            value_string(entry.get("longName")),
            value_string(entry.get("shortName")),
            value_string(entry.get("hwModel")),
            value_string(entry.get("role")),
            iso_from_json(entry.get("lastSeenAt")),
        );
    }
    Ok(())
}

fn read_message_jsonl(path: &Path, plan: &mut ImportPlan) -> Result<(), LegacyDataError> {
    for value in read_json_lines(path, plan, "LEGACY_DATA_MESSAGE_JSON_INVALID")? {
        add_message_from_value(plan, None, None, None, &value, "message-log");
    }
    Ok(())
}

fn read_message_jsonl_fallbacks(
    source_dir: &Path,
    plan: &mut ImportPlan,
) -> Result<(), LegacyDataError> {
    for name in ["message-log.jsonl", "message-log.jsonl.migrated"] {
        let path = source_dir.join(name);
        if !path.is_file() {
            continue;
        }
        let before = plan.messages.len();
        read_message_jsonl(&path, plan)?;
        if plan.messages.len() > before {
            mark_source_used(&mut plan.sources, name);
            return Ok(());
        }
    }
    Ok(())
}

fn add_message_from_value(
    plan: &mut ImportPlan,
    fallback_id: Option<&str>,
    fallback_channel: Option<i64>,
    fallback_observed_at: Option<String>,
    value: &serde_json::Value,
    source_kind: &str,
) {
    let Some(entry) = value.as_object() else {
        increment_skip(plan, "LEGACY_DATA_MESSAGE_INVALID");
        return;
    };
    let sender = entry
        .get("from")
        .and_then(serde_json::Value::as_object)
        .and_then(|node| {
            value_string(node.get("meshId")).or_else(|| value_string(node.get("meshIdNormalized")))
        });
    let source_id = value_string(entry.get("flowId")).or_else(|| fallback_id.map(String::from));
    let channel = value_i64(entry.get("channel")).or(fallback_channel);
    let observed_at = iso_from_json(entry.get("timestampMs")).or(fallback_observed_at);
    add_message(
        plan,
        source_id.as_deref(),
        value_string(entry.get("type")).as_deref(),
        value_string(entry.get("detail")),
        sender.as_deref(),
        None,
        value_i64(entry.get("meshPacketId")),
        channel,
        observed_at,
        source_kind,
    );
}

fn read_telemetry_jsonl(path: &Path, plan: &mut ImportPlan) -> Result<(), LegacyDataError> {
    for value in read_json_lines(path, plan, "LEGACY_DATA_TELEMETRY_JSON_INVALID")? {
        add_telemetry_from_value(plan, None, None, None, &value, "telemetry-log");
    }
    Ok(())
}

fn read_telemetry_jsonl_fallbacks(
    source_dir: &Path,
    plan: &mut ImportPlan,
) -> Result<(), LegacyDataError> {
    for name in [
        "telemetry-records.jsonl",
        "telemetry-records.jsonl.migrated",
    ] {
        let path = source_dir.join(name);
        if !path.is_file() {
            continue;
        }
        let before = plan.telemetry.len();
        read_telemetry_jsonl(&path, plan)?;
        if plan.telemetry.len() > before {
            mark_source_used(&mut plan.sources, name);
            return Ok(());
        }
    }
    Ok(())
}

fn add_telemetry_from_value(
    plan: &mut ImportPlan,
    fallback_id: Option<&str>,
    fallback_mesh_id: Option<&str>,
    fallback_observed_at: Option<String>,
    value: &serde_json::Value,
    source_kind: &str,
) {
    let Some(entry) = value.as_object() else {
        increment_skip(plan, "LEGACY_DATA_TELEMETRY_INVALID");
        return;
    };
    let telemetry = entry
        .get("telemetry")
        .and_then(serde_json::Value::as_object);
    let metrics = telemetry
        .and_then(|telemetry| telemetry.get("metrics"))
        .and_then(serde_json::Value::as_object)
        .map(safe_metrics)
        .unwrap_or_default();
    let node_mesh_id = entry
        .get("node")
        .and_then(serde_json::Value::as_object)
        .and_then(|node| value_string(node.get("meshId")))
        .or_else(|| value_string(entry.get("meshId")))
        .or_else(|| fallback_mesh_id.map(String::from));
    let kind = telemetry.and_then(|telemetry| value_string(telemetry.get("kind")));
    let source_id = value_string(entry.get("id")).or_else(|| fallback_id.map(String::from));
    let observed_at = iso_from_json(entry.get("timestampMs")).or(fallback_observed_at);
    let telemetry_time_seconds = telemetry
        .and_then(|telemetry| value_i64(telemetry.get("timeSeconds")))
        .and_then(positive_u32);
    add_telemetry(
        plan,
        source_id.as_deref(),
        node_mesh_id.as_deref(),
        kind,
        metrics,
        observed_at,
        telemetry_time_seconds,
        source_kind,
    );
}

fn read_json_file(path: &Path) -> Result<serde_json::Value, LegacyDataError> {
    let metadata = fs::metadata(path).map_err(|_| LegacyDataError::SourceReadFailed)?;
    if metadata.len() > MAX_JSON_INPUT_BYTES {
        return Err(LegacyDataError::SourceTooLarge);
    }
    let bytes = fs::read(path).map_err(|_| LegacyDataError::SourceReadFailed)?;
    serde_json::from_slice(&bytes).map_err(|_| LegacyDataError::SourceJsonInvalid)
}

fn read_json_lines(
    path: &Path,
    plan: &mut ImportPlan,
    invalid_code: &str,
) -> Result<Vec<serde_json::Value>, LegacyDataError> {
    let metadata = fs::metadata(path).map_err(|_| LegacyDataError::SourceReadFailed)?;
    if metadata.len() > MAX_JSON_INPUT_BYTES {
        return Err(LegacyDataError::SourceTooLarge);
    }
    let content = fs::read_to_string(path).map_err(|_| LegacyDataError::SourceReadFailed)?;
    let mut values = Vec::new();
    for line in content.lines().filter(|line| !line.trim().is_empty()) {
        match serde_json::from_str(line) {
            Ok(value) => values.push(value),
            Err(_) => increment_skip(plan, invalid_code),
        }
    }
    Ok(values)
}

#[allow(clippy::too_many_arguments)]
fn add_node(
    plan: &mut ImportPlan,
    mesh_id: Option<&str>,
    user_id: Option<String>,
    long_name: Option<String>,
    short_name: Option<String>,
    hardware_model: Option<String>,
    role: Option<String>,
    seen_at: Option<String>,
) {
    let Some(node_num) = mesh_id.and_then(parse_node_number) else {
        increment_skip(plan, "LEGACY_DATA_NODE_IDENTIFIER_INVALID");
        return;
    };
    let Some(seen_at) = seen_at else {
        increment_skip(plan, "LEGACY_DATA_NODE_TIME_INVALID");
        return;
    };
    let node = LegacyNode {
        node_num,
        user_id: bounded_text(user_id, 128),
        long_name: bounded_text(long_name, 256),
        short_name: bounded_text(short_name, 64),
        hardware_model: bounded_text(hardware_model, 128),
        role: bounded_text(role, 128),
        seen_at,
    };
    match plan.nodes.get(&node_num) {
        Some(existing) if existing.seen_at >= node.seen_at => {}
        _ => {
            plan.nodes.insert(node_num, node);
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn add_message(
    plan: &mut ImportPlan,
    source_id: Option<&str>,
    kind: Option<&str>,
    text: Option<String>,
    sender: Option<&str>,
    destination: Option<i64>,
    packet_id: Option<i64>,
    channel: Option<i64>,
    observed_at: Option<String>,
    source_kind: &str,
) {
    if !kind.is_some_and(|kind| kind.to_ascii_lowercase().contains("text")) {
        increment_skip(plan, "LEGACY_DATA_MESSAGE_KIND_UNSUPPORTED");
        return;
    }
    let Some(source_id) = bounded_text(source_id.map(String::from), 256) else {
        increment_skip(plan, "LEGACY_DATA_MESSAGE_IDENTIFIER_INVALID");
        return;
    };
    let Some(sender) = sender.and_then(parse_node_number) else {
        increment_skip(plan, "LEGACY_DATA_MESSAGE_SENDER_INVALID");
        return;
    };
    let Some(text) = bounded_payload_text(text, 512).filter(|text| !text.is_empty()) else {
        increment_skip(plan, "LEGACY_DATA_MESSAGE_TEXT_INVALID");
        return;
    };
    let Some(observed_at) = observed_at else {
        increment_skip(plan, "LEGACY_DATA_MESSAGE_TIME_INVALID");
        return;
    };
    let Some(channel) = channel.and_then(|value| u8::try_from(value).ok()) else {
        increment_skip(plan, "LEGACY_DATA_MESSAGE_CHANNEL_INVALID");
        return;
    };
    plan.messages.push(LegacyMessage {
        stable_key: stable_key(source_kind, &source_id),
        sender,
        destination: destination.and_then(to_u32),
        packet_id: packet_id.and_then(to_u32),
        channel: Some(channel),
        text,
        observed_at,
    });
}

#[allow(clippy::too_many_arguments)]
fn add_telemetry(
    plan: &mut ImportPlan,
    source_id: Option<&str>,
    node_mesh_id: Option<&str>,
    kind: Option<String>,
    metrics: BTreeMap<String, serde_json::Value>,
    observed_at: Option<String>,
    telemetry_time_seconds: Option<u32>,
    source_kind: &str,
) {
    let Some(source_id) = bounded_text(source_id.map(String::from), 256) else {
        increment_skip(plan, "LEGACY_DATA_TELEMETRY_IDENTIFIER_INVALID");
        return;
    };
    let Some(node_num) = node_mesh_id.and_then(parse_node_number) else {
        increment_skip(plan, "LEGACY_DATA_TELEMETRY_NODE_INVALID");
        return;
    };
    let Some(metric_kind) = bounded_text(kind, 64) else {
        increment_skip(plan, "LEGACY_DATA_TELEMETRY_KIND_INVALID");
        return;
    };
    let Some(observed_at) = observed_at else {
        increment_skip(plan, "LEGACY_DATA_TELEMETRY_TIME_INVALID");
        return;
    };
    if metrics.is_empty() {
        increment_skip(plan, "LEGACY_DATA_TELEMETRY_METRICS_INVALID");
        return;
    }
    plan.telemetry.push(LegacyTelemetry {
        stable_key: stable_key(source_kind, &source_id),
        node_num,
        packet_id: None,
        metric_kind,
        metrics,
        observed_at,
        telemetry_time_seconds,
    });
}

fn safe_metrics(
    values: &serde_json::Map<String, serde_json::Value>,
) -> BTreeMap<String, serde_json::Value> {
    values
        .iter()
        .filter_map(|(key, value)| {
            let key = bounded_text(Some(key.clone()), 96)?;
            match value {
                serde_json::Value::Number(number)
                    if number.as_f64().is_some_and(f64::is_finite) =>
                {
                    Some((key, value.clone()))
                }
                serde_json::Value::String(value) => bounded_payload_text(Some(value.clone()), 512)
                    .map(|value| (key, serde_json::Value::String(value))),
                serde_json::Value::Bool(_) => Some((key, value.clone())),
                _ => None,
            }
        })
        .collect()
}

fn increment_skip(plan: &mut ImportPlan, code: &str) {
    *plan.skipped.entry(String::from(code)).or_insert(0) += 1;
}

fn add_skipped_count(skipped: &mut BTreeMap<String, u64>, code: &str, count: usize) {
    if count > 0 {
        *skipped.entry(String::from(code)).or_insert(0) += count as u64;
    }
}

fn stable_key(kind: &str, source_id: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(kind.as_bytes());
    digest.update([0]);
    digest.update(source_id.as_bytes());
    hex_digest(digest.finalize())
}

fn stable_id(migration_id: &str, kind: &str, stable_key: &str) -> String {
    format!("legacy-{kind}-{migration_id}-{}", &stable_key[..24])
}

fn parse_node_number(value: &str) -> Option<u32> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    if let Some(value) = value.strip_prefix('!').or_else(|| value.strip_prefix("0x")) {
        return u32::from_str_radix(value, 16).ok();
    }
    value
        .parse::<u32>()
        .ok()
        .or_else(|| u32::from_str_radix(value, 16).ok())
}

fn to_u32(value: i64) -> Option<u32> {
    u32::try_from(value).ok()
}

fn positive_u32(value: i64) -> Option<u32> {
    to_u32(value).filter(|value| *value > 0)
}

fn bounded_text(value: Option<String>, max_length: usize) -> Option<String> {
    let value = value?.trim().to_owned();
    if value.is_empty() || value.chars().count() > max_length || value.contains('\0') {
        return None;
    }
    Some(value)
}

fn bounded_payload_text(value: Option<String>, max_length: usize) -> Option<String> {
    let value = value?;
    if value.chars().count() > max_length || value.contains('\0') {
        return None;
    }
    Some(value)
}

fn value_string(value: Option<&serde_json::Value>) -> Option<String> {
    value.and_then(serde_json::Value::as_str).map(String::from)
}

fn value_i64(value: Option<&serde_json::Value>) -> Option<i64> {
    value.and_then(serde_json::Value::as_i64).or_else(|| {
        value
            .and_then(serde_json::Value::as_str)
            .and_then(|value| value.parse::<i64>().ok())
    })
}

fn iso_from_json(value: Option<&serde_json::Value>) -> Option<String> {
    value_i64(value)
        .and_then(iso_from_millis)
        .or_else(|| value_string(value).and_then(|value| iso_from_rfc3339(&value)))
}

fn iso_from_millis(value: i64) -> Option<String> {
    DateTime::<Utc>::from_timestamp_millis(value)
        .map(|value| value.to_rfc3339_opts(SecondsFormat::Millis, true))
}

fn iso_from_rfc3339(value: &str) -> Option<String> {
    DateTime::parse_from_rfc3339(value).ok().map(|value| {
        value
            .with_timezone(&Utc)
            .to_rfc3339_opts(SecondsFormat::Millis, true)
    })
}

fn table_columns(connection: &Connection, table: &str) -> Result<HashSet<String>, LegacyDataError> {
    let mut statement = connection
        .prepare(&format!("PRAGMA table_info({table})"))
        .map_err(|_| LegacyDataError::SourceDatabaseInvalid)?;
    let rows = statement
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(|_| LegacyDataError::SourceDatabaseInvalid)?;
    rows.map(|row| row.map_err(|_| LegacyDataError::SourceDatabaseInvalid))
        .collect()
}

fn table_has_columns(
    connection: &Connection,
    table: &str,
    expected: &[&str],
) -> Result<bool, LegacyDataError> {
    let columns = table_columns(connection, table)?;
    Ok(expected.iter().all(|column| columns.contains(*column)))
}

fn select_or_null(columns: &HashSet<String>, column: &str) -> String {
    if columns.contains(column) {
        String::from(column)
    } else {
        String::from("NULL")
    }
}

fn select_or_null_prefixed(columns: &HashSet<String>, prefix: &str, column: &str) -> String {
    if columns.contains(column) {
        format!("{prefix}.{column}")
    } else {
        String::from("NULL")
    }
}

fn open_target_database(path: &Path) -> Result<Connection, LegacyDataError> {
    if !path.is_file() {
        return Err(LegacyDataError::TargetMissing);
    }
    Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|_| LegacyDataError::TargetDatabaseInvalid)
}

fn validate_target_database(connection: &Connection) -> Result<(), LegacyDataError> {
    let required = [
        "schema_migrations",
        "settings",
        "mesh_observations",
        "nodes",
        "messages",
        "telemetry",
    ];
    for table in required {
        let found = connection
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
                [table],
                |_| Ok(true),
            )
            .optional()
            .map_err(|_| LegacyDataError::TargetDatabaseInvalid)?;
        if found.is_none() {
            return Err(LegacyDataError::TargetSchemaUnsupported);
        }
    }
    let version = connection
        .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| {
            row.get::<_, Option<i64>>(0)
        })
        .map_err(|_| LegacyDataError::TargetDatabaseInvalid)?;
    if version != Some(SUPPORTED_GATEWAY_SCHEMA_VERSION) {
        return Err(LegacyDataError::TargetSchemaUnsupported);
    }
    integrity_check(connection, LegacyDataError::TargetIntegrityFailed)
}

fn integrity_check(connection: &Connection, error: LegacyDataError) -> Result<(), LegacyDataError> {
    let result = connection
        .query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
        .map_err(|_| error)?;
    if !result.eq_ignore_ascii_case("ok") {
        return Err(error);
    }
    let foreign_key_violation = connection
        .query_row("PRAGMA foreign_key_check", [], |_| Ok(()))
        .optional()
        .map_err(|_| error)?;
    if foreign_key_violation.is_some() {
        return Err(error);
    }
    Ok(())
}

fn has_prior_import(
    connection: &Connection,
    mesh_network_id: &str,
) -> Result<bool, LegacyDataError> {
    let prefix = format!("legacy.data.import.{mesh_network_id}.");
    let mut statement = connection
        .prepare("SELECT key FROM settings WHERE key LIKE 'legacy.data.import.%'")
        .map_err(|_| LegacyDataError::TargetDatabaseInvalid)?;
    let keys = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|_| LegacyDataError::TargetDatabaseInvalid)?;
    for key in keys {
        if key
            .map_err(|_| LegacyDataError::TargetDatabaseInvalid)?
            .starts_with(&prefix)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn snapshot_database(connection: &Connection, backup: &Path) -> Result<(), LegacyDataError> {
    connection
        .backup(rusqlite::DatabaseName::Main, backup, None)
        .map_err(|_| LegacyDataError::BackupFailed)
}

fn create_backup_and_apply(
    plan: &ImportPlan,
    request: &LegacyDataImportRequest,
    backup: &Path,
    manifest_path: &Path,
) -> Result<BackupManifest, LegacyDataError> {
    let mut connection = preflight_target(request)?;
    connection
        .busy_timeout(Duration::ZERO)
        .map_err(|_| LegacyDataError::ImportInProgress)?;
    let data_version_before = data_version(&connection)?;
    snapshot_database(&connection, backup)?;
    let backup_handle = OpenOptions::new()
        .read(true)
        .write(true)
        .open(backup)
        .map_err(|_| LegacyDataError::BackupFailed)?;
    verify_private_file(&backup_handle, LegacyDataError::BackupFailed)?;
    backup_handle
        .sync_all()
        .map_err(|_| LegacyDataError::BackupFailed)?;
    verify_gateway_database_file(backup, LegacyDataError::BackupFailed)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Exclusive)
        .map_err(|_| LegacyDataError::ImportInProgress)?;
    if data_version(&transaction)? != data_version_before {
        return Err(LegacyDataError::ImportInProgress);
    }
    if has_prior_import(&transaction, &plan.mesh_network_id)? {
        return Err(LegacyDataError::TargetAlreadyImported);
    }
    let backup_file = backup
        .file_name()
        .and_then(|name| name.to_str())
        .map(String::from)
        .ok_or(LegacyDataError::BackupFailed)?;
    let manifest = BackupManifest {
        schema_version: BACKUP_MANIFEST_SCHEMA_VERSION,
        migration_id: plan.migration_id.clone(),
        mesh_network_id: plan.mesh_network_id.clone(),
        target_identity_sha256: target_identity_sha256(&request.target_database)?,
        gateway_schema_version: SUPPORTED_GATEWAY_SCHEMA_VERSION,
        backup_file,
        backup_sha256: sha256_file_with_error(backup, LegacyDataError::BackupFailed)?,
    };
    write_backup_manifest(manifest_path, &manifest)?;
    sync_parent_directory(manifest_path)?;
    apply_plan(plan, &manifest, &transaction)?;
    transaction
        .commit()
        .map_err(|_| LegacyDataError::ImportRecoveryRequired)?;
    Ok(manifest)
}

fn data_version(connection: &Connection) -> Result<i64, LegacyDataError> {
    connection
        .query_row("PRAGMA data_version", [], |row| row.get(0))
        .map_err(|_| LegacyDataError::TargetDatabaseInvalid)
}

fn apply_plan(
    plan: &ImportPlan,
    manifest: &BackupManifest,
    transaction: &rusqlite::Transaction<'_>,
) -> Result<(), LegacyDataError> {
    for node in plan.nodes.values() {
        let observation_id = format!("legacy-node-{}-{}", plan.migration_id, node.node_num);
        insert_legacy_observation(transaction, &observation_id, &node.seen_at)?;
        transaction
            .execute(
                "INSERT INTO nodes (mesh_network_id, node_num, user_id, long_name, short_name, hardware_model, role, first_seen_at, last_seen_at, last_observation_id) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8, ?9) \
                 ON CONFLICT(mesh_network_id, node_num) DO UPDATE SET \
                 user_id = CASE WHEN excluded.last_seen_at >= nodes.last_seen_at THEN COALESCE(excluded.user_id, nodes.user_id) ELSE nodes.user_id END, \
                 long_name = CASE WHEN excluded.last_seen_at >= nodes.last_seen_at THEN COALESCE(excluded.long_name, nodes.long_name) ELSE nodes.long_name END, \
                 short_name = CASE WHEN excluded.last_seen_at >= nodes.last_seen_at THEN COALESCE(excluded.short_name, nodes.short_name) ELSE nodes.short_name END, \
                 hardware_model = CASE WHEN excluded.last_seen_at >= nodes.last_seen_at THEN COALESCE(excluded.hardware_model, nodes.hardware_model) ELSE nodes.hardware_model END, \
                 role = CASE WHEN excluded.last_seen_at >= nodes.last_seen_at THEN COALESCE(excluded.role, nodes.role) ELSE nodes.role END, \
                 first_seen_at = CASE WHEN excluded.first_seen_at < nodes.first_seen_at THEN excluded.first_seen_at ELSE nodes.first_seen_at END, \
                 last_seen_at = CASE WHEN excluded.last_seen_at > nodes.last_seen_at THEN excluded.last_seen_at ELSE nodes.last_seen_at END, \
                 last_observation_id = CASE WHEN excluded.last_seen_at >= nodes.last_seen_at THEN excluded.last_observation_id ELSE nodes.last_observation_id END",
                params![
                    plan.mesh_network_id,
                    node.node_num,
                    node.user_id,
                    node.long_name,
                    node.short_name,
                    node.hardware_model,
                    node.role,
                    node.seen_at,
                    observation_id,
                ],
            )
            .map_err(|_| LegacyDataError::ImportFailed)?;
    }
    for message in &plan.messages {
        let observation_id = stable_id(&plan.migration_id, "observation", &message.stable_key);
        let message_id = stable_id(&plan.migration_id, "message", &message.stable_key);
        insert_legacy_observation(transaction, &observation_id, &message.observed_at)?;
        transaction
            .execute(
                "INSERT INTO messages (id, observation_id, mesh_network_id, sender, destination, packet_id, channel, text, observed_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    message_id,
                    observation_id,
                    plan.mesh_network_id,
                    message.sender,
                    message.destination,
                    message.packet_id,
                    message.channel,
                    message.text,
                    message.observed_at,
                ],
            )
            .map_err(|_| LegacyDataError::ImportFailed)?;
    }
    for telemetry in &plan.telemetry {
        let observation_id = stable_id(&plan.migration_id, "observation", &telemetry.stable_key);
        let telemetry_id = stable_id(&plan.migration_id, "telemetry", &telemetry.stable_key);
        let metrics =
            serde_json::to_string(&telemetry.metrics).map_err(|_| LegacyDataError::ImportFailed)?;
        insert_legacy_observation(transaction, &observation_id, &telemetry.observed_at)?;
        transaction
            .execute(
                "INSERT INTO telemetry (id, observation_id, mesh_network_id, node_num, packet_id, metric_kind, metrics, observed_at, telemetry_time_seconds) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    telemetry_id,
                    observation_id,
                    plan.mesh_network_id,
                    telemetry.node_num,
                    telemetry.packet_id,
                    telemetry.metric_kind,
                    metrics,
                    telemetry.observed_at,
                    telemetry.telemetry_time_seconds,
                ],
            )
            .map_err(|_| LegacyDataError::ImportFailed)?;
    }
    let key = import_setting_key(&plan.mesh_network_id, &plan.migration_id);
    let value = serde_json::to_string(manifest).map_err(|_| LegacyDataError::ImportFailed)?;
    transaction
        .execute(
            "INSERT INTO settings (key, value) VALUES (?1, ?2)",
            params![key, value],
        )
        .map_err(|_| LegacyDataError::ImportFailed)?;
    Ok(())
}

fn insert_legacy_observation(
    transaction: &rusqlite::Transaction<'_>,
    id: &str,
    observed_at: &str,
) -> Result<(), LegacyDataError> {
    transaction
        .execute(
            "INSERT INTO mesh_observations (id, schema_version, transport, session_connected_at, ingested_at, server_ingested_at, device_rx_time_seconds, backlog_classification, normalized_from_radio) \
             VALUES (?1, 1, 'simulator', ?2, ?2, ?2, NULL, 'backlog', ?3)",
            params![id, observed_at, LEGACY_OBSERVATION_JSON],
        )
        .map_err(|_| LegacyDataError::ImportFailed)?;
    Ok(())
}

fn import_setting_key(mesh_network_id: &str, migration_id: &str) -> String {
    format!("legacy.data.import.{mesh_network_id}.{migration_id}")
}

fn verify_import(
    plan: &ImportPlan,
    manifest: &BackupManifest,
    target_database: &Path,
) -> Result<(), LegacyDataError> {
    let connection = open_target_database(target_database)?;
    validate_target_database(&connection)?;
    let marker = import_marker_value(&connection, manifest)
        .map_err(|_| LegacyDataError::ImportVerificationFailed)?;
    if marker.as_ref() != Some(manifest) {
        return Err(LegacyDataError::ImportVerificationFailed);
    }
    let message_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM messages WHERE id LIKE ?1",
            [format!("legacy-message-{}-%", plan.migration_id)],
            |row| row.get(0),
        )
        .map_err(|_| LegacyDataError::ImportVerificationFailed)?;
    let telemetry_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM telemetry WHERE id LIKE ?1",
            [format!("legacy-telemetry-{}-%", plan.migration_id)],
            |row| row.get(0),
        )
        .map_err(|_| LegacyDataError::ImportVerificationFailed)?;
    if message_count != plan.messages.len() as i64 || telemetry_count != plan.telemetry.len() as i64
    {
        return Err(LegacyDataError::ImportVerificationFailed);
    }
    for node in plan.nodes.values() {
        let exists = connection
            .query_row(
                "SELECT 1 FROM nodes WHERE mesh_network_id = ?1 AND node_num = ?2",
                params![plan.mesh_network_id, node.node_num],
                |_| Ok(()),
            )
            .is_ok();
        if !exists {
            return Err(LegacyDataError::ImportVerificationFailed);
        }
    }
    integrity_check(&connection, LegacyDataError::ImportVerificationFailed)
}

fn verify_gateway_database_file(
    path: &Path,
    error: LegacyDataError,
) -> Result<(), LegacyDataError> {
    let connection =
        Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).map_err(|_| error)?;
    validate_target_database(&connection).map_err(|_| error)
}

fn backup_manifest_path(backup: &Path) -> Result<PathBuf, LegacyDataError> {
    let file_name = backup
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(LegacyDataError::RollbackFailed)?;
    Ok(backup.with_file_name(format!("{file_name}.manifest.json")))
}

fn target_identity_sha256(target: &Path) -> Result<String, LegacyDataError> {
    let parent = target.parent().ok_or(LegacyDataError::BackupFailed)?;
    let file_name = target.file_name().ok_or(LegacyDataError::BackupFailed)?;
    let canonical = fs::canonicalize(parent)
        .map_err(|_| LegacyDataError::BackupFailed)?
        .join(file_name);
    let mut digest = Sha256::new();
    digest.update(canonical.to_string_lossy().as_bytes());
    Ok(hex_digest(digest.finalize()))
}

#[cfg(unix)]
fn sync_parent_directory(path: &Path) -> Result<(), LegacyDataError> {
    let parent = path.parent().ok_or(LegacyDataError::BackupFailed)?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| LegacyDataError::BackupFailed)
}

#[cfg(windows)]
fn sync_parent_directory(_path: &Path) -> Result<(), LegacyDataError> {
    Ok(())
}

fn write_backup_manifest(path: &Path, manifest: &BackupManifest) -> Result<(), LegacyDataError> {
    let bytes = serde_json::to_vec(manifest).map_err(|_| LegacyDataError::BackupFailed)?;
    let mut file = create_private_file(path, LegacyDataError::BackupFailed)?;
    verify_private_file(&file, LegacyDataError::BackupFailed)?;
    file.write_all(&bytes)
        .map_err(|_| LegacyDataError::BackupFailed)?;
    file.sync_all().map_err(|_| LegacyDataError::BackupFailed)
}

fn read_backup_manifest(path: &Path) -> Result<BackupManifest, LegacyDataError> {
    let metadata = fs::metadata(path).map_err(|_| LegacyDataError::RollbackFailed)?;
    if !metadata.is_file() || metadata.len() > MAX_BACKUP_MANIFEST_BYTES {
        return Err(LegacyDataError::RollbackFailed);
    }
    let bytes = fs::read(path).map_err(|_| LegacyDataError::RollbackFailed)?;
    serde_json::from_slice(&bytes).map_err(|_| LegacyDataError::RollbackFailed)
}

fn import_marker_value(
    connection: &Connection,
    manifest: &BackupManifest,
) -> Result<Option<BackupManifest>, LegacyDataError> {
    let key = import_setting_key(&manifest.mesh_network_id, &manifest.migration_id);
    let value = connection
        .query_row("SELECT value FROM settings WHERE key = ?1", [key], |row| {
            row.get::<_, String>(0)
        })
        .optional()
        .map_err(|_| LegacyDataError::RollbackFailed)?;
    value
        .map(|value| serde_json::from_str(&value).map_err(|_| LegacyDataError::RollbackFailed))
        .transpose()
}

fn verify_rollback_proof(
    request: &LegacyDataRollbackRequest,
    manifest: &BackupManifest,
) -> Result<(), LegacyDataError> {
    verify_private_path_file(&request.backup_database, LegacyDataError::RollbackFailed)?;
    verify_private_path_file(
        &backup_manifest_path(&request.backup_database)?,
        LegacyDataError::RollbackFailed,
    )?;
    let backup_file = request
        .backup_database
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(LegacyDataError::RollbackFailed)?;
    let target_identity = target_identity_sha256(&request.target_database)
        .map_err(|_| LegacyDataError::RollbackFailed)?;
    if manifest.schema_version != BACKUP_MANIFEST_SCHEMA_VERSION
        || manifest.gateway_schema_version != SUPPORTED_GATEWAY_SCHEMA_VERSION
        || manifest.backup_file != backup_file
        || manifest.target_identity_sha256 != target_identity
        || !is_lower_hex(&manifest.migration_id, 24)
        || manifest.mesh_network_id.is_empty()
        || !is_lower_hex(&manifest.backup_sha256, 64)
    {
        return Err(LegacyDataError::RollbackFailed);
    }
    let actual_backup_sha256 =
        sha256_file_with_error(&request.backup_database, LegacyDataError::RollbackFailed)?;
    if actual_backup_sha256 != manifest.backup_sha256 {
        return Err(LegacyDataError::RollbackFailed);
    }
    verify_gateway_database_file(&request.backup_database, LegacyDataError::RollbackFailed)?;
    if let Ok(target) = open_target_database(&request.target_database) {
        match validate_target_database(&target) {
            Ok(()) if import_marker_value(&target, manifest)?.as_ref() != Some(manifest) => {
                return Err(LegacyDataError::RollbackFailed);
            }
            Ok(())
            | Err(LegacyDataError::TargetIntegrityFailed)
            | Err(LegacyDataError::TargetDatabaseInvalid) => {}
            Err(_) => return Err(LegacyDataError::RollbackFailed),
        }
    }
    Ok(())
}

fn verify_manifest_metadata(
    plan: &ImportPlan,
    request: &LegacyDataImportRequest,
    backup: &Path,
    manifest: &BackupManifest,
) -> Result<(), LegacyDataError> {
    let expected_file = backup
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(LegacyDataError::ImportRecoveryRequired)?;
    let target_identity = target_identity_sha256(&request.target_database)
        .map_err(|_| LegacyDataError::ImportRecoveryRequired)?;
    if manifest.schema_version == BACKUP_MANIFEST_SCHEMA_VERSION
        && manifest.gateway_schema_version == SUPPORTED_GATEWAY_SCHEMA_VERSION
        && manifest.migration_id == plan.migration_id
        && manifest.mesh_network_id == plan.mesh_network_id
        && manifest.target_identity_sha256 == target_identity
        && manifest.backup_file == expected_file
        && is_lower_hex(&manifest.backup_sha256, 64)
    {
        Ok(())
    } else {
        Err(LegacyDataError::ImportRecoveryRequired)
    }
}

fn is_lower_hex(value: &str, expected_length: usize) -> bool {
    value.len() == expected_length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn restore_database(target: &Path, backup: &Path) -> Result<(), LegacyDataError> {
    let target_is_usable = open_target_database(target)
        .and_then(|connection| integrity_check(&connection, LegacyDataError::TargetDatabaseInvalid))
        .is_ok();
    if !target_is_usable {
        return replace_unusable_target(target, backup);
    }
    let mut connection = open_restore_target(target)?;
    connection
        .restore(
            rusqlite::DatabaseName::Main,
            backup,
            None::<fn(rusqlite::backup::Progress)>,
        )
        .map_err(|_| LegacyDataError::RollbackFailed)?;
    connection
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .map_err(|_| LegacyDataError::RollbackFailed)?;
    validate_target_database(&connection).map_err(|_| LegacyDataError::RollbackFailed)
}

fn replace_unusable_target(target: &Path, backup: &Path) -> Result<(), LegacyDataError> {
    match fs::symlink_metadata(target) {
        Ok(metadata) if !metadata.file_type().is_file() => {
            return Err(LegacyDataError::RollbackFailed);
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Err(LegacyDataError::RollbackFailed),
    }
    let parent = target.parent().ok_or(LegacyDataError::RollbackFailed)?;
    let name = target
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(LegacyDataError::RollbackFailed)?;
    let candidate = parent.join(format!(".{name}.legacy-restore-candidate"));
    let quarantine = parent.join(format!(".{name}.legacy-restore-corrupt"));
    let candidate_file = create_private_file(&candidate, LegacyDataError::RollbackFailed)?;
    verify_private_file(&candidate_file, LegacyDataError::RollbackFailed)?;
    drop(candidate_file);
    if fs::copy(backup, &candidate).is_err()
        || verify_private_path_file(&candidate, LegacyDataError::RollbackFailed).is_err()
        || verify_gateway_database_file(&candidate, LegacyDataError::RollbackFailed).is_err()
    {
        let _ = fs::remove_file(&candidate);
        return Err(LegacyDataError::RollbackFailed);
    }
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(&candidate)
        .and_then(|file| file.sync_all())
        .map_err(|_| LegacyDataError::RollbackFailed)?;

    let paths = [
        target.to_path_buf(),
        PathBuf::from(format!("{}-wal", target.display())),
        PathBuf::from(format!("{}-shm", target.display())),
    ];
    let quarantines = [
        quarantine.clone(),
        PathBuf::from(format!("{}-wal", quarantine.display())),
        PathBuf::from(format!("{}-shm", quarantine.display())),
    ];
    for path in &quarantines {
        if path
            .try_exists()
            .map_err(|_| LegacyDataError::RollbackFailed)?
        {
            let _ = fs::remove_file(&candidate);
            return Err(LegacyDataError::RollbackFailed);
        }
    }

    let mut moved = Vec::new();
    for (path, quarantined) in paths.iter().zip(&quarantines) {
        if path
            .try_exists()
            .map_err(|_| LegacyDataError::RollbackFailed)?
        {
            if fs::rename(path, quarantined).is_err() {
                restore_quarantined_files(&moved);
                let _ = fs::remove_file(&candidate);
                return Err(LegacyDataError::RollbackFailed);
            }
            moved.push((path.clone(), quarantined.clone()));
        }
    }
    if fs::rename(&candidate, target).is_err() {
        restore_quarantined_files(&moved);
        let _ = fs::remove_file(&candidate);
        return Err(LegacyDataError::RollbackFailed);
    }
    if sync_parent_directory(target).is_err()
        || verify_gateway_database_file(target, LegacyDataError::RollbackFailed).is_err()
    {
        let _ = fs::remove_file(target);
        restore_quarantined_files(&moved);
        return Err(LegacyDataError::RollbackFailed);
    }
    Ok(())
}

fn restore_quarantined_files(moved: &[(PathBuf, PathBuf)]) {
    for (original, quarantined) in moved.iter().rev() {
        let _ = fs::rename(quarantined, original);
    }
}

fn open_restore_target(target: &Path) -> Result<Connection, LegacyDataError> {
    match fs::metadata(target) {
        Ok(metadata) if metadata.is_file() => Connection::open_with_flags(
            target,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|_| LegacyDataError::RollbackFailed),
        Ok(_) => Err(LegacyDataError::RollbackFailed),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let file = create_private_file(target, LegacyDataError::RollbackFailed)?;
            verify_private_file(&file, LegacyDataError::RollbackFailed)?;
            drop(file);
            Connection::open_with_flags(
                target,
                OpenFlags::SQLITE_OPEN_READ_WRITE
                    | OpenFlags::SQLITE_OPEN_CREATE
                    | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .map_err(|_| LegacyDataError::RollbackFailed)
        }
        Err(_) => Err(LegacyDataError::RollbackFailed),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        LegacyDataError, LegacyDataImportRequest, LegacyDataRollbackRequest, apply_legacy_data,
        inspect_legacy_data, rollback_legacy_data,
    };
    use rusqlite::Connection;
    use std::{
        fs,
        path::{Path, PathBuf},
    };

    #[cfg(unix)]
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    fn temporary_directory(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "cmclient-legacy-data-{name}-{}",
            std::process::id()
        ))
    }

    fn initialize_target(path: &Path) {
        let connection = Connection::open(path).expect("target should open");
        connection
            .execute_batch(
                "CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);
                 INSERT INTO schema_migrations (version, name) VALUES (8, 'fixture');
                 CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);
                 INSERT INTO settings (key, value) VALUES ('fixture.existing', 'true');
                 CREATE TABLE mesh_observations (id TEXT PRIMARY KEY, schema_version INTEGER NOT NULL CHECK (schema_version = 1), transport TEXT NOT NULL CHECK (transport IN ('tcp', 'serial', 'simulator')), session_connected_at TEXT NOT NULL, ingested_at TEXT NOT NULL, server_ingested_at TEXT NOT NULL, device_rx_time_seconds INTEGER CHECK (device_rx_time_seconds IS NULL OR (device_rx_time_seconds >= 0 AND device_rx_time_seconds <= 4294967295)), backlog_classification TEXT NOT NULL CHECK (backlog_classification IN ('backlog', 'live', 'unknown')), normalized_from_radio TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);
                 CREATE TABLE nodes (mesh_network_id TEXT NOT NULL, node_num INTEGER NOT NULL CHECK (node_num >= 0 AND node_num <= 4294967295), user_id TEXT, long_name TEXT, short_name TEXT, hardware_model TEXT, role TEXT, first_seen_at TEXT NOT NULL, last_seen_at TEXT NOT NULL, last_observation_id TEXT NOT NULL, PRIMARY KEY (mesh_network_id, node_num));
                 CREATE TABLE messages (id TEXT PRIMARY KEY, observation_id TEXT NOT NULL UNIQUE REFERENCES mesh_observations(id), mesh_network_id TEXT NOT NULL, sender INTEGER NOT NULL CHECK (sender >= 0 AND sender <= 4294967295), destination INTEGER CHECK (destination IS NULL OR (destination >= 0 AND destination <= 4294967295)), packet_id INTEGER CHECK (packet_id IS NULL OR (packet_id >= 0 AND packet_id <= 4294967295)), channel INTEGER CHECK (channel IS NULL OR (channel >= 0 AND channel <= 255)), text TEXT NOT NULL, observed_at TEXT NOT NULL);
                 CREATE TABLE telemetry (id TEXT PRIMARY KEY, observation_id TEXT NOT NULL UNIQUE REFERENCES mesh_observations(id), mesh_network_id TEXT NOT NULL, node_num INTEGER NOT NULL CHECK (node_num >= 0 AND node_num <= 4294967295), packet_id INTEGER CHECK (packet_id IS NULL OR (packet_id >= 0 AND packet_id <= 4294967295)), metric_kind TEXT NOT NULL, metrics TEXT NOT NULL, observed_at TEXT NOT NULL, telemetry_time_seconds INTEGER CHECK (telemetry_time_seconds IS NULL OR (telemetry_time_seconds > 0 AND telemetry_time_seconds <= 4294967295)));",
            )
            .expect("target schema should initialize");
    }

    fn initialize_sources(directory: &Path) {
        let callmesh = Connection::open(directory.join("callmesh-data.sqlite"))
            .expect("legacy callmesh source should open");
        callmesh
            .execute_batch(
                "CREATE TABLE nodes (mesh_id TEXT PRIMARY KEY, mesh_id_original TEXT, short_name TEXT, long_name TEXT, hw_model TEXT, role TEXT, last_seen_at INTEGER);
                 INSERT INTO nodes VALUES ('!00000042', '!00000042', 'FN', 'Fixture Node', 'T-Echo', 'CLIENT', 1721260800000);
                 CREATE TABLE message_log (flow_id TEXT PRIMARY KEY, channel INTEGER, timestamp_ms INTEGER, type TEXT, detail TEXT, mesh_packet_id INTEGER, reply_id INTEGER);
                 CREATE TABLE message_nodes (flow_id TEXT, role TEXT, mesh_id TEXT);
                 INSERT INTO message_log VALUES ('message-fixture', 2, 1721260801000, 'Text', 'history-value-not-in-report', 15, NULL);
                 INSERT INTO message_nodes VALUES ('message-fixture', 'from', '!00000042');",
            )
            .expect("legacy callmesh schema should initialize");
        let telemetry = Connection::open(directory.join("telemetry-records.sqlite"))
            .expect("legacy telemetry source should open");
        telemetry
            .execute_batch(
                "CREATE TABLE telemetry_records (id TEXT PRIMARY KEY, mesh_id TEXT, node_mesh_id TEXT, timestamp_ms INTEGER, telemetry_kind TEXT, telemetry_time_seconds INTEGER);
                 CREATE TABLE telemetry_metrics (record_id TEXT, metric_key TEXT, number_value REAL, text_value TEXT, json_value TEXT);
                 INSERT INTO telemetry_records VALUES ('telemetry-fixture', '!00000042', '!00000042', 1721260802000, 'deviceMetrics', 1721260802);
                 INSERT INTO telemetry_metrics VALUES ('telemetry-fixture', 'batteryLevel', 73, NULL, NULL);",
            )
            .expect("legacy telemetry schema should initialize");
    }

    fn initialize_inline_telemetry_source(directory: &Path) {
        let telemetry = Connection::open(directory.join("telemetry-records.sqlite"))
            .expect("legacy inline telemetry source should open");
        telemetry
            .execute_batch(
                "CREATE TABLE telemetry_records (id TEXT PRIMARY KEY, mesh_id TEXT, timestamp_ms INTEGER, data TEXT);
                 INSERT INTO telemetry_records VALUES ('telemetry-inline', '!00000042', 1721260802000, '{\"id\":\"telemetry-inline\",\"meshId\":\"!00000042\",\"timestampMs\":1721260802000,\"telemetry\":{\"kind\":\"deviceMetrics\",\"timeSeconds\":1721260802,\"metrics\":{\"batteryLevel\":73}}}');",
            )
            .expect("legacy inline telemetry schema should initialize");
    }

    fn initialize_inline_message_source(directory: &Path) {
        let callmesh = Connection::open(directory.join("callmesh-data.sqlite"))
            .expect("legacy inline message source should open");
        callmesh
            .execute_batch(
                "CREATE TABLE message_log (flow_id TEXT PRIMARY KEY, channel INTEGER, timestamp_ms INTEGER, position INTEGER, data TEXT);
                 INSERT INTO message_log VALUES ('inline-message', 2, 1721260801000, 0, '{\"flowId\":\"inline-message\",\"channel\":2,\"timestampMs\":1721260801000,\"type\":\"Text\",\"detail\":\"legacy-inline-message\",\"from\":{\"meshId\":\"!00000042\"},\"replyId\":999,\"meshPacketId\":15}');",
            )
            .expect("legacy inline message schema should initialize");
    }

    fn initialize_normalized_message_source(directory: &Path) {
        let callmesh = Connection::open(directory.join("callmesh-data.sqlite"))
            .expect("legacy normalized message source should open");
        callmesh
            .execute_batch(
                "CREATE TABLE message_log (flow_id TEXT PRIMARY KEY, channel INTEGER, timestamp_ms INTEGER, type TEXT, detail TEXT, mesh_packet_id INTEGER);
                 CREATE TABLE message_nodes (flow_id TEXT, role TEXT, mesh_id TEXT, mesh_id_normalized TEXT, mesh_id_original TEXT);
                 INSERT INTO message_log VALUES ('normalized-message', 2, 1721260801000, 'Text', '  preserved whitespace  ', 15);
                 INSERT INTO message_nodes VALUES ('normalized-message', 'from', NULL, '!00000042', '!00000099');",
            )
            .expect("legacy normalized message schema should initialize");
    }

    fn initialize_jsonl_fallback_sources(directory: &Path) {
        fs::write(directory.join("message-log.jsonl"), "{broken\n")
            .expect("invalid current message log should write");
        fs::write(
            directory.join("message-log.jsonl.migrated"),
            "{\"flowId\":\"fallback-message\",\"channel\":2,\"timestampMs\":1721260801000,\"type\":\"Text\",\"detail\":\"fallback\",\"from\":{\"meshId\":\"!00000042\"}}\n",
        )
        .expect("migrated message log should write");
        fs::write(directory.join("telemetry-records.jsonl"), "not-json\n")
            .expect("invalid current telemetry log should write");
        fs::write(
            directory.join("telemetry-records.jsonl.migrated"),
            "{\"id\":\"fallback-telemetry\",\"meshId\":\"!00000042\",\"timestampMs\":1721260802000,\"telemetry\":{\"kind\":\"deviceMetrics\",\"timeSeconds\":0,\"metrics\":{\"label\":\"  spaced  \"}}}\n",
        )
        .expect("migrated telemetry log should write");
    }

    fn request(directory: &Path) -> LegacyDataImportRequest {
        LegacyDataImportRequest {
            source_dir: directory.join("legacy"),
            target_database: directory.join("gateway.sqlite"),
            mesh_network_id: String::from("fixture-network"),
            backup_dir: directory.join("backups"),
        }
    }

    #[test]
    fn dry_run_is_value_safe_and_does_not_mutate_the_target() {
        let directory = temporary_directory("dry-run");
        let _ = fs::remove_dir_all(&directory);
        fs::create_dir_all(directory.join("legacy")).expect("source directory should exist");
        initialize_sources(&directory.join("legacy"));
        initialize_target(&directory.join("gateway.sqlite"));

        let report = inspect_legacy_data(&request(&directory)).expect("dry run should succeed");
        assert!(report.dry_run);
        assert_eq!(report.records.nodes, 1);
        assert_eq!(report.records.messages, 1);
        assert_eq!(report.records.telemetry, 1);
        let serialized = serde_json::to_string(&report).expect("report should serialize");
        assert!(!serialized.contains("history-value-not-in-report"));
        let target =
            Connection::open(directory.join("gateway.sqlite")).expect("target should open");
        let message_count: i64 = target
            .query_row("SELECT COUNT(*) FROM messages", [], |row| row.get(0))
            .expect("count should succeed");
        assert_eq!(message_count, 0);
        target
            .execute(
                "INSERT INTO settings (key, value) VALUES ('legacy.data.import.fixture-network.previous', '{}')",
                [],
            )
            .expect("import marker should insert");
        assert_eq!(
            inspect_legacy_data(&request(&directory)),
            Err(LegacyDataError::TargetAlreadyImported)
        );
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn apply_verifies_the_projection_and_rollback_restores_the_snapshot() {
        let directory = temporary_directory("apply-rollback");
        let _ = fs::remove_dir_all(&directory);
        fs::create_dir_all(directory.join("legacy")).expect("source directory should exist");
        initialize_sources(&directory.join("legacy"));
        initialize_target(&directory.join("gateway.sqlite"));
        let request = request(&directory);

        let report = apply_legacy_data(&request).expect("apply should succeed");
        assert!(!report.dry_run);
        let backup = directory
            .join("backups")
            .join(report.backup_file.expect("backup should be reported"));
        assert!(backup.is_file());
        let target = Connection::open(&request.target_database).expect("target should open");
        let counts = target
            .query_row(
                "SELECT (SELECT COUNT(*) FROM nodes), (SELECT COUNT(*) FROM messages), (SELECT COUNT(*) FROM telemetry)",
                [],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?, row.get::<_, i64>(2)?)),
            )
            .expect("counts should query");
        assert_eq!(counts, (1, 1, 1));
        let observation_count: i64 = target
            .query_row("SELECT COUNT(*) FROM mesh_observations", [], |row| {
                row.get(0)
            })
            .expect("historical provenance observations should exist");
        assert_eq!(observation_count, 3);
        let observation = target
            .query_row(
                "SELECT normalized_from_radio FROM mesh_observations",
                [],
                |row| row.get::<_, String>(0),
            )
            .expect("historical observation should exist");
        assert_eq!(observation, "{\"schemaVersion\":1,\"kind\":\"other\"}");
        drop(target);
        let repeated = apply_legacy_data(&request).expect("committed journal should be idempotent");
        assert_eq!(repeated.migration_id, report.migration_id);
        assert_eq!(repeated.records, report.records);

        let rollback = rollback_legacy_data(&LegacyDataRollbackRequest {
            target_database: request.target_database.clone(),
            backup_database: backup,
        })
        .expect("rollback should succeed");
        assert!(rollback.restored);
        let restored =
            Connection::open(&request.target_database).expect("restored target should open");
        let message_count: i64 = restored
            .query_row("SELECT COUNT(*) FROM messages", [], |row| row.get(0))
            .expect("count should query");
        assert_eq!(message_count, 0);
        let existing: String = restored
            .query_row(
                "SELECT value FROM settings WHERE key = 'fixture.existing'",
                [],
                |row| row.get(0),
            )
            .expect("existing setting should be restored");
        assert_eq!(existing, "true");
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn reads_the_legacy_inline_telemetry_schema_without_preserving_raw_data() {
        let directory = temporary_directory("inline-telemetry");
        let _ = fs::remove_dir_all(&directory);
        fs::create_dir_all(directory.join("legacy")).expect("source directory should exist");
        initialize_inline_telemetry_source(&directory.join("legacy"));
        initialize_target(&directory.join("gateway.sqlite"));

        let report = inspect_legacy_data(&request(&directory)).expect("dry run should succeed");
        assert_eq!(report.records.nodes, 0);
        assert_eq!(report.records.messages, 0);
        assert_eq!(report.records.telemetry, 1);
        assert!(
            report
                .sources
                .iter()
                .any(|source| source.name == "telemetry-records.sqlite" && source.used)
        );
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn reads_the_legacy_inline_message_schema_without_treating_reply_as_destination() {
        let directory = temporary_directory("inline-message");
        let _ = fs::remove_dir_all(&directory);
        fs::create_dir_all(directory.join("legacy")).expect("source directory should exist");
        initialize_inline_message_source(&directory.join("legacy"));
        initialize_target(&directory.join("gateway.sqlite"));
        let request = request(&directory);

        let report = apply_legacy_data(&request).expect("import should succeed");
        assert_eq!(report.records.messages, 1);
        let target = Connection::open(&request.target_database).expect("target should open");
        let message = target
            .query_row(
                "SELECT destination, packet_id, text FROM messages",
                [],
                |row| {
                    Ok((
                        row.get::<_, Option<i64>>(0)?,
                        row.get::<_, Option<i64>>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .expect("message should exist");
        assert_eq!(message.0, None);
        assert_eq!(message.1, Some(15));
        assert_eq!(message.2, "legacy-inline-message");
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn preserves_payload_whitespace_and_reads_normalized_only_senders() {
        let directory = temporary_directory("normalized-sender");
        let _ = fs::remove_dir_all(&directory);
        fs::create_dir_all(directory.join("legacy")).expect("source directory should exist");
        initialize_normalized_message_source(&directory.join("legacy"));
        initialize_target(&directory.join("gateway.sqlite"));
        let request = request(&directory);

        apply_legacy_data(&request).expect("normalized sender should import");
        let target = Connection::open(&request.target_database).expect("target should open");
        let message = target
            .query_row("SELECT sender, text FROM messages", [], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .expect("message should exist");
        assert_eq!(message, (0x42, String::from("  preserved whitespace  ")));
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn skips_bad_jsonl_lines_and_falls_back_to_migrated_files() {
        let directory = temporary_directory("jsonl-fallback");
        let _ = fs::remove_dir_all(&directory);
        fs::create_dir_all(directory.join("legacy")).expect("source directory should exist");
        initialize_jsonl_fallback_sources(&directory.join("legacy"));
        initialize_target(&directory.join("gateway.sqlite"));
        let request = request(&directory);

        let report = apply_legacy_data(&request).expect("migrated fallback should import");
        assert_eq!(report.records.messages, 1);
        assert_eq!(report.records.telemetry, 1);
        assert!(report.skipped.iter().any(|skipped| {
            skipped.code == "LEGACY_DATA_MESSAGE_JSON_INVALID" && skipped.count == 1
        }));
        assert!(report.skipped.iter().any(|skipped| {
            skipped.code == "LEGACY_DATA_TELEMETRY_JSON_INVALID" && skipped.count == 1
        }));
        assert!(
            report
                .sources
                .iter()
                .any(|source| { source.name == "message-log.jsonl.migrated" && source.used })
        );
        let target = Connection::open(&request.target_database).expect("target should open");
        let telemetry = target
            .query_row(
                "SELECT metrics, telemetry_time_seconds FROM telemetry",
                [],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<i64>>(1)?)),
            )
            .expect("telemetry should exist");
        assert_eq!(telemetry.0, "{\"label\":\"  spaced  \"}");
        assert_eq!(telemetry.1, None);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn rollback_rejects_tampered_and_cross_target_backups_before_mutation() {
        let directory = temporary_directory("rollback-proof");
        let _ = fs::remove_dir_all(&directory);
        fs::create_dir_all(directory.join("legacy")).expect("source directory should exist");
        initialize_sources(&directory.join("legacy"));
        initialize_target(&directory.join("gateway.sqlite"));
        initialize_target(&directory.join("other.sqlite"));
        let request = request(&directory);
        let report = apply_legacy_data(&request).expect("import should succeed");
        let backup = request
            .backup_dir
            .join(report.backup_file.expect("backup should be reported"));

        let other = LegacyDataRollbackRequest {
            target_database: directory.join("other.sqlite"),
            backup_database: backup.clone(),
        };
        assert_eq!(
            rollback_legacy_data(&other),
            Err(LegacyDataError::RollbackFailed)
        );
        let other_settings: i64 = Connection::open(&other.target_database)
            .expect("other target should open")
            .query_row("SELECT COUNT(*) FROM settings", [], |row| row.get(0))
            .expect("settings should count");
        assert_eq!(other_settings, 1);

        let mut bytes = fs::read(&backup).expect("backup should read");
        let last = bytes.last_mut().expect("backup should not be empty");
        *last ^= 0xff;
        fs::write(&backup, bytes).expect("backup should tamper");
        assert_eq!(
            rollback_legacy_data(&LegacyDataRollbackRequest {
                target_database: request.target_database.clone(),
                backup_database: backup,
            }),
            Err(LegacyDataError::RollbackFailed)
        );
        let messages: i64 = Connection::open(&request.target_database)
            .expect("target should open")
            .query_row("SELECT COUNT(*) FROM messages", [], |row| row.get(0))
            .expect("messages should count");
        assert_eq!(messages, 1);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn post_commit_verification_failure_restores_the_snapshot() {
        let directory = temporary_directory("verification-rollback");
        let _ = fs::remove_dir_all(&directory);
        fs::create_dir_all(directory.join("legacy")).expect("source directory should exist");
        initialize_sources(&directory.join("legacy"));
        initialize_target(&directory.join("gateway.sqlite"));
        let request = request(&directory);
        let target = Connection::open(&request.target_database).expect("target should open");
        target
            .execute_batch(
                "CREATE TRIGGER discard_legacy_import_marker
                 BEFORE INSERT ON settings
                 WHEN NEW.key LIKE 'legacy.data.import.%'
                 BEGIN SELECT RAISE(IGNORE); END;",
            )
            .expect("verification failure trigger should create");
        drop(target);

        assert_eq!(
            apply_legacy_data(&request),
            Err(LegacyDataError::ImportRolledBack)
        );
        let restored = Connection::open(&request.target_database).expect("target should restore");
        let counts = restored
            .query_row(
                "SELECT (SELECT COUNT(*) FROM nodes), (SELECT COUNT(*) FROM messages), (SELECT COUNT(*) FROM telemetry)",
                [],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?, row.get::<_, i64>(2)?)),
            )
            .expect("counts should query");
        assert_eq!(counts, (0, 0, 0));
        let marker_count: i64 = restored
            .query_row(
                "SELECT COUNT(*) FROM settings WHERE key LIKE 'legacy.data.import.%'",
                [],
                |row| row.get(0),
            )
            .expect("markers should count");
        assert_eq!(marker_count, 0);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn rejects_source_target_overlap_and_changed_source_manifests() {
        let directory = temporary_directory("source-preflight");
        let _ = fs::remove_dir_all(&directory);
        fs::create_dir_all(directory.join("legacy")).expect("source directory should exist");
        initialize_sources(&directory.join("legacy"));
        initialize_target(&directory.join("gateway.sqlite"));
        let request = request(&directory);

        let mut overlap = request.clone();
        overlap.source_dir = directory.join("overlap");
        fs::create_dir_all(&overlap.source_dir).expect("overlap directory should exist");
        fs::hard_link(
            &request.target_database,
            overlap.source_dir.join("callmesh-data.sqlite"),
        )
        .expect("overlap source should link target");
        assert_eq!(
            inspect_legacy_data(&overlap),
            Err(LegacyDataError::SourceTargetOverlap)
        );

        let plan = super::build_plan(&request).expect("plan should build");
        fs::write(directory.join("legacy/message-log.jsonl"), "{}\n")
            .expect("new source should write");
        assert_eq!(
            super::ensure_sources_unchanged(&plan, &request.source_dir),
            Err(LegacyDataError::SourceChanged)
        );
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn recovers_precommit_artifacts_and_restores_a_missing_target_from_wal_snapshot() {
        let directory = temporary_directory("journal-wal");
        let _ = fs::remove_dir_all(&directory);
        fs::create_dir_all(directory.join("legacy")).expect("source directory should exist");
        initialize_sources(&directory.join("legacy"));
        initialize_target(&directory.join("gateway.sqlite"));
        let request = request(&directory);

        let target = Connection::open(&request.target_database).expect("target should open");
        target
            .execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA wal_autocheckpoint=0;
                 INSERT INTO settings (key, value) VALUES ('fixture.wal', 'present');",
            )
            .expect("WAL fixture should initialize");
        let plan = super::build_plan(&request).expect("plan should build");
        fs::create_dir_all(&request.backup_dir).expect("backup directory should exist");
        #[cfg(unix)]
        fs::set_permissions(&request.backup_dir, fs::Permissions::from_mode(0o700))
            .expect("backup permissions should set");
        let stale = request
            .backup_dir
            .join(format!("legacy-data-{}.sqlite", plan.migration_id));
        fs::write(&stale, b"partial").expect("stale backup should write");
        #[cfg(unix)]
        fs::set_permissions(&stale, fs::Permissions::from_mode(0o600))
            .expect("stale permissions should set");

        let report = apply_legacy_data(&request).expect("stale journal should recover");
        let backup = request
            .backup_dir
            .join(report.backup_file.expect("backup should be reported"));
        let standalone = Connection::open(&backup).expect("standalone snapshot should open");
        let wal_value: String = standalone
            .query_row(
                "SELECT value FROM settings WHERE key = 'fixture.wal'",
                [],
                |row| row.get(0),
            )
            .expect("WAL row should be in snapshot");
        assert_eq!(wal_value, "present");
        drop(standalone);
        drop(target);
        fs::remove_file(&request.target_database).expect("target should be removed");
        let _ = fs::remove_file(request.target_database.with_extension("sqlite-wal"));
        let _ = fs::remove_file(request.target_database.with_extension("sqlite-shm"));

        rollback_legacy_data(&LegacyDataRollbackRequest {
            target_database: request.target_database.clone(),
            backup_database: backup,
        })
        .expect("missing target should restore from bound backup");
        let restored = Connection::open(&request.target_database).expect("target should restore");
        let wal_value: String = restored
            .query_row(
                "SELECT value FROM settings WHERE key = 'fixture.wal'",
                [],
                |row| row.get(0),
            )
            .expect("WAL row should restore");
        assert_eq!(wal_value, "present");
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn restores_a_corrupt_bound_target_but_rejects_an_unrelated_sqlite_database() {
        let directory = temporary_directory("disaster-rollback");
        let _ = fs::remove_dir_all(&directory);
        fs::create_dir_all(directory.join("legacy")).expect("source directory should exist");
        initialize_sources(&directory.join("legacy"));
        initialize_target(&directory.join("gateway.sqlite"));
        let request = request(&directory);
        let report = apply_legacy_data(&request).expect("import should succeed");
        let backup = request
            .backup_dir
            .join(report.backup_file.expect("backup should be reported"));

        let original = fs::read(&request.target_database).expect("target should read");
        let unrelated = Connection::open(&request.target_database).expect("target should open");
        unrelated
            .execute_batch("DROP TABLE settings; CREATE TABLE unrelated (value TEXT);")
            .expect("unrelated schema should replace settings");
        drop(unrelated);
        assert_eq!(
            rollback_legacy_data(&LegacyDataRollbackRequest {
                target_database: request.target_database.clone(),
                backup_database: backup.clone(),
            }),
            Err(LegacyDataError::RollbackFailed)
        );

        fs::write(&request.target_database, &original[..512])
            .expect("target should become corrupt");
        rollback_legacy_data(&LegacyDataRollbackRequest {
            target_database: request.target_database.clone(),
            backup_database: backup,
        })
        .expect("corrupt target should restore from proof");
        let restored = Connection::open(&request.target_database).expect("target should restore");
        let messages: i64 = restored
            .query_row("SELECT COUNT(*) FROM messages", [], |row| row.get(0))
            .expect("messages should count");
        assert_eq!(messages, 0);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn rollback_never_replaces_a_target_directory() {
        let directory = temporary_directory("directory-target");
        let _ = fs::remove_dir_all(&directory);
        fs::create_dir_all(directory.join("legacy")).expect("source directory should exist");
        initialize_sources(&directory.join("legacy"));
        initialize_target(&directory.join("gateway.sqlite"));
        let request = request(&directory);
        let report = apply_legacy_data(&request).expect("import should succeed");
        let backup = request
            .backup_dir
            .join(report.backup_file.expect("backup should be reported"));
        fs::remove_file(&request.target_database).expect("target should remove");
        fs::create_dir(&request.target_database).expect("target directory should create");
        let sentinel = request.target_database.join("sentinel");
        fs::write(&sentinel, "preserve").expect("sentinel should write");

        assert_eq!(
            rollback_legacy_data(&LegacyDataRollbackRequest {
                target_database: request.target_database.clone(),
                backup_database: backup,
            }),
            Err(LegacyDataError::RollbackFailed)
        );
        assert_eq!(
            fs::read_to_string(sentinel).expect("sentinel should remain"),
            "preserve"
        );
        let _ = fs::remove_dir_all(directory);
    }

    #[cfg(unix)]
    #[test]
    fn backup_permissions_are_private_without_mutating_existing_directories() {
        let directory = temporary_directory("permissions");
        let _ = fs::remove_dir_all(&directory);
        fs::create_dir_all(directory.join("legacy")).expect("source directory should exist");
        initialize_sources(&directory.join("legacy"));
        initialize_target(&directory.join("gateway.sqlite"));
        let request = request(&directory);

        let report = apply_legacy_data(&request).expect("private backup should succeed");
        assert_eq!(
            fs::metadata(&request.backup_dir)
                .expect("backup directory should exist")
                .mode()
                & 0o777,
            0o700
        );
        for name in [
            report.backup_file.expect("backup should be reported"),
            report
                .backup_manifest_file
                .expect("manifest should be reported"),
        ] {
            assert_eq!(
                fs::metadata(request.backup_dir.join(name))
                    .expect("artifact should exist")
                    .mode()
                    & 0o777,
                0o600
            );
        }

        let shared = directory.join("shared");
        fs::create_dir(&shared).expect("shared directory should exist");
        fs::set_permissions(&shared, fs::Permissions::from_mode(0o755))
            .expect("shared permissions should set");
        let mut unsafe_request = request.clone();
        unsafe_request.backup_dir = shared.clone();
        unsafe_request.mesh_network_id = String::from("second-network");
        assert_eq!(
            apply_legacy_data(&unsafe_request),
            Err(LegacyDataError::BackupFailed)
        );
        assert_eq!(
            fs::metadata(&shared)
                .expect("shared directory should remain")
                .mode()
                & 0o777,
            0o755
        );
        let _ = fs::remove_dir_all(directory);
    }
}
