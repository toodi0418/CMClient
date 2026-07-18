use rusqlite::Connection;
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

fn temporary_directory(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "cmclient-migrate-cli-{name}-{}",
        std::process::id()
    ))
}

fn fixture_source(directory: &PathBuf) -> PathBuf {
    let source = directory.join("client-preferences.json");
    fs::create_dir_all(directory).expect("temporary directory should exist");
    fs::write(
        &source,
        r#"{"webDashboardEnabled":false,"apiKey":"must-not-appear"}"#,
    )
    .expect("fixture should be written");
    source
}

#[test]
fn dry_run_reports_a_safe_candidate_without_echoing_a_legacy_secret() {
    let directory = temporary_directory("dry-run");
    let _ = fs::remove_dir_all(&directory);
    let source = fixture_source(&directory);

    let output = Command::new(env!("CARGO_BIN_EXE_cmclient-migrate"))
        .args(["settings", "--source"])
        .arg(source)
        .arg("--dry-run")
        .output()
        .expect("migration command should run");

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("stdout should be UTF-8");
    assert!(stdout.contains("\"dryRun\":true"));
    assert!(stdout.contains("LEGACY_SETTINGS_SECRET_SKIPPED"));
    assert!(!stdout.contains("must-not-appear"));
    let _ = fs::remove_dir_all(directory);
}

#[test]
fn apply_creates_only_a_new_agent_configuration() {
    let directory = temporary_directory("apply");
    let _ = fs::remove_dir_all(&directory);
    let source = fixture_source(&directory);
    let target = directory.join("new-config/agent.toml");

    let first = Command::new(env!("CARGO_BIN_EXE_cmclient-migrate"))
        .args(["settings", "--source"])
        .arg(&source)
        .args(["--write-agent-config"])
        .arg(&target)
        .output()
        .expect("migration command should run");
    assert!(first.status.success());
    assert_eq!(
        fs::read_to_string(&target).expect("new config should exist"),
        "[agent]\nmanagement_web_enabled = false\n"
    );

    let second = Command::new(env!("CARGO_BIN_EXE_cmclient-migrate"))
        .args(["settings", "--source"])
        .arg(source)
        .args(["--write-agent-config"])
        .arg(target)
        .output()
        .expect("migration command should run");
    assert!(!second.status.success());
    assert_eq!(
        String::from_utf8(second.stderr)
            .expect("stderr should be UTF-8")
            .trim(),
        "LEGACY_SETTINGS_TARGET_EXISTS"
    );
    let _ = fs::remove_dir_all(directory);
}

fn initialize_gateway_target(path: &Path) {
    let connection = Connection::open(path).expect("target database should open");
    connection
        .execute_batch(
            "CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);
             INSERT INTO schema_migrations (version, name) VALUES (8, 'fixture');
             CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);
             CREATE TABLE mesh_observations (id TEXT PRIMARY KEY, schema_version INTEGER NOT NULL, transport TEXT NOT NULL, session_connected_at TEXT NOT NULL, ingested_at TEXT NOT NULL, server_ingested_at TEXT NOT NULL, device_rx_time_seconds INTEGER, backlog_classification TEXT NOT NULL, normalized_from_radio TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);
             CREATE TABLE nodes (mesh_network_id TEXT NOT NULL, node_num INTEGER NOT NULL, user_id TEXT, long_name TEXT, short_name TEXT, hardware_model TEXT, role TEXT, first_seen_at TEXT NOT NULL, last_seen_at TEXT NOT NULL, last_observation_id TEXT NOT NULL, PRIMARY KEY (mesh_network_id, node_num));
             CREATE TABLE messages (id TEXT PRIMARY KEY, observation_id TEXT NOT NULL UNIQUE REFERENCES mesh_observations(id), mesh_network_id TEXT NOT NULL, sender INTEGER NOT NULL, destination INTEGER, packet_id INTEGER, channel INTEGER, text TEXT NOT NULL, observed_at TEXT NOT NULL);
             CREATE TABLE telemetry (id TEXT PRIMARY KEY, observation_id TEXT NOT NULL UNIQUE REFERENCES mesh_observations(id), mesh_network_id TEXT NOT NULL, node_num INTEGER NOT NULL, packet_id INTEGER, metric_kind TEXT NOT NULL, metrics TEXT NOT NULL, observed_at TEXT NOT NULL, telemetry_time_seconds INTEGER);",
        )
        .expect("target schema should initialize");
}

fn initialize_legacy_sources(directory: &Path) {
    let callmesh = Connection::open(directory.join("callmesh-data.sqlite"))
        .expect("source database should open");
    callmesh
        .execute_batch(
            "CREATE TABLE nodes (mesh_id TEXT PRIMARY KEY, mesh_id_original TEXT, short_name TEXT, long_name TEXT, hw_model TEXT, role TEXT, last_seen_at INTEGER);
             INSERT INTO nodes VALUES ('!00000042', '!00000042', 'FN', 'Fixture Node', 'T-Echo', 'CLIENT', 1721260800000);
             CREATE TABLE message_log (flow_id TEXT PRIMARY KEY, channel INTEGER, timestamp_ms INTEGER, type TEXT, detail TEXT, mesh_packet_id INTEGER, reply_id INTEGER);
             CREATE TABLE message_nodes (flow_id TEXT, role TEXT, mesh_id TEXT);
             INSERT INTO message_log VALUES ('message-fixture', 2, 1721260801000, 'Text', 'fixture history text', 15, 999);
             INSERT INTO message_nodes VALUES ('message-fixture', 'from', '!00000042');",
        )
        .expect("source schema should initialize");
    let telemetry = Connection::open(directory.join("telemetry-records.sqlite"))
        .expect("telemetry source should open");
    telemetry
        .execute_batch(
            "CREATE TABLE telemetry_records (id TEXT PRIMARY KEY, mesh_id TEXT, node_mesh_id TEXT, timestamp_ms INTEGER, telemetry_kind TEXT, telemetry_time_seconds INTEGER);
             CREATE TABLE telemetry_metrics (record_id TEXT, metric_key TEXT, number_value REAL, text_value TEXT, json_value TEXT);
             INSERT INTO telemetry_records VALUES ('telemetry-fixture', '!00000042', '!00000042', 1721260802000, 'deviceMetrics', 1721260802);
             INSERT INTO telemetry_metrics VALUES ('telemetry-fixture', 'batteryLevel', 73, NULL, NULL);",
        )
        .expect("telemetry source schema should initialize");
}

#[test]
fn data_import_requires_explicit_gateway_stop_confirmation_and_rolls_back_from_reported_backup() {
    let directory = temporary_directory("data-import");
    let _ = fs::remove_dir_all(&directory);
    let source_dir = directory.join("legacy");
    fs::create_dir_all(&source_dir).expect("source directory should exist");
    initialize_legacy_sources(&source_dir);
    let target = directory.join("gateway.sqlite");
    initialize_gateway_target(&target);
    let backup_dir = directory.join("backups");
    let binary = env!("CARGO_BIN_EXE_cmclient-migrate");

    let rejected = Command::new(binary)
        .args(["data", "import", "--source-dir"])
        .arg(&source_dir)
        .args(["--target-database"])
        .arg(&target)
        .args(["--mesh-network-id", "fixture-network", "--backup-dir"])
        .arg(&backup_dir)
        .arg("--apply")
        .output()
        .expect("migration command should run");
    assert!(!rejected.status.success());
    assert_eq!(
        String::from_utf8(rejected.stderr)
            .expect("stderr should be UTF-8")
            .trim(),
        "LEGACY_DATA_GATEWAY_STOP_CONFIRMATION_REQUIRED"
    );

    let applied = Command::new(binary)
        .args(["data", "import", "--source-dir"])
        .arg(&source_dir)
        .args(["--target-database"])
        .arg(&target)
        .args(["--mesh-network-id", "fixture-network", "--backup-dir"])
        .arg(&backup_dir)
        .args(["--apply", "--confirm-gateway-stopped"])
        .output()
        .expect("migration command should run");
    assert!(applied.status.success());
    let report: serde_json::Value =
        serde_json::from_slice(&applied.stdout).expect("data report should be JSON");
    assert_eq!(report["dryRun"], false);
    assert_eq!(report["records"]["messages"], 1);
    let backup = backup_dir.join(
        report["backupFile"]
            .as_str()
            .expect("backup filename should be reported"),
    );
    let migrated = Connection::open(&target).expect("migrated target should open");
    let destination: Option<i64> = migrated
        .query_row("SELECT destination FROM messages", [], |row| row.get(0))
        .expect("message should exist");
    assert_eq!(destination, None);
    drop(migrated);

    let rolled_back = Command::new(binary)
        .args(["data", "rollback", "--target-database"])
        .arg(&target)
        .args(["--backup-database"])
        .arg(backup)
        .arg("--confirm-gateway-stopped")
        .output()
        .expect("rollback command should run");
    assert!(rolled_back.status.success());
    let restored = Connection::open(&target).expect("restored target should open");
    let messages: i64 = restored
        .query_row("SELECT COUNT(*) FROM messages", [], |row| row.get(0))
        .expect("count should query");
    assert_eq!(messages, 0);
    let _ = fs::remove_dir_all(directory);
}
