use cmclient_legacy_migration::{
    ChildGatewayMaintenanceRunner, GatewayMaintenanceRunner, MigrationError,
    ProductMigrationRequest, pending_migration_source, run_or_resume_product_migration,
    source_contains_known_state,
};
use cmclient_runtime_primitives::ExclusiveFileLock;
use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

fn temporary_directory(name: &str) -> PathBuf {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be available")
        .as_nanos();
    let test_root = option_env!("CARGO_TARGET_TMPDIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::current_exe()
                .expect("test executable should resolve")
                .parent()
                .expect("test executable should have a parent")
                .parent()
                .expect("test target should have a parent")
                .join("cmclient-test-tmp")
        });
    test_root.join(format!(
        "cmclient-migrate-cli-{name}-{}-{suffix}",
        std::process::id()
    ))
}

struct Fixture {
    root: PathBuf,
    source: PathBuf,
    target: PathBuf,
}

impl Fixture {
    fn new(name: &str) -> Self {
        let root = temporary_directory(name);
        let source = root.join("legacy");
        let target = root.join("home/.cmclient");
        fs::create_dir_all(source.join("backups")).expect("source should be created");
        fs::create_dir_all(target.parent().expect("target should have a parent"))
            .expect("target parent should be created");
        fs::write(
            source.join("agent.toml"),
            b"[agent]\nmanagement_web_enabled = true\n",
        )
        .expect("config should be written");
        fs::write(
            source.join("secrets.json"),
            br#"{"version":1,"callmesh-api-key":"fixture-private"}"#,
        )
        .expect("secrets should be written");
        fs::write(source.join("gateway.sqlite"), b"fixture-database")
            .expect("database should be written");
        fs::write(source.join("backups/one.sqlite"), b"fixture-backup")
            .expect("backup should be written");
        fs::write(source.join("ignored.log"), b"ignored").expect("unknown file should be written");
        fs::write(source.join("agent.lock"), b"").expect("legacy lock should be written");
        Self {
            root,
            source,
            target,
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn source_snapshot(root: &Path) -> BTreeMap<String, Option<Vec<u8>>> {
    let mut snapshot = BTreeMap::new();
    let mut directories = vec![(root.to_path_buf(), String::new())];
    while let Some((directory, prefix)) = directories.pop() {
        for entry in fs::read_dir(directory).expect("source tree should be readable") {
            let entry = entry.expect("source entry should be readable");
            let name = entry
                .file_name()
                .into_string()
                .expect("fixture names should be UTF-8");
            let relative = if prefix.is_empty() {
                name
            } else {
                format!("{prefix}/{name}")
            };
            if entry
                .file_type()
                .expect("source metadata should be readable")
                .is_dir()
            {
                snapshot.insert(relative.clone(), None);
                directories.push((entry.path(), relative));
            } else {
                snapshot.insert(
                    relative,
                    Some(fs::read(entry.path()).expect("source file should be readable")),
                );
            }
        }
    }
    snapshot
}

fn binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_cmclient-migrate"))
}

fn product_command(fixture: &Fixture) -> Command {
    let mut command = Command::new(binary());
    command
        .args(["product", "--source-root"])
        .arg(&fixture.source)
        .arg("--target-root")
        .arg(&fixture.target)
        .arg("--gateway-program")
        .arg(binary())
        .arg("--test-maintenance-helper");
    command
}

fn wait_for_marker(child: &mut Child, marker: &Path) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        if marker.exists() {
            return;
        }
        if let Some(status) = child.try_wait().expect("child status should be readable") {
            panic!("migration child exited before pause marker: {status}");
        }
        thread::sleep(Duration::from_millis(20));
    }
    let _ = child.kill();
    let _ = child.wait();
    panic!("migration child did not reach durable phase marker");
}

#[test]
fn reports_the_packaged_migration_version() {
    let output = Command::new(binary())
        .arg("--version")
        .output()
        .expect("migration command should run");

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout)
            .expect("stdout should be UTF-8")
            .trim(),
        format!("cmclient-migrate {}", env!("CARGO_PKG_VERSION"))
    );
}

#[test]
fn product_requires_explicit_absolute_paths() {
    let output = Command::new(binary())
        .args([
            "product",
            "--source-root",
            "relative-source",
            "--target-root",
            "relative-target",
            "--gateway-program",
        ])
        .arg(binary())
        .arg("--test-maintenance-helper")
        .output()
        .expect("migration command should run");
    assert!(!output.status.success());
    assert_eq!(
        String::from_utf8(output.stderr)
            .expect("stderr should be UTF-8")
            .trim(),
        "LEGACY_MIGRATION_PATH_INVALID"
    );
}

#[test]
fn real_process_kill_resumes_from_every_durable_phase() {
    for phase in ["detected", "staged", "verified", "activated", "complete"] {
        let fixture = Fixture::new(&format!("kill-{phase}"));
        let before = source_snapshot(&fixture.source);
        let marker = fixture.root.join(format!("{phase}.marker"));
        let mut child = product_command(&fixture)
            .arg("--test-pause-after")
            .arg(phase)
            .arg("--test-pause-file")
            .arg(&marker)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("migration child should spawn");
        wait_for_marker(&mut child, &marker);
        child.kill().expect("migration child should be killed");
        child.wait().expect("migration child should be reaped");
        if phase == "detected" {
            assert_eq!(
                pending_migration_source(&fixture.target)
                    .expect("journal should be readable")
                    .as_deref(),
                Some(
                    fs::canonicalize(&fixture.source)
                        .expect("source should canonicalize")
                        .as_path()
                )
            );
        }
        if phase == "complete" {
            let journal: serde_json::Value = serde_json::from_slice(
                &fs::read(fixture.target.join("state/migration.json"))
                    .expect("complete journal should exist"),
            )
            .expect("complete journal should be valid JSON");
            let stage = fixture
                .target
                .join("cache/migration-stage")
                .join(journal["transactionId"].as_str().expect("transaction id"));
            assert!(
                stage.join("secrets.json").exists(),
                "hard kill after Complete should occur before stage cleanup"
            );
        }

        let output = product_command(&fixture)
            .output()
            .expect("migration resume should run");
        assert!(
            output.status.success(),
            "phase {phase} failed to resume: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let report: serde_json::Value =
            serde_json::from_slice(&output.stdout).expect("outcome should be JSON");
        assert_eq!(report["phase"], "complete");
        assert_eq!(report["migrated"], true);
        assert_eq!(
            fs::read(fixture.target.join("cmclient.db")).expect("database should activate"),
            b"fixture-database"
        );
        assert!(!fixture.target.join("ignored.log").exists());
        assert!(
            fs::read_dir(fixture.target.join("cache/migration-stage"))
                .expect("stage root should exist")
                .next()
                .is_none(),
            "phase {phase} resume retained migration stage"
        );
        assert_eq!(before, source_snapshot(&fixture.source));
    }
}

#[test]
fn existing_legacy_agent_lock_is_honored_without_modification() {
    let fixture = Fixture::new("source-lock");
    let before = source_snapshot(&fixture.source);
    let lock_path = fixture.source.join("agent.lock");
    let held = ExclusiveFileLock::try_acquire(&lock_path).expect("fixture lock should acquire");
    let output = product_command(&fixture)
        .output()
        .expect("migration command should run");
    assert!(!output.status.success());
    assert_eq!(
        String::from_utf8(output.stderr)
            .expect("stderr should be UTF-8")
            .trim(),
        "LEGACY_MIGRATION_SOURCE_IN_USE"
    );
    drop(held);
    assert_eq!(before, source_snapshot(&fixture.source));
}

#[test]
fn nested_legacy_agent_lock_is_reused_without_modification() {
    let fixture = Fixture::new("nested-source-lock");
    fs::remove_file(fixture.source.join("agent.lock")).expect("root lock should be removed");
    fs::create_dir_all(fixture.source.join("run")).expect("run directory should be created");
    fs::write(fixture.source.join("run/agent.lock"), b"").expect("nested lock should be written");
    let before = source_snapshot(&fixture.source);
    let output = product_command(&fixture)
        .output()
        .expect("migration command should run");
    assert!(
        output.status.success(),
        "nested source lock migration failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(before, source_snapshot(&fixture.source));
}

#[test]
fn discovery_is_bounded_to_known_state() {
    let root = temporary_directory("discovery");
    let missing = root.join("missing");
    assert!(!source_contains_known_state(&missing).expect("missing root should be absent"));
    fs::create_dir_all(&root).expect("root should be created");
    fs::write(root.join("unknown.log"), b"ignored").expect("unknown file should be written");
    assert!(!source_contains_known_state(&root).expect("unknown file should be ignored"));
    fs::write(root.join("config.toml"), b"[agent]\n").expect("known file should be written");
    assert!(source_contains_known_state(&root).expect("known state should be detected"));
    fs::remove_dir_all(root).expect("fixture should be removed");
}

fn maintenance_runner(mode: &str, timeout: Duration) -> ChildGatewayMaintenanceRunner {
    maintenance_runner_with_arguments(mode, timeout, Vec::new())
}

fn maintenance_runner_with_arguments(
    mode: &str,
    timeout: Duration,
    additional_arguments: Vec<OsString>,
) -> ChildGatewayMaintenanceRunner {
    let mut arguments = vec![
        OsString::from("__test-maintenance"),
        OsString::from("--test-mode"),
        OsString::from(mode),
    ];
    arguments.extend(additional_arguments);
    ChildGatewayMaintenanceRunner::with_prefix_and_timeout(binary(), arguments, timeout)
        .expect("test maintenance runner should be valid")
}

#[test]
fn child_maintenance_contract_is_strict_retryable_and_bounded() {
    let root = temporary_directory("maintenance-contract");
    fs::create_dir_all(&root).expect("fixture root should be created");
    let source = root.join("source.sqlite");
    fs::write(&source, b"fixture-database").expect("source should be written");

    let success_stage = root.join("success.sqlite");
    let report = maintenance_runner("success", Duration::from_secs(5))
        .migrate_database(&source, &success_stage)
        .expect("valid helper report should pass");
    assert_eq!(report.message_type, "gateway.offline-maintenance-report");
    assert_eq!(report.operation, "backup_migrate_verify");
    assert_eq!(
        fs::read(success_stage).expect("stage should exist"),
        b"fixture-database"
    );

    for (mode, expected) in [
        ("retryable", MigrationError::MaintenanceRetryable),
        ("failure", MigrationError::MaintenanceFailed),
        ("malformed", MigrationError::MaintenanceReportInvalid),
        ("oversize", MigrationError::MaintenanceReportInvalid),
    ] {
        assert_eq!(
            maintenance_runner(mode, Duration::from_secs(5))
                .migrate_database(&source, &root.join(format!("{mode}.sqlite"))),
            Err(expected)
        );
    }
    assert_eq!(
        maintenance_runner("timeout", Duration::from_millis(150))
            .migrate_database(&source, &root.join("timeout.sqlite")),
        Err(MigrationError::MaintenanceTimedOut)
    );
    fs::remove_dir_all(root).expect("fixture should be removed");
}

#[test]
fn maintenance_parent_exit_cannot_leave_a_stdout_holding_descendant() {
    let root = temporary_directory("maintenance-descendant");
    fs::create_dir_all(&root).expect("fixture root should be created");
    let source = root.join("source.sqlite");
    let staged = root.join("staged.sqlite");
    let marker = root.join("descendant-survived");
    fs::write(&source, b"fixture-database").expect("source should be written");

    let started_at = Instant::now();
    assert_eq!(
        maintenance_runner_with_arguments(
            "descendant-exit",
            Duration::from_secs(5),
            vec![
                OsString::from("--test-marker"),
                marker.clone().into_os_string(),
            ],
        )
        .migrate_database(&source, &staged),
        Err(MigrationError::MaintenanceFailed)
    );
    assert!(
        started_at.elapsed() < Duration::from_millis(1500),
        "a descendant retaining stdout must not delay maintenance cleanup"
    );
    thread::sleep(Duration::from_millis(2200));
    assert!(
        !marker.exists(),
        "maintenance descendant escaped its process tree after the parent exited"
    );
    fs::remove_dir_all(root).expect("fixture should be removed");
}

#[test]
fn child_failure_prioritizes_a_concurrent_source_identity_change() {
    let fixture = Fixture::new("child-source-change");
    let source = fixture.source.join("gateway.sqlite");
    let stage_root = fixture.target.join("cache/migration-stage");
    let mutator = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if fs::read_dir(&stage_root)
                .ok()
                .and_then(|mut entries| entries.next())
                .is_some()
            {
                let contents = fs::read(&source).expect("source should remain readable");
                let replacement = source.with_extension("replacement");
                fs::write(&replacement, contents).expect("replacement should be written");
                fs::remove_file(&source).expect("source should be replaced");
                fs::rename(replacement, source).expect("replacement should activate");
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
        panic!("migration did not enter staging before child timeout");
    });
    let runner = maintenance_runner("timeout", Duration::from_millis(500));
    let result = run_or_resume_product_migration(
        &ProductMigrationRequest {
            source_root: fixture.source.clone(),
            target_root: fixture.target.clone(),
        },
        &runner,
    );
    mutator.join().expect("source mutator should finish");
    assert_eq!(result, Err(MigrationError::SourceChanged));
    let journal: serde_json::Value = serde_json::from_slice(
        &fs::read(fixture.target.join("state/migration.json"))
            .expect("recovery journal should exist"),
    )
    .expect("recovery journal should be valid JSON");
    assert_eq!(journal["recoveryCode"], "LEGACY_MIGRATION_SOURCE_CHANGED");
}

#[cfg(windows)]
#[test]
fn reparse_known_leaf_is_rejected_when_windows_allows_test_symlinks() {
    use std::os::windows::fs::symlink_file;

    let root = temporary_directory("windows-reparse");
    fs::create_dir_all(&root).expect("fixture root should be created");
    fs::write(root.join("actual.toml"), b"[agent]\n").expect("target should be written");
    if symlink_file(root.join("actual.toml"), root.join("config.toml")).is_err() {
        fs::remove_dir_all(root).expect("fixture should be removed");
        return;
    }
    assert_eq!(
        source_contains_known_state(&root),
        Err(MigrationError::SourceUnsafe)
    );
    fs::remove_dir_all(root).expect("fixture should be removed");
}
