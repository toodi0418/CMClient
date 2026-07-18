use std::{fs, path::PathBuf, process::Command};

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
