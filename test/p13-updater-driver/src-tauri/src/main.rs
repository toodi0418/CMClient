#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::{
    env, fs,
    path::{Path, PathBuf},
    process,
    sync::mpsc::{self, Receiver, SyncSender},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tauri::{AppHandle, Manager, RunEvent};
use tauri_plugin_updater::UpdaterExt;
use url::Url;

const MODE_ENV: &str = "CMCLIENT_P13_UPDATER_MODE";
const ENDPOINT_ENV: &str = "CMCLIENT_P13_UPDATER_ENDPOINT";
const PUBKEY_ENV: &str = "CMCLIENT_P13_UPDATER_PUBKEY";
const TEST_CA_ENV: &str = "CMCLIENT_P13_UPDATER_CA_FILE";
const CAMPAIGN_ROOT_ENV: &str = "CMCLIENT_CAMPAIGN_ROOT";
const TIMEOUT_MS_ENV: &str = "CMCLIENT_P13_UPDATER_TIMEOUT_MS";
const RELAUNCH_MARKER: &str = "updater-relaunch.pending";
const RELAUNCH_MARKER_MAX_AGE: Duration = Duration::from_secs(5 * 60);
const WORKER_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

const EXIT_OK: i32 = 0;
const EXIT_NO_UPDATE: i32 = 10;
const EXIT_CONFIGURATION: i32 = 20;
const EXIT_CHECK: i32 = 21;
const EXIT_DOWNLOAD: i32 = 22;
const EXIT_INSTALL_RETURNED: i32 = 23;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Probe,
    Check,
    Download,
    Install,
}

impl Mode {
    fn from_environment() -> Result<Self, ()> {
        match env::var(MODE_ENV).as_deref().unwrap_or("probe") {
            "probe" => Ok(Self::Probe),
            "check" => Ok(Self::Check),
            "download" => Ok(Self::Download),
            "install" => Ok(Self::Install),
            _ => Err(()),
        }
    }
}

fn timeout() -> Result<Duration, ()> {
    let value = env::var(TIMEOUT_MS_ENV).unwrap_or_else(|_| String::from("5000"));
    parse_timeout(&value)
}

fn parse_timeout(value: &str) -> Result<Duration, ()> {
    let milliseconds = value.parse::<u64>().map_err(|_| ())?;
    if !(100..=30_000).contains(&milliseconds) {
        return Err(());
    }
    Ok(Duration::from_millis(milliseconds))
}

fn endpoint_override() -> Result<Option<Url>, ()> {
    let Ok(value) = env::var(ENDPOINT_ENV) else {
        return Ok(None);
    };
    parse_endpoint(&value).map(Some)
}

fn parse_endpoint(value: &str) -> Result<Url, ()> {
    let endpoint = Url::parse(value).map_err(|_| ())?;
    let loopback = matches!(endpoint.host_str(), Some("127.0.0.1" | "localhost" | "::1"));
    if endpoint.scheme() != "https"
        || !loopback
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
    {
        return Err(());
    }
    Ok(endpoint)
}

fn campaign_file(variable: &str) -> Result<Option<Vec<u8>>, ()> {
    let Ok(path) = env::var(variable) else {
        return Ok(None);
    };
    let root = env::var(CAMPAIGN_ROOT_ENV).map_err(|_| ())?;
    let root = PathBuf::from(root).canonicalize().map_err(|_| ())?;
    let path = PathBuf::from(path).canonicalize().map_err(|_| ())?;
    if !path.starts_with(&root) || !path.is_file() {
        return Err(());
    }
    fs::read(path).map(Some).map_err(|_| ())
}

fn campaign_marker() -> Result<PathBuf, ()> {
    let root = env::var(CAMPAIGN_ROOT_ENV).map_err(|_| ())?;
    let root = PathBuf::from(root).canonicalize().map_err(|_| ())?;
    Ok(root.join(RELAUNCH_MARKER))
}

async fn execute(app: AppHandle, mode: Mode) -> i32 {
    if mode == Mode::Probe {
        println!("P13_TAURI_HEADLESS_APPHANDLE_OK");
        return EXIT_OK;
    }
    let relaunch_marker = if mode == Mode::Install {
        match campaign_marker() {
            Ok(marker) if marker.is_file() => {
                let now_ms = match unix_time_ms() {
                    Ok(value) => value,
                    Err(()) => return EXIT_CONFIGURATION,
                };
                match consume_relaunch_marker(
                    &marker,
                    &app.package_info().version.to_string(),
                    process::id(),
                    now_ms,
                ) {
                    Ok(true) => {
                        println!("P13_TAURI_RELAUNCH_OK");
                        return EXIT_OK;
                    }
                    Ok(false) => Some(marker),
                    Err(()) => return EXIT_CONFIGURATION,
                }
            }
            Ok(marker) => Some(marker),
            Err(()) => return EXIT_CONFIGURATION,
        }
    } else {
        None
    };

    let mut builder = app.updater_builder().timeout(match timeout() {
        Ok(value) => value,
        Err(()) => return EXIT_CONFIGURATION,
    });

    match endpoint_override() {
        Ok(Some(endpoint)) => {
            builder = match builder.endpoints(vec![endpoint]) {
                Ok(value) => value,
                Err(_) => return EXIT_CONFIGURATION,
            };
        }
        Ok(None) => {}
        Err(()) => return EXIT_CONFIGURATION,
    }

    match campaign_file(TEST_CA_ENV) {
        Ok(Some(pem)) => {
            let certificate = match reqwest::Certificate::from_pem(&pem) {
                Ok(value) => value,
                Err(_) => return EXIT_CONFIGURATION,
            };
            builder = builder
                .configure_client(move |client| client.add_root_certificate(certificate.clone()));
        }
        Ok(None) => {}
        Err(()) => return EXIT_CONFIGURATION,
    }

    let updater = match builder.build() {
        Ok(value) => value,
        Err(_) => return EXIT_CONFIGURATION,
    };
    let update = match updater.check().await {
        Ok(Some(value)) => value,
        Ok(None) => {
            println!("P13_TAURI_NO_UPDATE");
            return EXIT_NO_UPDATE;
        }
        Err(_) => return EXIT_CHECK,
    };

    if mode == Mode::Check {
        println!("P13_TAURI_UPDATE_AVAILABLE version={}", update.version);
        return EXIT_OK;
    }

    if mode == Mode::Download {
        return match update.download(|_, _| {}, || {}).await {
            Ok(bytes) => {
                println!("P13_TAURI_DOWNLOAD_VERIFIED bytes={}", bytes.len());
                EXIT_OK
            }
            Err(_) => EXIT_DOWNLOAD,
        };
    }

    if let Some(marker) = &relaunch_marker {
        let created_at_ms = match unix_time_ms() {
            Ok(value) => value,
            Err(()) => return EXIT_CONFIGURATION,
        };
        let marker_value = serde_json::json!({
            "schemaVersion": 1,
            "targetVersion": update.version,
            "sourcePid": process::id(),
            "createdAtMs": created_at_ms,
        });
        if fs::write(marker, marker_value.to_string()).is_err() {
            return EXIT_CONFIGURATION;
        }
    }
    if update.download_and_install(|_, _| {}, || {}).await.is_err() {
        if let Some(marker) = relaunch_marker {
            let _ = fs::remove_file(marker);
        }
        return EXIT_DOWNLOAD;
    }

    #[cfg(not(target_os = "windows"))]
    app.restart();

    #[cfg(target_os = "windows")]
    EXIT_INSTALL_RETURNED
}

fn unix_time_ms() -> Result<u64, ()> {
    let milliseconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ())?
        .as_millis();
    u64::try_from(milliseconds).map_err(|_| ())
}

fn consume_relaunch_marker(
    path: &Path,
    current_version: &str,
    current_pid: u32,
    now_ms: u64,
) -> Result<bool, ()> {
    let contents = fs::read(path).map_err(|_| ())?;
    fs::remove_file(path).map_err(|_| ())?;
    let value: serde_json::Value = serde_json::from_slice(&contents).map_err(|_| ())?;
    let object = value.as_object().ok_or(())?;
    let valid_shape = object.len() == 4
        && object
            .get("schemaVersion")
            .and_then(serde_json::Value::as_u64)
            == Some(1)
        && object
            .get("targetVersion")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| value == current_version)
        && object
            .get("sourcePid")
            .and_then(serde_json::Value::as_u64)
            .is_some_and(|value| value != u64::from(current_pid))
        && object
            .get("createdAtMs")
            .and_then(serde_json::Value::as_u64)
            .is_some_and(|created_at_ms| {
                now_ms >= created_at_ms
                    && now_ms - created_at_ms
                        <= u64::try_from(RELAUNCH_MARKER_MAX_AGE.as_millis()).unwrap_or(u64::MAX)
            });
    Ok(valid_shape)
}

fn validate_headless(no_window_configs: bool, no_webview_windows: bool) -> Result<(), ()> {
    if no_window_configs && no_webview_windows {
        Ok(())
    } else {
        Err(())
    }
}

fn wait_for_worker_signal(receiver: &Receiver<()>, timeout: Duration) -> Result<(), ()> {
    receiver.recv_timeout(timeout).map_err(|_| ())
}

fn release_worker_on_ready(
    event: &RunEvent,
    sender: &mut Option<SyncSender<()>>,
) -> Result<(), ()> {
    if !matches!(event, RunEvent::Ready) {
        return Ok(());
    }
    sender.take().ok_or(())?.send(()).map_err(|_| ())
}

fn spawn_worker(app: AppHandle, mode: Mode) -> Result<SyncSender<()>, ()> {
    let (executor_ready_sender, executor_ready_receiver) = mpsc::sync_channel(0);
    let (lifecycle_sender, lifecycle_receiver) = mpsc::sync_channel(1);
    thread::Builder::new()
        .name(String::from("cmclient-p13-updater"))
        .spawn(move || {
            let code = tauri::async_runtime::block_on(async move {
                if executor_ready_sender.send(()).is_err()
                    || wait_for_worker_signal(&lifecycle_receiver, WORKER_HANDSHAKE_TIMEOUT)
                        .is_err()
                {
                    return EXIT_CONFIGURATION;
                }
                execute(app, mode).await
            });
            process::exit(code);
        })
        .map_err(|_| ())?;
    wait_for_worker_signal(&executor_ready_receiver, WORKER_HANDSHAKE_TIMEOUT)?;
    Ok(lifecycle_sender)
}

fn main() {
    let mode = match Mode::from_environment() {
        Ok(value) => value,
        Err(()) => {
            std::process::exit(EXIT_CONFIGURATION);
        }
    };
    let public_key = env::var(PUBKEY_ENV).ok();
    let mut plugin = tauri_plugin_updater::Builder::new();
    if let Some(public_key) = public_key {
        plugin = plugin.pubkey(public_key);
    }

    let app = tauri::Builder::default()
        .plugin(plugin.build())
        .build(tauri::generate_context!())
        .expect("P13_TAURI_RUNTIME_FAILED");
    let mut lifecycle_sender = match spawn_worker(app.handle().clone(), mode) {
        Ok(sender) => Some(sender),
        Err(()) => process::exit(EXIT_CONFIGURATION),
    };
    app.run(move |app_handle, event| {
        let ready = matches!(event, RunEvent::Ready);
        if (ready
            && validate_headless(
                app_handle.config().app.windows.is_empty(),
                app_handle.webview_windows().is_empty(),
            )
            .is_err())
            || release_worker_on_ready(&event, &mut lifecycle_sender).is_err()
        {
            process::exit(EXIT_CONFIGURATION);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::{
        Mode, consume_relaunch_marker, parse_endpoint, parse_timeout, release_worker_on_ready,
        validate_headless, wait_for_worker_signal,
    };
    use std::{fs, path::PathBuf, process, sync::mpsc, thread, time::Duration};
    use tauri::RunEvent;

    #[test]
    fn mode_values_are_closed() {
        assert_ne!(Mode::Probe, Mode::Install);
    }

    #[test]
    fn timeout_and_endpoint_inputs_are_bounded() {
        assert_eq!(parse_timeout("5000").unwrap().as_millis(), 5000);
        assert!(parse_timeout("99").is_err());
        assert!(parse_timeout("30001").is_err());
        assert!(parse_endpoint("https://127.0.0.1:9443/manifest/valid").is_ok());
        assert!(parse_endpoint("http://127.0.0.1:9443/manifest/valid").is_err());
        assert!(parse_endpoint("https://example.com/manifest/valid").is_err());
    }

    #[test]
    fn relaunch_marker_requires_fresh_version_and_different_source_process() {
        let path = fixture_marker("valid");
        fs::write(
            &path,
            r#"{"schemaVersion":1,"targetVersion":"0.2.0","sourcePid":7,"createdAtMs":1000}"#,
        )
        .unwrap();
        assert!(consume_relaunch_marker(&path, "0.2.0", 8, 1100).unwrap());
        assert!(!path.exists());

        for (name, marker) in [
            (
                "version",
                r#"{"schemaVersion":1,"targetVersion":"0.1.0","sourcePid":7,"createdAtMs":1000}"#,
            ),
            (
                "pid",
                r#"{"schemaVersion":1,"targetVersion":"0.2.0","sourcePid":8,"createdAtMs":1000}"#,
            ),
            (
                "stale",
                r#"{"schemaVersion":1,"targetVersion":"0.2.0","sourcePid":7,"createdAtMs":1000}"#,
            ),
        ] {
            let path = fixture_marker(name);
            fs::write(&path, marker).unwrap();
            let now_ms = if name == "stale" { 1_000_000 } else { 1100 };
            assert!(!consume_relaunch_marker(&path, "0.2.0", 8, now_ms).unwrap());
            assert!(!path.exists());
        }
    }

    #[test]
    fn helper_refuses_any_configured_or_runtime_window() {
        assert!(validate_headless(true, true).is_ok());
        assert!(validate_headless(false, true).is_err());
        assert!(validate_headless(true, false).is_err());
    }

    #[test]
    fn worker_stays_blocked_until_the_lifecycle_is_ready() {
        let (sender, receiver) = mpsc::sync_channel(1);
        let mut sender = Some(sender);
        assert!(release_worker_on_ready(&RunEvent::Resumed, &mut sender).is_ok());
        assert!(receiver.try_recv().is_err());
        assert!(release_worker_on_ready(&RunEvent::Ready, &mut sender).is_ok());
        assert!(wait_for_worker_signal(&receiver, Duration::from_secs(1)).is_ok());
        assert!(release_worker_on_ready(&RunEvent::Ready, &mut sender).is_err());

        let (sender, receiver) = mpsc::sync_channel(0);
        let worker = thread::spawn(move || sender.send(()).unwrap());
        assert!(wait_for_worker_signal(&receiver, Duration::from_secs(1)).is_ok());
        worker.join().unwrap();

        let (sender, receiver) = mpsc::sync_channel(0);
        drop(sender);
        assert!(wait_for_worker_signal(&receiver, Duration::from_millis(1)).is_err());
    }

    fn fixture_marker(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "cmclient-p13-relaunch-{name}-{}.json",
            process::id()
        ))
    }
}
