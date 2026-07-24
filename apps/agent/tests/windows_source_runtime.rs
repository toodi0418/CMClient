#![cfg(windows)]

use cmclient_control_api::{
    ControlClient, ControlEndpoint, GatewayControlStatus, ManagementWebControlStatus,
    default_local_endpoint,
};
use reqwest::blocking::{Client, Response};
use reqwest::header::{COOKIE, HOST, ORIGIN, SET_COOKIE};
use serde_json::Value;
use std::fs::{self, File, OpenOptions};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const MANAGEMENT_HOST: &str = "127.0.0.1:7080";
const MANAGEMENT_ORIGIN: &str = "http://127.0.0.1:7080";

struct Fixture {
    root: PathBuf,
    home: PathBuf,
    data: PathBuf,
    private_node: PathBuf,
    gateway_entrypoint: PathBuf,
    web_root: PathBuf,
    stdout: PathBuf,
    stderr: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let campaign_root = std::env::var_os("CMCLIENT_CAMPAIGN_ROOT")
            .map(PathBuf::from)
            .filter(|path| path.is_absolute())
            .expect("Windows source qualification requires absolute CMCLIENT_CAMPAIGN_ROOT");
        fs::create_dir_all(&campaign_root).expect("campaign root should exist");

        let root = campaign_root.join(format!(
            "windows-source-runtime-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock should be valid")
                .as_nanos()
        ));
        let home = root.join("home");
        let data = home.join(".cmclient");
        for directory in [
            root.clone(),
            home.clone(),
            data.clone(),
            root.join("temp"),
            root.join("appdata"),
            root.join("localappdata"),
            root.join("programdata"),
        ] {
            fs::create_dir_all(directory).expect("fixture directory should exist");
        }
        fs::write(
            data.join("config.toml"),
            "[agent]\nmanagement_web_enabled = true\n",
        )
        .expect("Agent config should write");

        let private_node = std::env::var_os("CMCLIENT_PRIVATE_NODE")
            .map(PathBuf::from)
            .expect("Windows source qualification requires staged CMCLIENT_PRIVATE_NODE");
        assert!(private_node.is_absolute(), "staged Node must be absolute");
        let private_node = fs::canonicalize(private_node).expect("staged Node should resolve");
        let metadata = fs::symlink_metadata(&private_node).expect("staged Node metadata");
        assert!(metadata.is_file(), "staged Node must be a regular file");
        use std::os::windows::fs::MetadataExt;
        assert_eq!(
            metadata.file_attributes() & 0x0400,
            0,
            "staged Node must not be a reparse point"
        );
        let version = Command::new(&private_node)
            .arg("--version")
            .env_clear()
            .output()
            .expect("staged Node should run without PATH");
        assert!(version.status.success(), "staged Node version should run");
        assert_eq!(
            String::from_utf8(version.stdout)
                .expect("staged Node version should be UTF-8")
                .trim(),
            "v24.18.0"
        );

        let manifest_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let gateway_entrypoint = fs::canonicalize(manifest_root.join("../gateway/dist/main.js"))
            .expect("production Gateway entrypoint should exist after source build");
        let web_root = fs::canonicalize(manifest_root.join("../web/dist"))
            .expect("production Web root should exist after source build");
        assert!(
            web_root.join("index.html").is_file(),
            "production Web root should contain index.html"
        );

        Self {
            stdout: root.join("agent.stdout.log"),
            stderr: root.join("agent.stderr.log"),
            root,
            home,
            data,
            private_node,
            gateway_entrypoint,
            web_root,
        }
    }

    fn endpoint(&self) -> ControlEndpoint {
        default_local_endpoint(&self.data).expect("control endpoint should derive")
    }

    fn spawn(&self) -> RunningAgent {
        let system_root = std::env::var_os("SystemRoot").expect("SystemRoot should be present");
        let windir = std::env::var_os("WINDIR").unwrap_or_else(|| system_root.clone());
        let comspec = std::env::var_os("ComSpec").expect("ComSpec should be present");
        let temp = self.root.join("temp");
        let appdata = self.root.join("appdata");
        let localappdata = self.root.join("localappdata");
        let programdata = self.root.join("programdata");

        let mut command = Command::new(env!("CARGO_BIN_EXE_cmclient-agent"));
        command
            .arg("--serve")
            .env_clear()
            .env("USERPROFILE", &self.home)
            .env("HOME", &self.home)
            .env("TEMP", &temp)
            .env("TMP", &temp)
            .env("TMPDIR", &temp)
            .env("APPDATA", &appdata)
            .env("LOCALAPPDATA", &localappdata)
            .env("PROGRAMDATA", &programdata)
            .env("SystemRoot", system_root)
            .env("WINDIR", windir)
            .env("ComSpec", comspec)
            .env("CMCLIENT_CAMPAIGN_ROOT", &self.root)
            .env("CMCLIENT_PRIVATE_NODE", &self.private_node)
            .env("CMCLIENT_GATEWAY_ENTRYPOINT", &self.gateway_entrypoint)
            .env("CMCLIENT_WEB_ROOT", &self.web_root)
            .stdout(Stdio::from(private_log(&self.stdout)))
            .stderr(Stdio::from(private_log(&self.stderr)));
        let child = command.spawn().expect("real Agent process should spawn");
        RunningAgent { child }
    }

    fn wait_for_control(&self, agent: &mut RunningAgent) -> ControlClient {
        let client = ControlClient::new_with_timeout(self.endpoint(), Duration::from_secs(30))
            .expect("control client should initialize");
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if client.status().is_ok() {
                return client;
            }
            if let Ok(Some(status)) = agent.child.try_wait() {
                panic!(
                    "Agent exited before Control became ready ({status}); stderr: {}",
                    read_log(&self.stderr)
                );
            }
            assert!(
                Instant::now() < deadline,
                "real Agent Control endpoint did not become ready; stderr: {}",
                read_log(&self.stderr)
            );
            thread::sleep(Duration::from_millis(100));
        }
    }

    fn wait_for_status<F>(
        &self,
        client: &ControlClient,
        mut predicate: F,
    ) -> cmclient_control_api::ControlStatus
    where
        F: FnMut(&cmclient_control_api::ControlStatus) -> bool,
    {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Ok(status) = client.status() {
                if predicate(&status) {
                    return status;
                }
            }
            assert!(
                Instant::now() < deadline,
                "Agent status did not reach the expected state; stderr: {}",
                read_log(&self.stderr)
            );
            thread::sleep(Duration::from_millis(100));
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

struct RunningAgent {
    child: Child,
}

impl RunningAgent {
    fn shutdown(&mut self, client: &ControlClient) {
        let _ = client.shutdown_agent();
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match self.child.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(100)),
                Ok(None) | Err(_) => {
                    let _ = self.child.kill();
                    let _ = self.child.wait();
                    return;
                }
            }
        }
    }
}

impl Drop for RunningAgent {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

#[test]
#[ignore = "requires a staged official Node runtime and production source build"]
fn production_agent_uses_staged_private_node_control_and_web() {
    let fixture = Fixture::new();
    let mut agent = fixture.spawn();
    let client = fixture.wait_for_control(&mut agent);
    let initial = fixture.wait_for_status(&client, |status| {
        status.management_web == ManagementWebControlStatus::Running
            && status.management_web_url.is_some()
            && status.gateway == GatewayControlStatus::Stopped
    });
    let base = initial
        .management_web_url
        .expect("management Web URL should be advertised");
    let http = Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("HTTP client should build");

    let root = http
        .get(format!("{}/", base))
        .header(HOST, MANAGEMENT_HOST)
        .send()
        .expect("management Web root should respond");
    assert_eq!(root.status(), reqwest::StatusCode::OK);
    assert!(
        root.text()
            .expect("management Web root should read")
            .contains("<html")
    );

    let spoofed = http
        .get(format!("{}/api/v1/auth/session", base))
        .header(HOST, "evil.example:7080")
        .send()
        .expect("spoofed host request should respond");
    assert_eq!(spoofed.status(), reqwest::StatusCode::FORBIDDEN);
    assert_eq!(stable_code(spoofed), "MANAGEMENT_HOST_DENIED");

    let session = http
        .get(format!("{}/api/v1/auth/session", base))
        .header(HOST, MANAGEMENT_HOST)
        .send()
        .expect("local session should respond");
    assert_eq!(session.status(), reqwest::StatusCode::OK);
    let cookie = session_cookie(&session);
    let session_json = response_json(session);
    assert_eq!(session_json["schemaVersion"], 1);
    let csrf = session_json["csrfToken"]
        .as_str()
        .expect("local session should contain CSRF")
        .to_owned();
    assert_eq!(csrf.len(), 32);

    let started = client
        .start()
        .expect("Gateway should start through Control");
    assert!(matches!(
        started.gateway,
        GatewayControlStatus::Running | GatewayControlStatus::Degraded
    ));
    fixture.wait_for_status(&client, |status| {
        status.gateway == GatewayControlStatus::Running
    });

    let health = http
        .get(format!("{}/api/v1/system/health", base))
        .header(HOST, MANAGEMENT_HOST)
        .header(COOKIE, &cookie)
        .send()
        .expect("Gateway health should proxy through management Web");
    assert_eq!(health.status(), reqwest::StatusCode::OK);
    assert_eq!(response_json(health), serde_json::json!({"status": "ok"}));

    let missing_csrf = http
        .post(format!("{}/api/v1/diagnostics/integrity-check", base))
        .header(HOST, MANAGEMENT_HOST)
        .header(ORIGIN, MANAGEMENT_ORIGIN)
        .header(COOKIE, &cookie)
        .header("idempotency-key", "windows-source-no-csrf")
        .send()
        .expect("missing CSRF request should respond");
    assert_eq!(missing_csrf.status(), reqwest::StatusCode::FORBIDDEN);
    assert_eq!(stable_code(missing_csrf), "MANAGEMENT_CSRF_INVALID");

    let diagnostics = http
        .post(format!("{}/api/v1/diagnostics/integrity-check", base))
        .header(HOST, MANAGEMENT_HOST)
        .header(ORIGIN, MANAGEMENT_ORIGIN)
        .header(COOKIE, &cookie)
        .header("x-csrf-token", &csrf)
        .header("idempotency-key", "windows-source-with-csrf")
        .header("content-type", "application/json")
        .body("{}")
        .send()
        .expect("authenticated diagnostics request should respond");
    assert_eq!(diagnostics.status(), reqwest::StatusCode::ACCEPTED);
    let diagnostics_json = response_json(diagnostics);
    assert!(diagnostics_json["jobId"].as_str().is_some());
    assert_eq!(diagnostics_json["reused"], false);

    client.stop().expect("Gateway should stop through Control");
    fixture.wait_for_status(&client, |status| {
        status.gateway == GatewayControlStatus::Stopped
    });
    agent.shutdown(&client);
}

fn private_log(path: &Path) -> File {
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("test log should open")
}

fn read_log(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_else(|error| {
        if error.kind() == ErrorKind::NotFound {
            String::new()
        } else {
            format!("<log unavailable: {error}>")
        }
    })
}

fn session_cookie(response: &Response) -> String {
    response
        .headers()
        .get(SET_COOKIE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .filter(|value| value.starts_with("cmclient.sid="))
        .map(str::to_owned)
        .expect("local session should set cmclient.sid")
}

fn stable_code(response: Response) -> String {
    serde_json::from_str::<Value>(&response.text().unwrap_or_default())
        .ok()
        .and_then(|value| value["code"].as_str().map(str::to_owned))
        .unwrap_or_else(|| String::from("<missing-code>"))
}

fn response_json(response: Response) -> Value {
    serde_json::from_str(&response.text().expect("HTTP response should be readable"))
        .expect("HTTP response should be JSON")
}
