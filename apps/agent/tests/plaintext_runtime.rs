#![cfg(unix)]

use cmclient_agent_core::{
    RuntimePaths,
    secrets::AgentSecretStore,
    setup::{CURRENT_TERMS_VERSION, SetupStore},
};
use cmclient_control_api::{
    ControlClient, ControlEndpoint, ControlSecretKind, default_local_endpoint,
};
use std::{
    fs,
    fs::OpenOptions,
    io::ErrorKind,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use std::os::unix::fs::PermissionsExt;

const FIXTURE_SECRET: &str = "fixture-callmesh-value";
const FIXTURE_CMCLOUD_ENDPOINT: &str = "wss://cmcloud.example.invalid/agent/v1";
const FIXTURE_CMCLOUD_PAIRING_CODE: &str = "fixture-pairing-code-0123456789";
const FIXTURE_CMCLOUD_CREDENTIAL: &str = "fixture-device-credential-0123456789";
const INHERITED_CALLMESH_SECRET: &str = "fixture-parent-callmesh-ignored";
const INHERITED_APRS_SECRET: &str = "fixture-parent-aprs-ignored";
const INHERITED_CONTROL_TOKEN: &str = "fixture-parent-control-token-ignored";

struct Fixture {
    root: PathBuf,
    data: PathBuf,
    logs: PathBuf,
    secret_parent: PathBuf,
    secret_file: PathBuf,
    marker: PathBuf,
    private_node: PathBuf,
    gateway_entrypoint: PathBuf,
    agent_stdout: PathBuf,
    agent_stderr: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let root = unique_private_directory("cmclient-agent-plaintext-e2e");
        let home = root.join("home");
        let data = home.join(".cmclient");
        let cache = data.join("cache");
        let logs = data.join("logs");
        for directory in [&home, &data, &cache, &logs] {
            fs::create_dir(directory).expect("fixture directory should exist");
            set_private_mode(directory);
        }
        let paths = RuntimePaths {
            data_dir: data.clone(),
            config_dir: data.clone(),
            cache_dir: cache.clone(),
            log_dir: logs.clone(),
        };
        let setup = SetupStore::open(&paths).expect("setup state should initialize");
        setup
            .accept_terms(CURRENT_TERMS_VERSION)
            .expect("fixture terms should be accepted");
        let fence = setup
            .begin_validation()
            .expect("fixture validation should begin");
        setup
            .mark_ready(fence)
            .expect("fixture setup should become ready");
        let secrets = AgentSecretStore::runtime(&data).expect("secret store should initialize");
        secrets
            .begin_cmcloud_enrollment(
                FIXTURE_CMCLOUD_ENDPOINT,
                FIXTURE_CMCLOUD_PAIRING_CODE,
                "2.0.0-rc.1",
            )
            .expect("CMCloud pairing fixture should begin");
        secrets
            .record_cmcloud_issued_credential(0, 1, 1, FIXTURE_CMCLOUD_CREDENTIAL)
            .expect("CMCloud credential fixture should persist");
        secrets
            .activate_cmcloud_credential(0, 1, 1)
            .expect("CMCloud credential fixture should activate");
        let secret_parent = data.clone();

        let marker = root.join("gateway-marker");
        let gateway_script = root.join("gateway-fixture.mjs");
        let gateway_source = r#"import { createHmac } from "node:crypto";
import { writeFileSync } from "node:fs";
import { createServer } from "node:http";

const OWNERSHIP_PATH = "/_cmclient/bootstrap/ownership";
const OWNERSHIP_PROTOCOL = "cmclient-bootstrap-ownership-v1";
const OWNERSHIP_DOMAIN = "cmclient.gateway.bootstrap-ownership.v1";
const OWNERSHIP_CHALLENGE_HEADER =
  "x-cmclient-gateway-ownership-challenge";
const OWNERSHIP_PROOF_HEADER = "x-cmclient-gateway-ownership-proof";

const marker = "__CMCLIENT_FIXTURE_MARKER__";
let boundaryError;
if (Object.hasOwn(process.env, "CMCLIENT_PLAINTEXT_SECRET_FILE")) {
  boundaryError = "selector-leaked";
} else if (
  Object.hasOwn(process.env, "CMCLIENT_APRS_PASSCODE") ||
  Object.hasOwn(process.env, "CMCLIENT_CONTROL_TOKEN") ||
  Object.hasOwn(process.env, "CMCLIENT_SYSTEMD_SECRET_STORE")
) {
  boundaryError = "sensitive-env-leaked";
} else if (Object.hasOwn(process.env, "CMCLIENT_CALLMESH_URL")) {
  boundaryError = "url-override-leaked";
} else if (Object.hasOwn(process.env, "CMCLIENT_CALLMESH_API_KEY")) {
  boundaryError = "sensitive-env-leaked";
} else if (
  process.argv.some((argument) =>
    argument.includes("fixture-callmesh-value") ||
    argument.includes("fixture-parent-callmesh-ignored")
  )
) {
  boundaryError = "sensitive-argv-leaked";
}

let input = Buffer.alloc(0);
let bootstrap;
let server;
let shuttingDown = false;
let ownershipProven = false;

process.stdin.on("data", (chunk) => {
  input = Buffer.concat([input, chunk]);
  if (!bootstrap) {
    if (input.length < 4) return;
    const length = input.readUInt32BE(0);
    if (length < 1 || length > 16384 || input.length < length + 4) return;
    bootstrap = JSON.parse(input.subarray(4, length + 4).toString("utf8"));
    input = input.subarray(length + 4);
    const markerState = boundaryError ?? (
      Object.hasOwn(bootstrap, "cmCloudDeviceCredential")
        ? bootstrap.cmCloudDeviceCredential === "fixture-device-credential-0123456789"
          ? "credential-present"
          : "credential-unexpected"
        : "credential-absent"
    );
    writeFileSync(marker, markerState, { mode: 0o600 });
    startGateway();
  }
  if (input.includes(Buffer.from("CMCLIENT_SHUTDOWN\n"))) shutdown();
});
process.stdin.once("end", shutdown);
process.stdin.resume();

function startGateway() {
  server = createServer((request, response) => {
    if (!ownershipProven) {
      return sendJson(response, 403, { code: "GATEWAY_OWNERSHIP_REQUIRED" });
    }
    if (
      request.headers["x-cmclient-gateway-capability"] !== bootstrap.capability
    ) {
      return sendJson(response, 403, { code: "GATEWAY_CAPABILITY_REQUIRED" });
    }
    if (request.url === "/api/v1/system/health") {
      return sendJson(response, 200, { status: "ok" });
    }
    if (request.url === "/api/v1/system/version") {
      return sendJson(response, 200, {
        schemaVersion: 1,
        component: "gateway",
        identity: {
          schemaVersion: 1,
          product: "CMClient",
          version: process.env.CMCLIENT_BUILD_VERSION,
          sourceCommit: process.env.CMCLIENT_BUILD_COMMIT,
          sourceTree: process.env.CMCLIENT_BUILD_TREE,
          channel: process.env.CMCLIENT_BUILD_CHANNEL,
          target: {
            os: process.env.CMCLIENT_TARGET_OS,
            architecture: process.env.CMCLIENT_TARGET_ARCHITECTURE,
            profile: process.env.CMCLIENT_RUNTIME_PROFILE,
            packageProfile: process.env.CMCLIENT_PACKAGE_PROFILE,
          },
        },
      });
    }
    return sendJson(response, 404, { code: "NOT_FOUND" });
  });
  server.on("upgrade", (request, socket, head) => {
    const address = server.address();
    const challenge = exactRawHeader(
      request.rawHeaders,
      OWNERSHIP_CHALLENGE_HEADER,
    );
    if (
      address === null ||
      typeof address === "string" ||
      request.method !== "GET" ||
      request.url !== OWNERSHIP_PATH ||
      exactRawHeader(request.rawHeaders, "host") !==
        `127.0.0.1:${address.port}` ||
      exactRawHeader(request.rawHeaders, "connection")?.toLowerCase() !==
        "upgrade" ||
      exactRawHeader(request.rawHeaders, "upgrade")?.toLowerCase() !==
        OWNERSHIP_PROTOCOL ||
      exactRawHeader(request.rawHeaders, "content-length") !== "0" ||
      request.headers["transfer-encoding"] !== undefined ||
      request.headers["x-cmclient-gateway-capability"] !== undefined ||
      !isLowerHex(challenge, 64) ||
      head.length !== 0
    ) {
      socket.end(
        "HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n",
        "ascii",
      );
      return;
    }

    const transcript = `${OWNERSHIP_DOMAIN}\n${bootstrap.startupNonce}\n${process.pid}\n127.0.0.1\n${address.port}\n${challenge}`;
    const proof = createHmac("sha256", bootstrap.capability)
      .update(transcript, "utf8")
      .digest("hex");
    socket.end(
      `HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 0\r\n${OWNERSHIP_PROOF_HEADER}: ${proof}\r\n\r\n`,
      "ascii",
    );
    ownershipProven = true;
  });
  server.listen(0, "127.0.0.1", () => {
    const address = server.address();
    const body = Buffer.from(
      JSON.stringify({
        schemaVersion: 1,
        type: "gateway.ready",
        pid: process.pid,
        startupNonce: bootstrap.startupNonce,
        host: "127.0.0.1",
        port: address.port,
      }),
      "utf8",
    );
    const frame = Buffer.allocUnsafe(body.length + 4);
    frame.writeUInt32BE(body.length, 0);
    body.copy(frame, 4);
    process.stdout.end(frame);
  });
}

function exactRawHeader(rawHeaders, expectedName) {
  const values = [];
  for (let index = 0; index < rawHeaders.length; index += 2) {
    if (rawHeaders[index]?.toLowerCase() === expectedName.toLowerCase()) {
      values.push(rawHeaders[index + 1] ?? "");
    }
  }
  return values.length === 1 ? values[0] : undefined;
}

function isLowerHex(value, length) {
  return (
    typeof value === "string" &&
    value.length === length &&
    /^[0-9a-f]+$/.test(value)
  );
}

function sendJson(response, status, value) {
  const body = JSON.stringify(value);
  response.writeHead(status, {
    "content-type": "application/json",
    "content-length": Buffer.byteLength(body),
    connection: "close",
  });
  response.end(body);
}

function shutdown() {
  if (shuttingDown) return;
  shuttingDown = true;
  if (!server) return process.exit(0);
  server.close(() => process.exit(0));
}
"#
        .replace(
            "__CMCLIENT_FIXTURE_MARKER__",
            &marker.to_string_lossy().replace('\\', "\\\\").replace('"', "\\\""),
        );
        fs::write(&gateway_script, gateway_source).expect("gateway fixture should write");
        set_private_mode(&gateway_script);

        let config = data.join("config.toml");
        fs::write(
            &config,
            "[agent]\nmanagement_web_enabled = false\n\n[cmcloud]\nagent_websocket_url = \"wss://cmcloud.example.invalid/agent/v1\"\n",
        )
            .expect("Agent config should write");
        set_private_mode(&config);
        let private_node = std::env::split_paths(
            &std::env::var_os("PATH").expect("source test PATH should exist"),
        )
        .map(|directory| directory.join("node"))
        .find(|candidate| candidate.is_file())
        .and_then(|candidate| fs::canonicalize(candidate).ok())
        .expect("source test Node should resolve to an absolute file");
        let agent_stdout = root.join("agent-stdout.log");
        let agent_stderr = root.join("agent-stderr.log");

        Self {
            secret_file: secret_parent.join("secrets.json"),
            root,
            data,
            logs,
            secret_parent,
            marker,
            private_node,
            gateway_entrypoint: gateway_script,
            agent_stdout,
            agent_stderr,
        }
    }

    fn spawn_agent(&self) -> RunningAgent {
        let stdout = private_append_file(&self.agent_stdout);
        let stderr = private_append_file(&self.agent_stderr);
        let mut command = Command::new(env!("CARGO_BIN_EXE_cmclient-agent"));
        command
            .arg("--serve")
            .env_clear()
            .env("HOME", self.root.join("home"))
            .env(
                "PATH",
                std::env::var("PATH").expect("source test PATH should exist"),
            )
            .env("CMCLIENT_PRIVATE_NODE", &self.private_node)
            .env("CMCLIENT_GATEWAY_ENTRYPOINT", &self.gateway_entrypoint)
            .env("CMCLIENT_PLAINTEXT_SECRET_FILE", &self.secret_file)
            .env("CMCLIENT_CALLMESH_API_KEY", INHERITED_CALLMESH_SECRET)
            .env("CMCLIENT_APRS_PASSCODE", INHERITED_APRS_SECRET)
            .env("CMCLIENT_CONTROL_TOKEN", INHERITED_CONTROL_TOKEN)
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr));
        #[cfg(target_os = "macos")]
        command.env("CMCLIENT_SYSTEMD_SECRET_STORE", "parent-only-sentinel");
        let agent = command.spawn().expect("Agent process should spawn");
        RunningAgent { child: agent }
    }

    fn endpoint(&self) -> ControlEndpoint {
        default_local_endpoint(&self.data).expect("endpoint should derive")
    }

    fn wait_for_client(&self, agent: &mut RunningAgent) -> ControlClient {
        let client = ControlClient::new_with_timeout(self.endpoint(), Duration::from_millis(250))
            .expect("control client should initialize");
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if client.status().is_ok() {
                return client;
            }
            if let Ok(Some(status)) = agent.child.try_wait() {
                panic!("Agent exited before control endpoint was ready: {status}");
            }
            assert!(
                Instant::now() < deadline,
                "Agent control endpoint did not become ready"
            );
            thread::sleep(Duration::from_millis(50));
        }
    }

    fn wait_for_marker(&self, expected: &str) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match fs::read_to_string(&self.marker) {
                Ok(value) => {
                    let value = value.trim();
                    if value == expected {
                        return;
                    }
                    panic!("gateway fixture reported unexpected marker state: {value}");
                }
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(error) => panic!("gateway marker should read: {error}"),
            }
            assert!(
                Instant::now() < deadline,
                "gateway fixture did not report marker state {expected}"
            );
            thread::sleep(Duration::from_millis(25));
        }
    }

    fn clear_marker(&self) {
        match fs::remove_file(&self.marker) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => panic!("gateway marker should remove: {error}"),
        }
    }

    fn assert_secret_absent_from_outputs(&self) {
        let mut paths = vec![self.agent_stdout.clone(), self.agent_stderr.clone()];
        paths.extend(
            fs::read_dir(&self.logs)
                .expect("runtime log directory should read")
                .map(|entry| entry.expect("runtime log entry should read").path())
                .filter(|path| {
                    path.file_name()
                        .and_then(|name| name.to_str())
                        .is_some_and(|name| {
                            ["agent.jsonl", "gateway.jsonl"].iter().any(|prefix| {
                                name == *prefix || name.starts_with(&format!("{prefix}."))
                            })
                        })
                }),
        );
        for path in &paths {
            if let Ok(contents) = fs::read_to_string(path) {
                for sensitive in [
                    FIXTURE_SECRET,
                    FIXTURE_CMCLOUD_CREDENTIAL,
                    INHERITED_CALLMESH_SECRET,
                    INHERITED_APRS_SECRET,
                    INHERITED_CONTROL_TOKEN,
                ] {
                    assert!(!contents.contains(sensitive));
                }
            }
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
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match self.child.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(50)),
                Ok(None) => {
                    let _ = self.child.kill();
                    let _ = self.child.wait();
                    return;
                }
                Err(_) => {
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
fn production_plaintext_secret_survives_agent_restart_and_stays_out_of_gateway_env() {
    let fixture = Fixture::new();

    let mut first_agent = fixture.spawn_agent();
    let first_client = fixture.wait_for_client(&mut first_agent);
    assert!(fixture.secret_file.exists());
    let stored = first_client
        .store_secret(ControlSecretKind::CallMeshApiKey, FIXTURE_SECRET)
        .expect("Agent should store the fixture secret through Control API");
    assert!(stored.stored);
    let diagnostics = first_client
        .diagnostics_bundle()
        .expect("Agent diagnostics should load");
    assert!(
        !serde_json::to_string(&diagnostics)
            .expect("diagnostics should serialize")
            .contains(FIXTURE_SECRET)
    );
    assert_eq!(
        fs::metadata(&fixture.secret_parent)
            .expect("secret parent")
            .permissions()
            .mode()
            & 0o7777,
        0o700
    );
    assert_eq!(
        fs::metadata(&fixture.secret_file)
            .expect("secret file")
            .permissions()
            .mode()
            & 0o7777,
        0o600
    );
    first_agent.shutdown(&first_client);

    let mut second_agent = fixture.spawn_agent();
    let second_client = fixture.wait_for_client(&mut second_agent);
    fixture.wait_for_marker("credential-present");
    let second_lifecycle =
        ControlClient::new_with_timeout(fixture.endpoint(), Duration::from_secs(10))
            .expect("long-lived control client should initialize");
    second_lifecycle.stop().expect("Gateway child should stop");
    fixture.clear_marker();
    second_lifecycle
        .start()
        .expect("Gateway child should start");
    fixture.wait_for_marker("credential-present");

    let removed = second_client
        .remove_secret(ControlSecretKind::CallMeshApiKey)
        .expect("Agent should remove the fixture secret through Control API");
    assert!(removed.stored);
    second_agent.shutdown(&second_client);

    let mut third_agent = fixture.spawn_agent();
    let third_client = fixture.wait_for_client(&mut third_agent);
    fixture.wait_for_marker("credential-present");
    let third_lifecycle =
        ControlClient::new_with_timeout(fixture.endpoint(), Duration::from_secs(10))
            .expect("long-lived control client should initialize");
    third_lifecycle.stop().expect("Gateway child should stop");
    fixture.clear_marker();
    third_lifecycle
        .start()
        .expect("Gateway child should restart");
    fixture.wait_for_marker("credential-present");
    third_lifecycle.stop().expect("Gateway child should stop");
    third_agent.shutdown(&third_client);

    let document = fs::read_to_string(&fixture.secret_file).expect("secret document should remain");
    assert!(document.contains("\"version\":1"));
    assert!(!document.contains(FIXTURE_SECRET));
    fixture.assert_secret_absent_from_outputs();
}

fn unique_private_directory(prefix: &str) -> PathBuf {
    for attempt in 0..100u32 {
        let candidate =
            std::env::temp_dir().join(format!("{prefix}-{}-{attempt}", std::process::id()));
        match fs::create_dir(&candidate) {
            Ok(()) => {
                set_private_mode(&candidate);
                return candidate;
            }
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
            Err(error) => panic!("fixture root should create: {error}"),
        }
    }
    panic!("fixture root allocation exhausted");
}

fn set_private_mode(path: &Path) {
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .expect("fixture path should be private");
}

fn private_append_file(path: &Path) -> fs::File {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("fixture output should open");
    file.set_permissions(fs::Permissions::from_mode(0o600))
        .expect("fixture output should be private");
    file
}
