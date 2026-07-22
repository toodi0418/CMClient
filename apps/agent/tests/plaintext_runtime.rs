#![cfg(unix)]

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
const CALLMESH_URL: &str = "https://callmesh.example.invalid";
const INHERITED_CALLMESH_SECRET: &str = "fixture-parent-callmesh-ignored";
const INHERITED_APRS_SECRET: &str = "fixture-parent-aprs-ignored";
const INHERITED_CONTROL_TOKEN: &str = "fixture-parent-control-token-ignored";

struct Fixture {
    root: PathBuf,
    config: PathBuf,
    data: PathBuf,
    cache: PathBuf,
    logs: PathBuf,
    secret_parent: PathBuf,
    secret_file: PathBuf,
    marker: PathBuf,
    agent_stdout: PathBuf,
    agent_stderr: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let root = unique_private_directory("cmclient-agent-plaintext-e2e");
        let config_dir = root.join("config");
        let data = root.join("data");
        let cache = root.join("cache");
        let logs = root.join("logs");
        let secret_parent = root.join("credentials");
        let home = root.join("home");
        for directory in [&config_dir, &data, &cache, &logs, &secret_parent, &home] {
            fs::create_dir(directory).expect("fixture directory should exist");
            set_private_mode(directory);
        }

        let gateway_script = root.join("gateway-fixture.mjs");
        fs::write(
            &gateway_script,
            r#"import { createHmac } from "node:crypto";
import { writeFileSync } from "node:fs";
import { createServer } from "node:http";

const OWNERSHIP_PATH = "/_cmclient/bootstrap/ownership";
const OWNERSHIP_PROTOCOL = "cmclient-bootstrap-ownership-v1";
const OWNERSHIP_DOMAIN = "cmclient.gateway.bootstrap-ownership.v1";
const OWNERSHIP_CHALLENGE_HEADER =
  "x-cmclient-gateway-ownership-challenge";
const OWNERSHIP_PROOF_HEADER = "x-cmclient-gateway-ownership-proof";

const marker = process.argv[2];
let markerState = "key-absent";
if (Object.hasOwn(process.env, "CMCLIENT_PLAINTEXT_SECRET_FILE")) {
  markerState = "selector-leaked";
} else if (
  Object.hasOwn(process.env, "CMCLIENT_APRS_PASSCODE") ||
  Object.hasOwn(process.env, "CMCLIENT_CONTROL_TOKEN") ||
  Object.hasOwn(process.env, "CMCLIENT_SYSTEMD_SECRET_STORE")
) {
  markerState = "sensitive-env-leaked";
} else if (process.env.CMCLIENT_CALLMESH_URL !== "https://callmesh.example.invalid") {
  markerState = "url-mismatch";
} else if (Object.hasOwn(process.env, "CMCLIENT_CALLMESH_API_KEY")) {
  markerState =
    process.env["CMCLIENT_CALLMESH_API_KEY"] === "fixture-callmesh-value"
      ? "key-present"
      : "key-unexpected";
}
writeFileSync(marker, markerState, { mode: 0o600 });

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
    if (length < 1 || length > 4096 || input.length < length + 4) return;
    bootstrap = JSON.parse(input.subarray(4, length + 4).toString("utf8"));
    input = input.subarray(length + 4);
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
"#,
        )
        .expect("gateway fixture should write");
        set_private_mode(&gateway_script);

        let marker = root.join("gateway-marker");
        let config = config_dir.join("agent.toml");
        let config_text = format!(
            "[agent]\ngateway_command = [\"node\", {}, {}]\nmanagement_web_enabled = false\n\n[callmesh]\nurl = \"{}\"\n",
            toml_string(&gateway_script),
            toml_string(&marker),
            CALLMESH_URL,
        );
        fs::write(&config, config_text).expect("Agent config should write");
        set_private_mode(&config);
        let agent_stdout = root.join("agent-stdout.log");
        let agent_stderr = root.join("agent-stderr.log");

        Self {
            secret_file: secret_parent.join("runtime.json"),
            root,
            config,
            data,
            cache,
            logs,
            secret_parent,
            marker,
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
            .env("CMCLIENT_AGENT_CONFIG", &self.config)
            .env("CMCLIENT_DATA_DIR", &self.data)
            .env(
                "CMCLIENT_CONFIG_DIR",
                self.config.parent().expect("config parent"),
            )
            .env("CMCLIENT_CACHE_DIR", &self.cache)
            .env("CMCLIENT_LOG_DIR", &self.logs)
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
        default_local_endpoint(&self.data)
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
            if let Ok(value) = fs::read_to_string(&self.marker) {
                if value == expected {
                    return;
                }
                if !value.is_empty() {
                    panic!("Gateway fixture reported unexpected boundary state: {value}");
                }
            }
            assert!(
                Instant::now() < deadline,
                "Gateway fixture did not report {expected}"
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
        let agent_log = self.logs.join("agent.jsonl");
        let gateway_log = self.logs.join("gateway.jsonl");
        for path in [
            &self.agent_stdout,
            &self.agent_stderr,
            &agent_log,
            &gateway_log,
        ] {
            if let Ok(contents) = fs::read_to_string(path) {
                for sensitive in [
                    FIXTURE_SECRET,
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
    assert!(!fixture.secret_file.exists());
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
    fixture.clear_marker();
    second_client.start().expect("Gateway child should start");
    fixture.wait_for_marker("key-present");
    second_client.stop().expect("Gateway child should stop");

    let removed = second_client
        .remove_secret(ControlSecretKind::CallMeshApiKey)
        .expect("Agent should remove the fixture secret through Control API");
    assert!(removed.stored);
    second_agent.shutdown(&second_client);

    let mut third_agent = fixture.spawn_agent();
    let third_client = fixture.wait_for_client(&mut third_agent);
    fixture.clear_marker();
    third_client.start().expect("Gateway child should restart");
    fixture.wait_for_marker("key-absent");
    third_client.stop().expect("Gateway child should stop");
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

fn toml_string(path: &Path) -> String {
    let value = path.to_str().expect("fixture path should be UTF-8");
    format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
}
