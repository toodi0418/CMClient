import assert from "node:assert/strict";
import {
  chmod,
  mkdir,
  mkdtemp,
  readFile,
  rm,
  stat,
  symlink,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import test from "node:test";

const execute = promisify(execFile);
const manager = "scripts/cmclient-systemd.sh";
const systemdTest = process.platform === "win32" ? test.skip : test;

async function runManager(argumentsList, environment = {}) {
  return execute("bash", [manager, ...argumentsList], {
    env: {
      ...process.env,
      CMCLIENT_SYSTEMD_ALLOW_NON_LINUX: "1",
      ...environment,
    },
  });
}

function escaped(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

systemdTest(
  "systemd manager uses one service-home root and retains it on uninstall",
  async () => {
    const directory = await mkdtemp(join(tmpdir(), "cmclient-systemd-"));
    const agentV1 = join(directory, "releases/v1/bin/cmclient-agent");
    const agentV2 = join(directory, "releases/v2/bin/cmclient-agent");
    const unitDir = join(directory, "units");
    const serviceHome = join(directory, "home");
    const runtimeRoot = join(serviceHome, ".cmclient");
    const systemctl = join(directory, "systemctl");
    const calls = join(directory, "systemctl-calls");
    const { stdout: groupOutput } = await execute("id", ["-gn"]);
    const user = process.env.USER ?? process.env.LOGNAME ?? "runner";
    const group = groupOutput.trim();

    try {
      await mkdir(join(directory, "releases/v1/bin"), { recursive: true });
      await mkdir(join(directory, "releases/v2/bin"), { recursive: true });
      await writeFile(agentV1, "#!/usr/bin/env sh\nexit 0\n");
      await writeFile(agentV2, "#!/usr/bin/env sh\nexit 0\n");
      await chmod(agentV1, 0o755);
      await chmod(agentV2, 0o755);
      await writeFile(
        systemctl,
        `#!/usr/bin/env sh\nprintf '%s\\n' "$*" >> '${calls}'\n`,
      );
      await chmod(systemctl, 0o755);

      const shared = [
        "--agent",
        agentV1,
        "--unit-dir",
        unitDir,
        "--home",
        serviceHome,
        "--service-user",
        user,
        "--service-group",
        group,
        "--systemctl",
        systemctl,
        "--skip-user-setup",
      ];

      await runManager(["install", ...shared]);
      const unit = await readFile(
        join(unitDir, "cmclient-agent.service"),
        "utf8",
      );
      assert.match(unit, new RegExp(`ExecStart=${escaped(agentV1)} --serve`));
      assert.match(
        unit,
        new RegExp(`ExecStartPre=${escaped(agentV1)} --check-config`),
      );
      assert.match(
        unit,
        new RegExp(`Environment="HOME=${escaped(serviceHome)}"`),
      );
      assert.match(unit, new RegExp(`ReadWritePaths=${escaped(runtimeRoot)}`));
      assert.match(unit, /ProtectHome=read-only/);
      assert.match(unit, /NoNewPrivileges=true/);
      assert.match(unit, /CapabilityBoundingSet=\nAmbientCapabilities=/);
      assert.match(unit, /ProtectSystem=strict/);
      assert.doesNotMatch(
        unit,
        /CMCLIENT_(?:AGENT_CONFIG|DATA_DIR|CONFIG_DIR|CACHE_DIR|LOG_DIR|SYSTEMD_SECRET_STORE)/,
      );
      assert.doesNotMatch(unit, /LoadCredential|secret-store|vault/i);
      assert.doesNotMatch(unit, /CALLMESH|API_KEY|PASSCODE|TOKEN/);

      for (const relative of [
        "",
        "state",
        "run",
        "cache",
        "logs",
        "backups",
        "updates",
      ]) {
        const path = relative ? join(runtimeRoot, relative) : runtimeRoot;
        assert.equal((await stat(path)).mode & 0o777, 0o700);
      }
      await assert.rejects(stat(join(runtimeRoot, "secrets.json")));
      assert.match(
        await readFile(calls, "utf8"),
        /daemon-reload\nenable --now cmclient-agent.service\n/,
      );

      await writeFile(
        join(runtimeRoot, "retained-state"),
        "must survive uninstall",
      );
      const upgraded = shared.map((value) =>
        value === agentV1 ? agentV2 : value,
      );
      await runManager(["install", ...upgraded]);
      const upgradedUnit = await readFile(
        join(unitDir, "cmclient-agent.service"),
        "utf8",
      );
      assert.match(
        upgradedUnit,
        new RegExp(`ExecStart=${escaped(agentV2)} --serve`),
      );
      assert.equal(
        await readFile(join(runtimeRoot, "retained-state"), "utf8"),
        "must survive uninstall",
      );

      await runManager(["uninstall", ...upgraded]);
      await assert.rejects(
        readFile(join(unitDir, "cmclient-agent.service"), "utf8"),
      );
      assert.equal(
        await readFile(join(runtimeRoot, "retained-state"), "utf8"),
        "must survive uninstall",
      );
      assert.match(
        await readFile(calls, "utf8"),
        /disable --now cmclient-agent.service/,
      );
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  },
);

systemdTest(
  "systemd manager rejects unsafe paths and legacy split-root options",
  async () => {
    const directory = await mkdtemp(
      join(tmpdir(), "cmclient-systemd-invalid-"),
    );
    try {
      const unsafeAgent = join(directory, "agent with spaces");
      await writeFile(unsafeAgent, "fixture");
      await assert.rejects(
        runManager([
          "install",
          "--agent",
          unsafeAgent,
          "--unit-dir",
          join(directory, "units"),
          "--skip-user-setup",
        ]),
        /SYSTEMD_PATH_INVALID/,
      );
      await assert.rejects(
        runManager(["render", "--data-dir", join(directory, "data")]),
        /SYSTEMD_USAGE_INVALID_ARGUMENT/,
      );

      const safeAgent = join(directory, "agent");
      const serviceHome = join(directory, "home");
      const externalRoot = join(directory, "external-root");
      await writeFile(safeAgent, "#!/usr/bin/env sh\nexit 0\n");
      await chmod(safeAgent, 0o755);
      await mkdir(serviceHome);
      await mkdir(externalRoot);
      await symlink(externalRoot, join(serviceHome, ".cmclient"));
      await assert.rejects(
        runManager([
          "install",
          "--agent",
          safeAgent,
          "--unit-dir",
          join(directory, "units"),
          "--home",
          serviceHome,
          "--service-user",
          "runner",
          "--service-group",
          "runner",
          "--skip-user-setup",
        ]),
        /SYSTEMD_RUNTIME_DIRECTORY_INVALID/,
      );
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  },
);

test("CI exercises systemd 249 with canonical plaintext runtime state", async () => {
  const integration = await readFile(
    "scripts/cmclient-systemd-integration.sh",
    "utf8",
  );
  const plaintextRuntime = await readFile(
    "apps/agent/tests/plaintext_runtime.rs",
    "utf8",
  );
  const workflow = await readFile(".github/workflows/ci.yml", "utf8");
  const { stdout: integrationIndex } = await execute("git", [
    "ls-files",
    "--stage",
    "--",
    "scripts/cmclient-systemd-integration.sh",
  ]);

  assert.match(
    integrationIndex,
    /^100755 [a-f0-9]{40,64} 0\tscripts\/cmclient-systemd-integration\.sh\s*$/,
  );
  assert.match(integration, /EXPECTED_SYSTEMD_VERSION="249"/);
  assert.match(integration, /RUNTIME_ROOT="\$SERVICE_HOME\/\.cmclient"/);
  assert.match(
    integration,
    /CONTROL_SOCKET="\$RUNTIME_ROOT\/run\/control\.sock"/,
  );
  assert.match(integration, /SECRETS_FILE="\$RUNTIME_ROOT\/secrets\.json"/);
  assert.match(integration, /config\.toml/);
  assert.doesNotMatch(integration, /CREDENTIALS_DIRECTORY=|LoadCredential/);
  assert.doesNotMatch(integration, /secret-store\.key|\.secret"/);
  assert.match(integration, /unix:\/\/\$CONTROL_SOCKET/);
  assert.match(integration, /"managementWeb":"disabled"/);
  assert.doesNotMatch(integration, /"management_web":"disabled"/);
  assert.doesNotMatch(integration, /\bgateway_port\b|CMCLIENT_GATEWAY_PORT/);
  assert.doesNotMatch(integration, /\/usr\/bin\/sleep/);
  assert.match(integration, /cmclient-systemd-gateway-fixture\.py/);
  assert.match(integration, /bootstrap\.get\("type"\) != "gateway\.bootstrap"/);
  assert.match(integration, /"type": "gateway\.ready"/);
  assert.match(integration, /listener\.bind\(\("127\.0\.0\.1", 0\)\)/);
  assert.match(integration, /\/_cmclient\/bootstrap\/ownership/);
  assert.match(integration, /cmclient-bootstrap-ownership-v1/);
  assert.match(integration, /cmclient\.gateway\.bootstrap-ownership\.v1/);
  assert.match(integration, /x-cmclient-gateway-ownership-challenge/);
  assert.match(integration, /x-cmclient-gateway-ownership-proof/);
  assert.match(integration, /digestmod=hashlib\.sha256/);
  assert.match(integration, /"x-cmclient-gateway-capability" not in headers/);
  assert.match(integration, /ownership_proven\.set\(\)/);
  assert.match(integration, /\/api\/v1\/system\/version/);
  assert.match(integration, /CMCLIENT_SHUTDOWN\\n/);
  assert.match(plaintextRuntime, /import \{ createHmac \} from "node:crypto"/);
  assert.match(plaintextRuntime, /server\.on\("upgrade"/);
  assert.match(plaintextRuntime, /\/_cmclient\/bootstrap\/ownership/);
  assert.match(plaintextRuntime, /cmclient-bootstrap-ownership-v1/);
  assert.match(plaintextRuntime, /cmclient\.gateway\.bootstrap-ownership\.v1/);
  assert.match(
    plaintextRuntime,
    /createHmac\("sha256", bootstrap\.capability\)/,
  );
  assert.match(
    plaintextRuntime,
    /request\.headers\["x-cmclient-gateway-capability"\] !== undefined/,
  );
  assert.match(plaintextRuntime, /ownershipProven = true/);
  assert.match(integration, /systemctl restart "\$SERVICE_NAME"/);
  assert.match(workflow, /linux-systemd-smoke:/);
  assert.match(workflow, /runs-on: ubuntu-22\.04/);
  assert.match(workflow, /bash scripts\/cmclient-systemd-integration\.sh/);
});

systemdTest(
  "systemd logs prefer bounded canonical JSONL and sanitize journal fallback",
  async () => {
    const directory = await mkdtemp(join(tmpdir(), "cmclient-systemd-logs-"));
    const serviceHome = join(directory, "home");
    const logDir = join(serviceHome, ".cmclient/logs");
    const journalctl = join(directory, "journalctl");
    const journalCalls = join(directory, "journalctl-calls");

    try {
      await mkdir(logDir, { recursive: true });
      await writeFile(join(logDir, "agent.jsonl"), '{"code":"AGENT_LEGACY"}\n');
      await writeFile(
        join(logDir, "gateway.jsonl"),
        '{"code":"GATEWAY_LEGACY"}\n',
      );
      await writeFile(
        join(logDir, "agent.jsonl.2026-07-21"),
        '{"code":"AGENT_DAILY_OLD"}\n',
      );
      await writeFile(
        join(logDir, "agent.jsonl.2026-07-22"),
        '{"code":"AGENT_CURRENT"}\n',
      );
      await writeFile(
        join(logDir, "gateway.jsonl.2026-07-22"),
        '{"code":"GATEWAY_CURRENT"}\n',
      );
      await writeFile(
        journalctl,
        `#!/usr/bin/env sh\nprintf '%s\\n' "$*" >> '${journalCalls}'\nprintf '%s\\n' 'AGENT_START_FAILED' 'raw secret must not escape'\n`,
      );
      await chmod(journalctl, 0o755);

      const { stdout: applicationOutput } = await runManager([
        "logs",
        "--home",
        serviceHome,
        "--journalctl",
        journalctl,
        "--lines",
        "1",
      ]);
      assert.match(applicationOutput, /AGENT_CURRENT/);
      assert.match(applicationOutput, /GATEWAY_CURRENT/);
      assert.doesNotMatch(applicationOutput, /DAILY_OLD|LEGACY/);
      await assert.rejects(readFile(journalCalls, "utf8"));

      await rm(join(logDir, "agent.jsonl.2026-07-21"));
      await rm(join(logDir, "agent.jsonl.2026-07-22"));
      await rm(join(logDir, "gateway.jsonl.2026-07-22"));
      const { stdout: legacyOutput } = await runManager([
        "logs",
        "--home",
        serviceHome,
        "--journalctl",
        journalctl,
        "--lines",
        "1",
      ]);
      assert.match(legacyOutput, /AGENT_LEGACY/);
      assert.match(legacyOutput, /GATEWAY_LEGACY/);
      await assert.rejects(readFile(journalCalls, "utf8"));

      await rm(join(logDir, "agent.jsonl"));
      await rm(join(logDir, "gateway.jsonl"));
      await writeFile(join(logDir, "agent.jsonl.2026-99-99"), "invalid\n");
      await assert.rejects(
        runManager(["logs", "--home", serviceHome]),
        /SYSTEMD_LOG_FILE_INVALID/,
      );
      await rm(join(logDir, "agent.jsonl.2026-99-99"));
      const { stdout: fallbackOutput } = await runManager([
        "logs",
        "--home",
        serviceHome,
        "--unit-dir",
        join(directory, "units"),
        "--journalctl",
        journalctl,
        "--lines",
        "17",
        "--skip-user-setup",
      ]);
      assert.equal(fallbackOutput, "AGENT_START_FAILED\n");
      assert.match(
        await readFile(journalCalls, "utf8"),
        /--unit cmclient-agent\.service --no-pager --lines 17 --output cat/,
      );
      await assert.rejects(
        runManager(["logs", "--home", serviceHome, "--lines", "10001"]),
        /SYSTEMD_LOG_LINES_INVALID/,
      );

      const externalLog = join(directory, "external.jsonl");
      await writeFile(externalLog, '{"code":"MUST_NOT_BE_READ"}\n');
      await symlink(externalLog, join(logDir, "agent.jsonl.2026-07-22"));
      await assert.rejects(
        runManager(["logs", "--home", serviceHome]),
        /SYSTEMD_LOG_FILE_INVALID/,
      );
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  },
);
