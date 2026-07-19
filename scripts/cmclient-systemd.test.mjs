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

systemdTest(
  "systemd manager upgrades its unit and retains runtime state on uninstall",
  async () => {
    const directory = await mkdtemp(join(tmpdir(), "cmclient-systemd-"));
    const agentV1 = join(directory, "releases/v1/bin/cmclient-agent");
    const agentV2 = join(directory, "releases/v2/bin/cmclient-agent");
    const unitDir = join(directory, "units");
    const configDir = join(directory, "config");
    const dataDir = join(directory, "data");
    const cacheDir = join(directory, "cache");
    const logDir = join(directory, "logs");
    const systemctl = join(directory, "systemctl");
    const calls = join(directory, "systemctl-calls");
    const { stdout: groupOutput } = await execute("id", ["-gn"]);
    const user = process.env.USER ?? process.env.LOGNAME ?? "runner";
    const group = groupOutput.trim();

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
      "--config-dir",
      configDir,
      "--data-dir",
      dataDir,
      "--cache-dir",
      cacheDir,
      "--log-dir",
      logDir,
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
    assert.match(unit, new RegExp(`ExecStart=${agentV1} --serve`));
    assert.match(unit, new RegExp(`ExecStartPre=${agentV1} --check-config`));
    assert.match(unit, new RegExp(`CMCLIENT_DATA_DIR=${dataDir}`));
    assert.match(unit, /CMCLIENT_SYSTEMD_SECRET_STORE=1/);
    assert.match(
      unit,
      new RegExp(
        `LoadCredential=cmclient-secret-store-key:${configDir}/secret-store.key`,
      ),
    );
    assert.match(unit, /NoNewPrivileges=true/);
    assert.match(unit, /CapabilityBoundingSet=\nAmbientCapabilities=/);
    assert.match(unit, /ProtectSystem=strict/);
    assert.doesNotMatch(unit, /CALLMESH|API_KEY|PASSCODE|TOKEN/);
    const wrappingKey = await readFile(join(configDir, "secret-store.key"));
    assert.equal(wrappingKey.length, 32);
    assert.equal(
      (await stat(join(configDir, "secret-store.key"))).mode & 0o777,
      0o600,
    );
    assert.match(
      await readFile(calls, "utf8"),
      /daemon-reload\nenable --now cmclient-agent.service\n/,
    );

    await writeFile(join(dataDir, "retained-state"), "must survive uninstall");
    const upgraded = shared.map((value) =>
      value === agentV1 ? agentV2 : value,
    );
    await runManager(["install", ...upgraded]);
    const upgradedUnit = await readFile(
      join(unitDir, "cmclient-agent.service"),
      "utf8",
    );
    assert.match(upgradedUnit, new RegExp(`ExecStart=${agentV2} --serve`));
    assert.equal(
      await readFile(join(dataDir, "retained-state"), "utf8"),
      "must survive uninstall",
    );
    assert.deepEqual(
      await readFile(join(configDir, "secret-store.key")),
      wrappingKey,
    );

    await runManager(["uninstall", ...upgraded]);
    await assert.rejects(
      readFile(join(unitDir, "cmclient-agent.service"), "utf8"),
    );
    assert.equal(
      await readFile(join(dataDir, "retained-state"), "utf8"),
      "must survive uninstall",
    );
    assert.deepEqual(
      await readFile(join(configDir, "secret-store.key")),
      wrappingKey,
    );
    assert.match(
      await readFile(calls, "utf8"),
      /disable --now cmclient-agent.service/,
    );

    await rm(join(configDir, "secret-store.key"));
    await mkdir(join(dataDir, "secrets"), { recursive: true });
    await writeFile(
      join(dataDir, "secrets", "aprs-passcode.secret"),
      "retained ciphertext",
    );
    await assert.rejects(
      runManager(["install", ...upgraded]),
      /SYSTEMD_SECRET_STORE_KEY_MISSING/,
    );
    await rm(directory, { recursive: true, force: true });
  },
);

systemdTest(
  "systemd manager rejects non-executable or unsafe Agent paths",
  async () => {
    const directory = await mkdtemp(
      join(tmpdir(), "cmclient-systemd-invalid-"),
    );
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
    await rm(directory, { recursive: true, force: true });
  },
);

systemdTest(
  "systemd manager fails closed when the secret store cannot be scanned",
  async () => {
    const directory = await mkdtemp(join(tmpdir(), "cmclient-systemd-scan-"));
    const agent = join(directory, "bin/cmclient-agent");
    const unitDir = join(directory, "units");
    const configDir = join(directory, "config");
    const dataDir = join(directory, "data");
    const cacheDir = join(directory, "cache");
    const logDir = join(directory, "logs");
    const systemctl = join(directory, "systemctl");
    const calls = join(directory, "systemctl-calls");
    const { stdout: groupOutput } = await execute("id", ["-gn"]);
    const user = process.env.USER ?? process.env.LOGNAME ?? "runner";
    const group = groupOutput.trim();

    try {
      await mkdir(join(directory, "bin"), { recursive: true });
      await writeFile(agent, "#!/usr/bin/env sh\nexit 0\n");
      await chmod(agent, 0o755);
      await writeFile(
        systemctl,
        `#!/usr/bin/env sh\nprintf '%s\\n' "$*" >> '${calls}'\n`,
      );
      await chmod(systemctl, 0o755);

      const shared = [
        "--agent",
        agent,
        "--unit-dir",
        unitDir,
        "--config-dir",
        configDir,
        "--data-dir",
        dataDir,
        "--cache-dir",
        cacheDir,
        "--log-dir",
        logDir,
        "--service-user",
        user,
        "--service-group",
        group,
        "--systemctl",
        systemctl,
        "--skip-user-setup",
      ];

      await runManager(["install", ...shared]);
      await rm(join(configDir, "secret-store.key"));
      const secretsDir = join(dataDir, "secrets");
      await mkdir(secretsDir, { recursive: true });
      await chmod(secretsDir, 0o000);
      await assert.rejects(
        runManager(["install", ...shared]),
        /SYSTEMD_SECRET_STORE_SCAN_FAILED/,
      );
      await chmod(secretsDir, 0o750);

      await rm(secretsDir, { recursive: true, force: true });
      const externalSecretsDir = join(directory, "external-secrets");
      await mkdir(externalSecretsDir);
      await symlink(externalSecretsDir, secretsDir);
      await assert.rejects(
        runManager(["install", ...shared]),
        /SYSTEMD_SECRET_STORE_DIRECTORY_INVALID/,
      );
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  },
);

systemdTest(
  "CI exercises systemd 249 credentials and the dedicated Control socket",
  async () => {
    const integration = await readFile(
      "scripts/cmclient-systemd-integration.sh",
      "utf8",
    );
    const workflow = await readFile(".github/workflows/ci.yml", "utf8");
    const integrationMode = (
      await stat("scripts/cmclient-systemd-integration.sh")
    ).mode;

    assert.notEqual(integrationMode & 0o111, 0);
    assert.match(integration, /EXPECTED_SYSTEMD_VERSION="249"/);
    assert.match(integration, /CREDENTIALS_DIRECTORY=/);
    assert.match(integration, /unix:\/\/\$CONTROL_SOCKET/);
    assert.match(integration, /systemctl restart "\$SERVICE_NAME"/);
    assert.match(workflow, /linux-systemd-smoke:/);
    assert.match(workflow, /runs-on: ubuntu-22\.04/);
    assert.match(workflow, /bash scripts\/cmclient-systemd-integration\.sh/);
  },
);
