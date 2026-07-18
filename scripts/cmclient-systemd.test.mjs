import assert from "node:assert/strict";
import { chmod, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
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

systemdTest("systemd manager installs a hardened Agent-only unit and retains runtime state on uninstall", async () => {
  const directory = await mkdtemp(join(tmpdir(), "cmclient-systemd-"));
  const agent = join(directory, "release/bin/cmclient-agent");
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

  await mkdir(join(directory, "release/bin"), { recursive: true });
  await writeFile(agent, "#!/usr/bin/env sh\nexit 0\n");
  await chmod(agent, 0o755);
  await writeFile(systemctl, `#!/usr/bin/env sh\nprintf '%s\\n' "$*" >> '${calls}'\n`);
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
  const unit = await readFile(join(unitDir, "cmclient-agent.service"), "utf8");
  assert.match(unit, new RegExp(`ExecStart=${agent} --serve`));
  assert.match(unit, new RegExp(`ExecStartPre=${agent} --check-config`));
  assert.match(unit, new RegExp(`CMCLIENT_DATA_DIR=${dataDir}`));
  assert.match(unit, /NoNewPrivileges=true/);
  assert.match(unit, /CapabilityBoundingSet=\nAmbientCapabilities=/);
  assert.match(unit, /ProtectSystem=strict/);
  assert.doesNotMatch(unit, /CALLMESH|API_KEY|PASSCODE|TOKEN/);
  assert.match(await readFile(calls, "utf8"), /daemon-reload\nenable --now cmclient-agent.service\n/);

  await writeFile(join(dataDir, "retained-state"), "must survive uninstall");
  await runManager(["uninstall", ...shared]);
  await assert.rejects(readFile(join(unitDir, "cmclient-agent.service"), "utf8"));
  assert.equal(await readFile(join(dataDir, "retained-state"), "utf8"), "must survive uninstall");
  assert.match(await readFile(calls, "utf8"), /disable --now cmclient-agent.service/);
  await rm(directory, { recursive: true, force: true });
});

systemdTest("systemd manager rejects non-executable or unsafe Agent paths", async () => {
  const directory = await mkdtemp(join(tmpdir(), "cmclient-systemd-invalid-"));
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
});
