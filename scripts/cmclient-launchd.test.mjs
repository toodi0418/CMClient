import assert from "node:assert/strict";
import { chmod, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import test from "node:test";

const execute = promisify(execFile);
const manager = "scripts/cmclient-launchd.sh";
const launchdTest = process.platform === "win32" ? test.skip : test;

async function runManager(argumentsList, environment = {}) {
  return execute("bash", [manager, ...argumentsList], {
    env: {
      ...process.env,
      CMCLIENT_LAUNCHD_ALLOW_NON_DARWIN: "1",
      ...environment,
    },
  });
}

launchdTest("launchd manager installs a per-user Agent plist without credential values", async () => {
  const directory = await mkdtemp(join(tmpdir(), "cmclient-launchd-"));
  const agent = join(directory, "release/bin/cmclient-agent");
  const home = join(directory, "home");
  const plist = join(home, "Library/LaunchAgents/io.cmclient.agent.plist");
  const data = join(home, "Library/Application Support/CMClient");
  const cache = join(home, "Library/Caches/CMClient");
  const launchctl = join(directory, "launchctl");
  const plutil = join(directory, "plutil");
  const calls = join(directory, "launchctl-calls");

  await mkdir(join(directory, "release/bin"), { recursive: true });
  await writeFile(agent, "#!/usr/bin/env sh\nexit 0\n");
  await chmod(agent, 0o755);
  await writeFile(launchctl, `#!/usr/bin/env sh\nprintf '%s\\n' "$*" >> '${calls}'\n`);
  await writeFile(plutil, "#!/usr/bin/env sh\nexit 0\n");
  await chmod(launchctl, 0o755);
  await chmod(plutil, 0o755);

  const shared = [
    "--agent",
    agent,
    "--plist",
    plist,
    "--data-dir",
    data,
    "--config-dir",
    data,
    "--cache-dir",
    cache,
    "--log-dir",
    join(data, "Logs"),
  ];
  const environment = {
    HOME: home,
    CMCLIENT_LAUNCHCTL: launchctl,
    CMCLIENT_PLUTIL: plutil,
  };

  await runManager(["install", ...shared], environment);
  const contents = await readFile(plist, "utf8");
  assert.match(contents, /<string>io\.cmclient\.agent<\/string>/);
  assert.match(contents, new RegExp(`<string>${agent}<\\/string>`));
  assert.match(contents, /<key>KeepAlive<\/key>/);
  assert.doesNotMatch(contents, /CALLMESH|API_KEY|PASSCODE|TOKEN/);
  assert.match(await readFile(calls, "utf8"), /bootstrap gui\/\d+ .*io\.cmclient\.agent\.plist/);

  await writeFile(join(data, "retained-state"), "must survive uninstall");
  await runManager(["uninstall", ...shared], environment);
  await assert.rejects(readFile(plist, "utf8"));
  assert.equal(await readFile(join(data, "retained-state"), "utf8"), "must survive uninstall");
  await rm(directory, { recursive: true, force: true });
});

launchdTest("launchd manager rejects unsafe executable paths", async () => {
  const directory = await mkdtemp(join(tmpdir(), "cmclient-launchd-invalid-"));
  await assert.rejects(
    runManager(["render", "--agent", join(directory, "agent|unsafe")]),
    /LAUNCHD_PATH_INVALID/,
  );
  await rm(directory, { recursive: true, force: true });
});
