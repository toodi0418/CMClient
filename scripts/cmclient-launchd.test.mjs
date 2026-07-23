import assert from "node:assert/strict";
import {
  chmod,
  link,
  mkdir,
  mkdtemp,
  readFile,
  rm,
  symlink,
  writeFile,
} from "node:fs/promises";
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

launchdTest(
  "launchd manager upgrades its plist and retains runtime state on uninstall",
  async () => {
    const directory = await mkdtemp(join(tmpdir(), "cmclient-launchd-"));
    const agentV1 = join(directory, "releases/v1/bin/cmclient-agent");
    const agentV2 = join(directory, "releases/v2/bin/cmclient-agent");
    const home = join(directory, "home");
    const plist = join(home, "Library/LaunchAgents/io.cmclient.agent.plist");
    const data = join(home, "Library/Application Support/CMClient");
    const cache = join(home, "Library/Caches/CMClient");
    const launchctl = join(directory, "launchctl");
    const plutil = join(directory, "plutil");
    const calls = join(directory, "launchctl-calls");

    await mkdir(join(directory, "releases/v1/bin"), { recursive: true });
    await mkdir(join(directory, "releases/v2/bin"), { recursive: true });
    await writeFile(agentV1, "#!/usr/bin/env sh\nexit 0\n");
    await writeFile(agentV2, "#!/usr/bin/env sh\nexit 0\n");
    await chmod(agentV1, 0o755);
    await chmod(agentV2, 0o755);
    await writeFile(
      launchctl,
      `#!/usr/bin/env sh\nprintf '%s\\n' "$*" >> '${calls}'\n`,
    );
    await writeFile(plutil, "#!/usr/bin/env sh\nexit 0\n");
    await chmod(launchctl, 0o755);
    await chmod(plutil, 0o755);

    const shared = [
      "--agent",
      agentV1,
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
    assert.match(contents, new RegExp(`<string>${agentV1}<\\/string>`));
    assert.match(contents, /<key>KeepAlive<\/key>/);
    assert.match(
      contents,
      /<key>StandardOutPath<\/key>\s*<string>\/dev\/null<\/string>/,
    );
    assert.match(
      contents,
      /<key>StandardErrorPath<\/key>\s*<string>\/dev\/null<\/string>/,
    );
    assert.doesNotMatch(contents, /agent\.(?:stdout|stderr)\.log/);
    assert.doesNotMatch(contents, /CALLMESH|API_KEY|PASSCODE|TOKEN/);
    assert.doesNotMatch(contents, /CMCLIENT_PLAINTEXT_SECRET_FILE/);
    assert.match(
      await readFile(calls, "utf8"),
      /bootstrap gui\/\d+ .*io\.cmclient\.agent\.plist/,
    );

    await writeFile(join(data, "retained-state"), "must survive uninstall");
    const upgraded = shared.map((value) =>
      value === agentV1 ? agentV2 : value,
    );
    await runManager(["install", ...upgraded], environment);
    const upgradedContents = await readFile(plist, "utf8");
    assert.match(upgradedContents, new RegExp(`<string>${agentV2}<\\/string>`));
    assert.equal(
      await readFile(join(data, "retained-state"), "utf8"),
      "must survive uninstall",
    );

    await runManager(["uninstall", ...upgraded], environment);
    await assert.rejects(readFile(plist, "utf8"));
    assert.equal(
      await readFile(join(data, "retained-state"), "utf8"),
      "must survive uninstall",
    );
    await rm(directory, { recursive: true, force: true });
  },
);

launchdTest(
  "launchd renders only an owner-private plaintext secret file path",
  async () => {
    const directory = await mkdtemp(join(tmpdir(), "cmclient-launchd-secret-"));
    const home = join(directory, "home");
    const secretDirectory = join(directory, "private");
    const secretFile = join(secretDirectory, "agent-secrets.json");
    await mkdir(home, { recursive: true });
    await mkdir(secretDirectory, { mode: 0o700 });
    await writeFile(
      secretFile,
      '{"version":1,"callmesh-api-key":"fixture-value"}',
    );
    await chmod(secretFile, 0o600);

    const { stdout } = await runManager(
      [
        "render",
        "--agent",
        join(directory, "CMClient/bin/cmclient-agent"),
        "--plaintext-secret-file",
        secretFile,
      ],
      { HOME: home },
    );

    assert.match(stdout, /<key>CMCLIENT_PLAINTEXT_SECRET_FILE<\/key>/);
    assert.match(stdout, new RegExp(`<string>${secretFile}<\\/string>`));
    assert.doesNotMatch(stdout, /fixture-value/);

    const fromEnvironment = await runManager(
      ["render", "--agent", join(directory, "CMClient/bin/cmclient-agent")],
      {
        HOME: home,
        CMCLIENT_PLAINTEXT_SECRET_FILE: secretFile,
      },
    );
    assert.match(
      fromEnvironment.stdout,
      /<key>CMCLIENT_PLAINTEXT_SECRET_FILE<\/key>/,
    );
    assert.match(
      fromEnvironment.stdout,
      new RegExp(`<string>${secretFile}<\\/string>`),
    );
    assert.doesNotMatch(fromEnvironment.stdout, /fixture-value/);

    await chmod(secretFile, 0o644);
    await assert.rejects(
      runManager(
        [
          "render",
          "--agent",
          join(directory, "CMClient/bin/cmclient-agent"),
          "--plaintext-secret-file",
          secretFile,
        ],
        { HOME: home },
      ),
      /LAUNCHD_SECRET_FILE_INVALID/,
    );
    await assert.rejects(
      runManager(
        [
          "render",
          "--agent",
          join(directory, "CMClient/bin/cmclient-agent"),
          "--plaintext-secret-file",
          "relative-secrets.json",
        ],
        { HOME: home },
      ),
      /LAUNCHD_PATH_INVALID/,
    );

    await assert.rejects(
      runManager(
        [
          "render",
          "--agent",
          join(directory, "CMClient/bin/cmclient-agent"),
          "--plaintext-secret-file",
          join(directory, "missing-parent/agent-secrets.json"),
        ],
        { HOME: home },
      ),
      /LAUNCHD_SECRET_FILE_PARENT_INVALID/,
    );

    await chmod(secretFile, 0o600);
    const hardlink = join(secretDirectory, "agent-secrets-hardlink.json");
    await link(secretFile, hardlink);
    await assert.rejects(
      runManager(
        [
          "render",
          "--agent",
          join(directory, "CMClient/bin/cmclient-agent"),
          "--plaintext-secret-file",
          secretFile,
        ],
        { HOME: home },
      ),
      /LAUNCHD_SECRET_FILE_INVALID/,
    );
    await rm(directory, { recursive: true, force: true });
  },
);

launchdTest("launchd manager rejects unsafe executable paths", async () => {
  const directory = await mkdtemp(join(tmpdir(), "cmclient-launchd-invalid-"));
  await assert.rejects(
    runManager(["render", "--agent", join(directory, "agent|unsafe")]),
    /LAUNCHD_PATH_INVALID/,
  );
  await rm(directory, { recursive: true, force: true });
});

launchdTest("launchd logs tail only bounded application JSONL", async () => {
  const directory = await mkdtemp(join(tmpdir(), "cmclient-launchd-logs-"));
  const home = join(directory, "home");
  const logDir = join(directory, "logs");

  try {
    await mkdir(home, { recursive: true });
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

    const { stdout } = await runManager(
      ["logs", "--log-dir", logDir, "--lines", "1"],
      { HOME: home },
    );
    assert.match(stdout, /AGENT_CURRENT/);
    assert.match(stdout, /GATEWAY_CURRENT/);
    assert.doesNotMatch(stdout, /DAILY_OLD|LEGACY/);

    await rm(join(logDir, "agent.jsonl.2026-07-21"));
    await rm(join(logDir, "agent.jsonl.2026-07-22"));
    await rm(join(logDir, "gateway.jsonl.2026-07-22"));
    const { stdout: legacyOutput } = await runManager(
      ["logs", "--log-dir", logDir, "--lines", "1"],
      { HOME: home },
    );
    assert.match(legacyOutput, /AGENT_LEGACY/);
    assert.match(legacyOutput, /GATEWAY_LEGACY/);
    await rm(join(logDir, "agent.jsonl"));
    await rm(join(logDir, "gateway.jsonl"));
    await writeFile(join(logDir, "agent.jsonl.2026-99-99"), "invalid\n");
    await assert.rejects(
      runManager(["logs", "--log-dir", logDir], { HOME: home }),
      /LAUNCHD_LOG_FILE_INVALID/,
    );
    await rm(join(logDir, "agent.jsonl.2026-99-99"));
    await assert.rejects(
      runManager(["logs", "--log-dir", logDir], { HOME: home }),
      /LAUNCHD_LOGS_UNAVAILABLE/,
    );
    await assert.rejects(
      runManager(["logs", "--log-dir", logDir, "--lines", "10001"], {
        HOME: home,
      }),
      /LAUNCHD_LOG_LINES_INVALID/,
    );

    const externalLog = join(directory, "external.jsonl");
    await writeFile(externalLog, '{"code":"MUST_NOT_BE_READ"}\n');
    await symlink(externalLog, join(logDir, "agent.jsonl.2026-07-22"));
    await assert.rejects(
      runManager(["logs", "--log-dir", logDir], { HOME: home }),
      /LAUNCHD_LOG_FILE_INVALID/,
    );
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
