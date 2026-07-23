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

function escaped(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

launchdTest(
  "launchd manager uses one user-home root and retains it on uninstall",
  async () => {
    const directory = await mkdtemp(join(tmpdir(), "cmclient-launchd-"));
    const agentV1 = join(directory, "releases/v1/bin/cmclient-agent");
    const agentV2 = join(directory, "releases/v2/bin/cmclient-agent");
    const home = join(directory, "home");
    const runtimeRoot = join(home, ".cmclient");
    const plist = join(home, "Library/LaunchAgents/io.cmclient.agent.plist");
    const launchctl = join(directory, "launchctl");
    const plutil = join(directory, "plutil");
    const calls = join(directory, "launchctl-calls");

    try {
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

      const shared = ["--agent", agentV1, "--plist", plist];
      const environment = {
        HOME: home,
        CMCLIENT_LAUNCHCTL: launchctl,
        CMCLIENT_PLUTIL: plutil,
      };

      await runManager(["install", ...shared], environment);
      const contents = await readFile(plist, "utf8");
      assert.match(contents, /<string>io\.cmclient\.agent<\/string>/);
      assert.match(
        contents,
        new RegExp(`<string>${escaped(agentV1)}<\\/string>`),
      );
      assert.match(contents, new RegExp(`<string>${escaped(home)}<\\/string>`));
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
      assert.doesNotMatch(
        contents,
        /CMCLIENT_(?:AGENT_CONFIG|DATA_DIR|CONFIG_DIR|CACHE_DIR|LOG_DIR|PLAINTEXT_SECRET_FILE)/,
      );
      assert.doesNotMatch(contents, /CALLMESH|API_KEY|PASSCODE|TOKEN/);

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
        /bootstrap gui\/\d+ .*io\.cmclient\.agent\.plist/,
      );

      await writeFile(
        join(runtimeRoot, "retained-state"),
        "must survive uninstall",
      );
      const upgraded = shared.map((value) =>
        value === agentV1 ? agentV2 : value,
      );
      await runManager(["install", ...upgraded], environment);
      const upgradedContents = await readFile(plist, "utf8");
      assert.match(
        upgradedContents,
        new RegExp(`<string>${escaped(agentV2)}<\\/string>`),
      );
      assert.equal(
        await readFile(join(runtimeRoot, "retained-state"), "utf8"),
        "must survive uninstall",
      );

      await runManager(["uninstall", ...upgraded], environment);
      await assert.rejects(readFile(plist, "utf8"));
      assert.equal(
        await readFile(join(runtimeRoot, "retained-state"), "utf8"),
        "must survive uninstall",
      );
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  },
);

launchdTest(
  "launchd renders only HOME and rejects legacy path selectors",
  async () => {
    const directory = await mkdtemp(join(tmpdir(), "cmclient-launchd-root-"));
    const home = join(directory, "home");
    try {
      await mkdir(home, { recursive: true });
      const { stdout } = await runManager(
        ["render", "--agent", join(directory, "CMClient/bin/cmclient-agent")],
        { HOME: home },
      );
      assert.match(stdout, /<key>HOME<\/key>/);
      assert.match(stdout, new RegExp(`<string>${escaped(home)}<\\/string>`));
      assert.doesNotMatch(stdout, /CMCLIENT_/);
      assert.doesNotMatch(stdout, /secrets\.json|credential|secret/i);

      await assert.rejects(
        runManager(
          [
            "render",
            "--agent",
            join(directory, "CMClient/bin/cmclient-agent"),
            "--plaintext-secret-file",
            join(home, ".cmclient/secrets.json"),
          ],
          { HOME: home },
        ),
        /LAUNCHD_USAGE_INVALID_ARGUMENT/,
      );

      const safeAgent = join(directory, "CMClient/bin/cmclient-agent");
      const externalRoot = join(directory, "external-root");
      await mkdir(join(directory, "CMClient/bin"), { recursive: true });
      await writeFile(safeAgent, "#!/usr/bin/env sh\nexit 0\n");
      await chmod(safeAgent, 0o755);
      await mkdir(externalRoot);
      await symlink(externalRoot, join(home, ".cmclient"));
      await assert.rejects(
        runManager(["install", "--agent", safeAgent], { HOME: home }),
        /LAUNCHD_RUNTIME_DIRECTORY_INVALID/,
      );
      await assert.rejects(
        runManager(
          [
            "render",
            "--agent",
            join(directory, "CMClient/bin/cmclient-agent"),
            "--data-dir",
            join(directory, "foreign"),
          ],
          { HOME: home },
        ),
        /LAUNCHD_USAGE_INVALID_ARGUMENT/,
      );
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  },
);

launchdTest("launchd manager rejects unsafe executable paths", async () => {
  const directory = await mkdtemp(join(tmpdir(), "cmclient-launchd-invalid-"));
  try {
    await assert.rejects(
      runManager(["render", "--agent", join(directory, "agent|unsafe")], {
        HOME: join(directory, "home"),
      }),
      /LAUNCHD_PATH_INVALID/,
    );
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

launchdTest("launchd logs tail only bounded canonical JSONL", async () => {
  const directory = await mkdtemp(join(tmpdir(), "cmclient-launchd-logs-"));
  const home = join(directory, "home");
  const logDir = join(home, ".cmclient/logs");

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

    const { stdout } = await runManager(["logs", "--lines", "1"], {
      HOME: home,
    });
    assert.match(stdout, /AGENT_CURRENT/);
    assert.match(stdout, /GATEWAY_CURRENT/);
    assert.doesNotMatch(stdout, /DAILY_OLD|LEGACY/);

    await rm(join(logDir, "agent.jsonl.2026-07-21"));
    await rm(join(logDir, "agent.jsonl.2026-07-22"));
    await rm(join(logDir, "gateway.jsonl.2026-07-22"));
    const { stdout: legacyOutput } = await runManager(
      ["logs", "--lines", "1"],
      {
        HOME: home,
      },
    );
    assert.match(legacyOutput, /AGENT_LEGACY/);
    assert.match(legacyOutput, /GATEWAY_LEGACY/);

    await rm(join(logDir, "agent.jsonl"));
    await rm(join(logDir, "gateway.jsonl"));
    await writeFile(join(logDir, "agent.jsonl.2026-99-99"), "invalid\n");
    await assert.rejects(
      runManager(["logs"], { HOME: home }),
      /LAUNCHD_LOG_FILE_INVALID/,
    );
    await rm(join(logDir, "agent.jsonl.2026-99-99"));
    await assert.rejects(
      runManager(["logs"], { HOME: home }),
      /LAUNCHD_LOGS_UNAVAILABLE/,
    );
    await assert.rejects(
      runManager(["logs", "--lines", "10001"], { HOME: home }),
      /LAUNCHD_LOG_LINES_INVALID/,
    );

    const externalLog = join(directory, "external.jsonl");
    await writeFile(externalLog, '{"code":"MUST_NOT_BE_READ"}\n');
    await symlink(externalLog, join(logDir, "agent.jsonl.2026-07-22"));
    await assert.rejects(
      runManager(["logs"], { HOME: home }),
      /LAUNCHD_LOG_FILE_INVALID/,
    );
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
