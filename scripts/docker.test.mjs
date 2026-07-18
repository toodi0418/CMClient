import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

const repositoryRoot = new URL("../", import.meta.url);

test("Docker deployment uses the CMClient runtime and mandatory restrictions", async () => {
  const [dockerfile, compose, entrypoint, runtime] = await Promise.all([
    readFile(new URL("Dockerfile", repositoryRoot), "utf8"),
    readFile(new URL("docker-compose.yml", repositoryRoot), "utf8"),
    readFile(new URL("scripts/container-entrypoint.mjs", repositoryRoot), "utf8"),
    readFile(new URL("scripts/container-runtime.mjs", repositoryRoot), "utf8"),
  ]);

  assert.match(
    dockerfile,
    /pnpm --filter @cmclient\/gateway deploy --legacy --prod/,
  );
  assert.match(dockerfile, /USER cmclient:cmclient/);
  assert.match(dockerfile, /CMD \["gateway"\]/);
  assert.match(entrypoint, /env: gatewayEnvironment\(\)/);
  assert.match(runtime, /CMCLIENT_DEPLOYMENT_MODE: "docker"/);
  assert.match(compose, /CMCLIENT_DEPLOYMENT_MODE: docker/);
  assert.match(compose, /CMCLIENT_GATEWAY_HOST: 0\.0\.0\.0/);
  assert.match(compose, /cmclient-internal:\n    internal: true/);
  assert.equal((compose.match(/read_only: true/g) || []).length, 2);
  assert.equal((compose.match(/no-new-privileges:true/g) || []).length, 2);
  assert.equal((compose.match(/cap_drop:\n      - ALL/g) || []).length, 2);
  assert.equal((compose.match(/privileged: false/g) || []).length, 2);
  assert.equal((compose.match(/^    ports:/gm) || []).length, 1);
  assert.match(compose, /127\.0\.0\.1:\$\{CMCLIENT_WEB_PORT:-8080\}:8080/);
  assert.doesNotMatch(compose, /docker\.sock|network_mode: host|devices:/);
  assert.doesNotMatch(
    `${dockerfile}\n${compose}\n${entrypoint}\n${runtime}`,
    /TENMAN|TMAG_|AUTO_UPDATE|git clone|callmesh-client/i,
  );
});
