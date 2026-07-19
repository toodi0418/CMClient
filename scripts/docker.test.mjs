import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

const repositoryRoot = new URL("../", import.meta.url);

test("Docker deployment uses the CMClient runtime and mandatory restrictions", async () => {
  const [dockerfile, dockerignore, compose, entrypoint, runtime, smoke] =
    await Promise.all([
      readFile(new URL("Dockerfile", repositoryRoot), "utf8"),
      readFile(new URL(".dockerignore", repositoryRoot), "utf8"),
      readFile(new URL("docker-compose.yml", repositoryRoot), "utf8"),
      readFile(
        new URL("scripts/container-entrypoint.mjs", repositoryRoot),
        "utf8",
      ),
      readFile(
        new URL("scripts/container-runtime.mjs", repositoryRoot),
        "utf8",
      ),
      readFile(new URL("scripts/docker-smoke.sh", repositoryRoot), "utf8"),
    ]);

  assert.match(
    dockerfile,
    /pnpm --filter @cmclient\/gateway deploy --prod --frozen-lockfile \/opt\/cmclient\/gateway/,
  );
  assert.doesNotMatch(dockerfile, /--legacy/);
  assert.match(dockerfile, /USER cmclient:cmclient/);
  assert.match(dockerfile, /workspace\/proto \/app\/proto/);
  assert.match(dockerfile, /CMD \["gateway"\]/);
  assert.match(entrypoint, /env: gatewayEnvironment\(\)/);
  assert.match(runtime, /CMCLIENT_DEPLOYMENT_MODE: "docker"/);
  assert.match(compose, /CMCLIENT_DEPLOYMENT_MODE: docker/);
  assert.match(compose, /CMCLIENT_GATEWAY_HOST: 0\.0\.0\.0/);
  assert.match(compose, /cmclient-internal:\n {4}internal: true/);
  assert.match(compose, /cmclient-web:\n {4}internal: true/);
  const gatewayService = composeService(compose, "gateway");
  const webService = composeService(compose, "web");
  const ingressService = composeService(compose, "ingress");
  assert.match(
    webService,
    /networks:\n {6}- cmclient-internal\n {6}- cmclient-web/,
  );
  assert.doesNotMatch(webService, /cmclient-ingress|\n {4}ports:/);
  assert.match(ingressService, /command: \["ingress"\]/);
  assert.match(runtime, /DEFAULT_INGRESS_UPSTREAM = "http:\/\/web:8080"/);
  assert.doesNotMatch(entrypoint, /CMCLIENT_INGRESS_UPSTREAM/);
  assert.match(
    ingressService,
    /networks:\n {6}- cmclient-web\n {6}- cmclient-ingress/,
  );
  assert.doesNotMatch(
    ingressService,
    /cmclient-internal|cmclient-egress|\n {4}(?:volumes|secrets):/,
  );
  assert.match(gatewayService, /cmclient-internal/);
  assert.doesNotMatch(gatewayService, /cmclient-web|cmclient-ingress/);
  assert.equal((compose.match(/read_only: true/g) || []).length, 3);
  assert.equal((compose.match(/no-new-privileges:true/g) || []).length, 3);
  assert.equal((compose.match(/cap_drop:\n {6}- ALL/g) || []).length, 3);
  assert.equal((compose.match(/privileged: false/g) || []).length, 3);
  assert.equal((compose.match(/^ {4}ports:/gm) || []).length, 1);
  assert.match(ingressService, /^ {4}ports:/m);
  assert.match(compose, /127\.0\.0\.1:\$\{CMCLIENT_WEB_PORT:-8080\}:8080/);
  assert.doesNotMatch(compose, /docker\.sock|network_mode: host|devices:/);
  assert.doesNotMatch(
    dockerignore,
    /^(?:\/?scripts\/?|\/?proto\/?|pnpm-lock\.yaml|pnpm-workspace\.yaml|\*\.ya?ml)$/m,
  );
  assert.doesNotMatch(
    `${dockerfile}\n${compose}\n${entrypoint}\n${runtime}`,
    /TENMAN|TMAG_|AUTO_UPDATE|git clone|callmesh-client/i,
  );
  assert.match(smoke, /--force-recreate/);
  assert.match(smoke, /docker-compose\.yml/);
  assert.match(smoke, /port ingress 8080/);
  assert.match(smoke, /assert_network_topology web 2 2/);
  assert.match(smoke, /assert_network_topology ingress 2 1/);
  assert.match(smoke, /DOCKER_INGRESS_GATEWAY_ISOLATION_FAILED/);
  assert.match(smoke, /ps --all/);
  assert.match(smoke, /packaging-lifecycle-sentinel/);
});

function composeService(compose, name) {
  const servicesEnd = compose.indexOf("\nvolumes:");
  const services = compose.slice(0, servicesEnd);
  const marker = `  ${name}:\n`;
  const start = services.indexOf(marker);
  assert.notEqual(start, -1, `Docker service ${name} must exist`);
  const rest = services.slice(start + marker.length);
  const next = rest.search(/^ {2}[a-zA-Z0-9_-]+:\n/m);
  return next < 0
    ? services.slice(start)
    : services.slice(start, start + marker.length + next);
}
