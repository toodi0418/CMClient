import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

const repositoryRoot = new URL("../", import.meta.url);
const removedLegacyPattern = new RegExp(
  ["TEN", "MAN|TM", "AG_|AUTO_UPDATE|git clone|callmesh-client"].join(""),
  "i",
);

test("Docker deployment uses the CMClient runtime and mandatory restrictions", async () => {
  const [
    dockerfile,
    dockerignore,
    compose,
    entrypoint,
    runtime,
    smoke,
    releaseWorkflow,
  ] = await Promise.all([
    readFile(new URL("Dockerfile", repositoryRoot), "utf8"),
    readFile(new URL(".dockerignore", repositoryRoot), "utf8"),
    readFile(new URL("docker-compose.yml", repositoryRoot), "utf8"),
    readFile(
      new URL("scripts/container-entrypoint.mjs", repositoryRoot),
      "utf8",
    ),
    readFile(new URL("scripts/container-runtime.mjs", repositoryRoot), "utf8"),
    readFile(new URL("scripts/docker-smoke.sh", repositoryRoot), "utf8"),
    readFile(
      new URL(".github/workflows/release-build.yml", repositoryRoot),
      "utf8",
    ),
  ]);

  assert.match(
    dockerfile,
    /pnpm --filter @cmclient\/gateway deploy --prod --frozen-lockfile \/opt\/cmclient\/gateway/,
  );
  assert.match(
    dockerfile,
    /^# syntax=docker\/dockerfile:1@sha256:[a-f0-9]{64}$/m,
  );
  assert.doesNotMatch(dockerfile, /--legacy/);
  assert.match(dockerfile, /USER cmclient:cmclient/);
  assert.match(dockerfile, /workspace\/proto \/app\/proto/);
  assert.match(dockerfile, /CMD \["gateway"\]/);
  assert.match(
    dockerfile,
    /CMCLIENT_BUILD_VERSION=\$\{CMCLIENT_BUILD_VERSION\}/,
  );
  assert.match(dockerfile, /CMCLIENT_BUILD_COMMIT=\$\{CMCLIENT_BUILD_COMMIT\}/);
  assert.equal(
    (
      dockerfile.match(
        /node:22-bookworm-slim@sha256:6c74791e557ce11fc957704f6d4fe134a7bc8d6f5ca4403205b2966bd488f6b3/g,
      ) || []
    ).length,
    2,
  );
  assert.match(entrypoint, /env: gatewayEnvironment\(\)/);
  assert.match(runtime, /CMCLIENT_DEPLOYMENT_MODE: "docker"/);
  assert.match(compose, /CMCLIENT_DEPLOYMENT_MODE: docker/);
  assert.match(compose, /CMCLIENT_GATEWAY_HOST: 0\.0\.0\.0/);
  assert.match(compose, /cmclient-internal:\n {4}internal: true/);
  assert.match(compose, /cmclient-web:\n {4}internal: true/);
  const gatewayService = composeService(compose, "gateway");
  const webService = composeService(compose, "web");
  const ingressService = composeService(compose, "ingress");
  for (const service of [gatewayService, webService, ingressService]) {
    assert.match(service, /^ {4}init: true$/m);
  }
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
    removedLegacyPattern,
  );
  assert.match(smoke, /--force-recreate/);
  assert.match(smoke, /CMCLIENT_SMOKE_PREBUILT/);
  assert.match(smoke, /up\+=\(--no-build\)/);
  assert.match(smoke, /recreate\+=\(--no-build\)/);
  assert.match(smoke, /api\/v1\/system\/version/);
  assert.match(smoke, /CMCLIENT_EXPECTED_COMMIT/);
  assert.match(smoke, /docker-compose\.yml/);
  assert.match(smoke, /port ingress 8080/);
  assert.match(smoke, /assert_network_topology web 2 2/);
  assert.match(smoke, /assert_network_topology ingress 2 1/);
  assert.match(smoke, /DOCKER_INGRESS_GATEWAY_ISOLATION_FAILED/);
  assert.match(smoke, /ps --all/);
  assert.match(smoke, /packaging-lifecycle-sentinel/);
  assert.match(
    releaseWorkflow,
    /os: ubuntu-22\.04\n\s+target: linux-x86_64\n\s+platform: linux\/amd64/,
  );
  assert.match(
    releaseWorkflow,
    /os: ubuntu-22\.04-arm\n\s+target: linux-aarch64\n\s+platform: linux\/arm64/,
  );
  assert.match(releaseWorkflow, /--platform "\$\{\{ matrix\.platform \}\}"/);
  assert.match(releaseWorkflow, /docker buildx create \\/);
  assert.match(releaseWorkflow, /--driver docker-container \\/);
  assert.match(releaseWorkflow, /docker buildx inspect --bootstrap/);
  assert.match(releaseWorkflow, /docker buildx rm --force/);
  assert.match(
    releaseWorkflow,
    /--build-arg "SOURCE_DATE_EPOCH=\$source_epoch"/,
  );
  assert.match(
    releaseWorkflow,
    /--build-arg "CMCLIENT_BUILD_VERSION=\$version"/,
  );
  assert.match(
    releaseWorkflow,
    /--build-arg "CMCLIENT_BUILD_COMMIT=\$GITHUB_SHA"/,
  );
  assert.match(
    releaseWorkflow,
    /--build-arg "CMCLIENT_BUILD_CHANNEL=\$build_channel"/,
  );
  assert.match(
    releaseWorkflow,
    /--output "type=oci,dest=\$archive,rewrite-timestamp=true"/,
  );
  assert.match(
    releaseWorkflow,
    /org\.opencontainers\.image\.revision=\$GITHUB_SHA/,
  );
  assert.match(releaseWorkflow, /release-supply-chain\.mjs stage-docker/);
  assert.match(releaseWorkflow, /--target "\$\{\{ matrix\.target \}\}"/);
  assert.match(releaseWorkflow, /release-supply-chain\.mjs include-docker/);
  assert.match(releaseWorkflow, /--compose docker-compose\.yml/);
  assert.match(
    releaseWorkflow,
    /--file "release-dist\/cmclient-docker-compose-\$version\.yml" \\\n\s+config --quiet/,
  );
  assert.match(releaseWorkflow, /pattern: cmclient-docker-oci-\*/);
  assert.match(
    releaseWorkflow,
    /skopeo copy \\\n\s+--format v2s2 \\\n\s+"oci-archive:\$archive" \\\n\s+"docker-archive:\$docker_archive:\$release_image"/,
  );
  assert.match(releaseWorkflow, /docker load --input "\$docker_archive"/);
  assert.match(
    releaseWorkflow,
    /docker image inspect "\$release_image" >\/dev\/null/,
  );
  assert.doesNotMatch(releaseWorkflow, /docker-daemon:/);
  assert.match(releaseWorkflow, /CMCLIENT_SMOKE_PREBUILT=1/);
  assert.match(releaseWorkflow, /oci-archive:release-dist\/cmclient-docker/);
  assert.match(releaseWorkflow, /for target in linux-x86_64 linux-aarch64/);
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
