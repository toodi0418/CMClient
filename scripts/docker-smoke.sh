#!/usr/bin/env bash
set -euo pipefail

project="cmclient-smoke-$$"
port="${CMCLIENT_SMOKE_PORT:-18080}"
image="${CMCLIENT_IMAGE:-cmclient:smoke-${project}}"
compose=(docker compose --project-name "${project}" --file docker-compose.yml)

cleanup() {
  "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

CMCLIENT_IMAGE="${image}" CMCLIENT_WEB_PORT="${port}" "${compose[@]}" config --quiet
up=(up --detach)
if [[ "${CMCLIENT_SMOKE_PREBUILT:-0}" != "1" ]]; then
  up+=(--build)
else
  up+=(--no-build)
fi
CMCLIENT_IMAGE="${image}" CMCLIENT_WEB_PORT="${port}" "${compose[@]}" "${up[@]}"

published_port="$("${compose[@]}" port ingress 8080)"
if [[ ! "${published_port}" =~ ^127\.0\.0\.1:[0-9]+$ ]]; then
  "${compose[@]}" ps --all
  echo "DOCKER_WEB_PUBLISHED_PORT_INVALID" >&2
  exit 1
fi
url="http://${published_port}"

service_networks() {
  local service="$1"
  local container
  container="$("${compose[@]}" ps --quiet "${service}")"
  docker inspect \
    --format '{{range $name, $network := .NetworkSettings.Networks}}{{printf "%s " $name}}{{end}}' \
    "${container}"
}

assert_network_topology() {
  local service="$1"
  local expected_count="$2"
  local expected_internal="$3"
  local count=0
  local internal_count=0
  local network
  for network in $(service_networks "${service}"); do
    count=$((count + 1))
    if [[ "$(docker network inspect --format '{{.Internal}}' "${network}")" == "true" ]]; then
      internal_count=$((internal_count + 1))
    fi
  done
  if [[ "${count}" -ne "${expected_count}" || "${internal_count}" -ne "${expected_internal}" ]]; then
    echo "DOCKER_SERVICE_NETWORK_TOPOLOGY_INVALID:${service}" >&2
    exit 1
  fi
}

assert_ingress_gateway_isolation() {
  if "${compose[@]}" exec --no-TTY ingress node --input-type=module --eval '
    try {
      await fetch("http://gateway:8081/api/v1/system/health", {
        signal: AbortSignal.timeout(2_000),
      });
      process.exit(0);
    } catch {
      process.exit(1);
    }
  '; then
    echo "DOCKER_INGRESS_GATEWAY_ISOLATION_FAILED" >&2
    exit 1
  fi
}

verify_application() {
  if CMCLIENT_SMOKE_URL="${url}" node --input-type=module --eval '
    const response = await fetch(`${process.env.CMCLIENT_SMOKE_URL}/api/v1/system/capabilities`);
    if (!response.ok) process.exit(1);
    const body = await response.json();
    const capabilities = body.capabilities;
    if (capabilities.docker.available !== true) process.exit(1);
    if (capabilities.update.reasonCode !== "CAPABILITY_UNAVAILABLE_DOCKER") process.exit(1);
    if (process.env.CMCLIENT_EXPECTED_VERSION) {
      const versionResponse = await fetch(`${process.env.CMCLIENT_SMOKE_URL}/api/v1/system/version`);
      if (!versionResponse.ok) process.exit(1);
      const build = await versionResponse.json();
      if (build.version !== process.env.CMCLIENT_EXPECTED_VERSION) process.exit(1);
      if (build.commit !== process.env.CMCLIENT_EXPECTED_COMMIT) process.exit(1);
      if (build.channel !== process.env.CMCLIENT_EXPECTED_CHANNEL) process.exit(1);
    }
  '; then
    CMCLIENT_SMOKE_URL="${url}" node --input-type=module --eval '
      const response = await fetch(process.env.CMCLIENT_SMOKE_URL);
      if (!response.ok || !response.headers.get("content-type")?.includes("text/html")) process.exit(1);
    '
    return 0
  fi
  return 1
}

wait_for_application() {
  for _ in $(seq 1 45); do
    if verify_application; then
      return 0
    fi
    sleep 2
  done
  "${compose[@]}" ps --all
  "${compose[@]}" logs --no-color
  return 1
}

wait_for_application
assert_network_topology gateway 2 1
assert_network_topology web 2 2
assert_network_topology ingress 2 1
assert_ingress_gateway_isolation
"${compose[@]}" exec --no-TTY gateway node --input-type=module --eval '
  const schema = await import("/app/gateway/dist/protobuf/schema.js");
  const loaded = await schema.loadMeshtasticSchema();
  if (!loaded.fromRadio || !loaded.position) process.exit(1);
'
"${compose[@]}" exec --no-TTY gateway node --input-type=module --eval '
  import { writeFileSync } from "node:fs";
  writeFileSync("/var/lib/cmclient/packaging-lifecycle-sentinel", "must survive recreate");
'
recreate=(up --detach --force-recreate)
if [[ "${CMCLIENT_SMOKE_PREBUILT:-0}" == "1" ]]; then
  recreate+=(--no-build)
fi
CMCLIENT_IMAGE="${image}" CMCLIENT_WEB_PORT="${port}" "${compose[@]}" "${recreate[@]}"
wait_for_application
assert_network_topology web 2 2
assert_ingress_gateway_isolation
"${compose[@]}" exec --no-TTY gateway node --input-type=module --eval '
  import { readFileSync } from "node:fs";
  if (readFileSync("/var/lib/cmclient/packaging-lifecycle-sentinel", "utf8") !== "must survive recreate") {
    process.exit(1);
  }
'
