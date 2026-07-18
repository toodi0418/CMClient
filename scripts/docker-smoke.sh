#!/usr/bin/env bash
set -euo pipefail

project="cmclient-smoke-$$"
port="${CMCLIENT_SMOKE_PORT:-18080}"
image="cmclient:smoke-${project}"
compose=(docker compose --project-name "${project}" --file docker-compose.yml)

cleanup() {
  "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

CMCLIENT_IMAGE="${image}" CMCLIENT_WEB_PORT="${port}" "${compose[@]}" config --quiet
CMCLIENT_IMAGE="${image}" CMCLIENT_WEB_PORT="${port}" "${compose[@]}" up --build --detach

url="http://127.0.0.1:${port}"
verify_application() {
  if CMCLIENT_SMOKE_URL="${url}" node --input-type=module --eval '
    const response = await fetch(`${process.env.CMCLIENT_SMOKE_URL}/api/v1/system/capabilities`);
    if (!response.ok) process.exit(1);
    const body = await response.json();
    const capabilities = body.capabilities;
    if (capabilities.docker.available !== true) process.exit(1);
    if (capabilities.update.reasonCode !== "CAPABILITY_UNAVAILABLE_DOCKER") process.exit(1);
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
  for _ in $(seq 1 30); do
    if verify_application; then
      return 0
    fi
    sleep 2
  done
  "${compose[@]}" logs --no-color
  return 1
}

wait_for_application
"${compose[@]}" exec --no-TTY gateway node --input-type=module --eval '
  import { writeFileSync } from "node:fs";
  writeFileSync("/var/lib/cmclient/packaging-lifecycle-sentinel", "must survive recreate");
'
CMCLIENT_IMAGE="${image}" CMCLIENT_WEB_PORT="${port}" "${compose[@]}" up --detach --force-recreate
wait_for_application
"${compose[@]}" exec --no-TTY gateway node --input-type=module --eval '
  import { readFileSync } from "node:fs";
  if (readFileSync("/var/lib/cmclient/packaging-lifecycle-sentinel", "utf8") !== "must survive recreate") {
    process.exit(1);
  }
'
