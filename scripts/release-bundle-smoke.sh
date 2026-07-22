#!/usr/bin/env bash
set -euo pipefail

bundle="${1:?bundle path is required}"
target="${2:?target is required}"
expected_version="${3:?expected version is required}"
expected_commit="${4:?expected source commit is required}"
surface="${5:-headless}"
bundle="$(cd "$bundle" && pwd)"

[[ "$expected_commit" =~ ^[a-f0-9]{40}$ ]] || {
  echo "RELEASE_SMOKE_SOURCE_COMMIT_INVALID" >&2
  exit 1
}
case "$expected_version" in
  *-dev.*) expected_channel="dev" ;;
  *-*) expected_channel="beta" ;;
  *) expected_channel="stable" ;;
esac

suffix=""
if [[ "$target" == windows-* ]]; then
  suffix=".exe"
fi

case "$surface" in
  headless|service) ;;
  desktop) ;;
  *) echo "RELEASE_SMOKE_SURFACE_INVALID" >&2; exit 1 ;;
esac

agent="$bundle/bin/cmclient-agent$suffix"
cli="$bundle/bin/cmclient$suffix"
migrate="$bundle/bin/cmclient-migrate$suffix"
for executable in "$agent" "$cli" "$migrate"; do
  [[ -f "$executable" ]] || { echo "RELEASE_SMOKE_EXECUTABLE_MISSING" >&2; exit 1; }
  if [[ "$target" != windows-* && ! -x "$executable" ]]; then
    echo "RELEASE_SMOKE_EXECUTABLE_NOT_EXECUTABLE" >&2
    exit 1
  fi
done

node_major="$(node -p 'Number(process.versions.node.split(".")[0])')"
(( node_major >= 22 )) || { echo "RELEASE_SMOKE_NODE_VERSION_UNSUPPORTED" >&2; exit 1; }

(
  cd "$bundle"
  node --input-type=module --eval '
    const schema = await import("./gateway/dist/protobuf/schema.js");
    const loaded = await schema.loadMeshtasticSchema();
    if (!loaded.fromRadio || !loaded.position) process.exit(1);
  '
)
"$migrate" --version >/dev/null

runtime="$(mktemp -d)"
agent_pid=""
desktop_pid=""
cleanup() {
  if [[ -n "$desktop_pid" ]]; then
    kill "$desktop_pid" >/dev/null 2>&1 || true
    wait "$desktop_pid" >/dev/null 2>&1 || true
  fi
  if [[ -n "$agent_pid" ]]; then
    kill "$agent_pid" >/dev/null 2>&1 || true
    wait "$agent_pid" >/dev/null 2>&1 || true
  fi
  rm -rf "$runtime"
}
trap cleanup EXIT

mkdir -p "$runtime/data" "$runtime/config" "$runtime/cache" "$runtime/logs"
printf '[agent]\nmanagement_web_enabled = true\n' >"$runtime/agent.toml"

export CMCLIENT_AGENT_CONFIG="$runtime/agent.toml"
export CMCLIENT_DATA_DIR="$runtime/data"
export CMCLIENT_CONFIG_DIR="$runtime/config"
export CMCLIENT_CACHE_DIR="$runtime/cache"
export CMCLIENT_LOG_DIR="$runtime/logs"

"$agent" --serve >"$runtime/agent.log" 2>&1 &
agent_pid="$!"

control_ready=false
for _ in $(seq 1 80); do
  if "$cli" --quiet status >/dev/null 2>&1; then
    control_ready=true
    break
  fi
  sleep 0.1
done
[[ "$control_ready" == true ]] || { cat "$runtime/agent.log" >&2; exit 1; }
"$cli" --quiet start

application_ready=false
for _ in $(seq 1 100); do
  if EXPECTED_VERSION="$expected_version" EXPECTED_COMMIT="$expected_commit" EXPECTED_CHANNEL="$expected_channel" node --input-type=module --eval '
    const root = await fetch("http://127.0.0.1:7080/");
    const health = await fetch("http://127.0.0.1:7080/api/v1/system/health");
    const version = await fetch("http://127.0.0.1:7080/api/v1/system/version");
    if (!root.ok || !(await root.text()).includes("id=\"app\"")) process.exit(1);
    if (!health.ok || (await health.json()).status !== "ok") process.exit(1);
    if (!version.ok) process.exit(1);
    const build = await version.json();
    if (build.version !== process.env.EXPECTED_VERSION) process.exit(1);
    if (build.commit !== process.env.EXPECTED_COMMIT) process.exit(1);
    if (build.channel !== process.env.EXPECTED_CHANNEL) process.exit(1);
  ' >/dev/null 2>&1; then
    application_ready=true
    break
  fi
  sleep 0.2
done
[[ "$application_ready" == true ]] || { cat "$runtime/agent.log" >&2; exit 1; }

if [[ "$surface" == "desktop" ]]; then
  desktop="$bundle/bin/cmclient-desktop$suffix"
  [[ -f "$desktop" ]] || { echo "RELEASE_SMOKE_DESKTOP_MISSING" >&2; exit 1; }
  if [[ "$target" != windows-* && ! -x "$desktop" ]]; then
    echo "RELEASE_SMOKE_DESKTOP_NOT_EXECUTABLE" >&2
    exit 1
  fi
  if [[ "$target" == linux-* ]]; then
    command -v dbus-run-session >/dev/null || { echo "RELEASE_SMOKE_DBUS_MISSING" >&2; exit 1; }
    command -v xvfb-run >/dev/null || { echo "RELEASE_SMOKE_XVFB_MISSING" >&2; exit 1; }
    WEBKIT_DISABLE_COMPOSITING_MODE=1 dbus-run-session -- \
      xvfb-run -a "$desktop" >"$runtime/desktop.log" 2>&1 &
  else
    "$desktop" >"$runtime/desktop.log" 2>&1 &
  fi
  desktop_pid="$!"
  desktop_running=false
  for _ in $(seq 1 30); do
    if kill -0 "$desktop_pid" >/dev/null 2>&1; then
      desktop_running=true
      break
    fi
    sleep 0.1
  done
  [[ "$desktop_running" == true ]] || { cat "$runtime/desktop.log" >&2; exit 1; }
  sleep 2
  kill -0 "$desktop_pid" >/dev/null 2>&1 || {
    cat "$runtime/desktop.log" >&2
    exit 1
  }
  kill "$desktop_pid" >/dev/null 2>&1 || true
  wait "$desktop_pid" >/dev/null 2>&1 || true
  desktop_pid=""
fi

"$cli" --quiet stop
