#!/usr/bin/env bash
set -euo pipefail

bundle="${1:?bundle path is required}"
target="${2:?target is required}"
bundle="$(cd "$bundle" && pwd)"

suffix=""
if [[ "$target" == windows-* ]]; then
  suffix=".exe"
fi

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
cleanup() {
  if [[ -n "$agent_pid" ]]; then
    kill "$agent_pid" >/dev/null 2>&1 || true
    wait "$agent_pid" >/dev/null 2>&1 || true
  fi
  rm -rf "$runtime"
}
trap cleanup EXIT

gateway_port="$(node --input-type=module --eval '
  import net from "node:net";
  const server = net.createServer();
  server.listen(0, "127.0.0.1", () => {
    console.log(server.address().port);
    server.close();
  });
')"
mkdir -p "$runtime/data" "$runtime/config" "$runtime/cache" "$runtime/logs"
printf '[agent]\ngateway_port = %s\nmanagement_web_enabled = true\n' "$gateway_port" >"$runtime/agent.toml"

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
  if node --input-type=module --eval '
    const root = await fetch("http://127.0.0.1:7080/");
    const health = await fetch("http://127.0.0.1:7080/api/v1/system/health");
    if (!root.ok || !(await root.text()).includes("id=\"app\"")) process.exit(1);
    if (!health.ok || (await health.json()).status !== "ok") process.exit(1);
  ' >/dev/null 2>&1; then
    application_ready=true
    break
  fi
  sleep 0.2
done
[[ "$application_ready" == true ]] || { cat "$runtime/agent.log" >&2; exit 1; }

"$cli" --quiet stop
