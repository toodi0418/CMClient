#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="cmclient-agent.service"
EXPECTED_SYSTEMD_VERSION="249"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MANAGER="$SCRIPT_DIR/cmclient-systemd.sh"

fail() {
  printf '%s\n' "$1" >&2
  exit 1
}

if [[ "$(uname -s)" != "Linux" ]]; then
  fail "SYSTEMD_SMOKE_PLATFORM_UNSUPPORTED"
fi
if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
  fail "SYSTEMD_SMOKE_PRIVILEGE_REQUIRED"
fi
if [[ "${CI:-}" != "true" && "${CMCLIENT_SYSTEMD_SMOKE_ALLOW_LOCAL:-0}" != "1" ]]; then
  fail "SYSTEMD_SMOKE_CI_REQUIRED"
fi
if [[ $# -ne 2 ]]; then
  fail "SYSTEMD_SMOKE_USAGE_INVALID"
fi

AGENT_SOURCE="$(realpath "$1")"
CLI_SOURCE="$(realpath "$2")"
if [[ ! -x "$AGENT_SOURCE" || ! -x "$CLI_SOURCE" ]]; then
  fail "SYSTEMD_SMOKE_BINARY_NOT_EXECUTABLE"
fi

systemd_version="$(systemctl --version | awk 'NR == 1 { print $2 }')"
if [[ "$systemd_version" != "$EXPECTED_SYSTEMD_VERSION" ]]; then
  fail "SYSTEMD_SMOKE_VERSION_UNEXPECTED"
fi
system_state="$(systemctl is-system-running 2>/dev/null || true)"
if [[ "$system_state" != "running" && "$system_state" != "degraded" ]]; then
  fail "SYSTEMD_SMOKE_MANAGER_UNAVAILABLE"
fi

existing_load_state="$(systemctl show "$SERVICE_NAME" --property=LoadState --value 2>/dev/null || true)"
if [[ "$existing_load_state" != "not-found" || -e "/etc/systemd/system/$SERVICE_NAME" ]]; then
  fail "SYSTEMD_SMOKE_SERVICE_ALREADY_EXISTS"
fi

SERVICE_USER="${CMCLIENT_SMOKE_SERVICE_USER:-${SUDO_USER:-}}"
if [[ ! "$SERVICE_USER" =~ ^[a-z_][a-z0-9_-]{0,63}$ ]] || ! id "$SERVICE_USER" >/dev/null 2>&1; then
  fail "SYSTEMD_SMOKE_SERVICE_USER_INVALID"
fi
SERVICE_GROUP="$(id -gn "$SERVICE_USER")"
if [[ ! "$SERVICE_GROUP" =~ ^[a-z_][a-z0-9_-]{0,63}$ ]]; then
  fail "SYSTEMD_SMOKE_SERVICE_GROUP_INVALID"
fi

run_id="${GITHUB_RUN_ID:-local}"
run_attempt="${GITHUB_RUN_ATTEMPT:-1}"
if [[ ! "$run_id" =~ ^(local|[1-9][0-9]*)$ || ! "$run_attempt" =~ ^[1-9][0-9]*$ ]]; then
  fail "SYSTEMD_SMOKE_RUN_ID_INVALID"
fi
run_label="$run_id-$run_attempt-$$"
INSTALL_ROOT="/opt/cmclient-systemd-smoke-$run_label"
CONFIG_DIR="/etc/cmclient-systemd-smoke-$run_label"
DATA_DIR="/var/lib/cmclient-systemd-smoke-$run_label"
CACHE_DIR="/var/cache/cmclient-systemd-smoke-$run_label"
LOG_DIR="/var/log/cmclient-systemd-smoke-$run_label"
AGENT_BINARY="$INSTALL_ROOT/bin/cmclient-agent"
CLI_BINARY="$INSTALL_ROOT/bin/cmclient"
CONTROL_SOCKET="$DATA_DIR/control.sock"
SECRET_STORE_KEY="$CONFIG_DIR/secret-store.key"
CIPHERTEXT="$DATA_DIR/secrets/callmesh-api-key.secret"

manager_arguments=(
  --agent "$AGENT_BINARY"
  --config-dir "$CONFIG_DIR"
  --data-dir "$DATA_DIR"
  --cache-dir "$CACHE_DIR"
  --log-dir "$LOG_DIR"
  --service-user "$SERVICE_USER"
  --service-group "$SERVICE_GROUP"
)

cleanup() {
  local status=$?
  trap - EXIT
  set +e
  if [[ "$status" -ne 0 ]]; then
    systemctl status "$SERVICE_NAME" --no-pager --full >&2
    journalctl --unit "$SERVICE_NAME" --no-pager --lines 100 >&2
  fi
  bash "$MANAGER" uninstall "${manager_arguments[@]}" >/dev/null 2>&1
  systemctl reset-failed "$SERVICE_NAME" >/dev/null 2>&1
  rm -rf "$INSTALL_ROOT" "$CONFIG_DIR" "$DATA_DIR" "$CACHE_DIR" "$LOG_DIR"
  exit "$status"
}
trap cleanup EXIT

install -d -m 0755 "$INSTALL_ROOT/bin"
install -m 0755 "$AGENT_SOURCE" "$AGENT_BINARY"
install -m 0755 "$CLI_SOURCE" "$CLI_BINARY"
install -d -m 0750 -o root -g "$SERVICE_GROUP" "$CONFIG_DIR"
printf '%s\n' \
  '[agent]' \
  'gateway_command = ["/usr/bin/sleep", "3600"]' \
  'gateway_port = 4810' \
  'management_web_enabled = false' \
  '' \
  '[callmesh]' \
  'url = "https://callmesh.invalid"' \
  >"$CONFIG_DIR/agent.toml"
chown root:"$SERVICE_GROUP" "$CONFIG_DIR/agent.toml"
chmod 0640 "$CONFIG_DIR/agent.toml"

bash "$MANAGER" install "${manager_arguments[@]}"
systemd-analyze verify "/etc/systemd/system/$SERVICE_NAME"

wait_for_control_socket() {
  local attempt
  for attempt in $(seq 1 30); do
    if systemctl is-active --quiet "$SERVICE_NAME" && [[ -S "$CONTROL_SOCKET" ]]; then
      return 0
    fi
    sleep 1
  done
  fail "SYSTEMD_SMOKE_SERVICE_START_FAILED"
}

assert_control_status() {
  local attempt status_json
  for attempt in $(seq 1 15); do
    if status_json="$("$CLI_BINARY" --json --endpoint "unix://$CONTROL_SOCKET" status 2>/dev/null)" \
      && [[ "$status_json" == *'"agent":"running"'* ]] \
      && [[ "$status_json" == *'"managementWeb":"disabled"'* ]]; then
      return 0
    fi
    sleep 1
  done
  fail "SYSTEMD_SMOKE_CONTROL_ENDPOINT_FAILED"
}

wait_for_control_socket
if [[ "$(stat -c '%a' "$CONTROL_SOCKET")" != "600" ]]; then
  fail "SYSTEMD_SMOKE_CONTROL_SOCKET_MODE_INVALID"
fi
if [[ "$(stat -c '%U:%G' "$SECRET_STORE_KEY")" != "root:root" \
  || "$(stat -c '%a' "$SECRET_STORE_KEY")" != "600" \
  || "$(wc -c <"$SECRET_STORE_KEY")" != "32" ]]; then
  fail "SYSTEMD_SMOKE_WRAPPING_KEY_INVALID"
fi

main_pid="$(systemctl show "$SERVICE_NAME" --property=MainPID --value)"
if [[ ! "$main_pid" =~ ^[1-9][0-9]*$ ]]; then
  fail "SYSTEMD_SMOKE_MAIN_PID_INVALID"
fi
credentials_directory="$(tr '\0' '\n' <"/proc/$main_pid/environ" | sed -n 's/^CREDENTIALS_DIRECTORY=//p')"
if [[ ! "$credentials_directory" =~ ^/run/credentials/ \
  || ! -f "$credentials_directory/cmclient-secret-store-key" \
  || "$(wc -c <"$credentials_directory/cmclient-secret-store-key")" != "32" ]]; then
  fail "SYSTEMD_SMOKE_CREDENTIAL_NOT_LOADED"
fi
assert_control_status

secret_value="cmclient-systemd-smoke-$run_label"
printf '%s\n' "$secret_value" \
  | "$CLI_BINARY" --quiet --endpoint "unix://$CONTROL_SOCKET" secret set callmesh-api-key
if [[ ! -f "$CIPHERTEXT" || "$(stat -c '%a' "$CIPHERTEXT")" != "600" ]]; then
  fail "SYSTEMD_SMOKE_CIPHERTEXT_INVALID"
fi
if LC_ALL=C grep -aFq "$secret_value" "$CIPHERTEXT"; then
  fail "SYSTEMD_SMOKE_PLAINTEXT_PERSISTED"
fi
unset secret_value

wrapping_key_digest="$(sha256sum "$SECRET_STORE_KEY" | awk '{ print $1 }')"
bash "$MANAGER" install "${manager_arguments[@]}"
if [[ "$(sha256sum "$SECRET_STORE_KEY" | awk '{ print $1 }')" != "$wrapping_key_digest" ]]; then
  fail "SYSTEMD_SMOKE_WRAPPING_KEY_REPLACED"
fi

systemctl restart "$SERVICE_NAME"
wait_for_control_socket
assert_control_status

printf '[cmclient-systemd-smoke] systemd %s LoadCredential and Unix control socket verified\n' \
  "$systemd_version"
