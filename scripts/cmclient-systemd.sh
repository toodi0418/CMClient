#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="cmclient-agent.service"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMPLATE_PATH="${SCRIPT_DIR}/../packaging/systemd/cmclient-agent.service.in"
DEFAULT_UNIT_DIR="/etc/systemd/system"

UNIT_DIR="${CMCLIENT_SYSTEMD_UNIT_DIR:-$DEFAULT_UNIT_DIR}"
INSTALL_ROOT="${CMCLIENT_INSTALL_ROOT:-/opt/cmclient/current}"
AGENT_BINARY="${CMCLIENT_AGENT_BINARY:-$INSTALL_ROOT/bin/cmclient-agent}"
CONFIG_DIR="${CMCLIENT_CONFIG_DIR:-/etc/cmclient}"
DATA_DIR="${CMCLIENT_DATA_DIR:-/var/lib/cmclient}"
CACHE_DIR="${CMCLIENT_CACHE_DIR:-/var/cache/cmclient}"
LOG_DIR="${CMCLIENT_LOG_DIR:-/var/log/cmclient}"
SECRET_STORE_KEY=""
SERVICE_USER="${CMCLIENT_SERVICE_USER:-cmclient}"
SERVICE_GROUP="${CMCLIENT_SERVICE_GROUP:-cmclient}"
SERIAL_GROUP="${CMCLIENT_SERIAL_GROUP:-}"
SYSTEMCTL="${CMCLIENT_SYSTEMCTL:-systemctl}"
JOURNALCTL="${CMCLIENT_JOURNALCTL:-journalctl}"
LOG_LINES="${CMCLIENT_LOG_LINES:-200}"
SKIP_USER_SETUP=0

log() {
  printf '[cmclient-systemd] %s\n' "$*"
}

fail() {
  printf '%s\n' "$1" >&2
  exit 1
}

valid_daily_stamp() {
  local stamp="$1"
  local year month day max_day

  [[ "$stamp" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || return 1
  year=$((10#${stamp:0:4}))
  month=$((10#${stamp:5:2}))
  day=$((10#${stamp:8:2}))
  (( year >= 1970 && month >= 1 && month <= 12 && day >= 1 )) || return 1
  case "$month" in
    2)
      max_day=28
      if (( year % 400 == 0 || (year % 4 == 0 && year % 100 != 0) )); then
        max_day=29
      fi
      ;;
    4|6|9|11) max_day=30 ;;
    *) max_day=31 ;;
  esac
  (( day <= max_day ))
}

usage() {
  cat <<'EOF'
Usage: scripts/cmclient-systemd.sh <install|uninstall|start|stop|restart|status|enable|disable|logs|render> [options]

Install options:
  --agent <absolute path>        Agent executable (default: /opt/cmclient/current/bin/cmclient-agent)
  --unit-dir <absolute path>     Unit directory (default: /etc/systemd/system)
  --config-dir <absolute path>   Agent configuration directory (default: /etc/cmclient)
  --data-dir <absolute path>     Persistent Agent data directory (default: /var/lib/cmclient)
  --cache-dir <absolute path>    Agent cache directory (default: /var/cache/cmclient)
  --log-dir <absolute path>      Agent log directory (default: /var/log/cmclient)
  --service-user <name>          Non-login account (default: cmclient)
  --service-group <name>         Service group (default: cmclient)
  --serial-group <name>          Existing group allowed to access serial devices
  --systemctl <absolute path>    Override for packaging tests
  --journalctl <absolute path>   Override for packaging tests
  --lines <1..10000>             Lines for logs (default: 200)
  --skip-user-setup              Packaging-test only: do not create or modify accounts

The manager never accepts credentials or writes secret values. `uninstall`
removes the unit only; Agent configuration, the service wrapping key, data,
cache, and logs are retained.
EOF
}

require_linux() {
  if [[ "${CMCLIENT_SYSTEMD_ALLOW_NON_LINUX:-0}" != "1" && "$(uname -s)" != "Linux" ]]; then
    fail "SYSTEMD_PLATFORM_UNSUPPORTED"
  fi
}

validate_path() {
  local value="$1"
  if [[ ! "$value" =~ ^/[A-Za-z0-9_./@:+,-]+$ ]]; then
    fail "SYSTEMD_PATH_INVALID"
  fi
}

validate_account_name() {
  local value="$1"
  if [[ ! "$value" =~ ^[a-z_][a-z0-9_-]{0,63}$ ]]; then
    fail "SYSTEMD_ACCOUNT_INVALID"
  fi
}

validate_positive_integer() {
  local value="$1"
  if [[ ! "$value" =~ ^[1-9][0-9]*$ || ${#value} -gt 5 ]] || (( 10#$value > 10000 )); then
    fail "SYSTEMD_LOG_LINES_INVALID"
  fi
}

run_privileged() {
  if [[ "$SKIP_USER_SETUP" == "1" && "$UNIT_DIR" != "$DEFAULT_UNIT_DIR" ]]; then
    "$@"
  elif [[ "${EUID:-$(id -u)}" -eq 0 ]]; then
    "$@"
  elif command -v sudo >/dev/null 2>&1; then
    sudo -- "$@"
  else
    fail "SYSTEMD_PRIVILEGE_REQUIRED"
  fi
}

service_unit_path() {
  printf '%s/%s\n' "$UNIT_DIR" "$SERVICE_NAME"
}

ensure_service_account() {
  if [[ "$SKIP_USER_SETUP" == "1" ]]; then
    return
  fi
  if ! getent group "$SERVICE_GROUP" >/dev/null; then
    run_privileged groupadd --system "$SERVICE_GROUP"
  fi
  if ! id "$SERVICE_USER" >/dev/null 2>&1; then
    run_privileged useradd \
      --system \
      --gid "$SERVICE_GROUP" \
      --home-dir "$DATA_DIR" \
      --create-home \
      --shell /usr/sbin/nologin \
      "$SERVICE_USER"
  fi
  if [[ -n "$SERIAL_GROUP" ]] && ! getent group "$SERIAL_GROUP" >/dev/null; then
    fail "SYSTEMD_SERIAL_GROUP_NOT_FOUND"
  fi
}

install_directory() {
  local path="$1"
  local mode="$2"
  if [[ "$SKIP_USER_SETUP" == "1" ]]; then
    install -d -m "$mode" "$path"
  else
    run_privileged install -d -m "$mode" -o "$SERVICE_USER" -g "$SERVICE_GROUP" "$path"
  fi
}

prepare_directories() {
  if [[ "$SKIP_USER_SETUP" == "1" ]]; then
    install -d -m 0750 "$CONFIG_DIR"
  else
    run_privileged install -d -m 0750 -o root -g "$SERVICE_GROUP" "$CONFIG_DIR"
  fi
  install_directory "$DATA_DIR" 0750
  install_directory "$CACHE_DIR" 0750
  install_directory "$LOG_DIR" 0750
}

ensure_secret_store_key() {
  local key_size
  if run_privileged test -e "$SECRET_STORE_KEY" || run_privileged test -L "$SECRET_STORE_KEY"; then
    if ! run_privileged test -f "$SECRET_STORE_KEY" || run_privileged test -L "$SECRET_STORE_KEY"; then
      fail "SYSTEMD_SECRET_STORE_KEY_INVALID"
    fi
    key_size="$(run_privileged wc -c "$SECRET_STORE_KEY" | awk '{print $1}')"
    if [[ "$key_size" != "32" ]]; then
      fail "SYSTEMD_SECRET_STORE_KEY_INVALID"
    fi
    if [[ "$SKIP_USER_SETUP" == "1" ]]; then
      chmod 0600 "$SECRET_STORE_KEY"
    else
      run_privileged chown root:root "$SECRET_STORE_KEY"
      run_privileged chmod 0600 "$SECRET_STORE_KEY"
    fi
    return
  fi

  if run_privileged test -e "$DATA_DIR/secrets" || run_privileged test -L "$DATA_DIR/secrets"; then
    if ! run_privileged test -d "$DATA_DIR/secrets" || run_privileged test -L "$DATA_DIR/secrets"; then
      fail "SYSTEMD_SECRET_STORE_DIRECTORY_INVALID"
    fi
    local existing_secret_entry
    if ! existing_secret_entry="$(run_privileged find "$DATA_DIR/secrets" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null)"; then
      fail "SYSTEMD_SECRET_STORE_SCAN_FAILED"
    fi
    if [[ -n "$existing_secret_entry" ]]; then
      fail "SYSTEMD_SECRET_STORE_KEY_MISSING"
    fi
  fi

  (
    local temporary_key
    temporary_key="$(mktemp)"
    trap 'rm -f "$temporary_key"' EXIT
    chmod 0600 "$temporary_key"
    dd if=/dev/urandom of="$temporary_key" bs=32 count=1 status=none
    if [[ "$SKIP_USER_SETUP" == "1" ]]; then
      install -m 0600 "$temporary_key" "$SECRET_STORE_KEY"
    else
      run_privileged install -o root -g root -m 0600 "$temporary_key" "$SECRET_STORE_KEY"
    fi
  )
}

render_unit() {
  local working_directory
  local supplementary_group_line="# No serial supplementary group requested"
  working_directory="$(dirname "$(dirname "$AGENT_BINARY")")"
  if [[ -n "$SERIAL_GROUP" ]]; then
    supplementary_group_line="SupplementaryGroups=$SERIAL_GROUP"
  fi
  validate_path "$working_directory"
  sed \
    -e "s|@SERVICE_USER@|$SERVICE_USER|g" \
    -e "s|@SERVICE_GROUP@|$SERVICE_GROUP|g" \
    -e "s|@SUPPLEMENTARY_GROUP_LINE@|$supplementary_group_line|g" \
    -e "s|@WORKING_DIRECTORY@|$working_directory|g" \
    -e "s|@AGENT_BINARY@|$AGENT_BINARY|g" \
    -e "s|@CONFIG_DIR@|$CONFIG_DIR|g" \
    -e "s|@DATA_DIR@|$DATA_DIR|g" \
    -e "s|@CACHE_DIR@|$CACHE_DIR|g" \
    -e "s|@LOG_DIR@|$LOG_DIR|g" \
    -e "s|@SECRET_STORE_KEY@|$SECRET_STORE_KEY|g" \
    "$TEMPLATE_PATH"
}

validate_configuration() {
  validate_path "$UNIT_DIR"
  validate_path "$AGENT_BINARY"
  validate_path "$CONFIG_DIR"
  validate_path "$DATA_DIR"
  validate_path "$CACHE_DIR"
  validate_path "$LOG_DIR"
  validate_path "$SECRET_STORE_KEY"
  validate_account_name "$SERVICE_USER"
  validate_account_name "$SERVICE_GROUP"
  if [[ -n "$SERIAL_GROUP" ]]; then
    validate_account_name "$SERIAL_GROUP"
  fi
  validate_positive_integer "$LOG_LINES"
  if [[ "${CMCLIENT_SYSTEMD_ALLOW_NON_LINUX:-0}" != "1" && ( "$SYSTEMCTL" != "systemctl" || "$JOURNALCTL" != "journalctl" ) ]]; then
    fail "SYSTEMD_COMMAND_OVERRIDE_FORBIDDEN"
  fi
  if [[ "$SKIP_USER_SETUP" == "1" && "${CMCLIENT_SYSTEMD_ALLOW_NON_LINUX:-0}" != "1" ]]; then
    fail "SYSTEMD_TEST_MODE_REQUIRED"
  fi
  if [[ ! -f "$TEMPLATE_PATH" ]]; then
    fail "SYSTEMD_TEMPLATE_MISSING"
  fi
}

install_service() {
  if [[ ! -x "$AGENT_BINARY" ]]; then
    fail "SYSTEMD_AGENT_NOT_EXECUTABLE"
  fi
  ensure_service_account
  prepare_directories
  ensure_secret_store_key
  local temporary_unit
  temporary_unit="$(mktemp)"
  trap 'rm -f "$temporary_unit"' RETURN
  render_unit > "$temporary_unit"
  run_privileged install -d -m 0755 "$UNIT_DIR"
  run_privileged install -m 0644 "$temporary_unit" "$(service_unit_path)"
  run_systemctl daemon-reload
  run_systemctl enable --now "$SERVICE_NAME"
  log "installed $SERVICE_NAME"
}

uninstall_service() {
  local unit_path
  unit_path="$(service_unit_path)"
  run_systemctl disable --now "$SERVICE_NAME" || true
  if [[ -f "$unit_path" ]]; then
    run_privileged rm -f "$unit_path"
  fi
  run_systemctl daemon-reload
  log "removed $SERVICE_NAME; retained configuration and runtime data"
}

run_systemctl() {
  run_privileged "$SYSTEMCTL" "$@"
}

run_journalctl() {
  # The journal is only an early-start fallback. Agent stderr is defined as a
  # stable code stream; filtering here prevents accidental raw output from a
  # failed or externally replaced executable from becoming manager output.
  run_privileged "$JOURNALCTL" \
    --unit "$SERVICE_NAME" \
    --no-pager \
    --lines "$LOG_LINES" \
    --output cat |
    while IFS= read -r line; do
      if [[ "$line" =~ ^[A-Z][A-Z0-9_]{2,127}$ ]]; then
        printf '%s\n' "$line"
      fi
    done
}

tail_application_logs() {
  local log_files=()
  local candidate
  local latest_daily
  local log_path
  local log_name
  local selected_log

  for log_name in agent.jsonl gateway.jsonl; do
    log_path="$LOG_DIR/$log_name"
    if [[ -L "$log_path" ]]; then
      fail "SYSTEMD_LOG_FILE_INVALID"
    fi
    if [[ -e "$log_path" ]]; then
      [[ -f "$log_path" ]] || fail "SYSTEMD_LOG_FILE_INVALID"
      selected_log="$log_path"
    else
      selected_log=""
    fi

    latest_daily=""
    for candidate in "$LOG_DIR/$log_name".[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]; do
      [[ -e "$candidate" || -L "$candidate" ]] || continue
      [[ ! -L "$candidate" && -f "$candidate" ]] || fail "SYSTEMD_LOG_FILE_INVALID"
      valid_daily_stamp "${candidate##*.}" || fail "SYSTEMD_LOG_FILE_INVALID"
      if [[ -z "$latest_daily" || "$candidate" > "$latest_daily" ]]; then
        latest_daily="$candidate"
      fi
    done
    if [[ -n "$latest_daily" ]]; then
      selected_log="$latest_daily"
    fi
    if [[ -n "$selected_log" ]]; then
      log_files+=("$selected_log")
    fi
  done

  if (( ${#log_files[@]} > 0 )); then
    tail -n "$LOG_LINES" -- "${log_files[@]}"
    return
  fi

  run_journalctl
}

COMMAND="${1:-}"
if [[ -z "$COMMAND" || "$COMMAND" == "--help" || "$COMMAND" == "-h" ]]; then
  usage
  exit 0
fi
shift

while [[ $# -gt 0 ]]; do
  case "$1" in
    --agent) AGENT_BINARY="${2:-}"; shift 2 ;;
    --unit-dir) UNIT_DIR="${2:-}"; shift 2 ;;
    --config-dir) CONFIG_DIR="${2:-}"; shift 2 ;;
    --data-dir) DATA_DIR="${2:-}"; shift 2 ;;
    --cache-dir) CACHE_DIR="${2:-}"; shift 2 ;;
    --log-dir) LOG_DIR="${2:-}"; shift 2 ;;
    --service-user) SERVICE_USER="${2:-}"; shift 2 ;;
    --service-group) SERVICE_GROUP="${2:-}"; shift 2 ;;
    --serial-group) SERIAL_GROUP="${2:-}"; shift 2 ;;
    --systemctl) SYSTEMCTL="${2:-}"; shift 2 ;;
    --journalctl) JOURNALCTL="${2:-}"; shift 2 ;;
    --lines) LOG_LINES="${2:-}"; shift 2 ;;
    --skip-user-setup) SKIP_USER_SETUP=1; shift ;;
    *) fail "SYSTEMD_USAGE_INVALID_ARGUMENT" ;;
  esac
done

SECRET_STORE_KEY="$CONFIG_DIR/secret-store.key"

require_linux
validate_configuration

case "$COMMAND" in
  install) install_service ;;
  uninstall) uninstall_service ;;
  start|stop|restart|status|enable|disable) run_systemctl "$COMMAND" "$SERVICE_NAME" ;;
  logs) tail_application_logs ;;
  render) render_unit ;;
  *) fail "SYSTEMD_USAGE_INVALID_COMMAND" ;;
esac
