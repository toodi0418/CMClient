#!/usr/bin/env bash
set -euo pipefail

LABEL="io.cmclient.agent"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMPLATE_PATH="${SCRIPT_DIR}/../packaging/launchd/${LABEL}.plist.in"
INSTALL_ROOT="${CMCLIENT_INSTALL_ROOT:-/Applications/CMClient/current}"
AGENT_BINARY="${CMCLIENT_AGENT_BINARY:-$INSTALL_ROOT/bin/cmclient-agent}"
HOME_DIRECTORY="${HOME:?HOME is required}"
DATA_DIR="${CMCLIENT_DATA_DIR:-$HOME_DIRECTORY/Library/Application Support/CMClient}"
CONFIG_DIR="${CMCLIENT_CONFIG_DIR:-$HOME_DIRECTORY/Library/Application Support/CMClient}"
CACHE_DIR="${CMCLIENT_CACHE_DIR:-$HOME_DIRECTORY/Library/Caches/CMClient}"
LOG_DIR="${CMCLIENT_LOG_DIR:-$DATA_DIR/Logs}"
PLIST_PATH="${CMCLIENT_LAUNCHD_PLIST:-$HOME_DIRECTORY/Library/LaunchAgents/${LABEL}.plist}"
LAUNCHCTL="${CMCLIENT_LAUNCHCTL:-launchctl}"
PLUTIL="${CMCLIENT_PLUTIL:-plutil}"
LOG_LINES="${CMCLIENT_LOG_LINES:-200}"

log() {
  printf '[cmclient-launchd] %s\n' "$*"
}

fail() {
  printf '%s\n' "$1" >&2
  exit 1
}

usage() {
  cat <<'EOF'
Usage: scripts/cmclient-launchd.sh <install|uninstall|start|stop|restart|status|logs|render> [options]

Options:
  --agent <absolute path>       Agent executable
  --plist <absolute path>       LaunchAgent plist path
  --data-dir <absolute path>    Persistent Agent data directory
  --config-dir <absolute path>  Agent configuration directory
  --cache-dir <absolute path>   Agent cache directory
  --log-dir <absolute path>     Agent log directory
  --lines <positive integer>    Lines for logs (default: 200)

This is a per-user LaunchAgent. It never accepts, exports, or writes credentials.
`uninstall` removes only the plist and retains Agent configuration, data, cache, and logs.
EOF
}

require_darwin() {
  if [[ "${CMCLIENT_LAUNCHD_ALLOW_NON_DARWIN:-0}" != "1" && "$(uname -s)" != "Darwin" ]]; then
    fail "LAUNCHD_PLATFORM_UNSUPPORTED"
  fi
}

validate_path() {
  local value="$1"
  if [[ "$value" != /* || "$value" == *$'\n'* || "$value" == *$'\r'* || "$value" == *['|&<>\\']* ]]; then
    fail "LAUNCHD_PATH_INVALID"
  fi
}

validate_positive_integer() {
  if [[ ! "$1" =~ ^[1-9][0-9]*$ ]]; then
    fail "LAUNCHD_LOG_LINES_INVALID"
  fi
}

validate_configuration() {
  validate_path "$AGENT_BINARY"
  validate_path "$HOME_DIRECTORY"
  validate_path "$DATA_DIR"
  validate_path "$CONFIG_DIR"
  validate_path "$CACHE_DIR"
  validate_path "$LOG_DIR"
  validate_path "$PLIST_PATH"
  validate_positive_integer "$LOG_LINES"
  if [[ ! -f "$TEMPLATE_PATH" ]]; then
    fail "LAUNCHD_TEMPLATE_MISSING"
  fi
  if [[ "${CMCLIENT_LAUNCHD_ALLOW_NON_DARWIN:-0}" != "1" && ( "$LAUNCHCTL" != "launchctl" || "$PLUTIL" != "plutil" ) ]]; then
    fail "LAUNCHD_COMMAND_OVERRIDE_FORBIDDEN"
  fi
}

domain() {
  printf 'gui/%s\n' "$(id -u)"
}

service_target() {
  printf '%s/%s\n' "$(domain)" "$LABEL"
}

working_directory() {
  dirname "$(dirname "$AGENT_BINARY")"
}

render_plist() {
  local workdir
  workdir="$(working_directory)"
  validate_path "$workdir"
  sed \
    -e "s|@AGENT_BINARY@|$AGENT_BINARY|g" \
    -e "s|@WORKING_DIRECTORY@|$workdir|g" \
    -e "s|@HOME_DIRECTORY@|$HOME_DIRECTORY|g" \
    -e "s|@DATA_DIR@|$DATA_DIR|g" \
    -e "s|@CONFIG_DIR@|$CONFIG_DIR|g" \
    -e "s|@CACHE_DIR@|$CACHE_DIR|g" \
    -e "s|@LOG_DIR@|$LOG_DIR|g" \
    "$TEMPLATE_PATH"
}

prepare_directories() {
  install -d -m 0700 "$(dirname "$PLIST_PATH")"
  install -d -m 0700 "$DATA_DIR" "$CONFIG_DIR" "$CACHE_DIR" "$LOG_DIR"
}

bootout() {
  "$LAUNCHCTL" bootout "$(service_target)" >/dev/null 2>&1 || true
}

install_service() {
  if [[ ! -x "$AGENT_BINARY" ]]; then
    fail "LAUNCHD_AGENT_NOT_EXECUTABLE"
  fi
  prepare_directories
  local temporary_plist
  temporary_plist="$(mktemp)"
  trap 'rm -f "$temporary_plist"' RETURN
  render_plist > "$temporary_plist"
  "$PLUTIL" -lint "$temporary_plist" >/dev/null
  bootout
  install -m 0600 "$temporary_plist" "$PLIST_PATH"
  "$LAUNCHCTL" bootstrap "$(domain)" "$PLIST_PATH"
  "$LAUNCHCTL" kickstart -k "$(service_target)"
  log "installed $LABEL"
}

uninstall_service() {
  bootout
  rm -f "$PLIST_PATH"
  log "removed $LABEL; retained configuration and runtime data"
}

start_service() {
  [[ -f "$PLIST_PATH" ]] || fail "LAUNCHD_PLIST_MISSING"
  bootout
  "$LAUNCHCTL" bootstrap "$(domain)" "$PLIST_PATH"
  "$LAUNCHCTL" kickstart -k "$(service_target)"
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
    --plist) PLIST_PATH="${2:-}"; shift 2 ;;
    --data-dir) DATA_DIR="${2:-}"; shift 2 ;;
    --config-dir) CONFIG_DIR="${2:-}"; shift 2 ;;
    --cache-dir) CACHE_DIR="${2:-}"; shift 2 ;;
    --log-dir) LOG_DIR="${2:-}"; shift 2 ;;
    --lines) LOG_LINES="${2:-}"; shift 2 ;;
    *) fail "LAUNCHD_USAGE_INVALID_ARGUMENT" ;;
  esac
done

require_darwin
validate_configuration

case "$COMMAND" in
  install) install_service ;;
  uninstall) uninstall_service ;;
  start) start_service ;;
  stop) bootout ;;
  restart) "$LAUNCHCTL" kickstart -k "$(service_target)" ;;
  status) "$LAUNCHCTL" print "$(service_target)" ;;
  logs) tail -n "$LOG_LINES" "$LOG_DIR/agent.stdout.log" "$LOG_DIR/agent.stderr.log" ;;
  render) render_plist ;;
  *) fail "LAUNCHD_USAGE_INVALID_COMMAND" ;;
esac
