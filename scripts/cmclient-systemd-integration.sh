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
SERVICE_HOME="/home/cmclient-systemd-smoke-$run_label"
RUNTIME_ROOT="$SERVICE_HOME/.cmclient"
AGENT_BINARY="$INSTALL_ROOT/bin/cmclient-agent"
CLI_BINARY="$INSTALL_ROOT/bin/cmclient"
GATEWAY_FIXTURE="$INSTALL_ROOT/bin/cmclient-systemd-gateway-fixture.py"
CONTROL_SOCKET="$RUNTIME_ROOT/run/control.sock"
SECRETS_FILE="$RUNTIME_ROOT/secrets.json"

manager_arguments=(
  --agent "$AGENT_BINARY"
  --home "$SERVICE_HOME"
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
  rm -rf "$INSTALL_ROOT" "$SERVICE_HOME"
  exit "$status"
}
trap cleanup EXIT

install -d -m 0755 "$INSTALL_ROOT/bin"
install -m 0755 "$AGENT_SOURCE" "$AGENT_BINARY"
install -m 0755 "$CLI_SOURCE" "$CLI_BINARY"
cat >"$GATEWAY_FIXTURE" <<'PY'
#!/usr/bin/env python3
import hashlib
import hmac
import json
import os
import re
import socket
import struct
import sys
import threading

MAX_FRAME_BYTES = 16 * 1024
MAX_CALLMESH_API_KEY_BYTES = 4096
MAX_REQUEST_BYTES = 8192
OWNERSHIP_PATH = "/_cmclient/bootstrap/ownership"
OWNERSHIP_PROTOCOL = "cmclient-bootstrap-ownership-v1"
OWNERSHIP_DOMAIN = "cmclient.gateway.bootstrap-ownership.v1"
OWNERSHIP_CHALLENGE_HEADER = "x-cmclient-gateway-ownership-challenge"
OWNERSHIP_PROOF_HEADER = "x-cmclient-gateway-ownership-proof"


def read_exact(stream, length):
    result = bytearray()
    while len(result) < length:
        chunk = stream.read(length - len(result))
        if not chunk:
            raise RuntimeError("early eof")
        result.extend(chunk)
    return bytes(result)


def required_environment(name, pattern):
    value = os.environ.get(name, "")
    if re.fullmatch(pattern, value) is None:
        raise RuntimeError("invalid build identity")
    return value


def identity_report():
    return {
        "schemaVersion": 1,
        "component": "gateway",
        "identity": {
            "schemaVersion": 1,
            "product": "CMClient",
            "version": required_environment(
                "CMCLIENT_BUILD_VERSION", r"[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?"
            ),
            "sourceCommit": required_environment("CMCLIENT_BUILD_COMMIT", r"[0-9a-f]{40}"),
            "sourceTree": required_environment(
                "CMCLIENT_BUILD_TREE", r"(?:[0-9a-f]{40}|sha256:[0-9a-f]{64})"
            ),
            "channel": required_environment(
                "CMCLIENT_BUILD_CHANNEL", r"(?:dev|candidate|stable)"
            ),
            "target": {
                "os": required_environment("CMCLIENT_TARGET_OS", r"(?:windows|macos|linux)"),
                "architecture": required_environment(
                    "CMCLIENT_TARGET_ARCHITECTURE", r"(?:x86_64|aarch64|universal)"
                ),
                "profile": required_environment(
                    "CMCLIENT_RUNTIME_PROFILE", r"(?:native|docker)"
                ),
                "packageProfile": required_environment(
                    "CMCLIENT_PACKAGE_PROFILE", r"(?:workspace|setup|dmg|appimage|oci)"
                ),
            },
        },
    }


def send_json(connection, status, value):
    body = json.dumps(value, separators=(",", ":")).encode("utf-8")
    reason = {200: "OK", 403: "Forbidden", 404: "Not Found"}[status]
    header = (
        f"HTTP/1.1 {status} {reason}\r\n"
        "Content-Type: application/json\r\n"
        f"Content-Length: {len(body)}\r\n"
        "Connection: close\r\n\r\n"
    ).encode("ascii")
    connection.sendall(header + body)


def send_ownership_response(connection, capability, nonce, pid, port, challenge):
    transcript = (
        f"{OWNERSHIP_DOMAIN}\n{nonce}\n{pid}\n127.0.0.1\n{port}\n{challenge}"
    ).encode("ascii")
    proof = hmac.new(
        capability.encode("ascii"), transcript, digestmod=hashlib.sha256
    ).hexdigest()
    response = (
        "HTTP/1.1 200 OK\r\n"
        "Connection: close\r\n"
        "Content-Length: 0\r\n"
        f"{OWNERSHIP_PROOF_HEADER}: {proof}\r\n\r\n"
    ).encode("ascii")
    connection.sendall(response)


def reject_ownership_request(connection):
    connection.sendall(
        b"HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
    )


def serve_request(listener, capability, nonce, identity, ownership_proven):
    connection, _ = listener.accept()
    with connection:
        connection.settimeout(2)
        request = bytearray()
        while b"\r\n\r\n" not in request:
            chunk = connection.recv(1024)
            if not chunk or len(request) + len(chunk) > MAX_REQUEST_BYTES:
                return
            request.extend(chunk)
        header, separator, trailing = bytes(request).partition(b"\r\n\r\n")
        if not separator or trailing:
            return
        lines = header.split(b"\r\n")
        request_line = lines[0].decode("ascii").split()
        if len(request_line) != 3:
            send_json(connection, 404, {"code": "NOT_FOUND"})
            return
        headers = {}
        for raw_line in lines[1:]:
            if not raw_line:
                break
            name, separator, value = raw_line.partition(b":")
            if not separator:
                return
            key = name.decode("ascii").strip().lower()
            if key in headers:
                return
            headers[key] = value.decode("ascii").strip()

        if request_line[1] == OWNERSHIP_PATH:
            challenge = headers.get(OWNERSHIP_CHALLENGE_HEADER, "")
            port = listener.getsockname()[1]
            if not (
                request_line == ["GET", OWNERSHIP_PATH, "HTTP/1.1"]
                and headers.get("host") == f"127.0.0.1:{port}"
                and headers.get("connection", "").lower() == "upgrade"
                and headers.get("upgrade", "").lower() == OWNERSHIP_PROTOCOL
                and headers.get("content-length") == "0"
                and "transfer-encoding" not in headers
                and "x-cmclient-gateway-capability" not in headers
                and re.fullmatch(r"[0-9a-f]{64}", challenge) is not None
            ):
                reject_ownership_request(connection)
                return
            send_ownership_response(
                connection, capability, nonce, os.getpid(), port, challenge
            )
            ownership_proven.set()
            return

        if request_line[0] != "GET" or request_line[2] != "HTTP/1.1":
            send_json(connection, 404, {"code": "NOT_FOUND"})
            return
        if not ownership_proven.is_set():
            send_json(connection, 403, {"code": "GATEWAY_OWNERSHIP_REQUIRED"})
            return
        provided = headers.get("x-cmclient-gateway-capability", "")
        if not hmac.compare_digest(provided, capability):
            send_json(connection, 403, {"code": "GATEWAY_CAPABILITY_REQUIRED"})
        elif request_line[1] == "/api/v1/system/health":
            send_json(connection, 200, {"status": "ok"})
        elif request_line[1] == "/api/v1/system/version":
            send_json(connection, 200, identity)
        else:
            send_json(connection, 404, {"code": "NOT_FOUND"})


def main():
    frame_length = struct.unpack(">I", read_exact(sys.stdin.buffer, 4))[0]
    if frame_length < 1 or frame_length > MAX_FRAME_BYTES:
        raise RuntimeError("invalid frame length")
    bootstrap = json.loads(read_exact(sys.stdin.buffer, frame_length))
    fields = {"schemaVersion", "type", "startupNonce", "capability"}
    if set(bootstrap) not in (fields, fields | {"callMeshApiKey"}):
        raise RuntimeError("invalid bootstrap fields")
    if bootstrap.get("schemaVersion") != 1 or bootstrap.get("type") != "gateway.bootstrap":
        raise RuntimeError("invalid bootstrap type")
    nonce = bootstrap.get("startupNonce")
    capability = bootstrap.get("capability")
    callmesh_api_key = bootstrap.get("callMeshApiKey")
    if not isinstance(nonce, str) or re.fullmatch(r"[0-9a-f]{32}", nonce) is None:
        raise RuntimeError("invalid nonce")
    if not isinstance(capability, str) or re.fullmatch(r"[0-9a-f]{64}", capability) is None:
        raise RuntimeError("invalid capability")
    if callmesh_api_key is not None:
        if (
            not isinstance(callmesh_api_key, str)
            or not callmesh_api_key
            or len(callmesh_api_key.encode("utf-8")) > MAX_CALLMESH_API_KEY_BYTES
            or any(
                ord(character) <= 0x1F or ord(character) == 0x7F
                for character in callmesh_api_key
            )
        ):
            raise RuntimeError("invalid callmesh api key")
        if any(callmesh_api_key in value for value in [*sys.argv, *os.environ.values()]):
            raise RuntimeError("callmesh api key escaped private bootstrap")

    identity = identity_report()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 0)
        listener.bind(("127.0.0.1", 0))
        listener.listen(8)
        listener.settimeout(0.2)
        ready = json.dumps(
            {
                "schemaVersion": 1,
                "type": "gateway.ready",
                "pid": os.getpid(),
                "startupNonce": nonce,
                "host": "127.0.0.1",
                "port": listener.getsockname()[1],
            },
            separators=(",", ":"),
        ).encode("utf-8")
        sys.stdout.buffer.write(struct.pack(">I", len(ready)) + ready)
        sys.stdout.buffer.flush()

        stopped = threading.Event()
        ownership_proven = threading.Event()
        shutdown_error = []

        def watch_shutdown():
            command = sys.stdin.buffer.readline(MAX_FRAME_BYTES + 1)
            if command not in (b"", b"CMCLIENT_SHUTDOWN\n"):
                shutdown_error.append(True)
            stopped.set()

        threading.Thread(target=watch_shutdown, daemon=True).start()
        while not stopped.is_set():
            try:
                serve_request(listener, capability, nonce, identity, ownership_proven)
            except socket.timeout:
                pass
        if shutdown_error:
            raise RuntimeError("invalid shutdown command")


try:
    main()
except Exception:
    sys.stderr.write("GATEWAY_FIXTURE_FAILED\n")
    raise SystemExit(1)
PY
chown root:"$SERVICE_GROUP" "$GATEWAY_FIXTURE"
chmod 0755 "$GATEWAY_FIXTURE"
install -d -m 0700 -o "$SERVICE_USER" -g "$SERVICE_GROUP" "$RUNTIME_ROOT"
printf '%s\n' \
  '[agent]' \
  "gateway_command = [\"/usr/bin/python3\", \"$GATEWAY_FIXTURE\"]" \
  'management_web_enabled = false' \
  '' \
  '[callmesh]' \
  'url = "https://callmesh.invalid"' \
  >"$RUNTIME_ROOT/config.toml"
chown "$SERVICE_USER:$SERVICE_GROUP" "$RUNTIME_ROOT/config.toml"
chmod 0600 "$RUNTIME_ROOT/config.toml"

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
if [[ "$(stat -c '%U:%G' "$RUNTIME_ROOT")" != "$SERVICE_USER:$SERVICE_GROUP" \
  || "$(stat -c '%a' "$RUNTIME_ROOT")" != "700" ]]; then
  fail "SYSTEMD_SMOKE_RUNTIME_ROOT_INVALID"
fi

main_pid="$(systemctl show "$SERVICE_NAME" --property=MainPID --value)"
if [[ ! "$main_pid" =~ ^[1-9][0-9]*$ ]]; then
  fail "SYSTEMD_SMOKE_MAIN_PID_INVALID"
fi
if [[ "$(tr '\0' '\n' <"/proc/$main_pid/environ" | sed -n 's/^HOME=//p')" != "$SERVICE_HOME" ]]; then
  fail "SYSTEMD_SMOKE_HOME_INVALID"
fi
if tr '\0' '\n' <"/proc/$main_pid/environ" | grep -Eq '^CMCLIENT_'; then
  fail "SYSTEMD_SMOKE_APPLICATION_ENVIRONMENT_PRESENT"
fi
assert_control_status

secret_value="cmclient-systemd-smoke-$run_label"
printf '%s\n' "$secret_value" \
  | "$CLI_BINARY" --quiet --endpoint "unix://$CONTROL_SOCKET" secret set callmesh-api-key
if [[ ! -f "$SECRETS_FILE" \
  || "$(stat -c '%U:%G' "$SECRETS_FILE")" != "$SERVICE_USER:$SERVICE_GROUP" \
  || "$(stat -c '%a' "$SECRETS_FILE")" != "600" ]]; then
  fail "SYSTEMD_SMOKE_SECRETS_DOCUMENT_INVALID"
fi
stored_secret="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["callmesh-api-key"])' "$SECRETS_FILE")"
if [[ "$stored_secret" != "$secret_value" ]]; then
  fail "SYSTEMD_SMOKE_PLAINTEXT_SECRET_VALUE_INVALID"
fi
unset stored_secret secret_value

secrets_digest="$(sha256sum "$SECRETS_FILE" | awk '{ print $1 }')"
bash "$MANAGER" install "${manager_arguments[@]}"
if [[ "$(sha256sum "$SECRETS_FILE" | awk '{ print $1 }')" != "$secrets_digest" ]]; then
  fail "SYSTEMD_SMOKE_SECRETS_REPLACED"
fi

systemctl restart "$SERVICE_NAME"
wait_for_control_socket
assert_control_status

printf '[cmclient-systemd-smoke] systemd %s canonical plaintext runtime and Unix control socket verified\n' \
  "$systemd_version"
