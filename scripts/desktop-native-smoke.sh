#!/usr/bin/env bash
set -euo pipefail

stage="${1:?native Desktop stage path is required}"
target="${2:?target is required}"
version="${3:?version is required}"
stage="$(cd "$stage" && pwd)"

node scripts/desktop-native-bundles.mjs verify-stage \
  --target "$target" \
  --version "$version" \
  --input "$stage"

runtime_root() {
  local root="$1"
  find "$root" -type d -name cmclient-runtime -print -quit
}

verify_runtime() {
  local root="$1"
  [[ -n "$root" && -d "$root" ]] || {
    echo "NATIVE_DESKTOP_RUNTIME_MISSING" >&2
    exit 1
  }
  node scripts/desktop-native-bundles.mjs verify-runtime \
    --target "$target" \
    --version "$version" \
    --input "$root"
}

temporary="$(mktemp -d)"
mounted=""
native_pid=""
cleanup() {
  if [[ -n "$native_pid" ]]; then
    kill "$native_pid" >/dev/null 2>&1 || true
    wait "$native_pid" >/dev/null 2>&1 || true
  fi
  if [[ -n "$mounted" ]]; then
    hdiutil detach "$mounted" -quiet >/dev/null 2>&1 || true
  fi
  rm -rf "$temporary"
}
trap cleanup EXIT

launch_native_app() {
  local log="$temporary/native-app.log"
  if [[ "$target" == linux-* ]]; then
    command -v dbus-run-session >/dev/null || { echo "NATIVE_DESKTOP_DBUS_MISSING" >&2; exit 1; }
    command -v xvfb-run >/dev/null || { echo "NATIVE_DESKTOP_XVFB_MISSING" >&2; exit 1; }
    WEBKIT_DISABLE_COMPOSITING_MODE=1 APPIMAGE_EXTRACT_AND_RUN=1 \
      dbus-run-session -- xvfb-run -a "$@" >"$log" 2>&1 &
  else
    "$@" >"$log" 2>&1 &
  fi
  native_pid="$!"
  native_running=false
  for _ in $(seq 1 30); do
    if kill -0 "$native_pid" >/dev/null 2>&1; then
      native_running=true
      break
    fi
    sleep 0.1
  done
  [[ "$native_running" == true ]] || { cat "$log" >&2; exit 1; }
  sleep 2
  kill -0 "$native_pid" >/dev/null 2>&1 || { cat "$log" >&2; exit 1; }
  kill "$native_pid" >/dev/null 2>&1 || true
  wait "$native_pid" >/dev/null 2>&1 || true
  native_pid=""
}

case "$target" in
  darwin-*)
    dmg="$stage/cmclient-desktop-$target-$version.dmg"
    mounted="$temporary/mounted"
    mkdir -p "$mounted"
    hdiutil attach "$dmg" -readonly -nobrowse -mountpoint "$mounted" -quiet
    verify_runtime "$(runtime_root "$mounted")"
    app_binary="$(find "$mounted" -type f -path '*/Contents/MacOS/*' -perm -u+x -print -quit)"
    [[ -n "$app_binary" ]] || { echo "NATIVE_DESKTOP_APP_BINARY_MISSING" >&2; exit 1; }
    launch_native_app "$app_binary"
    ;;
  linux-*)
    deb="$stage/cmclient-desktop-$target-$version.deb"
    appimage="$stage/cmclient-desktop-$target-$version.AppImage"

    mkdir -p "$temporary/deb"
    dpkg-deb --extract "$deb" "$temporary/deb"
    verify_runtime "$(runtime_root "$temporary/deb")"
    deb_binary="$(find "$temporary/deb" -type f -path '*/usr/bin/*' -perm -u+x -print -quit)"
    [[ -n "$deb_binary" ]] || { echo "NATIVE_DESKTOP_DEB_BINARY_MISSING" >&2; exit 1; }
    launch_native_app "$deb_binary"

    cp "$appimage" "$temporary/CMClient.AppImage"
    chmod +x "$temporary/CMClient.AppImage"
    (
      cd "$temporary"
      ./CMClient.AppImage --appimage-extract >/dev/null
    )
    verify_runtime "$(runtime_root "$temporary/squashfs-root")"
    launch_native_app "$temporary/CMClient.AppImage"
    ;;
  *)
    echo "NATIVE_DESKTOP_SMOKE_TARGET_UNSUPPORTED" >&2
    exit 1
    ;;
esac
