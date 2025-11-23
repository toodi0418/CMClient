#!/usr/bin/env bash
set -euo pipefail

AUTO_UPDATE="${AUTO_UPDATE:-0}"
AUTO_UPDATE_REPO="${AUTO_UPDATE_REPO:-https://github.com/toodi0418/CMClient.git}"
AUTO_UPDATE_BRANCH="${AUTO_UPDATE_BRANCH:-main}"
AUTO_UPDATE_REMOTE="${AUTO_UPDATE_REMOTE:-origin}"
AUTO_UPDATE_WORKDIR="${AUTO_UPDATE_WORKDIR:-/data/callmesh/app-src}"
AUTO_UPDATE_POLL_SECONDS="${AUTO_UPDATE_POLL_SECONDS:-300}"
APP_DIR="/app"

log() {
  printf '[entrypoint] %s\n' "$*" >&2
}

validate_branch() {
  case "$1" in
    main|dev)
      echo "$1"
      ;;
    *)
      log "Invalid AUTO_UPDATE_BRANCH '$1', falling back to main"
      echo "main"
      ;;
  esac
}

AUTO_UPDATE_BRANCH="$(validate_branch "${AUTO_UPDATE_BRANCH}")"

ensure_repo() {
  mkdir -p "${AUTO_UPDATE_WORKDIR}"

  if [[ ! -d "${AUTO_UPDATE_WORKDIR}/.git" ]]; then
    log "Cloning ${AUTO_UPDATE_REPO}@${AUTO_UPDATE_BRANCH} into ${AUTO_UPDATE_WORKDIR}"
    git clone --branch "${AUTO_UPDATE_BRANCH}" "${AUTO_UPDATE_REPO}" "${AUTO_UPDATE_WORKDIR}"
  else
    git -C "${AUTO_UPDATE_WORKDIR}" remote set-url "${AUTO_UPDATE_REMOTE}" "${AUTO_UPDATE_REPO}"
    git -C "${AUTO_UPDATE_WORKDIR}" fetch --prune "${AUTO_UPDATE_REMOTE}" "${AUTO_UPDATE_BRANCH}"
    git -C "${AUTO_UPDATE_WORKDIR}" reset --hard "${AUTO_UPDATE_REMOTE}/${AUTO_UPDATE_BRANCH}"
    git -C "${AUTO_UPDATE_WORKDIR}" clean -fdx
  fi
}

install_dependencies() {
  cd "${AUTO_UPDATE_WORKDIR}"
  log "Installing dependencies (npm ci --omit=dev)"
  npm ci --omit=dev
}

check_for_remote_update() {
  if ! git -C "${AUTO_UPDATE_WORKDIR}" fetch --prune "${AUTO_UPDATE_REMOTE}" "${AUTO_UPDATE_BRANCH}" >/dev/null 2>&1; then
    log "git fetch failed, will retry in ${AUTO_UPDATE_POLL_SECONDS}s"
    return 1
  fi

  local local_rev remote_rev
  local_rev="$(git -C "${AUTO_UPDATE_WORKDIR}" rev-parse HEAD)"
  remote_rev="$(git -C "${AUTO_UPDATE_WORKDIR}" rev-parse "${AUTO_UPDATE_REMOTE}/${AUTO_UPDATE_BRANCH}")"

  if [[ "${local_rev}" != "${remote_rev}" ]]; then
    log "Detected new commit ${remote_rev}; scheduling restart"
    return 0
  fi

  return 1
}

monitor_updates() {
  local app_pid="$1"

  while kill -0 "${app_pid}" >/dev/null 2>&1; do
    sleep "${AUTO_UPDATE_POLL_SECONDS}"
    if check_for_remote_update; then
      log "Stopping current process (PID ${app_pid}) to apply update"
      kill "${app_pid}" >/dev/null 2>&1 || true
      return 1
    fi
  done

  return 0
}

if [[ "${AUTO_UPDATE}" == "1" ]]; then
  log "Auto-update enabled (branch=${AUTO_UPDATE_BRANCH}, poll=${AUTO_UPDATE_POLL_SECONDS}s)"
  while true; do
    ensure_repo
    install_dependencies

    cd "${AUTO_UPDATE_WORKDIR}"
    log "Starting application from ${AUTO_UPDATE_WORKDIR}"
    "$@" &
    app_pid=$!

    monitor_updates "${app_pid}"
    monitor_status=$?
    wait "${app_pid}"
    app_exit=$?

    if [[ "${monitor_status}" -eq 0 ]]; then
      log "Application exited with status ${app_exit}; not restarting"
      exit "${app_exit}"
    fi

    log "Restarting application to apply latest ${AUTO_UPDATE_BRANCH}"
  done
fi

cd "${APP_DIR}"
log "Auto-update disabled; starting bundled image"
exec "$@"
