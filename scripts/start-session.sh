#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

"$WORKSPACE_ROOT/scripts/doctor.sh"
require_python3

if [[ "$(git -C "$REPO_DIR" rev-parse --is-inside-work-tree 2>/dev/null || true)" == "true" ]] &&
  [[ -z "$(git -C "$REPO_DIR" status --porcelain)" ]]; then
  require_dev_branch
  git -C "$REPO_DIR" fetch "$CMCLIENT_REMOTE" "$CMCLIENT_BRANCH"
  git -C "$REPO_DIR" pull --ff-only "$CMCLIENT_REMOTE" "$CMCLIENT_BRANCH"
  log "Fast-forward sync complete for $CMCLIENT_BRANCH"

  mapfile -t recovery_tasks < <(
    "$CMCLIENT_PYTHON3" - "$WORKSPACE_ROOT/state/TASKS.json" <<'PY'
import json, sys
tasks = json.load(open(sys.argv[1], encoding="utf-8"))["tasks"]
for task in tasks:
    if task.get("status") in {"in_progress", "blocked"}:
        print(task["id"])
PY
  )
  for task in "${recovery_tasks[@]}"; do
    if git -C "$REPO_DIR" log --all --format='%s' |
      grep -E "^(feat|fix|refactor|test|docs|build|ci|chore|perf|security|release)\([a-z0-9-]+\): \[$task\] .+" >/dev/null 2>&1; then
      reconcile_args=("$task")
      if [[ "$CMCLIENT_AUTO_PUSH" == "1" ]]; then
        reconcile_args+=(--push-local)
      fi
      "$CMCLIENT_PYTHON3" "$SCRIPT_DIR/reconcile-task-state.py" \
        "${reconcile_args[@]}" \
        --repo "$REPO_DIR" \
        --state "$WORKSPACE_ROOT/state/TASKS.json" \
        --commits "$WORKSPACE_ROOT/state/COMMITS.md" \
        --remote "$CMCLIENT_REMOTE" \
        --branch "$CMCLIENT_BRANCH"
    fi
  done
else
  warn "Repository is dirty; sync and automatic checkpoint recovery were skipped"
fi

log "Next executable task:"
"$SCRIPT_DIR/next-task.sh"
