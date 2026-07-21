#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

require_python3
journal_status=0
"$CMCLIENT_PYTHON3" - "$WORKSPACE_ROOT/state/GRAPH_UPGRADE.json" <<'PY' || journal_status=$?
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
if not path.exists():
    raise SystemExit(0)
try:
    journal = json.loads(path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError) as error:
    raise SystemExit(f"invalid graph upgrade journal: {error}")
if journal.get("schema") != "cmclient-graph-upgrade-journal/v1":
    raise SystemExit("invalid graph upgrade journal schema")
operation_id = journal.get("operationId")
if not isinstance(operation_id, str) or not operation_id:
    raise SystemExit("invalid graph upgrade journal operationId")
statuses = {"running", "blocked", "complete"}
phases = {
    "prepared",
    "validated",
    "history-recorded",
    "workspace-staged",
    "repository-staged",
    "state-committed",
    "self-tested",
    "checkpointed",
    "pushed",
    "complete",
}
status = journal.get("status")
phase = journal.get("phase")
if status not in statuses or phase not in phases:
    raise SystemExit("invalid graph upgrade journal status or phase")
if (status == "complete") != (phase == "complete"):
    raise SystemExit("graph upgrade journal is only half complete")
if status == "complete" and phase == "complete":
    raise SystemExit(0)
raise SystemExit(42)
PY
if [[ "$journal_status" -eq 42 ]]; then
  upgrade_tool="$WORKSPACE_ROOT/scripts/upgrade-unified-task-graph-v2.py"
  [[ -f "$upgrade_tool" ]] ||
    die "Graph upgrade recovery is required but the upgrade tool is missing"
  "$CMCLIENT_PYTHON3" "$upgrade_tool" --apply
  "$CMCLIENT_PYTHON3" - "$WORKSPACE_ROOT/state/GRAPH_UPGRADE.json" <<'PY'
import json
import sys
from pathlib import Path

journal = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
if journal.get("status") != "complete" or journal.get("phase") != "complete":
    raise SystemExit("graph upgrade recovery returned without completing its journal")
PY
elif [[ "$journal_status" -ne 0 ]]; then
  die "Graph upgrade journal validation failed"
fi

"$WORKSPACE_ROOT/scripts/doctor.sh"

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
        --graph-lock "$SCRIPT_DIR/unified-task-graph-lock.json" \
        --license-provenance "$WORKSPACE_ROOT/state/LICENSE_PROVENANCE.json" \
        --remote "$CMCLIENT_REMOTE" \
        --branch "$CMCLIENT_BRANCH"
    fi
  done
else
  warn "Repository is dirty; sync and automatic checkpoint recovery were skipped"
fi

log "Next executable task:"
"$SCRIPT_DIR/next-task.sh"
