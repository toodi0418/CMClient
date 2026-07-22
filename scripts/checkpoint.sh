#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

TASK=""; TYPE=""; SCOPE=""; SUMMARY=""; SKIP_VERIFY=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --task) TASK="${2:-}"; shift 2;;
    --type) TYPE="${2:-}"; shift 2;;
    --scope) SCOPE="${2:-}"; shift 2;;
    --summary) SUMMARY="${2:-}"; shift 2;;
    --skip-verify) SKIP_VERIFY=1; shift;;
    *) die "Unknown argument: $1";;
  esac
done

[[ "$TASK" =~ ^P[0-9]{2}-T[0-9]{2}[a-z]?$ ]] || die "Invalid --task (example: P13-T02)"
[[ "$TYPE" =~ ^(feat|fix|refactor|test|docs|build|ci|chore|perf|security|release)$ ]] || die "Invalid --type: $TYPE"
[[ "$SCOPE" =~ ^[a-z0-9-]+$ ]] || die "Invalid --scope: $SCOPE"
[[ -n "$SUMMARY" ]] || die "--summary is required"
[[ "$SKIP_VERIFY" -eq 0 ]] || die "--skip-verify is disabled for the active unified-product graph"

require_repo
require_dev_branch
require_python3

SUBJECT="$TYPE($SCOPE): [$TASK] $SUMMARY"
[[ ${#SUBJECT} -le 100 ]] || die "Commit subject is too long (${#SUBJECT})"

has_checkpoint_subject() {
  git -C "$REPO_DIR" log --all --format='%s' |
    grep -E "^(feat|fix|refactor|test|docs|build|ci|chore|perf|security|release)\([a-z0-9-]+\): \[$TASK\] .+" >/dev/null 2>&1
}

reconcile_checkpoint() {
  local defer_scoped
  local reconcile_args=(
    "$TASK"
    --repo "$REPO_DIR"
    --state "$WORKSPACE_ROOT/state/TASKS.json"
    --commits "$WORKSPACE_ROOT/state/COMMITS.md"
    --remote "$CMCLIENT_REMOTE"
    --branch "$CMCLIENT_BRANCH"
    --graph-lock "$SCRIPT_DIR/unified-task-graph-lock.json"
    --license-provenance "$WORKSPACE_ROOT/state/LICENSE_PROVENANCE.json"
  )
  if [[ -n "${CMCLIENT_GRAPH_UPGRADE_OPERATION_ID:-}" ]]; then
    reconcile_args+=(--graph-upgrade-operation-id "$CMCLIENT_GRAPH_UPGRADE_OPERATION_ID")
  fi
  if [[ "$CMCLIENT_AUTO_PUSH" == "1" ]]; then
    reconcile_args+=(--push-local)
  fi
  defer_scoped="$("$CMCLIENT_PYTHON3" - "$WORKSPACE_ROOT/state/TASKS.json" "$TASK" <<'PY'
import json
import sys

state = json.load(open(sys.argv[1], encoding="utf-8"))
task = next((item for item in state.get("tasks", []) if item.get("id") == sys.argv[2]), None)
marker = "cmclient-windows-scoped-completion-attempts/v1"
print("1" if isinstance(task, dict) and (task.get("id") == "P18-T10" or task.get("completionProtocol") == marker) else "0")
PY
)"
  if [[ "$defer_scoped" == "1" ]]; then
    reconcile_args+=(--defer-scoped-completion-terminal)
  fi
  "$CMCLIENT_PYTHON3" "$SCRIPT_DIR/reconcile-task-state.py" "${reconcile_args[@]}"
}

collect_changed_paths() {
  local path existing found
  CHANGED_PATHS=()
  while IFS= read -r -d '' path; do
    found=0
    for existing in "${CHANGED_PATHS[@]}"; do
      if [[ "$existing" == "$path" ]]; then
        found=1
        break
      fi
    done
    if [[ "$found" -eq 0 ]]; then
      CHANGED_PATHS+=("$path")
    fi
  done < <(
    git -C "$REPO_DIR" diff --no-renames --name-only -z --
    git -C "$REPO_DIR" diff --cached --no-renames --name-only -z --
    git -C "$REPO_DIR" ls-files --others --exclude-standard -z --
  )
}

stage_intended_paths() {
  local path
  for path in "${INTENDED_PATHS[@]}"; do
    if [[ -e "$REPO_DIR/$path" || -L "$REPO_DIR/$path" ]]; then
      git -C "$REPO_DIR" add -- "$path"
    else
      git -C "$REPO_DIR" update-index --remove -- "$path"
    fi
  done
}

if [[ -z "$(git -C "$REPO_DIR" status --porcelain)" ]]; then
  if has_checkpoint_subject; then
    reconcile_checkpoint
    log "Recovered existing checkpoint for $TASK without creating a duplicate commit"
    exit 0
  fi
  die "Repository is clean and no recoverable checkpoint exists for $TASK"
fi

PRE_COMMIT_HEAD="$(git -C "$REPO_DIR" rev-parse HEAD)"
WORKFLOW_VALIDATION_ARGS=(
  "$TASK"
  --validate-checkpoint
  --expected-head "$PRE_COMMIT_HEAD"
  --state "$WORKSPACE_ROOT/state/TASKS.json"
  --graph-lock "$SCRIPT_DIR/unified-task-graph-lock.json"
  --license-provenance "$WORKSPACE_ROOT/state/LICENSE_PROVENANCE.json"
)
if [[ -n "${CMCLIENT_GRAPH_UPGRADE_OPERATION_ID:-}" ]]; then
  WORKFLOW_VALIDATION_ARGS+=(
    --graph-upgrade-operation-id "$CMCLIENT_GRAPH_UPGRADE_OPERATION_ID"
  )
fi
PRE_VERIFY_WORKFLOW="$("$CMCLIENT_PYTHON3" "$SCRIPT_DIR/task-state.py" "${WORKFLOW_VALIDATION_ARGS[@]}")"

if git -C "$REPO_DIR" log --all --format='%s' | grep -Fx -- "$SUBJECT" >/dev/null 2>&1; then
  die "Commit subject already exists: $SUBJECT"
fi
if has_checkpoint_subject; then
  die "Task ID already has a checkpoint commit: $TASK"
fi

collect_changed_paths
INTENDED_PATHS=("${CHANGED_PATHS[@]}")
[[ "${#INTENDED_PATHS[@]}" -gt 0 ]] || die "Repository has no checkpoint paths"

stage_intended_paths
"$WORKSPACE_ROOT/scripts/secret-scan.sh"
"$WORKSPACE_ROOT/scripts/verify.sh"

[[ "$(git -C "$REPO_DIR" rev-parse HEAD)" == "$PRE_COMMIT_HEAD" ]] ||
  die "Repository HEAD changed during checkpoint verification"
POST_VERIFY_WORKFLOW="$("$CMCLIENT_PYTHON3" "$SCRIPT_DIR/task-state.py" "${WORKFLOW_VALIDATION_ARGS[@]}")"
[[ "$POST_VERIFY_WORKFLOW" == "$PRE_VERIFY_WORKFLOW" ]] ||
  die "Task state, graph lock, or license provenance changed during checkpoint verification"

collect_changed_paths
NEW_PATHS=()
for path in "${CHANGED_PATHS[@]}"; do
  found=0
  for existing in "${INTENDED_PATHS[@]}"; do
    if [[ "$existing" == "$path" ]]; then
      found=1
      break
    fi
  done
  if [[ "$found" -eq 0 ]]; then
    NEW_PATHS+=("$path")
  fi
done
if [[ "${#NEW_PATHS[@]}" -gt 0 ]]; then
  warn "Verification introduced changed paths outside the checkpoint:"
  printf '  %s\n' "${NEW_PATHS[@]}" >&2
  die "Refusing to expand the checkpoint path set after verification"
fi

stage_intended_paths
if git -C "$REPO_DIR" diff --cached --quiet; then
  die "Staging is empty after verification"
fi

FILES="$(git -C "$REPO_DIR" diff --cached --name-only | sed -n '1,30p')"
STAT="$(git -C "$REPO_DIR" diff --cached --stat | tail -n 1)"
NOW="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
BODY="Task: $TASK
Validation: passed
Change: $STAT
Checkpoint-Time: $NOW

Files:
$FILES"

git -C "$REPO_DIR" commit -m "$SUBJECT" -m "$BODY"
COMMIT="$(git -C "$REPO_DIR" rev-parse HEAD)"

if [[ "$CMCLIENT_AUTO_PUSH" != "1" ]]; then
  warn "CMCLIENT_AUTO_PUSH=0; local checkpoint retained in progress (exit 20)"
  exit 20
fi

if ! git -C "$REPO_DIR" push "$CMCLIENT_REMOTE" "HEAD:$CMCLIENT_BRANCH"; then
  task_state_args=(
    "$TASK" blocked
    --commit "$COMMIT"
    --note "Checkpoint commit exists locally; push failed and must be retried without a new commit"
    --state "$WORKSPACE_ROOT/state/TASKS.json"
    --graph-lock "$SCRIPT_DIR/unified-task-graph-lock.json"
    --license-provenance "$WORKSPACE_ROOT/state/LICENSE_PROVENANCE.json"
  )
  if [[ -n "${CMCLIENT_GRAPH_UPGRADE_OPERATION_ID:-}" ]]; then
    task_state_args+=(
      --graph-upgrade-operation-id "$CMCLIENT_GRAPH_UPGRADE_OPERATION_ID"
    )
  fi
  "$CMCLIENT_PYTHON3" "$SCRIPT_DIR/task-state.py" "${task_state_args[@]}"
  "$CMCLIENT_PYTHON3" - "$WORKSPACE_ROOT/state/HANDOVER.md" "$NOW" "$TASK" "$COMMIT" "$SUBJECT" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
entry = (
    f"\n## Push Blocker {sys.argv[2]}\n"
    f"- Task: {sys.argv[3]}\n"
    f"- Local commit: {sys.argv[4]}\n"
    f"- Subject: {sys.argv[5]}\n"
    "- Action: retry the same SHA after resolving auth/network/non-fast-forward; never force push.\n"
)
with path.open("a", encoding="utf-8", newline="\n") as handle:
    handle.write(entry)
PY
  die "Commit created but push failed; state records the recoverable blocker"
fi

reconcile_checkpoint
log "Checkpoint succeeded: $SUBJECT"
log "Pushed: $CMCLIENT_REMOTE/$CMCLIENT_BRANCH @ $COMMIT"
