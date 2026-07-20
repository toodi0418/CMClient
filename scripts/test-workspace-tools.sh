#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON_BIN="${CMCLIENT_PYTHON3:-}"

if [[ -z "$PYTHON_BIN" ]]; then
  if command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="python3"
  elif command -v python >/dev/null 2>&1; then
    PYTHON_BIN="python"
  else
    printf 'Python 3 is required for workspace tooling tests\n' >&2
    exit 1
  fi
fi

export PYTHONDONTWRITEBYTECODE=1
export PYTHONUTF8=1

for test_file in \
  task-state-tools.test.py \
  reconcile-task-state.test.py \
  goal-completion-check.test.py \
  workflow-shell.test.py
do
  "$PYTHON_BIN" -B "$SCRIPT_DIR/$test_file"
done
