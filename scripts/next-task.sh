#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/lib/common.sh"
require_python3
"$CMCLIENT_PYTHON3" "$WORKSPACE_ROOT/scripts/next-task.py"
