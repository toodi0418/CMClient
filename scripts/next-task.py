#!/usr/bin/env python3
"""Print the active or next required task; never infer Goal completion."""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path


sys.dont_write_bytecode = True


def load_library():
    path = Path(__file__).with_name("task-state-lib.py")
    spec = importlib.util.spec_from_file_location("task_state_lib", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load task state library: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", type=Path)
    return parser.parse_args()


def main() -> int:
    library = load_library()
    args = parse_args()
    state = library.read_validated_state(args.state or library.DEFAULT_STATE_PATH)
    task = library.next_ready_task(state)
    if task is None:
        # This sentinel means only that scheduling cannot currently select work.
        print("NO_READY_TASK")
    else:
        print(json.dumps(task, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError) as error:
        print(f"next-task failed: {error}", file=sys.stderr)
        raise SystemExit(1)
