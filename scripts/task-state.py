#!/usr/bin/env python3
"""Update one task while preserving the original positional CLI."""

from __future__ import annotations

import argparse
import importlib.util
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
    parser.add_argument("task")
    parser.add_argument(
        "status", choices=["pending", "in_progress", "blocked", "done", "skipped"]
    )
    parser.add_argument("--commit")
    parser.add_argument("--note")
    parser.add_argument("--state", type=Path)
    parser.add_argument("--repo", type=Path)
    parser.add_argument("--checkpoint-base-commit")
    return parser.parse_args()


def main() -> int:
    library = load_library()
    args = parse_args()
    state_path = args.state or library.DEFAULT_STATE_PATH

    def mutation(state: dict) -> tuple[str, str]:
        checkpoint_base = args.checkpoint_base_commit
        if args.status == "in_progress":
            by_id = library.validate_task_graph(state)
            task = by_id.get(args.task)
            if task is None:
                raise library.TaskStateError(f"unknown task: {args.task}")
            entering = task.get("status") != "in_progress"
            missing_base = task.get("checkpointBaseCommit") is None
            if entering:
                repository_base = library.repository_checkpoint_base(
                    args.repo or library.DEFAULT_REPO_PATH
                )
                if checkpoint_base is not None:
                    asserted_base = library.normalize_git_object(
                        checkpoint_base,
                        "asserted checkpointBaseCommit",
                    )
                    if asserted_base != repository_base:
                        raise library.TaskStateError(
                            "asserted checkpointBaseCommit differs from clean dev HEAD"
                        )
                checkpoint_base = repository_base
            elif missing_base and checkpoint_base is None:
                checkpoint_base = library.repository_checkpoint_base(
                    args.repo or library.DEFAULT_REPO_PATH
                )
        return library.transition_task(
            state,
            args.task,
            args.status,
            commit=args.commit,
            note=args.note,
            checkpoint_base_commit=checkpoint_base,
        )

    _, (old_status, new_status) = library.mutate_state(state_path, mutation)
    print(f"{args.task}: {old_status} -> {new_status}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError) as error:
        print(f"task-state failed: {error}", file=sys.stderr)
        raise SystemExit(1)
