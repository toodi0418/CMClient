#!/usr/bin/env python3
"""Allocate and resume required product-defect repair tasks."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
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
    parser.add_argument("--repo", type=Path)
    parser.add_argument("--graph-lock", type=Path)
    parser.add_argument("--license-provenance", type=Path)
    parser.add_argument("--graph-upgrade-operation-id")
    commands = parser.add_subparsers(dest="command", required=True)

    start = commands.add_parser("start")
    start.add_argument("parent")
    start.add_argument("--title", required=True)
    start.add_argument("--note")
    start.add_argument("--affected-case", action="append", required=True)
    start.add_argument("--scope")
    start.add_argument("--checkpoint-base-commit")
    start.add_argument("--state", dest="command_state", type=Path)
    start.add_argument("--repo", dest="command_repo", type=Path)
    start.add_argument("--graph-lock", dest="command_graph_lock", type=Path)
    start.add_argument(
        "--license-provenance", dest="command_license_provenance", type=Path
    )
    start.add_argument(
        "--graph-upgrade-operation-id", dest="command_graph_upgrade_operation_id"
    )

    resume = commands.add_parser("resume")
    resume.add_argument("parent")
    resume.add_argument("repair")
    resume.add_argument("--note")
    resume.add_argument("--state", dest="command_state", type=Path)
    resume.add_argument("--graph-lock", dest="command_graph_lock", type=Path)
    resume.add_argument(
        "--license-provenance", dest="command_license_provenance", type=Path
    )
    resume.add_argument(
        "--graph-upgrade-operation-id", dest="command_graph_upgrade_operation_id"
    )

    resolve = commands.add_parser("resolve")
    resolve.add_argument("repair")
    resolve.add_argument(
        "--candidate",
        "--candidate-identity",
        dest="candidate_identity",
        required=True,
    )
    resolve.add_argument("--state", dest="command_state", type=Path)
    resolve.add_argument("--graph-lock", dest="command_graph_lock", type=Path)
    resolve.add_argument(
        "--license-provenance", dest="command_license_provenance", type=Path
    )
    resolve.add_argument(
        "--graph-upgrade-operation-id", dest="command_graph_upgrade_operation_id"
    )
    return parser.parse_args()


def main() -> int:
    library = load_library()
    args = parse_args()
    state_path = args.command_state or args.state or library.DEFAULT_STATE_PATH
    graph_lock_path = (
        args.command_graph_lock or args.graph_lock or library.DEFAULT_GRAPH_LOCK_PATH
    )
    license_path = args.command_license_provenance or args.license_provenance
    operation_id = (
        args.command_graph_upgrade_operation_id
        or args.graph_upgrade_operation_id
        or os.environ.get("CMCLIENT_GRAPH_UPGRADE_OPERATION_ID")
    )

    if args.command == "start":
        checkpoint_base = library.repository_checkpoint_base(
            args.command_repo or args.repo or library.DEFAULT_REPO_PATH
        )
        if args.checkpoint_base_commit is not None:
            asserted_base = library.normalize_git_object(
                args.checkpoint_base_commit,
                "asserted checkpointBaseCommit",
            )
            if asserted_base != checkpoint_base:
                raise library.TaskStateError(
                    "asserted checkpointBaseCommit differs from clean dev HEAD"
                )

        def mutation(state: dict) -> dict:
            return library.start_repair(
                state,
                args.parent,
                title=args.title,
                note=args.note,
                affected_cases=args.affected_case,
                scope=args.scope,
                checkpoint_base_commit=checkpoint_base,
            )
    elif args.command == "resume":
        def mutation(state: dict) -> dict:
            return library.resume_parent_after_repair(
                state,
                args.parent,
                args.repair,
                note=args.note,
            )
    else:
        def mutation(state: dict) -> dict:
            return library.resolve_candidate_invalidation(
                state,
                args.repair,
                args.candidate_identity,
            )

    _, task = library.mutate_state(
        state_path,
        mutation,
        graph_lock_path=graph_lock_path,
        license_path=license_path,
        graph_upgrade_operation_id=operation_id,
    )
    print(json.dumps(task, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError) as error:
        print(f"repair-task failed: {error}", file=sys.stderr)
        raise SystemExit(1)
