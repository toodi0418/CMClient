#!/usr/bin/env python3
"""Focused regressions for the graph-v3 Windows scoped-completion contract."""

from __future__ import annotations

import copy
import importlib.util
import json
import os
import sys
import unittest
from pathlib import Path
from unittest import mock


SCRIPTS = Path(__file__).resolve().parent
REPOSITORY = SCRIPTS.parent
WORKSPACE = Path(
    os.environ.get("CMCLIENT_WORKSPACE_ROOT", REPOSITORY.parent.parent)
).resolve()
GRAPH_LOCK = SCRIPTS / "unified-task-graph-lock.json"
STATE = WORKSPACE / "state/TASKS.json"
LICENSE = WORKSPACE / "state/LICENSE_PROVENANCE.json"
HISTORY = WORKSPACE / "state/GRAPH_HISTORY.json"


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load test subject: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


SCOPED = load_module(
    "cmclient_scoped_completion_test_subject", SCRIPTS / "scoped-completion.py"
)
TASK_STATE = load_module(
    "cmclient_task_state_v3_test_subject", SCRIPTS / "task-state-lib.py"
)


NOW = "2026-07-22T12:00:00Z"
EMPTY_ALLOWLIST_SHA256 = SCOPED.completion_tool_allowlist_sha256([])


def digest(character: str) -> str:
    return character * 64


def commit(character: str) -> str:
    return character * 40


def append(ledger: dict, event_type: str, payload: dict) -> dict:
    return SCOPED.append_event(ledger, event_type, payload, now=NOW)


def repair_definition(task_id: str, dependencies: list[str]) -> dict:
    return {
        "id": task_id,
        "phase": "P18",
        "title": f"Repair {task_id}",
        "status": "in_progress",
        "required": True,
        "manualGate": False,
        "lane": "windows-package",
        "priority": 20,
        "dependsOn": dependencies,
        "repairOf": SCOPED.COORDINATOR_TASK,
        "kind": "fix",
        "scope": SCOPED.REPAIR_SCOPE,
        "candidateReset": True,
        "affectedCases": ["FULL_VERIFY", "SOAK"],
        "caseGroups": ["COMP"],
        "owner": None,
        "startedAt": NOW,
        "completedAt": None,
        "commit": None,
        "checkpointBaseCommit": commit("a"),
        "notes": ["candidate-affecting deterministic test failure"],
        "failureClass": SCOPED.FAILURE_CLASS_CANDIDATE,
        "completionProtocol": SCOPED.PROTOCOL_MARKER,
    }


def attempt_definition(
    task_id: str, attempt_number: int, dependencies: list[str]
) -> dict:
    return {
        "id": task_id,
        "phase": "P18",
        "title": f"Retry attempt {attempt_number}",
        "status": "in_progress",
        "required": True,
        "manualGate": False,
        "lane": "windows-package",
        "priority": 20,
        "dependsOn": dependencies,
        "kind": "release",
        "scope": SCOPED.ATTEMPT_SCOPE,
        "candidateReset": False,
        "caseGroups": ["COMP"],
        "owner": None,
        "startedAt": NOW,
        "completedAt": None,
        "commit": None,
        "checkpointBaseCommit": commit("b"),
        "notes": [],
        "completionAttemptNumber": attempt_number,
        "completionProtocol": SCOPED.PROTOCOL_MARKER,
    }


def prepare_attempt(
    ledger: dict,
    *,
    task_id: str,
    attempt_number: int,
    candidate: str,
    precheck: str,
    manifest: str,
) -> dict:
    projection = SCOPED.validate_ledger(ledger)
    return append(
        ledger,
        "attempt_prepared",
        {
            "attemptTask": task_id,
            "attemptNumber": attempt_number,
            "priorPushedHead": (
                projection["head"]["eventHash"]
                if projection["head"] is not None
                else None
            ),
            "candidateIdentity": f"sha256:{candidate}",
            "precheckSha256": precheck,
            "intendedManifestSha256": manifest,
        },
    )


def push_attempt(
    ledger: dict,
    *,
    task_id: str,
    attempt_number: int,
    commit_id: str,
    manifest: str,
    candidate: str,
) -> dict:
    projection = SCOPED.validate_ledger(ledger, strict_head=False)
    return append(
        ledger,
        "attempt_pushed",
        {
            "attemptTask": task_id,
            "attemptNumber": attempt_number,
            "commit": commit_id,
            "originCommit": commit_id,
            "manifestSha256": manifest,
            "preparedEventHash": projection["prepared"]["eventHash"],
            "candidateIdentity": f"sha256:{candidate}",
        },
    )


def fail_and_allocate(
    ledger: dict,
    *,
    attempt_task: str,
    attempt_number: int,
    commit_id: str,
    repair_task: str,
    dependencies: list[str],
) -> None:
    append(
        ledger,
        "attempt_failed",
        {
            "attemptTask": attempt_task,
            "attemptNumber": attempt_number,
            "commit": commit_id,
            "failureClass": SCOPED.FAILURE_CLASS_CANDIDATE,
            "requestedClass": SCOPED.FAILURE_CLASS_TOOL,
            "candidateReset": True,
            "changedPaths": ["scripts/scoped-completion.py"],
            "allowlistSha256": EMPTY_ALLOWLIST_SHA256,
            "classificationReason": "the completion-tool-only allowlist is empty",
        },
    )
    append(
        ledger,
        "repair_allocated",
        {
            "repairTask": repair_task,
            "failedAttemptTask": attempt_task,
            "failedAttemptNumber": attempt_number,
            "candidateReset": True,
            "failureClass": SCOPED.FAILURE_CLASS_CANDIDATE,
            "taskDefinition": repair_definition(repair_task, dependencies),
        },
    )


def push_repair(
    ledger: dict,
    *,
    repair_task: str,
    repair_commit: str,
    attempt_task: str,
    attempt_number: int,
    dependencies: list[str],
) -> None:
    append(
        ledger,
        "repair_pushed",
        {
            "repairTask": repair_task,
            "commit": repair_commit,
            "originCommit": repair_commit,
            "nextAttemptTask": attempt_task,
            "nextAttemptNumber": attempt_number,
            "attemptDefinition": attempt_definition(
                attempt_task, attempt_number, dependencies
            ),
        },
    )


class ScopedCompletionContractTests(unittest.TestCase):
    def test_sensitive_event_payload_fields_fail_closed(self) -> None:
        valid = {
            "attemptTask": SCOPED.COORDINATOR_TASK,
            "attemptNumber": 1,
            "priorPushedHead": None,
            "candidateIdentity": f"sha256:{digest('a')}",
            "precheckSha256": digest("b"),
            "intendedManifestSha256": digest("c"),
        }
        SCOPED._validate_event_payload("attempt_prepared", valid)

        leaked = {**valid, "apiKey": "not-a-real-key"}
        with self.assertRaisesRegex(
            SCOPED.ScopedCompletionError, "payload fields are invalid"
        ):
            SCOPED._validate_event_payload("attempt_prepared", leaked)

        with self.assertRaisesRegex(
            SCOPED.ScopedCompletionError, "sensitive field"
        ):
            SCOPED._reject_sensitive_payload(
                {"taskDefinition": {"rawPacket": "fixture-data"}}
            )

    def test_graph_locked_empty_allowlist_forces_candidate_reset(self) -> None:
        graph = json.loads(GRAPH_LOCK.read_text(encoding="utf-8"))
        locked = graph["completionToolOnlyRepairAllowlist"]
        self.assertEqual(locked["paths"], [])
        self.assertEqual(locked["sha256"], EMPTY_ALLOWLIST_SHA256)

        with mock.patch.object(
            SCOPED,
            "_contract_paths",
            return_value=(GRAPH_LOCK, LICENSE, HISTORY, REPOSITORY),
        ):
            classification = SCOPED.classify_failure(
                SCOPED.FAILURE_CLASS_TOOL,
                changed_paths=["scripts/scoped-completion.py"],
                completion_tool_allowlist=[],
                locked_allowlist_sha256=locked["sha256"],
                frozen_allowlist_sha256=locked["sha256"],
                exclusions_proven=True,
            )
            with self.assertRaisesRegex(
                SCOPED.ScopedCompletionError, "caller allowlist differs"
            ):
                SCOPED.classify_failure(
                    SCOPED.FAILURE_CLASS_TOOL,
                    changed_paths=["scripts/scoped-completion.py"],
                    completion_tool_allowlist=["scripts/scoped-completion.py"],
                    locked_allowlist_sha256=locked["sha256"],
                    frozen_allowlist_sha256=locked["sha256"],
                    exclusions_proven=True,
                )

        self.assertEqual(
            classification["classification"], SCOPED.FAILURE_CLASS_CANDIDATE
        )
        self.assertTrue(classification["candidateReset"])
        self.assertFalse(classification["toolOnlyApproved"])
        self.assertIn("allowlist is empty", classification["reason"])

    def test_two_failures_allocate_unique_pairs_then_attempt_three_passes(self) -> None:
        ledger = SCOPED.new_ledger()

        prepare_attempt(
            ledger,
            task_id="P18-T10",
            attempt_number=1,
            candidate=digest("1"),
            precheck=digest("2"),
            manifest=digest("3"),
        )
        push_attempt(
            ledger,
            task_id="P18-T10",
            attempt_number=1,
            commit_id=commit("a"),
            manifest=digest("3"),
            candidate=digest("1"),
        )
        fail_and_allocate(
            ledger,
            attempt_task="P18-T10",
            attempt_number=1,
            commit_id=commit("a"),
            repair_task="P18-T20",
            dependencies=["P18-T09"],
        )
        push_repair(
            ledger,
            repair_task="P18-T20",
            repair_commit=commit("b"),
            attempt_task="P18-T21",
            attempt_number=2,
            dependencies=["P18-T09", "P18-T20"],
        )

        prepare_attempt(
            ledger,
            task_id="P18-T21",
            attempt_number=2,
            candidate=digest("4"),
            precheck=digest("5"),
            manifest=digest("6"),
        )
        push_attempt(
            ledger,
            task_id="P18-T21",
            attempt_number=2,
            commit_id=commit("c"),
            manifest=digest("6"),
            candidate=digest("4"),
        )
        fail_and_allocate(
            ledger,
            attempt_task="P18-T21",
            attempt_number=2,
            commit_id=commit("c"),
            repair_task="P18-T22",
            dependencies=["P18-T09", "P18-T20", "P18-T21"],
        )
        push_repair(
            ledger,
            repair_task="P18-T22",
            repair_commit=commit("d"),
            attempt_task="P18-T23",
            attempt_number=3,
            dependencies=["P18-T09", "P18-T20", "P18-T21", "P18-T22"],
        )

        prepare_attempt(
            ledger,
            task_id="P18-T23",
            attempt_number=3,
            candidate=digest("7"),
            precheck=digest("8"),
            manifest=digest("9"),
        )
        push_attempt(
            ledger,
            task_id="P18-T23",
            attempt_number=3,
            commit_id=commit("e"),
            manifest=digest("9"),
            candidate=digest("7"),
        )
        head = SCOPED.validate_ledger(ledger)["head"]
        append(
            ledger,
            "attempt_passed",
            {
                "attemptTask": "P18-T23",
                "attemptNumber": 3,
                "commit": commit("e"),
            },
        )
        append(
            ledger,
            "coordinator_completed",
            {
                "coordinatorTask": SCOPED.COORDINATOR_TASK,
                "headEventHash": head["eventHash"],
            },
        )

        projection = SCOPED.validate_ledger(ledger)
        self.assertEqual(projection["phase"], "complete")
        self.assertEqual(projection["coordinatorStatus"], "done")
        self.assertEqual(
            projection["definitionOrder"],
            ["P18-T20", "P18-T21", "P18-T22", "P18-T23"],
        )
        self.assertEqual(
            projection["coordinatorDependencies"],
            ["P18-T20", "P18-T21", "P18-T22", "P18-T23"],
        )
        self.assertEqual(projection["head"]["attemptTask"], "P18-T23")
        self.assertEqual(projection["head"]["attemptNumber"], 3)
        self.assertEqual(len(projection["commits"]), 5)

        tampered = copy.deepcopy(ledger)
        tampered["events"][9]["payload"]["nextAttemptTask"] = "P18-T21"
        tampered["events"][9]["eventHash"] = SCOPED._event_hash(
            tampered["events"][9]
        )
        with self.assertRaises(SCOPED.ScopedCompletionError):
            SCOPED.validate_ledger(tampered)

    def test_v3_contract_validates_and_preserves_global_p17_root(self) -> None:
        graph = json.loads(GRAPH_LOCK.read_text(encoding="utf-8"))
        state = json.loads(STATE.read_text(encoding="utf-8"))
        license_provenance = json.loads(LICENSE.read_text(encoding="utf-8"))

        TASK_STATE.validate_state_against_graph_lock(
            state, graph, license_provenance
        )
        self.assertEqual(graph["schema"], TASK_STATE.GRAPH_LOCK_SCHEMA_V3)
        self.assertEqual(graph["completionTask"], "P17-T07")
        self.assertEqual(graph["completionChecker"]["task"], "P17-T07")
        self.assertEqual(
            graph["completionCheckers"]["global"]["requiredActiveRoot"]
            ["completionChecker"],
            graph["completionChecker"],
        )
        self.assertEqual(
            graph["completionCheckers"]["windowsLiveFirst"]["task"],
            "P18-T10",
        )

        tampered = copy.deepcopy(graph)
        tampered["completionCheckers"]["global"]["requiredActiveRoot"][
            "completionChecker"
        ]["task"] = "P18-T10"
        with self.assertRaisesRegex(
            TASK_STATE.TaskStateError, "not preserved byte-for-byte"
        ):
            TASK_STATE._validate_v3_completion_contract(tampered)


if __name__ == "__main__":
    unittest.main(verbosity=2)
