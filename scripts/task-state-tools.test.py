#!/usr/bin/env python3
"""Focused regression tests for task state and repair workflow tools."""

from __future__ import annotations

import copy
import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from v2_graph_test_fixture import (
    P13_T05_NEW_ACCEPTANCE,
    P13_T10_NEW_ACCEPTANCE,
    p13_t12_definition_amendments,
    write_v2_contract,
)


SCRIPTS = Path(__file__).resolve().parent


def load_library():
    path = SCRIPTS / "task-state-lib.py"
    spec = importlib.util.spec_from_file_location("task_state_lib_test", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load task state library: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


LIB = load_library()


def task(
    task_id: str,
    status: str,
    dependencies: list[str] | None = None,
    *,
    required: bool = True,
    manual: bool = False,
) -> dict:
    return {
        "id": task_id,
        "phase": task_id.split("-", 1)[0],
        "title": task_id,
        "status": status,
        "required": required,
        "manualGate": manual,
        "dependsOn": dependencies or [],
        "kind": "test",
        "scope": "workflow",
        "owner": None,
        "startedAt": None,
        "completedAt": None,
        "commit": None,
        "notes": [],
    }


class TaskStateToolTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.repo_temporary = tempfile.TemporaryDirectory()
        cls.repo = Path(cls.repo_temporary.name) / "repo"
        cls.repo.mkdir()
        commands = (
            ("init", "-b", "dev"),
            ("config", "user.name", "Task State Test"),
            ("config", "user.email", "task-state@example.invalid"),
        )
        for command in commands:
            subprocess.run(
                ["git", "-C", str(cls.repo), *command],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
            )
        (cls.repo / "seed.txt").write_text("seed\n", encoding="utf-8")
        subprocess.run(
            ["git", "-C", str(cls.repo), "add", "seed.txt"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        subprocess.run(
            [
                "git",
                "-C",
                str(cls.repo),
                "-c",
                "commit.gpgsign=false",
                "commit",
                "-m",
                "seed",
            ],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        cls.base_commit = subprocess.run(
            ["git", "-C", str(cls.repo), "rev-parse", "HEAD"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
        ).stdout.strip()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.repo_temporary.cleanup()

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.state_path = self.root / "state" / "TASKS.json"
        self.state_path.parent.mkdir(parents=True)
        self.graph_lock_path = self.root / "unified-task-graph-lock.json"
        self.license_path = self.root / "state" / "LICENSE_PROVENANCE.json"

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def write(
        self,
        tasks: list[dict],
        *,
        definition_amendments: list[dict] | None = None,
        **extra: object,
    ) -> None:
        value = {"schemaVersion": 2, "project": "test", "tasks": tasks, **extra}
        write_v2_contract(
            value,
            state_path=self.state_path,
            graph_lock_path=self.graph_lock_path,
            license_path=self.license_path,
            source_baseline=self.base_commit,
            definition_amendments=definition_amendments,
        )

    def write_value(self, value: dict) -> None:
        self.state_path.write_text(
            json.dumps(value, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

    def read(self) -> dict:
        return json.loads(self.state_path.read_text(encoding="utf-8"))

    def run_script(self, name: str, *arguments: str) -> subprocess.CompletedProcess:
        command = [sys.executable, str(SCRIPTS / name), *arguments]
        if name == "task-state.py" or (
            name == "repair-task.py" and arguments and arguments[0] == "start"
        ):
            command.extend(["--repo", str(self.repo)])
        command.extend(["--state", str(self.state_path)])
        command.extend(
            [
                "--graph-lock",
                str(self.graph_lock_path),
                "--license-provenance",
                str(self.license_path),
            ]
        )
        return subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            check=False,
        )

    def write_p13_t12_amendment_fixture(self) -> None:
        started_at = "2026-07-21T07:55:31+00:00"
        invalidation = {
            "invalidatedAt": started_at,
            "repairOf": "P13-T05",
            "runtimeCandidate": True,
            "distributionCandidate": True,
            "affectedCases": ["TESTABILITY_GATES"],
            "resolvedByCandidate": None,
            "resolvedAt": None,
        }
        parent = task("P13-T05", "blocked", ["P13-T04"])
        parent.update(
            {
                "acceptance": copy.deepcopy(P13_T05_NEW_ACCEPTANCE),
                "candidateReset": True,
                "blockedByRepair": "P13-T12",
                "blockedAt": started_at,
                "blockReason": "audited fixture repair",
            }
        )
        downstream = task("P13-T10", "pending", ["P13-T04"])
        downstream["acceptance"] = copy.deepcopy(P13_T10_NEW_ACCEPTANCE)
        repair = task("P13-T12", "in_progress", ["P13-T04"])
        repair.update(
            {
                "kind": "fix",
                "candidateReset": True,
                "repairOf": "P13-T05",
                "affectedCases": ["TESTABILITY_GATES"],
                "startedAt": started_at,
                "candidateInvalidation": copy.deepcopy(invalidation),
            }
        )
        self.write(
            [task("P13-T04", "done"), parent, downstream, repair],
            definition_amendments=p13_t12_definition_amendments(),
            candidateInvalidations=[
                {"repairTask": "P13-T12", **copy.deepcopy(invalidation)}
            ],
        )

    def rewrite_amendment_contract(self, mutation) -> None:
        state = self.read()
        graph_lock = json.loads(self.graph_lock_path.read_text(encoding="utf-8"))
        mutation(graph_lock["definitionAmendments"][0])
        state["activeGraph"]["definitionAmendments"] = copy.deepcopy(
            graph_lock["definitionAmendments"]
        )
        graph_lock["graphSha256"] = LIB.canonical_sha256(
            {field: graph_lock.get(field) for field in LIB.GRAPH_PAYLOAD_FIELDS}
        )
        self.write_value(state)
        self.graph_lock_path.write_text(
            json.dumps(graph_lock, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

    def mark_reconciled(self, task_id: str) -> str:
        state = self.read()
        target = next(item for item in state["tasks"] if item["id"] == task_id)
        checkpoint = "b" * 40
        self.assertNotEqual(target.get("checkpointBaseCommit"), checkpoint)
        target["status"] = "done"
        target["commit"] = checkpoint
        target["completedAt"] = "2099-01-01T00:00:00+00:00"
        target.setdefault("notes", []).append(
            f"fix(workflow): [{task_id}] reconcile tested repair"
        )
        self.write_value(state)
        return checkpoint

    def test_next_task_preserves_cli_and_no_ready_is_not_completion(self) -> None:
        self.write(
            [
                task("P13-T01", "done"),
                task("P13-T02", "pending", ["P13-T01"]),
            ]
        )
        result = self.run_script("next-task.py")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["id"], "P13-T02")

        self.write(
            [
                task("P13-T01", "blocked"),
                task("P13-T02", "pending", ["P13-T01"]),
                task("P13-T03", "pending", required=False, manual=True),
            ]
        )
        result = self.run_script("next-task.py")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "NO_READY_TASK")
        self.assertTrue(any(item["status"] != "done" for item in self.read()["tasks"]))

    def test_invalid_graph_is_rejected_before_scheduling(self) -> None:
        invalid_graphs = [
            [task("invalid-task", "pending")],
            [task("P13-T01", "pending"), task("P13-T01", "pending")],
            [task("P13-T01", "pending", ["P13-T99"])],
            [task("P13-T01", "pending", ["P13-T02"]), task("P13-T02", "pending", ["P13-T01"])],
            [task("P13-T01", "in_progress"), task("P13-T02", "in_progress")],
            [task("P13-T01", "skipped")],
        ]
        for invalid in invalid_graphs:
            with self.subTest(tasks=invalid):
                self.write(invalid)
                result = self.run_script("next-task.py")
                self.assertEqual(result.returncode, 1)
                self.assertIn("next-task failed:", result.stderr)
                self.assertNotIn("NO_READY_TASK", result.stdout)

    def test_task_state_enforces_dependencies_and_terminal_status(self) -> None:
        self.write(
            [
                task("P13-T01", "done"),
                task("P13-T02", "pending", ["P13-T01"]),
            ]
        )
        result = self.run_script("task-state.py", "P13-T02", "in_progress")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "P13-T02: pending -> in_progress")
        started = self.read()["tasks"][1]
        self.assertIsNotNone(started["startedAt"])
        self.assertEqual(started["checkpointBaseCommit"], self.base_commit)
        before_done_attempt = self.state_path.read_bytes()
        result = self.run_script(
            "task-state.py", "P13-T02", "done", "--commit", "a" * 40
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("reserved for reconcile-task-state.py", result.stderr)
        self.assertEqual(self.state_path.read_bytes(), before_done_attempt)

        result = self.run_script(
            "task-state.py",
            "P13-T02",
            "in_progress",
            "--checkpoint-base-commit",
            "c" * 40,
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("checkpointBaseCommit is immutable", result.stderr)
        result = self.run_script("task-state.py", "P13-T02", "pending")
        self.assertEqual(result.returncode, 0, result.stderr)

        self.write([task("P13-T01", "skipped", required=False)])
        result = self.run_script("task-state.py", "P13-T01", "pending")
        self.assertEqual(result.returncode, 1)
        self.assertIn("terminal task cannot transition", result.stderr)

        self.write([task("P13-T01", "pending")])
        result = self.run_script("task-state.py", "P13-T01", "skipped")
        self.assertEqual(result.returncode, 1)
        self.assertIn("required skipped task is not declared superseded", result.stderr)

        self.write(
            [
                task("P13-T01", "pending"),
                task("P13-T02", "pending", ["P13-T01"]),
            ]
        )
        result = self.run_script("task-state.py", "P13-T02", "in_progress")
        self.assertEqual(result.returncode, 1)
        self.assertIn("unfinished dependencies", result.stderr)

    def test_terminal_idempotence_cannot_rewrite_history(self) -> None:
        completed = task("P13-T01", "done")
        completed.update(
            {
                "checkpointBaseCommit": "a" * 40,
                "commit": "b" * 40,
                "completedAt": "2099-01-01T00:00:00+00:00",
                "notes": ["fix(workflow): [P13-T01] completed"],
            }
        )
        self.write([completed])
        original = self.state_path.read_bytes()
        result = self.run_script("task-state.py", "P13-T01", "done")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.state_path.read_bytes(), original)
        for metadata in (
            ("--commit", "c" * 40),
            ("--note", "rewrite history"),
        ):
            result = self.run_script(
                "task-state.py", "P13-T01", "done", *metadata
            )
            self.assertEqual(result.returncode, 1)
            self.assertIn("terminal task history is immutable", result.stderr)
            self.assertEqual(self.state_path.read_bytes(), original)

        skipped = task("P13-T01", "skipped", required=False)
        skipped["notes"] = ["original skip"]
        self.write([skipped])
        original = self.state_path.read_bytes()
        result = self.run_script(
            "task-state.py", "P13-T01", "skipped", "--note", "rewrite"
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("terminal task history is immutable", result.stderr)
        self.assertEqual(self.state_path.read_bytes(), original)

    def test_checkpoint_base_requires_clean_dev(self) -> None:
        state = {
            "schemaVersion": 2,
            "project": "test",
            "tasks": [task("P13-T01", "pending")],
        }
        with self.assertRaisesRegex(
            LIB.TaskStateError, "checkpointBaseCommit is required"
        ):
            LIB.transition_task(state, "P13-T01", "in_progress")

        dirty_path = self.repo / "untracked.txt"
        dirty_path.write_text("dirty\n", encoding="utf-8")
        try:
            self.write([task("P13-T01", "pending")])
            result = self.run_script(
                "task-state.py", "P13-T01", "in_progress"
            )
            self.assertEqual(result.returncode, 1)
            self.assertIn("requires a clean Repository", result.stderr)

            self.write([task("P13-T01", "in_progress")])
            result = self.run_script(
                "task-state.py",
                "P13-T01",
                "in_progress",
                "--checkpoint-base-commit",
                self.base_commit,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(
                self.read()["tasks"][0]["checkpointBaseCommit"],
                self.base_commit,
            )
            result = self.run_script(
                "repair-task.py",
                "start",
                "P13-T01",
                "--title",
                "Repair dirty parent",
                "--affected-case",
                "FULL_VERIFY",
            )
            self.assertEqual(result.returncode, 1)
            self.assertIn("will not stash, reset, or mix", result.stderr)
            self.assertEqual(dirty_path.read_text(encoding="utf-8"), "dirty\n")
            self.assertEqual(len(self.read()["tasks"]), 1)
        finally:
            dirty_path.unlink(missing_ok=True)

    def test_lock_serializes_concurrent_atomic_updates(self) -> None:
        self.write([task("P13-T01", "pending")])
        base = [
            sys.executable,
            str(SCRIPTS / "task-state.py"),
            "P13-T01",
            "pending",
            "--graph-lock",
            str(self.graph_lock_path),
            "--license-provenance",
            str(self.license_path),
        ]
        processes = [
            subprocess.Popen(
                [*base, "--note", note, "--state", str(self.state_path)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
            )
            for note in ("first", "second")
        ]
        results = [process.communicate(timeout=15) for process in processes]
        self.assertTrue(all(process.returncode == 0 for process in processes), results)
        self.assertCountEqual(self.read()["tasks"][0]["notes"], ["first", "second"])
        self.assertFalse(list(self.state_path.parent.glob(".TASKS.json.*.tmp")))

    def test_v2_graph_and_license_drift_fail_before_scheduling(self) -> None:
        self.write(
            [
                task("P13-T01", "done"),
                task("P13-T02", "pending", ["P13-T01"]),
            ]
        )
        state = self.read()
        state["activeGraph"]["callMeshServiceModel"]["localMappingOverride"] = True
        self.write_value(state)
        result = self.run_script("next-task.py")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("activeGraph.callMeshServiceModel", result.stderr)

        self.write(
            [
                task("P13-T01", "done"),
                task("P13-T02", "pending", ["P13-T01"]),
            ]
        )
        provenance = json.loads(self.license_path.read_text(encoding="utf-8"))
        provenance["publicDevPushPermitted"] = False
        self.license_path.write_text(
            json.dumps(provenance, indent=2) + "\n", encoding="utf-8"
        )
        result = self.run_script("next-task.py")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("license provenance disagrees", result.stderr)

    def test_audited_definition_amendment_binds_repair_and_canonical_values(self) -> None:
        self.write_p13_t12_amendment_fixture()
        state = self.read()
        graph_lock = json.loads(self.graph_lock_path.read_text(encoding="utf-8"))
        license_provenance = json.loads(
            self.license_path.read_text(encoding="utf-8")
        )
        LIB.validate_state_against_graph_lock(state, graph_lock, license_provenance)

        self.rewrite_amendment_contract(
            lambda record: record.__setitem__(
                "oldValueSha256", "0" * 64
            )
        )
        with self.assertRaisesRegex(
            LIB.TaskStateError, "oldValueSha256 is not canonical"
        ):
            LIB.validate_state_against_graph_lock(
                self.read(),
                json.loads(self.graph_lock_path.read_text(encoding="utf-8")),
                license_provenance,
            )

        self.write_p13_t12_amendment_fixture()
        self.rewrite_amendment_contract(
            lambda record: record["evidence"][0].update(
                {
                    "source": "https://example.invalid/not-official",
                }
            )
        )
        with self.assertRaisesRegex(
            LIB.TaskStateError, "not an approved official source"
        ):
            LIB.validate_state_against_graph_lock(
                self.read(),
                json.loads(self.graph_lock_path.read_text(encoding="utf-8")),
                license_provenance,
            )

        self.write_p13_t12_amendment_fixture()
        state = self.read()
        repair = next(item for item in state["tasks"] if item["id"] == "P13-T12")
        repair["status"] = "pending"
        self.write_value(state)
        with self.assertRaisesRegex(
            LIB.TaskStateError, "active or completed P13-T12 repair"
        ):
            LIB.validate_state_against_graph_lock(
                state,
                json.loads(self.graph_lock_path.read_text(encoding="utf-8")),
                license_provenance,
            )

    def test_upgrade_journal_blocks_normal_tools_and_allows_exact_child(self) -> None:
        self.write(
            [
                task("P13-T01", "done"),
                task("P13-T02", "pending", ["P13-T01"]),
            ]
        )
        operation_id = "upgrade-fixture"
        (self.state_path.parent / "GRAPH_UPGRADE.json").write_text(
            json.dumps(
                {
                    "schema": "cmclient-graph-upgrade-journal/v1",
                    "operationId": operation_id,
                    "status": "running",
                    "phase": "state-committed",
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        blocked = self.run_script("next-task.py")
        self.assertNotEqual(blocked.returncode, 0)
        self.assertIn("GRAPH_UPGRADE_IN_PROGRESS", blocked.stderr)

        allowed = self.run_script(
            "next-task.py", "--graph-upgrade-operation-id", operation_id
        )
        self.assertEqual(allowed.returncode, 0, allowed.stderr)
        self.assertEqual(json.loads(allowed.stdout)["id"], "P13-T02")

    def test_corrupt_and_half_complete_upgrade_journals_fail_closed(self) -> None:
        self.write(
            [
                task("P13-T01", "done"),
                task("P13-T02", "pending", ["P13-T01"]),
            ]
        )
        journal_path = self.state_path.parent / "GRAPH_UPGRADE.json"
        invalid_documents = [
            {
                "schema": "cmclient-graph-upgrade-journal/v1",
                "operationId": "",
                "status": "running",
                "phase": "prepared",
            },
            {
                "schema": "cmclient-graph-upgrade-journal/v1",
                "operationId": "upgrade-fixture",
                "status": "invented",
                "phase": "prepared",
            },
            {
                "schema": "cmclient-graph-upgrade-journal/v1",
                "operationId": "upgrade-fixture",
                "status": "complete",
                "phase": "pushed",
            },
            {
                "schema": "cmclient-graph-upgrade-journal/v1",
                "operationId": "upgrade-fixture",
                "status": "running",
                "phase": "complete",
            },
        ]
        for document in invalid_documents:
            with self.subTest(document=document):
                journal_path.write_text(
                    json.dumps(document, indent=2) + "\n", encoding="utf-8"
                )
                result = self.run_script(
                    "next-task.py",
                    "--graph-upgrade-operation-id",
                    "upgrade-fixture",
                )
                self.assertNotEqual(result.returncode, 0)

        journal_path.write_text(
            json.dumps(
                {
                    "schema": "cmclient-graph-upgrade-journal/v1",
                    "operationId": "upgrade-fixture",
                    "status": "complete",
                    "phase": "complete",
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        result = self.run_script("next-task.py")
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_repair_start_blocks_parent_and_invalidates_candidates(self) -> None:
        self.write(
            [
                task("P13-T01", "done"),
                task("P13-T02", "done", ["P13-T01"]),
                task("P13-T03", "in_progress", ["P13-T01", "P13-T02"]),
                task("P13-T04", "pending", ["P13-T03"]),
            ]
        )
        result = self.run_script(
            "repair-task.py",
            "start",
            "P13-T03",
            "--title",
            "Repair observed defect",
            "--affected-case",
            "FULL_VERIFY",
            "--affected-case",
            "LIVE_DATA",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        repair = json.loads(result.stdout)
        self.assertEqual(repair["id"], "P13-T05")
        self.assertEqual(repair["dependsOn"], ["P13-T01", "P13-T02"])
        self.assertNotIn("P13-T03", repair["dependsOn"])
        self.assertEqual(repair["repairOf"], "P13-T03")
        self.assertEqual(repair["status"], "in_progress")

        state = self.read()
        by_id = {item["id"]: item for item in state["tasks"]}
        self.assertEqual(by_id["P13-T03"]["status"], "blocked")
        self.assertEqual(by_id["P13-T03"]["blockedByRepair"], "P13-T05")
        invalidation = state["candidateInvalidations"][-1]
        self.assertEqual(invalidation["repairTask"], "P13-T05")
        self.assertTrue(invalidation["runtimeCandidate"])
        self.assertTrue(invalidation["distributionCandidate"])
        self.assertEqual(invalidation["affectedCases"], ["FULL_VERIFY", "LIVE_DATA"])
        self.assertIsNone(invalidation["resolvedByCandidate"])

        retry = self.run_script(
            "repair-task.py",
            "start",
            "P13-T03",
            "--title",
            "Repair observed defect",
            "--affected-case",
            "FULL_VERIFY",
            "--affected-case",
            "LIVE_DATA",
        )
        self.assertEqual(retry.returncode, 0, retry.stderr)
        self.assertEqual(json.loads(retry.stdout)["id"], "P13-T05")
        self.assertEqual(len(self.read()["candidateInvalidations"]), 1)

    def test_repair_resume_adds_done_dependency_without_cycle(self) -> None:
        self.write(
            [
                task("P13-T01", "done"),
                task("P13-T02", "in_progress", ["P13-T01"]),
            ]
        )
        result = self.run_script(
            "repair-task.py",
            "start",
            "P13-T02",
            "--title",
            "Repair workflow defect",
            "--affected-case",
            "FULL_VERIFY",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        repair_id = json.loads(result.stdout)["id"]
        repair_commit = self.mark_reconciled(repair_id)
        result = self.run_script(
            "repair-task.py", "resume", "P13-T02", repair_id
        )
        self.assertEqual(result.returncode, 0, result.stderr)

        state = self.read()
        by_id = LIB.validate_task_graph(state)
        parent = by_id["P13-T02"]
        self.assertEqual(parent["status"], "in_progress")
        self.assertIn(repair_id, parent["dependsOn"])
        self.assertEqual(parent["lastRepairTask"], repair_id)
        self.assertEqual(parent["checkpointBaseCommit"], repair_commit)
        self.assertEqual(
            parent["checkpointBaseCommit"],
            repair_commit,
            "checkpoint.sh pre-commit base check can now pass",
        )
        self.assertNotIn("blockedByRepair", parent)

        before_retry = self.state_path.read_bytes()
        result = self.run_script(
            "repair-task.py", "resume", "P13-T02", repair_id
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.state_path.read_bytes(), before_retry)

    def test_repair_resume_rejects_non_monotonic_checkpoint_chain(self) -> None:
        parent = task("P13-T02", "in_progress")
        parent["checkpointBaseCommit"] = self.base_commit
        self.write([task("P13-T01", "done"), parent])
        result = self.run_script(
            "repair-task.py",
            "start",
            "P13-T02",
            "--title",
            "Repair chain defect",
            "--affected-case",
            "FULL_VERIFY",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        repair_id = json.loads(result.stdout)["id"]
        self.mark_reconciled(repair_id)
        state = self.read()
        blocked_parent = next(
            item for item in state["tasks"] if item["id"] == "P13-T02"
        )
        blocked_parent["checkpointBaseCommit"] = "c" * 40
        self.write_value(state)
        result = self.run_script(
            "repair-task.py", "resume", "P13-T02", repair_id
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("does not continue parent checkpoint base", result.stderr)

    def test_repair_resume_rejects_unreconciled_done_repair(self) -> None:
        self.write(
            [
                task("P13-T01", "done"),
                task("P13-T02", "in_progress", ["P13-T01"]),
            ]
        )
        result = self.run_script(
            "repair-task.py",
            "start",
            "P13-T02",
            "--title",
            "Repair defect",
            "--affected-case",
            "FULL_VERIFY",
        )
        repair_id = json.loads(result.stdout)["id"]
        state = self.read()
        repair = next(item for item in state["tasks"] if item["id"] == repair_id)
        repair["status"] = "done"
        self.write_value(state)
        result = self.run_script(
            "repair-task.py", "resume", "P13-T02", repair_id
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("full Git object ID", result.stderr)

    def test_repair_requires_nonempty_unique_affected_cases(self) -> None:
        self.write([task("P13-T01", "in_progress")])
        result = self.run_script(
            "repair-task.py", "start", "P13-T01", "--title", "Repair defect"
        )
        self.assertEqual(result.returncode, 2)
        result = self.run_script(
            "repair-task.py",
            "start",
            "P13-T01",
            "--title",
            "Repair defect",
            "--affected-case",
            "",
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("must not be empty", result.stderr)
        result = self.run_script(
            "repair-task.py",
            "start",
            "P13-T01",
            "--title",
            "Repair defect",
            "--affected-case",
            "FULL_VERIFY",
            "--affected-case",
            "FULL_VERIFY",
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("must be unique", result.stderr)
        result = self.run_script(
            "repair-task.py",
            "start",
            "P13-T01",
            "--title",
            "Repair defect",
            "--affected-case",
            "INVENTED_CASE",
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("unknown case IDs", result.stderr)

    def test_candidate_resolution_is_atomic_consistent_and_idempotent(self) -> None:
        self.write(
            [
                task("P13-T01", "done"),
                task("P13-T02", "in_progress", ["P13-T01"]),
            ]
        )
        result = self.run_script(
            "repair-task.py",
            "start",
            "P13-T02",
            "--title",
            "Repair candidate defect",
            "--affected-case",
            "FULL_VERIFY",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        repair_id = json.loads(result.stdout)["id"]
        self.mark_reconciled(repair_id)
        candidate = "A" * 64
        result = self.run_script(
            "repair-task.py",
            "resolve",
            repair_id,
            "--candidate",
            candidate,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        state = self.read()
        repair = next(item for item in state["tasks"] if item["id"] == repair_id)
        ledger = state["candidateInvalidations"][0]
        expected = f"sha256:{candidate.lower()}"
        self.assertEqual(
            repair["candidateInvalidation"]["resolvedByCandidate"], expected
        )
        self.assertEqual(ledger["resolvedByCandidate"], expected)
        self.assertEqual(
            repair["candidateInvalidation"]["resolvedAt"], ledger["resolvedAt"]
        )

        resolved_bytes = self.state_path.read_bytes()
        result = self.run_script(
            "repair-task.py",
            "resolve",
            repair_id,
            "--candidate",
            candidate.lower(),
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.state_path.read_bytes(), resolved_bytes)
        result = self.run_script(
            "repair-task.py",
            "resolve",
            repair_id,
            "--candidate",
            "c" * 64,
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("already resolved", result.stderr)
        self.assertEqual(self.state_path.read_bytes(), resolved_bytes)

        inconsistent = self.read()
        inconsistent["candidateInvalidations"][0]["resolvedByCandidate"] = (
            "sha256:" + "d" * 64
        )
        self.write_value(inconsistent)
        result = self.run_script("next-task.py")
        self.assertEqual(result.returncode, 1)
        self.assertIn("metadata disagrees", result.stderr)

    def test_repair_allocator_fails_after_checkpoint_id_limit(self) -> None:
        tasks = [task(f"P13-T{sequence:02d}", "done") for sequence in range(1, 99)]
        tasks.append(task("P13-T99", "in_progress"))
        state = {"schemaVersion": 2, "project": "test", "tasks": tasks}
        with self.assertRaisesRegex(LIB.TaskStateError, "ID space exhausted"):
            LIB.allocate_repair_id(state, tasks[-1])


if __name__ == "__main__":
    unittest.main(verbosity=2)
