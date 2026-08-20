#!/usr/bin/env python3

from __future__ import annotations

import json
import importlib.util
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from v2_graph_test_fixture import write_v2_contract


SCRIPT = Path(__file__).with_name("reconcile-task-state.py")
TASK = "P13-T02"
MODULE_NAME = "cmclient_reconcile_task_state_test_subject"
MODULE_SPEC = importlib.util.spec_from_file_location(MODULE_NAME, SCRIPT)
assert MODULE_SPEC is not None and MODULE_SPEC.loader is not None
MODULE = importlib.util.module_from_spec(MODULE_SPEC)
sys.modules[MODULE_NAME] = MODULE
MODULE_SPEC.loader.exec_module(MODULE)


class ReconcileFixture:
    def __init__(self, root: Path):
        self.root = root
        self.remote = root / "remote.git"
        self.repo = root / "repo"
        self.state = root / "state" / "TASKS.json"
        self.commits = root / "state" / "COMMITS.md"
        self.graph_lock = root / "unified-task-graph-lock.json"
        self.license_provenance = root / "state" / "LICENSE_PROVENANCE.json"
        self.git("init", "--bare", str(self.remote), cwd=root)
        self.git("init", str(self.repo), cwd=root)
        self.git("config", "user.name", "CMClient Test", cwd=self.repo)
        self.git("config", "user.email", "cmclient-test@example.invalid", cwd=self.repo)
        self.git("checkout", "-b", "dev", cwd=self.repo)
        (self.repo / "base.txt").write_text("base\n", encoding="utf-8")
        self.git("add", "base.txt", cwd=self.repo)
        self.git("commit", "-m", "test(repo): establish baseline", cwd=self.repo)
        self.git("remote", "add", "origin", str(self.remote), cwd=self.repo)
        self.git("push", "-u", "origin", "dev", cwd=self.repo)
        self.git("symbolic-ref", "HEAD", "refs/heads/dev", cwd=self.remote)
        self.base = self.git("rev-parse", "HEAD", cwd=self.repo)
        self.write_state()
        self.commits.parent.mkdir(parents=True, exist_ok=True)
        self.commits.write_text(
            "# Checkpoint Commit Log\n\n"
            "| Time | Task | Commit | Subject | Verification |\n"
            "|---|---|---|---|---|\n",
            encoding="utf-8",
        )

    @staticmethod
    def git(*args: str, cwd: Path) -> str:
        result = subprocess.run(
            ["git", *args],
            cwd=cwd,
            check=True,
            env={
                **os.environ,
                "GIT_AUTHOR_NAME": "CMClient Test",
                "GIT_AUTHOR_EMAIL": "cmclient-test@example.invalid",
                "GIT_COMMITTER_NAME": "CMClient Test",
                "GIT_COMMITTER_EMAIL": "cmclient-test@example.invalid",
                "GIT_TERMINAL_PROMPT": "0",
            },
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
        )
        return result.stdout.strip()

    def reset(self) -> None:
        for name in ("other", "ahead"):
            shutil.rmtree(self.root / name, ignore_errors=True)
        self.git("update-ref", "refs/heads/dev", self.base, cwd=self.remote)
        self.git("checkout", "-f", "-B", "dev", self.base, cwd=self.repo)
        self.git("clean", "-fdx", cwd=self.repo)
        self.git("update-ref", "refs/remotes/origin/dev", self.base, cwd=self.repo)
        work = subprocess.run(
            ["git", "show-ref", "--verify", "--quiet", "refs/heads/work"],
            cwd=self.repo,
        )
        if work.returncode == 0:
            self.git("branch", "-D", "work", cwd=self.repo)
        self.write_state()
        self.commits.write_text(
            "# Checkpoint Commit Log\n\n"
            "| Time | Task | Commit | Subject | Verification |\n"
            "|---|---|---|---|---|\n",
            encoding="utf-8",
        )

    def write_state(
        self,
        *,
        status: str = "in_progress",
        commit: str | None = None,
        dependency_status: str = "done",
    ) -> None:
        self.state.parent.mkdir(parents=True, exist_ok=True)
        payload = {
                    "schemaVersion": 2,
                    "tasks": [
                        {
                            "id": "P13-T01",
                            "status": dependency_status,
                            "commit": "1" * 40 if dependency_status == "done" else None,
                            "startedAt": "2026-07-19T00:00:00+00:00",
                            "completedAt": (
                                "2026-07-19T01:00:00+00:00"
                                if dependency_status == "done"
                                else None
                            ),
                            "dependsOn": [],
                            "notes": [],
                        },
                        {
                            "id": TASK,
                            "status": status,
                            "commit": commit,
                            "checkpointBaseCommit": self.base,
                            "startedAt": "2026-07-20T00:00:00+00:00",
                            "completedAt": None,
                            "dependsOn": ["P13-T01"],
                            "notes": [],
                        }
                    ],
                }
        write_v2_contract(
            payload,
            state_path=self.state,
            graph_lock_path=self.graph_lock,
            license_path=self.license_provenance,
            source_baseline=self.base,
            origin=str(self.remote),
        )

    def checkpoint(
        self,
        *,
        task: str = TASK,
        body_task: str | None = None,
        validation: str = "passed",
        summary: str = "recover checkpoint state",
        filename: str = "change.txt",
    ) -> str:
        path = self.repo / filename
        path.write_text(path.read_text(encoding="utf-8") + "x\n" if path.exists() else "x\n", encoding="utf-8")
        self.git("add", filename, cwd=self.repo)
        body_task = body_task if body_task is not None else task
        self.git(
            "commit",
            "-m",
            f"build(workflow): [{task}] {summary}",
            "-m",
            f"Task: {body_task}\nValidation: {validation}\nChange: fixture",
            cwd=self.repo,
        )
        return self.git("rev-parse", "HEAD", cwd=self.repo)

    def advance_remote(self, parent: str, message: str) -> str:
        tree = self.git("rev-parse", f"{parent}^{{tree}}", cwd=self.remote)
        commit = self.git(
            "commit-tree", tree, "-p", parent, "-m", message, cwd=self.remote
        )
        self.git("update-ref", "refs/heads/dev", commit, cwd=self.remote)
        return commit

    def run(self, *extra: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                TASK,
                "--repo",
                str(self.repo),
                "--state",
                str(self.state),
                "--commits",
                str(self.commits),
                "--remote",
                "origin",
                "--branch",
                "dev",
                "--graph-lock",
                str(self.graph_lock),
                "--license-provenance",
                str(self.license_provenance),
                *extra,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
        )

    def task(self) -> dict:
        value = json.loads(self.state.read_text(encoding="utf-8"))
        return next(task for task in value["tasks"] if task["id"] == TASK)


class ReconcileTaskStateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temporary = tempfile.TemporaryDirectory(prefix="cmclient reconcile shared ")
        cls.fixture = ReconcileFixture(Path(cls.temporary.name))

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temporary.cleanup()

    def setUp(self) -> None:
        self.fx = self.fixture
        self.fx.reset()

    def commit(self, sha: str = "a" * 40) -> object:
        return MODULE.CheckpointCommit(
            sha=sha,
            parents=(self.fx.base,),
            committed_at="2026-07-20T00:00:00+00:00",
            subject=f"build(workflow): [{TASK}] recover checkpoint state",
            body=f"Task: {TASK}\nValidation: passed\nChange: fixture",
        )

    def test_remote_checkpoint_reconciles_and_rerun_is_idempotent(self) -> None:
        sha = self.fx.checkpoint()
        self.fx.git("push", "origin", "dev", cwd=self.fx.repo)

        first = self.fx.run()
        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertEqual(json.loads(first.stdout)["action"], "reconciled")
        self.assertEqual(self.fx.task()["status"], "done")
        self.assertEqual(self.fx.task()["commit"], sha)
        self.assertEqual(self.fx.commits.read_text(encoding="utf-8").count(f"| {TASK} |"), 1)

        state_before = self.fx.state.read_bytes()
        commits_before = self.fx.commits.read_bytes()
        second = self.fx.run()
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertEqual(json.loads(second.stdout)["action"], "no-op")
        self.assertEqual(self.fx.state.read_bytes(), state_before)
        self.assertEqual(self.fx.commits.read_bytes(), commits_before)

    def test_sole_local_checkpoint_requires_opt_in_then_pushes_same_sha(self) -> None:
        sha = self.fx.checkpoint()
        refused = self.fx.run()
        self.assertNotEqual(refused.returncode, 0)
        self.assertIn("--push-local", refused.stderr)
        self.assertEqual(self.fx.task()["status"], "in_progress")

        recovered = self.fx.run("--push-local")
        self.assertEqual(recovered.returncode, 0, recovered.stderr)
        self.assertEqual(json.loads(recovered.stdout)["action"], "pushed-and-reconciled")
        remote_sha = self.fx.git("rev-parse", "origin/dev", cwd=self.fx.repo)
        self.assertEqual(remote_sha, sha)
        self.assertEqual(self.fx.task()["commit"], sha)

    def test_blocked_push_failure_recovers_the_exact_local_checkpoint(self) -> None:
        sha = self.fx.checkpoint()
        self.fx.write_state(status="blocked", commit=sha)
        recovered = self.fx.run("--push-local")
        self.assertEqual(recovered.returncode, 0, recovered.stderr)
        self.assertEqual(self.fx.task()["status"], "done")
        self.assertEqual(self.fx.task()["commit"], sha)
        self.assertEqual(
            self.fx.git("rev-parse", "refs/heads/dev", cwd=self.fx.remote), sha
        )

    def test_graph_drift_is_rejected_before_local_checkpoint_push(self) -> None:
        self.fx.checkpoint()
        graph_lock = json.loads(self.fx.graph_lock.read_text(encoding="utf-8"))
        graph_lock["callMeshServiceModel"]["localMappingOverride"] = True
        self.fx.graph_lock.write_text(
            json.dumps(graph_lock, indent=2) + "\n", encoding="utf-8"
        )
        rejected = self.fx.run("--push-local")
        self.assertNotEqual(rejected.returncode, 0)
        self.assertIn("activeGraph.callMeshServiceModel", rejected.stderr)
        self.assertEqual(
            self.fx.git("rev-parse", "refs/heads/dev", cwd=self.fx.remote),
            self.fx.base,
        )

    def test_checkpoint_parent_must_match_recorded_base(self) -> None:
        (self.fx.repo / "intervening.txt").write_text(
            "unowned intervening change\n", encoding="utf-8"
        )
        self.fx.git("add", "intervening.txt", cwd=self.fx.repo)
        self.fx.git(
            "commit", "-m", "test(repo): intervening ordinary commit", cwd=self.fx.repo
        )
        self.fx.checkpoint()
        self.fx.git("push", "origin", "dev", cwd=self.fx.repo)

        result = self.fx.run()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("checkpoint parent does not match", result.stderr)
        self.assertEqual(self.fx.task()["status"], "in_progress")

    def test_real_git_dirty_diverged_behind_and_branch_guards_fail_closed(self) -> None:
        self.fx.checkpoint()
        self.fx.git("push", "origin", "dev", cwd=self.fx.repo)
        (self.fx.repo / "dirty.txt").write_text("dirty\n", encoding="utf-8")
        with self.assertRaises(MODULE.ReconcileError) as dirty:
            MODULE.validate_repo("git", self.fx.repo, "dev")
        self.assertIn("Repository is dirty", str(dirty.exception))

        self.fx.reset()
        self.fx.checkpoint()
        checkpoint = MODULE.find_checkpoint_commit("git", self.fx.repo, TASK)
        self.fx.advance_remote(self.fx.base, "test(remote): diverge")
        with self.assertRaises(MODULE.ReconcileError) as diverged:
            MODULE.ensure_remote_commit(
                "git", self.fx.repo, checkpoint, "origin", "dev", True, False
            )
        self.assertIn("have diverged", str(diverged.exception))

        self.fx.reset()
        self.fx.checkpoint()
        self.fx.git("push", "origin", "dev", cwd=self.fx.repo)
        checkpoint = MODULE.find_checkpoint_commit("git", self.fx.repo, TASK)
        self.fx.advance_remote(checkpoint.sha, "test(remote): advance branch")
        with self.assertRaises(MODULE.ReconcileError) as behind:
            MODULE.ensure_remote_commit(
                "git", self.fx.repo, checkpoint, "origin", "dev", False, False
            )
        self.assertIn("is behind origin/dev", str(behind.exception))

        with self.assertRaises(MODULE.ReconcileError):
            MODULE.validate_identifiers(TASK, "main")
        self.fx.git("checkout", "-b", "work", cwd=self.fx.repo)
        with self.assertRaises(MODULE.ReconcileError) as wrong_branch:
            MODULE.validate_repo("git", self.fx.repo, "dev", require_clean=False)
        self.assertIn("expected branch 'dev'", str(wrong_branch.exception))

    def test_state_graph_done_sha_and_dependencies_fail_closed(self) -> None:
        commit = self.commit()
        state = json.loads(self.fx.state.read_text(encoding="utf-8"))
        reconciled, changed = MODULE.reconciled_state(state, TASK, commit)
        self.assertTrue(changed)
        target = next(task for task in reconciled["tasks"] if task["id"] == TASK)
        self.assertEqual(target["status"], "done")
        self.assertEqual(target["commit"], commit.sha)
        same, changed = MODULE.reconciled_state(reconciled, TASK, commit)
        self.assertFalse(changed)
        self.assertEqual(same, reconciled)

        target["commit"] = "0" * 40
        with self.assertRaises(MODULE.ReconcileError) as mismatch:
            MODULE.reconciled_state(reconciled, TASK, commit)
        self.assertIn("state SHA mismatch", str(mismatch.exception))

        self.fx.write_state()
        state = json.loads(self.fx.state.read_text(encoding="utf-8"))
        target = next(task for task in state["tasks"] if task["id"] == TASK)
        target["dependsOn"] = ["P13-T99"]
        with self.assertRaises(MODULE.ReconcileError) as missing:
            MODULE.reconciled_state(state, TASK, commit)
        self.assertIn("missing dependency: P13-T02 -> P13-T99", str(missing.exception))

        self.fx.write_state(dependency_status="pending")
        state = json.loads(self.fx.state.read_text(encoding="utf-8"))
        with self.assertRaises(MODULE.ReconcileError) as unfinished:
            MODULE.reconciled_state(state, TASK, commit)
        self.assertIn(
            "in_progress task has unfinished dependencies: P13-T02 -> P13-T01",
            str(unfinished.exception),
        )

        self.fx.write_state()
        state = json.loads(self.fx.state.read_text(encoding="utf-8"))
        dependency = next(task for task in state["tasks"] if task["id"] == "P13-T01")
        dependency["dependsOn"] = [TASK]
        with self.assertRaises(MODULE.ReconcileError) as cycle:
            MODULE.reconciled_state(state, TASK, commit)
        self.assertIn("task graph cycle", str(cycle.exception))

        self.fx.write_state()
        state = json.loads(self.fx.state.read_text(encoding="utf-8"))
        state["tasks"].append(
            {
                "id": "P13-T03",
                "status": "in_progress",
                "dependsOn": ["P13-T01"],
                "notes": [],
            }
        )
        with self.assertRaises(MODULE.ReconcileError) as multiple_active:
            MODULE.reconciled_state(state, TASK, commit)
        self.assertIn(
            "at most one task may be in_progress", str(multiple_active.exception)
        )

        self.fx.write_state()
        state = json.loads(self.fx.state.read_text(encoding="utf-8"))
        state["candidateInvalidations"] = [{}]
        with self.assertRaises(MODULE.ReconcileError) as invalidation:
            MODULE.reconciled_state(state, TASK, commit)
        self.assertIn("repairTask is invalid", str(invalidation.exception))

    def test_commit_log_is_idempotent_and_rejects_mismatch(self) -> None:
        commit = self.commit()
        header = (
            "# Checkpoint Commit Log\n\n"
            "| Time | Task | Commit | Subject | Verification |\n"
            "|---|---|---|---|---|\n"
        )
        logged, changed = MODULE.commit_log_update(header, TASK, commit)
        self.assertTrue(changed)
        same, changed = MODULE.commit_log_update(logged, TASK, commit)
        self.assertFalse(changed)
        self.assertEqual(same, logged)
        with self.assertRaises(MODULE.ReconcileError) as mismatch:
            MODULE.commit_log_update(logged.replace(commit.sha[:12], "f" * 12), TASK, commit)
        self.assertIn("SHA mismatch", str(mismatch.exception))
        with self.assertRaises(MODULE.ReconcileError) as malformed:
            MODULE.commit_log_update(
                logged.replace(f"`{commit.sha[:12]}`", "not-a-sha"), TASK, commit
            )
        self.assertIn("malformed row", str(malformed.exception))

    def test_structured_history_is_single_call_unique_and_exact(self) -> None:
        def record(sha: str, task: str, body_task: str, validation: str = "passed") -> str:
            return (
                f"{sha}\x00{'1' * 40}\x002026-07-20T00:00:00+00:00\x00"
                f"build(workflow): [{task}] recover state\x00"
                f"Task: {body_task}\nValidation: {validation}\n\x00"
            )

        valid = record("a" * 40, TASK, TASK)
        completed = subprocess.CompletedProcess(
            args=["git"], returncode=0, stdout=valid, stderr=""
        )
        with mock.patch.object(MODULE, "run_git", return_value=completed) as run:
            commit = MODULE.find_checkpoint_commit("git", Path("repo"), TASK)
        self.assertEqual(commit.sha, "a" * 40)
        self.assertEqual(run.call_count, 1)
        self.assertIn("log", run.call_args.args)
        self.assertIn("-z", run.call_args.args)

        # Older implementation commits may carry the task ID in their subject
        # but are not checkpoint records until they include the structured
        # Task/Validation body markers.
        legacy_subject = subprocess.CompletedProcess(
            args=["git"],
            returncode=0,
            stdout=(
                "e" * 40
                + "\x00"
                + "1" * 40
                + "\x002026-07-20T00:00:00+00:00\x00"
                + f"feat(gateway): [{TASK}] implementation\x00\x00"
            ),
            stderr="",
        )
        with mock.patch.object(MODULE, "run_git", return_value=legacy_subject):
            with self.assertRaises(MODULE.ReconcileError) as missing:
                MODULE.find_checkpoint_commit("git", Path("repo"), TASK)
        self.assertIn("no checkpoint commit found", str(missing.exception))

        duplicate = subprocess.CompletedProcess(
            args=["git"],
            returncode=0,
            stdout=valid + record("b" * 40, TASK, TASK),
            stderr="",
        )
        with mock.patch.object(MODULE, "run_git", return_value=duplicate):
            with self.assertRaises(MODULE.ReconcileError) as multiple:
                MODULE.find_checkpoint_commit("git", Path("repo"), TASK)
        self.assertIn("multiple checkpoint commits", str(multiple.exception))

        malformed = subprocess.CompletedProcess(
            args=["git"],
            returncode=0,
            stdout=record("c" * 40, TASK, "P13-T03", "skipped-by-human"),
            stderr="",
        )
        with mock.patch.object(MODULE, "run_git", return_value=malformed):
            with self.assertRaises(MODULE.ReconcileError) as exact:
                MODULE.find_checkpoint_commit("git", Path("repo"), TASK)
        self.assertIn("exactly one 'Task: P13-T02'", str(exact.exception))

        substring = subprocess.CompletedProcess(
            args=["git"],
            returncode=0,
            stdout=record("d" * 40, "P13-T02a", "P13-T02a"),
            stderr="",
        )
        with mock.patch.object(MODULE, "run_git", return_value=substring):
            with self.assertRaises(MODULE.ReconcileError) as absent:
                MODULE.find_checkpoint_commit("git", Path("repo"), TASK)
        self.assertIn("no checkpoint commit found for exact task", str(absent.exception))


if __name__ == "__main__":
    unittest.main(verbosity=2)
