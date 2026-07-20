#!/usr/bin/env python3
"""Crash-safe task graph state operations shared by workspace tools."""

from __future__ import annotations

import contextlib
import copy
import json
import os
import re
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterator, TypeVar


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = Path(
    os.environ.get("CMCLIENT_WORKSPACE_ROOT", REPOSITORY_ROOT.parent.parent)
).resolve()
DEFAULT_STATE_PATH = WORKSPACE_ROOT / "state/TASKS.json"
DEFAULT_REPO_PATH = REPOSITORY_ROOT
# Historical P12 repair tasks used a single lowercase suffix. Preserve those
# immutable IDs while allocating every new repair from the numeric sequence.
TASK_ID_PATTERN = re.compile(r"^(P\d{2})-T(\d{2})(?:[a-z])?$")
GIT_OBJECT_PATTERN = re.compile(r"^[0-9a-fA-F]{40}(?:[0-9a-fA-F]{24})?$")
CANDIDATE_SHA256_PATTERN = re.compile(r"^[0-9a-fA-F]{64}$")
CHECKPOINT_SUBJECT_PATTERN = re.compile(
    r"^(?:feat|fix|refactor|test|docs|build|ci|chore|perf|security|release)"
    r"\([a-z0-9-]+\): \[(P\d{2}-T\d{2}(?:[a-z])?)\] \S.*$"
)
ALLOWED_STATUSES = {"pending", "in_progress", "blocked", "done", "skipped"}
TERMINAL_STATUSES = {"done", "skipped"}
ALLOWED_REPAIR_CASES = {
    "FULL_VERIFY",
    "SECRET_SCAN",
    "SUPPLY_CHAIN",
    "TESTABILITY_GATES",
    "PACKAGE_MATRIX",
    "DOCKER_MATRIX",
    "LIVE_DATA",
    "CLIENTS",
    "RECOVERY",
    "LIVE_SOAK_24H",
    "CLEANUP",
    "DEFERRALS",
}
T = TypeVar("T")


class TaskStateError(ValueError):
    """Raised when task state or a requested state transition is invalid."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_timestamp(value: object, label: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise TaskStateError(f"{label} must be an ISO-8601 timestamp")
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as error:
        raise TaskStateError(f"{label} must be an ISO-8601 timestamp") from error
    if parsed.tzinfo is None:
        raise TaskStateError(f"{label} must include a timezone")
    return parsed.astimezone(timezone.utc)


def normalize_candidate_identity(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise TaskStateError("candidate identity must be an exact SHA-256 digest")
    candidate = value.strip()
    if candidate.lower().startswith("sha256:"):
        candidate = candidate.split(":", 1)[1]
    if not CANDIDATE_SHA256_PATTERN.fullmatch(candidate):
        raise TaskStateError("candidate identity must be an exact SHA-256 digest")
    return f"sha256:{candidate.lower()}"


def normalize_git_object(value: object, label: str) -> str:
    if not isinstance(value, str) or not GIT_OBJECT_PATTERN.fullmatch(value):
        raise TaskStateError(f"{label} must be a full Git object ID")
    return value.lower()


def repository_checkpoint_base(repo: Path = DEFAULT_REPO_PATH) -> str:
    """Return HEAD only for a clean dev worktree."""

    repo = Path(repo)

    def git(*arguments: str) -> str:
        result = subprocess.run(
            ["git", "-C", str(repo), *arguments],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            env={
                **os.environ,
                "GIT_TERMINAL_PROMPT": "0",
                "GCM_INTERACTIVE": "Never",
            },
        )
        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip()
            raise TaskStateError(
                f"git {' '.join(arguments)} failed for {repo}: {detail}"
            )
        return result.stdout.strip()

    if git("rev-parse", "--is-inside-work-tree") != "true":
        raise TaskStateError(f"not a Git worktree: {repo}")
    branch = git("branch", "--show-current")
    if branch != "dev" or branch == "main":
        raise TaskStateError(f"checkpoint base requires dev, found {branch!r}")
    dirty = git("status", "--porcelain=v1", "--untracked-files=all")
    if dirty:
        raise TaskStateError(
            "checkpoint base requires a clean Repository; repair start will not "
            "stash, reset, or mix uncommitted parent work into a repair checkpoint"
        )
    return normalize_git_object(
        git("rev-parse", "HEAD"), "Repository checkpoint base"
    )


def _lock_path(state_path: Path) -> Path:
    return state_path.with_name(".task-state.lock")


@contextlib.contextmanager
def state_lock(state_path: Path, timeout_seconds: float = 30.0) -> Iterator[None]:
    """Hold an exclusive one-byte lock that works on Windows and POSIX."""

    state_path = Path(state_path)
    lock_path = _lock_path(state_path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+b") as handle:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()
            os.fsync(handle.fileno())
        handle.seek(0)
        deadline = time.monotonic() + timeout_seconds

        if os.name == "nt":
            import msvcrt

            while True:
                try:
                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                    break
                except OSError as error:
                    if time.monotonic() >= deadline:
                        raise TaskStateError(
                            f"timed out locking task state: {state_path}"
                        ) from error
                    time.sleep(0.05)
            try:
                yield
            finally:
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            while True:
                try:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError as error:
                    if time.monotonic() >= deadline:
                        raise TaskStateError(
                            f"timed out locking task state: {state_path}"
                        ) from error
                    time.sleep(0.05)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def load_json(state_path: Path) -> dict:
    try:
        with Path(state_path).open("r", encoding="utf-8") as handle:
            value = json.load(handle)
    except json.JSONDecodeError as error:
        raise TaskStateError(f"invalid JSON in {state_path}: {error}") from error
    if not isinstance(value, dict):
        raise TaskStateError(f"task state root must be an object: {state_path}")
    return value


def atomic_write_json(state_path: Path, value: dict) -> None:
    """Durably replace JSON in the same directory without a partial state file."""

    state_path = Path(state_path)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    previous_mode = None
    try:
        previous_mode = state_path.stat().st_mode & 0o777
    except FileNotFoundError:
        pass

    payload = (
        json.dumps(value, ensure_ascii=False, indent=2).encode("utf-8") + b"\n"
    )
    descriptor, temporary_name = tempfile.mkstemp(
        dir=state_path.parent,
        prefix=f".{state_path.name}.",
        suffix=".tmp",
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        if previous_mode is not None:
            os.chmod(temporary, previous_mode)
        os.replace(temporary, state_path)
        if os.name != "nt":
            directory_fd = os.open(state_path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)


def task_index(state: dict) -> dict[str, dict]:
    tasks = state.get("tasks")
    if not isinstance(tasks, list):
        raise TaskStateError("tasks must be an array")

    by_id: dict[str, dict] = {}
    for position, task in enumerate(tasks):
        if not isinstance(task, dict):
            raise TaskStateError(f"task at index {position} must be an object")
        task_id = task.get("id")
        if not isinstance(task_id, str) or not TASK_ID_PATTERN.fullmatch(task_id):
            raise TaskStateError(f"invalid task ID at index {position}: {task_id!r}")
        if task_id in by_id:
            raise TaskStateError(f"duplicate task ID: {task_id}")
        by_id[task_id] = task
    return by_id


def _superseded_task_ids(state: dict) -> set[str]:
    active_graph = state.get("activeGraph")
    if active_graph is None:
        return set()
    if not isinstance(active_graph, dict):
        raise TaskStateError("activeGraph must be an object")
    values = active_graph.get("supersededTaskIds", [])
    if not isinstance(values, list) or not all(
        isinstance(task_id, str) for task_id in values
    ):
        raise TaskStateError("activeGraph.supersededTaskIds must be a string array")
    if len(values) != len(set(values)):
        raise TaskStateError("activeGraph.supersededTaskIds contains duplicates")
    return set(values)


def _validate_required_supersession(
    state: dict,
    task_id: str,
    task: dict,
    by_id: dict[str, dict],
) -> None:
    if task_id not in _superseded_task_ids(state):
        raise TaskStateError(
            f"required skipped task is not declared superseded: {task_id}"
        )
    replacements = task.get("supersededBy")
    if not isinstance(replacements, list) or not replacements or not all(
        isinstance(replacement, str) for replacement in replacements
    ):
        raise TaskStateError(
            f"required skipped task lacks supersededBy targets: {task_id}"
        )
    if len(replacements) != len(set(replacements)):
        raise TaskStateError(f"duplicate supersededBy target: {task_id}")
    for replacement in replacements:
        if replacement == task_id or replacement not in by_id:
            raise TaskStateError(
                f"invalid supersededBy target: {task_id} -> {replacement}"
            )


def _validate_invalidation_record(
    record: dict,
    *,
    repair_id: str,
    repair: dict,
    by_id: dict[str, dict],
    label: str,
) -> None:
    if record.get("repairOf") != repair.get("repairOf"):
        raise TaskStateError(f"{label}.repairOf disagrees with {repair_id}")
    parent_id = record.get("repairOf")
    if not isinstance(parent_id, str) or parent_id not in by_id:
        raise TaskStateError(f"{label}.repairOf is not an existing task")
    if record.get("runtimeCandidate") is not True:
        raise TaskStateError(f"{label}.runtimeCandidate must be true")
    if record.get("distributionCandidate") is not True:
        raise TaskStateError(f"{label}.distributionCandidate must be true")
    invalidated_at = _parse_timestamp(
        record.get("invalidatedAt"), f"{label}.invalidatedAt"
    )
    cases = record.get("affectedCases")
    if not isinstance(cases, list) or not cases or not all(
        isinstance(case, str) and bool(case.strip()) for case in cases
    ):
        raise TaskStateError(f"{label}.affectedCases must be a non-empty string array")
    normalized_cases = [case.strip() for case in cases]
    if normalized_cases != cases or len(cases) != len(set(cases)):
        raise TaskStateError(f"{label}.affectedCases must be normalized and unique")
    unknown_cases = sorted(set(cases) - ALLOWED_REPAIR_CASES)
    if unknown_cases:
        raise TaskStateError(
            f"{label}.affectedCases contains unknown case IDs: "
            + ", ".join(unknown_cases)
        )
    if cases != repair.get("affectedCases"):
        raise TaskStateError(f"{label}.affectedCases disagrees with {repair_id}")
    if record.get("invalidatedAt") != repair.get("startedAt"):
        raise TaskStateError(f"{label}.invalidatedAt disagrees with {repair_id}")

    resolved = record.get("resolvedByCandidate")
    resolved_at = record.get("resolvedAt")
    if resolved is None:
        if resolved_at is not None:
            raise TaskStateError(
                f"{label}.resolvedAt requires resolvedByCandidate"
            )
    else:
        normalized = normalize_candidate_identity(resolved)
        if normalized != resolved:
            raise TaskStateError(
                f"{label}.resolvedByCandidate must use canonical sha256: form"
            )
        resolution_time = _parse_timestamp(resolved_at, f"{label}.resolvedAt")
        if resolution_time < invalidated_at:
            raise TaskStateError(f"{label}.resolvedAt predates invalidatedAt")


def _validate_candidate_invalidations(
    state: dict,
    by_id: dict[str, dict],
) -> None:
    ledger = state.get("candidateInvalidations", [])
    if not isinstance(ledger, list):
        raise TaskStateError("candidateInvalidations must be an array")

    ledger_by_repair: dict[str, dict] = {}
    for index, record in enumerate(ledger):
        if not isinstance(record, dict):
            raise TaskStateError(
                f"candidateInvalidations[{index}] must be an object"
            )
        repair_id = record.get("repairTask")
        if not isinstance(repair_id, str) or repair_id not in by_id:
            raise TaskStateError(
                f"candidateInvalidations[{index}].repairTask is invalid"
            )
        if repair_id in ledger_by_repair:
            raise TaskStateError(
                f"duplicate candidate invalidation for repair: {repair_id}"
            )
        repair = by_id[repair_id]
        if repair.get("repairOf") is None:
            raise TaskStateError(
                f"candidate invalidation task is not a repair: {repair_id}"
            )
        _validate_invalidation_record(
            record,
            repair_id=repair_id,
            repair=repair,
            by_id=by_id,
            label=f"candidateInvalidations[{index}]",
        )
        ledger_by_repair[repair_id] = record

    compared_fields = (
        "invalidatedAt",
        "repairOf",
        "runtimeCandidate",
        "distributionCandidate",
        "affectedCases",
        "resolvedByCandidate",
        "resolvedAt",
    )
    for repair_id, repair in by_id.items():
        per_task = repair.get("candidateInvalidation")
        if per_task is None:
            continue
        if not isinstance(per_task, dict):
            raise TaskStateError(
                f"task {repair_id}.candidateInvalidation must be an object"
            )
        _validate_invalidation_record(
            per_task,
            repair_id=repair_id,
            repair=repair,
            by_id=by_id,
            label=f"task {repair_id}.candidateInvalidation",
        )
        ledger_record = ledger_by_repair.get(repair_id)
        if ledger_record is None:
            raise TaskStateError(
                f"task {repair_id} lacks candidateInvalidations ledger entry"
            )
        if any(
            per_task.get(field) != ledger_record.get(field)
            for field in compared_fields
        ):
            raise TaskStateError(
                f"candidate invalidation metadata disagrees for repair: {repair_id}"
            )

    for repair_id in ledger_by_repair:
        if by_id[repair_id].get("candidateInvalidation") is None:
            raise TaskStateError(
                f"candidate invalidation ledger lacks per-task metadata: {repair_id}"
            )


def validate_task_graph(state: dict) -> dict[str, dict]:
    """Validate identifiers, dependencies, DAG shape, and active-task count."""

    by_id = task_index(state)
    active: list[str] = []

    for task_id, task in by_id.items():
        status = task.get("status")
        if status not in ALLOWED_STATUSES:
            raise TaskStateError(f"invalid status for {task_id}: {status!r}")
        if status == "in_progress":
            active.append(task_id)

        match = TASK_ID_PATTERN.fullmatch(task_id)
        assert match is not None
        phase = task.get("phase")
        if phase is not None and phase != match.group(1):
            raise TaskStateError(
                f"task phase does not match ID: {task_id} has {phase!r}"
            )

        dependencies = task.get("dependsOn", [])
        if not isinstance(dependencies, list) or not all(
            isinstance(dependency, str) for dependency in dependencies
        ):
            raise TaskStateError(f"dependsOn must be a string array: {task_id}")
        if len(dependencies) != len(set(dependencies)):
            raise TaskStateError(f"duplicate dependency on task: {task_id}")
        for dependency in dependencies:
            if dependency == task_id:
                raise TaskStateError(f"task depends on itself: {task_id}")
            if dependency not in by_id:
                raise TaskStateError(
                    f"missing dependency: {task_id} -> {dependency}"
                )

    if len(active) > 1:
        raise TaskStateError(
            "at most one task may be in_progress: " + ", ".join(active)
        )

    for task_id, task in by_id.items():
        if task.get("required", True) and task.get("status") == "skipped":
            _validate_required_supersession(state, task_id, task, by_id)

    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(task_id: str) -> None:
        if task_id in visited:
            return
        if task_id in visiting:
            raise TaskStateError(f"task graph cycle at {task_id}")
        visiting.add(task_id)
        for dependency in by_id[task_id].get("dependsOn", []):
            visit(dependency)
        visiting.remove(task_id)
        visited.add(task_id)

    for task_id in by_id:
        visit(task_id)

    for task_id in active:
        unfinished = [
            dependency
            for dependency in by_id[task_id].get("dependsOn", [])
            if by_id[dependency].get("status") != "done"
        ]
        if unfinished:
            raise TaskStateError(
                f"in_progress task has unfinished dependencies: "
                f"{task_id} -> {', '.join(unfinished)}"
            )

    _validate_candidate_invalidations(state, by_id)

    return by_id


def read_validated_state(state_path: Path = DEFAULT_STATE_PATH) -> dict:
    with state_lock(state_path):
        state = load_json(state_path)
        validate_task_graph(state)
        return copy.deepcopy(state)


def mutate_state(
    state_path: Path,
    mutation: Callable[[dict], T],
) -> tuple[dict, T]:
    """Validate, mutate, validate again, then atomically persist under one lock."""

    with state_lock(state_path):
        state = load_json(state_path)
        validate_task_graph(state)
        original = copy.deepcopy(state)
        result = mutation(state)
        validate_task_graph(state)
        if state != original:
            atomic_write_json(state_path, state)
        return copy.deepcopy(state), result


def _dependencies_done(task: dict, by_id: dict[str, dict]) -> bool:
    return all(
        by_id[dependency].get("status") == "done"
        for dependency in task.get("dependsOn", [])
    )


def transition_task(
    state: dict,
    task_id: str,
    new_status: str,
    *,
    commit: str | None = None,
    note: str | None = None,
    checkpoint_base_commit: str | None = None,
    now: str | None = None,
) -> tuple[str, str]:
    by_id = validate_task_graph(state)
    task = by_id.get(task_id)
    if task is None:
        raise TaskStateError(f"unknown task: {task_id}")
    if new_status not in ALLOWED_STATUSES:
        raise TaskStateError(f"invalid target status: {new_status}")

    old_status = task["status"]
    if old_status in TERMINAL_STATUSES:
        if new_status != old_status:
            raise TaskStateError(
                f"terminal task cannot transition: {task_id} "
                f"{old_status} -> {new_status}"
            )
        if (
            commit is not None
            or note is not None
            or checkpoint_base_commit is not None
        ):
            raise TaskStateError(
                f"terminal task history is immutable: {task_id}"
            )
        return old_status, new_status
    if new_status == "done":
        raise TaskStateError(
            "done transitions are reserved for reconcile-task-state.py"
        )
    if checkpoint_base_commit is not None and new_status != "in_progress":
        raise TaskStateError(
            "checkpointBaseCommit is valid only for in_progress"
        )
    if commit is not None:
        normalized_commit = normalize_git_object(
            commit, f"task {task_id}.commit"
        )
        existing_commit = task.get("commit")
        if existing_commit is not None:
            normalized_existing_commit = normalize_git_object(
                existing_commit, f"task {task_id}.commit"
            )
            if normalized_commit != normalized_existing_commit:
                raise TaskStateError(f"task commit is immutable for {task_id}")
        elif new_status != "blocked":
            raise TaskStateError(
                f"task commit may only be recorded while blocking {task_id}"
            )
        commit = normalized_commit

    allowed = {
        "pending": {"pending", "in_progress", "blocked", "skipped"},
        "in_progress": {"in_progress", "pending", "blocked", "skipped"},
        "blocked": {"blocked", "pending", "in_progress", "skipped"},
        "done": {"done"},
        "skipped": {"skipped"},
    }
    if new_status not in allowed[old_status]:
        raise TaskStateError(
            f"illegal task transition: {task_id} {old_status} -> {new_status}"
        )

    if (
        new_status == "skipped"
        and old_status != "skipped"
        and task.get("required", True)
    ):
        _validate_required_supersession(state, task_id, task, by_id)

    timestamp = now or utc_now()
    if new_status in {"in_progress", "done"} and not _dependencies_done(task, by_id):
        unfinished = [
            dependency
            for dependency in task.get("dependsOn", [])
            if by_id[dependency].get("status") != "done"
        ]
        raise TaskStateError(
            f"cannot mark {task_id} {new_status}; unfinished dependencies: "
            + ", ".join(unfinished)
        )

    if new_status == "in_progress":
        existing_base = task.get("checkpointBaseCommit")
        supplied_base = (
            normalize_git_object(
                checkpoint_base_commit, f"task {task_id}.checkpointBaseCommit"
            )
            if checkpoint_base_commit is not None
            else None
        )
        if old_status != "in_progress" and supplied_base is None:
            raise TaskStateError(
                f"checkpointBaseCommit is required to start {task_id}"
            )
        if existing_base is None:
            if supplied_base is None:
                raise TaskStateError(
                    f"checkpointBaseCommit is required to start {task_id}"
                )
            task["checkpointBaseCommit"] = supplied_base
        else:
            normalized_existing = normalize_git_object(
                existing_base, f"task {task_id}.checkpointBaseCommit"
            )
            if supplied_base is not None and supplied_base != normalized_existing:
                raise TaskStateError(
                    f"checkpointBaseCommit is immutable for {task_id}"
                )
            task["checkpointBaseCommit"] = normalized_existing

    if new_status == "in_progress" and old_status != "in_progress":
        other_active = [
            item["id"]
            for item in state["tasks"]
            if item.get("status") == "in_progress" and item["id"] != task_id
        ]
        if other_active:
            raise TaskStateError(
                f"cannot start {task_id}; already in progress: {other_active[0]}"
            )
        if not task.get("startedAt"):
            task["startedAt"] = timestamp
        if old_status == "blocked":
            task["resumedAt"] = timestamp
            task.pop("blockedAt", None)
            task.pop("blockReason", None)
    elif new_status == "blocked" and old_status != "blocked":
        task["blockedAt"] = timestamp
        if note:
            task["blockReason"] = note
    elif new_status == "skipped" and old_status != "skipped":
        if commit is not None:
            raise TaskStateError(f"skipped task cannot record a commit: {task_id}")
        task["skippedAt"] = timestamp

    task["status"] = new_status
    if commit is not None:
        task["commit"] = commit
    if note:
        notes = task.setdefault("notes", [])
        if not isinstance(notes, list):
            raise TaskStateError(f"notes must be an array: {task_id}")
        notes.append(note)
    validate_task_graph(state)
    return old_status, new_status


def next_ready_task(state: dict) -> dict | None:
    by_id = validate_task_graph(state)
    active = [task for task in state["tasks"] if task["status"] == "in_progress"]
    if active:
        return active[0]

    for task in state["tasks"]:
        if not task.get("required", True) or task.get("manualGate", False):
            continue
        if task["status"] != "pending":
            continue
        if _dependencies_done(task, by_id):
            return task
    return None


def allocate_repair_id(state: dict, parent: dict) -> str:
    by_id = validate_task_graph(state)
    match = TASK_ID_PATTERN.fullmatch(parent["id"])
    assert match is not None
    phase = parent.get("phase", match.group(1))
    sequences = [
        int(candidate.group(2))
        for task_id in by_id
        if (candidate := TASK_ID_PATTERN.fullmatch(task_id))
        and candidate.group(1) == phase
    ]
    sequence = max(sequences, default=0) + 1
    if sequence > 99:
        raise TaskStateError(
            f"repair task ID space exhausted for {phase}; checkpoint IDs end at T99"
        )
    while f"{phase}-T{sequence:02d}" in by_id:
        sequence += 1
        if sequence > 99:
            raise TaskStateError(
                f"repair task ID space exhausted for {phase}; checkpoint IDs end at T99"
            )
    return f"{phase}-T{sequence:02d}"


def start_repair(
    state: dict,
    parent_id: str,
    *,
    title: str,
    note: str | None = None,
    affected_cases: list[str] | None = None,
    scope: str | None = None,
    checkpoint_base_commit: str | None = None,
    now: str | None = None,
) -> dict:
    by_id = validate_task_graph(state)
    parent = by_id.get(parent_id)
    if parent is None:
        raise TaskStateError(f"unknown parent task: {parent_id}")

    raw_cases = affected_cases or []
    if not raw_cases or not all(isinstance(case, str) for case in raw_cases):
        raise TaskStateError("repair requires at least one affected case")
    cases = [case.strip() for case in raw_cases]
    if any(not case for case in cases):
        raise TaskStateError("repair affected cases must not be empty")
    if len(cases) != len(set(cases)):
        raise TaskStateError("repair affected cases must be unique")
    unknown_cases = sorted(set(cases) - ALLOWED_REPAIR_CASES)
    if unknown_cases:
        raise TaskStateError(
            "repair affected cases contain unknown case IDs: "
            + ", ".join(unknown_cases)
        )
    if not isinstance(title, str) or not title.strip():
        raise TaskStateError("repair title must not be empty")
    base_commit = normalize_git_object(
        checkpoint_base_commit,
        "repair checkpointBaseCommit",
    )
    parent_base_value = parent.get("checkpointBaseCommit")
    if parent_base_value is None:
        parent["checkpointBaseCommit"] = base_commit
    else:
        parent_base = normalize_git_object(
            parent_base_value,
            f"repair parent {parent_id}.checkpointBaseCommit",
        )
        if parent_base != base_commit:
            raise TaskStateError(
                f"repair checkpointBaseCommit does not match parent {parent_id}"
            )

    existing_id = parent.get("blockedByRepair")
    if parent.get("status") == "blocked" and isinstance(existing_id, str):
        existing = by_id.get(existing_id)
        if existing and existing.get("repairOf") == parent_id:
            if existing.get("title") != title.strip():
                raise TaskStateError(
                    f"repair retry title disagrees with {existing_id}"
                )
            if existing.get("affectedCases") != cases:
                raise TaskStateError(
                    f"repair retry affected cases disagree with {existing_id}"
                )
            if existing.get("checkpointBaseCommit") != base_commit:
                raise TaskStateError(
                    f"checkpointBaseCommit is immutable for {existing_id}"
                )
            return existing

    if parent.get("status") != "in_progress":
        raise TaskStateError(
            f"repair parent must be in_progress: {parent_id} is "
            f"{parent.get('status')!r}"
        )
    timestamp = now or utc_now()
    repair_id = allocate_repair_id(state, parent)
    satisfied_predecessors = [
        dependency
        for dependency in parent.get("dependsOn", [])
        if by_id[dependency].get("status") == "done"
    ]
    invalidation = {
        "invalidatedAt": timestamp,
        "repairOf": parent_id,
        "runtimeCandidate": True,
        "distributionCandidate": True,
        "affectedCases": cases,
        "resolvedByCandidate": None,
        "resolvedAt": None,
    }

    parent["status"] = "blocked"
    parent["blockedAt"] = timestamp
    parent["blockedByRepair"] = repair_id
    parent["blockReason"] = note or f"Product defect tracked by {repair_id}"
    parent.setdefault("notes", []).append(
        f"Blocked for product repair {repair_id}: {title.strip()}"
    )

    repair = {
        "id": repair_id,
        "phase": parent.get("phase", repair_id.split("-", 1)[0]),
        "title": title.strip(),
        "status": "in_progress",
        "required": True,
        "dependsOn": satisfied_predecessors,
        "kind": "fix",
        "scope": scope or parent.get("scope") or "repair",
        "candidateReset": True,
        "checkpointBaseCommit": base_commit,
        "repairOf": parent_id,
        "affectedCases": cases,
        "candidateInvalidation": copy.deepcopy(invalidation),
        "acceptance": [
            "Reproduce the defect and prove a regression fails for the expected reason.",
            "Fix the defect and pass affected, adjacent, full verification, and secret scan.",
            "Checkpoint and push dev before resuming the parent task.",
        ],
        "owner": parent.get("owner"),
        "startedAt": timestamp,
        "completedAt": None,
        "commit": None,
        "notes": [note] if note else [],
    }
    state["tasks"].append(repair)
    ledger = state.setdefault("candidateInvalidations", [])
    if not isinstance(ledger, list):
        raise TaskStateError("candidateInvalidations must be an array")
    ledger.append({"repairTask": repair_id, **copy.deepcopy(invalidation)})
    validate_task_graph(state)
    return repair


def _validate_reconciled_checkpoint(task: dict) -> tuple[str, str]:
    task_id = task["id"]
    if task.get("status") != "done":
        raise TaskStateError(f"repair task is not done: {task_id}")
    commit = normalize_git_object(
        task.get("commit"),
        f"repair task {task_id}.commit",
    )
    base_commit = normalize_git_object(
        task.get("checkpointBaseCommit"),
        f"repair task {task_id}.checkpointBaseCommit",
    )
    if commit == base_commit:
        raise TaskStateError(
            f"repair checkpoint does not advance its base commit: {task_id}"
        )
    completed_at = _parse_timestamp(
        task.get("completedAt"), f"repair task {task_id}.completedAt"
    )
    started_at_value = task.get("startedAt")
    if started_at_value is not None:
        started_at = _parse_timestamp(
            started_at_value, f"repair task {task_id}.startedAt"
        )
        if completed_at < started_at:
            raise TaskStateError(
                f"repair checkpoint predates task start: {task_id}"
            )
    notes = task.get("notes")
    if not isinstance(notes, list):
        raise TaskStateError(f"repair task notes must be an array: {task_id}")
    checkpoint_subjects = [
        note
        for note in notes
        if isinstance(note, str)
        and (match := CHECKPOINT_SUBJECT_PATTERN.fullmatch(note))
        and match.group(1) == task_id
    ]
    if len(checkpoint_subjects) != 1:
        raise TaskStateError(
            f"repair task lacks one reconciled checkpoint subject: {task_id}"
        )
    return commit, base_commit


def resume_parent_after_repair(
    state: dict,
    parent_id: str,
    repair_id: str,
    *,
    note: str | None = None,
    now: str | None = None,
) -> dict:
    by_id = validate_task_graph(state)
    parent = by_id.get(parent_id)
    repair = by_id.get(repair_id)
    if parent is None:
        raise TaskStateError(f"unknown parent task: {parent_id}")
    if repair is None:
        raise TaskStateError(f"unknown repair task: {repair_id}")
    if repair.get("repairOf") != parent_id:
        raise TaskStateError(f"{repair_id} is not a repair of {parent_id}")
    repair_commit, repair_base = _validate_reconciled_checkpoint(repair)

    dependencies = parent.setdefault("dependsOn", [])
    if repair_id in dependencies and parent.get("status") == "in_progress":
        parent_base = normalize_git_object(
            parent.get("checkpointBaseCommit"),
            f"repair parent {parent_id}.checkpointBaseCommit",
        )
        if parent_base != repair_commit or parent.get("lastRepairTask") != repair_id:
            raise TaskStateError(
                f"resumed parent checkpoint state disagrees with {repair_id}"
            )
        return parent
    if parent.get("status") != "blocked":
        raise TaskStateError(
            f"repair parent must be blocked before resume: {parent_id} is "
            f"{parent.get('status')!r}"
        )
    blocked_by = parent.get("blockedByRepair")
    if blocked_by not in {None, repair_id}:
        raise TaskStateError(
            f"parent is blocked by a different repair: {parent_id} -> {blocked_by}"
        )
    other_active = [
        task["id"]
        for task in state["tasks"]
        if task.get("status") == "in_progress" and task["id"] != parent_id
    ]
    if other_active:
        raise TaskStateError(
            f"cannot resume {parent_id}; already in progress: {other_active[0]}"
        )
    parent_base = normalize_git_object(
        parent.get("checkpointBaseCommit"),
        f"repair parent {parent_id}.checkpointBaseCommit",
    )
    if parent_base != repair_base:
        raise TaskStateError(
            f"repair {repair_id} does not continue parent checkpoint base"
        )

    dependencies.append(repair_id)
    timestamp = now or utc_now()
    parent["status"] = "in_progress"
    parent["resumedAt"] = timestamp
    parent["lastRepairTask"] = repair_id
    parent["checkpointBaseCommit"] = repair_commit
    parent.pop("blockedAt", None)
    parent.pop("blockedByRepair", None)
    parent.pop("blockReason", None)
    parent.setdefault("notes", []).append(
        note or f"Resumed after pushed repair {repair_id}"
    )
    validate_task_graph(state)
    return parent


def resolve_candidate_invalidation(
    state: dict,
    repair_id: str,
    candidate_identity: str,
    *,
    now: str | None = None,
) -> dict:
    by_id = validate_task_graph(state)
    repair = by_id.get(repair_id)
    if repair is None:
        raise TaskStateError(f"unknown repair task: {repair_id}")
    if repair.get("repairOf") is None:
        raise TaskStateError(f"task is not a repair: {repair_id}")
    _validate_reconciled_checkpoint(repair)
    identity = normalize_candidate_identity(candidate_identity)

    per_task = repair.get("candidateInvalidation")
    ledger = state.get("candidateInvalidations")
    if not isinstance(per_task, dict) or not isinstance(ledger, list):
        raise TaskStateError(
            f"repair lacks candidate invalidation metadata: {repair_id}"
        )
    matches = [record for record in ledger if record.get("repairTask") == repair_id]
    if len(matches) != 1:
        raise TaskStateError(
            f"repair must have exactly one candidate invalidation record: {repair_id}"
        )
    ledger_record = matches[0]
    current = per_task.get("resolvedByCandidate")
    ledger_current = ledger_record.get("resolvedByCandidate")
    if current != ledger_current:
        raise TaskStateError(
            f"candidate invalidation resolution disagrees for repair: {repair_id}"
        )
    if current is not None:
        if current != identity:
            raise TaskStateError(
                f"candidate invalidation is already resolved by {current}"
            )
        return repair

    resolved_at = now or utc_now()
    per_task["resolvedByCandidate"] = identity
    per_task["resolvedAt"] = resolved_at
    ledger_record["resolvedByCandidate"] = identity
    ledger_record["resolvedAt"] = resolved_at
    validate_task_graph(state)
    return repair
