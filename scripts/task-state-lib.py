#!/usr/bin/env python3
"""Crash-safe task graph state operations shared by workspace tools."""

from __future__ import annotations

import contextlib
import copy
import hashlib
import json
import os
import re
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator, TypeVar


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = Path(
    os.environ.get("CMCLIENT_WORKSPACE_ROOT", REPOSITORY_ROOT.parent.parent)
).resolve()
DEFAULT_STATE_PATH = WORKSPACE_ROOT / "state/TASKS.json"
DEFAULT_REPO_PATH = REPOSITORY_ROOT
DEFAULT_GRAPH_LOCK_PATH = Path(__file__).with_name("unified-task-graph-lock.json")
GRAPH_LOCK_SCHEMA = "cmclient-unified-task-graph-lock/v2"
GRAPH_LOCK_SCHEMA_V3 = "cmclient-unified-task-graph-lock/v3"
GRAPH_LOCK_SCHEMAS = {GRAPH_LOCK_SCHEMA, GRAPH_LOCK_SCHEMA_V3}
LICENSE_PROVENANCE_SCHEMA = "cmclient-license-provenance/v1"
GRAPH_UPGRADE_JOURNAL_SCHEMA = "cmclient-graph-upgrade-journal/v1"
DEFINITION_AMENDMENT_SCHEMA = "cmclient-task-definition-amendment/v1"
GRAPH_UPGRADE_PHASES = {
    "prepared",
    "validated",
    "history-recorded",
    "workspace-staged",
    "repository-staged",
    "state-committed",
    "self-tested",
    "checkpointed",
    "pushed",
    "complete",
}
GRAPH_UPGRADE_STATUSES = {"running", "blocked", "complete"}
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
LOCKED_TASK_FIELDS = (
    "phase",
    "title",
    "required",
    "manualGate",
    "kind",
    "scope",
    "candidateReset",
    "acceptance",
)
V3_LOCKED_TASK_FIELDS = (
    "phase",
    "title",
    "required",
    "manualGate",
    "environmental",
    "lane",
    "priority",
    "kind",
    "scope",
    "candidateReset",
    "repairOf",
    "supersedesPartOf",
    "acceptance",
    "caseGroups",
    "caseAssertions",
    "evidenceClaim",
    "observesWithoutFinalizing",
)
GRAPH_PAYLOAD_FIELDS = (
    "tasks",
    "historicalSupersessions",
    "v2CoverageMap",
    "licenseGate",
    "repositoryIdentity",
    "targetPlatforms",
    "callMeshServiceModel",
    "candidateIdentity",
    "completionChecker",
    "repairProtocol",
    "definitionAmendments",
)
ACTIVE_GRAPH_FIELDS = (
    "id",
    "version",
    "source",
    "sourceSha256",
    "sourceBaseline",
    "branch",
    "completionTask",
    "manualReleaseTask",
    "importedAt",
    "completedHistorySha256",
    "supersededTaskIds",
    "historicalSupersessions",
    "v2CoverageMap",
    "licenseGate",
    "targetPlatforms",
    "callMeshServiceModel",
    "candidateIdentity",
    "completionChecker",
    "repairProtocol",
    "definitionAmendments",
)
V3_GRAPH_PAYLOAD_FIELDS = (
    *GRAPH_PAYLOAD_FIELDS,
    "activation",
    "completionCheckers",
    "scheduler",
    "supersessions",
    "existingTaskAmendments",
    "repairAllocation",
    "scopedCompletion",
    "activationInputs",
    "completionToolOnlyRepairAllowlist",
    "promotionBaseCommit",
)
V3_ACTIVE_GRAPH_FIELDS = (
    *ACTIVE_GRAPH_FIELDS,
    "activation",
    "completionCheckers",
    "scheduler",
    "supersessions",
    "existingTaskAmendments",
    "repairAllocation",
    "scopedCompletion",
    "activationInputs",
    "completionToolOnlyRepairAllowlist",
    "promotionBaseCommit",
)

DEFINITION_AMENDMENT_FIELDS = (
    "schema",
    "task",
    "repairTask",
    "field",
    "oldValue",
    "oldValueSha256",
    "newValue",
    "newValueSha256",
    "reason",
    "decision",
    "evidence",
    "recordedAt",
)
P13_AMENDMENT_EVIDENCE = {
    "https://nodejs.org/download/release/v24.18.0/docs/api/net.html#serverlistenhandle-backlog-callback": (
        "Listening on a file descriptor is not supported on Windows."
    ),
    "https://nodejs.org/download/release/v24.18.0/docs/api/child_process.html#subprocesssendmessage-sendhandle-options-callback": (
        "Sending IPC sockets is not supported on Windows."
    ),
}
# These digests bind the audited before/after acceptance arrays, rather than
# allowing an arbitrary text rewrite to be hidden behind a self-consistent
# amendment record.
P13_T05_OLD_ACCEPTANCE_SHA256 = (
    "8fdefb18da2932f5d7b0c7e950aca3cd5315e68fc13bad72b7028476806f4d98"
)
P13_T05_NEW_ACCEPTANCE_SHA256 = (
    "310fc79c9bb1869e9f0b308052f29cf1d947e395026ee82c8bfe684c04bbfb0a"
)
P13_T10_OLD_ACCEPTANCE_SHA256 = (
    "3f052516e955fbf3d31da553b2f516d926d9182826a9de751713851af2d8e575"
)
P13_T10_NEW_ACCEPTANCE_SHA256 = (
    "e09bcf76d2e3753fb3842b642bd50984580df90aeadb13c3270cc1dd09f2d057"
)
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


def canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def canonical_sha256(value: object) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _normalized_task_value(task: dict[str, Any], field: str) -> object:
    if field == "required":
        return task.get(field, True)
    if field in {
        "manualGate",
        "candidateReset",
        "environmental",
    }:
        return task.get(field, False)
    # v2 definitions intentionally distinguish an omitted acceptance field
    # from an explicit empty list.  Keep that legacy comparison stable; v3
    # fixed tasks carry their case/acceptance arrays explicitly in the graph.
    if field in {"caseGroups", "caseAssertions", "observesWithoutFinalizing"}:
        return copy.deepcopy(task.get(field, []))
    if field in {"acceptance", "evidenceClaim"}:
        return copy.deepcopy(task.get(field))
    return task.get(field)


def task_definition(
    task: dict[str, Any],
    fields: tuple[str, ...] = LOCKED_TASK_FIELDS,
) -> dict[str, Any]:
    return {
        "id": task.get("id"),
        **{field: _normalized_task_value(task, field) for field in fields},
        "dependsOn": copy.deepcopy(task.get("dependsOn", [])),
    }


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


def _load_document(path: Path, label: str) -> dict[str, Any]:
    path = Path(path)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise TaskStateError(f"cannot read {label} {path}: {error}") from error
    if not isinstance(value, dict):
        raise TaskStateError(f"{label} root must be an object: {path}")
    return value


def _upgrade_journal_path(state_path: Path) -> Path:
    return Path(state_path).with_name("GRAPH_UPGRADE.json")


def _upgrade_journal_paths(state_path: Path) -> tuple[Path, ...]:
    state_path = Path(state_path)
    return (
        _upgrade_journal_path(state_path),
        state_path.with_name("GRAPH_UPGRADE_V3.json"),
    )


def validate_upgrade_journal_guard(
    state_path: Path,
    operation_id: str | None = None,
) -> None:
    for journal_path in _upgrade_journal_paths(state_path):
        if not journal_path.exists():
            continue
        journal = _load_document(journal_path, "graph upgrade journal")
        if journal.get("schema") != GRAPH_UPGRADE_JOURNAL_SCHEMA:
            raise TaskStateError("graph upgrade journal schema is invalid")
        expected = journal.get("operationId")
        if not isinstance(expected, str) or not expected:
            raise TaskStateError("graph upgrade journal operationId is invalid")
        status = journal.get("status")
        phase = journal.get("phase")
        if status not in GRAPH_UPGRADE_STATUSES:
            raise TaskStateError("graph upgrade journal status is invalid")
        if phase not in GRAPH_UPGRADE_PHASES:
            raise TaskStateError("graph upgrade journal phase is invalid")
        status_complete = status == "complete"
        phase_complete = phase == "complete"
        if status_complete != phase_complete:
            raise TaskStateError("graph upgrade journal is only half complete")
        if status_complete and phase_complete:
            continue
        if operation_id != expected:
            raise TaskStateError(
                "GRAPH_UPGRADE_IN_PROGRESS: normal task workflow is paused until recovery"
            )


def _require_sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or not CANDIDATE_SHA256_PATTERN.fullmatch(value):
        raise TaskStateError(f"{label} must be a lowercase SHA-256 digest")
    if value != value.lower():
        raise TaskStateError(f"{label} must be a lowercase SHA-256 digest")
    return value


def _require_nonempty_collection(value: object, label: str) -> None:
    if not isinstance(value, (list, dict)) or not value:
        raise TaskStateError(f"{label} must be a non-empty array or object")


def _validate_definition_amendments(
    graph_lock: dict[str, Any],
    active: dict[str, Any],
    by_id: dict[str, dict],
    locked_by_id: dict[str, dict[str, Any]],
) -> None:
    """Validate the audited graph-v2 definition amendments.

    Definition changes are part of the canonical graph payload.  A record is
    therefore not an override: it must identify the repair that authorized the
    change, bind both complete values by digest, and match the value present in
    both the locked definition and active state.
    """

    amendments = graph_lock.get("definitionAmendments")
    if not isinstance(amendments, list):
        raise TaskStateError("graph lock definitionAmendments must be an array")
    if active.get("definitionAmendments") != amendments:
        raise TaskStateError(
            "activeGraph.definitionAmendments does not match the committed graph lock"
    )
    if not amendments:
        return
    if len(amendments) != 2:
        raise TaskStateError(
            "graph lock definitionAmendments must contain exactly two audited records"
        )

    expected_digests = {
        "P13-T05": (
            P13_T05_OLD_ACCEPTANCE_SHA256,
            P13_T05_NEW_ACCEPTANCE_SHA256,
        ),
        "P13-T10": (
            P13_T10_OLD_ACCEPTANCE_SHA256,
            P13_T10_NEW_ACCEPTANCE_SHA256,
        ),
    }
    records_by_task: dict[str, dict[str, Any]] = {}
    for record_index, record in enumerate(amendments):
        label = f"definitionAmendments[{record_index}]"
        if not isinstance(record, dict) or set(record) != set(
            DEFINITION_AMENDMENT_FIELDS
        ):
            raise TaskStateError(f"{label} fields are invalid")
        if record.get("schema") != DEFINITION_AMENDMENT_SCHEMA:
            raise TaskStateError(
                f"{label}.schema must be {DEFINITION_AMENDMENT_SCHEMA}"
            )

        task_id = record.get("task")
        if task_id not in expected_digests or task_id in records_by_task:
            raise TaskStateError(
                f"{label}.task must uniquely identify audited P13-T05 or P13-T10"
            )
        if record.get("repairTask") != "P13-T12":
            raise TaskStateError(f"{label}.repairTask must be P13-T12")
        if record.get("field") != "acceptance":
            raise TaskStateError(f"{label}.field must be acceptance")

        old_value = record.get("oldValue")
        new_value = record.get("newValue")
        if not isinstance(old_value, list) or not all(
            isinstance(value, str) for value in old_value
        ):
            raise TaskStateError(f"{label}.oldValue must be a string array")
        if not isinstance(new_value, list) or not all(
            isinstance(value, str) for value in new_value
        ):
            raise TaskStateError(f"{label}.newValue must be a string array")
        if old_value == new_value:
            raise TaskStateError(f"{label} oldValue and newValue must differ")

        old_digest = _require_sha256(
            record.get("oldValueSha256"), f"{label}.oldValueSha256"
        )
        new_digest = _require_sha256(
            record.get("newValueSha256"), f"{label}.newValueSha256"
        )
        if canonical_sha256(old_value) != old_digest:
            raise TaskStateError(f"{label}.oldValueSha256 is not canonical")
        if canonical_sha256(new_value) != new_digest:
            raise TaskStateError(f"{label}.newValueSha256 is not canonical")
        expected_old_digest, expected_new_digest = expected_digests[task_id]
        if old_digest != expected_old_digest:
            raise TaskStateError(f"{label} old acceptance digest is not audited")
        if new_digest != expected_new_digest:
            raise TaskStateError(f"{label} new acceptance digest is not audited")

        reason = record.get("reason")
        if not isinstance(reason, str) or not reason.strip():
            raise TaskStateError(f"{label}.reason must be non-empty")
        if record.get("decision") != "atomic-child-bind":
            raise TaskStateError(f"{label}.decision must be atomic-child-bind")
        _parse_timestamp(record.get("recordedAt"), f"{label}.recordedAt")

        evidence = record.get("evidence")
        if not isinstance(evidence, list) or len(evidence) != len(
            P13_AMENDMENT_EVIDENCE
        ):
            raise TaskStateError(
                f"{label}.evidence must contain the two official Node sources"
            )
        observed_sources: set[str] = set()
        for evidence_index, item in enumerate(evidence):
            evidence_label = f"{label}.evidence[{evidence_index}]"
            if not isinstance(item, dict) or set(item) != {"source", "finding"}:
                raise TaskStateError(f"{evidence_label} is invalid")
            source = item.get("source")
            if source not in P13_AMENDMENT_EVIDENCE:
                raise TaskStateError(
                    f"{evidence_label} is not an approved official source"
                )
            if item.get("finding") != P13_AMENDMENT_EVIDENCE[source]:
                raise TaskStateError(f"{evidence_label} finding is not approved")
            observed_sources.add(source)
        if observed_sources != set(P13_AMENDMENT_EVIDENCE):
            raise TaskStateError(f"{label} is missing an official source")

        target = by_id.get(task_id)
        locked_target = locked_by_id.get(task_id)
        if not isinstance(target, dict) or not isinstance(locked_target, dict):
            raise TaskStateError(f"{label} requires state and lock for {task_id}")
        if locked_target.get("acceptance") != new_value or _normalized_task_value(
            target, "acceptance"
        ) != new_value:
            raise TaskStateError(
                f"{label}.newValue does not match {task_id} acceptance"
            )
        records_by_task[task_id] = record

    if set(records_by_task) != set(expected_digests):
        raise TaskStateError(
            "definitionAmendments must contain P13-T05 and P13-T10 exactly once"
        )

    parent = by_id.get("P13-T05")
    repair = by_id.get("P13-T12")
    if not isinstance(parent, dict) or not isinstance(repair, dict):
        raise TaskStateError("definitionAmendments require P13-T05 and P13-T12 state")
    if (
        repair.get("repairOf") != "P13-T05"
        or repair.get("required", True) is not True
        or repair.get("kind") != "fix"
        or repair.get("candidateReset") is not True
        or repair.get("affectedCases") != ["TESTABILITY_GATES"]
        or repair.get("status") not in {"in_progress", "done"}
    ):
        raise TaskStateError(
            "definitionAmendments are not tied to an active or completed P13-T12 repair"
        )

    if repair.get("status") == "in_progress":
        if parent.get("status") != "blocked" or parent.get("blockedByRepair") != "P13-T12":
            raise TaskStateError(
                "in-progress P13-T12 amendment requires blocked P13-T05 parent"
            )
    elif parent.get("status") == "blocked":
        if parent.get("blockedByRepair") != "P13-T12":
            raise TaskStateError(
                "blocked P13-T05 parent must identify P13-T12 amendment repair"
            )
    elif parent.get("status") in {"in_progress", "done"}:
        dependencies = parent.get("dependsOn")
        if (
            not isinstance(dependencies, list)
            or dependencies.count("P13-T12") != 1
            or parent.get("lastRepairTask") != "P13-T12"
        ):
            raise TaskStateError(
                "resumed P13-T05 parent must incorporate completed P13-T12 repair"
            )
    else:
        raise TaskStateError("P13-T05 has an invalid status for its definition amendment")


def validate_license_provenance(
    graph_lock: dict[str, Any],
    evidence: dict[str, Any],
) -> None:
    gate = graph_lock.get("licenseGate")
    if not isinstance(gate, dict):
        raise TaskStateError("graph lock licenseGate must be an object")
    owner = gate.get("ownerDecision")
    if not isinstance(owner, dict):
        raise TaskStateError("graph lock license ownerDecision must be an object")
    if (
        owner.get("status") != "approved"
        or owner.get("route") != "GPL-3.0-only"
        or owner.get("publicDevPushPermitted") is not True
    ):
        raise TaskStateError("graph lock GPL-3.0-only license decision is not approved")
    if gate.get("evidencePath") != "state/LICENSE_PROVENANCE.json":
        raise TaskStateError("graph lock license evidence path is not canonical")

    required = gate.get("requiredFields")
    if not isinstance(required, list) or not all(
        isinstance(field, str) and field for field in required
    ):
        raise TaskStateError("graph lock license requiredFields is invalid")
    missing = [field for field in required if field not in evidence]
    if missing:
        raise TaskStateError(
            "license provenance is missing required fields: " + ", ".join(missing)
        )
    if evidence.get("schema") != LICENSE_PROVENANCE_SCHEMA:
        raise TaskStateError(
            f"license provenance schema must be {LICENSE_PROVENANCE_SCHEMA}"
        )
    if (
        evidence.get("status") != owner.get("status")
        or evidence.get("route") != owner.get("route")
        or evidence.get("publicDevPushPermitted") is not True
        or evidence.get("approvedAt") != owner.get("approvedAt")
        or evidence.get("approvalReference") != owner.get("approvalReference")
    ):
        raise TaskStateError("license provenance disagrees with the owner decision")
    for field in ("exactSources", "sourceDigests", "licenses", "notices"):
        _require_nonempty_collection(evidence.get(field), f"license provenance {field}")

    digests = evidence.get("sourceDigests")
    digest_values = list(digests.values()) if isinstance(digests, dict) else digests
    if not isinstance(digest_values, list) or not digest_values:
        raise TaskStateError("license provenance sourceDigests is invalid")
    for index, digest in enumerate(digest_values):
        if isinstance(digest, dict):
            digest = digest.get("sha256")
        _require_sha256(digest, f"license provenance sourceDigests[{index}]")


def _historical_completed_tasks(
    state: dict[str, Any], first_active_phase: str
) -> list[dict[str, Any]]:
    tasks = state.get("tasks")
    assert isinstance(tasks, list)
    boundary = next(
        (
            index
            for index, task in enumerate(tasks)
            if isinstance(task, dict)
            and isinstance(task.get("id"), str)
            and task["id"].startswith(f"{first_active_phase}-")
        ),
        None,
    )
    if boundary is None:
        raise TaskStateError(f"task state has no {first_active_phase} graph boundary")
    return [
        task
        for task in tasks[:boundary]
        if isinstance(task, dict) and task.get("status") == "done"
    ]


def _validate_state_against_graph_lock_v2(
    state: dict[str, Any],
    graph_lock: dict[str, Any],
    license_provenance: dict[str, Any],
) -> dict[str, dict]:
    by_id = validate_task_graph(state)
    if graph_lock.get("schema") != GRAPH_LOCK_SCHEMA:
        raise TaskStateError(f"graph lock schema must be {GRAPH_LOCK_SCHEMA}")
    active = state.get("activeGraph")
    if not isinstance(active, dict):
        raise TaskStateError("state.activeGraph must be an object")
    if active.get("id") != "unified-product" or active.get("version") != 2:
        raise TaskStateError("active graph must be unified-product@2")
    if graph_lock.get("id") != "unified-product" or graph_lock.get("version") != 2:
        raise TaskStateError("committed graph lock must be unified-product@2")
    if active.get("branch") != "dev" or graph_lock.get("branch") != "dev":
        raise TaskStateError("active graph and graph lock branch must be dev")

    historical = graph_lock.get("historicalSupersessions")
    if not isinstance(historical, list):
        raise TaskStateError("graph lock historicalSupersessions must be an array")
    historical_ids: list[str] = []
    for index, item in enumerate(historical):
        if not isinstance(item, dict) or item.get("graphVersion") != 1:
            raise TaskStateError(
                f"historicalSupersessions[{index}] must retain graphVersion 1"
            )
        old_id = item.get("old")
        replacements = item.get("new")
        if (
            not isinstance(old_id, str)
            or old_id in historical_ids
            or not isinstance(replacements, list)
            or not replacements
            or len(replacements) != len(set(replacements))
            or not all(isinstance(value, str) for value in replacements)
            or not isinstance(item.get("reason"), str)
            or not item["reason"]
        ):
            raise TaskStateError(f"historicalSupersessions[{index}] is invalid")
        historical_ids.append(old_id)
        task = by_id.get(old_id)
        if not isinstance(task, dict):
            raise TaskStateError(f"historical supersession task is missing: {old_id}")
        if (
            task.get("status") != "skipped"
            or task.get("supersededBy") != replacements
            or task.get("supersession")
            != {
                "graphId": graph_lock.get("id"),
                "graphVersion": 1,
                "reason": item["reason"],
            }
        ):
            raise TaskStateError(f"historical supersession drift: {old_id}")

    expected_active_values: dict[str, object] = {
        field: graph_lock.get(field) for field in ACTIVE_GRAPH_FIELDS
    }
    expected_active_values["supersededTaskIds"] = historical_ids
    for field, expected in expected_active_values.items():
        if active.get(field) != expected:
            raise TaskStateError(
                f"activeGraph.{field} does not match the committed graph lock"
            )
    _parse_timestamp(active.get("importedAt"), "activeGraph.importedAt")
    _require_sha256(active.get("sourceSha256"), "activeGraph.sourceSha256")
    normalize_git_object(active.get("sourceBaseline"), "activeGraph.sourceBaseline")

    first_active_phase = graph_lock.get("firstActivePhase")
    if first_active_phase != "P13":
        raise TaskStateError("graph lock firstActivePhase must be P13")
    completed_history = _historical_completed_tasks(state, first_active_phase)
    expected_history = _require_sha256(
        graph_lock.get("completedHistorySha256"),
        "graph lock completedHistorySha256",
    )
    if canonical_sha256(completed_history) != expected_history:
        raise TaskStateError("completed historical task state differs from graph lock")

    locked_tasks = graph_lock.get("tasks")
    if not isinstance(locked_tasks, list) or not locked_tasks:
        raise TaskStateError("graph lock tasks must be a non-empty array")
    declared_count = graph_lock.get("taskDefinitionCount")
    if declared_count is not None and declared_count != len(locked_tasks):
        raise TaskStateError("graph lock taskDefinitionCount is incorrect")
    locked_by_id: dict[str, dict[str, Any]] = {}
    for index, definition in enumerate(locked_tasks):
        if not isinstance(definition, dict) or not isinstance(definition.get("id"), str):
            raise TaskStateError(f"graph lock tasks[{index}] is invalid")
        task_id = definition["id"]
        if task_id in locked_by_id:
            raise TaskStateError(f"graph lock contains duplicate task: {task_id}")
        if set(definition) != {"id", *LOCKED_TASK_FIELDS, "dependsOn"}:
            raise TaskStateError(f"graph lock task definition fields are invalid: {task_id}")
        locked_by_id[task_id] = definition

    _validate_definition_amendments(graph_lock, active, by_id, locked_by_id)

    for task_id, definition in locked_by_id.items():
        task = by_id.get(task_id)
        if task is None:
            raise TaskStateError(f"locked active task is missing: {task_id}")
        for field in LOCKED_TASK_FIELDS:
            if _normalized_task_value(task, field) != definition.get(field):
                raise TaskStateError(f"locked task field changed: {task_id}.{field}")
        original_dependencies = definition.get("dependsOn")
        dependencies = task.get("dependsOn")
        if not isinstance(original_dependencies, list) or not isinstance(dependencies, list):
            raise TaskStateError(f"locked task dependencies are invalid: {task_id}")
        if dependencies[: len(original_dependencies)] != original_dependencies:
            raise TaskStateError(f"locked task original dependencies changed: {task_id}")
        for repair_id in dependencies[len(original_dependencies) :]:
            repair = by_id.get(repair_id)
            if (
                not isinstance(repair, dict)
                or repair.get("repairOf") != task_id
                or repair.get("required", True) is not True
                or repair.get("candidateReset") is not True
                or repair.get("status") != "done"
            ):
                raise TaskStateError(
                    f"locked task has an invalid appended repair dependency: "
                    f"{task_id} -> {repair_id}"
                )

    for task_id, task in by_id.items():
        phase = task.get("phase")
        if task_id in locked_by_id or not isinstance(phase, str) or phase < "P13":
            continue
        if (
            task.get("repairOf") not in locked_by_id
            or task.get("required", True) is not True
            or task.get("kind") != "fix"
            or task.get("candidateReset") is not True
        ):
            raise TaskStateError(f"extra active task is not a valid repair: {task_id}")

    coverage = graph_lock.get("v2CoverageMap")
    if not isinstance(coverage, list):
        raise TaskStateError("graph lock v2CoverageMap must be an array")
    coverage_ids: list[str] = []
    for index, item in enumerate(coverage):
        if not isinstance(item, dict):
            raise TaskStateError(f"v2CoverageMap[{index}] is invalid")
        legacy_id = item.get("legacyTask")
        targets = item.get("v2Tasks")
        if (
            not isinstance(legacy_id, str)
            or legacy_id in coverage_ids
            or not isinstance(targets, list)
            or not targets
            or len(targets) != len(set(targets))
            or not all(target in locked_by_id for target in targets)
            or not isinstance(item.get("reason"), str)
            or not item["reason"]
        ):
            raise TaskStateError(f"v2CoverageMap[{index}] is invalid")
        coverage_ids.append(legacy_id)
    if set(coverage_ids) != set(historical_ids):
        raise TaskStateError(
            "v2CoverageMap legacy tasks differ from historical supersessions"
        )

    identity = graph_lock.get("repositoryIdentity")
    if not isinstance(identity, dict):
        raise TaskStateError("graph lock repositoryIdentity must be an object")
    if (
        identity.get("branch") != "dev"
        or identity.get("protectedBranch") != "main"
        or identity.get("sourceBaseline") != graph_lock.get("sourceBaseline")
        or not isinstance(identity.get("origin"), str)
        or not identity["origin"]
    ):
        raise TaskStateError("graph lock repositoryIdentity is invalid")

    callmesh = graph_lock.get("callMeshServiceModel")
    if not isinstance(callmesh, dict) or (
        callmesh.get("productionBaseUrl") != "https://callmesh.tmmarc.org"
        or callmesh.get("productionAuthority") != "official-hosted-only"
        or callmesh.get("selfHosting") is not False
        or callmesh.get("productionEndpointOverride") is not False
        or callmesh.get("localMappingOverride") is not False
        or callmesh.get("mappingAuthority") != "CallMesh-only"
    ):
        raise TaskStateError("graph lock CallMesh service model is invalid")

    payload = {field: graph_lock.get(field) for field in GRAPH_PAYLOAD_FIELDS}
    if canonical_sha256(payload) != _require_sha256(
        graph_lock.get("graphSha256"), "graph lock graphSha256"
    ):
        raise TaskStateError("graph lock graphSha256 does not match canonical payload")
    validate_license_provenance(graph_lock, license_provenance)
    return by_id


def _validate_v3_optional_task_fields(
    task: dict[str, Any], *, label: str, fixed: bool = False
) -> None:
    """Validate the scheduler/claim fields introduced by graph v3."""

    lane = task.get("lane")
    priority = task.get("priority")
    if lane is None and priority is None and task.get("status") in {"done", "skipped"}:
        # Completed pre-v3 task objects are immutable historical evidence.
        return
    if not isinstance(lane, str) or not lane.strip():
        raise TaskStateError(f"{label}.lane must be a non-empty string")
    if not isinstance(priority, int) or isinstance(priority, bool) or priority < 0:
        raise TaskStateError(f"{label}.priority must be a non-negative integer")
    for field in ("caseGroups", "caseAssertions", "observesWithoutFinalizing"):
        value = task.get(field, [])
        if not isinstance(value, list) or any(
            not isinstance(item, str) or not item.strip() for item in value
        ):
            raise TaskStateError(f"{label}.{field} must be a string array")
        if len(value) != len(set(value)):
            raise TaskStateError(f"{label}.{field} contains duplicates")
    evidence = task.get("evidenceClaim")
    if evidence is not None and not isinstance(evidence, dict):
        raise TaskStateError(f"{label}.evidenceClaim must be an object or null")
    if evidence is not None:
        required = {
            "identityLevel",
            "observationOnly",
            "maySatisfyCaseGroups",
            "forbiddenClaims",
        }
        if set(evidence) != required:
            raise TaskStateError(f"{label}.evidenceClaim fields are invalid")
        if evidence.get("identityLevel") != "hardware-source":
            raise TaskStateError(f"{label}.evidenceClaim.identityLevel is invalid")
        if evidence.get("observationOnly") is not True:
            raise TaskStateError(f"{label}.evidenceClaim.observationOnly must be true")
        if evidence.get("maySatisfyCaseGroups") != ["HWS"]:
            raise TaskStateError(
                f"{label}.evidenceClaim.maySatisfyCaseGroups must be ['HWS']"
            )
        forbidden = evidence.get("forbiddenClaims")
        if not isinstance(forbidden, list) or not {
            "V3",
            "installed candidate",
            "final live",
            "recovery",
            "soak",
            "production",
        }.issubset(forbidden):
            raise TaskStateError(f"{label}.evidenceClaim.forbiddenClaims is incomplete")
    if fixed and task.get("repairOf") is not None and not isinstance(
        task.get("repairOf"), str
    ):
        raise TaskStateError(f"{label}.repairOf must be a task ID or null")
    if task.get("supersedesPartOf") is not None and not isinstance(
        task.get("supersedesPartOf"), str
    ):
        raise TaskStateError(f"{label}.supersedesPartOf must be a task ID or null")


def _validate_v3_scheduler(
    graph_lock: dict[str, Any], by_id: dict[str, dict[str, Any]]
) -> None:
    scheduler = graph_lock.get("scheduler")
    if not isinstance(scheduler, dict):
        raise TaskStateError("graph lock scheduler must be an object")
    priorities = scheduler.get("priorities")
    if not isinstance(priorities, dict) or not priorities:
        raise TaskStateError("graph lock scheduler priorities are invalid")
    if any(
        not isinstance(lane, str)
        or not isinstance(priority, int)
        or isinstance(priority, bool)
        or priority < 0
        for lane, priority in priorities.items()
    ):
        raise TaskStateError("graph lock scheduler priorities are invalid")
    if not isinstance(scheduler.get("selectionRule"), str):
        raise TaskStateError("graph lock scheduler selectionRule is invalid")
    for task_id, task in by_id.items():
        if task_id.startswith("P13-") or task_id.startswith("P14-") or task_id.startswith(
            ("P15-", "P16-", "P17-", "P18-")
        ):
            _validate_v3_optional_task_fields(task, label=f"task {task_id}")
            lane = task.get("lane")
            if lane is None and task.get("priority") is None and task.get(
                "status"
            ) in {"done", "skipped"}:
                continue
            if lane not in priorities:
                raise TaskStateError(f"task {task_id} uses unknown scheduler lane {lane!r}")
            if task.get("priority") != priorities[lane]:
                raise TaskStateError(f"task {task_id} priority disagrees with lane")


def _validate_v3_completion_contract(graph_lock: dict[str, Any]) -> None:
    checkers = graph_lock.get("completionCheckers")
    if not isinstance(checkers, dict) or not isinstance(checkers.get("global"), dict):
        raise TaskStateError("graph lock completionCheckers.global is invalid")
    global_checker = checkers["global"]
    preserved = global_checker.get("requiredActiveRoot", {}).get(
        "completionChecker"
    )
    if preserved != graph_lock.get("completionChecker"):
        raise TaskStateError("global completion checker was not preserved byte-for-byte")
    scoped = checkers.get("windowsLiveFirst")
    if not isinstance(scoped, dict) or scoped.get("task") != "P18-T10":
        raise TaskStateError("windowsLiveFirst completion checker is invalid")
    scoped_rule = graph_lock.get("scopedCompletion")
    if not isinstance(scoped_rule, dict) or scoped_rule.get("task") != "P18-T10":
        raise TaskStateError("scopedCompletion contract is invalid")
    if scoped_rule.get("mandatoryEarlyMilestone") != "P18-T02":
        raise TaskStateError("windowsLiveFirst mandatory milestone is invalid")
    if scoped_rule.get("continuousSoakHours") != 24:
        raise TaskStateError("scopedCompletion must require a 24-hour soak")
    allowlist = graph_lock.get("completionToolOnlyRepairAllowlist")
    if not isinstance(allowlist, dict):
        raise TaskStateError("completion-tool-only allowlist is invalid")
    paths = allowlist.get("paths")
    if not isinstance(paths, list) or len(paths) != len(set(paths)):
        raise TaskStateError("completion-tool-only allowlist paths are invalid")
    for path in paths:
        if (
            not isinstance(path, str)
            or not path
            or "*" in path
            or "?" in path
            or path.endswith("/")
            or "\\" in path
            or path.startswith("/")
        ):
            raise TaskStateError("completion-tool-only allowlist contains a glob or directory")
    expected = canonical_sha256({"paths": paths})
    if allowlist.get("sha256") != expected:
        raise TaskStateError("completion-tool-only allowlist digest drift")


def _validate_state_against_graph_lock_v3(
    state: dict[str, Any],
    graph_lock: dict[str, Any],
    license_provenance: dict[str, Any],
) -> dict[str, dict]:
    by_id = validate_task_graph(state)
    if graph_lock.get("schema") != GRAPH_LOCK_SCHEMA_V3:
        raise TaskStateError(f"graph lock schema must be {GRAPH_LOCK_SCHEMA_V3}")
    active = state.get("activeGraph")
    if not isinstance(active, dict):
        raise TaskStateError("state.activeGraph must be an object")
    if active.get("id") != "unified-product" or active.get("version") != 3:
        raise TaskStateError("active graph must be unified-product@3")
    if graph_lock.get("id") != "unified-product" or graph_lock.get("version") != 3:
        raise TaskStateError("committed graph lock must be unified-product@3")
    if active.get("branch") != "dev" or graph_lock.get("branch") != "dev":
        raise TaskStateError("active graph and graph lock branch must be dev")

    # The v2 history and completion contract are immutable roots of v3.
    historical = graph_lock.get("historicalSupersessions")
    if not isinstance(historical, list):
        raise TaskStateError("graph lock historicalSupersessions must be an array")
    historical_ids: list[str] = []
    for index, item in enumerate(historical):
        if not isinstance(item, dict) or item.get("graphVersion") != 1:
            raise TaskStateError(f"historicalSupersessions[{index}] must retain graphVersion 1")
        old_id, replacements, reason = item.get("old"), item.get("new"), item.get("reason")
        if (
            not isinstance(old_id, str)
            or old_id in historical_ids
            or not isinstance(replacements, list)
            or not replacements
            or len(replacements) != len(set(replacements))
            or not all(isinstance(value, str) for value in replacements)
            or not isinstance(reason, str)
            or not reason
        ):
            raise TaskStateError(f"historicalSupersessions[{index}] is invalid")
        historical_ids.append(old_id)
        task = by_id.get(old_id)
        if not isinstance(task, dict) or task.get("status") != "skipped":
            raise TaskStateError(f"historical supersession task is not skipped: {old_id}")
        if task.get("supersededBy") != replacements or task.get("supersession") != {
            "graphId": graph_lock.get("id"),
            "graphVersion": 1,
            "reason": reason,
        }:
            raise TaskStateError(f"historical supersession targets drift: {old_id}")

    v3_supersessions = graph_lock.get("supersessions")
    if not isinstance(v3_supersessions, list):
        raise TaskStateError("graph lock supersessions must be an array")
    p13 = next((item for item in v3_supersessions if isinstance(item, dict) and item.get("old") == "P13-T15"), None)
    if not isinstance(p13, dict) or p13.get("new") != ["P13-T17", "P15-T14"]:
        raise TaskStateError("P13-T15 v3 supersession is missing")
    old_repair = by_id.get("P13-T15")
    if not isinstance(old_repair, dict) or old_repair.get("status") != "skipped":
        raise TaskStateError("P13-T15 must be skipped after promotion")
    if old_repair.get("supersededBy") != ["P13-T17", "P15-T14"]:
        raise TaskStateError("P13-T15 supersededBy is invalid")
    if old_repair.get("supersession") != {
        "graphId": "unified-product",
        "graphVersion": 2,
        "reason": p13.get("reason"),
    }:
        raise TaskStateError("P13-T15 supersession metadata drift")
    if "P13-T15" not in historical_ids and "P13-T15" not in active.get("supersededTaskIds", []):
        raise TaskStateError("P13-T15 is not declared superseded")

    expected_active_values = {field: graph_lock.get(field) for field in V3_ACTIVE_GRAPH_FIELDS}
    expected_active_values["supersededTaskIds"] = historical_ids + ["P13-T15"]
    for field, expected in expected_active_values.items():
        if active.get(field) != expected:
            raise TaskStateError(f"activeGraph.{field} does not match the committed graph lock")
    _parse_timestamp(active.get("importedAt"), "activeGraph.importedAt")
    _require_sha256(active.get("sourceSha256"), "activeGraph.sourceSha256")
    normalize_git_object(active.get("sourceBaseline"), "activeGraph.sourceBaseline")
    normalize_git_object(active.get("promotionBaseCommit"), "activeGraph.promotionBaseCommit")

    first_active_phase = graph_lock.get("firstActivePhase")
    if first_active_phase != "P13":
        raise TaskStateError("graph lock firstActivePhase must be P13")
    completed_history = _historical_completed_tasks(state, first_active_phase)
    expected_history = _require_sha256(graph_lock.get("completedHistorySha256"), "graph lock completedHistorySha256")
    if canonical_sha256(completed_history) != expected_history:
        raise TaskStateError("completed historical task state differs from graph lock")

    locked_tasks = graph_lock.get("tasks")
    if not isinstance(locked_tasks, list) or not locked_tasks:
        raise TaskStateError("graph lock tasks must be a non-empty array")
    if graph_lock.get("taskDefinitionCount") != len(locked_tasks):
        raise TaskStateError("graph lock taskDefinitionCount is incorrect")
    locked_by_id: dict[str, dict[str, Any]] = {}
    for index, definition in enumerate(locked_tasks):
        if not isinstance(definition, dict) or not isinstance(definition.get("id"), str):
            raise TaskStateError(f"graph lock tasks[{index}] is invalid")
        task_id = definition["id"]
        if task_id in locked_by_id:
            raise TaskStateError(f"graph lock contains duplicate task: {task_id}")
        if set(definition) != {"id", *V3_LOCKED_TASK_FIELDS, "dependsOn"}:
            raise TaskStateError(f"graph lock v3 task fields are invalid: {task_id}")
        locked_by_id[task_id] = definition

    _validate_definition_amendments(graph_lock, active, by_id, locked_by_id)
    for task_id, definition in locked_by_id.items():
        task = by_id.get(task_id)
        if task is None:
            raise TaskStateError(f"locked active task is missing: {task_id}")
        _validate_v3_optional_task_fields(task, label=f"task {task_id}", fixed=True)
        for field in V3_LOCKED_TASK_FIELDS:
            if _normalized_task_value(task, field) != definition.get(field):
                raise TaskStateError(f"locked task field changed: {task_id}.{field}")
        if task_id == "P18-T02":
            if task.get("evidenceClaim") is None or task.get("caseGroups") != [
                "HWS"
            ]:
                raise TaskStateError("P18-T02 hardware-source evidence claim drift")
        elif task.get("evidenceClaim") is not None or "HWS" in task.get(
            "caseGroups", []
        ):
            raise TaskStateError(
                f"HWS evidence is reserved for P18-T02: {task_id}"
            )
        original_dependencies = definition.get("dependsOn")
        dependencies = task.get("dependsOn")
        if not isinstance(original_dependencies, list) or not isinstance(dependencies, list):
            raise TaskStateError(f"locked task dependencies are invalid: {task_id}")
        if dependencies[: len(original_dependencies)] != original_dependencies:
            raise TaskStateError(f"locked task original dependencies changed: {task_id}")
        for repair_id in dependencies[len(original_dependencies) :]:
            repair = by_id.get(repair_id)
            if not isinstance(repair, dict) or repair.get("status") != "done":
                raise TaskStateError(f"locked task has an unfinished appended dependency: {task_id} -> {repair_id}")
            if repair.get("repairOf") != task_id and not (
                task_id == "P18-T10" and repair_id.startswith("P18-T")
            ):
                raise TaskStateError(f"locked task has an invalid appended dependency: {task_id} -> {repair_id}")

    # Dynamic repairs/attempts are allowed only under their declared protocol.
    for task_id, task in by_id.items():
        if task_id in locked_by_id or not task_id.startswith(("P13-", "P14-", "P15-", "P16-", "P17-", "P18-")):
            continue
        if task_id == "P13-T15":
            continue
        if task_id.startswith("P18-T"):
            match = TASK_ID_PATTERN.fullmatch(task_id)
            assert match is not None
            number = int(match.group(2))
            if number < 20:
                raise TaskStateError(f"unexpected dynamic P18 task: {task_id}")
            _validate_v3_optional_task_fields(task, label=f"task {task_id}", fixed=False)
            if number % 2 == 0:
                parent_id = task.get("repairOf")
                parent = by_id.get(parent_id) if isinstance(parent_id, str) else None
                if (
                    task.get("kind") != "fix"
                    or not isinstance(task.get("candidateReset"), bool)
                    or not isinstance(parent, dict)
                    or not parent_id.startswith("P18-")
                    or task.get("lane") != parent.get("lane")
                    or task.get("priority") != parent.get("priority")
                    or (parent_id != "P18-T10" and task.get("candidateReset") is not True)
                ):
                    raise TaskStateError(f"dynamic P18 repair definition is invalid: {task_id}")
            else:
                if task.get("kind") != "release" or task.get("candidateReset") is not False or task.get("scope") != "windows-completion-attempt":
                    raise TaskStateError(f"dynamic P18 attempt definition is invalid: {task_id}")
            continue
        if task.get("repairOf") is None or task.get("kind") != "fix" or task.get("required", True) is not True:
            raise TaskStateError(f"extra active task is not a valid repair: {task_id}")
        _validate_v3_optional_task_fields(task, label=f"task {task_id}", fixed=False)

    coverage = graph_lock.get("v2CoverageMap")
    if not isinstance(coverage, list):
        raise TaskStateError("graph lock v2CoverageMap must be an array")
    coverage_ids: list[str] = []
    for index, item in enumerate(coverage):
        if not isinstance(item, dict):
            raise TaskStateError(f"v2CoverageMap[{index}] is invalid")
        legacy_id = item.get("legacyTask")
        targets = item.get("v2Tasks")
        if (
            not isinstance(legacy_id, str)
            or legacy_id in coverage_ids
            or not isinstance(targets, list)
            or not targets
            or len(targets) != len(set(targets))
            or not all(target in locked_by_id for target in targets)
            or not isinstance(item.get("reason"), str)
            or not item["reason"]
        ):
            raise TaskStateError(f"v2CoverageMap[{index}] is invalid")
        coverage_ids.append(legacy_id)
    if set(coverage_ids) != set(historical_ids):
        raise TaskStateError("v2CoverageMap legacy tasks differ from historical supersessions")
    identity = graph_lock.get("repositoryIdentity")
    if not isinstance(identity, dict) or identity.get("branch") != "dev" or identity.get("protectedBranch") != "main" or identity.get("sourceBaseline") != graph_lock.get("sourceBaseline"):
        raise TaskStateError("graph lock repositoryIdentity is invalid")
    callmesh = graph_lock.get("callMeshServiceModel")
    if not isinstance(callmesh, dict):
        raise TaskStateError("graph lock CallMesh service model is invalid")

    # Validate common fields
    if (callmesh.get("productionBaseUrl") != "https://callmesh.tmmarc.org"
        or callmesh.get("productionAuthority") != "official-hosted-only"
        or callmesh.get("selfHosting") is not False
        or callmesh.get("productionEndpointOverride") is not False
        or callmesh.get("localMappingOverride") is not False):
        raise TaskStateError("graph lock CallMesh service model is invalid")

    # Validate mappingAuthority and authority-specific fields
    mapping_authority = callmesh.get("mappingAuthority")
    if mapping_authority == "CallMesh-only":
        # Legacy model - no additional fields required
        pass
    elif mapping_authority == "CMCloud-only-for-CMClient":
        # New CMCloud authority model - validate additional required fields
        if (callmesh.get("oidcAuthority") != "CallMesh"
            or callmesh.get("cmClientConsumesCallMesh") is not False
            or not isinstance(callmesh.get("legacyMappingPolicy"), str)
            or callmesh.get("compatibilityProjection") != "CMCloud-to-CallMesh near-real-time"
            or callmesh.get("aprsDecisionAuthority") != "CMCloud"
            or callmesh.get("minimumPrecisionBits") != 28
            or callmesh.get("rfPreferenceWindowMs") != 2000
            or not isinstance(callmesh.get("cloudFallbackPolicy"), str)
            or callmesh.get("aprsToRf") is not False
            or not isinstance(callmesh.get("outagePolicy"), str)):
            raise TaskStateError("graph lock CallMesh service model CMCloud authority fields are invalid")
    else:
        raise TaskStateError(f"graph lock CallMesh service model has unsupported mappingAuthority: {mapping_authority!r}")
    _validate_v3_scheduler(graph_lock, by_id)
    _validate_v3_completion_contract(graph_lock)
    amendments = graph_lock.get("existingTaskAmendments")
    if not isinstance(amendments, list) or len({item.get("task") for item in amendments if isinstance(item, dict)}) != len(amendments):
        raise TaskStateError("existingTaskAmendments are invalid")
    for item in amendments:
        if not isinstance(item, dict) or item.get("task") not in locked_by_id:
            raise TaskStateError("existingTaskAmendments names an unknown task")
    activation = graph_lock.get("activationInputs")
    if not isinstance(activation, dict):
        raise TaskStateError("activationInputs must be an object")
    for key in ("planSha256", "baselineLockSha256", "baselineStateSha256", "baselineHistorySha256"):
        _require_sha256(activation.get(key), f"activationInputs.{key}")
    payload = {field: graph_lock.get(field) for field in V3_GRAPH_PAYLOAD_FIELDS}
    if canonical_sha256(payload) != _require_sha256(graph_lock.get("graphSha256"), "graph lock graphSha256"):
        raise TaskStateError("graph lock graphSha256 does not match canonical payload")
    validate_license_provenance(graph_lock, license_provenance)
    return by_id


def validate_state_against_graph_lock(
    state: dict[str, Any],
    graph_lock: dict[str, Any],
    license_provenance: dict[str, Any],
) -> dict[str, dict]:
    """Validate either the immutable v2 graph or the promoted v3 graph."""

    schema = graph_lock.get("schema")
    if schema == GRAPH_LOCK_SCHEMA:
        return _validate_state_against_graph_lock_v2(
            state, graph_lock, license_provenance
        )
    if schema == GRAPH_LOCK_SCHEMA_V3:
        return _validate_state_against_graph_lock_v3(
            state, graph_lock, license_provenance
        )
    raise TaskStateError(f"unsupported graph lock schema: {schema!r}")


def workflow_snapshot(
    state: dict[str, Any],
    graph_lock_path: Path,
    license_path: Path,
) -> dict[str, str]:
    return {
        "stateSha256": canonical_sha256(state),
        "graphLockSha256": sha256_file(graph_lock_path),
        "licenseProvenanceSha256": sha256_file(license_path),
    }


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
    expected_candidate_reset = repair.get("candidateReset") is not False
    if record.get("runtimeCandidate") is not expected_candidate_reset:
        raise TaskStateError(
            f"{label}.runtimeCandidate disagrees with candidateReset"
        )
    if record.get("distributionCandidate") is not expected_candidate_reset:
        raise TaskStateError(
            f"{label}.distributionCandidate disagrees with candidateReset"
        )
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
    source_repair = record.get("sourceRepair")
    if source_repair is None:
        if record.get("invalidatedAt") != repair.get("startedAt"):
            raise TaskStateError(f"{label}.invalidatedAt disagrees with {repair_id}")
    else:
        if (
            not isinstance(source_repair, str)
            or repair.get("supersedesPartOf") != source_repair
            or repair.get("status") not in {"pending", "blocked", "in_progress", "done"}
        ):
            raise TaskStateError(f"{label}.sourceRepair is invalid for {repair_id}")
        source = by_id.get(source_repair)
        source_record = source.get("supersededCandidateInvalidation") if isinstance(source, dict) else None
        if not isinstance(source_record, dict) or source_record.get("invalidatedAt") != record.get("invalidatedAt"):
            raise TaskStateError(f"{label}.sourceRepair preimage is unavailable")
        if not isinstance(record.get("targetScope"), str) or not record[
            "targetScope"
        ]:
            raise TaskStateError(f"{label}.targetScope is required for a split invalidation")
        if not isinstance(record.get("blocksScopedCompletion"), bool):
            raise TaskStateError(
                f"{label}.blocksScopedCompletion is required for a split invalidation"
            )

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
        "sourceRepair",
        "targetScope",
        "blocksScopedCompletion",
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


def read_validated_state(
    state_path: Path = DEFAULT_STATE_PATH,
    *,
    graph_lock_path: Path = DEFAULT_GRAPH_LOCK_PATH,
    license_path: Path | None = None,
    graph_upgrade_operation_id: str | None = None,
) -> dict:
    state_path = Path(state_path)
    graph_lock_path = Path(graph_lock_path)
    license_path = Path(license_path or state_path.with_name("LICENSE_PROVENANCE.json"))
    with state_lock(state_path):
        validate_upgrade_journal_guard(state_path, graph_upgrade_operation_id)
        state = load_json(state_path)
        graph_lock = _load_document(graph_lock_path, "unified task graph lock")
        license_provenance = _load_document(license_path, "license provenance")
        validate_state_against_graph_lock(state, graph_lock, license_provenance)
        return copy.deepcopy(state)


def mutate_state(
    state_path: Path,
    mutation: Callable[[dict], T],
    *,
    graph_lock_path: Path = DEFAULT_GRAPH_LOCK_PATH,
    license_path: Path | None = None,
    graph_upgrade_operation_id: str | None = None,
) -> tuple[dict, T]:
    """Validate, mutate, validate again, then atomically persist under one lock."""

    state_path = Path(state_path)
    graph_lock_path = Path(graph_lock_path)
    license_path = Path(license_path or state_path.with_name("LICENSE_PROVENANCE.json"))
    with state_lock(state_path):
        validate_upgrade_journal_guard(state_path, graph_upgrade_operation_id)
        state = load_json(state_path)
        graph_lock = _load_document(graph_lock_path, "unified task graph lock")
        license_provenance = _load_document(license_path, "license provenance")
        validate_state_against_graph_lock(state, graph_lock, license_provenance)
        expected_contract = workflow_snapshot(state, graph_lock_path, license_path)
        original = copy.deepcopy(state)
        result = mutation(state)
        repeated_lock = _load_document(graph_lock_path, "unified task graph lock")
        repeated_license = _load_document(license_path, "license provenance")
        if sha256_file(graph_lock_path) != expected_contract["graphLockSha256"]:
            raise TaskStateError("graph lock changed during task-state mutation")
        if (
            sha256_file(license_path)
            != expected_contract["licenseProvenanceSha256"]
        ):
            raise TaskStateError("license provenance changed during task-state mutation")
        validate_state_against_graph_lock(state, repeated_lock, repeated_license)
        if state != original:
            atomic_write_json(state_path, state)
        return copy.deepcopy(state), result


def checkpoint_readiness_snapshot(
    state_path: Path,
    task_id: str,
    expected_head: str,
    *,
    graph_lock_path: Path = DEFAULT_GRAPH_LOCK_PATH,
    license_path: Path | None = None,
    graph_upgrade_operation_id: str | None = None,
) -> dict[str, str]:
    state_path = Path(state_path)
    graph_lock_path = Path(graph_lock_path)
    license_path = Path(license_path or state_path.with_name("LICENSE_PROVENANCE.json"))
    expected_head = normalize_git_object(expected_head, "checkpoint expected HEAD")
    with state_lock(state_path):
        validate_upgrade_journal_guard(state_path, graph_upgrade_operation_id)
        state = load_json(state_path)
        graph_lock = _load_document(graph_lock_path, "unified task graph lock")
        license_provenance = _load_document(license_path, "license provenance")
        by_id = validate_state_against_graph_lock(
            state, graph_lock, license_provenance
        )
        task = by_id.get(task_id)
        if task is None:
            raise TaskStateError(f"unknown task: {task_id}")
        if task.get("status") != "in_progress":
            raise TaskStateError(
                f"task must be in_progress before checkpoint: {task.get('status')}"
            )
        unfinished = [
            dependency
            for dependency in task.get("dependsOn", [])
            if by_id[dependency].get("status") != "done"
        ]
        if unfinished:
            raise TaskStateError(
                "task dependencies are not done: " + ", ".join(unfinished)
            )
        checkpoint_base = normalize_git_object(
            task.get("checkpointBaseCommit"),
            f"task {task_id}.checkpointBaseCommit",
        )
        if checkpoint_base != expected_head:
            raise TaskStateError(
                "task checkpointBaseCommit does not match pre-commit HEAD: "
                f"{checkpoint_base} != {expected_head}"
            )
        return {
            "task": task_id,
            "checkpointBaseCommit": checkpoint_base,
            **workflow_snapshot(state, graph_lock_path, license_path),
        }


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

    candidates: list[tuple[int, int, dict]] = []
    for index, task in enumerate(state["tasks"]):
        if not task.get("required", True) or task.get("manualGate", False):
            continue
        if task["status"] != "pending":
            continue
        if _dependencies_done(task, by_id):
            # v3 schedules by lane priority and then by immutable graph order.
            # A v2 state has no priority, so its existing list order is kept.
            priority = task.get("priority", 1000)
            if not isinstance(priority, int) or isinstance(priority, bool):
                priority = 1000
            candidates.append((priority, index, task))
    if candidates:
        candidates.sort(key=lambda item: (item[0], item[1]))
        return candidates[0][2]
    return None


def allocate_repair_id(state: dict, parent: dict) -> str:
    by_id = validate_task_graph(state)
    match = TASK_ID_PATTERN.fullmatch(parent["id"])
    assert match is not None
    phase = parent.get("phase", match.group(1))
    if state.get("activeGraph", {}).get("version") == 3 and phase == "P18":
        reserved = set(by_id)
        for value in state.get("immutableTaskIds", []):
            if isinstance(value, str):
                reserved.add(value)
        for sequence in range(20, 99, 2):
            candidate_id = f"P18-T{sequence:02d}"
            if candidate_id not in reserved:
                return candidate_id
        raise TaskStateError(
            "repair task ID space exhausted for P18; even repair IDs end at T98"
        )
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
    if state.get("activeGraph", {}).get("version") == 3:
        repair.update(
            {
                "lane": parent.get("lane"),
                "priority": parent.get("priority"),
                "environmental": False,
                "supersedesPartOf": None,
                "caseGroups": [],
                "caseAssertions": [],
                "evidenceClaim": None,
                "observesWithoutFinalizing": [],
            }
        )
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
