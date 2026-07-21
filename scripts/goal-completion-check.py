#!/usr/bin/env python3
"""Fail-closed machine gate for the unified-product Goal.

The checker intentionally consumes explicit JSON inputs instead of deriving a
completion claim from ``next-task`` output.  Defaults are workspace-relative,
while every stateful path can be overridden for isolated tests and campaigns.

Candidate JSON uses ``schema = cmclient-unified-candidate/v1``.  The runtime
object identifies the exact Windows x86-64 executable bytes with ``id``,
``target``, ``fileName``, ``sha256``, and ``sizeBytes``.  The distribution
object contains these exact public ``artifacts``::

    windows-x86_64-setup, macos-universal-dmg,
    linux-x86_64-appimage, linux-aarch64-appimage, docker-compose

Each artifact has ``id``, ``kind``, ``target``, ``fileName``, ``sha256``, and
positive ``sizeBytes``.  The exact ``images`` set is ``cmclient-oci-index``,
``cmclient-oci-amd64``, and ``cmclient-oci-arm64``; image records use an exact
``sha256:<64 hex>`` digest and positive ``sizeBytes``.  Additional non-public
checksums/SBOM/provenance/update files belong in ``supportArtifacts``.

Evidence JSON uses ``schema = cmclient-unified-evidence/v1`` and has exactly one
passing, sanitized, candidate-bound record for every required case declared in
``REQUIRED_EVIDENCE_CASES`` below.  Every record repeats campaign/source/tree/
candidate identity and references an existing relative evidence file with an
exact SHA-256.  Subcase sets map directly to VERIFICATION.md; package/Docker V3
and production actions remain explicit pending manual deferrals.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import importlib.util
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from types import ModuleType
from typing import Any


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = Path(
    os.environ.get("CMCLIENT_WORKSPACE_ROOT", REPOSITORY_ROOT.parent.parent)
).resolve()
TASK_STATE_LIB = Path(__file__).with_name("task-state-lib.py")
RECONCILE_LIB = Path(__file__).with_name("reconcile-task-state.py")
DEFAULT_GRAPH_LOCK = Path(__file__).with_name("unified-task-graph-lock.json")
SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")
GIT_OBJECT_RE = re.compile(r"^[0-9a-fA-F]{40,64}$")
GRAPH_LOCK_SCHEMA = "cmclient-unified-task-graph-lock/v2"
CANDIDATE_SCHEMA = "cmclient-unified-candidate/v1"
EVIDENCE_SCHEMA = "cmclient-unified-evidence/v1"
INVALIDATION_RERUN_SCHEMA = "cmclient-invalidation-reruns/v1"
PRECHECK_ATTESTATION_SCHEMA = "cmclient-goal-precheck/v2"
FIRST_ACTIVE_PHASE = "P13"
GITHUB_RUN_URL_RE = re.compile(
    r"^https://github\.com/toodi0418/CMClient/actions/runs/(?P<run>[1-9][0-9]*)$"
)
GITHUB_JOB_URL_RE = re.compile(
    r"^https://github\.com/toodi0418/CMClient/actions/runs/"
    r"(?P<run>[1-9][0-9]*)/job/(?P<job>[1-9][0-9]*)$"
)
REQUIRED_PUBLIC_ARTIFACTS = {
    "windows-x86_64-setup": ("setup", "windows/x86_64"),
    "macos-universal-dmg": ("dmg", "macos/universal"),
    "linux-x86_64-appimage": ("appimage", "linux/x86_64"),
    "linux-aarch64-appimage": ("appimage", "linux/aarch64"),
    "docker-compose": ("compose", "linux/multi"),
}
REQUIRED_OCI_IMAGES = {
    "cmclient-oci-index": ("oci-index", "linux/multi"),
    "cmclient-oci-amd64": ("oci-image", "linux/amd64"),
    "cmclient-oci-arm64": ("oci-image", "linux/arm64"),
}
REQUIRED_SUPPORT_ARTIFACTS = {
    "checksums": ("checksums", "all"),
    "sbom": ("sbom", "all"),
    "provenance": ("provenance", "all"),
    "update-manifest": ("update-manifest", "native"),
}
REQUIRED_EVIDENCE_CASES = (
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
)
CAMPAIGN_CLEANUP_PATHS = (
    "physicalRoot",
    "logicalRoot",
    "verificationWorktree",
    "childHome",
    "temp",
    "build",
    "packages",
    "runtime",
    "evidence",
    "updateLab",
)
REQUIRED_QUALIFICATION_GATES = tuple(f"TG-{number:02d}" for number in range(1, 15))
REQUIRED_SUBCASES = {
    "SUPPLY_CHAIN": {"CHECKSUMS", "SBOM", "PROVENANCE", "UPDATE_MANIFEST"},
    "LIVE_DATA": {
        "MESHTASTIC_TCP_PASSIVE",
        "CALLMESH_PROVISION",
        "APRS_IS_VERIFIED",
    },
    "CLIENTS": {
        "PROXY_MULTI_CLIENT",
        "MANAGEMENT_WEB",
        "GRAPHICAL_MODE",
        "COMMAND_MODE",
    },
    "RECOVERY": {
        "PERSISTENCE",
        "BACKUP",
        "RESTORE",
        "UPDATE",
        "ROLLBACK",
        "RESET",
    },
    "DOCKER_MATRIX": {
        "DOCKER_COMPOSE_E2E",
        "DOCKER_AMD64_CANDIDATE",
        "DOCKER_ARM64_CANDIDATE",
        "DOCKER_UPDATE_ROLLBACK",
    },
    "CLEANUP": {
        "PROCESSES_CLOSED",
        "LISTENERS_CLOSED",
        "RAW_CAMPAIGN_REMOVED",
        "REPOSITORY_CLEAN",
    },
    "LIVE_SOAK_24H": {
        "HEALTH",
        "RSS",
        "HANDLES",
        "THREADS",
        "DB",
        "WAL",
        "LOG",
        "UPSTREAM_COUNT",
        "NO_DUPLICATE_DOMAIN",
        "APRS_ORDERING",
        "NO_ORPHANS",
        "RECOVERY_BUDGETS",
    },
}
PACKAGE_V3_DEFERRALS = {
    "WINDOWS_11_V3",
    "MACOS_INTEL_V3",
    "MACOS_APPLE_SILICON_V3",
    "LINUX_X86_64_V3",
    "LINUX_AARCH64_V3",
}
DOCKER_V3_DEFERRALS = {"DOCKER_AMD64_V3", "DOCKER_ARM64_V3"}
PRODUCTION_DEFERRALS = {
    "PRODUCTION_SIGNING",
    "MAIN_PROMOTION",
    "TAG_PUBLICATION",
}
ALL_REQUIRED_DEFERRALS = (
    PACKAGE_V3_DEFERRALS | DOCKER_V3_DEFERRALS | PRODUCTION_DEFERRALS
)
SECRET_KEY_NAMES = {
    "apikey",
    "authorization",
    "bearertoken",
    "clientsecret",
    "cookie",
    "credential",
    "credentials",
    "password",
    "passcode",
    "passphrase",
    "privatekey",
    "refreshtoken",
    "secret",
    "secrets",
    "sessioncookie",
    "token",
}
SECRET_VALUE_PATTERNS = (
    re.compile(r"-----BEGIN(?: [A-Z0-9]+)? PRIVATE KEY-----", re.IGNORECASE),
    re.compile(r"\b(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})\b"),
    re.compile(r"\b(?:sk-(?:proj-)?|tvly-(?:prod-)?)\w[A-Za-z0-9_-]{10,}\b"),
    re.compile(r"\b(?:bearer|basic)\s+[A-Za-z0-9._~+/=-]{12,}\b", re.IGNORECASE),
    re.compile(r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b"),
    re.compile(r"\b(?:api[_-]?key|password|passcode|passphrase|secret|access[_-]?token|refresh[_-]?token)\s*[:=]\s*[^\s,;]{6,}", re.IGNORECASE),
    re.compile(r"\b[a-z][a-z0-9+.-]*://[^\s/:@]+:[^\s/@]+@", re.IGNORECASE),
)


class Gate:
    def __init__(self) -> None:
        self.errors: list[str] = []

    def require(self, condition: bool, message: str) -> bool:
        if not condition:
            self.errors.append(message)
        return condition

    def error(self, message: str) -> None:
        self.errors.append(message)


def load_adjacent_module(path: Path, name: str, label: str, gate: Gate) -> ModuleType | None:
    try:
        spec = importlib.util.spec_from_file_location(name, path)
        if spec is None or spec.loader is None:
            raise ImportError("module loader is unavailable")
        module = importlib.util.module_from_spec(spec)
        previous = sys.dont_write_bytecode
        sys.dont_write_bytecode = True
        sys.modules[name] = module
        try:
            spec.loader.exec_module(module)
        except Exception:
            sys.modules.pop(name, None)
            raise
        finally:
            sys.dont_write_bytecode = previous
        return module
    except (ImportError, OSError, SyntaxError) as error:
        gate.error(f"cannot load {label}: {error}")
        return None


def load_task_state_module(gate: Gate) -> ModuleType | None:
    """Load the authoritative graph validator without making pycache."""

    return load_adjacent_module(
        TASK_STATE_LIB, "cmclient_task_state_lib", "task-state invariants", gate
    )


def load_reconcile_module(gate: Gate) -> ModuleType | None:
    """Load the canonical checkpoint-message parser and Git runner."""

    return load_adjacent_module(
        RECONCILE_LIB, "cmclient_reconcile_task_state", "checkpoint parser", gate
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, default=WORKSPACE_ROOT / "state/TASKS.json")
    parser.add_argument("--repo", type=Path, default=REPOSITORY_ROOT)
    parser.add_argument("--campaign", type=Path, default=WORKSPACE_ROOT / "state/CAMPAIGN.json")
    parser.add_argument("--candidate", type=Path, default=WORKSPACE_ROOT / "state/CANDIDATE.json")
    parser.add_argument("--evidence", type=Path, default=WORKSPACE_ROOT / "state/EVIDENCE.json")
    parser.add_argument("--graph-lock", type=Path, default=DEFAULT_GRAPH_LOCK)
    parser.add_argument(
        "--license-provenance",
        type=Path,
        default=WORKSPACE_ROOT / "state/LICENSE_PROVENANCE.json",
    )
    parser.add_argument(
        "--precheck-attestation",
        type=Path,
        default=WORKSPACE_ROOT / "state/GOAL_PRECHECK.json",
    )
    parser.add_argument(
        "--write-precheck-attestation",
        action="store_true",
        help="Write the exact P17 pre-check proof after an excluded run passes.",
    )
    parser.add_argument(
        "--exclude-task",
        action="append",
        default=[],
        metavar="TASK_ID",
        help="Repeatable; only the in-progress completion task may be excluded.",
    )
    return parser.parse_args()


def load_json(path: Path, label: str, gate: Gate) -> dict[str, Any] | None:
    if not path.is_file():
        gate.error(f"{label} file does not exist: {path}")
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        gate.error(f"cannot read {label} JSON {path}: {error}")
        return None
    if not isinstance(value, dict):
        gate.error(f"{label} JSON root must be an object: {path}")
        return None
    return value


def git(repo: Path, *args: str) -> tuple[bool, str]:
    environment = os.environ.copy()
    environment.update(
        {
            "GIT_TERMINAL_PROMPT": "0",
            "GCM_INTERACTIVE": "Never",
        }
    )
    try:
        result = subprocess.run(
            ["git", "-C", str(repo), *args],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=environment,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        return False, str(error)
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or f"exit {result.returncode}"
        return False, detail
    return True, result.stdout.strip()


def normalize_key(value: object) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value).lower())


def normalize_sha256(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    candidate = value.strip()
    if candidate.lower().startswith("sha256:"):
        candidate = candidate.split(":", 1)[1]
    return candidate.lower() if SHA256_RE.fullmatch(candidate) else None


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_sha256(value: object) -> str:
    payload = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def completion_state_projection(state: dict[str, Any], task_id: str) -> dict[str, Any]:
    projection = copy.deepcopy(state)
    tasks = projection.get("tasks")
    if not isinstance(tasks, list):
        return projection
    for task in tasks:
        if isinstance(task, dict) and task.get("id") == task_id:
            for field in ("status", "commit", "completedAt", "notes"):
                task.pop(field, None)
            break
    return projection


def atomic_write_private_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    payload = json.dumps(value, ensure_ascii=False, indent=2).encode("utf-8") + b"\n"
    try:
        with temporary.open("wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def precheck_file_bindings(
    candidate_path: Path,
    evidence_path: Path,
    graph_lock_path: Path,
    license_provenance_path: Path,
    gate: Gate,
) -> dict[str, str] | None:
    bindings: dict[str, str] = {}
    for name, path in (
        ("candidateSha256", candidate_path),
        ("evidenceSha256", evidence_path),
        ("graphLockSha256", graph_lock_path),
        ("licenseProvenanceSha256", license_provenance_path),
        ("checkerSha256", Path(__file__)),
    ):
        try:
            bindings[name] = sha256_file(path)
        except OSError as error:
            gate.error(f"cannot hash pre-check input {path}: {error}")
    return bindings if len(bindings) == 5 else None


def build_precheck_attestation(
    *,
    state: dict[str, Any],
    candidate: dict[str, Any],
    campaign_id: str | None,
    source_commit: str | None,
    source_tree: str | None,
    repo_head: str,
    repository_identity: str,
    file_bindings: dict[str, str],
    executed_at: str | None = None,
) -> dict[str, Any]:
    active = state.get("activeGraph")
    completion_task = active.get("completionTask") if isinstance(active, dict) else None
    return {
        "schema": PRECHECK_ATTESTATION_SCHEMA,
        "status": "pass",
        "task": completion_task,
        "excludedTasks": [completion_task],
        "executedAt": executed_at or datetime.now(timezone.utc).isoformat(),
        "repoHead": repo_head.lower(),
        "repositoryIdentity": repository_identity,
        "campaignId": campaign_id,
        "candidateId": candidate.get("candidateId"),
        "sourceCommit": source_commit,
        "sourceTree": source_tree,
        "stateProjectionSha256": canonical_sha256(
            completion_state_projection(state, str(completion_task))
        ),
        **file_bindings,
    }


def check_precheck_attestation(
    attestation: dict[str, Any],
    *,
    state: dict[str, Any],
    candidate: dict[str, Any],
    campaign_id: str | None,
    source_commit: str | None,
    source_tree: str | None,
    repository_identity: str,
    file_bindings: dict[str, str] | None,
    repo: Path,
    gate: Gate,
) -> None:
    active = state.get("activeGraph")
    completion_task = active.get("completionTask") if isinstance(active, dict) else None
    tasks = state.get("tasks")
    completion = next(
        (
            task
            for task in tasks
            if isinstance(task, dict) and task.get("id") == completion_task
        ),
        None,
    ) if isinstance(tasks, list) else None
    gate.require(
        attestation.get("schema") == PRECHECK_ATTESTATION_SCHEMA,
        f"pre-check attestation schema must be {PRECHECK_ATTESTATION_SCHEMA}",
    )
    gate.require(attestation.get("status") == "pass", "pre-check attestation must pass")
    gate.require(
        attestation.get("task") == completion_task
        and attestation.get("excludedTasks") == [completion_task],
        "pre-check attestation does not prove the configured completion exclusion",
    )
    gate.require(
        isinstance(completion, dict)
        and completion.get("status") == "done"
        and isinstance(completion.get("commit"), str),
        "pre-check attestation requires the completed checkpoint task",
    )
    checkpoint_base = completion.get("checkpointBaseCommit") if isinstance(completion, dict) else None
    gate.require(
        attestation.get("repoHead") == checkpoint_base,
        "pre-check repository HEAD does not match the completion checkpoint base",
    )
    expected_values = {
        "repositoryIdentity": repository_identity,
        "campaignId": campaign_id,
        "candidateId": candidate.get("candidateId"),
        "sourceCommit": source_commit,
        "sourceTree": source_tree,
        "stateProjectionSha256": canonical_sha256(
            completion_state_projection(state, str(completion_task))
        ),
    }
    if file_bindings is not None:
        expected_values.update(file_bindings)
    for field, expected in expected_values.items():
        gate.require(
            attestation.get(field) == expected,
            f"pre-check attestation binding differs: {field}",
        )
    executed_at = parse_time(
        attestation.get("executedAt"), "pre-check attestation executedAt", gate
    )
    commit_id = completion.get("commit") if isinstance(completion, dict) else None
    if isinstance(commit_id, str):
        ok, committed_at_raw = git(repo, "show", "-s", "--format=%cI", commit_id)
        if not ok:
            gate.error(f"cannot read completion checkpoint time: {committed_at_raw}")
        else:
            committed_at = parse_time(
                committed_at_raw, "completion checkpoint committedAt", gate
            )
            if executed_at is not None and committed_at is not None:
                gate.require(
                    executed_at <= committed_at,
                    "pre-check attestation was created after the completion checkpoint",
                )


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


def task_lock_value(task: dict[str, Any], field: str) -> object:
    if field == "required":
        return task.get(field, True)
    if field in {"manualGate", "candidateReset"}:
        return task.get(field, False)
    return task.get(field)


def check_graph_lock(
    state: dict[str, Any],
    active: dict[str, Any],
    lock: dict[str, Any],
    task_state: ModuleType,
    license_provenance: dict[str, Any],
    gate: Gate,
) -> None:
    try:
        task_state.validate_state_against_graph_lock(
            state, lock, license_provenance
        )
    except (TypeError, ValueError) as error:
        gate.error(f"v2 graph contract failed: {error}")
        return
    gate.require(
        lock.get("schema") == GRAPH_LOCK_SCHEMA,
        f"graph lock schema must be {GRAPH_LOCK_SCHEMA}",
    )
    metadata_fields = (
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
    )
    for field in metadata_fields:
        gate.require(
            active.get(field) == lock.get(field),
            f"activeGraph.{field} does not match committed graph lock",
        )
    parse_time(active.get("importedAt"), "activeGraph.importedAt", gate)
    for field in (
        "historicalSupersessions",
        "v2CoverageMap",
        "licenseGate",
        "targetPlatforms",
        "callMeshServiceModel",
        "candidateIdentity",
        "completionChecker",
        "repairProtocol",
    ):
        gate.require(
            active.get(field) == lock.get(field),
            f"activeGraph.{field} does not match committed graph lock",
        )
    first_active_phase = lock.get("firstActivePhase")
    gate.require(
        first_active_phase == FIRST_ACTIVE_PHASE,
        f"graph lock firstActivePhase must be {FIRST_ACTIVE_PHASE}",
    )
    gate.require(
        lock.get("taskDefinitionCount") == 57,
        "graph lock taskDefinitionCount must be 57",
    )
    gate.require(
        lock.get("canonicalPayloadFields") == list(task_state.GRAPH_PAYLOAD_FIELDS),
        "graph lock canonicalPayloadFields differ from the v2 contract",
    )
    gate.require(
        lock.get("activeGraphFields") == list(task_state.ACTIVE_GRAPH_FIELDS),
        "graph lock activeGraphFields differ from the v2 contract",
    )

    tasks = state.get("tasks")
    if not isinstance(tasks, list):
        return
    first_active = next(
        (
            index
            for index, task in enumerate(tasks)
            if isinstance(task, dict)
            and isinstance(task.get("id"), str)
            and task["id"].startswith(f"{first_active_phase}-")
        ),
        None,
    )
    if first_active is None:
        gate.error(f"task state has no {first_active_phase} active graph boundary")
        return
    completed_history = [
        task
        for task in tasks[:first_active]
        if isinstance(task, dict) and task.get("status") == "done"
    ]
    expected = normalize_sha256(lock.get("completedHistorySha256"))
    gate.require(
        expected is not None,
        "graph lock completedHistorySha256 is missing or invalid",
    )
    if expected is not None:
        gate.require(
            canonical_sha256(completed_history) == expected,
            "completed historical task state differs from the bootstrap checksum",
        )

    locked_tasks = lock.get("tasks")
    if not isinstance(locked_tasks, list):
        gate.error("graph lock tasks must be an array")
        return
    locked_by_id: dict[str, dict[str, Any]] = {}
    for index, definition in enumerate(locked_tasks):
        if not isinstance(definition, dict) or not isinstance(definition.get("id"), str):
            gate.error(f"graph lock tasks[{index}] is invalid")
            continue
        task_id = definition["id"]
        if task_id in locked_by_id:
            gate.error(f"graph lock contains duplicate task: {task_id}")
        locked_by_id[task_id] = definition

    supersessions = lock.get("historicalSupersessions")
    if not isinstance(supersessions, list):
        gate.error("graph lock historicalSupersessions must be an array")
        supersessions = []
    graph_payload = {
        field: lock.get(field) for field in task_state.GRAPH_PAYLOAD_FIELDS
    }
    gate.require(
        normalize_sha256(lock.get("graphSha256"))
        == canonical_sha256(graph_payload),
        "graph lock graphSha256 does not match its canonical definitions",
    )
    gate.require(
        isinstance(lock.get("repositoryIdentity"), dict)
        and isinstance(lock["repositoryIdentity"].get("origin"), str)
        and bool(lock["repositoryIdentity"]["origin"]),
        "graph lock repositoryIdentity is missing",
    )

    state_by_id = {
        task["id"]: task
        for task in tasks
        if isinstance(task, dict) and isinstance(task.get("id"), str)
    }
    for task_id, definition in locked_by_id.items():
        task = state_by_id.get(task_id)
        if task is None:
            gate.error(f"locked active task is missing: {task_id}")
            continue
        for field in LOCKED_TASK_FIELDS:
            gate.require(
                task_lock_value(task, field) == definition.get(field),
                f"locked task field changed: {task_id}.{field}",
            )
        original_dependencies = definition.get("dependsOn")
        dependencies = task.get("dependsOn")
        if not isinstance(original_dependencies, list) or not isinstance(dependencies, list):
            gate.error(f"locked task dependencies are invalid: {task_id}")
            continue
        gate.require(
            dependencies[: len(original_dependencies)] == original_dependencies,
            f"locked task original dependencies changed: {task_id}",
        )
        for repair_id in dependencies[len(original_dependencies) :]:
            repair = state_by_id.get(repair_id)
            gate.require(
                isinstance(repair, dict)
                and repair.get("repairOf") == task_id
                and repair.get("required", True)
                and repair.get("status") == "done"
                and repair.get("candidateReset") is True,
                f"locked task has an invalid appended repair dependency: {task_id} -> {repair_id}",
            )

    for task_id, task in state_by_id.items():
        phase = task.get("phase")
        if task_id in locked_by_id or not isinstance(phase, str):
            continue
        match = re.fullmatch(r"P(\d{2})", phase)
        if match is None or int(match.group(1)) < int(FIRST_ACTIVE_PHASE[1:]):
            continue
        parent = state_by_id.get(task.get("repairOf"))
        gate.require(
            task.get("required", True)
            and task.get("kind") == "fix"
            and task.get("candidateReset") is True
            and isinstance(parent, dict)
            and task.get("status") == "done",
            f"extra active task is not a completed repair: {task_id}",
        )
        if isinstance(parent, dict):
            parent_dependencies = parent.get("dependsOn")
            gate.require(
                isinstance(parent_dependencies, list)
                and parent_dependencies.count(task_id) == 1,
                f"extra active repair is not incorporated exactly once by its parent: {task_id}",
            )

    locked_superseded_ids: list[str] = []
    for index, item in enumerate(supersessions):
        if not isinstance(item, dict) or not isinstance(item.get("old"), str):
            gate.error(f"graph lock historicalSupersessions[{index}] is invalid")
            continue
        old_id = item["old"]
        locked_superseded_ids.append(old_id)
        task = state_by_id.get(old_id)
        gate.require(
            isinstance(task, dict)
            and task.get("supersededBy") == item.get("new")
            and isinstance(task.get("supersession"), dict)
            and task["supersession"].get("graphId") == lock.get("id")
            and task["supersession"].get("graphVersion") == 1
            and item.get("graphVersion") == 1
            and task["supersession"].get("reason") == item.get("reason"),
            f"supersession mapping differs from graph lock: {old_id}",
        )
    gate.require(
        active.get("supersededTaskIds") == locked_superseded_ids,
        "activeGraph.supersededTaskIds differs from graph lock",
    )
    coverage = lock.get("v2CoverageMap")
    if isinstance(coverage, list):
        for item in coverage:
            if not isinstance(item, dict) or not isinstance(item.get("v2Tasks"), list):
                continue
            for target_id in item["v2Tasks"]:
                target = state_by_id.get(target_id)
                if not isinstance(target, dict):
                    continue
                if target.get("required", True):
                    gate.require(
                        target.get("status") == "done",
                        f"required v2 coverage target is incomplete: {target_id}",
                    )
                else:
                    gate.require(
                        target.get("manualGate") is True
                        and target.get("status") in {"pending", "done"},
                        f"optional v2 coverage target is invalid: {target_id}",
                    )


def read_structured_checkpoint_history(
    repo: Path, gate: Gate
) -> tuple[ModuleType | None, list[dict[str, Any]]]:
    reconcile = load_reconcile_module(gate)
    if reconcile is None:
        return None, []
    try:
        payload = reconcile.run_git(
            "git",
            repo,
            "log",
            "--all",
            "-z",
            "--format=%H%x00%P%x00%T%x00%cI%x00%s%x00%b",
        ).stdout
    except RuntimeError as error:
        gate.error(f"cannot scan structured checkpoint history: {error}")
        return reconcile, []
    fields = payload.split("\x00")
    if fields and fields[-1] == "":
        fields.pop()
    if len(fields) % 6 != 0:
        gate.error("cannot parse structured checkpoint history")
        return reconcile, []
    records = [
        {
            "sha": fields[index],
            "parents": fields[index + 1].split(),
            "tree": fields[index + 2],
            "committedAt": fields[index + 3],
            "subject": fields[index + 4],
            "body": fields[index + 5],
        }
        for index in range(0, len(fields), 6)
    ]
    return reconcile, records


def history_ancestors(start: str, by_sha: dict[str, dict[str, Any]]) -> set[str]:
    visited: set[str] = set()
    pending = [start]
    while pending:
        sha = pending.pop()
        if sha in visited:
            continue
        visited.add(sha)
        record = by_sha.get(sha)
        if record is not None:
            pending.extend(record["parents"])
    return visited


def check_task_checkpoints(
    repo: Path,
    task_by_id: dict[str, dict[str, Any]],
    validated_task_ids: set[str],
    gate: Gate,
) -> None:
    reconcile, records = read_structured_checkpoint_history(repo, gate)
    if reconcile is None or not records:
        return
    by_sha = {record["sha"]: record for record in records}
    ok_head, head = git(repo, "rev-parse", "HEAD")
    if not ok_head:
        gate.error(f"cannot resolve HEAD for checkpoint validation: {head}")
        return
    reachable = history_ancestors(head, by_sha)
    implicated_by_task: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        subject = reconcile.SUBJECT_RE.fullmatch(record["subject"])
        subject_task = subject.group("task") if subject else None
        body_tasks = reconcile.TASK_LINE_RE.findall(record["body"])
        implicated = set(body_tasks)
        if subject_task:
            implicated.add(subject_task)
        for task_id in implicated:
            implicated_by_task.setdefault(task_id, []).append(record)
            if record["sha"] in reachable:
                task = task_by_id.get(task_id)
                if task is None:
                    gate.error(
                        f"reachable checkpoint commit references a task absent from state: {task_id}"
                    )
                elif task.get("commit") != record["sha"]:
                    gate.error(
                        f"reachable checkpoint commit differs from recorded task SHA: {task_id}"
                    )

    for task_id in sorted(validated_task_ids):
        task = task_by_id[task_id]
        implicated = implicated_by_task.get(task_id, [])
        if len(implicated) != 1:
            gate.error(
                f"task {task_id} must have exactly one implicated checkpoint commit; found {len(implicated)}"
            )
            continue
        record = implicated[0]
        try:
            reconcile.validate_checkpoint_message(
                reconcile.CheckpointCommit(
                    sha=record["sha"],
                    parents=tuple(record["parents"]),
                    committed_at=record["committedAt"],
                    subject=record["subject"],
                    body=record["body"],
                ),
                task_id,
            )
        except RuntimeError as error:
            gate.error(str(error))
        gate.require(
            task.get("commit") == record["sha"],
            f"task {task_id} recorded commit does not match its checkpoint",
        )
        gate.require(
            record["sha"] in reachable,
            f"task checkpoint is not reachable from HEAD: {task_id}",
        )
        parents = record["parents"]
        phase = task.get("phase")
        phase_match = re.fullmatch(r"P(\d{2})", phase) if isinstance(phase, str) else None
        historical = (
            phase_match is not None
            and int(phase_match.group(1)) < int(FIRST_ACTIVE_PHASE[1:])
        )
        if not parents:
            gate.error(f"task checkpoint has no parent: {task_id}")
        elif not historical and len(parents) != 1:
            gate.error(f"active task checkpoint must have exactly one parent: {task_id}")
        else:
            parent_records = [by_sha.get(parent_sha) for parent_sha in parents]
            gate.require(
                all(parent is not None for parent in parent_records)
                and any(
                    parent.get("tree") != record.get("tree")
                    for parent in parent_records
                    if parent is not None
                ),
                f"task checkpoint has an empty diff: {task_id}",
            )
        if not historical:
            checkpoint_base = task.get("checkpointBaseCommit")
            gate.require(
                isinstance(checkpoint_base, str)
                and GIT_OBJECT_RE.fullmatch(checkpoint_base) is not None,
                f"active task lacks a valid checkpointBaseCommit: {task_id}",
            )
            if isinstance(checkpoint_base, str):
                gate.require(
                    parents == [checkpoint_base.lower()],
                    f"active task checkpoint parent differs from recorded base: {task_id}",
                )

    ancestor_cache: dict[str, set[str]] = {}
    for task_id in sorted(validated_task_ids):
        task = task_by_id[task_id]
        task_commit = task.get("commit")
        if not isinstance(task_commit, str) or task_commit not in by_sha:
            continue
        ancestors = ancestor_cache.setdefault(
            task_commit, history_ancestors(task_commit, by_sha)
        )
        for dependency_id in task.get("dependsOn", []):
            dependency = task_by_id[dependency_id]
            dependency_commit = dependency.get("commit")
            if dependency.get("status") != "done" or not isinstance(
                dependency_commit, str
            ):
                continue
            gate.require(
                dependency_commit in ancestors and dependency_commit != task_commit,
                f"dependency checkpoint is not an ancestor: {dependency_id} -> {task_id}",
            )


def parse_time(value: object, label: str, gate: Gate) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        gate.error(f"{label} must be an ISO-8601 timestamp")
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        gate.error(f"{label} is not a valid ISO-8601 timestamp")
        return None
    if parsed.tzinfo is None:
        gate.error(f"{label} must include a timezone")
        return None
    return parsed.astimezone(timezone.utc)


def scan_secret_values(value: Any, label: str, gate: Gate, pointer: str = "$") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            child_pointer = f"{pointer}.{key}"
            if normalize_key(key) in SECRET_KEY_NAMES:
                gate.error(f"{label} contains a secret-like key at {child_pointer}")
            scan_secret_values(child, label, gate, child_pointer)
        return
    if isinstance(value, list):
        for index, child in enumerate(value):
            scan_secret_values(child, label, gate, f"{pointer}[{index}]")
        return
    if not isinstance(value, str):
        return
    for pattern in SECRET_VALUE_PATTERNS:
        if pattern.search(value):
            gate.error(f"{label} contains a secret-like value at {pointer}")
            return


def scan_secret_text(path: Path, gate: Gate) -> None:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError as error:
        gate.error(f"cannot scan evidence file {path}: {error}")
        return
    for pattern in SECRET_VALUE_PATTERNS:
        if pattern.search(text):
            gate.error(f"evidence file contains a secret-like value: {path}")
            return


def valid_file_name(value: object) -> bool:
    return (
        isinstance(value, str)
        and bool(value)
        and value not in {".", ".."}
        and "/" not in value
        and "\\" not in value
    )


def valid_size(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def validate_runtime_subject(runtime: dict[str, Any], gate: Gate) -> set[str]:
    gate.require(
        isinstance(runtime.get("id"), str) and bool(runtime.get("id")),
        "runtimeCandidate.id is missing",
    )
    gate.require(
        runtime.get("target") == "windows/x86_64",
        "runtimeCandidate.target must be windows/x86_64",
    )
    gate.require(
        valid_file_name(runtime.get("fileName")),
        "runtimeCandidate.fileName must be a file name",
    )
    digest = normalize_sha256(runtime.get("sha256"))
    gate.require(digest is not None, "runtimeCandidate.sha256 is invalid")
    gate.require(
        valid_size(runtime.get("sizeBytes")),
        "runtimeCandidate.sizeBytes must be a positive integer",
    )
    return {digest} if digest else set()


def validate_artifact_list(
    value: object,
    label: str,
    expected: dict[str, tuple[str, str]] | None,
    gate: Gate,
) -> tuple[set[str], set[str]]:
    if not isinstance(value, list):
        gate.error(f"{label} must be an array")
        return set(), set()
    ids: set[str] = set()
    digests: set[str] = set()
    file_names: set[str] = set()
    for index, item in enumerate(value):
        pointer = f"{label}[{index}]"
        if not isinstance(item, dict):
            gate.error(f"{pointer} must be an object")
            continue
        item_id = item.get("id")
        if not isinstance(item_id, str) or not item_id:
            gate.error(f"{pointer}.id is missing")
            continue
        if item_id in ids:
            gate.error(f"duplicate {label} id: {item_id}")
        ids.add(item_id)
        if expected is not None:
            contract = expected.get(item_id)
            if contract is None:
                gate.error(f"unexpected public artifact id: {item_id}")
            else:
                kind, target = contract
                gate.require(item.get("kind") == kind, f"{pointer}.kind must be {kind}")
                gate.require(item.get("target") == target, f"{pointer}.target must be {target}")
        else:
            gate.require(
                isinstance(item.get("kind"), str) and bool(item.get("kind")),
                f"{pointer}.kind is missing",
            )
            gate.require(
                isinstance(item.get("target"), str) and bool(item.get("target")),
                f"{pointer}.target is missing",
            )
        file_name = item.get("fileName")
        if not valid_file_name(file_name):
            gate.error(f"{pointer}.fileName must be a file name")
        elif file_name in file_names:
            gate.error(f"duplicate {label} fileName: {file_name}")
        else:
            file_names.add(file_name)
        digest = normalize_sha256(item.get("sha256"))
        if digest is None:
            gate.error(f"{pointer}.sha256 is invalid")
        else:
            digests.add(digest)
        gate.require(
            valid_size(item.get("sizeBytes")),
            f"{pointer}.sizeBytes must be a positive integer",
        )
    if expected is not None:
        missing = sorted(set(expected) - ids)
        for item_id in missing:
            gate.error(f"missing required public artifact: {item_id}")
        extra = ids - set(expected)
        if not extra:
            gate.require(
                len(value) == len(expected),
                f"{label} must contain exactly the required public artifacts",
            )
    return ids, digests


def validate_image_list(
    value: object, gate: Gate
) -> tuple[set[str], set[str]]:
    if not isinstance(value, list):
        gate.error("distributionCandidate.images must be an array")
        return set(), set()
    ids: set[str] = set()
    digests: set[str] = set()
    for index, item in enumerate(value):
        pointer = f"distributionCandidate.images[{index}]"
        if not isinstance(item, dict):
            gate.error(f"{pointer} must be an object")
            continue
        item_id = item.get("id")
        if not isinstance(item_id, str) or not item_id:
            gate.error(f"{pointer}.id is missing")
            continue
        if item_id in ids:
            gate.error(f"duplicate distributionCandidate.images id: {item_id}")
        ids.add(item_id)
        contract = REQUIRED_OCI_IMAGES.get(item_id)
        if contract is None:
            gate.error(f"unexpected OCI image id: {item_id}")
        else:
            kind, target = contract
            gate.require(item.get("kind") == kind, f"{pointer}.kind must be {kind}")
            gate.require(item.get("target") == target, f"{pointer}.target must be {target}")
        raw_digest = item.get("digest")
        digest = normalize_sha256(raw_digest)
        if (
            digest is None
            or not isinstance(raw_digest, str)
            or not raw_digest.lower().startswith("sha256:")
        ):
            gate.error(f"{pointer}.digest must be sha256:<64 hex>")
        else:
            digests.add(digest)
        gate.require(
            valid_size(item.get("sizeBytes")),
            f"{pointer}.sizeBytes must be a positive integer",
        )
    for item_id in sorted(set(REQUIRED_OCI_IMAGES) - ids):
        gate.error(f"missing required OCI image: {item_id}")
    if not (ids - set(REQUIRED_OCI_IMAGES)):
        gate.require(
            len(value) == len(REQUIRED_OCI_IMAGES),
            "distributionCandidate.images must contain exactly the required OCI subjects",
        )
    return ids, digests


def check_task_state(
    state: dict[str, Any], excluded: list[str], repo: Path,
    graph_lock: dict[str, Any], license_provenance: dict[str, Any], gate: Gate
) -> tuple[str | None, set[str]]:
    active = state.get("activeGraph")
    if not isinstance(active, dict):
        gate.error("state.activeGraph must be an object")
        return None, set()

    task_state = load_task_state_module(gate)
    if task_state is None:
        return None, set()
    try:
        task_by_id = task_state.validate_task_graph(state)
    except (ValueError, TypeError) as error:
        gate.error(f"task graph invariant failed: {error}")
        return active.get("branch") if isinstance(active.get("branch"), str) else None, set()
    check_graph_lock(
        state, active, graph_lock, task_state, license_provenance, gate
    )

    branch = active.get("branch")
    gate.require(branch == "dev", "active graph branch must be dev")
    completion_task = active.get("completionTask")
    checker_task = (active.get("completionChecker") or {}).get("task") if isinstance(active.get("completionChecker"), dict) else None
    gate.require(isinstance(completion_task, str), "active graph completionTask is missing")
    if checker_task is not None:
        gate.require(checker_task == completion_task, "completion checker task disagrees with completionTask")
    manual_release_task = active.get("manualReleaseTask")
    manual_release = task_by_id.get(manual_release_task)
    gate.require(
        isinstance(manual_release_task, str)
        and isinstance(manual_release, dict)
        and manual_release.get("required") is False
        and manual_release.get("manualGate") is True
        and manual_release.get("status") == "pending",
        "formal manual release task must exist and remain pending",
    )

    excluded_ids = set(excluded)
    if len(excluded_ids) != len(excluded):
        gate.error("duplicate --exclude-task values are not allowed")
    for task_id in sorted(excluded_ids):
        task = task_by_id.get(task_id)
        if task_id != completion_task or task_id != checker_task:
            gate.error(f"only the configured completion task may be excluded: {task_id}")
        elif task is None:
            gate.error(f"excluded completion task does not exist: {task_id}")
        elif task.get("status") != "in_progress":
            gate.error(f"excluded completion task must be in_progress: {task_id}")
    if len(excluded_ids) > 1:
        gate.error("only one completion task exclusion is permitted")

    superseded = active.get("supersededTaskIds")
    # The shared validator already checks type and duplicates.
    superseded_ids = set(superseded or [])
    graph_id = active.get("id")
    for task_id in sorted(superseded_ids):
        task = task_by_id.get(task_id)
        if task is None:
            gate.error(f"activeGraph lists an unknown superseded task: {task_id}")
            continue
        gate.require(
            task.get("required", True) and task.get("status") == "skipped",
            f"superseded historical task must be required and skipped: {task_id}",
        )
        supersession = task.get("supersession")
        gate.require(
            isinstance(supersession, dict)
            and supersession.get("graphId") == graph_id,
            f"superseded task has invalid graph metadata: {task_id}",
        )
        for replacement_id in task.get("supersededBy", []):
            replacement = task_by_id.get(replacement_id)
            if replacement is None:
                # Kept explicit even though task-state-lib rejects this first.
                gate.error(f"invalid supersededBy replacement: {task_id} -> {replacement_id}")
                continue
            if replacement_id in superseded_ids or replacement.get("status") == "skipped":
                gate.error(f"supersededBy replacement is itself superseded: {task_id} -> {replacement_id}")
            if replacement.get("required", True):
                replacement_done = replacement.get("status") == "done" or (
                    replacement_id in excluded_ids
                    and replacement.get("status") == "in_progress"
                )
                gate.require(
                    replacement_done,
                    f"required supersededBy replacement is incomplete: {task_id} -> {replacement_id}",
                )
            else:
                gate.require(
                    replacement.get("manualGate") is True
                    and replacement.get("status") in {"pending", "done"},
                    f"optional supersededBy replacement is not a valid manual gate: {task_id} -> {replacement_id}",
                )

    active_required_count = 0
    commit_owners: dict[str, list[str]] = {}
    validated_task_ids: set[str] = set()
    for task_id, task in task_by_id.items():
        required = task.get("required", True)
        status = task.get("status")
        if not required:
            # Optional/manual gates may remain pending for the autonomous Goal. If one
            # was performed, it must satisfy the same immutable checkpoint rules.
            if task.get("manualGate") is True and status not in {"pending", "done"}:
                gate.error(f"optional manual task must be pending or done: {task_id} ({status})")
            if status == "done":
                validated_task_ids.add(task_id)
                unfinished_dependencies = [
                    dependency
                    for dependency in task.get("dependsOn", [])
                    if task_by_id[dependency].get("status") != "done"
                ]
                if unfinished_dependencies:
                    gate.error(
                        f"done optional task has unfinished dependencies: {task_id} -> "
                        + ", ".join(unfinished_dependencies)
                    )
                commit = task.get("commit")
                if not isinstance(commit, str) or not GIT_OBJECT_RE.fullmatch(commit):
                    gate.error(f"done optional task lacks a valid commit: {task_id}")
                else:
                    commit_owners.setdefault(commit.lower(), []).append(task_id)
            continue
        if task_id in superseded_ids:
            continue
        active_required_count += 1
        if task_id in excluded_ids:
            continue
        if status != "done":
            gate.error(f"required active task is not done: {task_id} ({status})")
            continue
        validated_task_ids.add(task_id)
        unfinished_dependencies = [
            dependency
            for dependency in task.get("dependsOn", [])
            if task_by_id[dependency].get("status") != "done"
        ]
        if unfinished_dependencies:
            gate.error(
                f"done required task has unfinished dependencies: {task_id} -> "
                + ", ".join(unfinished_dependencies)
            )
        commit = task.get("commit")
        if not isinstance(commit, str) or not GIT_OBJECT_RE.fullmatch(commit):
            gate.error(f"done required task lacks a valid commit: {task_id}")
        else:
            commit_owners.setdefault(commit.lower(), []).append(task_id)

    gate.require(active_required_count > 0, "state has no active required tasks; NO_READY_TASK is not completion")
    gate.require(completion_task in task_by_id, "completion task is missing from state.tasks")

    for commit, owners in sorted(commit_owners.items()):
        if len(owners) > 1:
            gate.error(
                f"required active tasks share one commit {commit}: "
                + ", ".join(sorted(owners))
            )
    check_task_checkpoints(repo, task_by_id, validated_task_ids, gate)
    return branch if isinstance(branch, str) else None, superseded_ids


def allowed_post_freeze_path(raw_path: str) -> bool:
    path = PurePosixPath(raw_path.replace("\\", "/"))
    parts = tuple(part.lower() for part in path.parts if part not in {"", "."})
    if not parts or ".." in parts:
        return False
    if parts[0] == "docs":
        return True
    if len(parts) == 1 and parts[0] in {"agents.md", "changelog.md", "readme.md"}:
        return True
    return False


def normalize_remote_identity(value: str) -> str:
    raw = value.strip().replace("\\", "/").rstrip("/")
    github = re.fullmatch(
        r"(?:(?:https?://|ssh://git@)github\.com/|git@github\.com:)"
        r"(?P<owner>[^/]+)/(?P<repo>[^/]+?)(?:\.git)?",
        raw,
        re.IGNORECASE,
    )
    if github is not None:
        return f"github.com/{github.group('owner').lower()}/{github.group('repo').lower()}"
    if "://" not in raw and not re.match(r"^[^/]+@[^:]+:", raw):
        return os.path.normcase(str(Path(value).expanduser().resolve()))
    return raw.lower()


def check_repo(
    repo: Path,
    expected_branch: str | None,
    source_commit: str | None,
    gate: Gate,
    repository_identity: str,
) -> tuple[str | None, str | None]:
    if not repo.is_dir():
        gate.error(f"Repository directory does not exist: {repo}")
        return None, None
    ok, branch = git(repo, "branch", "--show-current")
    if not ok:
        gate.error(f"cannot read Repository branch: {branch}")
        return None, None
    gate.require(branch == "dev", f"Repository branch must be dev, found {branch!r}")
    if expected_branch:
        gate.require(branch == expected_branch, f"Repository branch differs from active graph: {branch!r}")

    ok, status = git(repo, "status", "--porcelain=v1", "--untracked-files=all")
    if not ok:
        gate.error(f"cannot read Repository status: {status}")
    elif status:
        gate.error("Repository is not clean")

    ok_url, remote_url = git(repo, "remote", "get-url", "origin")
    if not ok_url or not remote_url:
        gate.error("origin remote is not configured")
    else:
        gate.require(
            normalize_remote_identity(remote_url)
            == repository_identity,
            "origin remote does not match the expected CMClient repository",
        )

    ok_fetch, fetch_specs = git(repo, "config", "--get-all", "remote.origin.fetch")
    fetch_maps_dev = ok_fetch and any(
        spec.endswith("refs/heads/*:refs/remotes/origin/*")
        or spec.endswith("refs/heads/dev:refs/remotes/origin/dev")
        for spec in fetch_specs.splitlines()
    )
    gate.require(fetch_maps_dev, "origin has no fetch refspec for dev")

    ok_upstream, upstream = git(
        repo, "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}"
    )
    gate.require(
        ok_upstream and upstream == "origin/dev",
        "dev must track the configured origin/dev branch",
    )

    ok_head, head = git(repo, "rev-parse", "HEAD")
    ok_tracking, tracking = git(repo, "rev-parse", "refs/remotes/origin/dev")
    if not ok_head:
        gate.error(f"cannot resolve HEAD: {head}")
        head = None
    if not ok_tracking:
        gate.error(f"cannot resolve origin/dev: {tracking}")
        tracking = None

    remote: str | None = None
    if ok_url and remote_url:
        ok_ls_remote, remote_output = git(
            repo, "ls-remote", "--exit-code", "origin", "refs/heads/dev"
        )
        if not ok_ls_remote:
            gate.error("cannot verify the live origin dev ref")
        else:
            rows = [line.split() for line in remote_output.splitlines() if line.strip()]
            if len(rows) != 1 or len(rows[0]) < 2 or rows[0][1] != "refs/heads/dev":
                gate.error("origin does not expose exactly one refs/heads/dev")
            elif not GIT_OBJECT_RE.fullmatch(rows[0][0]):
                gate.error("origin dev returned an invalid object ID")
            else:
                remote = rows[0][0].lower()

    if tracking and remote:
        gate.require(
            tracking.lower() == remote,
            f"origin/dev tracking ref is stale: tracked={tracking}, remote={remote}",
        )
    if head and tracking:
        gate.require(
            head == tracking,
            f"dev diverges from origin/dev: HEAD={head}, origin/dev={tracking}",
        )
    if head and remote:
        gate.require(
            head.lower() == remote,
            f"dev differs from the live origin dev ref: HEAD={head}, remote={remote}",
        )

    if source_commit:
        ok, detail = git(repo, "merge-base", "--is-ancestor", source_commit, "HEAD")
        if not ok:
            gate.error(f"candidate source commit is not an ancestor of HEAD: {detail}")
        else:
            # Disabling rename detection makes both the removed source path and
            # the added destination visible, so moving product code below docs/
            # cannot bypass the post-freeze policy.
            ok, changed = git(
                repo, "diff", "--name-only", "--no-renames", f"{source_commit}..HEAD"
            )
            if not ok:
                gate.error(f"cannot inspect post-freeze changes: {changed}")
            else:
                for changed_path in (line.strip() for line in changed.splitlines()):
                    if changed_path and not allowed_post_freeze_path(changed_path):
                        gate.error(f"non-document/evidence path changed after candidate freeze: {changed_path}")
    return head, remote


def candidate_source(
    candidate: dict[str, Any], gate: Gate
) -> tuple[
    str | None,
    str | None,
    set[str],
    dict[str, dict[str, Any]],
    dict[str, Any],
    datetime | None,
]:
    gate.require(
        candidate.get("schema") == CANDIDATE_SCHEMA,
        f"candidate.schema must be {CANDIDATE_SCHEMA}",
    )
    candidate_id = candidate.get("candidateId")
    gate.require(
        isinstance(candidate_id, str) and bool(candidate_id),
        "candidate.candidateId is missing",
    )
    gate.require(
        isinstance(candidate.get("campaignId"), str)
        and bool(candidate.get("campaignId")),
        "candidate.campaignId is missing",
    )
    runtime = candidate.get("runtimeCandidate")
    distribution = candidate.get("distributionCandidate")
    if not isinstance(runtime, dict):
        gate.error("candidate.runtimeCandidate must be an object")
        runtime = {}
    if not isinstance(distribution, dict):
        gate.error("candidate.distributionCandidate must be an object")
        distribution = {}

    source_commit = runtime.get("sourceCommit")
    source_tree = runtime.get("sourceTree")
    gate.require(isinstance(source_commit, str) and bool(GIT_OBJECT_RE.fullmatch(source_commit)), "runtimeCandidate.sourceCommit is invalid")
    gate.require(isinstance(source_tree, str) and bool(GIT_OBJECT_RE.fullmatch(source_tree)), "runtimeCandidate.sourceTree is invalid")
    gate.require(distribution.get("sourceCommit") == source_commit, "distributionCandidate.sourceCommit does not match runtimeCandidate")
    gate.require(distribution.get("sourceTree") == source_tree, "distributionCandidate.sourceTree does not match runtimeCandidate")

    runtime_digests = validate_runtime_subject(runtime, gate)
    artifact_ids, artifact_digests = validate_artifact_list(
        distribution.get("artifacts"),
        "distributionCandidate.artifacts",
        REQUIRED_PUBLIC_ARTIFACTS,
        gate,
    )
    image_ids, image_digests = validate_image_list(
        distribution.get("images"), gate
    )
    support_ids, support_digests = validate_artifact_list(
        distribution.get("supportArtifacts"),
        "distributionCandidate.supportArtifacts",
        REQUIRED_SUPPORT_ARTIFACTS,
        gate,
    )
    overlap = support_ids & (artifact_ids | image_ids)
    if overlap:
        gate.error(
            "supportArtifacts reuse public subject IDs: "
            + ", ".join(sorted(overlap))
        )

    candidate_identities = (
        runtime_digests | artifact_digests | image_digests | support_digests
    )
    if isinstance(candidate_id, str) and candidate_id:
        candidate_identities.add(candidate_id.lower())
    subject_contracts: dict[str, dict[str, Any]] = {}
    for item in distribution.get("artifacts", []):
        if isinstance(item, dict) and isinstance(item.get("id"), str):
            subject_contracts[item["id"]] = {
                "digest": normalize_sha256(item.get("sha256")),
                "sizeBytes": item.get("sizeBytes"),
                "target": item.get("target"),
            }
    for item in distribution.get("images", []):
        if isinstance(item, dict) and isinstance(item.get("id"), str):
            subject_contracts[item["id"]] = {
                "digest": normalize_sha256(item.get("digest")),
                "sizeBytes": item.get("sizeBytes"),
                "target": item.get("target"),
            }
    for item in distribution.get("supportArtifacts", []):
        if isinstance(item, dict) and isinstance(item.get("id"), str):
            subject_contracts[item["id"]] = {
                "digest": normalize_sha256(item.get("sha256")),
                "sizeBytes": item.get("sizeBytes"),
                "target": item.get("target"),
            }
    runtime_contract = {
        "id": runtime.get("id"),
        "target": runtime.get("target"),
        "fileName": runtime.get("fileName"),
        "digest": normalize_sha256(runtime.get("sha256")),
        "sizeBytes": runtime.get("sizeBytes"),
    }

    created_at = parse_time(candidate.get("createdAt"), "candidate.createdAt", gate)
    freeze_at = parse_time(
        candidate.get("sourceFrozenAt"), "candidate.sourceFrozenAt", gate
    )
    for owner_name, owner in (("candidate", candidate), ("runtimeCandidate", runtime)):
        for key in ("sourceFrozenAt", "frozenAt", "freezeAt"):
            if owner_name == "candidate" and key == "sourceFrozenAt":
                continue
            if key in owner:
                parsed = parse_time(owner.get(key), f"{owner_name}.{key}", gate)
                if parsed is not None and freeze_at is not None:
                    gate.require(
                        parsed == freeze_at,
                        f"{owner_name}.{key} conflicts with candidate.sourceFrozenAt",
                    )
    if created_at is not None and freeze_at is not None:
        gate.require(
            created_at >= freeze_at,
            "candidate.createdAt predates candidate.sourceFrozenAt",
        )
    return (
        source_commit if isinstance(source_commit, str) else None,
        source_tree if isinstance(source_tree, str) else None,
        candidate_identities,
        subject_contracts,
        runtime_contract,
        freeze_at,
    )


def check_candidate_against_repo(repo: Path, source_commit: str | None, source_tree: str | None, gate: Gate) -> None:
    if not source_commit or not source_tree:
        return
    ok, actual_tree = git(repo, "rev-parse", f"{source_commit}^{{tree}}")
    if not ok:
        gate.error(f"cannot resolve candidate source tree: {actual_tree}")
        return
    gate.require(actual_tree.lower() == source_tree.lower(), f"candidate sourceTree mismatch: recorded={source_tree}, actual={actual_tree}")


def resolution_identities(value: Any) -> set[str]:
    identities: set[str] = set()
    if isinstance(value, str) and value.strip():
        raw = value.strip()
        identities.add(raw.lower())
        digest = normalize_sha256(raw)
        if digest:
            identities.add(digest)
    elif isinstance(value, dict):
        for key, child in value.items():
            if normalize_key(key) in {"candidateid", "id", "digest", "sha256", "candidatesha256"}:
                identities.update(resolution_identities(child))
    elif isinstance(value, list):
        for child in value:
            identities.update(resolution_identities(child))
    return identities


def check_candidate_invalidations(
    state: dict[str, Any], candidate_identities: set[str], candidate_digest: str, freeze_at: datetime | None, gate: Gate
) -> None:
    invalidations = state.get("candidateInvalidations", [])
    if not isinstance(invalidations, list):
        gate.error("state.candidateInvalidations must be an array")
        return
    final_identities = {value.lower() for value in candidate_identities}
    final_identities.add(candidate_digest.lower())
    final_identities.add(f"sha256:{candidate_digest.lower()}")
    tasks = state.get("tasks")
    task_by_id = (
        {
            task["id"]: task
            for task in tasks
            if isinstance(task, dict) and isinstance(task.get("id"), str)
        }
        if isinstance(tasks, list)
        else {}
    )
    for index, invalidation in enumerate(invalidations):
        if not isinstance(invalidation, dict):
            gate.error(f"candidateInvalidations[{index}] must be an object")
            continue
        repair_task = invalidation.get("repairTask")
        repair_of = invalidation.get("repairOf")
        affected_cases = invalidation.get("affectedCases")
        gate.require(
            isinstance(repair_task, str) and bool(repair_task),
            f"candidateInvalidations[{index}].repairTask is missing",
        )
        gate.require(
            isinstance(repair_of, str) and bool(repair_of),
            f"candidateInvalidations[{index}].repairOf is missing",
        )
        repair = task_by_id.get(repair_task)
        parent = task_by_id.get(repair_of)
        gate.require(
            isinstance(repair, dict),
            f"candidateInvalidations[{index}] references an unknown repair task",
        )
        gate.require(
            isinstance(parent, dict),
            f"candidateInvalidations[{index}] references an unknown repair parent",
        )
        if isinstance(repair, dict):
            gate.require(
                repair.get("repairOf") == repair_of,
                f"candidateInvalidations[{index}] repair linkage does not match task state",
            )
            gate.require(
                repair.get("required", True)
                and repair.get("status") == "done"
                and repair.get("candidateReset") is True,
                f"candidateInvalidations[{index}] repair task is not a completed candidate-reset repair",
            )
        gate.require(
            invalidation.get("runtimeCandidate") is True,
            f"candidateInvalidations[{index}] must invalidate runtimeCandidate",
        )
        gate.require(
            invalidation.get("distributionCandidate") is True,
            f"candidateInvalidations[{index}] must invalidate distributionCandidate",
        )
        gate.require(
            isinstance(affected_cases, list)
            and bool(affected_cases)
            and all(isinstance(case, str) and bool(case) for case in affected_cases),
            f"candidateInvalidations[{index}].affectedCases must contain rerun case IDs",
        )
        if isinstance(affected_cases, list):
            gate.require(
                set(affected_cases) <= set(REQUIRED_EVIDENCE_CASES),
                f"candidateInvalidations[{index}].affectedCases contains unknown case IDs",
            )
        invalidated_at = parse_time(invalidation.get("invalidatedAt"), f"candidateInvalidations[{index}].invalidatedAt", gate)
        if invalidated_at is None:
            continue
        resolved = resolution_identities(invalidation.get("resolvedByCandidate"))
        if not resolved:
            gate.error(f"candidate invalidation {index} is unresolved")
            continue
        if not (resolved & final_identities):
            gate.error(
                f"candidate invalidation {index} is not resolved by the final candidate"
            )
        gate.require(
            freeze_at is not None and freeze_at >= invalidated_at,
            f"final candidate freeze predates invalidation {index}",
        )


def resolve_evidence_path(
    base: Path, raw: object, label: str, gate: Gate
) -> Path | None:
    if not isinstance(raw, str) or not raw.strip():
        gate.error(f"{label} must contain a relative evidence path")
        return None
    relative = Path(raw)
    if relative.is_absolute():
        gate.error(f"{label} evidence path must be relative to the evidence ledger")
        return None
    root = base.resolve()
    resolved = (root / relative).resolve()
    try:
        resolved.relative_to(root)
    except ValueError:
        gate.error(f"{label} evidence path escapes the evidence ledger directory")
        return None
    return resolved


def check_evidence_subjects(
    value: object,
    expected: dict[str, dict[str, Any]],
    source_commit: str | None,
    source_tree: str | None,
    label: str,
    gate: Gate,
) -> None:
    if not isinstance(value, list):
        gate.error(f"{label}.subjects must be an array")
        return
    seen: set[str] = set()
    for index, subject in enumerate(value):
        pointer = f"{label}.subjects[{index}]"
        if not isinstance(subject, dict):
            gate.error(f"{pointer} must be an object")
            continue
        subject_id = subject.get("id")
        if not isinstance(subject_id, str) or not subject_id:
            gate.error(f"{pointer}.id is missing")
            continue
        if subject_id in seen:
            gate.error(f"duplicate {label} subject: {subject_id}")
        seen.add(subject_id)
        contract = expected.get(subject_id)
        if contract is None:
            gate.error(f"unexpected {label} subject: {subject_id}")
            continue
        gate.require(
            normalize_sha256(subject.get("digest")) == contract.get("digest"),
            f"{pointer}.digest does not match candidate",
        )
        gate.require(
            subject.get("sizeBytes") == contract.get("sizeBytes"),
            f"{pointer}.sizeBytes does not match candidate",
        )
        gate.require(
            subject.get("target") == contract.get("target"),
            f"{pointer}.target does not match candidate",
        )
        gate.require(
            subject.get("sourceCommit") == source_commit
            and subject.get("sourceTree") == source_tree,
            f"{pointer} source identity does not match candidate",
        )
        mode = subject.get("verificationMode")
        if mode == "local-byte":
            gate.require(
                subject.get("verifiedBeforeCleanup") is True,
                f"{pointer} must record pre-cleanup byte verification",
            )
        elif mode == "ci-metadata":
            gate.require(
                subject.get("locallyExecuted") is False,
                f"{pointer} CI metadata must not claim local execution",
            )
            ci = subject.get("ci")
            if not isinstance(ci, dict):
                gate.error(f"{pointer}.ci must be an object")
            else:
                for field in ("runUrl", "jobUrl"):
                    gate.require(
                        isinstance(ci.get(field), str)
                        and ci[field].startswith("https://"),
                        f"{pointer}.ci.{field} must be an HTTPS URL",
                    )
                run_url = ci.get("runUrl")
                job_url = ci.get("jobUrl")
                run_match = (
                    GITHUB_RUN_URL_RE.fullmatch(run_url)
                    if isinstance(run_url, str)
                    else None
                )
                job_match = (
                    GITHUB_JOB_URL_RE.fullmatch(job_url)
                    if isinstance(job_url, str)
                    else None
                )
                gate.require(
                    run_match is not None
                    and job_match is not None
                    and run_match.group("run") == job_match.group("run"),
                    f"{pointer}.ci URLs must identify one toodi0418/CMClient GitHub Actions run",
                )
                gate.require(
                    normalize_sha256(ci.get("apiDigestSha256"))
                    == contract.get("digest"),
                    f"{pointer}.ci.apiDigestSha256 does not match candidate",
                )
                gate.require(
                    ci.get("apiSizeBytes") == contract.get("sizeBytes"),
                    f"{pointer}.ci.apiSizeBytes does not match candidate",
                )
                expires_at = parse_time(
                    ci.get("expiresAt"), f"{pointer}.ci.expiresAt", gate
                )
                gate.require(
                    expires_at is not None and expires_at > datetime.now(timezone.utc),
                    f"{pointer} CI metadata is expired",
                )
        else:
            gate.error(f"{pointer}.verificationMode must be local-byte or ci-metadata")
    gate.require(
        seen == set(expected),
        f"{label}.subjects do not exactly match candidate subjects",
    )


def passing_subcase_ids(value: object, label: str, gate: Gate) -> set[str]:
    if not isinstance(value, list):
        gate.error(f"{label}.subcases must be an array")
        return set()
    ids: set[str] = set()
    for index, subcase in enumerate(value):
        pointer = f"{label}.subcases[{index}]"
        if not isinstance(subcase, dict):
            gate.error(f"{pointer} must be an object")
            continue
        subcase_id = subcase.get("id")
        if not isinstance(subcase_id, str) or not subcase_id:
            gate.error(f"{pointer}.id is missing")
            continue
        if subcase_id in ids:
            gate.error(f"duplicate {label} subcase: {subcase_id}")
        ids.add(subcase_id)
        gate.require(
            subcase.get("status") == "pass",
            f"{label} subcase must pass: {subcase_id}",
        )
    return ids


def check_invalidation_reruns(
    evidence: dict[str, Any],
    evidence_path: Path,
    invalidations: object,
    candidate_id: object,
    candidate_digest: str,
    freeze_at: datetime | None,
    by_case: dict[str, dict[str, Any]],
    gate: Gate,
) -> None:
    gate.require(
        evidence.get("invalidationRerunsSchema") == INVALIDATION_RERUN_SCHEMA,
        f"evidence.invalidationRerunsSchema must be {INVALIDATION_RERUN_SCHEMA}",
    )
    if not isinstance(invalidations, list):
        gate.error("candidate invalidations must be an array for rerun proof")
        return
    reruns = evidence.get("invalidationReruns")
    if not isinstance(reruns, list):
        gate.error("evidence.invalidationReruns must be an array")
        return
    invalidation_by_repair = {
        item.get("repairTask"): item
        for item in invalidations
        if isinstance(item, dict) and isinstance(item.get("repairTask"), str)
    }
    rerun_by_repair: dict[str, dict[str, Any]] = {}
    for index, rerun in enumerate(reruns):
        if not isinstance(rerun, dict) or not isinstance(rerun.get("repairTask"), str):
            gate.error(f"invalidationReruns[{index}] is invalid")
            continue
        repair_task = rerun["repairTask"]
        if repair_task in rerun_by_repair:
            gate.error(f"duplicate invalidation rerun: {repair_task}")
        rerun_by_repair[repair_task] = rerun
    gate.require(
        set(rerun_by_repair) == set(invalidation_by_repair),
        "invalidationReruns must contain exactly one entry per candidate invalidation",
    )

    for repair_task, invalidation in invalidation_by_repair.items():
        rerun = rerun_by_repair.get(repair_task)
        if rerun is None:
            continue
        label = f"invalidation rerun {repair_task}"
        gate.require(rerun.get("status") == "pass", f"{label} must pass")
        gate.require(rerun.get("sanitized") is True, f"{label} must be sanitized")
        gate.require(
            rerun.get("candidateId") == candidate_id,
            f"{label} candidateId does not match final candidate",
        )
        gate.require(
            normalize_sha256(rerun.get("candidateSha256")) == candidate_digest,
            f"{label} candidateSha256 does not match final candidate",
        )
        affected_cases = invalidation.get("affectedCases")
        gate.require(
            rerun.get("affectedCases") == affected_cases,
            f"{label} affectedCases do not match invalidation",
        )
        invalidated_at = parse_time(
            invalidation.get("invalidatedAt"), f"{label}.invalidatedAt", gate
        )
        executed_at = parse_time(rerun.get("executedAt"), f"{label}.executedAt", gate)
        if executed_at is not None:
            gate.require(
                invalidated_at is not None and executed_at >= invalidated_at,
                f"{label} executed before invalidation",
            )
            gate.require(
                freeze_at is not None and executed_at >= freeze_at,
                f"{label} executed before final candidate freeze",
            )
        refs = rerun.get("evidenceRefs")
        if not isinstance(refs, list):
            gate.error(f"{label}.evidenceRefs must be an array")
            continue
        refs_by_case: dict[str, dict[str, Any]] = {}
        for ref_index, ref in enumerate(refs):
            if not isinstance(ref, dict) or not isinstance(ref.get("caseId"), str):
                gate.error(f"{label}.evidenceRefs[{ref_index}] is invalid")
                continue
            case_id = ref["caseId"]
            if case_id in refs_by_case:
                gate.error(f"{label} has duplicate evidence ref: {case_id}")
            refs_by_case[case_id] = ref
        gate.require(
            set(refs_by_case) == set(affected_cases or []),
            f"{label}.evidenceRefs do not exactly cover affectedCases",
        )
        for case_id, ref in refs_by_case.items():
            ref_path = resolve_evidence_path(
                evidence_path.parent, ref.get("path"), f"{label} case {case_id}", gate
            )
            ref_digest = normalize_sha256(ref.get("sha256"))
            if ref_digest is None:
                gate.error(f"{label} case {case_id} has invalid sha256")
                continue
            top_level = by_case.get(case_id)
            if top_level is None:
                gate.error(f"{label} references unknown retained evidence case: {case_id}")
            else:
                gate.require(
                    ref.get("path") == top_level.get("path")
                    and ref_digest == normalize_sha256(top_level.get("sha256")),
                    f"{label} case {case_id} does not reference the retained case evidence",
                )
            if ref_path is None:
                continue
            if not ref_path.is_file():
                gate.error(f"{label} evidence file does not exist: {ref_path}")
                continue
            try:
                actual = sha256_file(ref_path)
            except OSError as error:
                gate.error(f"cannot hash {label} evidence file {ref_path}: {error}")
                continue
            gate.require(
                actual == ref_digest,
                f"{label} evidence digest mismatch: {case_id}",
            )
            scan_secret_text(ref_path, gate)


def check_evidence(
    evidence: dict[str, Any], evidence_path: Path, candidate_digest: str,
    source_commit: str | None, source_tree: str | None,
    candidate_id: object, campaign_id: object,
    subject_contracts: dict[str, dict[str, Any]],
    runtime_contract: dict[str, Any], invalidations: object,
    freeze_at: datetime | None, gate: Gate
) -> None:
    gate.require(
        evidence.get("schema") == EVIDENCE_SCHEMA,
        f"evidence.schema must be {EVIDENCE_SCHEMA}",
    )
    gate.require(evidence.get("sanitized") is True, "evidence.sanitized must be true")
    gate.require(
        evidence.get("candidateId") == candidate_id,
        "evidence candidateId does not match candidate",
    )
    gate.require(
        evidence.get("campaignId") == campaign_id,
        "evidence campaignId does not match candidate/campaign",
    )
    bound_digest = normalize_sha256(evidence.get("candidateSha256"))
    gate.require(bound_digest == candidate_digest, "evidence candidateSha256 does not bind the exact candidate JSON")
    gate.require(evidence.get("sourceCommit") == source_commit, "evidence sourceCommit does not match runtimeCandidate")
    gate.require(evidence.get("sourceTree") == source_tree, "evidence sourceTree does not match runtimeCandidate")

    runtime_subject = evidence.get("runtimeSubject")
    if not isinstance(runtime_subject, dict):
        gate.error("evidence.runtimeSubject must be an object")
    else:
        for field in ("id", "target", "fileName", "sizeBytes"):
            gate.require(
                runtime_subject.get(field) == runtime_contract.get(field),
                f"evidence.runtimeSubject.{field} does not match runtimeCandidate",
            )
        gate.require(
            normalize_sha256(runtime_subject.get("sha256"))
            == runtime_contract.get("digest"),
            "evidence.runtimeSubject.sha256 does not match runtimeCandidate",
        )
        gate.require(
            runtime_subject.get("sourceCommit") == source_commit
            and runtime_subject.get("sourceTree") == source_tree,
            "evidence.runtimeSubject source identity does not match candidate",
        )
        gate.require(
            runtime_subject.get("verificationMode") == "local-byte"
            and runtime_subject.get("verifiedBeforeCleanup") is True
            and runtime_subject.get("locallyExecuted") is True,
            "evidence.runtimeSubject must prove pre-cleanup local execution",
        )

    records = evidence.get("records")
    if not isinstance(records, list):
        gate.error("evidence.records must be an array")
        return
    by_case: dict[str, dict[str, Any]] = {}
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            gate.error(f"evidence.records[{index}] must be an object")
            continue
        case_id = record.get("caseId")
        if not isinstance(case_id, str) or not case_id:
            gate.error(f"evidence.records[{index}].caseId is missing")
            continue
        if case_id in by_case:
            gate.error(f"duplicate evidence case: {case_id}")
            continue
        by_case[case_id] = record

    for case_id in REQUIRED_EVIDENCE_CASES:
        if case_id not in by_case:
            gate.error(f"evidence missing required case: {case_id}")
    for case_id in sorted(set(by_case) - set(REQUIRED_EVIDENCE_CASES)):
        gate.error(f"unexpected evidence case outside schema: {case_id}")

    for case_id in REQUIRED_EVIDENCE_CASES:
        record = by_case.get(case_id)
        if record is None:
            continue
        label = f"evidence case {case_id}"
        gate.require(record.get("status") == "pass", f"{label} must pass")
        gate.require(record.get("sanitized") is True, f"{label} must be sanitized")
        gate.require(
            record.get("candidateId") == candidate_id,
            f"{label} candidateId does not match candidate",
        )
        gate.require(
            record.get("campaignId") == campaign_id,
            f"{label} campaignId does not match candidate/campaign",
        )
        gate.require(
            normalize_sha256(record.get("candidateSha256")) == candidate_digest,
            f"{label} does not bind the exact candidate JSON",
        )
        gate.require(
            record.get("sourceCommit") == source_commit,
            f"{label} sourceCommit does not match runtimeCandidate",
        )
        gate.require(
            record.get("sourceTree") == source_tree,
            f"{label} sourceTree does not match runtimeCandidate",
        )
        parse_time(record.get("executedAt"), f"{label}.executedAt", gate)
        path = resolve_evidence_path(
            evidence_path.parent, record.get("path"), label, gate
        )
        expected_digest = normalize_sha256(record.get("sha256"))
        if expected_digest is None:
            gate.error(f"{label} lacks an exact sha256")
            continue
        if path is None:
            continue
        if not path.is_file():
            gate.error(f"{label} evidence file does not exist: {path}")
            continue
        try:
            actual_digest = sha256_file(path)
        except OSError as error:
            gate.error(f"cannot hash {label} evidence file {path}: {error}")
            continue
        if actual_digest != expected_digest:
            gate.error(f"{label} evidence digest mismatch: {path}")
            continue
        scan_secret_text(path, gate)

    testability = by_case.get("TESTABILITY_GATES")
    if testability is not None:
        gates = passing_subcase_ids(
            testability.get("subcases"), "TESTABILITY_GATES", gate
        )
        gate.require(
            gates == set(REQUIRED_QUALIFICATION_GATES),
            "TESTABILITY_GATES must cover exactly TG-01 through TG-14",
        )

    for case_id, required_subcases in REQUIRED_SUBCASES.items():
        record = by_case.get(case_id)
        if record is None:
            continue
        subcases = passing_subcase_ids(record.get("subcases"), case_id, gate)
        gate.require(
            subcases == required_subcases,
            f"{case_id} does not contain the exact required subcase set",
        )

    package_matrix = by_case.get("PACKAGE_MATRIX")
    if package_matrix is not None:
        public_contracts = {
            subject_id: contract
            for subject_id, contract in subject_contracts.items()
            if subject_id in REQUIRED_PUBLIC_ARTIFACTS
            or subject_id in REQUIRED_OCI_IMAGES
        }
        check_evidence_subjects(
            package_matrix.get("subjects"),
            public_contracts,
            source_commit,
            source_tree,
            "PACKAGE_MATRIX",
            gate,
        )
        gate.require(
            package_matrix.get("verifiedLevels") == ["V0", "V1", "V2"],
            "PACKAGE_MATRIX must prove V0, V1, and V2",
        )
        v3_deferrals = package_matrix.get("v3Deferrals")
        gate.require(
            isinstance(v3_deferrals, list)
            and len(v3_deferrals) == len(set(v3_deferrals))
            and set(v3_deferrals) == PACKAGE_V3_DEFERRALS,
            "PACKAGE_MATRIX must explicitly defer every foreign/native V3 row",
        )

    supply_chain = by_case.get("SUPPLY_CHAIN")
    if supply_chain is not None:
        support_contracts = {
            subject_id: contract
            for subject_id, contract in subject_contracts.items()
            if subject_id in REQUIRED_SUPPORT_ARTIFACTS
        }
        check_evidence_subjects(
            supply_chain.get("subjects"),
            support_contracts,
            source_commit,
            source_tree,
            "SUPPLY_CHAIN",
            gate,
        )

    docker_matrix = by_case.get("DOCKER_MATRIX")
    if docker_matrix is not None:
        docker_subjects = {
            "docker-compose",
            "cmclient-oci-index",
            "cmclient-oci-amd64",
            "cmclient-oci-arm64",
        }
        subjects = docker_matrix.get("subjects")
        gate.require(
            isinstance(subjects, list)
            and len(subjects) == len(set(subjects))
            and set(subjects) == docker_subjects,
            "DOCKER_MATRIX subjects do not match the candidate Docker objects",
        )
        v3_deferrals = docker_matrix.get("v3Deferrals")
        gate.require(
            isinstance(v3_deferrals, list)
            and len(v3_deferrals) == len(set(v3_deferrals))
            and set(v3_deferrals) == DOCKER_V3_DEFERRALS,
            "DOCKER_MATRIX must explicitly defer both real-host V3 rows",
        )

    live_data = by_case.get("LIVE_DATA")
    if live_data is not None:
        gate.require(live_data.get("approved") is True, "LIVE_DATA must be approved")
        gate.require(
            live_data.get("rfTransmitted") is False,
            "LIVE_DATA must record that no RF was transmitted",
        )
        gate.require(
            live_data.get("radioMutated") is False,
            "LIVE_DATA must record that no radio was mutated",
        )

    live_soak = by_case.get("LIVE_SOAK_24H")
    if live_soak is not None:
        gate.require(
            live_soak.get("continuous") is True,
            "LIVE_SOAK_24H must be continuous",
        )
        duration = live_soak.get("durationSeconds")
        gate.require(
            isinstance(duration, int)
            and not isinstance(duration, bool)
            and duration >= 86400,
            "LIVE_SOAK_24H durationSeconds must be at least 86400",
        )
        started = parse_time(
            live_soak.get("startedAt"), "LIVE_SOAK_24H.startedAt", gate
        )
        ended = parse_time(
            live_soak.get("endedAt"), "LIVE_SOAK_24H.endedAt", gate
        )
        if started is not None and ended is not None:
            gate.require(
                (ended - started).total_seconds() >= 86400,
                "LIVE_SOAK_24H timestamps span less than 24 hours",
            )

    deferrals = by_case.get("DEFERRALS")
    if deferrals is not None:
        values = deferrals.get("deferrals")
        if not isinstance(values, list):
            gate.error("DEFERRALS.deferrals must be an array")
        else:
            ids: set[str] = set()
            for index, value in enumerate(values):
                if not isinstance(value, dict):
                    gate.error(f"DEFERRALS.deferrals[{index}] must be an object")
                    continue
                deferral_id = value.get("id")
                if not isinstance(deferral_id, str) or not deferral_id:
                    gate.error(f"DEFERRALS.deferrals[{index}].id is missing")
                    continue
                if deferral_id in ids:
                    gate.error(f"duplicate deferral id: {deferral_id}")
                ids.add(deferral_id)
                gate.require(
                    value.get("status") == "pending"
                    and value.get("manualGate") is True,
                    f"deferral must remain pending/manual: {deferral_id}",
                )
                gate.require(
                    isinstance(value.get("reason"), str)
                    and bool(value.get("reason")),
                    f"deferral reason is missing: {deferral_id}",
                )
            gate.require(
                ids == ALL_REQUIRED_DEFERRALS,
                "DEFERRALS does not contain the exact platform/production deferral set",
            )

    check_invalidation_reruns(
        evidence,
        evidence_path,
        invalidations,
        candidate_id,
        candidate_digest,
        freeze_at,
        by_case,
        gate,
    )


def check_campaign(campaign: dict[str, Any], gate: Gate) -> str | None:
    campaign_id = campaign.get("campaignId")
    gate.require(
        isinstance(campaign_id, str) and bool(campaign_id),
        "campaign.campaignId is missing",
    )
    gate.require(campaign.get("branch") == "dev", "campaign.branch must be dev")
    gate.require(campaign.get("status") == "closed", "campaign.status must be closed")
    gate.require(campaign.get("cleanupRequired") is False, "campaign.cleanupRequired must be false")
    gate.require(campaign.get("secretsRecorded") is False, "campaign.secretsRecorded must be false")
    environment_policy = campaign.get("environmentPolicy")
    gate.require(
        isinstance(environment_policy, dict)
        and environment_policy.get("parentGitProfilePreserved") is True
        and environment_policy.get("allGeneratedOutputBelowCampaign") is True,
        "campaign environment policy is incomplete",
    )
    external_gates = campaign.get("externalGates")
    gate.require(
        isinstance(external_gates, dict)
        and external_gates.get("mainModificationApproved") is False
        and external_gates.get("productionActionsApproved") is False,
        "campaign production/main approvals must remain false",
    )
    paths = campaign.get("paths")
    if not isinstance(paths, dict):
        gate.error("campaign.paths must be an object")
        return campaign_id if isinstance(campaign_id, str) else None
    for name in CAMPAIGN_CLEANUP_PATHS:
        raw_path = paths.get(name)
        if not isinstance(raw_path, str) or not raw_path.strip():
            gate.error(f"campaign.paths.{name} is missing")
            continue
        if not Path(raw_path).exists():
            continue
        if name == "physicalRoot":
            gate.error(f"raw campaign root still exists: {raw_path}")
        elif name == "logicalRoot":
            gate.error(f"logical campaign root is still mounted: {raw_path}")
        else:
            gate.error(f"campaign cleanup path still exists: {name}: {raw_path}")
    return campaign_id if isinstance(campaign_id, str) else None


def main() -> int:
    args = parse_args()
    gate = Gate()
    state = load_json(args.state, "task state", gate)
    candidate = load_json(args.candidate, "candidate", gate)
    evidence = load_json(args.evidence, "evidence", gate)
    campaign = load_json(args.campaign, "campaign", gate)
    graph_lock = load_json(args.graph_lock, "graph lock", gate)
    license_provenance = load_json(
        args.license_provenance, "license provenance", gate
    )
    precheck_attestation = (
        None
        if args.exclude_task
        else load_json(args.precheck_attestation, "pre-check attestation", gate)
    )
    if args.exclude_task:
        gate.require(
            args.write_precheck_attestation,
            "excluded completion run requires --write-precheck-attestation",
        )
    else:
        gate.require(
            not args.write_precheck_attestation,
            "--write-precheck-attestation requires the completion-task exclusion",
        )

    branch: str | None = None
    source_commit: str | None = None
    source_tree: str | None = None
    candidate_identities: set[str] = set()
    subject_contracts: dict[str, dict[str, Any]] = {}
    runtime_contract: dict[str, Any] = {}
    freeze_at: datetime | None = None
    candidate_digest: str | None = None
    precheck_repo_head: str | None = None
    repository_identity_document = (
        graph_lock.get("repositoryIdentity")
        if isinstance(graph_lock, dict)
        and isinstance(graph_lock.get("repositoryIdentity"), dict)
        else {}
    )
    repository_identity = (
        normalize_remote_identity(repository_identity_document.get("origin", ""))
        if isinstance(repository_identity_document.get("origin"), str)
        else ""
    )
    campaign_id = check_campaign(campaign, gate) if campaign is not None else None
    if state is not None:
        scan_secret_values(state, "task state", gate)
    if campaign is not None:
        scan_secret_values(campaign, "campaign", gate)
    if precheck_attestation is not None:
        scan_secret_values(precheck_attestation, "pre-check attestation", gate)
    if license_provenance is not None:
        scan_secret_values(license_provenance, "license provenance", gate)

    if candidate is not None:
        scan_secret_values(candidate, "candidate", gate)
        (
            source_commit,
            source_tree,
            candidate_identities,
            subject_contracts,
            runtime_contract,
            freeze_at,
        ) = candidate_source(candidate, gate)
        gate.require(
            candidate.get("campaignId") == campaign_id,
            "candidate campaignId does not match campaign",
        )
        try:
            candidate_digest = sha256_file(args.candidate)
        except OSError as error:
            gate.error(f"cannot hash candidate JSON {args.candidate}: {error}")

    if (
        state is not None
        and graph_lock is not None
        and license_provenance is not None
    ):
        branch, _ = check_task_state(
            state,
            args.exclude_task,
            args.repo,
            graph_lock,
            license_provenance,
            gate,
        )
    check_repo(args.repo, branch, source_commit, gate, repository_identity)
    check_candidate_against_repo(args.repo, source_commit, source_tree, gate)

    if state is not None and candidate_digest is not None:
        check_candidate_invalidations(state, candidate_identities, candidate_digest, freeze_at, gate)

    if evidence is not None:
        scan_secret_values(evidence, "evidence", gate)
        if candidate_digest is not None:
            check_evidence(
                evidence,
                args.evidence,
                candidate_digest,
                source_commit,
                source_tree,
                candidate.get("candidateId") if candidate is not None else None,
                campaign_id,
                subject_contracts,
                runtime_contract,
                state.get("candidateInvalidations", []) if state is not None else [],
                freeze_at,
                gate,
            )

    file_bindings = precheck_file_bindings(
        args.candidate,
        args.evidence,
        args.graph_lock,
        args.license_provenance,
        gate,
    )
    if args.exclude_task:
        ok_head, head = git(args.repo, "rev-parse", "HEAD")
        if not ok_head:
            gate.error(f"cannot resolve pre-check Repository HEAD: {head}")
        else:
            precheck_repo_head = head.lower()
        active = state.get("activeGraph") if state is not None else None
        completion_id = (
            active.get("completionTask") if isinstance(active, dict) else None
        )
        tasks = state.get("tasks") if state is not None else None
        completion = next(
            (
                task
                for task in tasks
                if isinstance(task, dict) and task.get("id") == completion_id
            ),
            None,
        ) if isinstance(tasks, list) else None
        if precheck_repo_head is not None:
            gate.require(
                isinstance(completion, dict)
                and completion.get("checkpointBaseCommit") == precheck_repo_head,
                "pre-check Repository HEAD must equal the completion checkpoint base",
            )
    elif (
        precheck_attestation is not None
        and state is not None
        and candidate is not None
    ):
        check_precheck_attestation(
            precheck_attestation,
            state=state,
            candidate=candidate,
            campaign_id=campaign_id,
            source_commit=source_commit,
            source_tree=source_tree,
            repository_identity=repository_identity,
            file_bindings=file_bindings,
            repo=args.repo,
            gate=gate,
        )

    if (
        not gate.errors
        and args.exclude_task
        and args.write_precheck_attestation
        and state is not None
        and candidate is not None
        and file_bindings is not None
        and precheck_repo_head is not None
    ):
        attestation = build_precheck_attestation(
            state=state,
            candidate=candidate,
            campaign_id=campaign_id,
            source_commit=source_commit,
            source_tree=source_tree,
            repo_head=precheck_repo_head,
            repository_identity=repository_identity,
            file_bindings=file_bindings,
        )
        try:
            atomic_write_private_json(args.precheck_attestation, attestation)
        except OSError as error:
            gate.error(
                f"cannot write pre-check attestation {args.precheck_attestation}: {error}"
            )

    payload = {
        "status": "pass" if not gate.errors else "fail",
        "checks": {
            "tasks": str(args.state),
            "repo": str(args.repo),
            "campaign": str(args.campaign),
            "candidate": str(args.candidate),
            "evidence": str(args.evidence),
            "graphLock": str(args.graph_lock),
            "licenseProvenance": str(args.license_provenance),
            "precheckAttestation": str(args.precheck_attestation),
            "repositoryIdentity": repository_identity,
            "excludedTasks": args.exclude_task,
        },
        "errors": gate.errors,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if not gate.errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
