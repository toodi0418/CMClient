#!/usr/bin/env python3
"""Crash-recoverable Windows scoped-completion attempt state machine."""

from __future__ import annotations

import argparse
import contextlib
import copy
import importlib.util
import json
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path, PurePosixPath
from types import ModuleType
from typing import Any, Callable


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = Path(
    os.environ.get("CMCLIENT_WORKSPACE_ROOT", REPOSITORY_ROOT.parent.parent)
).resolve()
TASK_STATE_LIBRARY_PATH = Path(__file__).with_name("task-state-lib.py")
DEFAULT_STATE_PATH = WORKSPACE_ROOT / "state/TASKS.json"
DEFAULT_LEDGER_PATH = (
    WORKSPACE_ROOT / "state/SCOPED_WINDOWS_COMPLETION_ATTEMPTS.json"
)
DEFAULT_GRAPH_LOCK_PATH = (
    REPOSITORY_ROOT / "scripts/unified-task-graph-lock.json"
)
DEFAULT_LICENSE_PATH = WORKSPACE_ROOT / "state/LICENSE_PROVENANCE.json"
DEFAULT_HISTORY_PATH = WORKSPACE_ROOT / "state/GRAPH_HISTORY.json"
DEFAULT_REPOSITORY_PATH = REPOSITORY_ROOT
DEFAULT_JOURNAL_PATH = WORKSPACE_ROOT / "state/GRAPH_UPGRADE_V3.json"
QUALIFICATION_MANIFEST_RELATIVE = (
    "docs/qualification/windows-server-2025-x86_64-development-candidate.json"
)
PRECHECK_RELATIVE_PREFIX = "state/completion/windows-live-first"
POSTCHECK_RELATIVE_PREFIX = "state/completion/windows-live-first"

LEDGER_SCHEMA = "cmclient-windows-scoped-completion-attempts/v1"
SCOPE = "windows-live-first"
COORDINATOR_TASK = "P18-T10"
PREREQUISITE_TASK = "P18-T09"
PROTOCOL_MARKER = LEDGER_SCHEMA
REPAIR_SCOPE = "windows-completion-repair"
ATTEMPT_SCOPE = "windows-completion-attempt"

EVENT_TYPES = {
    "attempt_prepared",
    "attempt_pushed",
    "reconciliation_blocked",
    "reconciliation_resumed",
    "attempt_failed",
    "repair_allocated",
    "repair_pushed",
    "attempt_passed",
    "coordinator_completed",
}
FAILURE_CLASS_RECONCILIATION = "reconciliation_blocker"
FAILURE_CLASS_TOOL = "completion_tool_defect"
FAILURE_CLASS_CANDIDATE = "candidate_affecting_defect"
FAILURE_CLASS_ALIASES = {
    "reconciliation-blocker": FAILURE_CLASS_RECONCILIATION,
    "reconciliationBlocker": FAILURE_CLASS_RECONCILIATION,
    FAILURE_CLASS_RECONCILIATION: FAILURE_CLASS_RECONCILIATION,
    "completion-tool-defect": FAILURE_CLASS_TOOL,
    "completionToolDefect": FAILURE_CLASS_TOOL,
    FAILURE_CLASS_TOOL: FAILURE_CLASS_TOOL,
    "candidate-affecting-defect": FAILURE_CLASS_CANDIDATE,
    "candidateAffectingDefect": FAILURE_CLASS_CANDIDATE,
    FAILURE_CLASS_CANDIDATE: FAILURE_CLASS_CANDIDATE,
}

SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")
TASK_ID_RE = re.compile(r"^P18-T(?P<number>[0-9]{2})$")
GLOB_RE = re.compile(r"[*?\[\]]")
EVENT_KEYS = {
    "sequence",
    "type",
    "recordedAt",
    "previousEventHash",
    "payload",
    "eventHash",
}
EVENT_PAYLOAD_KEYS = {
    "attempt_prepared": {
        "attemptTask", "attemptNumber", "priorPushedHead", "candidateIdentity",
        "precheckSha256", "intendedManifestSha256",
    },
    "attempt_pushed": {
        "attemptTask", "attemptNumber", "commit", "originCommit", "manifestSha256",
        "preparedEventHash", "candidateIdentity",
    },
    "reconciliation_blocked": {"attemptTask", "attemptNumber", "commit", "reason"},
    "reconciliation_resumed": {"attemptTask", "attemptNumber", "commit"},
    "attempt_failed": {
        "attemptTask", "attemptNumber", "commit", "failureClass", "requestedClass",
        "candidateReset", "changedPaths", "allowlistSha256", "classificationReason",
    },
    "repair_allocated": {
        "repairTask", "failedAttemptTask", "failedAttemptNumber", "candidateReset",
        "failureClass", "taskDefinition",
    },
    "repair_pushed": {
        "repairTask", "commit", "originCommit", "nextAttemptTask", "nextAttemptNumber",
        "attemptDefinition",
    },
    "attempt_passed": {"attemptTask", "attemptNumber", "commit"},
    "coordinator_completed": {"coordinatorTask", "headEventHash"},
}
SENSITIVE_KEY_RE = re.compile(
    r"(?:secret|password|passwd|token|api.?key|credential|private.?key|raw.?packet|callsign|coordinate|latitude|longitude|node.?id|identity)",
    re.IGNORECASE,
)
SAFE_IDENTITY_KEYS = {"candidateIdentity", "candidateId", "repositoryIdentity"}
SAFE_REASON_RE = re.compile(r"^[A-Za-z0-9 .,;:_()\-]{1,180}$")
TASK_MUTABLE_FIELDS = {
    "status",
    "startedAt",
    "completedAt",
    "commit",
    "notes",
    "candidateInvalidation",
}

_TASK_STATE_LIBRARY: ModuleType | None = None


class ScopedCompletionError(ValueError):
    """Raised when the scoped completion protocol fails closed."""


def task_state_library() -> ModuleType:
    """Load the repository task-state primitives without importing a hyphen name."""

    global _TASK_STATE_LIBRARY
    if _TASK_STATE_LIBRARY is not None:
        return _TASK_STATE_LIBRARY
    spec = importlib.util.spec_from_file_location(
        "cmclient_scoped_completion_task_state", TASK_STATE_LIBRARY_PATH
    )
    if spec is None or spec.loader is None:
        raise ScopedCompletionError(
            f"cannot load task-state library: {TASK_STATE_LIBRARY_PATH}"
        )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    _TASK_STATE_LIBRARY = module
    return module


def _contract_paths() -> tuple[Path, Path, Path, Path]:
    return (
        Path(os.environ.get("CMCLIENT_GRAPH_LOCK_PATH", DEFAULT_GRAPH_LOCK_PATH)),
        Path(os.environ.get("CMCLIENT_LICENSE_PATH", DEFAULT_LICENSE_PATH)),
        Path(os.environ.get("CMCLIENT_HISTORY_PATH", DEFAULT_HISTORY_PATH)),
        Path(os.environ.get("CMCLIENT_REPOSITORY_ROOT", DEFAULT_REPOSITORY_PATH)),
    )


def _operation_id(explicit: str | None = None) -> str | None:
    return explicit or os.environ.get("CMCLIENT_GRAPH_UPGRADE_OPERATION_ID")


def _scoped_path(relative: str) -> Path:
    """Resolve a protocol artifact and reject paths outside the campaign workspace."""

    path = (WORKSPACE_ROOT / PurePosixPath(relative)).resolve()
    root = WORKSPACE_ROOT.resolve()
    if path != root and root not in path.parents:
        raise ScopedCompletionError(f"scoped artifact escapes the workspace: {relative}")
    return path


def _manifest_path() -> Path:
    _graph, _license, _history, repository = _contract_paths()
    path = (repository / PurePosixPath(QUALIFICATION_MANIFEST_RELATIVE)).resolve()
    if repository.resolve() not in path.parents:
        raise ScopedCompletionError("qualification manifest escapes the Repository")
    return path


def _attempt_evidence_path(task_id: str, suffix: str) -> Path:
    if TASK_ID_RE.fullmatch(task_id) is None:
        raise ScopedCompletionError(f"invalid completion attempt task: {task_id}")
    return _scoped_path(f"{PRECHECK_RELATIVE_PREFIX}/{task_id}-{suffix}.json")


def _safe_load_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ScopedCompletionError(f"cannot read {label} {path}: {error}") from error
    if not isinstance(value, dict):
        raise ScopedCompletionError(f"{label} must be a JSON object")
    _reject_sensitive_payload(value, label)
    return value


def _file_sha256(path: Path, label: str) -> str:
    try:
        return task_state_library().sha256_file(path)
    except (OSError, ValueError) as error:
        raise ScopedCompletionError(f"cannot hash {label} {path}: {error}") from error


def _run_git(
    repository: Path, *arguments: str, check: bool = True
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        ["git", "-C", str(repository), *arguments],
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
    if check and result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise ScopedCompletionError(
            f"git {' '.join(arguments)} failed for {repository}: {detail}"
        )
    return result


def _git_output(repository: Path, *arguments: str) -> str:
    return _run_git(repository, *arguments).stdout.strip()


def _git_is_ancestor(repository: Path, ancestor: str, descendant: str) -> bool:
    result = _run_git(
        repository,
        "merge-base",
        "--is-ancestor",
        ancestor,
        descendant,
        check=False,
    )
    if result.returncode not in {0, 1}:
        detail = result.stderr.strip() or result.stdout.strip()
        raise ScopedCompletionError(f"git ancestry check failed: {detail}")
    return result.returncode == 0


def _git_snapshot(*, require_clean: bool = True) -> dict[str, str]:
    _graph, _license, _history, repository = _contract_paths()
    repository = repository.resolve()
    if _git_output(repository, "rev-parse", "--is-inside-work-tree") != "true":
        raise ScopedCompletionError(f"not a Git worktree: {repository}")
    top_level = Path(_git_output(repository, "rev-parse", "--show-toplevel")).resolve()
    if top_level != repository:
        raise ScopedCompletionError(
            "completion protocol refuses a second or nested Repository path"
        )
    branch = _git_output(repository, "branch", "--show-current")
    if branch != "dev":
        raise ScopedCompletionError(f"completion protocol requires dev, found {branch!r}")
    head = _git_object(_git_output(repository, "rev-parse", "HEAD"), "Repository HEAD")
    origin = _git_object(
        _git_output(repository, "rev-parse", "refs/remotes/origin/dev"),
        "origin/dev",
    )
    if require_clean:
        status = _git_output(
            repository, "status", "--porcelain", "--untracked-files=normal"
        )
        if status:
            raise ScopedCompletionError("Repository must be clean at reconciliation")
    source_tree = _git_object(
        _git_output(repository, "rev-parse", "HEAD^{tree}"),
        "Repository tree",
    )
    return {
        "repository": str(repository),
        "branch": branch,
        "head": head,
        "origin": origin,
        "tree": source_tree,
    }


def _validate_checkpoint_commit(
    task_id: str,
    *,
    commit: str,
    origin_commit: str,
    checkpoint_base: str,
) -> dict[str, str]:
    snapshot = _git_snapshot(require_clean=True)
    normalized_commit = _git_object(commit, "checkpoint commit")
    normalized_origin = _git_object(origin_commit, "origin/dev commit")
    base = _git_object(checkpoint_base, "checkpoint base commit")
    if snapshot["head"] != normalized_commit or snapshot["origin"] != normalized_origin:
        raise ScopedCompletionError(
            "checkpoint arguments do not match actual Repository HEAD/origin/dev"
        )
    if normalized_commit != normalized_origin:
        raise ScopedCompletionError("checkpoint is not pushed to origin/dev")
    if not _git_is_ancestor(Path(snapshot["repository"]), base, normalized_commit):
        raise ScopedCompletionError("checkpoint does not descend from its prepared base")
    parent = _git_output(Path(snapshot["repository"]), "rev-parse", f"{normalized_commit}^")
    if parent != base:
        raise ScopedCompletionError("completion checkpoint must be exactly one commit")
    subject = _git_output(
        Path(snapshot["repository"]), "show", "-s", "--format=%s", normalized_commit
    )
    expected = re.compile(
        rf"^(?:feat|fix|refactor|test|docs|build|ci|chore|perf|security|release)"
        rf"\([a-z0-9-]+\): \[{re.escape(task_id)}\] \S.*$"
    )
    if expected.fullmatch(subject) is None:
        raise ScopedCompletionError(
            f"checkpoint commit subject does not uniquely identify {task_id}"
        )
    return snapshot


def _validate_prepare_repository(checkpoint_base: str) -> dict[str, str]:
    """Validate the only allowed pre-check worktree mutation: the manifest."""

    snapshot = _git_snapshot(require_clean=False)
    base = _git_object(checkpoint_base, "checkpoint base commit")
    if snapshot["head"] != base:
        raise ScopedCompletionError(
            "pre-check Repository HEAD must equal the task checkpoint base"
        )
    repository = Path(snapshot["repository"])
    status = _git_output(
        repository, "status", "--porcelain", "--untracked-files=normal"
    )
    manifest_rel = QUALIFICATION_MANIFEST_RELATIVE
    changed: list[str] = []
    for line in status.splitlines():
        if len(line) < 4:
            raise ScopedCompletionError("cannot parse Repository status during prepare")
        changed.append(line[3:].strip().replace("\\", "/"))
    if sorted(set(changed)) != [manifest_rel]:
        raise ScopedCompletionError(
            "completion prepare may change only the canonical qualification manifest"
        )
    base_tree = _git_object(
        _git_output(repository, "rev-parse", f"{base}^{{tree}}"),
        "qualified source tree",
    )
    snapshot["base"] = base
    snapshot["baseTree"] = base_tree
    return snapshot


def _validate_attempt_artifacts(
    *,
    attempt_task: str,
    attempt_number: int,
    candidate_identity: str,
    precheck_sha256: str,
    manifest_sha256: str,
    checkpoint_base: str,
    phase: str,
) -> dict[str, Any]:
    """Validate fixed evidence locations and their cross-file identity bindings."""

    candidate = _candidate_identity(candidate_identity)
    expected_precheck = _sha256(precheck_sha256, "precheck digest")
    expected_manifest = _sha256(manifest_sha256, "manifest digest")
    if phase == "prepare":
        repository_snapshot = _validate_prepare_repository(checkpoint_base)
    else:
        repository_snapshot = _git_snapshot(require_clean=True)
        repository = Path(repository_snapshot["repository"])
        base = _git_object(checkpoint_base, "checkpoint base commit")
        repository_snapshot["base"] = base
        repository_snapshot["baseTree"] = _git_object(
            _git_output(repository, "rev-parse", f"{base}^{{tree}}"),
            "qualified source tree",
        )
    precheck_path = _attempt_evidence_path(attempt_task, "precheck")
    manifest_path = _manifest_path()
    actual_precheck = _file_sha256(precheck_path, "precheck")
    actual_manifest = _file_sha256(manifest_path, "qualification manifest")
    if actual_precheck != expected_precheck:
        raise ScopedCompletionError("precheck digest does not match the fixed evidence file")
    if actual_manifest != expected_manifest:
        raise ScopedCompletionError(
            "manifest digest does not match the fixed qualification artifact"
        )
    precheck = _safe_load_json(precheck_path, "precheck evidence")
    manifest = _safe_load_json(manifest_path, "qualification manifest")
    if manifest.get("schema") != "cmclient-windows-live-first-candidate/v1":
        raise ScopedCompletionError("qualification manifest schema is invalid")
    if manifest.get("scope") != SCOPE:
        raise ScopedCompletionError("qualification manifest scope is invalid")
    if manifest.get("attemptTask") != attempt_task or manifest.get("attemptNumber") != attempt_number:
        raise ScopedCompletionError("qualification manifest attempt binding differs")
    manifest_candidate = manifest.get("candidateIdentity", manifest.get("candidateId"))
    if manifest_candidate is None:
        raise ScopedCompletionError("qualification manifest lacks candidate identity")
    if _candidate_identity(manifest_candidate) != candidate:
        raise ScopedCompletionError("manifest candidate identity differs from prepared candidate")
    precheck_candidate = precheck.get("candidateIdentity", precheck.get("candidateId"))
    if precheck_candidate is not None and _candidate_identity(precheck_candidate) != candidate:
        raise ScopedCompletionError("precheck candidate identity differs from manifest")
    for document, label in ((precheck, "precheck"), (manifest, "manifest")):
        if document.get("attemptTask") is not None and document.get("attemptTask") != attempt_task:
            raise ScopedCompletionError(f"{label} attempt task binding differs")
        if document.get("attemptNumber") is not None and document.get("attemptNumber") != attempt_number:
            raise ScopedCompletionError(f"{label} attempt number binding differs")
    source_commit = _git_object(
        manifest.get("qualifiedSourceCommit"), "manifest qualified source commit"
    )
    if source_commit != checkpoint_base:
        raise ScopedCompletionError("manifest source commit is not the pre-check checkpoint base")
    source_tree = _git_object(
        manifest.get("qualifiedSourceTree"), "manifest qualified source tree"
    )
    if source_tree != repository_snapshot["baseTree"]:
        raise ScopedCompletionError("manifest source tree is not the qualified pre-check tree")
    if candidate.removeprefix("sha256:") == source_tree:
        raise ScopedCompletionError("candidate identity cannot equal a manifest-only source tree")
    for field in ("runtimeSha256", "setupSha256"):
        _sha256(manifest.get(field), f"manifest {field}")
    if _sha256(manifest.get("precheckSha256"), "manifest precheckSha256") != actual_precheck:
        raise ScopedCompletionError("manifest does not bind the exact precheck")
    if manifest.get("manifestOnly") is True or manifest.get("artifactKind") == "manifest-only":
        raise ScopedCompletionError("manifest-only artifact cannot be a qualified candidate")
    if phase == "prepare":
        repo_head = precheck.get("repoHead")
        if repo_head is not None and _git_object(repo_head, "precheck repoHead") != checkpoint_base:
            raise ScopedCompletionError("precheck repoHead does not equal checkpoint base")
    return {
        "candidateIdentity": candidate,
        "precheckSha256": actual_precheck,
        "manifestSha256": actual_manifest,
        "precheckPath": str(precheck_path),
        "manifestPath": str(manifest_path),
        "repository": repository_snapshot,
    }


def _validate_postcheck_artifact(
    state: dict[str, Any], projection: dict[str, Any]
) -> dict[str, Any]:
    head = projection.get("head")
    if not isinstance(head, dict):
        raise ScopedCompletionError("postcheck requires a pushed attempt head")
    task_id = head["attemptTask"]
    task = _task_index(state).get(task_id)
    if task is None:
        raise ScopedCompletionError(f"postcheck attempt task is missing: {task_id}")
    checkpoint_base = _git_object(
        task.get("checkpointBaseCommit"), f"{task_id}.checkpointBaseCommit"
    )
    artifacts = _validate_attempt_artifacts(
        attempt_task=task_id,
        attempt_number=head["attemptNumber"],
        candidate_identity=head["candidateIdentity"],
        precheck_sha256=projection["prepared"]["precheckSha256"],
        manifest_sha256=head["manifestSha256"],
        checkpoint_base=checkpoint_base,
        phase="pushed",
    )
    snapshot = artifacts["repository"]
    if snapshot["head"] != head["commit"] or snapshot["origin"] != head["originCommit"]:
        raise ScopedCompletionError("postcheck Repository head differs from pushed ledger head")
    path = _attempt_evidence_path(task_id, "postcheck")
    document = _safe_load_json(path, "postcheck evidence")
    if document.get("schema") != "cmclient-windows-live-first-postcheck/v1":
        raise ScopedCompletionError("postcheck schema is invalid")
    expected = {
        "status": "pass",
        "scope": SCOPE,
        "attemptTask": task_id,
        "attemptNumber": head["attemptNumber"],
        "commit": head["commit"],
        "manifestSha256": head["manifestSha256"],
        "candidateIdentity": head["candidateIdentity"],
    }
    for field, value in expected.items():
        if document.get(field) != value:
            raise ScopedCompletionError(f"postcheck binding differs: {field}")
    bindings = document.get("preCompletionBindings")
    if not isinstance(bindings, dict) or set(bindings) != {
        "qualifiedSourceCommit",
        "qualifiedSourceTree",
        "runtimeSha256",
        "setupSha256",
        "precheckSha256",
    }:
        raise ScopedCompletionError(
            "postcheck lacks exact source/tree/runtime/Setup/precheck bindings"
        )
    manifest = _safe_load_json(_manifest_path(), "qualification manifest")
    for field in (
        "qualifiedSourceCommit",
        "qualifiedSourceTree",
        "runtimeSha256",
        "setupSha256",
        "precheckSha256",
    ):
        if bindings.get(field) != manifest.get(field):
            raise ScopedCompletionError(f"postcheck pre-completion binding differs: {field}")
    by_id = _task_index(state)
    incomplete = [
        task_id
        for task_id in (f"P18-T{number:02d}" for number in range(1, 10))
        if task_id not in by_id or by_id[task_id].get("status") != "done"
    ]
    if incomplete:
        raise ScopedCompletionError(
            "Windows scoped closure is incomplete: " + ", ".join(incomplete)
        )
    return document


def _validate_contract(state_path: Path, *, operation_id: str | None = None) -> dict[str, Any]:
    library = task_state_library()
    graph_path, license_path, history_path, repository = _contract_paths()
    state = library.load_json(Path(state_path))
    graph = library._load_document(graph_path, "unified task graph lock")
    license_data = library._load_document(license_path, "license provenance")
    if graph.get("schema") != getattr(library, "GRAPH_LOCK_SCHEMA_V3", ""):
        raise ScopedCompletionError("scoped completion requires the graph v3 lock")
    try:
        library.validate_upgrade_journal_guard(Path(state_path), _operation_id(operation_id))
        library.validate_state_against_graph_lock(state, graph, license_data)
    except (TypeError, ValueError) as error:
        raise ScopedCompletionError(str(error)) from error
    if not history_path.is_file():
        raise ScopedCompletionError("immutable graph history is missing")
    if not repository.is_dir():
        raise ScopedCompletionError("Repository root is missing")
    return {
        "state": state,
        "graph": graph,
        "license": license_data,
        "history": library.load_json(history_path),
        "repository": repository,
    }


@contextlib.contextmanager
def _completion_locks(state_path: Path, ledger_path: Path):
    """Serialize graph validation, task state, allocation, and ledger transition."""

    library = task_state_library()
    if Path(state_path).resolve().parent != Path(ledger_path).resolve().parent:
        raise ScopedCompletionError(
            "task state and completion ledger must share one locked state directory"
        )
    with library.state_lock(Path(state_path)):
        yield


def _reject_sensitive_payload(value: object, label: str = "payload") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            if not isinstance(key, str) or (
                SENSITIVE_KEY_RE.search(key) and key not in SAFE_IDENTITY_KEYS
            ):
                raise ScopedCompletionError(f"{label} contains a sensitive field")
            _reject_sensitive_payload(child, f"{label}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_sensitive_payload(child, f"{label}[{index}]")
    elif isinstance(value, str):
        if len(value) > 2048 or "\x00" in value:
            raise ScopedCompletionError(f"{label} contains an invalid string")


def _validate_event_payload(event_type: str, payload: dict[str, Any]) -> None:
    expected = EVENT_PAYLOAD_KEYS.get(event_type)
    if expected is None or set(payload) != expected:
        raise ScopedCompletionError(f"{event_type} payload fields are invalid")
    _reject_sensitive_payload(payload)
    if "reason" in payload:
        reason = payload["reason"]
        if not isinstance(reason, str) or SAFE_REASON_RE.fullmatch(reason) is None:
            raise ScopedCompletionError("reconciliation reason is not sanitized")
    if "classificationReason" in payload:
        reason = payload["classificationReason"]
        if not isinstance(reason, str) or SAFE_REASON_RE.fullmatch(reason) is None:
            raise ScopedCompletionError("classification reason is not sanitized")


def _timestamp(value: str | None = None) -> str:
    timestamp = value or task_state_library().utc_now()
    normalized = timestamp[:-1] + "+00:00" if timestamp.endswith("Z") else timestamp
    try:
        parsed = datetime.fromisoformat(normalized)
    except (TypeError, ValueError) as error:
        raise ScopedCompletionError("event timestamp must be ISO-8601") from error
    if parsed.tzinfo is None:
        raise ScopedCompletionError("event timestamp must include a timezone")
    return timestamp


def _sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or SHA256_RE.fullmatch(value) is None:
        raise ScopedCompletionError(f"{label} must be an exact SHA-256 digest")
    return value.lower()


def _git_object(value: object, label: str) -> str:
    try:
        return task_state_library().normalize_git_object(value, label)
    except ValueError as error:
        raise ScopedCompletionError(str(error)) from error


def _candidate_identity(value: object) -> str:
    try:
        return task_state_library().normalize_candidate_identity(value)
    except ValueError as error:
        raise ScopedCompletionError(str(error)) from error


def _task_number(task_id: object, label: str) -> int:
    if not isinstance(task_id, str):
        raise ScopedCompletionError(f"{label} must be a P18 task ID")
    match = TASK_ID_RE.fullmatch(task_id)
    if match is None:
        raise ScopedCompletionError(f"{label} must be a P18 task ID")
    return int(match.group("number"))


def _normalize_repo_path(value: object, label: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ScopedCompletionError(f"{label} must be an exact Repository path")
    if "\\" in value or GLOB_RE.search(value):
        raise ScopedCompletionError(
            f"{label} must use slash-separated exact files without globs"
        )
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or value.endswith("/")
        or not path.parts
        or any(part in {"", ".", ".."} for part in path.parts)
        or re.match(r"^[A-Za-z]:", value)
    ):
        raise ScopedCompletionError(f"{label} must be a Repository-relative file")
    return path.as_posix()


def normalize_exact_paths(values: list[str], label: str) -> list[str]:
    normalized = [
        _normalize_repo_path(value, f"{label}[{index}]")
        for index, value in enumerate(values)
    ]
    if len(normalized) != len(set(normalized)):
        raise ScopedCompletionError(f"{label} contains duplicate paths")
    return sorted(normalized)


def completion_tool_allowlist_sha256(paths: list[str]) -> str:
    normalized = normalize_exact_paths(paths, "completion-tool allowlist")
    return task_state_library().canonical_sha256({"paths": normalized})


def classify_failure(
    requested_class: str,
    *,
    changed_paths: list[str] | None = None,
    completion_tool_allowlist: list[str] | None = None,
    locked_allowlist_sha256: str | None = None,
    frozen_allowlist_sha256: str | None = None,
    exclusions_proven: bool = False,
) -> dict[str, Any]:
    """Fail closed unless a tool-only repair matches the frozen exact allowlist."""

    graph_path, _license_path, _history_path, _repository = _contract_paths()
    locked_paths: list[str] | None = None
    if graph_path.is_file():
        try:
            graph = json.loads(graph_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise ScopedCompletionError(f"cannot read graph lock: {error}") from error
        allowlist_document = graph.get("completionToolOnlyRepairAllowlist")
        if not isinstance(allowlist_document, dict):
            raise ScopedCompletionError("graph-locked completion allowlist is missing")
        locked_paths = normalize_exact_paths(
            allowlist_document.get("paths", []), "graph-locked allowlist"
        )
        expected_locked = completion_tool_allowlist_sha256(locked_paths)
        if allowlist_document.get("sha256") != expected_locked:
            raise ScopedCompletionError("graph-locked completion allowlist digest is invalid")
        if completion_tool_allowlist is not None and normalize_exact_paths(
            completion_tool_allowlist, "completion-tool allowlist"
        ) != locked_paths:
            raise ScopedCompletionError("caller allowlist differs from graph-locked allowlist")
        completion_tool_allowlist = locked_paths

    normalized_class = FAILURE_CLASS_ALIASES.get(requested_class)
    if normalized_class is None:
        raise ScopedCompletionError(f"unknown failure class: {requested_class!r}")
    changes = normalize_exact_paths(changed_paths or [], "changed paths")
    allowlist = normalize_exact_paths(
        locked_paths if locked_paths is not None else (completion_tool_allowlist or []),
        "completion-tool allowlist",
    )
    computed_digest = completion_tool_allowlist_sha256(allowlist)
    locked_digest = (
        _sha256(locked_allowlist_sha256, "locked allowlist digest")
        if locked_allowlist_sha256 is not None
        else None
    )
    frozen_digest = (
        _sha256(frozen_allowlist_sha256, "frozen allowlist digest")
        if frozen_allowlist_sha256 is not None
        else None
    )

    if normalized_class == FAILURE_CLASS_TOOL and (
        locked_digest != computed_digest or frozen_digest != computed_digest
    ):
        raise ScopedCompletionError(
            "completion-tool defect requires both caller digests to equal the graph-locked allowlist"
        )

    if normalized_class == FAILURE_CLASS_RECONCILIATION:
        if changes:
            raise ScopedCompletionError(
                "a reconciliation blocker cannot claim Repository changes"
            )
        return {
            "requestedClass": normalized_class,
            "classification": normalized_class,
            "candidateReset": False,
            "changedPaths": [],
            "allowlistSha256": computed_digest,
            "toolOnlyApproved": False,
            "reason": "retry reconciliation against the same pushed attempt",
        }

    digest_matches = (
        locked_digest is not None
        and frozen_digest is not None
        and locked_digest == computed_digest
        and frozen_digest == computed_digest
    )
    exact_match = bool(changes) and bool(allowlist) and set(changes) <= set(allowlist)
    tool_only = (
        normalized_class == FAILURE_CLASS_TOOL
        and exclusions_proven
        and digest_matches
        and exact_match
    )
    classification = FAILURE_CLASS_TOOL if tool_only else FAILURE_CLASS_CANDIDATE
    if tool_only:
        reason = "all changed files match the frozen, exclusion-proven exact allowlist"
    elif not allowlist:
        reason = "the completion-tool-only allowlist is empty"
    elif not exclusions_proven:
        reason = "completion-tool-only exclusion proof is absent"
    elif not digest_matches:
        reason = "the current allowlist does not match its locked frozen digest"
    elif not exact_match:
        reason = "one or more changed files are outside the exact allowlist"
    else:
        reason = "the requested failure class is candidate-affecting"
    return {
        "requestedClass": normalized_class,
        "classification": classification,
        "candidateReset": classification == FAILURE_CLASS_CANDIDATE,
        "changedPaths": changes,
        "allowlistSha256": computed_digest,
        "toolOnlyApproved": tool_only,
        "reason": reason,
    }


def new_ledger() -> dict[str, Any]:
    return {
        "schema": LEDGER_SCHEMA,
        "scope": SCOPE,
        "coordinatorTask": COORDINATOR_TASK,
        "completionState": {"headAttempt": None},
        "events": [],
    }


def load_ledger(path: Path) -> dict[str, Any]:
    if not Path(path).exists():
        return new_ledger()
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ScopedCompletionError(f"cannot read attempt ledger {path}: {error}") from error
    if not isinstance(value, dict):
        raise ScopedCompletionError("attempt ledger root must be an object")
    return value


def _event_hash(event: dict[str, Any]) -> str:
    unhashed = {key: value for key, value in event.items() if key != "eventHash"}
    return task_state_library().canonical_sha256(unhashed)


def _require_payload_value(payload: dict[str, Any], key: str, label: str) -> Any:
    if key not in payload:
        raise ScopedCompletionError(f"{label} lacks {key}")
    return payload[key]


def _matching_attempt(payload: dict[str, Any], head: dict[str, Any]) -> None:
    if (
        payload.get("attemptTask") != head["attemptTask"]
        or payload.get("attemptNumber") != head["attemptNumber"]
    ):
        raise ScopedCompletionError("event does not identify the pushed head attempt")
    commit = payload.get("commit")
    if commit is None or _git_object(commit, "event commit") != head["commit"]:
        raise ScopedCompletionError("event commit does not match the pushed head")


def _validate_dynamic_definition(
    value: object,
    *,
    task_id: str,
    expected_scope: str,
) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ScopedCompletionError(f"dynamic task definition is missing for {task_id}")
    if value.get("id") != task_id or value.get("phase") != "P18":
        raise ScopedCompletionError(f"dynamic task definition ID is invalid: {task_id}")
    if value.get("scope") != expected_scope:
        raise ScopedCompletionError(f"dynamic task scope is invalid: {task_id}")
    if value.get("required") is not True or value.get("status") != "in_progress":
        raise ScopedCompletionError(f"dynamic task is not required/in_progress: {task_id}")
    if value.get("manualGate") is not False or value.get("kind") not in {"fix", "release"}:
        raise ScopedCompletionError(f"dynamic task template is invalid: {task_id}")
    if value.get("lane") != "windows-package" or value.get("priority") != 20:
        raise ScopedCompletionError(f"dynamic task lane/priority is invalid: {task_id}")
    if value.get("completionProtocol") != PROTOCOL_MARKER:
        raise ScopedCompletionError(f"dynamic task protocol marker is invalid: {task_id}")
    if value.get("caseGroups") != ["COMP"]:
        raise ScopedCompletionError(f"dynamic task case groups are invalid: {task_id}")
    dependencies = value.get("dependsOn")
    if not isinstance(dependencies, list) or len(dependencies) != len(set(dependencies)):
        raise ScopedCompletionError(f"dynamic task dependencies are invalid: {task_id}")
    if PREREQUISITE_TASK not in dependencies:
        raise ScopedCompletionError(f"dynamic task must depend on {PREREQUISITE_TASK}: {task_id}")
    return copy.deepcopy(value)


def _reduce_events(events: list[dict[str, Any]]) -> dict[str, Any]:
    projection: dict[str, Any] = {
        "phase": "empty",
        "head": None,
        "prepared": None,
        "currentAttempt": None,
        "currentRepair": None,
        "nextAttempt": None,
        "definitions": {},
        "definitionOrder": [],
        "taskStates": {},
        "taskCommits": {},
        "taskCompletedAt": {},
        "coordinatorStatus": None,
        "coordinatorStage": None,
        "coordinatorDependencies": [],
        "candidateResolutions": {},
        "commits": set(),
        "manifestDigests": set(),
        "lastEventHash": None,
    }
    previous_hash: str | None = None

    for index, event in enumerate(events):
        label = f"events[{index}]"
        if not isinstance(event, dict) or set(event) != EVENT_KEYS:
            raise ScopedCompletionError(f"{label} fields are invalid")
        if event.get("sequence") != index + 1:
            raise ScopedCompletionError(f"{label}.sequence is invalid")
        if event.get("type") not in EVENT_TYPES:
            raise ScopedCompletionError(f"{label}.type is invalid")
        _timestamp(event.get("recordedAt"))
        if event.get("previousEventHash") != previous_hash:
            raise ScopedCompletionError(f"{label} breaks the hash link")
        stored_hash = _sha256(event.get("eventHash"), f"{label}.eventHash")
        if stored_hash != _event_hash(event):
            raise ScopedCompletionError(f"{label} event hash is invalid")
        payload = event.get("payload")
        if not isinstance(payload, dict):
            raise ScopedCompletionError(f"{label}.payload must be an object")
        event_type = event["type"]
        _validate_event_payload(event_type, payload)
        phase = projection["phase"]

        if event_type == "attempt_prepared":
            if phase not in {"empty", "repair_pushed"}:
                raise ScopedCompletionError("attempt_prepared is out of order")
            task_id = _require_payload_value(payload, "attemptTask", label)
            attempt_number = _require_payload_value(payload, "attemptNumber", label)
            if phase == "empty":
                if task_id != COORDINATOR_TASK or attempt_number != 1:
                    raise ScopedCompletionError(
                        "the coordinator must be scoped completion attempt 1"
                    )
                expected_prior = None
            else:
                expected = projection["nextAttempt"]
                if not isinstance(expected, dict) or (
                    task_id != expected["task"]
                    or attempt_number != expected["number"]
                ):
                    raise ScopedCompletionError(
                        "prepared retry does not match the allocated odd attempt"
                    )
                expected_prior = projection["head"]["eventHash"]
            if payload.get("priorPushedHead") != expected_prior:
                raise ScopedCompletionError("prepared attempt has a stale prior head")
            candidate = _candidate_identity(
                _require_payload_value(payload, "candidateIdentity", label)
            )
            precheck = _sha256(
                _require_payload_value(payload, "precheckSha256", label),
                f"{label}.precheckSha256",
            )
            manifest = _sha256(
                _require_payload_value(payload, "intendedManifestSha256", label),
                f"{label}.intendedManifestSha256",
            )
            if "commit" in payload:
                raise ScopedCompletionError("attempt_prepared must not contain a commit")
            if manifest in projection["manifestDigests"]:
                raise ScopedCompletionError("attempt manifest digest cannot be reused")
            projection["manifestDigests"].add(manifest)
            projection["prepared"] = {
                "attemptTask": task_id,
                "attemptNumber": attempt_number,
                "candidateIdentity": candidate,
                "precheckSha256": precheck,
                "intendedManifestSha256": manifest,
                "eventHash": stored_hash,
            }
            projection["currentAttempt"] = {
                "task": task_id,
                "number": attempt_number,
            }
            if task_id == COORDINATOR_TASK:
                projection["coordinatorStatus"] = "in_progress"
            else:
                projection["taskStates"][task_id] = "in_progress"
                projection["coordinatorStatus"] = "blocked"
            projection["coordinatorStage"] = "checkpoint_pending"
            projection["phase"] = "prepared"

        elif event_type == "attempt_pushed":
            if phase != "prepared":
                raise ScopedCompletionError("attempt_pushed lacks a prepared attempt")
            prepared = projection["prepared"]
            if (
                payload.get("attemptTask") != prepared["attemptTask"]
                or payload.get("attemptNumber") != prepared["attemptNumber"]
                or payload.get("preparedEventHash") != prepared["eventHash"]
            ):
                raise ScopedCompletionError("attempt_pushed disagrees with preparation")
            commit = _git_object(
                _require_payload_value(payload, "commit", label), f"{label}.commit"
            )
            origin_commit = _git_object(
                _require_payload_value(payload, "originCommit", label),
                f"{label}.originCommit",
            )
            manifest = _sha256(
                _require_payload_value(payload, "manifestSha256", label),
                f"{label}.manifestSha256",
            )
            if commit != origin_commit:
                raise ScopedCompletionError("attempt commit does not equal origin/dev")
            if manifest != prepared["intendedManifestSha256"]:
                raise ScopedCompletionError("pushed manifest differs from preparation")
            candidate = _candidate_identity(
                _require_payload_value(payload, "candidateIdentity", label)
            )
            if candidate != prepared["candidateIdentity"]:
                raise ScopedCompletionError("pushed candidate differs from preparation")
            if commit in projection["commits"]:
                raise ScopedCompletionError("completion checkpoint commit cannot be reused")
            projection["commits"].add(commit)
            task_id = prepared["attemptTask"]
            projection["taskCommits"][task_id] = commit
            if task_id == COORDINATOR_TASK:
                projection["coordinatorStatus"] = "in_progress"
            else:
                projection["taskStates"][task_id] = "done"
                projection["taskCompletedAt"][task_id] = event["recordedAt"]
                projection["coordinatorStatus"] = "in_progress"
                projection["coordinatorDependencies"] = list(
                    projection["definitionOrder"]
                )
            projection["coordinatorStage"] = "postcheck_pending"
            projection["head"] = {
                "attemptTask": task_id,
                "attemptNumber": prepared["attemptNumber"],
                "commit": commit,
                "originCommit": origin_commit,
                "manifestSha256": manifest,
                "candidateIdentity": prepared["candidateIdentity"],
                "preparedEventHash": prepared["eventHash"],
                "eventHash": stored_hash,
            }
            projection["phase"] = "pushed"

        elif event_type == "reconciliation_blocked":
            if phase != "pushed" or projection["head"] is None:
                raise ScopedCompletionError("reconciliation_blocked is out of order")
            _matching_attempt(payload, projection["head"])
            if not isinstance(payload.get("reason"), str) or not payload["reason"].strip():
                raise ScopedCompletionError("reconciliation blocker reason is required")
            projection["coordinatorStatus"] = "blocked"
            projection["coordinatorStage"] = "reconciliation_blocked"
            projection["phase"] = "reconciliation_blocked"

        elif event_type == "reconciliation_resumed":
            if phase != "reconciliation_blocked" or projection["head"] is None:
                raise ScopedCompletionError("reconciliation_resumed is out of order")
            _matching_attempt(payload, projection["head"])
            projection["coordinatorStatus"] = "in_progress"
            projection["coordinatorStage"] = "postcheck_pending"
            projection["phase"] = "pushed"

        elif event_type == "attempt_failed":
            if phase != "pushed" or projection["head"] is None:
                raise ScopedCompletionError("attempt_failed is out of order")
            _matching_attempt(payload, projection["head"])
            failure_class = payload.get("failureClass")
            candidate_reset = payload.get("candidateReset")
            if failure_class not in {FAILURE_CLASS_TOOL, FAILURE_CLASS_CANDIDATE}:
                raise ScopedCompletionError("attempt_failed has invalid classification")
            if candidate_reset is not (failure_class == FAILURE_CLASS_CANDIDATE):
                raise ScopedCompletionError("failure candidateReset is inconsistent")
            projection["coordinatorStatus"] = "blocked"
            projection["coordinatorStage"] = "completion_postcheck_failed"
            projection["phase"] = "failed"

        elif event_type == "repair_allocated":
            if phase != "failed":
                raise ScopedCompletionError("repair_allocated is out of order")
            repair_id = _require_payload_value(payload, "repairTask", label)
            number = _task_number(repair_id, f"{label}.repairTask")
            if number < 20 or number > 98 or number % 2 != 0:
                raise ScopedCompletionError("completion repair must use P18-T20..T98 even")
            if repair_id in projection["definitions"]:
                raise ScopedCompletionError(f"completion task ID was reused: {repair_id}")
            definition = _validate_dynamic_definition(
                payload.get("taskDefinition"),
                task_id=repair_id,
                expected_scope=REPAIR_SCOPE,
            )
            if definition.get("kind") != "fix" or definition.get("repairOf") != COORDINATOR_TASK:
                raise ScopedCompletionError("completion repair definition is invalid")
            previous = events[index - 1]["payload"]
            if definition.get("candidateReset") is not previous.get("candidateReset"):
                raise ScopedCompletionError("repair candidateReset differs from failure")
            projection["definitions"][repair_id] = definition
            projection["definitionOrder"].append(repair_id)
            projection["taskStates"][repair_id] = "in_progress"
            projection["currentRepair"] = repair_id
            projection["coordinatorStatus"] = "blocked"
            projection["phase"] = "repair_allocated"

        elif event_type == "repair_pushed":
            if phase != "repair_allocated":
                raise ScopedCompletionError("repair_pushed is out of order")
            repair_id = projection["currentRepair"]
            if payload.get("repairTask") != repair_id:
                raise ScopedCompletionError("repair_pushed names the wrong repair")
            commit = _git_object(
                _require_payload_value(payload, "commit", label), f"{label}.commit"
            )
            origin_commit = _git_object(
                _require_payload_value(payload, "originCommit", label),
                f"{label}.originCommit",
            )
            if commit != origin_commit or commit in projection["commits"]:
                raise ScopedCompletionError("repair commit is not unique at origin/dev")
            projection["commits"].add(commit)
            projection["taskCommits"][repair_id] = commit
            projection["taskCompletedAt"][repair_id] = event["recordedAt"]
            projection["taskStates"][repair_id] = "done"

            attempt_id = _require_payload_value(payload, "nextAttemptTask", label)
            attempt_number = _require_payload_value(payload, "nextAttemptNumber", label)
            slot = _task_number(attempt_id, f"{label}.nextAttemptTask")
            if slot < 21 or slot > 99 or slot % 2 != 1:
                raise ScopedCompletionError("completion retry must use P18-T21..T99 odd")
            if attempt_number != projection["head"]["attemptNumber"] + 1:
                raise ScopedCompletionError("completion attempt number is not monotonic")
            if attempt_id in projection["definitions"]:
                raise ScopedCompletionError(f"completion task ID was reused: {attempt_id}")
            definition = _validate_dynamic_definition(
                payload.get("attemptDefinition"),
                task_id=attempt_id,
                expected_scope=ATTEMPT_SCOPE,
            )
            if (
                definition.get("kind") != "release"
                or definition.get("candidateReset") is not False
                or definition.get("completionAttemptNumber") != attempt_number
                or COORDINATOR_TASK in definition.get("dependsOn", [])
            ):
                raise ScopedCompletionError("completion attempt definition is invalid")
            projection["definitions"][attempt_id] = definition
            projection["definitionOrder"].append(attempt_id)
            projection["taskStates"][attempt_id] = "in_progress"
            projection["nextAttempt"] = {
                "task": attempt_id,
                "number": attempt_number,
            }
            projection["coordinatorStatus"] = "blocked"
            projection["phase"] = "repair_pushed"

        elif event_type == "attempt_passed":
            if phase != "pushed" or projection["head"] is None:
                raise ScopedCompletionError("attempt_passed is out of order")
            _matching_attempt(payload, projection["head"])
            candidate = projection["head"].get("candidateIdentity")
            if not isinstance(candidate, str):
                raise ScopedCompletionError("pushed head lacks candidate identity")
            for repair_id, definition in projection["definitions"].items():
                if definition.get("candidateReset") is True:
                    invalidation = definition.get("candidateInvalidation")
                    if isinstance(invalidation, dict):
                        previous = invalidation.get("invalidatedCandidate")
                        if previous == candidate:
                            raise ScopedCompletionError(
                                "a candidate-affecting repair cannot pass on the invalidated candidate"
                            )
                        projection["candidateResolutions"][repair_id] = {
                            "candidate": candidate,
                            "resolvedAt": event["recordedAt"],
                        }
            projection["coordinatorStatus"] = "in_progress"
            projection["coordinatorStage"] = "postcheck_passed"
            projection["phase"] = "passed"

        else:
            if phase != "passed" or payload.get("coordinatorTask") != COORDINATOR_TASK:
                raise ScopedCompletionError("coordinator_completed is out of order")
            if payload.get("headEventHash") != projection["head"]["eventHash"]:
                raise ScopedCompletionError("coordinator completed against a stale head")
            projection["coordinatorStatus"] = "done"
            projection["coordinatorStage"] = "completed"
            projection["coordinatorCompletedAt"] = event["recordedAt"]
            projection["phase"] = "complete"

        previous_hash = stored_hash
        projection["lastEventHash"] = stored_hash

    return projection


def validate_ledger(
    ledger: dict[str, Any], *, strict_head: bool = True
) -> dict[str, Any]:
    if ledger.get("schema") != LEDGER_SCHEMA:
        raise ScopedCompletionError(f"attempt ledger schema must be {LEDGER_SCHEMA}")
    if ledger.get("scope") != SCOPE or ledger.get("coordinatorTask") != COORDINATOR_TASK:
        raise ScopedCompletionError("attempt ledger scope/coordinator is invalid")
    events = ledger.get("events")
    if not isinstance(events, list):
        raise ScopedCompletionError("attempt ledger events must be an array")
    projection = _reduce_events(events)
    completion_state = ledger.get("completionState")
    if not isinstance(completion_state, dict):
        raise ScopedCompletionError("attempt ledger completionState must be an object")
    if strict_head and completion_state.get("headAttempt") != projection["head"]:
        raise ScopedCompletionError("attempt ledger head is stale or not pushed")
    return projection


def append_event(
    ledger: dict[str, Any],
    event_type: str,
    payload: dict[str, Any],
    *,
    now: str | None = None,
) -> dict[str, Any]:
    if event_type not in EVENT_TYPES:
        raise ScopedCompletionError(f"unknown ledger event type: {event_type}")
    projection = validate_ledger(ledger, strict_head=False)
    events = ledger["events"]
    event = {
        "sequence": len(events) + 1,
        "type": event_type,
        "recordedAt": _timestamp(now),
        "previousEventHash": projection["lastEventHash"],
        "payload": copy.deepcopy(payload),
    }
    event["eventHash"] = _event_hash(event)
    events.append(event)
    projection = validate_ledger(ledger, strict_head=False)
    ledger["completionState"] = {"headAttempt": copy.deepcopy(projection["head"])}
    validate_ledger(ledger)
    return copy.deepcopy(event)


def _task_index(state: dict[str, Any]) -> dict[str, dict[str, Any]]:
    tasks = state.get("tasks")
    if not isinstance(tasks, list):
        raise ScopedCompletionError("task state tasks must be an array")
    by_id: dict[str, dict[str, Any]] = {}
    for task in tasks:
        if not isinstance(task, dict) or not isinstance(task.get("id"), str):
            raise ScopedCompletionError("task state contains an invalid task")
        if task["id"] in by_id:
            raise ScopedCompletionError(f"duplicate task ID: {task['id']}")
        by_id[task["id"]] = task
    return by_id


def _used_p18_ids(state: dict[str, Any], ledger: dict[str, Any]) -> set[str]:
    used = set(_task_index(state))

    def collect(value: object) -> None:
        if isinstance(value, str) and TASK_ID_RE.fullmatch(value):
            used.add(value)
        elif isinstance(value, dict):
            for child in value.values():
                collect(child)
        elif isinstance(value, list):
            for child in value:
                collect(child)

    collect(ledger.get("events", []))
    graph_path, _license_path, history_path, _repository = _contract_paths()
    for path in (graph_path, history_path):
        if path.is_file():
            try:
                collect(json.loads(path.read_text(encoding="utf-8")))
            except (OSError, json.JSONDecodeError) as error:
                raise ScopedCompletionError(
                    f"cannot read immutable ID reservation source {path}: {error}"
                ) from error
    return used


def _allocate_id(
    state: dict[str, Any], ledger: dict[str, Any], *, repair: bool
) -> str:
    used = _used_p18_ids(state, ledger)
    start, stop = (20, 98) if repair else (21, 99)
    for number in range(start, stop + 1, 2):
        task_id = f"P18-T{number:02d}"
        if task_id not in used:
            return task_id
    kind = "repair" if repair else "attempt"
    raise ScopedCompletionError(
        f"Windows scoped completion {kind} task ID range is exhausted"
    )


def _normalize_cases(values: list[str] | None) -> list[str]:
    cases = values or ["FULL_VERIFY"]
    if not all(isinstance(case, str) and case.strip() == case for case in cases):
        raise ScopedCompletionError("affected cases must be normalized strings")
    if len(cases) != len(set(cases)):
        raise ScopedCompletionError("affected cases must be unique")
    allowed = getattr(task_state_library(), "ALLOWED_REPAIR_CASES", set(cases))
    unknown = sorted(set(cases) - set(allowed))
    if unknown:
        raise ScopedCompletionError("unknown affected cases: " + ", ".join(unknown))
    return list(cases)


def _repair_definition(
    state: dict[str, Any],
    repair_id: str,
    classification: dict[str, Any],
    affected_cases: list[str],
    *,
    checkpoint_base: str,
    invalidated_candidate: str | None = None,
    now: str,
) -> dict[str, Any]:
    by_id = _task_index(state)
    coordinator = by_id.get(COORDINATOR_TASK)
    if coordinator is None:
        raise ScopedCompletionError(f"missing coordinator task {COORDINATOR_TASK}")
    dependencies = list(coordinator.get("dependsOn", []))
    unfinished = [
        dependency
        for dependency in dependencies
        if dependency not in by_id or by_id[dependency].get("status") != "done"
    ]
    if unfinished:
        raise ScopedCompletionError(
            "repair allocation found unfinished coordinator dependencies: "
            + ", ".join(unfinished)
        )
    definition: dict[str, Any] = {
        "id": repair_id,
        "phase": "P18",
        "title": f"Repair Windows completion after attempt failure ({repair_id})",
        "status": "in_progress",
        "required": True,
        "manualGate": False,
        "lane": coordinator.get("lane"),
        "priority": coordinator.get("priority"),
        "dependsOn": dependencies,
        "repairOf": COORDINATOR_TASK,
        "kind": "fix",
        "scope": REPAIR_SCOPE,
        "candidateReset": classification["candidateReset"],
        "affectedCases": affected_cases,
        "caseGroups": ["COMP"],
        "owner": coordinator.get("owner"),
        "startedAt": now,
        "completedAt": None,
        "commit": None,
        "checkpointBaseCommit": checkpoint_base,
        "notes": [classification["reason"]],
        "failureClass": classification["classification"],
        "completionProtocol": PROTOCOL_MARKER,
    }
    if classification["candidateReset"]:
        definition["candidateInvalidation"] = {
            "invalidatedAt": now,
            "repairOf": COORDINATOR_TASK,
            "invalidatedCandidate": invalidated_candidate,
            "runtimeCandidate": True,
            "distributionCandidate": True,
            "affectedCases": affected_cases,
            "resolvedByCandidate": None,
            "resolvedAt": None,
        }
    return definition


def _attempt_definition(
    state: dict[str, Any],
    ledger: dict[str, Any],
    attempt_id: str,
    attempt_number: int,
    *,
    checkpoint_base: str,
    now: str,
) -> dict[str, Any]:
    by_id = _task_index(state)
    coordinator = by_id.get(COORDINATOR_TASK)
    if coordinator is None:
        raise ScopedCompletionError(f"missing coordinator task {COORDINATOR_TASK}")
    projection = validate_ledger(ledger, strict_head=False)
    if PREREQUISITE_TASK not in by_id or by_id[PREREQUISITE_TASK].get("status") != "done":
        raise ScopedCompletionError(
            f"{PREREQUISITE_TASK} must be present and done before allocating an attempt"
        )
    dependencies: list[str] = [PREREQUISITE_TASK]
    dependencies.extend(projection["definitionOrder"])
    dependencies = list(dict.fromkeys(dependencies))
    unfinished = [
        dependency
        for dependency in dependencies
        if dependency not in by_id or by_id[dependency].get("status") != "done"
    ]
    # The current repair is represented as in_progress until repair_pushed is
    # appended, but that same event is the authority making it done.
    current_repair = projection.get("currentRepair")
    unfinished = [item for item in unfinished if item != current_repair]
    if unfinished:
        raise ScopedCompletionError(
            "attempt allocation found unfinished dependencies: "
            + ", ".join(unfinished)
        )
    return {
        "id": attempt_id,
        "phase": "P18",
        "title": f"Retry Windows scoped completion (attempt {attempt_number})",
        "status": "in_progress",
        "required": True,
        "manualGate": False,
        "lane": "windows-package",
        "priority": 20,
        "dependsOn": dependencies,
        "kind": "release",
        "scope": ATTEMPT_SCOPE,
        "candidateReset": False,
        "caseGroups": ["COMP"],
        "owner": coordinator.get("owner"),
        "startedAt": now,
        "completedAt": None,
        "commit": None,
        "checkpointBaseCommit": checkpoint_base,
        "notes": [],
        "completionAttemptNumber": attempt_number,
        "completionProtocol": PROTOCOL_MARKER,
    }


def _definition_fields(definition: dict[str, Any]) -> set[str]:
    return set(definition) - TASK_MUTABLE_FIELDS


def _merge_dynamic_task(
    state: dict[str, Any], definition: dict[str, Any]
) -> dict[str, Any]:
    by_id = _task_index(state)
    task_id = definition["id"]
    current = by_id.get(task_id)
    if current is None:
        current = copy.deepcopy(definition)
        state["tasks"].append(current)
        return current
    if current.get("completionProtocol") != PROTOCOL_MARKER:
        raise ScopedCompletionError(
            f"allocated task ID collides with non-protocol task: {task_id}"
        )
    for field in _definition_fields(definition):
        if current.get(field) != definition.get(field):
            raise ScopedCompletionError(
                f"dynamic task definition drift: {task_id}.{field}"
            )
    return current


def _sync_candidate_invalidation(
    state: dict[str, Any],
    task: dict[str, Any],
    definition: dict[str, Any],
    resolution: dict[str, str] | None,
) -> None:
    expected = definition.get("candidateInvalidation")
    if expected is None:
        return
    expected = copy.deepcopy(expected)
    if resolution is not None:
        expected["resolvedByCandidate"] = resolution["candidate"]
        expected["resolvedAt"] = resolution["resolvedAt"]
    current = task.get("candidateInvalidation")
    if current is not None:
        for field in (
            "invalidatedAt",
            "repairOf",
            "invalidatedCandidate",
            "runtimeCandidate",
            "distributionCandidate",
            "affectedCases",
        ):
            if current.get(field) != expected.get(field):
                raise ScopedCompletionError(
                    f"candidate invalidation drift for {task['id']}.{field}"
                )
        current_candidate = current.get("resolvedByCandidate")
        if current_candidate not in {None, expected.get("resolvedByCandidate")}:
            raise ScopedCompletionError(
                f"candidate invalidation resolution drift for {task['id']}"
            )
    task["candidateInvalidation"] = expected

    records = state.setdefault("candidateInvalidations", [])
    if not isinstance(records, list):
        raise ScopedCompletionError("candidateInvalidations must be an array")
    matches = [record for record in records if record.get("repairTask") == task["id"]]
    if len(matches) > 1:
        raise ScopedCompletionError(
            f"duplicate candidate invalidation for {task['id']}"
        )
    ledger_record = {"repairTask": task["id"], **copy.deepcopy(expected)}
    if matches:
        index = records.index(matches[0])
        records[index] = ledger_record
    else:
        records.append(ledger_record)


def _expected_active_task(projection: dict[str, Any]) -> str | None:
    phase = projection["phase"]
    if phase == "prepared":
        return projection["currentAttempt"]["task"]
    if phase in {"pushed", "passed"}:
        return COORDINATOR_TASK
    if phase == "repair_allocated":
        return projection["currentRepair"]
    if phase == "repair_pushed":
        return projection["nextAttempt"]["task"]
    return None


def _reconcile_state(
    state: dict[str, Any], projection: dict[str, Any]
) -> dict[str, Any]:
    state = copy.deepcopy(state)
    by_id = _task_index(state)
    coordinator = by_id.get(COORDINATOR_TASK)
    if coordinator is None:
        raise ScopedCompletionError(f"missing coordinator task {COORDINATOR_TASK}")

    protocol_ids = set(projection["definitions"])
    unexpected = [
        task["id"]
        for task in state["tasks"]
        if task.get("completionProtocol") == PROTOCOL_MARKER
        and task["id"] not in protocol_ids
    ]
    if unexpected:
        raise ScopedCompletionError(
            "task state is ahead of the immutable ledger: " + ", ".join(unexpected)
        )

    for task_id in projection["definitionOrder"]:
        definition = projection["definitions"][task_id]
        task = _merge_dynamic_task(state, definition)
        expected_status = projection["taskStates"].get(task_id, "in_progress")
        if expected_status == "in_progress" and task.get("status") == "done":
            raise ScopedCompletionError(
                f"task state is ahead of the immutable ledger: {task_id} is done"
            )
        expected_commit = projection["taskCommits"].get(task_id)
        current_commit = task.get("commit")
        pushed_unreconciled = (
            expected_commit is None
            and current_commit is not None
            and (
                (
                    projection["phase"] == "prepared"
                    and projection["currentAttempt"]["task"] == task_id
                )
                or (
                    projection["phase"] == "repair_allocated"
                    and projection["currentRepair"] == task_id
                )
            )
        )
        if current_commit is not None and expected_commit is None and not pushed_unreconciled:
            raise ScopedCompletionError(
                f"task state is ahead of the immutable ledger: {task_id} has a commit"
            )
        if current_commit is not None and current_commit != expected_commit:
            raise ScopedCompletionError(f"task commit drift: {task_id}")
        task["status"] = expected_status
        if not pushed_unreconciled:
            task["commit"] = expected_commit
            task["completedAt"] = projection["taskCompletedAt"].get(task_id)
            task.pop("checkpointPushedAt", None)
            task.pop("completionStage", None)
        _sync_candidate_invalidation(
            state,
            task,
            definition,
            projection["candidateResolutions"].get(task_id),
        )

    if projection["phase"] != "empty":
        expected_status = projection["coordinatorStatus"]
        if coordinator.get("status") == "done" and expected_status != "done":
            raise ScopedCompletionError("coordinator state is ahead of the ledger")
        coordinator["status"] = expected_status
        coordinator["completionStage"] = projection["coordinatorStage"]
        current_attempt = projection["currentAttempt"]
        if current_attempt is not None:
            coordinator["completionAttemptTask"] = current_attempt["task"]
            coordinator["completionAttemptNumber"] = current_attempt["number"]
        head = projection["head"]
        coordinator["completionHeadEvent"] = (
            head["eventHash"] if head is not None else None
        )
        initial_commit = projection["taskCommits"].get(COORDINATOR_TASK)
        if initial_commit is not None:
            current_commit = coordinator.get("commit")
            if current_commit not in {None, initial_commit}:
                raise ScopedCompletionError("coordinator checkpoint commit drift")
            coordinator["commit"] = initial_commit
            coordinator.pop("checkpointPushedAt", None)
        if expected_status == "blocked":
            coordinator["blockedAt"] = coordinator.get("blockedAt") or _timestamp()
            if projection["phase"] in {"repair_allocated", "repair_pushed"}:
                coordinator["blockedByRepair"] = projection["currentRepair"]
                coordinator["blockReason"] = "completion_postcheck_failed"
            elif projection["phase"] == "reconciliation_blocked":
                coordinator.pop("blockedByRepair", None)
                coordinator["blockReason"] = "completion_reconciliation_blocked"
        else:
            coordinator.pop("blockedAt", None)
            coordinator.pop("blockedByRepair", None)
            coordinator.pop("blockReason", None)
        if expected_status == "done":
            coordinator["completedAt"] = projection["coordinatorCompletedAt"]

        for dependency in projection["coordinatorDependencies"]:
            if dependency not in coordinator.setdefault("dependsOn", []):
                coordinator["dependsOn"].append(dependency)

    expected_active = _expected_active_task(projection)
    active = [
        task["id"] for task in state["tasks"] if task.get("status") == "in_progress"
    ]
    if projection["phase"] != "empty":
        expected = [] if expected_active is None else [expected_active]
        if active != expected:
            raise ScopedCompletionError(
                f"scoped completion active task mismatch: {active!r} != {expected!r}"
            )
    try:
        task_state_library().validate_task_graph(state)
    except ValueError as error:
        raise ScopedCompletionError(str(error)) from error
    return state


def _sync_ledger_head(
    ledger: dict[str, Any], projection: dict[str, Any]
) -> None:
    ledger["completionState"] = {"headAttempt": copy.deepcopy(projection["head"])}


Mutation = Callable[
    [dict[str, Any], dict[str, Any], dict[str, Any]], dict[str, Any]
]


def _transaction(
    state_path: Path,
    ledger_path: Path,
    mutation: Mutation,
) -> dict[str, Any]:
    library = task_state_library()
    with _completion_locks(Path(state_path), Path(ledger_path)):
        contract = _validate_contract(Path(state_path))
        state = contract["state"]
        ledger = load_ledger(Path(ledger_path))
        projection = validate_ledger(ledger, strict_head=False)
        _sync_ledger_head(ledger, projection)
        state = _reconcile_state(state, projection)
        try:
            library.validate_state_against_graph_lock(
                state, contract["graph"], contract["license"]
            )
        except (TypeError, ValueError) as error:
            raise ScopedCompletionError(str(error)) from error
        result = mutation(state, ledger, projection)
        projection = validate_ledger(ledger, strict_head=False)
        _sync_ledger_head(ledger, projection)
        state = _reconcile_state(state, projection)
        validate_ledger(ledger)
        try:
            library.validate_state_against_graph_lock(
                state, contract["graph"], contract["license"]
            )
        except (TypeError, ValueError) as error:
            raise ScopedCompletionError(str(error)) from error
        # Ledger-first makes every interrupted two-file update replayable.
        library.atomic_write_json(Path(ledger_path), ledger)
        library.atomic_write_json(Path(state_path), state)
        return result


def prepare_attempt(
    state_path: Path,
    ledger_path: Path,
    *,
    attempt_task: str,
    candidate_identity: str,
    precheck_sha256: str,
    intended_manifest_sha256: str,
    now: str | None = None,
) -> dict[str, Any]:
    def mutation(
        state: dict[str, Any],
        ledger: dict[str, Any],
        projection: dict[str, Any],
    ) -> dict[str, Any]:
        if projection["phase"] not in {"empty", "repair_pushed"}:
            raise ScopedCompletionError("no completion attempt is ready to prepare")
        if projection["phase"] == "empty":
            expected_task, attempt_number = COORDINATOR_TASK, 1
        else:
            expected_task = projection["nextAttempt"]["task"]
            attempt_number = projection["nextAttempt"]["number"]
        if attempt_task != expected_task:
            raise ScopedCompletionError(
                f"expected attempt task {expected_task}, found {attempt_task}"
            )
        active = [
            task["id"] for task in state["tasks"] if task.get("status") == "in_progress"
        ]
        if active != [attempt_task]:
            raise ScopedCompletionError(
                f"attempt must be the sole in_progress task: {active!r}"
            )
        task = _task_index(state).get(attempt_task)
        if task is None:
            raise ScopedCompletionError(f"completion attempt task is missing: {attempt_task}")
        artifacts = _validate_attempt_artifacts(
            attempt_task=attempt_task,
            attempt_number=attempt_number,
            candidate_identity=candidate_identity,
            precheck_sha256=precheck_sha256,
            manifest_sha256=intended_manifest_sha256,
            checkpoint_base=_git_object(
                task.get("checkpointBaseCommit"),
                f"{attempt_task}.checkpointBaseCommit",
            ),
            phase="prepare",
        )
        event = append_event(
            ledger,
            "attempt_prepared",
            {
                "attemptTask": attempt_task,
                "attemptNumber": attempt_number,
                "priorPushedHead": (
                    projection["head"]["eventHash"]
                    if projection["head"] is not None
                    else None
                ),
                "candidateIdentity": artifacts["candidateIdentity"],
                "precheckSha256": artifacts["precheckSha256"],
                "intendedManifestSha256": artifacts["manifestSha256"],
            },
            now=now,
        )
        return {"event": event, "attemptTask": attempt_task}

    return _transaction(state_path, ledger_path, mutation)


def record_attempt_pushed(
    state_path: Path,
    ledger_path: Path,
    *,
    attempt_task: str,
    commit: str,
    origin_commit: str,
    manifest_sha256: str,
    now: str | None = None,
) -> dict[str, Any]:
    def mutation(
        state: dict[str, Any],
        ledger: dict[str, Any],
        projection: dict[str, Any],
    ) -> dict[str, Any]:
        if projection["phase"] != "prepared":
            raise ScopedCompletionError("attempt has not been prepared")
        prepared = projection["prepared"]
        if attempt_task != prepared["attemptTask"]:
            raise ScopedCompletionError("pushed task differs from prepared attempt")
        task = _task_index(state).get(attempt_task)
        if task is None:
            raise ScopedCompletionError(f"completion attempt task is missing: {attempt_task}")
        checkpoint_base = _git_object(
            task.get("checkpointBaseCommit"),
            f"{attempt_task}.checkpointBaseCommit",
        )
        snapshot = _validate_checkpoint_commit(
            attempt_task,
            commit=commit,
            origin_commit=origin_commit,
            checkpoint_base=checkpoint_base,
        )
        repository = Path(snapshot["repository"])
        changed = sorted(
            item.replace("\\", "/")
            for item in _git_output(
                repository,
                "diff-tree",
                "--no-commit-id",
                "--name-only",
                "-r",
                snapshot["head"],
            ).splitlines()
            if item
        )
        if changed != [QUALIFICATION_MANIFEST_RELATIVE]:
            raise ScopedCompletionError(
                "completion attempt checkpoint must change only the canonical manifest"
            )
        artifacts = _validate_attempt_artifacts(
            attempt_task=attempt_task,
            attempt_number=prepared["attemptNumber"],
            candidate_identity=prepared["candidateIdentity"],
            precheck_sha256=prepared["precheckSha256"],
            manifest_sha256=manifest_sha256,
            checkpoint_base=checkpoint_base,
            phase="pushed",
        )
        event = append_event(
            ledger,
            "attempt_pushed",
            {
                "attemptTask": attempt_task,
                "attemptNumber": prepared["attemptNumber"],
                "commit": snapshot["head"],
                "originCommit": snapshot["origin"],
                "manifestSha256": artifacts["manifestSha256"],
                "preparedEventHash": prepared["eventHash"],
                "candidateIdentity": prepared["candidateIdentity"],
            },
            now=now,
        )
        return {"event": event, "headAttempt": ledger["completionState"]["headAttempt"]}

    return _transaction(state_path, ledger_path, mutation)


def block_reconciliation(
    state_path: Path,
    ledger_path: Path,
    *,
    reason: str,
    now: str | None = None,
) -> dict[str, Any]:
    if not reason.strip():
        raise ScopedCompletionError("reconciliation blocker reason is required")

    def mutation(
        _state: dict[str, Any],
        ledger: dict[str, Any],
        projection: dict[str, Any],
    ) -> dict[str, Any]:
        if projection["phase"] != "pushed":
            raise ScopedCompletionError("no pushed attempt can be reconciliation-blocked")
        head = projection["head"]
        event = append_event(
            ledger,
            "reconciliation_blocked",
            {
                "attemptTask": head["attemptTask"],
                "attemptNumber": head["attemptNumber"],
                "commit": head["commit"],
                "reason": reason.strip(),
            },
            now=now,
        )
        return {"event": event, "allocatedTask": None}

    return _transaction(state_path, ledger_path, mutation)


def resume_reconciliation(
    state_path: Path,
    ledger_path: Path,
    *,
    now: str | None = None,
) -> dict[str, Any]:
    def mutation(
        _state: dict[str, Any],
        ledger: dict[str, Any],
        projection: dict[str, Any],
    ) -> dict[str, Any]:
        if projection["phase"] != "reconciliation_blocked":
            raise ScopedCompletionError("reconciliation is not blocked")
        head = projection["head"]
        event = append_event(
            ledger,
            "reconciliation_resumed",
            {
                "attemptTask": head["attemptTask"],
                "attemptNumber": head["attemptNumber"],
                "commit": head["commit"],
            },
            now=now,
        )
        return {"event": event, "attemptTask": head["attemptTask"]}

    return _transaction(state_path, ledger_path, mutation)


def fail_attempt(
    state_path: Path,
    ledger_path: Path,
    *,
    requested_class: str,
    changed_paths: list[str] | None = None,
    completion_tool_allowlist: list[str] | None = None,
    locked_allowlist_sha256: str | None = None,
    frozen_allowlist_sha256: str | None = None,
    exclusions_proven: bool = False,
    affected_cases: list[str] | None = None,
    now: str | None = None,
) -> dict[str, Any]:
    classification = classify_failure(
        requested_class,
        changed_paths=changed_paths,
        completion_tool_allowlist=completion_tool_allowlist,
        locked_allowlist_sha256=locked_allowlist_sha256,
        frozen_allowlist_sha256=frozen_allowlist_sha256,
        exclusions_proven=exclusions_proven,
    )
    if classification["classification"] == FAILURE_CLASS_RECONCILIATION:
        raise ScopedCompletionError(
            "use reconciliation_blocked; it must not allocate a task ID"
        )
    cases = _normalize_cases(affected_cases)
    event_time = _timestamp(now)

    def mutation(
        state: dict[str, Any],
        ledger: dict[str, Any],
        projection: dict[str, Any],
    ) -> dict[str, Any]:
        if projection["phase"] != "pushed":
            raise ScopedCompletionError("no pushed attempt is awaiting postcheck")
        head = projection["head"]
        failed = append_event(
            ledger,
            "attempt_failed",
            {
                "attemptTask": head["attemptTask"],
                "attemptNumber": head["attemptNumber"],
                "commit": head["commit"],
                "failureClass": classification["classification"],
                "requestedClass": classification["requestedClass"],
                "candidateReset": classification["candidateReset"],
                "changedPaths": classification["changedPaths"],
                "allowlistSha256": classification["allowlistSha256"],
                "classificationReason": classification["reason"],
            },
            now=event_time,
        )
        repair_id = _allocate_id(state, ledger, repair=True)
        definition = _repair_definition(
            state,
            repair_id,
            classification,
            cases,
            checkpoint_base=head["commit"],
            invalidated_candidate=head.get("candidateIdentity"),
            now=event_time,
        )
        allocated = append_event(
            ledger,
            "repair_allocated",
            {
                "repairTask": repair_id,
                "failedAttemptTask": head["attemptTask"],
                "failedAttemptNumber": head["attemptNumber"],
                "candidateReset": classification["candidateReset"],
                "failureClass": classification["classification"],
                "taskDefinition": definition,
            },
            now=event_time,
        )
        return {
            "failedEvent": failed,
            "allocatedEvent": allocated,
            "repairTask": repair_id,
            "classification": classification,
        }

    return _transaction(state_path, ledger_path, mutation)


def record_repair_pushed(
    state_path: Path,
    ledger_path: Path,
    *,
    repair_task: str,
    commit: str,
    origin_commit: str,
    now: str | None = None,
) -> dict[str, Any]:
    event_time = _timestamp(now)

    def mutation(
        state: dict[str, Any],
        ledger: dict[str, Any],
        projection: dict[str, Any],
    ) -> dict[str, Any]:
        if projection["phase"] != "repair_allocated":
            raise ScopedCompletionError("no completion repair is awaiting push")
        if repair_task != projection["currentRepair"]:
            raise ScopedCompletionError("pushed repair is not the allocated repair")
        active = [
            task["id"] for task in state["tasks"] if task.get("status") == "in_progress"
        ]
        if active != [repair_task]:
            raise ScopedCompletionError("repair must be the sole in_progress task")
        task = _task_index(state).get(repair_task)
        if task is None:
            raise ScopedCompletionError(f"allocated repair task is missing: {repair_task}")
        snapshot = _validate_checkpoint_commit(
            repair_task,
            commit=commit,
            origin_commit=origin_commit,
            checkpoint_base=_git_object(
                task.get("checkpointBaseCommit"),
                f"{repair_task}.checkpointBaseCommit",
            ),
        )
        attempt_id = _allocate_id(state, ledger, repair=False)
        attempt_number = projection["head"]["attemptNumber"] + 1
        attempt_definition = _attempt_definition(
            state,
            ledger,
            attempt_id,
            attempt_number,
            checkpoint_base=snapshot["head"],
            now=event_time,
        )
        event = append_event(
            ledger,
            "repair_pushed",
            {
                "repairTask": repair_task,
                "commit": snapshot["head"],
                "originCommit": snapshot["origin"],
                "nextAttemptTask": attempt_id,
                "nextAttemptNumber": attempt_number,
                "attemptDefinition": attempt_definition,
            },
            now=event_time,
        )
        return {
            "event": event,
            "repairTask": repair_task,
            "nextAttemptTask": attempt_id,
            "nextAttemptNumber": attempt_number,
        }

    return _transaction(state_path, ledger_path, mutation)


def pass_attempt(
    state_path: Path,
    ledger_path: Path,
    *,
    now: str | None = None,
) -> dict[str, Any]:
    event_time = _timestamp(now)

    def mutation(
        state: dict[str, Any],
        ledger: dict[str, Any],
        projection: dict[str, Any],
    ) -> dict[str, Any]:
        if projection["phase"] != "pushed":
            raise ScopedCompletionError("no pushed attempt is awaiting postcheck")
        _validate_postcheck_artifact(state, projection)
        head = projection["head"]
        passed = append_event(
            ledger,
            "attempt_passed",
            {
                "attemptTask": head["attemptTask"],
                "attemptNumber": head["attemptNumber"],
                "commit": head["commit"],
            },
            now=event_time,
        )
        completed = append_event(
            ledger,
            "coordinator_completed",
            {
                "coordinatorTask": COORDINATOR_TASK,
                "headEventHash": head["eventHash"],
            },
            now=event_time,
        )
        return {"passedEvent": passed, "completedEvent": completed}

    return _transaction(state_path, ledger_path, mutation)


def recover(
    state_path: Path,
    ledger_path: Path,
    *,
    observed_task: str | None = None,
    repository_head: str | None = None,
    origin_head: str | None = None,
    manifest_sha256: str | None = None,
    resume_blocked: bool = False,
    now: str | None = None,
) -> dict[str, Any]:
    """Replay valid events and reconcile explicitly observed pushed boundaries."""

    library = task_state_library()
    event_time = _timestamp(now)
    with _completion_locks(Path(state_path), Path(ledger_path)):
        contract = _validate_contract(Path(state_path))
        state = contract["state"]
        ledger = load_ledger(Path(ledger_path))
        projection = validate_ledger(ledger, strict_head=False)

        supplied_heads = repository_head is not None or origin_head is not None
        snapshot = _git_snapshot(require_clean=True) if supplied_heads else None
        normalized_head = snapshot["head"] if snapshot is not None else None
        normalized_origin = snapshot["origin"] if snapshot is not None else None
        if supplied_heads:
            if repository_head is None or origin_head is None:
                raise ScopedCompletionError(
                    "recovery requires both Repository HEAD and origin/dev"
                )
            if (
                _git_object(repository_head, "Repository HEAD") != normalized_head
                or _git_object(origin_head, "origin/dev") != normalized_origin
                or normalized_head != normalized_origin
            ):
                raise ScopedCompletionError(
                    "recovery arguments differ from actual HEAD/origin/dev"
                )

        recovered_event: dict[str, Any] | None = None
        if projection["phase"] == "prepared" and supplied_heads:
            prepared = projection["prepared"]
            if observed_task != prepared["attemptTask"]:
                raise ScopedCompletionError(
                    "recovery observed task does not match the prepared attempt"
                )
            if manifest_sha256 is None:
                raise ScopedCompletionError(
                    "attempt push recovery requires the manifest digest"
                )
            task = _task_index(state).get(prepared["attemptTask"])
            if task is None:
                raise ScopedCompletionError("prepared attempt task is missing")
            base = _git_object(
                task.get("checkpointBaseCommit"),
                f"{prepared['attemptTask']}.checkpointBaseCommit",
            )
            verified = _validate_checkpoint_commit(
                prepared["attemptTask"],
                commit=normalized_head,
                origin_commit=normalized_origin,
                checkpoint_base=base,
            )
            repository = Path(verified["repository"])
            changed = sorted(
                item.replace("\\", "/")
                for item in _git_output(
                    repository,
                    "diff-tree",
                    "--no-commit-id",
                    "--name-only",
                    "-r",
                    verified["head"],
                ).splitlines()
                if item
            )
            if changed != [QUALIFICATION_MANIFEST_RELATIVE]:
                raise ScopedCompletionError(
                    "recovered attempt checkpoint changed a non-manifest path"
                )
            artifacts = _validate_attempt_artifacts(
                attempt_task=prepared["attemptTask"],
                attempt_number=prepared["attemptNumber"],
                candidate_identity=prepared["candidateIdentity"],
                precheck_sha256=prepared["precheckSha256"],
                manifest_sha256=manifest_sha256,
                checkpoint_base=base,
                phase="pushed",
            )
            recovered_event = append_event(
                ledger,
                "attempt_pushed",
                {
                    "attemptTask": prepared["attemptTask"],
                    "attemptNumber": prepared["attemptNumber"],
                    "commit": verified["head"],
                    "originCommit": verified["origin"],
                    "manifestSha256": artifacts["manifestSha256"],
                    "preparedEventHash": prepared["eventHash"],
                    "candidateIdentity": prepared["candidateIdentity"],
                },
                now=event_time,
            )
            projection = validate_ledger(ledger)
        elif projection["phase"] == "repair_allocated" and supplied_heads:
            repair_id = projection["currentRepair"]
            if observed_task != repair_id:
                raise ScopedCompletionError(
                    "recovery observed task does not match the allocated repair"
                )
            base = projection["definitions"][repair_id].get("checkpointBaseCommit")
            if normalized_head == base:
                raise ScopedCompletionError("repair recovery observed no new commit")
            verified = _validate_checkpoint_commit(
                repair_id,
                commit=normalized_head,
                origin_commit=normalized_origin,
                checkpoint_base=_git_object(base, f"{repair_id}.checkpointBaseCommit"),
            )
            attempt_id = _allocate_id(state, ledger, repair=False)
            attempt_number = projection["head"]["attemptNumber"] + 1
            attempt_definition = _attempt_definition(
                state,
                ledger,
                attempt_id,
                attempt_number,
                checkpoint_base=verified["head"],
                now=event_time,
            )
            recovered_event = append_event(
                ledger,
                "repair_pushed",
                {
                    "repairTask": repair_id,
                    "commit": verified["head"],
                    "originCommit": verified["origin"],
                    "nextAttemptTask": attempt_id,
                    "nextAttemptNumber": attempt_number,
                    "attemptDefinition": attempt_definition,
                },
                now=event_time,
            )
            projection = validate_ledger(ledger)
        elif supplied_heads and projection["head"] is not None:
            if (
                normalized_head != projection["head"]["commit"]
                or (
                    manifest_sha256 is not None
                    and _sha256(manifest_sha256, "observed manifest digest")
                    != projection["head"]["manifestSha256"]
                )
            ):
                raise ScopedCompletionError("observed Repository state is not ledger head")

        if (
            not supplied_heads
            and projection["phase"]
            in {"pushed", "reconciliation_blocked", "passed", "complete"}
            and projection["head"] is not None
        ):
            snapshot = _git_snapshot(require_clean=True)
            if (
                snapshot["head"] != projection["head"]["commit"]
                or snapshot["origin"] != projection["head"]["originCommit"]
            ):
                raise ScopedCompletionError(
                    "actual Repository state is not the immutable ledger head"
                )

        if projection["phase"] == "reconciliation_blocked" and resume_blocked:
            head = projection["head"]
            recovered_event = append_event(
                ledger,
                "reconciliation_resumed",
                {
                    "attemptTask": head["attemptTask"],
                    "attemptNumber": head["attemptNumber"],
                    "commit": head["commit"],
                },
                now=event_time,
            )
            projection = validate_ledger(ledger)

        if projection["phase"] == "passed":
            recovered_event = append_event(
                ledger,
                "coordinator_completed",
                {
                    "coordinatorTask": COORDINATOR_TASK,
                    "headEventHash": projection["head"]["eventHash"],
                },
                now=event_time,
            )
            projection = validate_ledger(ledger)

        _sync_ledger_head(ledger, projection)
        state = _reconcile_state(state, projection)
        validate_ledger(ledger)
        try:
            library.validate_state_against_graph_lock(
                state, contract["graph"], contract["license"]
            )
        except (TypeError, ValueError) as error:
            raise ScopedCompletionError(str(error)) from error
        library.atomic_write_json(Path(ledger_path), ledger)
        library.atomic_write_json(Path(state_path), state)
        return {
            "phase": projection["phase"],
            "headAttempt": projection["head"],
            "recoveredEvent": recovered_event,
        }


def validate_files(state_path: Path, ledger_path: Path) -> dict[str, Any]:
    library = task_state_library()
    with _completion_locks(Path(state_path), Path(ledger_path)):
        contract = _validate_contract(Path(state_path))
        state = contract["state"]
        ledger = load_ledger(Path(ledger_path))
        projection = validate_ledger(ledger)
        reconciled = _reconcile_state(state, projection)
        if reconciled != state:
            raise ScopedCompletionError(
                "task state does not match the immutable attempt ledger; run recover"
            )
        try:
            library.validate_state_against_graph_lock(
                reconciled, contract["graph"], contract["license"]
            )
        except (TypeError, ValueError) as error:
            raise ScopedCompletionError(str(error)) from error
        if projection["head"] is not None and projection["phase"] in {
            "pushed",
            "reconciliation_blocked",
            "passed",
            "complete",
        }:
            snapshot = _git_snapshot(require_clean=True)
            if (
                snapshot["head"] != projection["head"]["commit"]
                or snapshot["origin"] != projection["head"]["originCommit"]
            ):
                raise ScopedCompletionError(
                    "Repository HEAD/origin does not match the ledger head"
                )
        return {
            "valid": True,
            "phase": projection["phase"],
            "eventCount": len(ledger["events"]),
            "headAttempt": projection["head"],
        }


def _common_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE_PATH)
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER_PATH)
    return parser


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Manage the Windows-live-first scoped completion protocol."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    common = _common_parser()

    prepare = subparsers.add_parser("prepare-attempt", parents=[common])
    prepare.add_argument("--attempt-task", required=True)
    prepare.add_argument("--candidate-identity", required=True)
    prepare.add_argument("--precheck-sha256", required=True)
    prepare.add_argument("--manifest-sha256", required=True)

    pushed = subparsers.add_parser("record-attempt-pushed", parents=[common])
    pushed.add_argument("--attempt-task", required=True)
    pushed.add_argument("--commit", required=True)
    pushed.add_argument("--origin-commit", required=True)
    pushed.add_argument("--manifest-sha256", required=True)

    block = subparsers.add_parser("block-reconciliation", parents=[common])
    block.add_argument("--reason", required=True)
    subparsers.add_parser("resume-reconciliation", parents=[common])

    failed = subparsers.add_parser("fail-attempt", parents=[common])
    failed.add_argument("--failure-class", required=True)
    failed.add_argument("--changed-path", action="append", default=[])
    failed.add_argument("--allowlist-path", action="append", default=[])
    failed.add_argument("--locked-allowlist-sha256")
    failed.add_argument("--frozen-allowlist-sha256")
    failed.add_argument("--allowlist-exclusions-proven", action="store_true")
    failed.add_argument("--affected-case", action="append", default=[])

    repair = subparsers.add_parser("record-repair-pushed", parents=[common])
    repair.add_argument("--repair-task", required=True)
    repair.add_argument("--commit", required=True)
    repair.add_argument("--origin-commit", required=True)

    subparsers.add_parser("pass-attempt", parents=[common])

    recovery = subparsers.add_parser("recover", parents=[common])
    recovery.add_argument("--observed-task")
    recovery.add_argument("--repository-head")
    recovery.add_argument("--origin-head")
    recovery.add_argument("--manifest-sha256")
    recovery.add_argument("--resume-blocked", action="store_true")

    subparsers.add_parser("validate", parents=[common])
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    common = {"state_path": args.state, "ledger_path": args.ledger}
    if args.command == "prepare-attempt":
        result = prepare_attempt(
            **common,
            attempt_task=args.attempt_task,
            candidate_identity=args.candidate_identity,
            precheck_sha256=args.precheck_sha256,
            intended_manifest_sha256=args.manifest_sha256,
        )
    elif args.command == "record-attempt-pushed":
        result = record_attempt_pushed(
            **common,
            attempt_task=args.attempt_task,
            commit=args.commit,
            origin_commit=args.origin_commit,
            manifest_sha256=args.manifest_sha256,
        )
    elif args.command == "block-reconciliation":
        result = block_reconciliation(**common, reason=args.reason)
    elif args.command == "resume-reconciliation":
        result = resume_reconciliation(**common)
    elif args.command == "fail-attempt":
        result = fail_attempt(
            **common,
            requested_class=args.failure_class,
            changed_paths=args.changed_path,
            completion_tool_allowlist=args.allowlist_path,
            locked_allowlist_sha256=args.locked_allowlist_sha256,
            frozen_allowlist_sha256=args.frozen_allowlist_sha256,
            exclusions_proven=args.allowlist_exclusions_proven,
            affected_cases=args.affected_case or None,
        )
    elif args.command == "record-repair-pushed":
        result = record_repair_pushed(
            **common,
            repair_task=args.repair_task,
            commit=args.commit,
            origin_commit=args.origin_commit,
        )
    elif args.command == "pass-attempt":
        result = pass_attempt(**common)
    elif args.command == "recover":
        result = recover(
            **common,
            observed_task=args.observed_task,
            repository_head=args.repository_head,
            origin_head=args.origin_head,
            manifest_sha256=args.manifest_sha256,
            resume_blocked=args.resume_blocked,
        )
    else:
        result = validate_files(**common)
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ScopedCompletionError, ValueError) as error:
        print(f"scoped-completion failed: {error}", file=sys.stderr)
        raise SystemExit(1)
