#!/usr/bin/env python3
"""Recover workspace task state from one verified checkpoint commit."""

from __future__ import annotations

import argparse
import copy
import importlib.util
import json
import os
import re
import stat
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = Path(
    os.environ.get("CMCLIENT_WORKSPACE_ROOT", REPOSITORY_ROOT.parent.parent)
).resolve()
TASK_STATE_LIB = Path(__file__).with_name("task-state-lib.py")
_TASK_STATE_MODULE: ModuleType | None = None
TASK_ID_RE = re.compile(r"P[0-9]{2}-T[0-9]{2}[a-z]?")
SUBJECT_RE = re.compile(
    r"(?:feat|fix|refactor|test|docs|build|ci|chore|perf|security|release)"
    r"\([a-z0-9-]+\): \[(?P<task>P[0-9]{2}-T[0-9]{2}[a-z]?)\] "
    r"(?P<summary>\S.*)"
)
TASK_LINE_RE = re.compile(r"^Task: (?P<task>[^\r\n]+)$", re.MULTILINE)
VALIDATION_LINE_RE = re.compile(
    r"^Validation: (?P<validation>[^\r\n]+)$", re.MULTILINE
)
COMMIT_ROW_RE = re.compile(
    r"^\|\s*(?P<time>[^|]+?)\s*\|\s*"
    r"(?P<task>P[0-9]{2}-T[0-9]{2}[a-z]?)\s*\|\s*"
    r"`(?P<sha>[0-9a-fA-F]{7,40})`\s*\|\s*"
    r"(?P<subject>.*?)\s*\|\s*(?P<validation>[^|]+?)\s*\|\s*$"
)


class ReconcileError(RuntimeError):
    pass


@dataclass(frozen=True)
class CheckpointCommit:
    sha: str
    parents: tuple[str, ...]
    committed_at: str
    subject: str
    body: str


def task_state_library() -> ModuleType:
    global _TASK_STATE_MODULE
    if _TASK_STATE_MODULE is not None:
        return _TASK_STATE_MODULE
    spec = importlib.util.spec_from_file_location(
        "cmclient_reconcile_task_state_lib", TASK_STATE_LIB
    )
    if spec is None or spec.loader is None:
        raise ReconcileError(f"cannot load task-state library: {TASK_STATE_LIB}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    _TASK_STATE_MODULE = module
    return module


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Reconcile a task after its unique checkpoint commit was pushed."
    )
    parser.add_argument("task", help="Exact task ID, for example P13-T02")
    parser.add_argument(
        "--repo",
        type=Path,
        default=REPOSITORY_ROOT,
        help="Git Repository path",
    )
    parser.add_argument(
        "--state",
        type=Path,
        default=WORKSPACE_ROOT / "state/TASKS.json",
        help="Workspace task-state JSON path",
    )
    parser.add_argument(
        "--commits",
        type=Path,
        default=WORKSPACE_ROOT / "state/COMMITS.md",
        help="Workspace checkpoint log path",
    )
    parser.add_argument("--remote", default="origin", help="Git remote name")
    parser.add_argument("--branch", default="dev", help="Expected branch name")
    parser.add_argument(
        "--graph-lock",
        type=Path,
        default=Path(__file__).with_name("unified-task-graph-lock.json"),
    )
    parser.add_argument("--license-provenance", type=Path)
    parser.add_argument("--graph-upgrade-operation-id")
    parser.add_argument(
        "--push-local",
        "--push",
        dest="push_local",
        action="store_true",
        help="Push an exact one-commit fast-forward before reconciling",
    )
    parser.add_argument(
        "--no-fetch",
        action="store_true",
        help="Use the existing remote-tracking ref (primarily for isolated tests)",
    )
    parser.add_argument(
        "--defer-scoped-completion-terminal",
        action="store_true",
        help=(
            "Record the pushed completion checkpoint without terminalizing its "
            "task; scoped-completion.py owns the ledger transition"
        ),
    )
    parser.add_argument("--git", default="git", help="Git executable")
    return parser.parse_args()


def run_git(
    git_executable: str,
    repo: Path,
    *args: str,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        [git_executable, "-C", str(repo), *args],
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
        raise ReconcileError(f"git {' '.join(args)} failed: {detail}")
    return result


def git_output(git_executable: str, repo: Path, *args: str) -> str:
    return run_git(git_executable, repo, *args).stdout.strip()


def validate_identifiers(task: str, branch: str) -> None:
    if TASK_ID_RE.fullmatch(task) is None:
        raise ReconcileError(f"invalid task ID: {task!r}")
    if branch == "main":
        raise ReconcileError("refusing to reconcile or push main")
    if not branch or branch.startswith("-"):
        raise ReconcileError(f"invalid branch name: {branch!r}")


def validate_repo(
    git_executable: str,
    repo: Path,
    branch: str,
    require_clean: bool = True,
) -> None:
    worktree = run_git(
        git_executable,
        repo,
        "rev-parse",
        "--is-inside-work-tree",
        check=False,
    )
    if worktree.returncode != 0 or worktree.stdout.strip() != "true":
        raise ReconcileError(f"not a Git worktree: {repo}")
    current = git_output(git_executable, repo, "branch", "--show-current")
    if current == "main":
        raise ReconcileError("refusing to operate on main")
    if current != branch:
        raise ReconcileError(f"expected branch {branch!r}, found {current!r}")
    if require_clean:
        dirty = git_output(
            git_executable, repo, "status", "--porcelain", "--untracked-files=normal"
        )
        if dirty:
            raise ReconcileError("Repository is dirty; checkpoint recovery is unsafe")


def fetch_remote_branch(
    git_executable: str, repo: Path, remote: str, branch: str
) -> None:
    run_git(
        git_executable,
        repo,
        "fetch",
        "--quiet",
        "--no-tags",
        remote,
        f"refs/heads/{branch}:refs/remotes/{remote}/{branch}",
    )


def remote_ref(git_executable: str, repo: Path, remote: str, branch: str) -> str:
    ref = f"refs/remotes/{remote}/{branch}"
    result = run_git(git_executable, repo, "rev-parse", "--verify", ref, check=False)
    if result.returncode != 0:
        raise ReconcileError(f"missing remote-tracking branch {remote}/{branch}")
    return ref


def is_ancestor(git_executable: str, repo: Path, ancestor: str, tip: str) -> bool:
    result = run_git(
        git_executable,
        repo,
        "merge-base",
        "--is-ancestor",
        ancestor,
        tip,
        check=False,
    )
    if result.returncode not in (0, 1):
        detail = result.stderr.strip() or result.stdout.strip()
        raise ReconcileError(f"git ancestry check failed: {detail}")
    return result.returncode == 0


def branch_distance(
    git_executable: str, repo: Path, tracked_ref: str
) -> tuple[int, int]:
    value = git_output(
        git_executable,
        repo,
        "rev-list",
        "--left-right",
        "--count",
        f"{tracked_ref}...HEAD",
    )
    fields = value.split()
    if len(fields) != 2:
        raise ReconcileError(f"unexpected rev-list count: {value!r}")
    return int(fields[0]), int(fields[1])


def read_commit(
    git_executable: str, repo: Path, sha: str
) -> CheckpointCommit:
    payload = run_git(
        git_executable,
        repo,
        "show",
        "-s",
        "--format=%H%x00%P%x00%cI%x00%s%x00%b",
        sha,
    ).stdout
    fields = payload.rstrip("\n").split("\x00", 4)
    if len(fields) != 5:
        raise ReconcileError(f"cannot parse commit metadata for {sha}")
    return CheckpointCommit(
        sha=fields[0],
        parents=tuple(fields[1].split()),
        committed_at=fields[2],
        subject=fields[3],
        body=fields[4],
    )


def read_all_commits(
    git_executable: str, repo: Path
) -> list[CheckpointCommit]:
    payload = run_git(
        git_executable,
        repo,
        "log",
        "--all",
        "-z",
        "--format=%H%x00%P%x00%cI%x00%s%x00%b",
    ).stdout
    fields = payload.split("\x00")
    if fields and fields[-1] == "":
        fields.pop()
    if len(fields) % 5 != 0:
        raise ReconcileError("cannot parse structured Git history")
    return [
        CheckpointCommit(
            sha=fields[index],
            parents=tuple(fields[index + 1].split()),
            committed_at=fields[index + 2],
            subject=fields[index + 3],
            body=fields[index + 4],
        )
        for index in range(0, len(fields), 5)
    ]


def validate_checkpoint_message(commit: CheckpointCommit, task: str) -> None:
    subject = SUBJECT_RE.fullmatch(commit.subject)
    task_lines = TASK_LINE_RE.findall(commit.body)
    validation_lines = VALIDATION_LINE_RE.findall(commit.body)
    if subject is None or subject.group("task") != task:
        raise ReconcileError(
            f"commit {commit.sha} does not have the exact [{task}] checkpoint subject"
        )
    if task_lines != [task]:
        raise ReconcileError(
            f"commit {commit.sha} must contain exactly one 'Task: {task}' body line"
        )
    if validation_lines != ["passed"]:
        raise ReconcileError(
            f"commit {commit.sha} must contain exactly one 'Validation: passed' body line"
        )


def find_checkpoint_commit(
    git_executable: str, repo: Path, task: str
) -> CheckpointCommit:
    implicated: list[CheckpointCommit] = []
    for commit in read_all_commits(git_executable, repo):
        subject = SUBJECT_RE.fullmatch(commit.subject)
        subject_task = subject.group("task") if subject else None
        body_tasks = TASK_LINE_RE.findall(commit.body)
        if subject_task == task or task in body_tasks:
            implicated.append(commit)

    if not implicated:
        raise ReconcileError(f"no checkpoint commit found for exact task {task}")
    if len(implicated) != 1:
        values = ", ".join(commit.sha for commit in implicated)
        raise ReconcileError(f"task {task} has multiple checkpoint commits: {values}")
    validate_checkpoint_message(implicated[0], task)
    return implicated[0]


def ensure_remote_commit(
    git_executable: str,
    repo: Path,
    commit: CheckpointCommit,
    remote: str,
    branch: str,
    push_local: bool,
    no_fetch: bool,
) -> bool:
    if not no_fetch:
        fetch_remote_branch(git_executable, repo, remote, branch)
    tracked_ref = remote_ref(git_executable, repo, remote, branch)
    behind, ahead = branch_distance(git_executable, repo, tracked_ref)
    if behind and ahead:
        raise ReconcileError(
            f"local {branch} and {remote}/{branch} have diverged ({behind} behind, {ahead} ahead)"
        )

    if is_ancestor(git_executable, repo, commit.sha, tracked_ref):
        if behind:
            raise ReconcileError(
                f"local {branch} is behind {remote}/{branch} by {behind} commit(s); sync it before recovery"
            )
        if ahead:
            raise ReconcileError(
                f"local {branch} has {ahead} additional unpushed commit(s); refusing recovery"
            )
        return False

    head = git_output(git_executable, repo, "rev-parse", "HEAD")
    if behind != 0 or ahead != 1 or head != commit.sha:
        raise ReconcileError(
            "checkpoint is not on the remote branch and is not the sole local fast-forward commit"
        )
    if not is_ancestor(git_executable, repo, tracked_ref, commit.sha):
        raise ReconcileError("local checkpoint is not a fast-forward of the remote branch")
    if not push_local:
        raise ReconcileError(
            "checkpoint exists only as the sole local commit; rerun with --push-local to push the same SHA"
        )

    run_git(
        git_executable,
        repo,
        "push",
        remote,
        f"{commit.sha}:refs/heads/{branch}",
    )
    fetch_remote_branch(git_executable, repo, remote, branch)
    tracked_sha = git_output(git_executable, repo, "rev-parse", tracked_ref)
    if tracked_sha != commit.sha or not is_ancestor(
        git_executable, repo, commit.sha, tracked_ref
    ):
        raise ReconcileError(
            f"remote verification failed: expected {commit.sha}, found {tracked_sha}"
        )
    return True


def load_state(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ReconcileError(f"cannot read task state {path}: {error}") from error
    if not isinstance(value, dict) or not isinstance(value.get("tasks"), list):
        raise ReconcileError(f"invalid task state document: {path}")
    return value


def find_task(state: dict, task_id: str) -> dict:
    matches = [task for task in state["tasks"] if task.get("id") == task_id]
    if len(matches) != 1:
        raise ReconcileError(
            f"expected exactly one state entry for {task_id}, found {len(matches)}"
        )
    return matches[0]


def validate_state_graph(
    state: dict,
    target_id: str,
    graph_lock: dict | None = None,
    license_provenance: dict | None = None,
) -> dict:
    try:
        library = task_state_library()
        if graph_lock is None or license_provenance is None:
            by_id = library.validate_task_graph(state)
        else:
            by_id = library.validate_state_against_graph_lock(
                state, graph_lock, license_provenance
            )
    except (TypeError, ValueError) as error:
        raise ReconcileError(f"task graph invariant failed: {error}") from error

    target = by_id.get(target_id)
    if target is None:
        raise ReconcileError(f"expected exactly one state entry for {target_id}, found 0")
    unfinished = [
        dependency
        for dependency in target.get("dependsOn", [])
        if by_id[dependency].get("status") != "done"
    ]
    if unfinished:
        raise ReconcileError(
            f"task {target_id} dependencies are not done: {', '.join(unfinished)}"
        )
    return target


def escaped_markdown_cell(value: str) -> str:
    return value.replace("\\", "\\\\").replace("|", "\\|").replace("\n", " ")


def commit_log_update(
    current: str, task: str, commit: CheckpointCommit
) -> tuple[str, bool]:
    rows = []
    for line in current.splitlines():
        cells = line.split("|")
        names_task = len(cells) > 2 and cells[2].strip() == task
        match = COMMIT_ROW_RE.fullmatch(line)
        if names_task and match is None:
            raise ReconcileError(f"COMMITS.md contains a malformed row for {task}")
        if match and match.group("task") == task:
            rows.append(match)
    if len(rows) > 1:
        raise ReconcileError(f"COMMITS.md contains duplicate rows for {task}")
    expected_subject = escaped_markdown_cell(commit.subject)
    if rows:
        row = rows[0]
        logged_sha = row.group("sha").lower()
        if not commit.sha.lower().startswith(logged_sha):
            raise ReconcileError(
                f"COMMITS.md SHA mismatch for {task}: {logged_sha} != {commit.sha}"
            )
        if row.group("subject").strip() != expected_subject:
            raise ReconcileError(f"COMMITS.md subject mismatch for {task}")
        if row.group("validation").strip() != "passed":
            raise ReconcileError(f"COMMITS.md validation mismatch for {task}")
        return current, False

    if not current:
        current = (
            "# Checkpoint Commit Log\n\n"
            "| Time | Task | Commit | Subject | Verification |\n"
            "|---|---|---|---|---|\n"
        )
    elif not current.endswith("\n"):
        current += "\n"
    row = (
        f"| {commit.committed_at} | {task} | `{commit.sha[:12]}` | "
        f"{expected_subject} | passed |\n"
    )
    return current + row, True


def reconciled_state(
    state: dict,
    task_id: str,
    commit: CheckpointCommit,
    *,
    defer_scoped_completion_terminal: bool = False,
) -> tuple[dict, bool]:
    task = validate_state_graph(state, task_id)
    try:
        checkpoint_base = task_state_library().normalize_git_object(
            task.get("checkpointBaseCommit"),
            f"task {task_id}.checkpointBaseCommit",
        )
    except ValueError as error:
        raise ReconcileError(str(error)) from error
    if commit.parents != (checkpoint_base,):
        raise ReconcileError(
            f"checkpoint parent does not match {task_id}.checkpointBaseCommit: "
            f"{list(commit.parents)!r} != {checkpoint_base}"
        )
    status = task.get("status")
    recorded_sha = task.get("commit")
    if recorded_sha not in (None, commit.sha):
        raise ReconcileError(
            f"state SHA mismatch for {task_id}: {recorded_sha} != {commit.sha}"
        )
    if status == "done":
        if recorded_sha != commit.sha:
            raise ReconcileError(f"done task {task_id} is missing the exact checkpoint SHA")
        return state, False
    if status not in ("in_progress", "blocked"):
        raise ReconcileError(
            f"task {task_id} is {status!r}; only in_progress or blocked tasks can recover"
        )

    result = copy.deepcopy(state)
    updated = find_task(result, task_id)
    updated["commit"] = commit.sha
    if defer_scoped_completion_terminal:
        if task_id != "P18-T10" and task.get("completionProtocol") != (
            "cmclient-windows-scoped-completion-attempts/v1"
        ):
            raise ReconcileError(
                "deferred terminalization is reserved for scoped Windows completion tasks"
            )
        if status != "in_progress":
            raise ReconcileError(
                "a scoped completion checkpoint must be the sole in_progress task"
            )
        updated["checkpointPushedAt"] = commit.committed_at
        updated["completionStage"] = "ledger_reconciliation_pending"
    else:
        updated["status"] = "done"
        updated["completedAt"] = commit.committed_at
        for field in ("blockedAt", "blockedByRepair", "blockReason"):
            updated.pop(field, None)
    notes = updated.setdefault("notes", [])
    if not isinstance(notes, list):
        raise ReconcileError(f"task {task_id} notes must be an array")
    if commit.subject not in notes:
        notes.append(commit.subject)
    return result, True


def atomic_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    mode = None
    try:
        try:
            mode = stat.S_IMODE(path.stat().st_mode)
        except FileNotFoundError:
            pass
        with temporary.open("wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        if mode is not None:
            os.chmod(temporary, mode)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def reconcile(args: argparse.Namespace) -> dict[str, object]:
    validate_identifiers(args.task, args.branch)
    repo = args.repo.resolve()
    state_path = args.state.resolve()
    commits_path = args.commits.resolve()
    graph_lock_path = args.graph_lock.resolve()
    license_path = (
        args.license_provenance.resolve()
        if args.license_provenance is not None
        else state_path.with_name("LICENSE_PROVENANCE.json")
    )
    operation_id = args.graph_upgrade_operation_id or os.environ.get(
        "CMCLIENT_GRAPH_UPGRADE_OPERATION_ID"
    )
    validate_repo(args.git, repo, args.branch)
    if not args.no_fetch:
        fetch_remote_branch(args.git, repo, args.remote, args.branch)
    commit = find_checkpoint_commit(args.git, repo, args.task)
    library = task_state_library()
    with library.state_lock(state_path):
        library.validate_upgrade_journal_guard(state_path, operation_id)
        state = load_state(state_path)
        graph_lock = library._load_document(
            graph_lock_path, "unified task graph lock"
        )
        license_provenance = library._load_document(
            license_path, "license provenance"
        )
        validate_state_graph(state, args.task, graph_lock, license_provenance)
        expected_state_digest = library.canonical_sha256(state)
        expected_graph_lock_digest = library.sha256_file(graph_lock_path)
        expected_license_digest = library.sha256_file(license_path)

    pushed = ensure_remote_commit(
        args.git,
        repo,
        commit,
        args.remote,
        args.branch,
        args.push_local,
        True,
    )
    tracked_ref = remote_ref(args.git, repo, args.remote, args.branch)
    expected_head = git_output(args.git, repo, "rev-parse", "HEAD")
    expected_remote = git_output(args.git, repo, "rev-parse", tracked_ref)

    with library.state_lock(state_path):
        library.validate_upgrade_journal_guard(state_path, operation_id)
        validate_repo(args.git, repo, args.branch)
        current_head = git_output(args.git, repo, "rev-parse", "HEAD")
        current_remote = git_output(args.git, repo, "rev-parse", tracked_ref)
        if current_head != expected_head or current_remote != expected_remote:
            raise ReconcileError("Git refs changed during reconciliation")
        behind, ahead = branch_distance(args.git, repo, tracked_ref)
        if behind or ahead:
            raise ReconcileError(
                f"local {args.branch} and {args.remote}/{args.branch} are not synchronized"
            )
        if not is_ancestor(args.git, repo, commit.sha, tracked_ref):
            raise ReconcileError(
                f"checkpoint {commit.sha} is not an ancestor of {args.remote}/{args.branch}"
            )
        repeated = read_commit(args.git, repo, commit.sha)
        validate_checkpoint_message(repeated, args.task)
        if repeated.sha != commit.sha:
            raise ReconcileError("checkpoint identity changed during reconciliation")

        state = load_state(state_path)
        if library.canonical_sha256(state) != expected_state_digest:
            raise ReconcileError("task state changed during reconciliation")
        if library.sha256_file(graph_lock_path) != expected_graph_lock_digest:
            raise ReconcileError("graph lock changed during reconciliation")
        if library.sha256_file(license_path) != expected_license_digest:
            raise ReconcileError("license provenance changed during reconciliation")
        graph_lock = library._load_document(
            graph_lock_path, "unified task graph lock"
        )
        license_provenance = library._load_document(
            license_path, "license provenance"
        )
        validate_state_graph(state, args.task, graph_lock, license_provenance)
        new_state, state_changed = reconciled_state(
            state,
            args.task,
            commit,
            defer_scoped_completion_terminal=args.defer_scoped_completion_terminal,
        )
        library.validate_state_against_graph_lock(
            new_state, graph_lock, license_provenance
        )
        current_log = commits_path.read_text(encoding="utf-8") if commits_path.exists() else ""
        new_log, log_changed = commit_log_update(current_log, args.task, commit)

        # Write the idempotent ledger first. A crash before the atomic state replace
        # is repaired by the next run without creating another row or commit.
        if log_changed:
            atomic_write(commits_path, new_log.encode("utf-8"))
        if state_changed:
            library.atomic_write_json(state_path, new_state)

    if state_changed:
        action = "pushed-and-reconciled" if pushed else "reconciled"
    elif log_changed:
        action = "commit-log-repaired"
    else:
        action = "no-op"
    return {
        "action": action,
        "task": args.task,
        "commit": commit.sha,
        "remote": f"{args.remote}/{args.branch}",
    }


def main() -> int:
    args = parse_args()
    result = reconcile(args)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ReconcileError, ValueError) as error:
        print(f"reconcile failed: {error}", file=sys.stderr)
        raise SystemExit(1)
