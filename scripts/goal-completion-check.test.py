#!/usr/bin/env python3
"""Regression tests for goal-completion-check.py using disposable Git repos."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path
from typing import Any

from v2_graph_test_fixture import GRAPH_PAYLOAD_FIELDS, write_v2_contract


SCRIPT = Path(__file__).with_name("goal-completion-check.py")


def load_checker_module() -> Any:
    spec = importlib.util.spec_from_file_location("goal_completion_check", SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load goal completion checker")
    module = importlib.util.module_from_spec(spec)
    previous = sys.dont_write_bytecode
    sys.dont_write_bytecode = True
    try:
        spec.loader.exec_module(module)
    finally:
        sys.dont_write_bytecode = previous
    return module


CHECKER = load_checker_module()
REQUIRED_ARTIFACTS = [
    ("windows-x86_64-setup", "setup", "windows/x86_64", "CMClient-Setup.exe"),
    ("macos-universal-dmg", "dmg", "macos/universal", "CMClient.dmg"),
    ("linux-x86_64-appimage", "appimage", "linux/x86_64", "CMClient-x86_64.AppImage"),
    ("linux-aarch64-appimage", "appimage", "linux/aarch64", "CMClient-aarch64.AppImage"),
    ("docker-compose", "compose", "linux/multi", "compose.yaml"),
]
REQUIRED_IMAGES = [
    ("cmclient-oci-index", "oci-index", "linux/multi"),
    ("cmclient-oci-amd64", "oci-image", "linux/amd64"),
    ("cmclient-oci-arm64", "oci-image", "linux/arm64"),
]
REQUIRED_SUPPORT = [
    ("checksums", "checksums", "all", "checksums.sha256"),
    ("sbom", "sbom", "all", "sbom.spdx.json"),
    ("provenance", "provenance", "all", "provenance.json"),
    ("update-manifest", "update-manifest", "native", "update-manifest.json"),
]
REQUIRED_CASES = (
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
REQUIRED_SUBCASES = {
    "SUPPLY_CHAIN": ["CHECKSUMS", "SBOM", "PROVENANCE", "UPDATE_MANIFEST"],
    "DOCKER_MATRIX": [
        "DOCKER_COMPOSE_E2E",
        "DOCKER_AMD64_CANDIDATE",
        "DOCKER_ARM64_CANDIDATE",
        "DOCKER_UPDATE_ROLLBACK",
    ],
    "LIVE_DATA": [
        "MESHTASTIC_TCP_PASSIVE",
        "CALLMESH_PROVISION",
        "APRS_IS_VERIFIED",
    ],
    "CLIENTS": [
        "PROXY_MULTI_CLIENT",
        "MANAGEMENT_WEB",
        "GRAPHICAL_MODE",
        "COMMAND_MODE",
    ],
    "RECOVERY": ["PERSISTENCE", "BACKUP", "RESTORE", "UPDATE", "ROLLBACK", "RESET"],
    "CLEANUP": [
        "PROCESSES_CLOSED",
        "LISTENERS_CLOSED",
        "RAW_CAMPAIGN_REMOVED",
        "REPOSITORY_CLEAN",
    ],
    "LIVE_SOAK_24H": [
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
    ],
}
PACKAGE_DEFERRALS = [
    "WINDOWS_11_V3",
    "MACOS_INTEL_V3",
    "MACOS_APPLE_SILICON_V3",
    "LINUX_X86_64_V3",
    "LINUX_AARCH64_V3",
]
DOCKER_DEFERRALS = ["DOCKER_AMD64_V3", "DOCKER_ARM64_V3"]
ALL_DEFERRALS = PACKAGE_DEFERRALS + DOCKER_DEFERRALS + [
    "PRODUCTION_SIGNING",
    "MAIN_PROMOTION",
    "TAG_PUBLICATION",
]


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def canonical_sha256(value: object) -> str:
    payload = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


class CompletionFixture:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.repo = root / "repo"
        self.origin = root / "origin.git"
        self.state_path = root / "TASKS.json"
        self.campaign_path = root / "CAMPAIGN.json"
        self.candidate_path = root / "CANDIDATE.json"
        self.evidence_path = root / "EVIDENCE.json"
        self.graph_lock_path = root / "unified-task-graph-lock.json"
        self.license_provenance_path = root / "LICENSE_PROVENANCE.json"
        self.precheck_path = root / "GOAL_PRECHECK.json"
        self.raw_campaign_root = root / "raw-campaign"
        subprocess.run(
            ["git", "init", "--bare", str(self.origin)],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
        )
        self.repo.mkdir()
        self.git("init", "-b", "dev")
        self.git("config", "user.name", "Completion Test")
        self.git("config", "user.email", "completion@example.invalid")
        (self.repo / "app.txt").write_text("fixture baseline\n", encoding="utf-8")
        self.git("add", "app.txt")
        self.git("commit", "-m", "fixture baseline")

        self.git("checkout", "-b", "historical-side")
        (self.repo / "historical-side.txt").write_text(
            "historical merge input\n", encoding="utf-8"
        )
        self.git("add", "historical-side.txt")
        self.git("commit", "-m", "fixture historical side")
        self.git("checkout", "dev")
        (self.repo / "app.txt").write_text("completed history\n", encoding="utf-8")
        self.git("add", "app.txt")
        self.git("commit", "-m", "fixture historical mainline")
        self.git(
            "merge",
            "--no-ff",
            "historical-side",
            "-m",
            "docs(history): [P12-T08] immutable completed history",
            "-m",
            "Task: P12-T08\nValidation: passed",
        )
        self.history_commit = self.git("rev-parse", "HEAD")
        self.git("branch", "-D", "historical-side")

        (self.repo / "app.txt").write_text("P13 replacement\n", encoding="utf-8")
        self.git("add", "app.txt")
        self.git(
            "commit",
            "-m",
            "security(secrets): [P13-T05] establish replacement",
            "-m",
            "Task: P13-T05\nValidation: passed",
        )
        self.p13_commit = self.git("rev-parse", "HEAD")

        (self.repo / "app.txt").write_text("candidate source\n", encoding="utf-8")
        self.git("add", "app.txt")
        self.git(
            "commit",
            "-m",
            "release(candidate): [P17-T06] freeze candidate source",
            "-m",
            "Task: P17-T06\nValidation: passed",
        )
        self.source_commit = self.git("rev-parse", "HEAD")
        self.source_tree = self.git("rev-parse", "HEAD^{tree}")

        (self.repo / "docs").mkdir()
        (self.repo / "docs" / "completion.md").write_text(
            "completion evidence\n", encoding="utf-8"
        )
        self.git("add", "docs/completion.md")
        self.git(
            "commit",
            "-m",
            "release(completion): [P17-T07] pass completion gate",
            "-m",
            "Task: P17-T07\nValidation: passed",
        )
        self.completion_commit = self.git("rev-parse", "HEAD")
        self.git("remote", "add", "origin", str(self.origin))
        self.git("push", "--set-upstream", "origin", "dev")

        self.candidate: dict[str, Any] = {
            "schemaVersion": 1,
            "schema": "cmclient-unified-candidate/v1",
            "candidateId": "candidate-final",
            "campaignId": "campaign-final",
            "createdAt": "2026-07-20T12:00:00Z",
            "sourceFrozenAt": "2026-07-20T12:00:00Z",
            "runtimeCandidate": {
                "id": "windows-x86_64-live-runtime",
                "target": "windows/x86_64",
                "fileName": "cmclient.exe",
                "sourceCommit": self.source_commit,
                "sourceTree": self.source_tree,
                "sha256": "1" * 64,
                "sizeBytes": 123456,
            },
            "distributionCandidate": {
                "sourceCommit": self.source_commit,
                "sourceTree": self.source_tree,
                "artifacts": [
                    {
                        "id": item_id,
                        "kind": kind,
                        "target": target,
                        "fileName": file_name,
                        "sha256": f"{index + 2:064x}",
                        "sizeBytes": 1000 + index,
                    }
                    for index, (item_id, kind, target, file_name) in enumerate(
                        REQUIRED_ARTIFACTS
                    )
                ],
                "images": [
                    {
                        "id": item_id,
                        "kind": kind,
                        "target": target,
                        "digest": f"sha256:{index + 20:064x}",
                        "sizeBytes": 2000 + index,
                    }
                    for index, (item_id, kind, target) in enumerate(REQUIRED_IMAGES)
                ],
                "supportArtifacts": [
                    {
                        "id": item_id,
                        "kind": kind,
                        "target": target,
                        "fileName": file_name,
                        "sha256": f"{index + 30:064x}",
                        "sizeBytes": 3000 + index,
                    }
                    for index, (item_id, kind, target, file_name) in enumerate(
                        REQUIRED_SUPPORT
                    )
                ],
            },
        }
        historical_task = {
            "id": "P12-T08",
            "phase": "P12",
            "title": "Immutable completed history",
            "status": "done",
            "required": True,
            "dependsOn": [],
            "commit": self.history_commit,
        }
        self.state: dict[str, Any] = {
            "schemaVersion": 2,
            "tasks": [
                historical_task,
                {
                    "id": "P12-T09",
                    "phase": "P12",
                    "status": "skipped",
                    "required": True,
                    "dependsOn": [],
                    "supersededBy": ["P13-T05"],
                    "supersession": {
                        "graphId": "unified-product",
                        "graphVersion": 1,
                        "reason": "rebaseline",
                    },
                },
                {
                    "id": "P13-T05",
                    "phase": "P13",
                    "title": "Establish replacement",
                    "kind": "security",
                    "scope": "secrets",
                    "candidateReset": True,
                    "status": "done",
                    "required": True,
                    "dependsOn": ["P12-T08"],
                    "checkpointBaseCommit": self.history_commit,
                    "commit": self.p13_commit,
                    "acceptance": ["Replacement passes verification."],
                },
                {
                    "id": "P17-T06",
                    "phase": "P17",
                    "title": "Freeze candidate",
                    "kind": "release",
                    "scope": "candidate",
                    "candidateReset": False,
                    "status": "done",
                    "required": True,
                    "dependsOn": ["P13-T05"],
                    "checkpointBaseCommit": self.p13_commit,
                    "commit": self.source_commit,
                    "acceptance": ["Candidate identity is frozen."],
                },
                {
                    "id": "P17-T07",
                    "phase": "P17",
                    "title": "Complete Goal",
                    "kind": "release",
                    "scope": "completion",
                    "candidateReset": False,
                    "status": "done",
                    "required": True,
                    "dependsOn": ["P17-T06"],
                    "checkpointBaseCommit": self.source_commit,
                    "commit": self.completion_commit,
                    "acceptance": ["Completion checker passes."],
                },
                {
                    "id": "P17-T08",
                    "phase": "P17",
                    "title": "Human production release",
                    "kind": "release",
                    "scope": "production",
                    "candidateReset": False,
                    "status": "pending",
                    "required": False,
                    "manualGate": True,
                    "dependsOn": ["P17-T07"],
                    "acceptance": ["Requires explicit human approval."],
                },
            ],
            "candidateInvalidations": [],
            "activeGraph": {
                "id": "unified-product",
                "version": 1,
                "source": "plans/unified-product/tasks.proposed.json",
                "sourceSha256": "a" * 64,
                "sourceBaseline": self.history_commit,
                "branch": "dev",
                "completionTask": "P17-T07",
                "manualReleaseTask": "P17-T08",
                "importedAt": "2026-07-20T11:33:12+00:00",
                "completionChecker": {"task": "P17-T07"},
                "targetPlatforms": {"windows": {"supported": ["x86_64"]}},
                "candidateIdentity": {"runtimeCandidate": "exact"},
                "repairProtocol": {"candidateEffect": "invalidate"},
                "supersededTaskIds": ["P12-T09"],
                "completedHistorySha256": canonical_sha256([historical_task]),
            },
        }
        filler_tasks = [
            {
                "id": f"P14-T{number:02d}",
                "phase": "P14",
                "title": f"Optional fixture definition {number}",
                "kind": "test",
                "scope": "fixture",
                "candidateReset": False,
                "status": "pending",
                "required": False,
                "manualGate": True,
                "dependsOn": ["P13-T05"],
                "acceptance": ["Fixture-only locked definition."],
            }
            for number in range(20, 73)
        ]
        p17_index = next(
            index
            for index, task in enumerate(self.state["tasks"])
            if task["id"] == "P17-T06"
        )
        self.state["tasks"][p17_index:p17_index] = filler_tasks
        self.graph_lock, self.license_provenance = write_v2_contract(
            self.state,
            state_path=self.state_path,
            graph_lock_path=self.graph_lock_path,
            license_path=self.license_provenance_path,
            source_baseline=self.history_commit,
            origin=str(self.origin),
        )
        self.graph_lock["completionTask"] = "P17-T07"
        self.graph_lock["manualReleaseTask"] = "P17-T08"
        self.graph_lock["candidateIdentity"] = {"runtimeCandidate": "exact"}
        self.graph_lock["completionChecker"] = {
            "task": "P17-T07",
            "preCheckpointArgs": ["--exclude-task", "P17-T07"],
            "postCheckpointRequired": True,
            "requiredEvidence": ["state/LICENSE_PROVENANCE.json"],
            "rule": "fixture completion rule",
        }
        for field in (
            "completionTask",
            "manualReleaseTask",
            "candidateIdentity",
            "completionChecker",
        ):
            self.state["activeGraph"][field] = json.loads(
                json.dumps(self.graph_lock[field])
            )
        self.refresh_graph_lock_digest()
        self.campaign: dict[str, Any] = {
            "schemaVersion": 1,
            "campaignId": "campaign-final",
            "branch": "dev",
            "status": "closed",
            "cleanupRequired": False,
            "secretsRecorded": False,
            "paths": {
                "physicalRoot": str(self.raw_campaign_root),
                "logicalRoot": str(root / "logical-root"),
                "verificationWorktree": str(self.raw_campaign_root / "src"),
                "childHome": str(self.raw_campaign_root / "home"),
                "temp": str(self.raw_campaign_root / "tmp"),
                "build": str(self.raw_campaign_root / "build"),
                "packages": str(self.raw_campaign_root / "packages"),
                "runtime": str(self.raw_campaign_root / "runtime"),
                "evidence": str(self.raw_campaign_root / "evidence"),
                "updateLab": str(self.raw_campaign_root / "update-lab"),
            },
            "environmentPolicy": {
                "parentGitProfilePreserved": True,
                "allGeneratedOutputBelowCampaign": True,
            },
            "externalGates": {
                "mainModificationApproved": False,
                "productionActionsApproved": False,
            },
        }
        self.evidence_files: dict[str, Path] = {}
        for case_id in REQUIRED_CASES:
            path = root / f"{case_id.lower()}.log"
            path.write_text(
                f"{case_id} passed for candidate-final\n", encoding="utf-8"
            )
            self.evidence_files[case_id] = path
        self.full_verify_path = self.evidence_files["FULL_VERIFY"]
        self.flush()
        self.write_precheck_attestation()

    @classmethod
    def clone_from(cls, template: "CompletionFixture", root: Path) -> "CompletionFixture":
        shutil.copytree(template.root, root, dirs_exist_ok=True)
        fixture = cls.__new__(cls)
        fixture.root = root
        fixture.repo = root / "repo"
        fixture.origin = root / "origin.git"
        fixture.state_path = root / "TASKS.json"
        fixture.campaign_path = root / "CAMPAIGN.json"
        fixture.candidate_path = root / "CANDIDATE.json"
        fixture.evidence_path = root / "EVIDENCE.json"
        fixture.graph_lock_path = root / "unified-task-graph-lock.json"
        fixture.license_provenance_path = root / "LICENSE_PROVENANCE.json"
        fixture.precheck_path = root / "GOAL_PRECHECK.json"
        fixture.raw_campaign_root = root / "raw-campaign"
        fixture.history_commit = template.history_commit
        fixture.p13_commit = template.p13_commit
        fixture.source_commit = template.source_commit
        fixture.source_tree = template.source_tree
        fixture.completion_commit = template.completion_commit
        fixture.state = json.loads(fixture.state_path.read_text(encoding="utf-8"))
        fixture.graph_lock = json.loads(
            fixture.graph_lock_path.read_text(encoding="utf-8")
        )
        fixture.license_provenance = json.loads(
            fixture.license_provenance_path.read_text(encoding="utf-8")
        )
        fixture.campaign = json.loads(
            fixture.campaign_path.read_text(encoding="utf-8")
        )
        fixture.campaign["paths"]["physicalRoot"] = str(fixture.raw_campaign_root)
        fixture.campaign["paths"]["logicalRoot"] = str(root / "logical-root")
        write_json(fixture.campaign_path, fixture.campaign)
        fixture.candidate = json.loads(
            fixture.candidate_path.read_text(encoding="utf-8")
        )
        fixture.evidence_files = {
            case_id: root / f"{case_id.lower()}.log" for case_id in REQUIRED_CASES
        }
        fixture.full_verify_path = fixture.evidence_files["FULL_VERIFY"]
        fixture.git("remote", "set-url", "origin", str(fixture.origin))
        fixture.graph_lock["repositoryIdentity"]["origin"] = str(fixture.origin)
        fixture.refresh_graph_lock_digest()
        write_json(fixture.graph_lock_path, fixture.graph_lock)
        fixture.write_precheck_attestation()
        return fixture

    def git(self, *args: str) -> str:
        result = subprocess.run(
            ["git", "-C", str(self.repo), *args],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
        )
        return result.stdout.strip()

    def push_dev(self) -> None:
        self.git("push", "origin", "dev")

    def add_repair_task(
        self,
        task_id: str,
        *,
        parent_id: str,
        checkpoint: bool = True,
        empty: bool = False,
    ) -> dict[str, Any]:
        parent = next(task for task in self.state["tasks"] if task["id"] == parent_id)
        checkpoint_base = self.git("rev-parse", "HEAD")
        if not empty:
            repair_path = self.repo / f"repair-{task_id.lower()}.txt"
            repair_path.write_text(
                f"{task_id} repair after {self.git('rev-parse', 'HEAD')}\n",
                encoding="utf-8",
            )
            self.git("add", repair_path.name)
        if checkpoint:
            arguments = [
                "commit",
                "-m",
                f"fix(repair): [{task_id}] verify repair checkpoint",
                "-m",
                f"Task: {task_id}\nValidation: passed",
            ]
        else:
            arguments = ["commit", "-m", f"fix: ordinary commit for {task_id}"]
        if empty:
            arguments.insert(1, "--allow-empty")
        self.git(*arguments)
        task = {
            "id": task_id,
            "phase": task_id.split("-", 1)[0],
            "title": f"Repair {parent_id}",
            "kind": "fix",
            "scope": "repair",
            "candidateReset": True,
            "status": "done",
            "required": True,
            "repairOf": parent_id,
            "affectedCases": ["FULL_VERIFY"],
            "dependsOn": list(parent.get("dependsOn", [])),
            "checkpointBaseCommit": checkpoint_base,
            "commit": self.git("rev-parse", "HEAD"),
            "acceptance": ["Repair checkpoint passes."],
        }
        manual_index = next(
            index
            for index, item in enumerate(self.state["tasks"])
            if item["id"] == "P17-T08"
        )
        self.state["tasks"].insert(manual_index, task)
        return task

    def refresh_graph_lock_digest(self) -> None:
        self.graph_lock["graphSha256"] = canonical_sha256(
            {
                field: self.graph_lock.get(field)
                for field in GRAPH_PAYLOAD_FIELDS
            }
        )

    def set_invalidation(
        self,
        *,
        invalidated_at: str,
        resolved_by: str | None,
        resolved_at: str | None,
    ) -> None:
        metadata = {
            "invalidatedAt": invalidated_at,
            "repairOf": "P12-T09",
            "runtimeCandidate": True,
            "distributionCandidate": True,
            "affectedCases": ["FULL_VERIFY"],
            "resolvedByCandidate": resolved_by,
            "resolvedAt": resolved_at,
        }
        repair = next(task for task in self.state["tasks"] if task["id"] == "P13-T05")
        repair.update(
            {
                "repairOf": "P12-T09",
                "candidateReset": True,
                "affectedCases": ["FULL_VERIFY"],
                "startedAt": invalidated_at,
                "candidateInvalidation": dict(metadata),
            }
        )
        self.state["candidateInvalidations"] = [
            {"repairTask": "P13-T05", **metadata}
        ]

    def flush(self) -> None:
        write_json(self.state_path, self.state)
        write_json(self.graph_lock_path, self.graph_lock)
        write_json(self.license_provenance_path, self.license_provenance)
        write_json(self.campaign_path, self.campaign)
        write_json(self.candidate_path, self.candidate)
        candidate_digest = sha256(self.candidate_path)
        distribution = self.candidate["distributionCandidate"]

        def evidence_subject(item: dict[str, Any], *, image: bool = False) -> dict[str, Any]:
            return {
                "id": item["id"],
                "digest": item["digest"] if image else item["sha256"],
                "sizeBytes": item["sizeBytes"],
                "target": item["target"],
                "sourceCommit": self.source_commit,
                "sourceTree": self.source_tree,
                "verificationMode": "local-byte",
                "verifiedBeforeCleanup": True,
            }

        public_subjects = [
            evidence_subject(item) for item in distribution["artifacts"]
        ] + [evidence_subject(item, image=True) for item in distribution["images"]]
        support_subjects = [
            evidence_subject(item) for item in distribution["supportArtifacts"]
        ]
        records: list[dict[str, Any]] = []
        for case_id in REQUIRED_CASES:
            path = self.evidence_files[case_id]
            record: dict[str, Any] = {
                "caseId": case_id,
                "status": "pass",
                "sanitized": True,
                "candidateId": self.candidate["candidateId"],
                "campaignId": self.campaign["campaignId"],
                "candidateSha256": candidate_digest,
                "sourceCommit": self.candidate["runtimeCandidate"]["sourceCommit"],
                "sourceTree": self.candidate["runtimeCandidate"]["sourceTree"],
                "executedAt": "2026-07-21T12:00:00Z",
                "path": path.name,
                "sha256": sha256(path),
            }
            if case_id in REQUIRED_SUBCASES:
                record["subcases"] = [
                    {"id": subcase, "status": "pass"}
                    for subcase in REQUIRED_SUBCASES[case_id]
                ]
            if case_id == "TESTABILITY_GATES":
                record["subcases"] = [
                    {"id": f"TG-{number:02d}", "status": "pass"}
                    for number in range(1, 15)
                ]
            elif case_id == "SUPPLY_CHAIN":
                record["subjects"] = support_subjects
            elif case_id == "PACKAGE_MATRIX":
                record["subjects"] = public_subjects
                record["verifiedLevels"] = ["V0", "V1", "V2"]
                record["v3Deferrals"] = list(PACKAGE_DEFERRALS)
            elif case_id == "DOCKER_MATRIX":
                record["subjects"] = [
                    "docker-compose",
                    "cmclient-oci-index",
                    "cmclient-oci-amd64",
                    "cmclient-oci-arm64",
                ]
                record["v3Deferrals"] = list(DOCKER_DEFERRALS)
            elif case_id == "LIVE_DATA":
                record.update(
                    {
                        "approved": True,
                        "rfTransmitted": False,
                        "radioMutated": False,
                    }
                )
            elif case_id == "LIVE_SOAK_24H":
                record.update(
                    {
                        "continuous": True,
                        "durationSeconds": 86400,
                        "startedAt": "2026-07-20T12:00:00Z",
                        "endedAt": "2026-07-21T12:00:00Z",
                    }
                )
            elif case_id == "DEFERRALS":
                record["deferrals"] = [
                    {
                        "id": item,
                        "status": "pending",
                        "manualGate": True,
                        "reason": "requires a real foreign host or explicit production approval",
                    }
                    for item in ALL_DEFERRALS
                ]
            records.append(record)
        invalidation_reruns = []
        for invalidation in self.state.get("candidateInvalidations", []):
            refs = []
            for case_id in invalidation.get("affectedCases", []):
                path = self.evidence_files[case_id]
                refs.append(
                    {
                        "caseId": case_id,
                        "path": path.name,
                        "sha256": sha256(path),
                    }
                )
            invalidation_reruns.append(
                {
                    "repairTask": invalidation["repairTask"],
                    "candidateId": self.candidate["candidateId"],
                    "candidateSha256": candidate_digest,
                    "affectedCases": list(invalidation.get("affectedCases", [])),
                    "status": "pass",
                    "sanitized": True,
                    "executedAt": "2026-07-21T12:00:00Z",
                    "evidenceRefs": refs,
                }
            )
        evidence = {
            "schemaVersion": 1,
            "schema": "cmclient-unified-evidence/v1",
            "invalidationRerunsSchema": "cmclient-invalidation-reruns/v1",
            "invalidationReruns": invalidation_reruns,
            "sanitized": True,
            "candidateId": self.candidate["candidateId"],
            "campaignId": self.campaign["campaignId"],
            "candidateSha256": candidate_digest,
            "sourceCommit": self.candidate["runtimeCandidate"]["sourceCommit"],
            "sourceTree": self.candidate["runtimeCandidate"]["sourceTree"],
            "runtimeSubject": {
                "id": self.candidate["runtimeCandidate"]["id"],
                "target": self.candidate["runtimeCandidate"]["target"],
                "fileName": self.candidate["runtimeCandidate"]["fileName"],
                "sha256": self.candidate["runtimeCandidate"]["sha256"],
                "sizeBytes": self.candidate["runtimeCandidate"]["sizeBytes"],
                "sourceCommit": self.source_commit,
                "sourceTree": self.source_tree,
                "verificationMode": "local-byte",
                "verifiedBeforeCleanup": True,
                "locallyExecuted": True,
            },
            "records": records,
        }
        write_json(self.evidence_path, evidence)

    def write_precheck_attestation(self) -> None:
        gate = CHECKER.Gate()
        bindings = CHECKER.precheck_file_bindings(
            self.candidate_path,
            self.evidence_path,
            self.graph_lock_path,
            self.license_provenance_path,
            gate,
        )
        if gate.errors or bindings is None:
            raise AssertionError(gate.errors)
        executed_at = self.git("show", "-s", "--format=%cI", self.source_commit)
        attestation = CHECKER.build_precheck_attestation(
            state=self.state,
            candidate=self.candidate,
            campaign_id=self.campaign["campaignId"],
            source_commit=self.source_commit,
            source_tree=self.source_tree,
            repo_head=self.source_commit,
            repository_identity=CHECKER.normalize_remote_identity(
                self.graph_lock["repositoryIdentity"]["origin"]
            ),
            file_bindings=bindings,
            executed_at=executed_at,
        )
        write_json(self.precheck_path, attestation)

    def run(self, *extra: str) -> subprocess.CompletedProcess[str]:
        environment = dict(os.environ)
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        return subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "--state",
                str(self.state_path),
                "--repo",
                str(self.repo),
                "--campaign",
                str(self.campaign_path),
                "--candidate",
                str(self.candidate_path),
                "--evidence",
                str(self.evidence_path),
                "--graph-lock",
                str(self.graph_lock_path),
                "--license-provenance",
                str(self.license_provenance_path),
                "--precheck-attestation",
                str(self.precheck_path),
                *extra,
            ],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            env=environment,
        )


class GoalCompletionCheckTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.template_temporary = tempfile.TemporaryDirectory()
        cls.template_fixture = CompletionFixture(Path(cls.template_temporary.name))

    @classmethod
    def tearDownClass(cls) -> None:
        cls.template_temporary.cleanup()

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.fixture = CompletionFixture.clone_from(
            self.template_fixture, Path(self.temporary.name)
        )

    def assert_passes(self, result: subprocess.CompletedProcess[str]) -> dict[str, Any]:
        payload = json.loads(result.stdout)
        self.assertEqual(result.returncode, 0, msg=result.stdout + result.stderr)
        self.assertEqual(payload["status"], "pass")
        self.assertEqual(payload["errors"], [])
        return payload

    def assert_fails(self, result: subprocess.CompletedProcess[str], fragment: str) -> dict[str, Any]:
        payload = json.loads(result.stdout)
        self.assertNotEqual(result.returncode, 0, msg=result.stdout + result.stderr)
        self.assertEqual(payload["status"], "fail")
        self.assertTrue(
            any(fragment.lower() in error.lower() for error in payload["errors"]),
            msg=f"missing {fragment!r} in {payload['errors']!r}",
        )
        return payload

    def assert_gate_fails(self, gate: Any, fragment: str) -> None:
        self.assertTrue(
            any(fragment.lower() in error.lower() for error in gate.errors),
            msg=f"missing {fragment!r} in {gate.errors!r}",
        )

    def assert_gate_passes(self, gate: Any) -> None:
        self.assertEqual(gate.errors, [])

    def task_gate(self, *excluded: str) -> Any:
        gate = CHECKER.Gate()
        CHECKER.check_task_state(
            self.fixture.state,
            list(excluded),
            self.fixture.repo,
            self.fixture.graph_lock,
            self.fixture.license_provenance,
            gate,
        )
        return gate

    def candidate_contract(self, gate: Any) -> tuple[Any, ...]:
        return CHECKER.candidate_source(self.fixture.candidate, gate)

    def evidence_gate(self) -> Any:
        gate = CHECKER.Gate()
        (
            source_commit,
            source_tree,
            _,
            subjects,
            runtime,
            freeze_at,
        ) = self.candidate_contract(gate)
        evidence = json.loads(self.fixture.evidence_path.read_text(encoding="utf-8"))
        CHECKER.check_evidence(
            evidence,
            self.fixture.evidence_path,
            sha256(self.fixture.candidate_path),
            source_commit,
            source_tree,
            self.fixture.candidate["candidateId"],
            self.fixture.campaign["campaignId"],
            subjects,
            runtime,
            self.fixture.state.get("candidateInvalidations", []),
            freeze_at,
            gate,
        )
        return gate

    def invalidation_gate(self) -> Any:
        gate = CHECKER.Gate()
        _, _, identities, _, _, freeze_at = self.candidate_contract(gate)
        CHECKER.check_candidate_invalidations(
            self.fixture.state,
            identities,
            sha256(self.fixture.candidate_path),
            freeze_at,
            gate,
        )
        return gate

    def test_complete_state_allows_optional_manual_pending(self) -> None:
        self.assert_passes(self.fixture.run())

    def test_pending_required_task_fails_even_when_no_task_would_be_ready(self) -> None:
        task = next(
            item for item in self.fixture.state["tasks"] if item["id"] == "P17-T06"
        )
        task["status"] = "blocked"
        task["commit"] = None
        self.fixture.flush()
        self.assert_gate_fails(
            self.task_gate(), "required active task is not done: P17-T06"
        )

    def test_done_optional_task_requires_a_real_checkpoint(self) -> None:
        optional = {
            "id": "P16-T12",
            "phase": "P16",
            "title": "Optional native qualification",
            "kind": "test",
            "scope": "qualification",
            "candidateReset": False,
            "status": "done",
            "required": False,
            "manualGate": True,
            "dependsOn": ["P17-T06"],
            "commit": None,
            "acceptance": ["Run on a real native host."],
        }
        release_index = next(
            index
            for index, item in enumerate(self.fixture.state["tasks"])
            if item["id"] == "P17-T08"
        )
        self.fixture.state["tasks"].insert(release_index, optional)
        locked = {
            key: value
            for key, value in optional.items()
            if key not in {"status", "commit"}
        }
        self.fixture.graph_lock["tasks"].insert(-1, locked)
        self.fixture.refresh_graph_lock_digest()
        self.fixture.flush()
        self.assert_gate_fails(
            self.task_gate(), "done optional task lacks a valid commit: P16-T12"
        )

    def test_cyclic_task_graph_fails(self) -> None:
        p13 = next(task for task in self.fixture.state["tasks"] if task["id"] == "P13-T05")
        p17 = next(task for task in self.fixture.state["tasks"] if task["id"] == "P17-T07")
        p13["dependsOn"] = ["P17-T07"]
        p17["dependsOn"] = ["P13-T05"]
        self.fixture.flush()
        self.assert_gate_fails(self.task_gate(), "task graph cycle")

    def test_required_tasks_cannot_share_a_commit(self) -> None:
        p13 = next(task for task in self.fixture.state["tasks"] if task["id"] == "P13-T05")
        p13["commit"] = self.fixture.source_commit
        self.fixture.flush()
        self.assert_gate_fails(
            self.task_gate(), "required active tasks share one commit"
        )

    def test_done_task_rejects_non_checkpoint_commit(self) -> None:
        self.fixture.add_repair_task(
            "P17-T09", parent_id="P17-T06", checkpoint=False
        )
        self.fixture.flush()
        self.assert_gate_fails(
            self.task_gate(), "exactly one implicated checkpoint commit; found 0"
        )

    def test_active_checkpoint_parent_must_match_recorded_base(self) -> None:
        task = next(
            item for item in self.fixture.state["tasks"] if item["id"] == "P13-T05"
        )
        task["checkpointBaseCommit"] = "f" * 40
        self.fixture.flush()
        self.assert_gate_fails(
            self.task_gate(),
            "active task checkpoint parent differs from recorded base: P13-T05",
        )

    def test_done_task_rejects_empty_checkpoint_commit(self) -> None:
        self.fixture.add_repair_task(
            "P17-T09", parent_id="P17-T06", empty=True
        )
        self.fixture.flush()
        self.assert_gate_fails(self.task_gate(), "checkpoint has an empty diff")

    def test_duplicate_implicated_checkpoint_commit_fails(self) -> None:
        duplicate = self.fixture.repo / "duplicate.txt"
        duplicate.write_text("duplicate P13 checkpoint\n", encoding="utf-8")
        self.fixture.git("add", duplicate.name)
        self.fixture.git(
            "commit",
            "-m",
            "security(secrets): [P13-T05] duplicate checkpoint",
            "-m",
            "Task: P13-T05\nValidation: passed",
        )
        self.assert_gate_fails(
            self.task_gate(), "exactly one implicated checkpoint commit; found 2"
        )

    def test_dependency_checkpoint_must_be_ancestor(self) -> None:
        repair = self.fixture.add_repair_task(
            "P17-T09", parent_id="P17-T07"
        )
        completion = next(
            task for task in self.fixture.state["tasks"] if task["id"] == "P17-T07"
        )
        completion["dependsOn"].append(repair["id"])
        self.fixture.flush()
        self.assert_gate_fails(
            self.task_gate(),
            "dependency checkpoint is not an ancestor: P17-T09 -> P17-T07",
        )

    def test_completed_history_mutation_fails(self) -> None:
        history = next(task for task in self.fixture.state["tasks"] if task["id"] == "P12-T08")
        history["title"] = "mutated terminal history"
        self.fixture.flush()
    def test_committed_graph_lock_contains_full_task_and_active_metadata(self) -> None:
        self.assertEqual(len(self.fixture.graph_lock["tasks"]), 57)
        self.assertEqual(self.fixture.graph_lock["taskDefinitionCount"], 57)
        for task in self.fixture.graph_lock["tasks"]:
            self.assertIsInstance(task.get("title"), str)
            self.assertIsInstance(task.get("acceptance"), list)
        for field in (
            "source",
            "importedAt",
            "historicalSupersessions",
            "v2CoverageMap",
            "licenseGate",
            "targetPlatforms",
            "callMeshServiceModel",
            "candidateIdentity",
            "completionChecker",
            "repairProtocol",
            "definitionAmendments",
        ):
            self.assertEqual(
                self.fixture.graph_lock[field],
                self.fixture.state["activeGraph"][field],
            )

    def test_v2_definition_count_and_contract_field_drift_fail(self) -> None:
        removed = self.fixture.graph_lock["tasks"].pop(1)
        self.fixture.state["tasks"] = [
            task for task in self.fixture.state["tasks"] if task["id"] != removed["id"]
        ]
        self.fixture.graph_lock["taskDefinitionCount"] = 56
        self.fixture.refresh_graph_lock_digest()
        self.assert_gate_fails(self.task_gate(), "taskDefinitionCount must be 57")

        self.fixture.graph_lock["taskDefinitionCount"] = 57
        self.fixture.graph_lock["tasks"].insert(1, removed)
        self.fixture.state["tasks"].insert(3, {
            **removed,
            "status": "pending",
            "required": False,
            "manualGate": True,
        })
        self.fixture.graph_lock["canonicalPayloadFields"] = ["tasks"]
        self.fixture.refresh_graph_lock_digest()
        self.assert_gate_fails(
            self.task_gate(), "canonicalPayloadFields differ from the v2 contract"
        )

    def test_definition_amendment_is_checked_by_completion_gate(self) -> None:
        self.fixture.graph_lock["definitionAmendments"] = [{"task": "P13-T06"}]
        self.fixture.state["activeGraph"]["definitionAmendments"] = json.loads(
            json.dumps(self.fixture.graph_lock["definitionAmendments"])
        )
        self.fixture.refresh_graph_lock_digest()
        self.fixture.flush()
        self.assert_gate_fails(
            self.task_gate(),
            "graph lock definitionAmendments must contain exactly two audited records",
        )

    def test_v2_coverage_drift_fails(self) -> None:
        coverage = self.fixture.graph_lock["v2CoverageMap"][0]
        coverage["v2Tasks"] = ["P99-T99"]
        self.fixture.state["activeGraph"]["v2CoverageMap"] = json.loads(
            json.dumps(self.fixture.graph_lock["v2CoverageMap"])
        )
        self.fixture.refresh_graph_lock_digest()
        self.assert_gate_fails(self.task_gate(), "v2CoverageMap[0] is invalid")

    def test_v2_historical_graph_version_drift_fails(self) -> None:
        historical = self.fixture.graph_lock["historicalSupersessions"][0]
        historical["graphVersion"] = 2
        self.fixture.state["activeGraph"]["historicalSupersessions"] = json.loads(
            json.dumps(self.fixture.graph_lock["historicalSupersessions"])
        )
        self.fixture.refresh_graph_lock_digest()
        self.assert_gate_fails(
            self.task_gate(), "must retain graphVersion 1"
        )

    def test_v2_callmesh_semantic_drift_fails(self) -> None:
        self.fixture.graph_lock["callMeshServiceModel"]["localMappingOverride"] = True
        self.fixture.state["activeGraph"]["callMeshServiceModel"][
            "localMappingOverride"
        ] = True
        self.fixture.refresh_graph_lock_digest()
        self.assert_gate_fails(self.task_gate(), "CallMesh service model is invalid")

    def test_license_provenance_drift_fails_completion_and_precheck_binding(self) -> None:
        self.fixture.license_provenance["publicDevPushPermitted"] = False
        self.fixture.flush()
        self.assert_fails(
            self.fixture.run(), "license provenance disagrees with the owner decision"
        )

    def test_locked_task_deletion_fails(self) -> None:
        self.fixture.state["tasks"] = [
            task
            for task in self.fixture.state["tasks"]
            if task["id"] != "P17-T08"
        ]
        self.fixture.flush()
        self.assert_gate_fails(self.task_gate(), "locked active task is missing")

    def test_locked_required_flag_mutation_fails(self) -> None:
        task = next(
            task for task in self.fixture.state["tasks"] if task["id"] == "P13-T05"
        )
        task["required"] = False
        self.fixture.flush()
        self.assert_gate_fails(self.task_gate(), "locked task field changed")

    def test_extra_supersession_fails(self) -> None:
        self.fixture.state["tasks"].insert(
            2,
            {
                "id": "P12-T10",
                "phase": "P12",
                "status": "skipped",
                "required": True,
                "dependsOn": [],
                "supersededBy": ["P13-T05"],
                "supersession": {
                    "graphId": "unified-product",
                    "reason": "unlocked supersession",
                },
            },
        )
        self.fixture.state["activeGraph"]["supersededTaskIds"].append("P12-T10")
        self.fixture.flush()
        self.assert_gate_fails(
            self.task_gate(),
            "activeGraph.supersededTaskIds does not match the committed graph lock",
        )

    def test_history_mutation_cannot_be_hidden_by_updating_active_hash(self) -> None:
        history = next(
            task for task in self.fixture.state["tasks"] if task["id"] == "P12-T08"
        )
        history["title"] = "mutated terminal history"
        self.fixture.state["activeGraph"]["completedHistorySha256"] = canonical_sha256(
            [history]
        )
        self.fixture.flush()
        self.assert_gate_fails(
            self.task_gate(),
            "activeGraph.completedHistorySha256 does not match the committed graph lock",
        )

    def test_superseded_replacement_must_exist(self) -> None:
        superseded = next(task for task in self.fixture.state["tasks"] if task["id"] == "P12-T09")
        superseded["supersededBy"] = ["P99-T99"]
        self.fixture.flush()
        self.assert_gate_fails(self.task_gate(), "invalid supersededBy target")

    def test_only_in_progress_completion_task_can_be_excluded(self) -> None:
        completion = next(task for task in self.fixture.state["tasks"] if task["id"] == "P17-T07")
        completion["status"] = "in_progress"
        completion["commit"] = None
        self.fixture.flush()
        with mock.patch.object(CHECKER, "check_task_checkpoints"):
            self.assert_gate_passes(self.task_gate("P17-T07"))
            self.assert_gate_fails(
                self.task_gate(), "required active task is not done: P17-T07"
            )

        completion["status"] = "done"
        completion["commit"] = self.fixture.completion_commit
        self.fixture.flush()
        self.assert_gate_fails(
            self.task_gate("P17-T07"), "must be in_progress"
        )

    def test_post_check_requires_bound_precheck_attestation(self) -> None:
        self.fixture.precheck_path.unlink()
        self.assert_fails(
            self.fixture.run(), "pre-check attestation file does not exist"
        )

        self.fixture.write_precheck_attestation()
        attestation = json.loads(
            self.fixture.precheck_path.read_text(encoding="utf-8")
        )
        attestation["candidateSha256"] = "f" * 64
        write_json(self.fixture.precheck_path, attestation)
        self.assert_fails(
            self.fixture.run(),
            "pre-check attestation binding differs: candidateSha256",
        )

    def test_excluded_precheck_must_write_machine_attestation(self) -> None:
        completion = next(
            task for task in self.fixture.state["tasks"] if task["id"] == "P17-T07"
        )
        completion["status"] = "in_progress"
        completion["commit"] = None
        completion["completedAt"] = None
        completion["notes"] = []
        self.fixture.flush()
        self.fixture.git("checkout", "-B", "dev", self.fixture.source_commit)
        subprocess.run(
            [
                "git",
                "--git-dir",
                str(self.fixture.origin),
                "update-ref",
                "refs/heads/dev",
                self.fixture.source_commit,
            ],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
        )
        self.fixture.git(
            "update-ref", "refs/remotes/origin/dev", self.fixture.source_commit
        )

        self.assert_fails(
            self.fixture.run("--exclude-task", "P17-T07"),
            "requires --write-precheck-attestation",
        )
        self.fixture.precheck_path.unlink(missing_ok=True)
        self.assert_passes(
            self.fixture.run(
                "--exclude-task",
                "P17-T07",
                "--write-precheck-attestation",
            )
        )
        attestation = json.loads(
            self.fixture.precheck_path.read_text(encoding="utf-8")
        )
        self.assertEqual(attestation["repoHead"], self.fixture.source_commit)
        self.assertEqual(attestation["excludedTasks"], ["P17-T07"])

    def test_dirty_repository_fails(self) -> None:
        source = self.fixture.source_commit

        def fake_git(_repo: Path, *args: str) -> tuple[bool, str]:
            if args[:2] == ("branch", "--show-current"):
                return True, "dev"
            if args[:2] == ("status", "--porcelain=v1"):
                return True, "?? untracked.txt"
            if args[:3] == ("remote", "get-url", "origin"):
                return True, "https://example.invalid/repo.git"
            if args[:3] == ("config", "--get-all", "remote.origin.fetch"):
                return True, "+refs/heads/*:refs/remotes/origin/*"
            if args and args[0] == "rev-parse":
                if "@{upstream}" in args:
                    return True, "origin/dev"
                return True, source
            if args[:2] == ("ls-remote", "--exit-code"):
                return True, f"{source}\trefs/heads/dev"
            if args[:2] == ("merge-base", "--is-ancestor"):
                return True, ""
            if args and args[0] == "diff":
                return True, ""
            raise AssertionError(f"unexpected git call: {args!r}")

        gate = CHECKER.Gate()
        with mock.patch.object(CHECKER, "git", side_effect=fake_git):
            CHECKER.check_repo(
                self.fixture.repo,
                "dev",
                self.fixture.source_commit,
                gate,
                "https://example.invalid/repo.git",
            )
        self.assert_gate_fails(gate, "Repository is not clean")

    def test_diverged_repository_fails(self) -> None:
        (self.fixture.repo / "docs").mkdir(exist_ok=True)
        (self.fixture.repo / "docs" / "note.md").write_text("post-freeze evidence note\n", encoding="utf-8")
        self.fixture.git("add", "docs/note.md")
        self.fixture.git("commit", "-m", "docs after freeze")
        self.assert_fails(self.fixture.run(), "dev diverges from origin/dev")

    def test_repository_without_configured_origin_fails(self) -> None:
        self.fixture.git("remote", "remove", "origin")
        self.assert_fails(self.fixture.run(), "origin remote is not configured")

    def test_repointed_origin_cannot_satisfy_completion(self) -> None:
        substitute = self.fixture.root / "substitute-origin.git"
        subprocess.run(
            ["git", "init", "--bare", str(substitute)],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
        )
        self.fixture.git("remote", "set-url", "origin", str(substitute))
        self.assert_fails(
            self.fixture.run(),
            "origin remote does not match the expected CMClient repository",
        )

    def test_stale_tracking_ref_fails_against_live_remote(self) -> None:
        other = self.fixture.root / "other"
        subprocess.run(
            ["git", "clone", "--branch", "dev", str(self.fixture.origin), str(other)],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
        )
        for key, value in (
            ("user.name", "Remote Test"),
            ("user.email", "remote@example.invalid"),
        ):
            subprocess.run(
                ["git", "-C", str(other), "config", key, value], check=True
            )
        (other / "docs").mkdir(exist_ok=True)
        (other / "docs" / "remote.md").write_text("remote advanced\n", encoding="utf-8")
        subprocess.run(["git", "-C", str(other), "add", "docs/remote.md"], check=True)
        subprocess.run(
            ["git", "-C", str(other), "commit", "-m", "remote advance"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        subprocess.run(
            ["git", "-C", str(other), "push", "origin", "dev"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.assert_fails(self.fixture.run(), "tracking ref is stale")

    def test_candidate_tree_mismatch_fails(self) -> None:
        self.fixture.candidate["runtimeCandidate"]["sourceTree"] = "f" * 40
        self.fixture.candidate["distributionCandidate"]["sourceTree"] = "f" * 40
        self.fixture.flush()
        gate = CHECKER.Gate()
        source_commit, source_tree, *_ = self.candidate_contract(gate)
        CHECKER.check_candidate_against_repo(
            self.fixture.repo, source_commit, source_tree, gate
        )
        self.assert_gate_fails(gate, "candidate sourceTree mismatch")

    def test_candidate_requires_every_exact_public_artifact(self) -> None:
        self.fixture.candidate["distributionCandidate"]["artifacts"].pop()
        self.fixture.flush()
        gate = CHECKER.Gate()
        self.candidate_contract(gate)
        self.assert_gate_fails(gate, "missing required public artifact")

    def test_candidate_rejects_malformed_oci_digest(self) -> None:
        self.fixture.candidate["distributionCandidate"]["images"][0]["digest"] = "bad"
        self.fixture.flush()
        gate = CHECKER.Gate()
        self.candidate_contract(gate)
        self.assert_gate_fails(gate, "digest must be sha256")

    def test_candidate_source_freeze_is_required_and_canonical(self) -> None:
        del self.fixture.candidate["sourceFrozenAt"]
        self.fixture.flush()
        gate = CHECKER.Gate()
        self.candidate_contract(gate)
        self.assert_gate_fails(gate, "candidate.sourceFrozenAt")

        self.fixture.candidate["sourceFrozenAt"] = "2026-07-20T12:00:00Z"
        self.fixture.candidate["runtimeCandidate"]["freezeAt"] = (
            "2026-07-20T11:59:59Z"
        )
        self.fixture.flush()
        gate = CHECKER.Gate()
        self.candidate_contract(gate)
        self.assert_gate_fails(gate, "conflicts with candidate.sourceFrozenAt")

    def test_cleanup_requires_closed_campaign_and_absent_root(self) -> None:
        self.fixture.campaign["cleanupRequired"] = True
        self.fixture.raw_campaign_root.mkdir()
        self.fixture.flush()
        gate = CHECKER.Gate()
        CHECKER.check_campaign(self.fixture.campaign, gate)
        self.assert_gate_fails(gate, "cleanupRequired must be false")
        self.assert_gate_fails(gate, "raw campaign root still exists")

    def test_cleanup_rejects_surviving_recorded_child_path(self) -> None:
        orphan_runtime = self.fixture.root / "orphan-runtime"
        orphan_runtime.mkdir()
        (orphan_runtime / "db.sqlite").write_text("residue\n", encoding="utf-8")
        self.fixture.campaign["paths"]["runtime"] = str(orphan_runtime)
        gate = CHECKER.Gate()
        CHECKER.check_campaign(self.fixture.campaign, gate)
        self.assert_gate_fails(gate, "campaign cleanup path still exists: runtime")

    def test_campaign_policy_and_external_gates_fail_closed(self) -> None:
        logical_root = Path(self.fixture.campaign["paths"]["logicalRoot"])
        logical_root.mkdir()
        self.fixture.campaign["secretsRecorded"] = None
        self.fixture.campaign["branch"] = "main"
        self.fixture.campaign["environmentPolicy"]["parentGitProfilePreserved"] = (
            False
        )
        self.fixture.campaign["externalGates"]["mainModificationApproved"] = True
        gate = CHECKER.Gate()
        CHECKER.check_campaign(self.fixture.campaign, gate)
        self.assert_gate_fails(gate, "campaign.secretsRecorded must be false")
        self.assert_gate_fails(gate, "campaign.branch must be dev")
        self.assert_gate_fails(gate, "campaign environment policy is incomplete")
        self.assert_gate_fails(
            gate, "campaign production/main approvals must remain false"
        )
        self.assert_gate_fails(gate, "logical campaign root is still mounted")

    def test_formal_manual_release_task_must_remain_pending(self) -> None:
        release = next(
            task for task in self.fixture.state["tasks"] if task["id"] == "P17-T08"
        )
        release["status"] = "done"
        self.fixture.flush()
        self.assert_gate_fails(
            self.task_gate(), "formal manual release task must exist and remain pending"
        )

    def test_post_freeze_docs_are_allowed_but_product_changes_fail(self) -> None:
        self.assertTrue(CHECKER.allowed_post_freeze_path("docs/evidence/audit.md"))
        self.assertTrue(CHECKER.allowed_post_freeze_path("README.md"))
        self.assertFalse(
            CHECKER.allowed_post_freeze_path("apps/gateway/evidence/claim.ts")
        )
        self.assertFalse(CHECKER.allowed_post_freeze_path("UNREVIEWED.md"))
        (self.fixture.repo / "docs").mkdir(exist_ok=True)
        (self.fixture.repo / "docs" / "audit.md").write_text("candidate audit\n", encoding="utf-8")
        self.fixture.git("add", "docs/audit.md")
        self.fixture.git("commit", "-m", "docs: candidate audit")
        self.fixture.push_dev()
        self.fixture.flush()
        self.assert_passes(self.fixture.run())

        (self.fixture.repo / "app.txt").write_text("changed after freeze\n", encoding="utf-8")
        self.fixture.git("add", "app.txt")
        self.fixture.git("commit", "-m", "bad product change")
        self.fixture.push_dev()
        self.fixture.flush()
        self.assert_fails(self.fixture.run(), "non-document/evidence path changed")

    def test_evidence_must_exist_be_exact_and_sanitized(self) -> None:
        evidence = json.loads(self.fixture.evidence_path.read_text(encoding="utf-8"))
        evidence["sanitized"] = False
        evidence["records"][0]["path"] = "missing.log"
        write_json(self.fixture.evidence_path, evidence)
        gate = self.evidence_gate()
        self.assert_gate_fails(gate, "evidence.sanitized must be true")
        self.assert_gate_fails(gate, "does not exist")

    def test_missing_required_evidence_case_fails(self) -> None:
        evidence = json.loads(self.fixture.evidence_path.read_text(encoding="utf-8"))
        evidence["records"] = [
            record for record in evidence["records"] if record["caseId"] != "RECOVERY"
        ]
        write_json(self.fixture.evidence_path, evidence)
        self.assert_gate_fails(
            self.evidence_gate(), "evidence missing required case: RECOVERY"
        )

    def test_recovery_evidence_cannot_omit_rollback_subcase(self) -> None:
        evidence = json.loads(self.fixture.evidence_path.read_text(encoding="utf-8"))
        recovery = next(
            record for record in evidence["records"] if record["caseId"] == "RECOVERY"
        )
        recovery["subcases"] = [
            subcase for subcase in recovery["subcases"] if subcase["id"] != "ROLLBACK"
        ]
        write_json(self.fixture.evidence_path, evidence)
        self.assert_gate_fails(
            self.evidence_gate(),
            "RECOVERY does not contain the exact required subcase set",
        )

    def test_live_soak_evidence_requires_exact_health_subcases(self) -> None:
        evidence = json.loads(self.fixture.evidence_path.read_text(encoding="utf-8"))
        soak = next(
            record
            for record in evidence["records"]
            if record["caseId"] == "LIVE_SOAK_24H"
        )
        soak["subcases"] = [
            subcase
            for subcase in soak["subcases"]
            if subcase["id"] != "RECOVERY_BUDGETS"
        ]
        write_json(self.fixture.evidence_path, evidence)
        self.assert_gate_fails(
            self.evidence_gate(),
            "LIVE_SOAK_24H does not contain the exact required subcase set",
        )

    def test_package_evidence_subject_must_match_candidate_digest(self) -> None:
        evidence = json.loads(self.fixture.evidence_path.read_text(encoding="utf-8"))
        package = next(
            record for record in evidence["records"] if record["caseId"] == "PACKAGE_MATRIX"
        )
        package["subjects"][0]["digest"] = "f" * 64
        write_json(self.fixture.evidence_path, evidence)
        self.assert_gate_fails(
            self.evidence_gate(), "digest does not match candidate"
        )

    def test_ci_metadata_subject_is_allowed_without_package_bytes(self) -> None:
        evidence = json.loads(self.fixture.evidence_path.read_text(encoding="utf-8"))
        package = next(
            record for record in evidence["records"] if record["caseId"] == "PACKAGE_MATRIX"
        )
        subject = next(
            item for item in package["subjects"] if item["id"] == "macos-universal-dmg"
        )
        subject.pop("verifiedBeforeCleanup")
        subject.update(
            {
                "verificationMode": "ci-metadata",
                "locallyExecuted": False,
                "ci": {
                    "runUrl": "https://github.com/toodi0418/CMClient/actions/runs/1",
                    "jobUrl": "https://github.com/toodi0418/CMClient/actions/runs/1/job/2",
                    "apiDigestSha256": subject["digest"],
                    "apiSizeBytes": subject["sizeBytes"],
                    "expiresAt": "2099-08-20T12:00:00Z",
                },
            }
        )
        write_json(self.fixture.evidence_path, evidence)
        self.assert_gate_passes(self.evidence_gate())

        subject["ci"]["apiDigestSha256"] = "a" * 64
        subject["ci"]["apiSizeBytes"] += 1
        write_json(self.fixture.evidence_path, evidence)
        gate = self.evidence_gate()
        self.assert_gate_fails(gate, "apiDigestSha256 does not match candidate")
        self.assert_gate_fails(gate, "apiSizeBytes does not match candidate")

        subject["ci"]["apiDigestSha256"] = subject["digest"]
        subject["ci"]["apiSizeBytes"] = subject["sizeBytes"]
        subject["ci"]["runUrl"] = "https://untrusted.invalid/actions/runs/1"
        subject["ci"]["expiresAt"] = "2000-01-01T00:00:00Z"
        write_json(self.fixture.evidence_path, evidence)
        gate = self.evidence_gate()
        self.assert_gate_fails(gate, "URLs must identify one toodi0418/CMClient")
        self.assert_gate_fails(gate, "CI metadata is expired")

    def test_secret_like_candidate_and_evidence_values_fail(self) -> None:
        self.fixture.candidate["apiKey"] = "not-allowed-secret-value"
        self.fixture.flush()
        gate = CHECKER.Gate()
        CHECKER.scan_secret_values(self.fixture.candidate, "candidate", gate)
        self.assert_gate_fails(gate, "secret-like key")

        del self.fixture.candidate["apiKey"]
        self.fixture.flush()
        evidence = json.loads(self.fixture.evidence_path.read_text(encoding="utf-8"))
        evidence["notes"] = "Authorization: Bearer abcdefghijklmnopqrstuvwxyz"
        write_json(self.fixture.evidence_path, evidence)
        gate = CHECKER.Gate()
        CHECKER.scan_secret_values(evidence, "evidence", gate)
        self.assert_gate_fails(gate, "secret-like value")

    def test_secret_like_state_and_campaign_values_fail(self) -> None:
        self.fixture.state["apiKey"] = "not-allowed-secret-value"
        self.fixture.campaign["authorization"] = "Bearer sensitive-value"
        gate = CHECKER.Gate()
        CHECKER.scan_secret_values(self.fixture.state, "task state", gate)
        CHECKER.scan_secret_values(self.fixture.campaign, "campaign", gate)
        self.assert_gate_fails(gate, "task state contains a secret-like key")
        self.assert_gate_fails(gate, "campaign contains a secret-like key")

    def test_unresolved_invalidation_fails_even_when_candidate_is_newer(self) -> None:
        self.fixture.set_invalidation(
            invalidated_at="2026-07-20T11:00:00Z",
            resolved_by=None,
            resolved_at=None,
        )
        self.fixture.flush()
        self.assert_gate_fails(
            self.invalidation_gate(), "candidate invalidation 0 is unresolved"
        )

    def test_invented_invalidation_case_cannot_bypass_retained_evidence(self) -> None:
        self.fixture.set_invalidation(
            invalidated_at="2026-07-20T11:00:00Z",
            resolved_by=None,
            resolved_at=None,
        )
        repair = next(
            task for task in self.fixture.state["tasks"] if task["id"] == "P13-T05"
        )
        repair["affectedCases"] = ["NOT_A_REAL_CASE"]
        repair["candidateInvalidation"]["affectedCases"] = ["NOT_A_REAL_CASE"]
        self.fixture.state["candidateInvalidations"][0]["affectedCases"] = [
            "NOT_A_REAL_CASE"
        ]
        self.assert_gate_fails(self.task_gate(), "contains unknown case IDs")

    def test_newer_invalidation_must_resolve_to_final_candidate(self) -> None:
        self.fixture.set_invalidation(
            invalidated_at="2026-07-20T13:00:00Z",
            resolved_by=f"sha256:{'9' * 64}",
            resolved_at="2026-07-20T14:00:00Z",
        )
        self.fixture.flush()
        self.assert_gate_fails(
            self.invalidation_gate(), "not resolved by the final candidate"
        )

        self.fixture.candidate["sourceFrozenAt"] = "2026-07-20T15:00:00Z"
        self.fixture.candidate["createdAt"] = "2026-07-20T15:00:00Z"
        self.fixture.flush()
        final_identity = f"sha256:{sha256(self.fixture.candidate_path)}"
        self.fixture.state["candidateInvalidations"][0]["resolvedByCandidate"] = final_identity
        repair = next(
            task for task in self.fixture.state["tasks"] if task["id"] == "P13-T05"
        )
        repair["candidateInvalidation"]["resolvedByCandidate"] = final_identity
        self.fixture.flush()
        self.assert_gate_passes(self.invalidation_gate())


if __name__ == "__main__":
    unittest.main(verbosity=2)
