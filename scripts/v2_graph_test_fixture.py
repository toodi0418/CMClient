#!/usr/bin/env python3
"""Small v2 graph documents for Repository workflow-tool tests."""

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from typing import Any


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
)


def canonical_sha256(value: object) -> str:
    payload = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _value(task: dict[str, Any], field: str) -> object:
    if field == "required":
        return task.get(field, True)
    if field in {"manualGate", "candidateReset"}:
        return task.get(field, False)
    return task.get(field)


def _definition(task: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": task["id"],
        **{field: copy.deepcopy(_value(task, field)) for field in LOCKED_TASK_FIELDS},
        "dependsOn": copy.deepcopy(task.get("dependsOn", [])),
    }


def write_v2_contract(
    state: dict[str, Any],
    *,
    state_path: Path,
    graph_lock_path: Path,
    license_path: Path,
    source_baseline: str,
    origin: str = "https://github.com/toodi0418/CMClient.git",
) -> tuple[dict[str, Any], dict[str, Any]]:
    tasks = state["tasks"]
    locked_tasks = [
        _definition(task)
        for task in tasks
        if isinstance(task.get("phase"), str)
        and task["phase"] >= "P13"
        and task.get("repairOf") is None
    ]
    if not locked_tasks:
        locked_tasks = [
            _definition(task) for task in tasks if task.get("repairOf") is None
        ]
    historical = []
    for task in tasks:
        supersession = task.get("supersession")
        if task.get("status") != "skipped" or not isinstance(supersession, dict):
            continue
        historical.append(
            {
                "graphVersion": 1,
                "old": task["id"],
                "new": copy.deepcopy(task["supersededBy"]),
                "reason": supersession["reason"],
            }
        )
    coverage = [
        {
            "legacyTask": item["old"],
            "v2Tasks": copy.deepcopy(item["new"]),
            "reason": f"v2 coverage for {item['old']}",
        }
        for item in historical
    ]
    owner_decision = {
        "status": "approved",
        "route": "GPL-3.0-only",
        "approvedAt": "2026-07-21",
        "approvalReference": "workflow fixture owner approval",
        "publicDevPushPermitted": True,
    }
    license_gate = {
        "evidencePath": "state/LICENSE_PROVENANCE.json",
        "ownerDecision": owner_decision,
        "requiredFields": [
            "schema",
            "status",
            "route",
            "exactSources",
            "sourceDigests",
            "licenses",
            "notices",
            "publicDevPushPermitted",
            "approvedAt",
            "approvalReference",
        ],
    }
    repository_identity = {
        "origin": origin,
        "branch": "dev",
        "sourceBaseline": source_baseline,
        "protectedBranch": "main",
    }
    callmesh = {
        "productionBaseUrl": "https://callmesh.tmmarc.org",
        "productionAuthority": "official-hosted-only",
        "clientDistribution": "open-source",
        "serverDistribution": "closed-source hosted service",
        "selfHosting": False,
        "productionEndpointOverride": False,
        "localMappingOverride": False,
        "mappingAuthority": "CallMesh-only",
        "testEndpointInjection": "tests only",
        "outagePolicy": "fail closed",
    }
    target_platforms = {"windows": {"supported": ["x86_64-pc-windows-msvc"]}}
    candidate_identity = {"runtimeCandidate": "exact"}
    completion_task = locked_tasks[-1]["id"]
    completion_checker = {"task": completion_task}
    repair_protocol = {"candidateEffect": "invalidate"}
    first_active = next(
        (
            index
            for index, task in enumerate(tasks)
            if task["id"].startswith("P13-")
        ),
        0,
    )
    completed_history = [
        task for task in tasks[:first_active] if task.get("status") == "done"
    ]
    imported_at = "2026-07-21T00:00:00+00:00"
    source_sha = "a" * 64
    graph_lock: dict[str, Any] = {
        "schema": "cmclient-unified-task-graph-lock/v2",
        "id": "unified-product",
        "version": 2,
        "source": "plans/unified-product/tasks.proposed.json",
        "sourceSha256": source_sha,
        "sourceBaseline": source_baseline,
        "branch": "dev",
        "completionTask": completion_task,
        "manualReleaseTask": completion_task,
        "importedAt": imported_at,
        "completedHistorySha256": canonical_sha256(completed_history),
        "firstActivePhase": "P13",
        "supersededTaskIds": [item["old"] for item in historical],
        "historicalSupersessions": historical,
        "v2CoverageMap": coverage,
        "licenseGate": license_gate,
        "repositoryIdentity": repository_identity,
        "targetPlatforms": target_platforms,
        "callMeshServiceModel": callmesh,
        "candidateIdentity": candidate_identity,
        "completionChecker": completion_checker,
        "repairProtocol": repair_protocol,
        "tasks": locked_tasks,
        "taskDefinitionCount": len(locked_tasks),
        "canonicalPayloadFields": list(GRAPH_PAYLOAD_FIELDS),
        "activeGraphFields": list(ACTIVE_GRAPH_FIELDS),
    }
    graph_lock["graphSha256"] = canonical_sha256(
        {field: graph_lock.get(field) for field in GRAPH_PAYLOAD_FIELDS}
    )
    state["activeGraph"] = {
        "id": graph_lock["id"],
        "version": graph_lock["version"],
        "source": graph_lock["source"],
        "sourceSha256": graph_lock["sourceSha256"],
        "sourceBaseline": graph_lock["sourceBaseline"],
        "branch": graph_lock["branch"],
        "completionTask": graph_lock["completionTask"],
        "manualReleaseTask": graph_lock["manualReleaseTask"],
        "importedAt": graph_lock["importedAt"],
        "completedHistorySha256": graph_lock["completedHistorySha256"],
        "supersededTaskIds": graph_lock["supersededTaskIds"],
        "historicalSupersessions": copy.deepcopy(historical),
        "v2CoverageMap": copy.deepcopy(coverage),
        "licenseGate": copy.deepcopy(license_gate),
        "targetPlatforms": copy.deepcopy(target_platforms),
        "callMeshServiceModel": copy.deepcopy(callmesh),
        "candidateIdentity": copy.deepcopy(candidate_identity),
        "completionChecker": copy.deepcopy(completion_checker),
        "repairProtocol": copy.deepcopy(repair_protocol),
    }
    license_provenance = {
        "schema": "cmclient-license-provenance/v1",
        **owner_decision,
        "exactSources": [{"name": "fixture", "source": "fixture.invalid"}],
        "sourceDigests": {"fixture": "b" * 64},
        "licenses": ["GPL-3.0-only"],
        "notices": ["NOTICE"],
    }
    for path, value in (
        (state_path, state),
        (graph_lock_path, graph_lock),
        (license_path, license_provenance),
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(value, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    return graph_lock, license_provenance
