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

P13_T05_ACCEPTANCE_PREFIX = (
    "Exact Tauri CLI/core/plugin, cargo-packager-updater and fixture dependency "
    "versions, lockfile hashes, sources and licenses are frozen before testing.",
    "A minimal independently built Tauri fixture uses "
    "createUpdaterArtifacts=v1Compatible so Tauri directly emits test-signed NSIS, "
    "app.tar.gz and AppImage-compatible updater payloads without "
    "conversion/repacking; a same-runner local HTTPS lab proves exact-format "
    "check/download/signature/install behavior.",
    "A clean Windows standard-user fixture proves currentUser NSIS, "
    "offline/per-user WebView2, PATH/autostart hooks, repair and upgrade without "
    "UAC before P14 work proceeds.",
    "A hidden official Tauri helper is the preferred route and is accepted only "
    "after proving exact headless AppHandle/check/download/install/relaunch, helper "
    "lifetime and replacement behavior. Public-preview cargo-packager-updater can "
    "be selected only with an explicit P13 owner risk acceptance and the same "
    "complete qualification; the final-product route cannot be deferred to "
    "P17-T08.",
)
P13_T05_OLD_GATEWAY_ACCEPTANCE = (
    "Gateway proves a fresh memory-only Agent bootstrap capability delivered through "
    "an inherited private pipe, never argv/env/disk/log, and an Agent-prebound inherited "
    "loopback listener. Agent strips/overwrites any client capability header, Gateway "
    "rejects direct/spoofed requests on every HTTP/SSE/health route, responses never "
    "reflect it, and child identity/port-race tests pass."
)
P13_T05_NEW_GATEWAY_ACCEPTANCE = (
    "Gateway proves a fresh memory-only Agent bootstrap capability delivered through "
    "an inherited private pipe, never argv/env/disk/log. After validating that bounded "
    "bootstrap frame, Gateway atomically binds 127.0.0.1:0 itself and reports the bound "
    "port, child PID and instance nonce only through the inherited private channel; "
    "fixed-port probing, release/rebind and unsupported Windows descriptor passing are "
    "forbidden. Agent strips/overwrites any client capability header, Gateway rejects "
    "direct/spoofed requests on every HTTP/SSE/health route, responses never reflect it, "
    "and child identity/port-takeover tests pass."
)
P13_T05_FINAL_ACCEPTANCE = (
    "Wrong key, bit flip, wrong target, oversize, timeout, downgrade, helper/installer "
    "death, port takeover and direct-Gateway bypass regressions pass before "
    "state/setup/backup work continues; state/UPDATER_DRIVER.json binds the final "
    "driver/version/maturity/evidence and any preview-risk approval, with no unproven "
    "fallback or custom archive/crypto path."
)
P13_T05_OLD_ACCEPTANCE = [
    *P13_T05_ACCEPTANCE_PREFIX,
    P13_T05_OLD_GATEWAY_ACCEPTANCE,
    P13_T05_FINAL_ACCEPTANCE,
]
P13_T05_NEW_ACCEPTANCE = [
    *P13_T05_ACCEPTANCE_PREFIX,
    P13_T05_NEW_GATEWAY_ACCEPTANCE,
    P13_T05_FINAL_ACCEPTANCE,
]
P13_T10_FIRST_ACCEPTANCE = (
    "Axum/Tower serves static Web and Agent-owned setup/lifecycle/update SSE; "
    "Fastify @fastify/sse serves Gateway domain/job SSE only, and Agent streaming "
    "proxy preserves separate event-ID/replay namespaces and authorization."
)
P13_T10_OLD_WEB_INGRESS_ACCEPTANCE = (
    "Agent pre-binds and passes the Gateway loopback listener, verifies child identity, "
    "strips/overwrites every client-supplied capability header, and injects the per-start "
    "memory-only capability only after authorization. Gateway rejects direct/spoofed "
    "HTTP/SSE/health requests, never reflects the capability, and port-race/bypass "
    "regressions pass."
)
P13_T10_NEW_WEB_INGRESS_ACCEPTANCE = (
    "Agent validates the Gateway bootstrap ready frame (OS-assigned port, child identity, "
    "and instance nonce), strips/overwrites every client-supplied capability header, and "
    "injects the per-start memory-only capability only after authorization. Gateway "
    "atomically binds 127.0.0.1:0 after bootstrap, rejects direct/spoofed HTTP/SSE/health "
    "requests, never reflects the capability, and port-takeover/bypass regressions pass; "
    "fixed-port probing, release/rebind, and unsupported Windows descriptor passing are "
    "forbidden."
)
P13_T10_ACCEPTANCE_SUFFIX = (
    "IPv4/IPv6 wildcard bind with deterministic bind-conflict policy, Host allowlist, no "
    "wildcard CORS, default loopback admission, authenticated LAN optional CIDR, "
    "Docker-always-auth, Origin/CSRF, generation/session revocation, rate limit, HTTP-LAN "
    "warning and redacted audit tests pass.",
    "The handwritten HTTP parser/static/TLS/session/rate-limit implementation is absent "
    "and no second auth authority exists in Gateway.",
)
P13_T10_OLD_ACCEPTANCE = [
    P13_T10_FIRST_ACCEPTANCE,
    P13_T10_OLD_WEB_INGRESS_ACCEPTANCE,
    *P13_T10_ACCEPTANCE_SUFFIX,
]
P13_T10_NEW_ACCEPTANCE = [
    P13_T10_FIRST_ACCEPTANCE,
    P13_T10_NEW_WEB_INGRESS_ACCEPTANCE,
    *P13_T10_ACCEPTANCE_SUFFIX,
]

P13_AMENDMENT_REASON = (
    "Node 24.18.0 does not support adopting a file-descriptor TCP listener "
    "or passing sockets through child stdio on Windows. Atomic child port-zero "
    "bind plus a private ready frame preserves the no-takeover invariant without "
    "a native libuv bridge or release/rebind fallback."
)
P13_AMENDMENT_EVIDENCE = [
    {
        "source": (
            "https://nodejs.org/download/release/v24.18.0/docs/api/"
            "net.html#serverlistenhandle-backlog-callback"
        ),
        "finding": "Listening on a file descriptor is not supported on Windows.",
    },
    {
        "source": (
            "https://nodejs.org/download/release/v24.18.0/docs/api/"
            "child_process.html#subprocesssendmessage-sendhandle-options-callback"
        ),
        "finding": "Sending IPC sockets is not supported on Windows.",
    },
]


def canonical_sha256(value: object) -> str:
    payload = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def p13_t12_definition_amendment(task_id: str = "P13-T05") -> dict[str, Any]:
    values = {
        "P13-T05": (P13_T05_OLD_ACCEPTANCE, P13_T05_NEW_ACCEPTANCE),
        "P13-T10": (P13_T10_OLD_ACCEPTANCE, P13_T10_NEW_ACCEPTANCE),
    }
    try:
        old_value, new_value = values[task_id]
    except KeyError as error:
        raise ValueError(f"unsupported P13-T12 amendment target: {task_id}") from error
    return {
        "schema": "cmclient-task-definition-amendment/v1",
        "task": task_id,
        "repairTask": "P13-T12",
        "field": "acceptance",
        "oldValue": copy.deepcopy(old_value),
        "oldValueSha256": canonical_sha256(old_value),
        "newValue": copy.deepcopy(new_value),
        "newValueSha256": canonical_sha256(new_value),
        "reason": P13_AMENDMENT_REASON,
        "decision": "atomic-child-bind",
        "evidence": copy.deepcopy(P13_AMENDMENT_EVIDENCE),
        "recordedAt": "2026-07-21T08:00:00Z",
    }


def p13_t12_definition_amendments() -> list[dict[str, Any]]:
    return [
        p13_t12_definition_amendment("P13-T05"),
        p13_t12_definition_amendment("P13-T10"),
    ]


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
    definition_amendments: list[dict[str, Any]] | None = None,
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
    definition_amendments = copy.deepcopy(definition_amendments or [])
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
        "definitionAmendments": definition_amendments,
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
        "definitionAmendments": copy.deepcopy(definition_amendments),
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
