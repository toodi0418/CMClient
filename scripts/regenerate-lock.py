#!/usr/bin/env python3
"""Regenerate the committed v3 task-graph lock from workspace task state."""

import hashlib
import json
import sys
from copy import deepcopy
from pathlib import Path

V3_GRAPH_PAYLOAD_FIELDS = (
    "tasks", "historicalSupersessions", "v2CoverageMap", "licenseGate",
    "repositoryIdentity", "targetPlatforms", "callMeshServiceModel",
    "candidateIdentity", "completionChecker", "repairProtocol",
    "definitionAmendments", "activation", "completionCheckers", "scheduler",
    "supersessions", "existingTaskAmendments", "repairAllocation",
    "scopedCompletion", "activationInputs", "completionToolOnlyRepairAllowlist",
    "promotionBaseCommit",
)

V3_ACTIVE_GRAPH_FIELDS = (
    "id", "version", "source", "sourceSha256", "sourceBaseline", "branch",
    "completionTask", "manualReleaseTask", "importedAt", "completedHistorySha256",
    "supersededTaskIds", "historicalSupersessions", "v2CoverageMap", "licenseGate",
    "targetPlatforms", "callMeshServiceModel", "candidateIdentity", "completionChecker",
    "repairProtocol", "definitionAmendments", "activation", "completionCheckers",
    "scheduler", "supersessions", "existingTaskAmendments", "repairAllocation",
    "scopedCompletion", "activationInputs", "completionToolOnlyRepairAllowlist",
    "promotionBaseCommit",
)

V3_LOCKED_TASK_FIELDS = (
    "phase", "title", "required", "manualGate", "environmental", "lane", "priority",
    "kind", "scope", "candidateReset", "repairOf", "supersedesPartOf", "acceptance",
    "caseGroups", "caseAssertions", "evidenceClaim", "observesWithoutFinalizing",
)


def canonical_sha256(value):
    payload = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def normalized_task_value(task, field):
    if field == "required":
        return task.get(field, True)
    if field in {"manualGate", "candidateReset", "environmental"}:
        return task.get(field, False)
    if field in {"caseGroups", "caseAssertions", "observesWithoutFinalizing"}:
        return deepcopy(task.get(field, []))
    return deepcopy(task.get(field))


def update_locked_task(definition, task):
    updated = {"id": definition["id"]}
    for field in V3_LOCKED_TASK_FIELDS:
        updated[field] = normalized_task_value(task, field)
    # Keep the existing dependency prefix: completed repairs may be frozen into
    # the promoted graph even though later repairs are appended at runtime.
    updated["dependsOn"] = deepcopy(definition["dependsOn"])
    return updated


def main():
    workspace_root = Path(__file__).resolve().parents[3]
    tasks_path = workspace_root / "state/TASKS.json"
    lock_path = Path(__file__).with_name("unified-task-graph-lock.json")

    try:
        tasks_data = json.loads(tasks_path.read_text(encoding="utf-8"))
        lock_data = json.loads(lock_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        print(f"Error loading task state or lock: {error}", file=sys.stderr)
        return 1

    active_graph = tasks_data.get("activeGraph")
    if not isinstance(active_graph, dict):
        print("Error: activeGraph is missing from TASKS.json", file=sys.stderr)
        return 1
    if active_graph.get("version") != 3:
        print("Error: activeGraph is not version 3", file=sys.stderr)
        return 1
    if lock_data.get("schema") != "cmclient-unified-task-graph-lock/v3":
        print("Error: committed lock is not a v3 graph lock", file=sys.stderr)
        return 1

    # Synchronize active-graph fields represented in the v3 lock. The
    # superseded ID list is derived by the validator and is not a lock field.
    for field in V3_ACTIVE_GRAPH_FIELDS:
        if field == "supersededTaskIds":
            continue
        if field not in active_graph:
            print(f"Error: activeGraph is missing {field}", file=sys.stderr)
            return 1
        lock_data[field] = deepcopy(active_graph[field])

    state_tasks = {
        task["id"]: task
        for task in tasks_data.get("tasks", [])
        if isinstance(task, dict) and isinstance(task.get("id"), str)
    }
    updated_tasks = []
    for definition in lock_data.get("tasks", []):
        task_id = definition.get("id") if isinstance(definition, dict) else None
        task = state_tasks.get(task_id)
        if task is None:
            print(f"Error: locked task is missing from TASKS.json: {task_id}", file=sys.stderr)
            return 1
        updated_tasks.append(update_locked_task(definition, task))
    lock_data["tasks"] = updated_tasks
    lock_data["taskDefinitionCount"] = len(updated_tasks)

    payload = {field: lock_data.get(field) for field in V3_GRAPH_PAYLOAD_FIELDS}
    lock_data["graphSha256"] = canonical_sha256(payload)

    try:
        lock_path.write_text(
            json.dumps(lock_data, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
    except OSError as error:
        print(f"Error writing lock file: {error}", file=sys.stderr)
        return 1

    print(f"Successfully regenerated {lock_path}")
    print(f"  mappingAuthority: {lock_data['callMeshServiceModel'].get('mappingAuthority')}")
    print(f"  graphSha256: {lock_data['graphSha256']}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
