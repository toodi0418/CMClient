import { describe, expect, it } from "vitest";

import { projectionForEvent } from "./realtime-refresh";

describe("realtime projection routing", () => {
  it.each([
    ["node.updated", "domain"],
    ["message.received", "domain"],
    ["telemetry.received", "domain"],
    ["position.decision", "domain"],
    ["mesh.transport.state", "domain"],
    ["aprs.outbox.sent", "aprs"],
    ["callmesh.status", "callmesh"],
    ["proxy.client", "proxy"],
    ["job.status_changed", undefined],
  ])("routes %s to %s", (type, expected) => {
    expect(projectionForEvent(type)).toBe(expected);
  });
});
