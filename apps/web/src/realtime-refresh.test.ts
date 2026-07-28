import { afterEach, describe, expect, it, vi } from "vitest";

import { createRefreshScheduler, projectionForEvent } from "./realtime-refresh";

afterEach(() => vi.useRealTimers());

describe("realtime projection routing", () => {
  it.each([
    ["gateway.status_changed", "gateway"],
    ["system.health_changed", "gateway"],
    ["node.updated", "nodes"],
    ["message.received", "messages"],
    ["telemetry.received", "telemetry"],
    ["position.decision", "positions"],
    ["mesh.transport.state", "meshtastic"],
    ["aprs.outbox.submitted", "aprs"],
    ["aprs.outbox.observer_confirmed", "aprs"],
    ["callmesh.status", "callmesh"],
    ["proxy.client", "proxy"],
    ["job.status_changed", undefined],
  ])("routes %s to %s", (type, expected) => {
    expect(projectionForEvent(type)).toBe(expected);
  });

  it("coalesces repeated realtime refreshes for the same projection", async () => {
    vi.useFakeTimers();
    const scheduler = createRefreshScheduler(500);
    const refresh = vi.fn().mockResolvedValue(undefined);

    scheduler.schedule("aprs", refresh);
    scheduler.schedule("aprs", refresh);
    scheduler.schedule("aprs", refresh);

    await vi.advanceTimersByTimeAsync(500);
    expect(refresh).toHaveBeenCalledTimes(1);

    scheduler.dispose();
  });

  it("serializes mixed projections and retains one trailing refresh", async () => {
    vi.useFakeTimers();
    const scheduler = createRefreshScheduler(100);
    const calls: string[] = [];
    let finishNodes: (() => void) | undefined;
    const nodes = vi.fn(
      () =>
        new Promise<void>((resolve) => {
          calls.push("nodes");
          finishNodes = resolve;
        }),
    );
    const aprs = vi.fn(async () => {
      calls.push("aprs");
    });

    scheduler.schedule("nodes", nodes);
    scheduler.schedule("aprs", aprs);
    await vi.advanceTimersByTimeAsync(100);
    expect(calls).toEqual(["nodes"]);

    scheduler.schedule("nodes", nodes);
    finishNodes?.();
    await vi.advanceTimersByTimeAsync(100);
    expect(calls).toEqual(["nodes", "aprs"]);

    await vi.advanceTimersByTimeAsync(100);
    expect(calls).toEqual(["nodes", "aprs", "nodes"]);
    scheduler.dispose();
  });
});
