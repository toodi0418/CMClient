import { describe, expect, it } from "vitest";

import {
  TransportConnectionStateMachine,
  TransportStateTransitionError,
  canTransition,
} from "./state";

describe("TransportConnectionStateMachine", () => {
  it("models the required connection and configuration lifecycle", () => {
    let tick = 0;
    const state = new TransportConnectionStateMachine(
      "tcp",
      () => new Date(`2026-07-18T00:00:0${tick++}.000Z`),
    );

    expect(state.state).toEqual({
      transport: "tcp",
      status: "disconnected",
      changedAt: "2026-07-18T00:00:00.000Z",
    });
    expect(state.transition("connecting").status).toBe("connecting");
    expect(state.transition("configuring").status).toBe("configuring");
    expect(state.transition("ready")).toEqual({
      transport: "tcp",
      status: "ready",
      changedAt: "2026-07-18T00:00:03.000Z",
    });
  });

  it("requires stable failure details for degraded and backoff states", () => {
    const state = new TransportConnectionStateMachine("serial");
    state.transition("connecting");
    expect(() => state.transition("degraded")).toThrow(
      TransportStateTransitionError,
    );
    expect(
      state.transition("degraded", { reasonCode: "SERIAL_OPEN_FAILED" }),
    ).toMatchObject({ status: "degraded", reasonCode: "SERIAL_OPEN_FAILED" });
    expect(() => state.transition("backoff", { attempt: 1 })).toThrow(
      TransportStateTransitionError,
    );
    expect(
      state.transition("backoff", {
        attempt: 1,
        reasonCode: "SERIAL_OPEN_FAILED",
      }),
    ).toMatchObject({
      status: "backoff",
      attempt: 1,
      reasonCode: "SERIAL_OPEN_FAILED",
    });
  });

  it("fails closed for skipped protocol phases and invalid reason codes", () => {
    const state = new TransportConnectionStateMachine("tcp");
    expect(() => state.transition("ready")).toThrow(
      TransportStateTransitionError,
    );
    expect(() =>
      state.transition("connecting", { reasonCode: "not_stable" }),
    ).toThrow(TransportStateTransitionError);
    expect(canTransition("ready", "disconnected")).toBe(true);
    expect(canTransition("disconnected", "ready")).toBe(false);
  });
});
