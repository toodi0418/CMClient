import { describe, expect, it } from "vitest";
import { createActor } from "xstate";

import { setupFlowMachine } from "./setup-flow";

describe("setup XState view model", () => {
  it("projects a fresh Agent flow through review, validation, and finish", () => {
    const actor = createActor(setupFlowMachine).start();

    actor.send({ type: "AGENT_PHASE_CHANGED", phase: "terms_required" });
    expect(actor.getSnapshot().matches("terms")).toBe(true);

    actor.send({ type: "AGENT_PHASE_CHANGED", phase: "credentials_required" });
    expect(actor.getSnapshot().matches({ credentials: "connection" })).toBe(
      true,
    );
    actor.send({ type: "REVIEW" });
    expect(actor.getSnapshot().matches({ credentials: "review" })).toBe(true);

    actor.send({ type: "AGENT_PHASE_CHANGED", phase: "validating" });
    expect(actor.getSnapshot().matches("validating")).toBe(true);
    actor.send({ type: "AGENT_PHASE_CHANGED", phase: "ready" });
    expect(actor.getSnapshot().matches("finish")).toBe(true);
  });

  it("resumes authoritative states and preserves a local review on duplicate snapshots", () => {
    const actor = createActor(setupFlowMachine).start();

    actor.send({ type: "AGENT_PHASE_CHANGED", phase: "credentials_required" });
    actor.send({ type: "REVIEW" });
    actor.send({ type: "AGENT_PHASE_CHANGED", phase: "credentials_required" });
    expect(actor.getSnapshot().matches({ credentials: "review" })).toBe(true);

    actor.send({ type: "AGENT_PHASE_CHANGED", phase: "recovery_required" });
    expect(actor.getSnapshot().matches("recovery")).toBe(true);
    actor.send({ type: "AGENT_PHASE_CHANGED", phase: "validating" });
    expect(actor.getSnapshot().matches("validating")).toBe(true);
  });

  it("lets an Agent terms change replace a completed browser view", () => {
    const actor = createActor(setupFlowMachine).start();
    actor.send({ type: "AGENT_PHASE_CHANGED", phase: "ready" });
    actor.send({ type: "AGENT_PHASE_CHANGED", phase: "terms_required" });

    expect(actor.getSnapshot().matches("terms")).toBe(true);
  });
});
