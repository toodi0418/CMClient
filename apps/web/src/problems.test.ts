import { describe, expect, it } from "vitest";

import {
  capabilityReasonKey,
  problemForCode,
  updateActivityKey,
} from "./problems";

describe("user-facing problem catalog", () => {
  it("explains management rate limits as a retryable temporary condition", () => {
    expect(problemForCode("MANAGEMENT_REQUEST_RATE_LIMITED")).toEqual({
      severity: "warning",
      titleKey: "problems.rateLimitedTitle",
      messageKey: "problems.rateLimitedMessage",
      retryable: true,
    });
  });

  it("keeps stale CallMesh and APRS provision states fail-closed but readable", () => {
    expect(problemForCode("CALLMESH_STALE_RESPONSE").retryable).toBe(false);
    expect(problemForCode("APRS_PROVISION_UNAVAILABLE").messageKey).toBe(
      "problems.aprsProvisionMessage",
    );
  });

  it("maps capability and update internals to user-facing labels", () => {
    expect(capabilityReasonKey("owned_by_agent")).toBe(
      "capabilityReason.ownedByAgent",
    );
    expect(updateActivityKey("UPDATE_SIGNATURE_VERIFIED")).toBe(
      "updates.activity.signatureVerified",
    );
  });
});
