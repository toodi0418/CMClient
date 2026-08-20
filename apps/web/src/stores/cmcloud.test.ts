import { createPinia, setActivePinia } from "pinia";
import { beforeEach, describe, expect, it } from "vitest";

import { GatewayApiError } from "@cmclient/api-client";
import type { CMCloudAccountProjection } from "@cmclient/contracts";

import { createCMCloudStore } from "./cmcloud";

describe("CMCloud store", () => {
  beforeEach(() => setActivePinia(createPinia()));

  const projection = (
    stations: CMCloudAccountProjection["stations"] = [],
  ): CMCloudAccountProjection => ({
    type: "account_projection" as const,
    schemaVersion: 1 as const,
    revision: 4,
    generation: 2,
    tenant: {
      id: "9660bc4b-bc0a-4d6f-b1a6-2278630b1a4b",
      name: "Fixture tenant",
    },
    account: {
      issuer: "https://callmesh.example.invalid/oidc",
      subject: "subject-1",
      displayName: "Fixture operator",
      role: "operator" as const,
      state: "approved" as const,
      mappingFreezeEpoch: 1,
    },
    stations,
    authority: { cmcloud: true as const, epoch: 1, revision: 4 },
    freshness: {
      projectedAt: "2026-08-20T00:00:00.000Z",
      staleAfterMs: 60_000,
    },
    errorState: null,
  });

  it("loads redacted enrollment status", async () => {
    const useCMCloud = createCMCloudStore({
      cmcloud: {
        status: async () => ({
          schemaVersion: 1,
          state: "active",
          endpoint: "wss://cmcloud.example.invalid/agent/v1",
          installationGeneration: 2,
          credentialVersion: 4,
        }),
        enroll: async () => {
          throw new Error("not used");
        },
        accountProjection: async () => projection(),
      },
    });
    const cmcloud = useCMCloud();

    await cmcloud.refresh();

    expect(cmcloud.status?.state).toBe("active");
    expect(cmcloud.status?.credentialVersion).toBe(4);
    expect(cmcloud.projection?.account.displayName).toBe("Fixture operator");
    expect(cmcloud.projectionStatus).toBe("ready");
    expect(cmcloud.projection?.stations).toEqual([]);
  });

  it("clears pairing input and reports stable enrollment failures", async () => {
    let received = "";
    const useCMCloud = createCMCloudStore({
      cmcloud: {
        status: async () => {
          throw new Error("not used");
        },
        enroll: async ({ pairingCode }) => {
          received = pairingCode;
          throw new GatewayApiError({ code: "CMCLOUD_ENROLLMENT_REJECTED" });
        },
        accountProjection: async () => projection(),
      },
    });
    const cmcloud = useCMCloud();

    cmcloud.setPairingCode("pairing-code-1234");
    await cmcloud.enroll();

    expect(received).toBe("pairing-code-1234");
    expect(cmcloud.pairingCode).toBe("");
    expect(cmcloud.errorCode).toBe("CMCLOUD_ENROLLMENT_REJECTED");
  });

  it("rejects malformed pairing input before calling the Agent", async () => {
    let called = false;
    const useCMCloud = createCMCloudStore({
      cmcloud: {
        status: async () => {
          throw new Error("not used");
        },
        enroll: async () => {
          called = true;
          throw new Error("must not call");
        },
        accountProjection: async () => projection(),
      },
    });
    const cmcloud = useCMCloud();
    cmcloud.setPairingCode("too-short");

    await cmcloud.enroll();

    expect(called).toBe(false);
    expect(cmcloud.errorCode).toBe("CMCLOUD_ENROLLMENT_REQUEST_INVALID");
  });

  it.each([
    "ACCOUNT_PROJECTION_UNAVAILABLE",
    "ACCOUNT_PROJECTION_STALE",
    "ACCOUNT_PROJECTION_AMBIGUOUS",
  ])("keeps enrollment usable when projection is %s", async (code) => {
    const useCMCloud = createCMCloudStore({
      cmcloud: {
        status: async () => ({
          schemaVersion: 1,
          state: "active",
          endpoint: "wss://cmcloud.example.invalid/agent/v1",
          installationGeneration: 2,
          credentialVersion: 4,
        }),
        enroll: async () => {
          throw new Error("not used");
        },
        accountProjection: async () => {
          throw new GatewayApiError({ code });
        },
      },
    });
    const cmcloud = useCMCloud();

    await cmcloud.refresh();

    expect(cmcloud.status?.state).toBe("active");
    expect(cmcloud.projection).toBeUndefined();
    expect(cmcloud.projectionStatus).toBe("unavailable");
    expect(cmcloud.projectionErrorCode).toBe(code);
  });

  it("marks an in-band projection error as degraded without inventing account data", async () => {
    const degraded = projection();
    degraded.errorState = {
      code: "ACCOUNT_PROJECTION_STALE",
      since: "2026-08-20T00:00:00.000Z",
    };
    const useCMCloud = createCMCloudStore({
      cmcloud: {
        status: async () => {
          throw new Error("not used");
        },
        enroll: async () => {
          throw new Error("not used");
        },
        accountProjection: async () => degraded,
      },
    });
    const cmcloud = useCMCloud();

    await cmcloud.refresh();

    expect(cmcloud.projection?.account.displayName).toBe("Fixture operator");
    expect(cmcloud.projectionStatus).toBe("degraded");
    expect(cmcloud.projectionErrorCode).toBe("ACCOUNT_PROJECTION_STALE");
  });
});
