import { createPinia, setActivePinia } from "pinia";
import { beforeEach, describe, expect, it } from "vitest";

import { GatewayApiError } from "@cmclient/api-client";

import { createCMCloudStore } from "./cmcloud";

describe("CMCloud store", () => {
  beforeEach(() => setActivePinia(createPinia()));

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
      },
    });
    const cmcloud = useCMCloud();

    await cmcloud.refresh();

    expect(cmcloud.status?.state).toBe("active");
    expect(cmcloud.status?.credentialVersion).toBe(4);
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
      },
    });
    const cmcloud = useCMCloud();
    cmcloud.setPairingCode("too-short");

    await cmcloud.enroll();

    expect(called).toBe(false);
    expect(cmcloud.errorCode).toBe("CMCLOUD_ENROLLMENT_REQUEST_INVALID");
  });
});
