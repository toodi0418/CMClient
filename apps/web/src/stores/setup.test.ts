import { createPinia, setActivePinia } from "pinia";
import { beforeEach, describe, expect, it } from "vitest";
import type { SetupStatus } from "@cmclient/contracts";

import { createSetupStore, type SetupClient } from "./setup";

const termsStatus: SetupStatus = {
  schemaVersion: 1 as const,
  phase: "terms_required" as const,
  setupRequired: true,
  termsRequired: true,
  credentialsRequired: false,
  validating: false,
  ready: false,
  recoveryRequired: false,
  reasonCode: "SETUP_TERMS_REQUIRED",
};

const credentialsStatus: SetupStatus = {
  ...termsStatus,
  phase: "credentials_required" as const,
  termsRequired: false,
  credentialsRequired: true,
  reasonCode: "SETUP_CREDENTIALS_REQUIRED",
};

describe("setup store", () => {
  beforeEach(() => setActivePinia(createPinia()));

  it("loads the Agent-owned status and discovers bounded candidates", async () => {
    const client: SetupClient = {
      setup: {
        status: async () => termsStatus,
        discovery: async () => ({
          schemaVersion: 1 as const,
          candidates: [
            {
              host: "127.0.0.1",
              port: 4403 as const,
              source: "loopback" as const,
            },
          ],
          callmeshUrl: "https://callmesh.tmmarc.org" as const,
        }),
        acceptTerms: async () => credentialsStatus,
        configure: async () => credentialsStatus,
        reset: async () => termsStatus,
      },
    };
    const setup = createSetupStore(client)();

    await setup.refresh();
    await setup.discover();

    expect(setup.phase).toBe("terms_required");
    expect(setup.candidates[0]?.host).toBe("127.0.0.1");
    expect(setup.callmeshUrl).toBe("https://callmesh.tmmarc.org");
  });

  it("advances terms without storing the API key in browser state", async () => {
    let receivedKey = "";
    const client: SetupClient = {
      setup: {
        status: async () => credentialsStatus,
        discovery: async () => ({
          schemaVersion: 1 as const,
          candidates: [],
          callmeshUrl: "https://callmesh.tmmarc.org" as const,
        }),
        acceptTerms: async () => credentialsStatus,
        configure: async (request) => {
          receivedKey = request.callmeshApiKey;
          return {
            ...credentialsStatus,
            phase: "ready" as const,
            setupRequired: false,
            credentialsRequired: false,
            ready: true,
            reasonCode: "SETUP_READY",
          } satisfies SetupStatus;
        },
        reset: async () => termsStatus,
      },
    };
    const setup = createSetupStore(client)();

    await setup.configure({
      meshtasticHost: "172.16.8.88",
      meshtasticPort: 4403,
      callmeshApiKey: "campaign-test-key",
    });

    expect(receivedKey).toBe("campaign-test-key");
    expect(JSON.stringify(setup.$state)).not.toContain("campaign-test-key");
    expect(setup.required).toBe(false);
  });
});
