import { createPinia, setActivePinia } from "pinia";
import { beforeEach, describe, expect, it } from "vitest";
import { GatewayApiError } from "@cmclient/api-client";
import { CURRENT_TERMS_VERSION, type SetupStatus } from "@cmclient/contracts";

import { createSetupStore, type SetupClient } from "./setup";

const termsStatus: SetupStatus = {
  schemaVersion: 1 as const,
  currentTermsVersion: CURRENT_TERMS_VERSION,
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
              host: "192.0.2.10",
              port: 4403 as const,
              source: "mdns" as const,
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
    expect(setup.candidates[0]?.host).toBe("192.0.2.10");
    expect(setup.callmeshUrl).toBe("https://callmesh.tmmarc.org");
  });

  it("resumes from REST and keeps the Agent projection current from setup SSE", async () => {
    let listener:
      | {
          onStatus(status: SetupStatus): void;
          onError(code: string): void;
        }
      | undefined;
    let stopped = false;
    const client: SetupClient = {
      setup: {
        status: async () => credentialsStatus,
        discovery: async () => ({
          schemaVersion: 1,
          candidates: [],
          callmeshUrl: "https://callmesh.tmmarc.org",
        }),
        acceptTerms: async () => credentialsStatus,
        configure: async () => credentialsStatus,
        reset: async () => termsStatus,
      },
      subscribe: (registered) => {
        listener = registered;
        return () => {
          stopped = true;
        };
      },
    };
    const setup = createSetupStore(client)();

    await setup.start();
    expect(setup.phase).toBe("credentials_required");
    expect(setup.connection).toBe("connecting");

    listener?.onStatus({
      ...credentialsStatus,
      phase: "validating",
      credentialsRequired: false,
      validating: true,
      reasonCode: "SETUP_VALIDATING",
    });
    expect(setup.phase).toBe("validating");
    expect(setup.connection).toBe("open");

    listener?.onError("AGENT_SETUP_EVENT_STREAM_UNAVAILABLE");
    expect(setup.connection).toBe("reconnecting");
    expect(setup.status?.reasonCode).toBe("SETUP_VALIDATING");
    setup.stop();
    expect(stopped).toBe(true);
    expect(setup.connection).toBe("stopped");
  });

  it("posts the exact terms version projected by the Agent", async () => {
    let acceptedVersion = "";
    const client: SetupClient = {
      setup: {
        status: async () => termsStatus,
        discovery: async () => ({
          schemaVersion: 1,
          candidates: [],
          callmeshUrl: "https://callmesh.tmmarc.org",
        }),
        acceptTerms: async (version) => {
          acceptedVersion = version;
          return credentialsStatus;
        },
        configure: async () => credentialsStatus,
        reset: async () => termsStatus,
      },
    };
    const setup = createSetupStore(client)();

    await setup.refresh();
    await setup.acceptTerms();

    expect(acceptedVersion).toBe(CURRENT_TERMS_VERSION);
  });

  it("does not synthesize a loopback candidate when LAN discovery is empty", async () => {
    const client: SetupClient = {
      setup: {
        status: async () => credentialsStatus,
        discovery: async () => ({
          schemaVersion: 1 as const,
          candidates: [],
          callmeshUrl: "https://callmesh.tmmarc.org" as const,
        }),
        acceptTerms: async () => credentialsStatus,
        configure: async () => credentialsStatus,
        reset: async () => termsStatus,
      },
    };
    const setup = createSetupStore(client)();

    await setup.discover();

    expect(setup.candidates).toEqual([]);
    expect(JSON.stringify(setup.$state)).not.toMatch(
      /127\.0\.0\.1|::1|localhost/i,
    );
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

  it.each([
    ["CALLMESH_CREDENTIAL_REJECTED", 401, false],
    ["CALLMESH_UNAVAILABLE", 503, true],
  ])(
    "keeps setup retryable for %s without retaining the key",
    async (code, status, retryable) => {
      const client: SetupClient = {
        setup: {
          status: async () => credentialsStatus,
          discovery: async () => ({
            schemaVersion: 1 as const,
            candidates: [],
            callmeshUrl: "https://callmesh.tmmarc.org" as const,
          }),
          acceptTerms: async () => credentialsStatus,
          configure: async () => {
            throw new GatewayApiError({ code, status, retryable });
          },
          reset: async () => termsStatus,
        },
      };
      const setup = createSetupStore(client)();
      const apiKey = "fixture-private-setup-key";

      await expect(
        setup.configure({
          meshtasticHost: "172.16.8.88",
          meshtasticPort: 4403,
          callmeshApiKey: apiKey,
        }),
      ).rejects.toMatchObject({ code, status, retryable });

      expect(setup.errorCode).toBe(code);
      expect(setup.required).toBe(true);
      expect(JSON.stringify(setup.$state)).not.toContain(apiKey);
    },
  );
});
