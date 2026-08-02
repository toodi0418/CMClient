import { createPinia, setActivePinia } from "pinia";
import { beforeEach, describe, expect, it } from "vitest";
import { createMemoryHistory } from "vue-router";

import { GatewayApiError } from "@cmclient/api-client";
import { CURRENT_TERMS_VERSION, type SetupStatus } from "@cmclient/contracts";
import { createSetupStore, type SetupClient } from "../stores/setup";
import { createManagementRouter, installSetupRouteGate, router } from "./index";

const termsStatus: SetupStatus = {
  schemaVersion: 1,
  currentTermsVersion: CURRENT_TERMS_VERSION,
  phase: "terms_required",
  setupRequired: true,
  termsRequired: true,
  credentialsRequired: false,
  validating: false,
  ready: false,
  recoveryRequired: false,
  reasonCode: "SETUP_TERMS_REQUIRED",
};
const readyStatus: SetupStatus = {
  schemaVersion: 1,
  currentTermsVersion: CURRENT_TERMS_VERSION,
  phase: "ready",
  setupRequired: false,
  termsRequired: false,
  credentialsRequired: false,
  validating: false,
  ready: true,
  recoveryRequired: false,
  reasonCode: "SETUP_READY",
};

beforeEach(() => setActivePinia(createPinia()));

describe("management web router", () => {
  it("exposes stable routes for the control shell", () => {
    expect(router.resolve("/").name).toBe("overview");
    expect(router.resolve("/meshtastic").name).toBe("meshtastic");
    expect(router.resolve("/aprs").name).toBe("aprs");
    expect(router.resolve("/callmesh").name).toBe("callmesh");
    expect(router.resolve("/cmcloud").name).toBe("cmcloud");
    expect(router.resolve("/logs").name).toBe("logs");
    expect(router.resolve("/updates").name).toBe("updates");
    expect(router.resolve("/settings").name).toBe("settings");
    expect(router.resolve("/diagnostics").name).toBe("diagnostics");
    expect(router.resolve("/setup").name).toBe("setup");
    expect(router.resolve("/missing").matched.at(-1)?.redirect).toBe("/");
  });

  it("fails closed to /setup and resumes the originally requested route", async () => {
    const setup = setupStore(termsStatus);
    const guardedRouter = createManagementRouter(createMemoryHistory(), {
      template: "<div />",
    });
    guardedRouter.addRoute({
      path: "/fixture-management",
      component: { template: "<div />" },
    });
    installSetupRouteGate(guardedRouter, setup);

    await guardedRouter.push("/fixture-management");
    expect(guardedRouter.currentRoute.value.fullPath).toBe("/setup");

    setup.applyStatus(readyStatus);
    await eventually();
    expect(guardedRouter.currentRoute.value.fullPath).toBe(
      "/fixture-management",
    );
  });

  it("redirects an active management route when Agent requires terms again", async () => {
    const setup = setupStore(readyStatus);
    const guardedRouter = createManagementRouter(createMemoryHistory(), {
      template: "<div />",
    });
    guardedRouter.addRoute({
      path: "/fixture-management",
      component: { template: "<div />" },
    });
    installSetupRouteGate(guardedRouter, setup);

    await guardedRouter.push("/fixture-management");
    setup.applyStatus(termsStatus);
    await eventually();

    expect(guardedRouter.currentRoute.value.fullPath).toBe("/setup");
  });

  it("keeps a deep link when setup status is temporarily rate limited", async () => {
    const setup = createSetupStore({
      setup: {
        status: async () => {
          throw new GatewayApiError({
            code: "MANAGEMENT_REQUEST_RATE_LIMITED",
            status: 429,
            retryable: true,
          });
        },
        discovery: async () => ({
          schemaVersion: 1,
          candidates: [],
          callmeshUrl: "https://callmesh.tmmarc.org",
        }),
        acceptTerms: async () => termsStatus,
        configure: async () => termsStatus,
        reset: async () => termsStatus,
        operationalReset: async () => termsStatus,
      },
    })();
    const guardedRouter = createManagementRouter(createMemoryHistory(), {
      template: "<div />",
    });
    guardedRouter.addRoute({
      path: "/fixture-management",
      component: { template: "<div />" },
    });
    installSetupRouteGate(guardedRouter, setup);

    await guardedRouter.push("/fixture-management");

    expect(setup.admission).toBe("unavailable");
    expect(guardedRouter.currentRoute.value.fullPath).toBe(
      "/fixture-management",
    );
  });
});

function setupStore(initialStatus: SetupStatus) {
  const client: SetupClient = {
    setup: {
      status: async () => initialStatus,
      discovery: async () => ({
        schemaVersion: 1,
        candidates: [],
        callmeshUrl: "https://callmesh.tmmarc.org",
      }),
      acceptTerms: async () => initialStatus,
      configure: async () => initialStatus,
      reset: async () => initialStatus,
      operationalReset: async () => initialStatus,
    },
  };
  const setup = createSetupStore(client)();
  setup.applyStatus(initialStatus);
  return setup;
}

async function eventually() {
  await new Promise((resolve) => setTimeout(resolve, 0));
  await new Promise((resolve) => setTimeout(resolve, 0));
}
