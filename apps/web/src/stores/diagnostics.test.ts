import { createPinia, setActivePinia } from "pinia";
import { beforeEach, describe, expect, it } from "vitest";

import { GatewayApiError } from "@cmclient/api-client";

import { createDiagnosticsStore, type DiagnosticsClient } from "./diagnostics";

const succeededJob = {
  id: "diagnostics-1",
  type: "diagnostics.integrity_check",
  status: "succeeded" as const,
  createdAt: "2026-07-18T00:00:00.000Z",
  updatedAt: "2026-07-18T00:00:01.000Z",
  completedAt: "2026-07-18T00:00:01.000Z",
};

describe("diagnostics store", () => {
  beforeEach(() => setActivePinia(createPinia()));

  it("submits an integrity-check job and reads its durable state", async () => {
    const useDiagnostics = createDiagnosticsStore({
      diagnostics: {
        integrityCheck: async () => ({ jobId: "diagnostics-1", reused: false }),
      },
      jobs: {
        get: async () => succeededJob,
        cancel: async () => succeededJob,
      },
    } satisfies DiagnosticsClient);
    const diagnostics = useDiagnostics();

    await diagnostics.runIntegrityCheck();

    expect(diagnostics.activeJobId).toBe("diagnostics-1");
    expect(diagnostics.job).toEqual(succeededJob);
    expect(diagnostics.errorCode).toBeUndefined();
  });

  it("retains a stable error code when diagnostics cannot be submitted", async () => {
    const useDiagnostics = createDiagnosticsStore({
      diagnostics: {
        integrityCheck: async () => {
          throw new GatewayApiError({ code: "GATEWAY_JOB_ENGINE_UNAVAILABLE" });
        },
      },
      jobs: {
        get: async () => succeededJob,
        cancel: async () => succeededJob,
      },
    } satisfies DiagnosticsClient);
    const diagnostics = useDiagnostics();

    await diagnostics.runIntegrityCheck();

    expect(diagnostics.errorCode).toBe("GATEWAY_JOB_ENGINE_UNAVAILABLE");
  });
});
