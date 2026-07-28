import { isGatewayApiError, type GatewayApiClient } from "@cmclient/api-client";
import type { JobDetail } from "@cmclient/contracts";
import { defineStore } from "pinia";

import { managementApi } from "../management-api";

export interface DiagnosticsClient {
  diagnostics: Pick<GatewayApiClient["diagnostics"], "integrityCheck">;
  jobs: Pick<GatewayApiClient["jobs"], "get" | "cancel">;
}

export function createDiagnosticsStore(
  client: DiagnosticsClient = managementApi,
) {
  return defineStore("diagnostics", {
    state: () => ({
      loading: false,
      errorCode: undefined as string | undefined,
      activeJobId: undefined as string | undefined,
      job: undefined as JobDetail | undefined,
    }),
    actions: {
      async runIntegrityCheck() {
        if (this.loading) {
          return;
        }
        this.loading = true;
        try {
          const accepted = await client.diagnostics.integrityCheck();
          this.activeJobId = accepted.jobId;
          this.job = await client.jobs.get(accepted.jobId);
          this.errorCode = undefined;
        } catch (error) {
          this.errorCode = isGatewayApiError(error)
            ? error.code
            : "GATEWAY_NETWORK_UNAVAILABLE";
        } finally {
          this.loading = false;
        }
      },
      async refresh() {
        if (this.loading || !this.activeJobId) {
          return;
        }
        this.loading = true;
        try {
          this.job = await client.jobs.get(this.activeJobId);
          this.errorCode = undefined;
        } catch (error) {
          this.errorCode = isGatewayApiError(error)
            ? error.code
            : "GATEWAY_NETWORK_UNAVAILABLE";
        } finally {
          this.loading = false;
        }
      },
      async cancel() {
        if (this.loading || !this.job) {
          return;
        }
        this.loading = true;
        try {
          this.job = await client.jobs.cancel(this.job.id);
          this.errorCode = undefined;
        } catch (error) {
          this.errorCode = isGatewayApiError(error)
            ? error.code
            : "GATEWAY_NETWORK_UNAVAILABLE";
        } finally {
          this.loading = false;
        }
      },
    },
  });
}

export const useDiagnosticsStore = createDiagnosticsStore();
