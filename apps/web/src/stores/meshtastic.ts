import { isGatewayApiError, type GatewayApiClient } from "@cmclient/api-client";
import type { MeshtasticRuntimeStatus } from "@cmclient/contracts";
import { defineStore } from "pinia";

import { managementApi } from "../management-api";

export interface MeshtasticClient {
  meshtastic: Pick<GatewayApiClient["meshtastic"], "status">;
}

export function createMeshtasticStore(
  client: MeshtasticClient = managementApi,
) {
  return defineStore("meshtastic", {
    state: () => ({
      loading: false,
      errorCode: undefined as string | undefined,
      status: undefined as MeshtasticRuntimeStatus | undefined,
    }),
    actions: {
      async refresh() {
        if (this.loading) {
          return;
        }
        this.loading = true;
        try {
          this.status = await client.meshtastic.status();
          this.errorCode = undefined;
        } catch (error) {
          this.errorCode = stableClientError(error);
        } finally {
          this.loading = false;
        }
      },
    },
  });
}

function stableClientError(error: unknown): string {
  return isGatewayApiError(error) ? error.code : "GATEWAY_NETWORK_UNAVAILABLE";
}

export const useMeshtasticStore = createMeshtasticStore();
