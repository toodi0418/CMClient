import { GatewayApiClient, isGatewayApiError } from "@cmclient/api-client";
import type { AprsOutboxEntry } from "@cmclient/contracts";
import { defineStore } from "pinia";

export interface AprsClient {
  aprs: Pick<GatewayApiClient["aprs"], "outbox">;
}

export function createAprsStore(client: AprsClient = new GatewayApiClient()) {
  return defineStore("aprs", {
    state: () => ({
      loading: false,
      errorCode: undefined as string | undefined,
      entries: [] as AprsOutboxEntry[],
    }),
    actions: {
      async refresh() {
        if (this.loading) {
          return;
        }
        this.loading = true;
        try {
          this.entries = (await client.aprs.outbox()).items;
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

export const useAprsStore = createAprsStore();
