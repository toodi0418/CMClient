import { GatewayApiClient, isGatewayApiError } from "@cmclient/api-client";
import type {
  AprsIgateSubmission,
  AprsOutboxEntry,
  AprsRuntimeStatus,
} from "@cmclient/contracts";
import { defineStore } from "pinia";

export interface AprsClient {
  aprs: Pick<
    GatewayApiClient["aprs"],
    "outbox" | "stationSubmissions" | "status"
  >;
}

export function createAprsStore(client: AprsClient = new GatewayApiClient()) {
  return defineStore("aprs", {
    state: () => ({
      loading: false,
      runtimeErrorCode: undefined as string | undefined,
      outboxErrorCode: undefined as string | undefined,
      stationErrorCode: undefined as string | undefined,
      status: undefined as AprsRuntimeStatus | undefined,
      entries: [] as AprsOutboxEntry[],
      stationSubmissions: [] as AprsIgateSubmission[],
    }),
    actions: {
      async refresh() {
        if (this.loading) {
          return;
        }
        this.loading = true;
        try {
          const [runtime, outbox, station] = await Promise.allSettled([
            client.aprs.status(),
            client.aprs.outbox(),
            client.aprs.stationSubmissions(),
          ]);
          if (runtime.status === "fulfilled") {
            this.status = runtime.value;
            this.runtimeErrorCode = undefined;
          } else {
            this.runtimeErrorCode = stableClientError(runtime.reason);
          }
          if (outbox.status === "fulfilled") {
            this.entries = outbox.value.items;
            this.outboxErrorCode = undefined;
          } else {
            this.outboxErrorCode = stableClientError(outbox.reason);
          }
          if (station.status === "fulfilled") {
            this.stationSubmissions = station.value.items;
            this.stationErrorCode = undefined;
          } else {
            this.stationErrorCode = stableClientError(station.reason);
          }
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

export const useAprsStore = createAprsStore();
