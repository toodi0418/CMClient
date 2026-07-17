import { GatewayApiClient, isGatewayApiError } from "@cmclient/api-client";
import type {
  MeshMessage,
  MeshNode,
  MeshTelemetry,
  PositionCanonicalEvent,
} from "@cmclient/contracts";
import { defineStore } from "pinia";

export interface DomainClient {
  domain: Pick<
    GatewayApiClient["domain"],
    "nodes" | "messages" | "telemetry" | "positions"
  >;
}

export function createDomainStore(
  client: DomainClient = new GatewayApiClient(),
) {
  return defineStore("domain", {
    state: () => ({
      loading: false,
      errorCode: undefined as string | undefined,
      nodes: [] as MeshNode[],
      messages: [] as MeshMessage[],
      telemetry: [] as MeshTelemetry[],
      positions: [] as PositionCanonicalEvent[],
    }),
    actions: {
      async refresh() {
        if (this.loading) {
          return;
        }
        this.loading = true;
        try {
          const [nodes, messages, telemetry, positions] = await Promise.all([
            client.domain.nodes(),
            client.domain.messages(),
            client.domain.telemetry(),
            client.domain.positions(),
          ]);
          this.nodes = nodes.items;
          this.messages = messages.items;
          this.telemetry = telemetry.items;
          this.positions = positions.items;
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

export const useDomainStore = createDomainStore();
