import { isGatewayApiError, type GatewayApiClient } from "@cmclient/api-client";
import type {
  MeshMessage,
  MeshNode,
  MeshTelemetry,
  PositionCanonicalEvent,
} from "@cmclient/contracts";
import { defineStore } from "pinia";

import { managementApi } from "../management-api";

export interface DomainClient {
  domain: Pick<
    GatewayApiClient["domain"],
    "nodes" | "messages" | "telemetry" | "positions"
  >;
}

export function createDomainStore(client: DomainClient = managementApi) {
  return defineStore("domain", {
    state: () => ({
      nodesLoading: false,
      messagesLoading: false,
      telemetryLoading: false,
      positionsLoading: false,
      nodesErrorCode: undefined as string | undefined,
      messagesErrorCode: undefined as string | undefined,
      telemetryErrorCode: undefined as string | undefined,
      positionsErrorCode: undefined as string | undefined,
      nodes: [] as MeshNode[],
      messages: [] as MeshMessage[],
      telemetry: [] as MeshTelemetry[],
      positions: [] as PositionCanonicalEvent[],
    }),
    actions: {
      async refresh() {
        await Promise.all([
          this.refreshNodes(),
          this.refreshMessages(),
          this.refreshTelemetry(),
          this.refreshPositions(),
        ]);
      },
      async refreshNodes() {
        if (this.nodesLoading) {
          return;
        }
        this.nodesLoading = true;
        try {
          const nodes = await client.domain.nodes();
          this.nodes = nodes.items;
          this.nodesErrorCode = undefined;
        } catch (error) {
          this.nodesErrorCode = isGatewayApiError(error)
            ? error.code
            : "GATEWAY_NETWORK_UNAVAILABLE";
        } finally {
          this.nodesLoading = false;
        }
      },
      async refreshMessages() {
        if (this.messagesLoading) {
          return;
        }
        this.messagesLoading = true;
        try {
          const messages = await client.domain.messages();
          this.messages = messages.items;
          this.messagesErrorCode = undefined;
        } catch (error) {
          this.messagesErrorCode = isGatewayApiError(error)
            ? error.code
            : "GATEWAY_NETWORK_UNAVAILABLE";
        } finally {
          this.messagesLoading = false;
        }
      },
      async refreshTelemetry() {
        if (this.telemetryLoading) {
          return;
        }
        this.telemetryLoading = true;
        try {
          const telemetry = await client.domain.telemetry();
          this.telemetry = telemetry.items;
          this.telemetryErrorCode = undefined;
        } catch (error) {
          this.telemetryErrorCode = isGatewayApiError(error)
            ? error.code
            : "GATEWAY_NETWORK_UNAVAILABLE";
        } finally {
          this.telemetryLoading = false;
        }
      },
      async refreshPositions() {
        if (this.positionsLoading) {
          return;
        }
        this.positionsLoading = true;
        try {
          const positions = await client.domain.positions();
          this.positions = positions.items;
          this.positionsErrorCode = undefined;
        } catch (error) {
          this.positionsErrorCode = isGatewayApiError(error)
            ? error.code
            : "GATEWAY_NETWORK_UNAVAILABLE";
        } finally {
          this.positionsLoading = false;
        }
      },
    },
  });
}

export const useDomainStore = createDomainStore();
