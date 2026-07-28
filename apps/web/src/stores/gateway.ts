import { isGatewayApiError, type GatewaySystemApi } from "@cmclient/api-client";
import {
  GatewayEventClient,
  type SseConnectionState,
} from "@cmclient/event-client";
import type {
  DomainEvent,
  SystemCapabilities,
  SystemStatus,
} from "@cmclient/contracts";
import { defineStore } from "pinia";

import { managementApi } from "../management-api";

export type GatewayAvailability = "checking" | "available" | "unavailable";

export interface GatewayClients {
  api: { system: GatewaySystemApi };
  events: {
    start(): void;
    stop(): void;
    onEvent(listener: (event: DomainEvent) => void): () => void;
    onError(listener: (error: unknown) => void): () => void;
    onStateChange(listener: (state: SseConnectionState) => void): () => void;
  };
}

function defaultClients(): GatewayClients {
  return {
    api: managementApi,
    events: new GatewayEventClient(),
  };
}

export function createGatewayStore(clients: GatewayClients = defaultClients()) {
  return defineStore("gateway", {
    state: () => ({
      availability: "checking" as GatewayAvailability,
      loading: false,
      status: undefined as SystemStatus | undefined,
      capabilities: undefined as SystemCapabilities | undefined,
      eventConnection: "idle" as SseConnectionState,
      lastEventType: undefined as string | undefined,
      recentEvents: [] as DomainEvent[],
      eventErrorCode: undefined as string | undefined,
      errorCode: undefined as string | undefined,
      initialized: false,
      unsubscribers: [] as Array<() => void>,
    }),
    actions: {
      async initialize() {
        if (!this.initialized) {
          this.initialized = true;
          this.unsubscribers = [
            clients.events.onStateChange((state) => {
              this.eventConnection = state;
            }),
            clients.events.onError((error) => {
              this.eventErrorCode = isGatewayApiError(error)
                ? error.code
                : "GATEWAY_NETWORK_UNAVAILABLE";
              if (this.availability !== "available") {
                this.errorCode = this.eventErrorCode;
              }
            }),
            clients.events.onEvent((event) => {
              this.lastEventType = event.type;
              this.recentEvents = [
                event,
                ...this.recentEvents.filter(
                  (current) => current.eventId !== event.eventId,
                ),
              ].slice(0, 50);
            }),
          ];
          clients.events.start();
        }

        await this.refresh();
      },
      async refresh() {
        if (this.loading) {
          return;
        }
        this.loading = true;

        try {
          const [status, capabilities] = await Promise.all([
            clients.api.system.status(),
            clients.api.system.capabilities(),
          ]);
          this.status = status;
          this.capabilities = capabilities;
          this.availability = "available";
          this.errorCode = undefined;
        } catch (error) {
          this.availability = "unavailable";
          this.errorCode = isGatewayApiError(error)
            ? error.code
            : "GATEWAY_NETWORK_UNAVAILABLE";
        } finally {
          this.loading = false;
        }
      },
      dispose() {
        for (const unsubscribe of this.unsubscribers) {
          unsubscribe();
        }
        this.unsubscribers = [];
        clients.events.stop();
        this.initialized = false;
        this.eventConnection = "stopped";
        this.eventErrorCode = undefined;
      },
    },
  });
}

export const useGatewayStore = createGatewayStore();
