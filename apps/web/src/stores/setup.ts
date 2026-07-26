import {
  GatewayApiClient,
  isGatewayApiError,
  type AgentSetupApi,
} from "@cmclient/api-client";
import type {
  SetupConfigureRequest,
  SetupDiscoveryResponse,
  SetupStatus,
} from "@cmclient/contracts";
import { defineStore } from "pinia";

export interface SetupClient {
  setup: AgentSetupApi;
}

function defaultClient(): SetupClient {
  return { setup: new GatewayApiClient().setup };
}

/**
 * Keeps setup progress in the Agent, while deliberately keeping the API key
 * out of Pinia state and browser persistence.
 */
export function createSetupStore(client: SetupClient = defaultClient()) {
  return defineStore("setup", {
    state: () => ({
      status: undefined as SetupStatus | undefined,
      candidates: [] as SetupDiscoveryResponse["candidates"],
      callmeshUrl: "https://callmesh.tmmarc.org",
      loading: false,
      discovering: false,
      errorCode: undefined as string | undefined,
      initialized: false,
    }),
    getters: {
      required: (state) => state.status?.setupRequired ?? true,
      phase: (state) => state.status?.phase ?? "uninitialized",
    },
    actions: {
      async refresh() {
        if (this.loading) {
          return this.status;
        }
        this.loading = true;
        try {
          this.status = await client.setup.status();
          this.errorCode = undefined;
          this.initialized = true;
          return this.status;
        } catch (error) {
          this.errorCode = isGatewayApiError(error)
            ? error.code
            : "AGENT_SETUP_UNAVAILABLE";
          throw error;
        } finally {
          this.loading = false;
        }
      },
      async discover() {
        if (this.discovering) {
          return this.candidates;
        }
        this.discovering = true;
        try {
          const result = await client.setup.discovery();
          this.candidates = result.candidates;
          this.callmeshUrl = result.callmeshUrl;
          this.errorCode = undefined;
          return this.candidates;
        } catch (error) {
          this.errorCode = isGatewayApiError(error)
            ? error.code
            : "AGENT_SETUP_DISCOVERY_FAILED";
          throw error;
        } finally {
          this.discovering = false;
        }
      },
      async acceptTerms() {
        this.loading = true;
        try {
          this.status = await client.setup.acceptTerms("cmclient-2.0-terms-v1");
          this.errorCode = undefined;
          return this.status;
        } catch (error) {
          this.errorCode = isGatewayApiError(error)
            ? error.code
            : "AGENT_SETUP_TERMS_FAILED";
          throw error;
        } finally {
          this.loading = false;
        }
      },
      async configure(request: SetupConfigureRequest) {
        this.loading = true;
        try {
          // The request is passed straight to the Agent and never copied into
          // this store, localStorage, telemetry, or URL state.
          this.status = await client.setup.configure(request);
          this.errorCode = undefined;
          return this.status;
        } catch (error) {
          this.errorCode = isGatewayApiError(error)
            ? error.code
            : "AGENT_SETUP_CONFIGURATION_FAILED";
          throw error;
        } finally {
          this.loading = false;
        }
      },
      async reset() {
        this.loading = true;
        try {
          this.status = await client.setup.reset("operational_reset");
          this.errorCode = undefined;
          return this.status;
        } catch (error) {
          this.errorCode = isGatewayApiError(error)
            ? error.code
            : "AGENT_SETUP_RESET_FAILED";
          throw error;
        } finally {
          this.loading = false;
        }
      },
    },
  });
}

export const useSetupStore = createSetupStore();
