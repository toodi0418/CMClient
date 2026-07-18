import { GatewayApiClient, isGatewayApiError } from "@cmclient/api-client";
import type { ProxyStatus } from "@cmclient/contracts";
import { defineStore } from "pinia";

export interface ProxyClient {
  proxy: Pick<GatewayApiClient["proxy"], "status">;
}

export function createProxyStore(client: ProxyClient = new GatewayApiClient()) {
  return defineStore("proxy", {
    state: () => ({
      errorCode: undefined as string | undefined,
      loading: false,
      status: undefined as ProxyStatus | undefined,
    }),
    actions: {
      async refresh() {
        if (this.loading) {
          return;
        }
        this.loading = true;
        try {
          this.status = await client.proxy.status();
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

export const useProxyStore = createProxyStore();
