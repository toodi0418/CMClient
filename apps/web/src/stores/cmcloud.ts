import {
  isGatewayApiError,
  type AgentCmCloudApi,
  type GatewayApiClient,
} from "@cmclient/api-client";
import type { CMCloudEnrollmentStatus } from "@cmclient/contracts";
import { defineStore } from "pinia";

import { managementApi } from "../management-api";

export interface CMCloudClient {
  cmcloud: Pick<AgentCmCloudApi, "status" | "enroll">;
}

export const CMCLOUD_PAIRING_CODE_PATTERN = /^[A-Za-z0-9_-]{16,512}$/;

export function createCMCloudStore(client: CMCloudClient = managementApi) {
  return defineStore("cmcloud", {
    state: () => ({
      loading: false,
      enrolling: false,
      errorCode: undefined as string | undefined,
      status: undefined as CMCloudEnrollmentStatus | undefined,
      pairingCode: "",
    }),
    actions: {
      async refresh() {
        if (this.loading || this.enrolling) {
          return;
        }
        this.loading = true;
        try {
          this.status = await client.cmcloud.status();
          this.errorCode = undefined;
        } catch (error) {
          this.errorCode = isGatewayApiError(error)
            ? error.code
            : "GATEWAY_NETWORK_UNAVAILABLE";
        } finally {
          this.loading = false;
        }
      },
      async enroll() {
        if (this.loading || this.enrolling) {
          return;
        }
        const pairingCode = this.pairingCode.trim();
        this.pairingCode = "";
        if (!CMCLOUD_PAIRING_CODE_PATTERN.test(pairingCode)) {
          this.errorCode = "CMCLOUD_ENROLLMENT_REQUEST_INVALID";
          return;
        }
        this.enrolling = true;
        try {
          this.status = await client.cmcloud.enroll({ pairingCode });
          this.errorCode = undefined;
        } catch (error) {
          this.errorCode = isGatewayApiError(error)
            ? error.code
            : "GATEWAY_NETWORK_UNAVAILABLE";
        } finally {
          this.enrolling = false;
        }
      },
      setPairingCode(value: string) {
        this.pairingCode = value.replace(/[^A-Za-z0-9_-]/g, "");
      },
    },
  });
}

export const useCMCloudStore = createCMCloudStore();

export type CMCloudApiClient = CMCloudClient & {
  cmcloud: Pick<GatewayApiClient["cmcloud"], "status" | "enroll">;
};
