import {
  isGatewayApiError,
  type AgentCmCloudApi,
  type GatewayApiClient,
} from "@cmclient/api-client";
import type {
  CMCloudAccountProjection,
  CMCloudEnrollmentStatus,
} from "@cmclient/contracts";
import { defineStore } from "pinia";

import { managementApi } from "../management-api";

export interface CMCloudClient {
  cmcloud: Pick<AgentCmCloudApi, "status" | "enroll" | "accountProjection">;
}

export const CMCLOUD_PAIRING_CODE_PATTERN = /^[A-Za-z0-9_-]{16,512}$/;
const PROJECTION_ERROR_CODES = new Set([
  "ACCOUNT_PROJECTION_UNAVAILABLE",
  "ACCOUNT_PROJECTION_STALE",
  "ACCOUNT_PROJECTION_AMBIGUOUS",
]);

type ProjectionStatus =
  "idle" | "loading" | "ready" | "degraded" | "unavailable";

function projectionErrorCode(error: unknown): string {
  if (isGatewayApiError(error) && PROJECTION_ERROR_CODES.has(error.code)) {
    return error.code;
  }
  return "ACCOUNT_PROJECTION_UNAVAILABLE";
}

export function createCMCloudStore(client: CMCloudClient = managementApi) {
  return defineStore("cmcloud", {
    state: () => ({
      loading: false,
      enrolling: false,
      errorCode: undefined as string | undefined,
      status: undefined as CMCloudEnrollmentStatus | undefined,
      pairingCode: "",
      projection: undefined as CMCloudAccountProjection | undefined,
      projectionStatus: "idle" as ProjectionStatus,
      projectionErrorCode: undefined as string | undefined,
    }),
    actions: {
      async refresh() {
        if (this.loading || this.enrolling) {
          return;
        }
        this.loading = true;
        this.projectionStatus = "loading";
        const [statusResult, projectionResult] = await Promise.allSettled([
          client.cmcloud.status(),
          client.cmcloud.accountProjection(),
        ]);
        if (statusResult.status === "fulfilled") {
          this.status = statusResult.value;
          this.errorCode = undefined;
        } else {
          this.errorCode = isGatewayApiError(statusResult.reason)
            ? statusResult.reason.code
            : "GATEWAY_NETWORK_UNAVAILABLE";
        }
        if (projectionResult.status === "fulfilled") {
          this.projection = projectionResult.value;
          this.projectionErrorCode = projectionResult.value.errorState
            ? PROJECTION_ERROR_CODES.has(projectionResult.value.errorState.code)
              ? projectionResult.value.errorState.code
              : "ACCOUNT_PROJECTION_UNAVAILABLE"
            : undefined;
          this.projectionStatus = projectionResult.value.errorState
            ? "degraded"
            : "ready";
        } else {
          this.projection = undefined;
          this.projectionErrorCode = projectionErrorCode(
            projectionResult.reason,
          );
          this.projectionStatus = "unavailable";
        }
        this.loading = false;
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
  cmcloud: Pick<
    GatewayApiClient["cmcloud"],
    "status" | "enroll" | "accountProjection"
  >;
};
