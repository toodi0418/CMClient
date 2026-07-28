import {
  GatewayApiError,
  isGatewayApiError,
  type AgentSetupApi,
} from "@cmclient/api-client";
import type {
  AgentSetupEvent,
  SetupConfigureRequest,
  SetupDiscoveryResponse,
  SetupStatus,
} from "@cmclient/contracts";
import {
  AGENT_EVENT_STREAM_PATHS,
  AgentSetupEventSchema,
} from "@cmclient/contracts";
import { Value } from "@sinclair/typebox/value";
import { defineStore } from "pinia";

import { managementApi } from "../management-api";

export interface SetupEventListener {
  onStatus(status: SetupStatus): void;
  onError(code: string): void;
}

export interface SetupClient {
  setup: AgentSetupApi;
  subscribe?: (listener: SetupEventListener) => () => void;
}

export type SetupAdmission = "checking" | "ready" | "required" | "unavailable";

function defaultClient(): SetupClient {
  const events = new BrowserSetupEventClient();
  return {
    setup: managementApi.setup,
    subscribe: (listener) => events.subscribe(listener),
  };
}

export interface BrowserSetupEventClientOptions {
  eventSource?: typeof EventSource;
  eventsUrl?: string;
}

export class BrowserSetupEventClient {
  private readonly eventSource: typeof EventSource | undefined;
  private readonly eventsUrl: string;

  constructor(options: BrowserSetupEventClientOptions = {}) {
    this.eventSource = options.eventSource ?? globalThis.EventSource;
    this.eventsUrl = validateSetupEventsUrl(
      options.eventsUrl ?? AGENT_EVENT_STREAM_PATHS.setup,
    );
  }

  subscribe(listener: SetupEventListener): () => void {
    if (!this.eventSource) {
      listener.onError("AGENT_SETUP_EVENT_STREAM_UNAVAILABLE");
      return () => {};
    }
    const source = new this.eventSource(this.eventsUrl);
    let closed = false;
    const onStatus = (event: Event) => {
      let payload: unknown;
      try {
        payload = JSON.parse((event as MessageEvent<string>).data);
      } catch {
        listener.onError("AGENT_SETUP_EVENT_INVALID");
        return;
      }
      if (!Value.Check(AgentSetupEventSchema, payload)) {
        listener.onError("AGENT_SETUP_EVENT_INVALID");
        return;
      }
      listener.onStatus((payload as AgentSetupEvent).payload);
    };
    const onError = () => {
      if (!closed) {
        listener.onError("AGENT_SETUP_EVENT_STREAM_UNAVAILABLE");
      }
    };
    source.addEventListener("setup.status", onStatus);
    source.addEventListener("error", onError);

    return () => {
      closed = true;
      source.removeEventListener("setup.status", onStatus);
      source.removeEventListener("error", onError);
      source.close();
    };
  }
}

/**
 * Keeps setup progress in the Agent, while deliberately keeping the API key
 * out of Pinia state and browser persistence.
 */
export function createSetupStore(client: SetupClient = defaultClient()) {
  let activeRefresh: Promise<SetupStatus | undefined> | undefined;

  return defineStore("setup", {
    state: () => ({
      status: undefined as SetupStatus | undefined,
      candidates: [] as SetupDiscoveryResponse["candidates"],
      callmeshUrl: "https://callmesh.tmmarc.org",
      loading: false,
      discovering: false,
      errorCode: undefined as string | undefined,
      initialized: false,
      admission: "checking" as SetupAdmission,
      connection: "idle" as
        "idle" | "connecting" | "open" | "reconnecting" | "stopped",
      started: false,
      unsubscribe: undefined as (() => void) | undefined,
    }),
    getters: {
      required: (state) => state.admission === "required",
      phase: (state) => state.status?.phase ?? "uninitialized",
    },
    actions: {
      applyStatus(status: SetupStatus) {
        this.status = status;
        this.initialized = true;
        this.admission = status.setupRequired ? "required" : "ready";
      },
      async start() {
        if (this.started) {
          return;
        }
        this.started = true;
        this.connection = "connecting";
        try {
          await this.refresh();
        } finally {
          if (this.started) {
            this.unsubscribe = client.subscribe?.({
              onStatus: (status) => {
                this.applyStatus(status);
                this.errorCode = undefined;
                this.connection = "open";
              },
              onError: (code) => {
                // Keep a recent REST admission failure visible while the SSE
                // transport reconnects, so an unrelated stream error does not
                // replace the actionable reason shown to the operator.
                if (!this.errorCode) {
                  this.errorCode = code;
                }
                this.connection = "reconnecting";
                if (!this.status) {
                  this.admission = "unavailable";
                }
              },
            });
          }
        }
      },
      stop() {
        this.unsubscribe?.();
        this.unsubscribe = undefined;
        this.started = false;
        this.connection = "stopped";
      },
      async refresh() {
        if (activeRefresh) {
          return activeRefresh;
        }
        this.loading = true;
        activeRefresh = client.setup
          .status()
          .then((status) => {
            this.applyStatus(status);
            this.errorCode = undefined;
            return this.status;
          })
          .catch((error) => {
            this.errorCode = isGatewayApiError(error)
              ? error.code
              : "AGENT_SETUP_UNAVAILABLE";
            if (!this.status) {
              this.admission = "unavailable";
            }
            throw error;
          })
          .finally(() => {
            this.loading = false;
            activeRefresh = undefined;
          });
        return activeRefresh;
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
          const termsVersion = this.status?.currentTermsVersion;
          if (!termsVersion) {
            throw new GatewayApiError({
              code: "AGENT_SETUP_STATUS_REQUIRED",
            });
          }
          this.applyStatus(await client.setup.acceptTerms(termsVersion));
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
          this.applyStatus(await client.setup.configure(request));
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
          this.applyStatus(await client.setup.reset("operational_reset"));
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

function validateSetupEventsUrl(url: string): string {
  const normalized = url.trim();
  if (!normalized.startsWith("/")) {
    throw new TypeError("Setup event URL must be an absolute path");
  }
  return normalized;
}
