import {
  UpdateControlStatusSchema,
  type UpdateControlStatus,
} from "@cmclient/contracts";
import { Value } from "@sinclair/typebox/value";
import { defineStore } from "pinia";

export const DEFAULT_UPDATE_STATUS_URL = "/api/v1/updates";
export const DEFAULT_UPDATE_EVENTS_URL = "/api/v1/updates/events";

export type UpdateConnectionState =
  "idle" | "connecting" | "open" | "reconnecting" | "stopped";

export class UpdateApiError extends Error {
  readonly code: string;

  constructor(code: string) {
    super(code);
    this.name = "UpdateApiError";
    this.code = code;
  }
}

export interface UpdateEventListener {
  onStatus(status: UpdateControlStatus): void;
  onError(code: string): void;
}

export interface UpdatesClient {
  status(): Promise<UpdateControlStatus>;
  subscribe(listener: UpdateEventListener): () => void;
}

export interface AgentUpdateClientOptions {
  fetch?: typeof fetch;
  eventSource?: typeof EventSource;
  statusUrl?: string;
  eventsUrl?: string;
}

export class AgentUpdateClient implements UpdatesClient {
  private readonly fetchImplementation: typeof fetch;
  private readonly eventSource: typeof EventSource;
  private readonly statusUrl: string;
  private readonly eventsUrl: string;

  constructor(options: AgentUpdateClientOptions = {}) {
    this.fetchImplementation =
      options.fetch ?? ((input, init) => globalThis.fetch(input, init));
    this.eventSource = options.eventSource ?? globalThis.EventSource;
    this.statusUrl = validateUpdateUrl(
      options.statusUrl ?? DEFAULT_UPDATE_STATUS_URL,
    );
    this.eventsUrl = validateUpdateUrl(
      options.eventsUrl ?? DEFAULT_UPDATE_EVENTS_URL,
    );
  }

  async status(): Promise<UpdateControlStatus> {
    let response: Response;
    try {
      response = await this.fetchImplementation(this.statusUrl, {
        headers: { accept: "application/json" },
      });
    } catch {
      throw new UpdateApiError("UPDATE_AGENT_UNAVAILABLE");
    }
    if (!response.ok) {
      throw new UpdateApiError(await responseErrorCode(response));
    }
    let payload: unknown;
    try {
      payload = await response.json();
    } catch {
      throw new UpdateApiError("UPDATE_RESPONSE_INVALID");
    }
    if (!Value.Check(UpdateControlStatusSchema, payload)) {
      throw new UpdateApiError("UPDATE_RESPONSE_INVALID");
    }
    return payload as UpdateControlStatus;
  }

  subscribe(listener: UpdateEventListener): () => void {
    if (!this.eventSource) {
      listener.onError("UPDATE_EVENT_STREAM_UNAVAILABLE");
      return () => {};
    }
    const source = new this.eventSource(this.eventsUrl);
    let closed = false;
    const onStatus = (event: Event) => {
      try {
        const payload: unknown = JSON.parse(
          (event as MessageEvent<string>).data,
        );
        if (!Value.Check(UpdateControlStatusSchema, payload)) {
          throw new Error("invalid update event");
        }
        listener.onStatus(payload as UpdateControlStatus);
      } catch {
        listener.onError("UPDATE_EVENT_INVALID");
      }
    };
    const onError = () => {
      if (!closed) {
        listener.onError("UPDATE_EVENT_STREAM_UNAVAILABLE");
      }
    };
    source.addEventListener("update.status_changed", onStatus);
    source.addEventListener("error", onError);

    return () => {
      closed = true;
      source.removeEventListener("update.status_changed", onStatus);
      source.removeEventListener("error", onError);
      source.close();
    };
  }
}

function defaultClient(): UpdatesClient {
  return new AgentUpdateClient();
}

export function createUpdatesStore(client: UpdatesClient = defaultClient()) {
  return defineStore("updates", {
    state: () => ({
      status: undefined as UpdateControlStatus | undefined,
      loading: false,
      errorCode: undefined as string | undefined,
      connection: "idle" as UpdateConnectionState,
      started: false,
      unsubscribe: undefined as (() => void) | undefined,
    }),
    actions: {
      async refresh() {
        if (this.loading) {
          return;
        }
        this.loading = true;
        try {
          this.status = await client.status();
          this.errorCode = undefined;
        } catch (error) {
          this.errorCode =
            error instanceof UpdateApiError
              ? error.code
              : "UPDATE_AGENT_UNAVAILABLE";
        } finally {
          this.loading = false;
        }
      },
      start() {
        if (this.started) {
          return;
        }
        this.started = true;
        this.connection = "connecting";
        this.unsubscribe = client.subscribe({
          onStatus: (status) => {
            this.status = status;
            this.errorCode = undefined;
            this.connection = "open";
          },
          onError: (code) => {
            this.errorCode = code;
            this.connection = "reconnecting";
          },
        });
        void this.refresh();
      },
      stop() {
        this.unsubscribe?.();
        this.unsubscribe = undefined;
        this.started = false;
        this.connection = "stopped";
      },
    },
  });
}

async function responseErrorCode(response: Response): Promise<string> {
  try {
    const payload: unknown = await response.json();
    if (
      payload &&
      typeof payload === "object" &&
      !Array.isArray(payload) &&
      "code" in payload &&
      typeof payload.code === "string" &&
      /^[A-Z0-9_]{1,128}$/.test(payload.code)
    ) {
      return payload.code;
    }
  } catch {
    // The stable fallback below intentionally hides transport details.
  }
  return "UPDATE_HTTP_" + response.status;
}

function validateUpdateUrl(url: string): string {
  const normalized = url.trim();
  if (!normalized.startsWith("/")) {
    throw new TypeError("Update API URL must be an absolute path");
  }
  return normalized;
}

export const useUpdatesStore = createUpdatesStore();
