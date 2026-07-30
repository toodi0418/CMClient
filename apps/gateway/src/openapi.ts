import { createHash } from "node:crypto";

import type { FastifyInstance } from "fastify";
import "@fastify/swagger";

type JsonPrimitive = boolean | null | number | string;
type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };

const AGENT_SESSION_SECURITY = [{ agentBrowserSession: [] }];
const AGENT_ERROR_RESPONSE = {
  description: "Stable Agent code-only error",
  content: {
    "application/json": {
      schema: {
        type: "object",
        additionalProperties: false,
        required: ["code"],
        properties: {
          code: { type: "string", pattern: "^[A-Z][A-Z0-9_]{0,127}$" },
        },
      },
    },
  },
};
const LAST_EVENT_ID_PARAMETER = {
  in: "header",
  name: "Last-Event-ID",
  required: false,
  schema: {
    type: "string",
    minLength: 1,
    maxLength: 128,
    pattern: "^[A-Za-z0-9._:-]+$",
  },
};
const CSRF_PARAMETER = {
  in: "header",
  name: "x-csrf-token",
  required: true,
  schema: { type: "string", minLength: 1, maxLength: 256 },
};

function agentJsonResponse(schema: string): JsonValue {
  return {
    description: "Agent-owned projection",
    content: {
      "application/json": {
        schema: { $ref: `#/components/schemas/${schema}` },
      },
    },
  };
}

function agentSseOperation(eventSchema: string): JsonValue {
  return {
    security: AGENT_SESSION_SECURITY,
    parameters: [LAST_EVENT_ID_PARAMETER],
    "x-cmclient-event-schema": {
      $ref: `#/components/schemas/${eventSchema}`,
    },
    responses: {
      "200": {
        description: "Agent-owned Server-Sent Events stream",
        content: {
          "text/event-stream": { schema: { type: "string" } },
        },
      },
      "400": AGENT_ERROR_RESPONSE,
      "503": AGENT_ERROR_RESPONSE,
    },
  };
}

const AGENT_MANAGEMENT_PATHS: Record<string, JsonValue> = {
  "/api/v1/setup/status": {
    get: {
      security: AGENT_SESSION_SECURITY,
      responses: {
        "200": agentJsonResponse("SetupStatus"),
        "503": AGENT_ERROR_RESPONSE,
      },
    },
  },
  "/api/v1/setup/discovery": {
    get: {
      security: AGENT_SESSION_SECURITY,
      responses: {
        "200": agentJsonResponse("SetupDiscoveryResponse"),
        "503": AGENT_ERROR_RESPONSE,
      },
    },
  },
  "/api/v1/setup/configure": {
    post: {
      security: AGENT_SESSION_SECURITY,
      "x-cmclient-error-schema": {
        $ref: "#/components/schemas/SetupErrorCode",
      },
      parameters: [CSRF_PARAMETER],
      requestBody: {
        required: true,
        content: {
          "application/json": {
            schema: { $ref: "#/components/schemas/SetupConfigureRequest" },
          },
        },
      },
      responses: {
        "200": agentJsonResponse("SetupStatus"),
        "400": AGENT_ERROR_RESPONSE,
        "401": AGENT_ERROR_RESPONSE,
        "408": AGENT_ERROR_RESPONSE,
        "409": AGENT_ERROR_RESPONSE,
        "503": AGENT_ERROR_RESPONSE,
      },
    },
  },
  "/api/v1/setup/terms": {
    post: {
      security: AGENT_SESSION_SECURITY,
      parameters: [CSRF_PARAMETER],
      requestBody: {
        required: true,
        content: {
          "application/json": {
            schema: { $ref: "#/components/schemas/SetupAcceptTermsRequest" },
          },
        },
      },
      responses: {
        "200": agentJsonResponse("SetupStatus"),
        "400": AGENT_ERROR_RESPONSE,
        "409": AGENT_ERROR_RESPONSE,
        "503": AGENT_ERROR_RESPONSE,
      },
    },
  },
  "/api/v1/setup/reset": {
    post: {
      security: AGENT_SESSION_SECURITY,
      parameters: [CSRF_PARAMETER],
      requestBody: {
        required: true,
        content: {
          "application/json": {
            schema: { $ref: "#/components/schemas/SetupResetRequest" },
          },
        },
      },
      responses: {
        "200": agentJsonResponse("SetupStatus"),
        "400": AGENT_ERROR_RESPONSE,
        "409": AGENT_ERROR_RESPONSE,
        "503": AGENT_ERROR_RESPONSE,
      },
    },
  },
  "/api/v1/reset/operational": {
    post: {
      security: AGENT_SESSION_SECURITY,
      parameters: [CSRF_PARAMETER],
      requestBody: {
        required: true,
        content: {
          "application/json": {
            schema: { $ref: "#/components/schemas/SetupResetRequest" },
          },
        },
      },
      responses: {
        "200": agentJsonResponse("SetupStatus"),
        "400": AGENT_ERROR_RESPONSE,
        "409": AGENT_ERROR_RESPONSE,
        "503": AGENT_ERROR_RESPONSE,
      },
    },
  },
  "/api/v1/setup/events": {
    get: agentSseOperation("AgentSetupEvent"),
  },
  "/api/v1/lifecycle/status": {
    get: {
      security: AGENT_SESSION_SECURITY,
      responses: {
        "200": agentJsonResponse("AgentLifecycleStatus"),
        "503": AGENT_ERROR_RESPONSE,
      },
    },
  },
  "/api/v1/lifecycle/events": {
    get: agentSseOperation("AgentLifecycleEvent"),
  },
  "/api/v1/updates": {
    get: {
      security: AGENT_SESSION_SECURITY,
      responses: {
        "200": agentJsonResponse("UpdateControlStatus"),
        "503": AGENT_ERROR_RESPONSE,
      },
    },
  },
  "/api/v1/updates/events": {
    get: agentSseOperation("AgentUpdateEvent"),
  },
};

export interface GatewayOpenApiDocument {
  [key: string]: unknown;
  components?: {
    schemas?: Record<string, unknown>;
    [key: string]: unknown;
  };
  info: { title: string; version: string; [key: string]: unknown };
  openapi: string;
  paths: Record<string, unknown>;
}

export async function createGatewayOpenApiDocument(
  app: FastifyInstance,
): Promise<GatewayOpenApiDocument> {
  await app.ready();
  const generated = app.swagger() as unknown as GatewayOpenApiDocument;
  const paths = {
    ...generated.paths,
    ...AGENT_MANAGEMENT_PATHS,
  } as Record<string, JsonValue>;
  for (const path of ["/api/v1/events", "/api/v1/jobs/{jobId}/events"]) {
    paths[path] = bindGatewaySseContract(paths[path]);
  }
  const aggregate = {
    ...generated,
    info: {
      ...generated.info,
      title: "CMClient Management API",
    },
    paths,
  };
  return sortJsonValue(aggregate as JsonValue) as GatewayOpenApiDocument;
}

function bindGatewaySseContract(pathItem: JsonValue | undefined): JsonValue {
  if (!isJsonObject(pathItem) || !isJsonObject(pathItem.get)) {
    throw new Error("GATEWAY_SSE_OPENAPI_PATH_MISSING");
  }
  const responses = isJsonObject(pathItem.get.responses)
    ? pathItem.get.responses
    : {};
  return {
    ...pathItem,
    get: {
      ...pathItem.get,
      "x-cmclient-event-schema": {
        $ref: "#/components/schemas/DomainEvent",
      },
      responses: {
        ...responses,
        "200": {
          description: "Gateway-owned Server-Sent Events stream",
          content: {
            "text/event-stream": { schema: { type: "string" } },
          },
        },
      },
    },
  };
}

function isJsonObject(
  value: JsonValue | undefined,
): value is { [key: string]: JsonValue } {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

export function gatewayOpenApiDocumentDigest(
  document: GatewayOpenApiDocument,
): string {
  return createHash("sha256")
    .update(JSON.stringify(sortJsonValue(document as JsonValue)))
    .digest("hex");
}

function sortJsonValue(value: JsonValue): JsonValue {
  if (Array.isArray(value)) {
    return value.map(sortJsonValue);
  }
  if (value === null || typeof value !== "object") {
    return value;
  }
  return Object.fromEntries(
    Object.entries(value)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, nested]) => [key, sortJsonValue(nested)]),
  );
}
