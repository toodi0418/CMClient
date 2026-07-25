import { describe, expect, it } from "vitest";

import { createGatewayApp } from "./app";
import {
  createGatewayOpenApiDocument,
  gatewayOpenApiDocumentDigest,
} from "./openapi";
import { MemoryLogger } from "./observability";

describe("Gateway OpenAPI contract", () => {
  it("is deterministic and binds Gateway routes plus Agent-owned schemas", async () => {
    const capability = "c".repeat(64);
    const app = createGatewayApp({ capability }, new MemoryLogger());

    const document = await createGatewayOpenApiDocument(app);
    const second = await createGatewayOpenApiDocument(app);

    expect(document).toEqual(second);
    expect(document.openapi).toBe("3.1.0");
    expect(Object.keys(document.paths).sort()).toEqual([
      "/api/v1/aprs",
      "/api/v1/aprs/outbox",
      "/api/v1/aprs/station-submissions",
      "/api/v1/backups",
      "/api/v1/callmesh",
      "/api/v1/diagnostics/integrity-check",
      "/api/v1/events",
      "/api/v1/events/recent",
      "/api/v1/jobs/{jobId}",
      "/api/v1/jobs/{jobId}/cancel",
      "/api/v1/jobs/{jobId}/events",
      "/api/v1/lifecycle/events",
      "/api/v1/lifecycle/status",
      "/api/v1/meshtastic",
      "/api/v1/messages",
      "/api/v1/nodes",
      "/api/v1/positions",
      "/api/v1/proxy",
      "/api/v1/setup/events",
      "/api/v1/setup/reset",
      "/api/v1/setup/status",
      "/api/v1/setup/terms",
      "/api/v1/system/capabilities",
      "/api/v1/system/health",
      "/api/v1/system/status",
      "/api/v1/system/version",
      "/api/v1/telemetry",
      "/api/v1/updates",
      "/api/v1/updates/events",
    ]);
    expect(Object.keys(document.components?.schemas ?? {}).sort()).toEqual([
      "AgentEvent",
      "AgentGatewayLifecycleState",
      "AgentLifecycleEvent",
      "AgentLifecycleStatus",
      "AgentSetupEvent",
      "AgentUpdateEvent",
      "DomainEvent",
      "SetupAcceptTermsRequest",
      "SetupPhase",
      "SetupResetRequest",
      "SetupStatus",
      "UpdateControlStatus",
    ]);
    expect(document.paths["/api/v1/setup/terms"]).toMatchObject({
      post: {
        security: [{ agentBrowserSession: [] }],
        requestBody: {
          content: {
            "application/json": {
              schema: {
                $ref: "#/components/schemas/SetupAcceptTermsRequest",
              },
            },
          },
        },
      },
    });
    expect(document.paths["/api/v1/lifecycle/events"]).toMatchObject({
      get: {
        parameters: [{ name: "Last-Event-ID" }],
        "x-cmclient-event-schema": {
          $ref: "#/components/schemas/AgentLifecycleEvent",
        },
      },
    });
    expect(document.paths["/api/v1/setup/events"]).toMatchObject({
      get: {
        "x-cmclient-event-schema": {
          $ref: "#/components/schemas/AgentSetupEvent",
        },
      },
    });
    expect(document.paths["/api/v1/updates/events"]).toMatchObject({
      get: {
        "x-cmclient-event-schema": {
          $ref: "#/components/schemas/AgentUpdateEvent",
        },
      },
    });
    expect(document.paths["/api/v1/events"]).toMatchObject({
      get: {
        parameters: [{ in: "header", name: "last-event-id" }],
        responses: {
          "200": { content: { "text/event-stream": {} } },
        },
        "x-cmclient-event-schema": {
          $ref: "#/components/schemas/DomainEvent",
        },
      },
    });
    expect(document.paths["/api/v1/diagnostics/integrity-check"]).toMatchObject(
      {
        post: {
          parameters: [{ in: "header", name: "idempotency-key" }],
        },
      },
    );
    expect(JSON.stringify(document)).not.toContain(capability);
    expect(gatewayOpenApiDocumentDigest(document)).toBe(
      "6c19501f8ad259113993ea3e30949b69f650bb7df5c7e647e5a8889ceb2a3c98",
    );

    await app.close();
  });
});
