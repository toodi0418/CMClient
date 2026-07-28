import { describe, expect, it } from "vitest";

import {
  GatewayApiClient,
  GatewayApiError,
  isGatewayApiError,
  setManagementCsrfToken,
} from "./index";

describe("gateway API client", () => {
  it("uses versioned routes, validates response data, and includes trace IDs", async () => {
    let url: string | undefined;
    let headers: Headers | undefined;
    const client = new GatewayApiClient({
      traceIdFactory: () => "trace-fixture",
      fetch: async (input, init) => {
        url = String(input);
        headers = new Headers(init?.headers);
        return jsonResponse({ status: "ok" });
      },
    });

    await expect(client.system.health()).resolves.toEqual({ status: "ok" });
    expect(url).toBe("/api/v1/system/health");
    expect(headers?.get("x-trace-id")).toBe("trace-fixture");
  });

  it("maps stable backend and transport errors without exposing prose", async () => {
    const unavailable = new GatewayApiClient({
      fetch: async () =>
        jsonResponse({ code: "GATEWAY_PROXY_UNAVAILABLE" }, 503),
    });
    const offline = new GatewayApiClient({
      fetch: async () => Promise.reject(new Error("socket detail")),
    });

    await expect(unavailable.system.health()).rejects.toMatchObject({
      code: "GATEWAY_PROXY_UNAVAILABLE",
      status: 503,
      retryable: true,
    });
    await expect(offline.system.health()).rejects.toMatchObject({
      code: "GATEWAY_NETWORK_UNAVAILABLE",
      retryable: true,
    });
  });

  it("rejects malformed success payloads and invalid job identifiers", async () => {
    const client = new GatewayApiClient({
      fetch: async () => jsonResponse({ status: "not-ok" }),
    });

    await expect(client.system.health()).rejects.toMatchObject({
      code: "GATEWAY_RESPONSE_INVALID",
    });
    await expect(client.jobs.get("bad/job")).rejects.toMatchObject({
      code: "CLIENT_INPUT_INVALID",
    });
    expect(isGatewayApiError(new GatewayApiError({ code: "FIXTURE" }))).toBe(
      true,
    );
  });

  it("reads the CallMesh overview through the versioned API", async () => {
    let url: string | undefined;
    const client = new GatewayApiClient({
      fetch: async (input) => {
        url = String(input);
        return jsonResponse({
          status: {
            state: "ready",
            updatedAt: "2026-07-18T00:00:00.000Z",
            activeMappingCount: 0,
            provisionState: "unavailable",
          },
          mappings: [],
        });
      },
    });

    await expect(client.callmesh.overview()).resolves.toMatchObject({
      status: { state: "ready", provisionState: "unavailable" },
    });
    expect(url).toBe("/api/v1/callmesh");
  });

  it("submits diagnostics with a client-generated idempotency key", async () => {
    let headers: Headers | undefined;
    let body: BodyInit | null | undefined;
    const client = new GatewayApiClient({
      traceIdFactory: () => "diagnostics-42",
      fetch: async (_input, init) => {
        headers = new Headers(init?.headers);
        body = init?.body;
        return jsonResponse({ jobId: "job-1", reused: false }, 202);
      },
    });

    await expect(client.diagnostics.integrityCheck()).resolves.toEqual({
      jobId: "job-1",
      reused: false,
    });
    expect(headers?.get("idempotency-key")).toBe("diagnostics-42");
    expect(headers?.get("content-type")).toBe("application/json");
    expect(body).toBe("{}");
  });

  it("adds the in-memory management CSRF token without persisting credentials", async () => {
    let headers: Headers | undefined;
    setManagementCsrfToken("a".repeat(32));
    const client = new GatewayApiClient({
      fetch: async (_input, init) => {
        headers = new Headers(init?.headers);
        return jsonResponse({ jobId: "job-1", reused: false }, 202);
      },
    });

    await client.diagnostics.integrityCheck();

    expect(headers?.get("x-csrf-token")).toBe("a".repeat(32));
    setManagementCsrfToken(undefined);
  });

  it("uses the Agent-owned setup status, terms, reset, and lifecycle contracts", async () => {
    const requests: Array<{ body: unknown; method: string; url: string }> = [];
    const client = new GatewayApiClient({
      fetch: async (input, init) => {
        const url = String(input);
        requests.push({
          body: init?.body ? JSON.parse(String(init.body)) : undefined,
          method: init?.method ?? "GET",
          url,
        });
        if (url.endsWith("/lifecycle/status")) {
          return jsonResponse({
            schemaVersion: 1,
            agent: "running",
            gateway: "stopped",
            managementWeb: "running",
            managementWebUrl: "http://127.0.0.1:7080",
            uptimeSeconds: 10,
            latestErrorCode: null,
          });
        }
        return jsonResponse({
          schemaVersion: 1,
          currentTermsVersion: "cmclient-2.0-terms-v1",
          phase: url.endsWith("/setup/status")
            ? "terms_required"
            : "credentials_required",
          setupRequired: true,
          termsRequired: url.endsWith("/setup/status"),
          credentialsRequired: !url.endsWith("/setup/status"),
          validating: false,
          ready: false,
          recoveryRequired: false,
          reasonCode: url.endsWith("/setup/status")
            ? "SETUP_TERMS_REQUIRED"
            : "SETUP_CREDENTIALS_REQUIRED",
        });
      },
    });

    await expect(client.setup.status()).resolves.toMatchObject({
      phase: "terms_required",
    });
    await expect(
      client.setup.acceptTerms("cmclient-2.0-terms-v1"),
    ).resolves.toMatchObject({ phase: "credentials_required" });
    await expect(
      client.setup.reset("operational_reset"),
    ).resolves.toMatchObject({ phase: "credentials_required" });
    await expect(client.lifecycle.status()).resolves.toMatchObject({
      gateway: "stopped",
    });

    expect(requests).toEqual([
      { body: undefined, method: "GET", url: "/api/v1/setup/status" },
      {
        body: { termsVersion: "cmclient-2.0-terms-v1" },
        method: "POST",
        url: "/api/v1/setup/terms",
      },
      {
        body: { confirmation: "operational_reset" },
        method: "POST",
        url: "/api/v1/setup/reset",
      },
      { body: undefined, method: "GET", url: "/api/v1/lifecycle/status" },
    ]);
  });

  it("rejects malformed setup command input before issuing a request", async () => {
    let requests = 0;
    const client = new GatewayApiClient({
      fetch: async () => {
        requests += 1;
        return jsonResponse({});
      },
    });

    await expect(
      client.setup.acceptTerms("bad terms version"),
    ).rejects.toMatchObject({ code: "CLIENT_INPUT_INVALID" });
    await expect(
      client.setup.reset("wrong" as "operational_reset"),
    ).rejects.toMatchObject({ code: "CLIENT_INPUT_INVALID" });
    expect(requests).toBe(0);
  });

  it.each([
    [401, "CALLMESH_CREDENTIAL_REJECTED", false],
    [503, "CALLMESH_UNAVAILABLE", true],
  ])(
    "preserves setup authentication error %s without putting the key in URL or errors",
    async (status, code, retryable) => {
      const apiKey = "fixture-private-setup-key";
      let requestUrl = "";
      const client = new GatewayApiClient({
        fetch: async (input) => {
          requestUrl = String(input);
          return jsonResponse({ code }, status);
        },
      });

      const error = await client.setup
        .configure({
          meshtasticHost: "172.16.8.88",
          meshtasticPort: 4403,
          callmeshApiKey: apiKey,
        })
        .catch((caught: unknown) => caught);

      expect(error).toMatchObject({ code, status, retryable });
      expect(requestUrl).toBe("/api/v1/setup/configure");
      expect(requestUrl).not.toContain(apiKey);
      expect(String(error)).not.toContain(apiKey);
    },
  );

  it("reads the privacy-safe proxy status through the versioned API", async () => {
    let url: string | undefined;
    const client = new GatewayApiClient({
      fetch: async (input) => {
        url = String(input);
        return jsonResponse(proxyStatus());
      },
    });

    await expect(client.proxy.status()).resolves.toMatchObject({
      state: "running",
      policy: { activeClients: 2, mode: "message" },
    });
    expect(url).toBe("/api/v1/proxy");
  });

  it("validates the Meshtastic and APRS runtime projections", async () => {
    const requested: string[] = [];
    const client = new GatewayApiClient({
      fetch: async (input) => {
        const url = String(input);
        requested.push(url);
        return url.endsWith("/meshtastic")
          ? jsonResponse({
              configured: true,
              meshNetworkId: "mesh-a",
              gatewayId: "gateway-a",
              connection: {
                transport: "serial",
                status: "ready",
                changedAt: "2026-07-18T00:00:00.000Z",
              },
              metrics: {
                bytesReceived: 10,
                bytesSent: 4,
                framesReceived: 2,
                framesSent: 1,
                malformedFrames: 0,
                reconnects: 0,
              },
            })
          : jsonResponse({
              configured: true,
              running: true,
              monitorStatus: "connected",
              mappedCallsigns: 2,
              pendingOutbox: 1,
              failedOutbox: 0,
              pendingStationSubmissions: 1,
              failedStationSubmissions: 0,
            });
      },
    });

    await expect(client.meshtastic.status()).resolves.toMatchObject({
      connection: { status: "ready" },
    });
    await expect(client.aprs.status()).resolves.toMatchObject({
      monitorStatus: "connected",
      mappedCallsigns: 2,
    });
    expect(requested).toEqual(["/api/v1/meshtastic", "/api/v1/aprs"]);
  });

  it("reads sanitized station delivery without accepting APRS identity or Data", async () => {
    let url: string | undefined;
    const client = new GatewayApiClient({
      fetch: async (input) => {
        url = String(input);
        return jsonResponse({
          items: [
            {
              id: "aprs-igate-00000000-0000-4000-8000-000000000001",
              packetKind: "telemetry-data",
              deliveryStatus: "observer_confirmed",
              attemptedAt: "2026-07-18T00:00:00.000Z",
              submittedAt: "2026-07-18T00:00:01.000Z",
              observerConfirmedAt: "2026-07-18T00:00:02.000Z",
              updatedAt: "2026-07-18T00:00:02.000Z",
              observationExpiresAt: "2026-07-18T03:00:00.000Z",
            },
          ],
        });
      },
    });

    const projection = await client.aprs.stationSubmissions();
    expect(url).toBe("/api/v1/aprs/station-submissions");
    expect(projection.items[0]).toMatchObject({
      packetKind: "telemetry-data",
      deliveryStatus: "observer_confirmed",
    });
    expect(projection.items[0]).not.toHaveProperty("callsign");
    expect(projection.items[0]).not.toHaveProperty("info");
  });

  it("encodes bounded telemetry range queries and rejects ambiguous nodes", async () => {
    let url: string | undefined;
    const client = new GatewayApiClient({
      fetch: async (input) => {
        url = String(input);
        return jsonResponse({ items: [] });
      },
    });

    await expect(
      client.domain.telemetry({
        meshNetworkId: "mesh-a",
        nodeNum: 42,
        metricKind: "deviceMetrics",
        from: "2026-07-18T00:00:00Z",
        to: "2026-07-18T00:00:00.500Z",
        limit: 25,
      }),
    ).resolves.toEqual({ items: [] });
    expect(url).toContain("/api/v1/telemetry?");
    expect(url).toContain("meshNetworkId=mesh-a");
    expect(url).toContain("nodeNum=42");
    expect(url).toContain("from=2026-07-18T00%3A00%3A00.000Z");
    expect(url).toContain("to=2026-07-18T00%3A00%3A00.500Z");
    expect(() => client.domain.telemetry({ nodeNum: 42 })).toThrow(
      expect.objectContaining({ code: "CLIENT_INPUT_INVALID" }),
    );
  });
});

function jsonResponse(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function proxyStatus() {
  return {
    state: "running",
    listener: { host: "127.0.0.1", port: 4403 },
    policy: {
      activeClients: 2,
      allowLan: false,
      allowedAddressCount: 0,
      maxClients: 16,
      maxWritesPerMinute: 120,
      mode: "message",
    },
    queue: {
      broadcastAccepted: 4,
      broadcastDropped: 0,
      broadcastFrames: 2,
      directAccepted: 1,
      directDropped: 0,
      pendingCorrelations: 0,
      queuedWrites: 0,
      writing: false,
    },
    recentAudit: [],
    upstream: {
      configFrameCount: 1,
      metrics: {
        bytesReceived: 10,
        bytesSent: 8,
        framesReceived: 2,
        framesSent: 1,
        malformedFrames: 0,
        reconnects: 0,
      },
      state: {
        changedAt: "2026-07-18T00:00:00.000Z",
        status: "ready",
        transport: "tcp",
      },
    },
  };
}
