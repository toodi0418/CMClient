import { once } from "node:events";
import { createServer, type IncomingMessage, type Server } from "node:http";
import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

import {
  buildCallMeshAgent,
  callMeshOptionsFromEnvironment,
  CallMeshClient,
} from "./callmesh";
import { GatewayDatabase } from "./persistence/database";

const recordedMappings = JSON.parse(
  readFileSync(
    new URL("../test/fixtures/callmesh/mappings.json", import.meta.url),
    "utf8",
  ),
) as { hash: string; items: unknown[] };

const provision = {
  callsign_base: "N0CALL",
  ssid: -7,
  symbol_table: "/",
  symbol_code: ">",
  symbol_overlay: null,
  comment: "Sanitized fixture",
};

describe("CallMesh client", () => {
  it("builds the Legacy-compatible client agent without shelling out", () => {
    expect(buildCallMeshAgent("2.0.0-rc.1", "darwin", "arm64")).toBe(
      "callmesh-client/2.0.0-rc.1 (macOS; arm64)",
    );
    expect(buildCallMeshAgent("2.0.0", "win32", "x64")).toBe(
      "callmesh-client/2.0.0 (Windows NT; x64)",
    );
    expect(
      callMeshOptionsFromEnvironment(
        {
          CMCLIENT_CALLMESH_API_KEY: "fixture-api-key",
          CMCLIENT_MESH_NETWORK_ID: "fixture-network",
        },
        "2.0.0-rc.1",
      ),
    ).toEqual({
      apiKey: "fixture-api-key",
      agent: expect.stringMatching(/^callmesh-client\/2\.0\.0-rc\.1 \(/),
      meshNetworkId: "fixture-network",
    });
  });

  it("uses the Legacy heartbeat/mappings contract and conditionally reuses the durable snapshot", async () => {
    const requests: CapturedRequest[] = [];
    let heartbeatCount = 0;
    const server = createServer((request, response) => {
      void captureRequest(request).then((captured) => {
        requests.push(captured);
        response.setHeader("content-type", "application/json");
        if (request.url === "/api/v1/client/heartbeat") {
          heartbeatCount += 1;
          response.end(
            JSON.stringify(
              heartbeat({ needs_update: heartbeatCount === 1, provision }),
            ),
          );
          return;
        }
        if (request.url === "/api/v1/client/mappings") {
          response.end(JSON.stringify(recordedMappings));
          return;
        }
        response.statusCode = 404;
        response.end(JSON.stringify({ error: "unexpected fixture path" }));
      });
    });
    const baseUrl = await listen(server);
    const database = new GatewayDatabase(":memory:");
    let now = "2026-07-18T00:00:30.000Z";
    const client = new CallMeshClient(
      {
        baseUrl,
        apiKey: "fixture-api-key",
        agent: "cmclient-gateway/2.0.0-rc.1 (fixture; arm64)",
        meshNetworkId: "fixture-network",
        clock: () => new Date(now),
      },
      database.callmeshMappings,
    );

    const first = await client.synchronize();
    now = "2026-07-18T00:02:30.000Z";
    const second = await client.synchronize();

    expect(first).toEqual({
      status: {
        state: "ready",
        updatedAt: "2026-07-18T00:00:30.000Z",
        activeMappingVersion: recordedMappings.hash,
        activeMappingCount: 1,
        activeMappingHash: recordedMappings.hash,
        provisionState: "valid",
        lastServerTime: "2026-07-18T00:00:00.000Z",
      },
      mappings: [normalizedMapping()],
    });
    expect(second).toEqual({
      ...first,
      status: {
        ...first.status,
        updatedAt: "2026-07-18T00:02:30.000Z",
      },
    });
    expect(client.getProvision()).toEqual({
      callsignBase: "N0CALL",
      ssid: -7,
      symbolTable: "/",
      symbolCode: ">",
      symbolOverlay: null,
      comment: "Sanitized fixture",
    });
    expect(client.getAprsState()).toEqual({
      mappings: [normalizedMapping()],
      mappingsFingerprint: expect.stringMatching(/^[a-f0-9]{64}$/),
      provision: {
        callsignBase: "N0CALL",
        ssid: -7,
        symbolTable: "/",
        symbolCode: ">",
        symbolOverlay: null,
        comment: "Sanitized fixture",
      },
      provisionFingerprint: expect.stringMatching(/^[a-f0-9]{64}$/),
    });
    expect(JSON.stringify(first)).not.toContain("Sanitized fixture");
    expect(JSON.stringify(first)).not.toContain("callsignBase");
    expect(JSON.stringify(first)).not.toContain("passcode");
    expect(JSON.stringify(first)).not.toContain("provisionFingerprint");
    expect(database.callmeshMappings.loadSnapshot()).toMatchObject({
      active: true,
      mappingHash: recordedMappings.hash,
      acceptedServerTime: "2026-07-18T00:00:00.000Z",
      lastHeartbeatAt: "2026-07-18T00:00:30.000Z",
      provisionExpiresAt: "2026-07-18T00:03:30.000Z",
      mappings: [normalizedMapping()],
    });
    const withoutKey = new CallMeshClient(
      {
        baseUrl,
        agent: "callmesh-client/2.0.0-rc.1 (fixture; arm64)",
        meshNetworkId: "fixture-network",
        clock: () => new Date(now),
      },
      database.callmeshMappings,
    );
    expect(withoutKey.getOverview()).toMatchObject({
      status: {
        state: "unavailable",
        reasonCode: "CALLMESH_NOT_CONFIGURED",
        activeMappingCount: 0,
        provisionState: "unavailable",
      },
      mappings: [],
    });
    expect(withoutKey.getProvision()).toBeUndefined();
    expect(withoutKey.getAprsState()).toBeUndefined();
    expect(requests).toEqual([
      expectedRequest("/api/v1/client/heartbeat", {
        local_hash: null,
        agent: "cmclient-gateway/2.0.0-rc.1 (fixture; arm64)",
      }),
      expectedRequest("/api/v1/client/mappings", { known_hash: null }),
      expectedRequest("/api/v1/client/heartbeat", {
        local_hash: recordedMappings.hash,
        agent: "cmclient-gateway/2.0.0-rc.1 (fixture; arm64)",
      }),
    ]);
    database.close();
    await close(server);
  });

  it("fails closed without a key and does not make an upstream request", async () => {
    let requests = 0;
    const client = new CallMeshClient({
      baseUrl: "https://callmesh.invalid",
      agent: "cmclient-gateway/2.0.0-rc.1 (fixture; arm64)",
      meshNetworkId: "fixture-network",
      fetch: async () => {
        requests += 1;
        return new Response(null, { status: 204 });
      },
    });

    await expect(client.synchronize()).resolves.toMatchObject({
      status: {
        state: "unavailable",
        reasonCode: "CALLMESH_NOT_CONFIGURED",
        activeMappingCount: 0,
        provisionState: "unavailable",
      },
      mappings: [],
    });
    expect(requests).toBe(0);
  });

  it("retries a transient heartbeat failure with bounded backoff", async () => {
    let requests = 0;
    const delays: number[] = [];
    const client = new CallMeshClient({
      baseUrl: "https://callmesh.invalid",
      apiKey: "fixture-api-key",
      agent: "cmclient-gateway/2.0.0-rc.1 (fixture; arm64)",
      meshNetworkId: "fixture-network",
      maximumRetries: 2,
      initialRetryDelayMs: 5,
      maximumRetryDelayMs: 8,
      sleep: async (delayMs) => {
        delays.push(delayMs);
      },
      fetch: async (input) => {
        requests += 1;
        if (requests < 3) {
          return new Response("unavailable", { status: 503 });
        }
        return new URL(String(input)).pathname.endsWith("/mappings")
          ? jsonResponse(recordedMappings)
          : jsonResponse(heartbeat({ provision: null }));
      },
    });

    const overview = await client.synchronize();

    expect(requests).toBe(4);
    expect(delays).toEqual([5, 8]);
    expect(overview.status).toMatchObject({
      state: "ready",
      activeMappingHash: recordedMappings.hash,
      provisionState: "revoked",
    });
    expect(client.getProvision()).toBeUndefined();
    expect(client.getMappingsForUse()).toEqual([]);
  });

  it.each([
    { status: 403, requests: 1, code: "CALLMESH_AUTH_INVALID" },
    { status: 408, requests: 3, code: "CALLMESH_HTTP_408" },
    { status: 429, requests: 3, code: "CALLMESH_HTTP_429" },
  ])(
    "classifies HTTP $status with the required retry policy",
    async ({ status, requests: expectedRequests, code }) => {
      let requests = 0;
      const client = new CallMeshClient({
        ...clientOptions(async () => {
          requests += 1;
          return new Response(null, { status });
        }),
        maximumRetries: 2,
        sleep: async () => undefined,
      });

      await client.synchronize();

      expect(requests).toBe(expectedRequests);
      expect(client.getOverview().status.reasonCode).toBe(code);
    },
  );

  it("bounds response bytes, rejects credential controls, and refuses redirects", async () => {
    expect(
      () =>
        new CallMeshClient({
          apiKey: "fixture\nkey",
          agent: "callmesh-client/2.0.0 (fixture; arm64)",
        }),
    ).toThrow("CALLMESH_CONFIGURATION_INVALID");

    const oversized = clientWithFetch(
      async () => new Response("x".repeat(512 * 1024 + 1), { status: 200 }),
    );
    await oversized.synchronize();
    expect(oversized.getOverview().status).toMatchObject({
      state: "degraded",
      reasonCode: "CALLMESH_SCHEMA_INVALID",
      provisionState: "invalid",
    });

    let redirectedRequests = 0;
    const redirectTarget = createServer((_request, response) => {
      redirectedRequests += 1;
      response.end(JSON.stringify(heartbeat()));
    });
    const redirectTargetUrl = await listen(redirectTarget);
    const redirectSource = createServer((_request, response) => {
      response.statusCode = 307;
      response.setHeader("location", redirectTargetUrl);
      response.end();
    });
    const redirectSourceUrl = await listen(redirectSource);
    const redirecting = new CallMeshClient({
      ...clientOptions(globalThis.fetch),
      baseUrl: redirectSourceUrl,
      maximumRetries: 0,
    });

    await redirecting.synchronize();

    expect(redirecting.getOverview().status).toMatchObject({
      state: "degraded",
      reasonCode: "CALLMESH_NETWORK_UNAVAILABLE",
    });
    expect(redirectedRequests).toBe(0);
    await close(redirectSource);
    await close(redirectTarget);
  });

  it("reports invalid credentials, timeout, and mapping conflicts with stable codes", async () => {
    const invalidKey = clientWithFetch(
      async () => new Response(null, { status: 401 }),
    );
    let timeoutRequests = 0;
    const timeout = new CallMeshClient({
      baseUrl: "https://callmesh.invalid",
      apiKey: "fixture-api-key",
      agent: "cmclient-gateway/2.0.0-rc.1 (fixture; arm64)",
      meshNetworkId: "fixture-network",
      timeoutMs: 1,
      maximumRetries: 1,
      sleep: async () => undefined,
      fetch: async (_input, init) => {
        timeoutRequests += 1;
        return new Response(
          new ReadableStream<Uint8Array>({
            start(controller) {
              controller.enqueue(new TextEncoder().encode("{"));
              init?.signal?.addEventListener(
                "abort",
                () => controller.error(new Error("fixture body timeout")),
                { once: true },
              );
            },
          }),
          { status: 200 },
        );
      },
    });
    const conflict = clientWithFetch(async (input) =>
      new URL(String(input)).pathname.endsWith("/mappings")
        ? jsonResponse({
            hash: recordedMappings.hash,
            items: [wireMapping("N0CALL"), wireMapping("N1CALL")],
          })
        : jsonResponse(heartbeat()),
    );
    const invalidFullCallsign = clientWithFetch(async (input) =>
      new URL(String(input)).pathname.endsWith("/mappings")
        ? jsonResponse({
            hash: recordedMappings.hash,
            items: [
              {
                mesh_id: "!0000002a",
                aprs_callsign: "N0CALL-16",
                enabled: true,
              },
            ],
          })
        : jsonResponse(heartbeat()),
    );
    const duplicateOwner = clientWithFetch(async (input) =>
      new URL(String(input)).pathname.endsWith("/mappings")
        ? jsonResponse({
            hash: recordedMappings.hash,
            items: [
              wireMapping("N0CALL", "!0000002a"),
              wireMapping("N0CALL", "!0000002b"),
            ],
          })
        : jsonResponse(heartbeat()),
    );

    await invalidKey.synchronize();
    await timeout.synchronize();
    await conflict.synchronize();
    await invalidFullCallsign.synchronize();
    await duplicateOwner.synchronize();

    expect(invalidKey.getOverview().status).toMatchObject({
      state: "unavailable",
      reasonCode: "CALLMESH_AUTH_INVALID",
    });
    expect(timeout.getOverview().status).toMatchObject({
      state: "degraded",
      reasonCode: "CALLMESH_NETWORK_UNAVAILABLE",
    });
    expect(timeoutRequests).toBe(2);
    expect(conflict.getOverview()).toMatchObject({
      status: {
        state: "degraded",
        reasonCode: "CALLMESH_MAPPING_CONFLICT",
      },
      mappings: [],
    });
    expect(invalidFullCallsign.getOverview().status).toMatchObject({
      state: "degraded",
      reasonCode: "CALLMESH_SCHEMA_INVALID",
      provisionState: "invalid",
    });
    expect(duplicateOwner.getOverview().status).toMatchObject({
      state: "degraded",
      reasonCode: "CALLMESH_MAPPING_CONFLICT",
      provisionState: "invalid",
    });
  });

  it("restores the last snapshot and rejects stale or conflicting revisions without downgrade", async () => {
    const database = new GatewayDatabase(":memory:");
    const initial = new CallMeshClient(
      clientOptions(async (input) =>
        new URL(String(input)).pathname.endsWith("/mappings")
          ? jsonResponse(recordedMappings)
          : jsonResponse(heartbeat()),
      ),
      database.callmeshMappings,
    );
    await initial.synchronize();

    const restored = new CallMeshClient(
      clientOptions(async () =>
        jsonResponse(heartbeat({ needs_update: false })),
      ),
      database.callmeshMappings,
    );
    await restored.synchronize();
    expect(restored.getOverview()).toMatchObject({
      status: { state: "ready", activeMappingHash: recordedMappings.hash },
      mappings: [normalizedMapping()],
    });

    const stale = new CallMeshClient(
      clientOptions(async () =>
        jsonResponse(
          heartbeat({
            hash: "mapping-older",
            server_time: "2026-07-17T23:59:59.000Z",
          }),
        ),
      ),
      database.callmeshMappings,
    );
    await stale.synchronize();
    expect(stale.getOverview().status).toMatchObject({
      state: "degraded",
      reasonCode: "CALLMESH_STALE_RESPONSE",
    });
    expect(stale.getOverview().mappings).toEqual([normalizedMapping()]);
    expect(stale.getProvision()).toMatchObject({ callsignBase: "N0CALL" });
    expect(database.callmeshMappings.loadSnapshot()?.mappingHash).toBe(
      recordedMappings.hash,
    );

    const conflict = new CallMeshClient(
      clientOptions(async () =>
        jsonResponse(
          heartbeat({
            hash: "mapping-conflict",
            needs_update: false,
            server_time: "2026-07-18T00:00:01.000Z",
          }),
        ),
      ),
      database.callmeshMappings,
    );
    await conflict.synchronize();
    expect(conflict.getOverview().status).toMatchObject({
      state: "degraded",
      reasonCode: "CALLMESH_RESPONSE_CONFLICT",
      activeMappingCount: 0,
      provisionState: "invalid",
    });
    expect(conflict.getOverview().mappings).toEqual([]);
    expect(conflict.getProvision()).toBeUndefined();
    const deactivated = database.callmeshMappings.loadSnapshot();
    expect(deactivated).toMatchObject({
      active: false,
      mappingHash: recordedMappings.hash,
    });
    expect(deactivated?.provision).toBeUndefined();
    expect(database.callmeshMappings.list()).toEqual([normalizedMapping()]);
    database.close();
  });

  it("keeps a last-good lease through transient failure but never past expiry", async () => {
    const database = new GatewayDatabase(":memory:");
    let now = "2026-07-18T00:00:30.000Z";
    const initial = new CallMeshClient(
      {
        ...clientOptions(async (input) =>
          new URL(String(input)).pathname.endsWith("/mappings")
            ? jsonResponse(recordedMappings)
            : jsonResponse(heartbeat()),
        ),
        clock: () => new Date(now),
      },
      database.callmeshMappings,
    );
    await initial.synchronize();

    now = "2026-07-18T00:01:30.000Z";
    const transient = new CallMeshClient(
      {
        ...clientOptions(async () => new Response(null, { status: 503 })),
        maximumRetries: 0,
        clock: () => new Date(now),
      },
      database.callmeshMappings,
    );
    await transient.synchronize();

    expect(transient.getOverview()).toMatchObject({
      status: {
        state: "degraded",
        reasonCode: "CALLMESH_HTTP_503",
        activeMappingCount: 1,
        provisionState: "valid",
      },
      mappings: [normalizedMapping()],
    });
    expect(transient.getProvision()).toMatchObject({ callsignBase: "N0CALL" });
    expect(transient.getMappingsForUse()).toEqual([normalizedMapping()]);

    now = "2026-07-18T00:03:30.000Z";
    expect(transient.getOverview().status.provisionState).toBe("expired");
    expect(transient.getProvision()).toBeUndefined();
    expect(transient.getMappingsForUse()).toEqual([]);
    expect(transient.getAprsState()).toBeUndefined();
    const restartedExpired = new CallMeshClient(
      {
        ...clientOptions(async () => new Response(null, { status: 503 })),
        clock: () => new Date(now),
      },
      database.callmeshMappings,
    );
    expect(restartedExpired.getOverview().status.provisionState).toBe(
      "expired",
    );
    expect(restartedExpired.getAprsState()).toBeUndefined();
    database.close();
  });

  it("durably deactivates the active identity on authentication failure", async () => {
    const database = new GatewayDatabase(":memory:");
    const initial = new CallMeshClient(
      clientOptions(async (input) =>
        new URL(String(input)).pathname.endsWith("/mappings")
          ? jsonResponse(recordedMappings)
          : jsonResponse(heartbeat()),
      ),
      database.callmeshMappings,
    );
    await initial.synchronize();

    const rejected = new CallMeshClient(
      clientOptions(async () => new Response(null, { status: 401 })),
      database.callmeshMappings,
    );
    await rejected.synchronize();

    expect(rejected.getOverview()).toMatchObject({
      status: {
        state: "unavailable",
        reasonCode: "CALLMESH_AUTH_INVALID",
        activeMappingCount: 0,
        provisionState: "invalid",
      },
      mappings: [],
    });
    expect(rejected.getProvision()).toBeUndefined();
    const deactivated = database.callmeshMappings.loadSnapshot();
    expect(deactivated).toMatchObject({
      active: false,
      mappingHash: recordedMappings.hash,
    });
    expect(deactivated?.provision).toBeUndefined();
    expect(database.callmeshMappings.list()).toEqual([normalizedMapping()]);

    const restarted = new CallMeshClient(
      clientOptions(async () => new Response(null, { status: 401 })),
      database.callmeshMappings,
    );
    expect(restarted.getOverview().mappings).toEqual([]);
    expect(restarted.getProvision()).toBeUndefined();
    await restarted.synchronize();
    expect(restarted.getOverview().status).toMatchObject({
      state: "unavailable",
      reasonCode: "CALLMESH_AUTH_INVALID",
      activeMappingCount: 0,
    });
    database.close();
  });

  it("isolates a corrupted snapshot and recovers without aborting Gateway startup", async () => {
    const database = new GatewayDatabase(":memory:");
    let recovering = false;
    const upstream = async (input: RequestInfo | URL) =>
      new URL(String(input)).pathname.endsWith("/mappings")
        ? jsonResponse(recordedMappings)
        : jsonResponse(
            heartbeat(
              recovering
                ? {
                    needs_update: false,
                    server_time: "2026-07-18T00:01:00.000Z",
                  }
                : {},
            ),
          );
    const initial = new CallMeshClient(
      clientOptions(upstream),
      database.callmeshMappings,
    );
    await initial.synchronize();
    database.connection
      .prepare(
        "UPDATE callmesh_sync_state SET provision_expires_at = ? WHERE id = 1",
      )
      .run("2026-07-19T00:00:00.000Z");

    const isolated = new CallMeshClient(
      clientOptions(upstream),
      database.callmeshMappings,
    );
    expect(isolated.getOverview()).toMatchObject({
      status: {
        state: "degraded",
        reasonCode: "CALLMESH_MAPPING_STORE_FAILED",
        activeMappingCount: 0,
        provisionState: "invalid",
      },
      mappings: [],
    });

    await isolated.synchronize();

    expect(isolated.getOverview().status).toMatchObject({
      state: "degraded",
      reasonCode: "CALLMESH_STALE_RESPONSE",
      activeMappingCount: 0,
    });

    recovering = true;
    const recovered = new CallMeshClient(
      clientOptions(upstream),
      database.callmeshMappings,
    );
    await recovered.synchronize();

    expect(recovered.getOverview().status).toMatchObject({
      state: "ready",
      activeMappingCount: 1,
      provisionState: "valid",
    });
    database.close();
  });

  it("refuses upstream sync when the durable history high-water is corrupt", async () => {
    const database = new GatewayDatabase(":memory:");
    const initial = new CallMeshClient(
      clientOptions(async (input) =>
        new URL(String(input)).pathname.endsWith("/mappings")
          ? jsonResponse(recordedMappings)
          : jsonResponse(heartbeat()),
      ),
      database.callmeshMappings,
    );
    await initial.synchronize();
    database.connection
      .prepare(
        "UPDATE callmesh_sync_history SET last_server_time = ? WHERE mapping_hash = ?",
      )
      .run("invalid-time", recordedMappings.hash);
    let requests = 0;
    const isolated = new CallMeshClient(
      clientOptions(async () => {
        requests += 1;
        return jsonResponse(
          heartbeat({ server_time: "2026-07-18T00:02:00.000Z" }),
        );
      }),
      database.callmeshMappings,
    );

    await isolated.synchronize();

    expect(requests).toBe(0);
    expect(isolated.getOverview()).toMatchObject({
      status: {
        state: "degraded",
        reasonCode: "CALLMESH_MAPPING_STORE_FAILED",
        activeMappingCount: 0,
        provisionState: "invalid",
      },
      mappings: [],
    });
    database.close();
  });

  it("refuses upstream sync when a durable snapshot has lost its history high-water", async () => {
    const database = new GatewayDatabase(":memory:");
    const initial = new CallMeshClient(
      clientOptions(async (input) =>
        new URL(String(input)).pathname.endsWith("/mappings")
          ? jsonResponse(recordedMappings)
          : jsonResponse(heartbeat()),
      ),
      database.callmeshMappings,
    );
    await initial.synchronize();
    database.connection.exec("DELETE FROM callmesh_sync_history");
    let requests = 0;
    const isolated = new CallMeshClient(
      clientOptions(async () => {
        requests += 1;
        return jsonResponse(
          heartbeat({ server_time: "2026-07-17T23:59:00.000Z" }),
        );
      }),
      database.callmeshMappings,
    );

    await isolated.synchronize();

    expect(requests).toBe(0);
    expect(isolated.getOverview()).toMatchObject({
      status: {
        state: "degraded",
        reasonCode: "CALLMESH_MAPPING_STORE_FAILED",
        activeMappingCount: 0,
        provisionState: "invalid",
      },
      mappings: [],
    });
    database.close();
  });

  it("reports a stable store error when a mapping transaction cannot begin", () => {
    const database = new GatewayDatabase(":memory:");
    database.connection.exec("BEGIN IMMEDIATE");
    expect(() => database.callmeshMappings.replace([])).toThrow(
      "CALLMESH_MAPPING_STORE_FAILED",
    );
    database.connection.exec("ROLLBACK");
    database.close();
  });

  it("rejects a mappings hash mismatch atomically and does not expose provision identity", async () => {
    const database = new GatewayDatabase(":memory:");
    const client = new CallMeshClient(
      clientOptions(async (input) =>
        new URL(String(input)).pathname.endsWith("/mappings")
          ? jsonResponse({ ...recordedMappings, hash: "wrong-hash" })
          : jsonResponse(heartbeat()),
      ),
      database.callmeshMappings,
    );

    await client.synchronize();

    expect(client.getOverview().status).toMatchObject({
      state: "degraded",
      reasonCode: "CALLMESH_RESPONSE_CONFLICT",
      provisionState: "invalid",
    });
    expect(database.callmeshMappings.loadSnapshot()).toBeUndefined();
    expect(JSON.stringify(client.getOverview())).not.toContain("N0CALL");
    database.close();
  });
});

interface CapturedRequest {
  method: string | undefined;
  url: string | undefined;
  contentType: string | undefined;
  apiKey: string | undefined;
  agent: string | undefined;
  authorization: string | undefined;
  body: unknown;
}

function clientOptions(fetch: typeof globalThis.fetch) {
  return {
    baseUrl: "https://callmesh.invalid",
    apiKey: "fixture-api-key",
    agent: "cmclient-gateway/2.0.0-rc.1 (fixture; arm64)",
    meshNetworkId: "fixture-network",
    clock: () => new Date("2026-07-18T00:00:30.000Z"),
    fetch,
  };
}

function clientWithFetch(fetch: typeof globalThis.fetch): CallMeshClient {
  return new CallMeshClient(clientOptions(fetch));
}

function heartbeat(overrides: Record<string, unknown> = {}) {
  return {
    hash: recordedMappings.hash,
    needs_update: true,
    provision,
    server_time: "2026-07-18T00:00:00.000Z",
    ...overrides,
  };
}

function wireMapping(callsignBase: string, meshId = "!0000002a") {
  return {
    mesh_id: meshId,
    callsign_base: callsignBase,
    ssid: -7,
    enabled: true,
    effective_at: "2026-07-18T00:00:00.000Z",
  };
}

function normalizedMapping() {
  return {
    version: recordedMappings.hash,
    effectiveAt: "2026-07-18T00:00:00.000Z",
    meshNetworkId: "fixture-network",
    nodeNum: 42,
    callsign: "N0CALL-7",
  };
}

function expectedRequest(url: string, body: unknown): CapturedRequest {
  return {
    method: "POST",
    url,
    contentType: "application/json",
    apiKey: "fixture-api-key",
    agent: "cmclient-gateway/2.0.0-rc.1 (fixture; arm64)",
    authorization: undefined,
    body,
  };
}

async function captureRequest(
  request: IncomingMessage,
): Promise<CapturedRequest> {
  const chunks: Buffer[] = [];
  for await (const chunk of request) {
    chunks.push(Buffer.from(chunk));
  }
  return {
    method: request.method,
    url: request.url,
    contentType: request.headers["content-type"],
    apiKey: request.headers["x-api-key"] as string | undefined,
    agent: request.headers["x-client-agent"] as string | undefined,
    authorization: request.headers.authorization,
    body: JSON.parse(Buffer.concat(chunks).toString("utf8")) as unknown,
  };
}

function jsonResponse(payload: unknown): Response {
  return new Response(JSON.stringify(payload), {
    status: 200,
    headers: { "content-type": "application/json" },
  });
}

async function listen(server: Server): Promise<string> {
  server.listen({ host: "127.0.0.1", port: 0 });
  await once(server, "listening");
  const address = server.address();
  if (!address || typeof address === "string") {
    throw new Error("fixture CallMesh server did not bind");
  }
  return `http://127.0.0.1:${address.port}`;
}

async function close(server: Server): Promise<void> {
  server.close();
  await once(server, "close");
}
