import { once } from "node:events";
import { createServer, type Server } from "node:http";
import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

import {
  CallMeshClient,
  CallMeshMappingRepository,
  type CallMeshMappingStore,
} from "./callmesh";
import { GatewayDatabase } from "./persistence/database";

const recordedMappings = JSON.parse(
  readFileSync(
    new URL("../test/fixtures/callmesh/mappings.json", import.meta.url),
    "utf8",
  ),
) as unknown;

describe("CallMesh client", () => {
  it("synchronizes heartbeat, provision, and sanitized mappings through a mock server", async () => {
    const requests: Array<{
      method: string | undefined;
      url: string | undefined;
      authorization: string | undefined;
    }> = [];
    const server = createServer((request, response) => {
      requests.push({
        method: request.method,
        url: request.url,
        authorization: request.headers.authorization,
      });
      if (request.url === "/v1/mappings") {
        response.setHeader("content-type", "application/json");
        response.end(JSON.stringify(recordedMappings));
        return;
      }
      response.statusCode = 204;
      response.end();
    });
    const baseUrl = await listen(server);
    const store = new MemoryMappingStore();
    const client = new CallMeshClient(
      {
        baseUrl,
        apiKey: "fixture-api-key",
        clock: () => new Date("2026-07-18T00:00:00.000Z"),
      },
      store,
    );

    const overview = await client.synchronize();

    expect(overview).toEqual({
      status: {
        state: "ready",
        updatedAt: "2026-07-18T00:00:00.000Z",
        activeMappingVersion: "mapping-2026-07",
        activeMappingCount: 1,
      },
      mappings: (recordedMappings as { mappings: unknown[] }).mappings,
    });
    expect(store.mappings).toEqual(overview.mappings);
    expect(requests).toEqual([
      {
        method: "POST",
        url: "/v1/heartbeat",
        authorization: "Bearer fixture-api-key",
      },
      {
        method: "POST",
        url: "/v1/provision",
        authorization: "Bearer fixture-api-key",
      },
      {
        method: "GET",
        url: "/v1/mappings",
        authorization: "Bearer fixture-api-key",
      },
    ]);
    await close(server);
  });

  it("fails closed without a key and does not make an upstream request", async () => {
    let requests = 0;
    const client = new CallMeshClient({
      baseUrl: "https://callmesh.invalid",
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
      },
      mappings: [],
    });
    expect(requests).toBe(0);
  });

  it("retries a transient upstream failure with bounded backoff", async () => {
    let requests = 0;
    const delays: number[] = [];
    const client = new CallMeshClient({
      baseUrl: "https://callmesh.invalid",
      apiKey: "fixture-api-key",
      maximumRetries: 2,
      initialRetryDelayMs: 5,
      maximumRetryDelayMs: 8,
      sleep: async (delayMs) => {
        delays.push(delayMs);
      },
      fetch: async () => {
        requests += 1;
        if (requests < 3) {
          return new Response("unavailable", { status: 503 });
        }
        return new Response(null, { status: 204 });
      },
    });

    const overview = await client.synchronize();

    expect(requests).toBe(5);
    expect(delays).toEqual([5, 8]);
    expect(overview.status).toMatchObject({
      state: "degraded",
      reasonCode: "CALLMESH_SCHEMA_INVALID",
    });
  });

  it("reports invalid credentials, timeout, and mapping conflicts with stable codes", async () => {
    const invalidKey = new CallMeshClient({
      baseUrl: "https://callmesh.invalid",
      apiKey: "fixture-api-key",
      fetch: async () => new Response(null, { status: 401 }),
    });
    const timeout = new CallMeshClient({
      baseUrl: "https://callmesh.invalid",
      apiKey: "fixture-api-key",
      timeoutMs: 1,
      maximumRetries: 1,
      sleep: async () => undefined,
      fetch: async (_input, init) =>
        new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener("abort", () => reject(new Error()));
        }),
    });
    const conflict = new CallMeshClient({
      baseUrl: "https://callmesh.invalid",
      apiKey: "fixture-api-key",
      fetch: async (input) => {
        if (new URL(String(input)).pathname === "/v1/mappings") {
          return jsonResponse({
            mappings: [mapping("N0CALL-7"), mapping("N1CALL-7")],
          });
        }
        return new Response(null, { status: 204 });
      },
    });

    await invalidKey.synchronize();
    await timeout.synchronize();
    await conflict.synchronize();

    expect(invalidKey.getOverview().status).toMatchObject({
      state: "unavailable",
      reasonCode: "CALLMESH_AUTH_INVALID",
    });
    expect(timeout.getOverview().status).toMatchObject({
      state: "degraded",
      reasonCode: "CALLMESH_NETWORK_UNAVAILABLE",
    });
    expect(conflict.getOverview()).toMatchObject({
      status: {
        state: "degraded",
        reasonCode: "CALLMESH_MAPPING_CONFLICT",
      },
      mappings: [],
    });
  });

  it("persists only a validated, non-conflicting mapping set", async () => {
    const database = new GatewayDatabase(":memory:");
    const repository = new CallMeshMappingRepository(database.connection);
    const client = new CallMeshClient(
      {
        baseUrl: "https://callmesh.invalid",
        apiKey: "fixture-api-key",
        fetch: async (input) => {
          if (new URL(String(input)).pathname === "/v1/mappings") {
            return jsonResponse(recordedMappings);
          }
          return new Response(null, { status: 204 });
        },
      },
      repository,
    );

    await client.synchronize();

    expect(repository.list()).toEqual(
      (recordedMappings as { mappings: unknown[] }).mappings,
    );

    const conflict = new CallMeshClient(
      {
        baseUrl: "https://callmesh.invalid",
        apiKey: "fixture-api-key",
        fetch: async (input) => {
          if (new URL(String(input)).pathname === "/v1/mappings") {
            return jsonResponse({
              mappings: [mapping("N0CALL-7"), mapping("N1CALL-7")],
            });
          }
          return new Response(null, { status: 204 });
        },
      },
      repository,
    );
    await conflict.synchronize();

    expect(repository.list()).toEqual([]);
    database.close();
  });
});

class MemoryMappingStore implements CallMeshMappingStore {
  mappings: unknown[] = [];

  replace(mappings: unknown[]): void {
    this.mappings = mappings;
  }
}

function mapping(callsign: string) {
  return {
    version: "mapping-2026-07",
    effectiveAt: "2026-07-18T00:00:00.000Z",
    meshNetworkId: "fixture-network",
    nodeNum: 42,
    callsign,
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
