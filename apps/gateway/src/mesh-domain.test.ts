import { describe, expect, it } from "vitest";

import { createMeshObservation } from "./observations";
import { MeshDomainStore } from "./mesh-domain";
import { GatewayDatabase } from "./persistence/database";
import { MeshtasticApplicationDecoder } from "./protobuf/application";
import { loadMeshtasticSchema } from "./protobuf/schema";

describe("MeshDomainStore", () => {
  it("keeps node, message, and telemetry data scoped to a mesh network", async () => {
    const database = new GatewayDatabase(":memory:");
    const schema = await loadMeshtasticSchema();
    const store = new MeshDomainStore(
      database,
      new MeshtasticApplicationDecoder(schema),
      { idFactory: idFactory("message-1", "telemetry-1") },
    );
    const node = observation(
      "observation-node",
      "2026-07-18T00:00:00.000Z",
      "NODEINFO_APP",
      schema.user
        .encode({
          id: "!fixture42",
          longName: "Fixture Node",
          shortName: "FN",
        })
        .finish(),
    );
    const message = observation(
      "observation-message",
      "2026-07-18T00:00:01.000Z",
      "TEXT_MESSAGE_APP",
      Buffer.from("fixture message"),
    );
    const telemetry = observation(
      "observation-telemetry",
      "2026-07-18T00:00:02.000Z",
      "TELEMETRY_APP",
      schema.telemetry
        .encode({ deviceMetrics: { batteryLevel: 73, voltage: 3.9 } })
        .finish(),
    );
    database.meshObservations.insert(node);
    database.meshObservations.insert(message);
    database.meshObservations.insert(telemetry);

    expect(store.persist("fixture-network-a", node)).toMatchObject({
      kind: "node",
      node: {
        meshNetworkId: "fixture-network-a",
        nodeNum: 42,
        longName: "Fixture Node",
      },
    });
    expect(store.persist("fixture-network-a", message)).toMatchObject({
      kind: "message",
      message: {
        id: "message-1",
        observationId: "observation-message",
        sender: 42,
        text: "fixture message",
      },
    });
    const storedTelemetry = store.persist("fixture-network-a", telemetry);
    expect(storedTelemetry).toMatchObject({
      kind: "telemetry",
      telemetry: {
        id: "telemetry-1",
        observationId: "observation-telemetry",
        metricKind: "deviceMetrics",
        metrics: { batteryLevel: 73 },
      },
    });
    if (storedTelemetry.kind !== "telemetry") {
      throw new Error("Telemetry fixture was not persisted");
    }
    expect(storedTelemetry.telemetry.metrics.voltage).toBeCloseTo(3.9, 6);
    expect(store.persist("fixture-network-a", message)).toMatchObject({
      kind: "message",
      message: { id: "message-1" },
    });
    expect(store.persist("fixture-network-b", node)).toMatchObject({
      kind: "node",
      node: { meshNetworkId: "fixture-network-b", nodeNum: 42 },
    });
    expect(database.meshNodes.find("fixture-network-a", 42)).toMatchObject({
      lastSeenAt: "2026-07-18T00:00:02.000Z",
      lastObservationId: "observation-telemetry",
    });
    expect(database.meshNodes.find("fixture-network-b", 42)).toMatchObject({
      longName: "Fixture Node",
      lastObservationId: "observation-node",
    });
    database.close();
  });

  it("does not let an older NodeInfo observation overwrite current metadata", async () => {
    const database = new GatewayDatabase(":memory:");
    const schema = await loadMeshtasticSchema();
    const store = new MeshDomainStore(
      database,
      new MeshtasticApplicationDecoder(schema),
    );
    const current = observation(
      "observation-current",
      "2026-07-18T00:00:02.000Z",
      "NODEINFO_APP",
      schema.user.encode({ longName: "Current Node" }).finish(),
    );
    const old = observation(
      "observation-old",
      "2026-07-18T00:00:01.000Z",
      "NODEINFO_APP",
      schema.user.encode({ longName: "Old Node" }).finish(),
    );
    database.meshObservations.insert(current);
    database.meshObservations.insert(old);

    store.persist("fixture-network", current);
    store.persist("fixture-network", old);

    expect(database.meshNodes.find("fixture-network", 42)).toMatchObject({
      longName: "Current Node",
      lastSeenAt: "2026-07-18T00:00:02.000Z",
      lastObservationId: "observation-current",
    });
    database.close();
  });
});

function observation(
  id: string,
  ingestedAt: string,
  portNum: string,
  payload: Uint8Array,
) {
  return createMeshObservation({
    id,
    transport: "simulator",
    sessionConnectedAt: "2026-07-18T00:00:00.000Z",
    ingestedAt,
    serverIngestedAt: ingestedAt,
    normalizedFromRadio: {
      schemaVersion: 1,
      kind: "packet",
      packet: {
        sender: 42,
        portNum,
        payloadBase64: Buffer.from(payload).toString("base64"),
      },
    },
  });
}

function idFactory(...ids: string[]): () => string {
  let index = 0;
  return () => ids[index++] ?? "unexpected-id";
}
