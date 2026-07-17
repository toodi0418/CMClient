import { describe, expect, it } from "vitest";

import { GatewayDatabase } from "./persistence/database";
import { classifyBacklog, createMeshObservation } from "./observations";

describe("Mesh observations", () => {
  it("classifies only definitely pre-session device observations as backlog", () => {
    expect(classifyBacklog("2026-07-18T00:01:00.000Z", 1784332800)).toBe(
      "backlog",
    );
    expect(classifyBacklog("2026-07-18T00:00:00.000Z", 1784332801)).toBe(
      "live",
    );
    expect(classifyBacklog("2026-07-18T00:00:00.500Z", 1784332800)).toBe(
      "unknown",
    );
    expect(classifyBacklog("2026-07-18T00:00:00.000Z", undefined)).toBe(
      "unknown",
    );
  });

  it("persists device receive, API ingest, server ingest, and session times", () => {
    const observation = createMeshObservation({
      id: "observation-1",
      transport: "tcp",
      sessionConnectedAt: "2026-07-18T00:01:00.000Z",
      ingestedAt: "2026-07-18T00:01:02.000Z",
      serverIngestedAt: "2026-07-18T00:01:02.004Z",
      normalizedFromRadio: {
        schemaVersion: 1,
        kind: "packet",
        fromRadioId: 10,
        packet: {
          sender: 20,
          packetId: 30,
          portNum: "POSITION_APP",
          deviceRxTimeSeconds: 1784332800,
        },
      },
    });
    const database = new GatewayDatabase(":memory:");

    expect(database.meshObservations.insert(observation)).toEqual({
      ...observation,
      backlogClassification: "backlog",
    });
    expect(database.meshObservations.find("observation-1")).toEqual({
      ...observation,
      backlogClassification: "backlog",
    });
    database.close();
  });

  it("rejects timestamps that could be misread as local time", () => {
    expect(() =>
      createMeshObservation({
        id: "observation-invalid",
        transport: "serial",
        sessionConnectedAt: "2026-07-18T00:00:00+08:00",
        ingestedAt: "2026-07-18T00:00:01.000Z",
        serverIngestedAt: "2026-07-18T00:00:01.001Z",
        normalizedFromRadio: { schemaVersion: 1, kind: "other" },
      }),
    ).toThrow("MESH_OBSERVATION_INVALID");
  });
});
