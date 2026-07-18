import { describe, expect, it } from "vitest";

import type { NormalizedMeshPacket } from "@cmclient/contracts";

import { MeshtasticApplicationDecoder } from "./application";
import { loadMeshtasticSchema } from "./schema";

describe("Meshtastic application payload decoder", () => {
  it("decodes NodeInfo, UTF-8 text, and telemetry payloads from the locked schema", async () => {
    const schema = await loadMeshtasticSchema();
    const decoder = new MeshtasticApplicationDecoder(schema);
    const nodePayload = schema.user
      .encode({
        id: "!fixture01",
        longName: "Fixture Node",
        shortName: "FN",
      })
      .finish();
    const telemetryPayload = schema.telemetry
      .encode({
        time: 1784332802,
        deviceMetrics: { batteryLevel: 73, voltage: 3.9 },
      })
      .finish();

    expect(decoder.decode(packet("NODEINFO_APP", nodePayload))).toEqual({
      kind: "node",
      node: {
        nodeNum: 42,
        userId: "!fixture01",
        longName: "Fixture Node",
        shortName: "FN",
      },
    });
    expect(
      decoder.decode(packet("TEXT_MESSAGE_APP", Buffer.from("fixture text"))),
    ).toEqual({
      kind: "message",
      message: { sender: 42, text: "fixture text" },
    });
    const decodedTelemetry = decoder.decode(
      packet("TELEMETRY_APP", telemetryPayload),
    );
    expect(decodedTelemetry).toMatchObject({
      kind: "telemetry",
      telemetry: {
        nodeNum: 42,
        metricKind: "deviceMetrics",
        metrics: { batteryLevel: 73 },
        telemetryTimeSeconds: 1784332802,
      },
    });
    if (decodedTelemetry.kind !== "telemetry") {
      throw new Error("Telemetry fixture did not decode");
    }
    expect(decodedTelemetry.telemetry.metrics.voltage).toBeCloseTo(3.9, 6);
  });

  it("rejects malformed payloads and unrecognized ports without inventing records", async () => {
    const decoder = new MeshtasticApplicationDecoder(
      await loadMeshtasticSchema(),
    );

    expect(
      decoder.decode(packet("NODEINFO_APP", new Uint8Array([0x80]))),
    ).toEqual({
      kind: "ignored",
      reasonCode: "MESH_APPLICATION_PAYLOAD_DECODE_FAILED",
    });
    expect(
      decoder.decode(packet("POSITION_APP", new Uint8Array([0x80]))),
    ).toEqual({
      kind: "ignored",
      reasonCode: "MESH_APPLICATION_PAYLOAD_DECODE_FAILED",
    });
    expect(
      decoder.decode({
        ...packet("TEXT_MESSAGE_APP", Buffer.from("fixture text")),
        channel: 256,
      }),
    ).toEqual({
      kind: "ignored",
      reasonCode: "MESH_MESSAGE_CHANNEL_INVALID",
    });
  });

  it("decodes POSITION_APP with schema-defined units and a payload identity", async () => {
    const schema = await loadMeshtasticSchema();
    const decoder = new MeshtasticApplicationDecoder(schema);
    const payload = schema.position
      .encode({
        latitudeI: 250_475_000,
        longitudeI: 1_215_175_000,
        altitude: 12,
        altitudeHae: 31,
        altitudeGeoidalSeparation: 19,
        timestamp: 1_784_332_800,
        timestampMillisAdjust: 250,
        time: 1_784_332_799,
        groundSpeed: 4,
        groundTrack: 12_345,
        seqNumber: 9,
        precisionBits: 32,
      })
      .finish();

    expect(decoder.decode(packet("POSITION_APP", payload))).toEqual({
      kind: "position",
      position: {
        nodeNum: 42,
        payloadHash: expect.stringMatching(/^[a-f0-9]{64}$/),
        sample: {
          latitudeI: 250_475_000,
          longitudeI: 1_215_175_000,
          altitudeMslMeters: 12,
          altitudeHaeMeters: 31,
          altitudeGeoidalSeparationMeters: 19,
          positionTimestampSeconds: 1_784_332_800,
          positionTimestampMillisAdjust: 250,
          positionTimeSeconds: 1_784_332_799,
          sequenceNumber: 9,
          precisionBits: 32,
          groundSpeedMetersPerSecond: 4,
          groundTrackDegrees: 123.45,
        },
      },
    });
  });
});

function packet(portNum: string, payload: Uint8Array): NormalizedMeshPacket {
  return {
    sender: 42,
    portNum,
    payloadBase64: Buffer.from(payload).toString("base64"),
  };
}
