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
    expect(decoder.decode(packet("POSITION_APP", new Uint8Array([1])))).toEqual(
      {
        kind: "ignored",
        reasonCode: "MESH_APPLICATION_PORT_UNSUPPORTED",
      },
    );
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
});

function packet(portNum: string, payload: Uint8Array): NormalizedMeshPacket {
  return {
    sender: 42,
    portNum,
    payloadBase64: Buffer.from(payload).toString("base64"),
  };
}
