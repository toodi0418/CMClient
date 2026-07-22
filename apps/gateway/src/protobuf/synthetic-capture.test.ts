import { describe, expect, it } from "vitest";

import type { NormalizedFromRadio } from "@cmclient/contracts";

import { MeshtasticApplicationDecoder } from "./application";
import { MeshtasticProtobufCodec } from "./protobuf";
import { loadMeshtasticSchema, type MeshtasticSchema } from "./schema";
import { projectSyntheticCapture } from "./synthetic-capture";

const PRIVATE_TEXT = "DO-NOT-LEAK-SYNTHETIC-CAPTURE-9f84c2";
const PRIVATE_NODE = 0xdead_beef;

describe("synthetic Meshtastic capture projection", () => {
  it("creates deterministic, forward-decodable fixtures for all allowlisted applications", async () => {
    const schema = await loadMeshtasticSchema();
    const applicationDecoder = new MeshtasticApplicationDecoder(schema);
    const codec = new MeshtasticProtobufCodec(schema);
    const cases = applicationCases(schema);

    for (const fixture of cases) {
      const source = packetSource(fixture.port, fixture.payload);
      const first = projectSyntheticCapture(schema, source, 17);
      const second = projectSyntheticCapture(schema, source, 17);

      expect(first.frame).toEqual(second.frame);
      expect(first.normalizedFromRadio).toEqual(second.normalizedFromRadio);
      expect(first.normalizedFromRadio).toEqual(
        codec.normalizeFromRadio(first.frame),
      );
      expect(
        applicationDecoder.decode(first.normalizedFromRadio.packet ?? {}),
      ).toMatchObject({ kind: fixture.decodedKind });
      expect(first.normalizedFromRadio.packet?.portNum).toBe(fixture.port);

      const serialized = JSON.stringify({
        frameHex: Buffer.from(first.frame).toString("hex"),
        normalizedFromRadio: first.normalizedFromRadio,
      });
      expect(serialized).not.toContain(PRIVATE_TEXT);
      expect(serialized).not.toContain(String(PRIVATE_NODE));
      expect(serialized).not.toContain(
        Buffer.from(fixture.payload).toString("base64"),
      );
      expect(containsBytes(first.frame, fixture.payload)).toBe(false);
    }
  });

  it("produces schema-valid config, other, and non-allowlisted packet frames", async () => {
    const schema = await loadMeshtasticSchema();
    const codec = new MeshtasticProtobufCodec(schema);
    const sources: NormalizedFromRadio[] = [
      {
        schemaVersion: 1,
        kind: "config_complete",
        fromRadioId: PRIVATE_NODE,
        configCompleteId: 3_735_928_559,
      },
      { schemaVersion: 1, kind: "other" },
      {
        schemaVersion: 1,
        kind: "packet",
        packet: {
          sender: PRIVATE_NODE,
          portNum: "PRIVATE_APP",
          payloadBase64: Buffer.from(PRIVATE_TEXT).toString("base64"),
        },
      },
    ];

    for (const source of sources) {
      const projection = projectSyntheticCapture(schema, source, 18);
      expect(
        schema.fromRadio.verify(schema.fromRadio.decode(projection.frame)),
      ).toBeNull();
      expect(codec.normalizeFromRadio(projection.frame)).toEqual(
        projection.normalizedFromRadio,
      );
      expect(projection.normalizedFromRadio.kind).toBe(source.kind);
      expect(JSON.stringify(projection)).not.toContain(PRIVATE_TEXT);
      expect(JSON.stringify(projection)).not.toContain(String(PRIVATE_NODE));
    }
    expect(sources[2]?.packet?.portNum).toBe("PRIVATE_APP");
    expect(
      projectSyntheticCapture(schema, sources[2]!, 18).normalizedFromRadio
        .packet?.portNum,
    ).toBeUndefined();
  });
});

function applicationCases(schema: MeshtasticSchema): Array<{
  port: "NODEINFO_APP" | "TEXT_MESSAGE_APP" | "TELEMETRY_APP" | "POSITION_APP";
  payload: Uint8Array;
  decodedKind: "node" | "message" | "telemetry" | "position";
}> {
  return [
    {
      port: "NODEINFO_APP",
      payload: schema.user
        .encode({
          id: `!${PRIVATE_TEXT}`,
          longName: PRIVATE_TEXT,
          shortName: "LEAK",
          publicKey: Buffer.from(PRIVATE_TEXT),
        })
        .finish(),
      decodedKind: "node",
    },
    {
      port: "TEXT_MESSAGE_APP",
      payload: Buffer.from(PRIVATE_TEXT),
      decodedKind: "message",
    },
    {
      port: "TELEMETRY_APP",
      payload: schema.telemetry
        .encode({
          time: 1_784_332_802,
          deviceMetrics: { batteryLevel: 73, voltage: 3.9 },
        })
        .finish(),
      decodedKind: "telemetry",
    },
    {
      port: "POSITION_APP",
      payload: schema.position
        .encode({
          latitudeI: 250_475_123,
          longitudeI: 1_215_175_321,
          altitude: 912,
          timestamp: 1_784_332_800,
          seqNumber: 987_654,
          precisionBits: 32,
        })
        .finish(),
      decodedKind: "position",
    },
  ];
}

function packetSource(
  portNum: string,
  payload: Uint8Array,
): NormalizedFromRadio {
  return {
    schemaVersion: 1,
    kind: "packet",
    fromRadioId: PRIVATE_NODE,
    packet: {
      sender: PRIVATE_NODE,
      destination: 0xcafe_babe,
      packetId: 3_735_928_559,
      channel: 7,
      portNum,
      payloadBase64: Buffer.from(payload).toString("base64"),
      deviceRxTimeSeconds: 1_784_332_800,
      rxSnr: 7.25,
      rxRssi: -42,
      hopLimit: 5,
      hopStart: 7,
      viaMqtt: false,
      transportMechanism: "TRANSPORT_LORA",
    },
  };
}

function containsBytes(haystack: Uint8Array, needle: Uint8Array): boolean {
  return Buffer.from(haystack).indexOf(needle) >= 0;
}
