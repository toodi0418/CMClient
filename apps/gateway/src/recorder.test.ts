import { createHash } from "node:crypto";

import { describe, expect, it } from "vitest";

import { createMeshObservation } from "./observations";
import { MeshtasticApplicationDecoder } from "./protobuf/application";
import { MeshtasticProtobufCodec } from "./protobuf/protobuf";
import { loadMeshtasticSchema } from "./protobuf/schema";
import { projectSyntheticCapture } from "./protobuf/synthetic-capture";
import {
  PacketFixtureSanitizer,
  PacketRecorder,
  replayPacketFixtures,
  type PacketRecordInput,
} from "./recorder";

describe("PacketRecorder", () => {
  it("owns bounded defensive copies of locally recorded raw frames", () => {
    const recorder = new PacketRecorder({ maximumEntries: 2 });
    const first = packetRecord("capture-one", "2026-07-18T00:00:00.000Z");
    const second = packetRecord("capture-two", "2026-07-18T00:00:01.000Z");
    const third = packetRecord("capture-three", "2026-07-18T00:00:02.000Z");
    const captured = recorder.record(first);
    first.rawFrame[0] = 0;
    first.observation.normalizedFromRadio.packet!.sender = 0;

    expect(captured.rawFrame[0]).not.toBe(0);
    expect(captured.observation.normalizedFromRadio.packet?.sender).toBe(42);
    recorder.record(second);
    recorder.record(third);

    expect(recorder.snapshot().map((record) => record.sequence)).toEqual([
      2, 3,
    ]);
  });

  it("rotates by duration and rejects frames outside the physical byte bound", () => {
    let clock = new Date("2026-07-22T00:00:00.000Z");
    const recorder = new PacketRecorder({
      maximumAgeMs: 1_000,
      maximumBytes: 16_384,
      maximumFrameBytes: 64,
      clock: () => clock,
    });
    recorder.record(packetRecord("capture-one", clock.toISOString()));
    clock = new Date(clock.getTime() + 1_001);
    recorder.record(packetRecord("capture-two", clock.toISOString()));
    expect(recorder.snapshot().map((record) => record.sequence)).toEqual([2]);

    expect(() =>
      recorder.record({
        ...packetRecord("capture-oversize", clock.toISOString()),
        rawFrame: new Uint8Array(65),
      }),
    ).toThrowError(/PACKET_RECORD_INVALID/);
  });

  it("seals replayable deterministic synthetic output and clears all retained raw records", async () => {
    const schema = await loadMeshtasticSchema();
    const options = {
      syntheticProjector: (
        source: PacketRecordInput["observation"]["normalizedFromRadio"],
        sequence: number,
      ) => projectSyntheticCapture(schema, source, sequence),
    };
    const first = new PacketRecorder(options);
    const second = new PacketRecorder(options);
    const input = packetRecord("capture-private", "2026-07-18T00:00:00.000Z");
    first.record(input);
    second.record(input);

    const firstExport = first.sealAndSanitize();
    const secondExport = second.sealAndSanitize();
    expect(firstExport).toEqual(secondExport);
    expect(firstExport.digest).toMatch(/^[0-9a-f]{64}$/);
    expect(first.snapshot()).toEqual([]);
    expect(second.snapshot()).toEqual([]);

    const codec = new MeshtasticProtobufCodec(schema);
    const applicationDecoder = new MeshtasticApplicationDecoder(schema);
    const replayDigest = async (): Promise<string> => {
      const decisions: unknown[] = [];
      await replayPacketFixtures(firstExport.fixtureSet, (fixture) => {
        const normalized = codec.normalizeFromRadio(
          Buffer.from(fixture.recording.rawFrameHex, "hex"),
        );
        decisions.push({
          normalized,
          decoded: applicationDecoder.decode(normalized.packet ?? {}),
        });
      });
      return createHash("sha256")
        .update(JSON.stringify(decisions), "utf8")
        .digest("hex");
    };
    expect(await replayDigest()).toBe(await replayDigest());
  });
});

describe("PacketFixtureSanitizer", () => {
  it("replaces identifiers and raw payloads while preserving deterministic timing relationships", () => {
    const recorder = new PacketRecorder();
    const first = recorder.record(
      packetRecord("capture-production-a", "2026-07-18T00:00:00.000Z"),
    );
    const second = recorder.record(
      packetRecord(
        "capture-production-b",
        "2026-07-18T00:00:04.000Z",
        "remote-gateway-secret",
      ),
    );
    const sanitizer = new PacketFixtureSanitizer();
    const sanitized = sanitizer.sanitize([first, second]);

    expect(sanitized).toEqual(sanitizer.sanitize([first, second]));
    expect(sanitized.fixtures).toHaveLength(2);
    expect(sanitized.fixtures[0]).toMatchObject({
      id: "fixture-record-000001",
      recording: {
        gatewayId: "fixture-gateway-a",
        meshNetworkId: "fixture-network-a",
        sessionConnectedAt: "2030-01-02T00:00:00.000Z",
        receivedAt: "2030-01-02T00:00:01.000Z",
        ingestedAt: "2030-01-02T00:00:02.000Z",
        serverIngestedAt: "2030-01-02T00:00:03.000Z",
      },
    });
    expect(sanitized.fixtures[1]).toMatchObject({
      id: "fixture-record-000002",
      recording: { gatewayId: "fixture-gateway-b" },
    });
    const firstPacket = sanitized.fixtures[0]?.normalizedFromRadio.packet;
    const secondPacket = sanitized.fixtures[1]?.normalizedFromRadio.packet;
    expect(firstPacket?.sender).toBe(secondPacket?.sender);
    expect(firstPacket?.packetId).toBe(secondPacket?.packetId);
    expect(firstPacket?.payloadBase64).not.toBe(
      Buffer.from("private message that must not leave the gateway").toString(
        "base64",
      ),
    );
    expect(sanitized.fixtures[0]?.recording.rawFrameHex).toHaveLength(64);
    expect(sanitized.fixtures[0]?.recording.rawFrameHex).not.toBe(
      Buffer.from("sensitive raw packet frame").toString("hex"),
    );
    expect(
      new Date((firstPacket?.deviceRxTimeSeconds ?? 0) * 1_000).toISOString(),
    ).toBe("2030-01-02T00:00:00.000Z");

    const serialized = JSON.stringify(sanitized);
    expect(serialized).not.toContain("capture-production-a");
    expect(serialized).not.toContain("remote-gateway-secret");
    expect(serialized).not.toContain("private-network-secret");
    expect(serialized).not.toContain("sensitive raw packet frame");
    expect(serialized).not.toContain("private message that must not leave");
  });
});

describe("replayPacketFixtures", () => {
  it("serializes callbacks in server ingest order and does not expose mutable fixture state", async () => {
    const recorder = new PacketRecorder();
    const first = recorder.record(
      packetRecord("capture-first", "2026-07-18T00:00:00.000Z"),
    );
    const second = recorder.record(
      packetRecord("capture-second", "2026-07-18T00:00:04.000Z"),
    );
    const fixtureSet = new PacketFixtureSanitizer().sanitize([first, second]);
    const replayed: string[] = [];

    await replayPacketFixtures(
      { ...fixtureSet, fixtures: [...fixtureSet.fixtures].reverse() },
      async (fixture) => {
        replayed.push(fixture.id);
        fixture.recording.gatewayId = "fixture-gateway-mutated";
      },
    );

    expect(replayed).toEqual([
      "fixture-record-000001",
      "fixture-record-000002",
    ]);
    expect(fixtureSet.fixtures[0]?.recording.gatewayId).toBe(
      "fixture-gateway-a",
    );
  });
});

function packetRecord(
  id: string,
  sessionConnectedAt: string,
  gatewayId = "actual-gateway-private",
): PacketRecordInput {
  const sessionMilliseconds = Date.parse(sessionConnectedAt);
  const receivedAt = new Date(sessionMilliseconds + 1_000).toISOString();
  const ingestedAt = new Date(sessionMilliseconds + 2_000).toISOString();
  const serverIngestedAt = new Date(sessionMilliseconds + 3_000).toISOString();
  return {
    gatewayId,
    meshNetworkId: "private-network-secret",
    transport: "tcp",
    transportMetadata: { connectionStatus: "ready", reconnectAttempt: 2 },
    rawFrame: Buffer.from("sensitive raw packet frame"),
    receivedAt,
    observation: createMeshObservation({
      id,
      transport: "tcp",
      sessionConnectedAt,
      ingestedAt,
      serverIngestedAt,
      normalizedFromRadio: {
        schemaVersion: 1,
        kind: "packet",
        fromRadioId: 42,
        packet: {
          sender: 42,
          destination: 99,
          packetId: 1001,
          portNum: "TEXT_MESSAGE_APP",
          payloadBase64: Buffer.from(
            "private message that must not leave the gateway",
          ).toString("base64"),
          encryptedPayloadBase64: Buffer.from(
            "encrypted private payload",
          ).toString("base64"),
          deviceRxTimeSeconds: Math.floor(sessionMilliseconds / 1_000),
        },
      },
    }),
  };
}
