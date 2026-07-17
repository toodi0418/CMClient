import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { describe, expect, it } from "vitest";

import { MeshtasticProtobufCodec } from "./protobuf";
import {
  MESHTASTIC_PROTO_SHA256,
  MESHTASTIC_SCHEMA_VERSION,
  defaultProtoDirectory,
  fingerprintMeshtasticProtoDirectory,
  loadMeshtasticSchema,
} from "./schema";

const fixture = JSON.parse(
  readFileSync(
    resolve(
      import.meta.dirname,
      "../../../../test/fixtures/meshtastic-protobuf-compatibility.json",
    ),
    "utf8",
  ),
) as {
  expectedConfigCompleteId: number;
  expectedPacket: unknown;
  fromRadioConfigCompleteHex: string;
  fromRadioPacketHex: string;
  schemaVersion: number;
  schemaVersionLabel: string;
};

describe("Meshtastic protobuf schema", () => {
  it("locks the complete Meshtastic proto corpus fingerprint", () => {
    expect(fixture.schemaVersion).toBe(1);
    expect(fixture.schemaVersionLabel).toBe(MESHTASTIC_SCHEMA_VERSION);
    expect(fingerprintMeshtasticProtoDirectory(defaultProtoDirectory())).toBe(
      MESHTASTIC_PROTO_SHA256,
    );
  });

  it("decodes the versioned config completion compatibility fixture", async () => {
    const schema = await loadMeshtasticSchema();
    const codec = new MeshtasticProtobufCodec(schema);
    const payload = Buffer.from(fixture.fromRadioConfigCompleteHex, "hex");

    expect(
      codec.isConfigComplete(payload, fixture.expectedConfigCompleteId),
    ).toBe(true);
    expect(codec.isConfigComplete(payload, 1)).toBe(false);
    expect(codec.normalizeFromRadio(payload)).toMatchObject({
      kind: "config_complete",
      configCompleteId: fixture.expectedConfigCompleteId,
    });
  });

  it("normalizes a recorded wire fixture without invented field IDs", async () => {
    const schema = await loadMeshtasticSchema();
    const codec = new MeshtasticProtobufCodec(schema);
    const payload = Buffer.from(fixture.fromRadioPacketHex, "hex");

    expect(codec.normalizeFromRadio(payload)).toEqual(fixture.expectedPacket);
    const request = codec.encodeWantConfig(42);
    expect(
      schema.toRadio.toObject(schema.toRadio.decode(request)),
    ).toMatchObject({
      wantConfigId: 42,
    });
    expect(schema.portNum.values.POSITION_APP).toBeDefined();
  });
});
