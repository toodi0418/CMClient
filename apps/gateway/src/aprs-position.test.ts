import { describe, expect, it } from "vitest";

import type { PositionCanonicalEvent } from "@cmclient/contracts";

import { encodeAprsPosition } from "./aprs-position";

const options = {
  source: "N0CALL-7",
  destination: "APCM20",
  symbolTable: "/",
  symbolCode: ">",
  comment: "fixture",
};
const optionsWithoutComment = {
  source: options.source,
  destination: options.destination,
  symbolTable: options.symbolTable,
  symbolCode: options.symbolCode,
};

describe("deterministic APRS position encoder", () => {
  it("encodes a canonical event as byte-stable APRS Data", () => {
    expect(encodeAprsPosition(event(), options)).toEqual({
      eventMarker: "CM2/aaaaaaaaaaaa",
      data: "N0CALL-7>APCM20:/180000z2500.00N/12130.00E>090/009/A=000000 fixture CM2/aaaaaaaaaaaa",
    });
  });

  it("excludes observation metadata and never substitutes HAE altitude for MSL", () => {
    const first = event({ altitudeHaeMeters: 42 });
    delete first.position.altitudeMslMeters;
    const second = { ...first, createdAt: "2026-07-18T00:00:59.000Z" };
    expect(encodeAprsPosition(first, optionsWithoutComment).data).toBe(
      encodeAprsPosition(second, optionsWithoutComment).data,
    );
    expect(encodeAprsPosition(first, optionsWithoutComment).data).toBe(
      "N0CALL-7>APCM20:/180000z2500.00N/12130.00E>090/009 CM2/aaaaaaaaaaaa",
    );
  });

  it("omits a partial speed or ground-track pair instead of synthesizing zeroes", () => {
    const partial = event();
    delete partial.position.groundSpeedMetersPerSecond;
    expect(encodeAprsPosition(partial, optionsWithoutComment).data).toBe(
      "N0CALL-7>APCM20:/180000z2500.00N/12130.00E>/A=000000 CM2/aaaaaaaaaaaa",
    );
  });
});

function event(
  position: Partial<PositionCanonicalEvent["position"]> = {},
): PositionCanonicalEvent {
  return {
    schemaVersion: 1,
    id: "position-event-fixture",
    canonicalKey: "a".repeat(64),
    meshNetworkId: "fixture-network",
    nodeNum: 42,
    sourceObservationId: "position-observation-fixture",
    payloadHash: "b".repeat(64),
    eventTime: "2026-07-18T00:00:00.000Z",
    eventTimeSource: "position_timestamp",
    position: {
      latitudeI: 250000000,
      longitudeI: 1215000000,
      precisionBits: 32,
      altitudeMslMeters: 0,
      groundSpeedMetersPerSecond: 4.5,
      groundTrackDegrees: 90,
      ...position,
    },
    createdAt: "2026-07-18T00:00:01.000Z",
  };
}
