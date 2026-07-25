import { describe, expect, it } from "vitest";

import type { PositionCanonicalEvent } from "@cmclient/contracts";

import {
  AprsPositionEncodingError,
  encodeAprsPosition,
  type AprsPositionEncodingOptions,
} from "./aprs-position";

const options: AprsPositionEncodingOptions = {
  mappingCallsign: "N0CALL-7",
  provisionIgateCallsign: "N1GATE-10",
  mappingSymbolTable: "/",
  mappingSymbolCode: ">",
  mappingComment: "Fixture tracker",
};
const optionsWithoutComment = { ...options };
delete optionsWithoutComment.mappingComment;

type PositionOverrides = {
  [Key in keyof PositionCanonicalEvent["position"]]?:
    PositionCanonicalEvent["position"][Key] | undefined;
};

describe("legacy-compatible APRS Tracker position encoder", () => {
  it("emits the fixed APTMAG/MESHD*/qAO wire format without CRLF", () => {
    expect(encodeAprsPosition(event(), options)).toEqual({
      data: "N0CALL-7>APTMAG,MESHD*,qAO,N1GATE-10:!2500.00N/12130.00E>090/009/A=000000Fixture tracker",
    });
    const data = encodeAprsPosition(event(), options).data;
    expect(data).not.toContain("CM2/");
    expect(data).not.toContain("\r");
    expect(data).not.toContain("\n");
  });

  it("uses mapping overlay/comment/altitude with legacy byte behavior", () => {
    const withoutPositionAltitude = event({
      altitudeMslMeters: undefined,
      groundSpeedMetersPerSecond: undefined,
      groundTrackDegrees: 45,
    });
    expect(
      encodeAprsPosition(withoutPositionAltitude, {
        ...options,
        mappingSymbolOverlay: "8",
        mappingSymbolCode: "[",
        mappingComment: "  Alpha\r\n  Beta  ",
        mappingAltitudeMeters: 10,
      }).data,
    ).toBe(
      "N0CALL-7>APTMAG,MESHD*,qAO,N1GATE-10:!2500.00N812130.00E[045/000/A=000033Alpha Beta",
    );
  });

  it("prefers position altitude and fills a missing course with zero", () => {
    const position = event({
      altitudeMslMeters: -20,
      groundTrackDegrees: undefined,
      groundSpeedMetersPerSecond: 4.5,
    });
    expect(
      encodeAprsPosition(position, {
        ...optionsWithoutComment,
        mappingAltitudeMeters: 500,
      }).data,
    ).toBe(
      "N0CALL-7>APTMAG,MESHD*,qAO,N1GATE-10:!2500.00N/12130.00E>000/009/A=000000",
    );
  });

  it("carries rounded minutes while retaining modern coordinate bounds", () => {
    expect(
      encodeAprsPosition(
        event({ latitudeI: 249_999_167, longitudeI: 1_219_999_167 }),
        optionsWithoutComment,
      ).data,
    ).toContain(":!2500.00N/12200.00E>");

    expect(() =>
      encodeAprsPosition(event({ latitudeI: 900_000_001 }), options),
    ).toThrow(AprsPositionEncodingError);
    expect(() =>
      encodeAprsPosition(event({ longitudeI: -1_800_000_001 }), options),
    ).toThrow(AprsPositionEncodingError);
  });

  it("fails closed without full precision or a trusted event time", () => {
    expect(() =>
      encodeAprsPosition(event({ precisionBits: 31 }), options),
    ).toThrow(AprsPositionEncodingError);
    const missingTime = event();
    delete missingTime.eventTime;
    expect(() => encodeAprsPosition(missingTime, options)).toThrow(
      AprsPositionEncodingError,
    );
  });

  it("enforces the 512-byte APRS line limit using UTF-8 bytes", () => {
    const base = encodeAprsPosition(event(), optionsWithoutComment).data;
    const exactLimitComment = "x".repeat(512 - Buffer.byteLength(base, "utf8"));
    expect(
      Buffer.byteLength(
        encodeAprsPosition(event(), {
          ...options,
          mappingComment: exactLimitComment,
        }).data,
        "utf8",
      ),
    ).toBe(512);
    expect(() =>
      encodeAprsPosition(event(), {
        ...options,
        mappingComment: `${exactLimitComment}x`,
      }),
    ).toThrow(AprsPositionEncodingError);
    expect(() =>
      encodeAprsPosition(event(), {
        ...options,
        mappingComment: "測".repeat(200),
      }),
    ).toThrow(AprsPositionEncodingError);
  });

  it("rejects unsafe callsigns and symbols", () => {
    expect(() =>
      encodeAprsPosition(event(), {
        ...options,
        provisionIgateCallsign: "N1GATE-16",
      }),
    ).toThrow(AprsPositionEncodingError);
    expect(() =>
      encodeAprsPosition(event(), {
        ...options,
        mappingSymbolOverlay: "\n",
      }),
    ).toThrow(AprsPositionEncodingError);
  });
});

function event(position: PositionOverrides = {}): PositionCanonicalEvent {
  const sample: PositionCanonicalEvent["position"] = {
    latitudeI: 250_000_000,
    longitudeI: 1_215_000_000,
    precisionBits: 32,
    altitudeMslMeters: 0,
    groundSpeedMetersPerSecond: 4.5,
    groundTrackDegrees: 90,
  };
  for (const [key, value] of Object.entries(position)) {
    if (value === undefined) {
      delete sample[key as keyof typeof sample];
    } else {
      Object.assign(sample, { [key]: value });
    }
  }
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
    position: sample,
    createdAt: "2026-07-18T00:00:01.000Z",
  };
}
