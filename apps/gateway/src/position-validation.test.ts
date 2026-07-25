import { describe, expect, it } from "vitest";

import type { PositionCanonicalEvent } from "@cmclient/contracts";

import { validatePositionForAprs } from "./position-validation";

const NOW = new Date("2026-07-18T00:10:00.000Z");

describe("position APRS validation", () => {
  it("requires exactly 32-bit precision and complete coordinates", () => {
    expect(
      validatePositionForAprs(event({ precisionBits: 31 }), { now: NOW }),
    ).toEqual({ accepted: false, code: "POSITION_PRECISION_INSUFFICIENT" });
    const missingLatitude = event({});
    missingLatitude.position = { ...missingLatitude.position };
    delete missingLatitude.position.latitudeI;
    expect(validatePositionForAprs(missingLatitude, { now: NOW })).toEqual({
      accepted: false,
      code: "POSITION_PRECISION_INSUFFICIENT",
    });
  });

  it("rejects 1970, future, and implausibly jumping clocks", () => {
    expect(
      validatePositionForAprs(event({}, "1970-01-01T00:00:01.000Z"), {
        now: NOW,
      }),
    ).toEqual({ accepted: false, code: "POSITION_CLOCK_INVALID" });
    expect(
      validatePositionForAprs(event({}, "2026-07-18T00:20:01.000Z"), {
        now: NOW,
      }),
    ).toEqual({ accepted: false, code: "POSITION_CLOCK_INVALID" });
    expect(
      validatePositionForAprs(event({}, "2026-07-20T00:00:00.000Z"), {
        now: new Date("2026-07-20T00:01:00.000Z"),
        previousTrustedEventTime: "2026-07-18T00:00:00.000Z",
      }),
    ).toEqual({ accepted: false, code: "POSITION_QUARANTINED" });
  });

  it("keeps MSL altitude zero and preserves partial speed/track for Legacy formatting", () => {
    const result = validatePositionForAprs(
      event({
        altitudeMslMeters: 0,
        altitudeHaeMeters: 42,
        groundTrackDegrees: 90,
      }),
      { now: NOW },
    );
    expect(result).toMatchObject({ accepted: true, speedTrackIncluded: true });
    if (!result.accepted) {
      throw new Error("fixture should be valid");
    }
    expect(result.event.position).toEqual({
      latitudeI: 250000000,
      longitudeI: 1215000000,
      precisionBits: 32,
      altitudeMslMeters: 0,
      altitudeHaeMeters: 42,
      groundTrackDegrees: 90,
    });
  });

  it("requires every provided speed and ground track component to be plausible", () => {
    expect(
      validatePositionForAprs(
        event({ groundSpeedMetersPerSecond: 121, groundTrackDegrees: 90 }),
        { now: NOW },
      ),
    ).toEqual({ accepted: false, code: "POSITION_SPEED_ANOMALY" });
    expect(
      validatePositionForAprs(
        event({ groundSpeedMetersPerSecond: 10, groundTrackDegrees: 90 }),
        { now: NOW },
      ),
    ).toMatchObject({ accepted: true, speedTrackIncluded: true });
  });
});

function event(
  position: Partial<PositionCanonicalEvent["position"]>,
  eventTime = "2026-07-18T00:00:00.000Z",
): PositionCanonicalEvent {
  return {
    schemaVersion: 1,
    id: "position-event-fixture",
    canonicalKey: "canonical-fixture",
    meshNetworkId: "fixture-network",
    nodeNum: 42,
    sourceObservationId: "position-observation-fixture",
    payloadHash: "a".repeat(64),
    eventTime,
    eventTimeSource: "position_timestamp",
    position: {
      latitudeI: 250000000,
      longitudeI: 1215000000,
      precisionBits: 32,
      ...position,
    },
    createdAt: "2026-07-18T00:00:01.000Z",
  };
}
