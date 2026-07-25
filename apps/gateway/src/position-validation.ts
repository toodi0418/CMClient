import type {
  PositionCanonicalEvent,
  PositionDecisionCode,
  PositionSample,
} from "@cmclient/contracts";

const EARLIEST_GPS_TIME_MS = Date.parse("2000-01-01T00:00:00.000Z");

export interface PositionValidationOptions {
  now?: Date;
  maximumFutureMs?: number;
  maximumForwardClockJumpMs?: number;
  previousTrustedEventTime?: string;
  maximumGroundSpeedMetersPerSecond?: number;
}

export type PositionValidationResult =
  | {
      accepted: true;
      event: PositionCanonicalEvent;
      speedTrackIncluded: boolean;
    }
  | { accepted: false; code: PositionDecisionCode };

export function validatePositionForAprs(
  event: PositionCanonicalEvent,
  options: PositionValidationOptions = {},
): PositionValidationResult {
  const position = event.position;
  if (
    position.precisionBits !== 32 ||
    position.latitudeI === undefined ||
    position.longitudeI === undefined
  ) {
    return { accepted: false, code: "POSITION_PRECISION_INSUFFICIENT" };
  }
  if (!event.eventTime || !isTrustedClock(event.eventTime, options)) {
    return { accepted: false, code: "POSITION_CLOCK_INVALID" };
  }
  if (
    options.previousTrustedEventTime &&
    isClockJump(
      options.previousTrustedEventTime,
      event.eventTime,
      options.maximumForwardClockJumpMs ?? 24 * 60 * 60 * 1_000,
    )
  ) {
    return { accepted: false, code: "POSITION_QUARANTINED" };
  }
  const speedTrack = normalizeSpeedTrack(
    position,
    options.maximumGroundSpeedMetersPerSecond ?? 120,
  );
  if (speedTrack === "invalid") {
    return { accepted: false, code: "POSITION_SPEED_ANOMALY" };
  }
  const positionWithoutSpeedTrack = { ...position };
  delete positionWithoutSpeedTrack.groundSpeedMetersPerSecond;
  delete positionWithoutSpeedTrack.groundTrackDegrees;
  return {
    accepted: true,
    event: {
      ...event,
      position: speedTrack.included ? position : positionWithoutSpeedTrack,
    },
    speedTrackIncluded: speedTrack.included,
  };
}

function isTrustedClock(
  value: string,
  options: PositionValidationOptions,
): boolean {
  const timestamp = Date.parse(value);
  const now = (options.now ?? new Date()).getTime();
  return (
    Number.isFinite(timestamp) &&
    timestamp >= EARLIEST_GPS_TIME_MS &&
    timestamp <= now + (options.maximumFutureMs ?? 5 * 60 * 1_000)
  );
}

function isClockJump(
  previous: string,
  candidate: string,
  maximumForwardClockJumpMs: number,
): boolean {
  const previousMs = Date.parse(previous);
  const candidateMs = Date.parse(candidate);
  return (
    Number.isFinite(previousMs) &&
    Number.isFinite(candidateMs) &&
    candidateMs - previousMs > maximumForwardClockJumpMs
  );
}

function normalizeSpeedTrack(
  position: PositionSample,
  maximumGroundSpeedMetersPerSecond: number,
): { included: boolean } | "invalid" {
  const speed = position.groundSpeedMetersPerSecond;
  const track = position.groundTrackDegrees;
  if (speed === undefined && track === undefined) {
    return { included: false };
  }
  const speedValid =
    speed === undefined ||
    (Number.isFinite(speed) &&
      speed >= 0 &&
      speed <= maximumGroundSpeedMetersPerSecond);
  const trackValid =
    track === undefined ||
    (Number.isFinite(track) && track >= 0 && track < 360);
  return speedValid && trackValid ? { included: true } : "invalid";
}
