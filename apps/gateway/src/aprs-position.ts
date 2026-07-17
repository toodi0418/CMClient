import type { PositionCanonicalEvent } from "@cmclient/contracts";

export interface AprsPositionEncodingOptions {
  comment?: string;
  destination: string;
  source: string;
  symbolCode: string;
  symbolTable: string;
}

export interface EncodedAprsPosition {
  data: string;
  eventMarker: string;
}

export class AprsPositionEncodingError extends Error {
  readonly code = "APRS_POSITION_ENCODING_INVALID";

  constructor() {
    super("APRS_POSITION_ENCODING_INVALID");
  }
}

export function encodeAprsPosition(
  event: PositionCanonicalEvent,
  options: AprsPositionEncodingOptions,
): EncodedAprsPosition {
  validateOptions(options);
  const position = event.position;
  if (
    position.precisionBits !== 32 ||
    position.latitudeI === undefined ||
    position.longitudeI === undefined ||
    !event.eventTime
  ) {
    throw new AprsPositionEncodingError();
  }
  const timestamp = formatTimestamp(event.eventTime);
  const latitude = formatCoordinate(position.latitudeI, "latitude");
  const longitude = formatCoordinate(position.longitudeI, "longitude");
  const courseSpeed = formatCourseSpeed(position);
  const altitude = formatAltitude(position.altitudeMslMeters);
  const eventMarker = `CM2/${event.canonicalKey.slice(0, 12)}`;
  const comment = [options.comment?.trim(), eventMarker]
    .filter(Boolean)
    .join(" ");
  return {
    eventMarker,
    data: `${options.source}>${options.destination}:/${timestamp}${latitude}${options.symbolTable}${longitude}${options.symbolCode}${courseSpeed}${altitude}${comment ? ` ${comment}` : ""}`,
  };
}

function validateOptions(options: AprsPositionEncodingOptions): void {
  if (
    !/^[A-Z0-9]{1,6}(?:-[0-9]{1,2})?$/.test(options.source) ||
    !/^[A-Z0-9]{1,6}$/.test(options.destination) ||
    !/^[ -~]$/.test(options.symbolTable) ||
    !/^[ -~]$/.test(options.symbolCode) ||
    (options.comment !== undefined &&
      (options.comment.length > 80 || /[\r\n]/.test(options.comment)))
  ) {
    throw new AprsPositionEncodingError();
  }
}

function formatTimestamp(eventTime: string): string {
  const date = new Date(eventTime);
  if (Number.isNaN(date.getTime())) {
    throw new AprsPositionEncodingError();
  }
  return `${pad(date.getUTCDate(), 2)}${pad(date.getUTCHours(), 2)}${pad(date.getUTCMinutes(), 2)}z`;
}

function formatCoordinate(
  value: number,
  kind: "latitude" | "longitude",
): string {
  const limit = kind === "latitude" ? 900_000_000 : 1_800_000_000;
  if (!Number.isInteger(value) || Math.abs(value) > limit) {
    throw new AprsPositionEncodingError();
  }
  const degreesWidth = kind === "latitude" ? 2 : 3;
  const hemisphere =
    kind === "latitude" ? (value < 0 ? "S" : "N") : value < 0 ? "W" : "E";
  let degrees = Math.floor(Math.abs(value) / 10_000_000);
  const fractionalDegrees = (Math.abs(value) % 10_000_000) / 10_000_000;
  let hundredthsMinutes = Math.round(fractionalDegrees * 60 * 100);
  if (hundredthsMinutes === 6_000) {
    degrees += 1;
    hundredthsMinutes = 0;
  }
  return `${pad(degrees, degreesWidth)}${pad(Math.floor(hundredthsMinutes / 100), 2)}.${pad(hundredthsMinutes % 100, 2)}${hemisphere}`;
}

function formatCourseSpeed(
  position: PositionCanonicalEvent["position"],
): string {
  const speed = position.groundSpeedMetersPerSecond;
  const track = position.groundTrackDegrees;
  if (speed === undefined || track === undefined) {
    return "";
  }
  if (
    !Number.isFinite(speed) ||
    !Number.isFinite(track) ||
    speed < 0 ||
    track < 0 ||
    track >= 360
  ) {
    throw new AprsPositionEncodingError();
  }
  const knots = Math.round(speed * 1.943844);
  const course = Math.round(track);
  if (knots > 999 || course > 359) {
    throw new AprsPositionEncodingError();
  }
  return `${pad(course, 3)}/${pad(knots, 3)}`;
}

function formatAltitude(altitudeMslMeters: number | undefined): string {
  if (altitudeMslMeters === undefined) {
    return "";
  }
  const feet = Math.round(altitudeMslMeters * 3.28084);
  if (!Number.isFinite(feet) || feet < 0 || feet > 999_999) {
    throw new AprsPositionEncodingError();
  }
  return `/A=${pad(feet, 6)}`;
}

function pad(value: number, width: number): string {
  return String(value).padStart(width, "0");
}
