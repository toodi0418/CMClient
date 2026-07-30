import type { PositionCanonicalEvent } from "@cmclient/contracts";

const APRS_DESTINATION = "APTMAG";
const APRS_TRACKER_PATH = "MESHD*,qAO";
const MAX_APRS_DATA_BYTES = 510;
const APRS_CALLSIGN = /^[A-Z0-9]{1,6}(?:-(?:[1-9]|1[0-5]))?$/;
const APRS_PRINTABLE_CHARACTER = /^[ -~]$/;

/** Values resolved from the active CallMesh mapping and provision. */
export interface AprsPositionEncodingOptions {
  mappingCallsign: string;
  mappingSymbolTable: string;
  mappingSymbolCode: string;
  mappingSymbolOverlay?: string | null;
  mappingComment?: string;
  mappingAltitudeMeters?: number;
  provisionAltitudeMeters?: number;
  provisionIgateCallsign: string;
}

export interface EncodedAprsPosition {
  /** Complete TNC2 line without CRLF. The APRS transport adds CRLF. */
  data: string;
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
    !isTimestamp(event.eventTime)
  ) {
    throw new AprsPositionEncodingError();
  }

  const latitude = formatCoordinate(position.latitudeI, "latitude");
  const longitude = formatCoordinate(position.longitudeI, "longitude");
  const symbolTable =
    options.mappingSymbolOverlay ?? options.mappingSymbolTable;
  const courseSpeed = formatCourseSpeed(position);
  const altitude = formatAltitude(resolveAltitudeMeters(position, options));
  const mandatory = `${options.mappingCallsign}>${APRS_DESTINATION},${APRS_TRACKER_PATH},${options.provisionIgateCallsign}:!${latitude}${symbolTable}${longitude}${options.mappingSymbolCode}${courseSpeed}${altitude}`;
  const comment = truncateOptionalComment(
    sanitizeComment(options.mappingComment),
    mandatory,
  );
  const data = `${mandatory}${comment}`;

  if (Buffer.byteLength(data, "utf8") > MAX_APRS_DATA_BYTES) {
    throw new AprsPositionEncodingError();
  }
  return { data };
}

function validateOptions(options: AprsPositionEncodingOptions): void {
  if (
    !APRS_CALLSIGN.test(options.mappingCallsign) ||
    !APRS_CALLSIGN.test(options.provisionIgateCallsign) ||
    !APRS_PRINTABLE_CHARACTER.test(options.mappingSymbolTable) ||
    !APRS_PRINTABLE_CHARACTER.test(options.mappingSymbolCode) ||
    (options.mappingSymbolOverlay !== undefined &&
      options.mappingSymbolOverlay !== null &&
      !APRS_PRINTABLE_CHARACTER.test(options.mappingSymbolOverlay)) ||
    (options.mappingComment !== undefined &&
      typeof options.mappingComment !== "string") ||
    (options.mappingAltitudeMeters !== undefined &&
      !Number.isFinite(options.mappingAltitudeMeters)) ||
    (options.provisionAltitudeMeters !== undefined &&
      !Number.isFinite(options.provisionAltitudeMeters))
  ) {
    throw new AprsPositionEncodingError();
  }
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
  const absoluteDegrees = Math.abs(value / 10_000_000);
  let degrees = Math.floor(absoluteDegrees);
  let minutes = (absoluteDegrees - degrees) * 60;
  if (minutes >= 59.995) {
    degrees += 1;
    minutes = 0;
  }
  if (
    degrees > (kind === "latitude" ? 90 : 180) ||
    (degrees === (kind === "latitude" ? 90 : 180) && minutes !== 0)
  ) {
    throw new AprsPositionEncodingError();
  }
  return `${pad(degrees, degreesWidth)}${minutes.toFixed(2).padStart(5, "0")}${hemisphere}`;
}

function formatCourseSpeed(
  position: PositionCanonicalEvent["position"],
): string {
  const speed = position.groundSpeedMetersPerSecond;
  const track = position.groundTrackDegrees;
  const hasSpeed = Number.isFinite(speed);
  const hasTrack = Number.isFinite(track);
  if (!hasSpeed && !hasTrack) {
    return "";
  }
  const course = hasTrack
    ? Math.round((((track as number) % 360) + 360) % 360)
    : 0;
  const knots = hasSpeed
    ? clamp(Math.round((speed as number) * 1.943844), 0, 999)
    : 0;
  return `${pad(course, 3)}/${pad(knots, 3)}`;
}

function resolveAltitudeMeters(
  position: PositionCanonicalEvent["position"],
  options: AprsPositionEncodingOptions,
): number | undefined {
  if (position.altitudeMslMeters !== undefined) {
    return position.altitudeMslMeters;
  }
  if (position.altitudeHaeMeters !== undefined) {
    if (position.altitudeGeoidalSeparationMeters !== undefined) {
      return (
        position.altitudeHaeMeters - position.altitudeGeoidalSeparationMeters
      );
    }
    // Meshtastic permits HAE when MSL is absent. Retain legacy wire behavior.
    return position.altitudeHaeMeters;
  }
  return (
    options.mappingAltitudeMeters ??
    (options.mappingCallsign === options.provisionIgateCallsign
      ? options.provisionAltitudeMeters
      : undefined)
  );
}

function formatAltitude(altitudeMeters: number | undefined): string {
  if (altitudeMeters === undefined) {
    return "";
  }
  if (!Number.isFinite(altitudeMeters)) {
    throw new AprsPositionEncodingError();
  }
  const feet = Math.round(altitudeMeters * 3.28084);
  if (!Number.isFinite(feet)) {
    throw new AprsPositionEncodingError();
  }
  const boundedFeet = clamp(feet, -99_999, 999_999);
  return boundedFeet < 0
    ? `/A=-${pad(Math.abs(boundedFeet), 5)}`
    : `/A=${pad(boundedFeet, 6)}`;
}

function sanitizeComment(comment: string | undefined): string {
  if (!comment) {
    return "";
  }
  return comment
    .replace(/[\r\n]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function truncateOptionalComment(comment: string, mandatory: string): string {
  let bytes = Buffer.byteLength(mandatory, "utf8");
  if (bytes > MAX_APRS_DATA_BYTES) {
    throw new AprsPositionEncodingError();
  }
  let retained = "";
  for (const codePoint of comment) {
    const codePointBytes = Buffer.byteLength(codePoint, "utf8");
    if (bytes + codePointBytes > MAX_APRS_DATA_BYTES) {
      break;
    }
    retained += codePoint;
    bytes += codePointBytes;
  }
  return retained;
}

function isTimestamp(value: string | undefined): value is string {
  return typeof value === "string" && Number.isFinite(Date.parse(value));
}

function clamp(value: number, minimum: number, maximum: number): number {
  return Math.min(maximum, Math.max(minimum, value));
}

function pad(value: number, width: number): string {
  return String(value).padStart(width, "0");
}
