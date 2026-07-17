import type {
  BacklogClassification,
  MeshObservation,
  NormalizedFromRadio,
  TransportKind,
} from "@cmclient/contracts";

const UTC_ISO_TIMESTAMP = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3})?Z$/;

export interface CaptureMeshObservationInput {
  id: string;
  transport: TransportKind;
  sessionConnectedAt: string;
  ingestedAt: string;
  serverIngestedAt: string;
  normalizedFromRadio: NormalizedFromRadio;
}

export class MeshObservationValidationError extends Error {
  readonly code = "MESH_OBSERVATION_INVALID";

  constructor() {
    super("MESH_OBSERVATION_INVALID");
  }
}

/**
 * Captures transport observations without assigning source event time. Position
 * ordering remains the responsibility of the later position domain pipeline.
 */
export function createMeshObservation(
  input: CaptureMeshObservationInput,
): MeshObservation {
  if (!input.id.trim() || input.id.length > 128) {
    throw new MeshObservationValidationError();
  }
  validateTimestamp(input.sessionConnectedAt);
  validateTimestamp(input.ingestedAt);
  validateTimestamp(input.serverIngestedAt);
  const deviceRxTimeSeconds =
    input.normalizedFromRadio.packet?.deviceRxTimeSeconds;
  return {
    schemaVersion: 1,
    ...input,
    ...(deviceRxTimeSeconds !== undefined ? { deviceRxTimeSeconds } : {}),
    backlogClassification: classifyBacklog(
      input.sessionConnectedAt,
      deviceRxTimeSeconds,
    ),
  };
}

/**
 * `rx_time` has only second precision. A second straddling the session start
 * is unknown rather than guessed; it is never a source position event time.
 */
export function classifyBacklog(
  sessionConnectedAt: string,
  deviceRxTimeSeconds: number | undefined,
): BacklogClassification {
  const sessionStartMs = timestampMilliseconds(sessionConnectedAt);
  if (deviceRxTimeSeconds === undefined) {
    return "unknown";
  }
  if (
    !Number.isInteger(deviceRxTimeSeconds) ||
    deviceRxTimeSeconds < 0 ||
    deviceRxTimeSeconds > 4_294_967_295
  ) {
    throw new MeshObservationValidationError();
  }
  const deviceSecondStartMs = deviceRxTimeSeconds * 1_000;
  const deviceSecondEndMs = deviceSecondStartMs + 999;
  if (deviceSecondEndMs < sessionStartMs) {
    return "backlog";
  }
  if (deviceSecondStartMs >= sessionStartMs) {
    return "live";
  }
  return "unknown";
}

function validateTimestamp(value: string): void {
  timestampMilliseconds(value);
}

function timestampMilliseconds(value: string): number {
  if (!UTC_ISO_TIMESTAMP.test(value)) {
    throw new MeshObservationValidationError();
  }
  const milliseconds = Date.parse(value);
  if (!Number.isFinite(milliseconds)) {
    throw new MeshObservationValidationError();
  }
  return milliseconds;
}
