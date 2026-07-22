import { createHash } from "node:crypto";

import type { NormalizedFromRadio } from "@cmclient/contracts";
import { Enum, Type, type Field } from "protobufjs";

import { MeshtasticProtobufCodec } from "./protobuf.js";
import type { MeshtasticSchema } from "./schema.js";

const APPLICATION_PORTS = [
  "NODEINFO_APP",
  "TEXT_MESSAGE_APP",
  "TELEMETRY_APP",
  "POSITION_APP",
] as const;
const SYNTHETIC_NODE_BASE = 0xf1c7_0000;
const SYNTHETIC_DESTINATION_BASE = 0xf1c8_0000;
const SYNTHETIC_PACKET_BASE = 0x5a00_0000;
const SYNTHETIC_FROM_RADIO_BASE = 0x4c00_0000;

type ApplicationPort = (typeof APPLICATION_PORTS)[number];

export interface SyntheticCaptureProjection {
  frame: Uint8Array;
  normalizedFromRadio: NormalizedFromRadio;
}

/**
 * Builds a replayable protobuf fixture from field presence only. No scalar or
 * byte value from the source is copied into the projected frame.
 */
export function projectSyntheticCapture(
  schema: MeshtasticSchema,
  source: NormalizedFromRadio,
  sequence: number,
): SyntheticCaptureProjection {
  assertSequence(sequence);
  const message: Record<string, unknown> = {
    ...(source.fromRadioId === undefined
      ? {}
      : { id: syntheticIdentifier(SYNTHETIC_FROM_RADIO_BASE, sequence) }),
  };

  switch (source.kind) {
    case "packet":
      message.packet = projectPacket(schema, source, sequence);
      break;
    case "config_complete":
      message.configCompleteId = syntheticIdentifier(
        SYNTHETIC_PACKET_BASE,
        sequence,
      );
      break;
    case "other":
      // Keep the frame non-empty without exposing an ignored source variant.
      message.rebooted = true;
      break;
  }

  const verificationError = schema.fromRadio.verify(message);
  if (verificationError) {
    throw new TypeError("SYNTHETIC_CAPTURE_SCHEMA_INVALID");
  }
  const frame = schema.fromRadio.encode(message).finish();
  const normalizedFromRadio = new MeshtasticProtobufCodec(
    schema,
  ).normalizeFromRadio(frame);
  if (normalizedFromRadio.kind !== source.kind) {
    throw new TypeError("SYNTHETIC_CAPTURE_KIND_MISMATCH");
  }
  return { frame, normalizedFromRadio };
}

function projectPacket(
  schema: MeshtasticSchema,
  source: NormalizedFromRadio,
  sequence: number,
): Record<string, unknown> {
  const packet = source.packet ?? {};
  const port = applicationPort(packet.portNum);
  const projected: Record<string, unknown> = {
    ...(packet.sender === undefined && port === undefined
      ? {}
      : { from: syntheticIdentifier(SYNTHETIC_NODE_BASE, sequence) }),
    ...(packet.destination === undefined
      ? {}
      : {
          to: syntheticIdentifier(SYNTHETIC_DESTINATION_BASE, sequence),
        }),
    ...(packet.packetId === undefined
      ? {}
      : { id: syntheticIdentifier(SYNTHETIC_PACKET_BASE, sequence) }),
    ...(packet.channel === undefined ? {} : { channel: 1 }),
    ...(packet.deviceRxTimeSeconds === undefined ? {} : { rxTime: 1 }),
    ...(packet.rxSnr === undefined ? {} : { rxSnr: 1 }),
    ...(packet.rxRssi === undefined ? {} : { rxRssi: -100 }),
    ...(packet.hopLimit === undefined ? {} : { hopLimit: 1 }),
    ...(packet.hopStart === undefined ? {} : { hopStart: 1 }),
    ...(packet.viaMqtt === undefined ? {} : { viaMqtt: true }),
    ...(packet.transportMechanism === undefined
      ? {}
      : { transportMechanism: 7 }),
  };

  if (port) {
    projected.decoded = {
      portnum: schema.portNum.values[port],
      payload: projectApplicationPayload(
        schema,
        port,
        packet.payloadBase64,
        sequence,
      ),
    };
  } else if (
    packet.portNum !== undefined ||
    packet.payloadBase64 !== undefined
  ) {
    projected.decoded = {
      ...(packet.payloadBase64 === undefined
        ? {}
        : { payload: syntheticBytes(sequence, "opaque-payload") }),
    };
  } else if (packet.encryptedPayloadBase64 !== undefined) {
    projected.encrypted = syntheticBytes(sequence, "encrypted-payload");
  }
  return projected;
}

function projectApplicationPayload(
  schema: MeshtasticSchema,
  port: ApplicationPort,
  payloadBase64: string | undefined,
  sequence: number,
): Uint8Array {
  switch (port) {
    case "NODEINFO_APP":
      return encodeProjectedMessage(
        schema.user,
        projectPayloadShape(schema.user, payloadBase64, sequence, "user"),
        { id: "!synthetic" },
      );
    case "TEXT_MESSAGE_APP":
      return Buffer.from(`Synthetic capture ${sequence}`, "utf8");
    case "TELEMETRY_APP": {
      const shape = projectPayloadShape(
        schema.telemetry,
        payloadBase64,
        sequence,
        "telemetry",
      );
      return encodeProjectedMessage(
        schema.telemetry,
        hasTelemetryMetrics(schema.telemetry, shape)
          ? shape
          : { deviceMetrics: { batteryLevel: 1 } },
        { deviceMetrics: { batteryLevel: 1 } },
      );
    }
    case "POSITION_APP": {
      const shape = projectPayloadShape(
        schema.position,
        payloadBase64,
        sequence,
        "position",
      );
      return encodeProjectedMessage(
        schema.position,
        Object.keys(shape).length > 0 ? shape : { precisionBits: 1 },
        { precisionBits: 1 },
      );
    }
  }
}

function projectPayloadShape(
  type: Type,
  payloadBase64: string | undefined,
  sequence: number,
  path: string,
): Record<string, unknown> {
  if (!payloadBase64) {
    return {};
  }
  try {
    const decoded = type.decode(Buffer.from(payloadBase64, "base64"));
    return projectMessageShape(type, decoded, sequence, path);
  } catch {
    return {};
  }
}

function projectMessageShape(
  type: Type,
  source: unknown,
  sequence: number,
  path: string,
): Record<string, unknown> {
  const record = source as Record<string, unknown>;
  const projected: Record<string, unknown> = {};
  for (const field of type.fieldsArray) {
    if (!Object.prototype.hasOwnProperty.call(record, field.name)) {
      continue;
    }
    const sourceValue = record[field.name];
    const fieldPath = `${path}.${field.name}`;
    if (field.map) {
      projected[field.name] = {
        synthetic: projectFieldValue(field, undefined, sequence, fieldPath),
      };
    } else if (field.repeated) {
      projected[field.name] = [
        projectFieldValue(
          field,
          Array.isArray(sourceValue) ? sourceValue[0] : undefined,
          sequence,
          fieldPath,
        ),
      ];
    } else {
      projected[field.name] = projectFieldValue(
        field,
        sourceValue,
        sequence,
        fieldPath,
      );
    }
  }
  return projected;
}

function projectFieldValue(
  field: Field,
  sourceValue: unknown,
  sequence: number,
  path: string,
): unknown {
  if (field.resolvedType instanceof Type) {
    return projectMessageShape(
      field.resolvedType,
      sourceValue ?? {},
      sequence,
      path,
    );
  }
  if (field.resolvedType instanceof Enum) {
    return Object.values(field.resolvedType.values).find((id) => id !== 0) ?? 0;
  }
  switch (field.type) {
    case "string":
      return syntheticString(field.name);
    case "bytes":
      return syntheticBytes(sequence, path);
    case "bool":
      return true;
    case "double":
    case "float":
    case "int32":
    case "sint32":
    case "sfixed32":
    case "uint32":
    case "fixed32":
    case "int64":
    case "sint64":
    case "sfixed64":
    case "uint64":
    case "fixed64":
      return 1;
    default:
      return 1;
  }
}

function encodeProjectedMessage(
  type: Type,
  projected: Record<string, unknown>,
  fallback: Record<string, unknown>,
): Uint8Array {
  const message = type.verify(projected) ? fallback : projected;
  const verificationError = type.verify(message);
  if (verificationError) {
    throw new TypeError("SYNTHETIC_CAPTURE_PAYLOAD_SCHEMA_INVALID");
  }
  return type.encode(message).finish();
}

function hasTelemetryMetrics(
  telemetryType: Type,
  projected: Record<string, unknown>,
): boolean {
  const variant = telemetryType.oneofs.variant;
  return (
    variant?.oneof.some((fieldName) => {
      const value = projected[fieldName];
      return (
        value !== null &&
        typeof value === "object" &&
        Object.keys(value).length > 0
      );
    }) ?? false
  );
}

function applicationPort(
  port: string | undefined,
): ApplicationPort | undefined {
  return APPLICATION_PORTS.find((candidate) => candidate === port);
}

function syntheticIdentifier(base: number, sequence: number): number {
  return base + ((sequence - 1) % 0xffff);
}

function syntheticString(fieldName: string): string {
  switch (fieldName) {
    case "id":
      return "!synthetic";
    case "longName":
      return "Synthetic Node";
    case "shortName":
      return "SN";
    default:
      return "synthetic";
  }
}

function syntheticBytes(sequence: number, field: string): Uint8Array {
  return createHash("sha256")
    .update(`cmclient-synthetic-capture-v1:${sequence}:${field}`)
    .digest()
    .subarray(0, 16);
}

function assertSequence(sequence: number): void {
  if (
    !Number.isSafeInteger(sequence) ||
    sequence < 1 ||
    sequence > 4_294_967_295
  ) {
    throw new TypeError("SYNTHETIC_CAPTURE_SEQUENCE_INVALID");
  }
}
