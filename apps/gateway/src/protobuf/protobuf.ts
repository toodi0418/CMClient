import type {
  NormalizedFromRadio,
  NormalizedMeshPacket,
} from "@cmclient/contracts";

import type { ConfigSessionCodec } from "../transport/config-session.js";
import type { MeshtasticSchema } from "./schema.js";

const TO_OBJECT_OPTIONS = {
  bytes: Uint8Array,
  defaults: false,
  enums: String,
  longs: Number,
  oneofs: true,
};

export class MeshtasticProtobufCodec implements ConfigSessionCodec {
  constructor(private readonly schema: MeshtasticSchema) {}

  encodeWantConfig(nonce: number): Uint8Array {
    return this.schema.toRadio.encode({ wantConfigId: nonce }).finish();
  }

  isConfigComplete(payload: Uint8Array, nonce: number): boolean {
    const normalized = this.normalizeFromRadio(payload);
    return normalized.configCompleteId === nonce;
  }

  normalizeFromRadio(payload: Uint8Array): NormalizedFromRadio {
    const message = this.schema.fromRadio.decode(payload);
    const source =
      asRecord(this.schema.fromRadio.toObject(message, TO_OBJECT_OPTIONS)) ??
      {};
    const packet = asRecord(source.packet);
    const configCompleteId = asUnsigned(source.configCompleteId);
    const fromRadioId = asUnsigned(source.id);
    return {
      schemaVersion: 1,
      ...(fromRadioId !== undefined ? { fromRadioId } : {}),
      ...(configCompleteId !== undefined ? { configCompleteId } : {}),
      ...(packet ? { packet: normalizeMeshPacket(packet) } : {}),
      kind: packet
        ? "packet"
        : configCompleteId !== undefined
          ? "config_complete"
          : "other",
    };
  }
}

export function normalizeMeshPacket(
  packet: Record<string, unknown>,
): NormalizedMeshPacket {
  const decoded = asRecord(packet.decoded);
  const encrypted = asBytes(packet.encrypted);
  const payload = decoded ? asBytes(decoded.payload) : undefined;
  const sender = asUnsigned(packet.from);
  const destination = asUnsigned(packet.to);
  const packetId = asUnsigned(packet.id);
  const channel = asUnsigned(packet.channel);
  const deviceRxTimeSeconds = asUnsigned(packet.rxTime);
  const rxSnr = asFinite(packet.rxSnr);
  const rxRssi = asInteger(packet.rxRssi);
  const hopLimit = asUnsigned(packet.hopLimit);
  const hopStart = asUnsigned(packet.hopStart);
  return {
    ...(sender !== undefined ? { sender } : {}),
    ...(destination !== undefined ? { destination } : {}),
    ...(packetId !== undefined ? { packetId } : {}),
    ...(channel !== undefined ? { channel } : {}),
    ...(decoded && typeof decoded.portnum === "string"
      ? { portNum: decoded.portnum }
      : {}),
    ...(payload
      ? { payloadBase64: Buffer.from(payload).toString("base64") }
      : {}),
    ...(encrypted
      ? { encryptedPayloadBase64: Buffer.from(encrypted).toString("base64") }
      : {}),
    ...(deviceRxTimeSeconds !== undefined ? { deviceRxTimeSeconds } : {}),
    ...(rxSnr !== undefined ? { rxSnr } : {}),
    ...(rxRssi !== undefined ? { rxRssi } : {}),
    ...(hopLimit !== undefined ? { hopLimit } : {}),
    ...(hopStart !== undefined ? { hopStart } : {}),
    ...(typeof packet.viaMqtt === "boolean" ? { viaMqtt: packet.viaMqtt } : {}),
    ...(typeof packet.transportMechanism === "string"
      ? { transportMechanism: packet.transportMechanism }
      : {}),
  };
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

function asBytes(value: unknown): Uint8Array | undefined {
  return value instanceof Uint8Array && value.length > 0 ? value : undefined;
}

function asUnsigned(value: unknown): number | undefined {
  return typeof value === "number" && Number.isInteger(value) && value >= 0
    ? value
    : undefined;
}

function asInteger(value: unknown): number | undefined {
  return typeof value === "number" && Number.isInteger(value)
    ? value
    : undefined;
}

function asFinite(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value)
    ? value
    : undefined;
}
