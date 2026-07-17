import type {
  MeshMessage,
  MeshNode,
  MeshTelemetry,
  NormalizedMeshPacket,
} from "@cmclient/contracts";

import type { MeshtasticSchema } from "./schema.js";

const TO_OBJECT_OPTIONS = {
  bytes: Uint8Array,
  defaults: false,
  enums: String,
  longs: Number,
  oneofs: true,
};

export type DecodedApplicationPayload =
  | {
      kind: "node";
      node: Pick<
        MeshNode,
        | "nodeNum"
        | "userId"
        | "longName"
        | "shortName"
        | "hardwareModel"
        | "role"
      >;
    }
  | {
      kind: "message";
      message: Pick<
        MeshMessage,
        "sender" | "destination" | "packetId" | "channel" | "text"
      >;
    }
  | {
      kind: "telemetry";
      telemetry: Pick<
        MeshTelemetry,
        | "nodeNum"
        | "packetId"
        | "metricKind"
        | "metrics"
        | "telemetryTimeSeconds"
      >;
    }
  | { kind: "ignored"; reasonCode: string };

export class MeshtasticApplicationDecoder {
  constructor(private readonly schema: MeshtasticSchema) {}

  decode(packet: NormalizedMeshPacket): DecodedApplicationPayload {
    if (!packet.portNum || !packet.payloadBase64) {
      return {
        kind: "ignored",
        reasonCode: "MESH_APPLICATION_PAYLOAD_MISSING",
      };
    }
    try {
      const payload = Buffer.from(packet.payloadBase64, "base64");
      switch (packet.portNum) {
        case "NODEINFO_APP":
          return this.decodeNode(packet, payload);
        case "TEXT_MESSAGE_APP":
          return this.decodeTextMessage(packet, payload);
        case "TELEMETRY_APP":
          return this.decodeTelemetry(packet, payload);
        default:
          return {
            kind: "ignored",
            reasonCode: "MESH_APPLICATION_PORT_UNSUPPORTED",
          };
      }
    } catch {
      return {
        kind: "ignored",
        reasonCode: "MESH_APPLICATION_PAYLOAD_DECODE_FAILED",
      };
    }
  }

  private decodeNode(
    packet: NormalizedMeshPacket,
    payload: Uint8Array,
  ): DecodedApplicationPayload {
    if (packet.sender === undefined) {
      return { kind: "ignored", reasonCode: "MESH_NODE_ID_MISSING" };
    }
    const source = toRecord(
      this.schema.user.toObject(
        this.schema.user.decode(payload),
        TO_OBJECT_OPTIONS,
      ),
    );
    if (!source) {
      return {
        kind: "ignored",
        reasonCode: "MESH_APPLICATION_PAYLOAD_DECODE_FAILED",
      };
    }
    return {
      kind: "node",
      node: {
        nodeNum: packet.sender,
        ...optionalStringProperty("userId", source.id),
        ...optionalStringProperty("longName", source.longName),
        ...optionalStringProperty("shortName", source.shortName),
        ...optionalStringProperty("hardwareModel", source.hwModel),
        ...optionalStringProperty("role", source.role),
      },
    };
  }

  private decodeTextMessage(
    packet: NormalizedMeshPacket,
    payload: Uint8Array,
  ): DecodedApplicationPayload {
    if (packet.sender === undefined) {
      return { kind: "ignored", reasonCode: "MESH_MESSAGE_SENDER_MISSING" };
    }
    if (packet.channel !== undefined && packet.channel > 255) {
      return { kind: "ignored", reasonCode: "MESH_MESSAGE_CHANNEL_INVALID" };
    }
    const text = new TextDecoder("utf-8", { fatal: true }).decode(payload);
    if (!text || text.length > 512) {
      return { kind: "ignored", reasonCode: "MESH_MESSAGE_CONTENT_INVALID" };
    }
    return {
      kind: "message",
      message: {
        sender: packet.sender,
        ...optionalNumberProperty("destination", packet.destination),
        ...optionalNumberProperty("packetId", packet.packetId),
        ...optionalNumberProperty("channel", packet.channel),
        text,
      },
    };
  }

  private decodeTelemetry(
    packet: NormalizedMeshPacket,
    payload: Uint8Array,
  ): DecodedApplicationPayload {
    if (packet.sender === undefined) {
      return { kind: "ignored", reasonCode: "MESH_TELEMETRY_NODE_ID_MISSING" };
    }
    const source = toRecord(
      this.schema.telemetry.toObject(
        this.schema.telemetry.decode(payload),
        TO_OBJECT_OPTIONS,
      ),
    );
    const metricKind = source && optionalString(source.variant);
    const metrics = metricKind
      ? primitiveRecord(source?.[metricKind])
      : undefined;
    if (!metricKind || !metrics || Object.keys(metrics).length === 0) {
      return {
        kind: "ignored",
        reasonCode: "MESH_TELEMETRY_METRICS_MISSING",
      };
    }
    const telemetryTimeSeconds = optionalTimestamp(source?.time);
    return {
      kind: "telemetry",
      telemetry: {
        nodeNum: packet.sender,
        ...optionalNumberProperty("packetId", packet.packetId),
        metricKind,
        metrics,
        ...(telemetryTimeSeconds !== undefined ? { telemetryTimeSeconds } : {}),
      },
    };
  }
}

function toRecord(value: unknown): Record<string, unknown> | undefined {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

function optionalString(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

function optionalStringProperty<Key extends string>(
  key: Key,
  value: unknown,
): Record<Key, string> | Record<never, never> {
  const text = optionalString(value);
  return text ? ({ [key]: text } as Record<Key, string>) : {};
}

function optionalNumberProperty<Key extends string>(
  key: Key,
  value: number | undefined,
): Record<Key, number> | Record<never, never> {
  return value === undefined ? {} : ({ [key]: value } as Record<Key, number>);
}

function optionalTimestamp(value: unknown): number | undefined {
  return typeof value === "number" &&
    Number.isInteger(value) &&
    value > 0 &&
    value <= 4_294_967_295
    ? value
    : undefined;
}

function primitiveRecord(value: unknown): MeshTelemetry["metrics"] | undefined {
  const source = toRecord(value);
  if (!source) {
    return undefined;
  }
  const metrics: MeshTelemetry["metrics"] = {};
  for (const [key, metric] of Object.entries(source)) {
    if (
      (typeof metric === "string" && metric.length <= 512) ||
      typeof metric === "boolean" ||
      (typeof metric === "number" && Number.isFinite(metric))
    ) {
      metrics[key] = metric;
    }
  }
  return metrics;
}
