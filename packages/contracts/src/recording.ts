import { Type, type Static } from "@sinclair/typebox";

import {
  ConnectionStatusSchema,
  NormalizedFromRadioSchema,
  TransportKindSchema,
} from "./transport.js";

const UTC_ISO_TIMESTAMP =
  "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d{3})?Z$";

export const PacketTransportMetadataSchema = Type.Object(
  {
    connectionStatus: ConnectionStatusSchema,
    reconnectAttempt: Type.Optional(Type.Integer({ minimum: 0 })),
  },
  { additionalProperties: false },
);

export const SanitizedPacketFixtureEntrySchema = Type.Object(
  {
    id: Type.String({ minLength: 1, maxLength: 128 }),
    sanitized: Type.Literal(true),
    recording: Type.Object(
      {
        rawFrameEncoding: Type.Literal("synthetic-hex"),
        rawFrameHex: Type.String({ pattern: "^[0-9a-f]+$" }),
        gatewayId: Type.String({ pattern: "^fixture-gateway-[a-z]+$" }),
        meshNetworkId: Type.String({ pattern: "^fixture-network-[a-z]+$" }),
        transport: TransportKindSchema,
        transportMetadata: PacketTransportMetadataSchema,
        sessionConnectedAt: Type.String({ pattern: UTC_ISO_TIMESTAMP }),
        receivedAt: Type.String({ pattern: UTC_ISO_TIMESTAMP }),
        ingestedAt: Type.String({ pattern: UTC_ISO_TIMESTAMP }),
        serverIngestedAt: Type.String({ pattern: UTC_ISO_TIMESTAMP }),
      },
      { additionalProperties: false },
    ),
    normalizedFromRadio: NormalizedFromRadioSchema,
  },
  { additionalProperties: false },
);

export const SanitizedPacketFixtureSetSchema = Type.Object(
  {
    schemaVersion: Type.Literal(1),
    dataset: Type.Literal("cmclient-sanitized-packet-recordings"),
    sanitized: Type.Literal(true),
    fixtures: Type.Array(SanitizedPacketFixtureEntrySchema),
  },
  { additionalProperties: false },
);

export type PacketTransportMetadata = Static<
  typeof PacketTransportMetadataSchema
>;
export type SanitizedPacketFixtureEntry = Static<
  typeof SanitizedPacketFixtureEntrySchema
>;
export type SanitizedPacketFixtureSet = Static<
  typeof SanitizedPacketFixtureSetSchema
>;
