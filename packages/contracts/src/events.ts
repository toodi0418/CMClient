import { Type, type Static } from "@sinclair/typebox";

const UTC_ISO_TIMESTAMP =
  "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d{3})?Z$";

export const DomainEventSchema = Type.Object(
  {
    eventId: Type.String({ minLength: 1, maxLength: 128 }),
    schemaVersion: Type.Literal(1),
    type: Type.String({
      minLength: 1,
      maxLength: 128,
      pattern: "^[a-z][a-z0-9_.-]*$",
    }),
    occurredAt: Type.String({ pattern: UTC_ISO_TIMESTAMP }),
    source: Type.String({
      minLength: 1,
      maxLength: 64,
      pattern: "^[a-z][a-z0-9_.-]*$",
    }),
    correlationId: Type.Optional(Type.String({ minLength: 1, maxLength: 128 })),
    payload: Type.Record(Type.String(), Type.Unknown()),
  },
  { additionalProperties: false },
);

export const DomainEventListSchema = Type.Object(
  { items: Type.Array(DomainEventSchema, { maxItems: 200 }) },
  { additionalProperties: false },
);

export type DomainEvent = Static<typeof DomainEventSchema>;
export type DomainEventList = Static<typeof DomainEventListSchema>;
