import { Type, type Static, type TSchema } from "@sinclair/typebox";

import { UpdateControlStatusSchema } from "./update.js";

const UTC_ISO_TIMESTAMP =
  "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d{3})?Z$";
const STABLE_ERROR_CODE = "^[A-Z][A-Z0-9_]{0,127}$";
const SETUP_REASON_CODE = "^SETUP_[A-Z0-9_]{1,121}$";
const TERMS_VERSION = "^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$";

export const SETUP_PHASES = [
  "uninitialized",
  "terms_required",
  "credentials_required",
  "validating",
  "ready",
  "recovery_required",
] as const;

export const AGENT_GATEWAY_LIFECYCLE_STATES = [
  "stopped",
  "starting",
  "running",
  "backoff",
  "degraded",
] as const;

export const AGENT_EVENT_STREAM_PATHS = {
  setup: "/api/v1/setup/events",
  lifecycle: "/api/v1/lifecycle/events",
  update: "/api/v1/updates/events",
} as const;

export const SetupPhaseSchema = Type.Union(
  SETUP_PHASES.map((phase) => Type.Literal(phase)),
  { $id: "SetupPhase" },
);

const SetupReasonCodeSchema = Type.String({ pattern: SETUP_REASON_CODE });

function setupStatusVariant(
  phase: (typeof SETUP_PHASES)[number],
  flags: {
    credentialsRequired: boolean;
    ready: boolean;
    recoveryRequired: boolean;
    termsRequired: boolean;
    validating: boolean;
  },
) {
  return Type.Object(
    {
      schemaVersion: Type.Literal(1),
      phase: Type.Literal(phase),
      setupRequired: Type.Literal(!flags.ready),
      termsRequired: Type.Literal(flags.termsRequired),
      credentialsRequired: Type.Literal(flags.credentialsRequired),
      validating: Type.Literal(flags.validating),
      ready: Type.Literal(flags.ready),
      recoveryRequired: Type.Literal(flags.recoveryRequired),
      reasonCode: SetupReasonCodeSchema,
    },
    { additionalProperties: false },
  );
}

export const SetupStatusSchema = Type.Union(
  [
    setupStatusVariant("uninitialized", {
      credentialsRequired: false,
      ready: false,
      recoveryRequired: false,
      termsRequired: true,
      validating: false,
    }),
    setupStatusVariant("terms_required", {
      credentialsRequired: false,
      ready: false,
      recoveryRequired: false,
      termsRequired: true,
      validating: false,
    }),
    setupStatusVariant("credentials_required", {
      credentialsRequired: true,
      ready: false,
      recoveryRequired: false,
      termsRequired: false,
      validating: false,
    }),
    setupStatusVariant("validating", {
      credentialsRequired: false,
      ready: false,
      recoveryRequired: false,
      termsRequired: false,
      validating: true,
    }),
    setupStatusVariant("ready", {
      credentialsRequired: false,
      ready: true,
      recoveryRequired: false,
      termsRequired: false,
      validating: false,
    }),
    setupStatusVariant("recovery_required", {
      credentialsRequired: false,
      ready: false,
      recoveryRequired: true,
      termsRequired: false,
      validating: false,
    }),
  ],
  { $id: "SetupStatus" },
);

export const SetupAcceptTermsRequestSchema = Type.Object(
  {
    termsVersion: Type.String({ pattern: TERMS_VERSION }),
  },
  { $id: "SetupAcceptTermsRequest", additionalProperties: false },
);

export const SetupResetRequestSchema = Type.Object(
  {
    confirmation: Type.Literal("operational_reset"),
  },
  { $id: "SetupResetRequest", additionalProperties: false },
);

export const AgentGatewayLifecycleStateSchema = Type.Union(
  AGENT_GATEWAY_LIFECYCLE_STATES.map((state) => Type.Literal(state)),
  { $id: "AgentGatewayLifecycleState" },
);

export const AgentLifecycleStatusSchema = Type.Object(
  {
    schemaVersion: Type.Literal(1),
    agent: Type.Literal("running"),
    gateway: AgentGatewayLifecycleStateSchema,
    managementWeb: Type.Union([
      Type.Literal("disabled"),
      Type.Literal("running"),
    ]),
    managementWebUrl: Type.Union([
      Type.String({
        maxLength: 512,
        pattern: "^https?://[^\\s]+$",
      }),
      Type.Null(),
    ]),
    uptimeSeconds: Type.Integer({ minimum: 0 }),
    latestErrorCode: Type.Union([
      Type.String({ pattern: STABLE_ERROR_CODE }),
      Type.Null(),
    ]),
  },
  { $id: "AgentLifecycleStatus", additionalProperties: false },
);

const AgentEventCommon = {
  schemaVersion: Type.Literal(1),
  occurredAt: Type.String({ pattern: UTC_ISO_TIMESTAMP }),
  source: Type.Literal("agent"),
  correlationId: Type.Optional(Type.String({ minLength: 1, maxLength: 128 })),
};

function agentEventId(stream: "setup" | "lifecycle" | "update") {
  return Type.String({
    maxLength: 128,
    pattern: `^agent:${stream}:[A-Za-z0-9._-]{1,96}$`,
  });
}

export const AgentSetupEventSchema = Type.Object(
  {
    ...AgentEventCommon,
    eventId: agentEventId("setup"),
    stream: Type.Literal("setup"),
    type: Type.Literal("setup.status"),
    payload: SetupStatusSchema,
  },
  { $id: "AgentSetupEvent", additionalProperties: false },
);

export const AgentLifecycleEventSchema = Type.Object(
  {
    ...AgentEventCommon,
    eventId: agentEventId("lifecycle"),
    stream: Type.Literal("lifecycle"),
    type: Type.Literal("lifecycle.status"),
    payload: AgentLifecycleStatusSchema,
  },
  { $id: "AgentLifecycleEvent", additionalProperties: false },
);

export const AgentUpdateEventSchema = Type.Object(
  {
    ...AgentEventCommon,
    eventId: agentEventId("update"),
    stream: Type.Literal("update"),
    type: Type.Literal("update.status"),
    payload: UpdateControlStatusSchema,
  },
  { $id: "AgentUpdateEvent", additionalProperties: false },
);

export const AgentEventSchema = Type.Union(
  [AgentSetupEventSchema, AgentLifecycleEventSchema, AgentUpdateEventSchema],
  { $id: "AgentEvent" },
);

export const AGENT_CONTRACT_SCHEMAS: readonly TSchema[] = [
  SetupPhaseSchema,
  SetupStatusSchema,
  SetupAcceptTermsRequestSchema,
  SetupResetRequestSchema,
  AgentGatewayLifecycleStateSchema,
  AgentLifecycleStatusSchema,
  UpdateControlStatusSchema,
  AgentSetupEventSchema,
  AgentLifecycleEventSchema,
  AgentUpdateEventSchema,
  AgentEventSchema,
];

export type SetupPhase = (typeof SETUP_PHASES)[number];
export type SetupStatus = Static<typeof SetupStatusSchema>;
export type SetupAcceptTermsRequest = Static<
  typeof SetupAcceptTermsRequestSchema
>;
export type SetupResetRequest = Static<typeof SetupResetRequestSchema>;
export type AgentGatewayLifecycleState =
  (typeof AGENT_GATEWAY_LIFECYCLE_STATES)[number];
export type AgentLifecycleStatus = Static<typeof AgentLifecycleStatusSchema>;
export type AgentSetupEvent = Static<typeof AgentSetupEventSchema>;
export type AgentLifecycleEvent = Static<typeof AgentLifecycleEventSchema>;
export type AgentUpdateEvent = Static<typeof AgentUpdateEventSchema>;
export type AgentEvent = Static<typeof AgentEventSchema>;
export type AgentEventStream = keyof typeof AGENT_EVENT_STREAM_PATHS;
