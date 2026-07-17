import { createHash } from "node:crypto";
import { DatabaseSync } from "node:sqlite";

import type {
  NodePositionState,
  PositionCanonicalEvent,
  PositionDecision,
  PositionDecisionCode,
  PositionEventTimeSource,
  PositionObservation,
  PositionSample,
} from "@cmclient/contracts";

export class PositionPersistenceError extends Error {
  readonly code = "POSITION_PERSISTENCE_FAILED";

  constructor() {
    super("POSITION_PERSISTENCE_FAILED");
  }
}

export interface CanonicalPositionResult {
  event: PositionCanonicalEvent;
  identity: string;
}

export type PositionDuplicateResult =
  | { kind: "new"; event: PositionCanonicalEvent }
  | {
      kind: "duplicate";
      event: PositionCanonicalEvent;
      decision: PositionDecision;
    };

export interface PositionMappingTarget {
  callsign: string;
  mappingVersion: string;
}

export interface PositionOrderPlan {
  advance: boolean;
  code: PositionDecisionCode;
  sequenceEpoch?: number;
}

export interface PositionHighWaterResult {
  decision: PositionDecision;
  event: PositionCanonicalEvent;
  state?: NodePositionState;
}

export class PositionRepository {
  constructor(private readonly database: DatabaseSync) {}

  insertOrFindObservation(
    observation: PositionObservation,
  ): PositionObservation {
    try {
      this.database
        .prepare(
          "INSERT OR IGNORE INTO position_observations (id, mesh_network_id, node_num, mesh_observation_id, gateway_id, transport, session_connected_at, ingested_at, server_ingested_at, device_rx_time_seconds, backlog_classification, packet_id, payload_hash, via_mqtt, rx_snr, rx_rssi, hop_limit, hop_start, position) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .run(
          observation.id,
          observation.meshNetworkId,
          observation.nodeNum,
          observation.meshObservationId,
          observation.gatewayId,
          observation.transport,
          observation.sessionConnectedAt,
          observation.ingestedAt,
          observation.serverIngestedAt,
          observation.deviceRxTimeSeconds ?? null,
          observation.backlogClassification,
          observation.packetId ?? null,
          observation.payloadHash,
          observation.viaMqtt === undefined
            ? null
            : observation.viaMqtt
              ? 1
              : 0,
          observation.rxSnr ?? null,
          observation.rxRssi ?? null,
          observation.hopLimit ?? null,
          observation.hopStart ?? null,
          JSON.stringify(observation.position),
        );
    } catch {
      throw new PositionPersistenceError();
    }
    const stored = this.findObservation(observation.id);
    if (!stored) {
      throw new PositionPersistenceError();
    }
    return stored;
  }

  findObservation(id: string): PositionObservation | undefined {
    const row = this.database
      .prepare("SELECT * FROM position_observations WHERE id = ?")
      .get(id);
    return row ? toPositionObservation(row) : undefined;
  }

  insertOrFindEvent(event: PositionCanonicalEvent): {
    created: boolean;
    event: PositionCanonicalEvent;
  } {
    const existing = this.findEventByCanonicalKey(event.canonicalKey);
    if (existing) {
      return { created: false, event: existing };
    }
    try {
      this.database
        .prepare(
          "INSERT OR IGNORE INTO position_events (id, canonical_key, mesh_network_id, node_num, source_observation_id, payload_hash, event_time, event_time_source, sequence_epoch, sequence_number, position, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .run(
          event.id,
          event.canonicalKey,
          event.meshNetworkId,
          event.nodeNum,
          event.sourceObservationId,
          event.payloadHash,
          event.eventTime ?? null,
          event.eventTimeSource ?? null,
          event.sequenceEpoch ?? null,
          event.sequenceNumber ?? null,
          JSON.stringify(event.position),
          event.createdAt,
        );
    } catch {
      throw new PositionPersistenceError();
    }
    const stored = this.findEventByCanonicalKey(event.canonicalKey);
    if (!stored) {
      throw new PositionPersistenceError();
    }
    return { created: stored.id === event.id, event: stored };
  }

  findEventByCanonicalKey(
    canonicalKey: string,
  ): PositionCanonicalEvent | undefined {
    const row = this.database
      .prepare("SELECT * FROM position_events WHERE canonical_key = ?")
      .get(canonicalKey);
    return row ? toPositionCanonicalEvent(row) : undefined;
  }

  listCanonicalEvents(limit: number): PositionCanonicalEvent[] {
    return this.database
      .prepare(
        "SELECT * FROM position_events ORDER BY COALESCE(event_time, created_at) DESC, id ASC LIMIT ?",
      )
      .all(limit)
      .map((row) => toPositionCanonicalEvent(row as Record<string, unknown>));
  }

  insertOrFindDecision(decision: PositionDecision): PositionDecision {
    try {
      this.database
        .prepare(
          "INSERT OR IGNORE INTO position_decisions (id, observation_id, canonical_event_id, code, decided_at, parameters) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .run(
          decision.id,
          decision.observationId,
          decision.canonicalEventId ?? null,
          decision.code,
          decision.decidedAt,
          JSON.stringify(decision.parameters),
        );
    } catch {
      throw new PositionPersistenceError();
    }
    const stored = this.findDecision(decision.id);
    if (!stored) {
      throw new PositionPersistenceError();
    }
    return stored;
  }

  findDecision(id: string): PositionDecision | undefined {
    const row = this.database
      .prepare("SELECT * FROM position_decisions WHERE id = ?")
      .get(id);
    return row ? toPositionDecision(row) : undefined;
  }
}

export class PositionDuplicateDetector {
  constructor(private readonly repository: PositionRepository) {}

  observe(observation: PositionObservation): PositionDuplicateResult {
    const storedObservation =
      this.repository.insertOrFindObservation(observation);
    const candidate = createCanonicalPositionEvent(storedObservation).event;
    const result = this.repository.insertOrFindEvent(candidate);
    if (result.created) {
      return { kind: "new", event: result.event };
    }
    const decision = this.repository.insertOrFindDecision({
      schemaVersion: 1,
      id: `position-duplicate-${shortHash(storedObservation.id)}`,
      observationId: storedObservation.id,
      canonicalEventId: result.event.id,
      code: "POSITION_DUPLICATE",
      decidedAt: storedObservation.serverIngestedAt,
      parameters: { canonicalKey: result.event.canonicalKey },
    });
    return { kind: "duplicate", event: result.event, decision };
  }
}

export class PositionHighWaterStore {
  constructor(private readonly database: DatabaseSync) {}

  apply(
    event: PositionCanonicalEvent,
    target: PositionMappingTarget,
    decidedAt: string,
  ): PositionHighWaterResult {
    if (!target.callsign.trim() || !target.mappingVersion.trim()) {
      throw new PositionPersistenceError();
    }
    let transactionOpen = false;
    try {
      this.database.exec("BEGIN IMMEDIATE");
      transactionOpen = true;
      const current = this.findState(
        event.meshNetworkId,
        event.nodeNum,
        target,
      );
      const plan: PositionOrderPlan = this.isBacklogObservation(
        event.sourceObservationId,
      )
        ? { advance: false, code: "POSITION_BACKLOG" as const }
        : decidePositionOrder(current, event);
      const eventWithEpoch =
        plan.sequenceEpoch === undefined
          ? event
          : this.assignSequenceEpoch(event, plan.sequenceEpoch);
      const state = plan.advance
        ? this.writeState(eventWithEpoch, target, decidedAt)
        : current;
      const decision = this.writeDecision(
        eventWithEpoch,
        target,
        plan.code,
        decidedAt,
      );
      this.database.exec("COMMIT");
      transactionOpen = false;
      return { event: eventWithEpoch, decision, ...(state ? { state } : {}) };
    } catch {
      if (transactionOpen) {
        this.database.exec("ROLLBACK");
      }
      throw new PositionPersistenceError();
    }
  }

  private findState(
    meshNetworkId: string,
    nodeNum: number,
    target: PositionMappingTarget,
  ): NodePositionState | undefined {
    const row = this.database
      .prepare(
        "SELECT * FROM node_position_state WHERE mesh_network_id = ? AND node_num = ? AND callsign = ? AND mapping_version = ?",
      )
      .get(meshNetworkId, nodeNum, target.callsign, target.mappingVersion);
    return row ? toNodePositionState(row) : undefined;
  }

  private isBacklogObservation(observationId: string): boolean {
    const row = this.database
      .prepare(
        "SELECT backlog_classification FROM position_observations WHERE id = ?",
      )
      .get(observationId);
    if (!row) {
      throw new PositionPersistenceError();
    }
    const classification = String(row.backlog_classification);
    if (!["backlog", "live", "unknown"].includes(classification)) {
      throw new PositionPersistenceError();
    }
    return classification === "backlog";
  }

  private assignSequenceEpoch(
    event: PositionCanonicalEvent,
    sequenceEpoch: number,
  ): PositionCanonicalEvent {
    this.database
      .prepare("UPDATE position_events SET sequence_epoch = ? WHERE id = ?")
      .run(sequenceEpoch, event.id);
    return { ...event, sequenceEpoch };
  }

  private writeState(
    event: PositionCanonicalEvent,
    target: PositionMappingTarget,
    updatedAt: string,
  ): NodePositionState {
    this.database
      .prepare(
        "INSERT INTO node_position_state (mesh_network_id, node_num, callsign, mapping_version, latest_canonical_event_id, latest_event_time, latest_sequence_epoch, latest_sequence_number, latest_latitude_i, latest_longitude_i, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(mesh_network_id, node_num, callsign, mapping_version) DO UPDATE SET latest_canonical_event_id = excluded.latest_canonical_event_id, latest_event_time = excluded.latest_event_time, latest_sequence_epoch = excluded.latest_sequence_epoch, latest_sequence_number = excluded.latest_sequence_number, latest_latitude_i = excluded.latest_latitude_i, latest_longitude_i = excluded.latest_longitude_i, updated_at = excluded.updated_at",
      )
      .run(
        event.meshNetworkId,
        event.nodeNum,
        target.callsign,
        target.mappingVersion,
        event.id,
        event.eventTime ?? null,
        event.sequenceEpoch ?? null,
        event.sequenceNumber ?? null,
        event.position.latitudeI ?? null,
        event.position.longitudeI ?? null,
        updatedAt,
      );
    const state = this.findState(event.meshNetworkId, event.nodeNum, target);
    if (!state) {
      throw new PositionPersistenceError();
    }
    return state;
  }

  private writeDecision(
    event: PositionCanonicalEvent,
    target: PositionMappingTarget,
    code: PositionDecisionCode,
    decidedAt: string,
  ): PositionDecision {
    const id = `position-order-${shortHash(
      `${event.id}|${target.callsign}|${target.mappingVersion}|${code}`,
    )}`;
    this.database
      .prepare(
        "INSERT OR IGNORE INTO position_decisions (id, observation_id, canonical_event_id, code, decided_at, parameters) VALUES (?, ?, ?, ?, ?, ?)",
      )
      .run(
        id,
        event.sourceObservationId,
        event.id,
        code,
        decidedAt,
        JSON.stringify({
          callsign: target.callsign,
          mappingVersion: target.mappingVersion,
        }),
      );
    const row = this.database
      .prepare("SELECT * FROM position_decisions WHERE id = ?")
      .get(id);
    if (!row) {
      throw new PositionPersistenceError();
    }
    return toPositionDecision(row);
  }
}

export function decidePositionOrder(
  current: NodePositionState | undefined,
  event: PositionCanonicalEvent,
): PositionOrderPlan {
  if (!event.eventTime) {
    return { advance: false, code: "APRS_SKIPPED_OUT_OF_ORDER" };
  }
  if (!current) {
    return {
      advance: true,
      code: "POSITION_ACCEPTED",
      ...(event.sequenceNumber === undefined ? {} : { sequenceEpoch: 0 }),
    };
  }
  if (!current.latestEventTime) {
    return {
      advance: true,
      code: "POSITION_ACCEPTED",
      ...(event.sequenceNumber === undefined
        ? {}
        : { sequenceEpoch: current.latestSequenceEpoch ?? 0 }),
    };
  }
  const timeOrder = event.eventTime.localeCompare(current.latestEventTime);
  if (timeOrder < 0) {
    return { advance: false, code: "POSITION_HISTORICAL" };
  }
  if (timeOrder > 0) {
    return {
      advance: true,
      code: "POSITION_ACCEPTED",
      ...(event.sequenceNumber === undefined
        ? {}
        : {
            sequenceEpoch:
              current.latestSequenceNumber !== undefined &&
              event.sequenceNumber < current.latestSequenceNumber
                ? (current.latestSequenceEpoch ?? 0) + 1
                : (current.latestSequenceEpoch ?? 0),
          }),
    };
  }
  if (
    event.sequenceNumber === undefined ||
    current.latestSequenceNumber === undefined
  ) {
    return { advance: false, code: "POSITION_SEQUENCE_CONFLICT" };
  }
  if (event.sequenceNumber > current.latestSequenceNumber) {
    return {
      advance: true,
      code: "POSITION_ACCEPTED",
      sequenceEpoch: current.latestSequenceEpoch ?? 0,
    };
  }
  if (event.sequenceNumber < current.latestSequenceNumber) {
    return { advance: false, code: "POSITION_HISTORICAL" };
  }
  return { advance: false, code: "POSITION_SEQUENCE_CONFLICT" };
}

export function createCanonicalPositionEvent(
  observation: PositionObservation,
): CanonicalPositionResult {
  const eventTime = selectEventTime(observation.position);
  const identity = JSON.stringify([
    "cmclient-position-event-v1",
    observation.meshNetworkId,
    observation.nodeNum,
    eventTime.source ?? null,
    eventTime.value ?? null,
    observation.position.sequenceNumber ?? null,
    observation.position.latitudeI ?? null,
    observation.position.longitudeI ?? null,
    observation.position.altitudeMslMeters ?? null,
    observation.position.altitudeHaeMeters ?? null,
    observation.position.altitudeGeoidalSeparationMeters ?? null,
    observation.position.precisionBits ?? null,
    observation.position.groundSpeedMetersPerSecond ?? null,
    observation.position.groundTrackDegrees ?? null,
    observation.payloadHash,
  ]);
  const canonicalKey = sha256(identity);
  return {
    identity,
    event: {
      schemaVersion: 1,
      id: `position-event-${canonicalKey}`,
      canonicalKey,
      meshNetworkId: observation.meshNetworkId,
      nodeNum: observation.nodeNum,
      sourceObservationId: observation.id,
      payloadHash: observation.payloadHash,
      ...(eventTime.value ? { eventTime: eventTime.value } : {}),
      ...(eventTime.source ? { eventTimeSource: eventTime.source } : {}),
      ...(observation.position.sequenceNumber === undefined
        ? {}
        : { sequenceNumber: observation.position.sequenceNumber }),
      position: structuredClone(observation.position),
      createdAt: observation.serverIngestedAt,
    },
  };
}

function selectEventTime(position: PositionSample): {
  source?: PositionEventTimeSource;
  value?: string;
} {
  if (
    position.positionTimestampSeconds &&
    position.positionTimestampSeconds > 0
  ) {
    const timestamp = Date.parse(
      new Date(
        position.positionTimestampSeconds * 1_000 +
          (position.positionTimestampMillisAdjust ?? 0),
      ).toISOString(),
    );
    if (Number.isFinite(timestamp)) {
      return {
        source: "position_timestamp",
        value: new Date(timestamp).toISOString(),
      };
    }
  }
  if (position.positionTimeSeconds && position.positionTimeSeconds > 0) {
    const timestamp = Date.parse(
      new Date(position.positionTimeSeconds * 1_000).toISOString(),
    );
    if (Number.isFinite(timestamp)) {
      return {
        source: "position_time",
        value: new Date(timestamp).toISOString(),
      };
    }
  }
  return position.sequenceNumber === undefined ? {} : { source: "sequence" };
}

function toPositionObservation(
  row: Record<string, unknown>,
): PositionObservation {
  const deviceRxTimeSeconds = optionalInteger(row.device_rx_time_seconds);
  const packetId = optionalInteger(row.packet_id);
  const viaMqtt = optionalBoolean(row.via_mqtt);
  if (
    deviceRxTimeSeconds === "invalid" ||
    packetId === "invalid" ||
    viaMqtt === "invalid"
  ) {
    throw new PositionPersistenceError();
  }
  return {
    schemaVersion: 1,
    id: String(row.id),
    meshNetworkId: String(row.mesh_network_id),
    nodeNum: requiredInteger(row.node_num),
    meshObservationId: String(row.mesh_observation_id),
    gatewayId: String(row.gateway_id),
    transport: String(row.transport) as PositionObservation["transport"],
    sessionConnectedAt: String(row.session_connected_at),
    ingestedAt: String(row.ingested_at),
    serverIngestedAt: String(row.server_ingested_at),
    ...(typeof deviceRxTimeSeconds === "number" ? { deviceRxTimeSeconds } : {}),
    backlogClassification: String(
      row.backlog_classification,
    ) as PositionObservation["backlogClassification"],
    ...(typeof packetId === "number" ? { packetId } : {}),
    payloadHash: String(row.payload_hash),
    ...(typeof viaMqtt === "boolean" ? { viaMqtt } : {}),
    ...optionalNumberProperty("rxSnr", row.rx_snr),
    ...optionalSignedIntegerProperty("rxRssi", row.rx_rssi),
    ...optionalIntegerProperty("hopLimit", row.hop_limit),
    ...optionalIntegerProperty("hopStart", row.hop_start),
    position: parsePositionSample(row.position),
  };
}

function toPositionCanonicalEvent(
  row: Record<string, unknown>,
): PositionCanonicalEvent {
  const sequenceEpoch = optionalInteger(row.sequence_epoch);
  const sequenceNumber = optionalInteger(row.sequence_number);
  if (sequenceEpoch === "invalid" || sequenceNumber === "invalid") {
    throw new PositionPersistenceError();
  }
  const eventTime = optionalString(row.event_time);
  const eventTimeSource = optionalString(row.event_time_source);
  return {
    schemaVersion: 1,
    id: String(row.id),
    canonicalKey: String(row.canonical_key),
    meshNetworkId: String(row.mesh_network_id),
    nodeNum: requiredInteger(row.node_num),
    sourceObservationId: String(row.source_observation_id),
    payloadHash: String(row.payload_hash),
    ...(eventTime ? { eventTime } : {}),
    ...(eventTimeSource
      ? { eventTimeSource: eventTimeSource as PositionEventTimeSource }
      : {}),
    ...(typeof sequenceEpoch === "number" ? { sequenceEpoch } : {}),
    ...(typeof sequenceNumber === "number" ? { sequenceNumber } : {}),
    position: parsePositionSample(row.position),
    createdAt: String(row.created_at),
  };
}

function toPositionDecision(row: Record<string, unknown>): PositionDecision {
  const canonicalEventId = optionalString(row.canonical_event_id);
  return {
    schemaVersion: 1,
    id: String(row.id),
    observationId: String(row.observation_id),
    ...(canonicalEventId ? { canonicalEventId } : {}),
    code: String(row.code) as PositionDecision["code"],
    decidedAt: String(row.decided_at),
    parameters: parseDecisionParameters(row.parameters),
  };
}

function toNodePositionState(row: Record<string, unknown>): NodePositionState {
  const latestSequenceEpoch = optionalInteger(row.latest_sequence_epoch);
  const latestSequenceNumber = optionalInteger(row.latest_sequence_number);
  const latestLatitudeI = optionalSignedInteger(row.latest_latitude_i);
  const latestLongitudeI = optionalSignedInteger(row.latest_longitude_i);
  if (
    latestSequenceEpoch === "invalid" ||
    latestSequenceNumber === "invalid" ||
    latestLatitudeI === "invalid" ||
    latestLongitudeI === "invalid"
  ) {
    throw new PositionPersistenceError();
  }
  const latestCanonicalEventId = optionalString(row.latest_canonical_event_id);
  const latestEventTime = optionalString(row.latest_event_time);
  return {
    schemaVersion: 1,
    meshNetworkId: String(row.mesh_network_id),
    nodeNum: requiredInteger(row.node_num),
    callsign: String(row.callsign),
    mappingVersion: String(row.mapping_version),
    ...(latestCanonicalEventId ? { latestCanonicalEventId } : {}),
    ...(latestEventTime ? { latestEventTime } : {}),
    ...(typeof latestSequenceEpoch === "number" ? { latestSequenceEpoch } : {}),
    ...(typeof latestSequenceNumber === "number"
      ? { latestSequenceNumber }
      : {}),
    ...(typeof latestLatitudeI === "number" ? { latestLatitudeI } : {}),
    ...(typeof latestLongitudeI === "number" ? { latestLongitudeI } : {}),
    updatedAt: String(row.updated_at),
  };
}

function parsePositionSample(value: unknown): PositionSample {
  try {
    const source = JSON.parse(String(value)) as unknown;
    if (!source || Array.isArray(source) || typeof source !== "object") {
      throw new PositionPersistenceError();
    }
    return source as PositionSample;
  } catch (error) {
    if (error instanceof PositionPersistenceError) {
      throw error;
    }
    throw new PositionPersistenceError();
  }
}

function parseDecisionParameters(
  value: unknown,
): PositionDecision["parameters"] {
  try {
    const source = JSON.parse(String(value)) as unknown;
    if (!source || Array.isArray(source) || typeof source !== "object") {
      throw new PositionPersistenceError();
    }
    const parameters: PositionDecision["parameters"] = {};
    for (const [key, parameter] of Object.entries(source)) {
      if (
        (typeof parameter === "string" && parameter.length <= 256) ||
        typeof parameter === "number" ||
        typeof parameter === "boolean"
      ) {
        parameters[key] = parameter;
        continue;
      }
      throw new PositionPersistenceError();
    }
    return parameters;
  } catch (error) {
    if (error instanceof PositionPersistenceError) {
      throw error;
    }
    throw new PositionPersistenceError();
  }
}

function requiredInteger(value: unknown): number {
  const parsed = optionalInteger(value);
  if (typeof parsed !== "number") {
    throw new PositionPersistenceError();
  }
  return parsed;
}

function optionalString(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

function optionalInteger(value: unknown): number | undefined | "invalid" {
  if (value === null) {
    return undefined;
  }
  return typeof value === "number" &&
    Number.isInteger(value) &&
    value >= 0 &&
    value <= 4_294_967_295
    ? value
    : "invalid";
}

function optionalSignedInteger(value: unknown): number | undefined | "invalid" {
  if (value === null) {
    return undefined;
  }
  return typeof value === "number" && Number.isInteger(value)
    ? value
    : "invalid";
}

function optionalBoolean(value: unknown): boolean | undefined | "invalid" {
  if (value === null) {
    return undefined;
  }
  if (value === 0) {
    return false;
  }
  if (value === 1) {
    return true;
  }
  return "invalid";
}

function optionalNumberProperty<Key extends string>(
  key: Key,
  value: unknown,
): Record<Key, number> | Record<never, never> {
  return typeof value === "number" && Number.isFinite(value)
    ? ({ [key]: value } as Record<Key, number>)
    : {};
}

function optionalIntegerProperty<Key extends string>(
  key: Key,
  value: unknown,
): Record<Key, number> | Record<never, never> {
  const parsed = optionalInteger(value);
  if (parsed === "invalid") {
    throw new PositionPersistenceError();
  }
  return typeof parsed === "number"
    ? ({ [key]: parsed } as Record<Key, number>)
    : {};
}

function optionalSignedIntegerProperty<Key extends string>(
  key: Key,
  value: unknown,
): Record<Key, number> | Record<never, never> {
  return typeof value === "number" && Number.isInteger(value)
    ? ({ [key]: value } as Record<Key, number>)
    : {};
}

function shortHash(value: string): string {
  return sha256(value).slice(0, 32);
}

function sha256(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}
