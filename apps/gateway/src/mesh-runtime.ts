import { createHash, randomUUID } from "node:crypto";

import type {
  CallMeshMapping,
  MeshtasticRuntimeStatus,
  PositionCanonicalEvent,
  PositionDecision,
  PositionDecisionCode,
  PositionObservation,
} from "@cmclient/contracts";

import { AprsRemoteHighWaterStore } from "./aprs-monitor.js";
import { encodeAprsPosition } from "./aprs-position.js";
import { DomainEventBus } from "./events.js";
import {
  MeshDomainStore,
  type StoredApplicationPayload,
} from "./mesh-domain.js";
import { createMeshObservation } from "./observations.js";
import { GatewayDatabase } from "./persistence/database.js";
import {
  PositionDuplicateDetector,
  PositionHighWaterStore,
  PositionRepository,
  type PositionMappingTarget,
} from "./position.js";
import { validatePositionForAprs } from "./position-validation.js";
import { MeshtasticApplicationDecoder } from "./protobuf/application.js";
import { MeshtasticProtobufCodec } from "./protobuf/protobuf.js";
import type {
  MeshtasticTransport,
  TransportEvent,
  TransportFrameEvent,
} from "./transport/types.js";

export interface MeshGatewayRuntimeOptions {
  applicationDecoder: MeshtasticApplicationDecoder;
  codec: MeshtasticProtobufCodec;
  database: GatewayDatabase;
  eventBus: DomainEventBus;
  gatewayId: string;
  mappingProvider?: () => readonly CallMeshMapping[];
  meshNetworkId: string;
  transport: MeshtasticTransport;
  aprs?: {
    comment?: string;
    destination?: string;
    symbolCode?: string;
    symbolTable?: string;
  };
  clock?: () => Date;
  idFactory?: () => string;
}

export interface MeshIngestResult {
  observationId: string;
  payload: StoredApplicationPayload;
  position?: {
    decision?: PositionDecision;
    event: PositionCanonicalEvent;
    outboxCreated: boolean;
  };
}

export class MeshGatewayRuntimeError extends Error {
  constructor(readonly code: string) {
    super(code);
    this.name = "MeshGatewayRuntimeError";
  }
}

/** Owns the single production path from a framed radio packet to domain state. */
export class MeshGatewayRuntime {
  private readonly clock: () => Date;
  private readonly idFactory: () => string;
  private readonly mappingProvider: () => readonly CallMeshMapping[];
  private readonly domainStore: MeshDomainStore;
  private readonly duplicateDetector: PositionDuplicateDetector;
  private readonly highWater: PositionHighWaterStore;
  private readonly remoteHighWater: AprsRemoteHighWaterStore;
  private unsubscribe: (() => void) | undefined;
  private started = false;

  constructor(private readonly options: MeshGatewayRuntimeOptions) {
    validateRuntimeIdentity(options.meshNetworkId, options.gatewayId);
    this.clock = options.clock ?? (() => new Date());
    this.idFactory = options.idFactory ?? randomUUID;
    this.mappingProvider =
      options.mappingProvider ??
      (() => options.database.callmeshMappings.list());
    this.domainStore = new MeshDomainStore(
      options.database,
      options.applicationDecoder,
    );
    this.duplicateDetector = new PositionDuplicateDetector(
      new PositionRepository(options.database.connection),
    );
    this.highWater = new PositionHighWaterStore(options.database.connection);
    this.remoteHighWater = new AprsRemoteHighWaterStore(
      options.database.connection,
    );
  }

  start(): void {
    if (this.started) {
      return;
    }
    this.started = true;
    this.unsubscribe = this.options.transport.subscribe((event) =>
      this.onTransportEvent(event),
    );
    void this.options.transport.connect().catch((error: unknown) => {
      this.publish("mesh.transport.error", {
        code: stableErrorCode(error, "MESH_TRANSPORT_CONNECT_FAILED"),
        transport: this.options.transport.kind,
      });
    });
  }

  async stop(): Promise<void> {
    if (!this.started) {
      return;
    }
    this.started = false;
    this.unsubscribe?.();
    this.unsubscribe = undefined;
    await this.options.transport.disconnect();
  }

  status(): MeshtasticRuntimeStatus {
    return {
      configured: true,
      meshNetworkId: this.options.meshNetworkId,
      gatewayId: this.options.gatewayId,
      connection: this.options.transport.state,
      metrics: this.options.transport.metrics,
    };
  }

  ingestFrame(frame: TransportFrameEvent): MeshIngestResult {
    const normalizedFromRadio = this.options.codec.normalizeFromRadio(
      frame.frame,
    );
    const serverIngestedAt = this.clock().toISOString();
    const observation = createMeshObservation({
      id: this.nextId("mesh-observation"),
      transport: this.options.transport.kind,
      sessionConnectedAt: frame.sessionConnectedAt ?? frame.receivedAt,
      ingestedAt: frame.receivedAt,
      serverIngestedAt,
      normalizedFromRadio,
    });
    this.options.database.meshObservations.insert(observation);
    const payload = this.domainStore.persist(
      this.options.meshNetworkId,
      observation,
    );
    this.publish("mesh.observation.persisted", {
      observationId: observation.id,
      transport: observation.transport,
      kind: normalizedFromRadio.kind,
    });
    this.publishDomainPayload(payload);
    if (payload.kind !== "position") {
      return { observationId: observation.id, payload };
    }

    const position = this.processPosition(observation, payload.position);
    return { observationId: observation.id, payload, position };
  }

  private onTransportEvent(event: TransportEvent): void {
    if (event.kind === "state") {
      this.publish("mesh.transport.state", {
        ...event.state,
        metrics: this.options.transport.metrics,
      });
      return;
    }
    if (event.kind === "error") {
      this.publish("mesh.transport.error", {
        code: event.code,
        transport: this.options.transport.kind,
      });
      return;
    }
    try {
      this.ingestFrame(event);
    } catch (error) {
      this.publish("mesh.ingest.error", {
        code: stableErrorCode(error, "MESH_INGEST_FAILED"),
        transport: this.options.transport.kind,
      });
    }
  }

  private processPosition(
    observation: ReturnType<typeof createMeshObservation>,
    decoded: Extract<
      StoredApplicationPayload,
      { kind: "position" }
    >["position"],
  ): NonNullable<MeshIngestResult["position"]> {
    const packet = observation.normalizedFromRadio.packet;
    const positionObservation: PositionObservation = {
      schemaVersion: 1,
      id: `position-${observation.id}`,
      meshNetworkId: this.options.meshNetworkId,
      nodeNum: decoded.nodeNum,
      meshObservationId: observation.id,
      gatewayId: this.options.gatewayId,
      transport: observation.transport,
      sessionConnectedAt: observation.sessionConnectedAt,
      ingestedAt: observation.ingestedAt,
      serverIngestedAt: observation.serverIngestedAt,
      ...(observation.deviceRxTimeSeconds === undefined
        ? {}
        : { deviceRxTimeSeconds: observation.deviceRxTimeSeconds }),
      backlogClassification: observation.backlogClassification,
      ...(packet?.packetId === undefined ? {} : { packetId: packet.packetId }),
      payloadHash: decoded.payloadHash,
      ...(packet?.viaMqtt === undefined ? {} : { viaMqtt: packet.viaMqtt }),
      ...(packet?.rxSnr === undefined ? {} : { rxSnr: packet.rxSnr }),
      ...(packet?.rxRssi === undefined ? {} : { rxRssi: packet.rxRssi }),
      ...(packet?.hopLimit === undefined ? {} : { hopLimit: packet.hopLimit }),
      ...(packet?.hopStart === undefined ? {} : { hopStart: packet.hopStart }),
      position: decoded.sample,
    };
    const duplicate = this.duplicateDetector.observe(positionObservation);
    this.publish("position.observed", {
      observationId: positionObservation.id,
      canonicalEventId: duplicate.event.id,
      nodeNum: positionObservation.nodeNum,
      meshNetworkId: positionObservation.meshNetworkId,
    });
    if (duplicate.kind === "duplicate") {
      this.publishDecision(duplicate.decision);
    }

    const mapping = selectActiveMapping(
      this.mappingProvider(),
      duplicate.event.meshNetworkId,
      duplicate.event.nodeNum,
      observation.serverIngestedAt,
    );
    if (mapping.kind === "none") {
      this.publish("position.unmapped", {
        canonicalEventId: duplicate.event.id,
        meshNetworkId: duplicate.event.meshNetworkId,
        nodeNum: duplicate.event.nodeNum,
      });
      return { event: duplicate.event, outboxCreated: false };
    }
    if (mapping.kind === "conflict") {
      this.publish("callmesh.mapping.conflict", {
        canonicalEventId: duplicate.event.id,
        meshNetworkId: duplicate.event.meshNetworkId,
        nodeNum: duplicate.event.nodeNum,
      });
      return { event: duplicate.event, outboxCreated: false };
    }

    const target: PositionMappingTarget = {
      callsign: mapping.mapping.callsign,
      mappingVersion: mapping.mapping.version,
    };
    const current = this.highWater.getState(
      duplicate.event.meshNetworkId,
      duplicate.event.nodeNum,
      target,
    );
    const validation = validatePositionForAprs(duplicate.event, {
      now: this.clock(),
      ...(current?.latestEventTime
        ? { previousTrustedEventTime: current.latestEventTime }
        : {}),
    });
    if (!validation.accepted) {
      const decision = this.recordDecision(
        positionObservation,
        duplicate.event,
        validation.code,
        target,
      );
      return { event: duplicate.event, decision, outboxCreated: false };
    }

    const monitorTarget = {
      ...target,
      meshNetworkId: duplicate.event.meshNetworkId,
      nodeNum: duplicate.event.nodeNum,
    };
    if (!this.remoteHighWater.canUpload(validation.event, monitorTarget)) {
      const decision = this.recordDecision(
        positionObservation,
        duplicate.event,
        "APRS_SKIPPED_OUT_OF_ORDER",
        target,
      );
      return { event: duplicate.event, decision, outboxCreated: false };
    }

    const encoded = encodeAprsPosition(validation.event, {
      source: target.callsign,
      destination: this.options.aprs?.destination ?? "APCM20",
      symbolTable: this.options.aprs?.symbolTable ?? "/",
      symbolCode: this.options.aprs?.symbolCode ?? ">",
      ...(this.options.aprs?.comment
        ? { comment: this.options.aprs.comment }
        : {}),
    });
    let enqueued:
      ReturnType<GatewayDatabase["aprsOutbox"]["enqueue"]> | undefined;
    const ordered = this.highWater.apply(
      validation.event,
      target,
      observation.serverIngestedAt,
      (acceptedEvent) => {
        enqueued = this.options.database.aprsOutbox.enqueue({
          callsign: target.callsign,
          canonicalEventId: acceptedEvent.id,
          data: encoded.data,
          now: observation.serverIngestedAt,
        });
      },
    );
    this.publishDecision(ordered.decision);
    if (ordered.decision.code !== "POSITION_ACCEPTED") {
      return {
        event: ordered.event,
        decision: ordered.decision,
        outboxCreated: false,
      };
    }

    if (!enqueued) {
      throw new MeshGatewayRuntimeError("APRS_OUTBOX_TRANSACTION_FAILED");
    }
    this.publish("aprs.outbox.queued", {
      outboxId: enqueued.entry.id,
      canonicalEventId: ordered.event.id,
      callsign: target.callsign,
      created: enqueued.created,
    });
    return {
      event: ordered.event,
      decision: ordered.decision,
      outboxCreated: enqueued.created,
    };
  }

  private recordDecision(
    observation: PositionObservation,
    event: PositionCanonicalEvent,
    code: PositionDecisionCode,
    target: PositionMappingTarget,
  ): PositionDecision {
    const digest = createHash("sha256")
      .update(
        [
          observation.id,
          event.id,
          target.callsign,
          target.mappingVersion,
          code,
        ].join("\u0000"),
      )
      .digest("hex");
    const decision = this.options.database.positions.insertOrFindDecision({
      schemaVersion: 1,
      id: `position-runtime-${digest}`,
      observationId: observation.id,
      canonicalEventId: event.id,
      code,
      decidedAt: observation.serverIngestedAt,
      parameters: {
        callsign: target.callsign,
        mappingVersion: target.mappingVersion,
      },
    });
    this.publishDecision(decision);
    return decision;
  }

  private publishDomainPayload(payload: StoredApplicationPayload): void {
    switch (payload.kind) {
      case "node":
        this.publish("node.updated", {
          meshNetworkId: payload.node.meshNetworkId,
          nodeNum: payload.node.nodeNum,
        });
        break;
      case "message":
        this.publish("message.received", {
          messageId: payload.message.id,
          meshNetworkId: payload.message.meshNetworkId,
          sender: payload.message.sender,
        });
        break;
      case "telemetry":
        this.publish("telemetry.received", {
          telemetryId: payload.telemetry.id,
          meshNetworkId: payload.telemetry.meshNetworkId,
          nodeNum: payload.telemetry.nodeNum,
        });
        break;
      case "ignored":
        this.publish("mesh.application.ignored", {
          code: payload.reasonCode,
        });
        break;
      case "position":
        this.publish("node.updated", {
          meshNetworkId: this.options.meshNetworkId,
          nodeNum: payload.position.nodeNum,
        });
        break;
    }
  }

  private publishDecision(decision: PositionDecision): void {
    this.publish("position.decision", {
      decisionId: decision.id,
      observationId: decision.observationId,
      ...(decision.canonicalEventId
        ? { canonicalEventId: decision.canonicalEventId }
        : {}),
      code: decision.code,
    });
  }

  private publish(type: string, payload: Record<string, unknown>): void {
    this.options.eventBus.publish({ type, source: "gateway", payload });
  }

  private nextId(prefix: string): string {
    const value = this.idFactory();
    if (!/^[a-zA-Z0-9-]{1,96}$/.test(value)) {
      throw new MeshGatewayRuntimeError("MESH_OBSERVATION_ID_INVALID");
    }
    return `${prefix}-${value}`;
  }
}

type ActiveMappingResult =
  | { kind: "none" }
  | { kind: "conflict" }
  | { kind: "mapping"; mapping: CallMeshMapping };

export function selectActiveMapping(
  mappings: readonly CallMeshMapping[],
  meshNetworkId: string,
  nodeNum: number,
  effectiveAt: string,
): ActiveMappingResult {
  const candidates = mappings
    .filter(
      (mapping) =>
        mapping.meshNetworkId === meshNetworkId &&
        mapping.nodeNum === nodeNum &&
        mapping.effectiveAt <= effectiveAt,
    )
    .sort(
      (left, right) =>
        right.effectiveAt.localeCompare(left.effectiveAt) ||
        right.version.localeCompare(left.version),
    );
  const latest = candidates[0];
  if (!latest) {
    return { kind: "none" };
  }
  const simultaneous = candidates.filter(
    (candidate) => candidate.effectiveAt === latest.effectiveAt,
  );
  if (
    new Set(simultaneous.map((candidate) => candidate.callsign)).size > 1 ||
    new Set(simultaneous.map((candidate) => candidate.version)).size > 1
  ) {
    return { kind: "conflict" };
  }
  return { kind: "mapping", mapping: latest };
}

function validateRuntimeIdentity(
  meshNetworkId: string,
  gatewayId: string,
): void {
  if (
    !meshNetworkId.trim() ||
    meshNetworkId.length > 128 ||
    !gatewayId.trim() ||
    gatewayId.length > 128 ||
    /[\r\n]/.test(meshNetworkId) ||
    /[\r\n]/.test(gatewayId)
  ) {
    throw new MeshGatewayRuntimeError("MESH_RUNTIME_CONFIGURATION_INVALID");
  }
}

function stableErrorCode(error: unknown, fallback: string): string {
  if (
    error instanceof Error &&
    "code" in error &&
    typeof error.code === "string" &&
    /^[A-Z][A-Z0-9_]{0,127}$/.test(error.code)
  ) {
    return error.code;
  }
  return fallback;
}
