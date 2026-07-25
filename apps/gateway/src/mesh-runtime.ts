import { createHash, randomUUID } from "node:crypto";
import { performance } from "node:perf_hooks";

import type {
  CallMeshMapping,
  MeshtasticRuntimeStatus,
  PositionCanonicalEvent,
  PositionDecision,
  PositionDecisionCode,
  PositionObservation,
} from "@cmclient/contracts";

import { AprsRemoteHighWaterStore } from "./aprs-monitor.js";
import type { AprsRuntimeState } from "./aprs-identity.js";
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
import { PacketRecorder } from "./recorder.js";
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
  meshNetworkId: string;
  transport: MeshtasticTransport;
  aprs?: {
    stateProvider?: () => AprsRuntimeState | undefined;
    onDecodedSummary?: (type: string, timestampMs: number) => void;
  };
  clock?: () => Date;
  idFactory?: () => string;
  stopTimeoutMs?: number;
  packetRecorder?: PacketRecorder;
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
  private readonly stopTimeoutMs: number;
  private readonly domainStore: MeshDomainStore;
  private readonly duplicateDetector: PositionDuplicateDetector;
  private readonly highWater: PositionHighWaterStore;
  private readonly remoteHighWater: AprsRemoteHighWaterStore;
  private unsubscribe: (() => void) | undefined;
  private connectPromise: Promise<void> | undefined;
  private stopPromise: Promise<void> | undefined;
  private teardownRequired = false;
  private lifecycleGeneration = 0;
  private started = false;

  constructor(private readonly options: MeshGatewayRuntimeOptions) {
    validateRuntimeIdentity(options.meshNetworkId, options.gatewayId);
    this.clock = options.clock ?? (() => new Date());
    this.idFactory = options.idFactory ?? randomUUID;
    this.stopTimeoutMs = options.stopTimeoutMs ?? 10_000;
    if (
      !Number.isInteger(this.stopTimeoutMs) ||
      this.stopTimeoutMs < 10 ||
      this.stopTimeoutMs > 120_000
    ) {
      throw new MeshGatewayRuntimeError("MESH_STOP_TIMEOUT_INVALID");
    }
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
    if (this.stopPromise || this.teardownRequired) {
      throw new MeshGatewayRuntimeError("MESH_RUNTIME_STOPPING");
    }
    this.started = true;
    const generation = ++this.lifecycleGeneration;
    this.unsubscribe = this.options.transport.subscribe((event) => {
      if (this.isActiveGeneration(generation)) {
        this.onTransportEvent(event);
      }
    });
    const connectPromise = this.options.transport
      .connect()
      .catch((error: unknown) => {
        if (this.isActiveGeneration(generation)) {
          this.publish("mesh.transport.error", {
            code: stableErrorCode(error, "MESH_TRANSPORT_CONNECT_FAILED"),
            transport: this.options.transport.kind,
          });
        }
      })
      .finally(() => {
        if (this.connectPromise === connectPromise) {
          this.connectPromise = undefined;
        }
      });
    this.connectPromise = connectPromise;
  }

  stop(): Promise<void> {
    if (this.stopPromise) {
      return this.stopPromise;
    }
    if (!this.started && !this.connectPromise && !this.teardownRequired) {
      return Promise.resolve();
    }
    this.started = false;
    this.teardownRequired = true;
    this.lifecycleGeneration += 1;
    this.unsubscribe?.();
    this.unsubscribe = undefined;
    const pendingConnect = this.connectPromise;
    const stopPromise = this.stopInternal(pendingConnect)
      .then(() => {
        this.teardownRequired = false;
      })
      .finally(() => {
        if (this.stopPromise === stopPromise) {
          this.stopPromise = undefined;
        }
      });
    this.stopPromise = stopPromise;
    return stopPromise;
  }

  private async stopInternal(
    pendingConnect: Promise<void> | undefined,
  ): Promise<void> {
    const deadline = performance.now() + this.stopTimeoutMs;
    let firstDisconnectFailed = false;
    let firstDisconnectError: unknown;
    try {
      await settleBefore(this.options.transport.disconnect(), deadline);
    } catch (error) {
      firstDisconnectFailed = true;
      firstDisconnectError = error;
    }
    let pendingConnectFailed = false;
    let pendingConnectError: unknown;
    if (pendingConnect) {
      try {
        await settleBefore(pendingConnect, deadline);
      } catch (error) {
        pendingConnectFailed = true;
        pendingConnectError = error;
      }
    }
    let retryAttempted = false;
    let retryFailed = false;
    let retryError: unknown;
    if (pendingConnect || firstDisconnectFailed) {
      retryAttempted = true;
      try {
        await settleBefore(this.options.transport.disconnect(), deadline);
      } catch (error) {
        retryFailed = true;
        retryError = error;
      }
    }
    const disconnected = this.options.transport.state.status === "disconnected";
    const firstDisconnectRecovered =
      firstDisconnectFailed &&
      retryAttempted &&
      !retryFailed &&
      !pendingConnectFailed &&
      disconnected;
    const terminalFailure =
      firstDisconnectFailed && !firstDisconnectRecovered
        ? { error: firstDisconnectError }
        : pendingConnectFailed
          ? { error: pendingConnectError }
          : retryFailed
            ? { error: retryError }
            : undefined;
    if (terminalFailure) {
      throw new MeshGatewayRuntimeError(
        stableErrorCode(
          terminalFailure.error,
          "MESH_TRANSPORT_DISCONNECT_FAILED",
        ),
      );
    }
    if (!disconnected) {
      throw new MeshGatewayRuntimeError(
        "MESH_TRANSPORT_DISCONNECT_UNCONFIRMED",
      );
    }
    this.sealPacketRecorder();
  }

  private isActiveGeneration(generation: number): boolean {
    return this.started && generation === this.lifecycleGeneration;
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
    const connection = this.options.transport.state;
    this.options.packetRecorder?.record({
      gatewayId: this.options.gatewayId,
      meshNetworkId: this.options.meshNetworkId,
      observation,
      rawFrame: frame.frame,
      receivedAt: frame.receivedAt,
      transport: this.options.transport.kind,
      transportMetadata: {
        connectionStatus: connection.status,
        ...(connection.status === "backoff"
          ? { reconnectAttempt: connection.attempt }
          : {}),
      },
    });
    this.options.database.meshObservations.insert(observation);
    const payload = this.domainStore.persist(
      this.options.meshNetworkId,
      observation,
    );
    try {
      this.options.aprs?.onDecodedSummary?.(
        aprsSummaryType(normalizedFromRadio.packet?.portNum, payload.kind),
        Date.parse(serverIngestedAt),
      );
    } catch (error) {
      this.publish("aprs.igate.counter.error", {
        code: stableErrorCode(error, "APRS_IGATE_COUNTER_FAILED"),
      });
    }
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

  private sealPacketRecorder(): void {
    const recorder = this.options.packetRecorder;
    if (!recorder) {
      return;
    }
    try {
      const sealed = recorder.sealAndSanitize();
      this.publish("mesh.capture.sealed", {
        digest: sealed.digest,
        fixtures: sealed.fixtureSet.fixtures.length,
        sanitized: true,
      });
    } catch {
      this.publish("mesh.capture.error", {
        code: "PACKET_FIXTURE_SANITIZATION_INVALID",
      });
    } finally {
      recorder.clear();
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

    const aprsState = this.options.aprs?.stateProvider?.();
    if (!this.options.aprs?.stateProvider || !aprsState) {
      const decision = this.recordDecision(
        positionObservation,
        duplicate.event,
        "APRS_PROVISION_UNAVAILABLE",
      );
      return { event: duplicate.event, decision, outboxCreated: false };
    }

    const validationClock = this.clock();
    const eligibility = validatePositionForAprs(duplicate.event, {
      now: validationClock,
    });
    if (!eligibility.accepted) {
      const decision = this.recordDecision(
        positionObservation,
        duplicate.event,
        eligibility.code,
      );
      return { event: duplicate.event, decision, outboxCreated: false };
    }

    const mapping = selectActiveMapping(
      aprsState.mappings,
      duplicate.event.meshNetworkId,
      duplicate.event.nodeNum,
      eligibility.event.eventTime!,
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
      now: validationClock,
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

    const aprsIdentity = aprsState.identity;
    const activeMapping = mapping.mapping;
    const mappingSymbolTable = Object.prototype.hasOwnProperty.call(
      activeMapping,
      "symbolTable",
    )
      ? (activeMapping.symbolTable ?? "/")
      : aprsIdentity.symbolTable;
    const mappingSymbolCode = Object.prototype.hasOwnProperty.call(
      activeMapping,
      "symbolCode",
    )
      ? (activeMapping.symbolCode ?? ">")
      : aprsIdentity.symbolCode;
    const mappingSymbolOverlay = Object.prototype.hasOwnProperty.call(
      activeMapping,
      "symbolOverlay",
    )
      ? activeMapping.symbolOverlay
      : aprsIdentity.symbolOverlay;
    const encoded = encodeAprsPosition(validation.event, {
      mappingCallsign: target.callsign,
      mappingSymbolTable,
      mappingSymbolCode,
      provisionIgateCallsign: aprsIdentity.callsign,
      ...(mappingSymbolOverlay === undefined ? {} : { mappingSymbolOverlay }),
      ...(typeof activeMapping.comment === "string"
        ? { mappingComment: activeMapping.comment }
        : {}),
      ...(typeof activeMapping.altitudeMeters === "number"
        ? { mappingAltitudeMeters: activeMapping.altitudeMeters }
        : {}),
      ...(typeof aprsState.provision.altitudeMeters === "number"
        ? { provisionAltitudeMeters: aprsState.provision.altitudeMeters }
        : {}),
    });
    const monitorTarget = {
      ...target,
      meshNetworkId: duplicate.event.meshNetworkId,
      nodeNum: duplicate.event.nodeNum,
    };
    if (
      !this.remoteHighWater.canUploadData(
        encoded.data,
        monitorTarget,
        validationClock.toISOString(),
      )
    ) {
      const decision = this.recordDecision(
        positionObservation,
        duplicate.event,
        "APRS_SKIPPED_RECENT_DUPLICATE",
        target,
      );
      return { event: duplicate.event, decision, outboxCreated: false };
    }
    let enqueued:
      ReturnType<GatewayDatabase["aprsOutbox"]["enqueue"]> | undefined;
    const ordered = this.highWater.apply(
      validation.event,
      target,
      observation.serverIngestedAt,
      {
        observationId: positionObservation.id,
        onAccepted: (acceptedEvent) => {
          enqueued = this.options.database.aprsOutbox.enqueue({
            callsign: target.callsign,
            canonicalEventId: acceptedEvent.id,
            data: encoded.data,
            now: observation.serverIngestedAt,
            provisionFingerprint: aprsState.provisionFingerprint,
            order: {
              meshNetworkId: acceptedEvent.meshNetworkId,
              nodeNum: acceptedEvent.nodeNum,
              mappingVersion: target.mappingVersion,
              ...(acceptedEvent.eventTime
                ? { eventTime: acceptedEvent.eventTime }
                : {}),
              ...(acceptedEvent.sequenceEpoch === undefined
                ? {}
                : { sequenceEpoch: acceptedEvent.sequenceEpoch }),
              ...(acceptedEvent.sequenceNumber === undefined
                ? {}
                : { sequenceNumber: acceptedEvent.sequenceNumber }),
            },
          });
          return enqueued.suppressed ? "APRS_SKIPPED_OUT_OF_ORDER" : undefined;
        },
      },
    );
    this.publishDecision(ordered.decision);
    if (
      enqueued?.created &&
      enqueued.suppressed &&
      enqueued.entry?.status === "failed"
    ) {
      this.publish("aprs.outbox.failed", {
        outboxId: enqueued.entry.id,
        canonicalEventId: enqueued.entry.canonicalEventId,
        callsign: enqueued.entry.callsign,
        status: enqueued.entry.status,
        attempts: enqueued.entry.attempts,
        ...(enqueued.entry.lastErrorCode
          ? { code: enqueued.entry.lastErrorCode }
          : {}),
      });
    }
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
    if (enqueued.suppressed || !enqueued.entry) {
      return {
        event: ordered.event,
        decision: ordered.decision,
        outboxCreated: false,
      };
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
    target?: PositionMappingTarget,
  ): PositionDecision {
    const digest = createHash("sha256")
      .update(
        [
          observation.id,
          event.id,
          target?.callsign ?? "",
          target?.mappingVersion ?? "",
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
      parameters: target
        ? {
            callsign: target.callsign,
            mappingVersion: target.mappingVersion,
          }
        : {},
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

function aprsSummaryType(
  portNum: string | undefined,
  payloadKind: StoredApplicationPayload["kind"],
): string {
  const normalizedPort = portNum
    ?.toLowerCase()
    .replace(/_app$/u, "")
    .replaceAll("_", "");
  if (normalizedPort === "textmessage") {
    return "text";
  }
  if (normalizedPort) {
    return normalizedPort;
  }
  if (payloadKind === "node") {
    return "nodeinfo";
  }
  return payloadKind;
}

async function settleBefore<T>(
  promise: Promise<T>,
  deadline: number,
): Promise<T> {
  const remainingMs = deadline - performance.now();
  if (remainingMs <= 0) {
    void promise.catch(() => undefined);
    throw new MeshGatewayRuntimeError("MESH_STOP_TIMEOUT");
  }
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(
          () => reject(new MeshGatewayRuntimeError("MESH_STOP_TIMEOUT")),
          remainingMs,
        );
      }),
    ]);
  } finally {
    if (timer) {
      clearTimeout(timer);
    }
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
