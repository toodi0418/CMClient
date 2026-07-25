import { hostname } from "node:os";
import { isAbsolute, join } from "node:path";

import { AprsIsRxClient, type AprsIsRxSession } from "./aprs-monitor.js";
import { AprsIsTcpClient, AprsOutboxWorker } from "./aprs-outbox.js";
import { AprsGatewayRuntime } from "./aprs-runtime.js";
import {
  connectionAuthorization,
  deriveAprsRuntimeState,
  observerConnectionAuthorization,
  type AprsRuntimeState,
} from "./aprs-identity.js";
import type { CallMeshAprsState } from "./callmesh.js";
import { DomainEventBus } from "./events.js";
import { MeshGatewayRuntime } from "./mesh-runtime.js";
import {
  MAX_OBSERVATION_RETENTION_BATCH_SIZE,
  OBSERVATION_RETENTION_HEADROOM,
  GatewayMaintenanceRuntime,
} from "./maintenance.js";
import { GatewayDatabase } from "./persistence/database.js";
import { MeshtasticApplicationDecoder } from "./protobuf/application.js";
import { MeshtasticProtobufCodec } from "./protobuf/protobuf.js";
import { loadMeshtasticSchema } from "./protobuf/schema.js";
import { projectSyntheticCapture } from "./protobuf/synthetic-capture.js";
import { PacketRecorder } from "./recorder.js";
import {
  NativeSerialPortAdapter,
  SerialMeshtasticTransport,
} from "./transport/serial.js";
import { TcpMeshtasticTransport } from "./transport/tcp.js";
import { PhysicalWriteGuard } from "./transport/physical-guard.js";

export class GatewayRuntimeConfigurationError extends Error {
  constructor(readonly code: string) {
    super(code);
    this.name = "GatewayRuntimeConfigurationError";
  }
}

export type AprsStateProvider = () => AprsRuntimeState | undefined;

export function createConfiguredGatewayMaintenanceRuntime(
  environment: Record<string, string | undefined>,
  database: GatewayDatabase,
  eventBus: DomainEventBus,
): GatewayMaintenanceRuntime {
  const messageBatchSize = parsePositiveInteger(
    environment.CMCLIENT_MESSAGE_RETENTION_BATCH_SIZE,
    1_000,
    "MESSAGE_RETENTION_CONFIGURATION_INVALID",
  );
  const positionBatchSize = parsePositiveInteger(
    environment.CMCLIENT_POSITION_RETENTION_BATCH_SIZE,
    1_000,
    "POSITION_RETENTION_CONFIGURATION_INVALID",
  );
  const telemetryBatchSize = parsePositiveInteger(
    environment.CMCLIENT_TELEMETRY_RETENTION_BATCH_SIZE,
    1_000,
    "TELEMETRY_RETENTION_CONFIGURATION_INVALID",
  );
  const minimumObservationBatchSize =
    messageBatchSize +
    positionBatchSize +
    telemetryBatchSize +
    OBSERVATION_RETENTION_HEADROOM;
  const observationBatchSize = parsePositiveInteger(
    environment.CMCLIENT_OBSERVATION_RETENTION_BATCH_SIZE,
    minimumObservationBatchSize,
    "OBSERVATION_RETENTION_CONFIGURATION_INVALID",
  );
  if (
    observationBatchSize < minimumObservationBatchSize ||
    observationBatchSize > MAX_OBSERVATION_RETENTION_BATCH_SIZE
  ) {
    throw new GatewayRuntimeConfigurationError(
      "OBSERVATION_RETENTION_CONFIGURATION_INVALID",
    );
  }
  return new GatewayMaintenanceRuntime({
    database,
    eventBus,
    aprsOutboxRetentionDays: parsePositiveInteger(
      environment.CMCLIENT_APRS_OUTBOX_RETENTION_DAYS,
      90,
      "APRS_OUTBOX_RETENTION_CONFIGURATION_INVALID",
    ),
    aprsOutboxBatchSize: parsePositiveInteger(
      environment.CMCLIENT_APRS_OUTBOX_RETENTION_BATCH_SIZE,
      1_000,
      "APRS_OUTBOX_RETENTION_CONFIGURATION_INVALID",
    ),
    jobRetentionDays: parsePositiveInteger(
      environment.CMCLIENT_JOB_RETENTION_DAYS,
      90,
      "JOB_RETENTION_CONFIGURATION_INVALID",
    ),
    jobBatchSize: parsePositiveInteger(
      environment.CMCLIENT_JOB_RETENTION_BATCH_SIZE,
      1_000,
      "JOB_RETENTION_CONFIGURATION_INVALID",
    ),
    messageRetentionDays: parsePositiveInteger(
      environment.CMCLIENT_MESSAGE_RETENTION_DAYS,
      30,
      "MESSAGE_RETENTION_CONFIGURATION_INVALID",
    ),
    messageBatchSize,
    positionRetentionDays: parsePositiveInteger(
      environment.CMCLIENT_POSITION_RETENTION_DAYS,
      30,
      "POSITION_RETENTION_CONFIGURATION_INVALID",
    ),
    positionBatchSize,
    observationBatchSize,
    retentionDays: parsePositiveInteger(
      environment.CMCLIENT_TELEMETRY_RETENTION_DAYS,
      30,
      "TELEMETRY_RETENTION_CONFIGURATION_INVALID",
    ),
    telemetryBatchSize,
    intervalMs: parsePositiveInteger(
      environment.CMCLIENT_TELEMETRY_RETENTION_INTERVAL_MS,
      60 * 60 * 1_000,
      "TELEMETRY_RETENTION_CONFIGURATION_INVALID",
    ),
  });
}

export async function createConfiguredMeshGatewayRuntime(
  environment: Record<string, string | undefined>,
  database: GatewayDatabase,
  eventBus: DomainEventBus,
  aprsStateProvider?: () => CallMeshAprsState | undefined,
): Promise<MeshGatewayRuntime | undefined> {
  const kind =
    environment.CMCLIENT_MESHTASTIC_TRANSPORT?.trim().toLowerCase() ||
    "disabled";
  if (kind === "disabled") {
    return undefined;
  }
  if (kind !== "tcp" && kind !== "serial") {
    throw new GatewayRuntimeConfigurationError(
      "MESHTASTIC_TRANSPORT_CONFIGURATION_INVALID",
    );
  }
  const meshNetworkId = boundedText(
    environment.CMCLIENT_MESH_NETWORK_ID?.trim() || "default",
    128,
    "MESH_NETWORK_CONFIGURATION_INVALID",
  );
  const gatewayId = boundedText(
    environment.CMCLIENT_GATEWAY_ID?.trim() || hostname(),
    128,
    "GATEWAY_ID_CONFIGURATION_INVALID",
  );
  const schema = await loadMeshtasticSchema();
  const codec = new MeshtasticProtobufCodec(schema);
  const physicalGuard = createPhysicalWriteGuard(environment, kind);
  const transport =
    kind === "tcp"
      ? new TcpMeshtasticTransport({
          configSession: codec,
          host: boundedText(
            environment.CMCLIENT_MESHTASTIC_TCP_HOST?.trim() || "127.0.0.1",
            255,
            "MESHTASTIC_TCP_CONFIGURATION_INVALID",
          ),
          port: parsePort(
            environment.CMCLIENT_MESHTASTIC_TCP_PORT,
            4403,
            "MESHTASTIC_TCP_CONFIGURATION_INVALID",
          ),
          ...(physicalGuard ? { physicalGuard } : {}),
        })
      : new SerialMeshtasticTransport({
          adapter: new NativeSerialPortAdapter(),
          baudRate: parsePositiveInteger(
            environment.CMCLIENT_MESHTASTIC_SERIAL_BAUD,
            115_200,
            "MESHTASTIC_SERIAL_CONFIGURATION_INVALID",
          ),
          configSession: codec,
          path: boundedText(
            environment.CMCLIENT_MESHTASTIC_SERIAL_PATH?.trim() || "",
            4_096,
            "MESHTASTIC_SERIAL_CONFIGURATION_INVALID",
          ),
        });
  return new MeshGatewayRuntime({
    applicationDecoder: new MeshtasticApplicationDecoder(schema),
    codec,
    database,
    eventBus,
    gatewayId,
    meshNetworkId,
    transport,
    ...(physicalGuard
      ? {
          packetRecorder: new PacketRecorder({
            maximumAgeMs: 5 * 60 * 1_000,
            maximumBytes: 1024 * 1024,
            maximumEntries: 2_048,
            maximumFrameBytes: 512,
            maximumSanitizedBytes: 4 * 1024 * 1024,
            syntheticProjector: (source, sequence) =>
              projectSyntheticCapture(schema, source, sequence),
          }),
        }
      : {}),
    aprs: {
      ...parseAprsEncodingOptions(environment),
      ...(aprsStateProvider
        ? { stateProvider: createAprsRuntimeStateProvider(aprsStateProvider) }
        : {}),
    },
  });
}

function createPhysicalWriteGuard(
  environment: Record<string, string | undefined>,
  transport: "tcp" | "serial",
): PhysicalWriteGuard | undefined {
  const configured = environment.CMCLIENT_MESHTASTIC_PHYSICAL_PROFILE;
  if (configured === undefined || configured.trim() === "") {
    return undefined;
  }
  const normalized = configured.trim().toLowerCase();
  if (["0", "false", "no", "off"].includes(normalized)) {
    return undefined;
  }
  if (!["1", "true", "yes", "on"].includes(normalized)) {
    throw new GatewayRuntimeConfigurationError(
      "PHYSICAL_PROFILE_CONFIGURATION_INVALID",
    );
  }
  const dataDirectory = environment.CMCLIENT_RUNTIME_ROOT?.trim();
  const sourceCommit = environment.CMCLIENT_BUILD_COMMIT?.trim();
  const sourceTree = environment.CMCLIENT_BUILD_TREE?.trim();
  const stage = environment.CMCLIENT_QUALIFICATION_STAGE?.trim();
  if (
    transport !== "tcp" ||
    environment.CMCLIENT_SUPERVISED !== "1" ||
    !dataDirectory ||
    !isAbsolute(dataDirectory) ||
    !sourceCommit ||
    !sourceTree ||
    !stage
  ) {
    throw new GatewayRuntimeConfigurationError(
      "PHYSICAL_PROFILE_CONFIGURATION_INVALID",
    );
  }
  return new PhysicalWriteGuard({
    physicalProfile: true,
    allowedRoot: dataDirectory,
    ledgerPath: join(
      dataDirectory,
      "qualification",
      "physical-write-ledger.sqlite",
    ),
    candidateId: `${sourceCommit}:${sourceTree}`,
    qualificationStage: boundedText(
      stage,
      128,
      "PHYSICAL_PROFILE_CONFIGURATION_INVALID",
    ),
  });
}

export function createConfiguredAprsGatewayRuntime(
  environment: Record<string, string | undefined>,
  database: GatewayDatabase,
  eventBus: DomainEventBus,
  aprsStateProvider?: () => CallMeshAprsState | undefined,
): AprsGatewayRuntime | undefined {
  rejectStaticAprsIdentity(environment);
  const explicitlyEnabled = parseOptionalBoolean(
    environment.CMCLIENT_APRS_ENABLED,
  );
  const enabled = explicitlyEnabled ?? false;
  if (!enabled) {
    return undefined;
  }
  if (!aprsStateProvider) {
    throw new GatewayRuntimeConfigurationError(
      "APRS_PROVISION_CONFIGURATION_REQUIRED",
    );
  }
  const stateProvider = createAprsRuntimeStateProvider(aprsStateProvider);
  const authorizationProvider = connectionAuthorization(stateProvider);
  const monitorAuthorizationProvider =
    observerConnectionAuthorization(stateProvider);
  const { host, port, timeoutMs } = parseAprsEndpointOptions(environment);
  const worker = new AprsOutboxWorker(
    database.aprsOutbox,
    new AprsIsTcpClient({
      host,
      port,
      authorizationProvider,
      timeoutMs,
    }),
    {
      authorizationProvider: () => stateProvider()?.provisionFingerprint,
      clock: () => new Date(),
      initialDelayMs: 1_000,
      maximumDelayMs: 60_000,
    },
  );
  return new AprsGatewayRuntime({
    database,
    eventBus,
    stateProvider,
    outbox: worker,
    monitorClientFactory: (filterExpression, provisionFingerprint) => ({
      connect: (onLine: (line: string) => void): Promise<AprsIsRxSession> =>
        new AprsIsRxClient({
          host,
          port,
          authorizationProvider: monitorAuthorizationProvider,
          provisionFingerprint: provisionFingerprint ?? "",
          filterExpression,
          timeoutMs,
        }).connect(onLine),
    }),
    flushIntervalMs: parsePositiveInteger(
      environment.CMCLIENT_APRS_FLUSH_INTERVAL_MS,
      5_000,
      "APRS_INTERVAL_CONFIGURATION_INVALID",
    ),
    monitorRefreshIntervalMs: parsePositiveInteger(
      environment.CMCLIENT_APRS_MONITOR_REFRESH_INTERVAL_MS,
      60_000,
      "APRS_INTERVAL_CONFIGURATION_INVALID",
    ),
  });
}

export function parseAprsEncodingOptions(
  environment: Record<string, string | undefined>,
): Record<string, never> {
  if (environment.CMCLIENT_APRS_DESTINATION?.trim()) {
    throw new GatewayRuntimeConfigurationError(
      "APRS_ENCODING_CONFIGURATION_INVALID",
    );
  }
  return {};
}

export function parseAprsEndpointOptions(
  environment: Record<string, string | undefined>,
): { host: string; port: number; timeoutMs: number } {
  return {
    host: boundedText(
      environment.CMCLIENT_APRS_HOST?.trim() || "asia.aprs2.net",
      255,
      "APRS_ENDPOINT_CONFIGURATION_INVALID",
    ),
    port: parsePort(
      environment.CMCLIENT_APRS_PORT,
      14_580,
      "APRS_ENDPOINT_CONFIGURATION_INVALID",
    ),
    timeoutMs: parsePositiveInteger(
      environment.CMCLIENT_APRS_TIMEOUT_MS,
      10_000,
      "APRS_ENDPOINT_CONFIGURATION_INVALID",
    ),
  };
}

function createAprsRuntimeStateProvider(
  provider: () => CallMeshAprsState | undefined,
): AprsStateProvider {
  return () => {
    const state = provider();
    if (!state) {
      return undefined;
    }
    try {
      return deriveAprsRuntimeState(state);
    } catch {
      return undefined;
    }
  };
}

function rejectStaticAprsIdentity(
  environment: Record<string, string | undefined>,
): void {
  const forbidden = [
    "CMCLIENT_APRS_LOGIN_CALLSIGN",
    "CMCLIENT_APRS_PASSCODE",
    "CMCLIENT_APRS_SYMBOL_TABLE",
    "CMCLIENT_APRS_SYMBOL_CODE",
    "CMCLIENT_APRS_COMMENT",
  ];
  if (forbidden.some((name) => environment[name]?.trim())) {
    throw new GatewayRuntimeConfigurationError(
      "APRS_STATIC_IDENTITY_FORBIDDEN",
    );
  }
}

function parseOptionalBoolean(value: string | undefined): boolean | undefined {
  if (value === undefined || value.trim() === "") {
    return undefined;
  }
  const normalized = value.trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(normalized)) {
    return true;
  }
  if (["0", "false", "no", "off"].includes(normalized)) {
    return false;
  }
  throw new GatewayRuntimeConfigurationError(
    "APRS_ENABLED_CONFIGURATION_INVALID",
  );
}

function parsePort(
  value: string | undefined,
  fallback: number,
  code: string,
): number {
  const port = parsePositiveInteger(value, fallback, code);
  if (port > 65_535) {
    throw new GatewayRuntimeConfigurationError(code);
  }
  return port;
}

function parsePositiveInteger(
  value: string | undefined,
  fallback: number,
  code: string,
): number {
  const source = value?.trim() || String(fallback);
  if (!/^\d+$/.test(source)) {
    throw new GatewayRuntimeConfigurationError(code);
  }
  const parsed = Number(source);
  if (!Number.isSafeInteger(parsed) || parsed < 1) {
    throw new GatewayRuntimeConfigurationError(code);
  }
  return parsed;
}

function boundedText(
  value: string,
  maximumLength: number,
  code: string,
): string {
  if (
    !value ||
    value.length > maximumLength ||
    value.includes("\r") ||
    value.includes("\n") ||
    value.includes("\u0000")
  ) {
    throw new GatewayRuntimeConfigurationError(code);
  }
  return value;
}
