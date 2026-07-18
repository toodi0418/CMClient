import { hostname } from "node:os";

import type { CallMeshMapping } from "@cmclient/contracts";

import { AprsIsRxClient, type AprsIsRxSession } from "./aprs-monitor.js";
import { AprsIsTcpClient, AprsOutboxWorker } from "./aprs-outbox.js";
import { AprsGatewayRuntime } from "./aprs-runtime.js";
import { DomainEventBus } from "./events.js";
import { MeshGatewayRuntime } from "./mesh-runtime.js";
import { GatewayMaintenanceRuntime } from "./maintenance.js";
import { GatewayDatabase } from "./persistence/database.js";
import { MeshtasticApplicationDecoder } from "./protobuf/application.js";
import { MeshtasticProtobufCodec } from "./protobuf/protobuf.js";
import { loadMeshtasticSchema } from "./protobuf/schema.js";
import {
  NativeSerialPortAdapter,
  SerialMeshtasticTransport,
} from "./transport/serial.js";
import { TcpMeshtasticTransport } from "./transport/tcp.js";

export class GatewayRuntimeConfigurationError extends Error {
  constructor(readonly code: string) {
    super(code);
    this.name = "GatewayRuntimeConfigurationError";
  }
}

export function createConfiguredGatewayMaintenanceRuntime(
  environment: Record<string, string | undefined>,
  database: GatewayDatabase,
  eventBus: DomainEventBus,
): GatewayMaintenanceRuntime {
  return new GatewayMaintenanceRuntime({
    database,
    eventBus,
    retentionDays: parsePositiveInteger(
      environment.CMCLIENT_TELEMETRY_RETENTION_DAYS,
      30,
      "TELEMETRY_RETENTION_CONFIGURATION_INVALID",
    ),
    telemetryBatchSize: parsePositiveInteger(
      environment.CMCLIENT_TELEMETRY_RETENTION_BATCH_SIZE,
      1_000,
      "TELEMETRY_RETENTION_CONFIGURATION_INVALID",
    ),
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
  mappingProvider?: () => readonly CallMeshMapping[],
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
    ...(mappingProvider ? { mappingProvider } : {}),
    aprs: parseAprsEncodingOptions(environment),
  });
}

export function createConfiguredAprsGatewayRuntime(
  environment: Record<string, string | undefined>,
  database: GatewayDatabase,
  eventBus: DomainEventBus,
  mappingProvider?: () => readonly CallMeshMapping[],
): AprsGatewayRuntime | undefined {
  const callsign = environment.CMCLIENT_APRS_LOGIN_CALLSIGN?.trim();
  const passcode = environment.CMCLIENT_APRS_PASSCODE?.trim();
  const explicitlyEnabled = parseOptionalBoolean(
    environment.CMCLIENT_APRS_ENABLED,
  );
  const enabled = explicitlyEnabled ?? Boolean(callsign || passcode);
  if (!enabled) {
    return undefined;
  }
  if (
    !callsign ||
    !/^[A-Z0-9]{1,6}(?:-[0-9]{1,2})?$/.test(callsign) ||
    !passcode ||
    !/^\d{1,5}$/.test(passcode) ||
    Number(passcode) > 32_767
  ) {
    throw new GatewayRuntimeConfigurationError(
      "APRS_CREDENTIAL_CONFIGURATION_INVALID",
    );
  }
  const host = boundedText(
    environment.CMCLIENT_APRS_HOST?.trim() || "rotate.aprs2.net",
    255,
    "APRS_ENDPOINT_CONFIGURATION_INVALID",
  );
  const port = parsePort(
    environment.CMCLIENT_APRS_PORT,
    14_580,
    "APRS_ENDPOINT_CONFIGURATION_INVALID",
  );
  const timeoutMs = parsePositiveInteger(
    environment.CMCLIENT_APRS_TIMEOUT_MS,
    10_000,
    "APRS_ENDPOINT_CONFIGURATION_INVALID",
  );
  const loginLine = `user ${callsign} pass ${passcode} vers CMClient 2.0`;
  const worker = new AprsOutboxWorker(
    database.aprsOutbox,
    new AprsIsTcpClient({ host, port, loginLine, timeoutMs }),
  );
  return new AprsGatewayRuntime({
    database,
    eventBus,
    ...(mappingProvider ? { mappingProvider } : {}),
    outbox: worker,
    monitorClientFactory: (filterExpression) => ({
      connect: (onLine: (line: string) => void): Promise<AprsIsRxSession> =>
        new AprsIsRxClient({
          host,
          port,
          loginLine,
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
): {
  comment?: string;
  destination: string;
  symbolCode: string;
  symbolTable: string;
} {
  const destination = environment.CMCLIENT_APRS_DESTINATION?.trim() || "APCM20";
  const symbolTable = environment.CMCLIENT_APRS_SYMBOL_TABLE ?? "/";
  const symbolCode = environment.CMCLIENT_APRS_SYMBOL_CODE ?? ">";
  const comment = environment.CMCLIENT_APRS_COMMENT?.trim();
  if (
    !/^[A-Z0-9]{1,6}$/.test(destination) ||
    !/^[ -~]$/.test(symbolTable) ||
    !/^[ -~]$/.test(symbolCode) ||
    (comment !== undefined &&
      (comment.length === 0 || comment.length > 80 || /[\r\n]/.test(comment)))
  ) {
    throw new GatewayRuntimeConfigurationError(
      "APRS_ENCODING_CONFIGURATION_INVALID",
    );
  }
  return {
    destination,
    symbolTable,
    symbolCode,
    ...(comment ? { comment } : {}),
  };
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
