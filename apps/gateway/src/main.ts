import { homedir } from "node:os";
import { join } from "node:path";

import { GatewayRuntime, parseGatewayListenOptions } from "./app.js";
import { createVerifiedGatewayBackup } from "./backup.js";
import { DomainEventBus } from "./events.js";
import { JobEngine } from "./jobs.js";
import { GatewayDatabase } from "./persistence/database.js";
import { CallMeshClient } from "./callmesh.js";
import type { AprsGatewayRuntime } from "./aprs-runtime.js";
import type { MeshGatewayRuntime } from "./mesh-runtime.js";
import type { GatewayMaintenanceRuntime } from "./maintenance.js";
import { MeshtasticProtobufCodec } from "./protobuf/protobuf.js";
import { loadMeshtasticSchema } from "./protobuf/schema.js";
import { ProxyAccessController } from "./proxy/policy.js";
import { ProxyRuntime, ProxyRuntimeError } from "./proxy/runtime.js";
import { ProxyConfigCache, ProxyUpstreamManager } from "./proxy/upstream.js";
import {
  createConfiguredAprsGatewayRuntime,
  createConfiguredGatewayMaintenanceRuntime,
  createConfiguredMeshGatewayRuntime,
} from "./runtime-config.js";
import { TcpMeshtasticTransport } from "./transport/tcp.js";

const dataDirectory = gatewayDataDirectory(process.env);
const database = new GatewayDatabase(join(dataDirectory, "gateway.sqlite"));
const events = new DomainEventBus();
const jobs = new JobEngine(database.jobs, events, {
  handlers: [
    {
      type: "diagnostics.integrity_check",
      handler: async (context) => {
        context.throwIfCancellationRequested();
        return { integrity: database.integrityCheck() };
      },
    },
    {
      type: "backup.create",
      handler: async (context) => {
        context.throwIfCancellationRequested();
        const result = await createVerifiedGatewayBackup(
          database.connection,
          join(dataDirectory, "backups"),
          context.job.id,
        );
        context.throwIfCancellationRequested();
        return { ...result };
      },
    },
  ],
});
jobs.recover();
const callmeshUrl = process.env.CMCLIENT_CALLMESH_URL?.trim();
const callmeshApiKey = process.env.CMCLIENT_CALLMESH_API_KEY;
const callmesh = new CallMeshClient(
  {
    baseUrl: callmeshUrl || "http://127.0.0.1:9",
    ...(callmeshUrl && callmeshApiKey ? { apiKey: callmeshApiKey } : {}),
  },
  database.callmeshMappings,
);
let proxy: ProxyRuntime | undefined;
let mesh: MeshGatewayRuntime | undefined;
let aprs: AprsGatewayRuntime | undefined;
let maintenance: GatewayMaintenanceRuntime | undefined;
let callmeshTimer: NodeJS.Timeout | undefined;
try {
  await synchronizeCallMesh(callmesh, events);
  const verifiedMappings = () => {
    const overview = callmesh.getOverview();
    return overview.status.state === "ready" ? overview.mappings : [];
  };
  proxy = await createConfiguredProxyRuntime(process.env, events);
  await proxy?.start();
  maintenance = createConfiguredGatewayMaintenanceRuntime(
    process.env,
    database,
    events,
  );
  maintenance.start();
  aprs = createConfiguredAprsGatewayRuntime(
    process.env,
    database,
    events,
    verifiedMappings,
  );
  aprs?.start();
  mesh = await createConfiguredMeshGatewayRuntime(
    process.env,
    database,
    events,
    verifiedMappings,
  );
  mesh?.start();
  callmeshTimer = setInterval(() => {
    void synchronizeCallMesh(callmesh, events).then(() =>
      aprs?.refreshMonitor(),
    );
  }, 60_000);
  callmeshTimer.unref();
} catch (error) {
  process.stderr.write(
    `${runtimeErrorCode(error, "GATEWAY_RUNTIME_START_FAILED")}\n`,
  );
  await mesh?.stop();
  await aprs?.stop();
  maintenance?.stop();
  await proxy?.stop();
  database.close();
  process.exit(1);
}
const runtime = new GatewayRuntime(
  parseGatewayListenOptions(process.env),
  undefined,
  undefined,
  events,
  {
    get: (jobId) => jobs.get(jobId),
    cancel: (jobId, correlationId) => jobs.cancel(jobId, correlationId),
    submitIntegrityCheck: (correlationId, idempotencyKey) =>
      jobs.submit({
        type: "diagnostics.integrity_check",
        input: {},
        ...(correlationId ? { correlationId } : {}),
        ...(idempotencyKey ? { idempotencyKey } : {}),
      }),
    submitBackup: (correlationId, idempotencyKey) =>
      jobs.submit({
        type: "backup.create",
        input: {},
        ...(correlationId ? { correlationId } : {}),
        ...(idempotencyKey ? { idempotencyKey } : {}),
      }),
  },
  {
    listNodes: (limit) => database.meshNodes.list(limit),
    listMessages: (limit) => database.meshMessages.list(limit),
    listTelemetry: (limit) => database.meshTelemetry.list(limit),
    queryTelemetry: (query) => database.meshTelemetry.query(query),
    listPositions: (limit) => database.positions.listCanonicalEvents(limit),
    listAprsOutbox: (limit) => database.aprsOutbox.list(limit),
  },
  callmesh,
  proxy,
  {
    status: () => mesh?.status() ?? { configured: false },
  },
  {
    status: () =>
      aprs?.status() ?? {
        configured: false,
        running: false,
        monitorStatus: "stopped",
        mappedCallsigns: 0,
        pendingOutbox: 0,
        failedOutbox: 0,
      },
  },
);
let shuttingDown = false;

async function shutdown(): Promise<void> {
  if (shuttingDown) {
    return;
  }
  shuttingDown = true;
  if (callmeshTimer) {
    clearInterval(callmeshTimer);
    callmeshTimer = undefined;
  }
  await mesh?.stop();
  await aprs?.stop();
  maintenance?.stop();
  await runtime.close();
  await proxy?.stop();
  database.close();
}

process.once("SIGINT", () => void shutdown().then(() => process.exit(0)));
process.once("SIGTERM", () => void shutdown().then(() => process.exit(0)));

runtime.start().catch((error: unknown) => {
  process.stderr.write(`${runtimeErrorCode(error, "GATEWAY_START_FAILED")}\n`);
  void shutdown().then(() => {
    process.exitCode = 1;
  });
});

async function synchronizeCallMesh(
  client: CallMeshClient,
  eventBus: DomainEventBus,
): Promise<void> {
  try {
    const overview = await client.synchronize();
    eventBus.publish({
      type: "callmesh.status",
      source: "gateway",
      payload: overview.status,
    });
  } catch (error) {
    eventBus.publish({
      type: "callmesh.error",
      source: "gateway",
      payload: { code: runtimeErrorCode(error, "CALLMESH_SYNC_FAILED") },
    });
  }
}

function gatewayDataDirectory(
  environment: Record<string, string | undefined>,
): string {
  return environment.CMCLIENT_DATA_DIR?.trim() || join(homedir(), ".cmclient");
}

async function createConfiguredProxyRuntime(
  environment: Record<string, string | undefined>,
  eventBus: DomainEventBus,
): Promise<ProxyRuntime | undefined> {
  if (!parseOptionalBoolean(environment.CMCLIENT_PROXY_ENABLED, false)) {
    return undefined;
  }
  const upstreamHost = environment.CMCLIENT_PROXY_UPSTREAM_HOST?.trim();
  const upstreamPort = parseRequiredPort(
    environment.CMCLIENT_PROXY_UPSTREAM_PORT,
  );
  if (!upstreamHost || upstreamPort === undefined) {
    throw new ProxyRuntimeError("PROXY_UPSTREAM_CONFIGURATION_REQUIRED");
  }
  const schema = await loadMeshtasticSchema();
  const mode = environment.CMCLIENT_PROXY_MODE?.trim() || "monitor";
  if (mode !== "monitor" && mode !== "message" && mode !== "full") {
    throw new ProxyRuntimeError("PROXY_MODE_CONFIGURATION_INVALID");
  }
  const allowLan = parseOptionalBoolean(
    environment.CMCLIENT_PROXY_ALLOW_LAN,
    false,
  );
  const allowlist = (environment.CMCLIENT_PROXY_ALLOWLIST ?? "")
    .split(",")
    .map((address) => address.trim())
    .filter(Boolean);
  const listenPort = parseRequiredPort(
    environment.CMCLIENT_PROXY_PORT ?? "4403",
  );
  if (listenPort === undefined) {
    throw new ProxyRuntimeError("PROXY_LISTEN_CONFIGURATION_INVALID");
  }
  const policy = new ProxyAccessController(schema, {
    allowLan,
    ...(allowlist.length ? { allowlist } : {}),
    bindHost: environment.CMCLIENT_PROXY_HOST?.trim() || "127.0.0.1",
    mode,
  });
  const transport = new TcpMeshtasticTransport({
    configSession: new MeshtasticProtobufCodec(schema),
    host: upstreamHost,
    port: upstreamPort,
  });
  return new ProxyRuntime({
    eventBus,
    listenPort,
    policy,
    schema,
    upstream: new ProxyUpstreamManager(transport, new ProxyConfigCache(schema)),
  });
}

function parseOptionalBoolean(
  value: string | undefined,
  fallback: boolean,
): boolean {
  if (value === undefined || value.trim() === "") {
    return fallback;
  }
  const normalized = value.trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(normalized)) {
    return true;
  }
  if (["0", "false", "no", "off"].includes(normalized)) {
    return false;
  }
  throw new ProxyRuntimeError("PROXY_BOOLEAN_CONFIGURATION_INVALID");
}

function parseRequiredPort(value: string | undefined): number | undefined {
  if (!value || !/^\d+$/.test(value)) {
    return undefined;
  }
  const port = Number(value);
  return Number.isInteger(port) && port >= 1 && port <= 65_535
    ? port
    : undefined;
}

function runtimeErrorCode(error: unknown, fallback: string): string {
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
