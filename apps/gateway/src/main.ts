import { homedir } from "node:os";
import { join } from "node:path";

import { GatewayRuntime, parseGatewayListenOptions } from "./app.js";
import { DomainEventBus } from "./events.js";
import { JobEngine } from "./jobs.js";
import { GatewayDatabase } from "./persistence/database.js";
import { CallMeshClient } from "./callmesh.js";
import { MeshtasticProtobufCodec } from "./protobuf/protobuf.js";
import { loadMeshtasticSchema } from "./protobuf/schema.js";
import { ProxyAccessController } from "./proxy/policy.js";
import { ProxyRuntime, ProxyRuntimeError } from "./proxy/runtime.js";
import { ProxyConfigCache, ProxyUpstreamManager } from "./proxy/upstream.js";
import { TcpMeshtasticTransport } from "./transport/tcp.js";

const database = new GatewayDatabase(gatewayDatabasePath(process.env));
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
void callmesh.synchronize();
let proxy: ProxyRuntime | undefined;
try {
  proxy = await createConfiguredProxyRuntime(process.env, events);
  await proxy?.start();
} catch (error) {
  const code =
    error instanceof Error && "code" in error
      ? String(error.code)
      : "PROXY_START_FAILED";
  process.stderr.write(`${code}\n`);
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
  },
  {
    listNodes: (limit) => database.meshNodes.list(limit),
    listMessages: (limit) => database.meshMessages.list(limit),
    listTelemetry: (limit) => database.meshTelemetry.list(limit),
    listPositions: (limit) => database.positions.listCanonicalEvents(limit),
    listAprsOutbox: (limit) => database.aprsOutbox.list(limit),
  },
  callmesh,
  proxy,
);
let shuttingDown = false;

async function shutdown(): Promise<void> {
  if (shuttingDown) {
    return;
  }
  shuttingDown = true;
  await runtime.close();
  await proxy?.stop();
  database.close();
}

process.once("SIGINT", () => void shutdown().then(() => process.exit(0)));
process.once("SIGTERM", () => void shutdown().then(() => process.exit(0)));

runtime.start().catch((error: unknown) => {
  const code =
    error instanceof Error && "code" in error
      ? String(error.code)
      : "GATEWAY_START_FAILED";
  process.stderr.write(`${code}\n`);
  database.close();
  process.exitCode = 1;
});

function gatewayDatabasePath(
  environment: Record<string, string | undefined>,
): string {
  const dataDirectory =
    environment.CMCLIENT_DATA_DIR?.trim() || join(homedir(), ".cmclient");
  return join(dataDirectory, "gateway.sqlite");
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
