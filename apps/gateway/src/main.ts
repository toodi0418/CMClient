import { homedir } from "node:os";
import { join } from "node:path";

import { GatewayRuntime, parseGatewayListenOptions } from "./app.js";
import { createVerifiedGatewayBackup } from "./backup.js";
import { DomainEventBus } from "./events.js";
import { JobEngine } from "./jobs.js";
import { GatewayDatabase } from "./persistence/database.js";
import { callMeshOptionsFromEnvironment, CallMeshClient } from "./callmesh.js";
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
import {
  createGatewaySupervisorShutdownInput,
  createStartupShutdownCoordinator,
  GatewayTrackedOperation,
  runCleanupPhases,
} from "./shutdown.js";
import { TcpMeshtasticTransport } from "./transport/tcp.js";
import { compiledGatewayBuildVersion } from "./system.js";
import {
  readGatewayBootstrap,
  registerGatewayOwnershipProofEndpoint,
  startSupervisedGateway,
  type GatewayBootstrapFrame,
} from "./bootstrap.js";
import { ConsoleStructuredLogger } from "./observability.js";

void runGateway().catch((error: unknown) => {
  process.stderr.write(`${runtimeErrorCode(error, "GATEWAY_MAIN_FAILED")}\n`);
  process.exit(1);
});

async function runGateway(): Promise<void> {
  const supervised = process.env.CMCLIENT_SUPERVISED === "1";
  const bootstrap: GatewayBootstrapFrame | undefined = supervised
    ? await readGatewayBootstrap(process.stdin)
    : undefined;
  let database: GatewayDatabase | undefined;
  let events: DomainEventBus | undefined;
  let jobs: JobEngine | undefined;
  let proxy: ProxyRuntime | undefined;
  let mesh: MeshGatewayRuntime | undefined;
  let aprs: AprsGatewayRuntime | undefined;
  let maintenance: GatewayMaintenanceRuntime | undefined;
  let runtime: GatewayRuntime | undefined;
  let callmeshTimer: NodeJS.Timeout | undefined;
  let databaseCloseAllowed = true;
  let detachSupervisorInput = (): void => undefined;
  const callmeshRefresh = new GatewayTrackedOperation((error) => {
    events?.publish({
      type: "callmesh.error",
      source: "gateway",
      payload: { code: runtimeErrorCode(error, "CALLMESH_REFRESH_FAILED") },
    });
  });
  const lifecycle = createStartupShutdownCoordinator<void>(async () => {
    detachSupervisorInput();
    if (callmeshTimer) {
      clearInterval(callmeshTimer);
      callmeshTimer = undefined;
    }
    await runCleanupPhases([
      [
        () => maintenance?.stop(),
        () => runtime?.close(),
        () => callmeshRefresh.stopAndDrain(),
      ],
      [() => mesh?.stop(), () => aprs?.stop(), () => proxy?.stop()],
      [
        async () => {
          try {
            await jobs?.stop();
          } catch (error) {
            databaseCloseAllowed = false;
            throw error;
          }
        },
      ],
      [() => (databaseCloseAllowed ? database?.close() : undefined)],
    ]);
  }, 30_000);
  const terminateAfterShutdown = (): void => {
    void lifecycle.shutdown().then(
      () => process.exit(0),
      (error: unknown) => {
        process.stderr.write(
          `${runtimeErrorCode(error, "GATEWAY_SHUTDOWN_FAILED")}\n`,
        );
        process.exit(1);
      },
    );
  };

  process.once("SIGINT", terminateAfterShutdown);
  process.once("SIGTERM", terminateAfterShutdown);
  if (supervised) {
    const supervisorInput = createGatewaySupervisorShutdownInput(
      terminateAfterShutdown,
    );
    const onData = (chunk: Buffer | string): void =>
      supervisorInput.push(chunk);
    const onEnd = (): void => supervisorInput.end();
    process.stdin.on("data", onData);
    process.stdin.once("end", onEnd);
    process.stdin.once("error", onEnd);
    process.stdin.resume();
    detachSupervisorInput = () => {
      process.stdin.off("data", onData);
      process.stdin.off("end", onEnd);
      process.stdin.off("error", onEnd);
      process.stdin.pause();
      detachSupervisorInput = (): void => undefined;
    };
  }

  const startup = await lifecycle.start(async (context) => {
    const dataDirectory = gatewayDataDirectory(process.env);
    const activeDatabase = new GatewayDatabase(
      join(dataDirectory, "gateway.sqlite"),
    );
    database = activeDatabase;
    context.throwIfShutdownRequested();
    const activeEvents = new DomainEventBus();
    events = activeEvents;
    const activeJobs = new JobEngine(activeDatabase.jobs, activeEvents, {
      handlers: [
        {
          type: "diagnostics.integrity_check",
          handler: async (context) => {
            context.throwIfCancellationRequested();
            return { integrity: activeDatabase.integrityCheck() };
          },
        },
        {
          type: "backup.create",
          handler: async (context) => {
            context.throwIfCancellationRequested();
            const result = await createVerifiedGatewayBackup(
              activeDatabase.connection,
              join(dataDirectory, "backups"),
              context.job.id,
              context.signal,
            );
            context.throwIfCancellationRequested();
            return { ...result };
          },
        },
      ],
    });
    jobs = activeJobs;
    activeJobs.recover();
    context.throwIfShutdownRequested();

    const callmesh = new CallMeshClient(
      callMeshOptionsFromEnvironment(
        process.env,
        compiledGatewayBuildVersion(),
      ),
      activeDatabase.callmeshMappings,
    );
    context.throwIfShutdownRequested();
    const listenOptions = bootstrap
      ? { host: "127.0.0.1", port: 0 }
      : parseGatewayListenOptions(process.env);
    const verifiedAprsState = () => callmesh.getAprsState();

    proxy = await createConfiguredProxyRuntime(process.env, activeEvents);
    context.throwIfShutdownRequested();
    maintenance = createConfiguredGatewayMaintenanceRuntime(
      process.env,
      activeDatabase,
      activeEvents,
    );
    context.throwIfShutdownRequested();
    aprs = createConfiguredAprsGatewayRuntime(
      process.env,
      activeDatabase,
      activeEvents,
      verifiedAprsState,
    );
    context.throwIfShutdownRequested();
    mesh = await createConfiguredMeshGatewayRuntime(
      process.env,
      activeDatabase,
      activeEvents,
      verifiedAprsState,
    );
    context.throwIfShutdownRequested();

    const activeRuntime = new GatewayRuntime(
      listenOptions,
      bootstrap ? new ConsoleStructuredLogger(process.stderr) : undefined,
      undefined,
      activeEvents,
      {
        get: (jobId) => activeJobs.get(jobId),
        cancel: (jobId, correlationId) =>
          activeJobs.cancel(jobId, correlationId),
        submitIntegrityCheck: (correlationId, idempotencyKey) =>
          activeJobs.submit({
            type: "diagnostics.integrity_check",
            input: {},
            ...(correlationId ? { correlationId } : {}),
            ...(idempotencyKey ? { idempotencyKey } : {}),
          }),
        submitBackup: (correlationId, idempotencyKey) =>
          activeJobs.submit({
            type: "backup.create",
            input: {},
            ...(correlationId ? { correlationId } : {}),
            ...(idempotencyKey ? { idempotencyKey } : {}),
          }),
      },
      {
        listNodes: (limit) => activeDatabase.meshNodes.list(limit),
        listMessages: (limit) => activeDatabase.meshMessages.list(limit),
        listTelemetry: (limit) => activeDatabase.meshTelemetry.list(limit),
        queryTelemetry: (query) => activeDatabase.meshTelemetry.query(query),
        listPositions: (limit) =>
          activeDatabase.positions.listCanonicalEvents(limit),
        listAprsOutbox: (limit) => activeDatabase.aprsOutbox.list(limit),
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
      bootstrap ? { capability: bootstrap.capability } : undefined,
    );
    runtime = activeRuntime;
    context.throwIfShutdownRequested();

    const startExternalRuntimes = async (): Promise<void> => {
      await callmeshRefresh.run(() =>
        synchronizeCallMesh(callmesh, activeEvents),
      );
      context.throwIfShutdownRequested();
      await proxy?.start();
      context.throwIfShutdownRequested();
      maintenance?.start();
      context.throwIfShutdownRequested();
      aprs?.start();
      context.throwIfShutdownRequested();
      mesh?.start();
      context.throwIfShutdownRequested();
    };

    if (bootstrap) {
      await startSupervisedGateway(
        process.stdout,
        bootstrap,
        async () => {
          const address = await activeRuntime.start();
          context.throwIfShutdownRequested();
          registerGatewayOwnershipProofEndpoint(
            activeRuntime.app.server,
            bootstrap,
            address,
          );
          return address;
        },
        startExternalRuntimes,
      );
    } else {
      await startExternalRuntimes();
      await activeRuntime.start();
      context.throwIfShutdownRequested();
    }

    callmeshTimer = setInterval(() => {
      void callmeshRefresh.run(async () => {
        await synchronizeCallMesh(callmesh, activeEvents);
        await aprs?.refreshMonitor();
      });
    }, 60_000);
    callmeshTimer.unref();
  });

  if (!startup.ok) {
    if (lifecycle.shutdownRequested) {
      return;
    }
    process.removeListener("SIGINT", terminateAfterShutdown);
    process.removeListener("SIGTERM", terminateAfterShutdown);
    process.stderr.write(
      `${runtimeErrorCode(startup.error, "GATEWAY_RUNTIME_START_FAILED")}\n`,
    );
    if ("cleanupError" in startup) {
      process.stderr.write(
        `${runtimeErrorCode(startup.cleanupError, "GATEWAY_SHUTDOWN_FAILED")}\n`,
      );
      process.exit(1);
    }
    process.exitCode = 1;
  }
}

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
  if (
    parseOptionalBoolean(
      environment.CMCLIENT_MESHTASTIC_PHYSICAL_PROFILE,
      false,
    )
  ) {
    throw new ProxyRuntimeError("PHYSICAL_PROFILE_SECOND_UPSTREAM_FORBIDDEN");
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
