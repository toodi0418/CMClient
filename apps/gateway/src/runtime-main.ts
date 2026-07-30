import type { CallMeshOverview } from "@cmclient/contracts";

import { GatewayRuntime } from "./app.js";
import { createVerifiedGatewayBackup } from "./backup.js";
import { DomainEventBus } from "./events.js";
import { JobEngine } from "./jobs.js";
import { GatewayDatabase } from "./persistence/database.js";
import {
  callMeshOptionsFromRuntime,
  CallMeshClient,
  CallMeshClientError,
} from "./callmesh.js";
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
  validateConfiguredMeshtasticEndpoint,
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
  GatewayBootstrapError,
  readGatewayBootstrap,
  registerGatewayOwnershipProofEndpoint,
  startSupervisedGateway,
} from "./bootstrap.js";
import { ConsoleStructuredLogger } from "./observability.js";
import { gatewayRuntimePaths } from "./runtime-paths.js";

export interface GatewayExternalStartupCallbacks {
  readonly validateMeshtastic: () => Promise<void>;
  readonly validateCallMesh: () => Promise<void>;
  readonly synchronizeCallMesh: () => Promise<void>;
  readonly startProxy: () => Promise<void>;
  readonly startMaintenance: () => void;
  readonly startAprs: () => void;
  readonly startMesh: () => void;
  readonly throwIfShutdownRequested: () => void;
}

/**
 * Setup validation intentionally authenticates without starting any transport
 * that can ingest Meshtastic or transmit APRS. The Agent commits config and
 * secrets before starting a second, normal Gateway process.
 */
export async function startGatewayExternalRuntimes(
  validationOnly: boolean,
  callbacks: GatewayExternalStartupCallbacks,
): Promise<void> {
  if (validationOnly) {
    await callbacks.validateMeshtastic();
    callbacks.throwIfShutdownRequested();
    await callbacks.validateCallMesh();
    return;
  }
  await callbacks.synchronizeCallMesh();
  callbacks.throwIfShutdownRequested();
  await callbacks.startProxy();
  callbacks.throwIfShutdownRequested();
  callbacks.startMaintenance();
  callbacks.throwIfShutdownRequested();
  callbacks.startAprs();
  callbacks.throwIfShutdownRequested();
  callbacks.startMesh();
  callbacks.throwIfShutdownRequested();
}

export async function runGateway(): Promise<void> {
  if (process.env.CMCLIENT_SUPERVISED !== "1") {
    throw new GatewayBootstrapError("GATEWAY_SUPERVISION_REQUIRED");
  }
  const setupValidationOnly = parseOptionalBoolean(
    process.env.CMCLIENT_SETUP_VALIDATION_ONLY,
    false,
  );
  const setupCommitStart = parseOptionalBoolean(
    process.env.CMCLIENT_SETUP_COMMIT_START,
    false,
  );
  const bootstrap = await readGatewayBootstrap(process.stdin);
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
  const supervisorInput = createGatewaySupervisorShutdownInput(
    terminateAfterShutdown,
  );
  const onData = (chunk: Buffer | string): void => supervisorInput.push(chunk);
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

  const startup = await lifecycle.start(async (context) => {
    const paths = gatewayRuntimePaths(process.env);
    const activeDatabase = new GatewayDatabase(paths.database);
    database = activeDatabase;
    context.throwIfShutdownRequested();
    const activeEvents = new DomainEventBus();
    events = activeEvents;
    const activeJobs = new JobEngine(activeDatabase.jobs, activeEvents, {
      setupGeneration: bootstrap.setupGeneration,
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
              paths.backups,
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
      callMeshOptionsFromRuntime(
        process.env,
        compiledGatewayBuildVersion(),
        bootstrap.callMeshApiKey,
      ),
      setupValidationOnly ? undefined : activeDatabase.callmeshMappings,
    );
    context.throwIfShutdownRequested();
    const verifiedAprsState = () => callmesh.getAprsState();

    if (!setupValidationOnly) {
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
        aprs,
      );
      context.throwIfShutdownRequested();
    }

    const activeRuntime = new GatewayRuntime(
      { host: "127.0.0.1", port: 0 },
      new ConsoleStructuredLogger(process.stderr),
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
        listStationSubmissions: (limit) =>
          aprs?.listStationSubmissions(limit) ?? [],
        status: () =>
          aprs?.status() ?? {
            configured: false,
            running: false,
            monitorStatus: "stopped",
            mappedCallsigns: 0,
            pendingOutbox: 0,
            failedOutbox: 0,
            unconfirmedOutbox: 0,
            pendingStationSubmissions: 0,
            failedStationSubmissions: 0,
            unconfirmedStationSubmissions: 0,
          },
      },
      { capability: bootstrap.capability },
    );
    runtime = activeRuntime;
    context.throwIfShutdownRequested();

    const startExternalRuntimes = async (): Promise<void> =>
      startGatewayExternalRuntimes(setupValidationOnly, {
        validateMeshtastic: async () => {
          try {
            await validateConfiguredMeshtasticEndpoint(process.env);
          } catch {
            throw new GatewayBootstrapError("SETUP_MESHTASTIC_UNREACHABLE");
          }
        },
        validateCallMesh: async () => {
          try {
            // The key exists only in the private bootstrap and this in-memory
            // client until the Agent commits the setup transaction.
            await callmesh.validateCredentials();
          } catch (error) {
            if (error instanceof CallMeshClientError) {
              if (error.code === "CALLMESH_CREDENTIAL_REJECTED") {
                throw new GatewayBootstrapError("CALLMESH_CREDENTIAL_REJECTED");
              }
              if (error.code === "CALLMESH_UNAVAILABLE") {
                throw new GatewayBootstrapError("CALLMESH_UNAVAILABLE");
              }
            }
            throw new GatewayBootstrapError("GATEWAY_EXTERNAL_START_FAILED");
          }
        },
        synchronizeCallMesh: async () => {
          const overview = await synchronizeCallMesh(callmesh, activeEvents);
          if (callMeshCredentialRejected(overview)) {
            throw new GatewayBootstrapError("CALLMESH_CREDENTIAL_REJECTED");
          }
          if (setupCommitStart && !callMeshReadyForSetupCommit(overview)) {
            throw new GatewayBootstrapError("CALLMESH_UNAVAILABLE");
          }
        },
        startProxy: async () => {
          await proxy?.start();
        },
        startMaintenance: () => maintenance?.start(),
        startAprs: () => aprs?.start(),
        startMesh: () => mesh?.start(),
        throwIfShutdownRequested: () => context.throwIfShutdownRequested(),
      });

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

    if (!setupValidationOnly) {
      callmeshTimer = setInterval(() => {
        void callmeshRefresh.run(async () => {
          const overview = await synchronizeCallMesh(callmesh, activeEvents);
          if (callMeshCredentialRejected(overview)) {
            terminateAfterShutdown();
            return;
          }
          await aprs?.refreshMonitor();
        });
      }, 60_000);
      callmeshTimer.unref();
    }
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
): Promise<CallMeshOverview> {
  try {
    const overview = await client.synchronize();
    eventBus.publish({
      type: "callmesh.status",
      source: "gateway",
      payload: overview.status,
    });
    return overview;
  } catch (error) {
    eventBus.publish({
      type: "callmesh.error",
      source: "gateway",
      payload: { code: runtimeErrorCode(error, "CALLMESH_SYNC_FAILED") },
    });
    throw error;
  }
}

function callMeshCredentialRejected(overview: CallMeshOverview): boolean {
  return (
    overview.status.reasonCode === "CALLMESH_AUTH_INVALID" ||
    overview.status.provisionState === "revoked"
  );
}

function callMeshReadyForSetupCommit(overview: CallMeshOverview): boolean {
  return (
    overview.status.state === "ready" &&
    overview.status.provisionState === "valid"
  );
}

async function createConfiguredProxyRuntime(
  environment: Record<string, string | undefined>,
  eventBus: DomainEventBus,
): Promise<ProxyRuntime | undefined> {
  if (!validateProxyUpstreamConfiguration(environment)) {
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

/**
 * Proxy has a dedicated transport until P14-T10 replaces it with the shared
 * ingest upstream. Fail before either runtime can open a second device session.
 */
export function validateProxyUpstreamConfiguration(
  environment: Record<string, string | undefined>,
): boolean {
  if (!parseOptionalBoolean(environment.CMCLIENT_PROXY_ENABLED, false)) {
    return false;
  }
  const meshTransport =
    environment.CMCLIENT_MESHTASTIC_TRANSPORT?.trim().toLowerCase() ||
    "disabled";
  if (meshTransport !== "disabled") {
    throw new ProxyRuntimeError("PROXY_SECOND_UPSTREAM_FORBIDDEN");
  }
  return true;
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
