import { homedir } from "node:os";
import { join } from "node:path";

import { GatewayRuntime, parseGatewayListenOptions } from "./app.js";
import { DomainEventBus } from "./events.js";
import { JobEngine } from "./jobs.js";
import { GatewayDatabase } from "./persistence/database.js";
import { CallMeshClient } from "./callmesh.js";

const database = new GatewayDatabase(gatewayDatabasePath(process.env));
const events = new DomainEventBus();
const jobs = new JobEngine(database.jobs, events);
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
const runtime = new GatewayRuntime(
  parseGatewayListenOptions(process.env),
  undefined,
  undefined,
  events,
  jobs,
  {
    listNodes: (limit) => database.meshNodes.list(limit),
    listMessages: (limit) => database.meshMessages.list(limit),
    listTelemetry: (limit) => database.meshTelemetry.list(limit),
    listPositions: (limit) => database.positions.listCanonicalEvents(limit),
    listAprsOutbox: (limit) => database.aprsOutbox.list(limit),
  },
  callmesh,
);
let shuttingDown = false;

async function shutdown(): Promise<void> {
  if (shuttingDown) {
    return;
  }
  shuttingDown = true;
  await runtime.close();
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
