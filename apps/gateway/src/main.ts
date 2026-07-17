import { GatewayRuntime, parseGatewayListenOptions } from "./app.js";

const runtime = new GatewayRuntime(parseGatewayListenOptions(process.env));
let shuttingDown = false;

async function shutdown(): Promise<void> {
  if (shuttingDown) {
    return;
  }
  shuttingDown = true;
  await runtime.close();
}

process.once("SIGINT", () => void shutdown().then(() => process.exit(0)));
process.once("SIGTERM", () => void shutdown().then(() => process.exit(0)));

runtime.start().catch((error: unknown) => {
  const code =
    error instanceof Error && "code" in error
      ? String(error.code)
      : "GATEWAY_START_FAILED";
  process.stderr.write(`${code}\n`);
  process.exitCode = 1;
});
