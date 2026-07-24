import {
  dispatchGatewayEntrypoint,
  isOfflineMaintenanceArguments,
} from "./entrypoint.js";
import {
  offlineMaintenanceExitCode,
  runOfflineMaintenanceCommand,
} from "./offline-maintenance.js";

const gatewayArguments = process.argv.slice(2);

void dispatchGatewayEntrypoint(gatewayArguments, {
  runOfflineMaintenance: () =>
    runOfflineMaintenanceCommand(process.stdin, process.stdout),
  runRuntime: async () => {
    const { runGateway } = await import("./runtime-main.js");
    await runGateway();
  },
}).catch((error: unknown) => {
  process.stderr.write(`${errorCode(error, "GATEWAY_MAIN_FAILED")}\n`);
  process.exitCode = isOfflineMaintenanceArguments(gatewayArguments)
    ? offlineMaintenanceExitCode(error)
    : 1;
});

function errorCode(error: unknown, fallback: string): string {
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
