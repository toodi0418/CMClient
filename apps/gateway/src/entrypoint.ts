export interface GatewayEntrypointHandlers {
  readonly runOfflineMaintenance: () => Promise<void>;
  readonly runRuntime: () => Promise<void>;
}

export class GatewayEntrypointUsageError extends Error {
  readonly code = "GATEWAY_MAIN_USAGE_INVALID";

  constructor() {
    super("GATEWAY_MAIN_USAGE_INVALID");
    this.name = "GatewayEntrypointUsageError";
  }
}

export async function dispatchGatewayEntrypoint(
  arguments_: readonly string[],
  handlers: GatewayEntrypointHandlers,
): Promise<void> {
  if (isOfflineMaintenanceArguments(arguments_)) {
    await handlers.runOfflineMaintenance();
    return;
  }
  if (arguments_.length !== 0) {
    throw new GatewayEntrypointUsageError();
  }
  await handlers.runRuntime();
}

export function isOfflineMaintenanceArguments(
  arguments_: readonly string[],
): boolean {
  return arguments_.length === 1 && arguments_[0] === "--offline-maintenance";
}
