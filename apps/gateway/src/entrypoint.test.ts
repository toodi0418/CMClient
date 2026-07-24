import { describe, expect, it, vi } from "vitest";

import {
  dispatchGatewayEntrypoint,
  GatewayEntrypointUsageError,
} from "./entrypoint";

describe("Gateway entrypoint dispatch", () => {
  it("dispatches the fixed offline maintenance argument without starting runtime", async () => {
    const runOfflineMaintenance = vi.fn(async () => undefined);
    const runRuntime = vi.fn(async () => undefined);

    await dispatchGatewayEntrypoint(["--offline-maintenance"], {
      runOfflineMaintenance,
      runRuntime,
    });

    expect(runOfflineMaintenance).toHaveBeenCalledOnce();
    expect(runRuntime).not.toHaveBeenCalled();
  });

  it("starts the normal runtime only when no arguments are supplied", async () => {
    const runOfflineMaintenance = vi.fn(async () => undefined);
    const runRuntime = vi.fn(async () => undefined);

    await dispatchGatewayEntrypoint([], {
      runOfflineMaintenance,
      runRuntime,
    });

    expect(runRuntime).toHaveBeenCalledOnce();
    expect(runOfflineMaintenance).not.toHaveBeenCalled();
  });

  it.each([
    ["--offline-maintenance", "unexpected"],
    ["--database", "C:\\private\\cmclient.sqlite"],
    ["--help"],
  ])("rejects every other argument vector", async (...arguments_) => {
    await expect(
      dispatchGatewayEntrypoint(arguments_, {
        runOfflineMaintenance: async () => undefined,
        runRuntime: async () => undefined,
      }),
    ).rejects.toBeInstanceOf(GatewayEntrypointUsageError);
  });
});
