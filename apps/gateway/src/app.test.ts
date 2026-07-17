import { describe, expect, it } from "vitest";

import {
  GatewayConfigurationError,
  GatewayRuntime,
  parseGatewayListenOptions,
} from "./app";

describe("GatewayRuntime", () => {
  it("fails closed for a non-loopback bind", () => {
    expect(() =>
      parseGatewayListenOptions({ CMCLIENT_GATEWAY_HOST: "0.0.0.0" }),
    ).toThrow(GatewayConfigurationError);
  });

  it("listens and closes gracefully", async () => {
    const runtime = new GatewayRuntime({ host: "127.0.0.1", port: 0 });
    let closed = false;
    runtime.app.addHook("onClose", () => {
      closed = true;
    });

    const address = await runtime.start();
    expect(address.port).toBeGreaterThan(0);
    expect(runtime.app.server.listening).toBe(true);
    await runtime.close();
    expect(closed).toBe(true);
    expect(runtime.app.server.listening).toBe(false);
  });
});
