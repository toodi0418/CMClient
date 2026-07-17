import { describe, expect, it } from "vitest";

import {
  GatewayConfigurationError,
  GatewayRuntime,
  createGatewayApp,
  parseGatewayListenOptions,
} from "./app";
import { MemoryLogger, redact } from "./observability";

describe("GatewayRuntime", () => {
  it("fails closed for a non-loopback bind", () => {
    expect(() =>
      parseGatewayListenOptions({ CMCLIENT_GATEWAY_HOST: "0.0.0.0" }),
    ).toThrow(GatewayConfigurationError);
  });

  it("propagates request IDs and redacts structured fields", async () => {
    const logger = new MemoryLogger();
    const app = createGatewayApp(logger);
    const response = await app.inject({
      method: "GET",
      url: "/missing",
      headers: {
        "x-correlation-id": "sync-42",
      },
    });
    expect(response.headers["x-trace-id"]).toMatch(/^[a-f0-9-]{36}$/);
    expect(logger.entries).toHaveLength(1);
    expect(logger.entries[0]).toMatchObject({ correlationId: "sync-42" });
    expect(
      redact({ nested: { apiKey: "secret", passcode: 1234 }, plain: "safe" }),
    ).toEqual({
      nested: { apiKey: "[REDACTED]", passcode: "[REDACTED]" },
      plain: "safe",
    });
    await app.close();
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
