import { describe, expect, it } from "vitest";

import { MemoryLogger } from "../observability";
import { loadMeshtasticSchema } from "../protobuf/schema";
import { ProxyOutboundRouter } from "./outbound";
import { ProxyAccessController, ProxyPolicyError } from "./policy";

describe("proxy access controller", () => {
  it("fails closed for LAN binds without explicit enablement and a numeric allowlist", async () => {
    const schema = await loadMeshtasticSchema();
    expect(
      () => new ProxyAccessController(schema, { bindHost: "0.0.0.0" }),
    ).toThrow(ProxyPolicyError);
    expect(
      () =>
        new ProxyAccessController(schema, {
          allowLan: true,
          bindHost: "0.0.0.0",
        }),
    ).toThrow(ProxyPolicyError);
    expect(
      () =>
        new ProxyAccessController(schema, {
          allowLan: true,
          allowlist: ["client.example.test"],
          bindHost: "0.0.0.0",
        }),
    ).toThrow(ProxyPolicyError);
  });

  it("admits only allowlisted clients and audits without recording raw addresses", async () => {
    const schema = await loadMeshtasticSchema();
    const logger = new MemoryLogger();
    const controller = new ProxyAccessController(schema, {
      allowLan: true,
      allowlist: ["192.0.2.40"],
      bindHost: "0.0.0.0",
      logger,
      maxClients: 1,
      mode: "full",
      traceIdFactory: () => "proxy-audit-trace",
    });

    expect(() =>
      controller.admit({ id: "client-denied", address: "192.0.2.41" }),
    ).toThrow(ProxyPolicyError);
    controller.admit({ id: "client-a", address: "192.0.2.40" });
    expect(() =>
      controller.admit({ id: "client-b", address: "192.0.2.40" }),
    ).toThrow(ProxyPolicyError);
    expect(controller.snapshot).toMatchObject({
      activeClientIds: ["client-a"],
      allowedAddressCount: 1,
      allowLan: true,
    });
    expect(controller.auditSnapshot()).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          action: "client_rejected",
          code: "PROXY_CLIENT_ADDRESS_NOT_ALLOWED",
        }),
        expect.objectContaining({
          action: "client_admitted",
          clientFingerprint: expect.stringMatching(/^[a-f0-9]{16}$/),
        }),
      ]),
    );
    expect(JSON.stringify(logger.entries)).not.toContain("192.0.2.40");
    expect(JSON.stringify(logger.entries)).not.toContain("192.0.2.41");
    expect(JSON.stringify(logger.entries)).not.toContain("client-a");
  });

  it("enforces monitor, message, and full write modes through the outbound router", async () => {
    const schema = await loadMeshtasticSchema();
    const textPortNum = schema.portNum.values.TEXT_MESSAGE_APP;
    if (typeof textPortNum !== "number") {
      throw new Error("Meshtastic text message port number is unavailable");
    }
    const textFrame = new Uint8Array(
      schema.toRadio
        .encode({
          packet: {
            decoded: {
              payload: new Uint8Array([104, 105]),
              portnum: textPortNum,
            },
          },
        })
        .finish(),
    );
    const configFrame = new Uint8Array(
      schema.toRadio.encode({ wantConfigId: 42 }).finish(),
    );
    expect(
      schema.toRadio.toObject(schema.toRadio.decode(textFrame), {
        enums: String,
        oneofs: true,
      }),
    ).toMatchObject({
      packet: { decoded: { portnum: "TEXT_MESSAGE_APP" } },
      payloadVariant: "packet",
    });
    const monitor = new ProxyAccessController(schema);
    monitor.admit({ id: "monitor", address: "127.0.0.1" });
    const monitorRouter = new ProxyOutboundRouter(
      schema,
      new FakeUpstream(),
      { deliver: () => true },
      { authorizer: monitor },
    );
    await expect(
      monitorRouter.submit({ clientId: "monitor", frame: textFrame }),
    ).rejects.toMatchObject({ code: "PROXY_MODE_MONITOR_READ_ONLY" });

    const message = new ProxyAccessController(schema, { mode: "message" });
    message.admit({ id: "message", address: "127.0.0.1" });
    const messageUpstream = new FakeUpstream();
    const messageRouter = new ProxyOutboundRouter(
      schema,
      messageUpstream,
      { deliver: () => true },
      { authorizer: message },
    );
    await expect(
      messageRouter.submit({ clientId: "message", frame: textFrame }),
    ).resolves.toEqual({ correlations: [] });
    await expect(
      messageRouter.submit({ clientId: "message", frame: configFrame }),
    ).rejects.toMatchObject({ code: "PROXY_MODE_MESSAGE_WRITE_FORBIDDEN" });
    expect(messageUpstream.frames).toEqual([textFrame]);

    const full = new ProxyAccessController(schema, { mode: "full" });
    full.admit({ id: "full", address: "127.0.0.1" });
    const fullRouter = new ProxyOutboundRouter(
      schema,
      new FakeUpstream(),
      { deliver: () => true },
      { authorizer: full },
    );
    await expect(
      fullRouter.submit({ clientId: "full", frame: configFrame }),
    ).resolves.toEqual({
      correlations: [{ id: 42, kind: "config" }],
    });
    monitorRouter.stop();
    messageRouter.stop();
    fullRouter.stop();
  });

  it("bounds per-client write rate independently from connection capacity", async () => {
    const schema = await loadMeshtasticSchema();
    const textPortNum = schema.portNum.values.TEXT_MESSAGE_APP;
    if (typeof textPortNum !== "number") {
      throw new Error("Meshtastic text message port number is unavailable");
    }
    let now = new Date("2026-07-18T00:00:00.000Z");
    const controller = new ProxyAccessController(schema, {
      clock: () => now,
      maxWritesPerMinute: 1,
      mode: "message",
    });
    controller.admit({ id: "client-a", address: "127.0.0.1" });
    const frame = new Uint8Array(
      schema.toRadio
        .encode({ packet: { decoded: { portnum: textPortNum } } })
        .finish(),
    );

    controller.authorizeOutbound("client-a", frame);
    expect(() => controller.authorizeOutbound("client-a", frame)).toThrow(
      "PROXY_WRITE_RATE_LIMITED",
    );
    now = new Date("2026-07-18T00:01:01.000Z");
    expect(() => controller.authorizeOutbound("client-a", frame)).not.toThrow();
  });
});

class FakeUpstream {
  readonly frames: Uint8Array[] = [];

  async writeFrame(frame: Uint8Array): Promise<void> {
    this.frames.push(new Uint8Array(frame));
  }
}
