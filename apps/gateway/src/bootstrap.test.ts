import { PassThrough } from "node:stream";

import { describe, expect, it } from "vitest";

import {
  GatewayBootstrapError,
  encodePrivateFrame,
  gatewayCapabilityMatches,
  GATEWAY_PRIVATE_FRAME_MAX_BYTES,
  readGatewayBootstrap,
  writeGatewayReady,
} from "./bootstrap.js";

const bootstrap = {
  schemaVersion: 1 as const,
  type: "gateway.bootstrap" as const,
  startupNonce: "a".repeat(32),
  capability: "b".repeat(64),
};

describe("private Gateway bootstrap", () => {
  it("reads a bounded fragmented frame and writes an exact ready frame", async () => {
    const input = new PassThrough();
    const encoded = encodePrivateFrame(bootstrap);
    const pending = readGatewayBootstrap(input);
    input.write(encoded.subarray(0, 3));
    input.write(encoded.subarray(3, 11));
    input.write(encoded.subarray(11));
    await expect(pending).resolves.toEqual(bootstrap);

    const output = new PassThrough();
    const chunks: Buffer[] = [];
    output.on("data", (chunk: Buffer) => chunks.push(chunk));
    writeGatewayReady(
      output,
      bootstrap,
      { host: "127.0.0.1", port: 49152 },
      42,
    );
    const ready = Buffer.concat(chunks);
    expect(ready.readUInt32BE(0)).toBe(ready.length - 4);
    expect(JSON.parse(ready.subarray(4).toString("utf8"))).toEqual({
      schemaVersion: 1,
      type: "gateway.ready",
      pid: 42,
      startupNonce: bootstrap.startupNonce,
      host: "127.0.0.1",
      port: 49152,
    });
  });

  it("fails closed for oversized, malformed, trailing, and timed-out input", async () => {
    expect(() =>
      encodePrivateFrame("x".repeat(GATEWAY_PRIVATE_FRAME_MAX_BYTES)),
    ).toThrow("GATEWAY_PRIVATE_FRAME_OVERSIZED");

    for (const frame of [
      Buffer.from([0, 0, 0, 1, 0xff]),
      Buffer.concat([encodePrivateFrame(bootstrap), Buffer.from([0])]),
      encodePrivateFrame({ ...bootstrap, capability: "not-a-capability" }),
    ]) {
      const input = new PassThrough();
      const pending = readGatewayBootstrap(input, 100);
      input.write(frame);
      await expect(pending).rejects.toBeInstanceOf(GatewayBootstrapError);
    }

    const stalled = new PassThrough();
    await expect(readGatewayBootstrap(stalled, 10)).rejects.toThrow(
      "GATEWAY_BOOTSTRAP_TIMEOUT",
    );
  });

  it("compares a full capability without accepting type or length drift", () => {
    expect(
      gatewayCapabilityMatches(bootstrap.capability, bootstrap.capability),
    ).toBe(true);
    expect(gatewayCapabilityMatches("b".repeat(63), bootstrap.capability)).toBe(
      false,
    );
    expect(
      gatewayCapabilityMatches([bootstrap.capability], bootstrap.capability),
    ).toBe(false);
    expect(gatewayCapabilityMatches("g".repeat(64), bootstrap.capability)).toBe(
      false,
    );
    expect(
      gatewayCapabilityMatches("\u00e9".repeat(64), bootstrap.capability),
    ).toBe(false);
  });
});
