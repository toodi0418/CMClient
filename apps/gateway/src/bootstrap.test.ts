import { createHmac } from "node:crypto";
import { createServer } from "node:http";
import { connect } from "node:net";
import { PassThrough } from "node:stream";

import { describe, expect, it } from "vitest";

import {
  GATEWAY_CAPABILITY_HEADER,
  GatewayBootstrapError,
  encodePrivateFrame,
  gatewayCapabilityMatches,
  GATEWAY_PRIVATE_FRAME_MAX_BYTES,
  GATEWAY_OWNERSHIP_CHALLENGE_HEADER,
  GATEWAY_OWNERSHIP_PATH,
  GATEWAY_OWNERSHIP_PROOF_HEADER,
  readGatewayBootstrap,
  registerGatewayOwnershipProofEndpoint,
  startSupervisedGateway,
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
    await writeGatewayReady(
      output,
      bootstrap,
      { host: "127.0.0.1", port: 49152 },
      42,
    );
    expect(output.writableEnded).toBe(true);
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

  it("finishes the private ready channel before slow external startup", async () => {
    const output = new PassThrough();
    const chunks: Buffer[] = [];
    output.on("data", (chunk: Buffer) => chunks.push(chunk));
    let releaseExternal = (): void => undefined;
    const externalGate = new Promise<void>((resolve) => {
      releaseExternal = resolve;
    });
    let markExternalStarted = (): void => undefined;
    const externalStartedSignal = new Promise<void>((resolve) => {
      markExternalStarted = resolve;
    });
    let externalStarted = false;
    let startupSettled = false;

    const startup = startSupervisedGateway(
      output,
      bootstrap,
      async () => ({ host: "127.0.0.1", port: 49153 }),
      async () => {
        externalStarted = true;
        markExternalStarted();
        await externalGate;
      },
    ).finally(() => {
      startupSettled = true;
    });

    await new Promise<void>((resolve) => output.once("finish", resolve));
    await externalStartedSignal;
    expect(output.writableEnded).toBe(true);
    expect(externalStarted).toBe(true);
    expect(startupSettled).toBe(false);
    const ready = Buffer.concat(chunks);
    expect(JSON.parse(ready.subarray(4).toString("utf8"))).toMatchObject({
      type: "gateway.ready",
      startupNonce: bootstrap.startupNonce,
      host: "127.0.0.1",
      port: 49153,
    });

    releaseExternal();
    await expect(startup).resolves.toBeUndefined();
  });

  it("proves ownership without disclosing the bootstrap capability", async () => {
    const server = createServer((_request, response) => {
      response.writeHead(404).end();
    });
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", () => resolve());
    });
    const address = server.address();
    if (address === null || typeof address === "string") {
      throw new Error("test server address unavailable");
    }
    registerGatewayOwnershipProofEndpoint(
      server,
      bootstrap,
      { host: "127.0.0.1", port: address.port },
      42,
    );
    const challenge = "c".repeat(64);
    const request =
      `GET ${GATEWAY_OWNERSHIP_PATH} HTTP/1.1\r\n` +
      `Host: 127.0.0.1:${address.port}\r\n` +
      "Connection: Upgrade\r\n" +
      "Upgrade: cmclient-bootstrap-ownership-v1\r\n" +
      `${GATEWAY_OWNERSHIP_CHALLENGE_HEADER}: ${challenge}\r\n` +
      "Content-Length: 0\r\n\r\n";

    const response = await sendRawRequest(address.port, request);
    const expectedProof = createHmac("sha256", bootstrap.capability)
      .update(
        `cmclient.gateway.bootstrap-ownership.v1\n${bootstrap.startupNonce}\n42\n127.0.0.1\n${address.port}\n${challenge}`,
      )
      .digest("hex");
    expect(response).toBe(
      `HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 0\r\n${GATEWAY_OWNERSHIP_PROOF_HEADER}: ${expectedProof}\r\n\r\n`,
    );
    expect(request).not.toContain(bootstrap.capability);
    expect(response).not.toContain(bootstrap.capability);

    for (const invalidRequest of [
      request.replace("HTTP/1.1", "HTTP/1.0"),
      request.replace(
        "Content-Length: 0\r\n",
        `${GATEWAY_CAPABILITY_HEADER}: ${bootstrap.capability}\r\nContent-Length: 0\r\n`,
      ),
    ]) {
      await expect(sendRawRequest(address.port, invalidRequest)).resolves.toBe(
        "HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n",
      );
    }

    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
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

    const hugeChunk = new PassThrough();
    const hugeChunkPending = readGatewayBootstrap(hugeChunk, 100);
    hugeChunk.write(Buffer.from([0, 0, 0]));
    hugeChunk.write(Buffer.alloc(GATEWAY_PRIVATE_FRAME_MAX_BYTES * 16));
    await expect(hugeChunkPending).rejects.toThrow(
      "GATEWAY_PRIVATE_FRAME_OVERSIZED",
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

async function sendRawRequest(port: number, request: string): Promise<string> {
  return await new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    const socket = connect(port, "127.0.0.1");
    socket.setTimeout(2_000, () => socket.destroy(new Error("test timeout")));
    socket.on("connect", () => socket.write(request, "ascii"));
    socket.on("data", (chunk: Buffer) => chunks.push(chunk));
    socket.on("end", () => resolve(Buffer.concat(chunks).toString("ascii")));
    socket.on("error", reject);
  });
}
