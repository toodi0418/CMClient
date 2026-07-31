import { createHmac } from "node:crypto";
import { createServer } from "node:http";
import { connect } from "node:net";
import { PassThrough } from "node:stream";

import { describe, expect, it } from "vitest";

import {
  GATEWAY_CAPABILITY_HEADER,
  GATEWAY_CALLMESH_API_KEY_MAX_BYTES,
  GATEWAY_CMCLOUD_DEVICE_CREDENTIAL_MAX_BYTES,
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
  schemaVersion: 2 as const,
  type: "gateway.bootstrap" as const,
  startupNonce: "a".repeat(32),
  capability: "b".repeat(64),
  setupGeneration: 7,
  callMeshApiKey: " fixture-private-callmesh-key ",
};
const cloudBootstrap = {
  ...bootstrap,
  cmCloudDeviceCredential: "cmcloud_device_credential_value",
};

describe("private Gateway bootstrap", () => {
  it("reads a bounded fragmented frame and writes an exact ready frame", async () => {
    const input = new PassThrough();
    const encoded = encodePrivateFrame(cloudBootstrap);
    const pending = readGatewayBootstrap(input);
    input.write(encoded.subarray(0, 3));
    input.write(encoded.subarray(3, 11));
    input.write(encoded.subarray(11));
    await expect(pending).resolves.toEqual(cloudBootstrap);

    const withoutCallMeshKey = {
      schemaVersion: bootstrap.schemaVersion,
      type: bootstrap.type,
      startupNonce: bootstrap.startupNonce,
      capability: bootstrap.capability,
      setupGeneration: bootstrap.setupGeneration,
    };
    const inputWithoutKey = new PassThrough();
    const pendingWithoutKey = readGatewayBootstrap(inputWithoutKey);
    inputWithoutKey.end(encodePrivateFrame(withoutCallMeshKey));
    await expect(pendingWithoutKey).resolves.toEqual(withoutCallMeshKey);

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
    expect(ready.includes(Buffer.from(bootstrap.callMeshApiKey))).toBe(false);
    expect(
      ready.includes(Buffer.from(cloudBootstrap.cmCloudDeviceCredential)),
    ).toBe(false);
  });

  it("does not publish ready until external startup succeeds", async () => {
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

    await externalStartedSignal;
    expect(output.writableEnded).toBe(false);
    expect(externalStarted).toBe(true);
    expect(startupSettled).toBe(false);

    releaseExternal();
    await expect(startup).resolves.toBeUndefined();
    expect(output.writableEnded).toBe(true);
    const ready = Buffer.concat(chunks);
    expect(JSON.parse(ready.subarray(4).toString("utf8"))).toMatchObject({
      type: "gateway.ready",
      startupNonce: bootstrap.startupNonce,
      host: "127.0.0.1",
      port: 49153,
    });
  });

  it("reports stable CallMesh setup failures without exposing the key", async () => {
    const output = new PassThrough();
    const chunks: Buffer[] = [];
    output.on("data", (chunk: Buffer) => chunks.push(chunk));
    await expect(
      startSupervisedGateway(
        output,
        bootstrap,
        async () => ({ host: "127.0.0.1", port: 49154 }),
        async () => {
          throw new GatewayBootstrapError("CALLMESH_CREDENTIAL_REJECTED");
        },
      ),
    ).rejects.toThrow("CALLMESH_CREDENTIAL_REJECTED");
    const failure = Buffer.concat(chunks);
    expect(JSON.parse(failure.subarray(4).toString("utf8"))).toEqual({
      schemaVersion: 1,
      type: "gateway.bootstrap.failed",
      startupNonce: bootstrap.startupNonce,
      code: "CALLMESH_CREDENTIAL_REJECTED",
    });
    expect(failure.includes(Buffer.from(bootstrap.callMeshApiKey))).toBe(false);
  });

  it("reports Meshtastic protocol validation failure before readiness", async () => {
    const output = new PassThrough();
    const chunks: Buffer[] = [];
    output.on("data", (chunk: Buffer) => chunks.push(chunk));
    await expect(
      startSupervisedGateway(
        output,
        bootstrap,
        async () => ({ host: "127.0.0.1", port: 49155 }),
        async () => {
          throw new GatewayBootstrapError("SETUP_MESHTASTIC_UNREACHABLE");
        },
      ),
    ).rejects.toThrow("SETUP_MESHTASTIC_UNREACHABLE");
    expect(
      JSON.parse(Buffer.concat(chunks).subarray(4).toString("utf8")),
    ).toEqual({
      schemaVersion: 1,
      type: "gateway.bootstrap.failed",
      startupNonce: bootstrap.startupNonce,
      code: "SETUP_MESHTASTIC_UNREACHABLE",
    });
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
    expect(request).not.toContain(bootstrap.callMeshApiKey);
    expect(response).not.toContain(bootstrap.callMeshApiKey);

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
      encodePrivateFrame({ ...bootstrap, schemaVersion: 1 }),
      encodePrivateFrame({ ...bootstrap, capability: "not-a-capability" }),
      encodePrivateFrame({ ...bootstrap, setupGeneration: 0 }),
      encodePrivateFrame({ ...bootstrap, setupGeneration: 1.5 }),
      encodePrivateFrame({ ...bootstrap, setupGeneration: Number.MAX_VALUE }),
      encodePrivateFrame({ ...bootstrap, unknown: true }),
    ]) {
      const input = new PassThrough();
      const pending = readGatewayBootstrap(input, 100);
      input.write(frame);
      await expect(pending).rejects.toBeInstanceOf(GatewayBootstrapError);
    }

    for (const callMeshApiKey of [
      null,
      42,
      "",
      "control\ncharacter",
      "x".repeat(GATEWAY_CALLMESH_API_KEY_MAX_BYTES + 1),
      "\u00e9".repeat(GATEWAY_CALLMESH_API_KEY_MAX_BYTES / 2 + 1),
    ]) {
      const input = new PassThrough();
      const pending = readGatewayBootstrap(input, 100);
      input.end(encodePrivateFrame({ ...bootstrap, callMeshApiKey }));
      await expect(pending).rejects.toThrow("GATEWAY_BOOTSTRAP_FRAME_INVALID");
    }

    for (const cmCloudDeviceCredential of [
      null,
      42,
      "x".repeat(15),
      "not a device credential",
      "x".repeat(GATEWAY_CMCLOUD_DEVICE_CREDENTIAL_MAX_BYTES + 1),
    ]) {
      const input = new PassThrough();
      const pending = readGatewayBootstrap(input, 100);
      input.end(encodePrivateFrame({ ...bootstrap, cmCloudDeviceCredential }));
      await expect(pending).rejects.toThrow("GATEWAY_BOOTSTRAP_FRAME_INVALID");
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
