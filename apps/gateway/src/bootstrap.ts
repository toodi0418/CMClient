import { createHmac, timingSafeEqual } from "node:crypto";
import type { Server } from "node:http";
import type { Duplex } from "node:stream";
import type { Readable, Writable } from "node:stream";

import {
  CMCLOUD_AUTHORITY_REQUIRED,
  type GatewayBootstrapErrorCode,
} from "./error-codes.js";

export const GATEWAY_CAPABILITY_HEADER = "x-cmclient-gateway-capability";
export const GATEWAY_PRIVATE_FRAME_MAX_BYTES = 16 * 1024;
export const GATEWAY_CALLMESH_API_KEY_MAX_BYTES = 4096;
export const GATEWAY_CMCLOUD_DEVICE_CREDENTIAL_MAX_BYTES = 512;
export const GATEWAY_BOOTSTRAP_DEADLINE_MS = 5000;
export const GATEWAY_OWNERSHIP_PATH = "/_cmclient/bootstrap/ownership";
export const GATEWAY_OWNERSHIP_CHALLENGE_HEADER =
  "x-cmclient-gateway-ownership-challenge";
export const GATEWAY_OWNERSHIP_PROOF_HEADER =
  "x-cmclient-gateway-ownership-proof";
const GATEWAY_OWNERSHIP_PROTOCOL = "cmclient-bootstrap-ownership-v1";
const GATEWAY_OWNERSHIP_DOMAIN = "cmclient.gateway.bootstrap-ownership.v1";

export interface GatewayBootstrapFrame {
  readonly schemaVersion: 2;
  readonly type: "gateway.bootstrap";
  readonly startupNonce: string;
  readonly capability: string;
  readonly setupGeneration: number;
  readonly callMeshApiKey?: string;
  readonly cmCloudDeviceCredential?: string;
}

export interface GatewayReadyFrame {
  schemaVersion: 1;
  type: "gateway.ready";
  pid: number;
  startupNonce: string;
  host: "127.0.0.1";
  port: number;
}

export interface GatewayBootstrapFailureFrame {
  readonly schemaVersion: 1;
  readonly type: "gateway.bootstrap.failed";
  readonly startupNonce: string;
  readonly code:
    | typeof CMCLOUD_AUTHORITY_REQUIRED
    | "CALLMESH_CREDENTIAL_REJECTED"
    | "CALLMESH_UNAVAILABLE"
    | "SETUP_MESHTASTIC_UNREACHABLE"
    | "GATEWAY_EXTERNAL_START_FAILED";
}

export class GatewayBootstrapError extends Error {
  constructor(readonly code: GatewayBootstrapErrorCode) {
    super(code);
  }
}

export function encodePrivateFrame(value: unknown): Buffer {
  const body = Buffer.from(JSON.stringify(value), "utf8");
  if (body.length === 0 || body.length > GATEWAY_PRIVATE_FRAME_MAX_BYTES) {
    throw new GatewayBootstrapError("GATEWAY_PRIVATE_FRAME_OVERSIZED");
  }
  const frame = Buffer.allocUnsafe(4 + body.length);
  frame.writeUInt32BE(body.length, 0);
  body.copy(frame, 4);
  return frame;
}

export async function readGatewayBootstrap(
  input: Readable,
  deadlineMs = GATEWAY_BOOTSTRAP_DEADLINE_MS,
): Promise<GatewayBootstrapFrame> {
  const value = await readPrivateFrame(input, deadlineMs);
  const exactKeys = [
    "capability",
    "schemaVersion",
    "setupGeneration",
    "startupNonce",
    "type",
  ];
  if (
    value !== null &&
    typeof value === "object" &&
    !Array.isArray(value) &&
    Object.hasOwn(value, "callMeshApiKey")
  ) {
    exactKeys.push("callMeshApiKey");
  }
  if (
    value !== null &&
    typeof value === "object" &&
    !Array.isArray(value) &&
    Object.hasOwn(value, "cmCloudDeviceCredential")
  ) {
    exactKeys.push("cmCloudDeviceCredential");
  }
  if (!isExactObject(value, exactKeys)) {
    throw new GatewayBootstrapError("GATEWAY_BOOTSTRAP_FRAME_INVALID");
  }
  if (
    value.schemaVersion !== 2 ||
    value.type !== "gateway.bootstrap" ||
    !isLowerHex(value.startupNonce, 32) ||
    !isLowerHex(value.capability, 64) ||
    !Number.isSafeInteger(value.setupGeneration) ||
    (value.setupGeneration as number) < 1 ||
    (value.callMeshApiKey !== undefined &&
      !isCallMeshApiKey(value.callMeshApiKey)) ||
    (value.cmCloudDeviceCredential !== undefined &&
      !isCmCloudDeviceCredential(value.cmCloudDeviceCredential))
  ) {
    throw new GatewayBootstrapError("GATEWAY_BOOTSTRAP_FRAME_INVALID");
  }
  return value as unknown as GatewayBootstrapFrame;
}

export async function writeGatewayReady(
  output: Writable,
  bootstrap: GatewayBootstrapFrame,
  address: { host: string; port: number },
  pid = process.pid,
): Promise<void> {
  if (
    address.host !== "127.0.0.1" ||
    !Number.isInteger(address.port) ||
    address.port < 1 ||
    address.port > 65_535 ||
    !Number.isInteger(pid) ||
    pid < 1
  ) {
    throw new GatewayBootstrapError("GATEWAY_READY_FRAME_INVALID");
  }
  const frame: GatewayReadyFrame = {
    schemaVersion: 1,
    type: "gateway.ready",
    pid,
    startupNonce: bootstrap.startupNonce,
    host: "127.0.0.1",
    port: address.port,
  };
  await endPrivateOutput(output, encodePrivateFrame(frame));
}

export async function writeGatewayBootstrapFailure(
  output: Writable,
  bootstrap: GatewayBootstrapFrame,
  code: GatewayBootstrapFailureFrame["code"],
): Promise<void> {
  await endPrivateOutput(
    output,
    encodePrivateFrame({
      schemaVersion: 1,
      type: "gateway.bootstrap.failed",
      startupNonce: bootstrap.startupNonce,
      code,
    } satisfies GatewayBootstrapFailureFrame),
  );
}

export async function startSupervisedGateway(
  output: Writable,
  bootstrap: GatewayBootstrapFrame,
  bindControlPlane: () => Promise<{ host: string; port: number }>,
  startExternalRuntimes: () => Promise<void>,
): Promise<void> {
  const address = await bindControlPlane();
  try {
    await startExternalRuntimes();
  } catch (error) {
    const code =
      error instanceof GatewayBootstrapError &&
      (error.code === "CALLMESH_CREDENTIAL_REJECTED" ||
        error.code === "CALLMESH_UNAVAILABLE" ||
        error.code === "SETUP_MESHTASTIC_UNREACHABLE")
        ? error.code
        : "GATEWAY_EXTERNAL_START_FAILED";
    await writeGatewayBootstrapFailure(output, bootstrap, code);
    throw error;
  }
  await writeGatewayReady(output, bootstrap, address);
}

export function registerGatewayOwnershipProofEndpoint(
  server: Server,
  bootstrap: GatewayBootstrapFrame,
  address: { host: string; port: number },
  pid = process.pid,
): void {
  if (
    address.host !== "127.0.0.1" ||
    !Number.isInteger(address.port) ||
    address.port < 1 ||
    address.port > 65_535 ||
    !Number.isInteger(pid) ||
    pid < 1
  ) {
    throw new GatewayBootstrapError("GATEWAY_OWNERSHIP_ENDPOINT_INVALID");
  }
  server.on("upgrade", (request, socket, head) => {
    const challenge = exactRawHeader(
      request.rawHeaders,
      GATEWAY_OWNERSHIP_CHALLENGE_HEADER,
    );
    if (
      request.method !== "GET" ||
      request.httpVersion !== "1.1" ||
      request.url !== GATEWAY_OWNERSHIP_PATH ||
      hasRawHeader(request.rawHeaders, GATEWAY_CAPABILITY_HEADER) ||
      exactRawHeader(request.rawHeaders, "host") !==
        `127.0.0.1:${address.port}` ||
      exactRawHeader(request.rawHeaders, "connection")?.toLowerCase() !==
        "upgrade" ||
      exactRawHeader(request.rawHeaders, "upgrade")?.toLowerCase() !==
        GATEWAY_OWNERSHIP_PROTOCOL ||
      exactRawHeader(request.rawHeaders, "content-length") !== "0" ||
      request.headers["transfer-encoding"] !== undefined ||
      !isLowerHex(challenge, 64) ||
      head.length !== 0
    ) {
      rejectOwnershipProbe(socket);
      return;
    }

    const proof = createHmac("sha256", bootstrap.capability)
      .update(
        gatewayOwnershipTranscript(
          bootstrap.startupNonce,
          pid,
          address.host,
          address.port,
          challenge,
        ),
      )
      .digest("hex");
    socket.end(
      `HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 0\r\n${GATEWAY_OWNERSHIP_PROOF_HEADER}: ${proof}\r\n\r\n`,
      "ascii",
    );
  });
}

export function gatewayCapabilityMatches(
  provided: unknown,
  expected: string,
): boolean {
  if (
    !isGatewayCapability(provided) ||
    !isGatewayCapability(expected) ||
    provided.length !== expected.length
  ) {
    return false;
  }
  return timingSafeEqual(Buffer.from(provided), Buffer.from(expected));
}

export function isGatewayCapability(value: unknown): value is string {
  return isLowerHex(value, 64);
}

async function readPrivateFrame(
  input: Readable,
  deadlineMs: number,
): Promise<unknown> {
  if (!Number.isInteger(deadlineMs) || deadlineMs < 1 || deadlineMs > 30_000) {
    throw new GatewayBootstrapError("GATEWAY_BOOTSTRAP_DEADLINE_INVALID");
  }
  return await new Promise((resolve, reject) => {
    let buffered = Buffer.alloc(0);
    let expectedLength: number | undefined;
    let settled = false;
    const timer = setTimeout(
      () => finish(new GatewayBootstrapError("GATEWAY_BOOTSTRAP_TIMEOUT")),
      deadlineMs,
    );
    timer.unref();

    const onData = (chunk: Buffer | string): void => {
      const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      if (
        bytes.length >
        GATEWAY_PRIVATE_FRAME_MAX_BYTES + 4 - buffered.length
      ) {
        finish(new GatewayBootstrapError("GATEWAY_PRIVATE_FRAME_OVERSIZED"));
        return;
      }
      buffered = Buffer.concat([buffered, bytes]);
      if (buffered.length >= 4 && expectedLength === undefined) {
        expectedLength = buffered.readUInt32BE(0);
        if (
          expectedLength < 1 ||
          expectedLength > GATEWAY_PRIVATE_FRAME_MAX_BYTES
        ) {
          finish(new GatewayBootstrapError("GATEWAY_PRIVATE_FRAME_OVERSIZED"));
          return;
        }
      }
      if (
        expectedLength === undefined ||
        buffered.length < expectedLength + 4
      ) {
        return;
      }
      if (buffered.length !== expectedLength + 4) {
        finish(
          new GatewayBootstrapError("GATEWAY_PRIVATE_FRAME_TRAILING_DATA"),
        );
        return;
      }
      let parsed: unknown;
      try {
        parsed = JSON.parse(buffered.subarray(4).toString("utf8"));
      } catch {
        finish(new GatewayBootstrapError("GATEWAY_PRIVATE_FRAME_INVALID"));
        return;
      }
      finish(undefined, parsed);
    };
    const onEnd = (): void =>
      finish(new GatewayBootstrapError("GATEWAY_BOOTSTRAP_EARLY_EOF"));
    const onError = (): void =>
      finish(new GatewayBootstrapError("GATEWAY_BOOTSTRAP_PIPE_FAILED"));

    function finish(error?: Error, value?: unknown): void {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      input.off("data", onData);
      input.off("end", onEnd);
      input.off("error", onError);
      input.pause();
      if (error) reject(error);
      else resolve(value);
    }

    input.on("data", onData);
    input.once("end", onEnd);
    input.once("error", onError);
    input.resume();
  });
}

function gatewayOwnershipTranscript(
  startupNonce: string,
  pid: number,
  host: string,
  port: number,
  challenge: string,
): string {
  return `${GATEWAY_OWNERSHIP_DOMAIN}\n${startupNonce}\n${pid}\n${host}\n${port}\n${challenge}`;
}

function exactRawHeader(
  rawHeaders: readonly string[],
  expectedName: string,
): string | undefined {
  const values: string[] = [];
  for (let index = 0; index < rawHeaders.length; index += 2) {
    if (rawHeaders[index]?.toLowerCase() === expectedName.toLowerCase()) {
      values.push(rawHeaders[index + 1] ?? "");
    }
  }
  return values.length === 1 ? values[0] : undefined;
}

function hasRawHeader(
  rawHeaders: readonly string[],
  expectedName: string,
): boolean {
  for (let index = 0; index < rawHeaders.length; index += 2) {
    if (rawHeaders[index]?.toLowerCase() === expectedName.toLowerCase()) {
      return true;
    }
  }
  return false;
}

function rejectOwnershipProbe(socket: Duplex): void {
  socket.end(
    "HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n",
    "ascii",
  );
}

async function endPrivateOutput(
  output: Writable,
  frame: Buffer,
): Promise<void> {
  if (output.destroyed || output.writableEnded) {
    throw new GatewayBootstrapError("GATEWAY_READY_PIPE_FAILED");
  }
  await new Promise<void>((resolve, reject) => {
    let settled = false;
    const finish = (error?: Error): void => {
      if (settled) return;
      settled = true;
      output.off("error", onError);
      output.off("finish", onFinish);
      if (error) reject(error);
      else resolve();
    };
    const onError = (): void =>
      finish(new GatewayBootstrapError("GATEWAY_READY_PIPE_FAILED"));
    const onFinish = (): void => finish();
    output.once("error", onError);
    output.once("finish", onFinish);
    try {
      output.end(frame);
    } catch {
      onError();
    }
  });
}

function isExactObject(
  value: unknown,
  keys: readonly string[],
): value is Record<string, unknown> {
  return (
    value !== null &&
    typeof value === "object" &&
    !Array.isArray(value) &&
    JSON.stringify(Object.keys(value).sort()) ===
      JSON.stringify([...keys].sort())
  );
}

function isLowerHex(value: unknown, length: number): value is string {
  return (
    typeof value === "string" &&
    value.length === length &&
    /^[0-9a-f]+$/.test(value)
  );
}

function isCallMeshApiKey(value: unknown): value is string {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    Buffer.byteLength(value, "utf8") > GATEWAY_CALLMESH_API_KEY_MAX_BYTES
  ) {
    return false;
  }
  for (let index = 0; index < value.length; index += 1) {
    const codeUnit = value.charCodeAt(index);
    if (codeUnit <= 0x1f || codeUnit === 0x7f) {
      return false;
    }
  }
  return true;
}

function isCmCloudDeviceCredential(value: unknown): value is string {
  return (
    typeof value === "string" &&
    value.length >= 16 &&
    value.length <= GATEWAY_CMCLOUD_DEVICE_CREDENTIAL_MAX_BYTES &&
    /^[A-Za-z0-9_-]+$/.test(value)
  );
}
