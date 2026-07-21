import { timingSafeEqual } from "node:crypto";
import type { Readable, Writable } from "node:stream";

export const GATEWAY_CAPABILITY_HEADER = "x-cmclient-gateway-capability";
export const GATEWAY_PRIVATE_FRAME_MAX_BYTES = 4096;
export const GATEWAY_BOOTSTRAP_DEADLINE_MS = 5000;

export interface GatewayBootstrapFrame {
  schemaVersion: 1;
  type: "gateway.bootstrap";
  startupNonce: string;
  capability: string;
}

export interface GatewayReadyFrame {
  schemaVersion: 1;
  type: "gateway.ready";
  pid: number;
  startupNonce: string;
  host: "127.0.0.1";
  port: number;
}

export class GatewayBootstrapError extends Error {
  constructor(readonly code: string) {
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
  if (
    !isExactObject(value, [
      "capability",
      "schemaVersion",
      "startupNonce",
      "type",
    ])
  ) {
    throw new GatewayBootstrapError("GATEWAY_BOOTSTRAP_FRAME_INVALID");
  }
  if (
    value.schemaVersion !== 1 ||
    value.type !== "gateway.bootstrap" ||
    !isLowerHex(value.startupNonce, 32) ||
    !isLowerHex(value.capability, 64)
  ) {
    throw new GatewayBootstrapError("GATEWAY_BOOTSTRAP_FRAME_INVALID");
  }
  return value as unknown as GatewayBootstrapFrame;
}

export function writeGatewayReady(
  output: Writable,
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
  output.write(encodePrivateFrame(frame));
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
