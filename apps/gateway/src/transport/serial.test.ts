import { describe, expect, it } from "vitest";

import type { SerialDevice } from "@cmclient/contracts";

import {
  type SerialConnection,
  type SerialPortAdapter,
  SerialMeshtasticTransport,
  listSerialDevices,
} from "./serial";
import { MeshtasticFrameDecoder, encodeMeshtasticFrame } from "./framing";

const codec = {
  encodeWantConfig(nonce: number): Uint8Array {
    return new Uint8Array([1, ...uint32(nonce)]);
  },
  isConfigComplete(payload: Uint8Array, nonce: number): boolean {
    return payload[0] === 2 && equals(payload.slice(1), uint32(nonce));
  },
};

describe("SerialMeshtasticTransport", () => {
  it("uses the shared frame/config boundary and drains outbound serial writes", async () => {
    const connection = new FakeSerialConnection();
    const adapter = new FakeSerialAdapter(connection, [
      { path: "/dev/cu.fixture-b", vendorId: "b" },
      { path: "/dev/cu.fixture-a", vendorId: "a" },
    ]);
    const transport = new SerialMeshtasticTransport({
      adapter,
      path: "/dev/cu.fixture-a",
      configSession: codec,
      random: () => 0,
    });
    const connecting = transport.connect();
    await waitFor(() => connection.writes.length === 1);
    connection.emitData(encodeMeshtasticFrame(new Uint8Array([2, 0, 0, 0, 1])));

    await connecting;
    expect(transport.state.status).toBe("ready");
    await transport.writeFrame(new Uint8Array([99]));
    expect(decodeWrites(connection.writes)).toEqual([
      new Uint8Array([1, 0, 0, 0, 1]),
      new Uint8Array([99]),
    ]);
    expect(await listSerialDevices(adapter)).toEqual([
      { path: "/dev/cu.fixture-b", vendorId: "b" },
      { path: "/dev/cu.fixture-a", vendorId: "a" },
    ]);

    await transport.disconnect();
    expect(connection.closed).toBe(true);
  });

  it("backs off when the adapter cannot open the selected device", async () => {
    const adapter = new FakeSerialAdapter(undefined, [], true);
    const transport = new SerialMeshtasticTransport({
      adapter,
      path: "/dev/cu.unavailable",
      configSession: codec,
      reconnect: { initialDelayMs: 100, maximumDelayMs: 100, jitterRatio: 0 },
      random: () => 0,
    });
    const backoff = new Promise<void>((resolve) => {
      transport.subscribe((event) => {
        if (event.kind === "state" && event.state.status === "backoff") {
          resolve();
        }
      });
    });
    const connecting = transport.connect().catch((error: unknown) => error);

    await backoff;
    expect(transport.state).toMatchObject({
      status: "backoff",
      reasonCode: "SERIAL_OPEN_FAILED",
    });
    await transport.disconnect();
    await expect(connecting).resolves.toMatchObject({
      code: "TRANSPORT_DISCONNECTED",
    });
  });
});

class FakeSerialAdapter implements SerialPortAdapter {
  constructor(
    private readonly connection: FakeSerialConnection | undefined,
    private readonly devices: SerialDevice[],
    private readonly failOpen = false,
  ) {}

  list(): Promise<SerialDevice[]> {
    return Promise.resolve(this.devices);
  }

  open(): Promise<SerialConnection> {
    if (this.failOpen || !this.connection) {
      return Promise.reject(new Error("fixture open failure"));
    }
    return Promise.resolve(this.connection);
  }
}

class FakeSerialConnection implements SerialConnection {
  readonly writes: Uint8Array[] = [];
  closed = false;
  private readonly closeListeners: Array<() => void> = [];
  private readonly dataListeners: Array<(chunk: Uint8Array) => void> = [];
  private readonly errorListeners: Array<() => void> = [];

  close(): Promise<void> {
    this.closed = true;
    for (const listener of this.closeListeners) {
      listener();
    }
    return Promise.resolve();
  }

  onClose(listener: () => void): void {
    this.closeListeners.push(listener);
  }

  onData(listener: (chunk: Uint8Array) => void): void {
    this.dataListeners.push(listener);
  }

  onError(listener: () => void): void {
    this.errorListeners.push(listener);
  }

  write(frame: Uint8Array): Promise<void> {
    this.writes.push(frame);
    return Promise.resolve();
  }

  emitData(chunk: Uint8Array): void {
    for (const listener of this.dataListeners) {
      listener(chunk);
    }
  }
}

function decodeWrites(writes: Uint8Array[]): Uint8Array[] {
  const decoder = new MeshtasticFrameDecoder();
  return writes.flatMap((write) => decoder.push(write));
}

async function waitFor(predicate: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 20; attempt += 1) {
    if (predicate()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
  throw new Error("fixture condition was not reached");
}

function uint32(value: number): Uint8Array {
  return new Uint8Array([
    (value >>> 24) & 0xff,
    (value >>> 16) & 0xff,
    (value >>> 8) & 0xff,
    value & 0xff,
  ]);
}

function equals(left: Uint8Array, right: Uint8Array): boolean {
  return (
    left.length === right.length &&
    left.every((value, index) => value === right[index])
  );
}
