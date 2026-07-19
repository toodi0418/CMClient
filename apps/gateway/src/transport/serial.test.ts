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
    const sessionConnectedAt: string[] = [];
    transport.subscribe((event) => {
      if (event.kind === "frame" && event.sessionConnectedAt) {
        sessionConnectedAt.push(event.sessionConnectedAt);
      }
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
    expect(sessionConnectedAt).toHaveLength(1);
    expect(sessionConnectedAt[0]).toMatch(
      /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/,
    );
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

  it("rejects a pending connect even when closing the serial device fails", async () => {
    const connection = new FakeSerialConnection(1);
    const transport = new SerialMeshtasticTransport({
      adapter: new FakeSerialAdapter(connection, []),
      path: "/dev/cu.fixture",
      configSession: codec,
      random: () => 0,
    });
    const connecting = transport.connect().catch((error: unknown) => error);
    await waitFor(() => connection.writes.length === 1);

    await expect(transport.disconnect()).rejects.toMatchObject({
      code: "SERIAL_DISCONNECT_FAILED",
    });
    await expect(connecting).resolves.toMatchObject({
      code: "TRANSPORT_DISCONNECTED",
    });
    expect(transport.state.status).toBe("configuring");

    await expect(transport.disconnect()).resolves.toBeUndefined();
    expect(connection.closeAttempts).toBe(2);
    expect(transport.state.status).toBe("disconnected");
  });

  it("times out a deferred open and closes its late handle before retrying", async () => {
    const adapter = new DeferredSerialAdapter();
    const transport = new SerialMeshtasticTransport({
      adapter,
      path: "/dev/cu.deferred",
      configSession: codec,
      openTimeoutMs: 10,
      reconnect: {
        initialDelayMs: 1_000,
        maximumDelayMs: 1_000,
        jitterRatio: 0,
      },
      random: () => 0,
    });
    const errors: string[] = [];
    transport.subscribe((event) => {
      if (event.kind === "error") {
        errors.push(event.code);
      }
    });
    const connecting = transport.connect().catch((error: unknown) => error);

    await waitFor(() => transport.state.status === "backoff");
    expect(transport.state).toMatchObject({
      status: "backoff",
      reasonCode: "SERIAL_OPEN_TIMEOUT",
    });
    expect(errors).toContain("SERIAL_OPEN_TIMEOUT");
    expect(adapter.openAttempts).toBe(1);
    expect(adapter.signal?.aborted).toBe(true);

    const lateConnection = new FakeSerialConnection(1);
    adapter.release(lateConnection);
    await waitFor(() => lateConnection.closed);
    expect(lateConnection.closeAttempts).toBe(2);
    expect(adapter.openAttempts).toBe(1);

    await transport.disconnect();
    await expect(connecting).resolves.toMatchObject({
      code: "TRANSPORT_DISCONNECTED",
    });
  });

  it("fails closed without waiting for an adapter that ignores open cancellation", async () => {
    const adapter = new DeferredSerialAdapter();
    const transport = new SerialMeshtasticTransport({
      adapter,
      path: "/dev/cu.stalled",
      configSession: codec,
      openTimeoutMs: 10,
      reconnect: {
        initialDelayMs: 1_000,
        maximumDelayMs: 1_000,
        jitterRatio: 0,
      },
      random: () => 0,
    });
    const connecting = transport.connect().catch((error: unknown) => error);

    await waitFor(() => transport.state.status === "backoff");
    await expect(transport.disconnect()).rejects.toMatchObject({
      code: "SERIAL_DISCONNECT_PENDING_OPEN",
    });

    expect(adapter.openAttempts).toBe(1);
    expect(adapter.signal?.aborted).toBe(true);
    expect(transport.state.status).toBe("backoff");
    await expect(connecting).resolves.toMatchObject({
      code: "TRANSPORT_DISCONNECTED",
    });

    const lateConnection = new FakeSerialConnection();
    adapter.release(lateConnection);
    await waitFor(() => lateConnection.closed);
    await transport.disconnect();
    expect(transport.state.status).toBe("disconnected");
  });

  it("keeps one active adapter open across repeated timeout backoff", async () => {
    const adapter = new AbortableSerialAdapter();
    const transport = new SerialMeshtasticTransport({
      adapter,
      path: "/dev/cu.blackhole",
      configSession: codec,
      openTimeoutMs: 5,
      reconnect: {
        initialDelayMs: 1,
        maximumDelayMs: 1,
        jitterRatio: 0,
      },
      random: () => 0,
    });
    const connecting = transport.connect().catch((error: unknown) => error);

    await waitFor(() => adapter.openAttempts >= 5);
    expect(adapter.maximumActiveOpens).toBe(1);
    expect(adapter.activeOpens).toBe(1);
    await expect(transport.disconnect()).rejects.toMatchObject({
      code: "SERIAL_DISCONNECT_PENDING_OPEN",
    });
    await waitFor(() => adapter.activeOpens === 0);
    await waitFor(() => adapter.openAttempts >= 5);
    await transport.disconnect();
    const stoppedAttempts = adapter.openAttempts;
    await new Promise((resolve) => setTimeout(resolve, 20));

    expect(adapter.openAttempts).toBe(stoppedAttempts);
    expect(transport.state.status).toBe("disconnected");
    await expect(connecting).resolves.toMatchObject({
      code: "TRANSPORT_DISCONNECTED",
    });
  });

  it("contains detached close failures and allows disconnect to retry", async () => {
    const connection = new FakeSerialConnection(2);
    const transport = new SerialMeshtasticTransport({
      adapter: new FakeSerialAdapter(connection, []),
      path: "/dev/cu.fixture",
      configSession: codec,
      random: () => 0,
    });
    const errors: string[] = [];
    transport.subscribe((event) => {
      if (event.kind === "error") {
        errors.push(event.code);
      }
    });
    const connecting = transport.connect().catch((error: unknown) => error);
    await waitFor(() => connection.writes.length === 1);

    connection.emitError();

    await waitFor(() => errors.includes("SERIAL_CLOSE_FAILED"));
    expect(connection.closeAttempts).toBe(2);
    expect(connection.closed).toBe(false);
    expect(errors).toEqual(["SERIAL_IO_FAILED", "SERIAL_CLOSE_FAILED"]);

    await transport.disconnect();
    expect(connection.closeAttempts).toBe(3);
    expect(connection.closed).toBe(true);
    await expect(connecting).resolves.toMatchObject({
      code: "TRANSPORT_DISCONNECTED",
    });
  });

  it("rejects serial deadlines above the bounded maximum", () => {
    expect(
      () =>
        new SerialMeshtasticTransport({
          adapter: new FakeSerialAdapter(new FakeSerialConnection(), []),
          path: "/dev/cu.fixture",
          configSession: codec,
          openTimeoutMs: 120_001,
        }),
    ).toThrowError(/SERIAL_CONFIGURATION_INVALID/);
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

class DeferredSerialAdapter implements SerialPortAdapter {
  private resolveOpen: ((connection: SerialConnection) => void) | undefined;
  openAttempts = 0;
  signal: AbortSignal | undefined;

  list(): Promise<SerialDevice[]> {
    return Promise.resolve([]);
  }

  open(options: {
    path: string;
    baudRate: number;
    signal?: AbortSignal;
  }): Promise<SerialConnection> {
    this.openAttempts += 1;
    this.signal = options.signal;
    return new Promise((resolve) => {
      this.resolveOpen = resolve;
    });
  }

  release(connection: SerialConnection): void {
    this.resolveOpen?.(connection);
    this.resolveOpen = undefined;
  }
}

class AbortableSerialAdapter implements SerialPortAdapter {
  activeOpens = 0;
  maximumActiveOpens = 0;
  openAttempts = 0;

  list(): Promise<SerialDevice[]> {
    return Promise.resolve([]);
  }

  open(options: {
    path: string;
    baudRate: number;
    signal?: AbortSignal;
  }): Promise<SerialConnection> {
    this.openAttempts += 1;
    this.activeOpens += 1;
    this.maximumActiveOpens = Math.max(
      this.maximumActiveOpens,
      this.activeOpens,
    );
    return new Promise((_resolve, reject) => {
      options.signal?.addEventListener(
        "abort",
        () => {
          this.activeOpens -= 1;
          reject(new Error("fixture open aborted"));
        },
        { once: true },
      );
    });
  }
}

class FakeSerialConnection implements SerialConnection {
  readonly writes: Uint8Array[] = [];
  closed = false;
  closeAttempts = 0;
  private readonly closeListeners: Array<() => void> = [];
  private readonly dataListeners: Array<(chunk: Uint8Array) => void> = [];
  private readonly errorListeners: Array<() => void> = [];

  constructor(private closeFailures = 0) {}

  close(): Promise<void> {
    this.closeAttempts += 1;
    if (this.closeFailures > 0) {
      this.closeFailures -= 1;
      return Promise.reject(new Error("fixture close failure"));
    }
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

  emitError(): void {
    for (const listener of this.errorListeners) {
      listener();
    }
  }
}

function decodeWrites(writes: Uint8Array[]): Uint8Array[] {
  const decoder = new MeshtasticFrameDecoder();
  return writes.flatMap((write) => decoder.push(write));
}

async function waitFor(predicate: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (predicate()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 1));
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
