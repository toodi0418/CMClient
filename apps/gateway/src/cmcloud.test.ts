import { describe, expect, it } from "vitest";

import {
  CMCLOUD_AGENT_SUBPROTOCOL,
  CmCloudAgentClient,
  CmCloudRawOutboxRepository,
  encodeCmCloudRawFrame,
  parseCmCloudRuntimeConfiguration,
  type CmCloudSocket,
} from "./cmcloud";
import type {
  CmCloudDirectAprsCapability,
  CmCloudDirectAprsDispatchResult,
  CmCloudDirectAprsEgress,
} from "./cmcloud-aprs";
import { GatewayDatabase } from "./persistence/database";

const INSTALLATION_ID = "00000000-0000-4000-8000-000000000001";
const RECEIPT_ID = "00000000-0000-4000-8000-000000000002";
const BOOT_ID = "00000000-0000-4000-8000-000000000003";
const MESSAGE_ID = "00000000-0000-4000-8000-000000000004";
const DISPATCH_ID = "00000000-0000-4000-8000-000000000006";
const DEVICE_CREDENTIAL = "credential_value_that_is_long_enough";
const CAPTURED_AT = "2026-07-31T00:00:00.000Z";

describe("CMCloud raw outbox", () => {
  it("retains exact raw bytes and never reuses a lane sequence after retention", () => {
    const database = new GatewayDatabase(":memory:");
    const outbox = new CmCloudRawOutboxRepository(
      database.connection,
      () => MESSAGE_ID,
    );
    const body = new Uint8Array([0, 1, 0xff, 2]);
    const first = outbox.enqueue({ body, capturedAt: CAPTURED_AT });
    body[2] = 0;

    expect(first).toMatchObject({
      messageId: MESSAGE_ID,
      lane: "live",
      laneSequence: 1,
      body: new Uint8Array([0, 1, 0xff, 2]),
      attempts: 0,
    });
    const sent = encodeCmCloudRawFrame(
      {
        messageId: first.messageId,
        lane: first.lane,
        laneSequence: first.laneSequence,
        capturedAt: first.capturedAt,
        connectionEpoch: 7,
        installationGeneration: 0,
        credentialVersion: 1,
        wireKind: "meshtastic.FromRadio",
      },
      first.body,
    );
    const headerLength = sent.readUInt16BE(5);
    expect(sent.subarray(7 + headerLength)).toEqual(
      Buffer.from([0, 1, 0xff, 2]),
    );

    outbox.recordAttempt(first.messageId, CAPTURED_AT);
    outbox.acknowledge({
      messageId: first.messageId,
      lane: "live",
      laneSequence: 1,
      receiptId: RECEIPT_ID,
      acknowledgedAt: "2026-07-31T00:00:01.000Z",
    });
    expect(outbox.deleteAcknowledgedBefore("2026-07-31T00:00:02.000Z")).toBe(1);

    const second = new CmCloudRawOutboxRepository(
      database.connection,
      () => "00000000-0000-4000-8000-000000000005",
    ).enqueue({
      body: new Uint8Array([4]),
      capturedAt: "2026-07-31T00:00:03.000Z",
    });
    expect(second.laneSequence).toBe(2);
    database.close();
  });

  it("sends CMC1 only after server hello and removes a row only after raw_ack", async () => {
    const database = new GatewayDatabase(":memory:");
    const socket = new FixtureSocket();
    const client = createClient(database, () => socket);

    client.start();
    socket.open();
    await settle();
    expect(socket.sent).toEqual([
      expect.objectContaining({
        binary: false,
        data: expect.stringContaining('"type":"client_hello"'),
      }),
    ]);
    socket.message(serverHello(7));
    const queued = client.enqueueRawFrame(
      new Uint8Array([0, 9, 0xfe]),
      CAPTURED_AT,
    );
    await settle();

    const frame = socket.sent.find((entry) => entry.binary)?.data;
    expect(frame).toBeInstanceOf(Buffer);
    const decoded = decodeRawFrame(frame as Buffer);
    expect(decoded.header).toMatchObject({
      messageId: queued.messageId,
      lane: "live",
      laneSequence: queued.laneSequence,
      connectionEpoch: 7,
      wireKind: "meshtastic.FromRadio",
    });
    expect(decoded.body).toEqual(Buffer.from([0, 9, 0xfe]));
    expect(client.status()).toMatchObject({ state: "ready", pendingOutbox: 1 });

    socket.message({
      type: "raw_ack",
      messageId: queued.messageId,
      receiptId: RECEIPT_ID,
      lane: "live",
      laneSequence: queued.laneSequence,
      disposition: "received",
    });
    await settle();
    expect(client.status()).toMatchObject({ state: "ready", pendingOutbox: 0 });
    expect(database.cmcloudRawOutbox.find(queued.messageId)).toMatchObject({
      receiptId: RECEIPT_ID,
    });
    await client.stop();
    database.close();
  });

  it("settles the durable raw outbox when CMCloud drops an MQTT packet", async () => {
    const database = new GatewayDatabase(":memory:");
    const socket = new FixtureSocket();
    const client = createClient(database, () => socket);

    client.start();
    socket.open();
    socket.message(serverHello(7));
    const queued = client.enqueueRawFrame(
      new Uint8Array([0, 9, 0xfe]),
      CAPTURED_AT,
    );
    await settle();

    socket.message({
      type: "raw_ack",
      messageId: queued.messageId,
      receiptId: RECEIPT_ID,
      lane: "live",
      laneSequence: queued.laneSequence,
      disposition: "dropped_mqtt",
    });
    await settle();

    expect(client.status()).toMatchObject({ state: "ready", pendingOutbox: 0 });
    expect(database.cmcloudRawOutbox.find(queued.messageId)).toMatchObject({
      receiptId: RECEIPT_ID,
      acknowledgedAt: expect.any(String),
    });
    await client.stop();
    database.close();
  });

  it("replays the exact stored body with a new epoch after reconnect", async () => {
    const database = new GatewayDatabase(":memory:");
    const sockets: FixtureSocket[] = [];
    const client = createClient(database, () => {
      const socket = new FixtureSocket();
      sockets.push(socket);
      return socket;
    });

    client.start();
    const firstSocket = sockets[0];
    if (!firstSocket) throw new Error("first fixture socket missing");
    firstSocket.open();
    firstSocket.message(serverHello(7));
    const queued = client.enqueueRawFrame(
      new Uint8Array([8, 7, 6]),
      CAPTURED_AT,
    );
    await settle();
    const firstFrame = decodeRawFrame(
      firstSocket.binaryFrames()[0] ?? Buffer.alloc(0),
    );

    firstSocket.remoteClose();
    await delay(20);
    const secondSocket = sockets[1];
    if (!secondSocket) throw new Error("second fixture socket missing");
    secondSocket.open();
    secondSocket.message(serverHello(8));
    await settle();
    const replay = decodeRawFrame(
      secondSocket.binaryFrames()[0] ?? Buffer.alloc(0),
    );
    expect(replay.header).toMatchObject({
      messageId: queued.messageId,
      laneSequence: queued.laneSequence,
      connectionEpoch: 8,
    });
    expect(replay.body).toEqual(firstFrame.body);

    secondSocket.message({
      type: "raw_ack",
      messageId: queued.messageId,
      receiptId: RECEIPT_ID,
      lane: "live",
      laneSequence: queued.laneSequence,
      disposition: "duplicate",
    });
    await settle();
    expect(client.status().pendingOutbox).toBe(0);
    await client.stop();
    database.close();
  });

  it("requires an explicit Cloud mode and keeps its credential out of environment", () => {
    expect(parseCmCloudRuntimeConfiguration({})).toBeUndefined();
    expect(() =>
      parseCmCloudRuntimeConfiguration({
        CMCLIENT_CMCLOUD_DEVICE_CREDENTIAL: DEVICE_CREDENTIAL,
      }),
    ).toThrowError(
      expect.objectContaining({ code: "CMCLOUD_SECRET_ENVIRONMENT_FORBIDDEN" }),
    );
    expect(() =>
      parseCmCloudRuntimeConfiguration({
        CMCLIENT_CMCLOUD_MODE: "required",
        CMCLIENT_CMCLOUD_URL: "wss://cmcloud.tmmarc.org/agent/v1",
        CMCLIENT_CMCLOUD_INSTALLATION_ID: INSTALLATION_ID,
        CMCLIENT_CMCLOUD_INSTALLATION_GENERATION: "0",
        CMCLIENT_CMCLOUD_CREDENTIAL_VERSION: "1",
        CMCLIENT_CMCLOUD_DEVICE_CREDENTIAL: DEVICE_CREDENTIAL,
      }),
    ).toThrowError(
      expect.objectContaining({ code: "CMCLOUD_SECRET_ENVIRONMENT_FORBIDDEN" }),
    );
  });

  it("preserves a pre-hello mandatory-upgrade rejection as a terminal code", async () => {
    const database = new GatewayDatabase(":memory:");
    const socket = new FixtureSocket();
    const client = createClient(database, () => socket);

    client.start();
    socket.open();
    socket.message({
      type: "error",
      code: "CLIENT_UPGRADE_REQUIRED",
      message: "upgrade required",
    });
    await settle();

    expect(client.status()).toMatchObject({
      state: "blocked",
      terminalCode: "CLIENT_UPGRADE_REQUIRED",
    });
    await client.stop();
    database.close();
  });

  it("fails closed when pairing would retain an issued credential only in Node", async () => {
    const database = new GatewayDatabase(":memory:");
    const socket = new FixtureSocket();
    const client = createClient(database, () => socket);

    client.start();
    socket.open();
    socket.message({
      ...serverHello(7),
      enrollmentAckRequired: true,
      issuedDeviceCredential: DEVICE_CREDENTIAL,
    });
    await settle();

    expect(client.status()).toMatchObject({
      state: "blocked",
      terminalCode: "CMCLOUD_ENROLLMENT_REQUIRES_AGENT",
    });
    expect(
      socket.sent.some(
        (entry) =>
          typeof entry.data === "string" &&
          entry.data.includes("enrollment_ack"),
      ),
    ).toBe(false);
    await client.stop();
    database.close();
  });

  it("advertises direct APRS only after a verified capability is locally ready and acknowledges an exact dispatch once", async () => {
    const database = new GatewayDatabase(":memory:");
    const socket = new FixtureSocket();
    const egress = new FixtureDirectAprsEgress();
    const client = createClient(database, () => socket, egress);
    const data =
      "BM5GSV-5>APTMAG,MESHD*,qAR,BM3FFG-2:!2404.57N/12032.42Ek/A=000141";

    client.start();
    socket.open();
    socket.message(
      serverHello(7, {
        aprsMode: "enabled",
        directAprs: { callsign: "BM5GSV-5", verified: true },
      }),
    );
    await settle();

    expect(egress.capability).toEqual({
      callsign: "BM5GSV-5",
      verified: true,
    });
    expect(latestControl(socket, "client_heartbeat")).toMatchObject({
      directAprsReady: true,
    });

    socket.message({ type: "aprs_dispatch", dispatchId: DISPATCH_ID, data });
    await settle();
    expect(egress.submissions).toEqual([data]);
    expect(latestControl(socket, "aprs_dispatch_ack")).toEqual({
      type: "aprs_dispatch_ack",
      dispatchId: DISPATCH_ID,
      outcome: "submitted",
    });

    socket.message({ type: "aprs_dispatch", dispatchId: DISPATCH_ID, data });
    await settle();
    expect(egress.submissions).toEqual([data]);
    expect(latestControl(socket, "aprs_dispatch_ack")).toEqual({
      type: "aprs_dispatch_ack",
      dispatchId: DISPATCH_ID,
      outcome: "submitted",
    });

    await client.stop();
    database.close();
  });

  it("retries a retryable APRS dispatch with the same dispatch ID", async () => {
    const database = new GatewayDatabase(":memory:");
    const socket = new FixtureSocket();
    const egress = new FixtureDirectAprsEgress({
      outcome: "retryable_failure",
      errorCode: "CMCLOUD_DIRECT_APRS_NOT_READY",
    });
    const client = createClient(database, () => socket, egress);
    const data = "BM5GSV-5>APTMAG:!2404.57N/12032.42Ek";

    client.start();
    socket.open();
    socket.message(
      serverHello(7, {
        aprsMode: "enabled",
        directAprs: { callsign: "BM5GSV-5", verified: true },
      }),
    );
    await settle();

    socket.message({ type: "aprs_dispatch", dispatchId: DISPATCH_ID, data });
    await settle();
    socket.message({ type: "aprs_dispatch", dispatchId: DISPATCH_ID, data });
    await settle();

    expect(egress.submissions).toEqual([data, data]);
    expect(latestControl(socket, "aprs_dispatch_ack")).toEqual({
      type: "aprs_dispatch_ack",
      dispatchId: DISPATCH_ID,
      outcome: "retryable_failure",
      errorCode: "CMCLOUD_DIRECT_APRS_NOT_READY",
    });

    await client.stop();
    database.close();
  });

  it("keeps the CMCloud session alive while a non-enabled APRS policy disables direct egress", async () => {
    const database = new GatewayDatabase(":memory:");
    const socket = new FixtureSocket();
    const egress = new FixtureDirectAprsEgress();
    const client = createClient(database, () => socket, egress);

    client.start();
    socket.open();
    socket.message(
      serverHello(7, {
        aprsMode: "shadow",
        directAprs: { callsign: "BM5GSV-5", verified: true },
      }),
    );
    await settle();

    expect(client.status()).toMatchObject({ state: "ready" });
    expect(egress.capability).toBeUndefined();
    expect(latestControl(socket, "client_heartbeat")).toMatchObject({
      directAprsReady: false,
    });

    await client.stop();
    database.close();
  });

  it("synchronously fences direct APRS when a terminal CMCloud error starts a deferred socket close", async () => {
    const database = new GatewayDatabase(":memory:");
    const socket = new FixtureSocket(true);
    const egress = new FixtureDirectAprsEgress();
    const client = createClient(database, () => socket, egress);
    const data = "BM5GSV-5>APTMAG:!2404.57N/12032.42Ek";

    client.start();
    socket.open();
    socket.message(
      serverHello(7, {
        aprsMode: "enabled",
        directAprs: { callsign: "BM5GSV-5", verified: true },
      }),
    );
    await settle();
    expect(egress.capability).toEqual({
      callsign: "BM5GSV-5",
      verified: true,
    });

    socket.message({
      type: "error",
      code: "CLIENT_UPGRADE_REQUIRED",
      message: "upgrade required",
    });
    await settle();
    const sentBeforeDispatch = socket.sent.length;

    expect(client.status()).toMatchObject({
      state: "blocked",
      terminalCode: "CLIENT_UPGRADE_REQUIRED",
    });
    expect(egress.capability).toBeUndefined();

    socket.message({ type: "aprs_dispatch", dispatchId: DISPATCH_ID, data });
    await settle();
    expect(egress.submissions).toEqual([]);
    expect(socket.sent).toHaveLength(sentBeforeDispatch);

    await client.stop();
    database.close();
  });

  it("fails closed without a verified direct capability", async () => {
    const database = new GatewayDatabase(":memory:");
    const socket = new FixtureSocket();
    const egress = new FixtureDirectAprsEgress();
    const client = createClient(database, () => socket, egress);

    client.start();
    socket.open();
    socket.message(serverHello(7, { aprsMode: "enabled" }));
    await settle();
    expect(egress.capability).toBeUndefined();
    expect(latestControl(socket, "client_heartbeat")).toMatchObject({
      directAprsReady: false,
    });

    socket.message({
      type: "aprs_dispatch",
      dispatchId: DISPATCH_ID,
      data: "BM5GSV-5>APTMAG:!2404.57N/12032.42Ek",
    });
    await settle();
    expect(egress.submissions).toEqual([]);
    expect(latestControl(socket, "aprs_dispatch_ack")).toMatchObject({
      outcome: "retryable_failure",
      errorCode: "CMCLOUD_DIRECT_APRS_NOT_READY",
    });

    await client.stop();
    database.close();
  });

  it("forwards an uncertain APRS write once and does not synthesize a retry", async () => {
    const database = new GatewayDatabase(":memory:");
    const socket = new FixtureSocket();
    const egress = new FixtureDirectAprsEgress({
      outcome: "uncertain",
      errorCode: "CMCLOUD_DIRECT_APRS_WRITE_UNCERTAIN",
    });
    const client = createClient(database, () => socket, egress);
    const data = "BM5GSV-5>APTMAG:!2404.57N/12032.42Ek";

    client.start();
    socket.open();
    socket.message(
      serverHello(7, {
        aprsMode: "enabled",
        directAprs: { callsign: "BM5GSV-5", verified: true },
      }),
    );
    await settle();
    socket.message({ type: "aprs_dispatch", dispatchId: DISPATCH_ID, data });
    await settle();

    expect(egress.submissions).toEqual([data]);
    expect(latestControl(socket, "aprs_dispatch_ack")).toEqual({
      type: "aprs_dispatch_ack",
      dispatchId: DISPATCH_ID,
      outcome: "uncertain",
      errorCode: "CMCLOUD_DIRECT_APRS_WRITE_UNCERTAIN",
    });

    await client.stop();
    database.close();
  });
});

function createClient(
  database: GatewayDatabase,
  socketFactory: () => FixtureSocket,
  directAprsEgress?: CmCloudDirectAprsEgress,
): CmCloudAgentClient {
  return new CmCloudAgentClient({
    url: "wss://cmcloud.tmmarc.org/agent/v1",
    installationId: INSTALLATION_ID,
    installationGeneration: 0,
    credentialVersion: 1,
    deviceCredential: DEVICE_CREDENTIAL,
    clientVersion: "2.0.0",
    outbox: database.cmcloudRawOutbox,
    bootIdFactory: () => BOOT_ID,
    socketFactory: (url, protocols, options) => {
      expect(url).toBe("wss://cmcloud.tmmarc.org/agent/v1");
      expect(protocols).toEqual([CMCLOUD_AGENT_SUBPROTOCOL]);
      expect(options.headers.Authorization).toBe(`Bearer ${DEVICE_CREDENTIAL}`);
      return socketFactory();
    },
    reconnectInitialDelayMs: 10,
    reconnectMaximumDelayMs: 10,
    ...(directAprsEgress ? { directAprsEgress } : {}),
  });
}

function serverHello(
  connectionEpoch: number,
  overrides: Record<string, unknown> = {},
): Record<string, unknown> {
  return {
    type: "server_hello",
    protocolVersion: 1,
    connectionEpoch,
    installationGeneration: 0,
    credentialVersion: 1,
    heartbeatIntervalMs: 30_000,
    minimumClientVersion: "2.0.0",
    aprsMode: "disabled",
    ...overrides,
  };
}

function latestControl(
  socket: FixtureSocket,
  type: string,
): Record<string, unknown> | undefined {
  return socket.sent
    .filter((entry) => !entry.binary && typeof entry.data === "string")
    .map((entry) => JSON.parse(entry.data as string) as Record<string, unknown>)
    .filter((entry) => entry.type === type)
    .at(-1);
}

function decodeRawFrame(frame: Buffer): {
  header: Record<string, unknown>;
  body: Buffer;
} {
  expect(frame.subarray(0, 4).toString("ascii")).toBe("CMC1");
  expect(frame[4]).toBe(1);
  const headerLength = frame.readUInt16BE(5);
  return {
    header: JSON.parse(frame.subarray(7, 7 + headerLength).toString("utf8")),
    body: frame.subarray(7 + headerLength),
  };
}

async function settle(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

async function delay(milliseconds: number): Promise<void> {
  await new Promise<void>((resolve) => setTimeout(resolve, milliseconds));
}

class FixtureSocket implements CmCloudSocket {
  readonly sent: Array<{ data: string | Buffer; binary: boolean }> = [];
  private readonly listeners = {
    open: new Set<() => void>(),
    message: new Set<(data: unknown, isBinary: boolean) => void>(),
    close: new Set<(code: number, reason: Buffer) => void>(),
    error: new Set<(error: Error) => void>(),
  };

  constructor(private readonly deferClose = false) {}

  on(event: "open", listener: () => void): unknown;
  on(
    event: "message",
    listener: (data: unknown, isBinary: boolean) => void,
  ): unknown;
  on(event: "close", listener: (code: number, reason: Buffer) => void): unknown;
  on(event: "error", listener: (error: Error) => void): unknown;
  on(
    event: "open" | "message" | "close" | "error",
    listener:
      | (() => void)
      | ((data: unknown, isBinary: boolean) => void)
      | ((code: number, reason: Buffer) => void)
      | ((error: Error) => void),
  ): unknown {
    switch (event) {
      case "open":
        this.listeners.open.add(listener as () => void);
        break;
      case "message":
        this.listeners.message.add(
          listener as (data: unknown, isBinary: boolean) => void,
        );
        break;
      case "close":
        this.listeners.close.add(
          listener as (code: number, reason: Buffer) => void,
        );
        break;
      case "error":
        this.listeners.error.add(listener as (error: Error) => void);
        break;
    }
    return this;
  }

  send(
    data: string | Uint8Array,
    options: { binary: boolean },
    callback: (error?: Error) => void,
  ): void {
    this.sent.push({
      data: typeof data === "string" ? data : Buffer.from(data),
      binary: options.binary,
    });
    callback();
  }

  close(code = 1000, reason = ""): void {
    if (!this.deferClose) {
      this.emitClose(code, reason);
    }
  }

  open(): void {
    for (const listener of this.listeners.open) listener();
  }

  message(payload: object): void {
    const data = Buffer.from(JSON.stringify(payload), "utf8");
    for (const listener of this.listeners.message) listener(data, false);
  }

  remoteClose(): void {
    this.emitClose(1006, "network lost");
  }

  binaryFrames(): Buffer[] {
    return this.sent
      .filter((entry) => entry.binary)
      .map((entry) => Buffer.from(entry.data));
  }

  private emitClose(code: number, reason: string): void {
    for (const listener of this.listeners.close) {
      listener(code, Buffer.from(reason, "utf8"));
    }
  }
}

class FixtureDirectAprsEgress implements CmCloudDirectAprsEgress {
  capability: CmCloudDirectAprsCapability | undefined;
  readonly submissions: string[] = [];
  private listener: (() => void) | undefined;

  constructor(
    private readonly result: CmCloudDirectAprsDispatchResult = {
      outcome: "submitted",
    },
  ) {}

  setReadinessListener(listener: () => void): void {
    this.listener = listener;
  }

  async configure(capability?: CmCloudDirectAprsCapability): Promise<void> {
    this.capability = capability;
    this.listener?.();
  }

  ready(): boolean {
    return this.capability !== undefined;
  }

  async submit(data: string): Promise<CmCloudDirectAprsDispatchResult> {
    this.submissions.push(data);
    return this.result;
  }

  async stop(): Promise<void> {
    this.capability = undefined;
    this.listener?.();
  }
}
