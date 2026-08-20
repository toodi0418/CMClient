import { EventEmitter, once } from "node:events";
import { createServer, type Server, type Socket } from "node:net";

import { describe, expect, it } from "vitest";

import {
  CmCloudDirectAprsEgressRuntime,
  isValidCmCloudTrackerDispatch,
  parseCmCloudDirectAprsCapability,
} from "./cmcloud-aprs";
import { deriveAprsPasscode } from "./aprs-identity";

describe("CMCloud direct APRS egress", () => {
  it.each([
    ["BU2GE", "BU2GE"],
    ["BU2GE-0", "BU2GE"],
    ["BU2GE-15", "BU2GE-15"],
  ])(
    "normalizes the CMCloud direct APRS identity %s to %s",
    (callsign, expected) => {
      expect(
        parseCmCloudDirectAprsCapability({ callsign, verified: true }),
      ).toEqual({ callsign: expected, verified: true });
    },
  );

  it("rejects an APRS SSID above 15", () => {
    expect(() =>
      parseCmCloudDirectAprsCapability({
        callsign: "BU2GE-16",
        verified: true,
      }),
    ).toThrowError(
      expect.objectContaining({
        code: "CMCLOUD_DIRECT_APRS_CAPABILITY_INVALID",
      }),
    );
  });

  it.each([
    ["BU2GE", "BU2GE"],
    ["BU2GE-0", "BU2GE"],
  ])(
    "uses the normalized zero-SSID identity %s as %s in its APRS-IS login",
    async (callsign, expected) => {
      const fixture = await startAprsFixture("verified", expected);
      const egress = new CmCloudDirectAprsEgressRuntime({
        host: "127.0.0.1",
        port: fixture.port,
        timeoutMs: 1_000,
        reconnectDelayMs: 100,
      });
      try {
        await egress.configure({ callsign, verified: true });
        expect(fixture.lines[0]).toBe(
          `user ${expected} pass ${deriveAprsPasscode("BU2GE")} vers CMClient 2.0`,
        );
      } finally {
        await egress.stop();
        await fixture.close();
      }
    },
  );

  it("writes the CMCloud TNC2 payload byte-for-byte after verified APRS-IS login", async () => {
    const fixture = await startAprsFixture("verified");
    const egress = new CmCloudDirectAprsEgressRuntime({
      host: "127.0.0.1",
      port: fixture.port,
      timeoutMs: 1_000,
      reconnectDelayMs: 100,
    });
    const data =
      "BM5GSV-5>APTMAG,MESHD*,qAO,BM5GSV-5:!2404.57N/12032.42Ek/A=000141 家有大狗狗";
    try {
      await egress.configure(trackerCapability());
      expect(egress.ready()).toBe(true);

      await expect(egress.submit(data)).resolves.toEqual({
        outcome: "submitted",
      });
      await waitUntil(() => fixture.lines.length === 2);
      expect(fixture.lines[0]).toMatch(
        /^user BM5GSV-5 pass \d+ vers CMClient 2\.0$/u,
      );
      expect(fixture.wire[1]).toEqual(Buffer.from(`${data}\r\n`, "utf8"));
    } finally {
      await egress.stop();
      await fixture.close();
    }
  });

  it("allows a valid iGate station packet through the shared APRS writer", async () => {
    const fixture = await startAprsFixture("verified");
    const egress = new CmCloudDirectAprsEgressRuntime({
      host: "127.0.0.1",
      port: fixture.port,
      timeoutMs: 1_000,
      reconnectDelayMs: 100,
    });
    const data = "BM5GSV-5>APTMAG,TCPIP*:T#001,0,1,0,0,0,00000000";
    try {
      await egress.configure(trackerCapability());

      await expect(egress.submit(data)).resolves.toEqual({
        outcome: "submitted",
      });
      await waitUntil(() => fixture.lines.length === 2);
      expect(fixture.wire[1]).toEqual(Buffer.from(`${data}\r\n`, "utf8"));
    } finally {
      await egress.stop();
      await fixture.close();
    }
  });

  it("accepts only the provisioned Tracker path and rejects observer traffic", () => {
    const capability = trackerCapability();
    const valid = "BX4ACP-7>APTMAG,MESHD*,qAO,BM5GSV-5:!2404.57N/12032.42Ek";

    expect(isValidCmCloudTrackerDispatch(valid, capability)).toBe(true);
    expect(
      isValidCmCloudTrackerDispatch(
        "BX4ACP-7>APTMAG,MESHD*,qAO,OTHER-1:!2404.57N/12032.42Ek",
        capability,
      ),
    ).toBe(false);
    expect(
      isValidCmCloudTrackerDispatch(
        "BX4ACP-7>APTMAG,TCPIP*,qAC,BU2GE-CC:!2404.57N/12032.42Ek",
        capability,
      ),
    ).toBe(false);
    expect(isValidCmCloudTrackerDispatch("not a TNC2 line", capability)).toBe(
      false,
    );
  });

  it("never becomes ready for an unverified APRS-IS login or an absent CMCloud capability", async () => {
    const fixture = await startAprsFixture("unverified");
    const egress = new CmCloudDirectAprsEgressRuntime({
      host: "127.0.0.1",
      port: fixture.port,
      timeoutMs: 1_000,
      reconnectDelayMs: 100,
    });
    try {
      await egress.configure(trackerCapability());
      expect(egress.ready()).toBe(false);
      await expect(
        egress.submit(
          "BM5GSV-5>APTMAG,MESHD*,qAO,BM5GSV-5:!2404.57N/12032.42Ek",
        ),
      ).resolves.toEqual({
        outcome: "retryable_failure",
        errorCode: "CMCLOUD_DIRECT_APRS_NOT_READY",
      });
      expect(() =>
        parseCmCloudDirectAprsCapability({
          callsign: "BM5GSV-5",
          verified: false,
        }),
      ).toThrowError(
        expect.objectContaining({
          code: "CMCLOUD_DIRECT_APRS_CAPABILITY_INVALID",
        }),
      );
    } finally {
      await egress.stop();
      await fixture.close();
    }
  });

  it("rejects a lone UTF-16 surrogate before Node can alter the APRS payload", async () => {
    const fixture = await startAprsFixture("verified");
    const egress = new CmCloudDirectAprsEgressRuntime({
      host: "127.0.0.1",
      port: fixture.port,
      timeoutMs: 1_000,
      reconnectDelayMs: 100,
    });
    try {
      await egress.configure(trackerCapability());
      await expect(
        egress.submit(
          "BM5GSV-5>APTMAG,MESHD*,qAO,BM5GSV-5:!2404.57N/12032.42Ek\uD800",
        ),
      ).resolves.toEqual({
        outcome: "retryable_failure",
        errorCode: "CMCLOUD_APRS_DISPATCH_INVALID",
      });
      expect(fixture.lines).toHaveLength(1);
    } finally {
      await egress.stop();
      await fixture.close();
    }
  });

  it("bounds a stalled APRS write as uncertain and tears down the session", async () => {
    const socket = new HangingAprsSocket();
    const egress = new CmCloudDirectAprsEgressRuntime({
      host: "fixture.invalid",
      port: 14_580,
      timeoutMs: 100,
      reconnectDelayMs: 100,
      socketFactory: () => socket as unknown as Socket,
    });
    try {
      await egress.configure(trackerCapability());
      await expect(
        egress.submit(
          "BM5GSV-5>APTMAG,MESHD*,qAO,BM5GSV-5:!2404.57N/12032.42Ek",
        ),
      ).resolves.toEqual({
        outcome: "uncertain",
        errorCode: "CMCLOUD_DIRECT_APRS_WRITE_UNCERTAIN",
      });
      expect(socket.destroyed).toBe(true);
      expect(socket.destroyCalls).toBe(1);
      expect(egress.ready()).toBe(false);
    } finally {
      await egress.stop();
    }
  });
});

function trackerCapability() {
  return {
    callsign: "BM5GSV-5",
    verified: true as const,
    provision: {
      callsignBase: "BM5GSV",
      ssid: 5,
      symbolTable: "/",
      symbolCode: "I",
    },
  };
}

class HangingAprsSocket extends EventEmitter {
  connecting = false;
  destroyed = false;
  writable = true;
  destroyCalls = 0;

  setNoDelay(): this {
    return this;
  }

  unref(): this {
    return this;
  }

  write(
    value: string | Buffer,
    callback?: (error?: Error | null) => void,
  ): boolean {
    const line = Buffer.from(value).toString("utf8");
    if (line.startsWith("user ")) {
      callback?.();
      queueMicrotask(() => {
        this.emit(
          "data",
          Buffer.from("# logresp BM5GSV-5 verified, fixture\r\n"),
        );
      });
    }
    return true;
  }

  destroy(): this {
    this.destroyCalls += 1;
    if (!this.destroyed) {
      this.destroyed = true;
      this.writable = false;
      this.emit("close");
    }
    return this;
  }
}

async function startAprsFixture(
  status: "verified" | "unverified",
  callsign = "BM5GSV-5",
): Promise<{
  readonly port: number;
  readonly lines: string[];
  readonly wire: Buffer[];
  close(): Promise<void>;
}> {
  const lines: string[] = [];
  const wire: Buffer[] = [];
  const server = createServer((socket) => {
    attachLineCollector(socket, (line, raw) => {
      lines.push(line);
      wire.push(raw);
      if (lines.length === 1) {
        socket.write(`# logresp ${callsign} ${status}, fixture\r\n`);
      }
    });
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  if (!address || typeof address === "string") {
    throw new Error("APRS fixture did not obtain a TCP port");
  }
  return {
    port: address.port,
    lines,
    wire,
    close: () => closeServer(server),
  };
}

function attachLineCollector(
  socket: Socket,
  onLine: (line: string, raw: Buffer) => void,
): void {
  let buffered = Buffer.alloc(0);
  socket.on("data", (chunk: Buffer) => {
    buffered = Buffer.concat([buffered, Buffer.from(chunk)]);
    let boundary: number;
    while ((boundary = buffered.indexOf("\r\n")) >= 0) {
      const raw = buffered.subarray(0, boundary + 2);
      buffered = buffered.subarray(boundary + 2);
      onLine(raw.subarray(0, -2).toString("utf8"), raw);
    }
  });
}

function closeServer(server: Server): Promise<void> {
  return new Promise((resolve, reject) => {
    server.close((error) => (error ? reject(error) : resolve()));
  });
}

async function waitUntil(predicate: () => boolean): Promise<void> {
  const deadline = Date.now() + 1_000;
  while (!predicate()) {
    if (Date.now() >= deadline) {
      throw new Error("fixture condition timed out");
    }
    await new Promise<void>((resolve) => setTimeout(resolve, 5));
  }
}
