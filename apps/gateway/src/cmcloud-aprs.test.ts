import { once } from "node:events";
import { createServer, type Server, type Socket } from "node:net";

import { describe, expect, it } from "vitest";

import {
  CmCloudDirectAprsEgressRuntime,
  parseCmCloudDirectAprsCapability,
} from "./cmcloud-aprs";

describe("CMCloud direct APRS egress", () => {
  it("writes the CMCloud TNC2 payload byte-for-byte after verified APRS-IS login", async () => {
    const fixture = await startAprsFixture("verified");
    const egress = new CmCloudDirectAprsEgressRuntime({
      host: "127.0.0.1",
      port: fixture.port,
      timeoutMs: 1_000,
      reconnectDelayMs: 100,
    });
    const data =
      "BM5GSV-5>APTMAG,MESHD*,qAR,BM3FFG-2:!2404.57N/12032.42Ek/A=000141 家有大狗狗";
    try {
      await egress.configure({ callsign: "BM5GSV-5", verified: true });
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

  it("never becomes ready for an unverified APRS-IS login or an absent CMCloud capability", async () => {
    const fixture = await startAprsFixture("unverified");
    const egress = new CmCloudDirectAprsEgressRuntime({
      host: "127.0.0.1",
      port: fixture.port,
      timeoutMs: 1_000,
      reconnectDelayMs: 100,
    });
    try {
      await egress.configure({ callsign: "BM5GSV-5", verified: true });
      expect(egress.ready()).toBe(false);
      await expect(
        egress.submit("BM5GSV-5>APTMAG:!2404.57N/12032.42Ek"),
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
});

async function startAprsFixture(status: "verified" | "unverified"): Promise<{
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
        socket.write(`# logresp BM5GSV-5 ${status}, fixture\r\n`);
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
