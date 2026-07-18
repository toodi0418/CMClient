import { describe, expect, it } from "vitest";

import type { ProxyConfigFrame, ProxyUpstreamEvent } from "./upstream";
import {
  ProxySessionError,
  ProxySessionManager,
  type ProxyClientSink,
} from "./sessions";

describe("proxy client session manager", () => {
  it("seeds a new client from config cache before broadcasting upstream frames", async () => {
    const upstream = new FakeUpstream([
      configFrame("my_info", new Uint8Array([1])),
      configFrame("config", new Uint8Array([2])),
    ]);
    const manager = new ProxySessionManager(upstream);
    const client = new FakeClient("client-1");
    manager.start();
    manager.attach(client);
    upstream.emit({
      kind: "frame",
      frame: new Uint8Array([3]),
      receivedAt: timestamp,
    });

    await waitFor(() => client.frames.length === 3);
    expect(client.frames).toEqual([
      new Uint8Array([1]),
      new Uint8Array([2]),
      new Uint8Array([3]),
    ]);
    manager.stop();
    expect(client.closedWith).toBe("PROXY_SESSION_MANAGER_STOPPED");
  });

  it("evicts only a slow client when its bounded queue applies backpressure", async () => {
    const upstream = new FakeUpstream([]);
    const manager = new ProxySessionManager(upstream, {
      maxQueuedFrames: 1,
      maxQueuedBytes: 1_024,
    });
    const fast = new FakeClient("fast");
    const slow = new FakeClient("slow", true);
    manager.attach(fast);
    manager.attach(slow);

    expect(manager.broadcast(new Uint8Array([1]))).toEqual({
      accepted: 2,
      dropped: 0,
    });
    await waitFor(() => fast.frames.length === 1);
    expect(manager.broadcast(new Uint8Array([2]))).toEqual({
      accepted: 1,
      dropped: 1,
    });
    await waitFor(() => fast.frames.length === 2);
    expect(slow.closedWith).toBe("PROXY_CLIENT_BACKPRESSURE");
    expect(manager.snapshot.map((session) => session.id)).toEqual(["fast"]);
  });

  it("rejects duplicate or over-limit client sessions", () => {
    const manager = new ProxySessionManager(new FakeUpstream([]), {
      maxClients: 1,
    });
    manager.attach(new FakeClient("client-1"));
    expect(() => manager.attach(new FakeClient("client-1"))).toThrow(
      ProxySessionError,
    );
    expect(() => manager.attach(new FakeClient("client-2"))).toThrow(
      ProxySessionError,
    );
  });
});

const timestamp = "2026-07-18T00:00:00.000Z";

class FakeUpstream {
  private readonly listeners = new Set<(event: ProxyUpstreamEvent) => void>();
  readonly configCache: { snapshot(): ProxyConfigFrame[] };

  constructor(entries: ProxyConfigFrame[]) {
    this.configCache = {
      snapshot: () =>
        entries.map((entry) => ({
          ...entry,
          frame: new Uint8Array(entry.frame),
        })),
    };
  }

  subscribe(listener: (event: ProxyUpstreamEvent) => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  emit(event: ProxyUpstreamEvent): void {
    for (const listener of this.listeners) {
      listener(event);
    }
  }
}

class FakeClient implements ProxyClientSink {
  readonly frames: Uint8Array[] = [];
  closedWith: string | undefined;
  private resolveWrite: (() => void) | undefined;

  constructor(
    readonly id: string,
    private readonly blockWrites = false,
  ) {}

  close(code: string): void {
    this.closedWith = code;
    this.resolveWrite?.();
  }

  async write(frame: Uint8Array): Promise<void> {
    if (this.blockWrites) {
      await new Promise<void>((resolve) => {
        this.resolveWrite = resolve;
      });
      return;
    }
    this.frames.push(frame);
  }
}

function configFrame(
  kind: ProxyConfigFrame["kind"],
  frame: Uint8Array,
): ProxyConfigFrame {
  return { kind, frame, receivedAt: timestamp };
}

async function waitFor(predicate: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 20; attempt += 1) {
    if (predicate()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  throw new Error("proxy session fixture did not settle");
}
