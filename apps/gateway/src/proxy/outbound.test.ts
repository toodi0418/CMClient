import { describe, expect, it } from "vitest";

import { loadMeshtasticSchema } from "../protobuf/schema";
import type { ProxyConfigFrame, ProxyUpstreamEvent } from "./upstream";
import { ProxyOutboundError, ProxyOutboundRouter } from "./outbound";
import { ProxySessionManager, type ProxyClientSink } from "./sessions";

const timestamp = "2026-07-18T00:00:00.000Z";

describe("proxy outbound serialization and request router", () => {
  it("serializes all client writes before passing a copied ToRadio frame upstream", async () => {
    const schema = await loadMeshtasticSchema();
    const upstream = new FakeUpstream(true);
    const router = new ProxyOutboundRouter(schema, upstream, {
      deliver: () => true,
    });
    const first = new Uint8Array(
      schema.toRadio.encode({ heartbeat: {} }).finish(),
    );
    const second = new Uint8Array(
      schema.toRadio.encode({ disconnect: true }).finish(),
    );
    const firstBeforeMutation = new Uint8Array(first);

    const firstWrite = router.submit({ clientId: "client-a", frame: first });
    first[0] = 0;
    await waitFor(() => upstream.frames.length === 1);
    const secondWrite = router.submit({ clientId: "client-b", frame: second });
    await new Promise((resolve) => setTimeout(resolve, 1));
    expect(upstream.frames).toEqual([firstBeforeMutation]);
    expect(router.snapshot).toMatchObject({ queuedWrites: 1, writing: true });

    upstream.releaseFirstWrite();
    await expect(firstWrite).resolves.toEqual({ correlations: [] });
    await expect(secondWrite).resolves.toEqual({ correlations: [] });
    expect(upstream.frames).toEqual([firstBeforeMutation, second]);
    router.stop();
  });

  it("routes reply, ACK, and config completion only to the owning client", async () => {
    const schema = await loadMeshtasticSchema();
    const upstream = new FakeUpstream();
    const sessions = new ProxySessionManager(upstream);
    const clientA = new FakeClient("client-a");
    const clientB = new FakeClient("client-b");
    const router = new ProxyOutboundRouter(schema, upstream, sessions);
    sessions.attach(clientA);
    sessions.attach(clientB);
    sessions.start((event) => router.handleUpstreamEvent(event));

    const request = new Uint8Array(
      schema.toRadio
        .encode({
          packet: {
            id: 101,
            wantAck: true,
            decoded: { wantResponse: true },
          },
        })
        .finish(),
    );
    await expect(
      router.submit({ clientId: "client-a", frame: request }),
    ).resolves.toEqual({
      correlations: [
        { kind: "request", id: 101 },
        { kind: "ack", id: 101 },
      ],
    });
    expect(router.snapshot.pendingCorrelations).toBe(2);

    const reply = new Uint8Array(
      schema.fromRadio
        .encode({ packet: { decoded: { replyId: 101 } } })
        .finish(),
    );
    const acknowledgement = new Uint8Array(
      schema.fromRadio
        .encode({ packet: { decoded: { requestId: 101 } } })
        .finish(),
    );
    upstream.emit({ kind: "frame", frame: reply, receivedAt: timestamp });
    upstream.emit({
      kind: "frame",
      frame: acknowledgement,
      receivedAt: timestamp,
    });

    await waitFor(() => clientA.frames.length === 2);
    expect(clientA.frames).toEqual([reply, acknowledgement]);
    expect(clientB.frames).toEqual([]);
    expect(router.snapshot.pendingCorrelations).toBe(0);

    const config = new Uint8Array(
      schema.toRadio.encode({ wantConfigId: 303 }).finish(),
    );
    await router.submit({ clientId: "client-a", frame: config });
    upstream.emit({
      kind: "frame",
      frame: new Uint8Array(
        schema.fromRadio.encode({ configCompleteId: 303 }).finish(),
      ),
      receivedAt: timestamp,
    });
    await waitFor(() => clientA.frames.length === 3);
    expect(clientB.frames).toEqual([]);
    sessions.stop();
    router.stop();
  });

  it("cancels an in-flight client write without blocking later queue cleanup", async () => {
    const schema = await loadMeshtasticSchema();
    const upstream = new FakeUpstream(true);
    const router = new ProxyOutboundRouter(schema, upstream, {
      deliver: () => true,
    });
    const write = router.submit({
      clientId: "client-a",
      frame: new Uint8Array(
        schema.toRadio.encode({ wantConfigId: 303 }).finish(),
      ),
    });

    await waitFor(() => upstream.frames.length === 1);
    router.cancelClient("client-a");
    await expect(write).rejects.toMatchObject({
      code: "PROXY_CLIENT_DISCONNECTED",
    });
    expect(router.snapshot.pendingCorrelations).toBe(0);
    upstream.releaseFirstWrite();
    await waitFor(() => !router.snapshot.writing);
    router.stop();
  });

  it("rejects invalid or conflicting correlations without writing them upstream", async () => {
    const schema = await loadMeshtasticSchema();
    const upstream = new FakeUpstream();
    const router = new ProxyOutboundRouter(schema, upstream, {
      deliver: () => true,
    });
    const config = new Uint8Array(
      schema.toRadio.encode({ wantConfigId: 404 }).finish(),
    );

    await router.submit({ clientId: "client-a", frame: config });
    await expect(
      router.submit({ clientId: "client-b", frame: config }),
    ).rejects.toMatchObject({ code: "PROXY_CORRELATION_CONFLICT" });
    await expect(
      router.submit({
        clientId: "client-b",
        frame: new Uint8Array(
          schema.toRadio
            .encode({ packet: { wantAck: true, decoded: {} } })
            .finish(),
        ),
      }),
    ).rejects.toBeInstanceOf(ProxyOutboundError);
    expect(upstream.frames).toEqual([config]);
    router.stop();
  });
});

class FakeUpstream {
  private readonly listeners = new Set<(event: ProxyUpstreamEvent) => void>();
  readonly configCache = { snapshot: (): ProxyConfigFrame[] => [] };
  readonly frames: Uint8Array[] = [];
  private release: (() => void) | undefined;
  private firstWrite = true;

  constructor(private readonly blockFirstWrite = false) {}

  async writeFrame(frame: Uint8Array): Promise<void> {
    this.frames.push(new Uint8Array(frame));
    if (this.blockFirstWrite && this.firstWrite) {
      this.firstWrite = false;
      await new Promise<void>((resolve) => {
        this.release = resolve;
      });
    }
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

  releaseFirstWrite(): void {
    this.release?.();
  }
}

class FakeClient implements ProxyClientSink {
  readonly frames: Uint8Array[] = [];

  constructor(readonly id: string) {}

  close(): void {}

  async write(frame: Uint8Array): Promise<void> {
    this.frames.push(new Uint8Array(frame));
  }
}

async function waitFor(predicate: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 20; attempt += 1) {
    if (predicate()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  throw new Error("proxy outbound fixture did not settle");
}
