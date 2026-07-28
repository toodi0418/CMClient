import { afterEach, describe, expect, it, vi } from "vitest";

import { createManagementRequestQueue } from "./management-request-queue";

afterEach(() => vi.useRealTimers());

describe("management request queue", () => {
  it("serializes requests after its small initial budget is spent", async () => {
    let current = 0;
    const calls: number[] = [];
    const waits: number[] = [];
    const queue = createManagementRequestQueue(
      async () => {
        calls.push(current);
        return new Response(null, { status: 200 });
      },
      {
        capacity: 1,
        refillIntervalMs: 100,
        now: () => current,
        wait: async (milliseconds) => {
          waits.push(milliseconds);
          current += milliseconds;
        },
      },
    );

    await Promise.all([
      queue.fetch("/one"),
      queue.fetch("/two"),
      queue.fetch("/three"),
    ]);

    expect(calls).toEqual([0, 100, 200]);
    expect(waits).toEqual([100, 100]);
  });

  it("backs off the shared queue after a rate-limited response", async () => {
    let current = 0;
    const calls: number[] = [];
    let responses = 0;
    const queue = createManagementRequestQueue(
      async () => {
        calls.push(current);
        responses += 1;
        return new Response(null, { status: responses === 1 ? 429 : 200 });
      },
      {
        capacity: 1,
        refillIntervalMs: 100,
        rateLimitDelayMs: 500,
        now: () => current,
        wait: async (milliseconds) => {
          current += milliseconds;
        },
      },
    );

    await Promise.all([queue.fetch("/one"), queue.fetch("/two")]);

    expect(calls).toEqual([0, 500]);
  });

  it("aborts a request that never settles", async () => {
    vi.useFakeTimers();
    let aborted = false;
    const queue = createManagementRequestQueue(
      async (_input, init) =>
        new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener(
            "abort",
            () => {
              aborted = true;
              reject(new DOMException("Timed out", "AbortError"));
            },
            { once: true },
          );
        }),
      { requestTimeoutMs: 25 },
    );

    const request = queue.fetch("/waiting");
    await vi.advanceTimersByTimeAsync(25);

    await expect(request).rejects.toMatchObject({ name: "AbortError" });
    expect(aborted).toBe(true);
  });
});
