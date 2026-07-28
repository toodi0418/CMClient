export interface ManagementRequestQueue {
  fetch: typeof fetch;
}

export interface ManagementRequestQueueOptions {
  capacity?: number;
  refillIntervalMs?: number;
  rateLimitDelayMs?: number;
  maxRateLimitDelayMs?: number;
  requestTimeoutMs?: number;
  now?: () => number;
  wait?: (milliseconds: number) => Promise<void>;
}

/**
 * Shares one browser-side request budget across every management API client.
 * The Agent stays authoritative for admission; this prevents a busy Web UI
 * from repeatedly consuming its protection budget after the initial load.
 */
export function createManagementRequestQueue(
  fetchImplementation: typeof fetch = (input, init) =>
    globalThis.fetch(input, init),
  options: ManagementRequestQueueOptions = {},
): ManagementRequestQueue {
  const capacity = options.capacity ?? 8;
  const refillIntervalMs = options.refillIntervalMs ?? 1_100;
  const rateLimitDelayMs = options.rateLimitDelayMs ?? 1_100;
  const maxRateLimitDelayMs = options.maxRateLimitDelayMs ?? 8_000;
  const requestTimeoutMs = options.requestTimeoutMs ?? 10_000;
  const now = options.now ?? (() => Date.now());
  const wait = options.wait ?? delay;
  let tokens = capacity;
  let lastRefillAt = now();
  let cooldownUntil = 0;
  let consecutiveRateLimits = 0;
  let tail = Promise.resolve();

  const run = <T>(operation: () => Promise<T>): Promise<T> => {
    const result = tail.then(operation, operation);
    tail = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  };

  const takeTurn = async () => {
    refill();
    const current = now();
    const tokenDelay =
      tokens >= 1 ? 0 : Math.ceil((1 - tokens) * refillIntervalMs);
    const waitMs = Math.max(tokenDelay, cooldownUntil - current, 0);
    if (waitMs > 0) {
      await wait(waitMs);
      refill();
    }
    tokens = Math.max(0, tokens - 1);
  };

  const refill = () => {
    const current = now();
    const elapsed = Math.max(0, current - lastRefillAt);
    if (elapsed > 0) {
      tokens = Math.min(capacity, tokens + elapsed / refillIntervalMs);
      lastRefillAt = current;
    }
  };

  return {
    fetch(input, init) {
      return run(async () => {
        await takeTurn();
        const response = await fetchWithTimeout(
          fetchImplementation,
          input,
          init,
          requestTimeoutMs,
        );
        if (response.status === 429) {
          consecutiveRateLimits = Math.min(consecutiveRateLimits + 1, 4);
          tokens = 0;
          lastRefillAt = now();
          cooldownUntil = Math.max(
            cooldownUntil,
            now() +
              retryAfterMilliseconds(
                response.headers.get("retry-after"),
                now(),
              ) +
              Math.min(
                maxRateLimitDelayMs,
                rateLimitDelayMs * 2 ** (consecutiveRateLimits - 1),
              ),
          );
        } else if (response.ok) {
          consecutiveRateLimits = 0;
        }
        return response;
      });
    },
  };
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

async function fetchWithTimeout(
  fetchImplementation: typeof fetch,
  input: Parameters<typeof fetch>[0],
  init: Parameters<typeof fetch>[1],
  timeoutMs: number,
): Promise<Response> {
  if (timeoutMs <= 0) {
    return fetchImplementation(input, init);
  }
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  const signal = init?.signal
    ? AbortSignal.any([init.signal, controller.signal])
    : controller.signal;
  try {
    return await fetchImplementation(input, {
      ...(init ?? {}),
      signal,
    });
  } finally {
    clearTimeout(timer);
  }
}

function retryAfterMilliseconds(value: string | null, current: number): number {
  if (!value) {
    return 0;
  }
  const seconds = Number(value);
  if (Number.isFinite(seconds) && seconds >= 0) {
    return seconds * 1_000;
  }
  const timestamp = Date.parse(value);
  return Number.isFinite(timestamp) ? Math.max(0, timestamp - current) : 0;
}
