export interface ReconnectBackoffOptions {
  initialDelayMs?: number;
  maximumDelayMs?: number;
  jitterRatio?: number;
}

export type RandomSource = () => number;

export class ReconnectBackoff {
  private readonly initialDelayMs: number;
  private readonly maximumDelayMs: number;
  private readonly jitterRatio: number;

  constructor(options: ReconnectBackoffOptions = {}) {
    this.initialDelayMs = options.initialDelayMs ?? 500;
    this.maximumDelayMs = options.maximumDelayMs ?? 30_000;
    this.jitterRatio = options.jitterRatio ?? 0.2;
    if (
      !Number.isInteger(this.initialDelayMs) ||
      !Number.isInteger(this.maximumDelayMs) ||
      this.initialDelayMs < 1 ||
      this.maximumDelayMs < this.initialDelayMs ||
      this.jitterRatio < 0 ||
      this.jitterRatio > 1
    ) {
      throw new RangeError("Reconnect backoff configuration is invalid");
    }
  }

  delayForAttempt(attempt: number, random: RandomSource = Math.random): number {
    if (!Number.isInteger(attempt) || attempt < 1) {
      throw new RangeError("Reconnect attempt is invalid");
    }
    const exponent = Math.min(attempt - 1, 16);
    const base = Math.min(
      this.initialDelayMs * 2 ** exponent,
      this.maximumDelayMs,
    );
    const sample = random();
    if (!Number.isFinite(sample) || sample < 0 || sample > 1) {
      throw new RangeError("Reconnect jitter source is invalid");
    }
    const factor = 1 - this.jitterRatio + sample * this.jitterRatio * 2;
    return Math.max(
      1,
      Math.min(this.maximumDelayMs, Math.round(base * factor)),
    );
  }
}
