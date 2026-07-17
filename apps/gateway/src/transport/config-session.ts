export interface ConfigSessionCodec {
  encodeWantConfig(nonce: number): Uint8Array;
  isConfigComplete(payload: Uint8Array, nonce: number): boolean;
}

export type NonceFactory = () => number;

export class ConfigSessionError extends Error {
  readonly code = "MESHTASTIC_CONFIG_SESSION_INVALID";
}

export class ConfigSession {
  private nonce: number | undefined;

  constructor(
    private readonly codec: ConfigSessionCodec,
    private readonly nonceFactory: NonceFactory = defaultNonce,
  ) {}

  begin(): Uint8Array {
    const nonce = this.nonceFactory();
    if (!Number.isInteger(nonce) || nonce < 1 || nonce > 0xffff_ffff) {
      throw new ConfigSessionError();
    }
    const request = this.codec.encodeWantConfig(nonce);
    if (request.length === 0) {
      throw new ConfigSessionError();
    }
    this.nonce = nonce;
    return request;
  }

  observe(payload: Uint8Array): boolean {
    return (
      this.nonce !== undefined &&
      this.codec.isConfigComplete(payload, this.nonce)
    );
  }

  reset(): void {
    this.nonce = undefined;
  }
}

function defaultNonce(): number {
  return Math.floor(Math.random() * 0xffff_ffff) + 1;
}
