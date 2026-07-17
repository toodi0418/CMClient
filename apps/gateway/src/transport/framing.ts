const MAGIC = 0x94c3;
const HEADER_BYTES = 4;

export interface MeshtasticFrameDecoderOptions {
  maxPayloadBytes?: number;
}

export interface FrameDecoderMetrics {
  malformedFrames: number;
  discardedBytes: number;
}

export class MeshtasticFrameError extends Error {
  readonly code = "MESHTASTIC_FRAME_INVALID";
}

export class MeshtasticFrameDecoder {
  private buffer: Uint8Array<ArrayBufferLike> = new Uint8Array();
  private readonly maxPayloadBytes: number;
  private malformedFrames = 0;
  private discardedBytes = 0;

  constructor(options: MeshtasticFrameDecoderOptions = {}) {
    this.maxPayloadBytes = options.maxPayloadBytes ?? 512;
    if (
      !Number.isInteger(this.maxPayloadBytes) ||
      this.maxPayloadBytes < 1 ||
      this.maxPayloadBytes > 65_535
    ) {
      throw new MeshtasticFrameError();
    }
  }

  push(chunk: Uint8Array): Uint8Array[] {
    this.buffer = concat(this.buffer, chunk);
    const frames: Uint8Array[] = [];
    while (this.buffer.length >= HEADER_BYTES) {
      if (readUint16(this.buffer, 0) !== MAGIC) {
        this.discardInvalidByte();
        continue;
      }
      const payloadLength = readUint16(this.buffer, 2);
      if (payloadLength === 0 || payloadLength > this.maxPayloadBytes) {
        this.malformedFrames += 1;
        this.buffer = this.buffer.slice(2);
        this.discardedBytes += 2;
        continue;
      }
      const frameLength = HEADER_BYTES + payloadLength;
      if (this.buffer.length < frameLength) {
        break;
      }
      frames.push(this.buffer.slice(HEADER_BYTES, frameLength));
      this.buffer = this.buffer.slice(frameLength);
    }
    return frames;
  }

  get metrics(): FrameDecoderMetrics {
    return {
      malformedFrames: this.malformedFrames,
      discardedBytes: this.discardedBytes,
    };
  }

  private discardInvalidByte(): void {
    this.malformedFrames += 1;
    this.discardedBytes += 1;
    this.buffer = this.buffer.slice(1);
  }
}

export function encodeMeshtasticFrame(
  payload: Uint8Array,
  maxPayloadBytes = 512,
): Uint8Array {
  if (
    payload.length === 0 ||
    payload.length > maxPayloadBytes ||
    payload.length > 65_535
  ) {
    throw new MeshtasticFrameError();
  }
  const frame = new Uint8Array(HEADER_BYTES + payload.length);
  frame[0] = MAGIC >> 8;
  frame[1] = MAGIC & 0xff;
  frame[2] = payload.length >> 8;
  frame[3] = payload.length & 0xff;
  frame.set(payload, HEADER_BYTES);
  return frame;
}

function concat(left: Uint8Array, right: Uint8Array): Uint8Array {
  const result = new Uint8Array(left.length + right.length);
  result.set(left);
  result.set(right, left.length);
  return result;
}

function readUint16(value: Uint8Array, offset: number): number {
  return (value[offset]! << 8) | value[offset + 1]!;
}
