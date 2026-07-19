const MAGIC = 0x94c3;
const HEADER_BYTES = 4;

export interface MeshtasticFrameDecoderOptions {
  maxPayloadBytes?: number;
}

export interface FrameDecoderMetrics {
  bufferedBytes: number;
  copiedBytes: number;
  copyOperations: number;
  malformedFrames: number;
  discardedBytes: number;
  scanSteps: number;
}

export class MeshtasticFrameError extends Error {
  readonly code = "MESHTASTIC_FRAME_INVALID";
}

export class MeshtasticFrameDecoder {
  private buffer: Uint8Array<ArrayBufferLike> = new Uint8Array();
  private readonly maxPayloadBytes: number;
  private malformedFrames = 0;
  private discardedBytes = 0;
  private copiedBytes = 0;
  private copyOperations = 0;
  private scanSteps = 0;

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
    const input = concat(this.buffer, chunk);
    this.copiedBytes += input.length;
    this.copyOperations += 1;
    const frames: Uint8Array[] = [];
    let offset = 0;
    while (input.length - offset >= HEADER_BYTES) {
      this.scanSteps += 1;
      if (readUint16(input, offset) !== MAGIC) {
        this.malformedFrames += 1;
        this.discardedBytes += 1;
        offset += 1;
        continue;
      }
      const payloadLength = readUint16(input, offset + 2);
      if (payloadLength === 0 || payloadLength > this.maxPayloadBytes) {
        this.malformedFrames += 1;
        this.discardedBytes += 2;
        offset += 2;
        continue;
      }
      const frameLength = HEADER_BYTES + payloadLength;
      if (input.length - offset < frameLength) {
        break;
      }
      frames.push(input.slice(offset + HEADER_BYTES, offset + frameLength));
      offset += frameLength;
    }
    // Copy the bounded incomplete tail so a large source chunk is not retained.
    this.buffer = input.slice(offset);
    this.copiedBytes += this.buffer.length;
    this.copyOperations += 1;
    return frames;
  }

  get metrics(): FrameDecoderMetrics {
    return {
      bufferedBytes: this.buffer.length,
      copiedBytes: this.copiedBytes,
      copyOperations: this.copyOperations,
      malformedFrames: this.malformedFrames,
      discardedBytes: this.discardedBytes,
      scanSteps: this.scanSteps,
    };
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
