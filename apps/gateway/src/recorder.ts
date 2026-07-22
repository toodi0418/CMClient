import { createHash } from "node:crypto";
import { Value } from "@sinclair/typebox/value";

import type {
  MeshObservation,
  NormalizedFromRadio,
  NormalizedMeshPacket,
  PacketTransportMetadata,
  SanitizedPacketFixtureEntry,
  SanitizedPacketFixtureSet,
  TransportKind,
} from "@cmclient/contracts";
import { SanitizedPacketFixtureSetSchema } from "@cmclient/contracts";

const FIXTURE_EPOCH_MS = Date.parse("2030-01-02T00:00:00.000Z");
const UTC_ISO_TIMESTAMP = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3})?Z$/;

export interface PacketRecordInput {
  gatewayId: string;
  meshNetworkId: string;
  observation: MeshObservation;
  rawFrame: Uint8Array;
  receivedAt: string;
  transport: TransportKind;
  transportMetadata: PacketTransportMetadata;
}

export interface RecordedPacket extends PacketRecordInput {
  sequence: number;
}

export interface PacketRecorderOptions {
  maximumEntries?: number;
  maximumBytes?: number;
  maximumAgeMs?: number;
  maximumFrameBytes?: number;
  maximumSanitizedBytes?: number;
  clock?: () => Date;
  syntheticProjector?: SyntheticCaptureProjector;
}

export interface SyntheticCaptureProjection {
  frame: Uint8Array;
  normalizedFromRadio: NormalizedFromRadio;
}

export type SyntheticCaptureProjector = (
  source: NormalizedFromRadio,
  sequence: number,
) => SyntheticCaptureProjection;

export interface SanitizedPacketExport {
  fixtureSet: SanitizedPacketFixtureSet;
  digest: string;
}

export class PacketRecorderError extends Error {
  readonly code = "PACKET_RECORD_INVALID";

  constructor() {
    super("PACKET_RECORD_INVALID");
  }
}

export class PacketFixtureSanitizerError extends Error {
  readonly code = "PACKET_FIXTURE_SANITIZATION_INVALID";

  constructor() {
    super("PACKET_FIXTURE_SANITIZATION_INVALID");
  }
}

export class PacketReplayError extends Error {
  readonly code = "PACKET_REPLAY_INVALID";

  constructor() {
    super("PACKET_REPLAY_INVALID");
  }
}

/**
 * Keeps raw frames only in the bounded local recorder. Exporting requires the
 * sanitizer below; no recorder method serializes raw bytes to a fixture.
 */
export class PacketRecorder {
  private readonly maximumEntries: number;
  private readonly maximumBytes: number;
  private readonly maximumAgeMs: number;
  private readonly maximumFrameBytes: number;
  private readonly maximumSanitizedBytes: number;
  private readonly clock: () => Date;
  private readonly syntheticProjector: SyntheticCaptureProjector | undefined;
  private nextSequence = 1;
  private retainedBytes = 0;
  private readonly records: Array<{
    capturedAtMs: number;
    retainedBytes: number;
    record: RecordedPacket;
  }> = [];

  constructor(options: PacketRecorderOptions = {}) {
    this.maximumEntries = options.maximumEntries ?? 1_000;
    this.maximumBytes = options.maximumBytes ?? 4 * 1024 * 1024;
    this.maximumAgeMs = options.maximumAgeMs ?? 5 * 60 * 1_000;
    this.maximumFrameBytes = options.maximumFrameBytes ?? 65_535;
    this.maximumSanitizedBytes =
      options.maximumSanitizedBytes ?? 4 * 1024 * 1024;
    this.clock = options.clock ?? (() => new Date());
    this.syntheticProjector = options.syntheticProjector;
    if (
      !boundedPositiveInteger(this.maximumEntries, 100_000) ||
      !boundedPositiveInteger(this.maximumBytes, 64 * 1024 * 1024) ||
      !boundedPositiveInteger(this.maximumAgeMs, 24 * 60 * 60 * 1_000) ||
      !boundedPositiveInteger(this.maximumFrameBytes, 65_535) ||
      !boundedPositiveInteger(this.maximumSanitizedBytes, 64 * 1024 * 1024)
    ) {
      throw new PacketRecorderError();
    }
  }

  record(input: PacketRecordInput): RecordedPacket {
    validateRecordInput(input);
    if (input.rawFrame.length > this.maximumFrameBytes) {
      throw new PacketRecorderError();
    }
    const capturedAtMs = this.clock().getTime();
    if (!Number.isSafeInteger(capturedAtMs) || capturedAtMs < 0) {
      throw new PacketRecorderError();
    }
    this.evictExpired(capturedAtMs);
    const record: RecordedPacket = {
      sequence: this.nextSequence++,
      ...cloneRecordInput(input),
    };
    const retainedBytes = estimateRetainedBytes(record);
    if (retainedBytes > this.maximumBytes) {
      throw new PacketRecorderError();
    }
    while (
      this.records.length >= this.maximumEntries ||
      this.retainedBytes + retainedBytes > this.maximumBytes
    ) {
      this.evictOldest();
    }
    this.records.push({ capturedAtMs, retainedBytes, record });
    this.retainedBytes += retainedBytes;
    return cloneRecordedPacket(record);
  }

  snapshot(): RecordedPacket[] {
    return this.records.map(({ record }) => cloneRecordedPacket(record));
  }

  sealAndSanitize(): SanitizedPacketExport {
    const fixtureSet = new PacketFixtureSanitizer(
      this.syntheticProjector,
    ).sanitize(this.snapshot());
    const serialized = JSON.stringify(fixtureSet);
    if (
      Buffer.byteLength(serialized, "utf8") > this.maximumSanitizedBytes ||
      !Value.Check(SanitizedPacketFixtureSetSchema, fixtureSet)
    ) {
      throw new PacketFixtureSanitizerError();
    }
    const digest = createHash("sha256")
      .update(serialized, "utf8")
      .digest("hex");
    this.clear();
    return { fixtureSet, digest };
  }

  clear(): void {
    this.records.splice(0);
    this.retainedBytes = 0;
  }

  private evictExpired(nowMs: number): void {
    const oldestAllowed = nowMs - this.maximumAgeMs;
    while (this.records[0] && this.records[0].capturedAtMs < oldestAllowed) {
      this.evictOldest();
    }
  }

  private evictOldest(): void {
    const removed = this.records.shift();
    if (removed) {
      this.retainedBytes -= removed.retainedBytes;
    }
  }
}

export class PacketFixtureSanitizer {
  constructor(
    private readonly syntheticProjector?: SyntheticCaptureProjector,
  ) {}

  sanitize(records: readonly RecordedPacket[]): SanitizedPacketFixtureSet {
    const ordered = [...records].sort(
      (left, right) => left.sequence - right.sequence,
    );
    if (
      ordered.some(
        (record, index) =>
          !Number.isInteger(record.sequence) ||
          record.sequence < 1 ||
          (index > 0 && record.sequence === ordered[index - 1]?.sequence),
      )
    ) {
      throw new PacketFixtureSanitizerError();
    }
    for (const record of ordered) {
      try {
        validateRecordInput(record);
      } catch {
        throw new PacketFixtureSanitizerError();
      }
    }
    const timestampShiftMs = calculateTimestampShift(ordered);
    const aliases = new SanitizationAliases();
    return {
      schemaVersion: 1,
      dataset: "cmclient-sanitized-packet-recordings",
      sanitized: true,
      fixtures: ordered.map((record) =>
        sanitizeRecord(
          record,
          timestampShiftMs,
          aliases,
          this.syntheticProjector,
        ),
      ),
    };
  }
}

export async function replayPacketFixtures(
  fixtureSet: SanitizedPacketFixtureSet,
  consume: (fixture: SanitizedPacketFixtureEntry) => void | Promise<void>,
): Promise<void> {
  if (
    fixtureSet.schemaVersion !== 1 ||
    fixtureSet.dataset !== "cmclient-sanitized-packet-recordings" ||
    !fixtureSet.sanitized
  ) {
    throw new PacketReplayError();
  }
  const fixtureIds = new Set<string>();
  const ordered = [...fixtureSet.fixtures].sort((left, right) => {
    const chronological = left.recording.serverIngestedAt.localeCompare(
      right.recording.serverIngestedAt,
    );
    return chronological || left.id.localeCompare(right.id);
  });
  for (const fixture of ordered) {
    if (!validFixture(fixture) || fixtureIds.has(fixture.id)) {
      throw new PacketReplayError();
    }
    fixtureIds.add(fixture.id);
    await consume(structuredClone(fixture));
  }
}

function sanitizeRecord(
  record: RecordedPacket,
  timestampShiftMs: number,
  aliases: SanitizationAliases,
  syntheticProjector?: SyntheticCaptureProjector,
): SanitizedPacketFixtureEntry {
  const sanitizedFromRadio = sanitizeNormalizedFromRadio(
    record.observation.normalizedFromRadio,
    timestampShiftMs,
    aliases,
    record.sequence,
  );
  const projection = syntheticProjector?.(
    structuredClone(sanitizedFromRadio),
    record.sequence,
  );
  if (
    projection &&
    (projection.frame.length === 0 ||
      projection.frame.length > 65_535 ||
      projection.normalizedFromRadio.kind !== sanitizedFromRadio.kind)
  ) {
    throw new PacketFixtureSanitizerError();
  }
  return {
    id: `fixture-record-${String(record.sequence).padStart(6, "0")}`,
    sanitized: true,
    recording: {
      rawFrameEncoding: "synthetic-hex",
      rawFrameHex: projection
        ? Buffer.from(projection.frame).toString("hex")
        : syntheticFrameHex(record.sequence),
      gatewayId: aliases.gateway(record.gatewayId),
      meshNetworkId: aliases.network(record.meshNetworkId),
      transport: record.transport,
      transportMetadata: {
        connectionStatus: record.transportMetadata.connectionStatus,
        ...(record.transportMetadata.reconnectAttempt === undefined
          ? {}
          : { reconnectAttempt: 0 }),
      },
      sessionConnectedAt: shiftTimestamp(
        record.observation.sessionConnectedAt,
        timestampShiftMs,
      ),
      receivedAt: shiftTimestamp(record.receivedAt, timestampShiftMs),
      ingestedAt: shiftTimestamp(
        record.observation.ingestedAt,
        timestampShiftMs,
      ),
      serverIngestedAt: shiftTimestamp(
        record.observation.serverIngestedAt,
        timestampShiftMs,
      ),
    },
    normalizedFromRadio: projection?.normalizedFromRadio ?? sanitizedFromRadio,
  };
}

function sanitizeNormalizedFromRadio(
  fromRadio: NormalizedFromRadio,
  timestampShiftMs: number,
  aliases: SanitizationAliases,
  sequence: number,
): NormalizedFromRadio {
  const packet = fromRadio.packet;
  return {
    schemaVersion: 1,
    kind: fromRadio.kind,
    ...(fromRadio.fromRadioId === undefined
      ? {}
      : { fromRadioId: aliases.node(fromRadio.fromRadioId) }),
    ...(fromRadio.configCompleteId === undefined
      ? {}
      : { configCompleteId: aliases.packet(fromRadio.configCompleteId) }),
    ...(packet
      ? {
          packet: sanitizeMeshPacket(
            packet,
            timestampShiftMs,
            aliases,
            sequence,
          ),
        }
      : {}),
  };
}

function sanitizeMeshPacket(
  packet: NormalizedMeshPacket,
  timestampShiftMs: number,
  aliases: SanitizationAliases,
  sequence: number,
): NormalizedMeshPacket {
  return {
    ...(packet.sender === undefined
      ? {}
      : { sender: aliases.node(packet.sender) }),
    ...(packet.destination === undefined
      ? {}
      : { destination: aliases.node(packet.destination) }),
    ...(packet.packetId === undefined
      ? {}
      : { packetId: aliases.packet(packet.packetId) }),
    ...(packet.channel === undefined ? {} : { channel: 0 }),
    ...(packet.portNum === undefined ? {} : { portNum: packet.portNum }),
    ...(packet.payloadBase64 === undefined
      ? {}
      : { payloadBase64: syntheticPayloadBase64(sequence, "payload") }),
    ...(packet.encryptedPayloadBase64 === undefined
      ? {}
      : {
          encryptedPayloadBase64: syntheticPayloadBase64(
            sequence,
            "encrypted-payload",
          ),
        }),
    ...(packet.deviceRxTimeSeconds === undefined
      ? {}
      : {
          deviceRxTimeSeconds: shiftSeconds(
            packet.deviceRxTimeSeconds,
            timestampShiftMs,
          ),
        }),
    ...(packet.rxSnr === undefined ? {} : { rxSnr: 0 }),
    ...(packet.rxRssi === undefined ? {} : { rxRssi: -100 }),
    ...(packet.hopLimit === undefined ? {} : { hopLimit: 0 }),
    ...(packet.hopStart === undefined ? {} : { hopStart: 0 }),
    ...(packet.viaMqtt === undefined ? {} : { viaMqtt: false }),
    ...(packet.transportMechanism === undefined
      ? {}
      : { transportMechanism: "synthetic" }),
  };
}

function calculateTimestampShift(records: readonly RecordedPacket[]): number {
  if (records.length === 0) {
    return 0;
  }
  const timestamps = records.flatMap((record) => [
    timestampMilliseconds(record.observation.sessionConnectedAt),
    timestampMilliseconds(record.receivedAt),
    timestampMilliseconds(record.observation.ingestedAt),
    timestampMilliseconds(record.observation.serverIngestedAt),
  ]);
  const earliest = Math.min(...timestamps);
  if (!Number.isFinite(earliest)) {
    throw new PacketFixtureSanitizerError();
  }
  return FIXTURE_EPOCH_MS - earliest;
}

function validateRecordInput(input: PacketRecordInput): void {
  if (
    !input.gatewayId.trim() ||
    !input.meshNetworkId.trim() ||
    input.gatewayId.length > 256 ||
    input.meshNetworkId.length > 256 ||
    input.rawFrame.length === 0 ||
    input.rawFrame.length > 65_535
  ) {
    throw new PacketRecorderError();
  }
  if (
    !validTimestamp(input.receivedAt) ||
    !validTimestamp(input.observation.sessionConnectedAt) ||
    !validTimestamp(input.observation.ingestedAt) ||
    !validTimestamp(input.observation.serverIngestedAt)
  ) {
    throw new PacketRecorderError();
  }
  if (
    input.observation.deviceRxTimeSeconds !== undefined &&
    (!Number.isInteger(input.observation.deviceRxTimeSeconds) ||
      input.observation.deviceRxTimeSeconds < 0 ||
      input.observation.deviceRxTimeSeconds > 4_294_967_295)
  ) {
    throw new PacketRecorderError();
  }
}

function boundedPositiveInteger(value: number, maximum: number): boolean {
  return Number.isSafeInteger(value) && value >= 1 && value <= maximum;
}

function estimateRetainedBytes(record: RecordedPacket): number {
  return (
    record.rawFrame.length +
    Buffer.byteLength(record.gatewayId, "utf8") +
    Buffer.byteLength(record.meshNetworkId, "utf8") +
    Buffer.byteLength(JSON.stringify(record.observation), "utf8") +
    256
  );
}

function cloneRecordInput(input: PacketRecordInput): PacketRecordInput {
  return {
    gatewayId: input.gatewayId,
    meshNetworkId: input.meshNetworkId,
    observation: structuredClone(input.observation),
    rawFrame: new Uint8Array(input.rawFrame),
    receivedAt: input.receivedAt,
    transport: input.transport,
    transportMetadata: { ...input.transportMetadata },
  };
}

function cloneRecordedPacket(record: RecordedPacket): RecordedPacket {
  return { sequence: record.sequence, ...cloneRecordInput(record) };
}

function timestampMilliseconds(value: string): number {
  if (!validTimestamp(value)) {
    throw new PacketFixtureSanitizerError();
  }
  return Date.parse(value);
}

function validTimestamp(value: string): boolean {
  return UTC_ISO_TIMESTAMP.test(value) && Number.isFinite(Date.parse(value));
}

function validFixture(fixture: SanitizedPacketFixtureEntry): boolean {
  return (
    fixture.sanitized &&
    /^fixture-record-\d+$/.test(fixture.id) &&
    /^[0-9a-f]+$/.test(fixture.recording.rawFrameHex) &&
    /^fixture-gateway-[a-z]+$/.test(fixture.recording.gatewayId) &&
    /^fixture-network-[a-z]+$/.test(fixture.recording.meshNetworkId) &&
    validTimestamp(fixture.recording.sessionConnectedAt) &&
    validTimestamp(fixture.recording.receivedAt) &&
    validTimestamp(fixture.recording.ingestedAt) &&
    validTimestamp(fixture.recording.serverIngestedAt)
  );
}

function shiftTimestamp(value: string, shiftMilliseconds: number): string {
  return new Date(
    timestampMilliseconds(value) + shiftMilliseconds,
  ).toISOString();
}

function shiftSeconds(value: number, shiftMilliseconds: number): number {
  const shifted = Math.floor((value * 1_000 + shiftMilliseconds) / 1_000);
  if (!Number.isInteger(shifted) || shifted < 0 || shifted > 4_294_967_295) {
    throw new PacketFixtureSanitizerError();
  }
  return shifted;
}

function syntheticFrameHex(sequence: number): string {
  return createHash("sha256")
    .update(`cmclient-synthetic-frame-v1:${sequence}`)
    .digest("hex");
}

function syntheticPayloadBase64(sequence: number, field: string): string {
  return Buffer.from(`fixture-${field}-${sequence}`, "utf8").toString("base64");
}

class SanitizationAliases {
  private readonly gateways = new Map<string, string>();
  private readonly networks = new Map<string, string>();
  private readonly nodes = new Map<number, number>();
  private readonly packets = new Map<number, number>();

  gateway(value: string): string {
    return this.aliasString(this.gateways, value, "fixture-gateway-");
  }

  network(value: string): string {
    return this.aliasString(this.networks, value, "fixture-network-");
  }

  node(value: number): number {
    return this.aliasNumber(this.nodes, value, 0xf1c7a000);
  }

  packet(value: number): number {
    return this.aliasNumber(this.packets, value, 1_000);
  }

  private aliasString(
    aliases: Map<string, string>,
    value: string,
    prefix: string,
  ): string {
    const existing = aliases.get(value);
    if (existing) {
      return existing;
    }
    const alias = `${prefix}${alphabeticalAlias(aliases.size)}`;
    aliases.set(value, alias);
    return alias;
  }

  private aliasNumber(
    aliases: Map<number, number>,
    value: number,
    offset: number,
  ): number {
    const existing = aliases.get(value);
    if (existing !== undefined) {
      return existing;
    }
    const alias = offset + aliases.size + 1;
    aliases.set(value, alias);
    return alias;
  }
}

function alphabeticalAlias(index: number): string {
  let value = index;
  let alias = "";
  do {
    alias = String.fromCharCode(97 + (value % 26)) + alias;
    value = Math.floor(value / 26) - 1;
  } while (value >= 0);
  return alias;
}
