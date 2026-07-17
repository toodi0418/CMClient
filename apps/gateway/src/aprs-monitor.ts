import net, { type Socket } from "node:net";
import { DatabaseSync } from "node:sqlite";

import type { PositionCanonicalEvent } from "@cmclient/contracts";

export interface AprsMonitorTarget {
  callsign: string;
  mappingVersion: string;
  meshNetworkId: string;
  nodeNum: number;
}

export interface AprsRemotePosition {
  callsign: string;
  eventMarker: string;
  eventTime: string;
}

export interface AprsRemoteHighWaterState extends AprsMonitorTarget {
  latestEventMarker: string;
  latestEventTime: string;
  receivedAt: string;
}

export interface AprsMonitorResult {
  kind: "advanced" | "ignored" | "not_new";
  reason?: "malformed" | "unmapped_callsign";
  remote?: AprsRemotePosition;
  state?: AprsRemoteHighWaterState;
}

export interface AprsIsRxSession {
  close(): Promise<void>;
}

export class AprsMonitorError extends Error {
  readonly code = "APRS_MONITOR_INVALID";

  constructor() {
    super("APRS_MONITOR_INVALID");
  }
}

export class AprsRemoteHighWaterStore {
  constructor(private readonly database: DatabaseSync) {}

  apply(
    remote: AprsRemotePosition,
    target: AprsMonitorTarget,
    receivedAt: string,
  ): { advanced: boolean; state: AprsRemoteHighWaterState } {
    validateTarget(target);
    if (
      remote.callsign !== target.callsign ||
      !isMarker(remote.eventMarker) ||
      !isTimestamp(remote.eventTime) ||
      !isTimestamp(receivedAt)
    ) {
      throw new AprsMonitorError();
    }
    let transactionOpen = false;
    try {
      this.database.exec("BEGIN IMMEDIATE");
      transactionOpen = true;
      const current = this.find(target);
      if (
        current &&
        remote.eventTime.localeCompare(current.latestEventTime) <= 0
      ) {
        this.database.exec("COMMIT");
        transactionOpen = false;
        return { advanced: false, state: current };
      }
      this.database
        .prepare(
          "INSERT INTO aprs_remote_high_water (mesh_network_id, node_num, callsign, mapping_version, latest_event_time, latest_event_marker, received_at) VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(mesh_network_id, node_num, callsign, mapping_version) DO UPDATE SET latest_event_time = excluded.latest_event_time, latest_event_marker = excluded.latest_event_marker, received_at = excluded.received_at",
        )
        .run(
          target.meshNetworkId,
          target.nodeNum,
          target.callsign,
          target.mappingVersion,
          remote.eventTime,
          remote.eventMarker,
          receivedAt,
        );
      const state = this.find(target);
      if (!state) {
        throw new AprsMonitorError();
      }
      this.database.exec("COMMIT");
      transactionOpen = false;
      return { advanced: true, state };
    } catch {
      if (transactionOpen) {
        this.database.exec("ROLLBACK");
      }
      throw new AprsMonitorError();
    }
  }

  find(target: AprsMonitorTarget): AprsRemoteHighWaterState | undefined {
    validateTarget(target);
    const row = this.database
      .prepare(
        "SELECT * FROM aprs_remote_high_water WHERE mesh_network_id = ? AND node_num = ? AND callsign = ? AND mapping_version = ?",
      )
      .get(
        target.meshNetworkId,
        target.nodeNum,
        target.callsign,
        target.mappingVersion,
      );
    return row ? toRemoteState(row) : undefined;
  }

  canUpload(event: PositionCanonicalEvent, target: AprsMonitorTarget): boolean {
    const remote = this.find(target);
    if (!event.eventTime || !isTimestamp(event.eventTime)) {
      return false;
    }
    if (!remote) {
      return true;
    }
    const localMarker = `CM2/${event.canonicalKey.slice(0, 12)}`;
    const localMinute = new Date(event.eventTime);
    localMinute.setUTCSeconds(0, 0);
    if (
      localMarker === remote.latestEventMarker &&
      localMinute.toISOString() === remote.latestEventTime
    ) {
      return true;
    }
    return (
      Date.parse(event.eventTime) >= Date.parse(remote.latestEventTime) + 60_000
    );
  }
}

export class AprsIsMonitor {
  private readonly targetsByCallsign: ReadonlyMap<string, AprsMonitorTarget>;

  constructor(
    targets: readonly AprsMonitorTarget[],
    private readonly highWater: AprsRemoteHighWaterStore,
  ) {
    const targetsByCallsign = new Map<string, AprsMonitorTarget>();
    for (const target of targets) {
      validateTarget(target);
      if (targetsByCallsign.has(target.callsign)) {
        throw new AprsMonitorError();
      }
      targetsByCallsign.set(target.callsign, { ...target });
    }
    this.targetsByCallsign = targetsByCallsign;
  }

  filterExpression(): string {
    if (this.targetsByCallsign.size === 0) {
      throw new AprsMonitorError();
    }
    return `b/${[...this.targetsByCallsign.keys()].join("/")}`;
  }

  observeLine(line: string, receivedAt: string): AprsMonitorResult {
    const remote = parseCmClientAprsLine(line, receivedAt);
    if (!remote) {
      return { kind: "ignored", reason: "malformed" };
    }
    const target = this.targetsByCallsign.get(remote.callsign);
    if (!target) {
      return { kind: "ignored", reason: "unmapped_callsign" };
    }
    const result = this.highWater.apply(remote, target, receivedAt);
    return {
      kind: result.advanced ? "advanced" : "not_new",
      remote,
      state: result.state,
    };
  }
}

export class AprsIsRxClient {
  constructor(
    private readonly options: {
      host: string;
      port: number;
      loginLine: string;
      filterExpression: string;
      timeoutMs?: number;
    },
  ) {
    if (
      !options.host.trim() ||
      !Number.isInteger(options.port) ||
      options.port < 1 ||
      options.port > 65_535 ||
      !options.loginLine.trim() ||
      /[\r\n]/.test(options.loginLine) ||
      !isFilterExpression(options.filterExpression) ||
      (options.timeoutMs !== undefined &&
        (!Number.isFinite(options.timeoutMs) || options.timeoutMs <= 0))
    ) {
      throw new AprsMonitorError();
    }
  }

  async connect(onLine: (line: string) => void): Promise<AprsIsRxSession> {
    const socket = net.createConnection({
      host: this.options.host,
      port: this.options.port,
    });
    try {
      await onceConnected(socket, this.options.timeoutMs ?? 10_000);
      await write(
        socket,
        `${this.options.loginLine} filter ${this.options.filterExpression}\r\n`,
      );
      attachLineReader(socket, onLine);
      return { close: () => closeSocket(socket) };
    } catch {
      socket.destroy();
      throw new AprsMonitorError();
    }
  }
}

export function parseCmClientAprsLine(
  line: string,
  receivedAt: string,
): AprsRemotePosition | undefined {
  if (line.length > 512 || /[\r\n]/.test(line) || !isTimestamp(receivedAt)) {
    return undefined;
  }
  const header =
    /^([A-Z0-9]{1,6}(?:-[0-9]{1,2})?)>[A-Z0-9]{1,6}(?:-[0-9]{1,2})?(?:,[^:\r\n]*)?:(.*)$/.exec(
      line,
    );
  if (!header) {
    return undefined;
  }
  const data = header[2]!;
  const position =
    /^\/(\d{2})(\d{2})(\d{2})z(\d{2})(\d{2}\.\d{2})[NS][ -~](\d{3})(\d{2}\.\d{2})[EW][ -~]/.exec(
      data,
    );
  const marker = /(?:^|\s)(CM2\/[a-f0-9]{12})$/.exec(data);
  if (!position || !marker || !validPosition(position)) {
    return undefined;
  }
  const eventTime = resolveAprsTimestamp(
    Number(position[1]),
    Number(position[2]),
    Number(position[3]),
    receivedAt,
  );
  if (!eventTime) {
    return undefined;
  }
  return {
    callsign: header[1]!,
    eventMarker: marker[1]!,
    eventTime,
  };
}

function validPosition(position: RegExpExecArray): boolean {
  const latitudeDegrees = Number(position[4]);
  const latitudeMinutes = Number(position[5]);
  const longitudeDegrees = Number(position[6]);
  const longitudeMinutes = Number(position[7]);
  return (
    latitudeDegrees <= 90 &&
    latitudeMinutes < 60 &&
    !(latitudeDegrees === 90 && latitudeMinutes > 0) &&
    longitudeDegrees <= 180 &&
    longitudeMinutes < 60 &&
    !(longitudeDegrees === 180 && longitudeMinutes > 0)
  );
}

function resolveAprsTimestamp(
  day: number,
  hour: number,
  minute: number,
  receivedAt: string,
): string | undefined {
  if (day < 1 || day > 31 || hour > 23 || minute > 59) {
    return undefined;
  }
  const received = new Date(receivedAt);
  const candidates = [-1, 0, 1]
    .map(
      (monthOffset) =>
        new Date(
          Date.UTC(
            received.getUTCFullYear(),
            received.getUTCMonth() + monthOffset,
            day,
            hour,
            minute,
          ),
        ),
    )
    .filter((candidate) => candidate.getUTCDate() === day);
  const candidate = candidates.sort(
    (left, right) =>
      Math.abs(left.getTime() - received.getTime()) -
      Math.abs(right.getTime() - received.getTime()),
  )[0];
  if (
    !candidate ||
    Math.abs(candidate.getTime() - received.getTime()) > 36 * 60 * 60 * 1_000
  ) {
    return undefined;
  }
  return candidate.toISOString();
}

function validateTarget(target: AprsMonitorTarget): void {
  if (
    !/^[A-Z0-9]{1,6}(?:-[0-9]{1,2})?$/.test(target.callsign) ||
    !target.mappingVersion.trim() ||
    !target.meshNetworkId.trim() ||
    !Number.isInteger(target.nodeNum) ||
    target.nodeNum < 0 ||
    target.nodeNum > 4_294_967_295
  ) {
    throw new AprsMonitorError();
  }
}

function toRemoteState(row: Record<string, unknown>): AprsRemoteHighWaterState {
  const state: AprsRemoteHighWaterState = {
    callsign: String(row.callsign),
    mappingVersion: String(row.mapping_version),
    meshNetworkId: String(row.mesh_network_id),
    nodeNum: Number(row.node_num),
    latestEventMarker: String(row.latest_event_marker),
    latestEventTime: String(row.latest_event_time),
    receivedAt: String(row.received_at),
  };
  validateTarget(state);
  if (
    !isMarker(state.latestEventMarker) ||
    !isTimestamp(state.latestEventTime) ||
    !isTimestamp(state.receivedAt)
  ) {
    throw new AprsMonitorError();
  }
  return state;
}

function isMarker(value: string): boolean {
  return /^CM2\/[a-f0-9]{12}$/.test(value);
}

function isFilterExpression(value: string): boolean {
  return /^b\/[A-Z0-9]{1,6}(?:-[0-9]{1,2})?(?:\/[A-Z0-9]{1,6}(?:-[0-9]{1,2})?)*$/.test(
    value,
  );
}

function isTimestamp(value: string): boolean {
  return Number.isFinite(Date.parse(value));
}

function onceConnected(socket: Socket, timeoutMs: number): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      socket.destroy();
      reject(new AprsMonitorError());
    }, timeoutMs);
    socket.once("connect", () => {
      clearTimeout(timer);
      resolve();
    });
    socket.once("error", () => {
      clearTimeout(timer);
      reject(new AprsMonitorError());
    });
  });
}

function write(socket: Socket, value: string): Promise<void> {
  return new Promise((resolve, reject) =>
    socket.write(value, (error) => (error ? reject(error) : resolve())),
  );
}

function attachLineReader(
  socket: Socket,
  onLine: (line: string) => void,
): void {
  let buffer = "";
  socket.on("data", (chunk: Buffer) => {
    buffer += chunk.toString("utf8");
    if (buffer.length > 1_024) {
      socket.destroy();
      return;
    }
    let lineEnd = buffer.indexOf("\n");
    while (lineEnd >= 0) {
      const line = buffer.slice(0, lineEnd).replace(/\r$/, "");
      buffer = buffer.slice(lineEnd + 1);
      if (line.length <= 512) {
        onLine(line);
      }
      lineEnd = buffer.indexOf("\n");
    }
  });
}

function closeSocket(socket: Socket): Promise<void> {
  if (socket.destroyed) {
    return Promise.resolve();
  }
  return new Promise((resolve) => {
    socket.once("close", resolve);
    socket.end();
  });
}
