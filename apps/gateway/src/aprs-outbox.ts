import { randomUUID } from "node:crypto";
import net, { type Socket } from "node:net";
import { DatabaseSync } from "node:sqlite";

import type { AprsOutboxEntry as PublicAprsOutboxEntry } from "@cmclient/contracts";

import {
  PROVISION_FINGERPRINT_PATTERN,
  type AprsAuthorizationProvider,
  type AprsConnectionAuthorization,
} from "./aprs-identity.js";

export type AprsOutboxStatus = "queued" | "sending" | "sent" | "failed";

export interface AprsOutboxEntry extends PublicAprsOutboxEntry {
  data: string;
  meshNetworkId?: string;
  nodeNum?: number;
  mappingVersion?: string;
  eventTime?: string;
  sequenceEpoch?: number;
  sequenceNumber?: number;
  provisionFingerprint?: string;
}

export interface AprsOrderSnapshot {
  meshNetworkId: string;
  nodeNum: number;
  mappingVersion: string;
  eventTime?: string;
  sequenceEpoch?: number;
  sequenceNumber?: number;
}

export interface EnqueueAprsInput {
  callsign: string;
  canonicalEventId: string;
  data: string;
  now: string;
  provisionFingerprint: string;
  order?: AprsOrderSnapshot;
}

export interface EnqueueAprsResult {
  created: boolean;
  suppressed: boolean;
  entry?: AprsOutboxEntry;
}

export interface AprsTransport {
  send(data: string, provisionFingerprint: string): Promise<void>;
}

export interface AprsRetryOptions {
  initialDelayMs?: number;
  maximumDelayMs?: number;
}

export interface AprsOutboxWorkerOptions extends AprsRetryOptions {
  authorizationProvider: () => string | undefined;
  clock?: () => Date;
}

interface OrderSnapshot {
  canonicalEventId: string;
  meshNetworkId: string;
  nodeNum: number;
  mappingVersion?: string;
  eventTime?: string;
  sequenceEpoch?: number;
  sequenceNumber?: number;
}

export class AprsOutboxError extends Error {
  readonly code = "APRS_OUTBOX_FAILED";

  constructor() {
    super("APRS_OUTBOX_FAILED");
  }
}

export class AprsAuthorizationError extends Error {
  readonly code = "APRS_PROVISION_UNAVAILABLE";

  constructor() {
    super("APRS_PROVISION_UNAVAILABLE");
    this.name = "AprsAuthorizationError";
  }
}

export class AprsOutboxRepository {
  constructor(private readonly database: DatabaseSync) {}

  enqueue(input: EnqueueAprsInput): EnqueueAprsResult {
    if (
      !/^[A-Z0-9]{1,6}(?:-[0-9]{1,2})?$/.test(input.callsign) ||
      !input.canonicalEventId.trim() ||
      !input.data.trim() ||
      /[\r\n]/.test(input.data) ||
      !isTimestamp(input.now) ||
      !PROVISION_FINGERPRINT_PATTERN.test(input.provisionFingerprint)
    ) {
      throw new AprsOutboxError();
    }
    const snapshot = this.resolveOrderSnapshot(input);
    const id = `aprs-outbox-${randomUUID()}`;
    let savepointOpen = false;
    let disposition: "enqueue" | "conflict";
    try {
      this.database.exec("SAVEPOINT aprs_outbox_enqueue");
      savepointOpen = true;
      const existing = this.findByIdentity(
        input.callsign,
        input.canonicalEventId,
      );
      if (existing) {
        this.database.exec("RELEASE aprs_outbox_enqueue");
        savepointOpen = false;
        return { created: false, suppressed: false, entry: existing };
      }
      const evaluatedDisposition = this.enqueueDisposition(
        snapshot,
        input.callsign,
        input.canonicalEventId,
      );
      if (evaluatedDisposition === "suppress") {
        this.database.exec("RELEASE aprs_outbox_enqueue");
        savepointOpen = false;
        return { created: false, suppressed: true };
      }
      disposition = evaluatedDisposition;
      this.database
        .prepare(
          "DELETE FROM aprs_outbox WHERE mesh_network_id = ? AND node_num = ? AND callsign = ? AND status IN ('queued', 'failed') AND event_time IS NOT NULL AND (event_time < ? OR (event_time = ? AND mapping_version IS NOT NULL AND mapping_version = ? AND sequence_epoch IS NOT NULL AND sequence_number IS NOT NULL AND ? IS NOT NULL AND ? IS NOT NULL AND (sequence_epoch < ? OR (sequence_epoch = ? AND sequence_number < ?))))",
        )
        .run(
          snapshot.meshNetworkId,
          snapshot.nodeNum,
          input.callsign,
          snapshot.eventTime!,
          snapshot.eventTime!,
          snapshot.mappingVersion ?? null,
          snapshot.sequenceEpoch ?? null,
          snapshot.sequenceNumber ?? null,
          snapshot.sequenceEpoch ?? null,
          snapshot.sequenceEpoch ?? null,
          snapshot.sequenceNumber ?? null,
        );
      this.database
        .prepare(
          "INSERT OR IGNORE INTO aprs_outbox (id, callsign, canonical_event_id, data, status, attempts, next_attempt_at, last_error_code, created_at, updated_at, mesh_network_id, node_num, mapping_version, event_time, sequence_epoch, sequence_number, provision_fingerprint) VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .run(
          id,
          input.callsign,
          input.canonicalEventId,
          input.data,
          disposition === "conflict" ? "failed" : "queued",
          input.now,
          disposition === "conflict" ? "APRS_ORDER_UNPROVEN" : null,
          input.now,
          input.now,
          snapshot.meshNetworkId,
          snapshot.nodeNum,
          snapshot.mappingVersion ?? null,
          snapshot.eventTime!,
          snapshot.sequenceEpoch ?? null,
          snapshot.sequenceNumber ?? null,
          input.provisionFingerprint,
        );
      this.database.exec("RELEASE aprs_outbox_enqueue");
      savepointOpen = false;
    } catch {
      if (savepointOpen) {
        this.database.exec("ROLLBACK TO aprs_outbox_enqueue");
        this.database.exec("RELEASE aprs_outbox_enqueue");
      }
      throw new AprsOutboxError();
    }
    const entry = this.findByIdentity(input.callsign, input.canonicalEventId);
    if (!entry) {
      throw new AprsOutboxError();
    }
    return {
      created: entry.id === id,
      suppressed: disposition === "conflict",
      entry,
    };
  }

  claimDue(now: string, limit = 10): AprsOutboxEntry[] {
    if (!isTimestamp(now) || !Number.isInteger(limit) || limit < 1) {
      throw new AprsOutboxError();
    }
    const claimed: AprsOutboxEntry[] = [];
    const claimedIdentities = new Set<string>();
    this.database.exec("BEGIN IMMEDIATE");
    try {
      const rows = this.database
        .prepare(
          "SELECT id FROM aprs_outbox INDEXED BY aprs_outbox_due_order_index WHERE status IN ('queued', 'failed') AND next_attempt_at <= ? ORDER BY next_attempt_at ASC, created_at ASC, id ASC LIMIT ?",
        )
        .all(now, limit);
      for (const row of rows) {
        const id = String(row.id);
        const current = this.find(id);
        if (!current) {
          continue;
        }
        const identity = orderIdentity(current);
        if (!identity || claimedIdentities.has(identity)) {
          continue;
        }
        const activeSending = this.database
          .prepare(
            "SELECT 1 FROM aprs_outbox WHERE mesh_network_id = ? AND node_num = ? AND callsign = ? AND status = 'sending' LIMIT 1",
          )
          .get(current.meshNetworkId!, current.nodeNum!, current.callsign);
        if (activeSending) {
          continue;
        }
        this.database
          .prepare(
            "UPDATE aprs_outbox SET status = 'sending', updated_at = ? WHERE id = ?",
          )
          .run(now, id);
        const entry = this.find(id);
        if (entry) {
          claimed.push(entry);
          claimedIdentities.add(identity);
        }
      }
      this.database.exec("COMMIT");
      return claimed;
    } catch {
      this.database.exec("ROLLBACK");
      throw new AprsOutboxError();
    }
  }

  prepareSend(
    id: string,
    now: string,
    retryDelayMs: number,
    currentProvisionFingerprint: string | undefined,
  ):
    | { authorized: true; entry: AprsOutboxEntry }
    | { authorized: false; entry?: AprsOutboxEntry } {
    if (
      !isTimestamp(now) ||
      !Number.isFinite(retryDelayMs) ||
      retryDelayMs < 0
    ) {
      throw new AprsOutboxError();
    }
    let transactionOpen = false;
    try {
      this.database.exec("BEGIN IMMEDIATE");
      transactionOpen = true;
      const entry = this.required(id);
      if (entry.status !== "sending") {
        throw new AprsOutboxError();
      }
      if (
        !entry.provisionFingerprint ||
        !PROVISION_FINGERPRINT_PATTERN.test(
          currentProvisionFingerprint ?? "",
        ) ||
        entry.provisionFingerprint !== currentProvisionFingerprint
      ) {
        this.database.prepare("DELETE FROM aprs_outbox WHERE id = ?").run(id);
        this.database.exec("COMMIT");
        transactionOpen = false;
        return { authorized: false };
      }
      const order = this.evaluateSendOrder(entry);
      if (order === "stale") {
        this.database.prepare("DELETE FROM aprs_outbox WHERE id = ?").run(id);
        this.database.exec("COMMIT");
        transactionOpen = false;
        return { authorized: false };
      }
      if (order === "ambiguous") {
        const deferred = this.deferUnproven(entry, now, retryDelayMs);
        this.database.exec("COMMIT");
        transactionOpen = false;
        return { authorized: false, entry: deferred };
      }
      this.database.exec("COMMIT");
      transactionOpen = false;
      return { authorized: true, entry };
    } catch {
      if (transactionOpen) {
        this.database.exec("ROLLBACK");
      }
      throw new AprsOutboxError();
    }
  }

  markSent(
    id: string,
    now: string,
    provisionFingerprint: string,
  ): AprsOutboxEntry {
    if (
      !isTimestamp(now) ||
      !PROVISION_FINGERPRINT_PATTERN.test(provisionFingerprint)
    ) {
      throw new AprsOutboxError();
    }
    let transactionOpen = false;
    try {
      this.database.exec("BEGIN IMMEDIATE");
      transactionOpen = true;
      const current = this.required(id);
      if (
        current.status !== "sending" ||
        current.provisionFingerprint !== provisionFingerprint
      ) {
        throw new AprsOutboxError();
      }
      this.database
        .prepare(
          "UPDATE aprs_outbox SET status = 'sent', sent_at = ?, updated_at = ?, last_error_code = NULL WHERE id = ? AND status = 'sending'",
        )
        .run(now, now, id);
      const entry = this.required(id);
      if (entry.status !== "sent" || !hasCompleteSourceSnapshot(entry)) {
        throw new AprsOutboxError();
      }
      this.database
        .prepare(
          "INSERT INTO aprs_delivery_high_water (mesh_network_id, node_num, callsign, latest_canonical_event_id, latest_event_time, latest_sequence_epoch, latest_sequence_number, delivered_at, latest_mapping_version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(mesh_network_id, node_num, callsign) DO UPDATE SET latest_canonical_event_id = excluded.latest_canonical_event_id, latest_event_time = excluded.latest_event_time, latest_sequence_epoch = excluded.latest_sequence_epoch, latest_sequence_number = excluded.latest_sequence_number, delivered_at = excluded.delivered_at, latest_mapping_version = excluded.latest_mapping_version WHERE excluded.latest_event_time > aprs_delivery_high_water.latest_event_time OR (excluded.latest_event_time = aprs_delivery_high_water.latest_event_time AND (excluded.latest_canonical_event_id = aprs_delivery_high_water.latest_canonical_event_id OR (excluded.latest_mapping_version IS NOT NULL AND aprs_delivery_high_water.latest_mapping_version IS NOT NULL AND excluded.latest_mapping_version = aprs_delivery_high_water.latest_mapping_version AND excluded.latest_sequence_epoch IS NOT NULL AND excluded.latest_sequence_number IS NOT NULL AND aprs_delivery_high_water.latest_sequence_epoch IS NOT NULL AND aprs_delivery_high_water.latest_sequence_number IS NOT NULL AND (excluded.latest_sequence_epoch > aprs_delivery_high_water.latest_sequence_epoch OR (excluded.latest_sequence_epoch = aprs_delivery_high_water.latest_sequence_epoch AND excluded.latest_sequence_number > aprs_delivery_high_water.latest_sequence_number)))))",
        )
        .run(
          entry.meshNetworkId,
          entry.nodeNum,
          entry.callsign,
          entry.canonicalEventId,
          entry.eventTime,
          entry.sequenceEpoch ?? null,
          entry.sequenceNumber ?? null,
          now,
          entry.mappingVersion ?? null,
        );
      this.database.exec("COMMIT");
      transactionOpen = false;
      return entry;
    } catch {
      if (transactionOpen) {
        this.database.exec("ROLLBACK");
      }
      throw new AprsOutboxError();
    }
  }

  markFailed(
    id: string,
    now: string,
    retryDelayMs: number,
    errorCode: string,
  ): AprsOutboxEntry | undefined {
    const current = this.required(id);
    if (
      current.status !== "sending" ||
      !isTimestamp(now) ||
      !Number.isFinite(retryDelayMs) ||
      retryDelayMs < 0 ||
      !errorCode.trim() ||
      /[\r\n]/.test(errorCode)
    ) {
      throw new AprsOutboxError();
    }
    let transactionOpen = false;
    try {
      this.database.exec("BEGIN IMMEDIATE");
      transactionOpen = true;
      const order = this.evaluateSendOrder(current);
      if (order === "stale") {
        this.database.prepare("DELETE FROM aprs_outbox WHERE id = ?").run(id);
        this.database.exec("COMMIT");
        transactionOpen = false;
        return undefined;
      }
      const nextAttemptAt = new Date(
        Date.parse(now) + retryDelayMs,
      ).toISOString();
      this.database
        .prepare(
          "UPDATE aprs_outbox SET status = 'failed', attempts = ?, next_attempt_at = ?, last_error_code = ?, updated_at = ? WHERE id = ? AND status = 'sending'",
        )
        .run(current.attempts + 1, nextAttemptAt, errorCode, now, id);
      const entry = this.required(id);
      if (entry.status !== "failed") {
        throw new AprsOutboxError();
      }
      this.database.exec("COMMIT");
      transactionOpen = false;
      return entry;
    } catch {
      if (transactionOpen) {
        this.database.exec("ROLLBACK");
      }
      throw new AprsOutboxError();
    }
  }

  resumeInterrupted(now: string): number {
    if (!isTimestamp(now)) {
      throw new AprsOutboxError();
    }
    const result = this.database
      .prepare(
        "UPDATE aprs_outbox SET status = 'failed', attempts = attempts + 1, next_attempt_at = ?, last_error_code = 'APRS_TX_INTERRUPTED', updated_at = ? WHERE status = 'sending'",
      )
      .run(now, now);
    return Number(result.changes);
  }

  discard(id: string): void {
    try {
      this.database.prepare("DELETE FROM aprs_outbox WHERE id = ?").run(id);
    } catch {
      throw new AprsOutboxError();
    }
  }

  find(id: string): AprsOutboxEntry | undefined {
    const row = this.database
      .prepare("SELECT * FROM aprs_outbox WHERE id = ?")
      .get(id);
    return row ? toEntry(row) : undefined;
  }

  findByIdentity(
    callsign: string,
    canonicalEventId: string,
  ): AprsOutboxEntry | undefined {
    const row = this.database
      .prepare(
        "SELECT * FROM aprs_outbox WHERE callsign = ? AND canonical_event_id = ?",
      )
      .get(callsign, canonicalEventId);
    return row ? toEntry(row) : undefined;
  }

  list(limit: number): PublicAprsOutboxEntry[] {
    if (!Number.isInteger(limit) || limit < 1 || limit > 200) {
      throw new AprsOutboxError();
    }
    return this.database
      .prepare(
        "SELECT * FROM aprs_outbox ORDER BY updated_at DESC, id ASC LIMIT ?",
      )
      .all(limit)
      .map((row) => publicEntry(toEntry(row as Record<string, unknown>)));
  }

  deleteSentBefore(cutoffExclusive: string, limit = 1_000): number {
    if (
      !isTimestamp(cutoffExclusive) ||
      !Number.isInteger(limit) ||
      limit < 1 ||
      limit > 10_000
    ) {
      throw new AprsOutboxError();
    }
    try {
      const result = this.database
        .prepare(
          "DELETE FROM aprs_outbox WHERE id IN (SELECT outbox.id FROM aprs_outbox AS outbox INDEXED BY aprs_outbox_sent_retention_index WHERE outbox.status = 'sent' AND outbox.sent_at IS NOT NULL AND outbox.sent_at < ? AND EXISTS (SELECT 1 FROM aprs_delivery_high_water AS delivery WHERE delivery.mesh_network_id = outbox.mesh_network_id AND delivery.node_num = outbox.node_num AND delivery.callsign = outbox.callsign AND outbox.event_time IS NOT NULL AND (delivery.latest_canonical_event_id = outbox.canonical_event_id OR delivery.latest_event_time > outbox.event_time OR (delivery.latest_event_time = outbox.event_time AND delivery.latest_mapping_version IS NOT NULL AND outbox.mapping_version IS NOT NULL AND delivery.latest_mapping_version = outbox.mapping_version AND delivery.latest_sequence_epoch IS NOT NULL AND delivery.latest_sequence_number IS NOT NULL AND outbox.sequence_epoch IS NOT NULL AND outbox.sequence_number IS NOT NULL AND (delivery.latest_sequence_epoch > outbox.sequence_epoch OR (delivery.latest_sequence_epoch = outbox.sequence_epoch AND delivery.latest_sequence_number > outbox.sequence_number))))) ORDER BY outbox.sent_at ASC, outbox.id ASC LIMIT ?)",
        )
        .run(cutoffExclusive, limit);
      return Number(result.changes);
    } catch {
      throw new AprsOutboxError();
    }
  }

  deleteSuperseded(limit = 1_000): number {
    if (!Number.isInteger(limit) || limit < 1 || limit > 10_000) {
      throw new AprsOutboxError();
    }
    try {
      const result = this.database
        .prepare(
          "DELETE FROM aprs_outbox WHERE id IN (SELECT older.id FROM aprs_outbox AS older WHERE older.status IN ('queued', 'failed') AND (EXISTS (SELECT 1 FROM aprs_delivery_high_water AS delivery WHERE delivery.mesh_network_id = older.mesh_network_id AND delivery.node_num = older.node_num AND delivery.callsign = older.callsign AND older.event_time IS NOT NULL AND (delivery.latest_canonical_event_id = older.canonical_event_id OR delivery.latest_event_time > older.event_time OR (delivery.latest_event_time = older.event_time AND delivery.latest_mapping_version IS NOT NULL AND older.mapping_version IS NOT NULL AND delivery.latest_mapping_version = older.mapping_version AND delivery.latest_sequence_epoch IS NOT NULL AND delivery.latest_sequence_number IS NOT NULL AND older.sequence_epoch IS NOT NULL AND older.sequence_number IS NOT NULL AND (delivery.latest_sequence_epoch > older.sequence_epoch OR (delivery.latest_sequence_epoch = older.sequence_epoch AND delivery.latest_sequence_number > older.sequence_number))))) OR EXISTS (SELECT 1 FROM aprs_outbox AS newer INDEXED BY aprs_outbox_active_order_index WHERE newer.mesh_network_id = older.mesh_network_id AND newer.node_num = older.node_num AND newer.callsign = older.callsign AND newer.id <> older.id AND newer.status IN ('queued', 'sending', 'failed') AND newer.event_time IS NOT NULL AND older.event_time IS NOT NULL AND (newer.event_time > older.event_time OR (newer.event_time = older.event_time AND newer.mapping_version IS NOT NULL AND older.mapping_version IS NOT NULL AND newer.mapping_version = older.mapping_version AND newer.sequence_epoch IS NOT NULL AND newer.sequence_number IS NOT NULL AND older.sequence_epoch IS NOT NULL AND older.sequence_number IS NOT NULL AND (newer.sequence_epoch > older.sequence_epoch OR (newer.sequence_epoch = older.sequence_epoch AND newer.sequence_number > older.sequence_number)))))) ORDER BY older.updated_at ASC, older.id ASC LIMIT ?)",
        )
        .run(limit);
      return Number(result.changes);
    } catch {
      throw new AprsOutboxError();
    }
  }

  private required(id: string): AprsOutboxEntry {
    const entry = this.find(id);
    if (!entry) {
      throw new AprsOutboxError();
    }
    return entry;
  }

  private resolveOrderSnapshot(input: EnqueueAprsInput): OrderSnapshot {
    const event = this.database
      .prepare(
        "SELECT mesh_network_id, node_num, event_time, sequence_epoch, sequence_number FROM position_events WHERE id = ?",
      )
      .get(input.canonicalEventId);
    if (!event) {
      throw new AprsOutboxError();
    }
    const canonicalSequenceEpoch = optionalInteger(event.sequence_epoch);
    const canonicalSequenceNumber = optionalInteger(event.sequence_number);
    const eventTime = optionalTimestamp(event.event_time);
    const snapshot: OrderSnapshot = input.order
      ? {
          canonicalEventId: input.canonicalEventId,
          meshNetworkId: input.order.meshNetworkId,
          nodeNum: input.order.nodeNum,
          mappingVersion: input.order.mappingVersion,
          ...(input.order.eventTime
            ? { eventTime: input.order.eventTime }
            : {}),
          ...(input.order.sequenceEpoch === undefined
            ? {}
            : { sequenceEpoch: input.order.sequenceEpoch }),
          ...(input.order.sequenceNumber === undefined
            ? {}
            : { sequenceNumber: input.order.sequenceNumber }),
        }
      : {
          canonicalEventId: input.canonicalEventId,
          meshNetworkId: String(event.mesh_network_id),
          nodeNum: Number(event.node_num),
          ...(eventTime ? { eventTime } : {}),
          ...(typeof canonicalSequenceEpoch === "number"
            ? { sequenceEpoch: canonicalSequenceEpoch }
            : {}),
          ...(typeof canonicalSequenceNumber === "number"
            ? { sequenceNumber: canonicalSequenceNumber }
            : {}),
        };
    if (
      !validOrderSnapshot(snapshot) ||
      snapshot.meshNetworkId !== String(event.mesh_network_id) ||
      snapshot.nodeNum !== Number(event.node_num) ||
      snapshot.eventTime !== eventTime ||
      snapshot.sequenceNumber !==
        (typeof canonicalSequenceNumber === "number"
          ? canonicalSequenceNumber
          : undefined)
    ) {
      throw new AprsOutboxError();
    }
    return snapshot;
  }

  private enqueueDisposition(
    snapshot: OrderSnapshot,
    callsign: string,
    canonicalEventId: string,
  ): "enqueue" | "conflict" | "suppress" {
    const delivery = this.findDelivery(snapshot, callsign);
    if (delivery) {
      const comparison = compareOrder(snapshot, delivery);
      if (
        delivery.canonicalEventId === canonicalEventId ||
        comparison !== "newer"
      ) {
        return "suppress";
      }
    }
    const active = this.database
      .prepare(
        "SELECT * FROM aprs_outbox WHERE mesh_network_id = ? AND node_num = ? AND callsign = ? AND status IN ('queued', 'sending', 'failed')",
      )
      .all(snapshot.meshNetworkId, snapshot.nodeNum, callsign)
      .map((row) => toEntry(row as Record<string, unknown>));
    let ambiguous = false;
    for (const entry of active) {
      const comparison = compareOrder(snapshot, entryOrder(entry));
      if (comparison === "older") {
        return "suppress";
      }
      if (comparison !== "newer") {
        ambiguous = true;
      }
    }
    if (!ambiguous) {
      return "enqueue";
    }
    return active.length < 2 ? "conflict" : "suppress";
  }

  private evaluateSendOrder(
    entry: AprsOutboxEntry,
  ): "current" | "stale" | "ambiguous" {
    const snapshot = entryOrder(entry);
    if (!validOrderSnapshot(snapshot)) {
      return "ambiguous";
    }
    const remoteOrder = this.evaluateRemoteSendOrder(snapshot, entry.callsign);
    if (remoteOrder === "stale") {
      return "stale";
    }
    let ambiguous = remoteOrder === "ambiguous";
    const delivery = this.findDelivery(snapshot, entry.callsign);
    if (delivery) {
      if (delivery.canonicalEventId === entry.canonicalEventId) {
        return "stale";
      }
      const comparison = compareOrder(snapshot, delivery);
      if (comparison === "older") {
        return "stale";
      }
      if (comparison !== "newer") {
        return "ambiguous";
      }
    }
    const active = this.database
      .prepare(
        "SELECT * FROM aprs_outbox WHERE mesh_network_id = ? AND node_num = ? AND callsign = ? AND id <> ? AND status IN ('queued', 'sending', 'failed')",
      )
      .all(snapshot.meshNetworkId, snapshot.nodeNum, entry.callsign, entry.id)
      .map((row) => toEntry(row as Record<string, unknown>));
    for (const other of active) {
      const comparison = compareOrder(snapshot, entryOrder(other));
      if (comparison === "older") {
        return "stale";
      }
      if (comparison !== "newer") {
        ambiguous = true;
      }
    }
    return ambiguous ? "ambiguous" : "current";
  }

  private evaluateRemoteSendOrder(
    snapshot: OrderSnapshot,
    callsign: string,
  ): "current" | "stale" | "ambiguous" {
    const event = this.database
      .prepare("SELECT canonical_key FROM position_events WHERE id = ?")
      .get(snapshot.canonicalEventId);
    const canonicalKey = event?.canonical_key;
    if (
      typeof canonicalKey !== "string" ||
      !/^[a-f0-9]{64}$/.test(canonicalKey)
    ) {
      return "ambiguous";
    }
    const rows = this.database
      .prepare(
        "SELECT latest_event_time, latest_event_marker FROM aprs_remote_high_water WHERE mesh_network_id = ? AND node_num = ? AND callsign = ?",
      )
      .all(snapshot.meshNetworkId, snapshot.nodeNum, callsign);
    if (rows.length === 0) {
      return "current";
    }
    const eventTime = Date.parse(snapshot.eventTime!);
    const localMinute = new Date(eventTime);
    localMinute.setUTCSeconds(0, 0);
    const eventMarker = `CM2/${canonicalKey.slice(0, 12)}`;
    let ambiguous = false;
    for (const row of rows) {
      const remoteEventTime = row.latest_event_time;
      const remoteEventMarker = row.latest_event_marker;
      if (
        typeof remoteEventTime !== "string" ||
        !isTimestamp(remoteEventTime) ||
        typeof remoteEventMarker !== "string" ||
        !/^CM2\/[a-f0-9]{12}$/.test(remoteEventMarker)
      ) {
        ambiguous = true;
        continue;
      }
      if (
        remoteEventMarker === eventMarker &&
        remoteEventTime === localMinute.toISOString()
      ) {
        continue;
      }
      const remoteTime = Date.parse(remoteEventTime);
      if (eventTime < remoteTime) {
        return "stale";
      }
      if (eventTime < remoteTime + 60_000) {
        ambiguous = true;
      }
    }
    return ambiguous ? "ambiguous" : "current";
  }

  private findDelivery(
    snapshot: OrderSnapshot,
    callsign: string,
  ): OrderSnapshot | undefined {
    const row = this.database
      .prepare(
        "SELECT latest_canonical_event_id, latest_mapping_version, latest_event_time, latest_sequence_epoch, latest_sequence_number FROM aprs_delivery_high_water WHERE mesh_network_id = ? AND node_num = ? AND callsign = ?",
      )
      .get(snapshot.meshNetworkId, snapshot.nodeNum, callsign);
    if (!row) {
      return undefined;
    }
    const sequenceEpoch = optionalInteger(row.latest_sequence_epoch);
    const sequenceNumber = optionalInteger(row.latest_sequence_number);
    return {
      canonicalEventId: String(row.latest_canonical_event_id),
      meshNetworkId: snapshot.meshNetworkId,
      nodeNum: snapshot.nodeNum,
      ...(typeof row.latest_mapping_version === "string"
        ? { mappingVersion: row.latest_mapping_version }
        : {}),
      ...(optionalTimestamp(row.latest_event_time)
        ? { eventTime: optionalTimestamp(row.latest_event_time)! }
        : {}),
      ...(typeof sequenceEpoch === "number" ? { sequenceEpoch } : {}),
      ...(typeof sequenceNumber === "number" ? { sequenceNumber } : {}),
    };
  }

  private deferUnproven(
    entry: AprsOutboxEntry,
    now: string,
    retryDelayMs: number,
  ): AprsOutboxEntry {
    const nextAttemptAt = new Date(
      Date.parse(now) + retryDelayMs,
    ).toISOString();
    this.database
      .prepare(
        "UPDATE aprs_outbox SET status = 'failed', attempts = ?, next_attempt_at = ?, last_error_code = 'APRS_ORDER_UNPROVEN', updated_at = ? WHERE id = ? AND status = 'sending'",
      )
      .run(entry.attempts + 1, nextAttemptAt, now, entry.id);
    return this.required(entry.id);
  }
}

function publicEntry(entry: AprsOutboxEntry): PublicAprsOutboxEntry {
  return {
    id: entry.id,
    callsign: entry.callsign,
    canonicalEventId: entry.canonicalEventId,
    status: entry.status,
    attempts: entry.attempts,
    nextAttemptAt: entry.nextAttemptAt,
    createdAt: entry.createdAt,
    updatedAt: entry.updatedAt,
    ...(entry.lastErrorCode ? { lastErrorCode: entry.lastErrorCode } : {}),
    ...(entry.sentAt ? { sentAt: entry.sentAt } : {}),
  };
}

export class AprsOutboxWorker {
  private readonly clock: () => Date;
  private readonly retry: Required<AprsRetryOptions>;
  private activeFlush: Promise<AprsOutboxEntry[]> | undefined;

  constructor(
    private readonly repository: AprsOutboxRepository,
    private readonly transport: AprsTransport,
    private readonly options: AprsOutboxWorkerOptions,
  ) {
    this.clock = options.clock ?? (() => new Date());
    this.retry = {
      initialDelayMs: options.initialDelayMs ?? 1_000,
      maximumDelayMs: options.maximumDelayMs ?? 60_000,
    };
    if (
      !Number.isFinite(this.retry.initialDelayMs) ||
      this.retry.initialDelayMs <= 0 ||
      !Number.isFinite(this.retry.maximumDelayMs) ||
      this.retry.maximumDelayMs < this.retry.initialDelayMs
    ) {
      throw new AprsOutboxError();
    }
  }

  flush(limit?: number): Promise<AprsOutboxEntry[]> {
    if (this.activeFlush) {
      return this.activeFlush;
    }
    const operation = this.runFlush(limit);
    this.activeFlush = operation;
    void operation.then(
      () => this.completeFlush(operation),
      () => this.completeFlush(operation),
    );
    return operation;
  }

  private async runFlush(limit?: number): Promise<AprsOutboxEntry[]> {
    const now = this.clock().toISOString();
    this.repository.resumeInterrupted(now);
    this.repository.deleteSuperseded(1_000);
    const entries = this.repository.claimDue(now, limit);
    const results: AprsOutboxEntry[] = [];
    for (const entry of entries) {
      const retryDelayMs = retryDelay(entry.attempts + 1, this.retry);
      const currentFingerprint = resolveCurrentProvisionFingerprint(
        this.options.authorizationProvider,
      );
      const authorization = this.repository.prepareSend(
        entry.id,
        this.clock().toISOString(),
        retryDelayMs,
        currentFingerprint,
      );
      if (!authorization.authorized) {
        if (authorization.entry) {
          results.push(authorization.entry);
        }
        continue;
      }
      try {
        const provisionFingerprint = authorization.entry.provisionFingerprint;
        if (!provisionFingerprint) {
          this.repository.discard(authorization.entry.id);
          continue;
        }
        await this.transport.send(
          authorization.entry.data,
          provisionFingerprint,
        );
        results.push(
          this.repository.markSent(
            authorization.entry.id,
            this.clock().toISOString(),
            provisionFingerprint,
          ),
        );
      } catch (error) {
        const current = resolveCurrentProvisionFingerprint(
          this.options.authorizationProvider,
        );
        if (
          error instanceof AprsAuthorizationError ||
          !authorization.entry.provisionFingerprint ||
          current !== authorization.entry.provisionFingerprint
        ) {
          this.repository.discard(authorization.entry.id);
          continue;
        }
        const failed = this.repository.markFailed(
          authorization.entry.id,
          this.clock().toISOString(),
          retryDelayMs,
          "APRS_TX_FAILED",
        );
        if (failed) {
          results.push(failed);
        }
      }
    }
    return results;
  }

  private completeFlush(operation: Promise<AprsOutboxEntry[]>): void {
    if (this.activeFlush === operation) {
      this.activeFlush = undefined;
    }
  }
}

export class AprsIsTcpClient implements AprsTransport {
  constructor(
    private readonly options: {
      host: string;
      port: number;
      authorizationProvider: AprsAuthorizationProvider;
      timeoutMs?: number;
    },
  ) {
    if (
      !options.host.trim() ||
      !Number.isInteger(options.port) ||
      options.port < 1 ||
      options.port > 65_535 ||
      typeof options.authorizationProvider !== "function" ||
      (options.timeoutMs !== undefined &&
        (!Number.isFinite(options.timeoutMs) || options.timeoutMs <= 0))
    ) {
      throw new AprsOutboxError();
    }
  }

  async send(data: string, provisionFingerprint: string): Promise<void> {
    if (
      !data.trim() ||
      /[\r\n]/.test(data) ||
      !PROVISION_FINGERPRINT_PATTERN.test(provisionFingerprint)
    ) {
      throw new AprsOutboxError();
    }
    resolveAuthorization(
      this.options.authorizationProvider,
      provisionFingerprint,
    );
    const socket = net.createConnection({
      host: this.options.host,
      port: this.options.port,
    });
    try {
      await onceConnected(socket, this.options.timeoutMs ?? 10_000);
      const authorization = resolveAuthorization(
        this.options.authorizationProvider,
        provisionFingerprint,
      );
      await write(socket, `${authorization.loginLine}\r\n`);
      resolveAuthorization(
        this.options.authorizationProvider,
        provisionFingerprint,
      );
      await write(socket, `${data}\r\n`);
      socket.end();
    } catch (error) {
      socket.destroy();
      if (error instanceof AprsAuthorizationError) {
        throw error;
      }
      throw new AprsOutboxError();
    }
  }
}

function retryDelay(
  attempt: number,
  retry: Required<AprsRetryOptions>,
): number {
  return Math.min(
    retry.maximumDelayMs,
    retry.initialDelayMs * 2 ** Math.max(0, attempt - 1),
  );
}

function onceConnected(socket: Socket, timeoutMs: number): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      socket.destroy();
      reject(new AprsOutboxError());
    }, timeoutMs);
    socket.once("connect", () => {
      clearTimeout(timer);
      resolve();
    });
    socket.once("error", () => {
      clearTimeout(timer);
      reject(new AprsOutboxError());
    });
  });
}

function write(socket: Socket, value: string): Promise<void> {
  return new Promise((resolve, reject) =>
    socket.write(value, (error) => (error ? reject(error) : resolve())),
  );
}

const MAX_APRS_LOGIN_LINE_BYTES = 512;

function resolveAuthorization(
  provider: AprsAuthorizationProvider,
  expectedProvisionFingerprint: string,
): AprsConnectionAuthorization {
  let authorization: AprsConnectionAuthorization | undefined;
  try {
    authorization = provider();
  } catch {
    throw new AprsAuthorizationError();
  }
  if (
    !authorization ||
    authorization.provisionFingerprint !== expectedProvisionFingerprint ||
    !PROVISION_FINGERPRINT_PATTERN.test(authorization.provisionFingerprint) ||
    !isValidLoginLine(authorization.loginLine)
  ) {
    throw new AprsAuthorizationError();
  }
  return authorization;
}

function isValidLoginLine(value: unknown): value is string {
  return (
    typeof value === "string" &&
    value.trim().length > 0 &&
    !/[\r\n]/.test(value) &&
    Buffer.byteLength(value, "utf8") <= MAX_APRS_LOGIN_LINE_BYTES
  );
}

function resolveCurrentProvisionFingerprint(
  provider: () => string | undefined,
): string | undefined {
  try {
    const fingerprint = provider();
    return fingerprint && PROVISION_FINGERPRINT_PATTERN.test(fingerprint)
      ? fingerprint
      : undefined;
  } catch {
    return undefined;
  }
}

function toEntry(row: Record<string, unknown>): AprsOutboxEntry {
  const status = String(row.status);
  if (!["queued", "sending", "sent", "failed"].includes(status)) {
    throw new AprsOutboxError();
  }
  const nodeNum = optionalInteger(row.node_num);
  const sequenceEpoch = optionalInteger(row.sequence_epoch);
  const sequenceNumber = optionalInteger(row.sequence_number);
  if (
    nodeNum === "invalid" ||
    sequenceEpoch === "invalid" ||
    sequenceNumber === "invalid"
  ) {
    throw new AprsOutboxError();
  }
  return {
    id: String(row.id),
    callsign: String(row.callsign),
    canonicalEventId: String(row.canonical_event_id),
    data: String(row.data),
    status: status as AprsOutboxStatus,
    attempts: Number(row.attempts),
    nextAttemptAt: String(row.next_attempt_at),
    createdAt: String(row.created_at),
    updatedAt: String(row.updated_at),
    ...(typeof row.mesh_network_id === "string"
      ? { meshNetworkId: row.mesh_network_id }
      : {}),
    ...(typeof nodeNum === "number" ? { nodeNum } : {}),
    ...(typeof row.mapping_version === "string"
      ? { mappingVersion: row.mapping_version }
      : {}),
    ...(typeof row.event_time === "string"
      ? { eventTime: row.event_time }
      : {}),
    ...(typeof sequenceEpoch === "number" ? { sequenceEpoch } : {}),
    ...(typeof sequenceNumber === "number" ? { sequenceNumber } : {}),
    ...(typeof row.provision_fingerprint === "string" &&
    PROVISION_FINGERPRINT_PATTERN.test(row.provision_fingerprint)
      ? { provisionFingerprint: row.provision_fingerprint }
      : {}),
    ...(typeof row.last_error_code === "string"
      ? { lastErrorCode: row.last_error_code }
      : {}),
    ...(typeof row.sent_at === "string" ? { sentAt: row.sent_at } : {}),
  };
}

function isTimestamp(value: string): boolean {
  return Number.isFinite(Date.parse(value));
}

function optionalTimestamp(value: unknown): string | undefined {
  return typeof value === "string" && isTimestamp(value) ? value : undefined;
}

function optionalInteger(value: unknown): number | undefined | "invalid" {
  if (value === null || value === undefined) {
    return undefined;
  }
  const number = Number(value);
  return Number.isSafeInteger(number) && number >= 0 ? number : "invalid";
}

function entryOrder(entry: AprsOutboxEntry): OrderSnapshot {
  return {
    canonicalEventId: entry.canonicalEventId,
    meshNetworkId: entry.meshNetworkId ?? "",
    nodeNum: entry.nodeNum ?? -1,
    ...(entry.mappingVersion ? { mappingVersion: entry.mappingVersion } : {}),
    ...(entry.eventTime ? { eventTime: entry.eventTime } : {}),
    ...(entry.sequenceEpoch === undefined
      ? {}
      : { sequenceEpoch: entry.sequenceEpoch }),
    ...(entry.sequenceNumber === undefined
      ? {}
      : { sequenceNumber: entry.sequenceNumber }),
  };
}

function orderIdentity(entry: AprsOutboxEntry): string | undefined {
  return entry.meshNetworkId !== undefined && entry.nodeNum !== undefined
    ? `${entry.meshNetworkId}\u0000${entry.nodeNum}\u0000${entry.callsign}`
    : undefined;
}

function hasCompleteSourceSnapshot(
  entry: AprsOutboxEntry,
): entry is AprsOutboxEntry & {
  meshNetworkId: string;
  nodeNum: number;
  eventTime: string;
} {
  return validOrderSnapshot(entryOrder(entry));
}

function validOrderSnapshot(snapshot: OrderSnapshot): boolean {
  return (
    Boolean(snapshot.canonicalEventId.trim()) &&
    Boolean(snapshot.meshNetworkId.trim()) &&
    Number.isInteger(snapshot.nodeNum) &&
    snapshot.nodeNum >= 0 &&
    snapshot.nodeNum <= 4_294_967_295 &&
    (snapshot.mappingVersion === undefined ||
      Boolean(snapshot.mappingVersion.trim())) &&
    snapshot.eventTime !== undefined &&
    isTimestamp(snapshot.eventTime) &&
    (snapshot.sequenceEpoch === undefined ||
      (Number.isSafeInteger(snapshot.sequenceEpoch) &&
        snapshot.sequenceEpoch >= 0)) &&
    (snapshot.sequenceNumber === undefined ||
      (Number.isSafeInteger(snapshot.sequenceNumber) &&
        snapshot.sequenceNumber >= 0 &&
        snapshot.sequenceNumber <= 4_294_967_295))
  );
}

function compareOrder(
  left: OrderSnapshot,
  right: OrderSnapshot,
): "newer" | "older" | "same" | "ambiguous" {
  if (!validOrderSnapshot(left) || !validOrderSnapshot(right)) {
    return "ambiguous";
  }
  const timeOrder = left.eventTime!.localeCompare(right.eventTime!);
  if (timeOrder > 0) {
    return "newer";
  }
  if (timeOrder < 0) {
    return "older";
  }
  if (left.canonicalEventId === right.canonicalEventId) {
    return "same";
  }
  if (
    left.mappingVersion === undefined ||
    right.mappingVersion === undefined ||
    left.mappingVersion !== right.mappingVersion ||
    left.sequenceEpoch === undefined ||
    right.sequenceEpoch === undefined ||
    left.sequenceNumber === undefined ||
    right.sequenceNumber === undefined
  ) {
    return "ambiguous";
  }
  if (left.sequenceEpoch > right.sequenceEpoch) {
    return "newer";
  }
  if (left.sequenceEpoch < right.sequenceEpoch) {
    return "older";
  }
  if (left.sequenceNumber > right.sequenceNumber) {
    return "newer";
  }
  if (left.sequenceNumber < right.sequenceNumber) {
    return "older";
  }
  return "ambiguous";
}
