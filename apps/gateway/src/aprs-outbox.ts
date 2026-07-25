import { randomUUID } from "node:crypto";
import net, { type Socket } from "node:net";
import { DatabaseSync } from "node:sqlite";

import type { AprsOutboxEntry as PublicAprsOutboxEntry } from "@cmclient/contracts";

import {
  PROVISION_FINGERPRINT_PATTERN,
  type AprsAuthorizationProvider,
  type AprsConnectionAuthorization,
} from "./aprs-identity.js";
import { parseCmClientAprsLine } from "./aprs-monitor.js";

export type AprsOutboxStatus = "queued" | "sending" | "sent" | "failed";
export type AprsDeliveryStatus =
  | "queued"
  | "sending"
  | "failed"
  | "submitted"
  | "observer_confirmed"
  | "observation_expired";

const APRS_OBSERVATION_WINDOW_MS = 3 * 60 * 60 * 1_000;
const MAX_APRS_DATA_BYTES = 510;

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
  prepareVerifiedSession?(
    provisionFingerprint: string,
  ): Promise<AprsVerifiedTransportSession>;
  send(
    data: string,
    provisionFingerprint: string,
    expectedSession?: AprsVerifiedTransportSession,
    transmissionGate?: () => boolean,
  ): Promise<void>;
  close?(): Promise<void>;
}

export interface AprsVerifiedTransportSession {
  readonly generation: number;
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

export class AprsTransmissionFencedError extends Error {
  readonly code = "APRS_TX_FENCED";

  constructor() {
    super("APRS_TX_FENCED");
    this.name = "AprsTransmissionFencedError";
  }
}

export class AprsOutboxRepository {
  constructor(private readonly database: DatabaseSync) {}

  enqueue(input: EnqueueAprsInput): EnqueueAprsResult {
    if (
      !/^[A-Z0-9]{1,6}(?:-[0-9]{1,2})?$/.test(input.callsign) ||
      !input.canonicalEventId.trim() ||
      !isValidAprsData(input.data) ||
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
          "INSERT OR IGNORE INTO aprs_outbox (id, callsign, canonical_event_id, data, status, delivery_status, attempts, next_attempt_at, last_error_code, created_at, updated_at, mesh_network_id, node_num, mapping_version, event_time, sequence_epoch, sequence_number, provision_fingerprint) VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .run(
          id,
          input.callsign,
          input.canonicalEventId,
          input.data,
          disposition === "conflict" ? "failed" : "queued",
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
            "UPDATE aprs_outbox SET status = 'sending', delivery_status = 'sending', updated_at = ? WHERE id = ?",
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
      if (!isValidAprsData(entry.data)) {
        this.database.prepare("DELETE FROM aprs_outbox WHERE id = ?").run(id);
        this.database.exec("COMMIT");
        transactionOpen = false;
        return { authorized: false };
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
      if (this.isRecentExactDuplicate(entry, now)) {
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

  markSubmitted(
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
      const packet = parseCmClientAprsLine(current.data);
      if (!packet || packet.callsign !== current.callsign) {
        throw new AprsOutboxError();
      }
      this.database
        .prepare(
          "INSERT INTO aprs_local_transmissions (callsign, destination, info, transmitted_at) VALUES (?, ?, ?, ?) ON CONFLICT(callsign, destination, info) DO UPDATE SET transmitted_at = excluded.transmitted_at",
        )
        .run(packet.callsign, packet.destination, packet.info, now);
      const observationExpiresAt = new Date(
        Date.parse(now) + APRS_OBSERVATION_WINDOW_MS,
      ).toISOString();
      this.database
        .prepare(
          "UPDATE aprs_outbox SET status = 'sent', delivery_status = 'submitted', sent_at = ?, submitted_at = ?, observation_expires_at = ?, observer_confirmed_at = NULL, updated_at = ?, last_error_code = NULL WHERE id = ? AND status = 'sending'",
        )
        .run(now, now, observationExpiresAt, now, id);
      const evidence = this.database
        .prepare(
          "SELECT first_observed_at, last_observed_at FROM aprs_observed_packets WHERE callsign = ? AND destination = ? AND info = ?",
        )
        .get(packet.callsign, packet.destination, packet.info) as
        Record<string, unknown> | undefined;
      const observerConfirmedAt = evidence
        ? [
            String(evidence.first_observed_at),
            String(evidence.last_observed_at),
          ]
            .filter(
              (observedAt, index, values) =>
                values.indexOf(observedAt) === index &&
                isTimestamp(observedAt) &&
                Date.parse(observedAt) >= Date.parse(current.updatedAt) &&
                Date.parse(observedAt) <= Date.parse(observationExpiresAt),
            )
            .sort()[0]
        : undefined;
      const exactPending = this.database
        .prepare(
          "SELECT * FROM aprs_outbox WHERE callsign = ? AND provision_fingerprint = ? AND delivery_status IN ('sending', 'submitted') ORDER BY created_at ASC, id ASC",
        )
        .all(packet.callsign, provisionFingerprint)
        .map((row) => toEntry(row as Record<string, unknown>))
        .filter((candidate) => {
          const candidatePacket = parseCmClientAprsLine(candidate.data);
          return (
            candidatePacket?.destination === packet.destination &&
            candidatePacket.info === packet.info
          );
        });
      if (
        observerConfirmedAt &&
        exactPending.length === 1 &&
        exactPending[0]!.id === id
      ) {
        this.database
          .prepare(
            "UPDATE aprs_outbox SET delivery_status = 'observer_confirmed', observer_confirmed_at = ?, updated_at = CASE WHEN updated_at > ? THEN updated_at ELSE ? END WHERE id = ? AND delivery_status = 'submitted'",
          )
          .run(
            observerConfirmedAt,
            observerConfirmedAt,
            observerConfirmedAt,
            id,
          );
      }
      const entry = this.required(id);
      if (
        entry.status !== "sent" ||
        !["submitted", "observer_confirmed"].includes(entry.deliveryStatus) ||
        !hasCompleteSourceSnapshot(entry)
      ) {
        throw new AprsOutboxError();
      }
      if (
        observerConfirmedAt &&
        entry.deliveryStatus === "observer_confirmed"
      ) {
        this.advanceDeliveryHighWater(entry, observerConfirmedAt);
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

  /** @deprecated Use markSubmitted; a socket write is not delivery proof. */
  markSent(
    id: string,
    now: string,
    provisionFingerprint: string,
  ): AprsOutboxEntry {
    return this.markSubmitted(id, now, provisionFingerprint);
  }

  confirmObserved(
    callsign: string,
    destination: string,
    info: string,
    observedAt: string,
    currentProvisionFingerprint: string,
  ): AprsOutboxEntry[] {
    if (
      !/^[A-Z0-9]{1,6}(?:-[0-9]{1,2})?$/.test(callsign) ||
      !/^[A-Z0-9]{1,6}(?:-[0-9]{1,2})?$/.test(destination) ||
      !info ||
      /[\r\n]/.test(info) ||
      !isTimestamp(observedAt) ||
      !PROVISION_FINGERPRINT_PATTERN.test(currentProvisionFingerprint)
    ) {
      throw new AprsOutboxError();
    }
    let transactionOpen = false;
    try {
      this.database.exec("BEGIN IMMEDIATE");
      transactionOpen = true;
      const candidates = this.database
        .prepare(
          "SELECT * FROM aprs_outbox WHERE callsign = ? AND provision_fingerprint = ? AND status = 'sent' AND delivery_status = 'submitted' AND submitted_at IS NOT NULL AND submitted_at <= ? AND observation_expires_at IS NOT NULL AND observation_expires_at >= ? ORDER BY submitted_at ASC, id ASC",
        )
        .all(callsign, currentProvisionFingerprint, observedAt, observedAt)
        .map((row) => toEntry(row as Record<string, unknown>));
      const matching = candidates.filter((candidate) => {
        const packet = parseCmClientAprsLine(candidate.data);
        return packet?.destination === destination && packet.info === info;
      });
      if (matching.length !== 1) {
        this.database.exec("COMMIT");
        transactionOpen = false;
        return [];
      }
      const candidate = matching[0]!;
      this.database
        .prepare(
          "UPDATE aprs_outbox SET delivery_status = 'observer_confirmed', observer_confirmed_at = ?, updated_at = ? WHERE id = ? AND delivery_status = 'submitted'",
        )
        .run(observedAt, observedAt, candidate.id);
      const entry = this.required(candidate.id);
      if (entry.deliveryStatus !== "observer_confirmed") {
        throw new AprsOutboxError();
      }
      this.advanceDeliveryHighWater(entry, observedAt);
      this.database.exec("COMMIT");
      transactionOpen = false;
      return [entry];
    } catch {
      if (transactionOpen) {
        this.database.exec("ROLLBACK");
      }
      throw new AprsOutboxError();
    }
  }

  reconcileObservedCache(
    currentProvisionFingerprint: string,
  ): AprsOutboxEntry[] {
    if (!PROVISION_FINGERPRINT_PATTERN.test(currentProvisionFingerprint)) {
      throw new AprsOutboxError();
    }
    try {
      const evidence = this.database
        .prepare(
          "SELECT callsign, destination, info, first_observed_at, last_observed_at FROM aprs_observed_packets WHERE destination <> '' ORDER BY first_observed_at ASC, callsign ASC, destination ASC, info ASC",
        )
        .all();
      const confirmed = new Map<string, AprsOutboxEntry>();
      for (const row of evidence) {
        const firstObservedAt = String(row.first_observed_at);
        const lastObservedAt = String(row.last_observed_at);
        const observedAtValues =
          firstObservedAt === lastObservedAt
            ? [firstObservedAt]
            : [firstObservedAt, lastObservedAt];
        for (const observedAt of observedAtValues) {
          for (const entry of this.confirmObserved(
            String(row.callsign),
            String(row.destination),
            String(row.info),
            observedAt,
            currentProvisionFingerprint,
          )) {
            confirmed.set(entry.id, entry);
          }
        }
      }
      return [...confirmed.values()];
    } catch (error) {
      if (error instanceof AprsOutboxError) {
        throw error;
      }
      throw new AprsOutboxError();
    }
  }

  expireSubmitted(now: string): number {
    if (!isTimestamp(now)) {
      throw new AprsOutboxError();
    }
    try {
      return Number(
        this.database
          .prepare(
            "UPDATE aprs_outbox SET delivery_status = 'observation_expired', updated_at = ? WHERE delivery_status = 'submitted' AND observation_expires_at IS NOT NULL AND observation_expires_at <= ?",
          )
          .run(now, now).changes,
      );
    } catch {
      throw new AprsOutboxError();
    }
  }

  private advanceDeliveryHighWater(
    entry: AprsOutboxEntry,
    deliveredAt: string,
  ): void {
    if (!hasCompleteSourceSnapshot(entry)) {
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
        deliveredAt,
        entry.mappingVersion ?? null,
      );
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
          "UPDATE aprs_outbox SET status = 'failed', delivery_status = 'failed', attempts = ?, next_attempt_at = ?, last_error_code = ?, updated_at = ? WHERE id = ? AND status = 'sending'",
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

  releaseClaim(id: string, now: string): AprsOutboxEntry {
    if (!isTimestamp(now)) {
      throw new AprsOutboxError();
    }
    let transactionOpen = false;
    try {
      this.database.exec("BEGIN IMMEDIATE");
      transactionOpen = true;
      const current = this.required(id);
      if (current.status !== "sending") {
        throw new AprsOutboxError();
      }
      const result = this.database
        .prepare(
          "UPDATE aprs_outbox SET status = 'queued', delivery_status = 'queued', updated_at = ?, last_error_code = NULL WHERE id = ? AND status = 'sending'",
        )
        .run(now, id);
      if (Number(result.changes) !== 1) {
        throw new AprsOutboxError();
      }
      const released = this.required(id);
      this.database.exec("COMMIT");
      transactionOpen = false;
      return released;
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
        "UPDATE aprs_outbox SET status = 'failed', delivery_status = 'failed', attempts = attempts + 1, next_attempt_at = ?, last_error_code = 'APRS_TX_INTERRUPTED', updated_at = ? WHERE status = 'sending'",
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
          "DELETE FROM aprs_outbox WHERE id IN (SELECT outbox.id FROM aprs_outbox AS outbox INDEXED BY aprs_outbox_sent_retention_index WHERE outbox.status = 'sent' AND outbox.sent_at IS NOT NULL AND outbox.sent_at < ? AND (outbox.delivery_status = 'observation_expired' OR (outbox.delivery_status = 'observer_confirmed' AND EXISTS (SELECT 1 FROM aprs_delivery_high_water AS delivery WHERE delivery.mesh_network_id = outbox.mesh_network_id AND delivery.node_num = outbox.node_num AND delivery.callsign = outbox.callsign AND outbox.event_time IS NOT NULL AND (delivery.latest_canonical_event_id = outbox.canonical_event_id OR delivery.latest_event_time > outbox.event_time OR (delivery.latest_event_time = outbox.event_time AND delivery.latest_mapping_version IS NOT NULL AND outbox.mapping_version IS NOT NULL AND delivery.latest_mapping_version = outbox.mapping_version AND delivery.latest_sequence_epoch IS NOT NULL AND delivery.latest_sequence_number IS NOT NULL AND outbox.sequence_epoch IS NOT NULL AND outbox.sequence_number IS NOT NULL AND (delivery.latest_sequence_epoch > outbox.sequence_epoch OR (delivery.latest_sequence_epoch = outbox.sequence_epoch AND delivery.latest_sequence_number > outbox.sequence_number))))))) ORDER BY outbox.sent_at ASC, outbox.id ASC LIMIT ?)",
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
          `DELETE FROM aprs_outbox WHERE id IN (
            SELECT older.id
            FROM aprs_outbox AS older
            WHERE older.status IN ('queued', 'failed')
              AND (
                EXISTS (
                  SELECT 1
                  FROM (
                    SELECT mesh_network_id, node_num, callsign, latest_canonical_event_id, latest_mapping_version, latest_event_time, latest_sequence_epoch, latest_sequence_number
                    FROM aprs_delivery_high_water
                    UNION ALL
                    SELECT mesh_network_id, node_num, callsign, latest_canonical_event_id, latest_mapping_version, latest_event_time, latest_sequence_epoch, latest_sequence_number
                    FROM aprs_legacy_submission_barriers
                  ) AS barrier
                  WHERE barrier.mesh_network_id = older.mesh_network_id
                    AND barrier.node_num = older.node_num
                    AND barrier.callsign = older.callsign
                    AND older.event_time IS NOT NULL
                    AND (
                      barrier.latest_canonical_event_id = older.canonical_event_id
                      OR barrier.latest_event_time > older.event_time
                      OR (
                        barrier.latest_event_time = older.event_time
                        AND barrier.latest_mapping_version IS NOT NULL
                        AND older.mapping_version IS NOT NULL
                        AND barrier.latest_mapping_version = older.mapping_version
                        AND barrier.latest_sequence_epoch IS NOT NULL
                        AND barrier.latest_sequence_number IS NOT NULL
                        AND older.sequence_epoch IS NOT NULL
                        AND older.sequence_number IS NOT NULL
                        AND (
                          barrier.latest_sequence_epoch > older.sequence_epoch
                          OR (
                            barrier.latest_sequence_epoch = older.sequence_epoch
                            AND barrier.latest_sequence_number > older.sequence_number
                          )
                        )
                      )
                    )
                )
                OR EXISTS (
                  SELECT 1
                  FROM aprs_outbox AS newer INDEXED BY aprs_outbox_active_order_index
                  WHERE newer.mesh_network_id = older.mesh_network_id
                    AND newer.node_num = older.node_num
                    AND newer.callsign = older.callsign
                    AND newer.id <> older.id
                    AND newer.status IN ('queued', 'sending', 'failed')
                    AND newer.event_time IS NOT NULL
                    AND older.event_time IS NOT NULL
                    AND (
                      newer.event_time > older.event_time
                      OR (
                        newer.event_time = older.event_time
                        AND newer.mapping_version IS NOT NULL
                        AND older.mapping_version IS NOT NULL
                        AND newer.mapping_version = older.mapping_version
                        AND newer.sequence_epoch IS NOT NULL
                        AND newer.sequence_number IS NOT NULL
                        AND older.sequence_epoch IS NOT NULL
                        AND older.sequence_number IS NOT NULL
                        AND (
                          newer.sequence_epoch > older.sequence_epoch
                          OR (
                            newer.sequence_epoch = older.sequence_epoch
                            AND newer.sequence_number > older.sequence_number
                          )
                        )
                      )
                    )
                )
              )
            ORDER BY older.updated_at ASC, older.id ASC
            LIMIT ?
          )`,
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
    for (const barrier of this.findOrderBarriers(snapshot, callsign)) {
      const comparison = compareOrder(snapshot, barrier);
      if (
        barrier.canonicalEventId === canonicalEventId ||
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
    let ambiguous = false;
    for (const barrier of this.findOrderBarriers(snapshot, entry.callsign)) {
      if (barrier.canonicalEventId === entry.canonicalEventId) {
        return "stale";
      }
      const comparison = compareOrder(snapshot, barrier);
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

  private isRecentExactDuplicate(entry: AprsOutboxEntry, now: string): boolean {
    const packet = parseCmClientAprsLine(entry.data);
    if (!packet || packet.callsign !== entry.callsign || !isTimestamp(now)) {
      return true;
    }
    const observerCutoff = new Date(
      Date.parse(now) - 3 * 60 * 60 * 1_000,
    ).toISOString();
    const localCutoff = new Date(Date.parse(now) - 30_000).toISOString();
    return Boolean(
      this.database
        .prepare(
          "SELECT 1 FROM aprs_observed_packets WHERE callsign = ? AND info = ? AND (destination = ? OR destination = '') AND last_observed_at >= ?",
        )
        .get(
          packet.callsign,
          packet.info,
          packet.destination,
          observerCutoff,
        ) ||
      this.database
        .prepare(
          "SELECT 1 FROM aprs_local_transmissions WHERE callsign = ? AND info = ? AND (destination = ? OR destination = '') AND transmitted_at >= ?",
        )
        .get(packet.callsign, packet.info, packet.destination, localCutoff),
    );
  }

  private findOrderBarriers(
    snapshot: OrderSnapshot,
    callsign: string,
  ): OrderSnapshot[] {
    const rows = this.database
      .prepare(
        "SELECT latest_canonical_event_id, latest_mapping_version, latest_event_time, latest_sequence_epoch, latest_sequence_number FROM aprs_delivery_high_water WHERE mesh_network_id = ? AND node_num = ? AND callsign = ? UNION ALL SELECT latest_canonical_event_id, latest_mapping_version, latest_event_time, latest_sequence_epoch, latest_sequence_number FROM aprs_legacy_submission_barriers WHERE mesh_network_id = ? AND node_num = ? AND callsign = ?",
      )
      .all(
        snapshot.meshNetworkId,
        snapshot.nodeNum,
        callsign,
        snapshot.meshNetworkId,
        snapshot.nodeNum,
        callsign,
      );
    return rows.map((row) => {
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
    });
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
        "UPDATE aprs_outbox SET status = 'failed', delivery_status = 'failed', attempts = ?, next_attempt_at = ?, last_error_code = 'APRS_ORDER_UNPROVEN', updated_at = ? WHERE id = ? AND status = 'sending'",
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
    deliveryStatus: entry.deliveryStatus,
    attempts: entry.attempts,
    nextAttemptAt: entry.nextAttemptAt,
    createdAt: entry.createdAt,
    updatedAt: entry.updatedAt,
    ...(entry.lastErrorCode ? { lastErrorCode: entry.lastErrorCode } : {}),
    ...(entry.sentAt ? { sentAt: entry.sentAt } : {}),
    ...(entry.submittedAt ? { submittedAt: entry.submittedAt } : {}),
    ...(entry.observerConfirmedAt
      ? { observerConfirmedAt: entry.observerConfirmedAt }
      : {}),
    ...(entry.observationExpiresAt
      ? { observationExpiresAt: entry.observationExpiresAt }
      : {}),
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

  flush(
    limit?: number,
    shouldContinue?: () => boolean,
  ): Promise<AprsOutboxEntry[]> {
    if (this.activeFlush) {
      return this.activeFlush;
    }
    const operation = this.runFlush(limit, shouldContinue);
    this.activeFlush = operation;
    void operation.then(
      () => this.completeFlush(operation),
      () => this.completeFlush(operation),
    );
    return operation;
  }

  async close(): Promise<void> {
    await this.activeFlush?.catch(() => undefined);
    await this.transport.close?.();
  }

  private async runFlush(
    limit?: number,
    shouldContinue: () => boolean = () => true,
  ): Promise<AprsOutboxEntry[]> {
    const batchLimit = limit ?? 10;
    if (!Number.isInteger(batchLimit) || batchLimit < 1) {
      throw new AprsOutboxError();
    }
    const now = this.clock().toISOString();
    this.repository.expireSubmitted(now);
    this.repository.resumeInterrupted(now);
    this.repository.deleteSuperseded(1_000);
    const entries = this.repository.claimDue(now, batchLimit);
    const results: AprsOutboxEntry[] = [];
    for (let index = 0; index < entries.length; index += 1) {
      const entry = entries[index]!;
      if (!shouldContinue()) {
        this.releaseRemainingClaims(entries, index);
        break;
      }
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
          undefined,
          shouldContinue,
        );
        results.push(
          this.repository.markSubmitted(
            authorization.entry.id,
            this.clock().toISOString(),
            provisionFingerprint,
          ),
        );
      } catch (error) {
        if (error instanceof AprsTransmissionFencedError) {
          this.releaseRemainingClaims(entries, index);
          break;
        }
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

  private releaseRemainingClaims(
    entries: readonly AprsOutboxEntry[],
    startIndex: number,
  ): void {
    for (let index = startIndex; index < entries.length; index += 1) {
      this.repository.releaseClaim(
        entries[index]!.id,
        this.clock().toISOString(),
      );
    }
  }

  private completeFlush(operation: Promise<AprsOutboxEntry[]>): void {
    if (this.activeFlush === operation) {
      this.activeFlush = undefined;
    }
  }
}

export class AprsIsTcpClient implements AprsTransport {
  private session: AprsTxSession | undefined;
  private operationTail: Promise<void> = Promise.resolve();
  private verifiedSessionGeneration = 0;

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

  prepareVerifiedSession(
    provisionFingerprint: string,
  ): Promise<AprsVerifiedTransportSession> {
    const operation = this.operationTail.then(async () => {
      const session = await this.requireVerifiedSession(provisionFingerprint);
      return { generation: session.generation };
    });
    this.operationTail = operation.then(
      () => undefined,
      () => undefined,
    );
    return operation;
  }

  send(
    data: string,
    provisionFingerprint: string,
    expectedSession?: AprsVerifiedTransportSession,
    transmissionGate?: () => boolean,
  ): Promise<void> {
    const operation = this.operationTail.then(() =>
      this.sendSerialized(
        data,
        provisionFingerprint,
        expectedSession,
        transmissionGate,
      ),
    );
    this.operationTail = operation.then(
      () => undefined,
      () => undefined,
    );
    return operation;
  }

  close(): Promise<void> {
    const operation = this.operationTail.then(() => this.closeCurrentSession());
    this.operationTail = operation.then(
      () => undefined,
      () => undefined,
    );
    return operation;
  }

  private async sendSerialized(
    data: string,
    provisionFingerprint: string,
    expectedSession?: AprsVerifiedTransportSession,
    transmissionGate?: () => boolean,
  ): Promise<void> {
    if (!isValidAprsData(data)) {
      throw new AprsOutboxError();
    }
    const session = await this.requireVerifiedSession(
      provisionFingerprint,
      expectedSession,
    );
    if (transmissionGate && !transmissionGate()) {
      throw new AprsTransmissionFencedError();
    }
    try {
      await write(session.socket, `${data}\r\n`);
    } catch {
      await this.closeCurrentSession();
      throw new AprsOutboxError();
    }
  }

  private async requireVerifiedSession(
    provisionFingerprint: string,
    expectedSession?: AprsVerifiedTransportSession,
  ): Promise<AprsTxSession> {
    if (
      !PROVISION_FINGERPRINT_PATTERN.test(provisionFingerprint) ||
      (expectedSession !== undefined &&
        (!Number.isInteger(expectedSession.generation) ||
          expectedSession.generation < 1))
    ) {
      throw new AprsOutboxError();
    }
    let authorization: AprsConnectionAuthorization;
    let callsign: string;
    try {
      authorization = resolveAuthorization(
        this.options.authorizationProvider,
        provisionFingerprint,
      );
      callsign = authorizationCallsign(authorization);
    } catch (error) {
      await this.closeCurrentProvisionSession(provisionFingerprint);
      throw error;
    }
    let session = this.session;
    if (session && session.provisionFingerprint !== provisionFingerprint) {
      await this.closeCurrentSession();
      session = undefined;
    } else if (session && !authorizationMatches(session, authorization)) {
      await this.closeCurrentSession();
      throw new AprsAuthorizationError();
    }

    if (
      expectedSession &&
      (!session ||
        !isVerifiedSession(session) ||
        session.generation !== expectedSession.generation)
    ) {
      if (session && !isVerifiedSession(session)) {
        await this.closeCurrentSession();
      }
      throw new AprsOutboxError();
    }

    if (!session || !isVerifiedSession(session)) {
      if (session) {
        await this.closeCurrentSession();
      }
      session = await this.openVerifiedSession(
        authorization,
        callsign,
        provisionFingerprint,
      );
    }

    let currentAuthorization: AprsConnectionAuthorization;
    try {
      currentAuthorization = resolveAuthorization(
        this.options.authorizationProvider,
        provisionFingerprint,
      );
    } catch (error) {
      await this.closeCurrentProvisionSession(provisionFingerprint);
      throw error;
    }
    if (
      this.session !== session ||
      !isVerifiedSession(session) ||
      !authorizationMatches(session, currentAuthorization)
    ) {
      await this.closeCurrentSession();
      throw new AprsAuthorizationError();
    }
    if (expectedSession && session.generation !== expectedSession.generation) {
      throw new AprsOutboxError();
    }
    return session;
  }

  private async openVerifiedSession(
    authorization: AprsConnectionAuthorization,
    callsign: string,
    provisionFingerprint: string,
  ): Promise<AprsTxSession> {
    const socket = net.createConnection({
      host: this.options.host,
      port: this.options.port,
    });
    const session: AprsTxSession = {
      socket,
      state: "connecting",
      generation: 0,
      callsign,
      loginLine: authorization.loginLine,
      provisionFingerprint,
    };
    this.session = session;
    const timeoutMs = this.options.timeoutMs ?? 10_000;
    const verification = waitForVerifiedLogresp(
      socket,
      callsign,
      timeoutMs,
      () => this.invalidateSession(session),
    );
    void verification.catch(() => undefined);
    try {
      await onceConnected(socket, timeoutMs);
      const connectedAuthorization = resolveAuthorization(
        this.options.authorizationProvider,
        provisionFingerprint,
      );
      if (
        this.session !== session ||
        !authorizationMatches(session, connectedAuthorization)
      ) {
        throw new AprsAuthorizationError();
      }
      session.state = "login-sent";
      await write(socket, `${authorization.loginLine}\r\n`);
      await verification;
      const verifiedAuthorization = resolveAuthorization(
        this.options.authorizationProvider,
        provisionFingerprint,
      );
      if (
        this.session !== session ||
        session.state !== "login-sent" ||
        !authorizationMatches(session, verifiedAuthorization)
      ) {
        throw new AprsAuthorizationError();
      }
      session.state = "verified";
      session.generation = ++this.verifiedSessionGeneration;
      socket.unref();
      return session;
    } catch (error) {
      await this.closeSession(session);
      if (error instanceof AprsAuthorizationError) {
        throw error;
      }
      throw new AprsOutboxError();
    }
  }

  private invalidateSession(session: AprsTxSession): void {
    if (session.state !== "closing") {
      session.state = "closed";
    }
    if (this.session === session) {
      this.session = undefined;
    }
    if (!session.socket.destroyed) {
      session.socket.destroy();
    }
  }

  private async closeCurrentSession(): Promise<void> {
    const session = this.session;
    if (!session) {
      return;
    }
    await this.closeSession(session);
  }

  private async closeCurrentProvisionSession(
    provisionFingerprint: string,
  ): Promise<void> {
    const session = this.session;
    if (!session || session.provisionFingerprint !== provisionFingerprint) {
      return;
    }
    await this.closeSession(session);
  }

  private async closeSession(session: AprsTxSession): Promise<void> {
    if (this.session === session) {
      this.session = undefined;
    }
    session.state = "closing";
    await destroySocket(session.socket);
    session.state = "closed";
  }
}

type AprsTxSessionState =
  "connecting" | "login-sent" | "verified" | "closing" | "closed";

interface AprsTxSession {
  callsign: string;
  generation: number;
  loginLine: string;
  provisionFingerprint: string;
  socket: Socket;
  state: AprsTxSessionState;
}

function isValidAprsData(data: string): boolean {
  return (
    data.trim().length > 0 &&
    !/[\r\n]/.test(data) &&
    Buffer.byteLength(data, "utf8") <= MAX_APRS_DATA_BYTES
  );
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
    let settled = false;
    const finish = (error?: AprsOutboxError) => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timer);
      socket.off("connect", onConnect);
      socket.off("error", onError);
      socket.off("end", onEnd);
      socket.off("close", onClose);
      if (error) {
        reject(error);
      } else {
        resolve();
      }
    };
    const onConnect = () => finish();
    const onError = () => finish(new AprsOutboxError());
    const onEnd = () => finish(new AprsOutboxError());
    const onClose = () => finish(new AprsOutboxError());
    const timer = setTimeout(() => {
      socket.destroy();
      finish(new AprsOutboxError());
    }, timeoutMs);
    timer.unref();
    socket.once("connect", onConnect);
    socket.once("error", onError);
    socket.once("end", onEnd);
    socket.once("close", onClose);
  });
}

function write(socket: Socket, value: string): Promise<void> {
  return new Promise((resolve, reject) =>
    socket.write(value, (error) => (error ? reject(error) : resolve())),
  );
}

const MAX_APRS_LOGIN_LINE_BYTES = 512;
const APRS_CALLSIGN_PATTERN = /^[A-Z0-9]{1,6}(?:-[0-9]{1,2})?$/;

function waitForVerifiedLogresp(
  socket: Socket,
  expectedCallsign: string,
  timeoutMs: number,
  invalidate: () => void,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const decoder = new TextDecoder("utf-8", { fatal: true });
    let buffer = "";
    let settled = false;
    const finish = (error?: AprsOutboxError) => {
      if (settled) {
        if (error) {
          invalidate();
        }
        return;
      }
      settled = true;
      clearTimeout(timer);
      if (error) {
        invalidate();
        reject(error);
      } else {
        resolve();
      }
    };
    const processLine = (line: string): boolean => {
      if (Buffer.byteLength(line, "utf8") > MAX_APRS_LOGIN_LINE_BYTES) {
        finish(new AprsOutboxError());
        return false;
      }
      const logresp = parseLogresp(line);
      if (logresp === undefined) {
        return true;
      }
      if (
        logresp === "malformed" ||
        logresp.callsign !== expectedCallsign ||
        logresp.status !== "verified"
      ) {
        finish(new AprsOutboxError());
        return false;
      }
      finish();
      return true;
    };
    const onData = (chunk: Buffer) => {
      try {
        buffer += decoder.decode(chunk, { stream: true });
      } catch {
        finish(new AprsOutboxError());
        return;
      }
      let lineEnd = buffer.indexOf("\n");
      while (lineEnd >= 0) {
        const line = buffer.slice(0, lineEnd).replace(/\r$/, "");
        buffer = buffer.slice(lineEnd + 1);
        if (!processLine(line)) {
          return;
        }
        lineEnd = buffer.indexOf("\n");
      }
      if (Buffer.byteLength(buffer, "utf8") > MAX_APRS_LOGIN_LINE_BYTES) {
        finish(new AprsOutboxError());
      }
    };
    const onFailure = () => finish(new AprsOutboxError());
    const timer = setTimeout(onFailure, timeoutMs);
    timer.unref();
    socket.on("data", onData);
    socket.on("error", onFailure);
    socket.on("end", onFailure);
    socket.on("close", onFailure);
  });
}

function parseLogresp(
  line: string,
):
  | { callsign: string; status: "verified" | "unverified" }
  | "malformed"
  | undefined {
  if (!/^#\s*logresp\b/i.test(line)) {
    return undefined;
  }
  const match =
    /^#\s*logresp\s+([^\s,]+)\s+(verified|unverified)(?:[\s,]|$)/i.exec(line);
  if (!match || !APRS_CALLSIGN_PATTERN.test(match[1]!)) {
    return "malformed";
  }
  return {
    callsign: match[1]!,
    status: match[2]!.toLowerCase() as "verified" | "unverified",
  };
}

function authorizationCallsign(
  authorization: AprsConnectionAuthorization,
): string {
  const match = /^user\s+([^\s]+)\s+pass\s+[^\s]+(?:\s|$)/.exec(
    authorization.loginLine,
  );
  if (!match || !APRS_CALLSIGN_PATTERN.test(match[1]!)) {
    throw new AprsAuthorizationError();
  }
  return match[1]!;
}

function authorizationMatches(
  session: AprsTxSession,
  authorization: AprsConnectionAuthorization,
): boolean {
  try {
    return (
      authorization.provisionFingerprint === session.provisionFingerprint &&
      authorization.loginLine === session.loginLine &&
      authorizationCallsign(authorization) === session.callsign
    );
  } catch {
    return false;
  }
}

function isVerifiedSession(session: AprsTxSession): boolean {
  return (
    session.state === "verified" &&
    !session.socket.destroyed &&
    session.socket.writable
  );
}

function destroySocket(socket: Socket): Promise<void> {
  if (socket.destroyed) {
    return Promise.resolve();
  }
  return new Promise((resolve) => {
    socket.once("close", resolve);
    socket.destroy();
  });
}

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
  const deliveryStatus = String(row.delivery_status);
  if (
    ![
      "queued",
      "sending",
      "failed",
      "submitted",
      "observer_confirmed",
      "observation_expired",
    ].includes(deliveryStatus)
  ) {
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
    deliveryStatus: deliveryStatus as AprsDeliveryStatus,
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
    ...(typeof row.submitted_at === "string"
      ? { submittedAt: row.submitted_at }
      : {}),
    ...(typeof row.observer_confirmed_at === "string"
      ? { observerConfirmedAt: row.observer_confirmed_at }
      : {}),
    ...(typeof row.observation_expires_at === "string"
      ? { observationExpiresAt: row.observation_expires_at }
      : {}),
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
