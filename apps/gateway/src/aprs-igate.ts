import { randomUUID } from "node:crypto";
import { DatabaseSync } from "node:sqlite";

import type {
  AprsIgateSubmission as PublicAprsIgateSubmission,
  CallMeshProvision,
} from "@cmclient/contracts";

import { PROVISION_FINGERPRINT_PATTERN } from "./aprs-identity.js";
import { parseCmClientAprsLine } from "./aprs-monitor.js";

const APRS_DESTINATION = "APTMAG";
const APRS_IGATE_PATH = "TCPIP*";
const APRS_LINE_LIMIT_BYTES = 512;
const APRS_DATA_LIMIT_BYTES = APRS_LINE_LIMIT_BYTES - 2;
const MINUTE_MS = 60_000;
const TELEMETRY_BUCKET_MS = MINUTE_MS;
const TELEMETRY_WINDOW_MS = 10 * MINUTE_MS;
const LOCAL_REPEAT_WINDOW_MS = 30_000;
const MIN_BEACON_INTERVAL_MS = MINUTE_MS;
const MAX_BEACON_INTERVAL_MS = 24 * 60 * MINUTE_MS;
const OBSERVATION_WINDOW_MS = 3 * 60 * MINUTE_MS;
const APRS_CALLSIGN_PATTERN = /^[A-Z0-9]{1,6}(?:-(?:[1-9]|1[0-5]))?$/;
const APRS_DESTINATION_PATTERN = /^[A-Z0-9]{1,6}(?:-[0-9]{1,2})?$/;

export const APRS_IGATE_DEFAULT_BEACON_INTERVAL_MS = 10 * MINUTE_MS;
export const APRS_IGATE_STATUS_INTERVAL_MS = 60 * MINUTE_MS;
export const APRS_IGATE_DEFINITION_INTERVAL_MS = 6 * 60 * MINUTE_MS;
export const APRS_IGATE_TELEMETRY_INTERVAL_MS = TELEMETRY_WINDOW_MS;

const POSITION_TYPES = new Set([
  "position",
  "waypoint",
  "envtelemetry",
  "telemetry",
  "remotetelemetry",
  "remoteposition",
]);
const MESSAGE_TYPES = new Set(["text", "message", "data", "storeforward"]);
const CONTROL_TYPES = new Set([
  "nodeinfo",
  "routing",
  "routerequest",
  "routereply",
  "routeerror",
  "admin",
  "config",
  "traceroute",
  "remotehardware",
  "neighborinfo",
  "keyverification",
]);

export type AprsIgatePacketKind =
  | "beacon"
  | "status"
  | "telemetry-parm"
  | "telemetry-unit"
  | "telemetry-eqns"
  | "telemetry-data";

export interface AprsIgatePacket {
  /** Complete TNC2 line without CRLF. The APRS transport adds CRLF. */
  data: string;
  kind: AprsIgatePacketKind;
}

export interface AprsIgateCounters {
  all: number;
  forwardedAprs: number;
  position: number;
  message: number;
  control: number;
}

export interface AprsIgatePersistentState {
  /** The only scheduler field retained across a process restart. */
  lastSuccessfulTelemetrySequence: number;
}

export type AprsIgateDeliveryStatus =
  | "sending"
  | "transmission_uncertain"
  | "submitted"
  | "observer_confirmed"
  | "observation_expired";

export interface AprsIgateSubmission {
  id: string;
  callsign: string;
  destination: string;
  info: string;
  packetKind: AprsIgatePacketKind;
  provisionFingerprint: string;
  deliveryStatus: AprsIgateDeliveryStatus;
  attemptedAt: string;
  updatedAt: string;
  submittedAt?: string;
  localWriteCompletedAt?: string;
  observerConfirmedAt?: string;
  observationExpiresAt: string;
}

export interface AprsIgateTransmissionIntent {
  created: boolean;
  /** Whether the caller must perform an APRS socket write for this intent. */
  writeRequired: boolean;
  /** Durable local-cache reservation for a periodic write of a fixed packet. */
  repeatReservationAt?: string;
  submission: AprsIgateSubmission;
}

export interface AprsIgateDeliveryCounts {
  pending: number;
  uncertain: number;
  unconfirmed: number;
}

export interface AprsIgateWriteOutcome {
  packet: AprsIgatePacket;
  successful: boolean;
}

export type AprsIgateWriter = (
  packet: AprsIgatePacket,
) => boolean | Promise<boolean>;

export interface AprsIgateFamilyOptions {
  provision: CallMeshProvision;
  version: string;
  beaconIntervalMs?: number;
  lastSuccessfulTelemetrySequence?: number;
}

interface TelemetryBucket {
  all: number;
  forwardedAprs: number;
  position: number;
  message: number;
  control: number;
}

export class AprsIgateEncodingError extends Error {
  readonly code = "APRS_IGATE_ENCODING_INVALID";

  constructor() {
    super("APRS_IGATE_ENCODING_INVALID");
    this.name = "AprsIgateEncodingError";
  }
}

export class AprsIgatePersistenceError extends Error {
  readonly code = "APRS_IGATE_PERSISTENCE_FAILED";

  constructor() {
    super("APRS_IGATE_PERSISTENCE_FAILED");
    this.name = "AprsIgatePersistenceError";
  }
}

export class AprsIgateRepository {
  constructor(private readonly database: DatabaseSync) {}

  loadLastSuccessfulSequence(callsign: string): number {
    validateCallsign(callsign);
    try {
      const row = this.database
        .prepare(
          "SELECT last_successful_telemetry_sequence FROM aprs_igate_state WHERE callsign = ?",
        )
        .get(callsign);
      if (!row) {
        return 0;
      }
      const sequence = Number(row.last_successful_telemetry_sequence);
      if (!isTelemetrySequence(sequence, true)) {
        throw new AprsIgatePersistenceError();
      }
      return sequence;
    } catch {
      throw new AprsIgatePersistenceError();
    }
  }

  persistLastSuccessfulSequence(
    callsign: string,
    provisionFingerprint: string,
    sequence: number,
    updatedAt: string,
  ): void {
    validateCallsign(callsign);
    if (
      !PROVISION_FINGERPRINT_PATTERN.test(provisionFingerprint) ||
      !Number.isInteger(sequence) ||
      sequence < 0 ||
      sequence > 999 ||
      !isTimestamp(updatedAt)
    ) {
      throw new AprsIgatePersistenceError();
    }
    try {
      this.database
        .prepare(
          "INSERT INTO aprs_igate_state (callsign, provision_fingerprint, last_successful_telemetry_sequence, updated_at) VALUES (?, ?, ?, ?) ON CONFLICT(callsign) DO UPDATE SET provision_fingerprint = excluded.provision_fingerprint, last_successful_telemetry_sequence = excluded.last_successful_telemetry_sequence, updated_at = excluded.updated_at",
        )
        .run(callsign, provisionFingerprint, sequence, updatedAt);
    } catch {
      throw new AprsIgatePersistenceError();
    }
  }

  beginTransmission(
    packetValue: AprsIgatePacket,
    provisionFingerprint: string,
    attemptedAt: string,
  ): AprsIgateTransmissionIntent {
    const parsed = parseCmClientAprsLine(packetValue.data);
    if (
      !parsed ||
      !isPacketKind(packetValue.kind) ||
      parsed.destination !== APRS_DESTINATION ||
      !isClientOriginatedIgateData(packetValue.data, parsed.callsign) ||
      !PROVISION_FINGERPRINT_PATTERN.test(provisionFingerprint) ||
      !isTimestamp(attemptedAt)
    ) {
      throw new AprsIgatePersistenceError();
    }
    const observationExpiresAt = new Date(
      Date.parse(attemptedAt) + OBSERVATION_WINDOW_MS,
    ).toISOString();
    let transactionOpen = false;
    try {
      this.database.exec("BEGIN IMMEDIATE");
      transactionOpen = true;
      this.expireActiveInsideTransaction(attemptedAt);
      const localCutoff = new Date(
        Date.parse(attemptedAt) - LOCAL_REPEAT_WINDOW_MS,
      ).toISOString();
      const existing = this.database
        .prepare(
          "SELECT * FROM aprs_igate_submissions WHERE provision_fingerprint = ? AND callsign = ? AND destination = ? AND info = ? AND delivery_status IN ('sending', 'transmission_uncertain', 'submitted') ORDER BY attempted_at ASC, id ASC",
        )
        .all(
          provisionFingerprint,
          parsed.callsign,
          parsed.destination,
          parsed.info,
        );
      if (existing.length > 1) {
        throw new AprsIgatePersistenceError();
      }
      if (existing.length === 1) {
        const submission = toSubmission(existing[0] as Record<string, unknown>);
        if (submission.deliveryStatus === "submitted") {
          const local = this.database
            .prepare(
              "SELECT transmitted_at FROM aprs_local_transmissions WHERE callsign = ? AND destination = ? AND info = ?",
            )
            .get(
              submission.callsign,
              submission.destination,
              submission.info,
            ) as Record<string, unknown> | undefined;
          const transmittedAt =
            typeof local?.transmitted_at === "string"
              ? local.transmitted_at
              : undefined;
          if (local && !isTimestamp(transmittedAt ?? "")) {
            throw new AprsIgatePersistenceError();
          }
          if (
            !transmittedAt ||
            (Date.parse(transmittedAt) < Date.parse(localCutoff) &&
              Date.parse(transmittedAt) <= Date.parse(attemptedAt))
          ) {
            this.database
              .prepare(
                "INSERT INTO aprs_local_transmissions (callsign, destination, info, transmitted_at) VALUES (?, ?, ?, ?) ON CONFLICT(callsign, destination, info) DO UPDATE SET transmitted_at = excluded.transmitted_at",
              )
              .run(
                submission.callsign,
                submission.destination,
                submission.info,
                attemptedAt,
              );
            this.database.exec("COMMIT");
            transactionOpen = false;
            return {
              created: false,
              writeRequired: true,
              repeatReservationAt: attemptedAt,
              submission,
            };
          }
        }
        this.database.exec("COMMIT");
        transactionOpen = false;
        return { created: false, writeRequired: false, submission };
      }
      const recent = this.database
        .prepare(
          "SELECT submission.* FROM aprs_igate_submissions AS submission JOIN aprs_local_transmissions AS local ON local.callsign = submission.callsign AND local.destination = submission.destination AND local.info = submission.info WHERE submission.provision_fingerprint = ? AND submission.callsign = ? AND submission.destination = ? AND submission.info = ? AND local.transmitted_at >= ? AND local.transmitted_at <= ? ORDER BY local.transmitted_at DESC, submission.updated_at DESC, submission.id ASC LIMIT 1",
        )
        .get(
          provisionFingerprint,
          parsed.callsign,
          parsed.destination,
          parsed.info,
          localCutoff,
          attemptedAt,
        );
      if (recent) {
        const submission = toSubmission(recent as Record<string, unknown>);
        this.database.exec("COMMIT");
        transactionOpen = false;
        return { created: false, writeRequired: false, submission };
      }
      const observerProven = this.database
        .prepare(
          "SELECT * FROM aprs_igate_submissions WHERE provision_fingerprint = ? AND callsign = ? AND destination = ? AND info = ? AND delivery_status = 'observer_confirmed' AND local_write_completed_at IS NULL AND observer_confirmed_at >= ? AND observer_confirmed_at <= ? ORDER BY observer_confirmed_at DESC, updated_at DESC, id ASC LIMIT 1",
        )
        .get(
          provisionFingerprint,
          parsed.callsign,
          parsed.destination,
          parsed.info,
          localCutoff,
          attemptedAt,
        );
      if (observerProven) {
        const submission = toSubmission(
          observerProven as Record<string, unknown>,
        );
        this.database.exec("COMMIT");
        transactionOpen = false;
        return { created: false, writeRequired: false, submission };
      }
      const id = `aprs-igate-${randomUUID()}`;
      this.database
        .prepare(
          "INSERT INTO aprs_igate_submissions (id, callsign, destination, info, packet_kind, provision_fingerprint, delivery_status, attempted_at, submitted_at, observer_confirmed_at, observation_expires_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, 'sending', ?, NULL, NULL, ?, ?)",
        )
        .run(
          id,
          parsed.callsign,
          parsed.destination,
          parsed.info,
          packetValue.kind,
          provisionFingerprint,
          attemptedAt,
          observationExpiresAt,
          attemptedAt,
        );
      const submission = this.requiredSubmission(id);
      this.database.exec("COMMIT");
      transactionOpen = false;
      return { created: true, writeRequired: true, submission };
    } catch {
      if (transactionOpen) {
        rollback(this.database);
      }
      throw new AprsIgatePersistenceError();
    }
  }

  markTransmissionUncertain(
    id: string,
    updatedAt: string,
  ): AprsIgateSubmission {
    validateSubmissionId(id);
    if (!isTimestamp(updatedAt)) {
      throw new AprsIgatePersistenceError();
    }
    try {
      const result = this.database
        .prepare(
          "UPDATE aprs_igate_submissions SET delivery_status = 'transmission_uncertain', updated_at = ? WHERE id = ? AND delivery_status = 'sending' AND attempted_at <= ? AND observation_expires_at >= ?",
        )
        .run(updatedAt, id, updatedAt, updatedAt);
      if (Number(result.changes) !== 1) {
        throw new AprsIgatePersistenceError();
      }
      return this.requiredSubmission(id);
    } catch {
      throw new AprsIgatePersistenceError();
    }
  }

  cancelUnwritten(id: string): boolean {
    validateSubmissionId(id);
    let transactionOpen = false;
    try {
      this.database.exec("BEGIN IMMEDIATE");
      transactionOpen = true;
      const current = this.requiredSubmission(id);
      if (current.deliveryStatus === "observer_confirmed") {
        this.database.exec("COMMIT");
        transactionOpen = false;
        return false;
      }
      if (current.deliveryStatus !== "sending") {
        throw new AprsIgatePersistenceError();
      }
      const result = this.database
        .prepare(
          "DELETE FROM aprs_igate_submissions WHERE id = ? AND delivery_status = 'sending'",
        )
        .run(id);
      if (Number(result.changes) !== 1) {
        throw new AprsIgatePersistenceError();
      }
      this.database.exec("COMMIT");
      transactionOpen = false;
      return true;
    } catch {
      if (transactionOpen) {
        rollback(this.database);
      }
      throw new AprsIgatePersistenceError();
    }
  }

  markSubmitted(id: string, submittedAt: string): AprsIgateSubmission {
    validateSubmissionId(id);
    if (!isTimestamp(submittedAt)) {
      throw new AprsIgatePersistenceError();
    }
    let transactionOpen = false;
    try {
      this.database.exec("BEGIN IMMEDIATE");
      transactionOpen = true;
      const current = this.requiredSubmission(id);
      if (
        current.deliveryStatus !== "sending" ||
        Date.parse(submittedAt) < Date.parse(current.attemptedAt) ||
        Date.parse(submittedAt) > Date.parse(current.observationExpiresAt)
      ) {
        throw new AprsIgatePersistenceError();
      }
      const evidence = this.database
        .prepare(
          "SELECT first_observed_at, last_observed_at FROM aprs_observed_packets WHERE callsign = ? AND destination = ? AND info = ?",
        )
        .get(current.callsign, current.destination, current.info) as
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
                Date.parse(observedAt) >= Date.parse(submittedAt) &&
                Date.parse(observedAt) <=
                  Date.parse(current.observationExpiresAt),
            )
            .sort()[0]
        : undefined;
      const nextStatus = observerConfirmedAt
        ? "observer_confirmed"
        : "submitted";
      this.database
        .prepare(
          "UPDATE aprs_igate_submissions SET delivery_status = ?, submitted_at = ?, local_write_completed_at = ?, observer_confirmed_at = ?, updated_at = CASE WHEN updated_at > ? THEN updated_at ELSE ? END WHERE id = ? AND delivery_status = 'sending'",
        )
        .run(
          nextStatus,
          submittedAt,
          submittedAt,
          observerConfirmedAt ?? null,
          observerConfirmedAt ?? submittedAt,
          observerConfirmedAt ?? submittedAt,
          id,
        );
      this.database
        .prepare(
          "INSERT INTO aprs_local_transmissions (callsign, destination, info, transmitted_at) VALUES (?, ?, ?, ?) ON CONFLICT(callsign, destination, info) DO UPDATE SET transmitted_at = excluded.transmitted_at",
        )
        .run(current.callsign, current.destination, current.info, submittedAt);
      const sequence = telemetrySequence(current);
      if (sequence !== undefined) {
        this.persistSequenceInsideTransaction(current, sequence, submittedAt);
      }
      const submitted = this.requiredSubmission(id);
      this.database.exec("COMMIT");
      transactionOpen = false;
      return submitted;
    } catch {
      if (transactionOpen) {
        rollback(this.database);
      }
      throw new AprsIgatePersistenceError();
    }
  }

  markRepeatedSubmitted(
    id: string,
    reservedAt: string,
    completedAt: string,
  ): AprsIgateSubmission {
    validateSubmissionId(id);
    if (!isTimestamp(reservedAt) || !isTimestamp(completedAt)) {
      throw new AprsIgatePersistenceError();
    }
    let transactionOpen = false;
    try {
      this.database.exec("BEGIN IMMEDIATE");
      transactionOpen = true;
      const current = this.requiredSubmission(id);
      if (
        !["submitted", "observer_confirmed"].includes(current.deliveryStatus) ||
        !current.localWriteCompletedAt ||
        Date.parse(reservedAt) < Date.parse(current.localWriteCompletedAt) ||
        Date.parse(completedAt) < Date.parse(reservedAt) ||
        Date.parse(completedAt) > Date.parse(current.observationExpiresAt)
      ) {
        throw new AprsIgatePersistenceError();
      }
      const local = this.database
        .prepare(
          "SELECT transmitted_at FROM aprs_local_transmissions WHERE callsign = ? AND destination = ? AND info = ?",
        )
        .get(current.callsign, current.destination, current.info) as
        Record<string, unknown> | undefined;
      if (local?.transmitted_at !== reservedAt) {
        throw new AprsIgatePersistenceError();
      }
      const submissionResult = this.database
        .prepare(
          "UPDATE aprs_igate_submissions SET local_write_completed_at = ?, updated_at = CASE WHEN updated_at > ? THEN updated_at ELSE ? END WHERE id = ? AND delivery_status IN ('submitted', 'observer_confirmed')",
        )
        .run(completedAt, completedAt, completedAt, id);
      if (Number(submissionResult.changes) !== 1) {
        throw new AprsIgatePersistenceError();
      }
      const localResult = this.database
        .prepare(
          "UPDATE aprs_local_transmissions SET transmitted_at = ? WHERE callsign = ? AND destination = ? AND info = ? AND transmitted_at = ?",
        )
        .run(
          completedAt,
          current.callsign,
          current.destination,
          current.info,
          reservedAt,
        );
      if (Number(localResult.changes) !== 1) {
        throw new AprsIgatePersistenceError();
      }
      const submitted = this.requiredSubmission(id);
      this.database.exec("COMMIT");
      transactionOpen = false;
      return submitted;
    } catch {
      if (transactionOpen) {
        rollback(this.database);
      }
      throw new AprsIgatePersistenceError();
    }
  }

  recoverInterrupted(
    provisionFingerprint: string,
    recoveredAt: string,
  ): AprsIgateSubmission[] {
    validateFingerprint(provisionFingerprint);
    if (!isTimestamp(recoveredAt)) {
      throw new AprsIgatePersistenceError();
    }
    try {
      this.database
        .prepare(
          "UPDATE aprs_igate_submissions SET delivery_status = 'transmission_uncertain', updated_at = ? WHERE provision_fingerprint = ? AND delivery_status = 'sending' AND observation_expires_at >= ?",
        )
        .run(recoveredAt, provisionFingerprint, recoveredAt);
      this.expireActive(recoveredAt, provisionFingerprint);
      return this.listInternal(200, provisionFingerprint).filter(
        (entry) => entry.deliveryStatus === "transmission_uncertain",
      );
    } catch {
      throw new AprsIgatePersistenceError();
    }
  }

  confirmObserved(
    provisionFingerprint: string,
    callsign: string,
    destination: string,
    info: string,
    observedAt: string,
  ): AprsIgateSubmission[] {
    validateFingerprint(provisionFingerprint);
    validateCallsign(callsign);
    validateDestination(destination);
    if (!isAprsInfo(info) || !isTimestamp(observedAt)) {
      throw new AprsIgatePersistenceError();
    }
    let transactionOpen = false;
    try {
      this.database.exec("BEGIN IMMEDIATE");
      transactionOpen = true;
      const ids = this.database
        .prepare(
          "SELECT id FROM aprs_igate_submissions WHERE provision_fingerprint = ? AND callsign = ? AND destination = ? AND info = ? AND ((delivery_status = 'submitted' AND submitted_at IS NOT NULL AND submitted_at <= ?) OR (delivery_status = 'transmission_uncertain' AND attempted_at <= ?)) AND observation_expires_at >= ? ORDER BY attempted_at ASC, id ASC",
        )
        .all(
          provisionFingerprint,
          callsign,
          destination,
          info,
          observedAt,
          observedAt,
          observedAt,
        )
        .map((row) => String(row.id));
      if (ids.length === 0) {
        this.database.exec("COMMIT");
        transactionOpen = false;
        return [];
      }
      if (ids.length > 1) {
        throw new AprsIgatePersistenceError();
      }
      const id = ids[0]!;
      const confirmed = this.confirmInsideTransaction(id, observedAt);
      this.database.exec("COMMIT");
      transactionOpen = false;
      return [confirmed];
    } catch {
      if (transactionOpen) {
        this.database.exec("ROLLBACK");
      }
      throw new AprsIgatePersistenceError();
    }
  }

  reconcileObserved(
    provisionFingerprint: string,
    reconciledAt: string,
  ): AprsIgateSubmission[] {
    validateFingerprint(provisionFingerprint);
    if (!isTimestamp(reconciledAt)) {
      throw new AprsIgatePersistenceError();
    }
    let transactionOpen = false;
    try {
      this.database.exec("BEGIN IMMEDIATE");
      transactionOpen = true;
      this.expireActiveInsideTransaction(reconciledAt, provisionFingerprint);
      const rows = this.database
        .prepare(
          "SELECT submission.id AS id, observed.last_observed_at AS observed_at FROM aprs_igate_submissions AS submission JOIN aprs_observed_packets AS observed ON observed.callsign = submission.callsign AND observed.destination = submission.destination AND observed.info = submission.info WHERE submission.provision_fingerprint = ? AND ((submission.delivery_status = 'submitted' AND submission.submitted_at IS NOT NULL AND observed.last_observed_at >= submission.submitted_at) OR (submission.delivery_status = 'transmission_uncertain' AND observed.last_observed_at >= submission.attempted_at)) AND observed.last_observed_at <= submission.observation_expires_at AND observed.last_observed_at <= ? ORDER BY submission.attempted_at ASC, submission.id ASC",
        )
        .all(provisionFingerprint, reconciledAt) as Array<{
        id: unknown;
        observed_at: unknown;
      }>;
      const ids = new Set<string>();
      const confirmed: AprsIgateSubmission[] = [];
      for (const row of rows) {
        const id = String(row.id);
        const observedAt = row.observed_at;
        validateSubmissionId(id);
        if (ids.has(id) || !isTimestamp(observedAt)) {
          throw new AprsIgatePersistenceError();
        }
        ids.add(id);
        confirmed.push(this.confirmInsideTransaction(id, observedAt));
      }
      this.database.exec("COMMIT");
      transactionOpen = false;
      return confirmed;
    } catch {
      if (transactionOpen) {
        rollback(this.database);
      }
      throw new AprsIgatePersistenceError();
    }
  }

  expireActive(now: string, provisionFingerprint?: string): number {
    if (!isTimestamp(now)) {
      throw new AprsIgatePersistenceError();
    }
    if (provisionFingerprint !== undefined) {
      validateFingerprint(provisionFingerprint);
    }
    try {
      return this.expireActiveInsideTransaction(now, provisionFingerprint);
    } catch {
      throw new AprsIgatePersistenceError();
    }
  }

  /** @deprecated Use expireActive. */
  expireSubmitted(now: string): number {
    return this.expireActive(now);
  }

  deleteTerminalBefore(cutoffExclusive: string, limit = 1_000): number {
    if (
      !isTimestamp(cutoffExclusive) ||
      !Number.isInteger(limit) ||
      limit < 1 ||
      limit > 10_000
    ) {
      throw new AprsIgatePersistenceError();
    }
    try {
      return Number(
        this.database
          .prepare(
            "DELETE FROM aprs_igate_submissions WHERE id IN (SELECT id FROM aprs_igate_submissions INDEXED BY aprs_igate_submissions_retention_index WHERE delivery_status IN ('observer_confirmed', 'observation_expired') AND updated_at < ? ORDER BY updated_at ASC, id ASC LIMIT ?)",
          )
          .run(cutoffExclusive, limit).changes,
      );
    } catch {
      throw new AprsIgatePersistenceError();
    }
  }

  list(limit = 200): AprsIgateSubmission[] {
    if (!Number.isInteger(limit) || limit < 1 || limit > 200) {
      throw new AprsIgatePersistenceError();
    }
    try {
      return this.listInternal(limit);
    } catch {
      throw new AprsIgatePersistenceError();
    }
  }

  listPublic(limit = 200): PublicAprsIgateSubmission[] {
    return this.list(limit).map(publicSubmission);
  }

  deliveryCounts(provisionFingerprint?: string): AprsIgateDeliveryCounts {
    if (provisionFingerprint !== undefined) {
      validateFingerprint(provisionFingerprint);
    }
    try {
      const rows = this.database
        .prepare(
          `SELECT delivery_status, COUNT(*) AS count FROM aprs_igate_submissions${
            provisionFingerprint === undefined
              ? ""
              : " WHERE provision_fingerprint = ?"
          } GROUP BY delivery_status`,
        )
        .all(
          ...(provisionFingerprint === undefined ? [] : [provisionFingerprint]),
        ) as Array<{
        delivery_status: unknown;
        count: unknown;
      }>;
      const count = (statuses: readonly AprsIgateDeliveryStatus[]) =>
        rows
          .filter((row) =>
            statuses.includes(row.delivery_status as AprsIgateDeliveryStatus),
          )
          .reduce((total, row) => total + Number(row.count), 0);
      return {
        pending: count(["sending", "transmission_uncertain", "submitted"]),
        uncertain: count(["transmission_uncertain"]),
        unconfirmed: count(["observation_expired"]),
      };
    } catch {
      throw new AprsIgatePersistenceError();
    }
  }

  private expireActiveInsideTransaction(
    now: string,
    provisionFingerprint?: string,
  ): number {
    const result =
      provisionFingerprint === undefined
        ? this.database
            .prepare(
              "UPDATE aprs_igate_submissions SET delivery_status = 'observation_expired', updated_at = ? WHERE delivery_status IN ('sending', 'transmission_uncertain', 'submitted') AND observation_expires_at <= ?",
            )
            .run(now, now)
        : this.database
            .prepare(
              "UPDATE aprs_igate_submissions SET delivery_status = 'observation_expired', updated_at = ? WHERE provision_fingerprint = ? AND delivery_status IN ('sending', 'transmission_uncertain', 'submitted') AND observation_expires_at <= ?",
            )
            .run(now, provisionFingerprint, now);
    return Number(result.changes);
  }

  private persistSequenceInsideTransaction(
    submission: AprsIgateSubmission,
    sequence: number,
    updatedAt: string,
  ): void {
    this.database
      .prepare(
        "INSERT INTO aprs_igate_state (callsign, provision_fingerprint, last_successful_telemetry_sequence, updated_at) VALUES (?, ?, ?, ?) ON CONFLICT(callsign) DO UPDATE SET provision_fingerprint = excluded.provision_fingerprint, last_successful_telemetry_sequence = excluded.last_successful_telemetry_sequence, updated_at = excluded.updated_at",
      )
      .run(
        submission.callsign,
        submission.provisionFingerprint,
        sequence,
        updatedAt,
      );
  }

  private confirmInsideTransaction(
    id: string,
    observedAt: string,
  ): AprsIgateSubmission {
    const current = this.requiredSubmission(id);
    const result = this.database
      .prepare(
        "UPDATE aprs_igate_submissions SET delivery_status = 'observer_confirmed', observer_confirmed_at = ?, updated_at = ? WHERE id = ? AND delivery_status IN ('transmission_uncertain', 'submitted')",
      )
      .run(observedAt, observedAt, id);
    if (Number(result.changes) !== 1) {
      throw new AprsIgatePersistenceError();
    }
    if (current.deliveryStatus === "transmission_uncertain") {
      const sequence = telemetrySequence(current);
      if (sequence !== undefined) {
        this.persistSequenceInsideTransaction(current, sequence, observedAt);
      }
    }
    return this.requiredSubmission(id);
  }

  private listInternal(
    limit: number,
    provisionFingerprint?: string,
  ): AprsIgateSubmission[] {
    const statement =
      provisionFingerprint === undefined
        ? this.database.prepare(
            "SELECT * FROM aprs_igate_submissions ORDER BY attempted_at DESC, id ASC LIMIT ?",
          )
        : this.database.prepare(
            "SELECT * FROM aprs_igate_submissions WHERE provision_fingerprint = ? ORDER BY attempted_at DESC, id ASC LIMIT ?",
          );
    const rows =
      provisionFingerprint === undefined
        ? statement.all(limit)
        : statement.all(provisionFingerprint, limit);
    return rows.map((row) => toSubmission(row as Record<string, unknown>));
  }

  private requiredSubmission(id: string): AprsIgateSubmission {
    const row = this.database
      .prepare("SELECT * FROM aprs_igate_submissions WHERE id = ?")
      .get(id);
    if (!row) {
      throw new AprsIgatePersistenceError();
    }
    return toSubmission(row as Record<string, unknown>);
  }
}

export function encodeAprsIgateBeacon(
  provision: CallMeshProvision,
): AprsIgatePacket {
  const callsign = provisionCallsign(provision);
  const latitude = formatCoordinate(provision.latitude, "latitude");
  const longitude = formatCoordinate(provision.longitude, "longitude");
  const symbolTable = provision.symbolOverlay ?? provision.symbolTable;
  const symbolCode = provision.symbolCode;
  validateSymbol(symbolTable);
  validateSymbol(symbolCode);

  const altitude = formatAltitude(provision.altitudeMeters);
  const phg = formatPhg(provision.phg);
  const mandatory = `${header(callsign)}:!${latitude}${symbolTable}${longitude}${symbolCode}${altitude}${phg}`;
  const comment = truncateOptionalComment(
    sanitizeComment(provision.comment),
    mandatory,
  );
  return packet("beacon", `${mandatory}${comment}`);
}

export function encodeAprsIgateStatus(
  provision: CallMeshProvision,
  version: string,
): AprsIgatePacket {
  if (
    typeof version !== "string" ||
    !version.trim() ||
    /[\r\n]/u.test(version) ||
    containsUnsafeControl(version)
  ) {
    throw new AprsIgateEncodingError();
  }
  return packet(
    "status",
    `${header(provisionCallsign(provision))}:>TMAG Client v${version}`,
  );
}

export function encodeAprsIgateTelemetryDefinitions(
  provision: CallMeshProvision,
): AprsIgatePacket[] {
  const callsign = provisionCallsign(provision);
  const destination = messageDestination(callsign);
  const prefix = `${header(callsign)}::${destination}:`;
  return [
    packet(
      "telemetry-parm",
      `${prefix}PARM.ALL_PKTS_10M,FWD_APRS_10M,POS_PKTS_10M,MSG_PKTS_10M,CTRL_PKTS_10M`,
    ),
    packet("telemetry-unit", `${prefix}UNIT.cnt,cnt,cnt,cnt,cnt`),
    packet("telemetry-eqns", `${prefix}EQNS.0,1,0,0,1,0,0,1,0,0,1,0,0,1,0`),
  ];
}

export function encodeAprsIgateTelemetryData(
  provision: CallMeshProvision,
  sequence: number,
  counters: AprsIgateCounters,
): AprsIgatePacket {
  if (!Number.isInteger(sequence) || sequence < 1 || sequence > 999) {
    throw new AprsIgateEncodingError();
  }
  const values = [
    counters.all,
    counters.forwardedAprs,
    counters.position,
    counters.message,
    counters.control,
  ].map(formatTelemetryValue);
  const info = `T#${String(sequence).padStart(3, "0")},${values.join(",")},00000000`;
  return packet(
    "telemetry-data",
    `${header(provisionCallsign(provision))}:${info}`,
  );
}

/**
 * Timer-free iGate family scheduler. The runtime supplies exact verified-login
 * and clock events, then persists only persistentState() after a successful
 * telemetry write.
 */
export class AprsIgateFamily {
  private readonly provision: CallMeshProvision;
  private readonly version: string;
  private readonly beaconIntervalMs: number;
  private readonly buckets = new Map<number, TelemetryBucket>();
  private verified = false;
  private verifiedBefore = false;
  private statusSuccessfullyWritten = false;
  private lastBeaconAt: number | undefined;
  private lastDefinitionAt: number | undefined;
  private lastTelemetryAt: number | undefined;
  private nextBeaconAt = Number.NEGATIVE_INFINITY;
  private nextDefinitionAt = Number.NEGATIVE_INFINITY;
  private nextTelemetryAt = Number.NEGATIVE_INFINITY;
  private nextStatusAt = Number.NEGATIVE_INFINITY;
  private telemetrySequence: number;
  private lastSuccessfulTelemetrySequence: number;

  constructor(options: AprsIgateFamilyOptions) {
    encodeAprsIgateBeacon(options.provision);
    encodeAprsIgateStatus(options.provision, options.version);
    encodeAprsIgateTelemetryDefinitions(options.provision);
    const restoredSequence = options.lastSuccessfulTelemetrySequence ?? 0;
    if (
      !Number.isInteger(restoredSequence) ||
      restoredSequence < 0 ||
      restoredSequence > 999
    ) {
      throw new AprsIgateEncodingError();
    }
    this.provision = { ...options.provision };
    this.version = options.version;
    this.beaconIntervalMs = clampBeaconInterval(
      options.beaconIntervalMs ?? APRS_IGATE_DEFAULT_BEACON_INTERVAL_MS,
    );
    this.telemetrySequence = restoredSequence;
    this.lastSuccessfulTelemetrySequence = restoredSequence;
  }

  recordDecodedSummary(type: string, timestampMs: number): void {
    const bucket = this.bucket(timestampMs);
    bucket.all += 1;
    const normalizedType = typeof type === "string" ? type.toLowerCase() : "";
    if (POSITION_TYPES.has(normalizedType)) {
      bucket.position += 1;
    } else if (MESSAGE_TYPES.has(normalizedType)) {
      bucket.message += 1;
    } else if (CONTROL_TYPES.has(normalizedType)) {
      bucket.control += 1;
    }
    this.prune(timestampMs);
  }

  recordTrackerForward(timestampMs: number): void {
    this.bucket(timestampMs).forwardedAprs += 1;
    this.prune(timestampMs);
  }

  async onVerifiedLogin(
    nowMs: number,
    writer: AprsIgateWriter,
  ): Promise<AprsIgateWriteOutcome[]> {
    validateTime(nowMs);
    validateWriter(writer);
    if (this.verified) {
      return this.runDue(nowMs, writer);
    }
    const reconnect = this.verifiedBefore;
    this.verified = true;
    this.verifiedBefore = true;

    if (!reconnect) {
      this.nextBeaconAt = nowMs;
      this.nextDefinitionAt = nowMs;
      this.nextTelemetryAt = nowMs;
      this.nextStatusAt = nowMs;
    } else {
      this.nextBeaconAt =
        this.lastBeaconAt === undefined ? nowMs : nowMs + this.beaconIntervalMs;
      this.nextDefinitionAt = dueFromSuccessfulAnchor(
        this.lastDefinitionAt,
        APRS_IGATE_DEFINITION_INTERVAL_MS,
        nowMs,
      );
      this.nextTelemetryAt = dueFromSuccessfulAnchor(
        this.lastTelemetryAt,
        APRS_IGATE_TELEMETRY_INTERVAL_MS,
        nowMs,
      );
      this.nextStatusAt = this.statusSuccessfullyWritten
        ? nowMs + APRS_IGATE_STATUS_INTERVAL_MS
        : nowMs;
    }
    return this.runDue(nowMs, writer);
  }

  onDisconnected(): void {
    this.verified = false;
  }

  async runDue(
    nowMs: number,
    writer: AprsIgateWriter,
  ): Promise<AprsIgateWriteOutcome[]> {
    validateTime(nowMs);
    validateWriter(writer);
    if (!this.verified) {
      return [];
    }

    const outcomes: AprsIgateWriteOutcome[] = [];
    if (nowMs >= this.nextBeaconAt) {
      const outcome = await attemptWrite(
        encodeAprsIgateBeacon(this.provision),
        writer,
      );
      outcomes.push(outcome);
      if (outcome.successful) {
        this.lastBeaconAt = nowMs;
      }
      this.nextBeaconAt = nowMs + this.beaconIntervalMs;
    }

    if (nowMs >= this.nextDefinitionAt) {
      let definitionsSuccessful = true;
      for (const definition of encodeAprsIgateTelemetryDefinitions(
        this.provision,
      )) {
        const outcome = await attemptWrite(definition, writer);
        outcomes.push(outcome);
        if (!outcome.successful) {
          definitionsSuccessful = false;
          break;
        }
      }
      if (definitionsSuccessful) {
        this.lastDefinitionAt = nowMs;
      }
      this.nextDefinitionAt = nowMs + APRS_IGATE_DEFINITION_INTERVAL_MS;
    }

    if (nowMs >= this.nextTelemetryAt) {
      this.telemetrySequence = (this.telemetrySequence % 999) + 1;
      const telemetry = encodeAprsIgateTelemetryData(
        this.provision,
        this.telemetrySequence,
        this.windowCounters(nowMs),
      );
      const outcome = await attemptWrite(telemetry, writer);
      outcomes.push(outcome);
      if (outcome.successful) {
        this.lastSuccessfulTelemetrySequence = this.telemetrySequence;
        this.lastTelemetryAt = nowMs;
      }
      this.nextTelemetryAt = nowMs + APRS_IGATE_TELEMETRY_INTERVAL_MS;
    }

    if (nowMs >= this.nextStatusAt) {
      const outcome = await attemptWrite(
        encodeAprsIgateStatus(this.provision, this.version),
        writer,
      );
      outcomes.push(outcome);
      if (outcome.successful) {
        this.statusSuccessfullyWritten = true;
      }
      this.advanceStatusBoundary(nowMs);
    }
    return outcomes;
  }

  persistentState(): AprsIgatePersistentState {
    return {
      lastSuccessfulTelemetrySequence: this.lastSuccessfulTelemetrySequence,
    };
  }

  nextDueAt(): number | undefined {
    if (!this.verified) {
      return undefined;
    }
    return Math.min(
      this.nextBeaconAt,
      this.nextDefinitionAt,
      this.nextTelemetryAt,
      this.nextStatusAt,
    );
  }

  private bucket(timestampMs: number): TelemetryBucket {
    validateTime(timestampMs);
    const bucketStart =
      Math.floor(timestampMs / TELEMETRY_BUCKET_MS) * TELEMETRY_BUCKET_MS;
    let bucket = this.buckets.get(bucketStart);
    if (!bucket) {
      bucket = {
        all: 0,
        forwardedAprs: 0,
        position: 0,
        message: 0,
        control: 0,
      };
      this.buckets.set(bucketStart, bucket);
    }
    return bucket;
  }

  private prune(nowMs: number): void {
    const cutoff = nowMs - TELEMETRY_WINDOW_MS;
    for (const bucketStart of this.buckets.keys()) {
      if (bucketStart + TELEMETRY_BUCKET_MS <= cutoff) {
        this.buckets.delete(bucketStart);
      }
    }
  }

  private windowCounters(nowMs: number): AprsIgateCounters {
    this.prune(nowMs);
    const counters: AprsIgateCounters = {
      all: 0,
      forwardedAprs: 0,
      position: 0,
      message: 0,
      control: 0,
    };
    const cutoff = nowMs - TELEMETRY_WINDOW_MS;
    for (const [bucketStart, bucket] of this.buckets) {
      if (bucketStart + TELEMETRY_BUCKET_MS <= cutoff || bucketStart > nowMs) {
        continue;
      }
      counters.all += bucket.all;
      counters.forwardedAprs += bucket.forwardedAprs;
      counters.position += bucket.position;
      counters.message += bucket.message;
      counters.control += bucket.control;
    }
    return counters;
  }

  private advanceStatusBoundary(nowMs: number): void {
    if (!Number.isFinite(this.nextStatusAt)) {
      this.nextStatusAt = nowMs + APRS_IGATE_STATUS_INTERVAL_MS;
      return;
    }
    do {
      this.nextStatusAt += APRS_IGATE_STATUS_INTERVAL_MS;
    } while (this.nextStatusAt <= nowMs);
  }
}

async function attemptWrite(
  packetValue: AprsIgatePacket,
  writer: AprsIgateWriter,
): Promise<AprsIgateWriteOutcome> {
  try {
    return {
      packet: packetValue,
      successful: (await writer(packetValue)) === true,
    };
  } catch {
    return { packet: packetValue, successful: false };
  }
}

function provisionCallsign(provision: CallMeshProvision): string {
  const base = provision.callsignBase;
  const ssid = provision.ssid;
  if (
    typeof base !== "string" ||
    !/^[A-Z0-9]{1,6}$/u.test(base) ||
    !Number.isInteger(ssid) ||
    ssid < -15 ||
    ssid > 15
  ) {
    throw new AprsIgateEncodingError();
  }
  return ssid === 0 ? base : `${base}${ssid < 0 ? ssid : `-${ssid}`}`;
}

function header(callsign: string): string {
  return `${callsign}>${APRS_DESTINATION},${APRS_IGATE_PATH}`;
}

function packet(kind: AprsIgatePacketKind, data: string): AprsIgatePacket {
  if (
    /[\r\n]/u.test(data) ||
    Buffer.byteLength(data, "utf8") > APRS_DATA_LIMIT_BYTES
  ) {
    throw new AprsIgateEncodingError();
  }
  return { kind, data };
}

function formatCoordinate(
  value: number | undefined,
  kind: "latitude" | "longitude",
): string {
  const limit = kind === "latitude" ? 90 : 180;
  if (
    value === undefined ||
    !Number.isFinite(value) ||
    Math.abs(value) > limit
  ) {
    throw new AprsIgateEncodingError();
  }
  const absolute = Math.abs(value);
  let degrees = Math.floor(absolute);
  let minutes = (absolute - degrees) * 60;
  if (minutes >= 59.995) {
    degrees += 1;
    minutes = 0;
  }
  if (degrees > limit || (degrees === limit && minutes !== 0)) {
    throw new AprsIgateEncodingError();
  }
  const width = kind === "latitude" ? 2 : 3;
  const hemisphere =
    kind === "latitude" ? (value < 0 ? "S" : "N") : value < 0 ? "W" : "E";
  return `${String(degrees).padStart(width, "0")}${minutes.toFixed(2).padStart(5, "0")}${hemisphere}`;
}

function formatAltitude(value: number | null | undefined): string {
  if (value === undefined || value === null) {
    return "";
  }
  if (!Number.isFinite(value)) {
    throw new AprsIgateEncodingError();
  }
  const feet = Math.round(value * 3.28084);
  const clamped = Math.min(999_999, Math.max(0, feet));
  return `/A=${String(clamped).padStart(6, "0")}`;
}

function formatPhg(value: string | null | undefined): string {
  if (value === undefined || value === null) {
    return "";
  }
  const normalized = String(value).trim().toUpperCase();
  if (!/^[0-9]{3,4}$/u.test(normalized)) {
    return "";
  }
  return `PHG${normalized.length === 3 ? `${normalized}0` : normalized}`;
}

function sanitizeComment(value: string | undefined): string {
  if (!value) {
    return "";
  }
  return String(value)
    .replace(/[\r\n]+/gu, " ")
    .replace(/\s+/gu, " ")
    .trim();
}

function truncateOptionalComment(comment: string, mandatory: string): string {
  const mandatoryBytes = Buffer.byteLength(mandatory, "utf8");
  if (mandatoryBytes > APRS_DATA_LIMIT_BYTES) {
    throw new AprsIgateEncodingError();
  }
  let retained = "";
  let retainedBytes = mandatoryBytes;
  for (const codePoint of comment) {
    const codePointBytes = Buffer.byteLength(codePoint, "utf8");
    if (retainedBytes + codePointBytes > APRS_DATA_LIMIT_BYTES) {
      break;
    }
    retained += codePoint;
    retainedBytes += codePointBytes;
  }
  return retained;
}

function messageDestination(callsign: string): string {
  return callsign
    .toUpperCase()
    .replace(/[^A-Z0-9-]/gu, "")
    .slice(0, 9)
    .padEnd(9, " ");
}

function formatTelemetryValue(value: number): string {
  if (!Number.isFinite(value)) {
    throw new AprsIgateEncodingError();
  }
  return String(Math.min(999, Math.max(0, Math.round(value))));
}

function validateSymbol(value: string): void {
  if (typeof value !== "string" || !/^[ -~]$/u.test(value)) {
    throw new AprsIgateEncodingError();
  }
}

function containsUnsafeControl(value: string): boolean {
  for (const character of value) {
    const code = character.codePointAt(0);
    if (code !== undefined && (code < 0x20 || code === 0x7f)) {
      return true;
    }
  }
  return false;
}

function clampBeaconInterval(value: number): number {
  if (!Number.isFinite(value) || value <= 0) {
    throw new AprsIgateEncodingError();
  }
  return Math.min(
    MAX_BEACON_INTERVAL_MS,
    Math.max(MIN_BEACON_INTERVAL_MS, Math.round(value)),
  );
}

function dueFromSuccessfulAnchor(
  lastSuccessfulAt: number | undefined,
  intervalMs: number,
  nowMs: number,
): number {
  if (lastSuccessfulAt === undefined) {
    return nowMs;
  }
  return Math.max(lastSuccessfulAt + intervalMs, nowMs);
}

function validateTime(value: number): void {
  if (!Number.isFinite(value) || value < 0) {
    throw new AprsIgateEncodingError();
  }
}

function validateWriter(writer: AprsIgateWriter): void {
  if (typeof writer !== "function") {
    throw new AprsIgateEncodingError();
  }
}

function validateCallsign(value: unknown): asserts value is string {
  if (typeof value !== "string" || !APRS_CALLSIGN_PATTERN.test(value)) {
    throw new AprsIgatePersistenceError();
  }
}

function validateDestination(value: unknown): asserts value is string {
  if (typeof value !== "string" || !APRS_DESTINATION_PATTERN.test(value)) {
    throw new AprsIgatePersistenceError();
  }
}

function isPacketKind(value: unknown): value is AprsIgatePacketKind {
  return (
    value === "beacon" ||
    value === "status" ||
    value === "telemetry-parm" ||
    value === "telemetry-unit" ||
    value === "telemetry-eqns" ||
    value === "telemetry-data"
  );
}

function isDeliveryStatus(value: unknown): value is AprsIgateDeliveryStatus {
  return (
    value === "sending" ||
    value === "transmission_uncertain" ||
    value === "submitted" ||
    value === "observer_confirmed" ||
    value === "observation_expired"
  );
}

function validateFingerprint(value: unknown): asserts value is string {
  if (typeof value !== "string" || !PROVISION_FINGERPRINT_PATTERN.test(value)) {
    throw new AprsIgatePersistenceError();
  }
}

function validateSubmissionId(value: unknown): asserts value is string {
  if (typeof value !== "string" || !/^aprs-igate-[a-f0-9-]{36}$/u.test(value)) {
    throw new AprsIgatePersistenceError();
  }
}

function isTimestamp(value: unknown): value is string {
  return typeof value === "string" && Number.isFinite(Date.parse(value));
}

function isTelemetrySequence(value: number, allowZero: boolean): boolean {
  return (
    Number.isInteger(value) && value >= (allowZero ? 0 : 1) && value <= 999
  );
}

function isClientOriginatedIgateData(data: string, callsign: string): boolean {
  return (
    Buffer.byteLength(data, "utf8") <= APRS_DATA_LIMIT_BYTES &&
    data.startsWith(`${callsign}>${APRS_DESTINATION},${APRS_IGATE_PATH}:`)
  );
}

function isAprsInfo(value: unknown): value is string {
  return (
    typeof value === "string" &&
    value.length > 0 &&
    !/[\r\n]/u.test(value) &&
    Buffer.byteLength(value, "utf8") <= APRS_DATA_LIMIT_BYTES
  );
}

function toSubmission(row: Record<string, unknown>): AprsIgateSubmission {
  const id = row.id;
  const callsign = row.callsign;
  const destination = row.destination;
  const info = row.info;
  const packetKind = row.packet_kind;
  const provisionFingerprint = row.provision_fingerprint;
  const deliveryStatus = row.delivery_status;
  const attemptedAt = row.attempted_at;
  const submittedAt = row.submitted_at;
  const localWriteCompletedAt = row.local_write_completed_at;
  const observerConfirmedAt = row.observer_confirmed_at;
  const observationExpiresAt = row.observation_expires_at;
  const updatedAt = row.updated_at;

  validateCallsign(callsign);
  validateDestination(destination);
  if (
    typeof id !== "string" ||
    !/^aprs-igate-[a-f0-9-]{36}$/u.test(id) ||
    !isAprsInfo(info) ||
    destination !== APRS_DESTINATION ||
    Buffer.byteLength(
      `${callsign}>${destination},${APRS_IGATE_PATH}:${info}`,
      "utf8",
    ) > APRS_DATA_LIMIT_BYTES ||
    !isPacketKind(packetKind) ||
    typeof provisionFingerprint !== "string" ||
    !PROVISION_FINGERPRINT_PATTERN.test(provisionFingerprint) ||
    !isDeliveryStatus(deliveryStatus) ||
    !isTimestamp(attemptedAt) ||
    !isTimestamp(updatedAt) ||
    !isTimestamp(observationExpiresAt) ||
    Date.parse(observationExpiresAt) !==
      Date.parse(attemptedAt) + OBSERVATION_WINDOW_MS ||
    Date.parse(updatedAt) < Date.parse(attemptedAt)
  ) {
    throw new AprsIgatePersistenceError();
  }

  const hasConfirmation = observerConfirmedAt !== null;
  const hasSubmission = submittedAt !== null;
  const hasLocalWrite = localWriteCompletedAt !== null;
  if (
    (deliveryStatus === "observer_confirmed") !== hasConfirmation ||
    (hasSubmission &&
      (!isTimestamp(submittedAt) ||
        Date.parse(submittedAt) < Date.parse(attemptedAt) ||
        Date.parse(submittedAt) > Date.parse(observationExpiresAt))) ||
    (deliveryStatus === "submitted" && (!hasSubmission || !hasLocalWrite)) ||
    (hasLocalWrite &&
      (!hasSubmission ||
        !isTimestamp(localWriteCompletedAt) ||
        Date.parse(localWriteCompletedAt) < Date.parse(submittedAt) ||
        Date.parse(localWriteCompletedAt) >
          Date.parse(observationExpiresAt))) ||
    (hasConfirmation &&
      (!isTimestamp(observerConfirmedAt) ||
        Date.parse(observerConfirmedAt) < Date.parse(attemptedAt) ||
        Date.parse(observerConfirmedAt) > Date.parse(observationExpiresAt) ||
        (hasSubmission &&
          Date.parse(observerConfirmedAt) < Date.parse(submittedAt))))
  ) {
    throw new AprsIgatePersistenceError();
  }

  return {
    id,
    callsign,
    destination,
    info,
    packetKind,
    provisionFingerprint,
    deliveryStatus,
    attemptedAt,
    updatedAt,
    observationExpiresAt,
    ...(typeof submittedAt === "string" ? { submittedAt } : {}),
    ...(typeof localWriteCompletedAt === "string"
      ? { localWriteCompletedAt }
      : {}),
    ...(typeof observerConfirmedAt === "string" ? { observerConfirmedAt } : {}),
  };
}

function telemetrySequence(
  submission: AprsIgateSubmission,
): number | undefined {
  if (submission.packetKind !== "telemetry-data") {
    return undefined;
  }
  const match = /^T#([0-9]{3}),/u.exec(submission.info);
  const sequence = Number(match?.[1]);
  if (!match || !isTelemetrySequence(sequence, false)) {
    throw new AprsIgatePersistenceError();
  }
  return sequence;
}

function publicSubmission(
  submission: AprsIgateSubmission,
): PublicAprsIgateSubmission {
  return {
    id: submission.id,
    packetKind: submission.packetKind,
    deliveryStatus: submission.deliveryStatus,
    attemptedAt: submission.attemptedAt,
    updatedAt: submission.updatedAt,
    observationExpiresAt: submission.observationExpiresAt,
    ...(submission.submittedAt ? { submittedAt: submission.submittedAt } : {}),
    ...(submission.localWriteCompletedAt
      ? { localWriteCompletedAt: submission.localWriteCompletedAt }
      : {}),
    ...(submission.observerConfirmedAt
      ? { observerConfirmedAt: submission.observerConfirmedAt }
      : {}),
  };
}

function rollback(database: DatabaseSync): void {
  try {
    database.exec("ROLLBACK");
  } catch {
    // Preserve the stable persistence failure from the original operation.
  }
}
