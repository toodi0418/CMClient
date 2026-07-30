import { describe, expect, it } from "vitest";

import type { CallMeshProvision } from "@cmclient/contracts";

import {
  APRS_IGATE_DEFINITION_INTERVAL_MS,
  APRS_IGATE_STATUS_INTERVAL_MS,
  APRS_IGATE_TELEMETRY_INTERVAL_MS,
  AprsIgateEncodingError,
  AprsIgateFamily,
  AprsIgatePersistenceError,
  AprsIgateRepository,
  encodeAprsIgateBeacon,
  encodeAprsIgateStatus,
  encodeAprsIgateTelemetryData,
  encodeAprsIgateTelemetryDefinitions,
  type AprsIgatePacket,
} from "./aprs-igate";
import { parseCmClientAprsLine } from "./aprs-monitor";
import { GatewayDatabase } from "./persistence/database";

const MINUTE_MS = 60_000;
const START = 1_800_000_000_000;
const PROVISION_FINGERPRINT = "a".repeat(64);
const provision: CallMeshProvision = {
  callsignBase: "N1GATE",
  ssid: 10,
  symbolTable: "/",
  symbolCode: "I",
  symbolOverlay: null,
  latitude: 25.079_166_666_666_666,
  longitude: 121.473_666_666_666_66,
  altitudeMeters: 10,
  phg: "123",
  comment: "  Station\r\n beacon  ",
};

describe("Legacy-compatible APRS iGate packet family", () => {
  it("encodes the provision beacon, PHG, status, and definitions exactly", () => {
    expect(encodeAprsIgateBeacon(provision)).toEqual({
      kind: "beacon",
      data: "N1GATE-10>APTMAG,TCPIP*:!2504.75N/12128.42EI/A=000033PHG1230Station beacon",
    });
    expect(encodeAprsIgateStatus(provision, "2.0.0-rc.1")).toEqual({
      kind: "status",
      data: "N1GATE-10>APTMAG,TCPIP*:>TMAG Client v2.0.0-rc.1",
    });
    expect(encodeAprsIgateTelemetryDefinitions(provision)).toEqual([
      {
        kind: "telemetry-parm",
        data: "N1GATE-10>APTMAG,TCPIP*::N1GATE-10:PARM.ALL_PKTS_10M,FWD_APRS_10M,POS_PKTS_10M,MSG_PKTS_10M,CTRL_PKTS_10M",
      },
      {
        kind: "telemetry-unit",
        data: "N1GATE-10>APTMAG,TCPIP*::N1GATE-10:UNIT.cnt,cnt,cnt,cnt,cnt",
      },
      {
        kind: "telemetry-eqns",
        data: "N1GATE-10>APTMAG,TCPIP*::N1GATE-10:EQNS.0,1,0,0,1,0,0,1,0,0,1,0,0,1,0",
      },
    ]);
  });

  it("uses overlay, omits invalid optional extensions, and formats SSID zero", () => {
    const noSsid = withoutComment({
      ...provision,
      ssid: 0,
      symbolOverlay: "8",
      altitudeMeters: null,
      phg: null,
    });
    expect(encodeAprsIgateBeacon(noSsid).data).toBe(
      "N1GATE>APTMAG,TCPIP*:!2504.75N812128.42EI",
    );
    expect(encodeAprsIgateTelemetryDefinitions(noSsid)[0]?.data).toContain(
      "::N1GATE   :PARM.",
    );
  });

  it("preserves Legacy minute carry and southern/western hemispheres", () => {
    expect(
      encodeAprsIgateBeacon(
        withoutComment({
          ...provision,
          latitude: -24.999_916_7,
          longitude: -121.999_916_7,
          altitudeMeters: null,
          phg: null,
        }),
      ).data,
    ).toContain(":!2500.00S/12200.00WI");
  });

  it("truncates only the optional comment at a UTF-8 code-point boundary", () => {
    const withoutCommentData = encodeAprsIgateBeacon(
      withoutComment(provision),
    ).data;
    const encoded = encodeAprsIgateBeacon({
      ...provision,
      comment: "測".repeat(400),
    }).data;
    expect(Buffer.byteLength(`${encoded}\r\n`, "utf8")).toBeLessThanOrEqual(
      512,
    );
    expect(encoded.startsWith(withoutCommentData)).toBe(true);
    const retained = encoded.slice(withoutCommentData.length);
    expect(retained).toMatch(/^測+$/u);
    expect(Buffer.byteLength(`${encoded}測\r\n`, "utf8")).toBeGreaterThan(512);
  });

  it("budgets isolated UTF-16 surrogates using replacement-character bytes", () => {
    const withoutCommentData = encodeAprsIgateBeacon(
      withoutComment(provision),
    ).data;
    const encoded = encodeAprsIgateBeacon({
      ...provision,
      comment: "\ud800".repeat(400),
    }).data;
    const suffix = encoded.slice(withoutCommentData.length);
    expect(suffix.length).toBeGreaterThan(0);
    expect([...suffix].every((value) => value === "\ud800")).toBe(true);
    expect(
      Buffer.from(encoded, "utf8").includes(Buffer.from([0xef, 0xbf, 0xbd])),
    ).toBe(true);
    expect(Buffer.byteLength(`${encoded}\r\n`, "utf8")).toBeLessThanOrEqual(
      512,
    );
    expect(Buffer.byteLength(`${encoded}\ud800\r\n`, "utf8")).toBeGreaterThan(
      512,
    );
  });

  it("rounds and clamps telemetry values without zero padding", () => {
    const encoded = encodeAprsIgateTelemetryData(provision, 999, {
      all: 1_005,
      forwardedAprs: -2,
      position: 1.6,
      message: 1.4,
      control: 0,
    }).data;
    expect(encoded.endsWith(":T#999,999,0,2,1,0,00000000")).toBe(true);
  });

  it("fails closed when mandatory data is invalid or cannot fit", () => {
    expect(() => encodeAprsIgateBeacon(withoutLatitude(provision))).toThrow(
      AprsIgateEncodingError,
    );
    expect(() => encodeAprsIgateStatus(provision, "x".repeat(600))).toThrow(
      AprsIgateEncodingError,
    );
  });

  it("classifies every Legacy packet type into rolling one-minute buckets", async () => {
    const family = new AprsIgateFamily({ provision, version: "2.0.0" });
    const positionTypes = [
      "position",
      "waypoint",
      "envtelemetry",
      "telemetry",
      "remotetelemetry",
      "remoteposition",
    ];
    const messageTypes = ["text", "message", "data", "storeforward"];
    const controlTypes = [
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
    ];
    for (const type of [
      ...positionTypes,
      ...messageTypes,
      ...controlTypes,
      "unknown",
    ]) {
      family.recordDecodedSummary(type, START - 9 * MINUTE_MS);
    }
    family.recordTrackerForward(START - 2 * MINUTE_MS);

    const writes = await family.onVerifiedLogin(START, successfulWriter());
    expect(writes.map((write) => write.packet.kind)).toEqual([
      "beacon",
      "telemetry-parm",
      "telemetry-unit",
      "telemetry-eqns",
      "telemetry-data",
      "status",
    ]);
    expect(writes[4]?.packet.data.endsWith(":T#001,22,1,6,4,11,00000000")).toBe(
      true,
    );
    expect(family.persistentState()).toEqual({
      lastSuccessfulTelemetrySequence: 1,
    });
  });

  it("excludes a bucket at the exact preceding-window boundary", async () => {
    const family = new AprsIgateFamily({ provision, version: "2.0.0" });
    family.recordDecodedSummary("position", START - 10 * MINUTE_MS);
    family.recordDecodedSummary("message", START - 10 * MINUTE_MS - 1);
    const writes = await family.onVerifiedLogin(START, successfulWriter());
    expect(writes[4]?.packet.data.endsWith(":T#001,1,0,1,0,0,00000000")).toBe(
      true,
    );
  });

  it("emits only after verification and at exact successful-write boundaries", async () => {
    const family = new AprsIgateFamily({
      provision,
      version: "2.0.0",
      beaconIntervalMs: 1,
    });
    expect(await family.runDue(START, successfulWriter())).toEqual([]);
    await family.onVerifiedLogin(START, successfulWriter());
    expect(
      await family.runDue(START + MINUTE_MS - 1, successfulWriter()),
    ).toEqual([]);
    expect(
      (await family.runDue(START + MINUTE_MS, successfulWriter())).map(
        (write) => write.packet.kind,
      ),
    ).toEqual(["beacon"]);
    expect(
      (
        await family.runDue(
          START + APRS_IGATE_TELEMETRY_INTERVAL_MS,
          successfulWriter(),
        )
      ).map((write) => write.packet.kind),
    ).toEqual(["beacon", "telemetry-data"]);
    expect(
      (
        await family.runDue(
          START + APRS_IGATE_STATUS_INTERVAL_MS,
          successfulWriter(),
        )
      ).map((write) => write.packet.kind),
    ).toEqual(["beacon", "telemetry-data", "status"]);
    expect(
      (
        await family.runDue(
          START + APRS_IGATE_DEFINITION_INTERVAL_MS,
          successfulWriter(),
        )
      ).map((write) => write.packet.kind),
    ).toEqual([
      "beacon",
      "telemetry-parm",
      "telemetry-unit",
      "telemetry-eqns",
      "telemetry-data",
      "status",
    ]);
  });

  it("does not repeat a successful status immediately on same-process reconnect", async () => {
    const family = new AprsIgateFamily({ provision, version: "2.0.0" });
    await family.onVerifiedLogin(START, successfulWriter());
    family.onDisconnected();
    expect(
      await family.onVerifiedLogin(START + MINUTE_MS, successfulWriter()),
    ).toEqual([]);
    const beforeStatus = await family.runDue(
      START + MINUTE_MS + APRS_IGATE_STATUS_INTERVAL_MS - 1,
      successfulWriter(),
    );
    expect(beforeStatus.map((write) => write.packet.kind)).not.toContain(
      "status",
    );
    expect(
      (
        await family.runDue(
          START + MINUTE_MS + APRS_IGATE_STATUS_INTERVAL_MS,
          successfulWriter(),
        )
      ).map((write) => write.packet.kind),
    ).toContain("status");
  });

  it("restarts a successful beacon interval from verified reconnect", async () => {
    const family = new AprsIgateFamily({ provision, version: "2.0.0" });
    await family.onVerifiedLogin(START, successfulWriter());
    family.onDisconnected();
    await family.onVerifiedLogin(START + MINUTE_MS, successfulWriter());

    const oldBoundary = await family.runDue(
      START + APRS_IGATE_TELEMETRY_INTERVAL_MS,
      successfulWriter(),
    );
    expect(oldBoundary.map((write) => write.packet.kind)).not.toContain(
      "beacon",
    );
    const reconnectBoundary = await family.runDue(
      START + MINUTE_MS + APRS_IGATE_TELEMETRY_INTERVAL_MS,
      successfulWriter(),
    );
    expect(reconnectBoundary.map((write) => write.packet.kind)).toContain(
      "beacon",
    );
  });

  it("retries a family with no successful anchor on verified reconnect", async () => {
    const family = new AprsIgateFamily({ provision, version: "2.0.0" });
    const first = await family.onVerifiedLogin(
      START,
      writerFailing("telemetry-data"),
    );
    expect(first[4]?.packet.data).toContain(":T#001,");
    expect(first[4]?.successful).toBe(false);
    expect(family.persistentState().lastSuccessfulTelemetrySequence).toBe(0);

    family.onDisconnected();
    const reconnect = await family.onVerifiedLogin(
      START + MINUTE_MS,
      successfulWriter(),
    );
    expect(reconnect.map((write) => write.packet.kind)).toEqual([
      "telemetry-data",
    ]);
    expect(reconnect[0]?.packet.data).toContain(":T#002,");
    expect(family.persistentState().lastSuccessfulTelemetrySequence).toBe(2);
  });

  it("does not move the status cadence on a duplicate verified event", async () => {
    const family = new AprsIgateFamily({ provision, version: "2.0.0" });
    await family.onVerifiedLogin(START, successfulWriter());
    const duplicate = await family.onVerifiedLogin(
      START + MINUTE_MS,
      successfulWriter(),
    );
    expect(duplicate).toEqual([]);
    const boundary = await family.runDue(
      START + APRS_IGATE_STATUS_INTERVAL_MS,
      successfulWriter(),
    );
    expect(boundary.map((write) => write.packet.kind)).toContain("status");
  });

  it("consumes failed telemetry numbers in memory but persists only success", async () => {
    const family = new AprsIgateFamily({
      provision,
      version: "2.0.0",
      lastSuccessfulTelemetrySequence: 41,
    });
    await family.onVerifiedLogin(START, successfulWriter());
    expect(family.persistentState().lastSuccessfulTelemetrySequence).toBe(42);

    const failed = await family.runDue(
      START + APRS_IGATE_TELEMETRY_INTERVAL_MS,
      writerFailing("telemetry-data"),
    );
    expect(failed.at(-1)).toMatchObject({
      packet: { kind: "telemetry-data" },
      successful: false,
    });
    expect(failed.at(-1)?.packet.data).toContain(":T#043,");
    expect(family.persistentState().lastSuccessfulTelemetrySequence).toBe(42);

    const retried = await family.runDue(
      START + 2 * APRS_IGATE_TELEMETRY_INTERVAL_MS,
      successfulWriter(),
    );
    const telemetry = retried.find(
      (write) => write.packet.kind === "telemetry-data",
    );
    expect(telemetry?.packet.data).toContain(":T#044,");
    expect(family.persistentState().lastSuccessfulTelemetrySequence).toBe(44);

    const restarted = new AprsIgateFamily({
      provision,
      version: "2.0.0",
      lastSuccessfulTelemetrySequence: 42,
    });
    const restartWrites = await restarted.onVerifiedLogin(
      START,
      successfulWriter(),
    );
    expect(
      restartWrites[4]?.packet.data.endsWith(":T#043,0,0,0,0,0,00000000"),
    ).toBe(true);
  });

  it("keeps a failed status on the verified-login hourly cadence", async () => {
    const family = new AprsIgateFamily({ provision, version: "2.0.0" });
    const first = await family.onVerifiedLogin(START, writerFailing("status"));
    expect(first.at(-1)?.successful).toBe(false);
    const beforeStatus = await family.runDue(
      START + APRS_IGATE_STATUS_INTERVAL_MS - 1,
      successfulWriter(),
    );
    expect(beforeStatus.map((write) => write.packet.kind)).not.toContain(
      "status",
    );
    expect(
      (
        await family.runDue(
          START + APRS_IGATE_STATUS_INTERVAL_MS,
          successfulWriter(),
        )
      ).map((write) => write.packet.kind),
    ).toContain("status");
  });

  it("persists the last successful telemetry sequence across restart", async () => {
    const database = new GatewayDatabase(":memory:");
    const repository = new AprsIgateRepository(database.connection);
    expect(repository.loadLastSuccessfulSequence("N1GATE-10")).toBe(0);

    const first = new AprsIgateFamily({ provision, version: "2.0.0" });
    await first.onVerifiedLogin(START, successfulWriter());
    const firstSequence =
      first.persistentState().lastSuccessfulTelemetrySequence;
    repository.persistLastSuccessfulSequence(
      "N1GATE-10",
      PROVISION_FINGERPRINT,
      firstSequence,
      new Date(START).toISOString(),
    );

    const restoredRepository = new AprsIgateRepository(database.connection);
    expect(restoredRepository.loadLastSuccessfulSequence("N1GATE-10")).toBe(1);
    const restarted = new AprsIgateFamily({
      provision,
      version: "2.0.0",
      lastSuccessfulTelemetrySequence:
        restoredRepository.loadLastSuccessfulSequence("N1GATE-10"),
    });
    const restartWrites = await restarted.onVerifiedLogin(
      START + MINUTE_MS,
      successfulWriter(),
    );
    expect(restartWrites[4]?.packet.data).toContain(":T#002,");
    database.close();
  });

  it("persists one active exact intent and confirms it only in fingerprint scope", () => {
    const database = new GatewayDatabase(":memory:");
    const repository = new AprsIgateRepository(database.connection);
    const attemptedAt = new Date(START).toISOString();
    const submittedAt = new Date(START + 1).toISOString();
    const observedAt = new Date(START + MINUTE_MS).toISOString();
    const packet = encodeAprsIgateStatus(provision, "2.0.0");
    const intent = repository.beginTransmission(
      packet,
      PROVISION_FINGERPRINT,
      attemptedAt,
    );
    expect(intent).toMatchObject({
      created: true,
      writeRequired: true,
      submission: { deliveryStatus: "sending", attemptedAt },
    });
    expect(
      repository.beginTransmission(packet, PROVISION_FINGERPRINT, submittedAt),
    ).toEqual({
      created: false,
      writeRequired: false,
      submission: intent.submission,
    });
    const submitted = repository.markSubmitted(
      intent.submission.id,
      submittedAt,
    );
    const observed = parseCmClientAprsLine(
      packet.data.replace("TCPIP*", "TCPIP*,qAC,T2TEST"),
    )!;

    expect(
      repository.confirmObserved(
        "b".repeat(64),
        observed.callsign,
        observed.destination,
        observed.info,
        observedAt,
      ),
    ).toEqual([]);
    expect(
      repository.confirmObserved(
        PROVISION_FINGERPRINT,
        observed.callsign,
        observed.destination,
        observed.info,
        observedAt,
      ),
    ).toEqual([
      expect.objectContaining({
        id: submitted.id,
        deliveryStatus: "observer_confirmed",
        observerConfirmedAt: observedAt,
      }),
    ]);
    expect(repository.listPublic()).toEqual([
      {
        id: submitted.id,
        packetKind: "status",
        deliveryStatus: "observer_confirmed",
        attemptedAt,
        submittedAt,
        localWriteCompletedAt: submittedAt,
        observerConfirmedAt: observedAt,
        updatedAt: observedAt,
        observationExpiresAt: submitted.observationExpiresAt,
      },
    ]);
    expect(repository.listPublic()[0]).not.toHaveProperty("callsign");
    expect(repository.listPublic()[0]).not.toHaveProperty("info");
    database.close();
  });

  it("atomically records local submission cache and successful T# sequence", () => {
    const database = new GatewayDatabase(":memory:");
    const repository = new AprsIgateRepository(database.connection);
    const attemptedAt = new Date(START).toISOString();
    const submittedAt = new Date(START + 1).toISOString();
    const packet = encodeAprsIgateTelemetryData(provision, 42, {
      all: 1,
      forwardedAprs: 2,
      position: 3,
      message: 4,
      control: 5,
    });
    const intent = repository.beginTransmission(
      packet,
      PROVISION_FINGERPRINT,
      attemptedAt,
    ).submission;

    expect(repository.markSubmitted(intent.id, submittedAt)).toMatchObject({
      deliveryStatus: "submitted",
      submittedAt,
      localWriteCompletedAt: submittedAt,
    });
    expect(repository.loadLastSuccessfulSequence("N1GATE-10")).toBe(42);
    expect(
      database.connection
        .prepare(
          "SELECT destination, transmitted_at FROM aprs_local_transmissions WHERE callsign = ? AND info = ?",
        )
        .get(intent.callsign, intent.info),
    ).toEqual({ destination: "APTMAG", transmitted_at: submittedAt });
    database.close();
  });

  it("repeats a fixed submitted station packet after its durable local cache window", () => {
    const database = new GatewayDatabase(":memory:");
    const repository = new AprsIgateRepository(database.connection);
    const packet = encodeAprsIgateBeacon(provision);
    const attemptedAt = new Date(START).toISOString();
    const firstCompletedAt = new Date(START + 1).toISOString();
    const repeatReservedAt = new Date(START + 31_001).toISOString();
    const repeatCompletedAt = new Date(START + 31_002).toISOString();
    const first = repository.beginTransmission(
      packet,
      PROVISION_FINGERPRINT,
      attemptedAt,
    );
    expect(first).toMatchObject({ created: true, writeRequired: true });
    repository.markSubmitted(first.submission.id, firstCompletedAt);

    const repeat = new AprsIgateRepository(
      database.connection,
    ).beginTransmission(packet, PROVISION_FINGERPRINT, repeatReservedAt);
    expect(repeat).toMatchObject({
      created: false,
      writeRequired: true,
      repeatReservationAt: repeatReservedAt,
      submission: {
        id: first.submission.id,
        deliveryStatus: "submitted",
        submittedAt: firstCompletedAt,
      },
    });
    const parsed = parseCmClientAprsLine(packet.data)!;
    expect(
      database.connection
        .prepare(
          "SELECT transmitted_at FROM aprs_local_transmissions WHERE callsign = ? AND destination = ? AND info = ?",
        )
        .get(parsed.callsign, parsed.destination, parsed.info),
    ).toEqual({ transmitted_at: repeatReservedAt });
    expect(
      new AprsIgateRepository(database.connection).beginTransmission(
        packet,
        PROVISION_FINGERPRINT,
        new Date(Date.parse(repeatReservedAt) + 1).toISOString(),
      ),
    ).toMatchObject({ created: false, writeRequired: false });

    expect(
      repository.markRepeatedSubmitted(
        first.submission.id,
        repeatReservedAt,
        repeatCompletedAt,
      ),
    ).toMatchObject({
      id: first.submission.id,
      deliveryStatus: "submitted",
      submittedAt: firstCompletedAt,
      localWriteCompletedAt: repeatCompletedAt,
    });
    expect(repository.list()).toHaveLength(1);
    expect(repository.deliveryCounts(PROVISION_FINGERPRINT)).toEqual({
      pending: 1,
      uncertain: 0,
      unconfirmed: 0,
    });
    database.close();
  });

  it("does not retry a transmission whose local write outcome is uncertain", () => {
    const database = new GatewayDatabase(":memory:");
    const repository = new AprsIgateRepository(database.connection);
    const packet = encodeAprsIgateBeacon(provision);
    const attemptedAt = new Date(START).toISOString();
    const intent = repository.beginTransmission(
      packet,
      PROVISION_FINGERPRINT,
      attemptedAt,
    );
    repository.markTransmissionUncertain(
      intent.submission.id,
      new Date(START + 1).toISOString(),
    );

    expect(
      repository.beginTransmission(
        packet,
        PROVISION_FINGERPRINT,
        new Date(START + 60_000).toISOString(),
      ),
    ).toMatchObject({
      created: false,
      writeRequired: false,
      submission: {
        id: intent.submission.id,
        deliveryStatus: "transmission_uncertain",
      },
    });
    database.close();
  });

  it("does not attribute an observer packet before local socket completion", () => {
    const database = new GatewayDatabase(":memory:");
    const repository = new AprsIgateRepository(database.connection);
    const attemptedAt = new Date(START).toISOString();
    const observedAt = new Date(START + 1).toISOString();
    const submittedAt = new Date(START + 2).toISOString();
    const packet = encodeAprsIgateTelemetryData(provision, 43, {
      all: 1,
      forwardedAprs: 2,
      position: 3,
      message: 4,
      control: 5,
    });
    const intent = repository.beginTransmission(
      packet,
      PROVISION_FINGERPRINT,
      attemptedAt,
    ).submission;
    const parsed = parseCmClientAprsLine(packet.data)!;

    expect(
      repository.confirmObserved(
        PROVISION_FINGERPRINT,
        parsed.callsign,
        parsed.destination,
        parsed.info,
        observedAt,
      ),
    ).toEqual([]);
    const pending = repository.list();
    expect(pending).toEqual([
      expect.objectContaining({
        id: intent.id,
        deliveryStatus: "sending",
      }),
    ]);
    expect(pending[0]).not.toHaveProperty("submittedAt");
    expect(pending[0]).not.toHaveProperty("localWriteCompletedAt");
    expect(pending[0]).not.toHaveProperty("observerConfirmedAt");
    expect(repository.loadLastSuccessfulSequence("N1GATE-10")).toBe(0);
    expect(
      database.connection
        .prepare(
          "SELECT transmitted_at FROM aprs_local_transmissions WHERE callsign = ? AND destination = ? AND info = ?",
        )
        .get(parsed.callsign, parsed.destination, parsed.info),
    ).toBeUndefined();
    expect(
      new AprsIgateRepository(database.connection).beginTransmission(
        packet,
        PROVISION_FINGERPRINT,
        submittedAt,
      ),
    ).toMatchObject({
      created: false,
      submission: { id: intent.id, deliveryStatus: "sending" },
    });
    expect(repository.markSubmitted(intent.id, submittedAt)).toMatchObject({
      deliveryStatus: "submitted",
      submittedAt,
      updatedAt: submittedAt,
    });
    expect(repository.loadLastSuccessfulSequence("N1GATE-10")).toBe(43);
    expect(
      database.connection
        .prepare(
          "SELECT transmitted_at FROM aprs_local_transmissions WHERE callsign = ? AND destination = ? AND info = ?",
        )
        .get(parsed.callsign, parsed.destination, parsed.info),
    ).toEqual({ transmitted_at: submittedAt });
    const confirmedAt = new Date(START + 3).toISOString();
    expect(
      repository.confirmObserved(
        PROVISION_FINGERPRINT,
        parsed.callsign,
        parsed.destination,
        parsed.info,
        confirmedAt,
      ),
    ).toEqual([
      expect.objectContaining({
        id: intent.id,
        deliveryStatus: "observer_confirmed",
        submittedAt,
        observerConfirmedAt: confirmedAt,
      }),
    ]);
    database.close();
  });

  it("recovers observer-proven uncertain telemetry without claiming a local write", async () => {
    const database = new GatewayDatabase(":memory:");
    const repository = new AprsIgateRepository(database.connection);
    const attemptedAt = new Date(START).toISOString();
    const observedAt = new Date(START + MINUTE_MS).toISOString();
    const packet = encodeAprsIgateTelemetryData(provision, 43, {
      all: 1,
      forwardedAprs: 2,
      position: 3,
      message: 4,
      control: 5,
    });
    repository.persistLastSuccessfulSequence(
      "N1GATE-10",
      PROVISION_FINGERPRINT,
      42,
      new Date(START - 1).toISOString(),
    );
    const intent = repository.beginTransmission(
      packet,
      PROVISION_FINGERPRINT,
      attemptedAt,
    ).submission;
    const parsed = parseCmClientAprsLine(packet.data)!;

    expect(
      repository.recoverInterrupted(PROVISION_FINGERPRINT, observedAt),
    ).toEqual([
      expect.objectContaining({
        id: intent.id,
        deliveryStatus: "transmission_uncertain",
      }),
    ]);
    expect(
      repository.beginTransmission(packet, PROVISION_FINGERPRINT, observedAt),
    ).toMatchObject({ created: false, submission: { id: intent.id } });
    database.connection
      .prepare(
        "INSERT INTO aprs_observed_packets (callsign, destination, info, first_observed_at, last_observed_at) VALUES (?, ?, ?, ?, ?)",
      )
      .run(
        parsed.callsign,
        parsed.destination,
        parsed.info,
        observedAt,
        observedAt,
      );
    const reconciled = repository.reconcileObserved(
      PROVISION_FINGERPRINT,
      observedAt,
    );
    expect(reconciled).toEqual([
      expect.objectContaining({
        id: intent.id,
        deliveryStatus: "observer_confirmed",
      }),
    ]);
    expect(reconciled[0]).not.toHaveProperty("submittedAt");
    expect(reconciled[0]).not.toHaveProperty("localWriteCompletedAt");
    expect(
      database.connection
        .prepare(
          "SELECT transmitted_at FROM aprs_local_transmissions WHERE callsign = ? AND destination = ? AND info = ?",
        )
        .get(parsed.callsign, parsed.destination, parsed.info),
    ).toBeUndefined();
    const restartedRepository = new AprsIgateRepository(database.connection);
    expect(restartedRepository.loadLastSuccessfulSequence("N1GATE-10")).toBe(
      43,
    );
    expect(
      restartedRepository.beginTransmission(
        packet,
        PROVISION_FINGERPRINT,
        new Date(Date.parse(observedAt) + 1).toISOString(),
      ),
    ).toMatchObject({
      created: false,
      submission: {
        id: intent.id,
        deliveryStatus: "observer_confirmed",
        observerConfirmedAt: observedAt,
      },
    });
    const restartedFamily = new AprsIgateFamily({
      provision,
      version: "2.0.0",
      lastSuccessfulTelemetrySequence:
        restartedRepository.loadLastSuccessfulSequence("N1GATE-10"),
    });
    const restartWrites = await restartedFamily.onVerifiedLogin(
      Date.parse(observedAt) + 2,
      () => true,
    );
    expect(
      restartWrites.find(({ packet: value }) => value.kind === "telemetry-data")
        ?.packet.data,
    ).toContain(":T#044,");
    expect(repository.deliveryCounts(PROVISION_FINGERPRINT)).toEqual({
      pending: 0,
      uncertain: 0,
      unconfirmed: 0,
    });
    database.close();
  });

  it("rejects an uncertain exact observation older than the transmission intent", () => {
    const database = new GatewayDatabase(":memory:");
    const repository = new AprsIgateRepository(database.connection);
    const observedAt = new Date(START).toISOString();
    const attemptedAt = new Date(START + MINUTE_MS).toISOString();
    const uncertainAt = new Date(START + MINUTE_MS + 1).toISOString();
    const packet = encodeAprsIgateTelemetryData(provision, 43, {
      all: 1,
      forwardedAprs: 2,
      position: 3,
      message: 4,
      control: 5,
    });
    const parsed = parseCmClientAprsLine(packet.data)!;
    repository.persistLastSuccessfulSequence(
      "N1GATE-10",
      PROVISION_FINGERPRINT,
      42,
      new Date(START - 1).toISOString(),
    );
    database.connection
      .prepare(
        "INSERT INTO aprs_observed_packets (callsign, destination, info, first_observed_at, last_observed_at) VALUES (?, ?, ?, ?, ?)",
      )
      .run(
        parsed.callsign,
        parsed.destination,
        parsed.info,
        observedAt,
        observedAt,
      );
    const intent = repository.beginTransmission(
      packet,
      PROVISION_FINGERPRINT,
      attemptedAt,
    ).submission;
    repository.markTransmissionUncertain(intent.id, uncertainAt);

    expect(
      repository.confirmObserved(
        PROVISION_FINGERPRINT,
        parsed.callsign,
        parsed.destination,
        parsed.info,
        observedAt,
      ),
    ).toEqual([]);
    expect(
      repository.reconcileObserved(PROVISION_FINGERPRINT, uncertainAt),
    ).toEqual([]);
    expect(repository.list()[0]).toMatchObject({
      id: intent.id,
      deliveryStatus: "transmission_uncertain",
    });
    expect(repository.loadLastSuccessfulSequence("N1GATE-10")).toBe(42);
    database.close();
  });

  it("expires every unconfirmed active state at the intent boundary", () => {
    const database = new GatewayDatabase(":memory:");
    const repository = new AprsIgateRepository(database.connection);
    const attemptedAt = new Date(START).toISOString();
    const expiresAt = new Date(START + 3 * 60 * MINUTE_MS).toISOString();
    const packet = encodeAprsIgateBeacon(provision);
    const intent = repository.beginTransmission(
      packet,
      PROVISION_FINGERPRINT,
      attemptedAt,
    ).submission;
    repository.markTransmissionUncertain(
      intent.id,
      new Date(START + 1).toISOString(),
    );

    expect(
      repository.expireActive(
        new Date(Date.parse(expiresAt) - 1).toISOString(),
      ),
    ).toBe(0);
    expect(repository.expireActive(expiresAt)).toBe(1);
    expect(repository.list()[0]).toMatchObject({
      deliveryStatus: "observation_expired",
      observationExpiresAt: expiresAt,
      updatedAt: expiresAt,
    });
    expect(repository.deliveryCounts()).toEqual({
      pending: 0,
      uncertain: 0,
      unconfirmed: 1,
    });
    database.close();
  });

  it("deletes terminal station history in bounded update order only", () => {
    const database = new GatewayDatabase(":memory:");
    const repository = new AprsIgateRepository(database.connection);
    const firstAttemptedAt = new Date(START).toISOString();
    const firstObservedAt = new Date(START + 1).toISOString();
    const secondAttemptedAt = new Date(START + MINUTE_MS).toISOString();
    const secondExpiresAt = new Date(
      START + MINUTE_MS + 3 * 60 * MINUTE_MS,
    ).toISOString();
    const activeAttemptedAt = new Date(
      START + MINUTE_MS + 4 * 60 * MINUTE_MS,
    ).toISOString();
    const confirmed = repository.beginTransmission(
      encodeAprsIgateStatus(provision, "2.0.0"),
      PROVISION_FINGERPRINT,
      firstAttemptedAt,
    ).submission;
    const confirmedPacket = parseCmClientAprsLine(
      encodeAprsIgateStatus(provision, "2.0.0").data,
    )!;
    repository.markSubmitted(confirmed.id, firstAttemptedAt);
    repository.confirmObserved(
      PROVISION_FINGERPRINT,
      confirmedPacket.callsign,
      confirmedPacket.destination,
      confirmedPacket.info,
      firstObservedAt,
    );
    const expired = repository.beginTransmission(
      encodeAprsIgateBeacon(provision),
      PROVISION_FINGERPRINT,
      secondAttemptedAt,
    ).submission;
    repository.expireActive(secondExpiresAt, PROVISION_FINGERPRINT);
    const active = repository.beginTransmission(
      encodeAprsIgateTelemetryDefinitions(provision)[0]!,
      PROVISION_FINGERPRINT,
      activeAttemptedAt,
    ).submission;

    expect(
      repository.deleteTerminalBefore(
        new Date(Date.parse(activeAttemptedAt) + 1).toISOString(),
        1,
      ),
    ).toBe(1);
    expect(repository.list().map((submission) => submission.id)).toEqual([
      active.id,
      expired.id,
    ]);
    expect(repository.list().map((submission) => submission.id)).not.toContain(
      confirmed.id,
    );
    database.close();
  });

  it("rejects a station packet with the wrong destination", () => {
    const database = new GatewayDatabase(":memory:");
    const repository = new AprsIgateRepository(database.connection);
    const packet = encodeAprsIgateBeacon(provision);
    expect(() =>
      repository.beginTransmission(
        {
          ...packet,
          data: packet.data.replace(">APTMAG", ">APZ999"),
        },
        PROVISION_FINGERPRINT,
        new Date(START).toISOString(),
      ),
    ).toThrow(AprsIgatePersistenceError);
    expect(repository.list()).toEqual([]);
    database.close();
  });
});

function successfulWriter(): (packet: AprsIgatePacket) => boolean {
  return () => true;
}

function writerFailing(
  kind: AprsIgatePacket["kind"],
): (packet: AprsIgatePacket) => boolean {
  return (packet) => packet.kind !== kind;
}

function withoutComment(value: CallMeshProvision): CallMeshProvision {
  const copy = { ...value };
  delete copy.comment;
  return copy;
}

function withoutLatitude(value: CallMeshProvision): CallMeshProvision {
  const copy = { ...value };
  delete copy.latitude;
  return copy;
}
