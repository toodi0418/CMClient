import { describe, expect, it, vi } from "vitest";

import type { CallMeshProvision } from "@cmclient/contracts";

import type {
  CmCloudDirectAprsCapability,
  CmCloudDirectAprsDispatchResult,
  CmCloudDirectAprsEgress,
} from "./cmcloud-aprs";
import { CmCloudDirectAprsIgateRuntime } from "./cmcloud-igate";
import { GatewayDatabase } from "./persistence/database";

const DIRECT_PROVISION: CallMeshProvision = {
  callsignBase: "BU2GE",
  ssid: 4,
  symbolTable: "/",
  symbolCode: "I",
  latitude: 25.079_166_666_666_666,
  longitude: 121.473_666_666_666_66,
  altitudeMeters: 10,
};

const DIRECT_CAPABILITY: CmCloudDirectAprsCapability = {
  callsign: "BU2GE-4",
  verified: true,
  provision: DIRECT_PROVISION,
};

describe("CMCloud direct APRS iGate family", () => {
  it("emits the initial beacon, status, and telemetry through the one verified direct egress", async () => {
    const database = new GatewayDatabase(":memory:");
    const egress = new FixtureDirectAprsEgress();
    const runtime = new CmCloudDirectAprsIgateRuntime({
      database: database.connection,
      egress,
      version: "2.0.0",
      clock: () => new Date("2026-08-06T12:00:00.000Z"),
      tickIntervalMs: 60_000,
    });

    try {
      await runtime.configure(DIRECT_CAPABILITY);
      egress.readyState = true;
      await runtime.tick();

      expect(egress.submissions).toHaveLength(6);
      expect(egress.submissions[0]).toBe(
        "BU2GE-4>APTMAG,TCPIP*:!2504.75N/12128.42EI/A=000033",
      );
      expect(egress.submissions).toContain(
        "BU2GE-4>APTMAG,TCPIP*:>TMAG Client v2.0.0",
      );
      expect(egress.submissions).toContain(
        "BU2GE-4>APTMAG,TCPIP*:T#001,0,0,0,0,0,00000000",
      );
      expect(
        database.connection
          .prepare(
            "SELECT callsign, packet_kind, delivery_status FROM aprs_igate_submissions ORDER BY attempted_at ASC, id ASC",
          )
          .all(),
      ).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            callsign: "BU2GE-4",
            packet_kind: "beacon",
            delivery_status: "submitted",
          }),
          expect.objectContaining({
            callsign: "BU2GE-4",
            packet_kind: "status",
            delivery_status: "submitted",
          }),
          expect.objectContaining({
            callsign: "BU2GE-4",
            packet_kind: "telemetry-data",
            delivery_status: "submitted",
          }),
        ]),
      );
      expect(runtime.aprsRuntimeStatus()).toMatchObject({
        configured: true,
        running: true,
        monitorStatus: "stopped",
        pendingStationSubmissions: 6,
        failedStationSubmissions: 0,
        unconfirmedStationSubmissions: 0,
        directAprs: {
          capabilityState: "granted",
          profileState: "configured",
          directAprsReady: true,
          beaconState: "active",
        },
      });
      expect(runtime.aprsRuntimeStatus().directAprs).toEqual({
        capabilityState: "granted",
        profileState: "configured",
        directAprsReady: true,
        beaconState: "active",
      });
      expect(runtime.listStationSubmissions()).toHaveLength(6);
    } finally {
      await runtime.stop();
      database.close();
    }
  });

  it("runs the beacon cadence after the verified direct login without opening another APRS connection", async () => {
    vi.useFakeTimers();
    const database = new GatewayDatabase(":memory:");
    const egress = new FixtureDirectAprsEgress();
    const runtime = new CmCloudDirectAprsIgateRuntime({
      database: database.connection,
      egress,
      version: "2.0.0",
      clock: () => new Date(Date.now()),
      beaconIntervalMs: 60_000,
      tickIntervalMs: 60_000,
    });

    try {
      await runtime.configure(DIRECT_CAPABILITY);
      egress.readyState = true;
      await runtime.tick();
      expect(
        egress.submissions.filter((data) => data.includes(":!")),
      ).toHaveLength(1);

      await vi.advanceTimersByTimeAsync(60_000);
      expect(
        egress.submissions.filter((data) => data.includes(":!")),
      ).toHaveLength(2);
      expect(egress.configureCalls).toBe(0);
    } finally {
      await runtime.stop();
      database.close();
      vi.useRealTimers();
    }
  });

  it("includes successfully written CMCloud Tracker forwards in the next telemetry window", async () => {
    let now = new Date("2026-08-06T12:00:00.000Z");
    const database = new GatewayDatabase(":memory:");
    const egress = new FixtureDirectAprsEgress();
    const runtime = new CmCloudDirectAprsIgateRuntime({
      database: database.connection,
      egress,
      version: "2.0.0",
      clock: () => now,
      tickIntervalMs: 60_000,
    });

    try {
      await runtime.configure(DIRECT_CAPABILITY);
      egress.readyState = true;
      await runtime.tick();
      expect(egress.submissions).toContain(
        "BU2GE-4>APTMAG,TCPIP*:T#001,0,0,0,0,0,00000000",
      );

      now = new Date("2026-08-06T12:01:00.000Z");
      runtime.recordTrackerForward();
      now = new Date("2026-08-06T12:10:00.000Z");
      await runtime.tick();

      expect(egress.submissions).toContain(
        "BU2GE-4>APTMAG,TCPIP*:T#002,0,1,0,0,0,00000000",
      );
    } finally {
      await runtime.stop();
      database.close();
    }
  });

  it("retains a captured Tracker forward through a transient CMCloud reconnect", async () => {
    let now = new Date("2026-08-06T12:00:00.000Z");
    const database = new GatewayDatabase(":memory:");
    const egress = new FixtureDirectAprsEgress();
    const runtime = new CmCloudDirectAprsIgateRuntime({
      database: database.connection,
      egress,
      version: "2.0.0",
      clock: () => now,
      tickIntervalMs: 60_000,
    });

    try {
      await runtime.configure(DIRECT_CAPABILITY);
      egress.readyState = true;
      await runtime.tick();
      const recordTrackerForward = runtime.captureTrackerForwardRecorder();
      expect(recordTrackerForward).toBeTypeOf("function");

      now = new Date("2026-08-06T12:01:00.000Z");
      runtime.suspend();
      recordTrackerForward?.(now.getTime());
      await runtime.configure(DIRECT_CAPABILITY);

      now = new Date("2026-08-06T12:10:00.000Z");
      await runtime.tick();
      expect(egress.submissions).toContain(
        "BU2GE-4>APTMAG,TCPIP*:T#002,0,1,0,0,0,00000000",
      );
    } finally {
      await runtime.stop();
      database.close();
    }
  });

  it("does not assign a captured Tracker forward to a replacement station profile", async () => {
    let now = new Date("2026-08-06T12:00:00.000Z");
    const database = new GatewayDatabase(":memory:");
    const egress = new FixtureDirectAprsEgress();
    const runtime = new CmCloudDirectAprsIgateRuntime({
      database: database.connection,
      egress,
      version: "2.0.0",
      clock: () => now,
      tickIntervalMs: 60_000,
    });

    try {
      await runtime.configure({
        ...DIRECT_CAPABILITY,
        provision: { ...DIRECT_PROVISION, comment: "first" },
      });
      egress.readyState = true;
      await runtime.tick();
      const recordTrackerForward = runtime.captureTrackerForwardRecorder();
      expect(recordTrackerForward).toBeTypeOf("function");

      runtime.suspend();
      now = new Date("2026-08-06T12:01:00.000Z");
      await runtime.configure({
        ...DIRECT_CAPABILITY,
        provision: { ...DIRECT_PROVISION, comment: "replacement" },
      });
      recordTrackerForward?.(now.getTime());

      expect(egress.submissions).toContain(
        "BU2GE-4>APTMAG,TCPIP*:T#002,0,0,0,0,0,00000000",
      );
    } finally {
      await runtime.stop();
      database.close();
    }
  });

  it("uses the base callsign for a centrally granted zero SSID", async () => {
    const database = new GatewayDatabase(":memory:");
    const egress = new FixtureDirectAprsEgress();
    const runtime = new CmCloudDirectAprsIgateRuntime({
      database: database.connection,
      egress,
      version: "2.0.0",
      clock: () => new Date("2026-08-06T12:00:00.000Z"),
      tickIntervalMs: 60_000,
    });

    try {
      await runtime.configure({
        callsign: "BU2GE-0",
        verified: true,
        provision: { ...DIRECT_PROVISION, ssid: 0 },
      });
      egress.readyState = true;
      await runtime.tick();

      expect(egress.submissions[0]).toMatch(/^BU2GE>APTMAG,TCPIP\*:/u);
      expect(egress.submissions).not.toContain(
        expect.stringContaining("BU2GE-0"),
      );
    } finally {
      await runtime.stop();
      database.close();
    }
  });

  it("does not present retired profile submissions as current CMCloud station delivery", async () => {
    const database = new GatewayDatabase(":memory:");
    const egress = new FixtureDirectAprsEgress();
    const runtime = new CmCloudDirectAprsIgateRuntime({
      database: database.connection,
      egress,
      version: "2.0.0",
      clock: () => new Date("2026-08-06T12:00:00.000Z"),
      tickIntervalMs: 60_000,
    });

    try {
      await runtime.configure({
        ...DIRECT_CAPABILITY,
        provision: { ...DIRECT_PROVISION, comment: "first" },
      });
      egress.readyState = true;
      await runtime.tick();

      await runtime.configure({
        ...DIRECT_CAPABILITY,
        provision: { ...DIRECT_PROVISION, comment: "second" },
      });

      expect(egress.submissions).toHaveLength(12);
      expect(runtime.listStationSubmissions()).toHaveLength(6);
      expect(runtime.aprsRuntimeStatus().pendingStationSubmissions).toBe(6);
    } finally {
      await runtime.stop();
      database.close();
    }
  });

  it("reports a direct write failure instead of mislabeling it as APRS-IS verification wait", async () => {
    const database = new GatewayDatabase(":memory:");
    const egress = new FixtureDirectAprsEgress({
      outcome: "retryable_failure",
      errorCode: "CMCLOUD_DIRECT_APRS_WRITE_FAILED",
    });
    const runtime = new CmCloudDirectAprsIgateRuntime({
      database: database.connection,
      egress,
      version: "2.0.0",
      clock: () => new Date("2026-08-06T12:00:00.000Z"),
      tickIntervalMs: 60_000,
    });

    try {
      await runtime.configure(DIRECT_CAPABILITY);
      egress.readyState = true;
      await runtime.tick();

      expect(runtime.status()).toMatchObject({
        directAprsReady: true,
        beaconState: "error",
        lastErrorCode: "CMCLOUD_DIRECT_APRS_WRITE_FAILED",
      });
    } finally {
      await runtime.stop();
      database.close();
    }
  });

  it("shows a missing beacon profile without emitting a fabricated station packet", async () => {
    const database = new GatewayDatabase(":memory:");
    const egress = new FixtureDirectAprsEgress();
    const runtime = new CmCloudDirectAprsIgateRuntime({
      database: database.connection,
      egress,
      version: "2.0.0",
    });

    try {
      await runtime.configure({ callsign: "BU2GE-4", verified: true });
      egress.readyState = true;
      await runtime.tick();

      expect(egress.submissions).toEqual([]);
      expect(runtime.status()).toEqual({
        configured: false,
        running: false,
        capabilityState: "granted",
        profileState: "missing",
        directAprsReady: true,
        beaconState: "missing_profile",
      });
    } finally {
      await runtime.stop();
      database.close();
    }
  });

  it("does not manufacture a location when CMCloud has not granted a beacon-ready profile", async () => {
    const database = new GatewayDatabase(":memory:");
    const egress = new FixtureDirectAprsEgress();
    const runtime = new CmCloudDirectAprsIgateRuntime({
      database: database.connection,
      egress,
      version: "2.0.0",
    });

    try {
      await runtime.configure({
        ...DIRECT_CAPABILITY,
        provision: {
          callsignBase: "BU2GE",
          ssid: 4,
          symbolTable: "/",
          symbolCode: "I",
        },
      });
      egress.readyState = true;
      await runtime.tick();

      expect(egress.submissions).toEqual([]);
      expect(runtime.status()).toEqual({
        configured: false,
        running: false,
        capabilityState: "granted",
        profileState: "invalid",
        directAprsReady: true,
        beaconState: "error",
        lastErrorCode: "CMCLOUD_DIRECT_APRS_BEACON_PROVISION_INVALID",
      });
    } finally {
      await runtime.stop();
      database.close();
    }
  });
});

class FixtureDirectAprsEgress implements CmCloudDirectAprsEgress {
  readonly submissions: string[] = [];
  readyState = false;
  configureCalls = 0;

  constructor(
    private readonly result: CmCloudDirectAprsDispatchResult = {
      outcome: "submitted",
    },
  ) {}

  configure(): Promise<void> {
    this.configureCalls += 1;
    return Promise.resolve();
  }

  ready(): boolean {
    return this.readyState;
  }

  submit(data: string): Promise<CmCloudDirectAprsDispatchResult> {
    this.submissions.push(data);
    return Promise.resolve(this.result);
  }

  stop(): Promise<void> {
    this.readyState = false;
    return Promise.resolve();
  }

  setReadinessListener(): void {}
}
