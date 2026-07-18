import { describe, expect, it } from "vitest";

import {
  aprsCallmeshDetail,
  meshtasticDetail,
  proxyDetail,
} from "./service-status";

describe("desktop service status details", () => {
  it("summarizes bounded Meshtastic, APRS/CallMesh, and proxy facts", () => {
    expect(
      meshtasticDetail({
        state: "running",
        transport: "tcp",
        framesReceived: 24,
      }),
    ).toBe("tcp / 24 frames");
    expect(
      aprsCallmeshDetail({
        state: "running",
        aprsState: "connected",
        callmeshState: "ready",
        activeMappingCount: 3,
        pendingCount: 1,
        failedCount: 0,
      }),
    ).toBe("connected / ready / 3 maps / 1 pending");
    expect(
      proxyDetail({
        state: "running",
        mode: "monitor",
        activeClients: 2,
        maxClients: 16,
      }),
    ).toBe("monitor / 2/16 clients");
  });

  it("shows stable reason codes and checking states without backend messages", () => {
    expect(meshtasticDetail(undefined)).toBe("checking");
    expect(
      aprsCallmeshDetail({
        state: "degraded",
        reasonCode: "CONTROL_TIMEOUT",
      }),
    ).toBe("CONTROL_TIMEOUT");
    expect(
      proxyDetail({
        state: "unavailable",
        reasonCode: "CONTROL_COMMAND_FAILED",
      }),
    ).toBe("CONTROL_COMMAND_FAILED");
  });
});
