import { describe, expect, it } from "vitest";

import { TypeCompiler } from "@sinclair/typebox/compiler";

import { JOB_STATUSES, SystemCapabilitiesSchema } from "./index";

describe("job contracts", () => {
  it("includes terminal rollback states", () => {
    expect(JOB_STATUSES).toContain("rolled_back");
    expect(JOB_STATUSES).toContain("failed");
  });
});

describe("system capabilities contract", () => {
  it("requires every declared capability", () => {
    const check = TypeCompiler.Compile(SystemCapabilitiesSchema);
    expect(
      check.Check({
        schemaVersion: 1,
        platform: "linux",
        build: { version: "2.0.0-dev.0", commit: "abc123", channel: "dev" },
        capabilities: {
          managementWeb: { available: true },
          update: { available: true },
          tray: {
            available: false,
            reasonCode: "CAPABILITY_UNAVAILABLE_PLATFORM",
          },
          serial: { available: true },
          service: { available: true },
          autoStart: { available: true },
          docker: { available: true },
        },
      }),
    ).toBe(true);
    expect(
      check.Check({
        schemaVersion: 1,
        platform: "linux",
        build: { version: "2.0.0-dev.0", commit: "abc123", channel: "dev" },
        capabilities: {
          managementWeb: { available: true },
          update: { available: true },
          tray: { available: false },
          serial: { available: true },
          service: { available: true },
          autoStart: { available: true },
          docker: { available: true },
        },
      }),
    ).toBe(false);
  });
});
