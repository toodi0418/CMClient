import { describe, expect, it } from "vitest";

import { TypeCompiler } from "@sinclair/typebox/compiler";

import {
  DomainEventSchema,
  JobDetailSchema,
  JOB_STATUSES,
  SystemCapabilitiesSchema,
} from "./index";

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

describe("domain event contract", () => {
  it("requires the versioned SSE envelope fields", () => {
    const check = TypeCompiler.Compile(DomainEventSchema);
    expect(
      check.Check({
        eventId: "event-1",
        schemaVersion: 1,
        type: "gateway.ready",
        occurredAt: "2026-07-18T00:00:00.000Z",
        source: "gateway",
        payload: { port: 4810 },
      }),
    ).toBe(true);
    expect(
      check.Check({
        eventId: "event-1",
        schemaVersion: 1,
        type: "invalid event",
        occurredAt: "2026-07-18T00:00:00.000Z",
        source: "gateway",
        payload: {},
      }),
    ).toBe(false);
  });
});

describe("job contract", () => {
  it("keeps API job state free of execution input", () => {
    const check = TypeCompiler.Compile(JobDetailSchema);
    expect(
      check.Check({
        id: "job-1",
        type: "diagnostics.integrity_check",
        status: "succeeded",
        createdAt: "2026-07-18T00:00:00.000Z",
        updatedAt: "2026-07-18T00:00:01.000Z",
        completedAt: "2026-07-18T00:00:01.000Z",
      }),
    ).toBe(true);
    expect(
      check.Check({
        id: "job-1",
        type: "diagnostics.integrity_check",
        status: "succeeded",
        createdAt: "2026-07-18T00:00:00.000Z",
        updatedAt: "2026-07-18T00:00:01.000Z",
        input: { secret: "must-not-leak" },
      }),
    ).toBe(false);
  });
});
