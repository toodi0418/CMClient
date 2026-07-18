import { describe, expect, it } from "vitest";

import type { MeshTelemetry } from "@cmclient/contracts";

import { buildTelemetryChartModel } from "./telemetry-chart";

describe("telemetry chart model", () => {
  it("groups numeric metrics by node and orders history by observation time", () => {
    const model = buildTelemetryChartModel(
      [
        telemetry("second", "2026-07-18T00:02:00.000Z", {
          batteryLevel: 72,
          charging: false,
        }),
        telemetry("first", "2026-07-18T00:01:00.000Z", {
          batteryLevel: 73,
          firmware: "fixture",
        }),
      ],
      "en-US",
      ["#27734d", "#925908"],
    );

    expect(model.seriesCount).toBe(1);
    expect(model.option.color).toEqual(["#27734d", "#925908"]);
    expect(model.option.series).toEqual([
      expect.objectContaining({
        name: "fixture-network #42 · deviceMetrics.batteryLevel",
        type: "line",
        data: [
          [Date.parse("2026-07-18T00:01:00.000Z"), 73],
          [Date.parse("2026-07-18T00:02:00.000Z"), 72],
        ],
      }),
    ]);
  });

  it("does not invent chart values for text and boolean telemetry", () => {
    const model = buildTelemetryChartModel(
      [telemetry("text", "2026-07-18T00:01:00.000Z", { online: true })],
      "zh-TW",
      ["#27734d"],
    );

    expect(model.seriesCount).toBe(0);
    expect(model.option.series).toEqual([]);
  });
});

function telemetry(
  id: string,
  observedAt: string,
  metrics: MeshTelemetry["metrics"],
): MeshTelemetry {
  return {
    schemaVersion: 1,
    id,
    observationId: `observation-${id}`,
    meshNetworkId: "fixture-network",
    nodeNum: 42,
    metricKind: "deviceMetrics",
    metrics,
    observedAt,
  };
}
