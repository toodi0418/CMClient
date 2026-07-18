import type { EChartsOption, LineSeriesOption } from "echarts";

import type { MeshTelemetry } from "@cmclient/contracts";

export interface TelemetryChartModel {
  option: EChartsOption;
  seriesCount: number;
}

export interface TelemetryChartTheme {
  background: string;
  border: string;
  muted: string;
  text: string;
}

export function buildTelemetryChartModel(
  entries: readonly MeshTelemetry[],
  locale: string,
  colors: readonly string[],
  theme: TelemetryChartTheme = {
    background: "#ffffff",
    border: "#d0dcd2",
    muted: "#58705d",
    text: "#19231c",
  },
): TelemetryChartModel {
  const grouped = new Map<string, Array<[number, number]>>();
  for (const entry of entries) {
    const timestamp = Date.parse(entry.observedAt);
    if (!Number.isFinite(timestamp)) {
      continue;
    }
    for (const [metric, value] of Object.entries(entry.metrics)) {
      if (typeof value !== "number" || !Number.isFinite(value)) {
        continue;
      }
      const key = `${entry.meshNetworkId} #${entry.nodeNum} · ${entry.metricKind}.${metric}`;
      const points = grouped.get(key) ?? [];
      points.push([timestamp, value]);
      grouped.set(key, points);
    }
  }
  const series: LineSeriesOption[] = [...grouped.entries()]
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([name, points]) => ({
      name,
      type: "line",
      data: points.sort((left, right) => left[0] - right[0]),
      showSymbol: points.length < 12,
      symbolSize: 6,
      connectNulls: false,
      smooth: false,
      lineStyle: { width: 2 },
      emphasis: { focus: "series" },
    }));
  const dateTime = new Intl.DateTimeFormat(locale, {
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });
  return {
    seriesCount: series.length,
    option: {
      animation: false,
      aria: { enabled: true, decal: { show: true } },
      color: [...colors],
      grid: { top: 58, right: 24, bottom: 72, left: 58, containLabel: true },
      legend: {
        type: "scroll",
        top: 14,
        left: 18,
        right: 18,
        textStyle: { color: theme.text },
      },
      tooltip: {
        trigger: "axis",
        axisPointer: { type: "cross" },
        backgroundColor: theme.background,
        borderColor: theme.border,
        textStyle: { color: theme.text },
        valueFormatter: (value) =>
          typeof value === "number"
            ? String(Number(value.toFixed(4)))
            : String(value),
      },
      xAxis: {
        type: "time",
        axisLabel: {
          color: theme.muted,
          hideOverlap: true,
          formatter: (value: number) => dateTime.format(new Date(value)),
        },
        axisLine: { lineStyle: { color: theme.border } },
        splitLine: { lineStyle: { color: theme.border } },
      },
      yAxis: {
        type: "value",
        scale: true,
        splitNumber: 5,
        axisLabel: { color: theme.muted },
        axisLine: { lineStyle: { color: theme.border } },
        splitLine: { lineStyle: { color: theme.border } },
      },
      dataZoom: [
        { type: "inside", filterMode: "none" },
        { type: "slider", height: 18, bottom: 20, filterMode: "none" },
      ],
      series,
    },
  };
}
