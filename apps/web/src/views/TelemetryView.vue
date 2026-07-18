<script setup lang="ts">
import { nextTick, onBeforeUnmount, onMounted, ref, watch } from "vue";
import { RefreshCw } from "@lucide/vue";
import type { ECharts } from "echarts/core";
import Button from "primevue/button";
import { useI18n } from "vue-i18n";

import { useDomainStore } from "@/stores/domain";
import { buildTelemetryChartModel } from "@/telemetry-chart";

const domain = useDomainStore();
const { t, locale } = useI18n();
const chartElement = ref<HTMLElement>();
const seriesCount = ref(0);

let chart: ECharts | undefined;
let chartInitialization: Promise<ECharts> | undefined;
let resizeObserver: ResizeObserver | undefined;
let resizeFrame: number | undefined;
let themeObserver: MutationObserver | undefined;

async function refreshTelemetry() {
  await domain.refresh();
  await nextTick();
  await renderChart();
}

async function renderChart() {
  if (!chartElement.value) {
    return;
  }
  const styles = getComputedStyle(document.documentElement);
  const model = buildTelemetryChartModel(
    domain.telemetry,
    locale.value,
    [
      styles.getPropertyValue("--cm-accent").trim(),
      styles.getPropertyValue("--cm-warning").trim(),
      "#4f88c6",
      styles.getPropertyValue("--cm-danger").trim(),
      "#7f9f4b",
      "#b56fa4",
    ],
    {
      background: styles.getPropertyValue("--cm-surface-raised").trim(),
      border: styles.getPropertyValue("--cm-border-divider").trim(),
      muted: styles.getPropertyValue("--cm-text-muted").trim(),
      text: styles.getPropertyValue("--cm-text").trim(),
    },
  );
  seriesCount.value = model.seriesCount;
  if (model.seriesCount === 0) {
    chart?.clear();
    return;
  }
  await nextTick();
  if (!chart) {
    chartInitialization ??= import("@/echarts-runtime").then((echarts) =>
      echarts.init(chartElement.value!, undefined, { renderer: "canvas" }),
    );
    chart = await chartInitialization;
  }
  chart.setOption(model.option, { notMerge: true });
}

watch(
  [() => domain.telemetry, locale],
  () => void nextTick().then(() => renderChart()),
  { deep: true },
);

onMounted(async () => {
  resizeObserver = new ResizeObserver(() => {
    if (resizeFrame !== undefined) {
      return;
    }
    resizeFrame = window.requestAnimationFrame(() => {
      resizeFrame = undefined;
      chart?.resize();
    });
  });
  if (chartElement.value) {
    resizeObserver.observe(chartElement.value);
  }
  themeObserver = new MutationObserver(() => void renderChart());
  themeObserver.observe(document.documentElement, {
    attributes: true,
    attributeFilter: ["data-theme"],
  });
  await refreshTelemetry();
});

onBeforeUnmount(() => {
  resizeObserver?.disconnect();
  if (resizeFrame !== undefined) {
    window.cancelAnimationFrame(resizeFrame);
    resizeFrame = undefined;
  }
  themeObserver?.disconnect();
  chart?.dispose();
  chart = undefined;
  chartInitialization = undefined;
});
</script>

<template>
  <section class="page-grid" :aria-label="t('domain.telemetry')">
    <div class="status-panel">
      <div class="panel-heading">
        <div>
          <p class="section-placeholder__eyebrow">
            {{ t("navigation.telemetry") }}
          </p>
          <h2>{{ t("domain.telemetry") }}</h2>
        </div>
        <Button
          unstyled
          class="page-action"
          type="button"
          :aria-label="t('common.refresh')"
          :title="t('common.refresh')"
          :disabled="domain.loading"
          @click="refreshTelemetry"
        >
          <RefreshCw :size="17" aria-hidden="true" />
        </Button>
      </div>
      <p v-if="domain.loading" class="status-message">
        {{ t("common.loading") }}
      </p>
      <p v-else-if="domain.errorCode" class="status-message">
        {{ t("common.unavailable") }}
        <code class="stable-code">{{ domain.errorCode }}</code>
      </p>
      <p v-else-if="!domain.telemetry.length" class="status-message">
        {{ t("domain.empty") }}
      </p>
      <p v-else-if="seriesCount === 0" class="status-message">
        {{ t("domain.noNumericTelemetry") }}
      </p>
      <div
        v-show="domain.telemetry.length && seriesCount > 0"
        ref="chartElement"
        class="telemetry-chart"
        role="img"
        :aria-label="t('domain.telemetryChart')"
      />
    </div>
    <div v-if="domain.telemetry.length" class="status-panel record-list">
      <article
        v-for="entry in domain.telemetry"
        :key="entry.id"
        class="record-row"
      >
        <div>
          <strong>{{ entry.metricKind }}</strong>
          <span>{{ t("domain.network") }}: {{ entry.meshNetworkId }}</span>
        </div>
        <div>
          <span>{{ t("domain.node") }}</span>
          <code>{{ entry.nodeNum }}</code>
        </div>
        <div>
          <span>{{ t("domain.metric") }}</span>
          <code>{{ Object.keys(entry.metrics).join(", ") }}</code>
        </div>
      </article>
    </div>
  </section>
</template>
