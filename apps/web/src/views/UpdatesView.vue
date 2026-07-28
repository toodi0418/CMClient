<script setup lang="ts">
import { computed, onMounted, onUnmounted } from "vue";
import { RefreshCw } from "@lucide/vue";
import Button from "primevue/button";
import { useI18n } from "vue-i18n";

import CapabilityStatus from "@/components/CapabilityStatus.vue";
import ProblemNotice from "@/components/ProblemNotice.vue";
import { updateActivityKey } from "@/problems";
import { useGatewayStore } from "@/stores/gateway";
import { useUpdatesStore } from "@/stores/updates";

const gateway = useGatewayStore();
const updates = useUpdatesStore();
const { locale, t } = useI18n();
const updateCapability = computed(
  () => gateway.capabilities?.capabilities.nativeUpdate,
);
const updateCapabilityReasonCode = computed(() => {
  const capability = updateCapability.value;
  return capability && "reasonCode" in capability
    ? capability.reasonCode
    : undefined;
});
const job = computed(() => updates.status?.job);
const phase = computed(() => job.value?.phase ?? "idle");
const phaseLabel = computed(() => t("updates.phase." + phase.value));
const transferLabel = computed(() => {
  if (!job.value || job.value.bytesDownloaded === null) {
    return "--";
  }
  const total =
    job.value.bytesTotal === null ? "--" : formatBytes(job.value.bytesTotal);
  return [formatBytes(job.value.bytesDownloaded), total].join(" / ");
});
const speedLabel = computed(() => {
  if (!job.value || job.value.bytesPerSecond === null) {
    return "--";
  }
  return formatBytes(job.value.bytesPerSecond) + "/s";
});

async function refresh(): Promise<void> {
  await Promise.all([gateway.refresh(), updates.refresh()]);
}

function formatBytes(bytes: number): string {
  const units = ["B", "KiB", "MiB", "GiB"];
  const exponent = Math.min(
    Math.floor(Math.log(Math.max(bytes, 1)) / Math.log(1024)),
    units.length - 1,
  );
  const value = bytes / 1024 ** exponent;
  return [
    new Intl.NumberFormat(locale.value, {
      maximumFractionDigits: exponent === 0 ? 0 : 1,
    }).format(value),
    units[exponent],
  ].join(" ");
}

function formatUpdatedAt(value: string | undefined): string {
  if (!value) {
    return "--";
  }
  const date = new Date(value);
  if (Number.isNaN(date.valueOf())) {
    return value;
  }
  return new Intl.DateTimeFormat(locale.value, {
    dateStyle: "medium",
    timeStyle: "medium",
  }).format(date);
}

onMounted(() => {
  updates.start();
  void gateway.refresh();
});
onUnmounted(() => updates.stop());
</script>

<template>
  <section class="page-grid" :aria-label="t('navigation.updates')">
    <div class="status-panel">
      <div class="panel-heading">
        <div>
          <p class="section-placeholder__eyebrow">
            {{ t("updates.agent") }}
          </p>
          <h2>{{ t("updates.status") }}</h2>
        </div>
        <Button
          unstyled
          class="page-action"
          type="button"
          :aria-label="t('common.refresh')"
          :title="t('common.refresh')"
          :disabled="updates.loading"
          @click="refresh"
          ><RefreshCw :size="17" aria-hidden="true"
        /></Button>
      </div>
      <template v-if="updates.status">
        <dl class="facts-grid update-facts">
          <div>
            <dt>{{ t("updates.phaseLabel") }}</dt>
            <dd>
              <span class="status-badge" :data-state="phase">
                {{ phaseLabel }}
              </span>
            </dd>
          </div>
          <div>
            <dt>{{ t("updates.updatedAt") }}</dt>
            <dd>{{ formatUpdatedAt(job?.updatedAt) }}</dd>
          </div>
          <div>
            <dt>{{ t("updates.transfer") }}</dt>
            <dd>{{ transferLabel }}</dd>
          </div>
          <div>
            <dt>{{ t("updates.speed") }}</dt>
            <dd>{{ speedLabel }}</dd>
          </div>
        </dl>
        <div v-if="job" class="update-job-meta">
          <span>{{ t("updates.job") }}</span>
          <code>{{ job.id }}</code>
          <ProblemNotice v-if="job.errorCode" :code="job.errorCode" compact />
        </div>
        <p v-else class="status-message">{{ t("updates.idle") }}</p>
        <div v-if="job?.recentLogCodes.length" class="update-log">
          <p>{{ t("updates.log") }}</p>
          <ul>
            <li v-for="code in job.recentLogCodes" :key="code">
              {{ t(updateActivityKey(code)) }}
            </li>
          </ul>
        </div>
        <ProblemNotice
          v-if="updates.errorCode"
          :code="updates.errorCode"
          compact
        />
      </template>
      <ProblemNotice
        v-else-if="updates.errorCode"
        :code="updates.errorCode"
        show-retry
        @retry="refresh"
      />
      <p v-else class="status-message">{{ t("common.loading") }}</p>
    </div>

    <div class="status-panel">
      <div class="panel-heading">
        <div>
          <p class="section-placeholder__eyebrow">
            {{ t("updates.release") }}
          </p>
          <h2>{{ t("updates.currentVersion") }}</h2>
        </div>
      </div>
      <dl class="facts-grid">
        <div>
          <dt>{{ t("updates.currentVersion") }}</dt>
          <dd>{{ gateway.status?.identity.identity.version ?? "--" }}</dd>
        </div>
        <div>
          <dt>{{ t("updates.channel") }}</dt>
          <dd>{{ gateway.status?.identity.identity.channel ?? "--" }}</dd>
        </div>
        <div>
          <dt>{{ t("updates.owner") }}</dt>
          <dd>{{ t("updates.agent") }}</dd>
        </div>
        <div>
          <dt>{{ t("updates.capability") }}</dt>
          <dd>
            <CapabilityStatus
              :available="updateCapability?.available ?? false"
              :reason-code="updateCapabilityReasonCode"
            />
          </dd>
        </div>
      </dl>
    </div>
  </section>
</template>
