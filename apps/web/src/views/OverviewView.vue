<script setup lang="ts">
import { computed } from "vue";
import { useI18n } from "vue-i18n";

import { useGatewayStore } from "@/stores/gateway";

const { t } = useI18n();
const gateway = useGatewayStore();

const availableCapabilities = computed(
  () =>
    Object.values(gateway.capabilities?.capabilities ?? {}).filter(
      (capability) => capability.available,
    ).length,
);

const gatewayMessage = computed(() => {
  if (gateway.availability === "available") {
    return t("dashboard.connected");
  }
  if (gateway.availability === "unavailable") {
    return t("dashboard.unavailable");
  }
  return t("dashboard.connecting");
});
</script>

<template>
  <section class="overview-grid" :aria-label="t('navigation.overview')">
    <div class="signal-board">
      <div class="signal-board__header">
        <span>{{ t("dashboard.runtime") }}</span>
        <span class="signal-board__time">{{
          t(`gateway.${gateway.availability}`)
        }}</span>
      </div>
      <div class="signal-board__body">
        <strong>{{ t(`gateway.${gateway.availability}`) }}</strong>
        <p>{{ gatewayMessage }}</p>
        <code v-if="gateway.errorCode" class="stable-code">{{
          gateway.errorCode
        }}</code>
      </div>
    </div>
    <div class="overview-strip" :aria-label="t('shell.operations')">
      <div>
        <span>{{ t("dashboard.version") }}</span>
        <strong>{{ gateway.status?.identity.identity.version ?? "--" }}</strong>
      </div>
      <div>
        <span>{{ t("dashboard.eventConnection") }}</span>
        <strong>{{ t(`eventState.${gateway.eventConnection}`) }}</strong>
      </div>
      <div>
        <span>{{ t("dashboard.capabilities") }}</span>
        <strong>{{
          gateway.capabilities ? availableCapabilities : "--"
        }}</strong>
      </div>
    </div>
  </section>
</template>
