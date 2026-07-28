<script setup lang="ts">
import { computed } from "vue";
import { Send } from "@lucide/vue";
import { useI18n } from "vue-i18n";

import CapabilityStatus from "@/components/CapabilityStatus.vue";
import ProblemNotice from "@/components/ProblemNotice.vue";
import { useGatewayStore } from "@/stores/gateway";

const gateway = useGatewayStore();
const { t } = useI18n();
const capability = computed(
  () => gateway.capabilities?.capabilities.remoteDispatch,
);
const capabilityReasonCode = computed(() => {
  const value = capability.value;
  return value && "reasonCode" in value ? value.reasonCode : undefined;
});
</script>

<template>
  <section class="page-grid" :aria-label="t('navigation.remoteDispatch')">
    <div class="status-panel">
      <div class="panel-heading">
        <div>
          <p class="section-placeholder__eyebrow">
            {{ t("remoteDispatch.module") }}
          </p>
          <h2>{{ t("remoteDispatch.status") }}</h2>
        </div>
        <Send class="panel-symbol" :size="22" aria-hidden="true" />
      </div>
      <p v-if="gateway.loading" class="status-message">
        {{ t("common.loading") }}
      </p>
      <template v-else-if="capability">
        <div class="connection-summary">
          <CapabilityStatus
            :available="capability.available"
            :reason-code="capabilityReasonCode"
          />
        </div>
        <ProblemNotice
          v-if="!capability.available"
          :code="capabilityReasonCode"
          compact
        />
      </template>
      <p v-else class="status-message">{{ t("common.unavailable") }}</p>
    </div>
  </section>
</template>
