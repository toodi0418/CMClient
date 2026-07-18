<script setup lang="ts">
import { computed } from "vue";
import { Send } from "@lucide/vue";
import { useI18n } from "vue-i18n";

import { useGatewayStore } from "@/stores/gateway";

const gateway = useGatewayStore();
const { t } = useI18n();
const capability = computed(
  () => gateway.capabilities?.capabilities.remoteDispatch,
);
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
      <div v-else-if="capability" class="connection-summary">
        <span
          class="status-badge"
          :data-state="capability.available ? 'available' : 'unavailable'"
        >
          {{
            capability.available
              ? t("common.available")
              : t("common.unavailable")
          }}
        </span>
        <code v-if="!capability.available">{{ capability.reasonCode }}</code>
      </div>
      <p v-else class="status-message">{{ t("common.unavailable") }}</p>
    </div>
  </section>
</template>
