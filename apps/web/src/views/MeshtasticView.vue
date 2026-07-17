<script setup lang="ts">
import { computed } from "vue";
import { RadioTower, RefreshCw } from "@lucide/vue";
import Button from "primevue/button";
import { useI18n } from "vue-i18n";

import { useGatewayStore } from "@/stores/gateway";

const gateway = useGatewayStore();
const { t } = useI18n();

const serialCapability = computed(
  () => gateway.capabilities?.capabilities.serial,
);
</script>

<template>
  <section class="page-grid" :aria-label="t('navigation.meshtastic')">
    <div class="status-panel">
      <div class="panel-heading">
        <div>
          <p class="section-placeholder__eyebrow">
            {{ t("meshtastic.transport") }}
          </p>
          <h2>{{ t("meshtastic.serialCapability") }}</h2>
        </div>
        <RadioTower class="panel-symbol" :size="22" aria-hidden="true" />
      </div>
      <p v-if="gateway.loading" class="status-message">
        {{ t("common.loading") }}
      </p>
      <template v-else-if="serialCapability">
        <p class="status-message">
          {{
            serialCapability.available
              ? t("meshtastic.ready")
              : t("meshtastic.unavailable")
          }}
        </p>
        <div class="connection-summary">
          <span
            class="status-badge"
            :data-state="
              serialCapability.available ? 'available' : 'unavailable'
            "
          >
            {{
              serialCapability.available
                ? t("common.available")
                : t("common.notConfigured")
            }}
          </span>
          <code v-if="!serialCapability.available">{{
            serialCapability.reasonCode
          }}</code>
        </div>
      </template>
      <p v-else class="status-message">{{ t("common.unavailable") }}</p>
    </div>

    <div class="status-panel">
      <div class="panel-heading">
        <div>
          <p class="section-placeholder__eyebrow">
            {{ t("meshtastic.transport") }}
          </p>
          <h2>{{ t("meshtastic.eventConnection") }}</h2>
        </div>
        <Button
          unstyled
          class="page-action"
          type="button"
          :aria-label="t('common.refresh')"
          :title="t('common.refresh')"
          :disabled="gateway.loading"
          @click="gateway.refresh"
        >
          <RefreshCw :size="17" aria-hidden="true" />
        </Button>
      </div>
      <div class="connection-summary">
        <span
          class="status-badge"
          :data-state="
            gateway.eventConnection === 'open' ? 'available' : 'unavailable'
          "
        >
          {{ t(`eventState.${gateway.eventConnection}`) }}
        </span>
        <code v-if="gateway.lastEventType">{{ gateway.lastEventType }}</code>
        <code v-else-if="gateway.errorCode">{{ gateway.errorCode }}</code>
      </div>
    </div>
  </section>
</template>
