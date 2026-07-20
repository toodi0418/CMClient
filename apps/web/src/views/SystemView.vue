<script setup lang="ts">
import { computed } from "vue";
import { RefreshCw } from "@lucide/vue";
import Button from "primevue/button";
import { useI18n } from "vue-i18n";

import { useGatewayStore } from "@/stores/gateway";

const gateway = useGatewayStore();
const { t } = useI18n();

const capabilityRows = computed(() =>
  Object.entries(gateway.capabilities?.capabilities ?? {}).map(
    ([key, capability]) => ({ key, capability }),
  ),
);
</script>

<template>
  <section class="page-grid" :aria-label="t('navigation.system')">
    <div class="status-panel">
      <div class="panel-heading">
        <div>
          <p class="section-placeholder__eyebrow">{{ t("system.health") }}</p>
          <h2>{{ t(`gateway.${gateway.availability}`) }}</h2>
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
      <p v-if="gateway.loading" class="status-message">
        {{ t("common.loading") }}
      </p>
      <p v-else-if="gateway.errorCode" class="status-message">
        {{ t("common.unavailable") }}
        <code class="stable-code">{{ gateway.errorCode }}</code>
      </p>
      <dl v-else-if="gateway.status" class="facts-grid">
        <div>
          <dt>{{ t("system.platform") }}</dt>
          <dd>
            {{ gateway.status.identity.identity.target.os }} /
            {{ gateway.status.identity.identity.target.architecture }} /
            {{ gateway.status.identity.identity.target.profile }} /
            {{ gateway.status.identity.identity.target.packageProfile }}
          </dd>
        </div>
        <div>
          <dt>{{ t("system.version") }}</dt>
          <dd>{{ gateway.status.identity.identity.version }}</dd>
        </div>
        <div>
          <dt>{{ t("system.channel") }}</dt>
          <dd>{{ gateway.status.identity.identity.channel }}</dd>
        </div>
        <div>
          <dt>{{ t("system.commit") }}</dt>
          <dd>
            <code>{{ gateway.status.identity.identity.sourceCommit }}</code>
          </dd>
        </div>
      </dl>
    </div>

    <div class="status-panel">
      <div class="panel-heading">
        <div>
          <p class="section-placeholder__eyebrow">{{ t("system.build") }}</p>
          <h2>{{ t("system.capabilityMatrix") }}</h2>
        </div>
      </div>
      <div v-if="capabilityRows.length" class="capability-list">
        <div
          v-for="row in capabilityRows"
          :key="row.key"
          class="capability-row"
        >
          <span>{{ t(`capability.${row.key}`) }}</span>
          <span
            class="status-badge"
            :data-state="row.capability.available ? 'available' : 'unavailable'"
          >
            {{
              row.capability.available
                ? t("common.available")
                : t("common.notConfigured")
            }}
          </span>
          <code v-if="!row.capability.available">{{
            row.capability.reasonCode
          }}</code>
        </div>
      </div>
      <p v-else class="status-message">{{ t("common.unavailable") }}</p>
    </div>
  </section>
</template>
