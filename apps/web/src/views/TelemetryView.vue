<script setup lang="ts">
import { onMounted } from "vue";
import { RefreshCw } from "@lucide/vue";
import Button from "primevue/button";
import { useI18n } from "vue-i18n";

import { useDomainStore } from "@/stores/domain";

const domain = useDomainStore();
const { t } = useI18n();

onMounted(() => void domain.refresh());
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
          @click="domain.refresh"
          ><RefreshCw :size="17" aria-hidden="true"
        /></Button>
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
      <div v-else class="record-list">
        <article
          v-for="entry in domain.telemetry"
          :key="entry.id"
          class="record-row"
        >
          <div>
            <strong>{{ entry.metricKind }}</strong
            ><span>{{ t("domain.network") }}: {{ entry.meshNetworkId }}</span>
          </div>
          <div>
            <span>{{ t("domain.node") }}</span
            ><code>{{ entry.nodeNum }}</code>
          </div>
          <div>
            <span>{{ t("domain.metric") }}</span
            ><code>{{ Object.keys(entry.metrics).join(", ") }}</code>
          </div>
        </article>
      </div>
    </div>
  </section>
</template>
