<script setup lang="ts">
import { useI18n } from "vue-i18n";

import { useGatewayStore } from "@/stores/gateway";

const gateway = useGatewayStore();
const { t } = useI18n();
</script>

<template>
  <section class="page-grid" :aria-label="t('logs.events')">
    <div class="status-panel">
      <div class="panel-heading">
        <div>
          <p class="section-placeholder__eyebrow">{{ t("navigation.logs") }}</p>
          <h2>{{ t("logs.events") }}</h2>
        </div>
      </div>
      <p v-if="!gateway.recentEvents.length" class="status-message">
        {{ t("logs.empty") }}
      </p>
      <div v-else class="record-list">
        <article
          v-for="event in gateway.recentEvents"
          :key="event.eventId"
          class="record-row"
        >
          <div>
            <strong>{{ event.type }}</strong>
            <span>{{ event.source }}</span>
          </div>
          <div>
            <span>{{ t("logs.eventId") }}</span>
            <code>{{ event.eventId }}</code>
          </div>
          <time>{{ event.occurredAt }}</time>
        </article>
      </div>
    </div>
  </section>
</template>
