<script setup lang="ts">
import { onMounted } from "vue";
import { RefreshCw } from "@lucide/vue";
import Button from "primevue/button";
import { useI18n } from "vue-i18n";

import { useAprsStore } from "@/stores/aprs";

const aprs = useAprsStore();
const { t } = useI18n();

onMounted(() => void aprs.refresh());
</script>

<template>
  <section class="page-grid" :aria-label="t('aprs.outbox')">
    <div class="status-panel">
      <div class="panel-heading">
        <div>
          <p class="section-placeholder__eyebrow">{{ t("navigation.aprs") }}</p>
          <h2>{{ t("aprs.outbox") }}</h2>
        </div>
        <Button
          unstyled
          class="page-action"
          type="button"
          :aria-label="t('common.refresh')"
          :title="t('common.refresh')"
          :disabled="aprs.loading"
          @click="aprs.refresh"
          ><RefreshCw :size="17" aria-hidden="true"
        /></Button>
      </div>
      <p v-if="aprs.loading" class="status-message">
        {{ t("common.loading") }}
      </p>
      <p v-else-if="aprs.errorCode" class="status-message">
        {{ t("common.unavailable") }}
        <code class="stable-code">{{ aprs.errorCode }}</code>
      </p>
      <p v-else-if="!aprs.entries.length" class="status-message">
        {{ t("aprs.empty") }}
      </p>
      <div v-else class="record-list">
        <article
          v-for="entry in aprs.entries"
          :key="entry.id"
          class="record-row"
        >
          <div>
            <strong>{{ entry.callsign }}</strong>
            <span
              >{{ t("aprs.canonicalEvent") }}:
              {{ entry.canonicalEventId }}</span
            >
          </div>
          <div>
            <span>{{ t("aprs.state") }}</span>
            <span class="status-badge" :data-state="entry.status">
              {{ t(`aprs.status.${entry.status}`) }}
            </span>
          </div>
          <div>
            <span>{{ t("aprs.attempts") }}: {{ entry.attempts }}</span>
            <time>{{ entry.sentAt ?? entry.nextAttemptAt }}</time>
            <code v-if="entry.lastErrorCode" class="stable-code">{{
              entry.lastErrorCode
            }}</code>
          </div>
        </article>
      </div>
    </div>
  </section>
</template>
