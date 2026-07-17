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
  <section class="page-grid" :aria-label="t('domain.messages')">
    <div class="status-panel">
      <div class="panel-heading">
        <div>
          <p class="section-placeholder__eyebrow">
            {{ t("navigation.messages") }}
          </p>
          <h2>{{ t("domain.messages") }}</h2>
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
      <p v-else-if="!domain.messages.length" class="status-message">
        {{ t("domain.empty") }}
      </p>
      <div v-else class="record-list">
        <article
          v-for="message in domain.messages"
          :key="message.id"
          class="record-row record-row--message"
        >
          <p>{{ message.text }}</p>
          <div>
            <span>{{ t("domain.sender") }}</span
            ><code>{{ message.sender }}</code>
          </div>
          <time>{{ message.observedAt }}</time>
        </article>
      </div>
    </div>
  </section>
</template>
