<script setup lang="ts">
import { useI18n } from "vue-i18n";

const { t } = useI18n();
</script>

<template>
  <section class="overview-grid" :aria-label="t('navigation.overview')">
    <div class="signal-board">
      <div class="signal-board__header">
        <span>{{ t("overview.signal") }}</span>
        <span class="signal-board__time">{{ t("overview.local") }}</span>
      </div>
      <div class="signal-board__body">
        <strong>{{ t("overview.awaitingStatus") }}</strong>
        <p>{{ t("overview.awaitingDescription") }}</p>
      </div>
    </div>
    <div class="overview-strip" :aria-label="t('shell.operations')">
      <div>
        <span>{{ t("overview.transport") }}</span>
        <strong>--</strong>
      </div>
      <div>
        <span>{{ t("overview.eventStream") }}</span>
        <strong>--</strong>
      </div>
      <div>
        <span>{{ t("overview.activeJobs") }}</span>
        <strong>--</strong>
      </div>
    </div>
  </section>
</template>
