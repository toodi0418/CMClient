<script setup lang="ts">
import { useI18n } from "vue-i18n";

defineProps<{
  labelKey: string;
  summaryKey: string;
}>();

const { t } = useI18n();
</script>

<template>
  <section class="section-placeholder">
    <p class="section-placeholder__eyebrow">{{ t("section.module") }}</p>
    <h2>{{ t(labelKey) }}</h2>
    <p>{{ t(summaryKey) }}</p>
  </section>
</template>
