<script setup lang="ts">
import { computed } from "vue";
import { useI18n } from "vue-i18n";

import { capabilityReasonKey } from "@/problems";

const props = defineProps<{
  available: boolean;
  reasonCode?: string | null | undefined;
}>();

const { t } = useI18n();
const label = computed(() =>
  props.available
    ? t("common.available")
    : t(capabilityReasonKey(props.reasonCode)),
);
</script>

<template>
  <span
    class="status-badge"
    :data-state="available ? 'available' : 'unavailable'"
  >
    {{ label }}
  </span>
</template>
