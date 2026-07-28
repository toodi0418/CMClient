<script setup lang="ts">
import { computed } from "vue";
import { CircleAlert, CircleCheck, Info, RefreshCw } from "@lucide/vue";
import Button from "primevue/button";
import { useI18n } from "vue-i18n";

import { problemForCode } from "@/problems";

const props = withDefaults(
  defineProps<{
    code?: string | null | undefined;
    title?: string | undefined;
    message?: string | undefined;
    compact?: boolean;
    showRetry?: boolean;
  }>(),
  {
    code: undefined,
    title: undefined,
    message: undefined,
    compact: false,
    showRetry: false,
  },
);

const emit = defineEmits<{ retry: [] }>();
const { t } = useI18n();
const problem = computed(() => problemForCode(props.code));
const title = computed(() => props.title ?? t(problem.value.titleKey));
const message = computed(() => props.message ?? t(problem.value.messageKey));
const icon = computed(() => {
  if (problem.value.severity === "error") {
    return CircleAlert;
  }
  if (problem.value.severity === "warning") {
    return Info;
  }
  return CircleCheck;
});
</script>

<template>
  <section
    class="problem-notice"
    :class="[
      `problem-notice--${problem.severity}`,
      { 'problem-notice--compact': compact },
    ]"
    role="status"
  >
    <component
      :is="icon"
      class="problem-notice__icon"
      :size="20"
      aria-hidden="true"
    />
    <div class="problem-notice__content">
      <strong>{{ title }}</strong>
      <p>{{ message }}</p>
      <div class="problem-notice__actions">
        <Button
          v-if="showRetry && problem.retryable"
          unstyled
          class="problem-notice__retry"
          type="button"
          @click="emit('retry')"
        >
          <RefreshCw :size="15" aria-hidden="true" />
          <span>{{ t("problems.retry") }}</span>
        </Button>
        <details v-if="code" class="problem-notice__details">
          <summary>{{ t("problems.technicalDetails") }}</summary>
          <code>{{ code }}</code>
        </details>
      </div>
    </div>
  </section>
</template>
