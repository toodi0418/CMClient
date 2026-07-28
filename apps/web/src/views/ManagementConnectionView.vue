<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from "vue";
import { LoaderCircle, RefreshCw } from "@lucide/vue";
import Button from "primevue/button";
import { useI18n } from "vue-i18n";

import ProblemNotice from "@/components/ProblemNotice.vue";
import { useSetupStore } from "@/stores/setup";

const props = withDefaults(
  defineProps<{
    checking?: boolean | undefined;
    unavailable?: boolean | undefined;
    errorCode?: string | undefined;
    loading?: boolean | undefined;
    retryAction?: (() => Promise<unknown> | unknown) | undefined;
  }>(),
  {
    checking: undefined,
    unavailable: undefined,
    errorCode: undefined,
    loading: undefined,
    retryAction: undefined,
  },
);
const setup = useSetupStore();
const { t } = useI18n();
const retryAttempt = ref(0);
let retryTimer: ReturnType<typeof setTimeout> | undefined;
const isChecking = computed(
  () => props.checking ?? setup.admission === "checking",
);
const isUnavailable = computed(
  () => props.unavailable ?? setup.admission === "unavailable",
);
const errorCode = computed(() => props.errorCode ?? setup.errorCode);
const loading = computed(() => props.loading ?? setup.loading);

function clearRetryTimer() {
  if (retryTimer !== undefined) {
    clearTimeout(retryTimer);
    retryTimer = undefined;
  }
}

function queueRetry() {
  if (retryTimer !== undefined || !isUnavailable.value) {
    return;
  }
  const delay = Math.min(1_000 * 2 ** retryAttempt.value, 8_000);
  retryAttempt.value += 1;
  retryTimer = setTimeout(() => {
    retryTimer = undefined;
    void retry();
  }, delay);
}

async function retry() {
  clearRetryTimer();
  try {
    if (props.retryAction) {
      await props.retryAction();
    } else if (setup.started) {
      await setup.refresh();
    } else {
      await setup.start();
    }
    if (isUnavailable.value) {
      queueRetry();
    } else {
      retryAttempt.value = 0;
    }
  } catch {
    queueRetry();
  }
}

watch(
  isUnavailable,
  (unavailable) => {
    if (unavailable) {
      queueRetry();
      return;
    }
    clearRetryTimer();
    retryAttempt.value = 0;
  },
  { immediate: true },
);

onBeforeUnmount(clearRetryTimer);
</script>

<template>
  <main class="management-connection" aria-live="polite">
    <section class="management-connection__panel">
      <LoaderCircle
        v-if="isChecking"
        class="management-connection__spinner"
        :size="42"
        aria-hidden="true"
      />
      <div v-else class="management-connection__mark" aria-hidden="true">
        CM
      </div>
      <p class="management-connection__eyebrow">CMCLIENT</p>
      <h1>
        {{
          t(
            isChecking
              ? "connection.checkingTitle"
              : "connection.unavailableTitle",
          )
        }}
      </h1>
      <p class="management-connection__copy">
        {{
          t(
            isChecking
              ? "connection.checkingMessage"
              : "connection.unavailableMessage",
          )
        }}
      </p>
      <ProblemNotice
        v-if="isUnavailable && errorCode"
        :code="errorCode"
        compact
      />
      <Button
        v-if="isUnavailable"
        unstyled
        class="command-action management-connection__retry"
        type="button"
        :disabled="loading"
        @click="retry"
      >
        <RefreshCw :size="16" aria-hidden="true" />
        {{ t("problems.retry") }}
      </Button>
    </section>
  </main>
</template>
