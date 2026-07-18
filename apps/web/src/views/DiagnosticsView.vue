<script setup lang="ts">
import { computed } from "vue";
import { Activity, RefreshCw, ShieldCheck, X } from "@lucide/vue";
import Button from "primevue/button";
import { useI18n } from "vue-i18n";

import { useDiagnosticsStore } from "@/stores/diagnostics";

const diagnostics = useDiagnosticsStore();
const { t } = useI18n();

const active = computed(() =>
  ["queued", "running", "waiting", "cancelling", "rolling_back"].includes(
    diagnostics.job?.status ?? "",
  ),
);
</script>

<template>
  <section class="page-grid" :aria-label="t('navigation.diagnostics')">
    <div class="status-panel">
      <div class="panel-heading">
        <div>
          <p class="section-placeholder__eyebrow">
            {{ t("navigation.diagnostics") }}
          </p>
          <h2>{{ t("diagnostics.integrityCheck") }}</h2>
        </div>
        <div class="panel-actions">
          <Button
            v-if="diagnostics.job"
            unstyled
            class="page-action"
            type="button"
            :aria-label="t('common.refresh')"
            :title="t('common.refresh')"
            :disabled="diagnostics.loading"
            @click="diagnostics.refresh"
            ><RefreshCw :size="17" aria-hidden="true"
          /></Button>
          <Button
            v-if="active"
            unstyled
            class="page-action"
            type="button"
            :aria-label="t('diagnostics.cancel')"
            :title="t('diagnostics.cancel')"
            :disabled="diagnostics.loading"
            @click="diagnostics.cancel"
            ><X :size="17" aria-hidden="true"
          /></Button>
        </div>
      </div>
      <div class="diagnostics-command">
        <div>
          <Activity :size="19" aria-hidden="true" />
          <span>{{ t("diagnostics.integrityCheck") }}</span>
        </div>
        <Button
          unstyled
          class="command-action"
          type="button"
          :disabled="diagnostics.loading || active"
          @click="diagnostics.runIntegrityCheck"
          ><ShieldCheck :size="17" aria-hidden="true" />
          <span>{{ t("diagnostics.run") }}</span>
        </Button>
      </div>
      <p v-if="diagnostics.errorCode" class="status-message">
        {{ t("common.unavailable") }}
        <code class="stable-code">{{ diagnostics.errorCode }}</code>
      </p>
      <p v-else-if="!diagnostics.job" class="status-message">
        {{ t("diagnostics.ready") }}
      </p>
      <dl v-else class="facts-grid">
        <div>
          <dt>{{ t("diagnostics.status") }}</dt>
          <dd>
            <span class="status-badge">{{
              t(`job.status.${diagnostics.job.status}`)
            }}</span>
          </dd>
        </div>
        <div>
          <dt>{{ t("diagnostics.jobId") }}</dt>
          <dd>
            <code>{{ diagnostics.job.id }}</code>
          </dd>
        </div>
        <div>
          <dt>{{ t("diagnostics.createdAt") }}</dt>
          <dd>
            <time>{{ diagnostics.job.createdAt }}</time>
          </dd>
        </div>
        <div>
          <dt>{{ t("diagnostics.updatedAt") }}</dt>
          <dd>
            <time>{{ diagnostics.job.updatedAt }}</time>
          </dd>
        </div>
      </dl>
      <code v-if="diagnostics.job?.error" class="stable-code">{{
        diagnostics.job.error.code
      }}</code>
    </div>
  </section>
</template>
