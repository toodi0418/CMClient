<script setup lang="ts">
import { computed } from "vue";
import { RefreshCw } from "@lucide/vue";
import Button from "primevue/button";
import { useI18n } from "vue-i18n";

import CapabilityStatus from "@/components/CapabilityStatus.vue";
import ProblemNotice from "@/components/ProblemNotice.vue";
import { useGatewayStore } from "@/stores/gateway";

const gateway = useGatewayStore();
const { t } = useI18n();

const capabilityRows = computed(() =>
  Object.entries(gateway.capabilities?.capabilities ?? {}).map(
    ([key, capability]) => ({
      key,
      capability,
      reasonCode:
        "reasonCode" in capability ? capability.reasonCode : undefined,
    }),
  ),
);
</script>

<template>
  <section class="page-grid" :aria-label="t('navigation.system')">
    <div class="status-panel">
      <div class="panel-heading">
        <div>
          <p class="section-placeholder__eyebrow">{{ t("system.health") }}</p>
          <h2>{{ t(`gateway.${gateway.availability}`) }}</h2>
        </div>
        <Button
          unstyled
          class="page-action"
          type="button"
          :aria-label="t('common.refresh')"
          :title="t('common.refresh')"
          :disabled="gateway.loading"
          @click="gateway.refresh"
        >
          <RefreshCw :size="17" aria-hidden="true" />
        </Button>
      </div>
      <p v-if="gateway.loading" class="status-message">
        {{ t("common.loading") }}
      </p>
      <ProblemNotice
        v-else-if="gateway.errorCode && !gateway.status"
        :code="gateway.errorCode"
        show-retry
        @retry="gateway.refresh"
      />
      <dl v-else-if="gateway.status" class="facts-grid">
        <div>
          <dt>{{ t("system.platform") }}</dt>
          <dd>
            {{ gateway.status.identity.identity.target.os }} /
            {{ gateway.status.identity.identity.target.architecture }} /
            {{ gateway.status.identity.identity.target.profile }} /
            {{ gateway.status.identity.identity.target.packageProfile }}
          </dd>
        </div>
        <div>
          <dt>{{ t("system.version") }}</dt>
          <dd>{{ gateway.status.identity.identity.version }}</dd>
        </div>
        <div>
          <dt>{{ t("system.channel") }}</dt>
          <dd>{{ gateway.status.identity.identity.channel }}</dd>
        </div>
        <div>
          <dt>{{ t("system.commit") }}</dt>
          <dd>
            <code>{{ gateway.status.identity.identity.sourceCommit }}</code>
          </dd>
        </div>
      </dl>
      <ProblemNotice
        v-if="gateway.errorCode && gateway.status"
        :code="gateway.errorCode"
        compact
        show-retry
        @retry="gateway.refresh"
      />
    </div>

    <div class="status-panel">
      <div class="panel-heading">
        <div>
          <p class="section-placeholder__eyebrow">{{ t("system.build") }}</p>
          <h2>{{ t("system.capabilityMatrix") }}</h2>
        </div>
      </div>
      <div v-if="capabilityRows.length" class="capability-list">
        <div
          v-for="row in capabilityRows"
          :key="row.key"
          class="capability-row"
        >
          <span>{{ t(`capability.${row.key}`) }}</span>
          <CapabilityStatus
            :available="row.capability.available"
            :reason-code="row.reasonCode"
          />
        </div>
      </div>
      <p v-else class="status-message">{{ t("common.unavailable") }}</p>
    </div>
  </section>
</template>
