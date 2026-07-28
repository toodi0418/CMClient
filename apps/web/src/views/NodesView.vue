<script setup lang="ts">
import { onMounted } from "vue";
import { RefreshCw } from "@lucide/vue";
import Button from "primevue/button";
import { useI18n } from "vue-i18n";

import ProblemNotice from "@/components/ProblemNotice.vue";
import { useDomainStore } from "@/stores/domain";

const domain = useDomainStore();
const { t } = useI18n();

onMounted(() => void domain.refreshNodes());
</script>

<template>
  <section class="page-grid" :aria-label="t('domain.nodes')">
    <div class="status-panel">
      <div class="panel-heading">
        <div>
          <p class="section-placeholder__eyebrow">
            {{ t("navigation.nodes") }}
          </p>
          <h2>{{ t("domain.nodes") }}</h2>
        </div>
        <Button
          unstyled
          class="page-action"
          type="button"
          :aria-label="t('common.refresh')"
          :title="t('common.refresh')"
          :disabled="domain.nodesLoading"
          @click="domain.refreshNodes"
        >
          <RefreshCw :size="17" aria-hidden="true" />
        </Button>
      </div>
      <p
        v-if="domain.nodesLoading && !domain.nodes.length"
        class="status-message"
      >
        {{ t("common.loading") }}
      </p>
      <ProblemNotice
        v-else-if="domain.nodesErrorCode && !domain.nodes.length"
        :code="domain.nodesErrorCode"
        show-retry
        @retry="domain.refreshNodes"
      />
      <p v-else-if="!domain.nodes.length" class="status-message">
        {{ t("domain.empty") }}
      </p>
      <div v-else class="record-list">
        <article
          v-for="node in domain.nodes"
          :key="`${node.meshNetworkId}:${node.nodeNum}`"
          class="record-row"
        >
          <div>
            <strong>{{
              node.longName ?? node.userId ?? `#${node.nodeNum}`
            }}</strong
            ><span>{{ t("domain.network") }}: {{ node.meshNetworkId }}</span>
          </div>
          <div>
            <span>{{ t("domain.node") }}</span
            ><code>{{ node.nodeNum }}</code>
          </div>
          <div>
            <span>{{ t("domain.lastSeen") }}</span
            ><time>{{ node.lastSeenAt }}</time>
          </div>
        </article>
      </div>
      <ProblemNotice
        v-if="domain.nodesErrorCode && domain.nodes.length"
        :code="domain.nodesErrorCode"
        compact
        show-retry
        @retry="domain.refreshNodes"
      />
    </div>
  </section>
</template>
