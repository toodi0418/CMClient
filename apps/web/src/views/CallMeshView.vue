<script setup lang="ts">
import { onMounted } from "vue";
import { RefreshCw } from "@lucide/vue";
import Button from "primevue/button";
import { useI18n } from "vue-i18n";

import ProblemNotice from "@/components/ProblemNotice.vue";
import { useCallMeshStore } from "@/stores/callmesh";

const callmesh = useCallMeshStore();
const { t } = useI18n();

onMounted(() => void callmesh.refresh());
</script>

<template>
  <section class="page-grid" :aria-label="t('navigation.callmesh')">
    <div class="status-panel">
      <div class="panel-heading">
        <div>
          <p class="section-placeholder__eyebrow">
            {{ t("navigation.callmesh") }}
          </p>
          <h2>{{ t("callmesh.status") }}</h2>
        </div>
        <Button
          unstyled
          class="page-action"
          type="button"
          :aria-label="t('common.refresh')"
          :title="t('common.refresh')"
          :disabled="callmesh.loading"
          @click="callmesh.refresh"
          ><RefreshCw :size="17" aria-hidden="true"
        /></Button>
      </div>
      <p v-if="callmesh.loading" class="status-message">
        {{ t("common.loading") }}
      </p>
      <ProblemNotice
        v-else-if="callmesh.errorCode && !callmesh.status"
        :code="callmesh.errorCode"
        show-retry
        @retry="callmesh.refresh"
      />
      <dl v-else-if="callmesh.status" class="facts-grid">
        <div>
          <dt>{{ t("callmesh.state") }}</dt>
          <dd>
            <span class="status-badge" :data-state="callmesh.status.state">
              {{ t(`callmesh.stateLabel.${callmesh.status.state}`) }}
            </span>
          </dd>
        </div>
        <div>
          <dt>{{ t("callmesh.mappingCount") }}</dt>
          <dd>{{ callmesh.status.activeMappingCount }}</dd>
        </div>
        <div>
          <dt>{{ t("callmesh.mappingVersion") }}</dt>
          <dd>{{ callmesh.status.activeMappingVersion ?? "--" }}</dd>
        </div>
        <div>
          <dt>{{ t("callmesh.updatedAt") }}</dt>
          <dd>
            <time>{{ callmesh.status.updatedAt }}</time>
          </dd>
        </div>
      </dl>
      <ProblemNotice
        v-if="callmesh.status?.reasonCode && !callmesh.errorCode"
        :code="callmesh.status.reasonCode"
        compact
      />
      <ProblemNotice
        v-if="callmesh.errorCode && callmesh.status"
        :code="callmesh.errorCode"
        compact
        show-retry
        @retry="callmesh.refresh"
      />
    </div>

    <div class="status-panel">
      <div class="panel-heading">
        <div>
          <p class="section-placeholder__eyebrow">
            {{ t("callmesh.mappingVersion") }}
          </p>
          <h2>{{ t("callmesh.mappings") }}</h2>
        </div>
      </div>
      <p v-if="!callmesh.mappings.length" class="status-message">
        {{ t("callmesh.empty") }}
      </p>
      <div v-else class="record-list">
        <article
          v-for="mapping in callmesh.mappings"
          :key="`${mapping.version}:${mapping.effectiveAt}:${mapping.meshNetworkId}:${mapping.nodeNum}:${mapping.callsign}`"
          class="record-row"
        >
          <div>
            <strong>{{ mapping.callsign }}</strong>
            <span>{{ t("domain.network") }}: {{ mapping.meshNetworkId }}</span>
          </div>
          <div>
            <span>{{ t("domain.node") }}</span>
            <code>{{ mapping.nodeNum }}</code>
          </div>
          <div>
            <span>{{ mapping.version }}</span>
            <time>{{ mapping.effectiveAt }}</time>
          </div>
        </article>
      </div>
    </div>
  </section>
</template>
