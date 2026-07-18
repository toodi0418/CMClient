<script setup lang="ts">
import { computed, onMounted, watch } from "vue";
import { RefreshCw } from "@lucide/vue";
import Button from "primevue/button";
import { useI18n } from "vue-i18n";

import { useGatewayStore } from "@/stores/gateway";
import { useProxyStore } from "@/stores/proxy";

const gateway = useGatewayStore();
const proxy = useProxyStore();
const { t } = useI18n();

const auditEntries = computed(
  () => proxy.status?.recentAudit.slice().reverse() ?? [],
);

watch(
  () => gateway.lastEventType,
  (eventType) => {
    if (eventType?.startsWith("proxy.")) {
      void proxy.refresh();
    }
  },
);

onMounted(() => void proxy.refresh());
</script>

<template>
  <section class="page-grid" :aria-label="t('proxy.title')">
    <div class="status-panel">
      <div class="panel-heading">
        <div>
          <p class="section-placeholder__eyebrow">
            {{ t("navigation.proxy") }}
          </p>
          <h2>{{ t("proxy.title") }}</h2>
        </div>
        <Button
          unstyled
          class="page-action"
          type="button"
          :aria-label="t('common.refresh')"
          :title="t('common.refresh')"
          :disabled="proxy.loading"
          @click="proxy.refresh"
          ><RefreshCw :size="17" aria-hidden="true"
        /></Button>
      </div>

      <p v-if="proxy.loading && !proxy.status" class="status-message">
        {{ t("common.loading") }}
      </p>
      <p v-else-if="proxy.errorCode && !proxy.status" class="status-message">
        {{ t("common.unavailable") }}
        <code class="stable-code">{{ proxy.errorCode }}</code>
      </p>
      <template v-else-if="proxy.status">
        <div class="record-list">
          <article class="record-row">
            <div>
              <strong>{{ t("proxy.listener") }}</strong>
              <span
                >{{ proxy.status.listener.host }}:{{
                  proxy.status.listener.port
                }}</span
              >
            </div>
            <div>
              <span>{{ t("proxy.mode") }}</span>
              <span class="status-badge" :data-state="proxy.status.state">
                {{ t(`proxy.state.${proxy.status.state}`) }}
              </span>
            </div>
            <div>
              <span>{{ t("proxy.clients") }}</span>
              <strong
                >{{ proxy.status.policy.activeClients }} /
                {{ proxy.status.policy.maxClients }}</strong
              >
            </div>
          </article>
          <article class="record-row">
            <div>
              <strong>{{ t("proxy.upstream") }}</strong>
              <span>{{ proxy.status.upstream.state.transport }}</span>
            </div>
            <div>
              <span>{{ t("proxy.connection") }}</span>
              <span
                class="status-badge"
                :data-state="proxy.status.upstream.state.status"
              >
                {{
                  t(
                    `proxy.connectionState.${proxy.status.upstream.state.status}`,
                  )
                }}
              </span>
            </div>
            <div>
              <span
                >{{ t("proxy.configFrames") }}:
                {{ proxy.status.upstream.configFrameCount }}</span
              >
              <code
                v-if="proxy.status.upstream.lastErrorCode"
                class="stable-code"
                >{{ proxy.status.upstream.lastErrorCode }}</code
              >
            </div>
          </article>
          <article class="record-row">
            <div>
              <strong>{{ t("proxy.queue") }}</strong>
              <span
                >{{ t("proxy.pending") }}:
                {{ proxy.status.queue.pendingCorrelations }}</span
              >
            </div>
            <div>
              <span
                >{{ t("proxy.writes") }}:
                {{ proxy.status.queue.queuedWrites }}</span
              >
              <span
                >{{ t("proxy.broadcast") }}:
                {{ proxy.status.queue.broadcastFrames }}</span
              >
            </div>
            <div>
              <span
                >{{ t("proxy.drops") }}:
                {{
                  proxy.status.queue.broadcastDropped +
                  proxy.status.queue.directDropped
                }}</span
              >
              <code v-if="proxy.status.lastErrorCode" class="stable-code">{{
                proxy.status.lastErrorCode
              }}</code>
            </div>
          </article>
        </div>
        <div v-if="auditEntries.length" class="record-list proxy-audit">
          <p class="section-placeholder__eyebrow">{{ t("proxy.audit") }}</p>
          <article
            v-for="entry in auditEntries"
            :key="`${entry.occurredAt}-${entry.clientFingerprint}-${entry.action}`"
            class="record-row"
          >
            <div>
              <strong>{{ t(`proxy.auditAction.${entry.action}`) }}</strong>
              <time>{{ entry.occurredAt }}</time>
            </div>
            <div>
              <span>{{ entry.mode }}</span>
              <span>{{ entry.variant ?? "-" }}</span>
            </div>
            <div>
              <code v-if="entry.code" class="stable-code">{{
                entry.code
              }}</code>
            </div>
          </article>
        </div>
      </template>
    </div>
  </section>
</template>
