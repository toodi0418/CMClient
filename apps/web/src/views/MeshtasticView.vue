<script setup lang="ts">
import { computed, onMounted, watch } from "vue";
import { RadioTower, RefreshCw } from "@lucide/vue";
import Button from "primevue/button";
import { useI18n } from "vue-i18n";

import { useGatewayStore } from "@/stores/gateway";
import { useMeshtasticStore } from "@/stores/meshtastic";

const gateway = useGatewayStore();
const meshtastic = useMeshtasticStore();
const { t } = useI18n();

const serialCapability = computed(
  () => gateway.capabilities?.capabilities.serial,
);

watch(
  () => gateway.lastEventType,
  (eventType) => {
    if (eventType?.startsWith("mesh.")) {
      void meshtastic.refresh();
    }
  },
);

async function refreshRuntime() {
  await Promise.all([gateway.refresh(), meshtastic.refresh()]);
}

onMounted(() => void meshtastic.refresh());
</script>

<template>
  <section class="page-grid" :aria-label="t('navigation.meshtastic')">
    <div class="status-panel">
      <div class="panel-heading">
        <div>
          <p class="section-placeholder__eyebrow">
            {{ t("meshtastic.transport") }}
          </p>
          <h2>{{ t("meshtastic.runtime") }}</h2>
        </div>
        <Button
          unstyled
          class="page-action"
          type="button"
          :aria-label="t('common.refresh')"
          :title="t('common.refresh')"
          :disabled="meshtastic.loading"
          @click="refreshRuntime"
        >
          <RefreshCw :size="17" aria-hidden="true" />
        </Button>
      </div>
      <p v-if="meshtastic.loading && !meshtastic.status" class="status-message">
        {{ t("common.loading") }}
      </p>
      <p
        v-else-if="meshtastic.errorCode && !meshtastic.status"
        class="status-message"
      >
        {{ t("common.unavailable") }}
        <code class="stable-code">{{ meshtastic.errorCode }}</code>
      </p>
      <template v-else-if="meshtastic.status">
        <dl class="facts-grid">
          <div>
            <dt>{{ t("meshtastic.configured") }}</dt>
            <dd>
              <span
                class="status-badge"
                :data-state="
                  meshtastic.status.configured ? 'available' : 'unavailable'
                "
              >
                {{
                  meshtastic.status.configured
                    ? t("common.available")
                    : t("common.notConfigured")
                }}
              </span>
            </dd>
          </div>
          <div>
            <dt>{{ t("meshtastic.connection") }}</dt>
            <dd>
              <span
                v-if="meshtastic.status.connection"
                class="status-badge"
                :data-state="meshtastic.status.connection.status"
              >
                {{
                  t(
                    `meshtastic.connectionState.${meshtastic.status.connection.status}`,
                  )
                }}
              </span>
              <span v-else>{{ t("common.notConfigured") }}</span>
            </dd>
          </div>
          <div>
            <dt>{{ t("meshtastic.transportKind") }}</dt>
            <dd>{{ meshtastic.status.connection?.transport ?? "--" }}</dd>
          </div>
          <div>
            <dt>{{ t("meshtastic.network") }}</dt>
            <dd>{{ meshtastic.status.meshNetworkId ?? "--" }}</dd>
          </div>
          <div>
            <dt>{{ t("meshtastic.gateway") }}</dt>
            <dd>{{ meshtastic.status.gatewayId ?? "--" }}</dd>
          </div>
          <div>
            <dt>{{ t("meshtastic.frames") }}</dt>
            <dd>
              {{ meshtastic.status.metrics?.framesReceived ?? 0 }} /
              {{ meshtastic.status.metrics?.framesSent ?? 0 }}
            </dd>
          </div>
          <div>
            <dt>{{ t("meshtastic.malformed") }}</dt>
            <dd>{{ meshtastic.status.metrics?.malformedFrames ?? 0 }}</dd>
          </div>
          <div>
            <dt>{{ t("meshtastic.reconnects") }}</dt>
            <dd>{{ meshtastic.status.metrics?.reconnects ?? 0 }}</dd>
          </div>
        </dl>
        <code
          v-if="meshtastic.status.connection?.reasonCode"
          class="stable-code"
          >{{ meshtastic.status.connection.reasonCode }}</code
        >
        <code v-if="meshtastic.errorCode" class="stable-code">{{
          meshtastic.errorCode
        }}</code>
      </template>
    </div>

    <div class="status-panel">
      <div class="panel-heading">
        <div>
          <p class="section-placeholder__eyebrow">
            {{ t("meshtastic.transport") }}
          </p>
          <h2>{{ t("meshtastic.serialCapability") }}</h2>
        </div>
        <RadioTower class="panel-symbol" :size="22" aria-hidden="true" />
      </div>
      <p v-if="gateway.loading" class="status-message">
        {{ t("common.loading") }}
      </p>
      <template v-else-if="serialCapability">
        <p class="status-message">
          {{
            serialCapability.available
              ? t("meshtastic.ready")
              : t("meshtastic.unavailable")
          }}
        </p>
        <div class="connection-summary">
          <span
            class="status-badge"
            :data-state="
              serialCapability.available ? 'available' : 'unavailable'
            "
          >
            {{
              serialCapability.available
                ? t("common.available")
                : t("common.notConfigured")
            }}
          </span>
          <code v-if="!serialCapability.available">{{
            serialCapability.reasonCode
          }}</code>
        </div>
      </template>
      <p v-else class="status-message">{{ t("common.unavailable") }}</p>
    </div>

    <div class="status-panel">
      <div class="panel-heading">
        <div>
          <p class="section-placeholder__eyebrow">
            {{ t("meshtastic.transport") }}
          </p>
          <h2>{{ t("meshtastic.eventConnection") }}</h2>
        </div>
        <RadioTower class="panel-symbol" :size="22" aria-hidden="true" />
      </div>
      <div class="connection-summary">
        <span
          class="status-badge"
          :data-state="
            gateway.eventConnection === 'open' ? 'available' : 'unavailable'
          "
        >
          {{ t(`eventState.${gateway.eventConnection}`) }}
        </span>
        <code v-if="gateway.lastEventType">{{ gateway.lastEventType }}</code>
        <code v-else-if="gateway.errorCode">{{ gateway.errorCode }}</code>
      </div>
    </div>
  </section>
</template>
