<script setup lang="ts">
import { onMounted, watch } from "vue";
import { RefreshCw } from "@lucide/vue";
import Button from "primevue/button";
import { useI18n } from "vue-i18n";

import { useAprsStore } from "@/stores/aprs";
import { useGatewayStore } from "@/stores/gateway";

const aprs = useAprsStore();
const gateway = useGatewayStore();
const { t } = useI18n();

watch(
  () => gateway.lastEventType,
  (eventType) => {
    if (eventType?.startsWith("aprs.")) {
      void aprs.refresh();
    }
  },
);

onMounted(() => void aprs.refresh());
</script>

<template>
  <section class="page-grid" :aria-label="t('aprs.outbox')">
    <div class="status-panel">
      <div class="panel-heading">
        <div>
          <p class="section-placeholder__eyebrow">{{ t("navigation.aprs") }}</p>
          <h2>{{ t("aprs.runtime") }}</h2>
        </div>
        <Button
          unstyled
          class="page-action"
          type="button"
          :aria-label="t('common.refresh')"
          :title="t('common.refresh')"
          :disabled="aprs.loading"
          @click="aprs.refresh"
          ><RefreshCw :size="17" aria-hidden="true"
        /></Button>
      </div>
      <p v-if="aprs.loading && !aprs.status" class="status-message">
        {{ t("common.loading") }}
      </p>
      <p
        v-else-if="aprs.runtimeErrorCode && !aprs.status"
        class="status-message"
      >
        {{ t("common.unavailable") }}
        <code class="stable-code">{{ aprs.runtimeErrorCode }}</code>
      </p>
      <template v-else-if="aprs.status">
        <dl class="facts-grid">
          <div>
            <dt>{{ t("aprs.state") }}</dt>
            <dd>
              <span
                class="status-badge"
                :data-state="aprs.status.running ? 'available' : 'unavailable'"
              >
                {{
                  aprs.status.running
                    ? t("aprs.running")
                    : aprs.status.configured
                      ? t("aprs.stopped")
                      : t("common.notConfigured")
                }}
              </span>
            </dd>
          </div>
          <div>
            <dt>{{ t("aprs.monitor") }}</dt>
            <dd>
              <span
                class="status-badge"
                :data-state="aprs.status.monitorStatus"
              >
                {{ t(`aprs.monitorStatus.${aprs.status.monitorStatus}`) }}
              </span>
            </dd>
          </div>
          <div>
            <dt>{{ t("aprs.mappedCallsigns") }}</dt>
            <dd>{{ aprs.status.mappedCallsigns }}</dd>
          </div>
          <div>
            <dt>{{ t("aprs.pending") }}</dt>
            <dd>{{ aprs.status.pendingOutbox }}</dd>
          </div>
          <div>
            <dt>{{ t("aprs.failed") }}</dt>
            <dd>{{ aprs.status.failedOutbox }}</dd>
          </div>
          <div>
            <dt>{{ t("aprs.stationPending") }}</dt>
            <dd>{{ aprs.status.pendingStationSubmissions }}</dd>
          </div>
          <div>
            <dt>{{ t("aprs.stationFailed") }}</dt>
            <dd>{{ aprs.status.failedStationSubmissions }}</dd>
          </div>
          <div>
            <dt>{{ t("aprs.configured") }}</dt>
            <dd>
              {{
                aprs.status.configured
                  ? t("common.available")
                  : t("common.notConfigured")
              }}
            </dd>
          </div>
        </dl>
        <code v-if="aprs.status.lastErrorCode" class="stable-code">{{
          aprs.status.lastErrorCode
        }}</code>
        <code v-if="aprs.runtimeErrorCode" class="stable-code">{{
          aprs.runtimeErrorCode
        }}</code>
      </template>
    </div>

    <div class="status-panel">
      <div class="panel-heading">
        <div>
          <p class="section-placeholder__eyebrow">{{ t("navigation.aprs") }}</p>
          <h2>{{ t("aprs.outbox") }}</h2>
        </div>
      </div>
      <p v-if="aprs.loading && !aprs.entries.length" class="status-message">
        {{ t("common.loading") }}
      </p>
      <p
        v-else-if="aprs.outboxErrorCode && !aprs.entries.length"
        class="status-message"
      >
        {{ t("common.unavailable") }}
        <code class="stable-code">{{ aprs.outboxErrorCode }}</code>
      </p>
      <p v-else-if="!aprs.entries.length" class="status-message">
        {{ t("aprs.empty") }}
      </p>
      <div v-else class="record-list">
        <article
          v-for="entry in aprs.entries"
          :key="entry.id"
          class="record-row"
        >
          <div>
            <strong>{{ entry.callsign }}</strong>
            <span
              >{{ t("aprs.canonicalEvent") }}:
              {{ entry.canonicalEventId }}</span
            >
          </div>
          <div>
            <span>{{ t("aprs.transportState") }}</span>
            <span class="status-badge" :data-state="entry.status">
              {{ t(`aprs.status.${entry.status}`) }}
            </span>
            <span>{{ t("aprs.deliveryState") }}</span>
            <span class="status-badge" :data-state="entry.deliveryStatus">
              {{ t(`aprs.deliveryStatus.${entry.deliveryStatus}`) }}
            </span>
          </div>
          <div>
            <span>{{ t("aprs.attempts") }}: {{ entry.attempts }}</span>
            <template v-if="entry.observerConfirmedAt">
              <span>{{ t("aprs.observerConfirmedAt") }}</span>
              <time>{{ entry.observerConfirmedAt }}</time>
            </template>
            <template
              v-else-if="
                entry.deliveryStatus === 'observation_expired' &&
                entry.observationExpiresAt
              "
            >
              <span>{{ t("aprs.observationExpiresAt") }}</span>
              <time>{{ entry.observationExpiresAt }}</time>
            </template>
            <template v-else-if="entry.submittedAt ?? entry.sentAt">
              <span>{{ t("aprs.submittedAt") }}</span>
              <time>{{ entry.submittedAt ?? entry.sentAt }}</time>
            </template>
            <template v-else>
              <span>{{ t("aprs.nextAttemptAt") }}</span>
              <time>{{ entry.nextAttemptAt }}</time>
            </template>
            <code v-if="entry.lastErrorCode" class="stable-code">{{
              entry.lastErrorCode
            }}</code>
          </div>
        </article>
      </div>
      <code v-if="aprs.outboxErrorCode" class="stable-code">{{
        aprs.outboxErrorCode
      }}</code>
    </div>

    <div class="status-panel">
      <div class="panel-heading">
        <div>
          <p class="section-placeholder__eyebrow">{{ t("navigation.aprs") }}</p>
          <h2>{{ t("aprs.stationDelivery") }}</h2>
        </div>
      </div>
      <p
        v-if="aprs.loading && !aprs.stationSubmissions.length"
        class="status-message"
      >
        {{ t("common.loading") }}
      </p>
      <p
        v-else-if="aprs.stationErrorCode && !aprs.stationSubmissions.length"
        class="status-message"
      >
        {{ t("common.unavailable") }}
        <code class="stable-code">{{ aprs.stationErrorCode }}</code>
      </p>
      <p v-else-if="!aprs.stationSubmissions.length" class="status-message">
        {{ t("aprs.stationEmpty") }}
      </p>
      <div v-else class="record-list">
        <article
          v-for="submission in aprs.stationSubmissions"
          :key="submission.id"
          class="record-row"
        >
          <div>
            <strong>{{
              t(`aprs.stationPacketKind.${submission.packetKind}`)
            }}</strong>
          </div>
          <div>
            <span>{{ t("aprs.deliveryState") }}</span>
            <span class="status-badge" :data-state="submission.deliveryStatus">
              {{ t(`aprs.stationDeliveryStatus.${submission.deliveryStatus}`) }}
            </span>
          </div>
          <div>
            <template v-if="submission.observerConfirmedAt">
              <span>{{ t("aprs.observerConfirmedAt") }}</span>
              <time>{{ submission.observerConfirmedAt }}</time>
            </template>
            <template v-else-if="submission.submittedAt">
              <span>{{ t("aprs.submittedAt") }}</span>
              <time>{{ submission.submittedAt }}</time>
            </template>
            <template v-else>
              <span>{{ t("aprs.attemptedAt") }}</span>
              <time>{{ submission.attemptedAt }}</time>
            </template>
          </div>
        </article>
      </div>
      <code v-if="aprs.stationErrorCode" class="stable-code">{{
        aprs.stationErrorCode
      }}</code>
    </div>
  </section>
</template>
