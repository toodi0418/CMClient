<script setup lang="ts">
import { computed, onMounted } from "vue";
import { RefreshCw } from "@lucide/vue";
import Button from "primevue/button";
import { useI18n } from "vue-i18n";

import ProblemNotice from "@/components/ProblemNotice.vue";
import { useAprsStore } from "@/stores/aprs";

const aprs = useAprsStore();
const { t } = useI18n();

const sharedBlockingErrorCode = computed(() => {
  if (aprs.status || aprs.entries.length || aprs.stationSubmissions.length) {
    return undefined;
  }
  const codes = [
    aprs.runtimeErrorCode,
    aprs.outboxErrorCode,
    aprs.stationErrorCode,
  ].filter((code): code is string => Boolean(code));

  return codes.length === 3 && codes.every((code) => code === codes[0])
    ? codes[0]
    : undefined;
});

onMounted(() => void aprs.refresh());
</script>

<template>
  <section class="page-grid" :aria-label="t('aprs.outbox')">
    <ProblemNotice
      v-if="sharedBlockingErrorCode"
      class="page-problem-notice"
      :code="sharedBlockingErrorCode"
      show-retry
      @retry="aprs.refresh"
    />
    <template v-else>
      <div class="status-panel">
        <div class="panel-heading">
          <div>
            <p class="section-placeholder__eyebrow">
              {{ t("navigation.aprs") }}
            </p>
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
        <ProblemNotice
          v-else-if="aprs.runtimeErrorCode && !aprs.status"
          :code="aprs.runtimeErrorCode"
          show-retry
          @retry="aprs.refresh"
        />
        <template v-else-if="aprs.status">
          <dl class="facts-grid">
            <div>
              <dt>{{ t("aprs.state") }}</dt>
              <dd>
                <span
                  class="status-badge"
                  :data-state="
                    aprs.status.directAprs
                      ? aprs.status.directAprs.beaconState
                      : aprs.status.running
                        ? 'available'
                        : 'unavailable'
                  "
                >
                  {{
                    aprs.status.directAprs
                      ? t(
                          `aprs.directBeaconState.${aprs.status.directAprs.beaconState}`,
                        )
                      : aprs.status.running
                        ? t("aprs.running")
                        : aprs.status.configured
                          ? t("aprs.stopped")
                          : t("common.notConfigured")
                  }}
                </span>
              </dd>
            </div>
            <div v-if="aprs.status.directAprs">
              <dt>{{ t("aprs.directCapability") }}</dt>
              <dd>
                <span
                  class="status-badge"
                  :data-state="aprs.status.directAprs.capabilityState"
                >
                  {{
                    t(
                      `aprs.directCapabilityState.${aprs.status.directAprs.capabilityState}`,
                    )
                  }}
                </span>
              </dd>
            </div>
            <div v-if="aprs.status.directAprs">
              <dt>{{ t("aprs.directProfile") }}</dt>
              <dd>
                <span
                  class="status-badge"
                  :data-state="aprs.status.directAprs.profileState"
                >
                  {{
                    t(
                      `aprs.directProfileState.${aprs.status.directAprs.profileState}`,
                    )
                  }}
                </span>
              </dd>
            </div>
            <div v-if="aprs.status.directAprs">
              <dt>{{ t("aprs.directConnection") }}</dt>
              <dd>
                <span
                  class="status-badge"
                  :data-state="
                    aprs.status.directAprs.directAprsReady
                      ? 'available'
                      : 'connecting'
                  "
                >
                  {{
                    aprs.status.directAprs.directAprsReady
                      ? t("aprs.directReady")
                      : t("aprs.directWaiting")
                  }}
                </span>
              </dd>
            </div>
            <div v-if="aprs.status.directAprs">
              <dt>{{ t("aprs.directBeacon") }}</dt>
              <dd>
                <span
                  class="status-badge"
                  :data-state="aprs.status.directAprs.beaconState"
                >
                  {{
                    t(
                      `aprs.directBeaconState.${aprs.status.directAprs.beaconState}`,
                    )
                  }}
                </span>
              </dd>
            </div>
            <div v-else>
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
            <div v-if="!aprs.status.directAprs">
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
              <dt>{{ t("aprs.outboxUnconfirmed") }}</dt>
              <dd>{{ aprs.status.unconfirmedOutbox }}</dd>
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
              <dt>{{ t("aprs.stationUnconfirmed") }}</dt>
              <dd>{{ aprs.status.unconfirmedStationSubmissions }}</dd>
            </div>
            <div
              v-if="
                !aprs.status.directAprs && aprs.status.monitorLastActivityAt
              "
            >
              <dt>{{ t("aprs.monitorActivityAt") }}</dt>
              <dd>
                <time>{{ aprs.status.monitorLastActivityAt }}</time>
              </dd>
            </div>
            <div v-if="!aprs.status.directAprs">
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
          <ProblemNotice
            v-if="aprs.status.lastErrorCode"
            :code="aprs.status.lastErrorCode"
            compact
          />
          <ProblemNotice
            v-if="aprs.runtimeErrorCode"
            :code="aprs.runtimeErrorCode"
            compact
          />
        </template>
      </div>

      <div class="status-panel">
        <div class="panel-heading">
          <div>
            <p class="section-placeholder__eyebrow">
              {{ t("navigation.aprs") }}
            </p>
            <h2>{{ t("aprs.outbox") }}</h2>
          </div>
        </div>
        <p v-if="aprs.loading && !aprs.entries.length" class="status-message">
          {{ t("common.loading") }}
        </p>
        <ProblemNotice
          v-else-if="aprs.outboxErrorCode && !aprs.entries.length"
          :code="aprs.outboxErrorCode"
          show-retry
          @retry="aprs.refresh"
        />
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
              <ProblemNotice
                v-if="entry.lastErrorCode"
                :code="entry.lastErrorCode"
                compact
              />
            </div>
          </article>
        </div>
        <ProblemNotice
          v-if="aprs.outboxErrorCode && aprs.entries.length"
          :code="aprs.outboxErrorCode"
          compact
        />
      </div>

      <div class="status-panel">
        <div class="panel-heading">
          <div>
            <p class="section-placeholder__eyebrow">
              {{ t("navigation.aprs") }}
            </p>
            <h2>{{ t("aprs.stationDelivery") }}</h2>
          </div>
        </div>
        <p
          v-if="aprs.loading && !aprs.stationSubmissions.length"
          class="status-message"
        >
          {{ t("common.loading") }}
        </p>
        <ProblemNotice
          v-else-if="aprs.stationErrorCode && !aprs.stationSubmissions.length"
          :code="aprs.stationErrorCode"
          show-retry
          @retry="aprs.refresh"
        />
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
              <span
                class="status-badge"
                :data-state="submission.deliveryStatus"
              >
                {{
                  t(`aprs.stationDeliveryStatus.${submission.deliveryStatus}`)
                }}
              </span>
            </div>
            <div>
              <template v-if="submission.observerConfirmedAt">
                <span>{{ t("aprs.observerConfirmedAt") }}</span>
                <time>{{ submission.observerConfirmedAt }}</time>
              </template>
              <template v-else-if="submission.localWriteCompletedAt">
                <span>{{ t("aprs.lastLocalWriteAt") }}</span>
                <time>{{ submission.localWriteCompletedAt }}</time>
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
        <ProblemNotice
          v-if="aprs.stationErrorCode && aprs.stationSubmissions.length"
          :code="aprs.stationErrorCode"
          compact
        />
      </div>
    </template>
  </section>
</template>
