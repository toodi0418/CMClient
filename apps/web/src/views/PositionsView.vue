<script setup lang="ts">
import { computed, onMounted } from "vue";
import { MapPinned, RefreshCw } from "@lucide/vue";
import Button from "primevue/button";
import { useI18n } from "vue-i18n";

import { useDomainStore } from "@/stores/domain";

const domain = useDomainStore();
const { t } = useI18n();

const plottedPositions = computed(() =>
  domain.positions.filter(
    (event) =>
      event.position.latitudeI !== undefined &&
      event.position.longitudeI !== undefined,
  ),
);

function latitude(event: (typeof domain.positions)[number]) {
  return (event.position.latitudeI! / 10_000_000).toFixed(5);
}

function longitude(event: (typeof domain.positions)[number]) {
  return (event.position.longitudeI! / 10_000_000).toFixed(5);
}

function markerStyle(event: (typeof domain.positions)[number]) {
  return {
    left: `${((event.position.longitudeI! / 10_000_000 + 180) / 360) * 100}%`,
    top: `${((90 - event.position.latitudeI! / 10_000_000) / 180) * 100}%`,
  };
}

onMounted(() => void domain.refresh());
</script>

<template>
  <section class="page-grid" :aria-label="t('domain.positions')">
    <div class="status-panel">
      <div class="panel-heading">
        <div>
          <p class="section-placeholder__eyebrow">
            {{ t("navigation.positions") }}
          </p>
          <h2>{{ t("domain.positions") }}</h2>
        </div>
        <Button
          unstyled
          class="page-action"
          type="button"
          :aria-label="t('common.refresh')"
          :title="t('common.refresh')"
          :disabled="domain.loading"
          @click="domain.refresh"
          ><RefreshCw :size="17" aria-hidden="true"
        /></Button>
      </div>
      <p v-if="domain.loading" class="status-message">
        {{ t("common.loading") }}
      </p>
      <p v-else-if="domain.errorCode" class="status-message">
        {{ t("common.unavailable") }}
        <code class="stable-code">{{ domain.errorCode }}</code>
      </p>
      <p v-else-if="!plottedPositions.length" class="status-message">
        {{ t("domain.empty") }}
      </p>
      <div v-else class="position-map" :aria-label="t('domain.coordinates')">
        <MapPinned class="position-map__symbol" :size="24" aria-hidden="true" />
        <span
          v-for="event in plottedPositions"
          :key="event.id"
          class="position-map__point"
          :style="markerStyle(event)"
          :title="`${event.meshNetworkId} #${event.nodeNum}`"
        />
      </div>
    </div>
    <div v-if="plottedPositions.length" class="status-panel record-list">
      <article
        v-for="event in plottedPositions"
        :key="event.id"
        class="record-row"
      >
        <div>
          <strong>{{ event.meshNetworkId }} #{{ event.nodeNum }}</strong
          ><span>{{ t("domain.coordinates") }}</span>
        </div>
        <code>{{ latitude(event) }}, {{ longitude(event) }}</code
        ><time>{{ event.eventTime ?? event.createdAt }}</time>
      </article>
    </div>
  </section>
</template>
