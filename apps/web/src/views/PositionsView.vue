<script setup lang="ts">
import {
  computed,
  nextTick,
  onBeforeUnmount,
  onMounted,
  ref,
  watch,
} from "vue";
import { LocateFixed, RefreshCw } from "@lucide/vue";
import type { Coords, LayerGroup, Map as LeafletMap } from "leaflet";
import "leaflet/dist/leaflet.css";
import Button from "primevue/button";
import { useI18n } from "vue-i18n";

import { useDomainStore } from "@/stores/domain";

const domain = useDomainStore();
const { t } = useI18n();
const mapElement = ref<HTMLElement>();
const plottedPositions = computed(() =>
  domain.positions.filter(
    (event) =>
      event.position.latitudeI !== undefined &&
      event.position.longitudeI !== undefined,
  ),
);

let leaflet: typeof import("leaflet") | undefined;
let map: LeafletMap | undefined;
let markers: LayerGroup | undefined;
let resizeObserver: ResizeObserver | undefined;
let resizeFrame: number | undefined;
let fittedInitialPositions = false;

function latitude(event: (typeof domain.positions)[number]) {
  return (event.position.latitudeI! / 10_000_000).toFixed(5);
}

function longitude(event: (typeof domain.positions)[number]) {
  return (event.position.longitudeI! / 10_000_000).toFixed(5);
}

async function refreshPositions() {
  await domain.refresh();
  await nextTick();
  await initializeMap();
  renderMarkers();
}

async function initializeMap() {
  if (map || !mapElement.value) {
    return;
  }
  leaflet = await import("leaflet");
  const L = leaflet;
  map = L.map(mapElement.value, {
    center: [23.7, 121],
    zoom: 6,
    minZoom: 2,
    maxZoom: 18,
    worldCopyJump: true,
    attributionControl: false,
    fadeAnimation: false,
    markerZoomAnimation: false,
    zoomAnimation: false,
  });
  const OfflineGrid = L.GridLayer.extend({
    createTile(coords: Coords) {
      const tile = document.createElement("div");
      const tileSize = 256;
      const northWest = L.CRS.EPSG3857.pointToLatLng(
        L.point(coords.x * tileSize, coords.y * tileSize),
        coords.z,
      );
      const southEast = L.CRS.EPSG3857.pointToLatLng(
        L.point((coords.x + 1) * tileSize, (coords.y + 1) * tileSize),
        coords.z,
      );
      tile.className = "offline-map-tile";
      if (northWest.lat >= 0 && southEast.lat < 0) {
        tile.classList.add("offline-map-tile--equator");
      }
      if (northWest.lng <= 0 && southEast.lng > 0) {
        tile.classList.add("offline-map-tile--meridian");
      }
      const label = document.createElement("span");
      label.textContent = `${northWest.lat.toFixed(1)}°, ${northWest.lng.toFixed(1)}°`;
      tile.append(label);
      return tile;
    },
  });
  const offlineGrid = new OfflineGrid();
  L.setOptions(offlineGrid, { noWrap: true, tileSize: 256 });
  offlineGrid.addTo(map);
  L.control.scale({ imperial: false, position: "bottomleft" }).addTo(map);
  markers = L.layerGroup().addTo(map);
  resizeObserver = new ResizeObserver(() => {
    if (resizeFrame !== undefined) {
      return;
    }
    resizeFrame = window.requestAnimationFrame(() => {
      resizeFrame = undefined;
      map?.invalidateSize(false);
    });
  });
  resizeObserver.observe(mapElement.value);
}

function renderMarkers() {
  if (!leaflet || !map || !markers) {
    return;
  }
  markers.clearLayers();
  const coordinates: Array<[number, number]> = [];
  const colors = ["#2f8f61", "#d28a34", "#3e7bb6", "#b45468", "#66823c"];
  for (const event of plottedPositions.value) {
    const point: [number, number] = [
      event.position.latitudeI! / 10_000_000,
      event.position.longitudeI! / 10_000_000,
    ];
    coordinates.push(point);
    const color = colors[event.nodeNum % colors.length]!;
    const marker = leaflet.circleMarker(point, {
      radius: 7,
      weight: 2,
      color: "#f2f3ee",
      fillColor: color,
      fillOpacity: 0.94,
    });
    const label = document.createElement("div");
    const identity = document.createElement("strong");
    identity.textContent = `${event.meshNetworkId} #${event.nodeNum}`;
    const coordinate = document.createElement("span");
    coordinate.textContent = `${latitude(event)}, ${longitude(event)}`;
    label.append(identity, coordinate);
    marker.bindTooltip(label, { direction: "top", offset: [0, -6] });
    markers.addLayer(marker);
  }
  if (!fittedInitialPositions && coordinates.length > 0) {
    fitAllPositions();
    fittedInitialPositions = true;
  }
}

function fitAllPositions() {
  if (!leaflet || !map || plottedPositions.value.length === 0) {
    return;
  }
  const coordinates = plottedPositions.value.map((event): [number, number] => [
    event.position.latitudeI! / 10_000_000,
    event.position.longitudeI! / 10_000_000,
  ]);
  if (coordinates.length === 1) {
    map.setView(coordinates[0]!, 13);
    return;
  }
  map.fitBounds(leaflet.latLngBounds(coordinates), {
    padding: [28, 28],
    maxZoom: 13,
  });
}

watch(plottedPositions, () => renderMarkers(), { deep: true });

onMounted(() => void refreshPositions());
onBeforeUnmount(() => {
  resizeObserver?.disconnect();
  if (resizeFrame !== undefined) {
    window.cancelAnimationFrame(resizeFrame);
    resizeFrame = undefined;
  }
  map?.stop();
  map?.remove();
  map = undefined;
  markers = undefined;
});
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
        <div class="panel-actions">
          <Button
            unstyled
            class="page-action"
            type="button"
            :aria-label="t('domain.fitPositions')"
            :title="t('domain.fitPositions')"
            :disabled="!plottedPositions.length"
            @click="fitAllPositions"
          >
            <LocateFixed :size="17" aria-hidden="true" />
          </Button>
          <Button
            unstyled
            class="page-action"
            type="button"
            :aria-label="t('common.refresh')"
            :title="t('common.refresh')"
            :disabled="domain.loading"
            @click="refreshPositions"
          >
            <RefreshCw :size="17" aria-hidden="true" />
          </Button>
        </div>
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
      <div
        v-show="!domain.loading && !domain.errorCode && plottedPositions.length"
        ref="mapElement"
        class="position-map"
        role="application"
        :aria-label="t('domain.coordinates')"
      />
    </div>
    <div v-if="plottedPositions.length" class="status-panel record-list">
      <article
        v-for="event in plottedPositions"
        :key="event.id"
        class="record-row"
      >
        <div>
          <strong>{{ event.meshNetworkId }} #{{ event.nodeNum }}</strong>
          <span>{{ t("domain.coordinates") }}</span>
        </div>
        <code>{{ latitude(event) }}, {{ longitude(event) }}</code>
        <time>{{ event.eventTime ?? event.createdAt }}</time>
      </article>
    </div>
  </section>
</template>
