<script setup lang="ts">
import { computed } from "vue";
import { RouterLink, RouterView, useRoute } from "vue-router";

import { useShellStore } from "@/stores/shell";

const shell = useShellStore();
const route = useRoute();

const primaryNavigation = [
  { label: "Overview", to: "/", mark: "OV" },
  { label: "System", to: "/system", mark: "SY" },
  { label: "Meshtastic", to: "/meshtastic", mark: "ME" },
  { label: "Nodes", to: "/nodes", mark: "ND" },
  { label: "Positions", to: "/positions", mark: "PO" },
  { label: "Messages", to: "/messages", mark: "MS" },
  { label: "Telemetry", to: "/telemetry", mark: "TE" },
  { label: "APRS", to: "/aprs", mark: "AP" },
];

const supportNavigation = [
  { label: "Logs", to: "/logs", mark: "LG" },
  { label: "Settings", to: "/settings", mark: "ST" },
  { label: "Diagnostics", to: "/diagnostics", mark: "DG" },
];

const pageLabel = computed(
  () => (route.meta.label as string | undefined) ?? "CMClient",
);
</script>

<template>
  <div
    class="console-shell"
    :class="{
      'is-rail-collapsed': shell.desktopRailCollapsed,
      'is-mobile-nav-open': shell.mobileNavigationOpen,
    }"
  >
    <header class="topbar">
      <button
        class="brand-mark"
        type="button"
        aria-label="切換導覽列"
        @click="shell.toggleDesktopRail"
      >
        <span>CM</span>
      </button>
      <div class="product-name">
        <span>CMCLIENT</span>
        <small>CONTROL PLANE</small>
      </div>
      <div class="topbar-spacer" />
      <div class="gateway-indicator" :data-state="shell.gatewayAvailability">
        <span class="status-pip" aria-hidden="true" />
        <span>{{ shell.gatewayAvailability }}</span>
      </div>
      <button
        class="mobile-menu"
        type="button"
        aria-label="開啟導覽"
        @click="shell.toggleMobileNavigation"
      >
        <span class="mobile-menu__bars" aria-hidden="true" />
      </button>
    </header>

    <aside class="side-rail" aria-label="主要導覽">
      <nav class="navigation-list">
        <RouterLink
          v-for="item in primaryNavigation"
          :key="item.to"
          :to="item.to"
          class="navigation-link"
          @click="shell.closeMobileNavigation"
        >
          <span class="navigation-mark" aria-hidden="true">{{
            item.mark
          }}</span>
          <span class="navigation-label">{{ item.label }}</span>
        </RouterLink>
      </nav>
      <div class="rail-divider" />
      <nav
        class="navigation-list navigation-list--support"
        aria-label="支援導覽"
      >
        <RouterLink
          v-for="item in supportNavigation"
          :key="item.to"
          :to="item.to"
          class="navigation-link"
          @click="shell.closeMobileNavigation"
        >
          <span class="navigation-mark" aria-hidden="true">{{
            item.mark
          }}</span>
          <span class="navigation-label">{{ item.label }}</span>
        </RouterLink>
      </nav>
      <div class="rail-footer">LOCAL / V2.0</div>
    </aside>

    <main class="workspace" @click="shell.closeMobileNavigation">
      <div class="workspace-heading">
        <p>OPERATIONS</p>
        <h1>{{ pageLabel }}</h1>
      </div>
      <RouterView />
    </main>
  </div>
</template>
