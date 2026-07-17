<script setup lang="ts">
import { computed } from "vue";
import {
  Gauge,
  LayoutDashboard,
  MapPinned,
  Menu,
  MessageSquareText,
  Network,
  PanelLeftClose,
  PanelLeftOpen,
  Radio,
  Satellite,
  ScrollText,
  Server,
  Settings,
  Stethoscope,
} from "@lucide/vue";
import Button from "primevue/button";
import { RouterLink, RouterView, useRoute } from "vue-router";

import { useShellStore } from "@/stores/shell";

const shell = useShellStore();
const route = useRoute();

const primaryNavigation = [
  { label: "Overview", to: "/", icon: LayoutDashboard },
  { label: "System", to: "/system", icon: Server },
  { label: "Meshtastic", to: "/meshtastic", icon: Radio },
  { label: "Nodes", to: "/nodes", icon: Network },
  { label: "Positions", to: "/positions", icon: MapPinned },
  { label: "Messages", to: "/messages", icon: MessageSquareText },
  { label: "Telemetry", to: "/telemetry", icon: Gauge },
  { label: "APRS", to: "/aprs", icon: Satellite },
];

const supportNavigation = [
  { label: "Logs", to: "/logs", icon: ScrollText },
  { label: "Settings", to: "/settings", icon: Settings },
  { label: "Diagnostics", to: "/diagnostics", icon: Stethoscope },
];

const pageLabel = computed(
  () => (route.meta.label as string | undefined) ?? "CMClient",
);

const railToggleIcon = computed(() =>
  shell.desktopRailCollapsed ? PanelLeftOpen : PanelLeftClose,
);
</script>

<template>
  <div
    class="console-shell min-h-screen font-sans"
    :class="{
      'is-rail-collapsed': shell.desktopRailCollapsed,
      'is-mobile-nav-open': shell.mobileNavigationOpen,
    }"
  >
    <header class="topbar">
      <Button
        unstyled
        class="brand-mark"
        type="button"
        aria-label="切換導覽列"
        title="切換導覽列"
        @click="shell.toggleDesktopRail"
      >
        <component :is="railToggleIcon" :size="18" aria-hidden="true" />
      </Button>
      <div class="product-name">
        <span>CMCLIENT</span>
        <small>CONTROL PLANE</small>
      </div>
      <div class="topbar-spacer" />
      <div class="gateway-indicator" :data-state="shell.gatewayAvailability">
        <span class="status-pip" aria-hidden="true" />
        <span>{{ shell.gatewayAvailability }}</span>
      </div>
      <Button
        unstyled
        class="mobile-menu"
        type="button"
        aria-label="開啟導覽"
        title="開啟導覽"
        @click="shell.toggleMobileNavigation"
      >
        <Menu :size="19" aria-hidden="true" />
      </Button>
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
          <component
            :is="item.icon"
            class="navigation-icon"
            :size="17"
            aria-hidden="true"
          />
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
          <component
            :is="item.icon"
            class="navigation-icon"
            :size="17"
            aria-hidden="true"
          />
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
