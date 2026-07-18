<script setup lang="ts">
import { computed, watch } from "vue";
import {
  Gauge,
  Cloud,
  LayoutDashboard,
  Languages,
  MapPinned,
  Menu,
  MessageSquareText,
  Monitor,
  Moon,
  Network,
  PanelLeftClose,
  PanelLeftOpen,
  Radio,
  RefreshCw,
  Satellite,
  ScrollText,
  Server,
  Settings,
  Stethoscope,
  Sun,
  Waypoints,
} from "@lucide/vue";
import Button from "primevue/button";
import { RouterLink, RouterView, useRoute } from "vue-router";
import { useI18n } from "vue-i18n";

import { isSupportedLocale, type ThemePreference } from "@/preferences";
import { useGatewayStore } from "@/stores/gateway";
import {
  isManagementSessionError,
  useManagementAuthStore,
} from "@/stores/management-auth";
import { usePreferencesStore } from "@/stores/preferences";
import { useShellStore } from "@/stores/shell";
import ManagementLoginView from "@/views/ManagementLoginView.vue";

const shell = useShellStore();
const gateway = useGatewayStore();
const auth = useManagementAuthStore();
const preferences = usePreferencesStore();
const route = useRoute();
const { t } = useI18n();

const primaryNavigation = [
  { labelKey: "navigation.overview", to: "/", icon: LayoutDashboard },
  { labelKey: "navigation.system", to: "/system", icon: Server },
  { labelKey: "navigation.meshtastic", to: "/meshtastic", icon: Radio },
  { labelKey: "navigation.proxy", to: "/proxy", icon: Waypoints },
  { labelKey: "navigation.nodes", to: "/nodes", icon: Network },
  { labelKey: "navigation.positions", to: "/positions", icon: MapPinned },
  { labelKey: "navigation.messages", to: "/messages", icon: MessageSquareText },
  { labelKey: "navigation.telemetry", to: "/telemetry", icon: Gauge },
  { labelKey: "navigation.aprs", to: "/aprs", icon: Satellite },
  { labelKey: "navigation.callmesh", to: "/callmesh", icon: Cloud },
];

const supportNavigation = [
  { labelKey: "navigation.logs", to: "/logs", icon: ScrollText },
  { labelKey: "navigation.updates", to: "/updates", icon: RefreshCw },
  { labelKey: "navigation.settings", to: "/settings", icon: Settings },
  { labelKey: "navigation.diagnostics", to: "/diagnostics", icon: Stethoscope },
];

const themeOptions = [
  {
    value: "light" as ThemePreference,
    labelKey: "preferences.light",
    icon: Sun,
  },
  {
    value: "dark" as ThemePreference,
    labelKey: "preferences.dark",
    icon: Moon,
  },
  {
    value: "system" as ThemePreference,
    labelKey: "preferences.system",
    icon: Monitor,
  },
];

const localeOptions = [
  { value: "zh-TW", labelKey: "preferences.zhTW" },
  { value: "en-US", labelKey: "preferences.enUS" },
];

const pageLabel = computed(() =>
  t((route.meta.labelKey as string | undefined) ?? "navigation.overview"),
);

const gatewayLabel = computed(() => t(`gateway.${shell.gatewayAvailability}`));

const requiresLogin = computed(
  () => auth.required || isManagementSessionError(gateway.errorCode),
);

const railToggleIcon = computed(() =>
  shell.desktopRailCollapsed ? PanelLeftOpen : PanelLeftClose,
);

watch(
  () => gateway.availability,
  (availability) => shell.setGatewayAvailability(availability),
  { immediate: true },
);

watch(
  () => gateway.errorCode,
  (errorCode) => {
    if (isManagementSessionError(errorCode)) {
      auth.requireLogin();
    }
  },
  { immediate: true },
);

function setLocale(event: Event) {
  const locale = (event.target as HTMLSelectElement).value;

  if (isSupportedLocale(locale)) {
    preferences.setLocale(locale);
  }
}

async function refreshAfterLogin() {
  await gateway.refresh();
}
</script>

<template>
  <ManagementLoginView
    v-if="requiresLogin"
    @authenticated="refreshAfterLogin"
  />
  <div
    v-else
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
        :aria-label="t('shell.toggleNavigation')"
        :title="t('shell.toggleNavigation')"
        @click="shell.toggleDesktopRail"
      >
        <component :is="railToggleIcon" :size="18" aria-hidden="true" />
      </Button>
      <div class="product-name">
        <span>CMCLIENT</span>
        <small>{{ t("shell.controlPlane") }}</small>
      </div>
      <div class="topbar-spacer" />
      <div class="gateway-indicator" :data-state="shell.gatewayAvailability">
        <span class="status-pip" aria-hidden="true" />
        <span>{{ gatewayLabel }}</span>
      </div>
      <Button
        unstyled
        class="mobile-menu"
        type="button"
        :aria-label="t('shell.openNavigation')"
        :title="t('shell.openNavigation')"
        @click="shell.toggleMobileNavigation"
      >
        <Menu :size="19" aria-hidden="true" />
      </Button>
    </header>

    <aside class="side-rail" :aria-label="t('shell.primaryNavigation')">
      <nav class="navigation-list" :aria-label="t('shell.primaryNavigation')">
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
          <span class="navigation-label">{{ t(item.labelKey) }}</span>
        </RouterLink>
      </nav>
      <div class="rail-divider" />
      <nav
        class="navigation-list navigation-list--support"
        :aria-label="t('shell.supportNavigation')"
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
          <span class="navigation-label">{{ t(item.labelKey) }}</span>
        </RouterLink>
      </nav>
      <section
        class="preference-controls"
        :aria-label="t('preferences.display')"
      >
        <p class="preference-controls__label">{{ t("preferences.display") }}</p>
        <div
          class="theme-selector"
          role="group"
          :aria-label="t('preferences.theme')"
        >
          <Button
            v-for="option in themeOptions"
            :key="option.value"
            unstyled
            class="theme-selector__button"
            :class="{ 'is-selected': preferences.theme === option.value }"
            type="button"
            :aria-label="t(option.labelKey)"
            :aria-pressed="preferences.theme === option.value"
            :title="t(option.labelKey)"
            @click="preferences.setTheme(option.value)"
          >
            <component :is="option.icon" :size="16" aria-hidden="true" />
            <span class="visually-hidden">{{ t(option.labelKey) }}</span>
          </Button>
        </div>
        <label class="locale-selector">
          <Languages :size="16" aria-hidden="true" />
          <span class="visually-hidden">{{ t("preferences.language") }}</span>
          <select
            :value="preferences.locale"
            :aria-label="t('preferences.language')"
            @change="setLocale"
          >
            <option
              v-for="option in localeOptions"
              :key="option.value"
              :value="option.value"
            >
              {{ t(option.labelKey) }}
            </option>
          </select>
        </label>
      </section>
      <div class="rail-footer">LOCAL / V2.0</div>
    </aside>

    <main class="workspace" @click="shell.closeMobileNavigation">
      <div class="workspace-heading">
        <p>{{ t("shell.operations") }}</p>
        <h1>{{ pageLabel }}</h1>
      </div>
      <RouterView />
    </main>
  </div>
</template>
