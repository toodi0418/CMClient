<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { Languages, Monitor, Moon, RotateCcw, Sun } from "@lucide/vue";
import Button from "primevue/button";
import Dialog from "primevue/dialog";
import InputText from "primevue/inputtext";
import { useI18n } from "vue-i18n";

import CapabilityStatus from "@/components/CapabilityStatus.vue";
import ProblemNotice from "@/components/ProblemNotice.vue";
import { isSupportedLocale, type ThemePreference } from "@/preferences";
import { useGatewayStore } from "@/stores/gateway";
import { usePreferencesStore } from "@/stores/preferences";
import { useSetupStore } from "@/stores/setup";

const gateway = useGatewayStore();
const preferences = usePreferencesStore();
const setup = useSetupStore();
const { t } = useI18n();
const operationalResetDialogVisible = ref(false);
const operationalResetConfirmation = ref("");
const canOperationalReset = computed(
  () => setup.status?.ready === true && !setup.loading,
);

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

const runtimeCapabilities = computed(() =>
  ["managementWeb", "nativeUpdate"]
    .map((key) => ({
      key,
      capability:
        gateway.capabilities?.capabilities[
          key as "managementWeb" | "nativeUpdate"
        ],
    }))
    .filter(
      (
        row,
      ): row is {
        key: "managementWeb" | "nativeUpdate";
        capability: NonNullable<typeof row.capability>;
      } => Boolean(row.capability),
    )
    .map((row) => ({
      ...row,
      reasonCode:
        "reasonCode" in row.capability ? row.capability.reasonCode : undefined,
    })),
);

onMounted(() => void gateway.refresh());

function setLocale(event: Event) {
  const locale = (event.target as HTMLSelectElement).value;
  if (isSupportedLocale(locale)) {
    preferences.setLocale(locale);
  }
}

function openOperationalResetDialog() {
  operationalResetConfirmation.value = "";
  operationalResetDialogVisible.value = true;
}

async function confirmOperationalReset() {
  if (operationalResetConfirmation.value !== "RESET") {
    return;
  }
  try {
    await setup.operationalReset();
    operationalResetDialogVisible.value = false;
    operationalResetConfirmation.value = "";
  } catch {
    // The setup store retains the stable Agent error code for the next view.
  }
}
</script>

<template>
  <section class="page-grid" :aria-label="t('navigation.settings')">
    <div class="status-panel">
      <div class="panel-heading">
        <div>
          <p class="section-placeholder__eyebrow">
            {{ t("navigation.settings") }}
          </p>
          <h2>{{ t("settings.display") }}</h2>
        </div>
      </div>
      <div class="settings-controls">
        <div class="settings-control">
          <span>{{ t("preferences.theme") }}</span>
          <div
            class="theme-selector"
            role="group"
            :aria-label="t('preferences.theme')"
          >
            <Button
              v-for="option in themeOptions"
              :key="option.value"
              unstyled
              class="theme-selector__button theme-selector__button--labelled"
              :class="{ 'is-selected': preferences.theme === option.value }"
              type="button"
              :aria-pressed="preferences.theme === option.value"
              @click="preferences.setTheme(option.value)"
            >
              <component :is="option.icon" :size="16" aria-hidden="true" />
              <span>{{ t(option.labelKey) }}</span>
            </Button>
          </div>
        </div>
        <label class="settings-control">
          <span>{{ t("preferences.language") }}</span>
          <span class="locale-selector">
            <Languages :size="16" aria-hidden="true" />
            <select :value="preferences.locale" @change="setLocale">
              <option value="zh-TW">{{ t("preferences.zhTW") }}</option>
              <option value="en-US">{{ t("preferences.enUS") }}</option>
            </select>
          </span>
        </label>
      </div>
    </div>

    <div class="status-panel">
      <div class="panel-heading">
        <div>
          <p class="section-placeholder__eyebrow">
            {{ t("settings.runtime") }}
          </p>
          <h2>{{ t("settings.capabilities") }}</h2>
        </div>
      </div>
      <ProblemNotice
        v-if="gateway.errorCode"
        :code="gateway.errorCode"
        show-retry
        @retry="gateway.refresh"
      />
      <div v-else class="capability-list">
        <div
          v-for="row in runtimeCapabilities"
          :key="row.key"
          class="capability-row"
        >
          <span>{{ t(`capability.${row.key}`) }}</span>
          <CapabilityStatus
            :available="row.capability.available"
            :reason-code="row.reasonCode"
          />
        </div>
      </div>
    </div>

    <div v-if="setup.status?.ready" class="status-panel">
      <div class="panel-heading">
        <div>
          <p class="section-placeholder__eyebrow">
            {{ t("settings.operationalReset.title") }}
          </p>
          <h2>{{ t("settings.operationalReset.title") }}</h2>
        </div>
      </div>
      <Button
        severity="danger"
        type="button"
        :disabled="!canOperationalReset"
        @click="openOperationalResetDialog"
      >
        <RotateCcw :size="16" aria-hidden="true" />
        <span>{{ t("settings.operationalReset.action") }}</span>
      </Button>
    </div>

    <Dialog
      v-model:visible="operationalResetDialogVisible"
      modal
      :closable="!setup.loading"
      :header="t('settings.operationalReset.title')"
    >
      <p>{{ t("settings.operationalReset.description") }}</p>
      <label class="settings-control">
        <span>{{ t("settings.operationalReset.confirmationLabel") }}</span>
        <InputText
          v-model="operationalResetConfirmation"
          autocomplete="off"
          :disabled="setup.loading"
        />
      </label>
      <template #footer>
        <Button
          severity="secondary"
          type="button"
          :disabled="setup.loading"
          @click="operationalResetDialogVisible = false"
        >
          {{ t("settings.operationalReset.cancel") }}
        </Button>
        <Button
          severity="danger"
          type="button"
          :disabled="operationalResetConfirmation !== 'RESET' || setup.loading"
          @click="confirmOperationalReset"
        >
          <RotateCcw :size="16" aria-hidden="true" />
          <span>{{ t("settings.operationalReset.confirm") }}</span>
        </Button>
      </template>
    </Dialog>
  </section>
</template>
