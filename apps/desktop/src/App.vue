<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import {
  ExternalLink,
  Minus,
  PanelTopClose,
  Play,
  RefreshCw,
  Square,
  X,
} from "@lucide/vue";
import { invoke, isTauri } from "@tauri-apps/api/core";
import { getCurrentWindow } from "@tauri-apps/api/window";

import { runWindowControl, type WindowControlAction } from "./window-controls";

type GatewayStatus =
  "stopped" | "starting" | "running" | "backoff" | "degraded";
type ManagementWebStatus = "disabled" | "running";
type AgentCommand = "start" | "stop" | "restart" | "enable_web" | "disable_web";
interface ControlStatus {
  schema_version: number;
  agent: string;
  agent_version: string;
  gateway: GatewayStatus;
  management_web: ManagementWebStatus;
  management_web_url: string | null;
  uptime_seconds: number;
  latest_error_code: string | null;
}

const status = ref<ControlStatus>();
const errorCode = ref<string>();
const busy = ref(false);
const appWindow = isTauri() ? getCurrentWindow() : undefined;
const windowControlTarget = appWindow
  ? {
      exit: () => invoke<void>("exit_desktop"),
      minimize: () => appWindow.minimize(),
      hide: () => appWindow.hide(),
    }
  : undefined;
const gatewayLabel = computed(() => status.value?.gateway ?? "stopped");
const managementWebLabel = computed(
  () => status.value?.management_web ?? "disabled",
);
const coreLabel = computed(() =>
  status.value?.agent === "running" ? "running" : "stopped",
);
const activeErrorCode = computed(
  () => errorCode.value ?? status.value?.latest_error_code,
);
const uptimeLabel = computed(() => formatUptime(status.value?.uptime_seconds));

async function controlWindow(action: WindowControlAction): Promise<void> {
  if (!windowControlTarget) {
    errorCode.value = "DESKTOP_WINDOW_CONTROL_UNAVAILABLE";
    return;
  }

  try {
    await runWindowControl(windowControlTarget, action);
  } catch {
    errorCode.value = "DESKTOP_WINDOW_CONTROL_FAILED";
  }
}

async function refresh(): Promise<void> {
  busy.value = true;
  try {
    status.value = await invoke<ControlStatus>("agent_status");
    errorCode.value = undefined;
  } catch {
    errorCode.value = "DESKTOP_AGENT_UNAVAILABLE";
  } finally {
    busy.value = false;
  }
}

async function command(command: AgentCommand): Promise<void> {
  busy.value = true;
  try {
    status.value = await invoke<ControlStatus>("agent_command", { command });
    errorCode.value = undefined;
  } catch {
    errorCode.value =
      command === "enable_web" || command === "disable_web"
        ? "DESKTOP_MANAGEMENT_WEB_CONTROL_FAILED"
        : "DESKTOP_AGENT_COMMAND_FAILED";
  } finally {
    busy.value = false;
  }
}

async function toggleManagementWeb(): Promise<void> {
  await command(
    managementWebLabel.value === "running" ? "disable_web" : "enable_web",
  );
}

async function openManagementWeb(): Promise<void> {
  busy.value = true;
  try {
    await invoke("open_management_web");
    errorCode.value = undefined;
  } catch {
    errorCode.value = "DESKTOP_MANAGEMENT_WEB_OPEN_FAILED";
  } finally {
    busy.value = false;
  }
}

function formatUptime(seconds: number | undefined): string {
  if (seconds === undefined) {
    return "--";
  }
  const hours = Math.floor(seconds / 3_600);
  const minutes = Math.floor((seconds % 3_600) / 60);
  const remainingSeconds = seconds % 60;
  return [hours, minutes, remainingSeconds]
    .map((value) => String(value).padStart(2, "0"))
    .join(":");
}

onMounted(() => void refresh());
</script>

<template>
  <main class="desktop-shell">
    <header class="desktop-header" data-tauri-drag-region="deep">
      <div
        class="window-controls"
        aria-label="Window controls"
        data-tauri-drag-region="false"
      >
        <button
          class="window-control window-control--exit"
          type="button"
          aria-label="Exit CMClient"
          title="Exit CMClient"
          data-tauri-drag-region="false"
          @click="controlWindow('exit')"
        >
          <X class="window-control__icon" :size="8" :stroke-width="3" />
        </button>
        <button
          class="window-control window-control--minimize"
          type="button"
          aria-label="Minimize"
          title="Minimize"
          data-tauri-drag-region="false"
          @click="controlWindow('minimize')"
        >
          <Minus class="window-control__icon" :size="8" :stroke-width="3" />
        </button>
        <button
          class="window-control window-control--hide"
          type="button"
          aria-label="Hide to tray"
          title="Hide to tray"
          data-tauri-drag-region="false"
          @click="controlWindow('hide')"
        >
          <PanelTopClose
            class="window-control__icon"
            :size="8"
            :stroke-width="3"
          />
        </button>
      </div>
      <span class="desktop-header__title">CMCLIENT DESKTOP</span>
    </header>
    <section class="status-surface">
      <div class="status-heading">
        <div>
          <p class="eyebrow">LOCAL SUPERVISOR</p>
          <h1>CMClient</h1>
        </div>
        <span class="agent-version">{{ status?.agent_version ?? "--" }}</span>
      </div>
      <div class="status-lights" aria-label="Runtime status">
        <div class="status-light" :data-state="coreLabel">
          <span class="status-light__pip" aria-hidden="true" />
          <span>Core</span>
          <strong>{{ coreLabel }}</strong>
        </div>
        <div class="status-light" :data-state="managementWebLabel">
          <span class="status-light__pip" aria-hidden="true" />
          <span>Web</span>
          <strong>{{ managementWebLabel }}</strong>
        </div>
        <div class="status-light" :data-state="gatewayLabel">
          <span class="status-light__pip" aria-hidden="true" />
          <span>Gateway</span>
          <strong>{{ gatewayLabel }}</strong>
        </div>
      </div>
      <dl>
        <div>
          <dt>Agent version</dt>
          <dd>{{ status?.agent_version ?? "checking" }}</dd>
        </div>
        <div>
          <dt>Uptime</dt>
          <dd>{{ uptimeLabel }}</dd>
        </div>
      </dl>
      <code v-if="activeErrorCode">{{ activeErrorCode }}</code>
      <div class="management-controls">
        <div class="web-toggle-row">
          <span>Management Web</span>
          <button
            class="web-switch"
            type="button"
            role="switch"
            :aria-checked="managementWebLabel === 'running'"
            :aria-label="
              managementWebLabel === 'running'
                ? 'Disable Management Web'
                : 'Enable Management Web'
            "
            :title="
              managementWebLabel === 'running'
                ? 'Disable Management Web'
                : 'Enable Management Web'
            "
            :disabled="busy"
            @click="toggleManagementWeb"
          >
            <span class="web-switch__thumb" />
          </button>
        </div>
        <div class="command-toolbar" aria-label="Runtime commands">
          <button
            type="button"
            aria-label="Open Management Web"
            title="Open Management Web"
            :disabled="busy || managementWebLabel !== 'running'"
            @click="openManagementWeb"
          >
            <ExternalLink :size="17" aria-hidden="true" />
          </button>
          <button
            type="button"
            aria-label="Restart Gateway"
            title="Restart Gateway"
            :disabled="busy"
            @click="command('restart')"
          >
            <RefreshCw :size="17" aria-hidden="true" />
          </button>
          <button
            type="button"
            aria-label="Refresh status"
            title="Refresh status"
            :disabled="busy"
            @click="refresh"
          >
            <RefreshCw :size="17" aria-hidden="true" />
          </button>
          <button
            type="button"
            aria-label="Start Gateway"
            title="Start Gateway"
            :disabled="busy"
            @click="command('start')"
          >
            <Play :size="17" aria-hidden="true" />
          </button>
          <button
            type="button"
            aria-label="Stop Gateway"
            title="Stop Gateway"
            :disabled="busy"
            @click="command('stop')"
          >
            <Square :size="15" aria-hidden="true" />
          </button>
        </div>
      </div>
    </section>
  </main>
</template>
