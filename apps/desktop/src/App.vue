<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from "vue";
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
import { listen, type UnlistenFn } from "@tauri-apps/api/event";
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
interface UpdateControlJob {
  id: string;
  phase: string;
  updatedAt: string;
  errorCode: string | null;
  bytesDownloaded: number | null;
  bytesTotal: number | null;
  bytesPerSecond: number | null;
  recentLogCodes: string[];
}
interface UpdateControlStatus {
  schemaVersion: number;
  job: UpdateControlJob | null;
}

const status = ref<ControlStatus>();
const errorCode = ref<string>();
const busy = ref(false);
const updateStatus = ref<UpdateControlStatus>();
const updateErrorCode = ref<string>();
const updateBusy = ref(false);
let unlistenUpdateEvents: UnlistenFn | undefined;
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
const updateJob = computed(() => updateStatus.value?.job);
const updatePhase = computed(() => updateJob.value?.phase ?? "idle");
const updatePhaseLabel = computed(() =>
  updatePhase.value
    .split("_")
    .map((value) => value.charAt(0).toUpperCase() + value.slice(1))
    .join(" "),
);
const updateTransferLabel = computed(() => {
  if (!updateJob.value || updateJob.value.bytesDownloaded === null) {
    return "--";
  }
  return [
    formatBytes(updateJob.value.bytesDownloaded),
    updateJob.value.bytesTotal === null
      ? "--"
      : formatBytes(updateJob.value.bytesTotal),
  ].join(" / ");
});
const updateSpeedLabel = computed(() =>
  updateJob.value?.bytesPerSecond === null ||
  updateJob.value?.bytesPerSecond === undefined
    ? "--"
    : formatBytes(updateJob.value.bytesPerSecond) + "/s",
);

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

async function refreshUpdate(): Promise<void> {
  updateBusy.value = true;
  try {
    updateStatus.value = await invoke<UpdateControlStatus>(
      "agent_update_status",
    );
    updateErrorCode.value = undefined;
  } catch {
    updateErrorCode.value = "DESKTOP_UPDATE_STATUS_UNAVAILABLE";
  } finally {
    updateBusy.value = false;
  }
}

async function subscribeToUpdateEvents(): Promise<void> {
  try {
    unlistenUpdateEvents = await listen<string>(
      "agent-update-status",
      (event) => {
        try {
          const update = JSON.parse(event.payload) as UpdateControlStatus;
          if (
            update.schemaVersion !== 1 ||
            !Object.hasOwn(update, "job") ||
            (update.job !== null && typeof update.job !== "object")
          ) {
            throw new Error("invalid update status");
          }
          updateStatus.value = update;
          updateErrorCode.value = undefined;
        } catch {
          updateErrorCode.value = "DESKTOP_UPDATE_EVENT_INVALID";
        }
      },
    );
  } catch {
    updateErrorCode.value = "DESKTOP_UPDATE_EVENT_UNAVAILABLE";
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

function formatBytes(bytes: number): string {
  const units = ["B", "KiB", "MiB", "GiB"];
  const exponent = Math.min(
    Math.floor(Math.log(Math.max(bytes, 1)) / Math.log(1024)),
    units.length - 1,
  );
  const value = bytes / 1024 ** exponent;
  return (
    value.toLocaleString(undefined, {
      maximumFractionDigits: exponent === 0 ? 0 : 1,
    }) +
    " " +
    units[exponent]
  );
}

onMounted(() => {
  void refresh();
  void refreshUpdate();
  if (isTauri()) {
    void subscribeToUpdateEvents();
  }
});
onUnmounted(() => unlistenUpdateEvents?.());
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
      <section class="update-status" aria-label="Update Agent status">
        <div class="update-status__heading">
          <span>Update Agent</span>
          <button
            class="update-status__refresh"
            type="button"
            aria-label="Refresh update status"
            title="Refresh update status"
            :disabled="updateBusy"
            @click="refreshUpdate"
          >
            <RefreshCw :size="15" aria-hidden="true" />
          </button>
        </div>
        <div class="update-status__metrics">
          <div>
            <span>Phase</span>
            <strong :data-state="updatePhase">{{ updatePhaseLabel }}</strong>
          </div>
          <div>
            <span>Transfer</span>
            <strong>{{ updateTransferLabel }}</strong>
          </div>
          <div>
            <span>Speed</span>
            <strong>{{ updateSpeedLabel }}</strong>
          </div>
        </div>
        <div v-if="updateJob" class="update-status__job">
          <code>{{ updateJob.id }}</code>
          <code v-if="updateJob.errorCode">{{ updateJob.errorCode }}</code>
        </div>
        <ul v-if="updateJob?.recentLogCodes.length" class="update-status__log">
          <li v-for="code in updateJob.recentLogCodes" :key="code">
            <code>{{ code }}</code>
          </li>
        </ul>
        <code v-if="updateErrorCode" class="update-status__error">{{
          updateErrorCode
        }}</code>
      </section>
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
