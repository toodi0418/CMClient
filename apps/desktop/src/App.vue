<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { Minus, PanelTopClose, X } from "@lucide/vue";
import { invoke, isTauri } from "@tauri-apps/api/core";
import { getCurrentWindow } from "@tauri-apps/api/window";

import { runWindowControl, type WindowControlAction } from "./window-controls";

type GatewayStatus =
  "stopped" | "starting" | "running" | "backoff" | "degraded";
interface ControlStatus {
  schema_version: number;
  agent: string;
  gateway: GatewayStatus;
}

const status = ref<ControlStatus>();
const errorCode = ref<string>();
const busy = ref(false);
const appWindow = isTauri() ? getCurrentWindow() : undefined;
const gatewayLabel = computed(() => status.value?.gateway ?? "stopped");

async function controlWindow(action: WindowControlAction): Promise<void> {
  if (!appWindow) {
    errorCode.value = "DESKTOP_WINDOW_CONTROL_UNAVAILABLE";
    return;
  }

  try {
    await runWindowControl(appWindow, action);
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

async function command(command: "start" | "stop" | "restart"): Promise<void> {
  busy.value = true;
  try {
    status.value = await invoke<ControlStatus>("agent_command", { command });
    errorCode.value = undefined;
  } catch {
    errorCode.value = "DESKTOP_AGENT_COMMAND_FAILED";
  } finally {
    busy.value = false;
  }
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
      <p class="eyebrow">LOCAL AGENT</p>
      <h1>CMClient</h1>
      <dl>
        <div>
          <dt>Agent</dt>
          <dd>{{ status?.agent ?? "checking" }}</dd>
        </div>
        <div>
          <dt>Gateway</dt>
          <dd :data-state="gatewayLabel">{{ gatewayLabel }}</dd>
        </div>
      </dl>
      <code v-if="errorCode">{{ errorCode }}</code>
      <div class="commands">
        <button :disabled="busy" @click="refresh">Refresh</button>
        <button :disabled="busy" @click="command('start')">Start</button>
        <button :disabled="busy" @click="command('restart')">Restart</button>
        <button :disabled="busy" @click="command('stop')">Stop</button>
      </div>
    </section>
  </main>
</template>
