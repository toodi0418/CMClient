<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { invoke } from "@tauri-apps/api/core";

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
const gatewayLabel = computed(() => status.value?.gateway ?? "stopped");

async function refresh(): Promise<void> {
  busy.value = true;
  try {
    status.value = await invoke<ControlStatus>("agent_status");
    errorCode.value = undefined;
  } catch (error) {
    errorCode.value =
      error instanceof Error ? error.message : "DESKTOP_AGENT_UNAVAILABLE";
  } finally {
    busy.value = false;
  }
}

async function command(command: "start" | "stop" | "restart"): Promise<void> {
  busy.value = true;
  try {
    status.value = await invoke<ControlStatus>("agent_command", { command });
    errorCode.value = undefined;
  } catch (error) {
    errorCode.value =
      error instanceof Error ? error.message : "DESKTOP_AGENT_COMMAND_FAILED";
  } finally {
    busy.value = false;
  }
}

onMounted(() => void refresh());
</script>

<template>
  <main class="desktop-shell">
    <header class="desktop-header">
      <span>CMCLIENT DESKTOP</span>
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
