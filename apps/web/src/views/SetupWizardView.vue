<script setup lang="ts">
import { computed, onMounted, ref, watch } from "vue";
import {
  ArrowRight,
  Check,
  CircleAlert,
  KeyRound,
  LoaderCircle,
  RefreshCw,
  RotateCcw,
  ShieldCheck,
  Wifi,
} from "@lucide/vue";
import Button from "primevue/button";

import type { SetupConfigureRequest } from "@cmclient/contracts";
import { useSetupStore } from "@/stores/setup";

const setup = useSetupStore();
const termsAccepted = ref(false);
const meshtasticHost = ref("127.0.0.1");
const meshtasticPort = 4403;
const meshNetworkId = ref("default");
const gatewayId = ref("cmclient-gateway");
const callmeshApiKey = ref("");
const selectedCandidate = ref("");
const localError = ref("");

const phase = computed(() => setup.phase);
const isTerms = computed(
  () => phase.value === "uninitialized" || phase.value === "terms_required",
);
const isCredentials = computed(() => phase.value === "credentials_required");
const isValidating = computed(() => phase.value === "validating");
const isRecovery = computed(() => phase.value === "recovery_required");
const isReady = computed(() => phase.value === "ready");
const busy = computed(() => setup.loading || setup.discovering);

onMounted(async () => {
  try {
    await setup.refresh();
    if (setup.phase !== "terms_required" && setup.phase !== "uninitialized") {
      await setup.discover();
    }
  } catch {
    localError.value = "Agent setup service is not available.";
  }
});

watch(
  () => setup.phase,
  (nextPhase) => {
    if (
      (nextPhase === "credentials_required" || nextPhase === "validating") &&
      setup.candidates.length === 0
    ) {
      void setup.discover().catch(() => undefined);
    }
  },
);

function chooseCandidate(event: Event) {
  const host = (event.target as HTMLSelectElement).value;
  selectedCandidate.value = host;
  if (host) {
    meshtasticHost.value = host;
  }
}

async function acceptTerms() {
  localError.value = "";
  if (!termsAccepted.value) {
    localError.value = "Accept the terms to continue.";
    return;
  }
  try {
    await setup.acceptTerms();
    await setup.discover();
  } catch {
    localError.value = setup.errorCode ?? "Unable to accept terms.";
  }
}

async function configure() {
  localError.value = "";
  if (!meshtasticHost.value.trim() || !callmeshApiKey.value) {
    localError.value = "Meshtastic host and CallMesh API key are required.";
    return;
  }
  const key = callmeshApiKey.value;
  try {
    const request: SetupConfigureRequest = {
      meshtasticHost: meshtasticHost.value.trim(),
      meshtasticPort,
      callmeshApiKey: key,
      ...(meshNetworkId.value.trim()
        ? { meshNetworkId: meshNetworkId.value.trim() }
        : {}),
      ...(gatewayId.value.trim() ? { gatewayId: gatewayId.value.trim() } : {}),
    };
    await setup.configure(request);
  } catch {
    localError.value = setup.errorCode ?? "Setup validation failed.";
  } finally {
    // Never retain the credential in reactive state after the request.
    callmeshApiKey.value = "";
  }
}

async function reset() {
  localError.value = "";
  try {
    await setup.reset();
    termsAccepted.value = false;
    callmeshApiKey.value = "";
  } catch {
    localError.value = setup.errorCode ?? "Unable to reset setup.";
  }
}

async function rediscover() {
  localError.value = "";
  try {
    await setup.discover();
  } catch {
    localError.value = setup.errorCode ?? "Discovery failed.";
  }
}
</script>

<template>
  <main class="setup-page" aria-labelledby="setup-title">
    <section class="setup-panel">
      <div class="setup-brand">
        <div class="setup-brand__mark" aria-hidden="true">CM</div>
        <div>
          <p class="setup-eyebrow">CMCLIENT 2.0</p>
          <h1 id="setup-title">Initial setup</h1>
        </div>
      </div>

      <ol class="setup-steps" aria-label="Setup progress">
        <li :class="{ active: isTerms, complete: !isTerms && !isRecovery }">
          <span>1</span>
          <small>Terms</small>
        </li>
        <li
          :class="{
            active: isCredentials || isValidating,
            complete: isReady,
          }"
        >
          <span>2</span>
          <small>Connection</small>
        </li>
        <li :class="{ active: isValidating || isReady, complete: isReady }">
          <span>3</span>
          <small>Finish</small>
        </li>
      </ol>

      <div v-if="isTerms" class="setup-content">
        <div class="setup-icon" aria-hidden="true">
          <ShieldCheck :size="24" />
        </div>
        <p class="setup-kicker">Before you begin</p>
        <h2>Connect your CMClient to CallMesh</h2>
        <p class="setup-copy">
          CMClient uses the official CallMesh service and your Meshtastic node
          connection to provide the complete Web management experience. Nothing
          is transmitted until setup is complete.
        </p>
        <label class="setup-consent">
          <input v-model="termsAccepted" type="checkbox" />
          <span
            >I agree to the CMClient terms of use and understand that the
            supplied API key is stored locally for this installation.</span
          >
        </label>
        <p v-if="localError" class="setup-error" role="alert">
          <CircleAlert :size="16" aria-hidden="true" /> {{ localError }}
        </p>
        <Button
          class="setup-primary"
          type="button"
          :disabled="busy"
          label="Continue"
          @click="acceptTerms"
        >
          <template #icon
            ><ArrowRight :size="17" aria-hidden="true"
          /></template>
        </Button>
      </div>

      <div v-else-if="isCredentials" class="setup-content">
        <div class="setup-icon" aria-hidden="true"><Wifi :size="24" /></div>
        <p class="setup-kicker">Connection details</p>
        <h2>Choose your Meshtastic node</h2>
        <p class="setup-copy">
          The Agent performs a passive TCP reachability check on port 4403.
          Radio settings are never changed by setup.
        </p>

        <div v-if="setup.candidates.length" class="setup-discovery">
          <label for="discovered-node">Discovered nodes</label>
          <div class="setup-inline-control">
            <select
              id="discovered-node"
              :value="selectedCandidate"
              @change="chooseCandidate"
            >
              <option value="">Select a discovered node</option>
              <option
                v-for="candidate in setup.candidates"
                :key="`${candidate.host}:${candidate.port}`"
                :value="candidate.host"
              >
                {{ candidate.host }}:{{ candidate.port }} ({{
                  candidate.source
                }})
              </option>
            </select>
            <Button
              class="setup-icon-button"
              text
              type="button"
              :loading="setup.discovering"
              aria-label="Refresh discovered nodes"
              title="Refresh discovered nodes"
              @click="rediscover"
            >
              <RefreshCw :size="17" aria-hidden="true" />
            </Button>
          </div>
        </div>

        <div class="setup-form-grid">
          <label>
            Meshtastic host
            <input
              v-model="meshtasticHost"
              autocomplete="off"
              spellcheck="false"
            />
          </label>
          <label>
            Port
            <input :value="meshtasticPort" readonly />
          </label>
          <label>
            Mesh network ID <span>(optional)</span>
            <input v-model="meshNetworkId" autocomplete="off" />
          </label>
          <label>
            Gateway ID <span>(optional)</span>
            <input v-model="gatewayId" autocomplete="off" />
          </label>
        </div>

        <label class="setup-field setup-key-field">
          <span
            ><KeyRound :size="16" aria-hidden="true" /> CallMesh API key</span
          >
          <input
            v-model="callmeshApiKey"
            type="password"
            autocomplete="new-password"
            spellcheck="false"
          />
          <small>Official endpoint: {{ setup.callmeshUrl }}</small>
        </label>

        <p v-if="localError" class="setup-error" role="alert">
          <CircleAlert :size="16" aria-hidden="true" /> {{ localError }}
        </p>
        <div class="setup-actions">
          <Button
            class="setup-primary"
            type="button"
            :loading="setup.loading"
            :disabled="busy || !callmeshApiKey"
            label="Validate and start"
            @click="configure"
          >
            <template #icon
              ><ArrowRight :size="17" aria-hidden="true"
            /></template>
          </Button>
        </div>
      </div>

      <div
        v-else-if="isValidating"
        class="setup-content setup-content--centered"
      >
        <LoaderCircle class="setup-spinner" :size="42" aria-hidden="true" />
        <p class="setup-kicker">Agent validation</p>
        <h2>Starting your local services</h2>
        <p class="setup-copy">
          CMClient is applying the configuration and starting the Gateway. Keep
          this page open; it will continue automatically.
        </p>
        <Button
          text
          type="button"
          label="Refresh status"
          @click="setup.refresh"
        />
      </div>

      <div v-else-if="isReady" class="setup-content setup-content--centered">
        <div class="setup-success" aria-hidden="true"><Check :size="26" /></div>
        <p class="setup-kicker">Ready</p>
        <h2>CMClient is ready</h2>
        <p class="setup-copy">
          The full management console is now available. Gateway and APRS status
          will appear in the dashboard as services come online.
        </p>
      </div>

      <div v-else class="setup-content setup-content--centered">
        <div class="setup-icon setup-icon--warning" aria-hidden="true">
          <CircleAlert :size="24" />
        </div>
        <p class="setup-kicker">Recovery required</p>
        <h2>Setup needs to be run again</h2>
        <p class="setup-copy">
          The Agent could not complete the last setup transaction. Reset the
          local setup state and start again.
        </p>
        <p v-if="localError" class="setup-error" role="alert">
          <CircleAlert :size="16" aria-hidden="true" /> {{ localError }}
        </p>
        <Button type="button" label="Reset setup" @click="reset">
          <template #icon><RotateCcw :size="17" aria-hidden="true" /></template>
        </Button>
      </div>
    </section>
  </main>
</template>

<style scoped>
.setup-page {
  min-height: 100vh;
  display: grid;
  place-items: center;
  padding: 2rem 1rem;
  background: #eef2f4;
  color: #14252d;
}

.setup-panel {
  width: min(100%, 720px);
  background: #ffffff;
  border: 1px solid #d8e1e4;
  border-radius: 8px;
  box-shadow: 0 18px 48px rgb(20 37 45 / 12%);
  overflow: hidden;
}

.setup-brand {
  display: flex;
  gap: 0.85rem;
  align-items: center;
  padding: 1.5rem 1.75rem 1.25rem;
  border-bottom: 1px solid #e8edef;
}

.setup-brand__mark {
  display: grid;
  width: 42px;
  height: 42px;
  place-items: center;
  border-radius: 8px;
  background: #1e6b72;
  color: #ffffff;
  font-weight: 750;
  letter-spacing: 0;
}

.setup-eyebrow,
.setup-kicker {
  margin: 0;
  color: #59717a;
  font-size: 0.72rem;
  font-weight: 750;
  letter-spacing: 0.08em;
  text-transform: uppercase;
}

.setup-brand h1 {
  margin: 0.2rem 0 0;
  font-size: 1.35rem;
  line-height: 1.2;
}

.setup-steps {
  display: flex;
  gap: 1rem;
  margin: 0;
  padding: 1rem 1.75rem;
  list-style: none;
  border-bottom: 1px solid #e8edef;
}

.setup-steps li {
  display: flex;
  align-items: center;
  gap: 0.45rem;
  color: #86979d;
  font-size: 0.8rem;
}

.setup-steps li span {
  display: grid;
  width: 24px;
  height: 24px;
  place-items: center;
  border: 1px solid currentColor;
  border-radius: 50%;
  font-size: 0.72rem;
  font-weight: 700;
}

.setup-steps li.active,
.setup-steps li.complete {
  color: #1e6b72;
}

.setup-steps li.complete span {
  background: #1e6b72;
  color: #fff;
}

.setup-content {
  padding: 2rem 1.75rem 2.25rem;
}

.setup-content--centered {
  text-align: center;
}

.setup-icon,
.setup-success {
  display: grid;
  width: 48px;
  height: 48px;
  place-items: center;
  margin-bottom: 1rem;
  border-radius: 50%;
  background: #dceff0;
  color: #1e6b72;
}

.setup-content--centered .setup-icon,
.setup-content--centered .setup-success,
.setup-content--centered .setup-spinner {
  margin-right: auto;
  margin-left: auto;
}

.setup-icon--warning {
  background: #fff2d8;
  color: #a86700;
}

.setup-success {
  background: #dff4e8;
  color: #177245;
}

.setup-content h2 {
  margin: 0.4rem 0 0.65rem;
  font-size: 1.55rem;
  line-height: 1.25;
}

.setup-copy {
  max-width: 58ch;
  margin: 0 0 1.35rem;
  color: #526970;
  line-height: 1.6;
}

.setup-consent {
  display: flex;
  gap: 0.7rem;
  align-items: flex-start;
  margin: 1.4rem 0;
  color: #30464e;
  line-height: 1.45;
}

.setup-consent input {
  width: 1rem;
  height: 1rem;
  margin-top: 0.2rem;
  accent-color: #1e6b72;
}

.setup-discovery {
  margin-bottom: 1rem;
}

.setup-discovery label,
.setup-form-grid label,
.setup-key-field {
  display: flex;
  flex-direction: column;
  gap: 0.4rem;
  color: #30464e;
  font-size: 0.84rem;
  font-weight: 650;
}

.setup-discovery select,
.setup-form-grid input,
.setup-key-field input {
  min-height: 40px;
  width: 100%;
  padding: 0.55rem 0.65rem;
  border: 1px solid #b9c9ce;
  border-radius: 5px;
  background: #fff;
  color: #14252d;
  font: inherit;
}

.setup-discovery select:focus,
.setup-form-grid input:focus,
.setup-key-field input:focus {
  outline: 2px solid rgb(30 107 114 / 28%);
  border-color: #1e6b72;
}

.setup-inline-control {
  display: flex;
  gap: 0.5rem;
}

.setup-icon-button {
  flex: 0 0 40px;
  min-height: 40px;
}

.setup-form-grid {
  display: grid;
  grid-template-columns: 2fr 1fr;
  gap: 0.9rem;
  margin-bottom: 1rem;
}

.setup-form-grid label span {
  color: #71838a;
  font-weight: 450;
}

.setup-key-field {
  margin-bottom: 1.1rem;
}

.setup-key-field span {
  display: inline-flex;
  gap: 0.4rem;
  align-items: center;
}

.setup-key-field small {
  color: #71838a;
  font-weight: 450;
  overflow-wrap: anywhere;
}

.setup-error {
  display: flex;
  gap: 0.45rem;
  align-items: center;
  margin: 0.75rem 0 1rem;
  color: #a33131;
  font-size: 0.86rem;
}

.setup-actions {
  display: flex;
  justify-content: flex-end;
}

.setup-primary {
  min-width: 152px;
}

.setup-spinner {
  color: #1e6b72;
  animation: setup-spin 1.2s linear infinite;
}

@keyframes setup-spin {
  to {
    transform: rotate(360deg);
  }
}

@media (max-width: 600px) {
  .setup-page {
    place-items: start center;
    padding: 0;
  }

  .setup-panel {
    min-height: 100vh;
    border: 0;
    border-radius: 0;
  }

  .setup-form-grid {
    grid-template-columns: 1fr;
  }
}
</style>
