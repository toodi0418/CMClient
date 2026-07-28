<script setup lang="ts">
import { useMachine } from "@xstate/vue";
import {
  computed,
  nextTick,
  onBeforeUnmount,
  onMounted,
  ref,
  watch,
} from "vue";
import {
  ArrowLeft,
  ArrowRight,
  Check,
  CircleAlert,
  KeyRound,
  Languages,
  LoaderCircle,
  RefreshCw,
  RotateCcw,
  ShieldCheck,
  Wifi,
} from "@lucide/vue";
import Form from "@primevue/forms/form";
import FormField from "@primevue/forms/formfield";
import type {
  FormInstance,
  FormResolverOptions,
  FormSubmitEvent,
} from "@primevue/forms/form";
import Button from "primevue/button";
import { useI18n } from "vue-i18n";

import type { SetupConfigureRequest } from "@cmclient/contracts";
import { isSupportedLocale, type SupportedLocale } from "@/preferences";
import { setupFlowMachine } from "@/setup-flow";
import { usePreferencesStore } from "@/stores/preferences";
import { useSetupStore } from "@/stores/setup";

interface SetupFormValues {
  meshtasticHost: string;
  meshNetworkId: string;
  gatewayId: string;
  callmeshApiKey: string;
}

interface ReviewedConfiguration {
  meshtasticHost: string;
  meshtasticPort: 4403;
  meshNetworkId?: string;
  gatewayId?: string;
}

const setup = useSetupStore();
const preferences = usePreferencesStore();
const { t } = useI18n();
const { snapshot: flow, send } = useMachine(setupFlowMachine);
const termsAccepted = ref(false);
const selectedCandidate = ref("");
const localErrorKey = ref("");
const reviewedConfiguration = ref<ReviewedConfiguration>();
const connectionForm = ref<FormInstance>();
const activeHeading = ref<HTMLElement>();
let pendingCredential = "";

const localeOptions: Array<{
  value: SupportedLocale;
  labelKey: "preferences.zhTW" | "preferences.enUS";
}> = [
  { value: "zh-TW", labelKey: "preferences.zhTW" },
  { value: "en-US", labelKey: "preferences.enUS" },
];

const connectionInitialValues = computed<SetupFormValues>(() => ({
  meshtasticHost: reviewedConfiguration.value?.meshtasticHost ?? "",
  meshNetworkId: reviewedConfiguration.value?.meshNetworkId ?? "default",
  gatewayId: reviewedConfiguration.value?.gatewayId ?? "cmclient-gateway",
  callmeshApiKey: "",
}));
const localError = computed(() => {
  const key =
    localErrorKey.value ||
    (!setup.initialized && setup.errorCode ? "setup.serviceUnavailable" : "");
  return key ? t(key) : "";
});
const isSynchronizing = computed(() => flow.value.matches("synchronizing"));
const isTerms = computed(() => flow.value.matches("terms"));
const isConnection = computed(() =>
  flow.value.matches({ credentials: "connection" }),
);
const isReview = computed(() => flow.value.matches({ credentials: "review" }));
const isValidating = computed(() => flow.value.matches("validating"));
const isRecovery = computed(() => flow.value.matches("recovery"));
const isReady = computed(() => flow.value.matches("finish"));
const busy = computed(() => setup.loading || setup.discovering);

watch(
  () => setup.phase,
  (phase) => send({ type: "AGENT_PHASE_CHANGED", phase }),
  { immediate: true },
);

watch(
  () => setup.phase,
  (phase) => {
    if (
      (phase === "credentials_required" || phase === "validating") &&
      setup.candidates.length === 0
    ) {
      void setup.discover().catch(() => undefined);
    }
  },
  { immediate: true },
);

watch(
  () => JSON.stringify(flow.value.value),
  () => {
    void nextTick(() => activeHeading.value?.focus());
  },
);

onMounted(async () => {
  if (!setup.started) {
    try {
      await setup.start();
    } catch {
      localErrorKey.value = "setup.serviceUnavailable";
    }
  }
});

onBeforeUnmount(() => {
  pendingCredential = "";
  connectionForm.value?.setFieldValue("callmeshApiKey", "");
});

function chooseCandidate(event: Event) {
  const host = (event.target as HTMLSelectElement).value;
  selectedCandidate.value = host;
  if (host) {
    connectionForm.value?.setFieldValue("meshtasticHost", host);
  }
}

function setLocale(event: Event) {
  const locale = (event.target as HTMLSelectElement).value;
  if (isSupportedLocale(locale)) {
    preferences.setLocale(locale);
  }
}

async function acceptTerms() {
  localErrorKey.value = "";
  if (!termsAccepted.value) {
    localErrorKey.value = "setup.acceptTermsError";
    return;
  }
  try {
    await setup.acceptTerms();
  } catch {
    localErrorKey.value = "setup.errorUnableToAcceptTerms";
    return;
  }
  try {
    await setup.discover();
  } catch {
    localErrorKey.value = "setup.discoveryFailed";
  }
}

function resolveConnectionForm({ values }: FormResolverOptions) {
  const errors: Record<string, Array<{ message: string }>> = {};
  const host = stringValue(values.meshtasticHost).trim();
  const meshNetworkId = stringValue(values.meshNetworkId).trim();
  const gatewayId = stringValue(values.gatewayId).trim();
  const apiKey = stringValue(values.callmeshApiKey);

  if (!host || host.length > 255 || /[\s/"\\]/.test(host)) {
    errors.meshtasticHost = [{ message: t("setup.invalidHost") }];
  }
  if (
    (meshNetworkId && meshNetworkId.length > 128) ||
    /["\\]/.test(meshNetworkId)
  ) {
    errors.meshNetworkId = [{ message: t("setup.invalidOptionalId") }];
  }
  if ((gatewayId && gatewayId.length > 128) || /["\\]/.test(gatewayId)) {
    errors.gatewayId = [{ message: t("setup.invalidOptionalId") }];
  }
  if (!apiKey || apiKey.length > 4096) {
    errors.callmeshApiKey = [{ message: t("setup.apiKeyRequired") }];
  }
  return {
    values: {
      meshtasticHost: host,
      meshNetworkId,
      gatewayId,
      callmeshApiKey: apiKey,
    } satisfies SetupFormValues,
    errors,
  };
}

function reviewConfiguration(event: FormSubmitEvent) {
  localErrorKey.value = "";
  if (!event.valid) {
    return;
  }
  const host = stringValue(event.values.meshtasticHost).trim();
  const meshNetworkId = stringValue(event.values.meshNetworkId).trim();
  const gatewayId = stringValue(event.values.gatewayId).trim();
  pendingCredential = stringValue(event.values.callmeshApiKey);
  reviewedConfiguration.value = {
    meshtasticHost: host,
    meshtasticPort: 4403,
    ...(meshNetworkId ? { meshNetworkId } : {}),
    ...(gatewayId ? { gatewayId } : {}),
  };

  // PrimeVue Forms must not retain the credential after leaving this step.
  event.reset();
  send({ type: "REVIEW" });
}

function editConfiguration() {
  localErrorKey.value = "";
  send({ type: "EDIT" });
  void nextTick(() => {
    const review = reviewedConfiguration.value;
    if (review) {
      connectionForm.value?.setValues({
        meshtasticHost: review.meshtasticHost,
        meshNetworkId: review.meshNetworkId ?? "",
        gatewayId: review.gatewayId ?? "",
        callmeshApiKey: pendingCredential,
      });
    }
  });
}

async function configure() {
  localErrorKey.value = "";
  const review = reviewedConfiguration.value;
  if (!review || !pendingCredential) {
    localErrorKey.value = "setup.requiredFields";
    send({ type: "EDIT" });
    return;
  }

  const request: SetupConfigureRequest = {
    ...review,
    callmeshApiKey: pendingCredential,
  };
  pendingCredential = "";
  let operation: ReturnType<typeof setup.configure>;
  try {
    // GatewayApiClient serializes the body synchronously. Clear the only
    // remaining Web-owned copy immediately after handing off that body.
    operation = setup.configure(request);
  } finally {
    request.callmeshApiKey = "";
  }

  try {
    await operation;
  } catch {
    localErrorKey.value =
      setup.errorCode === "CALLMESH_CREDENTIAL_REJECTED"
        ? "setup.callmeshCredentialRejected"
        : setup.errorCode === "CALLMESH_UNAVAILABLE"
          ? "setup.callmeshUnavailable"
          : "setup.validationFailed";
    send({ type: "EDIT" });
  }
}

async function reset() {
  localErrorKey.value = "";
  pendingCredential = "";
  reviewedConfiguration.value = undefined;
  try {
    await setup.reset();
    termsAccepted.value = false;
  } catch {
    localErrorKey.value = "setup.errorUnableToReset";
  }
}

async function rediscover() {
  localErrorKey.value = "";
  try {
    await setup.discover();
  } catch {
    localErrorKey.value = "setup.discoveryFailed";
  }
}

async function retryStatus() {
  localErrorKey.value = "";
  try {
    await setup.refresh();
  } catch {
    localErrorKey.value = "setup.serviceUnavailable";
  }
}

function stringValue(value: unknown): string {
  return typeof value === "string" ? value : "";
}
</script>

<template>
  <main class="setup-page" aria-labelledby="setup-title">
    <section class="setup-panel">
      <div class="setup-brand">
        <div class="setup-brand__mark" aria-hidden="true">CM</div>
        <div>
          <p class="setup-eyebrow">CMCLIENT 2.0</p>
          <h1 id="setup-title">{{ t("setup.title") }}</h1>
        </div>
        <label class="setup-language-selector">
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
      </div>

      <ol class="setup-steps" :aria-label="t('setup.progress')">
        <li :class="{ active: isTerms, complete: !isTerms && !isRecovery }">
          <span>1</span><small>{{ t("setup.terms") }}</small>
        </li>
        <li
          :class="{
            active: isConnection || isReview,
            complete: isValidating || isReady,
          }"
        >
          <span>2</span><small>{{ t("setup.connection") }}</small>
        </li>
        <li :class="{ active: isValidating, complete: isReady }">
          <span>3</span><small>{{ t("setup.finish") }}</small>
        </li>
      </ol>

      <div v-if="isSynchronizing" class="setup-content setup-content--centered">
        <LoaderCircle class="setup-spinner" :size="42" aria-hidden="true" />
        <h2 ref="activeHeading" tabindex="-1">{{ t("setup.syncing") }}</h2>
      </div>

      <div v-else-if="isTerms" class="setup-content">
        <div class="setup-icon" aria-hidden="true">
          <ShieldCheck :size="24" />
        </div>
        <p class="setup-kicker">{{ t("setup.beforeBegin") }}</p>
        <h2 ref="activeHeading" tabindex="-1">{{ t("setup.connectTitle") }}</h2>
        <p class="setup-copy">{{ t("setup.connectDescription") }}</p>
        <label class="setup-consent">
          <input v-model="termsAccepted" type="checkbox" />
          <span>{{ t("setup.termsConsent") }}</span>
        </label>
        <p v-if="localError" class="setup-error" role="alert">
          <CircleAlert :size="16" aria-hidden="true" /> {{ localError }}
        </p>
        <div class="setup-actions setup-actions--split">
          <Button
            v-if="!setup.initialized"
            text
            type="button"
            :label="t('setup.refreshStatus')"
            @click="retryStatus"
          />
          <Button
            class="setup-primary"
            type="button"
            :disabled="busy || !setup.initialized"
            :label="t('setup.continue')"
            @click="acceptTerms"
          >
            <template #icon
              ><ArrowRight :size="17" aria-hidden="true"
            /></template>
          </Button>
        </div>
      </div>

      <Form
        v-else-if="isConnection"
        ref="connectionForm"
        class="setup-content"
        autocomplete="off"
        :initial-values="connectionInitialValues"
        :resolver="resolveConnectionForm"
        :validate-on-value-update="false"
        :validate-on-blur="true"
        @submit="reviewConfiguration"
      >
        <div class="setup-icon" aria-hidden="true"><Wifi :size="24" /></div>
        <p class="setup-kicker">{{ t("setup.connectionDetails") }}</p>
        <h2 ref="activeHeading" tabindex="-1">{{ t("setup.chooseNode") }}</h2>
        <p class="setup-copy">{{ t("setup.passiveCheck") }}</p>

        <div class="setup-discovery">
          <label for="discovered-node">{{ t("setup.discoveredNodes") }}</label>
          <div class="setup-inline-control">
            <select
              id="discovered-node"
              :value="selectedCandidate"
              @change="chooseCandidate"
            >
              <option value="">{{ t("setup.selectDiscoveredNode") }}</option>
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
              :aria-label="t('setup.refreshDiscoveredNodes')"
              :title="t('setup.refreshDiscoveredNodes')"
              @click="rediscover"
            >
              <RefreshCw :size="17" aria-hidden="true" />
            </Button>
          </div>
        </div>

        <div class="setup-form-grid">
          <FormField v-slot="$field" name="meshtasticHost" as="label">
            <span>{{ t("setup.host") }}</span>
            <input
              id="meshtastic-host"
              v-bind="$field.props"
              autocomplete="off"
              spellcheck="false"
              :aria-invalid="$field.invalid"
              :aria-describedby="
                $field.invalid ? 'meshtastic-host-error' : undefined
              "
            />
            <small
              v-if="$field.invalid"
              id="meshtastic-host-error"
              class="setup-field-error"
              role="alert"
            >
              {{ $field.error?.message }}
            </small>
          </FormField>
          <label>
            <span>{{ t("setup.port") }}</span>
            <input value="4403" readonly />
          </label>
          <FormField v-slot="$field" name="meshNetworkId" as="label">
            <span
              >{{ t("setup.meshNetworkId") }} ({{ t("setup.optional") }})</span
            >
            <input
              v-bind="$field.props"
              autocomplete="off"
              spellcheck="false"
              :aria-invalid="$field.invalid"
            />
            <small
              v-if="$field.invalid"
              class="setup-field-error"
              role="alert"
              >{{ $field.error?.message }}</small
            >
          </FormField>
          <FormField v-slot="$field" name="gatewayId" as="label">
            <span>{{ t("setup.gatewayId") }} ({{ t("setup.optional") }})</span>
            <input
              v-bind="$field.props"
              autocomplete="off"
              spellcheck="false"
              :aria-invalid="$field.invalid"
            />
            <small
              v-if="$field.invalid"
              class="setup-field-error"
              role="alert"
              >{{ $field.error?.message }}</small
            >
          </FormField>
        </div>

        <FormField
          v-slot="$field"
          name="callmeshApiKey"
          as="label"
          class="setup-field setup-key-field"
        >
          <span
            ><KeyRound :size="16" aria-hidden="true" />
            {{ t("setup.callmeshApiKey") }}</span
          >
          <input
            v-bind="$field.props"
            type="password"
            autocomplete="off"
            spellcheck="false"
            :aria-invalid="$field.invalid"
            :aria-describedby="
              $field.invalid ? 'callmesh-key-error' : 'callmesh-endpoint'
            "
          />
          <small
            v-if="$field.invalid"
            id="callmesh-key-error"
            class="setup-field-error"
            role="alert"
            >{{ $field.error?.message }}</small
          >
          <small id="callmesh-endpoint">{{
            t("setup.officialEndpoint", { url: setup.callmeshUrl })
          }}</small>
        </FormField>

        <p v-if="localError" class="setup-error" role="alert">
          <CircleAlert :size="16" aria-hidden="true" /> {{ localError }}
        </p>
        <div class="setup-actions">
          <Button
            class="setup-primary"
            type="submit"
            :loading="setup.loading"
            :disabled="busy"
            :label="t('setup.reviewConfiguration')"
          >
            <template #icon
              ><ArrowRight :size="17" aria-hidden="true"
            /></template>
          </Button>
        </div>
      </Form>

      <div v-else-if="isReview" class="setup-content">
        <div class="setup-icon" aria-hidden="true">
          <ShieldCheck :size="24" />
        </div>
        <p class="setup-kicker">{{ t("setup.review") }}</p>
        <h2 ref="activeHeading" tabindex="-1">{{ t("setup.reviewTitle") }}</h2>
        <p class="setup-copy">{{ t("setup.reviewDescription") }}</p>
        <dl v-if="reviewedConfiguration" class="setup-review">
          <div>
            <dt>{{ t("setup.host") }}</dt>
            <dd>{{ reviewedConfiguration.meshtasticHost }}:4403</dd>
          </div>
          <div>
            <dt>{{ t("setup.meshNetworkId") }}</dt>
            <dd>
              {{
                reviewedConfiguration.meshNetworkId ?? t("setup.defaultValue")
              }}
            </dd>
          </div>
          <div>
            <dt>{{ t("setup.gatewayId") }}</dt>
            <dd>
              {{ reviewedConfiguration.gatewayId ?? t("setup.defaultValue") }}
            </dd>
          </div>
          <div>
            <dt>{{ t("setup.callmeshApiKey") }}</dt>
            <dd>
              <KeyRound :size="15" aria-hidden="true" />
              {{ t("setup.credentialProvided") }}
            </dd>
          </div>
        </dl>
        <p v-if="localError" class="setup-error" role="alert">
          <CircleAlert :size="16" aria-hidden="true" /> {{ localError }}
        </p>
        <div class="setup-actions setup-actions--split">
          <Button
            type="button"
            severity="secondary"
            :label="t('setup.editConfiguration')"
            @click="editConfiguration"
          >
            <template #icon
              ><ArrowLeft :size="17" aria-hidden="true"
            /></template>
          </Button>
          <Button
            class="setup-primary"
            type="button"
            :loading="setup.loading"
            :disabled="busy"
            :label="t('setup.validateAndStart')"
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
        aria-live="polite"
      >
        <LoaderCircle class="setup-spinner" :size="42" aria-hidden="true" />
        <p class="setup-kicker">{{ t("setup.agentValidation") }}</p>
        <h2 ref="activeHeading" tabindex="-1">
          {{ t("setup.startingServices") }}
        </h2>
        <p class="setup-copy">{{ t("setup.applyingConfiguration") }}</p>
        <Button
          text
          type="button"
          :label="t('setup.refreshStatus')"
          @click="setup.refresh"
        />
      </div>

      <div
        v-else-if="isReady"
        class="setup-content setup-content--centered"
        aria-live="polite"
      >
        <div class="setup-success" aria-hidden="true"><Check :size="26" /></div>
        <p class="setup-kicker">{{ t("setup.ready") }}</p>
        <h2 ref="activeHeading" tabindex="-1">{{ t("setup.readyTitle") }}</h2>
        <p class="setup-copy">{{ t("setup.readyDescription") }}</p>
      </div>

      <div v-else class="setup-content setup-content--centered">
        <div class="setup-icon setup-icon--warning" aria-hidden="true">
          <CircleAlert :size="24" />
        </div>
        <p class="setup-kicker">{{ t("setup.recoveryRequired") }}</p>
        <h2 ref="activeHeading" tabindex="-1">
          {{ t("setup.recoveryTitle") }}
        </h2>
        <p class="setup-copy">{{ t("setup.recoveryDescription") }}</p>
        <p v-if="localError" class="setup-error" role="alert">
          <CircleAlert :size="16" aria-hidden="true" /> {{ localError }}
        </p>
        <Button type="button" :label="t('setup.resetSetup')" @click="reset">
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
  background: #fff;
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
  flex: 0 0 42px;
  place-items: center;
  border-radius: 8px;
  background: #1e6b72;
  color: #fff;
  font-weight: 750;
}

.setup-eyebrow,
.setup-kicker {
  margin: 0;
  color: #59717a;
  font-size: 0.72rem;
  font-weight: 750;
  letter-spacing: 0;
  text-transform: uppercase;
}

.setup-brand h1 {
  margin: 0.2rem 0 0;
  font-size: 1.35rem;
  line-height: 1.2;
}

.setup-language-selector {
  display: flex;
  min-height: 36px;
  align-items: center;
  gap: 0.4rem;
  margin-left: auto;
  padding: 0 0.55rem;
  border: 1px solid #b9c9ce;
  border-radius: 5px;
  color: #1e6b72;
  background: #fff;
}

.setup-language-selector:focus-within,
input:focus,
select:focus {
  outline: 2px solid rgb(30 107 114 / 28%);
  outline-offset: 2px;
  border-color: #1e6b72;
}

.setup-language-selector select {
  max-width: 9rem;
  border: 0;
  outline: 0;
  color: #30464e;
  background: transparent;
  font: inherit;
  font-size: 0.8rem;
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
  border: 1px solid currentcolor;
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

.setup-content h2:focus {
  outline: none;
}

.setup-copy {
  max-width: 58ch;
  margin: 0 0 1.35rem;
  color: #526970;
  line-height: 1.6;
}

.setup-content--centered .setup-copy {
  margin-right: auto;
  margin-left: auto;
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

.setup-discovery > label,
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

.setup-key-field {
  margin-bottom: 1.1rem;
}

.setup-key-field > span,
.setup-review dd {
  display: inline-flex;
  gap: 0.4rem;
  align-items: center;
}

.setup-key-field small {
  color: #71838a;
  font-weight: 450;
  overflow-wrap: anywhere;
}

.setup-field-error,
.setup-key-field .setup-field-error {
  color: #a33131;
  font-weight: 550;
}

.setup-error {
  display: flex;
  gap: 0.45rem;
  align-items: center;
  margin: 0.75rem 0 1rem;
  color: #a33131;
  font-size: 0.86rem;
}

.setup-review {
  margin: 1.25rem 0 1.5rem;
  border-top: 1px solid #e8edef;
}

.setup-review div {
  display: grid;
  grid-template-columns: minmax(9rem, 0.8fr) minmax(0, 1.2fr);
  gap: 1rem;
  padding: 0.8rem 0;
  border-bottom: 1px solid #e8edef;
}

.setup-review dt {
  color: #59717a;
  font-size: 0.82rem;
}

.setup-review dd {
  min-width: 0;
  margin: 0;
  overflow-wrap: anywhere;
  color: #14252d;
  font-weight: 650;
}

.setup-actions {
  display: flex;
  justify-content: flex-end;
  gap: 0.75rem;
}

.setup-actions--split {
  justify-content: space-between;
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

  .setup-brand,
  .setup-content,
  .setup-steps {
    padding-right: 1rem;
    padding-left: 1rem;
  }

  .setup-brand {
    flex-wrap: wrap;
  }

  .setup-language-selector {
    max-width: 100%;
  }

  .setup-steps {
    justify-content: space-between;
    gap: 0.4rem;
  }

  .setup-steps li {
    gap: 0.25rem;
  }

  .setup-form-grid,
  .setup-review div {
    grid-template-columns: 1fr;
    gap: 0.3rem;
  }

  .setup-actions,
  .setup-actions--split {
    flex-direction: column-reverse;
  }

  .setup-actions :deep(button) {
    width: 100%;
  }
}
</style>
