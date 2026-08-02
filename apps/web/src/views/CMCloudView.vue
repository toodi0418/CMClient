<script setup lang="ts">
import { onMounted } from "vue";
import { Cloud, KeyRound, RefreshCw } from "@lucide/vue";
import Button from "primevue/button";
import InputText from "primevue/inputtext";
import { useI18n } from "vue-i18n";

import ProblemNotice from "@/components/ProblemNotice.vue";
import { useCMCloudStore } from "@/stores/cmcloud";

const cmcloud = useCMCloudStore();
const { t } = useI18n();

onMounted(() => void cmcloud.refresh());
</script>

<template>
  <section class="page-grid" :aria-label="t('navigation.cmcloud')">
    <div class="status-panel">
      <div class="panel-heading">
        <div>
          <p class="section-placeholder__eyebrow">
            {{ t("navigation.cmcloud") }}
          </p>
          <h2>{{ t("cmcloud.status") }}</h2>
        </div>
        <Button
          unstyled
          class="page-action"
          type="button"
          :aria-label="t('common.refresh')"
          :title="t('common.refresh')"
          :disabled="cmcloud.loading || cmcloud.enrolling"
          @click="cmcloud.refresh"
          ><RefreshCw :size="17" aria-hidden="true"
        /></Button>
      </div>
      <p v-if="cmcloud.loading" class="status-message">
        {{ t("common.loading") }}
      </p>
      <ProblemNotice
        v-else-if="cmcloud.errorCode && !cmcloud.status"
        :code="cmcloud.errorCode"
        show-retry
        @retry="cmcloud.refresh"
      />
      <dl v-else-if="cmcloud.status" class="facts-grid">
        <div>
          <dt>{{ t("cmcloud.state") }}</dt>
          <dd>
            <span class="status-badge" :data-state="cmcloud.status.state">
              {{ t(`cmcloud.stateLabel.${cmcloud.status.state}`) }}
            </span>
          </dd>
        </div>
        <div>
          <dt>{{ t("cmcloud.endpoint") }}</dt>
          <dd>
            <code>{{ cmcloud.status.endpoint ?? "--" }}</code>
          </dd>
        </div>
        <div>
          <dt>{{ t("cmcloud.installationGeneration") }}</dt>
          <dd>{{ cmcloud.status.installationGeneration ?? "--" }}</dd>
        </div>
        <div>
          <dt>{{ t("cmcloud.credentialVersion") }}</dt>
          <dd>{{ cmcloud.status.credentialVersion ?? "--" }}</dd>
        </div>
      </dl>
      <ProblemNotice
        v-if="cmcloud.errorCode && cmcloud.status"
        :code="cmcloud.errorCode"
        compact
        show-retry
        @retry="cmcloud.refresh"
      />
    </div>

    <div class="status-panel">
      <div class="panel-heading">
        <div>
          <p class="section-placeholder__eyebrow">
            {{ t("cmcloud.enrollment") }}
          </p>
          <h2>{{ t("cmcloud.pairDevice") }}</h2>
        </div>
        <Cloud class="panel-symbol" :size="21" aria-hidden="true" />
      </div>
      <form class="cmcloud-enrollment" @submit.prevent="cmcloud.enroll">
        <label class="management-login__field" for="cmcloud-pairing-code">
          <span
            ><KeyRound :size="16" aria-hidden="true" />
            {{ t("cmcloud.pairingCode") }}</span
          >
          <InputText
            id="cmcloud-pairing-code"
            :model-value="cmcloud.pairingCode"
            type="password"
            autocomplete="off"
            spellcheck="false"
            inputmode="text"
            :placeholder="t('cmcloud.pairingCodePlaceholder')"
            :disabled="cmcloud.loading || cmcloud.enrolling"
            @update:model-value="cmcloud.setPairingCode(String($event ?? ''))"
          />
        </label>
        <p class="status-message cmcloud-enrollment__hint">
          {{ t("cmcloud.pairingCodeHint") }}
        </p>
        <Button
          class="command-action"
          type="submit"
          :loading="cmcloud.enrolling"
          :disabled="cmcloud.loading || cmcloud.enrolling"
        >
          <KeyRound :size="17" aria-hidden="true" />
          <span>{{ t("cmcloud.enroll") }}</span>
        </Button>
      </form>
    </div>
  </section>
</template>
