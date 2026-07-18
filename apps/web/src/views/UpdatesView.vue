<script setup lang="ts">
import { computed, onMounted } from "vue";
import { RefreshCw } from "@lucide/vue";
import Button from "primevue/button";
import { useI18n } from "vue-i18n";

import { useGatewayStore } from "@/stores/gateway";

const gateway = useGatewayStore();
const { t } = useI18n();
const updateCapability = computed(
  () => gateway.capabilities?.capabilities.update,
);

onMounted(() => void gateway.refresh());
</script>

<template>
  <section class="page-grid" :aria-label="t('navigation.updates')">
    <div class="status-panel">
      <div class="panel-heading">
        <div>
          <p class="section-placeholder__eyebrow">
            {{ t("navigation.updates") }}
          </p>
          <h2>{{ t("updates.status") }}</h2>
        </div>
        <Button
          unstyled
          class="page-action"
          type="button"
          :aria-label="t('common.refresh')"
          :title="t('common.refresh')"
          :disabled="gateway.loading"
          @click="gateway.refresh"
          ><RefreshCw :size="17" aria-hidden="true"
        /></Button>
      </div>
      <p v-if="gateway.errorCode" class="status-message">
        {{ t("common.unavailable") }}
        <code class="stable-code">{{ gateway.errorCode }}</code>
      </p>
      <dl v-else class="facts-grid">
        <div>
          <dt>{{ t("updates.currentVersion") }}</dt>
          <dd>{{ gateway.status?.build.version ?? "--" }}</dd>
        </div>
        <div>
          <dt>{{ t("updates.channel") }}</dt>
          <dd>{{ gateway.status?.build.channel ?? "--" }}</dd>
        </div>
        <div>
          <dt>{{ t("updates.owner") }}</dt>
          <dd>
            <span
              class="status-badge"
              :data-state="
                updateCapability?.available ? 'available' : 'unavailable'
              "
            >
              {{
                updateCapability?.available
                  ? t("common.available")
                  : t("common.notConfigured")
              }}
            </span>
          </dd>
        </div>
        <div>
          <dt>{{ t("updates.capability") }}</dt>
          <dd>
            <code>{{
              updateCapability && !updateCapability.available
                ? updateCapability.reasonCode
                : "--"
            }}</code>
          </dd>
        </div>
      </dl>
    </div>
  </section>
</template>
