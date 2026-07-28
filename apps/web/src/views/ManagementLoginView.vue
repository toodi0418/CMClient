<script setup lang="ts">
import { ref } from "vue";
import { KeyRound } from "@lucide/vue";
import Button from "primevue/button";
import { useI18n } from "vue-i18n";

import ProblemNotice from "@/components/ProblemNotice.vue";
import { useManagementAuthStore } from "@/stores/management-auth";

const emit = defineEmits<{ authenticated: [] }>();
const { t } = useI18n();
const auth = useManagementAuthStore();
const password = ref("");

async function submit() {
  const submittedPassword = password.value;
  password.value = "";
  await auth.login(submittedPassword);
  if (!auth.errorCode && !auth.required) {
    emit("authenticated");
  }
}
</script>

<template>
  <main class="management-login">
    <form class="management-login__form" @submit.prevent="submit">
      <div class="management-login__mark" aria-hidden="true">
        <KeyRound :size="22" />
      </div>
      <div>
        <p class="management-login__eyebrow">CMCLIENT / LAN</p>
        <h1>{{ t("auth.title") }}</h1>
      </div>
      <label class="management-login__field">
        <span>{{ t("auth.password") }}</span>
        <input
          v-model="password"
          type="password"
          name="password"
          autocomplete="current-password"
          :disabled="auth.loading"
          required
        />
      </label>
      <ProblemNotice v-if="auth.errorCode" :code="auth.errorCode" compact />
      <Button
        unstyled
        class="command-action management-login__submit"
        type="submit"
        :disabled="auth.loading || !password"
      >
        {{ auth.loading ? t("auth.working") : t("auth.submit") }}
      </Button>
    </form>
  </main>
</template>
