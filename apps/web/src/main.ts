import { createApp } from "vue";
import { createPinia } from "pinia";
import Aura from "@primeuix/themes/aura";
import PrimeVue from "primevue/config";

import App from "./App.vue";
import { i18n } from "./i18n";
import { router } from "./router";
import { useGatewayStore } from "./stores/gateway";
import { useManagementAuthStore } from "./stores/management-auth";
import { usePreferencesStore } from "./stores/preferences";
import "./styles.css";

const app = createApp(App);
const pinia = createPinia();

app.use(pinia);
app.use(i18n);
usePreferencesStore(pinia).initialize();
app.use(router);
app.use(PrimeVue, {
  theme: {
    preset: Aura,
    options: {
      darkModeSelector: ".cm-dark",
    },
  },
});

async function mountApplication() {
  await useManagementAuthStore(pinia).initialize();
  void useGatewayStore(pinia).initialize();
  app.mount("#app");
}

void mountApplication();
