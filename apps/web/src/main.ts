import { createApp } from "vue";
import { createPinia } from "pinia";
import Aura from "@primeuix/themes/aura";
import PrimeVue from "primevue/config";

import App from "./App.vue";
import { i18n } from "./i18n";
import { installSetupRouteGate, router } from "./router";
import { useManagementAuthStore } from "./stores/management-auth";
import { usePreferencesStore } from "./stores/preferences";
import { useSetupStore } from "./stores/setup";
import "./styles.css";

const app = createApp(App);
const pinia = createPinia();

app.use(pinia);
app.use(i18n);
usePreferencesStore(pinia).initialize();
app.use(PrimeVue, {
  theme: {
    preset: Aura,
    options: {
      darkModeSelector: ".cm-dark",
    },
  },
});

async function mountApplication() {
  const setup = useSetupStore(pinia);
  const auth = useManagementAuthStore(pinia);
  installSetupRouteGate(router, setup);

  // Setup mutations require the loopback management session and CSRF token.
  await auth.initialize();
  try {
    await setup.start();
  } catch {
    // Router admission treats an unavailable setup projection as required.
  }
  app.use(router);
  await router.isReady();
  app.mount("#app");
}

void mountApplication();
