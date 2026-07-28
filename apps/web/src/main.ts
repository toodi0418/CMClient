import { createApp } from "vue";
import { createPinia } from "pinia";
import Aura from "@primeuix/themes/aura";
import PrimeVue from "primevue/config";

import App from "./App.vue";
import { i18n } from "./i18n";
import { installSetupRouteGate, router } from "./router";
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

function mountApplication() {
  const setup = useSetupStore(pinia);
  installSetupRouteGate(router, setup);
  app.use(router);
  app.mount("#app");
  // Mount before waiting for network admission so a stalled local request still
  // renders the bounded connection-recovery state.
  void setup.start().catch(() => undefined);
}

void mountApplication();
