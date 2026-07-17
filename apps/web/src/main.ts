import { createApp } from "vue";
import { createPinia } from "pinia";
import Aura from "@primeuix/themes/aura";
import PrimeVue from "primevue/config";

import App from "./App.vue";
import { i18n } from "./i18n";
import { router } from "./router";
import { useGatewayStore } from "./stores/gateway";
import { usePreferencesStore } from "./stores/preferences";
import "./styles.css";

const app = createApp(App);
const pinia = createPinia();

app.use(pinia);
app.use(i18n);
usePreferencesStore(pinia).initialize();
void useGatewayStore(pinia).initialize();
app.use(router);
app.use(PrimeVue, {
  theme: {
    preset: Aura,
    options: {
      darkModeSelector: ".cm-dark",
    },
  },
});
app.mount("#app");
