import { createApp } from "vue";
import { createPinia } from "pinia";
import Aura from "@primeuix/themes/aura";
import PrimeVue from "primevue/config";

import App from "./App.vue";
import { router } from "./router";
import "./styles.css";

const app = createApp(App);

app.use(createPinia());
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
