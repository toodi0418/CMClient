import {
  createMemoryHistory,
  createRouter,
  createWebHistory,
} from "vue-router";

const sections = [
  ["nodes", "navigation.nodes", "section.nodes"],
  ["positions", "navigation.positions", "section.positions"],
  ["messages", "navigation.messages", "section.messages"],
  ["telemetry", "navigation.telemetry", "section.telemetry"],
  ["aprs", "navigation.aprs", "section.aprs"],
  ["logs", "navigation.logs", "section.logs"],
  ["settings", "navigation.settings", "section.settings"],
  ["diagnostics", "navigation.diagnostics", "section.diagnostics"],
] as const;

export const router = createRouter({
  history:
    typeof window === "undefined" ? createMemoryHistory() : createWebHistory(),
  routes: [
    {
      path: "/",
      name: "overview",
      component: () => import("@/views/OverviewView.vue"),
      meta: { labelKey: "navigation.overview", group: "control" },
    },
    {
      path: "/system",
      name: "system",
      component: () => import("@/views/SystemView.vue"),
      meta: { labelKey: "navigation.system", group: "operations" },
    },
    {
      path: "/meshtastic",
      name: "meshtastic",
      component: () => import("@/views/MeshtasticView.vue"),
      meta: { labelKey: "navigation.meshtastic", group: "operations" },
    },
    ...sections.map(([name, labelKey, summaryKey]) => ({
      path: `/${name}`,
      name,
      component: () => import("@/views/SectionView.vue"),
      props: { labelKey, summaryKey },
      meta: { labelKey, group: "operations" },
    })),
    {
      path: "/:pathMatch(.*)*",
      redirect: "/",
    },
  ],
});
