import {
  createMemoryHistory,
  createRouter,
  createWebHistory,
} from "vue-router";

const sections = [
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
    {
      path: "/aprs",
      name: "aprs",
      component: () => import("@/views/AprsView.vue"),
      meta: { labelKey: "navigation.aprs", group: "operations" },
    },
    {
      path: "/callmesh",
      name: "callmesh",
      component: () => import("@/views/CallMeshView.vue"),
      meta: { labelKey: "navigation.callmesh", group: "operations" },
    },
    {
      path: "/logs",
      name: "logs",
      component: () => import("@/views/LogsView.vue"),
      meta: { labelKey: "navigation.logs", group: "operations" },
    },
    {
      path: "/nodes",
      name: "nodes",
      component: () => import("@/views/NodesView.vue"),
      meta: { labelKey: "navigation.nodes", group: "operations" },
    },
    {
      path: "/positions",
      name: "positions",
      component: () => import("@/views/PositionsView.vue"),
      meta: { labelKey: "navigation.positions", group: "operations" },
    },
    {
      path: "/messages",
      name: "messages",
      component: () => import("@/views/MessagesView.vue"),
      meta: { labelKey: "navigation.messages", group: "operations" },
    },
    {
      path: "/telemetry",
      name: "telemetry",
      component: () => import("@/views/TelemetryView.vue"),
      meta: { labelKey: "navigation.telemetry", group: "operations" },
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
