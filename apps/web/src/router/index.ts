import {
  createMemoryHistory,
  createRouter,
  createWebHistory,
} from "vue-router";

const sections = [
  ["system", "系統", "Gateway、Agent 與服務狀態"],
  ["meshtastic", "Meshtastic", "Radio transport 與裝置連線"],
  ["nodes", "Nodes", "Mesh node registry"],
  ["positions", "Positions", "位置事件與高水位"],
  ["messages", "Messages", "訊息收送紀錄"],
  ["telemetry", "Telemetry", "遙測觀測值"],
  ["aprs", "APRS", "APRS-IS outbox 與監看"],
  ["logs", "Logs", "診斷與事件記錄"],
  ["settings", "Settings", "本機執行設定"],
  ["diagnostics", "Diagnostics", "支援資料與健康檢查"],
] as const;

export const router = createRouter({
  history:
    typeof window === "undefined" ? createMemoryHistory() : createWebHistory(),
  routes: [
    {
      path: "/",
      name: "overview",
      component: () => import("@/views/OverviewView.vue"),
      meta: { label: "Overview", group: "control" },
    },
    ...sections.map(([name, label, summary]) => ({
      path: `/${name}`,
      name,
      component: () => import("@/views/SectionView.vue"),
      props: { label, summary },
      meta: { label, group: "operations" },
    })),
    {
      path: "/:pathMatch(.*)*",
      redirect: "/",
    },
  ],
});
