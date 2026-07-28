import {
  createMemoryHistory,
  createRouter,
  createWebHistory,
  type RouteComponent,
  type Router,
  type RouterHistory,
} from "vue-router";

import type { SetupAdmission } from "@/stores/setup";

export interface SetupRouteGate {
  admission: SetupAdmission;
  refresh: () => Promise<unknown>;
  $subscribe: (listener: () => void) => () => void;
}

export function createManagementRouter(
  history: RouterHistory = typeof window === "undefined"
    ? createMemoryHistory()
    : createWebHistory(),
  setupComponent: RouteComponent = () => import("@/views/SetupWizardView.vue"),
) {
  return createRouter({
    history,
    routes: [
      {
        path: "/setup",
        name: "setup",
        component: setupComponent,
        meta: { setup: true },
      },
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
        path: "/proxy",
        name: "proxy",
        component: () => import("@/views/ProxyView.vue"),
        meta: { labelKey: "navigation.proxy", group: "operations" },
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
        path: "/updates",
        name: "updates",
        component: () => import("@/views/UpdatesView.vue"),
        meta: { labelKey: "navigation.updates", group: "operations" },
      },
      {
        path: "/settings",
        name: "settings",
        component: () => import("@/views/SettingsView.vue"),
        meta: { labelKey: "navigation.settings", group: "operations" },
      },
      {
        path: "/diagnostics",
        name: "diagnostics",
        component: () => import("@/views/DiagnosticsView.vue"),
        meta: { labelKey: "navigation.diagnostics", group: "operations" },
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
        path: "/remote-dispatch",
        name: "remote-dispatch",
        component: () => import("@/views/RemoteDispatchView.vue"),
        meta: { labelKey: "navigation.remoteDispatch", group: "operations" },
      },
      {
        path: "/telemetry",
        name: "telemetry",
        component: () => import("@/views/TelemetryView.vue"),
        meta: { labelKey: "navigation.telemetry", group: "operations" },
      },
      {
        path: "/:pathMatch(.*)*",
        redirect: "/",
      },
    ],
  });
}

export function installSetupRouteGate(
  targetRouter: Router,
  setup: SetupRouteGate,
): () => void {
  let intendedPath = "/";

  targetRouter.beforeEach(async (to) => {
    if (setup.admission === "checking") {
      try {
        await setup.refresh();
      } catch {
        // Do not misrepresent a temporary API failure as an incomplete setup.
      }
    }
    if (setup.admission === "required" && to.name !== "setup") {
      intendedPath = to.fullPath;
      return { name: "setup" };
    }
    if (setup.admission === "ready" && to.name === "setup") {
      const destination = intendedPath === "/setup" ? "/" : intendedPath;
      intendedPath = "/";
      return destination;
    }
    return true;
  });

  return setup.$subscribe(() => {
    const current = targetRouter.currentRoute.value;
    if (current.matched.length === 0) {
      return;
    }
    if (setup.admission === "required" && current.name !== "setup") {
      intendedPath = current.fullPath;
      void targetRouter.replace({ name: "setup" });
    } else if (setup.admission === "ready" && current.name === "setup") {
      const destination = intendedPath === "/setup" ? "/" : intendedPath;
      intendedPath = "/";
      void targetRouter.replace(destination);
    }
  });
}

export const router = createManagementRouter();
