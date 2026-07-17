import { defineStore } from "pinia";

export type GatewayAvailability = "checking" | "available" | "unavailable";

export const useShellStore = defineStore("shell", {
  state: () => ({
    desktopRailCollapsed: false,
    mobileNavigationOpen: false,
    gatewayAvailability: "checking" as GatewayAvailability,
  }),
  actions: {
    setGatewayAvailability(availability: GatewayAvailability) {
      this.gatewayAvailability = availability;
    },
    toggleDesktopRail() {
      this.desktopRailCollapsed = !this.desktopRailCollapsed;
    },
    toggleMobileNavigation() {
      this.mobileNavigationOpen = !this.mobileNavigationOpen;
    },
    closeMobileNavigation() {
      this.mobileNavigationOpen = false;
    },
  },
});
