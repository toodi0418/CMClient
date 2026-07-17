import { beforeEach, describe, expect, it } from "vitest";
import { createPinia, setActivePinia } from "pinia";

import { useShellStore } from "./shell";

describe("shell store", () => {
  beforeEach(() => setActivePinia(createPinia()));

  it("tracks navigation affordances and gateway availability", () => {
    const shell = useShellStore();

    shell.toggleDesktopRail();
    shell.toggleMobileNavigation();
    shell.setGatewayAvailability("available");
    shell.closeMobileNavigation();

    expect(shell.desktopRailCollapsed).toBe(true);
    expect(shell.mobileNavigationOpen).toBe(false);
    expect(shell.gatewayAvailability).toBe("available");
  });
});
