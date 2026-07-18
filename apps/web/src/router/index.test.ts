import { describe, expect, it } from "vitest";

import { router } from "./index";

describe("management web router", () => {
  it("exposes stable routes for the control shell", () => {
    expect(router.resolve("/").name).toBe("overview");
    expect(router.resolve("/meshtastic").name).toBe("meshtastic");
    expect(router.resolve("/aprs").name).toBe("aprs");
    expect(router.resolve("/callmesh").name).toBe("callmesh");
    expect(router.resolve("/logs").name).toBe("logs");
    expect(router.resolve("/missing").matched.at(-1)?.redirect).toBe("/");
  });
});
