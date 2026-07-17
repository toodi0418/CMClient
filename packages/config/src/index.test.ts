import { describe, expect, it } from "vitest";

import { parseRuntimeConfig } from "./index";

describe("parseRuntimeConfig", () => {
  it("uses safe defaults for invalid ports", () => {
    expect(parseRuntimeConfig({ CMCLIENT_MANAGEMENT_PORT: "70000" })).toMatchObject({
      managementHost: "127.0.0.1",
      managementPort: 7080,
      webEnabled: true
    });
  });

  it("recognizes explicit false values", () => {
    expect(parseRuntimeConfig({ CMCLIENT_WEB_ENABLED: "off" }).webEnabled).toBe(false);
  });
});
