import { describe, expect, it } from "vitest";

import { parseBuildMetadata, parseRuntimeConfig } from "./index";

describe("parseRuntimeConfig", () => {
  it("uses safe defaults for invalid ports", () => {
    expect(
      parseRuntimeConfig({ CMCLIENT_MANAGEMENT_PORT: "70000" }),
    ).toMatchObject({
      managementHost: "127.0.0.1",
      managementPort: 7080,
      webEnabled: true,
    });
  });

  it("recognizes explicit false values", () => {
    expect(parseRuntimeConfig({ CMCLIENT_WEB_ENABLED: "off" }).webEnabled).toBe(
      false,
    );
  });
});

describe("parseBuildMetadata", () => {
  it("uses CI metadata without treating it as runtime configuration", () => {
    expect(
      parseBuildMetadata(
        {
          CMCLIENT_BUILD_VERSION: "2.0.0",
          CMCLIENT_BUILD_COMMIT: "deadbeef",
          CMCLIENT_BUILD_TIMESTAMP: "2030-01-02T03:04:05.000Z",
          CMCLIENT_RELEASE_CHANNEL: "beta",
        },
        { version: "2.0.0-dev.0", commit: "unknown" },
      ),
    ).toEqual({
      version: "2.0.0",
      commit: "deadbeef",
      channel: "beta",
      builtAt: "2030-01-02T03:04:05.000Z",
    });
  });
});
