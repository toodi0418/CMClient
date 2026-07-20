import { describe, expect, it } from "vitest";

import { parseProductIdentity, parseRuntimeConfig } from "./index";

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

describe("parseProductIdentity", () => {
  it("uses one exact build and target identity", () => {
    expect(
      parseProductIdentity(
        {
          CMCLIENT_BUILD_VERSION: "2.0.0",
          CMCLIENT_BUILD_COMMIT: "a".repeat(40),
          CMCLIENT_BUILD_TREE: "b".repeat(40),
          CMCLIENT_BUILD_CHANNEL: "candidate",
          CMCLIENT_TARGET_OS: "windows",
          CMCLIENT_TARGET_ARCHITECTURE: "x86_64",
          CMCLIENT_RUNTIME_PROFILE: "native",
          CMCLIENT_PACKAGE_PROFILE: "setup",
        },
        {
          version: "2.0.0-dev.0",
          sourceCommit: "c".repeat(40),
          sourceTree: "d".repeat(40),
          channel: "dev",
          target: {
            os: "linux",
            architecture: "x86_64",
            profile: "native",
            packageProfile: "workspace",
          },
        },
      ),
    ).toEqual({
      schemaVersion: 1,
      product: "CMClient",
      version: "2.0.0",
      sourceCommit: "a".repeat(40),
      sourceTree: "b".repeat(40),
      channel: "candidate",
      target: {
        os: "windows",
        architecture: "x86_64",
        profile: "native",
        packageProfile: "setup",
      },
    });
  });

  it("rejects legacy beta and unsupported Windows ARM64 identities", () => {
    const defaults = {
      version: "2.0.0-dev.0",
      sourceCommit: "a".repeat(40),
      sourceTree: "b".repeat(40),
      channel: "dev" as const,
      target: {
        os: "windows" as const,
        architecture: "x86_64" as const,
        profile: "native" as const,
        packageProfile: "workspace" as const,
      },
    };
    expect(() =>
      parseProductIdentity({ CMCLIENT_BUILD_CHANNEL: "beta" }, defaults),
    ).toThrow("BUILD_IDENTITY_INVALID");
    expect(() =>
      parseProductIdentity(
        { CMCLIENT_TARGET_ARCHITECTURE: "aarch64" },
        defaults,
      ),
    ).toThrow("BUILD_IDENTITY_INVALID");
  });
});
