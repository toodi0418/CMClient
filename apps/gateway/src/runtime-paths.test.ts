import { describe, expect, it } from "vitest";
import { join, normalize } from "node:path";

import { gatewayRuntimePaths } from "./runtime-paths";

const fixtureHome =
  process.platform === "win32" ? "C:/fixture-home" : "/fixture-home";
const campaignRoot =
  process.platform === "win32"
    ? "C:/campaign/home/.cmclient"
    : "/campaign/home/.cmclient";

describe("Gateway runtime paths", () => {
  it("uses the effective home .cmclient root and root-level database", () => {
    expect(
      gatewayRuntimePaths({
        HOME: fixtureHome,
        CMCLIENT_RUNTIME_PROFILE: "test",
      }),
    ).toEqual({
      root: normalize(join(fixtureHome, ".cmclient")),
      database: normalize(join(fixtureHome, ".cmclient/cmclient.db")),
      backups: normalize(join(fixtureHome, ".cmclient/backups")),
    });
  });

  it("accepts a custom root only with an explicit test flag", () => {
    expect(() =>
      gatewayRuntimePaths({
        HOME: fixtureHome,
        CMCLIENT_RUNTIME_ROOT:
          process.platform === "win32" ? "C:/foreign-root" : "/foreign-root",
      }),
    ).toThrowError("GATEWAY_RUNTIME_ROOT_INVALID");
    expect(
      gatewayRuntimePaths({
        HOME: fixtureHome,
        CMCLIENT_TEST_MODE: "1",
        CMCLIENT_RUNTIME_ROOT: campaignRoot,
      }).root,
    ).toBe(normalize(campaignRoot));
  });

  it("accepts only a complete absolute path set from the supervising Agent", () => {
    expect(
      gatewayRuntimePaths({
        CMCLIENT_SUPERVISED: "1",
        CMCLIENT_RUNTIME_ROOT: campaignRoot,
        CMCLIENT_DB_PATH: join(campaignRoot, "cmclient.db"),
        CMCLIENT_BACKUP_DIR: join(campaignRoot, "backups"),
      }).root,
    ).toBe(normalize(campaignRoot));
    expect(() =>
      gatewayRuntimePaths({
        CMCLIENT_SUPERVISED: "1",
        CMCLIENT_RUNTIME_ROOT: campaignRoot,
      }),
    ).toThrowError("GATEWAY_RUNTIME_PATHS_REQUIRED");
  });

  it("pins Docker to the documented root", () => {
    if (process.platform === "win32") return;
    expect(
      gatewayRuntimePaths({
        HOME: "/not-used",
        CMCLIENT_RUNTIME_PROFILE: "docker",
        CMCLIENT_RUNTIME_ROOT: "/home/cmclient/.cmclient",
      }),
    ).toEqual({
      root: "/home/cmclient/.cmclient",
      database: "/home/cmclient/.cmclient/cmclient.db",
      backups: "/home/cmclient/.cmclient/backups",
    });
    expect(() =>
      gatewayRuntimePaths({
        CMCLIENT_RUNTIME_PROFILE: "docker",
        CMCLIENT_RUNTIME_ROOT: "/var/lib/cmclient",
      }),
    ).toThrowError("GATEWAY_RUNTIME_ROOT_INVALID");
  });

  it("rejects paths outside the root", () => {
    expect(() =>
      gatewayRuntimePaths({
        HOME: fixtureHome,
        CMCLIENT_DB_PATH:
          process.platform === "win32"
            ? "C:/other/cmclient.db"
            : "/other/cmclient.db",
      }),
    ).toThrowError("GATEWAY_RUNTIME_CHILD_OUTSIDE_ROOT");
  });

  it("rejects legacy split-root environment variables", () => {
    expect(() =>
      gatewayRuntimePaths({
        HOME: fixtureHome,
        CMCLIENT_DATA_DIR: join(fixtureHome, "data"),
      }),
    ).toThrowError("GATEWAY_RUNTIME_LEGACY_ROOT_REJECTED");
  });
});
