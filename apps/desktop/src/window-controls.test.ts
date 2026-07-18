import { describe, expect, it } from "vitest";

import { runWindowControl, type WindowControlTarget } from "./window-controls";

function target(calls: string[]): WindowControlTarget {
  return {
    close: async () => {
      calls.push("close");
    },
    minimize: async () => {
      calls.push("minimize");
    },
    hide: async () => {
      calls.push("hide");
    },
  };
}

describe("runWindowControl", () => {
  it("maps each titlebar action to one native window operation", async () => {
    const calls: string[] = [];
    const window = target(calls);

    await runWindowControl(window, "exit");
    await runWindowControl(window, "minimize");
    await runWindowControl(window, "hide");

    expect(calls).toEqual(["close", "minimize", "hide"]);
  });
});
