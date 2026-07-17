import { describe, expect, it } from "vitest";

import { fixedClock } from "./index";

describe("fixedClock", () => {
  it("returns a fresh deterministic date", () => {
    const clock = fixedClock("2030-01-02T03:04:05.000Z");
    expect(clock.now().toISOString()).toBe("2030-01-02T03:04:05.000Z");
    expect(clock.now()).not.toBe(clock.now());
  });
});
