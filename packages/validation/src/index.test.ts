import { describe, expect, it } from "vitest";

import { boundedInteger, requiredString } from "./index";

describe("validation helpers", () => {
  it("returns stable codes instead of prose", () => {
    expect(requiredString("", "name")).toEqual([
      { path: "name", code: "VALIDATION_REQUIRED_STRING" }
    ]);
    expect(boundedInteger(70_000, "port", 1, 65_535)).toEqual([
      { path: "port", code: "VALIDATION_BOUNDED_INTEGER" }
    ]);
  });
});
