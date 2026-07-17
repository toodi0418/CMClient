import { describe, expect, it } from "vitest";

import { JOB_STATUSES } from "./index";

describe("job contracts", () => {
  it("includes terminal rollback states", () => {
    expect(JOB_STATUSES).toContain("rolled_back");
    expect(JOB_STATUSES).toContain("failed");
  });
});
