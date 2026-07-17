import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

const tokens = readFileSync(new URL("./tokens.css", import.meta.url), "utf8");

describe("management web design tokens", () => {
  it("provides semantic CSS and Tailwind token names for core UI states", () => {
    for (const token of [
      "--cm-canvas",
      "--cm-surface",
      "--cm-text",
      "--cm-accent",
      "--cm-warning",
      "--cm-danger",
      "--cm-focus-ring",
      "--cm-surface-selected",
      "--color-cm-accent",
      "--radius-control",
    ]) {
      expect(tokens).toContain(token);
    }
  });
});
