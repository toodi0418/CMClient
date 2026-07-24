import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

const document = readFileSync(
  new URL("../../index.html", import.meta.url),
  "utf8",
);
const bootstrapSource = readFileSync(
  new URL("../../public/theme-startup.js", import.meta.url),
  "utf8",
);

describe("theme startup document", () => {
  it("applies saved or system preferences before the application entry point", () => {
    const bootstrap = document.indexOf("/theme-startup.js");
    const application = document.indexOf("/src/main.ts");

    expect(bootstrap).toBeGreaterThan(-1);
    expect(application).toBeGreaterThan(bootstrap);
    expect(document).not.toMatch(/<script(?:\s[^>]*)?>\s*[^<\s]/);
    expect(bootstrapSource).toContain(
      'matchMedia("(prefers-color-scheme: dark)")',
    );
    expect(bootstrapSource).toContain('root.classList.toggle("cm-dark"');
  });
});
