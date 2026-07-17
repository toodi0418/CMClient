import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

const document = readFileSync(
  new URL("../../index.html", import.meta.url),
  "utf8",
);

describe("theme startup document", () => {
  it("applies saved or system preferences before the application entry point", () => {
    const bootstrap = document.indexOf("cmclient.web.preferences.v1");
    const application = document.indexOf("/src/main.ts");

    expect(bootstrap).toBeGreaterThan(-1);
    expect(application).toBeGreaterThan(bootstrap);
    expect(document).toContain('matchMedia("(prefers-color-scheme: dark)")');
    expect(document).toContain('root.classList.toggle("cm-dark"');
  });
});
