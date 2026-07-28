import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

const source = readFileSync(
  new URL("./SetupWizardView.vue", import.meta.url),
  "utf8",
);

describe("setup wizard static security and accessibility contract", () => {
  it("uses PrimeVue Forms and clears the transient credential before awaiting", () => {
    expect(source).toContain('from "@primevue/forms/form"');
    expect(source).toContain('pendingCredential = "";');
    expect(source).toMatch(
      /pendingCredential = "";[\s\S]*operation = setup\.configure\(request\);[\s\S]*request\.callmeshApiKey = "";/,
    );
    expect(source).toContain('autocomplete="off"');
    expect(source).not.toMatch(
      /localStorage|sessionStorage|console\.(?:log|error)/,
    );
  });

  it("keeps native keyboard submission and mobile layout contracts explicit", () => {
    expect(source).toContain('type="submit"');
    expect(source).toContain("@media (max-width: 600px)");
    expect(source).toContain("grid-template-columns: 1fr");
    expect(source).not.toMatch(
      /const meshtasticHost\s*=\s*ref\(["'](?:127\.0\.0\.1|::1|localhost)/i,
    );
  });
});
