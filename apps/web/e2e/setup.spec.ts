import { expect, test, type Page } from "@playwright/test";
import { CURRENT_TERMS_VERSION } from "@cmclient/contracts";

const termsStatus = {
  schemaVersion: 1,
  currentTermsVersion: CURRENT_TERMS_VERSION,
  phase: "terms_required",
  setupRequired: true,
  termsRequired: true,
  credentialsRequired: false,
  validating: false,
  ready: false,
  recoveryRequired: false,
  reasonCode: "SETUP_TERMS_REQUIRED",
};
const credentialsStatus = {
  ...termsStatus,
  phase: "credentials_required",
  termsRequired: false,
  credentialsRequired: true,
  reasonCode: "SETUP_CREDENTIALS_REQUIRED",
};
const readyStatus = {
  ...credentialsStatus,
  phase: "ready",
  setupRequired: false,
  credentialsRequired: false,
  ready: true,
  reasonCode: "SETUP_READY",
};

test("setup is route-gated, keyboard operable, responsive, and credential-safe", async ({
  page,
}) => {
  let status = termsStatus;
  let configureBody = "";
  await page.addInitScript(() => {
    window.localStorage.setItem(
      "cmclient.web.preferences.v1",
      JSON.stringify({ theme: "light", locale: "en-US" }),
    );
  });
  await mockSetup(
    page,
    () => status,
    (next) => {
      status = next;
    },
    (body) => {
      configureBody = body;
    },
  );

  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("/settings");
  await expect(page).toHaveURL(/\/setup$/);
  await expect(
    page.getByRole("heading", { name: "Connect your CMClient to CallMesh" }),
  ).toBeVisible();

  await page.getByRole("checkbox").check();
  await page.getByRole("button", { name: "Continue" }).click();
  await expect(
    page.getByRole("heading", { name: "Choose your Meshtastic node" }),
  ).toBeVisible();

  await page.getByRole("button", { name: "Review configuration" }).click();
  await expect(
    page.getByText("Enter a valid LAN host name or IP address."),
  ).toBeVisible();
  await expect(page.getByText("CallMesh API key is required.")).toBeVisible();

  await page.getByLabel("Meshtastic host").fill("192.0.2.10");
  const keyInput = page.getByLabel("CallMesh API key");
  await expect(keyInput).toHaveAttribute("autocomplete", "off");
  await keyInput.fill("fixture-private-setup-key");
  await keyInput.press("Enter");

  await expect(
    page.getByRole("heading", { name: "Confirm before validation" }),
  ).toBeVisible();
  await expect(page.getByText("Provided; value hidden")).toBeVisible();
  await expect(page.locator("body")).not.toContainText(
    "fixture-private-setup-key",
  );
  expect(await page.locator('input[type="password"]').count()).toBe(0);
  expect(
    await page.evaluate(() => document.documentElement.scrollWidth),
  ).toBeLessThanOrEqual(390);

  await page.getByRole("button", { name: "Validate and start" }).click();
  await expect(page).toHaveURL(/\/settings$/);
  await expect(page.getByRole("heading", { name: "Settings" })).toBeVisible();

  expect(configureBody).toContain("fixture-private-setup-key");
  expect(page.url()).not.toContain("fixture-private-setup-key");
  expect(
    await page.evaluate(() => JSON.stringify({ ...window.localStorage })),
  ).not.toContain("fixture-private-setup-key");
  expect(
    await page.evaluate(() => JSON.stringify({ ...window.sessionStorage })),
  ).not.toContain("fixture-private-setup-key");
});

async function mockSetup(
  page: Page,
  getStatus: () => typeof termsStatus,
  setStatus: (status: typeof termsStatus) => void,
  captureConfigureBody: (body: string) => void,
) {
  await page.route("**/api/v1/**", (route) =>
    route.fulfill({
      status: 503,
      contentType: "application/json",
      body: '{"code":"FIXTURE_UNAVAILABLE"}',
    }),
  );
  await page.route("**/api/v1/setup/status", (route) =>
    route.fulfill({
      contentType: "application/json",
      body: JSON.stringify(getStatus()),
    }),
  );
  await page.route("**/api/v1/setup/events", (route) =>
    route.fulfill({
      contentType: "text/event-stream",
      body: ": heartbeat\n\n",
    }),
  );
  await page.route("**/api/v1/setup/terms", async (route) => {
    setStatus(credentialsStatus);
    await route.fulfill({
      contentType: "application/json",
      body: JSON.stringify(credentialsStatus),
    });
  });
  await page.route("**/api/v1/setup/discovery", (route) =>
    route.fulfill({
      contentType: "application/json",
      body: JSON.stringify({
        schemaVersion: 1,
        candidates: [{ host: "192.0.2.10", port: 4403, source: "mdns" }],
        callmeshUrl: "https://callmesh.tmmarc.org",
      }),
    }),
  );
  await page.route("**/api/v1/setup/configure", async (route) => {
    captureConfigureBody(route.request().postData() ?? "");
    setStatus(readyStatus);
    await route.fulfill({
      contentType: "application/json",
      body: JSON.stringify(readyStatus),
    });
  });
  await page.route("**/api/v1/auth/session", (route) =>
    route.fulfill({
      contentType: "application/json",
      body: JSON.stringify({
        schemaVersion: 1,
        csrfToken: "a".repeat(32),
        expiresAt: 1_800_000_000,
      }),
    }),
  );
}
