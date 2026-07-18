import { expect, test, type Page } from "@playwright/test";

const build = {
  version: "2.0.0-e2e",
  commit: "e2e-fixture",
  channel: "dev",
};
const consoleErrors = new WeakMap<Page, string[]>();

test.beforeEach(async ({ page }) => {
  const errors: string[] = [];
  consoleErrors.set(page, errors);
  page.on("console", (message) => {
    if (message.type() === "error") {
      errors.push(message.text());
    }
  });
  await page.addInitScript(() => {
    window.localStorage.setItem(
      "cmclient.web.preferences.v1",
      JSON.stringify({ theme: "light", locale: "en-US" }),
    );
  });
  await mockGateway(page);
});

test.afterEach(async ({ page }) => {
  expect(consoleErrors.get(page)).toEqual([]);
});

test("settings and update status remain usable on desktop", async ({
  page,
}) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("/settings");

  const workspace = page.locator("main");
  await expect(
    workspace.getByRole("heading", { name: "Settings" }),
  ).toBeVisible();
  await workspace.getByRole("button", { name: "Dark" }).click();
  await expect(page.locator("html")).toHaveClass(/cm-dark/);
  await workspace
    .getByRole("combobox", { name: "Language" })
    .selectOption("zh-TW");
  await expect(page.locator("html")).toHaveAttribute("lang", "zh-Hant-TW");

  await page.getByRole("link", { name: "更新" }).click();
  await expect(
    workspace.getByRole("heading", { name: "更新", exact: true }),
  ).toBeVisible();
  await expect(workspace.getByText("CAPABILITY_OWNED_BY_AGENT")).toBeVisible();
  await expectNoHorizontalOverflow(page);
});

test("diagnostics job and navigation stay usable on mobile", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.route("**/api/v1/diagnostics/integrity-check", (route) =>
    route.fulfill({
      status: 202,
      contentType: "application/json",
      body: JSON.stringify({ jobId: "diagnostics-e2e", reused: false }),
    }),
  );
  await page.route("**/api/v1/jobs/diagnostics-e2e", (route) =>
    route.fulfill({
      contentType: "application/json",
      body: JSON.stringify({
        id: "diagnostics-e2e",
        type: "diagnostics.integrity_check",
        status: "succeeded",
        createdAt: "2026-07-18T00:00:00.000Z",
        updatedAt: "2026-07-18T00:00:01.000Z",
        completedAt: "2026-07-18T00:00:01.000Z",
      }),
    }),
  );
  await page.goto("/diagnostics");

  const workspace = page.locator("main");
  const accepted = page.waitForResponse(
    "**/api/v1/diagnostics/integrity-check",
  );
  const completed = page.waitForResponse("**/api/v1/jobs/diagnostics-e2e");
  await workspace.getByRole("button", { name: "Run check" }).click();
  await expect((await accepted).status()).toBe(202);
  await expect((await completed).status()).toBe(200);
  await expect(workspace.getByText("Succeeded")).toBeVisible();
  await page.getByRole("button", { name: "Open navigation" }).click();
  await expect(page.getByRole("link", { name: "Updates" })).toBeVisible();
  await expectNoHorizontalOverflow(page);
});

test("proxy runtime status remains legible across desktop and mobile", async ({
  page,
}) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("/proxy");

  const workspace = page.locator("main");
  await expect(
    workspace.getByRole("heading", { name: "TCP Proxy", level: 2 }),
  ).toBeVisible();
  await expect(workspace.getByText("127.0.0.1:4403")).toBeVisible();
  await expect(workspace.getByText("Ready", { exact: true })).toBeVisible();
  await expect(workspace.getByText("2 / 16")).toBeVisible();
  await expectNoHorizontalOverflow(page);

  await page.setViewportSize({ width: 390, height: 844 });
  await expect(workspace.getByText("127.0.0.1:4403")).toBeVisible();
  await expectNoHorizontalOverflow(page);
});

async function mockGateway(page: Page): Promise<void> {
  await page.route("**/api/v1/system/status", (route) =>
    route.fulfill({
      contentType: "application/json",
      body: JSON.stringify({ health: "ok", build }),
    }),
  );
  await page.route("**/api/v1/system/capabilities", (route) =>
    route.fulfill({
      contentType: "application/json",
      body: JSON.stringify({
        schemaVersion: 1,
        platform: "linux",
        build,
        capabilities: {
          managementWeb: {
            available: false,
            reasonCode: "CAPABILITY_OWNED_BY_AGENT",
          },
          update: { available: false, reasonCode: "CAPABILITY_OWNED_BY_AGENT" },
          tray: { available: false, reasonCode: "CAPABILITY_OWNED_BY_DESKTOP" },
          serial: { available: false, reasonCode: "CAPABILITY_NOT_CONFIGURED" },
          service: {
            available: false,
            reasonCode: "CAPABILITY_OWNED_BY_AGENT",
          },
          autoStart: {
            available: false,
            reasonCode: "CAPABILITY_OWNED_BY_AGENT",
          },
          docker: { available: true },
        },
      }),
    }),
  );
  await page.route("**/api/v1/events", (route) =>
    route.fulfill({
      contentType: "text/event-stream",
      body: ": heartbeat\n\n",
    }),
  );
  await page.route("**/api/v1/proxy", (route) =>
    route.fulfill({
      contentType: "application/json",
      body: JSON.stringify({
        state: "running",
        listener: { host: "127.0.0.1", port: 4403 },
        policy: {
          activeClients: 2,
          allowLan: false,
          allowedAddressCount: 0,
          maxClients: 16,
          maxWritesPerMinute: 120,
          mode: "message",
        },
        queue: {
          broadcastAccepted: 4,
          broadcastDropped: 1,
          broadcastFrames: 2,
          directAccepted: 1,
          directDropped: 0,
          pendingCorrelations: 0,
          queuedWrites: 0,
          writing: false,
        },
        recentAudit: [
          {
            action: "write_allowed",
            clientFingerprint: "0123456789abcdef",
            mode: "message",
            occurredAt: "2026-07-18T00:00:00.000Z",
            variant: "packet",
          },
        ],
        upstream: {
          configFrameCount: 3,
          metrics: {
            bytesReceived: 10,
            bytesSent: 8,
            framesReceived: 2,
            framesSent: 1,
            malformedFrames: 0,
            reconnects: 0,
          },
          state: {
            changedAt: "2026-07-18T00:00:00.000Z",
            status: "ready",
            transport: "tcp",
          },
        },
      }),
    }),
  );
}

async function expectNoHorizontalOverflow(page: Page): Promise<void> {
  await expect
    .poll(() =>
      page.evaluate(
        () => document.documentElement.scrollWidth <= window.innerWidth,
      ),
    )
    .toBe(true);
}
