import { expect, test, type Page, type Route } from "@playwright/test";

const identity = {
  schemaVersion: 1,
  component: "gateway",
  identity: {
    schemaVersion: 1,
    product: "CMClient",
    version: "2.0.0-e2e",
    sourceCommit: "a".repeat(40),
    sourceTree: "b".repeat(40),
    channel: "dev",
    target: {
      os: "linux",
      architecture: "x86_64",
      profile: "native",
      packageProfile: "workspace",
    },
  },
};
const setupReady = {
  schemaVersion: 1,
  currentTermsVersion: "cmclient-2.0-terms-v1",
  phase: "ready",
  setupRequired: false,
  termsRequired: false,
  credentialsRequired: false,
  validating: false,
  ready: true,
  recoveryRequired: false,
  reasonCode: "SETUP_READY",
};
const updateStatus = {
  schemaVersion: 1,
  job: {
    id: "update-e2e",
    phase: "downloading",
    updatedAt: "2026-07-18T00:00:00.000Z",
    errorCode: null,
    bytesDownloaded: 524288,
    bytesTotal: 1048576,
    bytesPerSecond: 262144,
    recentLogCodes: ["UPDATE_DOWNLOAD_STARTED", "UPDATE_SIGNATURE_VERIFIED"],
  },
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
  await expect(workspace.getByText("下載中", { exact: true })).toBeVisible();
  await expect(workspace.getByText("512 KiB / 1 MiB")).toBeVisible();
  await expect(workspace.getByText("已驗證更新簽章")).toBeVisible();
  await expect(workspace.getByText("由 Agent 管理")).toBeVisible();
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

test("Meshtastic and APRS runtime projections remain operable on mobile", async ({
  page,
}) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("/meshtastic");

  const workspace = page.locator("main");
  await expect(
    workspace.getByRole("heading", { name: "Radio runtime" }),
  ).toBeVisible();
  await expect(workspace.getByText("Ready", { exact: true })).toBeVisible();
  await expect(workspace.getByText("mesh-e2e", { exact: true })).toBeVisible();
  await expect(workspace.getByText("12 / 4", { exact: true })).toBeVisible();

  await page.getByRole("link", { name: "APRS" }).click();
  await expect(
    workspace.getByRole("heading", { name: "APRS-IS runtime" }),
  ).toBeVisible();
  await expect(workspace.getByText("Connected", { exact: true })).toBeVisible();
  await expect(workspace.getByText("N0CALL-7", { exact: true })).toBeVisible();

  await page.setViewportSize({ width: 390, height: 844 });
  await expectNoHorizontalOverflow(page);
});

test("offline positions and telemetry render real visual layers without overflow", async ({
  page,
}) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("/positions");

  const map = page.getByRole("application", { name: "Coordinates" });
  await expect(map).toBeVisible();
  await expect(map.locator(".offline-map-tile")).not.toHaveCount(0);
  await expect(map.locator(".leaflet-interactive")).toHaveCount(3);

  await page.getByRole("link", { name: "Telemetry" }).click();
  const chart = page.locator(".telemetry-chart");
  const canvas = chart.locator("canvas");
  await expect(chart).toBeVisible();
  await expect(canvas).toHaveCount(1);
  await expect
    .poll(() =>
      canvas.evaluate((canvas: HTMLCanvasElement) => {
        const context = canvas.getContext("2d");
        if (!context || canvas.width === 0 || canvas.height === 0) return 0;
        const pixels = context.getImageData(
          0,
          0,
          canvas.width,
          canvas.height,
        ).data;
        let painted = 0;
        for (let index = 3; index < pixels.length; index += 64) {
          if (pixels[index] !== 0) painted += 1;
        }
        return painted;
      }),
    )
    .toBeGreaterThan(20);

  await page.setViewportSize({ width: 390, height: 844 });
  await expect(chart).toBeVisible();
  await expectNoHorizontalOverflow(page);
});

test("domain SSE events trigger a coalesced telemetry projection refresh", async ({
  page,
}) => {
  let telemetryRequests = 0;
  let sentEvent = false;
  await page.unroute("**/api/v1/telemetry");
  await page.unroute("**/api/v1/events");
  await page.route("**/api/v1/telemetry", (route) => {
    telemetryRequests += 1;
    return route.fulfill({
      contentType: "application/json",
      body: JSON.stringify({
        items: telemetryFixture(telemetryRequests === 1 ? 3.8 : 4.2),
      }),
    });
  });
  await page.route("**/api/v1/events", async (route) => {
    if (!sentEvent) {
      sentEvent = true;
      await new Promise((resolve) => setTimeout(resolve, 150));
      const event = {
        eventId: "telemetry-e2e",
        schemaVersion: 1,
        type: "telemetry.received",
        occurredAt: "2026-07-18T00:01:00.000Z",
        source: "gateway",
        payload: { nodeNum: 42 },
      };
      return route.fulfill({
        contentType: "text/event-stream",
        body: `id: ${event.eventId}\nevent: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`,
      });
    }
    return route.fulfill({
      contentType: "text/event-stream",
      body: ": heartbeat\n\n",
    });
  });

  await page.goto("/telemetry");
  await expect.poll(() => telemetryRequests).toBeGreaterThanOrEqual(2);
  await expect(page.locator(".telemetry-chart canvas")).toBeVisible();
  await expect(page.getByRole("img", { name: /4\.2/ })).toBeVisible();
});

test("remote dispatch stays visibly fail-closed behind its capability", async ({
  page,
}) => {
  await page.goto("/remote-dispatch");
  await expect(page.getByRole("link", { name: "Remote Dispatch" })).toHaveCount(
    0,
  );
  await expect(
    page.getByRole("heading", { name: "Dispatch service status" }),
  ).toBeVisible();
  await expect(
    page.getByText("This feature is not available in this release"),
  ).toBeVisible();
});

test("a temporary setup rate limit preserves a deep link and recovers", async ({
  page,
}) => {
  let setupRequests = 0;
  await page.unroute("**/api/v1/setup/status");
  await page.route("**/api/v1/setup/status", (route) => {
    setupRequests += 1;
    return route.fulfill(
      setupRequests === 1
        ? {
            status: 429,
            contentType: "application/json",
            body: JSON.stringify({ code: "MANAGEMENT_REQUEST_RATE_LIMITED" }),
          }
        : {
            contentType: "application/json",
            body: JSON.stringify(setupReady),
          },
    );
  });

  await page.goto("/aprs");
  await expect(page).toHaveURL(/\/aprs$/);
  await expect(
    page.getByRole("heading", {
      name: "The management page cannot be updated yet",
    }),
  ).toBeVisible();
  await expect(
    page.getByText("CMClient is handling several requests"),
  ).toBeVisible();
  await expect(
    page.getByText("MANAGEMENT_REQUEST_RATE_LIMITED"),
  ).not.toBeVisible();
  // The preceding 429 is intentional and has been verified through the
  // human-facing recovery state. Keep the rest of this test strict about
  // unexpected browser-console errors.
  consoleErrors.set(page, []);
  await expect(
    page.getByRole("heading", { name: "APRS-IS runtime" }),
  ).toBeVisible();
  await expect(page).toHaveURL(/\/aprs$/);
});

test("a pending setup request still renders the connection recovery state", async ({
  page,
}) => {
  await page.unroute("**/api/v1/setup/status");
  await page.route(
    "**/api/v1/setup/status",
    () => new Promise<never>(() => undefined),
  );

  await page.goto("/aprs");
  await expect(
    page.getByRole("heading", {
      name: "Checking local services",
    }),
  ).toBeVisible();
});

test("a temporary local-session rate limit recovers without showing login", async ({
  page,
}) => {
  let sessions = 0;
  await page.unroute("**/api/v1/auth/session");
  await page.route("**/api/v1/auth/session", (route) => {
    sessions += 1;
    return route.fulfill(
      sessions === 1
        ? {
            status: 429,
            contentType: "application/json",
            body: JSON.stringify({ code: "MANAGEMENT_REQUEST_RATE_LIMITED" }),
          }
        : {
            contentType: "application/json",
            body: JSON.stringify({
              schemaVersion: 1,
              csrfToken: "a".repeat(32),
              expiresAt: 1_784_344_000,
            }),
          },
    );
  });

  await page.goto("/diagnostics");
  await expect(
    page.getByRole("heading", {
      name: "The management page cannot be updated yet",
    }),
  ).toBeVisible();
  await expect(
    page.getByText("CMClient is handling several requests"),
  ).toBeVisible();
  await expect(
    page.getByText("MANAGEMENT_REQUEST_RATE_LIMITED"),
  ).not.toBeVisible();
  consoleErrors.set(page, []);
  await expect(
    page.getByRole("heading", { name: "Diagnostics" }),
  ).toBeVisible();
});

test("APRS combines a shared temporary failure into one clear recovery state", async ({
  page,
}) => {
  const rateLimited = (route: Route) =>
    route.fulfill({
      status: 429,
      contentType: "application/json",
      body: JSON.stringify({ code: "MANAGEMENT_REQUEST_RATE_LIMITED" }),
    });

  await page.unroute("**/api/v1/aprs");
  await page.unroute("**/api/v1/aprs/outbox");
  await page.unroute("**/api/v1/aprs/station-submissions");
  await page.route("**/api/v1/aprs", rateLimited);
  await page.route("**/api/v1/aprs/outbox", rateLimited);
  await page.route("**/api/v1/aprs/station-submissions", rateLimited);

  await page.goto("/aprs");
  const notice = page.locator(".page-problem-notice");
  await expect(notice).toHaveCount(1);
  await expect(notice).toContainText("CMClient is handling several requests");
  await expect(notice.locator("code")).not.toBeVisible();
  consoleErrors.set(page, []);
});

test("LAN management login unlocks protected commands with the CSRF token", async ({
  page,
}) => {
  let authenticated = false;
  let protectedHeader: string | null = null;
  let loginBody: string | null = null;
  await page.route("**/api/v1/system/status", (route) =>
    route.fulfill({
      status: authenticated ? 200 : 401,
      contentType: "application/json",
      body: JSON.stringify(
        authenticated
          ? { schemaVersion: 2, health: "ok", identity }
          : { code: "MANAGEMENT_SESSION_INVALID" },
      ),
    }),
  );
  await page.route("**/api/v1/auth/login", async (route) => {
    loginBody = route.request().postData() ?? null;
    authenticated = true;
    await route.fulfill({
      contentType: "application/json",
      body: JSON.stringify({
        schemaVersion: 1,
        csrfToken: "a".repeat(32),
        expiresAt: 1_784_344_000,
      }),
    });
  });
  await page.route("**/api/v1/diagnostics/integrity-check", async (route) => {
    protectedHeader = route.request().headers()["x-csrf-token"] ?? null;
    await route.fulfill({
      status: 202,
      contentType: "application/json",
      body: JSON.stringify({ jobId: "diagnostics-lan", reused: false }),
    });
  });
  await page.route("**/api/v1/jobs/diagnostics-lan", (route) =>
    route.fulfill({
      contentType: "application/json",
      body: JSON.stringify({
        id: "diagnostics-lan",
        type: "diagnostics.integrity_check",
        status: "succeeded",
        createdAt: "2026-07-18T00:00:00.000Z",
        updatedAt: "2026-07-18T00:00:01.000Z",
        completedAt: "2026-07-18T00:00:01.000Z",
      }),
    }),
  );

  await page.goto("/diagnostics");
  await expect(
    page.getByRole("heading", { name: "Management sign in" }),
  ).toBeVisible();
  consoleErrors.set(page, []);
  await page.getByLabel("Password").fill("correct-password");
  await page.getByRole("button", { name: "Sign in" }).click();

  const workspace = page.locator("main.workspace");
  await expect(
    workspace.getByRole("heading", { name: "Diagnostics" }),
  ).toBeVisible();
  expect(loginBody).toBe('{"password":"correct-password"}');
  await workspace.getByRole("button", { name: "Run check" }).click();
  await expect.poll(() => protectedHeader).toBe("a".repeat(32));
  await expectNoHorizontalOverflow(page);
  expect(
    await page.evaluate(() =>
      window.localStorage.getItem("cmclient.web.preferences.v1"),
    ),
  ).not.toContain("correct-password");
});

async function mockGateway(page: Page): Promise<void> {
  await page.route("**/api/v1/auth/session", (route) =>
    route.fulfill({
      contentType: "application/json",
      body: JSON.stringify({
        schemaVersion: 1,
        csrfToken: "a".repeat(32),
        expiresAt: 1_784_344_000,
      }),
    }),
  );
  await page.route("**/api/v1/setup/status", (route) =>
    route.fulfill({
      contentType: "application/json",
      body: JSON.stringify(setupReady),
    }),
  );
  await page.route("**/api/v1/updates", (route) =>
    route.fulfill({
      contentType: "application/json",
      body: JSON.stringify(updateStatus),
    }),
  );
  await page.route("**/api/v1/updates/events", (route) =>
    route.fulfill({
      contentType: "text/event-stream",
      body:
        "id: update-e2e\nevent: update.status_changed\ndata: " +
        JSON.stringify(updateStatus) +
        "\n\n",
    }),
  );
  await page.route("**/api/v1/system/status", (route) =>
    route.fulfill({
      contentType: "application/json",
      body: JSON.stringify({ schemaVersion: 2, health: "ok", identity }),
    }),
  );
  await page.route("**/api/v1/system/capabilities", (route) =>
    route.fulfill({
      contentType: "application/json",
      body: JSON.stringify({
        schemaVersion: 2,
        identity,
        capabilities: {
          managementWeb: { available: false, reasonCode: "owned_by_agent" },
          commandMode: { available: false, reasonCode: "owned_by_agent" },
          graphicalMode: {
            available: false,
            reasonCode: "owned_by_graphical_mode",
          },
          loginAutostart: { available: false, reasonCode: "owned_by_agent" },
          serial: { available: false, reasonCode: "not_configured" },
          nativeUpdate: { available: false, reasonCode: "owned_by_agent" },
          dockerPullRecreateUpdate: {
            available: false,
            reasonCode: "unavailable_in_native",
          },
          localControl: { available: false, reasonCode: "owned_by_agent" },
          remoteDispatch: { available: false, reasonCode: "not_enabled" },
        },
      }),
    }),
  );
  await page.route("**/api/v1/setup/events", (route) =>
    route.fulfill({
      contentType: "text/event-stream",
      body: ": heartbeat\n\n",
    }),
  );
  await page.route("**/api/v1/events", (route) =>
    route.fulfill({
      contentType: "text/event-stream",
      body: ": heartbeat\n\n",
    }),
  );
  await page.route("**/api/v1/nodes", (route) =>
    route.fulfill({ contentType: "application/json", body: '{"items":[]}' }),
  );
  await page.route("**/api/v1/messages", (route) =>
    route.fulfill({ contentType: "application/json", body: '{"items":[]}' }),
  );
  await page.route("**/api/v1/positions", (route) =>
    route.fulfill({
      contentType: "application/json",
      body: JSON.stringify({ items: positionFixtures() }),
    }),
  );
  await page.route("**/api/v1/telemetry", (route) =>
    route.fulfill({
      contentType: "application/json",
      body: JSON.stringify({ items: telemetryFixture(3.9) }),
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
  await page.route("**/api/v1/meshtastic", (route) =>
    route.fulfill({
      contentType: "application/json",
      body: JSON.stringify({
        configured: true,
        meshNetworkId: "mesh-e2e",
        gatewayId: "gateway-e2e",
        connection: {
          transport: "serial",
          status: "ready",
          changedAt: "2026-07-18T00:00:00.000Z",
        },
        metrics: {
          bytesReceived: 1024,
          bytesSent: 256,
          framesReceived: 12,
          framesSent: 4,
          malformedFrames: 0,
          reconnects: 1,
        },
      }),
    }),
  );
  await page.route("**/api/v1/aprs", (route) =>
    route.fulfill({
      contentType: "application/json",
      body: JSON.stringify({
        configured: true,
        running: true,
        monitorStatus: "connected",
        mappedCallsigns: 1,
        pendingOutbox: 1,
        failedOutbox: 0,
        unconfirmedOutbox: 0,
        pendingStationSubmissions: 0,
        failedStationSubmissions: 0,
        unconfirmedStationSubmissions: 0,
      }),
    }),
  );
  await page.route("**/api/v1/aprs/outbox", (route) =>
    route.fulfill({
      contentType: "application/json",
      body: JSON.stringify({
        items: [
          {
            id: "outbox-e2e",
            callsign: "N0CALL-7",
            canonicalEventId: "position-e2e",
            status: "queued",
            deliveryStatus: "queued",
            attempts: 0,
            nextAttemptAt: "2026-07-18T00:00:00.000Z",
            createdAt: "2026-07-18T00:00:00.000Z",
            updatedAt: "2026-07-18T00:00:00.000Z",
          },
        ],
      }),
    }),
  );
  await page.route("**/api/v1/aprs/station-submissions", (route) =>
    route.fulfill({ contentType: "application/json", body: '{"items":[]}' }),
  );
}

function positionFixtures() {
  return [
    [42, 25.0478, 121.5319],
    [43, 24.1477, 120.6736],
    [44, 22.6273, 120.3014],
  ].map(([nodeNum, latitude, longitude], index) => ({
    schemaVersion: 1,
    id: `position-${index}`,
    canonicalKey: `fixture-${index}`,
    meshNetworkId: "mesh-e2e",
    nodeNum,
    sourceObservationId: `observation-${index}`,
    payloadHash: String(index).padStart(64, "0"),
    eventTime: `2026-07-18T00:0${index}:00.000Z`,
    eventTimeSource: "position_timestamp",
    position: {
      latitudeI: Math.round(Number(latitude) * 10_000_000),
      longitudeI: Math.round(Number(longitude) * 10_000_000),
      precisionBits: 32,
    },
    createdAt: `2026-07-18T00:0${index}:01.000Z`,
  }));
}

function telemetryFixture(voltage: number) {
  return [
    {
      schemaVersion: 1,
      id: `telemetry-${voltage}`,
      observationId: `observation-${voltage}`,
      meshNetworkId: "mesh-e2e",
      nodeNum: 42,
      metricKind: "deviceMetrics",
      metrics: { voltage, batteryLevel: 78 },
      observedAt: "2026-07-18T00:00:00.000Z",
    },
    {
      schemaVersion: 1,
      id: "telemetry-environment",
      observationId: "observation-environment",
      meshNetworkId: "mesh-e2e",
      nodeNum: 43,
      metricKind: "environmentMetrics",
      metrics: { temperature: 27.5, relativeHumidity: 68 },
      observedAt: "2026-07-18T00:01:00.000Z",
    },
  ];
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
