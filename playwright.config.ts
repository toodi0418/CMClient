import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./apps/web/e2e",
  outputDir: "./test-results/playwright",
  timeout: 30_000,
  fullyParallel: true,
  forbidOnly: Boolean(process.env.CI),
  retries: process.env.CI ? 1 : 0,
  reporter: process.env.CI ? "github" : "list",
  use: {
    baseURL: "http://127.0.0.1:5175",
    trace: "retain-on-failure",
  },
  webServer: {
    command:
      "pnpm --filter @cmclient/web exec vite --host 127.0.0.1 --port 5175",
    url: "http://127.0.0.1:5175",
    reuseExistingServer: !process.env.CI,
    timeout: 30_000,
  },
});
