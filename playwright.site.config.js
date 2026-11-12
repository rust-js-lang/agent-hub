import { defineConfig, devices } from "@playwright/test";

// Test the production bundle under the same subpath as a project Pages site.
const baseURL = "http://127.0.0.1:4174/cross-agent-message/";
export default defineConfig({
  testDir: "site/e2e",
  timeout: 30_000,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 1 : 0,
  workers: process.env.CI ? 2 : undefined,
  reporter: process.env.CI ? "github" : "list",
  use: { baseURL, trace: "retain-on-failure" },
  projects: [
    { name: "desktop", use: { ...devices["Desktop Chrome"] } },
    { name: "mobile", use: { ...devices["Pixel 7"] } },
  ],
  webServer: {
    command:
      "bunx vite preview site --host 127.0.0.1 --port 4174 --strictPort --base /cross-agent-message/",
    url: baseURL,
    reuseExistingServer: !process.env.CI,
    timeout: 30_000,
  },
});
