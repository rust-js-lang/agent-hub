import { defineConfig, devices } from "@playwright/test";
import siteConfig from "./website/next.config.mjs";

// Test the static export using the same base path Next.js builds for deployment.
const baseURL = `http://127.0.0.1:4174${siteConfig.basePath}/`;
export default defineConfig({
  testDir: "website/e2e",
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
    command: "node scripts/preview-site.mjs --host 127.0.0.1 --port 4174",
    url: baseURL,
    reuseExistingServer: !process.env.CI,
    timeout: 30_000,
  },
});
