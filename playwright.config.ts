import { defineConfig, devices } from "@playwright/test";

// The dashboard as it ships: `bun run build`'s `web/dist`, served by a hub
// of its own, on its own port, with a history file new to each run. The
// tests share that hub, so each names its agents apart.
const port = 7879;
export const hub = `http://127.0.0.1:${port}`;

export default defineConfig({
  testDir: "e2e",
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 1 : 0,
  reporter: process.env.CI ? "github" : "list",
  use: {
    baseURL: hub,
    trace: "retain-on-failure",
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
  webServer: {
    command: `cargo run -q -p cross-agent-message -- serve --port ${port} --db target/e2e/hub-${Date.now()}.db`,
    url: `${hub}/api/agents`,
    reuseExistingServer: false,
    // The first run builds the hub.
    timeout: 300_000,
    stdout: "pipe",
  },
});
