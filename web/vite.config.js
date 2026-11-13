import { defineConfig } from "vite";

// The page is `dashboard`, compiled to JSX by `rust-js build --package
// dashboard`. `/api` is the hub, `cargo run -- serve`, or `AGENT_HUB_URL`.
export default defineConfig({
  oxc: { jsx: { runtime: "automatic" } },
  server: {
    proxy: { "/api": process.env.AGENT_HUB_URL ?? "http://127.0.0.1:7878" },
  },
});
