import { defineConfig } from "vite";

// The page is `dashboard`, compiled to JSX by `rust-js build --package
// dashboard`. `/api` is the hub, `cargo run -- serve`, or `CROSS_AGENT_HUB`.
export default defineConfig({
  oxc: { jsx: { runtime: "automatic" } },
  server: {
    proxy: { "/api": process.env.CROSS_AGENT_HUB ?? "http://127.0.0.1:7878" },
  },
});
