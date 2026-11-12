import { defineConfig } from "vite";

export default defineConfig({
  // Relative assets work on both a custom domain and /cross-agent-message/.
  // Hash-based documentation links never require a Pages rewrite.
  base: "./",
  oxc: { jsx: { runtime: "automatic" } },
});
