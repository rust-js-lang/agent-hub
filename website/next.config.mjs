import { fileURLToPath } from "node:url";

/** @type {import('next').NextConfig} */
const nextConfig = {
  output: "export",
  trailingSlash: true,
  // Override with SITE_BASE_PATH="" for a custom domain or root deployment.
  basePath: process.env.SITE_BASE_PATH ?? "/cross-agent-message",
  turbopack: { root: fileURLToPath(new URL("..", import.meta.url)) },
};

export default nextConfig;
