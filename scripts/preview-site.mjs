// A dependency-free preview of exactly what GitHub Pages will serve.
import { createServer } from "node:http";
import { readFile, stat } from "node:fs/promises";
import { resolve, sep, extname } from "node:path";
import { parseArgs } from "node:util";
import config from "../website/next.config.mjs";

const { values } = parseArgs({
  options: {
    host: { type: "string", default: "127.0.0.1" },
    port: { type: "string", default: "4173" },
  },
});
const root = resolve("website/out");
const base = config.basePath;
const types = {
  ".html": "text/html",
  ".css": "text/css",
  ".js": "text/javascript",
  ".json": "application/json",
  ".txt": "text/plain",
  ".svg": "image/svg+xml",
  ".ico": "image/x-icon",
  ".woff2": "font/woff2",
};

createServer(async (request, response) => {
  try {
    const pathname = decodeURIComponent(
      new URL(request.url, "http://localhost").pathname,
    );
    if (base && pathname === "/") {
      response.writeHead(302, { Location: `${base}/` }).end();
      return;
    }
    if (base && pathname !== base && !pathname.startsWith(`${base}/`))
      throw new Error("outside base path");
    let file = resolve(root, `.${pathname.slice(base.length) || "/"}`);
    if (file !== root && !file.startsWith(root + sep))
      throw new Error("outside export");
    if ((await stat(file)).isDirectory()) file = resolve(file, "index.html");
    const body = await readFile(file);
    response.writeHead(200, {
      "Content-Type": `${types[extname(file)] ?? "application/octet-stream"}; charset=utf-8`,
    });
    response.end(body);
  } catch {
    const body = await readFile(resolve(root, "404.html")).catch(
      () => "Not found. Run bun run site:build first.",
    );
    response
      .writeHead(404, { "Content-Type": "text/html; charset=utf-8" })
      .end(body);
  }
}).listen(Number(values.port), values.host, () => {
  console.log(`Site preview: http://${values.host}:${values.port}${base}/`);
});
