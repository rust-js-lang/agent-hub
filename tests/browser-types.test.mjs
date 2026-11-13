import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { test } from "node:test";
import { findCompiler } from "../rust-js/tooling/build.js";

const root = resolve(fileURLToPath(new URL("..", import.meta.url)));
const client = join(root, "dashboard/src/client.rs");
const cli = join(root, "rust-js/tooling/cli.js");
// Temp fixtures must use this application's compiler, not an unrelated native
// package installed above the temporary directory.
const compiler = await findCompiler(root);
const lock = readFileSync(join(root, "Cargo.lock"), "utf8");
const version = name => {
  const match = lock.match(new RegExp(`name = "${name}"\\nversion = "([^"]+)"`));
  assert.ok(match, `${name} must be pinned in the application's lockfile`);
  return match[1];
};
const send = 'protocol::Send { from: "a".into(), to: "b".into(), body: "hi".into(), key: None }';
const success = `pub async fn example() -> Result<protocol::Message, client::Error> {\n    client::post(&api::SEND, &${send}).await\n}`;
const cases = [
  ["GET cannot call a POST endpoint", "E0308",
    'pub async fn example() -> Result<protocol::Message, client::Error> {\n    client::get(&api::SEND, &()).await\n}', success],
  ["a POST body must match its endpoint", "E0308",
    'pub async fn example() -> Result<protocol::Message, client::Error> {\n    client::post(&api::SEND, &protocol::Register { name: "a".into(), machine: "m".into(), reconnect: false }).await\n}', success],
  ["the response type cannot be chosen independently", "E0308",
    `pub async fn example() -> Result<protocol::Agent, client::Error> {\n    client::post(&api::SEND, &${send}).await\n}`, success],
  ["advertised features cannot be invented outside the transport", "E0451",
    'pub fn example(info: protocol::HubInfo) -> client::Advertised {\n    client::Advertised { info }\n}',
    'pub async fn example() -> Result<client::Advertised, client::Error> {\n    client::probe().await\n}'],
  ["a stream requires the successful probe value", "E0061",
    'pub fn example() -> Result<&\'static webapi::EventSource, client::Error> {\n    client::stream(&api::EVENTS, &api::Events::default(), |_, _| {})\n}',
    'pub fn example(advertised: &client::Advertised) -> Result<&\'static webapi::EventSource, client::Error> {\n    client::stream(&api::EVENTS, &api::Events::default(), advertised, |_, _| {})\n}'],
];

for (const [name, code, invalid, valid] of cases) {
  test(name, { timeout: 90_000 }, () => {
    const dir = mkdtempSync(join(tmpdir(), "cam-browser-contract-"));
    try {
      writeFileSync(join(dir, "Cargo.toml"), `[package]
name = "browser_contract_fixture"
version = "0.0.0"
edition = "2024"
[workspace]
[lib]
path = "lib.rs"
[dependencies]
protocol = { path = ${JSON.stringify(join(root, "protocol"))} }
js = { package = "rust-js-builtins", path = ${JSON.stringify(join(root, "rust-js/builtins"))} }
webapi = { package = "rust-js-webapi", path = ${JSON.stringify(join(root, "rust-js/webapi"))} }
serde = "=${version("serde")}"
serde_json = "=${version("serde_json")}"
`);
      writeFileSync(join(dir, "Cargo.lock"), lock);
      const compile = source => {
        writeFileSync(join(dir, "lib.rs"), `#[path = ${JSON.stringify(client)}] pub mod client;\nuse protocol::endpoints as api;\n${source}\n`);
        // rust-js clippy runs rustc's type checks with the browser bindings.
        // It does not emit over the real client.rs's generated JavaScript.
        return spawnSync(process.execPath, [cli, "clippy"], {
          cwd: dir, encoding: "utf8", timeout: 40_000,
          env: { ...process.env, RUST_JS_COMPILER: compiler, CARGO_NET_OFFLINE: "true", RUSTC_WRAPPER: "",
            CARGO_TARGET_DIR: join(root, "target/browser-type-contracts") },
        });
      };
      const twin = compile(valid);
      assert.equal(twin.error, undefined, twin.error?.message);
      assert.equal(twin.status, 0, `valid twin must compile:\n${twin.stderr}`);
      const refused = compile(invalid);
      assert.equal(refused.error, undefined, refused.error?.message);
      assert.notEqual(refused.status, 0, "the invalid call unexpectedly compiled");
      assert.match(refused.stderr, new RegExp(`error\\[${code}\\]`));
      assert.match(refused.stderr, /lib\.rs:4:\d+/, "refusal must point at the invalid call");
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
}
