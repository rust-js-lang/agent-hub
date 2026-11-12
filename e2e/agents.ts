// Agents, as the tests play them: the command line, run as an agent would.

import { type ChildProcess, execFileSync, spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { db, hub } from "../playwright.config";

// The binary `cargo run` built for the test hub.
const binary = "target/debug/cross-agent-message";
const env = { ...process.env, CROSS_AGENT_HUB: hub };

/** Runs `cross-agent-message <args>` and gives what it printed. */
export function cli(...args: string[]): string {
  return execFileSync(binary, args, { env, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] });
}

/** A name no other test uses: `claude-3f9a1c`. */
export function agentName(base: string): string {
  return `${base}-${randomBytes(3).toString("hex")}`;
}

/** Names an agent and makes the hub see it, as its first command does. */
export function agent(base: string): string {
  const name = agentName(base);
  cli("agents", "--as", name);
  return name;
}

/** `watch` as `name`, running until it's killed. */
export function watch(name: string): ChildProcess {
  return spawn(binary, ["watch", "--as", name], { env, stdio: "pipe" });
}

/** The hub's key, as the person reads it on the hub's machine. */
export function hubKey(): string {
  return cli("key", "--db", db).trim();
}
