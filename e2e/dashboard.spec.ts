import { execFileSync } from "node:child_process";
import { db } from "../playwright.config";
import { expect, test } from "@playwright/test";
import { agent, agentName, cli, hubKey, watch } from "./agents";

test("listen addresses distinguish local readiness, pending interfaces, and blocked HTTP", async ({ page }) => {
  await page.route("**/api/hub", async route => {
    const response = await route.fetch();
    await route.fulfill({ response, json: { ...await response.json(), addresses: [
      { address: "127.0.0.1:7878", state: "ready", remoteSeen: false },
      { address: "100.64.0.1:7878", state: "ready", remoteSeen: true },
      { address: "100.64.0.2:7878", state: "pending", remoteSeen: false },
      { address: "100.64.0.3:7878", state: "blocked", remoteSeen: false, reason: "The firewall may be blocking this installed executable." },
    ] } });
  });
  await open(page);
  const addresses = page.getByRole("list", { name: "Hub addresses" });
  await expect(addresses.getByRole("listitem")).toHaveCount(4);
  await expect(addresses.getByRole("listitem").filter({ hasText: "127.0.0.1:7878" })).not.toContainText("seen from another machine");
  await expect(addresses.getByRole("listitem").filter({ hasText: "100.64.0.1:7878" })).toContainText("seen from another machine");
  await expect(addresses.getByRole("listitem").filter({ hasText: "100.64.0.2:7878" })).toContainText("pending");
  await expect(addresses.getByRole("listitem").filter({ hasText: "100.64.0.3:7878" })).toContainText("blocked");
  await expect(addresses).toContainText("The firewall may be blocking this installed executable.");
});

/** The dashboard, connected to the hub's live events. */
async function open(page: import("@playwright/test").Page) {
  await page.goto("/");
  await expect(page.getByRole("heading", { name: "Agent Hub" })).toBeVisible();
  await expect(page.getByText("Live", { exact: true })).toBeVisible();
}

function messages(page: import("@playwright/test").Page) {
  return page.getByRole("region", { name: "Messages" }).getByRole("article");
}

function agentButton(page: import("@playwright/test").Page, name: string) {
  return page.getByRole("navigation", { name: "Agents" }).getByRole("button", { name: new RegExp(`^${name}\\b`) });
}

test("a message appears the moment it's sent, from and to whom", async ({ page }) => {
  const codex = agent("codex");
  const claude = agent("claude");
  await open(page);

  cli("send", "--as", codex, claude, "I took the parser; you take the tests.");

  const message = messages(page).filter({ hasText: "I took the parser; you take the tests." });
  await expect(message).toHaveCount(1);
  await expect(message.locator(".from")).toHaveText(codex);
  await expect(message.locator(".to")).toHaveText(claude);
  await expect(message.locator(".state")).toContainText("unread");
  await expect(message).toHaveClass(/\bunread\b/);
});

test("a message is shown read once its recipient reads its inbox", async ({ page }) => {
  const codex = agent("codex");
  const claude = agent("claude");
  cli("send", "--as", codex, claude, "Read me.");
  await open(page);
  const message = messages(page).filter({ hasText: "Read me." });
  await expect(message).toHaveClass(/\bunread\b/);
  await expect(agentButton(page, claude)).toContainText("1 unread");

  const inbox = cli("inbox", "--as", claude);
  expect(inbox).toContain(`from ${codex}`);
  expect(inbox).toContain("Read me.");

  await expect(message).not.toHaveClass(/\bunread\b/);
  await expect(message.locator(".state")).toContainText("read ");
  await expect(agentButton(page, claude)).not.toContainText("unread");
});

test("picking an agent shows only the messages it sent or was sent", async ({ page }) => {
  const a = agent("alpha");
  const b = agent("beta");
  const c = agent("gamma");
  cli("send", "--as", a, b, `${a} to ${b}`);
  cli("send", "--as", b, c, `${b} to ${c}`);
  cli("send", "--as", c, a, `${c} to ${a}`);
  await open(page);

  await agentButton(page, a).click();
  await expect(agentButton(page, a)).toHaveClass(/\bchosen\b/);
  await expect(messages(page).filter({ hasText: `${a} to ${b}` })).toHaveCount(1);
  await expect(messages(page).filter({ hasText: `${c} to ${a}` })).toHaveCount(1);
  await expect(messages(page).filter({ hasText: `${b} to ${c}` })).toHaveCount(0);

  await page.getByRole("button", { name: /^Everyone/ }).click();
  await expect(messages(page).filter({ hasText: `${b} to ${c}` })).toHaveCount(1);
});

test("an agent shows as watching while its watch runs", async ({ page, request }) => {
  const claude = agent("claude");
  const codex = agent("codex");
  await open(page);
  await expect(agentButton(page, claude)).toContainText("seen ");

  const watcher = watch(claude);
  let heard = "";
  watcher.stdout?.on("data", (chunk) => (heard += chunk));
  try {
    await expect(agentButton(page, claude)).toContainText("watching");
    cli("send", "--as", codex, claude, "Are you there?");
    const tu = agentName("urgent-human");
    await request.post("/api/person", { data: { name: tu, key: hubKey() } });
    await request.post("/api/messages", { data: { from: tu, to: claude, body: "Urgent.", key: hubKey() } });
    await expect.poll(() => heard).toContain(`from ${tu}, the person`);
    expect(heard).not.toContain("Urgent.");
    expect(heard).not.toContain("Are you there?");
  } finally {
    watcher.kill();
  }
  await expect(agentButton(page, claude)).toContainText("seen ");
});

test("the history is the hub's: it's there after a reload", async ({ page }) => {
  const codex = agent("codex");
  const claude = agent("claude");
  cli("send", "--as", codex, claude, "Still here after a reload.\nWith a second line.");
  await open(page);
  await page.reload();
  await expect(page.getByText("Live", { exact: true })).toBeVisible();

  const message = messages(page).filter({ hasText: "Still here after a reload." });
  await expect(message.locator(".body")).toHaveText("Still here after a reload.\nWith a second line.");
});

test("the hub refuses a message to an agent it hasn't seen, and nothing is shown", async ({ page }) => {
  const codex = agent("codex");
  const nobody = agentName("nobody");
  await open(page);

  expect(() => cli("send", "--as", codex, nobody, "Lost?")).toThrow(/no agent is named/);
  await expect(agentButton(page, codex)).toBeVisible();
  await expect(messages(page).filter({ hasText: "Lost?" })).toHaveCount(0);
});

function compose(page: import("@playwright/test").Page) {
  return page.getByRole("region", { name: "Write to the agents" });
}

/** Signs the person in on this page, as `name`, with `key`. */
async function signIn(page: import("@playwright/test").Page, name: string, key: string) {
  await compose(page).getByLabel("Your name").fill(name);
  await compose(page).getByLabel("The hub's key").fill(key);
  await compose(page).getByRole("button", { name: "Sign in" }).click();
}

test("the person signs in with the hub's key and writes to an agent, who reads it as theirs", async ({ page }) => {
  const claude = agent("claude");
  const codex = agent("codex");
  const tu = agentName("tu");
  await open(page);

  // A key that isn't the hub's signs no one in.
  await signIn(page, tu, "0000-0000-0000");
  await expect(compose(page).getByRole("alert")).toContainText("that isn't this hub's key");

  // Typed as a phone's number keys type it, digits alone: the page writes
  // the dashes.
  const key = hubKey();
  await compose(page).getByLabel("Your name").fill(tu);
  await compose(page).getByLabel("The hub's key").fill("");
  await compose(page).getByLabel("The hub's key").pressSequentially(key.replaceAll("-", ""));
  await expect(compose(page).getByLabel("The hub's key")).toHaveValue(key);
  await compose(page).getByRole("button", { name: "Sign in" }).click();
  await expect(compose(page)).toContainText(`Writing as ${tu}.`);
  await expect(agentButton(page, tu)).toContainText("the person");

  // Sent to the one picked: nothing is sent while there's nothing to say.
  await expect(compose(page).getByRole("button", { name: "Send" })).toBeDisabled();
  await compose(page).getByLabel("To").selectOption(claude);
  await compose(page).getByLabel("Message").fill("Ship it when the tests pass.");
  await compose(page).getByRole("button", { name: "Send" }).click();

  const message = messages(page).filter({ hasText: "Ship it when the tests pass." });
  await expect(message).toHaveCount(1);
  await expect(message.locator(".from")).toHaveText(tu);
  await expect(message.locator(".to")).toHaveText(claude);
  await expect(message.locator(".badge")).toHaveText("the person");
  await expect(compose(page).getByLabel("Message")).toHaveValue("");

  // The agent reads who it's from; no agent's command writes as them.
  expect(cli("inbox", "--as", claude)).toContain(`from ${tu}, the person`);
  expect(() => cli("send", "--as", tu, codex, "As them.")).toThrow(/is the person's name on this hub/);

  // An answer to the person is read once it's on their page.
  cli("send", "--as", claude, tu, "Shipping.");
  const answer = messages(page).filter({ hasText: "Shipping." });
  await expect(answer.locator(".state")).toContainText("read ");
  await expect(answer).not.toHaveClass(/\bunread\b/);

  // To a channel, every agent in it is sent a copy, and the page shows it
  // once, with how many have read it. `#everyone` is offered first.
  await compose(page).getByLabel("To").selectOption("#everyone");
  await compose(page).getByLabel("Message").fill("Stand-up in five.");
  await compose(page).getByRole("button", { name: "Send" }).click();
  const all = messages(page).filter({ hasText: "Stand-up in five." });
  await expect(all).toHaveCount(1);
  await expect(all.locator(".to")).toHaveText("#everyone");
  await expect(all.locator(".state")).toContainText(/read by 0 of \d+/);
  expect(cli("inbox", "--as", codex)).toContain(`from ${tu}, the person, to #everyone`);
  await expect(all.locator(".state")).toContainText(/read by 1 of \d+/);
  // One an agent joined is offered too, by its name and how many are in it.
  cli("join", "--as", claude, "#reviewers");
  await page.reload();
  await expect(compose(page).getByLabel("To").locator("option", { hasText: "#reviewers · 1 agent" })).toHaveCount(1);

  // Signed in on this device still, after a reload; and out when asked.
  await page.reload();
  await expect(compose(page)).toContainText(`Writing as ${tu}.`);
  await compose(page).getByRole("button", { name: "Sign out" }).click();
  await expect(compose(page).getByRole("button", { name: "Sign in" })).toBeVisible();
});


test("away agents are collapsed and the signed-in person can retire one without losing history", async ({ page }) => {
  const sender = agent("sender");
  const stale = agent("stale");
  cli("send", "--as", sender, stale, `History for ${stale}`);
  execFileSync("python3", ["-c", "import sqlite3,sys; db=sqlite3.connect(sys.argv[1]); db.execute('UPDATE agents SET last_seen = ? WHERE name = ?', (int(sys.argv[3]),sys.argv[2])); db.commit()", db, stale, String(Date.now() - 2 * 3600_000)]);
  await open(page);
  const away = page.locator("details.away-agents");
  await expect(away.locator("summary")).toContainText("Away");
  await expect(agentButton(page, stale)).not.toBeVisible();
  await away.locator("summary").click();
  await expect(agentButton(page, stale)).toBeVisible();
  await expect(page.getByRole("button", { name: `Retire ${stale}`, exact: true })).toHaveCount(0);
  await signIn(page, agentName("tu-retire"), hubKey());
  await expect(compose(page)).toContainText("Writing as");
  await page.getByRole("button", { name: `Retire ${stale}`, exact: true }).click();
  await expect(agentButton(page, stale)).toHaveCount(0);
  await expect(messages(page).filter({ hasText: `History for ${stale}` })).toHaveCount(1);
});

test("retirement ends a running CLI watch instead of reconnecting and reviving it", async ({ page }) => {
  const name = agent("watch-retire");
  const watcher = watch(name);
  let heard = "";
  let exitCode: number | null = null;
  watcher.stdout?.on("data", chunk => heard += chunk);
  watcher.on("exit", code => exitCode = code);
  try {
    await open(page);
    await expect(agentButton(page, name)).toContainText("watching");
    cli("retire", "--as", name);
    await expect.poll(() => exitCode).toBe(0);
    expect(heard).toContain("was retired. This watch has ended.");
    await expect(agentButton(page, name)).toHaveCount(0);
  } finally { watcher.kill(); }
});

test("an upgraded hub never discards a draft and reloads once the empty page is idle", async ({ page }) => {
  await page.clock.install();
  let build = "before-upgrade";
  await page.route("**/api/hub", route => route.fulfill({ json: { version: "0.1.0", build, started: 1 } }));
  await page.addInitScript(() => {
    const Original = window.EventSource;
    window.EventSource = class extends Original {
      constructor(url: string | URL, init?: EventSourceInit) {
        super(url, init);
        (window as any).testEventSource = this;
      }
    };
  });
  agent("reload-recipient");
  await open(page);
  await signIn(page, agentName("reload-person"), hubKey());
  await expect(compose(page)).toContainText("Writing as");
  const draft = "This half-written message must survive an upgrade.";
  await compose(page).getByLabel("Message").fill(draft);
  build = "after-upgrade";
  await page.evaluate(() => (window as any).testEventSource.dispatchEvent(new Event("open")));
  await expect(page.getByRole("status")).toContainText("The hub was upgraded");
  await page.clock.runFor(31_000);
  await expect(compose(page).getByLabel("Message")).toHaveValue(draft);
  await expect(page.getByRole("status")).toBeVisible();
  await page.reload();
  await expect(compose(page).getByLabel("Message")).toHaveValue(draft);
  await compose(page).getByLabel("Message").fill("");
  build = "third-build";
  await page.evaluate(() => (window as any).testEventSource.dispatchEvent(new Event("open")));
  await expect(page.getByRole("status")).toBeVisible();
  await page.clock.runFor(31_000);
  await expect(page.getByRole("status")).toHaveCount(0);
  await expect(compose(page).getByLabel("Message")).toHaveValue("");
});

test("a CLOSED stream reconnects after an upgrade while a draft delays reload", async ({ page }) => {
  await page.clock.install();
  let build = "before-closed";
  await page.route("**/api/hub", async route => {
    const response = await route.fetch();
    await route.fulfill({ response, json: { ...await response.json(), build } });
  });
  await page.addInitScript(() => {
    const Original = window.EventSource;
    (window as any).testSources = [];
    window.EventSource = class extends Original {
      constructor(url: string | URL, init?: EventSourceInit) {
        super(url, init);
        (window as any).testSources.push(this);
      }
    };
  });
  agent("closed-recipient");
  await open(page);
  await signIn(page, agentName("closed-person"), hubKey());
  await expect(compose(page)).toContainText("Writing as");
  const draft = "Keep editing while messages reconnect.";
  await compose(page).getByLabel("Message").fill(draft);
  build = "after-closed";
  await page.evaluate(() => {
    const source = (window as any).testSources.at(-1);
    source.close();
    source.dispatchEvent(new Event("error"));
  });
  await expect(page.getByRole("status")).toContainText("The hub was upgraded");
  await page.clock.runFor(1000);
  await expect(page.getByText("Live", { exact: true })).toBeVisible();
  expect(await page.evaluate(() => (window as any).testSources.length)).toBe(2);
  await expect(compose(page).getByLabel("Message")).toHaveValue(draft);
  await page.clock.runFor(31_000);
  await expect(compose(page).getByLabel("Message")).toHaveValue(draft);
  await expect(page.getByRole("status")).toBeVisible();
});

test("a committed send with a lost response replays one nonce and delivers once", async ({ page }) => {
  const recipient = agent("nonce-recipient");
  const person = agentName("nonce-person");
  await open(page);
  await signIn(page, person, hubKey());
  await expect(compose(page)).toContainText(`Writing as ${person}.`);
  const attempts: { nonce?: string; body: string | null }[] = [];
  await page.route("**/api/messages", async route => {
    if (route.request().method() !== "POST") return route.continue();
    attempts.push({ nonce: route.request().headers()["agent-hub-nonce"], body: route.request().postData() });
    const response = await route.fetch();
    if (attempts.length === 1) await route.abort("connectionreset");
    else await route.fulfill({ response });
  });
  const body = `Deliver only once to ${recipient}.`;
  await compose(page).getByLabel("To").selectOption(recipient);
  await compose(page).getByLabel("Message").fill(body);
  await compose(page).getByRole("button", { name: "Send" }).click();
  await expect(compose(page).getByLabel("Message")).toHaveValue("");
  expect(attempts).toHaveLength(2);
  expect(attempts[0].nonce).toBeTruthy();
  expect(attempts[1]).toEqual(attempts[0]);
  const inbox = cli("inbox", "--as", recipient);
  expect(inbox.split(body)).toHaveLength(2);
  expect(cli("inbox", "--as", recipient)).not.toContain(body);
});

test("a hub without write-nonce gets one ambiguous send attempt and keeps the draft", async ({ page }) => {
  const recipient = agent("legacy-nonce-recipient");
  await page.route("**/api/hub", async route => {
    const response = await route.fetch();
    const info = await response.json();
    await route.fulfill({ response, json: { ...info, features: info.features.filter((f: string) => f !== "write-nonce") } });
  });
  await open(page);
  const person = agentName("legacy-nonce-person");
  await signIn(page, person, hubKey());
  await expect(compose(page)).toContainText(`Writing as ${person}.`);
  let attempts = 0;
  await page.route("**/api/messages", async route => {
    if (route.request().method() !== "POST") return route.continue();
    attempts += 1;
    expect(route.request().headers()["agent-hub-nonce"]).toBeUndefined();
    await route.fetch(); // Commit, then lose the acknowledgement.
    await route.abort("connectionreset");
  });
  const body = `Keep this draft for ${recipient}.`;
  await compose(page).getByLabel("To").selectOption(recipient);
  await compose(page).getByLabel("Message").fill(body);
  await compose(page).getByRole("button", { name: "Send" }).click();
  await expect(compose(page).getByRole("alert")).toContainText("can't be reached");
  await expect(compose(page).getByRole("button", { name: "Send" })).toBeEnabled();
  expect(attempts).toBe(1);
  await expect(compose(page).getByLabel("Message")).toHaveValue(body);
  expect(cli("inbox", "--as", recipient).split(body)).toHaveLength(2);
});

test("an explicit refusal is shown once without replaying a send", async ({ page }) => {
  const recipient = agent("refusal-recipient");
  await open(page);
  const person = agentName("refusal-person");
  await signIn(page, person, hubKey());
  await expect(compose(page)).toContainText(`Writing as ${person}.`);
  let attempts = 0;
  await page.route("**/api/messages", async route => {
    if (route.request().method() !== "POST") return route.continue();
    attempts += 1;
    await route.fulfill({ status: 503, json: { code: "busy", reason: "Try after this request finishes." } });
  });
  await compose(page).getByLabel("To").selectOption(recipient);
  await compose(page).getByLabel("Message").fill("Preserve the refused draft.");
  await compose(page).getByRole("button", { name: "Send" }).click();
  await expect(compose(page).getByRole("alert")).toHaveText("Try after this request finishes.");
  await expect(compose(page).getByRole("button", { name: "Send" })).toBeEnabled();
  expect(attempts).toBe(1);
  await expect(compose(page).getByLabel("Message")).toHaveValue("Preserve the refused draft.");
});
