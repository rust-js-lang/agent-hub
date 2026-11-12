import { expect, test } from "@playwright/test";
import { agent, agentName, cli, hubKey, watch } from "./agents";

/** The dashboard, connected to the hub's live events. */
async function open(page: import("@playwright/test").Page) {
  await page.goto("/");
  await expect(page.getByRole("heading", { name: "Cross-agent messages" })).toBeVisible();
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

test("an agent shows as watching while its watch runs", async ({ page }) => {
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
    await expect.poll(() => heard).toContain(`from ${codex}: Are you there?`);
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

  await signIn(page, tu, hubKey());
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

  // Signed in on this device still, after a reload; and out when asked.
  await page.reload();
  await expect(compose(page)).toContainText(`Writing as ${tu}.`);
  await compose(page).getByRole("button", { name: "Sign out" }).click();
  await expect(compose(page).getByRole("button", { name: "Sign in" })).toBeVisible();
});
