import { test, expect } from "@playwright/test";

async function openGuideMenu(page) {
  const toggle = page.getByRole("button", { name: "Browse guides" });
  if (await toggle.isVisible()) await toggle.click();
}

const guides = [
  ["introduction", "A shared line for your agents."],
  ["getting-started", "From clone to conversation."],
  ["messaging", "A message that waits for you."],
  ["channels", "Keep the team in the loop."],
  ["holds", "Make room for the heavy work."],
  ["across-machines", "Different machines. Same conversation."],
  ["dashboard", "Your seat at the conversation."],
  ["commands", "A small command vocabulary."],
  ["architecture", "Small pieces. Clear boundaries."],
  ["development", "Build it. Make it yours."],
];

test("landing page loads its assets under the GitHub Pages subpath", async ({
  page,
}) => {
  const errors = [];
  const failed = [];
  page.on("pageerror", (error) => errors.push(error.message));
  page.on("response", (response) => {
    if (response.status() >= 400 && response.url().includes("127.0.0.1"))
      failed.push(response.url());
  });
  await page.goto("./");
  await expect(page).toHaveTitle(/cross-agent-message/);
  await expect(page.getByRole("heading", { level: 1 })).toHaveText(
    "Good agents.Better together.",
  );
  await expect(
    page.getByRole("link", { name: "Get started", exact: false }).first(),
  ).toBeVisible();
  await expect(page.locator(".hero")).toHaveCSS("display", "grid");
  expect(
    await page
      .locator("body")
      .evaluate((body) => body.scrollWidth <= window.innerWidth),
  ).toBe(true);
  expect(errors).toEqual([]);
  expect(failed).toEqual([]);
});

test("command demo switches between direct, channel and hold examples", async ({
  page,
}) => {
  await page.goto("./");
  await page.getByRole("tab", { name: "Team channel" }).click();
  await expect(page.getByRole("tabpanel")).toContainText("'#everyone'");
  await expect(page.getByRole("tabpanel")).toContainText(
    "Sent to #everyone: 2 agents",
  );
  await expect(page.getByRole("tab", { name: "Team channel" })).toHaveAttribute(
    "aria-selected",
    "true",
  );
  await page.getByRole("tab", { name: "Take a turn" }).click();
  await expect(page.getByRole("tabpanel")).toContainText("hold check-mini");
  await page.getByRole("tab", { name: "Direct message" }).click();
  await expect(page.getByRole("tabpanel")).toContainText("Sent to codex");
});

test("get started, guide navigation, back and home work", async ({ page }) => {
  await page.goto("./");
  await page
    .getByRole("link", { name: "Get started", exact: false })
    .first()
    .click();
  await expect(page).toHaveURL(/#docs\/getting-started$/);
  await expect(page.getByRole("heading", { level: 1 })).toHaveText(
    "From clone to conversation.",
  );
  const navigation = page.getByRole("navigation", {
    name: "Documentation",
    exact: true,
    includeHidden: true,
  });
  await openGuideMenu(page);
  await navigation.getByRole("link", { name: "Channels", exact: true }).click();
  await expect(page.getByRole("heading", { level: 1 })).toHaveText(
    "Keep the team in the loop.",
  );
  await expect(
    navigation.getByRole("link", {
      name: "Channels",
      exact: true,
      includeHidden: true,
    }),
  ).toHaveAttribute("aria-current", "page");
  await page.goBack();
  await expect(page.getByRole("heading", { level: 1 })).toHaveText(
    "From clone to conversation.",
  );
  await page
    .getByRole("link", { name: "cross-agent-message home", exact: true })
    .click();
  await expect(page.getByRole("heading", { level: 1 })).toContainText(
    "Good agents.",
  );
});

test("all guides support direct links and browser reloads", async ({
  page,
}) => {
  for (const [slug, title] of guides) {
    await page.goto(`./#docs/${slug}`);
    await expect(page.getByRole("heading", { level: 1 })).toHaveText(title);
    expect(
      await page
        .locator("body")
        .evaluate((body) => body.scrollWidth <= window.innerWidth),
    ).toBe(true);
  }
  await page.reload();
  await expect(page.getByRole("heading", { level: 1 })).toHaveText(
    "Build it. Make it yours.",
  );
});

test("guide search filters navigation and handles no matches", async ({
  page,
}) => {
  await page.goto("./#docs/introduction");
  await openGuideMenu(page);
  const search = page.getByPlaceholder("Search guides…");
  const navigation = page.getByRole("navigation", {
    name: "Documentation",
    exact: true,
    includeHidden: true,
  });
  await search.fill("channels");
  await expect(navigation.getByRole("link")).toHaveCount(1);
  await expect(
    navigation.getByRole("link", {
      name: "Channels",
      exact: true,
      includeHidden: true,
    }),
  ).toBeVisible();
  await search.fill("nothing-matches-this");
  await expect(navigation.getByRole("status")).toContainText("No guides found");
  await search.fill("");
  await expect(navigation.getByRole("link")).toHaveCount(guides.length);
});

test("copy button puts the actual command on the clipboard", async ({
  page,
  context,
}) => {
  await context.grantPermissions(["clipboard-read", "clipboard-write"]);
  await page.goto("./#docs/getting-started");
  const code = page.locator(".code-block").first();
  const text = await code.locator("code").innerText();
  await code.getByRole("button", { name: "Copy command" }).click();
  await expect(code.getByRole("status")).toHaveText("Copied!");
  expect(await page.evaluate(() => navigator.clipboard.readText())).toBe(text);
});

test("unknown documentation route offers recovery", async ({ page }) => {
  await page.goto("./#docs/not-a-guide");
  await expect(page.getByRole("heading", { level: 1 })).toHaveText(
    "Let's get you back on track.",
  );
  await page.getByRole("link", { name: "Back to introduction" }).click();
  await expect(page.getByRole("heading", { level: 1 })).toHaveText(
    "A shared line for your agents.",
  );
});

test("adjacent guide links and source link point to real destinations", async ({
  page,
}) => {
  await page.goto("./#docs/introduction");
  await page
    .getByRole("navigation", { name: "Adjacent guides" })
    .getByRole("link")
    .click();
  await expect(page).toHaveURL(/#docs\/getting-started$/);
  await expect(
    page
      .getByRole("navigation", { name: "Main navigation" })
      .getByRole("link", { name: "GitHub" }),
  ).toHaveAttribute(
    "href",
    "https://github.com/rust-js-lang/cross-agent-message",
  );
});

test("keyboard skip link keeps the current guide and focuses content", async ({
  page,
}) => {
  await page.goto("./#docs/channels");
  await page.keyboard.press("Tab");
  await expect(
    page.getByRole("link", { name: "Skip to content" }),
  ).toBeFocused();
  await page.keyboard.press("Enter");
  await expect(page).toHaveURL(/#docs\/channels$/);
  await expect(page.locator("#content")).toBeFocused();
});

test("responsive layouts keep diagram cards separate and docs content near the top", async ({
  page,
}, testInfo) => {
  for (const width of [320, 390, 768, 820, 1024, 1280]) {
    await page.setViewportSize({ width, height: 1000 });
    await page.goto("./");
    const layout = await page.evaluate(() => {
      const rect = (selector) =>
        document.querySelector(selector).getBoundingClientRect();
      const frame = rect(".hero-visual");
      const cards = [
        ".node-claude",
        ".hub-node",
        ".message-note",
        ".node-codex",
      ].map(rect);
      const overlapping = cards.some((a, i) =>
        cards
          .slice(i + 1)
          .some(
            (b) =>
              a.left < b.right &&
              a.right > b.left &&
              a.top < b.bottom &&
              a.bottom > b.top,
          ),
      );
      return {
        overlapping,
        contained: cards.every(
          (r) =>
            r.left >= frame.left &&
            r.right <= frame.right &&
            r.top >= frame.top &&
            r.bottom <= frame.bottom,
        ),
        brandHeight: rect(".brand > span:last-child").height,
        overflow: document.documentElement.scrollWidth > innerWidth,
      };
    });
    expect(layout, `landing layout at ${width}px`).toEqual({
      overlapping: false,
      contained: true,
      brandHeight: expect.any(Number),
      overflow: false,
    });
    expect(layout.brandHeight, `unwrapped brand at ${width}px`).toBeLessThan(
      28,
    );
    await page.screenshot({
      path: testInfo.outputPath(`home-${width}.png`),
      fullPage: true,
    });
    await page.goto("./#docs/getting-started");
    if (width <= 760) {
      await expect(
        page.getByRole("button", { name: "Browse guides" }),
      ).toHaveAttribute("aria-expanded", "false");
      expect(
        await page
          .locator(".doc-article")
          .evaluate((article) => article.getBoundingClientRect().top),
      ).toBeLessThan(260);
      await openGuideMenu(page);
      await expect(page.getByPlaceholder("Search guides…")).toBeVisible();
      await page
        .getByRole("navigation", { name: "Documentation", exact: true })
        .getByRole("link", { name: "Channels", exact: true })
        .click();
      await expect(
        page.getByRole("button", { name: "Browse guides" }),
      ).toHaveAttribute("aria-expanded", "false");
      await expect(page.getByRole("heading", { level: 1 })).toHaveText(
        "Keep the team in the loop.",
      );
    }
    await page.screenshot({
      path: testInfo.outputPath(`docs-${width}.png`),
      fullPage: true,
    });
  }
});
