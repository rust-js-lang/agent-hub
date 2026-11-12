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

test("CSS Modules scope website classes away from global class names", async ({
  page,
}) => {
  await page.goto("./");
  const hero = page.getByTestId("hero");
  const code = page.getByTestId("code-block");
  await expect(hero).toHaveCSS("display", "grid");
  await expect(code).toHaveCSS("overflow", "hidden");
  expect(await hero.getAttribute("class")).not.toMatch(/(?:^|\s)hero(?:\s|$)/);
  expect(await code.getAttribute("class")).not.toMatch(/(?:^|\s)codeBlock(?:\s|$)/);

  await page.addStyleTag({
    content: ".hero { display: none !important; } .codeBlock { overflow: visible !important; }",
  });
  await expect(hero).toBeVisible();
  await expect(hero).toHaveCSS("display", "grid");
  await expect(code).toHaveCSS("overflow", "hidden");
});

test("landing page loads its assets under the configured deployment path", async ({
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
  await expect(page.locator('[data-testid="hero"]')).toHaveCSS("display", "grid");
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
  await expect(page).toHaveURL(/\/docs\/getting-started\/$/);
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
    await page.goto(`./docs/${slug}/`);
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
  await page.goto("./docs/introduction/");
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
  await page.goto("./docs/getting-started/");
  const code = page.locator('[data-testid="code-block"]').first();
  const text = await code.locator("code").innerText();
  await code.getByRole("button", { name: "Copy command" }).click();
  await expect(code.getByRole("status")).toHaveText("Copied!");
  expect(await page.evaluate(() => navigator.clipboard.readText())).toBe(text);
});

test("unknown documentation route offers recovery", async ({ page }) => {
  const response = await page.goto("./docs/not-a-guide/");
  expect(response.status()).toBe(404);
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
  await page.goto("./docs/introduction/");
  await page
    .getByRole("navigation", { name: "Adjacent guides" })
    .getByRole("link")
    .click();
  await expect(page).toHaveURL(/\/docs\/getting-started\/$/);
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
  await page.goto("./docs/channels/");
  await page.keyboard.press("Tab");
  await expect(
    page.getByRole("link", { name: "Skip to content" }),
  ).toBeFocused();
  await page.keyboard.press("Enter");
  await expect(page).toHaveURL(/\/docs\/channels\/$/);
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
      const frame = rect('[data-testid="hero-visual"]');
      const cards = [
        '[data-testid="node-claude"]',
        '[data-testid="hub-node"]',
        '[data-testid="message-note"]',
        '[data-testid="node-codex"]',
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
        brandHeight: rect('[aria-label="cross-agent-message home"] > span:last-child').height,
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
    await page.goto("./docs/getting-started/");
    if (width <= 760) {
      await expect(
        page.getByRole("button", { name: "Browse guides" }),
      ).toHaveAttribute("aria-expanded", "false");
      expect(
        await page
          .locator('[data-testid="doc-article"]')
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

test("Next exports guide content and metadata before JavaScript runs", async ({
  browser,
  baseURL,
}) => {
  const context = await browser.newContext({ javaScriptEnabled: false });
  const page = await context.newPage();
  const response = await page.goto(`${baseURL}docs/getting-started/`);
  expect(response.status()).toBe(200);
  await expect(page).toHaveTitle("From clone to conversation.");
  await expect(page.getByRole("heading", { level: 1 })).toHaveText(
    "From clone to conversation.",
  );
  await expect(
    page.getByRole("heading", { name: "1. Clone and install" }),
  ).toBeVisible();
  await expect(page.locator('meta[name="description"]')).toHaveAttribute(
    "content",
    /Build the project from source/,
  );
  await context.close();
});

test("Next Link navigation keeps the loaded document", async ({ page }) => {
  await page.goto("./");
  await page.evaluate(() => {
    window.navigationMarker = "same-document";
  });
  await page
    .getByRole("link", { name: "Get started", exact: false })
    .first()
    .click();
  await expect(page).toHaveURL(/\/docs\/getting-started\/$/);
  await expect(page.getByRole("heading", { level: 1 })).toHaveText(
    "From clone to conversation.",
  );
  expect(await page.evaluate(() => window.navigationMarker)).toBe(
    "same-document",
  );
});

test("Inter loads from self-hosted assets on the landing page and docs", async ({
  page,
  baseURL,
}) => {
  const googleFontRequests = [];
  const fontResponses = [];
  page.on("request", (request) => {
    const hostname = new URL(request.url()).hostname;
    if (["fonts.googleapis.com", "fonts.gstatic.com"].includes(hostname)) {
      googleFontRequests.push(request.url());
    }
  });
  page.on("response", (response) => {
    if (response.request().resourceType() === "font") {
      fontResponses.push({ url: response.url(), status: response.status() });
    }
  });

  for (const path of ["./", "./docs/getting-started/"]) {
    await page.goto(path);
    const font = await page.evaluate(async () => {
      await document.fonts.ready;
      return {
        family: getComputedStyle(document.body).fontFamily,
        loaded: [...document.fonts]
          .filter((face) => face.status === "loaded")
          .map((face) => face.family),
      };
    });
    expect(font.family).toMatch(/Inter/i);
    expect(
      font.loaded.some(
        (name) => /Inter/i.test(name) && !/Fallback/i.test(name),
      ),
    ).toBe(true);
  }

  expect(fontResponses.length).toBeGreaterThan(0);
  for (const response of fontResponses) {
    const url = new URL(response.url);
    expect(url.origin).toBe(new URL(baseURL).origin);
    expect(url.pathname).toContain("/_next/static/media/");
    expect(response.status).toBe(200);
  }
  expect(googleFontRequests).toEqual([]);
});
