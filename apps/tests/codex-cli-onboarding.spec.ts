import { expect, test, type Page } from "@playwright/test";

const SETTINGS_SNAPSHOT = {
  updateAutoCheck: true,
  closeToTrayOnClose: false,
  closeToTraySupported: false,
  lowTransparency: false,
  lightweightModeOnCloseToTray: false,
  codexCliGuideDismissed: false,
  webAccessPasswordConfigured: false,
  locale: "zh-CN",
  localeOptions: ["zh-CN", "en"],
  serviceAddr: "localhost:48760",
  serviceListenMode: "loopback",
  serviceListenModeOptions: ["loopback", "all_interfaces"],
  routeStrategy: "ordered",
  routeStrategyOptions: ["ordered", "balanced"],
  freeAccountMaxModel: "auto",
  freeAccountMaxModelOptions: ["auto", "gpt-5"],
  modelForwardRules: "",
  accountMaxInflight: 1,
  gatewayOriginator: "codex-cli",
  gatewayOriginatorDefault: "codex-cli",
  gatewayUserAgentVersion: "1.0.0",
  gatewayUserAgentVersionDefault: "1.0.0",
  gatewayResidencyRequirement: "",
  gatewayResidencyRequirementOptions: ["", "us"],
  pluginMarketMode: "builtin",
  pluginMarketSourceUrl: "",
  upstreamProxyUrl: "",
  upstreamStreamTimeoutMs: 600000,
  upstreamTotalTimeoutMs: 0,
  sseKeepaliveIntervalMs: 15000,
  backgroundTasks: {
    usagePollingEnabled: true,
    usagePollIntervalSecs: 600,
    gatewayKeepaliveEnabled: true,
    gatewayKeepaliveIntervalSecs: 180,
    tokenRefreshPollingEnabled: true,
    tokenRefreshPollIntervalSecs: 60,
    usageRefreshWorkers: 4,
    httpWorkerFactor: 4,
    httpWorkerMin: 8,
    httpStreamWorkerFactor: 1,
    httpStreamWorkerMin: 2,
  },
  envOverrides: {},
  envOverrideCatalog: [],
  envOverrideReservedKeys: [],
  envOverrideUnsupportedKeys: [],
  theme: "tech",
  appearancePreset: "classic",
};

async function mockRuntimeAndRpc(page: Page) {
  let settingsSnapshot = { ...SETTINGS_SNAPSHOT };
  const settingsSetPayloads: Record<string, unknown>[] = [];

  await page.route(/\/api\/runtime\/?(?:\?.*)?$/, async (route) => {
    await route.fulfill({
      contentType: "application/json; charset=utf-8",
      body: JSON.stringify({
        mode: "web-gateway",
        rpcBaseUrl: "/api/rpc",
        canManageService: false,
        canSelfUpdate: false,
        canCloseToTray: false,
        canOpenLocalDir: false,
        canUseBrowserFileImport: true,
        canUseBrowserDownloadExport: true,
      }),
    });
  });

  await page.route(/\/api\/rpc\/?(?:\?.*)?$/, async (route) => {
    const payload = route.request().postDataJSON();
    const method = typeof payload?.method === "string" ? payload.method : "";
    const id = payload?.id ?? 1;
    const params =
      payload?.params && typeof payload.params === "object"
        ? (payload.params as Record<string, unknown>)
        : {};

    const ok = (result: unknown) =>
      route.fulfill({
        contentType: "application/json; charset=utf-8",
        body: JSON.stringify({
          jsonrpc: "2.0",
          id,
          result,
        }),
      });

    if (method === "appSettings/get") {
      await ok(settingsSnapshot);
      return;
    }
    if (method === "appSettings/set") {
      settingsSetPayloads.push(params);
      settingsSnapshot = {
        ...settingsSnapshot,
        ...params,
      };
      await ok(settingsSnapshot);
      return;
    }
    if (method === "initialize") {
      await ok({
        userAgent: "codex_cli_rs/0.1.19",
        codexHome: "C:/Users/Test/.codex",
        platformFamily: "windows",
        platformOs: "windows",
      });
      return;
    }
    if (method === "aggregateApi/list") {
      await ok({ items: [] });
      return;
    }
    if (method === "gateway/concurrencyRecommendation/get") {
      await ok({
        usageRefreshWorkers: 4,
        httpWorkerFactor: 4,
        httpWorkerMin: 8,
        httpStreamWorkerFactor: 1,
        httpStreamWorkerMin: 2,
        accountMaxInflight: 1,
      });
      return;
    }

    await route.fulfill({
      status: 500,
      contentType: "application/json; charset=utf-8",
      body: JSON.stringify({
        jsonrpc: "2.0",
        id,
        error: {
          code: -32000,
          message: `Unhandled RPC method in test: ${method}`,
        },
      }),
    });
  });

  return { settingsSetPayloads };
}

test("temporary Codex CLI guide close survives a hard reload in the same tab", async ({
  page,
}) => {
  await mockRuntimeAndRpc(page);

  await page.goto("/aggregate-api/");
  await expect(
    page.getByRole("heading", { name: "Codex CLI 首次接入引导" }),
  ).toBeVisible();

  await page.getByRole("button", { name: "本次关闭" }).click();
  await expect(
    page.getByRole("heading", { name: "Codex CLI 首次接入引导" }),
  ).not.toBeVisible();

  await page.reload();
  await expect(
    page.getByRole("columnheader", { name: "供应商 / URL" }).last(),
  ).toBeVisible();
  await expect(
    page.getByRole("heading", { name: "Codex CLI 首次接入引导" }),
  ).not.toBeVisible();
});

test("collapsed sidebar brand uses the app title", async ({ page }) => {
  await mockRuntimeAndRpc(page);
  await page.addInitScript(() => {
    window.sessionStorage.setItem("codexmanager.codexCliGuide.sessionDismissed", "1");
  });

  await page.goto("/");
  const brandButton = page.getByRole("button", {
    name: "重新打开 Codex CLI 引导",
  });
  await expect(brandButton).toHaveAttribute("title", "重新打开 Codex CLI 引导");

  await page.getByRole("button", { name: "收起侧边栏" }).click();

  await expect(page.getByRole("button", { name: "CodexManager" })).toHaveAttribute(
    "title",
    "CodexManager",
  );
  await expect(page.getByRole("button", { name: "展开侧边栏" })).toHaveAttribute(
    "title",
    "展开侧边栏",
  );
});

test("checking don't show again persists the guide dismissal before reload", async ({
  page,
}) => {
  const { settingsSetPayloads } = await mockRuntimeAndRpc(page);

  await page.goto("/aggregate-api/");
  await expect(
    page.getByRole("heading", { name: "Codex CLI 首次接入引导" }),
  ).toBeVisible();

  await page
    .locator('[role="checkbox"][aria-label="下次不再显示这份引导"]')
    .setChecked(true);
  await page.getByRole("button", { name: "保存并关闭" }).click();

  await expect
    .poll(() =>
      settingsSetPayloads.some(
        (payload) => payload.codexCliGuideDismissed === true,
      ),
    )
    .toBe(true);
  await expect(
    page.getByRole("heading", { name: "Codex CLI 首次接入引导" }),
  ).not.toBeVisible();

  await page.evaluate(() => window.sessionStorage.clear());
  await page.reload();
  await expect(
    page.getByRole("columnheader", { name: "供应商 / URL" }).last(),
  ).toBeVisible();
  await expect(
    page.getByRole("heading", { name: "Codex CLI 首次接入引导" }),
  ).not.toBeVisible();
});
