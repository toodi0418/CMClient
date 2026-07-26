import { afterEach, describe, expect, it } from "vitest";

import { i18n, setI18nLocale } from "./index";

afterEach(() => setI18nLocale("zh-TW"));

describe("management web i18n", () => {
  it("switches shared messages between the supported locales", () => {
    expect(i18n.global.t("navigation.overview")).toBe("總覽");

    setI18nLocale("en-US");

    expect(i18n.global.t("navigation.overview")).toBe("Overview");
    expect(i18n.global.t("preferences.system")).toBe("System");
    expect(i18n.global.t("setup.title")).toBe("Initial setup");
    expect(i18n.global.t("setup.validateAndStart")).toBe("Validate and start");
  });

  it("translates the setup wizard in Traditional Chinese", () => {
    expect(i18n.global.t("setup.title")).toBe("初始設定");
    expect(i18n.global.t("setup.validateAndStart")).toBe("驗證並啟動");
  });
});
