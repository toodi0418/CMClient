import { describe, expect, it } from "vitest";

import {
  documentLanguage,
  parseStoredPreferences,
  resolveLocale,
  resolveTheme,
} from "./preferences";

describe("web preferences", () => {
  it("uses only supported stored values", () => {
    expect(parseStoredPreferences('{"theme":"dark","locale":"en-US"}')).toEqual(
      {
        theme: "dark",
        locale: "en-US",
      },
    );
    expect(
      parseStoredPreferences('{"theme":"violet","locale":"fr-FR"}'),
    ).toEqual({});
    expect(parseStoredPreferences("not-json")).toEqual({});
  });

  it("resolves system theme and locale preferences deterministically", () => {
    expect(resolveTheme("system", true)).toBe("dark");
    expect(resolveTheme("system", false)).toBe("light");
    expect(resolveTheme("light", true)).toBe("light");
    expect(resolveLocale(["en-GB", "zh-TW"])).toBe("en-US");
    expect(resolveLocale(["ja-JP"])).toBe("zh-TW");
    expect(documentLanguage("zh-TW")).toBe("zh-Hant-TW");
  });
});
