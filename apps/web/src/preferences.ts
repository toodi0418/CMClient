export const preferenceStorageKey = "cmclient.web.preferences.v1";

export const themePreferences = ["light", "dark", "system"] as const;
export const supportedLocales = ["zh-TW", "en-US"] as const;

export type ThemePreference = (typeof themePreferences)[number];
export type ResolvedTheme = Exclude<ThemePreference, "system">;
export type SupportedLocale = (typeof supportedLocales)[number];

export type StoredPreferences = {
  theme?: ThemePreference;
  locale?: SupportedLocale;
};

export function isThemePreference(value: unknown): value is ThemePreference {
  return (
    typeof value === "string" &&
    themePreferences.includes(value as ThemePreference)
  );
}

export function isSupportedLocale(value: unknown): value is SupportedLocale {
  return (
    typeof value === "string" &&
    supportedLocales.includes(value as SupportedLocale)
  );
}

export function parseStoredPreferences(
  value: string | null,
): StoredPreferences {
  if (!value) {
    return {};
  }

  try {
    const parsed: unknown = JSON.parse(value);

    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return {};
    }

    const preferences = parsed as Record<string, unknown>;

    return {
      ...(isThemePreference(preferences.theme)
        ? { theme: preferences.theme }
        : {}),
      ...(isSupportedLocale(preferences.locale)
        ? { locale: preferences.locale }
        : {}),
    };
  } catch {
    return {};
  }
}

export function resolveTheme(
  preference: ThemePreference,
  systemPrefersDark: boolean,
): ResolvedTheme {
  if (preference === "system") {
    return systemPrefersDark ? "dark" : "light";
  }

  return preference;
}

export function resolveLocale(
  locales: readonly string[] | undefined,
): SupportedLocale {
  if (locales?.some((locale) => locale.toLowerCase().startsWith("en"))) {
    return "en-US";
  }

  return "zh-TW";
}

export function documentLanguage(locale: SupportedLocale) {
  return locale === "zh-TW" ? "zh-Hant-TW" : locale;
}

export function themeColor(theme: ResolvedTheme) {
  return theme === "dark" ? "#151817" : "#eef3ee";
}
