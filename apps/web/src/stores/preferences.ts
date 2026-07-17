import { defineStore } from "pinia";

import { setI18nLocale } from "@/i18n";
import {
  documentLanguage,
  parseStoredPreferences,
  preferenceStorageKey,
  resolveLocale,
  resolveTheme,
  themeColor,
  type ResolvedTheme,
  type SupportedLocale,
  type ThemePreference,
} from "@/preferences";

function readStoredPreferences() {
  try {
    return parseStoredPreferences(
      window.localStorage.getItem(preferenceStorageKey),
    );
  } catch {
    return {};
  }
}

function systemPrefersDark() {
  return window.matchMedia?.("(prefers-color-scheme: dark)").matches ?? false;
}

function applyDocumentTheme(theme: ResolvedTheme) {
  const root = document.documentElement;

  root.dataset.theme = theme;
  root.classList.toggle("cm-dark", theme === "dark");
  root.style.colorScheme = theme;
  document
    .querySelector('meta[name="theme-color"]')
    ?.setAttribute("content", themeColor(theme));
}

let systemThemeQuery: MediaQueryList | undefined;

export const usePreferencesStore = defineStore("preferences", {
  state: () => ({
    theme: "system" as ThemePreference,
    resolvedTheme: "dark" as ResolvedTheme,
    locale: "zh-TW" as SupportedLocale,
  }),
  actions: {
    initialize() {
      const stored = readStoredPreferences();

      this.theme = stored.theme ?? "system";
      this.locale = stored.locale ?? resolveLocale(navigator.languages);
      this.applyPreferences();
      this.watchSystemTheme();
    },
    setTheme(theme: ThemePreference) {
      this.theme = theme;
      this.applyPreferences();
      this.persist();
    },
    setLocale(locale: SupportedLocale) {
      this.locale = locale;
      this.applyPreferences();
      this.persist();
    },
    applyPreferences() {
      this.resolvedTheme = resolveTheme(this.theme, systemPrefersDark());
      applyDocumentTheme(this.resolvedTheme);
      document.documentElement.lang = documentLanguage(this.locale);
      setI18nLocale(this.locale);
    },
    persist() {
      try {
        window.localStorage.setItem(
          preferenceStorageKey,
          JSON.stringify({ theme: this.theme, locale: this.locale }),
        );
      } catch {
        // Storage can be disabled; the in-memory preference remains usable.
      }
    },
    watchSystemTheme() {
      if (systemThemeQuery) {
        return;
      }

      systemThemeQuery = window.matchMedia?.("(prefers-color-scheme: dark)");
      systemThemeQuery?.addEventListener("change", () => {
        if (this.theme === "system") {
          this.applyPreferences();
        }
      });
    },
  },
});
