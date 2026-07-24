/* global document, localStorage, matchMedia */

(() => {
  const key = "cmclient.web.preferences.v1";
  const root = document.documentElement;
  let preferences;

  try {
    preferences = JSON.parse(localStorage.getItem(key) || "{}");
  } catch {
    preferences = {};
  }

  const theme =
    preferences.theme === "light" ||
    preferences.theme === "dark" ||
    preferences.theme === "system"
      ? preferences.theme
      : "system";
  const resolvedTheme =
    theme === "system"
      ? matchMedia("(prefers-color-scheme: dark)").matches
        ? "dark"
        : "light"
      : theme;
  const locale = preferences.locale === "en-US" ? "en-US" : "zh-Hant-TW";

  root.dataset.theme = resolvedTheme;
  root.classList.toggle("cm-dark", resolvedTheme === "dark");
  root.lang = locale;
  root.style.colorScheme = resolvedTheme;
})();
