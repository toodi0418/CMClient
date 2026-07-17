import { createI18n } from "vue-i18n";

import type { SupportedLocale } from "@/preferences";

import { messages } from "./messages";

export const i18n = createI18n({
  legacy: false,
  locale: "zh-TW",
  fallbackLocale: "zh-TW",
  messages,
});

export function setI18nLocale(locale: SupportedLocale) {
  i18n.global.locale.value = locale;
}
