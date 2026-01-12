const DEFAULT_TIMEZONE = 'Asia/Taipei';

function normalizeTimezone(value) {
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (trimmed) {
      return trimmed;
    }
  }
  return DEFAULT_TIMEZONE;
}

function getAppTimezone(env = process.env) {
  if (env && typeof env.TMAG_TIMEZONE === 'string') {
    const normalized = env.TMAG_TIMEZONE.trim();
    if (normalized) {
      return normalized;
    }
  }
  return DEFAULT_TIMEZONE;
}

function formatTimestampLabel(date, { timeZone = getAppTimezone(), locale = 'zh-TW', showSeconds = true } = {}) {
  const dt = date instanceof Date ? date : new Date(date);
  if (Number.isNaN(dt.getTime())) {
    return '';
  }
  const formatter = new Intl.DateTimeFormat(locale, {
    timeZone,
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: showSeconds ? '2-digit' : undefined,
    hour12: false
  });
  return formatter.format(dt);
}

module.exports = {
  DEFAULT_TIMEZONE,
  getAppTimezone,
  normalizeTimezone,
  formatTimestampLabel
};
