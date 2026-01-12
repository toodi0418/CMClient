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

module.exports = {
  DEFAULT_TIMEZONE,
  getAppTimezone,
  normalizeTimezone
};
