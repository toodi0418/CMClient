'use strict';

const normalizeMeshId = (meshId) => {
  if (meshId == null) return null;
  const value = String(meshId).trim();
  if (!value) return null;
  if (value.startsWith('!')) {
    return value.toLowerCase();
  }
  if (value.startsWith('0x') || value.startsWith('0X')) {
    return `!${value.slice(2).toLowerCase()}`;
  }
  return value.startsWith('!') ? value.toLowerCase() : `!${value.toLowerCase()}`;
};

class NodeDatabase {
  constructor() {
    this.nodes = new Map();
  }

  upsert(meshId, payload = {}) {
    const normalized = normalizeMeshId(meshId);
    if (!normalized) {
      return { changed: false, node: null };
    }
    const existing = this.nodes.get(normalized) || {
      meshId: normalized,
      meshIdOriginal: null,
      shortName: null,
      longName: null,
      hwModel: null,
      hwModelLabel: null,
      role: null,
      roleLabel: null,
      latitude: null,
      longitude: null,
      altitude: null,
      lastSeenAt: null
    };
    const merged = {
      meshId: normalized,
      meshIdOriginal: payload.meshIdOriginal ?? existing.meshIdOriginal ?? null,
      shortName: payload.shortName ?? existing.shortName ?? null,
      longName: payload.longName ?? existing.longName ?? null,
      hwModel: payload.hwModel ?? existing.hwModel ?? null,
      hwModelLabel: payload.hwModelLabel ?? existing.hwModelLabel ?? null,
      role: payload.role ?? existing.role ?? null,
      roleLabel: payload.roleLabel ?? existing.roleLabel ?? null,
      latitude: payload.latitude ?? existing.latitude ?? null,
      longitude: payload.longitude ?? existing.longitude ?? null,
      altitude: payload.altitude ?? existing.altitude ?? null,
      lastSeenAt: payload.lastSeenAt ?? existing.lastSeenAt ?? Date.now()
    };

    if (!Number.isFinite(merged.latitude) || Math.abs(merged.latitude) > 90) {
      merged.latitude = null;
    }
    if (!Number.isFinite(merged.longitude) || Math.abs(merged.longitude) > 180) {
      merged.longitude = null;
    }
    if (
      merged.latitude !== null &&
      merged.longitude !== null &&
      Math.abs(merged.latitude) < 1e-6 &&
      Math.abs(merged.longitude) < 1e-6
    ) {
      merged.latitude = null;
      merged.longitude = null;
    }
    if (!Number.isFinite(merged.altitude)) {
      merged.altitude = null;
    }
    if (merged.latitude === null || merged.longitude === null) {
      merged.altitude = null;
    }

    const changed =
      merged.meshIdOriginal !== existing.meshIdOriginal ||
      merged.shortName !== existing.shortName ||
      merged.longName !== existing.longName ||
      merged.hwModel !== existing.hwModel ||
      merged.hwModelLabel !== existing.hwModelLabel ||
      merged.role !== existing.role ||
      merged.roleLabel !== existing.roleLabel ||
      merged.latitude !== existing.latitude ||
      merged.longitude !== existing.longitude ||
      merged.altitude !== existing.altitude ||
      merged.lastSeenAt !== existing.lastSeenAt;
    if (changed) {
      this.nodes.set(normalized, merged);
    }
    return {
      changed,
      node: { ...merged }
    };
  }

  get(meshId) {
    const normalized = normalizeMeshId(meshId);
    if (!normalized) {
      return null;
    }
    const stored = this.nodes.get(normalized);
    return stored ? { ...stored } : null;
  }

  merge(meshId, info = {}) {
    const normalized = normalizeMeshId(meshId);
    if (!normalized) return;
    const result = this.upsert(normalized, info);
    return result.node;
  }

  list() {
    return Array.from(this.nodes.values())
      .map((node) => ({ ...node }))
      .sort((a, b) => {
        const timeA = Number.isFinite(a.lastSeenAt)
          ? a.lastSeenAt
          : typeof a.lastSeenAt === 'string'
            ? (Date.parse(a.lastSeenAt) || 0)
            : 0;
        const timeB = Number.isFinite(b.lastSeenAt)
          ? b.lastSeenAt
          : typeof b.lastSeenAt === 'string'
            ? (Date.parse(b.lastSeenAt) || 0)
            : 0;
        if (timeA !== timeB) {
          return timeB - timeA;
        }
        const labelA = (a.longName || a.shortName || a.meshId || '').toLowerCase();
        const labelB = (b.longName || b.shortName || b.meshId || '').toLowerCase();
        if (labelA < labelB) return -1;
        if (labelA > labelB) return 1;
        return 0;
      });
  }

  replace(entries = []) {
    this.nodes.clear();
    if (!Array.isArray(entries)) {
      return this.list();
    }
    const parseNumber = (value) => {
      const num = Number(value);
      if (Number.isFinite(num)) return num;
      if (typeof value === 'string' && value.trim()) {
        const parsed = Number(value.trim());
        if (Number.isFinite(parsed)) return parsed;
      }
      return null;
    };

    for (const entry of entries) {
      const normalized = normalizeMeshId(entry?.meshId ?? entry?.meshIdNormalized ?? entry?.meshIdOriginal);
      if (!normalized) continue;
      let lastSeenAt = null;
      const candidate = entry?.lastSeenAt;
      if (Number.isFinite(candidate)) {
        lastSeenAt = Number(candidate);
      } else if (typeof candidate === 'string' && candidate.trim()) {
        const numeric = Number(candidate);
        if (Number.isFinite(numeric)) {
          lastSeenAt = numeric;
        } else {
          const parsedDate = Date.parse(candidate);
          if (!Number.isNaN(parsedDate)) {
            lastSeenAt = parsedDate;
          }
        }
      }
      let latitude = parseNumber(
        entry?.latitude ??
          entry?.lat ??
          entry?.position?.latitude ??
          entry?.position?.lat
      );
      let longitude = parseNumber(
        entry?.longitude ??
          entry?.lon ??
          entry?.position?.longitude ??
          entry?.position?.lon
      );
      let altitude = parseNumber(
        entry?.altitude ??
          entry?.alt ??
          entry?.position?.altitude ??
          entry?.position?.alt
      );
      if (!Number.isFinite(latitude) || Math.abs(latitude) > 90) {
        latitude = null;
      }
      if (!Number.isFinite(longitude) || Math.abs(longitude) > 180) {
        longitude = null;
      }
      if (
        latitude !== null &&
        longitude !== null &&
        Math.abs(latitude) < 1e-6 &&
        Math.abs(longitude) < 1e-6
      ) {
        latitude = null;
        longitude = null;
      }
      if (!Number.isFinite(altitude)) {
        altitude = null;
      }
      if (latitude === null || longitude === null) {
        altitude = null;
      }
      const node = {
        meshId: normalized,
        meshIdOriginal: entry?.meshIdOriginal ?? entry?.meshId ?? null,
        shortName: entry?.shortName ?? null,
        longName: entry?.longName ?? null,
        hwModel: entry?.hwModel ?? null,
        hwModelLabel: entry?.hwModelLabel ?? null,
        role: entry?.role ?? null,
        roleLabel: entry?.roleLabel ?? null,
        latitude,
        longitude,
        altitude,
        lastSeenAt
      };
      this.nodes.set(normalized, node);
    }
    return this.list();
  }

  serialize() {
    return this.list();
  }

  clear() {
    const count = this.nodes.size;
    this.nodes.clear();
    return count;
  }

  size() {
    return this.nodes.size;
  }
}

module.exports = {
  NodeDatabase,
  nodeDatabase: new NodeDatabase()
};
