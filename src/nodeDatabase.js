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
      role: null,
      lastSeenAt: null
    };
    const merged = {
      meshId: normalized,
      meshIdOriginal: payload.meshIdOriginal ?? existing.meshIdOriginal ?? null,
      shortName: payload.shortName ?? existing.shortName ?? null,
      longName: payload.longName ?? existing.longName ?? null,
      hwModel: payload.hwModel ?? existing.hwModel ?? null,
      role: payload.role ?? existing.role ?? null,
      lastSeenAt: payload.lastSeenAt ?? Date.now()
    };
    const changed =
      merged.meshIdOriginal !== existing.meshIdOriginal ||
      merged.shortName !== existing.shortName ||
      merged.longName !== existing.longName ||
      merged.hwModel !== existing.hwModel ||
      merged.role !== existing.role ||
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
        const labelA = (a.longName || a.shortName || a.meshId || '').toLowerCase();
        const labelB = (b.longName || b.shortName || b.meshId || '').toLowerCase();
        if (labelA < labelB) return -1;
        if (labelA > labelB) return 1;
        return 0;
      });
  }
}

module.exports = {
  NodeDatabase,
  nodeDatabase: new NodeDatabase()
};
