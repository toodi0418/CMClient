'use strict';

const { nodeDatabase } = require('../nodeDatabase');
const { normalizeMeshId } = require('../callmesh/aprsBridge');

const sanitizeSummaryNode = (node) => {
  if (!node || typeof node !== 'object') {
    return node;
  }

  const normalized = normalizeMeshId(
    node.meshIdNormalized || node.meshId || node.meshIdOriginal || null
  );
  const record = normalized && nodeDatabase?.get ? nodeDatabase.get(normalized) : null;
  if (!record) {
    return stripInlineNodeNames(node);
  }

  const meshIdValue =
    record.meshId ||
    record.meshIdNormalized ||
    record.meshIdOriginal ||
    node.meshId ||
    node.meshIdNormalized ||
    node.meshIdOriginal ||
    normalized;
  const preferredLabel =
    record.longName || record.shortName || meshIdValue || node.label || null;
  const label =
    preferredLabel && meshIdValue && !preferredLabel.includes(meshIdValue)
      ? `${preferredLabel} (${meshIdValue})`
      : preferredLabel || meshIdValue;

  return {
    ...node,
    meshId: record.meshId ?? node.meshId ?? meshIdValue,
    meshIdNormalized: normalized,
    meshIdOriginal: record.meshIdOriginal ?? node.meshIdOriginal ?? meshIdValue,
    longName: record.longName ?? null,
    shortName: record.shortName ?? null,
    label: label ?? null
  };
};

const stripInlineNodeNames = (node) => {
  const meshIdValue =
    node.meshId || node.meshIdNormalized || node.meshIdOriginal || null;
  const sanitized = { ...node, longName: null, shortName: null };
  if (meshIdValue) {
    sanitized.label = meshIdValue;
  } else if (typeof sanitized.label === 'string') {
    const match = sanitized.label.match(/(![0-9a-f]{2,})/i);
    if (match) {
      sanitized.label = match[1];
    }
  }
  return sanitized;
};

const sanitizeSummaryForDisplay = (summary) => {
  if (!summary || typeof summary !== 'object') {
    return summary;
  }
  return {
    ...summary,
    from: sanitizeSummaryNode(summary.from),
    to: sanitizeSummaryNode(summary.to),
    relay: sanitizeSummaryNode(summary.relay),
    nextHop: sanitizeSummaryNode(summary.nextHop)
  };
};

module.exports = {
  sanitizeSummaryForDisplay
};
