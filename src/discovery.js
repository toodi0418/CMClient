'use strict';

const net = require('net');
const BonjourModule = require('bonjour-service');
const Bonjour = BonjourModule.Bonjour || BonjourModule.default || BonjourModule;

function discoverMeshtasticDevices(options = {}) {
  const { timeout = 5000 } = options;
  return new Promise((resolve, reject) => {
    const bonjour = new Bonjour();
    const results = new Map();

    let resolved = false;

    const finalize = () => {
      if (resolved) return;
      resolved = true;
      browser.stop();
      bonjour.destroy();
      resolve(Array.from(results.values()));
    };

    const browser = bonjour.find({ type: 'meshtastic' });

    browser.on('up', (service) => {
      const normalized = normalizeService(service);
      if (!normalized) {
        return;
      }
      const key = normalized.fqdn || `${normalized.host}:${normalized.port}`;
      const existing = results.get(key);
      if (existing) {
        results.set(key, mergeServiceInfo(existing, normalized));
      } else {
        results.set(key, normalized);
      }
    });

    browser.on('error', (err) => {
      if (resolved) return;
      resolved = true;
      browser.stop();
      bonjour.destroy();
      reject(err);
    });

    setTimeout(finalize, timeout);
  });
}

function normalizeService(service) {
  if (!service) return null;
  const collected = [];
  if (Array.isArray(service.addresses)) {
    for (const addr of service.addresses) {
      if (addr && !collected.includes(addr)) {
        collected.push(addr);
      }
    }
  }
  if (service.referer?.address && !collected.includes(service.referer.address)) {
    collected.push(service.referer.address);
  }
  const addresses = orderAddresses(collected);
  const txt = service.txt || {};
  const fqdn = normalizeHostname(service.fqdn);
  const hostCandidates = [
    normalizeHostname(service.host),
    fqdn,
    addresses.find((addr) => net.isIP(addr) === 4),
    addresses[0]
  ];
  const host = hostCandidates.find((value) => value && value.length) || '';

  return {
    name: service.name || txt.shortname || txt.id || '',
    host,
    port: service.port,
    addresses,
    txt,
    fqdn,
    raw: {
      type: service.type,
      protocol: service.protocol,
      subtypes: Array.isArray(service.subtypes) ? service.subtypes.slice() : undefined
    }
  };
}

function mergeServiceInfo(existing, incoming) {
  const mergedAddresses = orderAddresses([
    ...(existing.addresses || []),
    ...(incoming.addresses || [])
  ]);
  const mergedTxt = {
    ...(existing.txt || {}),
    ...(incoming.txt || {})
  };
  const hostCandidates = [
    incoming.host,
    existing.host,
    mergedAddresses.find((addr) => net.isIP(addr) === 4),
    mergedAddresses[0]
  ];
  const host = hostCandidates.find((value) => value && value.length) || '';

  return {
    ...existing,
    ...incoming,
    name: incoming.name || existing.name,
    host,
    port: incoming.port ?? existing.port,
    addresses: mergedAddresses,
    txt: mergedTxt,
    fqdn: incoming.fqdn || existing.fqdn,
    raw: {
      ...(existing.raw || {}),
      ...(incoming.raw || {})
    }
  };
}

function normalizeHostname(value) {
  if (typeof value !== 'string') {
    return '';
  }
  const trimmed = value.trim();
  if (!trimmed) return '';
  return trimmed.endsWith('.') ? trimmed.slice(0, -1) : trimmed;
}

function orderAddresses(addresses) {
  const unique = [];
  for (const addr of addresses) {
    if (addr && !unique.includes(addr)) {
      unique.push(addr);
    }
  }
  const ipv4 = unique.filter((addr) => net.isIP(addr) === 4);
  const ipv6 = unique.filter((addr) => net.isIP(addr) === 6);
  const rest = unique.filter((addr) => net.isIP(addr) === 0);
  return [...ipv4, ...ipv6, ...rest];
}

module.exports = {
  discoverMeshtasticDevices
};
