'use strict';

const os = require('os');
const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

const DEFAULT_BASE_URL = 'https://callmesh.tmmarc.org';
const DEFAULT_PRODUCT = 'callmesh-client';

class CallMeshClient {
  constructor(options) {
    const {
      apiKey,
      baseUrl = DEFAULT_BASE_URL,
      product = DEFAULT_PRODUCT,
      version,
      agent,
      platform,
      fetchImpl = globalThis.fetch
    } = options || {};

    if (!apiKey) {
      throw new Error('CallMeshClient 需要提供 apiKey');
    }
    if (typeof fetchImpl !== 'function') {
      throw new Error('CallMeshClient 需要提供可用的 fetch 實作');
    }

    this.apiKey = apiKey;
    this.baseUrl = baseUrl.replace(/\/+$/, '');
    this.product = product;
    this.version = version || getPackageVersion();
    this.fetch = fetchImpl;
    this.agentString = agent || buildAgentString({
      product: this.product,
      version: this.version,
      platformOverride: platform
    });
  }

  async heartbeat({ localHash, timeout = 10000 } = {}) {
    return this._post('/api/v1/client/heartbeat', {
      body: {
        local_hash: localHash ?? null,
        agent: this.agentString
      },
      timeout
    });
  }

  async fetchMappings({ knownHash, timeout = 15000 } = {}) {
    return this._post('/api/v1/client/mappings', {
      body: {
        known_hash: knownHash ?? null
      },
      timeout
    });
  }

  async _post(pathname, { body, timeout }) {
    const url = new URL(pathname, `${this.baseUrl}/`).toString();
    const controller = timeout ? new AbortController() : null;
    let timeoutId;

    if (controller && timeout > 0) {
      timeoutId = setTimeout(() => controller.abort(), timeout);
    }

    let response;
    try {
      response = await this.fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-API-Key': this.apiKey,
          'X-Client-Agent': this.agentString
        },
        body: JSON.stringify(body),
        signal: controller?.signal
      });
    } catch (err) {
      if (err.name === 'AbortError') {
        throw new Error(`請求逾時 (${timeout}ms)`);
      }
      throw err;
    } finally {
      if (timeoutId) clearTimeout(timeoutId);
    }

    if (!response.ok) {
      let message = `CallMesh API 失敗 (${response.status})`;
      try {
        const data = await response.json();
        if (data?.message) {
          message = `${message}: ${data.message}`;
        }
      } catch {
        // ignore
      }
      throw new Error(message);
    }

    return response.json();
  }
}

function buildAgentString({ product, version, platformOverride }) {
  const resolvedProduct = product || DEFAULT_PRODUCT;
  const resolvedVersion = version || getPackageVersion();
  const platformInfo = platformOverride || detectPlatformInfo();
  const arch = normalizeArch(os.arch());
  const platformSection = arch ? `${platformInfo}; ${arch}` : platformInfo;
  return `${resolvedProduct}/${resolvedVersion} (${platformSection})`;
}

function detectPlatformInfo() {
  const platform = os.platform();
  if (platform === 'win32') {
    const release = os.release().split('.').slice(0, 2).join('.');
    return `Windows NT ${release}`;
  }

  if (platform === 'darwin') {
    const version = getMacVersion();
    return `macOS ${version}`;
  }

  if (platform === 'linux') {
    const distro = getLinuxDistro();
    return distro || 'Linux';
  }

  return os.type();
}

function getMacVersion() {
  try {
    const output = execSync('sw_vers -productVersion', { encoding: 'utf8' }).trim();
    if (output) {
      const parts = output.split('.');
      if (parts.length > 2) {
        return `${parts[0]}.${parts[1]}`;
      }
      return output;
    }
  } catch {
    // ignore
  }
  // Fallback: derive from Darwin version (coarse)
  const darwinRelease = os.release().split('.');
  const major = Number(darwinRelease[0] || 0);
  if (major >= 23) return '14.0';
  if (major === 22) return '13.0';
  if (major === 21) return '12.0';
  return 'macOS';
}

function getLinuxDistro() {
  try {
    const osReleasePath = '/etc/os-release';
    if (fs.existsSync(osReleasePath)) {
      const content = fs.readFileSync(osReleasePath, 'utf8');
      const map = {};
      for (const line of content.split('\n')) {
        const match = line.match(/^([^=]+)=(.*)$/);
        if (match) {
          const key = match[1];
          let value = match[2];
          if (value.startsWith('"') && value.endsWith('"')) {
            value = value.slice(1, -1);
          }
          map[key] = value;
        }
      }
      if (map.PRETTY_NAME) return map.PRETTY_NAME;
      if (map.NAME && map.VERSION_ID) return `${map.NAME} ${map.VERSION_ID}`;
      if (map.NAME) return map.NAME;
    }
  } catch {
    // ignore
  }
  return null;
}

function normalizeArch(arch) {
  switch (arch) {
    case 'x64':
      return 'x64';
    case 'arm64':
      return 'arm64';
    case 'ia32':
      return 'x86';
    default:
      return arch;
  }
}

function getPackageVersion() {
  try {
    const packagePath = path.resolve(__dirname, '..', '..', 'package.json');
    const pkg = JSON.parse(fs.readFileSync(packagePath, 'utf8'));
    return pkg.version || '1.0.0';
  } catch {
    return '1.0.0';
  }
}

module.exports = {
  CallMeshClient,
  buildAgentString,
  detectPlatformInfo
};
