(() => {
  const connectionModeSelect = document.getElementById('connection-mode');
  const serialSettingsBlock = document.getElementById('serial-settings-block');
  const serialDeviceSelect = document.getElementById('serial-device-select');
  const serialRefreshBtn = document.getElementById('serial-refresh-btn');
  const serialStatus = document.getElementById('serial-status');
  const settingsHostInput = document.getElementById('settings-host');
  const discoverBtn = document.getElementById('discover-btn');
  const discoverStatus = document.getElementById('discover-status');
  const apiKeyInput = document.getElementById('api-key');
  const apiKeyHint = document.getElementById('api-key-hint');
  const saveApiKeyBtn = document.getElementById('save-api-key');
  const aprsServerInput = document.getElementById('aprs-server');
  const aprsBeaconIntervalInput = document.getElementById('aprs-beacon-interval');
  const webUiEnabledCheckbox = document.getElementById('web-ui-enabled');
  const tracerouteEnabledCheckbox = document.getElementById('traceroute-enabled');
  const tracerouteRateMinutesInput = document.getElementById('traceroute-rate-minutes');
  const tracerouteIntervalSecondsInput = document.getElementById('traceroute-interval-seconds');
  const tenmanShareCheckbox = document.getElementById('tenman-share-enabled');
  const webUrlInput = document.getElementById('web-url');
  const openWebBtn = document.getElementById('open-web');
  const resetDataBtn = document.getElementById('reset-data-btn');
  const settingsStatus = document.getElementById('settings-status');
  const discoverModal = document.getElementById('discover-modal');
  const discoverModalBody = document.getElementById('discover-modal-body');
  const discoverModalCancel = document.getElementById('discover-modal-cancel');

  const DEFAULT_APRS_SERVER = 'asia.aprs2.net';
  const DEFAULT_APRS_BEACON_MINUTES = 10;
  const DEFAULT_TRACEROUTE_RATE_MINUTES = 30;
  const DEFAULT_TRACEROUTE_INTERVAL_SECONDS = 60;

  let preferences = {};
  let discoveredDevices = [];
  let pendingSaveTimer = null;
  let statusSubscription = null;
  let webProbeTimer = null;
  let lastWebUrl = '';

  function setStatus(message, variant = '') {
    if (!settingsStatus) return;
    settingsStatus.textContent = message || '';
    if (variant) {
      settingsStatus.dataset.variant = variant;
    } else {
      delete settingsStatus.dataset.variant;
    }
  }

  function setDiscoverStatus(message, variant = '') {
    if (!discoverStatus) return;
    discoverStatus.textContent = message || '';
    if (variant) {
      discoverStatus.dataset.variant = variant;
    } else {
      delete discoverStatus.dataset.variant;
    }
  }

  function setSerialStatus(message, variant = '') {
    if (!serialStatus) return;
    serialStatus.textContent = message || '';
    if (variant) {
      serialStatus.dataset.variant = variant;
    } else {
      delete serialStatus.dataset.variant;
    }
  }

  function isIPv4(value) {
    if (!value || typeof value !== 'string') return false;
    const parts = value.split('.');
    if (parts.length !== 4) return false;
    return parts.every((part) => {
      const num = Number(part);
      return Number.isFinite(num) && num >= 0 && num <= 255 && String(num) === part;
    });
  }

  function pickDeviceAddress(device) {
    if (!device || typeof device !== 'object') return null;
    const address = Array.isArray(device.addresses)
      ? device.addresses.find((item) => isIPv4(item))
      : null;
    return address || device.host || null;
  }

  function showDiscoverModal(devices) {
    if (!discoverModal || !discoverModalBody) return;
    discoverModalBody.innerHTML = '';
    if (!devices.length) {
      setDiscoverStatus('未找到 Meshtastic 裝置，請確認裝置是否與本機同網段。', 'warn');
      hideDiscoverModal();
      return;
    }
    devices.forEach((device, index) => {
      const button = document.createElement('button');
      const label = device.name || device.host || 'Meshtastic';
      const address = pickDeviceAddress(device);
      const port = device.port ?? 4403;
      const metaParts = [];
      if (address) metaParts.push(`${address}:${port}`);
      if (device.txt) {
        const txtEntries = Object.entries(device.txt)
          .filter(([, value]) => value)
          .map(([key, value]) => `${key}=${value}`);
        if (txtEntries.length) metaParts.push(txtEntries.join(', '));
      }
      button.textContent = metaParts.length ? `${label} · ${metaParts.join(' · ')}` : label;
      button.dataset.index = String(index);
      discoverModalBody.appendChild(button);
    });
    discoverModal.classList.remove('hidden');
  }

  function hideDiscoverModal() {
    discoverModal?.classList.add('hidden');
  }

  function buildSerialHost(path) {
    if (!path) return '';
    return `serial://${path}`;
  }

  function parseSerialHost(host) {
    if (!host) return null;
    const trimmed = host.trim();
    if (!trimmed.toLowerCase().startsWith('serial:')) return null;
    let remainder = trimmed.slice(trimmed.indexOf(':') + 1);
    if (remainder.startsWith('//')) {
      remainder = remainder.slice(2);
    }
    const queryIndex = remainder.indexOf('?');
    if (queryIndex >= 0) {
      remainder = remainder.slice(0, queryIndex);
    }
    const atIndex = remainder.lastIndexOf('@');
    if (atIndex > 0) {
      remainder = remainder.slice(0, atIndex);
    }
    const path = remainder.trim();
    return path ? { path } : null;
  }

  function normalizeConnectionMode(mode) {
    return mode === 'serial' ? 'serial' : 'tcp';
  }

  function applyConnectionMode(mode) {
    const normalized = normalizeConnectionMode(mode);
    if (connectionModeSelect) {
      connectionModeSelect.value = normalized;
    }
    if (serialSettingsBlock) {
      serialSettingsBlock.classList.toggle('hidden', normalized !== 'serial');
    }
    if (discoverBtn) {
      discoverBtn.disabled = normalized === 'serial';
      discoverBtn.classList.toggle('hidden', normalized === 'serial');
    }
  }

  function applySerialSelection(path) {
    if (!serialDeviceSelect) return;
    if (path) {
      serialDeviceSelect.value = path;
    } else {
      serialDeviceSelect.value = '';
    }
    if (normalizeConnectionMode(connectionModeSelect?.value) === 'serial' && path) {
      settingsHostInput.value = buildSerialHost(path);
    }
  }

  function getTracerouteRateMinutes() {
    const raw = Number(tracerouteRateMinutesInput?.value);
    if (!Number.isFinite(raw) || raw <= 0) return DEFAULT_TRACEROUTE_RATE_MINUTES;
    return Math.max(15, Math.round(raw));
  }

  function getTracerouteIntervalSeconds() {
    const raw = Number(tracerouteIntervalSecondsInput?.value);
    if (!Number.isFinite(raw) || raw <= 0) return DEFAULT_TRACEROUTE_INTERVAL_SECONDS;
    return Math.max(15, Math.round(raw));
  }

  function getAprsBeaconMinutes() {
    const raw = Number(aprsBeaconIntervalInput?.value);
    if (!Number.isFinite(raw) || raw <= 0) return DEFAULT_APRS_BEACON_MINUTES;
    const rounded = Math.round(raw);
    if (rounded < 1) return 1;
    if (rounded > 1440) return 1440;
    return rounded;
  }

  function buildPreferencesPayload() {
    return {
      connectionMode: normalizeConnectionMode(connectionModeSelect?.value),
      host: settingsHostInput?.value.trim() || '',
      serialPath: serialDeviceSelect?.value || '',
      webDashboardEnabled: Boolean(webUiEnabledCheckbox?.checked),
      shareWithTenmanMap: tenmanShareCheckbox ? Boolean(tenmanShareCheckbox.checked) : true,
      autoTracerouteEnabled: tracerouteEnabledCheckbox ? Boolean(tracerouteEnabledCheckbox.checked) : true,
      tracerouteRateMinutes: getTracerouteRateMinutes(),
      tracerouteIntervalSeconds: getTracerouteIntervalSeconds(),
      aprsServer: aprsServerInput?.value.trim() || DEFAULT_APRS_SERVER,
      aprsBeaconMinutes: getAprsBeaconMinutes()
    };
  }

  async function refreshSerialDeviceList({ quiet = false } = {}) {
    if (!window.tmagSettings?.listSerialPorts) {
      setSerialStatus('無法取得 Serial 清單', 'error');
      return;
    }
    try {
      const ports = await window.tmagSettings.listSerialPorts();
      const normalized = Array.isArray(ports)
        ? ports.filter((item) => item && typeof item.path === 'string')
        : [];
      const current = serialDeviceSelect?.value || '';
      if (serialDeviceSelect) {
        serialDeviceSelect.innerHTML = '';
        const placeholder = document.createElement('option');
        placeholder.value = '';
        placeholder.textContent = '請選擇 Serial 裝置';
        serialDeviceSelect.appendChild(placeholder);
        for (const port of normalized) {
          const option = document.createElement('option');
          option.value = port.path;
          const labelParts = [port.path];
          if (port.friendlyName) {
            labelParts.push(port.friendlyName);
          } else if (port.manufacturer) {
            labelParts.push(port.manufacturer);
          }
          option.textContent = labelParts.join(' / ');
          serialDeviceSelect.appendChild(option);
        }
        if (current) {
          serialDeviceSelect.value = current;
        } else if (preferences.serialPath) {
          serialDeviceSelect.value = preferences.serialPath;
        }
      }
      if (!quiet) {
        setSerialStatus(
          normalized.length ? `找到 ${normalized.length} 個裝置` : '未找到裝置',
          normalized.length ? 'success' : 'warn'
        );
      }
    } catch (err) {
      setSerialStatus(`取得 Serial 清單失敗：${err.message || err}`, 'error');
    }
  }

  async function savePreferences({ restart = true, statusMessage = '已套用設定' } = {}) {
    if (!window.tmagSettings?.save) return;
    if (pendingSaveTimer) {
      clearTimeout(pendingSaveTimer);
      pendingSaveTimer = null;
    }
    const payload = buildPreferencesPayload();
    try {
      const result = await window.tmagSettings.save({
        ...payload,
        apiKeyAction: 'keep',
        restart
      });
      if (result?.success) {
        setStatus(result.statusText || statusMessage);
        preferences = payload;
      } else {
        setStatus(result?.error || '儲存失敗', 'error');
      }
    } catch (err) {
      setStatus(`儲存失敗：${err.message || err}`, 'error');
    }
  }

  function schedulePreferencesSave({ restart = true, statusMessage } = {}) {
    if (pendingSaveTimer) {
      clearTimeout(pendingSaveTimer);
    }
    pendingSaveTimer = setTimeout(() => {
      savePreferences({ restart, statusMessage });
    }, 600);
  }

  async function probeWebUi(url) {
    if (!url) return;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 2500);
    try {
      const response = await fetch(url, { method: 'GET', signal: controller.signal });
      if (response.ok) {
        setStatus('Web UI 已啟動，請用瀏覽器開啟查看。');
        hideDiscoverModal();
      } else {
        setStatus('Web UI 尚未就緒，請稍候或重新啟動後端。', 'warn');
      }
    } catch (err) {
      setStatus('Web UI 尚未就緒，請稍候或重新啟動後端。', 'warn');
    } finally {
      clearTimeout(timeout);
    }
  }

  function scheduleWebUiProbe(delayMs = 800) {
    if (webProbeTimer) {
      clearTimeout(webProbeTimer);
    }
    webProbeTimer = setTimeout(() => {
      probeWebUi(lastWebUrl);
    }, delayMs);
  }

  async function handleSaveApiKey() {
    if (!window.tmagSettings?.save) return;
    const trimmed = apiKeyInput.value.trim();
    saveApiKeyBtn.disabled = true;
    const actionLabel = trimmed ? '正在驗證 API Key...' : '正在清除 API Key...';
    setStatus(actionLabel, '');
    try {
      const result = await window.tmagSettings.save({
        ...buildPreferencesPayload(),
        apiKeyAction: trimmed ? 'set' : 'clear',
        apiKey: trimmed,
        restart: true
      });
      if (result?.success) {
        apiKeyInput.value = '';
        setStatus(result.statusText || (trimmed ? 'API Key 驗證成功' : 'API Key 已清除'));
        await loadSettings();
      } else {
        setStatus(result?.error || 'API Key 驗證失敗', 'error');
      }
    } catch (err) {
      setStatus(`API Key 驗證失敗：${err.message || err}`, 'error');
    } finally {
      saveApiKeyBtn.disabled = false;
    }
  }

  async function loadSettings() {
    try {
      hideDiscoverModal();
      const data = await window.tmagSettings.get();
      preferences = data.preferences || {};
      applyConnectionMode(preferences.connectionMode || 'tcp');
      settingsHostInput.value = preferences.host || '';
      const serialSpec = parseSerialHost(settingsHostInput.value);
      if (serialSpec) {
        applyConnectionMode('serial');
      }
      await refreshSerialDeviceList({ quiet: true });
      applySerialSelection(preferences.serialPath || serialSpec?.path || '');
      if (aprsServerInput) {
        aprsServerInput.value = preferences.aprsServer || DEFAULT_APRS_SERVER;
      }
      if (aprsBeaconIntervalInput) {
        const minutes = Number(preferences.aprsBeaconMinutes);
        const normalized =
          Number.isFinite(minutes) && minutes >= 1
            ? Math.min(Math.round(minutes), 1440)
            : DEFAULT_APRS_BEACON_MINUTES;
        aprsBeaconIntervalInput.value = String(normalized);
      }
      if (webUiEnabledCheckbox) {
        webUiEnabledCheckbox.checked = preferences.webDashboardEnabled !== false;
      }
      if (tenmanShareCheckbox) {
        tenmanShareCheckbox.checked = preferences.shareWithTenmanMap !== false;
      }
      if (tracerouteEnabledCheckbox) {
        tracerouteEnabledCheckbox.checked = preferences.autoTracerouteEnabled !== false;
      }
      if (tracerouteRateMinutesInput) {
        const rate = Number(preferences.tracerouteRateMinutes);
        tracerouteRateMinutesInput.value = String(
          Number.isFinite(rate) && rate >= 1
            ? Math.max(15, Math.round(rate))
            : DEFAULT_TRACEROUTE_RATE_MINUTES
        );
      }
      if (tracerouteIntervalSecondsInput) {
        const interval = Number(preferences.tracerouteIntervalSeconds);
        tracerouteIntervalSecondsInput.value = String(
          Number.isFinite(interval) && interval >= 1
            ? Math.max(15, Math.round(interval))
            : DEFAULT_TRACEROUTE_INTERVAL_SECONDS
        );
      }

      const verified = Boolean(data.verification?.verified);
      const degraded = Boolean(data.verification?.degraded);
      const heartbeat = data.verification?.lastHeartbeatAt;
      const verifiedLabel = verified
        ? degraded
          ? '已驗證（降級）'
          : '已驗證'
        : '尚未驗證';
      apiKeyHint.textContent = `Key 狀態：${verifiedLabel}${heartbeat ? ` · 上次心跳 ${heartbeat}` : ''}`;
      if (webUrlInput && data.web) {
        lastWebUrl = `http://${data.web.host}:${data.web.port}/`;
        if ('value' in webUrlInput) {
          webUrlInput.value = lastWebUrl;
        } else {
          webUrlInput.textContent = lastWebUrl;
        }
      }

      setStatus('設定已載入');
      hideDiscoverModal();
      scheduleWebUiProbe(500);
    } catch (err) {
      setStatus(`載入設定失敗：${err.message || err}`, 'error');
    }
  }

  async function handleDiscover() {
    if (normalizeConnectionMode(connectionModeSelect?.value) === 'serial') {
      setDiscoverStatus('Serial 模式不支援自動搜尋', 'warn');
      return;
    }
    if (!window.tmagSettings?.discover) return;
    setDiscoverStatus('正在搜尋區網內的裝置...', 'info');
    try {
      const results = await window.tmagSettings.discover({ timeout: 4000 });
      discoveredDevices = Array.isArray(results) ? results : [];
      if (!discoveredDevices.length) {
        hideDiscoverModal();
        showDiscoverModal([]);
        return;
      }
      setDiscoverStatus(`找到 ${discoveredDevices.length} 個裝置。`, 'success');
      showDiscoverModal(discoveredDevices);
    } catch (err) {
      setDiscoverStatus(`搜尋失敗：${err.message || err}`, 'error');
      hideDiscoverModal();
    }
  }

  function handleDiscoverSelect(index) {
    const device = discoveredDevices[index];
    if (!device) return;
    const address = pickDeviceAddress(device);
    if (address) {
      settingsHostInput.value = address;
      schedulePreferencesSave({ restart: true, statusMessage: `已套用 ${device.name || address}` });
    } else {
      setDiscoverStatus('該裝置沒有可用的位址', 'warn');
    }
    hideDiscoverModal();
  }

  async function handleResetData() {
    if (!window.confirm('確定要清除所有本地資料與 API Key 嗎？')) {
      return;
    }
    if (!window.tmagSettings?.resetData) return;
    setStatus('正在重置資料...', '');
    try {
      await window.tmagSettings.resetData();
      setStatus('已重置資料，請重新設定 API Key 與連線參數');
      await loadSettings();
    } catch (err) {
      setStatus(`重置失敗：${err.message || err}`, 'error');
    }
  }

  connectionModeSelect?.addEventListener('change', () => {
    applyConnectionMode(connectionModeSelect.value);
    schedulePreferencesSave({ restart: true, statusMessage: '連線模式已更新' });
  });

  serialDeviceSelect?.addEventListener('change', () => {
    const value = serialDeviceSelect.value || '';
    if (value) {
      applySerialSelection(value);
      setSerialStatus(`已選擇 ${value}`, 'success');
    }
    schedulePreferencesSave({ restart: true, statusMessage: 'Serial 設定已更新' });
  });

  serialRefreshBtn?.addEventListener('click', () => {
    refreshSerialDeviceList();
  });

  settingsHostInput?.addEventListener('change', () => {
    const trimmed = settingsHostInput.value.trim();
    const serialSpec = parseSerialHost(trimmed);
    if (serialSpec) {
      applyConnectionMode('serial');
      applySerialSelection(serialSpec.path);
    }
    schedulePreferencesSave({ restart: true, statusMessage: '連線目標已更新' });
  });

  discoverBtn?.addEventListener('click', handleDiscover);

  discoverModalBody?.addEventListener('click', (event) => {
    const target = event.target.closest('button');
    if (!target) return;
    const index = Number(target.dataset.index);
    if (Number.isFinite(index)) {
      handleDiscoverSelect(index);
    }
  });

  discoverModalCancel?.addEventListener('click', hideDiscoverModal);
  discoverModal?.addEventListener('click', (event) => {
    if (event.target === discoverModal) hideDiscoverModal();
  });

  saveApiKeyBtn?.addEventListener('click', handleSaveApiKey);

  aprsServerInput?.addEventListener('change', () => {
    if (!aprsServerInput.value.trim()) {
      aprsServerInput.value = DEFAULT_APRS_SERVER;
    }
    schedulePreferencesSave({ restart: true, statusMessage: 'APRS 伺服器已更新' });
  });

  aprsBeaconIntervalInput?.addEventListener('change', () => {
    const minutes = getAprsBeaconMinutes();
    aprsBeaconIntervalInput.value = String(minutes);
    schedulePreferencesSave({ restart: true, statusMessage: 'APRS 信標間隔已更新' });
  });

  webUiEnabledCheckbox?.addEventListener('change', () => {
    schedulePreferencesSave({ restart: true, statusMessage: 'Web UI 狀態已更新' });
  });

  tracerouteEnabledCheckbox?.addEventListener('change', () => {
    schedulePreferencesSave({ restart: true, statusMessage: 'Traceroute 設定已更新' });
  });

  tracerouteRateMinutesInput?.addEventListener('change', () => {
    tracerouteRateMinutesInput.value = String(getTracerouteRateMinutes());
    schedulePreferencesSave({ restart: true, statusMessage: 'Traceroute 間隔已更新' });
  });

  tracerouteIntervalSecondsInput?.addEventListener('change', () => {
    tracerouteIntervalSecondsInput.value = String(getTracerouteIntervalSeconds());
    schedulePreferencesSave({ restart: true, statusMessage: 'Traceroute 佇列間隔已更新' });
  });

  tenmanShareCheckbox?.addEventListener('change', () => {
    schedulePreferencesSave({ restart: true, statusMessage: 'TenManMap 分享設定已更新' });
  });

  openWebBtn?.addEventListener('click', async () => {
    if (!window.tmagSettings?.openWeb) return;
    try {
      await window.tmagSettings.openWeb();
    } catch (err) {
      setStatus(`開啟 Web UI 失敗：${err.message || err}`, 'error');
    }
  });

  resetDataBtn?.addEventListener('click', handleResetData);

  if (window.tmagSettingsEvents?.onStatus) {
    statusSubscription = window.tmagSettingsEvents.onStatus((message) => {
      if (message) {
        setStatus(message, 'warn');
        scheduleWebUiProbe(1200);
      }
    });
  }

  loadSettings();
})();
