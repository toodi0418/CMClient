(() => {
  const backendStatusEl = document.getElementById('backend-status');
  const webStatusEl = document.getElementById('web-status');
  const webUrlEl = document.getElementById('web-url');
  const nodeLongEl = document.getElementById('node-long');
  const nodeShortEl = document.getElementById('node-short');
  const statusMessageEl = document.getElementById('status-message');
  const versionEl = document.getElementById('app-version');
  const openSettingsBtn = document.getElementById('open-settings');
  const openWebBtn = document.getElementById('open-web');
  const openAboutBtn = document.getElementById('open-about');
  const aboutModal = document.getElementById('about-modal');
  const closeAboutBtn = document.getElementById('close-about');
  const minimizeBtn = document.getElementById('window-minimize');
  const closeBtn = document.getElementById('window-close');
  const lightCallmesh = document.getElementById('light-callmesh');
  const lightAprsWrap = document.getElementById('light-aprs-wrap');
  const lightAprs = document.getElementById('light-aprs');
  const lightWeb = document.getElementById('light-web');
  const lightNode = document.getElementById('light-node');
  const lightRx = document.getElementById('light-rx');

  let refreshTimer = null;
  let selfInfo = null;
  let nodeSnapshot = [];
  let lastWebUrl = '';
  let pendingResize = null;
  let selfErrorCount = 0;
  const appStartedAt = Date.now();
  let rxTimer = null;
  let aprsTimer = null;

  function setValue(el, text, variant = '') {
    if (!el) return;
    el.textContent = text || '';
    el.classList.remove('ok', 'warn', 'danger');
    if (variant) {
      el.classList.add(variant);
    }
  }

  function setDot(el, variant) {
    if (!el) return;
    el.classList.remove('ok', 'warn', 'danger');
    if (variant) {
      el.classList.add(variant);
    }
  }

  function flashRx() {
    if (!lightRx) return;
    lightRx.classList.remove('flash');
    lightRx.classList.add('rx');
    requestAnimationFrame(() => {
      lightRx.classList.add('flash');
    });
    if (rxTimer) {
      clearTimeout(rxTimer);
    }
    rxTimer = setTimeout(() => {
      lightRx.classList.remove('flash');
      lightRx.classList.remove('rx');
    }, 700);
  }

  function flashAprs() {
    if (!lightAprs || lightAprsWrap?.classList.contains('hidden')) return;
    lightAprs.classList.remove('flash-aprs');
    requestAnimationFrame(() => {
      lightAprs.classList.add('flash-aprs');
    });
    if (aprsTimer) {
      clearTimeout(aprsTimer);
    }
    aprsTimer = setTimeout(() => {
      lightAprs.classList.remove('flash-aprs');
    }, 700);
  }

  function scheduleResize() {
    if (!window.tmagStatus?.resize) return;
    if (pendingResize) {
      cancelAnimationFrame(pendingResize);
    }
    pendingResize = requestAnimationFrame(() => {
      pendingResize = null;
      const height = Math.ceil(document.documentElement.scrollHeight);
      window.tmagStatus.resize(height);
    });
  }

  function updateNodeDisplay() {
    const meshId = selfInfo?.meshId || null;
    let longName = '';
    let shortName = '';
    if (meshId && Array.isArray(nodeSnapshot)) {
      const match = nodeSnapshot.find((node) => {
        if (!node) return false;
        return (
          node.meshId === meshId ||
          node.meshIdNormalized === meshId ||
          node.meshIdOriginal === meshId
        );
      });
      if (match) {
        longName = match.longName || match.label || '';
        shortName = match.shortName || '';
      }
    }
    if (!longName && selfInfo?.name) {
      longName = selfInfo.name;
    }
    setValue(nodeLongEl, longName || '尚未提供長名稱');
    setValue(nodeShortEl, shortName || '尚未提供短名稱');
    setDot(lightNode, longName || shortName ? 'ok' : 'warn');
    scheduleResize();
  }

  async function refreshSelfInfo(webUrl, webEnabled) {
    if (!webEnabled || !webUrl) {
      selfInfo = null;
      nodeSnapshot = [];
      updateNodeDisplay();
      selfErrorCount = 0;
      return;
    }
    if (!window.tmagStatus?.getSelf) return;
    const payload = await window.tmagStatus.getSelf();
    if (payload?.error) {
      selfErrorCount += 1;
      const elapsed = Date.now() - appStartedAt;
      if (elapsed > 12000 && selfErrorCount >= 2) {
        setStatusMessage(`節點資訊讀取失敗：${payload.error}`, 'warn');
      }
      updateNodeDisplay();
      return;
    }
    selfErrorCount = 0;
    if (statusMessageEl?.textContent?.startsWith('節點資訊讀取失敗')) {
      setStatusMessage('', '');
    }
    selfInfo = payload?.selfInfo || null;
    nodeSnapshot = Array.isArray(payload?.nodeSnapshot) ? payload.nodeSnapshot : [];
    updateNodeDisplay();
  }

  async function refreshStatus() {
    if (!window.tmagStatus?.get) return;
    try {
      const data = await window.tmagStatus.get();
      if (!data) return;
      const running = Boolean(data.backendRunning);
      setValue(backendStatusEl, running ? '執行中' : '已停止', running ? 'ok' : 'warn');

      const webEnabled = data.preferences?.webDashboardEnabled !== false;
      setValue(webStatusEl, webEnabled ? '啟用' : '停用', webEnabled ? 'ok' : 'warn');
      if (openWebBtn) {
        openWebBtn.disabled = !webEnabled;
      }
      if (webUrlEl) {
        webUrlEl.textContent = data.web?.url || '-';
        webUrlEl.title = data.web?.url || '';
      }
      if (versionEl) {
        versionEl.textContent = data.appVersion ? `v${data.appVersion}` : 'v-';
      }
      setDot(lightWeb, webEnabled ? 'ok' : 'warn');
      const callmesh = data.callmesh || null;
      if (!callmesh || !callmesh.hasKey) {
        setDot(lightCallmesh, 'danger');
      } else if (callmesh.verified && !callmesh.degraded) {
        setDot(lightCallmesh, 'ok');
      } else if (callmesh.degraded) {
        setDot(lightCallmesh, 'warn');
      } else {
        setDot(lightCallmesh, 'warn');
      }
      const aprs = callmesh?.aprs || null;
      const aprsProvisioned = Boolean(aprs?.callsign);
      if (lightAprsWrap) {
        lightAprsWrap.classList.toggle('hidden', !aprsProvisioned);
      }
      if (aprsProvisioned) {
        setDot(lightAprs, aprs?.connected ? 'ok' : 'warn');
      }
      if (data.web?.url && data.web.url !== lastWebUrl) {
        lastWebUrl = data.web.url;
        selfInfo = null;
        nodeSnapshot = [];
      }
      await refreshSelfInfo(data.web?.url || '', webEnabled);
      if (data.lastStatusMessage) {
        setStatusMessage(data.lastStatusMessage, running ? 'ok' : 'warn');
      }
      scheduleResize();
    } catch (err) {
      setStatusMessage(`狀態讀取失敗：${err.message || err}`, 'warn');
      scheduleResize();
    }
  }

  function setStatusMessage(message, variant = '') {
    if (!statusMessageEl) return;
    statusMessageEl.textContent = message || '';
    statusMessageEl.classList.remove('ok', 'warn');
    if (variant) {
      statusMessageEl.classList.add(variant);
    }
    scheduleResize();
  }

  openSettingsBtn?.addEventListener('click', async () => {
    if (!window.tmagStatus?.openSettings) return;
    await window.tmagStatus.openSettings();
  });

  openWebBtn?.addEventListener('click', async () => {
    if (!window.tmagStatus?.openWeb) return;
    await window.tmagStatus.openWeb();
  });

  const closeAbout = () => {
    aboutModal?.classList.add('hidden');
  };

  openAboutBtn?.addEventListener('click', () => {
    aboutModal?.classList.remove('hidden');
  });

  closeAboutBtn?.addEventListener('click', closeAbout);

  aboutModal?.addEventListener('click', (event) => {
    if (event.target === aboutModal) {
      closeAbout();
    }
  });

  minimizeBtn?.addEventListener('click', async () => {
    if (!window.tmagStatus?.minimize) return;
    await window.tmagStatus.minimize();
  });

  closeBtn?.addEventListener('click', async () => {
    if (!window.tmagStatus?.close) return;
    await window.tmagStatus.close();
  });

  if (window.tmagStatus?.onMessage) {
    window.tmagStatus.onMessage((message) => {
      if (message) {
        setStatusMessage(message, 'warn');
      }
    });
  }

  if (window.tmagStatus?.onRx) {
    window.tmagStatus.onRx(() => {
      flashRx();
    });
  }

  if (window.tmagStatus?.onAprs) {
    window.tmagStatus.onAprs(() => {
      flashAprs();
    });
  }

  refreshStatus();
  refreshTimer = setInterval(refreshStatus, 5000);

  window.addEventListener('beforeunload', () => {
    if (refreshTimer) {
      clearInterval(refreshTimer);
      refreshTimer = null;
    }
    if (pendingResize) {
      cancelAnimationFrame(pendingResize);
      pendingResize = null;
    }
    if (rxTimer) {
      clearTimeout(rxTimer);
      rxTimer = null;
    }
    if (aprsTimer) {
      clearTimeout(aprsTimer);
      aprsTimer = null;
    }
  });
})();
