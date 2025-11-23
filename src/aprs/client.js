'use strict';

const net = require('net');
const EventEmitter = require('events');

class APRSClient extends EventEmitter {
  constructor(options = {}) {
    super();
    this.server = options.server || 'asia.aprs2.net';
    this.port = options.port || 14580;
    this.callsign = options.callsign;
    this.passcode = options.passcode;
    this.version = options.version || '0.0.0';
    this.softwareName = options.softwareName || 'TMAG';
    this.log = options.log || (() => {});
    const filterCandidate =
      Object.prototype.hasOwnProperty.call(options, 'filterCommand') ?
        options.filterCommand :
        'filter m/2';
    if (typeof filterCandidate === 'string' && filterCandidate.trim()) {
      this.filterCommand = filterCandidate.trim();
    } else {
      this.filterCommand = null;
    }

    this.socket = null;
    this.reconnectTimer = null;
    this.retryDelayMs = 30_000;
    this.connected = false;
    this.keepaliveIntervalMs = options.keepaliveIntervalMs || 30_000;
    this.keepaliveTimer = null;
    this.keepaliveKickTimer = null;
  }

  sendLine(line) {
    if (!this.connected || !this.socket) {
      this._log('APRS', `tx skipped (not connected): ${line}`);
      return false;
    }
    const payload = line.endsWith('\r\n') ? line : `${line}\r\n`;
    try {
      this.socket.write(payload);
      this._log('APRS', `tx ${line}`);
      return true;
    } catch (err) {
      this._log('APRS', `tx fail ${err.message}`);
      return false;
    }
  }

  updateConfig(config) {
    let changed = false;
    if (config.server && config.server !== this.server) {
      this.server = config.server;
      changed = true;
    }
    if (config.port && config.port !== this.port) {
      this.port = config.port;
      changed = true;
    }
    if (config.callsign && config.callsign !== this.callsign) {
      this.callsign = config.callsign;
      changed = true;
    }
    if (typeof config.passcode === 'number' && config.passcode !== this.passcode) {
      this.passcode = config.passcode;
      changed = true;
    }
    if (config.version && config.version !== this.version) {
      this.version = config.version;
      changed = true;
    }
    if (Object.prototype.hasOwnProperty.call(config, 'filterCommand')) {
      const candidate = config.filterCommand;
      const normalized =
        typeof candidate === 'string' && candidate.trim() ? candidate.trim() : null;
      if (normalized !== this.filterCommand) {
        this.filterCommand = normalized;
        changed = true;
      }
    }
    if (changed && this.connected) {
      this._log('APRS', 'configuration changed，重新連線');
      this.disconnect();
      this.connect();
    }
  }

  connect() {
    if (!this.server || !this.callsign || this.passcode == null) {
      this._log('APRS', '配置不足，無法連線');
      return;
    }
    this._clearReconnect();
    this._clearKeepalive();
    if (this.socket) {
      this.socket.removeAllListeners();
      this.socket.destroy();
      this.socket = null;
    }
    this.connected = false;
    this._log('APRS', `connecting ${this.server}:${this.port} as ${this.callsign}`);
    this.socket = net.createConnection(
      { host: this.server, port: this.port },
      () => this._onConnect()
    );
    this.socket.setKeepAlive(true, 60_000);

    this.socket.on('data', (data) => {
      const text = data.toString('utf8');
      if (!text) return;
      const lines = text.split(/\r?\n/);
      for (const rawLine of lines) {
        const line = rawLine.trim();
        if (!line) continue;
        this._log('APRS', `rx ${line}`);
        this.emit('line', line);
      }
    });

    this.socket.on('error', (err) => {
      this._log('APRS', `error ${err.message}`);
    });

    this.socket.on('close', () => {
      this._log('APRS', 'connection closed');
      this.connected = false;
      this._clearKeepalive();
      this.socket?.destroy();
      this.socket = null;
      this.emit('disconnected');
      this._scheduleReconnect();
    });
  }

  disconnect() {
    this._clearReconnect();
    this._clearKeepalive();
    if (this.socket) {
      this._log('APRS', 'disconnecting');
      this.socket.end();
      this.socket.destroy();
      this.socket = null;
    }
    this.connected = false;
  }

  _onConnect() {
    this.connected = true;
    this._log('APRS', 'connected');
    const loginFilterSection = this.filterCommand ? '' : ' filter m/2';
    const login = `user ${this.callsign} pass ${this.passcode} vers ${this.softwareName} ${this.version}${loginFilterSection}\r\n`;
    this.socket.write(login);
    this.socket.write(`# TMAG connected at ${new Date().toISOString()}\r\n`);
    if (this.filterCommand) {
      this.socket.write(`${this.filterCommand}\r\n`);
    }
    this._scheduleKeepalive();
    this.emit('connected', {
      server: this.server,
      port: this.port,
      callsign: this.callsign
    });
  }

  _scheduleReconnect() {
    if (this.reconnectTimer) return;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, this.retryDelayMs);
  }

  _clearReconnect() {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
  }

  _scheduleKeepalive() {
    this._clearKeepalive();
    const sendKeepalive = () => {
      if (!this.connected || !this.socket) return;
      try {
        this.socket.write('# keepalive\r\n');
      } catch (err) {
        this._log('APRS', `keepalive fail ${err.message}`);
      }
    };

    const initialDelay = Math.max(5_000, Math.min(20_000, Math.floor(this.keepaliveIntervalMs / 2))); // 5-20s
    this.keepaliveKickTimer = setTimeout(() => {
      this.keepaliveKickTimer = null;
      sendKeepalive();
    }, initialDelay);

    this.keepaliveTimer = setInterval(() => {
      sendKeepalive();
    }, this.keepaliveIntervalMs);
  }

  _clearKeepalive() {
    if (this.keepaliveTimer) {
      clearInterval(this.keepaliveTimer);
      this.keepaliveTimer = null;
    }
    if (this.keepaliveKickTimer) {
      clearTimeout(this.keepaliveKickTimer);
      this.keepaliveKickTimer = null;
    }
  }

  _log(tag, message) {
    try {
      this.log(tag, message);
    } catch {
      // ignore
    }
  }
}

module.exports = {
  APRSClient
};
