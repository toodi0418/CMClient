'use strict';

const net = require('net');
const { SerialPort } = require('serialport');
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');

const DEFAULT_TCP_HOST = '127.0.0.1';
const DEFAULT_TCP_PORT = 4403;
const DEFAULT_PROXY_HOST = '0.0.0.0';
const DEFAULT_PROXY_PORT = 4403;
const DEFAULT_SERIAL_BAUD = 115200;
const DEFAULT_RECONNECT_DELAY_MS = 5000;
const DEFAULT_CONNECT_TIMEOUT_MS = 15000;

function log(tag, message) {
  const timestamp = new Date().toISOString();
  console.log(`[${timestamp}] [${tag}] ${message}`);
}

function toPositiveNumber(value, fallback) {
  const numeric = Number(value);
  return Number.isFinite(numeric) && numeric > 0 ? numeric : fallback;
}

function parseSerialEndpoint(input) {
  if (!input || typeof input !== 'string') return null;
  const trimmed = input.trim();
  if (!trimmed.toLowerCase().startsWith('serial:')) {
    return null;
  }

  let remainder = trimmed.slice(trimmed.indexOf(':') + 1);
  if (remainder.startsWith('//')) {
    remainder = remainder.slice(2);
  }

  let query = '';
  const queryIndex = remainder.indexOf('?');
  if (queryIndex >= 0) {
    query = remainder.slice(queryIndex + 1);
    remainder = remainder.slice(0, queryIndex);
  }

  let baudRate = null;
  const atIndex = remainder.lastIndexOf('@');
  if (atIndex > 0) {
    const candidate = remainder.slice(atIndex + 1).trim();
    if (/^\d+$/.test(candidate)) {
      baudRate = Number(candidate);
      remainder = remainder.slice(0, atIndex);
    }
  }

  if (query) {
    const params = new URLSearchParams(query);
    for (const key of ['baud', 'baudrate']) {
      if (params.has(key)) {
        const value = Number(params.get(key));
        if (Number.isFinite(value) && value > 0) {
          baudRate = value;
          break;
        }
      }
    }
  }

  const path = remainder.trim();
  if (!path) {
    return null;
  }

  return {
    path,
    baudRate: Number.isFinite(baudRate) && baudRate > 0 ? baudRate : null
  };
}

function buildConfig(argv) {
  const envConnection =
    typeof process.env.MESHTASTIC_CONNECTION === 'string'
      ? process.env.MESHTASTIC_CONNECTION.trim().toLowerCase()
      : '';
  const envHost =
    typeof process.env.MESHTASTIC_HOST === 'string' ? process.env.MESHTASTIC_HOST.trim() : '';
  const hostInput = (argv.host ? String(argv.host).trim() : '') || envHost || DEFAULT_TCP_HOST;
  const serialSpec = parseSerialEndpoint(hostInput);

  let connection = argv.connection || envConnection || '';
  connection = typeof connection === 'string' ? connection.trim().toLowerCase() : '';
  if (connection !== 'tcp' && connection !== 'serial') {
    connection =
      serialSpec ||
      (typeof argv.serialPath === 'string' && argv.serialPath.trim()) ||
      (typeof process.env.MESHTASTIC_SERIAL_PATH === 'string' &&
        process.env.MESHTASTIC_SERIAL_PATH.trim())
        ? 'serial'
        : 'tcp';
  }

  const serialPathFromArgs =
    typeof argv.serialPath === 'string' && argv.serialPath.trim() ? argv.serialPath.trim() : '';
  const serialPathFromEnv =
    typeof process.env.MESHTASTIC_SERIAL_PATH === 'string'
      ? process.env.MESHTASTIC_SERIAL_PATH.trim()
      : '';
  const serialPath = serialPathFromArgs || serialPathFromEnv || serialSpec?.path || '';

  let serialBaud =
    serialSpec?.baudRate ||
    toPositiveNumber(argv.serialBaud, null) ||
    toPositiveNumber(process.env.MESHTASTIC_SERIAL_BAUD, DEFAULT_SERIAL_BAUD);

  if (!serialBaud) {
    serialBaud = DEFAULT_SERIAL_BAUD;
  }

  const host = hostInput || DEFAULT_TCP_HOST;
  const port =
    toPositiveNumber(argv.port, null) ||
    toPositiveNumber(process.env.MESHTASTIC_PORT, DEFAULT_TCP_PORT);

  const listenHost =
    (typeof argv.listenHost === 'string' && argv.listenHost.trim()) ||
    (typeof process.env.MESHTASTIC_PROXY_HOST === 'string' &&
      process.env.MESHTASTIC_PROXY_HOST.trim()) ||
    (typeof process.env.PROXY_HOST === 'string' && process.env.PROXY_HOST.trim()) ||
    DEFAULT_PROXY_HOST;

  const listenPort =
    toPositiveNumber(argv.listenPort, null) ||
    toPositiveNumber(process.env.MESHTASTIC_PROXY_PORT, null) ||
    toPositiveNumber(process.env.PROXY_PORT, DEFAULT_PROXY_PORT);

  const reconnectDelayMs =
    toPositiveNumber(argv.reconnectDelay, null) ||
    toPositiveNumber(process.env.PROXY_RECONNECT_DELAY_MS, DEFAULT_RECONNECT_DELAY_MS);

  const connectTimeoutMs =
    toPositiveNumber(argv.connectTimeout, null) ||
    toPositiveNumber(process.env.PROXY_CONNECT_TIMEOUT_MS, DEFAULT_CONNECT_TIMEOUT_MS);

  if (connection === 'serial' && !serialPath) {
    throw new Error('Serial 模式必須提供 --serial-path 或 MESHTASTIC_SERIAL_PATH');
  }

  return {
    connection,
    host,
    port,
    serialPath,
    serialBaud,
    listenHost,
    listenPort,
    reconnectDelayMs,
    connectTimeoutMs
  };
}

class MeshtasticTcpProxyService {
  constructor(config) {
    this.config = config;
    this.server = null;
    this.clients = new Set();
    this.upstreamSocket = null;
    this.serialPort = null;
    this.connectTimer = null;
    this.reconnectTimer = null;
    this.stopping = false;
    this.upstreamConnected = false;
    this.connectionAttempt = 0;
  }

  async start() {
    this.startServer();
    this.connectUpstream();
  }

  async stop() {
    this.stopping = true;
    this.upstreamConnected = false;

    if (this.connectTimer) {
      clearTimeout(this.connectTimer);
      this.connectTimer = null;
    }

    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }

    if (this.server) {
      const server = this.server;
      this.server = null;
      await new Promise((resolve) => {
        server.close(() => resolve());
      }).catch(() => {});
    }

    for (const client of this.clients) {
      try {
        client.destroy();
      } catch {
        // ignore
      }
    }
    this.clients.clear();

    if (this.upstreamSocket) {
      try {
        this.upstreamSocket.removeAllListeners();
        this.upstreamSocket.destroy();
      } catch {
        // ignore
      }
      this.upstreamSocket = null;
    }

    if (this.serialPort) {
      const port = this.serialPort;
      this.serialPort = null;
      try {
        port.removeAllListeners();
      } catch {
        // ignore
      }
      try {
        if (port.isOpen) {
          await new Promise((resolve) => port.close(() => resolve()));
        }
      } catch {
        // ignore
      }
    }

    log('proxy', 'service stopped');
  }

  startServer() {
    if (this.server) {
      return;
    }

    this.server = net.createServer((client) => {
      const clientId = `${client.remoteAddress || 'unknown'}:${client.remotePort || 'unknown'}`;
      this.clients.add(client);
      log('proxy', `client connected: ${clientId}`);

      client.on('data', (chunk) => {
        if (!this.upstreamConnected) {
          log('proxy', `drop ${chunk.length} bytes from ${clientId}: upstream disconnected`);
          return;
        }
        this.writeToUpstream(chunk, clientId);
      });

      client.on('error', (err) => {
        log('proxy', `client error (${clientId}): ${err.message}`);
      });

      client.on('close', () => {
        this.clients.delete(client);
        log('proxy', `client disconnected: ${clientId}`);
      });
    });

    this.server.on('error', (err) => {
      log('proxy', `server error: ${err.message}`);
    });

    this.server.listen(this.config.listenPort, this.config.listenHost, () => {
      log('proxy', `listening on ${this.config.listenHost}:${this.config.listenPort}`);
    });
  }

  connectUpstream() {
    if (this.stopping) {
      return;
    }
    this.connectionAttempt += 1;
    if (this.config.connection === 'serial') {
      this.connectSerial();
    } else {
      this.connectTcp();
    }
  }

  connectTcp() {
    const { host, port, connectTimeoutMs } = this.config;
    log('upstream', `connecting to tcp://${host}:${port} (attempt ${this.connectionAttempt})`);

    const socket = net.createConnection(
      {
        host,
        port,
        timeout: connectTimeoutMs
      },
      () => {
        this.upstreamSocket = socket;
        this.upstreamConnected = true;
        if (this.connectTimer) {
          clearTimeout(this.connectTimer);
          this.connectTimer = null;
        }
        socket.setTimeout(0);
        socket.setKeepAlive(true, 15000);
        log('upstream', `connected to tcp://${host}:${port}`);
      }
    );

    this.upstreamSocket = socket;

    this.connectTimer = setTimeout(() => {
      this.connectTimer = null;
      if (!this.upstreamConnected && this.upstreamSocket === socket) {
        socket.destroy(new Error(`connect timeout after ${connectTimeoutMs}ms`));
      }
    }, connectTimeoutMs);
    this.connectTimer.unref?.();

    socket.on('data', (chunk) => {
      this.broadcast(chunk);
    });

    socket.on('error', (err) => {
      log('upstream', `tcp error: ${err.message}`);
    });

    socket.on('close', () => {
      this.handleUpstreamClosed('tcp');
    });

    socket.on('end', () => {
      this.handleUpstreamClosed('tcp');
    });
  }

  connectSerial() {
    const { serialPath, serialBaud, connectTimeoutMs } = this.config;
    log('upstream', `connecting to serial://${serialPath} @ ${serialBaud} (attempt ${this.connectionAttempt})`);

    let port;
    try {
      port = new SerialPort({
        path: serialPath,
        baudRate: serialBaud,
        autoOpen: false
      });
    } catch (err) {
      log('upstream', `serial create error: ${err.message}`);
      this.scheduleReconnect();
      return;
    }

    this.serialPort = port;
    this.connectTimer = setTimeout(() => {
      this.connectTimer = null;
      if (!this.upstreamConnected && this.serialPort === port) {
        log('upstream', `serial connect timeout after ${connectTimeoutMs}ms`);
        try {
          port.close(() => {});
        } catch {
          // ignore
        }
      }
    }, connectTimeoutMs);
    this.connectTimer.unref?.();

    port.on('open', () => {
      this.upstreamConnected = true;
      if (this.connectTimer) {
        clearTimeout(this.connectTimer);
        this.connectTimer = null;
      }
      log('upstream', `connected to serial://${serialPath} @ ${serialBaud}`);
    });

    port.on('data', (chunk) => {
      this.broadcast(chunk);
    });

    port.on('error', (err) => {
      log('upstream', `serial error: ${err.message}`);
    });

    port.on('close', () => {
      this.handleUpstreamClosed('serial');
    });

    port.open((err) => {
      if (err) {
        log('upstream', `serial open failed: ${err.message}`);
        this.handleUpstreamClosed('serial');
      }
    });
  }

  handleUpstreamClosed(type) {
    if (this.stopping) {
      return;
    }

    if (this.connectTimer) {
      clearTimeout(this.connectTimer);
      this.connectTimer = null;
    }

    if (type === 'tcp' && this.upstreamSocket) {
      try {
        this.upstreamSocket.removeAllListeners();
        this.upstreamSocket.destroy();
      } catch {
        // ignore
      }
      this.upstreamSocket = null;
    }

    if (type === 'serial' && this.serialPort) {
      try {
        this.serialPort.removeAllListeners();
      } catch {
        // ignore
      }
      this.serialPort = null;
    }

    const wasConnected = this.upstreamConnected;
    this.upstreamConnected = false;
    log('upstream', wasConnected ? `${type} disconnected` : `${type} unavailable`);
    this.scheduleReconnect();
  }

  scheduleReconnect() {
    if (this.stopping || this.reconnectTimer) {
      return;
    }
    log('upstream', `reconnect in ${this.config.reconnectDelayMs}ms`);
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connectUpstream();
    }, this.config.reconnectDelayMs);
    this.reconnectTimer.unref?.();
  }

  writeToUpstream(chunk, clientId = 'unknown') {
    if (!chunk || chunk.length === 0) {
      return;
    }

    if (!this.upstreamConnected) {
      log('proxy', `drop ${chunk.length} bytes from ${clientId}: upstream disconnected`);
      return;
    }

    if (this.config.connection === 'serial') {
      const port = this.serialPort;
      if (!port) {
        log('proxy', `drop ${chunk.length} bytes from ${clientId}: serial port unavailable`);
        return;
      }
      port.write(chunk, (err) => {
        if (err) {
          log('proxy', `write to serial failed (${clientId}): ${err.message}`);
          return;
        }
        if (typeof port.drain === 'function') {
          port.drain((drainErr) => {
            if (drainErr) {
              log('proxy', `serial drain failed (${clientId}): ${drainErr.message}`);
            }
          });
        }
      });
      return;
    }

    const socket = this.upstreamSocket;
    if (!socket) {
      log('proxy', `drop ${chunk.length} bytes from ${clientId}: tcp socket unavailable`);
      return;
    }

    socket.write(chunk, (err) => {
      if (err) {
        log('proxy', `write to tcp failed (${clientId}): ${err.message}`);
      }
    });
  }

  broadcast(chunk) {
    if (!chunk || chunk.length === 0 || this.clients.size === 0) {
      return;
    }

    for (const client of this.clients) {
      if (client.destroyed) {
        continue;
      }
      try {
        client.write(chunk);
      } catch (err) {
        log('proxy', `broadcast failed: ${err.message}`);
      }
    }
  }
}

async function main() {
  const argv = await yargs(hideBin(process.argv))
    .scriptName('meshtastic-tcp-proxy')
    .option('connection', {
      alias: 'C',
      choices: ['tcp', 'serial'],
      describe: '上游 Meshtastic 連線方式'
    })
    .option('host', {
      alias: 'H',
      type: 'string',
      describe: 'TCP 上游主機，或 serial:// 路徑'
    })
    .option('port', {
      alias: 'P',
      type: 'number',
      describe: 'TCP 上游連接埠'
    })
    .option('serial-path', {
      type: 'string',
      describe: 'Serial 上游裝置路徑'
    })
    .option('serial-baud', {
      type: 'number',
      describe: 'Serial 上游鮑率'
    })
    .option('listen-host', {
      type: 'string',
      describe: 'Proxy 監聽位址'
    })
    .option('listen-port', {
      type: 'number',
      describe: 'Proxy 監聽埠號'
    })
    .option('reconnect-delay', {
      type: 'number',
      describe: '上游斷線重連延遲（毫秒）'
    })
    .option('connect-timeout', {
      type: 'number',
      describe: '上游連線逾時（毫秒）'
    })
    .help()
    .version(false)
    .parse();

  const config = buildConfig({
    connection: argv.connection,
    host: argv.host,
    port: argv.port,
    serialPath: argv.serialPath,
    serialBaud: argv.serialBaud,
    listenHost: argv.listenHost,
    listenPort: argv.listenPort,
    reconnectDelay: argv.reconnectDelay,
    connectTimeout: argv.connectTimeout
  });

  log(
    'config',
    JSON.stringify(
      {
        connection: config.connection,
        upstream:
          config.connection === 'serial'
            ? `serial://${config.serialPath}@${config.serialBaud}`
            : `tcp://${config.host}:${config.port}`,
        listen: `${config.listenHost}:${config.listenPort}`,
        reconnectDelayMs: config.reconnectDelayMs,
        connectTimeoutMs: config.connectTimeoutMs
      },
      null,
      2
    )
  );

  const service = new MeshtasticTcpProxyService(config);

  const shutdown = async (signal) => {
    log('proxy', `received ${signal}, shutting down`);
    process.removeListener('SIGINT', sigintHandler);
    process.removeListener('SIGTERM', sigtermHandler);
    await service.stop();
    process.exit(0);
  };

  const sigintHandler = () => {
    shutdown('SIGINT').catch((err) => {
      log('proxy', `shutdown error: ${err.message}`);
      process.exit(1);
    });
  };

  const sigtermHandler = () => {
    shutdown('SIGTERM').catch((err) => {
      log('proxy', `shutdown error: ${err.message}`);
      process.exit(1);
    });
  };

  process.on('SIGINT', sigintHandler);
  process.on('SIGTERM', sigtermHandler);

  await service.start();
}

main().catch((err) => {
  log('proxy', `fatal error: ${err.message}`);
  process.exit(1);
});
