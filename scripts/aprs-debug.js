'use strict';

const net = require('net');

const HOST = process.env.APRS_HOST || 'asia.aprs2.net';
const PORT = Number(process.env.APRS_PORT) || 14580;
const CALLSIGN = process.env.APRS_CALLSIGN || process.env.CALLSIGN;
const PASSCODE = process.env.APRS_PASSCODE || process.env.PASSCODE;
const FILTER = process.env.APRS_FILTER || process.env.TMAG_APRS_FEED_FILTER || '';
const VERSION = process.env.APRS_VERSION || 'TMAG-Debug 0.0.0';
const KEEPALIVE_MS = Number(process.env.APRS_KEEPALIVE_MS) || 30_000;

if (!CALLSIGN || !PASSCODE) {
  console.error('請設定 APRS_CALLSIGN 與 APRS_PASSCODE 環境變數再執行。');
  process.exit(1);
}

const log = (direction, message) => {
  const timestamp = new Date().toISOString();
  process.stdout.write(`[${timestamp}] [${direction}] ${message}\n`);
};

const client = net.createConnection({ host: HOST, port: PORT }, () => {
  log('INFO', `connected to ${HOST}:${PORT} as ${CALLSIGN}`);
  const trimmedFilter = FILTER.trim();
  const defaultFilter = trimmedFilter ? '' : ' filter m/2';
  let login = `user ${CALLSIGN} pass ${PASSCODE} vers ${VERSION}${defaultFilter}\r\n`;
  client.write(login);
  if (trimmedFilter) {
    client.write(`${trimmedFilter}\r\n`);
  }
  client.write(`# aprs-debug connected ${new Date().toISOString()}\r\n`);
});

const keepalive = setInterval(() => {
  try {
    client.write('# keepalive\r\n');
  } catch (err) {
    log('ERR', `keepalive failed: ${err.message}`);
  }
}, KEEPALIVE_MS);

client.on('data', (chunk) => {
  const text = chunk.toString('utf8');
  if (!text) return;
  text.split(/\r?\n/).forEach((line) => {
    if (line) {
      log('RX', line);
    }
  });
});

client.on('error', (err) => {
  log('ERR', err.message);
});

client.on('close', () => {
  clearInterval(keepalive);
  log('INFO', 'connection closed');
});

process.on('SIGINT', () => {
  log('INFO', 'SIGINT received, closing connection');
  client.end();
});
