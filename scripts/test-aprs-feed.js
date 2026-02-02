#!/usr/bin/env node
'use strict';

const net = require('net');

const DEFAULT_HOST = process.env.APRS_TEST_HOST || 'asia.aprs2.net';
const DEFAULT_PORT = Number(process.env.APRS_TEST_PORT || 14580);
const DEFAULT_FILTER = process.env.APRS_TEST_FILTER || 'filter b/BU,BV,BM,BX';
const KEEPALIVE_MS = 30_000;

const [,, callsign, passcode, filterArg] = process.argv;
if (!callsign || !passcode) {
  console.error('Usage: node scripts/test-aprs-feed.js <CALLSIGN> <PASSCODE> [FILTER_COMMAND]');
  process.exit(1);
}

const filterCommand = filterArg && filterArg.trim() ? filterArg.trim() : DEFAULT_FILTER;

const socket = net.createConnection({ host: DEFAULT_HOST, port: DEFAULT_PORT });

const log = (msg) => {
  const ts = new Date().toISOString();
  console.log(`[${ts}] ${msg}`);
};

let keepaliveTimer = null;
const scheduleKeepalive = () => {
  clearTimeout(keepaliveTimer);
  keepaliveTimer = setTimeout(() => {
    socket.write('# keepalive\r\n');
    scheduleKeepalive();
  }, KEEPALIVE_MS);
};

socket.on('connect', () => {
  log(`connected to ${DEFAULT_HOST}:${DEFAULT_PORT}`);
  socket.write(`user ${callsign} pass ${passcode} vers TMAG-TEST 0.1\r\n`);
  if (filterCommand) {
    log(`sending filter: ${filterCommand}`);
    socket.write(`${filterCommand}\r\n`);
  }
  scheduleKeepalive();
});

socket.on('data', (buf) => {
  const lines = buf.toString('utf8').split(/\r?\n/);
  for (const line of lines) {
    if (!line.trim()) continue;
    log(`rx ${line.trim()}`);
  }
});

socket.on('error', (err) => {
  log(`error: ${err.message}`);
});

socket.on('close', () => {
  log('connection closed');
  clearTimeout(keepaliveTimer);
});

process.on('SIGINT', () => {
  log('SIGINT received, closing connection');
  socket.end();
});
