#!/usr/bin/env node
'use strict';

const net = require('net');
const readline = require('readline');

const HOST = process.env.APRS_HOST || 'asia.aprs2.net';
const PORT = Number(process.env.APRS_PORT || 14580);
const CALLSIGN = process.env.APRS_CALLSIGN;
const PASSCODE = process.env.APRS_PASSCODE;
const FILTER = process.env.APRS_FILTER || 'filter m/2';

if (!CALLSIGN || !PASSCODE) {
  console.error('請先設定 APRS_CALLSIGN 與 APRS_PASSCODE 環境變數');
  process.exit(1);
}

const client = net.createConnection({ host: HOST, port: PORT }, () => {
  console.log(`[TEST] connected to ${HOST}:${PORT}`);
  const login = `user ${CALLSIGN} pass ${PASSCODE} vers TMAG-TEST 0.0.1 ${FILTER}\r\n`;
  client.write(login);
  client.write(`# test connection at ${new Date().toISOString()}\r\n`);
});

client.setEncoding('utf8');
client.on('data', (chunk) => {
  process.stdout.write(`[RX] ${chunk}`);
});

client.on('error', (err) => {
  console.error('[TEST] error:', err.message);
});

client.on('close', () => {
  console.log('[TEST] connection closed');
  process.exit(0);
});

const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
rl.on('line', (line) => {
  client.write(`${line}\r\n`);
});
