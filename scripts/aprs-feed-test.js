#!/usr/bin/env node
'use strict';

const { APRSClient } = require('../src/aprs/client');
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');

function normalizeFilterCommand(raw) {
  if (raw === undefined || raw === null) return null;
  const trimmed = String(raw).trim();
  if (!trimmed) return null;
  const lowered = trimmed.toLowerCase();
  if (lowered === 'none' || lowered === 'default') {
    return null;
  }
  if (trimmed.startsWith('#')) {
    return trimmed;
  }
  if (/^filter\s+/i.test(trimmed)) {
    return `#${trimmed}`;
  }
  return `#filter ${trimmed}`;
}

const argv = yargs(hideBin(process.argv))
  .scriptName('aprs-feed-test')
  .usage('$0 [選項]')
  .option('server', {
    alias: 's',
    type: 'string',
    default: process.env.APRS_SERVER || 'asia.aprs2.net',
    describe: 'APRS-IS 伺服器位址'
  })
  .option('port', {
    alias: 'p',
    type: 'number',
    default: Number(process.env.APRS_PORT || 14580),
    describe: 'APRS-IS 連接埠'
  })
  .option('callsign', {
    alias: 'c',
    type: 'string',
    default: process.env.APRS_CALLSIGN || 'BU2GE-Z',
    describe: '登入時使用的呼號（含 SSID）'
  })
  .option('passcode', {
    alias: 'k',
    type: 'number',
    default: Number(process.env.APRS_PASSCODE || 18160),
    describe: '對應呼號的 APRS passcode'
  })
  .option('filter', {
    alias: 'f',
    type: 'string',
    default: process.env.APRS_FILTER || 'filter m/2',
    describe: '登入後送出的 filter 指令；可輸入 "none" 使用伺服器預設，其他內容會自動補上 #filter'
  })
  .option('summary', {
    alias: 'i',
    type: 'number',
    default: Number(process.env.APRS_SUMMARY_INTERVAL || 30),
    describe: '每隔幾秒輸出一次接收統計'
  })
  .option('duration', {
    alias: 'd',
    type: 'number',
    describe: '執行多久後自動結束（秒），預設為持續運行'
  })
  .option('raw', {
    type: 'boolean',
    default: false,
    describe: '顯示伺服器的註解行（以 # 開頭）'
  })
  .option('verbose', {
    alias: 'v',
    type: 'boolean',
    default: false,
    describe: '顯示 APRSClient 的 tx/rx 詳細 log'
  })
  .option('keepalive', {
    type: 'number',
    default: Number(process.env.APRS_KEEPALIVE_MS || 30_000),
    describe: 'keepalive 週期（毫秒）'
  })
  .help()
  .alias('h', 'help')
  .strict()
  .parse();

function formatTs(date = new Date()) {
  return date.toISOString();
}

function log(tag, message) {
  console.log(`[${formatTs()}] [${tag}] ${message}`);
}

const stats = {
  totalLines: 0,
  dataLines: 0,
  commentLines: 0,
  lastDataAt: null
};

const filterCommand = normalizeFilterCommand(argv.filter);

const client = new APRSClient({
  server: argv.server,
  port: argv.port,
  callsign: argv.callsign,
  passcode: Number(argv.passcode),
  version: 'aprs-test',
  softwareName: 'CMClient',
  filterCommand,
  keepaliveIntervalMs: argv.keepalive,
  log: (tag, message) => {
    if (!argv.verbose && tag === 'APRS') {
      // 僅在 verbose 模式下輸出 client 的 tx/rx 細節
      if (!/^tx |^rx |^keepalive/.test(message)) {
        log(tag, message);
      }
      return;
    }
    log(tag, message);
  }
});

client.on('connected', ({ server, port, callsign }) => {
  log(
    'INFO',
    `已連線至 ${server}:${port}，使用呼號 ${callsign}，等待伺服器資料...`
  );
});

client.on('disconnected', () => {
  log('INFO', '連線中斷，將自動重試（保持腳本運行）');
});

client.on('line', (line) => {
  stats.totalLines += 1;
  if (line.startsWith('#')) {
    stats.commentLines += 1;
    if (argv.raw) {
      log('SRV', line);
    }
    return;
  }
  stats.dataLines += 1;
  stats.lastDataAt = Date.now();
  log('DATA', line);
});

client.connect();

const summaryIntervalMs = Math.max(5_000, (argv.summary || 30) * 1_000);
const summaryTimer = setInterval(() => {
  const lastDataAge = stats.lastDataAt
    ? `${Math.round((Date.now() - stats.lastDataAt) / 1000)} 秒前`
    : '尚未收到';
  log(
    'STAT',
    `總行數=${stats.totalLines}，資料行=${stats.dataLines}，註解行=${stats.commentLines}，最後資料=${lastDataAge}`
  );
}, summaryIntervalMs);

let durationTimer = null;
if (argv.duration && Number.isFinite(argv.duration) && argv.duration > 0) {
  durationTimer = setTimeout(() => {
    log('INFO', `已達執行時間 ${argv.duration} 秒，準備結束`);
    shutdown(0);
  }, argv.duration * 1000);
}

function shutdown(code = 0) {
  clearInterval(summaryTimer);
  if (durationTimer) {
    clearTimeout(durationTimer);
    durationTimer = null;
  }
  try {
    client.disconnect();
  } catch {
    // ignore
  }
  setTimeout(() => process.exit(code), 200);
}

process.on('SIGINT', () => {
  log('INFO', '收到 Ctrl+C，正在斷線 ...');
  shutdown(0);
});

process.on('SIGTERM', () => {
  log('INFO', '收到 SIGTERM，正在斷線 ...');
  shutdown(0);
});
