'use strict';

/**
 * Helper launcher to ensure Electron boots in GUI mode.
 * Some environments set `ELECTRON_RUN_AS_NODE=1` (for CLI debugging),
 * which makes the main process behave like plain Node and breaks `ipcMain`.
 * This wrapper clears that flag before spawning the Electron binary.
 */

const { spawn } = require('child_process');

const electronBinary = require('electron');
if (typeof electronBinary !== 'string' || !electronBinary.length) {
  console.error('無法解析 electron 執行檔路徑。');
  process.exit(1);
}

const args = process.argv.slice(2);
const env = { ...process.env };
delete env.ELECTRON_RUN_AS_NODE;

const child = spawn(electronBinary, args, {
  stdio: 'inherit',
  env
});

child.on('exit', (code, signal) => {
  if (signal) {
    process.kill(process.pid, signal);
    return;
  }
  process.exit(code ?? 0);
});

child.on('error', (err) => {
  console.error('啟動 Electron 失敗:', err);
  process.exit(1);
});
