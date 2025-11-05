'use strict';

/**
 * Rewrite telemetry JSONL records so that every entry's sample time and telemetry
 * timestamp align with the recorded receipt time (`timestampMs`).
 *
 * Usage:
 *   node scripts/fix-telemetry-timestamps.js /path/to/telemetry-records.jsonl
 *
 * The script writes to a temporary file in the same directory and replaces the
 * original on success. A backup copy with the suffix `.bak` will be kept.
 */

const fs = require('fs');
const path = require('path');
const readline = require('readline');

async function main() {
  const inputPath = process.argv[2];
  if (!inputPath) {
    console.error('請提供 telemetry JSONL 路徑，例如:');
    console.error('  node scripts/fix-telemetry-timestamps.js ~/.config/callmesh/telemetry-records.jsonl');
    process.exitCode = 1;
    return;
  }

  const resolvedPath = path.resolve(inputPath);
  if (!fs.existsSync(resolvedPath)) {
    console.error(`找不到檔案: ${resolvedPath}`);
    process.exitCode = 1;
    return;
  }

  const dir = path.dirname(resolvedPath);
  const base = path.basename(resolvedPath);
  const backupPath = path.join(dir, `${base}.bak`);
  const tempPath = path.join(dir, `${base}.tmp-${Date.now()}`);

  const readStream = fs.createReadStream(resolvedPath, 'utf8');
  const writeStream = fs.createWriteStream(tempPath, { encoding: 'utf8' });
  const rl = readline.createInterface({ input: readStream, crlfDelay: Infinity });

  let lineNumber = 0;
  let fixedCount = 0;

  rl.on('line', (line) => {
    lineNumber += 1;
    const trimmed = line.trim();
    if (!trimmed) {
      writeStream.write('\n');
      return;
    }
    try {
      const record = JSON.parse(trimmed);
      const timestampMs = Number(record.timestampMs ?? record.timestamp_ms ?? null);
      if (Number.isFinite(timestampMs)) {
        record.timestampMs = timestampMs;
        record.timestamp = new Date(timestampMs).toISOString();
        record.sampleTimeMs = timestampMs;
        record.sampleTime = new Date(timestampMs).toISOString();
        const timeSeconds = Math.floor(timestampMs / 1000);
        const telemetry = record.telemetry && typeof record.telemetry === 'object' ? record.telemetry : {};
        record.telemetry = {
          ...telemetry,
          timeMs: timestampMs,
          timeSeconds,
          kind: telemetry.kind || 'unknown'
        };
        fixedCount += 1;
      }
      writeStream.write(`${JSON.stringify(record)}\n`);
    } catch (err) {
      console.error(`第 ${lineNumber} 行解析失敗，維持原樣: ${err.message}`);
      writeStream.write(`${line}\n`);
    }
  });

  await new Promise((resolve) => rl.once('close', resolve));
  writeStream.end();
  await new Promise((resolve) => writeStream.once('close', resolve));

  try {
    if (fs.existsSync(backupPath)) {
      fs.unlinkSync(backupPath);
    }
    fs.renameSync(resolvedPath, backupPath);
    fs.renameSync(tempPath, resolvedPath);
  } catch (err) {
    console.error(`替換檔案失敗: ${err.message}`);
    console.error(`暫存檔保留於 ${tempPath}`);
    process.exitCode = 1;
    return;
  }

  console.log(`已更新 ${resolvedPath}，共調整 ${fixedCount} 筆記錄。`);
  console.log(`原始檔案備份於 ${backupPath}`);
}

main().catch((err) => {
  console.error(`執行失敗: ${err.message}`);
  process.exitCode = 1;
});

