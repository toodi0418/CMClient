#!/usr/bin/env node
const { execFileSync } = require('child_process');
const fs = require('fs');
const path = require('path');

const projectRoot = path.resolve(__dirname, '..');
const distDir = path.join(projectRoot, 'dist');
const stageDir = path.join(distDir, 'linux-app');
const productName = 'TMAG Monitor';
const outputDir = path.join(distDir, `${productName}-linux-x64`);
const zipOutput = path.join(distDir, `${productName.replace(/\s+/g, '_')}-linux-x64.zip`);

const electronPkg = require('electron/package.json');
const electronVersion = electronPkg.version;
const zipName = `electron-v${electronVersion}-linux-x64.zip`;
const downloadUrl = `https://github.com/electron/electron/releases/download/v${electronVersion}/${zipName}`;

function log(step) {
  console.log(`[build-linux] ${step}`);
}

function run(cmd, args, opts = {}) {
  log(`running: ${cmd} ${args.join(' ')}`);
  execFileSync(cmd, args, { stdio: 'inherit', ...opts });
}

function rimraf(target) {
  if (fs.existsSync(target)) {
    log(`removing ${target}`);
    fs.rmSync(target, { recursive: true, force: true });
  }
}

function ensureDir(dir) {
  fs.mkdirSync(dir, { recursive: true });
}

(async () => {
  ensureDir(distDir);

  const electronZipPath = path.join(distDir, zipName);
  if (!fs.existsSync(electronZipPath)) {
    log(`downloading Electron ${electronVersion} runtime`);
    run('curl', ['-L', '-o', electronZipPath, downloadUrl]);
  } else {
    log(`reusing cached ${zipName}`);
  }

  rimraf(outputDir);
  rimraf(stageDir);

  ensureDir(stageDir);
  ensureDir(outputDir);

  log('extracting electron runtime');
  run('unzip', ['-q', '-o', electronZipPath, '-d', outputDir]);

  const resourcesDir = path.join(outputDir, 'resources');
  const defaultAsar = path.join(resourcesDir, 'default_app.asar');
  if (fs.existsSync(defaultAsar)) {
    fs.rmSync(defaultAsar, { force: true });
  }
  ensureDir(path.join(resourcesDir, 'app'));

  log('staging application files');
  const includeItems = [
    'package.json',
    'package-lock.json',
    'src',
    'proto'
  ];
  includeItems.forEach((item) => {
    const srcPath = path.join(projectRoot, item);
    if (fs.existsSync(srcPath)) {
      const destPath = path.join(stageDir, item);
      fs.cpSync(srcPath, destPath, { recursive: true });
    }
  });

  log('installing production dependencies');
  run('npm', ['install', '--omit=dev', '--ignore-scripts'], { cwd: stageDir });

  log('copying staged app into Electron resources');
  fs.cpSync(stageDir, path.join(resourcesDir, 'app'), { recursive: true });

  rimraf(stageDir);

  log('packing zip archive');
  if (fs.existsSync(zipOutput)) {
    fs.rmSync(zipOutput);
  }
  run('zip', ['-rq', zipOutput, path.basename(outputDir)], { cwd: distDir });

  log('Linux build ready');
  console.log(`Output directory: ${outputDir}`);
  console.log(`ZIP archive:      ${zipOutput}`);
})();
