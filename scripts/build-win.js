#!/usr/bin/env node
const { execFileSync } = require('child_process');
const fs = require('fs');
const path = require('path');
const os = require('os');

const projectRoot = path.resolve(__dirname, '..');
const distDir = path.join(projectRoot, 'dist');
const stageDir = path.join(distDir, 'win-app');
const productName = 'TMAG Monitor';
const pkg = require(path.join(projectRoot, 'package.json'));
const appVersion = typeof pkg?.version === 'string' && pkg.version.trim() ? pkg.version.trim() : '0.0.0';
const versionTag = `v${appVersion}`;
const productLabel = `${productName}-${versionTag}-win32-x64`;
const outputDir = path.join(distDir, productLabel);
const zipOutput = path.join(distDir, `${productName.replace(/\s+/g, '_')}-${versionTag}-win32-x64.zip`);

const electronPkg = require('electron/package.json');
const electronVersion = electronPkg.version;
const zipName = `electron-v${electronVersion}-win32-x64.zip`;
const downloadUrl = `https://github.com/electron/electron/releases/download/v${electronVersion}/${zipName}`;

function log(step) {
  console.log(`[build-win] ${step}`);
}

function quoteForCmd(arg) {
  if (!/[\s"]/u.test(arg)) return arg;
  return `"${arg.replace(/"/g, '""')}"`;
}

function quoteForPwsh(arg) {
  if (!/[\s']/u.test(arg)) return `'${arg}'`;
  return `'${arg.replace(/'/g, "''")}'`;
}

function run(cmd, args, opts = {}) {
  log(`running: ${cmd} ${args.join(' ')}`);
  if (process.platform === 'win32' && cmd === 'npm') {
    const commandLine = [cmd, ...args.map(quoteForCmd)].join(' ');
    const shell = process.env.ComSpec || 'cmd.exe';
    execFileSync(shell, ['/d', '/s', '/c', commandLine], { stdio: 'inherit', ...opts });
    return;
  }
  if (process.platform === 'win32' && cmd === 'zip') {
    const destination = args[1];
    const source = args[2];
    const psCommand = [
      'Compress-Archive',
      '-Path',
      quoteForPwsh(source),
      '-DestinationPath',
      quoteForPwsh(destination),
      '-Force'
    ].join(' ');
    execFileSync('powershell.exe', ['-NoLogo', '-NoProfile', '-Command', psCommand], {
      stdio: 'inherit',
      ...opts
    });
    return;
  }
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
  if (process.platform === 'win32') {
    run('tar', ['-xf', electronZipPath, '-C', outputDir]);
  } else {
    run('unzip', ['-q', '-o', electronZipPath, '-d', outputDir]);
  }

  // When unzipping into outputDir, the contents land directly in place.
  const resourcesDir = path.join(outputDir, 'resources');
  const defaultAsar = path.join(resourcesDir, 'default_app.asar');
  if (fs.existsSync(defaultAsar)) {
    fs.rmSync(defaultAsar, { force: true });
  }
  ensureDir(path.join(resourcesDir, 'app'));

  log('staging application files');
  // Copy selective project files to stageDir
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

  log('Windows build ready');
  console.log(`Output directory: ${outputDir}`);
  console.log(`ZIP archive:      ${zipOutput}`);
})();
