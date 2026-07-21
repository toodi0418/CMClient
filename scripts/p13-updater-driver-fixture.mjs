import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import {
  access,
  cp,
  mkdir,
  readFile,
  readdir,
  rm,
  stat,
  writeFile,
} from "node:fs/promises";
import { basename, dirname, isAbsolute, relative, resolve } from "node:path";
import { clearTimeout, setTimeout } from "node:timers";
import { pathToFileURL } from "node:url";

export const FIXTURE_RELATIVE_ROOT = "test/p13-updater-driver";
export const FORBIDDEN_REPOSITORY_OUTPUTS = Object.freeze([
  "target",
  "src-tauri/target",
  "src-tauri/gen",
]);

const CARGO_REGISTRY_SOURCE =
  "registry+https://github.com/rust-lang/crates.io-index";
const NPM_REGISTRY_SOURCE = "https://registry.npmjs.org/";

const EXPECTED = Object.freeze({
  cli: {
    version: "2.11.4",
    integrity:
      "sha512-R8xGtMpwyetawSqm9kYOuMmEqkhUbvcUy8n0aNXIxollKBLESUu5f4Fx+64hgASYm1H+jSWq6jCW6zqTnH6hqQ==",
    license: "Apache-2.0 OR MIT",
    source: NPM_REGISTRY_SOURCE,
  },
  crates: {
    "tauri-build": {
      version: "2.6.3",
      checksum:
        "bc9ce40b16101cb6ea63d3e221567affd1c3a9205f95d7bc574941a10636b632",
      license: "Apache-2.0 OR MIT",
      source: CARGO_REGISTRY_SOURCE,
    },
    tauri: {
      version: "2.11.5",
      checksum:
        "667b20e2726d572dea2de7370da16e188eb06008faf9a92fab7cdc46791190b5",
      license: "Apache-2.0 OR MIT",
      source: CARGO_REGISTRY_SOURCE,
    },
    "tauri-plugin-updater": {
      version: "2.10.1",
      checksum:
        "806d9dac662c2e4594ff03c647a552f2c9bd544e7d0f683ec58f872f952ce4af",
      license: "Apache-2.0 OR MIT",
      source: CARGO_REGISTRY_SOURCE,
    },
    reqwest: {
      version: "0.13.4",
      checksum:
        "219c5811de6525e5416c7d5d53bb656d3afdbc6c5af816e0802bcfa42dbdc1c3",
      license: "MIT OR Apache-2.0",
      source: CARGO_REGISTRY_SOURCE,
    },
    serde_json: {
      version: "1.0.151",
      checksum:
        "c841b55ecdae098c80dcae9cf767f6f8a0c2cdb3416bbef72181df4d0fe73f14",
      license: "MIT OR Apache-2.0",
      source: CARGO_REGISTRY_SOURCE,
    },
    url: {
      version: "2.5.8",
      checksum:
        "ff67a8a4397373c3ef660812acab3268222035010ab8680ec4215f38ba3d0eed",
      license: "MIT OR Apache-2.0",
      source: CARGO_REGISTRY_SOURCE,
    },
  },
  comparisonDriver: {
    name: "cargo-packager-updater",
    version: "0.2.3",
    checksum:
      "eec09acab5c2227aba2e592d431708305bdeb6d507703f6cd8983fb57b6c5ef7",
    license: "Apache-2.0 OR MIT",
    source: CARGO_REGISTRY_SOURCE,
    maturity: "public-preview",
  },
});

const SOURCE_FILES = Object.freeze({
  cargoToml: "src-tauri/Cargo.toml",
  cargoLock: "src-tauri/Cargo.lock",
  config: "src-tauri/tauri.conf.json",
  main: "src-tauri/src/main.rs",
  hooks: "src-tauri/windows/hooks.nsh",
  lab: "lab/server.mjs",
  lock: "fixture-lock.json",
});

function sha256(value) {
  return createHash("sha256").update(value).digest("hex");
}

function canonicalText(value) {
  return value.replace(/\r\n?/g, "\n");
}

function error(code, detail) {
  return `${code}${detail ? `: ${detail}` : ""}`;
}

function parseCargoPackages(lockText) {
  const packages = new Map();
  for (const block of lockText.split(/\r?\n\[\[package\]\]\r?\n/)) {
    const name = /^name = "([^"]+)"$/m.exec(block)?.[1];
    const version = /^version = "([^"]+)"$/m.exec(block)?.[1];
    const checksum = /^checksum = "([a-f0-9]{64})"$/m.exec(block)?.[1];
    const source = /^source = "([^"]+)"$/m.exec(block)?.[1];
    if (name && version) {
      const values = packages.get(name) ?? [];
      values.push({ version, checksum, source });
      packages.set(name, values);
    }
  }
  return packages;
}

function exactCargoDependency(cargoToml, dependency, version) {
  const escaped = dependency.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  return new RegExp(
    `^${escaped}\\s*=\\s*\\{[^\\n]*version\\s*=\\s*"=${version.replaceAll(".", "\\.")}"[^\\n]*\\}$`,
    "m",
  ).test(cargoToml);
}

export async function readFixtureDocuments(repositoryRoot = resolve(".")) {
  const fixtureRoot = resolve(repositoryRoot, FIXTURE_RELATIVE_ROOT);
  const entries = await Promise.all(
    Object.entries(SOURCE_FILES).map(async ([name, path]) => [
      name,
      await readFile(resolve(fixtureRoot, path), "utf8"),
    ]),
  );
  const desktopPackage = await readFile(
    resolve(repositoryRoot, "apps/desktop/package.json"),
    "utf8",
  );
  const pnpmLock = await readFile(
    resolve(repositoryRoot, "pnpm-lock.yaml"),
    "utf8",
  );
  return { ...Object.fromEntries(entries), desktopPackage, pnpmLock };
}

export function validateFixtureDocuments(documents) {
  const errors = [];
  const cargoToml = documents.cargoToml ?? "";
  if (!/^\[workspace\]$/m.test(cargoToml)) {
    errors.push(error("P13_UPDATER_FIXTURE_NOT_STANDALONE"));
  }
  if (/cargo-packager-updater|minisign|\bzip\s*=|\btar\s*=/i.test(cargoToml)) {
    errors.push(error("P13_UPDATER_FIXTURE_CUSTOM_OR_PREVIEW_DRIVER"));
  }
  for (const [dependency, metadata] of Object.entries(EXPECTED.crates)) {
    if (!exactCargoDependency(cargoToml, dependency, metadata.version)) {
      errors.push(
        error(
          "P13_UPDATER_FIXTURE_DEPENDENCY_NOT_EXACT",
          `${dependency}=${metadata.version}`,
        ),
      );
    }
  }

  let config;
  try {
    config = JSON.parse(documents.config);
  } catch {
    errors.push(error("P13_UPDATER_FIXTURE_CONFIG_INVALID"));
  }
  if (config) {
    if (
      !Array.isArray(config.app?.windows) ||
      config.app.windows.length !== 0
    ) {
      errors.push(error("P13_UPDATER_FIXTURE_WINDOW_CONFIG_PRESENT"));
    }
    if (config.bundle?.createUpdaterArtifacts !== "v1Compatible") {
      errors.push(error("P13_UPDATER_FIXTURE_ARTIFACT_MODE_INVALID"));
    }
    if (config.bundle?.useLocalToolsDir !== true) {
      errors.push(error("P13_UPDATER_FIXTURE_TOOL_CACHE_NOT_REDIRECTABLE"));
    }
    if (
      config.bundle?.windows?.nsis?.installMode !== "currentUser" ||
      config.bundle?.windows?.nsis?.installerHooks !== "windows/hooks.nsh"
    ) {
      errors.push(error("P13_UPDATER_FIXTURE_NSIS_CONTRACT_INVALID"));
    }
    if (
      config.bundle?.windows?.webviewInstallMode?.type !== "offlineInstaller" ||
      config.bundle.windows.webviewInstallMode.silent !== true
    ) {
      errors.push(error("P13_UPDATER_FIXTURE_WEBVIEW_MODE_INVALID"));
    }
    const endpoints = config.plugins?.updater?.endpoints;
    if (
      !Array.isArray(endpoints) ||
      endpoints.length !== 1 ||
      !endpoints[0].startsWith("https://127.0.0.1:") ||
      config.plugins.updater.dangerousInsecureTransportProtocol === true ||
      config.plugins.updater.dangerousAcceptInvalidCerts === true ||
      config.plugins.updater.dangerousAcceptInvalidHostnames === true
    ) {
      errors.push(error("P13_UPDATER_FIXTURE_HTTPS_CONTRACT_INVALID"));
    }
  }

  const main = documents.main ?? "";
  for (const pattern of [
    /tauri_plugin_updater::UpdaterExt/,
    /\.updater_builder\(\)/,
    /\.download\(/,
    /\.download_and_install\(/,
    /webview_windows\(\)\.is_empty\(\)/,
    /windows_subsystem = "windows"/,
  ]) {
    if (!pattern.test(main)) {
      errors.push(
        error("P13_UPDATER_FIXTURE_HELPER_CONTRACT_MISSING", pattern.source),
      );
    }
  }
  if (
    /cargo_packager|minisign|ZipArchive|tar::Archive|danger_accept_invalid|WebviewWindowBuilder/.test(
      main,
    )
  ) {
    errors.push(error("P13_UPDATER_FIXTURE_FORBIDDEN_HELPER_PATH"));
  }

  const hooks = documents.hooks ?? "";
  for (const pattern of [
    /NSIS_HOOK_POSTINSTALL/,
    /HKCU "Environment" "Path"/,
    /HKCU "Software\\Microsoft\\Windows\\CurrentVersion\\Run"/,
    /NSIS_HOOK_POSTUNINSTALL/,
    /WM_SETTINGCHANGE/,
    /SendMessage[^\n]*\/TIMEOUT=5000/,
  ]) {
    if (!pattern.test(hooks)) {
      errors.push(
        error("P13_UPDATER_FIXTURE_HOOK_CONTRACT_MISSING", pattern.source),
      );
    }
  }
  if (/\bHKLM\b|RequestExecutionLevel|runas/i.test(hooks)) {
    errors.push(error("P13_UPDATER_FIXTURE_HOOK_ELEVATION_FORBIDDEN"));
  }

  const lab = documents.lab ?? "";
  if (
    !/createServer/.test(lab) ||
    !/"127\.0\.0\.1"/.test(lab) ||
    !/bit-flip/.test(lab) ||
    !/wrong-target/.test(lab) ||
    !/downgrade/.test(lab) ||
    !/oversize/.test(lab) ||
    !/timeout/.test(lab)
  ) {
    errors.push(error("P13_UPDATER_FIXTURE_FAULT_LAB_INCOMPLETE"));
  }

  const packages = parseCargoPackages(documents.cargoLock ?? "");
  for (const [dependency, metadata] of Object.entries(EXPECTED.crates)) {
    const match = packages
      .get(dependency)
      ?.find((candidate) => candidate.version === metadata.version);
    if (
      !match ||
      match.checksum !== metadata.checksum ||
      match.source !== metadata.source
    ) {
      errors.push(
        error(
          "P13_UPDATER_FIXTURE_CARGO_LOCK_DRIFT",
          `${dependency}=${metadata.version}`,
        ),
      );
    }
  }
  const unexpectedCargoSources = new Set(
    [...packages.values()]
      .flat()
      .map((candidate) => candidate.source)
      .filter(Boolean)
      .filter((source) => source !== CARGO_REGISTRY_SOURCE),
  );
  if (unexpectedCargoSources.size > 0) {
    errors.push(error("P13_UPDATER_FIXTURE_CARGO_SOURCE_DRIFT"));
  }

  let fixtureLock;
  try {
    fixtureLock = JSON.parse(documents.lock);
  } catch {
    errors.push(error("P13_UPDATER_FIXTURE_LOCK_INVALID"));
  }
  if (fixtureLock) {
    if (
      fixtureLock.schemaVersion !== 2 ||
      fixtureLock.selectedDriver !== "tauri-plugin-updater" ||
      fixtureLock.previewRiskApproval !== null ||
      fixtureLock.artifactMode !== "v1Compatible"
    ) {
      errors.push(error("P13_UPDATER_FIXTURE_LOCK_DECISION_INVALID"));
    }
    const lockedCli = fixtureLock.dependencies?.npm?.["@tauri-apps/cli"];
    if (
      lockedCli?.version !== EXPECTED.cli.version ||
      lockedCli?.integrity !== EXPECTED.cli.integrity ||
      lockedCli?.license !== EXPECTED.cli.license ||
      lockedCli?.source !== EXPECTED.cli.source
    ) {
      errors.push(error("P13_UPDATER_FIXTURE_CLI_PROVENANCE_DRIFT"));
    }
    for (const [dependency, metadata] of Object.entries(EXPECTED.crates)) {
      const locked = fixtureLock.dependencies?.cargo?.[dependency];
      if (
        locked?.version !== metadata.version ||
        locked?.checksum !== metadata.checksum ||
        locked?.license !== metadata.license ||
        locked?.source !== metadata.source
      ) {
        errors.push(
          error("P13_UPDATER_FIXTURE_CRATE_PROVENANCE_DRIFT", dependency),
        );
      }
    }
    const comparison = fixtureLock.driverComparison;
    if (
      comparison?.name !== EXPECTED.comparisonDriver.name ||
      comparison?.version !== EXPECTED.comparisonDriver.version ||
      comparison?.checksum !== EXPECTED.comparisonDriver.checksum ||
      comparison?.license !== EXPECTED.comparisonDriver.license ||
      comparison?.source !== EXPECTED.comparisonDriver.source ||
      comparison?.maturity !== EXPECTED.comparisonDriver.maturity ||
      comparison?.selected !== false ||
      comparison?.riskApprovalRequired !== true ||
      comparison?.riskApproval !== null
    ) {
      errors.push(error("P13_UPDATER_FIXTURE_COMPARISON_PROVENANCE_DRIFT"));
    }
    for (const [name, contents] of Object.entries({
      cargoToml: documents.cargoToml,
      cargoLock: documents.cargoLock,
      config: documents.config,
      main: documents.main,
      hooks: documents.hooks,
      lab: documents.lab,
    })) {
      if (fixtureLock.files?.[name] !== sha256(canonicalText(contents ?? ""))) {
        errors.push(error("P13_UPDATER_FIXTURE_FILE_DIGEST_DRIFT", name));
      }
    }
  }

  let desktopPackage;
  try {
    desktopPackage = JSON.parse(documents.desktopPackage);
  } catch {
    errors.push(error("P13_UPDATER_FIXTURE_CLI_PACKAGE_INVALID"));
  }
  if (
    desktopPackage?.devDependencies?.["@tauri-apps/cli"] !==
      EXPECTED.cli.version ||
    !documents.pnpmLock?.includes(
      `'@tauri-apps/cli@${EXPECTED.cli.version}':`,
    ) ||
    !documents.pnpmLock?.includes(EXPECTED.cli.integrity)
  ) {
    errors.push(error("P13_UPDATER_FIXTURE_CLI_LOCK_DRIFT"));
  }
  return errors;
}

export async function checkFixture(repositoryRoot = resolve(".")) {
  const documents = await readFixtureDocuments(repositoryRoot);
  const errors = validateFixtureDocuments(documents);
  for (const generated of FORBIDDEN_REPOSITORY_OUTPUTS.map((path) =>
    resolve(repositoryRoot, FIXTURE_RELATIVE_ROOT, path),
  )) {
    try {
      await access(generated);
      errors.push(error("P13_UPDATER_FIXTURE_REPO_OUTPUT_PRESENT", generated));
    } catch {
      // Expected: fixture output belongs to the campaign target directory.
    }
  }
  return errors;
}

function containsPath(parent, child) {
  const value = relative(resolve(parent), resolve(child));
  return value === "" || (!value.startsWith("..") && !isAbsolute(value));
}

export function campaignPaths(repositoryRoot, campaignRoot) {
  if (!campaignRoot || !isAbsolute(campaignRoot)) {
    throw new Error("P13_UPDATER_CAMPAIGN_ROOT_INVALID");
  }
  const repository = resolve(repositoryRoot);
  const campaign = resolve(campaignRoot);
  if (
    containsPath(repository, campaign) ||
    containsPath(campaign, repository)
  ) {
    throw new Error("P13_UPDATER_CAMPAIGN_OVERLAPS_REPOSITORY");
  }
  const root = resolve(campaign, "p13-updater-driver");
  return {
    root,
    source: resolve(root, "source"),
    target: resolve(root, "target"),
    cargoHome: resolve(root, "cargo-home"),
    temp: resolve(root, "tmp"),
    home: resolve(root, "home"),
    appData: resolve(root, "home", "AppData", "Roaming"),
    localAppData: resolve(root, "home", "AppData", "Local"),
    cache: resolve(root, "cache"),
    signing: resolve(root, "signing"),
    tls: resolve(root, "tls"),
    evidence: resolve(root, "evidence"),
  };
}

async function prepareCampaign(paths) {
  await Promise.all(
    Object.values(paths).map((path) =>
      mkdir(path, { recursive: true, mode: 0o700 }),
    ),
  );
}

async function prepareCampaignSource(repositoryRoot, paths) {
  const fixtureSource = resolve(repositoryRoot, FIXTURE_RELATIVE_ROOT);
  const fixtureRoot = resolve(paths.source, FIXTURE_RELATIVE_ROOT);
  const iconSource = resolve(repositoryRoot, "apps/desktop/src-tauri/icons");
  const iconTarget = resolve(paths.source, "apps/desktop/src-tauri/icons");
  await rm(paths.source, { recursive: true, force: true });
  await Promise.all([
    mkdir(dirname(fixtureRoot), { recursive: true, mode: 0o700 }),
    mkdir(dirname(iconTarget), { recursive: true, mode: 0o700 }),
  ]);
  await Promise.all([
    cp(fixtureSource, fixtureRoot, { recursive: true }),
    cp(iconSource, iconTarget, { recursive: true }),
  ]);
  return fixtureRoot;
}

export function childEnvironment(
  paths,
  additions = {},
  sourceEnvironment = process.env,
) {
  const inherited = Object.fromEntries(
    Object.entries(sourceEnvironment).filter(
      ([name]) =>
        !/api.?key|authorization|cookie|credential|pass(code|word)?|private.?key|secret|session|token/i.test(
          name,
        ),
    ),
  );
  const parentHome =
    sourceEnvironment.USERPROFILE?.trim() || sourceEnvironment.HOME?.trim();
  const rustupHome =
    sourceEnvironment.RUSTUP_HOME?.trim() ||
    (parentHome ? resolve(parentHome, ".rustup") : "");
  if (!rustupHome || !isAbsolute(rustupHome)) {
    throw new Error("P13_UPDATER_RUSTUP_HOME_INVALID");
  }
  return {
    ...inherited,
    CI: "true",
    CARGO_HOME: paths.cargoHome,
    CARGO_TARGET_DIR: paths.target,
    TEMP: paths.temp,
    TMP: paths.temp,
    TMPDIR: paths.temp,
    HOME: paths.home,
    USERPROFILE: paths.home,
    APPDATA: paths.appData,
    LOCALAPPDATA: paths.localAppData,
    XDG_CACHE_HOME: paths.cache,
    XDG_CONFIG_HOME: resolve(paths.home, ".config"),
    RUSTUP_HOME: resolve(rustupHome),
    RUSTUP_TOOLCHAIN: sourceEnvironment.RUSTUP_TOOLCHAIN?.trim() || "1.96.0",
    CMCLIENT_CAMPAIGN_ROOT: resolve(paths.root, ".."),
    ...additions,
  };
}

function spawnChecked(command, argumentsList, options = {}) {
  return new Promise((resolveChild, rejectChild) => {
    const child = spawn(command, argumentsList, {
      cwd: options.cwd,
      env: options.env,
      stdio: options.stdio ?? "inherit",
      windowsHide: true,
    });
    let timedOut = false;
    const timer = options.timeoutMs
      ? setTimeout(() => {
          timedOut = true;
          child.kill("SIGKILL");
        }, options.timeoutMs)
      : null;
    child.once("error", rejectChild);
    child.once("exit", (code, signal) => {
      if (timer) clearTimeout(timer);
      if (timedOut) {
        rejectChild(new Error("P13_UPDATER_CHILD_TIMEOUT"));
      } else if (code !== 0) {
        rejectChild(
          new Error(
            `P13_UPDATER_CHILD_FAILED:${String(code)}:${signal ?? "none"}`,
          ),
        );
      } else {
        resolveChild();
      }
    });
  });
}

function tauriCliScript(repositoryRoot) {
  return resolve(
    repositoryRoot,
    "apps/desktop/node_modules/@tauri-apps/cli/tauri.js",
  );
}

async function ensureSigningKey(repositoryRoot, fixtureRoot, paths) {
  const privateKeyPath = resolve(paths.signing, "fixture.key");
  const publicKeyPath = `${privateKeyPath}.pub`;
  try {
    await Promise.all([access(privateKeyPath), access(publicKeyPath)]);
  } catch {
    await spawnChecked(
      process.execPath,
      [
        tauriCliScript(repositoryRoot),
        "signer",
        "generate",
        "--ci",
        "--password",
        "cmclient-p13-fixture-only",
        "--write-keys",
        privateKeyPath,
      ],
      {
        cwd: fixtureRoot,
        env: childEnvironment(paths),
        stdio: "ignore",
        timeoutMs: 30_000,
      },
    );
  }
  return {
    privateKey: await readFile(privateKeyPath, "utf8"),
    privateKeyPath,
    publicKeyPath,
  };
}

function platformBundle() {
  if (process.platform === "win32") return "nsis";
  if (process.platform === "darwin") return "app";
  if (process.platform === "linux") return "appimage";
  throw new Error("P13_UPDATER_PLATFORM_UNSUPPORTED");
}

const ARTIFACT_BUNDLES = Object.freeze({
  nsis: {
    directory: "nsis",
    roles: Object.freeze([
      { role: "installer", suffix: ".exe" },
      { role: "installerSignature", suffix: ".exe.sig" },
      { role: "updaterPayload", suffix: ".nsis.zip" },
      { role: "updaterSignature", suffix: ".nsis.zip.sig" },
    ]),
  },
  app: {
    directory: "macos",
    roles: Object.freeze([
      { role: "updaterPayload", suffix: ".app.tar.gz" },
      { role: "updaterSignature", suffix: ".app.tar.gz.sig" },
    ]),
  },
  appimage: {
    directory: "appimage",
    roles: Object.freeze([
      { role: "updaterPayload", suffix: ".AppImage.tar.gz" },
      { role: "updaterSignature", suffix: ".AppImage.tar.gz.sig" },
    ]),
  },
});

const TARGET_ARCHITECTURES = Object.freeze({
  "x86_64-pc-windows-msvc": Object.freeze(["x64"]),
  "aarch64-pc-windows-msvc": Object.freeze(["arm64"]),
  "x86_64-apple-darwin": Object.freeze(["x64", "x86_64"]),
  "aarch64-apple-darwin": Object.freeze(["aarch64", "arm64"]),
  "universal-apple-darwin": Object.freeze(["universal"]),
  "x86_64-unknown-linux-gnu": Object.freeze(["amd64", "x64", "x86_64"]),
  "x86_64-unknown-linux-musl": Object.freeze(["amd64", "x64", "x86_64"]),
  "aarch64-unknown-linux-gnu": Object.freeze(["aarch64", "arm64"]),
  "aarch64-unknown-linux-musl": Object.freeze(["aarch64", "arm64"]),
});

function hostTargetTriple() {
  const host = `${process.platform}:${process.arch}`;
  const targets = {
    "win32:x64": "x86_64-pc-windows-msvc",
    "win32:arm64": "aarch64-pc-windows-msvc",
    "darwin:x64": "x86_64-apple-darwin",
    "darwin:arm64": "aarch64-apple-darwin",
    "linux:x64": "x86_64-unknown-linux-gnu",
    "linux:arm64": "aarch64-unknown-linux-gnu",
  };
  const target = targets[host];
  if (!target) throw new Error("P13_UPDATER_HOST_TARGET_UNSUPPORTED");
  return target;
}

function validateArtifactVersion(version) {
  if (!/^\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?$/.test(version ?? "")) {
    throw new Error("P13_UPDATER_FIXTURE_VERSION_INVALID");
  }
  return version;
}

function artifactDirectory(paths, bundle, targetDirectoryTriple) {
  const definition = ARTIFACT_BUNDLES[bundle];
  if (!definition) throw new Error("P13_UPDATER_BUNDLE_INVALID");
  return resolve(
    paths.target,
    ...(targetDirectoryTriple ? [targetDirectoryTriple] : []),
    "release",
    "bundle",
    definition.directory,
  );
}

function stripSuffix(value, suffix) {
  if (!value.toLowerCase().endsWith(suffix.toLowerCase())) return null;
  return value.slice(0, -suffix.length);
}

function artifactIdentity(stem, version, bundle, targetTriple) {
  const marker = `_${version}_`;
  const markerIndex = stem.lastIndexOf(marker);
  if (markerIndex < 1) {
    if (
      bundle === "nsis" ||
      /_\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?_/.test(stem)
    ) {
      return null;
    }
    return {
      architecture: TARGET_ARCHITECTURES[targetTriple]?.[0] ?? null,
      versionBinding: "fresh-build-context",
    };
  }
  let architecture = stem.slice(markerIndex + marker.length);
  if (bundle === "nsis") {
    if (!architecture.endsWith("-setup")) return null;
    architecture = architecture.slice(0, -"-setup".length);
  }
  return architecture
    ? { architecture, versionBinding: "artifact-filename" }
    : null;
}

async function sha256File(path) {
  return new Promise((resolveHash, rejectHash) => {
    const hash = createHash("sha256");
    const stream = createReadStream(path);
    stream.on("error", rejectHash);
    stream.on("data", (chunk) => hash.update(chunk));
    stream.on("end", () => resolveHash(hash.digest("hex")));
  });
}

function decodeOuterBase64(value, code) {
  const encoded = value.trim();
  if (!/^[A-Za-z0-9+/]+={0,2}$/.test(encoded)) {
    throw new Error(code);
  }
  return Buffer.from(encoded, "base64").toString("utf8");
}

function decodeInnerBase64(value, code) {
  if (!/^[A-Za-z0-9+/]+={0,2}$/.test(value)) {
    throw new Error(code);
  }
  return Buffer.from(value, "base64");
}

function parseSignerPublicKey(encodedPublicKey) {
  const lines = decodeOuterBase64(
    encodedPublicKey,
    "P13_UPDATER_SIGNER_PUBLIC_KEY_INVALID",
  )
    .trimEnd()
    .split(/\r?\n/);
  if (!lines[0]?.startsWith("untrusted comment:") || lines.length !== 2) {
    throw new Error("P13_UPDATER_SIGNER_PUBLIC_KEY_INVALID");
  }
  const key = decodeInnerBase64(
    lines[1],
    "P13_UPDATER_SIGNER_PUBLIC_KEY_INVALID",
  );
  if (key.length !== 42) {
    throw new Error("P13_UPDATER_SIGNER_PUBLIC_KEY_INVALID");
  }
  return {
    fingerprint: `sha256:${sha256(key)}`,
    keyId: key.subarray(2, 10),
  };
}

function verifySignatureBinding(encodedSignature, signedFileName, signer) {
  const lines = decodeOuterBase64(
    encodedSignature,
    "P13_UPDATER_ARTIFACT_SIGNATURE_INVALID",
  )
    .trimEnd()
    .split(/\r?\n/);
  if (
    lines.length !== 4 ||
    !lines[0]?.startsWith("untrusted comment:") ||
    !lines[2]?.startsWith("trusted comment:")
  ) {
    throw new Error("P13_UPDATER_ARTIFACT_SIGNATURE_INVALID");
  }
  const signature = decodeInnerBase64(
    lines[1],
    "P13_UPDATER_ARTIFACT_SIGNATURE_INVALID",
  );
  if (
    signature.length !== 74 ||
    !signature.subarray(2, 10).equals(signer.keyId)
  ) {
    throw new Error("P13_UPDATER_ARTIFACT_SIGNER_MISMATCH");
  }
  const signedName = /(?:^|\t)file:(.+)$/.exec(lines[2])?.[1];
  if (signedName !== signedFileName) {
    throw new Error("P13_UPDATER_ARTIFACT_SIGNATURE_FILE_MISMATCH");
  }
}

async function describeArtifact(paths, path, buildStartedAtMs) {
  if (!containsPath(paths.root, path)) {
    throw new Error("P13_UPDATER_ARTIFACT_OUTSIDE_CAMPAIGN");
  }
  const metadata = await stat(path);
  if (
    !metadata.isFile() ||
    metadata.size < 1 ||
    metadata.mtimeMs < buildStartedAtMs - 2_000
  ) {
    throw new Error("P13_UPDATER_ARTIFACT_STALE_OR_EMPTY");
  }
  return {
    relativePath: relative(paths.root, path).replaceAll("\\", "/"),
    size: metadata.size,
    sha256: await sha256File(path),
    modifiedAtUtc: metadata.mtime.toISOString(),
  };
}

export async function inspectArtifacts(
  paths,
  {
    bundle = platformBundle(),
    version,
    targetTriple = hostTargetTriple(),
    targetDirectoryTriple = targetTriple,
    buildStartedAtMs,
    publicKeyPath = resolve(paths.signing, "fixture.key.pub"),
  } = {},
) {
  validateArtifactVersion(version);
  if (!Number.isFinite(buildStartedAtMs) || buildStartedAtMs <= 0) {
    throw new Error("P13_UPDATER_BUILD_START_REQUIRED");
  }
  const allowedArchitectures = TARGET_ARCHITECTURES[targetTriple];
  if (!allowedArchitectures) {
    throw new Error("P13_UPDATER_TARGET_UNSUPPORTED");
  }
  const definition = ARTIFACT_BUNDLES[bundle];
  const directory = artifactDirectory(paths, bundle, targetDirectoryTriple);
  if (!containsPath(paths.root, directory)) {
    throw new Error("P13_UPDATER_ARTIFACT_OUTSIDE_CAMPAIGN");
  }
  if (!containsPath(paths.root, publicKeyPath)) {
    throw new Error("P13_UPDATER_SIGNER_PUBLIC_KEY_OUTSIDE_CAMPAIGN");
  }
  let entries;
  try {
    entries = (await readdir(directory, { withFileTypes: true })).filter(
      (entry) => entry.isFile(),
    );
  } catch {
    throw new Error("P13_UPDATER_ARTIFACT_SET_INCOMPLETE");
  }

  const selected = {};
  const stems = new Set();
  const architectures = new Set();
  const versionBindings = new Set();
  for (const { role, suffix } of definition.roles) {
    const matches = entries
      .map((entry) => ({
        entry,
        stem: stripSuffix(entry.name, suffix),
      }))
      .map(({ entry, stem }) => ({
        entry,
        stem,
        identity:
          stem === null
            ? null
            : artifactIdentity(stem, version, bundle, targetTriple),
      }))
      .filter(({ identity }) => identity !== null);
    if (matches.length !== 1) {
      throw new Error("P13_UPDATER_ARTIFACT_SET_INCOMPLETE_OR_AMBIGUOUS");
    }
    const [{ entry, stem, identity }] = matches;
    selected[role] = resolve(directory, entry.name);
    stems.add(stem);
    architectures.add(identity.architecture);
    versionBindings.add(identity.versionBinding);
  }
  if (
    stems.size !== 1 ||
    architectures.size !== 1 ||
    versionBindings.size !== 1
  ) {
    throw new Error("P13_UPDATER_ARTIFACT_SET_MISMATCH");
  }
  const [architecture] = architectures;
  const [versionBinding] = versionBindings;
  if (!allowedArchitectures.includes(architecture)) {
    throw new Error("P13_UPDATER_ARTIFACT_TARGET_MISMATCH");
  }

  const signer = parseSignerPublicKey(await readFile(publicKeyPath, "utf8"));
  const signaturePairs = [
    ["installerSignature", "installer"],
    ["updaterSignature", "updaterPayload"],
  ].filter(([signatureRole]) => selected[signatureRole]);
  for (const [signatureRole, artifactRole] of signaturePairs) {
    verifySignatureBinding(
      await readFile(selected[signatureRole], "utf8"),
      basename(selected[artifactRole]),
      signer,
    );
  }

  const artifacts = {};
  for (const { role } of definition.roles) {
    artifacts[role] = await describeArtifact(
      paths,
      selected[role],
      buildStartedAtMs,
    );
  }
  const fixtureSource = resolve(paths.source, FIXTURE_RELATIVE_ROOT);
  const cargoLockPath = resolve(fixtureSource, SOURCE_FILES.cargoLock);
  const fixtureLockPath = resolve(fixtureSource, SOURCE_FILES.lock);
  const evidence = {
    schemaVersion: 2,
    generatedAtUtc: new Date().toISOString(),
    bundle,
    version,
    versionBinding,
    targetTriple,
    artifactArchitecture: architecture,
    artifactMode: "v1Compatible",
    buildStartedAtUtc: new Date(buildStartedAtMs).toISOString(),
    selectedDriver: {
      name: "tauri-plugin-updater",
      version: EXPECTED.crates["tauri-plugin-updater"].version,
      source: EXPECTED.crates["tauri-plugin-updater"].source,
      license: EXPECTED.crates["tauri-plugin-updater"].license,
      maturity: "official-maintained",
    },
    comparisonDriver: {
      ...EXPECTED.comparisonDriver,
      selected: false,
      riskApprovalRequired: true,
      riskApproval: null,
    },
    toolchain: {
      node: process.version,
      tauriCli: { ...EXPECTED.cli },
      tauriBuild: { ...EXPECTED.crates["tauri-build"] },
      tauriCore: { ...EXPECTED.crates.tauri },
    },
    fixtureDependencies: {
      npm: { "@tauri-apps/cli": { ...EXPECTED.cli } },
      cargo: Object.fromEntries(
        Object.entries(EXPECTED.crates).map(([name, metadata]) => [
          name,
          { ...metadata },
        ]),
      ),
    },
    dependencyLocks: {
      cargoLockSha256: await sha256File(cargoLockPath),
      fixtureLockSha256: await sha256File(fixtureLockPath),
    },
    signer: {
      format: "tauri-minisign-ed25519",
      publicKeyFingerprint: signer.fingerprint,
      keyMaterialRecorded: false,
    },
    signatureBinding: {
      signerKeyIdMatched: true,
      trustedCommentFileMatched: true,
      cryptographicVerificationOwner: "tauri-plugin-updater-live-matrix",
    },
    artifacts,
  };
  await mkdir(paths.evidence, { recursive: true, mode: 0o700 });
  await writeFile(
    resolve(paths.evidence, `artifacts-${bundle}-${version}.json`),
    `${JSON.stringify(evidence, null, 2)}\n`,
    { mode: 0o600 },
  );
  return evidence;
}

async function runBuild(repositoryRoot, fixtureRoot, paths, bundle) {
  const signing = await ensureSigningKey(repositoryRoot, fixtureRoot, paths);
  const publicKey = (await readFile(signing.publicKeyPath, "utf8")).trim();
  const version = validateArtifactVersion(
    process.env.CMCLIENT_P13_FIXTURE_VERSION ?? "0.1.0",
  );
  const targetDirectoryTriple = process.env.CARGO_BUILD_TARGET?.trim();
  const targetTriple = targetDirectoryTriple || hostTargetTriple();
  const buildStartedAtMs = Date.now();
  await spawnChecked(
    process.execPath,
    [
      tauriCliScript(repositoryRoot),
      "build",
      ...(targetDirectoryTriple ? ["--target", targetDirectoryTriple] : []),
      "--ci",
      "--bundles",
      bundle,
      "--config",
      JSON.stringify({
        version,
        plugins: { updater: { pubkey: publicKey } },
      }),
    ],
    {
      cwd: fixtureRoot,
      env: childEnvironment(paths, {
        TAURI_SIGNING_PRIVATE_KEY: signing.privateKey.trim(),
        TAURI_SIGNING_PRIVATE_KEY_PASSWORD: "cmclient-p13-fixture-only",
      }),
      timeoutMs: 30 * 60_000,
    },
  );
  return inspectArtifacts(paths, {
    bundle,
    version,
    targetTriple,
    targetDirectoryTriple,
    buildStartedAtMs,
    publicKeyPath: signing.publicKeyPath,
  });
}

async function runCargo(fixtureRoot, paths, command) {
  const manifest = resolve(fixtureRoot, "src-tauri/Cargo.toml");
  await spawnChecked(
    "cargo",
    [command, "--locked", "--manifest-path", manifest],
    {
      cwd: fixtureRoot,
      env: childEnvironment(paths),
      timeoutMs: 20 * 60_000,
    },
  );
}

export function cargoExecutablePath(paths, targetTriple = "") {
  return resolve(
    paths.target,
    ...(targetTriple.trim() ? [targetTriple.trim()] : []),
    "debug",
    `cmclient-p13-updater-fixture${process.platform === "win32" ? ".exe" : ""}`,
  );
}

async function runProbe(fixtureRoot, paths) {
  await runCargo(fixtureRoot, paths, "build");
  const executable = cargoExecutablePath(paths, process.env.CARGO_BUILD_TARGET);
  await spawnChecked(executable, [], {
    cwd: paths.root,
    env: childEnvironment(paths, { CMCLIENT_P13_UPDATER_MODE: "probe" }),
    timeoutMs: 15_000,
  });
}

async function main(argumentsList = process.argv.slice(2)) {
  const command = argumentsList[0] ?? "verify";
  const repositoryRoot = resolve(".");
  const errors = await checkFixture(repositoryRoot);
  if (errors.length > 0) {
    process.stderr.write(`${errors.join("\n")}\n`);
    return 1;
  }
  if (command === "verify") {
    process.stdout.write("P13_UPDATER_FIXTURE_CONTRACT_OK\n");
    return 0;
  }

  const paths = campaignPaths(
    repositoryRoot,
    process.env.CMCLIENT_CAMPAIGN_ROOT,
  );
  await prepareCampaign(paths);
  const fixtureRoot = await prepareCampaignSource(repositoryRoot, paths);
  if (command === "cargo-check" || command === "cargo-test") {
    await runCargo(fixtureRoot, paths, command.slice("cargo-".length));
  } else if (command === "probe") {
    await runProbe(fixtureRoot, paths);
  } else if (command === "build") {
    await runBuild(
      repositoryRoot,
      fixtureRoot,
      paths,
      argumentsList[1] ?? platformBundle(),
    );
  } else if (command === "inspect") {
    const startedAt =
      argumentsList[3] ?? process.env.CMCLIENT_P13_BUILD_STARTED_AT_UTC;
    const buildStartedAtMs = startedAt ? Date.parse(startedAt) : Number.NaN;
    const targetDirectoryTriple = process.env.CARGO_BUILD_TARGET?.trim();
    await inspectArtifacts(paths, {
      bundle: argumentsList[1] ?? platformBundle(),
      version: argumentsList[2] ?? process.env.CMCLIENT_P13_FIXTURE_VERSION,
      targetTriple: targetDirectoryTriple || hostTargetTriple(),
      targetDirectoryTriple,
      buildStartedAtMs,
    });
  } else {
    throw new Error("P13_UPDATER_FIXTURE_COMMAND_INVALID");
  }
  process.stdout.write(`P13_UPDATER_FIXTURE_${command.toUpperCase()}_OK\n`);
  return 0;
}

const invokedPath = process.argv[1];
if (
  invokedPath &&
  import.meta.url === pathToFileURL(resolve(invokedPath)).href
) {
  process.exitCode = await main();
}
