import { spawnSync } from "node:child_process";
import { lstat, readFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";

const SCANNER_PATH = "scripts/legacy-removal-scan.mjs";

export const ALLOWED_REMOVAL_EVIDENCE = new Map([
  [
    "AGENTS.md",
    [
      {
        code: "LEGACY_MAP_REFERENCE",
        line: 9,
        count: 2,
        context:
          "- 不直接搬 Legacy Electron、raw HTTP、TENMAN/TENMAP、舊 updater 或舊 `@cm` Bot。",
      },
      {
        code: "LEGACY_BOT_REFERENCE",
        line: 9,
        count: 1,
        context:
          "- 不直接搬 Legacy Electron、raw HTTP、TENMAN/TENMAP、舊 updater 或舊 `@cm` Bot。",
      },
    ],
  ],
  [
    "crates/legacy-migration/src/lib.rs",
    [
      {
        code: "LEGACY_MAP_REFERENCE",
        line: 134,
        count: 1,
        context: '            "shareWithTenmanMap" => findings.push(finding(',
      },
      {
        code: "LEGACY_MAP_REFERENCE",
        line: 137,
        count: 1,
        context: '                "LEGACY_SETTINGS_REMOVED_TENMAN",',
      },
      {
        code: "LEGACY_MAP_REFERENCE",
        line: 254,
        count: 1,
        context: '            finding.field == "shareWithTenmanMap"',
      },
    ],
  ],
  [
    "docs/architecture/ARCHITECTURE_DECISIONS.md",
    [
      {
        code: "LEGACY_MAP_REFERENCE",
        line: 68,
        count: 2,
        context:
          "TENMAN, TENMAP, their queues/privacy text/environment variables, and the old",
      },
      {
        code: "LEGACY_BOT_REFERENCE",
        line: 69,
        count: 1,
        context:
          "`@cm` Bot are removed without a compatibility layer. The later Remote Message",
      },
    ],
  ],
  [
    "docs/architecture/legacy-settings-migration.md",
    [
      {
        code: "LEGACY_MAP_REFERENCE",
        line: 24,
        count: 2,
        context: "| `shareWithTenmanMap` | `LEGACY_SETTINGS_REMOVED_TENMAN` |",
      },
      {
        code: "LEGACY_MAP_REFERENCE",
        line: 106,
        count: 2,
        context:
          "claim. CallMesh credentials/mappings, APRS cache/backtrack data, TENMAN/TENMAP",
      },
    ],
  ],
  [
    "docs/legacy-feature-matrix.md",
    [
      {
        code: "LEGACY_MAP_REFERENCE",
        line: 46,
        count: 2,
        context:
          "| TENMAN and TENMAP sharing | Remove | Includes outbound sharing, queues, retries, environment variables, logs, privacy text, docs, UI, and database remnants |",
      },
      {
        code: "LEGACY_MAP_REFERENCE",
        line: 47,
        count: 2,
        context:
          "| Old `@cm` auto-reply Bot | Remove | It is coupled to TENMAN/TENMAP and is not a CMClient 2.0 feature |",
      },
      {
        code: "LEGACY_BOT_REFERENCE",
        line: 47,
        count: 1,
        context:
          "| Old `@cm` auto-reply Bot | Remove | It is coupled to TENMAN/TENMAP and is not a CMClient 2.0 feature |",
      },
      {
        code: "LEGACY_MAP_REFERENCE",
        line: 48,
        count: 1,
        context:
          "| TenManMap bidirectional message bridge | Remove | No protocol, WebSocket, queue, or compatibility shim remains |",
      },
      {
        code: "LEGACY_MAP_REFERENCE",
        line: 63,
        count: 1,
        context:
          "| Remote Message Dispatch | Later independent capability. It is neither a TENMAN replacement nor an `@cm` compatibility layer; only a feature flag/contract may appear before its later phase. |",
      },
      {
        code: "LEGACY_BOT_REFERENCE",
        line: 63,
        count: 1,
        context:
          "| Remote Message Dispatch | Later independent capability. It is neither a TENMAN replacement nor an `@cm` compatibility layer; only a feature flag/contract may appear before its later phase. |",
      },
    ],
  ],
  [
    "docs/legacy-inventory.md",
    [
      {
        code: "LEGACY_MAP_REFERENCE",
        line: 77,
        count: 2,
        context:
          "raw HTTP server, CallMesh coupling, TENMAN/TENMAP, and self-updating Node",
      },
    ],
  ],
  [
    "docs/testing/legacy-characterization.md",
    [
      {
        code: "LEGACY_MAP_REFERENCE",
        line: 49,
        count: 2,
        context:
          "| Removed features | TENMAN/TENMAP and `@cm` code/docs | Repository scan proves no code, environment variable, database, UI, fixture, or documentation compatibility path remains | P11 |",
      },
      {
        code: "LEGACY_BOT_REFERENCE",
        line: 49,
        count: 1,
        context:
          "| Removed features | TENMAN/TENMAP and `@cm` code/docs | Repository scan proves no code, environment variable, database, UI, fixture, or documentation compatibility path remains | P11 |",
      },
    ],
  ],
  [
    "scripts/docker.test.mjs",
    [
      {
        code: "LEGACY_MAP_REFERENCE",
        line: 73,
        count: 1,
        context: "    /TENMAN|TMAG_|AUTO_UPDATE|git clone|callmesh-client/i,",
      },
    ],
  ],
  [
    "test/fixtures/legacy-settings-sanitized.json",
    [
      {
        code: "LEGACY_MAP_REFERENCE",
        line: 5,
        count: 1,
        context: '  "shareWithTenmanMap": false,',
      },
    ],
  ],
]);

const FORBIDDEN_ARTIFACT_PATHS = [
  /(?:^|\/)(?:\.env(?:[._-][^/]*)?|[^/]+\.env(?:[._-][^/]*)?)$/i,
  /\.(?:sqlite3?|db3?)(?:-(?:wal|shm))?$/i,
  /\.(?:tar|tgz|tar\.(?:gz|zst)|zip)$/i,
  /\.(?:log|out)$/i,
];

const FORBIDDEN_LEGACY_RUNTIME_PATHS = [
  /^\.gitmodules$/,
  /^src(?:\/|$)/,
  /^meshtastic(?:-device)?$/,
  /^decrypt_meshtastic\.py$/,
  /^test_[^/]*\.js$/,
  /^scripts\/(?:aprs-feed-test|build-linux|build-win|fix-telemetry-timestamps|run-electron|test-aprs-feed|testAprsConnection)\.js$/,
  /^docs\/(?:callmesh-client|meshwaya-anti-backtrack)\.md$/,
  /^meshtastic_aprs_antibacktrack_spec\.md$/,
];

const FORBIDDEN_PACKAGE_DEPENDENCIES = new Set([
  "@yao-pkg/pkg",
  "bonjour-service",
  "chart.js",
  "electron",
  "electron-packager",
  "geojson-vt",
  "maplibre-gl",
  "unishox2.siara.cc",
  "vt-pbf",
  "ws",
  "yargs",
]);

const FORBIDDEN_PACKAGE_SCRIPT =
  /(?:electron(?:-packager)?|@yao-pkg\/pkg|node\s+src\/index\.js|scripts\/(?:build-(?:linux|win)|run-electron)\.js)/i;

const REQUIRED_RETAINED_PATHS = new Set([
  "crates/legacy-migration/src/lib.rs",
  "crates/legacy-migration/tests/cli.rs",
  "docs/architecture/legacy-settings-migration.md",
  "docs/legacy-feature-matrix.md",
  "docs/legacy-inventory.md",
  "docs/testing/legacy-characterization.md",
  "proto/meshtastic/mesh.proto",
  "test/fixtures/legacy-settings-sanitized.json",
]);

const FORBIDDEN_CONTENT = [
  ["LEGACY_MAP_REFERENCE", /tenman(?:map)?|tenmap/gi],
  ["LEGACY_ENV_REFERENCE", /\bTMAG_[A-Z0-9_]+\b/g],
  ["LEGACY_BOT_REFERENCE", /@cm(?![A-Z0-9_-])|(?:legacy|old)[\s_.-]*bot/gi],
];

function searchableText(bytes) {
  const decoded = bytes.includes(0)
    ? bytes.toString("latin1").replaceAll("\0", "")
    : bytes.toString("utf8");
  return decoded.normalize("NFKC");
}

function rawViolations(path, bytes) {
  const violations = [];
  if (
    FORBIDDEN_ARTIFACT_PATHS.some((pattern) => pattern.test(path)) ||
    /tenman(?:map)?|tenmap|tmag/i.test(path)
  ) {
    violations.push({ path, code: "LEGACY_ARTIFACT_PATH", count: 1 });
  }
  if (FORBIDDEN_LEGACY_RUNTIME_PATHS.some((pattern) => pattern.test(path))) {
    violations.push({ path, code: "LEGACY_RUNTIME_PATH", count: 1 });
  }
  if (path === SCANNER_PATH) {
    return violations;
  }

  const binary = bytes.includes(0);
  const lines = searchableText(bytes).split(/\r?\n/);
  for (const [index, context] of lines.entries()) {
    for (const [code, pattern] of FORBIDDEN_CONTENT) {
      if (binary && code === "LEGACY_BOT_REFERENCE") {
        continue;
      }
      const count = context.match(pattern)?.length ?? 0;
      if (count > 0) {
        violations.push({ path, code, line: index + 1, context, count });
      }
    }
  }
  return violations;
}

function packageManifestViolations(path, bytes) {
  if (!/(?:^|\/)package\.json$/.test(path)) {
    return [];
  }

  let manifest;
  try {
    manifest = JSON.parse(bytes.toString("utf8"));
  } catch {
    return [{ path, code: "LEGACY_SCAN_INVALID_PACKAGE_MANIFEST", count: 1 }];
  }

  const violations = [];
  const dependencySections = [
    "dependencies",
    "devDependencies",
    "optionalDependencies",
    "peerDependencies",
  ];
  for (const section of dependencySections) {
    for (const dependency of Object.keys(manifest[section] ?? {})) {
      if (
        FORBIDDEN_PACKAGE_DEPENDENCIES.has(dependency) ||
        dependency.startsWith("@electron/")
      ) {
        violations.push({
          path,
          code: "LEGACY_PACKAGE_DEPENDENCY",
          detail: `${section}:${dependency}`,
          count: 1,
        });
      }
    }
  }
  for (const [name, command] of Object.entries(manifest.scripts ?? {})) {
    if (typeof command === "string" && FORBIDDEN_PACKAGE_SCRIPT.test(command)) {
      violations.push({
        path,
        code: "LEGACY_PACKAGE_SCRIPT",
        detail: name,
        count: 1,
      });
    }
  }
  if (
    path === "package.json" &&
    ["dependencies", "optionalDependencies", "peerDependencies"].some(
      (section) => Object.keys(manifest[section] ?? {}).length > 0,
    )
  ) {
    violations.push({
      path,
      code: "LEGACY_ROOT_RUNTIME_DEPENDENCY",
      count: 1,
    });
  }
  return violations;
}

export function scanTrackedMode(path, mode) {
  return mode === "160000" ? [{ path, code: "LEGACY_GITLINK", count: 1 }] : [];
}

export function scanEntry(path, bytes) {
  const remaining = [
    ...rawViolations(path, bytes),
    ...packageManifestViolations(path, bytes),
  ];
  const allowances = ALLOWED_REMOVAL_EVIDENCE.get(path) ?? [];
  const mismatches = [];

  for (const allowance of allowances) {
    const matchIndex = remaining.findIndex(
      (violation) =>
        violation.code === allowance.code &&
        violation.line === allowance.line &&
        violation.count === allowance.count &&
        violation.context === allowance.context,
    );
    if (matchIndex === -1) {
      const actual = remaining.find(
        (violation) =>
          violation.code === allowance.code &&
          violation.line === allowance.line,
      );
      mismatches.push({
        path,
        code: "LEGACY_EVIDENCE_MISMATCH",
        rule: allowance.code,
        line: allowance.line,
        count: actual?.count ?? 0,
        expected: allowance.count,
      });
      continue;
    }
    remaining.splice(matchIndex, 1);
  }

  return [...remaining, ...mismatches];
}

export async function scanTrackedRepository(root = process.cwd()) {
  const tracked = spawnSync("git", ["-C", root, "ls-files", "--stage", "-z"], {
    encoding: "utf8",
    maxBuffer: 16 * 1024 * 1024,
  });
  if (tracked.status !== 0) {
    throw new Error("LEGACY_REMOVAL_SCAN_GIT_FAILED");
  }
  const entries = tracked.stdout
    .split("\0")
    .filter(Boolean)
    .map((record) => {
      const separator = record.indexOf("\t");
      const metadata = record.slice(0, separator).split(" ");
      if (separator < 0 || metadata.length !== 3 || metadata[2] !== "0") {
        throw new Error("LEGACY_REMOVAL_SCAN_INDEX_FAILED");
      }
      return { mode: metadata[0], path: record.slice(separator + 1) };
    })
    .sort((left, right) => left.path.localeCompare(right.path));
  const violations = [];
  const trackedPaths = new Set(entries.map(({ path }) => path));
  for (const requiredPath of REQUIRED_RETAINED_PATHS) {
    if (!trackedPaths.has(requiredPath)) {
      violations.push({
        path: requiredPath,
        code: "LEGACY_RETAINED_EVIDENCE_MISSING",
        count: 1,
      });
    }
  }
  for (const { mode, path } of entries) {
    violations.push(...scanTrackedMode(path, mode));
    let metadata;
    try {
      metadata = await lstat(join(root, path));
    } catch (error) {
      if (error?.code === "ENOENT") {
        continue;
      }
      throw error;
    }
    if (metadata.isDirectory()) {
      violations.push(...scanEntry(path, Buffer.alloc(0)));
      continue;
    }
    if (!metadata.isFile()) {
      violations.push({
        path,
        code: "LEGACY_SCAN_UNSUPPORTED_ENTRY",
        count: 1,
      });
      continue;
    }
    let bytes;
    try {
      bytes = await readFile(join(root, path));
    } catch (error) {
      if (error?.code === "ENOENT") {
        continue;
      }
      throw error;
    }
    violations.push(...scanEntry(path, bytes));
  }
  return violations.sort(
    (left, right) =>
      left.path.localeCompare(right.path) ||
      left.code.localeCompare(right.code) ||
      (left.line ?? 0) - (right.line ?? 0),
  );
}

async function main() {
  const violations = await scanTrackedRepository(resolve(process.cwd()));
  if (violations.length === 0) {
    process.stdout.write("Legacy removal scan passed\n");
    return;
  }
  for (const violation of violations) {
    const line = violation.line === undefined ? "" : ` line=${violation.line}`;
    const detail =
      violation.detail === undefined ? "" : ` detail=${violation.detail}`;
    const count =
      violation.expected === undefined
        ? `count=${violation.count}`
        : `rule=${violation.rule} count=${violation.count} expected=${violation.expected}`;
    process.stderr.write(
      `${violation.code} ${violation.path}${line}${detail} ${count}\n`,
    );
  }
  process.exitCode = 1;
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(resolve(process.argv[1])).href
) {
  main().catch((error) => {
    process.stderr.write(`${error.message}\n`);
    process.exitCode = 1;
  });
}
