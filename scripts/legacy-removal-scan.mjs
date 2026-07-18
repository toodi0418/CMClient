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
        context:
          "| `shareWithTenmanMap` | `LEGACY_SETTINGS_REMOVED_TENMAN` |",
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
        line: 37,
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

const FORBIDDEN_CONTENT = [
  ["LEGACY_MAP_REFERENCE", /tenman(?:map)?|tenmap/gi],
  ["LEGACY_ENV_REFERENCE", /\bTMAG_[A-Z0-9_]+\b/g],
  [
    "LEGACY_BOT_REFERENCE",
    /@cm(?![A-Z0-9_-])|(?:legacy|old)[\s_.-]*bot/gi,
  ],
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

export function scanEntry(path, bytes) {
  const remaining = rawViolations(path, bytes);
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
          violation.code === allowance.code && violation.line === allowance.line,
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
  const tracked = spawnSync("git", ["-C", root, "ls-files", "-z"], {
    encoding: "utf8",
    maxBuffer: 16 * 1024 * 1024,
  });
  if (tracked.status !== 0) {
    throw new Error("LEGACY_REMOVAL_SCAN_GIT_FAILED");
  }
  const paths = tracked.stdout.split("\0").filter(Boolean).sort();
  const violations = [];
  for (const path of paths) {
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
    const count =
      violation.expected === undefined
        ? `count=${violation.count}`
        : `rule=${violation.rule} count=${violation.count} expected=${violation.expected}`;
    process.stderr.write(
      `${violation.code} ${violation.path}${line} ${count}\n`,
    );
  }
  process.exitCode = 1;
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  main().catch((error) => {
    process.stderr.write(`${error.message}\n`);
    process.exitCode = 1;
  });
}
