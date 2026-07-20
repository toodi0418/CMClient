import assert from "node:assert/strict";
import { cp, mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import test from "node:test";

import { checkDocumentation } from "./documentation-contract.mjs";

const REPOSITORY_ROOT = resolve(".");
const FIXTURE_PATHS = [
  "AGENTS.md",
  "CHANGELOG.md",
  "README.md",
  "apps/README.md",
  "apps/agent/src/main.rs",
  "apps/service-host/src/main.rs",
  "apps/gateway/src/app.ts",
  "apps/gateway/src/aprs-runtime.ts",
  "apps/gateway/src/jobs.ts",
  "apps/gateway/src/main.ts",
  "apps/gateway/src/maintenance.ts",
  "apps/gateway/src/mesh-runtime.ts",
  "apps/gateway/src/proxy/runtime.ts",
  "crates/control-api/src/lib.rs",
  "docs",
  "package.json",
  "packages/README.md",
  "scripts/cmclient-windows-service.ps1",
];

test("documentation covers production routes, events, artifacts, and local links", async () => {
  assert.deepEqual(await checkDocumentation(), []);
});

test("rejects obsolete public product claims in the authoritative contract", async () => {
  await withFixture(async (root) => {
    const relativePath = "docs/architecture/CMCLIENT_2_OVERVIEW.md";
    const path = join(root, relativePath);
    const source = await readFile(path, "utf8");
    const claim = "Every target builds `desktop`, `headless`, and `cli`";
    await writeFile(path, `${source}\n${claim}.\n`);

    const errors = await checkDocumentation(root);
    assert.ok(
      errors.includes(
        `authoritative unified contract contains obsolete claim: ${relativePath} -> ${claim}`,
      ),
      errors.join("\n"),
    );
  });
});

test("rejects a documented path when its Gateway or Agent method is wrong", async () => {
  await withFixture(async (root) => {
    await replaceInFile(
      root,
      "docs/api/README.md",
      "POST /api/v1/jobs/:jobId/cancel",
      "GET /api/v1/jobs/:jobId/cancel",
    );
    await replaceInFile(
      root,
      "docs/api/jobs.md",
      "POST /api/v1/jobs/{jobId}/cancel",
      "GET /api/v1/jobs/{jobId}/cancel",
    );
    await replaceInFile(
      root,
      "docs/api/jobs.md",
      "cancel with `POST\n/api/v1/jobs/:jobId/cancel`",
      "cancel with `GET\n/api/v1/jobs/:jobId/cancel`",
    );
    await replaceInFile(
      root,
      "docs/architecture/management-web.md",
      "POST /api/v1/auth/login",
      "GET /api/v1/auth/login",
    );
    await replaceInFile(
      root,
      "docs/api/README.md",
      "POST /api/v1/auth/login",
      "GET /api/v1/auth/login",
    );

    const errors = await checkDocumentation(root);
    assert.ok(
      errors.includes(
        "Gateway route is undocumented: POST /api/v1/jobs/:jobId/cancel",
      ),
      errors.join("\n"),
    );
    assert.ok(
      errors.includes("Agent route is undocumented: POST /api/v1/auth/login"),
      errors.join("\n"),
    );
  });
});

test("rejects a documented route that has no production owner", async () => {
  await withFixture(async (root) => {
    const path = join(root, "docs/api/README.md");
    const source = await readFile(path, "utf8");
    await writeFile(
      path,
      `${source}\nRetired fixture: \`GET /api/v1/phantom\`.\n`,
    );

    const errors = await checkDocumentation(root);
    assert.ok(
      errors.includes(
        "documentation contains non-production route: GET /api/v1/phantom",
      ),
      errors.join("\n"),
    );
  });
});

test("rejects missing and phantom entries in the production event catalog", async () => {
  await withFixture(async (root) => {
    await replaceInFile(
      root,
      "docs/api/events.md",
      "mesh.transport.state",
      "phantom.event",
    );

    const errors = await checkDocumentation(root);
    assert.ok(
      errors.includes("event catalog is missing type: mesh.transport.state"),
      errors.join("\n"),
    );
    assert.ok(
      errors.includes(
        "event catalog contains non-production type: phantom.event",
      ),
      errors.join("\n"),
    );
  });
});

test("rejects a dynamic private event publish helper call", async () => {
  await withFixture(async (root) => {
    await replaceInFile(
      root,
      "apps/gateway/src/mesh-runtime.ts",
      'this.publish("mesh.transport.state",',
      "this.publish(dynamicEventType,",
    );

    const errors = await checkDocumentation(root);
    assert.ok(
      errors.some((error) =>
        error.startsWith(
          "event publish type must use direct or conditional literals: apps/gateway/src/mesh-runtime.ts:",
        ),
      ),
      errors.join("\n"),
    );
  });
});

test("rejects a Windows service name override that production does not support", async () => {
  await withFixture(async (root) => {
    const path = join(root, "docs/admin/deployment.md");
    const source = await readFile(path, "utf8");
    await writeFile(
      path,
      `${source}\nThe unsupported stale claim says -ServiceName overrides the singleton.\n`,
    );

    const errors = await checkDocumentation(root);
    assert.ok(
      errors.includes(
        "Windows service documentation claims an unsupported -ServiceName override",
      ),
      errors.join("\n"),
    );
  });
});

test("rejects service endpoint and remote token boundary documentation drift", async () => {
  await withFixture(async (root) => {
    await replaceInFile(
      root,
      "docs/admin/configuration-security.md",
      "unix:///var/lib/cmclient/control.sock",
      "unix:///tmp/cmclient.sock",
    );
    await replaceInFile(
      root,
      "docs/admin/configuration-security.md",
      "32 through 4096 UTF-8 bytes",
      "an unspecified number of bytes",
    );

    const errors = await checkDocumentation(root);
    assert.ok(
      errors.includes(
        "required documentation content is missing: docs/admin/configuration-security.md -> unix:///var/lib/cmclient/control.sock",
      ),
      errors.join("\n"),
    );
    assert.ok(
      errors.includes(
        "required documentation content is missing: docs/admin/configuration-security.md -> 32 through 4096 UTF-8 bytes",
      ),
      errors.join("\n"),
    );
  });
});

test("rejects a missing canonical release artifact file name", async () => {
  await withFixture(async (root) => {
    const fileName = "cmclient-docker-linux-aarch64-2.0.0-rc.1.oci.tar";
    await replaceInFile(
      root,
      "docs/releases/2.0.0-rc.1.md",
      `${fileName}\n`,
      "",
    );

    const errors = await checkDocumentation(root);
    assert.ok(
      errors.includes(`release notes are missing artifact: ${fileName}`),
    );
  });
});

test("checks inline, titled, root-relative, anchored, and reference links", async () => {
  await withFixture(async (root) => {
    const readmePath = join(root, "README.md");
    const readme = await readFile(readmePath, "utf8");
    await writeFile(
      readmePath,
      `${readme}\n[API routes](docs/api/README.md#gateway-route-index "Routes")\n[Events](/docs/api/events.md)\n[Control API][control-api]\n[Missing](docs/api/not-present.md)\n\n[control-api]: <docs/api/local-control.md> "Local control"\n`,
    );

    const errors = (await checkDocumentation(root)).filter((error) =>
      error.includes("documentation link"),
    );
    assert.deepEqual(errors, [
      "broken documentation link: README.md -> docs/api/not-present.md",
    ]);
  });
});

test("checks fragment-only links against the current Markdown document", async () => {
  await withFixture(async (root) => {
    const readmePath = join(root, "README.md");
    const readme = await readFile(readmePath, "utf8");
    await writeFile(
      readmePath,
      `${readme}\n[Valid local heading](#cmclient-20)\n[Missing local heading](#not-present)\n`,
    );

    const errors = await checkDocumentation(root);
    assert.ok(
      errors.includes("broken documentation anchor: README.md -> #not-present"),
      errors.join("\n"),
    );
    assert.equal(
      errors.some((error) => error.includes("README.md -> #cmclient-20")),
      false,
    );
  });
});

async function withFixture(callback) {
  const root = await mkdtemp(join(tmpdir(), "cmclient-doc-contract-"));
  try {
    for (const relativePath of FIXTURE_PATHS) {
      const destination = join(root, relativePath);
      await mkdir(dirname(destination), { recursive: true });
      await cp(join(REPOSITORY_ROOT, relativePath), destination, {
        recursive: true,
      });
    }
    await callback(root);
  } finally {
    await rm(root, { force: true, recursive: true });
  }
}

async function replaceInFile(root, relativePath, before, after) {
  const path = join(root, relativePath);
  const source = await readFile(path, "utf8");
  assert.ok(source.includes(before), `${relativePath} fixture text is missing`);
  await writeFile(path, source.replaceAll(before, after));
}
