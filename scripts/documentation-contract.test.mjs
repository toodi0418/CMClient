import assert from "node:assert/strict";
import { cp, mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import test from "node:test";

import {
  checkDocumentation,
  extractAgentRoutes,
  extractGatewayRoutes,
} from "./documentation-contract.mjs";

const REPOSITORY_ROOT = resolve(".");
const FIXTURE_PATHS = [
  "AGENTS.md",
  "Cargo.lock",
  "Cargo.toml",
  "CHANGELOG.md",
  "LICENSE",
  "NOTICE",
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
  "crates/agent-core/src/web.rs",
  "crates/control-api/src/lib.rs",
  "docs",
  "package.json",
  "packages/README.md",
  "scripts/cmclient-windows-service.ps1",
];

test("documentation covers production routes, events, artifacts, and local links", async () => {
  assert.deepEqual(await checkDocumentation(), []);
});

test("pins Gateway SSE ownership and excludes handwritten wire framing", async () => {
  const [packageSource, appSource, runtimeDocumentation, eventDocumentation] =
    await Promise.all([
      readFile(join(REPOSITORY_ROOT, "apps/gateway/package.json"), "utf8"),
      readFile(join(REPOSITORY_ROOT, "apps/gateway/src/app.ts"), "utf8"),
      readFile(
        join(REPOSITORY_ROOT, "docs/architecture/gateway-runtime.md"),
        "utf8",
      ),
      readFile(join(REPOSITORY_ROOT, "docs/api/events.md"), "utf8"),
    ]);
  const gatewayPackage = JSON.parse(packageSource);
  const normalizedRuntimeDocumentation = runtimeDocumentation.replace(
    /\s+/g,
    " ",
  );
  const normalizedEventDocumentation = eventDocumentation.replace(/\s+/g, " ");

  assert.equal(gatewayPackage.dependencies?.["@fastify/sse"], "0.5.0");
  assert.ok(appSource.includes('from "@fastify/sse"'));
  assert.ok(appSource.includes("app.register(fastifySSE"));
  const gatewayRouteKeys = new Set(
    extractGatewayRoutes(appSource).map(
      ({ method, path }) => `${method} ${path}`,
    ),
  );
  assert.ok(gatewayRouteKeys.has("GET /api/v1/events"));
  assert.ok(gatewayRouteKeys.has("GET /api/v1/jobs/:jobId/events"));

  for (const token of [
    "exact `@fastify/sse` `0.5.0` pin",
    "wire framing",
    "writable-stream backpressure",
    "CMClient owns event IDs",
    "without parsing or reframing",
    "separate route namespaces, event-ID spaces, and replay stores",
  ]) {
    assert.ok(normalizedRuntimeDocumentation.includes(token), token);
    assert.ok(normalizedEventDocumentation.includes(token), token);
  }

  const forbiddenFragments = [
    ["reply", ".hijack"].join(""),
    ["write", "Head"].join(""),
    ["format", "Sse"].join(""),
    ["text/event", "-stream"].join(""),
    [": heart", "beat\\n\\n"].join(""),
  ];
  for (const fragment of forbiddenFragments) {
    assert.equal(appSource.includes(fragment), false, fragment);
  }
});

test("extracts concrete Axum Agent routes and excludes any fallbacks", () => {
  const routes = extractAgentRoutes(`
    Router::new()
      .route("/api/v1/status", get(status))
      .route(
        "/api/v1/actions",
        post(create_action),
      )
      .route("/api/v1/config", put(replace_config))
      .route("/api/v1/config", patch(update_config))
      .route("/api/v1/config", delete(delete_config))
      .route("/api/v1/status", head(status_headers))
      .route("/api/v1/status", options(status_options))
      .route("/api/v1/{*path}", any(proxy_or_deny));

    #[cfg(test)]
    mod tests {
      fn fixture_router() -> Router {
        Router::new().route("/api/v1/test-only", get(test_only))
      }
    }
  `);

  assert.deepEqual(routes, [
    { method: "GET", path: "/api/v1/status" },
    { method: "POST", path: "/api/v1/actions" },
    { method: "PUT", path: "/api/v1/config" },
    { method: "PATCH", path: "/api/v1/config" },
    { method: "DELETE", path: "/api/v1/config" },
    { method: "HEAD", path: "/api/v1/status" },
    { method: "OPTIONS", path: "/api/v1/status" },
  ]);
});

test("rejects altered GPL license bytes and manifest license drift", async () => {
  await withFixture(async (root) => {
    const licensePath = join(root, "LICENSE");
    const license = await readFile(licensePath, "utf8");
    await writeFile(licensePath, `${license}\n`);
    await replaceInFile(
      root,
      "Cargo.toml",
      'license = "GPL-3.0-only"',
      'license = "MIT"',
    );
    await replaceInFile(
      root,
      "package.json",
      '"license": "GPL-3.0-only"',
      '"license": "MIT"',
    );

    const errors = await checkDocumentation(root);
    assert.ok(
      errors.some((error) =>
        error.startsWith("root LICENSE SHA-256 is invalid:"),
      ),
      errors.join("\n"),
    );
    assert.ok(
      errors.includes(
        "Cargo workspace license must be GPL-3.0-only: received MIT",
      ),
      errors.join("\n"),
    );
    assert.ok(
      errors.includes(
        "package.json license must be GPL-3.0-only: received MIT",
      ),
      errors.join("\n"),
    );
  });
});

test("rejects stale Cargo.lock provenance", async () => {
  await withFixture(async (root) => {
    const path = join(root, "Cargo.lock");
    const source = await readFile(path, "utf8");
    await writeFile(path, `${source}\n`);

    const errors = await checkDocumentation(root);
    assert.ok(
      errors.some((error) =>
        error.startsWith("license provenance Cargo.lock SHA-256 is stale:"),
      ),
      errors.join("\n"),
    );
  });
});

test("rejects notice, provenance, and hosted CallMesh contract drift", async () => {
  await withFixture(async (root) => {
    await replaceInFile(
      root,
      "NOTICE",
      "7f1110dd7737c7884012cc899862f9d7427b9c51",
      "0000000000000000000000000000000000000000",
    );
    await replaceInFile(
      root,
      "docs/architecture/license-provenance.md",
      "ce3d3f9376b9a2552fc22c7d962ee9b25ebeda9e748301284be730fbff21b8f1",
      "0000000000000000000000000000000000000000000000000000000000000000",
    );
    await replaceInFile(
      root,
      "README.md",
      "only production provision",
      "one optional production provision",
    );
    await replaceInFile(
      root,
      "README.md",
      "or support production",
      "but does support production",
    );

    const errors = await checkDocumentation(root);
    assert.ok(
      errors.includes(
        "NOTICE license provenance is missing: 7f1110dd7737c7884012cc899862f9d7427b9c51",
      ),
      errors.join("\n"),
    );
    assert.ok(
      errors.includes(
        "required documentation content is missing: docs/architecture/license-provenance.md -> ce3d3f9376b9a2552fc22c7d962ee9b25ebeda9e748301284be730fbff21b8f1",
      ),
      errors.join("\n"),
    );
    assert.ok(
      errors.includes(
        "README hosted CallMesh contract is missing: only production provision and mapping authority",
      ),
      errors.join("\n"),
    );
    assert.ok(
      errors.includes(
        "README hosted CallMesh contract is missing: CMClient does not ship a CallMesh server or support production endpoint and local mapping overrides.",
      ),
      errors.join("\n"),
    );
  });
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

test("checks the typed Control operation catalog exactly", async () => {
  await withFixture(async (root) => {
    await replaceInFile(
      root,
      "docs/api/local-control.md",
      "| `Status` | Agent, Gateway, Management Web, identity, uptime, and stable error status |",
      "| `Phantom` | Agent, Gateway, Management Web, identity, uptime, and stable error status |",
    );

    const errors = await checkDocumentation(root);
    assert.ok(
      errors.includes("Control IPC documentation is missing operation: Status"),
      errors.join("\n"),
    );
    assert.ok(
      errors.includes(
        "Control IPC documentation contains unknown operation: Phantom",
      ),
      errors.join("\n"),
    );
  });
});

test("rejects an HTTP or remote-token Control transport regression", async () => {
  await withFixture(async (root) => {
    const documentationPath = join(root, "docs/api/local-control.md");
    const documentation = await readFile(documentationPath, "utf8");
    await writeFile(
      documentationPath,
      `${documentation}\nRetired route: GET /api/v1/control/status.\n`,
    );
    const sourcePath = join(root, "crates/control-api/src/lib.rs");
    const source = await readFile(sourcePath, "utf8");
    await writeFile(
      sourcePath,
      `${source}\n// HTTP/1.1 CMCLIENT_CONTROL_TOKEN\n`,
    );

    const errors = await checkDocumentation(root);
    assert.ok(
      errors.includes(
        "Control IPC documentation contains an obsolete HTTP route",
      ),
      errors.join("\n"),
    );
    assert.ok(
      errors.includes(
        "Control IPC source contains removed transport: HTTP/1.1",
      ),
      errors.join("\n"),
    );
    assert.ok(
      errors.includes(
        "Control IPC source contains removed transport: CMCLIENT_CONTROL_TOKEN",
      ),
      errors.join("\n"),
    );
  });
});

test("rejects missing local Control hardening guarantees", async () => {
  await withFixture(async (root) => {
    const removals = [
      "ambiguous nonblocking zero-byte read",
      "zero-length write",
      "solely a liveness probe",
      "emits no protocol bytes",
      "Peer PID is not consulted",
      "trailing available bytes",
      "connect and request setup",
      "cancels and joins every active request or subscription",
      "`ConnectionRefused`",
      "All other probe failures fail closed",
    ];
    for (const token of removals) {
      await replaceInFile(root, "docs/api/local-control.md", token, "removed");
    }

    const errors = await checkDocumentation(root);
    for (const token of removals) {
      assert.ok(
        errors.includes(`Control IPC documentation is missing: ${token}`),
        errors.join("\n"),
      );
    }
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

test("rejects unified home and removed remote token boundary drift", async () => {
  await withFixture(async (root) => {
    await replaceInFile(
      root,
      "docs/admin/configuration-security.md",
      "~/.cmclient/secrets.json",
      "~/.cmclient/other-secrets.json",
    );
    await replaceInFile(
      root,
      "docs/admin/configuration-security.md",
      "deprecated and unavailable",
      "supported for remote CLI use",
    );

    const errors = await checkDocumentation(root);
    assert.ok(
      errors.includes(
        "required documentation content is missing: docs/admin/configuration-security.md -> ~/.cmclient/secrets.json",
      ),
      errors.join("\n"),
    );
    assert.ok(
      errors.includes(
        "required documentation content is missing: docs/admin/configuration-security.md -> deprecated and unavailable",
      ),
      errors.join("\n"),
    );
  });
});

test("rejects obsolete split roots in current runtime documentation", async () => {
  await withFixture(async (root) => {
    const relativePath = "docs/user/using-cmclient.md";
    const path = join(root, relativePath);
    const source = await readFile(path, "utf8");
    await writeFile(path, `${source}\nLegacy runtime root: /etc/cmclient.\n`);

    const errors = await checkDocumentation(root);
    assert.ok(
      errors.includes(
        `current runtime documentation contains obsolete claim: ${relativePath} -> /etc/cmclient`,
      ),
      errors.join("\n"),
    );
  });
});

test("rejects the removed remote Control token flow", async () => {
  await withFixture(async (root) => {
    const relativePath = "docs/api/cli.md";
    const path = join(root, relativePath);
    const source = await readFile(path, "utf8");
    await writeFile(
      path,
      `${source}\nFor remote CLI access, use the same value through the \`CMCLIENT_CONTROL_TOKEN\` environment variable.\n`,
    );

    const errors = await checkDocumentation(root);
    assert.ok(
      errors.includes(
        `current runtime documentation contains obsolete remote token flow: ${relativePath}`,
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
