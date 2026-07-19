import { readFile, readdir, stat } from "node:fs/promises";
import { dirname, join, posix, relative, resolve, sep } from "node:path";
import { pathToFileURL } from "node:url";

import ts from "typescript";

import {
  dockerArtifactPlan,
  dockerComposeArtifactPlan,
  nativeDesktopArtifactPlan,
  releaseArtifactPlan,
} from "./release-artifacts.mjs";

const REQUIRED_DOCUMENT_SECTIONS = new Map([
  [
    "docs/user/getting-started.md",
    [
      "Verify an artifact",
      "First local run",
      "Docker first run",
      "Where state lives",
    ],
  ],
  [
    "docs/user/using-cmclient.md",
    [
      "Web areas",
      "Desktop supervisor",
      "CLI essentials",
      "Theme, language, and limits",
    ],
  ],
  [
    "docs/admin/deployment.md",
    [
      "Portable archives",
      "Native Desktop packages",
      "Linux systemd",
      "macOS launchd",
      "Windows Service",
      "Docker OCI",
      "Upgrade and removal",
    ],
  ],
  [
    "docs/admin/configuration-security.md",
    [
      "Minimal configuration",
      "Generate the Management password hash",
      "Management LAN boundary",
      "Remote CLI HMAC",
      "Fail-closed rules",
    ],
  ],
  [
    "docs/admin/operations.md",
    [
      "Health and identity",
      "Logs and events",
      "Jobs, backup, and diagnostics",
      "Updates and recovery",
      "Legacy migration",
      "Common stable codes",
    ],
  ],
  [
    "docs/developer/README.md",
    [
      "Toolchain and bootstrap",
      "Change workflow",
      "CI and release gates",
      "Boundaries that must not regress",
    ],
  ],
  [
    "docs/api/README.md",
    [
      "Error envelopes",
      "Query and body validation",
      "Agent browser routes",
      "Gateway route index",
      "Shared contracts",
    ],
  ],
  [
    "docs/api/domain-projections.md",
    [
      "Runtime projections",
      "Mesh lists",
      "Positions and APRS outbox",
      "CallMesh mapping",
      "Events snapshot",
    ],
  ],
  [
    "docs/releases/2.0.0-rc.1.md",
    [
      "Delivery set",
      "Verification",
      "Main changes",
      "Removed and disabled behavior",
      "Upgrade and known limits",
      "Promotion gate",
    ],
  ],
  ["CHANGELOG.md", ["2.0.0-rc.1"]],
]);

const REQUIRED_DOCUMENT_TOKENS = new Map([
  [
    "docs/user/getting-started.md",
    ["SHA256SUMS", "cmclient-agent --serve", "CMCLIENT_IMAGE"],
  ],
  [
    "docs/user/using-cmclient.md",
    ["cmclient status", "cmclient events --follow", "remoteDispatch"],
  ],
  [
    "docs/admin/deployment.md",
    [
      "cmclient-systemd.sh",
      "unix:///var/lib/cmclient/control.sock",
      "cmclient-windows-service.ps1",
      "fixed singleton",
      "SCM stop/shutdown",
      "service-host.jsonl",
      "cmclient-docker-compose-2.0.0-rc.1.yml",
    ],
  ],
  [
    "docs/admin/configuration-security.md",
    [
      "[callmesh]",
      "[management_lan]",
      "AGENT_SECRET_STORE_UNAVAILABLE",
      "unix:///var/lib/cmclient/control.sock",
      "restarts only that Gateway",
      "32 through 4096 UTF-8 bytes",
    ],
  ],
  [
    "docs/admin/operations.md",
    [
      "AGENT_INSTANCE_ALREADY_RUNNING",
      "cmclient-migrate",
      "CMCLIENT_LOG_MAX_BYTES",
      "RUNTIME_LOG_POLICY_INVALID",
      "unix:///var/lib/cmclient/control.sock",
      "does not carry the previous ID",
      "bounded journal tail",
      "latestErrorCode",
    ],
  ],
  [
    "docs/developer/README.md",
    ["cargo test --workspace --locked", "Release Build Matrix"],
  ],
  [
    "docs/api/README.md",
    ['{"code":"CONTROL_COMMAND_FAILED"}', "Gateway route index"],
  ],
  [
    "docs/api/domain-projections.md",
    ["precisionBits === 32", "GATEWAY_DOMAIN_DATA_UNAVAILABLE"],
  ],
  ["docs/releases/2.0.0-rc.1.md", ["SHA256SUMS", "P12-T05"]],
  ["CHANGELOG.md", ["2.0.0-rc.1"]],
]);

const REQUIRED_DOCUMENTS = [...REQUIRED_DOCUMENT_SECTIONS.keys()];

const MARKDOWN_SCAN_EXCLUDED_DIRECTORIES = new Set([
  ".git",
  ".pnpm-store",
  "coverage",
  "dist",
  "node_modules",
  "release-build",
  "release-dist",
  "target",
]);

const HTTP_METHODS = new Set([
  "GET",
  "POST",
  "PUT",
  "PATCH",
  "DELETE",
  "HEAD",
  "OPTIONS",
]);
const EVENT_TYPE_PATTERN = /^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)+$/;
const AGENT_SYNTHETIC_EVENT_TYPES = new Set(["gateway.heartbeat"]);

export async function checkDocumentation(repositoryRoot = resolve(".")) {
  const errors = [];
  const markdownPaths = await listMarkdownFiles(repositoryRoot);
  const contents = new Map();
  for (const relativePath of markdownPaths) {
    contents.set(
      relativePath,
      await readFile(join(repositoryRoot, relativePath), "utf8"),
    );
  }

  for (const relativePath of REQUIRED_DOCUMENTS) {
    if (!contents.has(relativePath)) {
      errors.push(`missing documentation: ${relativePath}`);
      continue;
    }
    checkRequiredDocumentContent(
      relativePath,
      contents.get(relativePath),
      errors,
    );
  }

  checkReadmeIndex(contents.get("README.md") ?? "", errors);

  const documentedRoutes = extractDocumentedRoutes(
    [...contents.values()].join("\n"),
  );
  const gatewaySource = await readFile(
    join(repositoryRoot, "apps/gateway/src/app.ts"),
    "utf8",
  );
  const gatewayAnalysis = analyzeGatewayRoutes(gatewaySource);
  const gatewayRoutes = gatewayAnalysis.routes;
  errors.push(...gatewayAnalysis.errors);
  checkRouteCoverage(gatewayRoutes, documentedRoutes, "Gateway", errors);

  const agentSource = await readFile(
    join(repositoryRoot, "apps/agent/src/main.rs"),
    "utf8",
  );
  const controlSource = await readFile(
    join(repositoryRoot, "crates/control-api/src/lib.rs"),
    "utf8",
  );
  const agentRoutes = extractAgentRoutes(agentSource, controlSource);
  checkRouteCoverage(agentRoutes, documentedRoutes, "Agent", errors);
  checkUnexpectedDocumentedRoutes(
    [...gatewayRoutes, ...agentRoutes],
    documentedRoutes,
    errors,
  );
  checkCanonicalRouteCatalog(
    contents.get("docs/api/README.md") ?? "",
    [
      ...gatewayRoutes,
      ...agentRoutes.filter(({ path }) => !path.startsWith("/api/v1/control/")),
    ],
    "API index",
    errors,
  );
  checkCanonicalRouteCatalog(
    contents.get("docs/api/local-control.md") ?? "",
    agentRoutes.filter(({ path }) => path.startsWith("/api/v1/control/")),
    "Control API reference",
    errors,
  );

  const [windowsManagerSource, serviceHostSource] = await Promise.all([
    readFile(
      join(repositoryRoot, "scripts/cmclient-windows-service.ps1"),
      "utf8",
    ),
    readFile(join(repositoryRoot, "apps/service-host/src/main.rs"), "utf8"),
  ]);
  checkWindowsServiceDocumentation(
    contents.get("docs/admin/deployment.md") ?? "",
    windowsManagerSource,
    serviceHostSource,
    errors,
  );

  const productionEventTypes = new Set(AGENT_SYNTHETIC_EVENT_TYPES);
  const gatewaySourcePaths = await listTypeScriptSources(
    repositoryRoot,
    join(repositoryRoot, "apps/gateway/src"),
  );
  for (const relativePath of gatewaySourcePaths) {
    const source = await readFile(join(repositoryRoot, relativePath), "utf8");
    const eventAnalysis = analyzePublishedEventTypes(source, relativePath);
    errors.push(...eventAnalysis.errors);
    for (const eventType of eventAnalysis.eventTypes) {
      productionEventTypes.add(eventType);
    }
  }
  checkEventCatalog(
    contents.get("docs/api/events.md") ?? "",
    productionEventTypes,
    errors,
  );

  const packageJson = JSON.parse(
    await readFile(join(repositoryRoot, "package.json"), "utf8"),
  );
  checkReleaseArtifactCatalog(
    contents.get("docs/releases/2.0.0-rc.1.md") ?? "",
    packageJson.version,
    errors,
  );

  await checkLocalMarkdownLinks(repositoryRoot, contents, errors);
  return errors;
}

async function listMarkdownFiles(repositoryRoot, directory = repositoryRoot) {
  const paths = [];
  const entries = await readdir(directory, { withFileTypes: true });
  entries.sort((left, right) => left.name.localeCompare(right.name, "en"));
  for (const entry of entries) {
    if (
      entry.isDirectory() &&
      MARKDOWN_SCAN_EXCLUDED_DIRECTORIES.has(entry.name)
    ) {
      continue;
    }
    const absolutePath = join(directory, entry.name);
    if (entry.isDirectory()) {
      paths.push(...(await listMarkdownFiles(repositoryRoot, absolutePath)));
    } else if (entry.isFile() && entry.name.toLowerCase().endsWith(".md")) {
      paths.push(toRepositoryPath(relative(repositoryRoot, absolutePath)));
    }
  }
  return paths;
}

async function listTypeScriptSources(repositoryRoot, directory) {
  const paths = [];
  const entries = await readdir(directory, { withFileTypes: true });
  entries.sort((left, right) => left.name.localeCompare(right.name, "en"));
  for (const entry of entries) {
    const absolutePath = join(directory, entry.name);
    if (entry.isDirectory()) {
      paths.push(
        ...(await listTypeScriptSources(repositoryRoot, absolutePath)),
      );
    } else if (
      entry.isFile() &&
      entry.name.endsWith(".ts") &&
      !entry.name.endsWith(".test.ts") &&
      !entry.name.endsWith(".spec.ts") &&
      !entry.name.endsWith(".d.ts")
    ) {
      paths.push(toRepositoryPath(relative(repositoryRoot, absolutePath)));
    }
  }
  return paths;
}

function checkRequiredDocumentContent(relativePath, documentation, errors) {
  for (const heading of REQUIRED_DOCUMENT_SECTIONS.get(relativePath) ?? []) {
    const section = markdownSection(documentation, heading);
    if (section === undefined) {
      errors.push(
        `required documentation heading is missing: ${relativePath} -> ${heading}`,
      );
    } else if (section.replace(/\s/g, "").length < 20) {
      errors.push(
        `required documentation section is empty: ${relativePath} -> ${heading}`,
      );
    }
  }
  for (const token of REQUIRED_DOCUMENT_TOKENS.get(relativePath) ?? []) {
    if (!documentation.includes(token)) {
      errors.push(
        `required documentation content is missing: ${relativePath} -> ${token}`,
      );
    }
  }
}

function checkReadmeIndex(readme, errors) {
  const indexedPaths = new Set(
    extractMarkdownDestinations(readme)
      .map((destination) => localDestinationPath(destination))
      .filter(Boolean)
      .map((path) => posix.normalize(path.replace(/^\//, ""))),
  );
  for (const relativePath of REQUIRED_DOCUMENTS) {
    const directoryIndex = relativePath.endsWith("/README.md")
      ? relativePath.slice(0, -"README.md".length)
      : undefined;
    if (
      !indexedPaths.has(relativePath) &&
      (!directoryIndex || !indexedPaths.has(directoryIndex))
    ) {
      errors.push(`README is missing documentation link: ${relativePath}`);
    }
  }
}

function checkWindowsServiceDocumentation(
  documentation,
  managerSource,
  serviceHostSource,
  errors,
) {
  const managerName = managerSource.match(
    /\$ServiceName\s*=\s*"([A-Za-z][A-Za-z0-9_-]{0,63})"/,
  )?.[1];
  const hostName = serviceHostSource.match(
    /const SERVICE_NAME:\s*&str\s*=\s*"([A-Za-z][A-Za-z0-9_-]{0,63})"/,
  )?.[1];
  if (!managerName || !hostName || managerName !== hostName) {
    errors.push("Windows service identity source contract is invalid");
    return;
  }

  const normalizedDocumentation = documentation.replace(/\s+/g, " ");
  const customNameParameter = /\[string\]\s*\$ServiceName\b/.test(
    managerSource,
  );
  if (customNameParameter && !documentation.includes("-ServiceName")) {
    errors.push("Windows service documentation is missing -ServiceName");
  }
  if (
    !customNameParameter &&
    normalizedDocumentation.includes("-ServiceName overrides")
  ) {
    errors.push(
      "Windows service documentation claims an unsupported -ServiceName override",
    );
  }
  if (
    !customNameParameter &&
    !normalizedDocumentation.includes(`fixed singleton \`${managerName}\``)
  ) {
    errors.push(
      `Windows service documentation is missing fixed singleton ${managerName}`,
    );
  }
}

export function extractGatewayRoutes(source) {
  return analyzeGatewayRoutes(source).routes;
}

function analyzeGatewayRoutes(source) {
  const sourceFile = ts.createSourceFile(
    "apps/gateway/src/app.ts",
    source,
    ts.ScriptTarget.Latest,
    true,
    ts.ScriptKind.TS,
  );
  const routes = new Map();
  const errors = [];
  visit(sourceFile, (node) => {
    if (
      !ts.isCallExpression(node) ||
      !ts.isPropertyAccessExpression(node.expression) ||
      !ts.isIdentifier(node.expression.expression) ||
      node.expression.expression.text !== "app"
    ) {
      return;
    }
    const method = node.expression.name.text.toUpperCase();
    const path = node.arguments[0];
    if (!HTTP_METHODS.has(method)) return;
    if (!path || !ts.isStringLiteralLike(path)) {
      const { line } = sourceFile.getLineAndCharacterOfPosition(
        node.getStart(),
      );
      errors.push(
        `Gateway route registration must use a direct path literal: apps/gateway/src/app.ts:${line + 1}`,
      );
      return;
    }
    if (path.text.startsWith("/api/v1/")) {
      routes.set(routeKey(method, path.text), { method, path: path.text });
    }
  });
  return { routes: [...routes.values()], errors };
}

export function extractAgentRoutes(agentSource, controlSource) {
  const routes = new Map();
  const addRoute = (method, path) => {
    if (HTTP_METHODS.has(method) && path.startsWith("/api/v1/")) {
      routes.set(routeKey(method, path), { method, path });
    }
  };

  for (const match of agentSource.matchAll(
    /\(\s*"([A-Z]+)"\s*,\s*"(\/api\/v1\/[^"\s]+)"\s*\)/g,
  )) {
    addRoute(match[1], match[2]);
  }
  for (const match of agentSource.matchAll(
    /request\.method\s*==\s*"([A-Z]+)"\s*&&\s*request\.path\s*==\s*"(\/api\/v1\/[^"\s]+)"/g,
  )) {
    addRoute(match[1], match[2]);
  }
  for (const match of controlSource.matchAll(
    /\[\s*"([A-Z]+)"\s*,\s*"(\/api\/v1\/[^"\s]+)"\s*,\s*"HTTP\/1\.[01]"\s*\]/g,
  )) {
    addRoute(match[1], match[2]);
  }

  const secretSegments = extractControlSecretSegments(controlSource);
  for (const method of ["PUT", "DELETE"]) {
    for (const segment of secretSegments) {
      addRoute(method, `/api/v1/control/secrets/${segment}`);
    }
  }
  return [...routes.values()];
}

function extractControlSecretSegments(source) {
  const block = source.match(
    /pub const fn path_segment\([^)]*\)[^{]*\{\s*match self \{([\s\S]*?)\n\s*\}\n\s*\}/,
  )?.[1];
  if (!block) return [];
  return [...block.matchAll(/Self::[A-Za-z]+\s*=>\s*"([a-z0-9-]+)"/g)].map(
    (match) => match[1],
  );
}

export function extractDocumentedRoutes(documentation) {
  const routes = new Set();
  for (const match of documentation.matchAll(
    /\b(GET|POST|PUT|PATCH|DELETE|HEAD|OPTIONS)\s+(\/api\/v1\/[A-Za-z0-9_./:{}|~-]+)/g,
  )) {
    for (const path of expandRouteAlternatives(match[2])) {
      routes.add(routeKey(match[1], path));
    }
  }
  return routes;
}

function expandRouteAlternatives(path) {
  const match = path.match(/\{([^{}]*\|[^{}]*)\}/);
  if (!match || match.index === undefined) return [path];
  return match[1]
    .split("|")
    .flatMap((value) =>
      expandRouteAlternatives(
        `${path.slice(0, match.index)}${value}${path.slice(match.index + match[0].length)}`,
      ),
    );
}

function checkRouteCoverage(routes, documentedRoutes, owner, errors) {
  for (const { method, path } of routes) {
    if (!documentedRoutes.has(routeKey(method, path))) {
      errors.push(`${owner} route is undocumented: ${method} ${path}`);
    }
  }
}

function checkUnexpectedDocumentedRoutes(routes, documentedRoutes, errors) {
  const productionRoutes = new Set(
    routes.map(({ method, path }) => routeKey(method, path)),
  );
  for (const route of [...documentedRoutes].sort()) {
    if (!productionRoutes.has(route)) {
      errors.push(`documentation contains non-production route: ${route}`);
    }
  }
}

function checkCanonicalRouteCatalog(documentation, routes, owner, errors) {
  const expected = new Set(
    routes.map(({ method, path }) => routeKey(method, path)),
  );
  const actual = extractDocumentedRoutes(documentation);
  compareExactSets(
    expected,
    actual,
    (route) => `${owner} is missing route: ${route}`,
    (route) => `${owner} contains unexpected route: ${route}`,
    errors,
  );
}

function routeKey(method, path) {
  return `${method} ${path.replace(/:([A-Za-z][A-Za-z0-9_]*)/g, "{$1}")}`;
}

export function extractPublishedEventTypes(source, sourcePath) {
  return analyzePublishedEventTypes(source, sourcePath).eventTypes;
}

function analyzePublishedEventTypes(source, sourcePath) {
  const sourceFile = ts.createSourceFile(
    sourcePath,
    source,
    ts.ScriptTarget.Latest,
    true,
    ts.ScriptKind.TS,
  );
  const eventTypes = new Set();
  const errors = [];
  visit(sourceFile, (node) => {
    if (
      ts.isPropertyAccessExpression(node) &&
      node.expression.kind === ts.SyntaxKind.ThisKeyword &&
      node.name.text === "publish" &&
      (!ts.isCallExpression(node.parent) || node.parent.expression !== node)
    ) {
      const { line } = sourceFile.getLineAndCharacterOfPosition(
        node.getStart(),
      );
      errors.push(
        `private event publish helper must be called directly: ${sourcePath}:${line + 1}`,
      );
      return;
    }
    if (
      !ts.isCallExpression(node) ||
      !ts.isPropertyAccessExpression(node.expression) ||
      node.expression.name.text !== "publish"
    ) {
      return;
    }
    const input = node.arguments[0];
    const callTypes = new Set();
    let supported = false;
    if (ts.isObjectLiteralExpression(input)) {
      const typeProperty = input.properties.find(
        (property) =>
          "name" in property && propertyName(property.name) === "type",
      );
      if (typeProperty && ts.isPropertyAssignment(typeProperty)) {
        supported = collectEventTypeLiterals(
          typeProperty.initializer,
          callTypes,
        );
      } else if (
        typeProperty &&
        ts.isShorthandPropertyAssignment(typeProperty)
      ) {
        supported = collectEventTypeLiterals(typeProperty.name, callTypes);
      }
    } else if (input) {
      supported = collectEventTypeLiterals(input, callTypes);
    }
    if (!supported) {
      const { line } = sourceFile.getLineAndCharacterOfPosition(
        node.getStart(),
      );
      errors.push(
        `event publish type must use direct or conditional literals: ${sourcePath}:${line + 1}`,
      );
      return;
    }
    for (const eventType of callTypes) eventTypes.add(eventType);
  });
  return { eventTypes, errors };
}

function collectEventTypeLiterals(node, eventTypes) {
  if (ts.isStringLiteralLike(node)) {
    if (!EVENT_TYPE_PATTERN.test(node.text)) return false;
    eventTypes.add(node.text);
    return true;
  }
  if (ts.isParenthesizedExpression(node)) {
    return collectEventTypeLiterals(node.expression, eventTypes);
  }
  if (ts.isConditionalExpression(node)) {
    return (
      collectEventTypeLiterals(node.whenTrue, eventTypes) &&
      collectEventTypeLiterals(node.whenFalse, eventTypes)
    );
  }
  if (ts.isIdentifier(node) && isPrivatePublishForwardingParameter(node)) {
    return true;
  }
  return false;
}

function isPrivatePublishForwardingParameter(identifier) {
  let current = identifier.parent;
  while (current && !ts.isSourceFile(current)) {
    if (ts.isMethodDeclaration(current)) {
      const firstParameter = current.parameters[0];
      return (
        propertyName(current.name) === "publish" &&
        current.modifiers?.some(
          (modifier) => modifier.kind === ts.SyntaxKind.PrivateKeyword,
        ) === true &&
        firstParameter !== undefined &&
        ts.isIdentifier(firstParameter.name) &&
        firstParameter.name.text === identifier.text
      );
    }
    current = current.parent;
  }
  return false;
}

function propertyName(name) {
  return ts.isIdentifier(name) || ts.isStringLiteralLike(name)
    ? name.text
    : undefined;
}

function visit(node, callback) {
  callback(node);
  ts.forEachChild(node, (child) => visit(child, callback));
}

function checkEventCatalog(documentation, expectedEventTypes, errors) {
  const catalog = fencedSection(documentation, "Event catalog");
  if (catalog === undefined) {
    errors.push("event catalog code block is missing");
    return;
  }
  const actualEventTypes = new Set();
  for (const line of catalog.split("\n")) {
    const entry = line.trim();
    if (!entry) continue;
    const eventType = entry.match(
      /^([a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)+)/,
    )?.[1];
    if (!eventType) {
      errors.push(`event catalog has invalid entry: ${entry}`);
      continue;
    }
    if (actualEventTypes.has(eventType)) {
      errors.push(`event catalog contains duplicate type: ${eventType}`);
    }
    actualEventTypes.add(eventType);
  }
  compareExactSets(
    expectedEventTypes,
    actualEventTypes,
    (eventType) => `event catalog is missing type: ${eventType}`,
    (eventType) => `event catalog contains non-production type: ${eventType}`,
    errors,
  );
}

function checkReleaseArtifactCatalog(documentation, version, errors) {
  const expected = new Set([
    ...releaseArtifactPlan(version).map((artifact) => artifact.fileName),
    ...nativeDesktopArtifactPlan(version).map((artifact) => artifact.fileName),
    ...dockerArtifactPlan(version).map((artifact) => artifact.fileName),
    dockerComposeArtifactPlan(version).fileName,
  ]);
  const match = documentation.match(
    /The exact portable and platform file names are:\s*\n+```text\s*\n([\s\S]*?)```/,
  );
  if (!match) {
    errors.push("release artifact catalog code block is missing");
    return;
  }
  const actual = new Set(
    match[1]
      .split("\n")
      .map((line) => line.trim())
      .filter(Boolean),
  );
  compareExactSets(
    expected,
    actual,
    (fileName) => `release notes are missing artifact: ${fileName}`,
    (fileName) => `release notes contain unexpected artifact: ${fileName}`,
    errors,
  );
}

function fencedSection(documentation, heading) {
  const escapedHeading = heading.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const headingMatch = new RegExp(`^## ${escapedHeading}\\s*$`, "m").exec(
    documentation,
  );
  if (!headingMatch) return undefined;
  const afterHeading = documentation.slice(
    headingMatch.index + headingMatch[0].length,
  );
  const nextHeading = afterHeading.search(/^##\s/m);
  const section =
    nextHeading === -1 ? afterHeading : afterHeading.slice(0, nextHeading);
  return section?.match(/```(?:text)?\s*\n([\s\S]*?)```/)?.[1];
}

function markdownSection(documentation, heading) {
  const escapedHeading = heading.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const headingMatch = new RegExp(`^(#{2,6}) ${escapedHeading}\\s*$`, "m").exec(
    documentation,
  );
  if (!headingMatch) return undefined;
  const level = headingMatch[1].length;
  const afterHeading = documentation.slice(
    headingMatch.index + headingMatch[0].length,
  );
  const nextHeading = new RegExp(`^#{1,${level}}\\s`, "m").exec(afterHeading);
  return nextHeading ? afterHeading.slice(0, nextHeading.index) : afterHeading;
}

function compareExactSets(
  expected,
  actual,
  missingMessage,
  extraMessage,
  errors,
) {
  for (const value of [...expected].sort()) {
    if (!actual.has(value)) errors.push(missingMessage(value));
  }
  for (const value of [...actual].sort()) {
    if (!expected.has(value)) errors.push(extraMessage(value));
  }
}

async function checkLocalMarkdownLinks(repositoryRoot, contents, errors) {
  for (const [relativePath, text] of contents) {
    for (const destination of extractMarkdownDestinations(text)) {
      const parsed = parseLocalDestination(destination);
      if (!parsed) continue;
      const absolutePath =
        parsed.path.length === 0
          ? join(repositoryRoot, relativePath)
          : parsed.path.startsWith("/")
            ? resolve(repositoryRoot, parsed.path.slice(1))
            : resolve(dirname(join(repositoryRoot, relativePath)), parsed.path);
      const repositoryRelative = relative(repositoryRoot, absolutePath);
      if (
        repositoryRelative === ".." ||
        repositoryRelative.startsWith(`..${sep}`)
      ) {
        errors.push(
          `documentation link escapes repository: ${relativePath} -> ${destination}`,
        );
        continue;
      }
      let metadata;
      try {
        metadata = await stat(absolutePath);
      } catch {
        errors.push(
          `broken documentation link: ${relativePath} -> ${destination}`,
        );
        continue;
      }
      if (
        parsed.fragment &&
        metadata.isFile() &&
        absolutePath.toLowerCase().endsWith(".md")
      ) {
        const targetPath = toRepositoryPath(repositoryRelative);
        const targetText =
          contents.get(targetPath) ?? (await readFile(absolutePath, "utf8"));
        if (!markdownAnchors(targetText).has(parsed.fragment)) {
          errors.push(
            `broken documentation anchor: ${relativePath} -> ${destination}`,
          );
        }
      }
    }
  }
}

function extractMarkdownDestinations(markdown) {
  const text = maskMarkdownCode(markdown);
  const destinations = [];
  const references = new Map();
  for (const match of text.matchAll(/^\s{0,3}\[([^\]\n]+)\]:\s*(.+)$/gm)) {
    const destination = parseMarkdownDestination(match[2]);
    if (destination) references.set(normalizeReference(match[1]), destination);
  }

  const inlinePattern = /!?\[[^\]\n]*\]\(/g;
  for (const match of text.matchAll(inlinePattern)) {
    const opening = match.index + match[0].length - 1;
    const closing = findClosingParenthesis(text, opening);
    if (closing === -1) continue;
    const destination = parseMarkdownDestination(
      text.slice(opening + 1, closing),
    );
    if (destination) destinations.push(destination);
  }

  for (const match of text.matchAll(/!?\[([^\]\n]+)\]\[([^\]\n]*)\]/g)) {
    const reference = normalizeReference(match[2] || match[1]);
    const destination = references.get(reference);
    if (destination) destinations.push(destination);
  }
  for (const match of text.matchAll(/!?\[([^\]\n]+)\]/g)) {
    const following = text[match.index + match[0].length];
    if (following === "(" || following === "[") continue;
    const destination = references.get(normalizeReference(match[1]));
    if (destination) destinations.push(destination);
  }
  return destinations;
}

function maskMarkdownCode(markdown) {
  return markdown
    .replace(
      /(^|\n)(\s*)(`{3,}|~{3,})[^\n]*\n[\s\S]*?\n\2\3(?=\n|$)/g,
      (block) => block.replace(/[^\n]/g, " "),
    )
    .replace(/`+[^`\n]*`+/g, (code) => " ".repeat(code.length));
}

function findClosingParenthesis(text, opening) {
  let depth = 1;
  let angleDestination = false;
  let escaped = false;
  for (let index = opening + 1; index < text.length; index += 1) {
    const character = text[index];
    if (escaped) {
      escaped = false;
      continue;
    }
    if (character === "\\") {
      escaped = true;
      continue;
    }
    if (character === "<" && depth === 1) angleDestination = true;
    if (character === ">" && angleDestination) angleDestination = false;
    if (angleDestination) continue;
    if (character === "(") depth += 1;
    if (character === ")") {
      depth -= 1;
      if (depth === 0) return index;
    }
  }
  return -1;
}

function parseMarkdownDestination(rawDestination) {
  const value = rawDestination.trim();
  if (!value) return undefined;
  if (value.startsWith("<")) {
    const end = value.indexOf(">");
    return end === -1 ? undefined : value.slice(1, end);
  }
  let escaped = false;
  for (let index = 0; index < value.length; index += 1) {
    const character = value[index];
    if (escaped) {
      escaped = false;
      continue;
    }
    if (character === "\\") {
      escaped = true;
      continue;
    }
    if (/\s/.test(character)) {
      return unescapeMarkdownDestination(value.slice(0, index));
    }
  }
  return unescapeMarkdownDestination(value);
}

function unescapeMarkdownDestination(value) {
  return value.replace(/\\([\\()` ])/g, "$1");
}

function normalizeReference(value) {
  return value.trim().replace(/\s+/g, " ").toLowerCase();
}

function localDestinationPath(destination) {
  return parseLocalDestination(destination)?.path;
}

function parseLocalDestination(destination) {
  if (
    /^[a-z][a-z0-9+.-]*:/i.test(destination) ||
    destination.startsWith("//")
  ) {
    return undefined;
  }
  const hashIndex = destination.indexOf("#");
  const queryIndex = destination.indexOf("?");
  const pathEnd = [hashIndex, queryIndex]
    .filter((index) => index >= 0)
    .reduce((minimum, index) => Math.min(minimum, index), destination.length);
  try {
    return {
      path: decodeURIComponent(destination.slice(0, pathEnd)),
      fragment:
        hashIndex === -1
          ? undefined
          : decodeURIComponent(destination.slice(hashIndex + 1)).toLowerCase(),
    };
  } catch {
    return { path: "\0", fragment: undefined };
  }
}

function markdownAnchors(markdown) {
  const anchors = new Set();
  const duplicates = new Map();
  const text = maskMarkdownCode(markdown);
  for (const match of text.matchAll(/^\s{0,3}#{1,6}\s+(.+?)\s*#*\s*$/gm)) {
    const base = githubHeadingSlug(match[1]);
    if (!base) continue;
    const count = duplicates.get(base) ?? 0;
    duplicates.set(base, count + 1);
    anchors.add(count === 0 ? base : `${base}-${count}`);
  }
  for (const match of text.matchAll(
    /<(?:a|[a-z][a-z0-9-]*)\b[^>]*(?:id|name)=["']([^"']+)["'][^>]*>/gi,
  )) {
    anchors.add(match[1].toLowerCase());
  }
  return anchors;
}

function githubHeadingSlug(heading) {
  return heading
    .replace(/<[^>]+>/g, "")
    .replace(/!\[([^\]]*)\]\([^)]*\)/g, "$1")
    .replace(/\[([^\]]+)\]\([^)]*\)/g, "$1")
    .replace(/[`*_~]/g, "")
    .toLowerCase()
    .trim()
    .replace(/[^\p{Letter}\p{Number}\s_-]/gu, "")
    .replace(/\s+/g, "-");
}

function toRepositoryPath(path) {
  return path.split(sep).join("/");
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(resolve(process.argv[1])).href
) {
  const errors = await checkDocumentation(resolve(process.argv[2] ?? "."));
  if (errors.length > 0) {
    process.stderr.write(`${errors.join("\n")}\n`);
    process.exitCode = 1;
  }
}
