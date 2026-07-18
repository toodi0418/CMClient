import assert from "node:assert/strict";
import { createServer } from "node:http";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  createWebServer,
  gatewayEnvironment,
  parseWebUpstream,
  resolveStaticPath,
} from "./container-runtime.mjs";

test("container web serves the SPA and proxies only API requests", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "cmclient-container-web-"));
  const upstream = createServer((request, response) => {
    assert.equal(request.url, "/api/v1/system/capabilities");
    assert.equal(request.headers["x-correlation-id"], "smoke-42");
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({ ok: true }));
  });
  await mkdir(join(root, "assets"));
  await writeFile(join(root, "index.html"), "<main>CMClient 2.0</main>");
  await writeFile(join(root, "assets", "app.js"), "export {};");
  await listen(upstream);
  const upstreamAddress = upstream.address();
  assert.ok(upstreamAddress && typeof upstreamAddress !== "string");
  const web = createWebServer({
    webRoot: root,
    upstream: `http://127.0.0.1:${upstreamAddress.port}`,
  });
  await listen(web);
  const webAddress = web.address();
  assert.ok(webAddress && typeof webAddress !== "string");
  const baseUrl = `http://127.0.0.1:${webAddress.port}`;
  t.after(async () => {
    await close(web);
    await close(upstream);
    await rm(root, { force: true, recursive: true });
  });

  const [shell, api] = await Promise.all([
    fetch(`${baseUrl}/nodes`),
    fetch(`${baseUrl}/api/v1/system/capabilities`, {
      headers: { "x-correlation-id": "smoke-42" },
    }),
  ]);

  assert.equal(shell.status, 200);
  assert.equal(await shell.text(), "<main>CMClient 2.0</main>");
  assert.equal(api.status, 200);
  assert.deepEqual(await api.json(), { ok: true });
});

test("container web rejects unsafe static paths and unavailable upstreams", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "cmclient-container-web-"));
  await writeFile(join(root, "index.html"), "<main>CMClient 2.0</main>");
  const web = createWebServer({
    webRoot: root,
    upstream: "http://127.0.0.1:1",
  });
  await listen(web);
  const webAddress = web.address();
  assert.ok(webAddress && typeof webAddress !== "string");
  const baseUrl = `http://127.0.0.1:${webAddress.port}`;
  t.after(async () => {
    await close(web);
    await rm(root, { force: true, recursive: true });
  });

  assert.equal(resolveStaticPath(root, "/%2e%2e/private"), undefined);
  const missing = await fetch(`${baseUrl}/private.txt`);
  assert.equal(missing.status, 404);
  const unavailable = await fetch(`${baseUrl}/api/v1/system/health`);
  assert.equal(unavailable.status, 502);
  assert.deepEqual(await unavailable.json(), {
    code: "GATEWAY_PROXY_UNAVAILABLE",
  });
});

test("container runtime forces Docker mode and validates its internal upstream", () => {
  assert.deepEqual(
    gatewayEnvironment({
      CMCLIENT_DEPLOYMENT_MODE: "desktop",
      CMCLIENT_GATEWAY_HOST: "127.0.0.1",
      CMCLIENT_GATEWAY_PORT: "9191",
    }),
    {
      CMCLIENT_DEPLOYMENT_MODE: "docker",
      CMCLIENT_GATEWAY_HOST: "0.0.0.0",
      CMCLIENT_GATEWAY_PORT: "9191",
    },
  );
  assert.throws(
    () => parseWebUpstream("https://gateway:8081"),
    /DOCKER_WEB_UPSTREAM_INVALID/,
  );
  assert.throws(
    () => parseWebUpstream("http://token@gateway:8081"),
    /DOCKER_WEB_UPSTREAM_INVALID/,
  );
});

async function listen(server) {
  await new Promise((resolvePromise) => {
    server.listen({ host: "127.0.0.1", port: 0 }, resolvePromise);
  });
}

async function close(server) {
  await new Promise((resolvePromise, reject) => {
    server.close((error) => (error ? reject(error) : resolvePromise()));
  });
}
