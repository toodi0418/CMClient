import assert from "node:assert/strict";
import { createServer } from "node:http";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { clearTimeout, setTimeout } from "node:timers";

import {
  createIngressServer,
  createWebServer,
  gatewayEnvironment,
  parseIngressUpstream,
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

test("container ingress transparently proxies only to its fixed Web upstream", async (t) => {
  const web = createServer((request, response) => {
    assert.equal(request.method, "POST");
    assert.equal(request.url, "/api/v1/jobs?limit=1");
    assert.equal(request.headers["x-correlation-id"], "ingress-42");
    let body = "";
    request.setEncoding("utf8");
    request.on("data", (chunk) => {
      body += chunk;
    });
    request.on("end", () => {
      assert.equal(body, '{"command":"scan"}');
      response.writeHead(202, { "content-type": "application/json" });
      response.end('{"accepted":true}');
    });
  });
  await listen(web);
  const webAddress = web.address();
  assert.ok(webAddress && typeof webAddress !== "string");
  const ingress = createIngressServer({
    upstream: `http://127.0.0.1:${webAddress.port}`,
  });
  await listen(ingress);
  const ingressAddress = ingress.address();
  assert.ok(ingressAddress && typeof ingressAddress !== "string");
  t.after(async () => {
    await close(ingress);
    await close(web);
  });

  const response = await fetch(
    `http://127.0.0.1:${ingressAddress.port}/api/v1/jobs?limit=1`,
    {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-correlation-id": "ingress-42",
      },
      body: '{"command":"scan"}',
    },
  );
  assert.equal(response.status, 202);
  assert.deepEqual(await response.json(), { accepted: true });
});

test("container ingress and Web release the upstream SSE stream when the client disconnects", async (t) => {
  let activeConnections = 0;
  let resolveConnected;
  let resolveDisconnected;
  const connected = new Promise((resolvePromise) => {
    resolveConnected = resolvePromise;
  });
  const disconnected = new Promise((resolvePromise) => {
    resolveDisconnected = resolvePromise;
  });
  const gateway = createServer((_request, response) => {
    activeConnections += 1;
    resolveConnected();
    response.once("close", () => {
      activeConnections -= 1;
      resolveDisconnected();
    });
    response.writeHead(200, { "content-type": "text/event-stream" });
    response.write("data: ready\n\n");
  });
  await listen(gateway);
  const gatewayAddress = gateway.address();
  assert.ok(gatewayAddress && typeof gatewayAddress !== "string");
  const web = createWebServer({
    upstream: `http://127.0.0.1:${gatewayAddress.port}`,
    webRoot: ".",
  });
  await listen(web);
  const webAddress = web.address();
  assert.ok(webAddress && typeof webAddress !== "string");
  const ingress = createIngressServer({
    upstream: `http://127.0.0.1:${webAddress.port}`,
  });
  await listen(ingress);
  const ingressAddress = ingress.address();
  assert.ok(ingressAddress && typeof ingressAddress !== "string");
  t.after(async () => {
    ingress.closeAllConnections();
    web.closeAllConnections();
    gateway.closeAllConnections();
    await Promise.all([close(ingress), close(web), close(gateway)]);
  });

  const controller = new globalThis.AbortController();
  const response = await fetch(
    `http://127.0.0.1:${ingressAddress.port}/api/v1/events`,
    { signal: controller.signal },
  );
  assert.ok(response.body);
  const reader = response.body.getReader();
  await reader.read();
  await within(connected, "upstream SSE connection did not open");
  assert.equal(activeConnections, 1);

  controller.abort();
  await reader.closed.catch(() => undefined);
  await within(disconnected, "upstream SSE connection did not close");
  assert.equal(activeConnections, 0);
});

test("container proxy closes the downstream stream when the upstream response aborts", async (t) => {
  let abortUpstream;
  const upstream = createServer((_request, response) => {
    response.writeHead(200, { "content-type": "text/event-stream" });
    response.write("data: ready\n\n");
    abortUpstream = () => response.socket?.destroy();
  });
  await listen(upstream);
  const upstreamAddress = upstream.address();
  assert.ok(upstreamAddress && typeof upstreamAddress !== "string");
  const ingress = createIngressServer({
    upstream: `http://127.0.0.1:${upstreamAddress.port}`,
  });
  await listen(ingress);
  const ingressAddress = ingress.address();
  assert.ok(ingressAddress && typeof ingressAddress !== "string");
  t.after(async () => {
    ingress.closeAllConnections();
    upstream.closeAllConnections();
    await Promise.all([close(ingress), close(upstream)]);
  });

  const response = await fetch(
    `http://127.0.0.1:${ingressAddress.port}/api/v1/events`,
  );
  assert.ok(response.body);
  const reader = response.body.getReader();
  await reader.read();
  assert.ok(abortUpstream);
  abortUpstream();
  await within(
    reader.closed.catch(() => undefined),
    "downstream SSE connection remained open after the upstream aborted",
  );
});

test("container ingress rejects alternate targets and fails closed when Web is unavailable", async (t) => {
  assert.throws(
    () => parseIngressUpstream("https://web:8080"),
    /DOCKER_INGRESS_UPSTREAM_INVALID/,
  );
  assert.throws(
    () => parseIngressUpstream("http://gateway:8081/api"),
    /DOCKER_INGRESS_UPSTREAM_INVALID/,
  );
  const ingress = createIngressServer({ upstream: "http://127.0.0.1:1" });
  await listen(ingress);
  const address = ingress.address();
  assert.ok(address && typeof address !== "string");
  t.after(() => close(ingress));

  const response = await fetch(`http://127.0.0.1:${address.port}/healthz`);
  assert.equal(response.status, 502);
  assert.deepEqual(await response.json(), {
    code: "INGRESS_PROXY_UNAVAILABLE",
  });
});

test("container runtime forces Docker mode and validates its internal upstream", () => {
  assert.deepEqual(
    gatewayEnvironment({
      CMCLIENT_RUNTIME_PROFILE: "native",
      CMCLIENT_GATEWAY_HOST: "127.0.0.1",
      CMCLIENT_GATEWAY_PORT: "9191",
      CMCLIENT_DATA_DIR: "/legacy",
      CMCLIENT_CALLMESH_API_KEY: "must-not-reach-child",
    }),
    {
      CMCLIENT_RUNTIME_PROFILE: "docker",
      CMCLIENT_PACKAGE_PROFILE: "oci",
      HOME: "/home/cmclient",
      CMCLIENT_RUNTIME_ROOT: "/home/cmclient/.cmclient",
      CMCLIENT_DB_PATH: "/home/cmclient/.cmclient/cmclient.db",
      CMCLIENT_BACKUP_DIR: "/home/cmclient/.cmclient/backups",
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

async function within(promise, message) {
  let timeout;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timeout = setTimeout(() => reject(new Error(message)), 2_000);
        timeout.unref();
      }),
    ]);
  } finally {
    clearTimeout(timeout);
  }
}
