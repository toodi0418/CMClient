import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { once } from "node:events";
import { mkdir, rm, stat, writeFile } from "node:fs/promises";
import {
  createServer as createHttpServer,
  request as httpRequest,
} from "node:http";
import { createServer as createTcpServer } from "node:net";
import { dirname, isAbsolute, resolve } from "node:path";
import { spawn } from "node:child_process";
import { pathToFileURL } from "node:url";

const CAPABILITY_HEADER = "x-cmclient-gateway-capability";
const MAX_FRAME_BYTES = 16 * 1024;
const READY_TIMEOUT_MS = 15_000;

export async function runGatewayBootstrapLab({ entrypoint, dataRoot }) {
  const resolvedEntrypoint = resolve(entrypoint);
  const resolvedDataRoot = resolve(dataRoot);
  assert.equal(isAbsolute(resolvedEntrypoint), true);
  assert.equal(isAbsolute(resolvedDataRoot), true);
  assert.equal((await stat(resolvedEntrypoint)).isFile(), true);
  await mkdir(resolvedDataRoot, { recursive: true });

  const startupNonce = randomBytes(16).toString("hex");
  const capability = randomBytes(32).toString("hex");
  const child = spawn(process.execPath, [resolvedEntrypoint], {
    cwd: dirname(resolvedEntrypoint),
    env: childEnvironment(resolvedDataRoot),
    stdio: ["pipe", "pipe", "pipe"],
    windowsHide: true,
  });
  let stderr = "";
  child.stderr.setEncoding("utf8");
  child.stderr.on("data", (chunk) => {
    if (stderr.length < 64 * 1024) stderr += chunk;
  });
  const exited = once(child, "exit");

  try {
    child.stdin.write(
      encodeFrame({
        schemaVersion: 1,
        type: "gateway.bootstrap",
        startupNonce,
        capability,
      }),
    );
    let ready;
    try {
      ready = await readFrame(child.stdout, READY_TIMEOUT_MS);
    } catch (error) {
      const [code, signal] = await withTimeout(
        exited,
        5_000,
        "gateway startup failure",
      );
      assert.equal(stderr.includes(capability), false);
      assert.equal(stderr.includes(startupNonce), false);
      throw new Error(
        `${error.message}; exit=${String(code)} signal=${String(signal)} stderr=${stderr.trim()}`,
        { cause: error },
      );
    }
    assert.deepEqual(Object.keys(ready).sort(), [
      "host",
      "pid",
      "port",
      "schemaVersion",
      "startupNonce",
      "type",
    ]);
    assert.equal(ready.schemaVersion, 1);
    assert.equal(ready.type, "gateway.ready");
    assert.equal(ready.pid, child.pid);
    assert.equal(ready.startupNonce, startupNonce);
    assert.equal(ready.host, "127.0.0.1");
    assert.ok(
      Number.isInteger(ready.port) && ready.port > 0 && ready.port <= 65_535,
    );

    const direct = await gatewayRequest(ready.port);
    assert.equal(direct.statusCode, 403);
    const wrong = await gatewayRequest(ready.port, "f".repeat(64));
    assert.equal(wrong.statusCode, 403);
    const accepted = await gatewayRequest(ready.port, capability);
    assert.equal(accepted.statusCode, 200);
    assert.deepEqual(JSON.parse(accepted.body), { status: "ok" });
    assert.equal(accepted.rawHeaders.includes(capability), false);
    assert.equal(accepted.body.includes(capability), false);

    await assertPortOwned(ready.port);
    await assertAgentOverwrite(ready.port, capability);
    assert.equal(stderr.includes(capability), false);
    assert.equal(stderr.includes(startupNonce), false);

    child.stdin.write("CMCLIENT_SHUTDOWN\n");
    child.stdin.end();
    const [code, signal] = await withTimeout(
      exited,
      30_000,
      "gateway shutdown",
    );
    assert.equal(signal, null);
    assert.equal(code, 0, stderr);
    const faultCases = await runBootstrapFaultCases(
      resolvedEntrypoint,
      resolvedDataRoot,
    );
    return {
      schemaVersion: 1,
      status: "passed",
      cases: [
        "bounded-private-pipe-bootstrap",
        "atomic-loopback-port-zero-bind",
        "ready-pid-nonce-validation",
        "direct-and-wrong-capability-rejected",
        "agent-strip-and-overwrite",
        "capability-not-reflected-or-logged",
        "port-takeover-rejected",
        "graceful-helper-shutdown",
        ...faultCases,
      ],
    };
  } finally {
    if (child.exitCode === null && child.signalCode === null) child.kill();
    await rm(resolvedDataRoot, { recursive: true, force: true });
  }
}

async function runBootstrapFaultCases(entrypoint, dataRoot) {
  const cases = [
    {
      name: "oversized-bootstrap-rejected",
      expected: "GATEWAY_PRIVATE_FRAME_OVERSIZED",
      input: (() => {
        const prefix = Buffer.alloc(4);
        prefix.writeUInt32BE(MAX_FRAME_BYTES + 1);
        return prefix;
      })(),
    },
    {
      name: "malformed-bootstrap-rejected",
      expected: "GATEWAY_PRIVATE_FRAME_INVALID",
      input: Buffer.from([0, 0, 0, 1, 0xff]),
    },
    {
      name: "invalid-bootstrap-schema-rejected",
      expected: "GATEWAY_BOOTSTRAP_FRAME_INVALID",
      input: encodeFrame({ schemaVersion: 2, type: "gateway.bootstrap" }),
    },
    {
      name: "bootstrap-early-eof-rejected",
      expected: "GATEWAY_BOOTSTRAP_EARLY_EOF",
      input: null,
      end: true,
    },
    {
      name: "bootstrap-timeout-rejected",
      expected: "GATEWAY_BOOTSTRAP_TIMEOUT",
      input: null,
    },
  ];
  const passed = [];
  for (const fixture of cases) {
    const fixtureRoot = resolve(dataRoot, fixture.name);
    await mkdir(fixtureRoot, { recursive: true });
    const child = spawn(process.execPath, [entrypoint], {
      cwd: dirname(entrypoint),
      env: childEnvironment(fixtureRoot),
      stdio: ["pipe", "ignore", "pipe"],
      windowsHide: true,
    });
    let stderr = "";
    child.stderr.setEncoding("utf8");
    child.stderr.on("data", (chunk) => (stderr += chunk));
    if (fixture.input) child.stdin.write(fixture.input);
    if (fixture.end) child.stdin.end();
    const [code, signal] = await withTimeout(
      once(child, "exit"),
      fixture.name.includes("timeout") ? 10_000 : 5_000,
      fixture.name,
    );
    assert.equal(signal, null);
    assert.equal(code, 1, stderr);
    assert.ok(stderr.includes(fixture.expected), stderr);
    passed.push(fixture.name);
  }
  return passed;
}

export function rewriteAgentHeaders(headers, capability) {
  assert.match(capability, /^[0-9a-f]{64}$/);
  const rewritten = {};
  for (const [name, value] of Object.entries(headers)) {
    if (name.toLowerCase() !== CAPABILITY_HEADER) rewritten[name] = value;
  }
  rewritten[CAPABILITY_HEADER] = capability;
  return rewritten;
}

function childEnvironment(dataRoot) {
  const inherited = {};
  for (const name of ["PATH", "SystemRoot", "WINDIR", "ComSpec"]) {
    if (process.env[name]) inherited[name] = process.env[name];
  }
  return {
    ...inherited,
    CMCLIENT_SUPERVISED: "1",
    CMCLIENT_QUALIFICATION_MODE: "1",
    CMCLIENT_RUNTIME_ROOT: dataRoot,
    CMCLIENT_DB_PATH: resolve(dataRoot, "cmclient.db"),
    CMCLIENT_BACKUP_DIR: resolve(dataRoot, "backups"),
    CMCLIENT_BUILD_VERSION: "2.0.0-rc.1",
    CMCLIENT_BUILD_COMMIT: "a".repeat(40),
    CMCLIENT_BUILD_TREE: "b".repeat(40),
    CMCLIENT_BUILD_CHANNEL: "dev",
    CMCLIENT_RUNTIME_PROFILE: "native",
    CMCLIENT_PACKAGE_PROFILE: "workspace",
    CMCLIENT_TARGET_OS: "windows",
    CMCLIENT_TARGET_ARCHITECTURE: "x86_64",
  };
}

function encodeFrame(value) {
  const body = Buffer.from(JSON.stringify(value), "utf8");
  assert.ok(body.length > 0 && body.length <= MAX_FRAME_BYTES);
  const frame = Buffer.allocUnsafe(4 + body.length);
  frame.writeUInt32BE(body.length, 0);
  body.copy(frame, 4);
  return frame;
}

async function readFrame(stream, timeoutMs) {
  return await new Promise((resolveFrame, reject) => {
    let buffered = Buffer.alloc(0);
    let expected;
    const timer = setTimeout(
      () => finish(new Error("gateway ready frame timed out")),
      timeoutMs,
    );
    const onData = (chunk) => {
      buffered = Buffer.concat([buffered, chunk]);
      if (buffered.length >= 4 && expected === undefined) {
        expected = buffered.readUInt32BE(0);
        if (expected < 1 || expected > MAX_FRAME_BYTES) {
          finish(new Error("gateway ready frame size invalid"));
          return;
        }
      }
      if (expected !== undefined && buffered.length >= expected + 4) {
        if (buffered.length !== expected + 4) {
          finish(new Error("gateway ready frame has trailing bytes"));
          return;
        }
        try {
          finish(undefined, JSON.parse(buffered.subarray(4).toString("utf8")));
        } catch (error) {
          finish(error);
        }
      }
    };
    const onEnd = () => finish(new Error("gateway exited before ready"));
    const onError = (error) => finish(error);
    function finish(error, value) {
      clearTimeout(timer);
      stream.off("data", onData);
      stream.off("end", onEnd);
      stream.off("error", onError);
      if (error) reject(error);
      else resolveFrame(value);
    }
    stream.on("data", onData);
    stream.once("end", onEnd);
    stream.once("error", onError);
  });
}

async function gatewayRequest(port, capability) {
  return await new Promise((resolveRequest, reject) => {
    const request = httpRequest({
      host: "127.0.0.1",
      port,
      path: "/api/v1/system/health",
      method: "GET",
      headers: capability ? { [CAPABILITY_HEADER]: capability } : {},
    });
    request.once("response", (response) => {
      let body = "";
      response.setEncoding("utf8");
      response.on("data", (chunk) => (body += chunk));
      response.once("end", () =>
        resolveRequest({
          statusCode: response.statusCode,
          rawHeaders: response.rawHeaders,
          body,
        }),
      );
    });
    request.once("error", reject);
    request.end();
  });
}

async function assertPortOwned(port) {
  const server = createTcpServer();
  const error = once(server, "error");
  server.listen(port, "127.0.0.1");
  const [observed] = await withTimeout(error, 5_000, "port takeover rejection");
  assert.equal(observed.code, "EADDRINUSE");
  server.close();
}

async function assertAgentOverwrite(gatewayPort, capability) {
  const proxy = createHttpServer(async (request, response) => {
    const upstream = await gatewayRequest(
      gatewayPort,
      rewriteAgentHeaders(request.headers, capability)[CAPABILITY_HEADER],
    );
    response.writeHead(upstream.statusCode, {
      "content-type": "application/json",
    });
    response.end(upstream.body);
  });
  proxy.listen(0, "127.0.0.1");
  await once(proxy, "listening");
  const address = proxy.address();
  assert.ok(address && typeof address !== "string");
  try {
    const response = await gatewayRequest(address.port, "e".repeat(64));
    assert.equal(response.statusCode, 200);
  } finally {
    proxy.close();
    await once(proxy, "close");
  }
}

function withTimeout(promise, timeoutMs, label) {
  return Promise.race([
    promise,
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error(`${label} timed out`)), timeoutMs),
    ),
  ]);
}

async function main(argv = process.argv.slice(2)) {
  const entrypointIndex = argv.indexOf("--entrypoint");
  const dataRootIndex = argv.indexOf("--data-root");
  if (entrypointIndex < 0 || dataRootIndex < 0) {
    throw new Error(
      "usage: --entrypoint <absolute-main.js> --data-root <campaign-path>",
    );
  }
  const result = await runGatewayBootstrapLab({
    entrypoint: argv[entrypointIndex + 1],
    dataRoot: argv[dataRootIndex + 1],
  });
  await writeFile(
    `${resolve(argv[dataRootIndex + 1])}.evidence.json`,
    `${JSON.stringify(result, null, 2)}\n`,
    { mode: 0o600 },
  );
  process.stdout.write(`${JSON.stringify(result)}\n`);
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(resolve(process.argv[1])).href
) {
  await main();
}
