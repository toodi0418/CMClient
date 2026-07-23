import { createReadStream } from "node:fs";
import { stat } from "node:fs/promises";
import { createServer, request as createRequest } from "node:http";
import { extname, isAbsolute, relative, resolve } from "node:path";

const DEFAULT_WEB_UPSTREAM = "http://gateway:8081";
const DEFAULT_INGRESS_UPSTREAM = "http://web:8080";
const STATIC_RESPONSE_HEADERS = {
  "referrer-policy": "same-origin",
  "x-content-type-options": "nosniff",
};
const REQUEST_HEADER_NAMES = [
  "accept",
  "content-length",
  "content-type",
  "idempotency-key",
  "last-event-id",
  "x-correlation-id",
  "x-trace-id",
];
const RESPONSE_HEADER_NAMES = [
  "cache-control",
  "content-length",
  "content-type",
  "etag",
  "last-modified",
  "x-correlation-id",
  "x-trace-id",
];

export function gatewayEnvironment(environment = process.env) {
  const safeEnvironment = { ...environment };
  for (const name of [
    "CMCLIENT_DATA_DIR",
    "CMCLIENT_CONFIG_DIR",
    "CMCLIENT_CACHE_DIR",
    "CMCLIENT_LOG_DIR",
    "CMCLIENT_CALLMESH_API_KEY",
    "CMCLIENT_APRS_PASSCODE",
    "CMCLIENT_MANAGEMENT_ADMIN_TOKEN",
    "CMCLIENT_CONTROL_TOKEN",
    "CMCLIENT_PLAINTEXT_SECRET_FILE",
    "CMCLIENT_SYSTEMD_SECRET_STORE",
    "CREDENTIALS_DIRECTORY",
  ]) {
    delete safeEnvironment[name];
  }
  return {
    ...safeEnvironment,
    CMCLIENT_RUNTIME_PROFILE: "docker",
    CMCLIENT_PACKAGE_PROFILE: "oci",
    HOME: "/home/cmclient",
    CMCLIENT_RUNTIME_ROOT: "/home/cmclient/.cmclient",
    CMCLIENT_DB_PATH: "/home/cmclient/.cmclient/cmclient.db",
    CMCLIENT_BACKUP_DIR: "/home/cmclient/.cmclient/backups",
    CMCLIENT_GATEWAY_HOST: "0.0.0.0",
    CMCLIENT_GATEWAY_PORT: environment.CMCLIENT_GATEWAY_PORT?.trim() || "8081",
  };
}

export function parseWebUpstream(value = DEFAULT_WEB_UPSTREAM) {
  return parseInternalUpstream(value, "DOCKER_WEB_UPSTREAM_INVALID");
}

export function parseIngressUpstream(value = DEFAULT_INGRESS_UPSTREAM) {
  return parseInternalUpstream(value, "DOCKER_INGRESS_UPSTREAM_INVALID");
}

function parseInternalUpstream(value, errorCode) {
  let upstream;
  try {
    upstream = new URL(value);
  } catch {
    throw new Error(errorCode);
  }
  if (
    upstream.protocol !== "http:" ||
    !upstream.hostname ||
    upstream.username ||
    upstream.password ||
    upstream.hash ||
    upstream.pathname !== "/" ||
    upstream.search
  ) {
    throw new Error(errorCode);
  }
  return upstream;
}

export function resolveStaticPath(webRoot, pathname) {
  let decodedPath;
  try {
    decodedPath = decodeURIComponent(pathname);
  } catch {
    return undefined;
  }
  if (
    decodedPath.includes("\0") ||
    decodedPath.split("/").some((segment) => segment === "..")
  ) {
    return undefined;
  }

  const requestedPath = decodedPath.replace(/^\/+/, "");
  const fileName =
    requestedPath === "" || extname(requestedPath) === ""
      ? "index.html"
      : requestedPath;
  const root = resolve(webRoot);
  const candidate = resolve(root, fileName);
  const candidateRelativePath = relative(root, candidate);
  if (
    candidateRelativePath === "" ||
    candidateRelativePath.startsWith("..") ||
    isAbsolute(candidateRelativePath)
  ) {
    return undefined;
  }
  return candidate;
}

export function createWebServer({ webRoot, upstream }) {
  const parsedUpstream = parseWebUpstream(upstream);
  return createServer((incoming, outgoing) => {
    const requestUrl = new URL(incoming.url || "/", "http://cmclient-web");
    if (requestUrl.pathname === "/healthz") {
      writeResponse(outgoing, 200, "ok\n", "text/plain; charset=utf-8");
      return;
    }
    if (
      requestUrl.pathname === "/api" ||
      requestUrl.pathname.startsWith("/api/")
    ) {
      proxyHttpRequest(incoming, outgoing, parsedUpstream, requestUrl, {
        unavailableCode: "GATEWAY_PROXY_UNAVAILABLE",
      });
      return;
    }
    void serveStaticFile(incoming, outgoing, webRoot, requestUrl.pathname);
  });
}

export async function startWebServer({
  webRoot,
  upstream,
  host = "0.0.0.0",
  port = 8080,
}) {
  if (!Number.isInteger(port) || port < 0 || port > 65_535) {
    throw new Error("DOCKER_WEB_LISTEN_CONFIGURATION_INVALID");
  }
  const server = createWebServer({ webRoot, upstream });
  await listen(server, host, port);
  return server;
}

export function createIngressServer({
  upstream = DEFAULT_INGRESS_UPSTREAM,
} = {}) {
  const parsedUpstream = parseIngressUpstream(upstream);
  return createServer((incoming, outgoing) => {
    const requestUrl = new URL(incoming.url || "/", "http://cmclient-ingress");
    proxyHttpRequest(incoming, outgoing, parsedUpstream, requestUrl, {
      unavailableCode: "INGRESS_PROXY_UNAVAILABLE",
    });
  });
}

export async function startIngressServer({
  host = "0.0.0.0",
  port = 8080,
} = {}) {
  if (!Number.isInteger(port) || port < 0 || port > 65_535) {
    throw new Error("DOCKER_INGRESS_LISTEN_CONFIGURATION_INVALID");
  }
  const server = createIngressServer();
  await listen(server, host, port);
  return server;
}

function proxyHttpRequest(
  incoming,
  outgoing,
  upstream,
  requestUrl,
  { unavailableCode },
) {
  const upstreamUrl = new URL(
    `${requestUrl.pathname}${requestUrl.search}`,
    upstream,
  );
  const requestHeaders = selectHeaders(incoming.headers, REQUEST_HEADER_NAMES);
  let activeUpstreamResponse;
  const upstreamRequest = createRequest(
    upstreamUrl,
    {
      headers: requestHeaders,
      method: incoming.method,
    },
    (upstreamResponse) => {
      activeUpstreamResponse = upstreamResponse;
      upstreamResponse.once("aborted", rejectRequest);
      upstreamResponse.once("error", rejectRequest);
      upstreamResponse.once("close", () => {
        if (!upstreamResponse.complete) {
          rejectRequest();
        }
      });
      const responseHeaders = {
        ...STATIC_RESPONSE_HEADERS,
        ...selectHeaders(upstreamResponse.headers, RESPONSE_HEADER_NAMES),
      };
      outgoing.writeHead(upstreamResponse.statusCode || 502, responseHeaders);
      upstreamResponse.pipe(outgoing);
    },
  );
  const destroyUpstream = () => {
    activeUpstreamResponse?.destroy();
    upstreamRequest.destroy();
  };
  const rejectRequest = () => {
    destroyUpstream();
    if (incoming.aborted || outgoing.destroyed) {
      return;
    }
    if (!outgoing.headersSent) {
      writeJson(outgoing, 502, { code: unavailableCode });
    } else {
      outgoing.destroy();
    }
  };
  upstreamRequest.once("error", rejectRequest);
  incoming.once("aborted", destroyUpstream);
  outgoing.once("close", () => {
    if (!outgoing.writableFinished) {
      destroyUpstream();
    }
  });
  incoming.pipe(upstreamRequest);
}

async function listen(server, host, port) {
  await new Promise((resolvePromise, reject) => {
    server.once("error", reject);
    server.listen({ host, port }, () => {
      server.off("error", reject);
      resolvePromise();
    });
  });
}

async function serveStaticFile(incoming, outgoing, webRoot, pathname) {
  if (incoming.method !== "GET" && incoming.method !== "HEAD") {
    writeJson(outgoing, 405, { code: "WEB_METHOD_NOT_ALLOWED" });
    return;
  }
  const filePath = resolveStaticPath(webRoot, pathname);
  if (!filePath) {
    writeJson(outgoing, 404, { code: "WEB_ASSET_NOT_FOUND" });
    return;
  }
  try {
    const file = await stat(filePath);
    if (!file.isFile()) {
      writeJson(outgoing, 404, { code: "WEB_ASSET_NOT_FOUND" });
      return;
    }
    const isIndex = filePath.endsWith("/index.html");
    outgoing.writeHead(200, {
      ...STATIC_RESPONSE_HEADERS,
      "cache-control": isIndex
        ? "no-cache"
        : "public, max-age=31536000, immutable",
      "content-length": file.size,
      "content-type": contentType(filePath),
    });
    if (incoming.method === "HEAD") {
      outgoing.end();
      return;
    }
    createReadStream(filePath).pipe(outgoing);
  } catch {
    writeJson(outgoing, 404, { code: "WEB_ASSET_NOT_FOUND" });
  }
}

function selectHeaders(headers, names) {
  const selected = {};
  for (const name of names) {
    const value = headers[name];
    if (typeof value === "string") {
      selected[name] = value;
    }
  }
  return selected;
}

function contentType(filePath) {
  switch (extname(filePath)) {
    case ".css":
      return "text/css; charset=utf-8";
    case ".html":
      return "text/html; charset=utf-8";
    case ".js":
      return "text/javascript; charset=utf-8";
    case ".json":
      return "application/json; charset=utf-8";
    case ".png":
      return "image/png";
    case ".svg":
      return "image/svg+xml";
    default:
      return "application/octet-stream";
  }
}

function writeJson(response, statusCode, body) {
  writeResponse(
    response,
    statusCode,
    `${JSON.stringify(body)}\n`,
    "application/json",
  );
}

function writeResponse(response, statusCode, body, contentType) {
  response.writeHead(statusCode, {
    ...STATIC_RESPONSE_HEADERS,
    "content-length": Buffer.byteLength(body),
    "content-type": contentType,
  });
  response.end(body);
}
