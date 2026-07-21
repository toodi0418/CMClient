import { readFile } from "node:fs/promises";
import { createServer } from "node:https";
import { isAbsolute, relative, resolve } from "node:path";
import { pathToFileURL } from "node:url";

export const LAB_CASES = Object.freeze([
  "valid",
  "bit-flip",
  "wrong-target",
  "downgrade",
  "oversize",
  "timeout",
]);

const DEFAULT_PORT = 9443;
const DEFAULT_TIMEOUT_DELAY_MS = 10_000;
const DEFAULT_OVERSIZE_BYTES = 512 * 1024 * 1024;
const OVERSIZE_CHUNK_BYTES = 64 * 1024;

function integer(value, fallback, minimum, maximum, name) {
  const parsed = value === undefined ? fallback : Number(value);
  if (!Number.isInteger(parsed) || parsed < minimum || parsed > maximum) {
    throw new Error(`P13_UPDATER_LAB_${name}_INVALID`);
  }
  return parsed;
}

export function isCampaignPath(campaignRoot, candidate) {
  const root = resolve(campaignRoot);
  const path = resolve(candidate);
  const child = relative(root, path);
  return child !== "" && !child.startsWith("..") && !isAbsolute(child);
}

function requiredCampaignFile(environment, variable, campaignRoot) {
  const value = environment[variable];
  if (!value || !isCampaignPath(campaignRoot, value)) {
    throw new Error(`P13_UPDATER_LAB_${variable}_INVALID`);
  }
  return resolve(value);
}

export function validateLabEnvironment(environment = process.env) {
  const campaignRoot = environment.CMCLIENT_CAMPAIGN_ROOT;
  if (!campaignRoot || !isAbsolute(campaignRoot)) {
    throw new Error("P13_UPDATER_LAB_CAMPAIGN_ROOT_INVALID");
  }
  const target = environment.CMCLIENT_P13_UPDATE_TARGET ?? "windows-x86_64";
  if (!/^(windows|darwin|linux)-(x86_64|aarch64)$/.test(target)) {
    throw new Error("P13_UPDATER_LAB_TARGET_INVALID");
  }
  const version = environment.CMCLIENT_P13_UPDATE_VERSION ?? "0.2.0";
  if (!/^\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?$/.test(version)) {
    throw new Error("P13_UPDATER_LAB_VERSION_INVALID");
  }
  return {
    campaignRoot: resolve(campaignRoot),
    certificatePath: requiredCampaignFile(
      environment,
      "CMCLIENT_P13_TLS_CERT_FILE",
      campaignRoot,
    ),
    keyPath: requiredCampaignFile(
      environment,
      "CMCLIENT_P13_TLS_KEY_FILE",
      campaignRoot,
    ),
    payloadPath: requiredCampaignFile(
      environment,
      "CMCLIENT_P13_UPDATE_PAYLOAD",
      campaignRoot,
    ),
    signaturePath: requiredCampaignFile(
      environment,
      "CMCLIENT_P13_UPDATE_SIGNATURE",
      campaignRoot,
    ),
    host: "127.0.0.1",
    port: integer(
      environment.CMCLIENT_P13_LAB_PORT,
      DEFAULT_PORT,
      1024,
      65_535,
      "PORT",
    ),
    target,
    version,
    timeoutDelayMs: integer(
      environment.CMCLIENT_P13_TIMEOUT_DELAY_MS,
      DEFAULT_TIMEOUT_DELAY_MS,
      100,
      60_000,
      "TIMEOUT_DELAY",
    ),
    oversizeBytes: integer(
      environment.CMCLIENT_P13_OVERSIZE_BYTES,
      DEFAULT_OVERSIZE_BYTES,
      1024 * 1024,
      4 * 1024 * 1024 * 1024,
      "OVERSIZE_BYTES",
    ),
  };
}

export function buildManifest({
  caseName,
  host = "127.0.0.1",
  port = DEFAULT_PORT,
  target = "windows-x86_64",
  version = "0.2.0",
  signature = "fixture-signature",
}) {
  if (!LAB_CASES.includes(caseName)) {
    throw new Error("P13_UPDATER_LAB_CASE_INVALID");
  }
  const platform =
    caseName === "wrong-target" ? `unsupported-${target}` : target;
  const advertisedVersion = caseName === "downgrade" ? "0.0.1" : version;
  return {
    version: advertisedVersion,
    notes: `p13-fixture-${caseName}`,
    pub_date: "2026-07-21T00:00:00Z",
    platforms: {
      [platform]: {
        signature,
        url: `https://${host}:${port}/payload/${caseName}`,
      },
    },
  };
}

function sendJson(response, status, value) {
  const body = Buffer.from(JSON.stringify(value));
  response.writeHead(status, {
    "content-length": body.length,
    "content-type": "application/json",
    "cache-control": "no-store",
  });
  response.end(body);
}

export function streamOversize(request, response, totalBytes) {
  const chunk = Buffer.alloc(OVERSIZE_CHUNK_BYTES, 0xa5);
  let remaining = totalBytes;
  let stopped = false;
  const stop = () => {
    stopped = true;
  };
  request.once("aborted", stop);
  response.once("close", stop);
  response.once("error", stop);
  response.writeHead(200, {
    "content-length": totalBytes,
    "content-type": "application/octet-stream",
    "cache-control": "no-store",
  });
  const write = () => {
    while (!stopped && remaining > 0) {
      const size = Math.min(remaining, chunk.length);
      remaining -= size;
      if (!response.write(chunk.subarray(0, size))) {
        response.once("drain", write);
        return;
      }
    }
    if (!stopped) {
      response.end();
    }
  };
  write();
}

export async function startLab(configuration) {
  const [certificate, key, payload, signatureText] = await Promise.all([
    readFile(configuration.certificatePath),
    readFile(configuration.keyPath),
    readFile(configuration.payloadPath),
    readFile(configuration.signaturePath, "utf8"),
  ]);
  const signature = signatureText.trim();
  if (!signature) {
    throw new Error("P13_UPDATER_LAB_SIGNATURE_EMPTY");
  }

  const server = createServer(
    { cert: certificate, key },
    (request, response) => {
      const requestUrl = new URL(
        request.url ?? "/",
        `https://${configuration.host}:${configuration.port}`,
      );
      if (requestUrl.pathname === "/health") {
        response.writeHead(204, { "cache-control": "no-store" });
        response.end();
        return;
      }
      const manifestMatch = /^\/manifest\/([^/]+)$/.exec(requestUrl.pathname);
      if (manifestMatch) {
        const caseName = manifestMatch[1];
        if (!LAB_CASES.includes(caseName)) {
          sendJson(response, 404, { error: "unknown-case" });
          return;
        }
        const send = () =>
          sendJson(
            response,
            200,
            buildManifest({ caseName, signature, ...configuration }),
          );
        if (caseName === "timeout") {
          setTimeout(send, configuration.timeoutDelayMs);
        } else {
          send();
        }
        return;
      }
      const payloadMatch = /^\/payload\/([^/]+)$/.exec(requestUrl.pathname);
      if (!payloadMatch || !LAB_CASES.includes(payloadMatch[1])) {
        sendJson(response, 404, { error: "not-found" });
        return;
      }
      const caseName = payloadMatch[1];
      if (caseName === "oversize") {
        streamOversize(request, response, configuration.oversizeBytes);
        return;
      }
      const body = Buffer.from(payload);
      if (caseName === "bit-flip" && body.length > 0) {
        body[Math.floor(body.length / 2)] ^= 0x01;
      }
      response.writeHead(200, {
        "content-length": body.length,
        "content-type": "application/octet-stream",
        "cache-control": "no-store",
      });
      response.end(body);
    },
  );

  await new Promise((resolveListen, rejectListen) => {
    server.once("error", rejectListen);
    server.listen(configuration.port, configuration.host, resolveListen);
  });
  return server;
}

async function main() {
  const configuration = validateLabEnvironment();
  const server = await startLab(configuration);
  process.stdout.write(
    `P13_UPDATER_LAB_READY host=${configuration.host} port=${configuration.port}\n`,
  );
  const close = () => server.close(() => process.exit(0));
  process.once("SIGINT", close);
  process.once("SIGTERM", close);
}

const invokedPath = process.argv[1];
if (
  invokedPath &&
  import.meta.url === pathToFileURL(resolve(invokedPath)).href
) {
  await main();
}
