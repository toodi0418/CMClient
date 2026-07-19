import process from "node:process";
import { spawn } from "node:child_process";

import {
  gatewayEnvironment,
  startIngressServer,
  startWebServer,
} from "./container-runtime.mjs";

const command = process.argv[2] || "gateway";

if (command === "gateway") {
  await runGateway();
} else if (command === "web") {
  await runWeb();
} else if (command === "ingress") {
  await runIngress();
} else {
  process.stderr.write("DOCKER_ENTRYPOINT_COMMAND_INVALID\n");
  process.exitCode = 64;
}

async function runGateway() {
  const child = spawn(process.execPath, ["/app/gateway/dist/main.js"], {
    env: gatewayEnvironment(),
    stdio: "inherit",
  });
  const forwardSignal = (signal) => {
    if (!child.killed) {
      child.kill(signal);
    }
  };
  process.once("SIGINT", () => forwardSignal("SIGINT"));
  process.once("SIGTERM", () => forwardSignal("SIGTERM"));
  const code = await new Promise((resolvePromise, reject) => {
    child.once("error", reject);
    child.once("exit", (exitCode) => resolvePromise(exitCode ?? 1));
  });
  process.exitCode = code;
}

async function runWeb() {
  const port = parsePort(
    process.env.CMCLIENT_WEB_PORT || "8080",
    "DOCKER_WEB_LISTEN_CONFIGURATION_INVALID",
  );
  const server = await startWebServer({
    port,
    upstream: process.env.CMCLIENT_WEB_UPSTREAM,
    webRoot: process.env.CMCLIENT_WEB_ROOT || "/app/web",
  });
  const close = () => server.close();
  process.once("SIGINT", close);
  process.once("SIGTERM", close);
}

async function runIngress() {
  const port = parsePort(
    process.env.CMCLIENT_INGRESS_PORT || "8080",
    "DOCKER_INGRESS_LISTEN_CONFIGURATION_INVALID",
  );
  const server = await startIngressServer({ port });
  const close = () => server.close();
  process.once("SIGINT", close);
  process.once("SIGTERM", close);
}

function parsePort(value, errorCode) {
  if (!/^\d+$/.test(value)) {
    throw new Error(errorCode);
  }
  const port = Number(value);
  if (!Number.isInteger(port) || port < 1 || port > 65_535) {
    throw new Error(errorCode);
  }
  return port;
}
