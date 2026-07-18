import process from "node:process";
import { spawn } from "node:child_process";

import { gatewayEnvironment, startWebServer } from "./container-runtime.mjs";

const command = process.argv[2] || "gateway";

if (command === "gateway") {
  await runGateway();
} else if (command === "web") {
  await runWeb();
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
  const port = parsePort(process.env.CMCLIENT_WEB_PORT || "8080");
  const server = await startWebServer({
    port,
    upstream: process.env.CMCLIENT_WEB_UPSTREAM,
    webRoot: process.env.CMCLIENT_WEB_ROOT || "/app/web",
  });
  const close = () => server.close();
  process.once("SIGINT", close);
  process.once("SIGTERM", close);
}

function parsePort(value) {
  if (!/^\d+$/.test(value)) {
    throw new Error("DOCKER_WEB_LISTEN_CONFIGURATION_INVALID");
  }
  const port = Number(value);
  if (!Number.isInteger(port) || port < 1 || port > 65_535) {
    throw new Error("DOCKER_WEB_LISTEN_CONFIGURATION_INVALID");
  }
  return port;
}
