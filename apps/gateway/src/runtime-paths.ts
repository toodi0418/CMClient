import { homedir } from "node:os";
import { isAbsolute, join, normalize, relative, resolve } from "node:path";

export interface GatewayRuntimePaths {
  readonly root: string;
  readonly database: string;
  readonly backups: string;
}

export class GatewayRuntimePathError extends Error {
  constructor(readonly code = "GATEWAY_RUNTIME_PATH_CONFIGURATION_INVALID") {
    super(code);
    this.name = "GatewayRuntimePathError";
  }
}

/**
 * Resolve the one mutable runtime root. Native production startup may only use
 * the effective home directory; Docker has one explicit fixed root. A custom
 * root is available only to an explicitly marked test/lab process.
 */
export function gatewayRuntimePaths(
  environment: Record<string, string | undefined>,
): GatewayRuntimePaths {
  for (const legacy of [
    "CMCLIENT_DATA_DIR",
    "CMCLIENT_CONFIG_DIR",
    "CMCLIENT_CACHE_DIR",
    "CMCLIENT_LOG_DIR",
  ]) {
    if (environment[legacy]?.trim()) {
      throw new GatewayRuntimePathError("GATEWAY_RUNTIME_LEGACY_ROOT_REJECTED");
    }
  }
  const profile = environment.CMCLIENT_RUNTIME_PROFILE?.trim().toLowerCase();
  const docker = profile === "docker";
  const supervised = environment.CMCLIENT_SUPERVISED?.trim() === "1";
  const testOverride =
    environment.CMCLIENT_TEST_MODE?.trim() === "1" ||
    environment.CMCLIENT_QUALIFICATION_MODE?.trim() === "1";
  const expectedRoot = docker
    ? normalize("/home/cmclient/.cmclient")
    : normalize(join(effectiveHome(environment), ".cmclient"));
  const configuredRoot = environment.CMCLIENT_RUNTIME_ROOT?.trim();
  if (
    supervised &&
    (!configuredRoot ||
      !environment.CMCLIENT_DB_PATH?.trim() ||
      !environment.CMCLIENT_BACKUP_DIR?.trim())
  ) {
    throw new GatewayRuntimePathError("GATEWAY_RUNTIME_PATHS_REQUIRED");
  }
  const root = configuredRoot
    ? validateRoot(
        configuredRoot,
        expectedRoot,
        docker,
        supervised,
        testOverride,
      )
    : expectedRoot;
  const database = resolveChildPath(
    environment.CMCLIENT_DB_PATH,
    root,
    "cmclient.db",
    true,
  );
  const backups = resolveChildPath(
    environment.CMCLIENT_BACKUP_DIR,
    root,
    "backups",
    true,
  );
  return { root, database, backups };
}

function effectiveHome(
  environment: Record<string, string | undefined>,
): string {
  const home =
    process.platform === "win32"
      ? environment.USERPROFILE?.trim()
      : environment.HOME?.trim();
  const fallback =
    home ||
    environment.HOME?.trim() ||
    environment.USERPROFILE?.trim() ||
    homedir();
  if (!isAbsolute(fallback)) {
    throw new GatewayRuntimePathError("GATEWAY_RUNTIME_HOME_INVALID");
  }
  return normalize(fallback);
}

function validateRoot(
  configured: string,
  expected: string,
  docker: boolean,
  supervised: boolean,
  testOverride: boolean,
): string {
  if (!isAbsolute(configured)) {
    throw new GatewayRuntimePathError("GATEWAY_RUNTIME_ROOT_NOT_ABSOLUTE");
  }
  const root = normalize(resolve(configured));
  if (docker && root !== expected) {
    throw new GatewayRuntimePathError("GATEWAY_RUNTIME_ROOT_INVALID");
  }
  if (!docker && !supervised && !testOverride && root !== expected) {
    throw new GatewayRuntimePathError("GATEWAY_RUNTIME_ROOT_INVALID");
  }
  return root;
}

function resolveChildPath(
  configured: string | undefined,
  root: string,
  childName: string,
  exact: boolean,
): string {
  const candidate = configured?.trim();
  if (!candidate) {
    return join(root, childName);
  }
  if (!isAbsolute(candidate)) {
    throw new GatewayRuntimePathError("GATEWAY_RUNTIME_CHILD_NOT_ABSOLUTE");
  }
  const resolved = normalize(resolve(candidate));
  const rel = relative(root, resolved);
  if (rel.startsWith("..") || isAbsolute(rel)) {
    throw new GatewayRuntimePathError("GATEWAY_RUNTIME_CHILD_OUTSIDE_ROOT");
  }
  if (exact && resolved !== join(root, childName)) {
    throw new GatewayRuntimePathError("GATEWAY_RUNTIME_CHILD_INVALID");
  }
  return resolved;
}
