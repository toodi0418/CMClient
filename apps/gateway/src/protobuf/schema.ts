import { createHash } from "node:crypto";
import { existsSync, readFileSync, readdirSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { Root, type Enum, type Type } from "protobufjs";

export const MESHTASTIC_SCHEMA_VERSION = "meshtastic-proto-v1";
export const MESHTASTIC_PROTO_SHA256 =
  "762fc01e0e6520b03487c6cc7b4afbafeadc39f10a66fa17def966e9ea428602";

export interface MeshtasticSchema {
  fromRadio: Type;
  portNum: Enum;
  root: Root;
  toRadio: Type;
}

export async function loadMeshtasticSchema(
  protoDirectory: string = defaultProtoDirectory(),
): Promise<MeshtasticSchema> {
  const root = new Root();
  root.resolvePath = (origin, target) => {
    const direct = resolve(protoDirectory, target);
    return existsSync(direct) ? direct : resolve(dirname(origin), target);
  };
  await root.load(resolve(protoDirectory, "meshtastic/mesh.proto"));
  root.resolveAll();
  return {
    root,
    fromRadio: root.lookupType("meshtastic.FromRadio"),
    toRadio: root.lookupType("meshtastic.ToRadio"),
    portNum: root.lookupEnum("meshtastic.PortNum"),
  };
}

export function fingerprintMeshtasticProtoDirectory(
  protoDirectory: string,
): string {
  const directory = resolve(protoDirectory, "meshtastic");
  const hash = createHash("sha256");
  for (const filename of readdirSync(directory)
    .filter((name) => name.endsWith(".proto"))
    .sort()) {
    hash.update(filename);
    hash.update("\0");
    hash.update(readFileSync(resolve(directory, filename)));
    hash.update("\0");
  }
  return hash.digest("hex");
}

export function defaultProtoDirectory(): string {
  return resolve(dirname(fileURLToPath(import.meta.url)), "../../../../proto");
}
