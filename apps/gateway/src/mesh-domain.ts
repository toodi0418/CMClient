import { randomUUID } from "node:crypto";

import type {
  MeshMessage,
  MeshNode,
  MeshObservation,
  MeshTelemetry,
} from "@cmclient/contracts";

import {
  MeshtasticApplicationDecoder,
  type DecodedApplicationPayload,
} from "./protobuf/application.js";
import { GatewayDatabase } from "./persistence/database.js";

export interface MeshDomainStoreOptions {
  idFactory?: () => string;
}

export type StoredApplicationPayload =
  | { kind: "node"; node: MeshNode }
  | { kind: "message"; message: MeshMessage }
  | { kind: "telemetry"; telemetry: MeshTelemetry }
  | { kind: "ignored"; reasonCode: string };

export class MeshDomainStoreError extends Error {
  readonly code = "MESH_DOMAIN_STORE_INVALID";

  constructor() {
    super("MESH_DOMAIN_STORE_INVALID");
  }
}

export class MeshDomainStore {
  private readonly idFactory: () => string;

  constructor(
    private readonly database: GatewayDatabase,
    private readonly decoder: MeshtasticApplicationDecoder,
    options: MeshDomainStoreOptions = {},
  ) {
    this.idFactory = options.idFactory ?? randomUUID;
  }

  persist(
    meshNetworkId: string,
    observation: MeshObservation,
  ): StoredApplicationPayload {
    if (!meshNetworkId.trim() || meshNetworkId.length > 128) {
      throw new MeshDomainStoreError();
    }
    const packet = observation.normalizedFromRadio.packet;
    if (observation.normalizedFromRadio.kind !== "packet" || !packet) {
      return { kind: "ignored", reasonCode: "MESH_APPLICATION_PACKET_MISSING" };
    }
    return this.persistDecoded(
      meshNetworkId,
      observation,
      this.decoder.decode(packet),
    );
  }

  private persistDecoded(
    meshNetworkId: string,
    observation: MeshObservation,
    decoded: DecodedApplicationPayload,
  ): StoredApplicationPayload {
    if (decoded.kind === "ignored") {
      return decoded;
    }
    if (decoded.kind === "node") {
      return {
        kind: "node",
        node: this.upsertNode(meshNetworkId, observation, decoded.node),
      };
    }
    if (decoded.kind === "message") {
      const existing = this.database.meshMessages.findByObservation(
        observation.id,
      );
      if (existing) {
        return { kind: "message", message: existing };
      }
      this.upsertNode(meshNetworkId, observation, {
        nodeNum: decoded.message.sender,
      });
      return {
        kind: "message",
        message: this.database.meshMessages.insert({
          schemaVersion: 1,
          id: this.nextId(),
          observationId: observation.id,
          meshNetworkId,
          ...decoded.message,
          observedAt: observation.ingestedAt,
        }),
      };
    }
    const existing = this.database.meshTelemetry.findByObservation(
      observation.id,
    );
    if (existing) {
      return { kind: "telemetry", telemetry: existing };
    }
    this.upsertNode(meshNetworkId, observation, {
      nodeNum: decoded.telemetry.nodeNum,
    });
    return {
      kind: "telemetry",
      telemetry: this.database.meshTelemetry.insert({
        schemaVersion: 1,
        id: this.nextId(),
        observationId: observation.id,
        meshNetworkId,
        ...decoded.telemetry,
        observedAt: observation.ingestedAt,
      }),
    };
  }

  private upsertNode(
    meshNetworkId: string,
    observation: MeshObservation,
    node: Pick<
      MeshNode,
      "nodeNum" | "userId" | "longName" | "shortName" | "hardwareModel" | "role"
    >,
  ): MeshNode {
    return this.database.meshNodes.upsert({
      schemaVersion: 1,
      meshNetworkId,
      ...node,
      firstSeenAt: observation.ingestedAt,
      lastSeenAt: observation.ingestedAt,
      lastObservationId: observation.id,
    });
  }

  private nextId(): string {
    const id = this.idFactory();
    if (!id || id.length > 128) {
      throw new MeshDomainStoreError();
    }
    return id;
  }
}
