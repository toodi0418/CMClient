import type {
  TransportConnectionState,
  TransportKind,
  TransportMetrics,
} from "@cmclient/contracts";

export interface TransportFrameEvent {
  kind: "frame";
  frame: Uint8Array;
  receivedAt: string;
  sessionConnectedAt?: string;
}

export interface TransportStateEvent {
  kind: "state";
  state: TransportConnectionState;
}

export interface TransportErrorEvent {
  kind: "error";
  code: string;
}

export type TransportEvent =
  TransportFrameEvent | TransportStateEvent | TransportErrorEvent;

export type TransportEventListener = (event: TransportEvent) => void;

/**
 * TCP, Serial, and the deterministic simulator implement this boundary. Domain
 * consumers only see framed bytes and stable connection state, never a socket.
 */
export interface MeshtasticTransport {
  readonly kind: TransportKind;
  readonly state: TransportConnectionState;
  readonly metrics: TransportMetrics;

  connect(): Promise<void>;
  disconnect(): Promise<void>;
  writeFrame(frame: Uint8Array): Promise<void>;
  subscribe(listener: TransportEventListener): () => void;
}
