import type {
  ConnectionStatus,
  TransportConnectionState,
  TransportKind,
} from "@cmclient/contracts";

export interface ConnectionTransitionOptions {
  attempt?: number;
  reasonCode?: string;
}

export type Clock = () => Date;

export class TransportStateTransitionError extends Error {
  readonly code = "TRANSPORT_STATE_TRANSITION_INVALID";
}

export class TransportConnectionStateMachine {
  private current: TransportConnectionState;

  constructor(
    private readonly transport: TransportKind,
    private readonly clock: Clock = () => new Date(),
  ) {
    this.current = { transport, status: "disconnected", changedAt: now(clock) };
  }

  get state(): TransportConnectionState {
    return this.current;
  }

  transition(
    next: ConnectionStatus,
    options: ConnectionTransitionOptions = {},
  ): TransportConnectionState {
    if (next === this.current.status) {
      validateTransition(next, options);
      if (next === "backoff") {
        this.current = {
          transport: this.transport,
          status: next,
          changedAt: now(this.clock),
          attempt: options.attempt,
          reasonCode: options.reasonCode,
        } as TransportConnectionState;
      }
      return this.current;
    }
    if (!canTransition(this.current.status, next)) {
      throw new TransportStateTransitionError();
    }
    validateTransition(next, options);
    this.current = {
      transport: this.transport,
      status: next,
      changedAt: now(this.clock),
      ...(next === "backoff" ? { attempt: options.attempt } : {}),
      ...(options.reasonCode ? { reasonCode: options.reasonCode } : {}),
    } as TransportConnectionState;
    return this.current;
  }
}

export function canTransition(
  current: ConnectionStatus,
  next: ConnectionStatus,
): boolean {
  if (current === next || next === "disconnected") {
    return true;
  }
  return ALLOWED_TRANSITIONS[current].includes(next);
}

const ALLOWED_TRANSITIONS: Record<ConnectionStatus, ConnectionStatus[]> = {
  disconnected: ["connecting"],
  connecting: ["configuring", "degraded", "backoff"],
  configuring: ["ready", "degraded", "backoff"],
  ready: ["configuring", "degraded", "backoff"],
  degraded: ["connecting", "ready", "backoff"],
  backoff: ["connecting"],
};

function validateTransition(
  next: ConnectionStatus,
  options: ConnectionTransitionOptions,
): void {
  if (
    options.reasonCode !== undefined &&
    !/^[A-Z][A-Z0-9_]{1,127}$/.test(options.reasonCode)
  ) {
    throw new TransportStateTransitionError();
  }
  if (next === "backoff") {
    if (
      !Number.isInteger(options.attempt) ||
      options.attempt === undefined ||
      options.attempt < 1
    ) {
      throw new TransportStateTransitionError();
    }
    if (!options.reasonCode) {
      throw new TransportStateTransitionError();
    }
  }
  if (next === "degraded" && !options.reasonCode) {
    throw new TransportStateTransitionError();
  }
}

function now(clock: Clock): string {
  return clock().toISOString();
}
