export type RealtimeProjection =
  | "gateway"
  | "nodes"
  | "messages"
  | "telemetry"
  | "positions"
  | "meshtastic"
  | "aprs"
  | "callmesh"
  | "proxy";

export function projectionForEvent(
  type: string,
): RealtimeProjection | undefined {
  if (type.startsWith("gateway.") || type.startsWith("system.")) {
    return "gateway";
  }
  if (type.startsWith("node.")) {
    return "nodes";
  }
  if (type.startsWith("message.")) {
    return "messages";
  }
  if (type.startsWith("telemetry.")) {
    return "telemetry";
  }
  if (type.startsWith("position.")) {
    return "positions";
  }
  if (type.startsWith("mesh.")) {
    return "meshtastic";
  }
  if (type.startsWith("aprs.")) {
    return "aprs";
  }
  if (type.startsWith("callmesh.")) {
    return "callmesh";
  }
  if (type.startsWith("proxy.")) {
    return "proxy";
  }
  return undefined;
}

export interface RefreshScheduler {
  schedule(
    projection: RealtimeProjection,
    refresh: () => Promise<unknown> | unknown,
  ): void;
  dispose(): void;
}

export function createRefreshScheduler(delayMs = 750): RefreshScheduler {
  const pending = new Map<
    RealtimeProjection,
    () => Promise<unknown> | unknown
  >();
  let timer: ReturnType<typeof setTimeout> | undefined;
  let refreshing = false;
  let disposed = false;

  const scheduleNext = () => {
    if (timer !== undefined || refreshing || disposed || pending.size === 0) {
      return;
    }
    timer = setTimeout(() => {
      timer = undefined;
      const next = pending.entries().next().value;
      if (!next) {
        return;
      }
      const [projection, refresh] = next;
      pending.delete(projection);
      refreshing = true;
      void Promise.resolve(refresh())
        .catch(() => undefined)
        .finally(() => {
          refreshing = false;
          scheduleNext();
        });
    }, delayMs);
  };

  return {
    schedule(projection, refresh) {
      if (disposed) {
        return;
      }
      // Replacing a pending projection coalesces bursts. If the same
      // projection is currently running, this becomes one trailing refresh.
      pending.set(projection, refresh);
      scheduleNext();
    },
    dispose() {
      disposed = true;
      if (timer !== undefined) {
        clearTimeout(timer);
        timer = undefined;
      }
      pending.clear();
    },
  };
}
