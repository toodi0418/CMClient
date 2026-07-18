import { createPinia, setActivePinia } from "pinia";
import { beforeEach, describe, expect, it, vi } from "vitest";

import {
  UpdateApiError,
  createUpdatesStore,
  type UpdateEventListener,
  type UpdatesClient,
} from "./updates";

const downloading = {
  schemaVersion: 1 as const,
  job: {
    id: "update-1",
    phase: "downloading" as const,
    updatedAt: "2026-07-18T06:00:00.000Z",
    errorCode: null,
    bytesDownloaded: 524_288,
    bytesTotal: 1_048_576,
    bytesPerSecond: 262_144,
    recentLogCodes: ["UPDATE_DOWNLOAD_STARTED"],
  },
};

describe("updates store", () => {
  beforeEach(() => setActivePinia(createPinia()));

  it("uses the Agent snapshot and then keeps it current from SSE", async () => {
    let listener: UpdateEventListener | undefined;
    const stop = vi.fn();
    const useUpdates = createUpdatesStore({
      status: async () => downloading,
      subscribe: (registered) => {
        listener = registered;
        return stop;
      },
    } satisfies UpdatesClient);
    const updates = useUpdates();

    updates.start();
    await Promise.resolve();

    expect(updates.status).toEqual(downloading);
    expect(updates.connection).toBe("connecting");
    listener?.onStatus({
      ...downloading,
      job: { ...downloading.job, phase: "rolling_back" },
    });
    expect(updates.status?.job?.phase).toBe("rolling_back");
    expect(updates.connection).toBe("open");

    updates.stop();
    expect(stop).toHaveBeenCalledOnce();
    expect(updates.connection).toBe("stopped");
  });

  it("keeps a stable Agent error code when the snapshot cannot load", async () => {
    const useUpdates = createUpdatesStore({
      status: async () => {
        throw new UpdateApiError("CONTROL_COMMAND_FAILED");
      },
      subscribe: () => () => {},
    } satisfies UpdatesClient);
    const updates = useUpdates();

    await updates.refresh();

    expect(updates.status).toBeUndefined();
    expect(updates.errorCode).toBe("CONTROL_COMMAND_FAILED");
  });
});
