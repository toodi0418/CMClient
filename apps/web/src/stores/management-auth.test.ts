import { createPinia, setActivePinia } from "pinia";
import { beforeEach, describe, expect, it } from "vitest";

import { setManagementCsrfToken } from "@cmclient/api-client";

import {
  BrowserManagementAuthClient,
  ManagementAuthError,
  createManagementAuthStore,
  isManagementSessionError,
} from "./management-auth";

describe("management auth store", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
    setManagementCsrfToken(undefined);
  });

  it("keeps only the CSRF token in memory after a successful login", async () => {
    let request: RequestInit | undefined;
    const client = new BrowserManagementAuthClient(async (_input, init) => {
      request = init;
      return jsonResponse({
        schemaVersion: 1,
        csrfToken: "a".repeat(32),
        expiresAt: 1_784_344_000,
      });
    });
    const auth = createManagementAuthStore(client)();

    await auth.login("correct-password");

    expect(request).toMatchObject({
      method: "POST",
      credentials: "same-origin",
      body: '{"password":"correct-password"}',
    });
    expect(auth.csrfToken).toBe("a".repeat(32));
    expect(auth.expiresAt).toBe(1_784_344_000);
    expect(auth.required).toBe(false);
    expect(auth.errorCode).toBeUndefined();
    expect(Object.keys(auth.$state)).not.toContain("password");
  });

  it("does not retain a token after a rejected login or expired session", async () => {
    const client = {
      async login() {
        throw new ManagementAuthError("MANAGEMENT_CREDENTIALS_INVALID");
      },
    };
    const auth = createManagementAuthStore(client)();

    await auth.login("wrong-password");
    auth.requireLogin();

    expect(auth.errorCode).toBe("MANAGEMENT_CREDENTIALS_INVALID");
    expect(auth.csrfToken).toBeUndefined();
    expect(auth.required).toBe(true);
    expect(isManagementSessionError("MANAGEMENT_SESSION_EXPIRED")).toBe(true);
    expect(isManagementSessionError("MANAGEMENT_CSRF_INVALID")).toBe(false);
  });
});

function jsonResponse(payload: unknown): Response {
  return new Response(JSON.stringify(payload), {
    headers: { "content-type": "application/json" },
  });
}
