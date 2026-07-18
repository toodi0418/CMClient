import { setManagementCsrfToken } from "@cmclient/api-client";
import { defineStore } from "pinia";

const LOGIN_URL = "/api/v1/auth/login";
const SESSION_ERROR_CODES = new Set([
  "MANAGEMENT_SESSION_INVALID",
  "MANAGEMENT_SESSION_EXPIRED",
]);

export class ManagementAuthError extends Error {
  readonly code: string;

  constructor(code: string) {
    super(code);
    this.name = "ManagementAuthError";
    this.code = code;
  }
}

export interface ManagementAuthClient {
  login(password: string): Promise<{ csrfToken: string; expiresAt: number }>;
}

export class BrowserManagementAuthClient implements ManagementAuthClient {
  constructor(
    private readonly fetchImplementation: typeof fetch = (input, init) =>
      globalThis.fetch(input, init),
  ) {}

  async login(
    password: string,
  ): Promise<{ csrfToken: string; expiresAt: number }> {
    let response: Response;
    try {
      response = await this.fetchImplementation(LOGIN_URL, {
        method: "POST",
        credentials: "same-origin",
        headers: {
          accept: "application/json",
          "content-type": "application/json",
        },
        body: JSON.stringify({ password }),
      });
    } catch {
      throw new ManagementAuthError("MANAGEMENT_AUTH_UNAVAILABLE");
    }
    if (!response.ok) {
      throw new ManagementAuthError(await responseErrorCode(response));
    }
    const payload = await responseJson(response);
    if (!isLoginResponse(payload)) {
      throw new ManagementAuthError("MANAGEMENT_AUTH_RESPONSE_INVALID");
    }
    return payload;
  }
}

export function isManagementSessionError(code: string | undefined): boolean {
  return code !== undefined && SESSION_ERROR_CODES.has(code);
}

export function createManagementAuthStore(
  client: ManagementAuthClient = new BrowserManagementAuthClient(),
) {
  return defineStore("management-auth", {
    state: () => ({
      csrfToken: undefined as string | undefined,
      expiresAt: undefined as number | undefined,
      loading: false,
      errorCode: undefined as string | undefined,
      required: false,
    }),
    actions: {
      requireLogin() {
        this.csrfToken = undefined;
        this.expiresAt = undefined;
        this.required = true;
        setManagementCsrfToken(undefined);
      },
      async login(password: string) {
        if (this.loading || !password) {
          return;
        }
        this.loading = true;
        this.errorCode = undefined;
        try {
          const session = await client.login(password);
          this.csrfToken = session.csrfToken;
          this.expiresAt = session.expiresAt;
          this.required = false;
          setManagementCsrfToken(session.csrfToken);
        } catch (error) {
          this.errorCode =
            error instanceof ManagementAuthError
              ? error.code
              : "MANAGEMENT_AUTH_UNAVAILABLE";
        } finally {
          this.loading = false;
        }
      },
    },
  });
}

async function responseErrorCode(response: Response): Promise<string> {
  const payload = await responseJson(response);
  if (
    payload &&
    typeof payload === "object" &&
    !Array.isArray(payload) &&
    "code" in payload &&
    typeof payload.code === "string" &&
    /^[A-Z0-9_]{1,128}$/.test(payload.code)
  ) {
    return payload.code;
  }
  return `MANAGEMENT_AUTH_HTTP_${response.status}`;
}

async function responseJson(response: Response): Promise<unknown> {
  try {
    return await response.json();
  } catch {
    return undefined;
  }
}

function isLoginResponse(
  payload: unknown,
): payload is { csrfToken: string; expiresAt: number } {
  return (
    payload !== null &&
    typeof payload === "object" &&
    !Array.isArray(payload) &&
    "schemaVersion" in payload &&
    payload.schemaVersion === 1 &&
    "csrfToken" in payload &&
    typeof payload.csrfToken === "string" &&
    /^[a-f0-9]{32}$/i.test(payload.csrfToken) &&
    "expiresAt" in payload &&
    typeof payload.expiresAt === "number" &&
    Number.isSafeInteger(payload.expiresAt) &&
    payload.expiresAt > 0
  );
}

export const useManagementAuthStore = createManagementAuthStore();
