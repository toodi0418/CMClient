export type ProblemSeverity = "info" | "warning" | "error";

export interface UserProblem {
  severity: ProblemSeverity;
  titleKey: string;
  messageKey: string;
  retryable: boolean;
}

const DEFAULT_PROBLEM: UserProblem = {
  severity: "error",
  titleKey: "problems.genericTitle",
  messageKey: "problems.genericMessage",
  retryable: true,
};

const RATE_LIMIT_PROBLEM: UserProblem = {
  severity: "warning",
  titleKey: "problems.rateLimitedTitle",
  messageKey: "problems.rateLimitedMessage",
  retryable: true,
};

const SESSION_PROBLEM: UserProblem = {
  severity: "warning",
  titleKey: "problems.sessionTitle",
  messageKey: "problems.sessionMessage",
  retryable: false,
};

const LOCAL_SERVICE_PROBLEM: UserProblem = {
  severity: "warning",
  titleKey: "problems.localServiceTitle",
  messageKey: "problems.localServiceMessage",
  retryable: true,
};

const CALLMESH_STALE_PROBLEM: UserProblem = {
  severity: "warning",
  titleKey: "problems.callmeshStaleTitle",
  messageKey: "problems.callmeshStaleMessage",
  retryable: false,
};

const APRS_PROVISION_PROBLEM: UserProblem = {
  severity: "warning",
  titleKey: "problems.aprsProvisionTitle",
  messageKey: "problems.aprsProvisionMessage",
  retryable: false,
};

const PROXY_CONFIGURATION_PROBLEM: UserProblem = {
  severity: "info",
  titleKey: "problems.proxyConfigurationTitle",
  messageKey: "problems.proxyConfigurationMessage",
  retryable: false,
};

const NOT_CONFIGURED_PROBLEM: UserProblem = {
  severity: "info",
  titleKey: "problems.notConfiguredTitle",
  messageKey: "problems.notConfiguredMessage",
  retryable: false,
};

const NOT_ENABLED_PROBLEM: UserProblem = {
  severity: "info",
  titleKey: "problems.notEnabledTitle",
  messageKey: "problems.notEnabledMessage",
  retryable: false,
};

export function problemForCode(code: string | null | undefined): UserProblem {
  if (!code) {
    return DEFAULT_PROBLEM;
  }
  if (/^MANAGEMENT_(?:REQUEST|LOGIN)_RATE_LIMITED$/.test(code)) {
    return RATE_LIMIT_PROBLEM;
  }
  if (
    code === "MANAGEMENT_SESSION_INVALID" ||
    code === "MANAGEMENT_SESSION_EXPIRED" ||
    code === "MANAGEMENT_LOCAL_SESSION_DENIED"
  ) {
    return SESSION_PROBLEM;
  }
  if (code === "CALLMESH_STALE_RESPONSE") {
    return CALLMESH_STALE_PROBLEM;
  }
  if (code === "APRS_PROVISION_UNAVAILABLE") {
    return APRS_PROVISION_PROBLEM;
  }
  if (code === "PROXY_RUNTIME_UNAVAILABLE") {
    return PROXY_CONFIGURATION_PROBLEM;
  }
  if (code === "not_configured" || /_NOT_CONFIGURED$/.test(code)) {
    return NOT_CONFIGURED_PROBLEM;
  }
  if (code === "not_enabled" || /_NOT_ENABLED$/.test(code)) {
    return NOT_ENABLED_PROBLEM;
  }
  if (
    code === "GATEWAY_NETWORK_UNAVAILABLE" ||
    code === "MANAGEMENT_AUTH_UNAVAILABLE" ||
    code === "AGENT_SETUP_UNAVAILABLE" ||
    /(?:_UNAVAILABLE|_TIMEOUT|_HTTP_5\d\d)$/.test(code)
  ) {
    return LOCAL_SERVICE_PROBLEM;
  }
  return DEFAULT_PROBLEM;
}

export function capabilityReasonKey(
  reasonCode: string | null | undefined,
): string {
  switch (reasonCode) {
    case "owned_by_agent":
      return "capabilityReason.ownedByAgent";
    case "owned_by_graphical_mode":
      return "capabilityReason.ownedByGraphicalMode";
    case "not_configured":
      return "capabilityReason.notConfigured";
    case "unavailable_in_native":
      return "capabilityReason.unavailableInNative";
    case "not_enabled":
      return "capabilityReason.notEnabled";
    default:
      return "capabilityReason.unavailable";
  }
}

export function updateActivityKey(code: string): string {
  switch (code) {
    case "UPDATE_DOWNLOAD_STARTED":
      return "updates.activity.downloadStarted";
    case "UPDATE_SIGNATURE_VERIFIED":
      return "updates.activity.signatureVerified";
    case "UPDATE_STAGED":
      return "updates.activity.staged";
    case "UPDATE_COMPLETED":
      return "updates.activity.completed";
    default:
      return "updates.activity.recorded";
  }
}

export function eventActivityKey(type: string): string {
  if (type.startsWith("node.")) {
    return "logs.activity.node";
  }
  if (type.startsWith("message.")) {
    return "logs.activity.message";
  }
  if (type.startsWith("telemetry.")) {
    return "logs.activity.telemetry";
  }
  if (type.startsWith("position.")) {
    return "logs.activity.position";
  }
  if (type.startsWith("aprs.")) {
    return "logs.activity.aprs";
  }
  if (type.startsWith("callmesh.")) {
    return "logs.activity.callmesh";
  }
  if (type.startsWith("proxy.")) {
    return "logs.activity.proxy";
  }
  return "logs.activity.generic";
}
