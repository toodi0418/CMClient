import { Value } from "@sinclair/typebox/value";
import {
  CallMeshProvisionSchema,
  type CallMeshMapping,
  type CallMeshProvision,
} from "@cmclient/contracts";

import type { CallMeshAprsState } from "./callmesh.js";

const APRS_CALLSIGN_BASE = /^[A-Z0-9]{1,6}$/;
const APRS_LOGIN_CALLSIGN_BASE = /^[A-Z0-9]{3,6}$/;
const APRS_LOGIN_CALLSIGN = /^[A-Z0-9]{3,6}(?:-[A-Z0-9]{1,2})?$/;
const MAX_APRS_LOGIN_CALLSIGN_BYTES = 9;
const APRS_PRINTABLE_CHARACTER = /^[ -~]$/;
const MAX_APRS_COMMENT_LENGTH = 80;
export const PROVISION_FINGERPRINT_PATTERN = /^[a-f0-9]{64}$/;

export interface AprsRuntimeIdentity {
  readonly callsign: string;
  readonly callsignBase: string;
  readonly ssid: number;
  readonly passcode: number;
  readonly symbolOverlay: string | null;
  readonly symbolTable: string;
  readonly effectiveSymbolTable: string;
  readonly symbolCode: string;
  readonly comment?: string;
}

export interface AprsRuntimeState {
  readonly mappings: readonly CallMeshMapping[];
  readonly mappingsFingerprint: string;
  readonly identity: AprsRuntimeIdentity;
  readonly provision: CallMeshProvision;
  readonly provisionFingerprint: string;
}

/** Provision-scoped authorization material for an APRS-IS connection. */
export interface AprsConnectionAuthorization {
  readonly loginLine: string;
  readonly provisionFingerprint: string;
}

export type AprsAuthorizationProvider = () =>
  AprsConnectionAuthorization | undefined;

export class AprsRuntimeIdentityError extends Error {
  readonly code = "APRS_PROVISION_INVALID";

  constructor() {
    super("APRS_PROVISION_INVALID");
    this.name = "AprsRuntimeIdentityError";
  }
}

export function deriveAprsRuntimeIdentity(
  candidate: unknown,
): AprsRuntimeIdentity {
  if (!Value.Check(CallMeshProvisionSchema, candidate)) {
    throw new AprsRuntimeIdentityError();
  }
  const provision = candidate as CallMeshProvision;
  const callsignBase = normalizeCallsignBase(provision.callsignBase);
  const symbolTable = normalizeSymbol(provision.symbolTable);
  const symbolCode = normalizeSymbol(provision.symbolCode);
  const symbolOverlay =
    provision.symbolOverlay === undefined || provision.symbolOverlay === null
      ? null
      : normalizeSymbol(provision.symbolOverlay);
  const comment = normalizeComment(provision.comment);
  const ssid = provision.ssid;
  if (!Number.isInteger(ssid) || ssid < -15 || ssid > 15) {
    throw new AprsRuntimeIdentityError();
  }
  return {
    callsign: ssid === 0 ? callsignBase : `${callsignBase}-${Math.abs(ssid)}`,
    callsignBase,
    ssid,
    passcode: deriveAprsPasscode(callsignBase),
    symbolOverlay,
    symbolTable,
    effectiveSymbolTable: symbolOverlay ?? symbolTable,
    symbolCode,
    ...(comment ? { comment } : {}),
  };
}

export function deriveAprsRuntimeState(
  state: CallMeshAprsState,
): AprsRuntimeState {
  if (
    !PROVISION_FINGERPRINT_PATTERN.test(state.provisionFingerprint) ||
    !PROVISION_FINGERPRINT_PATTERN.test(state.mappingsFingerprint)
  ) {
    throw new AprsRuntimeIdentityError();
  }
  const identity = deriveAprsRuntimeIdentity(state.provision);
  return {
    mappings: state.mappings.map((mapping) => ({ ...mapping })),
    mappingsFingerprint: state.mappingsFingerprint,
    identity,
    provision: { ...state.provision },
    provisionFingerprint: state.provisionFingerprint,
  };
}

export function connectionAuthorization(
  stateProvider: () => AprsRuntimeState | undefined,
): AprsAuthorizationProvider {
  return () => {
    const state = stateProvider();
    if (!state) {
      return undefined;
    }
    return {
      loginLine: `user ${state.identity.callsign} pass ${state.identity.passcode} vers CMClient 2.0`,
      provisionFingerprint: state.provisionFingerprint,
    };
  };
}

export function observerConnectionAuthorization(
  stateProvider: () => AprsRuntimeState | undefined,
): AprsAuthorizationProvider {
  return () => {
    const state = stateProvider();
    if (!state) {
      return undefined;
    }
    const callsign = deriveAprsObserverCallsign(state);
    return {
      loginLine: `user ${callsign} pass ${state.identity.passcode} vers CMClient 2.0`,
      provisionFingerprint: state.provisionFingerprint,
    };
  };
}

export function deriveAprsPasscode(callsignBase: string): number {
  const base = normalizeCallsignBase(callsignBase);
  let hash = 0x73e2;
  for (let index = 0; index < base.length; index += 1) {
    hash ^= base.charCodeAt(index) << 8;
    index += 1;
    if (index < base.length) {
      hash ^= base.charCodeAt(index);
    }
  }
  return hash & 0x7fff;
}

function normalizeCallsignBase(value: unknown): string {
  if (typeof value !== "string") {
    throw new AprsRuntimeIdentityError();
  }
  const normalized = value.toUpperCase().replace(/[^A-Z0-9]/g, "");
  if (!APRS_CALLSIGN_BASE.test(normalized)) {
    throw new AprsRuntimeIdentityError();
  }
  return normalized;
}

export function deriveAprsObserverCallsign(state: AprsRuntimeState): string {
  const { callsignBase, callsign, ssid } = state.identity;
  if (
    !APRS_LOGIN_CALLSIGN_BASE.test(callsignBase) ||
    !Number.isInteger(ssid) ||
    ssid < -15 ||
    ssid > 15
  ) {
    throw new AprsRuntimeIdentityError();
  }
  const expectedTransmitterCallsign =
    ssid === 0 ? callsignBase : `${callsignBase}-${Math.abs(ssid)}`;
  const observerSuffix = `C${Math.abs(ssid).toString(16).toUpperCase()}`;
  const observerCallsign = `${callsignBase}-${observerSuffix}`;
  if (
    callsign !== expectedTransmitterCallsign ||
    observerCallsign === callsign ||
    !APRS_LOGIN_CALLSIGN.test(observerCallsign) ||
    Buffer.byteLength(observerCallsign, "ascii") >
      MAX_APRS_LOGIN_CALLSIGN_BYTES ||
    state.mappings.some(
      (mapping) => mapping.callsign.toUpperCase() === observerCallsign,
    )
  ) {
    throw new AprsRuntimeIdentityError();
  }
  return observerCallsign;
}

function normalizeSymbol(value: unknown): string {
  if (typeof value !== "string" || !APRS_PRINTABLE_CHARACTER.test(value)) {
    throw new AprsRuntimeIdentityError();
  }
  return value;
}

function normalizeComment(value: string | undefined): string | undefined {
  if (value === undefined) {
    return undefined;
  }
  const normalized = value.replace(/\s+/g, " ").trim();
  if (
    !normalized ||
    normalized.length > MAX_APRS_COMMENT_LENGTH ||
    hasControlCharacter(normalized)
  ) {
    throw new AprsRuntimeIdentityError();
  }
  return normalized;
}

function hasControlCharacter(value: string): boolean {
  return [...value].some((character) => {
    const codePoint = character.codePointAt(0) ?? 0;
    return codePoint <= 0x1f || codePoint === 0x7f;
  });
}
