import { describe, expect, it } from "vitest";

import {
  AprsRuntimeIdentityError,
  connectionAuthorization,
  deriveAprsPasscode,
  deriveAprsRuntimeIdentity,
  deriveAprsRuntimeState,
  observerConnectionAuthorization,
} from "./aprs-identity";

const baseProvision = {
  callsignBase: "TEST01",
  ssid: -7,
  symbolTable: "/",
  symbolCode: ">",
};

describe("CallMesh provision APRS identity", () => {
  it("derives the Legacy APRS passcode vectors from a normalized base", () => {
    expect(deriveAprsPasscode("TEST01")).toBe(17_602);
    expect(deriveAprsPasscode("AB12CD")).toBe(16_598);
    expect(deriveAprsPasscode("ab-12cd")).toBe(16_598);
  });

  it("formats signed SSIDs without changing the base callsign passcode", () => {
    const negative = deriveAprsRuntimeIdentity(baseProvision);
    const positive = deriveAprsRuntimeIdentity({
      ...baseProvision,
      ssid: 15,
    });
    const zero = deriveAprsRuntimeIdentity({ ...baseProvision, ssid: 0 });

    expect(negative).toMatchObject({
      callsign: "TEST01-7",
      callsignBase: "TEST01",
      ssid: -7,
      passcode: 17_602,
    });
    expect(positive).toMatchObject({
      callsign: "TEST01-15",
      ssid: 15,
      passcode: 17_602,
    });
    expect(zero).toMatchObject({
      callsign: "TEST01",
      ssid: 0,
      passcode: 17_602,
    });
  });

  it("derives a provision-scoped observer login distinct from the TX identity", () => {
    const state = deriveAprsRuntimeState({
      provision: baseProvision,
      provisionFingerprint: "a".repeat(64),
      mappings: [],
      mappingsFingerprint: "b".repeat(64),
    });

    const transmitter = connectionAuthorization(() => state)();
    const observer = observerConnectionAuthorization(() => state)();

    expect(observer).toEqual({
      loginLine: "user TEST01-CM pass 17602 vers CMClient 2.0",
      provisionFingerprint: "a".repeat(64),
    });
    expect(observer?.loginLine).not.toBe(transmitter?.loginLine);
    expect(/^user\s+(\S+)/.exec(observer?.loginLine ?? "")?.[1]).toHaveLength(
      9,
    );
  });

  it("returns no observer authorization while provision state is unavailable", () => {
    expect(observerConnectionAuthorization(() => undefined)()).toBeUndefined();
  });

  it.each([
    ["A", 13_026],
    ["AB", 12_960],
  ])(
    "supports a %s short base callsign for the verified observer login",
    (callsignBase, passcode) => {
      const state = deriveAprsRuntimeState({
        provision: { ...baseProvision, callsignBase },
        provisionFingerprint: "a".repeat(64),
        mappings: [],
        mappingsFingerprint: "b".repeat(64),
      });

      expect(observerConnectionAuthorization(() => state)()).toEqual({
        loginLine: `user ${callsignBase}-CM pass ${passcode} vers CMClient 2.0`,
        provisionFingerprint: "a".repeat(64),
      });
    },
  );

  it("fails closed for invalid or colliding observer identities", () => {
    const state = deriveAprsRuntimeState({
      provision: baseProvision,
      provisionFingerprint: "a".repeat(64),
      mappings: [],
      mappingsFingerprint: "b".repeat(64),
    });
    const invalidStates = [
      {
        ...state,
        identity: {
          ...state.identity,
          callsignBase: "TOOLONG",
          callsign: "TOOLONG-7",
        },
      },
      {
        ...state,
        identity: { ...state.identity, callsign: "TEST01-CM" },
      },
      {
        ...state,
        mappings: [
          {
            version: "mapping-v1",
            effectiveAt: "2026-07-25T00:00:00.000Z",
            meshNetworkId: "fixture-network",
            nodeNum: 42,
            callsign: "TEST01-CM",
          },
        ],
      },
    ];

    for (const invalidState of invalidStates) {
      expect(() =>
        observerConnectionAuthorization(() => invalidState)(),
      ).toThrow(AprsRuntimeIdentityError);
    }
  });

  it("normalizes comment whitespace and resolves a symbol overlay for the wire", () => {
    expect(
      deriveAprsRuntimeIdentity({
        ...baseProvision,
        symbolTable: "\\",
        symbolCode: "&",
        symbolOverlay: "P",
        comment: "  Sanitized   field relay  ",
      }),
    ).toEqual({
      callsign: "TEST01-7",
      callsignBase: "TEST01",
      ssid: -7,
      passcode: 17_602,
      symbolOverlay: "P",
      symbolTable: "\\",
      effectiveSymbolTable: "P",
      symbolCode: "&",
      comment: "Sanitized field relay",
    });
  });

  it("uses the provision table when overlay is absent or explicitly disabled", () => {
    const absent = deriveAprsRuntimeIdentity(baseProvision);
    const disabled = deriveAprsRuntimeIdentity({
      ...baseProvision,
      symbolTable: "\\",
      symbolOverlay: null,
    });

    expect(absent).toMatchObject({
      symbolOverlay: null,
      symbolTable: "/",
      effectiveSymbolTable: "/",
    });
    expect(absent).not.toHaveProperty("comment");
    expect(disabled).toMatchObject({
      symbolOverlay: null,
      symbolTable: "\\",
      effectiveSymbolTable: "\\",
    });
  });

  it.each([
    null,
    { ...baseProvision, callsignBase: "test01" },
    { ...baseProvision, callsignBase: "-------" },
    { ...baseProvision, ssid: 16 },
    { ...baseProvision, ssid: 1.5 },
    { ...baseProvision, symbolTable: "" },
    { ...baseProvision, symbolCode: "\n" },
    { ...baseProvision, symbolOverlay: "PP" },
    { ...baseProvision, comment: "   " },
    { ...baseProvision, comment: "line\nfeed" },
    { ...baseProvision, unexpected: true },
  ])("fails closed for an invalid normalized provision %#", (provision) => {
    expect(() => deriveAprsRuntimeIdentity(provision)).toThrowError(
      expect.objectContaining({
        code: "APRS_PROVISION_INVALID",
      }),
    );
  });

  it("fails closed when passcode input normalizes outside a valid base", () => {
    for (const value of ["", "-------", "TOO-LONG-CALLSIGN"]) {
      expect(() => deriveAprsPasscode(value)).toThrow(AprsRuntimeIdentityError);
    }
  });
});
