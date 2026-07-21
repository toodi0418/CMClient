import assert from "node:assert/strict";
import test from "node:test";

import { rewriteAgentHeaders } from "./p13-gateway-bootstrap-lab.mjs";

test("Agent strips every client capability spelling and overwrites one private value", () => {
  const capability = "a".repeat(64);
  const rewritten = rewriteAgentHeaders(
    {
      host: "localhost",
      "X-CMClient-Gateway-Capability": "spoofed",
      "x-correlation-id": "fixture-42",
    },
    capability,
  );
  assert.deepEqual(rewritten, {
    host: "localhost",
    "x-correlation-id": "fixture-42",
    "x-cmclient-gateway-capability": capability,
  });
  assert.equal(JSON.stringify(rewritten).includes("spoofed"), false);
});

test("Agent rejects a malformed capability before forwarding", () => {
  assert.throws(
    () => rewriteAgentHeaders({}, "A".repeat(64)),
    /did not match the regular expression/,
  );
});
