"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");

const fixturePath = path.join(__dirname, "sanitized-packets.json");
const fixtureSet = JSON.parse(fs.readFileSync(fixturePath, "utf8"));

test("sanitized packet fixtures have a valid minimal shape", () => {
  assert.equal(fixtureSet.schemaVersion, 1);
  assert.equal(fixtureSet.sanitized, true);
  assert.equal(fixtureSet.privacy.containsProductionTraffic, false);
  assert.equal(fixtureSet.privacy.containsSecrets, false);
  assert.equal(fixtureSet.privacy.containsPersonalLocations, false);
  assert.ok(Array.isArray(fixtureSet.fixtures));
  assert.ok(fixtureSet.fixtures.length >= 6);

  const ids = new Set();
  for (const fixture of fixtureSet.fixtures) {
    assert.equal(fixture.sanitized, true, `${fixture.id} must be sanitized`);
    assert.match(fixture.id, /^[a-z0-9-]+$/);
    assert.equal(ids.has(fixture.id), false, `${fixture.id} must be unique`);
    ids.add(fixture.id);
    assert.equal(fixture.recording.rawFrameEncoding, "synthetic-hex");
    assert.match(fixture.recording.rawFrameHex, /^[0-9a-f]+$/i);
    assert.match(fixture.recording.gatewayId, /^fixture-gateway-[a-z]+$/);
    assert.match(fixture.recording.serverIngestedAt, /^\d{4}-\d{2}-\d{2}T/);
    assert.match(fixture.normalizedPacket.meshNetworkId, /^fixture-network-/);
    assert.match(fixture.normalizedPacket.nodeId, /^!f1c7a[0-9]{3}$/);
  }
});

test("position fixture expectations preserve CMClient 2.0 safety rules", () => {
  const positions = fixtureSet.fixtures.filter(
    (fixture) => fixture.category === "position",
  );
  assert.equal(positions.length, 4);

  for (const fixture of positions) {
    const { position } = fixture.normalizedPacket;
    assert.ok(Number.isInteger(position.precisionBits));
    if (fixture.expected.aprsUploadEligible) {
      assert.equal(position.precisionBits, 32);
      assert.equal(fixture.expected.decisionCode, "POSITION_ACCEPTED");
    }
  }

  assert.equal(
    positions.find(
      (fixture) => fixture.id === "position-rejected-insufficient-precision",
    ).expected.aprsUploadEligible,
    false,
  );
  assert.equal(
    positions.find(
      (fixture) => fixture.id === "position-backlog-older-than-high-water",
    ).recording.isBacklog,
    true,
  );
});

test("fixture serialization contains no credential markers", () => {
  const serialized = JSON.stringify(fixtureSet);
  assert.doesNotMatch(
    serialized,
    /(BEGIN (RSA|OPENSSH|EC) PRIVATE KEY|gh[pousr]_[A-Za-z0-9_]{20,}|CALLMESH_API_KEY\s*=|APRS_PASSCODE\s*=)/i,
  );
});
