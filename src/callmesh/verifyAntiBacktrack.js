'use strict';

const path = require('path');
const assert = require('assert');

// Mock dependencies to prevent side effects during require
const Module = require('module');
const originalRequire = Module.prototype.require;

Module.prototype.require = function (id) {
    if (id.endsWith('/client') || id.includes('aprs/client')) {
        return {
            CallMeshClient: class { },
            APRSClient: class { },
            buildAgentString: () => 'mock-agent'
        };
    }
    if (id.includes('nodeDatabase')) {
        return {
            nodeDatabase: {
                get: () => null,
                upsert: () => ({ changed: false, node: {} })
            }
        };
    }
    if (id.includes('telemetryDatabase')) {
        return {
            TelemetryDatabase: class { }
        };
    }
    if (id.includes('callmeshDataStore')) {
        return {
            CallMeshDataStore: class { }
        };
    }
    return originalRequire.call(this, id);
};

const { CallMeshAprsBridge } = require('./aprsBridge');

console.log('CallMeshAprsBridge loaded.');

// Test runner
async function runTests() {
    const bridge = new CallMeshAprsBridge({
        storageDir: '/tmp/test_storage'
    });

    // Mock emitLog to avoid console spam
    bridge.emitLog = (cat, msg) => {
        // console.log(`[LOG] ${cat}: ${msg}`);
    };

    // Mock scheduleAprsCachePersist
    bridge.scheduleAprsCachePersist = () => { };

    console.log('Starting Anti-Backtrack Verification...');

    const CALLSIGN = 'TEST-1';
    const now = 1000000;

    // Reset state
    bridge.aprsAntiBacktrackState.clear();

    // Test Case 1: First Upload (Should pass)
    console.log('Test 1: First Upload');
    const pos1 = { latitude: 25.0330, longitude: 121.5654 }; // Taipei 101
    const res1 = bridge.evaluateAprsAntiBacktrackGate(CALLSIGN, pos1, now);

    assert.strictEqual(res1.decision, 'upload');
    assert.strictEqual(res1.reason, 'first-fix');

    // Apply update
    bridge.applyAprsAntiBacktrackUpload(CALLSIGN, pos1, now);

    // Test Case 2: Normal Move (Car Speed)
    // Distance ~ 1km, Time 60s => 60km/h (Valid for Car)
    console.log('Test 2: Normal Move');
    const pos2 = { latitude: 25.0420, longitude: 121.5654 }; // ~1km North
    const time2 = now + 60000;
    const res2 = bridge.evaluateAprsAntiBacktrackGate(CALLSIGN, pos2, time2);

    assert.strictEqual(res2.decision, 'upload');
    assert.strictEqual(res2.reason, 'pass');
    assert(res2.speedKph < 200, 'Speed should be within limit');

    bridge.applyAprsAntiBacktrackUpload(CALLSIGN, pos2, time2);

    // Test Case 3: Impossible Move (Instant Jump)
    // Jump 10km in 20s => ~1800 km/h
    console.log('Test 3: Impossible Jump (Hold)');
    const pos3 = { latitude: 25.1320, longitude: 121.5654 }; // ~10km North
    const time3 = time2 + 20000; // 20s gap (> MIN_DT_MS 10s)
    const res3 = bridge.evaluateAprsAntiBacktrackGate(CALLSIGN, pos3, time3);

    assert.strictEqual(res3.decision, 'hold');
    assert.strictEqual(res3.reason, 'speed-exceeded');

    // Check pending state
    const state3 = bridge.getAprsAntiBacktrackState(CALLSIGN);
    assert(state3.pending, 'Should have pending state');
    assert.strictEqual(state3.pending.lat, pos3.latitude);

    // Test Case 4: Confirmation (Next point close to pending)
    // 10s later, nearby pending position
    console.log('Test 4: Confirmation');
    const pos4 = { latitude: 25.1321, longitude: 121.5655 }; // Very close to pos3
    const time4 = time3 + 10000;
    const res4 = bridge.evaluateAprsAntiBacktrackGate(CALLSIGN, pos4, time4);

    assert.strictEqual(res4.decision, 'upload');
    assert.strictEqual(res4.reason, 'pending-confirmed');

    // Apply update (simulating upload)
    bridge.applyAprsAntiBacktrackUpload(CALLSIGN, pos4, time4, res4);
    const state4 = bridge.getAprsAntiBacktrackState(CALLSIGN);
    assert(!state4.pending, 'Pending should be cleared');
    assert.strictEqual(state4.lastUploaded.timestampMs, time4);

    // Test Case 5: HSR Mode
    // Reset state to ensure we start from 'car' for this test
    const stateToReset = bridge.getAprsAntiBacktrackState(CALLSIGN);
    if (stateToReset) {
        stateToReset.mode = 'car';
        // Clear history to avoid far-from-cluster
        stateToReset.history = [stateToReset.lastUploaded];
    }

    // Enter HSR: > 240km/h
    console.log('Test 5: Enter HSR Mode');
    // From pos4, move 5km in 60s => 300km/h
    const pos5 = { latitude: 25.1770, longitude: 121.5655 }; // ~5km North
    const time5 = time4 + 60000;

    const res5 = bridge.evaluateAprsAntiBacktrackGate(CALLSIGN, pos5, time5);
    // First packet at high speed might be held if we strictly enforce Car limit before switching?
    // Logic check: Uses 'mode' from state. State is 'car' initially.
    // vmax = 200. Speed = 300. Should hold.

    assert.strictEqual(res5.decision, 'hold');
    assert.strictEqual(res5.reason, 'speed-exceeded'); // pending

    // Confirm it
    const pos6 = { latitude: 25.1771, longitude: 121.5656 };
    const time6 = time5 + 2000;
    const res6 = bridge.evaluateAprsAntiBacktrackGate(CALLSIGN, pos6, time6);
    assert.strictEqual(res6.decision, 'upload');

    // Apply upload - this should TRIGGER mode switch if implemented in applyAprsAntiBacktrackUpload
    bridge.applyAprsAntiBacktrackUpload(CALLSIGN, pos6, time6, res6);

    const state6 = bridge.getAprsAntiBacktrackState(CALLSIGN);
    assert.strictEqual(state6.mode, 'hsr', 'Should switch to HSR mode');

    // Test Case 6: HSR Valid Speed
    // Now in HSR mode, 300km/h should be valid.
    // Move another 5km in 60s
    console.log('Test 6: Valid HSR Speed');
    const pos7 = { latitude: 25.2220, longitude: 121.5656 };
    const time7 = time6 + 60000;
    const res7 = bridge.evaluateAprsAntiBacktrackGate(CALLSIGN, pos7, time7);

    assert.strictEqual(res7.decision, 'upload');
    assert.strictEqual(res7.reason, 'pass'); // Should pass directly

    console.log('All tests passed!');
}

runTests().catch(err => {
    console.error('Test Failed:', err);
    process.exit(1);
});
