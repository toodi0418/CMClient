'use strict';

const MeshtasticClient = require('./src/meshtasticClient');

function formatHexId(num) {
    if (num === undefined || num === null) return null;
    const hex = (num >>> 0).toString(16).toLowerCase();
    return '!' + hex.padStart(8, '0');
}

const targetNode = '!03919375';
const host = '10.37.45.18';
const INTERVAL_MS = 30000;
const TIMEOUT_MS = 30000;
const MAX_CONSECUTIVE_FAILURES = 5;

let consecutiveFailures = 0;
let totalTests = 0;
let successCount = 0;
let currentTimeout = null;
let isPending = false;

const client = new MeshtasticClient({
    transport: 'tcp',
    host: host,
    port: 4403,
    tracerouteEnabled: true
});

async function runTraceroute() {
    if (!client._selfNodeId) return;

    totalTests++;
    isPending = true;
    console.log(`\n[${new Date().toLocaleTimeString()}] [TEST #${totalTests}] Triggering traceroute to ${targetNode}...`);

    try {
        const destId = parseInt(targetNode.replace('!', ''), 16);
        await client.sendTraceroute(destId);

        // Start timeout timer
        currentTimeout = setTimeout(() => {
            if (isPending) {
                consecutiveFailures++;
                console.warn(`[WARN] Traceroute #${totalTests} TIMEOUT. Consecutive failures: ${consecutiveFailures}`);
                checkFailureLimit();
                isPending = false;
            }
        }, TIMEOUT_MS);

    } catch (err) {
        consecutiveFailures++;
        console.error(`[ERROR] Failed to send traceroute #${totalTests}: ${err.message}. Consecutive: ${consecutiveFailures}`);
        isPending = false;
        checkFailureLimit();
    }
}

function checkFailureLimit() {
    if (consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
        console.error(`\n[CRITICAL] FAILED: ${MAX_CONSECUTIVE_FAILURES} consecutive failures reached. Stopping.`);
        client.stop();
        process.exit(1);
    }
}

client.on('connected', () => {
    console.log(`[TEST] Connected to Meshtastic at ${host}:${client.options.port}. Waiting for node info...`);
});

client.on('myInfo', async (info) => {
    console.log(`[TEST] Received MyInfo. Starting repetitive test every ${INTERVAL_MS / 1000}s...`);
    runTraceroute();
    setInterval(runTraceroute, INTERVAL_MS);
});

client.on('traceroute-log', (entry) => {
    console.log(`[LOG] ${entry.tag}: ${entry.message}`);
});

client.on('traceroute', (entry) => {
    if (entry.status === 'success') {
        if (isPending && entry.to === targetNode) {
            clearTimeout(currentTimeout);
            isPending = false;
            consecutiveFailures = 0; // Reset consecutive failures on success
            successCount++;
            console.log(`[SUCCESS] Traceroute #${totalTests} responded in SUCCESS. Total successes: ${successCount}`);
        }
    } else if (entry.status === 'error') {
        if (isPending && entry.to === targetNode) {
            clearTimeout(currentTimeout);
            isPending = false;
            consecutiveFailures++;
            console.warn(`[ERROR_EVENT] Traceroute #${totalTests} returned ERROR. Consecutive: ${consecutiveFailures}`);
            checkFailureLimit();
        }
    }
});

client.on('error', (err) => {
    console.error(`[GLOBAL_ERROR] ${err.message}`);
});

console.log(`[TEST] Starting stress test client...`);
client.start();
