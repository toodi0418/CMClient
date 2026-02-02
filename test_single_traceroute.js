'use strict';

const MeshtasticClient = require('./src/meshtasticClient');

function formatHexId(num) {
    if (num === undefined || num === null) return null;
    const hex = (num >>> 0).toString(16).toLowerCase();
    return '!' + hex.padStart(8, '0');
}

const targetNode = '!03919375';
const host = '10.37.45.18';

const client = new MeshtasticClient({
    transport: 'tcp',
    host: host,
    port: 4403,
    tracerouteEnabled: true
});

console.log('[TEST] Starting single traceroute test...');

client.on('connected', () => {
    console.log(`[TEST] Connected to Meshtastic at ${host}:${client.options.port}`);
});

client.on('myInfo', async (info) => {
    console.log(`[TEST] Received MyInfo. Local node: ${formatHexId(info.raw)}`);
    console.log(`[TEST] Sending traceroute to ${targetNode}...`);

    const destId = parseInt(targetNode.replace('!', ''), 16);
    await client.sendTraceroute(destId);
    console.log('[TEST] Traceroute sent. Waiting for response (max 2 minutes)...');

    // Wait 2 minutes then exit
    setTimeout(() => {
        console.log('[TEST] Timeout. Exiting.');
        client.stop();
        process.exit(0);
    }, 120000);
});

client.on('traceroute-log', (entry) => {
    console.log(`[LOG] ${entry.tag}: ${entry.message}`);
});

client.on('traceroute', (entry) => {
    console.log(`[TRACEROUTE_EVENT] status=${entry.status}, from=${entry.from}, to=${entry.to}, hops=${entry.hops}, route=${JSON.stringify(entry.route)}`);

    if (entry.status === 'success' && entry.to === targetNode) {
        console.log('[TEST] SUCCESS! Received traceroute response.');
        client.stop();
        process.exit(0);
    }
});

client.on('error', (err) => {
    console.error(`[ERROR] ${err.message}`);
});

client.start();
