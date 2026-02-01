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
    tracerouteEnabled: false
});

console.log('[TEST] Final traceroute debug test - wait up to 60s, no retry...\n');

let testStartTime;
let attempt = 0;
let timeoutTimer = null;
const TIMEOUT_MS = 60000;

function clearTimers() {
    if (timeoutTimer) {
        clearTimeout(timeoutTimer);
        timeoutTimer = null;
    }
}

async function sendOnce() {
    attempt += 1;
    console.log(`→ [TRY #${attempt}] Sending traceroute request to ${targetNode}...\n`);

    testStartTime = Date.now();
    const destId = parseInt(targetNode.replace('!', ''), 16);
    try {
        await client.sendTraceroute(destId);
        console.log(`✓ Request sent.`);
    } catch (e) {
        console.error(`✗ Send failed: ${e.message}`);
    }

    timeoutTimer = setTimeout(() => {
        console.log(`\n✗ TIMEOUT after ${TIMEOUT_MS / 1000} seconds. Stop.`);
        client.stop();
        process.exit(1);
    }, TIMEOUT_MS);
}

client.on('connected', () => {
    console.log(`✓ Connected to Meshtastic at ${host}:${client.options.port}`);
});

client.on('ack', (packet) => {
    console.log(`[ACK] Received ACK for packetId: ${packet.id} from ${formatHexId(packet.from)}`);
});

client.on('retransmission', (packet) => {
    console.log(`[RE-TX] Retransmitting packetId: ${packet.id} (attempt ${packet.retries})`);
});

client.on('myInfo', async (info) => {
    console.log(`✓ Received MyInfo. Local node: ${formatHexId(info.raw)}`);
    console.log('→ 等待 10 秒讓裝置就緒再送出...\n');
    setTimeout(() => {
        sendOnce();
    }, 10000);
});

// Monitor ALL relevant packets
client.on('fromRadio', ({ message }) => {
    if (message.payloadVariant === 'clientNotification') {
        const note = message.clientNotification || {};
        console.log(`[NOTIFY] level=${note.level} msg=${note.message || ''} reply_id=${note.replyId || ''}`);
        return;
    }
    if (message.payloadVariant === 'queueStatus') {
        const qs = message.queueStatus || {};
        console.log(`[QUEUE] res=${qs.res} free=${qs.free} maxlen=${qs.maxlen} txQueue=${qs.txQueue} rxQueue=${qs.rxQueue}`);
        return;
    }
    if (message.payloadVariant === 'packet' && message.packet?.decoded) {
        const portnum = message.packet.decoded.portnum;
        const from = message.packet.from;
        const to = message.packet.to;

        // Debug: Log ALL packets concise
        let portName = portnum;
        if (portnum === 5 || portnum === 'ROUTING_APP') portName = 'ROUTING';
        if (portnum === 70 || portnum === 'TRACEROUTE_APP') portName = 'TRACEROUTE';
        if (portnum === 3 || portnum === 'POSITION_APP') portName = 'POSITION';
        if (portnum === 4 || portnum === 'NODEINFO_APP') portName = 'NODEINFO';
        if (portnum === 67 || portnum === 'TELEMETRY_APP') portName = 'TELEMETRY';

        console.log(`[Rx] from=${formatHexId(from)} to=${formatHexId(to)} port=${portName} id=${message.packet.id}`);

        // 5 = ROUTING_APP, 70 = TRACEROUTE_APP
        if (portnum === 5 || portnum === 70) {
            const elapsed = ((Date.now() - testStartTime) / 1000).toFixed(1);
            const portName = portnum === 5 ? 'ROUTING_APP' : 'TRACEROUTE_APP';
            console.log(`\n[${elapsed}s] !!! ${portName} PACKET RECEIVED !!!`);
            console.log(`  from: ${from} (${formatHexId(from)})`);
            console.log(`  to: ${to} (${formatHexId(to)})`);
            console.log(`  payload length: ${message.packet.decoded.payload?.length || 0}`);
            console.log(`  request_id: ${message.packet.decoded.requestId}`);
        }
    }
});

// Monitor Summary events (internal processing result)
client.on('summary', (summary) => {
    if (summary.type === 'Traceroute' || summary.type === 'RouteReply') {
        const elapsed = ((Date.now() - testStartTime) / 1000).toFixed(1);
        console.log(`\n[${elapsed}s] ➜ SUMMARY EVENT: ${summary.type}`);
        if (summary.trace) {
            console.log('  Has Trade Data:', JSON.stringify(summary.trace));
        } else {
            console.log('  NO Trace Data');
        }
    }
    if (summary.type === 'RouteError') {
        const elapsed = ((Date.now() - testStartTime) / 1000).toFixed(1);
        const from = summary.from?.meshId || summary.from?.meshIdNormalized || summary.from?.label || summary.from?.raw;
        const to = summary.to?.meshId || summary.to?.meshIdNormalized || summary.to?.label || summary.to?.raw;
        console.log(`\n[${elapsed}s] ➜ ROUTE ERROR: ${summary.details || ''} from=${from} to=${to} requestId=${summary.requestId}`);
    }
});

client.on('traceroute-log', (log) => {
    console.log(`[LOG:${log.tag}] ${log.message}`);
});

client.on('traceroute', (entry) => {
    const elapsed = ((Date.now() - testStartTime) / 1000).toFixed(1);
    console.log(`\n[${elapsed}s] ★★★ TRACEROUTE EVENT EMITTED ★★★`);
    console.log(`  status: ${entry.status}`);
    console.log(`  hops: ${entry.hops}`);

    if (entry.status === 'success') {
        console.log('\n✓✓✓ SUCCESS! Traceroute complete!');
        clearTimers();
        client.stop();
        process.exit(0);
    }
});

client.on('error', (err) => {
    console.error(`✗ ERROR: ${err.message}`);
});

client.start();
