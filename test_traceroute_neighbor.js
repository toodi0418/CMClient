const MeshtasticClient = require('./src/meshtasticClient');

// !06b38c10 which is seen in logs
const targetNode = '!06b38c10';

const client = new MeshtasticClient();
const testStartTime = Date.now();

function formatHexId(num) {
    if (num === undefined || num === null) return null;
    const hex = (num >>> 0).toString(16).toLowerCase();
    return '!' + hex.padStart(8, '0');
}

console.log('[TEST] Neighbor traceroute test (for !06b38c10) - waiting 45 seconds...');

client.on('connect', () => {
    console.log(`✓ Connected to Meshtastic at ${client.host}:${client.port}`);
});

client.on('ready', () => {
    console.log(`✓ Received MyInfo. Local node: ${formatHexId(client.selfNodeId)}`);

    // Allow some time for connection to stabilize
    setTimeout(() => {
        console.log(`→ Sending traceroute request to ${targetNode}...`);
        client.sendTraceroute(targetNode);
    }, 2000);
});

// Logs from client
client.on('traceroute-log', (log) => {
    if (log.tag !== 'DEBUG') {
        console.log(`[LOG:${log.tag}] ${log.message}`);
    } else {
        // Uncomment to see debug logs from client internals
        console.log(`[LOG:${log.tag}] ${log.message}`);
    }
});

// Monitor ALL relevant packets
client.on('fromRadio', ({ message }) => {
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
        if (portnum === 5 || portnum === 70 || portnum === 'ROUTING_APP' || portnum === 'TRACEROUTE_APP') {
            const elapsed = ((Date.now() - testStartTime) / 1000).toFixed(1);

            console.log(`\n[${elapsed}s] !!! ${portName} PACKET RECEIVED !!!`);
            console.log(`  from: ${from} (${formatHexId(from)})`);
            // Dump the decoded payload
            console.log('  decoded:', JSON.stringify(message.packet.decoded, null, 2));
        }
    }
});

client.on('traceroute', (entry) => {
    const elapsed = ((Date.now() - testStartTime) / 1000).toFixed(1);
    console.log(`\n[${elapsed}s] ★★★ TRACEROUTE EVENT EMITTED ★★★`);
    console.log(`  status: ${entry.status}`);
    console.log(`  hops: ${entry.hops}`);

    if (entry.status === 'success') {
        console.log('\n✓✓✓ SUCCESS! Traceroute complete!');
        client.stop();
        process.exit(0);
    }
});

client.on('summary', (summary) => {
    if (summary.type === 'Traceroute' || summary.type === 'RouteReply') {
        console.log(`\n➜ SUMMARY EVENT: ${summary.type}`);
        if (summary.trace) {
            console.log('  Has Trace Data:', JSON.stringify(summary.trace, null, 2));
        }
    }
});

client.on('error', (err) => {
    console.error('✗ Client error:', err);
    process.exit(1);
});

// Setup connection
try {
    client.connect({
        host: '10.37.45.18',
        port: 4403,
        tls: false
    });

    // Timeout
    setTimeout(() => {
        console.log('\n✗ TIMEOUT after 45 seconds.');
        client.stop();
        process.exit(1);
    }, 45000);
} catch (error) {
    console.error('Failed to start client:', error);
}
