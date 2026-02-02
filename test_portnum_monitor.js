'use strict';

const MeshtasticClient = require('./src/meshtasticClient');

const host = '10.37.45.18';
const targetNode = '!03919375';

const client = new MeshtasticClient({
    transport: 'tcp',
    host: host,
    port: 4403,
    tracerouteEnabled: false // Disable auto-traceroute
});

const portnumStats = {};

console.log('[TEST] Monitoring all packets for 60 seconds...');

client.on('connected', () => {
    console.log(`[TEST] Connected to Meshtastic at ${host}:${client.options.port}`);
});

client.on('myInfo', async (info) => {
    console.log(`[TEST] Received MyInfo. Local node: !${(info.raw >>> 0).toString(16).padStart(8, '0')}`);
    console.log(`[TEST] Now sending traceroute to ${targetNode}...`);

    const destId = parseInt(targetNode.replace('!', ''), 16);
    await client.sendTraceroute(destId);
    console.log('[TEST] Traceroute sent. Monitoring packets for 60 seconds...');

    // Report stats after 60 seconds
    setTimeout(() => {
        console.log('\n[STATS] Portnum distribution:');
        const sorted = Object.entries(portnumStats).sort((a, b) => b[1] - a[1]);
        for (const [portnum, count] of sorted) {
            console.log(`  ${portnum}: ${count} packets`);
        }
        client.stop();
        process.exit(0);
    }, 60000);
});

client.on('fromRadio', ({ message }) => {
    if (message.payloadVariant === 'packet' && message.packet?.decoded?.portnum != null) {
        const portnum = message.packet.decoded.portnum;
        portnumStats[portnum] = (portnumStats[portnum] || 0) + 1;

        // Log TRACEROUTE_APP packets immediately
        if (portnum === 70) {
            console.log(`[!!! TRACEROUTE_APP !!!] from=${message.packet.from}, to=${message.packet.to}`);
        }
    }
});

client.on('error', (err) => {
    console.error(`[ERROR] ${err.message}`);
});

client.start();
