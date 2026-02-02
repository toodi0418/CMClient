'use strict';

const MeshtasticClient = require('./src/meshtasticClient');

const host = '10.37.45.18';

const client = new MeshtasticClient({
    transport: 'tcp',
    host: host,
    port: 4403
});

console.log('[TEST] Testing basic connectivity...');

client.on('connected', () => {
    console.log(`[TEST] Connected to Meshtastic at ${host}:${client.options.port}`);
});

client.on('myInfo', async (info) => {
    console.log(`[TEST] Received MyInfo. Local node: !${(info.raw >>> 0).toString(16).padStart(8, '0')}`);
    console.log('[TEST] Sending test message...');

    try {
        await client.sendTextMessage({
            text: 'Test message from CMClient',
            destination: 0xFFFFFFFF, // broadcast
            channel: 0,
            wantAck: false
        });
        console.log('[TEST] Message sent successfully!');
    } catch (err) {
        console.error('[TEST] Failed to send message:', err.message);
    }

    // Exit after 5 seconds
    setTimeout(() => {
        console.log('[TEST] Test complete.');
        client.stop();
        process.exit(0);
    }, 5000);
});

client.on('summary', (summary) => {
    if (summary.type === 'Text') {
        console.log(`[MESSAGE] from=${summary.from?.label}, text="${summary.detail}"`);
    }
});

client.on('error', (err) => {
    console.error(`[ERROR] ${err.message}`);
});

client.start();
