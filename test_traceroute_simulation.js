'use strict';

console.log('Script started');
const MeshtasticClient = require('./src/meshtasticClient');
const protobuf = require('protobufjs');
const path = require('path');

const client = new MeshtasticClient({
    transport: 'tcp',
    host: '127.0.0.1',
    port: 4403,
    tracerouteEnabled: true
});

async function run() {
    console.log('Loading protos...');
    try {
        const root = new protobuf.Root();
        root.resolvePath = function (origin, target) {
            // "origin" is the file requesting the import
            // "target" is the string in the import statement, e.g. "meshtastic/channel.proto"
            return path.resolve(__dirname, 'proto', target);
        };

        await root.load('meshtastic/mesh.proto');
        console.log('Protos loaded');

        const Routing = root.lookupType('meshtastic.Routing');
        const RouteDiscovery = root.lookupType('meshtastic.RouteDiscovery');

        client.types = {
            routing: Routing,
            routeDiscovery: RouteDiscovery
        };
        client.portEnum = {
            values: { ROUTING_APP: 5, TRACEROUTE_APP: 70 },
            valuesById: { 5: 'ROUTING_APP', 70: 'TRACEROUTE_APP' }
        };
        client.nodeMap = new Map();

        // Mock _formatNode to return simple labels
        client._formatNode = (num) => ({ label: `!${(num >>> 0).toString(16)}` });

        console.log('Injecting packet...');

        const routeReply = {
            route: [0x11111111, 0x22222222, 0x33333333],
            routeBack: [0x33333333, 0x22222222, 0x11111111],
            snrTowards: [10, 8, 5],
            snrBack: [6, 9, 11]
        };
        const routingMessage = { routeReply };
        const encodedRouting = Routing.encode(routingMessage).finish();

        // Test parsing
        const result = client._decodePortPayload('ROUTING_APP', encodedRouting);
        console.log('Result:', JSON.stringify(result, null, 2));

        const hasRoute = result && result.trace && result.trace.route && result.trace.route.length === 3;
        const hasRouteBack = result && result.trace && result.trace.routeBack && result.trace.routeBack.length === 3;

        if (hasRoute && hasRouteBack) {
            console.log('✓ SUCCESS: Captured both outbound and inbound routes!');
            process.exit(0);
        } else {
            console.log('✗ FAILURE: Missing route data');
            process.exit(1);
        }

    } catch (err) {
        console.error('Error:', err);
        process.exit(1);
    }
}

run();
