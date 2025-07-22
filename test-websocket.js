const WebSocket = require('ws');

console.log('Testing WebSocket connection behavior...\n');

// Test connection to the WebSocket URL
const wsUrl = 'wss://nfttools-ws-proxy.nfttools.io';
const timeout = 10000; // 10 seconds

console.log(`Attempting to connect to: ${wsUrl}`);
console.log(`Timeout: ${timeout}ms\n`);

const ws = new WebSocket(wsUrl);
let connected = false;

// Set up timeout
const connectionTimeout = setTimeout(() => {
  console.log('Connection timeout reached!');
  console.log(`WebSocket state: ${ws.readyState}`);
  console.log('States: 0=CONNECTING, 1=OPEN, 2=CLOSING, 3=CLOSED\n');
  
  if (ws.readyState === WebSocket.CONNECTING) {
    console.log('WebSocket is still in CONNECTING state');
    console.log('Using terminate() instead of close()...');
    ws.terminate();
  } else if (ws.readyState === WebSocket.OPEN) {
    console.log('WebSocket is OPEN, closing gracefully...');
    ws.close();
  }
  
  if (!connected) {
    console.log('\n❌ Failed to connect within timeout period');
    console.log('The server should handle this gracefully now');
    process.exit(1);
  }
}, timeout);

ws.on('open', () => {
  connected = true;
  clearTimeout(connectionTimeout);
  console.log('✅ WebSocket connected successfully!');
  ws.close();
  process.exit(0);
});

ws.on('error', (error) => {
  clearTimeout(connectionTimeout);
  console.log('❌ WebSocket error:', error.message);
  console.log('This is expected if the service is down');
  process.exit(1);
});

ws.on('close', () => {
  console.log('WebSocket closed');
});