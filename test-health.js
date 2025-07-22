const http = require('http');

const options = {
  hostname: 'localhost',
  port: 3003,
  path: '/health',
  method: 'GET'
};

console.log('Checking server health...\n');

const req = http.request(options, (res) => {
  let data = '';

  res.on('data', (chunk) => {
    data += chunk;
  });

  res.on('end', () => {
    try {
      const health = JSON.parse(data);
      console.log('=== SERVER HEALTH CHECK ===');
      console.log(`Status: ${health.status}`);
      console.log(`Uptime: ${Math.round(health.uptime)} seconds`);
      console.log(`Password Set: ${health.passwordSet ? '✅ YES' : '❌ NO'}`);
      console.log('\nComponents:');
      console.log(`  Redis: ${health.components.redis}`);
      console.log(`  MongoDB: ${health.components.mongodb}`);
      console.log('\nQueue Metrics:');
      Object.entries(health.metrics.queue).forEach(([key, value]) => {
        console.log(`  ${key}: ${value}`);
      });
      console.log('\nMemory Usage:');
      Object.entries(health.metrics.memory).forEach(([key, value]) => {
        console.log(`  ${key}: ${value}`);
      });
    } catch (error) {
      console.error('Failed to parse response:', error);
      console.log('Raw response:', data);
    }
  });
});

req.on('error', (error) => {
  console.error('Request failed:', error.message);
});

req.end();