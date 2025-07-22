const http = require('http');

// Function to make API request
function makeRequest(path, method = 'GET', data = null) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: 'localhost',
      port: 3003,
      path: path,
      method: method,
      headers: {
        'Content-Type': 'application/json',
      }
    };

    const req = http.request(options, (res) => {
      let body = '';
      res.on('data', (chunk) => body += chunk);
      res.on('end', () => {
        try {
          resolve({ status: res.statusCode, data: JSON.parse(body) });
        } catch (e) {
          resolve({ status: res.statusCode, data: body });
        }
      });
    });

    req.on('error', reject);
    
    if (data) {
      req.write(JSON.stringify(data));
    }
    req.end();
  });
}

async function testMarketplaceOperations() {
  console.log('=== Testing Marketplace-Specific Operations ===\n');
  
  try {
    // 1. Get current tasks
    console.log('1. Fetching current tasks...');
    const tasksRes = await makeRequest('/api/tasks');
    if (tasksRes.status !== 200 || !tasksRes.data.tasks || tasksRes.data.tasks.length === 0) {
      console.log('No tasks found. Please ensure you have at least one running task.');
      return;
    }
    
    const task = tasksRes.data.tasks[0];
    console.log(`Found task: ${task._id} (${task.contract.slug})`);
    console.log(`Current status: ${task.running ? 'RUNNING' : 'STOPPED'}`);
    console.log(`Selected marketplaces: ${task.selectedMarketplaces.join(', ')}`);
    
    // 2. Get task status
    console.log('\n2. Checking task status...');
    const statusRes = await makeRequest('/api/task-status/' + task._id);
    if (statusRes.status === 200) {
      console.log('Task status:', JSON.stringify(statusRes.data, null, 2));
    }
    
    // 3. Test marketplace-specific stop
    const testMarketplace = task.selectedMarketplaces[0];
    console.log(`\n3. Testing marketplace-specific stop for ${testMarketplace}...`);
    console.log('This should:');
    console.log('  - Store current state as previousState');
    console.log('  - Transition to CANCELLING');
    console.log('  - When complete, restore previous state');
    console.log('  - Preserve runningPercentage, nextLoopTime, waitingTime');
    
    console.log('\nPress Enter to continue with the test...');
    await new Promise(resolve => process.stdin.once('data', resolve));
    
    // Note: You'll need to trigger the stopTask via WebSocket or add an API endpoint
    console.log('To test, use the client UI to stop a specific marketplace while watching the server logs.');
    console.log('Look for:');
    console.log('  - "[stopTask] Marketplace-specific stop" message');
    console.log('  - "[setMarketplaceCancellationComplete] Called" message');
    console.log('  - "Restoring task to previous state" message');
    
  } catch (error) {
    console.error('Error:', error);
  }
}

// Add endpoint to check task status (add this to your server)
console.log('Add this endpoint to your server for testing:\n');
console.log(`
app.get('/api/task-status/:taskId', async (req, res) => {
  const { taskId } = req.params;
  const status = taskStatus[taskId];
  if (status) {
    res.json({
      ...status,
      hasActiveTasks: activeTasks.has(taskId),
      completedTasks: completedTasks[taskId]
    });
  } else {
    res.status(404).json({ error: 'Task status not found' });
  }
});
`);

testMarketplaceOperations();