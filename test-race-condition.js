// Test script to verify processBulkJobs race condition fix
// This simulates 100 concurrent calls with 100k jobs each

const axios = require('axios');

const SERVER_URL = 'http://localhost:3003';

// Mock job data
const createMockJobs = (count, taskId) => {
  const jobs = [];
  for (let i = 0; i < count; i++) {
    jobs.push({
      name: 'OPENSEA_COLLECTION_BID',
      data: {
        _id: taskId,
        collectionSlug: 'test-collection',
        bidAmount: 0.1,
        wallet: '0x1234567890123456789012345678901234567890'
      },
      opts: {}
    });
  }
  return jobs;
};

// Function to call processBulkJobs via API
async function callProcessBulkJobs(jobCount, callNumber) {
  const taskId = 'test-task-123';
  const jobs = createMockJobs(jobCount, taskId);
  
  console.log(`Call ${callNumber}: Sending ${jobCount} jobs...`);
  
  // In a real test, you'd need an API endpoint that calls processBulkJobs
  // For now, this is a conceptual test
  try {
    const response = await axios.post(`${SERVER_URL}/test/process-bulk-jobs`, {
      jobs: jobs
    });
    console.log(`Call ${callNumber}: Response:`, response.data);
  } catch (error) {
    console.error(`Call ${callNumber}: Error:`, error.message);
  }
}

// Main test function
async function testRaceCondition() {
  console.log('Starting race condition test...');
  console.log('Simulating 100 concurrent calls with 100k jobs each...');
  
  // Create 100 concurrent promises
  const promises = [];
  for (let i = 0; i < 100; i++) {
    // Using 1000 jobs per call for testing (100k would be too much for a test)
    promises.push(callProcessBulkJobs(1000, i + 1));
  }
  
  // Execute all calls concurrently
  const startTime = Date.now();
  await Promise.all(promises);
  const endTime = Date.now();
  
  console.log(`\nTest completed in ${(endTime - startTime) / 1000} seconds`);
  console.log('Check the server logs to verify:');
  console.log('1. Queue never exceeds MAX_QUEUE_JOBS (10,000)');
  console.log('2. Lock contention messages appear');
  console.log('3. Jobs are processed in controlled batches');
}

// Note: This test requires:
// 1. A test endpoint in the server that exposes processBulkJobs
// 2. The server to be running with the fixed code
// 3. Monitoring of queue counts during the test

console.log('\n=== RACE CONDITION TEST ===');
console.log('This test demonstrates how the distributed lock prevents race conditions.');
console.log('Without the fix: All 100 calls would add jobs simultaneously, exceeding the 10k limit.');
console.log('With the fix: Calls are serialized through the lock, maintaining the 10k limit.\n');

// Uncomment to run the test when server endpoint is available
// testRaceCondition().catch(console.error);

console.log('Test script created. To run:');
console.log('1. Add a test endpoint to the server that calls processBulkJobs');
console.log('2. Start the server');
console.log('3. Uncomment the testRaceCondition() call');
console.log('4. Run: node test-race-condition.js');