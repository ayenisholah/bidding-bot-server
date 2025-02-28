import { initialize, startCountsLogger, stopCountsLogger } from '../init';

async function main() {
  // Initialize the limiter with a rate limit (e.g., 10 requests per second)
  await initialize(10);

  // Start logging the limiter counts every second
  startCountsLogger();

  // Example: To stop the logger after 60 seconds
  setTimeout(() => {
    stopCountsLogger();
    console.log('Counts logger stopped after 60 seconds');
  }, 60000);
}

// Run the example
main().catch(error => {
  console.error('Error in count logger example:', error);
});

// To run this example: ts-node src/examples/countLogger.ts 