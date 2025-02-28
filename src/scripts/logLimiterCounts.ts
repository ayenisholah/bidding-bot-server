import { initialize, startCountsLogger } from '../init';

async function main() {
  // Initialize the limiter with the rate limit from environment variable or default to 10
  const rateLimit = process.env.RATE_LIMIT ? parseInt(process.env.RATE_LIMIT) : 10;
  await initialize(rateLimit);

  console.log(`Starting limiter counts logger with rate limit: ${rateLimit}`);

  // Start logging the limiter counts every second
  startCountsLogger();

  // Keep the process running
  console.log('Press Ctrl+C to stop logging');
}

// Run the script
main().catch(error => {
  console.error('Error starting limiter counts logger:', error);
  process.exit(1);
});

// Handle graceful shutdown
process.on('SIGINT', () => {
  console.log('Stopping limiter counts logger...');
  process.exit(0);
}); 