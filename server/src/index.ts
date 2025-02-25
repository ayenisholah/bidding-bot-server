// ... existing code ...

try {
  // Add proper error handling and job completion
  const worker = new Worker(
    queueName,
    async (job) => {
      try {
        await processJob(job);
        // Explicitly return to mark job as completed
        return;
      } catch (error) {
        // Properly handle errors and mark job as failed
        if (error instanceof UnrecoverableError) {
          await job.moveToFailed(error, null, true);
        } else {
          throw error; // Let BullMQ retry recoverable errors
        }
      }
    },
    {
      connection,
      concurrency: 4,
      lockDuration: 30000, // Increase lock duration to prevent premature lock releases
    }
  );

  // Add proper error handling for the worker
  worker.on('failed', (job, err) => {
    console.error(`Job ${job.id} failed:`, err);
  });

  worker.on('error', err => {
    console.error('Worker error:', err);
  });

} catch (error) {
  console.error('Worker setup error:', error);
}

// ... existing code ...