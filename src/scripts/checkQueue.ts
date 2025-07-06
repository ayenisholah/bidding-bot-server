import { Queue } from 'bullmq';
import { config } from 'dotenv';
import { createBullMQConnection } from '../utils/redis';

config();

async function checkQueue() {
  try {
    const connection = await createBullMQConnection();
    const queue = new Queue('BIDDING_BOT', { connection });

    // Get queue status
    const isPaused = await queue.isPaused();
    console.log('Queue paused:', isPaused);

    // Get job counts
    const counts = await queue.getJobCounts();
    console.log('\nJob counts:', counts);

    // Get recent jobs
    const waitingJobs = await queue.getWaiting(0, 10);
    const activeJobs = await queue.getActive(0, 10);
    const completedJobs = await queue.getCompleted(0, 10);
    const failedJobs = await queue.getFailed(0, 10);

    console.log('\nWaiting jobs:', waitingJobs.length);
    waitingJobs.forEach(job => {
      console.log(`  - ${job.name} (ID: ${job.id})`);
      console.log(`    Data:`, job.data);
    });

    console.log('\nActive jobs:', activeJobs.length);
    activeJobs.forEach(job => {
      console.log(`  - ${job.name} (ID: ${job.id})`);
      console.log(`    Data:`, job.data);
    });

    console.log('\nFailed jobs:', failedJobs.length);
    failedJobs.forEach(job => {
      console.log(`  - ${job.name} (ID: ${job.id})`);
      console.log(`    Data:`, job.data);
      console.log(`    Failed reason:`, job.failedReason);
    });

    // Check for process-listing jobs specifically
    const allJobs = await queue.getJobs(['waiting', 'active', 'completed', 'failed']);
    const listingJobs = allJobs.filter(job => job.name === 'process-listing');
    console.log('\nTotal process-listing jobs:', listingJobs.length);
    listingJobs.forEach(job => {
      console.log(`  - State: ${job.getState()}`, `ID: ${job.id}`, `Data:`, job.data);
    });

    // Check repeatable jobs
    const repeatableJobs = await queue.getRepeatableJobs();
    console.log('\nRepeatable jobs:', repeatableJobs.length);
    repeatableJobs.forEach(job => {
      console.log(`  - ${job.name} (Key: ${job.key})`);
    });

    await queue.close();
    process.exit(0);
  } catch (error) {
    console.error('Error:', error);
    process.exit(1);
  }
}

checkQueue();