import { Queue, Job } from 'bullmq';
import Listing, { IListing } from '../models/listing.model';
import { createListing } from './list';
import { logger, ErrorCategory } from '../utils/logger';

// Use the main queue from index.ts
let mainQueue: Queue | null = null;

// Export a function to get the main queue
export async function getMainQueue(): Promise<Queue> {
  if (!mainQueue) {
    // Import the queue from index.ts dynamically to avoid circular dependency
    const { queue } = await import('../index');
    mainQueue = queue;
  }
  if (!mainQueue) {
    throw new Error('Main queue not initialized');
  }
  return mainQueue;
}

// Track active jobs for each listing
const activeJobs = new Map<string, string>();

/**
 * Schedule a listing to run at specified intervals
 */
export async function scheduleListing(listing: IListing): Promise<void> {
  try {
    const queue = await getMainQueue();
    const listingId = listing._id.toString();
    const jobId = `listing-${listingId}`;

    // Cancel existing job if any
    await cancelListingJob(listingId);

    if (!listing.running) {
      await logger.logInfo(ErrorCategory.QUEUE, `Listing ${listingId} is not running, skipping schedule`);
      return;
    }

    // Calculate delay in milliseconds
    const delayMs = convertToSeconds(listing.loopInterval.value, listing.loopInterval.unit) * 1000;

    // Execute immediately first, then set up repeating job
    await queue.add(
      'process-listing',
      { listingId },
      {
        jobId: `${jobId}-immediate`,
        removeOnComplete: true,
        removeOnFail: true
      }
    );

    // Then add the repeating job for future executions
    const job = await queue.add(
      'process-listing',
      { listingId },
      {
        jobId,
        removeOnComplete: true,
        removeOnFail: true,
        repeat: {
          every: delayMs,
        },
      }
    );

    activeJobs.set(listingId, job.id!);

    await logger.logQueueJobAdded(jobId, {
      taskId: listingId,
      queueName: 'BIDDING_BOT', // Use main queue name
      delay: delayMs,
    });
  } catch (error) {
    await logger.logQueueError('Failed to schedule listing', {
      listingId: listing._id.toString(),
      error: error as Error,
      errorMessage: error instanceof Error ? error.message : String(error),
    });
  }
}

/**
 * Cancel a scheduled listing job
 */
export async function cancelListingJob(listingId: string): Promise<void> {
  try {
    const queue = await getMainQueue();

    // Always remove any repeatable jobs for this listing
    const jobSchedulers = await queue.getJobSchedulers(0, -1, true);
    let removedRepeatable = false;

    for (const scheduler of jobSchedulers) {
      // Check if this is a listing job by examining the data
      if (scheduler.name === 'process-listing') {
        // Parse the job key to check if it's for this listing
        // The key format includes the listingId in the pattern
        if (scheduler.key.includes(listingId) ||
          (scheduler.id && scheduler.id.includes(listingId))) {
          // Use the new removeJobScheduler method
          if (scheduler.id) {
            await queue.removeJobScheduler(scheduler.id);
            await logger.logInfo(ErrorCategory.QUEUE, `Removed repeatable job for listing ${listingId}`);
            removedRepeatable = true;
          }
        }
      }
    }

    // Also check if we have a jobId in our map
    const jobId = activeJobs.get(listingId);
    if (jobId && !jobId.startsWith('repeat:')) {
      // Only try to remove non-repeatable jobs
      const job = await queue.getJob(jobId);
      if (job) {
        await job.remove();
        await logger.logInfo(ErrorCategory.QUEUE, `Removed job ${jobId} for listing ${listingId}`);
      }
    }

    // Also try to remove immediate job if it exists
    const immediateJobId = `listing-${listingId}-immediate`;
    const immediateJob = await queue.getJob(immediateJobId);
    if (immediateJob) {
      await immediateJob.remove();
      await logger.logInfo(ErrorCategory.QUEUE, `Removed immediate job ${immediateJobId} for listing ${listingId}`);
    }

    // Clean up the active jobs map
    activeJobs.delete(listingId);

    if (!removedRepeatable && !jobId) {
      await logger.logInfo(ErrorCategory.QUEUE, `No jobs found to cancel for listing ${listingId}`);
    }
  } catch (error) {
    await logger.logQueueError('Failed to cancel listing job', {
      listingId,
      error: error as Error,
      errorMessage: error instanceof Error ? error.message : String(error),
    });
  }
}

/**
 * Update listing schedule (used when loopInterval changes)
 */
export async function updateListingSchedule(listing: IListing): Promise<void> {
  await scheduleListing(listing);
}

/**
 * Process a listing job - to be called from the main worker in index.ts
 */
export async function processListingJob(job: Job): Promise<void> {
  const { listingId } = job.data;

  try {
    // Update progress to prevent stalling
    await job.updateProgress(10);

    await logger.logQueueJobStarted(job.name, {
      jobId: job.id,
      listingId,
      queueName: 'BIDDING_BOT',
    });

    // Update progress
    await job.updateProgress(20);

    // Fetch latest listing data
    const listing = await Listing.findById(listingId);

    if (!listing) {
      await logger.logInfo(ErrorCategory.QUEUE, `Listing ${listingId} not found, canceling job`);
      // Cancel the repeating job since the listing no longer exists
      await cancelListingJob(listingId);
      return; // Return successfully so the job doesn't retry
    }

    if (!listing.running) {
      await logger.logInfo(ErrorCategory.QUEUE, `Listing ${listingId} is not running, skipping`);
      // Cancel the repeating job
      await cancelListingJob(listingId);
      return;
    }

    // Update progress before long operation
    await job.updateProgress(50);

    // Process the listing
    await createListing(listing);

    // Update progress
    await job.updateProgress(80);

    // Update last processed time
    await Listing.findByIdAndUpdate(listingId, {
      $set: {
        lastProcessedAt: new Date(),
      },
    });

    // Update progress to completion
    await job.updateProgress(100);

    await logger.logQueueJobCompleted(job.name, {
      jobId: job.id,
      listingId,
      queueName: 'BIDDING_BOT',
    });

  } catch (error) {
    await logger.logQueueJobFailed(job.name, {
      jobId: job.id,
      listingId,
      queueName: 'BIDDING_BOT',
      error: error as Error,
      errorMessage: error instanceof Error ? error.message : String(error),
    });

    throw error;
  }
}

/**
 * Initialize listing scheduler
 * - Loads all active listings and schedules them on the main queue
 */
export async function initializeListingScheduler(): Promise<void> {
  try {
    console.log('Initializing listing scheduler...');

    // Get the main queue
    const queue = await getMainQueue();

    // Clean up old listing jobs
    const jobSchedulers = await queue.getJobSchedulers(0, -1, true);
    for (const scheduler of jobSchedulers) {
      if (scheduler.name === 'process-listing' && scheduler.id) {
        await queue.removeJobScheduler(scheduler.id);
      }
    }

    // Find all active listings
    const activeListings = await Listing.find({ running: true });

    console.log(`Found ${activeListings.length} active listings to schedule`);

    // Schedule each active listing
    for (const listing of activeListings) {
      await scheduleListing(listing);
    }

    console.log('Listing scheduler initialized successfully');
  } catch (error) {
    await logger.logGeneralError('Failed to initialize listing scheduler', {
      error: error as Error,
      errorMessage: error instanceof Error ? error.message : String(error),
    });
  }
}

/**
 * Shutdown listing scheduler gracefully
 */
export async function shutdownListingScheduler(): Promise<void> {
  try {
    console.log('Shutting down listing scheduler...');

    // Cancel all active listing jobs
    const queue = await getMainQueue();
    const jobSchedulers = await queue.getJobSchedulers(0, -1, true);

    for (const scheduler of jobSchedulers) {
      if (scheduler.name === 'process-listing' && scheduler.id) {
        await queue.removeJobScheduler(scheduler.id);
      }
    }

    // Clear active jobs map
    activeJobs.clear();

    console.log('Listing scheduler shut down successfully');
  } catch (error) {
    console.error('Error shutting down listing scheduler:', error);
  }
}

// Helper function moved here to avoid circular dependencies
function convertToSeconds(value: number, unit: string): number {
  switch (unit.toLowerCase()) {
    case 'minutes':
      return value * 60;
    case 'hours':
      return value * 60 * 60;
    case 'days':
      return value * 60 * 60 * 24;
    default:
      return value; // assume already in seconds
  }
}