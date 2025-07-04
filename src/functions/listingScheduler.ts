import { Queue, Worker, Job, QueueEvents } from 'bullmq';
import { createBullMQConnection } from '../utils/redis';
import Listing, { IListing } from '../models/listing.model';
import { createListing } from './list';
import { logger, ErrorCategory } from '../utils/logger';

// Use hash tags to ensure all keys for this queue go to the same Redis slot
const LISTING_QUEUE_NAME = 'listing-scheduler';
const WORKER_CONCURRENCY = 5;

// Queue configuration
let connection: any;
let listingQueue: Queue | null = null;
let queueEvents: QueueEvents | null = null;

// Initialize queue instances asynchronously
async function initializeQueue() {
  if (!connection) {
    connection = await createBullMQConnection();

    const queueOptions = {
      connection,
      prefix: '{listing}',
      defaultJobOptions: {
        attempts: 3,
        backoff: {
          type: 'exponential',
          delay: 5000,
        },
        removeOnComplete: true,
        removeOnFail: true,
      },
    };

    listingQueue = new Queue(LISTING_QUEUE_NAME, queueOptions);
    queueEvents = new QueueEvents(LISTING_QUEUE_NAME, { connection, prefix: '{listing}' });
  }
  return { listingQueue, queueEvents };
}

// Export a function to get the queue
export async function getListingQueue(): Promise<Queue> {
  if (!listingQueue) {
    await initializeQueue();
  }
  if (!listingQueue) {
    throw new Error('Failed to initialize listing queue');
  }
  return listingQueue;
}

// Track active jobs for each listing
const activeJobs = new Map<string, string>();

/**
 * Schedule a listing to run at specified intervals
 */
export async function scheduleListing(listing: IListing): Promise<void> {
  try {
    const queue = await getListingQueue();
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

    // Add job to queue
    const job = await queue.add(
      'process-listing',
      { listingId },
      {
        jobId,
        repeat: {
          every: delayMs,
        },
      }
    );

    activeJobs.set(listingId, job.id!);

    await logger.logQueueJobAdded(jobId, {
      taskId: listingId,
      queueName: LISTING_QUEUE_NAME,
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
    const queue = await getListingQueue();
    
    // Always remove any repeatable jobs for this listing
    const repeatableJobs = await queue.getRepeatableJobs(0, -1, true);
    let removedRepeatable = false;
    
    for (const repeatJob of repeatableJobs) {
      // Check if this is a listing job by examining the data
      if (repeatJob.name === 'process-listing') {
        // Parse the job key to check if it's for this listing
        // The key format includes the listingId in the pattern
        if (repeatJob.key.includes(listingId) || 
            (repeatJob.id && repeatJob.id.includes(listingId))) {
          // Use removeRepeatableByKey which is simpler and still works
          await queue.removeRepeatableByKey(repeatJob.key);
          await logger.logInfo(ErrorCategory.QUEUE, `Removed repeatable job for listing ${listingId}`);
          removedRepeatable = true;
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
 * Initialize worker after connection is established
 */
let listingWorker: Worker | null = null;

async function initializeWorker() {
  if (!listingWorker && connection) {
    listingWorker = new Worker(
      LISTING_QUEUE_NAME,
      async (job: Job) => {
        const { listingId } = job.data;

        try {
          // Update progress to prevent stalling
          await job.updateProgress(10);
          
          await logger.logQueueJobStarted(job.name, {
            jobId: job.id,
            listingId,
            queueName: LISTING_QUEUE_NAME,
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
            queueName: LISTING_QUEUE_NAME,
          });

        } catch (error) {
          await logger.logQueueJobFailed(job.name, {
            jobId: job.id,
            listingId,
            queueName: LISTING_QUEUE_NAME,
            error: error as Error,
            errorMessage: error instanceof Error ? error.message : String(error),
          });

          throw error;
        }
      },
      {
        connection,
        concurrency: WORKER_CONCURRENCY,
        prefix: '{listing}',
        stalledInterval: 300000, // 5 minutes
        maxStalledCount: 1,
      }
    );

    // Worker event handlers
    listingWorker.on('completed', (job) => {
      console.log(`Listing job ${job.id} completed successfully`);
    });

    listingWorker.on('failed', (job, err) => {
      console.error(`Listing job ${job?.id} failed:`, err);
    });
  }

  return listingWorker;
}

// Initialize queue event handlers
async function initializeQueueEvents() {
  const { queueEvents: events } = await initializeQueue();

  if (!events) {
    throw new Error('Failed to initialize queue events');
  }

  events.on('waiting', ({ jobId }) => {
    console.log(`Listing job ${jobId} is waiting`);
  });

  events.on('active', ({ jobId }) => {
    console.log(`Listing job ${jobId} is active`);
  });

  events.on('completed', ({ jobId }) => {
    console.log(`Listing job ${jobId} completed`);
  });

  events.on('failed', ({ jobId, failedReason }) => {
    console.error(`Listing job ${jobId} failed: ${failedReason}`);
  });
}

/**
 * Initialize listing scheduler
 * - Loads all active listings and schedules them
 */
export async function initializeListingScheduler(): Promise<void> {
  try {
    console.log('Initializing listing scheduler...');

    // Initialize queue first
    await initializeQueue();

    // Initialize worker and queue events
    await initializeWorker();
    await initializeQueueEvents();

    // Clean up old jobs
    const queue = await getListingQueue();
    await queue.drain();

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

    if (listingWorker) {
      await listingWorker.close();
      listingWorker = null;
    }

    if (queueEvents) {
      await queueEvents.close();
      queueEvents = null;
    }

    if (listingQueue) {
      await listingQueue.close();
      listingQueue = null;
    }

    if (connection) {
      // Don't close the connection as it might be shared
      connection = null;
    }

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