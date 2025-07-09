import { Queue, Job } from 'bullmq';
import Listing, { IListing } from '../models/listing.model';
import { createListing } from './list';
import { logger, ErrorCategory } from '../utils/logger';
import WebSocketManager from '../utils/websocket-manager';
import { queue } from '..';

// Use the main queue from index.ts
let mainQueue: Queue | null = null;

// Export a function to get the main queue
export async function getMainQueue(): Promise<Queue> {
  if (!mainQueue) {
    // Import the queue from index.ts dynamically to avoid circular dependency
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
      'PROCESS_LISTING',
      { listingId },
      {
        jobId: `${jobId}-immediate`,
        removeOnComplete: true,
        removeOnFail: false, // Keep failed jobs for debugging
        attempts: 3, // Retry failed jobs 3 times
        backoff: {
          type: 'exponential',
          delay: 2000, // Start with 2 second delay
        },
      }
    );

    // Then add the repeating job for future executions
    const job = await queue.add(
      'PROCESS_LISTING',
      { listingId },
      {
        jobId,
        removeOnComplete: true,
        removeOnFail: false, // Keep failed jobs for debugging
        attempts: 3, // Retry failed jobs 3 times
        backoff: {
          type: 'exponential',
          delay: 2000, // Start with 2 second delay
        },
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
      if (scheduler.name === 'PROCESS_LISTING') {
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
export async function processListingJob(job: Job): Promise<boolean> {
  console.log('\n=== PROCESS LISTING JOB STARTED ===');
  const { listingId } = job.data;
  console.log('Job ID:', job.id);
  console.log('Listing ID:', listingId);

  try {
    console.log('Step 1: Updating progress to 10...');
    // Update progress to prevent stalling
    await job.updateProgress(10);

    console.log('Step 2: Logging job start...');
    await logger.logQueueJobStarted(job.name, {
      jobId: job.id,
      listingId,
      queueName: 'BIDDING_BOT',
    });

    console.log('Step 3: Updating progress to 20...');
    // Update progress
    await job.updateProgress(20);

    console.log('Step 4: Fetching listing from database...');
    // Fetch latest listing data
    const listing = await Listing.findById(listingId);
    console.log('Fetched listing:', listing ? 'Found' : 'Not found');
    if (listing) {
      console.log('Listing running status:', listing.running);
      console.log('Listing selectedMarketplaces:', listing.selectedMarketplaces);
    }

    if (!listing) {
      console.log('!!! LISTING NOT FOUND - RETURNING EARLY !!!');
      await logger.logInfo(ErrorCategory.QUEUE, `Listing ${listingId} not found, canceling job`);
      // Cancel the repeating job since the listing no longer exists
      await cancelListingJob(listingId);
      return false // Return successfully so the job doesn't retry
    }

    if (!listing.running) {
      console.log('!!! LISTING NOT RUNNING - RETURNING EARLY !!!');
      await logger.logInfo(ErrorCategory.QUEUE, `Listing ${listingId} is not running, skipping`);
      // Cancel the repeating job
      await cancelListingJob(listingId);
      return false;
    }

    console.log('Step 5: Listing is valid and running, continuing...');
    console.log('About to check instant price matching...');

    console.log('About to update progress to 50...');
    // Update progress before long operation
    await job.updateProgress(50);
    console.log('Progress updated to 50');

    // Fetch fresh listing data again after cleanup to ensure we have the latest state
    const freshListing = await Listing.findById(listingId);
    if (!freshListing) {
      await logger.logInfo(ErrorCategory.QUEUE, `Listing ${listingId} not found after cleanup, skipping`);
      return false;
    }

    // Process the listing - pass all fresh data including instantPriceOverride if available
    const listingData = {
      ...freshListing.toObject(),
    };
    console.log('\n=== ABOUT TO CALL CREATE LISTING ===');
    console.log('Listing ID:', listingId);
    console.log('Running:', freshListing.running);
    console.log('Selected Marketplaces:', freshListing.selectedMarketplaces);
    console.log('Listing data keys:', Object.keys(listingData));

    console.log('\n=== CALLING CREATE LISTING FROM SCHEDULER ===');
    await createListing(listingData);
    console.log('=== CREATE LISTING RETURNED ===');

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

    return true

  } catch (error) {
    console.error('!!! ERROR IN PROCESS LISTING JOB !!!');
    console.error('Error type:', error?.constructor?.name);
    console.error('Error message:', error instanceof Error ? error.message : String(error));
    console.error('Error stack:', error instanceof Error ? error.stack : 'No stack trace');

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
 * - Subscribes to WebSocket for instant price matching
 * - Immediately processes listings on startup
 */
export async function initializeListingScheduler(wsManager?: WebSocketManager): Promise<void> {
  try {
    console.log('Initializing listing scheduler...');

    // Get the main queue
    const queue = await getMainQueue();

    // Clean up old listing jobs
    const jobSchedulers = await queue.getJobSchedulers(0, -1, true);
    for (const scheduler of jobSchedulers) {
      if (scheduler.name === 'PROCESS_LISTING' && scheduler.id) {
        await queue.removeJobScheduler(scheduler.id);
      }
    }

    // Find all active listings
    const activeListings = await Listing.find({ running: true });

    console.log(`Found ${activeListings.length} active listings to schedule`);

    // Process each active listing
    for (const listing of activeListings) {
      const listingId = listing._id.toString();

      // 1. Subscribe to WebSocket if instant price matching is enabled
      if (listing.instantPriceMatching && wsManager) {
        try {
          const contractAddress = listing.contract.contractAddress;
          const contractSlug = listing.contract.slug;

          console.log(`Subscribing to WebSocket for instant price matching: ${contractSlug} (${contractAddress})`);

          // Subscribe to all marketplaces for this listing
          const subscriptionResults = [];
          for (const marketplace of listing.selectedMarketplaces) {
            try {
              await wsManager.subscribe(
                contractSlug,
                contractAddress,
                marketplace.toLowerCase(),
                'listing' // subscription type
              );
              subscriptionResults.push(`${marketplace}: success`);
              console.log(`✅ Successfully subscribed to ${marketplace} for listing events`);
            } catch (error) {
              subscriptionResults.push(`${marketplace}: failed`);
              console.error(`❌ Failed to subscribe to ${marketplace}:`, error);
            }
          }

          await logger.logInfo(ErrorCategory.QUEUE,
            `Subscribed to WebSocket for listing ${listingId} with instant price matching`,
            {
              contractSlug,
              contractAddress,
              tokenId: listing.tokenId,
              marketplaces: listing.selectedMarketplaces,
              subscriptionResults,
              instantPriceMatchingActive: listing.instantPriceMatching
            }
          );
        } catch (error) {
          await logger.logWebSocketError('Failed to subscribe listing to WebSocket', {
            listingId,
            error: error as Error,
            errorMessage: error instanceof Error ? error.message : String(error)
          });
        }
      }

      // 2. Schedule the listing jobs
      await scheduleListing(listing);

      // 3. Immediately call createListing to list the NFT
      try {
        console.log(`\n=== INITIAL LISTING ON STARTUP ===`);
        console.log(`Listing ID: ${listingId}`);
        console.log(`Token ID: ${listing.tokenId}`);
        console.log(`Running: ${listing.running}`);
        console.log(`Instant Price Matching: ${listing.instantPriceMatching}`);

        const listingData = listing.toObject();
        await createListing(listingData);

        await logger.logInfo(ErrorCategory.QUEUE,
          `Processed initial listing for ${listingId} on startup`
        );
      } catch (error) {
        await logger.logGeneralError('Failed to process initial listing on startup', {
          listingId,
          error: error as Error,
          errorMessage: error instanceof Error ? error.message : String(error)
        });
      }
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
      if (scheduler.name === 'PROCESS_LISTING' && scheduler.id) {
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

/**
 * Get listing subscription diagnostics
 */
export async function getListingSubscriptionDiagnostics(wsManager?: WebSocketManager): Promise<{
  activeListings: number;
  instantPriceMatchingListings: number;
  subscriptionStats: any;
  connectionStatus: boolean;
}> {
  try {
    // Get all active listings with instant price matching
    const instantPriceMatchingListings = await Listing.find({
      running: true,
      instantPriceMatching: true
    }).select('tokenId contract selectedMarketplaces');

    // Get WebSocket subscription stats
    const subscriptionStats = wsManager ? wsManager.getListingSubscriptionStats() : null;
    const connectionStatus = wsManager ? wsManager.isConnected() : false;

    // Log diagnostics
    await logger.logInfo(ErrorCategory.WEBSOCKET, 'Listing subscription diagnostics', {
      activeListings: instantPriceMatchingListings.length,
      subscriptionStats,
      connectionStatus,
      listings: instantPriceMatchingListings.map(l => ({
        tokenId: l.tokenId,
        contract: l.contract,
        marketplaces: l.selectedMarketplaces
      }))
    });

    return {
      activeListings: instantPriceMatchingListings.length,
      instantPriceMatchingListings: instantPriceMatchingListings.length,
      subscriptionStats,
      connectionStatus
    };
  } catch (error) {
    await logger.logWebSocketError('Failed to get listing subscription diagnostics', {
      error: error as Error
    });
    throw error;
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