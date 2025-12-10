import { Queue, Job } from 'bullmq';
import Listing, { IListing } from '../models/listing.model';
import { createListing } from './list';
import { errorLog, warnLog } from '../utils/debug-file-logger';
import WebSocketManager from '../utils/websocket-manager';
import { PROCESS_LISTING, queue } from '..';
import { prefetchTokenTraits } from '../functions/tokenMetadata';
import { getPassword } from '../utils/crypto';

// Color constants for console output
const GREEN = '\x1b[32m';
const YELLOW = '\x1b[33m';
const RED = '\x1b[31m';
const BLUE = '\x1b[34m';
const RESET = '\x1b[0m';

// Use the main queue from index.ts
let mainQueue: Queue | null = null;

// Export a function to get the main queue
export async function getMainQueue(): Promise<Queue> {
  if (!mainQueue) {
    // Import the queue from index.ts dynamically to avoid circular dependency
    mainQueue = queue;
  }
  if (!mainQueue) {
    errorLog('Queue', '[listingScheduler.ts] Main queue not initialized');
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
  // console.log(BLUE + `[LISTING SCHEDULER] Listing ID: ${listing._id}` + RESET);
  // console.log(BLUE + `[LISTING SCHEDULER] Token ID: ${listing.tokenId}` + RESET);
  // console.log(BLUE + `[LISTING SCHEDULER] Timestamp: ${new Date().toISOString()}` + RESET);

  try {
    const queue = await getMainQueue();
    const listingId = listing._id.toString();

    // Cancel existing job if any
    await cancelListingJob(listingId);

    if (!listing.running) {
      // console.log(YELLOW + '[LISTING SCHEDULER] Schedule Step 3: Listing not running, returning early' + RESET);
      return;
    }

    // Calculate delay in milliseconds
    const delayMs = convertToSeconds(listing.loopInterval.value, listing.loopInterval.unit) * 1000;
    // console.log(BLUE + `[LISTING SCHEDULER] Schedule Step 3.1: Delay in ms: ${delayMs}` + RESET);

    // Add a single repeating job with immediate execution
    const job = await queue.add(
      PROCESS_LISTING,
      { listingId },
      {
        jobId: `listing-${listingId}`,
        removeOnComplete: true,
        removeOnFail: true,
        attempts: 3,
        priority: 2,
        backoff: {
          type: 'exponential',
          delay: 2000,
        },
        repeat: {
          every: delayMs,
          immediately: true, // Execute immediately on first add
        },
      }
    );

    // console.log(GREEN + `[LISTING SCHEDULER] Schedule Step 5: Job added with ID: ${job.id}` + RESET);
    activeJobs.set(listingId, job.id!);
    // console.log(GREEN + '[LISTING SCHEDULER] Schedule Step 6: scheduleListing completed successfully' + RESET);

  } catch (error) {
    errorLog('Listing', `Failed to schedule listing | listingId=${listing._id} | tokenId=${listing.tokenId}`, error);
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
      if (scheduler.name === PROCESS_LISTING) {
        // Parse the job key to check if it's for this listing
        // The key format includes the listingId in the pattern
        if (scheduler.key.includes(listingId) ||
          (scheduler.id && scheduler.id.includes(listingId))) {
          // Use the new removeJobScheduler method
          if (scheduler.id) {
            await queue.removeJobScheduler(scheduler.id);

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

      }
    }

    // Also try to remove immediate job if it exists
    const immediateJobId = `listing-${listingId}-immediate`;
    const immediateJob = await queue.getJob(immediateJobId);
    if (immediateJob) {
      await immediateJob.remove();

    }

    // Clean up the active jobs map
    activeJobs.delete(listingId);

    if (!removedRepeatable && !jobId) {
      // console.log(`No active job found for listing ${listingId}`);
    }
  } catch (error) {
    errorLog('Listing', `[listingScheduler.ts] Failed to cancel listing job | listingId=${listingId}`, error);
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
  const { listingId } = job.data;
  // console.log(BLUE + `[LISTING SCHEDULER] Job ID: ${job.id}` + RESET);
  // console.log(BLUE + `[LISTING SCHEDULER] Listing ID: ${listingId}` + RESET);

  try {
    // Check if password is available before processing
    const password = getPassword();
    if (!password) {
      return true; // Return true to mark job as completed (not failed)
    }

    // Update progress to prevent stalling
    // Progress reporting removed - await job.updateProgress(10);

    // Update progress
    // Progress reporting removed - await job.updateProgress(20);

    // Fetch latest listing data
    const listing = await Listing.findById(listingId);
    // console.log(listing ? GREEN + '[LISTING SCHEDULER] Fetched listing: Found' + RESET : YELLOW + '[LISTING SCHEDULER] Fetched listing: Not found' + RESET);
    if (listing) {
      // console.log(BLUE + `[LISTING SCHEDULER] Listing running status: ${listing.running}` + RESET);
      // console.log(BLUE + `[LISTING SCHEDULER] Listing selectedMarketplaces: ${listing.selectedMarketplaces}` + RESET);
    }

    if (!listing) {
      // console.log('!!! LISTING NOT FOUND - RETURNING EARLY !!!');

      // Cancel the repeating job since the listing no longer exists
      await cancelListingJob(listingId);
      return false // Return successfully so the job doesn't retry
    }

    if (!listing.running) {
      // console.log('!!! LISTING NOT RUNNING - RETURNING EARLY !!!');

      // Cancel the repeating job
      await cancelListingJob(listingId);
      return false;
    }

    // console.log('Step 5: Listing is valid and running, continuing...');
    // console.log('About to check instant price matching...');

    // console.log('About to update progress to 50...');
    // Update progress before long operation
    // Progress reporting removed - await job.updateProgress(50);
    // console.log('Progress updated to 50');

    // Fetch fresh listing data again after cleanup to ensure we have the latest state
    const freshListing = await Listing.findById(listingId);
    if (!freshListing) {

      return false;
    }

    // Process the listing - pass all fresh data including instantPriceOverride if available
    const listingData = {
      ...freshListing.toObject(),
    };
    // console.log('\n=== ABOUT TO CALL CREATE LISTING ===');
    // console.log('Listing ID:', listingId);
    // console.log('Running:', freshListing.running);
    // console.log('Selected Marketplaces:', freshListing.selectedMarketplaces);
    // console.log('Listing data keys:', Object.keys(listingData));

    // console.log('\n=== CALLING CREATE LISTING FROM SCHEDULER ===');
    await createListing(listingData);
    // console.log('=== CREATE LISTING RETURNED ===');

    // Update progress
    // Progress reporting removed - await job.updateProgress(80);

    // Update last processed time
    await Listing.findByIdAndUpdate(listingId, {
      $set: {
        lastProcessedAt: new Date(),
      },
    });

    // Update progress to completion
    // Progress reporting removed - await job.updateProgress(100);

    // console.log(GREEN + '[LISTING SCHEDULER] ✅ Listing job completed successfully' + RESET);

    return true

  } catch (error) {
    errorLog('Listing', `Process listing job failed | jobId=${job.id} | listingId=${listingId}`, error);

    // Only show brief error message - detailed errors are already logged by marketplace functions
    const errorMsg = error instanceof Error ? error.message : 'Unknown error';
    if (!errorMsg.includes('Magic Eden') && !errorMsg.includes('Blur') && !errorMsg.includes('OpenSea')) {
      errorLog('Listing', `Process listing job failed: ${errorMsg}`);
    }

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
    // Check if password is available before initializing
    const password = getPassword();
    if (!password) {
      console.log('⏸️  Listing scheduler initialization skipped - no password available');
      return;
    }

    // Initializing listing scheduler

    // Get the main queue
    const queue = await getMainQueue();

    // Clean up old listing jobs
    const jobSchedulers = await queue.getJobSchedulers(0, -1, true);
    for (const scheduler of jobSchedulers) {
      if (scheduler.name === PROCESS_LISTING && scheduler.id) {
        await queue.removeJobScheduler(scheduler.id);
      }
    }

    // Find all active listings
    const activeListings = await Listing.find({ running: true });

    // Process active listings

    // Pre-fetch traits for listings with trait-based pricing
    const traitListings = activeListings.filter(listing => 
      (listing.dynamicPricingBasis === 'HIGHEST_TRAIT_FLOOR' || listing.dynamicPricingBasis === 'SPECIFIC_TRAIT') &&
      listing.selectedTrait?.trait_type && 
      listing.selectedTrait?.value !== undefined
    );
    
    if (traitListings.length > 0) {
      // console.log(`Pre-fetching traits for ${traitListings.length} trait-based listings...`);
      const tokensToFetch = traitListings.map(listing => ({
        contractAddress: listing.contract.contractAddress,
        tokenId: listing.tokenId
      }));
      
      await prefetchTokenTraits(tokensToFetch).catch(err => {
        errorLog('Metadata', 'Error pre-fetching token traits', err);
      });
    }

    // Process each active listing
    for (const listing of activeListings) {
      const listingId = listing._id.toString();

      // 1. Subscribe to WebSocket if instant price matching is enabled
      if (listing.instantPriceMatching && wsManager) {
        try {
          const contractAddress = listing.contract.contractAddress;
          const contractSlug = listing.contract.slug;

          // console.log(`Subscribing to WebSocket for instant price matching: ${contractSlug} (${contractAddress})`);

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
              // console.log(`✅ Successfully subscribed to ${marketplace} for listing events`);
            } catch (error) {
              subscriptionResults.push(`${marketplace}: failed`);
              errorLog('WebSocket', `Failed to subscribe to ${marketplace}`, error);
            }
          }

        } catch (error) {
          errorLog('WebSocket', `[listingScheduler.ts] Failed to subscribe listing to WebSocket | listingId=${listingId}`, error);
        }
      }

      // 2. Schedule the listing jobs
      await scheduleListing(listing);

      // 3. Immediately call createListing to list the NFT
      try {
        // console.log(`\n=== INITIAL LISTING ON STARTUP ===`);
        // console.log(`Listing ID: ${listingId}`);
        // console.log(`Token ID: ${listing.tokenId}`);
        // console.log(`Running: ${listing.running}`);
        // console.log(`Instant Price Matching: ${listing.instantPriceMatching}`);

        const listingData = listing.toObject();
        await createListing(listingData);

      } catch (error) {
        errorLog('Listing', `[listingScheduler.ts] Failed to process initial listing on startup | listingId=${listingId}`, error);
      }
    }

    // Listing scheduler initialized successfully
  } catch (error) {
    errorLog('Listing', '[listingScheduler.ts] Failed to initialize listing scheduler', error);
  }
}

/**
 * Shutdown listing scheduler gracefully
 */
export async function shutdownListingScheduler(): Promise<void> {
  try {
    // console.log('Shutting down listing scheduler...');

    // Cancel all active listing jobs
    const queue = await getMainQueue();
    const jobSchedulers = await queue.getJobSchedulers(0, -1, true);

    for (const scheduler of jobSchedulers) {
      if (scheduler.name === PROCESS_LISTING && scheduler.id) {
        await queue.removeJobScheduler(scheduler.id);
      }
    }

    // Clear active jobs map
    activeJobs.clear();

    // console.log('Listing scheduler shut down successfully');
  } catch (error) {
    errorLog('Scheduler', 'Error shutting down listing scheduler', error);
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

    return {
      activeListings: instantPriceMatchingListings.length,
      instantPriceMatchingListings: instantPriceMatchingListings.length,
      subscriptionStats,
      connectionStatus
    };
  } catch (error) {
    errorLog('WebSocket', '[listingScheduler.ts] Failed to get listing subscription diagnostics', error);
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