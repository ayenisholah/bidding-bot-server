import { IListing } from "../types/listing.types";
import Listing from "../models/listing.model";
import { getAllFloorPrices } from "./collection";
import { PROCESS_LISTING_UPDATE, queue } from "../index";

// Store current best prices for listings we're monitoring
const currentListingPrices: Map<string, {
  opensea?: number;
  blur?: number;
  magiceden?: number;
}> = new Map();

// Store processed message IDs to prevent duplicate processing
const processedMessageIds = new Set<string>();

// Clean up old message IDs every 5 minutes to prevent memory leak
setInterval(() => {
  const previousSize = processedMessageIds.size;
  processedMessageIds.clear();
  if (previousSize > 0) {
    console.log(`[Price Monitor] Cleared ${previousSize} processed message IDs`);
  }
}, 5 * 60 * 1000);

// Log statistics every minute for debugging
setInterval(() => {
  if (currentListingPrices.size > 0) {
    console.log(`[Price Monitor Stats] Active price tracking: ${currentListingPrices.size} listings, Processed messages: ${processedMessageIds.size}`);
  }
}, 60 * 1000);

const colors = {
  opensea: '\x1b[34m',    // Cyan
  blur: '\x1b[33m',       // Magenta
  magiceden: '\x1b[35m',  // Yellow
  unknown: '\x1b[31m',    // Red
  reset: '\x1b[0m'        // Reset
};
/**
 * Monitor WebSocket events for instant price matching
 * @param wsManager - The WebSocket manager instance (not used anymore, kept for compatibility)
 * @param message - The incoming WebSocket message
 */
export async function handleListingPriceUpdate(message: any): Promise<void> {
  try {
    // Accept both 'listings' and 'both' subscription types (when a client subscribes to both bidding and listing events)
    if (message.eventType === 'LISTING_CREATED') {      // New format - direct properties
      const contractAddress = message.contractAddress?.toLowerCase();
      const id = message.id;
      const tokenId = message.tokenId;
      const marketplace = message.marketplace?.toLowerCase();
      const price = parseFloat(message.price) || 0;
      const seller = message.fromAddress?.toLowerCase();
      const marketplaceColor = colors[marketplace as keyof typeof colors] || colors.unknown;

      // Check if we've already processed this message
      if (id && processedMessageIds.has(id)) {
        return;
      }

      console.log(marketplaceColor + `incoming ${marketplace} listing for ${contractAddress} #${tokenId} - ${price} ETH from ${seller} at ${message.createdAt}`.toUpperCase() + colors.reset);

      if (!contractAddress || !tokenId || !price || !marketplace) {
        return;
      }

      // Mark this message as processed
      if (id) {
        processedMessageIds.add(id);
      }

      await processListingUpdate({
        contractAddress,
        tokenId,
        marketplace,
        price,
        seller
      });
      return;
    }
    // Ignore legacy format - only process new format
    return;
  } catch (error) {
    console.error('Error handling listing price update:', error);
  }
}

/**
 * Process listing update regardless of message format
 */
async function processListingUpdate(
  data: {
    contractAddress: string;
    tokenId: string;
    marketplace: string;
    price: number;
    seller: string;
  }
): Promise<void> {
  const { contractAddress, tokenId, marketplace, price, seller } = data;

  // Find all active listings for this token with instant price matching enabled
  const activeListings = await Listing.find({
    'contract.contractAddress': new RegExp(`^${contractAddress}$`, 'i'),
    tokenId: tokenId.toString(),
    running: true,
    instantPriceMatching: true,
  });

  for (const listing of activeListings) {
    // Skip if this is our own listing
    if (seller && seller === listing.wallet.toLowerCase()) {
      console.log(`Skipping our own listing from ${seller}`);
      continue;
    }

    // Get the key for price tracking
    const priceKey = `${contractAddress}-${tokenId}`;

    // Initialize price tracking if needed
    if (!currentListingPrices.has(priceKey)) {
      currentListingPrices.set(priceKey, {});
    }

    const prices = currentListingPrices.get(priceKey)!;
    const marketplaceLower = marketplace.toLowerCase() as 'opensea' | 'blur' | 'magiceden';

    // Update the current price for this marketplace
    prices[marketplaceLower] = price;

    // Check if we need to adjust our price
    const shouldAdjust = await shouldAdjustPrice(listing, marketplaceLower, price);

    if (shouldAdjust) {
      // Calculate the new undercut price
      const undercutMargin = parseFloat(listing.underCutMargin) || 0.0001;
      const newPrice = price - undercutMargin;

      // Create a unique job ID based on listing ID and marketplace
      const jobId = `listing-update-${listing._id}-${marketplace}`;

      try {
        // Check if a job already exists for this listing/marketplace combination
        const existingJob = await queue.getJob(jobId);

        if (existingJob) {
          // Get the existing job's price
          const existingPrice = existingJob.data.newPrice;

          // If the new price is lower than existing, skip this update
          // The existing job will handle the better price
          if (newPrice >= existingPrice) {
            // Skip adding this job as the existing one has a better price
            console.log(`Skipping price update for ${tokenId} - existing job has better price (${existingPrice} vs ${newPrice})`);
            continue;
          }

          // Check if job is being processed
          const jobState = await existingJob.getState();
          if (jobState === 'active') {
            console.log(`Skipping price update for ${tokenId} - existing job is being processed`);
            continue;
          }

          // Job exists but new price is better - let it complete naturally
          // The job processor should check current market conditions
          console.log(`Existing job for ${tokenId} will be superseded by market conditions`);
        }

        // Add the job to the queue
        await queue.add(
          PROCESS_LISTING_UPDATE,
          {
            listingId: listing._id.toString(),
            newPrice,
            competitorPrice: price,
            marketplace
          },
          {
            jobId,
            removeOnComplete: true,
            removeOnFail: true,
            attempts: 3,
            priority: 1, // Higher priority than regular listings
            backoff: {
              type: 'exponential',
              delay: 2000
            }
          }
        );

      } catch (error) {
        console.error(`Failed to enqueue instant price match job for ${tokenId}:`, error);

      }
    }
  }
}

/**
 * Check if we should adjust our listing price
 */
async function shouldAdjustPrice(
  listing: IListing,
  marketplace: 'opensea' | 'blur' | 'magiceden',
  competitorPrice: number
): Promise<boolean> {
  try {
    // Get our current listing price
    const ourPrice = listing.finalListingPrice?.[marketplace] || 0;

    // If we don't have a price listed, don't adjust
    if (!ourPrice) return false;

    // Check if competitor is undercutting us (or matching our price)
    if (competitorPrice > ourPrice) return false;

    // Get undercut margin (default to 0.0001 ETH if not specified)
    const undercutMargin = parseFloat(listing.underCutMargin) || 0.0001;

    // Check minimum listing price - always enforce this constraint
    const minPrice = parseFloat(listing.minListingPrice) || 0;
    const newPrice = competitorPrice - undercutMargin;

    if (newPrice < minPrice) {

      return false;
    }

    // Check if we're still profitable
    if (listing.buyPrice) {
      const minimumProfit = parseFloat(listing.minimumProfit) || 0;
      const profit = newPrice - listing.buyPrice;

      if (profit < minimumProfit && minimumProfit > 0) {

        return false;
      }
    }

    return true;
  } catch (error) {
    console.error('Error checking price adjustment:', error);
    return false;
  }
}

/**
 * Get floor prices for all marketplaces
 */
export async function getFloorPrices(collectionSlug: string): Promise<{
  opensea: number;
  blur: number;
  magiceden: number;
}> {
  try {
    return await getAllFloorPrices(collectionSlug);
  } catch (error) {
    console.error('Error fetching floor prices:', error);
    return { opensea: 0, blur: 0, magiceden: 0 };
  }
}

/**
 * Clear price cache for a specific listing
 */
export function clearListingPriceCache(contractAddress: string, tokenId: string): void {
  const priceKey = `${contractAddress.toLowerCase()}-${tokenId}`;
  currentListingPrices.delete(priceKey);
}

/**
 * Extend instant price matching session for a listing
 */
export async function extendInstantPriceMatchingSession(listingId: string, durationMinutes: number = 10): Promise<void> {
  const newExpiration = new Date(Date.now() + durationMinutes * 60 * 1000);

  await Listing.findByIdAndUpdate(listingId, {
    instantPriceMatchingActiveUntil: newExpiration,
  });

}