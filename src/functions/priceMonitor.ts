import { IListing } from "../types/listing.types";
import Listing from "../models/listing.model";
import { getAllFloorPrices, getOpenseaFloor } from "./collection";
import { PROCESS_LISTING_UPDATE, queue } from "../index";
import { listingPriceCache } from "../utils/listing-price-cache";
import { roundToNineDecimals } from "../utils/decimal-precision";

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
  if (processedMessageIds.size > 0) {
    console.log(`[Price Monitor Stats] Processed messages: ${processedMessageIds.size}`);
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
        seller,
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
    if (seller && seller.toLowerCase() === listing.wallet.toLowerCase()) {
      console.log(`Skipping our own listing from ${seller}`);
      continue;
    }
    const shouldAdjust = await shouldAdjustPrice(listing, price);

    if (shouldAdjust) {
      // Calculate the new undercut price
      const undercutMargin = parseFloat(listing.underCutMargin) || 0.0000;
      const newPrice = roundToNineDecimals(price - undercutMargin);
      // Create a unique job ID based on listing ID and marketplace
      const jobId = `listing-update-${listing._id}-${marketplace}`;

      try {

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
  competitorPrice: number
): Promise<boolean> {
  try {
    // Get our current listing price from cache
    const ourPrice = await listingPriceCache.getLowestPrice(
      listing.contract.contractAddress,
      listing.tokenId
    ) || 0;

    // If we don't have a price listed, don't adjust
    if (!ourPrice) return false;

    // Check if competitor price is higher than ours (no need to adjust)
    if (competitorPrice > ourPrice) return false;

    // Get undercut margin (default to 0.0000 ETH if not specified)
    const undercutMargin = parseFloat(listing.underCutMargin) || 0.0000;

    // Check minimum listing price - always enforce this constraint
    const minPrice = parseFloat(listing.minListingPrice) || 0;
    const newPrice = roundToNineDecimals(competitorPrice - undercutMargin);

    if (newPrice < minPrice) return false;

    // Check if we're still profitable
    if (listing.buyPrice) {
      const minimumProfit = parseFloat(listing.minimumProfit) || 0;
      const profit = newPrice - listing.buyPrice;

      if (profit < minimumProfit && minimumProfit > 0) return false;
    }

    // Check floor price constraint if enabled
    const doNotListBelowFloor = listing.doNotListBelowFloor === true; // Default to false
    if (doNotListBelowFloor) {
      const floorPrice = await getOpenseaFloor(listing.contract.slug);
      if (newPrice < floorPrice) {
        console.log(`New price ${newPrice} would be below floor price ${floorPrice}, skipping adjustment`);
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
 * Extend instant price matching session for a listing
 */
export async function extendInstantPriceMatchingSession(listingId: string, durationMinutes: number = 10): Promise<void> {
  const newExpiration = new Date(Date.now() + durationMinutes * 60 * 1000);
  await Listing.findByIdAndUpdate(listingId, {
    instantPriceMatchingActiveUntil: newExpiration,
  });
}