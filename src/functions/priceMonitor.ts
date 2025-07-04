import { IListing } from "../types/listing.types";
import Listing from "../models/listing.model";
import { getAllFloorPrices } from "./collection";
import { logger, ErrorCategory } from "../utils/logger";
import { cancelListingJob, scheduleListing } from "./listingScheduler";
import { createListing } from "./list";
import WebSocketManager from "../utils/websocket-manager";

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
  processedMessageIds.clear();
}, 5 * 60 * 1000);

const colors = {
  opensea: '\x1b[36m',    // Cyan
  blur: '\x1b[35m',       // Magenta
  magiceden: '\x1b[33m',  // Yellow
  unknown: '\x1b[31m',    // Red
  reset: '\x1b[0m'        // Reset
};
/**
 * Monitor WebSocket events for instant price matching
 * @param wsManager - The WebSocket manager instance
 * @param message - The incoming WebSocket message
 */
export async function handleListingPriceUpdate(wsManager: WebSocketManager | null, message: any): Promise<void> {
  try {
    // Color codes for marketplaces

    // Check if this is a listing event - handle new format
    if (message.eventType === 'ORDER_CREATED' && message.subscriptionType === 'listings') {      // New format - direct properties
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

      // Only log messages that match the expected format
      console.log(`\n${marketplaceColor}[${marketplace?.toUpperCase()}]${colors.reset} ${contractAddress} #${tokenId} - ${price} ETH from ${seller} at ${message.createdAt}`);

      if (!contractAddress || !tokenId || !price || !marketplace) {
        return;
      }

      // Mark this message as processed
      if (id) {
        processedMessageIds.add(id);
      }

      await processListingUpdate(wsManager, {
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
  wsManager: WebSocketManager | null,
  data: {
    contractAddress: string;
    tokenId: string;
    marketplace: string;
    price: number;
    seller?: string;
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
    if (seller && seller === listing.wallet.toLowerCase()) continue;

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

      await logger.logInfo(ErrorCategory.GENERAL,
        `Instant price match triggered for token ${tokenId} on ${marketplace}. Competitor price: ${price} ETH, New price: ${newPrice} ETH`
      );

      // Cancel current listing job
      await cancelListingJob(listing._id.toString());

      // Fetch fresh listing data from database
      const freshListing = await Listing.findById(listing._id);
      if (!freshListing) {
        console.error(`Listing ${listing._id} not found when fetching fresh data`);
        continue;
      }

      // Immediately relist with adjusted price
      const adjustedListing = {
        ...freshListing.toObject(),
        // Only relist on the marketplace where we were undercut
        selectedMarketplaces: [marketplace.toUpperCase()],
        // Use instant price override to bypass calculateListingPrice
        instantPriceOverride: newPrice,
      } as IListing;

      await createListing(adjustedListing);

      // Reschedule the regular listing job
      await scheduleListing(listing);

      // Send WebSocket notification
      if (wsManager) {
        wsManager.sendMessage({
          type: 'instantPriceMatch',
          data: {
            listingId: listing._id.toString(),
            tokenId,
            marketplace,
            competitorPrice: price,
            newPrice: newPrice,
            action: 'price_adjusted',
          }
        });
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

    // Check if competitor is undercutting us
    if (competitorPrice >= ourPrice) return false;

    // Get undercut margin (default to 0.0001 ETH if not specified)
    const undercutMargin = parseFloat(listing.underCutMargin) || 0.0001;

    // Check minimum listing price
    const minPrice = parseFloat(listing.minListingPrice) || 0;
    const newPrice = competitorPrice - undercutMargin;

    if (newPrice < minPrice) {
      await logger.logInfo(ErrorCategory.GENERAL,
        `Cannot undercut competitor price ${competitorPrice} as it would go below minimum ${minPrice}`
      );
      return false;
    }

    // Check if we're still profitable
    if (listing.buyPrice) {
      const minimumProfit = parseFloat(listing.minimumProfit) || 0;
      const profit = newPrice - listing.buyPrice;

      if (profit < minimumProfit) {
        await logger.logInfo(ErrorCategory.GENERAL,
          `Cannot undercut competitor as profit ${profit} would be below minimum ${minimumProfit}`
        );
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