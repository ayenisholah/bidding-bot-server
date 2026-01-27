import { IListing } from "../types/listing.types";
import Listing from "../models/listing.model";
import { getAllFloorPrices } from "./collection";
import { getCachedFloorPrice } from "../utils/collection-cache";
import { PROCESS_LISTING_UPDATE, queue } from "../index";
import { roundToNineDecimals } from "../utils/decimal-precision";
import { errorLog, warnLog } from "../utils/debug-file-logger";
import { tokenHasTrait } from "../functions/tokenMetadata";
import { getOpenseaTraitFloor } from "../utils/opensea-trait-floor";
import { axiosInstance, limiter } from "../init";
import { API_KEY } from "../index";
import { BASE_URL } from "../constants";
import { apiDedup } from "../utils/api-dedup";

// Store processed message IDs to prevent duplicate processing
const processedMessageIds = new Set<string>();

// Clean up old message IDs every 5 minutes to prevent memory leak
setInterval(() => {
  // const previousSize = processedMessageIds.size;
  processedMessageIds.clear();
}, 5 * 60 * 1000);

// Log statistics every minute for debugging
setInterval(() => {
  if (processedMessageIds.size > 0) {
    // console.log(BLUE + `[PRICE MONITOR - Stats] Processed messages: ${processedMessageIds.size}` + RESET);
  }
}, 60 * 1000);

// const colors = {
//   opensea: '\x1b[34m',    // Cyan
//   blur: '\x1b[33m',       // Magenta
//   magiceden: '\x1b[35m',  // Yellow
//   unknown: '\x1b[31m',    // Red
//   reset: '\x1b[0m'        // Reset
// };
/**
 * Get real-time listing price for our specific token
 * @param listing - The listing object with contract and wallet info
 * @returns Current listing price or null if not listed
 */
async function getRealTimeListingPrice(listing: IListing): Promise<number | null> {
  try {
    // For now, we'll check the collection's best listings and look for our wallet
    // This is a simplified approach - ideally we'd have a token-specific endpoint
    const url = `${BASE_URL}/opensea/api/v2/listings/collection/${listing.contract.slug}/best?limit=50`;
    
    const { data } = await apiDedup(
      `opensea-listings-${listing.contract.slug}`,
      () => limiter.schedule(() => axiosInstance.get(url, {
        headers: { 'x-api-key': API_KEY }
      })),
      { category: 'Listing' }
    );

    if (!data?.listings || data.listings.length === 0) {
      return null;
    }

    // Find our listing by matching wallet address and token ID
    const ourListing = data.listings.find((l: any) => {
      const offerer = l.protocol_data?.parameters?.offerer?.toLowerCase();
      const listingTokenId = l.protocol_data?.parameters?.offer?.[0]?.identifierOrCriteria;
      return offerer === listing.wallet.toLowerCase() && listingTokenId === listing.tokenId;
    });

    if (!ourListing) {
      // console.log(YELLOW + `[PRICE MONITOR - Real-time] No active listing found for token ${listing.tokenId}` + RESET);
      return null;
    }

    const price = parseFloat(ourListing.price.current.value) / Math.pow(10, ourListing.price.current.decimals);
    
    return price;
  } catch (error) {
    errorLog('Listing', `[priceMonitor.ts] Failed to get price for token ${listing.tokenId}`, error);
    return null;
  }
}

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
      // const marketplaceColor = colors[marketplace as keyof typeof colors] || colors.unknown;

      // Check if we've already processed this message
      if (id && processedMessageIds.has(id)) {
        return;
      }

      // console.log(marketplaceColor + `[PRICE MONITOR - ${marketplace.toUpperCase()}] Incoming listing for ${contractAddress} #${tokenId} - ${price} ETH from ${seller}` + colors.reset);

      if (!contractAddress || !tokenId || !price || !marketplace) {
        // console.log(YELLOW + `[PRICE MONITOR - Warning] Incomplete message data - skipping` + RESET);
        warnLog('WebSocket', '[priceMonitor.ts] Incomplete price monitor message data - skipping');
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
    errorLog('WebSocket', 'Failed to handle listing price update', error);
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
  
  
  try {
    // Find all active listings for this CONTRACT (not just token) with instant price matching enabled
    // This enables listing wars between different tokens in the same collection
    const activeListings = await Listing.find({
      'contract.contractAddress': new RegExp(`^${contractAddress}$`, 'i'),
      running: true,
      instantPriceMatching: true,
    });

    // console.log(BLUE + `[PRICE MONITOR - Query] Found ${activeListings.length} active instant price matching listings` + RESET);

    for (const listing of activeListings) {
      // ALWAYS skip listings from our own wallet (prevent self-competition)
      if (seller && seller.toLowerCase() === listing.wallet.toLowerCase()) {
        // console.log(YELLOW + `[PRICE MONITOR - Skip] Skipping same-wallet listing: ${listing.wallet} token ${listing.tokenId} ignoring ${seller} token ${tokenId}` + RESET);
        continue;
      }

      // Check trait filtering if using trait-based dynamic pricing
      if ((listing.dynamicPricingBasis === 'HIGHEST_TRAIT_FLOOR' || listing.dynamicPricingBasis === 'SPECIFIC_TRAIT') 
          && listing.selectedTrait?.trait_type && listing.selectedTrait?.value !== undefined) {
        
        // console.log(BLUE + `[PRICE MONITOR - Trait Config] Listing ${listing.tokenId} is configured for trait ${listing.selectedTrait.trait_type}:${listing.selectedTrait.value}` + RESET);
        
        // First check if our own listing token has the configured trait
        const ourTokenHasTrait = await tokenHasTrait(contractAddress, listing.tokenId, listing.selectedTrait.trait_type, listing.selectedTrait.value);
        
        if (!ourTokenHasTrait) {
          // Our token doesn't have the configured trait - log warning and compete with all tokens
          // console.log(YELLOW + `[PRICE MONITOR - Warning] Listing token ${listing.tokenId} doesn't have configured trait ${listing.selectedTrait.trait_type}:${listing.selectedTrait.value}` + RESET);
          // console.log(YELLOW + `[PRICE MONITOR - Warning] Trait filtering disabled - will compete with all tokens` + RESET);
          warnLog('Listing', `[priceMonitor.ts] Listing configured for trait it does not possess | listingId=${listing._id} | tokenId=${listing.tokenId}`);
          // Continue without trait filtering
        } else {
          // Our token has the trait - now check if competitor has it too
          // console.log(GREEN + `[PRICE MONITOR - Trait Confirmed] Listing token ${listing.tokenId} has the configured trait` + RESET);
          // console.log(BLUE + `[PRICE MONITOR - Trait Check] Checking if competitor token ${tokenId} has trait ${listing.selectedTrait.trait_type}:${listing.selectedTrait.value}` + RESET);
          
          const competitorHasTrait = await tokenHasTrait(contractAddress, tokenId, listing.selectedTrait.trait_type, listing.selectedTrait.value);
          
          if (!competitorHasTrait) {
            // console.log(YELLOW + `[PRICE MONITOR - Skip] Competitor token ${tokenId} doesn't have required trait ${listing.selectedTrait.trait_type}:${listing.selectedTrait.value}` + RESET);
            continue;
          }
          
          // console.log(GREEN + `[PRICE MONITOR - Trait Match] Both tokens have matching trait, proceeding with listing war` + RESET);
        }
      }

      // LISTING WAR: Process competitor listings from different wallets
      // console.log(BLUE + `[PRICE MONITOR - Competitor] Token ${listing.tokenId} (${listing.wallet}) reacting to competitor token ${tokenId} (${seller}) at ${price} ETH` + RESET);

      // No need to clear cache - we're using real-time prices now!
      const shouldAdjust = await shouldAdjustPrice(listing, price);

      if (shouldAdjust) {
        // Calculate the new undercut price
        const undercutMargin = parseFloat(listing.underCutMargin) || 0.0000;
        const newPrice = roundToNineDecimals(price - undercutMargin);
        // Create a unique job ID based on listing ID and marketplace
        const jobId = `listing-update-${listing._id}-${marketplace}`;

        // console.log(GREEN + `[PRICE MONITOR - Queue] Enqueueing instant price match job: ${newPrice} ETH` + RESET);

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
          errorLog('Queue', `Failed to enqueue instant price match job | listingId=${listing._id} | tokenId=${tokenId}`, error);
        }
      }
    }
  } catch (error) {
    errorLog('Database', `Failed to process listing update | contractAddress=${contractAddress} | tokenId=${tokenId}`, error);
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
    // console.log(BLUE + `[PRICE CHECK] Token ${listing.tokenId} - Checking adjustment for competitor price: ${competitorPrice} ETH` + RESET);
    
    // Get our current listing price in REAL-TIME - no cache!
    const ourPrice = await getRealTimeListingPrice(listing) || 0;


    // For listing wars: if we don't have a current listing price, we should still compete
    // Calculate what our competitive price would be
    const undercutMargin = parseFloat(listing.underCutMargin) || 0.000001;
    const targetCompetitivePrice = roundToNineDecimals(competitorPrice - undercutMargin);
    

    // If we don't have a current price, we should list competitively
    if (!ourPrice) {
      // console.log(YELLOW + `[PRICE CHECK] Token ${listing.tokenId} - No current price, should list competitively at ${targetCompetitivePrice}` + RESET);
      return true;
    }

    // Check if we need to adjust: only skip if competitor price is HIGHER than ours
    // If prices are equal (tie), we should continue the war!
    if (competitorPrice > ourPrice) {
      // console.log(BLUE + `[PRICE CHECK] Token ${listing.tokenId} - Competitor price ${competitorPrice} > our price ${ourPrice}, no adjustment needed` + RESET);
      return false;
    }
    
    // Special handling for tie scenarios - always try to maintain competitive edge
    if (competitorPrice === ourPrice) {
      // console.log(YELLOW + `[PRICE CHECK] Token ${listing.tokenId} - TIE DETECTED! Both at ${ourPrice} ETH, continuing listing war` + RESET);
    }

    // console.log(GREEN + `[PRICE CHECK] Token ${listing.tokenId} - Competitor undercut us! ${competitorPrice} < ${ourPrice}, checking constraints...` + RESET);

    // Check minimum listing price - always enforce this constraint
    const minPrice = parseFloat(listing.minListingPrice) || 0;
    const newPrice = targetCompetitivePrice; // Use the already calculated competitive price


    if (newPrice < minPrice) {
      // console.log(YELLOW + `[PRICE CHECK] Token ${listing.tokenId} - New price ${newPrice} < min price ${minPrice}, skipping adjustment` + RESET);
      warnLog('Listing', `[priceMonitor.ts] Price adjustment blocked by minimum price constraint | listingId=${listing._id} | tokenId=${listing.tokenId} | newPrice=${newPrice} | minPrice=${minPrice}`);
      return false;
    }

    // Check if we're still profitable
    if (listing.buyPrice) {
      const minimumProfit = parseFloat(listing.minimumProfit) || 0;
      const profit = newPrice - listing.buyPrice;

      if (profit < minimumProfit && minimumProfit > 0) {
        // console.log(YELLOW + `[PRICE CHECK] Token ${listing.tokenId} - Insufficient profit ${profit} < ${minimumProfit}, skipping adjustment` + RESET);
        warnLog('Listing', `[priceMonitor.ts] Price adjustment blocked by profit constraint | listingId=${listing._id} | tokenId=${listing.tokenId} | profit=${profit} | minProfit=${minimumProfit}`);
        return false;
      }
    }

    // Check floor price constraint if enabled
    const doNotListBelowFloor = listing.doNotListBelowFloor === true; // Default to false
    if (doNotListBelowFloor) {
      let floorPrice: number;
      
      // Use trait floor if trait-based pricing is enabled
      if ((listing.dynamicPricingBasis === 'HIGHEST_TRAIT_FLOOR' || listing.dynamicPricingBasis === 'SPECIFIC_TRAIT') 
          && listing.selectedTrait?.trait_type && listing.selectedTrait?.value !== undefined) {
        floorPrice = await getOpenseaTraitFloor(listing.contract.slug, listing.selectedTrait.trait_type, listing.selectedTrait.value);
      } else {
        floorPrice = await getCachedFloorPrice(listing.contract.slug);
      }
      
      if (newPrice < floorPrice) {
        // console.log(YELLOW + `[PRICE CHECK] Token ${listing.tokenId} - New price ${newPrice} would be below floor price ${floorPrice}, skipping adjustment` + RESET);
        warnLog('Listing', `[priceMonitor.ts] Price adjustment blocked by floor price constraint | listingId=${listing._id} | tokenId=${listing.tokenId} | newPrice=${newPrice} | floorPrice=${floorPrice}`);
        return false;
      }
    }

    // console.log(GREEN + `[PRICE CHECK] Token ${listing.tokenId} - ✅ All checks passed, should adjust price from ${ourPrice} to ${newPrice}` + RESET);

    return true;
  } catch (error) {
    errorLog('Listing', `Error checking price adjustment | listingId=${listing._id} | tokenId=${listing.tokenId}`, error);
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
    // console.log(BLUE + `[PRICE MONITOR - Floor] Fetching floor prices for ${collectionSlug}` + RESET);
    const floorPrices = await getAllFloorPrices(collectionSlug);
    // console.log(GREEN + `[PRICE MONITOR - Floor] Retrieved floor prices: OpenSea=${floorPrices.opensea}, Blur=${floorPrices.blur}, MagicEden=${floorPrices.magiceden}` + RESET);

    return floorPrices;
  } catch (error) {
    errorLog('API', `Failed to fetch floor prices | slug=${collectionSlug}`, error);
    return { opensea: 0, blur: 0, magiceden: 0 };
  }
}


/**
 * Extend instant price matching session for a listing
 */
export async function extendInstantPriceMatchingSession(listingId: string, durationMinutes: number = 10): Promise<void> {
  try {
    
    const newExpiration = new Date(Date.now() + durationMinutes * 60 * 1000);
    const result = await Listing.findByIdAndUpdate(listingId, {
      instantPriceMatchingActiveUntil: newExpiration,
    });

    if (result) {
      // console.log(GREEN + `[PRICE MONITOR - Extend] ✅ Extended instant price matching until ${newExpiration.toISOString()}` + RESET);
    } else {
      // console.log(YELLOW + `[PRICE MONITOR - Extend] Warning: Listing ${listingId} not found` + RESET);
      warnLog('Database', `Listing not found for instant price matching extension | listingId=${listingId}`);
    }
  } catch (error) {
    errorLog('Database', `Failed to extend instant price matching session | listingId=${listingId}`, error);
  }
}