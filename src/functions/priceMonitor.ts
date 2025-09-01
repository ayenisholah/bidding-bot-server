import { IListing } from "../types/listing.types";
import Listing from "../models/listing.model";
import { getAllFloorPrices, getOpenseaFloor } from "./collection";
import { PROCESS_LISTING_UPDATE, queue } from "../index";
import { listingPriceCache } from "../utils/listing-price-cache";
import { roundToNineDecimals } from "../utils/decimal-precision";
import { logger, ErrorCategory } from "../utils/logger";
import { tokenHasTrait } from "../functions/tokenMetadata";
import { getOpenseaTraitFloor } from "../utils/opensea-trait-floor";
import { axiosInstance, limiter } from "../init";
import { API_KEY } from "../index";
import { BASE_URL } from "../constants";

// Store processed message IDs to prevent duplicate processing
const processedMessageIds = new Set<string>();

// Color constants for console output
const GREEN = '\x1b[32m';
const YELLOW = '\x1b[33m';
const RED = '\x1b[31m';
const BLUE = '\x1b[34m';
const CYAN = '\x1b[36m';
const RESET = '\x1b[0m';

// Clean up old message IDs every 5 minutes to prevent memory leak
setInterval(() => {
  const previousSize = processedMessageIds.size;
  processedMessageIds.clear();
  if (previousSize > 0) {
    // console.log(CYAN + `[PRICE MONITOR - Cleanup] Cleared ${previousSize} processed message IDs` + RESET);
    logger.logInfo(ErrorCategory.GENERAL, 'Price monitor message ID cleanup', {
      operation: 'cleanup_message_ids',
      clearedCount: previousSize,
      component: 'price_monitor'
    });
  }
}, 5 * 60 * 1000);

// Log statistics every minute for debugging
setInterval(() => {
  if (processedMessageIds.size > 0) {
    // console.log(BLUE + `[PRICE MONITOR - Stats] Processed messages: ${processedMessageIds.size}` + RESET);
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
 * Get real-time listing price for our specific token
 * @param listing - The listing object with contract and wallet info
 * @returns Current listing price or null if not listed
 */
async function getRealTimeListingPrice(listing: IListing): Promise<number | null> {
  try {
    // For now, we'll check the collection's best listings and look for our wallet
    // This is a simplified approach - ideally we'd have a token-specific endpoint
    const url = `${BASE_URL}/opensea/api/v2/listings/collection/${listing.contract.slug}/best?limit=50`;
    
    const { data } = await limiter.schedule(() => axiosInstance.get(url, {
      headers: { 'x-api-key': API_KEY }
    }));

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
    
    // console.log(CYAN + `[PRICE MONITOR - Real-time] Token ${listing.tokenId} current price: ${price} ETH` + RESET);
    return price;
  } catch (error) {
    // console.log(YELLOW + `[PRICE MONITOR - Real-time] Failed to get price for token ${listing.tokenId}: ${error}` + RESET);
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
      const marketplaceColor = colors[marketplace as keyof typeof colors] || colors.unknown;

      // Check if we've already processed this message
      if (id && processedMessageIds.has(id)) {
        return;
      }

      // console.log(marketplaceColor + `[PRICE MONITOR - ${marketplace.toUpperCase()}] Incoming listing for ${contractAddress} #${tokenId} - ${price} ETH from ${seller}` + colors.reset);

      if (!contractAddress || !tokenId || !price || !marketplace) {
        // console.log(YELLOW + `[PRICE MONITOR - Warning] Incomplete message data - skipping` + RESET);
        logger.logWarning(ErrorCategory.WEBSOCKET, 'Incomplete price monitor message data', {
          contractAddress,
          tokenId,
          price,
          marketplace,
          seller,
          messageId: id,
          component: 'price_monitor'
        });
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
    console.error(RED + '[PRICE MONITOR - Error] Failed to handle listing price update' + RESET, error);
    logger.logWebSocketError('Failed to handle listing price update', {
      error: error as Error,
      errorMessage: error instanceof Error ? error.message : String(error),
      component: 'price_monitor',
      operation: 'handle_listing_price_update'
    });
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
  
  // console.log(CYAN + `[PRICE MONITOR - Process] Processing listing update for ${contractAddress}:${tokenId}` + RESET);
  
  try {
    // Find all active listings for this CONTRACT (not just token) with instant price matching enabled
    // This enables listing wars between different tokens in the same collection
    const activeListings = await Listing.find({
      'contract.contractAddress': new RegExp(`^${contractAddress}$`, 'i'),
      running: true,
      instantPriceMatching: true,
    });

    // console.log(BLUE + `[PRICE MONITOR - Query] Found ${activeListings.length} active instant price matching listings` + RESET);

    if (activeListings.length > 0) {
      logger.logInfo(ErrorCategory.GENERAL, 'Processing instant price matching listings', {
        contractAddress,
        tokenId,
        marketplace,
        price,
        seller,
        activeListingsCount: activeListings.length,
        component: 'price_monitor',
        operation: 'process_listing_update'
      });
    }

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
          
          logger.logWarning(ErrorCategory.BUSINESS_LOGIC, 'Listing configured for trait it does not possess', {
            listingId: listing._id.toString(),
            contractAddress,
            tokenId: listing.tokenId,
            configuredTrait: listing.selectedTrait,
            component: 'price_monitor',
            operation: 'trait_filtering_check'
          });
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

          logger.logInfo(ErrorCategory.QUEUE, 'Instant price match job enqueued', {
            listingId: listing._id.toString(),
            contractAddress,
            tokenId,
            marketplace,
            competitorPrice: price,
            newPrice,
            undercutMargin,
            jobId,
            component: 'price_monitor',
            operation: 'enqueue_instant_price_match'
          });

        } catch (error) {
          console.error(RED + `[PRICE MONITOR - Error] Failed to enqueue instant price match job for ${tokenId}` + RESET, error);
          logger.logQueueError('Failed to enqueue instant price match job', {
            listingId: listing._id.toString(),
            contractAddress,
            tokenId,
            marketplace,
            competitorPrice: price,
            newPrice,
            jobId,
            error: error as Error,
            errorMessage: error instanceof Error ? error.message : String(error),
            component: 'price_monitor'
          });
        }
      }
    }
  } catch (error) {
    console.error(RED + `[PRICE MONITOR - Error] Failed to process listing update for ${contractAddress}:${tokenId}` + RESET, error);
    logger.logDatabaseError('Failed to process listing update', {
      contractAddress,
      tokenId,
      marketplace,
      price,
      seller,
      error: error as Error,
      errorMessage: error instanceof Error ? error.message : String(error),
      component: 'price_monitor',
      operation: 'process_listing_update'
    });
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

    // console.log(CYAN + `[PRICE CHECK] Token ${listing.tokenId} - Our real-time price: ${ourPrice} ETH` + RESET);

    // For listing wars: if we don't have a current listing price, we should still compete
    // Calculate what our competitive price would be
    const undercutMargin = parseFloat(listing.underCutMargin) || 0.000001;
    const targetCompetitivePrice = roundToNineDecimals(competitorPrice - undercutMargin);
    
    // console.log(CYAN + `[PRICE CHECK] Token ${listing.tokenId} - Target competitive price: ${targetCompetitivePrice} ETH` + RESET);

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

    // console.log(CYAN + `[PRICE CHECK] Token ${listing.tokenId} - Calculated new price: ${newPrice} ETH (competitor: ${competitorPrice} - margin: ${undercutMargin})` + RESET);

    if (newPrice < minPrice) {
      // console.log(YELLOW + `[PRICE CHECK] Token ${listing.tokenId} - New price ${newPrice} < min price ${minPrice}, skipping adjustment` + RESET);
      logger.logWarning(ErrorCategory.BUSINESS_LOGIC, 'Price adjustment blocked by minimum price constraint', {
        listingId: listing._id.toString(),
        contractAddress: listing.contract.contractAddress,
        tokenId: listing.tokenId,
        competitorPrice,
        calculatedPrice: newPrice,
        minPrice,
        undercutMargin,
        component: 'price_monitor',
        operation: 'price_adjustment_check'
      });
      return false;
    }

    // Check if we're still profitable
    if (listing.buyPrice) {
      const minimumProfit = parseFloat(listing.minimumProfit) || 0;
      const profit = newPrice - listing.buyPrice;

      // console.log(CYAN + `[PRICE CHECK] Token ${listing.tokenId} - Profit check: ${profit} ETH (required: ${minimumProfit} ETH)` + RESET);
      if (profit < minimumProfit && minimumProfit > 0) {
        // console.log(YELLOW + `[PRICE CHECK] Token ${listing.tokenId} - Insufficient profit ${profit} < ${minimumProfit}, skipping adjustment` + RESET);
        logger.logWarning(ErrorCategory.BUSINESS_LOGIC, 'Price adjustment blocked by profit constraint', {
          listingId: listing._id.toString(),
          contractAddress: listing.contract.contractAddress,
          tokenId: listing.tokenId,
          competitorPrice,
          calculatedPrice: newPrice,
          buyPrice: listing.buyPrice,
          calculatedProfit: profit,
          minimumProfit,
          component: 'price_monitor',
          operation: 'price_adjustment_check'
        });
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
        // console.log(CYAN + `[PRICE CHECK] Token ${listing.tokenId} - Using trait floor for ${listing.selectedTrait.trait_type}:${listing.selectedTrait.value}` + RESET);
        floorPrice = await getOpenseaTraitFloor(listing.contract.slug, listing.selectedTrait.trait_type, listing.selectedTrait.value);
      } else {
        floorPrice = await getOpenseaFloor(listing.contract.slug);
      }
      
      if (newPrice < floorPrice) {
        // console.log(YELLOW + `[PRICE CHECK] Token ${listing.tokenId} - New price ${newPrice} would be below floor price ${floorPrice}, skipping adjustment` + RESET);
        logger.logWarning(ErrorCategory.BUSINESS_LOGIC, 'Price adjustment blocked by floor price constraint', {
          listingId: listing._id.toString(),
          contractAddress: listing.contract.contractAddress,
          tokenId: listing.tokenId,
          collectionSlug: listing.contract.slug,
          competitorPrice,
          calculatedPrice: newPrice,
          floorPrice,
          isTraitFloor: listing.dynamicPricingBasis === 'HIGHEST_TRAIT_FLOOR' || listing.dynamicPricingBasis === 'SPECIFIC_TRAIT',
          trait: listing.selectedTrait,
          component: 'price_monitor',
          operation: 'price_adjustment_check'
        });
        return false;
      }
    }

    // console.log(GREEN + `[PRICE CHECK] Token ${listing.tokenId} - ✅ All checks passed, should adjust price from ${ourPrice} to ${newPrice}` + RESET);
    
    logger.logInfo(ErrorCategory.GENERAL, 'Price adjustment approved', {
      listingId: listing._id.toString(),
      contractAddress: listing.contract.contractAddress,
      tokenId: listing.tokenId,
      competitorPrice,
      currentPrice: ourPrice,
      newPrice,
      undercutMargin,
      component: 'price_monitor',
      operation: 'price_adjustment_approved'
    });
    
    return true;
  } catch (error) {
    console.error(RED + `[PRICE CHECK] Token ${listing.tokenId} - Error checking price adjustment` + RESET, error);
    logger.logGeneralError('Error checking price adjustment', {
      listingId: listing._id.toString(),
      contractAddress: listing.contract.contractAddress,
      tokenId: listing.tokenId,
      competitorPrice,
      error: error as Error,
      errorMessage: error instanceof Error ? error.message : String(error),
      component: 'price_monitor',
      operation: 'price_adjustment_check'
    });
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
    
    logger.logInfo(ErrorCategory.API, 'Floor prices retrieved successfully', {
      collectionSlug,
      floorPrices,
      component: 'price_monitor',
      operation: 'get_floor_prices'
    });
    
    return floorPrices;
  } catch (error) {
    console.error(RED + `[PRICE MONITOR - Error] Failed to fetch floor prices for ${collectionSlug}` + RESET, error);
    
    logger.logApiError('Failed to fetch floor prices', {
      collectionSlug,
      error: error as Error,
      errorMessage: error instanceof Error ? error.message : String(error),
      component: 'price_monitor',
      operation: 'get_floor_prices'
    });
    
    return { opensea: 0, blur: 0, magiceden: 0 };
  }
}


/**
 * Extend instant price matching session for a listing
 */
export async function extendInstantPriceMatchingSession(listingId: string, durationMinutes: number = 10): Promise<void> {
  try {
    // console.log(CYAN + `[PRICE MONITOR - Extend] Extending instant price matching session for listing ${listingId} by ${durationMinutes} minutes` + RESET);
    
    const newExpiration = new Date(Date.now() + durationMinutes * 60 * 1000);
    const result = await Listing.findByIdAndUpdate(listingId, {
      instantPriceMatchingActiveUntil: newExpiration,
    });

    if (result) {
      // console.log(GREEN + `[PRICE MONITOR - Extend] ✅ Extended instant price matching until ${newExpiration.toISOString()}` + RESET);
      
      logger.logInfo(ErrorCategory.DATABASE, 'Instant price matching session extended', {
        listingId,
        durationMinutes,
        newExpiration: newExpiration.toISOString(),
        component: 'price_monitor',
        operation: 'extend_instant_price_matching'
      });
    } else {
      // console.log(YELLOW + `[PRICE MONITOR - Extend] Warning: Listing ${listingId} not found` + RESET);
      
      logger.logWarning(ErrorCategory.DATABASE, 'Listing not found for instant price matching extension', {
        listingId,
        durationMinutes,
        component: 'price_monitor',
        operation: 'extend_instant_price_matching'
      });
    }
  } catch (error) {
    console.error(RED + `[PRICE MONITOR - Error] Failed to extend instant price matching session for ${listingId}` + RESET, error);
    
    logger.logDatabaseError('Failed to extend instant price matching session', {
      listingId,
      durationMinutes,
      error: error as Error,
      errorMessage: error instanceof Error ? error.message : String(error),
      component: 'price_monitor',
      operation: 'extend_instant_price_matching'
    });
  }
}