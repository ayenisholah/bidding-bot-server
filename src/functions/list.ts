import { config } from "dotenv";
import { IListing } from "../types/listing.types";
import { NETWORK, BASE_URL } from "../constants";
import { decryptPrivateKey } from "../utils/crypto";
import { AlchemyProvider, Wallet as Web3Wallet } from "ethers";
import { getOpenseaFloor } from "./collection";
import { logger, ErrorCategory } from "../utils/logger";
import { updateListingExpiration } from "./relistExpired";
import Listing from "../models/listing.model";
import { listOnOpensea } from "../marketplace/opensea/list";
import { listOnBlur } from "../marketplace/blur/list";
import { listOnMagicEden } from "../marketplace/magiceden/list";
import { walletCache } from "../utils/wallet-cache";
import { roundToNineDecimals } from "../utils/decimal-precision";
import { axiosInstance, limiter } from "../init";
import { API_KEY } from "..";
import { getOpenseaTraitFloor } from "../utils/opensea-trait-floor";
import { getTraitFilteredListings } from "../functions/tokenMetadata";

config()

// Color constants for console output
const GREEN = '\x1b[32m';
const YELLOW = '\x1b[33m';
const RED = '\x1b[31m';
const BLUE = '\x1b[34m';
const RESET = '\x1b[0m';

const ALCHEMY_API_KEY = process.env.ALCHEMY_API_KEY
const provider = new AlchemyProvider(NETWORK, ALCHEMY_API_KEY)

export async function createListing(data: IListing) {
  // console.log(BLUE + `[LISTING - Create] Token ID: ${data.tokenId}` + RESET);
  // console.log(BLUE + `[LISTING - Create] Running: ${data.running}` + RESET);
  // console.log(BLUE + `[LISTING - Create] Selected Marketplaces: ${data.selectedMarketplaces}` + RESET);
  // console.log(BLUE + `[LISTING - Create] Listing ID: ${data._id}` + RESET);
  // console.log(BLUE + `[LISTING - Create] Timestamp: ${new Date().toISOString()}` + RESET);

  try {
    // Only process if listing is running
    if (!data.running) {
      // console.log(YELLOW + '[LISTING - Create] Listing not running, returning early' + RESET);
      logger.logWarning(ErrorCategory.BUSINESS_LOGIC, 'Listing creation skipped - not running', {
        listingId: data._id?.toString(),
        tokenId: data.tokenId,
        contractAddress: data.contract.contractAddress,
        component: 'listing',
        operation: 'create_listing_check'
      });
      return;
    }

    
    logger.logInfo(ErrorCategory.DATABASE, 'Starting listing creation', {
      listingId: data._id?.toString(),
      tokenId: data.tokenId,
      contractAddress: data.contract.contractAddress,
      selectedMarketplaces: data.selectedMarketplaces,
      component: 'listing',
      operation: 'create_listing_start'
    });
    
    // Fetch fresh data if we have an _id
    let freshData = data;
    if (data._id && typeof data._id === 'object') {
      // console.log(BLUE + `[LISTING - Create] Step 1.1: Querying database for listing ${data._id}...` + RESET);
      const queryStartTime = Date.now();

      try {
        const freshListing = await Promise.race([
          Listing.findById(data._id),
          new Promise<any>((_, reject) =>
            setTimeout(() => reject(new Error('Database query timeout')), 30000)
          )
        ]);

        const queryTime = Date.now() - queryStartTime;
        // console.log(GREEN + `[LISTING - Create] Step 1.2: Database query completed in ${queryTime}ms` + RESET);

        if (freshListing) {
          // Use the complete fresh listing data and merge with any passed data
          freshData = {
            ...freshListing.toObject(), // Convert mongoose document to plain object
            ...data, // Override with any explicitly passed data
            finalListingPrice: freshListing.finalListingPrice || data.finalListingPrice,
            listingExpiration: freshListing.listingExpiration || data.listingExpiration,
          };
          // console.log(GREEN + '[LISTING - Create] Step 1.3: Fresh data retrieved and merged' + RESET);
          
          logger.logInfo(ErrorCategory.DATABASE, 'Fresh listing data retrieved', {
            listingId: data._id?.toString(),
            tokenId: data.tokenId,
            queryTimeMs: queryTime,
            component: 'listing',
            operation: 'fetch_fresh_data'
          });
        } else {
          // console.log(YELLOW + '[LISTING - Create] Step 1.3: No fresh listing found in database' + RESET);
          
          logger.logWarning(ErrorCategory.DATABASE, 'No fresh listing found in database', {
            listingId: data._id?.toString(),
            tokenId: data.tokenId,
            component: 'listing',
            operation: 'fetch_fresh_data'
          });
        }
      } catch (dbError) {
        console.error(RED + '[LISTING - Create] Step 1.3: Database query error' + RESET, dbError);
        
        logger.logDatabaseError('Database query failed during listing creation', {
          listingId: data._id?.toString(),
          tokenId: data.tokenId,
          contractAddress: data.contract.contractAddress,
          queryTimeMs: Date.now() - queryStartTime,
          error: dbError as Error,
          errorMessage: dbError instanceof Error ? dbError.message : 'Unknown error',
          component: 'listing',
          operation: 'fetch_fresh_data'
        });
        
        throw new Error(`Database query failed: ${dbError instanceof Error ? dbError.message : 'Unknown error'}`);
      }
    }

    // check if we're at the floor
    // if we're at the floor with greater than 1 minute  

    // console.log('Step 2: Cleaning up expired marketplace listings...');
    // Clean up expired marketplace listings
    const now = new Date();
    const marketplaces: Array<'opensea' | 'blur' | 'magiceden'> = ['opensea', 'blur', 'magiceden'];

    for (const marketplace of marketplaces) {
      const expiration = freshData.listingExpiration?.[marketplace];
      if (expiration && new Date(expiration) <= now) {
        // Expired - remove from final listing price
        if (freshData.finalListingPrice?.[marketplace] !== undefined) {
          // console.log(`Removing expired ${marketplace} listing price for token ${freshData.tokenId}`);
          delete freshData.finalListingPrice[marketplace];

          // Also update in database
          if (freshData._id) {
            await Listing.findByIdAndUpdate(freshData._id, {
              $unset: { [`finalListingPrice.${marketplace}`]: 1 }
            });
          }
        }
      }
    }

    // console.log('Step 3: Getting wallet information...');
    // Get walletId
    const walletId = freshData.walletId;
    if (!walletId) {
      throw new Error('No walletId found in listing data');
    }

    // console.log(`Step 3.1: Fetching wallet ${walletId} from cache...`);
    const walletFetchStart = Date.now();
    const wallet = await walletCache.getWalletById(walletId.toString());
    // console.log(`Step 3.2: Wallet fetched in ${Date.now() - walletFetchStart}ms`);

    // console.log('Step 3.3: Decrypting private key...');
    const private_key = decryptPrivateKey(wallet?.encryptedPrivateKey as string) as string;
    const web3Wallet = new Web3Wallet(private_key, provider)
    // console.log('Step 3.4: Wallet initialized');

    // Move collection details fetching to individual marketplace functions
    // This prevents unnecessary API calls when marketplaces don't need the data
    // console.log('Step 4: Preparing marketplace-specific data...');

    const walletAddress = freshData.wallet
    const durationInSeconds = convertToSeconds(freshData.listDuration.value, freshData.listDuration.unit);
    const selectedMarketplaces = freshData.selectedMarketplaces
    const contractAddress = freshData.contract.contractAddress
    const tokenId = freshData.tokenId
    const contractSlug = freshData.contract.slug

    // Debug: Log all listing data
    // console.log('\n=== LISTING PARAMETERS DEBUG ===');
    // console.log('Contract Address:', contractAddress);
    // console.log('Token ID:', tokenId);
    // console.log('Wallet Address:', walletAddress);
    // console.log('Selected Marketplaces:', selectedMarketplaces);
    // console.log('\nPricing Configuration:');
    // console.log('- Pricing Strategy:', freshData.pricingStrategy);
    // console.log('- Listing Price Type:', freshData.listingPriceType);
    // console.log('- Base Price:', freshData.basePrice);
    // console.log('- OpenSea Price:', freshData.openseaListingPrice);
    // console.log('- Blur Price:', freshData.blurListingPrice);
    // console.log('- Magic Eden Price:', freshData.magicEdenListingPrice);
    // console.log('- Min Listing Price:', freshData.minListingPrice);
    // console.log('- Minimum Profit:', freshData.minimumProfit);
    // console.log('- Buy Price:', freshData.buyPrice);
    // console.log('\nDuration Settings:');
    // console.log('- List Duration:', freshData.listDuration);
    // console.log('- Duration in Seconds:', durationInSeconds);
    // console.log('\nCreator Fees:');
    // Creator fees will be fetched by individual marketplaces as needed
    // console.log('\nInstant Price Matching:');
    // console.log('- Enabled:', freshData.instantPriceMatching);
    // console.log('- Undercut Margin:', freshData.underCutMargin);

    // console.log('Step 5: Marketplace-specific API calls will be handled by each marketplace...');
    // Blur access token will be fetched by Blur listing function when needed

    // console.log('Step 6: Creating listing promises for marketplaces...');
    // Create listing promises for all marketplaces
    const listingPromises = selectedMarketplaces.map(async (marketplace) => {
      const marketplaceLower = marketplace.toLowerCase() as 'opensea' | 'blur' | 'magiceden';

      // Check if this is an instant price match listing
      let listingPrice: number;
      if (freshData.instantPriceOverride !== undefined) {
        listingPrice = freshData.instantPriceOverride;
        // console.log(`Using instant price override: ${listingPrice} ETH for ${marketplace}`);

        // Enforce minimum listing price
        const minPrice = parseFloat(freshData.minListingPrice) || 0;
        if (listingPrice < minPrice) {
          // console.log(`Instant price ${listingPrice} is below minimum ${minPrice}, adjusting to minimum`);
          listingPrice = minPrice;
        }

        // Enforce minimum profit
        if (freshData.buyPrice && +freshData.minimumProfit > 0) {
          const minProfit = parseFloat(freshData.minimumProfit) || 0;
          const minPriceForProfit = freshData.buyPrice + minProfit;

          if (listingPrice < minPriceForProfit) {
            // console.log(`Instant price ${listingPrice} would result in insufficient profit, adjusting to ${minPriceForProfit} for minimum profit ${minProfit}`);
            listingPrice = minPriceForProfit;
          }
        }

        // Enforce floor price constraint if enabled
        const doNotListBelowFloor = freshData.doNotListBelowFloor === true; // Default to false
        if (doNotListBelowFloor) {
          let floorPrice: number;
          
          // Use trait floor if trait-based pricing is enabled
          if ((freshData.dynamicPricingBasis === 'HIGHEST_TRAIT_FLOOR' || freshData.dynamicPricingBasis === 'SPECIFIC_TRAIT') 
              && freshData.selectedTrait?.trait_type && freshData.selectedTrait?.value !== undefined) {
            // console.log(`Using trait floor for ${freshData.selectedTrait.trait_type}:${freshData.selectedTrait.value}`);
            floorPrice = await getOpenseaTraitFloor(freshData.contract.slug, freshData.selectedTrait.trait_type, freshData.selectedTrait.value);
          } else {
            floorPrice = await getOpenseaFloor(freshData.contract.slug);
          }
          
          if (listingPrice < floorPrice) {
            // console.log(`Instant price ${listingPrice} is below floor price ${floorPrice}, adjusting to floor price`);
            listingPrice = floorPrice;
          }
        }

        // Round to 9 decimal places after adjustments
        listingPrice = roundToNineDecimals(listingPrice);
      } else {
        const calculatedPrice = await calculateListingPrice(freshData, marketplaceLower);
        listingPrice = roundToNineDecimals(calculatedPrice);
      }

      // console.log(`\n=== ${marketplace.toUpperCase()} LISTING ===`);
      if (freshData.instantPriceOverride !== undefined) {
        // console.log('Instant Price Override:', listingPrice);
      } else {
        // console.log('Calculated Price:', listingPrice);
      }

      // Additional validation
      if (listingPrice <= 0) {
        console.error(`ERROR: Invalid listing price ${listingPrice} for ${marketplace}!`);
        // console.log('Data values used in calculation:');
        // console.log('- basePrice:', freshData.basePrice, 'type:', typeof freshData.basePrice);
        // Use proper property name for magic eden
        const propName = marketplaceLower === 'magiceden' ? 'magicEdenListingPrice' : `${marketplaceLower}ListingPrice`;
        // console.log('- marketplace specific price:', freshData[propName as keyof IListing]);
        return;
      }

      // Check if we need to adjust our listing based on undercut margin
      const shouldSkipListing = await shouldSkipListingWithUndercutCheck(
        freshData.contract.slug, 
        walletAddress, 
        listingPrice,
        parseFloat(freshData.underCutMargin) || 0.00001,
        marketplaceLower,
        (freshData.dynamicPricingBasis === 'HIGHEST_TRAIT_FLOOR' || freshData.dynamicPricingBasis === 'SPECIFIC_TRAIT') 
          && freshData.selectedTrait?.trait_type && freshData.selectedTrait?.value !== undefined 
          ? freshData.selectedTrait 
          : undefined
      );
      
      if (shouldSkipListing) {
        // We have the best listing with proper margin, check expiration time
        const marketplaceExpiration = freshData.listingExpiration?.[marketplaceLower];
        if (marketplaceExpiration) {
          const expirationTime = new Date(marketplaceExpiration).getTime();
          const now = Date.now();
          const timeUntilExpiration = expirationTime - now;
          
          if (timeUntilExpiration > 60000) { // More than 1 minute remaining
            const minutesRemaining = Math.floor(timeUntilExpiration / 60000);
            // console.log(GREEN + `[LISTING - SKIPPED] Already have optimal listing on ${marketplace} with ${minutesRemaining} minutes remaining` + RESET);
            
            logger.logInfo(ErrorCategory.BUSINESS_LOGIC, 'Listing skipped - optimal position maintained', {
              listingId: freshData._id?.toString(),
              marketplace,
              listingPrice,
              undercutMargin: freshData.underCutMargin,
              minutesRemaining,
              component: 'listing',
              operation: 'skip_optimal_listing'
            });
            return;
          } else {
            // Less than 1 minute remaining, set price to floor
            let floorPrice: number;
            
            // Use trait floor if trait-based pricing is enabled
            if ((freshData.dynamicPricingBasis === 'HIGHEST_TRAIT_FLOOR' || freshData.dynamicPricingBasis === 'SPECIFIC_TRAIT') 
                && freshData.selectedTrait?.trait_type && freshData.selectedTrait?.value !== undefined) {
              // console.log(`[LISTING - FLOOR MATCH] Using trait floor for ${freshData.selectedTrait.trait_type}:${freshData.selectedTrait.value}`);
              floorPrice = await getOpenseaTraitFloor(freshData.contract.slug, freshData.selectedTrait.trait_type, freshData.selectedTrait.value);
            } else {
              floorPrice = await getOpenseaFloor(freshData.contract.slug);
            }
            
            listingPrice = roundToNineDecimals(floorPrice);
            // console.log(YELLOW + `[LISTING - FLOOR MATCH] Best listing expiring soon on ${marketplace}, relisting at floor price: ${listingPrice} ETH` + RESET);
          }
        }
      }

      try {
        let result;
        switch (marketplaceLower) {
          case 'opensea':
            // console.log(`Calling listOnOpensea with price: ${listingPrice} ETH`);
            // console.log('Parameters:', { contractAddress, tokenId, listingPrice, contractSlug, durationInSeconds });
            result = await listOnOpensea(contractAddress, tokenId, listingPrice, web3Wallet, contractSlug, freshData.listDuration);
            break;
          case 'blur':
            // console.log(`Calling listOnBlur with price: ${listingPrice} ETH (as string: "${listingPrice.toString()}")`);
            // console.log('Parameters:', { contractAddress, tokenId, price: listingPrice.toString(), contractSlug, private_key, durationInSeconds });
            result = await listOnBlur(contractAddress, tokenId, listingPrice.toString(), web3Wallet, private_key, contractSlug, freshData.listDuration);
            break;
          case 'magiceden':
            // console.log(`Calling listOnMagicEden with price: ${listingPrice} ETH`);
            // console.log('Parameters:', { walletAddress, contractAddress, tokenId, listingPrice, durationInSeconds });
            result = await listOnMagicEden(walletAddress, contractAddress, tokenId, listingPrice, freshData.listDuration, web3Wallet);
            break;
          default:
            console.warn(`Unknown marketplace: ${marketplace}`);
            return;
        }

        // Update listing expiration if successful
        if (result && freshData._id) { //@TODO
          // if (freshData._id) {
          const expirationDate = new Date(Date.now() + durationInSeconds * 1000);
          await updateListingExpiration(freshData._id.toString(), marketplaceLower as 'opensea' | 'blur' | 'magiceden', expirationDate);

          // Update final listing price
          await Listing.findByIdAndUpdate(freshData._id, {
            [`finalListingPrice.${marketplaceLower}`]: listingPrice,
          });
        }
      } catch (error) {
        // Error is already handled gracefully in the marketplace-specific functions
        // Just throw it to be caught by Promise.allSettled
        throw error;
      }
    });

    // console.log('Step 7: Executing all marketplace listings concurrently...');
    // console.log(`Step 7.1: Total marketplaces to list on: ${listingPromises.length}`);

    const allSettledStart = Date.now();
    const results = await Promise.allSettled(listingPromises);

    // console.log(`Step 7.2: All marketplace listings completed in ${Date.now() - allSettledStart}ms`);
    // console.log('Step 7.3: Results summary:');
    results.forEach((result, index) => {
      const marketplace = selectedMarketplaces[index];
      if (result.status === 'fulfilled') {
        // console.log(`  - ${marketplace}: Success`);
      } else {
        // console.log(`  - ${marketplace}: Failed - ${result.reason}`);
      }
    });

    // console.log(GREEN + '[LISTING - Create] Step 8: createListing function completed successfully' + RESET);
    
    logger.logInfo(ErrorCategory.GENERAL, 'Listing creation completed successfully', {
      listingId: data._id?.toString(),
      tokenId: data.tokenId,
      contractAddress: data.contract.contractAddress,
      selectedMarketplaces: data.selectedMarketplaces,
      component: 'listing',
      operation: 'create_listing_complete'
    });

  } catch (error) {
    console.error(RED + `[LISTING - Create] ❌ Unexpected listing error for ${data.tokenId}` + RESET, error);
    
    // Only log unexpected errors that weren't handled by marketplace-specific functions
    if (!error || (error instanceof Error && !error.message.includes('Magic Eden') && !error.message.includes('Blur') && !error.message.includes('OpenSea'))) {
      logger.logMarketplaceError('Error creating listing', {
        listingId: data._id?.toString(),
        contractAddress: data.contract.contractAddress,
        tokenId: data.tokenId,
        selectedMarketplaces: data.selectedMarketplaces,
        operation: 'create_listing',
        error: error as Error,
        errorMessage: error instanceof Error ? error.message : String(error),
        component: 'listing'
      });
    } else {
      // Marketplace-specific error, log as debug only
      logger.logDebug(ErrorCategory.MARKETPLACE, 'Marketplace-specific listing error', {
        listingId: data._id?.toString(),
        contractAddress: data.contract.contractAddress,
        tokenId: data.tokenId,
        selectedMarketplaces: data.selectedMarketplaces,
        error: error as Error,
        errorMessage: error instanceof Error ? error.message : String(error),
        component: 'listing',
        operation: 'create_listing_marketplace_error'
      });
    }
  }
}

function convertToSeconds(value: number, unit: string): number {
  switch (unit.toLowerCase()) {
    case 'minutes':
      return value * 60;
    case 'hours':
      return value * 60 * 60;
    case 'days':
      return value * 60 * 60 * 24;
    default:
      return 900; // default to 15 minutes (900 seconds) if unit is not recognized
  }
}

/**
 * Calculates the listing price for a marketplace based on pricing strategy
 * 
 * @param data - The listing data containing pricing configuration
 * @param marketplace - The marketplace to calculate price for ('opensea', 'blur', or 'magiceden')
 * @returns The calculated listing price in ETH
 */
async function calculateListingPrice(data: IListing, marketplace: 'opensea' | 'blur' | 'magiceden'): Promise<number> {
  // console.log(`\n--- Calculating price for ${marketplace} ---`);
  // console.log('Pricing Strategy:', data.pricingStrategy);
  // console.log('Listing Price Type:', data.listingPriceType);

  let calculatedPrice: number;

  // DYNAMIC_PRICING strategy with strict constraint enforcement
  if (data.pricingStrategy === 'DYNAMIC_PRICING') {
    // Start with base calculation
    let dynamicPrice: number;

    // If buyPrice exists, use it for profit calculation
    if (data.buyPrice && +data.minimumProfit > 0) {
      dynamicPrice = +data.buyPrice + +data.minimumProfit;
      // console.log('Dynamic pricing from buyPrice:', `${data.buyPrice} + ${data.minimumProfit} = ${dynamicPrice}`);
    } else {
      // Fallback to basePrice + minimumProfit
      dynamicPrice = +data.basePrice - +data.underCutMargin;
      // console.log('Dynamic pricing from basePrice:', `${data.basePrice} - ${data.underCutMargin} = ${dynamicPrice}`);
    }

    // Apply minListingPrice constraint - never go below this
    const minPrice = parseFloat(data.minListingPrice) || 0;
    if (minPrice > 0 && dynamicPrice < minPrice) {
      // console.log(`Dynamic price ${dynamicPrice} is below minimum ${minPrice}, adjusting to minimum`);
      dynamicPrice = minPrice;
    }

    calculatedPrice = dynamicPrice;
  }
  // FIXED_PRICING strategy
  else if (data.pricingStrategy === 'FIXED_PRICING') {
    if (data.listingPriceType === 'GENERAL_LISTING_PRICE') {
      // Use the general listing price for all marketplaces
      const generalPrice = Number(data.basePrice) || 0;
      // console.log('Using general listing price:', data.basePrice, '→', generalPrice);
      calculatedPrice = generalPrice;
    } else if (data.listingPriceType === 'MARKETPLACE_LISTING_PRICE') {
      // Use marketplace-specific prices
      let marketplacePrice = 0;
      switch (marketplace) {
        case 'opensea':
          marketplacePrice = Number(data.openseaListingPrice) || Number(data.basePrice) || 0;
          // console.log('OpenSea price:', data.openseaListingPrice, 'fallback to base:', data.basePrice, '→', marketplacePrice);
          break;
        case 'blur':
          marketplacePrice = Number(data.blurListingPrice) || Number(data.basePrice) || 0;
          // console.log('Blur price:', data.blurListingPrice, 'fallback to base:', data.basePrice, '→', marketplacePrice);
          break;
        case 'magiceden':
          marketplacePrice = Number(data.magicEdenListingPrice) || Number(data.basePrice) || 0;
          // console.log('Magic Eden price:', data.magicEdenListingPrice, 'fallback to base:', data.basePrice, '→', marketplacePrice);
          break;
        default:
          marketplacePrice = Number(data.basePrice) || 0;
          // console.log('Unknown marketplace, using base price:', data.basePrice, '→', marketplacePrice);
      }
      calculatedPrice = marketplacePrice;
    } else {
      // Default fallback
      calculatedPrice = Number(data.basePrice) || 0;
    }
  } else {
    // Default fallback
    calculatedPrice = Number(data.basePrice) || 0;
  }

  // Apply floor price constraint if enabled
  const doNotListBelowFloor = data.doNotListBelowFloor === true; // Default to false if not specified
  if (doNotListBelowFloor) {
    let floorPrice: number;
    
    // Use trait floor if trait-based pricing is enabled
    if ((data.dynamicPricingBasis === 'HIGHEST_TRAIT_FLOOR' || data.dynamicPricingBasis === 'SPECIFIC_TRAIT') 
        && data.selectedTrait?.trait_type && data.selectedTrait?.value !== undefined) {
      // console.log(`Using trait floor for ${data.selectedTrait.trait_type}:${data.selectedTrait.value}`);
      floorPrice = await getOpenseaTraitFloor(data.contract.slug, data.selectedTrait.trait_type, data.selectedTrait.value);
    } else {
      floorPrice = await getOpenseaFloor(data.contract.slug);
    }
    
    if (calculatedPrice < floorPrice) {
      // console.log(`Price ${calculatedPrice} is below floor price ${floorPrice}, adjusting to floor price`);
      calculatedPrice = floorPrice;
    }
  }

  // console.log('Final price after all constraints:', calculatedPrice);
  return calculatedPrice;
}

// Interface definitions for OpenSea API response
interface ProtocolParameters {
  offerer: string;
  offer: any[];
  consideration: any[];
}

interface ProtocolData {
  parameters: ProtocolParameters;
  signature: string | null;
}

interface Price {
  current: {
    currency: string;
    decimals: number;
    value: string;
  };
}

interface OpenseaListing {
  order_hash: string;
  chain: string;
  type: string;
  price: Price;
  protocol_data: ProtocolData;
  protocol_address: string;
}

interface ListingsResponse {
  listings: OpenseaListing[];
  next: string;
}

/**
 * Check if we should skip listing based on undercut margin analysis
 * 
 * @param collectionSlug - The OpenSea collection slug
 * @param walletAddress - Our wallet address
 * @param ourListingPrice - The price we want to list at
 * @param undercutMargin - Our required undercut margin
 * @param marketplace - The marketplace being checked
 * @returns True if we should skip listing (we have optimal position), false if we should list
 */
export async function shouldSkipListingWithUndercutCheck(
  collectionSlug: string, 
  walletAddress: string, 
  ourListingPrice: number, 
  undercutMargin: number,
  marketplace: string,
  selectedTrait?: { trait_type: string; value: any }
): Promise<boolean> {
  try {
    // console.log(BLUE + `[LISTING - Undercut Check] Checking optimal listing position for ${marketplace}` + RESET);
    
    // Check if we need trait-filtered listings
    let listings: any[] = [];
    
    if (selectedTrait?.trait_type && selectedTrait?.value !== undefined) {
      
      // Get trait-filtered listings
      const traitListings = await getTraitFilteredListings(collectionSlug, selectedTrait.trait_type, selectedTrait.value, 3);
      
      // Convert to OpenSea format for consistency
      listings = traitListings.map(item => ({
        price: {
          current: {
            value: (item.price * 1e18).toString(),
            decimals: 18
          }
        },
        protocol_data: {
          parameters: {
            offerer: item.contractAddress // Note: this is not the actual offerer, we'd need additional data
          }
        }
      }));
      
      if (listings.length === 0) {
        // console.log(YELLOW + `[LISTING - Undercut Check] No trait-filtered listings found, should list` + RESET);
        return false;
      }
    } else {
      // Get the best (floor) listings to analyze the market
      const { data } = await limiter.schedule(() => axiosInstance.get<ListingsResponse>(
        `${BASE_URL}/opensea/api/v2/listings/collection/${collectionSlug}/best?limit=3`, // Get top 3 to analyze
        {
          headers: { 'x-api-key': API_KEY }
        }
      ));

      if (!data?.listings || data.listings.length === 0) {
        // console.log(YELLOW + `[LISTING - Undercut Check] No listings found, should list` + RESET);
        return false;
      }
      
      listings = data.listings;
    }

    const floorListing = listings[0];
    const floorPrice = parseFloat(floorListing.price.current.value) / Math.pow(10, floorListing.price.current.decimals);
    const floorOfferer = floorListing.protocol_data.parameters.offerer?.toLowerCase();
    
    
    // For trait-based listings, we can't easily determine the offerer, so skip that check
    if (!selectedTrait) {
      // If we are the floor listing
      if (floorOfferer === walletAddress.toLowerCase()) {
        // console.log(BLUE + `[LISTING - Undercut Check] We have the floor listing` + RESET);
        
        // Check if there's a second listing and if we're maintaining proper margin
        if (listings.length > 1) {
          const secondPrice = parseFloat(listings[1].price.current.value) / Math.pow(10, listings[1].price.current.decimals);
          const priceDifference = secondPrice - floorPrice;
          
          
          // LISTING WAR LOGIC: If difference is 0 (tie), continue the war by undercutting
          if (priceDifference === 0) {
            // console.log(YELLOW + `[LISTING - Undercut Check] Tie detected (difference = 0), continuing listing war` + RESET);
            return false;
          }
          
          // LISTING WAR STRATEGY: If difference equals our undercut margin, preemptively undercut to maintain advantage
          // This prevents competitors from easily matching our position
          if (Math.abs(priceDifference - undercutMargin) < 0.000001) {
            // console.log(YELLOW + `[LISTING - Undercut Check] Competitor at exact undercut margin (${priceDifference} ≈ ${undercutMargin}), preemptively continuing war` + RESET);
            return false;
          }
          
          // If the difference is significantly larger than our undercut margin, we might want to adjust
          if (priceDifference > undercutMargin * 2) {
            // console.log(YELLOW + `[LISTING - Undercut Check] Large gap detected (${priceDifference} > ${undercutMargin * 2}), should relist` + RESET);
            return false;
          }
        }
        
        // console.log(GREEN + `[LISTING - Undercut Check] ✅ Optimal position maintained` + RESET);
        return true;
      }
    }
    
    // We're not the floor (or checking trait-based), check if we need to undercut
    const targetPrice = floorPrice - undercutMargin;
    const priceDifference = Math.abs(ourListingPrice - targetPrice);
    
    // console.log(YELLOW + `[LISTING - Undercut Check] Should relist to maintain undercut margin` + RESET);
    return false;

  } catch (error: any) {
    console.error(RED + `[LISTING - Undercut Check] Error checking undercut position` + RESET, error);
    
    logger.logApiError('Failed to check undercut listing position', {
      collectionSlug,
      walletAddress,
      ourListingPrice,
      undercutMargin,
      marketplace,
      error: error as Error,
      errorMessage: error instanceof Error ? error.message : String(error),
      component: 'listing',
      operation: 'undercut_check'
    });
    
    // On error, don't skip - allow listing to proceed
    return false;
  }
}

/**
 * Checks if the given wallet address has the best (floor) listing for a collection
 * 
 * @param collectionSlug - The OpenSea collection slug
 * @param walletAddress - The wallet address to check against the best listing's offerer
 * @param selectedTrait - Optional trait to filter listings by
 * @returns True if the wallet has the best listing, false otherwise
 */
export async function checkIfWalletHasBestListing(
  collectionSlug: string, 
  walletAddress: string,
  selectedTrait?: { trait_type: string; value: any }
): Promise<boolean> {
  try {
    // For trait-based checks, we can't easily determine wallet ownership
    // This would require additional API calls to get listing details
    if (selectedTrait?.trait_type && selectedTrait?.value !== undefined) {
      // console.log(YELLOW + `[LISTING - Best Check] Trait-based best listing check not fully supported, returning false` + RESET);
      return false;
    }

    const { data } = await limiter.schedule(() => axiosInstance.get<ListingsResponse>(
      `${BASE_URL}/opensea/api/v2/listings/collection/${collectionSlug}/best?limit=1`,
      {
        headers: { 'x-api-key': API_KEY }
      }
    ));

    // Check if listings exist and extract offerer
    const offerer = data?.listings?.[0]?.protocol_data?.parameters?.offerer;

    if (!offerer) {
      return false;
    }

    // Case-insensitive comparison
    return offerer.toLowerCase() === walletAddress.toLowerCase();
  } catch (error: any) {
    // Handle 404 errors specifically
    if (error?.response?.status === 404) {
      await logger.logOpenseaCollectionError('Collection not found on OpenSea', {
        collection: collectionSlug,
        collectionSlug: collectionSlug,
        endpoint: `${BASE_URL}/opensea/api/v2/listings/collection/${collectionSlug}/best`,
        method: 'GET',
        statusCode: 404,
        operation: 'check_offerer',
        errorCode: 'COLLECTION_NOT_FOUND',
        errorMessage: error?.response?.data?.errors?.[0]?.message || 'Collection not found',
        error: new Error(`OpenSea collection not found: ${collectionSlug}`)
      });
      return false;
    }

    // For other errors, log as API error
    await logger.logApiError('Failed to check offerer', {
      collection: collectionSlug,
      collectionSlug: collectionSlug,
      endpoint: `${BASE_URL}/opensea/api/v2/listings/collection/${collectionSlug}/best`,
      method: 'GET',
      statusCode: error?.response?.status,
      operation: 'check_offerer',
      errorCode: error?.code || 'API_ERROR',
      errorMessage: error?.response?.data?.errors?.[0]?.message || error?.message || 'Unknown error',
      error: error
    });

    return false;
  }
}