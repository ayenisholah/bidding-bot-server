import { config } from "dotenv";
import { IFee, IListing } from "../types/listing.types";
import { ICollectionDetails } from "../types/collection.types";
import { NETWORK } from "../constants";
import Wallet from "../models/wallet.model";
import { decryptPrivateKey } from "../utils/crypto";
import { AlchemyProvider, Wallet as Web3Wallet } from "ethers";
import { getCollectionDetails } from "./collection";
import { logger, ErrorCategory } from "../utils/logger";
import { getAccessToken } from "../marketplace/blur";
import { updateListingExpiration } from "./relistExpired";
import Listing from "../models/listing.model";
import { listOnOpensea } from "../marketplace/opensea/list";
import { listOnBlur } from "../marketplace/blur/list";
import { listOnMagicEden } from "../marketplace/magiceden/list";
import { walletCache } from "../utils/wallet-cache";

config()

const ALCHEMY_API_KEY = process.env.ALCHEMY_API_KEY
const provider = new AlchemyProvider(NETWORK, ALCHEMY_API_KEY)

export async function createListing(data: IListing) {
  console.log('\n=== CREATE LISTING CALLED ===');
  console.log('Token ID:', data.tokenId);
  console.log('Running:', data.running);
  console.log('Selected Marketplaces:', data.selectedMarketplaces);

  try {
    // Only process if listing is running
    if (!data.running) {

      return;
    }

    // Fetch fresh data if we have an _id
    let freshData = data;
    if (data._id && typeof data._id === 'object') {
      const freshListing = await Listing.findById(data._id);
      if (freshListing) {
        // Use the complete fresh listing data and merge with any passed data
        freshData = {
          ...freshListing.toObject(), // Convert mongoose document to plain object
          ...data, // Override with any explicitly passed data
          finalListingPrice: freshListing.finalListingPrice || data.finalListingPrice,
          listingExpiration: freshListing.listingExpiration || data.listingExpiration,
        };
      }
    }

    // check if we're at the floor
    // if we're at the floor with greater than 1 minute  

    // Clean up expired marketplace listings
    const now = new Date();
    const marketplaces: Array<'opensea' | 'blur' | 'magiceden'> = ['opensea', 'blur', 'magiceden'];

    for (const marketplace of marketplaces) {
      const expiration = freshData.listingExpiration?.[marketplace];
      if (expiration && new Date(expiration) <= now) {
        // Expired - remove from final listing price
        if (freshData.finalListingPrice?.[marketplace] !== undefined) {
          console.log(`Removing expired ${marketplace} listing price for token ${freshData.tokenId}`);
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

    // Get walletId
    const walletId = freshData.walletId;
    if (!walletId) {
      throw new Error('No walletId found in listing data');
    }

    const wallet = await walletCache.getWalletById(walletId.toString());
    const private_key = decryptPrivateKey(wallet?.encryptedPrivateKey as string) as string;
    const web3Wallet = new Web3Wallet(private_key, provider)
    const collectionData: ICollectionDetails | null = await getCollectionDetails(freshData.contract.slug, freshData.contract.contractAddress);
    const creatorFees = collectionData?.creator_fees as IFee
    const enforceCreatorFee = collectionData?.enforceCreatorFee as boolean
    const walletAddress = freshData.wallet
    const totalCreatorFee = enforceCreatorFee && creatorFees ? Object.values(creatorFees).reduce((sum, fee) => sum + fee, 0) : 0;
    const durationInSeconds = convertToSeconds(freshData.listDuration.value, freshData.listDuration.unit);
    const selectedMarketplaces = freshData.selectedMarketplaces
    const contractAddress = freshData.contract.contractAddress
    const tokenId = freshData.tokenId

    // Debug: Log all listing data
    console.log('\n=== LISTING PARAMETERS DEBUG ===');
    console.log('Contract Address:', contractAddress);
    console.log('Token ID:', tokenId);
    console.log('Wallet Address:', walletAddress);
    console.log('Selected Marketplaces:', selectedMarketplaces);
    console.log('\nPricing Configuration:');
    console.log('- Pricing Strategy:', freshData.pricingStrategy);
    console.log('- Listing Price Type:', freshData.listingPriceType);
    console.log('- Base Price:', freshData.basePrice);
    console.log('- OpenSea Price:', freshData.openseaListingPrice);
    console.log('- Blur Price:', freshData.blurListingPrice);
    console.log('- Magic Eden Price:', freshData.magicEdenListingPrice);
    console.log('- Min Listing Price:', freshData.minListingPrice);
    console.log('- Minimum Profit:', freshData.minimumProfit);
    console.log('- Buy Price:', freshData.buyPrice);
    console.log('\nDuration Settings:');
    console.log('- List Duration:', freshData.listDuration);
    console.log('- Duration in Seconds:', durationInSeconds);
    console.log('\nCreator Fees:');
    console.log('- Enforce Creator Fee:', enforceCreatorFee);
    console.log('- Creator Fees:', creatorFees);
    console.log('- Total Creator Fee:', totalCreatorFee);
    console.log('\nInstant Price Matching:');
    console.log('- Enabled:', freshData.instantPriceMatching);
    console.log('- Undercut Margin:', freshData.underCutMargin);

    // Get Blur access token first if needed
    const needsBlurToken = selectedMarketplaces.some(m => m.toLowerCase() === 'blur');
    const blurAccessToken = needsBlurToken ? await getAccessToken(private_key) : null;

    if (needsBlurToken && !blurAccessToken) {
      throw new Error('Failed to get Blur access token');
    }
    // if we at the floor return return
    // else  adjust price

    // Create listing promises for all marketplaces
    const listingPromises = selectedMarketplaces.map(async (marketplace) => {
      const marketplaceLower = marketplace.toLowerCase() as 'opensea' | 'blur' | 'magiceden';

      // Check if this is an instant price match listing
      let listingPrice: number;
      if (freshData.instantPriceOverride !== undefined) {
        listingPrice = freshData.instantPriceOverride;
        console.log(`Using instant price override: ${listingPrice} ETH for ${marketplace}`);

        // Enforce minimum listing price
        const minPrice = parseFloat(freshData.minListingPrice) || 0;
        if (listingPrice < minPrice) {
          console.log(`Instant price ${listingPrice} is below minimum ${minPrice}, adjusting to minimum`);
          listingPrice = minPrice;
        }

        // Enforce minimum profit
        if (freshData.buyPrice && +freshData.minimumProfit > 0) {
          const minProfit = parseFloat(freshData.minimumProfit) || 0;
          const minPriceForProfit = freshData.buyPrice + minProfit;

          if (listingPrice < minPriceForProfit) {
            console.log(`Instant price ${listingPrice} would result in insufficient profit, adjusting to ${minPriceForProfit} for minimum profit ${minProfit}`);
            listingPrice = minPriceForProfit;
          }
        }

        // Round to 4 decimal places after adjustments
        listingPrice = parseFloat(listingPrice.toFixed(6));

        if (listingPrice !== freshData.instantPriceOverride) {

        }

        if (freshData.finalListingPrice && freshData.listingExpiration) {
          const currentPrice = freshData.finalListingPrice[marketplaceLower];
          const expiration = freshData.listingExpiration[marketplaceLower];

          if (currentPrice && Math.abs(currentPrice - listingPrice) < 0.0001) { // Compare with small tolerance
            console.log('Prices match within tolerance');
            if (expiration) {
              const expirationTime = new Date(expiration).getTime();
              const now = Date.now();
              const timeRemaining = expirationTime - now;

              console.log('Expiration time:', new Date(expiration));
              console.log('Current time:', new Date(now));
              console.log('Time remaining (ms):', timeRemaining);
              console.log('Time remaining (minutes):', timeRemaining / 60000);

              if (timeRemaining > 60 * 1000) { // More than 1 minute remaining
                const minutes = Math.floor(timeRemaining / 60000);
                const seconds = Math.floor((timeRemaining % 60000) / 1000);
                console.log(`Already have active listing at ${currentPrice} ETH on ${marketplace} with ${minutes}m ${seconds}s remaining, skipping`);

                // return;
              } else {
                const seconds = Math.floor(timeRemaining / 1000);
                console.log(`Existing listing at ${currentPrice} ETH expires in ${seconds}s, proceeding with relist`);
              }
            } else {
              console.log('No expiration date found');
            }
          } else {
            console.log('Prices do not match or no current price');
          }
        } else {
          console.log('No finalListingPrice or listingExpiration data');
        }
      } else {
        const calculatedPrice = calculateListingPrice(freshData, marketplaceLower);
        listingPrice = parseFloat(calculatedPrice.toFixed(6));
      }

      console.log(`\n=== ${marketplace.toUpperCase()} LISTING ===`);
      if (freshData.instantPriceOverride !== undefined) {
        console.log('Instant Price Override:', listingPrice);
      } else {
        console.log('Calculated Price:', listingPrice);
      }

      // Additional validation
      if (listingPrice <= 0) {
        console.error(`ERROR: Invalid listing price ${listingPrice} for ${marketplace}!`);
        console.log('Data values used in calculation:');
        console.log('- basePrice:', freshData.basePrice, 'type:', typeof freshData.basePrice);
        // Use proper property name for magic eden
        const propName = marketplaceLower === 'magiceden' ? 'magicEdenListingPrice' : `${marketplaceLower}ListingPrice`;
        console.log('- marketplace specific price:', freshData[propName as keyof IListing]);
        return;
      }

      try {
        let result;
        switch (marketplaceLower) {
          case 'opensea':
            console.log(`Calling listOnOpensea with price: ${listingPrice} ETH`);
            console.log('Parameters:', { contractAddress, tokenId, listingPrice, enforceCreatorFee, durationInSeconds, creatorFees });
            result = await listOnOpensea(contractAddress, tokenId, listingPrice, web3Wallet, enforceCreatorFee, freshData.listDuration, creatorFees);
            break;
          case 'blur':
            console.log(`Calling listOnBlur with price: ${listingPrice} ETH (as string: "${listingPrice.toString()}")`);
            console.log('Parameters:', { contractAddress, tokenId, price: listingPrice.toString(), durationInSeconds, totalCreatorFee });
            result = await listOnBlur(contractAddress, tokenId, listingPrice.toString(), web3Wallet, blurAccessToken!, freshData.listDuration, totalCreatorFee);
            break;
          case 'magiceden':
            console.log(`Calling listOnMagicEden with price: ${listingPrice} ETH`);
            console.log('Parameters:', { walletAddress, contractAddress, tokenId, listingPrice, durationInSeconds });
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

    // Execute all listings concurrently
    const results = await Promise.allSettled(listingPromises);

    // Results are already logged gracefully in marketplace-specific functions
    // No need for additional error logging here

  } catch (error) {
    // Only log unexpected errors that weren't handled by marketplace-specific functions
    if (!error || (error instanceof Error && !error.message.includes('Magic Eden') && !error.message.includes('Blur') && !error.message.includes('OpenSea'))) {
      await logger.logMarketplaceError('Error creating listing', {
        contractAddress: data.contract.contractAddress,
        tokenId: data.tokenId,
        selectedMarketplaces: data.selectedMarketplaces,
        operation: 'create_listing',
        error: error as Error,
        errorMessage: error instanceof Error ? error.message : String(error)
      });
      console.error(`❌ Unexpected listing error for ${data.tokenId}`);
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
function calculateListingPrice(data: IListing, marketplace: 'opensea' | 'blur' | 'magiceden'): number {
  console.log(`\n--- Calculating price for ${marketplace} ---`);
  console.log('Pricing Strategy:', data.pricingStrategy);
  console.log('Listing Price Type:', data.listingPriceType);

  // If DYNAMIC_PRICING is set, handle it elsewhere (not implemented here)
  if (data.pricingStrategy === 'DYNAMIC_PRICING') {
    // For now, return the general listing price as fallback
    if (+data.minimumProfit <= 0) return +data.basePrice
    const dynamicPrice = +data.basePrice + +data.minimumProfit || 0;
    console.log('Dynamic pricing calculation:', `${data.basePrice} + ${data.minimumProfit} = ${dynamicPrice}`);
    return dynamicPrice;
  }

  // FIXED_PRICING strategy
  if (data.pricingStrategy === 'FIXED_PRICING') {
    if (data.listingPriceType === 'GENERAL_LISTING_PRICE') {
      // Use the general listing price for all marketplaces
      const generalPrice = Number(data.basePrice) || 0;
      console.log('Using general listing price:', data.basePrice, '→', generalPrice);
      return generalPrice;
    } else if (data.listingPriceType === 'MARKETPLACE_LISTING_PRICE') {
      // Use marketplace-specific prices
      let marketplacePrice = 0;
      switch (marketplace) {
        case 'opensea':
          marketplacePrice = Number(data.openseaListingPrice) || Number(data.basePrice) || 0;
          console.log('OpenSea price:', data.openseaListingPrice, 'fallback to base:', data.basePrice, '→', marketplacePrice);
          break;
        case 'blur':
          marketplacePrice = Number(data.blurListingPrice) || Number(data.basePrice) || 0;
          console.log('Blur price:', data.blurListingPrice, 'fallback to base:', data.basePrice, '→', marketplacePrice);
          break;
        case 'magiceden':
          marketplacePrice = Number(data.magicEdenListingPrice) || Number(data.basePrice) || 0;
          console.log('Magic Eden price:', data.magicEdenListingPrice, 'fallback to base:', data.basePrice, '→', marketplacePrice);
          break;
        default:
          marketplacePrice = Number(data.basePrice) || 0;
          console.log('Unknown marketplace, using base price:', data.basePrice, '→', marketplacePrice);
      }
      return marketplacePrice;
    }
  }

  // Default fallback
  const fallbackPrice = Number(data.basePrice) || 0;
  console.log('Using fallback price:', data.basePrice, '→', fallbackPrice);
  return fallbackPrice;
}