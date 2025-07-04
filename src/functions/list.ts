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

config()

const ALCHEMY_API_KEY = process.env.ALCHEMY_API_KEY
const provider = new AlchemyProvider(NETWORK, ALCHEMY_API_KEY)

export async function createListing(data: IListing) {
  try {
    console.log('\n=== INCOMING LISTING DATA ===');
    console.log(JSON.stringify(data, null, 2));
    console.log('=== END LISTING DATA ===\n');

    // Only process if listing is running
    if (!data.running) {
      await logger.logInfo(ErrorCategory.GENERAL, `Skipping listing for token ${data.tokenId} - listing is not running`);
      return;
    }

    const wallet = await Wallet.findById(data.walletId.toString());
    const private_key = await decryptPrivateKey(wallet?.encryptedPrivateKey as string) as string;
    const web3Wallet = new Web3Wallet(private_key, provider)
    const collectionData: ICollectionDetails | null = await getCollectionDetails(data.contract.slug, data.contract.contractAddress);
    const creatorFees = collectionData?.creator_fees as IFee
    const enforceCreatorFee = collectionData?.enforceCreatorFee as boolean
    const walletAddress = data.wallet
    const totalCreatorFee = enforceCreatorFee && creatorFees ? Object.values(creatorFees).reduce((sum, fee) => sum + fee, 0) : 0;
    const durationInSeconds = convertToSeconds(data.listDuration.value, data.listDuration.unit);
    const selectedMarketplaces = data.selectedMarketplaces
    const contractAddress = data.contract.contractAddress
    const tokenId = data.tokenId

    // Debug: Log all listing data
    console.log('\n=== LISTING PARAMETERS DEBUG ===');
    console.log('Contract Address:', contractAddress);
    console.log('Token ID:', tokenId);
    console.log('Wallet Address:', walletAddress);
    console.log('Selected Marketplaces:', selectedMarketplaces);
    console.log('\nPricing Configuration:');
    console.log('- Pricing Strategy:', data.pricingStrategy);
    console.log('- Listing Price Type:', data.listingPriceType);
    console.log('- Base Price:', data.basePrice);
    console.log('- OpenSea Price:', data.openseaListingPrice);
    console.log('- Blur Price:', data.blurListingPrice);
    console.log('- Magic Eden Price:', data.magicEdenListingPrice);
    console.log('- Min Listing Price:', data.minListingPrice);
    console.log('- Minimum Profit:', data.minimumProfit);
    console.log('- Buy Price:', data.buyPrice);
    console.log('\nDuration Settings:');
    console.log('- List Duration:', data.listDuration);
    console.log('- Duration in Seconds:', durationInSeconds);
    console.log('\nCreator Fees:');
    console.log('- Enforce Creator Fee:', enforceCreatorFee);
    console.log('- Creator Fees:', creatorFees);
    console.log('- Total Creator Fee:', totalCreatorFee);
    console.log('\nInstant Price Matching:');
    console.log('- Enabled:', data.instantPriceMatching);
    console.log('- Undercut Margin:', data.underCutMargin);

    // Get Blur access token first if needed
    const needsBlurToken = selectedMarketplaces.some(m => m.toLowerCase() === 'blur');
    const blurAccessToken = needsBlurToken ? await getAccessToken(private_key) : null;

    if (needsBlurToken && !blurAccessToken) {
      throw new Error('Failed to get Blur access token');
    }

    // Create listing promises for all marketplaces
    const listingPromises = selectedMarketplaces.map(async (marketplace) => {
      const marketplaceLower = marketplace.toLowerCase() as 'opensea' | 'blur' | 'magiceden';
      const calculatedPrice = calculateListingPrice(data, marketplaceLower);
      const listingPrice = parseFloat(calculatedPrice.toFixed(4));

      console.log(`\n=== ${marketplace.toUpperCase()} LISTING ===`);
      console.log('Calculated Price (raw):', calculatedPrice);
      console.log('Listing Price (rounded):', listingPrice);

      // Additional validation
      if (listingPrice <= 0) {
        console.error(`ERROR: Invalid listing price ${listingPrice} for ${marketplace}!`);
        console.log('Data values used in calculation:');
        console.log('- basePrice:', data.basePrice, 'type:', typeof data.basePrice);
        // Use proper property name for magic eden
        const propName = marketplaceLower === 'magiceden' ? 'magicEdenListingPrice' : `${marketplaceLower}ListingPrice`;
        console.log('- marketplace specific price:', data[propName as keyof IListing]);
        return;
      }

      try {
        let result;
        switch (marketplaceLower) {
          case 'opensea':
            console.log(`Calling listOnOpensea with price: ${listingPrice} ETH`);
            console.log('Parameters:', { contractAddress, tokenId, listingPrice, enforceCreatorFee, durationInSeconds, creatorFees });
            // result = await listOnOpensea(contractAddress, tokenId, listingPrice, web3Wallet, enforceCreatorFee, durationInSeconds, creatorFees);
            break;
          case 'blur':
            console.log(`Calling listOnBlur with price: ${listingPrice} ETH (as string: "${listingPrice.toString()}")`);
            console.log('Parameters:', { contractAddress, tokenId, price: listingPrice.toString(), durationInSeconds, totalCreatorFee });
            // result = await listOnBlur(contractAddress, tokenId, listingPrice.toString(), web3Wallet, blurAccessToken!, durationInSeconds, totalCreatorFee);
            break;
          case 'magiceden':
            console.log(`Calling listOnMagicEden with price: ${listingPrice} ETH`);
            console.log('Parameters:', { walletAddress, contractAddress, tokenId, listingPrice, durationInSeconds });
            // result = await listOnMagicEden(walletAddress, contractAddress, tokenId, listingPrice, durationInSeconds, web3Wallet);
            break;
          default:
            console.warn(`Unknown marketplace: ${marketplace}`);
            return;
        }

        // Update listing expiration if successful
        // if (result && data._id) { @TODO
        if (data._id) {
          const expirationDate = new Date(Date.now() + durationInSeconds * 1000);
          await updateListingExpiration(data._id.toString(), marketplaceLower as 'opensea' | 'blur' | 'magiceden', expirationDate);

          // Update final listing price
          await Listing.findByIdAndUpdate(data._id, {
            [`finalListingPrice.${marketplaceLower}`]: listingPrice,
          });
        }
      } catch (error) {
        console.error(`Failed to list on ${marketplace}:`, error);
        throw error;
      }
    });

    // Execute all listings concurrently
    await Promise.allSettled(listingPromises);

  } catch (error) {
    await logger.logMarketplaceError('Error creating listing', {
      contractAddress: data.contract.contractAddress,
      tokenId: data.tokenId,
      selectedMarketplaces: data.selectedMarketplaces,
      operation: 'create_listing',
      error: error as Error,
      errorMessage: error instanceof Error ? error.message : String(error)
    });
    console.log(error);
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
      return 900; // assume already in seconds
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