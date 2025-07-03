import { config } from "dotenv";
import { IFee, IListing } from "../types/listing.types";
import { NETWORK } from "../constants";
import Wallet from "../models/wallet.model";
import { decryptPrivateKey } from "../utils/crypto";
import { AlchemyProvider, Wallet as Web3Wallet } from "ethers";
import { getCollectionDetails } from "./collection";
import { logger } from "../utils/logger";
import { listOnOpensea } from "../marketplace/opensea/list";
import { listOnBlur } from "../marketplace/blur/list";
import { getAccessToken } from "../marketplace/blur";
import { listOnMagicEden } from "../marketplace/magiceden/list";

config()

const ALCHEMY_API_KEY = process.env.ALCHEMY_API_KEY
const provider = new AlchemyProvider(NETWORK, ALCHEMY_API_KEY)

export async function createListing(data: IListing) {
  try {
    const wallet = await Wallet.findById(data.walletId.toString());
    const private_key = await decryptPrivateKey(wallet?.encryptedPrivateKey as string) as string;
    const web3Wallet = new Web3Wallet(private_key, provider)
    const collectionData = await getCollectionDetails(data.contract.slug, data.contract.contractAddress);
    const creatorFees = collectionData?.creator_fees as IFee
    const enforceCreatorFee = collectionData?.enforceCreatorFee as boolean
    const walletAddress = data.wallet
    const totalCreatorFee = enforceCreatorFee && creatorFees ? Object.values(creatorFees).reduce((sum, fee) => sum + fee, 0) : 0;
    const durationInSeconds = convertToSeconds(data.listDuration.value, data.listDuration.unit);
    const selectedMarketplaces = data.selectedMarketplaces
    const contractAddress = data.contract.contractAddress
    const tokenId = data.tokenId
    // const instantPriceMatching = data.instantPriceMatching

    // Get Blur access token first if needed
    const needsBlurToken = selectedMarketplaces.some(m => m.toLowerCase() === 'blur');
    const blurAccessToken = needsBlurToken ? await getAccessToken(private_key) : null;
    
    if (needsBlurToken && !blurAccessToken) {
      throw new Error('Failed to get Blur access token');
    }

    // Create listing promises for all marketplaces
    const listingPromises = selectedMarketplaces.map(async (marketplace) => {
      const marketplaceLower = marketplace.toLowerCase() as 'opensea' | 'blur' | 'magiceden';
      const listingPrice = parseFloat(calculateListingPrice(data, marketplaceLower).toFixed(4));

      switch (marketplaceLower) {
        case 'opensea':
          return listOnOpensea(contractAddress, tokenId, listingPrice, web3Wallet, enforceCreatorFee, durationInSeconds, creatorFees);
        case 'blur':
          return listOnBlur(contractAddress, tokenId, listingPrice.toString(), web3Wallet, blurAccessToken!, durationInSeconds, totalCreatorFee);
        case 'magiceden':
          return listOnMagicEden(walletAddress, contractAddress, tokenId, listingPrice, durationInSeconds, web3Wallet);
        default:
          console.warn(`Unknown marketplace: ${marketplace}`);
          return Promise.resolve();
      }
    });

    // Execute all listings concurrently
    await Promise.all(listingPromises);

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
  // If DYNAMIC_PRICING is set, handle it elsewhere (not implemented here)
  if (data.pricingStrategy === 'DYNAMIC_PRICING') {
    // For now, return the general listing price as fallback
    return Number(data.listingPrice) || 0;
  }

  // FIXED_PRICING strategy
  if (data.pricingStrategy === 'FIXED_PRICING') {
    if (data.listingPriceType === 'GENERAL_LISTING_PRICE') {
      // Use the general listing price for all marketplaces
      return Number(data.listingPrice) || 0;
    } else if (data.listingPriceType === 'MARKETPLACE_LISTING_PRICE') {
      // Use marketplace-specific prices
      switch (marketplace) {
        case 'opensea':
          return Number(data.openseaListingPrice) || Number(data.listingPrice) || 0;
        case 'blur':
          return Number(data.blurListingPrice) || Number(data.listingPrice) || 0;
        case 'magiceden':
          return Number(data.magicEdenListingPrice) || Number(data.listingPrice) || 0;
        default:
          return Number(data.listingPrice) || 0;
      }
    }
  }

  // Default fallback
  return Number(data.listingPrice) || 0;
}