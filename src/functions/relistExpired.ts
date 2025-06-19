import { IListingData } from "../types/listing.types";
import Listing from "../models/listing.model";
import { createListing } from "./list";

interface MarketplaceStatus {
  marketplace: 'opensea' | 'blur' | 'magiceden';
  isExpired: boolean;
  expiresAt: Date | null;
  lastListedAt: Date | null;
}

/**
 * Check if a listing has expired for a specific marketplace
 */
export function isListingExpired(expiresAt: Date | null | undefined): boolean {
  if (!expiresAt) return true;
  return new Date() > new Date(expiresAt);
}

/**
 * Get the expiration status for all marketplaces of a listing
 */
export async function getListingExpirationStatus(listingId: string): Promise<MarketplaceStatus[]> {
  const listing = await Listing.findById(listingId);
  if (!listing) throw new Error('Listing not found');

  const statuses: MarketplaceStatus[] = [];

  if (listing.selectedMarketplaces.includes('opensea')) {
    statuses.push({
      marketplace: 'opensea',
      isExpired: isListingExpired(listing.listingExpiration?.opensea),
      expiresAt: listing.listingExpiration?.opensea || null,
      lastListedAt: listing.lastListedAt?.opensea || null
    });
  }

  if (listing.selectedMarketplaces.includes('blur')) {
    statuses.push({
      marketplace: 'blur',
      isExpired: isListingExpired(listing.listingExpiration?.blur),
      expiresAt: listing.listingExpiration?.blur || null,
      lastListedAt: listing.lastListedAt?.blur || null
    });
  }

  if (listing.selectedMarketplaces.includes('magiceden')) {
    statuses.push({
      marketplace: 'magiceden',
      isExpired: isListingExpired(listing.listingExpiration?.magiceden),
      expiresAt: listing.listingExpiration?.magiceden || null,
      lastListedAt: listing.lastListedAt?.magiceden || null
    });
  }

  return statuses;
}

/**
 * Relist expired marketplaces for a listing
 */
export async function relistExpiredMarketplaces(listingId: string): Promise<void> {
  const listing = await Listing.findById(listingId).populate('walletId');
  if (!listing || !listing.running) {
    console.log(`Listing ${listingId} not found or not running`);
    return;
  }

  const expiredMarketplaces = await getListingExpirationStatus(listingId);
  const marketplacesToRelist = expiredMarketplaces
    .filter(status => status.isExpired)
    .map(status => status.marketplace);

  if (marketplacesToRelist.length === 0) {
    console.log(`No expired marketplaces for listing ${listingId}`);
    return;
  }

  console.log(`Relisting on expired marketplaces: ${marketplacesToRelist.join(', ')}`);

  // Prepare listing data with only expired marketplaces
  const listingData: IListingData = {
    contract: listing.contract,
    tokenId: listing.tokenId,
    selectedMarketplaces: marketplacesToRelist,
    instantPriceMatching: listing.instantPriceMatching,
    listingPrice: listing.listingPrice,
    underCutMargin: listing.underCutMargin,
    openseaListingPrice: listing.openseaListingPrice,
    blurListingPrice: listing.blurListingPrice,
    magicEdenListingPrice: listing.magicEdenListingPrice,
    minProfit: listing.minProfit,
    loopInterval: listing.loopInterval,
    listDuration: listing.listDuration,
    listingPriceType: listing.listingPriceType,
    pricingStrategy: listing.pricingStrategy,
    floorPrice: listing.floorPrice,
    buyPrice: listing.buyPrice,
    collectionFloor: 0, // This should be fetched fresh if needed
    wallet: listing.wallet,
    walletId: listing.walletId
  };

  // Clear finalListingPrice for expired marketplaces before relisting
  const updateFields: any = {};
  marketplacesToRelist.forEach(marketplace => {
    updateFields[`finalListingPrice.${marketplace}`] = 0;
  });

  await Listing.findByIdAndUpdate(listingId, { $set: updateFields });

  // Relist on expired marketplaces
  await createListing(listingData);
}

/**
 * Process all active listings and relist expired ones
 */
export async function processExpiredListings(): Promise<void> {
  try {
    const activeListings = await Listing.find({ running: true, listed: true });
    
    console.log(`Processing ${activeListings.length} active listings for expiration...`);
    
    for (const listing of activeListings) {
      try {
        await relistExpiredMarketplaces(listing._id.toString());
      } catch (error) {
        console.error(`Error processing listing ${listing._id}:`, error);
      }
    }
  } catch (error) {
    console.error('Error processing expired listings:', error);
  }
}

/**
 * Calculate and update listing expiration times based on list duration
 */
export function calculateExpirationTime(durationInSeconds: number): Date {
  return new Date(Date.now() + durationInSeconds * 1000);
}