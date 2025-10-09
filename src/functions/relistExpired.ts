import Listing, { IListing } from '../models/listing.model';
import { createListing } from './list';
import { logger, ErrorCategory, MarketplaceSubCategory } from '../utils/logger';

/**
 * Check if a listing has expired on a specific marketplace
 */
function isExpiredOnMarketplace(listing: IListing, marketplace: 'opensea' | 'blur' | 'magiceden'): boolean {
  const expiration = listing.listingExpiration?.[marketplace];
  if (!expiration) return true; // No expiration set means it needs listing
  
  return new Date() > new Date(expiration);
}

/**
 * Get marketplaces where the listing has expired
 */
function getExpiredMarketplaces(listing: IListing): string[] {
  const expired: string[] = [];
  
  listing.selectedMarketplaces.forEach((marketplace) => {
    const marketplaceLower = marketplace.toLowerCase() as 'opensea' | 'blur' | 'magiceden';
    if (isExpiredOnMarketplace(listing, marketplaceLower)) {
      expired.push(marketplace);
    }
  });
  
  return expired;
}

/**
 * Relist an NFT only on marketplaces where it has expired
 */
async function relistOnExpiredMarketplaces(listing: IListing): Promise<void> {
  try {
    const expiredMarketplaces = getExpiredMarketplaces(listing);
    
    if (expiredMarketplaces.length === 0) {

      return;
    }

    // Fetch fresh listing data from database
    const freshListing = await Listing.findById(listing._id);
    if (!freshListing) {
      console.error(`Listing ${listing._id} not found when fetching fresh data for relisting`);
      return;
    }
    
    // Create a temporary listing object with only expired marketplaces
    const relistData = {
      ...freshListing.toObject(),
      selectedMarketplaces: expiredMarketplaces,
    } as unknown as IListing;
    
    // Relist only on expired marketplaces
    await createListing(relistData);
    
  } catch (error) {
    await logger.logMarketplaceError('Failed to relist expired listing', {
      listingId: listing._id.toString(),
      tokenId: listing.tokenId,
      contractAddress: listing.contract.contractAddress,
      marketplaceSubCategory: MarketplaceSubCategory.LISTING,
      error: error as Error,
      errorMessage: error instanceof Error ? error.message : String(error),
    });
  }
}

/**
 * Check all active listings for expiration and relist as needed
 */
export async function checkAndRelistExpired(): Promise<void> {
  try {
    // console.log('Checking for expired listings...');
    
    // Find all active listings
    const activeListings = await Listing.find({ 
      running: true,
      listed: true 
    });
    
    // console.log(`Found ${activeListings.length} active listings to check`);
    
    let expiredCount = 0;
    
    // Check each listing for expiration
    for (const listing of activeListings) {
      const expiredMarketplaces = getExpiredMarketplaces(listing);
      
      if (expiredMarketplaces.length > 0) {
        expiredCount++;
        await relistOnExpiredMarketplaces(listing);
      }
    }
    
    // console.log(`Processed ${expiredCount} expired listings`);

  } catch (error) {
    await logger.logMarketplaceError('Failed to check expired listings', {
      operation: 'check_expired',
      marketplaceSubCategory: MarketplaceSubCategory.LISTING,
      error: error as Error,
      errorMessage: error instanceof Error ? error.message : String(error),
    });
  }
}

/**
 * Update listing expiration for a specific marketplace
 */
export async function updateListingExpiration(
  listingId: string,
  marketplace: 'opensea' | 'blur' | 'magiceden',
  expirationDate: Date
): Promise<void> {
  try {
    await Listing.findByIdAndUpdate(listingId, {
      $set: {
        [`listingExpiration.${marketplace}`]: expirationDate,
        lastListedAt: new Date(),
      },
    });

  } catch (error) {
    await logger.logMarketplaceError('Failed to update listing expiration', {
      listingId,
      marketplace,
      operation: 'update_expiration',
      marketplaceSubCategory: MarketplaceSubCategory.LISTING,
      error: error as Error,
      errorMessage: error instanceof Error ? error.message : String(error),
    });
    throw error;
  }
}

/**
 * Initialize periodic expiration checking
 * Runs every 5 minutes by default
 */
export function initializeExpirationMonitor(intervalMinutes: number = 5): NodeJS.Timeout {
  // console.log(`Starting expiration monitor (checking every ${intervalMinutes} minutes)`);
  
  // Run immediately on startup
  checkAndRelistExpired().catch(console.error);
  
  // Then run periodically
  const interval = setInterval(() => {
    checkAndRelistExpired().catch(console.error);
  }, intervalMinutes * 60 * 1000);
  
  return interval;
}

/**
 * Stop the expiration monitor
 */
export function stopExpirationMonitor(interval: NodeJS.Timeout): void {
  clearInterval(interval);
  // console.log('Expiration monitor stopped');
}