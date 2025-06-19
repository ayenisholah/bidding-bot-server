import { processExpiredListings } from "./relistExpired";
import Listing from "../models/listing.model";

/**
 * Interval ID for the scheduler
 */
let schedulerInterval: NodeJS.Timeout | null = null;

/**
 * Start the listing scheduler that checks for expired listings
 * @param intervalMinutes - How often to check for expired listings (default: 5 minutes)
 */
export function startListingScheduler(intervalMinutes: number = 5): void {
  if (schedulerInterval) {
    console.log('Listing scheduler is already running');
    return;
  }

  const intervalMs = intervalMinutes * 60 * 1000;
  
  console.log(`Starting listing scheduler with ${intervalMinutes} minute interval`);
  
  // Run immediately on start
  processExpiredListings().catch(console.error);
  
  // Then run periodically
  schedulerInterval = setInterval(() => {
    processExpiredListings().catch(console.error);
  }, intervalMs);
}

/**
 * Stop the listing scheduler
 */
export function stopListingScheduler(): void {
  if (schedulerInterval) {
    clearInterval(schedulerInterval);
    schedulerInterval = null;
    console.log('Listing scheduler stopped');
  }
}

/**
 * Get scheduler status
 */
export function getSchedulerStatus(): { running: boolean; interval: number | null } {
  return {
    running: schedulerInterval !== null,
    interval: schedulerInterval ? 5 : null // Default interval in minutes
  };
}

/**
 * Update finalListingPrice to 0 for expired marketplaces
 * This should be called when checking listing status
 */
export async function clearExpiredListingPrices(): Promise<void> {
  try {
    const listings = await Listing.find({ listed: true });
    
    for (const listing of listings) {
      const updates: any = {};
      let hasExpired = false;
      
      // Check each marketplace
      if (listing.listingExpiration?.opensea && new Date() > listing.listingExpiration.opensea) {
        updates['finalListingPrice.opensea'] = 0;
        hasExpired = true;
      }
      
      if (listing.listingExpiration?.blur && new Date() > listing.listingExpiration.blur) {
        updates['finalListingPrice.blur'] = 0;
        hasExpired = true;
      }
      
      if (listing.listingExpiration?.magiceden && new Date() > listing.listingExpiration.magiceden) {
        updates['finalListingPrice.magiceden'] = 0;
        hasExpired = true;
      }
      
      // Update if any marketplace has expired
      if (hasExpired) {
        await Listing.findByIdAndUpdate(listing._id, { $set: updates });
        console.log(`Cleared expired prices for listing ${listing.contract.contractAddress} #${listing.tokenId}`);
      }
    }
  } catch (error) {
    console.error('Error clearing expired listing prices:', error);
  }
}

/**
 * Get upcoming expirations for monitoring
 * @param hoursAhead - How many hours ahead to look for expirations (default: 1)
 */
export async function getUpcomingExpirations(hoursAhead: number = 1): Promise<any[]> {
  const futureTime = new Date(Date.now() + hoursAhead * 60 * 60 * 1000);
  
  const listings = await Listing.find({
    running: true,
    listed: true,
    $or: [
      { 'listingExpiration.opensea': { $lte: futureTime } },
      { 'listingExpiration.blur': { $lte: futureTime } },
      { 'listingExpiration.magiceden': { $lte: futureTime } }
    ]
  });
  
  return listings.map(listing => ({
    id: listing._id,
    contract: listing.contract.contractAddress,
    tokenId: listing.tokenId,
    expirations: {
      opensea: listing.listingExpiration?.opensea,
      blur: listing.listingExpiration?.blur,
      magiceden: listing.listingExpiration?.magiceden
    }
  }));
}