import Listing from "../models/listing.model";
import { logger, ErrorCategory } from "../utils/logger";

/**
 * Clean up expired final listing prices for all listings
 * This runs periodically to ensure expired prices are removed
 * regardless of whether the listing is running or not
 */
export async function cleanupExpiredPrices(): Promise<void> {
  try {
    // console.log('Starting cleanup of expired listing prices...');
    
    const now = new Date();
    const marketplaces: Array<'opensea' | 'blur' | 'magiceden'> = ['opensea', 'blur', 'magiceden'];
    
    // Find all listings that have any listing expiration dates
    const listings = await Listing.find({
      $or: [
        { 'listingExpiration.opensea': { $exists: true } },
        { 'listingExpiration.blur': { $exists: true } },
        { 'listingExpiration.magiceden': { $exists: true } }
      ]
    });
    
    // console.log(`Found ${listings.length} listings to check for expired prices`);
    
    let cleanedCount = 0;
    
    for (const listing of listings) {
      const updates: any = {};
      let hasExpired = false;
      
      // Check each marketplace
      for (const marketplace of marketplaces) {
        const expiration = listing.listingExpiration?.[marketplace];
        const hasPrice = listing.finalListingPrice?.[marketplace] !== undefined;
        
        if (expiration && hasPrice && new Date(expiration) <= now) {
          // This marketplace has expired
          updates[`finalListingPrice.${marketplace}`] = 1;
          hasExpired = true;
          // console.log(`Removing expired ${marketplace} price for listing ${listing._id} (token ${listing.tokenId})`);
        }
      }
      
      // Update the database if any prices expired
      if (hasExpired) {
        await Listing.findByIdAndUpdate(listing._id, {
          $unset: updates
        });
        cleanedCount++;
      }
    }
    
    // console.log(`Cleanup complete. Removed expired prices from ${cleanedCount} listings`);

  } catch (error) {
    console.error('Error cleaning up expired prices:', error);
    await logger.logGeneralError('Failed to clean up expired prices', {
      error: error as Error,
      errorMessage: error instanceof Error ? error.message : String(error)
    });
  }
}

/**
 * Initialize periodic cleanup of expired prices
 * Runs every 5 minutes by default
 */
export function initializeExpiredPriceCleanup(intervalMinutes: number = 5): NodeJS.Timeout {
  // console.log(`Starting expired price cleanup monitor (checking every ${intervalMinutes} minutes)`);
  
  // Run immediately on startup
  cleanupExpiredPrices().catch(console.error);
  
  // Then run periodically
  const interval = setInterval(() => {
    cleanupExpiredPrices().catch(console.error);
  }, intervalMinutes * 60 * 1000);
  
  return interval;
}

/**
 * Stop the expired price cleanup monitor
 */
export function stopExpiredPriceCleanup(interval: NodeJS.Timeout): void {
  clearInterval(interval);
  // console.log('Expired price cleanup monitor stopped');
}