import { wsManager } from '../index';
import { IListing } from '../types/listing.types';

/**
 * Unsubscribe a listing from WebSocket events
 * Called when a listing is stopped or instant price matching is disabled
 */
export async function unsubscribeListingFromWebSocket(listing: IListing): Promise<void> {
  if (!wsManager || !wsManager.isConnected()) {
    return;
  }

  const contractAddress = listing.contract.contractAddress;
  const contractSlug = listing.contract.slug;
  
  console.log(`\n=== WebSocket Unsubscription ===`);
  console.log(`Contract: ${contractSlug} (${contractAddress})`);
  console.log(`Token ID: ${listing.tokenId}`);
  
  for (const marketplace of listing.selectedMarketplaces) {
    try {
      await wsManager.unsubscribe(
        contractSlug,
        contractAddress,
        marketplace.toLowerCase()
      );
      console.log(`✅ Unsubscribed from ${marketplace} listing events`);
    } catch (error) {
      console.error(`❌ Failed to unsubscribe from ${marketplace}:`, error);
      // Don't throw - unsubscription failure is not critical
    }
  }
}