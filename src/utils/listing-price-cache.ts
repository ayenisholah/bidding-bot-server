import { LRUCache } from 'lru-cache';
import { getCacheStats } from './lru-helpers';
import { errorLog } from './debug-file-logger';

interface ListingPriceData {
  price: number;
  wallet: string;
  timestamp: number;
  updatedAt: string;
}

class ListingPriceCache {
  // Single-tier LRU cache (Phase 2 Redis → LRU Migration)
  private priceCache: LRUCache<string, ListingPriceData>;

  constructor() {
    // Initialize memory cache with 10k max entries and 24 hour TTL (matches old Redis TTL)
    this.priceCache = new LRUCache<string, ListingPriceData>({
      max: 10000,
      ttl: 24 * 60 * 60 * 1000 // 24 hours (was 30 minutes)
    });
  }

  /**
   * Get the lowest cached price for a token
   * @param contractAddress - NFT contract address
   * @param tokenId - Token ID
   * @returns The lowest price or null if not cached
   */
  async getLowestPrice(contractAddress: string, tokenId: string): Promise<number | null> {
    const key = `${contractAddress}:${tokenId}`.toLowerCase();
    const cached = this.priceCache.get(key);
    return cached ? cached.price : null;
  }

  /**
   * Update the cached price for a token
   * @param contractAddress - NFT contract address
   * @param tokenId - Token ID
   * @param price - New listing price
   * @param wallet - Wallet address that created the listing
   * @returns true if the price was updated, false if existing price was lower
   */
  async updatePrice(contractAddress: string, tokenId: string, price: number, wallet: string): Promise<boolean> {
    const key = `${contractAddress}:${tokenId}`.toLowerCase();

    try {
      // Get current lowest price
      const current = await this.getLowestPrice(contractAddress, tokenId);

      // Only update if no existing price or new price is lower (exact comparison, no rounding)
      if (!current || price < current) {
        const data: ListingPriceData = {
          price: price, // Store exact raw price without any rounding
          wallet: wallet.toLowerCase(),
          timestamp: Date.now(),
          updatedAt: new Date().toISOString()
        };

        // Update LRU cache
        this.priceCache.set(key, data);
        return true;
      }

      // New price is higher or equal, skip update
      return false;
    } catch (error) {
      errorLog('[Cache]', `Failed to update listing price cache - contract=${contractAddress} tokenId=${tokenId} price=${price}`, error as Error);
      return false;
    }
  }

  /**
   * Clear the cached price for a token
   * @param contractAddress - NFT contract address
   * @param tokenId - Token ID
   */
  async clearPrice(contractAddress: string, tokenId: string): Promise<void> {
    const key = `${contractAddress}:${tokenId}`.toLowerCase();
    this.priceCache.delete(key);
  }

  /**
   * Clear all cached prices (use with caution)
   */
  async clearAll(): Promise<void> {
    this.priceCache.clear();
  }

  /**
   * Get cache statistics
   */
  getStats() {
    return {
      memoryCacheSize: this.priceCache.size,
      memoryCacheStats: getCacheStats(this.priceCache)
    };
  }

  /**
   * Get the internal LRU cache for debugging/monitoring
   * @internal - For use by lru-cache-sampler.ts only
   */
  public getCache(): LRUCache<string, ListingPriceData> {
    return this.priceCache;
  }
}

// Export singleton instance
export const listingPriceCache = new ListingPriceCache();