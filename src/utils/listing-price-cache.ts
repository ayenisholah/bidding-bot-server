import { LRUCache } from 'lru-cache';
import { getCacheStats } from './lru-helpers';
import redisClient, { getRedisConnection, withRedisRetry } from './redis';
import { errorLog, warnLog } from './debug-file-logger';

interface ListingPriceData {
  price: number;
  wallet: string;
  timestamp: number;
  updatedAt: string;
}

class ListingPriceCache {
  // L1: Memory cache for fast access
  private priceCache: LRUCache<string, ListingPriceData>;
  private readonly ttl: number = 24 * 60 * 60; // 24 hours in seconds

  constructor() {
    // Initialize memory cache with 10k max entries and 30 min TTL
    this.priceCache = new LRUCache<string, ListingPriceData>({
      max: 10000,
      ttl: 30 * 60 * 1000 // 30 minutes
    });
  }

  /**
   * Get the lowest cached price for a token
   * @param contractAddress - NFT contract address
   * @param tokenId - Token ID
   * @returns The lowest price or null if not cached
   */
  async getLowestPrice(contractAddress: string, tokenId: string): Promise<number | null> {
    const memoryKey = `${contractAddress}:${tokenId}`.toLowerCase();
    
    // L1: Check memory cache first
    const memoryCached = this.priceCache.get(memoryKey);
    if (memoryCached) {
      return memoryCached.price;
    }
    
    // L2: Check Redis cache
    const redisKey = `listing:price:${contractAddress}:${tokenId}`.toLowerCase();
    
    const result = await withRedisRetry(async () => {
      const redis = getRedisConnection();
      if (!redis || !redisClient.isReady()) {
        return null;
      }
      
      const cached = await redis.get(redisKey);
      if (cached) {
        const data = JSON.parse(cached) as ListingPriceData;
        // Update memory cache with Redis data
        this.priceCache.set(memoryKey, data);
        return data.price;
      }
      return null;
    }, 'getLowestPrice', 3);

    return result;
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
    const memoryKey = `${contractAddress}:${tokenId}`.toLowerCase();
    const redisKey = `listing:price:${contractAddress}:${tokenId}`.toLowerCase();
    
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
        
        // Update Redis cache
        const updated = await withRedisRetry(async () => {
          const redis = getRedisConnection();
          if (!redis || !redisClient.isReady()) {
            warnLog('ListingPriceCache', 'Redis not available, skipping cache update');
            return false;
          }

          await redis.setex(redisKey, this.ttl, JSON.stringify(data));
          return true;
        }, 'updatePrice', 3);
        
        if (updated) {
          // Update memory cache
          this.priceCache.set(memoryKey, data);
          if (!current) {
            // console.log(`[ListingPriceCache] Set initial price for ${contractAddress}:${tokenId} to ${price} ETH`);
          } else {
            // console.log(`[ListingPriceCache] Updated price for ${contractAddress}:${tokenId} from ${current} to ${price} ETH`);
          }
        }
        
        return updated || false;
      }
      
      // New price is higher or equal, skip update
      // console.log(`[ListingPriceCache] Existing price ${current} ETH is lower than or equal to new price ${price} ETH, skipping update`);
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
    const memoryKey = `${contractAddress}:${tokenId}`.toLowerCase();
    const redisKey = `listing:price:${contractAddress}:${tokenId}`.toLowerCase();
    
    try {
      // Clear memory cache
      this.priceCache.delete(memoryKey);
      
      // Clear Redis cache
      await withRedisRetry(async () => {
        const redis = getRedisConnection();
        if (!redis || !redisClient.isReady()) {
          return;
        }
        
        await redis.del(redisKey);
      }, 'clearPrice', 3);

      // console.log(`[ListingPriceCache] Cleared price for ${contractAddress}:${tokenId}`);
    } catch (error) {
      errorLog('[Cache]', `Failed to clear listing price cache - contract=${contractAddress} tokenId=${tokenId}`, error as Error);
    }
  }

  /**
   * Clear all cached prices (use with caution)
   */
  async clearAll(): Promise<void> {
    try {
      // Clear memory cache
      this.priceCache.clear();
      
      // Clear Redis cache using pattern
      await withRedisRetry(async () => {
        const redis = getRedisConnection();
        if (!redis || !redisClient.isReady()) {
          return;
        }
        
        // Use SCAN to find all listing price keys
        const keys: string[] = [];
        
        // For single node, scan directly
        const stream = redis.scanStream({
          match: 'listing:price:*',
          count: 100
        });
        
        await new Promise<void>((resolve, reject) => {
          stream.on('data', (resultKeys: string[]) => {
            keys.push(...resultKeys);
          });
          
          stream.on('end', () => {
            resolve();
          });
          
          stream.on('error', (err: any) => {
            reject(err);
          });
        });
        
        // Delete all found keys
        if (keys.length > 0) {
          const pipeline = redis.pipeline();
          keys.forEach(key => {
            pipeline.del(key);
          });
          await pipeline.exec();
          // console.log(`[ListingPriceCache] Cleared ${keys.length} cached prices`);
        }
      }, 'clearAll', 3);
    } catch (error) {
      errorLog('[Cache]', 'Failed to clear all listing price cache', error as Error);
    }
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