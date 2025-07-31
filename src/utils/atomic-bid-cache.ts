import redisClient from './redis';

/**
 * AtomicBidCache: Redis-based cache for atomic bid deduplication
 * Uses SET NX (set if not exists) to ensure only one process can claim a bid
 */
export class AtomicBidCache {
  private keyPrefix: string;
  private ttl: number;

  constructor(keyPrefix = '{seenBids}:', ttl = 30) {
    this.keyPrefix = keyPrefix;
    this.ttl = ttl; // TTL in seconds
  }

  /**
   * Atomically check and add a bid hash to the cache
   * Returns true if the bid was successfully added (not seen before)
   * Returns false if the bid already exists (duplicate)
   */
  async checkAndAdd(bidHash: string): Promise<boolean> {
    try {
      const redis = redisClient.getClient();
      if (!redis) {
        console.error('[AtomicBidCache] Redis client not available');
        return true; // Fail open - allow processing if Redis is down
      }

      const key = `${this.keyPrefix}${bidHash}`;
      
      // SET NX EX - Set if not exists with expiration
      // Returns 'OK' if set was successful, null if key already exists
      const result = await redis.set(key, Date.now().toString(), 'PX', this.ttl * 1000, 'NX');
      
      return result === 'OK';
    } catch (error) {
      console.error('[AtomicBidCache] Error checking bid:', error);
      return true; // Fail open
    }
  }

  /**
   * Remove a bid hash from the cache
   */
  async remove(bidHash: string): Promise<boolean> {
    try {
      const redis = redisClient.getClient();
      if (!redis) {
        return false;
      }

      const key = `${this.keyPrefix}${bidHash}`;
      const result = await redis.del(key);
      
      return result === 1;
    } catch (error) {
      console.error('[AtomicBidCache] Error removing bid:', error);
      return false;
    }
  }

  /**
   * Check if a bid hash exists in the cache
   */
  async has(bidHash: string): Promise<boolean> {
    try {
      const redis = redisClient.getClient();
      if (!redis) {
        return false;
      }

      const key = `${this.keyPrefix}${bidHash}`;
      const exists = await redis.exists(key);
      
      return exists === 1;
    } catch (error) {
      console.error('[AtomicBidCache] Error checking bid existence:', error);
      return false;
    }
  }

  /**
   * Clear all bids with the current key prefix
   * Use with caution!
   */
  async clear(): Promise<void> {
    try {
      const redis = redisClient.getClient();
      if (!redis) {
        return;
      }

      const keys = await redis.keys(`${this.keyPrefix}*`);
      if (keys.length > 0) {
        await redis.del(...keys);
      }
    } catch (error) {
      console.error('[AtomicBidCache] Error clearing cache:', error);
    }
  }

  /**
   * Get cache statistics
   */
  async getStats(): Promise<{ size: number; ttl: number }> {
    try {
      const redis = redisClient.getClient();
      if (!redis) {
        return { size: 0, ttl: this.ttl };
      }

      const keys = await redis.keys(`${this.keyPrefix}*`);
      return {
        size: keys.length,
        ttl: this.ttl
      };
    } catch (error) {
      console.error('[AtomicBidCache] Error getting stats:', error);
      return { size: 0, ttl: this.ttl };
    }
  }
}

// Export a singleton instance
export const atomicBidCache = new AtomicBidCache('{seenBids}:', 30); // 30 second TTL