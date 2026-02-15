import { errorLog } from './debug-file-logger';
import { LRUCache } from 'lru-cache';

// LRU Cache configuration (single-tier, no MongoDB)
const L1_CACHE_SIZE = 1000000; // Max 1M token trait entries (increased for production scale)
const L1_CACHE_TTL = 24 * 60 * 60 * 1000; // 24 hours TTL

interface TokenTraitType {
  trait_type: string;
  value: any;
  display_type?: string;
  rarity?: number;
}

interface TokenTraitData {
  tokenId: string;
  contractAddress: string;
  traits: TokenTraitType[];
  lastUpdated: Date;
  floorPrice?: number; // Trait-specific floor price if applicable
}

// Cache hit/miss tracking
interface TokenTraitCacheStats {
  hits: number;
  misses: number;
  sets: number;
  totalRequests: number;
  hitRate: string;
  size: number;
  maxSize: number;
}

const cacheStats: Omit<TokenTraitCacheStats, 'hitRate' | 'size' | 'maxSize'> = {
  hits: 0,
  misses: 0,
  sets: 0,
  totalRequests: 0
};

// Log stats every 5 minutes
setInterval(() => {
  if (cacheStats.totalRequests > 0) {
    const stats = getTokenTraitCacheStats();
  }
}, 300000); // 5 minutes

class TokenTraitCache {
  // LRU cache (single-tier, no MongoDB)
  public tokenTraitsLRU: LRUCache<string, TokenTraitData>;

  constructor() {
    this.tokenTraitsLRU = new LRUCache<string, TokenTraitData>({
      max: L1_CACHE_SIZE,
      ttl: L1_CACHE_TTL,
      updateAgeOnGet: true, // LRU behavior - keep hot tokens fresh
      allowStale: false
    });
  }

  /**
   * Get cache key for a token
   */
  private getCacheKey(contractAddress: string, tokenId: string): string {
    return `traits:${contractAddress.toLowerCase()}:${tokenId}`;
  }

  /**
   * Set token traits in cache (LRU only - no MongoDB)
   */
  async setTokenTraits(data: TokenTraitData): Promise<void> {
    const key = this.getCacheKey(data.contractAddress, data.tokenId);
    const normalizedData = {
      ...data,
      contractAddress: data.contractAddress.toLowerCase(),
      lastUpdated: data.lastUpdated || new Date()
    };

    try {
      // Update L1 cache only (MongoDB removed!)
      this.tokenTraitsLRU.set(key, normalizedData);
      cacheStats.sets++;


    } catch (error) {
      errorLog('Cache', `Failed to cache token traits | contract=${data.contractAddress} | tokenId=${data.tokenId}`, error);
    }
  }

  /**
   * Get token traits from cache (LRU only - no MongoDB)
   */
  async getTokenTraits(contractAddress: string, tokenId: string): Promise<TokenTraitData | null> {
    const key = this.getCacheKey(contractAddress, tokenId);
    cacheStats.totalRequests++;

    try {
      // Try L1 cache (only cache tier!)
      const cached = this.tokenTraitsLRU.get(key);
      if (cached) {
        cacheStats.hits++;
        // CACHE_HIT log removed - was generating 11.3M lines/day (1.2GB)
        return cached;
      }

      // Cache miss - return null, let caller fetch from API
      cacheStats.misses++;
      return null;

    } catch (error) {
      errorLog('Cache', `Failed to get token traits from cache | contract=${contractAddress} | tokenId=${tokenId}`, error);
      cacheStats.misses++;
      return null;
    }
  }

  /**
   * Check if a token has a specific trait
   */
  async tokenHasTrait(
    contractAddress: string,
    tokenId: string,
    traitType: string,
    traitValue: any
  ): Promise<boolean> {
    const traits = await this.getTokenTraits(contractAddress, tokenId);

    if (!traits) {
      return false;
    }

    return traits.traits.some(trait =>
      trait.trait_type === traitType &&
      trait.value === traitValue
    );
  }


  /**
   * Get cache statistics
   */
  async getCacheStats(): Promise<TokenTraitCacheStats> {
    const hitRate = cacheStats.totalRequests > 0
      ? ((cacheStats.hits / cacheStats.totalRequests) * 100).toFixed(2) + '%'
      : '0%';

    return {
      size: this.tokenTraitsLRU.size,
      maxSize: L1_CACHE_SIZE,
      hits: cacheStats.hits,
      misses: cacheStats.misses,
      sets: cacheStats.sets,
      totalRequests: cacheStats.totalRequests,
      hitRate
    };
  }

  /**
   * Shutdown cache (clear LRU)
   */
  async shutdown(): Promise<void> {
    try {
      this.tokenTraitsLRU.clear();
      console.log('[TOKEN TRAIT CACHE] Cache cleared');
    } catch (error) {
      errorLog('Cache', 'Error shutting down token trait cache', error);
    }
  }

  /**
   * Get the internal LRU cache for debugging/monitoring
   * @internal - For use by lru-cache-sampler.ts only
   */
  public getCache(): LRUCache<string, TokenTraitData> {
    return this.tokenTraitsLRU;
  }
}

// Export singleton instance
export const tokenTraitCache = new TokenTraitCache();

// Export types
export { TokenTraitType as TokenTrait, TokenTraitData };

// Export stats function for health endpoint
export function getTokenTraitCacheStats(): TokenTraitCacheStats {
  const hitRate = cacheStats.totalRequests > 0
    ? ((cacheStats.hits / cacheStats.totalRequests) * 100).toFixed(2) + '%'
    : '0%';

  return {
    size: tokenTraitCache.tokenTraitsLRU?.size || 0,
    maxSize: L1_CACHE_SIZE,
    hits: cacheStats.hits,
    misses: cacheStats.misses,
    sets: cacheStats.sets,
    totalRequests: cacheStats.totalRequests,
    hitRate
  };
}
