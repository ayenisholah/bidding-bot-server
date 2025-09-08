import { logger, ErrorCategory } from './logger';
import { getRedisConnection } from './redis';

// Color constants for console output
const RED = '\x1b[31m';
const RESET = '\x1b[0m';

// Cache configuration
const MEMORY_CACHE_SIZE = 1000; // Maximum tokens in memory
const REDIS_TTL = 7 * 24 * 60 * 60; // 7 days in seconds
const MEMORY_TTL = 60 * 60 * 1000; // 1 hour in milliseconds

interface TokenTrait {
  trait_type: string;
  value: any;
  display_type?: string;
  rarity?: number;
}

interface TokenTraitData {
  tokenId: string;
  contractAddress: string;
  traits: TokenTrait[];
  lastUpdated: Date;
  floorPrice?: number; // Trait-specific floor price if applicable
}

interface MemoryCacheEntry {
  data: TokenTraitData;
  expires: number;
}

class TokenTraitCache {
  private memoryCache: Map<string, MemoryCacheEntry> = new Map();
  private accessOrder: string[] = []; // LRU tracking

  constructor() {
    // No need for initialization - we'll use the shared Redis client on demand
  }

  /**
   * Get cache key for a token
   */
  private getCacheKey(contractAddress: string, tokenId: string): string {
    return `traits:${contractAddress.toLowerCase()}:${tokenId}`;
  }

  /**
   * Update LRU access order
   */
  private updateAccessOrder(key: string): void {
    const index = this.accessOrder.indexOf(key);
    if (index > -1) {
      this.accessOrder.splice(index, 1);
    }
    this.accessOrder.push(key);
  }

  /**
   * Evict least recently used entries if cache is full
   */
  private evictLRU(): void {
    while (this.memoryCache.size >= MEMORY_CACHE_SIZE && this.accessOrder.length > 0) {
      const keyToEvict = this.accessOrder.shift();
      if (keyToEvict) {
        this.memoryCache.delete(keyToEvict);
        // console.log(YELLOW + `[TOKEN TRAIT CACHE] Evicted LRU entry: ${keyToEvict}` + RESET);
      }
    }
  }

  /**
   * Set token traits in cache
   */
  async setTokenTraits(data: TokenTraitData): Promise<void> {
    const key = this.getCacheKey(data.contractAddress, data.tokenId);
    
    try {
      // Store in memory cache with expiration
      this.evictLRU();
      this.memoryCache.set(key, {
        data,
        expires: Date.now() + MEMORY_TTL
      });
      this.updateAccessOrder(key);

      // Store in Redis if available
      try {
        const redis = getRedisConnection();
        if (redis && redis.status === 'ready') {
          await redis.setex(
            key,
            REDIS_TTL,
            JSON.stringify(data)
          );
        }
      } catch {
        // Redis not available, continue with memory cache only
        console.warn('[TOKEN TRAIT CACHE] Redis not available for write, using memory cache only');
      }

      // console.log(GREEN + `[TOKEN TRAIT CACHE] Cached traits for ${key}` + RESET);
      logger.logInfo(ErrorCategory.DATABASE, 'Token traits cached successfully', {
        contractAddress: data.contractAddress,
        tokenId: data.tokenId,
        traitCount: data.traits.length,
        component: 'token_trait_cache',
        operation: 'set_token_traits'
      });

    } catch (error) {
      console.error(RED + `[TOKEN TRAIT CACHE] Failed to cache traits for ${key}` + RESET, error);
      logger.logCacheError('write', 'token_trait', 'Failed to cache token traits', {
        contractAddress: data.contractAddress,
        tokenId: data.tokenId,
        error: error as Error,
        errorMessage: error instanceof Error ? error.message : String(error),
        component: 'token_trait_cache',
        operation: 'set_token_traits'
      });
    }
  }

  /**
   * Get token traits from cache
   */
  async getTokenTraits(contractAddress: string, tokenId: string): Promise<TokenTraitData | null> {
    const key = this.getCacheKey(contractAddress, tokenId);

    try {
      // Check memory cache first
      const memoryCacheEntry = this.memoryCache.get(key);
      if (memoryCacheEntry && memoryCacheEntry.expires > Date.now()) {
        this.updateAccessOrder(key);
        // console.log(BLUE + `[TOKEN TRAIT CACHE] Memory hit for ${key}` + RESET);
        return memoryCacheEntry.data;
      }

      // Remove expired entry from memory
      if (memoryCacheEntry) {
        this.memoryCache.delete(key);
        const index = this.accessOrder.indexOf(key);
        if (index > -1) {
          this.accessOrder.splice(index, 1);
        }
      }

      // Check Redis if available
      try {
        const redis = getRedisConnection();
        if (redis && redis.status === 'ready') {
          const redisData = await redis.get(key);
          if (redisData) {
            const data = JSON.parse(redisData) as TokenTraitData;
            
            // Store back in memory cache
            this.evictLRU();
            this.memoryCache.set(key, {
              data,
              expires: Date.now() + MEMORY_TTL
            });
            this.updateAccessOrder(key);
            
            // console.log(CYAN + `[TOKEN TRAIT CACHE] Redis hit for ${key}` + RESET);
            return data;
          }
        }
      } catch {
        // Redis not available, continue without it
        console.warn('[TOKEN TRAIT CACHE] Redis not available for read');
      }

      return null;

    } catch (error) {
      console.error(RED + `[TOKEN TRAIT CACHE] Failed to get traits for ${key}` + RESET, error);
      logger.logCacheError('read', 'token_trait', 'Failed to get token traits from cache', {
        contractAddress,
        tokenId,
        error: error as Error,
        errorMessage: error instanceof Error ? error.message : String(error),
        component: 'token_trait_cache',
        operation: 'get_token_traits'
      });
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
   * Get all tokens with a specific trait (from memory cache only)
   */
  getTokensWithTrait(contractAddress: string, traitType: string, traitValue: any): string[] {
    const matchingTokenIds: string[] = [];
    const contractLower = contractAddress.toLowerCase();

    for (const [key, entry] of this.memoryCache.entries()) {
      if (entry.expires > Date.now() && key.startsWith(`traits:${contractLower}:`)) {
        const hasTrait = entry.data.traits.some(trait =>
          trait.trait_type === traitType &&
          trait.value === traitValue
        );
        
        if (hasTrait) {
          matchingTokenIds.push(entry.data.tokenId);
        }
      }
    }

    return matchingTokenIds;
  }

  /**
   * Clear specific token from cache
   */
  async clearToken(contractAddress: string, tokenId: string): Promise<void> {
    const key = this.getCacheKey(contractAddress, tokenId);

    try {
      // Remove from memory cache
      this.memoryCache.delete(key);
      const index = this.accessOrder.indexOf(key);
      if (index > -1) {
        this.accessOrder.splice(index, 1);
      }

      // Remove from Redis if available
      try {
        const redis = getRedisConnection();
        if (redis && redis.status === 'ready') {
          await redis.del(key);
        }
      } catch {
        // Redis not available, continue
        console.warn('[TOKEN TRAIT CACHE] Redis not available for delete');
      }

      // console.log(YELLOW + `[TOKEN TRAIT CACHE] Cleared cache for ${key}` + RESET);

    } catch (error) {
      console.error(RED + `[TOKEN TRAIT CACHE] Failed to clear cache for ${key}` + RESET, error);
      logger.logCacheError('delete', 'token_trait', 'Failed to clear token from cache', {
        contractAddress,
        tokenId,
        error: error as Error,
        errorMessage: error instanceof Error ? error.message : String(error),
        component: 'token_trait_cache',
        operation: 'clear_token'
      });
    }
  }

  /**
   * Clear all tokens for a contract
   */
  async clearContract(contractAddress: string): Promise<void> {
    const contractLower = contractAddress.toLowerCase();
    const keysToRemove: string[] = [];

    try {
      // Find all keys for this contract in memory
      for (const key of this.memoryCache.keys()) {
        if (key.startsWith(`traits:${contractLower}:`)) {
          keysToRemove.push(key);
        }
      }

      // Remove from memory cache
      for (const key of keysToRemove) {
        this.memoryCache.delete(key);
        const index = this.accessOrder.indexOf(key);
        if (index > -1) {
          this.accessOrder.splice(index, 1);
        }
      }

      // Remove from Redis if available
      try {
        const redis = getRedisConnection();
        if (redis && redis.status === 'ready' && keysToRemove.length > 0) {
          await redis.del(...keysToRemove);
        }
      } catch {
        // Redis not available, continue
        console.warn('[TOKEN TRAIT CACHE] Redis not available for bulk delete');
      }

      // console.log(YELLOW + `[TOKEN TRAIT CACHE] Cleared ${keysToRemove.length} entries for contract ${contractAddress}` + RESET);

    } catch (error) {
      console.error(RED + `[TOKEN TRAIT CACHE] Failed to clear contract cache` + RESET, error);
      logger.logCacheError('delete', 'token_trait', 'Failed to clear contract from cache', {
        contractAddress,
        keysRemoved: keysToRemove.length,
        error: error as Error,
        errorMessage: error instanceof Error ? error.message : String(error),
        component: 'token_trait_cache',
        operation: 'clear_contract'
      });
    }
  }

  /**
   * Get cache statistics
   */
  getCacheStats(): {
    memorySize: number;
    memoryHits: number;
    totalKeys: number;
    oldestAccess: string | null;
    newestAccess: string | null;
  } {
    return {
      memorySize: this.memoryCache.size,
      memoryHits: this.accessOrder.length,
      totalKeys: this.memoryCache.size,
      oldestAccess: this.accessOrder[0] || null,
      newestAccess: this.accessOrder[this.accessOrder.length - 1] || null
    };
  }

  /**
   * Shutdown cache connections
   */
  async shutdown(): Promise<void> {
    try {
      // No need to close Redis connection as it's shared
      // Just clear memory cache
      this.memoryCache.clear();
      this.accessOrder = [];
      
      console.log('[TOKEN TRAIT CACHE] Memory cache cleared');
      
    } catch (error) {
      console.error(RED + '[TOKEN TRAIT CACHE] Error during shutdown' + RESET, error);
      logger.logGeneralError('Error shutting down token trait cache', {
        error: error as Error,
        errorMessage: error instanceof Error ? error.message : String(error),
        component: 'token_trait_cache',
        operation: 'shutdown'
      });
    }
  }
}

// Export singleton instance
export const tokenTraitCache = new TokenTraitCache();

// Export types
export { TokenTrait, TokenTraitData };