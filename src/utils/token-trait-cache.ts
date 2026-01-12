import { debugLog, warnLog, errorLog } from './debug-file-logger';
import { BoundedCache } from './bounded-cache';

// Cache configuration
const CACHE_SIZE = 50000; // Maximum tokens in cache (was 1000 memory + 100K MongoDB)
const CACHE_TTL = 4 * 60 * 60 * 1000; // 4 hours in milliseconds

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

class TokenTraitCache {
  private cache: BoundedCache<string, TokenTraitData>;

  constructor() {
    this.cache = new BoundedCache<string, TokenTraitData>(CACHE_SIZE, CACHE_TTL, 'tokenTraitCache');
  }

  /**
   * Get cache key for a token
   */
  private getCacheKey(contractAddress: string, tokenId: string): string {
    return `traits:${contractAddress.toLowerCase()}:${tokenId}`;
  }

  /**
   * Set token traits in cache
   */
  async setTokenTraits(data: TokenTraitData): Promise<void> {
    const key = this.getCacheKey(data.contractAddress, data.tokenId);

    try {
      this.cache.set(key, {
        ...data,
        contractAddress: data.contractAddress.toLowerCase(),
        lastUpdated: data.lastUpdated || new Date()
      });
    } catch (error) {
      errorLog('Cache', `Failed to cache token traits | contract=${data.contractAddress} | tokenId=${data.tokenId}`, error);
    }
  }

  /**
   * Get token traits from cache
   */
  async getTokenTraits(contractAddress: string, tokenId: string): Promise<TokenTraitData | null> {
    const key = this.getCacheKey(contractAddress, tokenId);

    try {
      const cached = this.cache.get(key);
      if (cached) {
        return cached;
      }
      return null;
    } catch (error) {
      errorLog('Cache', `Failed to get token traits from cache | contract=${contractAddress} | tokenId=${tokenId}`, error);
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
   * Get all tokens with a specific trait (from cache only)
   */
  getTokensWithTrait(contractAddress: string, traitType: string, traitValue: any): string[] {
    const matchingTokenIds: string[] = [];
    const contractLower = contractAddress.toLowerCase();
    const keyPrefix = `traits:${contractLower}:`;

    // Iterate through all keys in the cache
    for (const key of this.cache.keys()) {
      if (key.startsWith(keyPrefix)) {
        const data = this.cache.get(key);
        if (data) {
          const hasTrait = data.traits.some(trait =>
            trait.trait_type === traitType &&
            trait.value === traitValue
          );

          if (hasTrait) {
            matchingTokenIds.push(data.tokenId);
          }
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
      this.cache.delete(key);
    } catch (error) {
      errorLog('Cache', `Failed to clear token from cache | contract=${contractAddress} | tokenId=${tokenId}`, error);
    }
  }

  /**
   * Clear all tokens for a contract
   */
  async clearContract(contractAddress: string): Promise<void> {
    const contractLower = contractAddress.toLowerCase();
    const keyPrefix = `traits:${contractLower}:`;
    let clearedCount = 0;

    try {
      // Collect keys to remove (avoid modifying during iteration)
      const keysToRemove: string[] = [];
      for (const key of this.cache.keys()) {
        if (key.startsWith(keyPrefix)) {
          keysToRemove.push(key);
        }
      }

      // Remove collected keys
      for (const key of keysToRemove) {
        this.cache.delete(key);
        clearedCount++;
      }

      debugLog('Cache', `Cleared ${clearedCount} trait cache entries for contract ${contractAddress.slice(0, 10)}...`);
    } catch (error) {
      errorLog('Cache', `Failed to clear contract from cache | contract=${contractAddress} | keysRemoved=${clearedCount}`, error);
    }
  }

  /**
   * Get cache statistics
   */
  async getCacheStats(): Promise<{
    size: number;
    maxSize: number;
    ttl: number;
    oldestEntry: number | null;
    newestEntry: number | null;
  }> {
    return this.cache.getStats();
  }

  /**
   * Shutdown cache (no-op for in-memory cache, kept for API compatibility)
   */
  async shutdown(): Promise<void> {
    try {
      this.cache.clear();
      console.log('[TOKEN TRAIT CACHE] Cache cleared');
    } catch (error) {
      errorLog('Cache', 'Error shutting down token trait cache', error);
    }
  }
}

// Export singleton instance
export const tokenTraitCache = new TokenTraitCache();

// Export types
export { TokenTrait, TokenTraitData };
