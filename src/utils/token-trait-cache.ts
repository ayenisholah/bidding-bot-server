import { debugLog, errorLog } from './debug-file-logger';
import { LRUCache } from 'lru-cache';
import TokenTrait from '../models/token-trait.model';

// L1 Cache configuration (hot cache for frequently accessed tokens)
// Right-sized to 20K max (200 tasks × 100 active tokens × 2x safety buffer = 20K)
const L1_CACHE_SIZE = 20000; // Right-sized for 5-min hot window
const L1_CACHE_TTL = 5 * 60 * 1000; // 5 minutes (frequently accessed tokens stay hot)

// L2 is MongoDB with 30-day TTL (configured in model via expires index)

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

class TokenTraitCache {
  // L1: Hot cache (LRU for most-used tokens)
  private hotCache: LRUCache<string, TokenTraitData>;

  constructor() {
    this.hotCache = new LRUCache<string, TokenTraitData>({
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
   * Set token traits in cache (updates both L1 and L2)
   */
  async setTokenTraits(data: TokenTraitData): Promise<void> {
    const key = this.getCacheKey(data.contractAddress, data.tokenId);
    const normalizedData = {
      ...data,
      contractAddress: data.contractAddress.toLowerCase(),
      lastUpdated: data.lastUpdated || new Date()
    };

    try {
      // Update L1 (hot cache)
      this.hotCache.set(key, normalizedData);

      // Update L2 (MongoDB) - upsert to handle both create and update
      await TokenTrait.findOneAndUpdate(
        {
          contractAddress: normalizedData.contractAddress,
          tokenId: normalizedData.tokenId
        },
        {
          $set: {
            traits: normalizedData.traits,
            floorPrice: normalizedData.floorPrice,
            lastUpdated: normalizedData.lastUpdated
          }
        },
        { upsert: true, new: true }
      );

      debugLog('Cache', `Cached token traits | contract=${data.contractAddress.slice(0, 10)}... | tokenId=${data.tokenId} | traits=${data.traits.length}`);
    } catch (error) {
      errorLog('Cache', `Failed to cache token traits | contract=${data.contractAddress} | tokenId=${data.tokenId}`, error);
    }
  }

  /**
   * Get token traits from cache (checks L1 then L2)
   */
  async getTokenTraits(contractAddress: string, tokenId: string): Promise<TokenTraitData | null> {
    const key = this.getCacheKey(contractAddress, tokenId);

    try {
      // Try L1 (hot cache)
      const hot = this.hotCache.get(key);
      if (hot) {
        debugLog('Cache', `L1 hit | contract=${contractAddress.slice(0, 10)}... | tokenId=${tokenId}`);
        return hot;
      }

      // Try L2 (MongoDB)
      const doc = await TokenTrait.findOne({
        contractAddress: contractAddress.toLowerCase(),
        tokenId: tokenId
      });

      if (doc) {
        const data: TokenTraitData = {
          contractAddress: doc.contractAddress,
          tokenId: doc.tokenId,
          traits: doc.traits,
          floorPrice: doc.floorPrice,
          lastUpdated: doc.lastUpdated
        };

        // Promote to L1 (hot cache)
        this.hotCache.set(key, data);
        debugLog('Cache', `L2 hit (promoted to L1) | contract=${contractAddress.slice(0, 10)}... | tokenId=${tokenId}`);
        return data;
      }

      // Cache miss (neither L1 nor L2)
      debugLog('Cache', `L1+L2 miss | contract=${contractAddress.slice(0, 10)}... | tokenId=${tokenId}`);
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
   * Get all tokens with a specific trait (from L1 cache + MongoDB)
   */
  async getTokensWithTrait(contractAddress: string, traitType: string, traitValue: any): Promise<string[]> {
    const matchingTokenIds: string[] = [];
    const contractLower = contractAddress.toLowerCase();

    try {
      // Search L1 cache first (fast)
      const keyPrefix = `traits:${contractLower}:`;
      for (const key of this.hotCache.keys()) {
        if (key.startsWith(keyPrefix)) {
          const data = this.hotCache.get(key);
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

      // Search MongoDB for additional matches (slower but complete)
      const docs = await TokenTrait.find({
        contractAddress: contractLower,
        'traits.trait_type': traitType,
        'traits.value': traitValue
      }).select('tokenId');

      for (const doc of docs) {
        if (!matchingTokenIds.includes(doc.tokenId)) {
          matchingTokenIds.push(doc.tokenId);
        }
      }

      debugLog('Cache', `Found ${matchingTokenIds.length} tokens with trait ${traitType}=${traitValue}`);
    } catch (error) {
      errorLog('Cache', `Failed to get tokens with trait | contract=${contractAddress} | trait=${traitType}`, error);
    }

    return matchingTokenIds;
  }

  /**
   * Clear specific token from cache (both L1 and L2)
   */
  async clearToken(contractAddress: string, tokenId: string): Promise<void> {
    const key = this.getCacheKey(contractAddress, tokenId);

    try {
      // Clear from L1
      this.hotCache.delete(key);

      // Clear from L2 (MongoDB)
      await TokenTrait.deleteOne({
        contractAddress: contractAddress.toLowerCase(),
        tokenId: tokenId
      });

      debugLog('Cache', `Cleared token | contract=${contractAddress.slice(0, 10)}... | tokenId=${tokenId}`);
    } catch (error) {
      errorLog('Cache', `Failed to clear token from cache | contract=${contractAddress} | tokenId=${tokenId}`, error);
    }
  }

  /**
   * Clear all tokens for a contract (both L1 and L2)
   */
  async clearContract(contractAddress: string): Promise<void> {
    const contractLower = contractAddress.toLowerCase();
    const keyPrefix = `traits:${contractLower}:`;
    let clearedCount = 0;

    try {
      // Clear from L1
      const keysToRemove: string[] = [];
      for (const key of this.hotCache.keys()) {
        if (key.startsWith(keyPrefix)) {
          keysToRemove.push(key);
        }
      }

      for (const key of keysToRemove) {
        this.hotCache.delete(key);
        clearedCount++;
      }

      // Clear from L2 (MongoDB)
      const result = await TokenTrait.deleteMany({
        contractAddress: contractLower
      });

      clearedCount += result.deletedCount || 0;

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
    mongoCount?: number;
  }> {
    let mongoCount = 0;
    try {
      mongoCount = await TokenTrait.countDocuments();
    } catch (error) {
      errorLog('Cache', 'Failed to count MongoDB documents', error);
    }

    return {
      size: this.hotCache.size,
      maxSize: L1_CACHE_SIZE,
      ttl: L1_CACHE_TTL,
      oldestEntry: this.hotCache.size > 0 ? Date.now() - L1_CACHE_TTL : null,
      newestEntry: this.hotCache.size > 0 ? Date.now() : null,
      mongoCount // L2 storage count
    };
  }

  /**
   * Shutdown cache (clear L1, L2 persists in MongoDB)
   */
  async shutdown(): Promise<void> {
    try {
      this.hotCache.clear();
      console.log('[TOKEN TRAIT CACHE] L1 cache cleared (MongoDB L2 persists)');
    } catch (error) {
      errorLog('Cache', 'Error shutting down token trait cache', error);
    }
  }
}

// Export singleton instance
export const tokenTraitCache = new TokenTraitCache();

// Export types
export { TokenTraitType as TokenTrait, TokenTraitData };
