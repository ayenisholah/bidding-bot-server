import { errorLog, warnLog, debugLog } from './debug-file-logger';
import TokenTraitCacheModel from '../models/tokenTraitCache.model';

// Cache configuration
const MEMORY_CACHE_SIZE = 1000; // Maximum tokens in memory
const MEMORY_TTL = 60 * 60 * 1000; // 1 hour in milliseconds
const MONGODB_CACHE_MAX_SIZE = 100000; // Maximum documents in MongoDB collection

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

      // Store in MongoDB (bounded cache with LRU eviction)
      try {
        await TokenTraitCacheModel.findOneAndUpdate(
          {
            contractAddress: data.contractAddress.toLowerCase(),
            tokenId: data.tokenId
          },
          {
            $set: {
              traits: data.traits,
              lastUpdated: data.lastUpdated || new Date(),
              accessedAt: new Date()
            }
          },
          { upsert: true, new: true }
        );

        // Check if we need to evict old entries (bounded cache)
        await this.evictMongoDBIfNeeded();
      } catch (mongoErr) {
        // MongoDB not available or error, continue with memory cache only
        warnLog('Cache', `MongoDB write failed for token trait cache | error=${(mongoErr as Error).message}`);
      }

    } catch (error) {
      errorLog('Cache', `Failed to cache token traits | contract=${data.contractAddress} | tokenId=${data.tokenId}`, error);
    }
  }

  /**
   * Evict oldest entries from MongoDB if cache exceeds max size
   */
  private async evictMongoDBIfNeeded(): Promise<void> {
    try {
      const count = await TokenTraitCacheModel.countDocuments();
      if (count > MONGODB_CACHE_MAX_SIZE) {
        // Delete oldest 10% of entries based on accessedAt
        const deleteCount = Math.floor(MONGODB_CACHE_MAX_SIZE * 0.1);
        const oldestDocs = await TokenTraitCacheModel
          .find({})
          .sort({ accessedAt: 1 })
          .limit(deleteCount)
          .select('_id');

        const idsToDelete = oldestDocs.map(doc => doc._id);
        await TokenTraitCacheModel.deleteMany({ _id: { $in: idsToDelete } });

        debugLog('Cache', `MongoDB trait cache eviction: removed ${deleteCount} oldest entries | total was ${count}`);
      }
    } catch (error) {
      // Non-critical, just log
      warnLog('Cache', `MongoDB eviction check failed | error=${(error as Error).message}`);
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

      // Check MongoDB
      try {
        const mongoDoc = await TokenTraitCacheModel.findOneAndUpdate(
          {
            contractAddress: contractAddress.toLowerCase(),
            tokenId: tokenId
          },
          { $set: { accessedAt: new Date() } }, // Update access time for LRU
          { new: true }
        );

        if (mongoDoc) {
          const data: TokenTraitData = {
            contractAddress: mongoDoc.contractAddress,
            tokenId: mongoDoc.tokenId,
            traits: mongoDoc.traits.map((t: { trait_type: string; value: any; display_type?: string; rarity?: number }) => ({
              trait_type: t.trait_type,
              value: t.value,
              display_type: t.display_type,
              rarity: t.rarity
            })),
            lastUpdated: mongoDoc.lastUpdated
          };

          // Store back in memory cache
          this.evictLRU();
          this.memoryCache.set(key, {
            data,
            expires: Date.now() + MEMORY_TTL
          });
          this.updateAccessOrder(key);

          return data;
        }
      } catch (mongoErr) {
        // MongoDB not available, continue without it
        warnLog('Cache', `MongoDB read failed for token trait cache | error=${(mongoErr as Error).message}`);
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

      // Remove from MongoDB
      try {
        await TokenTraitCacheModel.deleteOne({
          contractAddress: contractAddress.toLowerCase(),
          tokenId: tokenId
        });
      } catch (mongoErr) {
        warnLog('Cache', `MongoDB delete failed for token trait cache | error=${(mongoErr as Error).message}`);
      }

    } catch (error) {
      errorLog('Cache', `Failed to clear token from cache | contract=${contractAddress} | tokenId=${tokenId}`, error);
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

      // Remove from MongoDB
      try {
        const result = await TokenTraitCacheModel.deleteMany({
          contractAddress: contractLower
        });
        debugLog('Cache', `MongoDB cleared ${result.deletedCount} trait cache entries for contract ${contractAddress.slice(0, 10)}...`);
      } catch (mongoErr) {
        warnLog('Cache', `MongoDB bulk delete failed for token trait cache | error=${(mongoErr as Error).message}`);
      }

    } catch (error) {
      errorLog('Cache', `Failed to clear contract from cache | contract=${contractAddress} | keysRemoved=${keysToRemove.length}`, error);
    }
  }

  /**
   * Get cache statistics
   */
  async getCacheStats(): Promise<{
    memorySize: number;
    memoryHits: number;
    mongoDBSize: number;
    oldestAccess: string | null;
    newestAccess: string | null;
  }> {
    let mongoDBSize = 0;
    try {
      mongoDBSize = await TokenTraitCacheModel.countDocuments();
    } catch {
      // MongoDB not available
    }

    return {
      memorySize: this.memoryCache.size,
      memoryHits: this.accessOrder.length,
      mongoDBSize,
      oldestAccess: this.accessOrder[0] || null,
      newestAccess: this.accessOrder[this.accessOrder.length - 1] || null
    };
  }

  /**
   * Shutdown cache connections
   */
  async shutdown(): Promise<void> {
    try {
      // Just clear memory cache - MongoDB connection is shared
      this.memoryCache.clear();
      this.accessOrder = [];

      console.log('[TOKEN TRAIT CACHE] Memory cache cleared');

    } catch (error) {
      errorLog('Cache', 'Error shutting down token trait cache', error);
    }
  }
}

// Export singleton instance
export const tokenTraitCache = new TokenTraitCache();

// Export types
export { TokenTrait, TokenTraitData };