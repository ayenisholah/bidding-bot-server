import { LRUCache } from 'lru-cache';
import { debugLog, infoLog } from './debug-file-logger';

/**
 * LRU-based cache for rarity token data
 * Replaces MongoDB RarityCacheModel with pure LRU implementation
 *
 * TTL: 1 day
 */

const RARITY_TTL = 1 * 24 * 60 * 60 * 1000; // 1 day in ms
const MAX_CACHE_SIZE = 1000000; // 1M entries (increased for production scale - support very large rarity datasets)

export interface RarityData {
  tokenIds: string[];
  totalCount: number;
  lastUpdated: string; // ISO date string
}

class RarityCache {
  private cache: LRUCache<string, RarityData>;

  constructor() {
    this.cache = new LRUCache<string, RarityData>({
      max: MAX_CACHE_SIZE,
      ttl: RARITY_TTL,
      updateAgeOnGet: true,
      updateAgeOnHas: true,
    });
  }

  /**
   * Generate cache key for rarity data
   */
  private getKey(collectionSlug: string, rarityCategory: string): string {
    return `${collectionSlug.toLowerCase()}:${rarityCategory.toLowerCase()}`;
  }

  /**
   * Get rarity tokens from cache
   */
  async getRarityTokens(collectionSlug: string, rarityCategory: string): Promise<RarityData | null> {
    const key = this.getKey(collectionSlug, rarityCategory);
    const data = this.cache.get(key);
    if (data) {
      debugLog('RarityCache', `GET | key=${key} | tokenCount=${data.tokenIds.length}`);
    }
    return data || null;
  }

  /**
   * Set rarity tokens in cache
   */
  async setRarityTokens(
    collectionSlug: string,
    rarityCategory: string,
    tokenIds: string[]
  ): Promise<void> {
    const key = this.getKey(collectionSlug, rarityCategory);
    const data: RarityData = {
      tokenIds,
      totalCount: tokenIds.length,
      lastUpdated: new Date().toISOString(),
    };

    this.cache.set(key, data);
    debugLog('RarityCache', `SET | key=${key} | tokenCount=${tokenIds.length}`);
  }

  /**
   * Delete rarity tokens from cache
   */
  async deleteRarityTokens(collectionSlug: string, rarityCategory: string): Promise<void> {
    const key = this.getKey(collectionSlug, rarityCategory);
    this.cache.delete(key);
    debugLog('RarityCache', `DELETE | key=${key}`);
  }

  /**
   * Delete all rarity data for a collection (all categories)
   */
  async deleteCollection(collectionSlug: string): Promise<number> {
    const prefix = `${collectionSlug.toLowerCase()}:`;
    let deletedCount = 0;

    for (const key of this.cache.keys()) {
      if (key.startsWith(prefix)) {
        this.cache.delete(key);
        deletedCount++;
      }
    }

    if (deletedCount > 0) {
      infoLog('RarityCache', `DELETE_COLLECTION | slug=${collectionSlug} | deletedKeys=${deletedCount}`);
    }
    return deletedCount;
  }

  /**
   * Check if rarity data exists and is fresh
   */
  async exists(collectionSlug: string, rarityCategory: string): Promise<boolean> {
    const key = this.getKey(collectionSlug, rarityCategory);
    return this.cache.has(key);
  }

  /**
   * Get remaining TTL for a cache entry (approximation)
   */
  async getTTL(collectionSlug: string, rarityCategory: string): Promise<number> {
    const key = this.getKey(collectionSlug, rarityCategory);
    const data = this.cache.get(key);

    if (!data) {
      return -1;
    }

    // LRU cache doesn't expose getRemainingTTL, so return full TTL if exists
    return RARITY_TTL / 1000; // Return in seconds for backward compatibility
  }

  /**
   * Get the internal LRU cache for debugging/monitoring
   * @internal - For use by lru-cache-sampler.ts only
   */
  public getCache(): LRUCache<string, RarityData> {
    return this.cache;
  }
}

// Export singleton instance
export const rarityCache = new RarityCache();
