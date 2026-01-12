import { getRedisConnection, withRedisRetry } from './redis';
import { errorLog, debugLog, infoLog } from './debug-file-logger';

/**
 * Redis-based cache for rarity token data
 * Replaces MongoDB RarityCacheModel with pure Redis implementation
 *
 * Key structure: rarity:{collectionSlug}:{rarityCategory}
 * TTL: 1 day (86400 seconds)
 */

const RARITY_KEY_PREFIX = 'rarity:';
const RARITY_TTL_SECONDS = 1 * 24 * 60 * 60; // 1 days

export interface RarityData {
  tokenIds: string[];
  totalCount: number;
  lastUpdated: string; // ISO date string
}

class RarityCache {
  /**
   * Generate Redis key for rarity data
   */
  private getKey(collectionSlug: string, rarityCategory: string): string {
    return `${RARITY_KEY_PREFIX}${collectionSlug.toLowerCase()}:${rarityCategory.toLowerCase()}`;
  }

  /**
   * Get rarity tokens from cache
   */
  async getRarityTokens(collectionSlug: string, rarityCategory: string): Promise<RarityData | null> {
    const key = this.getKey(collectionSlug, rarityCategory);

    try {
      const redis = getRedisConnection();
      const result = await withRedisRetry(async () => {
        return redis.get(key);
      }, 'RarityCache.getRarityTokens');

      if (!result) {
        return null;
      }

      const data = JSON.parse(result) as RarityData;
      debugLog('RarityCache', `GET | key=${key} | tokenCount=${data.tokenIds.length}`);
      return data;
    } catch (error) {
      errorLog('RarityCache', `Failed to get rarity tokens | key=${key}`, error);
      return null;
    }
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

    try {
      const redis = getRedisConnection();
      const data: RarityData = {
        tokenIds,
        totalCount: tokenIds.length,
        lastUpdated: new Date().toISOString(),
      };

      await withRedisRetry(async () => {
        await redis.setex(key, RARITY_TTL_SECONDS, JSON.stringify(data));
      }, 'RarityCache.setRarityTokens');

      debugLog('RarityCache', `SET | key=${key} | tokenCount=${tokenIds.length} | ttl=${RARITY_TTL_SECONDS}s`);
    } catch (error) {
      errorLog('RarityCache', `Failed to set rarity tokens | key=${key}`, error);
    }
  }

  /**
   * Delete rarity tokens from cache
   */
  async deleteRarityTokens(collectionSlug: string, rarityCategory: string): Promise<void> {
    const key = this.getKey(collectionSlug, rarityCategory);

    try {
      const redis = getRedisConnection();
      await withRedisRetry(async () => {
        await redis.del(key);
      }, 'RarityCache.deleteRarityTokens');

      debugLog('RarityCache', `DELETE | key=${key}`);
    } catch (error) {
      errorLog('RarityCache', `Failed to delete rarity tokens | key=${key}`, error);
    }
  }

  /**
   * Delete all rarity data for a collection (all categories)
   */
  async deleteCollection(collectionSlug: string): Promise<number> {
    const pattern = `${RARITY_KEY_PREFIX}${collectionSlug.toLowerCase()}:*`;

    try {
      const redis = getRedisConnection();
      let deletedCount = 0;

      await withRedisRetry(async () => {
        // Use SCAN to find all keys matching the pattern
        let cursor = '0';
        const keysToDelete: string[] = [];

        do {
          const [newCursor, keys] = await redis.scan(cursor, 'MATCH', pattern, 'COUNT', 100);
          cursor = newCursor;
          keysToDelete.push(...keys);
        } while (cursor !== '0');

        if (keysToDelete.length > 0) {
          deletedCount = await redis.del(...keysToDelete);
        }
      }, 'RarityCache.deleteCollection');

      infoLog('RarityCache', `DELETE_COLLECTION | slug=${collectionSlug} | deletedKeys=${deletedCount}`);
      return deletedCount;
    } catch (error) {
      errorLog('RarityCache', `Failed to delete collection rarity data | slug=${collectionSlug}`, error);
      return 0;
    }
  }

  /**
   * Check if rarity data exists and is fresh
   */
  async exists(collectionSlug: string, rarityCategory: string): Promise<boolean> {
    const key = this.getKey(collectionSlug, rarityCategory);

    try {
      const redis = getRedisConnection();
      const result = await withRedisRetry(async () => {
        return redis.exists(key);
      }, 'RarityCache.exists');

      return result === 1;
    } catch (error) {
      errorLog('RarityCache', `Failed to check existence | key=${key}`, error);
      return false;
    }
  }

  /**
   * Get TTL remaining for a cache entry
   */
  async getTTL(collectionSlug: string, rarityCategory: string): Promise<number> {
    const key = this.getKey(collectionSlug, rarityCategory);

    try {
      const redis = getRedisConnection();
      const ttl = await withRedisRetry(async () => {
        return redis.ttl(key);
      }, 'RarityCache.getTTL');

      return ttl ?? -1;
    } catch (error) {
      errorLog('RarityCache', `Failed to get TTL | key=${key}`, error);
      return -1;
    }
  }
}

// Export singleton instance
export const rarityCache = new RarityCache();
