import { getTraitFloorPrice } from '../functions/tokenMetadata';
import { errorLog, debugLog } from './debug-file-logger';
import {
  traitFloorLRU,
  trackTraitFloorHit,
  trackTraitFloorMiss
} from '../marketplace/opensea/cache';

/**
 * Get OpenSea floor price for a specific trait with LRU caching
 */
export async function getOpenseaTraitFloor(
  collectionSlug: string,
  traitType: string,
  traitValue: any
): Promise<number> {
  const cacheKey = `${collectionSlug.toLowerCase()}:${traitType}:${traitValue}`;

  // Check LRU cache (< 1ms)
  const cached = traitFloorLRU.get(cacheKey);
  if (cached !== undefined) {
    trackTraitFloorHit();
    debugLog('Performance', `[OS-TraitFloor] LRU_HIT (<1ms) | ${collectionSlug}:${traitType}:${traitValue} | price=${cached}`);
    return cached;
  }

  // Fetch from API (800-2500ms)
  trackTraitFloorMiss();
  debugLog('Performance', `[OS-TraitFloor] CACHE_MISS | ${collectionSlug}:${traitType}:${traitValue} | fetching...`);
  const fetchStart = Date.now();

  try {
    // Fetch from API
    const floorPrice = await getTraitFloorPrice(collectionSlug, traitType, traitValue);
    const fetchTime = Date.now() - fetchStart;

    if (floorPrice !== null && floorPrice > 0) {
      traitFloorLRU.set(cacheKey, floorPrice);
      debugLog('Performance', `[OS-TraitFloor] API_FETCH | ${collectionSlug}:${traitType}:${traitValue} | price=${floorPrice} | fetchTime=${fetchTime}ms`);
      return floorPrice;
    }

    debugLog('Performance', `[OS-TraitFloor] API_FETCH | ${collectionSlug}:${traitType}:${traitValue} | price=0 (no floor) | fetchTime=${fetchTime}ms`);
    return 0;

  } catch (error) {
    errorLog('TRAIT FLOOR', `Error fetching floor price for ${cacheKey} - collection=${collectionSlug} traitType=${traitType}`, error as Error);
    return 0;
  }
}

/**
 * Clear trait floor cache for a collection
 */
export function clearTraitFloorCache(collectionSlug?: string): void {
  // Always clear entire cache (LRU doesn't expose keys iterator for selective clearing)
  traitFloorLRU.clear();

  if (collectionSlug) {
    debugLog('Performance', `[OS-TraitFloor] Cleared cache for collection ${collectionSlug}`);
  } else {
    debugLog('Performance', `[OS-TraitFloor] Cleared entire cache`);
  }
}

/**
 * Get trait floor cache stats
 */
export function getTraitFloorCacheStats(): {
  size: number;
} {
  return {
    size: traitFloorLRU.size
  };
}

// Note: LRU cache handles TTL and cleanup automatically, no manual cleanup needed