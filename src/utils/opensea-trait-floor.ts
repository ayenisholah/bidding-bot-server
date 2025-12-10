import { getTraitFloorPrice } from '../functions/tokenMetadata';
import { errorLog } from './debug-file-logger';

// Simple in-memory cache for trait floor prices
interface TraitFloorCacheEntry {
  price: number;
  timestamp: number;
}

const traitFloorCache = new Map<string, TraitFloorCacheEntry>();
const TRAIT_FLOOR_CACHE_TTL = 5 * 60 * 1000; // 5 minutes

// Color constants
const GREEN = '\x1b[32m';
const YELLOW = '\x1b[33m';
const BLUE = '\x1b[34m';
const RESET = '\x1b[0m';

/**
 * Get cache key for trait floor
 */
function getTraitFloorKey(collectionSlug: string, traitType: string, traitValue: any): string {
  return `${collectionSlug}:${traitType}:${JSON.stringify(traitValue)}`;
}

/**
 * Get OpenSea floor price for a specific trait with caching
 */
export async function getOpenseaTraitFloor(
  collectionSlug: string,
  traitType: string,
  traitValue: any
): Promise<number> {
  const cacheKey = getTraitFloorKey(collectionSlug, traitType, traitValue);
  const now = Date.now();

  // Check cache
  const cached = traitFloorCache.get(cacheKey);
  if (cached && (now - cached.timestamp) < TRAIT_FLOOR_CACHE_TTL) {
    // console.log(BLUE + `[TRAIT FLOOR] Cache hit for ${cacheKey}: ${cached.price} ETH` + RESET);
    return cached.price;
  }

  // console.log(YELLOW + `[TRAIT FLOOR] Cache miss, fetching floor for ${cacheKey}` + RESET);

  try {
    // Fetch from API
    const floorPrice = await getTraitFloorPrice(collectionSlug, traitType, traitValue);
    
    if (floorPrice !== null) {
      // Cache the result
      traitFloorCache.set(cacheKey, {
        price: floorPrice,
        timestamp: now
      });

      // console.log(GREEN + `[TRAIT FLOOR] Fetched and cached floor price: ${floorPrice} ETH` + RESET);

      return floorPrice;
    }

    // If no floor price found, return 0
    // console.log(YELLOW + `[TRAIT FLOOR] No floor price found for trait, returning 0` + RESET);
    return 0;

  } catch (error) {
    errorLog('TRAIT FLOOR', `Error fetching floor price for ${cacheKey} - collection=${collectionSlug} traitType=${traitType}`, error as Error);

    // Return 0 on error
    return 0;
  }
}

/**
 * Clear trait floor cache for a collection
 */
export function clearTraitFloorCache(collectionSlug?: string): void {
  if (collectionSlug) {
    // Clear specific collection
    const keysToDelete: string[] = [];
    for (const key of traitFloorCache.keys()) {
      if (key.startsWith(`${collectionSlug}:`)) {
        keysToDelete.push(key);
      }
    }
    
    keysToDelete.forEach(key => traitFloorCache.delete(key));
    // console.log(YELLOW + `[TRAIT FLOOR] Cleared ${keysToDelete.length} entries for collection ${collectionSlug}` + RESET);
  } else {
    // Clear all
    const size = traitFloorCache.size;
    traitFloorCache.clear();
    // console.log(YELLOW + `[TRAIT FLOOR] Cleared all ${size} cache entries` + RESET);
  }
}

/**
 * Get trait floor cache stats
 */
export function getTraitFloorCacheStats(): {
  size: number;
  collections: string[];
  oldestEntry: number | null;
} {
  const collections = new Set<string>();
  let oldestTimestamp: number | null = null;

  for (const [key, entry] of traitFloorCache.entries()) {
    const collectionSlug = key.split(':')[0];
    collections.add(collectionSlug);
    
    if (oldestTimestamp === null || entry.timestamp < oldestTimestamp) {
      oldestTimestamp = entry.timestamp;
    }
  }

  return {
    size: traitFloorCache.size,
    collections: Array.from(collections),
    oldestEntry: oldestTimestamp
  };
}

// Clean up expired entries periodically
setInterval(() => {
  const now = Date.now();
  let removed = 0;
  
  for (const [key, entry] of traitFloorCache.entries()) {
    if (now - entry.timestamp > TRAIT_FLOOR_CACHE_TTL) {
      traitFloorCache.delete(key);
      removed++;
    }
  }
  
  if (removed > 0) {
    // console.log(YELLOW + `[TRAIT FLOOR] Cleaned up ${removed} expired cache entries` + RESET);
  }
}, 60 * 1000); // Run every minute