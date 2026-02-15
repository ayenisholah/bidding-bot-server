import { LRUCache } from 'lru-cache';

/**
 * Atomically add entry to LRU cache if key doesn't exist or is expired
 *
 * @param cache - LRUCache instance
 * @param key - Cache key
 * @param value - Cache value
 * @returns true if entry was added, false if key already exists and is valid
 *
 * @example
 * if (!addIfNotExists(errorCache, errorKey, true)) {
 *   return; // Already logged this error recently
 * }
 */
export function addIfNotExists<K extends {}, V extends {}>(
  cache: LRUCache<K, V>,
  key: K,
  value: V
): boolean {
  if (cache.has(key)) {
    return false; // Key exists and not expired
  }
  cache.set(key, value);
  return true;
}

/**
 * Get cache statistics
 *
 * @param cache - LRUCache instance
 * @returns Object with cache statistics
 */
export function getCacheStats<K extends {}, V extends {}>(cache: LRUCache<K, V>): {
  size: number;
  maxSize: number;
  ttl: number | undefined;
} {
  return {
    size: cache.size,
    maxSize: cache.max,
    ttl: cache.ttl
  };
}
