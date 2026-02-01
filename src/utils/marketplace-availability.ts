import { LRUCache } from 'lru-cache';
import { debugLog } from './debug-file-logger';

type Marketplace = 'blur' | 'magiceden' | 'opensea';

const UNAVAILABLE_TTL = 3600 * 1000; // 1 hour in milliseconds
const loggedUnavailable = new Set<string>();

// Marketplace Unavailability LRU Cache
// Tracks collections that returned 400/404 from marketplace APIs
// Prevents repeated API calls to known-unavailable collections
export const marketplaceUnavailableLRU = new LRUCache<string, boolean>({
  max: 1000,  // 3 marketplaces × ~300 collections = 900 entries (1000 for headroom)
  ttl: UNAVAILABLE_TTL,  // 1 hour
  allowStale: false,
  updateAgeOnGet: false,
  updateAgeOnHas: false
});

/**
 * Check if a collection is known to be unavailable on a marketplace
 */
export function isCollectionUnavailable(
  marketplace: Marketplace,
  identifier: string // slug for blur/opensea, contractAddress for magiceden
): boolean {
  const cacheKey = `${marketplace}:${identifier.toLowerCase()}`;
  return marketplaceUnavailableLRU.has(cacheKey);
}

/**
 * Mark a collection as unavailable on a marketplace (400/404 received)
 */
export function markCollectionUnavailable(
  marketplace: Marketplace,
  identifier: string,
  statusCode: number
): void {
  const cacheKey = `${marketplace}:${identifier.toLowerCase()}`;
  marketplaceUnavailableLRU.set(cacheKey, true);

  if (!loggedUnavailable.has(cacheKey)) {
    loggedUnavailable.add(cacheKey);
    debugLog('Marketplace', `Collection not available on ${marketplace} | identifier=${identifier} | status=${statusCode} | cached for 1 hour`);
  }
}

/**
 * Clear unavailability cache (e.g., when task is edited or restarted)
 */
export function clearUnavailabilityCache(
  marketplace: Marketplace,
  identifier: string
): void {
  const cacheKey = `${marketplace}:${identifier.toLowerCase()}`;
  marketplaceUnavailableLRU.delete(cacheKey);
  loggedUnavailable.delete(cacheKey);
}
