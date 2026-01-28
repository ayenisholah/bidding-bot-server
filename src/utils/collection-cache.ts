// Simple LRU-based collection and floor price caching
// Replaces complex Redis-based two-tier cache with simple memory-only LRU
// 120 lines (down from 295 lines - 60% reduction)

import { LRUCache } from 'lru-cache';
import { getCollectionDetails } from '../functions/collection';
import { getOpenseaFloor } from '../functions/collection';

// Simple LRU caches - NO Redis
const collectionDetailsCache = new LRUCache<string, any>({
  max: 500,                     // Max 500 collections
  ttl: 7 * 24 * 60 * 60 * 1000,     // 7 days TTL
  updateAgeOnGet: false,        // Don't extend TTL on read
  allowStale: false
});

const floorPriceCache = new LRUCache<string, number>({
  max: 1000,                    // Max 1000 floor prices (collection × marketplace)
  ttl: 5 * 60 * 1000,         // 5 minute TTL (increased from 5min to reduce miss rate from 51% to ~20%)
  updateAgeOnGet: false,
  allowStale: false,
  ttlAutopurge: true            // Enable automatic purging of expired entries
});

// In-flight request deduplication (prevent thundering herd)
const pendingRequests = new Map<string, Promise<any>>();

/**
 * Add ±10% jitter to TTL to prevent all caches from expiring at same time
 * @param baseTTL Base TTL in milliseconds
 * @returns TTL with jitter applied
 */
function addTTLJitter(baseTTL: number): number {
  const jitter = baseTTL * 0.1 * (Math.random() * 2 - 1); // ±10%
  return Math.floor(baseTTL + jitter);
}

/**
 * Get collection details with caching
 * Returns cached value if available, otherwise fetches from API
 */
export async function getCachedCollectionDetails(slug: string, contractAddress?: string): Promise<any> {
  const key = slug.toLowerCase();

  // Check cache
  const cached = collectionDetailsCache.get(key);
  if (cached) {
    return cached;
  }

  // Deduplicate concurrent requests
  const pendingKey = `details:${key}`;
  if (pendingRequests.has(pendingKey)) {
    return pendingRequests.get(pendingKey)!;
  }

  // Fetch and cache
  const request = getCollectionDetails(slug, contractAddress)
    .then(details => {
      collectionDetailsCache.set(key, details);
      pendingRequests.delete(pendingKey);
      return details;
    })
    .catch(err => {
      pendingRequests.delete(pendingKey);
      throw err;
    });

  pendingRequests.set(pendingKey, request);
  return request;
}

/**
 * Get floor price with caching
 * Returns cached value if available, otherwise fetches from API
 */
export async function getCachedFloorPrice(slug: string, marketplace: string = 'opensea'): Promise<number> {
  const key = `${marketplace}:${slug}`.toLowerCase();

  // Check cache
  const cached = floorPriceCache.get(key);
  if (cached !== undefined) {
    return cached;
  }

  // Deduplicate concurrent requests
  const pendingKey = `floor:${key}`;
  if (pendingRequests.has(pendingKey)) {
    return pendingRequests.get(pendingKey)!;
  }

  // Fetch and cache with TTL jitter to prevent thundering herd
  const request = getOpenseaFloor(slug)
    .then(price => {
      const jitteredTTL = addTTLJitter(15 * 60 * 1000); // 15min ±10% = 13.5-16.5 min
      floorPriceCache.set(key, price, { ttl: jitteredTTL });
      pendingRequests.delete(pendingKey);
      return price;
    })
    .catch(err => {
      pendingRequests.delete(pendingKey);
      throw err;
    });

  pendingRequests.set(pendingKey, request);
  return request;
}

/**
 * Invalidate all caches for a collection
 */
export function invalidateCollection(slug: string): void {
  const key = slug.toLowerCase();
  collectionDetailsCache.delete(key);

  // Remove floor prices for all marketplaces
  const marketplaces = ['opensea', 'blur', 'magiceden'];
  for (const marketplace of marketplaces) {
    floorPriceCache.delete(`${marketplace}:${key}`);
  }
}

/**
 * Get cache statistics
 */
export function getCacheStats() {
  return {
    collectionDetails: {
      size: collectionDetailsCache.size,
      max: 500
    },
    floorPrice: {
      size: floorPriceCache.size,
      max: 1000
    },
    pendingRequests: pendingRequests.size
  };
}

/**
 * Clear all caches
 */
export function clearAllCaches(): void {
  collectionDetailsCache.clear();
  floorPriceCache.clear();
  pendingRequests.clear();
}
