import { LRUCache } from 'lru-cache';
// Collection Details LRU (24 hours TTL - long-lived, rarely changes)
// Standardized to 1000 max (10x typical usage, good safety buffer)
// Memory impact: ~10MB max (10KB per collection × 1000 collections)
export const collectionDetailsLRU = new LRUCache<string, any>({
  max: 1000,
  ttl: 24 * 60 * 60 * 1000,  // 24 hours
  allowStale: true,
  updateAgeOnGet: false
});

// OpenSea Floor Price LRU (5 min TTL per user requirement)
// Standardized to 1000 max (10x typical usage, good safety buffer)
// Renamed from floorPriceLRU to openseaFloorLRU for consistency with other marketplace caches
export const openseaFloorLRU = new LRUCache<string, number>({
  max: 1000,
  ttl: 5 * 60 * 1000,  // 5 minutes
  allowStale: true,
  updateAgeOnGet: false
});

// Balance LRU (5 min TTL per user requirement)
// Standardized to 1000 max (sufficient for large-scale operations)
export const balanceLRU = new LRUCache<string, number>({
  max: 1000,
  ttl: 60 * 1000,  // 60 seconds (balance changes more frequently)
  allowStale: false,
  updateAgeOnGet: false
});

// Seaport Counter LRU (30 second TTL - optimization for bid placement)
// Counter only increments when bid is placed, safe to cache short-term
// Invalidated immediately after successful bid placement
export const seaportCounterLRU = new LRUCache<string, bigint>({
  max: 100,
  ttl: 30 * 1000,  // 30 seconds
  allowStale: false,
  updateAgeOnGet: false
});

// Trait Floor LRU (5 min TTL)
// Right-sized to 10K max (100 collections × 5 trait types × 20 values)
export const traitFloorLRU = new LRUCache<string, number>({
  max: 10000, // Reduced from 20K to 10K
  ttl: 5 * 60 * 1000,
  allowStale: true,
  updateAgeOnGet: false
});

// ============================================
// NEW: Redis to LRU Migration Caches
// ============================================

// Blur Floor Price LRU (5 min TTL - matches Redis)
export const blurFloorLRU = new LRUCache<string, number>({
  max: 1000,
  ttl: 5 * 60 * 1000, // 5 minutes
  allowStale: true,
  updateAgeOnGet: false,
  updateAgeOnHas: false
});

// MagicEden Floor Price LRU (1 min TTL - matches Redis)
export const magicEdenFloorLRU = new LRUCache<string, number>({
  max: 1000,
  ttl: 60 * 1000, // 1 minute
  allowStale: true,
  updateAgeOnGet: false,
  updateAgeOnHas: false
});

// Collection Items LRU (1 hour TTL - matches Redis)
// Stores token IDs lists for collections
export const collectionItemsLRU = new LRUCache<string, string[]>({
  max: 1000,
  ttl: 60 * 60 * 1000, // 1 hour
  allowStale: true,
  updateAgeOnGet: false,
  updateAgeOnHas: false
});

// Collection Traits Metadata LRU (24 hour TTL - user requirement)
// USER REQUIREMENT: max 1M entries, 24h TTL
export const collectionTraitsLRU = new LRUCache<string, any>({
  max: 1_000_000, // 1 million entries (user requirement)
  ttl: 24 * 60 * 60 * 1000, // 24 hours (user requirement)
  allowStale: true,
  updateAgeOnGet: false,
  updateAgeOnHas: false
});

// Trait Floors Detail LRU (1 min TTL - matches Redis)
// Stores detailed trait floor prices by trait combinations
export const traitFloorsDetailLRU = new LRUCache<string, any>({
  max: 10000, // Increased from 5K to 10K for better high-traffic capacity
  ttl: 60 * 1000, // 1 minute
  allowStale: true,
  updateAgeOnGet: false,
  updateAgeOnHas: false
});

// Collection Events LRU (10 sec TTL - matches Redis)
// Stores collection events (sales, listings, offers)
export const collectionEventsLRU = new LRUCache<string, any>({
  max: 1000,
  ttl: 10 * 1000, // 10 seconds
  allowStale: true,
  updateAgeOnGet: false,
  updateAgeOnHas: false
});

// ============================================
// Address Balances LRU Cache (Phase 3 Migration)
// ============================================
// Migrated from Redis pattern: {address_balances:{addr}}
// Stores multi-currency balance data from OpenSea API
// Note: Uses AddressBalancesResponse type from index.ts (exported from there)

/**
 * Address Balances Cache
 *
 * Stores multi-currency balance data for wallet addresses
 * - Key: lowercase wallet address
 * - Value: AddressBalancesResponse object (Balance[] with full Currency details)
 * - TTL: 30 seconds (matches Redis behavior)
 * - Max: 1000 entries (~2MB memory)
 * - Usage: UI display only (not critical path)
 */
export const addressBalancesLRU = new LRUCache<string, any>({
  max: 1000,
  ttl: 30 * 1000, // 30 seconds
  allowStale: false,
  updateAgeOnGet: false
});

// ============================================
// Best Offer LRU Caches (for client UI only)
// ============================================
// Server bidding logic always fetches fresh offers - these serve client UI only
// 5 minute TTL matches floor price caches for consistency

// OpenSea Best Offer LRU (5min TTL - for client consumption only)
export const openseaBestOfferLRU = new LRUCache<string, any>({
  max: 1000,
  ttl: 5 * 60 * 1000, // 5 minutes
  allowStale: true,
  updateAgeOnGet: false,
  updateAgeOnHas: false
});

// Blur Best Offer LRU (5min TTL - for client consumption only)
export const blurBestOfferLRU = new LRUCache<string, any>({
  max: 1000,
  ttl: 5 * 60 * 1000, // 5 minutes
  allowStale: true,
  updateAgeOnGet: false,
  updateAgeOnHas: false
});

// MagicEden Best Offer LRU (5min TTL - for client consumption only)
export const magicedenBestOfferLRU = new LRUCache<string, any>({
  max: 1000,
  ttl: 5 * 60 * 1000, // 5 minutes
  allowStale: true,
  updateAgeOnGet: false,
  updateAgeOnHas: false
});

// ============================================
// OpenSea Listings Cache (Phase 4 Migration)
// ============================================
// Replaces opensea-cache.ts getCachedOrFetch for listings
// Stores array of token IDs (strings) from best listings API

/**
 * OpenSea Listings Cache
 *
 * Stores best listings for collections (token ID arrays)
 * - Key: `${collectionSlug}:${limit}`
 * - Value: string[] (array of token IDs)
 * - TTL: 60 seconds (matches opensea-cache.ts CACHE_TTL.LISTINGS)
 * - Max: 1000 entries (~1MB memory)
 * - Usage: fetchOpenseaListings() for finding cheapest NFTs
 */
export const openseaListingsLRU = new LRUCache<string, string[]>({
  max: 1000,
  ttl: 60 * 1000, // 60 seconds
  allowStale: false,
  updateAgeOnGet: false
});

// Cache statistics
interface CacheStats {
  hits: number;
  misses: number;
}

const stats = {
  collectionDetails: { hits: 0, misses: 0 } as CacheStats,
  floor: { hits: 0, misses: 0 } as CacheStats,
  balance: { hits: 0, misses: 0 } as CacheStats,
  seaportCounter: { hits: 0, misses: 0 } as CacheStats,
  traitFloor: { hits: 0, misses: 0 } as CacheStats
};

const startTime = Date.now();

export function getCacheStats() {
  const uptime = Math.floor((Date.now() - startTime) / 1000 / 60); // minutes
  return {
    collectionDetails: {
      ...stats.collectionDetails,
      hitRate: `${((stats.collectionDetails.hits / (stats.collectionDetails.hits + stats.collectionDetails.misses)) * 100 || 0).toFixed(1)}%`,
      size: collectionDetailsLRU.size
    },
    floor: {
      ...stats.floor,
      hitRate: `${((stats.floor.hits / (stats.floor.hits + stats.floor.misses)) * 100 || 0).toFixed(1)}%`,
      size: openseaFloorLRU.size
    },
    balance: {
      ...stats.balance,
      hitRate: `${((stats.balance.hits / (stats.balance.hits + stats.balance.misses)) * 100 || 0).toFixed(1)}%`,
      size: balanceLRU.size
    },
    seaportCounter: {
      ...stats.seaportCounter,
      hitRate: `${((stats.seaportCounter.hits / (stats.seaportCounter.hits + stats.seaportCounter.misses)) * 100 || 0).toFixed(1)}%`,
      size: seaportCounterLRU.size
    },
    traitFloor: {
      ...stats.traitFloor,
      hitRate: `${((stats.traitFloor.hits / (stats.traitFloor.hits + stats.traitFloor.misses)) * 100 || 0).toFixed(1)}%`,
      size: traitFloorLRU.size
    },
    uptime: `${uptime}min`
  };
}

export function resetCacheStats() {
  stats.collectionDetails.hits = stats.collectionDetails.misses = 0;
  stats.floor.hits = stats.floor.misses = 0;
  stats.balance.hits = stats.balance.misses = 0;
  stats.seaportCounter.hits = stats.seaportCounter.misses = 0;
  stats.traitFloor.hits = stats.traitFloor.misses = 0;
}

// Log stats every 5 minutes (aggregated, not per-access)
setInterval(() => {
  const cacheStats = getCacheStats();
  const { infoLog } = require('../../utils/debug-file-logger');
  infoLog('Performance',
    `[OS-Cache 5min Summary] ` +
    `collDetails: ${cacheStats.collectionDetails.hits} hits, ${cacheStats.collectionDetails.misses} misses (${cacheStats.collectionDetails.hitRate}, ${cacheStats.collectionDetails.size} cached) | ` +
    `floor: ${cacheStats.floor.hits} hits, ${cacheStats.floor.misses} misses (${cacheStats.floor.hitRate}, ${cacheStats.floor.size} cached) | ` +
    `balance: ${cacheStats.balance.hits} hits, ${cacheStats.balance.misses} misses (${cacheStats.balance.hitRate}, ${cacheStats.balance.size} cached) | ` +
    `seaportCounter: ${cacheStats.seaportCounter.hits} hits, ${cacheStats.seaportCounter.misses} misses (${cacheStats.seaportCounter.hitRate}, ${cacheStats.seaportCounter.size} cached) | ` +
    `traitFloor: ${cacheStats.traitFloor.hits} hits, ${cacheStats.traitFloor.misses} misses (${cacheStats.traitFloor.hitRate}, ${cacheStats.traitFloor.size} cached) | ` +
    `uptime: ${cacheStats.uptime}`
  );
}, 5 * 60 * 1000);

// Tracking functions
export const trackCollectionDetailsHit = () => stats.collectionDetails.hits++;
export const trackCollectionDetailsMiss = () => stats.collectionDetails.misses++;
export const trackFloorHit = () => stats.floor.hits++;
export const trackFloorMiss = () => stats.floor.misses++;
export const trackBalanceHit = () => stats.balance.hits++;
export const trackBalanceMiss = () => stats.balance.misses++;
export const trackSeaportCounterHit = () => stats.seaportCounter.hits++;
export const trackSeaportCounterMiss = () => stats.seaportCounter.misses++;
export const trackTraitFloorHit = () => stats.traitFloor.hits++;
export const trackTraitFloorMiss = () => stats.traitFloor.misses++;
