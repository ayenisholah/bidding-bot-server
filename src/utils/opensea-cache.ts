import redisClient from './redis';
import { logger, DatabaseOperation } from './logger';

// Get Redis connection
const getRedis = () => {
  try {
    return redisClient.getClient();
  } catch (error) {
    logger.logDatabaseConnectionError('Failed to get Redis client for OpenSea cache', {
      operation: 'opensea_cache',
      error: error as Error,
      errorMessage: error instanceof Error ? error.message : String(error)
    });
    return null;
  }
};

/**
 * Generic caching helper for OpenSea operations
 * @param cacheKey Redis key with {hashslot} pattern
 * @param ttl Time to live in seconds
 * @param fetchFn Function to fetch data if not cached
 * @param parseNumber Whether to parse the result as a number
 * @returns Cached or fetched data
 */
export async function getCachedOrFetch<T>(
  cacheKey: string,
  ttl: number,
  fetchFn: () => Promise<T>,
  parseNumber: boolean = false
): Promise<T> {
  const redis = getRedis();

  if (redis && redisClient.isReady()) {
    try {
      const cached = await redis.get(cacheKey);
      if (cached) {
        if (parseNumber) {
          return parseFloat(cached) as unknown as T;
        }
        return JSON.parse(cached);
      }
    } catch (error) {
      // Log but continue without cache
      await logger.logDatabaseError('Redis get error for OpenSea cache', {
        databaseOperation: DatabaseOperation.READ,
        collection_name: 'opensea_cache',
        cacheKey,
        error: error as Error,
        errorMessage: error instanceof Error ? error.message : String(error)
      });
    }
  }

  // Fetch fresh data
  const result = await fetchFn();

  // Try to cache the result
  if (redis && redisClient.isReady() && result !== null && result !== undefined) {
    try {
      const value = parseNumber ? String(result) : JSON.stringify(result);
      await redis.setex(cacheKey, ttl, value);
    } catch (error) {
      // console.log(error);
    }
  }

  return result;
}

// Cache TTL constants
export const CACHE_TTL = {
  FLOOR_PRICE: 60,        // 1 minute for floor prices
  LISTINGS: 60,           // 1 minute for listings
  TRAIT_FLOOR: 60,        // 1 minute for trait floors
  BALANCE: 60,            // 1 minute for balances
  COLLECTION: 3600,       // 1 hour for collection details
  COLLECTION_ITEMS: 3600  // 1 hour for collection items
};

// Cache key generators
export const CACHE_KEYS = {
  floorPrice: (slug: string) => `opensea:floor:${slug}`,
  listings: (slug: string, limit: number) => `opensea:listings:${slug}:${limit}`,
  traitFloor: (slug: string, traitType: string, values: string[]) =>
    `opensea:trait-floor:${slug}:${traitType}:${values.join(',')}`,
  balance: (walletAddress: string, tokenAddress: string) =>
    `balance:${walletAddress}:${tokenAddress}`,
  collection: (slug: string) => `opensea:collection:${slug}`,
  collectionItems: (variablesHash: string) => `opensea:items:${variablesHash}`
};