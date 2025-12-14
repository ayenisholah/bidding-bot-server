import redisClient from './redis';
import { debugLog } from './debug-file-logger';

type Marketplace = 'blur' | 'magiceden' | 'opensea';

const UNAVAILABLE_TTL = 3600; // 1 hour in seconds
const loggedUnavailable = new Set<string>(); // Track logged messages to avoid spam

/**
 * Check if a collection is known to be unavailable on a marketplace
 */
export async function isCollectionUnavailable(
  marketplace: Marketplace,
  identifier: string // slug for blur/opensea, contractAddress for magiceden
): Promise<boolean> {
  const cacheKey = `marketplace:unavailable:${marketplace}:${identifier.toLowerCase()}`;
  try {
    const redis = redisClient.getClient();
    if (redis) {
      const cached = await redis.get(cacheKey);
      return cached === 'true';
    }
  } catch {
    // Redis error - assume available to allow API call
  }
  return false;
}

/**
 * Mark a collection as unavailable on a marketplace (400/404 received)
 */
export async function markCollectionUnavailable(
  marketplace: Marketplace,
  identifier: string,
  statusCode: number
): Promise<void> {
  const cacheKey = `marketplace:unavailable:${marketplace}:${identifier.toLowerCase()}`;
  const logKey = `${marketplace}:${identifier.toLowerCase()}`;

  try {
    const redis = redisClient.getClient();
    if (redis) {
      await redis.set(cacheKey, 'true', 'EX', UNAVAILABLE_TTL);
    }
  } catch {
    // Redis error - continue without caching
  }

  // Log once per collection/marketplace combo (avoid spam)
  if (!loggedUnavailable.has(logKey)) {
    loggedUnavailable.add(logKey);
    debugLog('Marketplace', `Collection not available on ${marketplace} | identifier=${identifier} | status=${statusCode} | cached for 1 hour`);
  }
}

/**
 * Clear unavailability cache (e.g., when task is edited or restarted)
 */
export async function clearUnavailabilityCache(
  marketplace: Marketplace,
  identifier: string
): Promise<void> {
  const cacheKey = `marketplace:unavailable:${marketplace}:${identifier.toLowerCase()}`;
  const logKey = `${marketplace}:${identifier.toLowerCase()}`;

  try {
    const redis = redisClient.getClient();
    if (redis) {
      await redis.del(cacheKey);
    }
    loggedUnavailable.delete(logKey);
  } catch {
    // Ignore errors
  }
}
