import stringify from 'json-stable-stringify';
import crypto from 'crypto';
import { AxiosResponse } from 'axios';
import { axiosInstance, limiter } from "../init"; // Import Redis client
import { DistributedLockManager } from "../utils/lock";
import { CircuitBreaker } from "../utils/circuit-breaker";
import { API_KEY } from "..";
import redisClient from '../utils/redis';
import { CollectionItemsListQuery } from "../queries";
import { BASE_URL } from "../constants";

import { ICollectionDetails, ICollectionFee } from "../types/collection.types";
import { setOpenseaMarketplaceFee } from "../utils/marketplace-fee-cache";
import { errorLog, warnLog, debugLog } from "../utils/debug-file-logger";
import { apiDedup } from "../utils/api-dedup";

const collectionCache: { [key: string]: ICollectionDetails } = {};
const MAX_CACHE_ITEMS = 1000;

const OPENSEA_FEE_ADDRESS = "0x0000a26b00c1f0df003000390027140000faa719";
const OPENSEA_FEE_ADDRESS_LOWER = OPENSEA_FEE_ADDRESS.toLowerCase();

function computeOpenseaMarketplaceFeeBps(fees?: ICollectionFee[]): number | undefined {
  if (!Array.isArray(fees)) {
    return undefined;
  }

  for (const fee of fees) {
    if (!fee?.recipient) {
      continue;
    }

    if (fee.recipient.toLowerCase() === OPENSEA_FEE_ADDRESS_LOWER) {
      if (typeof fee.fee !== 'number' || Number.isNaN(fee.fee)) {
        return undefined;
      }

      return Math.round(fee.fee * 100);
    }
  }

  return undefined;
}

function ensureOpenseaMarketplaceFee(details?: ICollectionDetails) {
  if (!details) {
    return;
  }

  let basisPoints: number | undefined;

  if (typeof details.openseaMarketplaceFeeBps === 'number' && Number.isFinite(details.openseaMarketplaceFeeBps)) {
    basisPoints = Math.round(details.openseaMarketplaceFeeBps);
  }

  if (basisPoints === undefined) {
    basisPoints = computeOpenseaMarketplaceFeeBps(details.fees);
    if (basisPoints !== undefined) {
      details.openseaMarketplaceFeeBps = basisPoints;
    }
  }

  if (basisPoints !== undefined) {
    setOpenseaMarketplaceFee(BigInt(basisPoints));
  }
}

// Helper function to get Redis connection
const getRedis = () => redisClient.getClient();

const lockManager = new DistributedLockManager({
  lockPrefix: 'collection:fetch:',
  defaultTTLSeconds: 300 // Reduced from 10 hours to 5 minutes
});

// Circuit breaker for OpenSea API calls to prevent retry storms
// Updated settings to be less aggressive after 2025-12-02 outage analysis
const openseaCircuitBreaker = new CircuitBreaker({
  failureThreshold: 10,    // Open circuit after 10 consecutive failures (increased from 5)
  resetTimeout: 120000,    // Wait 120s before attempting recovery (increased from 60s)
  halfOpenRequests: 5      // Test recovery with 5 requests (increased from 3)
});

// Helper function to add timeout to promises
async function withTimeout<T>(promise: Promise<T>, timeoutMs: number, operation: string): Promise<T> {
  const timeoutPromise = new Promise<never>((_, reject) => {
    setTimeout(() => reject(new Error(`${operation} timed out after ${timeoutMs}ms`)), timeoutMs);
  });
  return Promise.race([promise, timeoutPromise]);
}

export async function getCollectionDetails(slug: string, contractAddress?: string) {
  if (collectionCache[slug]) {
    ensureOpenseaMarketplaceFee(collectionCache[slug]);
    return collectionCache[slug];
  }

  // Check Redis cache
  const cacheKey = `opensea:collection:${slug}`;
  const redis = getRedis();

  if (redis && redisClient.isReady()) {
    try {
      const cached = await redis.get(cacheKey);
      if (cached) {
        const details = JSON.parse(cached);
        collectionCache[slug] = details; // Update memory cache
        ensureOpenseaMarketplaceFee(collectionCache[slug]);
        return details;
      }
    } catch {
      warnLog('Cache', `[collection.ts] Error reading collection details from Redis cache | slug=${slug} | key=${cacheKey}`);
    }
  }

  const lockKey = `{${slug}}:collection`;

  try {
    const result = await withTimeout(
      lockManager.withLock(lockKey, async () => {
        if (collectionCache[slug]) {
          return collectionCache[slug];
        }

        const { data: collection } = await withTimeout(
          apiDedup(
            `opensea-collection-details-${slug.toLowerCase()}`,
            () => limiter.schedule(() => axiosInstance.get(
              `${BASE_URL}/opensea/api/v2/collections/${slug}`,
              {
                headers: {
                  'x-api-key': API_KEY
                }
              }
            )),
            { category: 'Collection' }
          ),
          20000, // 20 second timeout for API call
          `OpenSea API call for ${slug}`
        );

        let creator_fees: { [key: string]: number } = {};
        let enforceCreatorFee = false;

        // DEBUG: Log raw fees from OpenSea API
        debugLog('FeeDebug', `[${slug}] Raw API fees | fees=${JSON.stringify(collection?.fees)}`);

        if (collection?.fees && Array.isArray(collection.fees)) {
          // Process creator fees (exclude OpenSea platform fee and optional fees)
          for (const fee of collection.fees) {
            const isOpenSeaFee = fee.recipient.toLowerCase() === OPENSEA_FEE_ADDRESS_LOWER;

            // DEBUG: Log each fee being processed
            debugLog('FeeDebug', `[${slug}] Processing fee | recipient=${fee.recipient} | fee=${fee.fee} | required=${fee.required} | isOpenSea=${isOpenSeaFee}`);

            // Skip OpenSea platform fee
            if (isOpenSeaFee) {
              debugLog('FeeDebug', `[${slug}] SKIP: OpenSea platform fee`);
              continue;
            }

            // Skip optional creator fees (fulfiller can opt-in at fill time)
            if (!fee.required) {
              debugLog('FeeDebug', `[${slug}] SKIP: Optional fee (required=false)`);
              continue;
            }

            creator_fees[fee.recipient] = fee.fee * 100;
            enforceCreatorFee = true;
            debugLog('FeeDebug', `[${slug}] ADDED: Creator fee | recipient=${fee.recipient} | bps=${fee.fee * 100}`);
          }
        }

        // If no creator fees found, use default
        if (Object.keys(creator_fees).length === 0) {
          creator_fees = { null: 0 };
          debugLog('FeeDebug', `[${slug}] No creator fees found, using default { null: 0 }`);
        }

        // DEBUG: Log final creator_fees
        debugLog('FeeDebug', `[${slug}] Final creator_fees | result=${JSON.stringify(creator_fees)} | enforceCreatorFee=${enforceCreatorFee}`);

        if (Object.keys(collectionCache).length >= MAX_CACHE_ITEMS) {
          delete collectionCache[Object.keys(collectionCache)[0]];
        }

        const result = {
          ...collection,
          creator_fees: creator_fees,
          enforceCreatorFee: enforceCreatorFee
        };

        ensureOpenseaMarketplaceFee(result);
        collectionCache[slug] = result;

        // Cache in Redis with 1-hour TTL
        if (redis && redisClient.isReady()) {
          try {
            await redis.setex(cacheKey, 3600, JSON.stringify(result));
          } catch {
            warnLog('Cache', `[collection.ts] Error caching collection details to Redis | slug=${slug}`);
          }
        }
        return result;
      }),
      30000, // 30 second timeout for entire operation
      `Collection details fetch for ${slug}`
    );

    if (!result) {
      errorLog('Collection', `[collection.ts] Failed to acquire lock for collection details | slug=${slug} | contractAddress=${contractAddress}`);
      throw new Error('Failed to acquire lock for collection details');
    }
    // {
    //   getCollectionDetails: [
    //     {
    //       fee: 1,
    //       recipient: '0x0000a26b00c1f0df003000390027140000faa719',
    //       required: true
    //     },
    //     {
    //       fee: 1,
    //       recipient: '0xa858ddc0445d8131dac4d1de01f834ffcba52ef1',
    //       required: false
    //     }
    //   ]
    // }
    return result;
  } catch (error) {
    // Log the error
    errorLog('Collection', `[collection.ts] Failed to fetch collection details | slug=${slug} | contractAddress=${contractAddress}`, error);

    // Return a minimal collection object to prevent complete failure
    const fallbackResult: ICollectionDetails & { error: boolean; errorMessage: string } = {
      collection: slug,
      name: slug,
      description: '',
      image_url: '',
      banner_image_url: '',
      owner: '',
      safelist_status: 'not_requested',
      category: '',
      is_disabled: false,
      is_nsfw: false,
      trait_offers_enabled: false,
      collection_offers_enabled: true,
      opensea_url: `https://opensea.io/collection/${slug}`,
      project_url: '',
      wiki_url: '',
      discord_url: '',
      telegram_url: '',
      twitter_username: '',
      instagram_username: '',
      contracts: [],
      editors: [],
      fees: [],
      rarity: {
        calculated_at: '',
        max_rank: 0,
        total_supply: 0,
        strategy_id: '',
        strategy_version: ''
      },
      total_supply: 0,
      created_date: new Date().toISOString(),
      payment_tokens: [],
      creator_fees: {},
      enforceCreatorFee: false,
      error: true,
      errorMessage: error instanceof Error ? error.message : String(error)
    };

    // Cache the fallback for a short time to prevent hammering
    collectionCache[slug] = fallbackResult;
    setTimeout(() => delete collectionCache[slug], 60000); // Clear after 1 minute

    return fallbackResult;
  }
}

export async function getOpenseaFloor(collectionSlug: string) {
  const CACHE_TTL = 60; // 1 minute cache (updated from 30 seconds)
  const cacheKey = `opensea:floor:${collectionSlug}`;
  const lockKey = `floor:{opensea}:${collectionSlug}`;

  // Try to get from cache first
  try {
    const cachedFloor = getRedis() ? await getRedis().get(cacheKey) : null;
    if (cachedFloor) {
      return JSON.parse(cachedFloor);
    }
  } catch {
    warnLog('Cache', `[collection.ts] Error reading OpenSea floor price from cache | slug=${collectionSlug} | key=${cacheKey}`);
  }

  // If not in cache, try to acquire lock and fetch
  const result = await lockManager.withLock(lockKey, async () => {
    // Double check cache after acquiring lock
    const cachedFloor = getRedis() ? await getRedis().get(cacheKey) : null;
    if (cachedFloor) {
      return JSON.parse(cachedFloor);
    }

    // Retry configuration for floor price fetching
    const MAX_RETRIES = 3;
    const BASE_DELAY = 500; // 500ms, 1000ms, 2000ms exponential backoff

    const apiUrl = `${BASE_URL}/opensea/api/v2/listings/collection/${collectionSlug}/best?limit=1`;

    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      try {
        const { data } = await apiDedup(
          `opensea-floor-${collectionSlug}`,
          () => openseaCircuitBreaker.execute(() =>
            limiter.schedule({ priority: 0 }, () => axiosInstance.get<ListingsResponse>(
              apiUrl,
              {
                headers: { 'x-api-key': API_KEY }
              }
            ))
          ),
          { category: 'Collection' }
        );

        // Check if listings exist and has item
        const floor_price = Number(data?.listings[0]?.price?.current?.value) / 1e18 || 0;

        // If floor is 0 and we have retries left, retry (likely temporary API issue)
        if (floor_price === 0 && attempt < MAX_RETRIES) {
          warnLog('API', `[collection.ts] OpenSea floor price is 0, retrying (${attempt}/${MAX_RETRIES}) | slug=${collectionSlug}`);
          await new Promise(resolve => setTimeout(resolve, BASE_DELAY * Math.pow(2, attempt - 1)));
          continue;
        }

        // Only cache positive floor prices - don't cache 0 values
        if (floor_price > 0 && getRedis()) {
          await getRedis().set(cacheKey, JSON.stringify(floor_price), 'EX', CACHE_TTL);
        }

        return floor_price;

      } catch (error: any) {
        // Handle 404 errors specifically - collection might be delisted or renamed (no retry)
        if (error?.response?.status === 404) {
          warnLog('API', `[collection.ts] Collection not found on OpenSea | slug=${collectionSlug} | status=404`);

          // Cache the zero floor price for a short time to avoid repeated 404s
          if (getRedis()) {
            await getRedis().set(cacheKey, JSON.stringify(0), 'EX', 300); // Cache for 5 minutes
          }
          return 0; // Return 0 floor price for non-existent collections
        }

        // Retry on other errors if attempts remaining
        if (attempt < MAX_RETRIES) {
          // Better error parsing: show HTTP status or network error code
          const errorCode = error?.response?.status || error?.code || 'UNKNOWN';
          warnLog('API', `[collection.ts] OpenSea floor price fetch failed, retrying (${attempt}/${MAX_RETRIES}) | slug=${collectionSlug} | error=${errorCode}`);
          await new Promise(resolve => setTimeout(resolve, BASE_DELAY * Math.pow(2, attempt - 1)));
          continue;
        }

        // Final attempt failed - log and re-throw
        const errorCode = error?.response?.status || error?.code || 'UNKNOWN';
        errorLog('API', `[collection.ts] Failed to fetch OpenSea floor price after ${MAX_RETRIES} attempts | slug=${collectionSlug} | error=${errorCode}`, error);
        throw error;
      }
    }

    // Should not reach here, but return 0 as fallback
    return 0;
  });

  if (!result) {
    // If we couldn't acquire the lock, another request is fetching - wait with retries
    const maxRetries = 5;
    for (let i = 0; i < maxRetries; i++) {
      const waitTime = Math.min(100 * Math.pow(2, i), 2000); // Exponential backoff, max 2s
      await new Promise(resolve => setTimeout(resolve, waitTime));

      try {
        const cachedFloor = getRedis() ? await getRedis().get(cacheKey) : null;
        if (cachedFloor) {
          return JSON.parse(cachedFloor);
        }
      } catch {
        warnLog('Cache', `[collection.ts] Error reading from cache after lock failure | slug=${collectionSlug}`);
      }
    }

    // After all retries, still no data - log and throw error
    errorLog('Collection', `[collection.ts] Failed to fetch OpenSea floor price after retries | slug=${collectionSlug}`);
    throw new Error('Failed to fetch OpenSea floor price');
  }

  return result;
}

// Circuit breaker for Blur API calls to prevent repeated failures
interface BlurCircuitBreakerState {
  failureCount: number;
  lastFailure: number;
}

const blurCircuitBreaker = new Map<string, BlurCircuitBreakerState>();
const BLUR_CIRCUIT_OPEN_THRESHOLD = 5;
const BLUR_CIRCUIT_RESET_TIME = 300000; // 5 minutes

function shouldSkipBlurAPI(collectionSlug: string): boolean {
  const state = blurCircuitBreaker.get(collectionSlug);
  if (!state) return false;

  const now = Date.now();
  if (now - state.lastFailure > BLUR_CIRCUIT_RESET_TIME) {
    blurCircuitBreaker.delete(collectionSlug);
    return false;
  }

  if (state.failureCount >= BLUR_CIRCUIT_OPEN_THRESHOLD) {
    return true;
  }
  return false;
}

function recordBlurAPIFailure(collectionSlug: string, errorStatus: number) {
  // Only track 403 and 408 errors for circuit breaker
  if (errorStatus !== 403 && errorStatus !== 408) {
    return;
  }

  const state = blurCircuitBreaker.get(collectionSlug);
  if (!state) {
    blurCircuitBreaker.set(collectionSlug, { failureCount: 1, lastFailure: Date.now() });
    return;
  }

  state.failureCount++;
  state.lastFailure = Date.now();
}

export async function getBlurFloor(collectionSlug: string): Promise<number> {
  const CACHE_TTL = 60; // 1 minute cache (updated from 30 seconds)
  const cacheKey = `blur:floor:{${collectionSlug}}`;
  const lockKey = `floor:{blur}:${collectionSlug}`;

  // Try to get from cache first
  try {
    const cachedFloor = getRedis() ? await getRedis().get(cacheKey) : null;
    if (cachedFloor) {
      return JSON.parse(cachedFloor);
    }
  } catch {
    warnLog('Cache', `[collection.ts] Error reading Blur floor from cache | slug=${collectionSlug}`);
  }

  // Check circuit breaker before making API call
  if (shouldSkipBlurAPI(collectionSlug)) {
    const state = blurCircuitBreaker.get(collectionSlug);
    warnLog('Collection', `[BlurCircuitBreaker] ${collectionSlug} | OPEN - skipping API | failureCount=${state?.failureCount} | lastFailure=${new Date(state?.lastFailure || 0).toISOString()}`);
    return 0;
  }

  // If not in cache, try to acquire lock and fetch
  const result = await lockManager.withLock(lockKey, async () => {
    // Double check cache after acquiring lock
    const cachedFloor = getRedis() ? await getRedis().get(cacheKey) : null;
    if (cachedFloor) {
      return JSON.parse(cachedFloor);
    }

    // Retry configuration for floor price fetching
    const MAX_RETRIES = 3;
    const BASE_DELAY = 500; // 500ms, 1000ms, 2000ms exponential backoff

    // Import Blur functions dynamically to avoid circular dependency
    const { fetchBlurCollectionStats } = await import('../marketplace/blur/bid');

    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      try {
        // CRITICAL FIX: fetchBlurCollectionStats returns a NUMBER, not an object!
        const blurFloor = await fetchBlurCollectionStats(collectionSlug);

        // blurFloor is already a number (e.g., 0.0497)
        const floor_price = blurFloor ? Number(blurFloor) : 0;

        // If floor is 0 and we have retries left, retry (likely temporary API issue)
        if (floor_price === 0 && attempt < MAX_RETRIES) {
          warnLog('Collection', `[getBlurFloor] ${collectionSlug} | Floor is 0, retrying (${attempt}/${MAX_RETRIES})`);
          await new Promise(resolve => setTimeout(resolve, BASE_DELAY * Math.pow(2, attempt - 1)));
          continue;
        }

        // Only cache positive floor prices - don't cache 0 values
        if (floor_price > 0 && getRedis()) {
          await getRedis().set(cacheKey, JSON.stringify(floor_price), 'EX', CACHE_TTL);
        }

        // Reset circuit breaker on success
        if (blurCircuitBreaker.has(collectionSlug)) {
          blurCircuitBreaker.delete(collectionSlug);
        }

        return floor_price;

      } catch (error: any) {
        const errorStatus = error?.response?.status;

        // Record failure for circuit breaker (403/408 only)
        if (errorStatus === 403 || errorStatus === 408) {
          recordBlurAPIFailure(collectionSlug, errorStatus);
          const state = blurCircuitBreaker.get(collectionSlug);
          warnLog('Collection', `[BlurCircuitBreaker] ${collectionSlug} | Failure recorded: ${state?.failureCount}/${BLUR_CIRCUIT_OPEN_THRESHOLD} | status=${errorStatus}`);
        }

        // Retry on errors if attempts remaining
        if (attempt < MAX_RETRIES) {
          warnLog('Collection', `[getBlurFloor] ${collectionSlug} | Fetch failed, retrying (${attempt}/${MAX_RETRIES}) | status=${errorStatus}`);
          await new Promise(resolve => setTimeout(resolve, BASE_DELAY * Math.pow(2, attempt - 1)));
          continue;
        }

        // Final attempt failed - log and return 0
        errorLog('Collection', `[getBlurFloor] ${collectionSlug} | Error fetching Blur floor price after ${MAX_RETRIES} attempts | status=${errorStatus}`, error);
        return 0;
      }
    }

    // Should not reach here, but return 0 as fallback
    return 0;
  });

  return result || 0;
}

export async function getMagicEdenFloor(collectionSlug: string): Promise<number> {
  const CACHE_TTL = 60; // 1 minute cache (updated from 30 seconds)
  const cacheKey = `magiceden:floor}:${collectionSlug}`;
  const lockKey = `floor:magiceden}:${collectionSlug}`;

  // Try to get from cache first
  try {
    const cachedFloor = getRedis() ? await getRedis().get(cacheKey) : null;
    if (cachedFloor) {
      return JSON.parse(cachedFloor);
    }
  } catch {
    warnLog('Cache', `[collection.ts] Error reading Magic Eden floor from cache | slug=${collectionSlug}`);
  }

  // If not in cache, try to acquire lock and fetch
  const result = await lockManager.withLock(lockKey, async () => {
    // Double check cache after acquiring lock
    const cachedFloor = getRedis() ? await getRedis().get(cacheKey) : null;
    if (cachedFloor) {
      return JSON.parse(cachedFloor);
    }

    // Retry configuration for floor price fetching
    const MAX_RETRIES = 3;
    const BASE_DELAY = 500; // 500ms, 1000ms, 2000ms exponential backoff

    // Import Magic Eden functions dynamically to avoid circular dependency
    const { fetchMagicEdenCollectionStats } = await import('../marketplace/magiceden');

    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      try {
        const stats = await fetchMagicEdenCollectionStats(collectionSlug);

        const floor_price = stats ? Number(stats) : 0;

        // If floor is 0 and we have retries left, retry (likely temporary API issue)
        if (floor_price === 0 && attempt < MAX_RETRIES) {
          warnLog('API', `[collection.ts] MagicEden floor price is 0, retrying (${attempt}/${MAX_RETRIES}) | slug=${collectionSlug}`);
          await new Promise(resolve => setTimeout(resolve, BASE_DELAY * Math.pow(2, attempt - 1)));
          continue;
        }

        // Only cache positive floor prices - don't cache 0 values
        if (floor_price > 0 && getRedis()) {
          await getRedis().set(cacheKey, JSON.stringify(floor_price), 'EX', CACHE_TTL);
        }

        return floor_price;

      } catch (error: any) {
        // Retry on errors if attempts remaining
        if (attempt < MAX_RETRIES) {
          // Better error parsing: show HTTP status or network error code
          const errorCode = error?.response?.status || error?.code || 'UNKNOWN';
          warnLog('API', `[collection.ts] MagicEden floor price fetch failed, retrying (${attempt}/${MAX_RETRIES}) | slug=${collectionSlug} | error=${errorCode}`);
          await new Promise(resolve => setTimeout(resolve, BASE_DELAY * Math.pow(2, attempt - 1)));
          continue;
        }

        // Final attempt failed - log and return 0
        const errorCode = error?.response?.status || error?.code || 'UNKNOWN';
        errorLog('API', `[collection.ts] Error fetching Magic Eden floor price after ${MAX_RETRIES} attempts | slug=${collectionSlug} | error=${errorCode}`, error);
        return 0;
      }
    }

    // Should not reach here, but return 0 as fallback
    return 0;
  });

  return result || 0;
}

export async function getAllFloorPrices(collectionSlug: string): Promise<{
  opensea: number;
  blur: number;
  magiceden: number;
}> {
  const [opensea, blur, magiceden] = await Promise.allSettled([
    getOpenseaFloor(collectionSlug),
    getBlurFloor(collectionSlug),
    getMagicEdenFloor(collectionSlug)
  ]);

  return {
    opensea: opensea.status === 'fulfilled' ? opensea.value : 0,
    blur: blur.status === 'fulfilled' ? blur.value : 0,
    magiceden: magiceden.status === 'fulfilled' ? magiceden.value : 0,
  };
}

export async function getOpenseaCollectionStats(collectionSlug: string) {
  const { data } = await apiDedup(
    `opensea-stats-${collectionSlug.toLowerCase()}`,
    () => limiter.schedule(() => axiosInstance.get(
      `${BASE_URL}/opensea/api/v2/collections/${collectionSlug}/stats`,
      {
        headers: { 'x-api-key': API_KEY }
      }
    )),
    { category: 'Collection' }
  );
  if (!data?.total) {
    warnLog('Collection', `[collection.ts] No listings found for collection | slug=${collectionSlug}`);
    throw new Error(`No listings found for collection ${collectionSlug}`);
  }
  return data?.total?.floor_price;
}

export async function getCollectionEvents(
  collectionSlug: string,
  eventTypes: string[] = ['all', 'cancel', 'listing', 'offer', 'order', 'sale', 'transfer'],
  limit: number = 50
) {
  const cacheKey = `collectionEvents:${collectionSlug}:${eventTypes.join(',')}}:${limit}`;
  const cachedData = getRedis() ? await getRedis().get(cacheKey) : null;
  if (cachedData) {
    return JSON.parse(cachedData);
  }

  try {
    const params = new URLSearchParams();
    eventTypes.forEach(type => params.append('event_type', type));
    params.append('limit', limit.toString());

    const { data } = await apiDedup(
      `opensea-events-${collectionSlug}-${eventTypes.sort().join(',')}-${limit}`,
      () => limiter.schedule(() => axiosInstance.get<CollectionEventResponse>(
        `${BASE_URL}/opensea/api/v2/events/collection/${collectionSlug}`,
        {
          params: params,
          headers: { 'x-api-key': API_KEY }
        }
      )),
      { category: 'Collection' }
    );

    // Cache for a short period (e.g., 30 seconds) since events data changes frequently
    if (getRedis()) {
      await getRedis().setex(cacheKey, 10, JSON.stringify(data));
    }
    return data;
  } catch (error) {
    errorLog('Collection', `[collection.ts] Error fetching collection events | slug=${collectionSlug}`, error);
    throw error;
  }
}
function hashData(data: any): string {
  const stableString = stringify(data) as string;
  return crypto.createHash('sha256')
    .update(stableString, 'utf8')
    .digest('hex');
}

export async function fetchOpenSeaCollectionItems(
  variables: CollectionItemsQueryVariables,
  allTokenIds: string[] = [],
  cursor?: string,
  taskId?: string,
  expectedCount?: number
): Promise<string[]> {
  const CACHE_TTL = 3600; // 1 hour cache
  const cacheKey = `opensea:collection-items:${hashData(variables)}`;
  const lockKey = `opensea:lock:collection-items:${variables.collectionSlug}:${hashData(variables)}`;

  // Try to get from cache first (only for initial request)
  if (!cursor) {
    try {
      const cachedData = getRedis() ? await getRedis().get(cacheKey) : null;
      if (cachedData) {
        return JSON.parse(cachedData);
      }
    } catch {
      warnLog('Cache', `[collection.ts] Error reading collection items from cache | slug=${variables.collectionSlug}`);
    }
  }

  // For paginated requests or cache miss, fetch data
  try {
    // Only use lock for initial request
    if (!cursor) {
      const result = await lockManager.withLock(lockKey, async () => {
        // Double check cache after acquiring lock
        const cachedData = getRedis() ? await getRedis().get(cacheKey) : null;
        if (cachedData) {
          return JSON.parse(cachedData);
        }

        // Fetch data
        return await fetchCollectionItemsRecursive(variables, allTokenIds, cursor, 1, taskId, expectedCount);
      });

      if (result) {
        // Cache the final result
        if (getRedis() && result.length > 0) {
          await getRedis().set(cacheKey, JSON.stringify(result), 'EX', CACHE_TTL);
        }
        return result;
      }

      // If lock acquisition failed, wait and try cache again
      await new Promise(resolve => setTimeout(resolve, 100));
      const cachedData = getRedis() ? await getRedis().get(cacheKey) : null;
      if (cachedData) {
        return JSON.parse(cachedData);
      }
      throw new Error('Failed to acquire lock for collection items fetch');
    } else {
      // For paginated requests, fetch directly without lock
      return await fetchCollectionItemsRecursive(variables, allTokenIds, cursor, 1, taskId, expectedCount);
    }
  } catch (error) {
    errorLog('Collection', `[collection.ts] Error fetching OpenSea collection items | slug=${variables.collectionSlug}`, error);
    throw new Error(`Failed to fetch collection items for ${variables.collectionSlug}`);
  }
}

// Helper function for recursive fetching
async function fetchCollectionItemsRecursive(
  variables: CollectionItemsQueryVariables,
  allTokenIds: string[] = [],
  cursor?: string,
  pageNumber: number = 1,
  taskId?: string,
  expectedCount?: number
): Promise<string[]> {
  const queryVariables = {
    ...variables,
    cursor: cursor
  };
  const payload = {
    operationName: 'CollectionItemsListQuery',
    query: CollectionItemsListQuery,
    variables: queryVariables
  };

  try {
    const { data } = await apiDedup(
      `opensea-items-${variables.collectionSlug}-${cursor || 'initial'}`,
      () => limiter.schedule(() => axiosInstance.post<CollectionItemsResponse>(`${BASE_URL}/opensea/graphql`, payload, {
        headers: {
          "x-api-key": API_KEY
        },
      })),
      { category: 'Collection' }
    );

    // Check if collectionItems is null
    if (!data?.data?.collectionItems) {
      if (allTokenIds.length > 0) {
        return [...new Set(allTokenIds)];
      }
      return [];
    }
    const tokenIds = data.data.collectionItems.items.map((item) => item.tokenId);

    if (tokenIds.length === 0 && allTokenIds.length > 0) {
      return [...new Set(allTokenIds)];
    }

    // Add current page's token IDs to our accumulated list
    const updatedTokenIds = [...allTokenIds, ...tokenIds];

    // Update processing percentage if taskId is provided
    if (taskId) {
      // Import taskStatus from main module - need to handle this differently
      // For now, we'll use a callback approach or direct module access
      try {
        // Calculate percentage based on tokens fetched vs expected
        let percentage = 0;
        if (expectedCount && expectedCount > 0) {
          percentage = Math.min(95, (updatedTokenIds.length / expectedCount) * 100);
        } else {
          // If no expected count, use a more conservative progress estimate
          // Assuming average collection size and 100 items per page
          // Use a logarithmic scale that approaches but never reaches 95%
          percentage = Math.min(95, Math.log10(pageNumber + 1) * 30);
        }

        // Update task status - we'll need to export a function from index.ts
        const { updateTaskProcessingPercentage } = require('..');
        if (updateTaskProcessingPercentage) {
          updateTaskProcessingPercentage(taskId, percentage);
        }
      } catch {
        // Silently continue if update fails
      }
    }

    // Get the next cursor
    const nextCursor = data.data.collectionItems.nextPageCursor;

    if (nextCursor) {
      return fetchCollectionItemsRecursive(variables, updatedTokenIds, nextCursor, pageNumber + 1, taskId, expectedCount);
    }

    // Reset processing percentage when done
    if (taskId) {
      try {
        const { updateTaskProcessingPercentage } = require('..');
        if (updateTaskProcessingPercentage) {
          updateTaskProcessingPercentage(taskId, 0);
        }
      } catch {
        // Silently continue
      }
    }

    const uniqueTokenIds = [...new Set(updatedTokenIds)];

    return uniqueTokenIds;
  } catch (error) {
    // Return what we have so far instead of failing completely
    if (allTokenIds.length > 0) {
      return [...new Set(allTokenIds)];
    }
    throw error;
  }
}

interface TraitValue {
  value: string;
  count: number;
  __typename?: string;
}

interface TraitRange {
  min: number;
  max: number;
  __typename?: string;
}

interface CollectionAttribute {
  traitType: string;
  values?: TraitValue[];
  range?: TraitRange;
  __typename?: string;
}

interface TraitResponse {
  data: {
    collectionBySlug: {
      id: string;
      attributes: {
        items: CollectionAttribute[];
        nextPageCursor: string | null;
      };
      __typename: string;
    };
  };
}

export async function fetchOpenseaTraits(
  slug: string,
  limit: number = 100,
  cursor?: string
): Promise<CollectionAttribute[]> {
  const CACHE_TTL = 3600; // 1 hour cache
  const cacheKey = `opensea:traits:${slug}`;
  const lockKey = `opensea:lock:traits:${slug}`;

  // Try to get from cache first if no cursor (first page)
  if (!cursor) {
    try {
      const cachedTraits = getRedis() ? await getRedis().get(cacheKey) : null;
      if (cachedTraits) {
        return JSON.parse(cachedTraits);
      }
    } catch {
      warnLog('Cache', `[collection.ts] Error reading traits from cache | slug=${slug} | key=${cacheKey}`);
    }
  }

  try {
    // Only use lock for initial request
    if (!cursor) {
      const result = await lockManager.withLock(lockKey, async () => {
        // Double check cache after acquiring lock
        const cachedTraits = getRedis() ? await getRedis().get(cacheKey) : null;
        if (cachedTraits) {
          return JSON.parse(cachedTraits);
        }

        // Fetch traits recursively
        return await fetchOpenseaTraitsRecursive(slug, limit, cursor);
      });

      if (result) {
        // Cache the final result
        if (getRedis() && result.length > 0) {
          await getRedis().set(cacheKey, JSON.stringify(result), 'EX', CACHE_TTL);
        }
        return result;
      }

      // If lock acquisition failed, wait and try cache again
      await new Promise(resolve => setTimeout(resolve, 100));
      const cachedTraits = getRedis() ? await getRedis().get(cacheKey) : null;
      if (cachedTraits) {
        return JSON.parse(cachedTraits);
      }
      throw new Error('Failed to acquire lock for traits fetch');
    } else {
      // For paginated requests, fetch directly without lock
      return await fetchOpenseaTraitsRecursive(slug, limit, cursor);
    }
  } catch (error) {
    errorLog('Collection', `[collection.ts] Error fetching OpenSea traits | slug=${slug}`, error);
    throw new Error(`Failed to fetch traits for collection ${slug}: ${error instanceof Error ? error.message : 'Unknown error'}`);
  }
}

// Helper function for recursive fetching
async function fetchOpenseaTraitsRecursive(
  slug: string,
  limit: number = 100,
  cursor?: string
): Promise<CollectionAttribute[]> {
  const payload = {
    operationName: 'TraitFiltersSectionQuery',
    query: "query TraitFiltersSectionQuery($slug: String!, $limit: Int!, $cursor: String) {\n  collectionBySlug(slug: $slug) {\n    __typename\n    ... on Collection {\n      id\n      attributes(limit: $limit, cursor: $cursor) {\n        items {\n          traitType\n          ...TraitFilterPanelFragment\n          __typename\n        }\n        nextPageCursor\n        __typename\n      }\n      __typename\n    }\n  }\n}\nfragment TraitFilterPanelFragment on CollectionAttribute {\n  traitType\n  range {\n    __typename\n  }\n  values {\n    __typename\n  }\n  ...TraitFiltersStringTraitInputFragment\n  ...TraitFiltersNumericTraitInputFragment\n  __typename\n}\nfragment TraitFiltersStringTraitInputFragment on CollectionAttribute {\n  traitType\n  values {\n    value\n    count\n    __typename\n  }\n  __typename\n}\nfragment TraitFiltersNumericTraitInputFragment on CollectionAttribute {\n  traitType\n  range {\n    min\n    max\n    __typename\n  }\n  __typename\n}",
    variables: {
      limit,
      slug,
      cursor
    }
  };

  const { data } = await apiDedup(
    `opensea-traits-${slug}-${cursor || 'initial'}`,
    () => limiter.schedule({ priority: 0 }, () =>
      axiosInstance.post<TraitResponse>(`${BASE_URL}/opensea/graphql`, payload, {
        headers: {
          "x-api-key": API_KEY
        },
      })
    ),
    { category: 'Collection' }
  );

  if (!data?.data?.collectionBySlug?.attributes) {
    warnLog('Collection', `[collection.ts] No attributes found for collection | slug=${slug}`);
    throw new Error(`No attributes found for collection ${slug}`);
  }

  let attributes = data.data.collectionBySlug.attributes.items;
  const nextCursor = data.data.collectionBySlug.attributes.nextPageCursor;

  // If there's more data, fetch recursively
  if (nextCursor) {
    const nextPageAttributes = await fetchOpenseaTraitsRecursive(slug, limit, nextCursor);
    attributes = [...attributes, ...nextPageAttributes];
  }

  // Clean up the attributes data structure
  const cleanedAttributes = attributes.map(attr => {
    const cleaned: any = {
      traitType: attr.traitType,
      values: attr.values?.map(v => ({
        value: v.value,
        count: v.count
      })) || []
    };

    // Only include range if it exists and is not null
    if (attr.range) {
      cleaned.range = {
        min: attr.range.min,
        max: attr.range.max
      };
    }

    return cleaned;
  });

  return cleanedAttributes;
}

interface TraitFloorPrice {
  native?: {
    unit: string;
    __typename?: string;
  };
  usd?: number;
  token?: {
    unit: string;
    symbol: string;
    contractAddress: string;
    __typename?: string;
  };
  __typename?: string;
}

interface TraitWithFloorPrice {
  traitType: string;
  value: string;
  count: number;
  floorPrice?: TraitFloorPrice;
}

export async function fetchOpenseaTraitFloors(
  slug: string,
  traits: Array<{ traitType: string; values: string[] }>
): Promise<TraitWithFloorPrice[]> {
  const CACHE_TTL = 60; // 1 minute cache
  const traitHash = hashData(traits);
  const cacheKey = `opensea:trait-floors:${slug}:${traitHash}`;
  const lockKey = `opensea:lock:trait-floors:${slug}:${traitHash}`;

  // Try to get from cache first
  try {
    const cachedData = getRedis() ? await getRedis().get(cacheKey) : null;
    if (cachedData) {
      return JSON.parse(cachedData);
    }
  } catch {
    warnLog('Cache', `[collection.ts] Error reading trait floors from cache | slug=${slug}`);
  }

  // If not in cache, try to acquire lock and fetch
  const result = await lockManager.withLock(lockKey, async () => {
    // Double check cache after acquiring lock
    const cachedData = getRedis() ? await getRedis().get(cacheKey) : null;
    if (cachedData) {
      return JSON.parse(cachedData);
    }

    try {
      // Build the attributes filter for the query
      const attributesFilter = traits.map(trait => ({
        traitType: trait.traitType,
        values: trait.values
      }));

      const payload = {
        operationName: 'CollectionItemsListQuery',
        query: `query CollectionItemsListQuery($collectionSlug: String!, $filter: CollectionItemsFilter, $limit: Int!) {
          collectionItems(
            collectionSlug: $collectionSlug
            filter: $filter
            limit: $limit
            sort: { by: PRICE, direction: ASC }
          ) {
            items {
              id
              tokenId
              attributes {
                traitType
                value
                floorPrice {
                  native {
                    unit
                    __typename
                  }
                  usd
                  token {
                    unit
                    symbol
                    contractAddress
                    __typename
                  }
                  __typename
                }
                stats {
                  itemCount
                  percent
                  __typename
                }
                __typename
              }
              bestListing {
                pricePerItem {
                  usd
                  token {
                    unit
                    symbol
                    __typename
                  }
                  __typename
                }
                __typename
              }
              __typename
            }
            __typename
          }
        }`,
        variables: {
          collectionSlug: slug,
          filter: {
            attributes: attributesFilter
          },
          limit: 1  // We only need one item per trait combo to get floor prices
        }
      };

      const { data } = await apiDedup(
        `opensea-trait-floors-${slug}-${hashData(traits)}`,
        () => limiter.schedule(() =>
          axiosInstance.post(`${BASE_URL}/opensea/graphql`, payload, {
            headers: {
              "x-api-key": API_KEY
            },
          })
        ),
        { category: 'Collection' }
      );

      // Extract unique trait floor prices from the response
      const traitFloorMap = new Map<string, TraitWithFloorPrice>();

      if (data?.data?.collectionItems?.items) {
        for (const item of data.data.collectionItems.items) {
          if (item.attributes) {
            for (const attr of item.attributes) {
              const key = `${attr.traitType}:${attr.value}`;
              if (!traitFloorMap.has(key) && attr.floorPrice) {
                traitFloorMap.set(key, {
                  traitType: attr.traitType,
                  value: attr.value,
                  count: attr.stats?.itemCount || 0,
                  floorPrice: attr.floorPrice
                });
              }
            }
          }
        }
      }

      const result = Array.from(traitFloorMap.values());

      // Cache the result
      if (getRedis() && result.length > 0) {
        await getRedis().set(cacheKey, JSON.stringify(result), 'EX', CACHE_TTL);
      }

      return result;
    } catch (error) {
      errorLog('Collection', `[collection.ts] Error fetching OpenSea trait floors | slug=${slug}`, error);
      throw error;
    }
  });

  if (!result) {
    // If we couldn't acquire the lock, wait briefly and try cache again
    await new Promise(resolve => setTimeout(resolve, 100));
    try {
      const cachedData = getRedis() ? await getRedis().get(cacheKey) : null;
      if (cachedData) {
        return JSON.parse(cachedData);
      }
    } catch {
      warnLog('Cache', `[collection.ts] Error reading trait floors from cache after lock failure | slug=${slug}`);
    }
    throw new Error(`Failed to fetch trait floors for collection ${slug}: Could not acquire lock`);
  }

  return result;
}

interface CollectionItemsFilter {
  attributes?: Array<{
    traitType: string;
    values: string[];
  }>;
}

interface CollectionItemsSort {
  by: 'PRICE' | 'CREATED_DATE' | 'RARITY';
  direction: 'ASC' | 'DESC';
}

export interface CollectionItemsQueryVariables {
  collectionSlug: string;
  filter?: CollectionItemsFilter;
  limit?: number;
  sort?: CollectionItemsSort;
  cursor?: string;
}

interface TokenPrice {
  unit: number;
  symbol: string;
}

interface BestListing {
  pricePerItem: {
    token: TokenPrice;
  };
}

interface LastSale {
  native: {
    unit: number;
  };
}

interface Rarity {
  rank: number;
  category: string;
}

interface CollectionItem {
  id: string;
  name: string;
  imageUrl: string;
  tokenId: string;
  bestListing: BestListing | null;
  lastSale: LastSale | null;
  rarity: Rarity;
}

export interface CollectionItemsResponse {
  data: {
    collectionItems: {
      items: CollectionItem[];
      nextPageCursor: string;
    };
  };
}

interface CollectionEventResponse {
  asset_events: AssetEvent[];
  next: string;
}

interface AssetEvent {
  event_type: string;
  order_hash: string;
  order_type: string;
  chain: string;
  protocol_address: string;
  start_date: number;
  expiration_date: number;
  asset: null;
  quantity: number;
  maker: string;
  taker: string;
  payment: Payment;
  criteria: Criteria;
  event_timestamp: number;
  is_private_listing: boolean;
}

interface Payment {
  quantity: string;
  token_address: string;
  decimals: number;
  symbol: string;
}

interface Criteria {
  collection: {
    slug: string;
  };
  contract: {
    address: string;
  };
  trait: null;
  encoded_token_ids: null;
}

interface Price {
  current: {
    currency: string;
    decimals: number;
    value: string;
  }
}

interface OfferItem {
  itemType: number;
  token: string;
  identifierOrCriteria: string;
  startAmount: string;
  endAmount: string;
}

interface ConsiderationItem {
  itemType: number;
  token: string;
  identifierOrCriteria: string;
  startAmount: string;
  endAmount: string;
  recipient: string;
}

interface ProtocolParameters {
  offerer: string;
  offer: OfferItem[];
  consideration: ConsiderationItem[];
  startTime: string;
  endTime: string;
  orderType: number;
  zone: string;
  zoneHash: string;
  salt: string;
  conduitKey: string;
  totalOriginalConsiderationItems: number;
  counter: string | number;
}

interface ProtocolData {
  parameters: ProtocolParameters;
  signature: string | null;
}

interface Listing {
  order_hash: string;
  chain: string;
  type: string;
  price: Price;
  protocol_data: ProtocolData;
  protocol_address: string;
}

interface ListingsResponse {
  listings: Listing[];
  next: string;
}

// GraphQL response types for collection token query
interface CollectionTokenItem {
  tokenId: string;
  assetContract?: {
    address: string;
  };
}

interface CollectionTokensQueryResponse {
  data: {
    collectionItems: {
      items: CollectionTokenItem[];
      nextPageCursor: string | null;
    };
  };
}

/**
 * Fetches token ID to contract address mapping for a collection.
 * Used for multi-contract collections where different tokens may belong to different contracts.
 *
 * @param collectionSlug - The OpenSea collection slug
 * @returns Record mapping token IDs to their contract addresses
 */
export async function getCollectionTokenContractMap(collectionSlug: string): Promise<Record<string, string>> {
  const map: Record<string, string> = {};

  try {
    // GraphQL query to fetch token items with contract information
    const query = `
      query CollectionTokensQuery($collectionSlug: String!, $cursor: String, $limit: Int) {
        collectionItems(
          collectionSlug: $collectionSlug
          cursor: $cursor
          sort: { by: TOKEN_ID, direction: ASC }
          limit: $limit
        ) {
          items {
            tokenId
            assetContract {
              address
            }
          }
          nextPageCursor
        }
      }
    `;

    let cursor: string | null = null;
    let hasMore = true;
    const BATCH_SIZE = 200; // Fetch in batches
    const MAX_TOKENS = 10000; // Safety limit to prevent infinite loops
    let totalFetched = 0;

    // Paginate through all tokens in the collection
    while (hasMore && totalFetched < MAX_TOKENS) {
      const payload: {
        query: string;
        variables: {
          collectionSlug: string;
          cursor: string | null;
          limit: number;
        };
      } = {
        query,
        variables: {
          collectionSlug,
          cursor,
          limit: BATCH_SIZE
        }
      };

      const response: AxiosResponse<CollectionTokensQueryResponse> = await apiDedup(
        `opensea-token-contracts-${collectionSlug}-${cursor || 'initial'}`,
        () => limiter.schedule(() =>
          axiosInstance.post<CollectionTokensQueryResponse>(
            `${BASE_URL}/opensea/graphql`,
            payload,
            {
              headers: {
                'x-api-key': API_KEY
              }
            }
          )
        ),
        { category: 'Collection' }
      );

      const items: CollectionTokenItem[] = response?.data?.data?.collectionItems?.items || [];
      const nextCursor: string | null = response?.data?.data?.collectionItems?.nextPageCursor ?? null;

      // Build the mapping
      for (const item of items) {
        if (item?.tokenId && item?.assetContract?.address) {
          map[item.tokenId] = item.assetContract.address.toLowerCase();
          totalFetched++;
        }
      }

      // Check if there are more items to fetch
      if (!nextCursor || items.length === 0) {
        hasMore = false;
      } else {
        cursor = nextCursor;
      }
    }

    return map;
  } catch (error) {
    // Log error but return empty map to allow graceful degradation
    errorLog('Collection', `[collection.ts] Error fetching collection token contract map | slug=${collectionSlug}`, error);

    return map; // Return whatever we've collected so far
  }
}
