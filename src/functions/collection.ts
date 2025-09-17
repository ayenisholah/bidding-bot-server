import { config } from "dotenv";
import stringify from 'json-stable-stringify';
import crypto from 'crypto';
import { axiosInstance, limiter } from "../init"; // Import Redis client
import { DistributedLockManager } from "../utils/lock";
import { API_KEY } from "..";
import redisClient from '../utils/redis';
import { CollectionItemsListQuery } from "../queries";
import { BASE_URL } from "../constants";
import { logger, DatabaseOperation } from "../utils/logger";
import { ICollectionDetails, ICollectionFee } from "../types/collection.types";
import { setOpenseaMarketplaceFee } from "../utils/marketplace-fee-cache";

config()

const collectionCache: { [key: string]: ICollectionDetails } = {};
const MAX_CACHE_ITEMS = 1000;

const OPENSEA_FEE_ADDRESS = "0x0000a26b00c1F0DF003000390027140000fAa719";
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
    } catch (error) {
      await logger.logDatabaseError('Error reading collection details from Redis cache', {
        databaseOperation: DatabaseOperation.READ,
        collection_name: 'opensea_collection_cache',
        collection: slug,
        cacheKey,
        error: error as Error,
        errorMessage: error instanceof Error ? error.message : String(error)
      });
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
          limiter.schedule(() => axiosInstance.get(
            `${BASE_URL}/opensea/api/v2/collections/${slug}`,
            {
              headers: {
                'x-api-key': API_KEY
              }
            }
          )),
          20000, // 20 second timeout for API call
          `OpenSea API call for ${slug}`
        );

        let creator_fees: { [key: string]: number } = {};
        let enforceCreatorFee = false;
        if (collection?.fees && Array.isArray(collection.fees)) {
          // console.log(`[Fee Processing] ${slug} - Starting to process ${collection.fees.length} fees`);

          // Process all fees except the OpenSea platform fee
          for (const fee of collection.fees) {
            const isOpenSeaFee = fee.recipient.toLowerCase() === OPENSEA_FEE_ADDRESS_LOWER;


            // Skip OpenSea platform fee
            if (isOpenSeaFee) {
              continue;
            }

            creator_fees[fee.recipient] = fee.fee * 100;

            // If any creator fee is required, set enforceCreatorFee to true
            if (fee.required) {
              enforceCreatorFee = true;
            }
          }
        }

        // If no creator fees found, use default
        if (Object.keys(creator_fees).length === 0) {
          creator_fees = { null: 0 };
        }

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
          } catch (error) {
            console.log(error);
          }
        }
        return result;
      }),
      30000, // 30 second timeout for entire operation
      `Collection details fetch for ${slug}`
    );

    if (!result) {
      await logger.logOpenseaCollectionError('Failed to acquire lock for collection details', {
        collection: slug,
        contractAddress,
        operation: 'get_collection_details',
        errorCode: 'LOCK_ACQUISITION_FAILED'
      });
      throw new Error('Failed to acquire lock for collection details');
    }
    console.log({ getCollectionDetails: result.fees });

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
    await logger.logOpenseaCollectionError(`Failed to fetch collection details: ${error}`, {
      collection: slug,
      contractAddress,
      operation: 'get_collection_details',
      errorCode: 'FETCH_FAILED',
      error: error as Error
    });

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
  const lockKey = `floor:opensea}:${collectionSlug}`;

  // Try to get from cache first
  try {
    const cachedFloor = getRedis() ? await getRedis().get(cacheKey) : null;
    if (cachedFloor) {
      return JSON.parse(cachedFloor);
    }
  } catch (error) {
    await logger.logDatabaseError('Error reading OpenSea floor price from cache', {
      databaseOperation: DatabaseOperation.READ,
      collection_name: 'getRedis()_cache',
      collection: collectionSlug,
      cacheKey,
      error: error as Error,
      errorMessage: error instanceof Error ? error.message : String(error)
    });
    console.error('Error reading from cache:', error);
  }

  // If not in cache, try to acquire lock and fetch
  const result = await lockManager.withLock(lockKey, async () => {
    // Double check cache after acquiring lock
    const cachedFloor = getRedis() ? await getRedis().get(cacheKey) : null;
    if (cachedFloor) {
      return JSON.parse(cachedFloor);
    }
    try {
      const { data } = await limiter.schedule(() => axiosInstance.get<ListingsResponse>(
        `${BASE_URL}/opensea/api/v2/listings/collection/${collectionSlug}/best?limit=1`,
        {
          headers: { 'x-api-key': API_KEY }
        }
      ));
      // Check if listings exist and has item
      const floor_price = Number(data?.listings[0]?.price?.current?.value) / 1e18 || 0;
      // Cache the result
      if (getRedis()) {
        await getRedis().set(cacheKey, JSON.stringify(floor_price), 'EX', CACHE_TTL);
      }
      return floor_price;
    } catch (error: any) {
      // Handle 404 errors specifically - collection might be delisted or renamed
      if (error?.response?.status === 404) {
        await logger.logOpenseaCollectionError('Collection not found on OpenSea', {
          collection: collectionSlug,
          collectionSlug: collectionSlug,
          endpoint: `${BASE_URL}/opensea/api/v2/listings/collection/${collectionSlug}/best`,
          method: 'GET',
          statusCode: 404,
          operation: 'get_floor_price',
          errorCode: 'COLLECTION_NOT_FOUND',
          errorMessage: error?.response?.data?.errors?.[0]?.message || 'Collection not found',
          error: new Error(`OpenSea collection not found: ${collectionSlug}`)
        });

        // Cache the zero floor price for a short time to avoid repeated 404s
        if (getRedis()) {
          await getRedis().set(cacheKey, JSON.stringify(0), 'EX', 300); // Cache for 5 minutes
        }
        return 0; // Return 0 floor price for non-existent collections
      }

      // For other errors, log as API error and re-throw
      await logger.logApiError('Failed to fetch OpenSea floor price', {
        collection: collectionSlug,
        collectionSlug: collectionSlug,
        endpoint: `${BASE_URL}/opensea/api/v2/listings/collection/${collectionSlug}/best`,
        method: 'GET',
        statusCode: error?.response?.status,
        operation: 'get_floor_price',
        errorCode: error?.code || 'API_ERROR',
        errorMessage: error?.response?.data?.errors?.[0]?.message || error?.message || 'Unknown error',
        error: error
      });

      throw error; // Re-throw to let the lock manager handle it
    }
  });

  if (!result) {
    // If we couldn't acquire the lock, wait briefly and try cache again
    await new Promise(resolve => setTimeout(resolve, 100));
    try {
      const cachedFloor = getRedis() ? await getRedis().get(cacheKey) : null;
      if (cachedFloor) {
        return JSON.parse(cachedFloor);
      }
    } catch (error) {
      await logger.logDatabaseError('Error reading from cache after lock failure', {
        databaseOperation: DatabaseOperation.READ,
        collection_name: 'getRedis()_cache',
        collection: collectionSlug,
        cacheKey,
        error: error as Error,
        errorMessage: error instanceof Error ? error.message : String(error)
      });
      console.error('Error reading from cache after lock failure:', error);
    }
    await logger.logOpenseaCollectionError('Failed to fetch OpenSea floor price', {
      collection: collectionSlug,
      operation: 'get_floor_price',
      errorCode: 'FLOOR_PRICE_FETCH_FAILED'
    });
    throw new Error('Failed to fetch OpenSea floor price');
  }

  return result;
}

export async function getBlurFloor(collectionSlug: string): Promise<number> {
  const CACHE_TTL = 60; // 1 minute cache (updated from 30 seconds)
  const cacheKey = `blur:floor}:${collectionSlug}`;
  const lockKey = `floor:blur}:${collectionSlug}`;

  // Try to get from cache first
  try {
    const cachedFloor = getRedis() ? await getRedis().get(cacheKey) : null;
    if (cachedFloor) {
      return JSON.parse(cachedFloor);
    }
  } catch (error) {
    console.error('Error reading Blur floor from cache:', error);
  }

  // If not in cache, try to acquire lock and fetch
  const result = await lockManager.withLock(lockKey, async () => {
    // Double check cache after acquiring lock
    const cachedFloor = getRedis() ? await getRedis().get(cacheKey) : null;
    if (cachedFloor) {
      return JSON.parse(cachedFloor);
    }

    try {
      // Import Blur functions dynamically to avoid circular dependency
      const { fetchBlurCollectionStats } = await import('../marketplace/blur/bid');
      const stats = await fetchBlurCollectionStats(collectionSlug);

      const floor_price = stats?.floorPrice?.amount ?
        parseFloat(stats.floorPrice.amount) : 0;

      // Cache the result
      if (getRedis()) {
        await getRedis().set(cacheKey, JSON.stringify(floor_price), 'EX', CACHE_TTL);
      }
      return floor_price;
    } catch (error) {
      console.error('Error fetching Blur floor price:', error);
      return 0;
    }
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
  } catch (error) {
    console.error('Error reading Magic Eden floor from cache:', error);
  }

  // If not in cache, try to acquire lock and fetch
  const result = await lockManager.withLock(lockKey, async () => {
    // Double check cache after acquiring lock
    const cachedFloor = getRedis() ? await getRedis().get(cacheKey) : null;
    if (cachedFloor) {
      return JSON.parse(cachedFloor);
    }

    try {
      // Import Magic Eden functions dynamically to avoid circular dependency
      const { fetchMagicEdenCollectionStats } = await import('../marketplace/magiceden');
      const stats = await fetchMagicEdenCollectionStats(collectionSlug);

      const floor_price = stats?.floorPrice?.amount ?
        Number(stats.floorPrice.amount) / 1e18 : 0;

      // Cache the result
      if (getRedis()) {
        await getRedis().set(cacheKey, JSON.stringify(floor_price), 'EX', CACHE_TTL);
      }
      return floor_price;
    } catch (error) {
      console.error('Error fetching Magic Eden floor price:', error);
      return 0;
    }
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
  const { data } = await limiter.schedule(() => axiosInstance.get(
    `${BASE_URL}/opensea/api/v2/collections/${collectionSlug}/stats`,
    {
      headers: { 'x-api-key': API_KEY }
    }
  ));
  if (!data?.total) {
    await logger.logOpenseaCollectionError(`No listings found for collection ${collectionSlug}`, {
      collection: collectionSlug,
      operation: 'get_floor_price',
      errorCode: 'NO_LISTINGS_FOUND'
    });
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

    const { data } = await limiter.schedule(() => axiosInstance.get<CollectionEventResponse>(
      `${BASE_URL}/opensea/api/v2/events/collection/${collectionSlug}`,
      {
        params: params,
        headers: { 'x-api-key': API_KEY }
      }
    ));

    // Cache for a short period (e.g., 30 seconds) since events data changes frequently
    if (getRedis()) {
      await getRedis().setex(cacheKey, 10, JSON.stringify(data));
    }
    return data;
  } catch (error) {
    await logger.logOpenseaCollectionError('Error fetching collection events', {
      collectionSlug: collectionSlug,
      operation: 'fetch_collection_events',
      error: error as Error,
      errorCode: (error as any)?.response?.status,
      errorMessage: (error as any)?.response?.data || (error instanceof Error ? error.message : String(error))
    });
    console.error('Error fetching collection events:', error);
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

  // Debug log the initial request
  if (!cursor) {
    // console.log(`[DEBUG] Fetching OpenSea collection items for ${variables.collectionSlug} with filter:`, JSON.stringify(variables.filter));
  }

  // Try to get from cache first (only for initial request)
  if (!cursor) {
    try {
      const cachedData = getRedis() ? await getRedis().get(cacheKey) : null;
      if (cachedData) {
        // console.log(`[DEBUG] Cache hit for ${variables.collectionSlug}`);
        return JSON.parse(cachedData);
      }
      // console.log(`[DEBUG] Cache miss for ${variables.collectionSlug}`);
    } catch (error) {
      console.error('Error reading collection items from cache:', error);
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
    // Enhanced error logging
    console.error(`[ERROR] OpenSea API error for ${variables.collectionSlug}:`);
    console.error(`  Status: ${(error as any)?.response?.status}`);
    console.error(`  Data: ${JSON.stringify((error as any)?.response?.data)}`);
    console.error(`  Headers: ${JSON.stringify((error as any)?.response?.headers)}`);
    console.error(`  Message: ${error instanceof Error ? error.message : String(error)}`);

    await logger.logOpenseaCollectionError('Error fetching OpenSea collection items', {
      collectionSlug: variables.collectionSlug,
      operation: 'fetch_collection_items',
      error: error as Error,
      errorCode: (error as any)?.response?.status,
      errorMessage: (error as any)?.response?.data || (error instanceof Error ? error.message : String(error))
    });
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
    const { data } = await limiter.schedule(() => axiosInstance.post<CollectionItemsResponse>(`${BASE_URL}/opensea/graphql`, payload, {
      headers: {
        "x-api-key": API_KEY
      },
    }));

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

        // console.log(`[fetchCollectionItems] Task ${taskId}: Page ${pageNumber}, ${updatedTokenIds.length} tokens, ${percentage.toFixed(1)}% complete`);
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
      // console.log(`[WARN] Returning partial results (${allTokenIds.length} tokens) due to error`);
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
    } catch (error) {
      await logger.logDatabaseError('Error reading traits from cache', {
        databaseOperation: DatabaseOperation.READ,
        collection_name: 'getRedis()_cache',
        collection: slug,
        cacheKey,
        error: error as Error,
        errorMessage: error instanceof Error ? error.message : String(error)
      });
      console.error('Error reading traits from cache:', error);
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
    await logger.logOpenseaCollectionError(`Error fetching OpenSea traits for ${slug}`, {
      collection: slug,
      operation: 'fetch_traits',
      error: error as Error,
      errorCode: (error as any)?.response?.status,
      errorMessage: (error as any)?.response?.data || (error instanceof Error ? error.message : String(error))
    });
    console.error(`Error fetching OpenSea traits for ${slug}:`, error);
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

  const { data } = await limiter.schedule(() =>
    axiosInstance.post<TraitResponse>(`${BASE_URL}/opensea/graphql`, payload, {
      headers: {
        "x-api-key": API_KEY
      },
    })
  );

  if (!data?.data?.collectionBySlug?.attributes) {
    await logger.logOpenseaCollectionError(`No attributes found for collection ${slug}`, {
      collection: slug,
      operation: 'fetch_traits',
      errorCode: 'NO_ATTRIBUTES_FOUND'
    });
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
  } catch (error) {
    console.error('Error reading trait floors from cache:', error);
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

      const { data } = await limiter.schedule(() =>
        axiosInstance.post(`${BASE_URL}/opensea/graphql`, payload, {
          headers: {
            "x-api-key": API_KEY
          },
        })
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
      await logger.logOpenseaCollectionError(`Error fetching OpenSea trait floors for ${slug}`, {
        collection: slug,
        operation: 'fetch_trait_floors',
        traits: traits.map(t => ({ traitType: t.traitType, valuesCount: t.values.length })),
        error: error as Error,
        errorCode: (error as any)?.response?.status,
        errorMessage: (error as any)?.response?.data || (error instanceof Error ? error.message : String(error))
      });
      console.error(`Error fetching OpenSea trait floors for ${slug}:`, error);
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
    } catch (error) {
      console.error('Error reading from cache after lock failure:', error);
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
