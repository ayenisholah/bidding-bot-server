import { limiter, axiosInstance } from "../init";
import { API_KEY } from "..";
import { BASE_URL } from "../constants";
import { CollectionItemsListQuery } from "../queries";
import RarityCache from "../models/rarityCache.model";
import { DistributedLockManager } from "../utils/lock";
import redisClient from '../utils/redis';
import { errorLog } from '../utils/debug-file-logger';
import { apiDedup } from '../utils/api-dedup';

const getRedis = () => redisClient.getClient();

const lockManager = new DistributedLockManager({
  lockPrefix: 'rarity:fetch:',
  defaultTTLSeconds: 300 // 5 minutes
});

interface GraphQLItem {
  id: string;
  tokenId: string;
  rarity: {
    rank: number;
    category: "LEGENDARY" | "EPIC" | "RARE" | string;
  };
}

interface GraphQLResponse {
  data: {
    collectionItems: {
      items: GraphQLItem[];
      nextPageCursor: string | null;
    };
  };
}

interface RarityFetchResult {
  tokenIds: number[];
  totalCount: number;
  cached: boolean;
  lastUpdated: Date;
}

/**
 * Recursively fetch rarity tokens from OpenSea GraphQL API with rate limiting
 */
async function fetchRarityTokensRecursive(
  collectionSlug: string,
  rarityCategory: "LEGENDARY" | "EPIC" | "RARE",
  cursor: string | null = null,
  accumulatedTokenIds: number[] = []
): Promise<number[]> {
  const payload = {
    operationName: "CollectionItemsListQuery",
    query: CollectionItemsListQuery,
    variables: {
      collectionSlug,
      cursor,
      limit: 100,
      sort: { by: "RARITY", direction: "DESC" },
    },
  };

  try {
    // Use limiter to respect rate limits
    const { data } = await apiDedup(
      `opensea-rarity-${collectionSlug}-${rarityCategory}-${cursor || 'initial'}`,
      () => limiter.schedule(() =>
        axiosInstance.post<GraphQLResponse>(
          `${BASE_URL}/opensea/graphql`,
          payload,
          {
            headers: { "x-api-key": API_KEY },
          }
        )
      ),
      { category: 'Rarity' }
    );

    const items = data?.data?.collectionItems?.items || [];
    const nextCursor = data?.data?.collectionItems?.nextPageCursor;

    // Filter items by the selected rarity category and extract tokenIds
    const filteredTokenIds = items
      .filter((item) => item.rarity?.category === rarityCategory)
      .map((item) => parseInt(item.tokenId, 10))
      .filter((id) => !isNaN(id));

    const allTokenIds = [...accumulatedTokenIds, ...filteredTokenIds];

    // Early stopping optimization based on rarity sort order (DESC)
    const shouldStopEarly = (() => {
      const categoriesInResponse = new Set(
        items.map((item) => item.rarity?.category).filter(Boolean)
      );

      if (rarityCategory === "LEGENDARY") {
        // Stop if we see any other category (EPIC, RARE, or lower)
        return Array.from(categoriesInResponse).some(
          (cat) => cat !== "LEGENDARY"
        );
      } else if (rarityCategory === "EPIC") {
        // Stop if we see RARE (lower than EPIC in sort order)
        return categoriesInResponse.has("RARE");
      } else if (rarityCategory === "RARE") {
        // Stop if we see categories that are neither LEGENDARY nor EPIC nor RARE
        return Array.from(categoriesInResponse).some(
          (cat) => cat !== "LEGENDARY" && cat !== "EPIC" && cat !== "RARE"
        );
      }
      return false;
    })();

    if (shouldStopEarly) {
      return allTokenIds;
    }

    // If there's a next cursor, recursively fetch the next page
    if (nextCursor) {
      return fetchRarityTokensRecursive(
        collectionSlug,
        rarityCategory,
        nextCursor,
        allTokenIds
      );
    }

    // Return all accumulated token IDs
    return allTokenIds;
  } catch (error) {
    errorLog('Rarity', 'Error fetching rarity tokens recursively', error);
    throw error;
  }
}

/**
 * Fetch rarity tokens with Redis caching, distributed lock, and MongoDB persistence
 */
export async function fetchRarityTokens(
  collectionSlug: string,
  contractAddress: string,
  rarityCategory: "LEGENDARY" | "EPIC" | "RARE"
): Promise<RarityFetchResult> {
  const normalizedSlug = collectionSlug.toLowerCase();

  // Redis cache configuration
  const CACHE_TTL = 3600; // 1 hour
  const cacheKey = `rarity:${normalizedSlug}:${rarityCategory}`;
  const lockKey = `rarity:lock:${normalizedSlug}:${rarityCategory}`;

  try {
    // 1. Try Redis cache first (fastest)
    if (getRedis()) {
      const cachedData = await getRedis().get(cacheKey);
      if (cachedData) {
        const parsed = JSON.parse(cachedData);
        if (parsed.totalCount > 0) {
          return {
            ...parsed,
            cached: true,
          };
        }
      }
    }

    // 2. Try MongoDB cache (persistent)
    const mongoCache = await RarityCache.findOne({
      collectionSlug: normalizedSlug,
      rarityCategory,
    });

    if (mongoCache && mongoCache.totalCount > 0) {
      // Update Redis cache for next time
      if (getRedis()) {
        await getRedis().set(
          cacheKey,
          JSON.stringify({
            tokenIds: mongoCache.tokenIds,
            totalCount: mongoCache.totalCount,
            lastUpdated: mongoCache.lastUpdated,
          }),
          "EX",
          CACHE_TTL
        );
      }

      return {
        tokenIds: mongoCache.tokenIds,
        totalCount: mongoCache.totalCount,
        cached: true,
        lastUpdated: mongoCache.lastUpdated,
      };
    }

    // 3. Cache miss - fetch from OpenSea with distributed lock
    const result = await lockManager.withLock(
      lockKey,
      async () => {
        // Double-check cache after acquiring lock
        if (getRedis()) {
          const cachedData = await getRedis().get(cacheKey);
          if (cachedData) {
            const parsed = JSON.parse(cachedData);
            if (parsed.totalCount > 0) {
              return parsed;
            }
          }
        }

        // Fetch from OpenSea API
        const tokenIds = await fetchRarityTokensRecursive(
          normalizedSlug,
          rarityCategory
        );

        // Store in MongoDB
        const cacheEntry = await RarityCache.findOneAndUpdate(
          {
            collectionSlug: normalizedSlug,
            rarityCategory,
          },
          {
            collectionSlug: normalizedSlug,
            contractAddress: contractAddress || "",
            rarityCategory,
            tokenIds,
            totalCount: tokenIds.length,
            lastUpdated: new Date(),
          },
          { upsert: true, new: true }
        );

        return {
          tokenIds: cacheEntry.tokenIds,
          totalCount: cacheEntry.totalCount,
          lastUpdated: cacheEntry.lastUpdated,
        };
      },
      300000 // 5 minute lock timeout
    );

    // Cache in Redis
    if (getRedis() && result.totalCount > 0) {
      await getRedis().set(
        cacheKey,
        JSON.stringify({
          tokenIds: result.tokenIds,
          totalCount: result.totalCount,
          lastUpdated: result.lastUpdated,
        }),
        "EX",
        CACHE_TTL
      );
    }

    return {
      ...result,
      cached: false,
    };
  } catch (error) {
    errorLog('Rarity', 'Error fetching rarity tokens', error);
    throw error;
  }
}
