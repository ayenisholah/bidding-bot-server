import { limiter, axiosInstance } from "../init";
import { API_KEY } from "..";
import { BASE_URL } from "../constants";
import { CollectionItemsListQuery } from "../queries";
import { rarityCache } from "../utils/rarity-cache";
import { DistributedLockManager } from "../utils/lock";
import { errorLog } from '../utils/debug-file-logger';
import { apiDedup } from '../utils/api-dedup';

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
 * Fetch rarity tokens with Redis caching and distributed lock
 */
export async function fetchRarityTokens(
  collectionSlug: string,
  rarityCategory: "LEGENDARY" | "EPIC" | "RARE"
): Promise<RarityFetchResult> {
  const normalizedSlug = collectionSlug.toLowerCase();
  const lockKey = `rarity:lock:${normalizedSlug}:${rarityCategory}`;

  try {
    // 1. Try Redis cache first
    const cached = await rarityCache.getRarityTokens(normalizedSlug, rarityCategory);
    if (cached && cached.totalCount > 0) {
      return {
        tokenIds: cached.tokenIds.map(id => parseInt(id, 10)),
        totalCount: cached.totalCount,
        cached: true,
        lastUpdated: new Date(cached.lastUpdated),
      };
    }

    // 2. Cache miss - fetch from OpenSea with distributed lock
    const result = await lockManager.withLock(
      lockKey,
      async () => {
        // Double-check cache after acquiring lock
        const cachedAfterLock = await rarityCache.getRarityTokens(normalizedSlug, rarityCategory);
        if (cachedAfterLock && cachedAfterLock.totalCount > 0) {
          return {
            tokenIds: cachedAfterLock.tokenIds.map(id => parseInt(id, 10)),
            totalCount: cachedAfterLock.totalCount,
            lastUpdated: new Date(cachedAfterLock.lastUpdated),
          };
        }

        // Fetch from OpenSea API
        const tokenIds = await fetchRarityTokensRecursive(
          normalizedSlug,
          rarityCategory
        );

        const now = new Date();

        // Store in Redis cache (convert numbers to strings for storage)
        await rarityCache.setRarityTokens(
          normalizedSlug,
          rarityCategory,
          tokenIds.map(id => id.toString())
        );

        return {
          tokenIds,
          totalCount: tokenIds.length,
          lastUpdated: now,
        };
      },
      300000 // 5 minute lock timeout
    );

    if (!result) {
      throw new Error(`Failed to acquire lock for rarity fetch: ${lockKey}`);
    }

    return {
      tokenIds: result.tokenIds,
      totalCount: result.totalCount,
      lastUpdated: result.lastUpdated,
      cached: false,
    };
  } catch (error) {
    errorLog('Rarity', 'Error fetching rarity tokens', error);
    throw error;
  }
}
