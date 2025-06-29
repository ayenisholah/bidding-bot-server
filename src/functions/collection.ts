import { config } from "dotenv";
import stringify from 'json-stable-stringify';
import crypto from 'crypto';
import { axiosInstance, limiter } from "../init"; // Import Redis client
import { DistributedLockManager } from "../utils/lock";
import { API_KEY, redis } from "..";
import { CollectionItemsListQuery } from "../queries";
import { BASE_URL } from "../constants";
import { logger, DatabaseOperation } from "../utils/logger";

config()

const collectionCache: { [key: string]: any } = {};
const MAX_CACHE_ITEMS = 1000;

const lockManager = new DistributedLockManager({
  lockPrefix: '{collection}:fetch:',
  defaultTTLSeconds: 36000
});

export async function getCollectionDetails(slug: string, contractAddress: string) {

  if (collectionCache[slug]) {
    return collectionCache[slug];
  }
  const lockKey = `{${slug}}:collection`;
  const result = await lockManager.withLock(lockKey, async () => {
    if (collectionCache[slug]) {
      return collectionCache[slug];
    }
    const { data: collection } = await limiter.schedule(() => axiosInstance
      .get(
        `${BASE_URL}/opensea/api/v2/collections/${slug}`,
        {
          headers: {
            'x-api-key': API_KEY
          }
        }
      ));

    let creator_fees;
    let enforceCreatorFee = false;
    if (collection?.fees?.length > 1) {
      creator_fees = {
        [collection.fees[1].recipient]: collection.fees[1].fee * 100
      };
      enforceCreatorFee = collection.fees[1].required;
    } else {
      creator_fees = { null: 0 }
    }

    if (Object.keys(collectionCache).length >= MAX_CACHE_ITEMS) {
      delete collectionCache[Object.keys(collectionCache)[0]];
    }

    const result = {
      address: collection.editors[0],
      primary_asset_contracts_address: contractAddress,
      creator_fees: creator_fees,
      enforceCreatorFee: enforceCreatorFee,
      ...collection
    };

    collectionCache[slug] = result;
    return result;

  });

  if (!result) {
    await logger.logOpenseaCollectionError('Failed to acquire lock for collection details', {
      collection: slug,
      contractAddress,
      operation: 'get_collection_details',
      errorCode: 'LOCK_ACQUISITION_FAILED'
    });
    throw new Error('Failed to acquire lock for collection details');
  }

  return result;
}

export async function getOpenseaFloor(collectionSlug: string) {
  const CACHE_TTL = 30; // 30 seconds cache
  const cacheKey = `opensea:floor:${collectionSlug}`;
  const lockKey = `floor:${collectionSlug}`;

  // Try to get from cache first
  try {
    const cachedFloor = redis ? await redis.get(cacheKey) : null;
    if (cachedFloor) {
      return JSON.parse(cachedFloor);
    }
  } catch (error) {
    await logger.logDatabaseError('Error reading OpenSea floor price from cache', {
      databaseOperation: DatabaseOperation.READ,
      collection_name: 'redis_cache',
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
    const cachedFloor = redis ? await redis.get(cacheKey) : null;
    if (cachedFloor) {
      return JSON.parse(cachedFloor);
    }
    const { data } = await limiter.schedule(() => axiosInstance.get<ListingsResponse>(
      `${BASE_URL}/opensea/api/v2/listings/collection/${collectionSlug}/best?limit=1`,
      {
        headers: { 'x-api-key': API_KEY }
      }
    ));
    // Check if listings exist and has item
    const floor_price = Number(data?.listings[0]?.price?.current?.value) / 1e18 || 0;
    // Cache the result
    if (redis) {
      await redis.set(cacheKey, JSON.stringify(floor_price), 'EX', CACHE_TTL);
    }
    return floor_price;
  });

  if (!result) {
    // If we couldn't acquire the lock, wait briefly and try cache again
    await new Promise(resolve => setTimeout(resolve, 100));
    try {
      const cachedFloor = redis ? await redis.get(cacheKey) : null;
      if (cachedFloor) {
        return JSON.parse(cachedFloor);
      }
    } catch (error) {
      await logger.logDatabaseError('Error reading from cache after lock failure', {
        databaseOperation: DatabaseOperation.READ,
        collection_name: 'redis_cache',
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
  const cacheKey = `{collectionEvents:${collectionSlug}:${eventTypes.join(',')}}:${limit}`;
  const cachedData = redis ? await redis.get(cacheKey) : null;
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
    if (redis) {
      await redis.setex(cacheKey, 10, JSON.stringify(data));
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
  allTokenIds: number[] = [],
  cursor?: string
): Promise<number[]> {
  try {
    // Create a cache key based on the variables (excluding cursor for the initial request)
    const cacheKey = hashData(variables);
    const cachedData = redis ? await redis.get(cacheKey) : null;
    if (cachedData) {
      return JSON.parse(cachedData);
    }

    const queryVariables = {
      ...variables,
      cursor: cursor
    };
    const payload = {
      operationName: 'CollectionItemsListQuery',
      query: CollectionItemsListQuery,
      variables: queryVariables
    };

    const { data } = await limiter.schedule(() => axiosInstance.post<CollectionItemsResponse>(`${BASE_URL}/opensea/graphql`, payload, {
      headers: {
        "x-api-key": API_KEY
      },
    }));

    // Extract token IDs from the current page
    const tokenIds = data.data.collectionItems.items.map((item) => {
      const match = item.name.match(/#(\d+)$/);
      if (match && match[1]) {
        return parseInt(match[1], 10);
      }
      return parseInt(item.name.replace(/\D/g, '')) || 0;
    });
    // Add current page's token IDs to our accumulated list
    const updatedTokenIds = [...allTokenIds, ...tokenIds];
    // Get the next cursor
    const nextCursor = data.data.collectionItems.nextPageCursor;
    // If there's a next cursor, recursively fetch the next page
    if (nextCursor) {
      return fetchOpenSeaCollectionItems(variables, updatedTokenIds, nextCursor);
    }

    // No more pages, get unique token IDs
    const uniqueTokenIds = [...new Set(updatedTokenIds)];

    // Cache the final result (only for initial requests that completed pagination)
    if (redis) {
      await redis.setex(cacheKey, 24 * 60 * 60, JSON.stringify(uniqueTokenIds));
    }
    return uniqueTokenIds;
  } catch (error) {
    await logger.logOpenseaCollectionError('Error fetching OpenSea collection items', {
      collectionSlug: variables.collectionSlug,
      operation: 'fetch_collection_items',
      error: error as Error,
      errorCode: (error as any)?.response?.status,
      errorMessage: (error as any)?.response?.data || (error instanceof Error ? error.message : String(error))
    });
    console.error('Error fetching OpenSea collection items:', error);
    throw new Error(`Failed to fetch collection items`);
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
  const startTime = Date.now();
  const CACHE_TTL = 3600; // 1 hour cache
  const cacheKey = `opensea:traits:${slug}`;

  // Try to get from cache first if no cursor (first page)
  if (!cursor) {
    try {
      const cachedTraits = redis ? await redis.get(cacheKey) : null;
      if (cachedTraits) {
        console.log(`✅ Fetched traits from cache for ${slug} in ${Date.now() - startTime}ms`);
        return JSON.parse(cachedTraits);
      }
    } catch (error) {
      await logger.logDatabaseError('Error reading traits from cache', {
        databaseOperation: DatabaseOperation.READ,
        collection_name: 'redis_cache',
        collection: slug,
        cacheKey,
        error: error as Error,
        errorMessage: error instanceof Error ? error.message : String(error)
      });
      console.error('Error reading traits from cache:', error);
    }
  }

  try {
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
      const nextPageAttributes = await fetchOpenseaTraits(slug, limit, nextCursor);
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

    // Cache the complete result (only for initial requests)
    if (!cursor && cleanedAttributes.length > 0) {
      if (redis) {
        await redis.set(cacheKey, JSON.stringify(cleanedAttributes), 'EX', CACHE_TTL);
      }
    }

    return cleanedAttributes;
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

    return Array.from(traitFloorMap.values());
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
    throw new Error(`Failed to fetch trait floors for collection ${slug}: ${error instanceof Error ? error.message : 'Unknown error'}`);
  }
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