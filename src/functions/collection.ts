import { config } from "dotenv";
import stringify from 'json-stable-stringify';
import crypto from 'crypto';
import { axiosInstance, limiter } from "../init"; // Import Redis client
import { DistributedLockManager } from "../utils/lock";
import { API_KEY, redis } from "..";
import { CollectionItemsListQuery } from "../queries";
import { BASE_URL } from "../constants";

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

    try {
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
    } catch (error: any) {

      throw error
    }
  });

  if (!result) {
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
    const cachedFloor = await redis.get(cacheKey);
    if (cachedFloor) {
      return JSON.parse(cachedFloor);
    }
  } catch (error) {
    console.error('Error reading from cache:', error);
  }

  // If not in cache, try to acquire lock and fetch
  const result = await lockManager.withLock(lockKey, async () => {
    // Double check cache after acquiring lock
    try {
      const cachedFloor = await redis.get(cacheKey);
      if (cachedFloor) {
        return JSON.parse(cachedFloor);
      }
    } catch (error) {
      console.error('Error reading from cache after lock:', error);
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
      try {
        await redis.set(cacheKey, JSON.stringify(floor_price), 'EX', CACHE_TTL);
      } catch (error) {
        console.error('Error writing to cache:', error);
      }

      return floor_price;
    } catch (error: any) {
      throw error;
    }
  });

  if (!result) {
    // If we couldn't acquire the lock, wait briefly and try cache again
    await new Promise(resolve => setTimeout(resolve, 100));
    try {
      const cachedFloor = await redis.get(cacheKey);
      if (cachedFloor) {
        return JSON.parse(cachedFloor);
      }
    } catch (error) {
      console.error('Error reading from cache after lock failure:', error);
    }
    throw new Error('Failed to fetch OpenSea floor price');
  }

  return result;
}

export async function getOpenseaCollectionStats(collectionSlug: string) {
  try {
    const { data } = await limiter.schedule(() => axiosInstance.get(
      `${BASE_URL}/opensea/api/v2/collections/${collectionSlug}/stats`,
      {
        headers: { 'x-api-key': API_KEY }
      }
    ));
    if (!data?.total) {
      throw new Error(`No listings found for collection ${collectionSlug}`);
    }
    return data?.total?.floor_price;
  } catch (error: any) {
    throw error;
  }
}

export async function getCollectionEvents(
  collectionSlug: string,
  eventTypes: string[] = ['all', 'cancel', 'listing', 'offer', 'order', 'sale', 'transfer'],
  limit: number = 50
) {
  const cacheKey = `{collectionEvents:${collectionSlug}:${eventTypes.join(',')}}:${limit}`;
  const cachedData = await redis.get(cacheKey);
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
    await redis.setex(cacheKey, 10, JSON.stringify(data));
    return data;
  } catch (error) {
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
    const cachedData = await redis.get(cacheKey);
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
    await redis.setex(cacheKey, 24 * 60 * 60, JSON.stringify(uniqueTokenIds));
    return uniqueTokenIds;
  } catch (error) {
    console.error('Error fetching OpenSea collection items:', error);
    throw new Error(`Failed to fetch collection items`);
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