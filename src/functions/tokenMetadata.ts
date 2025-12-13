import { axiosInstance, limiter } from '../init';
import { BASE_URL } from '../constants';
import { API_KEY } from '..';
import { tokenTraitCache, TokenTrait, TokenTraitData } from '../utils/token-trait-cache';
import { errorLog, debugLog, warnLog } from '../utils/debug-file-logger';
import { apiDedup } from '../utils/api-dedup';

// GraphQL queries
// Note: Using /opensea/graphql endpoint with itemByIdentifier query
const ItemAttributesQuery = `
  query ItemAttributesQuery($identifier: ItemIdentifierInput!) {
    itemByIdentifier(identifier: $identifier) {
      __typename
      ... on Item {
        id
        tokenId
        contractAddress
        attributes {
          traitType
          value
          __typename
        }
      }
    }
  }
`;

const CollectionItemsListQuery = `
  query CollectionItemsListQuery($collections: [CollectionSlug!]!, $count: Int, $cursor: String, $where: CollectionItemsListInput) {
    collections(first: 1, collections: $collections) {
      edges {
        node {
          id
          name
          slug
          items(first: $count, after: $cursor, input: $where) {
            edges {
              node {
                id
                tokenId
                contractAddress
                price {
                  amount
                  value
                  currency {
                    symbol
                    decimals
                  }
                }
                traits {
                  name
                  value
                  rarity {
                    score
                  }
                }
              }
            }
            pageInfo {
              hasNextPage
              endCursor
            }
          }
          stats {
            floor
            totalSupply
          }
        }
      }
    }
  }
`;

interface GraphQLAttribute {
  traitType: string;
  value: any;
  __typename?: string;
}

interface GraphQLItemByIdentifierResponse {
  data: {
    itemByIdentifier: {
      __typename: string;
      id: string;
      tokenId: string;
      contractAddress: string;
      attributes: GraphQLAttribute[];
    } | null;
  };
}

interface TraitFloorInfo {
  trait_type: string;
  value: any;
  floor: number;
  listedCount: number;
}

interface CollectionTraitStats {
  slug: string;
  traitFloors: TraitFloorInfo[];
}

/**
 * Fetch token metadata and traits from OpenSea GraphQL
 */
export async function fetchTokenMetadata(
  contractAddress: string,
  tokenId: string
): Promise<TokenTraitData | null> {
  // Check cache first
  const cached = await tokenTraitCache.getTokenTraits(contractAddress, tokenId);
  if (cached) {
    // console.log(BLUE + `[TOKEN METADATA] Using cached traits for ${contractAddress}:${tokenId}` + RESET);
    return cached;
  }

  try {
    const response = await apiDedup(
      `opensea-token-metadata-${contractAddress.toLowerCase()}-${tokenId}`,
      () => limiter.schedule(() =>
        axiosInstance.post<GraphQLItemByIdentifierResponse>(
          `${BASE_URL}/opensea/graphql`,
          {
            operationName: 'ItemAttributesQuery',
            query: ItemAttributesQuery,
            variables: {
              identifier: {
                chain: 'ethereum',
                contractAddress,
                tokenId
              }
            }
          },
          {
            headers: {
              'x-api-key': API_KEY,
              'Content-Type': 'application/json'
            }
          }
        )
      ),
      { category: 'Metadata' }
    );

    if (!response.data?.data?.itemByIdentifier) {
      // DEBUG: Log why the API returned no item data
      const responseKeys = Object.keys(response.data || {}).join(',') || 'empty';
      const dataKeys = Object.keys(response.data?.data || {}).join(',') || 'empty';
      debugLog('Metadata', `fetchTokenMetadata: API returned NULL item | contract=${contractAddress.slice(0, 10)}... | tokenId=${tokenId} | responseKeys=[${responseKeys}] | dataKeys=[${dataKeys}]`);
      return null;
    }

    const item = response.data.data.itemByIdentifier;
    const attributes = item.attributes || [];

    // Convert GraphQL attributes to our TokenTrait format
    const traits: TokenTrait[] = attributes.map(attr => ({
      trait_type: attr.traitType,
      value: attr.value
    }));

    const tokenTraitData: TokenTraitData = {
      contractAddress,
      tokenId,
      traits,
      lastUpdated: new Date()
    };

    // Cache the result
    await tokenTraitCache.setTokenTraits(tokenTraitData);

    // console.log(GREEN + `[TOKEN METADATA] Successfully fetched ${traits.length} traits for ${contractAddress}:${tokenId}` + RESET);

    return tokenTraitData;

  } catch (error: any) {
    // Handle specific error cases
    if (error.response?.status === 400) {
      // DEBUG: Log 400 errors with error message
      const errorMsg = error.response?.data?.message || error.response?.data?.error || error.response?.data?.errors?.[0] || 'unknown';
      debugLog('Metadata', `fetchTokenMetadata: HTTP 400 | contract=${contractAddress.slice(0, 10)}... | tokenId=${tokenId} | errorMsg=${JSON.stringify(errorMsg).slice(0, 200)}`);
    } else if (error.response?.status) {
      // Log other HTTP errors
      debugLog('Metadata', `fetchTokenMetadata: HTTP ${error.response.status} | contract=${contractAddress.slice(0, 10)}... | tokenId=${tokenId} | errorMsg=${error.message}`);
      errorLog('Metadata', `Failed to fetch token metadata | contractAddress=${contractAddress} | tokenId=${tokenId}`, error);
    } else {
      // Network/timeout errors
      debugLog('Metadata', `fetchTokenMetadata: Network Error | contract=${contractAddress.slice(0, 10)}... | tokenId=${tokenId} | errorCode=${error.code || 'unknown'} | errorMsg=${error.message}`);
      errorLog('Metadata', `Failed to fetch token metadata | contractAddress=${contractAddress} | tokenId=${tokenId}`, error);
    }

    return null;
  }
}

/**
 * Get floor price for a specific trait
 */
export async function getTraitFloorPrice(
  collectionSlug: string,
  traitType: string,
  traitValue: any
): Promise<number | null> {

  try {
    // Use the collection items query with trait filter
    const response = await apiDedup(
      `opensea-trait-floor-${collectionSlug}-${traitType}-${traitValue}`,
      () => limiter.schedule(() =>
        axiosInstance.post(
          `${BASE_URL}/opensea/graphql`,
          {
            query: CollectionItemsListQuery,
            variables: {
              collections: [collectionSlug],
              count: 1,
              where: {
                traits: [
                  {
                    name: traitType,
                    value: traitValue
                  }
                ],
                isListedForSale: true
              }
            }
          },
          {
            headers: {
              'x-api-key': API_KEY,
              'Content-Type': 'application/json'
            }
          }
        )
      ),
      { category: 'Metadata' }
    );

    const edges = response.data?.data?.collections?.edges;
    if (!edges || edges.length === 0) {
      // console.log(YELLOW + `[TOKEN METADATA] No collection data found for ${collectionSlug}` + RESET);
      return null;
    }

    const items = edges[0]?.node?.items?.edges;
    if (!items || items.length === 0) {
      // console.log(YELLOW + `[TOKEN METADATA] No items with trait ${traitType}:${traitValue} found` + RESET);
      return null;
    }

    // Get the floor price from the first (cheapest) listed item
    const floorItem = items[0].node;
    if (floorItem.price) {
      const price = parseFloat(floorItem.price.value) / Math.pow(10, floorItem.price.currency.decimals);
      
      // console.log(GREEN + `[TOKEN METADATA] Trait floor price: ${price} ETH` + RESET);

      return price;
    }

    return null;

  } catch (error) {
    errorLog('Metadata', `Failed to fetch trait floor price | slug=${collectionSlug} | trait=${traitType}:${traitValue}`, error);
    return null;
  }
}

/**
 * Get listings filtered by trait
 */
export async function getTraitFilteredListings(
  collectionSlug: string,
  traitType: string,
  traitValue: any,
  limit: number = 10
): Promise<Array<{
  tokenId: string;
  contractAddress: string;
  price: number;
  traits: TokenTrait[];
}>> {

  try {
    const response = await apiDedup(
      `opensea-trait-listings-${collectionSlug}-${traitType}-${traitValue}-${limit}`,
      () => limiter.schedule(() =>
        axiosInstance.post(
          `${BASE_URL}/opensea/graphql`,
          {
            query: CollectionItemsListQuery,
            variables: {
              collections: [collectionSlug],
              count: limit,
              where: {
                traits: [
                  {
                    name: traitType,
                    value: traitValue
                  }
                ],
                isListedForSale: true
              }
            }
          },
          {
            headers: {
              'x-api-key': API_KEY,
              'Content-Type': 'application/json'
            }
          }
        )
      ),
      { category: 'Metadata' }
    );

    const edges = response.data?.data?.collections?.edges;
    if (!edges || edges.length === 0) {
      return [];
    }

    const items = edges[0]?.node?.items?.edges || [];
    
    const listings = items.map((edge: any) => {
      const node = edge.node;
      const price = node.price ? 
        parseFloat(node.price.value) / Math.pow(10, node.price.currency.decimals) : 
        0;

      const traits: TokenTrait[] = (node.traits || []).map((trait: any) => ({
        trait_type: trait.name,
        value: trait.value,
        rarity: trait.rarity?.score
      }));

      return {
        tokenId: node.tokenId,
        contractAddress: node.contractAddress,
        price,
        traits
      };
    });

    // console.log(GREEN + `[TOKEN METADATA] Found ${listings.length} listings with trait ${traitType}:${traitValue}` + RESET);

    return listings;

  } catch (error) {
    errorLog('Metadata', `Failed to fetch trait filtered listings | slug=${collectionSlug} | trait=${traitType}:${traitValue}`, error);
    return [];
  }
}

/**
 * Pre-fetch traits for multiple tokens
 */
export async function prefetchTokenTraits(
  tokens: Array<{ contractAddress: string; tokenId: string }>
): Promise<void> {

  const fetchPromises = tokens.map(token =>
    fetchTokenMetadata(token.contractAddress, token.tokenId).catch(err => {
      errorLog('Metadata', `Failed to prefetch ${token.contractAddress}:${token.tokenId}`, err);
      return null;
    })
  );

  await Promise.allSettled(fetchPromises);

  // console.log(GREEN + `[TOKEN METADATA] Pre-fetch completed for ${tokens.length} tokens` + RESET);
}

/**
 * Check if a token has a specific trait (with caching)
 */
export async function tokenHasTrait(
  contractAddress: string,
  tokenId: string,
  traitType: string,
  traitValue: any
): Promise<boolean> {
  const startTime = Date.now();
  const contractShort = contractAddress.slice(0, 10);

  debugLog('Metadata', `tokenHasTrait: START | contract=${contractShort}... | tokenId=${tokenId} | traitType="${traitType}" | traitValue="${traitValue}"`);

  // Try cache first
  const cacheStartTime = Date.now();
  const cachedHasTrait = await tokenTraitCache.tokenHasTrait(contractAddress, tokenId, traitType, traitValue);
  const cacheElapsed = Date.now() - cacheStartTime;

  if (cachedHasTrait) {
    debugLog('Metadata', `tokenHasTrait: CACHE HIT | contract=${contractShort}... | tokenId=${tokenId} | traitType="${traitType}" | traitValue="${traitValue}" | result=true | cacheMs=${cacheElapsed} | totalMs=${Date.now() - startTime}`);
    return true;
  }

  debugLog('Metadata', `tokenHasTrait: CACHE MISS | contract=${contractShort}... | tokenId=${tokenId} | cacheMs=${cacheElapsed} | fetching metadata...`);

  // If not in cache, fetch metadata
  const fetchStartTime = Date.now();
  const metadata = await fetchTokenMetadata(contractAddress, tokenId);
  const fetchElapsed = Date.now() - fetchStartTime;

  if (!metadata) {
    warnLog('Metadata', `tokenHasTrait: FETCH FAILED | contract=${contractShort}... | tokenId=${tokenId} | traitType="${traitType}" | traitValue="${traitValue}" | fetchMs=${fetchElapsed} | totalMs=${Date.now() - startTime} | returning false`);
    return false;
  }

  debugLog('Metadata', `tokenHasTrait: FETCH SUCCESS | contract=${contractShort}... | tokenId=${tokenId} | traitsCount=${metadata.traits.length} | fetchMs=${fetchElapsed}`);

  // Check if trait exists
  const matchingTrait = metadata.traits.find(trait =>
    trait.trait_type === traitType &&
    trait.value === traitValue
  );

  const result = !!matchingTrait;

  if (result) {
    debugLog('Metadata', `tokenHasTrait: MATCH FOUND | contract=${contractShort}... | tokenId=${tokenId} | traitType="${traitType}" | traitValue="${traitValue}" | result=true | totalMs=${Date.now() - startTime}`);
  } else {
    // Log available values for this trait type to help debug mismatches
    const sameTypeTrait = metadata.traits.find(t => t.trait_type === traitType);
    const availableValue = sameTypeTrait ? sameTypeTrait.value : 'TRAIT_TYPE_NOT_FOUND';
    const allTraitTypes = metadata.traits.map(t => t.trait_type).join(', ');

    debugLog('Metadata', `tokenHasTrait: NO MATCH | contract=${contractShort}... | tokenId=${tokenId} | traitType="${traitType}" | traitValue="${traitValue}" | actualValue="${availableValue}" | allTraitTypes=[${allTraitTypes}] | result=false | totalMs=${Date.now() - startTime}`);
  }

  return result;
}

/**
 * Get all trait floor prices for a collection
 */
export async function getCollectionTraitFloors(
  collectionSlug: string,
  traits: Array<{ trait_type: string; value: any }>
): Promise<CollectionTraitStats> {

  const traitFloors: TraitFloorInfo[] = [];

  // Fetch floor prices for each trait in parallel
  const floorPromises = traits.map(async (trait) => {
    const floor = await getTraitFloorPrice(collectionSlug, trait.trait_type, trait.value);
    
    if (floor !== null) {
      traitFloors.push({
        trait_type: trait.trait_type,
        value: trait.value,
        floor,
        listedCount: 1 // Could be expanded to include actual count
      });
    }
  });

  await Promise.allSettled(floorPromises);

  // console.log(GREEN + `[TOKEN METADATA] Retrieved ${traitFloors.length} trait floor prices` + RESET);

  return {
    slug: collectionSlug,
    traitFloors
  };
}