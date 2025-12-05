import { axiosInstance, limiter } from '../init';
import { BASE_URL } from '../constants';
import { API_KEY } from '..';
import { tokenTraitCache, TokenTrait, TokenTraitData } from '../utils/token-trait-cache';
import { errorLog, debugLog } from '../utils/debug-file-logger';

// GraphQL queries
// Note: Using /opensea/graphql endpoint instead of /graphql for OpenSea API compatibility
const ItemAttributesQuery = `
  query ItemAttributesQuery($contractAddress: String!, $tokenId: String!) {
    contract(address: $contractAddress) {
      address
      name
      nft(tokenId: $tokenId) {
        tokenId
        tokenUrl
        tokenMetadata {
          name
          description
          attributes {
            traitType
            value
            displayType
          }
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
  displayType?: string;
}

interface GraphQLTokenResponse {
  data: {
    contract: {
      address: string;
      name: string;
      nft: {
        tokenId: string;
        tokenUrl: string;
        tokenMetadata: {
          name: string;
          description: string;
          attributes: GraphQLAttribute[];
        };
      };
    };
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

  // DEBUG: Log entry for cache miss
  debugLog('Metadata', `fetchTokenMetadata ENTRY | contract=${contractAddress.slice(0, 10)}... | tokenId=${tokenId}`);

  try {
    const response = await limiter.schedule(() =>
      axiosInstance.post<GraphQLTokenResponse>(
        `${BASE_URL}/opensea/graphql`,
        {
          query: ItemAttributesQuery,
          variables: {
            contractAddress,
            tokenId
          }
        },
        {
          headers: {
            'x-api-key': API_KEY,
            'Content-Type': 'application/json'
          }
        }
      )
    );

    if (!response.data?.data?.contract?.nft) {
      // DEBUG: Log why the API returned no NFT data
      const responseKeys = Object.keys(response.data || {}).join(',') || 'empty';
      const dataKeys = Object.keys(response.data?.data || {}).join(',') || 'empty';
      const contractKeys = Object.keys(response.data?.data?.contract || {}).join(',') || 'empty';
      debugLog('Metadata', `fetchTokenMetadata: API returned NULL NFT | contract=${contractAddress.slice(0, 10)}... | tokenId=${tokenId} | responseKeys=[${responseKeys}] | dataKeys=[${dataKeys}] | contractKeys=[${contractKeys}]`);
      return null;
    }

    const nftData = response.data.data.contract.nft;
    const attributes = nftData.tokenMetadata?.attributes || [];

    // Convert GraphQL attributes to our TokenTrait format
    const traits: TokenTrait[] = attributes.map(attr => ({
      trait_type: attr.traitType,
      value: attr.value,
      display_type: attr.displayType
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
    const response = await limiter.schedule(() =>
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
    const response = await limiter.schedule(() =>
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
  // DEBUG: Log entry with parameters
  debugLog('Metadata', `tokenHasTrait ENTRY | contract=${contractAddress.slice(0, 10)}... | tokenId=${tokenId} | trait=${traitType}:${traitValue}`);

  // Try cache first
  const hasTrait = await tokenTraitCache.tokenHasTrait(contractAddress, tokenId, traitType, traitValue);

  if (hasTrait) {
    debugLog('Metadata', `tokenHasTrait: Cache HIT | tokenId=${tokenId} | result=true`);
    return true;
  }

  // If not in cache, fetch metadata
  debugLog('Metadata', `tokenHasTrait: Cache MISS | tokenId=${tokenId} | fetching metadata...`);
  const metadata = await fetchTokenMetadata(contractAddress, tokenId);

  if (!metadata) {
    debugLog('Metadata', `tokenHasTrait: Metadata fetch FAILED | tokenId=${tokenId} | returning false`);
    return false;
  }

  // DEBUG: Log traits found and comparison
  const traitsList = metadata.traits.map(t => `${t.trait_type}:${t.value}`).slice(0, 5).join(', ');
  const hasMoreTraits = metadata.traits.length > 5 ? ` (+${metadata.traits.length - 5} more)` : '';
  debugLog('Metadata', `tokenHasTrait: Metadata fetched | tokenId=${tokenId} | traitsCount=${metadata.traits.length} | traits=[${traitsList}${hasMoreTraits}]`);

  const result = metadata.traits.some(trait =>
    trait.trait_type === traitType &&
    trait.value === traitValue
  );

  debugLog('Metadata', `tokenHasTrait EXIT | tokenId=${tokenId} | looking for ${traitType}:${traitValue} | result=${result}`);
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