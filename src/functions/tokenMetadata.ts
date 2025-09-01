import { axiosInstance, limiter } from '../init';
import { BASE_URL } from '../constants';
import { API_KEY } from '..';
import { tokenTraitCache, TokenTrait, TokenTraitData } from '../utils/token-trait-cache';
import { logger, ErrorCategory } from '../utils/logger';

// Color constants for console output
const GREEN = '\x1b[32m';
const YELLOW = '\x1b[33m';
const RED = '\x1b[31m';
const BLUE = '\x1b[34m';
const CYAN = '\x1b[36m';
const RESET = '\x1b[0m';

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

  // console.log(CYAN + `[TOKEN METADATA] Fetching traits from OpenSea for ${contractAddress}:${tokenId}` + RESET);

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
      // console.log(YELLOW + `[TOKEN METADATA] No metadata found for ${contractAddress}:${tokenId}` + RESET);
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

    logger.logInfo(ErrorCategory.API, 'Token metadata fetched successfully', {
      contractAddress,
      tokenId,
      traitCount: traits.length,
      component: 'token_metadata',
      operation: 'fetch_token_metadata'
    });

    return tokenTraitData;

  } catch (error: any) {
    // Handle specific error cases
    if (error.response?.status === 400) {
      // console.log(YELLOW + `[TOKEN METADATA] Bad request for ${contractAddress}:${tokenId} - Token might not exist or be unsupported` + RESET);
      
      // Don't log full error for 400s to reduce noise
      logger.logApiError('Token metadata bad request', {
        contractAddress,
        tokenId,
        endpoint: `${BASE_URL}/opensea/graphql`,
        status: 400,
        errorMessage: error.response?.data?.message || 'Bad Request',
        component: 'token_metadata',
        operation: 'fetch_token_metadata'
      });
    } else {
      // Log full error for other status codes
      console.error(RED + `[TOKEN METADATA] Error fetching metadata for ${contractAddress}:${tokenId}` + RESET, error);
      
      logger.logApiError('Failed to fetch token metadata', {
        contractAddress,
        tokenId,
        endpoint: `${BASE_URL}/opensea/graphql`,
        method: 'POST',
        error: error as Error,
        errorMessage: error instanceof Error ? error.message : String(error),
        component: 'token_metadata',
        operation: 'fetch_token_metadata'
      });
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
  // console.log(CYAN + `[TOKEN METADATA] Fetching floor price for trait ${traitType}:${traitValue} in ${collectionSlug}` + RESET);

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
      
      logger.logInfo(ErrorCategory.API, 'Trait floor price fetched', {
        collectionSlug,
        traitType,
        traitValue,
        floorPrice: price,
        component: 'token_metadata',
        operation: 'get_trait_floor_price'
      });
      
      return price;
    }

    return null;

  } catch (error) {
    console.error(RED + `[TOKEN METADATA] Error fetching trait floor price` + RESET, error);
    
    logger.logApiError('Failed to fetch trait floor price', {
      collectionSlug,
      traitType,
      traitValue,
      endpoint: `${BASE_URL}/opensea/graphql`,
      method: 'POST',
      error: error as Error,
      errorMessage: error instanceof Error ? error.message : String(error),
      component: 'token_metadata',
      operation: 'get_trait_floor_price'
    });

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
  // console.log(CYAN + `[TOKEN METADATA] Fetching listings for trait ${traitType}:${traitValue} in ${collectionSlug}` + RESET);

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
    console.error(RED + `[TOKEN METADATA] Error fetching trait filtered listings` + RESET, error);
    
    logger.logApiError('Failed to fetch trait filtered listings', {
      collectionSlug,
      traitType,
      traitValue,
      limit,
      endpoint: `${BASE_URL}/opensea/graphql`,
      method: 'POST',
      error: error as Error,
      errorMessage: error instanceof Error ? error.message : String(error),
      component: 'token_metadata',
      operation: 'get_trait_filtered_listings'
    });

    return [];
  }
}

/**
 * Pre-fetch traits for multiple tokens
 */
export async function prefetchTokenTraits(
  tokens: Array<{ contractAddress: string; tokenId: string }>
): Promise<void> {
  // console.log(CYAN + `[TOKEN METADATA] Pre-fetching traits for ${tokens.length} tokens` + RESET);

  const fetchPromises = tokens.map(token =>
    fetchTokenMetadata(token.contractAddress, token.tokenId).catch(err => {
      console.error(RED + `[TOKEN METADATA] Failed to prefetch ${token.contractAddress}:${token.tokenId}` + RESET, err);
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
  // Try cache first
  const hasTrait = await tokenTraitCache.tokenHasTrait(contractAddress, tokenId, traitType, traitValue);
  
  if (hasTrait) {
    return true;
  }

  // If not in cache, fetch metadata
  const metadata = await fetchTokenMetadata(contractAddress, tokenId);
  
  if (!metadata) {
    return false;
  }

  return metadata.traits.some(trait => 
    trait.trait_type === traitType && 
    trait.value === traitValue
  );
}

/**
 * Get all trait floor prices for a collection
 */
export async function getCollectionTraitFloors(
  collectionSlug: string,
  traits: Array<{ trait_type: string; value: any }>
): Promise<CollectionTraitStats> {
  // console.log(CYAN + `[TOKEN METADATA] Fetching floor prices for ${traits.length} traits in ${collectionSlug}` + RESET);

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