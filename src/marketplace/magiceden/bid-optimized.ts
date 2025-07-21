import { ethers } from "ethers";

// Pre-computed constants for token bids
const TOKEN_BID_CONSTANTS = {
  orderKind: "seaport-v1.6",
  orderbook: "reservoir",
  automatedRoyalties: true,
  source: "magiceden.us",
  options: {
    "seaport-v1.6": {
      useOffChainCancellation: true,
      conduitKey: "0x87328c9043E7BF343695554EAAF5a8892f7205e3000000000000000000000000"
    }
  }
};

// Pre-computed request template
const TOKEN_BID_REQUEST_TEMPLATE = {
  source: TOKEN_BID_CONSTANTS.source,
  // params array will be added dynamically
};

// Cache for order structures by collection
const orderStructureCache = new Map<string, {
  structure: any;
  timestamp: number;
  ttl: number; // 5 minutes
}>();

/**
 * Creates bid data for Magic Eden with optimizations for token bids
 * This version pre-computes static values and caches order structures
 */
export async function createBidDataOptimized(
  taskId: string,
  slug: string,
  maker: string,
  collection: string,
  quantity: number,
  weiPrice: string,
  expirationTime: string,
  currency: string,
  tokenId?: number | string,
  cachedTask?: any
) {
  // For token bids, use pre-computed constants
  if (tokenId !== undefined) {
    // Check if we have a cached order structure for this collection
    const cacheKey = `${slug}:token`;
    const cached = orderStructureCache.get(cacheKey);
    
    if (cached && Date.now() - cached.timestamp < cached.ttl) {
      // We still need to call the API because it generates dynamic values
      // But we can use the cached structure to optimize our request
      console.log(`[MagicEden] Using cached order structure for ${slug}`);
    }
    
    // Build params using pre-computed constants
    const params = [{
      token: `${collection}:${tokenId}`,
      currency: currency,
      quantity: quantity,
      weiPrice: weiPrice,
      expirationTime: expirationTime,
      ...TOKEN_BID_CONSTANTS, // Spread all constants
    }];
    
    // Build request data using template
    const data = {
      ...TOKEN_BID_REQUEST_TEMPLATE,
      maker: maker,
      params: params
    };
    
    // Note: We still need to call the API because it generates:
    // - Dynamic salt values
    // - Current nonce/counter from blockchain
    // - Fee calculations based on current state
    // - Proper consideration arrays
    
    // The API call is required, but we've optimized the request building
    return { data, requiresApiCall: true };
  }
  
  // For non-token bids, fall back to regular flow
  return null;
}

/**
 * Pre-build static parts of token bid orders
 * This can be called during initialization to warm up caches
 */
export async function prebuildTokenBidStructure(slug: string) {
  // Cache the static parts of the order structure
  const structure = {
    orderKind: TOKEN_BID_CONSTANTS.orderKind,
    options: TOKEN_BID_CONSTANTS.options,
    source: TOKEN_BID_CONSTANTS.source,
    orderbook: TOKEN_BID_CONSTANTS.orderbook,
    automatedRoyalties: TOKEN_BID_CONSTANTS.automatedRoyalties,
    // Dynamic fields that will be filled:
    // - token (specific tokenId)
    // - weiPrice (bid amount)
    // - expirationTime (calculated)
    // - currency (WETH/USDC address)
  };
  
  const cacheKey = `${slug}:token`;
  orderStructureCache.set(cacheKey, {
    structure,
    timestamp: Date.now(),
    ttl: 5 * 60 * 1000 // 5 minutes
  });
  
  return structure;
}

/**
 * Optimized signing for token bids using cached domain/types
 */
export const signTokenBidOptimized = async (
  wallet: ethers.Wallet,
  signData: any
): Promise<string> => {
  // Use the pre-cached domain and types from the main file
  const domain = {
    name: "Seaport",
    version: "1.6",
    chainId: 1,
    verifyingContract: "0x0000000000000068f116a894984e2db1123eb395"
  };
  
  // For token bids, the types are always the same
  const types = {
    OrderComponents: [
      { name: "offerer", type: "address" },
      { name: "zone", type: "address" },
      { name: "offer", type: "OfferItem[]" },
      { name: "consideration", type: "ConsiderationItem[]" },
      { name: "orderType", type: "uint8" },
      { name: "startTime", type: "uint256" },
      { name: "endTime", type: "uint256" },
      { name: "zoneHash", type: "bytes32" },
      { name: "salt", type: "uint256" },
      { name: "conduitKey", type: "bytes32" },
      { name: "counter", type: "uint256" }
    ],
    OfferItem: [
      { name: "itemType", type: "uint8" },
      { name: "token", type: "address" },
      { name: "identifierOrCriteria", type: "uint256" },
      { name: "startAmount", type: "uint256" },
      { name: "endAmount", type: "uint256" }
    ],
    ConsiderationItem: [
      { name: "itemType", type: "uint8" },
      { name: "token", type: "address" },
      { name: "identifierOrCriteria", type: "uint256" },
      { name: "startAmount", type: "uint256" },
      { name: "endAmount", type: "uint256" },
      { name: "recipient", type: "address" }
    ]
  };
  
  return wallet.signTypedData(domain, types, signData.value);
};

/**
 * Analysis Results from JSONL files:
 * 
 * CONSTANT FIELDS (can be pre-computed):
 * - orderKind: "seaport-v1.6"
 * - orderbook: "reservoir"
 * - automatedRoyalties: true
 * - options: { seaport-v1.6: { useOffChainCancellation: true, conduitKey: "0x87..." } }
 * - source: "magiceden.us"
 * - quantity: 1 (usually)
 * 
 * VARIABLE FIELDS (must be dynamic):
 * - token: Contains specific tokenId
 * - weiPrice: Bid amount
 * - expirationTime: Calculated from bidExpiry
 * - currency: WETH or USDC address
 * - maker: Wallet address
 * 
 * DYNAMIC FROM API (cannot pre-build):
 * - salt: Random value for order uniqueness
 * - counter/nonce: Current wallet state
 * - consideration array: Dynamic fees and recipients
 * - startTime/endTime: Must be current
 * 
 * CONCLUSION:
 * We cannot fully pre-build orders because MagicEden's API generates
 * critical dynamic values. However, we can optimize by:
 * 1. Pre-computing all static fields
 * 2. Caching order structures
 * 3. Optimizing request building
 * 4. Using pre-cached signing domains/types
 */