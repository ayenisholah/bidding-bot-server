

import crypto from 'crypto';
import { ethers, Wallet } from "ethers";
import { BASE_URL, SEAPORT_CONTRACT_ADDRESS, SEAPORT_MIN_ABI } from "../../constants";
import { axiosInstance, limiter, isTaskBlacklisted } from "../../init";
import { activeTasks, API_KEY, decrementBidCount, trackBidRate, incrementOwnBidCounter, updateTaskCancellationPercentage } from "../..";
import { getCollectionTokenContractMap } from "../../functions/collection";
import { getCachedCollectionDetails } from "../../utils/collection-cache";
import { config } from "dotenv";
import axios from "axios";
import { v4 as uuidv4 } from 'uuid';
import { orderCache } from '../../utils/order-cache';
import { USDC, WETH } from "../../utils/currencies";
import { CollectionItemsListQuery } from "../../queries";
import { CollectionItemsQueryVariables, CollectionItemsResponse } from "../../functions";
import { fetchERC20Balance } from "../../utils/balance";
import { InsufficientBalanceError } from "../errors";
import { cancellationTracker } from "../../utils/cancellation-tracker";
import { getOpenseaMarketplaceFee } from "../../utils/marketplace-fee-cache";
import { errorLog, warnLog} from "../../utils/debug-file-logger";
import { apiDedup } from "../../utils/api-dedup";
import { MAX_PAIN_OVERRIDE_ADDRESS, MAX_PAIN_SLUG, MAX_PAIN_TOKEN_OVERRIDES } from "../../maxPainOverrides";
import { LRUCache } from 'lru-cache';
import { addIfNotExists } from '../../utils/lru-helpers';
import { safeTokenIdToString } from '../../utils/tokenId';
import {
  seaportCounterLRU,
  trackSeaportCounterHit,
  trackSeaportCounterMiss,
  balanceLRU,
  trackBalanceHit,
  trackBalanceMiss,
  openseaListingsLRU
} from './cache';
import { mainProvider as provider } from '../../utils/provider';

// Cache for deduplicating insufficient balance error logs (5 second TTL per wallet/collection)
// This prevents log flooding when multiple parallel jobs hit the same balance check
const insufficientBalanceLogCache = new LRUCache<string, boolean>({
  max: 1000,
  ttl: 60000 // 60 seconds (longer dedup window for error logs)
});

// LRU-only caches for performance-critical hot paths (< 1ms access vs 10-20ms Redis)
// These caches use memory-only storage for fastest possible access during counterbidding

// Trait floor cache (LRU ONLY - no Redis for speed)
// Right-sized to 10K max for 60-second hot counterbid window (10x safety buffer)
const traitFloorLRU = new LRUCache<string, number>({
  max: 10000,            // Hot cache for active counterbidding traits
  ttl: 5 * 60 * 1000,    // 5 minutes (reduce API calls during counterbidding)
  allowStale: true,       // Return stale on fetch failure
  updateAgeOnGet: false,
  updateAgeOnHas: false
});


// Contract resolution cache (LRU ONLY - no Redis for speed)
// Right-sized to 5K max (200 tasks × 50 tokens × 2x safety buffer → 5K)
const contractResolutionLRU = new LRUCache<string, string>({
  max: 5000,            // Typical token IDs across active tasks
  ttl: 24 * 60 * 60 * 1000, // 24 hours (contract mappings are immutable on blockchain)
  allowStale: true,
  updateAgeOnGet: false
});

// Multi-contract slug cache
// Right-sized to 500 max (<100 collections, 5x safety buffer)
const multiContractSlugLRU = new LRUCache<string, boolean>({
  max: 500,
  ttl: 24 * 60 * 60 * 1000, // 24 hours (multi-contract status is immutable)
  allowStale: true,
  updateAgeOnGet: false
});

// Contract map cache (full token→contract mapping for collections)
// Right-sized to 500 max (one entry per collection, <100 collections, 5x safety buffer)
const contractMapLRU = new LRUCache<string, Record<string, string>>({
  max: 500,
  ttl: 24 * 60 * 60 * 1000, // 24 hours (token mappings are immutable)
  allowStale: true,
  updateAgeOnGet: false
});

// Cache statistics tracking
let cacheStats = {
  traitFloor: { hits: 0, misses: 0 },
  contractResolution: { hits: 0, misses: 0 },
  lastReset: Date.now()
};

// Function to get cache statistics
export function getCacheStats() {
  const uptime = Date.now() - cacheStats.lastReset;
  const totalTraitFloor = cacheStats.traitFloor.hits + cacheStats.traitFloor.misses;
  const totalContractResolution = cacheStats.contractResolution.hits + cacheStats.contractResolution.misses;

  return {
    uptime: Math.floor(uptime / 1000), // seconds
    traitFloor: {
      ...cacheStats.traitFloor,
      hitRate: totalTraitFloor > 0 ? ((cacheStats.traitFloor.hits / totalTraitFloor) * 100).toFixed(1) + '%' : 'N/A',
      cacheSize: traitFloorLRU.size,
      cacheMax: traitFloorLRU.max
    },
    contractResolution: {
      ...cacheStats.contractResolution,
      hitRate: totalContractResolution > 0 ? ((cacheStats.contractResolution.hits / totalContractResolution) * 100).toFixed(1) + '%' : 'N/A',
      cacheSize: contractResolutionLRU.size,
      cacheMax: contractResolutionLRU.max
    },
    multiContractSlug: {
      cacheSize: multiContractSlugLRU.size,
      cacheMax: multiContractSlugLRU.max
    },
    contractMap: {
      cacheSize: contractMapLRU.size,
      cacheMax: contractMapLRU.max
    }
  };
}

// Function to reset cache statistics
export function resetCacheStats() {
  cacheStats = {
    traitFloor: { hits: 0, misses: 0 },
    contractResolution: { hits: 0, misses: 0 },
    lastReset: Date.now()
  };
}

// Performance timing utilitie
// Use the shared debug-logs directory (handled by debug-file-logger.ts)

// Helper function to extract meaningful error messages from OpenSea API responses
function extractOpenseaErrorMessage(error: any): string {
  if (error?.response?.data) {
    const data = error.response.data;

    // Handle array of errors
    if (data.errors && Array.isArray(data.errors) && data.errors.length > 0) {
      return data.errors.join(', ');
    }

    // Handle single error message
    if (data.error) {
      return data.error;
    }

    // Handle message field
    if (data.message) {
      return data.message;
    }

    // Handle detail field
    if (data.detail) {
      return data.detail;
    }

    // If it's a string, return it
    if (typeof data === 'string') {
      return data;
    }

    // Fallback to JSON stringify
    return JSON.stringify(data);
  }

  // Fallback to standard error message
  return error?.message || String(error);
}

// Using Mongoose for database operations

config()

// Pre-calculated constants for better performance
const OPENSEA_ITEM_ZONE = "0x000056f7000000ece9003ca63978907a00ffd100"
const OPENSEA_COLLECTION_ZONE = "0x004C00500000aD104D7DBd00e3ae0A5C00560C00"
const ZONE_HASH = "0x0000000000000000000000000000000000000000000000000000000000000000"
const CONDUIT_KEY = "0x0000007b02230091a7ed01230072f7006a004d60a8d4e71d599b8104250f0000"
const SEAPORT_1_6 = "0x0000000000000068f116a894984e2db1123eb395"
const OPENSEA_FEE_ADDRESS = "0x0000a26b00c1f0df003000390027140000faa719"
const SEAPORT_CONTRACT = new ethers.Contract(SEAPORT_CONTRACT_ADDRESS, SEAPORT_MIN_ABI, provider);

// Pre-calculated values for optimization
const DIVIDER = 10000n;
const BASIS_MAP: { [key: number]: number } = { 2: 1e16, 3: 1e15, 4: 1e14 };

// Ceiling division for BigInt - rounds UP to ensure fees are never under-calculated
// Standard BigInt division truncates toward zero, which can result in 0 fees for small bids
// Example: (39n * 250n) / 10000n = 0n (wrong), but ceilDiv gives 1n (correct)
function ceilDiv(numerator: bigint, denominator: bigint): bigint {
  return (numerator + denominator - 1n) / denominator;
}

// Wallet cache to avoid recreating wallet instances
// Uses hashed private key as cache key for security (never store plaintext keys as map keys)
// 60-second TTL to limit exposure window if memory is compromised
const walletCache = new LRUCache<string, Wallet>({
  max: 50,
  ttl: 60 * 1000 // 1 minute
});

function hashPrivateKey(privateKey: string): string {
  return crypto.createHash('sha256').update(privateKey).digest('hex');
}

function getWallet(privateKey: string): Wallet {
  const cacheKey = hashPrivateKey(privateKey);
  // Only log first 8 chars of hash for security (never log full key or hash)
  const cached = walletCache.get(cacheKey);
  if (cached) {
    return cached;
  }

  const wallet = new Wallet(privateKey, provider);
  walletCache.set(cacheKey, wallet);

  return wallet;
}

// Seaport counter getter - cached for 30s to reduce RPC calls (50-300ms savings)
// Invalidated immediately after successful bid placement
async function getSeaportCounter(walletAddress: string): Promise<bigint> {
  const normalizedAddress = walletAddress.toLowerCase();
  // Check cache first
  const cached = seaportCounterLRU.get(normalizedAddress);
  if (cached !== undefined) {
    trackSeaportCounterHit();
    return cached;
  }

  // Cache miss - fetch from blockchain
  trackSeaportCounterMiss();
  const counter = await SEAPORT_CONTRACT.getCounter(walletAddress);

  // Cache for 30 seconds
  seaportCounterLRU.set(normalizedAddress, counter);

  return counter;
}

// ============================================================================
// HTTP Request Abort Infrastructure
// Track active requests for abort capability during task stop
// ============================================================================
const activeRequests = new Map<string, AbortController[]>();

function addActiveRequest(taskId: string, controller: AbortController): void {
  if (!activeRequests.has(taskId)) {
    activeRequests.set(taskId, []);
  }
  activeRequests.get(taskId)!.push(controller);
}

function removeActiveRequest(taskId: string, controller: AbortController): void {
  const controllers = activeRequests.get(taskId);
  if (controllers) {
    const index = controllers.indexOf(controller);
    if (index > -1) controllers.splice(index, 1);
    if (controllers.length === 0) activeRequests.delete(taskId);
  }
}

/**
 * Cancel all active OpenSea HTTP requests for a task.
 * Called during stopTask Phase 0 for immediate abort.
 * @param taskId - The task ID to cancel requests for
 * @returns Number of requests cancelled
 */
export async function cancelOpenseaRequest(taskId: string): Promise<number> {
  const controllers = activeRequests.get(taskId);
  if (!controllers || controllers.length === 0) {
    return 0;
  }

  let cancelledCount = 0;
  for (let i = 0; i < controllers.length; i++) {
    try {
      controllers[i].abort();
      cancelledCount++;
    } catch (err) {
      errorLog('HttpAbort', `ABORT_ERROR | marketplace=opensea | taskId=${taskId} | requestIndex=${i} | error=${(err as Error).message}`, err);
    }
  }

  activeRequests.delete(taskId);

  return cancelledCount;
}

// ============================================================================
// Caching infrastructure for multi-contract collection resolution
// multiContractSlugCache: In-memory LRU (rarely changes, 500 collections max)
// contractMapCache: Redis-backed (expensive GraphQL queries, shared across instances)
const contractMapPromises = new Map<string, Promise<Record<string, string>>>();

const domain = {
  name: 'Seaport',
  version: '1.6',
  chainId: '1',
  verifyingContract: SEAPORT_1_6
};

const types = {
  OrderComponents: [
    {
      name: 'offerer',
      type: 'address'
    },
    {
      name: 'zone',
      type: 'address'
    },
    {
      name: 'offer',
      type: 'OfferItem[]'
    },
    {
      name: 'consideration',
      type: 'ConsiderationItem[]'
    },
    {
      name: 'orderType',
      type: 'uint8'
    },
    {
      name: 'startTime',
      type: 'uint256'
    },
    {
      name: 'endTime',
      type: 'uint256'
    },
    {
      name: 'zoneHash',
      type: 'bytes32'
    },
    {
      name: 'salt',
      type: 'uint256'
    },
    {
      name: 'conduitKey',
      type: 'bytes32'
    },
    {
      name: 'counter',
      type: 'uint256'
    }
  ],
  OfferItem: [
    {
      name: 'itemType',
      type: 'uint8'
    },
    {
      name: 'token',
      type: 'address'
    },
    {
      name: 'identifierOrCriteria',
      type: 'uint256'
    },
    {
      name: 'startAmount',
      type: 'uint256'
    },
    {
      name: 'endAmount',
      type: 'uint256'
    }
  ],
  ConsiderationItem: [
    {
      name: 'itemType',
      type: 'uint8'
    },
    {
      name: 'token',
      type: 'address'
    },
    {
      name: 'identifierOrCriteria',
      type: 'uint256'
    },
    {
      name: 'startAmount',
      type: 'uint256'
    },
    {
      name: 'endAmount',
      type: 'uint256'
    },
    {
      name: 'recipient',
      type: 'address'
    }
  ]
};

/**
 * Resolves the correct contract address for a given token ID in a collection.
 *
 * Priority:
 * 1. Max Pain override (if applicable)
 * 2. Multi-contract collection mapping (cached)
 * 3. Fallback to provided address
 *
 * @param collectionSlug - The OpenSea collection slug
 * @param tokenId - The token ID to resolve
 * @param fallbackAddress - Default contract address to use
 * @returns The resolved contract address
 */
export async function resolveContractAddress(
  collectionSlug: string | undefined,
  tokenId: string | number,
  fallbackAddress: string
): Promise<string> {
  // Early return if no slug provided
  if (!collectionSlug) {
    return fallbackAddress;
  }

  const normalizedSlug = collectionSlug.toLowerCase();
  const tokenKey = safeTokenIdToString(tokenId);
  const cacheKey = `${normalizedSlug}:${tokenKey}`;

  // STEP 1: Try LRU cache (< 1ms)
  const cached = contractResolutionLRU.get(cacheKey);
  if (cached !== undefined) {
    cacheStats.contractResolution.hits++;
    return cached;
  }

  cacheStats.contractResolution.misses++;
  // STEP 2: Check Max Pain Override (Priority: Highest)
  if (normalizedSlug === MAX_PAIN_SLUG && MAX_PAIN_TOKEN_OVERRIDES.has(tokenKey)) {
    const address = MAX_PAIN_OVERRIDE_ADDRESS;
    contractResolutionLRU.set(cacheKey, address);
    return address;
  }

  try {
    // STEP 3: Check if multi-contract collection (LRU cached)
    let isMultiContract = multiContractSlugLRU.get(normalizedSlug);
    if (isMultiContract === undefined) {
      const details = await getCachedCollectionDetails(normalizedSlug); // Uses LRU cache
      const contractAddresses = Array.isArray(details?.contracts)
        ? details.contracts
            .map((contract: { address?: string | null }) => contract?.address?.toLowerCase())
            .filter((address: string | null | undefined): address is string => Boolean(address))
        : [];
      isMultiContract = new Set(contractAddresses).size > 1;
      multiContractSlugLRU.set(normalizedSlug, isMultiContract);
    }

    // STEP 4: If not multi-contract, use fallback
    if (!isMultiContract) {
      contractResolutionLRU.set(cacheKey, fallbackAddress);
      return fallbackAddress;
    }

    // STEP 5: Get contract map (LRU cached, NO Redis for hot path)
    let contractMap = contractMapLRU.get(normalizedSlug);
    if (!contractMap) {
      // Use existing dedup promise logic but skip Redis
      let pendingPromise = contractMapPromises.get(normalizedSlug);

      if (!pendingPromise) {
        pendingPromise = getCollectionTokenContractMap(normalizedSlug)
          .then((map) => {
            contractMapLRU.set(normalizedSlug, map); // Store in LRU only
            contractMapPromises.delete(normalizedSlug);
            return map;
          })
          .catch((error) => {
            contractMapPromises.delete(normalizedSlug);
            errorLog('Performance', `[OS-ContractResolve] Failed to fetch contract map | slug=${normalizedSlug}`, error);
            throw error;
          });

        contractMapPromises.set(normalizedSlug, pendingPromise);
      }

      contractMap = await pendingPromise;
    }

    // STEP 6: Return mapped contract or fallback
    const resolvedAddress = contractMap?.[tokenKey] ?? fallbackAddress;
    contractResolutionLRU.set(cacheKey, resolvedAddress);
    return resolvedAddress;

  } catch (error) {
    // Log error for debugging
    const errorMessage = error instanceof Error ? error.message : String(error);
    errorLog('Performance', `[OS-ContractResolve] Contract resolution failed | slug=${normalizedSlug} | token=${tokenKey} | error=${errorMessage}`);

    // Fallback to default address
    contractResolutionLRU.set(cacheKey, fallbackAddress);
    return fallbackAddress;
  }
}

async function buildItemOffer(taskId: string, offerSpecification: ItemOfferSpecification, currency: string, slug?: string) {
  const {
    assetContractAddress,
    tokenId,
    quantity,
    priceWei,
    expirationSeconds,
    walletAddress
  } = offerSpecification
  const task = activeTasks.get(taskId)

  if (!task) return

  // Apply contract resolution with override logic
  const resolvedContract = await resolveContractAddress(
    slug,
    tokenId,
    assetContractAddress
  );

  const consideration = getItemConsideration(
    resolvedContract,  // Use resolved contract instead of original
    tokenId,
    quantity,
    walletAddress
  )

  const now = BigInt(Math.floor(Date.now() / 1000))
  const startTime = now.toString()
  const endTime = (now + expirationSeconds).toString()

  const offer = {
    offerer: walletAddress,
    offer: getOffer(priceWei, currency),
    consideration,
    startTime,
    endTime,
    orderType: 2,
    zone: OPENSEA_ITEM_ZONE,
    zoneHash: ZONE_HASH,
    salt: getSalt(),
    conduitKey: CONDUIT_KEY,
    totalOriginalConsiderationItems: consideration.length.toString(),
    counter: 0,
  }

  return offer
}
/**
 * Creates an offer on OpenSea.
 * @param walletAddress - The wallet address of the offerer.
 * @param privateKey - The private key of the offerer's wallet.
 * @param slug - The slug of the collection.
 * @param offerPrice - The price of the offer in wei.
 * @param creatorFees - The fees for the creators.
 * @param expiry - bid expiry in seconds.
 * @param openseaTraits - Optional traits for the offer.
 */
export async function bidOnOpensea(
  taskId: string,
  bidCount: string,
  wallet_address: string,
  private_key: string,
  slug: string,
  offer_price: number,
  creator_fees: IFee,
  expiry: number = 900,
  currency = WETH,
  contractAddress: string,
  opensea_traits?: string,
  asset?: { contractAddress: string, tokenId: string | number },
  source: 'scheduled' | 'counterbid' = 'scheduled'
) {
  // OpenSea requires minimum 10 minutes (600 seconds) expiration
  // Enforce minimum to prevent "Expiration time must be at least 10 minutes past submission time" errors
  const OPENSEA_MIN_EXPIRY = 900; // 15 minutes in seconds
  if (expiry < OPENSEA_MIN_EXPIRY) {
    expiry = OPENSEA_MIN_EXPIRY;
  }

  try {
    // Initialize timing tracker
    let offerPriceEthFinal: number;
    let currencySymbol: string;


    try {

      // Check if task is blacklisted (stopped)
      if (isTaskBlacklisted(taskId, 'opensea')) {
        const error = new Error(`Bid execution blocked - task ${taskId} is stopped for OpenSea`);
        throw error;
      }

      // Early validation - fail fast
      const task = activeTasks.get(taskId)
      if (!task || !task.running || !task.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("opensea")) {
        return
      }

      const openseaMarketplaceFeeBps = getOpenseaMarketplaceFee();


      // Pre-calculate values
      const decimal = currency.toLowerCase() === USDC.toLowerCase() ? 6 : 18
      currencySymbol = currency.toLowerCase() === USDC.toLowerCase() ? "USDC" : "WETH"
      const offerPriceEth = Number(offer_price) / Math.pow(10, decimal);

      let decimals;
      if (offerPriceEth >= 1) {
        decimals = 2;
      } else if (offerPriceEth >= 0.1) {
        decimals = 3;
      } else {
        decimals = 4;
      }

      const roundedEth = currency.toLowerCase() === USDC.toLowerCase() ? Number(offerPriceEth.toFixed(2)) : Number(offerPriceEth);
      const basis = BASIS_MAP[decimals] || 1e14;
      const roundedWei = decimal === 18 ? Math.round(roundedEth * Math.pow(10, decimal) / basis) * basis : Math.round(roundedEth * Math.pow(10, decimal))
      const offerPrice = BigInt(roundedWei.toString());
      offerPriceEthFinal = Number(roundedWei) / Math.pow(10, decimal);

      // Get wallet from cache
      const wallet = getWallet(private_key);

      // Check balance with LRU cache (60s TTL)
      // Cache key includes wallet + currency to track different token balances
      const balanceCacheKey = `${wallet_address.toLowerCase()}-${currency.toLowerCase()}`;
      let wethBalance = balanceLRU.get(balanceCacheKey);

      if (wethBalance !== undefined) {
        trackBalanceHit();
      } else {
        trackBalanceMiss();
        wethBalance = await fetchERC20Balance(wallet_address, currency);

        // Cache for 60 seconds
        balanceLRU.set(balanceCacheKey, wethBalance);
      }

      if (offerPriceEthFinal > wethBalance) {
        const error = new InsufficientBalanceError(
          'opensea',
          currencySymbol,
          offerPriceEthFinal,
          wethBalance,
          wallet_address,
          slug
        );
        // Only log once per wallet/collection combo within 5 seconds to prevent log flooding
        const dedupeKey = `opensea-${wallet_address}-${slug}`;
        if (addIfNotExists(insufficientBalanceLogCache, dedupeKey, true)) {
          errorLog('API', `[OpenSea] Insufficient balance for bid | taskId=${taskId} | wallet=${wallet_address.slice(0, 10)}... | collection=${slug} | bidAmount=${offerPriceEthFinal} ${currencySymbol} | available=${wethBalance}`, error);
        }
        throw error;
      }

      if (asset) {
        const offer = await buildItemOffer(task._id, {
          assetContractAddress: asset.contractAddress,
          tokenId: safeTokenIdToString(asset.tokenId),
          walletAddress: wallet_address,
          quantity: 1,
          expirationSeconds: BigInt(expiry),
          priceWei: BigInt(roundedWei)
        }, currency, slug)

        if (!offer) {
          return
        }

        const opensea_consideration = {
          itemType: 1,
          token: currency,
          identifierOrCriteria: "0",
          startAmount: ceilDiv(offerPrice * openseaMarketplaceFeeBps, DIVIDER).toString(),
          endAmount: ceilDiv(offerPrice * openseaMarketplaceFeeBps, DIVIDER).toString(),
          recipient: OPENSEA_FEE_ADDRESS
        };

        offer.consideration.push(opensea_consideration);
        offer.totalOriginalConsiderationItems = (Number(offer.totalOriginalConsiderationItems) + 1).toString();

        // FeeDebug logs removed - too noisy in production

        for (const address in creator_fees) {
          let fee: bigint | number = creator_fees[address];
          fee = BigInt(Math.round(fee).toString());

          // Skip null address or zero fees
          if (address === 'null' || address.toLowerCase() === '0x0000000000000000000000000000000000000000' || fee === 0n) {
            continue;
          }

          // Add all creator fees (OpenSea requires all fees marked as required=true)
          const feeAmount = ceilDiv(offerPrice * fee, DIVIDER).toString();

          const consideration_item = {
            itemType: 1,
            token: currency,
            identifierOrCriteria: "0",
            startAmount: feeAmount,
            endAmount: feeAmount,
            recipient: address
          };
          offer.consideration.push(consideration_item);
          offer.totalOriginalConsiderationItems = (Number(offer.totalOriginalConsiderationItems) + 1).toString();

        }

        const itemSignature = await signOffer(wallet, offer)

        if (!itemSignature) {
          return
        }

        // check if the tasks is still active before you make the offer
        const [taskIdFromBidCount] = bidCount.split(":")
        if (isTaskBlacklisted(taskIdFromBidCount, 'opensea') || !task || !task?.running || !task.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("OPENSEA".toLowerCase())) {
          return
        }

        const itemResponse = await postItemOffer(taskId, offer, itemSignature, slug)
        const itemOrderHash = itemResponse?.order?.order_hash

        const sanitizedExpiry = expiry > 900 ? expiry : 900
        if (offerPrice && itemOrderHash && sanitizedExpiry && slug && taskId && asset.tokenId) {
          const expiresAt = Date.now() + sanitizedExpiry * 1000;

          // Place new bid immediately (appends to array, no auto-cancellation)
          // Multiple orders per identifier are now supported
          await orderCache.set({
            orderHash: itemOrderHash,
            expiresAt: expiresAt,
            offerPrice: offerPrice.toString(),
            createdAt: Date.now(),
            identifier: safeTokenIdToString(asset.tokenId),
            marketplace: 'opensea',
            slug: slug,
            taskId: taskId
          });
          // Track bid rate ONLY for successfully placed bids
          trackBidRate('opensea', taskId);

          // Invalidate caches after successful bid placement
          seaportCounterLRU.delete(wallet_address.toLowerCase());
          balanceLRU.delete(`${wallet_address.toLowerCase()}-${currency.toLowerCase()}`);
        }
      }

      else if (opensea_traits && typeof opensea_traits !== "undefined") {
        const offerPrice = BigInt(roundedWei.toString());

        const payload: IPayload = {
          criteria: {
            collection: {
              slug: slug
            }
          },
          protocol_data: {
            parameters: {
              offerer: wallet_address,
              offer: [
                {
                  itemType: 1,
                  token: currency,
                  identifierOrCriteria: 0,
                  startAmount: (Date.now() / 1000).toString(),
                  endAmount: (Date.now() / 1000 + 100000).toString()
                }
              ],
              consideration: [],
              startTime: '1666480886',
              endTime: '1666680886',
              orderType: 2,
              zone: OPENSEA_COLLECTION_ZONE,
              zoneHash: ZONE_HASH,
              conduitKey: CONDUIT_KEY,
              totalOriginalConsiderationItems: 2,
              counter: '0'
            },
            signature: '0x0'
          },
          protocol_address: SEAPORT_1_6
        }
        // reset consideration list and count
        payload.protocol_data.parameters.consideration = [];
        payload.protocol_data.parameters.totalOriginalConsiderationItems = 2;

        // set correct slug for collection
        payload.criteria.collection.slug = slug;

        if (opensea_traits && typeof opensea_traits !== "undefined") {
          const parsedTraits = JSON.parse(opensea_traits);
          payload.criteria.trait = parsedTraits;
        } else {
          delete payload.criteria.trait
        }

        const buildPayload = {
          quantity: 1,
          criteria: payload.criteria,
          offerer: wallet_address,
          protocol_address: SEAPORT_1_6
        };

        // Parallel operations for collection offers
        const [data, counter] = await Promise.all([
          buildOffer(taskId, buildPayload),
          getSeaportCounter(wallet_address)
        ]);

        if (!data || !data.partialParameters) {
          return
        }

        payload.protocol_data.parameters.startTime = BigInt(Math.floor(Date.now() / 1000)).toString();
        payload.protocol_data.parameters.endTime = BigInt(Math.floor(Date.now() / 1000 + expiry)).toString();
        payload.protocol_data.parameters.offerer = wallet_address;
        payload.protocol_data.parameters.offer[0].startAmount = offerPrice.toString();
        payload.protocol_data.parameters.offer[0].token = currency;
        payload.protocol_data.parameters.offer[0].endAmount = offerPrice.toString();
        payload.protocol_data.parameters.consideration.push(data.partialParameters.consideration[0]);

        const opensea_consideration = {
          itemType: 1,
          token: currency,
          identifierOrCriteria: 0,
          startAmount: ceilDiv(offerPrice * openseaMarketplaceFeeBps, DIVIDER).toString(),
          endAmount: ceilDiv(offerPrice * openseaMarketplaceFeeBps, DIVIDER).toString(),
          recipient: OPENSEA_FEE_ADDRESS
        };
        payload.protocol_data.parameters.consideration.push(opensea_consideration);

        for (const address in creator_fees) {
          let fee: bigint | number = creator_fees[address];
          fee = BigInt(Math.round(fee).toString());

          // Skip null address or zero fees
          if (address === 'null' || address.toLowerCase() === '0x0000000000000000000000000000000000000000' || fee === 0n) {
            continue;
          }

          // Add all creator fees (OpenSea requires all fees marked as required=true)
          const consideration_item = {
            itemType: 1,
            token: currency,
            identifierOrCriteria: 0,
            startAmount: ceilDiv(offerPrice * fee, DIVIDER).toString(),
            endAmount: ceilDiv(offerPrice * fee, DIVIDER).toString(),
            recipient: address
          };

          payload.protocol_data.parameters.consideration.push(consideration_item);
          payload.protocol_data.parameters.totalOriginalConsiderationItems += 1;
        }


        payload.protocol_data.parameters.zone = data.partialParameters.zone;
        payload.protocol_data.parameters.zoneHash = data.partialParameters.zoneHash;
        payload.protocol_data.parameters.salt = getSalt();
        payload.protocol_data.parameters.counter = counter.toString();

        const signObj = await wallet.signTypedData(
          domain,
          types,
          payload.protocol_data.parameters
        );
        payload.protocol_data.signature = signObj;
        payload.protocol_address = SEAPORT_1_6;

        if (isTaskBlacklisted(taskId, 'opensea') || !task || !task.running || !task.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("opensea")) {
          return
        }

        await submitOfferToOpensea(decimal, currencySymbol, bidCount, offerPrice.toString(), payload, expiry)
      }

      else {
        const offerPrice = BigInt(roundedWei.toString());
        const payload: IPayload = {
          criteria: {
            collection: {
              slug: slug
            }
          },
          protocol_data: {
            parameters: {
              offerer: wallet_address,
              offer: [
                {
                  itemType: 1,
                  token: currency,
                  identifierOrCriteria: 0,
                  startAmount: (Date.now() / 1000).toString(),
                  endAmount: (Date.now() / 1000 + 100000).toString()
                }
              ],
              consideration: [],
              startTime: '1666480886',
              endTime: '1666680886',
              orderType: 2,
              zone: OPENSEA_COLLECTION_ZONE,
              zoneHash: ZONE_HASH,
              conduitKey: CONDUIT_KEY,
              totalOriginalConsiderationItems: 2,
              counter: '0'
            },
            signature: '0x0'
          },
          protocol_address: SEAPORT_1_6
        }
        // reset consideration list and count
        payload.protocol_data.parameters.consideration = [];
        payload.protocol_data.parameters.totalOriginalConsiderationItems = 2;

        // set correct slug for collection
        payload.criteria.collection.slug = slug;

        delete payload.criteria.trait


        const buildPayload = {
          quantity: 1,
          criteria: payload.criteria,
          offerer: wallet_address,
          protocol_address: SEAPORT_1_6
        };

        // Use buildOffer API to get correct itemType for both ERC-721 and ERC-1155 collections
        const [data, counter] = await Promise.all([
          buildOffer(taskId, buildPayload),
          getSeaportCounter(wallet_address)
        ]);


        if (!data || !data.partialParameters) {
          return
        }

        payload.protocol_data.parameters.startTime = BigInt(Math.floor(Date.now() / 1000)).toString();
        payload.protocol_data.parameters.endTime = BigInt(Math.floor(Date.now() / 1000 + expiry)).toString();
        payload.protocol_data.parameters.offerer = wallet_address;
        payload.protocol_data.parameters.offer[0].startAmount = offerPrice.toString();
        payload.protocol_data.parameters.offer[0].token = currency;
        payload.protocol_data.parameters.offer[0].endAmount = offerPrice.toString();
        payload.protocol_data.parameters.consideration.push(data.partialParameters.consideration[0]);

        const opensea_consideration = {
          itemType: 1,
          token: currency,
          identifierOrCriteria: 0,
          startAmount: ceilDiv(offerPrice * openseaMarketplaceFeeBps, DIVIDER).toString(),
          endAmount: ceilDiv(offerPrice * openseaMarketplaceFeeBps, DIVIDER).toString(),
          recipient: OPENSEA_FEE_ADDRESS
        };
        payload.protocol_data.parameters.consideration.push(opensea_consideration);

        for (const address in creator_fees) {
          let fee: bigint | number = creator_fees[address];
          fee = BigInt(Math.round(fee).toString());

          // Skip null address or zero fees
          if (address === 'null' || address.toLowerCase() === '0x0000000000000000000000000000000000000000' || fee === 0n) {
            continue;
          }

          // Add all creator fees (OpenSea requires all fees marked as required=true)
          const consideration_item = {
            itemType: 1,
            token: currency,
            identifierOrCriteria: 0,
            startAmount: ceilDiv(offerPrice * fee, DIVIDER).toString(),
            endAmount: ceilDiv(offerPrice * fee, DIVIDER).toString(),
            recipient: address
          };

          payload.protocol_data.parameters.consideration.push(consideration_item);
          payload.protocol_data.parameters.totalOriginalConsiderationItems += 1;
        }

        payload.protocol_data.parameters.zone = data.partialParameters.zone;
        payload.protocol_data.parameters.zoneHash = data.partialParameters.zoneHash;
        payload.protocol_data.parameters.salt = getSalt();
        payload.protocol_data.parameters.counter = counter.toString();

        const signObj = await wallet.signTypedData(
          domain,
          types,
          payload.protocol_data.parameters
        );
        payload.protocol_data.signature = signObj;
        payload.protocol_address = SEAPORT_1_6;

        if (isTaskBlacklisted(taskId, 'opensea') || !task || !task.running || !task.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("opensea")) {
          return
        }
        await submitOfferToOpensea(decimal, currencySymbol, bidCount, offerPrice.toString(), payload, expiry)
      }

    } catch (error) {
      // Note: Removed duplicate "inner error" log - error is already logged by outer catch
      throw error;
    }
  } catch (error) {
    errorLog('OpenSea', `bidOnOpensea failed | taskId=${taskId} | slug=${slug}`, error);
    throw error;
  }
}

/**
 * Posts an offer to OpenSea.
 * @param payload - The payload of the offer.
 */
async function submitOfferToOpensea(decimal: number, currencySymbol: string, bidCount: string, offerPrice: string, payload: IPayload, expiry = 900) {
  const [taskId] = bidCount.split(":")

  // Early blacklist check before entering limiter queue
  if (isTaskBlacklisted(taskId, 'opensea')) {
    return;
  }
  const task = activeTasks.get(taskId)
  if (!task || !task?.running || !task.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("OPENSEA".toLowerCase())) {
    return
  }

  // Create AbortController for HTTP request abort capability
  const controller = new AbortController();
  addActiveRequest(taskId, controller);

  let offer: OpenseaOffer;
  try {
    const response = await limiter.schedule(async () => {
      // Re-check blacklist AFTER waiting in limiter queue - this is the critical fix
      if (isTaskBlacklisted(taskId, 'opensea')) {
        throw new Error('Task stopped while waiting in limiter queue');
      }

      return axiosInstance.request<OpenseaOffer>({
        method: 'POST',
        url: `${BASE_URL}/opensea/api/v2/offers`,
        headers: {
          'content-type': 'application/json',
          'x-api-key': API_KEY,
          "Cache-Control": "no-store", // ⛔ Prevents caching
          "Pragma": "no-cache", // ⛔ Prevents old cache policies from affecting response
          "Expires": "0", // ⛔ Forces fresh reques
        },
        data: JSON.stringify(payload),
        signal: controller.signal,
        "axios-retry": { retries: 3 }
      });
    });
    offer = response.data;


  } catch (error) {
    // Handle abort gracefully - don't log as error
    if (axios.isCancel(error)) {
      return;
    }
    errorLog('API', `[OpenSea] Failed to submit offer | taskId=${taskId} | collection=${payload.criteria.collection.slug} | bidAmount=${Number(offerPrice) / Math.pow(10, decimal)} ${currencySymbol} | status=${(error as any)?.response?.status} | ${extractOpenseaErrorMessage(error)}`, error);
    throw error;
  } finally {
    removeActiveRequest(taskId, controller);
  }

  const order_hash = offer.order_hash
  const identifier: string = offer?.criteria?.trait?.type
    && offer?.criteria?.trait?.value
    ? `${offer.criteria.trait?.type}:${offer.criteria.trait?.value}`
    : "collection"

  const slug = offer?.criteria?.collection?.slug
  const sanitizedExpiry = expiry > 900 ? expiry : 900

  if (offerPrice && order_hash && sanitizedExpiry && slug && taskId && identifier) {
    const expiresAt = Date.now() + sanitizedExpiry * 1000;
    const createdAt = Date.now();

    // Place new bid immediately (appends to array, no auto-cancellation)
    // Multiple orders per identifier are now supported
    await orderCache.set({
      orderHash: order_hash,
      expiresAt: expiresAt,
      offerPrice: offerPrice.toString(),
      createdAt: createdAt,
      identifier: identifier,
      marketplace: 'opensea',
      slug: slug,
      taskId: taskId
    });
    // Track bid rate ONLY for successfully placed bids
    trackBidRate('opensea', taskId);

    // Invalidate caches after successful bid placement
    const wallet_address = payload.protocol_data.parameters.offerer;
    const currency = payload.protocol_data.parameters.offer[0]?.token || '';
    seaportCounterLRU.delete(wallet_address.toLowerCase());
    balanceLRU.delete(`${wallet_address.toLowerCase()}-${currency.toLowerCase()}`);
  }
}

/**
 * Builds an offer on OpenSea with request deduplication.
 * @param buildPayload - The payload to build the offer.
 */
async function buildOffer(taskId: string, buildPayload: any) {
  // Early blacklist check BEFORE entering limiter queue (Fix 3: Bids after stopTask)
  if (isTaskBlacklisted(taskId, 'opensea')) {
    return null;
  }

  const controller = new AbortController();
  addActiveRequest(taskId, controller);

  try{
    const { data } = await limiter.schedule(async () => {
      // Re-check blacklist AFTER waiting in limiter queue
      if (isTaskBlacklisted(taskId, 'opensea')) {
        throw new Error('Task stopped while waiting in limiter queue');
      }

      return axiosInstance.request<PartialParameters>({
        method: 'POST',
        url: `${BASE_URL}/opensea/api/v2/offers/build`,
        headers: {
          'content-type': 'application/json',
          'x-api-key': API_KEY,
        },
        data: JSON.stringify(buildPayload),
        signal: controller.signal
      });
    });
    return data;
  } catch (error: any) {
    if (axios.isCancel(error)) {
      return null;
    }

    errorLog('API', `[OpenSea] Failed to build offer | taskId=${taskId} | status=${error?.response?.status} | ${extractOpenseaErrorMessage(error)}`, error);
    throw error;
  } finally {
    removeActiveRequest(taskId, controller);
  }
}

// Internal function to cancel order without tracking completion (used for synchronized cancellation)
async function cancelOrderInternal(orderHash: string, protocolAddress: string, privateKey: string, taskId: string, updateTracking: boolean = true) {
  try {
    if (!orderHash || !protocolAddress || !privateKey) return

    const offererSignature = await signCancelOrder(taskId, orderHash, protocolAddress, privateKey);
    if (!offererSignature) return;
    const url = `${BASE_URL}/opensea/api/v2/orders/chain/ethereum/protocol/${protocolAddress}/${orderHash}/cancel`;
    const headers = {
      'content-type': 'application/json',
      'x-api-key': API_KEY
    };

    const body = { offerer_signature: offererSignature };

    // Check if limiter is initialized
    if (!limiter) {
      const error = new Error('Limiter not initialized');
      errorLog('OpenSea', `Limiter not initialized - cannot process cancelOrder request | taskId=${taskId} | orderHash=${orderHash}`, error);
      throw error;
    }

    const response = await limiter.schedule(() => axiosInstance.post(url, body, { headers }))

    // Only update tracking if requested (for synchronized cancellation control)
    // When updateTracking=false, the caller (bulkCancelOpenseaBid) handles all tracking
    if (updateTracking) {
      // Decrement bidStats for UI display
      decrementBidCount('opensea', taskId)
      // Track cancellation completion
      cancellationTracker.recordCompletion(taskId, 1, 'opensea');

      // Update cancellation percentage using centralized function
      await updateTaskCancellationPercentage(taskId, undefined, 'opensea');
    }

    return response.data;
  } catch (error) {
    if (error) {
      errorLog('API', `[OpenSea] Failed to cancel order | taskId=${taskId} | orderHash=${orderHash}`, error);
    }
    throw error;
  }
}

// Public function to cancel order with tracking (original behavior)
export async function cancelOrder(orderHash: string, protocolAddress: string, privateKey: string, taskId: string) {
  return await cancelOrderInternal(orderHash, protocolAddress, privateKey, taskId, true);
}

// Public function to cancel order without tracking (for synchronized cancellation)
export async function cancelOrderWithoutTracking(orderHash: string, protocolAddress: string, privateKey: string, taskId: string) {
  return await cancelOrderInternal(orderHash, protocolAddress, privateKey, taskId, false);
}

async function signCancelOrder(taskId: string, orderHash: string, protocolAddress: string, privateKey: string) {
  if (!orderHash) return

  const wallet = getWallet(privateKey);
  const domain = {
    name: 'Seaport',
    version: '1.6',
    chainId: '1',
    verifyingContract: protocolAddress
  };
  const types = {
    OrderHash: [
      { name: 'orderHash', type: 'bytes32' }
    ]
  };

  const value = {
    orderHash: orderHash
  };
  const signature = await wallet.signTypedData(domain, types, value);
  return signature;
}

async function signOffer(wallet: ethers.Wallet, offer: Record<string, unknown>) {
  return await wallet.signTypedData(domain, types, offer)
}

const getOffer = (priceWei: bigint, currency: string) => {
  return [
    {
      itemType: 1, // ERC 20
      token: currency,
      identifierOrCriteria: 0,
      startAmount: priceWei.toString(),
      endAmount: priceWei.toString(),
    },
  ]
}

/**
 * Fetches existing offers for a token from a specific maker (wallet)
 * Used to sync local state when OpenSea returns "Order already exists"
 */
async function fetchMakerOfferForToken(
  contractAddress: string,
  tokenId: string,
  makerAddress: string
): Promise<{
  orderHash: string;
  expirationTime: number;
  currentPrice: string;
  slug: string;
  tokenId: string;
} | null> {
  // ENTRY log

  const url = `${BASE_URL}/opensea/api/v2/orders/ethereum/seaport/offers`;
  const params = {
    asset_contract_address: contractAddress,
    token_ids: tokenId,
    maker: makerAddress
  };

  try {
    const response = await apiDedup(
      `opensea-maker-offer-${contractAddress.toLowerCase()}-${tokenId}-${makerAddress.toLowerCase()}`,
      () => limiter.schedule(() =>
        axiosInstance.get(url, {
          params,
          headers: {
            'accept': '*/*',
            'x-api-key': API_KEY
          }
        })
      ),
      { category: 'OpenSea' }
    );

    // Find active order from this maker
    const orders = response.data?.orders || [];
    const now = Date.now();

    // API RESPONSE log



    const activeOrder = orders.find((order: any) =>
      order.expiration_time * 1000 > now && !order.cancelled && !order.finalized
    );

    if (!activeOrder) {
      // NO ACTIVE ORDER log
      return null;
    }

    // Extract slug from taker_asset_bundle
    const takerAsset = activeOrder.taker_asset_bundle?.assets?.[0];
    const slug = takerAsset?.collection?.collection || '';

    // ACTIVE ORDER FOUND log

    return {
      orderHash: activeOrder.order_hash,
      expirationTime: activeOrder.expiration_time,  // Unix timestamp in seconds
      currentPrice: activeOrder.current_price,       // Wei string
      slug: slug,
      tokenId: takerAsset?.token_id || tokenId
    };
  } catch (fetchError) {
    errorLog('API', `[OpenSea] fetchMakerOfferForToken FAILED | token=${tokenId} | contract=${contractAddress}`, fetchError);
    throw fetchError;
  }
}

async function postItemOffer(taskId: string, offer: unknown, signature: string, slug?: string) {
  // Early blacklist check BEFORE entering limiter queue (Fix 3: Bids after stopTask)
  if (isTaskBlacklisted(taskId, 'opensea')) {
    return null;
  }

  const task = activeTasks.get(taskId);
  const collectionSlug = slug || task?.contract?.slug?.toLowerCase();

  const parameters = offer as any;

  // Create AbortController for HTTP request abort capability
  const controller = new AbortController();
  addActiveRequest(taskId, controller);

  // Fix contract addresses in consideration items (parallelized for performance)
  if (collectionSlug && parameters?.consideration) {
    await Promise.all(
      parameters.consideration.map(async (item: any) => {
        // itemType 2 = ERC721 token
        if (Number(item.itemType) === 2 && item?.token) {
          const targetTokenId = item.identifierOrCriteria;

          // Apply override logic
          item.token = await resolveContractAddress(
            collectionSlug,
            targetTokenId,
            item.token
          );
        }
      })
    );
  }

  const payload = {
    parameters,
    signature,
    protocol_address: SEAPORT_CONTRACT_ADDRESS,
  }

  // Log request payload for debugging

  try {
    const { data } = await limiter.schedule(async () => {
      // Re-check blacklist AFTER waiting in limiter queue - critical fix for in-flight token bids
      if (isTaskBlacklisted(taskId, 'opensea')) {
        throw new Error('Task stopped while waiting in limiter queue');
      }

      return axiosInstance.post(`${BASE_URL}/opensea/api/v2/orders/ethereum/seaport/offers`, payload, {
        headers: {
          'content-type': 'application/json',
          'x-api-key': API_KEY,
          "Cache-Control": "no-store",
          "Pragma": "no-cache",
          "Expires": "0",
        },
        signal: controller.signal,
        "axios-retry": { retries: 3 },
      });
    })

    return data
  } catch (error: any) {
    if (axios.isCancel(error)) {
      return null;
    }

    // Handle "Order already exists" by syncing with OpenSea
    const errorMessage = extractOpenseaErrorMessage(error);
    if (error?.response?.status === 400 && errorMessage.includes('Order already exists')) {
      // ERROR DETECTED log

      try {
        // Extract details from the offer payload
        const consideration = parameters?.consideration?.[0];
        const contractAddress = consideration?.token;
        const tokenId = consideration?.identifierOrCriteria;
        const makerAddress = parameters?.offerer;

        // PAYLOAD EXTRACTION log

        if (contractAddress && tokenId && makerAddress) {
          // FETCH INITIATED log

          // Fetch existing order from OpenSea
          const existingOffer = await fetchMakerOfferForToken(contractAddress, tokenId, makerAddress);

          if (existingOffer) {
            // Convert expiration_time (Unix seconds) to Date
            const expiresAt = new Date(existingOffer.expirationTime * 1000);

            // Convert current_price (Wei string) to ETH string
            const decimal = 18; // WETH decimal
            const offerPriceEth = Number(existingOffer.currentPrice) / Math.pow(10, decimal);
            // Check if we already have this order in cache
            const cachedOrders = await orderCache.get(taskId, 'opensea', existingOffer.tokenId);

            if (cachedOrders && !cachedOrders.some(o => o.orderHash === existingOffer.orderHash)) {
              // Cache doesn't have this orderHash - likely stale after server restart
              warnLog('OpenSea',
                `[ORDER_SYNC] Cache out of sync with OpenSea - updating | ` +
                `taskId=${taskId} | tokenId=${existingOffer.tokenId} | ` +
                `cachedHashes=${cachedOrders.map(o => o.orderHash.slice(0, 16)).join(',')}... | ` +
                `apiHash=${existingOffer.orderHash.slice(0, 16)}...`
              );
            }

            // Sync cache with OpenSea's truth (NO cancellation - this is recovery)
            const expiresAtTimestamp = expiresAt.getTime();
            const createdAt = Date.now();
            await orderCache.set({
              orderHash: existingOffer.orderHash,
              expiresAt: expiresAtTimestamp,
              offerPrice: offerPriceEth.toString(),
              createdAt: createdAt,
              identifier: existingOffer.tokenId,
              marketplace: 'opensea',
              slug: existingOffer.slug || collectionSlug || '',
              taskId: taskId
            });

            // TOP OFFER CACHE UPDATE log

            // NOTE: Do NOT track bid rate here - this is an existing order, not a new bid
            // trackBidRate('opensea', taskId); // REMOVED: prevents double-counting existing orders

            // Increment ownBidCounter - this is a confirmed bid in cache
            incrementOwnBidCounter('opensea', existingOffer.orderHash);

            // SYNC SUCCESS log (warnLog for visibility - always logged)
            warnLog('API', `[OpenSea] ORDER_ALREADY_EXISTS SYNC SUCCESS | taskId=${taskId} | orderHash=${existingOffer.orderHash} | token=${existingOffer.tokenId} | price=${offerPriceEth} ETH | slug=${existingOffer.slug} | expiresAt=${expiresAt.toISOString()}`);

            // Return the synced order data (similar to successful bid response)
            return { order: { order_hash: existingOffer.orderHash } };
          } else {
            // NO OFFER FOUND log
            warnLog('API', `[OpenSea] ORDER_ALREADY_EXISTS sync SKIPPED - no active offer found on OpenSea | taskId=${taskId} | token=${tokenId}`);
          }
        } else {
          // MISSING PAYLOAD DATA log
          warnLog('API', `[OpenSea] ORDER_ALREADY_EXISTS sync SKIPPED - missing payload data | taskId=${taskId} | hasContract=${!!contractAddress} | hasToken=${!!tokenId} | hasMaker=${!!makerAddress}`);
        }
      } catch (syncError) {
        // SYNC FAILURE log
        errorLog('API', `[OpenSea] ORDER_ALREADY_EXISTS sync FAILED | taskId=${taskId} | slug=${collectionSlug}`, syncError);
      }
    }

    errorLog('API', `[OpenSea] Failed to post item offer | taskId=${taskId} | status=${error?.response?.status} | ${errorMessage}`, error);
    throw error;
  } finally {
    removeActiveRequest(taskId, controller);
  }
}

const getItemConsideration = (
  assetContractAddress: string,
  tokenId: string,
  quantity: number,
  walletAddress: string

) => {
  const fees = [
    getItemTokenConsideration(assetContractAddress, tokenId, quantity, walletAddress)
  ]
  return fees
}

const getSalt = () => {
  return Math.floor(Math.random() * 100_000).toString()
}

const getItemTokenConsideration = (
  assetContractAddress: string,
  tokenId: string,
  quantity: number,
  walletAddress: string
) => {
  return {
    itemType: 2,
    token: assetContractAddress,
    identifierOrCriteria: tokenId,
    startAmount: quantity.toString(),
    endAmount: quantity.toString(),
    recipient: walletAddress,
  }
}

export async function fetchItemOffers(contractAddress: string, tokenId: string) {
  const query = {
    operationName: "UseItemOffersQuery",
    query: `query UseItemOffersQuery($filter: ItemOffersFilter, $cursor: String, $limit: Int!, $address: Address, $identifier: ItemIdentifierInput!) {
      itemByIdentifier(identifier: $identifier) {
        ... on Item {
          ...ItemOwnedQuantity
          __typename
        }
        __typename
      }
      itemOffers(filter: $filter, cursor: $cursor, limit: $limit) {
        items {
          id
          ...ItemOrdersTableRow
          ...useCancelOrders
          __typename
        }
        nextPageCursor
        __typename
      }
    }

    fragment ItemOrdersTableRow on Order {
      id
      type
      pricePerItem {
        token {
          unit
          __typename
        }
        ...TokenPrice
        __typename
      }
      quantityRemaining
      maker {
        ...AccountLockup
        ...ProfilePreviewTooltip
        __typename
      }
      endTime
      ...useCancelOrders
      __typename
    }

    fragment TokenPrice on Price {
      usd
      token {
        unit
        symbol
        contractAddress
        chain {
          identifier
          __typename
        }
        __typename
      }
      __typename
    }

    fragment AccountLockup on ProfileIdentifier {
      address
      displayName
      imageUrl
      ...profileUrl
      __typename
    }

    fragment profileUrl on ProfileIdentifier {
      address
      __typename
    }

    fragment useCancelOrders on BaseOrder {
      id
      marketplace {
        identifier
        __typename
      }
      maker {
        address
        __typename
      }
      __typename
    }

    fragment ProfilePreviewTooltip on ProfileIdentifier {
      address
      ...ProfilePreviewTooltipContent
      __typename
    }

    fragment ProfilePreviewTooltipContent on ProfileIdentifier {
      address
      __typename
    }

    fragment ItemOwnedQuantity on Item {
      ownership(address: $address) {
        id
        quantity
        __typename
      }
      __typename
    }`,
    variables: {
      address: "0x1b8afd0de8a9368ce635be8b9572c65d8f590b21",
      filter: {
        chain: "ethereum",
        contractAddress: contractAddress,
        tokenId: tokenId
      },
      identifier: {
        chain: "ethereum",
        contractAddress: contractAddress,
        tokenId: tokenId
      },
      limit: 2
    }
  };

  try {
    // Direct call without deduplication - token offers change with every bid (fresh data required)
    const itemOffers = await limiter.schedule(() =>
      axiosInstance.post(
        `${BASE_URL}/opensea/graphql`,
        query,
        {
          headers: {
            'content-type': 'application/json',
            'x-api-key': API_KEY
          }
        }
      )
    );

    // Validate response structure
    const responseData = (itemOffers.data) as unknown as GraphQLItemOfferResponse;
    if (!responseData?.data?.itemOffers?.items) {
      warnLog('ItemOffers', `Invalid response structure | contract=${contractAddress} | token=${tokenId}`);
      return null;
    }

    const order = responseData.data.itemOffers.items
      ?.filter((offer) => offer && new Date(offer?.endTime).getTime() > Date.now()) // remove expired offers
      .map((offer) => ({
        amount: offer?.pricePerItem?.token?.unit || 0,
        owner: offer?.maker?.address || '0x0000000000000000000000000000000000000000',
        currency: offer?.pricePerItem?.token?.symbol?.toUpperCase() || 'WETH',
        usd: offer?.pricePerItem?.usd || 0,
        decimal: offer?.pricePerItem?.token?.symbol?.toUpperCase() === "WETH" ? 18 : offer?.pricePerItem?.token?.symbol?.toUpperCase() === "ETH" ? 18 : offer?.pricePerItem?.token?.symbol?.toUpperCase() === "BETH" ? 18 : offer?.pricePerItem?.token?.symbol?.toUpperCase() === "USDC" ? 6 : offer?.pricePerItem?.token?.symbol?.toUpperCase() === "USDT" ? 6 : 18,
        eth: offer?.pricePerItem?.token?.unit || 0
      })) || [];

    if (order.length === 0) {
      warnLog('ItemOffers', `No valid offers found | contract=${contractAddress} | token=${tokenId}`);
      return null;
    } else if (order.length === 1) {
      // Default offer structure for second offer
      const defaultOffer = {
        amount: 0,
        owner: '0x0000000000000000000000000000000000000000',
        currency: 'WETH',
        usd: 0,
        decimal: 18,
        eth: 0
      };
      return [order[0], defaultOffer];
    }
    return order;

  } catch (error) {
    errorLog('OpenSea', `[fetchItemOffers] API_ERROR | status=${(error as any)?.response?.status} | url=${BASE_URL}/opensea/graphql | contract=${contractAddress} | token=${tokenId}`, (error as any)?.response?.data);
    return null;
  }
}

export async function fetchOpenseaOffers(
  offerType: 'COLLECTION' | 'TRAIT' | 'TOKEN',
  collectionSlug: string,
  identifiers: Record<string, string> | string,
  priority?: number
) {
  if (offerType === 'COLLECTION') {
    // Use OpenSea GraphQL API for collection offers
    const query = {
      operationName: "CollectionOffersTableQuery",
      query: `query CollectionOffersTableQuery($collectionSlug: String!, $filter: CollectionOffersFilter, $sort: CollectionOfferAggregateSort!, $cursor: String, $limit: Int!) {
        collectionOfferAggregates(
          collectionSlug: $collectionSlug
          filter: $filter
          sort: $sort
          cursor: $cursor
          limit: $limit
        ) {
          items {
            id
            totalOffers
            offerPrice {
              token {
                unit
                __typename
              }
              __typename
            }
            ...OfferAggregateTable
            __typename
          }
          nextPageCursor
          __typename
        }
        collectionBySlug(slug: $collectionSlug) {
          ...OfferAggregateTable_collection
          __typename
        }
      }
      fragment OfferAggregateTable_collection on Collection {
        ...useCollectionOffers
        ...OfferAggregateTableRow_collection
        __typename
      }
      fragment useCollectionOffers on CollectionResult {
        __typename
        ... on Collection {
          slug
          flags {
            isOffersEnabled
            __typename
          }
          traitOffersEnabled
          chain {
            identifier
            arch
            __typename
          }
          __typename
        }
      }
      fragment OfferAggregateTableRow_collection on Collection {
        ...useCollectionOffers
        __typename
      }
      fragment OfferAggregateTable on OfferAggregate {
        id
        ...OfferAggregateTableRow
        __typename
      }
      fragment OfferAggregateTableRow on OfferAggregate {
        offerPrice {
          token {
            unit
            contractAddress
            __typename
          }
          ...TokenPrice
          __typename
        }
        totalValue {
          token {
            unit
            __typename
          }
          ...TokenPrice
          __typename
        }
        totalOffers
        bidders {
          quantity
          __typename
        }
        ...OfferAggregateBidders
        __typename
      }
      fragment TokenPrice on Price {
        usd
        token {
          unit
          symbol
          contractAddress
          chain {
            identifier
            __typename
          }
          __typename
        }
        __typename
      }
      fragment OfferAggregateBidders on OfferAggregate {
        offerPrice {
          ...TokenPrice
          __typename
        }
        totalValue {
          ...TokenPrice
          __typename
        }
        totalOffers
        bidders {
          quantity
          imageUrl
          address
          ...AccountLockup
          __typename
        }
        __typename
      }
      fragment AccountLockup on ProfileIdentifier {
        address
        displayName
        imageUrl
        ...profileUrl
        __typename
      }
      fragment profileUrl on ProfileIdentifier {
        displayName
        address
        __typename
      }`,
      variables: {
        collectionSlug: collectionSlug,
        filter: {},
        limit: 2,
        sort: {
          by: "OFFER_PRICE",
          direction: "DESC"
        }
      }
    };

    // Direct call without deduplication - consistent with fetchItemOffers approach
    let response: any;
    try {
      response = await limiter?.schedule(
        priority !== undefined ? { priority } : {},
        () => axiosInstance.post(`${BASE_URL}/opensea/graphql`, query, {
          headers: {
            'content-type': 'application/json',
            'x-api-key': API_KEY
          }
        })
      );
    } catch (error) {
      errorLog('OpenSea', `[fetchCollectionOffers] API_ERROR | status=${(error as any)?.response?.status} | url=${BASE_URL}/opensea/graphql | slug=${collectionSlug}`, (error as any)?.response?.data);
      return [{amount:0,owner:'0x0000000000000000000000000000000000000000',currency:'WETH',usd:0,decimal:18,eth:0},{amount:0,owner:'0x0000000000000000000000000000000000000000',currency:'WETH',usd:0,decimal:18,eth:0}];
    }
    // Parse the GraphQL response
    const offerAggregates = response.data?.data?.collectionOfferAggregates?.items || [];

    // Map to the expected format and get top 2 offers
    const offers = offerAggregates
      .slice(0, 2)
      .map((aggregate: any) => {
        // Get the first bidder if available
        const firstBidder = aggregate?.bidders?.[0];

        return {
          amount: aggregate?.offerPrice?.token?.unit || 0,
          owner: firstBidder?.address || 'unknown',
          currency: aggregate?.offerPrice?.token?.symbol || 'WETH',
          usd: aggregate?.offerPrice?.usd || 0,
          decimal: aggregate?.offerPrice?.token?.symbol?.toUpperCase() === 'USDC' ? 6 : 18,
          eth: aggregate?.offerPrice?.token?.unit || 0
        };
      });

    // Default offer structure
    const defaultOffer = {
      amount: 0,
      owner: '0x0000000000000000000000000000000000000000',
      currency: 'WETH',
      usd: 0,
      decimal: 18,
      eth: 0
    };

    // Ensure we always return at least 2 offers
    if (offers.length === 0) {
      return [defaultOffer, defaultOffer];
    } else if (offers.length === 1) {
      return [offers[0], defaultOffer];
    }
    return offers
  } else if (offerType === 'TRAIT') {
    const { type, value } = identifiers as Record<string, string>;

    // Create tasks for both collection and trait offers
    const tasks = [
      // Collection offers task
      () => {
        const collectionQuery = {
          operationName: "CollectionOffersTableQuery",
          query: `query CollectionOffersTableQuery($collectionSlug: String!, $filter: CollectionOffersFilter, $sort: CollectionOfferAggregateSort!, $cursor: String, $limit: Int!) {
            collectionOfferAggregates(
              collectionSlug: $collectionSlug
              filter: $filter
              sort: $sort
              cursor: $cursor
              limit: $limit
            ) {
              items {
                id
                totalOffers
                offerPrice {
                  token {
                    unit
                    __typename
                  }
                  __typename
                }
                ...OfferAggregateTable
                __typename
              }
              nextPageCursor
              __typename
            }
            collectionBySlug(slug: $collectionSlug) {
              ...OfferAggregateTable_collection
              __typename
            }
          }
          fragment OfferAggregateTable_collection on Collection {
            ...useCollectionOffers
            ...OfferAggregateTableRow_collection
            __typename
          }
          fragment useCollectionOffers on CollectionResult {
            __typename
            ... on Collection {
              slug
              flags {
                isOffersEnabled
                __typename
              }
              traitOffersEnabled
              chain {
                identifier
                arch
                __typename
              }
              __typename
            }
          }
          fragment OfferAggregateTableRow_collection on Collection {
            ...useCollectionOffers
            __typename
          }
          fragment OfferAggregateTable on OfferAggregate {
            id
            ...OfferAggregateTableRow
            __typename
          }
          fragment OfferAggregateTableRow on OfferAggregate {
            offerPrice {
              token {
                unit
                contractAddress
                __typename
              }
              ...TokenPrice
              __typename
            }
            totalValue {
              token {
                unit
                __typename
              }
              ...TokenPrice
              __typename
            }
            totalOffers
            bidders {
              quantity
              __typename
            }
            ...OfferAggregateBidders
            __typename
          }
          fragment TokenPrice on Price {
            usd
            token {
              unit
              symbol
              contractAddress
              chain {
                identifier
                __typename
              }
              __typename
            }
            __typename
          }
          fragment OfferAggregateBidders on OfferAggregate {
            offerPrice {
              ...TokenPrice
              __typename
            }
            totalValue {
              ...TokenPrice
              __typename
            }
            totalOffers
            bidders {
              quantity
              imageUrl
              address
              ...AccountLockup
              __typename
            }
            __typename
          }
          fragment AccountLockup on ProfileIdentifier {
            address
            displayName
            imageUrl
            ...profileUrl
            __typename
          }
          fragment profileUrl on ProfileIdentifier {
            displayName
            address
            __typename
          }`,
          variables: {
            collectionSlug: collectionSlug,
            filter: {},
            limit: 10,
            sort: {
              by: "OFFER_PRICE",
              direction: "DESC"
            }
          }
        };

        // Direct call without deduplication
        return limiter?.schedule(
          priority !== undefined ? { priority } : {},
          () => axiosInstance.post(`${BASE_URL}/opensea/graphql`, collectionQuery, {
            headers: {
              'content-type': 'application/json',
              'x-api-key': API_KEY
            }
          })
        );
      },

      // Trait offers task
      () => {
        const traitQuery = {
          operationName: "TraitOffersTableQuery",
          query: `query TraitOffersTableQuery($collectionSlug: String!, $filter: TraitOffersFilter, $sort: TraitOfferAggregateSort!, $cursor: String, $limit: Int!) {
            traitOfferAggregates(
              collectionSlug: $collectionSlug
              filter: $filter
              sort: $sort
              cursor: $cursor
              limit: $limit
            ) {
              items {
                id
                totalOffers
                offerPrice {
                  usd
                  __typename
                }
                ...OfferAggregateTable
                __typename
              }
              nextPageCursor
              __typename
            }
            collectionBySlug(slug: $collectionSlug) {
              ...OfferAggregateTable_collection
              __typename
            }
          }
          fragment OfferAggregateTable_collection on Collection {
            ...useCollectionOffers
            ...OfferAggregateTableRow_collection
            __typename
          }
          fragment useCollectionOffers on CollectionResult {
            __typename
            ... on Collection {
              slug
              flags {
                isOffersEnabled
                __typename
              }
              traitOffersEnabled
              chain {
                identifier
                arch
                __typename
              }
              __typename
            }
          }
          fragment OfferAggregateTableRow_collection on Collection {
            ...useCollectionOffers
            __typename
          }
          fragment OfferAggregateTable on OfferAggregate {
            id
            ...OfferAggregateTableRow
            __typename
          }
          fragment OfferAggregateTableRow on OfferAggregate {
            offerPrice {
              token {
                unit
                contractAddress
                __typename
              }
              ...TokenPrice
              __typename
            }
            totalValue {
              token {
                unit
                __typename
              }
              ...TokenPrice
              __typename
            }
            totalOffers
            bidders {
              quantity
              __typename
            }
            ...OfferAggregateBidders
            __typename
          }
          fragment TokenPrice on Price {
            usd
            token {
              unit
              symbol
              contractAddress
              chain {
                identifier
                __typename
              }
              __typename
            }
            __typename
          }
          fragment OfferAggregateBidders on OfferAggregate {
            offerPrice {
              ...TokenPrice
              __typename
            }
            totalValue {
              ...TokenPrice
              __typename
            }
            totalOffers
            bidders {
              quantity
              imageUrl
              address
              ...AccountLockup
              __typename
            }
            __typename
          }
          fragment AccountLockup on ProfileIdentifier {
            address
            displayName
            imageUrl
            ...profileUrl
            __typename
          }
          fragment profileUrl on ProfileIdentifier {
            displayName
            address
            __typename
          }`,
          variables: {
            collectionSlug: collectionSlug,
            filter: {
              traitType: type,
              traitValue: value
            },
            limit: 10,
            sort: {
              by: "OFFER_PRICE",
              direction: "DESC"
            }
          }
        };

        // Direct call without deduplication
        return limiter?.schedule(
          priority !== undefined ? { priority } : {},
          () => axiosInstance.post(`${BASE_URL}/opensea/graphql`, traitQuery, {
            headers: {
              'content-type': 'application/json',
              'x-api-key': API_KEY
            }
          })
        );
      }
    ];

    // Execute both queries in parallel
    const results = await Promise.allSettled(tasks.map(t => t()));
    const [collectionResponse, traitResponse] = results.map(r =>
      r.status === 'fulfilled' ? r.value : null
    );

    // Parse responses
    const collectionOffers = collectionResponse?.data?.data?.collectionOfferAggregates?.items || [];
    const traitOffers = traitResponse?.data?.data?.traitOfferAggregates?.items || [];



    // Combine and map all offers
    const allOffers = [...collectionOffers, ...traitOffers]
      .map((aggregate: any) => {
        // Get the first bidder if available
        const firstBidder = aggregate.bidders?.[0];

        return {
          amount: aggregate.offerPrice?.token?.unit || 0,
          owner: firstBidder?.address || 'unknown',
          currency: aggregate.offerPrice?.token?.symbol || 'WETH',
          usd: aggregate.offerPrice?.usd || 0,
          decimal: aggregate.offerPrice?.token?.symbol?.toUpperCase() === 'USDC' ? 6 : 18,
          eth: aggregate.offerPrice?.token?.unit || 0
        };
      })
      // Sort by USD value descending and take top 2
      .sort((a, b) => +b.eth - +a.eth)
      .slice(0, 2);

    // Default offer structure
    const defaultOffer = {
      amount: 0,
      owner: '0x0000000000000000000000000000000000000000',
      currency: 'WETH',
      usd: 0,
      decimal: 18,
      eth: 0
    };

    // Ensure we always return at least 2 offers
    if (allOffers.length === 0) {
      return [defaultOffer, defaultOffer];
    } else if (allOffers.length === 1) {
      return [allOffers[0], defaultOffer];
    }

    return allOffers;
  }
}

export async function openseaTraitFloor(slug: string, trait: {
  traitType: string;
  values: string[];
}) {
  // Use LRU-only cache for fastest access (< 1ms vs 10-20ms Redis)
  const lruKey = `${slug}:${trait.traitType}:${trait.values.join(',')}`;

  // Try LRU cache (< 1ms)
  const cached = traitFloorLRU.get(lruKey);
  if (cached !== undefined) {
    cacheStats.traitFloor.hits++;
    return cached;
  }

  // Fetch from API (500-2000ms) - Phase 4: Direct fetch, no Redis layer
  cacheStats.traitFloor.misses++;

  const variables: CollectionItemsQueryVariables = {
    collectionSlug: slug,
    filter: {
      attributes: [trait]
    },
    limit: 1,
    sort: {
      by: 'PRICE',
      direction: 'ASC',
    },
  };
  const queryVariables = {
    ...variables,
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
    const floor = data.data.collectionItems.items[0].bestListing?.pricePerItem.token.unit;

    // Store in LRU cache
    if (floor !== null && floor !== undefined) {
      traitFloorLRU.set(lruKey, floor);
    }

    return floor;
  } catch (error) {
    errorLog('OpenSea', `[openseaTraitFloor] API_ERROR | status=${(error as any)?.response?.status} | url=${BASE_URL}/opensea/graphql | slug=${slug}`, (error as any)?.response?.data);
    return undefined;
  }
}

export async function fetchOpenseaListings(taskId: string, collectionSlug: string, limit?: number, priority?: number) {
  if (!limit) return;

  // Check LRU cache first
  const listingKey = `${collectionSlug}:${limit}`;
  const cached = openseaListingsLRU.get(listingKey);
  if (cached) {
    return cached;
  }

  // Cache miss - fetch from API
  const baseUrl = `${BASE_URL}/opensea/api/v2/listings/collection/${collectionSlug}/best`;
  let allListings: OpenseaOrder[] = [];
  let nextCursor: string | null = null;
  let uniqueTokenIds: string[] = [];

  while (uniqueTokenIds.length < limit) {
    const params: any = {
      next: nextCursor,
      limit: Math.min(100, limit * 2 - uniqueTokenIds.length) // Fetch more to account for duplicates
    }
    const { data } = await limiter.schedule(priority !== undefined ? { priority } : {}, () => axiosInstance.get<OpenseaListingData>(baseUrl, {
      headers: {
        'accept': 'application/json',
        'x-api-key': API_KEY
      },
      params: params
    }))

    allListings = [...allListings, ...data.listings];
    uniqueTokenIds = [...new Set(allListings.map((item) =>
      item.protocol_data.parameters.offer[0].identifierOrCriteria
    ))];

    nextCursor = data.next;
    if (!nextCursor) break;
  }

  const result = uniqueTokenIds.slice(0, limit);
  // Store in LRU cache
  openseaListingsLRU.set(listingKey, result);

  return result;
}

interface OpenseaListingData {
  listings: OpenseaOrder[]
  next: string
}

interface OpenseaOrder {
  order_hash: string;
  chain: string;
  type: string;
  price: OpenseaPrice;
  protocol_data: OpenseaProtocolData;
  protocol_address: string;
}

interface OpenseaPrice {
  current: OpenseaPriceCurrent;
}

interface OpenseaPriceCurrent {
  currency: string;
  decimals: number;
  value: string;
}

interface OpenseaProtocolData {
  parameters: OpenseaParameters;
  signature: null;
}

interface OpenseaParameters {
  offerer: string;
  offer: OpenseaOfferItem[];
  consideration: OpenseaConsideration[];
  startTime: string;
  endTime: string;
  orderType: number;
  zone: string;
  zoneHash: string;
  salt: string;
  conduitKey: string;
  totalOriginalConsiderationItems: number;
  counter: number;
}

interface OpenseaOfferItem {
  itemType: number;
  token: string;
  identifierOrCriteria: string;
  startAmount: string;
  endAmount: string;
}

interface OpenseaOffer {
  order_hash: string;
  criteria?: {
    trait?: {
      type: string;
      value: string;
    };
    collection?: {
      slug: string;
    };
  };
}

interface OpenseaConsideration {
  itemType: number;
  token: string;
  identifierOrCriteria: string;
  startAmount: string;
  endAmount: string;
  recipient: string;
}

export interface IFee {
  [address: string]: number;
}

interface IPayload {
  criteria: ICriteria;
  protocol_data: IProtocolData;
  protocol_address: string;
  [key: string]: any; // Allow additional properties
}

interface ICriteria {
  collection: {
    slug: string;
  };
  trait?: any; // Optional trait, can be more specific if needed
  [key: string]: any; // Allow additional properties
}

interface IProtocolData {
  parameters: {
    offerer: string;
    offer: IOfferItem[];
    consideration: IConsiderationItem[];
    startTime: string;
    endTime: string;
    orderType: number;
    zone: string;
    zoneHash: string;
    conduitKey: string;
    totalOriginalConsiderationItems: number;
    counter: string;
    [key: string]: any; // Allow additional properties
  };
  signature: string;
  [key: string]: any; // Allow additional properties
}

interface IOfferItem {
  itemType: number;
  token: string;
  identifierOrCriteria: number;
  startAmount: string;
  endAmount: string;
  [key: string]: any; // Allow additional properties
}

interface IConsiderationItem {
  itemType: number;
  token: string;
  identifierOrCriteria: number;
  startAmount: string;
  endAmount: string;
  recipient: string;
  [key: string]: any; // Allow additional properties
}

interface PartialParameters {
  consideration: Array<{
    itemType: number;
    token: string;
    identifierOrCriteria: string;
    startAmount: string;
    endAmount: string;
    recipient: string;
    [key: string]: any; // Allow additional properties
  }>;
  zone: string;
  zoneHash: string;
  [key: string]: any; // Allow additional properties
}

interface ItemOfferSpecification {
  assetContractAddress: string
  tokenId: string
  quantity: number
  priceWei: bigint
  expirationSeconds: bigint
  walletAddress: string
}

export interface OpenseaOfferData {
  amount: number;
  owner: string;
  currency: string;
  usd: number;
  eth: number;
  decimal: number;
}

export interface GraphQLItemOfferResponse {
  data: {
    itemOffers: ItemOffers;
  };
}

export interface ItemOffers {
  items: OpenSeaOrder[];
}

export interface OpenSeaOrder {
  pricePerItem: IPrice;
  maker: OrderProfile;
  endTime: string;
}

export interface IPrice {
  token: ITokenPrice;
  usd: number;
}

export interface ITokenPrice {
  unit: number;
  symbol: string;
}

export interface OrderProfile {
  address: string;
}
/**
 * Get cache instances for debugging/monitoring
 * @internal - For use by lru-cache-sampler.ts only
 */
export function getContractResolutionLRU(): LRUCache<string, string> {
  return contractResolutionLRU;
}

export function getTraitFloorLRU(): LRUCache<string, number> {
  return traitFloorLRU;
}

export function getMultiContractSlugLRU(): LRUCache<string, boolean> {
  return multiContractSlugLRU;
}

export function getContractMapLRU(): LRUCache<string, Record<string, string>> {
  return contractMapLRU;
}

export function getOpenseaWalletCache(): LRUCache<string, Wallet> {
  return walletCache;
}

export function getOpenseaInsufficientBalanceLogCache(): LRUCache<string, boolean> {
  return insufficientBalanceLogCache;
}
