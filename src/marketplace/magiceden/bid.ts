/* eslint-disable */

// To apply stashed changes to just this file: git stash apply stash@{0} -- src/marketplace/magiceden/bid.ts

import { ethers, Wallet } from "ethers";
import { axiosInstance, limiter, isTaskBlacklisted } from "../../init";
import { config } from "dotenv";
import { activeTasks, API_KEY, currentTasks, decrementBidCount, MAGENTA, pQueue, trackBidRate, updateTaskCancellationPercentage } from "../..";
import { DistributedLockManager } from '../../utils/lock';
import Order from '../../models/order.model';
import { withRetry } from '../../utils/db-retry';
import redisClient from '../../utils/redis';
import axios from "axios";
import { USDC, WETH } from "../../utils/currencies";
import { BASE_URL } from "../../constants";
import { fetchERC20Balance } from "../../utils/balance";
import { convertBigNumbers } from "../../utils/bigNumberConverter";
import { logger, DatabaseOperation, ErrorCategory, MarketplaceSubCategory } from "../../utils/logger";
import { InsufficientBalanceError, MinimumPriceError, RateLimitError, InvalidRequestError } from "../errors";
import { cancellationTracker } from "../../utils/cancellation-tracker";


const YELLOW = '\x1b[33m';
const CYAN = '\x1b[36m';
const GREEN = '\x1b[32m';

// Helper function to extract meaningful error messages from MagicEden API responses
function extractMagicEdenErrorMessage(error: any): string {
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

const RED = '\x1b[31m';
const RESET = '\x1b[0m';

config()

// Environment-specific configuration for MagicEden
const MAGICEDEN_CONFIG = {
  domain: 'magiceden.us',
  feeRecipient: '0xca9337244b5f04cb946391bc8b8a980e988f9a6a'
}

const ALCHEMY_API_KEY = process.env.ALCHEMY_API_KEY as string;
// Singleton provider with connection pooling
const provider = new ethers.AlchemyProvider('mainnet', ALCHEMY_API_KEY);

const lockManager = new DistributedLockManager({
  lockPrefix: 'magiceden:fetch:',
  defaultTTLSeconds: 30
});

const activeRequests = new Map<string, AbortController[]>();

// Pre-calculated constants for performance
const BASIS_CONSTANTS: Record<number, number> = {
  2: 1e16,
  3: 1e15,
  4: 1e14
};

// Wallet cache to avoid recreating wallet instances
const walletCache = new Map<string, Wallet>();
function getWallet(privateKey: string): Wallet {
  if (!walletCache.has(privateKey)) {
    walletCache.set(privateKey, new Wallet(privateKey, provider));
  }
  return walletCache.get(privateKey)!;
}

// Cached domain and types for signing
const domainCache = {
  name: "Seaport",
  version: "1.6",
  chainId: 1,
  verifyingContract: "0x0000000000000068f116a894984e2db1123eb395"
};

const typesCache = {
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


// Request deduplication for concurrent requests
const pendingRequests = new Map<string, Promise<any>>();

// Batch queue for database operations
const orderBatchQueue: Array<{
  slug: string;
  orderHash: string;
  expiresAt: Date;
  offerPrice: string;
  identifier: string;
  taskId: string;
  marketplace: string;
}> = [];

let batchProcessor: NodeJS.Timeout | null = null;

// Cleanup function for graceful shutdown
export async function cleanupMagicEden() {
  // Clear pending requests
  pendingRequests.clear();

  // Process any remaining batched orders
  if (batchProcessor) {
    clearTimeout(batchProcessor);
    batchProcessor = null;
  }

  if (orderBatchQueue.length > 0) {
    const batch = [...orderBatchQueue];
    orderBatchQueue.length = 0;

    try {
      await withRetry(async () => {
        return await Order.insertMany(batch);
      });
    } catch (error) {
      await logger.logDatabaseError('Failed to create orders during cleanup', {
        databaseOperation: DatabaseOperation.CREATE,
        collection_name: 'orders',
        affectedRows: batch.length,
        error: error as Error
      });
    }
  }
}

/**
 * Places a bid on Magic Eden.
 * @param maker - The maker of the bid.
 * @param collection - The collection to bid on.
 * @param quantity - The quantity to bid.
 * @param weiPrice - The price in wei.
 * @param expirationTime - The expiration time of the bid.
 * @param privateKey - The private key of the maker's wallet.
 * @param slug - Collection slug
 * @param currency - The currency to bid with (defaults to WETH)
 * @param traits - Optional traits for the offer.
 */

export async function bidOnMagiceden(
  taskId: string,
  bidCount: string,
  maker: string,
  collection: string,
  quantity: number,
  weiPrice: string,
  privateKey: string,
  slug: string,
  bidExpiry: number,
  currency = WETH,
  trait?: Trait,
  tokenId?: number
) {

  try {
    if (isTaskBlacklisted(taskId, 'magiceden')) {
      const error = new Error(`Bid execution blocked - task ${taskId} is stopped for MagicEden`);
      throw error;
    }

    // Early validation before expensive operations
    const slugLower = slug.toLowerCase();
    const task = currentTasks.find((task) =>
      task.contract.slug.toLowerCase() === slugLower &&
      task.selectedMarketplaces.includes("MagicEden")
    );

    if (!task) {

      return;
    }

    // Pre-calculate all constants upfront
    const duration = bidExpiry / 60 || 15; // minutes
    const currentTime = Date.now();
    const expiration = Math.floor((currentTime + (duration * 60 * 1000)) / 1000);

    const isUSDC = currency.toLowerCase() === USDC.toLowerCase();
    const decimal = isUSDC ? 6 : 18;
    const decimalPower = Math.pow(10, decimal);
    const currencySymbol = isUSDC ? "USDC" : "WETH";

    // Get wallet from cache
    const wallet = getWallet(privateKey);

    const wethBalance = await fetchERC20Balance(maker, currency);
    const offerPriceEth = Math.round(Number(weiPrice)) / decimalPower;

    // Optimize decimal calculation
    const decimals = offerPriceEth >= 1 ? 2 : offerPriceEth >= 0.1 ? 3 : 4;
    const roundedEth = isUSDC ? Number(offerPriceEth.toFixed(2)) : Number(offerPriceEth);

    const basis = BASIS_CONSTANTS[decimals];
    const roundedWei = decimal === 18 ?
      Math.round(roundedEth * decimalPower / basis) * basis :
      Math.round(roundedEth * decimalPower);

    // Early validation - fail fast before expensive operations
    if (offerPriceEth > wethBalance) {
      const error = new InsufficientBalanceError(
        'magiceden',
        currencySymbol,
        offerPriceEth,
        wethBalance,
        maker,
        slug
      );
      await logger.logMagicEdenBidError('Insufficient balance for bid', {
        taskId,
        walletAddress: maker,
        collection: slug,
        bidAmount: offerPriceEth,
        currency: currencySymbol,
        availableBalance: wethBalance,
        requiredBalance: offerPriceEth,
        error
      });
      throw error;
    }

    // Route based on offer type
    let order;
    if (!trait && !tokenId) {
      // Collection offer - use v5 API
      order = await createBidData(
        taskId,
        slug,
        maker,
        collection,
        quantity,
        Math.round(Number(weiPrice)).toString(),
        expiration.toString(),
        currency,
        trait,
        tokenId
      );
    } else {
      order = generateMagicEdenOrder(
        taskId,
        slug,
        maker,
        collection,
        quantity,
        Math.round(Number(weiPrice)).toString(),
        expiration.toString(),
        currency,
        trait,
        tokenId
      );
    }
    if (!order) return;
    await submitSignedOrderData(
      taskId,
      roundedWei.toString(),
      decimal,
      currencySymbol,
      bidCount,
      order,
      wallet,
      slug,
      bidExpiry,
      trait,
      tokenId,
      task // Pass the cached task
    );
    return order;
  } catch (error) {
    throw error;
  }
}

/**
 * Creates collection offer bid data by calling MagicEden's v5 API.
 * This is only used for collection offers. Trait and token offers use generateMagicEdenOrder.
 * @param taskId - The task ID
 * @param slug - The collection slug
 * @param maker - The maker of the bid
 * @param collection - The collection contract address
 * @param quantity - The quantity to bid
 * @param weiPrice - The price in wei
 * @param expirationTime - The expiration time of the bid
 * @param currency - The currency (WETH)
 * @param trait - Optional traits for the offer (should be undefined for collection offers)
 * @param tokenId - Optional token ID for token bids (should be undefined for collection offers)
 * @returns The order structure from MagicEden API
 */
async function createBidData(
  taskId: string,
  slug: string,
  maker: string,
  collection: string,
  quantity: number,
  weiPrice: string,
  expirationTime: string,
  currency = WETH,
  trait?: Trait,
  tokenId?: number | string
): Promise<any> {
  const orderKind = trait || tokenId ? "seaport-v1.6" : "payment-processor-v2";
  // Build options based on bid type
  const options = tokenId ? {
    [tokenId.toString()]: {
      useOffChainCancellation: true
    }
  } : {};

  const params = [
    {
      ...(tokenId ? {} : { collection: collection }),
      currency: currency,
      quantity: quantity,
      weiPrice: weiPrice,
      expirationTime: expirationTime,
      orderKind: orderKind,
      orderbook: "reservoir",
      options: options,
      automatedRoyalties: true,
      ...(tokenId ? { token: `${collection}:${tokenId}` } : trait ? {
        collection: collection,
        attributeKey: trait.attributeKey,
        attributeValue: trait.attributeValue
      } : {})
    }
  ];

  const data = {
    maker: maker,
    source: MAGICEDEN_CONFIG.domain,
    params: params
  };

  try {
    const controller = new AbortController();
    addActiveRequest(taskId, controller);

    const timeoutId = setTimeout(() => controller.abort(), 30000);

    try {
      const { data: order } = await limiter.schedule(() => axiosInstance.post<CreateBidData>(
        `${BASE_URL}/magiceden/v3/rtp/ethereum/execute/bid/v5`,
        data,
        {
          headers: {
            'content-type': 'application/json',
            'x-api-key': API_KEY
          },
          signal: controller.signal,
          timeout: 30000,
          "axios-retry": { retries: 0 }
        }
      ));

      clearTimeout(timeoutId);

      return order;
    } finally {
      clearTimeout(timeoutId);
      const controllers = activeRequests.get(taskId) || [];
      const index = controllers.indexOf(controller);
      if (index >= 0) {
        controllers.splice(index, 1);
      }
      if (controllers.length === 0) {
        activeRequests.delete(taskId);
      }
    }
  } catch (error: any) {
    if (axios.isCancel(error)) {
      return null;
    }

    const errorMessage = extractMagicEdenErrorMessage(error);
    await logger.logMagicEdenBidError('Failed to create bid data on Magic Eden', {
      taskId,
      collection: slug,
      tokenId: tokenId?.toString(),
      traitType: trait?.attributeKey,
      traitValue: trait?.attributeValue,
      operation: 'create_bid_data',
      error: error,
      errorCode: error?.response?.status,
      errorMessage: errorMessage
    });
    throw error;
  }
}

/**
 * Generates order structure for Magic Eden based on bid type (for trait and token offers).
 * @param taskId - The task ID
 * @param slug - The collection slug
 * @param maker - The maker of the bid
 * @param collection - The collection contract address
 * @param quantity - The quantity to bid
 * @param weiPrice - The price in wei
 * @param expirationTime - The expiration time of the bid
 * @param currency - The currency (WETH)
 * @param trait - Optional traits for the offer
 * @param tokenId - Optional token ID for token bids
 * @returns The order structure
 */
function generateMagicEdenOrder(
  taskId: string,
  slug: string,
  maker: string,
  collection: string,
  quantity: number,
  weiPrice: string,
  expirationTime: string,
  currency = WETH,
  trait?: Trait,
  tokenId?: number | string
): any {
  const currentTime = Math.floor(Date.now() / 1000);
  const salt = ethers.hexlify(ethers.randomBytes(32));
  const nonce = "0";

  // For collection offers, generate a numeric nonce instead of using salt
  const collectionNonce = Math.floor(Math.random() * Number.MAX_SAFE_INTEGER).toString();

  // Calculate marketplace fee (0.5% = 50 basis points)
  const marketplaceFee = Math.floor(Number(weiPrice) * 50 / 10000).toString();

  if (tokenId) {
    // TOKEN bid - uses seaport-v1.6
    return {
      "steps": [
        {
          "id": "order-signature",
          "action": "Order signature",
          "description": "Authorize order",
          "kind": "signature",
          "items": [
            {
              "status": "incomplete",
              "orderIndexes": [0],
              "data": {
                "sign": {
                  "signatureKind": "eip712",
                  "domain": domainCache,
                  "types": typesCache,
                  "value": {
                    "kind": "single-token",
                    "offerer": maker,
                    "zone": "0x0000000000000000000000000000000000000000",
                    "offer": [
                      {
                        "itemType": 1,
                        "token": currency,
                        "identifierOrCriteria": "0",
                        "startAmount": weiPrice,
                        "endAmount": weiPrice
                      }
                    ],
                    "consideration": [
                      {
                        "itemType": 2,
                        "token": collection,
                        "identifierOrCriteria": tokenId.toString(),
                        "startAmount": "1",
                        "endAmount": "1",
                        "recipient": maker
                      },
                      {
                        "itemType": 1,
                        "token": currency,
                        "identifierOrCriteria": "0",
                        "startAmount": marketplaceFee,
                        "endAmount": marketplaceFee,
                        "recipient": MAGICEDEN_CONFIG.feeRecipient
                      }
                    ],
                    "orderType": 0,
                    "startTime": currentTime.toString(),
                    "endTime": expirationTime,
                    "zoneHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
                    "salt": salt,
                    "conduitKey": "0x87328c9043e7bf343695554eaaf5a8892f7205e3000000000000000000000000",
                    "counter": nonce,
                    "signature": "0x0000000000000000000000000000000000000000000000000000000000000000"
                  },
                  "primaryType": "OrderComponents"
                },
                "post": {
                  "endpoint": "/order/v3",
                  "method": "POST",
                  "body": {
                    "order": {
                      "kind": "seaport-v1.6",
                      "data": {
                        "kind": "single-token",
                        "offerer": maker,
                        "zone": "0x0000000000000000000000000000000000000000",
                        "offer": [
                          {
                            "itemType": 1,
                            "token": currency,
                            "identifierOrCriteria": "0",
                            "startAmount": weiPrice,
                            "endAmount": weiPrice
                          }
                        ],
                        "consideration": [
                          {
                            "itemType": 2,
                            "token": collection,
                            "identifierOrCriteria": tokenId.toString(),
                            "startAmount": "1",
                            "endAmount": "1",
                            "recipient": maker
                          },
                          {
                            "itemType": 1,
                            "token": currency,
                            "identifierOrCriteria": "0",
                            "startAmount": marketplaceFee,
                            "endAmount": marketplaceFee,
                            "recipient": MAGICEDEN_CONFIG.feeRecipient
                          }
                        ],
                        "orderType": 0,
                        "startTime": currentTime.toString(),
                        "endTime": expirationTime,
                        "zoneHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
                        "salt": salt,
                        "conduitKey": "0x87328c9043e7bf343695554eaaf5a8892f7205e3000000000000000000000000",
                        "counter": nonce,
                        "signature": "0x0000000000000000000000000000000000000000000000000000000000000000"
                      }
                    },
                    "orderbook": "reservoir",
                    "source": MAGICEDEN_CONFIG.domain
                  }
                }
              }
            }
          ]
        }
      ],
      "errors": []
    };
  } else if (trait) {
    // TRAIT bid - uses seaport-v1.6
    return {
      "steps": [
        {
          "id": "order-signature",
          "action": "Order signature",
          "description": "Authorize order",
          "kind": "signature",
          "items": [
            {
              "status": "incomplete",
              "orderIndexes": [0],
              "data": {
                "sign": {
                  "signatureKind": "eip712",
                  "domain": domainCache,
                  "types": typesCache,
                  "value": {
                    "kind": "token-list",
                    "offerer": maker,
                    "zone": "0x0000000000000000000000000000000000000000",
                    "offer": [
                      {
                        "itemType": 1,
                        "token": currency,
                        "identifierOrCriteria": "0",
                        "startAmount": weiPrice,
                        "endAmount": weiPrice
                      }
                    ],
                    "consideration": [
                      {
                        "itemType": 4,
                        "token": collection,
                        "identifierOrCriteria": "0",
                        "startAmount": quantity.toString(),
                        "endAmount": quantity.toString(),
                        "recipient": maker
                      },
                      {
                        "itemType": 1,
                        "token": currency,
                        "identifierOrCriteria": "0",
                        "startAmount": marketplaceFee,
                        "endAmount": marketplaceFee,
                        "recipient": MAGICEDEN_CONFIG.feeRecipient
                      }
                    ],
                    "orderType": 0,
                    "startTime": currentTime.toString(),
                    "endTime": expirationTime,
                    "zoneHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
                    "salt": salt,
                    "conduitKey": "0x87328c9043e7bf343695554eaaf5a8892f7205e3000000000000000000000000",
                    "counter": nonce,
                    "signature": "0x0000000000000000000000000000000000000000000000000000000000000000"
                  },
                  "primaryType": "OrderComponents"
                },
                "post": {
                  "endpoint": "/order/v3",
                  "method": "POST",
                  "body": {
                    "attribute": {
                      "collection": collection,
                      "key": trait.attributeKey,
                      "value": trait.attributeValue
                    }
                  }
                }
              }
            }
          ]
        }
      ],
      "errors": []
    };
  } else {
    return {
      "steps": [
        {
          "id": "order-signature",
          "action": "Order signature",
          "description": "Authorize order",
          "kind": "signature",
          "items": [
            {
              "status": "incomplete",
              "orderIndexes": [0],
              "data": {
                "sign": {
                  "signatureKind": "eip712",
                  "domain": {
                    "name": "PaymentProcessor",
                    "version": "2",
                    "chainId": 1,
                    "verifyingContract": "0x000000001bb835ab17164ff58f013a115ec465b8"
                  },
                  "types": {
                    "CollectionOfferApproval": [
                      { "name": "protocol", "type": "uint8" },
                      { "name": "cosigner", "type": "address" },
                      { "name": "buyer", "type": "address" },
                      { "name": "beneficiary", "type": "address" },
                      { "name": "marketplace", "type": "address" },
                      { "name": "fallbackRoyaltyRecipient", "type": "address" },
                      { "name": "paymentMethod", "type": "address" },
                      { "name": "tokenAddress", "type": "address" },
                      { "name": "tokenId", "type": "uint256" },
                      { "name": "amount", "type": "uint256" },
                      { "name": "itemPrice", "type": "uint256" },
                      { "name": "nonce", "type": "uint256" },
                      { "name": "expiration", "type": "uint256" },
                      { "name": "marketplaceFeeNumerator", "type": "uint256" },
                      { "name": "maxRoyaltyFeeNumerator", "type": "uint256" },
                      { "name": "requestedFillAmount", "type": "uint256" },
                      { "name": "minimumFillAmount", "type": "uint256" }
                    ]
                  },
                  "value": {
                    "protocol": 1,
                    "cosigner": "0x0000000000000000000000000000000000000000",
                    "buyer": maker,
                    "beneficiary": maker,
                    "marketplace": MAGICEDEN_CONFIG.feeRecipient,
                    "fallbackRoyaltyRecipient": "0x0000000000000000000000000000000000000000",
                    "paymentMethod": currency,
                    "tokenAddress": collection,
                    "tokenId": "0",
                    "amount": weiPrice,
                    "itemPrice": weiPrice,
                    "nonce": collectionNonce,
                    "expiration": expirationTime,
                    "marketplaceFeeNumerator": "50",
                    "maxRoyaltyFeeNumerator": "0",
                    "requestedFillAmount": quantity.toString(),
                    "minimumFillAmount": "1"
                  },
                  "primaryType": "CollectionOfferApproval"
                },
                "post": {
                  "endpoint": "/order/v4",
                  "method": "POST",
                  "body": {}
                }
              }
            }
          ]
        }
      ],
      "errors": []
    };
  }
}

/**
 * Signs the order data.
 * @param wallet - The wallet of the offerer.
 * @param signData - The data to be signed.
 * @returns The signature.
 */
async function signOrderData(taskId: string, wallet: ethers.Wallet, signData: any, trait?: Trait): Promise<string> {

  if (!signData) {
    throw new Error('Asset does not exist.');
  }

  // Use cached domain and types for better performance
  const domain = trait ? domainCache : (signData.domain || domainCache);
  const types = trait ? typesCache : signData.types;

  try {
    const convertedValue = convertBigNumbers(signData.value);

    const signature = await wallet.signTypedData(
      domain,
      types,
      convertedValue
    );

    return signature;
  } catch (error) {
    throw error;
  }
}

/**
 * Sends the signed order data to the API.
 * @param signature - The signature of the order data.
 * @param data - The order data.
 * @param slug - Collection slug
* @param expiry - bid expiry in seconds
* 
* 
* @param trait - Collection trait
 * @returns The response from the API.
 */
async function sendSignedOrderData(
  taskId: string,
  offerPrice: string | number,
  decimal: number,
  currencySymbol: string,
  bidCount: string,
  signature: string,
  data: any,
  slug: string,
  expiry: number,
  trait?: Trait,
  tokenId?: number | string,
  cachedTask?: any // Accept cached task
) {
  const endpoint = trait || tokenId
    ? `${BASE_URL}/magiceden/v3/rtp/ethereum/order/v3`
    : `${BASE_URL}/magiceden/v3/rtp/ethereum/order/v4`;

  // Define url variable at function scope so it's accessible in error handler
  let url: string = endpoint;

  try {
    const [_parsedTaskId] = bidCount.split(":");

    const task = cachedTask || currentTasks.find((task) =>
      task.contract.slug.toLowerCase() === slug.toLowerCase() &&
      task.selectedMarketplaces.includes("MagicEden")
    );
    if (!task) return;

    // check if the tasks is still active before you make the offer
    if (isTaskBlacklisted(taskId, 'magiceden')) {
      return;
    }
    const currentTask = activeTasks.get(taskId)
    if (!currentTask?.running || !currentTask?.selectedMarketplaces?.map((marketplace) => marketplace.toLowerCase()).includes("magiceden".toLowerCase())) return

    const controller = new AbortController();
    addActiveRequest(taskId, controller);

    try {

      const startTime = Date.now();
      // For collection offers (v4), signature is in the payload, not in query params
      url = (!trait && !tokenId) ? endpoint : `${endpoint}?signature=${encodeURIComponent(signature)}`;

      const { data: offerResponse } = await limiter.schedule(() =>
        axiosInstance.post(
          url,
          data,
          {
            headers: {
              'content-type': 'application/json',
              'x-api-key': API_KEY,
            },
            signal: controller.signal,
            "axios-retry": { retries: 0 } // Disable automatic retries to prevent duplicate error logging
          }
        )
      );
      const responseTime = Date.now() - startTime;
      const successMessage = tokenId ? `🎉 TOKEN OFFER POSTED TO MAGICEDEN SUCCESSFULLY FOR: ${slug.toUpperCase()} TOKEN: ${tokenId} ${Number(offerPrice) / Math.pow(10, decimal)} ${currencySymbol} 🎉` :
        trait ?
          `🎉 TRAIT OFFER POSTED TO MAGICEDEN SUCCESSFULLY FOR: ${slug.toUpperCase()} TRAIT: ${JSON.stringify(trait)} ${Number(offerPrice) / Math.pow(10, decimal)} ${currencySymbol} 🎉`
          : `🎉 OFFER POSTED TO MAGICEDEN SUCCESSFULLY FOR: ${slug.toUpperCase()} ${Number(offerPrice) / Math.pow(10, decimal)} ${currencySymbol} 🎉`

      const identifier = trait ? `${trait.attributeKey}:${trait.attributeValue}` : tokenId ? tokenId : 'collection';

      const payload = JSON.stringify(offerResponse);

      const task = currentTasks.find((task) => task.contract.slug.toLowerCase() === slug.toLowerCase() && task.selectedMarketplaces.includes("MagicEden"))
      if (!task) return

      // Batch database operations
      orderBatchQueue.push({
        slug: slug,
        orderHash: payload,
        expiresAt: new Date(Date.now() + expiry * 1000),
        offerPrice: offerPrice.toString(),
        identifier: identifier.toString(),
        taskId: taskId,
        marketplace: "magiceden"
      });

      // Process batch if not already scheduled
      if (!batchProcessor) {
        batchProcessor = setTimeout(async () => {
          const batch = [...orderBatchQueue];
          orderBatchQueue.length = 0;
          batchProcessor = null;

          if (batch.length > 0) {
            try {
              await withRetry(async () => {
                return await Order.insertMany(batch);
              });
            } catch (error) {
              // Log error but don't fail the bid
              await logger.logDatabaseError('Failed to batch create orders', {
                databaseOperation: DatabaseOperation.CREATE,
                collection_name: 'orders',
                affectedRows: batch.length,
                error: error as Error
              });
            }
          }
        }, 100); // Batch every 100ms
      }
      trackBidRate("magiceden", taskId)
      // Log successful bid
      console.log(MAGENTA, successMessage, RESET);

      if (!task) return
      return offerResponse;
    } finally {
      const controllers = activeRequests.get(taskId) || [];
      const index = controllers.indexOf(controller);
      if (index >= 0) {
        controllers.splice(index, 1);
      }
      if (controllers.length === 0) {
        activeRequests.delete(taskId);
      }
    }
  } catch (error: any) {
    if (axios.isCancel(error)) {
      return null;
    }
    // Enhanced error logging
    const errorMessage = extractMagicEdenErrorMessage(error);
    await logger.logMagicEdenBidError('Failed to send signed order data to Magic Eden', {
      taskId,
      collection: slug,
      tokenId: tokenId?.toString(),
      traitType: trait?.attributeKey,
      traitValue: trait?.attributeValue,
      operation: 'send_signed_order',
      bidAmount: offerPrice,
      currency: currencySymbol,
      error: error,
      errorCode: error?.response?.status,
      errorMessage: errorMessage
    });
    throw error
  }
}

/**
 * Submits signed order data.
 * @param order - The order data.
 * @param wallet - The wallet of the offerer.
 * @param trait - Optional Trait
 * @param slug - Collection slug

 */
export async function submitSignedOrderData(
  taskId: string,
  offerPrice: string | number,
  decimal: number,
  currencySymbol: string,
  bidCount: string,
  order: CreateBidData,
  wallet: ethers.Wallet,
  slug: string,
  expiry: number,
  trait?: Trait,
  tokenId?: number | string,
  cachedTask?: any // Accept cached task
) {
  const task = cachedTask || currentTasks.find((task) =>
    task.contract.slug.toLowerCase() === slug.toLowerCase() &&
    task.selectedMarketplaces.includes("MagicEden")
  );
  if (!task) return;

  // Set source for all order types
  if (order?.steps) {
    for (const step of order.steps) {
      if (!step.items) continue;

      for (const item of step.items) {
        // Set source
        if (item.data?.post?.body) {
          item.data.post.body.source = MAGICEDEN_CONFIG.domain;
        }
      }
    }
  }
  const signData = order?.steps
    ?.find((step) => step.id === "order-signature")
    ?.items?.[0]?.data?.sign || null;

  const signature = await signOrderData(taskId, wallet, signData, trait);
  let data: any;

  if (trait) {
    const orderSignatureStep = order.steps?.find(step => step.id === "order-signature") as any;
    const attributeStep = order.steps?.find(step => step.id === "order-signature") as any
    const attribute = attributeStep?.items?.[0]?.data?.post?.body?.attribute;
    const valueObject = orderSignatureStep?.items?.[0]?.data?.sign?.value;

    data = {
      "order": {
        "kind": "seaport-v1.6",
        "data": {
          "kind": "token-list",
          ...valueObject,
          "signature": "0x0000000000000000000000000000000000000000000000000000000000000000"
        }
      },
      "attribute": {
        "collection": attribute.collection,
        "key": trait.attributeKey,
        "value": trait.attributeValue
      },
      "isNonFlagged": false,
      "orderbook": "reservoir",
      "source": MAGICEDEN_CONFIG.domain
    };
  } else if (tokenId) {
    data = order?.steps
      ?.find((step) => step.id === "order-signature")
      ?.items?.[0]?.data?.post.body;
  } else {


    if (signData) {
      const payload: any = signData.value;

      // Validate critical fields for collection offers
      const requiredFields = ['buyer', 'protocol', 'tokenAddress', 'amount', 'nonce', 'expiration', 'paymentMethod'];
      requiredFields.forEach(field => {
      });

      const { buyer, ...rest } = payload;

      // Check if this order came from v5 API (collection offers)
      const isV5Order = order?.steps?.some((step: any) =>
        step.id === "order-signature" &&
        step.items?.[0]?.data?.post?.body
      );

      if (isV5Order) {
        // For v5 API collection offers, use the post body from the order steps
        const postBody = order?.steps
          ?.find((step: any) => step.id === "order-signature")
          ?.items?.[0]?.data?.post?.body;

        if (postBody) {
          data = postBody;
          // Parse signature and update the r, s, v values in the post body
          const sig = signature.startsWith('0x') ? signature.slice(2) : signature;
          const r = `0x${sig.slice(0, 64)}`;
          const s = `0x${sig.slice(64, 128)}`;
          const v = parseInt(sig.slice(128, 130), 16);

          // Update the signature fields in the data
          if (data.items?.[0]?.order?.data) {
            data.items[0].order.data.r = r;
            data.items[0].order.data.s = s;
            data.items[0].order.data.v = v;
          }
        }
      } else {
        // For hardcoded collection offers (shouldn't happen, but as fallback)

        // Parse the signature to extract r, s, v values
        const sig = signature.startsWith('0x') ? signature.slice(2) : signature;
        const r = `0x${sig.slice(0, 64)}`;
        const s = `0x${sig.slice(64, 128)}`;
        const v = parseInt(sig.slice(128, 130), 16);

        data = {
          items: [
            {
              order: {
                kind: "payment-processor-v2",
                data: {
                  kind: "collection-offer-approval",
                  sellerOrBuyer: buyer,
                  ...rest,
                  r: r,
                  s: s,
                  v: v,
                },
              },
              orderbook: "reservoir",
            },
          ],
          source: MAGICEDEN_CONFIG.domain,
        };
      }
    } else {
      await logger.logMagicEdenBidError('Sign data not found in order steps', {
        taskId,
        collection: slug,
        operation: 'sign_order',
        errorCode: 'MISSING_SIGN_DATA'
      });
      if (process.env.NODE_ENV !== 'production') {
        console.error('Sign data not found in order steps.');
      }
    }

  }

  const result = await sendSignedOrderData(
    taskId,
    offerPrice,
    decimal,
    currencySymbol,
    bidCount,
    signature,
    data,
    slug,
    expiry,
    trait,
    tokenId,
    task // Pass the cached task
  );
  return result;

}

export async function cancelMagicEdenBid(orderIds: string[], privateKey: string, taskId: string) {
  try {
    if (!orderIds?.length) return;
    const processedOrderIds = await Promise.all(orderIds?.map(async (orderId: any) => {
      try {
        const parsed = JSON.parse(orderId);
        return parsed.orderId || orderId;
      } catch {
        // If parsing fails, it's already a plain orderId string
        return orderId;
      }
    }));

    if (!processedOrderIds.length) return;

    const { data } = await limiter.schedule(() => axiosInstance.post<MagicEdenCancelOfferCancel>(
      `${BASE_URL}/magiceden/v3/rtp/ethereum/execute/cancel/v3`,
      { orderIds: processedOrderIds },
      {
        headers: {
          'content-type': 'application/json',
          'x-api-key': API_KEY
        }
      }
    ));

    const cancelStep = data?.steps?.find((step) => step.id === "cancellation-signature")
    const cancelItem = cancelStep?.items[0]?.data?.sign
    const cancelData = cancelItem ? cancelItem : {
      "signatureKind": "eip712",
      "domain": {
        "name": "Off-Chain Cancellation",
        "version": "1.0.0",
        "chainId": 1
      },
      "types": { "OrderHashes": [{ "name": "orderHashes", "type": "bytes32[]" }] },
      "value": {
        "orderHashes": processedOrderIds
      },
      "primaryType": "OrderHashes"
    }
    const signature = await signCancelOrder(taskId, cancelData, privateKey);
    if (!signature) {
      await logger.logMagicEdenBidError('Failed to generate signature for cancel order', {
        taskId,
        operation: 'cancel_bid_signature',
        orderIds: processedOrderIds,
        errorCode: 'SIGNATURE_GENERATION_FAILED'
      });
      if (process.env.NODE_ENV !== 'production') {
        console.error('Failed to generate signature for cancel order');
      }
      return;
    }
    const body = cancelStep?.items[0].data.post.body
    const cancelBody = cancelItem ? body : {
      orderIds: processedOrderIds
      ,
      orderKind: 'payment-processor-v2'
    }

    const { data: cancelResponse } = await limiter.schedule(() => axiosInstance.post(
      `${BASE_URL}/magiceden/v3/rtp/ethereum/execute/cancel-signature/v1?signature=${signature}`,
      cancelBody,
      {
        headers: {
          'content-type': 'application/json',
          'x-api-key': API_KEY
        }
      }
    ))
    orderIds.forEach(() => {
      decrementBidCount('magiceden', taskId);
    });

    // Track cancellation completion - MagicEden cancels multiple orders at once
    cancellationTracker.recordCompletion(taskId, orderIds.length, 'magiceden');

    // Update cancellation percentage using centralized function
    await updateTaskCancellationPercentage(taskId, undefined, 'magiceden');

  } catch (error: any) {
    const errorMessage = error?.response?.data?.message || error?.message || error;

    // Check if this is a "No matching order(s)" error - this means the order was already cancelled
    if (errorMessage?.includes('No matching order') || error?.response?.status === 404) {

      // Still track as completed since the orders are effectively cancelled
      orderIds.forEach(() => {
        decrementBidCount('magiceden', taskId);
      });

      // Track cancellation completion
      cancellationTracker.recordCompletion(taskId, orderIds.length, 'magiceden');

      // Update cancellation percentage
      await updateTaskCancellationPercentage(taskId, undefined, 'magiceden');

      // Don't throw error for already cancelled orders
      return;
    }

    await logger.logMagicEdenBidError('Failed to cancel MagicEden bid', {
      taskId,
      operation: 'cancel_bid',
      orderIds: orderIds,
      error: error,
      errorCode: error?.response?.status,
      errorMessage: errorMessage
    });
    throw error;
  }
}

async function signCancelOrder(taskId: string, cancelItem: any | undefined, privateKey: string) {

  if (!cancelItem) return

  const wallet = getWallet(privateKey);

  const signature = await wallet.signTypedData(
    cancelItem.domain,
    cancelItem.types,
    convertBigNumbers(cancelItem.value)
  );

  return signature;

}

export async function fetchMagicEdenOffer(type: "COLLECTION" | "TRAIT" | "TOKEN", contractAddress: string, identifier?: string | {
  attributeKey: string;
  attributeValue: string;
}) {

  if (type === "COLLECTION") {
    const { data } = await limiter?.schedule(() =>
      axiosInstance.get<MagicedenCollectionResponse>(`${BASE_URL}/magiceden/v3/rtp/ethereum/collections/${contractAddress}/bids/v1?type=collection`, {
        headers: {
          'content-type': 'application/json',
          'x-api-key': API_KEY
        }
      })
    );

    const offers = data?.orders.map((order) => {
      return {
        amount: order?.price?.amount?.raw,
        owner: order?.maker,
        currency: order?.price.currency.symbol.toUpperCase(),
        usd: order?.price?.amount?.usd,
        decimal: order.price.currency.decimals,
        eth: order.price.amount.native
      }
    })?.sort((a, b) => Number(b?.usd) - Number(a?.usd))

    // Default offer structure
    const defaultOffer = {
      amount: 0,
      owner: '0x0000000000000000000000000000000000000000',
      currency: 'ETH',
      usd: 0,
      decimal: 18,
      eth: 0
    };

    // Ensure we always return at least 2 offers
    if (!offers || offers.length === 0) {
      return [defaultOffer, defaultOffer];
    } else if (offers.length === 1) {
      return [offers[0], defaultOffer];
    }

    return offers
  } else if (type === "TOKEN") {
    const tasks = [
      () => limiter.schedule(() => axiosInstance.get<MagicedenCollectionResponse>(`${BASE_URL}/magiceden/v3/rtp/ethereum/orders/bids/v6?token=${contractAddress}:${identifier}&sortBy=price&status=active&excludeEOA=false&limit=2`, {
        headers: {
          'content-type': 'application/json',
          'x-api-key': API_KEY
        }
      })),

      () => limiter.schedule(() =>
        axiosInstance.get<MagicedenCollectionResponse>(`${BASE_URL}/magiceden/v3/rtp/ethereum/collections/${contractAddress}/bids/v1?type=collection&sortBy=price&status=active&excludeEOA=false&limit=2`, {
          headers: {
            'content-type': 'application/json',
            'x-api-key': API_KEY
          }
        })
      )
    ]

    const [tokenResponse, collectionResponse] = await pQueue.addAll(tasks, {
      throwOnTimeout: true,
    });
    const data = [...tokenResponse.data.orders, ...collectionResponse.data.orders]
    const offers = data.map((order) => {
      return {
        amount: order?.price?.amount?.raw,
        owner: order?.maker,
        currency: order?.price.currency.symbol.toUpperCase(),
        usd: order?.price?.amount?.usd,
        decimal: order.price.currency.decimals,
        eth: order.price.amount.native
      }
    })?.sort((a, b) => Number(b.usd) - Number(a.usd))

    // Default offer structure
    const defaultOffer = {
      amount: 0,
      owner: '0x0000000000000000000000000000000000000000',
      currency: 'ETH',
      usd: 0,
      decimal: 18,
      eth: 0
    };

    // Ensure we always return at least 2 offers
    if (!offers || offers.length === 0) {
      return [defaultOffer, defaultOffer];
    } else if (offers.length === 1) {
      return [offers[0], defaultOffer];
    }

    return offers

  } else if (type === "TRAIT" && typeof identifier === "object" && identifier.attributeKey && identifier.attributeValue) {
    const tasks = [
      () => limiter.schedule(() => axiosInstance.get<MagicedenCollectionResponse>(`${BASE_URL}/magiceden/v3/rtp/ethereum/collections/${contractAddress}/bids/v1?type=collection&sources=magiceden.io&sources=magiceden.us&limit=2&sortBy=price`, {
        headers: {
          'content-type': 'application/json',
          'x-api-key': API_KEY
        }
      })),
      () => {
        const URL = `${BASE_URL}/magiceden/v3/rtp/ethereum/orders/bids/v5?collection=${contractAddress}&attribute%5B${identifier?.attributeKey}%5D=${identifier.attributeValue}&sortBy=price&status=active&limit=2`;
        return limiter.schedule(() => axiosInstance.get<MagicEdenTraitOfferResponse>(
          URL,
          {
            headers: {
              'x-api-key': API_KEY,
            }
          }
        ));
      }
    ];

    const [collectionResponse, traitResponse] = await pQueue.addAll(tasks, {
      throwOnTimeout: true,
    });
    const data = [...collectionResponse.data.orders, ...traitResponse.data.orders];
    const offers = data.map((order) => {
      return {
        amount: order?.price?.amount?.raw,
        owner: order?.maker,
        currency: order?.price.currency.symbol.toUpperCase(),
        usd: order?.price?.amount?.usd,
        decimal: order.price.currency.decimals,
        eth: order.price.amount.native
      }
    })?.sort((a, b) => Number(b.usd) - Number(a.usd))
    // Default offer structure
    const defaultOffer = {
      amount: 0,
      owner: '0x0000000000000000000000000000000000000000',
      currency: 'ETH',
      usd: 0,
      decimal: 18,
      eth: 0
    };

    // Ensure we always return at least 2 offers
    if (!offers || offers.length === 0) {
      return [defaultOffer, defaultOffer];
    } else if (offers.length === 1) {
      return [offers[0], defaultOffer];
    }

    return offers
  }
}

export async function fetchMagicEdenTraitFloor(contractAddress: string, trait: { attributeKey: string; attributeValue: string }) {
  const CACHE_TTL = 120; // 2 minutes cache for trait data
  const cacheKey = `magiceden:trait:${contractAddress}:${trait.attributeKey}:${trait.attributeValue}`;

  // Try to get from cache first
  try {
    const redis = redisClient.getClient();
    const cachedFloor = await redis.get(cacheKey);
    if (cachedFloor) {
      return JSON.parse(cachedFloor);
    }
  } catch (error) {
    // Log cache error but continue to fetch fresh data
    if (process.env.NODE_ENV !== 'production') {
      console.error('Error reading MagicEden trait floor from cache:', error);
    }
    await logger.logDatabaseError('Error reading MagicEden trait floor from Redis cache', {
      databaseOperation: DatabaseOperation.READ,
      collection_name: 'redis_cache',
      operation: 'get_magiceden_trait_floor',
      contractAddress,
      cacheKey,
      error: error as Error,
      errorMessage: error instanceof Error ? error.message : String(error)
    });
  }

  try {
    const queryParams = new URLSearchParams({
      collection: contractAddress,
      includeQuantity: "true",
      includeLastSale: "true",
      excludeSpam: "true",
      excludeBurnt: "true",
      sortBy: "floorAskPrice",
      sortDirection: "asc",
      limit: "1",
      includeAttributes: "false",
      includeTopBid: "true",
    });
    queryParams.append(`attributes[${trait.attributeKey}][]`, `${trait.attributeValue}`);
    queryParams.append("excludeSources[]", "nftx.io");
    queryParams.append("excludeSources[]", "sudoswap.xyz");
    const fullUrl = `${BASE_URL}/magiceden/v3/rtp/ethereum/tokens/v7?${queryParams.toString()}`;
    const { data } = await limiter.schedule(() => axiosInstance.get<TokenDataList>(fullUrl, {
      headers: {
        'x-api-key': API_KEY,
      },
    }))
    const floor = data?.tokens[0]?.market?.floorAsk?.price?.amount?.native

    // Cache the result
    if (floor !== undefined) {
      try {
        const redis = redisClient.getClient();
        await redis.setex(cacheKey, CACHE_TTL, JSON.stringify(floor));
      } catch (error) {
        // Log cache error but don't fail the request
        if (process.env.NODE_ENV !== 'production') {
          console.error('Error caching MagicEden trait floor:', error);
        }
        await logger.logDatabaseError('Error writing MagicEden trait floor to Redis cache', {
          databaseOperation: DatabaseOperation.CREATE,
          collection_name: 'redis_cache',
          operation: 'set_magiceden_trait_floor',
          contractAddress,
          cacheKey,
          error: error as Error,
          errorMessage: error instanceof Error ? error.message : String(error)
        });
      }
    }

    return floor
  } catch (error) {
    if (error) {
      if (process.env.NODE_ENV !== 'production') {
        console.error('MagicEden trait floor error:', error instanceof Error ? error.message : String(error));
      }
      await logger.logMagicEdenCollectionError('Failed to fetch MagicEden trait floor', {
        contractAddress,
        traitKey: trait.attributeKey,
        traitValue: trait.attributeValue,
        operation: 'fetch_trait_floor',
        error: error as Error,
        errorMessage: error instanceof Error ? error.message : String(error)
      });
    }
  }
}

export async function fetchMagicEdenCollectionStats(contractAddress: string) {
  const CACHE_TTL = 30; // 30 seconds cache
  const cacheKey = `magiceden:stats:${contractAddress}`;
  const lockKey = `stats:${contractAddress}`;

  // Try to get from cache first
  try {
    const redis = redisClient.getClient();
    const cachedStats = await redis.get(cacheKey);
    if (cachedStats) {
      return JSON.parse(cachedStats);
    }
  } catch (error) {
    await logger.logDatabaseError('Error reading MagicEden stats from Redis cache', {
      databaseOperation: DatabaseOperation.READ,
      collection_name: 'redis_cache',
      operation: 'get_magiceden_stats',
      contractAddress,
      cacheKey,
      error: error as Error,
      errorMessage: error instanceof Error ? error.message : String(error)
    });
    if (process.env.NODE_ENV !== 'production') {
      console.error('Error reading from cache:', error);
    }
  }

  // If not in cache, try to acquire lock and fetch
  const result = await lockManager.withLock(lockKey, async () => {
    // Double check cache after acquiring lock
    try {
      const redis = redisClient.getClient();
      const cachedStats = await redis.get(cacheKey);
      if (cachedStats) {
        return JSON.parse(cachedStats);
      }
    } catch (error) {
      await logger.logDatabaseError('Error reading MagicEden stats from Redis cache after lock', {
        databaseOperation: DatabaseOperation.READ,
        collection_name: 'redis_cache',
        operation: 'get_magiceden_stats_after_lock',
        contractAddress,
        cacheKey,
        error: error as Error,
        errorMessage: error instanceof Error ? error.message : String(error)
      });
      if (process.env.NODE_ENV !== 'production') {
        console.error('Error reading from cache after lock:', error);
      }
    }

    const queryParams = {
      includeQuantity: true,
      includeLastSale: true,
      excludeSpam: true,
      excludeBurnt: true,
      collection: contractAddress,
      sortBy: 'floorAskPrice',
      sortDirection: 'asc',
      limit: 1,
      includeAttributes: false,
    };

    const { data } = await limiter.schedule(() =>
      axiosInstance.get(`${BASE_URL}/magiceden/v3/rtp/ethereum/tokens/v7`, {
        params: queryParams,
        headers: {
          'x-api-key': API_KEY
        }
      })
    );
    const stats = data?.tokens[0]?.market?.floorAsk?.price?.amount?.decimal || 0;
    // Cache the result
    if (stats !== undefined) {
      try {
        const redis = redisClient.getClient();
        await redis.set(cacheKey, JSON.stringify(stats), 'EX', CACHE_TTL);
      } catch (error) {
        await logger.logDatabaseError('Error writing MagicEden stats to Redis cache', {
          databaseOperation: DatabaseOperation.CREATE,
          collection_name: 'redis_cache',
          operation: 'set_magiceden_stats',
          contractAddress,
          cacheKey,
          error: error as Error,
          errorMessage: error instanceof Error ? error.message : String(error)
        });
        if (process.env.NODE_ENV !== 'production') {
          console.error('Error writing to cache:', error);
        }
      }
    }

    return stats;

  });
  if (!result) {
    // If we couldn't acquire the lock, wait briefly and try cache again
    await new Promise(resolve => setTimeout(resolve, 100));
    try {
      const redis = redisClient.getClient();
      const cachedStats = await redis.get(cacheKey);
      if (cachedStats) {
        return JSON.parse(cachedStats);
      }
    } catch (error) {
      await logger.logDatabaseError('Error reading from cache after lock failure', {
        databaseOperation: DatabaseOperation.READ,
        collection_name: 'redis_cache',
        operation: 'get_magiceden_stats_retry',
        contractAddress,
        cacheKey,
        error: error as Error,
        errorMessage: error instanceof Error ? error.message : String(error)
      });
      if (process.env.NODE_ENV !== 'production') {
        console.error('Error reading from cache after lock failure:', error);
      }
    }
    await logger.logMagicEdenCollectionError('Failed to fetch collection stats', {
      contractAddress,
      operation: 'fetch_collection_stats',
      errorCode: 'STATS_FETCH_FAILED'
    });
    throw new Error('Failed to fetch collection stats');
  }
  return result;
}

export async function fetchMagicEdenTokens(taskId: string, collectionId: string, limit?: number) {
  const CACHE_TTL = 60; // 1 minute cache for token listings
  const cacheKey = `magiceden:tokens:${collectionId}:${limit || 'all'}`;
  const lockKey = `tokens:${collectionId} `;

  // Try to get from cache first
  try {
    const redis = redisClient.getClient();
    const cachedTokens = await redis.get(cacheKey);
    if (cachedTokens) {
      return JSON.parse(cachedTokens);
    }
  } catch (error) {
    // Log cache error but continue to fetch fresh data
    if (process.env.NODE_ENV !== 'production') {
      console.error('Error reading MagicEden tokens from cache:', error);
    }
    await logger.logDatabaseError('Error reading MagicEden tokens from Redis cache', {
      databaseOperation: DatabaseOperation.READ,
      collection_name: 'redis_cache',
      operation: 'get_magiceden_tokens',
      collectionId,
      cacheKey,
      error: error as Error,
      errorMessage: error instanceof Error ? error.message : String(error)
    });
  }

  return await lockManager.withLock(
    lockKey,
    async () => {
      // Double-check cache after acquiring lock
      try {
        const redis = redisClient.getClient();
        const cachedTokens = await redis.get(cacheKey);
        if (cachedTokens) {
          return JSON.parse(cachedTokens);
        }
      } catch (error) {
        // Continue if cache check fails
      }

      const params: any = {
        excludeSpam: true,
        excludeBurnt: true,
        collection: collectionId,
        sortBy: "floorAskPrice",
        sortDirection: "asc",
        limit: 50,
        excludeSources: ["nftx.io", "sudoswap.xyz"],
        normalizeRoyalties: false,
        continuation: null
      };
      const url = `${BASE_URL}/magiceden/v3/rtp/ethereum/tokens/v7`;
      const allTokens: number[] = [];

      let totalFetched = 0;
      if (!limit) return
      do {
        const { data } = await limiter.schedule(() => axiosInstance.get<TokenResponseMagiceden>(
          url,
          {
            headers: {
              accept: "application/json",
              "x-api-key": API_KEY,
            },
            params
          }
        ))

        const tokens = data?.tokens?.map((item: any) => +item?.token?.tokenId);
        allTokens.push(...tokens);
        totalFetched += tokens.length;
        params.continuation = data?.continuation;
      } while (params.continuation && totalFetched < limit);

      const result = allTokens.slice(0, limit);

      // Cache the result
      try {
        const redis = redisClient.getClient();
        await redis.setex(cacheKey, CACHE_TTL, JSON.stringify(result));
      } catch (error) {
        // Log cache error but don't fail the request
        if (process.env.NODE_ENV !== 'production') {
          console.error('Error caching MagicEden tokens:', error);
        }
        await logger.logDatabaseError('Error writing MagicEden tokens to Redis cache', {
          databaseOperation: DatabaseOperation.CREATE,
          collection_name: 'redis_cache',
          operation: 'set_magiceden_tokens',
          collectionId,
          cacheKey,
          error: error as Error,
          errorMessage: error instanceof Error ? error.message : String(error)
        });
      }

      return result;

    },
    60 // Lock TTL in seconds - longer for token fetching since it's paginated
  );
}
interface TokenMagiceden {
  chainId: number;
  contract: string;
  tokenId: string;
  name: string;
  description: string;
  image: string;
  imageSmall: string;
  imageLarge: string;
  metadata: {
    imageOriginal: string;
    imageMimeType: string;
    tokenURI: string;
  };
  media: any; // Adjust type if you know the structure of media
  kind: string;
  isFlagged: boolean;
  isSpam: boolean;
  isNsfw: boolean;
  metadataDisabled: boolean;
  lastFlagUpdate: string;
  lastFlagChange: string;
  supply: string;
  remainingSupply: string;
  rarity: number;
  rarityRank: number;
  collection: {
    id: string;
    name: string;
    image: string;
    slug: string;
    symbol: string;
    creator: string;
    tokenCount: number;
    metadataDisabled: boolean;
    floorAskPrice: {
      currency: {
        contract: string;
        name: string;
        symbol: string;
        decimals: number;
      };
      amount: {
        raw: string;
        decimal: number;
        usd: number;
        native: number;
      };
    };
  };
  owner: string;
  mintedAt: string;
  createdAt: string;
  decimals: number | null;
  mintStages: any[]; // Adjust type if you know the structure of mintStages
}

interface MarketMagiceden {
  floorAsk: {
    id: string;
    price: {
      currency: {
        contract: string;
        name: string;
        symbol: string;
        decimals: number;
      };
      amount: {
        raw: string;
        decimal: number;
        usd: number;
        native: number;
      };
    };
    maker: string;
    validFrom: number;
    validUntil: number;
    source: {
      id: string;
      domain: string;
      name: string;
      icon: string;
      url: string;
    };
  };
}

interface TokenResponseMagiceden {
  tokens: {
    token: TokenMagiceden;
    market: MarketMagiceden;
    updatedAt: string;
    media: {
      image: string;
      imageMimeType: string;
    };
  }[];
  continuation: string;
}

interface MagicEdenCancelOfferCancel {
  steps: StepCancel[];
}

interface StepCancel {
  id: string;
  action: string;
  description: string;
  kind: string;
  items: ItemCancel[];
}

interface ItemCancel {
  status: string;
  orderIds: string[];
  data: DataCancel;
}

interface DataCancel {
  sign: SignCancel;
  post: PostCancel;
}

interface SignCancel {
  signatureKind: string;
  domain: DomainCancel;
  types: any;
  value: ValueCancel;
}

interface DomainCancel {
  name: string;
  version: string;
  chainId: number;
  verifyingContract: string;
}

interface ValueCancel {
  orderHashes: string[];
}

interface PostCancel {
  endpoint: string;
  method: string;
  body: BodyCancel;
}

interface BodyCancel {
  orderIds: string[];
  orderKind: string;
}

interface StepItem {
  status?: string;
  orderIndexes?: number[];
  data?: {
    sign: {
      signatureKind: string;
      domain: {
        name: string;
        version: string;
        chainId: number;
        verifyingContract: string;
      };
      types: {
        CollectionOfferApproval: { name: string; type: string; }[];
      };
      value: {
        protocol?: number;
        cosigner?: string;
        buyer?: string;
        consideration?: Array<{
          recipient: string;
          [key: string]: any;
        }>;
        r: string;
        s: string;
        v: number;
      };
      primaryType: string;
    };
    post: {
      endpoint: string;
      method: string;
      body: {
        order?: {
          data: {
            consideration?: Array<{
              recipient: string;
              [key: string]: any;
            }>;
          };
        };
        source: string;
      };
    };
  };
  [key: string]: any; // Allow additional properties
}

interface Step {
  id: string;
  action: string;
  description: string;
  kind: string;
  items: StepItem[];
  [key: string]: any; // Allow additional properties

}

interface CreateBidData {
  steps: Step[];
  errors: any[];
  [key: string]: any; // Allow additional properties

}

interface Trait {
  attributeKey: string;
  attributeValue: string;
};

interface MagicedenCollectionOrder {
  id: string;
  kind: string;
  side: string;
  status: string;
  tokenSetId: string;
  tokenSetSchemaHash: string;
  contract: string;
  contractKind: string;
  maker: string;
  taker: string;
  price: {
    currency: {
      contract: string;
      name: string;
      symbol: string;
      decimals: number;
    };
    amount: {
      raw: string;
      decimal: number;
      usd: number;
      native: number;
    };
    netAmount: {
      raw: string;
      decimal: number;
      usd: number;
      native: number;
    };
  };
  validFrom: number;
  validUntil: number;
  quantityFilled: number;
  quantityRemaining: number;
  criteria: {
    kind: string;
    data: {
      collection: {
        id: string;
        name: string;
        image: string;
      };
    };
  };
  source: {
    id: string;
    domain: string;
    name: string;
    icon: string;
    url: string;
  };
  feeBps: number;
  feeBreakdown: Array<{
    kind: string;
    recipient: string;
    bps: number;
  }>;
  expiration: number;
  isReservoir: boolean | null;
  createdAt: string;
  updatedAt: string;
  originatedAt: string | null;
  isNativeOffChainCancellable: boolean;
  depth: Array<{
    price: number;
    quantity: number;
  }>;
}

export interface MagicedenCollectionResponse {
  orders: MagicedenCollectionOrder[];
  continuation: string;
}

export interface MagicedenTraitToken {
  chainId: number;
  contract: string;
  tokenId: string;
  name: string;
  description: string;
  image: string;
  imageSmall: string;
  imageLarge: string;
  metadata: {
    imageOriginal: string;
    imageMimeType: string;
    tokenURI: string;
  };
  media: any; // Adjust type if you know the structure
  kind: string;
  isFlagged: boolean;
  isSpam: boolean;
  isNsfw: boolean;
  metadataDisabled: boolean;
  lastFlagUpdate: string;
  lastFlagChange: string;
  supply: string;
  remainingSupply: string;
  rarity: number;
  rarityRank: number;
  collection: {
    id: string;
    name: string;
    image: string;
    slug: string;
    symbol: string;
    creator: string;
    tokenCount: number;
    metadataDisabled: boolean;
    floorAskPrice: {
      currency: {
        contract: string;
        name: string;
        symbol: string;
        decimals: number;
      };
      amount: {
        raw: string;
        decimal: number;
        usd: number;
        native: number;
      };
    };
  };
  owner: string;
  mintedAt: string;
  createdAt: string;
  decimals: number | null;
  mintStages: any[]; // Adjust type if you know the structure
}

export interface MagicedenTraitMarket {
  floorAsk: {
    id: string;
    price: {
      currency: {
        contract: string;
        name: string;
        symbol: string;
        decimals: number;
      };
      amount: {
        raw: string;
        decimal: number;
        usd: number;
        native: number;
      };
    };
    maker: string;
    validFrom: number;
    validUntil: number;
    source: {
      id: string;
      domain: string;
      name: string;
      icon: string;
      url: string;
    };
  };
}

export interface MagicedenTraitTokenResponse {
  tokens: {
    token: MagicedenTraitToken;
    market: MagicedenTraitMarket;
    updatedAt: string;
    media: {
      image: string;
      imageMimeType: string;
    };
  }[];
  continuation: string;
}

// ME_KEY="a86c5cf0-205b-4a6c-a317-0bac8dcea553"

export interface MagicEdenTraitOfferResponse {
  orders: MagicEdenTraitOfferOrder[];
  continuation: string;
}

interface MagicEdenTraitOfferOrder {
  id: string;
  kind: string;
  side: string;
  status: string;
  tokenSetId: string;
  tokenSetSchemaHash: string;
  contract: string;
  contractKind: string;
  maker: string;
  taker: string;
  price: {
    currency: {
      contract: string;
      name: string;
      symbol: string;
      decimals: number;
    };
    amount: {
      raw: string;
      decimal: number;
      usd: number;
      native: number;
    };
    netAmount: {
      raw: string;
      decimal: number;
      usd: number;
      native: number;
    };
  };
  validFrom: number;
  validUntil: number;
  quantityFilled: number;
  quantityRemaining: number;
  criteria: {
    kind: string;
    data: {
      collection: {
        id: string;
      };
    };
  };
  source: {
    id: string;
    domain: string;
    name: string;
    icon: string;
    url: string;
  };
  feeBps: number;
  feeBreakdown: {
    kind: string;
    recipient: string;
    bps: number;
  }[];
  expiration: number;
  isReservoir: boolean | null;
  createdAt: string;
  updatedAt: string;
  originatedAt: string | null;
  isNativeOffChainCancellable: boolean;
}

interface Currency {
  contract: string;
  name: string;
  symbol: string;
  decimals: number;
}

interface PriceAmount {
  raw: string;
  decimal: number;
  usd: number;
  native: number;
}

interface Price {
  currency: Currency;
  amount: PriceAmount;
}
interface Source {
  id: string;
  domain: string;
  name: string;
  icon: string;
  url: string;
}

interface FloorAsk {
  id: string;
  price: Price;
  maker: string;
  validFrom: number;
  validUntil: number;
  quantityFilled: string;
  quantityRemaining: string;
  source: Source;
}

interface Media {
  image: string;
}

export async function cancelMagicedenRequest(taskId: string): Promise<{ success: boolean; cancelledCount: number; error?: string }> {
  // Input validation
  if (!taskId || typeof taskId !== 'string') {
    await logger.logWarning(ErrorCategory.MARKETPLACE, 'Invalid taskId provided to cancelMagicedenRequest', {
      taskId,
      marketplace: 'magiceden',
      marketplaceSubCategory: MarketplaceSubCategory.CANCELLATION
    });
    return { success: false, cancelledCount: 0, error: 'Invalid taskId' };
  }

  const controllers = activeRequests.get(taskId);
  if (!controllers || controllers.length === 0) {

    return { success: false, cancelledCount: 0 };
  }

  try {
    let cancelledCount = 0;

    // Cancel all controllers with timeout protection
    await Promise.all(controllers.map(async (controller) => {
      try {
        const timeoutPromise = new Promise<void>((_, reject) => {
          setTimeout(() => reject(new Error('Abort timeout')), 5000);
        });

        const abortPromise = new Promise<void>((resolve) => {
          controller.abort();
          resolve();
        });

        await Promise.race([abortPromise, timeoutPromise]);
        cancelledCount++;
      } catch (error) {
        await logger.logWarning(ErrorCategory.MARKETPLACE, 'Failed to abort MagicEden request controller', {
          taskId,
          marketplace: 'magiceden',
          marketplaceSubCategory: MarketplaceSubCategory.CANCELLATION,
          errorMessage: error instanceof Error ? error.message : String(error)
        });
      }
    }));

    activeRequests.delete(taskId);

    return { success: true, cancelledCount };
  } catch (error) {
    await logger.logMarketplaceError('Failed to cancel MagicEden requests', {
      taskId,
      marketplace: 'magiceden',
      marketplaceSubCategory: MarketplaceSubCategory.CANCELLATION,
      errorMessage: error instanceof Error ? error.message : String(error)
    });
    return { success: false, cancelledCount: 0, error: error instanceof Error ? error.message : String(error) };
  }
}

function addActiveRequest(taskId: string, controller: AbortController) {
  if (!activeRequests.has(taskId)) {
    activeRequests.set(taskId, []);
  }
  activeRequests.get(taskId)?.push(controller);
}

interface TokenDataList {
  tokens: TokenEntry[];
  continuation: string;
}

interface TokenEntry {
  token: TokenDetails;
  market: MarketDetails;
  updatedAt: string;
  media: Media;
}

interface TokenDetails {
  chainId: number;
  contract: string;
  tokenId: string;
  name: string;
  description: string | null;
  image: string;
  imageSmall: string;
  imageLarge: string;
  metadata: TokenMetadata;
  media: null;
  kind: string;
  isFlagged: boolean;
  isSpam: boolean;
  isNsfw: boolean;
  metadataDisabled: boolean;
  lastFlagUpdate: string;
  lastFlagChange: string;
  supply: string;
  remainingSupply: string;
  rarity: number;
  rarityRank: number;
  collection: CollectionDetails;
  owner: string;
  mintedAt: string;
  createdAt: string;
  decimals: number | null;
  mintStages: any[]; // Could be further typed if schema is known
}

interface TokenMetadata {
  imageOriginal: string;
  imageMimeType: string;
  tokenURI: string;
}

interface CollectionDetails {
  id: string;
  name: string;
  image: string;
  slug: string;
  symbol: string;
  creator: string;
  tokenCount: number;
  metadataDisabled: boolean;
  floorAskPrice: FloorAskPrice;
}

interface FloorAskPrice {
  currency: Currency;
  amount: Amount;
}

interface Amount {
  raw: string;
  decimal: number;
  usd: number;
  native: number;
}

interface MarketDetails {
  floorAsk: FloorAsk;
}

