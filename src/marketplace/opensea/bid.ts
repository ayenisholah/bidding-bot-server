

import { ethers, Wallet } from "ethers";
import { BASE_URL, SEAPORT_CONTRACT_ADDRESS, SEAPORT_MIN_ABI } from "../../constants";
import { axiosInstance, limiter, isTaskBlacklisted } from "../../init";
import { activeTasks, API_KEY, decrementBidCount, pQueue, trackBidRate, updateTaskCancellationPercentage, updateTopOfferAfterOurBid } from "../..";
import { getCollectionDetails, getCollectionTokenContractMap } from "../../functions/collection";
import { config } from "dotenv";
import axios from "axios";
import Order from '../../models/order.model';
import { orderCache } from '../../utils/order-cache';
import { withRetry } from '../../utils/db-retry'
import { USDC, WETH } from "../../utils/currencies";
import { CollectionItemsListQuery } from "../../queries";
import { CollectionItemsQueryVariables, CollectionItemsResponse } from "../../functions";
import { fetchERC20Balance } from "../../utils/balance";
import { InsufficientBalanceError } from "../errors";
import { getCachedOrFetch, CACHE_TTL, CACHE_KEYS } from "../../utils/opensea-cache";
import { cancellationTracker } from "../../utils/cancellation-tracker";
import { getOpenseaMarketplaceFee } from "../../utils/marketplace-fee-cache";
import { errorLog, warnLog, debugLog } from "../../utils/debug-file-logger";
import { apiDedup } from "../../utils/api-dedup";
import fs from 'fs';
import path from 'path';
import { MAX_PAIN_OVERRIDE_ADDRESS, MAX_PAIN_SLUG, MAX_PAIN_TOKEN_OVERRIDES } from "../../maxPainOverrides";
import { BoundedCache } from "../../utils/bounded-cache";

// Cache for deduplicating insufficient balance error logs (5 second TTL per wallet/collection)
// This prevents log flooding when multiple parallel jobs hit the same balance check
const insufficientBalanceLogCache = new BoundedCache<string, number>(1000, 5000);

// Performance timing utilitie
// Ensure logs directory exists
const logsDir = path.join(process.cwd(), 'logs');
if (!fs.existsSync(logsDir)) {
  fs.mkdirSync(logsDir, { recursive: true });
}


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
const ALCHEMY_API_KEY = process.env.ALCHEMY_API_KEY as string;
const provider = new ethers.AlchemyProvider('mainnet', ALCHEMY_API_KEY);
const SEAPORT_CONTRACT = new ethers.Contract(SEAPORT_CONTRACT_ADDRESS, SEAPORT_MIN_ABI, provider);

// Pre-calculated values for optimization
const DIVIDER = 10000n;
const BASIS_MAP: { [key: number]: number } = { 2: 1e16, 3: 1e15, 4: 1e14 };

// Wallet cache to avoid recreating wallet instances
const walletCache = new Map<string, Wallet>();
function getWallet(privateKey: string): Wallet {
  if (!walletCache.has(privateKey)) {
    walletCache.set(privateKey, new Wallet(privateKey, provider));
  }
  return walletCache.get(privateKey)!;
}

// Caching infrastructure for multi-contract collection resolution
const multiContractSlugCache = new Map<string, boolean>();
const contractMapCache = new Map<string, Record<string, string>>();
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

const activeRequests = new Map<string, AbortController[]>();

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
  const tokenKey = tokenId.toString();

  // STEP 1: Check Max Pain Override (Priority: Highest)
  // This check happens FIRST for optimal performance
  if (normalizedSlug === MAX_PAIN_SLUG && MAX_PAIN_TOKEN_OVERRIDES.has(tokenKey)) {
    return MAX_PAIN_OVERRIDE_ADDRESS;
  }

  try {
    // STEP 2: Check if collection has multiple contracts
    if (!multiContractSlugCache.has(normalizedSlug)) {
      const details = await getCollectionDetails(normalizedSlug);
      const contractAddresses = Array.isArray(details?.contracts)
        ? details.contracts
            .map((contract: { address?: string | null }) => contract?.address?.toLowerCase())
            .filter((address: string | null | undefined): address is string => Boolean(address))
        : [];

      multiContractSlugCache.set(normalizedSlug, new Set(contractAddresses).size > 1);
    }

    // STEP 3: If not a multi-contract collection, use fallback
    if (!multiContractSlugCache.get(normalizedSlug)) {
      return fallbackAddress;
    }

    // STEP 4: Fetch token-to-contract mapping for multi-contract collections
    if (!contractMapCache.has(normalizedSlug)) {
      let pendingPromise = contractMapPromises.get(normalizedSlug);

      if (!pendingPromise) {
        pendingPromise = getCollectionTokenContractMap(normalizedSlug)
          .then((map) => {
            contractMapCache.set(normalizedSlug, map);
            contractMapPromises.delete(normalizedSlug);
            return map;
          })
          .catch((error) => {
            contractMapPromises.delete(normalizedSlug);
            throw error;
          });

        contractMapPromises.set(normalizedSlug, pendingPromise);
      }

      await pendingPromise;
    }

    // STEP 5: Return mapped contract or fallback
    const contractMap = contractMapCache.get(normalizedSlug) || {};
    return contractMap[tokenKey] ?? fallbackAddress;

  } catch {
    // Silent failure - always return a valid address
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
  asset?: { contractAddress: string, tokenId: number }
) {

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


      // Fetch balance first
      const wethBalance = await fetchERC20Balance(wallet_address, currency);

      // Balance validation after parallel fetch
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
        if (insufficientBalanceLogCache.addIfNotExists(dedupeKey, Date.now())) {
          errorLog('API', `[OpenSea] Insufficient balance for bid | taskId=${taskId} | wallet=${wallet_address.slice(0, 10)}... | collection=${slug} | bidAmount=${offerPriceEthFinal} ${currencySymbol} | available=${wethBalance}`, error);
        }
        throw error;
      }

      if (asset) {
        const offer = await buildItemOffer(task._id, {
          assetContractAddress: asset.contractAddress,
          tokenId: asset.tokenId.toString(),
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
          startAmount: ((offerPrice * openseaMarketplaceFeeBps) / DIVIDER).toString(),
          endAmount: ((offerPrice * openseaMarketplaceFeeBps) / DIVIDER).toString(),
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
          const feeAmount = ((offerPrice * fee) / DIVIDER).toString();

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
          const expiresAt = new Date(Date.now() + sanitizedExpiry * 1000);
          const orderData = {
            slug: slug,
            orderHash: itemOrderHash,
            expiresAt: expiresAt,
            offerPrice: offerPrice.toString(),
            identifier: asset.tokenId.toString(),
            taskId: taskId,
            marketplace: 'opensea'
          };
          await withRetry(async () => {
            const savedOrder = await Order.create(orderData);
            // Update in-memory cache
            orderCache.set({
              _id: savedOrder._id.toString(),
              orderHash: savedOrder.orderHash,
              expiresAt: savedOrder.expiresAt.getTime(),
              offerPrice: savedOrder.offerPrice,
              createdAt: savedOrder.createdAt.getTime(),
              identifier: savedOrder.identifier,
              marketplace: savedOrder.marketplace,
              slug: savedOrder.slug,
              taskId: savedOrder.taskId
            });
          });
        }
        trackBidRate('opensea', taskId);

        // Log successful token bid
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
          SEAPORT_CONTRACT.getCounter(wallet_address)
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
          startAmount: ((offerPrice * openseaMarketplaceFeeBps) / DIVIDER).toString(),
          endAmount: ((offerPrice * openseaMarketplaceFeeBps) / DIVIDER).toString(),
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
            startAmount: ((offerPrice * fee) / DIVIDER).toString(),
            endAmount: ((offerPrice * fee) / DIVIDER).toString(),
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


        const counter = await SEAPORT_CONTRACT.getCounter(wallet_address)

        // Note: For collection bids without traits, we're using hardcoded data instead of calling buildOffer
        const data = {
          partialParameters: {
            consideration: [{
              itemType: 4,
              token: contractAddress,
              identifierOrCriteria: 0,
              startAmount: "1",
              endAmount: "1",
              recipient: wallet_address
            }],
            zone: "0x000056f7000000ece9003ca63978907a00ffd100",
            zoneHash: "0x0000000000000000000000000000000000000000000000000000000000000000"
          },
          encodedTokenIds: ""
        }


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
          startAmount: ((offerPrice * openseaMarketplaceFeeBps) / DIVIDER).toString(),
          endAmount: ((offerPrice * openseaMarketplaceFeeBps) / DIVIDER).toString(),
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
            startAmount: ((offerPrice * fee) / DIVIDER).toString(),
            endAmount: ((offerPrice * fee) / DIVIDER).toString(),
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
      errorLog('OpenSea', `bidOnOpensea inner error | taskId=${taskId} | slug=${slug}`, error);
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
  const bidStartTime = Date.now();

  // Early blacklist check before entering limiter queue
  if (isTaskBlacklisted(taskId, 'opensea')) {
    debugLog('InFlightBid', `BLOCKED_EARLY | taskId=${taskId} | marketplace=opensea | reason=blacklisted`, { taskId, marketplace: 'opensea' });
    return;
  }
  const task = activeTasks.get(taskId)
  if (!task || !task?.running || !task.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("OPENSEA".toLowerCase())) return

  debugLog('InFlightBid', `ENTERING_LIMITER | taskId=${taskId} | marketplace=opensea | collection=${payload.criteria?.collection?.slug} | timestamp=${bidStartTime}`, { taskId, marketplace: 'opensea' });

  let offer: OpenseaOffer;
  try {
    const response = await limiter.schedule(async () => {
      // Re-check blacklist AFTER waiting in limiter queue - this is the critical fix
      if (isTaskBlacklisted(taskId, 'opensea')) {
        debugLog('InFlightBid', `BLOCKED_POST_QUEUE | taskId=${taskId} | marketplace=opensea | waitTime=${Date.now() - bidStartTime}ms`, { taskId, marketplace: 'opensea' });
        throw new Error('Task stopped while waiting in limiter queue');
      }

      debugLog('InFlightBid', `EXECUTING_REQUEST | taskId=${taskId} | marketplace=opensea | waitTime=${Date.now() - bidStartTime}ms`, { taskId, marketplace: 'opensea' });

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
        "axios-retry": { retries: 0 }
      });
    });
    offer = response.data;

    debugLog('InFlightBid', `BID_PLACED | taskId=${taskId} | marketplace=opensea | collection=${payload.criteria?.collection?.slug} | totalTime=${Date.now() - bidStartTime}ms`, { taskId, marketplace: 'opensea' });

  } catch (error) {
    errorLog('API', `[OpenSea] Failed to submit offer | taskId=${taskId} | collection=${payload.criteria.collection.slug} | bidAmount=${Number(offerPrice) / Math.pow(10, decimal)} ${currencySymbol} | status=${(error as any)?.response?.status} | ${extractOpenseaErrorMessage(error)}`, error);
    throw error;
  }

  const order_hash = offer.order_hash
  const identifier: string = offer?.criteria?.trait?.type
    && offer?.criteria?.trait?.value
    ? `${offer.criteria.trait?.type}:${offer.criteria.trait?.value}`
    : "collection"

  const slug = offer?.criteria?.collection?.slug
  const sanitizedExpiry = expiry > 900 ? expiry : 900

  if (offerPrice && order_hash && sanitizedExpiry && slug && taskId && identifier) {
    const expiresAt = new Date(Date.now() + sanitizedExpiry * 1000);
    const orderData = {
      slug: slug,
      orderHash: order_hash,
      expiresAt: expiresAt,
      offerPrice: offerPrice.toString(),
      identifier: identifier,
      taskId: taskId,
      marketplace: 'opensea'
    };
    await withRetry(async () => {
      const savedOrder = await Order.create(orderData);
      // Update in-memory cache
      orderCache.set({
        _id: savedOrder._id.toString(),
        orderHash: savedOrder.orderHash,
        expiresAt: savedOrder.expiresAt.getTime(),
        offerPrice: savedOrder.offerPrice,
        createdAt: savedOrder.createdAt.getTime(),
        identifier: savedOrder.identifier,
        marketplace: savedOrder.marketplace,
        slug: savedOrder.slug,
        taskId: savedOrder.taskId
      });
    });
  }
  trackBidRate('opensea', taskId);

  // Log successful bid
}

/**
 * Builds an offer on OpenSea with request deduplication.
 * @param buildPayload - The payload to build the offer.
 */
async function buildOffer(taskId: string, buildPayload: any) {
  const controller = new AbortController();
  addActiveRequest(taskId, controller);
  const buildStartTime = Date.now();

  try {
    const { data } = await limiter.schedule(async () => {
      // Re-check blacklist AFTER waiting in limiter queue
      if (isTaskBlacklisted(taskId, 'opensea')) {
        debugLog('InFlightBid', `BUILD_BLOCKED_POST_QUEUE | taskId=${taskId} | marketplace=opensea | waitTime=${Date.now() - buildStartTime}ms`, { taskId, marketplace: 'opensea' });
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
    const controllers = activeRequests.get(taskId) || [];
    const index = controllers.indexOf(controller);
    if (index >= 0) {
      controllers.splice(index, 1);
    }
    if (controllers.length === 0) {
      activeRequests.delete(taskId);
    }
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
    decrementBidCount('opensea', taskId)

    // Only update tracking if requested (for synchronized cancellation control)
    if (updateTracking) {
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
  debugLog('API', `[OpenSea] fetchMakerOfferForToken ENTRY | contract=${contractAddress} | token=${tokenId} | maker=${makerAddress.slice(0, 10)}...`, {
    marketplace: 'opensea',
    tokenId,
    wallet: makerAddress
  });

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
    debugLog('API', `[OpenSea] fetchMakerOfferForToken API response | ordersReturned=${orders.length} | token=${tokenId}`, {
      marketplace: 'opensea',
      tokenId
    });

    // Log filtering details for debugging
    if (orders.length > 0) {
      const orderDetails = orders.map((o: any) => ({
        hash: o.order_hash?.slice(0, 16),
        expired: o.expiration_time * 1000 <= now,
        cancelled: o.cancelled,
        finalized: o.finalized
      }));
      debugLog('API', `[OpenSea] fetchMakerOfferForToken order status | token=${tokenId} | details=${JSON.stringify(orderDetails)}`, {
        marketplace: 'opensea',
        tokenId
      });
    }

    const activeOrder = orders.find((order: any) =>
      order.expiration_time * 1000 > now && !order.cancelled && !order.finalized
    );

    if (!activeOrder) {
      // NO ACTIVE ORDER log
      debugLog('API', `[OpenSea] fetchMakerOfferForToken NO active order found | token=${tokenId} | totalOrders=${orders.length}`, {
        marketplace: 'opensea',
        tokenId
      });
      return null;
    }

    // Extract slug from taker_asset_bundle
    const takerAsset = activeOrder.taker_asset_bundle?.assets?.[0];
    const slug = takerAsset?.collection?.collection || '';
    const priceEth = Number(activeOrder.current_price) / Math.pow(10, 18);
    const expiresInMin = Math.round((activeOrder.expiration_time * 1000 - now) / 60000);

    // ACTIVE ORDER FOUND log
    debugLog('API', `[OpenSea] fetchMakerOfferForToken FOUND active order | token=${tokenId} | orderHash=${activeOrder.order_hash.slice(0, 16)}... | price=${priceEth.toFixed(6)} ETH | slug=${slug} | expiresIn=${expiresInMin}min`, {
      marketplace: 'opensea',
      tokenId,
      orderHash: activeOrder.order_hash,
      slug
    });

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
  const task = activeTasks.get(taskId);
  const collectionSlug = slug || task?.contract?.slug?.toLowerCase();
  const bidStartTime = Date.now();

  const parameters = offer as any;

  // Fix contract addresses in consideration items
  if (collectionSlug && parameters?.consideration) {
    for (const item of parameters.consideration) {
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
    }
  }

  const payload = {
    parameters,
    signature,
    protocol_address: SEAPORT_CONTRACT_ADDRESS,
  }

  try {
    const { data } = await limiter.schedule(async () => {
      // Re-check blacklist AFTER waiting in limiter queue - critical fix for in-flight token bids
      if (isTaskBlacklisted(taskId, 'opensea')) {
        debugLog('InFlightBid', `POST_ITEM_BLOCKED_POST_QUEUE | taskId=${taskId} | marketplace=opensea | waitTime=${Date.now() - bidStartTime}ms`, { taskId, marketplace: 'opensea' });
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
        "axios-retry": { retries: 0 },
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
      debugLog('API', `[OpenSea] ORDER_ALREADY_EXISTS detected | taskId=${taskId} | slug=${collectionSlug} | errorMsg=${errorMessage}`, {
        taskId,
        marketplace: 'opensea',
        slug: collectionSlug
      });

      try {
        // Extract details from the offer payload
        const consideration = parameters?.consideration?.[0];
        const contractAddress = consideration?.token;
        const tokenId = consideration?.identifierOrCriteria;
        const makerAddress = parameters?.offerer;

        // PAYLOAD EXTRACTION log
        debugLog('API', `[OpenSea] ORDER_ALREADY_EXISTS payload extracted | taskId=${taskId} | contract=${contractAddress?.slice(0, 16)}... | token=${tokenId} | maker=${makerAddress?.slice(0, 10)}...`, {
          taskId,
          marketplace: 'opensea',
          tokenId,
          wallet: makerAddress
        });

        if (contractAddress && tokenId && makerAddress) {
          // FETCH INITIATED log
          debugLog('API', `[OpenSea] ORDER_ALREADY_EXISTS fetching existing offer | taskId=${taskId} | token=${tokenId}`, {
            taskId,
            marketplace: 'opensea',
            tokenId
          });

          // Fetch existing order from OpenSea
          const existingOffer = await fetchMakerOfferForToken(contractAddress, tokenId, makerAddress);

          if (existingOffer) {
            // Convert expiration_time (Unix seconds) to Date
            const expiresAt = new Date(existingOffer.expirationTime * 1000);

            // Convert current_price (Wei string) to ETH string
            const decimal = 18; // WETH decimal
            const offerPriceEth = Number(existingOffer.currentPrice) / Math.pow(10, decimal);
            const offerPriceWei = existingOffer.currentPrice;

            // MONGODB SAVE log
            debugLog('API', `[OpenSea] ORDER_ALREADY_EXISTS saving to MongoDB | taskId=${taskId} | orderHash=${existingOffer.orderHash.slice(0, 16)}... | token=${existingOffer.tokenId} | price=${offerPriceEth} ETH`, {
              taskId,
              marketplace: 'opensea',
              tokenId: existingOffer.tokenId,
              orderHash: existingOffer.orderHash
            });

            // Store in MongoDB Order model
            const orderData = {
              slug: existingOffer.slug || collectionSlug || '',
              orderHash: existingOffer.orderHash,
              expiresAt: expiresAt,
              offerPrice: offerPriceEth.toString(),
              identifier: existingOffer.tokenId,
              taskId: taskId,
              marketplace: 'opensea'
            };

            const savedOrder = await Order.create(orderData);

            // ORDERCACHE UPDATE log
            debugLog('API', `[OpenSea] ORDER_ALREADY_EXISTS updating orderCache | taskId=${taskId} | orderId=${savedOrder._id}`, {
              taskId,
              marketplace: 'opensea',
              tokenId: existingOffer.tokenId,
              orderHash: existingOffer.orderHash
            });

            // Update in-memory orderCache
            orderCache.set({
              _id: savedOrder._id.toString(),
              orderHash: savedOrder.orderHash,
              expiresAt: savedOrder.expiresAt.getTime(),
              offerPrice: savedOrder.offerPrice,
              createdAt: savedOrder.createdAt.getTime(),
              identifier: savedOrder.identifier,
              marketplace: savedOrder.marketplace,
              slug: savedOrder.slug,
              taskId: savedOrder.taskId
            });

            // TOP OFFER CACHE UPDATE log
            debugLog('API', `[OpenSea] ORDER_ALREADY_EXISTS updating topOfferCache | taskId=${taskId} | token=${existingOffer.tokenId} | price=${offerPriceEth} ETH`, {
              taskId,
              marketplace: 'opensea',
              tokenId: existingOffer.tokenId
            });

            // Update marketplaceTopOfferCache - mark us as top bidder
            // This ensures counterbid handlers know we have an active offer
            updateTopOfferAfterOurBid(
              taskId,
              'opensea',
              existingOffer.tokenId,
              offerPriceEth,
              offerPriceWei,
              makerAddress
            );

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
  const itemOffers = await apiDedup(
    `opensea-item-offers-${contractAddress.toLowerCase()}-${tokenId}`,
    () => limiter.schedule(() => axiosInstance.post(
      `${BASE_URL}/opensea/graphql`,
      query,
      {
        headers: {
          'content-type': 'application/json',
          'x-api-key': API_KEY
        }
      }
    )),
    { category: 'OpenSea' }
  );

  // Default offer structure
  const defaultOffer = {
    amount: 0,
    owner: '0x0000000000000000000000000000000000000000',
    currency: 'WETH',
    usd: 0,
    decimal: 18,
    eth: 0
  };

  const order = ((itemOffers.data) as unknown as GraphQLItemOfferResponse)?.data?.itemOffers?.items
    ?.filter((offer) => offer && new Date(offer?.endTime).getTime() > Date.now()) // remove expired offers
    .map((offer) => ({
      amount: offer?.pricePerItem?.token?.unit || 0,
      owner: offer?.maker?.address || '0x0000000000000000000000000000000000000000',
      currency: offer?.pricePerItem?.token?.symbol?.toUpperCase() || 'WETH',
      usd: offer?.pricePerItem?.usd || 0,
      decimal: offer?.pricePerItem?.token?.symbol?.toUpperCase() === "WETH" ? 18 : offer?.pricePerItem?.token?.symbol?.toUpperCase() === "ETH" ? 18 : offer?.pricePerItem?.token?.symbol?.toUpperCase() === "BETH" ? 18 : offer?.pricePerItem?.token?.symbol?.toUpperCase() === "USDC" ? 6 : offer?.pricePerItem?.token?.symbol?.toUpperCase() === "USDT" ? 6 : 18,
      eth: offer?.pricePerItem?.token?.unit || 0
    })) || []

  // Ensure we always return at least 2 offers
  if (order.length === 0) {
    return [defaultOffer, defaultOffer];
  } else if (order.length === 1) {
    return [order[0], defaultOffer];
  }
  return order
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

    const response = await apiDedup(
      `opensea-collection-offers-${collectionSlug}`,
      () => limiter?.schedule(priority !== undefined ? { priority } : {}, () =>
        axiosInstance.post(`${BASE_URL}/opensea/graphql`, query, {
          headers: {
            'content-type': 'application/json',
            'x-api-key': API_KEY
          }
        })
      ),
      { category: 'OpenSea' }
    );
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

        return apiDedup(
          `opensea-trait-offers-collection-${collectionSlug}`,
          () => limiter?.schedule(priority !== undefined ? { priority } : {}, () =>
            axiosInstance.post(`${BASE_URL}/opensea/graphql`, collectionQuery, {
              headers: {
                'content-type': 'application/json',
                'x-api-key': API_KEY
              }
            })
          ),
          { category: 'OpenSea' }
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

        return apiDedup(
          `opensea-trait-offers-${collectionSlug}-${type}-${value}`,
          () => limiter?.schedule(priority !== undefined ? { priority } : {}, () =>
            axiosInstance.post(`${BASE_URL}/opensea/graphql`, traitQuery, {
              headers: {
                'content-type': 'application/json',
                'x-api-key': API_KEY
              }
            })
          ),
          { category: 'OpenSea' }
        );
      }
    ];

    // Execute both queries in parallel
    const [collectionResponse, traitResponse] = await pQueue.addAll(tasks, {
      throwOnTimeout: true,
    });

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
  const cacheKey = CACHE_KEYS.traitFloor(slug, trait.traitType, trait.values);

  return getCachedOrFetch(
    cacheKey,
    CACHE_TTL.TRAIT_FLOOR,
    async () => {
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
      }
      const queryVariables = {
        ...variables,
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
      const floor = data.data.collectionItems.items[0].bestListing?.pricePerItem.token.unit
      return floor
    },
    true // parseNumber flag to handle the numeric floor value
  );
}

export async function fetchOpenseaListings(taskId: string, collectionSlug: string, limit?: number, priority?: number) {
  if (!limit) return;

  const cacheKey = CACHE_KEYS.listings(collectionSlug, limit);

  return getCachedOrFetch(
    cacheKey,
    CACHE_TTL.LISTINGS,
    async () => {
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

      return uniqueTokenIds.slice(0, limit);
    }
  );
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

export async function cancelOpenseaRequest(taskId: string): Promise<{ success: boolean; cancelledCount: number; error?: string }> {
  // Input validation
  if (!taskId || typeof taskId !== 'string') {
    warnLog('API', `[OpenSea] Invalid taskId provided to cancelOpenseaRequest | taskId=${taskId}`);
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
        warnLog('API', `[OpenSea] Failed to abort request controller | taskId=${taskId} | ${error instanceof Error ? error.message : String(error)}`);
      }
    }));

    activeRequests.delete(taskId);

    return { success: true, cancelledCount };
  } catch (error) {
    errorLog('API', `[OpenSea] Failed to cancel requests | taskId=${taskId}`, error);
    return { success: false, cancelledCount: 0, error: error instanceof Error ? error.message : String(error) };
  }
}

function addActiveRequest(taskId: string, controller: AbortController) {
  if (!activeRequests.has(taskId)) {
    activeRequests.set(taskId, []);
  }
  activeRequests.get(taskId)?.push(controller);
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
    itemByIdentifier: ItemByIdentifier | null;
    itemOffers: ItemOffers;
  };
}

export interface ItemByIdentifier {
  ownership: Ownership | null;
  __typename: string;
}

export interface Ownership {
  // Define if needed
}

export interface ItemOffers {
  items: OpenSeaOrder[];
}

export interface OpenSeaOrder {
  id: string;
  type: string;
  pricePerItem: IPrice;
  quantityRemaining: string;
  maker: OrderProfile;
  endTime: string;
  marketplace: Marketplace;
  __typename: string;
}

export interface IPrice {
  token: ITokenPrice;
  usd: number;
  __typename: string;
}

export interface ITokenPrice {
  unit: number;
  symbol: string;
  contractAddress: string;
  chain: IChain;
  __typename: string;
}

export interface IChain {
  identifier: string;
  __typename: string;
}

export interface OrderProfile {
  address: string;
  displayName: string | null;
  imageUrl: string;
  __typename: string;
}

export interface Marketplace {
  identifier: string;
  __typename: string;
}
