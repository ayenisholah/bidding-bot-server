

import { ethers, Wallet } from "ethers";
import { BASE_URL, SEAPORT_CONTRACT_ADDRESS, SEAPORT_MIN_ABI } from "../../constants";
import { axiosInstance, limiter, isTaskBlacklisted } from "../../init";
import { activeTasks, API_KEY, BLUE, decrementBidCount, pQueue, trackBidRate } from "../..";
import { config } from "dotenv";
import axios from "axios";
import Order from '../../models/order.model';
import { withRetry } from '../../utils/db-retry'
import { USDC, WETH } from "../../utils/currencies";
import { CollectionItemsListQuery } from "../../queries";
import { CollectionItemsQueryVariables, CollectionItemsResponse } from "../../functions";
import { fetchERC20Balance } from "../../utils/balance";
import { logger, ErrorCategory, MarketplaceSubCategory } from "../../utils/logger";
import { InsufficientBalanceError } from "../errors";
import { getCachedOrFetch, CACHE_TTL, CACHE_KEYS } from "../../utils/opensea-cache";
import fs from 'fs';
import path from 'path';

// Performance timing utilities
interface BidTimingLog {
  taskId: string;
  timestamp: string;
  bidType: 'collection' | 'trait' | 'token';
  slug: string;
  wallet: string;
  amount: number;
  currency: string;
  steps: {
    [stepName: string]: {
      startTime: number;
      endTime: number;
      duration: number;
      success: boolean;
      error?: string;
    };
  };
  totalDuration: number;
  success: boolean;
  errorMessage?: string;
}

// Ensure logs directory exists
const logsDir = path.join(process.cwd(), 'logs');
if (!fs.existsSync(logsDir)) {
  fs.mkdirSync(logsDir, { recursive: true });
}

const timingLogFile = path.join(logsDir, 'opensea-bid-timing.log');

function logTimingData(timingData: BidTimingLog) {
  try {
    const logEntry = JSON.stringify(timingData) + '\n';
    fs.appendFileSync(timingLogFile, logEntry);
  } catch (error) {
    console.error('Failed to write timing log:', error);
    // Log to Axiom but don't await to avoid blocking
    logger.logGeneralError('Failed to write timing log', {
      operation: 'write_timing_log',
      error: error as Error,
      errorMessage: error instanceof Error ? error.message : String(error)
    }).catch(() => { /* Intentionally ignore logging errors */ }); // Ignore logging errors for timing logs
  }
}

class BidTimer {
  private startTime: number;
  private steps: BidTimingLog['steps'] = {};
  private currentStep: string | null = null;

  constructor() {
    this.startTime = Date.now();
  }

  startStep(stepName: string) {
    if (this.currentStep) {
      this.endStep(this.currentStep, false, 'Step interrupted');
    }
    this.currentStep = stepName;
    this.steps[stepName] = {
      startTime: Date.now(),
      endTime: 0,
      duration: 0,
      success: false
    };
  }

  endStep(stepName: string, success: boolean = true, error?: string) {
    if (this.steps[stepName]) {
      const endTime = Date.now();
      this.steps[stepName].endTime = endTime;
      this.steps[stepName].duration = endTime - this.steps[stepName].startTime;
      this.steps[stepName].success = success;
      if (error) {
        this.steps[stepName].error = error;
      }
    }
    if (this.currentStep === stepName) {
      this.currentStep = null;
    }
  }

  getTimingData(taskId: string, bidType: 'collection' | 'trait' | 'token', slug: string, wallet: string, amount: number, currency: string, success: boolean, errorMessage?: string): BidTimingLog {
    const totalDuration = Date.now() - this.startTime;
    return {
      taskId,
      timestamp: new Date().toISOString(),
      bidType,
      slug,
      wallet,
      amount,
      currency,
      steps: this.steps,
      totalDuration,
      success,
      errorMessage
    };
  }
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
const OPENSEA_FEE_ADDRESS = "0x0000a26b00c1F0DF003000390027140000fAa719"
const ALCHEMY_API_KEY = process.env.ALCHEMY_API_KEY as string;
const provider = new ethers.AlchemyProvider('mainnet', ALCHEMY_API_KEY);
const SEAPORT_CONTRACT = new ethers.Contract(SEAPORT_CONTRACT_ADDRESS, SEAPORT_MIN_ABI, provider);
const RESET = '\x1b[0m';

// Pre-calculated values for optimization
const OPENSEA_FEE_BIGINT = 50n;
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

async function buildItemOffer(taskId: string, offerSpecification: ItemOfferSpecification, currency: string) {
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
  const consideration = getItemConsideration(
    assetContractAddress,
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
 * @param enforceCreatorFee - Whether to enforce creator fees.
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
  enforceCreatorFee: boolean,
  expiry: number = 900,
  currency = WETH,
  opensea_traits?: string,
  asset?: { contractAddress: string, tokenId: number }
) {
  // Initialize timing tracker
  const timer = new BidTimer();
  const bidType: 'collection' | 'trait' | 'token' = asset ? 'token' : opensea_traits ? 'trait' : 'collection';
  let success = false;
  let errorMessage: string | undefined;
  let offerPriceEthFinal: number;
  let currencySymbol: string;


  try {
    timer.startStep('validation');

    // Check if task is blacklisted (stopped)
    if (isTaskBlacklisted(taskId, 'opensea')) {
      const error = new Error(`Bid execution blocked - task ${taskId} is stopped for OpenSea`);
      console.log(`[OpenSea] Job execution blocked for stopped task: ${taskId}`);
      timer.endStep('validation', false, 'Task is blacklisted');
      throw error;
    }

    // Early validation - fail fast
    const task = activeTasks.get(taskId)
    if (!task || !task.running || !task.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("opensea")) {
      timer.endStep('validation', false, 'Task validation failed');
      return
    }

    timer.endStep('validation');

    timer.startStep('price_calculation');

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

    timer.endStep('price_calculation');

    timer.startStep('wallet_setup');

    // Get wallet from cache
    const wallet = getWallet(private_key);

    timer.endStep('wallet_setup');

    timer.startStep('balance_check');

    // Fetch balance first
    const wethBalance = await fetchERC20Balance(wallet_address, currency);

    // Balance validation after parallel fetch
    if (offerPriceEthFinal > wethBalance) {
      timer.endStep('balance_check', false, 'Insufficient balance');
      const error = new InsufficientBalanceError(
        'opensea',
        currencySymbol,
        offerPriceEthFinal,
        wethBalance,
        wallet_address,
        slug
      );
      await logger.logOpenseaBidError('Insufficient balance for bid', {
        taskId,
        walletAddress: wallet_address,
        collection: slug,
        bidAmount: offerPriceEthFinal,
        currency: currencySymbol,
        availableBalance: wethBalance,
        requiredBalance: offerPriceEthFinal,
        error
      });
      throw error;
    }

    timer.endStep('balance_check');

    if (asset) {
      timer.startStep('token_offer_build');

      const offer = await buildItemOffer(task._id, {
        assetContractAddress: asset.contractAddress,
        tokenId: asset.tokenId.toString(),
        walletAddress: wallet_address,
        quantity: 1,
        expirationSeconds: BigInt(expiry),
        priceWei: BigInt(roundedWei)
      }, currency)

      if (!offer) {
        timer.endStep('token_offer_build', false, 'Build item offer failed');
        return
      }

      timer.endStep('token_offer_build');
      timer.startStep('token_fees_setup');

      const opensea_consideration = {
        itemType: 1,
        token: currency,
        identifierOrCriteria: "0",
        startAmount: ((offerPrice * OPENSEA_FEE_BIGINT) / DIVIDER).toString(),
        endAmount: ((offerPrice * OPENSEA_FEE_BIGINT) / DIVIDER).toString(),
        recipient: OPENSEA_FEE_ADDRESS
      };

      offer.consideration.push(opensea_consideration);
      offer.totalOriginalConsiderationItems = (Number(offer.totalOriginalConsiderationItems) + 1).toString();
      for (const address in creator_fees) {
        let fee: bigint | number = creator_fees[address];
        fee = BigInt(Math.round(fee).toString());
        if (enforceCreatorFee) {
          const consideration_item = {
            itemType: 1,
            token: currency,
            identifierOrCriteria: "0",
            startAmount: ((offerPrice * fee) / DIVIDER).toString(),
            endAmount: ((offerPrice * fee) / DIVIDER).toString(),
            recipient: address
          };
          offer.consideration.push(consideration_item);
          offer.totalOriginalConsiderationItems = (Number(offer.totalOriginalConsiderationItems) + 1).toString();
        }
      }

      timer.endStep('token_fees_setup');
      timer.startStep('token_signing');

      const itemSignature = await signOffer(wallet, offer)

      if (!itemSignature) {
        timer.endStep('token_signing', false, 'Signing failed');
        return
      }

      timer.endStep('token_signing');
      timer.startStep('token_final_validation');

      // check if the tasks is still active before you make the offer
      const [taskId] = bidCount.split(":")
      if (isTaskBlacklisted(taskId, 'opensea') || !task || !task?.running || !task.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("OPENSEA".toLowerCase())) {
        timer.endStep('token_final_validation', false, 'Task no longer active or blacklisted');
        return
      }

      timer.endStep('token_final_validation');
      timer.startStep('token_submission');

      const itemResponse = await postItemOffer(taskId, offer, itemSignature)
      const itemOrderHash = itemResponse?.order?.order_hash

      timer.endStep('token_submission');
      timer.startStep('token_database_save');

      const sanitizedExpiry = expiry > 900 ? expiry : 900
      if (offerPrice && itemOrderHash && sanitizedExpiry && slug && taskId && asset.tokenId) {
        await withRetry(async () => {
          await Order.create({
            slug: slug,
            orderHash: itemOrderHash,
            expiresAt: new Date(Date.now() + sanitizedExpiry * 1000),
            offerPrice: offerPrice.toString(),
            identifier: asset.tokenId.toString(),
            taskId: taskId,
            marketplace: 'opensea'
          });
        });
      }

      timer.endStep('token_database_save');

      trackBidRate('opensea', taskId);
      const successMessage = `🎉 TOKEN OFFER POSTED TO OPENSEA SUCCESSFULLY FOR: ${slug.toUpperCase()}  TOKEN: ${asset.tokenId} ${Number(offerPriceEthFinal)} ${currencySymbol} 🎉`
      console.log(BLUE, successMessage, RESET);
      success = true;
    }
    else {
      timer.startStep('collection_payload_setup');

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


      timer.endStep('collection_payload_setup');
      timer.startStep('build_collection_offer');

      // Parallel operations for collection offers
      const [data, counter] = await Promise.all([
        buildOffer(taskId, buildPayload),
        SEAPORT_CONTRACT.getCounter(wallet_address)
      ]);

      if (!data || !data.partialParameters) {
        timer.endStep('build_collection_offer', false, 'Build offer or counter fetch failed');
        return
      }


      timer.endStep('build_collection_offer');
      timer.startStep('collection_offer_assembly');

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
        startAmount: ((offerPrice * OPENSEA_FEE_BIGINT) / DIVIDER).toString(),
        endAmount: ((offerPrice * OPENSEA_FEE_BIGINT) / DIVIDER).toString(),
        recipient: OPENSEA_FEE_ADDRESS
      };
      payload.protocol_data.parameters.consideration.push(opensea_consideration);

      for (const address in creator_fees) {
        let fee: bigint | number = creator_fees[address];
        fee = BigInt(Math.round(fee).toString());
        if (enforceCreatorFee) {
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
      }

      payload.protocol_data.parameters.zone = data.partialParameters.zone;
      payload.protocol_data.parameters.zoneHash = data.partialParameters.zoneHash;
      payload.protocol_data.parameters.salt = getSalt();
      payload.protocol_data.parameters.counter = counter.toString();

      timer.endStep('collection_offer_assembly');
      timer.startStep('collection_signing');

      const signObj = await wallet.signTypedData(
        domain,
        types,
        payload.protocol_data.parameters
      );
      payload.protocol_data.signature = signObj;
      payload.protocol_address = SEAPORT_1_6;

      timer.endStep('collection_signing');
      timer.startStep('collection_final_validation');

      if (isTaskBlacklisted(taskId, 'opensea') || !task || !task.running || !task.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("opensea")) {
        timer.endStep('collection_final_validation', false, 'Task no longer active or blacklisted');
        return
      }

      timer.endStep('collection_final_validation');
      timer.startStep('collection_submission');

      await submitOfferToOpensea(decimal, currencySymbol, bidCount, offerPrice.toString(), payload, expiry, opensea_traits)

      timer.endStep('collection_submission');
      success = true;
    }

  } catch (error) {
    errorMessage = error instanceof Error ? error.message : String(error);
    throw error;
  } finally {
    // Log timing data
    const timingData = timer.getTimingData(
      taskId,
      bidType,
      slug,
      wallet_address.substring(0, 8) + '...',
      offer_price,
      'ETH',
      success,
      errorMessage
    );
    logTimingData(timingData);
  }
}

/**
 * Posts an offer to OpenSea.
 * @param payload - The payload of the offer.
 */
async function submitOfferToOpensea(decimal: number, currencySymbol: string, bidCount: string, offerPrice: string, payload: IPayload, expiry = 900, opensea_traits?: string) {
  const [taskId] = bidCount.split(":")
  if (isTaskBlacklisted(taskId, 'opensea')) {
    console.log(`[OpenSea] Offer submission blocked for stopped task: ${taskId}`);
    return;
  }
  const task = activeTasks.get(taskId)
  if (!task || !task?.running || !task.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("OPENSEA".toLowerCase())) return

  let offer: OpenseaOffer;
  try {
    const response = await limiter.schedule(() => axiosInstance.request<OpenseaOffer>({
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
    }))
    offer = response.data;

  } catch (error) {

    // Check for merkle root error in submit response
    const errorData = (error as any)?.response?.data;
    if (errorData && typeof errorData === 'string' && errorData.includes('merkle')) {
      console.log('[OPENSEA_DEBUG] MERKLE ROOT ERROR IN SUBMIT:', errorData);
    } else if (errorData?.error && errorData.error.includes('merkle')) {
      console.log('[OPENSEA_DEBUG] MERKLE ROOT ERROR IN SUBMIT:', errorData.error);
    } else if (errorData?.message && errorData.message.includes('merkle')) {
      console.log('[OPENSEA_DEBUG] MERKLE ROOT ERROR IN SUBMIT:', errorData.message);
    } else if (errorData?.errors && Array.isArray(errorData.errors)) {
      errorData.errors.forEach((err: any, index: number) => {
        if (err.includes && err.includes('merkle')) {
          console.log(`[OPENSEA_DEBUG] MERKLE ROOT ERROR IN SUBMIT (error[${index}]):`, err);
        }
      });
    }

    await logger.logOpenseaBidError('Failed to submit OpenSea offer', {
      taskId,
      collection: payload.criteria.collection.slug,
      bidAmount: Number(offerPrice) / Math.pow(10, decimal),
      currency: currencySymbol,
      error: error as Error,
      errorCode: (error as any)?.response?.status,
      errorMessage: extractOpenseaErrorMessage(error)
    });
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
    await withRetry(async () => {
      await Order.create({
        slug: slug,
        orderHash: order_hash,
        expiresAt: new Date(Date.now() + sanitizedExpiry * 1000),
        offerPrice: offerPrice.toString(),
        identifier: identifier,
        taskId: taskId,
        marketplace: 'opensea'
      });
    });
  }
  trackBidRate('opensea', taskId);

  // Log successful bid

  const successMessage = opensea_traits ?
    `🎉 TRAIT OFFER POSTED TO OPENSEA SUCCESSFULLY FOR: ${payload.criteria.collection.slug.toUpperCase()}  TRAIT: ${opensea_traits} ${(Number(offerPrice) / Math.pow(10, decimal))} ${currencySymbol} 🎉`
    : `🎉 COLLECTION OFFER POSTED TO OPENSEA SUCCESSFULLY FOR: ${payload.criteria.collection.slug.toUpperCase()} ${(Number(offerPrice) / Math.pow(10, decimal))} ${currencySymbol} 🎉`
  console.log(BLUE, successMessage, RESET);
}

/**
 * Builds an offer on OpenSea with request deduplication.
 * @param buildPayload - The payload to build the offer.
 */
async function buildOffer(taskId: string, buildPayload: any) {
  const controller = new AbortController();
  addActiveRequest(taskId, controller);

  try {
    const { data } = await limiter.schedule(() =>
      axiosInstance.request<PartialParameters>({
        method: 'POST',
        url: `${BASE_URL}/opensea/api/v2/offers/build`,
        headers: {
          'content-type': 'application/json',
          'x-api-key': API_KEY,
        },
        data: JSON.stringify(buildPayload),
        signal: controller.signal
      })
    );

    return data;
  } catch (error: any) {
    if (axios.isCancel(error)) {
      return null;
    }


    // Check for merkle root error
    const errorData = error?.response?.data;
    if (errorData && typeof errorData === 'string' && errorData.includes('merkle')) {
      console.log('[OPENSEA_DEBUG] MERKLE ROOT ERROR DETECTED:', errorData);
    } else if (errorData?.error && errorData.error.includes('merkle')) {
      console.log('[OPENSEA_DEBUG] MERKLE ROOT ERROR DETECTED:', errorData.error);
    } else if (errorData?.message && errorData.message.includes('merkle')) {
      console.log('[OPENSEA_DEBUG] MERKLE ROOT ERROR DETECTED:', errorData.message);
    }

    await logger.logOpenseaBidError('Failed to build OpenSea offer', {
      taskId,
      operation: 'build_offer',
      error: error,
      errorCode: error?.response?.status,
      errorMessage: extractOpenseaErrorMessage(error)
    });
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

export async function cancelOrder(orderHash: string, protocolAddress: string, privateKey: string, taskId: string) {
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
      console.error('Limiter not initialized - cannot process cancelOrder request');
      throw new Error('Limiter not initialized');
    }

    const response = await limiter.schedule(() => axiosInstance.post(url, body, { headers }))
    console.log(JSON.stringify({ cancelled: true }));
    decrementBidCount('opensea', taskId)
    return response.data;
  } catch (error) {
    if (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      console.error(`Failed to cancel OpenSea order: ${errorMessage}`);
      await logger.logOpenseaBidError('Failed to cancel OpenSea order', {
        taskId,
        orderHash,
        operation: 'cancel_order',
        error: error as Error,
        errorMessage: errorMessage
      });
    }
    throw error;
  }
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

async function postItemOffer(taskId: string, offer: unknown, signature: string) {
  const payload = {
    parameters: offer,
    signature,
    protocol_address: SEAPORT_CONTRACT_ADDRESS,
  }

  try {
    const { data } = await limiter.schedule(() => axiosInstance.post(`${BASE_URL}/opensea/api/v2/orders/ethereum/seaport/offers`, payload, {
      headers: {
        'content-type': 'application/json',
        'x-api-key': API_KEY,
        "Cache-Control": "no-store",
        "Pragma": "no-cache",
        "Expires": "0",
      },
      "axios-retry": { retries: 0 },
    }))
    return data
  } catch (error: any) {
    if (axios.isCancel(error)) {
      return null;
    }
    await logger.logOpenseaBidError('Failed to post item offer to OpenSea', {
      taskId,
      operation: 'post_item_offer',
      error: error,
      errorCode: error?.response?.status,
      errorMessage: extractOpenseaErrorMessage(error)
    });
    throw error
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
  const itemOffers = await limiter.schedule(() => axiosInstance.post(
    `${BASE_URL}/opensea/graphql`,
    query,
    {
      headers: {
        'content-type': 'application/json',
        'x-api-key': API_KEY
      }
    }
  ));

  const order = ((itemOffers.data) as unknown as GraphQLItemOfferResponse).data.itemOffers.items
    .filter((offer) => new Date(offer.endTime).getTime() > Date.now()) // remove expired offers
    .map((offer) => ({
      amount: offer.pricePerItem.token.unit,
      owner: offer.maker.address,
      currency: offer.pricePerItem.token.symbol.toUpperCase(),
      usd: offer.pricePerItem.usd,
      decimal: offer.pricePerItem.token.symbol.toUpperCase() === "WETH" ? 18 : offer.pricePerItem.token.symbol.toUpperCase() === "ETH" ? 18 : offer.pricePerItem.token.symbol.toUpperCase() === "BETH" ? 18 : offer.pricePerItem.token.symbol.toUpperCase() === "USDC" ? 6 : offer.pricePerItem.token.symbol.toUpperCase() === "USDT" ? 6 : 18,
      eth: offer.pricePerItem.token.unit
    }))

  return order
}

export async function fetchOpenseaOffers(
  offerType: 'COLLECTION' | 'TRAIT' | 'TOKEN',
  collectionSlug: string,
  contractAddress: string,
  identifiers: Record<string, string> | string
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

    const response = await limiter?.schedule(() =>
      axiosInstance.post(`${BASE_URL}/opensea/graphql`, query, {
        headers: {
          'content-type': 'application/json',
          'x-api-key': API_KEY
        }
      })
    );
    // Parse the GraphQL response
    const offerAggregates = response.data?.data?.collectionOfferAggregates?.items || [];

    // Map to the expected format and get top 2 offers
    const offers = offerAggregates
      .slice(0, 2)
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
      });

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

        return limiter?.schedule(() =>
          axiosInstance.post(`${BASE_URL}/opensea/graphql`, collectionQuery, {
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

        return limiter?.schedule(() =>
          axiosInstance.post(`${BASE_URL}/opensea/graphql`, traitQuery, {
            headers: {
              'content-type': 'application/json',
              'x-api-key': API_KEY
            }
          })
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


    console.log({ allOffers });


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

export async function fetchOpenseaListings(taskId: string, collectionSlug: string, limit?: number) {
  if (!limit) return;

  const cacheKey = CACHE_KEYS.listings(collectionSlug, limit);

  return getCachedOrFetch(
    cacheKey,
    CACHE_TTL.LISTINGS,
    async () => {
      const baseUrl = `${BASE_URL}/opensea/api/v2/listings/collection/${collectionSlug}/best`;
      let allListings: OpenseaOrder[] = [];
      let nextCursor: string | null = null;
      let uniqueTokenIds: number[] = [];

      while (uniqueTokenIds.length < limit) {
        const params: any = {
          next: nextCursor,
          limit: Math.min(100, limit * 2 - uniqueTokenIds.length) // Fetch more to account for duplicates
        }
        const { data } = await limiter.schedule(() => axiosInstance.get<OpenseaListingData>(baseUrl, {
          headers: {
            'accept': 'application/json',
            'x-api-key': API_KEY
          },
          params: params
        }))

        allListings = [...allListings, ...data.listings];
        uniqueTokenIds = [...new Set(allListings.map((item) =>
          +item.protocol_data.parameters.offer[0].identifierOrCriteria
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
    await logger.logWarning(ErrorCategory.MARKETPLACE, 'Invalid taskId provided to cancelOpenseaRequest', {
      taskId,
      marketplace: 'opensea',
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
        await logger.logWarning(ErrorCategory.MARKETPLACE, 'Failed to abort OpenSea request controller', {
          taskId,
          marketplace: 'opensea',
          marketplaceSubCategory: MarketplaceSubCategory.CANCELLATION,
          errorMessage: error instanceof Error ? error.message : String(error)
        });
      }
    }));

    activeRequests.delete(taskId);

    return { success: true, cancelledCount };
  } catch (error) {
    await logger.logMarketplaceError('Failed to cancel OpenSea requests', {
      taskId,
      marketplace: 'opensea',
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
