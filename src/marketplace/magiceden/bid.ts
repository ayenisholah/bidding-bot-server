import { ethers, Wallet } from "ethers";
import { axiosInstance, limiter } from "../../init";
import { config } from "dotenv";
import { activeTasks, API_KEY, BASE_URL, currentTasks, decrementBidCount, MAGENTA, pQueue, redis, trackBidRate } from "../..";
import { createBalanceChecker } from "../../utils/balance";
import { DistributedLockManager } from '../../utils/lock';
import { PrismaClient } from '@prisma/client'
import axios from "axios";
import { USDC, WETH } from "../../utils/currencies";

const prisma = new PrismaClient()

const RED = '\x1b[31m';
const RESET = '\x1b[0m';

config()

const ALCHEMY_API_KEY = process.env.ALCHEMY_API_KEY as string;
const provider = new ethers.providers.AlchemyProvider('mainnet', ALCHEMY_API_KEY);


const deps = {
  redis: redis,
  provider: new ethers.providers.AlchemyProvider("mainnet", ALCHEMY_API_KEY),
};

const balanceChecker = createBalanceChecker(deps);

const lockManager = new DistributedLockManager({
  lockPrefix: '{magiceden}:fetch:',
  defaultTTLSeconds: 30
});

const activeRequests = new Map<string, AbortController[]>();

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
  tokenId?: string | number
) {
  try {

    const task = currentTasks.find((task) => task.contract.slug.toLowerCase() === slug.toLowerCase() && task.selectedMarketplaces.includes("MagicEden"))
    if (!task) return

    const duration = bidExpiry / 60 || 15; // minutes
    const currentTime = new Date().getTime();
    const expiration = Math.floor((currentTime + (duration * 60 * 1000)) / 1000);

    const decimal = currency.toLowerCase() === USDC.toLowerCase() ? 6 : 18

    const wallet = new Wallet(privateKey, provider);
    const offerPriceEth = Math.round(Number(weiPrice)) / Math.pow(10, decimal)
    const wethBalance = await balanceChecker.getERC20Balance(maker, currency);

    let decimals;
    if (offerPriceEth >= 1) {
      decimals = 2;
    } else if (offerPriceEth >= 0.1) {
      decimals = 3;
    } else {
      decimals = 4;
    }

    const roundedEth = currency.toLowerCase() === USDC.toLowerCase() ? Number(offerPriceEth.toFixed(2)) : Number(offerPriceEth);

    const basis = decimals === 2 ? 1e16 : decimals === 3 ? 1e15 : 1e14
    const roundedWei = decimal === 18 ? Math.round(roundedEth * Math.pow(10, decimal) / basis) * basis : Math.round(roundedEth * Math.pow(10, decimal))

    const currencySymbol = currency.toLowerCase() === USDC.toLowerCase() ? "USDC" : "WETH"

    if (offerPriceEth > wethBalance) {
      const identifier = trait ? `${trait.attributeKey}:${trait.attributeValue}` : tokenId ? tokenId : 'collection';
      const message = `[${slug.toUpperCase()}${identifier ? ` - ${identifier}` : ''}] Offer price: ${offerPriceEth} ${currencySymbol} is greater than available ${currencySymbol} balance: ${wethBalance} ${currencySymbol}. SKIPPING  MAGICEDEN...`
      throw new Error(message)
    }

    const order = await createBidData(taskId, slug, maker, collection, quantity, Math.round(Number(weiPrice)).toString(), expiration.toString(), currency, trait, tokenId);

    if (!order) return
    try {
      if (tokenId) {
        await submitSignedOrderData(taskId, roundedWei.toString(), decimal, currencySymbol, bidCount, order, wallet, slug, bidExpiry, undefined, tokenId)
      } else if (trait) {
        await submitSignedOrderData(taskId, roundedWei.toString(), decimal, currencySymbol, bidCount, order, wallet, slug, bidExpiry, trait, undefined)
      } else {
        await submitSignedOrderData(taskId, roundedWei.toString(), decimal, currencySymbol, bidCount, order, wallet, slug, bidExpiry, undefined, undefined)
      }
    } catch (error: any) {
      throw error
    }
    return order
  } catch (error: any) {
    throw error
  }
}

/**
 * Creates bid data for Magic Eden.
 * @param maker - The maker of the bid.
 * @param collection - The collection to bid on.
 * @param quantity - The quantity to bid.
 * @param weiPrice - The price in wei.
 * @param expirationTime - The expiration time of the bid.
 * @param traits - Optional traits for the offer.
 * @returns The bid data.
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
  tokenId?: number | string,
) {

  const task = currentTasks.find((task) => task.contract.slug.toLowerCase() === slug.toLowerCase() && task?.selectedMarketplaces?.map((marketplace) => marketplace.toLowerCase()).includes("magiceden"))

  if (!task) return

  const options = trait || tokenId ? {
    "seaport-v1.6": {
      "useOffChainCancellation": true,
      "conduitKey": "0x87328c9043E7BF343695554EAAF5a8892f7205e3000000000000000000000000",

    }
  } : {
    "payment-processor-v2": {
      useOffChainCancellation: true,
    }
  }

  const orderKind = trait || tokenId ? "seaport-v1.6" : "payment-processor-v2"

  const params = [
    {
      ...(tokenId ? {} : { collection: collection }),
      currency: currency,
      quantity: quantity,
      weiPrice: (Math.round(Number(weiPrice))).toString(),
      expirationTime: expirationTime.toString(),
      orderKind: orderKind,
      orderbook: "reservoir",
      options: options,
      automatedRoyalties: true,
      ...(tokenId ? { token: `${collection}:${tokenId}` } : trait ? { attributeKey: trait.attributeKey, attributeValue: trait.attributeValue } : {})
    }
  ];

  const data = {
    maker: maker,
    source: "magiceden.us",
    params: params
  };

  try {
    const controller = new AbortController();
    addActiveRequest(taskId, controller);

    try {
      const { data: order } = await limiter.schedule(() => axiosInstance.post<CreateBidData>(
        `${BASE_URL}/magiceden/v3/rtp/ethereum/execute/bid/v5`,
        data,
        {
          headers: {
            'content-type': 'application/json',
            'x-api-key': API_KEY
          },
          signal: controller.signal
        }
      ));
      return order;
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
      console.log(`Request for task ${taskId} was cancelled`);
      return null;
    }
    throw error
  }
}

/**
 * Signs the order data.
 * @param wallet - The wallet of the offerer.
 * @param signData - The data to be signed.
 * @returns The signature.
 */
async function signOrderData(taskId: string, wallet: ethers.Wallet, signData: any, trait?: Trait): Promise<string> {
  try {
    if (!signData) {
      throw new Error('Asset does not exist.');
    }

    const domain = trait ? {
      "name": "Seaport",
      "version": "1.6",
      "chainId": 1,
      "verifyingContract": "0x0000000000000068f116a894984e2db1123eb395"
    } : signData && signData.domain ? signData.domain : {
      "name": "Seaport",
      "version": "1.6",
      "chainId": 1,
      "verifyingContract": "0x0000000000000068f116a894984e2db1123eb395"
    }

    const types = trait ? {
      "OrderComponents": [
        { "name": "offerer", "type": "address" },
        { "name": "zone", "type": "address" },
        { "name": "offer", "type": "OfferItem[]" },
        { "name": "consideration", "type": "ConsiderationItem[]" },
        { "name": "orderType", "type": "uint8" },
        { "name": "startTime", "type": "uint256" },
        { "name": "endTime", "type": "uint256" },
        { "name": "zoneHash", "type": "bytes32" },
        { "name": "salt", "type": "uint256" },
        { "name": "conduitKey", "type": "bytes32" },
        { "name": "counter", "type": "uint256" }
      ],
      "OfferItem": [
        { "name": "itemType", "type": "uint8" },
        { "name": "token", "type": "address" },
        { "name": "identifierOrCriteria", "type": "uint256" },
        { "name": "startAmount", "type": "uint256" },
        { "name": "endAmount", "type": "uint256" }
      ],
      "ConsiderationItem": [
        { "name": "itemType", "type": "uint8" },
        { "name": "token", "type": "address" },
        { "name": "identifierOrCriteria", "type": "uint256" },
        { "name": "startAmount", "type": "uint256" },
        { "name": "endAmount", "type": "uint256" },
        { "name": "recipient", "type": "address" }
      ]
    } : signData.types
    const signature = await wallet._signTypedData(
      domain,
      types,
      signData.value
    );
    return signature;
  } catch (error: any) {
    throw error
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
async function sendSignedOrderData(taskId: string, offerPrice: string | number, decimal: number, currencySymbol: string, bidCount: string, signature: string, data: any, slug: string, expiry: number, trait?: Trait, tokenId?: number | string) {
  try {

    const [taskId, count] = bidCount.split(":")

    const task = await currentTasks.find((task) =>
      task.contract.slug.toLowerCase() === slug.toLowerCase() &&
      task.selectedMarketplaces.includes("MagicEden")
    )
    if (!task) return

    const endpoint = trait || tokenId
      ? `${BASE_URL}/magiceden/v3/rtp/ethereum/order/v3`
      : `${BASE_URL}/magiceden/v3/rtp/ethereum/order/v4`


    // check if the tasks is still active before you make the offer
    const currentTask = activeTasks.get(taskId)
    if (!currentTask?.running || !currentTask?.selectedMarketplaces?.map((marketplace) => marketplace.toLowerCase()).includes("magiceden".toLowerCase())) return

    const controller = new AbortController();
    addActiveRequest(taskId, controller);

    try {
      const { data: offerResponse } = await limiter.schedule(() =>
        axiosInstance.post(
          `${endpoint}?signature=${encodeURIComponent(signature)}`,
          data,
          {
            headers: {
              'content-type': 'application/json',
              'x-api-key': API_KEY,
            },
            signal: controller.signal
          }
        )
      );
      const successMessage = tokenId ? `🎉 TOKEN OFFER POSTED TO MAGICEDEN SUCCESSFULLY FOR: ${slug.toUpperCase()} TOKEN: ${tokenId} ${Number(offerPrice) / Math.pow(10, decimal)} ${currencySymbol} 🎉` :
        trait ?
          `🎉 TRAIT OFFER POSTED TO MAGICEDEN SUCCESSFULLY FOR: ${slug.toUpperCase()} TRAIT: ${JSON.stringify(trait)} ${Number(offerPrice) / Math.pow(10, decimal)} ${currencySymbol} 🎉`
          : `🎉 OFFER POSTED TO MAGICEDEN SUCCESSFULLY FOR: ${slug.toUpperCase()} ${Number(offerPrice) / Math.pow(10, decimal)} ${currencySymbol} 🎉`

      const identifier = trait ? `${trait.attributeKey}:${trait.attributeValue}` : tokenId ? tokenId : 'collection';


      const payload = JSON.stringify(offerResponse);

      const task = currentTasks.find((task) => task.contract.slug.toLowerCase() === slug.toLowerCase() && task.selectedMarketplaces.includes("MagicEden"))
      if (!task) return

      await prisma.order.create({
        data: {
          slug: slug,
          orderHash: payload,
          expiresAt: new Date(Date.now() + expiry * 1000),
          offerPrice: offerPrice.toString(),
          identifier: identifier.toString(),
          taskId: taskId,
          marketplace: "magiceden"
        }
      })
      trackBidRate("magiceden", taskId)
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
      console.log(`Request for task ${taskId} was cancelled`);
      return null;
    }
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
export async function submitSignedOrderData(taskId: string, offerPrice: string | number, decimal: number, currencySymbol: string, bidCount: string, order: CreateBidData, wallet: ethers.Wallet, slug: string, expiry: number, trait?: Trait, tokenId?: number | string) {
  const task = currentTasks.find((task) => task.contract.slug.toLowerCase() === slug.toLowerCase() && task.selectedMarketplaces.includes("MagicEden"))
  if (!task) return

  if (order && Array.isArray(order.steps)) {
    order.steps.forEach((step) => {
      if (Array.isArray(step.items)) {
        step.items.forEach((item) => {
          if (item.data?.post?.body) {
            item.data.post.body.source = "magiceden.us";
          }
        });
      }
    });
  }

  if (order && Array.isArray(order.steps)) {
    order.steps.forEach((step) => {
      if (Array.isArray(step.items)) {
        step.items.forEach((item) => {
          if (Array.isArray(item.data?.sign?.value?.consideration)) {
            item.data.sign.value.consideration.forEach((considerationItem) => {
              if (considerationItem.recipient.toLowerCase() === "0x6fa303e72bed54f515a513496f922bc331e2f27e".toLowerCase()) {
                considerationItem.recipient = "0xca9337244b5f04cb946391bc8b8a980e988f9a6a";
              }
            });
          }
          if (Array.isArray(item.data?.post?.body?.order?.data?.consideration)) {
            item.data?.post?.body?.order?.data?.consideration?.forEach((considerationItem) => {
              if (considerationItem.recipient.toLowerCase() === "0x6fa303e72bed54f515a513496f922bc331e2f27e".toLowerCase()) {
                considerationItem.recipient = "0xca9337244b5f04cb946391bc8b8a980e988f9a6a".toLowerCase();
              }
            });
          }
        });
      }
    });
  }

  try {
    const signData = order?.steps
      ?.find((step) => step.id === "order-signature")
      ?.items?.[0]?.data?.sign;

    const signature = await signOrderData(taskId, wallet, signData, trait);
    let data: any;

    if (trait) {
      const orderSignatureStep = order.steps.find(step => step.id === "order-signature") as any;
      const attributeStep = order.steps.find(step => step.id === "order-signature") as any
      const attribute = attributeStep?.items[0]?.data?.post?.body?.attribute;
      const valueObject = orderSignatureStep.items[0].data.sign.value;

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
        "source": "magiceden.us"
      };
    } else if (tokenId) {
      data = order?.steps
        ?.find((step) => step.id === "order-signature")
        ?.items?.[0]?.data?.post.body;
    } else {
      if (signData) {
        const payload = signData.value;
        const { buyer, ...rest } = payload;
        data = {
          items: [
            {
              order: {
                kind: "payment-processor-v2",
                data: {
                  kind: "collection-offer-approval",
                  sellerOrBuyer: buyer,
                  ...rest,
                  r: "0x0000000000000000000000000000000000000000000000000000000000000000",
                  s: "0x0000000000000000000000000000000000000000000000000000000000000000",
                  v: 0,
                },
              },
              orderbook: "reservoir",
            },
          ],
          source: "magiceden.us",
        };
      } else {
        console.error('Sign data not found in order steps.');
      }

    }

    const result = await sendSignedOrderData(taskId, offerPrice, decimal, currencySymbol, bidCount, signature, data, slug, expiry, trait, tokenId);
    return result;
  } catch (error: any) {
    throw error
  }

}

export async function cancelMagicEdenBid(orderIds: string[], privateKey: string, taskId: string) {
  try {
    if (!orderIds?.length) return;
    const processedOrderIds = await Promise.all(orderIds?.map(async (orderId: any) => {
      try {
        const parsed = JSON.parse(orderId);
        return parsed.orderId || orderId;
      } catch (error: any) {
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
      console.error('Failed to generate signature for cancel order');
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
    console.log(JSON.stringify(cancelResponse));
    console.log(`Successfully cancelled ${orderIds.length} MagicEden bids for task: ${taskId}`.toUpperCase());
    orderIds.forEach(() => {
      decrementBidCount('magiceden', taskId);
    });

  } catch (error: any) {
    const message = `Error cancelling MagicEden bid: ${error?.response?.data?.message || error?.message || error}`
    console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
    console.log(RED + message + RESET)
    console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
  }
}


async function signCancelOrder(taskId: string, cancelItem: any | undefined, privateKey: string) {
  try {
    if (!cancelItem) return

    const wallet = new Wallet(privateKey, provider);

    const signature = await wallet._signTypedData(
      cancelItem.domain,
      cancelItem.types,
      cancelItem.value
    );

    return signature;
  } catch (error: any) {
    throw error
  }
}

export async function fetchMagicEdenOffer(type: "COLLECTION" | "TRAIT" | "TOKEN", contractAddress: string, identifier?: string | {
  attributeKey: string;
  attributeValue: string;
}) {
  try {
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
      return offers
    }
  } catch (error: any) {
    throw error
  }
}

export async function fetchMagicEdenCollectionStats(contractAddress: string) {
  const CACHE_TTL = 30; // 30 seconds cache
  const cacheKey = `magiceden:stats:${contractAddress}`;
  const lockKey = `stats:${contractAddress}`;

  // Try to get from cache first
  try {
    const cachedStats = await redis.get(cacheKey);
    if (cachedStats) {
      return JSON.parse(cachedStats);
    }
  } catch (error) {
    console.error('Error reading from cache:', error);
  }

  // If not in cache, try to acquire lock and fetch
  const result = await lockManager.withLock(lockKey, async () => {
    // Double check cache after acquiring lock
    try {
      const cachedStats = await redis.get(cacheKey);
      if (cachedStats) {
        return JSON.parse(cachedStats);
      }
    } catch (error) {
      console.error('Error reading from cache after lock:', error);
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

    try {
      const { data } = await limiter.schedule(() =>
        axiosInstance.get(`${BASE_URL}/magiceden/v3/rtp/ethereum/tokens/v7`, {
          params: queryParams,
          headers: {
            'x-api-key': API_KEY
          }
        })
      );

      const stats = data?.tokens[0]?.market?.floorAsk?.price?.amount?.decimal;

      // Cache the result
      if (stats !== undefined) {
        try {
          await redis.set(cacheKey, JSON.stringify(stats), 'EX', CACHE_TTL);
        } catch (error) {
          console.error('Error writing to cache:', error);
        }
      }

      return stats;
    } catch (error: any) {
      throw error;
    }
  });

  if (!result) {
    // If we couldn't acquire the lock, wait briefly and try cache again
    await new Promise(resolve => setTimeout(resolve, 100));
    try {
      const cachedStats = await redis.get(cacheKey);
      if (cachedStats) {
        return JSON.parse(cachedStats);
      }
    } catch (error) {
      console.error('Error reading from cache after lock failure:', error);
    }
    throw new Error('Failed to fetch collection stats');
  }

  return result;
}

export async function fetchMagicEdenTokens(taskId: string, collectionId: string, limit?: number) {
  const lockKey = `tokens:${collectionId} `;

  return await lockManager.withLock(
    lockKey,
    async () => {
      const startTime = Date.now();
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
      try {
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

        return allTokens.slice(0, limit);
      } catch (error: any) {
        throw error
      }
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

interface TypesCancel {
  OrderHashes: OrderHashCancel[];
}

interface OrderHashCancel {
  name: string;
  type: string;
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

interface OrderData {
  kind: string;
  sellerOrBuyer: string;
  protocol: number;
  cosigner: string;
  beneficiary: string;
  marketplace: string;
  fallbackRoyaltyRecipient: string;
  paymentMethod: string;
  tokenAddress: string;
  amount: string;
  itemPrice: string;
  expiration: string;
  marketplaceFeeNumerator: string;
  nonce: string;
  masterNonce: string;
  r: string;
  s: string;
  v: number;
}

interface Order {
  kind: string;
  data: OrderData;
}

interface Item {
  order: Order;
  orderbook: string;
}

interface SignedData {
  items: Item[];
  source: string;
}

interface Trait {
  attributeKey: string;
  attributeValue: string;
};



interface MagicEdenTokenOrder {
  id: string;
  kind: string;
  side: 'buy' | 'sell';
  status: 'active' | string;
  tokenSetId: string;
  tokenSetSchemaHash: string;
  contract: string;
  contractKind: string;
  maker: string;
  taker: string;
  price: MagicEdenTokenPrice;
  validFrom: number;
  validUntil: number;
  quantityFilled: number;
  quantityRemaining: number;
  criteria: MagicEdenTokenCriteria;
  source: MagicEdenTokenSource;
  feeBps: number;
  feeBreakdown: MagicEdenTokenFeeBreakdown[];
  expiration: number;
  isReservoir: boolean | null;
  createdAt: string;
  updatedAt: string;
  originatedAt: string | null;
  isNativeOffChainCancellable: boolean;
}

interface MagicEdenTokenPrice {
  currency: MagicEdenTokenCurrency;
  amount: MagicEdenTokenAmount;
  netAmount: MagicEdenTokenAmount;
}

interface MagicEdenTokenCurrency {
  contract: string;
  name: string;
  symbol: string;
  decimals: number;
}

interface MagicEdenTokenAmount {
  raw: string;
  decimal: number;
  usd: number;
  native: number;
}

interface MagicEdenTokenCriteria {
  kind: 'token' | 'collection' | 'attribute';
  data: MagicEdenTokenCriteriaData;
}

interface MagicEdenTokenCriteriaData {
  token?: {
    tokenId: string;
  };
  collection?: {
    id: string;
  };
  attribute?: {
    key: string;
    value: string;
  };
}

interface MagicEdenTokenSource {
  id: string;
  domain: string;
  name: string;
  icon: string;
  url: string;
}

interface MagicEdenTokenFeeBreakdown {
  kind: 'marketplace' | 'royalty';
  recipient: string;
  bps: number;
}

interface MagicEdenTokenResponse {
  orders: MagicEdenTokenOrder[];
  continuation: string | null;
}

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

interface QueryParams {
  includeQuantity: boolean;
  includeLastSale: boolean;
  excludeSpam: boolean;
  excludeBurnt: boolean;
  collection: string;
  sortBy: string;
  sortDirection: string;
  limit: number;
  includeAttributes: boolean;
  excludeSources: string[];
  [key: string]: any; // This allows for dynamic keys
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

interface FeeBreakdown {
  kind: 'royalty' | 'marketplace';
  bps: number;
  recipient: string;
  rawAmount: string;
  source?: string;
}

interface LastSale {
  orderSource: string;
  fillSource: string;
  timestamp: number;
  price: {
    currency: Currency;
    amount: PriceAmount;
    netAmount: PriceAmount;
  };
  royaltyFeeBps: number;
  marketplaceFeeBps: number;
  paidFullRoyalty: boolean;
  feeBreakdown: FeeBreakdown[];
}

interface Collection {
  id: string;
  name: string;
  image: string;
  slug: string;
  symbol: string;
  creator: string;
  tokenCount: number;
  metadataDisabled: boolean;
  floorAskPrice: Price;
}

interface Token {
  chainId: number;
  contract: string;
  tokenId: string;
  name: string;
  description: string;
  image: string;
  imageSmall: string;
  imageLarge: string;
  media: null;
  kind: string;
  isFlagged: boolean;
  isSpam: boolean;
  isNsfw: boolean;
  metadataDisabled: boolean;
  lastFlagUpdate: string;
  lastFlagChange: null;
  supply: string;
  remainingSupply: string;
  rarity: number;
  rarityRank: number;
  collection: Collection;
  lastSale: LastSale;
  owner: string;
  mintedAt: string;
  createdAt: string;
  decimals: null;
  mintStages: any[];
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

interface Market {
  floorAsk: FloorAsk;
}

interface Media {
  image: string;
}

interface TokenData {
  token: Token;
  market: Market;
  updatedAt: string;
  media: Media;
}

export function cancelMagicedenRequest(taskId: string): boolean {
  const controllers = activeRequests.get(taskId);
  if (controllers && controllers.length > 0) {
    controllers.forEach(controller => controller.abort());
    activeRequests.delete(taskId);
    console.log(`Aborted ${controllers.length} MagicEden requests for task ${taskId}`);
    return true;
  }
  console.log(`No active MagicEden requests found for task ${taskId}`);
  return false;
}

function addActiveRequest(taskId: string, controller: AbortController) {
  if (!activeRequests.has(taskId)) {
    activeRequests.set(taskId, []);
  }
  activeRequests.get(taskId)?.push(controller);
}