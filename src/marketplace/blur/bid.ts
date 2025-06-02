import { BigNumber, ethers, utils, Wallet } from "ethers";
import { axiosInstance, limiter } from "../../init";
import { activeTasks, API_KEY, currentTasks, decrementBidCount, errorStats, logBidError, pQueue, redis, RESET, trackBidRate } from "../..";
import { config } from "dotenv";
import { createBalanceChecker } from "../../utils/balance";
import { DistributedLockManager } from "../../utils/lock";
import { PrismaClient } from '@prisma/client'
import axios from "axios";
import { BASE_URL } from "../../constants";

const prisma = new PrismaClient()

config()
const BLUR_API_URL = `${BASE_URL}/blur`;
const ALCHEMY_API_KEY = process.env.ALCHEMY_API_KEY as string;

const deps = {
  redis: redis,
  provider: new ethers.providers.AlchemyProvider("mainnet", ALCHEMY_API_KEY),
};
const balanceChecker = createBalanceChecker(deps);
const provider = new ethers.providers.AlchemyProvider('mainnet', ALCHEMY_API_KEY);

const lockManager = new DistributedLockManager({
  lockPrefix: '{blur}:lock:',
  defaultTTLSeconds: 60 // 1 minute lock timeout
});

const activeRequests = new Map<string, AbortController[]>();

function addActiveRequest(taskId: string, controller: AbortController) {
  if (!activeRequests.has(taskId)) {
    activeRequests.set(taskId, []);
  }
  activeRequests.get(taskId)?.push(controller);
}

export function cancelBlurRequest(taskId: string): boolean {
  const controllers = activeRequests.get(taskId);
  if (controllers && controllers.length > 0) {
    controllers.forEach(controller => controller.abort());
    activeRequests.delete(taskId);
    console.log(`Aborted ${controllers.length} Blur requests for task ${taskId}`);
    return true;
  }
  console.log(`No active Blur requests found for task ${taskId}`);
  return false;
}

/**
 * Creates an offer on Blur.
 * @param walletAddress - The wallet address of the offerer.
 * @param privateKey - The private key of the offerer's wallet.
 * @param contractAddress - The contract address of the collection.
 * @param offerPrice - The price of the offer in wei.
 * @param traits - Optional traits for the offer.
 */
export async function bidOnBlur(
  taskId: string,
  bidCount: string,
  wallet_address: string,
  private_key: string,
  contractAddress: string,
  offer_price: BigNumber | bigint,
  slug: string,
  expiry = 900,
  traits?: string
) {
  const bethBalance = await balanceChecker.getBethBalance(wallet_address);
  let offerPriceEth: string = (Number(offer_price) / 1e18).toString()
  // const leverage = 200
  // const totalOfferAmount = await getCachedOfferAmount(OFFER_AMOUNT_KEY);
  // if (totalOfferAmount + offerPriceEth >= leverage * bethBalance) {
  //   const jobs: Job[] = await queue.getJobs(['prioritized']);
  //   const blurJobs = jobs.filter(job =>
  //     [BLUR_SCHEDULE, BLUR_TRAIT_BID].includes(job.name)
  //   );

  //   if (blurJobs.length > 0) {
  //     await queue.pause()
  //     await Promise.all(blurJobs.map(job => job.remove()));
  //     console.log(RED + `REMOVING ${blurJobs.length} BLUR JOB(S) DUE TO INSUFFICIENT BETH BALANCE` + RESET);
  //     await queue.resume()
  //   }
  //   return
  // }

  if (Number(offerPriceEth) > bethBalance) {
    const message = `Offer price: ${offerPriceEth} BETH  is greater than available BETH balance: ${bethBalance} BETH. SKIPPING ...`
    throw new Error(message)
  }

  const offerPrice = BigNumber.from(offer_price.toString());
  const accessToken = await getAccessToken(BLUR_API_URL, private_key);

  offerPriceEth = (Math.floor(Number(utils.formatUnits(offerPrice)) * 100) / 100).toFixed(2);

  if (Number(offerPriceEth) === 0) {
    const message = `Offer price is less than the minimum Blur offer price. SKIPPING ...`.toUpperCase()

    if (!errorStats[taskId]) {
      errorStats[taskId] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      }
    }
    errorStats[taskId].blur++
    await logBidError(taskId, "INVALID BID PRICE", message, "error", "blur");
    return
  }

  const wallet = new Wallet(private_key, provider);
  const basePayload = {
    price: {
      unit: 'BETH',
      amount: offerPriceEth,
    },
    quantity: 1,
    expirationTime: new Date(Date.now() + (expiry * 1000)).toISOString(),
    contractAddress: contractAddress,
  };

  const buildPayload = traits ? {
    ...basePayload,
    criteria: {
      type: "TRAIT",
      value: JSON.parse(traits)
    }
  } : basePayload;

  let build: any;

  if (!accessToken) {
    throw new Error('Access token is undefined');
  }
  build = await formatBidOnBlur(taskId, BLUR_API_URL, accessToken, wallet_address, buildPayload);


  let data = build?.signatures?.[0];
  if (!data) {
    build = await formatBidOnBlur(taskId, BLUR_API_URL, accessToken, wallet_address, buildPayload);
    data = build?.signatures?.[0];
  }
  if (!data) {
    return;
  }

  const signObj = await wallet._signTypedData(
    data?.signData?.domain,
    data?.signData?.types,
    data?.signData?.value
  );

  const submitPayload = {
    signature: signObj,
    marketplaceData: data?.marketplaceData,
  };

  const cancelPayload = {
    contractAddress,
    criteriaPrices: [
      {
        price: offerPriceEth,
        criteria: {
          "type": traits ? "TRAIT" : "COLLECTION",
          value: traits ? JSON.parse(traits) : {}
        }
      }
    ]
  }
  await submitBidToBlur(taskId, bidCount, offer_price, BLUR_API_URL, accessToken, wallet_address, submitPayload, slug, cancelPayload, expiry, traits);
};

/**
 * Gets an access token.
 * @param url - The URL to get the access token from.
 * @param privateKey - The private key of the wallet.
 * @returns The access token.
 */
async function getAccessToken(url: string, private_key: string): Promise<string | undefined> {
  const wallet = new Wallet(private_key, provider);
  const lockKey = `auth:${wallet.address}`;

  return await lockManager.withLock(lockKey, async () => {
    const options = { walletAddress: wallet.address };
    const headers = {
      'content-type': 'application/json',
      'x-api-key': API_KEY
    };

    const key = `{blurAccessToken}:${wallet.address}`
    const cachedToken = await redis.get(key);
    if (cachedToken) {
      return cachedToken;
    }

    let response: any = await limiter.schedule(() => axiosInstance
      .post(`${url}/auth/challenge`, options, { headers }));
    const message = response.data.message;
    const signature = await wallet.signMessage(message);
    const data = {
      message: message,
      walletAddress: wallet.address,
      expiresOn: response.data.expiresOn,
      hmac: response.data.hmac,
      signature: signature
    };
    response = await limiter.schedule(() => axiosInstance
      .post(`${url}/auth/login`, data, { headers }));
    const accessToken = response.data.accessToken;
    await redis.set(key, accessToken, 'EX', 5 * 60);

    return accessToken;

  });
};

/**
 * Sends a request to format a bid on Blur.
 * @param url - The URL to send the request to.
 * @param accessToken - The access token for authentication.
 * @param walletAddress - The wallet address of the offerer.
 * @param buildPayload - The payload for the bid.
 * @returns The formatted bid data.
 */
async function formatBidOnBlur(
  taskId: string,
  url: string,
  accessToken: string,
  walletAddress: string,
  buildPayload: any
) {
  const controller = new AbortController();
  addActiveRequest(taskId, controller);

  try {
    const { data } = await limiter.schedule(() =>
      axiosInstance.request<BlurBidResponse>({
        method: 'POST',
        url: `${url}/v1/collection-bids/format`,
        headers: {
          'content-type': 'application/json',
          authToken: accessToken,
          walletAddress: walletAddress.toLowerCase(),
          'x-api-key': API_KEY,
        },
        data: JSON.stringify(buildPayload),
        signal: controller.signal
      })
    );
    return data;
  } catch (error: any) {
    if (axios.isCancel(error)) {
      console.log(`Request for task ${taskId} was cancelled`);
      return null;
    }
    throw error
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

/**
 * Submits a bid to Blur.
 * @param url - The URL to send the request to.
 * @param accessToken - The access token for authentication.
 * @param walletAddress - The wallet address of the offerer.
 * @param submitPayload - The payload for the bid submission.
 * @param slug - The slug of the collection.
 */
async function submitBidToBlur(
  taskId: string,
  bidCount: string,
  offer_price: BigNumber | bigint,
  url: string,
  accessToken: string,
  walletAddress: string,
  submitPayload: SubmitPayload,
  slug: string,
  cancelPayload: any,
  expiry = 900,
  traits?: string
) {
  const controller = new AbortController();
  addActiveRequest(taskId, controller);

  try {
    const [taskId] = bidCount.split(":")

    const currentTask = activeTasks.get(taskId)
    if (!currentTask?.running || !currentTask.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("BLUR".toLowerCase())) return

    let running = currentTasks.find((task) => task.contract.slug.toLowerCase() === slug.toLowerCase())
    if (!running) return
    const { data: offers } = await limiter.schedule(() =>
      axiosInstance.request({
        method: 'POST',
        url: `${url}/v1/collection-bids/submit`,
        headers: {
          'content-type': 'application/json',
          authToken: accessToken,
          walletAddress: walletAddress.toLowerCase(),
          'x-api-key': API_KEY,
        },
        data: JSON.stringify(submitPayload),
        signal: controller.signal
      })
    );


    console.log({ offers });




    if (offers.success) {
      const successMessage = traits ? `🎉 TRAIT OFFER POSTED TO BLUR SUCCESSFULLY FOR: ${slug.toUpperCase()} 🎉 TRAIT: ${traits}` : `🎉 OFFER POSTED TO BLUR SUCCESSFULLY FOR: ${slug.toUpperCase()} 🎉`
      console.log("\x1b[33m", successMessage, RESET);

      let identifier = ''
      if (traits) {
        const traitsObj = JSON.parse(traits);
        const [traitType, traitValue] = Object.entries(traitsObj)[0];
        identifier = `${traitType}:${traitValue}`
      } else {
        identifier = "collection"
      }

      await prisma.order.create({
        data: {
          slug: slug,
          orderHash: JSON.stringify(cancelPayload),
          expiresAt: new Date(Date.now() + expiry * 1000),
          offerPrice: offer_price.toString(),
          identifier: identifier,
          taskId: taskId,
          marketplace: "blur"
        }
      })
      trackBidRate("blur", taskId)
    }

    if (offers.errors) {
      console.error('Error:', JSON.stringify(offers.errors));
    }
  } catch (error: any) {
    if (axios.isCancel(error)) {
      console.log(`Request for task ${taskId} was cancelled`);
      return;
    }
    throw error
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

export async function cancelBlurBid(data: BlurCancelPayload) {
  try {
    if (!data || !data.payload || !data.privateKey) return
    const { payload, privateKey, taskId } = data
    const wallet = new Wallet(privateKey, provider);
    const walletAddress = wallet.address
    const accessToken = await getAccessToken(BLUR_API_URL, privateKey);
    const endpoint = `${BLUR_API_URL}/v1/collection-bids/cancel`
    const { data: cancelResponse } = await limiter.schedule(() => axiosInstance.post(endpoint, payload, {
      headers: {
        'content-type': 'application/json',
        authToken: accessToken,
        walletAddress: walletAddress.toLowerCase(),
        'x-api-key': API_KEY,
      }
    }))
    decrementBidCount('blur', taskId);
    console.log(JSON.stringify(cancelResponse));
  } catch (error: any) {
    if (error.response?.data?.message?.message !== 'No bids found') {
      console.log("cancelBlurBid: ", error?.response?.data || error?.response || error?.code);
    }

    // console.log(error.response.data || error.message);
  }
}

export async function fetchBlurBid(collection: string, criteriaType: 'TRAIT' | 'COLLECTION', criteriaValue: Record<string, string>) {
  const url = `${BASE_URL}/blur/v1/collections/${collection}/executable-bids`;

  if (criteriaType === 'TRAIT') {
    const tasks = [
      () => limiter.schedule(() => axiosInstance.get<BlurBidResponse>(url, {
        params: {
          filters: JSON.stringify({
            criteria: {
              type: 'TRAIT',
              value: criteriaValue
            }
          })
        },
        headers: {
          'content-type': 'application/json',
          'x-api-key': API_KEY,
        }
      })),
      () => limiter.schedule(() => axiosInstance.get<BlurBidResponse>(url, {
        params: {
          filters: JSON.stringify({
            criteria: {
              type: 'COLLECTION',
              value: {}
            }
          })
        },
        headers: {
          'content-type': 'application/json',
          'x-api-key': API_KEY,
        }
      }))
    ];

    const [traitResponse, collectionResponse] = await pQueue.addAll(tasks, {
      throwOnTimeout: true,
    });

    const traitData = traitResponse.data;
    const collectionData = collectionResponse.data;

    // Combine trait and collection price levels
    const combinedData = {
      success: traitData.success && collectionData.success,
      priceLevels: [
        ...(traitData.priceLevels || []),
        ...(collectionData.priceLevels || [])
      ].sort((a, b) => Number(b.price) - Number(a.price))
    };

    return combinedData;

  } else {
    const { data } = await limiter.schedule(() => axiosInstance.get<BlurBidResponse>(url, {
      params: {
        filters: JSON.stringify({
          criteria: {
            type: 'COLLECTION',
            value: {}
          }
        })
      },
      headers: {
        'content-type': 'application/json',
        'x-api-key': API_KEY,
      }
    }));
    return data;
  }


}


export async function fetchBlurCollectionStats(slug: string) {
  const CACHE_TTL = 30; // 30 seconds cache
  const cacheKey = `blur:stats:${slug}`;
  const lockKey = `stats:${slug}`;

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

    const cachedStats = await redis.get(cacheKey);
    if (cachedStats) {
      return JSON.parse(cachedStats);
    }
    const url = `${BASE_URL}/blur/v1/collections/${slug}/tokens`;
    const { data } = await limiter.schedule(() => axiosInstance.get<BlurTokensResponse>(url, {
      headers: {
        'content-type': 'application/json',
        'x-api-key': API_KEY,
      }
    }));

    const listings = data?.tokens?.sort((a, b) => Number(a?.price?.amount) - Number(b?.price?.amount));
    const floor_price = Number(listings[0]?.price?.amount) || 0;

    await redis.set(cacheKey, JSON.stringify(floor_price), 'EX', CACHE_TTL);
    return floor_price || 0;

  });

  if (!result) {
    return 0
  }
  return result;
}

export async function fetchBlurTraitFloor(slug: string, trait: { type: string; value: string; }) {
  try {
    const filters = {
      traits: [{
        type: trait.type,
        values: [trait.value],
      }],
      hasAsks: true,
    };
    const encodedFilters = encodeURIComponent(JSON.stringify(filters));

    const url = `${BASE_URL}/blur/v1/collections/${slug}/tokens?filters=${encodedFilters}`;
    const { data } = await limiter.schedule(() =>
      axiosInstance.get<BlurTokensResponse>(url, {
        headers: {
          'x-api-key': API_KEY,
        },
      })
    );
    const amount = data?.tokens[0]?.price?.amount;
    return amount !== undefined ? Number(amount) : undefined
  } catch (error) {
    console.log(error);
  }
}


interface PriceLevel {
  criteriaType: string;
  criteriaValue: Record<string, unknown>;
  price: string;
  executableSize: number;
  numberBidders: number;
  bidderAddressesSample: any[];
}

interface BlurBidResponse {
  success: boolean;
  priceLevels: PriceLevel[];
}

interface BlurCancelPayload {
  payload: {
    contractAddress: string;
    criteriaPrices: Array<{
      price: string;
      criteria?: {
        type: string;
        value: Record<string, string>;
      }
    }>;
  };
  privateKey: string;
  taskId: string;
}

interface SubmitPayload {
  signature: string;
  marketplaceData: string[];
  [key: string]: any;
}


export interface BlurTokensResponse {
  success: boolean;
  contractAddress: string;
  totalCount: number;
  tokens: IBlurToken[];
}

export interface IBlurToken {
  tokenId: string;
  name: string;
  imageUrl: string;
  traits: {
    Tier?: string;
    State: string;
    [key: string]: string | undefined;
  };
  rarityScore: number;
  rarityRank: number;
  price: TokenPrice | null;
  highestBid: TokenPrice | null;
  lastSale: TokenPrice | null;
  lastCostBasis: TokenPrice | null;
  owner: {
    address: string;
    username: string | null;
  };
  numberOwnedByOwner: number;
  isSuspicious: boolean;
}

export interface TokenPrice {
  amount: string;
  unit: string;
  listedAt: string;
  marketplace?: string;
}
