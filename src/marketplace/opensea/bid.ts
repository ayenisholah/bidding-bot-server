import { BigNumber, ethers, Wallet } from "ethers";
import { SEAPORT_CONTRACT_ADDRESS, SEAPORT_MIN_ABI } from "../../constants";
import { axiosInstance, limiter } from "../../init";
import { activeTasks, AddressBalancesResponse, API_KEY, BASE_URL, BLUE, currentTasks, decrementBidCount, logBidError, markTaskAsAborted, OPENSEA_SCHEDULE, OPENSEA_TOKEN_BID, OPENSEA_TRAIT_BID, pQueue, queue, redis, trackBidRate } from "../..";
import { config } from "dotenv";
import { createBalanceChecker } from "../../utils/balance";
import { Job } from "bullmq";
import axios from "axios";
import { PrismaClient } from '@prisma/client'
import { USDC, WETH } from "../../utils/currencies";
import { MagicedenCollectionResponse, MagicEdenTraitOfferResponse } from "../magiceden";

const prisma = new PrismaClient()

config()

const OPENSEA_ITEM_ZONE = "0x000056f7000000ece9003ca63978907a00ffd100"
const OPENSEA_COLLECTION_ZONE = "0x004C00500000aD104D7DBd00e3ae0A5C00560C00"
const ZONE_HASH = "0x0000000000000000000000000000000000000000000000000000000000000000"
const CONDUIT_KEY = "0x0000007b02230091a7ed01230072f7006a004d60a8d4e71d599b8104250f0000"
const SEAPORT_1_6 = "0x0000000000000068f116a894984e2db1123eb395"
const OPENSEA_FEE_ADDRESS = "0x0000a26b00c1F0DF003000390027140000fAa719"
const ALCHEMY_API_KEY = process.env.ALCHEMY_API_KEY as string;
const provider = new ethers.providers.AlchemyProvider('mainnet', ALCHEMY_API_KEY);
const SEAPORT_CONTRACT = new ethers.Contract(SEAPORT_CONTRACT_ADDRESS, SEAPORT_MIN_ABI, provider);
const RED = '\x1b[31m';
const RESET = '\x1b[0m';

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


const deps = {
  redis: redis,
  provider: new ethers.providers.AlchemyProvider("mainnet", ALCHEMY_API_KEY),
};

const balanceChecker = createBalanceChecker(deps);

const activeRequests = new Map<string, AbortController[]>();

async function buildItemOffer(taskId: string, offerSpecification: ItemOfferSpecification, currency: string) {
  try {
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
  } catch (error: any) {
    throw error
  }
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
  try {
    const divider = BigNumber.from(10000);
    const decimal = currency.toLowerCase() === USDC.toLowerCase() ? 6 : 18
    const currencySymbol = currency.toLowerCase() === USDC.toLowerCase() ? "USDC" : "WETH"

    const task = activeTasks.get(taskId)
    if (!task || !task.running || !task.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("opensea")) return

    interface TotalOfferAmount {
      [currency: string]: number;
    }
    const totalOfferAmount: TotalOfferAmount = {};
    const wethBalance = await balanceChecker.getERC20Balance(wallet_address, currency);
    for (const task of currentTasks) {
      const openseaOrders = await prisma.order.findMany({
        where: {
          taskId: task._id,
          marketplace: 'opensea',
          expiresAt: {
            gt: new Date()
          }
        }
      });

      for (const order of openseaOrders) {
        totalOfferAmount[currency] += Number(order.offerPrice) / Math.pow(10, decimal); // Convert from wei to ETH
      }
    }
    const offerPriceEth = Number(offer_price) / Math.pow(10, decimal);

    const leverage = 1000;

    if (totalOfferAmount[currency] + offerPriceEth >= leverage * wethBalance) {
      markTaskAsAborted(taskId)
      cancelOpenseaRequest(taskId)
      const jobs = await queue.getJobs(['prioritized']);
      const openseaJobs: Job[] = jobs.filter(job =>
        [OPENSEA_SCHEDULE, OPENSEA_TRAIT_BID, OPENSEA_TOKEN_BID].includes(job?.name)
      );

      if (openseaJobs.length > 0) {
        await queue.pause()
        await Promise.allSettled(openseaJobs.map(job => job.remove()));
        await queue.resume()
      }
      const identifier = opensea_traits ? opensea_traits : asset?.tokenId ? asset?.tokenId : slug;
      throw new Error(`INSUFFICIENT ${currencySymbol} BALANCE: Total offer amount: ${totalOfferAmount[currency]} + offer price: ${offerPriceEth} is greater than the leverage: ${leverage} * ${currencySymbol} balance: ${wethBalance} for ${slug.toUpperCase()}${identifier ? ` - ${identifier}` : ''}`);
    }

    // Determine precision based on price range

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
    const offerPrice = BigNumber.from(roundedWei.toString());
    const offerPriceEthFinal = Number(roundedWei) / Math.pow(10, decimal);

    if (offerPriceEthFinal > wethBalance) {
      const identifier = opensea_traits ? opensea_traits : asset?.tokenId ? asset?.tokenId : slug;
      throw new Error(`[${slug.toUpperCase()}${identifier ? ` - ${identifier}` : ''}] Offer price: ${offerPriceEthFinal} ${currencySymbol} is greater than available ${currencySymbol} balance: ${wethBalance} ${currencySymbol}.`);
    }

    const wallet = new Wallet(private_key, provider);
    const openseaFee = BigNumber.from(50);

    if (asset) {
      const offer = await buildItemOffer(task._id, {
        assetContractAddress: asset.contractAddress,
        tokenId: asset.tokenId.toString(),
        walletAddress: wallet_address,
        quantity: 1,
        expirationSeconds: BigInt(expiry),
        priceWei: BigInt(roundedWei)
      }, currency)

      if (!offer) return

      const opensea_consideration = {
        itemType: 1,
        token: currency,
        identifierOrCriteria: "0",
        startAmount: +offerPrice.mul(openseaFee).div(divider).toString(),
        endAmount: +offerPrice.mul(openseaFee).div(divider).toString(),
        recipient: OPENSEA_FEE_ADDRESS
      };

      offer.consideration.push(opensea_consideration);
      offer.totalOriginalConsiderationItems = (Number(offer.totalOriginalConsiderationItems) + 1).toString();
      for (const address in creator_fees) {
        let fee: BigNumber | number = creator_fees[address];
        fee = BigNumber.from(Math.round(fee).toString());
        if (enforceCreatorFee) {
          const consideration_item = {
            itemType: 1,
            token: currency,
            identifierOrCriteria: "0",
            startAmount: Number(offerPrice.mul(fee).div(divider).toString()),
            endAmount: Number(offerPrice.mul(fee).div(divider).toString()),
            recipient: address
          };
          offer.consideration.push(consideration_item);
          offer.totalOriginalConsiderationItems = (Number(offer.totalOriginalConsiderationItems) + 1).toString();
        }
      }
      const itemSignature = await signOffer(wallet, offer)

      if (!itemSignature) return

      // check if the tasks is still active before you make the offer
      const [taskId] = bidCount.split(":")
      if (!task || !task?.running || !task.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("OPENSEA".toLowerCase())) return

      const itemResponse = await postItemOffer(taskId, offer, itemSignature, slug)
      const itemOrderHash = itemResponse?.order?.order_hash

      const sanitizedExpiry = expiry > 900 ? expiry : 900

      if (offerPrice && itemOrderHash && sanitizedExpiry && slug && taskId && asset.tokenId) {

        await prisma.order.create({
          data: {
            slug: slug,
            orderHash: itemOrderHash,
            expiresAt: new Date(Date.now() + sanitizedExpiry * 1000),
            offerPrice: offerPrice.toString(),
            identifier: asset.tokenId.toString(),
            taskId: taskId,
            marketplace: 'opensea'
          }
        })

      }

      trackBidRate('opensea', taskId);
      const successMessage = `🎉 TOKEN OFFER POSTED TO OPENSEA SUCCESSFULLY FOR: ${slug.toUpperCase()}  TOKEN: ${asset.tokenId} ${Number(offerPriceEthFinal)} ${currencySymbol} 🎉`
      console.log(BLUE, successMessage, RESET);
    }
    else {
      const divider = BigNumber.from(10000);
      const offerPrice = BigNumber.from(roundedWei.toString());

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

      if (opensea_traits && typeof opensea_traits !== undefined) {
        payload.criteria.trait = JSON.parse(opensea_traits)
      } else {
        delete payload.criteria.trait
      }

      const buildPayload = {
        quantity: 1,
        criteria: payload.criteria,
        offerer: wallet_address,
        protocol_address: SEAPORT_1_6
      };

      try {
        const data = await buildOffer(taskId, buildPayload)


        if (!data || !data.partialParameters) return
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
          startAmount: offerPrice.mul(openseaFee).div(divider).toString(),
          endAmount: offerPrice.mul(openseaFee).div(divider).toString(),
          recipient: OPENSEA_FEE_ADDRESS
        };
        payload.protocol_data.parameters.consideration.push(opensea_consideration);

        for (const address in creator_fees) {
          let fee: BigNumber | number = creator_fees[address];
          fee = BigNumber.from(Math.round(fee).toString());
          if (enforceCreatorFee) {
            const consideration_item = {
              itemType: 1,
              token: currency,
              identifierOrCriteria: 0,
              startAmount: offerPrice.mul(fee).div(divider).toString(),
              endAmount: offerPrice.mul(fee).div(divider).toString(),
              recipient: address
            };

            payload.protocol_data.parameters.consideration.push(consideration_item);
            payload.protocol_data.parameters.totalOriginalConsiderationItems += 1;
          }
        }

        payload.protocol_data.parameters.zone = data.partialParameters.zone;
        payload.protocol_data.parameters.zoneHash = data.partialParameters.zoneHash;
        payload.protocol_data.parameters.salt = getSalt();
        const counter = await SEAPORT_CONTRACT.getCounter(wallet_address);
        payload.protocol_data.parameters.counter = counter.toString();

        const signObj = await wallet._signTypedData(
          domain,
          types,
          payload.protocol_data.parameters
        );
        payload.protocol_data.signature = signObj;
        payload.protocol_address = SEAPORT_1_6;


        if (!task || !task.running || !task.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("opensea")) return

        await submitOfferToOpensea(decimal, currencySymbol, bidCount, offerPrice.toString(), payload, expiry, opensea_traits)
      } catch (error: any) {
        throw error
      }
    }
  } catch (error: any) {
    throw error
  }
}


/**
 * Posts an offer to OpenSea.
 * @param payload - The payload of the offer.
 */
async function submitOfferToOpensea(decimal: number, currencySymbol: string, bidCount: string, offerPrice: string, payload: IPayload, expiry = 900, opensea_traits?: string) {
  try {
    const [taskId] = bidCount.split(":")
    const task = activeTasks.get(taskId)
    if (!task || !task?.running || !task.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("OPENSEA".toLowerCase())) return


    try {
      const { data: offer } = await
        limiter.schedule(() => axiosInstance.request<OpenseaOffer>({
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

      const order_hash = offer.order_hash
      const identifier: string = offer?.criteria?.trait?.type
        && offer?.criteria?.trait?.value
        ? `${offer.criteria.trait?.type}:${offer.criteria.trait?.value}`
        : "collection"

      const slug = offer?.criteria?.collection?.slug
      const sanitizedExpiry = expiry > 900 ? expiry : 900

      if (offerPrice && order_hash && sanitizedExpiry && slug && taskId && identifier) {
        await prisma.order.create({
          data: {
            slug: slug,
            orderHash: order_hash,
            expiresAt: new Date(Date.now() + sanitizedExpiry * 1000),
            offerPrice: offerPrice.toString(),
            identifier: identifier,
            taskId: taskId,
            marketplace: 'opensea'
          }
        })
      }
      trackBidRate('opensea', taskId);
      const successMessage = opensea_traits ?
        `🎉 TRAIT OFFER POSTED TO OPENSEA SUCCESSFULLY FOR: ${payload.criteria.collection.slug.toUpperCase()}  TRAIT: ${opensea_traits} ${(Number(offerPrice) / Math.pow(10, decimal))} ${currencySymbol} 🎉`
        : `🎉 COLLECTION OFFER POSTED TO OPENSEA SUCCESSFULLY FOR: ${payload.criteria.collection.slug.toUpperCase()} ${(Number(offerPrice) / Math.pow(10, decimal))} ${currencySymbol} 🎉`
      console.log(BLUE, successMessage, RESET);
    } catch (error: any) {
      throw error
    }
  } catch (error: any) {
    throw error
  }
}

/**
 * Builds an offer on OpenSea.
 * @param buildPayload - The payload to build the offer.
 */
async function buildOffer(taskId: string, buildPayload: any) {
  try {
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
      throw error
    }
    finally {
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

export async function cancelOrder(orderHash: string, protocolAddress: string, privateKey: string, taskId: string) {
  if (!orderHash || !protocolAddress || !privateKey) return

  const offererSignature = await signCancelOrder(taskId, orderHash, protocolAddress, privateKey);
  if (!offererSignature) return;
  const url = `${BASE_URL}/opensea/api/v2/orders/chain/ethereum/protocol/${protocolAddress}/${orderHash}/cancel`;
  const headers = {
    'content-type': 'application/json',
    'x-api-key': API_KEY
  };

  const body = { offerer_signature: offererSignature };

  try {
    const response = await limiter.schedule(() => axiosInstance.post(url, body, { headers }))
    console.log(JSON.stringify({ cancelled: true }));
    decrementBidCount('opensea', taskId)
    return response.data;
  } catch (error: any) {
    return
  }
}

async function signCancelOrder(taskId: string, orderHash: string, protocolAddress: string, privateKey: string) {
  if (!orderHash) return

  const wallet = new Wallet(privateKey, provider);
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

  if (!orderHash) return
  const value = {
    orderHash: orderHash
  };
  try {
    if (!value) return
    const signature = await wallet._signTypedData(domain, types, value);
    return signature;
  } catch (error: any) {
    throw error
  }
}

async function signOffer(wallet: ethers.Wallet, offer: Record<string, unknown>) {
  return await wallet._signTypedData(domain, types, offer)
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

async function postItemOffer(taskId: string, offer: unknown, signature: string, slug: string) {
  try {
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
      throw error
    }
  } catch (error: any) {
    if (axios.isCancel(error)) {
      console.log(`Request for task ${taskId} was cancelled`);
      return null;
    }
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
    startAmount: quantity,
    endAmount: quantity,
    recipient: walletAddress,
  }
}

type OpenseaTopOffers = {
  amount: number;
  owner: string;
}

export async function fetchItemOffers(contractAddress: string, tokenId: string) {
  try {
    const tasks = [
      () => limiter.schedule(() => axiosInstance.get<MagicedenCollectionResponse>(`${BASE_URL}/magiceden/v3/rtp/ethereum/collections/${contractAddress}/bids/v1?type=collection&sortBy=price`, {
        headers: {
          'content-type': 'application/json',
          'x-api-key': API_KEY
        }
      })),

      () => limiter.schedule(() => axiosInstance.get<MagicedenCollectionResponse>(`${BASE_URL}/magiceden/v3/rtp/ethereum/orders/bids/v6?token=${contractAddress}:${tokenId}&sortBy=price&status=active&excludeEOA=false`, {
        headers: {
          'content-type': 'application/json',
          'x-api-key': API_KEY
        }
      })),
    ];

    const [collectionResponse, tokenResponse] = await pQueue.addAll(tasks, {
      throwOnTimeout: true,
    });

    const data = [...collectionResponse.data.orders, ...tokenResponse.data.orders].filter((offer) => offer.source.name.toLowerCase() === "opensea")
    const offers = data.map((order) => {
      return {
        amount: order?.price?.amount?.raw,
        owner: order?.maker,
        currency: order?.price.currency.symbol.toUpperCase(),
        usd: order?.price?.amount?.usd,
        decimal: order.price.currency.decimals,
        eth: order.price.amount.native
      }
    })?.sort((a, b) => Number(b.usd) - Number(a.usd)).slice(0, 2)
    return offers
  } catch (error: any) {
    throw error
  }
}


export async function fetchOpenseaOffers(
  offerType: 'COLLECTION' | 'TRAIT' | 'TOKEN',
  collectionSlug: string,
  contractAddress: string,
  identifiers: Record<string, string> | string
) {
  try {
    if (offerType === 'COLLECTION') {
      const { data } = await limiter?.schedule(() =>
        axiosInstance.get<MagicedenCollectionResponse>(`${BASE_URL}/magiceden/v3/rtp/ethereum/collections/${contractAddress}/bids/v1?type=collection&sortBy=price`, {
          headers: {
            'content-type': 'application/json',
            'x-api-key': API_KEY
          }
        })
      );
      const offers = data?.orders
        .filter((offer) => offer.source.name.toLowerCase() === "opensea")
        .map((order) => {
          return {
            amount: order?.price?.amount?.raw,
            owner: order?.maker,
            currency: order?.price.currency.symbol,
            usd: order?.price?.amount?.usd,
            decimal: order.price.currency.decimals,
            eth: order.price.amount.native
          }
        })?.sort((a, b) => Number(b?.usd) - Number(a?.usd)).slice(0, 2)


      return offers
    } else if (offerType === 'TRAIT') {
      const { type, value } = identifiers as Record<string, string>;
      const tasks = [
        () => limiter.schedule(() => axiosInstance.get<MagicedenCollectionResponse>(`${BASE_URL}/magiceden/v3/rtp/ethereum/collections/${contractAddress}/bids/v1?type=collection&sortBy=price`, {
          headers: {
            'content-type': 'application/json',
            'x-api-key': API_KEY
          }
        })),

        () => {

          const URL = `${BASE_URL}/magiceden/v3/rtp/ethereum/orders/bids/v5?collection=${contractAddress}&attribute%5B${type}%5D=${value}&status=active&sortBy=price`;

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

      const data = [...collectionResponse.data.orders, ...traitResponse.data.orders].filter((offer) => offer.source.name.toLowerCase() === "opensea")
      const offers = data.map((order) => {
        return {
          amount: order?.price?.amount?.raw,
          owner: order?.maker,
          currency: order?.price.currency.symbol.toUpperCase(),
          usd: order?.price?.amount?.usd,
          decimal: order.price.currency.decimals,
          eth: order.price.amount.native
        }
      })?.sort((a, b) => Number(b.usd) - Number(a.usd)).slice(0, 2)
      return offers
    }
  } catch (error: any) {
    throw error
  }
}



export async function fetchOpenseaListings(taskId: string, collectionSlug: string, limit?: number) {
  try {
    const baseUrl = `${BASE_URL}/opensea/api/v2/listings/collection/${collectionSlug}/best`;
    let allListings: OpenseaOrder[] = [];
    let nextCursor: string | null = null;
    let uniqueTokenIds: number[] = [];

    if (!limit) return

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
  } catch (error: any) {
    throw error
  }
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
  offer: OpenseaOffer[];
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

interface OpenseaOffer {
  itemType: number;
  token: string;
  identifierOrCriteria: string;
  startAmount: string;
  endAmount: string;
}

interface OpenseaConsideration {
  itemType: number;
  token: string;
  identifierOrCriteria: string;
  startAmount: string;
  endAmount: string;
  recipient: string;
}


interface Price {
  currency: string;
  decimals: number;
  value: string;
}

interface Collection {
  slug: string;
}

interface Address {
  address: string;
}

interface Trait {
  type?: string;
  value?: string;
}

interface Criteria {
  collection: Collection;
  contract: Address;
  trait: Trait | null;
  encoded_token_ids: any; // Adjust type as necessary
}

interface OfferItem {
  itemType: number;
  token: string;
  identifierOrCriteria: string;
  startAmount: string;
  endAmount: string;
}

interface ConsiderationItem {
  itemType: number;
  token: string;
  identifierOrCriteria: string;
  startAmount: string;
  endAmount: string;
  recipient: string;
}

interface Parameters {
  offerer: string;
  offer: OfferItem[];
  consideration: ConsiderationItem[];
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

interface ProtocolData {
  parameters: Parameters;
  signature: string;
}

interface OpenseaOffer {
  order_hash: string;
  chain: string;
  price: Price;
  criteria: Criteria;
  protocol_data: ProtocolData;
  protocol_address: string;
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



interface OpenseaGraphResponse {
  data: {
    itemOffers: OpenseaGraphOrdersConnection;
  };
  extensions: {
    debugInfo: {
      additionalInformation: {
        'x-trace-id': string;
        'x-cache-status': string;
        'cache-control': string;
        'x-ratelimit-remaining': number;
      };
    };
  };
}

interface OpenseaGraphOrdersConnection {
  items: OpenseaGraphOrder[];
  nextPageCursor: string;
  __typename: 'OrdersConnection';
}

interface OpenseaGraphOrder {
  id: string;
  type: 'CRITERIA_TRAIT' | 'CRITERIA_COLLECTION';
  pricePerItem: OpenseaGraphPrice;
  quantityRemaining: string;
  maker: OpenseaGraphOrderProfile;
  endTime: string;
  marketplace: OpenseaGraphMarketplace;
  __typename: 'Order';
}

interface OpenseaGraphPrice {
  token: OpenseaGraphTokenPrice;
  usd: number;
  __typename: 'Price';
}

interface OpenseaGraphTokenPrice {
  unit: number;
  __typename: 'TokenPrice';
  symbol: string;
  contractAddress: string;
  chain: OpenseaGraphChain;
}

interface OpenseaGraphChain {
  identifier: string;
  __typename: 'Chain';
}

interface OpenseaGraphOrderProfile {
  address: string;
  displayName: string | null;
  imageUrl: string;
  __typename: 'OrderProfile';
}

interface OpenseaGraphMarketplace {
  identifier: string;
  __typename: 'Marketplace';
}

export function cancelOpenseaRequest(taskId: string): boolean {
  const controllers = activeRequests.get(taskId);
  if (controllers && controllers.length > 0) {
    controllers.forEach(controller => controller.abort());
    activeRequests.delete(taskId);
    console.log(`Aborted ${controllers.length} OpenSea requests for task ${taskId}`);
    return true;
  }
  console.log(`No active OpenSea requests found for task ${taskId}`);
  return false;
}

function addActiveRequest(taskId: string, controller: AbortController) {
  if (!activeRequests.has(taskId)) {
    activeRequests.set(taskId, []);
  }
  activeRequests.get(taskId)?.push(controller);
}


interface Chain {
  identifier: string;
  arch?: string;
  __typename: string;
}

interface TokenPrice {
  unit: number;
  __typename: string;
  contractAddress: string;
  symbol: string;
  chain: Chain;
}

interface Price {
  token: TokenPrice;
  __typename: string;
  usd: number;
}

interface Bidder {
  quantity: number;
  __typename: string;
  imageUrl: string | null;
  address: string;
  displayName: string;
}

interface CollectionOfferAggregate {
  id: string;
  totalOffers: number;
  offerPrice: Price;
  totalValue: Price;
  bidders: Bidder[];
  __typename: string;
}

interface CollectionFlags {
  isOffersEnabled: boolean;
  __typename: string;
}

interface Collection {
  __typename: string;
  slug: string;
  flags: CollectionFlags;
  traitOffersEnabled: boolean;
  chain: Chain;
}

interface DebugInformation {
  'x-trace-id': string;
  'x-cache-status': string;
  age: number;
  'cache-control': string;
  'x-ratelimit-remaining': number;
}

interface CollectionOfferAggregatesConnection {
  items: CollectionOfferAggregate[];
  nextPageCursor: string;
  __typename: string;
}

interface CollectionOfferResponse {
  data: {
    collectionOfferAggregates: CollectionOfferAggregatesConnection;
    collectionBySlug: Collection;
  };
  extensions: {
    debugInfo: {
      additionalInformation: DebugInformation;
    };
  };
}

export interface OpenseaOfferData {
  amount: number;
  owner: string;
  currency: string;
  usd: number;
  eth: number;
  decimal: number;
}

interface Chain {
  identifier: string;
  arch?: string;
  __typename: string;
}

interface TokenPrice {
  unit: number;
  contractAddress: string;
  __typename: string;
  symbol: string;
  chain: Chain;
}

interface Price {
  usd: number;
  __typename: string;
  token: TokenPrice;
}

interface Bidder {
  quantity: number;
  __typename: string;
  address: string;
  displayName: string;
}

interface TraitOfferAggregate {
  id: string;
  totalOffers: number;
  offerPrice: Price;
  totalValue: {
    token: TokenPrice;
    usd: number;
    __typename: string;
  };
  bidders: Bidder[];
  __typename: string;
}

interface CollectionFlags {
  isOffersEnabled: boolean;
  __typename: string;
}

interface Collection {
  __typename: string;
  slug: string;
  flags: CollectionFlags;
  traitOffersEnabled: boolean;
  chain: Chain;
}

interface DebugInfo {
  additionalInformation: {
    'x-trace-id': string;
    'x-cache-status': string;
    'cache-control': string;
    'x-ratelimit-remaining': number;
  };
}

interface TraitOfferResponse {
  data: {
    traitOfferAggregates: {
      items: TraitOfferAggregate[];
      nextPageCursor: null | string;
      __typename: string;
    };
    collectionBySlug: Collection;
  };
  extensions: {
    debugInfo: DebugInfo;
  };
}