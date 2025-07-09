import { ethers, Wallet } from "ethers";
import { BASE_URL, SEAPORT_CONTRACT_ADDRESS, SEAPORT_MIN_ABI } from "../../constants";
import { axiosInstance, limiter } from "../../init";
import { activeTasks, API_KEY, BLUE, decrementBidCount, pQueue, trackBidRate } from "../..";
import { config } from "dotenv";
import axios from "axios";
import { PrismaClient } from '@prisma/client'
import { USDC, WETH } from "../../utils/currencies";
import { MagicedenCollectionResponse, MagicEdenTraitOfferResponse } from "../magiceden";
import { CollectionItemsListQuery } from "../../queries";
import { CollectionItemsQueryVariables, CollectionItemsResponse } from "../../functions";
import { fetchERC20Balance } from "../../utils/balance";
import { logger } from "../../utils/logger";

const prisma = new PrismaClient()

config()
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
  const divider = 10000n;
  const decimal = currency.toLowerCase() === USDC.toLowerCase() ? 6 : 18
  const currencySymbol = currency.toLowerCase() === USDC.toLowerCase() ? "USDC" : "WETH"
  const task = activeTasks.get(taskId)
  if (!task || !task.running || !task.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("opensea")) return

  const wethBalance = await fetchERC20Balance(wallet_address, currency);
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
  const basis = decimals === 2 ? 1e16 : decimals === 3 ? 1e15 : 1e14
  const roundedWei = decimal === 18 ? Math.round(roundedEth * Math.pow(10, decimal) / basis) * basis : Math.round(roundedEth * Math.pow(10, decimal))
  const offerPrice = BigInt(roundedWei.toString());
  const offerPriceEthFinal = Number(roundedWei) / Math.pow(10, decimal);

  if (offerPriceEthFinal > wethBalance) {
    const identifier = opensea_traits ? opensea_traits : asset?.tokenId ? asset?.tokenId : slug;  
    throw new Error(`[${slug.toUpperCase()}${identifier ? ` - ${identifier}` : ''}] Offer price: ${offerPriceEthFinal} ${currencySymbol} is greater than available ${currencySymbol} balance: ${wethBalance} ${currencySymbol}.`);
  }

  const wallet = new Wallet(private_key, provider);
  const openseaFee = 50n;

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
      startAmount: ((offerPrice * openseaFee) / divider).toString(),
      endAmount: ((offerPrice * openseaFee) / divider).toString(),
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
          startAmount: ((offerPrice * fee) / divider).toString(),
          endAmount: ((offerPrice * fee) / divider).toString(),
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

    const itemResponse = await postItemOffer(taskId, offer, itemSignature)
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
    const divider = 10000n;
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
      startAmount: ((offerPrice * openseaFee) / divider).toString(),
      endAmount: ((offerPrice * openseaFee) / divider).toString(),
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
          startAmount: ((offerPrice * fee) / divider).toString(),
          endAmount: ((offerPrice * fee) / divider).toString(),
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

    const signObj = await wallet.signTypedData(
      domain,
      types,
      payload.protocol_data.parameters
    );
    payload.protocol_data.signature = signObj;
    payload.protocol_address = SEAPORT_1_6;


    if (!task || !task.running || !task.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("opensea")) return

    await submitOfferToOpensea(decimal, currencySymbol, bidCount, offerPrice.toString(), payload, expiry, opensea_traits)
  }
}


/**
 * Posts an offer to OpenSea.
 * @param payload - The payload of the offer.
 */
async function submitOfferToOpensea(decimal: number, currencySymbol: string, bidCount: string, offerPrice: string, payload: IPayload, expiry = 900, opensea_traits?: string) {
  const [taskId] = bidCount.split(":")
  const task = activeTasks.get(taskId)
  if (!task || !task?.running || !task.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("OPENSEA".toLowerCase())) return

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
}

/**
 * Builds an offer on OpenSea.
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
    await logger.logOpenseaBidError('Failed to build OpenSea offer', {
      taskId,
      operation: 'build_offer',
      error: error,
      errorCode: error?.response?.status,
      errorMessage: error?.response?.data || error?.message || String(error)
    });
    throw error;
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

    const response = await limiter.schedule(() => axiosInstance.post(url, body, { headers }))
    console.log(JSON.stringify({ cancelled: true }));
    decrementBidCount('opensea', taskId)
    return response.data;
  } catch (error) {
    if (error) {
      console.error(`Failed to cancel OpenSea order: ${error}`);
    }
    throw error;
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
  if (!value) return
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
      errorMessage: error?.response?.data || error?.message || String(error)
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
  const tasks = [
    () => limiter.schedule(() => axiosInstance.get(`${BASE_URL}/magiceden/v3/rtp/ethereum/collections/${contractAddress}/bids/v1?type=collection&sortBy=price`, {
      headers: {
        'content-type': 'application/json',
        'x-api-key': API_KEY
      }
    })),
    () => limiter.schedule(() => axiosInstance.get(`${BASE_URL}/magiceden/v3/rtp/ethereum/orders/bids/v6?token=${contractAddress}:${tokenId}&sortBy=price&status=active&excludeEOA=false`, {
      headers: {
        'content-type': 'application/json',
        'x-api-key': API_KEY
      }
    })),

    () => limiter.schedule(() => axiosInstance.post(
      `${BASE_URL}/opensea/graphql`,
      query,
      {
        headers: {
          'content-type': 'application/json',
          'x-api-key': API_KEY
        }
      }
    ))
  ];

  const [collectionResponse, tokenResponse, itemOffers] = await pQueue.addAll(tasks, {
    throwOnTimeout: true,
  });

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
  })
  const allOffers = [...offers, ...order]
  const uniqueKey = (o: { owner: string; amount: string }) => `${o.owner}-${o.amount}`;
  const seen = new Set<string>();

  const uniqueOffers = allOffers.filter((offer) => {
    const key = uniqueKey(offer);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  }).sort((a, b) => Number(b.usd) - Number(a.usd)).slice(0, 2)
  return uniqueOffers
}


export async function fetchOpenseaOffers(
  offerType: 'COLLECTION' | 'TRAIT' | 'TOKEN',
  collectionSlug: string,
  contractAddress: string,
  identifiers: Record<string, string> | string
) {
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
}

export async function openseaTraitFloor(slug: string, trait: {
  traitType: string;
  values: string[];
}) {
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
}


export async function fetchOpenseaListings(taskId: string, collectionSlug: string, limit?: number) {
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

export function cancelOpenseaRequest(taskId: string): boolean {
  const controllers = activeRequests.get(taskId);
  if (controllers && controllers.length > 0) {
    controllers.forEach(controller => controller.abort());
    activeRequests.delete(taskId);
    return true;
  }
  return false;
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
  items: Order[];
}

export interface Order {
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
