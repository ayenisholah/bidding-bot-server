import { config } from "dotenv";
import { axiosInstance, limiter } from "../init";
import { IListingData, IFee } from "../types/listing.types";
import { BASE_URL, NETWORK } from "../constants";
import { listOnOpensea } from "../marketplace/opensea/list";
import Wallet from "../models/wallet.model";
import { decryptPrivateKey } from "../utils/crypto";
import { AlchemyProvider, Wallet as Web3Wallet } from "ethers";
const API_KEY = process.env.API_KEY;
const ALCHEMY_API_KEY = process.env.ALCHEMY_API_KEY

const provider = new AlchemyProvider(NETWORK, ALCHEMY_API_KEY)

config()

function convertToSeconds(value: number, unit: string): number {
  switch (unit.toLowerCase()) {
    case 'minutes':
      return value * 60;
    case 'hours':
      return value * 60 * 60;
    case 'days':
      return value * 60 * 60 * 24;
    default:
      return 900; // assume already in seconds
  }
}

function calculateListingPrice(data: IListingData, marketplace: 'opensea' | 'blur' | 'magiceden'): number {
  if (data.pricingStrategy === "DYNAMIC_PRICING") {
    const minProfitValue = parseFloat(data.minProfit.value);

    if (data.minProfit.type === "eth") {
      return data.buyPrice + minProfitValue;
    } else if (data.minProfit.type === "percentage") {
      const profitAmount = data.buyPrice * (minProfitValue / 100);
      return data.buyPrice + profitAmount;
    }

    return data.buyPrice;
  }

  // FIXED_PRICING - use configured listing prices
  // Get marketplace-specific listing price or fallback to general listing price
  let marketplacePrice: { value: string; type: "percentage" | "eth" };

  switch (marketplace) {
    case 'opensea':
      marketplacePrice = data.openseaListingPrice.value ? data.openseaListingPrice : data.listingPrice;
      break;
    case 'blur':
      marketplacePrice = data.blurListingPrice.value ? data.blurListingPrice : data.listingPrice;
      break;
    case 'magiceden':
      marketplacePrice = data.magicEdenListingPrice.value ? data.magicEdenListingPrice : data.listingPrice;
      break;
    default:
      marketplacePrice = data.listingPrice;
  }

  // Calculate base price from marketplace price
  if (marketplacePrice.value && marketplacePrice.value !== "") {
    const priceValue = parseFloat(marketplacePrice.value);

    if (marketplacePrice.type === "percentage") {
      // All percentage pricing is based on buyPrice
      return data.buyPrice * (priceValue / 100);
    } else {
      // Direct ETH value
      return priceValue;
    }
  }

  // No specific price set, use buyPrice as fallback
  return data.buyPrice;
}

export async function createListing(data: IListingData) {
  try {
    console.log(JSON.stringify(data));

    const wallet = await Wallet.findById(data.walletId.toString());

    console.log({ wallet });

    const private_key = decryptPrivateKey(wallet?.encryptedPrivateKey as string) as string;
    if (data.selectedMarketplaces.includes("opensea")) {
      console.log("Listing on OpenSea...");

      const web3Wallet = new Web3Wallet(private_key, provider)

      const collectionData = await getCollectionData(data.contract.slug);

      if (collectionData) {
        // Calculate the listing price using the comprehensive pricing function
        const price = calculateListingPrice(data, 'opensea');

        const durationInSeconds = convertToSeconds(data.listDuration.value, data.listDuration.unit);

        await listOnOpensea(
          data.contract.contractAddress,
          data.tokenId,
          price,
          web3Wallet,
          collectionData.enforceCreatorFee,
          durationInSeconds,
          collectionData.creator_fees,
        );
      }
    }

  } catch (error) {
    console.log(error);
  }
}

export const ListingData: IListingData = {
  contract: {
    slug: "say-gm",
    contractAddress: "0x48c7f48622a2c31fc72b326332cf1e2357c8ef30"
  },
  tokenId: "4427",
  selectedMarketplaces: ["opensea", "blur", "magiceden"],
  instantPriceMatching: false,
  listingPrice: {
    value: "",
    type: "eth"
  },
  underCutMargin: "0.0001",
  openseaListingPrice: {
    value: "",
    type: "eth"
  },
  blurListingPrice: {
    value: "",
    type: "eth"
  },
  magicEdenListingPrice: {
    value: "",
    type: "eth"
  },
  minProfit: {
    value: "0.01",
    type: "eth"
  },
  loopInterval: {
    value: 15,
    unit: "minutes"
  },
  listDuration: {
    value: 15,
    unit: "minutes"
  },
  listingPriceType: "GENERAL_LISTING_PRICE",
  pricingStrategy: "DYNAMIC_PRICING",
  floorPrice: 0,
  buyPrice: 0.0394,
  collectionFloor: 0.005,
  wallet: "0x1b8AfD0DE8a9368ce635Be8B9572c65d8F590B21",
  walletId: "684be3548d0934d32c507154"
};

export async function getCollectionData(collectionSlug: string) {
  try {
    const { data: collection } = await limiter.schedule(() => axiosInstance
      .get(
        `${BASE_URL}/opensea/api/v2/collections/${collectionSlug}`,
        {
          headers: {
            'x-api-key': API_KEY
          }
        }
      ))

    let creator_fees: IFee;

    console.table(collection.fees)

    let enforceCreatorFee = false;

    if (collection.fees.length > 1) {
      creator_fees = {
        [collection.fees[1].recipient]: collection.fees[1].fee * 100
      };
      enforceCreatorFee = collection.fees[1].required;
    } else {
      creator_fees = { "null": 0 }
    }
    return {
      address: collection.editors[0],
      primary_asset_contracts_address: collection.contracts[0].address,
      creator_fees: creator_fees,
      enforceCreatorFee: enforceCreatorFee
    };
  } catch (error) {
    console.log('🌵💜🐢 error', error);
  }
};