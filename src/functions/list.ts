// import { config } from "dotenv";
import { IListing } from "../types/listing.types";
// import { NETWORK } from "../constants";
// import Wallet from "../models/wallet.model";
// import { decryptPrivateKey } from "../utils/crypto";
// import { AlchemyProvider, Wallet as Web3Wallet } from "ethers";
// import { getCollectionDetails } from "./collection";
// import { logger } from "../utils/logger";
// const ALCHEMY_API_KEY = process.env.ALCHEMY_API_KEY

// const provider = new AlchemyProvider(NETWORK, ALCHEMY_API_KEY)

// config()

// function convertToSeconds(value: number, unit: string): number {
//   switch (unit.toLowerCase()) {
//     case 'minutes':
//       return value * 60;
//     case 'hours':
//       return value * 60 * 60;
//     case 'days':
//       return value * 60 * 60 * 24;
//     default:
//       return 900; // assume already in seconds
//   }
// }


// export async function createListing(data: IListing) {
//   try {
//     console.log(JSON.stringify(data));

//     const wallet = await Wallet.findById(data.walletId.toString());

//     const private_key = await decryptPrivateKey(wallet?.encryptedPrivateKey as string) as string;
//     const web3Wallet = new Web3Wallet(private_key, provider)
//     const collectionData = await getCollectionDetails(data.contract.slug, data.contract.contractAddress);
//     const creatorFees = collectionData?.creator_fees as IFee
//     const enforceCreatorFee = collectionData?.enforceCreatorFee as boolean

//     // Calculate total creator fee as a number (sum of all fees)
//     const totalCreatorFee = creatorFees ? Object.values(creatorFees).reduce((sum, fee) => sum + fee, 0) : 0;
//     const durationInSeconds = convertToSeconds(data.listDuration.value, data.listDuration.unit);


//   } catch (error) {
//     await logger.logMarketplaceError('Error creating listing', {
//       contractAddress: data.contract.contractAddress,
//       tokenId: data.tokenId,
//       selectedMarketplaces: data.selectedMarketplaces,
//       operation: 'create_listing',
//       error: error as Error,
//       errorMessage: error instanceof Error ? error.message : String(error)
//     });
//     console.log(error);
//   }
// }

export const ListingData: IListing = {
  "_id": "",
  "user": "6863bc1a48bdb57f78ea2c59",
  "tokenId": "21589",
  "contract": {
    "slug": "lilpudgys",
    "contractAddress": "0x524cab2ec69124574082676e6f654a18df49a048"
  },
  "wallet": "0x29f23bdc8e57174a723c4db42f5fa24fd9a09dd1",
  "walletId": "6863c6ffadb7b55577b8fe76",
  "selectedMarketplaces": [
    "opensea",
    "blur",
    "magiceden"
  ],
  "running": false,
  "finalListingPrice": {
    "opensea": 0,
    "blur": 0,
    "magiceden": 0
  },
  "instantPriceMatching": false,
  "underCutMargin": "0.000",
  "openseaListingPrice": "",
  "blurListingPrice": "",
  "magicEdenListingPrice": "",
  "minListingPrice": "",
  "loopInterval": {
    "value": 20,
    "unit": "minutes"
  },
  "listDuration": {
    "value": 20,
    "unit": "minutes"
  },
  "listingPriceType": "GENERAL_LISTING_PRICE",
  "pricingStrategy": "DYNAMIC_PRICING",
  "floorPrice": 0,
  "buyPrice": 1.499,
  "listed": false,
  "listingPrice": "5.25",
  "dynamicPricingBasis": "HIGHEST_TRAIT_FLOOR",
  "selectedTrait": {
    "trait_type": "Body",
    "value": "Kimono Flower Blue"
  }
}