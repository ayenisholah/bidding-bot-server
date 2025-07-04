export interface FormState {
  contract: {
    slug: string;
    contractAddress: string;
  };
  tokenId: string;
  selectedMarketplaces: string[];
  instantPriceMatching: boolean;
  listingPrice: string;
  underCutMargin: string;
  openseaListingPrice: string;
  blurListingPrice: string;
  magicEdenListingPrice: string;
  minListingPrice: string;
  loopInterval: {
    value: number;
    unit: string;
  };
  listDuration: {
    value: number;
    unit: string;
  };
  listingPriceType: "GENERAL_LISTING_PRICE" | "MARKETPLACE_LISTING_PRICE";
  pricingStrategy: "FIXED_PRICING" | "DYNAMIC_PRICING";
  floorPrice: number;
  dynamicPricingBasis?:
  | "HIGHEST_TRAIT_FLOOR"
  | "SPECIFIC_TRAIT"
  | "COLLECTION_FLOOR"
  | "BUY_PRICE";
  selectedTrait?: {
    trait_type: string;
    value: string | number;
  };
}

export interface IListingBase {
  user: string;
  tokenId: string;
  contract: {
    slug: string;
    contractAddress: string;
  };
  wallet: string;
  walletId: string;
  selectedMarketplaces: string[];
  running: boolean;
  instantPriceMatching: boolean;
  basePrice: string;
  underCutMargin: string;
  openseaListingPrice: string;
  blurListingPrice: string;
  magicEdenListingPrice: string;
  minListingPrice: string;
  minimumProfit: string;
  loopInterval: {
    value: number;
    unit: string;
  };
  listDuration: {
    value: number;
    unit: string;
  };
  listingPriceType: "GENERAL_LISTING_PRICE" | "MARKETPLACE_LISTING_PRICE";
  pricingStrategy: "FIXED_PRICING" | "DYNAMIC_PRICING";
  floorPrice: number;
  buyPrice: number;
  dynamicPricingBasis?:
  | "HIGHEST_TRAIT_FLOOR"
  | "SPECIFIC_TRAIT"
  | "COLLECTION_FLOOR"
  | "BUY_PRICE";
  selectedTrait?: {
    trait_type: string;
    value: string | number;
  };
  finalListingPrice?: { opensea: number; blur: number; magiceden: number };
  listingExpiration?: {
    opensea?: Date;
    blur?: Date;
    magiceden?: Date;
  };
  lastListedAt?: {
    opensea?: Date;
    blur?: Date;
    magiceden?: Date;
  };
  listed: boolean;
  createdAt?: Date;
  updatedAt?: Date;
  instantPriceOverride?: number; // Used for instant price matching to bypass calculateListingPrice
}

// For use with Mongoose Document
export interface IListing extends IListingBase {
  _id: any; // This will be overridden by Mongoose Document type
}

export interface IFee {
  [key: string]: number
}
