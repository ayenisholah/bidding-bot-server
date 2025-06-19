
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
  tags: { name: string; color: string }[];
  safetyOptions: {
    doNotListBelowFloor: boolean;
    undercutProtection: boolean;
  };
  instantPriceMatching: boolean;
  listingPrice: {
    value: string;
    type: "percentage" | "eth";
  };
  underCutMargin: string;
  openseaListingPrice: {
    value: string;
    type: "percentage" | "eth";
  };
  blurListingPrice: {
    value: string;
    type: "percentage" | "eth";
  };
  magicEdenListingPrice: {
    value: string;
    type: "percentage" | "eth";
  };
  minProfit: {
    value: string;
    type: "percentage" | "eth";
  };
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
  finalListingPrice?: { opensea: number; blur: number; magiceden: number }
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
}

// For use with Mongoose Document
export interface IListing extends IListingBase {
  _id: any; // This will be overridden by Mongoose Document type
}


// Interface for listing data
export interface IListingData {
  contract: {
    slug: string;
    contractAddress: string;
  };
  tokenId: string;
  selectedMarketplaces: string[];
  instantPriceMatching: boolean;
  listingPrice: {
    value: string;
    type: "eth" | "percentage";
  };
  underCutMargin: string;
  openseaListingPrice: {
    value: string;
    type: "eth" | "percentage";
  };
  blurListingPrice: {
    value: string;
    type: "eth" | "percentage";
  };
  magicEdenListingPrice: {
    value: string;
    type: "eth" | "percentage";
  };
  minProfit: {
    value: string;
    type: "eth" | "percentage";
  };
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
  collectionFloor: number;
  wallet: string;
  walletId: string;
}

export interface IFee {
  [key: string]: number
}
