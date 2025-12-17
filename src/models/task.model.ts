import mongoose, { Schema, Model } from "mongoose";

export interface SelectedTraits {
  [marketplace: string]: {
    [category: string]: {
      name: string;
    }[];
  };
}

export interface ITask {
  _id: string;
  user: string;
  contract: {
    slug: string;
    contractAddress: string;
  };
  wallet: string;
  selectedMarketplaces: string[];
  running: boolean;
  tags: { name: string; color: string }[];
  selectedTraits: SelectedTraits;
  traits: {
    categories: Record<string, string>;
    counts: Record<
      string,
      Record<string, { count: number; availableInMarketplaces: string[] }>
    >;
  };
  outbidOptions: {
    outbid: boolean;
    blurOutbidMargin: number | null;
    openseaOutbidMargin: number | null;
    magicedenOutbidMargin: number | null;
    counterbid: boolean;
    cancelMaxWhenOutbid: boolean;
    crossTypeMarginMultiplier?: number | null;
  };
  bidPrice: {
    min: number;
    max: number | null;
    minType: "percentage" | "eth" | "usdc";
    maxType: "percentage" | "eth" | "usdc";
    hardCapEnabled?: boolean;
    hardCap?: number | null;
  };

  openseaBidPrice: {
    min: number;
    max: number | null;
    minType: "percentage" | "eth" | "usdc";
    maxType: "percentage" | "eth" | "usdc";
    hardCapEnabled?: boolean;
    hardCap?: number | null;
  };

  blurBidPrice: {
    min: number;
    max: number | null;
    minType: "percentage" | "eth" | "usdc";
    maxType: "percentage" | "eth" | "usdc";
    hardCapEnabled?: boolean;
    hardCap?: number | null;
  };

  magicEdenBidPrice: {
    min: number;
    max: number | null;
    minType: "percentage" | "eth" | "usdc";
    maxType: "percentage" | "eth" | "usdc";
    hardCapEnabled?: boolean;
    hardCap?: number | null;
  };

  stopOptions: {
    minFloorPrice: number | null;
    maxFloorPrice: number | null;
    maxPurchase: number | null;
    actualPurchase: number;
    triggerStopOptions: boolean;
    traitPrices: Record<string, { min: number; max: number }>;
  };
  bidDuration: {
    value: number;
    unit: string;
  };
  loopInterval: {
    value: number;
    unit: string;
  };
  walletId: string;
  tokenIds: (number | string)[];
  bidType: "collection" | "token" | "rarity";
  rarityCategory?: "LEGENDARY" | "EPIC" | "RARE";
  bidPriceType: "GENERAL_BID_PRICE" | "MARKETPLACE_BID_PRICE";
  slugValid: boolean;
  magicEdenValid: boolean;
  blurValid: boolean;
  openseaValid: boolean;
  traitsOperator: { value: string; label: string };
  autoList?: boolean;
  note?: string;
  priority: 1 | 2 | 3 | 4 | 5;
  listingConfig?: {
    selectedMarketplaces: string[];
    pricingStrategy: "FIXED_PRICING" | "DYNAMIC_PRICING";
    listingPriceType: "GENERAL_LISTING_PRICE" | "MARKETPLACE_LISTING_PRICE";
    basePrice: string;
    openseaListingPrice: string;
    blurListingPrice: string;
    magicEdenListingPrice: string;
    minListingPrice: string;
    minimumProfit: string;
    underCutMargin: string;
    dynamicPricingBasis?:
    | "HIGHEST_TRAIT_FLOOR"
    | "SPECIFIC_TRAIT"
    | "COLLECTION_FLOOR"
    | "BUY_PRICE";
    selectedTrait?: {
      trait_type: string;
      value: string | number;
    };
    listDuration: {
      value: number;
      unit: string;
    };
    loopInterval: {
      value: number;
      unit: string;
    };
    instantPriceMatching: boolean;
  };
}

const TaskSchema: Schema = new Schema(
  {
    user: { type: String, ref: "User", required: true },
    contract: {
      slug: { type: String, required: true },
      contractAddress: { type: String, required: true },
    },
    wallet: { type: String },
    // New field for wallet reference - will migrate to this
    walletId: { type: mongoose.Schema.Types.ObjectId, ref: "Wallet" },
    selectedMarketplaces: { type: [String], required: true },
    running: { type: Boolean, default: false },
    tags: [{ name: String, color: String }],
    selectedTraits: {
      type: Schema.Types.Mixed,
    },
    traits: {
      categories: { type: Schema.Types.Mixed },
      counts: {
        type: Schema.Types.Mixed,
        required: false,
        default: {},
      },
    },
    outbidOptions: {
      outbid: { type: Boolean, default: false },
      blurOutbidMargin: { type: Number, default: null },
      openseaOutbidMargin: { type: Number, default: null },
      magicedenOutbidMargin: { type: Number, default: null },
      counterbid: { type: Boolean, default: false },
      cancelMaxWhenOutbid: { type: Boolean, default: false },
      crossTypeMarginMultiplier: { type: Number, default: 1 },
    },
    bidPrice: {
      min: { type: Number, required: true },
      max: { type: Number, required: false, default: null },
      minType: { type: String, enum: ["percentage", "eth", "usdc"], required: true },
      maxType: { type: String, enum: ["percentage", "eth", "usdc"], required: true },
      hardCapEnabled: { type: Boolean, default: false },
      hardCap: { type: Number, default: null },
    },
    openseaBidPrice: {
      min: { type: Number, required: true },
      max: { type: Number, required: false, default: null },
      minType: { type: String, enum: ["percentage", "eth", "usdc"], required: true },
      maxType: { type: String, enum: ["percentage", "eth", "usdc"], required: true },
      hardCapEnabled: { type: Boolean, default: false },
      hardCap: { type: Number, default: null },
    },
    blurBidPrice: {
      min: { type: Number, required: true },
      max: { type: Number, required: false, default: null },
      minType: { type: String, enum: ["percentage", "eth", "usdc"], required: true },
      maxType: { type: String, enum: ["percentage", "eth", "usdc"], required: true },
      hardCapEnabled: { type: Boolean, default: false },
      hardCap: { type: Number, default: null },
    },
    magicEdenBidPrice: {
      min: { type: Number, required: true },
      max: { type: Number, required: false, default: null },
      minType: { type: String, enum: ["percentage", "eth", "usdc"], required: true },
      maxType: { type: String, enum: ["percentage", "eth", "usdc"], required: true },
      hardCapEnabled: { type: Boolean, default: false },
      hardCap: { type: Number, default: null },
    },
    stopOptions: {
      minFloorPrice: { type: Number, required: true },
      maxFloorPrice: { type: Number, required: true },
      maxPurchase: { type: Number, required: true },
      actualPurchase: { type: Number, default: 0 },
      triggerStopOptions: { type: Boolean, default: false },
      traitPrices: { type: Schema.Types.Mixed, default: {} },
    },
    bidDuration: {
      value: { type: Number, required: false, default: 15 },
      unit: { type: String, required: false, default: "minutes" }

    },
    loopInterval: {
      value: { type: Number, required: false, default: 1 },
      unit: { type: String, required: false, default: "minutes" }
    },
    tokenIds: { type: [Schema.Types.Mixed], required: false },
    bidType: {
      type: String,
      enum: ["collection", "token", "rarity"],
      default: "collection"
    },
    rarityCategory: {
      type: String,
      enum: ["LEGENDARY", "EPIC", "RARE"],
      required: false
    },
    bidPriceType: {
      type: String,
      enum: ["GENERAL_BID_PRICE", "MARKETPLACE_BID_PRICE"],
      default: "GENERAL_BID_PRICE"
    },
    slugValid: { type: Boolean, default: null },
    magicEdenValid: { type: Boolean, default: null },
    blurValid: { type: Boolean, default: null },
    openseaValid: { type: Boolean, default: null },
    traitsOperator: {
      value: { type: String, default: "or" },
      label: { type: String, default: "OR" }
    },
    autoList: { type: Boolean, default: false },
    note: { type: String, default: "" },
    priority: { type: Number, min: 1, max: 5, default: 3 },
    listingConfig: {
      selectedMarketplaces: { type: [String] },
      pricingStrategy: {
        type: String,
        enum: ["FIXED_PRICING", "DYNAMIC_PRICING"],
      },
      listingPriceType: {
        type: String,
        enum: ["GENERAL_LISTING_PRICE", "MARKETPLACE_LISTING_PRICE"],
      },
      basePrice: { type: String },
      openseaListingPrice: { type: String },
      blurListingPrice: { type: String },
      magicEdenListingPrice: { type: String },
      minListingPrice: { type: String },
      minimumProfit: { type: String },
      underCutMargin: { type: String, default: "0.0000" },
      dynamicPricingBasis: {
        type: String,
        enum: [
          "HIGHEST_TRAIT_FLOOR",
          "SPECIFIC_TRAIT",
          "COLLECTION_FLOOR",
          "BUY_PRICE",
        ],
      },
      selectedTrait: {
        trait_type: { type: String },
        value: { type: Schema.Types.Mixed },
      },
      listDuration: {
        value: { type: Number },
        unit: { type: String },
      },
      loopInterval: {
        value: { type: Number },
        unit: { type: String },
      },
      instantPriceMatching: { type: Boolean, default: false },
    }
  },
  { timestamps: true }
);

const Task = (mongoose.models.Task || mongoose.model<ITask>("Task", TaskSchema)) as Model<ITask>;
export default Task;
