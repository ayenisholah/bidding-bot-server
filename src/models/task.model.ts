import mongoose, { Schema, Document } from "mongoose";

interface SelectedTraits {
  [key: string]: {
    name: string;
    availableInMarketplaces: string[];
  }[];
}

export interface ITask extends Document {
  _id: mongoose.Types.ObjectId;
  user: string;
  contract: {
    slug: string;
    contractAddress: string;
  };
  wallet: {
    address: string;
    privateKey: string;
    openseaApproval: boolean;
    blurApproval: boolean;
    magicedenApproval: boolean;
  };
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
  };
  bidPrice: {
    min: number;
    max: number | null;
    minType: "percentage" | "eth" | "usdc";
    maxType: "percentage" | "eth" | "usdc";
  };

  openseaBidPrice: {
    min: number;
    max: number | null;
    minType: "percentage" | "eth" | "usdc";
    maxType: "percentage" | "eth" | "usdc";
  };

  blurBidPrice: {
    min: number;
    max: number | null;
    minType: "percentage" | "eth" | "usdc";
    maxType: "percentage" | "eth" | "usdc";
  };

  magicEdenBidPrice: {
    min: number;
    max: number | null;
    minType: "percentage" | "eth" | "usdc";
    maxType: "percentage" | "eth" | "usdc";
  };

  stopOptions: {
    pauseAllBids: boolean;
    stopAllBids: boolean;
    cancelAllBids: boolean;
    minFloorPrice: number;
    maxFloorPrice: number;
    minTraitPrice: number;
    maxTraitPrice: number;
    maxPurchase: number;
    triggerStopOptions: boolean;
  };
  bidDuration: {
    value: number;
    unit: string;
  };
  loopInterval: {
    value: number;
    unit: string;
  };
  tokenIds: (number | string)[];
  bidType: "collection" | "token";
  bidPriceType: "GENERAL_BID_PRICE" | "MARKETPLACE_BID_PRICE";
  slugValid: boolean;
  magicEdenValid: boolean;
  blurValid: boolean;
  openseaValid: boolean;
  traitsOperator: { value: string; label: string };
}

const TaskSchema: Schema = new Schema(
  {
    user: { type: String, ref: "User", required: true },
    contract: {
      slug: { type: String, required: true },
      contractAddress: { type: String, required: true },
    },
    wallet: {
      address: { type: String, required: true },
      privateKey: { type: String, required: true },
      openseaApproval: { type: Boolean, default: false },
      blurApproval: { type: Boolean, default: false },
      magicedenApproval: { type: Boolean, default: false }
    },
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
        required: true,
      },
    },
    outbidOptions: {
      outbid: { type: Boolean, default: false },
      blurOutbidMargin: { type: Number, default: null },
      openseaOutbidMargin: { type: Number, default: null },
      magicedenOutbidMargin: { type: Number, default: null },
      counterbid: { type: Boolean, default: false },
    },
    bidPrice: {
      min: { type: Number, required: true },
      max: { type: Number, required: false, default: null },
      minType: { type: String, enum: ["percentage", "eth", "usdc"], required: true },
      maxType: { type: String, enum: ["percentage", "eth", "usdc"], required: true },
    },
    openseaBidPrice: {
      min: { type: Number, required: true },
      max: { type: Number, required: false, default: null },
      minType: { type: String, enum: ["percentage", "eth", "usdc"], required: true },
      maxType: { type: String, enum: ["percentage", "eth", "usdc"], required: true },
    },
    blurBidPrice: {
      min: { type: Number, required: true },
      max: { type: Number, required: false, default: null },
      minType: { type: String, enum: ["percentage", "eth", "usdc"], required: true },
      maxType: { type: String, enum: ["percentage", "eth", "usdc"], required: true },
    },
    magicEdenBidPrice: {
      min: { type: Number, required: true },
      max: { type: Number, required: false, default: null },
      minType: { type: String, enum: ["percentage", "eth", "usdc"], required: true },
      maxType: { type: String, enum: ["percentage", "eth", "usdc"], required: true },
    },
    stopOptions: {
      minFloorPrice: { type: Number, required: true },
      maxFloorPrice: { type: Number, required: true },
      minTraitPrice: { type: Number, required: true },
      maxTraitPrice: { type: Number, required: true },
      maxPurchase: { type: Number, required: true },
      pauseAllBids: { type: Boolean, default: false },
      stopAllBids: { type: Boolean, default: false },
      cancelAllBids: { type: Boolean, default: false },
      triggerStopOptions: { type: Boolean, default: false },
    },
    bidDuration: {
      value: { type: Number, required: false, default: 15 },
      unit: { type: String, required: false, default: "minutes" }

    },
    loopInterval: {
      value: { type: Number, required: false, default: 15 },
      unit: { type: String, required: false, default: "minutes" }
    },
    tokenIds: { type: [Schema.Types.Mixed], required: false },
    bidType: {
      type: String,
      enum: ["collection", "token"],
      default: "collection"
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
    }
  },
  { timestamps: true }
);

const Task = mongoose.model<ITask>("Task", TaskSchema);
export default Task;
