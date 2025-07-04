import mongoose, { Schema, Document } from "mongoose";
import { IListingBase } from "../types/listing.types";

export interface IListing extends IListingBase, Document {
  _id: mongoose.Types.ObjectId;
}

const ListingSchema: Schema = new Schema(
  {
    user: { type: String, ref: "User", required: true },
    tokenId: { type: String, required: true },
    contract: {
      slug: { type: String, required: true },
      contractAddress: { type: String, required: true },
    },
    wallet: { type: String },
    walletId: { type: mongoose.Schema.Types.ObjectId, ref: "Wallet" },
    selectedMarketplaces: { type: [String], required: true },
    running: { type: Boolean, default: false },
    finalListingPrice: {
      opensea: { type: Number, default: 0 },
      blur: { type: Number, default: 0 },
      magiceden: { type: Number, default: 0 },
    },
    listingExpiration: {
      opensea: { type: Date },
      blur: { type: Date },
      magiceden: { type: Date },
    },
    lastListedAt: {
      opensea: { type: Date },
      blur: { type: Date },
      magiceden: { type: Date },
    },
    instantPriceMatching: { type: Boolean, default: false },
    basePrice: { type: String, default: "" },
    underCutMargin: { type: String, default: "" },
    openseaListingPrice: { type: String, default: "" },
    blurListingPrice: { type: String, default: "" },
    magicEdenListingPrice: { type: String, default: "" },
    minListingPrice: { type: String, default: "" },
    minimumProfit: { type: String, default: "" },
    loopInterval: {
      value: { type: Number, required: false, default: 15 },
      unit: { type: String, required: false, default: "minutes" },
    },
    listDuration: {
      value: { type: Number, required: false, default: 15 },
      unit: { type: String, required: false, default: "minutes" },
    },
    listingPriceType: {
      type: String,
      enum: ["GENERAL_LISTING_PRICE", "MARKETPLACE_LISTING_PRICE"],
      default: "GENERAL_LISTING_PRICE",
    },
    pricingStrategy: {
      type: String,
      enum: ["FIXED_PRICING", "DYNAMIC_PRICING"],
      default: "FIXED_PRICING",
    },
    floorPrice: { type: Number, default: 0 },
    buyPrice: { type: Number },
    dynamicPricingBasis: {
      type: String,
      enum: [
        "HIGHEST_TRAIT_FLOOR",
        "SPECIFIC_TRAIT",
        "COLLECTION_FLOOR",
        "BUY_PRICE",
      ],
      default: "COLLECTION_FLOOR",
    },
    selectedTrait: {
      trait_type: { type: String },
      value: { type: Schema.Types.Mixed },
    },
    listed: { type: Boolean, default: false },
  },
  { timestamps: true }
);

const Listing = mongoose.model<IListing>("Listing", ListingSchema);
export default Listing;
