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
    tags: [{ name: String, color: String }],
    finalListingPrice: {
      opensea: { type: Number, default: 0 },
      blur: { type: Number, default: 0 },
      magiceden: { type: Number, default: 0 },
    },
    instantPriceMatching: { type: Boolean, default: false },
    listingPrice: {
      value: { type: String },
      type: {
        type: String,
        enum: ["percentage", "eth"],
      },
    },
    underCutMargin: { type: String, default: "" },
    openseaListingPrice: {
      value: { type: String },
      type: {
        type: String,
        enum: ["percentage", "eth"],
      },
    },
    blurListingPrice: {
      value: { type: String },
      type: {
        type: String,
        enum: ["percentage", "eth"],
      },
    },
    magicEdenListingPrice: {
      value: { type: String },
      type: {
        type: String,
        enum: ["percentage", "eth"],
      },
    },
    minProfit: {
      value: { type: String, default: "0" },
      type: {
        type: String,
        enum: ["percentage", "eth"],
        default: "eth",
      },
    },
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
    listed: { type: Boolean, default: false },
  },
  { timestamps: true }
);

const Listing = mongoose.model<IListing>("Listing", ListingSchema);
export default Listing;
