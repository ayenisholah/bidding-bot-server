import mongoose, { Schema, Document } from "mongoose";
import { IListingBase } from "../types/listing.types";

export interface IListing extends IListingBase, Document {
  _id: mongoose.Types.ObjectId;
  cleanupExpiredListings(): boolean;
  checkAndCleanExpiredListings(): Promise<boolean>;
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
    underCutMargin: { type: String, default: "0.0000" },
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
    doNotListBelowFloor: { type: Boolean, default: false },
    selectedTrait: {
      trait_type: { type: String },
      value: { type: Schema.Types.Mixed },
    },
    listed: { type: Boolean, default: false },
    instantPriceOverride: { type: Number }, // Price override for instant price matching
    instantPriceMatchingActiveUntil: { type: Date }, // When instant price matching session expires
  },
  { timestamps: true }
);

// Method to clean up expired marketplace listings
ListingSchema.methods.cleanupExpiredListings = function() {
  const now = new Date();
  const marketplaces: Array<'opensea' | 'blur' | 'magiceden'> = ['opensea', 'blur', 'magiceden'];
  let hasChanges = false;
  
  for (const marketplace of marketplaces) {
    const expiration = this.listingExpiration?.[marketplace];
    if (expiration && new Date(expiration) <= now) {
      // Expired - remove the final listing price
      if (this.finalListingPrice?.[marketplace] !== undefined) {
        delete this.finalListingPrice[marketplace];
        hasChanges = true;
      }
    }
  }
  
  // Also clean up expired instant price matching sessions
  if (this.instantPriceMatchingActiveUntil && new Date(this.instantPriceMatchingActiveUntil) <= now) {
    this.instantPriceOverride = undefined;
    this.instantPriceMatchingActiveUntil = undefined;
    hasChanges = true;
  }
  
  return hasChanges;
};

// Pre-save hook to clean up expired listings before saving
ListingSchema.pre('save', function(this: IListing, next) {
  this.cleanupExpiredListings();
  next();
});

// Instance method to manually clean up expired listings
ListingSchema.methods.checkAndCleanExpiredListings = async function() {
  const hasChanges = this.cleanupExpiredListings();
  if (hasChanges) {
    await this.save();
  }
  return hasChanges;
};

const Listing = mongoose.model<IListing>("Listing", ListingSchema);
export default Listing;
