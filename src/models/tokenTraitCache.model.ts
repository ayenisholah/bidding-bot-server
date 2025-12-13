import mongoose, { Schema, Document } from "mongoose";

export interface ITokenTraitCache extends Document {
  _id: mongoose.Types.ObjectId;
  contractAddress: string;
  tokenId: string;
  traits: Array<{
    trait_type: string;
    value: any;
    display_type?: string;
    rarity?: number;
  }>;
  lastUpdated: Date;
  accessedAt: Date; // For LRU eviction
}

const TokenTraitCacheSchema: Schema = new Schema(
  {
    contractAddress: { type: String, required: true, lowercase: true },
    tokenId: { type: String, required: true },
    traits: [{
      trait_type: { type: String, required: true },
      value: { type: Schema.Types.Mixed, required: true },
      display_type: { type: String },
      rarity: { type: Number }
    }],
    lastUpdated: { type: Date, default: Date.now },
    accessedAt: { type: Date, default: Date.now, index: true } // Index for LRU queries
  },
  { timestamps: true }
);

// Compound unique index on contractAddress + tokenId
TokenTraitCacheSchema.index(
  { contractAddress: 1, tokenId: 1 },
  { unique: true }
);

// TTL index to auto-expire documents after 7 days of no access
TokenTraitCacheSchema.index(
  { accessedAt: 1 },
  { expireAfterSeconds: 7 * 24 * 60 * 60 } // 7 days
);

const TokenTraitCache =
  mongoose.models.TokenTraitCache ||
  mongoose.model<ITokenTraitCache>("TokenTraitCache", TokenTraitCacheSchema);

export default TokenTraitCache;
