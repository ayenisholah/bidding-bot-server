import mongoose, { Schema, Document } from "mongoose";

export interface ITokenTrait extends Document {
  contractAddress: string;
  tokenId: string;
  traits: Array<{
    trait_type: string;
    value: any;
    display_type?: string;
    rarity?: number;
  }>;
  floorPrice?: number;
  lastUpdated: Date;
}

const TokenTraitSchema: Schema = new Schema(
  {
    contractAddress: { type: String, required: true, index: true },
    tokenId: { type: String, required: true, index: true },
    traits: [
      {
        trait_type: { type: String, required: true },
        value: { type: Schema.Types.Mixed, required: true },
        display_type: { type: String },
        rarity: { type: Number }
      }
    ],
    floorPrice: { type: Number },
    lastUpdated: { type: Date, default: Date.now, expires: 2592000 } // 30 day TTL (2592000 seconds)
  }
);

// Compound index for efficient lookups by contract + token
TokenTraitSchema.index({ contractAddress: 1, tokenId: 1 }, { unique: true });

const TokenTrait = mongoose.model<ITokenTrait>("TokenTrait", TokenTraitSchema);
export default TokenTrait;
