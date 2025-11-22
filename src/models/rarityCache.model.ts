import mongoose, { Schema, Document } from "mongoose";

export interface IRarityCache extends Document {
  _id: mongoose.Types.ObjectId;
  collectionSlug: string;
  contractAddress: string;
  rarityCategory: "LEGENDARY" | "EPIC" | "RARE";
  tokenIds: number[];
  totalCount: number;
  lastUpdated: Date;
}

const RarityCacheSchema: Schema = new Schema(
  {
    collectionSlug: { type: String, required: true },
    contractAddress: { type: String, required: true },
    rarityCategory: {
      type: String,
      enum: ["LEGENDARY", "EPIC", "RARE"],
      required: true,
    },
    tokenIds: { type: [Number], required: true },
    totalCount: { type: Number, required: true },
    lastUpdated: { type: Date, default: Date.now },
  },
  { timestamps: true }
);

// Create compound unique index on collectionSlug and rarityCategory
RarityCacheSchema.index(
  { collectionSlug: 1, rarityCategory: 1 },
  { unique: true }
);

const RarityCache =
  mongoose.models.RarityCache ||
  mongoose.model<IRarityCache>("RarityCache", RarityCacheSchema);

export default RarityCache;
