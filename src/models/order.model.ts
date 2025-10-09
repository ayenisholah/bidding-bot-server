import mongoose, { Schema } from "mongoose";

export interface IOrder {
  _id: string;
  orderHash: string;
  expiresAt: Date;
  offerPrice: string;
  identifier: string;
  taskId: string;
  marketplace: string;
  slug: string;
  createdAt: Date;
  updatedAt: Date;
}

const OrderSchema: Schema = new Schema(
  {
    orderHash: { type: String, required: true },
    expiresAt: { type: Date, required: true },
    offerPrice: { type: String, required: true },
    identifier: { type: String, required: true },
    taskId: { type: String, required: true },
    marketplace: { type: String, required: true },
    slug: { type: String, required: true }
  },
  { timestamps: true }
);

// Add indices for optimal query performance
OrderSchema.index({ taskId: 1 });
OrderSchema.index({ marketplace: 1 });
OrderSchema.index({ taskId: 1, marketplace: 1 });
OrderSchema.index({ expiresAt: 1 });
OrderSchema.index({ orderHash: 1 });
// Critical compound index for the most frequent query pattern
OrderSchema.index({ taskId: 1, marketplace: 1, expiresAt: 1 });
// Index for active order queries (most common pattern)
OrderSchema.index({ expiresAt: 1, taskId: 1 });

const Order = mongoose.model<IOrder>("Order", OrderSchema)
export default Order;