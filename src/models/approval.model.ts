import mongoose, { Schema } from "mongoose";

export interface IApproval {
  _id: string;
  address: string;
  openseaApproval: boolean;
  magicedenApproval: boolean;
  currency: string;
  createdAt: Date;
  updatedAt: Date;
}

const ApprovalSchema: Schema = new Schema(
  {
    address: { type: String, required: true, unique: true },
    openseaApproval: { type: Boolean, default: false },
    magicedenApproval: { type: Boolean, default: false },
    currency: { type: String, required: true }
  },
  { timestamps: true }
);

// Add indices for optimal query performance
ApprovalSchema.index({ address: 1 }, { unique: true });

const Approval = mongoose.model<IApproval>("Approval", ApprovalSchema)
export default Approval;