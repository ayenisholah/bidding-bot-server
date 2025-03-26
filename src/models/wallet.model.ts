import mongoose, { model, Model, Schema } from "mongoose";

interface IWallet extends Document {
  _id: mongoose.Types.ObjectId;
  user: String;
  name: string;
  address: string;
  privateKey: string;
  openseaApproval: boolean
  blurApproval: boolean
  magicedenApproval: boolean
}

const WalletSchema = new Schema<IWallet>(
  {
    user: { type: String, ref: "User", required: true },
    name: { type: String, required: true },
    address: { type: String, required: true },
    privateKey: { type: String, required: true },
    openseaApproval: { type: Boolean, default: false },
    blurApproval: { type: Boolean, default: false },
    magicedenApproval: { type: Boolean, default: false }
  },
  { timestamps: true }
);

// MongoDB command to fetch all wallets:
// db.wallets.find({})

const Wallet = model<IWallet>("Wallet", WalletSchema);

export default Wallet;
