import mongoose, { model, Schema, Document } from "mongoose";

export interface IWallet extends Document {
  _id: mongoose.Types.ObjectId;
  user: String;
  name: string;
  address: string;
  encryptedPrivateKey: string;
}

const WalletSchema = new Schema<IWallet>(
  {
    user: { type: String, ref: "User", required: true },
    name: { type: String, required: true },
    address: { type: String, required: true },
    encryptedPrivateKey: { type: String, required: false },
  },
  { timestamps: true }
);

const Wallet = model<IWallet>("Wallet", WalletSchema);

export { Wallet };
export default Wallet;
