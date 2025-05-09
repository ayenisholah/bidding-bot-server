import mongoose, { Document, Schema } from "mongoose";
interface IUser extends Document {
  apiKey: string;
  rateLimit: number;
  email: string;
  password: string;
  completed: boolean;
  verificationToken?: string
  resetPasswordToken?: string
  resetPasswordExpires?: Date
}

const userSchema: Schema<IUser> = new Schema(
  {
    apiKey: { type: String, required: true },
    rateLimit: { type: Number, default: 8 },
    email: { type: String, required: true, unique: true },
    password: { type: String, required: true },
    completed: { type: Boolean, default: false },
    verificationToken: { type: String },
    resetPasswordToken: { type: String },
    resetPasswordExpires: { type: Date },
  },
  { timestamps: true }
);

const User = mongoose.model<IUser>("User", userSchema);

export default User;
