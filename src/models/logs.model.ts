import mongoose from "mongoose";

export interface IBidLogs {
  taskId: string;
  title: string;
  message: string;
  messageHash?: string;
  type: "skipped" | "warning" | "error";
  marketplace: "opensea" | "magiceden" | "blur";
}

const bidLogsSchema = new mongoose.Schema<IBidLogs>({
  taskId: { type: String, required: true },
  title: { type: String, required: true },
  message: { type: String, required: true },
  messageHash: { type: String, required: false },
  type: { type: String, required: true },
  marketplace: { type: String, required: true }
},
  { timestamps: true }
);

bidLogsSchema.index({ taskId: 1 });
bidLogsSchema.index({ taskId: 1, messageHash: 1, marketplace: 1 });

const BidLogs = mongoose.model<IBidLogs>('BidLogs', bidLogsSchema);

export default BidLogs;
