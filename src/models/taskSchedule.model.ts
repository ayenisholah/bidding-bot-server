import mongoose, { Schema, Document } from "mongoose";

export interface ITaskSchedule extends Document {
  taskId: string;
  marketplace: string;
  scheduledAt: Date;
  nextFireTime: Date;
  loopInterval: number;
  status: 'PENDING' | 'FIRING' | 'COMPLETED' | 'CANCELLED';
  executionId: string;
  firedAt?: Date;
  attempts: number;
  createdAt: Date;
  updatedAt: Date;
}

const TaskScheduleSchema: Schema = new Schema(
  {
    taskId: { type: String, required: true, index: true },
    marketplace: { type: String, required: true },
    scheduledAt: { type: Date, required: true },
    nextFireTime: { type: Date, required: true },
    loopInterval: { type: Number, required: true },
    status: {
      type: String,
      enum: ['PENDING', 'FIRING', 'COMPLETED', 'CANCELLED'],
      default: 'PENDING',
      required: true
    },
    executionId: { type: String, required: true },
    firedAt: { type: Date },
    attempts: { type: Number, default: 0 }
  },
  { timestamps: true }
);

// Unique constraint: one schedule per task/marketplace combination
TaskScheduleSchema.index({ taskId: 1, marketplace: 1 }, { unique: true });

// Watchdog query index: find overdue PENDING schedules
TaskScheduleSchema.index({ status: 1, nextFireTime: 1 });

// Cleanup query index: find old COMPLETED/CANCELLED schedules
TaskScheduleSchema.index({ status: 1, updatedAt: 1 });

const TaskSchedule = mongoose.model<ITaskSchedule>("TaskSchedule", TaskScheduleSchema);
export default TaskSchedule;
