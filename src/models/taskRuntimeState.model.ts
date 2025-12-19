import mongoose, { Schema, Document } from "mongoose";

/**
 * TaskRuntimeState stores volatile per-task state in MongoDB
 * This moves unbounded in-memory state to persistent storage
 * with an in-memory LRU cache layer for hot tasks
 */

export interface MarketplaceCounts {
  opensea: number;
  magiceden: number;
  blur: number;
}

export interface MarketplaceTokenIds {
  opensea: string[];
  magiceden: string[];
}

export interface MarketplaceTraits {
  opensea: Record<string, string[]>;
  magiceden: Record<string, string[]>;
  blur: Record<string, string[]>;
}

export interface MarketplaceBooleans {
  opensea: boolean;
  magiceden: boolean;
  blur: boolean;
}

export interface MarketplaceDates {
  opensea?: Date;
  magiceden?: Date;
  blur?: Date;
}

export interface TaskStateInfo {
  status: 'STARTED' | 'QUEUEING' | 'RUNNING' | 'COMPLETED' | 'SCHEDULED' | 'STOPPED';
  queuedAt?: Date;        // When task entered QUEUEING state
  jobsTotal?: number;     // Total jobs queued for current loop
  jobsCompleted?: number; // Jobs completed so far in current loop
  lastActivityAt?: Date;  // Last job completion time
}

export interface ITaskRuntimeState extends Document {
  taskId: string;

  // Bid statistics per marketplace
  bidStats: MarketplaceCounts;

  // Skip statistics per marketplace
  skipStats: MarketplaceCounts;

  // Error statistics per marketplace
  errorStats: MarketplaceCounts;

  // Current token IDs being bid on
  currentTokenIds: MarketplaceTokenIds;

  // Current traits being bid on
  currentTraits: MarketplaceTraits;

  // Floor prices per marketplace
  floorPrices: MarketplaceCounts;

  // Best offers per marketplace
  bestOffers: MarketplaceCounts;

  // Warning flags per marketplace
  warningBids: MarketplaceBooleans;

  // Task state for QUEUEING watchdog recovery
  taskState?: TaskStateInfo;

  // Next loop times per marketplace (for timer recovery)
  nextLoopTimes?: MarketplaceDates;

  // Timestamps for cache management
  accessedAt: Date;
  createdAt: Date;
  updatedAt: Date;
}

const MarketplaceCountsSchema = new Schema({
  opensea: { type: Number, default: 0 },
  magiceden: { type: Number, default: 0 },
  blur: { type: Number, default: 0 }
}, { _id: false });

const MarketplaceTokenIdsSchema = new Schema({
  opensea: { type: [String], default: [] },
  magiceden: { type: [String], default: [] }
}, { _id: false });

const MarketplaceBooleansSchema = new Schema({
  opensea: { type: Boolean, default: false },
  magiceden: { type: Boolean, default: false },
  blur: { type: Boolean, default: false }
}, { _id: false });

const MarketplaceDatesSchema = new Schema({
  opensea: { type: Date },
  magiceden: { type: Date },
  blur: { type: Date }
}, { _id: false });

const TaskStateInfoSchema = new Schema({
  status: {
    type: String,
    enum: ['STARTED', 'QUEUEING', 'RUNNING', 'COMPLETED', 'SCHEDULED', 'STOPPED'],
    default: 'STARTED'
  },
  queuedAt: { type: Date },
  jobsTotal: { type: Number, default: 0 },
  jobsCompleted: { type: Number, default: 0 },
  lastActivityAt: { type: Date, default: Date.now }
}, { _id: false });

const TaskRuntimeStateSchema: Schema = new Schema(
  {
    taskId: { type: String, required: true, unique: true, index: true },

    bidStats: { type: MarketplaceCountsSchema, default: () => ({ opensea: 0, magiceden: 0, blur: 0 }) },
    skipStats: { type: MarketplaceCountsSchema, default: () => ({ opensea: 0, magiceden: 0, blur: 0 }) },
    errorStats: { type: MarketplaceCountsSchema, default: () => ({ opensea: 0, magiceden: 0, blur: 0 }) },
    floorPrices: { type: MarketplaceCountsSchema, default: () => ({ opensea: 0, magiceden: 0, blur: 0 }) },
    bestOffers: { type: MarketplaceCountsSchema, default: () => ({ opensea: 0, magiceden: 0, blur: 0 }) },

    currentTokenIds: { type: MarketplaceTokenIdsSchema, default: () => ({ opensea: [], magiceden: [] }) },

    // Store traits as mixed type since it's Record<string, string[]>
    currentTraits: {
      type: Schema.Types.Mixed,
      default: () => ({ opensea: {}, magiceden: {}, blur: {} })
    },

    warningBids: { type: MarketplaceBooleansSchema, default: () => ({ opensea: false, magiceden: false, blur: false }) },

    // Task state for QUEUEING watchdog recovery
    taskState: { type: TaskStateInfoSchema },

    // Next loop times per marketplace (for timer recovery)
    nextLoopTimes: { type: MarketplaceDatesSchema },

    accessedAt: { type: Date, default: Date.now }
  },
  { timestamps: true }
);

// TTL Index: Auto-expire documents after 7 days of no access (also used for LRU eviction queries)
TaskRuntimeStateSchema.index({ accessedAt: 1 }, { expireAfterSeconds: 7 * 24 * 60 * 60 });

// Index for QUEUEING watchdog: find tasks stuck in QUEUEING state
TaskRuntimeStateSchema.index({ 'taskState.status': 1, 'taskState.queuedAt': 1 });

const TaskRuntimeState = mongoose.model<ITaskRuntimeState>("TaskRuntimeState", TaskRuntimeStateSchema);
export default TaskRuntimeState;
