import mongoose, { Schema, Document } from 'mongoose';

interface RedisKeyCategory {
  pattern: string;
  keyCount: number;
  totalMemoryBytes: number;
  sampleKeys?: string[];
}

interface CacheStats {
  name: string;
  size: number;
  maxSize: number;
  ttl: number;
  estimatedMemoryBytes?: number;
}

interface GlobalStateStats {
  bidStats: { taskCount: number; totalEntries: number };
  skipStats: { taskCount: number; totalEntries: number };
  errorStats: { taskCount: number; totalEntries: number };
  counterbidCounts: { taskCount: number; totalEntries: number };
  processedOwnBids: { setSize: number };
  balances: { walletCount: number; totalEntries: number };
  taskStatus: { taskCount: number };
  bestOffers: { taskCount: number; totalEntries: number };
  floorPrices: { taskCount: number; totalEntries: number };
}

interface MongoCollectionStats {
  name: string;
  sizeBytes: number;
  documentCount: number;
  avgDocumentSize: number;
  indexSizes: { [indexName: string]: number };
  totalIndexSize: number;
}

interface MemoryCleanupStats {
  initialized: boolean;
  activeCleanupIntervals: number;
  currentSizes: {
    queueingProgress: number;
    failedTaskStartQueue: number;
    failedTaskStopQueue: number;
    taskLocks: number;
    loopSchedulingLocks: number;
  };
}

interface LRUCachesStats {
  processedMessages: number;
}

interface UnboundedDataStructuresStats {
  traitsToRemove: number;
  removedTokens: number;
  tasksBeingRestarted: number;
  tasksBeingCancelled: number;
  tasksInCompletionProcess: number;
  failedStartQueue: number;
  failedStopQueue: number;
  queueingProgress: number;
  taskLocks: number;
  loopSchedulingLocks: number;
}

export interface IMemorySnapshot extends Document {
  timestamp: Date;

  // Node.js process memory
  processMemory: {
    rss: number;
    heapTotal: number;
    heapUsed: number;
    heapUsedPercentage: number;
    external: number;
    arrayBuffers: number;
  };

  // Redis memory by category
  redisMemory: {
    categories: {
      orderCache: RedisKeyCategory;
      orderIdIndex: RedisKeyCategory;
      taskOrdersSets: RedisKeyCategory;
      taskSchedule: RedisKeyCategory;
      taskRuntime: RedisKeyCategory;
      rarityCache: RedisKeyCategory;
      approvalCache: RedisKeyCategory;
      bullMQMain: RedisKeyCategory;
      bullMQBlur: RedisKeyCategory;
      other: RedisKeyCategory;
    };
    totals: {
      totalKeys: number;
      totalMemoryBytes: number;
      totalMemoryMB: number;
    };
  };

  // In-memory caches
  inMemoryCaches: CacheStats[];

  // Global state
  globalState: GlobalStateStats;

  // BullMQ queues
  queues: {
    main: { waiting: number; active: number; completed: number; failed: number; delayed: number };
    blur: { waiting: number; active: number; completed: number; failed: number; delayed: number };
  };

  // MongoDB collections
  mongoCollections: MongoCollectionStats[];

  // Memory cleanup service stats
  memoryCleanup: MemoryCleanupStats;

  // LRU caches (processedMessages with automatic TTL)
  lruCaches: LRUCachesStats;

  // Unbounded data structures monitoring
  unboundedDataStructures: UnboundedDataStructuresStats;

  // Metadata
  metadata: {
    snapshotDurationMs: number;
    errorCount: number;
    errors?: string[];
  };
}

const RedisKeyCategorySchema = new Schema({
  pattern: { type: String, required: true },
  keyCount: { type: Number, required: true },
  totalMemoryBytes: { type: Number, required: true },
  sampleKeys: [String]
}, { _id: false });

const CacheStatsSchema = new Schema({
  name: { type: String, required: true },
  size: { type: Number, required: true },
  maxSize: { type: Number, required: true },
  ttl: { type: Number, required: true },
  estimatedMemoryBytes: Number
}, { _id: false });

const MongoCollectionStatsSchema = new Schema({
  name: { type: String, required: true },
  sizeBytes: { type: Number, required: true },
  documentCount: { type: Number, required: true },
  avgDocumentSize: { type: Number, required: true },
  indexSizes: { type: Schema.Types.Mixed },
  totalIndexSize: { type: Number, required: true }
}, { _id: false });

const MemoryCleanupStatsSchema = new Schema({
  initialized: { type: Boolean, required: true },
  activeCleanupIntervals: { type: Number, required: true },
  currentSizes: {
    queueingProgress: { type: Number, required: true },
    failedTaskStartQueue: { type: Number, required: true },
    failedTaskStopQueue: { type: Number, required: true },
    taskLocks: { type: Number, required: true },
    loopSchedulingLocks: { type: Number, required: true }
  }
}, { _id: false });

const LRUCachesStatsSchema = new Schema({
  processedMessages: { type: Number, required: true }
}, { _id: false });

const UnboundedDataStructuresStatsSchema = new Schema({
  traitsToRemove: { type: Number, required: true },
  removedTokens: { type: Number, required: true },
  tasksBeingRestarted: { type: Number, required: true },
  tasksBeingCancelled: { type: Number, required: true },
  tasksInCompletionProcess: { type: Number, required: true },
  failedStartQueue: { type: Number, required: true },
  failedStopQueue: { type: Number, required: true },
  queueingProgress: { type: Number, required: true },
  taskLocks: { type: Number, required: true },
  loopSchedulingLocks: { type: Number, required: true }
}, { _id: false });

const MemorySnapshotSchema = new Schema<IMemorySnapshot>(
  {
    timestamp: {
      type: Date,
      required: true,
      default: Date.now,
      index: { expires: '30d' } // TTL index - auto-delete after 30 days
    },

    processMemory: {
      rss: { type: Number, required: true },
      heapTotal: { type: Number, required: true },
      heapUsed: { type: Number, required: true },
      heapUsedPercentage: { type: Number, required: true },
      external: { type: Number, required: true },
      arrayBuffers: { type: Number, required: true }
    },

    redisMemory: {
      categories: {
        orderCache: { type: RedisKeyCategorySchema, required: true },
        orderIdIndex: { type: RedisKeyCategorySchema, required: true },
        taskOrdersSets: { type: RedisKeyCategorySchema, required: true },
        taskSchedule: { type: RedisKeyCategorySchema, required: true },
        taskRuntime: { type: RedisKeyCategorySchema, required: true },
        rarityCache: { type: RedisKeyCategorySchema, required: true },
        approvalCache: { type: RedisKeyCategorySchema, required: true },
        bullMQMain: { type: RedisKeyCategorySchema, required: true },
        bullMQBlur: { type: RedisKeyCategorySchema, required: true },
        other: { type: RedisKeyCategorySchema, required: true }
      },
      totals: {
        totalKeys: { type: Number, required: true },
        totalMemoryBytes: { type: Number, required: true },
        totalMemoryMB: { type: Number, required: true }
      }
    },

    inMemoryCaches: [CacheStatsSchema],

    globalState: {
      bidStats: {
        taskCount: { type: Number, required: true },
        totalEntries: { type: Number, required: true }
      },
      skipStats: {
        taskCount: { type: Number, required: true },
        totalEntries: { type: Number, required: true }
      },
      errorStats: {
        taskCount: { type: Number, required: true },
        totalEntries: { type: Number, required: true }
      },
      counterbidCounts: {
        taskCount: { type: Number, required: true },
        totalEntries: { type: Number, required: true }
      },
      processedOwnBids: {
        setSize: { type: Number, required: true }
      },
      balances: {
        walletCount: { type: Number, required: true },
        totalEntries: { type: Number, required: true }
      },
      taskStatus: {
        taskCount: { type: Number, required: true }
      },
      bestOffers: {
        taskCount: { type: Number, required: true },
        totalEntries: { type: Number, required: true }
      },
      floorPrices: {
        taskCount: { type: Number, required: true },
        totalEntries: { type: Number, required: true }
      }
    },

    queues: {
      main: {
        waiting: { type: Number, required: true },
        active: { type: Number, required: true },
        completed: { type: Number, required: true },
        failed: { type: Number, required: true },
        delayed: { type: Number, required: true }
      },
      blur: {
        waiting: { type: Number, required: true },
        active: { type: Number, required: true },
        completed: { type: Number, required: true },
        failed: { type: Number, required: true },
        delayed: { type: Number, required: true }
      }
    },

    mongoCollections: [MongoCollectionStatsSchema],

    memoryCleanup: { type: MemoryCleanupStatsSchema, required: true },
    lruCaches: { type: LRUCachesStatsSchema, required: true },
    unboundedDataStructures: { type: UnboundedDataStructuresStatsSchema, required: true },

    metadata: {
      snapshotDurationMs: { type: Number, required: true },
      errorCount: { type: Number, required: true },
      errors: [String]
    }
  },
  {
    timestamps: true,
    collection: 'memorysnapshots'
  }
);

// Index for fast queries (descending order - newest first)
MemorySnapshotSchema.index({ timestamp: -1 });

export const MemorySnapshot = mongoose.model<IMemorySnapshot>(
  'MemorySnapshot',
  MemorySnapshotSchema
);
