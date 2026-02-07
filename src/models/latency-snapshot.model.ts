import mongoose, { Document, Schema } from 'mongoose';

/**
 * Statistics for latency measurements
 */
export interface LatencyStats {
  p50: number;
  p95: number;
  p99: number;
  avg: number;
  max: number;
  sampleCount: number;
}

/**
 * Extended statistics including success rate
 */
export interface ExecutionStats extends LatencyStats {
  successRate: number;
}

/**
 * System context for correlation analysis
 */
export interface SnapshotContext {
  heapUsedMB: number;
  heapPercent: number;
  rssMB: number;
  queueDepth: {
    main: number;
    counterbid: number;
  };
  activeJobs: {
    main: number;
    counterbid: number;
  };
}

/**
 * Latency snapshot document interface
 */
export interface ILatencySnapshot extends Document {
  timestamp: Date;

  // Forwarding latency stats (proxy → server network time)
  forwardingLatency: {
    overall: LatencyStats;
    byMarketplace: {
      opensea: LatencyStats;
      blur: LatencyStats;
      magiceden: LatencyStats;
    };
  };

  // Execution latency stats (proxy receivedAt → job completion)
  executionLatency: {
    overall: ExecutionStats;
    byMarketplace: {
      opensea: ExecutionStats;
      blur: ExecutionStats;
      magiceden: ExecutionStats;
    };
  };

  // Context for correlation analysis
  context: SnapshotContext;

  metadata: {
    snapshotDurationMs: number;
  };
}

/**
 * Latency statistics schema
 */
const LatencyStatsSchema = new Schema<LatencyStats>(
  {
    p50: { type: Number, required: true, default: 0 },
    p95: { type: Number, required: true, default: 0 },
    p99: { type: Number, required: true, default: 0 },
    avg: { type: Number, required: true, default: 0 },
    max: { type: Number, required: true, default: 0 },
    sampleCount: { type: Number, required: true, default: 0 },
  },
  { _id: false }
);

/**
 * Execution statistics schema (includes success rate)
 */
const ExecutionStatsSchema = new Schema<ExecutionStats>(
  {
    p50: { type: Number, required: true, default: 0 },
    p95: { type: Number, required: true, default: 0 },
    p99: { type: Number, required: true, default: 0 },
    avg: { type: Number, required: true, default: 0 },
    max: { type: Number, required: true, default: 0 },
    sampleCount: { type: Number, required: true, default: 0 },
    successRate: { type: Number, required: true, default: 0 },
  },
  { _id: false }
);

/**
 * Context schema
 */
const SnapshotContextSchema = new Schema<SnapshotContext>(
  {
    heapUsedMB: { type: Number, required: true, default: 0 },
    heapPercent: { type: Number, required: true, default: 0 },
    rssMB: { type: Number, required: true, default: 0 },
    queueDepth: {
      main: { type: Number, required: true, default: 0 },
      counterbid: { type: Number, required: true, default: 0 },
    },
    activeJobs: {
      main: { type: Number, required: true, default: 0 },
      counterbid: { type: Number, required: true, default: 0 },
    },
  },
  { _id: false }
);

/**
 * Latency snapshot schema
 */
const LatencySnapshotSchema = new Schema<ILatencySnapshot>(
  {
    timestamp: {
      type: Date,
      required: true,
      default: Date.now,
      index: true, // Fast time-range queries
    },

    forwardingLatency: {
      overall: {
        type: LatencyStatsSchema,
        required: true,
      },
      byMarketplace: {
        opensea: {
          type: LatencyStatsSchema,
          required: true,
        },
        blur: {
          type: LatencyStatsSchema,
          required: true,
        },
        magiceden: {
          type: LatencyStatsSchema,
          required: true,
        },
      },
    },

    executionLatency: {
      overall: {
        type: ExecutionStatsSchema,
        required: true,
      },
      byMarketplace: {
        opensea: {
          type: ExecutionStatsSchema,
          required: true,
        },
        blur: {
          type: ExecutionStatsSchema,
          required: true,
        },
        magiceden: {
          type: ExecutionStatsSchema,
          required: true,
        },
      },
    },

    context: {
      type: SnapshotContextSchema,
      required: true,
    },

    metadata: {
      snapshotDurationMs: {
        type: Number,
        required: true,
        default: 0,
      },
    },
  },
  {
    timestamps: false, // We manage timestamp manually
    collection: 'latencysnapshots',
  }
);

// TTL index for automatic 30-day deletion
LatencySnapshotSchema.index({ timestamp: 1 }, { expireAfterSeconds: 30 * 24 * 60 * 60 });

// Compound index for time-range queries
LatencySnapshotSchema.index({ timestamp: -1 });

/**
 * Latency snapshot model
 */
export const LatencySnapshot = mongoose.model<ILatencySnapshot>(
  'LatencySnapshot',
  LatencySnapshotSchema
);
