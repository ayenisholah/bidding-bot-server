/**
 * Bid Trace Logging System Types
 *
 * Comprehensive type definitions for tracing bid attempts through
 * their complete lifecycle - from WebSocket receipt to API completion.
 */

export type BidTraceLevel = 'trace' | 'debug' | 'info' | 'warn' | 'error';

export type BidTraceStage =
  // WebSocket ingestion
  | 'ws_received'
  | 'ws_routed'

  // Deduplication stages
  | 'dedup_orderhash_check'
  | 'dedup_orderhash_passed'
  | 'dedup_orderhash_blocked'
  | 'dedup_pending_check'
  | 'dedup_pending_passed'
  | 'dedup_pending_blocked'

  // Decision points
  | 'top_bid_check'
  | 'top_bid_skip'
  | 'price_calculation'
  | 'balance_check'
  | 'balance_insufficient'

  // Job lifecycle
  | 'job_queued'
  | 'job_started'
  | 'job_completed'
  | 'job_failed'

  // Bid operations
  | 'bid_api_start'
  | 'bid_api_success'
  | 'bid_api_error'
  | 'cancel_start'
  | 'cancel_complete'

  // Early exits
  | 'early_return';

export type BidTraceMarketplace = 'opensea' | 'magiceden' | 'blur';
export type BidTraceBidType = 'token' | 'trait' | 'collection' | 'rarity';

export interface BidTraceEntry {
  // Correlation IDs - critical for linking events
  traceId: string; // Unique ID for this specific bid attempt
  correlationId: string; // Links events across tasks (orderHash-based)

  // Timing
  timestamp: string; // ISO 8601
  elapsedMs?: number; // Time since trace creation

  // Event classification
  stage: BidTraceStage;
  level: BidTraceLevel;
  marketplace: BidTraceMarketplace;

  // Task/Bid identifiers
  taskId: string;
  tokenId?: string;
  traitKey?: string; // For trait bids: "traitType:traitValue"
  collectionSlug: string;
  contractAddress?: string;
  bidType: BidTraceBidType;

  // Bid details
  orderHash?: string; // Incoming competitor order hash
  ourOrderHash?: string; // Our bid's order hash (after placement)
  incomingBidAmount?: string; // Wei
  ourBidAmount?: string; // Wei

  // Decision data
  decision?: string; // Human-readable decision reason
  dedupKey?: string; // Key used for deduplication check
  existingBidAmount?: string; // If we have an existing bid

  // Job context
  jobId?: string; // BullMQ job ID
  jobName?: string; // Job type (e.g., OPENSEA_TOKEN_BID_COUNTERBID)
  jobAttempt?: number; // Current attempt number
  queueDepth?: number; // Jobs waiting in queue at this moment

  // Error context
  error?: {
    message: string;
    code?: string;
    stack?: string;
  };

  // Arbitrary metadata for extensibility
  metadata?: Record<string, unknown>;
}

export interface BidTraceFilter {
  traceId?: string;
  correlationId?: string;
  taskId?: string;
  tokenId?: string;
  traitKey?: string;
  marketplace?: BidTraceMarketplace;
  stage?: BidTraceStage | BidTraceStage[];
  level?: BidTraceLevel | BidTraceLevel[];
  startTime?: Date;
  endTime?: Date;
  orderHash?: string;
  collectionSlug?: string;
  bidType?: BidTraceBidType;
  hasError?: boolean;
}

export interface BidTraceSummary {
  traceId: string;
  correlationId: string;
  taskId: string;
  tokenId?: string;
  traitKey?: string;
  marketplace: BidTraceMarketplace;
  bidType: BidTraceBidType;
  collectionSlug: string;
  startTime: string;
  endTime?: string;
  durationMs?: number;
  outcome: 'success' | 'rejected' | 'failed' | 'pending';
  rejectReason?: string;
  errorMessage?: string;
  stages: BidTraceStage[];
  ourBidAmount?: string;
}

export interface BidTraceQueryResult {
  traces: BidTraceEntry[];
  summary: {
    total: number;
    byLevel: Record<BidTraceLevel, number>;
    byStage: Record<string, number>;
    byOutcome: {
      success: number;
      rejected: number;
      failed: number;
      pending: number;
    };
  };
  pagination: {
    total: number;
    limit: number;
    offset: number;
    hasMore: boolean;
  };
}

export interface BidTraceDuplicateGroup {
  correlationId: string;
  tokenId: string;
  taskId: string;
  marketplace: BidTraceMarketplace;
  timeWindowMs: number;
  traces: BidTraceEntry[];
  successfulBids: number;
  firstBidTime: string;
  lastBidTime: string;
}

/**
 * Context passed through job data for trace continuity
 */
export interface BidTraceContext {
  traceId: string;
  correlationId: string;
  startTime: number;
}
