/**
 * Cursor-Based Pull Architecture - Shared Types
 *
 * These types match the ws-proxy NormalizedBidEvent schema and API responses.
 * They are duplicated here to avoid cross-project dependencies.
 */

export interface NormalizedBidEvent {
  // Monotonic sequence number (never reused)
  seq: number;

  // Timestamp when event was received from marketplace
  receivedAt: number;

  // Normalized fields
  marketplace: 'opensea' | 'blur' | 'magiceden';
  eventType: 'bid_created' | 'trait_offer' | 'collection_offer' | 'listing_created' | 'item_sold';

  // Collection identifiers
  slug: string;
  contractAddress: string;

  // Bid details (optional for non-bid events)
  tokenId?: string;
  traitType?: string;
  traitValue?: string;

  // Order details
  orderHash: string;
  maker: string;
  priceWei: string;
  priceEth: number;

  // Currency
  currency: string;
  currencyDecimals: number;

  // Raw payload for edge cases
  rawPayload: unknown;
}

export interface PullResponse {
  events: NormalizedBidEvent[];
  head: number;      // Latest sequence number
  tail: number;      // Oldest available sequence number
  count: number;     // Events in buffer
  timestamp: number; // Server time
}

export interface HeadResponse {
  head: number;
  tail: number;
  count: number;
  timestamp: number;
}

export interface EventLogStats {
  capacity: number;
  count: number;
  head: number;
  tail: number;
  totalEvents: number;
  overwrites: number;
  utilizationPercent: number;
}

/**
 * Pull consumer configuration
 */
export interface PullConsumerConfig {
  proxyUrl: string;           // ws-proxy URL (e.g., https://nfttools-ws-proxy.nfttools.io/)
  pullIntervalMs?: number;    // How often to pull (default 50ms)
  batchSize?: number;         // Max events per pull (default 1000)
}

/**
 * Pull consumer metrics
 */
export interface PullConsumerMetrics {
  puller: {
    cursor: number;
    pullCount: number;
    eventsProcessed: number;
    gapsDetected: number;
    errors: number;
    lastPullLatencyMs: number;
    avgPullLatencyMs: number;
  };
  dispatcher: {
    eventsDispatched: number;
    eventsFiltered: number;
    eventsDeduplicated: number;
    activePartitions: number;
    totalPending: number;
    totalQueued: number;
  };
  backpressure: {
    state: 'normal' | 'slowdown' | 'paused';
    utilization: number;
  };
}
