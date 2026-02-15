/**
 * Task Metrics Type Definitions
 *
 * Shared type definitions for task metrics, bid rates, and WebSocket updates.
 * These types are used across the codebase for type safety and consistency.
 */

// ============================================================================
// Type Definitions
// ============================================================================

/**
 * Task-specific update with delta changes
 */
export interface TaskUpdate {
  taskId: string;
  delta: Partial<TaskBidData>;
  timestamp: number;
}

/**
 * Global update containing bidRates and RPS
 */
export interface GlobalUpdate {
  bidRates: BidRates;
  rps: IRPS;
  timestamp: number;
}

/**
 * Heartbeat message to keep connections alive
 */
export interface HeartbeatMessage {
  timestamp: number;
  serverInstance?: string;
}

/**
 * Task bid data structure (complete state)
 */
export interface TaskBidData {
  bidCounts: { [marketplace: string]: number };
  skipCounts: { [marketplace: string]: number };
  errorCounts: { [marketplace: string]: number };
  floorPrices: { [marketplace: string]: number };
  bestOffers: { [marketplace: string]: number };
  warningBids: { [marketplace: string]: boolean };
  warningMessage: { [marketplace: string]: string };
  taskStatus: TaskStatus;
  ownBidCounts: { [marketplace: string]: number };
  loopCounterStats: { [marketplace: string]: number };
  counterbidCounts: { [marketplace: string]: number };
}

/**
 * Task status structure
 */
export interface TaskStatus {
  running: boolean;
  runningPercentage: number;
  cancellationPercentage: number;
  marketplaceStatus: {
    [marketplace: string]: {
      status: string;
      statusReason?: string;
    };
  };
}

/**
 * Bid rates structure
 */
export interface BidRates {
  [taskId: string]: {
    opensea: number;
    blur: number;
    magiceden: number;
  };
}

/**
 * RPS (Requests Per Second) structure
 */
export interface IRPS {
  [taskId: string]: number;
}
