/**
 * Global state management
 * Centralized state variables and synchronization functions
 */

import { Queue } from "bullmq";
import { taskRuntimeCache } from "../utils/task-runtime-cache";
import { debugLog, warnLog, errorLog } from "../utils/debug-file-logger";

// ============================================
// TYPE DEFINITIONS
// ============================================

export interface BidCounts {
  [key: string]: {
    opensea: number;
    magiceden: number;
    blur: number;
  };
}

export interface WalletBalances {
  [walletAddress: string]: {
    [contractAddress: string]: {
      opensea: number;
      magiceden: number;
      blur: number;
    };
  };
}

export interface BottomListings {
  [key: string]: {
    opensea: string[];
    magiceden: number[];
  }
}

export interface CurrentTraits {
  [key: string]: {
    opensea: Record<string, string[]>;
    magiceden: Record<string, string[]>;
    blur: Record<string, string[]>
  }
}

export type MarketplaceStatus = 'RUNNING' | 'WAITING' | 'CANCELLING' | 'SCHEDULED' | 'COMPLETED' | 'QUEUED' | 'QUEUEING' | 'STARTED' | "STOPPED" | "PROCESSING";

export interface MarketplaceTaskStatus {
  status: MarketplaceStatus;
  runningPercentage: number;
  cancellationPercentage: number;
  processingPercentage: number;
  totalJobs: number;
  completedJobs: number;
  nextLoopTime?: Date;
  waitingTime?: number;
  cancellationStartTime?: number;
  lastUpdate?: number;
  lastProgressUpdate?: number;
  completionTimestamp?: number;
  previousState?: MarketplaceStatus;
  expectedCancellationJobs?: number;
  remainingOrders?: number;
  lastCancellationCheck?: number;
  insufficientBalance?: boolean;
  balanceErrorDetails?: {
    wallet: string;
    requiredAmount: number;
    availableAmount: number;
    currency: string;
  };
}

export interface TaskStatus {
  [key: string]: {
    selectedMarketplaces: string[];
    lastUpdate?: number;
    marketplaces: {
      opensea?: MarketplaceTaskStatus;
      magiceden?: MarketplaceTaskStatus;
      blur?: MarketplaceTaskStatus;
    };
  };
}

export interface WarningBids {
  [key: string]: {
    opensea: boolean;
    magiceden: boolean;
    blur: boolean;
  };
}

export interface WarningMessage {
  [key: string]: {
    opensea: string;
    magiceden: string;
    blur: string;
  };
}

export interface CollectionFloorState {
  floorPrices: { opensea: number; blur: number; magiceden: number };
  bestOffers: { opensea: number; blur: number; magiceden: number };
  lastFetched: number;      // Unix timestamp
  isRefreshing: boolean;    // Prevents duplicate background fetches
}

// ============================================
// GLOBAL STATE VARIABLES
// ============================================

export const bidStats: BidCounts = {};
export const ownBidCounts: { [marketplace: string]: number } = {};
// Track processed own bid hashes - unbounded Set, cleared on task stop/restart
export const processedOwnBids = new Set<string>();

// Helper function to maintain addIfNotExists API compatibility
export function addOwnBidIfNotExists(orderHash: string): boolean {
  if (processedOwnBids.has(orderHash)) {
    return false; // Already exists
  }
  processedOwnBids.add(orderHash);
  return true; // Successfully added (new entry)
}

export const currentTokenIds: BottomListings = {};
export const currentTraits: CurrentTraits = {}
export const taskStatus: TaskStatus = {};
export const bestOffers: BidCounts = {};
export const floorPrices: BidCounts = {};
export const balances: WalletBalances = {}

// Track per-collection floor price state with refresh tracking for non-blocking /api/floor
export const collectionFloorCache = new Map<string, CollectionFloorState>();
export const FLOOR_STALE_THRESHOLD = 30000; // 30 seconds
export const warningBids: WarningBids = {};
export const warningMessage: WarningMessage = {};
export const errorStats: BidCounts = {};
export const skipStats: BidCounts = {};
export const counterbidCounts: BidCounts = {};

// Message flow tracking counters (for debugging WebSocket message loss)
export let rawWsMessagesReceived = 0;
export let rawOurBidsReceived = 0;
export let bidEventsReceived = 0;        // All BID_CREATED events received at WebSocket
export let bidEventsReachingHandler = 0; // Events that reach handleCounterBid
export let noMarketplaceMatchCounter = 0; // Messages that don't match any marketplace routing

// Filter reason tracking (for investigating message loss in handlers)
export const filteredCounters = {
  taskNotFound: 0,
  taskNotRunning: 0,
  openseaNotSelected: 0,
  openseaNotEnabled: 0,
  marketplaceStopped: 0,
  dedupBlocked: 0
};

// Track own bid deduplication (final safety net in incrementOwnBidCounter)
export const ownBidsDeduplicated: { [marketplace: string]: number } = {};

// Increment functions for message flow counters
export function incrementRawWsMessagesReceived(): void {
  rawWsMessagesReceived++;
}

export function incrementRawOurBidsReceived(): void {
  rawOurBidsReceived++;
}

export function incrementBidEventsReceived(): void {
  bidEventsReceived++;
}

export function incrementBidEventsReachingHandler(): void {
  bidEventsReachingHandler++;
}

export function incrementNoMarketplaceMatchCounter(): void {
  noMarketplaceMatchCounter++;
}

export function incrementFilteredCounter(reason: keyof typeof filteredCounters): void {
  filteredCounters[reason]++;
}

export function incrementOwnBidsDeduplicated(marketplace: string): void {
  if (!ownBidsDeduplicated[marketplace]) {
    ownBidsDeduplicated[marketplace] = 0;
  }
  ownBidsDeduplicated[marketplace]++;
}

// ============================================
// RESILIENT HELPER FUNCTIONS
// ============================================
// Helper functions with built-in retry logic for transient failures

/**
 * Initialize order cache with background retry
 * Non-blocking: task starts immediately, cache populates in background
 */
export async function initializeOrderCacheWithRetry(
  taskId: string,
  incrementOwnBidCounter: (marketplace: 'opensea' | 'magiceden' | 'blur', orderHash: string, skipDedup?: boolean) => void,
  maxRetries = 3
): Promise<void> {
  // NOTE: Counters track only new bids during current task execution (not cached orders)
  // This prevents initialization mismatch between bidStats and ownBidCounts
  // Both counters start at 0 for fresh task runs
  // No initialization needed - function intentionally empty
  return;
}

/**
 * Add jobs to BullMQ queue with retry logic for transient Redis failures
 */
export async function addBulkWithRetry(
  targetQueue: Queue,
  jobs: any[],
  queueName: string,
  taskId: string,
  maxRetries = 3
): Promise<void> {
  if (jobs.length === 0) return;

  const RETRYABLE_ERRORS = ['ECONNREFUSED', 'ECONNRESET', 'Connection lost', 'Connection is closed', 'Command timed out', 'READONLY'];

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      await targetQueue.addBulk(jobs);
      if (attempt > 1) {
        debugLog('Queue', `${queueName} addBulk succeeded on attempt ${attempt} for task ${taskId}`);
      }
      return;
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : String(err);
      const isRetryable = RETRYABLE_ERRORS.some(e => errorMsg.includes(e));

      if (isRetryable && attempt < maxRetries) {
        const delay = Math.min(1000 * Math.pow(2, attempt - 1), 5000);
        warnLog('Queue', `${queueName} addBulk failed for task ${taskId} (attempt ${attempt}/${maxRetries}), retrying in ${delay}ms: ${errorMsg}`);
        await new Promise(resolve => setTimeout(resolve, delay));
        continue;
      }

      errorLog('Queue', `${queueName} addBulk failed for task ${taskId} after ${attempt} attempts: ${errorMsg}`, err);
      throw err;
    }
  }
}

// ============================================
// TASK STATE REDIS PERSISTENCE
// ============================================

export async function syncTaskStateToRedis(taskId: string): Promise<void> {
  try {
    await taskRuntimeCache.setState(taskId, {
      bidStats: bidStats[taskId] || { opensea: 0, magiceden: 0, blur: 0 },
      skipStats: skipStats[taskId] || { opensea: 0, magiceden: 0, blur: 0 },
      errorStats: errorStats[taskId] || { opensea: 0, magiceden: 0, blur: 0 },
      floorPrices: floorPrices[taskId] || { opensea: 0, magiceden: 0, blur: 0 },
      bestOffers: bestOffers[taskId] || { opensea: 0, magiceden: 0, blur: 0 },
      warningBids: warningBids[taskId] || { opensea: false, magiceden: false, blur: false },
    });
  } catch (err) {
    debugLog('RuntimeState', `Failed to sync task ${taskId} to Redis: ${err instanceof Error ? err.message : err}`);
  }
}

export async function loadTaskStateFromRedis(taskId: string): Promise<void> {
  try {
    const data = await taskRuntimeCache.getState(taskId);

    if (data) {
      if (data.bidStats) bidStats[taskId] = { ...data.bidStats };
      if (data.skipStats) skipStats[taskId] = { ...data.skipStats };
      if (data.errorStats) errorStats[taskId] = { ...data.errorStats };
      if (data.floorPrices) floorPrices[taskId] = { ...data.floorPrices };
      if (data.bestOffers) bestOffers[taskId] = { ...data.bestOffers };
      if (data.warningBids) warningBids[taskId] = { ...data.warningBids };

      debugLog('RuntimeState', `Loaded state from Redis for task ${taskId}`);
    }
  } catch (err) {
    debugLog('RuntimeState', `Failed to load task ${taskId} from Redis: ${err instanceof Error ? err.message : err}`);
  }
}

export async function deleteTaskStateFromRedis(taskId: string): Promise<void> {
  try {
    await taskRuntimeCache.deleteState(taskId);
  } catch (err) {
    debugLog('RuntimeState', `Failed to delete task ${taskId} from Redis: ${err instanceof Error ? err.message : err}`);
  }
}

export async function syncAllActiveTaskStates(activeTasks: Map<string, any>): Promise<void> {
  const taskIds = Array.from(activeTasks.keys());
  if (taskIds.length === 0) return;

  let successCount = 0;
  const errors: string[] = [];

  await Promise.all(taskIds.map(async (taskId) => {
    try {
      await taskRuntimeCache.setState(taskId, {
        bidStats: bidStats[taskId] || { opensea: 0, magiceden: 0, blur: 0 },
        skipStats: skipStats[taskId] || { opensea: 0, magiceden: 0, blur: 0 },
        errorStats: errorStats[taskId] || { opensea: 0, magiceden: 0, blur: 0 },
        floorPrices: floorPrices[taskId] || { opensea: 0, magiceden: 0, blur: 0 },
        bestOffers: bestOffers[taskId] || { opensea: 0, magiceden: 0, blur: 0 },
        warningBids: warningBids[taskId] || { opensea: false, magiceden: false, blur: false },
      });
      successCount++;
    } catch (err) {
      errors.push(`${taskId}: ${err instanceof Error ? err.message : err}`);
    }
  }));

  if (successCount > 0) {
    debugLog('RuntimeState', `Synced ${successCount} task states to Redis`);
  }
  if (errors.length > 0) {
    warnLog('RuntimeState', `Failed to sync ${errors.length} task states: ${errors.slice(0, 3).join(', ')}${errors.length > 3 ? '...' : ''}`);
  }
}

