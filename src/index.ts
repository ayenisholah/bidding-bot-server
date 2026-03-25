/* eslint-disable */
/**
 * JOB REMOVAL POLICY:
 * Jobs should ONLY be removed from the queue through:
 * 1. clearQueueAndRestartJobs() - for system restart/recovery (uses obliterate)
 * 2. stopTask() -> removeAllTaskJobsFromQueue() - for task management
 * 3. cancelListingJob() - for listing management (in listingScheduler.ts)
 * 
 * No other function should directly remove jobs from the queue.
 * This ensures proper cleanup and prevents race conditions.
 */
import express from "express";
import { config } from "dotenv";
import fs from 'fs';
import path from 'path';
import archiver from 'archiver';
import v8 from 'v8';

config()
console.log('[STARTUP] Environment configuration loaded');

import bodyParser from "body-parser";
import { clearTimeout } from 'timers';
import crypto from 'crypto';
import cors from "cors";
import helmet from "helmet";
import stringify from 'json-stable-stringify';
import WebSocket, { Server as WebSocketServer } from 'ws';
import http from 'http';
import os from 'os';
import { axiosInstance, initialize, limiter, removeTaskFromBlacklist, isTaskBlacklisted, getLimiterQueueDetails, cancelTaskJobs, abortPendingLimiterJobs } from "./init";
import { bidOnOpensea, cancelOpenseaRequest, cancelOrderWithoutTracking, fetchItemOffers, fetchOpenseaListings, fetchOpenseaOffers, IFee, OpenseaOfferData, openseaTraitFloor, resolveContractAddress } from "./marketplace/opensea";
import { bidOnBlur, cancelBlurBid, cancelBlurRequest, fetchBlurBid, fetchBlurCollectionStats, fetchBlurTraitFloor, getAccessToken, getAuthMetrics, getTimeoutMetrics, transferRestrictedCollections, authMetrics, timeoutMetrics } from "./marketplace/blur/bid";
import { bidOnMagiceden, cancelMagicEdenBid, cancelMagicedenRequest, fetchMagicEdenCollectionStats, fetchMagicEdenOffer, fetchMagicEdenTokens, fetchMagicEdenTraitFloor } from "./marketplace/magiceden";
import { CollectionItemsQueryVariables, fetchOpenSeaCollectionItems, fetchOpenseaTraits, fetchRarityTokens, getBlurFloor, getMagicEdenFloor, tokenHasTrait } from "./functions";
import mongoose from 'mongoose';
import Task, { ITask, SelectedTraits } from "./models/task.model";
import { Queue, Worker, Job, JobType, JobsOptions, QueueEvents, QueueOptions } from "bullmq";
import Wallet, { IWallet } from "./models/wallet.model";
import { createBullMQConnection, checkBullMQConnectionsHealth, closeAllBullMQConnections, getBullMQConnections } from "./utils/redis";
import { BASE_URL, ERC20_MIN_ABI } from "./constants";
import { Contract, ethers, Wallet as Web3Wallet, parseEther, MaxUint256 } from "ethers";
import { DistributedLockManager } from "./utils/lock";
import { TaskStateManager } from "./utils/task-state-manager";
import { safeGetJob, safeMarkJobAsProcessed } from "./utils/redis-job-fallback";
import { safeTokenIdToString } from './utils/tokenId';

import { logsCache, BidLogEntry } from './utils/logs-cache';
import { orderCache, CachedOrder } from "./utils/order-cache";
import { marketplaceApprovalCache } from "./utils/marketplace-approval-cache";
import { USDC, WETH } from "./utils/currencies";
import axios from "axios";
import { decryptPrivateKey, setPassword, getPassword, decryptPassword, recoverPasswordFromRedis, renewPasswordTTL, reEncryptWallet, clearDecryptedKeyCache, getDecryptedKeyCacheStats, clearDecryptedKey } from "./utils/crypto";
import { getTokenTraitCacheStats } from './utils/token-trait-cache';
import { getLogsCacheStats } from './utils/logs-cache';
import { getMarketplaceApprovalCacheStats } from './utils/marketplace-approval-cache';
import { logProcessStart, logProcessExit, logSignal, logLifecycleEvent, stopMemoryMonitor, getMemoryStats, getGCStats } from "./utils/process-lifecycle-logger";
import { startProactiveGC, stopProactiveGC, getProactiveGCStats } from "./utils/proactive-gc";
import { fetchWalletNFTs } from "./utils/nft";
import WebSocketManager from "./utils/websocket-manager";
import { initializeListingScheduler, shutdownListingScheduler, scheduleListing, cancelListingJob, processListingJob } from "./functions/listingScheduler";
import { initializeExpirationMonitor, stopExpirationMonitor } from "./functions/relistExpired";
import { initializeExpiredPriceCleanup, stopExpiredPriceCleanup } from "./functions/cleanupExpiredPrices";
import { handleListingPriceUpdate } from "./functions/priceMonitor";
import { createListing } from "./functions/list";
import { BullMQErrorFormatter } from "./utils/bullmq-error-formatter";
import { apiDedup } from "./utils/api-dedup";
import { tradingDisabledCache } from "./utils/trading-disabled-cache";
import { IListing } from "./types/listing.types";
import { LRUCache } from 'lru-cache';
import { addIfNotExists } from './utils/lru-helpers';
import { walletCache } from "./utils/wallet-cache";
import { getCachedCollectionDetails, getCachedFloorPrice, getCachedOpenseaBestOffer, getCachedBlurBestOffer, getCachedMagicEdenBestOffer } from "./utils/collection-cache";
import { convertBidDurationToHours } from "./utils/duration";
import { taskCircuitBreaker } from "./utils/task-circuit-breaker";
import { openseaFloorBreakerRegistry } from "./utils/circuit-breaker";
import { blurRateLimiter } from "./utils/blur-rate-limiter";
import { approvalCache } from "./utils/approval-cache";
import { blurCompletionTracker } from "./utils/blur-completion-tracker";
import { cancellationTracker } from "./utils/cancellation-tracker";
import { cancellationDebugLogger } from "./utils/cancellation-debug-logger";
import { errorLog, warnLog, getAllCategories, type LogCategory } from "./utils/debug-file-logger";
import { clearUnavailabilityCache } from "./utils/marketplace-availability";
import { blurAuthBlockLRU, blurWalletPauseLRU, blurAccessTokenLRU } from './marketplace/blur/cache';
import { addressBalancesLRU } from './marketplace/opensea/cache';

import { listingPriceCache } from "./utils/listing-price-cache";
import Listing from "./models/listing.model";
import { MarketplaceBidRateTracker } from "./utils/metrics-rate-tracker";
import { BroadcastManager } from "./utils/broadcast-manager";
import { memorySnapshotService } from "./services/memory-snapshot.service";
import { memoryBreakdownService } from "./services/memory-breakdown.service";
import { memoryCleanupService } from "./services/memory-cleanup.service";
import { collectionDataWarmer } from "./services/collection-data-warmer.service";
import { recordForwarding, getForwardingStats, initForwardingLatencyTracker, shutdownForwardingLatencyTracker } from "./services/forwarding-latency-tracker";
import { recordExecution as recordCounterbidExecution, getAllExecutionStats, initExecutionLatencyTracker, shutdownExecutionLatencyTracker } from "./services/execution-latency-tracker";
// REMOVED: import { concurrencyManager } from "./services/concurrency-manager"; // No longer needed with single queue
import { startPeriodicSnapshots, stopPeriodicSnapshots, captureLatencySnapshot } from "./services/latency-snapshot.service";
import { LatencySnapshot } from "./models/latency-snapshot.model";

// Global type declarations
declare global {
  var cancellationTimeouts: Map<string, NodeJS.Timeout> | undefined;
}

// Refactored modules - utilities and state management
import { loadOurWalletAddresses, isOurWallet, addOurWalletAddress, removeOurWalletAddress } from "./utils/wallet-tracking";
import { getTaskIdForLog, maskSensitiveIdentifier, extractDetailedOpenSeaError } from "./utils/task-helpers";
import {
  handleInsufficientBalanceError,
  shouldLogBalanceError,
  parseOpenseaBalanceError,
  getMarketplacesForCurrency,
  getTokenAddressForCurrency
} from "./utils/balance-helpers";
import {
  bidStats,
  ownBidCounts,
  processedOwnBids,
  addOwnBidIfNotExists,
  getProcessedOwnBidsStats,
  currentTokenIds,
  currentTraits,
  taskStatus,
  bestOffers,
  floorPrices,
  collectionFloorCache,
  FLOOR_STALE_THRESHOLD,
  warningBids,
  warningMessage,
  errorStats,
  skipStats,
  counterbidCounts,
  rawWsMessagesReceived,
  rawOurBidsReceived,
  bidEventsReceived,
  bidEventsReachingHandler,
  noMarketplaceMatchCounter,
  filteredCounters,
  ownBidsDeduplicated,
  incrementRawWsMessagesReceived,
  incrementRawOurBidsReceived,
  incrementBidEventsReceived,
  incrementBidEventsReachingHandler,
  incrementNoMarketplaceMatchCounter,
  incrementFilteredCounter,
  incrementOwnBidsDeduplicated,
  type BidCounts,
  type MarketplaceStatus
} from "./state/global-state";

// WebSocket URL - read from environment variable with fallback to production default
const MARKETPLACE_WS_URL = process.env.MARKETPLACE_WS_URL || "wss://nfttools-ws-proxy.nfttools.io";


// Initialize process lifecycle logging immediately on startup
logProcessStart();

// Start proactive GC monitoring
startProactiveGC();

// Initialize latency trackers
initForwardingLatencyTracker();
initExecutionLatencyTracker();

export const RATE_LIMIT = Number(process.env.RATE_LIMIT)
export const API_KEY = process.env.API_KEY;

// Validate API_KEY is loaded from environment
if (!API_KEY) {
  errorLog('Startup', 'API_KEY environment variable is not set! Check .env file.');
  throw new Error('API_KEY is required but not found in environment variables');
}
if (process.env.NODE_ENV === 'development') {
  console.log(`[STARTUP] API_KEY loaded successfully (${API_KEY.substring(0, 8)}...)`);
}

// Debug configuration - single variable controls all debug logging
// Replaces: DEBUG_SCHEDULED_BID, DEBUG_TOP_BIDS, DEBUG_JOB_KEYS, DEBUG_WETH_BALANCE, DEBUG_AXIOS_RETRY
const DEBUG = process.env.DEBUG === 'true';

// NOTE: Global state variables (bidStats, taskStatus, etc.) are now imported from ./state/global-state
// NOTE: Helper functions (initializeOrderCacheWithRetry, addBulkWithRetry, sync functions) are now imported from ./state/global-state

// Task status history tracking
interface TaskStatusChange {
  timestamp: string;
  from: string;
  to: string;
  context?: string;
  marketplace?: string;
  stack?: string;
}
const taskStatusHistory: { [taskId: string]: TaskStatusChange[] } = {};

// Message idempotency tracking

// WebSocket message deduplication cache with automatic TTL
// Expanded to 1M max for higher traffic capacity and burst handling
const processedMessages = new LRUCache<string, number>({
  max: 1000000,      // 1M entries for production-scale traffic bursts
  ttl: 60 * 1000,    // 1 minute TTL (shorter than original 5min for better memory usage)
  updateAgeOnGet: false,  // Don't reset TTL on access (we just need deduplication window)
});

// Blur rate limiting is now handled by BlurRateLimiter class
// Hash tags ensure all keys for each queue land in the same hash slot (required for Redis Cluster atomic operations)
// Different prefixes distribute queues across cluster nodes for better performance
const QUEUE_NAME = '{main-queue}BIDDING_BOT';
// REMOVED: const COUNTERBID_QUEUE_NAME = '{counterbid-queue}COUNTERBID_QUEUE'; // No longer needed with single queue

// Type definitions (duplicates removed - now imported from global-state.ts)
interface LoopCount {
  [taskId: string]: {
    [marketplace: string]: number;
  };
}

let traitsToRemove: { [key: string]: string[] } = {}
let removedTokens: { [key: string]: { opensea: string[]; magiceden: string[] } } = {}

// =============================================================================
// DEDUPLICATION ARCHITECTURE
// =============================================================================
// Multi-layer deduplication prevents duplicate processing and API calls:
//
// Layer 1: ws-proxy global dedup (60s TTL) - prevents duplicates across 3 OpenSea2 connections
// Layer 2: ws-proxy per-client dedup (10min TTL) - prevents duplicate sends to same client
// Layer 3: seenOrderHashes (60s TTL) - prevents duplicate processing within task handlers
// Layer 4: processedOwnBids (60s TTL) - prevents duplicate own bid counting
//
// Note: seenOrderHashes TTL reduced from 5min to 60s to align with ws-proxy Layer 1 (no redundancy)
// =============================================================================

// Use LRUCache with TTL for memory-efficient tracking
// Increased to 1M max for production scale (handle extreme burst scenarios)
const seenOrderHashes = new LRUCache<string, boolean>({
  max: 1000000,                  // Increased to 1M for extreme burst traffic handling
  ttl: 60 * 1000,                // 60-second TTL (aligned with ws-proxy)
  updateAgeOnGet: false,          // Don't reset TTL on reads
  allowStale: false,              // Never return expired entries
});

// Unified error log cache for deduplication - prevents log flooding from repeated errors
// Key format: "{errorType}-{taskId}-{jobId}" or similar → timestamp
const errorLogCache = new LRUCache<string, boolean>({
  max: 5000,
  ttl: 5000 // 5 seconds
});

// Collection stats endpoint cache - stores complete API response
// Eliminates Redis roundtrip (50-200ms) → <1ms LRU access
const collectionStatsLRU = new LRUCache<string, any>({
  max: 1000,                        // Max 1000 different collections
  ttl: 24 * 60 * 60 * 1000,        // 24 hours TTL (traits rarely change)
  updateAgeOnGet: false,
  allowStale: false
});

// Update rate limiting cache - tracks update endpoint requests per IP
interface RateLimitEntry {
  count: number;
  resetAt: number;
}
const updateRateLimitLRU = new LRUCache<string, RateLimitEntry>({
  max: 10000,
  ttl: 60 * 60 * 1000, // 1 hour
  allowStale: false,
  updateAgeOnGet: false
});

// Collection data endpoint cache - stores complete API response from /api/collection
// 24 hour TTL - collection metadata rarely changes
// Memory: ~50KB per collection × 1000 max = ~50MB max (standardized minimum)
const collectionDataLRU = new LRUCache<string, any>({
  max: 1000,                 // Max 1000 different collections (standardized minimum)
  ttl: 24 * 60 * 60 * 1000,  // 24 hours TTL
  updateAgeOnGet: false,     // Don't update age on cache reads
  allowStale: false          // Don't serve stale entries
});

// =============================================================================
// OUR WALLET ADDRESS TRACKING
// =============================================================================
// In-memory Set of our wallet addresses (lowercase) for fast O(1) lookup
// Used to detect and skip counterbids against our own bids
// NOTE: Wallet tracking functions (loadOurWalletAddresses, addOurWalletAddress, removeOurWalletAddress, isOurWallet) are now imported from ./utils/wallet-tracking

// Helper functions for seen items management
function addSeenItem(cache: LRUCache<string, number>, key: string): void {
  const now = Date.now();
  cache.set(key, now);
  // LRUCache handles cleanup automatically
}

// Note: Manual .delete() calls after queuing counterbids are intentional
// They allow immediate re-processing of the same order/bid if needed
// The TTL system acts as a safety net for any items not manually deleted

/**
 * Atomic check-and-add for deduplication: returns true if this is a NEW item (not seen before),
 * false if it was already seen. This prevents race conditions in concurrent handlers.
 */
function markSeenIfNew(cache: LRUCache<string, boolean>, key: string): boolean {
  const exists = cache.get(key);
  if (exists) return false;  // Already seen
  cache.set(key, true);
  return true;  // New item
}

/**
 * Check if a marketplace is stopped due to insufficient balance.
 * Used to skip processing queued jobs after balance error is detected.
 */
// NOTE: Balance helper functions (handleInsufficientBalanceError, shouldLogBalanceError) are now imported from ./utils/balance-helpers
// Balance errors are logged but do NOT stop the marketplace - they're treated as normal bid errors

// Set up periodic intervals
let expirationMonitorInterval: NodeJS.Timeout | null = null;
let expiredPriceCleanupInterval: NodeJS.Timeout | null = null;
let validateBidCountInterval: NodeJS.Timeout | null = null;

const SERVER_IP = process.env.SERVER_IP as string
const USERNAME = process.env.USERNAME as string
const allowedOrigins = [
  'http://localhost:3001',
  'https://localhost:3001',
  `http://${SERVER_IP}:3001`,
  `https://${SERVER_IP}:3001`,
  `https://${USERNAME}.nfttools.io`,
  `https://${USERNAME}.nfttools.io`
];

const SEAPORT = '0x1e0049783f008a0085193e00003d00cd54003c71';

const TOKEN_BID_PRIORITY = {
  OPENSEA: 2,
  MAGICEDEN: 2,
  BLUR: 2
};

const TRAIT_BID_PRIORITY = {
  OPENSEA: 2,
  MAGICEDEN: 2,
  BLUR: 2
};



const lockManager = new DistributedLockManager({
  lockPrefix: 'marketplace:',
  defaultTTLSeconds: 60
});

// Create TaskStateManager instance for thread-safe state management
const taskStateManager = new TaskStateManager(lockManager);

// Create a getter for redis to ensure it's always current

// For internal use

// Export redis as a getter property
export { lockManager };
// queue is already exported via "export let queue: Queue" above
export { listingPriceCache };
// DEPRECATED: Redis getter removed - Redis is BullMQ-only
// Removed netEnabled - using isConnected from connectivity monitoring instead

// Helper function to extract taskId from various contexts for logging
// NOTE: Task helper functions (getTaskIdForLog, parseOpenSeaResponseData, maskSensitiveIdentifier, extractDetailedOpenSeaError) are now imported from ./utils/task-helpers

class RateLimiter {
  private tokens: number;
  private lastRefill: number;
  private readonly rate: number;

  constructor(rate: number) {
    this.tokens = rate;
    this.lastRefill = Date.now();
    this.rate = rate;
  }

  async acquire(): Promise<void> {
    while (this.tokens <= 0) {
      const now = Date.now();
      const timePassed = now - this.lastRefill;
      const refillAmount = Math.floor(timePassed * (this.rate / 1000));

      if (refillAmount > 0) {
        this.tokens = Math.min(this.rate, this.tokens + refillAmount);
        this.lastRefill = now;
      }

      if (this.tokens <= 0) {
        await new Promise(resolve => setTimeout(resolve, 10)); // 10ms granularity for smoother token refilling (was 100ms)
      }
    }
    this.tokens--;
  }
}


// Initialize workers later in startServer function (single-queue architecture with 4 workers)
let workers: Worker[] = [];
const NUM_WORKERS = 4; // Total worker instances

// Track all BullMQ connections for proper cleanup
let bullMQConnections: any[] = [];

const taskTimeouts = new Map<string, NodeJS.Timeout>();
const taskCountdownTimers = new Map<string, NodeJS.Timeout>();
// Track timer execution IDs to detect cancelled timers (cancellation token pattern)
const timerExecutionIds = new Map<string, string>();
// Track consecutive no-progress calls for infinite loop prevention
// Removed cancellationNoProgressCount - now using cancellationTracker
const MAX_NO_PROGRESS_CALLS = 10;

// Add a Map to track task locks
const taskLocks = new Map<string, TaskLock>();
// Add a Map to track loop scheduling locks with timestamps
const loopSchedulingLocks = new Map<string, { locked: boolean; timestamp: number }>();
const LOCK_TIMEOUT_MS = 5 * 60 * 1000; // 5 minutes

// Add a Map to track tasks being scheduled to prevent duplicates
const tasksBeingScheduled = new Map<string, boolean>();
// Track tasks being restarted to prevent duplicate restart attempts
const tasksBeingRestarted = new Set<string>();
// Track tasks being cancelled - set synchronously at START of stopTask() for race-free checks
const tasksBeingCancelled = new Set<string>();

// Track task restart counts to prevent infinite restart loops
const taskRestartCounts = new Map<string, { count: number; lastRestart: number }>();
const MAX_RESTARTS_PER_HOUR = 3;

// Track active cancellation operations
interface CancellationOperation {
  taskId: string;
  marketplace?: string;
  totalJobs: number;
  completedJobs: number;
  startTime: number;
}

const activeCancellationOperations = new Map<string, CancellationOperation>();

// Track orders that successfully cancelled on-chain (API confirmed)
// Key: taskId, Value: Set of orderHashes that confirmed cancellation
const successfullyCancelledOrders = new Map<string, Set<string>>();

// Track active Blur jobs per wallet to prevent concurrent processing
// Blur job completion tracking is now handled by blurCompletionTracker

function unlockTask(taskId: string): void {
  const currentLock = taskLocks.get(taskId);
  if (currentLock) {
    currentLock.locked = false;
  }
}

// Helper function to acquire loop scheduling lock
function acquireLoopSchedulingLock(taskId: string): boolean {
  const existingLock = loopSchedulingLocks.get(taskId);
  const now = Date.now();

  // Check if lock exists and is not stale
  if (existingLock && existingLock.locked) {
    // Check if lock is stale (older than 5 minutes)
    if (now - existingLock.timestamp > LOCK_TIMEOUT_MS) {
      loopSchedulingLocks.delete(taskId);
    } else {
      return false;
    }
  }

  loopSchedulingLocks.set(taskId, { locked: true, timestamp: now });
  return true;
}

// Helper function to release loop scheduling lock
function releaseLoopSchedulingLock(taskId: string): void {
  if (loopSchedulingLocks.has(taskId)) {
    loopSchedulingLocks.delete(taskId);
  }
}

// Helper function to acquire scheduled bid lock with stale detection
function acquireScheduledBidLock(taskId: string, marketplace: string): void {
  const lockKey = `${taskId}-${marketplace}-scheduled`;
  const existingLock = taskLocks.get(lockKey);
  const now = Date.now();

  if (existingLock && existingLock.locked) {
    // Check if lock is stale (expired)
    if (existingLock.lockUntil && now > existingLock.lockUntil) {
      // Throw error for stale lock - let retry policy handle it
      const staleTime = now - existingLock.lockUntil;
      throw new Error(`Stale lock detected for task ${taskId} in ${marketplace} scheduled bid. Lock expired ${staleTime}ms ago`);
    }
    // Lock is held by another process - throw error to skip
    throw new Error(`Task ${taskId} is already being processed by ${marketplace} scheduled bid function`);
  }

  // Acquire lock for 15 minutes (matching job timeout)
  taskLocks.set(lockKey, {
    locked: true,
    lockUntil: now + (15 * 60 * 1000), // 15 minutes
    lastReleaseTime: undefined
  });
}

// Helper function to extend scheduled bid lock during long operations
function extendScheduledBidLock(taskId: string, marketplace: string, extensionMs: number = 5 * 60 * 1000): boolean {
  const lockKey = `${taskId}-${marketplace}-scheduled`;
  const lock = taskLocks.get(lockKey);

  if (!lock || !lock.locked) {
    return false;
  }

  const now = Date.now();

  // Only extend if lock is about to expire (within 2 minutes)
  if (lock.lockUntil && (lock.lockUntil - now) < (2 * 60 * 1000)) {
    lock.lockUntil = now + extensionMs;
    return true;
  }

  return false;
}

// Helper function to release scheduled bid lock
function releaseScheduledBidLock(taskId: string, marketplace: string): void {
  const lockKey = `${taskId}-${marketplace}-scheduled`;
  const lock = taskLocks.get(lockKey);
  if (lock) {
    lock.lastReleaseTime = Date.now();
    taskLocks.delete(lockKey);
  }
}

// Improved lock manager with automatic timeout
class TimedLockManager {
  private locks = new Map<string, {
    acquired: number;
    timeout: NodeJS.Timeout;
    marketplace: string;
    warningTimeouts?: { fifty: NodeJS.Timeout; seventyFive: NodeJS.Timeout };
  }>();
  private readonly MAX_LOCK_DURATION = 300000; // Default 5 minutes

  acquireLock(taskId: string, marketplace: string): boolean {
    const lockKey = `${taskId}-${marketplace}-scheduled`;

    // Check if lock exists
    const existingLock = this.locks.get(lockKey);
    if (existingLock) {
      const elapsed = Date.now() - existingLock.acquired;
      if (elapsed > this.MAX_LOCK_DURATION) {
        this.releaseLock(taskId, marketplace);
      } else {
        // Lock is still valid
        return false;
      }
    }

    const now = Date.now();

    // Set up warning timeouts at 50% and 75% of lock duration
    const fiftyPercentWarning = setTimeout(() => {
    }, this.MAX_LOCK_DURATION * 0.5);

    const seventyFivePercentWarning = setTimeout(() => {
    }, this.MAX_LOCK_DURATION * 0.75);

    // Acquire new lock with auto-release timeout
    const timeout = setTimeout(() => {
      // Wrap everything in a try-catch to ensure we see any errors
      try {
        this.releaseLock(taskId, marketplace);

        // When a lock expires, it means the scheduled bid processor got stuck or failed
        // This causes tasks to get stuck at 0% running with parent jobs expecting children
        // Extract taskId from lockKey (format: taskId-marketplace-scheduled) and trigger restart
        const lockParts = lockKey.split('-');
        if (lockParts.length >= 1) {
          const extractedTaskId = lockParts[0];

          // Use setImmediate to ensure async execution
          setImmediate(async () => {
            try {
              await triggerTaskRestart(extractedTaskId);
            } catch (error) {
              errorLog('TimedLockManager', `Failed to trigger task restart for task ${extractedTaskId}`, error);
            }
          });
        }
      } catch (error) {
        errorLog('TimedLockManager', 'Error in timeout handler', error);
      }
    }, this.MAX_LOCK_DURATION);

    this.locks.set(lockKey, {
      acquired: now,
      timeout,
      marketplace,
      warningTimeouts: {
        fifty: fiftyPercentWarning,
        seventyFive: seventyFivePercentWarning
      }
    });

    return true;
  }

  releaseLock(taskId: string, marketplace: string): void {
    const lockKey = `${taskId}-${marketplace}-scheduled`;
    const lock = this.locks.get(lockKey);

    if (lock) {
      clearTimeout(lock.timeout);
      // Clear warning timeouts if they exist
      if (lock.warningTimeouts) {
        clearTimeout(lock.warningTimeouts.fifty);
        clearTimeout(lock.warningTimeouts.seventyFive);
      }
      this.locks.delete(lockKey);
      const heldDuration = Date.now() - lock.acquired;
    }
  }

  // Extend an existing lock by resetting its timeouts
  extendLock(taskId: string, marketplace: string): boolean {
    const lockKey = `${taskId}-${marketplace}-scheduled`;
    const lock = this.locks.get(lockKey);

    if (!lock) {
      return false;
    }

    const elapsed = Date.now() - lock.acquired;
    const percentComplete = (elapsed / this.MAX_LOCK_DURATION) * 100;

    // Clear existing timeouts
    clearTimeout(lock.timeout);
    if (lock.warningTimeouts) {
      clearTimeout(lock.warningTimeouts.fifty);
      clearTimeout(lock.warningTimeouts.seventyFive);
    }


    // Set up new warning timeouts based on elapsed time
    const remainingTime = this.MAX_LOCK_DURATION - elapsed;

    // Only set up warning timeouts for future events
    if (elapsed < this.MAX_LOCK_DURATION * 0.5) {
      lock.warningTimeouts!.fifty = setTimeout(() => {
      }, this.MAX_LOCK_DURATION * 0.5 - elapsed);
    }

    if (elapsed < this.MAX_LOCK_DURATION * 0.75) {
      lock.warningTimeouts!.seventyFive = setTimeout(() => {
      }, this.MAX_LOCK_DURATION * 0.75 - elapsed);
    }

    // Set up new timeout for full duration from original acquisition time
    lock.timeout = setTimeout(() => {
      try {
        this.releaseLock(taskId, marketplace);

        const lockParts = lockKey.split('-');
        if (lockParts.length >= 1) {
          const extractedTaskId = lockParts[0];

          setImmediate(async () => {
            try {
              await triggerTaskRestart(extractedTaskId);
            } catch (error) {
              errorLog('TimedLockManager', `Failed to trigger task restart for task ${extractedTaskId}`, error);
            }
          });
        }
      } catch (error) {
        errorLog('TimedLockManager', 'Error in extended timeout handler', error);
      }
    }, this.MAX_LOCK_DURATION);

    return true;
  }

  // Check if a lock is held
  isLocked(taskId: string, marketplace: string): boolean {
    const lockKey = `${taskId}-${marketplace}-scheduled`;
    const lock = this.locks.get(lockKey);

    if (!lock) return false;

    const elapsed = Date.now() - lock.acquired;
    if (elapsed > this.MAX_LOCK_DURATION) {
      // Auto-release stale lock
      this.releaseLock(taskId, marketplace);
      return false;
    }

    return true;
  }
}

// Create global instance
const timedLockManager = new TimedLockManager();

// Task Status Manager for handling status transitions
class TaskStatusManager {
  private statusTimeouts = new Map<string, NodeJS.Timeout>();

  // Ensure status progresses from STARTED to RUNNING for a specific marketplace
  ensureStatusProgression(taskId: string, marketplace: string, fromStatus: string, toStatus: string, timeoutMs: number = 10000) {
    const key = `${taskId}-${marketplace}-${fromStatus}-${toStatus}`;

    // Clear existing timeout if any
    this.clearStatusTimeout(key);

    // Set new timeout for status progression
    const timeout = setTimeout(() => {
      const taskState = taskStatus[taskId];
      if (!taskState || !taskState.marketplaces) return;

      const marketplaceStatus = taskState.marketplaces[marketplace as 'opensea' | 'magiceden' | 'blur'];
      if (!marketplaceStatus) return;

      if (marketplaceStatus.status === fromStatus) {

        // Force status transition for this specific marketplace
        const transitioned = setTaskStatus(taskId, toStatus, 'forced progression timeout', marketplace);

        if (!transitioned) {
          errorLog('StatusManager', `Failed to transition task ${taskId}/${marketplace} from ${fromStatus} to ${toStatus}`, undefined);

          // If it's stuck in STARTED, we may need to check parent jobs for this marketplace
          if (fromStatus === 'STARTED' && toStatus === 'RUNNING') {
            this.checkAndFixStuckStartedTask(taskId, marketplace);
          }
        }
      }
    }, timeoutMs);

    this.statusTimeouts.set(key, timeout);
  }

  clearStatusTimeout(key: string) {
    const timeout = this.statusTimeouts.get(key);
    if (timeout) {
      clearTimeout(timeout);
      this.statusTimeouts.delete(key);
    }
  }

  // Fix tasks stuck in STARTED status for a specific marketplace
  async checkAndFixStuckStartedTask(taskId: string, marketplace: string) {

    const task = activeTasks.get(taskId);
    if (!task) {
      return;
    }

    // Check if parent job exists for this marketplace
    const parentJobId = getParentJobId(taskId, marketplace);
    const parentJob = parentJobs.get(parentJobId);
    const hasParentJob = !!parentJob;
    const totalJobs = parentJob?.totalJobs || 0;

    if (hasParentJob && totalJobs > 0) {
      // Parent job exists for this marketplace, safe to transition to RUNNING
      // Recovered from stuck STARTED - will transition to RUNNING when jobs are processed
    } else {
      // No parent job found for this marketplace, may need to restart

      // Set to SCHEDULED to trigger next loop for this marketplace
      setTaskStatus(taskId, 'SCHEDULED', 'no jobs found, scheduling next loop', marketplace);
      await scheduleNextLoop(taskId, marketplace);
    }
  }

  // Clear all timeouts for a task
  clearTaskTimeouts(taskId: string) {
    for (const [key, timeout] of this.statusTimeouts.entries()) {
      if (key.startsWith(taskId)) {
        clearTimeout(timeout);
        this.statusTimeouts.delete(key);
      }
    }
  }
}

// Create global instance for status progression monitoring
const statusProgressionManager = new TaskStatusManager();

// Initialize queue and queueEvents with Redis connection
// These will be initialized in startServer()
export let queue: Queue;
let queueEvents: QueueEvents;
let QUEUE_OPTIONS: QueueOptions;

// DEPRECATED: Redis connection removed - Redis is BullMQ-only
// All non-BullMQ Redis usage migrated to in-memory LRU caches

export const MAGENTA = '\x1b[35m';
export const BLUE = '\x1b[34m';
export const RESET = '\x1b[0m';
export const GREEN = '\x1b[32m';
export const YELLOW = '\x1b[33m';
export const RED = '\x1b[31m';
export const GOLD = '\x1b[33m';

const OPENSEA = "OPENSEA";
const MAGICEDEN = "MAGICEDEN";
const BLUR = "BLUR";

export const currentTasks: ITask[] = [];

// Timeout tracking for monitoring
const timeoutStats = {
  count: 0,
  lastHour: [] as { timestamp: number; jobId: string }[],
  byJobType: {} as Record<string, number>
};

// Task indexing for O(1) lookup performance
const contractToTasksMap = new Map<string, Set<ITask>>();
const slugToTasksMap = new Map<string, Set<ITask>>();

// Update indexes when tasks change
function updateTaskIndexes(tasks: ITask[]): void {
  contractToTasksMap.clear();
  slugToTasksMap.clear();

  tasks.forEach(task => {
    if (!task.running || !task.contract) return;

    const contractKey = task.contract.contractAddress?.toLowerCase();
    const slugKey = task.contract.slug?.toLowerCase();

    if (contractKey) {
      if (!contractToTasksMap.has(contractKey)) {
        contractToTasksMap.set(contractKey, new Set());
      }
      contractToTasksMap.get(contractKey)!.add(task);
    }

    if (slugKey) {
      if (!slugToTasksMap.has(slugKey)) {
        slugToTasksMap.set(slugKey, new Set());
      }
      slugToTasksMap.get(slugKey)!.add(task);
    }
  });
}

// Fast task lookup - O(1) instead of O(n)
function getRelevantTasks(contractAddress?: string, slug?: string): ITask[] {
  const tasks = new Set<ITask>();

  if (contractAddress) {
    const contractTasks = contractToTasksMap.get(contractAddress.toLowerCase());
    if (contractTasks) {
      contractTasks.forEach(task => tasks.add(task));
    }
  }

  if (slug) {
    const slugTasks = slugToTasksMap.get(slug.toLowerCase());
    if (slugTasks) {
      slugTasks.forEach(task => tasks.add(task));
    }
  }
  return Array.from(tasks);
}

// Add a single task to the indexes
function addTaskToIndexes(task: ITask): void {
  if (!task.running || !task.contract) return;

  const taskId = task._id.toString();
  const contractKey = task.contract.contractAddress?.toLowerCase();
  const slugKey = task.contract.slug?.toLowerCase();

  if (contractKey) {
    if (!contractToTasksMap.has(contractKey)) {
      contractToTasksMap.set(contractKey, new Set());
    }
    contractToTasksMap.get(contractKey)!.add(task);
  }

  if (slugKey) {
    if (!slugToTasksMap.has(slugKey)) {
      slugToTasksMap.set(slugKey, new Set());
    }
    slugToTasksMap.get(slugKey)!.add(task);
  }

}

// Remove a single task from the indexes
// Note: Uses taskId matching instead of reference equality because task objects
// are recreated via spread operator on start/stop, making Set.delete(task) fail silently
function removeTaskFromIndexes(task: ITask): void {
  const taskId = task._id.toString();
  const contractKey = task.contract?.contractAddress?.toLowerCase();
  const slugKey = task.contract?.slug?.toLowerCase();

  let removedFromContract = false;
  let removedFromSlug = false;

  if (contractKey && contractToTasksMap.has(contractKey)) {
    const contractTasks = contractToTasksMap.get(contractKey)!;
    // Find task by ID instead of reference (Set.delete uses reference equality)
    for (const t of contractTasks) {
      if (t._id.toString() === taskId) {
        contractTasks.delete(t);
        removedFromContract = true;
        break;
      }
    }
    if (contractTasks.size === 0) {
      contractToTasksMap.delete(contractKey);
    }
  }

  if (slugKey && slugToTasksMap.has(slugKey)) {
    const slugTasks = slugToTasksMap.get(slugKey)!;
    // Find task by ID instead of reference (Set.delete uses reference equality)
    for (const t of slugTasks) {
      if (t._id.toString() === taskId) {
        slugTasks.delete(t);
        removedFromSlug = true;
        break;
      }
    }
    if (slugTasks.size === 0) {
      slugToTasksMap.delete(slugKey);
    }
  }

}

// DEPRECATED: SCHEDULE job constants - kept only for cleaning up legacy jobs in queue
// Direct timer execution via executeMarketplaceBidLoop() is now used instead
// TODO: Remove these constants after all legacy SCHEDULE jobs have been drained from the queue
export const OPENSEA_SCHEDULE = "OPENSEA_SCHEDULE"
export const BLUR_SCHEDULE = "BLUR_SCHEDULE"
export const MAGICEDEN_SCHEDULE = "MAGICEDEN_SCHEDULE"
export const OPENSEA_COLLECTION_BID = "OPENSEA_COLLECTION_BID"
export const OPENSEA_TOKEN_BID = "OPENSEA_TOKEN_BID"
export const OPENSEA_TRAIT_BID = "OPENSEA_TRAIT_BID"
export const MAGICEDEN_COLLECTION_BID = "MAGICEDEN_COLLECTION_BID"
export const MAGICEDEN_TOKEN_BID = "MAGICEDEN_TOKEN_BID"
export const MAGICEDEN_TRAIT_BID = "MAGICEDEN_TRAIT_BID"
export const OPENSEA_TOKEN_BID_COUNTERBID = "OPENSEA_TOKEN_BID_COUNTERBID"
export const OPENSEA_TRAIT_BID_COUNTERBID = "OPENSEA_TRAIT_BID_COUNTERBID"
export const OPENSEA_COLLECTION_BID_COUNTERBID = "OPENSEA_COLLECTION_BID_COUNTERBID"
export const BLUR_COLLECTION_BID = "BLUR_COLLECTION_BID"
export const BLUR_TRAIT_BID = "BLUR_TRAIT_BID"
export const BLUR_COLLECTION_BID_COUNTERBID = "BLUR_COLLECTION_BID_COUNTERBID"
export const BLUR_TRAIT_BID_COUNTERBID = "BLUR_TRAIT_BID_COUNTERBID"
export const PROCESS_LISTING = "PROCESS_LISTING"
export const PROCESS_LISTING_UPDATE = "PROCESS_LISTING_UPDATE"
export const MAGICEDEN_COLLECTION_BID_COUNTERBID = "MAGICEDEN_COLLECTION_BID_COUNTERBID"
export const MAGICEDEN_TOKEN_BID_COUNTERBID = "MAGICEDEN_TOKEN_BID_COUNTERBID"
export const MAGICEDEN_TRAIT_BID_COUNTERBID = "MAGICEDEN_TRAIT_BID_COUNTERBID"
export const CANCEL_OPENSEA_BID = "CANCEL_OPENSEA_BID"
const CANCEL_OPENSEA_COUNTERBID = "CANCEL_OPENSEA_COUNTERBID"
const CANCEL_OPENSEA_OUTBID = "CANCEL_OPENSEA_OUTBID"
export const CANCEL_MAGICEDEN_BID = "CANCEL_MAGICEDEN_BID"
export const CANCEL_BLUR_BID = "CANCEL_BLUR_BID"

// Retry configuration for cancellation jobs
const CANCELLATION_JOB_RETRY_ATTEMPTS = 3; // 3 attempts total (1 initial + 2 retries)
const CANCELLATION_JOB_RETRY_DELAY = 2000;
// Helper function to create cancellation job options with retry configuration
function getCancellationJobOptions(taskId: string, orderId: string, jobType: string) {
  // Only CANCEL_OPENSEA_COUNTERBID and CANCEL_OPENSEA_OUTBID get priority 1
  // All other cancellations (CANCEL_OPENSEA_BID, CANCEL_MAGICEDEN_BID, CANCEL_BLUR_BID) keep priority 0
  const shouldDeprioritize = jobType === CANCEL_OPENSEA_COUNTERBID || jobType === CANCEL_OPENSEA_OUTBID;

  return {
    jobId: `${jobType}-${taskId}-${orderId}`,
    ...(shouldDeprioritize && { priority: 1 }),  // Only add priority if it's counterbid/outbid cancellation
    attempts: CANCELLATION_JOB_RETRY_ATTEMPTS, // 3 attempts with exponential backoff
    backoff: {
      type: 'exponential' as const,
      delay: CANCELLATION_JOB_RETRY_DELAY,
    },
    removeOnFail: true, // Keep failed jobs for inspection
    removeOnComplete: true
  };
}

const MAGICEDEN_MARKETPLACE = "0x9A1D00bEd7CD04BCDA516d721A596eb22Aac6834"
const MAX_RETRIES: number = 5;
const ALCHEMY_API_KEY = process.env.ALCHEMY_API_KEY as string;
const OPENSEA_PROTOCOL_ADDRESS = "0x0000000000000068F116a894984e2DB1123eB395"
const STANDARD_INTERVAL = 5000; // 5 seconds - standard interval for most timers
const HEALTH_CHECK_INTERVAL = process.env.NODE_ENV === 'development' ? 5000 : 60000; // 5 seconds in dev, 60 seconds in prod
// Initialize with default marketplaces - will be updated by WebSocket
let enabledMarketplaces: string[] = ['opensea', 'magiceden', 'blur']
// Use deterministic client ID based on API_KEY or a fallback
const generateDeterministicClientId = (): string => {
  const baseKey = process.env.API_KEY || 'bidding-bot-default';
  return baseKey
};

const clientId = generateDeterministicClientId();

const app = express();
const port = process.env.PORT || 3003;

// Security headers with helmet
app.use(helmet({
  contentSecurityPolicy: {
    directives: {
      defaultSrc: ["'self'"],
      scriptSrc: ["'self'", "'unsafe-inline'"],
      styleSrc: ["'self'", "'unsafe-inline'"],
      imgSrc: ["'self'", "data:", "https:"],
      connectSrc: ["'self'", "wss:", "https:", "wss://nfttools-ws-proxy.nfttools.io", "https://updates.nfttools.io"],
      fontSrc: ["'self'"],
      objectSrc: ["'none'"],
      mediaSrc: ["'self'"],
      frameSrc: ["'none'"],
      upgradeInsecureRequests: [],
    },
  },
  hsts: {
    maxAge: 31536000, // 1 year
    includeSubDomains: true,
    preload: true
  },
  noSniff: true,
  xssFilter: true,
  referrerPolicy: { policy: 'strict-origin-when-cross-origin' },
  frameguard: { action: 'deny' },
  permittedCrossDomainPolicies: false,
}));

// Additional security headers
app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'DENY');
  res.setHeader('X-XSS-Protection', '1; mode=block');
  res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
  res.setHeader('Permissions-Policy', 'camera=(), microphone=(), geolocation=()');

  // Remove fingerprinting headers
  res.removeHeader('X-Powered-By');

  next();
});

app.use(bodyParser.json({ limit: '10mb' }));
app.use(bodyParser.urlencoded({ extended: true, limit: '10mb' }));

app.use(cors({
  origin: function (origin, callback) {
    if (!origin || allowedOrigins.includes(origin)) {
      callback(null, true);
    } else {
      callback(new Error('Not allowed by CORS: ' + origin));
    }
  },
  methods: ['GET', 'POST', 'PUT', 'DELETE'],
  credentials: true,
  maxAge: 86400 // 24 hours
}));

const server = http.createServer(app);
const wss = new WebSocketServer({ server });
export let wsManager: WebSocketManager;
let retryCount: number = 0;

// WebSocket Connection Metrics
interface ConnectionMetrics {
  connectionStartTime: number | null;
  connectionEndTime: number | null;
  connectionDuration: number;
  totalConnections: number;
  totalDisconnections: number;
  averageConnectionDuration: number;
  longestConnection: number;
  shortestConnection: number;
  currentConnectionId: string | null;
}

const connectionMetrics: ConnectionMetrics = {
  connectionStartTime: null,
  connectionEndTime: null,
  connectionDuration: 0,
  totalConnections: 0,
  totalDisconnections: 0,
  averageConnectionDuration: 0,
  longestConnection: 0,
  shortestConnection: Infinity,
  currentConnectionId: null
};

const connectionHistory: Array<{
  id: string;
  startTime: number;
  endTime: number;
  duration: number;
  reason: string;
}> = [];

// WebSocket broadcast manager (replaces Redis Streams)
const broadcastManager = new BroadcastManager();

// Backward compatibility shim for clients.size references
const clients = {
  get size() { return broadcastManager.getStats().clients; }
};

const walletsArr: string[] = []
const MIN_BID_DURATION = 60

// Redis cluster health monitoring
let redisFailures = 0;
let lastRedisRecovery = 0;
const REDIS_FAILURE_THRESHOLD = 2; // Consecutive failures before recovery
const REDIS_RECOVERY_COOLDOWN = 60000; // 1 minute cooldown between recovery attempts

// Helper to detect Redis errors
function isRedisError(error: any): boolean {
  const errorMessage = error instanceof Error ? error.message : String(error);
  return errorMessage.includes('ECONNREFUSED') ||
    errorMessage.includes('ECONNRESET') ||
    errorMessage.includes('ETIMEDOUT') ||
    errorMessage.includes('Connection is closed') ||
    errorMessage.includes('Connection lost') ||
    errorMessage.includes('READONLY');
}

function cleanupMemory() {
  if (global.gc) {
    global.gc();
  }
}

async function checkRedisHealth(): Promise<boolean> {
  try {
    // Check BullMQ connection health instead of general Redis
    const bullmqHealth = await checkBullMQConnectionsHealth();
    const allHealthy = Object.values(bullmqHealth).every((h: any) => h.healthy);
    return allHealthy;
  } catch (error) {
    // If we can't check health, it's not healthy
    return false;
  }
}

async function handleRedisFailure() {
  const now = Date.now();

  // Check cooldown
  if (now - lastRedisRecovery < REDIS_RECOVERY_COOLDOWN) {
    const cooldownRemaining = Math.ceil((REDIS_RECOVERY_COOLDOWN - (now - lastRedisRecovery)) / 1000);
    return;
  }


  try {
    lastRedisRecovery = now;
    await clearQueueAndRestartJobs();

    // Reset failure counter after successful recovery
    redisFailures = 0;
  } catch (error) {
    errorLog('RedisRecovery', 'Failed to recover from Redis failure', error);
    // Don't reset failure counter if recovery fails
  }
}

async function monitorHealth() {
  try {
    if (!queue) return;
    const isRedisHealthy = await checkRedisHealth();
    if (!isRedisHealthy) {
      redisFailures++;

      if (redisFailures >= REDIS_FAILURE_THRESHOLD) {
        await handleRedisFailure();
      }
      return; // Skip rest of health check if Redis is down
    } else {
      // Reset failure counter if Redis is healthy
      if (redisFailures > 0) {
        redisFailures = 0;
      }
    }

    const counts = await queue.getJobCounts();
    const used = process.memoryUsage();
    const memoryStats = {
      rss: `${Math.round(used.rss / 1024 / 1024)}MB`,
      heapTotal: `${Math.round(used.heapTotal / 1024 / 1024)}MB`,
      heapUsed: `${Math.round(used.heapUsed / 1024 / 1024)}MB`,
      external: `${Math.round(used.external / 1024 / 1024)}MB`,
    }

    // Get max heap from NODE_OPTIONS or use default
    const maxHeapMatch = process.execArgv.join(' ').match(/--max-old-space-size=(\d+)/);
    const MAX_HEAP_MB = maxHeapMatch ? parseInt(maxHeapMatch[1]) : 8192;

    const heapUsedMB = used.heapUsed / 1024 / 1024;
    const heapTotalMB = used.heapTotal / 1024 / 1024;
    const heapPercentOfMax = (heapUsedMB / MAX_HEAP_MB) * 100;
    const heapPercentOfTotal = (used.heapUsed / used.heapTotal) * 100;


    // Check if queue is paused
    const isPaused = await queue.isPaused();

    // Memory threshold warnings (proactive GC adjusts these downward)
    const MEMORY_WARNING_MB = 3000;  // 69% of heap - GC may be struggling
    const MEMORY_CRITICAL_MB = 3500; // 80% of heap - GC failure, manual intervention needed

    if (heapUsedMB > MEMORY_CRITICAL_MB) {
      errorLog('Memory', `CRITICAL: Heap at ${Math.round(heapUsedMB)}MB - approaching ${MAX_HEAP_MB}MB limit`);
      cleanupMemory();
      // Force GC if available
      if (global.gc) {
        global.gc();
      }
    } else if (heapUsedMB > MEMORY_WARNING_MB) {
      warnLog('Memory', `WARNING: Heap at ${Math.round(heapUsedMB)}MB - approaching limit`);
    }

    // Also trigger cleanup at 75% of max (fallback)
    if (heapPercentOfMax > 75) {
      cleanupMemory();
    }

    const totalJobs = Object.values(counts).reduce((a, b) => a + b, 0);


    Object.entries(memoryStats).forEach(([key, value]) => {
    });


    // Show password status first
    const hasPassword = !!getPassword();
    if (!hasPassword) {
    } else {
    }

    // Show paused status prominently
    if (isPaused) {
    } else {
    }

    // Show waiting job monitor status
    if (counts.waiting > 0) {
    }

    Object.entries(counts).forEach(([key, value]) => {
      const color = key === 'waiting' && value > 0 ? RED :
        value > RATE_LIMIT * 2 ? RED :
          value > RATE_LIMIT ? YELLOW : GREEN;
    });

    // Show trading disabled cache stats
    const disabledStats = tradingDisabledCache.getStats();
    if (disabledStats.total > 0) {
      Object.entries(disabledStats.byMarketplace).forEach(([marketplace, count]) => {
      });

      // Show cache details
      const cacheDetails = tradingDisabledCache.getCacheStats();
    }

    // No need to call updateAllBidRates() - tracker handles updates automatically

  } catch (error) {
    errorLog('HealthCheck', 'Error during health check', error);
  }
}

setInterval(monitorHealth, HEALTH_CHECK_INTERVAL);


// Queue monitoring removed - using simple capacity-based throttling instead

// Auto-resume checker - runs every 60 seconds to check for password availability
const PASSWORD_CHECK_INTERVAL = STANDARD_INTERVAL;
let isCheckingPassword = false;

async function checkPasswordAndResume() {
  // Prevent concurrent checks
  if (isCheckingPassword) return;

  try {
    isCheckingPassword = true;

    // If password is already set, nothing to do
    if (getPassword()) return;

    // Try to recover password from Redis
    const recovered = await recoverPasswordFromRedis();
    if (recovered) {
      await resumeQueueAndWorkers();
    }
  } catch (error) {
    errorLog('PasswordCheck', 'Error checking password', error);
  } finally {
    isCheckingPassword = false;
  }
}

// Note: Password auto-recovery checker will be started in startServer() after Redis is ready

// Queue monitor will be initialized after queues are created in startServer()

// Add this near the top with other global variables
export const activeTasks: Map<string, ITask> = new Map();

// Track failed task starts/stops for automatic retry
interface FailedTaskRetry {
  task: ITask;
  retryCount: number;
  nextRetryAt: number;  // timestamp
  lastError: string;
}

const failedTaskStartQueue = new Map<string, FailedTaskRetry>();
const failedTaskStopQueue = new Map<string, FailedTaskRetry>();

// Track queueing progress for progress-based watchdog timeout
// Key: `${taskId}-${marketplace}`, Value: { lastProgressAt, jobsQueued, totalJobs }
interface QueueingProgress {
  lastProgressAt: Date;
  jobsQueued: number;
  totalJobs: number;
}
const queueingProgress = new Map<string, QueueingProgress>();

// Get task with ID by index
function getActiveTaskWithIdByIndex(index: number): { id: string, task: ITask } | undefined {
  const tasksEntries = Array.from(activeTasks.entries());
  if (index >= 0 && index < tasksEntries.length) {
    const [id, task] = tasksEntries[index];
    return { id, task };
  }
  return undefined;
}

// Authorized queue clear function - centralizes all queue obliteration

// Clear queue and restart all jobs on server start
// NOTE: This is the only function (besides stopTask) that should obliterate queues
// All other job removal should go through stopTask -> removeAllTaskJobsFromQueue
async function clearQueueAndRestartJobs() {
  try {
    // Pause main queue before obliterating
    await queue.pause();

    // Wait for queue to fully pause and check status
    let retries = 10;
    while (retries > 0) {
      const isPaused = await queue.isPaused();
      if (isPaused) break;
      await new Promise(resolve => setTimeout(resolve, 500));
      retries--;
    }

    // Additional safety wait
    await new Promise(resolve => setTimeout(resolve, 1000));

    // Obliterate main queue with force option
    await queue.obliterate({ force: true });

    // Check if password is available before resuming queue
    const password = getPassword();
    if (password) {
      // Resume main queue only if password is available
      await queue.resume();
    }

    // Clear any stored job states
    parentJobs.clear();
    // Clear all countdown timers
    taskCountdownTimers.forEach((timer) => clearInterval(timer));
    taskCountdownTimers.clear();

    // Clear all task timeouts
    taskTimeouts.forEach((timeout) => clearTimeout(timeout));
    taskTimeouts.clear();

    // Note: We no longer clear taskStatus here to preserve tracking during system recovery

    // Fetch all tasks from database
    await fetchCurrentTasks();

    // Only start scheduled loop if password is available
    if (password) {
      // Wait 5 seconds before restarting scheduled loop
      await new Promise(resolve => setTimeout(resolve, 5000));

      // Start the scheduled loop
      await runScheduledLoop();
    } else {
    }

  } catch (error) {
    errorLog('QueueClear', 'Error during clearQueueAndRestartJobs', error);
    throw error;
  }
}

// Network connectivity monitoring
let isConnected = true;
let connectivityLostAt: number | null = null;

// Export network status for use in other modules if needed
export const getNetworkStatus = () => isConnected;

async function checkInternetConnectivity(): Promise<boolean> {
  // Multiple connectivity check endpoints for redundancy
  const CONNECTIVITY_ENDPOINTS = [
    { name: 'Google', url: 'https://www.google.com', expectedStatus: 200 },
    { name: 'Cloudflare', url: 'https://1.1.1.1', expectedStatus: 200 },
    { name: 'Cloudflare DNS', url: 'https://cloudflare-dns.com', expectedStatus: 200 }
  ];

  const MAX_RETRIES = 3;
  const RETRY_DELAY = 2000; // 2 seconds between retries

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {

    // Try each endpoint - success if ANY endpoint responds
    for (const endpoint of CONNECTIVITY_ENDPOINTS) {
      try {
        const response = await axios.get(endpoint.url, { timeout: 5000 });

        if (response.status === endpoint.expectedStatus || response.status === 204) {
          return true; // Internet is available
        }
      } catch (error: any) {
        // Endpoint failed, try next one
        const errorMsg = error?.message || String(error);
      }
    }

    // All endpoints failed for this attempt
    if (attempt < MAX_RETRIES) {
      warnLog('Health', `[ConnectivityCheck] All endpoints failed, retrying in ${RETRY_DELAY}ms (${attempt}/${MAX_RETRIES})`);
      await new Promise(resolve => setTimeout(resolve, RETRY_DELAY));
    }
  }

  // All retries exhausted and all endpoints failed
  errorLog('ConnectivityMonitor', 'Internet connectivity lost after retry attempts');
  return false;
}

async function handleConnectivityRestored() {

  // Calculate how long connectivity was lost
  const downtime = connectivityLostAt ? Date.now() - connectivityLostAt : 0;
  const downtimeMinutes = Math.floor(downtime / 60000);

  if (downtime > 0) {
  }

  // No need to resume queues - BullMQ handles retries automatically

  // Only clear and restart jobs if connectivity was lost for more than 1 minute
  if (downtime > 60000) {
    await clearQueueAndRestartJobs();
  } else {
  }

  // Always reconnect WebSocket
  if (wsManager) {
    await connectWebSocket();
  }

  // Always reinitialize listing scheduler
  if (wsManager) {
    await initializeListingScheduler(wsManager);
  }

  // Reset connectivity lost timestamp
  connectivityLostAt = null;
}



let connectivityCheckInterval: NodeJS.Timeout | null;

// Track recently seen ITEM_SOLD events to prevent duplicates
const recentItemSoldEvents = new Set<string>();
// Clean up old events every 60 seconds to prevent memory growth
// Regular cleanup to maintain memory efficiency
setInterval(() => {
  recentItemSoldEvents.clear();
}, STANDARD_INTERVAL);

function startConnectivityMonitoring() {
  // Check connectivity every 60 seconds
  connectivityCheckInterval = setInterval(async () => {
    const currentlyConnected = await checkInternetConnectivity();

    if (!isConnected && currentlyConnected) {
      // Connection restored
      isConnected = true;
      await handleConnectivityRestored();
    } else if (isConnected && !currentlyConnected) {
      // Connection lost
      isConnected = false;
      connectivityLostAt = Date.now();
      errorLog('ConnectivityMonitor', 'Internet connectivity lost!');
    }
  }, STANDARD_INTERVAL);

}

function stopConnectivityMonitoring() {
  if (connectivityCheckInterval) {
    clearInterval(connectivityCheckInterval);
    connectivityCheckInterval = null;
  }
}

// Modify fetchCurrentTasks to populate activeTasks
async function fetchCurrentTasks() {
  try {
    const tasks = await Task.find({})
    const wallets = (await walletCache.getAllWallets()).map((wallet) => wallet.address)

    walletsArr.push(...wallets);
    const formattedTasks = tasks.map((task) => ({
      ...task.toObject(),
      _id: task._id.toString(),
      user: task.user.toString()
    })) as ITask[];

    // Clear and populate activeTasks Map
    activeTasks.clear();

    // Clean up orphaned timers (only if task no longer exists in database)
    const validTaskIds = new Set(formattedTasks.map(task => task._id));
    Object.keys(taskStatus).forEach(taskId => {
      if (!validTaskIds.has(taskId)) {
        // Note: We no longer delete taskStatus here to preserve cancellation tracking
        // Only clean up associated timers
        const countdownTimer = taskCountdownTimers.get(taskId);
        if (countdownTimer) {
          clearInterval(countdownTimer);
          taskCountdownTimers.delete(taskId);
        }

        // Clean up marketplace-specific timers
        const marketplacesList = ['opensea', 'magiceden', 'blur'];
        for (const marketplace of marketplacesList) {
          const timerKey = `${taskId}-${marketplace}`;
          const marketplaceTimer = taskCountdownTimers.get(timerKey);
          if (marketplaceTimer) {
            clearInterval(marketplaceTimer);
            taskCountdownTimers.delete(timerKey);
          }
        }

        const taskTimeout = taskTimeouts.get(taskId);
        if (taskTimeout) {
          clearTimeout(taskTimeout);
          taskTimeouts.delete(taskId);
        }
      }
    });

    formattedTasks.forEach(task => {
      activeTasks.set(task._id, task);
    });

    currentTasks.push(...formattedTasks);
    updateTaskIndexes(currentTasks);

    // Restore task state from Redis for running tasks
    const runningTasks = formattedTasks.filter(task => task.running);
    if (runningTasks.length > 0) {

      for (const task of runningTasks) {
        try {
          // Tasks will be restarted fresh by runScheduledLoop
        } catch (err) {
          errorLog('Recovery', `Failed to restore state for task ${task._id}: ${err instanceof Error ? err.message : err}`);
        }
      }
    }

    // Task initialization complete
  } catch (error) {
    errorLog('Database', 'Failed to fetch current tasks', error as Error);
  }
}

interface BulkJobParent {
  parentId: string;
  totalJobs: number;
  completedJobs: number;
  failedJobs: number;        // Number of jobs that failed
  startTime: number;
  taskId: string;
  jobName: string;
  loopNumber: number;         // Loop number this parent job belongs to
  // Optional bid tracking fields (only used for cancellation jobs)
  totalBids?: number;        // Total number of bids to cancel
  completedBids?: number;    // Number of successfully cancelled bids
  // Hybrid tracking fields for accurate job counting
  expectedJobs?: number;      // Jobs expected from preprocessing
  actualJobs?: number;        // Jobs actually added to queue
  allJobsQueued?: boolean;    // Flag when queueing is complete
}

const parentJobs = new Map<string, BulkJobParent>();

// Track processed jobs to prevent double counting
const processedJobsPerParent = new Map<string, Set<string>>();

// Cancellation parent jobs for tracking cancellation progress (similar to parentJobs)
// Removed old cancellation parent job tracking - now using cancellationTracker

// Preprocessing results stored per task
interface TaskPreprocessing {
  bidType: 'token' | 'trait' | 'collection';
  opensea: {
    enabled: boolean;
    traits: { type: string; value: string }[];
    tokens: string[];
    jobCount: number;
    skipReason?: string;
  };
  blur: {
    enabled: boolean;
    traits: { [key: string]: string }[];
    tokens: number[];
    jobCount: number;
    skipReason?: string;
  };
  magiceden: {
    enabled: boolean;
    traits: { attributeKey: string; attributeValue: string }[];
    tokens: number[];
    jobCount: number;
    skipReason?: string;
  };
}

const taskPreprocessing = new Map<string, TaskPreprocessing>();

// Lock for scheduled job processing to prevent concurrent execution
const scheduledJobLocks = new Map<string, boolean>();

// Queue management for password-based security
async function pauseQueueAndWorkers() {
  try {

    // Pause main queue
    await queue.pause();

    // Pause all workers
    for (const worker of workers) {
      await worker.pause();
    }

  } catch (error) {
    errorLog('QueuePause', 'Error pausing queue and workers', error);
  }
}

async function resumeQueueAndWorkers() {
  try {

    // Resume main queue
    await queue.resume();

    // Resume all workers
    for (const worker of workers) {
      await worker.resume();
    }


    // Retry any failed jobs that were due to missing password
    try {
      const failedJobs = await queue.getFailed();

      // Retry jobs that failed due to missing password
      const passwordFailedJobs = failedJobs.filter((job: Job) =>
        job.failedReason?.includes('No password available')
      );

      if (passwordFailedJobs.length > 0) {
        await Promise.all(passwordFailedJobs.map((job: Job) => job.retry()));
      }
    } catch (retryError) {
      errorLog('JobRetry', 'Error retrying failed jobs', retryError);
    }
  } catch (error) {
    errorLog('QueueResume', 'Error resuming queue and worker', error);
  }
}

// Worker health monitoring
let workerHealthInterval: NodeJS.Timeout | null = null;
const WORKER_HEALTH_CHECK_INTERVAL = STANDARD_INTERVAL;

// Worker failure tracking
const workerFailures = new Map<number, { count: number; lastFailure: number; errorTypes: string[] }>();
const WORKER_FAILURE_THRESHOLD = 3; // Failures before restart
const WORKER_FAILURE_WINDOW = 5 * 60 * 1000; // 5 minutes
let lastWorkerRestartTime = 0;
const WORKER_RESTART_COOLDOWN = 5 * 60 * 1000; // 5 minutes between restarts


function startWorkerHealthMonitoring() {
  // Don't run immediate health check - workers need time to connect to Redis
  // BullMQ workers take 5-10 seconds to establish connections and emit 'ready' event


  // Wait 5 seconds before starting health checks
  setTimeout(() => {
    // Run first health check after startup delay
    checkWorkerHealth();

    // Set up periodic health checks
    workerHealthInterval = setInterval(() => {
      checkWorkerHealth();
    }, WORKER_HEALTH_CHECK_INTERVAL);

  }, 5000); // 5 second startup delay (workers connect in 5-10s, install script adds 10s buffer)
}


async function checkWorkerHealth() {
  try {
    // Check Redis connections health
    const connectionHealth = await checkBullMQConnectionsHealth();
    const unhealthyConnections = Object.entries(connectionHealth)
      .filter(([_, status]) => !status.healthy);

    if (unhealthyConnections.length > 0) {
      errorLog('HealthCheck', `Unhealthy Redis connections detected: ${unhealthyConnections.length}`);
      unhealthyConnections.forEach(([id, status]) => {
        errorLog('HealthCheck', `Redis connection ${id}: ${status.error}`);
      });
    }

    // Check worker status
    const workerStatuses: Array<{
      index: number;
      name: string;
      running: boolean;
      paused: boolean;
      concurrency: number;
      healthy: boolean;
      error?: string;
    }> = [];

    // Check all workers
    workers.forEach((worker, index) => {
      try {
        workerStatuses.push({
          index: index,
          name: `worker-${index}`,
          running: worker.isRunning(),
          paused: worker.isPaused(),
          concurrency: worker.concurrency,
          healthy: worker.isRunning() && !worker.isPaused()
        });
      } catch (error) {
        workerStatuses.push({
          index: index,
          name: `worker-${index}`,
          running: false,
          paused: false,
          concurrency: 0,
          healthy: false,
          error: error instanceof Error ? error.message : 'Unknown error'
        });
      }
    });

    const unhealthyWorkers = workerStatuses.filter(status => !status.healthy);

    if (unhealthyWorkers.length > 0) {
      errorLog('HealthCheck', `Unhealthy workers detected: ${unhealthyWorkers.length}/${workers.length}`);

      let shouldRestartAll = false;

      // Attempt to restart unhealthy workers
      for (const workerStatus of unhealthyWorkers) {
        if ('error' in workerStatus && workerStatus.error) {
          errorLog('HealthCheck', `Worker ${workerStatus.index}: ${workerStatus.error}`);

          // Track the failure
          const thresholdExceeded = trackWorkerFailure(workerStatus.index, workerStatus.error);
          if (thresholdExceeded) {
            shouldRestartAll = true;
          }
        } else if (workerStatus.paused) {
          try {
            if (workerStatus.index < workers.length) {
              await workers[workerStatus.index].resume();
            }
          } catch (error) {
            errorLog('HealthCheck', `Failed to resume worker ${workerStatus.index}`, error);

            // Track resume failure
            const thresholdExceeded = trackWorkerFailure(workerStatus.index, `Resume failed: ${error}`);
            if (thresholdExceeded) {
              shouldRestartAll = true;
            }
          }
        } else if (!workerStatus.running) {
          // Worker is not running at all
          errorLog('HealthCheck', `Worker ${workerStatus.index} is not running`);
          const thresholdExceeded = trackWorkerFailure(workerStatus.index, 'Worker not running');
          if (thresholdExceeded) {
            shouldRestartAll = true;
          }
        }
      }

      // Check if we should restart all workers
      if (shouldRestartAll) {
        const now = Date.now();
        if (now - lastWorkerRestartTime > WORKER_RESTART_COOLDOWN) {

          // Log failure details
          workerFailures.forEach((failure, index) => {
            if (failure.count >= WORKER_FAILURE_THRESHOLD) {
              failure.errorTypes.slice(-3).forEach(err => console.log(RED + `  - ${err}` + RESET));
            }
          });

          lastWorkerRestartTime = now;

          // Clear failure tracking
          workerFailures.clear();

          // Clear queues and restart
          await clearQueueAndRestartJobs();
        } else {
          const cooldownRemaining = Math.ceil((WORKER_RESTART_COOLDOWN - (now - lastWorkerRestartTime)) / 1000);
        }
      }
    }

    // Check queue status
    const mainQueueCounts = await queue.getJobCounts();

    // Log health summary
    const healthSummary = {
      connections: {
        total: Object.keys(connectionHealth).length,
        healthy: Object.keys(connectionHealth).length - unhealthyConnections.length
      },
      workers: {
        total: 1,
        healthy: 1 - unhealthyWorkers.length
      },
      queues: {
        main: {
          waiting: mainQueueCounts.waiting || 0,
          active: mainQueueCounts.active || 0,
          failed: mainQueueCounts.failed || 0
        }
      }
    };

    // Connection pool monitoring
    const connectionPoolStats = getBullMQConnections();
    const connectionAges = connectionPoolStats.map(conn => ({
      id: conn.id,
      age: Math.floor(conn.age / 1000), // Convert to seconds
      healthy: connectionHealth[conn.id]?.healthy || false
    }));

    // Log connection pool information if there are old connections or in development
    const oldConnections = connectionAges.filter(conn => conn.age > 3600); // Connections older than 1 hour

    // Only log if there are issues or if in development mode
    if (unhealthyConnections.length > 0 || unhealthyWorkers.length > 0 || oldConnections.length > 0 || process.env.NODE_ENV === 'development') {

      if (oldConnections.length > 0) {
        oldConnections.forEach(conn => {
        });
      }
    }

    // Log detailed connection pool stats to monitoring system

    // Enhanced Redis connection pool monitoring
    const maxConnections = connectionPoolStats.length;
    const connectionPoolWarningThreshold = 40; // Warn when 80% of assumed max 50 connections
    const connectionPoolCriticalThreshold = 45; // Critical when 90% of max connections

    if (maxConnections >= connectionPoolCriticalThreshold) {
      errorLog('ConnectionPool', `Redis connection pool at critical level: ${maxConnections} connections`);
      errorLog('Database', `Redis connection pool critical: ${maxConnections}/${connectionPoolCriticalThreshold}`, new Error('Connection pool exhausted'));
    } else if (maxConnections >= connectionPoolWarningThreshold) {
      warnLog('Database', `Redis connection pool warning: ${maxConnections}/${connectionPoolWarningThreshold}`);
    }

    // Check for connection leaks (very old connections)
    // Note: BullMQ legitimately maintains ~6-8 long-lived connections:
    // - 2 for main queue (queue + worker)
    // - 2 for blur queue (queue + worker)
    // - 1 for scheduler
    // - 1 for events
    // Only warn if we have more old connections than expected (indicating actual leaks)
    const connectionLeakThreshold = 2 * 60 * 60; // 2 hours in seconds
    const expectedLongLivedConnections = 8; // BullMQ expected connections
    const possibleLeaks = connectionAges.filter(conn => conn.age > connectionLeakThreshold);

    if (possibleLeaks.length > expectedLongLivedConnections) {
      warnLog('Database', `Possible Redis connection leaks: ${possibleLeaks.length} connections older than 2 hours (expected ~${expectedLongLivedConnections} for BullMQ)`);
    }

    // MongoDB connection pool monitoring
    try {
      const mongoClient = mongoose.connection.getClient();
      // @ts-ignore - Access internal connection pool stats
      const poolStats = mongoClient?.topology?.s?.pool;

      if (poolStats) {
        const maxPoolSize = Number(process.env.MONGO_MAX_POOL_SIZE) || 100;
        const minPoolSize = Number(process.env.MONGO_MIN_POOL_SIZE) || 30;

        // Calculate current pool metrics
        const totalConnections = poolStats.totalConnectionCount || 0;
        const availableConnections = poolStats.availableConnectionCount || 0;
        const activeConnections = totalConnections - availableConnections;
        const poolUtilization = totalConnections > 0 ? (activeConnections / maxPoolSize) * 100 : 0;

        // Log MongoDB pool statistics

        // Warning threshold: 80% of maxPoolSize
        const warningThreshold = maxPoolSize * 0.8;
        // Critical threshold: 90% of maxPoolSize
        const criticalThreshold = maxPoolSize * 0.9;

        if (activeConnections >= criticalThreshold) {
          errorLog('MongoDBPool', `MongoDB connection pool at critical level: ${activeConnections}/${maxPoolSize} connections in use (${Math.round(poolUtilization)}%)`);
          errorLog('Database', `MongoDB connection pool critical: ${activeConnections}/${maxPoolSize} (${Math.round(poolUtilization)}%)`, new Error('Connection pool near exhaustion'));
        } else if (activeConnections >= warningThreshold) {
          warnLog('Database', `MongoDB connection pool warning: ${activeConnections}/${maxPoolSize} (${Math.round(poolUtilization)}%)`);
        }

        // Check if pool is below minimum (possible connection issues)
        if (totalConnections < minPoolSize && mongoose.connection.readyState === 1) {
          warnLog('Database', `MongoDB pool below minimum: ${totalConnections}/${minPoolSize}`);
        }
      }
    } catch (mongoMonitorError) {
      // Don't let MongoDB monitoring errors crash the health check
      errorLog('MongoDBPoolMonitoring', 'Error monitoring MongoDB pool', mongoMonitorError);
    }

  } catch (error) {
    errorLog('WorkerHealthCheck', 'Error during worker health check', error);
  }
}


// Track worker failures
function trackWorkerFailure(workerIndex: number, error: string) {
  const now = Date.now();
  const failure = workerFailures.get(workerIndex) || { count: 0, lastFailure: 0, errorTypes: [] };

  // Reset count if outside the failure window
  if (now - failure.lastFailure > WORKER_FAILURE_WINDOW) {
    failure.count = 0;
    failure.errorTypes = [];
  }

  failure.count++;
  failure.lastFailure = now;
  failure.errorTypes.push(error);

  // Keep only last 10 error types
  if (failure.errorTypes.length > 10) {
    failure.errorTypes = failure.errorTypes.slice(-10);
  }

  workerFailures.set(workerIndex, failure);

  errorLog('WorkerFailure', `Worker ${workerIndex} failure tracked: ${failure.count}/${WORKER_FAILURE_THRESHOLD} in window`);
  errorLog('WorkerFailure', `Worker ${workerIndex} error`, error);

  return failure.count >= WORKER_FAILURE_THRESHOLD;
}


// =============================================================================
// CRASH DETECTION AND TASK RECOVERY
// =============================================================================

/**
 * Detect if previous shutdown was graceful or a crash
 * Uses LRU cache to determine shutdown state (within process lifecycle)
 */
async function detectPreviousShutdown(): Promise<'graceful' | 'crash' | 'first-start'> {
  try {
    // Graceful shutdown recovery removed - always treat as first start
    return 'first-start';

    // Shutdown timestamp exists but reason is not 'graceful' = crash
    return 'crash';
  } catch (error) {
    warnLog('Startup', 'Failed to detect previous shutdown type');
    return 'first-start'; // Safe default
  }
}

/**
 * Save running tasks and listings to LRU cache before shutdown
 * Called during graceful shutdown (SIGINT/SIGTERM)
 * Note: Only persists within process lifecycle - graceful restart only works within same process
 */
async function saveRunningTasksBeforeShutdown(): Promise<void> {
  try {
    // Get all currently running tasks
    const runningTasks = await Task.find({ running: true }).select('_id').lean();
    const runningListings = await Listing.find({ running: true }).select('_id').lean();

    const taskIds = runningTasks.map(t => t._id.toString());
    const listingIds = runningListings.map(l => l._id.toString());

    // Graceful shutdown recovery removed - tasks won't auto-resume after restart

    logLifecycleEvent('GRACEFUL_SHUTDOWN_TASKS_SAVED', {
      taskCount: taskIds.length,
      listingCount: listingIds.length,
    });

  } catch (error) {
    errorLog('Startup', 'Failed to save running tasks before shutdown');
  }
}

/**
 * Resume previously running tasks after crash recovery
 * Only called when password is available and crash was detected
 * Note: Only works within same process lifecycle (LRU cache)
 */
async function resumePreviouslyRunningTasks(): Promise<void> {
  try {
    // Graceful shutdown recovery removed - tasks won't auto-resume after restart

    logLifecycleEvent('CRASH_RECOVERY_TASKS_RESUMED', {
      taskCount: 0,
      listingCount: 0,
    });

  } catch (error) {
    errorLog('Startup', 'Failed to resume previously running tasks');
  }
}

async function startServer() {
  try {
    // Development mode setup
    if (process.env.NODE_ENV === 'development') {
      // Make processAllActiveListings available globally in dev mode
      (global as any).processAllActiveListings = processAllActiveListings;
    }

    // REMOVED: Redis ready check - BullMQ connections are checked individually during createBullMQConnection()


    // Create BullMQ connections

    let mainQueueConnection: any;
    let mainQueueEventsConnection: any;

    try {
      // Main queue connection
      mainQueueConnection = await createBullMQConnection('main-queue');
      bullMQConnections.push(mainQueueConnection);

      // Main queue events connection
      mainQueueEventsConnection = await createBullMQConnection('main-queue-events');
      bullMQConnections.push(mainQueueEventsConnection);

      // Verify all connections are healthy before proceeding
      const healthCheck = await checkBullMQConnectionsHealth();
      const unhealthyConnections = Object.entries(healthCheck).filter(([_, status]) => !status.healthy);

      if (unhealthyConnections.length > 0) {
        errorLog('BullMQInit', 'Unhealthy Redis connections detected');
        unhealthyConnections.forEach(([id, status]) => {
          errorLog('HealthCheck', `Redis connection ${id}: ${status.error}`);
        });
        throw new Error('One or more Redis connections are unhealthy');
      }

    } catch (error) {
      errorLog('BullMQInit', 'Failed to create BullMQ connections', error);
      // Close any connections that were created
      await closeAllBullMQConnections();
      throw new Error('Failed to initialize BullMQ connections');
    }

    // Initialize queue options with main queue connection
    QUEUE_OPTIONS = {
      connection: mainQueueConnection,
      defaultJobOptions: {
        removeOnComplete: true,
        removeOnFail: true,
        attempts: 1,  // Prevent BullMQ's default 3 retries to avoid duplicate job processing
      }
    };

    // Initialize queue and events with their own connections
    queue = new Queue(QUEUE_NAME, QUEUE_OPTIONS);
    queueEvents = new QueueEvents(QUEUE_NAME, { connection: mainQueueEventsConnection });

    console.log('[STARTUP] BullMQ queue initialized (single queue architecture)');

    // Initialize memory snapshot service
    // Note: Redis param kept for backwards compatibility but not used (Redis is BullMQ-only)
    memorySnapshotService.initialize(null as any, queue, null, {
      memoryCleanupService,
      processedMessages,
      collectionStatsLRU,
      collectionDataLRU,
      traitsToRemove,
      removedTokens,
      tasksBeingRestarted,
      tasksBeingCancelled,
      tasksInCompletionProcess,
      failedTaskStartQueue,
      failedTaskStopQueue,
      queueingProgress,
      taskLocks,
      loopSchedulingLocks
    });
    console.log('[STARTUP] Memory snapshot service initialized');

    // Initialize memory cleanup service (processedMessages now uses lru-cache)
    memoryCleanupService.initialize({
      queueingProgress,
      failedTaskStartQueue,
      failedTaskStopQueue,
      taskLocks,
      loopSchedulingLocks,
      blurTransferRestrictedCollections: transferRestrictedCollections,
      blurAuthMetrics: authMetrics,
      blurTimeoutMetrics: timeoutMetrics
    });
    console.log('[STARTUP] Memory cleanup service initialized');

    // Start background collection data warming (4 min interval, Priority 999)
    collectionDataWarmer.startBackgroundWarming();
    console.log('[STARTUP] Background collection data warming started');

    // Initialize queue event handlers
    initializeQueueEventHandlers();


    // Single-queue architecture: 4 workers with evenly distributed concurrency
    const RATE_LIMIT_NUM = Number(RATE_LIMIT);
    /**
     * Helper function to create a worker instance
     * Each worker processes all job types (scheduled bids + counterbids)
     */
    async function createWorker(index: number): Promise<Worker> {
      const workerConcurrency = RATE_LIMIT_NUM

      const workerConnection = await createBullMQConnection(`worker-${index}`);
      bullMQConnections.push(workerConnection);

      const worker = new Worker(
        QUEUE_NAME,
        async (job) => {
          // TIMEOUT CONFIGURATION (Optimized for aggressive fast failure)
          // Cascade: Axios (30s) → Lock (45s) → Stall (60s) → Job (60s)
          // - Lock expires BEFORE stall detection (prevents race conditions)
          // - Ultra-fast failure (45s lock, 60s job) maximizes retry opportunities
          // - Allows 1-2 sequential API calls per job (competitive bidding priority)
          const jobTimeout = 60000; // 1 minute (aggressive timeout for fast retries)
          const abortController = new AbortController();

          const timeoutPromise = new Promise((_, reject) => {
            setTimeout(() => {
              abortController.abort(); // Signal cancellation to any async operations
              const customJobId = (job.opts as any)?.jobId || job.id;
              reject(new Error(`Job ${customJobId} (ID: ${job.id}) timed out after ${jobTimeout}ms - aborted`));
            }, jobTimeout);
          });
          try {
            const result = await Promise.race([
              processJob(job, abortController.signal),
              timeoutPromise
            ]);
            return result || { success: true };
          } catch (error: any) {
            // If it's a password error, throw it to fail the job properly
            if (error.code === 'NO_PASSWORD' || error.message?.includes('No password available')) {
              throw error;
            }
            // For other errors, also throw to ensure proper retry
            throw error;
          }
        },
        {
          connection: workerConnection,
          concurrency: workerConcurrency, // Evenly distributed concurrency
          lockDuration: 45000,       // 45 seconds - allows 1-2 sequential 30s API calls
          stalledInterval: 15000,    // 15 seconds - high-frequency stall detection
          maxStalledCount: 4         // 4 checks = 60s (triggers AFTER lock expires at 45s)
        }
      );

      // Add ready event listener for startup visibility
      worker.on('ready', () => {
      });

      // Add error handlers
      worker.on('error', async (error) => {
        errorLog('Worker', `Worker ${index} error`, error);
        trackWorkerFailure(index, `Worker error: ${error.message || String(error)}`);

        // Check if this is a Redis error
        if (isRedisError(error)) {
          errorLog('Worker', `Worker ${index} Redis connection error - attempting recovery`, error);
          try {
            // Force reconnection by recreating connection
            await workerConnection.disconnect();
            const newConnection = await createBullMQConnection(`worker-${index}`);
            // Update connection in global array
            const connIndex = bullMQConnections.indexOf(workerConnection);
            if (connIndex >= 0) {
              bullMQConnections[connIndex] = newConnection;
            }

            // Worker will automatically reconnect using the new connection
          } catch (reconnectError) {
            errorLog('Worker', `Failed to recover worker ${index} Redis connection`, reconnectError);
          }
        }
      });

      worker.on('failed', async (job, error) => {
        const taskId = getTaskIdForLog(job);
        // Deduplicate worker failure logs to prevent log flooding
        const workerErrorKey = `worker-failed-${index}-${taskId}-${job?.id}`;
        if (addIfNotExists(errorLogCache, workerErrorKey, true)) {
          errorLog('Worker', `Worker ${index} - Task ${taskId} - Job ${job?.id} failed`, error);
        }

        // Track execution latency for failed counterbid jobs
        if (job && job.timestamp) {
          const isCounterbid = job.name === OPENSEA_TOKEN_BID_COUNTERBID ||
            job.name === OPENSEA_TRAIT_BID_COUNTERBID ||
            job.name === OPENSEA_COLLECTION_BID_COUNTERBID ||
            job.name === MAGICEDEN_TOKEN_BID_COUNTERBID ||
            job.name === MAGICEDEN_TRAIT_BID_COUNTERBID ||
            job.name === MAGICEDEN_COLLECTION_BID_COUNTERBID;

          // Counterbid execution tracking removed - no longer needed
        }

        // Handle failed bid jobs - immediately mark as processed
        if (job && job.id && typeof job.id === 'string' && job.id.startsWith('bid-')) {

          // Extract task and marketplace from job ID (format: bid-taskId-marketplace-...)
          const parts = job.id.split('-');
          if (parts.length >= 3) {
            const taskId = parts[1];
            const marketplace = parts[2].toLowerCase();

            // Extract loopNumber from job data if available for precise parent lookup
            const jobLoopNumber = job.data?.loopNumber as number | undefined;

            // Mark job as processed to allow loop completion
            markJobAsProcessed(taskId, marketplace, job.id, false, jobLoopNumber).catch(err => {
              errorLog('Worker', 'Worker failed to mark failed bid job as processed', err);
            });
          }
        }

        // Handle failed cancellation jobs - track them as completed
        if (job && job.name && (job.name === CANCEL_OPENSEA_BID || job.name === CANCEL_OPENSEA_COUNTERBID || job.name === CANCEL_OPENSEA_OUTBID || job.name === CANCEL_MAGICEDEN_BID || job.name === CANCEL_BLUR_BID)) {
          const { taskId, identifier, orderId } = job.data;
          const marketplace = (job.name === CANCEL_OPENSEA_BID || job.name === CANCEL_OPENSEA_COUNTERBID || job.name === CANCEL_OPENSEA_OUTBID) ? 'opensea' :
            job.name === CANCEL_MAGICEDEN_BID ? 'magiceden' : 'blur';


          // Record the failed cancellation as a completion to ensure we reach 100%
          cancellationTracker.recordCompletion(taskId, 1, marketplace);
          updateTaskCancellationPercentage(taskId, undefined, marketplace).catch(err => {
            const failedTaskId = getTaskIdForLog(job);
            errorLog('CancellationTracking', `Failed to update cancellation percentage for task ${failedTaskId} after job failure`, err);
          });

          // Delete the order from cache to clean up
          if (orderId && identifier) {
            await safeDeleteFromCache(taskId, marketplace, identifier, orderId);
          }
        }

        // Only track as worker failure if it's a systemic issue
        if (error.message?.includes('could not renew lock') ||
          error.message?.includes('Missing lock') ||
          error.message?.includes('connection lost')) {
          trackWorkerFailure(index, `Job processing error: ${error.message}`);
        }
      });

      // Add completion hook for guaranteed cleanup verification
      worker.on('completed', async (job) => {
        // Track execution latency for counterbid jobs
        if (job && job.timestamp) {
          const isCounterbid = job.name === OPENSEA_TOKEN_BID_COUNTERBID ||
            job.name === OPENSEA_TRAIT_BID_COUNTERBID ||
            job.name === OPENSEA_COLLECTION_BID_COUNTERBID ||
            job.name === MAGICEDEN_TOKEN_BID_COUNTERBID ||
            job.name === MAGICEDEN_TRAIT_BID_COUNTERBID ||
            job.name === MAGICEDEN_COLLECTION_BID_COUNTERBID;

          // Counterbid execution tracking removed - no longer needed
        }

        // Verify cache was cleaned (defensive check)
        if (job && (job.name === CANCEL_OPENSEA_BID || job.name === CANCEL_OPENSEA_COUNTERBID || job.name === CANCEL_OPENSEA_OUTBID || job.name === CANCEL_MAGICEDEN_BID) && job.data.orderId) {
          const taskId = job.data.taskId;
          const orderHash = job.data.orderId;  // Primary identifier (blockchain hash)
          const marketplace = (job.name === CANCEL_OPENSEA_BID || job.name === CANCEL_OPENSEA_COUNTERBID || job.name === CANCEL_OPENSEA_OUTBID) ? 'opensea' : 'magiceden';
          const identifier = job.data.identifier;

          // Check if THE SPECIFIC order (by orderHash) still exists
          if (identifier && orderHash) {
            const cachedOrder = orderCache.getByOrderHash(taskId, orderHash);

            if (cachedOrder) {
              // THIS is the bug - the specific order still exists after cancellation
              warnLog('WorkerCleanup',
                `ORDER_STILL_IN_CACHE_AFTER_COMPLETION | taskId=${taskId} | orderHash=${orderHash?.substring(0, 16)}... | identifier=${identifier}`,
                { taskId, marketplace, identifier }
              );

              // Retry deletion (same parameters as primary deletion)
              await safeDeleteFromCache(taskId, marketplace, identifier, orderHash);

              // Verify retry succeeded
              const stillThere = orderCache.getByOrderHash(taskId, orderHash);
              if (stillThere) {
                errorLog('WorkerCleanup',
                  `CRITICAL: ORDER_STILL_IN_CACHE_AFTER_RETRY | taskId=${taskId}`,
                  new Error('Order deletion failed twice'),
                  { taskId, marketplace, identifier, orderHash }
                );
              }
            }
          }
        }
      });

      // Add failure hook for guaranteed cleanup (even if error handler failed to delete)
      worker.on('failed', async (job, error) => {
        if (job && (job.name === CANCEL_OPENSEA_BID || job.name === CANCEL_OPENSEA_COUNTERBID || job.name === CANCEL_OPENSEA_OUTBID || job.name === CANCEL_MAGICEDEN_BID)) {
          const taskId = job.data.taskId;
          const marketplace = (job.name === CANCEL_OPENSEA_BID || job.name === CANCEL_OPENSEA_COUNTERBID || job.name === CANCEL_OPENSEA_OUTBID) ? 'opensea' : 'magiceden';
          const identifier = job.data.identifier;

          // GUARANTEED CLEANUP: Even if error handler failed to delete
          if (job.data.orderId && identifier) {
            const orderHash = job.data.orderId;
            warnLog('WorkerCleanup', `GUARANTEED_CLEANUP_ON_FAILURE | taskId=${taskId} | marketplace=${marketplace} | orderHash=${orderHash?.substring(0, 16)}... | error=${error.message}`, { taskId });
            await safeDeleteFromCache(taskId, marketplace, identifier, orderHash);
          }
        }
      });

      return worker;
    }

    // Create workers (4 instances)
    for (let i = 0; i < NUM_WORKERS; i++) {
      const worker = await createWorker(i);
      workers.push(worker);
    }

    // REMOVED: Separate counterbid worker creation (unified into single worker array - all workers process all job types)
    // REMOVED: Concurrency manager integration (no longer needed with single queue)
    // REMOVED: Counterbid queue event handlers (no separate counterbid queue)
    // REMOVED: Reconciliation loop (no dynamic concurrency allocation)

    const minMain = Math.floor(RATE_LIMIT / 4);

    // Additional startup verification log

    // Update memory snapshot service with worker references (single queue architecture)
    // Note: Redis param kept for backwards compatibility but not used (Redis is BullMQ-only)
    memorySnapshotService.initialize(
      null as any,
      queue,
      null,
      {
        memoryCleanupService,
        processedMessages,
        collectionStatsLRU,
        collectionDataLRU,
        traitsToRemove,
        removedTokens,
        tasksBeingRestarted,
        tasksBeingCancelled,
        tasksInCompletionProcess,
        failedTaskStartQueue,
        failedTaskStopQueue,
        queueingProgress,
        taskLocks,
        loopSchedulingLocks
      },
      workers
    );

    // Initialize worker health monitoring
    startWorkerHealthMonitoring();



    // Add process-level error handler for uncaught BullMQ errors
    process.on('unhandledRejection', async (reason, promise) => {
      if (reason instanceof Error && reason.message.includes('could not renew lock')) {
        await BullMQErrorFormatter.formatError(reason);
      } else {
        errorLog('UnhandledRejection', 'Unhandled Rejection', reason);
      }
    });

    // Add uncaught exception handler for production stability
    process.on('uncaughtException', async (error) => {
      errorLog('UncaughtException', 'Uncaught exception', error);

      try {
        // Log to file
        errorLog('General', 'Uncaught exception - server shutting down', error);
      } catch (logError) {
        errorLog('Logger', 'Failed to log uncaught exception', logError);
      }

      // Give some time for error logging before exiting
      errorLog('UncaughtException', 'Server shutting down due to uncaught exception');

      // Try graceful shutdown
      try {
        // Update all running tasks to not running
        await Task.updateMany({ running: true }, { running: false });

        // Stop all workers
        for (const worker of workers) {
          await worker.close();
        }

        // Close BullMQ connections
        await closeAllBullMQConnections();

        // Close MongoDB connection
        await mongoose.connection.close();

      } catch (shutdownError) {
        errorLog('Shutdown', 'Error during graceful shutdown', shutdownError);
      }

      // Exit with error code
      process.exit(1);
    });

    const mongoUri = process.env.MONGODB_URI;
    if (!mongoUri) {
      throw new Error('MONGODB_URI environment variable is not set');
    }

    try {
      await mongoose.connect(mongoUri, {
        // Connection timeouts
        serverSelectionTimeoutMS: 60000,
        connectTimeoutMS: 60000,
        socketTimeoutMS: 90000,

        // Connection pool optimization for high throughput (100+ QPS)
        maxPoolSize: Number(process.env.MONGO_MAX_POOL_SIZE) || 100, // Increased to handle high concurrency
        minPoolSize: Number(process.env.MONGO_MIN_POOL_SIZE) || 30, // More warm connections for faster startup
        maxIdleTimeMS: 60000, // Keep connections open longer for frequent queries
        waitQueueTimeoutMS: 60000, // Max wait time for connection from pool (increased from 10s to handle event loop blocks)

        // Performance optimizations
        retryWrites: true,
        family: 4, // Use IPv4, skip trying IPv6
        heartbeatFrequencyMS: 10000, // Reduced from 5000 to default for less network traffic

        // Write concern for performance (faster writes, less durability)

        // Buffer settings for immediate error feedback
        bufferCommands: false,  // Disable command buffering for immediate errors

        // Compression for reduced network traffic
        compressors: ['zlib'],

        // Write concern optimization for performance  
        writeConcern: {
          w: 1      // Wait for acknowledgment from primary only
        }
      });

      // Wait for connection to be fully ready
      let retries = 0;
      const maxRetries = 30; // 30 seconds max wait
      while (mongoose.connection.readyState !== 1 && retries < maxRetries) {
        await new Promise(resolve => setTimeout(resolve, 1000));
        retries++;
      }

      if (mongoose.connection.readyState !== 1) {
        throw new Error('MongoDB connection not ready after 30 seconds');
      }

      // Give connection pool time to warm up (establish minPoolSize connections)
      await new Promise(resolve => setTimeout(resolve, 2000));

      // Add connection pool monitoring for high-throughput operations
      mongoose.connection.on('disconnected', () => {
      });

      mongoose.connection.on('reconnected', () => {
      });

      mongoose.connection.on('error', (err) => {
        errorLog('MongoDBConnection', 'MongoDB connection error', err);
      });

      // Connection pool monitoring (added 2025-12-03 to diagnose pool exhaustion)
      const mongoClient = mongoose.connection.getClient();

      // Log pool metrics every minute when DEBUG=true
      if (DEBUG) {
        setInterval(() => {
          try {
            // Cast to any to access internal MongoDB driver topology properties
            const poolStats = (mongoClient as any)?.topology?.s?.pool;
            if (poolStats) {
            }
          } catch (error) {
            // Silent fail - pool monitoring is non-critical
          }
        }, 60000); // Log every minute
      }

      // Log connection pool stats in development
      if (process.env.NODE_ENV === 'development') {
        setInterval(() => {
          const readyState = mongoose.connection.readyState;
          const stateMap = { 0: 'disconnected', 1: 'connected', 2: 'connecting', 3: 'disconnecting' };
        }, STANDARD_INTERVAL);
      }
    } catch (error) {
      errorLog('Database', `Failed to connect to MongoDB: ${mongoUri.replace(/\/\/[^:]+:[^@]+@/, '//***:***@')}`, error instanceof Error ? error : new Error(String(error)));
      throw error;
    }

    // Detect if previous shutdown was graceful or a crash
    const shutdownType = await detectPreviousShutdown();
    logLifecycleEvent('SHUTDOWN_TYPE_DETECTED', { type: shutdownType });

    // Reset all running tasks and listings on server restart
    // This ensures a clean state regardless of how the server was stopped
    try {
      const stoppedTasks = await Task.updateMany(
        { running: true },
        { $set: { running: false } }
      );

      const stoppedListings = await Listing.updateMany(
        { running: true },
        { $set: { running: false } }
      );

      if (stoppedTasks.modifiedCount > 0 || stoppedListings.modifiedCount > 0) {
        console.log(`🔄 Server restart detected - stopped ${stoppedTasks.modifiedCount} running tasks and ${stoppedListings.modifiedCount} running listings`);
        if (shutdownType === 'crash') {
          console.log('💡 Crash recovery: Tasks will be auto-resumed after password verification');
        }
      }
    } catch (error) {
      errorLog('ServerRestart', 'Error stopping tasks/listings on server restart', error);
    }

    // Try to recover password (file in production, Redis in development)
    let passwordRecovered = false;
    const maxRetries = 3;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      // Check BullMQ connection health (needed for development recovery)
      const bullmqHealthy = await checkRedisHealth();
      if (!bullmqHealthy) {
        await new Promise(resolve => setTimeout(resolve, 1000));
        continue;
      }

      // Development only: try to recover password from Redis
      if (process.env.NODE_ENV !== 'production') {
        passwordRecovered = await recoverPasswordFromRedis();
      }
      // Production: no password persistence, always requires manual entry

      if (passwordRecovered) {
        break;
      } else if (attempt < maxRetries) {
        await new Promise(resolve => setTimeout(resolve, attempt * 1000));
      }
    }

    // Check if password is set (either from recovery or needs to be set)
    const currentPassword = getPassword();
    if (!currentPassword) {
      // Don't start jobs if there's no password
      console.log('⏸️  No password available - server in paused state');
      console.log('💡 Server will remain in paused state until password is provided via API');
    } else {
      if (passwordRecovered) {
        console.log('✅ Password recovered from Redis (development) - operations will resume');

        // If crash recovery and password available, resume previously running tasks (dev only)
        if (shutdownType === 'crash') {
          console.log('🔄 Crash recovery: Auto-resuming previously running tasks...');
          await resumePreviouslyRunningTasks();
        }
      } else {
        console.log('✅ Password available - operations will resume');
      }
    }

    // Clear queue and restart all jobs on server start - AFTER MongoDB is connected and password check
    await clearQueueAndRestartJobs();

    // If no password was found, ensure queues and workers are paused
    if (!currentPassword) {
      await pauseQueueAndWorkers();
    } else {
      // Verify all workers are ready before processing jobs

      // Give workers time to fully initialize
      await new Promise(resolve => setTimeout(resolve, 2000));

      // Check worker health
      const workerStatuses: Array<{
        index: number;
        running: boolean;
        paused: boolean;
        healthy: boolean;
        error?: any;
      }> = [];

      // Check all workers
      workers.forEach((worker, index) => {
        try {
          workerStatuses.push({
            index,
            running: worker.isRunning(),
            paused: worker.isPaused(),
            healthy: worker.isRunning() && !worker.isPaused()
          });
        } catch (error) {
          workerStatuses.push({
            index,
            running: false,
            paused: false,
            healthy: false,
            error
          });
        }
      });

      const unhealthyWorkers = workerStatuses.filter(s => !s.healthy);
      if (unhealthyWorkers.length > 0) {
        errorLog('WorkerHealth', `${unhealthyWorkers.length}/${workers.length} workers are not ready`);
        unhealthyWorkers.forEach(w => {
          errorLog('WorkerHealth', `Worker ${w.index}: running=${w.running}, paused=${w.paused}`);
        });

        // Try to resume paused workers
        for (const workerStatus of unhealthyWorkers) {
          if (!workerStatus.running || workerStatus.paused) {
            try {
              if (workerStatus.index < workers.length) {
                await workers[workerStatus.index].resume();
              }
            } catch (error) {
              errorLog('WorkerResume', `Failed to resume worker ${workerStatus.index}`, error);
            }
          }
        }
      } else {
      }

      // Verify Redis connections one more time
      const finalHealthCheck = await checkBullMQConnectionsHealth();
      const finalUnhealthy = Object.entries(finalHealthCheck).filter(([_, status]) => !status.healthy);

      if (finalUnhealthy.length > 0) {
        errorLog('RedisHealth', 'Some Redis connections are still unhealthy');
        finalUnhealthy.forEach(([id, status]) => {
          errorLog('HealthCheck', `Redis connection ${id}: ${status.error}`);
        });
      } else {
      }
    }

    try {
      await initialize(RATE_LIMIT);
      // Initialize wallet cache with pre-decryption
      const password = await getPassword();
      if (password) {
        await walletCache.initialize(password);
      }
    } catch (redisError) {
      errorLog('RedisOperation', 'Redis operation failed', redisError);
    }

    // ===== DATABASE INITIALIZATION (BEFORE server.listen()) =====
    // All database operations must complete before accepting HTTP connections
    // This prevents "MongoNotConnectedError" race conditions on startup

    // Initialize WebSocket connection monitoring
    try {
      startConnectionMonitoring();
    } catch (error) {
      errorLog('STARTUP', 'Failed to initialize WebSocket connection monitoring', error as Error);
    }

    // Connect WebSocket before initializing listing scheduler
    try {
      await connectWebSocket();
    } catch (error) {
      errorLog('WebSocket', 'Failed to connect to marketplace WebSocket', error as Error);
    }

    // Initialize listing scheduler with WebSocket manager (only if password available)
    // This queries the Listing collection
    if (currentPassword) {
      try {
        await initializeListingScheduler(wsManager);
      } catch (error) {
        errorLog('STARTUP', 'Failed to initialize listing scheduler', error as Error);
      }
    } else {
      console.log('⏸️  Listing scheduler skipped - password required');
    }

    // Initialize expiration monitor
    expirationMonitorInterval = initializeExpirationMonitor();

    // Initialize expired price cleanup (queries and updates Listing collection immediately)
    try {
      expiredPriceCleanupInterval = initializeExpiredPriceCleanup();
    } catch (error) {
      errorLog('STARTUP', 'Failed to initialize expired price cleanup', error as Error);
    }

    // Initialize orphaned job recovery system
    orphanedJobRecovery.startRecovery(30000); // Check every 30 seconds

    // Start internet connectivity monitoring
    startConnectivityMonitoring();

    // Fetch current tasks from database
    try {
      await fetchCurrentTasks();
    } catch (error) {
      errorLog('STARTUP', 'Failed to fetch current tasks from database', error as Error);
    }

    // Warm all task collections (deduplicates automatically: 100 tasks → 30 collections)
    try {
      const allTasks = Array.from(activeTasks.values());
      collectionDataWarmer.warmAllTaskCollections(allTasks);
    } catch (error) {
      errorLog('STARTUP', 'Failed to initialize collection warming', error as Error);
    }

    // Initialize bid counts from database immediately after loading tasks
    // This ensures bidStats is populated before clients connect (fixes Active:0 bug on server restart)
    try {
      await validateBidCount();
      console.log('✅ Bid counts initialized from database');
    } catch (error) {
      errorLog('STARTUP', 'Failed to initialize bid counts from database', error as Error);
    }

    // Initialize periodic intervals AFTER MongoDB connection is established
    // These were previously top-level causing "Client must be connected" errors on startup
    validateBidCountInterval = setInterval(validateBidCount, STANDARD_INTERVAL);
    setInterval(logJobTrackingStatus, STANDARD_INTERVAL);

    // Direct WebSocket broadcast manager initialized
    console.log('✅ Direct WebSocket broadcast manager initialized');

    // ===== START HTTP SERVER =====
    // All database operations complete - safe to accept connections
    server.listen(port, () => {
      console.log(`Server running on port ${port}`);
      console.log(`Environment: ${process.env.NODE_ENV || 'development'}`);
    });

    // Event Loop Lag Monitoring - detects when event loop is blocked
    // This helps identify why the client might not read from WebSocket fast enough
    let lastEventLoopCheck = Date.now();
    const EVENT_LOOP_CHECK_INTERVAL = 1000; // Check every 1 second
    const EVENT_LOOP_LAG_WARNING_THRESHOLD = 100; // Warn if lag > 100ms
    const EVENT_LOOP_LAG_CRITICAL_THRESHOLD = 500; // Critical if lag > 500ms

    setInterval(() => {
      const now = Date.now();
      const expectedTime = lastEventLoopCheck + EVENT_LOOP_CHECK_INTERVAL;
      const lag = now - expectedTime;

      if (lag > EVENT_LOOP_LAG_CRITICAL_THRESHOLD) {
        warnLog('EventLoop', `CRITICAL lag detected: ${lag}ms - event loop severely blocked | This may cause WebSocket backpressure`);
      } else if (lag > EVENT_LOOP_LAG_WARNING_THRESHOLD) {
      }

      lastEventLoopCheck = now;
    }, EVENT_LOOP_CHECK_INTERVAL);

    // Initialize periodic cleanup for aborted tasks (every 30 minutes)
    setInterval(() => {
      cleanupAbortedTasks();
    }, 30 * 60 * 1000); // 30 minutes

    // Initialize memory monitoring (every 15 minutes)
    const FIFTEEN_MINUTES = 15 * 60 * 1000;

    // Initial snapshot (30 seconds after startup)
    setTimeout(async () => {
      try {
        await memorySnapshotService.takeSnapshot();
      } catch (error) {
        errorLog('MemoryMonitor', 'Initial snapshot failed', error as Error);
      }
    }, 30000);

    // Recurring snapshots every 15 minutes
    setInterval(async () => {
      try {
        await memorySnapshotService.takeSnapshot();
      } catch (error) {
        errorLog('MemoryMonitor', 'Scheduled snapshot failed', error as Error);
      }
    }, FIFTEEN_MINUTES);


    // Initialize periodic cleanup for stale failed task entries (every hour)
    setInterval(() => {
      const oneHourAgo = Date.now() - 3600000;
      let removedCount = 0;

      // Clean taskPreprocessing (stale > 1 hour)
      for (const [taskId, data] of taskPreprocessing.entries()) {
        if ((data as any).timestamp && (data as any).timestamp < oneHourAgo) {
          taskPreprocessing.delete(taskId);
          removedCount++;
        }
      }

      // Clean failed queues (stale > 1 hour or task no longer exists)
      for (const taskId of failedTaskStartQueue.keys()) {
        if (!taskStatus[taskId]) {
          failedTaskStartQueue.delete(taskId);
          failedTaskStopQueue.delete(taskId);
          queueingProgress.delete(taskId);
          removedCount++;
        }
      }

      if (removedCount > 0) {
      }
    }, 3600000); // Every hour


    // EMERGENCY FIX: Removed manual collectionFloorCache cleanup interval
    // LRU cache with TTL handles cleanup automatically, no need for manual 5-minute scans
    // This eliminates periodic full-Map iterations that contributed to event loop blocking

    // Start all tasks that have running: true in database
    // This handles server restarts where tasks were previously running
    let tasksToRestart = 0;
    for (const [taskId, task] of activeTasks) {
      if (task.running) {
        tasksToRestart++;
        // Initialize task status if not exists
        if (!taskStatus[taskId]) {
          const task = activeTasks.get(taskId);
          taskStatus[taskId] = {
            selectedMarketplaces: task?.selectedMarketplaces.map(m => m.toLowerCase()) || [],
            lastUpdate: Date.now(),
            marketplaces: {}
          };
        }

        // Start the task with a small delay to ensure everything is initialized
        setTimeout(() => {
          startTask(task, true);
        }, tasksToRestart * 1000); // Stagger task starts by 1 second each
      }
    }

    if (tasksToRestart > 0) {
    } else {
    }

    // Initialize latency snapshot service (5-minute intervals)
    await startPeriodicSnapshots();

    // Check for orphaned scheduled tasks (tasks stuck in SCHEDULED state without timers)
    const orphanedScheduledTasks = Array.from(activeTasks.entries())
      .filter(([taskId, task]) => {
        const status = taskStatus[taskId];

        // Check if any marketplace is in SCHEDULED state
        const scheduledMarketplaces = status?.marketplaces ?
          Object.entries(status.marketplaces)
            .filter(([_, mpStatus]) => mpStatus?.status === 'SCHEDULED')
            .map(([mp]) => mp) : [];

        if (scheduledMarketplaces.length === 0) return false;

        // FIX: Check if ANY scheduled marketplace is missing its timer
        // Timers are stored with keys like `${taskId}-${marketplace}`, not just `taskId`
        const hasMissingTimer = scheduledMarketplaces.some(mp => {
          const timerKey = `${taskId}-${mp}`;
          return !taskTimeouts.has(timerKey);
        });

        return task.running && hasMissingTimer;
      });

    if (orphanedScheduledTasks.length > 0) {

      for (const [taskId, task] of orphanedScheduledTasks) {
        const timeSinceLastUpdate = taskStatus[taskId]?.lastUpdate ?
          Date.now() - taskStatus[taskId].lastUpdate : 'unknown';

        const marketplaceStatuses = taskStatus[taskId]?.marketplaces;
        const totalJobs = marketplaceStatuses ? Object.values(marketplaceStatuses).reduce((sum, mp) => sum + (mp?.totalJobs || 0), 0) : 0;
        const completedJobs = marketplaceStatuses ? Object.values(marketplaceStatuses).reduce((sum, mp) => sum + (mp?.completedJobs || 0), 0) : 0;

        // Initialize task status if needed
        if (!taskStatus[taskId]) {
          const task = activeTasks.get(taskId);
          taskStatus[taskId] = {
            selectedMarketplaces: task?.selectedMarketplaces.map(m => m.toLowerCase()) || [],
            lastUpdate: Date.now(),
            marketplaces: {}
          };
        }

        // Restart the orphaned task immediately
        setTimeout(() => {
          startTask(task, true);
        }, 500); // Small delay to ensure system is ready
      }
    } else {
    }

    // Start all listings that have running: true in database
    // This handles server restarts where listings were previously running
    try {
      const runningListings = await Listing.find({ running: true });
      let listingsToRestart = runningListings.length;

      if (listingsToRestart > 0) {

        for (let i = 0; i < runningListings.length; i++) {
          const listing = runningListings[i];
          // Stagger listing starts by 500ms each to avoid overwhelming the system
          setTimeout(async () => {
            try {
              await scheduleListing(listing);
            } catch (error) {
              errorLog('ListingRestart', `Failed to restart listing ${listing._id}`, error);
            }
          }, i * 500);
        }
      } else {
      }
    } catch (error) {
      errorLog('ListingFetch', 'Failed to fetch running listings', error);
    }

    // Start password auto-recovery checker now that Redis is connected
    setInterval(checkPasswordAndResume, PASSWORD_CHECK_INTERVAL);


    // NOTE: All recovery intervals removed - recovery logic was causing issues
    // Removed: checkStuckTasks, verifyScheduledTaskTimers, cleanupStuckJobs,
    //          taskScheduleWatchdog, queueingTaskWatchdog, cleanupOldSchedules

    // Start failed task start/stop retry loops
    const TASK_RETRY_INTERVAL = 5000; // 5 seconds
    setInterval(processFailedTaskRetries, TASK_RETRY_INTERVAL);
    setInterval(processFailedTaskStopRetries, TASK_RETRY_INTERVAL);

    // Cleanup stale floor cache entries every 5 minutes
    const FLOOR_CACHE_CLEANUP_INTERVAL = 5 * 60 * 1000; // 5 minutes
    const MAX_FLOOR_CACHE_AGE = 10 * 60 * 1000; // 10 minutes
    setInterval(() => {
      const now = Date.now();
      let cleaned = 0;
      for (const [key, state] of collectionFloorCache.entries()) {
        if ((now - state.lastFetched) > MAX_FLOOR_CACHE_AGE) {
          collectionFloorCache.delete(key);
          cleaned++;
        }
      }
      if (cleaned > 0) {
      }
    }, FLOOR_CACHE_CLEANUP_INTERVAL);

    // Start periodic message loss detection logging
    const MSG_LOSS_CHECK_INTERVAL = 60000; // 60 seconds
    setInterval(() => {
      const wsReceived = wsManager?.getOpenseaBidCreatedReceived?.() || 0;
      const ownBidsCounted = ownBidCounts['opensea'] || 0;
      if (wsReceived > 0 || ownBidsCounted > 0) {
        const gap = wsReceived - ownBidsCounted;
      }
    }, MSG_LOSS_CHECK_INTERVAL);

    // Start periodic circuit breaker stats logging and cleanup
    const CIRCUIT_BREAKER_STATS_INTERVAL = 60000; // 60 seconds
    setInterval(() => {
      const openseaStats = openseaFloorBreakerRegistry.getStats();

      // Only log if there are open or half-open breakers (indicates issues)
      if (openseaStats.open > 0 || openseaStats.halfOpen > 0) {
        warnLog('CircuitBreaker', `STATS | registry=opensea:floor | total=${openseaStats.total} | open=${openseaStats.open} | halfOpen=${openseaStats.halfOpen} | closed=${openseaStats.closed}`);
      }

      // Cleanup stale breakers (closed state, no activity for 5 minutes)
      openseaFloorBreakerRegistry.cleanup();
    }, CIRCUIT_BREAKER_STATS_INTERVAL);

    // Start periodic orphan scan for parentJobs/processedJobsPerParent
    // Removes entries for tasks that no longer exist (safety net for edge cases)
    const ORPHAN_SCAN_INTERVAL = 5 * 60 * 1000; // 5 minutes
    setInterval(() => {
      const activeTaskIds = new Set(activeTasks.keys());
      let orphanedCount = 0;

      for (const [key, value] of parentJobs.entries()) {
        const taskId = value.taskId;
        if (!activeTaskIds.has(taskId)) {
          parentJobs.delete(key);
          processedJobsPerParent.delete(key);
          orphanedCount++;
        }
      }

      if (orphanedCount > 0) {
      }
    }, ORPHAN_SCAN_INTERVAL);

    // Start periodic queue stats logging for monitoring queue health
    const QUEUE_STATS_INTERVAL = 30000; // 30 seconds
    setInterval(async () => {
      try {
        const mainWaiting = await queue.getWaitingCount();
        const mainActive = await queue.getActiveCount();
        const mainDelayed = await queue.getDelayedCount();

        // Always log queue stats for monitoring (useful for diagnosing stuck tasks)

        // Warn if queue is backing up
        if (mainWaiting > 1000) {
          warnLog('QueueStats', `HIGH_BACKLOG | mainQueue: waiting=${mainWaiting}`);
        }
      } catch (error) {
        errorLog('QueueStats', 'Failed to fetch queue stats', error);
      }
    }, QUEUE_STATS_INTERVAL);

    // Load wallet addresses for self-bid detection
    await loadOurWalletAddresses();


  } catch (error) {
    errorLog('ServerStartup', 'Failed to start server', error);
    process.exit(1);
  }
}

// Cleanup handler for graceful shutdown
process.on('SIGINT', async () => {
  logSignal('SIGINT');

  // Save running tasks BEFORE stopping them (for crash recovery)
  await saveRunningTasksBeforeShutdown();

  // Update all running tasks to not running
  try {
    const result = await Task.updateMany(
      { running: true },
      { running: false }
    );
  } catch (error) {
    errorLog('TaskStateUpdate', 'Error updating task states', error);
  }

  // Stop connectivity monitoring
  stopConnectivityMonitoring();

  // Stop background collection data warming
  collectionDataWarmer.stopBackgroundWarming();

  // Stop memory cleanup service
  memoryCleanupService.stopCleanup();

  // Shutdown listing scheduler
  await shutdownListingScheduler();

  // Stop expiration monitor
  if (expirationMonitorInterval) {
    stopExpirationMonitor(expirationMonitorInterval);
  }

  // Stop expired price cleanup
  if (expiredPriceCleanupInterval) {
    stopExpiredPriceCleanup(expiredPriceCleanupInterval);
  }

  // Stop orphaned job recovery
  orphanedJobRecovery.stopRecovery();

  // Stop job reconciliation service

  // Redis cluster monitoring disabled - using single node
  // stopRedisClusterMonitoring();

  // Stop validate bid count interval
  if (validateBidCountInterval) {
    clearInterval(validateBidCountInterval);
  }

  // Stop memory monitor
  stopMemoryMonitor();

  // Stop proactive GC
  stopProactiveGC();

  // Shutdown latency trackers
  shutdownForwardingLatencyTracker();
  shutdownExecutionLatencyTracker();

  // Stop latency snapshot service
  await stopPeriodicSnapshots();

  // Disconnect WebSocket and remove all listeners
  if (wsManager) {
    wsManager.removeAllListeners();
    wsManager.disconnect();
  }

  // Cleanup broadcast manager
  broadcastManager.destroy();

  // Clean up all active tasks (memory leak fix)
  for (const taskId of activeTasks.keys()) {
    try {
      cleanupTaskResources(taskId);
    } catch (e) {
      warnLog('Lifecycle', `Failed to cleanup task ${taskId}: ${e}`);
    }
  }

  // Close all BullMQ connections
  for (const connection of bullMQConnections) {
    try {
      await connection.quit();
    } catch (error) {
      errorLog('ConnectionClose', 'Error closing connection', error);
    }
  }

  // Final cleanup before exit
  seenOrderHashes.clear();

  // Clear task status history (memory leak fix - 2025-12-24)
  for (const taskId in taskStatusHistory) {
    delete taskStatusHistory[taskId];
  }

  logProcessExit(0, 'SIGINT graceful shutdown');
  process.exit(0);
});

process.on('SIGTERM', async () => {
  logSignal('SIGTERM');

  // Save running tasks BEFORE stopping them (for crash recovery)
  await saveRunningTasksBeforeShutdown();

  // Update all running tasks to not running
  try {
    const result = await Task.updateMany(
      { running: true },
      { running: false }
    );
  } catch (error) {
    errorLog('TaskStateUpdate', 'Error updating task states', error);
  }

  // Stop connectivity monitoring
  stopConnectivityMonitoring();

  // Stop background collection data warming
  collectionDataWarmer.stopBackgroundWarming();

  // Stop memory cleanup service
  memoryCleanupService.stopCleanup();

  // Shutdown listing scheduler
  await shutdownListingScheduler();

  // Stop expiration monitor
  if (expirationMonitorInterval) {
    stopExpirationMonitor(expirationMonitorInterval);
  }

  // Stop expired price cleanup
  if (expiredPriceCleanupInterval) {
    stopExpiredPriceCleanup(expiredPriceCleanupInterval);
  }

  // Stop orphaned job recovery
  orphanedJobRecovery.stopRecovery();

  // Redis cluster monitoring disabled - using single node
  // stopRedisClusterMonitoring();

  // Stop validate bid count interval
  if (validateBidCountInterval) {
    clearInterval(validateBidCountInterval);
  }

  // Stop memory monitor
  stopMemoryMonitor();

  // Stop proactive GC
  stopProactiveGC();

  // Shutdown latency trackers
  shutdownForwardingLatencyTracker();
  shutdownExecutionLatencyTracker();

  // Stop latency snapshot service
  await stopPeriodicSnapshots();

  // Disconnect WebSocket and remove all listeners
  if (wsManager) {
    wsManager.removeAllListeners();
    wsManager.disconnect();
  }

  // Cleanup broadcast manager
  broadcastManager.destroy();

  // Clean up all active tasks (memory leak fix)
  for (const taskId of activeTasks.keys()) {
    try {
      cleanupTaskResources(taskId);
    } catch (e) {
      warnLog('Lifecycle', `Failed to cleanup task ${taskId}: ${e}`);
    }
  }

  // Close all BullMQ connections
  for (const connection of bullMQConnections) {
    try {
      await connection.quit();
    } catch (error) {
      errorLog('ConnectionClose', 'Error closing connection', error);
    }
  }

  seenOrderHashes.clear();

  // Clear task status history (memory leak fix - 2025-12-24)
  for (const taskId in taskStatusHistory) {
    delete taskStatusHistory[taskId];
  }

  logProcessExit(0, 'SIGTERM graceful shutdown');
  process.exit(0);
});

startServer()
  .catch(async error => {
    errorLog('General', 'Failed to start server', error as Error);
    errorLog('ServerStartup', 'Failed to start server', error);
  });

export const rps = {
  currentRPS: 0,
  maxRPS: RATE_LIMIT ?? 0,
}
const BID_WINDOW_SECONDS = 10;

let bidRates = {
  opensea: { bidsPerSecond: 0, totalBids: 0, windowPeriod: BID_WINDOW_SECONDS },
  blur: { bidsPerSecond: 0, totalBids: 0, windowPeriod: BID_WINDOW_SECONDS },
  magiceden: { bidsPerSecond: 0, totalBids: 0, windowPeriod: BID_WINDOW_SECONDS }
}

// Helper function to determine if a task should be in STOPPED status
// A task should only be STOPPED when BOTH conditions are met:
// 1. task.running === false
// 2. cancellationPercentage === 100
// 3. No active orders exist in the database (when at 100%)
async function shouldTaskBeStopped(taskId: string): Promise<boolean> {
  const task = activeTasks.get(taskId);
  const status = taskStatus[taskId];

  if (!status) {
    return false;
  }

  const isNotRunning = !task || !task.running;


  // Check if ALL marketplaces have completed cancellation (100%)
  const marketplaceStatuses = status.marketplaces;


  // FIXED: Change >= 100 to === 100 to match enhanced system
  const allMarketplacesCancelled = marketplaceStatuses && Object.values(marketplaceStatuses).every(mp =>
    mp && (mp.cancellationPercentage === 100 || mp.status === 'COMPLETED' || mp.status === 'STOPPED')
  );


  // If all marketplaces cancelled, verify no active orders exist
  if (allMarketplacesCancelled) {
    try {
      const activeOrderCount = await orderCache.countByTask(taskId);

      if (activeOrderCount > 0) {
        return false;
      }
    } catch (error) {
      errorLog('TaskStatusCheck', `Error checking active orders for task ${taskId}`, error);
      // If we can't verify, don't transition to STOPPED
      return false;
    }
  }

  const shouldStop = isNotRunning && allMarketplacesCancelled;

  if (shouldStop) {
  }

  return shouldStop;
}

// Helper function to ensure task transitions to STOPPED when cancellation is complete
async function setTaskCancellationComplete(taskId: string) {

  const task = activeTasks.get(taskId)

  // Only proceed if taskStatus exists (don't create new ones here)
  if (!taskStatus[taskId]) {
    return;
  }

  // Let the cancellation tracker handle percentage updates naturally

  // For global cancellation completion, transition ONLY marketplaces that are actually 100% complete
  const cancellingMarketplaces = Object.entries(taskStatus[taskId]?.marketplaces || {})
    .filter(([_, status]) => status?.status === 'CANCELLING')
    .map(([mp, _]) => mp);


  // CRITICAL BUG FIX: Only transition marketplaces that are actually at 100% completion
  const marketplacesToStop = [];
  for (const mp of cancellingMarketplaces) {
    const marketplaceStatus = taskStatus[taskId]?.marketplaces[mp as keyof typeof taskStatus[string]['marketplaces']];
    const cancellationPercentage = marketplaceStatus?.cancellationPercentage || 0;


    // FIXED: Only transition when exactly 100% complete OR no orders to cancel (0 total jobs)
    if (cancellationPercentage === 100 || (marketplaceStatus?.totalJobs === 0 && marketplaceStatus?.completedJobs === 0)) {
      marketplacesToStop.push(mp);
    } else {
    }
  }


  // Set only validated marketplaces to STOPPED
  for (const mp of marketplacesToStop) {
    setTaskStatus(taskId, 'STOPPED', 'LEGACY: marketplace 100% complete', mp);
    if (taskStatus[taskId].marketplaces[mp as keyof typeof taskStatus[string]['marketplaces']]) {
      delete taskStatus[taskId].marketplaces[mp as keyof typeof taskStatus[string]['marketplaces']]!.nextLoopTime;
      taskStatus[taskId].marketplaces[mp as keyof typeof taskStatus[string]['marketplaces']]!.waitingTime = 0;
    }
  }

  // Clean up cancellation operation tracking
  const cancellationOp = activeCancellationOperations.get(taskId);
  if (cancellationOp) {
    activeCancellationOperations.delete(taskId);
  }
}


// Helper function to safely set task status with regression protection and validation
function setTaskStatus(taskId: string, newStatus: string, context?: string, marketplace?: string): boolean {
  const timestamp = new Date().toISOString();
  const stackTrace = new Error().stack?.split('\n').slice(2, 5).join('\n    ');

  // TaskFlow logging: Capture state transition attempt - but skip no-op transitions
  const mpKey = marketplace?.toLowerCase() as 'opensea' | 'magiceden' | 'blur' | undefined;
  const oldStatus = mpKey ? taskStatus[taskId]?.marketplaces?.[mpKey]?.status : 'INIT';
  const caller = stackTrace?.split('\n')[0]?.trim().substring(0, 80) || 'unknown';

  // Skip logging for no-op transitions (STARTED→STARTED, QUEUEING→QUEUEING, etc.)
  // This was generating 6K+ redundant log lines/day
  const isNoOpTransition = oldStatus === newStatus;
  if (!isNoOpTransition) {
  }

  if (!taskStatus[taskId]) {

    const task = activeTasks.get(taskId);
    const selectedMarketplaces = task?.selectedMarketplaces || [];

    taskStatus[taskId] = {
      selectedMarketplaces: selectedMarketplaces.map(m => m.toLowerCase()),
      lastUpdate: Date.now(),
      marketplaces: {}
    };

    // Initialize marketplace status for selected marketplaces
    selectedMarketplaces.forEach(m => {
      const marketplaceName = m.toLowerCase() as 'opensea' | 'magiceden' | 'blur';
      taskStatus[taskId].marketplaces[marketplaceName] = {
        status: 'STARTED',
        runningPercentage: 0,
        cancellationPercentage: 0,
        processingPercentage: 0,
        totalJobs: 0,
        completedJobs: 0,
        lastUpdate: Date.now()
      };
    });

    // Store status change history
    if (!taskStatusHistory[taskId]) {
      taskStatusHistory[taskId] = [];
    }
    taskStatusHistory[taskId].push({
      timestamp,
      from: 'NONE',
      to: newStatus,
      context: context || 'Initial creation',
      marketplace,
      stack: stackTrace
    });

    // Trim to prevent unbounded memory growth (keep last 100 entries)
    const MAX_STATUS_HISTORY = 100;
    if (taskStatusHistory[taskId].length > MAX_STATUS_HISTORY) {
      taskStatusHistory[taskId] = taskStatusHistory[taskId].slice(-MAX_STATUS_HISTORY);
    }

    // TaskFlow logging: Initial task status created

    return true;
  }

  // Handle marketplace-specific update
  if (marketplace) {
    const marketplaceName = marketplace.toLowerCase() as 'opensea' | 'magiceden' | 'blur';

    // Initialize marketplace status if not exists
    if (!taskStatus[taskId].marketplaces[marketplaceName]) {
      taskStatus[taskId].marketplaces[marketplaceName] = {
        status: 'STARTED',
        runningPercentage: 0,
        cancellationPercentage: 0,
        processingPercentage: 0,
        totalJobs: 0,
        completedJobs: 0,
        lastUpdate: Date.now()
      };
    }

    const currentMarketplaceStatus = taskStatus[taskId].marketplaces[marketplaceName]!.status;

    // Skip redundant state transitions to avoid log spam
    if (currentMarketplaceStatus === newStatus) {
      // Silently skip redundant transitions
      return true;
    }

    // Validate state transitions to prevent invalid flows
    // Allow STOPPED -> CANCELLING transition for stopTask flow
    const invalidTransitions: { [key: string]: string[] } = {
      'COMPLETED': ['RUNNING', 'PROCESSING', 'QUEUEING', 'QUEUED'], // Can only go to SCHEDULED or STOPPED
      'SCHEDULED': ['RUNNING', 'PROCESSING', 'QUEUEING', 'QUEUED', 'COMPLETED'] // Can only go to STARTED or STOPPED
    };

    // Special case: Allow STOPPED -> CANCELLING transition
    const isStopTaskTransition = currentMarketplaceStatus === 'STOPPED' && newStatus === 'CANCELLING';

    if (!isStopTaskTransition && invalidTransitions[currentMarketplaceStatus]?.includes(newStatus)) {
      // TaskFlow logging: Log blocked transition
      warnLog('StateTransition',
        `BLOCKED: ${currentMarketplaceStatus} → ${newStatus} | taskId=${taskId} | mp=${marketplace} | invalid_transition`,
        { taskId, marketplace: marketplaceName, oldStatus: currentMarketplaceStatus, newStatus, reason: 'invalid_transition', context }
      );
      errorLog('TaskStatusTransition', `Invalid state transition: ${currentMarketplaceStatus} -> ${newStatus} for task ${taskId}/${marketplace}`, undefined);
      errorLog('TaskStatusTransition', `Context: ${context || 'No context provided'}`);
      errorLog('TaskStatusTransition', `Called from: ${stackTrace}`);
      return false;
    }

    // CRITICAL DEBUG LOG FOR STOPPED TRANSITIONS
    if (newStatus === 'STOPPED') {

      // Get current cancellation percentage
      const currentCancelPct = taskStatus[taskId].marketplaces[marketplaceName]?.cancellationPercentage || 0;
    }

    const loopInfo = loopCounterStats[taskId]?.[marketplace] ? ` (Loop #${loopCounterStats[taskId][marketplace]})` : '';

    // Update marketplace status
    taskStatus[taskId].marketplaces[marketplaceName]!.status = newStatus as MarketplaceStatus;
    taskStatus[taskId].marketplaces[marketplaceName]!.lastUpdate = Date.now();

    // Clean up queueing progress tracking when transitioning FROM QUEUEING
    // This is the ONLY place where progress tracking should be cleaned up
    if (currentMarketplaceStatus === 'QUEUEING' && newStatus !== 'QUEUEING') {
      const progressKey = `${taskId}-${marketplaceName}`;
      if (queueingProgress.delete(progressKey)) {
      }
    }

    // Store status change history
    if (!taskStatusHistory[taskId]) {
      taskStatusHistory[taskId] = [];
    }
    taskStatusHistory[taskId].push({
      timestamp,
      from: currentMarketplaceStatus,
      to: newStatus,
      context: context || 'No context provided',
      marketplace,
      stack: stackTrace
    });

    // Trim to prevent unbounded memory growth (keep last 100 entries)
    const MAX_STATUS_HISTORY = 100;
    if (taskStatusHistory[taskId].length > MAX_STATUS_HISTORY) {
      taskStatusHistory[taskId] = taskStatusHistory[taskId].slice(-MAX_STATUS_HISTORY);
    }


    // Persist task state to MongoDB for QUEUEING watchdog recovery
    // This allows recovery of tasks stuck in QUEUEING state after server restart
    if (['QUEUEING', 'RUNNING', 'COMPLETED', 'SCHEDULED', 'STOPPED'].includes(newStatus)) {
      const taskStateUpdate: Record<string, unknown> = {
        'taskState.status': newStatus,
        'taskState.lastActivityAt': new Date(),
        accessedAt: new Date()
      };

      // Track when task entered QUEUEING for timeout detection
      if (newStatus === 'QUEUEING') {
        taskStateUpdate['taskState.queuedAt'] = new Date();
      }
    }

    return true;
  }

  // Marketplace parameter is required
  errorLog('TaskStatus', `Marketplace parameter is required for task ${taskId}`, undefined);
  errorLog('TaskStatus', `Called from: ${stackTrace}`);
  return false;
}


/**
 * Helper function to verify if a task has jobs queued
 */
function verifyTaskHasJobs(taskId: string): boolean {
  const task = activeTasks.get(taskId);
  if (!task) return false;

  let totalJobs = 0;

  // Check each marketplace's parent job
  for (const marketplace of task.selectedMarketplaces) {
    const parentJobId = getParentJobId(taskId, marketplace.toLowerCase());
    const parentJob = parentJobs.get(parentJobId);

    if (parentJob) {
      // Check actualJobs if available, otherwise totalJobs
      const jobCount = parentJob.actualJobs || parentJob.totalJobs || 0;
      totalJobs += jobCount;
    }
  }

  return totalJobs > 0;
}

// Helper function for marketplace-specific cancellation completion
async function setMarketplaceCancellationComplete(taskId: string, marketplace?: string) {
  // Only proceed if taskStatus exists (don't create new ones here)
  if (!taskStatus[taskId]) {
    return;
  }

  // Clean up no-progress counter
  // Removed cancellationNoProgressCount.delete(taskId) - now using cancellationTracker

  // Check if any marketplace has CANCELLING status (marketplace-specific cancellation)
  const marketplaceStatuses = taskStatus[taskId].marketplaces;
  const hasCancellingMarketplace = marketplaceStatuses && Object.values(marketplaceStatuses).some(mp => mp?.status === 'CANCELLING');

  if (hasCancellingMarketplace) {
    // This is a marketplace-specific cancellation

    // Verify no active orders remain for this marketplace
    if (marketplace) {
      try {
        const activeOrderCount = await orderCache.countByTaskAndMarketplace(taskId, marketplace.toLowerCase());

        if (activeOrderCount > 0) {
          return;
        }
      } catch (error) {
        errorLog('MarketplaceCancellation', `Error checking active orders for marketplace ${marketplace} - task ${taskId}`, error);
        // If we can't verify, don't mark as complete
        return;
      }
    }

    // Let the cancellation tracker handle percentage updates naturally for marketplace-specific cancellation

    // After cancellation is complete, transition back to STOPPED
    const task = activeTasks.get(taskId);
    setTaskStatus(taskId, 'STOPPED', 'marketplace-specific cancellation complete', marketplace);
    // Reset cancellation percentage for this specific marketplace
    await updateTaskCancellationPercentage(taskId, 0, marketplace);

    // Remove marketplace from blacklist to allow jobs to execute again
    if (marketplace) {
      const wasRemoved = removeTaskFromBlacklist(taskId, marketplace);
      if (wasRemoved) {
      }
    }

    // Keep all other progress values intact
    // Task restored with marketplace-specific status
  } else {
    // No previous state - this could be either:
    // 1. A global cancellation where one marketplace finished (don't mark as complete)
    // 2. An edge case where we lost state

    // Check if we have task-level tracking (indicates global cancellation)
    if (cancellationTracker.hasTaskLevelTracking(taskId)) {
      // This is part of a global cancellation - just log and let the tracker handle it

      // Do NOT call setTaskCancellationComplete here!
      // The task will transition to STOPPED when the aggregated percentage reaches 100%
    } else {
      // Edge case: no previous state and no task-level tracking
    }
  }

  // Clean up cancellation operation tracking
  const cancellationOp = activeCancellationOperations.get(`${taskId}-${marketplace}`);
  if (cancellationOp) {
    activeCancellationOperations.delete(`${taskId}-${marketplace}`);
  }
}

// Track loop counters with cleanup
const loopCounterStats: LoopCount = {};

// Helper function to clean up task resources
// preserveParentJobs: Set to true during stopTask to allow workers to complete job tracking:
//   - Preserves parent jobs (will be cleaned up by cleanupOldParentJobs() with 5-loop retention)
//   - Preserves cancellation tracking (will be cleaned up when cancellation reaches 100% in updateTaskCancellationPercentage())
function cleanupTaskResources(taskId: string, preserveWarnings = false, preserveParentJobs = false, preserveOrders = false): void {
  // TaskFlow logging: Entry

  let clearedTimeouts = 0;
  let clearedCountdowns = 0;

  // Clean up marketplace completion tracking

  // Clear loop counter
  if (loopCounterStats[taskId]) {
    delete loopCounterStats[taskId];
  }

  // Clear and delete timeout
  const timeout = taskTimeouts.get(taskId);
  if (timeout) {
    clearTimeout(timeout);
    taskTimeouts.delete(taskId);
    clearedTimeouts++;
  }

  // Clear and delete countdown timers (both legacy and marketplace-specific)
  // First clear the legacy timer (if any)
  const countdownTimer = taskCountdownTimers.get(taskId);
  if (countdownTimer) {
    clearInterval(countdownTimer);
    taskCountdownTimers.delete(taskId);
    clearedCountdowns++;
  }

  // Then clear any marketplace-specific timers
  const marketplacesList = ['opensea', 'magiceden', 'blur'];
  for (const marketplace of marketplacesList) {
    const timerKey = `${taskId}-${marketplace}`;

    // Clear marketplace-specific timeout
    const mpTimeout = taskTimeouts.get(timerKey);
    if (mpTimeout) {
      clearTimeout(mpTimeout);
      taskTimeouts.delete(timerKey);
      clearedTimeouts++;
    }

    // Clear marketplace-specific countdown
    const marketplaceTimer = taskCountdownTimers.get(timerKey);
    if (marketplaceTimer) {
      clearInterval(marketplaceTimer);
      taskCountdownTimers.delete(timerKey);
      clearedCountdowns++;
    }

    // Clear marketplace-specific execution ID (cancellation token)
    timerExecutionIds.delete(timerKey);
  }

  // Clear legacy execution ID (taskId without marketplace suffix)
  timerExecutionIds.delete(taskId);

  // TaskFlow logging: Timers cleared

  // Release loop scheduling lock
  releaseLoopSchedulingLock(taskId);

  // Clear scheduling flag
  if (tasksBeingScheduled.has(taskId)) {
    tasksBeingScheduled.delete(taskId);
  }

  // Clear marketplace adding jobs flag

  // Delete task state from Redis cache (fire-and-forget for performance)
  // Log final stats before clearing
  if (skipStats[taskId]) {
  }

  // Clear task-specific stats objects
  delete skipStats[taskId];
  delete errorStats[taskId];
  delete floorPrices[taskId];

  // Only clear warnings if not preserving them
  if (!preserveWarnings) {
    delete warningBids[taskId];
    delete warningMessage[taskId];
  } else {
  }

  delete bestOffers[taskId];

  // Clean up task status history (memory leak fix - 2025-12-24)
  delete taskStatusHistory[taskId];

  // Clear scheduling tracking
  lastScheduleTimes.delete(taskId);
  lastCompletionCheckTimes.delete(taskId);

  // Clear processed jobs tracking for all marketplaces and all loops
  const marketplaces = ['opensea', 'magiceden', 'blur'];

  // Track how many parent jobs are cleared (for logging)
  let clearedParentJobsCount = 0;

  // Clean up all parent jobs for this task (across all loops)
  // Skip if preserveParentJobs=true (during stopTask) to allow workers to complete job tracking
  // Parent jobs will be cleaned up by cleanupOldParentJobs() with 5-loop retention
  if (!preserveParentJobs) {
    const keysToDelete: string[] = [];
    for (const [key, parentJob] of parentJobs) {
      if (parentJob.taskId === taskId) {
        keysToDelete.push(key);
      }
    }
    keysToDelete.forEach(key => {
      parentJobs.delete(key);
      processedJobsPerParent.delete(key);
    });
    clearedParentJobsCount = keysToDelete.length;

    // TaskFlow logging: Parent jobs cleared
  } else {
  }

  // Clean up cancellation tracking
  // Skip if preserveParentJobs=true (during stopTask) to allow cancellation jobs to report progress
  // Cancellation tracking will be cleaned up in updateTaskCancellationPercentage() when percentage reaches 100%
  if (!preserveParentJobs) {
    marketplaces.forEach(marketplace => {
      cancellationTracker.clearTracking(taskId, marketplace);
    });
  } else {
  }

  // Clean up restart counter
  taskRestartCounts.delete(taskId);

  // Memory leak fixes - clean up task-specific data structures that were missing cleanup
  // Note: ownBidCounts is keyed by marketplace (opensea/magiceden/blur), not taskId - so we don't delete here
  // Note: counterbidCounts is keyed by taskId so this is valid
  delete counterbidCounts[taskId];
  taskPreprocessing.delete(taskId);
  scheduledJobLocks.delete(taskId);
  // Note: loopSchedulingLocks already cleaned via releaseLoopSchedulingLock() above

  // Clean up order cache for this task (fire-and-forget since this is a cleanup function)
  // Only clean order cache if not in active cancellation (preserveOrders=true during stopTask)
  if (!preserveOrders) {
    orderCache.deleteByTask(taskId);
  } else {
  }

  // TaskFlow logging: Complete
}

// ============================================================================
// Balance Error Detection & Currency-Based Marketplace Mapping
// ============================================================================

// NOTE: Additional balance helpers (parseOpenseaBalanceError, getMarketplacesForCurrency, getTokenAddressForCurrency) are now imported from ./utils/balance-helpers

/**
 * Helper function to complete a task marketplace due to insufficient balance
 * Sets status to COMPLETED with 100% progress and adds error details
 */
async function completeTaskDueToInsufficientBalance(
  taskId: string,
  marketplace: string,
  details: {
    wallet: string;
    requiredAmount: number;
    availableAmount: number;
    currency: string;
  }
): Promise<void> {
  if (!taskStatus[taskId]) {
    return;
  }

  const mpKey = marketplace.toLowerCase() as 'opensea' | 'magiceden' | 'blur';
  const mpStatus = taskStatus[taskId].marketplaces[mpKey];

  if (!mpStatus) {
    return;
  }

  // Get parent job to set completion counts
  const parentJobId = getParentJobId(taskId, marketplace);
  const parentJob = parentJobs.get(parentJobId);

  if (parentJob) {
    // Set completedJobs to totalJobs to show 100% completion
    if (parentJob.totalJobs > 0) {
      parentJob.completedJobs = parentJob.totalJobs;
    }
  }

  // Update marketplace status
  mpStatus.status = 'COMPLETED';
  mpStatus.runningPercentage = 100;
  mpStatus.completedJobs = mpStatus.totalJobs || 0;
  mpStatus.insufficientBalance = true;
  mpStatus.balanceErrorDetails = details;
  mpStatus.completionTimestamp = Date.now();
  mpStatus.lastUpdate = Date.now();

  taskStatus[taskId].lastUpdate = Date.now();

  // Log the event
  console.log(`[INSUFFICIENT_BALANCE] Task ${taskId} marketplace ${marketplace} completed due to insufficient balance:`, {
    wallet: details.wallet,
    required: `${details.requiredAmount} ${details.currency}`,
    available: `${details.availableAmount} ${details.currency}`,
    deficit: `${(details.requiredAmount - details.availableAmount).toFixed(6)} ${details.currency}`
  });
}

/**
 * Helper function to update task running percentage
 *
 * Fixed bug where tasks would get stuck when actualJobs > expectedJobs:
 * - Uses Math.min(actualJobs, expectedJobs) for percentage calculation
 * - Prevents tasks from being unable to reach 100% completion
 * - Handles cases where job retries or timeouts create extra jobs
 */
async function updateTaskRunningPercentage(taskId: string) {
  const task = activeTasks.get(taskId);
  if (!task || !taskStatus[taskId]) {
    return;
  }

  const selectedMarketplaces = task.selectedMarketplaces.map(m => m.toLowerCase());
  const enabledMarketplacesLower = enabledMarketplaces.map(m => m.toLowerCase());
  const marketplacesToCheck = selectedMarketplaces.filter(m => enabledMarketplacesLower.includes(m));

  let totalJobs = 0;
  let completedJobs = 0;
  const marketplaceBreakdown: Record<string, { total: number; completed: number; percentage: number }> = {};

  // First, clear any stale data for disabled marketplaces
  for (const marketplace of selectedMarketplaces) {
    if (!marketplacesToCheck.includes(marketplace)) {
      const mpKey = marketplace as 'opensea' | 'magiceden' | 'blur';
      if (taskStatus[taskId].marketplaces[mpKey]) {
        // Reset percentage to 0 for disabled marketplaces
        taskStatus[taskId].marketplaces[mpKey]!.runningPercentage = 0;
        taskStatus[taskId].marketplaces[mpKey]!.totalJobs = 0;
        taskStatus[taskId].marketplaces[mpKey]!.completedJobs = 0;

        // Clear parent job data for disabled marketplace
        const parentJobId = getParentJobId(taskId, marketplace);
        if (parentJobs.has(parentJobId)) {
          parentJobs.delete(parentJobId);
        }
      }
    }
  }

  // Calculate totals across all enabled marketplaces
  for (const marketplace of marketplacesToCheck) {
    const parentJobId = getParentJobId(taskId, marketplace);
    const parentJob = parentJobs.get(parentJobId);

    if (parentJob) {
      // Use the minimum of actualJobs and expectedJobs to handle cases where extra jobs are created
      let mpTotal = parentJob.totalJobs || 0;

      // If we have both actualJobs and expectedJobs, use the minimum to prevent percentage issues
      if (parentJob.actualJobs !== undefined && parentJob.expectedJobs !== undefined && parentJob.allJobsQueued) {
        const originalTotal = mpTotal;
        mpTotal = Math.min(parentJob.actualJobs, parentJob.expectedJobs);

        // Log if we're adjusting due to actualJobs exceeding expectedJobs
        if (parentJob.actualJobs > parentJob.expectedJobs) {
        }
      }

      // Log the values being used for transparency
      if (parentJob.allJobsQueued) {

        // Log partial queueing warning
        if (parentJob.actualJobs !== undefined && parentJob.expectedJobs !== undefined &&
          parentJob.actualJobs < parentJob.expectedJobs) {
        }

        // Detect stuck condition: allJobsQueued=true, totalJobs=0, but expectedJobs > 0
        if (mpTotal === 0 && parentJob.expectedJobs && parentJob.expectedJobs > 0) {
          // TaskFlow logging: Stuck condition detected
          warnLog('TaskProgress',
            `STUCK_DETECTED | taskId=${taskId} | mp=${marketplace} | allJobsQueued=true but totalJobs=0 | expectedJobs=${parentJob.expectedJobs}`,
            { taskId, marketplace: marketplace as 'opensea' | 'magiceden' | 'blur', expectedJobs: parentJob.expectedJobs, actualJobs: 0, trigger: 'zero_jobs_queued' }
          );

          // Check if we haven't already scheduled a restart for this task
          const restartKey = `restart-scheduled:${taskId}`;
          if (!tasksBeingRestarted.has(restartKey)) {
            tasksBeingRestarted.add(restartKey);

            // Schedule a restart after a brief delay to avoid race conditions
            setTimeout(async () => {
              try {
                await triggerTaskRestart(taskId);
              } finally {
                // Clear the restart flag after attempting restart
                tasksBeingRestarted.delete(restartKey);
              }
            }, 2000);

          }
        }
      } else {
      }

      const mpCompleted = parentJob.completedJobs || 0;
      const mpPercentage = mpTotal > 0 ? (mpCompleted / mpTotal) * 100 : 0;

      // TaskFlow logging: Per-marketplace progress (milestone-only: 25%, 50%, 75%, 100%)
      const milestones = [25, 50, 75, 100];
      const previousPercentage = mpTotal > 0 ? ((mpCompleted - 1) / mpTotal) * 100 : 0;
      const isAtMilestone = milestones.some(m => mpPercentage >= m && previousPercentage < m);
      if (isAtMilestone) {
      }

      marketplaceBreakdown[marketplace] = {
        total: mpTotal,
        completed: mpCompleted,
        percentage: mpPercentage
      };

      // Update marketplace-specific runningPercentage
      const mpKey = marketplace as 'opensea' | 'magiceden' | 'blur';
      if (taskStatus[taskId].marketplaces[mpKey]) {
        // Skip updating if marketplace was completed due to insufficient balance
        // Keep it at 100% with balance error details
        if (taskStatus[taskId].marketplaces[mpKey]!.insufficientBalance) {
          // Keep existing values (100%, COMPLETED status, balance error details)
          continue;
        }

        taskStatus[taskId].marketplaces[mpKey]!.runningPercentage = mpPercentage;
        taskStatus[taskId].marketplaces[mpKey]!.totalJobs = mpTotal;
        taskStatus[taskId].marketplaces[mpKey]!.completedJobs = mpCompleted;
      }

      totalJobs += mpTotal;
      completedJobs += mpCompleted;

      // Additional safety check for individual marketplace overflow
      if (mpCompleted > mpTotal && mpTotal > 0) {
      }


      // Check if this specific marketplace is complete
      if (mpTotal > 0 && mpCompleted >= mpTotal) {
        const marketplaceStatus = taskStatus[taskId]?.marketplaces?.[marketplace as 'opensea' | 'magiceden' | 'blur'];
        // TaskFlow logging: Marketplace completion detection
        if (marketplaceStatus && marketplaceStatus.status === 'RUNNING') {
          // Trigger marketplace-specific completion
          // DEBUG: Log BEFORE fire-and-forget call to checkTaskCompletionAndScheduleNext
          // IMPORTANT: This is fire-and-forget! Errors here are SILENTLY swallowed!
          // We wrap in try-catch to log any silent failures
          try {
            checkTaskCompletionAndScheduleNext(taskId, marketplace);
            // DEBUG: Log AFTER fire-and-forget call returns (may be async internally)
          } catch (completionError) {
            // CRITICAL: This is the fire-and-forget silent failure we're trying to catch!
            errorLog('TaskProgress',
              `COMPLETION_TRIGGER_FAILED | taskId=${taskId} | mp=${marketplace} | error=${completionError instanceof Error ? completionError.message : String(completionError)}`,
              completionError instanceof Error ? completionError : undefined,
              { taskId, marketplace: marketplace as 'opensea' | 'magiceden' | 'blur' }
            );
          }
        }
      }
    } else {
      marketplaceBreakdown[marketplace] = {
        total: 0,
        completed: 0,
        percentage: 0
      };
    }
  }

  // Check if we have partial queueing and if all actual jobs are complete
  let allActualJobsComplete = true;
  let hasPartialQueueing = false;
  let expectedTotal = 0;
  let actualTotal = 0;

  for (const marketplace of marketplacesToCheck) {
    const parentJobId = getParentJobId(taskId, marketplace);
    const parentJob = parentJobs.get(parentJobId);

    if (parentJob && parentJob.allJobsQueued) {
      expectedTotal += parentJob.expectedJobs || 0;
      actualTotal += parentJob.actualJobs || 0;

      // Check for partial queueing
      if (parentJob.actualJobs !== undefined && parentJob.expectedJobs !== undefined &&
        parentJob.actualJobs < parentJob.expectedJobs) {
        hasPartialQueueing = true;
      }

      // Check if all actual jobs for this marketplace are complete
      if (parentJob.actualJobs !== undefined &&
        parentJob.completedJobs < parentJob.actualJobs) {
        allActualJobsComplete = false;
      }
    }
  }

  // Update running percentage with overflow protection
  let runningPercentage = 0;
  if (totalJobs > 0) {
    runningPercentage = (completedJobs / totalJobs) * 100;
    // Additional safety check for total overflow
    if (completedJobs > totalJobs) {
    }
  }

  // Running percentage already calculated above

  // Log comprehensive task status update
  // Global status removed - now using marketplace-specific status only

  // Log marketplace breakdown
  for (const [mp, breakdown] of Object.entries(marketplaceBreakdown)) {
    const mpStatus = taskStatus[taskId].marketplaces[mp as 'opensea' | 'magiceden' | 'blur'];
  }

  // Update last update timestamp
  taskStatus[taskId].lastUpdate = Date.now();


  // Summary section - Keep only TOTAL SUMMARY logs
  // marketplacesToCheck.map(mp =>
  // `${mp.charAt(0).toUpperCase() + mp.slice(1)}: ${marketplaceBreakdown[mp]?.total || 0}`
  // ).join(', ') + `)` + RESET);

  // Overall progress bar
  const overallBarLength = 30;
  const overallFilledLength = Math.min(overallBarLength, Math.round((runningPercentage / 100) * overallBarLength));
  const overallProgressBar = '█'.repeat(overallFilledLength) + '░'.repeat(Math.max(0, overallBarLength - overallFilledLength));

  // Note: Removed global task completion check - each marketplace completes independently

  // Detect and handle partial queueing mismatch
  // This occurs when expectedJobs > actualJobs and allJobsQueued is false
  if (totalJobs === 0 && expectedTotal > 0) {
    let hasPartialQueueingMismatch = false;
    let mismatchDetails: string[] = [];

    for (const marketplace of marketplacesToCheck) {
      const parentJobId = getParentJobId(taskId, marketplace);
      const parentJob = parentJobs.get(parentJobId);

      if (parentJob &&
        parentJob.expectedJobs !== undefined &&
        parentJob.expectedJobs > 0 &&
        parentJob.actualJobs !== undefined &&
        parentJob.actualJobs < parentJob.expectedJobs &&
        !parentJob.allJobsQueued) {
        hasPartialQueueingMismatch = true;
        mismatchDetails.push(`${marketplace}: ${parentJob.actualJobs}/${parentJob.expectedJobs} jobs queued`);
      }
    }

    if (hasPartialQueueingMismatch) {

      // Check if we should trigger a restart
      const restartKey = `partial-queue-restart:${taskId}`;
      if (!tasksBeingRestarted.has(restartKey)) {
        tasksBeingRestarted.add(restartKey);


        // Schedule restart after a short delay
        setTimeout(async () => {
          try {
            await triggerTaskRestart(taskId);
          } catch (error) {
            errorLog('TaskRecovery', `Failed to restart task ${taskId}`, error);
          } finally {
            tasksBeingRestarted.delete(restartKey);
          }
        }, 5000); // 5 second delay
      } else {
      }
    }
  }
}

// Helper function to update task cancellation percentage (similar to updateTaskRunningPercentage)
// Removed updateTaskCancellationPercentage - now using cancellationTracker


// Track last completion check time to prevent rapid checks
const lastCompletionCheckTimes = new Map<string, number>();


// Track tasks currently in completion process to prevent duplicate calls
const tasksInCompletionProcess = new Set<string>();

// Helper function to check task completion and schedule next iteration
async function checkTaskCompletionAndScheduleNext(taskId: string, marketplace?: string) {
  const completionKey = marketplace ? `${taskId}-${marketplace}` : taskId;
  const mpKeyForLog = marketplace?.toLowerCase() as 'opensea' | 'magiceden' | 'blur' | undefined;

  // TaskFlow logging: Entry

  const task = activeTasks.get(taskId);
  if (!task) {
    return;
  }

  // Check if task/marketplace is already in completion process
  if (tasksInCompletionProcess.has(completionKey)) {
    return;
  }

  // Mark task/marketplace as in completion process
  tasksInCompletionProcess.add(completionKey);

  try {
    if (!marketplace) {
      errorLog('TaskCompletion', `ERROR: Called without marketplace parameter for task ${taskId}`, null, { taskId });
      tasksInCompletionProcess.delete(completionKey);
      return;
    }

    // Log completion transition
    const currentLoopCount = loopCounterStats[taskId]?.[marketplace] || 1;

    // Log current marketplace status before completion
    const mpStatus = taskStatus[taskId]?.marketplaces?.[marketplace as 'opensea' | 'magiceden' | 'blur'];

    setTaskStatus(taskId, 'COMPLETED', `marketplace completion - Loop #${currentLoopCount}`, marketplace);

    // Add 0.5-second delay before transitioning to SCHEDULED
    await new Promise(resolve => setTimeout(resolve, 500));

    // TaskFlow logging: Before scheduling next loop

    // Schedule next loop for this marketplace
    await scheduleNextLoop(taskId, marketplace);


    // Timer validation: Verify timer was created after scheduleNextLoop
    if (marketplace) {
      const timerKey = `${taskId}-${marketplace}`;
      const hasTimer = taskTimeouts.has(timerKey);
      const hasCountdown = taskCountdownTimers.has(timerKey);
      if (!hasTimer) {
        errorLog('TaskCompletion',
          `TIMER_MISSING_AFTER_SCHEDULE | taskId=${taskId} | mp=${marketplace} | timerKey=${timerKey} | hasCountdown=${hasCountdown}`,
          undefined, { taskId, marketplace: mpKeyForLog, timerKey, hasCountdown }
        );
      } else {
      }
    }

    // BUG FIX: Removed redundant setTaskStatus call - scheduleNextLoop() already sets status to SCHEDULED
    // The duplicate call was causing confusing log traces

    // FIX: PARENT_NOT_FOUND Bug (8499 occurrences in logs)
    // DO NOT delete parent jobs here! Workers are still processing jobs from the completed loop
    // and need the parent job to track completion via markJobAsProcessed().
    // Let cleanupOldParentJobs() handle cleanup naturally when the NEXT loop starts.
    // cleanupOldParentJobs() keeps parent jobs for 5 loops before cleanup (called at lines 9565, 9592).

  } catch (error) {
    errorLog('TaskCompletion',
      `HANDLER_ERROR | taskId=${taskId} | mp=${marketplace}`,
      error,
      { taskId, marketplace: mpKeyForLog, completionKey }
    );
    // Note: cleanup happens in finally block, no need to delete here
    throw error;
  } finally {
    // Always remove from completion process when done
    tasksInCompletionProcess.delete(completionKey);
  }
}

// Helper function to handle task cleanup (trait/token removal)
async function handleTaskCleanup(taskId: string) {
  const task = activeTasks.get(taskId);
  if (!task) return;

  const wallet = await walletCache.getWalletById(task.walletId);
  const { currencySymbol } = getCurrencyDetailsFromTask(task, "opensea");

  let totalCancellationJobs = 0;

  // Cancel removed trait bids
  if (traitsToRemove[taskId]?.length > 0) {
    const traitJobs = await cancelRemovedTraitBids(traitsToRemove[taskId], task._id, task.walletId, currencySymbol);
    totalCancellationJobs += traitJobs;
    delete traitsToRemove[taskId]; // ✅ Clear after queueing to prevent unbounded growth
  }

  // Cancel removed token bids
  if (removedTokens[taskId]?.opensea?.length > 0 || removedTokens[taskId]?.magiceden?.length > 0) {
    const tokenJobs = await cancelRemovedTokenBids(removedTokens[taskId], task._id, task.walletId, currencySymbol);
    totalCancellationJobs += tokenJobs;
    delete removedTokens[taskId]; // ✅ Clear after queueing to prevent unbounded growth
  }

  return totalCancellationJobs;
}

// Helper function to mark a job as processed regardless of outcome
async function markJobAsProcessed(
  taskId: string,
  marketplace: string,
  jobId: string,
  success: boolean = true,
  jobLoopNumber?: number  // Optional loop number from job.data.loopNumber for precise parent lookup
) {
  const currentLoopCount = loopCounterStats[taskId]?.[marketplace] || 1;
  const mpKeyForLog = marketplace.toLowerCase() as 'opensea' | 'magiceden' | 'blur';
  const mpLower = marketplace.toLowerCase();

  // JobProcessing ENTRY log removed - too noisy (fires for every bid job)

  // Validate task exists and is active
  const task = activeTasks.get(taskId);
  if (!task) {
    return;
  }

  // If jobLoopNumber is provided, use it directly for precise parent lookup
  // This is the preferred path for jobs created with loopNumber in their data
  let parentJobId: string;
  let parentJob: BulkJobParent | undefined;

  if (jobLoopNumber !== undefined && jobLoopNumber > 0) {
    // Direct lookup using the job's stored loop number - this is the precise path
    parentJobId = `${taskId}-${mpLower}-loop${jobLoopNumber}`.toLowerCase();
    parentJob = parentJobs.get(parentJobId);

    if (!parentJob) {
      // Parent job was already cleaned up - this is rare with 5-loop retention
      // but can happen for very slow jobs.
      return;
    }
  } else {
    // Legacy path: job doesn't have loopNumber in data, use fallback search
    // Get parent job with current loop number first
    parentJobId = getParentJobId(taskId, marketplace);
    parentJob = parentJobs.get(parentJobId);

    // If parent job doesn't exist, check if it's from a different loop
    if (!parentJob) {
      // Try to extract loop number from job ID
      const jobLoopMatch = jobId.match(/loop(\d+)/);
      const jobLoop = jobLoopMatch ? parseInt(jobLoopMatch[1]) : 0;

      if (jobLoop > 0 && jobLoop !== currentLoopCount) {
        // Job is from a different loop - try to find its parent job
        const oldParentJobId = `${taskId}-${marketplace}-loop${jobLoop}`.toLowerCase();
        const oldParentJob = parentJobs.get(oldParentJobId);

        if (oldParentJob) {
          // Process it with the old parent job
          parentJob = oldParentJob;
          parentJobId = oldParentJobId;
        } else {
          return;
        }
      } else {
        // Job ID doesn't contain loop number (e.g., scheduled jobs)
        // Search for any parent job for this task/marketplace within the last 5 loops
        for (let loopOffset = 0; loopOffset <= 5; loopOffset++) {
          const searchLoop = currentLoopCount - loopOffset;
          if (searchLoop < 1) continue;
          const searchParentJobId = `${taskId}-${mpLower}-loop${searchLoop}`.toLowerCase();
          const searchParentJob = parentJobs.get(searchParentJobId);
          if (searchParentJob) {
            parentJob = searchParentJob;
            parentJobId = searchParentJobId;
            break;
          }
        }
        if (!parentJob) {
          // No parent job found in recent loops
          return;
        }
      }
    }
  }

  // If we found an old parent job, skip loop validation
  // Otherwise validate the job belongs to the current loop
  if (parentJob.loopNumber !== currentLoopCount && parentJobId.includes(`loop${currentLoopCount}`)) {
    return;
  }

  const successCount = parentJob.completedJobs - (parentJob.failedJobs || 0);

  // Check if parent job is already complete - prevent overflow
  if (parentJob.completedJobs >= parentJob.totalJobs && parentJob.totalJobs > 0) {
    // Safety check: if allJobsQueued is false but we're already complete, something went wrong
    if (!parentJob.allJobsQueued) {
      warnLog('MarkJob', `FIXING_STATE | taskId=${taskId} | mp=${marketplace} | allJobsQueued was false but completedJobs=${parentJob.completedJobs} >= totalJobs=${parentJob.totalJobs} | setting allJobsQueued=true`, { taskId, marketplace: mpKeyForLog });
      parentJob.allJobsQueued = true; // Fix the state
    }
    return;
  }

  if (taskStatus[taskId]) {
    // Only transition to RUNNING from valid predecessor states
    // Skip if already RUNNING, COMPLETED, or SCHEDULED (late job from previous loop)
    const currentStatus = taskStatus[taskId].marketplaces?.[marketplace.toLowerCase() as 'opensea' | 'magiceden' | 'blur']?.status;
    if (currentStatus === 'STARTED' || currentStatus === 'QUEUED' || currentStatus === 'QUEUEING') {
      setTaskStatus(taskId, 'RUNNING', 'job marked as processed', marketplace);
    }
    // If status is COMPLETED/SCHEDULED, this is a late job from a previous loop - skip silently
  }

  // CRITICAL: Use a mutex lock to prevent race conditions during counter increment
  const counterLockKey = `counter-lock:${parentJobId}`;
  const lockManager = new DistributedLockManager({
    lockPrefix: 'counter:',
    defaultTTLSeconds: 5
  });

  const lockAcquired = await lockManager.acquireLock(counterLockKey, 5);

  if (!lockAcquired) {
    // BUG FIX: Don't continue without lock - schedule a retry instead
    errorLog('JobProcessing',
      `LOCK_FAILED_RETRY | parentJobId=${parentJobId} | jobId=${jobId} | scheduling retry in 500ms`,
      undefined,
      { taskId, marketplace: mpKeyForLog, parentJobId, jobId }
    );
    // Schedule retry after a short delay (pass jobLoopNumber for consistent parent lookup)
    setTimeout(() => {
      markJobAsProcessed(taskId, marketplace, jobId, success, jobLoopNumber).catch(err => {
        errorLog('JobProcessing', `Retry failed for job ${jobId}: ${err.message}`, err);
      });
    }, 500);
    return;
  }

  try {

    // Re-fetch parent job inside the lock to get latest values
    parentJob = parentJobs.get(parentJobId);
    if (!parentJob) {
      const missingTaskId = getTaskIdForLog({ data: { taskId, _id: taskId } });
      errorLog('JobProcessing', `Parent job disappeared during lock acquisition! Task ${missingTaskId}`, undefined);
      return;
    }

    // Log before increment for debugging
    const beforeCount = parentJob.completedJobs;
    const beforeFailedCount = parentJob.failedJobs || 0;

    // Increment appropriate counter based on success/failure
    if (success) {
      // Increment completed jobs with hard cap protection
      parentJob.completedJobs++;
    } else {
      // Increment both completed (for progress) and failed (for tracking) counters
      parentJob.completedJobs++;
      parentJob.failedJobs = (parentJob.failedJobs || 0) + 1;
    }

    // Additional safety check to prevent overflow (should never happen with above logic)
    if (parentJob.completedJobs > parentJob.totalJobs) {
      parentJob.completedJobs = parentJob.totalJobs;
    }

  } finally {
    // Always release the lock
    if (lockAcquired) {
      try {
        await lockManager.releaseLock(counterLockKey);
      } catch (error) {
        const releaseTaskId = getTaskIdForLog({ data: { taskId, _id: taskId } });
        errorLog('LockRelease', `Error releasing counter lock for task ${releaseTaskId}`, error);
      }
    }
  }

  // If this was the first job and totalJobs is 0, set it to 1
  if (parentJob.totalJobs === 0) {
    parentJob.totalJobs = 1;
  }
  // Update task running percentage
  try {
    await updateTaskRunningPercentage(taskId);
  } catch (error) {
    const percentTaskId = getTaskIdForLog({ data: { taskId, _id: taskId } });
    errorLog('JobProcessing', `Error updating task percentage for task ${percentTaskId}`, error);
    errorLog('JobProcessing', `Task ${percentTaskId}: This may cause stuck percentage!`, undefined);
  }

  // Check if all jobs are complete for this parent job
  if (parentJob.completedJobs >= parentJob.totalJobs) {
    const successCount = parentJob.completedJobs - (parentJob.failedJobs || 0);

    // Check if all parent jobs for this task are complete
    const allComplete = checkAllParentJobsComplete(taskId);

    if (allComplete) {
      const loopCounts = loopCounterStats[taskId] || {};
      const loopCountStr = Object.entries(loopCounts).map(([mp, count]) => `${mp}:${count}`).join(', ') || 'none';

      // Do NOT force completion here - let updateTaskRunningPercentage handle it naturally
      // This prevents duplicate completion calls
    }
  }
}

// Helper function to mark a cancellation job as processed (similar to markJobAsProcessed)
// Removed markCancellationJobAsProcessed and checkAllCancellationJobsComplete - now using cancellationTracker

/**
 * Mark a skipped job as processed to ensure loop completion tracking works.
 * This is CRITICAL for jobs that are skipped early (blacklist, abort) - they must still
 * contribute to the parent job's completedJobs counter, otherwise loops never complete.
 *
 * Without this: skipped jobs → completedJobs never increments → loop stuck at ~90% forever
 * With this: skipped jobs → markJobAsProcessed(success=false) → counter++ → loop completes
 */
async function markSkippedJobAsProcessed(job: Job): Promise<void> {
  if (!job.id || typeof job.id !== 'string' || !job.id.startsWith('bid-')) {
    return;
  }
  const parts = job.id.split('-');
  if (parts.length < 3) {
    return;
  }
  const taskId = parts[1];
  const marketplace = parts[2].toLowerCase();
  const jobLoopNumber = job.data?.loopNumber as number | undefined;
  // Fire-and-forget with catch to prevent unhandled promise rejection
  await markJobAsProcessed(taskId, marketplace, job.id, false, jobLoopNumber).catch(() => { });
}

// Helper function to clean up old parent jobs
// Increased from 2 to 5 loops to provide safety net for slow jobs with loopNumber in data
function cleanupOldParentJobs(taskId: string, currentLoop: number, keepLoops: number = 5) {
  const parentJobsToRemove: string[] = [];

  // Iterate through all parent jobs and find old ones for this task
  for (const [parentJobId, parentJob] of parentJobs.entries()) {
    if (parentJob.taskId === taskId && parentJob.loopNumber < currentLoop - keepLoops) {
      parentJobsToRemove.push(parentJobId);
    }
  }

  // Remove old parent jobs
  for (const parentJobId of parentJobsToRemove) {
    parentJobs.delete(parentJobId);
    processedJobsPerParent.delete(parentJobId);
  }

  if (parentJobsToRemove.length > 0) {
  }
}

// Helper function to clear task timers
function clearTaskTimers(taskId: string, marketplace?: string) {
  // If marketplace is specified, only clear timers for that marketplace
  // Otherwise, clear all timers for the task
  const timerKeyPattern = marketplace ? `${taskId}-${marketplace}` : taskId;

  // Clear timeouts
  for (const [key, timeout] of taskTimeouts.entries()) {
    if (marketplace) {
      // For marketplace-specific clearing, only clear exact matches
      if (key === timerKeyPattern) {
        clearTimeout(timeout);
        taskTimeouts.delete(key);
      }
    } else {
      // For global clearing, clear all timers that start with taskId
      if (key.startsWith(taskId)) {
        clearTimeout(timeout);
        taskTimeouts.delete(key);
      }
    }
  }

  // Clear countdown timers
  for (const [key, timer] of taskCountdownTimers.entries()) {
    if (marketplace) {
      // For marketplace-specific clearing, only clear exact matches
      if (key === timerKeyPattern) {
        clearInterval(timer);
        taskCountdownTimers.delete(key);
      }
    } else {
      // For global clearing, clear all timers that start with taskId
      if (key.startsWith(taskId)) {
        clearInterval(timer);
        taskCountdownTimers.delete(key);
      }
    }
  }
}


// Track last schedule time to prevent rapid re-scheduling
const lastScheduleTimes = new Map<string, number>();

/**
 * Direct Timer Execution: Execute marketplace bid loop directly without BullMQ SCHEDULE jobs.
 *
 * This function replaces the BullMQ SCHEDULE job mechanism that was causing:
 * - 418 scheduled job timeouts (10-minute BullMQ timeout)
 * - 165 stuck tasks requiring forced recovery
 * - 163 forced progression timeouts
 *
 * Benefits:
 * - No BullMQ timeout failures (was 10 minutes, now unlimited)
 * - Direct stack traces for debugging
 * - Reduced Redis queue load
 * - Faster execution (no queue pickup delay)
 *
 * @see docs/SCHEDULE-JOB-TIMEOUT-ANALYSIS-2025-12-29.md
 */
async function executeMarketplaceBidLoop(taskId: string, marketplace: string): Promise<void> {
  const maxRetries = 3;
  const retryDelays = [2000, 5000, 10000]; // 2s, 5s, 10s
  const mpKey = marketplace.toLowerCase() as 'opensea' | 'magiceden' | 'blur';
  const startTime = Date.now();


  // Reset per-loop stats for this marketplace at the START of each new loop
  // This ensures skipStats and errorStats show per-loop counts, not cumulative across loops
  const prevSkip = skipStats[taskId]?.[mpKey] || 0;
  const prevError = errorStats[taskId]?.[mpKey] || 0;

  // Initialize task-level stats objects if they don't exist
  if (!skipStats[taskId]) {
    skipStats[taskId] = { opensea: 0, magiceden: 0, blur: 0 };
  }
  if (!errorStats[taskId]) {
    errorStats[taskId] = { opensea: 0, magiceden: 0, blur: 0 };
  }

  // Reset the specific marketplace's stats to 0 for this new loop
  skipStats[taskId][mpKey] = 0;
  errorStats[taskId][mpKey] = 0;

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    const attemptStart = Date.now();

    try {
      // Validate task state before execution
      const currentTask = activeTasks.get(taskId);
      const isBlacklisted = isTaskBlacklisted(taskId, mpKey);

      if (!currentTask?.running) {
        return;
      }

      if (isBlacklisted) {
        return;
      }

      // Execute directly based on marketplace - no BullMQ queue
      switch (mpKey) {
        case 'opensea':
          await processOpenseaScheduledBid(currentTask);
          break;
        case 'blur':
          await processBlurScheduledBid(currentTask);
          break;
        case 'magiceden':
          await processMagicedenScheduledBid(currentTask);
          break;
        default:
          warnLog('MarketplaceExec', `EXEC_UNKNOWN_MARKETPLACE | taskId=${taskId} | marketplace=${marketplace}`, { taskId, marketplace: mpKey });
          return;
      }

      const attemptDuration = Date.now() - attemptStart;
      const totalDuration = Date.now() - startTime;

      return; // Success - exit retry loop

    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      const attemptDuration = Date.now() - attemptStart;

      if (attempt < maxRetries - 1) {
        const delayMs = retryDelays[attempt];
        warnLog('MarketplaceExec', `EXEC_RETRY | taskId=${taskId} | marketplace=${marketplace} | attempt=${attempt + 1}/${maxRetries} | delayMs=${delayMs} | attemptMs=${attemptDuration} | error=${errorMessage}`, { taskId, marketplace: mpKey });
        await new Promise(resolve => setTimeout(resolve, delayMs));
      } else {
        // Final failure - schedule recovery via next loop
        const totalDuration = Date.now() - startTime;
        errorLog('MarketplaceExec', `EXEC_EXHAUSTED | taskId=${taskId} | marketplace=${marketplace} | attempts=${maxRetries} | totalMs=${totalDuration} | error=${errorMessage}`, error);

        // Schedule next loop to recover - this will set a new timer
        const currentTask = activeTasks.get(taskId);
        const canRecover = currentTask?.running && !isTaskBlacklisted(taskId, mpKey);
        if (canRecover) {
          scheduleNextLoop(taskId, marketplace);
        } else {
          warnLog('MarketplaceExec', `EXEC_NO_RECOVERY | taskId=${taskId} | marketplace=${marketplace} | reason=${!currentTask?.running ? 'not_running' : 'blacklisted'}`, { taskId, marketplace: mpKey });
        }
      }
    }
  }
}

async function scheduleNextLoop(taskId: string, marketplace?: string): Promise<void> {
  const timerKey = marketplace ? `${taskId}-${marketplace}` : taskId;
  const mpKeyForLog = marketplace?.toLowerCase() as 'opensea' | 'magiceden' | 'blur' | undefined;

  // FIRST CHECK: tasksBeingCancelled Set (synchronous flag)
  // This MUST be the first check because it's set at the very start of stopTask(),
  // before any async operations. Prevents scheduling new timers for tasks being cancelled.
  if (tasksBeingCancelled.has(taskId)) {
    return;
  }

  // Get the task from activeTasks
  const task = activeTasks.get(taskId);
  if (!task) {
    warnLog('LoopScheduler',
      `EXIT_NO_TASK | taskId=${taskId} | reason=task_not_in_activeTasks`,
      { taskId, marketplace: mpKeyForLog, timerKey, reason: 'task_not_active' }
    );
    lastScheduleTimes.delete(taskId);
    return;
  }

  // EARLY EXIT: Don't schedule if marketplace is being cancelled or already stopped
  // This prevents the race condition where scheduleNextLoop is called by a completing job
  // after stopTask has already started the cancellation process
  if (marketplace) {
    const mpKey = marketplace.toLowerCase() as 'opensea' | 'magiceden' | 'blur';
    const mpStatusForSchedule = taskStatus[taskId]?.marketplaces?.[mpKey];
    if (mpStatusForSchedule?.status === 'CANCELLING' || mpStatusForSchedule?.status === 'STOPPED') {
      return;
    }
  } else {
    // For all-marketplace scheduling, check if task is being cancelled
    const anyMarketplaceCancelling = task.selectedMarketplaces.some(mp => {
      const mpLower = mp.toLowerCase() as 'opensea' | 'magiceden' | 'blur';
      const mpStatus = taskStatus[taskId]?.marketplaces?.[mpLower];
      return mpStatus?.status === 'CANCELLING' || mpStatus?.status === 'STOPPED';
    });
    if (anyMarketplaceCancelling) {
      return;
    }
  }

  // Calculate loopInterval with getExpiry()
  let loopInterval = getExpiry(task.loopInterval);

  // Handle immediate rescheduling when loopInterval = 0
  if (loopInterval < 0) {
    loopInterval = 60;
  }

  // Enforce minimum loop interval to prevent rapid restarts
  const MIN_LOOP_INTERVAL = 2; // 2 seconds minimum
  if (loopInterval < MIN_LOOP_INTERVAL) {
    loopInterval = MIN_LOOP_INTERVAL;
  }

  // Read loop counter for debug logging (DO NOT increment here)
  // Counter is initialized to 1 in startTaskInternal() and incremented in timer callback when this schedule fires.
  if (marketplace) {
    // Initialize task counters if they don't exist (for first-time access)
    if (!loopCounterStats[taskId]) {
      loopCounterStats[taskId] = {};
    }
    if (!loopCounterStats[taskId][marketplace]) {
      loopCounterStats[taskId][marketplace] = 1;
    }

  }
  // Note: All-marketplaces case doesn't increment per-marketplace counters

  // Set up next loop time and countdown timer
  const nextLoopTime = new Date(Date.now() + loopInterval * 1000);
  const secondsUntilNextLoop = Math.floor((nextLoopTime.getTime() - Date.now()) / 1000);

  // BUG FIX: Validate task is still active BEFORE any state changes
  // This prevents the race condition where status is set to SCHEDULED but timer is never created
  if (!activeTasks.has(taskId)) {
    lastScheduleTimes.delete(taskId);
    return;
  }

  // FIX: Status-Timer Atomicity Bug
  // Only set waitingTime and nextLoopTime here (needed by countdown timer).
  // DO NOT set status to SCHEDULED yet - that happens AFTER timer is successfully created.
  // This prevents orphaned SCHEDULED status when function exits early (e.g., at secondary exit check).
  if (taskStatus[taskId]) {
    if (marketplace) {
      // Set timing info for specific marketplace (status set after timer creation)
      const mpKey = marketplace as 'opensea' | 'magiceden' | 'blur';
      if (taskStatus[taskId].marketplaces && taskStatus[taskId].marketplaces[mpKey]) {
        taskStatus[taskId].marketplaces[mpKey].waitingTime = secondsUntilNextLoop;
        taskStatus[taskId].marketplaces[mpKey].nextLoopTime = nextLoopTime;
      }
    } else {
      // Set timing info for all marketplaces (status set after timer creation)
      for (const mp of task.selectedMarketplaces) {
        const mpLower = mp.toLowerCase() as 'opensea' | 'magiceden' | 'blur';
        if (taskStatus[taskId].marketplaces && taskStatus[taskId].marketplaces[mpLower]) {
          taskStatus[taskId].marketplaces[mpLower].waitingTime = secondsUntilNextLoop;
          taskStatus[taskId].marketplaces[mpLower].nextLoopTime = nextLoopTime;
        }
      }
    }
  }

  // Clear any existing countdown timer before setting a new one
  const existingCountdownTimer = taskCountdownTimers.get(timerKey);
  if (existingCountdownTimer) {
    clearInterval(existingCountdownTimer);
    taskCountdownTimers.delete(timerKey);
  }

  // Set up countdown timer
  const countdownTimer = setInterval(() => {
    if (!taskStatus[taskId]) {
      clearInterval(countdownTimer);
      taskCountdownTimers.delete(timerKey);
      return;
    }

    // Update countdown for specific marketplace or all marketplaces
    if (marketplace) {
      const mpKey = marketplace as 'opensea' | 'magiceden' | 'blur';
      if (taskStatus[taskId].marketplaces && taskStatus[taskId].marketplaces[mpKey]) {
        const mpStatus = taskStatus[taskId].marketplaces[mpKey];
        if (mpStatus.nextLoopTime) {
          const remainingSeconds = Math.max(0, Math.floor((mpStatus.nextLoopTime.getTime() - Date.now()) / 1000));
          mpStatus.waitingTime = remainingSeconds;

          if (remainingSeconds <= 0) {
            clearInterval(countdownTimer);
            taskCountdownTimers.delete(timerKey);
          }
        }
      }
    } else {
      // Update all marketplaces
      let allComplete = true;
      for (const mp of task.selectedMarketplaces) {
        const mpLower = mp.toLowerCase() as 'opensea' | 'magiceden' | 'blur';
        if (taskStatus[taskId].marketplaces && taskStatus[taskId].marketplaces[mpLower]) {
          const mpStatus = taskStatus[taskId].marketplaces[mpLower];
          if (mpStatus.nextLoopTime) {
            const remainingSeconds = Math.max(0, Math.floor((mpStatus.nextLoopTime.getTime() - Date.now()) / 1000));
            mpStatus.waitingTime = remainingSeconds;
            if (remainingSeconds > 0) {
              allComplete = false;
            }
          }
        }
      }

      if (allComplete) {
        clearInterval(countdownTimer);
        taskCountdownTimers.delete(timerKey);
      }
    }
  }, 1000);

  taskCountdownTimers.set(timerKey, countdownTimer);

  // Clear any existing timeout for this task/marketplace combination
  const existingTimeout = taskTimeouts.get(timerKey);
  if (existingTimeout) {
    clearTimeout(existingTimeout);
    taskTimeouts.delete(timerKey);
  }

  // Secondary safeguard: validate task is still active before creating timeout
  // Primary check is at line ~5109, this catches edge cases during countdown setup
  if (!activeTasks.has(taskId)) {
    warnLog('LoopScheduler',
      `SECONDARY_EXIT | taskId=${taskId} | mp=${marketplace || 'all'} | reason=task_deactivated_during_countdown_setup`,
      { taskId, marketplace: mpKeyForLog, timerKey, reason: 'task_deactivated_during_countdown_setup' }
    );
    cleanupTaskResources(taskId);
    lastScheduleTimes.delete(taskId);
    return;
  }

  // Generate unique execution ID for this timer (cancellation token pattern)
  // This allows us to detect if the timer was cancelled/replaced even if callback is queued in event loop
  const executionId = `${timerKey}-${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
  timerExecutionIds.set(timerKey, executionId);


  try {
    // Schedule the next iteration with setTimeout
    const timeoutId = setTimeout(async () => {
      // GUARD 1: Check if this timer was cancelled (different executionId means new timer was created or timer was cancelled)
      const currentExecutionId = timerExecutionIds.get(timerKey);
      if (currentExecutionId !== executionId) {
        return;
      }

      // GUARD 2: Check if timer still exists in map (may have been cleared by cleanupTaskResources)
      if (!taskTimeouts.has(timerKey)) {
        timerExecutionIds.delete(timerKey);
        return;
      }

      // GUARD 3: Check tasksBeingCancelled Set (synchronous flag set at START of stopTask)
      // This is the DEFINITIVE check that catches ALL cancellation scenarios because:
      // - It's set synchronously at the very start of stopTask()
      // - It's set BEFORE any async operations that could allow timers to slip through
      // - It catches timers created DURING the stopTask flow but BEFORE marketplaceStatus updates
      if (tasksBeingCancelled.has(taskId)) {
        // Clean up all timers for this task
        clearTaskTimers(taskId);
        timerExecutionIds.delete(timerKey);
        return;
      }

      // GUARD 4: Check if marketplace is being cancelled or stopped (status-based check)
      // This prevents the race condition where a timer fires after stopTask has started
      // but before the timer could be cleared (timer was created AFTER cleanupTaskResources ran)
      if (marketplace) {
        const mpKey = marketplace.toLowerCase() as 'opensea' | 'magiceden' | 'blur';
        const mpStatusForGuard4 = taskStatus[taskId]?.marketplaces?.[mpKey];
        if (mpStatusForGuard4?.status === 'CANCELLING' || mpStatusForGuard4?.status === 'STOPPED') {
          // Clean up this timer since we're stopping
          const timerKeyToClean = `${taskId}-${marketplace}`;
          const existingTimer = taskTimeouts.get(timerKeyToClean);
          if (existingTimer) {
            clearTimeout(existingTimer);
            taskTimeouts.delete(timerKeyToClean);
          }
          timerExecutionIds.delete(timerKey);
          return;
        }
      } else {
        // For all-marketplace timers, check if ANY marketplace is cancelling
        const currentTaskForGuard4 = activeTasks.get(taskId);
        if (currentTaskForGuard4) {
          for (const mp of currentTaskForGuard4.selectedMarketplaces) {
            const mpLower = mp.toLowerCase() as 'opensea' | 'magiceden' | 'blur';
            const mpStatusForGuard4 = taskStatus[taskId]?.marketplaces?.[mpLower];
            if (mpStatusForGuard4?.status === 'CANCELLING' || mpStatusForGuard4?.status === 'STOPPED') {
              taskTimeouts.delete(timerKey);
              timerExecutionIds.delete(timerKey);
              return;
            }
          }
        }
      }

      // Remove self from maps immediately to prevent double-execution
      taskTimeouts.delete(timerKey);
      timerExecutionIds.delete(timerKey);

      // FIX: Change status from SCHEDULED to STARTED immediately when timer fires
      // This prevents false "Missing timer for SCHEDULED task" warnings from verifyScheduledTaskTimers()
      // The health check only triggers recovery when status === 'SCHEDULED' && !hasTimer
      // By changing status to STARTED immediately, the condition fails and no false recovery occurs
      if (marketplace) {
        setTaskStatus(taskId, 'STARTED', 'timer fired, starting loop', marketplace);
      } else {
        const timerTask = activeTasks.get(taskId);
        if (timerTask) {
          for (const mp of timerTask.selectedMarketplaces) {
            setTaskStatus(taskId, 'STARTED', 'timer fired, starting loop', mp.toLowerCase());
          }
        }
      }

      // Acquire scheduling lock when timer expires, not before
      // Use marketplace-specific lock key if marketplace is provided
      const lockKey = marketplace ? `${taskId}-${marketplace}` : taskId;
      const lockAcquired = acquireLoopSchedulingLock(lockKey);
      if (!lockAcquired) {
        // BUG FIX: Don't silently fail - reschedule timer to retry
        warnLog('LockOp',
          `TIMER_LOCK_FAILED_RETRY | timerKey=${timerKey} | lockKey=${lockKey} | rescheduling in 5s`,
          { taskId, marketplace: mpKeyForLog, timerKey, lockKey, acquired: false }
        );
        // Reschedule after 5 seconds instead of silently failing
        setTimeout(async () => {
          const currentTask = activeTasks.get(taskId);
          if (currentTask?.running && marketplace) {
            try {
              await executeMarketplaceBidLoop(taskId, marketplace);
            } catch (err) {
              errorLog('TaskRecovery', `Rescheduled timer retry failed: ${(err as Error).message}`, err);
            }
          }
        }, 5000);
        return;
      }

      // --- 6.3.1.6: Queue Counterbid Job ---
      // Deterministic job ID prevents duplicate jobs in BullMQ
      try {
        // Double-check task is still active and running
        const currentTask = activeTasks.get(taskId);
        const currentStatus = taskStatus[taskId];

        // Only skip if all marketplaces are STOPPED (not CANCELLING, as user might want to restart)
        const allMarketplacesStopped = currentStatus?.marketplaces &&
          Object.values(currentStatus.marketplaces).every(mp => mp?.status === 'STOPPED');
        if (allMarketplacesStopped) {
          return;
        }

        if (currentTask?.running) {
          // INCREMENT LOOP COUNTER - at the START of each new loop when timer fires
          // This is the single source of truth for loop counting. Counter starts at 1 (initialized in startTaskInternal)
          // and increments by 1 each time the timer fires for a new loop.
          if (marketplace) {
            if (!loopCounterStats[taskId]) {
              loopCounterStats[taskId] = {};
            }
            const prevCount = loopCounterStats[taskId][marketplace] || 0;
            loopCounterStats[taskId][marketplace] = prevCount + 1;
            const newLoopCount = loopCounterStats[taskId][marketplace];
            cleanupOldParentJobs(taskId, newLoopCount);
          } else {
            // All marketplaces case - increment each marketplace's counter
            for (const mp of currentTask.selectedMarketplaces) {
              const mpLower = mp.toLowerCase();
              if (!loopCounterStats[taskId]) {
                loopCounterStats[taskId] = {};
              }
              const prevCount = loopCounterStats[taskId][mpLower] || 0;
              loopCounterStats[taskId][mpLower] = prevCount + 1;
            }
            const maxLoopCount = Math.max(...currentTask.selectedMarketplaces.map(mp => loopCounterStats[taskId][mp.toLowerCase()] || 1));
            cleanupOldParentJobs(taskId, maxLoopCount);
          }

          // Direct timer execution - call processor functions directly without SCHEDULE jobs
          if (marketplace) {
            // Execute specific marketplace that triggered this timer
            await executeMarketplaceBidLoop(taskId, marketplace);
          } else {
            // Execute all marketplaces in parallel (legacy behavior)
            const marketplaces = currentTask.selectedMarketplaces.map(m => m.toLowerCase());
            await Promise.all(
              marketplaces.map(mp => executeMarketplaceBidLoop(taskId, mp))
            );
          }
        } else {
          warnLog('TimerOp',
            `TIMER_ABORTED | taskId=${taskId} | reason=task_not_running | running=${currentTask?.running}`,
            { taskId, marketplace: mpKeyForLog, reason: 'task_not_running', running: currentTask?.running }
          );
        }
      } catch (error) {
        // Log detailed error information for debugging
        const errorMessage = error instanceof Error ? error.message : String(error);
        const errorStack = error instanceof Error ? error.stack : undefined;

        errorLog('TimerOp',
          `TIMER_ERROR | taskId=${taskId} | mp=${marketplace || 'all'} | error=${errorMessage}`,
          error,
          { taskId, marketplace: mpKeyForLog, errorMessage, errorStack }
        );

      } finally {
        // Always release the lock after execution
        releaseLoopSchedulingLock(lockKey);
      }
    }, loopInterval * 1000);

    taskTimeouts.set(timerKey, timeoutId);

    // FIX: Status-Timer Atomicity - NOW set status to SCHEDULED after timer is successfully created
    // This ensures status is only SCHEDULED when a timer actually exists
    if (marketplace) {
      setTaskStatus(taskId, 'SCHEDULED', 'next loop scheduled', marketplace);
    } else {
      // Set status for all marketplaces
      for (const mp of task.selectedMarketplaces) {
        const mpLower = mp.toLowerCase() as 'opensea' | 'magiceden' | 'blur';
        setTaskStatus(taskId, 'SCHEDULED', 'next loop scheduled', mpLower);
      }
    }

    // TaskFlow logging: Timeout created successfully

  } catch (error) {
    // If scheduling fails, clean up resources
    errorLog('LoopScheduler',
      `SCHEDULE_ERROR | taskId=${taskId} | mp=${marketplace || 'all'}`,
      error,
      { taskId, marketplace: mpKeyForLog }
    );
    lastScheduleTimes.delete(taskId);
  }
}

// ============================================================
// TASK STATE PERSISTENCE FUNCTIONS (for crash recovery)
// ============================================================

// NOTE: Recovery functions removed - recovery logic was causing issues
// Removed: recoverStuckTaskTimer(), verifyScheduledTaskTimers(), cleanupStuckJobs()
// See docs/RECOVERY-FUNCTIONS.md for historical documentation

// ============================================================
// END TASK STATE PERSISTENCE FUNCTIONS
// ============================================================


// Initialize queue event handlers - called after queue is created
function initializeQueueEventHandlers() {
  queueEvents.on('completed', async ({ jobId }) => {
    try {
      // Decrement counterbid counter if this was a tracked counterbid job
      decrementCounterbidCount(jobId);

      // Handle schedule job completions for sequential execution
      if (jobId.includes('-schedule')) {
        // Extract task ID and marketplace from schedule job ID
        if (jobId.includes('-opensea-schedule')) {
          const taskId = jobId.replace('-opensea-schedule', '');
        } else if (jobId.includes('-blur-schedule')) {
          const taskId = jobId.replace('-blur-schedule', '');
        }
        return;
      }

      // Skip cancellation jobs - they are handled in the cancel functions directly
      if (jobId.includes('CANCEL_OPENSEA_BID') ||
        jobId.includes('CANCEL_OPENSEA_COUNTERBID') ||
        jobId.includes('CANCEL_OPENSEA_OUTBID') ||
        jobId.includes('CANCEL_MAGICEDEN_BID') ||
        jobId.includes('CANCEL_BLUR_BID')) {
        return;
      }

      // Skip counterbid jobs - they are fire-and-forget, no parent tracking
      // CB jobs are triggered by WebSocket events and don't participate in the
      // parent job / loop tracking system. Processing them through markJobAsProcessed()
      // causes 4,000+ SKIP_NO_PARENT_IN_LOOPS errors per run since they don't have
      // parent jobs to find.
      if (jobId.startsWith('CB-')) {
        return;
      }

      // Try to extract task and marketplace info from job ID
      let taskId: string | undefined;
      let marketplace: string | undefined;

      // Handle bid jobs (e.g., bid-taskId-marketplace-...)
      if (jobId.startsWith('bid-')) {
        const parts = jobId.split('-');
        if (parts.length >= 3) {
          taskId = parts[1];
          marketplace = parts[2];
        }
      }
      // Handle other job formats
      else {
        // Handle numeric job IDs (like 94460, 94481) - these should be tracked but may not have task context
        if (/^\d+$/.test(jobId)) {
          return; // Skip numeric job IDs as they don't relate to task tracking
        }

        // Try to find task ID in the job ID
        const parts = jobId.split('-');
        if (parts.length >= 2) {
          // Check if first part is a valid task ID
          if (activeTasks.has(parts[0])) {
            taskId = parts[0];
            marketplace = parts[1] || 'unknown';
          } else if (activeTasks.has(parts[1])) {
            taskId = parts[1];
            marketplace = parts[2] || parts[0] || 'unknown';
          }
          // Check if any part is a valid task ID
          else {
            for (let i = 0; i < parts.length; i++) {
              if (activeTasks.has(parts[i])) {
                taskId = parts[i];
                marketplace = parts[i + 1] || parts[i - 1] || 'unknown';
                break;
              }
            }
          }
        }
      }

      // Process bid job completion
      if (taskId && marketplace) {
        const task = activeTasks.get(taskId);
        if (task && task.running) {
          await markJobAsProcessed(taskId, marketplace, jobId);
        } else if (!task) {
        } else if (!task.running) {
        }
      } else {
      }
    } catch (error) {
      // Extract taskId from jobId if possible
      let extractedTaskId = 'N/A';
      if (jobId.startsWith('bid-')) {
        const parts = jobId.split('-');
        if (parts.length >= 2) {
          extractedTaskId = parts[1];
        }
      }

      errorLog('QueueEvent', `Error handling job completion for job ${jobId}, task ${extractedTaskId}`, error);
      // Stack trace included in error object above

      // Log error details for debugging
      if (error instanceof Error) {
        // Error details included in error object above
      }

      // Don't let errors in event handler crash the process
      // This ensures other jobs can still be processed
    }
  });

  queueEvents.on('failed', async ({ jobId, failedReason }) => {
    // Decrement counterbid counter if this was a tracked counterbid job
    decrementCounterbidCount(jobId);

    // TaskFlow logging: Job failed event

    try {
      // Handle schedule job failures specially - log to file
      if (jobId.includes('-schedule')) {
        // Log scheduled job failures to file
        const logsDir = path.join(process.cwd(), 'debug-logs');

        // Get the job details with fallback support
        const job = await safeGetJob(queue, jobId);
        const errorDetails = {
          timestamp: new Date().toISOString(),
          jobId,
          jobName: job?.name,
          taskId: job?.data?._id,
          failedReason,
          error: job?.failedReason,
          stackTrace: job?.stacktrace,
          attemptNumber: job?.attemptsMade,
          data: job?.data
        };


        // Write to scheduled job failures log
        const logPath = path.join(logsDir, 'scheduled-job-failures.log');
        fs.appendFileSync(logPath, JSON.stringify(errorDetails, null, 2) + '\n---\n');

        // Handle timeout failures OR BullMQ infrastructure errors by recovering the task loop
        // "Missing key for job" happens when BullMQ's Redis key is deleted/corrupted during job processing
        const isTimeoutError = failedReason?.includes('timed out after');
        const isMissingKeyError = failedReason?.includes('Missing key for job');
        const isInfrastructureError = isTimeoutError || isMissingKeyError;

        if (isInfrastructureError && jobId.includes('-schedule')) {
          const taskId = jobId.split('-')[0];
          const errorType = isTimeoutError ? 'timeout' : 'missing_key';

          // Use a distributed lock to prevent multiple restarts
          const restartLockKey = `schedule-recovery-restart:${taskId}`;
          const lockManager = new DistributedLockManager({
            lockPrefix: 'task-restart:',
            defaultTTLSeconds: 60
          });

          try {
            const lockAcquired = await lockManager.acquireLock(restartLockKey, 60);
            if (lockAcquired) {
              try {

                // Get the task
                const task = await Task.findById(taskId);
                if (task) {
                  // Clear stuck parent jobs for all marketplaces
                  const marketplaces = ['opensea', 'magiceden', 'blur'];
                  for (const mp of marketplaces) {
                    const parentJobId = getParentJobId(taskId, mp);
                    if (parentJobs.has(parentJobId)) {
                      parentJobs.delete(parentJobId);
                      processedJobsPerParent.delete(parentJobId);
                    }
                  }

                  // Clear preprocessing data to force fresh preprocessing
                  taskPreprocessing.delete(taskId);

                  // Clear any scheduled bid locks for all marketplaces
                  for (const mp of marketplaces) {
                    try {
                      releaseScheduledBidLock(taskId, mp);
                    } catch (error) {
                      // Lock may not exist - this is expected
                    }
                  }

                  // Reset task status for all marketplaces and restart
                  for (const mp of task.selectedMarketplaces) {
                    setTaskStatus(taskId, 'SCHEDULED', `restarting after ${errorType}`, mp.toLowerCase());
                  }
                  await startTask(task, true);

                  // Log the restart
                  const restartLogPath = path.join(logsDir, 'task-restarts.log');
                  const restartDetails = {
                    timestamp: new Date().toISOString(),
                    taskId,
                    reason: `schedule_job_${errorType}`,
                    failedJobId: jobId,
                    failedReason
                  };
                  fs.appendFileSync(restartLogPath, JSON.stringify(restartDetails, null, 2) + '\n---\n');

                  // Log to debug log for visibility
                  errorLog('TaskScheduler', `Task recovered from ${errorType} error | taskId=${taskId} | jobId=${jobId}`);
                }
              } finally {
                await lockManager.releaseLock(restartLockKey);
              }
            } else {
            }
          } catch (lockError) {
            errorLog('QueueEvent', `Error acquiring restart lock for task ${taskId}`, lockError);
          }

          // Don't continue with the normal retry exhaustion logic
          return;
        }

        // Check if this is the final failure (after all retries)
        if (job && job.attemptsMade >= (job.opts?.attempts || 1)) {
          const taskId = job.data?._id;
          if (taskId && taskStatus[taskId]) {

            // Extract marketplace from job ID (e.g., "taskId-opensea-schedule")
            let marketplace = 'unknown';
            if (jobId.includes('-opensea-schedule')) marketplace = 'opensea';
            else if (jobId.includes('-magiceden-schedule')) marketplace = 'magiceden';
            else if (jobId.includes('-blur-schedule')) marketplace = 'blur';

            // Create empty parent job to complete the loop
            await ensureParentJobExists(taskId, marketplace);
            const parentJobId = getParentJobId(taskId, marketplace);
            const parentJob = parentJobs.get(parentJobId);
            if (parentJob) {
              parentJob.expectedJobs = 0;
              parentJob.totalJobs = 0;
              parentJob.actualJobs = 0;
              parentJob.completedJobs = 0;
              parentJob.allJobsQueued = true;
            }

            // Transition marketplace out of stuck QUEUEING status
            const marketplaceStatus = taskStatus[taskId]?.marketplaces?.[marketplace as 'opensea' | 'magiceden' | 'blur'];
            if (marketplaceStatus && marketplaceStatus.status === 'QUEUEING') {
              // Scheduled job failed - recovering without forcing RUNNING status

              // Check if this marketplace is complete to trigger next loop
              if (parentJob && parentJob.completedJobs >= parentJob.totalJobs) {
                await checkTaskCompletionAndScheduleNext(taskId, marketplace);
              }
            }
          }
        }

        return;
      }

      // Check if this is a cancellation job
      const isCancellationJob = jobId.includes('CANCEL_OPENSEA_BID') ||
        jobId.includes('CANCEL_OPENSEA_COUNTERBID') ||
        jobId.includes('CANCEL_OPENSEA_OUTBID') ||
        jobId.includes('CANCEL_MAGICEDEN_BID') ||
        jobId.includes('CANCEL_BLUR_BID');

      // Try to extract task and marketplace info from job ID
      let taskId: string | undefined;
      let marketplace: string | undefined;

      if (isCancellationJob) {
        // Extract marketplace from job name
        if (jobId.includes('CANCEL_OPENSEA_BID') || jobId.includes('CANCEL_OPENSEA_COUNTERBID') || jobId.includes('CANCEL_OPENSEA_OUTBID')) {
          marketplace = 'opensea';
          // Extract taskId from pattern: CANCEL_OPENSEA_*-{taskId}-{orderId}
          const match = jobId.match(/CANCEL_OPENSEA_(?:BID|COUNTERBID|OUTBID)-([^-]+)-/);
          if (match) taskId = match[1];
        } else if (jobId.includes('CANCEL_MAGICEDEN_BID')) {
          marketplace = 'magiceden';
          // Extract taskId from pattern: CANCEL_MAGICEDEN_BID-{taskId}-{batchId}
          const match = jobId.match(/CANCEL_MAGICEDEN_BID-([^-]+)-/);
          if (match) taskId = match[1];
        } else if (jobId.includes('CANCEL_BLUR_BID')) {
          marketplace = 'blur';
          // Extract taskId from pattern: CANCEL_BLUR_BID-{taskId}-{orderId}
          const match = jobId.match(/CANCEL_BLUR_BID-([^-]+)-/);
          if (match) taskId = match[1];
        }

        // Process cancellation job failure (still counts as processed)
        if (taskId && marketplace) {
          const task = activeTasks.get(taskId);
          if (task) {
            // Cancellation tracking is now handled in the cancel functions directly
          }
        }
      } else {
        // Handle bid jobs (e.g., bid-taskId-marketplace-...)
        if (jobId.startsWith('bid-')) {
          const parts = jobId.split('-');
          if (parts.length >= 3) {
            taskId = parts[1];
            marketplace = parts[2];
          }
        }
        // Handle other job formats
        else {
          // Try to find task ID in the job ID
          const parts = jobId.split('-');
          if (parts.length >= 2) {
            // Check if first part is a valid task ID
            if (activeTasks.has(parts[0])) {
              taskId = parts[0];
              marketplace = parts[1] || 'unknown';
            } else if (activeTasks.has(parts[1])) {
              taskId = parts[1];
              marketplace = parts[2] || parts[0] || 'unknown';
            }
          }
        }

        // Process bid job failure
        if (taskId && marketplace) {
          // Check if this is an insufficient balance error
          if (failedReason && (failedReason.includes('InsufficientBalanceError') || failedReason.includes('INSUFFICIENT_BALANCE'))) {
            // Parse the error message to extract balance details
            // Message format: "Insufficient {currency} balance: required {X} {currency}, available {Y} {currency} for wallet {address} on collection {slug}"
            const currencyMatch = failedReason.match(/Insufficient (\w+) balance/);
            const requiredMatch = failedReason.match(/required ([\d.]+) \w+/);
            const availableMatch = failedReason.match(/available ([\d.]+) \w+/);
            const walletMatch = failedReason.match(/for wallet (0x[a-fA-F0-9]{40})/);

            if (currencyMatch && requiredMatch && availableMatch && walletMatch) {
              const currency = currencyMatch[1];
              const requiredAmount = parseFloat(requiredMatch[1]);
              const availableAmount = parseFloat(availableMatch[1]);
              const wallet = walletMatch[1];

              // Immediately complete the marketplace due to insufficient balance
              await completeTaskDueToInsufficientBalance(taskId, marketplace, {
                wallet,
                requiredAmount,
                availableAmount,
                currency
              });

              // Don't increment error stats for balance errors (it's not really an error in job processing)
              // Just mark the job as processed and return
              const task = activeTasks.get(taskId);
              if (task && task.running) {
                await safeMarkJobAsProcessed(taskId, marketplace, jobId, false, async (tid, mp, increment) => {
                  await markJobAsProcessed(tid, mp, jobId, false);
                });
              }
              return; // Skip normal error handling
            }
          }

          // Check for OpenSea's API balance error (e.g., "does not have enough balance of payment asset")
          // This catches balance drops that weren't detected by pre-bid balance checks
          if (failedReason && failedReason.includes('does not have enough balance')) {
            const balanceError = parseOpenseaBalanceError(failedReason);
            if (balanceError) {
              const { found, required, currency } = balanceError;
              const foundEther = Number(found) / 1e18;
              const requiredEther = Number(required) / 1e18;

              console.log(`[Balance Error] Detected OpenSea balance error for ${currency}: found ${foundEther.toFixed(6)}, required ${requiredEther.toFixed(6)}`);

              // Get wallet address from job data
              const failedJob = await safeGetJob(queue, jobId);
              const walletAddress = failedJob?.data?.walletId ? await (async () => {
                const wallet = await walletCache.getWalletById(failedJob.data.walletId);
                return wallet?.address;
              })() : null;

              if (walletAddress) {
                // Invalidate the balance cache immediately so next check gets fresh data
                const tokenAddress = getTokenAddressForCurrency(currency);
                if (tokenAddress) {
                  const { invalidateBalanceCache } = await import('./utils/balance');
                  await invalidateBalanceCache(walletAddress);
                }

                // Stop all marketplaces that use this currency
                const affectedMarketplaces = getMarketplacesForCurrency(currency);
                console.log(`[Balance Error] Stopping marketplaces for ${currency}: ${affectedMarketplaces.join(', ')}`);

                for (const mp of affectedMarketplaces) {
                  await completeTaskDueToInsufficientBalance(taskId, mp, {
                    wallet: walletAddress,
                    requiredAmount: requiredEther,
                    availableAmount: foundEther,
                    currency
                  });
                }

                // Mark job as processed
                const task = activeTasks.get(taskId);
                if (task && task.running) {
                  await safeMarkJobAsProcessed(taskId, marketplace, jobId, false, async (tid, mp, _increment) => {
                    await markJobAsProcessed(tid, mp, jobId, false);
                  });
                }
                return; // Skip normal error handling
              }
            }
          }

          if (!errorStats[taskId]) {
            errorStats[taskId] = {
              opensea: 0,
              blur: 0,
              magiceden: 0
            }
          }
          // Increment error stats for the specific marketplace
          if (marketplace === 'opensea' || marketplace === 'blur' || marketplace === 'magiceden') {
            errorStats[taskId][marketplace]++;
          }
          const task = activeTasks.get(taskId);
          if (task && task.running) {
            await safeMarkJobAsProcessed(taskId, marketplace, jobId, false, async (tid, mp, increment) => {
              await markJobAsProcessed(tid, mp, jobId, false);
            });
          }
        }
      }
    } catch (error) {
      // Extract taskId from jobId if possible
      let extractedTaskId = 'N/A';
      if (jobId.startsWith('bid-')) {
        const parts = jobId.split('-');
        if (parts.length >= 2) {
          extractedTaskId = parts[1];
        }
      }

      errorLog('QueueEvent', `Error handling job failure for job ${jobId}, task ${extractedTaskId}`, error);
      // Stack trace included in error object above

      // Log error details for debugging
      if (error instanceof Error) {
        // Error details included in error object above
      }

      // Don't let errors in event handler crash the process
      // This ensures other jobs can still be processed
    }
  });

  // Handle stalled jobs - critical for preventing stuck tasks
  queueEvents.on('stalled', async ({ jobId }) => {
    try {

      // Skip schedule jobs - they are orchestration jobs, not actual bid jobs
      if (jobId.includes('-schedule')) {
        return;
      }

      // Check if this is a cancellation job
      const isCancellationJob = jobId.includes('CANCEL_OPENSEA_BID') ||
        jobId.includes('CANCEL_OPENSEA_COUNTERBID') ||
        jobId.includes('CANCEL_OPENSEA_OUTBID') ||
        jobId.includes('CANCEL_MAGICEDEN_BID') ||
        jobId.includes('CANCEL_BLUR_BID');

      // Try to extract task and marketplace info from job ID
      let taskId: string | undefined;
      let marketplace: string | undefined;

      if (isCancellationJob) {
        // Extract marketplace from job name
        if (jobId.includes('CANCEL_OPENSEA_BID') || jobId.includes('CANCEL_OPENSEA_COUNTERBID') || jobId.includes('CANCEL_OPENSEA_OUTBID')) {
          marketplace = 'opensea';
          // Extract taskId from pattern: CANCEL_OPENSEA_*-{taskId}-{orderId}
          const match = jobId.match(/CANCEL_OPENSEA_(?:BID|COUNTERBID|OUTBID)-([^-]+)-/);
          if (match) taskId = match[1];
        } else if (jobId.includes('CANCEL_MAGICEDEN_BID')) {
          marketplace = 'magiceden';
          // Extract taskId from pattern: CANCEL_MAGICEDEN_BID-{taskId}-{batchId}
          const match = jobId.match(/CANCEL_MAGICEDEN_BID-([^-]+)-/);
          if (match) taskId = match[1];
        } else if (jobId.includes('CANCEL_BLUR_BID')) {
          marketplace = 'blur';
          // Extract taskId from pattern: CANCEL_BLUR_BID-{taskId}-{orderId}
          const match = jobId.match(/CANCEL_BLUR_BID-([^-]+)-/);
          if (match) taskId = match[1];
        }

        // Process stalled cancellation job (treat as processed to unblock)
        if (taskId && marketplace) {
          const task = activeTasks.get(taskId);
          if (task) {
            // Cancellation tracking is now handled in the cancel functions directly
          }
        }
      } else {
        // Handle bid jobs (e.g., bid-taskId-marketplace-...)
        if (jobId.startsWith('bid-')) {
          const parts = jobId.split('-');
          if (parts.length >= 3) {
            taskId = parts[1];
            marketplace = parts[2];
          }
        }
        // Handle other job formats
        else {
          // Try to find task ID in the job ID
          const parts = jobId.split('-');
          if (parts.length >= 2) {
            // Check if first part is a valid task ID
            if (activeTasks.has(parts[0])) {
              taskId = parts[0];
              marketplace = parts[1] || 'unknown';
            } else if (activeTasks.has(parts[1])) {
              taskId = parts[1];
              marketplace = parts[2] || parts[0] || 'unknown';
            }
            // Check if any part is a valid task ID
            else {
              for (let i = 0; i < parts.length; i++) {
                if (activeTasks.has(parts[i])) {
                  taskId = parts[i];
                  marketplace = parts[i + 1] || parts[i - 1] || 'unknown';
                  break;
                }
              }
            }
          }
        }

        // Process stalled bid job (treat as completed to unblock stuck tasks)
        if (taskId && marketplace) {
          const task = activeTasks.get(taskId);
          if (task && task.running) {
            await safeMarkJobAsProcessed(taskId, marketplace, jobId, true, async (tid, mp, increment) => {
              await markJobAsProcessed(tid, mp, jobId);
            });
          }
        } else {
        }
      }
    } catch (error) {
      // Extract taskId from jobId when job is not in scope
      const parts = jobId.split('-');
      const stalledTaskId = (parts.length >= 3 && parts[0] === 'BID' && parts[2]) ? parts[2] : 'N/A';
      errorLog('QueueEvent', `Error handling stalled job ${jobId}, task ${stalledTaskId}`, error);
      // Stack trace included in error object above
      // Error object already logged

      // Log error details for debugging
      if (error instanceof Error) {
        // Error details included in error object above
        // Error message included in error object above
      }

      // Don't let errors in event handler crash the process
      // This ensures other jobs can still be processed
    }
  });

  // Blur queue events removed - Blur jobs now processed by main queue
  // Events are handled by the main queueEvents handlers above
}

bidRates = getAllBidRates();
const clientData = {
  bidRates: bidRates,
  bidCounts: bidStats,
  skipCounts: skipStats,
  errorCounts: errorStats,
  floorPrices,
  bestOffers,
  warningBids,
  warningMessage,
  rps,
  taskStatus,
  loopCounterStats,
  counterbidCounts
};

const HEARTBEAT_INTERVAL_MS = 30000; // 30 second heartbeat for connection health

// Global message caching for performance optimization
let lastGlobalHash: string | null = null;
let lastGlobalMessage: string | null = null;

/**
 * Fast hash function for global message deduplication
 */
function fastHash(str: string): string {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash = hash & hash; // Convert to 32-bit integer
  }
  return hash.toString(36);
}

// Broadcast counter for periodic full refresh
let broadcastCounter = 0;

/**
 * Optimized publish function with delta detection and message reuse
 * Publishes task metrics via WebSocket with subscription filtering
 * Metrics include bid rates, counts, RPS, and task status
 */
async function publishTaskMetrics() {
  // Early exit if no clients connected
  if (clients.size === 0) {
    return;
  }

  broadcastCounter++;
  const forceUpdate = (broadcastCounter % 10 === 0);  // Force update every 10 intervals (10 seconds)

  bidRates = getAllBidRates();

  // Global data (broadcast only if changed)
  const globalData = {
    bidRates,
    rps,
    ownBidCounts,
    connections: clients.size,
    timestamp: Date.now()
  };

  // Hash-based delta detection for global data
  const globalDataStr = JSON.stringify(globalData);
  const globalHash = fastHash(globalDataStr);

  if (globalHash !== lastGlobalHash || forceUpdate) {
    // Data changed OR forced update - serialize once and broadcast
    lastGlobalMessage = JSON.stringify({
      type: 'globalUpdate',
      data: globalData
    });
    lastGlobalHash = globalHash;
    broadcastManager.broadcastRaw(lastGlobalMessage);
  }

  // Per-task updates (with delta detection in BroadcastManager)
  const tasks = Object.keys(bidStats);

  // DIAGNOSTIC: Check for orphaned tasks (in taskStatus but not in bidStats)
  const orphanedTasks = Object.keys(taskStatus).filter(taskId => !bidStats[taskId]);
  const runningOrphanedTasks = orphanedTasks.filter(taskId => {
    const status = taskStatus[taskId];
    return Object.values(status.marketplaces).some(
      mp => ['RUNNING', 'WAITING', 'QUEUED', 'QUEUEING', 'STARTED', 'PROCESSING'].includes(mp.status)
    );
  });

  if (runningOrphanedTasks.length > 0) {
    warnLog('TaskMetrics',
      `ORPHANED_RUNNING_TASKS | count=${runningOrphanedTasks.length} | taskIds=[${runningOrphanedTasks.join(',')}] | ` +
      `reason=running tasks missing bidStats - will auto-initialize`,
      { orphanedTasks: runningOrphanedTasks });
  }

  // Log stopped orphaned tasks at debug level (this is expected)
  const stoppedOrphanedTasks = orphanedTasks.filter(taskId => !runningOrphanedTasks.includes(taskId));
  if (stoppedOrphanedTasks.length > 0) {
  }

  // AUTO-FIX: Initialize bidStats for running orphaned tasks
  if (orphanedTasks.length > 0) {
    for (const taskId of orphanedTasks) {
      const status = taskStatus[taskId];

      // Check if any marketplace is actually running (not STOPPED/IDLE/COMPLETED)
      const isRunning = Object.values(status.marketplaces).some(
        mp => ['RUNNING', 'WAITING', 'QUEUED', 'QUEUEING', 'STARTED', 'PROCESSING'].includes(mp.status)
      );

      if (isRunning) {
        // Auto-initialize missing bidStats for running tasks
        if (!bidStats[taskId]) {
          bidStats[taskId] = { opensea: 0, magiceden: 0, blur: 0 };
        }

        // Also initialize related stats if missing
        if (!skipStats[taskId]) {
          skipStats[taskId] = { opensea: 0, magiceden: 0, blur: 0 };
        }
        if (!errorStats[taskId]) {
          errorStats[taskId] = { opensea: 0, magiceden: 0, blur: 0 };
        }
        if (!loopCounterStats[taskId]) {
          loopCounterStats[taskId] = {};
        }
      } else {
        // Task is stopped - this is expected, just log at debug level
      }
    }
  }

  for (const taskId of tasks) {
    const taskData = {
      bidCounts: bidStats[taskId],
      skipCounts: skipStats[taskId],
      errorCounts: errorStats[taskId],
      floorPrices: floorPrices[taskId],
      bestOffers: bestOffers[taskId],
      warningBids: warningBids[taskId],
      warningMessage: warningMessage[taskId],
      taskStatus: taskStatus[taskId],
      loopCounterStats: loopCounterStats[taskId],
      counterbidCounts: counterbidCounts[taskId]
    };

    // Delta detection + broadcast (only if changed)
    // Note: BroadcastManager has forceUpdate flag to temporarily disable delta detection
    broadcastManager.broadcastTaskUpdate(taskId, taskData);
  }
}

// Periodic heartbeat to maintain WebSocket connections and ensure clients stay connected
setInterval(() => {
  const heartbeat = JSON.stringify({
    type: 'heartbeat',
    timestamp: Date.now()
  });

  const activeConnections = broadcastManager.broadcastRaw(heartbeat);

  if (activeConnections > 0) {
  }
}, HEARTBEAT_INTERVAL_MS);

// Real-time bid statistics publishing
// Configurable interval (default 1 second for near real-time updates)
const BROADCAST_INTERVAL = 1000
setInterval(() => {
  // Only publish if we have connected clients
  if (clients.size > 0) {
    publishTaskMetrics();
  }
}, BROADCAST_INTERVAL);

// Broadcast instant price match notification to all connected clients
export function broadcastInstantPriceMatch(data: {
  listingId: string;
  tokenId: string;
  marketplace: string;
  competitorPrice: number;
  newPrice: number;
  action: string;
}) {
  const message = JSON.stringify({
    type: 'instantPriceMatch',
    data
  });

  broadcastManager.broadcastRaw(message);
}

// Update the WebSocket connection handler to send initial state
wss.on('connection', async (ws) => {
  // Register with broadcast manager
  broadcastManager.addClient(ws);

  // AUTO-SUBSCRIBE to all active tasks
  const activeTaskIds = Array.from(activeTasks.keys());
  let successCount = 0;
  let failCount = 0;
  for (const taskId of activeTaskIds) {
    try {
      broadcastManager.subscribe(ws, taskId);
      successCount++;
      // Per-task log removed - was logging 28+ lines per connection (125MB/day)
    } catch (error) {
      failCount++;
      console.error(`[WebSocket] Auto-subscribe FAILED | taskId=${taskId} | error=${error}`);
    }
  }
  // Summary log preserved - single line shows count

  // Send the latest bid rates immediately upon connection
  if (ws.readyState === WebSocket.OPEN) {
    const initialMessage = JSON.stringify({
      type: "bidRatesUpdate",
      data: {
        bidRates,
        bidCounts: bidStats,
        skipCounts: skipStats,
        errorCounts: errorStats,
        floorPrices,
        bestOffers,
        warningBids,
        warningMessage,
        rps,
        taskStatus,
        ownBidCounts,
        loopCounterStats,
        counterbidCounts
      },
    });

    ws.send(initialMessage);
  }

  // Helper functions for message idempotency
  function isDuplicateMessage(messageId: string): boolean {
    // LRUCache.has() returns false for expired items automatically
    return processedMessages.has(messageId);
  }

  function markMessageProcessed(messageId: string): void {
    // LRUCache automatically handles TTL and max size
    processedMessages.set(messageId, Date.now());
    // No manual cleanup needed!
  }

  ws.onmessage = async (event: WebSocket.MessageEvent) => {
    let messageId: string | undefined;
    let endpoint: string = 'unknown';

    try {
      const message = JSON.parse(event.data as string);
      messageId = message._id;
      endpoint = message.endpoint;

      // Validate message structure
      if (!endpoint) {
        throw new Error("Invalid message structure: missing endpoint");
      }

      // Check for duplicate (idempotency)
      if (messageId && isDuplicateMessage(messageId)) {
        // Send acknowledgment for duplicate
        ws.send(JSON.stringify({
          type: 'ack',
          messageId,
          endpoint,
          status: 'duplicate',
          timestamp: Date.now()
        }));
        return;
      }

      // Store message ID to prevent duplicates
      if (messageId) {
        markMessageProcessed(messageId);
      }

      let result: any;

      switch (endpoint) {
        case 'updated-task':
          // Validate task data
          if (!message.data || !message.data._id) {
            throw new Error("Missing task data or task ID in update");
          }
          result = await updateTaskData(message.data);
          break;

        case 'new-task':
          result = await processNewTask(message.data);
          break;

        case "import-tasks":
          result = await processImportedTasks(message.data);
          break;

        case 'toggle-status':
          result = await updateStatus(message.data);
          break;

        case 'stop-task':
          result = await stopTask(message.data, false);
          break;

        case 'delete-task':
          result = await deleteTask(message.data);
          break;

        case 'update-multiple-tasks-status':
          result = await updateMultipleTasksStatus(message.data);
          break;

        case 'globalMarketplaces':
          result = processGlobalMarketplaces(message.data);
          break;

        case 'update-marketplace':
          result = await updateMarketplace(message.data);
          break;

        case 'create-listing':
          result = await handleStartListing(message.data, ws);
          break;

        case 'stop-listing':
          result = await handleStopListing(message.data, ws);
          break;

        case 'update-listing':
          result = await handleUpdateListing(message.data, ws);
          break;

        case 'subscribe':
          // Subscribe to task updates via broadcast manager
          if (!message.data?.taskId) {
            throw new Error("Missing taskId in subscribe message");
          }
          const subscribeSuccess = broadcastManager.subscribe(ws, message.data.taskId);
          result = { subscribed: subscribeSuccess, taskId: message.data.taskId };
          break;

        case 'unsubscribe':
          // Unsubscribe from task updates
          if (!message.data?.taskId) {
            throw new Error("Missing taskId in unsubscribe message");
          }
          const unsubscribeSuccess = broadcastManager.unsubscribe(ws, message.data.taskId);
          result = { unsubscribed: unsubscribeSuccess, taskId: message.data.taskId };
          break;

        case 'catchup':
          // Catchup no longer supported (MessageBuffer removed for memory optimization)
          // Clients will receive next scheduled update after reconnection
          result = { error: 'Catchup no longer supported', taskId: message.data?.taskId };
          break;

        default:
          throw new Error(`Unknown endpoint: ${endpoint}`);
      }

      // Send success acknowledgment
      ws.send(JSON.stringify({
        type: 'ack',
        messageId,
        endpoint,
        status: 'success',
        result,
        timestamp: Date.now()
      }));

    } catch (error) {
      errorLog('API', `WebSocket message handler failed: endpoint=${endpoint}, messageId=${messageId}`, error as Error);

      // Send error acknowledgment to client
      try {
        ws.send(JSON.stringify({
          type: 'ack',
          messageId,
          endpoint,
          status: 'error',
          error: (error as Error).message,
          timestamp: Date.now()
        }));
      } catch (sendError) {
        // WebSocket might be closed
        errorLog('API', 'Failed to send error acknowledgment', sendError as Error);
      }
    }
  };

  ws.onclose = () => {
    // Remove client and cleanup subscriptions
    broadcastManager.removeClient(ws);
  };
});

// Blur rate limiting is now handled by Redis and the BlurRateLimiter
// No need for in-memory tracking as we trust Blur's API responses

// Process Blur jobs
async function processJob(job: Job, signal?: AbortSignal) {
  const startTime = Date.now();
  const { marketplace, type } = job.data || {};
  // Extract taskId with fallback to _id (some jobs use _id instead of taskId)
  const taskId = job.data?.taskId || job.data?._id?.toString() || 'none';

  // Track job pickup latency - how long the job waited in queue before worker picked it up
  const pickupTime = Date.now();
  const queuedAt = job.timestamp || pickupTime; // job.timestamp is when job was added to queue
  const pickupLatencyMs = pickupTime - queuedAt;
  if (pickupLatencyMs > 1000) {
    // Only log if pickup latency > 1 second (indicates queue backlog)
    warnLog('WorkerJob', `JOB_PICKUP_SLOW | jobId=${job.id} | taskId=${taskId} | pickupLatencyMs=${pickupLatencyMs} | queueWaitSec=${(pickupLatencyMs / 1000).toFixed(1)}`);
  }

  // Declare walletAddress outside try block for Blur rate limiting
  let walletAddress = '';

  try {
    // Update progress to indicate job started
    // Progress reporting removed - await job.updateProgress(5);

    // IMMEDIATE blacklist check before ANY processing - prevents stopped task jobs from executing
    // EXCEPT: CANCEL_* jobs must always execute for cleanup during task stop
    const isCancellationJob = [
      CANCEL_OPENSEA_BID,
      CANCEL_OPENSEA_COUNTERBID,
      CANCEL_OPENSEA_OUTBID,
      CANCEL_MAGICEDEN_BID,
      CANCEL_BLUR_BID
    ].includes(job.name);

    if (taskId && !isCancellationJob && isTaskBlacklisted(taskId, marketplace)) {
      warnLog('JobSkip', `BLACKLIST_SKIP | jobId=${job.id} | taskId=${taskId} | marketplace=${marketplace || 'unknown'} | type=${type || job.name} | reason=task_blacklisted_at_worker_entry`);
      // CRITICAL: Mark job as processed to allow loop completion tracking
      await markSkippedJobAsProcessed(job);
      return { skipped: true, reason: 'task_blacklisted' };
    }

    // Check if password is available before processing
    if (!getPassword()) {
      const error = new Error('No password available. Job will be retried when password is set.');
      (error as any).code = 'NO_PASSWORD';
      throw error;
    }

    // Check if already aborted
    if (signal?.aborted) {
      throw new Error('Job aborted before processing');
    }

    // Detect if this is a Blur job for rate limiting
    const isBlurJob = [
      BLUR_COLLECTION_BID,
      BLUR_TRAIT_BID,
      BLUR_COLLECTION_BID_COUNTERBID,
      BLUR_TRAIT_BID_COUNTERBID
    ].includes(job.name);

    // Blur-specific rate limiting (only for Blur jobs)
    if (isBlurJob) {
      // Extract wallet address from job data first (more reliable than parsing jobId)
      walletAddress = job.data?.address || job.data?.wallet;
      if (!walletAddress) {
        // Fallback to parsing jobId if address not in data
        const jobId = (job.opts as any)?.jobId || job.id;
        const jobParts = jobId.toString().split('-');
        const parsedWallet = jobParts[jobParts.length - 1];

        if (!parsedWallet || !parsedWallet.startsWith('0x')) {
          errorLog('Blur', `Cannot determine wallet address for job | taskId=${taskId}`);
          errorLog('BlurJob', `Cannot determine wallet address for job ${job.id}`, undefined);
          throw new Error('Cannot determine wallet address for rate limiting');
        }
        walletAddress = parsedWallet;
      }

      // CRITICAL: Check rate limits BEFORE processing the job
      if (walletAddress && walletAddress.startsWith('0x')) {
        // First check if we can make a request using the rate limiter
        const canProceed = blurRateLimiter.canMakeRequest(walletAddress);
        if (!canProceed) {
          const waitTime = blurRateLimiter.getWaitTime(walletAddress);

          try {
            // Reschedule the job with a delay
            await job.moveToDelayed(Date.now() + waitTime + 1000); // Add 1s buffer
          } catch (error) {
            // If we can't move to delayed (e.g., lock already released), job will be retried automatically
            warnLog('Queue', `Failed to move job to delayed (rate limit) | wallet=${walletAddress} | delay=${waitTime}ms`);
          }
          return; // Exit without processing
        }

        // Check completion-based rate limiting
        const requiredDelay = blurCompletionTracker.getRequiredDelay(walletAddress);
        if (requiredDelay > 0) {
          try {
            // Reschedule the job with a delay
            await job.moveToDelayed(Date.now() + requiredDelay + 1000); // Add 1s buffer
          } catch (error) {
            // If we can't move to delayed (e.g., lock already released), job will be retried automatically
            warnLog('Queue', `Failed to move job to delayed (completion) | wallet=${walletAddress} | delay=${requiredDelay}ms`);
          }
          return; // Exit without processing
        }

        // Check for authentication blocks first
        try {
          // Check authentication block (LRU cache)
          const block = blurAuthBlockLRU.get(walletAddress.toLowerCase());
          if (block && block.blockedUntil > Date.now()) {
            const waitTime = block.blockedUntil - Date.now();
            // Wait for auth block to expire
            await new Promise(resolve => setTimeout(resolve, waitTime + 2500));
          } else if (block) {
            // Block has expired, clean it up
            blurAuthBlockLRU.delete(walletAddress.toLowerCase());
          }

          // Then check rate limit pauses (LRU cache)
          const pause = blurWalletPauseLRU.get(walletAddress.toLowerCase());
          if (pause && pause.pauseUntil > Date.now()) {
            const waitTime = pause.pauseUntil - Date.now();
            const waitSeconds = Math.round(waitTime / 1000);

            // Wait for pause to expire
            await new Promise(resolve => setTimeout(resolve, waitTime + 2500));
          } else if (pause) {
            // Pause has expired, clean it up
            blurWalletPauseLRU.delete(walletAddress.toLowerCase());
          }
        } catch (error) {
          errorLog('Queue', `Redis auth/pause check failed | wallet=${walletAddress}`, error);
        }
      }
    }

    const result = await (async () => {
      switch (job.name) {
        case OPENSEA_COLLECTION_BID:
          await processOpenseaCollectionBid(job.data);
          return { completed: true };
        case MAGICEDEN_COLLECTION_BID:
          await processMagicedenCollectionBid(job.data);
          return { completed: true };
        case OPENSEA_COLLECTION_BID_COUNTERBID:
          await openseaCollectionCounterBid(job);
          return { completed: true };
        case MAGICEDEN_COLLECTION_BID_COUNTERBID:
          await magicedenCollectionCounterBid(job);
          return { completed: true };
        case OPENSEA_TOKEN_BID_COUNTERBID:
          await openseaTokenCounterBid(job);
          return { completed: true };
        case OPENSEA_TRAIT_BID_COUNTERBID:
          await openseaTraitCounterBid(job);
          return { completed: true };
        case MAGICEDEN_TOKEN_BID_COUNTERBID:
          await magicedenTokenCounterBid(job);
          return { completed: true };
        case MAGICEDEN_TRAIT_BID_COUNTERBID:
          await magicedenTraitCounterBid(job);
          return { completed: true };
        // OPENSEA_SCHEDULE case removed - now using direct timer execution via executeMarketplaceBidLoop()
        case OPENSEA_TRAIT_BID:
          await processOpenseaTraitBid(job.data);
          return { completed: true };
        case OPENSEA_TOKEN_BID:
          await processOpenseaTokenBid(job.data);
          return { completed: true };
        // MAGICEDEN_SCHEDULE case removed - now using direct timer execution via executeMarketplaceBidLoop()
        case MAGICEDEN_TRAIT_BID:
          await processMagicedenTraitBid(job.data);
          return { completed: true };
        case MAGICEDEN_TOKEN_BID:
          await processMagicedenTokenBid(job.data);
          return { completed: true };
        case CANCEL_OPENSEA_BID:
        case CANCEL_OPENSEA_COUNTERBID:
        case CANCEL_OPENSEA_OUTBID:
          try {
            const { taskId, orderId, identifier, source } = job.data;
            const jobType = job.name;
            // Log worker entry

            // Log before API call

            await bulkCancelOpenseaBid(job.data);

            // Log after successful cancellation

            return { completed: true };
          } catch (error) {
            const { taskId, orderId, identifier } = job.data;

            // Log cancellation error
            errorLog('Cancellation',
              `[CANCEL_API_ERROR] Failed to cancel order | orderId=${orderId?.substring(0, 16)}... | jobType=${job.name}`,
              error,
              { taskId, marketplace: 'opensea', identifier, orderHash: orderId });

            // Track the failure as a completion to ensure we reach 100%
            cancellationTracker.recordCompletion(taskId, 1, 'opensea');
            await updateTaskCancellationPercentage(taskId, undefined, 'opensea');
            throw error; // Re-throw to let the job fail properly
          }
        case CANCEL_MAGICEDEN_BID:
          try {
            // Log when CANCEL_MAGICEDEN_BID job is being processed
            const { taskId, orderIds } = job.data;

            await bulkCancelMagicedenBid(job.data);
          } catch (error) {
            errorLog('CancelMagicedenBid', 'Failed to cancel order', error);
            const { taskId } = job.data;
            cancellationTracker.recordCompletion(taskId, 1, 'magiceden');
            await updateTaskCancellationPercentage(taskId, undefined, 'magiceden');
            throw error; // Re-throw to let the job fail properly
          }
          return { completed: true };
        case CANCEL_BLUR_BID:
          try {
            // Log when CANCEL_BLUR_BID job is being processed
            const { taskId, orderKey } = job.data;

            await bulkCancelBlurBid(job.data);
          } catch (error) {
            errorLog('CancelBlurBid', 'Failed to cancel order', error);
            const { taskId } = job.data;
            cancellationTracker.recordCompletion(taskId, 1, 'blur');
            await updateTaskCancellationPercentage(taskId, undefined, 'blur');
            throw error; // Re-throw to let the job fail properly
          }
          return { completed: true };
        case BLUR_COLLECTION_BID:
          await processBlurCollectionBid(job.data);
          return { completed: true };
        case BLUR_TRAIT_BID:
          await processBlurTraitBid(job.data);
          return { completed: true };
        case BLUR_COLLECTION_BID_COUNTERBID:
          await blurCollectionCounterBid(job);
          return { completed: true };
        case BLUR_TRAIT_BID_COUNTERBID:
          await blurTraitCounterBid(job);
          return { completed: true };
        case PROCESS_LISTING:
          return await processListingJob(job);
        case PROCESS_LISTING_UPDATE:
          const updateResult = await processListingUpdateJob(job);
          return updateResult;
        default:
          return { completed: false, error: 'Unknown job type' };
      }
    })();
    return result;
  } catch (error) {
    const durationMs = Date.now() - startTime;

    // Track completion even on error for Blur jobs (for rate limiting purposes)
    if (walletAddress && walletAddress.startsWith('0x')) {
      blurCompletionTracker.trackCompletion(walletAddress);
    }

    throw error;
  } finally {
    // Track successful completion for Blur jobs
    if (walletAddress && walletAddress.startsWith('0x')) {
      blurCompletionTracker.trackCompletion(walletAddress);
    }
  }
}

interface TaskLock {
  locked: boolean;
  lockUntil: number;
  lastReleaseTime?: number;
}

/**
 * Process listing update job - handles instant price matching updates
 */
async function processListingUpdateJob(job: Job): Promise<boolean> {
  const { listingId, newPrice, competitorPrice, marketplace } = job.data;

  try {
    // Fetch the listing
    const listing = await Listing.findById(listingId);
    if (!listing || !listing.running) {
      return false;
    }

    // Update listing with instant price override
    const durationInSeconds = listing.listDuration.unit.toLowerCase() === 'minutes' ? listing.listDuration.value * 60 :
      listing.listDuration.unit.toLowerCase() === 'hours' ? listing.listDuration.value * 60 * 60 :
        listing.listDuration.unit.toLowerCase() === 'days' ? listing.listDuration.value * 60 * 60 * 24 : 900;
    const instantPriceMatchingActiveUntil = new Date(Date.now() + durationInSeconds * 1000);

    await Listing.findByIdAndUpdate(listingId, {
      instantPriceOverride: newPrice,
      instantPriceMatchingActiveUntil: instantPriceMatchingActiveUntil,
    });

    // Fetch fresh listing data
    const freshListing = await Listing.findById(listingId);
    if (!freshListing) {
      return false;
    }

    // Clear cache before creating listing to ensure fresh price comparison
    const { listingPriceCache } = await import('./utils/listing-price-cache');
    await listingPriceCache.clearPrice(freshListing.contract.contractAddress, freshListing.tokenId);

    // Create listing with adjusted price
    const adjustedListing = {
      ...freshListing.toObject(),
      instantPriceOverride: newPrice,
    } as IListing;

    await createListing(adjustedListing);

    // Ensure the listing has a scheduled job for continuous listing
    // This is important because instant price matching relies on the regular
    // scheduled job to continue listing with the override price
    if (freshListing.running && freshListing.loopInterval) {
      try {
        await scheduleListing(freshListing);
      } catch (scheduleError) {
        errorLog('ListingSchedule', `Failed to ensure scheduled job for listing ${listingId}`, scheduleError);
      }
    }

    // Broadcast instant price match notification
    broadcastInstantPriceMatch({
      listingId: listingId.toString(),
      tokenId: listing.tokenId,
      marketplace,
      competitorPrice,
      newPrice,
      action: 'price_adjusted'
    });

    return true;
  } catch (error) {
    errorLog('ListingUpdate', `Failed to process listing update for ${listingId}`, error);
    throw error;
  }
}



/**
 * Process failed task start retries.
 * Runs every 5 seconds to check for tasks ready to retry.
 */
async function processFailedTaskRetries(): Promise<void> {
  const now = Date.now();
  const readyToRetry: FailedTaskRetry[] = [];

  // Find tasks ready to retry
  for (const [taskId, entry] of failedTaskStartQueue.entries()) {
    if (entry.nextRetryAt <= now) {
      readyToRetry.push(entry);
    }
  }

  if (readyToRetry.length === 0) return;


  for (const entry of readyToRetry) {
    const taskId = entry.task._id?.toString();
    const taskSlug = entry.task.contract?.slug || taskId;

    // Check if task should still be running (user might have stopped it)
    const currentTask = taskId ? activeTasks.get(taskId) : null;
    if (!currentTask || !currentTask.running) {
      if (taskId) failedTaskStartQueue.delete(taskId);
      continue;
    }


    try {
      await startTask(entry.task, true);
      // Success - remove from queue
      if (taskId) failedTaskStartQueue.delete(taskId);
    } catch (err) {
      // Failed again - update for next retry
      const nextRetryCount = entry.retryCount + 1;
      const backoffMs = Math.min(5000 * Math.pow(2, nextRetryCount - 1), 60000);

      if (taskId) {
        failedTaskStartQueue.set(taskId, {
          ...entry,
          retryCount: nextRetryCount,
          nextRetryAt: Date.now() + backoffMs,
          lastError: (err as Error)?.message || String(err)
        });
      }

      warnLog('TaskRetry', `Start retry #${entry.retryCount} failed for ${taskSlug}, next retry in ${backoffMs}ms: ${(err as Error)?.message}`);
    }

    // Small delay between retries to not overwhelm system
    await new Promise(resolve => setTimeout(resolve, 500));
  }
}

/**
 * Process failed task stop retries.
 * Runs every 5 seconds to check for tasks ready to retry.
 */
async function processFailedTaskStopRetries(): Promise<void> {
  const now = Date.now();
  const readyToRetry: FailedTaskRetry[] = [];

  // Find tasks ready to retry
  for (const [taskId, entry] of failedTaskStopQueue.entries()) {
    if (entry.nextRetryAt <= now) {
      readyToRetry.push(entry);
    }
  }

  if (readyToRetry.length === 0) return;


  for (const entry of readyToRetry) {
    const taskId = entry.task._id?.toString();
    const taskSlug = entry.task.contract?.slug || taskId;


    try {
      await stopTask(entry.task, false);
      // Success - remove from queue
      if (taskId) {
        failedTaskStopQueue.delete(taskId);
        failedTaskStartQueue.delete(taskId);  // Also clear start retries
      }
    } catch (err) {
      // Failed again - update for next retry
      const nextRetryCount = entry.retryCount + 1;
      const backoffMs = Math.min(5000 * Math.pow(2, nextRetryCount - 1), 60000);

      if (taskId) {
        failedTaskStopQueue.set(taskId, {
          ...entry,
          retryCount: nextRetryCount,
          nextRetryAt: Date.now() + backoffMs,
          lastError: (err as Error)?.message || String(err)
        });
      }

      warnLog('TaskRetry', `Stop retry #${entry.retryCount} failed for ${taskSlug}, next retry in ${backoffMs}ms: ${(err as Error)?.message}`);
    }

    // Small delay between retries to not overwhelm system
    await new Promise(resolve => setTimeout(resolve, 500));
  }
}

async function runScheduledLoop() {
  if (!API_KEY) {
    // Don't pause queue, just skip task creation
    return
  }

  if (!RATE_LIMIT) {
    // Don't pause queue, just skip task creation
    return
  }

  const runningActiveTasks = Array.from(activeTasks.entries())
    .filter(([_, task]) => task.running)
    .map(([_, task]) => task);

  if (runningActiveTasks.length > 0) {
    // Start tasks sequentially
    for (const task of runningActiveTasks) {
      try {
        await startTask(task, true);
      } catch (err) {
        errorLog('ScheduledLoop', `Failed to start task ${task._id}`, err);
      }
    }
  }
}

async function processImportedTasks(tasks: ITask[]) {
  try {

    tasks.forEach(task => {
      activeTasks.set(task._id, task);
      currentTasks.push(task);
      // Use O(1) index add instead of O(n) full rebuild after loop
      if (task.running) {
        addTaskToIndexes(task);
      }
    });

    // Start running tasks sequentially
    const runningTasks = tasks.filter(task => task.running);
    if (runningTasks.length > 0) {
      for (const task of runningTasks) {
        try {
          await startTask(task, true);
        } catch (err) {
          errorLog('TaskImport', `Failed to start imported task ${task._id}`, err);
        }
      }
    }

  } catch (error) {
    errorLog('TaskImport', 'Error processing imported tasks', error);
  }
}


async function processNewTask(task: ITask) {
  try {
    activeTasks.set(task._id, task)
    currentTasks.push(task);
    // Use O(1) index add instead of O(n) full rebuild
    if (task.running) {
      addTaskToIndexes(task);
    }
    await startTask(task, task.running)
  } catch (error) {
    errorLog('TaskProcessing', `Error processing new task: ${task.contract.slug}`, error);
  }
}

async function processGlobalMarketplaces(incomingMarketplaces: string[]) {
  try {
    const tasks: ITask[] = Array.from(activeTasks.values());
    const marketplacesToProcess = incomingMarketplaces.map((m) => m.toLowerCase());

    // Calculate what's changing
    const newMarketplaces = marketplacesToProcess.filter(m => !enabledMarketplaces.map((em) => em.toLowerCase()).includes(m));
    // Update enabled marketplaces first
    enabledMarketplaces = marketplacesToProcess;

    // Early return if no marketplaces to process
    if (marketplacesToProcess.length === 0) {
      return;
    }

    // Only process if there are NEW marketplaces being added
    if (newMarketplaces.length === 0) {
      return;
    }

    // Extract the marketplace to process: use the first new marketplace
    const marketplaceToProcess: string = newMarketplaces[0];


    // Filter tasks that have this marketplace selected and are running
    const tasksToProcess = tasks.filter(task =>
      task.running && task.selectedMarketplaces.map((m) => m.toLowerCase()).includes(marketplaceToProcess)
    );

    // Process each task for this specific marketplace only
    await Promise.all(tasksToProcess.map(async (task) => {
      try {
        await startTask(task, task.running, marketplaceToProcess);
      } catch (error) {
        errorLog('MarketplaceProcessing', `Error processing marketplace ${marketplaceToProcess} for task ${task._id}`, error);
      }
    }));

    // Enabled marketplaces already updated at the beginning
  } catch (error) {
    errorLog('GlobalMarketplaceProcessing', 'Error processing global marketplaces', error);
  }
}

async function cancelRemovedTokenBids(removedTokens: { opensea: string[]; magiceden: string[] }, taskId: string, walletId: string, currencySymbol: string): Promise<number> {
  let totalJobsCreated = 0;
  try {
    const openseaTokensToCancel = await orderCache.getByTaskAndIdentifiers(taskId, removedTokens.opensea, "opensea");

    const magicedenTokensToCancel = await orderCache.getByTaskAndIdentifiers(taskId, removedTokens.magiceden, "magiceden");

    if (openseaTokensToCancel.length > 0) {
      const cancelData = openseaTokensToCancel.map((order) => {
        return {
          name: CANCEL_OPENSEA_BID,
          data: { walletId: walletId, orderId: order.orderHash, taskId, identifier: order.identifier }
        };
      });
      // Log cancellation enqueue
      await queue.addBulk(cancelData);
      totalJobsCreated += cancelData.length;
    }

    for (let i = 0; i < magicedenTokensToCancel.length; i += 500) {
      const batchOrders = magicedenTokensToCancel.slice(i, i + 500);
      const extractedOrderIds = await extractMagicedenOrderHash(batchOrders)
      const orderKeys = batchOrders.map((order) => order.orderHash)
      await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, walletId: walletId, orderKeys, taskId: taskId });
      totalJobsCreated++;
    }
  } catch (error) {
    errorLog('TokenBidCancellation', 'Error canceling removed token bids', error);
  }
  return totalJobsCreated;
}

async function cancelRemovedTraitBids(traitsToRemove: string[], taskId: string, walletId: string, currencySymbol: string): Promise<number> {
  let totalJobsCreated = 0;
  try {
    const ordersToCancel = await orderCache.getByTaskAndIdentifiers(taskId, traitsToRemove);


    const blurOrders = ordersToCancel.filter(order => order.marketplace.toLowerCase() === BLUR.toLowerCase());
    const openseaOrders = ordersToCancel.filter(order => order.marketplace.toLowerCase() === OPENSEA.toLowerCase());
    const magicedenOrders = ordersToCancel.filter(order => order.marketplace.toLowerCase() === MAGICEDEN.toLowerCase());

    if (blurOrders.length > 0) {
      const cancelData = blurOrders.map((order) => {
        const payload = JSON.parse(order.orderHash);
        return {
          name: CANCEL_BLUR_BID,
          data: { walletId: walletId, payload, taskId, identifier: order.identifier }
        };
      });
      // Log cancellation enqueue
      await queue.addBulk(cancelData);
      totalJobsCreated += cancelData.length;
    }

    if (openseaOrders.length > 0) {
      const cancelData = openseaOrders.map((order) => {
        return {
          name: CANCEL_OPENSEA_BID,
          data: { walletId: walletId, orderId: order.orderHash, taskId, identifier: order.identifier }
        };
      });
      // Log cancellation enqueue
      await queue.addBulk(cancelData);
      totalJobsCreated += cancelData.length;
    }

    for (let i = 0; i < magicedenOrders.length; i += 500) {
      const batchOrders = magicedenOrders.slice(i, i + 500);
      const extractedOrderIds = await extractMagicedenOrderHash(batchOrders)
      const orderKeys = batchOrders.map((order) => order.orderHash)
      await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, walletId: walletId, orderKeys, taskId });
      totalJobsCreated++;
    }
  } catch (error) {
    errorLog('TraitBidCancellation', 'Error canceling removed trait bids', error);
  }
  return totalJobsCreated;
}


async function updateTaskData(task: ITask) {
  if (process.env.NODE_ENV === 'development') {
    console.log(`[TaskUpdate][${task._id}] Starting task update - Contract: ${task.contract?.slug || 'unknown'}`);
    console.log(`[TaskUpdate][${task._id}] Update payload:`);
  }

  // Note: No MongoDB session/transaction needed - single document updates are atomic by MongoDB design
  const existingTaskIndex = currentTasks.findIndex(t => t._id === task._id);
  const existingTask = activeTasks.get(task._id);

  if (process.env.NODE_ENV === 'development') {
    console.log(`[TaskUpdate][${task._id}] Task lookup - Array index: ${existingTaskIndex}, Map exists: ${!!existingTask}`);
  }

  // Clear both warningBids and warningMessage when task is updated
  warningBids[task._id] = {
    opensea: false,
    magiceden: false,
    blur: false
  };

  if (warningMessage[task._id]) {
    delete warningMessage[task._id];
  }

  if (existingTaskIndex === -1 || !existingTask) {
    if (process.env.NODE_ENV === 'development') {
      console.log(`[TaskUpdate][${task._id}] ERROR: Task not found - Array index: ${existingTaskIndex}, Map exists: ${!!existingTask}`);
    }
    return { success: false, error: "Task not found" };
  }

  // Update database (single document update is atomic - no transaction needed)
  if (process.env.NODE_ENV === 'development') {
    console.log(`[TaskUpdate][${task._id}] Updating database...`);
  }

  const dbTask = await Task.findOneAndUpdate(
    { _id: task._id },
    { $set: task },
    {
      new: true,
      runValidators: true
    }
  );

  if (process.env.NODE_ENV === 'development') {
    console.log(`[TaskUpdate][${task._id}] Database update completed`);
  }

  if (!dbTask) {
    if (process.env.NODE_ENV === 'development') {
      console.log(`[TaskUpdate][${task._id}] ERROR: Database returned null after update`);
    }
    errorLog('TaskUpdate', `Task not found in database: ${task._id}`, undefined);
    return { success: false, error: "Task not found in database" };
  }

  // Update in-memory state after database confirms
  if (process.env.NODE_ENV === 'development') {
    console.log(`[TaskUpdate][${task._id}] Synchronizing in-memory state...`);
  }

  // Check if loopInterval has changed
  const loopIntervalChanged = existingTask.loopInterval.value !== task.loopInterval.value ||
    existingTask.loopInterval.unit !== task.loopInterval.unit;

  const wallet = await walletCache.getWalletById(task.walletId)

  // Ensure _id and user are strings for consistency with fetchCurrentTasks()
  // This fixes the bug where bid price changes don't take effect until task restart
  const updatedTask = {
    ...dbTask.toObject(),
    _id: dbTask._id.toString(),
    user: dbTask.user?.toString() || task.user
  } as ITask;

  currentTasks.splice(existingTaskIndex, 1, updatedTask);
  activeTasks.set(task._id, updatedTask);

  // Use O(1) index update instead of O(n) full rebuild
  // Remove old task from indexes, add new task if running
  removeTaskFromIndexes(existingTask);
  if (updatedTask.running) {
    addTaskToIndexes(updatedTask);
  }

  if (process.env.NODE_ENV === 'development') {
    console.log(`[TaskUpdate][${task._id}] In-memory state updated successfully`);
  }


  const filteredTasks = Object.fromEntries(
    Object.entries(task?.selectedTraits?.opensea || {}).filter(([_, traits]) => traits.length > 0)
  );

  const openseaTraits = Object.fromEntries(
    Object.entries(task?.selectedTraits?.opensea || {}).filter(([_, traits]) => traits.length > 0)
  );

  const magicedenTraits = Object.fromEntries(
    Object.entries(task?.selectedTraits?.magiceden || {}).filter(([_, traits]) => traits.length > 0)
  );

  const blurTraits = Object.fromEntries(
    Object.entries(task?.selectedTraits?.blur || {}).filter(([_, traits]) => traits.length > 0)
  );

  const openseaSelectedTraits = transformNewTask(openseaTraits)
  const magicedenSelectedTraits = transformNewTask(magicedenTraits)
  const blurSelectedTraits = transformNewTask(blurTraits)


  if (!currentTraits[task._id]) {
    currentTraits[task._id] = {
      opensea: {},
      magiceden: {},
      blur: {}
    }
  }

  // Add defensive check before property access
  if (currentTraits[task._id]) {
    currentTraits[task._id].opensea = openseaSelectedTraits
    currentTraits[task._id].magiceden = magicedenSelectedTraits
    currentTraits[task._id].blur = blurSelectedTraits
  } else {
    errorLog('TraitUpdate', `currentTraits[${task._id}] is undefined when trying to update marketplace traits`, undefined);
  }

  const oldTraits = Object.fromEntries(
    Object.entries(existingTask?.selectedTraits?.opensea || {}).filter(([_, traits]) => traits.length > 0)
  );

  const selectedTraits = transformNewTask(filteredTasks)
  const existingTaskTraits = transformNewTask(oldTraits)

  const traits = transformOpenseaTraits(selectedTraits)
  const staleTraits = transformOpenseaTraits(existingTaskTraits)
  const currentTask = activeTasks.get(task._id)

  if (!currentTask) return

  const traitBid = task.bidType === "collection" && selectedTraits && Object.keys(selectedTraits).length > 0
  const tokenBid = task.bidType === "token" || task.bidType === "rarity"
  const taskTokenIds = (currentTask?.tokenIds ?? []).map(id => id.toString())

  const autoIds = existingTask.tokenIds
    .filter(id => id.toString().toLowerCase().startsWith('bot'))
    .map(id => {
      const matches = id.toString().match(/\d+/);
      return matches ? parseInt(matches[0]) : null;
    })
    .filter(id => id !== null);

  const amount = autoIds[0]
  let openseaTokenIds: string[] = []
  let magicedenTokenIds: number[] = []

  if (!currentTokenIds[task._id]) {
    currentTokenIds[task._id] = {
      opensea: [],
      magiceden: []
    }
  }

  const previousOpenseaTokens = [...currentTokenIds[task._id]?.opensea, ...existingTask.tokenIds.map(id => id.toString())]
  const previousMagicedenTokens = [...currentTokenIds[task._id]?.magiceden, ...existingTask.tokenIds.map(id => Number(id))]

  const openseaBottonListings = autoIds ? await fetchOpenseaListings(task._id, task.contract.slug, amount) ?? [] : []
  const magicedenBottomListings = amount ? await fetchMagicEdenTokens(task._id, task.contract.contractAddress, amount) ?? [] : []

  if ((task.bidType === "token" || task.bidType === "rarity") && selectedTraits && Object.keys(selectedTraits).length > 0) {
    const attributes = transformSelectedTraitsToAttributes(selectedTraits)
    const expandedAttributes = expandAttributes(attributes)
    // Initialize tokens with bottomListings and taskTokenIds
    let openseaTokens: string[] = [...openseaBottonListings, ...taskTokenIds]
    let magicedenTokens: number[] = [...magicedenBottomListings, ...taskTokenIds.map((token) => +token)]

    if (currentTask.traitsOperator.value === "or") {
      await Promise.allSettled(
        expandedAttributes.map((attribute) => {
          const variable: CollectionItemsQueryVariables = {
            collectionSlug: task.contract.slug,
            filter: {
              attributes: [attribute],
            },
            limit: 100,
            sort: {
              by: 'PRICE',
              direction: 'ASC',
            },
          };

          return fetchOpenSeaCollectionItems(variable).then((items) => {
            openseaTokens.push(...items);
            magicedenTokens.push(...items.map(id => Number(id)));
          });
        })
      )
    } else if (currentTask.traitsOperator.value === "and") {
      const attributes = transformSelectedTraitsToAttributes(selectedTraits)
      const variable: CollectionItemsQueryVariables = {
        collectionSlug: currentTask.contract.slug,
        filter: {
          attributes,
        },
        limit: 100,
        sort: {
          by: 'PRICE',
          direction: 'ASC',
        },
      };
      const items = await fetchOpenSeaCollectionItems(variable);
      openseaTokens.push(...items);
      magicedenTokens.push(...items.map(id => Number(id)))
    }

    openseaTokens = [...openseaBottonListings, ...taskTokenIds, ...openseaTokens];
    openseaTokenIds = [...new Set(openseaTokens)];

    magicedenTokens = [...magicedenBottomListings, ...taskTokenIds.map((token) => +token), ...magicedenTokens]
    magicedenTokenIds = [...new Set(magicedenTokens)]

  } else if (task.bidType === "token" || task.bidType === "rarity") {
    openseaTokenIds = [...new Set([...openseaBottonListings, ...taskTokenIds])];
    magicedenTokenIds = [...new Set([...magicedenBottomListings, ...taskTokenIds.map((token) => +token)])];
  }

  const openseaTokensToRemove = previousOpenseaTokens.filter(token => !openseaTokenIds.includes(token.toString()))
  const magicedenTokensToRemove = previousMagicedenTokens.filter(token => !magicedenTokenIds.includes(+token)).map((token) => token.toString())

  const openseaTokensToAdd = openseaTokenIds.filter(token => !previousOpenseaTokens.includes(token));
  const magicedenTokensToAdd = magicedenTokenIds.filter(token => !previousMagicedenTokens.includes(token));

  removedTokens[task._id] = {
    opensea: openseaTokensToRemove,
    magiceden: magicedenTokensToRemove
  }

  const removedTraits = staleTraits.filter(staleTrait =>
    !traits.some(trait =>
      trait.type === staleTrait.type && trait.value === staleTrait.value
    )
  );

  const addedTraits = traits.filter(staleTrait =>
    !staleTraits.some(trait =>
      trait.type === staleTrait.type && trait.value === staleTrait.value
    )
  );

  const { currencySymbol } = getCurrencyDetailsFromTask(currentTask, "magiceden")

  traitsToRemove[task._id] = removedTraits.map((trait) => `${trait.type}:${trait.value}`)
  await cancelRemovedTraitBids(traitsToRemove[task._id], task._id, task.walletId, currencySymbol)
  await cancelRemovedTokenBids(removedTokens[task._id], task._id, task.walletId, currencySymbol)


  const newTask = { ...task, traits: {} } as ITask
  // Fetch collection details (with automatic caching)
  const collectionDetails = await getCachedCollectionDetails(task.contract.slug, task.contract.contractAddress);

  // Log full collection details for trait bid

  // creator_fees: JSON.stringify(collectionDetails.creator_fees),
  // has_null: collectionDetails.creator_fees.null !== undefined,
  // null_value: collectionDetails.creator_fees.null,
  // keys: Object.keys(collectionDetails.creator_fees)
  // });

  // Convert all creator fees to the IFee format (ensuring values are numbers)
  const creatorFees: IFee = Object.fromEntries(
    Object.entries(collectionDetails.creator_fees).map(([key, value]) => [key, Number(value)])
  );

  // DEBUG: Log fee conversion for scheduled bids

  const bidDuration = currentTask && currentTask.bidDuration ? currentTask.bidDuration : task.bidDuration
  const expiration = getExpiry(bidDuration)

  if (traitBid) {
    const openseaTraitJobs = (currentTask && currentTask.running && currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("opensea") && enabledMarketplaces.map((m) => m.toLowerCase()).includes('opensea')) ? addedTraits.map((trait) => ({
      name: OPENSEA_TRAIT_BID,
      data: {
        _id: task._id,
        address: task.wallet,
        walletId: task.walletId,
        slug: task.contract.slug,
        contractAddress: task.contract.contractAddress,
        creatorFees,
        enforceCreatorFee: collectionDetails.enforceCreatorFee,
        trait: JSON.stringify(trait),
        outbidOptions: task.outbidOptions,
        task: newTask,
      },
      opts: { priority: TRAIT_BID_PRIORITY.OPENSEA, attempts: 1 } as JobsOptions
    })) as Job[] : [] as Job[]

    const magicedenTraits = addedTraits.map((trait) => ({ attributeKey: trait.type, attributeValue: trait.value }))
    const magicedenTraitJobs = (currentTask && currentTask.running && currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("magiceden") && enabledMarketplaces.map((m) => m.toLowerCase())
      .includes('magiceden')) ? magicedenTraits.map((trait) => ({
        name: MAGICEDEN_TRAIT_BID,
        data: {
          _id: task._id,
          address: task.wallet,
          contractAddress: task.contract.contractAddress,
          quantity: 1,
          expiration: expiration,
          walletId: task.walletId,
          slug: task.contract.slug,
          trait,
          outbidOptions: task.outbidOptions,
          task: newTask,
        },
        opts: { priority: TRAIT_BID_PRIORITY.MAGICEDEN, attempts: 1 } as JobsOptions
      })) as Job[] : [] as Job[]


    const blurTraits = addedTraits.map((trait) => ({
      [trait.type]: trait.value
    }));


    const blurTraitJobs = (currentTask && currentTask.running && currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("blur") && enabledMarketplaces.map((m) => m.toLowerCase()).includes('blur')) ? blurTraits.map((trait) => ({
      name: BLUR_TRAIT_BID,
      data: {
        _id: task._id,
        address: task.wallet,
        walletId: task.walletId,
        contractAddress: task.contract.contractAddress,
        slug: task.contract.slug,
        trait: JSON.stringify(trait),
        expiry: expiration,
        outbidOptions: task.outbidOptions,
        task: newTask
      },
      opts: { priority: TRAIT_BID_PRIORITY.BLUR, attempts: 1 } as JobsOptions
    })) as Job[] : [] as Job[]


    // Queue trait bid jobs directly with queue.addBulk
    const queuePromises: Promise<void>[] = [];

    if (openseaTraitJobs.length > 0) {
      queuePromises.push((async () => {
        try {
          const preparedJobs = openseaTraitJobs.map(job => ({
            name: job.name,
            data: job.data,
            opts: {
              jobId: createJobKey(job),
              ...job.opts,
              removeOnComplete: true,
              removeOnFail: true
            }
          }));

          await queue.addBulk(preparedJobs);
          setTaskStatus(task._id, 'QUEUED', 'opensea trait jobs queued', 'opensea');
          await updateTaskRunningPercentage(task._id);
        } catch (error) {
          errorLog('JobQueue', `Failed to queue opensea trait jobs`, error);
          setTaskStatus(task._id, 'PROCESSING', 'job queueing failed - will retry', 'opensea');
          throw error;
        }
      })());
    }

    if (magicedenTraitJobs.length > 0) {
      queuePromises.push((async () => {
        try {
          const preparedJobs = magicedenTraitJobs.map(job => ({
            name: job.name,
            data: job.data,
            opts: {
              jobId: createJobKey(job),
              ...job.opts,
              removeOnComplete: true,
              removeOnFail: true
            }
          }));

          await queue.addBulk(preparedJobs);
          setTaskStatus(task._id, 'QUEUED', 'magiceden trait jobs queued', 'magiceden');
          await updateTaskRunningPercentage(task._id);
        } catch (error) {
          errorLog('JobQueue', `Failed to queue magiceden trait jobs`, error);
          setTaskStatus(task._id, 'PROCESSING', 'job queueing failed - will retry', 'magiceden');
          throw error;
        }
      })());
    }

    if (blurTraitJobs.length > 0) {
      queuePromises.push((async () => {
        try {
          const preparedJobs = blurTraitJobs.map(job => ({
            name: job.name,
            data: job.data,
            opts: {
              jobId: createJobKey(job),
              ...job.opts,
              removeOnComplete: true,
              removeOnFail: true
            }
          }));

          await queue.addBulk(preparedJobs);
          setTaskStatus(task._id, 'QUEUED', 'blur trait jobs queued', 'blur');
          await updateTaskRunningPercentage(task._id);
        } catch (error) {
          errorLog('JobQueue', `Failed to queue blur trait jobs`, error);
          setTaskStatus(task._id, 'PROCESSING', 'job queueing failed - will retry', 'blur');
          throw error;
        }
      })());
    }

    await Promise.all(queuePromises);
  } else if (tokenBid) {
    const openseaTokenJob = (currentTask && currentTask.running && currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("opensea") && enabledMarketplaces.map((m) => m.toLowerCase()).includes('opensea')) ? openseaTokensToAdd
      .filter(token => !isNaN(Number(token)) && String(token).toLowerCase().indexOf('bot') === -1)
      .map((token) => ({
        name: OPENSEA_TOKEN_BID,
        data: {
          _id: task._id,
          address: task.wallet,
          walletId: task.walletId,
          slug: task.contract.slug,
          creatorFees,
          enforceCreatorFee: collectionDetails.enforceCreatorFee,
          asset: { contractAddress: task.contract.contractAddress, tokenId: token },
          outbidOptions: task.outbidOptions,
          task: newTask,
        },
        opts: { priority: TOKEN_BID_PRIORITY.OPENSEA, attempts: 1 } as JobsOptions,
      })) as Job[] : [] as Job[]


    const magicedenTokenJob = (currentTask && currentTask.running && currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("magiceden") && enabledMarketplaces
      .map((m) => m.toLowerCase())
      .includes('magiceden')) ? magicedenTokensToAdd
        .filter(token => !isNaN(Number(token)))
        .map((token) => ({
          name: MAGICEDEN_TOKEN_BID,
          data: {
            _id: task._id,
            address: task.wallet,
            contractAddress: task.contract.contractAddress,
            quantity: 1,
            expiration: expiration,
            walletId: task.walletId,
            slug: task.contract.slug,
            tokenId: token,
            outbidOptions: task.outbidOptions,
            task: newTask,
          },
          opts: { priority: TOKEN_BID_PRIORITY.MAGICEDEN, attempts: 1 } as JobsOptions
        })) as Job[] : [] as Job[]


    if (!currentTokenIds[task._id]) {
      currentTokenIds[task._id] = {
        opensea: [],
        magiceden: []
      }
    }

    // Add defensive check before property access
    if (currentTokenIds[task._id]) {
      currentTokenIds[task._id].opensea = openseaTokenIds
      currentTokenIds[task._id].magiceden = magicedenTokenIds
    } else {
      errorLog('TokenIdUpdate', `currentTokenIds[${task._id}] is undefined when trying to update marketplace token IDs`, undefined);
    }

    // Queue token bid jobs directly with queue.addBulk
    const tokenQueuePromises: Promise<void>[] = [];

    if (magicedenTokenJob.length > 0) {
      tokenQueuePromises.push((async () => {
        try {
          const preparedJobs = magicedenTokenJob.map(job => ({
            name: job.name,
            data: job.data,
            opts: {
              jobId: createJobKey(job),
              ...job.opts,
              removeOnComplete: true,
              removeOnFail: true
            }
          }));

          await queue.addBulk(preparedJobs);
          setTaskStatus(task._id, 'QUEUED', 'magiceden token jobs queued', 'magiceden');
          await updateTaskRunningPercentage(task._id);
        } catch (error) {
          errorLog('JobQueue', `Failed to queue magiceden token jobs`, error);
          setTaskStatus(task._id, 'PROCESSING', 'job queueing failed - will retry', 'magiceden');
          throw error;
        }
      })());
    }

    if (openseaTokenJob.length > 0) {
      tokenQueuePromises.push((async () => {
        try {
          const preparedJobs = openseaTokenJob.map(job => ({
            name: job.name,
            data: job.data,
            opts: {
              jobId: createJobKey(job),
              ...job.opts,
              removeOnComplete: true,
              removeOnFail: true
            }
          }));

          await queue.addBulk(preparedJobs);
          setTaskStatus(task._id, 'QUEUED', 'opensea token jobs queued', 'opensea');
          await updateTaskRunningPercentage(task._id);
        } catch (error) {
          errorLog('JobQueue', `Failed to queue opensea token jobs`, error);
          setTaskStatus(task._id, 'PROCESSING', 'job queueing failed - will retry', 'opensea');
          throw error;
        }
      })());
    }

    await Promise.all(tokenQueuePromises);
  }

  // existingTask

  // Handle loopInterval changes for running tasks
  if (loopIntervalChanged && task.running && taskStatus[task._id]) {

    // Check each marketplace's current status
    const scheduledMarketplaces: string[] = [];
    const selectedMarketplacesLower = task.selectedMarketplaces.map(m => m.toLowerCase());

    if (taskStatus[task._id].marketplaces) {
      for (const [marketplace, status] of Object.entries(taskStatus[task._id].marketplaces)) {
        if (status?.status === 'SCHEDULED' && selectedMarketplacesLower.includes(marketplace)) {
          scheduledMarketplaces.push(marketplace);
        }
      }
    }


    // Cancel existing timers and reschedule for each scheduled marketplace
    for (const marketplace of scheduledMarketplaces) {
      const timerKey = `${task._id}-${marketplace}`;

      // Clear the countdown timer
      const countdownTimer = taskCountdownTimers.get(timerKey);
      if (countdownTimer) {
        clearInterval(countdownTimer);
        taskCountdownTimers.delete(timerKey);
      } else {
      }

      // Clear the scheduled timeout
      const timeoutId = taskTimeouts.get(timerKey);
      if (timeoutId) {
        clearTimeout(timeoutId);
        taskTimeouts.delete(timerKey);
      } else {
      }

      // Reschedule with new interval
      await scheduleNextLoop(task._id, marketplace);
    }
  }

  const start = existingTask.running === false && task.running === true
  const stop = existingTask.running === true && task.running === false

  if (start) {
    await startTask(task, true)
  }

  if (stop) {
    await stopTask(task, false)
  }

  if (process.env.NODE_ENV === 'development') {
    console.log(`[TaskUpdate][${task._id}] SUCCESS: Task update completed successfully`);
  }
  return { success: true, data: dbTask.toObject() };
}

const DRAIN_THRESHOLD = 900000; // 90% of MAX_QUEUE_JOBS - triggers backpressure early

// Cached queue depth for dynamic throttling (avoid expensive getJobCounts calls)
let cachedQueueDepth = 0;
let queueDepthCacheTimestamp = 0;
const QUEUE_DEPTH_CACHE_TTL = 500; // 0.5 seconds cache (fresher throttle decisions, reduces worker starvation from 2s → 500ms max)

async function getCachedQueueDepth(): Promise<number> {
  const now = Date.now();

  // Return cached value if still valid (within TTL)
  if (now - queueDepthCacheTimestamp < QUEUE_DEPTH_CACHE_TTL) {
    return cachedQueueDepth;
  }

  // Cache expired, fetch fresh counts
  const mainCounts = await queue.getJobCounts();

  cachedQueueDepth = mainCounts.waiting || 0;
  queueDepthCacheTimestamp = now;

  return cachedQueueDepth;
}

const BLUR_JOBS = [
  'BLUR_COLLECTION_BID',
  'BLUR_COLLECTION_BID_COUNTERBID',
  'BLUR_TRAIT_BID_COUNTERBID',
  'BLUR_TRAIT_BID'
];

// Counterbid job types for rate limiting
const MAIN_QUEUE_COUNTERBID_TYPES = [
  OPENSEA_TOKEN_BID_COUNTERBID,
  OPENSEA_TRAIT_BID_COUNTERBID,
  OPENSEA_COLLECTION_BID_COUNTERBID,
  MAGICEDEN_COLLECTION_BID_COUNTERBID,
  MAGICEDEN_TOKEN_BID_COUNTERBID,
  MAGICEDEN_TRAIT_BID_COUNTERBID,
];

const BLUR_QUEUE_COUNTERBID_TYPES = [
  BLUR_COLLECTION_BID_COUNTERBID,
  BLUR_TRAIT_BID_COUNTERBID,
];

// Helper function to extract marketplace from job name
function getMarketplaceFromJobName(jobName: string): string | null {
  const name = jobName.toUpperCase();
  if (name.includes('OPENSEA')) return 'opensea';
  if (name.includes('MAGICEDEN')) return 'magiceden';
  if (name.includes('BLUR')) return 'blur';
  return null;
}

const MAX_QUEUE_JOBS = 1000000; // Hard limit for queue capacity
// Helper function to get total jobs in main queue (Blur queue consolidated)
async function getTotalJobsInQueues(): Promise<number> {
  const counts = await queue.getJobCounts();
  return (counts.waiting || 0) + (counts.active || 0) + (counts.prioritized || 0);
}

// In-memory tracking for counterbid jobs (fast O(1) lookups instead of expensive Redis queries)
const counterbidJobTracker = {
  mainCount: 0,
  blurCount: 0,
  mainJobIds: new Set<string>(),
  blurJobIds: new Set<string>(),
};

function incrementCounterbidCount(isBlurQueue: boolean, jobId: string): void {
  if (isBlurQueue) {
    counterbidJobTracker.blurCount++;
    counterbidJobTracker.blurJobIds.add(jobId);
  } else {
    counterbidJobTracker.mainCount++;
    counterbidJobTracker.mainJobIds.add(jobId);
  }
}

function decrementCounterbidCount(jobId: string): void {
  if (counterbidJobTracker.mainJobIds.has(jobId)) {
    counterbidJobTracker.mainCount = Math.max(0, counterbidJobTracker.mainCount - 1);
    counterbidJobTracker.mainJobIds.delete(jobId);
  } else if (counterbidJobTracker.blurJobIds.has(jobId)) {
    counterbidJobTracker.blurCount = Math.max(0, counterbidJobTracker.blurCount - 1);
    counterbidJobTracker.blurJobIds.delete(jobId);
  }
}

function getCounterbidCount(isBlurQueue: boolean): number {
  return isBlurQueue ? counterbidJobTracker.blurCount : counterbidJobTracker.mainCount;
}

// processBulkJobs function removed - replaced with direct queue.addBulk() calls at all 11 call sites
// See: Lines 7595-7680 (trait/token bids), 12371, 12405 (cancellation), 17558, 17617, 18546, 21325, 21422 (scheduled bids)

// Helper functions below are still used by direct queue.addBulk implementations






// Helper function to check for duplicate jobs in queue
async function checkForDuplicateJobs(queue: Queue, jobsToAdd: Array<{ name: string, data: any, opts: any }>) {
  try {
    // Get all job IDs we're trying to add
    const jobIdsToAdd = new Set(jobsToAdd.map(job => job.opts?.jobId).filter(Boolean));

    if (jobIdsToAdd.size === 0) {
      // No jobIds to check, return all jobs
      return jobsToAdd;
    }

    // Check if these jobs already exist in the queue
    const existingJobs = await Promise.all(
      Array.from(jobIdsToAdd).map(jobId => queue.getJob(jobId as string))
    );

    // Create a set of existing job IDs
    const existingJobIds = new Set(
      existingJobs
        .filter(job => job !== null && job !== undefined)
        .map(job => job!.id)
        .filter(id => id !== null && id !== undefined)
    );

    // Filter out jobs that already exist
    const nonDuplicateJobs = jobsToAdd.filter(job => {
      const jobId = job.opts?.jobId;
      if (!jobId) return true; // Jobs without IDs can't be checked for duplicates

      const exists = existingJobIds.has(jobId);
      return !exists;
    });

    const duplicatesFound = jobsToAdd.length - nonDuplicateJobs.length;
    if (duplicatesFound > 0 || DEBUG) {
    }

    return nonDuplicateJobs;
  } catch (error) {
    // If checking fails, log the error but don't block job addition
    errorLog('DuplicateJobCheck', 'Error checking for duplicates', error);
    return jobsToAdd;
  }
}

// Helper function to chunk array
/**
 * Splits an array into smaller arrays (chunks) of a specified size.
 * 
 * @param array - The array to be split into chunks.
 * @param size - The maximum size of each chunk.
 * @returns An array of arrays (chunks), where each chunk is of the specified size.
 */
function chunk<T>(array: T[], size: number): T[][] {
  return Array.from({ length: Math.ceil(array.length / size) }, (_, i) => {
    const start = i * size; // Calculate the starting index of the current chunk
    const end = Math.min(start + size, array.length); // Calculate the ending index of the current chunk
    return array.slice(start, end); // Extract the chunk from the array
  });
}

function createJobKey(job: Job) {
  let uniqueKey;
  let type, value, trait;

  // Extract marketplace from job name for collection jobs
  const jobNameParts = job.name.split('_');
  const marketplace = jobNameParts[0]?.toLowerCase() || 'unknown';

  // Log key creation start for debugging
  if (DEBUG) {
  }

  switch (job.name) {
    case 'OPENSEA_TRAIT_BID':
      trait = JSON.parse(job.data.trait)
      type = removeChar(trait.type, '-')
      value = removeChar(trait.value, '-')
      uniqueKey = `opensea-${type}_${value}`
      break;
    case 'BLUR_TRAIT_BID':
      const traitObj = JSON.parse(job.data.trait);
      const traitKey = Object.keys(traitObj)[0];
      const traitValue = traitObj[traitKey];
      // Fix: Add separator between key and value
      uniqueKey = `blur-${removeChar(traitKey, '-')}_${removeChar(String(traitValue), '-')}`;
      break;
    case 'MAGICEDEN_TRAIT_BID':
      type = removeChar(job.data.trait.attributeKey, '-')
      value = removeChar(job.data.trait.attributeValue, '-')
      uniqueKey = `magiceden-${type}_${value}`
      break;
    case 'OPENSEA_TOKEN_BID':
      uniqueKey = `opensea-token_${job.data.asset.tokenId}`
      break;
    case 'MAGICEDEN_TOKEN_BID':
      uniqueKey = `magiceden-token_${job.data.tokenId}`;
      break;
    case 'CANCEL_OPENSEA_BID':
    case 'CANCEL_OPENSEA_COUNTERBID':
    case 'CANCEL_OPENSEA_OUTBID':
    case 'CANCEL_MAGICEDEN_BID':
    case 'CANCEL_BLUR_BID':
      // Cancellation jobs should not go through this function
      // but handle them just in case
      const cancelMarketplace = job.name.includes('OPENSEA') ? 'opensea' :
        job.name.includes('MAGICEDEN') ? 'magiceden' : 'blur';
      uniqueKey = `${cancelMarketplace}-cancel_${job.data.orderId || job.data.orderIds?.[0] || 'unknown'}`;
      break;
    default:
      // Fix: Include marketplace in collection jobs
      uniqueKey = `${marketplace}-collection`;
      if (DEBUG) {
      }
  }
  const baseKey = `${job?.data?._id}`

  const rawSlug = job?.data?.contract?.slug || job?.data?.slug || ''
  const slug = removeChar(rawSlug, '-')
  const walletAddress = job?.data?.address || ''
  const key = `bid-${baseKey}-${uniqueKey}-${slug}-${walletAddress}`

  // Enhanced debug logging for key generation
  if (DEBUG) {
  }

  return key
}

function generateJobKey(job: any): string {
  const taskId = job?.data?._id || '';
  const contractSlug = job?.data?.contract?.slug || job?.data?.slug || '';
  const name = job?.name || '';

  const key = `${taskId}-${contractSlug}-${name}`;

  // Log generateJobKey usage
  if (DEBUG) {
  }

  return key;
}

function removeChar(str: string, char: string): string {
  return str.split(char).join('');
}

// Function to trigger task restart when stuck at 0% due to queue capacity issues
async function triggerTaskRestart(taskId: string) {
  // TaskFlow logging: Entry

  const task = activeTasks.get(taskId);
  if (!task) {
    warnLog('TaskRecovery',
      `RESTART_NO_TASK | taskId=${taskId} | reason=task_not_in_activeTasks`,
      { taskId, reason: 'task_not_active' }
    );
    return;
  }

  // Check restart limit to prevent infinite loops
  const now = Date.now();
  const restartInfo = taskRestartCounts.get(taskId);
  let restartCount = 1;

  if (restartInfo) {
    // Reset counter if it's been more than an hour since last restart
    if (now - restartInfo.lastRestart > 3600000) { // 1 hour
      taskRestartCounts.set(taskId, { count: 1, lastRestart: now });
    } else if (restartInfo.count >= MAX_RESTARTS_PER_HOUR) {
      errorLog('TaskRecovery',
        `RESTART_LIMIT_REACHED | taskId=${taskId} | count=${restartInfo.count}/${MAX_RESTARTS_PER_HOUR} | TASK_ABANDONED`,
        undefined,
        { taskId, restartCount: restartInfo.count, maxRestarts: MAX_RESTARTS_PER_HOUR, abandoned: true }
      );

      return; // Don't restart
    } else {
      // Increment counter
      restartCount = restartInfo.count + 1;
      taskRestartCounts.set(taskId, { count: restartCount, lastRestart: now });
    }
  } else {
    // First restart
    taskRestartCounts.set(taskId, { count: 1, lastRestart: now });
  }


  // Clear any stuck parent jobs for all marketplaces
  const marketplaces = ['opensea', 'magiceden', 'blur'];
  let clearedParentJobs = 0;
  for (const marketplace of marketplaces) {
    const parentJobId = getParentJobId(taskId, marketplace);
    if (parentJobs.has(parentJobId)) {
      parentJobs.delete(parentJobId);
      clearedParentJobs++;
    }
  }

  // Clear processed jobs tracking
  marketplaces.forEach(marketplace => {
    const parentJobId = getParentJobId(taskId, marketplace);
    processedJobsPerParent.delete(parentJobId);
  });


  // Reset task status to SCHEDULED for all marketplaces to allow restart
  for (const mp of task.selectedMarketplaces) {
    setTaskStatus(taskId, 'SCHEDULED', 'restarting stuck task', mp.toLowerCase());
  }

  // Clear any preprocessing data
  taskPreprocessing.delete(taskId);


  try {
    // Restart the task with isRestart=true to prevent loop counter increment
    await startTask(task, true, undefined, true);
  } catch (error) {
    errorLog('TaskRecovery',
      `RESTART_FAILED | taskId=${taskId} | restartCount=${restartCount}`,
      error,
      { taskId, restartCount }
    );
  }
}

/**
 * Initialize completion tracking for a task
 */

// Wrapper function with retry logic for Redis timeouts
async function startTask(task: ITask, start: boolean, marketplace?: string, isRestart?: boolean) {
  const taskId = task._id.toString();
  const mpKeyForLog = marketplace?.toLowerCase() as 'opensea' | 'magiceden' | 'blur' | undefined;
  const MAX_RETRIES = 3;
  const RETRY_DELAYS = [2000, 4000, 8000]; // 2s, 4s, 8s
  const startTaskTime = Date.now();

  // Performance profiling entry

  // TaskFlow logging: Entry to startTask

  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    try {
      const result = await startTaskInternal(task, start, marketplace, isRestart);
      return result; // Pass through the result
    } catch (error) {
      // Check if it's a timeout error and we have retries left
      if (error instanceof Error &&
        error.message?.includes('Command timed out') &&
        attempt < MAX_RETRIES - 1) {
        warnLog('TaskFlow',
          `START_TASK_RETRY | taskId=${taskId} | attempt=${attempt + 1}/${MAX_RETRIES} | mp=${marketplace || 'all'}`,
          { taskId, marketplace: mpKeyForLog, attempt: attempt + 1, maxRetries: MAX_RETRIES }
        );
        await new Promise(resolve => setTimeout(resolve, RETRY_DELAYS[attempt]));
        continue; // Retry
      }

      // If not a timeout or no retries left, log detailed error and re-throw
      const errorMessage = error instanceof Error ? error.message : String(error);
      const errorStack = error instanceof Error ? error.stack : undefined;
      const errorName = error instanceof Error ? error.name : 'Unknown';

      errorLog('TaskFlow',
        `START_TASK_ERROR | taskId=${taskId} | mp=${marketplace || 'all'} | errorType=${errorName} | error=${errorMessage}`,
        error,
        {
          taskId,
          marketplace: mpKeyForLog,
          attempt: attempt + 1,
          errorType: errorName,
          errorMessage,
          errorStack,
          taskRunning: task.running,
          taskWalletId: task.walletId ? '***' : 'missing',
          taskSlug: task.contract?.slug
        }
      );
      throw error;
    }
  }

  // If all retries failed with timeout
  warnLog('TaskFlow',
    `START_TASK_TIMEOUT_EXHAUSTED | taskId=${taskId} | mp=${marketplace || 'all'} | retries=${MAX_RETRIES}`,
    { taskId, marketplace: mpKeyForLog, retries: MAX_RETRIES }
  );
  return { success: false, reason: 'timeout_exhausted' };
}

// Original startTask function implementation
async function startTaskInternal(task: ITask, start: boolean, marketplace?: string, isRestart?: boolean) {
  const taskId = task._id.toString();
  const mpKeyForLog = marketplace?.toLowerCase() as 'opensea' | 'magiceden' | 'blur' | undefined;
  const internalStartTime = Date.now();

  // Performance profiling entry

  // TaskFlow logging: Entry to startTaskInternal

  // Check start parameter immediately to avoid any unnecessary processing
  if (!start) {
    return { success: false, reason: 'start_false' };
  }

  // Check if password is available before starting task
  const password = getPassword();
  if (!password) {
    errorLog('TaskFlow', `START_INTERNAL_EXIT | taskId=${taskId} | reason=no_password`, undefined, { taskId, marketplace: mpKeyForLog, reason: 'no_password' });
    return { success: false, reason: 'no_password' };
  }

  // Validate wallet exists before starting task (Fix #3: prevents wallet-not-found cascade errors)
  const walletLookupStart = Date.now();
  const taskWallet = await walletCache.getWalletById(task.walletId);
  if (!taskWallet) {
    const safeWalletId = maskSensitiveIdentifier(task.walletId);
    errorLog('TaskFlow', `START_INTERNAL_EXIT | taskId=${taskId} | reason=wallet_not_found | walletId=${safeWalletId}`, undefined, { taskId, marketplace: mpKeyForLog, reason: 'wallet_not_found' });
    return { success: false, reason: 'wallet_not_found' };
  }

  // Load state from Redis on first start (not restart)
  if (!isRestart && !activeTasks.has(taskId)) {
  }

  // Clear marketplace availability cache to allow fresh checks on task start
  const slug = task.contract.slug;
  const contractAddress = task.contract.contractAddress;
  const cacheClsStart = Date.now();
  clearUnavailabilityCache('blur', slug);
  clearUnavailabilityCache('magiceden', contractAddress);
  clearUnavailabilityCache('opensea', slug);

  if (!taskWallet.encryptedPrivateKey) {
    const safeWalletId = maskSensitiveIdentifier(task.walletId);
    errorLog('TaskFlow', `START_INTERNAL_EXIT | taskId=${taskId} | reason=wallet_no_private_key | walletId=${safeWalletId}`, undefined, { taskId, marketplace: mpKeyForLog, reason: 'wallet_no_private_key' });
    return { success: false, reason: 'wallet_no_private_key' };
  }

  const startTime = Date.now();



  // If task is in CANCELLING state, we'll clean it up and allow the start
  // Check if any marketplace is in CANCELLING state
  const hasCancellingMarketplace = taskStatus[taskId]?.marketplaces &&
    Object.values(taskStatus[taskId].marketplaces).some(mpStatus => mpStatus?.status === 'CANCELLING');

  if (hasCancellingMarketplace) {

    // Remove task from blacklist to allow new jobs
    const wasRemoved = removeTaskFromBlacklist(taskId);

    // Reset cancellation percentage
    if (taskStatus[taskId]) {
      // Global cancellation properties removed - using marketplace-specific tracking
    }

    // Note: We intentionally DO NOT clear cancellation tracking here.
    // Clearing while cancellation jobs are still running causes RECORD_NO_TRACKING race conditions.
    // When the next stopTask is called, initializeTracking() will overwrite old tracking anyway.
    // This fix eliminates 8,926 RECORD_NO_TRACKING warnings from late job completions.
  }

  clearTaskTimers(taskId, marketplace);

  // Initialize BOTH bidStats and ownBidCounts atomically from single cache snapshot
  // This prevents race conditions where bids placed during startup cause immediate divergence
  const cacheInitStart = Date.now();
  try {
    const existingOrders = await orderCache.getActiveByTask(task._id);

    const counts: { [marketplace: string]: number } = {
      opensea: 0, magiceden: 0, blur: 0
    };

    for (const order of existingOrders) {
      const mp = order.marketplace.toLowerCase() as 'opensea' | 'magiceden' | 'blur';
      counts[mp]++;

      // Pre-populate dedup cache WITHOUT incrementing counter
      // Only WebSocket confirmations should increment ownBidCounts
      if (order.orderHash) {
        const normalizedHash = order.orderHash.startsWith('0x') ? order.orderHash : `0x${order.orderHash}`;
        processedOwnBids.set(normalizedHash, true);
      }
    }

    // Set bidStats for this task
    bidStats[task._id] = {
      opensea: counts['opensea'],
      magiceden: counts['magiceden'],
      blur: counts['blur']
    };

    const duration = Date.now() - cacheInitStart;
  } catch (err) {
    warnLog('TaskInit',
      `ORDER_CACHE_INIT | taskId=${taskId} | status=failed | ` +
      `error=${err instanceof Error ? err.message : String(err)}`, { taskId });

    // CRITICAL FIX: Initialize bidStats even if cache fails
    // Without this, publishTaskMetrics() won't include this task (line 6818)
    // Task will be orphaned: taskStatus exists but no metrics tracking
    bidStats[task._id] = {
      opensea: 0,
      magiceden: 0,
      blur: 0
    };
  }

  // Pre-fetch floor prices for selected marketplaces to warm the cache
  // This ensures counterbid handlers get cache hits instead of blocking API calls
  const marketplacesToPrefetch = marketplace
    ? [marketplace.toLowerCase()]
    : task.selectedMarketplaces.map(m => m.toLowerCase());


  // Fire-and-forget: don't block task start on floor price fetches
  Promise.all(marketplacesToPrefetch.map(async (mp) => {
    try {
      if (mp === 'opensea') {
        const price = await getCachedFloorPrice(task.contract.slug, 'opensea');
        if (price) {
        }
      } else if (mp === 'magiceden') {
        const price = await getCachedFloorPrice(task.contract.contractAddress, 'magiceden');
        if (price) {
        }
      } else if (mp === 'blur') {
        const price = await getCachedFloorPrice(task.contract.slug, 'blur');
        if (price) {
        }
      }
    } catch (error) {
      // Non-blocking - log and continue
    }
  })).catch(() => {
    // Silently ignore aggregate errors - individual errors already logged
  });

  // Track task start for collection warming (reference counting)
  // Collections already warmed at startup - this just increments reference count
  if (task.outbidOptions?.counterbid && marketplacesToPrefetch.includes('opensea')) {
    collectionDataWarmer.startWarming(task.contract.slug, taskId);
  }

  if (!marketplace) {
    skipStats[task._id] = {
      opensea: 0,
      magiceden: 0,
      blur: 0
    };
    // Reset error stats when starting task without marketplace parameter
    errorStats[taskId] = { opensea: 0, magiceden: 0, blur: 0 };

    // Starting task with marketplace-specific status tracking
    const timestamp = new Date().toISOString();

    const existingStatus = !!taskStatus[taskId];

    // Initialize task status
    if (!taskStatus[taskId]) {
      taskStatus[taskId] = {
        selectedMarketplaces: task.selectedMarketplaces.map(m => m.toLowerCase()),
        lastUpdate: Date.now(),
        marketplaces: {}
      };
    } else {
    }

    // Initialize marketplace status for each selected marketplace
    if (marketplace) {
      // Starting a specific marketplace
      const marketplaceName = marketplace.toLowerCase() as 'opensea' | 'magiceden' | 'blur';
      setTaskStatus(taskId, 'STARTED', `startTask called: isRestart=${isRestart}`, marketplace);
    } else {
      // Starting all marketplaces - but only enabled ones

      for (const mp of task.selectedMarketplaces) {
        const mpLower = mp.toLowerCase();
        const isEnabled = enabledMarketplaces.map(em => em.toLowerCase()).includes(mpLower);

        if (isEnabled) {
          setTaskStatus(taskId, 'STARTED', `startTask called: isRestart=${isRestart}, marketplace=ALL`, mp);
        } else {
          // Don't set STARTED status for disabled marketplaces
        }
      }
    }


    // Set up status progression monitoring for each marketplace
    if (marketplace) {
      const isEnabled = enabledMarketplaces.map(em => em.toLowerCase()).includes(marketplace.toLowerCase());
      if (isEnabled) {
        statusProgressionManager.ensureStatusProgression(taskId, marketplace, 'STARTED', 'RUNNING', 5000); // 5 seconds timeout
      }
    } else {
      // Set up monitoring only for enabled marketplaces
      const monitoredMarketplaces: string[] = [];
      for (const mp of task.selectedMarketplaces) {
        const mpLower = mp.toLowerCase();
        const isEnabled = enabledMarketplaces.map(em => em.toLowerCase()).includes(mpLower);
        if (isEnabled) {
          statusProgressionManager.ensureStatusProgression(taskId, mpLower, 'STARTED', 'RUNNING', 5000); // 5 seconds timeout
          monitoredMarketplaces.push(mpLower);
        }
      }
    }

    // Per-marketplace loop counters (nested structure)
    if (!loopCounterStats[taskId]) {
      loopCounterStats[taskId] = {};
    }

    if (marketplace) {
      // Single marketplace start - initialize to 1 if not exists
      // Counter is incremented in timer callback when scheduleNextLoop fires, not here
      if (!loopCounterStats[taskId][marketplace]) {
        loopCounterStats[taskId][marketplace] = 1;
      }
      // For restarts, keep existing value (already set)
      const currentLoopCount = loopCounterStats[taskId][marketplace];
      cleanupOldParentJobs(taskId, currentLoopCount);
    } else {
      // Starting all marketplaces - initialize each to 1 if not exists
      // Counter is incremented in timer callback when scheduleNextLoop fires, not here
      const loopCounterState: Record<string, number> = {};
      for (const mp of task.selectedMarketplaces) {
        const mpLower = mp.toLowerCase();
        if (!loopCounterStats[taskId][mpLower]) {
          loopCounterStats[taskId][mpLower] = 1;
        }
        loopCounterState[mpLower] = loopCounterStats[taskId][mpLower];
      }

      // Clean up old parent jobs
      const maxLoopCount = Math.max(...task.selectedMarketplaces.map(mp =>
        loopCounterStats[taskId][mp.toLowerCase()] || 1
      ));
      cleanupOldParentJobs(taskId, maxLoopCount);

      // Log complete loop counter state for all marketplaces
    }

    // Clear all processed jobs for this task to ensure fresh tracking
    let clearedProcessedJobs = 0;
    for (const [parentJobId, processedJobs] of processedJobsPerParent.entries()) {
      if (parentJobId.includes(taskId)) {
        processedJobsPerParent.delete(parentJobId);
        clearedProcessedJobs++;
      }
    }
    if (clearedProcessedJobs > 0) {
    }

    // FIX: REMOVED aggressive parent job cleanup that caused PARENT_NOT_FOUND warnings
    // Workers from previous loops are still processing jobs that reference old parent jobs
    // Let cleanupOldParentJobs() handle proper retention (keeps parent jobs for 5 loops)
    // See: cleanupOldParentJobs() at line 5187, called at lines 9589 and 9616

    // Clear pending job creation promises for this task
    let clearedPromises = 0;
    for (const [promiseKey] of pendingJobCreation.entries()) {
      if (promiseKey.includes(taskId)) {
        pendingJobCreation.delete(promiseKey);
        clearedPromises++;
      }
    }
    if (clearedPromises > 0) {
    }

    // Clear any task-specific state in warningBids and warningMessage
    if (warningBids[taskId]) {
      delete warningBids[taskId];
    }
    if (warningMessage[taskId]) {
      delete warningMessage[taskId];
    }
  } else {
    // Marketplace-specific start - reset only that marketplace's stats
    if (!errorStats[taskId]) {
      errorStats[taskId] = { opensea: 0, magiceden: 0, blur: 0 };
    }
    errorStats[taskId][marketplace.toLowerCase() as keyof typeof errorStats[typeof taskId]] = 0;

    // Clear processed jobs for this specific marketplace
    const parentJobId = getParentJobId(taskId, marketplace.toLowerCase());
    if (processedJobsPerParent.has(parentJobId)) {
      const processedJobsCount = processedJobsPerParent.get(parentJobId)?.size || 0;
      processedJobsPerParent.delete(parentJobId);
    }
  }

  await removeTaskFromBlacklist(taskId);

  if (!API_KEY) {
    warnLog('TaskInit', `ABORT | taskId=${taskId} | reason=no_api_key`, { taskId });
    return;
  }

  // Async bid log deletion - fire and forget
  // Convert bidDuration to hours for cache TTL
  const bidDurationHours = convertBidDurationToHours(task.bidDuration);
  logsCache.setTaskTTL(taskId, bidDurationHours);
  logsCache.deleteMany(taskId).catch((err: any) => {
    warnLog('Task', `Failed to clear old logs for ${taskId}: ${err.message}`);
  });

  // Ensure wallet approvals for all selected marketplaces
  try {
    const wallet = await walletCache.getWalletById(task.walletId);
    if (!wallet?.encryptedPrivateKey) {
      warnLog('TaskInit', `APPROVAL_ABORT | taskId=${taskId} | reason=wallet_missing_key`, { taskId });
      return;
    }

    const privateKey = decryptPrivateKey(wallet.encryptedPrivateKey);
    const validMarketplaces = ["opensea", "magiceden", "blur"];
    const selectedMarketplaces = marketplace ? [marketplace.toLowerCase()] : task.selectedMarketplaces.map(m => m.toLowerCase());
    const marketplacesToApprove = selectedMarketplaces.filter(m => validMarketplaces.includes(m));
    const approvalPhaseStart = Date.now();

    // Parallel approval checks with caching
    const approvalPromises = marketplacesToApprove.map(async (mp) => {
      let marketplaceAddress: string;

      switch (mp) {
        case "opensea":
          marketplaceAddress = SEAPORT;
          break;
        case "magiceden":
          marketplaceAddress = MAGICEDEN_MARKETPLACE;
          break;
        case "blur":
          // Blur doesn't require WETH approval - skip
          return { marketplace: mp, approved: true, cached: false };
        default:
          return { marketplace: mp, approved: false, cached: false };
      }

      // Check cache first
      const cached = await approvalCache.isApproved(wallet.address, marketplaceAddress, WETH);
      if (cached) {
        return { marketplace: mp, approved: true, cached: true };
      }

      const approved = await approveMarketplace(marketplaceAddress, privateKey, WETH);

      if (approved) {
        // Update cache with successful approval
        await approvalCache.setApproval(wallet.address, marketplaceAddress, WETH, {
          approved: true,
          timestamp: Date.now(),
          allowance: MaxUint256.toString()
        });
      } else {
        warnLog('TaskInit', `APPROVAL_FAILED | taskId=${taskId} | marketplace=${mp}`, { taskId });
      }

      return { marketplace: mp, approved, cached: false };
    });

    // Wait for all approvals to complete
    const approvalResults = await Promise.all(approvalPromises);
    const approvalPhaseDuration = Date.now() - approvalPhaseStart;
    const failedApprovals = approvalResults.filter(r => !r.approved);
    const cachedCount = approvalResults.filter(r => r.cached).length;


    if (failedApprovals.length > 0) {
      warnLog('TaskInit', `APPROVAL_FAILURES | taskId=${taskId} | failedMarketplaces=[${failedApprovals.map(f => f.marketplace).join(',')}]`, { taskId });
    }
  } catch (error) {
    errorLog('MarketplaceApproval', 'Error during marketplace approval check', error);
    // Continue with task execution even if approval check fails
  }

  const RATE_LIMIT = process.env.RATE_LIMIT;
  if (!RATE_LIMIT) {
    return;
  }

  // taskId already declared at function start

  try {
    await removeTaskFromAborted(taskId);

    // Only update task running status if not already running
    if (!task.running) {
      const updatedTask = { ...task, running: true };
      activeTasks.set(taskId, updatedTask);
      const activeTasksCount = activeTasks.size;
      const taskIndex = currentTasks.findIndex(t => t._id === task._id);
      if (taskIndex !== -1) {
        currentTasks[taskIndex].running = true;
      }

      // Add task to indexes for fast lookup
      addTaskToIndexes(updatedTask);

      // Auto-subscribe all connected clients to newly started task
      const subscribedCount = broadcastManager.subscribeAllClients(taskId);

      // Subscribe to WebSocket collection with retry logic
      const subscribeWithRetry = async (retryCount = 0, maxRetries = 3): Promise<void> => {
        try {
          if (wsManager?.isConnected()) {
            const newTask = { ...task, running: true };
            const wsSubStart = Date.now();
            await subscribeToCollections([newTask]);
          } else {
            warnLog('TaskInit', `WS_SUBSCRIBE_SKIP | taskId=${taskId} | slug=${task.contract.slug} | wsConnected=false | willRetry=${retryCount < maxRetries}`, { taskId });

            // Retry after delay if WebSocket not connected yet
            if (retryCount < maxRetries) {
              const retryDelayMs = Math.pow(2, retryCount) * 1000; // Exponential backoff: 1s, 2s, 4s
              setTimeout(async () => {
                await subscribeWithRetry(retryCount + 1, maxRetries);
              }, retryDelayMs);
            }
          }
        } catch (error) {
          errorLog('TaskInit', `WS_SUBSCRIBE_ERROR | taskId=${taskId} | slug=${task.contract.slug} | retryCount=${retryCount} | error=${error instanceof Error ? error.message : String(error)}`, error, { taskId });

          // Retry on error with exponential backoff
          if (retryCount < maxRetries) {
            const retryDelayMs = Math.pow(2, retryCount) * 1000; // Exponential backoff: 1s, 2s, 4s
            setTimeout(async () => {
              await subscribeWithRetry(retryCount + 1, maxRetries);
            }, retryDelayMs);
          } else {
            errorLog('TaskInit', `WS_SUBSCRIBE_FAILED_ALL_RETRIES | taskId=${taskId} | slug=${task.contract.slug} | maxRetries=${maxRetries}`, error, { taskId });
          }
        }
      };

      await subscribeWithRetry();
    } else {
    }

    const validMarketplaces = ["opensea", "magiceden", "blur"];
    const selectedMarketplaces = marketplace ? [marketplace.toLowerCase()] : task.selectedMarketplaces.map(m => m.toLowerCase());

    // Pre-filter marketplaces based on circuit breaker state
    const suspendedMarketplaces: string[] = [];
    const disabledMarketplaces: string[] = [];
    const availableMarketplaces = selectedMarketplaces
      .filter((m) => {
        const isEnabled = enabledMarketplaces.map((em) => em.toLowerCase()).includes(m);
        if (!isEnabled) disabledMarketplaces.push(m);
        return isEnabled;
      })
      .filter(m => validMarketplaces.includes(m))
      .filter(m => {
        // Check circuit breaker before creating jobs
        if (taskCircuitBreaker.isSuspended(taskId, m)) {
          suspendedMarketplaces.push(m);
          return false;
        }
        return true;
      });


    // Clear parentJobs for this task at the start of each loop
    // This ensures we start fresh and don't have stale data from previous loops
    if (availableMarketplaces.length > 0) {
      const marketplacesToClear = marketplace ? [marketplace.toLowerCase()] : ['opensea', 'magiceden', 'blur'];
      let clearedParentJobs = 0;
      let preservedParentJobs = 0;
      for (const mp of marketplacesToClear) {
        // Clear parent job for current loop
        const parentId = getParentJobId(taskId, mp);
        const existingParent = parentJobs.get(parentId);

        if (existingParent) {
          // Only delete parent jobs that have completed queueing
          // This prevents race condition where startTaskInternal() deletes parent
          // while queue.addBulk() is still executing or jobs are being tracked
          if (existingParent.allJobsQueued) {
            parentJobs.delete(parentId);
            clearedParentJobs++;
          } else {
            // Preserve parent job - queueing still in progress
            preservedParentJobs++;
          }
        }
      }
      if (clearedParentJobs > 0) {
      }
      if (preservedParentJobs > 0) {
      }

      // Clear old jobs from queue to prevent state bleed
    }

    // Calculate and store task-wide totals after parent job initialization
    if (marketplace && task.running) {
      // When adding a marketplace to running task, just increment totals
      const parentJobId = `${taskId}-${marketplace}`;
      const parentJob = parentJobs.get(parentJobId);
      if (parentJob && taskStatus[taskId]) {

        // Global properties removed - all tracking is marketplace-specific
      }
    } else {
      // Full recalculation for new tasks or full restarts
      let taskTotalJobs = 0;
      let taskCompletedJobs = 0;
      for (const mp of availableMarketplaces) {
        const parentJobId = `${taskId}-${mp}`;
        const parentJob = parentJobs.get(parentJobId);
        if (parentJob) {
          taskTotalJobs += parentJob.totalJobs;
          taskCompletedJobs += parentJob.completedJobs;
        }
      }

      // Global job totals removed - using marketplace-specific data only
    }

    // Direct timer execution - call processor functions directly without SCHEDULE jobs
    // This eliminates the 10-minute BullMQ timeout that was causing cascading failures
    const marketplacesForExecution = marketplace ? [marketplace.toLowerCase()].filter(m => availableMarketplaces.includes(m)) : availableMarketplaces;

    if (marketplacesForExecution.length > 0) {
      // Performance profiling: execution phase start (CRITICAL - this blocks for 5-60+ minutes)
      const executionPhaseStart = Date.now();

      // Calculate delay for MagicEden based on total marketplace count
      const hasMagiceden = marketplacesForExecution.includes('magiceden');
      let magicedenDelay = 0;
      if (marketplacesForExecution.length === 3 && hasMagiceden) {
        magicedenDelay = 2000; // 2 seconds if all 3 marketplaces
      } else if (marketplacesForExecution.length === 2 && hasMagiceden) {
        magicedenDelay = 1000; // 1 second if 2 marketplaces
      }

      // Execute marketplaces directly (no BullMQ SCHEDULE jobs)
      const executionPromises: Promise<void>[] = [];

      for (const mp of marketplacesForExecution) {
        if (mp === 'magiceden' && magicedenDelay > 0) {
          // Delay MagicEden execution to stagger API calls
          executionPromises.push(
            new Promise<void>((resolve) => {
              setTimeout(async () => {
                try {
                  await executeMarketplaceBidLoop(taskId, mp);
                } catch (error) {
                  errorLog('MarketplaceExec', `FAILED | taskId=${taskId} | marketplace=${mp} | error=${error instanceof Error ? error.message : String(error)}`, error, { taskId });
                }
                resolve();
              }, magicedenDelay);
            })
          );
        } else {
          // Execute immediately
          executionPromises.push(
            (async () => {
              try {
                await executeMarketplaceBidLoop(taskId, mp);
              } catch (error) {
                errorLog('MarketplaceExec', `FAILED | taskId=${taskId} | marketplace=${mp} | error=${error instanceof Error ? error.message : String(error)}`, error, { taskId });
              }
            })()
          );
        }
      }

      // Execute all marketplaces (some may be delayed)
      await Promise.all(executionPromises);
      // CRITICAL: This await blocks until ALL bid loops complete (5-60+ minutes for large token lists)
      const executionPhaseDuration = Date.now() - executionPhaseStart;
    } else {
      warnLog('MarketplaceExec', `NO_MARKETPLACES | taskId=${taskId} | reason=all_filtered_out`, { taskId });
    }

    // Task status will be updated to RUNNING when jobs are actually queued in scheduled bid functions
    if (marketplace) {
      // Also ensure skipStats and errorStats are initialized
      skipStats[taskId] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      };
      errorStats[taskId] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      };
      // The progress will be recalculated automatically as jobs complete
    }

  } catch (error) {
    errorLog('Task', `Error starting task ${taskId}`, error);
    return { success: false, reason: 'error', error };
  } finally {
    // Always release lock when done
    unlockTask(taskId);

    // Log performance metrics
    const duration = Date.now() - startTime;

    // Performance profiling: function exit with total duration

    // TaskFlow logging: startTaskInternal completed

    // Log warning if startup took too long
    if (duration > 5000) {
      warnLog('TaskFlow',
        `START_INTERNAL_SLOW | taskId=${taskId} | mp=${marketplace || 'all'} | duration=${duration}ms`,
        { taskId, marketplace: mpKeyForLog, duration }
      );
    }
  }

  return { success: true };
}

// Enhanced BullMQ job cancellation for active jobs
// Helper function to map full marketplace names to short prefixes used in counterbid job IDs
function getCounterbidMarketplacePrefix(marketplace: string): string {
  const prefixMap: Record<string, string> = {
    'opensea': 'os',
    'magiceden': 'me',
    'blur': 'blur'
  };
  return prefixMap[marketplace.toLowerCase()] || marketplace.toLowerCase();
}

// Enhanced job removal from all queue states using correct job ID format
// Remove all jobs for a specific task from the queue
// NOTE: This function should ONLY be called from stopTask
// No other part of the codebase should remove jobs directly
//
// Performance optimization (Fix 6):
// - Only scan 'waiting' and 'delayed' states (active jobs can't be removed, completed/failed don't need removal)
// - Scan once per state and filter by taskId (not once per marketplace per state)
// - This reduces Redis operations from 15 to 2 for the main queue
async function removeAllTaskJobsFromQueue(taskId: string, marketplace?: string) {
  const startTime = Date.now();
  const marketplaces = marketplace ? [marketplace.toLowerCase()] : ['opensea', 'blur', 'magiceden'];
  let totalRemoved = 0;
  const removalResults = [];
  let mainQueueRemoved = 0;


  for (const mp of marketplaces) {
    // Prefix for scheduled bid jobs (e.g., bid-taskId-opensea-...)
    const bidPrefix = `bid-${taskId}-${mp}-`;
    // Prefix for counterbid jobs (e.g., CB-taskId-os-..., CB-taskId-me-..., CB-taskId-blur-...)
    const cbPrefix = `CB-${taskId}-${getCounterbidMarketplacePrefix(mp)}-`;

    const marketplaceResult = {
      marketplace: mp,
      waiting: 0,
      delayed: 0,
      total: 0
    };

    try {
      // Fix 6: Only scan 'waiting' and 'delayed' states (reduced from 5 to 2)
      // - active: Can't remove (we don't have the lock token) - blacklist handles these
      // - completed/failed: Already done, no need to remove - cleanup happens automatically
      // This reduces Redis operations from 5 to 2 per marketplace (60% fewer calls)
      const jobStates = ['waiting', 'delayed'] as const;
      let dataIdMatches = 0;
      let prefixMatches = 0;

      for (const state of jobStates) {
        const jobs = await queue.getJobs([state], 0, -1);

        // Match by taskId in job data (works for all jobs including those without explicit jobId)
        // Also match by job ID prefix for counterbid jobs with explicit IDs (CB-)
        const taskJobs = jobs.filter(job => {
          if (!job) return false;
          // Primary match: Check job.data._id or job.data.taskId (works for all jobs)
          const jobTaskId = job.data?._id?.toString() || job.data?.taskId?.toString();
          if (jobTaskId === taskId) {
            dataIdMatches++;
            return true;
          }
          // Secondary match: job ID prefix for counterbid jobs (CB-) that may not have taskId in data
          if (job.id && (job.id.startsWith(bidPrefix) || job.id.startsWith(cbPrefix))) {
            prefixMatches++;
            return true;
          }
          return false;
        });

        if (taskJobs.length > 0) {
        }

        let stateSuccessCount = 0;
        let stateFailCount = 0;
        for (const job of taskJobs) {
          try {
            // Remove jobs (waiting, delayed only - no active jobs in this optimized version)
            await job.remove();
            (marketplaceResult as any)[state]++;
            totalRemoved++;
            mainQueueRemoved++;
            stateSuccessCount++;
          } catch (error: any) {
            stateFailCount++;
            // Log the specific error for debugging - common errors:
            // - "Missing lock for job" - job transitioned to active during removal
            // - "Job not found" - job already completed/removed
            const errorMsg = error?.message || String(error);
            if (errorMsg.includes('Missing lock') || errorMsg.includes('not found')) {
            } else {
              errorLog('StopJobRemoval', `JOB_REMOVAL_FAILED | taskId=${taskId} | jobId=${job.id} | state=${state} | error=${errorMsg}`, { taskId });
            }
          }
        }

        if (stateSuccessCount > 0 || stateFailCount > 0) {
        }
      }

      marketplaceResult.total = marketplaceResult.waiting + marketplaceResult.delayed;


    } catch (error: any) {
      errorLog('Queue', `Error removing ${mp} jobs for task ${taskId}`, error);
      (marketplaceResult as any).error = error?.message || String(error);
    }

    removalResults.push(marketplaceResult);
  }


  // Job state verification - wait and re-check for any missed jobs
  if (totalRemoved > 0) {

    // Wait a short time for jobs that might be transitioning between states
    await new Promise(resolve => setTimeout(resolve, 500));

    // Re-check for any remaining jobs
    let remainingJobs = 0;
    let pass2Removed = 0;
    for (const mp of marketplaces) {
      // Same prefixes as first pass - scheduled bids and counterbids
      const bidPrefix = `bid-${taskId}-${mp}-`;
      const cbPrefix = `CB-${taskId}-${getCounterbidMarketplacePrefix(mp)}-`;
      // Fix 6: Only scan 'waiting' and 'delayed' states (same optimization as pass1)
      const jobStates = ['waiting', 'delayed'] as const;

      for (const state of jobStates) {
        const jobs = await queue.getJobs([state], 0, -1);
        // Match by taskId in job data (works for all jobs including those without explicit jobId)
        // Also match by job ID prefix for counterbid jobs with explicit IDs (CB-)
        const taskJobs = jobs.filter(job => {
          if (!job) return false;
          // Primary match: Check job.data._id or job.data.taskId (works for all jobs)
          const jobTaskId = job.data?._id?.toString() || job.data?.taskId?.toString();
          if (jobTaskId === taskId) return true;
          // Secondary match: job ID prefix for counterbid jobs (CB-) that may not have taskId in data
          if (job.id && (job.id.startsWith(bidPrefix) || job.id.startsWith(cbPrefix))) return true;
          return false;
        });

        if (taskJobs.length > 0) {
          remainingJobs += taskJobs.length;
          warnLog('StopJobRemoval', `JOB_REMOVAL_PASS2_FOUND | taskId=${taskId} | mp=${mp} | state=${state} | remainingCount=${taskJobs.length}`, { taskId });

          // Try to remove them again
          for (const job of taskJobs) {
            try {
              await job.remove();
              totalRemoved++;
              mainQueueRemoved++;
              pass2Removed++;
            } catch (error: any) {
              const errorMsg = error?.message || String(error);
              if (errorMsg.includes('Missing lock') || errorMsg.includes('not found')) {
              } else {
                errorLog('StopJobRemoval', `JOB_REMOVAL_PASS2_FAILED | taskId=${taskId} | jobId=${job.id} | state=${state} | error=${errorMsg}`, { taskId });
              }
            }
          }
        }
      }
    }

  }

  // Blur jobs now in main queue - no separate blur queue to scan

  const elapsedMs = Date.now() - startTime;

  return {
    removed: totalRemoved,
    results: removalResults,
    success: true
  };
}

// Cancel marketplace-specific rate limiter requests
async function cancelMarketplaceRateLimiterJobs(taskId: string, marketplace?: string) {
  try {
    // Cancel Blur rate limiter requests
    if (!marketplace || marketplace.toLowerCase() === 'blur') {
      // For now, just log that we would cancel Blur rate limiter requests
      // TODO: Implement cancelTaskRequests method in BlurRateLimiter if needed
    }

    // Add other marketplace rate limiter cancellations as needed
    // OpenSea and MagicEden don't currently have specific rate limiters

  } catch (error) {
    errorLog('Queue', `Error cancelling rate limiter jobs for task ${taskId}`, error);
  }
}

/**
 * Safely delete order from cache with error handling
 * Returns true if deleted, false if failed
 */
async function safeDeleteFromCache(
  taskId: string,
  marketplace: string,
  identifier: string,
  orderHash: string
): Promise<boolean> {
  try {
    // O(1) direct deletion - same method as verification
    const deleted = orderCache.deleteOne(taskId, marketplace, identifier, orderHash);

    if (!deleted) {
      warnLog('OrderCache',
        `DELETE_NOT_FOUND | Order not in cache | taskId=${taskId} | orderHash=${orderHash?.substring(0, 16)}...`,
        { taskId, marketplace: marketplace as 'blur' | 'opensea' | 'magiceden', identifier }
      );
    }

    return deleted;
  } catch (error) {
    errorLog('OrderCache', `DELETE_FAILED | taskId=${taskId}`, error, {
      taskId,
      marketplace: marketplace as 'blur' | 'opensea' | 'magiceden'
    });
    return false;
  }
}

// Helper function to attempt cancellation of any remaining orders
async function attemptRemainingCancellations(taskId: string, marketplace: string): Promise<number> {

  try {
    // Fetch the remaining active orders from cache (auto-filters expired)
    const cachedOrders = await orderCache.getByTaskAndMarketplace(taskId, marketplace.toLowerCase());
    const remainingOrders = cachedOrders.slice(0, 500); // Limit to prevent overwhelming the system

    if (remainingOrders.length === 0) {
      return 0;
    }


    // Get the task to find wallet ID
    const task = await Task.findById(taskId);
    if (!task) {
      return remainingOrders.length;
    }

    // Log specific order details for debugging
    remainingOrders.slice(0, 5).forEach((order, index) => {
    });
    if (remainingOrders.length > 5) {
    }

    // Attempt to cancel based on marketplace
    const marketplaceLower = marketplace.toLowerCase();
    const ordersArray = remainingOrders;

    // Convert ObjectId to string (MongoDB returns ObjectId objects from findById)
    const walletIdStr = task.walletId?.toString();
    if (!walletIdStr) {
      errorLog('Cancellation', `attemptRemainingCancellations | SKIP | taskId=${taskId} | reason=no_walletId`);
      return remainingOrders.length;
    }

    switch (marketplaceLower) {
      case 'opensea':
        await cancelOpenseaBids(ordersArray, walletIdStr, taskId, 'WETH');
        break;
      case 'magiceden':
        await cancelMagicedenBids(ordersArray, walletIdStr, taskId);
        break;
      case 'blur':
        await cancelBlurBids(ordersArray, walletIdStr, taskId);
        break;
      default:
        return remainingOrders.length;
    }


    // Wait a bit for jobs to be processed
    await new Promise(resolve => setTimeout(resolve, 5000));

    // Re-check how many orders remain (Redis auto-filters expired via TTL)
    const stillActiveCount = await orderCache.countByTaskAndMarketplace(taskId, marketplace.toLowerCase());

    return stillActiveCount;

  } catch (error) {
    errorLog('Cancellation', 'Error attempting remaining cancellations', error);
    return -1; // Return -1 to indicate error
  }
}

// Enhanced cancellation completion validation with race condition protection
async function validateAndTransitionToStopped(taskId: string, marketplace: string, marketplaceName: string, retryCount = 0): Promise<void> {
  const MAX_RETRIES = 3;
  const RETRY_DELAY = 500; // 500ms delay between retries

  try {
    // First check: Count active orders for this marketplace (uses EXISTS check to handle stale SET members)
    const validCounts = await orderCache.countValidByTaskAndMarketplace(taskId);
    const activeOrderCount = validCounts[marketplace.toLowerCase()] || 0;

    // Get current tracking status to validate completion
    const trackingStatus = cancellationTracker.getStatus(taskId, marketplace);
    const isTrackingComplete = trackingStatus ? cancellationTracker.isComplete(taskId, marketplace) : false;

    // Log cache validation

    if (activeOrderCount > 0) {
      // Still have active orders - check if we should retry or if this is expected
      if (retryCount < MAX_RETRIES) {

        // Wait for potential database updates to complete
        await new Promise(resolve => setTimeout(resolve, RETRY_DELAY));

        // Retry validation
        return await validateAndTransitionToStopped(taskId, marketplace, marketplaceName, retryCount + 1);
      } else {
        // Max retries reached but still have orders - check for orphaned cache entries first

        // Enhanced validation: Check if orders in cache were successfully cancelled on-chain
        const cachedOrders = await orderCache.getByTaskAndMarketplace(taskId, marketplace.toLowerCase());
        const successfulCancellations = successfullyCancelledOrders.get(taskId) || new Set();

        // Identify orders that were successfully cancelled but still in cache (orphans)
        const orphanedInCache = cachedOrders.filter(order => {
          const orderId = order.orderHash;
          return successfulCancellations.has(orderId);
        });

        if (orphanedInCache.length > 0) {
          warnLog('StopTask', `ORPHANED_ORDERS_IN_CACHE | taskId=${taskId} | marketplace=${marketplaceName} | count=${orphanedInCache.length}`, { taskId });
          // Force delete only the confirmed-cancelled orders
          for (const order of orphanedInCache) {
            await safeDeleteFromCache(taskId, marketplace.toLowerCase(), order.identifier, order.orderHash);
          }
        }

        // Re-count after orphan cleanup
        const validCountsAfterCleanup = await orderCache.countValidByTaskAndMarketplace(taskId);
        const activeOrderCountAfterCleanup = validCountsAfterCleanup[marketplace.toLowerCase()] || 0;

        if (activeOrderCountAfterCleanup === 0) {
          // All orphans cleaned - transition to STOPPED
          setTaskStatus(taskId, 'STOPPED', `${marketplaceName} cancellation complete (orphan cleanup)`, marketplace);

          // Check if all marketplaces complete
          const allComplete = ['opensea', 'magiceden', 'blur'].every(mp => {
            const status = taskStatus[taskId]?.marketplaces[mp as keyof typeof taskStatus[string]['marketplaces']];
            return !status || status.status === 'STOPPED' || mp === marketplaceName;
          });
          if (allComplete) {
            cancellationTracker.clearTracking(taskId);
            successfullyCancelledOrders.delete(taskId);
          }
          return;
        }

        // Still have non-orphaned orders - attempt to cancel them
        const remainingAfterAttempt = await attemptRemainingCancellations(taskId, marketplace);

        if (remainingAfterAttempt === 0) {
          // Successfully cancelled all remaining orders
          setTaskStatus(taskId, 'STOPPED', `${marketplaceName} cancellation complete after cleanup`, marketplace);

          // Check if all marketplaces are now complete
          const allComplete = ['opensea', 'magiceden', 'blur'].every(mp => {
            const status = taskStatus[taskId]?.marketplaces[mp as keyof typeof taskStatus[string]['marketplaces']];
            return !status || status.status === 'STOPPED' || mp === marketplaceName;
          });
          if (allComplete) {
            cancellationTracker.clearTracking(taskId);
            // Clear success tracking for this task
            successfullyCancelledOrders.delete(taskId);
          }
          return;
        } else if (remainingAfterAttempt > 0) {
          // Still have orders after cancellation attempt
          // Trust the job completion handlers to clean cache - no premature force cleanup
          warnLog('StopTask', `ORDERS_REMAINING_AFTER_ATTEMPT | taskId=${taskId} | marketplace=${marketplaceName} | remaining=${remainingAfterAttempt}`, { taskId });

          // Reset the cancellation percentage to reflect actual state
          const mpKey = marketplaceName as 'opensea' | 'magiceden' | 'blur';
          if (trackingStatus && trackingStatus.totalBids > 0) {
            const actualCompleted = Math.max(0, trackingStatus.totalBids - remainingAfterAttempt);
            const actualPercentage = (actualCompleted / trackingStatus.totalBids) * 100;

            if (taskStatus[taskId]?.marketplaces[mpKey]) {
              taskStatus[taskId].marketplaces[mpKey]!.cancellationPercentage = Math.floor(actualPercentage);
            }
          }
        }
        return;
      }
    }

    // No active orders found - safe to transition to STOPPED
    if (isTrackingComplete || activeOrderCount === 0) {
      // Verify zero remaining in cache for this marketplace (uses EXISTS check to handle stale SET members)
      const validCountsCheck = await orderCache.countValidByTaskAndMarketplace(taskId);
      const remainingInCache = validCountsCheck[marketplaceName.toLowerCase()] || 0;

      if (remainingInCache > 0) {
        errorLog('StopTask', `INCOMPLETE CANCELLATION | taskId=${taskId} | marketplace=${marketplaceName} | cache=${remainingInCache} | db=${activeOrderCount}`, undefined, { taskId });
      }

      // Cleanup cache for this marketplace
      await orderCache.deleteByTaskAndMarketplace(taskId, marketplaceName);

      // CRITICAL: Set status to STOPPED BEFORE final broadcast
      // This ensures clients receive the correct status='STOPPED' in the final WebSocket message
      setTaskStatus(taskId, 'STOPPED', `${marketplaceName} cancellation complete - validated with enhanced checks`, marketplace);

      // Check if ALL marketplaces are now complete - if so, cleanup bidStats
      const allMarketplacesComplete = ['opensea', 'magiceden', 'blur'].every(mp => {
        const status = taskStatus[taskId]?.marketplaces[mp as keyof typeof taskStatus[string]['marketplaces']];
        return !status || status.status === 'STOPPED' || mp === marketplaceName;
      });

      if (allMarketplacesComplete && bidStats[taskId]) {
        // CRITICAL: Broadcast final state BEFORE deleting bidStats
        // Otherwise publishTaskMetrics() won't include this taskId in the loop (line 6785)
        // and the dashboard will never receive the final STOPPED status update
        const finalTaskData = {
          bidCounts: { opensea: 0, magiceden: 0, blur: 0 },
          skipCounts: skipStats[taskId] || {},
          errorCounts: errorStats[taskId] || {},
          counterbidCounts: counterbidCounts[taskId] || {},
          floorPrices: floorPrices[taskId] || {},
          bestOffers: bestOffers[taskId] || {},
          warningBids: warningBids[taskId] || {},
          warningMessage: warningMessage[taskId] || null,
          taskStatus: taskStatus[taskId],
          loopCounterStats: loopCounterStats[taskId] || {}
        };

        // Send final broadcast immediately
        broadcastManager.broadcastTaskUpdate(taskId, finalTaskData);


        // Now safe to delete bidStats - final state already broadcast
        delete bidStats[taskId];
        await orderCache.deleteByTask(taskId);

        // Check if all marketplaces are stopped - clean up taskStatus for fully stopped tasks
        if (taskStatus[taskId]) {
          const allStopped = Object.values(taskStatus[taskId].marketplaces).every(
            mp => ['STOPPED', 'COMPLETED', 'IDLE'].includes(mp.status)
          );

          if (allStopped) {
            // Clean up taskStatus for fully stopped tasks
            delete taskStatus[taskId];
          }
        }

        // Clear processedOwnBids cache - prevents memory leak and ensures fresh start on task restart
        processedOwnBids.clear();

        // All marketplace cancellation jobs complete - safe to cleanup tracking data
        cancellationTracker.clearTracking(taskId);
        successfullyCancelledOrders.delete(taskId);

      }

      // Log AFTER snapshot for this marketplace (Redis TTL handles expiry)
      const orderCountAfter = await orderCache.countByTaskAndMarketplace(taskId, marketplaceName);

      // Status already set to STOPPED earlier (before final broadcast)
    } else {
    }

  } catch (error) {
    errorLog('Cancellation', `Error validating ${marketplaceName} cancellation completion`, error);

    // On error, keep CANCELLING status to be safe
  }
}

// Centralized function to update task cancellation percentage - single source of truth
export async function updateTaskCancellationPercentage(taskId: string, forcePercentage?: number, marketplace?: string) {

  // Log current tracker state
  const allTrackings = cancellationTracker.getAllTrackings();
  const trackingKeys = Array.from(allTrackings.keys()).filter(k => k.includes(taskId));
  if (trackingKeys.length > 0) {
    allTrackings.forEach((tracking, key) => {
      if (key.includes(taskId)) {
        // Tracking state available
      }
    });
  } else {
    // UPDATE_PCT_NO_TRACKING log removed - logs when no tracking data exists, which is normal behavior (30MB/day)
  }

  if (!taskStatus[taskId]) {
    const task = activeTasks.get(taskId);
    taskStatus[taskId] = {
      selectedMarketplaces: task?.selectedMarketplaces.map(m => m.toLowerCase()) || [],
      lastUpdate: Date.now(),
      marketplaces: {}
    };

    // Initialize marketplace-specific cancellation status
    const selectedMarketplaces = task?.selectedMarketplaces || [];
    selectedMarketplaces.forEach(m => {
      const marketplaceName = m.toLowerCase() as 'opensea' | 'magiceden' | 'blur';
      taskStatus[taskId].marketplaces[marketplaceName] = {
        status: 'CANCELLING',
        runningPercentage: 0,
        cancellationPercentage: 0,
        processingPercentage: 0,
        totalJobs: 0,
        completedJobs: 0,
        lastUpdate: Date.now()
      };
    });

  }

  let percentage: number;

  if (forcePercentage !== undefined) {
    // Allow forcing a specific percentage (0 or 100)
    percentage = Math.min(100, Math.max(0, forcePercentage));
  } else {
    // Check if we have task-level tracking (global cancellation)
    const hasTaskLevel = cancellationTracker.hasTaskLevelTracking(taskId);

    if (hasTaskLevel) {
      // Use aggregated percentage for global cancellations
      const aggregatedPercentage = cancellationTracker.getAggregatedPercentage(taskId);
      if (aggregatedPercentage !== null) {
        percentage = aggregatedPercentage;
      } else {
        // UPDATE_PCT_EXIT_NULL log removed - logs when tracking data is null, normal behavior (30MB/day)
        return;
      }
    } else {
      // Use regular percentage for marketplace-specific cancellations
      const trackerPercentage = cancellationTracker.getPercentage(taskId, marketplace);
      if (trackerPercentage !== null) {
        percentage = trackerPercentage;
      } else {
        // UPDATE_PCT_EXIT_NULL log removed - logs when tracking data is null, normal behavior (30MB/day)
        return;
      }
    }
  }

  // Update marketplace-specific cancellation percentages

  const task = activeTasks.get(taskId);
  const selectedMarketplaces = task?.selectedMarketplaces || [];

  // If marketplace is specified, only update that specific marketplace
  if (marketplace) {
    const mpKey = marketplace.toLowerCase() as 'opensea' | 'magiceden' | 'blur';
    if (taskStatus[taskId].marketplaces[mpKey]) {
      // Get marketplace-specific percentage
      const marketplacePercentage = forcePercentage !== undefined ? forcePercentage :
        cancellationTracker.getPercentage(taskId, marketplace) ?? percentage;

      const prevPct = taskStatus[taskId].marketplaces[mpKey]!.cancellationPercentage;
      taskStatus[taskId].marketplaces[mpKey]!.cancellationPercentage = marketplacePercentage;
    }
  } else {
    // Global cancellation - update each marketplace with its own percentage
    const cancellingMarketplaces = Object.entries(taskStatus[taskId]?.marketplaces || {})
      .filter(([_, status]) => status?.status === 'CANCELLING')
      .map(([mp, _]) => mp);
    const marketplacesToUpdate = cancellingMarketplaces.length > 0 ? cancellingMarketplaces : selectedMarketplaces;

    for (const mp of marketplacesToUpdate) {
      const mpKey = mp.toLowerCase() as 'opensea' | 'magiceden' | 'blur';
      if (taskStatus[taskId].marketplaces[mpKey]) {
        // Get marketplace-specific percentage from tracker
        const marketplacePercentage = cancellationTracker.getPercentage(taskId, mp) ?? percentage;

        const prevPct = taskStatus[taskId].marketplaces[mpKey]!.cancellationPercentage;
        taskStatus[taskId].marketplaces[mpKey]!.cancellationPercentage = marketplacePercentage;
      }
    }
  }

  // If we've reached 100%, check for remaining active orders with enhanced validation
  // IMPORTANT: Only transition to STOPPED when marketplace-specific cancellation is truly complete
  if (marketplace) {
    // Marketplace-specific cancellation - check if THIS marketplace reached 100%
    const marketplaceName = marketplace.toLowerCase() as 'opensea' | 'magiceden' | 'blur';
    const isCancellingThisMarketplace = taskStatus[taskId]?.marketplaces[marketplaceName]?.status === 'CANCELLING';
    const marketplacePercentage = taskStatus[taskId]?.marketplaces[marketplaceName]?.cancellationPercentage || 0;

    // Use >= 100 to handle floating point precision and ensure transition at 100%+
    if (isCancellingThisMarketplace && marketplacePercentage >= 100) {

      // FOR NOW: Keep the old logic but add extensive logging to prove this is the issue

      await validateAndTransitionToStopped(taskId, marketplace, marketplaceName);
    } else {
    }
  } else if (percentage >= 100) {
    // Global cancellation - check all marketplaces
    const cancellingMarketplaces = Object.entries(taskStatus[taskId]?.marketplaces || {})
      .filter(([_, status]) => status?.status === 'CANCELLING')
      .map(([mp, _]) => mp);

    if (cancellingMarketplaces.length > 0) {
      try {
        // Redis TTL handles expiry
        const activeOrderCount = await orderCache.countByTask(taskId);

        if (activeOrderCount === 0) {
          // Verify zero remaining in cache
          const remainingInCache = await orderCache.countByTask(taskId);

          if (remainingInCache > 0) {
            errorLog('StopTask', `INCOMPLETE CANCELLATION | taskId=${taskId} | cache=${remainingInCache}`, undefined, { taskId });
          }

          // Now safe to cleanup bidStats and orderCache
          await orderCache.deleteByTask(taskId);
          if (bidStats[taskId]) {
            delete bidStats[taskId];
          }

          // Check if all marketplaces are stopped - clean up taskStatus for fully stopped tasks
          if (taskStatus[taskId]) {
            const allStopped = Object.values(taskStatus[taskId].marketplaces).every(
              mp => ['STOPPED', 'COMPLETED', 'IDLE'].includes(mp.status)
            );

            if (allStopped) {
              // Clean up taskStatus for fully stopped tasks
              delete taskStatus[taskId];
            }
          }

          // Clear processedOwnBids cache - prevents memory leak and ensures fresh start on task restart
          processedOwnBids.clear();

          // Clear failed task tracking maps (prevents unbounded accumulation)
          taskPreprocessing.delete(taskId);
          queueingProgress.delete(taskId);
          failedTaskStartQueue.delete(taskId);
          failedTaskStopQueue.delete(taskId);

          // Log AFTER snapshot
          const orderCountAfter = await orderCache.countByTask(taskId);

          // Transition all marketplaces back to STOPPED
          for (const mp of cancellingMarketplaces) {
            setTaskStatus(taskId, 'STOPPED', 'global cancellation complete - 100% and no active orders', mp);
          }

          // All cancellation jobs complete - safe to cleanup tracking data
          cancellationTracker.clearTracking(taskId);
          successfullyCancelledOrders.delete(taskId);
        } else {
          // Still have active orders at 100% - attempt to cancel them

          // Attempt to cancel remaining orders for each marketplace
          let totalRemaining = 0;
          for (const mp of cancellingMarketplaces) {
            const remainingForMarketplace = await attemptRemainingCancellations(taskId, mp);
            if (remainingForMarketplace >= 0) {
              totalRemaining += remainingForMarketplace;
            }
          }

          if (totalRemaining === 0) {
            // Transition all marketplaces to STOPPED
            for (const mp of cancellingMarketplaces) {
              setTaskStatus(taskId, 'STOPPED', 'global cancellation complete after cleanup', mp);
            }

            // All cancellation jobs complete - safe to cleanup tracking data
            cancellationTracker.clearTracking(taskId);
            successfullyCancelledOrders.delete(taskId);
          } else {
            // Adjust percentages based on actual remaining orders
            const trackingStatus = cancellationTracker.getStatus(taskId);
            if (trackingStatus && trackingStatus.totalBids > 0) {
              const actualCompleted = Math.max(0, trackingStatus.totalBids - totalRemaining);
              const actualPercentage = Math.floor((actualCompleted / trackingStatus.totalBids) * 100);

              // Update all cancelling marketplaces with adjusted percentage
              for (const mp of cancellingMarketplaces) {
                const mpKey = mp as keyof typeof taskStatus[string]['marketplaces'];
                if (taskStatus[taskId]?.marketplaces[mpKey]) {
                  taskStatus[taskId].marketplaces[mpKey]!.cancellationPercentage = actualPercentage;
                }
              }
            }
          }
        }
      } catch (error) {
        errorLog('Cancellation', 'Error checking for remaining orders', error);
      }
    }
  }
}

// Enhanced progress tracking for cancellation (now uses centralized function)
function updateCancellationProgress(taskId: string, completed: number, total: number) {
  // This function is deprecated - cancellation progress is now tracked by the cancellationTracker
  // Log progress for debugging only, don't update the actual percentage
  const percentage = total > 0 ? Math.min(100, Math.round((completed / total) * 100)) : 100;
}

// Helper function to fetch orders for cancellation
// Uses cache first (fast), falls back to MongoDB if cache is empty (ensures no orders missed)
async function fetchOrdersForCancellation(taskId: string, marketplace?: string): Promise<{
  opensea: any[];
  magiceden: any[];
  blur: any[];
  sources: { opensea: string; magiceden: string; blur: string };
}> {
  const sources: { opensea: string; magiceden: string; blur: string } = {
    opensea: 'none',
    magiceden: 'none',
    blur: 'none'
  };

  try {
    // Helper to get orders from Redis (source of truth, TTL handles expiry)
    const getOrdersWithFallback = async (mp: 'opensea' | 'magiceden' | 'blur'): Promise<any[]> => {
      // Redis is the source of truth - TTL handles expiry automatically
      const cacheOrders = await orderCache.getByTaskAndMarketplace(taskId, mp);
      if (cacheOrders.length > 0) {
        sources[mp] = 'cache';
        cancellationDebugLogger.log('ORDERS_SOURCE', `${mp}: Found ${cacheOrders.length} orders in Redis`, {
          marketplace: mp,
          count: cacheOrders.length,
          source: 'redis'
        });
        return cacheOrders;
      }

      // No orders found in Redis (source of truth)
      sources[mp] = 'empty';
      cancellationDebugLogger.log('ORDERS_SOURCE', `${mp}: No orders found in Redis`, {
        marketplace: mp,
        count: 0,
        source: 'empty'
      });
      return [];
    };

    if (marketplace) {
      // Fetch orders only for the specified marketplace
      const marketplaceLower = marketplace.toLowerCase() as 'opensea' | 'magiceden' | 'blur';
      const orders = await getOrdersWithFallback(marketplaceLower);

      return {
        opensea: marketplaceLower === 'opensea' ? orders : [],
        magiceden: marketplaceLower === 'magiceden' ? orders : [],
        blur: marketplaceLower === 'blur' ? orders : [],
        sources
      };
    } else {
      // Fetch orders for all marketplaces with fallback
      const [openseaOrders, magicedenOrders, blurOrders] = await Promise.all([
        getOrdersWithFallback('opensea'),
        getOrdersWithFallback('magiceden'),
        getOrdersWithFallback('blur')
      ]);

      return {
        opensea: openseaOrders,
        magiceden: magicedenOrders,
        blur: blurOrders,
        sources
      };
    }
  } catch (error) {
    errorLog('Cancellation', `Error fetching orders for task ${taskId}`, error);
    cancellationDebugLogger.log('ORDERS_SOURCE', 'ERROR fetching orders', {
      error: error instanceof Error ? error.message : String(error)
    });
    return {
      opensea: [],
      magiceden: [],
      blur: [],
      sources
    };
  }
}

async function stopTask(task: ITask, start: boolean, marketplace?: string, stoppedByWarning = false) {
  const taskId = task._id.toString();
  const stopStartTime = Date.now();

  // === NEW STOPTASK DEBUG LOGGING - ENTRY ===

  // Record BEFORE counts for audit (Redis is source of truth, TTL handles expiry)
  const orderCountBefore = await orderCache.countByTask(taskId);

  // Capture limiter state at entry - critical for diagnosing in-flight bid issues
  const limiterState = getLimiterQueueDetails(RATE_LIMIT);

  // Log timing for pre-checks

  // Start cancellation debug logging (only writes if DEBUG=true)
  cancellationDebugLogger.start(taskId, task.contract.slug);
  cancellationDebugLogger.log('ENTRY', 'stopTask called', {
    taskId,
    slug: task.contract.slug,
    marketplace: marketplace || 'all',
    stoppedByWarning,
    selectedMarketplaces: task.selectedMarketplaces
  });

  // Log comprehensive entry state for stopTask debugging

  try {
    // === PHASE 0: IMMEDIATE ABORT (<100ms target) ===
    // Critical: Execute abort operations FIRST before any other processing
    const phase0StartTime = Date.now();

    // 1. Blacklist task immediately to prevent new jobs from being queued
    cancelTaskJobs(taskId, marketplace);

    // 2. Abort jobs pending in Bottleneck rate limiter queue
    const limiterAbortStart = Date.now();
    const limiterAborted = abortPendingLimiterJobs(taskId);

    // 3. Abort in-flight HTTP requests for all marketplaces in parallel
    const httpAbortStart = Date.now();
    const [osAborted, meAborted, blurAborted] = await Promise.all([
      cancelOpenseaRequest(taskId),
      cancelMagicedenRequest(taskId),
      cancelBlurRequest(taskId)
    ]);

    // Fix 7: Unsubscribe from WebSocket EARLY (PHASE_0) to prevent counterbid handlers
    // from receiving events during cancellation. Only for full task stop (not marketplace-specific).
    let phase0UnsubscribeResult: { success: boolean; unsubscribed?: boolean; error?: string } | null = null;
    if (!marketplace) {
      const wsUnsubStart = Date.now();
      phase0UnsubscribeResult = await unsubscribeFromCollection(task);
      if (!phase0UnsubscribeResult.success) {
        errorLog('WebSocket', `[PHASE_0] Failed to unsubscribe from collection: taskId=${task._id}`, new Error(phase0UnsubscribeResult.error || 'Unknown error'));
      } else if (phase0UnsubscribeResult.unsubscribed) {
      } else {
      }
    }

    const phase0Elapsed = Date.now() - phase0StartTime;

    // === END PHASE 0 ===

    // Initialize taskStatus if needed
    if (!taskStatus[taskId]) {
      taskStatus[taskId] = {
        selectedMarketplaces: task.selectedMarketplaces.map(m => m.toLowerCase()),
        lastUpdate: Date.now(),
        marketplaces: {}
      }
    }

    // === STEP 1: PREPARE FOR TASK STOPPING ===
    // Clear scheduling data and prepare for status transitions
    // NOTE: Do NOT set STOPPED status yet - will transition to CANCELLING if orders exist, STOPPED if no orders
    if (!marketplace) {
      // Full task stop: clear global scheduling data
      delete loopCounterStats[task._id];
      if (taskStatus[taskId]) {
        // Clear marketplace-specific scheduling fields
        Object.keys(taskStatus[taskId].marketplaces).forEach(mp => {
          if (taskStatus[taskId].marketplaces[mp as keyof typeof taskStatus[string]['marketplaces']]) {
            delete taskStatus[taskId].marketplaces[mp as keyof typeof taskStatus[string]['marketplaces']]!.nextLoopTime;
            taskStatus[taskId].marketplaces[mp as keyof typeof taskStatus[string]['marketplaces']]!.waitingTime = 0;
          }
        });
      }
    }

    let WALLET_PRIVATE_KEY: string | undefined;
    let walletAddress: string | undefined;
    try {
      const wallet = await walletCache.getWalletById(task.walletId);
      walletAddress = wallet?.address;
      if (wallet?.encryptedPrivateKey) {
        WALLET_PRIVATE_KEY = decryptPrivateKey(wallet.encryptedPrivateKey);
      }
      cancellationDebugLogger.log('WALLET', WALLET_PRIVATE_KEY ? 'Decryption successful' : 'No private key found', {
        address: walletAddress ? `${walletAddress.slice(0, 6)}...${walletAddress.slice(-4)}` : 'N/A',
        hasPrivateKey: !!WALLET_PRIVATE_KEY
      });
    } catch (decryptError) {
      errorLog('StopTask', `Failed to decrypt wallet for task ${taskId}`, decryptError);
      cancellationDebugLogger.log('WALLET', 'Decryption FAILED', {
        error: decryptError instanceof Error ? decryptError.message : String(decryptError)
      });
      // Continue with stopTask even if wallet decryption fails
    }
    // === ENHANCED STOPTASK: IMMEDIATE JOB CANCELLATION ===
    let totalOperations = 0;
    let completedOperations = 0;
    let completionHandled = false; // Track if completion has been handled

    // Step 1: Blacklist task to prevent new jobs (immediate)
    // CRITICAL: Use cancelTaskJobs() which ADDS to blacklist, not isTaskBlacklisted() which only CHECKS
    const blacklistResult = cancelTaskJobs(taskId, marketplace);
    cancellationDebugLogger.log('BLACKLIST', 'Task blacklisted to prevent new jobs', {
      scope: marketplace || 'all',
      queuedJobs: blacklistResult.queuedJobs || 0
    });
    totalOperations += 1;
    completedOperations += 1;
    updateCancellationProgress(taskId, completedOperations, totalOperations + 3); // +3 for remaining major steps

    // === PHASE 1: JOB QUEUE CLEANUP ===
    const phase1StartTime = Date.now();

    // Step 2: Cancel active jobs and remove from queue (most important)
    const queueResult = await removeAllTaskJobsFromQueue(taskId, marketplace);
    cancellationDebugLogger.log('JOB_CANCEL', `Removed ${queueResult.removed} jobs from queue`, {
      jobsRemoved: queueResult.removed
    });
    totalOperations += Math.max(1, queueResult.removed);
    completedOperations += Math.max(1, queueResult.removed);
    updateCancellationProgress(taskId, completedOperations, totalOperations + 2); // +2 for remaining steps

    // Step 3: Cancel marketplace rate limiter requests
    await cancelMarketplaceRateLimiterJobs(taskId, marketplace);
    completedOperations += 1;
    updateCancellationProgress(taskId, completedOperations, totalOperations + 1); // +1 for remaining step

    // Log limiter state after initial cleanup
    const limiterStateAfterCleanup = getLimiterQueueDetails(RATE_LIMIT);
    const phase1Elapsed = Date.now() - phase1StartTime;

    // === EXISTING STOPTASK LOGIC CONTINUES ===
    if (!marketplace || task.selectedMarketplaces.length === 0) {
      markTaskAsAborted(taskId);
      const updatedTask = { ...task, running: false };
      activeTasks.set(taskId, updatedTask);

      // Remove task from indexes
      removeTaskFromIndexes(task);

      // Clean up all task resources - preserve warnings if stopped by warning
      // preserveParentJobs=true: During stopTask, preserve parent jobs so workers can still mark their completions
      // Parent jobs will be cleaned up by cleanupOldParentJobs() with 5-loop retention
      // preserveOrders=true: During stopTask, preserve order cache so cancellation can track progress
      // Order cache will be cleaned up in updateTaskCancellationPercentage when 100% complete
      cleanupTaskResources(taskId, stoppedByWarning, true, true);

      // Clear preprocessing data
      taskPreprocessing.delete(taskId);
      skipStats[taskId] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      };
      errorStats[taskId] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      };
      // NOTE: counterbidCounts is NOT reset here - it tracks cumulative counterbids across all loops

      // NOTE: Do NOT clear orderCache here - it will be cleared in updateTaskCancellationPercentage when 100% complete
      // This allows proper decrementBidCount tracking during cancellation

      // Decrement reference count for collection warming
      // Only stops warming if no other tasks use this collection
      collectionDataWarmer.stopWarming(task.contract.slug, taskId);
    }

    if (!marketplace && !stoppedByWarning) {
      // Only clear warningBids if NOT stopped by warning
      warningBids[taskId] = {
        opensea: false,
        magiceden: false,
        blur: false
      };
    }

    // === PHASE 2: ORDER FETCHING AND CANCELLATION ===
    const phase2StartTime = Date.now();

    // Create a distributed lock to prevent concurrent order fetching/cancellation
    const lockManager = new DistributedLockManager({
      lockPrefix: 'task-cancellation:',
      defaultTTLSeconds: 60 // 1 minute lock
    });

    // Use wallet ID in lock key to prevent conflicts across tasks using same wallet
    const wallet = await walletCache.getWalletById(task.walletId);
    const lockKey = `${wallet?.address || taskId}-${marketplace || 'all'}`;

    // Fetch orders for cancellation with lock
    let orders: { opensea: any[]; magiceden: any[]; blur: any[] };
    let orderSources: { opensea: string; magiceden: string; blur: string } = { opensea: 'none', magiceden: 'none', blur: 'none' };

    const fetchResult = await lockManager.withLock(lockKey, async () => {
      const fetchedOrders = await fetchOrdersForCancellation(taskId, marketplace);
      return fetchedOrders;
    });

    if (!fetchResult) {
      cancellationDebugLogger.log('ORDERS_FETCH', 'Failed to acquire lock for order fetching', {});
      cancellationDebugLogger.finish({ status: 'FAILED', reason: 'lock_acquisition_failed' });

      // Fix 7: WebSocket unsubscribe already done in PHASE_0, no need to do it again on lock failure
      return;
    }

    orders = { opensea: fetchResult.opensea, magiceden: fetchResult.magiceden, blur: fetchResult.blur };
    orderSources = fetchResult.sources;
    cancellationDebugLogger.log('ORDERS_FETCH', 'Orders retrieved - SUMMARY', {
      opensea: `${orders.opensea.length} (${orderSources.opensea})`,
      magiceden: `${orders.magiceden.length} (${orderSources.magiceden})`,
      blur: `${orders.blur.length} (${orderSources.blur})`,
      total: orders.opensea.length + orders.magiceden.length + orders.blur.length
    });

    // Log order counts with new prefix for Phase 2 tracking
    const phase2OrderFetchDuration = Date.now() - phase2StartTime;

    // === STEP 2: TRANSITION TO APPROPRIATE STATE BASED ON ORDERS ===
    // Determine which marketplaces have orders to cancel
    const marketplacesWithOrders = [];
    const marketplacesWithoutOrders = [];

    if (marketplace) {
      // Marketplace-specific stop
      const mp = marketplace.toLowerCase();
      const ordersForMarketplace = orders[mp as keyof typeof orders] || [];
      if (ordersForMarketplace.length > 0) {
        marketplacesWithOrders.push(mp);
        setTaskStatus(taskId, "CANCELLING", `Cancelling ${ordersForMarketplace.length} ${mp} orders`, marketplace);
      } else {
        // Check if marketplace is enabled before setting to STOPPED
        const isEnabled = enabledMarketplaces.map(m => m.toLowerCase()).includes(marketplace.toLowerCase());
        if (isEnabled) {
          marketplacesWithoutOrders.push(mp);
          setTaskStatus(taskId, "STOPPED", "No orders to cancel", marketplace);
        } else {
        }
      }
    } else {
      // Full task stop - check each marketplace
      const marketplaceData = [
        { name: 'opensea', orders: orders.opensea },
        { name: 'magiceden', orders: orders.magiceden },
        { name: 'blur', orders: orders.blur }
      ];

      for (const { name, orders: mpOrders } of marketplaceData) {
        if (mpOrders.length > 0) {
          marketplacesWithOrders.push(name);
          setTaskStatus(taskId, "CANCELLING", `Cancelling ${mpOrders.length} ${name} orders`, name);
        } else {
          // Only set to STOPPED if the marketplace is selected AND enabled
          const isSelected = task.selectedMarketplaces.map(m => m.toLowerCase()).includes(name);
          const isEnabled = enabledMarketplaces.map(m => m.toLowerCase()).includes(name);

          if (isSelected && isEnabled) {
            marketplacesWithoutOrders.push(name);
            setTaskStatus(taskId, "STOPPED", "No orders to cancel", name);
          } else if (isSelected && !isEnabled) {
          }
        }
      }
    }

    // SECONDARY TIMER CLEANUP: Clear any timers that were created AFTER the initial cleanup
    // This catches the race condition where:
    // 1. cleanupTaskResources() runs at the start of stopTask (clears 0 timers because none exist yet)
    // 2. A completing job calls scheduleNextLoop() which creates a NEW timer
    // 3. Status changes to CANCELLING (we're at this point now)
    // 4. Without this cleanup, that new timer would fire 60s later and transition to STARTED
    // By clearing timers HERE (after CANCELLING status is set), we catch any timers created during
    // the window between cleanupTaskResources() and this point.
    clearTaskTimers(taskId);

    // Count total active orders from database
    let totalBids = 0;
    const marketplaceTotals = {
      opensea: 0,
      magiceden: 0,
      blur: 0
    };

    // Take a snapshot of valid orders at cancellation start (cleans stale entries from Redis set)
    const validCounts = await orderCache.countValidByTaskAndMarketplace(taskId);

    // Force broadcast final state
    publishTaskMetrics();

    if (marketplace) {
      // For marketplace-specific stop, use only that marketplace's validated count
      const mpKey = marketplace.toLowerCase() as 'opensea' | 'magiceden' | 'blur';
      totalBids = validCounts[mpKey] || 0;
    } else {
      // For global stop, sum all validated counts
      totalBids = Object.values(validCounts).reduce((a, b) => a + b, 0);

      // Use validated counts for marketplace totals
      marketplaceTotals.opensea = validCounts.opensea;
      marketplaceTotals.magiceden = validCounts.magiceden;
      marketplaceTotals.blur = validCounts.blur;
    }

    // Check if there are no orders to cancel
    const totalOrderCount = orders.opensea.length + orders.magiceden.length + orders.blur.length;
    if (totalOrderCount === 0) {
      cancellationDebugLogger.log('STATE', 'No orders to cancel - transitioning to STOPPED', {
        totalOrderCount: 0
      });
      cancellationDebugLogger.finish({
        status: 'STOPPED',
        ordersFound: { opensea: 0, magiceden: 0, blur: 0 },
        reason: 'no_orders_to_cancel'
      });

      // Task remains in STOPPED state since no cancellation work is needed
      if (taskStatus[taskId]) {
        // Set marketplace-specific cancellation percentage to 100 since there's nothing to cancel
        if (marketplace) {
          const mpKey = marketplace.toLowerCase() as 'opensea' | 'magiceden' | 'blur';
          if (taskStatus[taskId].marketplaces[mpKey]) {
            taskStatus[taskId].marketplaces[mpKey]!.cancellationPercentage = 100;
          }
        } else {
          // Set all marketplaces cancellation percentage to 100
          const selectedMarketplaces = task.selectedMarketplaces || [];
          for (const mp of selectedMarketplaces) {
            const mpKey = mp.toLowerCase() as 'opensea' | 'magiceden' | 'blur';
            if (taskStatus[taskId].marketplaces[mpKey]) {
              taskStatus[taskId].marketplaces[mpKey]!.cancellationPercentage = 100;
            }
          }
        }
      }

      // Unsubscribe from WebSocket before early return (when task is fully stopped with no orders)
      if (!marketplace) {
        const unsubscribeResult = await unsubscribeFromCollection(task);
        if (!unsubscribeResult.success) {
          errorLog('WebSocket', `Failed to unsubscribe from collection during early exit: taskId=${task._id}`, new Error(unsubscribeResult.error || 'Unknown error'));
        } else if (unsubscribeResult.unsubscribed) {
        } else {
        }
      }

      // FIX: Initialize tracking with 0 even for early exit to prevent RECORD_NO_TRACKING errors
      // When stopTask is called multiple times, stale jobs from the FIRST call may still be
      // completing in BullMQ. Without tracking initialized, those jobs call recordCompletion()
      // and fail with RECORD_NO_TRACKING. Initializing with 0 allows those stale completions
      // to be handled gracefully (they'll just have no effect since total=0).
      cancellationTracker.initializeTracking(taskId, 0, marketplace);

      // Return early - task stays in STOPPED state
      return;
    }

    // Initialize cancellation tracking
    cancellationTracker.initializeTracking(taskId, totalBids, marketplace);

    // For global cancellations, set marketplace-specific totals
    if (!marketplace) {
      cancellationTracker.setMarketplaceTotals(taskId, marketplaceTotals);
    }

    // Set initial cancellation percentage
    if (taskStatus[taskId]) {
      await updateTaskCancellationPercentage(taskId, 0, marketplace);
    }

    // Log cache snapshot before cancellation starts
    const cacheCountBefore = await orderCache.countByTaskAndMarketplace(taskId, marketplace || 'all');

    // Start timeout monitor for stuck cancellations
    const STALL_TIMEOUT = 10 * 60 * 1000; // 10 minutes - allows time for large collections (1000+ orders)
    const cancellationStartTime = Date.now();

    const timeoutCheck = setInterval(async () => {
      if (!marketplace) {
        clearInterval(timeoutCheck);
        return;
      }

      const marketplaceName = marketplace.toLowerCase() as 'opensea' | 'magiceden' | 'blur';

      if (!taskStatus[taskId]?.marketplaces[marketplaceName]) {
        clearInterval(timeoutCheck);
        return;
      }

      const currentStatus = taskStatus[taskId].marketplaces[marketplaceName]!.status;
      const currentPct = taskStatus[taskId].marketplaces[marketplaceName]!.cancellationPercentage;

      // If already STOPPED, cleanup
      if (currentStatus !== 'CANCELLING') {
        clearInterval(timeoutCheck);
        return;
      }

      const elapsed = Date.now() - cancellationStartTime;

      if (elapsed > STALL_TIMEOUT) {
        // TIMEOUT: Force cache cleanup after 10 minutes (allows time for large collections)
        warnLog('StopTaskFlow', `CANCELLATION_TIMEOUT | taskId=${taskId} | marketplace=${marketplaceName} | pct=${currentPct} | elapsed=${elapsed}ms`, { taskId });

        // Check remaining orders
        const remainingOrders = await orderCache.countByTaskAndMarketplace(taskId, marketplaceName);

        if (remainingOrders > 0) {
          warnLog('StopTaskFlow', `TIMEOUT_CACHE_CLEANUP | taskId=${taskId} | marketplace=${marketplaceName} | remaining=${remainingOrders}`, { taskId });

          // FORCE DELETE ALL ORDERS FROM CACHE
          await orderCache.deleteByTaskAndMarketplace(taskId, marketplaceName);

          // Verify cleanup
          const afterCleanup = await orderCache.countByTaskAndMarketplace(taskId, marketplaceName);
        }

        // Force 100% and transition
        await updateTaskCancellationPercentage(taskId, 100, marketplace);
        await validateAndTransitionToStopped(taskId, marketplace, marketplaceName);

        clearInterval(timeoutCheck);
      }
    }, 30000); // Check every 30 seconds

    // Store interval for cleanup on early exit
    if (!global.cancellationTimeouts) {
      global.cancellationTimeouts = new Map<string, NodeJS.Timeout>();
    }
    global.cancellationTimeouts.set(`${taskId}-${marketplace}`, timeoutCheck);

    // Get currency symbol for OpenSea cancellations
    const { currencySymbol } = getCurrencyDetailsFromTask(task, "opensea");

    cancellationDebugLogger.log('STATE', 'Transitioning to CANCELLING', {
      totalOrderCount,
      marketplacesWithOrders,
      marketplacesWithoutOrders
    });

    switch (marketplace?.toLowerCase()) {
      case 'opensea': {
        // Cancel active HTTP requests first
        const httpAbortedCount = await cancelOpenseaRequest(taskId);

        // Now cancel actual orders
        if (orders.opensea.length > 0 && WALLET_PRIVATE_KEY) {
          cancellationDebugLogger.log('CANCEL_QUEUE', 'Queuing OpenSea cancellations', {
            orderCount: orders.opensea.length,
            orderHashes: orders.opensea.slice(0, 5).map((o: any) => o.orderHash)
          });

          // Track cancellation operation
          const cancellationOp: CancellationOperation = {
            taskId,
            marketplace: 'opensea',
            totalJobs: orders.opensea.length,
            completedJobs: 0,
            startTime: Date.now()
          };
          activeCancellationOperations.set(`${taskId}-opensea`, cancellationOp);

          await cancelOpenseaBids(orders.opensea, task.walletId, taskId, currencySymbol);

          // Log cache snapshot after jobs queued
          const cacheCountAfterQueue = await orderCache.countByTaskAndMarketplace(taskId, 'opensea');
        } else if (orders.opensea.length === 0) {

          // Let the cancellation tracker handle percentage updates naturally

          await setMarketplaceCancellationComplete(taskId, marketplace);
          completionHandled = true;
        } else if (!WALLET_PRIVATE_KEY) {
          await setMarketplaceCancellationComplete(taskId, marketplace);
          completionHandled = true;
        }

        const openseaParentJobId = `${taskId}-opensea`.toLowerCase()
        const openseaParentJob = parentJobs.get(openseaParentJobId)
        if (openseaParentJob) {
          parentJobs.delete(openseaParentJobId)
        }
        // Clear processed jobs tracking for OpenSea
        processedJobsPerParent.delete(openseaParentJobId);
        break;
      }
      case 'magiceden': {
        // Cancel active HTTP requests first
        await cancelMagicedenRequest(taskId);

        // Now cancel actual orders
        if (orders.magiceden.length > 0 && WALLET_PRIVATE_KEY) {
          cancellationDebugLogger.log('CANCEL_QUEUE', 'Queuing MagicEden cancellations', {
            orderCount: orders.magiceden.length,
            orderHashes: orders.magiceden.slice(0, 5).map((o: any) => o.orderHash)
          });

          // Track cancellation operation
          const cancellationOp: CancellationOperation = {
            taskId,
            marketplace: 'magiceden',
            totalJobs: orders.magiceden.length,
            completedJobs: 0,
            startTime: Date.now()
          };
          activeCancellationOperations.set(`${taskId}-magiceden`, cancellationOp);

          await cancelMagicedenBids(orders.magiceden, task.walletId, taskId);

          // Log cache snapshot after jobs queued
          const cacheCountAfterQueue = await orderCache.countByTaskAndMarketplace(taskId, 'magiceden');
        } else if (orders.magiceden.length === 0) {

          // Let the cancellation tracker handle percentage updates naturally

          setMarketplaceCancellationComplete(taskId, marketplace);
          completionHandled = true;
        } else if (!WALLET_PRIVATE_KEY) {
          setMarketplaceCancellationComplete(taskId, marketplace);
          completionHandled = true;
        }

        const magicedenParentJobId = `${taskId}-magiceden`.toLowerCase()
        const magicedenParentJob = parentJobs.get(magicedenParentJobId)
        if (magicedenParentJob) {
          parentJobs.delete(magicedenParentJobId)
        }
        // Clear processed jobs tracking for MagicEden
        processedJobsPerParent.delete(magicedenParentJobId);
        break;
      }
      case 'blur': {
        // Cancel active HTTP requests first
        await cancelBlurRequest(taskId);

        // Now cancel actual orders
        if (orders.blur.length > 0 && WALLET_PRIVATE_KEY) {
          cancellationDebugLogger.log('CANCEL_QUEUE', 'Queuing Blur cancellations', {
            orderCount: orders.blur.length,
            orderHashes: orders.blur.slice(0, 5).map((o: any) => o.orderHash)
          });

          // Track cancellation operation
          const cancellationOp: CancellationOperation = {
            taskId,
            marketplace: 'blur',
            totalJobs: orders.blur.length,
            completedJobs: 0,
            startTime: Date.now()
          };
          activeCancellationOperations.set(`${taskId}-blur`, cancellationOp);

          await cancelBlurBids(orders.blur, task.walletId, taskId);

          // Log cache snapshot after jobs queued
          const cacheCountAfterQueue = await orderCache.countByTaskAndMarketplace(taskId, 'blur');
        } else if (orders.blur.length === 0) {

          // Let the cancellation tracker handle percentage updates naturally

          setMarketplaceCancellationComplete(taskId, marketplace);
          completionHandled = true;
        } else if (!WALLET_PRIVATE_KEY) {
          setMarketplaceCancellationComplete(taskId, marketplace);
          completionHandled = true;
        }

        const blurParentJobId = `${taskId}-blur`.toLowerCase()
        const blurParentJob = parentJobs.get(blurParentJobId)
        if (blurParentJob) {
          parentJobs.delete(blurParentJobId)
        }
        // Clear processed jobs tracking for Blur
        processedJobsPerParent.delete(blurParentJobId);
        break;
      }
      default: {
        // Cancel HTTP requests for all marketplaces first
        const httpResults = await Promise.allSettled([
          cancelOpenseaRequest(taskId),
          cancelMagicedenRequest(taskId),
          cancelBlurRequest(taskId)
        ]);

        // Log HTTP cancellation results (all cancel functions now return number)
        const marketplaceNames = ['opensea', 'magiceden', 'blur'];
        const httpAbortCounts = httpResults.map((result, index) => {
          if (result.status === 'fulfilled') {
            return { marketplace: marketplaceNames[index], aborted: result.value };
          }
          return { marketplace: marketplaceNames[index], aborted: 0, error: (result as PromiseRejectedResult).reason?.message };
        });

        httpResults.forEach((result, index) => {
          if (result.status === 'rejected') {
            warnLog('Marketplace', `Failed to cancel ${marketplaceNames[index]} HTTP requests for task ${taskId} | error=${(result as PromiseRejectedResult).reason?.message}`);
          }
        });

        // Now cancel actual orders for all marketplaces
        const totalOrderCount = orders.opensea.length + orders.magiceden.length + orders.blur.length;

        if (totalOrderCount > 0 && WALLET_PRIVATE_KEY) {
          cancellationDebugLogger.log('CANCEL_QUEUE', 'Queuing ALL marketplace cancellations', {
            opensea: orders.opensea.length,
            magiceden: orders.magiceden.length,
            blur: orders.blur.length,
            total: totalOrderCount
          });

          // Track cancellation operation
          const cancellationOp: CancellationOperation = {
            taskId,
            marketplace: undefined,
            totalJobs: totalOrderCount,
            completedJobs: 0,
            startTime: Date.now()
          };
          activeCancellationOperations.set(taskId, cancellationOp);

          // Cancel orders for each marketplace
          const cancellationPromises = [];


          if (orders.opensea.length > 0) {
            cancellationPromises.push(cancelOpenseaBids(orders.opensea, task.walletId, taskId, currencySymbol));
          }

          if (orders.magiceden.length > 0) {
            cancellationPromises.push(cancelMagicedenBids(orders.magiceden, task.walletId, taskId));
          }

          if (orders.blur.length > 0) {
            cancellationPromises.push(cancelBlurBids(orders.blur, task.walletId, taskId));
          }

          await Promise.allSettled(cancellationPromises);

          // Log cache snapshot after jobs queued
          const cacheCountAfterQueue = await orderCache.countByTaskAndMarketplace(taskId, marketplace || 'all');
        } else if (totalOrderCount === 0) {

          // For global cancellation with no orders, let tracker handle it naturally

          await setTaskCancellationComplete(taskId);
        } else if (!WALLET_PRIVATE_KEY) {
          // Let the cancellation tracker handle percentage updates naturally
          await setTaskCancellationComplete(taskId);
        }

        // Delete parent jobs for all marketplaces
        const marketplaces = ['opensea', 'magiceden', 'blur']
        for (const mp of marketplaces) {
          const parentJobId = `${taskId}-${mp}`.toLowerCase()
          const parentJob = parentJobs.get(parentJobId)
          if (parentJob) {
            parentJobs.delete(parentJobId)
          }
          // Clear processed jobs tracking
          processedJobsPerParent.delete(parentJobId);
        }
        break;
      }
    }

    // === PHASE 2 END ===
    const phase2Elapsed = Date.now() - phase2StartTime;

    // === PHASE 3: STATE CLEANUP AND FINALIZATION ===
    const phase3StartTime = Date.now();

    const statusResult = await updateTaskStatus(task, start, marketplace);
    if (!statusResult.success) {
      warnLog('General', `Failed to update task status: taskId=${task._id}, marketplace=${marketplace || 'all'}`);
    }

    // Fix 7: WebSocket unsubscribe moved to PHASE_0 for earlier execution
    // This prevents counterbid handlers from receiving events during cancellation

    const removalResult = await removePendingAndWaitingBids(task, marketplace);
    if (!removalResult.success) {
      errorLog('Queue', `Failed to remove pending and waiting bids during task stop: taskId=${task._id}, marketplace=${marketplace || 'all'}, jobsRemoved=${removalResult.jobsRemoved}`, new Error(`${removalResult.errors.length} errors occurred`));
    } else {
    }

    if (!task?.selectedMarketplaces.length || !task.running) {
      if (taskStatus[taskId]) {
        // Clear marketplace-specific waiting times
        Object.keys(taskStatus[taskId].marketplaces).forEach(mp => {
          if (taskStatus[taskId].marketplaces[mp as keyof typeof taskStatus[string]['marketplaces']]) {
            delete taskStatus[taskId].marketplaces[mp as keyof typeof taskStatus[string]['marketplaces']]!.nextLoopTime;
            taskStatus[taskId].marketplaces[mp as keyof typeof taskStatus[string]['marketplaces']]!.waitingTime = 0;
          }
        });
      }

      // Clean up all task resources
      cleanupTaskResources(taskId);
    }

    // Clear countdown timer regardless of order count
    if (!task?.running) {
      const countdownTimer = taskCountdownTimers.get(taskId);
      if (countdownTimer) {
        clearInterval(countdownTimer);
        taskCountdownTimers.delete(taskId);
      }
    }

    // Only attempt to cancel bids if we have a valid wallet private key
    let cancellationResult: {
      success: boolean;
      results: {
        opensea?: { success: boolean; ordersFound: number; error?: string };
        magiceden?: { success: boolean; ordersFound: number; error?: string };
        blur?: { success: boolean; ordersFound: number; error?: string };
      };
      totalOrdersFound: number;
      errors: string[];
      duration: number;
    } = { success: true, totalOrdersFound: 0, errors: [], duration: 0, results: {} };

    if (WALLET_PRIVATE_KEY) {
      cancellationResult = await cancelAllRelatedBids(task, marketplace);
    } else {
      warnLog('Marketplace', `Skipping bid cancellation - no valid wallet key: taskId=${task._id}, marketplace=${marketplace || 'all'}`);
    }

    if (!cancellationResult.success) {
      errorLog('Marketplace', `Failed to cancel related bids during task stop: taskId=${task._id}, marketplace=${marketplace || 'all'}, ordersFound=${cancellationResult.totalOrdersFound}`, new Error(`${cancellationResult.errors.length} errors occurred`));
    } else if (cancellationResult.totalOrdersFound > 0) {
    }

    // === ENHANCED STOPTASK: FINAL COMPLETION ===
    completedOperations += 1;
    updateCancellationProgress(taskId, completedOperations, totalOperations);

    cancellationDebugLogger.log('CLEANUP', 'Cleanup resources complete', {
      completedOperations,
      totalOperations
    });

    if (!completionHandled) {
      if (marketplace) {
        await setMarketplaceCancellationComplete(taskId, marketplace);
      } else {
        await setTaskCancellationComplete(taskId);
      }
    } else {
    }

    // === PHASE 3 END ===
    const phase3Elapsed = Date.now() - phase3StartTime;

    // Final StopTaskFlow logging - capture tracker state before exit
    const limiterStateFinal = getLimiterQueueDetails(RATE_LIMIT);
    const orderCountAfter = orders.opensea.length + orders.magiceden.length + orders.blur.length;

    // Capture final cancellation tracker state
    const finalTaskTracking = cancellationTracker.getStatus(taskId);
    const finalOpenseaTracking = cancellationTracker.getStatus(taskId, 'opensea');
    const finalMagicedenTracking = cancellationTracker.getStatus(taskId, 'magiceden');
    const finalBlurTracking = cancellationTracker.getStatus(taskId, 'blur');
    const finalAggPct = cancellationTracker.getAggregatedPercentage(taskId);



    cancellationDebugLogger.log('COMPLETE', 'stopTask finished successfully', {
      finalStatus: marketplace ? 'MARKETPLACE_STOPPED' : 'TASK_STOPPED'
    });
    cancellationDebugLogger.finish({
      status: 'SUCCESS',
      ordersQueued: {
        opensea: orders.opensea.length,
        magiceden: orders.magiceden.length,
        blur: orders.blur.length
      },
      orderSources,
      cancellationResults: cancellationDebugLogger.getJobCounts(),
      completionHandled
    });


    // Clean up marketplace-specific resources for scheduleNextLoop
    if (marketplace) {
      const marketplaceLower = marketplace.toLowerCase();

      // Clear marketplace-specific countdown timer
      const timerKey = `${taskId}-${marketplaceLower}`;
      const countdownTimer = taskCountdownTimers.get(timerKey);
      if (countdownTimer) {
        clearInterval(countdownTimer);
        taskCountdownTimers.delete(timerKey);
      }

      // Clear marketplace-specific timeout
      const timeout = taskTimeouts.get(timerKey);
      if (timeout) {
        clearTimeout(timeout);
        taskTimeouts.delete(timerKey);
      }

      // Reset marketplace loop counter
      if (loopCounterStats[taskId] && loopCounterStats[taskId][marketplaceLower]) {
        const previousCount = loopCounterStats[taskId][marketplaceLower];
        delete loopCounterStats[taskId][marketplaceLower];

        // If all marketplaces are removed, clean up the task entry
        if (Object.keys(loopCounterStats[taskId]).length === 0) {
          delete loopCounterStats[taskId];
        }
      }

      // Clear marketplace from preprocessing if exists
      const preprocessing = taskPreprocessing.get(taskId);
      if (preprocessing) {
        if (marketplaceLower === 'opensea' && preprocessing.opensea) {
          preprocessing.opensea = {
            enabled: false,
            tokens: [],
            traits: [],
            jobCount: 0,
            skipReason: 'Marketplace stopped'
          };
        } else if (marketplaceLower === 'magiceden' && preprocessing.magiceden) {
          preprocessing.magiceden = {
            enabled: false,
            tokens: [],
            traits: [],
            jobCount: 0,
            skipReason: 'Marketplace stopped'
          };
        } else if (marketplaceLower === 'blur' && preprocessing.blur) {
          preprocessing.blur = {
            enabled: false,
            tokens: [],
            traits: [],
            jobCount: 0,
            skipReason: 'Marketplace stopped'
          };
        }
      }
    }

  } catch (error: any) {
    const limiterStateError = getLimiterQueueDetails(RATE_LIMIT);

    errorLog('StopTask', `Error in enhanced stopTask for ${taskId}`, {
      error: error.message,
      command: error.command,
      timestamp: new Date().toISOString()
    });

    cancellationDebugLogger.log('ERROR', 'stopTask failed with error', {
      error: error.message,
      stack: error.stack?.split('\n').slice(0, 3).join(' | ')
    });
    cancellationDebugLogger.finish({
      status: 'ERROR',
      error: error.message,
      cancellationResults: cancellationDebugLogger.getJobCounts()
    });

    // Force complete cancellation on error and leave task blacklisted

    if (marketplace) {
      await setMarketplaceCancellationComplete(taskId, marketplace);
    } else {
      await setTaskCancellationComplete(taskId);
    }

    // Log that task remains blacklisted due to error

    // Clean up marketplace-specific resources even on error
    if (marketplace) {
      try {
        const marketplaceLower = marketplace.toLowerCase();

        // Clear marketplace-specific countdown timer
        const timerKey = `${taskId}-${marketplaceLower}`;
        const countdownTimer = taskCountdownTimers.get(timerKey);
        if (countdownTimer) {
          clearInterval(countdownTimer);
          taskCountdownTimers.delete(timerKey);
        }

        // Clear marketplace-specific timeout
        const timeout = taskTimeouts.get(timerKey);
        if (timeout) {
          clearTimeout(timeout);
          taskTimeouts.delete(timerKey);
        }

        // Reset marketplace loop counter
        if (loopCounterStats[taskId] && loopCounterStats[taskId][marketplaceLower]) {
          delete loopCounterStats[taskId][marketplaceLower];
          if (Object.keys(loopCounterStats[taskId]).length === 0) {
            delete loopCounterStats[taskId];
          }
        }

        // Clear marketplace from preprocessing
        const preprocessing = taskPreprocessing.get(taskId);
        if (preprocessing) {
          if (marketplaceLower === 'opensea' && preprocessing.opensea) {
            preprocessing.opensea = {
              enabled: false,
              tokens: [],
              traits: [],
              jobCount: 0,
              skipReason: 'Marketplace stopped due to error'
            };
          } else if (marketplaceLower === 'magiceden' && preprocessing.magiceden) {
            preprocessing.magiceden = {
              enabled: false,
              tokens: [],
              traits: [],
              jobCount: 0,
              skipReason: 'Marketplace stopped due to error'
            };
          } else if (marketplaceLower === 'blur' && preprocessing.blur) {
            preprocessing.blur = {
              enabled: false,
              tokens: [],
              traits: [],
              jobCount: 0,
              skipReason: 'Marketplace stopped due to error'
            };
          }
        }
      } catch (cleanupError) {
        errorLog('StopTask', `Error during marketplace cleanup: ${cleanupError}`);
      }
    }
  } finally {
    // Note: cleanup() moved to updateTaskCancellationPercentage() when percentage reaches 100%
    // This fixes race condition where tracking data was deleted before async BullMQ jobs completed

    // FIX: Secondary timer cleanup at the very end of stopTask
    // This catches any timers that were created AFTER cleanupTaskResources() ran at the start of PHASE_0
    // (e.g., if a job completed during cancellation and triggered scheduleNextLoop)
    // Note: clearTaskTimers() handles both main timeout and marketplace-specific timeouts
    // Note: clearTaskTimers() logs its own debug info internally (CLEANUP_TIMERS log)
    clearTaskTimers(taskId);

    // Cleanup broadcast buffers and delta tracking for this task
    broadcastManager.cleanupTask(taskId);

    // ✅ Clear cancellation flag to prevent unbounded growth
    tasksBeingCancelled.delete(taskId);
  }
}

async function deleteTask(task: ITask) {
  try {
    await stopTask(task, false);
    const taskIndex = currentTasks.findIndex(t => t._id === task._id);
    if (taskIndex !== -1) {
      const taskToRemove = currentTasks[taskIndex];
      currentTasks.splice(taskIndex, 1);
      // Use O(1) index removal instead of O(n) full rebuild
      removeTaskFromIndexes(taskToRemove);
    }
    const taskId = task._id.toString();
    activeTasks.delete(task._id);

    // Clean up failed task retry queues
    failedTaskStartQueue.delete(taskId);
    failedTaskStopQueue.delete(taskId);

    // Clean up taskStatus
    delete taskStatus[taskId];

    // Clear countdown timer
    const countdownTimer = taskCountdownTimers.get(taskId);
    if (countdownTimer) {
      clearInterval(countdownTimer);
      taskCountdownTimers.delete(taskId);
    }

    // Unsubscribe from WebSocket for this task's collection
    if (wsManager && task.contract?.slug && task.contract?.contractAddress) {
      await wsManager.unsubscribe(task.contract.slug, task.contract.contractAddress);
    }

    // Clean up all task resources
    cleanupTaskResources(taskId);
  } catch (error) {
    errorLog('Task', 'Error deleting task', error);
  }
}


const BATCH_SIZE = 1000; // Process jobs in batches
const JOB_REMOVAL_TIMEOUT = 60000; // 60 seconds
const DIRECT_REMOVAL_TIMEOUT = 5000; // 5 seconds per job

/**
 * Removes pending and waiting bids for a task, with immediate active bid cancellation
 * @param task - The bidding task to remove bids for
 * @param marketplace - Optional marketplace filter (opensea, magiceden, blur)
 * @returns Promise with detailed operation results
 */
async function removePendingAndWaitingBids(
  task: ITask,
  marketplace?: string
): Promise<{
  success: boolean;
  jobsRemoved: number;
  bidsCancelled: number;
  errors: string[];
  duration: number;
}> {
  const startTime = Date.now();
  const taskId = task?._id?.toString();
  const contractSlug = task?.contract?.slug;
  const errors: string[] = [];
  let jobsRemoved = 0;
  let bidsCancelled = 0;

  try {
    // Input validation
    if (!task) {
      const error = 'Invalid task provided to removePendingAndWaitingBids';
      warnLog('Queue', error);
      return { success: false, jobsRemoved: 0, bidsCancelled: 0, errors: [error], duration: 0 };
    }

    if (!taskId) {
      const error = 'Task missing _id property';
      warnLog('Queue', error);
      return { success: false, jobsRemoved: 0, bidsCancelled: 0, errors: [error], duration: 0 };
    }

    if (!contractSlug) {
      const error = 'Task missing contract.slug property';
      warnLog('Queue', error);
      return { success: false, jobsRemoved: 0, bidsCancelled: 0, errors: [error], duration: 0 };
    }

    // Validate marketplace parameter if provided
    if (marketplace && !['opensea', 'magiceden', 'blur'].includes(marketplace.toLowerCase())) {
      const error = 'Invalid marketplace parameter';
      warnLog('Queue', `${error}: ${marketplace}`);
      return { success: false, jobsRemoved: 0, bidsCancelled: 0, errors: [error], duration: 0 };
    }


    // STEP 1: Immediately cancel active bids for fast response
    try {
      const wallet = await walletCache.getWalletById(task.walletId);
      if (wallet?.encryptedPrivateKey) {
        const privateKey = decryptPrivateKey(wallet.encryptedPrivateKey);

        // Cancel active bids in parallel for immediate response
        const bidCancellationPromises = [];

        if (!marketplace || marketplace.toLowerCase() === 'opensea') {
          bidCancellationPromises.push(
            fetchAndCancelOpenseaBids(task)
              .then(() => { bidsCancelled++; })
              .catch(error => {
                errorLog('Cancellation', `OpenSea bid cancellation failed | taskId=${task._id}`, error);
                errors.push(`OpenSea bid cancellation failed: ${error.message}`);
              })
          );
        }

        if (!marketplace || marketplace.toLowerCase() === 'magiceden') {
          bidCancellationPromises.push(
            fetchAndCancelMagicedenBids(task)
              .then(() => { bidsCancelled++; })
              .catch(error => {
                errorLog('Cancellation', `MagicEden bid cancellation failed | taskId=${task._id}`, error);
                errors.push(`MagicEden bid cancellation failed: ${error.message}`);
              })
          );
        }

        if (!marketplace || marketplace.toLowerCase() === 'blur') {
          bidCancellationPromises.push(
            fetchAndCancelBlurBids(task)
              .then(() => { bidsCancelled++; })
              .catch(error => {
                errorLog('Cancellation', `Blur bid cancellation failed | taskId=${task._id}`, error);
                errors.push(`Blur bid cancellation failed: ${error.message}`);
              })
          );
        }

        // Execute bid cancellations in parallel with timeout
        const bidCancellationTimeout = Promise.race([
          Promise.allSettled(bidCancellationPromises),
          new Promise((_, reject) => {
            setTimeout(() => reject(new Error('Bid cancellation timeout')), 10000);
          })
        ]);

        await bidCancellationTimeout;

      }
    } catch (error) {
      const errorMsg = `Active bid cancellation failed: ${error instanceof Error ? error.message : String(error)}`;
      errors.push(errorMsg);
      warnLog('Queue', `${errorMsg} for taskId=${taskId}`);
    }

    // STEP 2 & 3: Iterate through queue jobs and filter by taskId
    // This approach works for ALL jobs (with or without explicit jobId)
    try {

      // Determine which queues to check based on marketplace
      const queuesToCheck: { q: typeof queue, name: string }[] = [];
      // All jobs (including Blur) are in main queue
      queuesToCheck.push({ q: queue, name: 'main' });

      for (const { q, name: queueName } of queuesToCheck) {
        // Get all jobs in waiting, delayed, and active states
        const jobStates = ['waiting', 'delayed', 'active'] as const;

        for (const state of jobStates) {
          const jobs = await q.getJobs([state], 0, -1);

          // Filter jobs by taskId in job data (works for all jobs including those without explicit jobId)
          const taskJobs = jobs.filter(job => {
            if (!job) return false;
            const jobTaskId = job.data?._id?.toString() || job.data?.taskId?.toString();
            return jobTaskId === taskId;
          });


          // Remove jobs in parallel batches
          const chunkSize = 50;
          for (let i = 0; i < taskJobs.length; i += chunkSize) {
            const chunk = taskJobs.slice(i, i + chunkSize);
            const chunkPromises = chunk.map(async (job) => {
              try {
                await Promise.race([
                  job.remove(),
                  new Promise((_, reject) => {
                    setTimeout(() => reject(new Error('Job removal timeout')), DIRECT_REMOVAL_TIMEOUT);
                  })
                ]);
                jobsRemoved++;
              } catch (error) {
                // Job might be processing or already removed - not necessarily an error
              }
            });

            try {
              await Promise.race([
                Promise.allSettled(chunkPromises),
                new Promise((_, reject) => {
                  setTimeout(() => reject(new Error('Chunk processing timeout')), JOB_REMOVAL_TIMEOUT);
                })
              ]);
            } catch (error) {
              const errorMsg = `Chunk processing failed: ${error instanceof Error ? error.message : String(error)}`;
              errors.push(errorMsg);
              warnLog('Queue', errorMsg);
            }
          }
        }
      }

    } catch (error) {
      const errorMsg = `Queue-based job removal failed: ${error instanceof Error ? error.message : String(error)}`;
      errors.push(errorMsg);
      errorLog('Queue', errorMsg, error as Error);
    }

    const duration = Date.now() - startTime;
    const success = errors.length === 0;


    return { success, jobsRemoved, bidsCancelled, errors, duration };
  } catch (error) {
    const duration = Date.now() - startTime;
    const errorMsg = error instanceof Error ? error.message : String(error);

    errorLog('Queue', `Failed to remove pending and waiting bids: taskId=${taskId}`, error as Error);

    return {
      success: false,
      jobsRemoved,
      bidsCancelled,
      errors: [...errors, errorMsg],
      duration
    };
  }
}


/**
 * @deprecated This function is no longer used since 2026-01-05.
 * Job removal now uses queue iteration with job.data._id matching instead of job ID lookup.
 * Kept for reference only - scheduled bid jobs are created without explicit jobId,
 * so the generated IDs never existed in BullMQ.
 */

function getJobIds(marketplace: string, task: ITask) {
  const jobIds: string[] = []
  const selectedTraits = transformNewTask(task.selectedTraits?.[marketplace.toLowerCase()])
  const isTraitBid = task.bidType === "collection" && selectedTraits && Object.keys(selectedTraits).length > 0

  const tokenBid = task.bidType === "token" || task.bidType === "rarity"
  const traits = transformOpenseaTraits(selectedTraits);

  // Match createJobKey() format: bid-{taskId}-{uniqueKey}-{slug}-{walletAddress}
  const slug = removeChar(task.contract?.slug || '', '-')
  const walletAddress = task.wallet || ''
  const mp = marketplace.toLowerCase()

  if (isTraitBid) {
    traits.map((trait) => {
      // createJobKey format for traits: {marketplace}-{type}_{value}
      const type = removeChar(trait.type, '-')
      const value = removeChar(trait.value, '-')
      const uniqueKey = `${mp}-${type}_${value}`
      jobIds.push(`bid-${task._id}-${uniqueKey}-${slug}-${walletAddress}`)
    })
  } else if (tokenBid) {
    task.tokenIds?.map((tokenId) => {
      // createJobKey format for tokens: {marketplace}-token_{tokenId}
      const uniqueKey = `${mp}-token_${tokenId}`
      jobIds.push(`bid-${task._id}-${uniqueKey}-${slug}-${walletAddress}`)
    })
  } else {
    // createJobKey format for collection: {marketplace}-collection
    const uniqueKey = `${mp}-collection`
    jobIds.push(`bid-${task._id}-${uniqueKey}-${slug}-${walletAddress}`)
  }
  return jobIds
}

/**
 * Updates task status and manages warning bids initialization
 * @param task - The bidding task to update
 * @param running - Whether the task is starting (true) or stopping (false)
 * @param marketplace - Optional marketplace filter (opensea, magiceden, blur)
 * @returns Promise with operation result
 */
async function updateTaskStatus(
  task: ITask,
  running: boolean,
  marketplace?: string
): Promise<{ success: boolean; updated: boolean; error?: string }> {
  try {
    // Input validation
    if (!task) {
      warnLog('General', `Invalid task provided to updateTaskStatus: running=${running}, marketplace=${marketplace}`);
      return { success: false, updated: false, error: 'Invalid task provided' };
    }

    if (!task._id) {
      warnLog('General', `Task missing _id property: running=${running}, marketplace=${marketplace}`);
      return { success: false, updated: false, error: 'Task missing _id property' };
    }

    if (!task.contract?.slug) {
      warnLog('General', `Task ${task._id} missing contract.slug property: running=${running}, marketplace=${marketplace}`);
      return { success: false, updated: false, error: 'Task missing contract.slug property' };
    }

    if (typeof running !== 'boolean') {
      warnLog('General', `Invalid running parameter type for task ${task._id}: type=${typeof running}, marketplace=${marketplace}`);
      return { success: false, updated: false, error: 'Invalid running parameter type' };
    }

    // Validate marketplace parameter if provided
    if (marketplace && !['opensea', 'magiceden', 'blur'].includes(marketplace.toLowerCase())) {
      warnLog('General', `Invalid marketplace parameter for task ${task._id}: marketplace=${marketplace}`);
      return { success: false, updated: false, error: 'Invalid marketplace parameter' };
    }

    const taskId = task._id.toString();
    let taskUpdated = false;

    // Initialize warning bids if not already present
    if (!warningBids[taskId]) {
      warningBids[taskId] = {
        opensea: false,
        magiceden: false,
        blur: false
      };
    }

    // Update task status in currentTasks array
    const taskIndex = currentTasks.findIndex(t => t._id === taskId);
    if (taskIndex !== -1) {
      // For marketplace-specific operations, we still want to update the overall task status
      // unless it's a partial update that shouldn't affect the global running state
      if (marketplace) {
        // Log marketplace-specific status change but don't update global running state
      } else {
        // Update global task running state
        const previousRunning = currentTasks[taskIndex].running;
        currentTasks[taskIndex].running = running;
        taskUpdated = true;

      }
    } else {
      warnLog('General', `Task ${taskId} not found in currentTasks array (${task.contract.slug}), running=${running}, marketplace=${marketplace}, currentTasksCount=${currentTasks.length}`);
    }

    // Enhanced logging with structured data instead of console.log
    const statusMessage = running ? 'Started' : 'Stopped';
    // Enhanced logging with structured data instead of console.log


    return { success: true, updated: taskUpdated };
  } catch (error) {
    errorLog('General', `Failed to update task status: ${task?._id} (${task?.contract?.slug}), running=${running}, marketplace=${marketplace}`, error instanceof Error ? error : new Error(String(error)));
    return {
      success: false,
      updated: false,
      error: error instanceof Error ? error.message : String(error)
    };
  }
}

/**
 * Fetches and cancels OpenSea bids for a specific task
 * @param task - The bidding task to cancel bids for
 * @returns Promise with operation results
 */
async function fetchAndCancelOpenseaBids(
  task: ITask
): Promise<{ success: boolean; ordersFound: number; error?: string }> {
  const taskId = task?._id?.toString();
  const contractSlug = task?.contract?.slug;
  const startTime = Date.now();


  try {
    // Input validation
    if (!task || !taskId || !contractSlug) {
      const error = 'Invalid task provided to fetchAndCancelOpenseaBids';
      warnLog('StopCancelBatch', `OPENSEA_CANCEL_INVALID | taskId=${taskId} | error=${error}`, { taskId, marketplace: 'opensea' });
      return { success: false, ordersFound: 0, error };
    }

    // Fetch wallet and decrypt private key
    const walletFetchStart = Date.now();
    const wallet = await walletCache.getWalletById(task.walletId);
    if (!wallet?.encryptedPrivateKey) {
      const error = 'Wallet not found or missing private key';
      warnLog('StopCancelBatch', `OPENSEA_CANCEL_NO_WALLET | taskId=${taskId} | walletId=${task.walletId} | error=${error}`, { taskId, marketplace: 'opensea' });
      return { success: false, ordersFound: 0, error };
    }
    const WALLET_PRIVATE_KEY = decryptPrivateKey(wallet.encryptedPrivateKey);


    // Get currency symbol from task config
    const { currencySymbol } = getCurrencyDetailsFromTask(task, "magiceden");

    // Fetch all orders from Redis (source of truth - TTL handles expiry automatically)
    const BATCH_SIZE = 1000; // For processing cancellation API calls in batches
    let totalOrdersProcessed = 0;
    let totalBatchesProcessed = 0;
    let totalQueuedForCancellation = 0;


    try {
      // Fetch all orders from Redis at once
      const queryStart = Date.now();
      const allOpenseaBids = await orderCache.getByTaskAndMarketplace(taskId, OPENSEA.toLowerCase());
      const queryMs = Date.now() - queryStart;


      if (allOpenseaBids.length === 0) {
        const elapsedMs = Date.now() - startTime;
        return { success: true, ordersFound: 0 };
      }

      totalOrdersProcessed = allOpenseaBids.length;

      // Process orders in batches for cancellation API calls
      for (let i = 0; i < allOpenseaBids.length; i += BATCH_SIZE) {
        const batch = allOpenseaBids.slice(i, i + BATCH_SIZE);
        const batchNumber = Math.floor(i / BATCH_SIZE) + 1;
        totalBatchesProcessed++;


        // Debug log: Compare task.walletId vs WALLET_PRIVATE_KEY to identify the bug

        // Queue cancellations for this batch - use task.walletId (MongoDB ObjectId), NOT WALLET_PRIVATE_KEY
        const queueStart = Date.now();
        await cancelOpenseaBids(batch, task.walletId, taskId, currencySymbol);
        totalQueuedForCancellation += batch.length;

      }
    } catch (error) {
      errorLog('StopCancelBatch', `OPENSEA_FETCH_ERROR | taskId=${taskId} | error=${error instanceof Error ? error.message : String(error)}`, { taskId, marketplace: 'opensea' });
      errorLog('API', `[OpenSea] Failed to fetch orders for task ${taskId} (${contractSlug})`, error instanceof Error ? error : new Error(String(error)));
    }

    const elapsedMs = Date.now() - startTime;

    if (totalOrdersProcessed > 0) {
    }

    return { success: true, ordersFound: totalOrdersProcessed };
  } catch (error) {
    const elapsedMs = Date.now() - startTime;
    const errorMsg = error instanceof Error ? error.message : String(error);

    errorLog('StopCancelBatch', `OPENSEA_CANCEL_FAILED | taskId=${taskId} | slug=${contractSlug} | elapsedMs=${elapsedMs} | error=${errorMsg}`, { taskId, marketplace: 'opensea' });
    errorLog('API', `[OpenSea] Failed to fetch and cancel bids for task ${taskId} (${contractSlug})`, error instanceof Error ? error : new Error(String(error)));

    return { success: false, ordersFound: 0, error: errorMsg };
  }
}

/**
 * Fetches and cancels Blur bids for a specific task
 * @param task - The bidding task to cancel bids for
 * @returns Promise with operation results
 */
async function fetchAndCancelBlurBids(
  task: ITask
): Promise<{ success: boolean; ordersFound: number; error?: string }> {
  const taskId = task?._id?.toString();
  const contractSlug = task?.contract?.slug;
  const startTime = Date.now();


  try {
    // Input validation
    if (!task || !taskId || !contractSlug) {
      const error = 'Invalid task provided to fetchAndCancelBlurBids';
      warnLog('StopCancelBatch', `BLUR_CANCEL_INVALID | taskId=${taskId} | error=${error}`, { taskId, marketplace: 'blur' });
      return { success: false, ordersFound: 0, error };
    }

    // Fetch wallet and decrypt private key
    const walletFetchStart = Date.now();
    const wallet = await walletCache.getWalletById(task.walletId);
    if (!wallet?.encryptedPrivateKey) {
      const error = 'Wallet not found or missing private key';
      warnLog('StopCancelBatch', `BLUR_CANCEL_NO_WALLET | taskId=${taskId} | walletId=${task.walletId} | error=${error}`, { taskId, marketplace: 'blur' });
      return { success: false, ordersFound: 0, error };
    }
    const WALLET_PRIVATE_KEY = decryptPrivateKey(wallet.encryptedPrivateKey);


    // Fetch all orders from Redis (source of truth - TTL handles expiry automatically)
    const BATCH_SIZE = 1000; // For processing cancellation API calls in batches
    let totalOrdersProcessed = 0;
    let totalBatchesProcessed = 0;
    let totalQueuedForCancellation = 0;


    try {
      // Fetch all orders from Redis at once
      const queryStart = Date.now();
      const allBlurBids = await orderCache.getByTaskAndMarketplace(taskId, BLUR.toLowerCase());
      const queryMs = Date.now() - queryStart;


      if (allBlurBids.length === 0) {
        const elapsedMs = Date.now() - startTime;
        return { success: true, ordersFound: 0 };
      }

      totalOrdersProcessed = allBlurBids.length;

      // Process orders in batches for cancellation API calls
      for (let i = 0; i < allBlurBids.length; i += BATCH_SIZE) {
        const batch = allBlurBids.slice(i, i + BATCH_SIZE);
        const batchNumber = Math.floor(i / BATCH_SIZE) + 1;
        totalBatchesProcessed++;

        // Queue cancellations for this batch
        const queueStart = Date.now();
        await cancelBlurBids(batch, task.walletId, taskId);
        totalQueuedForCancellation += batch.length;

      }
    } catch (error) {
      errorLog('StopCancelBatch', `BLUR_FETCH_ERROR | taskId=${taskId} | error=${error instanceof Error ? error.message : String(error)}`, { taskId, marketplace: 'blur' });
    }

    // Completion logging
    const elapsedMs = Date.now() - startTime;
    if (totalOrdersProcessed > 0) {
    }

    return { success: true, ordersFound: totalOrdersProcessed };
  } catch (error) {
    const errorMsg = error instanceof Error ? error.message : String(error);
    const elapsedMs = Date.now() - startTime;
    errorLog('StopCancelBatch', `BLUR_CANCEL_FAILED | taskId=${taskId} | slug=${contractSlug} | error=${errorMsg} | elapsedMs=${elapsedMs}`, { taskId, marketplace: 'blur' });
    errorLog('API', `[Blur] Failed to fetch and cancel bids for task ${taskId} (${contractSlug})`, error instanceof Error ? error : new Error(String(error)));

    return { success: false, ordersFound: 0, error: errorMsg };
  }
}

/**
 * Fetches and cancels MagicEden bids for a specific task
 * @param task - The bidding task to cancel bids for
 * @returns Promise with operation results
 */
async function fetchAndCancelMagicedenBids(
  task: ITask
): Promise<{ success: boolean; ordersFound: number; error?: string }> {
  const taskId = task?._id?.toString();
  const contractSlug = task?.contract?.slug;
  const startTime = Date.now();


  try {
    // Input validation
    if (!task || !taskId || !contractSlug) {
      const error = 'Invalid task provided to fetchAndCancelMagicedenBids';
      warnLog('StopCancelBatch', `MAGICEDEN_CANCEL_INVALID | taskId=${taskId} | error=${error}`, { taskId, marketplace: 'magiceden' });
      return { success: false, ordersFound: 0, error };
    }

    // Fetch wallet and decrypt private key
    const walletFetchStart = Date.now();
    const wallet = await walletCache.getWalletById(task.walletId);
    if (!wallet?.encryptedPrivateKey) {
      const error = 'Wallet not found or missing private key';
      warnLog('StopCancelBatch', `MAGICEDEN_CANCEL_NO_WALLET | taskId=${taskId} | walletId=${task.walletId} | error=${error}`, { taskId, marketplace: 'magiceden' });
      return { success: false, ordersFound: 0, error };
    }

    // Fetch all orders from Redis (source of truth - TTL handles expiry automatically)
    const BATCH_SIZE = 1000; // For processing cancellation API calls in batches
    let totalOrdersProcessed = 0;
    let totalBatchesProcessed = 0;
    let totalQueuedForCancellation = 0;


    try {
      // Fetch all orders from Redis at once
      const queryStart = Date.now();
      const allMagicedenBids = await orderCache.getByTaskAndMarketplace(taskId, MAGICEDEN.toLowerCase());
      const queryMs = Date.now() - queryStart;


      if (allMagicedenBids.length === 0) {
        const elapsedMs = Date.now() - startTime;
        return { success: true, ordersFound: 0 };
      }

      totalOrdersProcessed = allMagicedenBids.length;

      // Set up parent job tracking for MagicEden (batches in groups of 500)
      const parentJobId = `${taskId}-CANCEL_MAGICEDEN_BID`;
      const totalBatchJobCount = Math.ceil(allMagicedenBids.length / 500);
      parentJobs.set(parentJobId, {
        parentId: parentJobId,
        totalJobs: totalBatchJobCount,
        completedJobs: 0,
        failedJobs: 0,
        startTime: Date.now(),
        taskId: taskId,
        jobName: CANCEL_MAGICEDEN_BID,
        loopNumber: 0  // Cancellation jobs don't have loop numbers
      });

      // Process orders in batches for cancellation API calls
      for (let i = 0; i < allMagicedenBids.length; i += BATCH_SIZE) {
        const batch = allMagicedenBids.slice(i, i + BATCH_SIZE);
        const batchNumber = Math.floor(i / BATCH_SIZE) + 1;
        totalBatchesProcessed++;
        const batchJobCount = Math.ceil(batch.length / 500);

        // Queue cancellations for this batch
        const queueStart = Date.now();
        await cancelMagicedenBids(batch, task.walletId, taskId);
        totalQueuedForCancellation += batch.length;

      }
    } catch (error) {
      errorLog('StopCancelBatch', `MAGICEDEN_FETCH_ERROR | taskId=${taskId} | error=${error instanceof Error ? error.message : String(error)}`, { taskId, marketplace: 'magiceden' });
    }

    // Completion logging
    const elapsedMs = Date.now() - startTime;
    if (totalOrdersProcessed > 0) {
    }

    return { success: true, ordersFound: totalOrdersProcessed };
  } catch (error) {
    const errorMsg = error instanceof Error ? error.message : String(error);
    const elapsedMs = Date.now() - startTime;
    errorLog('StopCancelBatch', `MAGICEDEN_CANCEL_FAILED | taskId=${taskId} | slug=${contractSlug} | error=${errorMsg} | elapsedMs=${elapsedMs}`, { taskId, marketplace: 'magiceden' });
    errorLog('API', `[MagicEden] Failed to fetch and cancel bids for task ${taskId} (${contractSlug})`, error instanceof Error ? error : new Error(String(error)));

    return { success: false, ordersFound: 0, error: errorMsg };
  }
}


/**
 * Cancels all related bids for a task across specified marketplaces
 * @param task - The bidding task to cancel bids for
 * @param WALLET_PRIVATE_KEY - Private key for wallet authentication
 * @param marketplace - Optional marketplace filter (opensea, magiceden, blur)
 * @param attempt - Retry attempt number for tracking
 * @returns Promise with detailed operation results
 */
async function cancelAllRelatedBids(
  task: ITask,
  marketplace?: string,
  attempt: number = 1
): Promise<{
  success: boolean;
  results: {
    opensea?: { success: boolean; ordersFound: number; error?: string };
    magiceden?: { success: boolean; ordersFound: number; error?: string };
    blur?: { success: boolean; ordersFound: number; error?: string };
  };
  totalOrdersFound: number;
  errors: string[];
  duration: number;
}> {
  const startTime = Date.now();
  const taskId = task?._id?.toString();
  const contractSlug = task?.contract?.slug;
  const errors: string[] = [];
  const results: any = {};
  let totalOrdersFound = 0;

  try {
    // Input validation
    if (!task) {
      const error = 'Invalid task provided to cancelAllRelatedBids';
      warnLog('API', `${error}, marketplace=${marketplace}, attempt=${attempt}`);
      return { success: false, results: {}, totalOrdersFound: 0, errors: [error], duration: 0 };
    }

    if (!taskId) {
      const error = 'Task missing _id property';
      warnLog('API', `${error}, marketplace=${marketplace}, attempt=${attempt}`);
      return { success: false, results: {}, totalOrdersFound: 0, errors: [error], duration: 0 };
    }

    if (!contractSlug) {
      const error = 'Task missing contract.slug property';
      warnLog('API', `${error}, taskId=${taskId}, marketplace=${marketplace}, attempt=${attempt}`);
      return { success: false, results: {}, totalOrdersFound: 0, errors: [error], duration: 0 };
    }

    // Validate marketplace parameter if provided
    if (marketplace && !['opensea', 'magiceden', 'blur'].includes(marketplace.toLowerCase())) {
      const error = 'Invalid marketplace parameter';
      warnLog('API', `${error}: ${marketplace} for task ${taskId} (${contractSlug}), attempt=${attempt}`);
      return { success: false, results: {}, totalOrdersFound: 0, errors: [error], duration: 0 };
    }


    // Process marketplace-specific cancellation
    if (marketplace?.toLowerCase()) {
      const parentJobId = `${taskId}-CANCEL_${marketplace.toUpperCase()}_BID`;
      // Don't delete parent job here - it's needed for tracking
      // parentJobs.delete(parentJobId);

      switch (marketplace.toLowerCase()) {
        case OPENSEA.toLowerCase():
          results.opensea = await fetchAndCancelOpenseaBids(task);
          totalOrdersFound += results.opensea.ordersFound;
          if (!results.opensea.success) {
            errors.push(`OpenSea cancellation failed: ${results.opensea.error}`);
          }
          break;
        case MAGICEDEN.toLowerCase():
          results.magiceden = await fetchAndCancelMagicedenBids(task);
          totalOrdersFound += results.magiceden.ordersFound;
          if (!results.magiceden.success) {
            errors.push(`MagicEden cancellation failed: ${results.magiceden.error}`);
          }
          break;
        case BLUR.toLowerCase():
          results.blur = await fetchAndCancelBlurBids(task);
          totalOrdersFound += results.blur.ordersFound;
          if (!results.blur.success) {
            errors.push(`Blur cancellation failed: ${results.blur.error}`);
          }
          break;
      }
    } else {
      // Execute all marketplace cancellations in parallel with timeout
      const cancellationPromises = [
        fetchAndCancelOpenseaBids(task),
        fetchAndCancelMagicedenBids(task),
        fetchAndCancelBlurBids(task)
      ];

      const cancellationTimeout = 60000; // 60 seconds
      const timeoutPromise = new Promise<never>((_, reject) => {
        setTimeout(() => reject(new Error('Bid cancellation timeout')), cancellationTimeout);
      });

      try {
        const allResults = await Promise.race([
          Promise.allSettled(cancellationPromises),
          timeoutPromise
        ]);

        // Process results
        const [openseaResult, magicedenResult, blurResult] = allResults;

        if (openseaResult.status === 'fulfilled') {
          results.opensea = openseaResult.value;
          totalOrdersFound += results.opensea.ordersFound;
          if (!results.opensea.success) {
            errors.push(`OpenSea cancellation failed: ${results.opensea.error}`);
          }
        } else {
          const error = `OpenSea cancellation rejected: ${openseaResult.reason}`;
          errors.push(error);
          results.opensea = { success: false, ordersFound: 0, error };
        }

        if (magicedenResult.status === 'fulfilled') {
          results.magiceden = magicedenResult.value;
          totalOrdersFound += results.magiceden.ordersFound;
          if (!results.magiceden.success) {
            errors.push(`MagicEden cancellation failed: ${results.magiceden.error}`);
          }
        } else {
          const error = `MagicEden cancellation rejected: ${magicedenResult.reason}`;
          errors.push(error);
          results.magiceden = { success: false, ordersFound: 0, error };
        }

        if (blurResult.status === 'fulfilled') {
          results.blur = blurResult.value;
          totalOrdersFound += results.blur.ordersFound;
          if (!results.blur.success) {
            errors.push(`Blur cancellation failed: ${results.blur.error}`);
          }
        } else {
          const error = `Blur cancellation rejected: ${blurResult.reason}`;
          errors.push(error);
          results.blur = { success: false, ordersFound: 0, error };
        }
      } catch (error) {
        const errorMsg = `Parallel cancellation failed: ${error instanceof Error ? error.message : String(error)}`;
        errors.push(errorMsg);
        errorLog('API', `Parallel cancellation failed for task ${taskId} (${contractSlug}), marketplace=${marketplace}, attempt=${attempt}`, error instanceof Error ? error : new Error(String(error)));
      }
    }

    const duration = Date.now() - startTime;
    const success = errors.length === 0;


    return { success, results, totalOrdersFound, errors, duration };
  } catch (error) {
    const duration = Date.now() - startTime;
    const errorMsg = error instanceof Error ? error.message : String(error);

    errorLog('API', `Failed to cancel all related bids for task ${taskId} (${contractSlug}), marketplace=${marketplace}, attempt=${attempt}, duration=${duration}ms`, error instanceof Error ? error : new Error(String(error)));

    return {
      success: false,
      results,
      totalOrdersFound,
      errors: [...errors, errorMsg],
      duration
    };
  }
}

/**
 * Verify that all orders for a task have been cancelled
 * Returns true if no orders remain, false otherwise
 */
async function verifyCancellationComplete(
  taskId: string,
  marketplace?: string
): Promise<{
  complete: boolean;
  remainingOrders: number;
  marketplaceBreakdown?: {
    opensea: number;
    magiceden: number;
    blur: number;
  };
}> {
  try {
    if (marketplace) {
      // Check specific marketplace using Redis orderCache
      const remainingOrders = await orderCache.countByTaskAndMarketplace(taskId, marketplace.toLowerCase());

      return {
        complete: remainingOrders === 0,
        remainingOrders
      };
    } else {
      // Check all marketplaces using Redis orderCache
      const [openSeaCount, magicEdenCount, blurCount] = await Promise.all([
        orderCache.countByTaskAndMarketplace(taskId, 'opensea'),
        orderCache.countByTaskAndMarketplace(taskId, 'magiceden'),
        orderCache.countByTaskAndMarketplace(taskId, 'blur')
      ]);

      const totalRemaining = openSeaCount + magicEdenCount + blurCount;

      return {
        complete: totalRemaining === 0,
        remainingOrders: totalRemaining,
        marketplaceBreakdown: {
          opensea: openSeaCount,
          magiceden: magicEdenCount,
          blur: blurCount
        }
      };
    }
  } catch (error) {
    errorLog('Cancellation', `Error verifying cancellation for task ${taskId}`, error);
    return {
      complete: false,
      remainingOrders: -1
    };
  }
}

async function extractMagicedenOrderHash(orders: any[]): Promise<string[]> {
  // Parallel parsing with error handling
  const orderData = await Promise.all(
    orders.map(async (order) => {
      if (!order) return null;
      try {
        return JSON.parse(order.orderHash);
      } catch (e) {
        errorLog('Order', 'Error parsing order hash', e);
        return null;
      }
    })
  );

  // Extract order IDs
  return orderData
    .filter(Boolean)
    .map(bid => {
      if (bid.results) return bid.results[0].orderId;
      if (bid.message && bid.orderId) return bid.orderId;
      return null;
    })
    .filter(Boolean);
}

// Set to track orders being cancelled globally (across all tasks)
const magicedenOrdersBeingCancelled = new Set<string>();

async function cancelMagicedenBids(orders: any[], walletId: string, taskId: string) {
  if (!orders.length) return;


  // Parent job tracking removed - now using cancellationTracker

  // Check cancellation tracker status
  const trackingStatus = cancellationTracker.getStatus(taskId, 'magiceden');

  // Filter out orders that are already being cancelled
  const ordersToCancel = [];
  const duplicateOrders = [];

  for (const order of orders) {
    const orderId = order.orderHash || order.orderHash;
    if (magicedenOrdersBeingCancelled.has(orderId)) {
      duplicateOrders.push(orderId);
    } else {
      magicedenOrdersBeingCancelled.add(orderId);
      ordersToCancel.push(order);
    }
  }

  if (duplicateOrders.length > 0) {
  }

  if (!ordersToCancel.length) {
    return;
  }


  for (let i = 0; i < ordersToCancel.length; i += 500) {
    const batchOrders = ordersToCancel.slice(i, i + 500);
    const extractedOrderIds = await extractMagicedenOrderHash(batchOrders)
    const orderKeys = batchOrders.map((order) => order.orderHash)
    const identifiers = batchOrders.map((order: any) => order.identifier)  // For defensive verification
    const slugs = batchOrders.map((order: any) => order.slug)              // For logging context
    const batchId = `batch-${i / 500}-${Date.now()}`; // Unique batch identifier


    const jobOptions = getCancellationJobOptions(taskId, batchId, CANCEL_MAGICEDEN_BID);

    await queue.add(CANCEL_MAGICEDEN_BID, {
      orderIds: extractedOrderIds,
      walletId,
      orderKeys,
      identifiers,  // For defensive verification in completion handler
      slugs,        // For logging context
      taskId,
      batchSize: batchOrders.length // Add batch size to job data
    }, jobOptions);
  }

  // Decrement the own bid counter by the number of cancelled orders

}

async function cancelBlurBids(orders: any[], walletId: string, taskId: string) {
  if (!orders.length) return


  // Parent job tracking removed - now using cancellationTracker

  // Check cancellation tracker status
  const trackingStatus = cancellationTracker.getStatus(taskId, 'blur');


  const cancelData = orders.map((order, index) => {
    const payload = JSON.parse(order.orderHash)
    const jobOptions = getCancellationJobOptions(taskId, order.orderHash, CANCEL_BLUR_BID);
    return {
      name: CANCEL_BLUR_BID,
      data: {
        walletId,
        payload,
        taskId,
        identifier: order.identifier,
        slug: order.slug                // For logging context
      },
      opts: jobOptions
    }
  }) as Job[]

  // Queue cancellation jobs directly with queue.addBulk
  if (cancelData.length > 0) {
    const preparedJobs = cancelData.map(job => ({
      name: job.name,
      data: job.data,
      opts: {
        ...job.opts,
        removeOnComplete: true,
        removeOnFail: false  // Keep failed cancels for retry
      }
    }));

    await queue.addBulk(preparedJobs);
  }

}

async function cancelOpenseaBids(orders: any[], walletId: string, taskId: string, currencySymbol: string) {
  // Debug log: Check if walletId is a valid MongoDB ObjectId or something else (like a private key)
  const isValidObjectId = /^[a-f\d]{24}$/i.test(walletId);

  if (!orders.length) return;


  // Parent job tracking removed - now using cancellationTracker

  const cancelData = orders.map((order, index) => {
    return {
      name: CANCEL_OPENSEA_BID,
      data: {
        walletId,
        orderId: order.orderHash,
        taskId,
        identifier: order.identifier,
        slug: order.slug                // For logging context
      },
      opts: getCancellationJobOptions(taskId, order.orderHash, CANCEL_OPENSEA_BID)
    }
  }) as Job[]

  // Track the job IDs we're about to create for reconciliation
  const expectedJobIds = cancelData.map(job => job.opts.jobId);

  // Expected job count tracking removed - using marketplace-specific counts

  // Queue cancellation jobs directly with queue.addBulk
  if (cancelData.length > 0) {
    const preparedJobs = cancelData.map(job => ({
      name: job.name,
      data: job.data,
      opts: {
        ...job.opts,
        removeOnComplete: true,
        removeOnFail: false  // Keep failed cancels for retry
      }
    }));

    await queue.addBulk(preparedJobs);
  }


  // Log current tracker state
  const trackingStatus = cancellationTracker.getStatus(taskId, 'opensea');
  if (trackingStatus) {
  }

  // Decrement the own bid counter by the number of cancelled orders
}

async function cancelOpenseaOrdersDirectly({
  taskId,
  walletId,
  privateKey,
  orders,
  source,
  identifier,
  reason
}: {
  taskId: string;
  walletId: string;
  privateKey: string;
  orders: any[];
  source: string;
  identifier?: string;
  reason?: string;
}) {
  if (!orders?.length) {
    return;
  }

  const metadataBase = {
    walletId,
    identifier,
    count: orders.length,
    reason
  };



  let completed = 0;

  try {
    for (const order of orders) {
      await cancelOrderWithoutTracking(order.orderHash, OPENSEA_PROTOCOL_ADDRESS, privateKey, taskId);
      await safeDeleteFromCache(taskId, 'opensea', order.identifier, order.orderHash);
      completed += 1;
    }
  } catch (error) {
    throw error;
  }

  if (completed > 0) {
    cancellationTracker.recordCompletion(taskId, completed, 'opensea');
    await updateTaskCancellationPercentage(taskId, undefined, 'opensea');
  }

}

async function cancelMagicEdenOrdersDirectly({
  taskId,
  walletId,
  privateKey,
  orders,
  source,
  identifier,
  reason
}: {
  taskId: string;
  walletId: string;
  privateKey: string;
  orders: any[];
  source: string;
  identifier?: string;
  reason?: string;
}) {
  if (!orders?.length) {
    return;
  }

  let completed = 0;

  try {
    // Extract order IDs from orders
    const orderIds = orders.map(order => order.orderHash);

    // Cancel all orders at once
    await cancelMagicEdenBid(orderIds, privateKey, taskId);

    // Delete orders from cache (source of truth)
    for (const order of orders) {
      await safeDeleteFromCache(taskId, 'magiceden', order.identifier, order.orderHash);
    }

    completed = orders.length;
  } catch (error) {
    throw error;
  }

  if (completed > 0) {
    cancellationTracker.recordCompletion(taskId, completed, 'magiceden');
    await updateTaskCancellationPercentage(taskId, undefined, 'magiceden');
  }
}

async function cancelBlurOrdersDirectly({
  taskId,
  walletId,
  privateKey,
  orders,
  source,
  identifier,
  reason
}: {
  taskId: string;
  walletId: string;
  privateKey: string;
  orders: any[];
  source: string;
  identifier?: string;
  reason?: string;
}) {
  const startTime = Date.now();


  if (!orders?.length) {
    return;
  }

  let completed = 0;
  let failed = 0;

  try {
    // Cancel each order sequentially (Blur requires individual cancellation)
    for (let i = 0; i < orders.length; i++) {
      const order = orders[i];
      const orderStart = Date.now();
      try {
        const payload = JSON.parse(order.orderHash);
        await cancelBlurBid({ payload, privateKey, taskId });
        await safeDeleteFromCache(taskId, 'blur', order.identifier, order.orderHash);
        completed += 1;

        // Log progress every 10 orders or on last order
        if ((i + 1) % 10 === 0 || i === orders.length - 1) {
        }
      } catch (orderError) {
        failed++;
        errorLog('StopCancelOrder', `BLUR_DIRECT_ORDER_FAILED | taskId=${taskId} | orderHash=${order.orderHash} | orderIndex=${i} | error=${orderError instanceof Error ? orderError.message : String(orderError)}`, { taskId, marketplace: 'blur' });
      }
    }
  } catch (error) {
    const elapsedMs = Date.now() - startTime;
    errorLog('StopCancelOrder', `BLUR_DIRECT_FAILED | taskId=${taskId} | completed=${completed} | failed=${failed} | total=${orders.length} | elapsedMs=${elapsedMs} | error=${error instanceof Error ? error.message : String(error)}`, { taskId, marketplace: 'blur' });
    throw error;
  }

  const elapsedMs = Date.now() - startTime;

  if (completed > 0) {
    cancellationTracker.recordCompletion(taskId, completed, 'blur');
    await updateTaskCancellationPercentage(taskId, undefined, 'blur');
  }

}

async function updateStatus(task: ITask & { stoppedByWarning?: boolean }) {
  try {
    const { _id: taskId, running, stoppedByWarning } = task;

    // If stopped by warning, ensure task is stopped but keep warning state
    if (stoppedByWarning) {

      // DO NOT clear warning state - let it persist until task is updated

      // Update activeTasks Map
      const updatedTask = { ...task, running: false };
      activeTasks.set(taskId, updatedTask);

      // Stop the task - pass stoppedByWarning flag
      await stopTask(task, false, undefined, true);
      return; // Don't restart the task
    }

    // Normal toggle logic
    const start = !running;

    if (start) {
      // Update activeTasks Map
      const updatedTask = { ...task, running: true };
      activeTasks.set(taskId, updatedTask);

      await startTask(task, true);
    } else {
      // Update activeTasks Map
      const updatedTask = { ...task, running: false };
      activeTasks.set(taskId, updatedTask);
      await stopTask(task, false);
    }
  } catch (error) {
    errorLog('Task', `Error updating status for task: ${task._id}`, error);
  }
}


async function updateMarketplace(task: ITask) {
  try {

    const { _id: taskId, selectedMarketplaces: newMarketplaces } = task;
    const taskIndex = currentTasks?.findIndex(task => task?._id === taskId);

    if (taskIndex === -1) return;

    const currentMarketplaces = currentTasks[taskIndex].selectedMarketplaces;
    const currentSet = new Set(currentMarketplaces.map(m => m.toLowerCase()));
    const newSet = new Set(newMarketplaces.map(m => m.toLowerCase()));

    const outgoing = Array.from(currentSet).find(m => !newSet.has(m));
    const incoming = Array.from(newSet).find(m => !currentSet.has(m));

    if (outgoing) {
    }

    currentTasks[taskIndex] = {
      ...currentTasks[taskIndex],
      selectedMarketplaces: [...newMarketplaces]
    };

    const updatedTask = {
      ...currentTasks[taskIndex],
      selectedMarketplaces: [...newMarketplaces]
    };
    currentTasks[taskIndex] = updatedTask;
    activeTasks.set(taskId, updatedTask);


    if (outgoing) {
      await handleOutgoingMarketplace(outgoing, task);
    }

    if (incoming) {
      const color = incoming.toLowerCase() === "magiceden" ? MAGENTA :
        incoming.toLowerCase() === "blur" ? GOLD : BLUE;


      // Only create jobs if the task is actually running
      // This prevents jobs from being created when marketplaces are added to stopped tasks
      if (updatedTask.running) {
        // Use startTask to handle marketplace addition - it will create the necessary jobs
        try {
          await startTask(updatedTask, true, incoming.toLowerCase());
        } catch (error) {
          errorLog('Task', `Error adding marketplace ${incoming} to task ${task._id}`, error);
        }
      } else {
      }
    }

  } catch (error) {
    errorLog('Task', `Error updating marketplace for task: ${task._id}`, error);
  }
}

async function handleOutgoingMarketplace(marketplace: string, task: ITask) {
  try {
    await stopTask(task, false, marketplace);
  } catch (error) {
    errorLog('Task', `Failed to handle outgoing marketplace ${marketplace} for task ${task.contract.slug}`, error);
  }
}

async function updateMultipleTasksStatus(data: { tasks: ITask[], running: boolean }) {
  const startTime = Date.now();
  const taskIds = data.tasks.map(t => t._id.toString());
  const targetStatus = data.running ? 'START' : 'STOP';

  // Performance profiling entry


  try {
    const { tasks, running } = data;

    if (running) {
      // Start tasks sequentially to prevent WebSocket subscription race conditions

      let succeeded = 0;
      let failed = 0;

      for (const task of tasks) {
        const taskId = task._id?.toString() || 'unknown';
        const taskSlug = task.contract?.slug || taskId;

        try {
          await startTask(task, true);
          succeeded++;
        } catch (err: any) {
          failed++;
          errorLog('BulkTaskStart', `Failed to start task ${taskId}`, err);
          warnLog('BulkStart', `TASK_START_FAILED | taskId=${taskId} | slug=${taskSlug} | error=${err?.message || String(err)}`);
        }
      }

      const duration = Date.now() - startTime;
    } else {
      // Stop tasks sequentially to prevent WebSocket unsubscription race conditions

      let succeeded = 0;
      let failed = 0;

      for (const task of tasks) {
        const taskId = task._id?.toString() || 'unknown';
        const taskSlug = task.contract?.slug || taskId;

        try {
          await stopTask(task, false);
          succeeded++;
        } catch (err: any) {
          failed++;
          errorLog('BulkTaskStop', `Failed to stop task ${taskId}`, err);
          warnLog('StopTaskFlow', `TASK_STOP_FAILED | taskId=${taskId} | slug=${taskSlug} | error=${err?.message || String(err)}`);
        }
      }

      const duration = Date.now() - startTime;

      // Clean up any orphaned data for stopped tasks
      for (const task of tasks) {
        const taskId = task._id.toString();

        // Ensure skipStats is cleaned up
        if (skipStats[taskId]) {
          delete skipStats[taskId];
        }

        // Ensure currentTraits is cleaned up
        if (currentTraits[taskId]) {
          delete currentTraits[taskId];
        }

        // Ensure currentTokenIds is cleaned up
        if (currentTokenIds[taskId]) {
          delete currentTokenIds[taskId];
        }

        // Ensure bestOffers is cleaned up
        if (bestOffers[taskId]) {
          delete bestOffers[taskId];
        }

        // NOTE: Do NOT delete bidStats here - it will be deleted in updateTaskCancellationPercentage when 100% complete
        // This allows proper decrementBidCount tracking during cancellation

        // Ensure errorStats is cleaned up
        if (errorStats[taskId]) {
          delete errorStats[taskId];
        }
      }
      const stopDuration = Date.now() - startTime;
    }
  } catch (error) {
    const duration = Date.now() - startTime;
    errorLog('BulkUpdate', `ERROR | action=${targetStatus} | taskCount=${data.tasks.length} | durationMs=${duration} | error=${error instanceof Error ? error.message : String(error)}`, error);
  }
}

let pingIntervalId: NodeJS.Timeout | null = null;

// WebSocket Connection Monitoring Utility Functions
function updateConnectionStats(duration: number, reason: string) {
  // Add to history
  if (connectionMetrics.currentConnectionId) {
    connectionHistory.push({
      id: connectionMetrics.currentConnectionId,
      startTime: connectionMetrics.connectionStartTime!,
      endTime: connectionMetrics.connectionEndTime!,
      duration,
      reason
    });

    // Keep only last 100 connections
    if (connectionHistory.length > 100) {
      connectionHistory.shift();
    }
  }

  // Update running averages
  const totalDuration = connectionHistory.reduce((sum, conn) => sum + conn.duration, 0);
  connectionMetrics.averageConnectionDuration = connectionHistory.length > 0 ? totalDuration / connectionHistory.length : 0;

  // Update min/max
  if (duration > connectionMetrics.longestConnection) {
    connectionMetrics.longestConnection = duration;
  }
  if (duration < connectionMetrics.shortestConnection) {
    connectionMetrics.shortestConnection = duration;
  }
}

function formatDuration(ms: number): string {
  const seconds = Math.floor(ms / 1000);
  const minutes = Math.floor(seconds / 60);
  const hours = Math.floor(minutes / 60);

  if (hours > 0) {
    return `${hours}h ${minutes % 60}m ${seconds % 60}s`;
  } else if (minutes > 0) {
    return `${minutes}m ${seconds % 60}s`;
  } else {
    return `${seconds}s`;
  }
}

function getCloseReason(code: number): string {
  const reasons: Record<number, string> = {
    1000: 'normal_close',
    1001: 'going_away',
    1002: 'protocol_error',
    1003: 'unsupported_data',
    1006: 'abnormal_closure',
    1011: 'internal_error',
    1012: 'service_restart'
  };
  return reasons[code] || 'unknown';
}

function logConnectionStats() {

  if (connectionMetrics.connectionStartTime) {
    const currentDuration = Date.now() - connectionMetrics.connectionStartTime;
  }

  // Recent disconnection reasons
  const recentDisconnections = connectionHistory.slice(-10);
  const reasonCounts = recentDisconnections.reduce((acc, conn) => {
    acc[conn.reason] = (acc[conn.reason] || 0) + 1;
    return acc;
  }, {} as Record<string, number>);

}

// WebSocket metrics persistence removed - metrics reset on server restart

function startConnectionMonitoring() {
  setInterval(() => {
    if (connectionMetrics.connectionStartTime && wsManager?.isConnected()) {
      const currentDuration = Date.now() - connectionMetrics.connectionStartTime;

      // Log every 5 minutes
      if (currentDuration % (5 * 60 * 1000) < 10000) { // Within 10 seconds of 5-minute mark
      }

      // Log connection stats every hour
      if (currentDuration % (60 * 60 * 1000) < 10000) { // Within 10 seconds of hour mark
        logConnectionStats();
      }
    }
  }, STANDARD_INTERVAL);
}


// Connection state management to prevent multiple simultaneous connections
let isConnecting = false;
let connectionPromise: Promise<void> | null = null;

async function connectWebSocket(): Promise<void> {

  // Return existing connection attempt if already in progress
  if (isConnecting && connectionPromise) {
    return connectionPromise;
  }

  // If already connected, just return
  if (wsManager?.isConnected()) {
    return;
  }

  // Start new connection attempt
  isConnecting = true;
  connectionPromise = _doConnect();

  try {
    await connectionPromise;
  } finally {
    isConnecting = false;
    connectionPromise = null;
  }
}

async function _doConnect(): Promise<void> {
  if (!wsManager) {
    wsManager = new WebSocketManager({
      url: MARKETPLACE_WS_URL,
      clientId: clientId,
      maxRetries: 0, // 0 means infinite retries
      reconnectBaseDelay: 1000,
      reconnectMaxDelay: 60000,
      heartbeatInterval: 30000, // CLIENT_PROTOCOL.md specifies 30 seconds
      connectionTimeout: 10000
    });

    // Set up event handlers
    wsManager.on('connected', async (connectionId) => {
      // ALWAYS-ON connection log (not behind DEBUG flag) for diagnosing WebSocket connectivity

      // Update connection metrics
      connectionMetrics.connectionStartTime = Date.now();
      connectionMetrics.currentConnectionId = connectionId;
      connectionMetrics.totalConnections++;
      retryCount = 0;

      // Clear activeSubscriptions to sync with server
      activeSubscriptions.clear();

      // Subscribe only running tasks after connection
      const runningTasks = currentTasks.filter(task => task.running);
      if (runningTasks.length > 0) {
        await subscribeToCollections(runningTasks as unknown as ITask[]);

        // Pre-warm caches for active tasks to improve counterbid performance
        runningTasks.forEach(async (task) => {
          if (task.wallet) {
            // Pre-load wallet data into cache
            walletCache.getWallet(task.wallet).catch(() => { });
          }
          if (task.contract?.contractAddress) {
            // Pre-load collection data into cache for each marketplace
            ['opensea', 'magiceden', 'blur'].forEach(async marketplace => {
              await getCachedFloorPrice(task.contract.contractAddress, marketplace);
            });
          }
        });
      }

      // Re-subscribe to listing collections for instant price matching
      resubscribeToListingCollections();
    });

    wsManager.on('disconnected', async (event: { code: number; reason: string }) => {
      // Calculate connection duration
      if (connectionMetrics.connectionStartTime) {
        connectionMetrics.connectionEndTime = Date.now();
        connectionMetrics.connectionDuration = connectionMetrics.connectionEndTime - connectionMetrics.connectionStartTime;
        connectionMetrics.totalDisconnections++;

        updateConnectionStats(connectionMetrics.connectionDuration, getCloseReason(event.code));

      } else {
      }

      // Clear subscription cache on disconnect
      activeSubscriptions.clear();
    });

    // Batch event handler for optimized processing
    // Batches are emitted by ws-proxy's BatchAggregator (100ms windows)
    // Individual events are also processed via 'message' event for backward compatibility
    wsManager.on('batch', (batch: { events: any[]; count: number; subscriptionType: string; receivedAt: number; localReceiveTime: number }) => {
      try {
        const processingLatency = Date.now() - batch.localReceiveTime;
        const proxyLatency = batch.localReceiveTime - batch.receivedAt;

        // Group events by collection for efficient O(1) task matching
        const byCollection = new Map<string, any[]>();
        for (const event of batch.events) {
          const key = event.slug || event.contractAddress || 'unknown';
          const existing = byCollection.get(key);
          if (existing) {
            existing.push(event);
          } else {
            byCollection.set(key, [event]);
          }
        }

        // Note: Individual events are already processed via processSingleMessage -> 'message' event
        // This batch handler is primarily for metrics and future optimizations (e.g., bulk dedup)
      } catch (error) {
        errorLog('WebSocket', `[BATCH_ERROR] Error processing batch: ${error}`);
      }
    });

    // Message handler with completion callback for accurate backpressure tracking (2025-12-29 optimization)
    wsManager.on('message', async (message: any, done?: () => void) => {
      try {
        incrementRawWsMessagesReceived();

        // Extract receivedAt timestamp from proxy (or fallback to server time)
        const receivedAt = message.receivedAt || Date.now();

        // Track ALL bid events at earliest point (before any filtering)
        if (message.eventType === 'BID_CREATED' || message.eventType === 'item_received_bid') {
          incrementBidEventsReceived();
          const traceId = message._traceId || 'no-trace';

          const maker = message.payload?.payload?.maker?.address ||
            message.payload?.maker?.address;
          if (maker && isOurWallet(maker)) {
            incrementRawOurBidsReceived();
            const orderHash = message.payload?.payload?.order?.orderHash ||
              message.payload?.order?.orderHash ||
              message.orderHash;
          }
        }

        // Handle initial data request synchronously
        if (message.endpoint === "get-initial-data") {
          const initialDataMessage = {
            type: 'bidRatesUpdate',
            data: clientData
          };
          wsManager.sendMessage(initialDataMessage);
          done?.(); // Signal completion for simple sync handlers
          return;
        }

        // Process messages without blocking (fire-and-forget with completion tracking)
        // Use .finally() to signal done() when handler completes without serializing messages
        // Pass receivedAt to measure true end-to-end execution latency
        handleMarketplaceMessage(message, receivedAt)
          .catch(error => {
            errorLog('WsConnection', 'Error processing marketplace message', error);
          })
          .finally(() => {
            done?.(); // Signal handler completion for backpressure tracking
          });

      } catch (error) {
        errorLog('WsConnection', 'Error in message handler', error);
        done?.(); // Signal completion even on sync error to prevent handler leak
      }
    });

    wsManager.on('error', async (error: Error) => {
      errorLog('WsConnection', `WebSocket connection error: connectionId=${connectionMetrics.currentConnectionId}, retryCount=${retryCount}, duration=${connectionMetrics.connectionStartTime ? Date.now() - connectionMetrics.connectionStartTime : 0}ms`, error);
    });

    wsManager.on('server_status', (status: any) => {
      // Update any UI or metrics as needed
    });

    wsManager.on('max_retries_exceeded', async () => {
      errorLog('WsConnection', `WebSocket max reconnection attempts exceeded: connectionId=${connectionMetrics.currentConnectionId}, retryCount=${MAX_RETRIES}`, new Error('Max retries exceeded'));
    });


  }

  // Connect with error handling
  try {
    await wsManager.connect();
  } catch (error) {
    errorLog('WsConnection', 'Failed to initiate WebSocket connection', error instanceof Error ? error : new Error(String(error)));
    // Don't rethrow - let the WebSocketManager handle reconnection
  }
}


/**
 * Unified handler for all marketplace messages
 * Handles both counterbid messages and listing price updates
 */
async function handleMarketplaceMessage(message: any, receivedAt: number) {

  const extractedMaker = (message?.payload?.payload?.maker?.address)?.toLowerCase();
  const extractedContract = (message?.payload?.payload?.asset_contract_criteria?.address)?.toLowerCase();
  const extractedSlug = (message?.payload?.payload?.collection?.slug)?.toLowerCase();
  const extractedOrderHash = message?.order_hash || message?.payload?.payload?.order_hash;

  // Normalize event type field - handle both message.event (OpenSea) and message.eventType (normalized)
  // This fixes the mismatch where messages arrive with message.event but handlers check message.eventType
  if (!message.eventType && message.event) {
    const eventMap: Record<string, string> = {
      'item_received_bid': 'BID_CREATED',
      'trait_offer': 'BID_CREATED',
      'collection_offer': 'BID_CREATED',
      'item_listed': 'LISTING_CREATED',
      'item_sold': 'ITEM_SOLD',
    };
    message.eventType = eventMap[message.event] || message.event.toUpperCase();
  }

  try {
    if (message.eventType === 'BID_CREATED') {
      return handleCounterBid(message, receivedAt)
    }

    else if (message.eventType === 'LISTING_CREATED') {
      // Handle listing price update for instant price matching
      return handleListingPriceUpdate(message);
    }

    else if (message.eventType === 'ITEM_SOLD') {
      return updateBalancesForItemSold(message);
    }

  } catch (error) {
    errorLog('WsConnection', 'Error in handleMarketplaceMessage', error);
  }
}


// Check if a token belongs to a rarity category (LEGENDARY/EPIC/RARE)
async function isTokenInRarityCategory(
  collectionSlug: string,
  contractAddress: string,
  tokenId: string,
  rarityCategory: "LEGENDARY" | "EPIC" | "RARE"
): Promise<boolean> {
  try {
    // Fetch rarity tokens for the category (uses Redis/MongoDB caching)
    const result = await fetchRarityTokens(
      collectionSlug,
      rarityCategory
    );
    // Check if tokenId is in the list (compare as numbers)
    return result.tokenIds.includes(Number(tokenId));
  } catch (error) {
    errorLog('Balance', `Failed to check rarity category for token ${tokenId}`, error instanceof Error ? error : new Error(String(error)));
    return false; // Fail safe - don't count if we can't verify
  }
}

// Check if a purchased token matches a task's criteria (bidType, tokenIds, rarity, traits)
async function doesTokenMatchTask(
  task: ITask,
  contractAddress: string,
  tokenId: string
): Promise<boolean> {
  const taskSlug = task.contract.slug;

  // 1. Check bidType
  switch (task.bidType) {
    case "collection":
      // Collection bids match any token, but still check selectedTraits if present
      break;

    case "token": {
      // Must be in task.tokenIds (check both string and number)
      const tokenIdStr = tokenId.toString();
      const tokenIdNum = Number(tokenId);
      const isInTokenIds = task.tokenIds.some(
        id => id.toString() === tokenIdStr || Number(id) === tokenIdNum
      );
      if (!isInTokenIds) {
        return false;
      }
      break;
    }

    case "rarity":
      // Must be in the rarity category
      if (task.rarityCategory) {
        const isInCategory = await isTokenInRarityCategory(
          taskSlug,
          contractAddress,
          tokenId,
          task.rarityCategory
        );
        if (!isInCategory) {
          return false;
        }
      }
      break;
  }

  // 2. Check selectedTraits (if present)
  if (task.selectedTraits && Object.keys(task.selectedTraits).length > 0) {
    // Use tokenHasTrait for each selected trait
    for (const [marketplace, categories] of Object.entries(task.selectedTraits)) {
      for (const [category, traits] of Object.entries(categories as Record<string, { name: string }[]>)) {
        for (const trait of traits) {
          const hasTrait = await tokenHasTrait(contractAddress, tokenId, category, trait.name);

          if (task.traitsOperator?.value === "and") {
            // AND: all traits must match - if one fails, return false
            if (!hasTrait) {
              return false;
            }
          } else {
            // OR: at least one trait must match - if one passes, return true
            if (hasTrait) {
              return true;
            }
          }
        }
      }
    }
    // If we get here:
    // - AND mode: all traits matched (didn't return false) → return true
    // - OR mode: no traits matched (didn't return true) → return false
    const result = task.traitsOperator?.value === "and";
    if (!result) {
    }
    return result;
  }

  return true; // No trait filter, matches by default
}

async function updateBalancesForItemSold(message: any) {
  try {
    // Extract data from the ITEM_SOLD event
    const data = message.data || message;
    const toAddress = data.toAddress?.toLowerCase();
    const contractAddress = data.contractAddress?.toLowerCase();
    const marketplace = data.marketplace?.toLowerCase() as 'opensea' | 'magiceden' | 'blur';
    const tokenId = data.tokenId?.toString();
    const price = data.price; // Purchase price

    if (!toAddress || !contractAddress || !marketplace || !tokenId) {
      return;
    }

    // Create a unique key for deduplication including transaction hash if available
    const txHash = data.transactionHash || '';
    const eventKey = txHash ?
      `${contractAddress}:${tokenId}:${txHash}` :
      `${contractAddress}:${tokenId}:${marketplace}:${toAddress}:${price}:${data.createdAt || Date.now()}`;


    // Add to the set to prevent future duplicates
    recentItemSoldEvents.add(eventKey);

    // Check if the toAddress belongs to one of our wallets (case-insensitive)
    const wallet: IWallet | null = await walletCache.getWallet(toAddress);

    if (!wallet) {
      // Not one of our wallets, ignore
      return;
    }

    // Trigger auto-listing for the purchased item
    await triggerAutoListingForPurchase(wallet, contractAddress, tokenId, price, marketplace);

    // Find all tasks for this wallet and contract, then check which ones match the purchased token
    try {
      const walletId = wallet._id?.toString();
      const tasks = await Task.find({
        walletId: walletId,
        'contract.contractAddress': { $regex: new RegExp(`^${contractAddress}$`, 'i') }
      });

      let matchedTaskCount = 0;
      for (const task of tasks) {
        // Check if this specific token matches this task's criteria
        const matches = await doesTokenMatchTask(task, contractAddress, tokenId);
        if (matches) {
          await Task.updateOne(
            { _id: task._id },
            { $inc: { 'stopOptions.actualPurchase': 1 } }
          );
          matchedTaskCount++;
        }
      }

      if (matchedTaskCount > 0) {
      } else if (tasks.length > 0) {
      }
    } catch (dbError) {
      errorLog('Balance', 'Error persisting actualPurchase to database', dbError instanceof Error ? dbError : new Error(String(dbError)));
    }

    // Check for auto-listing tasks

  } catch (error) {
    errorLog('Balance', 'Error updating balances for ITEM_SOLD', error);
  }
}

/**
 * Triggers auto-listing for purchased items if auto-listing is enabled on associated tasks
 */
async function triggerAutoListingForPurchase(
  wallet: IWallet,
  contractAddress: string,
  tokenId: string,
  price: number,
  marketplace: string
): Promise<void> {
  try {
    // Validate wallet parameter
    if (!wallet || !wallet.address || !wallet._id) {
      errorLog('Listing', 'Invalid wallet provided to triggerAutoListingForPurchase');
      return;
    }

    // Find tasks with auto-listing enabled for this contract and wallet
    const autoListingTasks = await Task.find({
      // $or: [
      //   { wallet: wallet.address },
      //   { walletId: wallet._id }
      // ],
      'contract.contractAddress': { $regex: new RegExp(`^${contractAddress}$`, 'i') },
      autoList: true,
      running: true,
      listingConfig: { $exists: true, $ne: null }
    });

    if (autoListingTasks.length === 0) {
      return;
    }


    // Process each auto-listing task
    for (const task of autoListingTasks) {
      const listingConfig = task.listingConfig!;


      // Create listing data based on task configuration
      const listingData: Partial<IListing> = {
        user: task.user,
        tokenId: tokenId,
        contract: {
          slug: task.contract.slug,
          contractAddress: task.contract.contractAddress
        },
        wallet: wallet.address,
        walletId: wallet._id.toString(),
        selectedMarketplaces: listingConfig.selectedMarketplaces,
        running: true,
        instantPriceMatching: listingConfig.instantPriceMatching,
        basePrice: listingConfig.basePrice,
        underCutMargin: listingConfig.underCutMargin,
        openseaListingPrice: listingConfig.openseaListingPrice,
        blurListingPrice: listingConfig.blurListingPrice,
        magicEdenListingPrice: listingConfig.magicEdenListingPrice,
        minListingPrice: listingConfig.minListingPrice,
        minimumProfit: listingConfig.minimumProfit,
        loopInterval: listingConfig.loopInterval,
        listDuration: listingConfig.listDuration,
        listingPriceType: listingConfig.listingPriceType,
        pricingStrategy: listingConfig.pricingStrategy,
        buyPrice: price, // Set buy price from purchase
        dynamicPricingBasis: listingConfig.dynamicPricingBasis,
        selectedTrait: listingConfig.selectedTrait,
        listed: false
      };

      // Handle dynamic pricing based on purchase price
      if (listingConfig.pricingStrategy === 'DYNAMIC_PRICING' && listingConfig.dynamicPricingBasis === 'BUY_PRICE') {
        const minimumProfit = parseFloat(listingConfig.minimumProfit) || 0;
        const calculatedPrice = price + minimumProfit;
        listingData.basePrice = calculatedPrice.toString();
      }

      try {
        // Check if listing already exists for this token
        const existingListing = await Listing.findOne({
          'contract.contractAddress': { $regex: new RegExp(`^${contractAddress}$`, 'i') },
          tokenId: tokenId,
          $or: [
            { wallet: wallet.address },
            { walletId: wallet._id }
          ]
        });

        if (existingListing) {
          continue;
        }

        // Create new listing
        const newListing = new Listing(listingData);
        await newListing.save();


        // Schedule the listing for immediate processing
        await scheduleListing(newListing);


      } catch (listingError) {
        errorLog('Listing', `Failed to create auto-listing for token ${tokenId}`, listingError);
        errorLog('General', `Auto-listing creation failed: taskId=${task._id}, contractAddress=${contractAddress}, tokenId=${tokenId}`, listingError instanceof Error ? listingError : new Error(String(listingError)));
      }
    }

  } catch (error) {
    errorLog('Listing', 'Error in triggerAutoListingForPurchase', error);
    errorLog('General', `Auto-listing trigger failed: contractAddress=${contractAddress}, tokenId=${tokenId}, wallet=${wallet?.address || 'undefined'}`, error instanceof Error ? error : new Error(String(error)));
  }
}

async function handleCounterBid(message: any, receivedAt: number) {
  try {
    // Track bid events reaching handler
    incrementBidEventsReachingHandler();
    const traceId = message._traceId || 'no-trace';

    // Mark that we received a bid event for the watchdog
    wsManager?.markBidEventReceived();

    // CounterBid received logging removed - too noisy

    const { contractAddress, slug } = getMarketplaceDetails(message);

    if (!contractAddress && !slug) {
      warnLog('MsgDelivery', `CP_NO_COLLECTION | eventType=${message.eventType || message.event || 'unknown'} | marketplace=${message.marketplace || 'unknown'} | keys=${Object.keys(message.payload || {}).slice(0, 5).join(',')}`);
      return;
    }

    // Use fast O(1) lookup instead of O(n) filter
    const relevantTasks = getRelevantTasks(contractAddress, slug);

    if (!relevantTasks.length) {
      return;
    }

    // Fire-and-forget counterbid processing (extreme scale optimization)
    // Deterministic job IDs provide deduplication at queue level
    // Pass receivedAt to measure true end-to-end execution latency
    relevantTasks.forEach(task => {
      handleCounterBidForTask(task, message, receivedAt).catch(err => {
        errorLog('CounterbidError', `Task ${task._id}: ${err.message}`);
      });
    });

  } catch (error) {
    errorLog('Counterbid', 'Error in handleCounterBid', error);
  }

}

function getMarketplaceDetails(message: any): { contractAddress?: string, slug?: string } {

  let result: { contractAddress?: string, slug?: string } = {};

  switch (message.marketplace) {
    case BLUR:
      result = handleBlurMessages(message);
      return result;
    case OPENSEA:
      result = handleOpenSeaMessages(message);
      return result;
    case MAGICEDEN:
      result = handleMagicEdenMessages(message);
      return result;
    default:
      return {};
  }
}

async function handleCounterBidForTask(task: any, message: any, receivedAt: number) {
  const selectedMarketplaces = task.selectedMarketplaces.map((m: string) => m.toLowerCase());

  // Extract correlation info from message for bid tracing
  const orderHash = message?.payload?.payload?.order_hash ||
    message?.['1']?.orderHash ||
    message?.orderHash ||
    `msg-${Date.now()}`;
  const rawTokenId = message?.payload?.payload?.protocol_data?.parameters?.consideration?.[0]?.identifierOrCriteria ||
    message?.['1']?.tokenId ||
    message?.tokenId;
  let tokenId: string | undefined;
  try {
    tokenId = rawTokenId ? safeTokenIdToString(rawTokenId) : undefined;
  } catch (error) {
    errorLog('TokenID', `Failed to convert tokenId in counterbid handler | value=${rawTokenId} | error=${error instanceof Error ? error.message : String(error)}`);
  }
  const collectionSlug = task?.contract?.slug || 'unknown';

  // Debug: Check if this is a test message before routing
  const testWallet = '0x1b8afd0de8a9368ce635be8b9572c65d8f590b21';
  const extractedMaker = (message?.payload?.payload?.maker?.address)?.toLowerCase();
  const isTestMessage = extractedMaker === testWallet;

  try {
    if (selectedMarketplaces.includes('blur') && message.marketplace === BLUR) {
      // Validate Blur message structure before accessing nested properties
      if (!message['1'] || typeof message['1'] !== 'object') {
        warnLog('WebSocket', `BLUR_INCOMPLETE - Invalid Blur WebSocket message structure: taskId=${task._id}, marketplace=${message.marketplace}, hasNestedData=${!!message['1']}, messageKeys=${Object.keys(message || {}).join(',')}`);
        return; // Skip processing invalid Blur messages
      }


      await handleBlurCounterbid(message['1'], task, receivedAt)
    }
    else if (selectedMarketplaces.includes('opensea') && message.marketplace === OPENSEA) {
      await handleOpenseaCounterbid(message, task, receivedAt)
    }
    else if (selectedMarketplaces.includes('magiceden') && message.marketplace === MAGICEDEN) {
      // Create trace for MagicEden counterbid

      await handleMagicEdenCounterbid(message, task, receivedAt)
    }
    else {
      // Message didn't match any marketplace routing condition
      incrementNoMarketplaceMatchCounter();
    }
  } catch (error: any) {
    errorLog('Counterbid', `Failed to process counterbid | task=${task._id} | marketplace=${message.marketplace}`, error);
  }
}


async function handleMagicEdenCounterbid(data: any, task: ITask, receivedAt: number) {
  // Structured log: start
  try {
    // Extract bid data early for logging
    const orderHash = data?.data?.id
    const maker = data?.data?.maker?.toLowerCase()
    const bidType = data?.data?.criteria?.kind
    const tokenId = +data?.data?.criteria?.data?.token?.tokenId;  // Will be NaN if not present
    const eth_price_raw = Number(data?.data?.price?.amount?.native)
    const makerFormatted = maker ? `${maker.slice(0, 6)}...${maker.slice(-4)}` : 'unknown';
    const bidTypeDisplay = bidType === 'token' ? `Token #${tokenId}` : bidType === 'collection' ? 'Collection' : bidType;

    const currentTask = activeTasks.get(task._id)
    if (!currentTask) {
      return;
    }

    if (!currentTask?.running) {
      return;
    }

    const taskMarketplaces = currentTask.selectedMarketplaces.map((market) => market.toLowerCase());
    if (!taskMarketplaces.includes("magiceden")) {
      return;
    }

    if (!enabledMarketplaces.map((m) => m.toLowerCase()).includes("magiceden")) {
      return;
    };

    // Early exit if incoming bid is from one of our own wallets
    // NOTE: Must be checked BEFORE balance check so own bids are always counted
    if (maker && isOurWallet(maker)) {
      incrementOwnBidCounter('magiceden', orderHash);
      return;
    }

    const expiration = getExpiry(currentTask.bidDuration)

    // Include task ID in the hash to make it task-specific
    // Use atomic check-and-add to prevent race conditions in concurrent handlers
    const taskSpecificHash = `${orderHash}-${task._id}`;
    const dedupPassed = markSeenIfNew(seenOrderHashes, taskSpecificHash);
    if (!dedupPassed) {
      return;  // Already being processed by another handler
    }

    // Parallel fetch wallet data and all wallets
    const [taskWallet, allWallets] = await Promise.all([
      walletCache.getWallet(task.wallet),
      walletCache.getAllWallets()
    ]);

    if (!taskWallet) {
      errorLog('Database', `Wallet not found for task ${task._id}: ${task.wallet}`, new Error('Wallet not found'), { taskId: task._id.toString(), marketplace: 'magiceden', slug: task.contract.slug });
      return;
    }

    const walletAddresses = allWallets.map((wallet) => wallet.address.toLowerCase())

    // Extract remaining bid data (bidType, tokenId already extracted at top for logging)
    const usdPrice = Number(data?.data?.price?.amount?.usd)
    const eth_price = Number(data?.data?.price?.amount?.native)


    // Initialize bestOffers and currentTokenIds BEFORE any early returns
    if (!bestOffers[task._id]) {
      bestOffers[task._id] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      }
    }

    if (!currentTokenIds[task._id]) {
      currentTokenIds[task._id] = {
        opensea: [],
        magiceden: [],
      }
    }


    if (!currentTask.outbidOptions.counterbid) {
      return;
    }


    // Only decrypt private key after checking if it's our own bid
    const WALLET_PRIVATE_KEY = await walletCache.getDecryptedPrivateKey(taskWallet);
    if (!WALLET_PRIVATE_KEY) {
      errorLog('Security', `Failed to decrypt private key for wallet ${task.wallet}`, new Error('Decryption failed'), { taskId: task._id.toString(), marketplace: 'magiceden', slug: task.contract.slug });
      return;
    }

    const { currencyType, currency, decimal, currencySymbol } = getCurrencyDetailsFromTask(currentTask, "magiceden")
    const usdcBid = currencySymbol.toLowerCase() === "usdc"
    const magicedenOutbidMargin = currentTask?.outbidOptions?.magicedenOutbidMargin || usdcBid ? 0.01 : 0.0001

    let incomingBidAmount: number = 0
    let offerPrice: number;

    switch (currencyType) {
      case "eth":
        incomingBidAmount = Math.round(+eth_price.toFixed(4) * Math.pow(10, 18)) * 1e14 / 1e14
        break;
      case "usdc":
        incomingBidAmount = Math.round(+usdPrice.toFixed(2) * Math.pow(10, 6)) * 1e4 / 1e4
        break;
    }

    const selectedTraits = transformNewTask(task?.selectedTraits?.magiceden)
    const bestOffer = (bestOffers[task._id]?.magiceden || 0) * Math.pow(10, decimal);
    const collectionOffer = bidType === "collection" ? incomingBidAmount / Math.pow(10, decimal) : 0;

    if (collectionOffer > bestOffer) {
      // Defensive check to ensure bestOffers[task._id] exists
      if (!bestOffers[task._id]) {
        bestOffers[task._id] = {
          opensea: 0,
          magiceden: 0,
          blur: 0
        };
      }
      bestOffers[task._id].magiceden = collectionOffer;
    }

    if (bidType === "token") {
      // Defensive check to ensure currentTokenIds[task._id] exists
      if (!currentTokenIds[task._id]) {
        currentTokenIds[task._id] = {
          opensea: [],
          magiceden: [],
        };
      }
      const currentTokens = currentTokenIds[task._id].magiceden
      const tokenIds = [...task.tokenIds.filter(id => !isNaN(Number(id))), ...currentTokens];
      const tokenBid = task.bidType === "token" || task.bidType === "rarity"

      if (!tokenBid || !tokenIds.includes(tokenId)) return

      // Fetch floor price (with automatic caching)
      const floor_price = await getCachedFloorPrice(task.contract.contractAddress, 'magiceden');
      if (!floor_price) {
        return;
      }

      const { maxBidPriceEth, minBidPriceEth } = await calculateBidPrice(currentTask, floor_price as number, "magiceden")
      const incomingBidAmountEth = incomingBidAmount / Math.pow(10, decimal)

      if (incomingBidAmountEth < minBidPriceEth) {
        return;
      }
      if (incomingBidAmountEth > maxBidPriceEth) {
        return;
      }

      const cachedOrder = await orderCache.getNewest(task._id, MAGICEDEN.toLowerCase(), tokenId.toString());
      // Filter out expired orders to prevent stale data usage
      const orders = (cachedOrder && cachedOrder.expiresAt > Date.now()) ? [cachedOrder] : [];

      const stopBid = await stopOption(currentTask, 'magiceden', Number(floor_price), true)
      if (stopBid) {
        if (orders.length > 0) {
          const orderKeys = orders.map((order) => order.orderHash)
          const extractedOrderIds = orders.map((order) => order.orderHash);
          await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, walletId: task.walletId, orderKeys: orderKeys, taskId: task._id });
        }
        return;
      }


      // Always counterbid incoming bids (cache-first check removed)

      const roundedEth = roundToValidPrecision(incomingBidAmountEth)

      offerPrice = Math.ceil((magicedenOutbidMargin * Math.pow(10, decimal)) + roundedEth * Math.pow(10, decimal))
      const offerPriceEth = offerPrice / Math.pow(10, decimal)

      if (maxBidPriceEth > 0 && offerPriceEth > maxBidPriceEth) {
        if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
          if (orders.length > 0) {
            const orderKeys = orders.map((order) => order.orderHash)
            const extractedOrderIds = await extractMagicedenOrderHash(orders)
            await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, walletId: task.walletId, orderKeys: orderKeys, taskId: task._id });
          }
        }
        return;
      }

      // Only counterbid if we have an existing order for this token
      const existingTokenOrder = await orderCache.getNewest(task._id.toString(), 'magiceden', tokenId.toString());
      if (!existingTokenOrder) {
        return;
      }

      const tokenOfferPriceForQueue = Math.ceil(Number(offerPrice));

      // Create counterbid job
      const bidData: IMagicedenTokenBidData = {
        taskId: task._id.toString(),
        address: task.wallet,
        contractAddress: task.contract.contractAddress,
        quantity: 1,
        offerPrice: String(tokenOfferPriceForQueue),  // Convert to string to preserve Wei precision
        tokenId: tokenId,
        expiration: expiration,
        walletId: task.walletId,
        slug: task.contract.slug,
        outbidOptions: task.outbidOptions,
        maxBidPriceEth,
        minBidPriceEth,
        enqueuedAt: receivedAt  // Proxy timestamp for true end-to-end latency
      }


      try {
        const counterbidJobId = `CB-${task._id}-me-token-${tokenId}`;  // No price in jobId

        // Check for existing job and update if new price is higher
        const existingJob = await queue.getJob(counterbidJobId);
        if (existingJob) {
          const existingPrice = Number(existingJob.data.offerPrice);
          if (tokenOfferPriceForQueue > existingPrice) {
            await existingJob.updateData({
              ...existingJob.data,
              offerPrice: String(tokenOfferPriceForQueue),  // Convert to string
              maxBidPriceEth: maxBidPriceEth,
              minBidPriceEth: minBidPriceEth,
              enqueuedAt: existingJob.data.enqueuedAt  // Preserve original enqueuedAt timestamp for accurate latency measurement
            });
          } else if (tokenOfferPriceForQueue === existingPrice) {
          } else {
          }
        } else {
          // No existing job - add new one
          await queue.add(
            MAGICEDEN_TOKEN_BID_COUNTERBID,
            bidData,
            {
              jobId: counterbidJobId,
              removeOnComplete: true,
              removeOnFail: true
            }
          );
        }
      } catch (error: any) {
        errorLog('Queue', `Failed to process MagicEden token counterbid`, error);
      }

      // Removed: seenOrderHashes.delete(taskSpecificHash) - let TTL handle expiration to prevent duplicate bids

      if (orders.length > 0) {
        const extractedOrderIds = await extractMagicedenOrderHash(orders)
        const orderKeys = orders.map((order) => order.orderHash)
        await queue.add(CANCEL_MAGICEDEN_BID, {
          orderIds: extractedOrderIds,
          walletId: task.walletId,
          orderKeys,
          taskId: task._id
        });
      }
    }
    else if (bidType === "attribute") {

      const trait = {
        attributeKey: data?.data?.criteria?.data?.attribute?.key,
        attributeValue: data?.data?.criteria?.data?.attribute?.value,
      }

      // Fix: trait-based token tasks have selectedTraits but empty tokenIds
      const hasTraitsForTokenTask = Object.keys(currentTask?.selectedTraits?.magiceden || {}).length > 0;
      const isTokenOrRarityTask = (task.bidType.toLowerCase() === "token" || task.bidType.toLowerCase() === "rarity") &&
        (task.tokenIds?.length > 0 || hasTraitsForTokenTask);

      const hasTraits = checkMagicEdenTrait(selectedTraits, trait)

      const traitIdentifier = `${trait.attributeKey}:${trait.attributeValue}`;

      // Validate trait data is present
      if (!trait.attributeKey || !trait.attributeValue) {
        return
      }

      if (!hasTraits) {
        // For token/rarity tasks, continue to Token-vs-Trait defense logic below
        // For other tasks (collection, trait), exit early since no matching traits
        if (!isTokenOrRarityTask) {
          return
        }
      }

      // Fetch floor price - needed for both trait bid processing AND Token-vs-Trait defense (with automatic caching)
      const floor_price = await getCachedFloorPrice(task.contract.contractAddress, 'magiceden');
      if (!floor_price) return

      const identifier = `${trait.attributeKey}:${trait.attributeValue}`

      // Variables needed for both trait processing and Token-vs-Trait defense
      const { maxBidPriceEth, minBidPriceEth } = await calculateBidPrice(currentTask, floor_price as number, "magiceden")
      const incomingBidAmountEth = incomingBidAmount / Math.pow(10, decimal)

      // =================================================================
      // TRAIT BID PROCESSING - Only for tasks that match the incoming trait
      // =================================================================
      if (hasTraits) {
        if (incomingBidAmountEth < minBidPriceEth) {
          return
        }
        if (incomingBidAmountEth > maxBidPriceEth) {
          return
        }

        const traitPriceKeyExists = currentTask?.stopOptions?.traitPrices && currentTask?.stopOptions?.triggerStopOptions && `magiceden:${identifier}` in currentTask?.stopOptions?.traitPrices;
        const traitFloor = traitPriceKeyExists ? await fetchMagicEdenTraitFloor(currentTask.contract.contractAddress, { attributeKey: trait.attributeKey, attributeValue: trait.attributeValue }) : undefined
        const cachedOrder = await orderCache.getNewest(task._id, MAGICEDEN.toLowerCase(), identifier);
        // Filter out expired orders to prevent stale data usage
        const orders = (cachedOrder && cachedOrder.expiresAt > Date.now()) ? [cachedOrder] : [];

        const stopBid = await stopOption(currentTask, 'magiceden', Number(floor_price), true, traitFloor, identifier)

        if (stopBid) {
          if (orders.length > 0) {
            const orderKeys = orders.map((order) => order.orderHash)
            const extractedOrderIds = await extractMagicedenOrderHash(orders)
            await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, walletId: task.walletId, orderKeys: orderKeys, taskId: task._id });
          }
          return
        }

        // Check marketplace top offer cache first (cache-first approach)
        // This prevents placing bids that won't become the top offer
        // Always counterbid incoming bids (cache-first check removed)

        offerPrice = Math.ceil((magicedenOutbidMargin * Math.pow(10, decimal)) + incomingBidAmount)
        const offerPriceEth = offerPrice / Math.pow(10, decimal)

        if (maxBidPriceEth > 0 && offerPriceEth > maxBidPriceEth) {
          if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
            if (orders.length > 0) {
              const orderKeys = orders.map((order) => order.orderHash)
              const extractedOrderIds = await extractMagicedenOrderHash(orders)
              await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, walletId: task.walletId, orderKeys: orderKeys, taskId: task._id });
            }
          }
          return;
        }

        // Handle collection bids with traits - must be inside hasTraits since it places trait bids
        if (task.bidType === "collection") {
          const bidData: IMagicedenTraitBidData = {
            taskId: task._id,
            bidCount: getIncrementedBidCount(MAGICEDEN, task.contract.slug, task._id),
            address: task.wallet,
            contractAddress: task.contract.contractAddress,
            quantity: 1,
            offerPrice: String(offerPrice),  // Convert to string to preserve Wei precision
            expiration: expiration,
            walletId: task.walletId,
            slug: task.contract.slug,
            trait: JSON.stringify(trait),
            enqueuedAt: receivedAt  // Proxy timestamp for true end-to-end latency
          }

          try {
            const counterbidJobId = `CB-${task._id}-me-trait-${identifier}`;  // No price in jobId

            // Check for existing job and update if new price is higher
            const existingJob = await queue.getJob(counterbidJobId);
            if (existingJob) {
              const existingPrice = existingJob.data.offerPrice;
              if (offerPrice > existingPrice) {
                await existingJob.updateData({
                  ...existingJob.data,
                  offerPrice: String(offerPrice),  // Convert to string to preserve Wei precision
                  enqueuedAt: existingJob.data.enqueuedAt  // Preserve original enqueuedAt timestamp for accurate latency measurement
                });
              } else if (offerPrice === existingPrice) {
              } else {
              }
            } else {
              // No existing job - add new one
              // Record forwarding latency with jobId for execution tracking
              recordForwarding('magiceden', receivedAt, Date.now(), counterbidJobId);

              await queue.add(MAGICEDEN_TRAIT_BID_COUNTERBID, bidData, {
                jobId: counterbidJobId,
                removeOnComplete: true,
                removeOnFail: true
              });
            }
          } catch (queueError: any) {
            errorLog('Queue', `Failed to process MagicEden trait counterbid`, queueError);
          }
        }
      } // END: if (hasTraits) block

      // =================================================================
      // TOKEN-vs-TRAIT DEFENSE - For token/rarity tasks defending against trait offers
      // This runs OUTSIDE the hasTraits block so token tasks can defend their token positions
      // when competitors place trait offers that cover their tokens
      // =================================================================
      // Check if task was stopped during earlier processing (async operations above)
      if (!currentTask.running) {
        return;
      }

      // When a trait offer comes in, check if we have existing token offers on tokens with this trait
      // This ensures we protect our positions on specific tokens when trait-wide offers appear
      const traitVariable: CollectionItemsQueryVariables = {
        collectionSlug: task.contract.slug,
        filter: {
          attributes: [{
            traitType: trait.attributeKey,
            values: [trait.attributeValue]
          }]
        },
        limit: 100,
        sort: { by: 'PRICE', direction: 'ASC' }
      };

      try {
        const tokensWithTrait = await fetchOpenSeaCollectionItems(traitVariable);
        let tokenCounterbidsQueued = 0;

        // Check if we have existing token offers on any of these tokens
        for (const tokenId of tokensWithTrait) {
          const cachedTokenOrder = await orderCache.getNewest(task._id, MAGICEDEN.toLowerCase(), tokenId);

          if (cachedTokenOrder) {
            // We have an existing offer on this token - queue instant counterbid
            // Use incoming trait offer as base price
            let basePriceWei = incomingBidAmount; // incoming trait offer in wei

            // Calculate offer price with crossTypeMarginMultiplier
            const marginMultiplier = currentTask.outbidOptions.crossTypeMarginMultiplier || 1;
            const multipliedMargin = magicedenOutbidMargin * marginMultiplier;
            const tokenOfferPrice = Math.ceil((multipliedMargin * Math.pow(10, decimal)) + basePriceWei);

            // IMPORTANT: Check if the calculated offer price exceeds max bid price
            const tokenOfferPriceEth = tokenOfferPrice / Math.pow(10, decimal);
            if (maxBidPriceEth > 0 && tokenOfferPriceEth > maxBidPriceEth) {
              continue; // Skip this token as it exceeds max bid price
            }

            const tokenBidData: IMagicedenTokenBidData = {
              taskId: task._id.toString(),
              address: task.wallet,
              contractAddress: task.contract.contractAddress,
              quantity: 1,
              offerPrice: String(tokenOfferPrice),  // Convert to string to preserve Wei precision
              expiration: expiration,
              walletId: task.walletId,
              slug: task.contract.slug,
              tokenId: parseInt(tokenId),
              outbidOptions: currentTask.outbidOptions,
              maxBidPriceEth,
              minBidPriceEth,
              enqueuedAt: receivedAt  // For accurate execution time tracking
            };

            try {
              const counterbidJobId = `CB-${task._id}-me-token-${tokenId}`;  // No price in jobId

              // Check for existing job and update if new price is higher
              const existingJob = await queue.getJob(counterbidJobId);
              if (existingJob) {
                const existingPrice = existingJob.data.offerPrice;
                if (tokenOfferPrice > existingPrice) {
                  await existingJob.updateData({
                    ...existingJob.data,
                    offerPrice: String(tokenOfferPrice),  // Convert to string to preserve Wei precision
                    maxBidPriceEth: maxBidPriceEth,
                    minBidPriceEth: minBidPriceEth,
                    enqueuedAt: existingJob.data.enqueuedAt  // Preserve original enqueuedAt timestamp for accurate latency measurement
                  });
                  tokenCounterbidsQueued++;
                } else if (tokenOfferPrice === existingPrice) {
                } else {
                }
              } else {
                // No existing job - add new one
                await queue.add(
                  MAGICEDEN_TOKEN_BID_COUNTERBID,
                  tokenBidData,
                  {
                    jobId: counterbidJobId,
                    removeOnComplete: true,
                    removeOnFail: true
                  }
                );
                tokenCounterbidsQueued++;
              }
            } catch (queueError: any) {
              errorLog('Queue', `Failed to process MagicEden token counterbid`, queueError);
            }
          }
        }

      } catch (error) {
        errorLog('MagicEden', `Error fetching tokens with trait ${trait.attributeKey}:${trait.attributeValue}`, error);
      }

      // Removed: seenOrderHashes.delete(taskSpecificHash) - let TTL handle expiration to prevent duplicate bids
    }
    else if (bidType === "collection") {
      // Allow all task types to respond to collection offers
      // Process each existing offer individually

      const incomingBidAmountEth = incomingBidAmount / Math.pow(10, decimal);
      // Fix: trait-based token tasks have selectedTraits but empty tokenIds
      const hasTraitsForTokenTask = Object.keys(currentTask?.selectedTraits?.magiceden || {}).length > 0;
      const isTokenOrRarityTask = (task.bidType.toLowerCase() === "token" || task.bidType.toLowerCase() === "rarity") &&
        (task.tokenIds?.length > 0 || hasTraitsForTokenTask);

      // Fetch floor price (with automatic caching)
      const floor_price = await getCachedFloorPrice(task.contract.contractAddress, 'magiceden');
      if (!floor_price) return

      const { maxBidPriceEth, minBidPriceEth } = await calculateBidPrice(currentTask, floor_price as number, "magiceden")

      // Check if incoming bid is within valid range
      if (incomingBidAmountEth < minBidPriceEth) {
        return
      }
      if (incomingBidAmountEth > maxBidPriceEth) {
        const message = `incoming bid ${incomingBidAmountEth} ${currencySymbol} for ${task.contract.slug}: collection is greater than max bid price ${maxBidPriceEth} ${currencySymbol}. Skipping ...`.toUpperCase()
        await logBidError(task._id, "INVALID BID PRICE", message, "skipped", "magiceden");

        // Cancel all orders if configured and price exceeds max
        if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
          const allOrders = await orderCache.getByTaskAndMarketplace(task._id, MAGICEDEN.toLowerCase());
          if (allOrders.length > 0) {
            const orderKeys = allOrders.map((order) => order.orderHash)
            const extractedOrderIds = await extractMagicedenOrderHash(allOrders)
            await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, walletId: task.walletId, orderKeys: orderKeys, taskId: task._id });
          }
        }
        return
      }

      const stopBid = await stopOption(currentTask, 'magiceden', Number(floor_price), true)
      if (stopBid) {
        const allOrders = await orderCache.getByTaskAndMarketplace(task._id, MAGICEDEN.toLowerCase());
        if (allOrders.length > 0) {
          const orderKeys = allOrders.map((order) => order.orderHash)
          const extractedOrderIds = await extractMagicedenOrderHash(allOrders)
          await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, walletId: task.walletId, orderKeys: orderKeys, taskId: task._id });
        }
        return
      }

      // Fetch ALL orders for this task from cache
      const allTaskOrders = await orderCache.getByTaskAndMarketplace(task._id, MAGICEDEN.toLowerCase());

      // Calculate the new offer price based on incoming bid
      const roundedEth = roundToValidPrecision(incomingBidAmountEth)
      // We'll calculate the actual offer price per order type in the loop below
      const baseOfferPrice = Math.ceil((magicedenOutbidMargin * Math.pow(10, decimal)) + (roundedEth * Math.pow(10, decimal)))
      const baseOfferPriceEth = baseOfferPrice / Math.pow(10, decimal)

      // Check if new offer would exceed max bid price (using base offer price for initial check)
      if (maxBidPriceEth > 0 && baseOfferPriceEth > maxBidPriceEth) {
        const message = `counter Offer ${baseOfferPriceEth} ${currencySymbol} for ${task.contract.slug} exceeds max bid price ${maxBidPriceEth} ${currencySymbol}.Skipping ...`.toUpperCase()
        await logBidError(task._id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "magiceden");

        if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
          // allTaskOrders is already filtered by task._id from cache
          if (allTaskOrders.length > 0) {
            const orderKeys = allTaskOrders.map((order) => order.orderHash)
            const extractedOrderIds = await extractMagicedenOrderHash(allTaskOrders)
            await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, walletId: task.walletId, orderKeys: orderKeys, taskId: task._id });
          }
        }
        return;
      }

      // Process each existing order
      let counterbidsQueued = 0;
      const ordersToCancel = [];

      for (const order of allTaskOrders) {
        // Always counterbid incoming bids (cache-first check removed)

        // Only counterbid orders that are <= incoming collection offer
        if (Number(order.offerPrice) <= incomingBidAmount) {
          const bidCount = getIncrementedBidCount(MAGICEDEN, currentTask.contract.slug, currentTask._id)

          // Calculate offer price based on order type
          let orderOfferPrice: number;
          if (order.identifier === 'collection') {
            // Collection vs collection: use normal margin
            orderOfferPrice = baseOfferPrice;
          } else {
            // For trait/token orders: use max(collection offer, cached/fetched top offer) + margin
            // This ensures we bid above the actual top offer, not just above the collection offer
            let basePriceEth = roundedEth; // incoming collection offer
            let cachedOfferForOrder = null;

            // Fetch from API to get current top offer
            {
              try {
                let topOffer: OpenseaOfferData | undefined;
                if (order.identifier.includes(':')) {
                  // Trait offer
                  const [attributeKey, attributeValue] = order.identifier.split(':');
                  const [fetchedOffer] = await fetchMagicEdenOffer("TRAIT", task.contract.contractAddress, JSON.stringify({ attributeKey, attributeValue })) as unknown as OpenseaOfferData[];
                  topOffer = fetchedOffer;
                } else {
                  // Token offer
                  const [fetchedOffer] = await fetchMagicEdenOffer("TOKEN", task.contract.contractAddress, order.identifier) as unknown as OpenseaOfferData[];
                  topOffer = fetchedOffer;
                }

                const topOfferEth = topOffer?.eth || 0;
                const walletsArr = await walletCache.getAllWallets();
                const topBidderOwner = topOffer?.owner?.toLowerCase() || '';
                const isOurBid = walletsArr.map(w => w.address.toLowerCase()).includes(topBidderOwner);

                cachedOfferForOrder = {
                  topOfferAmount: topOfferEth,
                  topOfferWei: (topOfferEth * Math.pow(10, decimal)).toString(),
                  topBidderAddress: topBidderOwner,
                  isOurBid: isOurBid,
                  timestamp: Date.now()
                };
              } catch (fetchError) {
                errorLog('Counterbid', `[ME] ${task.contract.slug} [${order.identifier}] | Trait/Token-vs-Collection: API fetch failed - using collection offer as base`, fetchError);
              }
            }

            if (cachedOfferForOrder && cachedOfferForOrder.topOfferAmount > basePriceEth && !cachedOfferForOrder.isOurBid) {
              basePriceEth = cachedOfferForOrder.topOfferAmount;
            }
            // Collection vs trait/token: use configurable margin multiplier
            const marginMultiplier = currentTask.outbidOptions.crossTypeMarginMultiplier || 1;
            const multipliedMargin = magicedenOutbidMargin * marginMultiplier;
            orderOfferPrice = Math.ceil((multipliedMargin * Math.pow(10, decimal)) + (basePriceEth * Math.pow(10, decimal)));
          }

          // Check if the calculated offer price exceeds max bid price
          const orderOfferPriceEth = orderOfferPrice / Math.pow(10, decimal);
          if (maxBidPriceEth > 0 && orderOfferPriceEth > maxBidPriceEth) {
            continue; // Skip this order as it exceeds max bid price
          }

          if (order.identifier.includes(':')) {
            // This is a trait offer
            const [attributeKey, attributeValue] = order.identifier.split(':');
            const trait = { attributeKey, attributeValue };

            const counterbidData: IMagicedenTraitBidData = {
              taskId: task._id,
              bidCount,
              address: task.wallet,
              contractAddress: task.contract.contractAddress,
              quantity: 1,
              offerPrice: String(orderOfferPrice),  // Convert to string to preserve Wei precision
              expiration: expiration,
              walletId: task.walletId,
              slug: task.contract.slug,
              trait: JSON.stringify(trait),
              enqueuedAt: receivedAt  // Proxy timestamp for true end-to-end latency
            }

            try {
              const counterbidJobId = `CB-${task._id}-me-trait-${order.identifier}-${orderOfferPrice}`;
              // Record forwarding latency with jobId for execution tracking
              recordForwarding('magiceden', receivedAt, Date.now(), counterbidJobId);

              await queue.add(MAGICEDEN_TRAIT_BID_COUNTERBID, counterbidData, {
                jobId: counterbidJobId,
                removeOnComplete: true,
                removeOnFail: true
              });
              counterbidsQueued++;
              ordersToCancel.push(order);
            } catch (queueError: any) {
              if (!queueError?.message?.includes('Job with this id already exists')) {
                errorLog('Queue', `Failed to queue MagicEden trait counterbid`, queueError);
              }
            }

          } else if (order.identifier === 'collection') {
            // This is a collection offer
            const bidData: IMagicedenCollectionBidData = {
              taskId: task._id.toString(),
              bidCount,
              address: task.wallet,
              contractAddress: task.contract.contractAddress,
              quantity: 1,
              offerPrice: String(orderOfferPrice),  // Convert to string to preserve Wei precision
              expiration: expiration,
              walletId: task.walletId,
              slug: task.contract.slug,
              currency,
              enqueuedAt: receivedAt  // Proxy timestamp for true end-to-end latency
            }

            try {
              const counterbidJobId = `CB-${task._id}-me-coll-${task.contract.slug}`;  // No price in jobId

              // Check for existing job and update if new price is higher
              const existingJob = await queue.getJob(counterbidJobId);
              if (existingJob) {
                const existingPrice = existingJob.data.offerPrice;
                if (orderOfferPrice > existingPrice) {
                  await existingJob.updateData({
                    ...existingJob.data,
                    offerPrice: String(orderOfferPrice),  // Convert to string to preserve Wei precision
                    enqueuedAt: existingJob.data.enqueuedAt  // Preserve original enqueuedAt timestamp for accurate latency measurement
                  });
                  counterbidsQueued++;
                  ordersToCancel.push(order);
                } else if (orderOfferPrice === existingPrice) {
                } else {
                }
              } else {
                // No existing job - add new one
                // Record forwarding latency with jobId for execution tracking
                recordForwarding('magiceden', receivedAt, Date.now(), counterbidJobId);

                await queue.add(
                  MAGICEDEN_COLLECTION_BID_COUNTERBID,
                  bidData,
                  {
                    jobId: counterbidJobId,
                    removeOnComplete: true,
                    removeOnFail: true
                  }
                );
                counterbidsQueued++;
                ordersToCancel.push(order);
              }
            } catch (queueError: any) {
              errorLog('Queue', `Failed to process MagicEden collection counterbid`, queueError);
            }

          } else {
            // This is a token offer
            const tokenId = order.identifier;

            const bidData: IMagicedenTokenBidData = {
              taskId: task._id.toString(),
              address: task.wallet,
              contractAddress: task.contract.contractAddress,
              quantity: 1,
              offerPrice: String(orderOfferPrice),  // Convert to string to preserve Wei precision
              expiration: expiration,
              walletId: task.walletId,
              slug: task.contract.slug,
              tokenId: parseInt(tokenId),
              outbidOptions: currentTask.outbidOptions,
              maxBidPriceEth,
              minBidPriceEth
            }


            try {
              const counterbidJobId = `CB-${task._id}-me-token-${tokenId}`;  // No price in jobId

              // Check for existing job and update if new price is higher
              const existingJob = await queue.getJob(counterbidJobId);
              if (existingJob) {
                const existingPrice = existingJob.data.offerPrice;
                if (orderOfferPrice > existingPrice) {
                  await existingJob.updateData({
                    ...existingJob.data,
                    offerPrice: String(orderOfferPrice),  // Convert to string to preserve Wei precision
                    maxBidPriceEth: maxBidPriceEth,
                    minBidPriceEth: minBidPriceEth,
                    enqueuedAt: existingJob.data.enqueuedAt  // Preserve original enqueuedAt timestamp for accurate latency measurement
                  });
                  counterbidsQueued++;
                  ordersToCancel.push(order);
                } else if (orderOfferPrice === existingPrice) {
                } else {
                }
              } else {
                // No existing job - add new one
                await queue.add(
                  MAGICEDEN_TOKEN_BID_COUNTERBID,
                  bidData,
                  {
                    jobId: counterbidJobId,
                    removeOnComplete: true,
                    removeOnFail: true
                  }
                );
                counterbidsQueued++;
                ordersToCancel.push(order);
              }
            } catch (queueError: any) {
              errorLog('Queue', `Failed to process MagicEden token counterbid`, queueError);
            }
          }
        } else {
        }
      }

      if (counterbidsQueued > 0) {

        // Cancel the orders that we're counterbidding
        if (ordersToCancel.length > 0) {
          const orderKeys = ordersToCancel.map((order) => order.orderHash)
          const extractedOrderIds = await extractMagicedenOrderHash(ordersToCancel)
          await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, walletId: task.walletId, orderKeys: orderKeys, taskId: task._id });
        }
      }

      // =================================================================
      // TOKEN-vs-COLLECTION DEFENSE - For token/rarity tasks defending against collection offers
      // Only defend tokens where we have existing bids in orderCache
      // =================================================================
      if ((task.bidType.toLowerCase() === "token" || task.bidType.toLowerCase() === "rarity") && task.tokenIds?.length > 0) {
        // Check if task was stopped during earlier processing (async operations above)
        if (!currentTask.running) {
          return;
        }
        let tokenCollectionCounterbidsQueued = 0;

        for (const targetTokenId of task.tokenIds) {
          // Check if we already have an order for this token (already processed above)
          const existingOrder = allTaskOrders.find(o => o.identifier === targetTokenId.toString());
          if (existingOrder) {
            // Already processed in the loop above
            continue;
          }

          // Only defend tokens where we have an existing bid in orderCache
          const existingTokenOrder = await orderCache.getNewest(task._id.toString(), 'magiceden', targetTokenId.toString());
          if (!existingTokenOrder) {
            continue; // Skip tokens we don't have bids on
          }

          // Check our current token offer price
          const ourTokenPrice = await orderCache.getOfferPrice(task._id, 'magiceden', targetTokenId.toString()) || 0;
          const ourTokenPriceEth = ourTokenPrice / Math.pow(10, decimal);

          // Only counterbid if incoming collection offer beats our token offer
          if (incomingBidAmountEth > ourTokenPriceEth) {
            // Use incoming collection offer as base price
            let basePriceEth = roundedEth; // incoming collection offer

            // Calculate token counterbid price with crossTypeMarginMultiplier
            const marginMultiplier = currentTask.outbidOptions.crossTypeMarginMultiplier || 1;
            const multipliedMargin = magicedenOutbidMargin * marginMultiplier;
            const tokenOfferPrice = Math.ceil((multipliedMargin * Math.pow(10, decimal)) + (basePriceEth * Math.pow(10, decimal)));
            const tokenOfferPriceEth = tokenOfferPrice / Math.pow(10, decimal);

            // Check max bid price constraint
            if (maxBidPriceEth > 0 && tokenOfferPriceEth > maxBidPriceEth) {
              continue;
            }

            const tokenOfferPriceForQueue = Math.ceil(Number(tokenOfferPrice));
            const basePriceWei = Math.ceil(basePriceEth * Math.pow(10, decimal));

            // Create counterbid job
            const tokenBidData: IMagicedenTokenBidData = {
              taskId: task._id.toString(),
              address: task.wallet,
              contractAddress: task.contract.contractAddress,
              quantity: 1,
              offerPrice: String(tokenOfferPriceForQueue),  // Convert to string to preserve Wei precision
              expiration: expiration,
              walletId: task.walletId,
              slug: task.contract.slug,
              tokenId: parseInt(targetTokenId.toString()),
              outbidOptions: currentTask.outbidOptions,
              maxBidPriceEth,
              minBidPriceEth,
              enqueuedAt: receivedAt  // For accurate execution time tracking
            };

            try {
              const counterbidJobId = `CB-${task._id}-me-token-${targetTokenId}-${tokenOfferPriceForQueue}`;
              // Record forwarding latency with jobId for execution tracking
              recordForwarding('magiceden', receivedAt, Date.now(), counterbidJobId);

              await queue.add(MAGICEDEN_TOKEN_BID_COUNTERBID, tokenBidData, {
                jobId: counterbidJobId,
                removeOnComplete: true,
                removeOnFail: true
              });
              tokenCollectionCounterbidsQueued++;
            } catch (queueError: any) {
              // Ignore duplicate job errors - this is expected with deterministic jobIds
              if (queueError?.message?.includes('Job with this id already exists')) {
              } else {
                errorLog('Queue', `Failed to queue token counterbid for collection defense`, queueError);
              }
            }
          }
        }

      }

      // Removed: seenOrderHashes.delete(taskSpecificHash) - let TTL handle expiration to prevent duplicate bids
    }
  } catch (error) {
    errorLog('Counterbid', `ME counterbid handler failed | task=${task._id}`, error);
  }
}

/**
 * Get active (non-expired) order from orderCache
 * Returns null if order doesn't exist or is expired
 *
 * @param taskId - Task ID (string or ObjectId)
 * @param marketplace - Marketplace name (lowercase)
 * @param identifier - Order identifier (tokenId, trait:value, or 'collection')
 * @returns Active order or null
 */
async function getActiveOrder(
  taskId: string | mongoose.Types.ObjectId,
  marketplace: string,
  identifier: string
): Promise<any | null> {
  const order = await orderCache.getNewest(taskId.toString(), marketplace.toLowerCase(), identifier);
  if (!order || order.expiresAt <= Date.now()) {
    return null;
  }
  return order;
}

/**
 * Build list of tokens to defend from task.tokenIds + orderCache tokens
 * Used by TOKEN-vs-TRAIT and TOKEN-vs-COLLECTION defense logic
 *
 * @param task - The bidding task
 * @param currentTask - Current task state (for trait checking)
 * @param allOrders - Optional pre-fetched orders (avoids redundant fetch)
 * @returns Array of token IDs to defend
 */
async function buildTokenDefenseList(
  task: ITask,
  currentTask: ITask,
  allOrders?: any[]
): Promise<string[]> {
  const tokenList: string[] = [];

  // Add manual token IDs
  if (task.tokenIds?.length > 0) {
    tokenList.push(...task.tokenIds.map(id => id.toString()));
  }

  // Add trait-based tokens from orderCache
  const hasTraitsForTokenTask = Object.keys(currentTask?.selectedTraits?.opensea || {}).length > 0;
  if (hasTraitsForTokenTask && tokenList.length === 0) {
    // Use provided allOrders or fetch fresh
    const orders = allOrders || await orderCache.getByTaskAndMarketplace(task._id, OPENSEA.toLowerCase());

    // Use Set to deduplicate identifiers (multiple orders per identifier now supported)
    const uniqueTokens = new Set<string>();
    for (const order of orders) {
      if (order.identifier && order.identifier !== 'collection' && !order.identifier.includes(':')) {
        uniqueTokens.add(order.identifier);
      }
    }
    tokenList.push(...Array.from(uniqueTokens));
  }

  return tokenList;
}

async function handleOpenseaCounterbid(data: any, task: ITask, receivedAt: number) {
  try {
    // ============================================================
    // PHASE 1: MINIMAL EXTRACTION - Extract only what's needed for early checks
    // ============================================================
    const orderHash = data?.payload?.payload?.order_hash;
    const maker = data?.payload?.payload?.maker?.address?.toLowerCase();
    const eventType = data.event;  // item_received_bid, trait_offer, collection_offer

    // ============================================================
    // PHASE 2: DEDUP CHECK - EXIT EARLIEST (prevents duplicate processing)
    // ============================================================
    // CRITICAL: Check dedup FIRST before any processing
    // Use atomic check-and-add to prevent race conditions in concurrent handlers
    const taskSpecificHash = `${orderHash}-${task._id}`;
    const dedupPassed = markSeenIfNew(seenOrderHashes, taskSpecificHash);
    if (!dedupPassed) {
      incrementFilteredCounter('dedupBlocked');

      // Enhanced logging for own wallet dedup blocks
      if (maker && isOurWallet(maker)) {
      } else {
      }
      return;  // Exit immediately - already being processed
    }

    // ============================================================
    // PHASE 3: OWN WALLET CHECK - EXIT SECOND (don't counterbid our own bids)
    // ============================================================
    // CRITICAL: Own wallet check AFTER message dedup (prevents counting same bid twice)
    // but BEFORE task validation (ensures own bids are always counted even if task inactive)
    if (maker && isOurWallet(maker)) {
      incrementOwnBidCounter('opensea', orderHash);
      return;  // Exit immediately - don't counterbid our own bids
    }

    // ============================================================
    // PHASE 4: TASK VALIDATION - Check if task is active and OpenSea is enabled
    // ============================================================
    const currentTask = activeTasks.get(task._id)
    if (!currentTask) {
      incrementFilteredCounter('taskNotFound');
      return;
    }

    if (!currentTask?.running) {
      incrementFilteredCounter('taskNotRunning');

      return;
    }

    const taskMarketplaces = currentTask.selectedMarketplaces.map((market) => market.toLowerCase());
    if (!taskMarketplaces.includes("opensea")) {
      incrementFilteredCounter('openseaNotSelected');
      return;
    }

    if (!enabledMarketplaces.map((m) => m.toLowerCase()).includes("opensea")) {
      incrementFilteredCounter('openseaNotEnabled');
      warnLog('MsgDelivery', `CP_NOT_ENABLED | OpenSea globally disabled`);
      return;
    }

    // ============================================================
    // PHASE 5: FULL DATA EXTRACTION - Only extract if message passed all filters
    // ============================================================
    // Checkpoint 10: Log handler entry (only for messages we're actually processing)

    // Log marketplace-to-handler latency if eventTime is available
    if (data.eventTime) {
      const wsLatency = Date.now() - data.eventTime;
    }

    // External bid detected - process counterbid

    const symbol = data?.payload?.payload?.payment_token?.symbol?.toLowerCase();
    const paymentTokenDecimal = Number(data?.payload?.payload?.payment_token?.decimals);

    const expiry = getExpiry(currentTask.bidDuration)
    const outbidOptions = currentTask.outbidOptions

    // OutbidOptions log removed - was logging identical config for EVERY counterbid event (83MB/day)
    // These options don't change per event, already logged at task start

    // Early exit if counterbid disabled
    if (!outbidOptions.counterbid) {
      return;
    }

    const quantity = Number(data?.payload?.payload?.quantity) || 1
    const base_price: number = Number(data?.payload?.payload?.base_price) / quantity;
    const eth_price: number = Number(data?.payload?.payload?.payment_token?.eth_price)
    const usd_price = Number(data?.payload?.payload?.payment_token?.usd_price)
    const etherPrice = usd_price / eth_price

    let incomingBidAmount = 0
    const { currencySymbol, decimal, currencyType } = getCurrencyDetailsFromTask(currentTask, "opensea")
    let similarCurrency: boolean = false

    switch (currencyType.toLowerCase()) {
      case "eth":
        if (symbol === "usdc" || symbol === "usdt") {
          const incomingBidAmountEth = (base_price / Math.pow(10, 6)) / etherPrice
          const roundedEth = roundToValidPrecision(incomingBidAmountEth)
          incomingBidAmount = roundedEth * Math.pow(10, 18)
        } else if (symbol === "weth" || symbol === "eth") {
          incomingBidAmount = base_price
          similarCurrency = true
        }
        break;
      case "usdc":
        if (symbol === "usdc" || symbol === "usdt") {
          incomingBidAmount = base_price
          similarCurrency = true
        } else if (symbol === "weth" || symbol === "eth") {
          incomingBidAmount = (base_price * usd_price) / Math.pow(10, 12)
        }
        break;
    }

    if (!Number.isFinite(incomingBidAmount)) {
      errorLog('MsgDelivery', `CP_CURRENCY_FAIL | slug=${task.contract?.slug} | symbol=${symbol} | etherPrice=${etherPrice} | base_price=${base_price} | incomingBidAmount=${incomingBidAmount} | currencyType=${currencyType}`);
      return;
    }

    const normalizedIncomingBidAmount = Math.ceil(incomingBidAmount);
    if (normalizedIncomingBidAmount !== incomingBidAmount) {
    }
    incomingBidAmount = normalizedIncomingBidAmount;

    const formattedBidAmout = base_price / Math.pow(10, paymentTokenDecimal)
    const bidDetails = `${formattedBidAmout} ${symbol.toUpperCase()}`
    const extraDetails = `${incomingBidAmount / Math.pow(10, decimal)} ${currencySymbol.toUpperCase()}`

    const tokenId = data.event === "item_received_bid" ? data.payload.payload.protocol_data.parameters.consideration?.find((item: any) => item.token.toLowerCase() === task.contract.contractAddress.toLowerCase())?.identifierOrCriteria : null

    // Skip item_received_bid events with no matching consideration item (prevents downstream null access)
    if (data.event === "item_received_bid" && !tokenId) {
      return;
    }

    // Extract trait_criteria with null safety to prevent "Cannot read properties of null (reading 'trait_type')" errors
    // OpenSea can send trait_offer events with null trait_criteria in the payload
    const traitCriteria = data?.payload?.payload?.trait_criteria;
    if (data.event === "trait_offer" && !traitCriteria) {
      return;
    }

    const trait = data.event === "trait_offer" && traitCriteria ? {
      type: traitCriteria.trait_type,
      value: traitCriteria.trait_name
    } : null

    // orderHash already extracted at top for logging
    const detectedBidType = tokenId ? 'token' : (trait ? 'trait' : 'collection');

    // ============================================================
    // PHASE 6: STATE INITIALIZATION & PROCESSING
    // ============================================================

    // ============================================================
    // PHASE 6.1: STATE INITIALIZATION
    // ============================================================
    // Initialize global tracking objects for this task execution
    if (!bestOffers[task._id]) {
      bestOffers[task._id] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      }
    }

    if (!currentTokenIds[task._id]) {
      currentTokenIds[task._id] = {
        opensea: [],
        magiceden: [],
      }
    }

    const incomingBidAmountEth = incomingBidAmount / Math.pow(10, decimal);

    // ============================================================
    // PHASE 6.2: PARALLEL DATA FETCHING
    // ============================================================
    // Fetch collection details + floor price in parallel (saves 300-1000ms)
    // Floor price is cached and reused across all event handlers to prevent duplicate API calls
    // Simplified cache usage - new LRU-based functions handle all caching internally
    const [collectionDetails, cachedFloorPrice] = await Promise.all([
      getCachedCollectionDetails(task.contract.slug, task.contract.contractAddress),
      getCachedFloorPrice(task.contract.slug, 'opensea')
    ]);

    // Convert all creator fees to the IFee format (ensuring values are numbers)
    const creatorFees: IFee = Object.fromEntries(
      Object.entries(collectionDetails.creator_fees).map(([key, value]) => [key, Number(value)])
    );

    // DEBUG: Log fee conversion for counterbid

    let offerPrice: number;
    let collectionOffer: bigint;

    const selectedTraits = transformNewTask(currentTask?.selectedTraits?.opensea)
    const bestOffer = (bestOffers[task._id]?.opensea || 0);

    // ============================================================
    // PHASE 6.2.5: COMMON CALCULATIONS
    // ============================================================
    // Calculate once, reuse across all event handlers (saves 15-150ms per event)

    const decimalDivisor = Math.pow(10, decimal);
    const roundedEth = roundToValidPrecision(incomingBidAmountEth);

    // Calculate bid price range and margins ONCE (saves 15-150ms)
    // All three event types need these values
    // Note: Uses cachedFloorPrice from Phase 6.2 - handlers will validate separately
    let maxBidPriceEth = 0;
    let minBidPriceEth = 0;
    let currency = '';
    let openseaOutbidMargin = 0;
    let marginMultiplier = 1;

    if (cachedFloorPrice) {
      const bidPriceResult = await calculateBidPrice(currentTask, cachedFloorPrice as number, "opensea");
      maxBidPriceEth = bidPriceResult.maxBidPriceEth;
      minBidPriceEth = bidPriceResult.minBidPriceEth;
      currency = bidPriceResult.currency;
      openseaOutbidMargin = calculateOutbidMargin(incomingBidAmountEth, currency);
      marginMultiplier = currentTask.outbidOptions.crossTypeMarginMultiplier || 1;

      // DEBUG: Log cross-type margin calculation (Phase 4 - Priority 2)
    }

    // ============================================================
    // PHASE 6.3: EVENT TYPE ROUTING
    // ============================================================
    // Route to appropriate handler: token (item_received_bid), trait (trait_offer), or collection (collection_offer)
    // Note: Cross-type defense (TOKEN-vs-TRAIT, TOKEN-vs-COLLECTION) is handled inline in each handler

    // Track counterbid activity for collection data warmer
    collectionDataWarmer.trackActivity(task.contract.slug);

    const collectionOfferEth = data.event === "collection_offer" ? incomingBidAmount / Math.pow(10, decimal) : 0

    if (collectionOfferEth > bestOffer) {
      // Defensive check to ensure bestOffers[task._id] exists
      if (!bestOffers[task._id]) {
        bestOffers[task._id] = {
          opensea: 0,
          magiceden: 0,
          blur: 0
        };
      }
      bestOffers[task._id].opensea = collectionOfferEth;
    }

    if (data.event === "item_received_bid") {
      // ============================================================
      // PHASE 6.3.1: TOKEN BID HANDLER (item_received_bid)
      // ============================================================
      // Process token-specific counterbids using orderCache + deterministic job IDs
      // SIMPLIFIED: No complex cache layers - orderCache is single source of truth

      // --- 6.3.1.1: Token Validation ---
      if (!currentTokenIds[task._id]) {
        currentTokenIds[task._id] = {
          opensea: [],
          magiceden: [],
        };
      }
      const currentTokens = currentTokenIds[task._id].opensea
      const tokenIds = [...task.tokenIds, ...currentTokens]

      if (!tokenIds.map((item) => +item).includes(+tokenId)) {
        return;
      }

      // --- 6.3.1.2: Floor Price & Our Order Lookup ---
      // Use floor price fetched in parallel earlier (saves 800-2500ms on cache miss)
      const floor_price = cachedFloorPrice;
      if (!floor_price) {
        return
      }

      // VALIDATION: Ensure floor price is reasonable (not negative, not zero, not suspiciously low)
      if (floor_price <= 0 || floor_price < 0.001) {
        if (process.env.NODE_ENV === 'development') {
          warnLog('Counterbid', `Suspicious floor price detected: ${floor_price} ETH for ${task.contract.slug} - Skipping counterbid`);
        }
        await logBidError(task._id, "INVALID FLOOR PRICE", `Floor price ${floor_price} ETH is invalid or suspiciously low for ${task.contract.slug}:${tokenId}. Skipping counterbid.`, "skipped", "opensea");
        return;
      }

      // --- 6.3.1.3: Price Comparison ---
      // Reuse maxBidPriceEth, minBidPriceEth, currency from Phase 6.2.5
      // Get our current order from orderCache (< 1ms LRU or 10-20ms Redis)
      const ourOrder = await getActiveOrder(task._id, OPENSEA, tokenId.toString());

      // Only counterbid if we have an existing order for this token
      if (!ourOrder) {
        return;
      }

      // Decide: Do we need to counterbid?
      // Compare incoming bid vs our current order price
      const ourCurrentPriceWei = Number(ourOrder.offerPrice);
      if (ourCurrentPriceWei >= incomingBidAmount) {
        return;
      }

      // --- 6.3.1.4: Calculate Counterbid Price ---
      // Calculate new bid price using hoisted values from Phase 6.2.5
      const offerPrice = Math.ceil((openseaOutbidMargin * decimalDivisor) + (roundedEth * decimalDivisor))
      const offerPriceEth = offerPrice / decimalDivisor

      // --- 6.3.1.5: Validate Constraints ---
      // Validate against task limits
      if (incomingBidAmountEth < minBidPriceEth) {
        return
      }

      if (incomingBidAmountEth > maxBidPriceEth) {
        if (currentTask.outbidOptions.cancelMaxWhenOutbid && ourOrder) {
          await queue.add(CANCEL_OPENSEA_BID, {
            walletId: task.walletId,
            orderId: ourOrder.orderHash,
            taskId: task._id,
            identifier: ourOrder.identifier
          });
        }
        return
      }

      // VALIDATION: Ensure calculated bid is not suspiciously different from incoming bid
      const bidDifferenceRatio = Math.abs(offerPriceEth - incomingBidAmountEth) / incomingBidAmountEth;
      if (bidDifferenceRatio > 0.5) {
        return;
      }

      if (offerPriceEth > maxBidPriceEth) {
        if (currentTask.outbidOptions.cancelMaxWhenOutbid && ourOrder) {
          await queue.add(CANCEL_OPENSEA_BID, {
            walletId: task.walletId,
            orderId: ourOrder.orderHash,
            taskId: task._id,
            identifier: ourOrder.identifier
          });
        }
        return;
      }

      const stopBid = await stopOption(currentTask, 'opensea', Number(floor_price), true)
      if (stopBid) {
        if (ourOrder) {
          await queue.add(CANCEL_OPENSEA_BID, {
            walletId: task.walletId,
            orderId: ourOrder.orderHash,
            taskId: task._id,
            identifier: ourOrder.identifier
          });
        }
        return
      }

      // Queue counterbid job (price comparison prevents duplicates)
      const tokenOfferPriceForQueue = Math.ceil(offerPrice);
      const counterbidJobId = `CB-${task._id}-os-token-${tokenId}`;  // No price in jobId

      const asset = {
        contractAddress: task.contract.contractAddress,
        tokenId: tokenId
      }

      const bidData: IProcessOpenseaTokenBidData = {
        _id: task._id,
        address: task.wallet,
        walletId: task.walletId,
        slug: task.contract.slug,
        offerPrice: String(tokenOfferPriceForQueue),  // Convert to string to preserve Wei precision
        creatorFees,
        enforceCreatorFee: collectionDetails.enforceCreatorFee,
        floor_price: floor_price,
        maxBidPriceEth: maxBidPriceEth,
        asset,
        outbidOptions: outbidOptions,
        oldOrderHash: ourOrder.orderHash  // For speed-first cancellation
      }


      // Fire-and-forget: non-blocking queue operation (saves 50-75ms)
      // Check for existing job and update if new price is higher
      queue.getJob(counterbidJobId).then(async (existingJob) => {
        if (existingJob) {
          const existingPrice = existingJob.data.offerPrice;
          if (tokenOfferPriceForQueue > existingPrice) {
            await existingJob.updateData({
              ...existingJob.data,
              offerPrice: String(tokenOfferPriceForQueue),  // Convert to string to preserve Wei precision
              floor_price: floor_price,
              maxBidPriceEth: maxBidPriceEth,
              enqueuedAt: existingJob.data.enqueuedAt  // Preserve original enqueuedAt timestamp for accurate latency measurement
            });
          } else if (tokenOfferPriceForQueue === existingPrice) {
          } else {
          }
        } else {
          // No existing job - add new one
          await queue.add(
            OPENSEA_TOKEN_BID_COUNTERBID,
            bidData,
            {
              jobId: counterbidJobId,
              removeOnComplete: true,
              removeOnFail: true
            }
          );
        }
      }).catch((error: any) => {
        errorLog('Queue', `Failed to process OpenSea token counterbid`, error);
      });
    }

    if (data.event === "trait_offer") {
      // ============================================================
      // PHASE 6.3.2: TRAIT BID HANDLER (trait_offer)
      // ============================================================
      // Process trait-specific counterbids + TOKEN-vs-TRAIT defense

      // --- 6.3.2.1: Trait Validation ---
      const hasTraitsForTokenTask = Object.keys(currentTask?.selectedTraits?.opensea || {}).length > 0;
      const isTokenOrRarityTask = (task.bidType.toLowerCase() === "token" || task.bidType.toLowerCase() === "rarity") &&
        (task.tokenIds?.length > 0 || hasTraitsForTokenTask);
      if (!trait) {
        return
      }

      let traitOffer = BigInt(incomingBidAmount)
      const hasTraits = checkOpenseaTrait(selectedTraits, traitCriteria)
      const identifier = `${trait.type}:${trait.value}`

      if (!hasTraits) {
        // For token/rarity tasks, continue to Token-vs-Trait defense logic below
        // For other tasks (collection, trait), exit early since no matching traits
        if (!isTokenOrRarityTask) {
          return
        }
      }

      // --- 6.3.2.2: Floor Price Lookup ---
      // Use floor price fetched in parallel earlier (saves 800-2500ms on cache miss)
      // Needed for both trait bid processing AND Token-vs-Trait defense
      const floor_price = cachedFloorPrice;
      if (!floor_price) {
        return
      }

      // --- 6.3.2.3: Calculate Bid Price Range ---
      // Reuse maxBidPriceEth, minBidPriceEth, currency, openseaOutbidMargin, roundedEth from Phase 6.2.5

      // =================================================================
      // --- 6.3.2.4: Trait Bid Processing (if hasTraits) ---
      // =================================================================
      // Process counterbid for matching trait offers
      if (hasTraits) {
        const traitPriceKeyExists = currentTask?.stopOptions?.traitPrices && currentTask?.stopOptions?.triggerStopOptions && `opensea:${identifier}` in currentTask?.stopOptions?.traitPrices;
        const traitFloor = traitPriceKeyExists ? await openseaTraitFloor(currentTask.contract.slug, { traitType: trait.type, values: [trait.value] }) : undefined

        // VALIDATION: Ensure floor price is reasonable (not negative, not zero, not suspiciously low)
        if (floor_price <= 0 || floor_price < 0.001) {
          if (process.env.NODE_ENV === 'development') {
            warnLog('Counterbid', `Suspicious floor price detected: ${floor_price} ETH for ${task.contract.slug} trait ${identifier} - Skipping counterbid`);
          }
          await logBidError(task._id, "INVALID FLOOR PRICE", `Floor price ${floor_price} ETH is invalid or suspiciously low for ${task.contract.slug}:${identifier}. Skipping counterbid.`, "skipped", "opensea");
          return;
        }

        const cachedOrder = await orderCache.getNewest(task._id, OPENSEA.toLowerCase(), identifier);
        // Filter out expired orders to prevent stale data usage
        const orders = (cachedOrder && cachedOrder.expiresAt > Date.now()) ? [cachedOrder] : [];

        if (incomingBidAmountEth < minBidPriceEth) {
          return
        } else if (incomingBidAmountEth > maxBidPriceEth) {
          const reason = `is greater than max bid price ${maxBidPriceEth} ${currencySymbol}`;
          const message = `incoming bid ${bidDetails} ${similarCurrency ? '' : `(${extraDetails})`} for ${task.contract.slug}: ${identifier} ${reason}.Skipping ...`.toUpperCase()
          await logBidError(task._id, "INVALID BID PRICE", message, "skipped", "opensea");

          if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
            if (orders.length > 0) {
              const cancelData = orders.map((order, index) => {
                return {
                  name: CANCEL_OPENSEA_BID,
                  data: { walletId: task.walletId, orderId: order.orderHash, taskId: task._id, identifier: order.identifier },
                }
              })
              // Log cancellation enqueue
              await queue.addBulk(cancelData)
            }
          }
          return
        }

        const stopBid = await stopOption(currentTask, 'opensea', Number(floor_price), true, traitFloor, identifier)

        if (stopBid) {
          if (orders.length > 0) {
            const cancelData = orders.map((order, index) => {
              return {
                name: CANCEL_OPENSEA_BID,
                data: { walletId: task.walletId, orderId: order.orderHash, taskId: task._id, identifier: order.identifier },
              }
            })
            // Log cancellation enqueue
            await queue.addBulk(cancelData)
          }
          return
        }

        // SIMPLIFIED: Get our current trait order from orderCache
        const ourOrder = await getActiveOrder(task._id, OPENSEA, identifier);

        // Only counterbid if we have an existing order for this trait
        if (!ourOrder) {
          return;
        }

        // Decide: Do we need to counterbid?
        const ourCurrentPriceWei = Number(ourOrder.offerPrice);
        if (ourCurrentPriceWei >= incomingBidAmount) {
          return;
        }

        offerPrice = Math.ceil((openseaOutbidMargin * decimalDivisor) + (roundedEth * decimalDivisor))
        const offerPriceEth = offerPrice / decimalDivisor

        // VALIDATION: Ensure calculated bid is not suspiciously different from incoming bid
        const bidDifferenceRatio = Math.abs(offerPriceEth - incomingBidAmountEth) / incomingBidAmountEth;
        if (bidDifferenceRatio > 0.5) {
          // If counter bid is >50% different from incoming bid, something is wrong
          const message = `Counter bid ${offerPriceEth} ETH differs significantly from incoming bid ${incomingBidAmountEth} ETH (${(bidDifferenceRatio * 100).toFixed(1)}% difference) for ${task.contract.slug}:${identifier}. Possible calculation error.`.toUpperCase();
          await logBidError(task._id, "SUSPICIOUS BID CALCULATION", message, "skipped", "opensea");
          if (process.env.NODE_ENV === 'development') {
            errorLog('Counterbid', message);
          }
          return;
        }

        if (maxBidPriceEth > 0 && offerPriceEth > maxBidPriceEth) {
          if (currentTask.outbidOptions.cancelMaxWhenOutbid && ourOrder) {
            await queue.add(CANCEL_OPENSEA_BID, {
              walletId: task.walletId,
              orderId: ourOrder.orderHash,
              taskId: task._id,
              identifier: ourOrder.identifier
            });
          }
          return;
        }

        const traitOffer = BigInt(offerPrice)

        // When a trait offer comes in, check if we have existing token offers on tokens with this trait
        // This ensures we protect our positions on specific tokens when trait-wide offers appear
        const traitVariable: CollectionItemsQueryVariables = {
          collectionSlug: task.contract.slug,
          filter: {
            attributes: [{
              traitType: trait.type,
              values: [trait.value]
            }]
          },
          limit: 100,
          sort: { by: 'PRICE', direction: 'ASC' }
        };

        try {
          const tokensWithTrait = await fetchOpenSeaCollectionItems(traitVariable);
          let tokenCounterbidsQueued = 0;

          // Check if we have existing token offers on any of these tokens
          for (const tokenId of tokensWithTrait) {
            const cachedTokenOrder = await orderCache.getNewest(task._id, OPENSEA.toLowerCase(), tokenId);

            if (cachedTokenOrder) {
              // We have an existing offer on this token - queue instant counterbid
              const asset = {
                contractAddress: task.contract.contractAddress,
                tokenId: parseInt(tokenId)
              };

              // SIMPLIFIED: Use incoming trait offer as base price
              const basePriceEth = roundedEth; // incoming trait offer

              // Calculate offer price with crossTypeMarginMultiplier (from Phase 6.2.5)
              const multipliedMargin = openseaOutbidMargin * marginMultiplier;

              // DEBUG: Log TOKEN-vs-TRAIT cross-type margin (Phase 4 - Priority 2)

              const tokenOfferPrice = Math.ceil((multipliedMargin * decimalDivisor) + (basePriceEth * decimalDivisor));

              // IMPORTANT: Check if the calculated offer price exceeds max bid price
              const tokenOfferPriceEth = tokenOfferPrice / Math.pow(10, decimal);
              if (maxBidPriceEth > 0 && tokenOfferPriceEth > maxBidPriceEth) {
                continue; // Skip this token as it exceeds max bid price
              }

              const tokenOfferPriceForQueue = Math.ceil(Number(tokenOfferPrice));
              const tokenBidData: IProcessOpenseaTokenBidData = {
                _id: task._id,
                address: task.wallet,
                walletId: task.walletId,
                slug: task.contract.slug,
                offerPrice: String(tokenOfferPriceForQueue),  // Convert to string to preserve Wei precision
                creatorFees,
                enforceCreatorFee: collectionDetails.enforceCreatorFee,
                floor_price: floor_price,      // Pass to avoid redundant API call
                maxBidPriceEth: maxBidPriceEth, // Pass to avoid redundant calculation
                asset,
                outbidOptions: outbidOptions
              };

              const counterbidJobId = `CB-${task._id}-os-token-${tokenId}`;  // No price in jobId
              // Fire-and-forget: non-blocking queue operation (saves 50-75ms)
              queue.getJob(counterbidJobId).then(async (existingJob) => {
                if (existingJob) {
                  const existingPrice = existingJob.data.offerPrice;
                  if (tokenOfferPriceForQueue > existingPrice) {
                    await existingJob.updateData({
                      ...existingJob.data,
                      offerPrice: String(tokenOfferPriceForQueue),  // Convert to string to preserve Wei precision
                      floor_price: floor_price,
                      maxBidPriceEth: maxBidPriceEth,
                      enqueuedAt: existingJob.data.enqueuedAt  // Preserve original enqueuedAt timestamp for accurate latency measurement
                    });
                    tokenCounterbidsQueued++;
                  } else if (tokenOfferPriceForQueue === existingPrice) {
                  } else {
                  }
                } else {
                  // No existing job - add new one
                  await queue.add(
                    OPENSEA_TOKEN_BID_COUNTERBID,
                    tokenBidData,
                    {
                      jobId: counterbidJobId,
                      removeOnComplete: true,
                      removeOnFail: true
                    }
                  );
                  tokenCounterbidsQueued++;
                }
              }).catch((queueError: any) => {
                errorLog('Queue', `Failed to process OpenSea token counterbid`, queueError);
              });
            }
          }

        } catch (error) {
          errorLog('OpenSea', `Error fetching tokens with trait ${trait.type}:${trait.value}`, error);
        }

        // Handle collection bids with traits (moved inside hasTraits block since it requires trait matching)
        if (task.bidType.toLowerCase() === "collection") {
          const bidCount = getIncrementedBidCount(OPENSEA, task.contract.slug, task._id)

          const traitOfferForQueue = Math.ceil(Number(traitOffer));
          const counterbidData: IProcessOpenseaTraitBidData = {
            _id: task._id,
            bidCount,
            address: task.wallet,
            walletId: task.walletId,
            slug: task.contract.slug,
            offerPrice: String(traitOfferForQueue),  // Convert to string to preserve Wei precision
            creatorFees,
            enforceCreatorFee: collectionDetails.enforceCreatorFee,
            expiry,
            trait: JSON.stringify(trait),
            oldOrderHash: ourOrder.orderHash,  // For speed-first cancellation
            floor_price: cachedFloorPrice  // Pass cached floor price to avoid redundant API call
          }

          try {
            const traitKey = `${trait.type}:${trait.value}`;
            const counterbidJobId = `CB-${task._id}-os-trait-${traitKey}`;  // No price in jobId

            // Check for existing job and update if new price is higher
            const existingJob = await queue.getJob(counterbidJobId);
            if (existingJob) {
              const existingPrice = existingJob.data.offerPrice;
              if (traitOfferForQueue > existingPrice) {
                await existingJob.updateData({
                  ...existingJob.data,
                  offerPrice: String(traitOfferForQueue),  // Convert to string to preserve Wei precision
                  floor_price: cachedFloorPrice,
                  enqueuedAt: existingJob.data.enqueuedAt  // Preserve original enqueuedAt timestamp for accurate latency measurement
                });
              } else if (traitOfferForQueue === existingPrice) {
              } else {
              }
            } else {
              // No existing job - add new one
              // Record forwarding latency with jobId for execution tracking
              recordForwarding('opensea', receivedAt, Date.now(), counterbidJobId);

              await queue.add(
                OPENSEA_TRAIT_BID_COUNTERBID,
                counterbidData,
                {
                  jobId: counterbidJobId,
                  removeOnComplete: true,
                  removeOnFail: true
                }
              );
            }
          } catch (queueError: any) {
            errorLog('Queue', `Failed to process OpenSea trait counterbid`, queueError);
          }
        }
      } // END: if (hasTraits) block

      // =================================================================
      // --- 6.3.2.5: TOKEN-vs-TRAIT Defense ---
      // =================================================================
      // Defend token positions against trait offers (cross-type defense)
      // This runs OUTSIDE the hasTraits block so token tasks can defend their positions
      // Fix: For trait-based token tasks, get tokens from order cache since tokenIds is empty

      // Build list of tokens to defend using helper function (avoids redundant orderCache fetch)
      const taskTokensToDefendForTrait = await buildTokenDefenseList(task, currentTask);

      if ((task.bidType.toLowerCase() === "token" || task.bidType.toLowerCase() === "rarity") && taskTokensToDefendForTrait.length > 0) {
        // Check if task was stopped during earlier processing (async operations above)
        if (!currentTask.running) {
          return;
        }
        let tokenTraitCounterbidsQueued = 0;

        // --- 6.3.2.5: TOKEN-vs-TRAIT Defense - Sequential with Early Filtering ---
        // OPTIMIZATION: Filter tokens BEFORE expensive trait checks to reduce API calls
        // Order: 1) Check order cache (<1ms), 2) Check price (<1ms), 3) Check trait (50-300ms)
        const tokensToDefend: string[] = [];
        let skippedNoOrder = 0;
        let skippedPriceTooLow = 0;
        let checkedTraits = 0;

        for (const targetTokenId of taskTokensToDefendForTrait) {
          try {
            // STEP 1: Check order cache (fast - <1ms)
            const existingTokenOrder = await getActiveOrder(task._id, OPENSEA, targetTokenId.toString());
            if (!existingTokenOrder) {
              skippedNoOrder++;
              continue; // No order, skip trait check
            }

            // STEP 2: Check price comparison (fast - <1ms)
            const ourTokenPrice = await orderCache.getOfferPrice(task._id.toString(), 'opensea', targetTokenId.toString()) || 0;
            const ourTokenPriceEth = ourTokenPrice / decimalDivisor;

            if (incomingBidAmountEth <= ourTokenPriceEth) {
              skippedPriceTooLow++;
              continue; // Incoming price doesn't beat our bid, skip trait check
            }

            // STEP 3: ONLY NOW check trait (expensive - 50-300ms on cache miss)
            checkedTraits++;
            const hasMatchingTrait = await tokenHasTrait(
              task.contract.contractAddress,
              targetTokenId.toString(),
              trait.type,
              trait.value
            );

            if (hasMatchingTrait) {
              tokensToDefend.push(targetTokenId);
            }
          } catch (error) {
            errorLog('OpenSea', `Error checking token ${targetTokenId} for TOKEN-vs-TRAIT defense`, error);
          }
        }


        // Queue counterbids for tokens that passed all checks
        for (const targetTokenId of tokensToDefend) {
          try {
            const existingTokenOrder = await getActiveOrder(task._id, OPENSEA, targetTokenId.toString());
            if (!existingTokenOrder) continue; // Should never happen, already filtered

            // Token has this trait - queue counterbid
            const ourTokenPrice = await orderCache.getOfferPrice(task._id.toString(), 'opensea', targetTokenId.toString()) || 0;
            const ourTokenPriceEth = ourTokenPrice / decimalDivisor;

            // Only counterbid if incoming trait offer beats our token offer (double-check)
            if (incomingBidAmountEth > ourTokenPriceEth) {
              // SIMPLIFIED: Use incoming trait offer as base price
              const basePriceEth = roundedEth; // incoming trait offer

              // Calculate token counterbid price with crossTypeMarginMultiplier (from Phase 6.2.5)
              const multipliedMargin = openseaOutbidMargin * marginMultiplier;

              // DEBUG: Log TOKEN-vs-TRAIT cross-type margin (Phase 4 - Priority 2)

              const tokenOfferPrice = Math.ceil((multipliedMargin * decimalDivisor) + (basePriceEth * decimalDivisor));
              const tokenOfferPriceEth = tokenOfferPrice / decimalDivisor;

              // Check max bid price constraint
              if (maxBidPriceEth > 0 && tokenOfferPriceEth > maxBidPriceEth) {
                continue;
              }

              const tokenOfferPriceForQueue = Math.ceil(Number(tokenOfferPrice));

              // Create token counterbid job (deterministic jobId prevents duplicates)
              const tokenBidData: IProcessOpenseaTokenBidData = {
                _id: task._id,
                address: task.wallet,
                walletId: task.walletId,
                slug: task.contract.slug,
                offerPrice: String(tokenOfferPriceForQueue),  // Convert to string to preserve Wei precision
                creatorFees,
                enforceCreatorFee: collectionDetails.enforceCreatorFee,
                floor_price: floor_price,
                maxBidPriceEth: maxBidPriceEth,
                asset: {
                  contractAddress: task.contract.contractAddress,
                  tokenId: parseInt(targetTokenId.toString())
                },
                outbidOptions: outbidOptions,
                oldOrderHash: existingTokenOrder.orderHash,  // For speed-first cancellation
                enqueuedAt: receivedAt  // For accurate execution time tracking
              };

              const counterbidJobId = `CB-${task._id}-os-token-${targetTokenId}-${tokenOfferPriceForQueue}`;
              // Record forwarding latency with jobId for execution tracking
              recordForwarding('opensea', receivedAt, Date.now(), counterbidJobId);

              // Fire-and-forget: non-blocking queue operation (saves 50-75ms)
              queue.add(OPENSEA_TOKEN_BID_COUNTERBID, tokenBidData, {
                jobId: counterbidJobId,
                removeOnComplete: true,
                removeOnFail: true
              }).then(() => {
                tokenTraitCounterbidsQueued++;
              }).catch((queueError: any) => {
                if (queueError?.message?.includes('Job with this id already exists')) {
                } else {
                  errorLog('Queue', `Failed to queue token counterbid for trait defense`, queueError);
                }
              });
            }
          } catch (error) {
            errorLog('OpenSea', `Error processing token ${targetTokenId} for TOKEN-vs-TRAIT defense`, error);
          }
        }

      }

      // Removed: seenOrderHashes.delete(taskSpecificHash) - let TTL handle expiration to prevent duplicate bids
    }

    if (data.event === "collection_offer") {
      // ============================================================
      // PHASE 6.3.3: COLLECTION BID HANDLER (collection_offer)
      // ============================================================
      // Process collection-wide counterbids + cross-type defenses

      // --- 6.3.3.1: Entry Point & Task Type Check ---
      // Reuse incomingBidAmountEth from top-level calculation


      // Determine if this is a token/rarity task that needs cross-type defense
      const hasTraitsForTokenTask = Object.keys(currentTask?.selectedTraits?.opensea || {}).length > 0;
      const isTokenOrRarityTask = (task.bidType.toLowerCase() === "token" || task.bidType.toLowerCase() === "rarity") &&
        (task.tokenIds?.length > 0 || hasTraitsForTokenTask);

      // --- 6.3.3.2: Floor Price & Bid Range ---
      // OPTIMIZATION: Reuse floor price from Phase 6.2 parallel fetch (saves 500-2500ms)
      const floor_price = cachedFloorPrice;
      if (!floor_price) {
        return;
      }

      // --- 6.3.3.3: Price Validation ---
      // Reuse maxBidPriceEth, minBidPriceEth, currency from Phase 6.2.5

      // [5] MIN PRICE CHECK
      if (incomingBidAmountEth < minBidPriceEth) {
        return;
      } else if (incomingBidAmountEth > maxBidPriceEth) {
        // [6] MAX PRICE CHECK
        const reason = `is greater than max bid price ${maxBidPriceEth} ${currencySymbol}`;
        const message = `incoming bid  ${bidDetails} ${similarCurrency ? '' : `(${extraDetails})`} for ${task.contract.slug}: collection ${reason}. Skipping ...`.toUpperCase()
        await logBidError(task._id, "INVALID BID PRICE", message, "skipped", "opensea");

        // Cancel all orders if configured
        if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
          const allOrders = await orderCache.getByTaskAndMarketplace(task._id, OPENSEA.toLowerCase());
          if (allOrders.length > 0) {
            const cancelData = allOrders.map((order) => {
              return {
                name: CANCEL_OPENSEA_BID,
                data: { walletId: task.walletId, orderId: order.orderHash, taskId: task._id, identifier: order.identifier },
              }
            })
            await queue.addBulk(cancelData)
          }
        }
        return;
      }

      // [7] STOP OPTION CHECK
      const stopBid = await stopOption(currentTask, 'opensea', Number(floor_price), true);
      if (stopBid) {
        const allOrders = await orderCache.getByTaskAndMarketplace(task._id, OPENSEA.toLowerCase());
        if (allOrders.length > 0) {
          const cancelData = allOrders.map((order) => {
            return {
              name: CANCEL_OPENSEA_BID,
              data: { walletId: task.walletId, orderId: order.orderHash, taskId: task._id, identifier: order.identifier },
            }
          })
          await queue.addBulk(cancelData)
        }
        return;
      }

      // --- 6.3.3.4: Fetch All Task Orders ---
      // Fetch ALL orders for this task from cache
      // [8] ORDER CACHE FETCH
      const allTaskOrders = await orderCache.getByTaskAndMarketplace(task._id, OPENSEA.toLowerCase());

      if (allTaskOrders.length === 0) {
        return;  // Early exit - no orders to process
      }

      // [9] MARGIN CALCULATION
      // Reuse openseaOutbidMargin, roundedEth, decimalDivisor from Phase 6.2.5
      const baseOfferPrice = Math.ceil((openseaOutbidMargin * decimalDivisor) + (roundedEth * decimalDivisor));
      const baseOfferPriceEth = baseOfferPrice / decimalDivisor;

      // [10] MAX PRICE VALIDATION (Before Loop)
      if (maxBidPriceEth > 0 && baseOfferPriceEth > maxBidPriceEth) {
        const message = `counter Offer price ${baseOfferPriceEth} ${currencySymbol} for ${task.contract.slug} exceeds max bid price ${maxBidPriceEth} ${currencySymbol} ON OPENSEA.Skipping ...`.toUpperCase()
        await logBidError(task._id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "opensea");

        if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
          // allTaskOrders is already filtered by task._id from cache
          if (allTaskOrders.length > 0) {
            const cancelData = allTaskOrders.map((order) => {
              return {
                name: CANCEL_OPENSEA_BID,
                data: { walletId: task.walletId, orderId: order.orderHash, taskId: task._id, identifier: order.identifier },
              }
            })
            await queue.addBulk(cancelData)
          }
        }
        return;
      }

      // Get wallet address for cache check
      const wallet = await walletCache.getWalletById(task.walletId);
      const walletAddress = wallet?.address || '';

      // [11] ORDER PROCESSING LOOP ENTRY

      // Process each existing order
      let counterbidsQueued = 0;
      for (const order of allTaskOrders) {
        // SIMPLIFIED: Check if we need to counterbid for this order
        // Compare incoming bid vs our current order price from orderCache
        const ourCurrentPriceWei = Number(order.offerPrice);
        const shouldCounterbid = ourCurrentPriceWei < incomingBidAmount;

        if (!shouldCounterbid) {
          continue;
        }

        // [13] OFFER PRICE COMPARISON
        const ourOfferPriceEth = Number(order.offerPrice) / Math.pow(10, decimal);

        if (Number(order.offerPrice) <= incomingBidAmount) {

          const bidCount = getIncrementedBidCount(OPENSEA, task.contract.slug, task._id);
          let orderOfferPrice: number;
          if (order.identifier === 'collection') {
            orderOfferPrice = baseOfferPrice;
          } else {
            // =================================================================
            // --- 6.3.3.7: TRAIT-vs-COLLECTION Defense ---
            // =================================================================
            // Defend trait positions against collection offers (cross-type defense)
            // Scenario: Incoming COLLECTION offer threatens our TRAIT position
            // Defense: Place higher TRAIT bids on our trait offers

            // SIMPLIFIED: Use incoming collection offer as base price (no API fetch needed)
            // We only need to bid above the collection offer to defend our trait position
            const basePriceEth = roundedEth; // incoming collection offer
            // marginMultiplier from Phase 6.2.5
            const multipliedMargin = openseaOutbidMargin * marginMultiplier;

            // DEBUG: Log TRAIT-vs-COLLECTION cross-type margin (Phase 4 - Priority 2)

            orderOfferPrice = Math.ceil((multipliedMargin * decimalDivisor) + (basePriceEth * decimalDivisor));
          }

          // Check if the calculated offer price exceeds max bid price
          const orderOfferPriceEth = orderOfferPrice / Math.pow(10, decimal);
          if (maxBidPriceEth > 0 && orderOfferPriceEth > maxBidPriceEth) {
            continue; // Skip this order as it exceeds max bid price
          }

          if (order.identifier.includes(':')) {
            const [traitType, traitValue] = order.identifier.split(':');
            const trait = { type: traitType, value: traitValue };

            const orderOfferPriceForQueue = Math.ceil(Number(orderOfferPrice));
            const counterbidData: IProcessOpenseaTraitBidData = {
              _id: task._id,
              bidCount,
              address: task.wallet,
              walletId: task.walletId,
              slug: task.contract.slug,
              offerPrice: String(orderOfferPriceForQueue),  // Convert to string to preserve Wei precision
              creatorFees,
              enforceCreatorFee: collectionDetails.enforceCreatorFee,
              expiry,
              trait: JSON.stringify(trait),
              floor_price: cachedFloorPrice,  // Pass cached floor price to avoid redundant API call
              enqueuedAt: receivedAt  // Proxy timestamp for true end-to-end latency
            }

            const traitKey = `${traitType}:${traitValue}`;
            try {
              const counterbidJobId = `CB-${task._id}-os-trait-${traitKey}-${orderOfferPriceForQueue}`;
              // Record forwarding latency with jobId for execution tracking
              recordForwarding('opensea', receivedAt, Date.now(), counterbidJobId);

              await queue.add(
                OPENSEA_TRAIT_BID_COUNTERBID,
                counterbidData,
                {
                  jobId: counterbidJobId,
                  removeOnComplete: true,
                  removeOnFail: true
                }
              );
              counterbidsQueued++;
            } catch (queueError: any) {
              if (!queueError?.message?.includes('Job with this id already exists')) {
                errorLog('Queue', `Failed to queue OpenSea trait counterbid`, queueError);
              } else {
              }
            }
          } else if (order.identifier === 'collection') {
            // [14] COLLECTION ORDER DETECTION
            collectionOffer = BigInt(orderOfferPrice);
            const collectionOfferForQueue = String(collectionOffer);  // Keep as string to preserve Wei precision

            const bidData: IOpenseaBidParams = {
              taskId: task._id,
              bidCount,
              walletAddress: task.wallet,
              walletId: task.walletId,
              slug: task.contract.slug,
              offerPrice: collectionOfferForQueue,  // Now a string
              creatorFees,
              enforceCreatorFee: collectionDetails.enforceCreatorFee,
              expiry,
              floor_price: cachedFloorPrice,  // Pass cached floor price to avoid redundant API call
              enqueuedAt: receivedAt  // Proxy timestamp for true end-to-end latency
            };

            // [15] QUEUE OPERATION
            try {
              const counterbidJobId = `CB-${task._id}-os-coll-${task.contract.slug}`;  // No price in jobId

              // Check for existing job and update if new price is higher
              const existingJob = await queue.getJob(counterbidJobId);
              if (existingJob) {
                const existingPrice = existingJob.data.offerPrice;
                if (collectionOfferForQueue > existingPrice) {
                  await existingJob.updateData({
                    ...existingJob.data,
                    offerPrice: String(collectionOfferForQueue),
                    floor_price: cachedFloorPrice,
                    enqueuedAt: existingJob.data.enqueuedAt  // Preserve original enqueuedAt timestamp for accurate latency measurement
                  });
                  counterbidsQueued++;
                } else if (collectionOfferForQueue === existingPrice) {
                } else {
                }
              } else {
                // No existing job - add new one
                // Record forwarding latency with jobId for execution tracking
                recordForwarding('opensea', receivedAt, Date.now(), counterbidJobId);

                await queue.add(
                  OPENSEA_COLLECTION_BID_COUNTERBID,
                  bidData,
                  {
                    jobId: counterbidJobId,
                    removeOnComplete: true,
                    removeOnFail: true
                  }
                );
                counterbidsQueued++;
              }
            } catch (queueError: any) {
              errorLog('Queue', `Failed to process OpenSea collection counterbid`, queueError);
            }
          } else {
            // This is a token offer
            const tokenId = order.identifier;
            const orderOfferPriceForQueue = Math.ceil(Number(orderOfferPrice));
            const bidData: IProcessOpenseaTokenBidData = {
              _id: task._id,
              address: task.wallet,
              walletId: task.walletId,
              slug: task.contract.slug,
              offerPrice: String(orderOfferPriceForQueue),  // Convert to string to preserve Wei precision
              creatorFees,
              enforceCreatorFee: collectionDetails.enforceCreatorFee,
              floor_price: floor_price,      // Pass to avoid redundant API call
              maxBidPriceEth: maxBidPriceEth, // Pass to avoid redundant calculation
              asset: {
                contractAddress: task.contract.contractAddress,
                tokenId: parseInt(tokenId)
              },
              outbidOptions: outbidOptions,
              enqueuedAt: receivedAt  // Proxy timestamp for true end-to-end latency
            }


            try {
              const counterbidJobId = `CB-${task._id}-os-token-${tokenId}`;  // No price in jobId

              // Check for existing job and update if new price is higher
              const existingJob = await queue.getJob(counterbidJobId);
              if (existingJob) {
                const existingPrice = existingJob.data.offerPrice;
                if (orderOfferPriceForQueue > existingPrice) {
                  await existingJob.updateData({
                    ...existingJob.data,
                    offerPrice: String(orderOfferPriceForQueue),  // Convert to string to preserve Wei precision
                    floor_price: floor_price,
                    maxBidPriceEth: maxBidPriceEth,
                    enqueuedAt: existingJob.data.enqueuedAt  // Preserve original enqueuedAt timestamp for accurate latency measurement
                  });
                  counterbidsQueued++;
                } else if (orderOfferPriceForQueue === existingPrice) {
                } else {
                }
              } else {
                // No existing job - add new one
                await queue.add(
                  OPENSEA_TOKEN_BID_COUNTERBID,
                  bidData,
                  {
                    jobId: counterbidJobId,
                    removeOnComplete: true,
                    removeOnFail: true
                  }
                );
                counterbidsQueued++;
              }
            } catch (queueError: any) {
              errorLog('Queue', `Failed to process OpenSea token counterbid`, queueError);
            }
          }
        }
      }

      // [16] LOOP COMPLETION


      // =================================================================
      // --- 6.3.3.6: TOKEN-vs-COLLECTION Defense ---
      // =================================================================
      // Defend token positions against collection offers (cross-type defense)
      // For token bidType tasks, proactively defend ALL tokens against collection offers
      // Fix: For trait-based token tasks, get tokens from order cache since tokenIds is empty

      // Build list of tokens to defend
      // [17] TOKEN DEFENSE SECTION

      // Build list of tokens to defend using helper function
      // Pass allTaskOrders to avoid redundant fetch (already have the orders)
      const taskTokensToDefend = await buildTokenDefenseList(task, currentTask, allTaskOrders);

      if ((task.bidType.toLowerCase() === "token" || task.bidType.toLowerCase() === "rarity") && taskTokensToDefend.length > 0) {
        // Check if task was stopped during earlier processing (async operations above)
        if (!currentTask.running) {
          return;
        }
        let tokenCollectionCounterbidsQueued = 0;

        // --- 6.3.3.6: TOKEN-vs-COLLECTION Defense - Sequential with Early Filtering ---
        // OPTIMIZATION: Filter tokens BEFORE expensive checks to reduce processing overhead
        // Order: 1) Check already processed, 2) Check order cache (<1ms), 3) Check price (<1ms)
        const tokensToDefendFromCollection: string[] = [];
        let skippedAlreadyProcessed = 0;
        let skippedNoOrder = 0;
        let skippedPriceTooLow = 0;

        for (const targetTokenId of taskTokensToDefend) {
          try {
            // STEP 1: Check if already processed in collection offer loop above (fast - O(n) array search)
            const existingOrder = allTaskOrders.find(o => o.identifier === targetTokenId.toString());
            if (existingOrder) {
              skippedAlreadyProcessed++;
              continue; // Already processed in the loop above
            }

            // STEP 2: Check order cache (fast - <1ms)
            const existingTokenOrder = await orderCache.getNewest(task._id.toString(), 'opensea', targetTokenId.toString());
            if (!existingTokenOrder) {
              skippedNoOrder++;
              continue; // Skip tokens we don't have bids on
            }

            // STEP 3: Check price comparison (fast - <1ms)
            const ourTokenPrice = await orderCache.getOfferPrice(task._id.toString(), 'opensea', targetTokenId.toString()) || 0;
            const ourTokenPriceEth = ourTokenPrice / Math.pow(10, decimal);

            if (incomingBidAmountEth <= ourTokenPriceEth) {
              skippedPriceTooLow++;
              continue; // Incoming price doesn't beat our bid
            }

            // Passed all checks - add to defense list
            tokensToDefendFromCollection.push(targetTokenId);
          } catch (error) {
            errorLog('OpenSea', `Error checking token ${targetTokenId} for TOKEN-vs-COLLECTION defense`, error);
          }
        }


        // Queue counterbids for tokens that passed all checks
        for (const targetTokenId of tokensToDefendFromCollection) {
          try {
            const existingTokenOrder = await orderCache.getNewest(task._id.toString(), 'opensea', targetTokenId.toString());
            if (!existingTokenOrder) continue; // Should never happen, already filtered

            // Only counterbid if incoming collection offer beats our token offer (double-check)
            const ourTokenPrice = await orderCache.getOfferPrice(task._id.toString(), 'opensea', targetTokenId.toString()) || 0;
            const ourTokenPriceEth = ourTokenPrice / Math.pow(10, decimal);

            if (incomingBidAmountEth > ourTokenPriceEth) {
              // SIMPLIFIED: Use incoming collection offer as base price (no API fetch needed)
              // We only need to bid above the collection offer to defend our token position
              const basePriceEth = roundedEth; // incoming collection offer

              // Calculate token counterbid price with crossTypeMarginMultiplier (from Phase 6.2.5)
              const multipliedMargin = openseaOutbidMargin * marginMultiplier;

              // DEBUG: Log TOKEN-vs-COLLECTION cross-type margin (Phase 4 - Priority 2)

              const tokenOfferPrice = Math.ceil((multipliedMargin * decimalDivisor) + (basePriceEth * decimalDivisor));
              const tokenOfferPriceEth = tokenOfferPrice / Math.pow(10, decimal);

              // Check max bid price constraint
              if (maxBidPriceEth > 0 && tokenOfferPriceEth > maxBidPriceEth) {
                continue;
              }

              const tokenOfferPriceForQueue = Math.ceil(Number(tokenOfferPrice));

              // Create token counterbid job (deterministic jobId prevents duplicates)
              const tokenBidData: IProcessOpenseaTokenBidData = {
                _id: task._id,
                address: task.wallet,
                walletId: task.walletId,
                slug: task.contract.slug,
                offerPrice: String(tokenOfferPriceForQueue),  // Convert to string to preserve Wei precision
                creatorFees,
                enforceCreatorFee: collectionDetails.enforceCreatorFee,
                floor_price: floor_price,
                maxBidPriceEth: maxBidPriceEth,
                asset: {
                  contractAddress: task.contract.contractAddress,
                  tokenId: parseInt(targetTokenId.toString())
                },
                outbidOptions: outbidOptions,
                oldOrderHash: existingTokenOrder.orderHash,  // For speed-first cancellation
                enqueuedAt: receivedAt  // For accurate execution time tracking
              };

              const counterbidJobId = `CB-${task._id}-os-token-${targetTokenId}-${tokenOfferPriceForQueue}`;
              // Record forwarding latency with jobId for execution tracking
              recordForwarding('opensea', receivedAt, Date.now(), counterbidJobId);

              // Fire-and-forget: non-blocking queue operation (saves 50-75ms)
              queue.add(OPENSEA_TOKEN_BID_COUNTERBID, tokenBidData, {
                jobId: counterbidJobId,
                removeOnComplete: true,
                removeOnFail: true
              }).then(() => {
                tokenCollectionCounterbidsQueued++;
              }).catch((queueError: any) => {
                if (queueError?.message?.includes('Job with this id already exists')) {
                } else {
                  errorLog('Queue', `Failed to queue token counterbid for collection defense`, queueError);
                }
              });
            }
          } catch (error) {
            errorLog('OpenSea', `Error processing token ${targetTokenId} for TOKEN-vs-COLLECTION defense`, error);
          }
        }

      }

      // Removed: seenOrderHashes.delete(taskSpecificHash) - let TTL handle expiration to prevent duplicate bids
    }
    // Removed: seenOrderHashes.delete(taskSpecificHash) - duplicate deletion removed
  } catch (error) {
    errorLog('OpenSea', `Error in OpenSea counterbid handler | taskId=${task._id.toString()}`, error);
  }
}

async function handleBlurCounterbid(data: any, task: ITask, receivedAt: number) {
  const incomingBid = data

  try {
    // Extract bid info early for logging
    const bidInfo = incomingBid?.stats?.[0] || incomingBid?.updates?.[0] || incomingBid;
    const incomingPrice = bidInfo?.bestPrice || bidInfo?.price || 0;
    const criteriaType = bidInfo?.criteriaType || 'unknown';
    const criteriaValue = bidInfo?.criteriaValue || {};
    const bidTypeDisplay = criteriaType === 'trait' ? `Trait: ${JSON.stringify(criteriaValue)}` : criteriaType === 'token' ? `Token #${criteriaValue?.identifier || 'unknown'}` : 'Collection';

    const currentTask = activeTasks.get(task._id)

    if (!currentTask) {
      return;
    }

    if (!currentTask?.running) {
      return;
    }

    const taskMarketplaces = currentTask?.selectedMarketplaces?.map((marketplace) => marketplace?.toLowerCase()) || [];
    if (!taskMarketplaces.includes("blur")) {
      return;
    }

    const enabledMarketplacesLower = enabledMarketplaces?.map((marketplace) => marketplace?.toLowerCase()) || [];
    if (!enabledMarketplacesLower.includes("blur")) {
      return;
    }

    if (!currentTask?.outbidOptions?.counterbid) {
      return;
    }

    const expiry = getExpiry(currentTask?.bidDuration)
    // Use cached wallet for better performance
    const wallets = await walletCache.getWallet(task.wallet);
    if (!wallets) {
      errorLog('Database', `Wallet not found for task ${task._id}: ${task.wallet}`, new Error('Wallet not found'), { taskId: task._id.toString(), marketplace: 'blur', slug: task.contract.slug });
      return;
    }
    const selectedTraits = transformNewTask(currentTask?.selectedTraits?.blur)
    const traitBid = currentTask?.bidType === "collection" && selectedTraits && Object.keys(selectedTraits)?.length > 0
    const blurOutbidMargin = currentTask?.outbidOptions?.blurOutbidMargin || 0.01


    // Log validation passed

    if (traitBid) {
      const isTraitBid = currentTask?.bidType === "collection" && selectedTraits && Object.keys(selectedTraits)?.length > 0
      if (!isTraitBid) {
        return;
      }

      const traits = transformBlurTraits(selectedTraits)
      const incomingTraitBids = incomingBid?.stats?.filter((item: any) => item?.criteriaType?.toLowerCase() === "trait") || incomingBid?.updates?.filter((item: any) => item?.criteriaType?.toLowerCase() === "trait")

      if (!incomingTraitBids) {
        return;
      }

      const hasMatchingTraits = checkBlurTraits(incomingTraitBids, traits);

      if (!hasMatchingTraits?.length) {
        return;
      }
      const transformedTraits = selectedTraits && Object.keys(selectedTraits)?.length > 0 ? transformOpenseaTraits(selectedTraits) : []

      for (const traitBid of hasMatchingTraits) {
        const trait = JSON.stringify(traitBid?.criteriaValue)
        const incomingPrice = Number(traitBid?.bestPrice || traitBid?.price || 0)
        const traits = JSON.stringify(traitBid?.criteriaValue)

        // Fetch floor price (with automatic caching)
        const floor_price = await getCachedFloorPrice(task?.contract?.slug, 'blur');
        const { maxBidPriceEth, minBidPriceEth } = await calculateBidPrice(task, Number(floor_price), "blur")
        const traitsObj = JSON.parse(trait);
        const [traitType, traitValue] = Object.entries(traitsObj)[0] as string[]
        const identifier = `${traitType}:${traitValue}`

        const incomingTrait = { type: traitType, value: traitValue }
        const activeTrait = transformedTraits?.some(function (trait) {
          return trait?.type?.toLowerCase() === incomingTrait?.type?.toLowerCase() && trait?.value?.toLowerCase() === incomingTrait?.value?.toLowerCase();
        });

        if (!activeTrait) {
          return;
        }

        const totalOffer = incomingPrice + blurOutbidMargin

        const cachedOrder = await orderCache.getNewest(task._id, BLUR.toLowerCase(), identifier);
        // Filter out expired orders to prevent stale data usage
        const orders = (cachedOrder && cachedOrder.expiresAt > Date.now()) ? [cachedOrder] : [];

        if (+totalOffer < minBidPriceEth) {
          return
        } else if (+totalOffer > maxBidPriceEth) {
          const reason = `is greater than max bid price ${maxBidPriceEth} BETH`
          const message = `incoming bid ${incomingPrice} + Blur Outbid Margin(${blurOutbidMargin}) BETH for ${task.contract.slug}: ${identifier} ${reason}. Skipping ...`.toUpperCase()

          if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
            if (orders.length > 0) {
              const cancelData = orders.map((order, index) => {
                try {
                  const payload = JSON.parse(order.orderHash)
                  return {
                    name: CANCEL_BLUR_BID,
                    data: { walletId: task.walletId, payload, taskId: task._id, identifier: order.identifier }
                  }
                } catch (e) {
                  errorLog('Counterbid', `[Blur] Failed to parse orderHash for order ${order.orderHash}`, e);
                  return null;
                }
              }).filter((item): item is NonNullable<typeof item> => item !== null)
              if (cancelData.length > 0) {
                await queue.addBulk(cancelData)
              }
            }
          }

          await logBidError(task._id, "INVALID BID PRICE", message, "skipped", "blur");

          return
        }

        const traitPriceKeyExists = currentTask?.stopOptions?.traitPrices && currentTask?.stopOptions?.triggerStopOptions && `blur:${identifier}` in currentTask?.stopOptions?.traitPrices;
        const traitFloor = traitPriceKeyExists ? await fetchBlurTraitFloor(currentTask?.contract?.slug, { type: traitType, value: traitValue as string }) : undefined

        const stopBid = await stopOption(currentTask, 'blur', Number(floor_price), true, traitFloor, identifier)

        if (stopBid) {

          if (orders.length > 0) {
            const cancelData = orders.map((order, index) => {
              try {
                const payload = JSON.parse(order.orderHash)
                return {
                  name: CANCEL_BLUR_BID,
                  data: { walletId: task.walletId, payload, taskId: task._id, identifier: order.identifier }
                }
              } catch (e) {
                errorLog('Counterbid', `[Blur] Failed to parse orderHash for order ${order.orderHash}`, e);
                return null;
              }
            }).filter((item): item is NonNullable<typeof item> => item !== null)
            if (cancelData.length > 0) {
              await queue.addBulk(cancelData)
            }
          }
          return
        }

        // Always counterbid incoming bids (cache-first check removed)

        let offerPrice: any = Number((blurOutbidMargin + Number(incomingPrice))?.toFixed(2))
        offerPrice = parseEther(offerPrice?.toString())

        const bidCount = getIncrementedBidCount(BLUR, task.contract.slug, task._id)

        const bidData: IBlurBidData = {
          taskId: task._id,
          bidCount: bidCount,
          address: task.wallet,
          walletId: task.walletId,
          contractAddress: task.contract.contractAddress,
          offerPrice: String(offerPrice),  // Convert to string to preserve Wei precision
          slug: task.contract.slug,
          traits,
          expiry,
          enqueuedAt: receivedAt  // Proxy timestamp for true end-to-end latency
        }




        const offerPriceEth = Number(offerPrice) / 1e18;

        try {
          const counterbidJobId = `CB-${task._id}-blur-trait-${identifier}`;  // No price in jobId

          // Check for existing job and update if new price is higher
          const existingJob = await queue.getJob(counterbidJobId);
          if (existingJob) {
            const existingPrice = Number(existingJob.data.offerPrice);
            if (Number(offerPrice) > existingPrice) {
              await existingJob.updateData({
                ...existingJob.data,
                offerPrice: String(offerPrice),  // Convert to string to preserve Wei precision
                enqueuedAt: existingJob.data.enqueuedAt  // Preserve original enqueuedAt timestamp for accurate latency measurement
              });
            } else if (Number(offerPrice) === existingPrice) {
            } else {
            }
          } else {
            // No existing job - add new one
            // Record forwarding latency with jobId for execution tracking
            recordForwarding('blur', receivedAt, Date.now(), counterbidJobId);

            await queue.add(BLUR_TRAIT_BID_COUNTERBID, bidData, {
              jobId: counterbidJobId,
              removeOnComplete: true,
              removeOnFail: true
            });
          }
        } catch (queueError: any) {
          errorLog('Queue', `Failed to process Blur trait counterbid`, queueError);
        }
      }

    } else {
      const incomingBidDetails = incomingBid?.stats?.filter((item: any) => item?.criteriaType?.toLowerCase() === "collection") || incomingBid?.updates?.filter((item: any) => item?.criteriaType?.toLowerCase() === "collection")

      if (!incomingBidDetails || !incomingBidDetails?.length) {
        return
      }

      const incomingBestPrice = incomingBidDetails?.sort((a: any, b: any) => +b?.bestPrice - +a?.bestPrice)[0].bestPrice
      const incomingBidAmount = (incomingBidDetails as any)?.sort((a: any, b: any) => +b.price - +a.price)[0].price

      const bestPrice = +incomingBestPrice > 0 ? incomingBestPrice : +incomingBidAmount > 0 ? incomingBidAmount : "0"

      // Fetch floor price (with automatic caching)
      const floor_price = await getCachedFloorPrice(task.contract.slug, 'blur');
      const { maxBidPriceEth, minBidPriceEth } = await calculateBidPrice(task, Number(floor_price), "blur")

      // Check if incoming bid is within valid range
      if (+bestPrice < minBidPriceEth) {
        return
      } else if (+bestPrice > maxBidPriceEth) {
        const reason = `is greater than max bid price ${maxBidPriceEth} BETH`;
        const message = `incoming bid ${bestPrice} BETH for ${task.contract.slug}: collection ${reason}. Skipping ...`.toUpperCase()
        await logBidError(task._id, "INVALID BID PRICE", message, "skipped", "blur");


        // Cancel all orders if configured
        if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
          const allOrders = await orderCache.getByTaskAndMarketplace(task._id, BLUR.toLowerCase());
          const cancelData = allOrders.map((order) => {
            try {
              const payload = JSON.parse(order.orderHash)
              return {
                name: CANCEL_BLUR_BID,
                data: {
                  walletId: task.walletId,
                  payload,
                  taskId: task._id,
                  identifier: order.identifier
                }
              }
            } catch (e) {
              errorLog('Counterbid', `[Blur] Failed to parse orderHash for order ${order.orderHash}`, e);
              return null;
            }
          }).filter((item): item is NonNullable<typeof item> => item !== null)
          if (cancelData.length > 0) {
            await queue.addBulk(cancelData);
          }
        }
        return
      }

      const stopBid = await stopOption(currentTask, 'blur', Number(floor_price), true)
      if (stopBid) {
        const allOrders = await orderCache.getByTaskAndMarketplace(task._id, BLUR.toLowerCase());
        if (allOrders.length > 0) {
          const cancelData = allOrders.map((order) => {
            try {
              const payload = JSON.parse(order.orderHash)
              return {
                name: CANCEL_BLUR_BID,
                data: {
                  walletId: task.walletId,
                  payload,
                  taskId: task._id,
                  identifier: order.identifier
                }
              }
            } catch (e) {
              errorLog('Counterbid', `[Blur] Failed to parse orderHash for order ${order.orderHash}`, e);
              return null;
            }
          }).filter((item): item is NonNullable<typeof item> => item !== null)
          if (cancelData.length > 0) {
            await queue.addBulk(cancelData);
          }
        }
        return
      }

      // Fetch ALL orders for this task from cache
      const allTaskOrders = await orderCache.getByTaskAndMarketplace(task._id, BLUR.toLowerCase());

      // Calculate incoming price in wei
      const incomingPriceWei = Number(bestPrice) * 1e18;

      // Calculate the new offer price
      const blurOutbidMargin = currentTask?.outbidOptions?.blurOutbidMargin || 0.01
      const rawPrice = (+bestPrice + Number(blurOutbidMargin.toFixed(2))) * 1e18
      // We'll calculate the actual offer price per order type in the loop below
      const baseOfferPrice = BigInt(Math.round(rawPrice / 1e16) * 1e16)

      // Check if base offer price exceeds max bid price
      const baseOfferPriceEth = Number(baseOfferPrice) / 1e18;
      if (maxBidPriceEth > 0 && baseOfferPriceEth > maxBidPriceEth) {
        const message = `counter Offer ${baseOfferPriceEth} BETH for ${task.contract.slug} exceeds max bid price ${maxBidPriceEth} BETH. Skipping ...`.toUpperCase()
        await logBidError(task._id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "blur");


        if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
          // allTaskOrders is already filtered by task._id from cache
          if (allTaskOrders.length > 0) {
            const cancelData = allTaskOrders.map((order) => {
              try {
                const payload = JSON.parse(order.orderHash)
                return {
                  name: CANCEL_BLUR_BID,
                  data: {
                    walletId: task.walletId,
                    payload,
                    taskId: task._id,
                    identifier: order.identifier
                  }
                }
              } catch (e) {
                errorLog('Counterbid', `[Blur] Failed to parse orderHash for order ${order.orderHash}`, e);
                return null;
              }
            }).filter((item): item is NonNullable<typeof item> => item !== null)
            if (cancelData.length > 0) {
              await queue.addBulk(cancelData);
            }
          }
        }
        return;
      }

      // Process each existing order
      let counterbidsQueued = 0;

      for (const order of allTaskOrders) {
        // Always counterbid incoming bids (cache-first check removed)

        // Only counterbid orders that are <= incoming collection offer
        if (Number(order.offerPrice) <= incomingPriceWei) {
          const bidCount = getIncrementedBidCount(BLUR, task.contract.slug, task._id)

          // Always use regular margin for Blur counterbids
          const orderOfferPrice = baseOfferPrice;

          // Check if the calculated offer price exceeds max bid price
          const orderOfferPriceEth = Number(orderOfferPrice) / 1e18;
          if (maxBidPriceEth > 0 && orderOfferPriceEth > maxBidPriceEth) {
            continue; // Skip this order as it exceeds max bid price
          }

          if (order.identifier.includes(':')) {
            // This is a trait offer
            const [traitType, traitValue] = order.identifier.split(':');
            const traits = JSON.stringify({ [traitType]: traitValue });

            const counterbidData: IBlurBidData = {
              taskId: task._id,
              bidCount,
              address: task.wallet,
              walletId: task.walletId,
              contractAddress: task.contract.contractAddress,
              offerPrice: String(orderOfferPrice),  // Convert to string to preserve Wei precision
              slug: task.contract.slug,
              traits,
              expiry,
              enqueuedAt: receivedAt  // Proxy timestamp for true end-to-end latency
            }

            const offerPriceEth = Number(orderOfferPrice) / 1e18;

            try {
              const counterbidJobId = `CB-${task._id}-blur-trait-${order.identifier}`;  // No price in jobId

              // Check for existing job and update if new price is higher
              const existingJob = await queue.getJob(counterbidJobId);
              if (existingJob) {
                const existingPrice = Number(existingJob.data.offerPrice);
                if (Number(orderOfferPrice) > existingPrice) {
                  await existingJob.updateData({
                    ...existingJob.data,
                    offerPrice: String(orderOfferPrice),  // Convert to string to preserve Wei precision
                    enqueuedAt: existingJob.data.enqueuedAt  // Preserve original enqueuedAt timestamp for accurate latency measurement
                  });
                  counterbidsQueued++;
                } else if (Number(orderOfferPrice) === existingPrice) {
                } else {
                }
              } else {
                // No existing job - add new one
                // Record forwarding latency with jobId for execution tracking
                recordForwarding('blur', receivedAt, Date.now(), counterbidJobId);

                await queue.add(BLUR_TRAIT_BID_COUNTERBID, counterbidData, {
                  jobId: counterbidJobId,
                  removeOnComplete: true,
                  removeOnFail: true
                });
                counterbidsQueued++;
              }
            } catch (queueError: any) {
              errorLog('Queue', `Failed to process Blur trait counterbid`, queueError);
            }

          } else if (order.identifier === 'collection') {
            // This is a collection offer
            const bidData: IBlurBidData = {
              taskId: task._id,
              bidCount,
              address: task.wallet,
              walletId: task.walletId,
              contractAddress: task.contract.contractAddress,
              offerPrice: String(orderOfferPrice),  // Convert to string to preserve Wei precision
              slug: task.contract.slug,
              expiry,
              enqueuedAt: receivedAt  // Proxy timestamp for true end-to-end latency
            }

            const offerPriceEthColl = Number(orderOfferPrice) / 1e18;

            try {
              const counterbidJobId = `CB-${task._id}-blur-coll-${task.contract.slug}`;  // No price in jobId

              // Check for existing job and update if new price is higher
              const existingJob = await queue.getJob(counterbidJobId);
              if (existingJob) {
                const existingPrice = Number(existingJob.data.offerPrice);
                if (Number(orderOfferPrice) > existingPrice) {
                  await existingJob.updateData({
                    ...existingJob.data,
                    offerPrice: String(orderOfferPrice),  // Convert to string to preserve Wei precision
                    enqueuedAt: existingJob.data.enqueuedAt  // Preserve original enqueuedAt timestamp for accurate latency measurement
                  });
                  counterbidsQueued++;
                } else if (Number(orderOfferPrice) === existingPrice) {
                } else {
                }
              } else {
                // No existing job - add new one
                // Record forwarding latency with jobId for execution tracking
                recordForwarding('blur', receivedAt, Date.now(), counterbidJobId);

                await queue.add(BLUR_COLLECTION_BID_COUNTERBID, bidData, {
                  jobId: counterbidJobId,
                  removeOnComplete: true,
                  removeOnFail: true
                });
                counterbidsQueued++;
              }
            } catch (queueError: any) {
              errorLog('Queue', `Failed to process Blur collection counterbid`, queueError);
            }
          }
        }
      }

    }

  } catch (error: any) {
    errorLog('Counterbid', `[Blur] Error in counterbid handler | taskId=${task._id} | slug=${task.contract?.slug}`, error);
  }
}

function handleMagicEdenMessages(message: any) {
  let slug, contractAddress;
  try {
    if (
      typeof message === "object" && message.event === "bid.created"
    ) {
      slug = message.payload?.payload?.collection?.slug;
      contractAddress = message.tags?.contract || message.payload?.payload?.asset_contract_criteria?.address;
    } else {
    }
  } catch (error) {
    errorLog('MagicEden', 'Error parsing MagicEden message', error);
  }
  return { contractAddress, slug }
}

function handleOpenSeaMessages(message: any) {
  let contractAddress, slug

  try {
    if (
      typeof message === "object" &&
      (message.event === "item_received_bid" ||
        message.event === "trait_offer" ||
        message.event === "collection_offer"
      )
    ) {
      slug = message.payload?.payload?.collection?.slug;
      contractAddress = message.payload?.payload?.asset_contract_criteria?.address;
    } else {
    }
  } catch (err) {
    errorLog('OpenSea', 'Error parsing OpenSea message', err);
  }
  return { contractAddress, slug }
}



function handleBlurMessages(message: any): { contractAddress?: string, slug?: string } {
  let contractAddress: string | undefined;
  let slug: string | undefined;
  try {
    if (message && message['1'] && message['1'].contractAddress) {
      contractAddress = message['1'].contractAddress;
      // Look up slug from contractToTasksMap since Blur messages don't include slug
      const contractKey = contractAddress!.toLowerCase();
      const tasks = contractToTasksMap.get(contractKey);
      if (tasks && tasks.size > 0) {
        // Get slug from the first matching task
        const firstTask = tasks.values().next().value;
        slug = firstTask?.contract?.slug;
      }
    }
  } catch (error) {
    errorLog('Blur', 'Failed to parse Blur message', error);
  }
  return { contractAddress, slug };
}

function hashBid(data: any): string {
  const stableString = stringify(data) as string;
  return crypto.createHash('sha256')
    .update(stableString, 'utf8')
    .digest('hex');
}

function calculateOutbidMargin(currentPrice: number, currency = WETH): number {
  if (currency === USDC) return 0.01
  if (currentPrice < 0.1) {
    return 0.0001;
  } else if (currentPrice < 1) {
    return 0.001;
  } else {
    return 0.01;
  }
}

function roundToValidPrecision(priceEth: number): number {
  if (priceEth < 0.1) {
    return Number(priceEth.toFixed(4));
  } else if (priceEth < 1) {
    return Number(priceEth.toFixed(3));
  } else {
    return Number(priceEth.toFixed(2));
  }
}

// pQueue removed - BullMQ workers now control concurrency naturally
// Previously created bottleneck: 256 workers competing for 64 pQueue slots
// This caused 49-minute job delays when queue backed up

function getTraitCount(traitsJson: any, attribute: any) {
  const traits = typeof traitsJson === 'string' ? JSON.parse(traitsJson) : traitsJson;
  const { traitType, values } = attribute;

  if (traits.counts && traits.counts[traitType] && traits.counts[traitType][values[0]]) {
    return traits.counts[traitType][values[0]].count;
  } else {
    return `Attribute "${traitType}" with value "${values[0]}" not found in traits.`;
  }
}

/**
 * Performs preprocessing for a specific task and marketplace
 * @param task The task to preprocess
 * @param marketplace The specific marketplace to preprocess for
 * @param availableMarketplaces List of enabled marketplaces for the task
 * @returns Preprocessing data for the specified marketplace
 */
async function performMarketplacePreprocessing(
  task: ITask,
  marketplace: string,
  availableMarketplaces: string[],
  priority?: number
): Promise<TaskPreprocessing> {
  const taskId = task._id;
  const slug = task.contract?.slug || 'unknown';


  // Initialize preprocessing result
  const preprocessing: TaskPreprocessing = {
    bidType: task.bidType as 'token' | 'trait' | 'collection',
    opensea: { enabled: false, traits: [], tokens: [] as string[], jobCount: 0 },
    blur: { enabled: false, traits: [], tokens: [], jobCount: 0 },
    magiceden: { enabled: false, traits: [], tokens: [], jobCount: 0 }
  };

  // Process autoIds for token fetching
  const autoIds = task.tokenIds
    .filter(id => id.toString().toLowerCase().startsWith('bot'))
    .map(id => {
      const matches = id.toString().match(/\d+/);
      return matches ? parseInt(matches[0]) : null;
    })
    .filter(id => id !== null);
  const amount = autoIds[0];

  // Fetch tokens if needed for token bid type
  let openseaTokens: string[] = [];
  let magicedenTokens: number[] = [];
  let traitTokens: string[] = [];

  // Token source tracking for debug logs
  const tokenSources = {
    bottomListings: { opensea: [] as string[], magiceden: [] as number[] },
    manualIds: [] as string[],
    traitTokens: [] as string[],
    combined: { opensea: [] as string[], magiceden: [] as number[] },
    deduplicated: { opensea: [] as string[], magiceden: [] as number[] }
  };

  // Track whether API was called and returned empty
  let openseaApiCalled = false;
  let openseaApiReturnedEmpty = false;
  let magicedenApiCalled = false;
  let magicedenApiReturnedEmpty = false;
  // Track whether trait token fetches failed (transient API error vs genuine zero tokens)
  let hadTraitFetchError = false;

  // Extract manual token IDs (needed for error checking)
  const manualTokenIds = task.tokenIds.filter(id => !isNaN(Number(id)) && !id.toString().toLowerCase().includes('bot')).map(id => id.toString());

  if (task.bidType === "token" || task.bidType === "rarity") {
    // First, fetch bottom listings if autoIds are present
    if (amount) {
      const [openseaListings, magicedenListings] = await Promise.all([
        marketplace === "opensea" && availableMarketplaces.includes("opensea") ?
          fetchOpenseaListings(taskId, task.contract.slug, amount, priority)
            .then(result => {
              openseaApiCalled = true;
              openseaApiReturnedEmpty = result ? result.length === 0 : true;
              tokenSources.bottomListings.opensea = result || [];
              return result || [];
            })
            .catch((err) => {
              return [];
            }) :
          Promise.resolve([]),
        marketplace === "magiceden" && availableMarketplaces.includes("magiceden") ?
          fetchMagicEdenTokens(taskId, task.contract.slug, amount, priority)
            .then(result => {
              magicedenApiCalled = true;
              magicedenApiReturnedEmpty = result ? result.length === 0 : true;
              tokenSources.bottomListings.magiceden = result || [];
              return result || [];
            })
            .catch((err) => {
              return [];
            }) :
          Promise.resolve([])
      ]);

      openseaTokens = [...(openseaListings || [])];
      magicedenTokens = [...(magicedenListings || [])];
    }

    // Add manually specified token IDs
    tokenSources.manualIds = [...manualTokenIds];
    if (manualTokenIds.length > 0) {
    }
    openseaTokens = [...openseaTokens, ...manualTokenIds];
    magicedenTokens = [...magicedenTokens, ...manualTokenIds.map(id => Number(id))];

    // If traits are selected for token bid type, fetch trait-based tokens
    // Fetch once and share between OpenSea and MagicEden
    if ((marketplace === "opensea" || marketplace === "magiceden") &&
      (availableMarketplaces.includes("opensea") || availableMarketplaces.includes("magiceden"))) {
      const openseaFilteredTraits = Object.fromEntries(
        Object.entries(task?.selectedTraits?.opensea || {}).filter(([_, traits]) => traits.length > 0)
      );

      if (Object.keys(openseaFilteredTraits).length > 0) {
        const selectedTraits = transformNewTask(openseaFilteredTraits);
        const attributes = transformSelectedTraitsToAttributes(selectedTraits);
        const expandedAttributes = expandAttributes(attributes);

        // Set task status to PROCESSING when starting trait token fetching
        setTaskStatus(taskId, "PROCESSING", "fetching trait tokens", marketplace);
        const procLoopNum = loopCounterStats[taskId]?.[marketplace] || 1;

        if (task.traitsOperator.value === "or") {
          await Promise.allSettled(
            expandedAttributes.map((attribute) => {
              const variable: CollectionItemsQueryVariables = {
                collectionSlug: task.contract.slug,
                filter: { attributes: [attribute] },
                limit: 100,
                sort: { by: 'PRICE', direction: 'ASC' },
              };

              // Calculate expected count for this trait
              const traitCount = getTraitCount(task.traits, attribute);
              const expectedCount = typeof traitCount === 'number' ? traitCount : 0;

              return fetchOpenSeaCollectionItems(variable, [], undefined, taskId, expectedCount, priority)
                .then((items) => {
                  if (items.length > 0) {
                    traitTokens.push(...items);
                  }
                })
                .catch((error) => {
                  hadTraitFetchError = true;
                  errorLog('Preprocessing', `Failed to fetch trait items for ${task.contract.slug}`, error);
                  // Continue without trait tokens rather than failing completely
                });
            })
          );
        } else if (task.traitsOperator.value === "and") {
          const variable: CollectionItemsQueryVariables = {
            collectionSlug: task.contract.slug,
            filter: { attributes },
            limit: 100,
            sort: { by: 'PRICE', direction: 'ASC' },
          };
          try {
            // Calculate expected count for AND operator (minimum of all trait counts)
            let expectedCount = Infinity;
            for (const attribute of attributes) {
              for (const value of attribute.values) {
                const count = getTraitCount(task.traits, { traitType: attribute.traitType, values: [value] });
                if (typeof count === 'number') {
                  expectedCount = Math.min(expectedCount, count);
                }
              }
            }
            expectedCount = expectedCount === Infinity ? 0 : expectedCount;

            const items = await fetchOpenSeaCollectionItems(variable, [], undefined, taskId, expectedCount, priority);
            if (items.length === 0) {
            }
            traitTokens.push(...items);
          } catch (error) {
            hadTraitFetchError = true;
            errorLog('Preprocessing', `Failed to fetch trait items for ${task.contract.slug}`, error);
            // Continue without trait tokens rather than failing completely
          }
        }

        // Calculate expected trait token count and track missing tokens
        let expectedTraitCount = 0;
        const missingTraits: string[] = [];

        if (task.traitsOperator.value === "or") {
          // For OR operator, sum counts for each individual trait
          for (const attribute of expandedAttributes) {
            const count = getTraitCount(task.traits, attribute);
            if (typeof count === 'number') {
              expectedTraitCount += count;
            } else {
              // Trait not found in counts
              missingTraits.push(`${attribute.traitType}: ${attribute.values[0]}`);
            }
          }
        } else if (task.traitsOperator.value === "and") {
          // For AND operator, we need to calculate the intersection count
          // This is more complex - for now, we'll use the minimum count among all traits
          let minCount = Infinity;
          for (const attribute of attributes) {
            for (const value of attribute.values) {
              const count = getTraitCount(task.traits, { traitType: attribute.traitType, values: [value] });
              if (typeof count === 'number') {
                minCount = Math.min(minCount, count);
              } else {
                missingTraits.push(`${attribute.traitType}: ${value}`);
              }
            }
          }
          expectedTraitCount = minCount === Infinity ? 0 : minCount;
        }

        // Calculate missing trait tokens
        const missingTraitTokenCount = Math.max(0, expectedTraitCount - traitTokens.length);

        if (missingTraitTokenCount > 0 || missingTraits.length > 0) {
          let message = `Expected ${expectedTraitCount} trait tokens but only fetched ${traitTokens.length}.`;
          if (missingTraits.length > 0) {
            message += ` Missing trait data for: ${missingTraits.join(", ")}`;
          }

          // Log missing trait tokens
          await logBidError(
            taskId,
            "Missing trait tokens",
            message,
            "skipped",
            "opensea"
          );

          // Update skipStats for both marketplaces since trait tokens are shared
          if (!skipStats[taskId]) {
            skipStats[taskId] = { opensea: 0, magiceden: 0, blur: 0 };
          }
          // Add defensive check before property access
          if (skipStats[taskId]) {
            skipStats[taskId].opensea = (skipStats[taskId].opensea || 0) + missingTraitTokenCount;
            skipStats[taskId].magiceden = (skipStats[taskId].magiceden || 0) + missingTraitTokenCount;
          } else {
            errorLog('Preprocessing', `skipStats[${taskId}] is undefined when trying to update opensea/magiceden counts`);
          }

        }

        // Track trait tokens for debug logs
        tokenSources.traitTokens = [...traitTokens];
        if (traitTokens.length > 0) {
        }

        // Add trait tokens to both OpenSea and MagicEden lists
        openseaTokens = [...openseaTokens, ...traitTokens];
        magicedenTokens = [...magicedenTokens, ...traitTokens.map(id => Number(id))];

        // Log combined tokens before deduplication
        tokenSources.combined.opensea = [...openseaTokens];
        tokenSources.combined.magiceden = [...magicedenTokens];

        // Reset processing percentage after trait token fetching is complete
        updateTaskProcessingPercentage(taskId, 0);
      }
    }
  }

  // Process the specific marketplace
  if (marketplace === "opensea") {
    preprocessing.opensea.enabled = true;

    // Transform OpenSea traits
    const filteredTraits = Object.fromEntries(
      Object.entries(task?.selectedTraits?.opensea || {}).filter(([_, traits]) => traits.length > 0)
    );
    const selectedTraits = transformNewTask(filteredTraits);
    const traitBid = selectedTraits && Object.keys(selectedTraits).length > 0;

    if (task.bidType === "token" || task.bidType === "rarity") {
      if (openseaTokens.length > 0) {
        // Track duplicates before deduplication
        const originalCount = openseaTokens.length;
        preprocessing.opensea.tokens = [...new Set(openseaTokens)];
        tokenSources.deduplicated.opensea = [...preprocessing.opensea.tokens];
        preprocessing.opensea.jobCount = preprocessing.opensea.tokens.length;

        // Log duplicates if any were removed
        const duplicateCount = originalCount - preprocessing.opensea.tokens.length;
        if (duplicateCount > 0) {
          // Find which tokens were duplicates and their sources
          const tokenCounts = new Map<string, number>();
          openseaTokens.forEach(token => {
            tokenCounts.set(token, (tokenCounts.get(token) || 0) + 1);
          });

          const duplicateTokens: string[] = [];
          const duplicateDetails: { token: string; count: number; sources: string[] }[] = [];
          tokenCounts.forEach((count, token) => {
            if (count > 1) {
              // Identify sources of duplicates
              const sources: string[] = [];
              if (tokenSources.bottomListings.opensea.includes(token)) sources.push('bottomListings');
              if (tokenSources.manualIds.includes(token)) sources.push('manual');
              if (tokenSources.traitTokens.includes(token)) sources.push('traits');
              duplicateDetails.push({ token, count, sources });
              duplicateTokens.push(`${token} (${count}x from ${sources.join('+')})`);
            }
          });

          // Detailed duplicate logging with source attribution

          // Log the duplicate tokens to bid errors for UI visibility
          await logBidError(
            taskId,
            "Duplicate tokens removed",
            `Removed ${duplicateCount} duplicate tokens from OpenSea: ${duplicateTokens.slice(0, 5).join(", ")}${duplicateTokens.length > 5 ? ` ...+${duplicateTokens.length - 5} more` : ''}`,
            "skipped",
            "opensea"
          );

          // Update skipStats
          if (!skipStats[taskId]) {
            skipStats[taskId] = { opensea: 0, magiceden: 0, blur: 0 };
          }
          // Add defensive check before property access
          if (skipStats[taskId]) {
            skipStats[taskId].opensea = (skipStats[taskId].opensea || 0) + duplicateCount;
          } else {
            errorLog('Preprocessing', `skipStats[${taskId}] is undefined when trying to update opensea duplicate count`);
          }

        }

        // If amount was specified, calculate total missing tokens
        if (amount && preprocessing.opensea.tokens.length < amount) {
          const totalMissing = amount - preprocessing.opensea.tokens.length;
          const alreadyAccountedFor = (skipStats[taskId]?.opensea || 0); // Duplicates + missing trait tokens already counted
          const additionalMissing = Math.max(0, totalMissing - alreadyAccountedFor);

          if (additionalMissing > 0) {
            await logBidError(
              taskId,
              "Insufficient tokens for target amount",
              `Target amount: ${amount}, Found: ${preprocessing.opensea.tokens.length}, Additional missing: ${additionalMissing}`,
              "skipped",
              "opensea"
            );

            // Add defensive check before property access
            if (skipStats[taskId]) {
              skipStats[taskId].opensea = (skipStats[taskId].opensea || 0) + additionalMissing;
            } else {
              errorLog('Preprocessing', `skipStats[${taskId}] is undefined when trying to update opensea additional missing count`);
            }
          }
        }
      } else {
        // Only throw error if API was called and returned empty (scenario 6)
        if (openseaApiCalled && openseaApiReturnedEmpty && !manualTokenIds.length && !traitTokens.length) {
          throw new Error(`No listings found on OpenSea for ${task.contract.slug}. Collection may have no active listings.`);
        }
        // If trait fetches failed due to API errors, skip this loop iteration silently —
        // do NOT set skipReason (which would trigger warningBids and stop the task).
        // The next loop iteration will retry the trait fetch.
        if (hadTraitFetchError) {
          preprocessing.opensea.jobCount = 0;
        } else {
          preprocessing.opensea.skipReason = `No tokens found for ${task.contract.slug}`;
          preprocessing.opensea.jobCount = 0;
        }
      }
    } else if (traitBid) {
      // Trait bid - need to check if collection supports trait offers
      const collectionDetails = await getCachedCollectionDetails(task.contract.slug, task.contract.contractAddress);
      if (collectionDetails.trait_offers_enabled) {
        preprocessing.opensea.traits = transformOpenseaTraits(selectedTraits);
        preprocessing.opensea.jobCount = preprocessing.opensea.traits.length;
      } else {
        preprocessing.opensea.skipReason = `Trait bidding not available for ${task.contract.slug}`;
        preprocessing.opensea.jobCount = 0;
        // Track skipped traits in skipStats
        const skippedTraitCount = Object.keys(selectedTraits).reduce((count, key) => {
          return count + (Array.isArray(selectedTraits[key]) ? selectedTraits[key].length : 1);
        }, 0);
        if (skippedTraitCount > 0) {
          if (!skipStats[taskId]) {
            skipStats[taskId] = { opensea: 0, magiceden: 0, blur: 0 };
          }
          skipStats[taskId].opensea += skippedTraitCount;
        }
      }
    } else if (task.bidType === "collection") {
      // Collection bid
      preprocessing.opensea.jobCount = 1;
    }
  } else if (marketplace === "blur") {
    preprocessing.blur.enabled = true;

    // Check if Blur supports this bid type
    if (task.bidType === "token" || task.bidType === "rarity") {
      preprocessing.blur.skipReason = "Token bids not supported on Blur";
      preprocessing.blur.jobCount = 0;
      // Track skipped tokens in skipStats
      const skippedTokenCount = task.tokenIds?.length || 0;
      if (skippedTokenCount > 0) {
        if (!skipStats[taskId]) {
          skipStats[taskId] = { opensea: 0, magiceden: 0, blur: 0 };
        }
        skipStats[taskId].blur += skippedTokenCount;
      }
    } else {
      // Transform Blur traits
      const filteredTraits = Object.fromEntries(
        Object.entries(task?.selectedTraits?.blur || {}).filter(([_, traits]) => traits.length > 0)
      );
      const selectedTraits = transformNewTask(filteredTraits);
      const traitBid = selectedTraits && Object.keys(selectedTraits).length > 0;

      if (traitBid) {
        // Trait bid
        preprocessing.blur.traits = transformBlurTraits(selectedTraits);
        preprocessing.blur.jobCount = preprocessing.blur.traits.length;
      } else if (task.bidType === "collection") {
        // Collection bid
        preprocessing.blur.jobCount = 1;
      }
    }
  } else if (marketplace === "magiceden") {
    preprocessing.magiceden.enabled = true;

    // Transform MagicEden traits
    const filteredTraits = Object.fromEntries(
      Object.entries(task?.selectedTraits?.magiceden || {}).filter(([_, traits]) => traits.length > 0)
    );
    const selectedTraits = transformNewTask(filteredTraits);
    const traitBid = selectedTraits && Object.keys(selectedTraits).length > 0;

    if (task.bidType === "token" || task.bidType === "rarity") {
      if (magicedenTokens.length > 0) {
        // Token bid
        // Track duplicates before deduplication
        const originalCount = magicedenTokens.length;
        preprocessing.magiceden.tokens = [...new Set(magicedenTokens)];
        tokenSources.deduplicated.magiceden = [...preprocessing.magiceden.tokens];
        preprocessing.magiceden.jobCount = preprocessing.magiceden.tokens.length;

        // Log duplicates if any were removed
        const duplicateCount = originalCount - preprocessing.magiceden.tokens.length;
        if (duplicateCount > 0) {
          // Find which tokens were duplicates and their sources
          const tokenCounts = new Map<number, number>();
          magicedenTokens.forEach(token => {
            tokenCounts.set(token, (tokenCounts.get(token) || 0) + 1);
          });

          const duplicateTokens: string[] = [];
          const duplicateDetails: { token: number; count: number; sources: string[] }[] = [];
          tokenCounts.forEach((count, token) => {
            if (count > 1) {
              // Identify sources of duplicates
              const sources: string[] = [];
              if (tokenSources.bottomListings.magiceden.includes(token)) sources.push('bottomListings');
              if (tokenSources.manualIds.includes(String(token))) sources.push('manual');
              if (tokenSources.traitTokens.includes(String(token))) sources.push('traits');
              duplicateDetails.push({ token, count, sources });
              duplicateTokens.push(`${token} (${count}x from ${sources.join('+')})`);
            }
          });

          // Detailed duplicate logging with source attribution

          // Log the duplicate tokens to bid errors for UI visibility
          await logBidError(
            taskId,
            "Duplicate tokens removed",
            `Removed ${duplicateCount} duplicate tokens from MagicEden: ${duplicateTokens.slice(0, 5).join(", ")}${duplicateTokens.length > 5 ? ` ...+${duplicateTokens.length - 5} more` : ''}`,
            "skipped",
            "magiceden"
          );

          // Update skipStats
          if (!skipStats[taskId]) {
            skipStats[taskId] = { opensea: 0, magiceden: 0, blur: 0 };
          }
          skipStats[taskId].magiceden = (skipStats[taskId].magiceden || 0) + duplicateCount;

        }

        // If amount was specified, calculate total missing tokens
        if (amount && preprocessing.magiceden.tokens.length < amount) {
          const totalMissing = amount - preprocessing.magiceden.tokens.length;
          const alreadyAccountedFor = (skipStats[taskId]?.magiceden || 0); // Duplicates + missing trait tokens already counted
          const additionalMissing = Math.max(0, totalMissing - alreadyAccountedFor);

          if (additionalMissing > 0) {
            await logBidError(
              taskId,
              "Insufficient tokens for target amount",
              `Target amount: ${amount}, Found: ${preprocessing.magiceden.tokens.length}, Additional missing: ${additionalMissing}`,
              "skipped",
              "magiceden"
            );

            skipStats[taskId].magiceden = (skipStats[taskId].magiceden || 0) + additionalMissing;
          }
        }
      } else {
        // Only throw error if API was called and returned empty (scenario 6)
        if (magicedenApiCalled && magicedenApiReturnedEmpty && !manualTokenIds.length) {
          throw new Error(`No tokens found on MagicEden for ${task.contract.slug}. Collection may have no active listings.`);
        }
        // If trait fetches failed due to API errors, skip this loop iteration silently —
        // do NOT set skipReason (which would trigger warningBids and stop the task).
        if (hadTraitFetchError) {
          preprocessing.magiceden.jobCount = 0;
        } else {
          preprocessing.magiceden.skipReason = `No tokens found for ${task.contract.slug}`;
          preprocessing.magiceden.jobCount = 0;
        }
      }
    } else if (traitBid) {
      // Trait bid
      preprocessing.magiceden.traits = Object.entries(selectedTraits).flatMap(([key, values]) =>
        values.map(value => ({ attributeKey: key, attributeValue: value }))
      );
      preprocessing.magiceden.jobCount = preprocessing.magiceden.traits.length;
    } else if (task.bidType === "collection") {
      // Collection bid
      preprocessing.magiceden.jobCount = 1;
    }
  }

  if (!currentTraits[taskId]) {
    currentTraits[taskId] = {
      opensea: {},
      magiceden: {},
      blur: {}
    };
  }

  // Convert preprocessed traits back to the format expected by currentTraits
  if (marketplace === "opensea" && preprocessing.opensea.traits.length > 0) {
    const openseaTraitMap: Record<string, string[]> = {};
    preprocessing.opensea.traits.forEach(trait => {
      if (!openseaTraitMap[trait.type]) {
        openseaTraitMap[trait.type] = [];
      }
      if (!openseaTraitMap[trait.type].includes(trait.value)) {
        openseaTraitMap[trait.type].push(trait.value);
      }
    });
    currentTraits[taskId].opensea = openseaTraitMap;
  }

  if (marketplace === "blur" && preprocessing.blur.traits.length > 0) {
    const blurTraitMap: Record<string, string[]> = {};
    preprocessing.blur.traits.forEach(trait => {
      const key = Object.keys(trait)[0];
      const value = trait[key];
      if (!blurTraitMap[key]) {
        blurTraitMap[key] = [];
      }
      if (!blurTraitMap[key].includes(value)) {
        blurTraitMap[key].push(value);
      }
    });
    currentTraits[taskId].blur = blurTraitMap;
  }

  if (marketplace === "magiceden" && preprocessing.magiceden.traits.length > 0) {
    const magicedenTraitMap: Record<string, string[]> = {};
    preprocessing.magiceden.traits.forEach(trait => {
      if (!magicedenTraitMap[trait.attributeKey]) {
        magicedenTraitMap[trait.attributeKey] = [];
      }
      if (!magicedenTraitMap[trait.attributeKey].includes(trait.attributeValue)) {
        magicedenTraitMap[trait.attributeKey].push(trait.attributeValue);
      }
    });
    currentTraits[taskId].magiceden = magicedenTraitMap;
  }

  // Final preprocessing summary with all token tracking
  const mpKey = marketplace === 'opensea' ? 'opensea' : 'magiceden';
  const finalTokenCount = marketplace === 'opensea'
    ? (preprocessing.opensea.tokens?.length || 0)
    : (preprocessing.magiceden.tokens?.length || 0);
  const finalJobCount = marketplace === 'opensea'
    ? (preprocessing.opensea.jobCount || 0)
    : marketplace === 'magiceden'
      ? (preprocessing.magiceden.jobCount || 0)
      : (preprocessing.blur.jobCount || 0);


  if (DEBUG) {
    const mpData = preprocessing[marketplace as keyof TaskPreprocessing];
    const enabled = typeof mpData === 'object' && 'enabled' in mpData ? mpData.enabled : false;
    const jobCount = typeof mpData === 'object' && 'jobCount' in mpData ? mpData.jobCount : 0;
    const skipReason = typeof mpData === 'object' && 'skipReason' in mpData ? mpData.skipReason : undefined;
    console.log(`[Preprocessing] ${slug} | EXIT | marketplace=${marketplace} | enabled=${enabled} | jobCount=${jobCount}${skipReason ? ` | skipReason=${skipReason}` : ''}`);
  }

  return preprocessing;
}

async function processOpenseaScheduledBid(task: ITask) {
  const taskId = task._id.toString();
  const currentLoopCount = loopCounterStats[taskId]?.opensea || 1;


  // Check blacklist at the VERY START to prevent any processing if task is being stopped
  if (isTaskBlacklisted(taskId, 'opensea')) {
    return;
  }

  let lockAcquired = false;

  // Timeout protection wrapper - 900 seconds max processing time (increased for event loop recovery)
  const TIMEOUT_DURATION = 900000; // 15 minutes
  const timeoutPromise = new Promise<never>((_, reject) => {
    setTimeout(() => {
      reject(new Error(`OpenSea scheduled bid timeout after ${TIMEOUT_DURATION}ms for task ${taskId}`));
    }, TIMEOUT_DURATION);
  });

  try {
    // Wrap the main processing logic in a timeout
    await Promise.race([
      (async () => {
        // Start log
        if (!enabledMarketplaces.map((m) => m.toLowerCase()).includes("opensea")) {
          if (DEBUG) {
          }
          return;
        }
        const currentTask = activeTasks.get(task._id);
        if (!currentTask || !currentTask.running || !currentTask.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("opensea")) {
          if (DEBUG) {
          }
          return;
        }

        // Transition from SCHEDULED to STARTED for this marketplace - AFTER validation
        setTaskStatus(taskId, 'STARTED', `opensea scheduled bid starting - Loop #${currentLoopCount}`, 'opensea');

        // Processing OpenSea scheduled bid

        // Get preprocessing data - first check the Map, then perform if needed
        let preprocessing = taskPreprocessing.get(taskId);

        if (!preprocessing || !preprocessing.opensea.enabled) {

          // Determine available marketplaces for this task
          const validMarketplaces = ["opensea", "magiceden", "blur"];
          const availableMarketplaces = currentTask.selectedMarketplaces
            .map(m => m.toLowerCase())
            .filter(m => enabledMarketplaces.map(em => em.toLowerCase()).includes(m))
            .filter(m => validMarketplaces.includes(m));

          // Perform preprocessing for OpenSea (with priority 0 for scheduled jobs)
          preprocessing = await performMarketplacePreprocessing(currentTask, "opensea", availableMarketplaces, 0);

          // Store in Map for other marketplaces to use
          taskPreprocessing.set(taskId, preprocessing);

          // If preprocessing didn't enable OpenSea, it means something went wrong
          if (!preprocessing.opensea.enabled) {
            if (DEBUG) {
            }
            errorLog('OpenSea', `Preprocessing failed to enable OpenSea | taskId=${taskId}`);
            throw new Error(`Preprocessing failed to enable OpenSea for task ${taskId}`);
          }
        }

        if (preprocessing.opensea.skipReason) {
          if (DEBUG) {
          }
          if (!warningBids[taskId]) {
            warningBids[taskId] = { opensea: false, magiceden: false, blur: false };
          }
          if (!warningMessage[taskId]) {
            warningMessage[taskId] = { opensea: "", magiceden: "", blur: "" };
          }
          warningBids[taskId].opensea = true;
          warningMessage[taskId].opensea = preprocessing.opensea.skipReason;
          return;
        }

        // Ensure task status exists in centralized state manager
        // Initialize task status if it doesn't exist or check for completion state
        if (!taskStatus[taskId]) {
          const activeTask = activeTasks.get(taskId);
          taskStatus[taskId] = {
            selectedMarketplaces: activeTask?.selectedMarketplaces.map(m => m.toLowerCase()) || [],
            lastUpdate: Date.now(),
            marketplaces: {}
          };
        }

        // Use timed lock manager for better timeout handling
        try {
          lockAcquired = timedLockManager.acquireLock(taskId, 'opensea');
          if (!lockAcquired) {
            if (DEBUG) {
            }
            return;
          }
        } catch (lockError) {
          errorLog('Lock', `Failed to acquire lock for task ${taskId}`, lockError);

          // If lock acquisition fails due to corruption, attempt cleanup
          if (lockError instanceof Error && lockError.message.includes('Missing lock')) {
            try {
              // Clean up any stale locks
              timedLockManager.releaseLock(taskId, 'opensea');
              // Try once more
              lockAcquired = timedLockManager.acquireLock(taskId, 'opensea');
              if (lockAcquired) {
              } else {
                throw new Error('Lock recovery failed');
              }
            } catch (recoveryError) {
              errorLog('Lock', `Failed to recover lock for task ${taskId}`, recoveryError);
              throw lockError; // Re-throw original error
            }
          } else {
            throw lockError;
          }
        }

        // Update currentTokenIds if we have token data
        if (!currentTokenIds[taskId]) {
          currentTokenIds[taskId] = {
            opensea: [],
            magiceden: [],
          }
        }
        currentTokenIds[taskId].opensea = preprocessing.opensea.tokens

        // Initialize warningBids early
        if (!warningBids[taskId]) {
          warningBids[taskId] = {
            opensea: false,
            magiceden: false,
            blur: false
          }
        }

        const WALLET_ADDRESS: string = currentTask.wallet;
        const wallets = await walletCache.getWalletById(currentTask.walletId);

        // Extend lock before fetching balances
        if (lockAcquired) {
          timedLockManager.extendLock(taskId, 'opensea');
        }

        // Fetch fresh WETH balance with timeout protection
        let wethBalance = 0;
        try {
          const { fetchERC20Balance } = await import('./utils/balance');
          const { WETH } = await import('./utils/currencies');
          wethBalance = await fetchERC20Balance(WALLET_ADDRESS, WETH);
        } catch (error) {
          errorLog('OpenSea', 'Failed to fetch WETH balance', error);
        }

        // Get collection details and floor price to populate LRU caches for processors
        let currency, collectionDetails, floorPrice;
        try {
          const currencyDetails = getCurrencyDetailsFromTask(currentTask, "opensea");
          currency = currencyDetails.currency;

          // Fetch and cache general data (applies to all bid types)
          // These calls automatically populate LRU caches for downstream processors
          collectionDetails = await getCachedCollectionDetails(currentTask.contract.slug, currentTask.contract.contractAddress);
          floorPrice = await getCachedFloorPrice(currentTask.contract.slug);


          // Log full collection details object
        } catch (error) {
          errorLog('OpenSea', 'Failed to get collection details or floor price', error);
          throw error;
        }

        // Pre-calculate bid price values ONCE for all processors (counterbid pattern)
        // This avoids redundant calculateBidPrice calls in each processor (15-150ms savings per bid)
        let scheduledBidCalc: {
          maxBidPriceEth: number;
          offerPriceEth: number;
          minBidPriceEth: number;
          currencySymbol: string;
          currency: string;
          decimal: number;
          floor_price: number;
        } | null = null;

        if (preprocessing.opensea.enabled && floorPrice !== undefined) {
          scheduledBidCalc = {
            ...await calculateBidPrice(
              currentTask,
              floorPrice,  // Use floor price fetched above
              "opensea"
            ),
            floor_price: floorPrice
          };
        }

        // creator_fees: JSON.stringify(collectionDetails.creator_fees),
        // has_null: collectionDetails.creator_fees.null !== undefined,
        // null_value: collectionDetails.creator_fees.null,
        // keys: Object.keys(collectionDetails.creator_fees)
        // });

        // Convert all creator fees to the IFee format (ensuring values are numbers)
        const creatorFees: IFee = Object.fromEntries(
          Object.entries(collectionDetails.creator_fees).map(([key, value]) => [key, Number(value)])
        );

        // Create jobs based on preprocessing results
        if (preprocessing.opensea.tokens.length > 0) {
          // Token bid
          const jobs = preprocessing.opensea.tokens
            .map((token) => ({
              name: OPENSEA_TOKEN_BID,
              data: {
                _id: taskId,
                address: WALLET_ADDRESS,
                walletId: task.walletId,
                slug: currentTask.contract.slug,
                creatorFees,
                enforceCreatorFee: collectionDetails.enforceCreatorFee,
                asset: { contractAddress: currentTask.contract.contractAddress, tokenId: token },
                outbidOptions: currentTask.outbidOptions,
                // Pass pre-calculated values to avoid redundant calculateBidPrice calls (15-150ms savings)
                floor_price: scheduledBidCalc?.floor_price,
                maxBidPriceEth: scheduledBidCalc?.maxBidPriceEth,
                minBidPriceEth: scheduledBidCalc?.minBidPriceEth,
                offerPriceEth: scheduledBidCalc?.offerPriceEth,
                currency: scheduledBidCalc?.currency,
                decimal: scheduledBidCalc?.decimal,
                currencySymbol: scheduledBidCalc?.currencySymbol,
              },
              opts: { priority: TOKEN_BID_PRIORITY.OPENSEA, attempts: 1 } as JobsOptions,
            })) as Job[]

          // Ensure parent job exists BEFORE processing jobs
          await ensureParentJobExists(taskId, 'opensea');
          const parentJobId = getParentJobId(taskId, 'opensea');
          const parentJob = parentJobs.get(parentJobId);
          if (parentJob) {
            parentJob.expectedJobs = preprocessing.opensea.tokens.length;
            parentJob.totalJobs = preprocessing.opensea.tokens.length; // Use expected count initially
          }

          try {
            // Extend lock before processing bulk jobs
            if (lockAcquired) {
              timedLockManager.extendLock(taskId, 'opensea');
            }

            // Transition to QUEUEING state before creating jobs
            setTaskStatus(taskId, 'QUEUEING', 'queueing opensea token bid jobs', 'opensea');
            const loopNum = loopCounterStats[taskId]?.['opensea'] || 1;

            // Queue jobs directly with queue.addBulk
            const preparedJobs = jobs.map(job => ({
              name: job.name,
              data: job.data,
              opts: {
                jobId: createJobKey(job),
                ...job.opts,
                removeOnComplete: true,
                removeOnFail: true
              }
            }));

            await queue.addBulk(preparedJobs);

            // Update parent job tracking
            if (parentJob) {
              parentJob.actualJobs = preparedJobs.length;
              parentJob.allJobsQueued = true;
            }

            // Transition to QUEUED after jobs are queued
            setTaskStatus(taskId, 'QUEUED', 'opensea token bid jobs queued', 'opensea');

            // Trigger percentage update
            await updateTaskRunningPercentage(taskId);

            // The task will transition to RUNNING when jobs start processing

          } catch (jobError) {
            errorLog('OpenSea', `Failed to queue token bid jobs: ${jobError}`);
            // Re-throw to trigger retry
            throw jobError;
          }
        } else if (preprocessing.opensea.traits.length > 0) {
          // Trait bid
          const traitJobs = preprocessing.opensea.traits.map((trait) => {
            const jobData = {
              _id: taskId,
              address: WALLET_ADDRESS,
              walletId: task.walletId,
              slug: currentTask.contract.slug,
              contractAddress: currentTask.contract.contractAddress,
              creatorFees,
              enforceCreatorFee: collectionDetails.enforceCreatorFee,
              trait: JSON.stringify(trait),
              outbidOptions: currentTask.outbidOptions,
              // Pass pre-calculated values to avoid redundant calculateBidPrice calls (15-150ms savings)
              floor_price: scheduledBidCalc?.floor_price,
              maxBidPriceEth: scheduledBidCalc?.maxBidPriceEth,
              minBidPriceEth: scheduledBidCalc?.minBidPriceEth,
              offerPriceEth: scheduledBidCalc?.offerPriceEth,
              currency: scheduledBidCalc?.currency,
              decimal: scheduledBidCalc?.decimal,
              currencySymbol: scheduledBidCalc?.currencySymbol,
            };
            return {
              name: OPENSEA_TRAIT_BID,
              data: jobData,
              opts: { priority: TRAIT_BID_PRIORITY.OPENSEA, attempts: 1 } as JobsOptions
            };
          }) as Job[]

          // Ensure parent job exists BEFORE processing jobs
          await ensureParentJobExists(taskId, 'opensea');
          const parentJobId = getParentJobId(taskId, 'opensea');
          const parentJob = parentJobs.get(parentJobId);
          if (parentJob) {
            parentJob.expectedJobs = preprocessing.opensea.traits.length;
            parentJob.totalJobs = preprocessing.opensea.traits.length; // Use expected count initially
          }

          try {
            // Extend lock before processing bulk trait jobs
            if (lockAcquired) {
              timedLockManager.extendLock(taskId, 'opensea');
            }

            // Transition to QUEUEING state before creating jobs
            setTaskStatus(taskId, 'QUEUEING', 'queueing opensea trait bid jobs', 'opensea');
            const loopNumTrait = loopCounterStats[taskId]?.['opensea'] || 1;

            // Queue jobs directly with queue.addBulk
            const preparedTraitJobs = traitJobs.map(job => ({
              name: job.name,
              data: job.data,
              opts: {
                jobId: createJobKey(job),
                ...job.opts,
                removeOnComplete: true,
                removeOnFail: true
              }
            }));

            await queue.addBulk(preparedTraitJobs);

            // Update parent job tracking
            if (parentJob) {
              parentJob.actualJobs = preparedTraitJobs.length;
              parentJob.allJobsQueued = true;
            }

            // Transition to QUEUED after jobs are queued
            setTaskStatus(taskId, 'QUEUED', 'opensea trait bid jobs queued', 'opensea');

            // Trigger percentage update
            await updateTaskRunningPercentage(taskId);

            // The task will transition to RUNNING when jobs start processing

          } catch (jobError) {
            errorLog('OpenSea', `Failed to queue trait bid jobs: ${jobError}`);
            // Re-throw to trigger retry
            throw jobError;
          }
        } else if (preprocessing.opensea.jobCount === 1) {
          // Collection bid
          const jobId = `bid-${taskId}-opensea-${currentTask.contract.slug}-collection`

          // Ensure parent job exists BEFORE adding job
          await ensureParentJobExists(taskId, 'opensea');
          const parentJobId = getParentJobId(taskId, 'opensea');
          const parentJob = parentJobs.get(parentJobId);
          if (parentJob) {
            parentJob.expectedJobs = 1;
            parentJob.totalJobs = 1;
            parentJob.actualJobs = 1;
            parentJob.allJobsQueued = true;
          }

          // Transition to QUEUEING state before creating job
          setTaskStatus(taskId, 'QUEUEING', 'queueing opensea collection bid job', 'opensea');
          const loopNumColl = loopCounterStats[taskId]?.['opensea'] || 1;

          await queue.add(
            OPENSEA_COLLECTION_BID,
            {
              taskId: taskId,
              collectionDetails,
              currency,
              // Pass pre-calculated values to avoid redundant calculateBidPrice calls (15-150ms savings)
              floor_price: scheduledBidCalc?.floor_price,
              maxBidPriceEth: scheduledBidCalc?.maxBidPriceEth,
              minBidPriceEth: scheduledBidCalc?.minBidPriceEth,
              offerPriceEth: scheduledBidCalc?.offerPriceEth,
              decimal: scheduledBidCalc?.decimal,
              currencySymbol: scheduledBidCalc?.currencySymbol,
            },
            { jobId, priority: 2, attempts: 1 }
          );


          // Transition to QUEUED after job is queued (not RUNNING)
          setTaskStatus(taskId, 'QUEUED', 'opensea collection bid job queued', 'opensea');

          // Trigger percentage update for single job
          await updateTaskRunningPercentage(taskId);
        } else {
          // No jobs to create - create empty parent job and complete the loop
          if (DEBUG) {
          }

          await ensureParentJobExists(taskId, 'opensea');
          const parentJobId = getParentJobId(taskId, 'opensea');
          const parentJob = parentJobs.get(parentJobId);
          if (parentJob) {
            parentJob.expectedJobs = 0;
            parentJob.totalJobs = 0;
            parentJob.actualJobs = 0;
            parentJob.completedJobs = 0;
            parentJob.allJobsQueued = true;
          }

          // Update task status to trigger completion flow
          // No jobs to process - loop will complete without transitioning to RUNNING
          await updateTaskRunningPercentage(taskId);

          // Trigger marketplace-specific completion for OpenSea
          await checkTaskCompletionAndScheduleNext(taskId, 'opensea');

          // Don't throw error - let the loop complete normally
        }
      })(), // End of main processing logic
      timeoutPromise
    ]);
  } catch (error) {
    errorLog('OpenSea', `Error processing OpenSea scheduled bid for task: ${task._id}`, error);

    // Enhanced error handling for timeout and lock issues
    const isTimeout = error instanceof Error && error.message.includes('timeout');
    const isLockError = error instanceof Error && (
      error.message.includes('Missing lock') ||
      error.message.includes('Lock acquisition failed') ||
      error.message.includes('LOCK ERROR')
    );

    if (isTimeout) {
      errorLog('OpenSea', `Task ${taskId} timed out after ${TIMEOUT_DURATION}ms`);

      // Log timeout with additional context
      errorLog('Queue', `OpenSea scheduled bid timeout: taskId=${taskId}, timeoutDuration=${TIMEOUT_DURATION}, lockAcquired=${lockAcquired}, currentLoopCount=${currentLoopCount}`, error instanceof Error ? error : new Error(String(error)));

      // Clean up resources on timeout
      if (lockAcquired) {
        try {
          timedLockManager.releaseLock(taskId, 'opensea');
        } catch (cleanupError) {
          errorLog('TimeoutCleanup', `Failed to release lock for task ${taskId}`, cleanupError);
        }
      }
    }

    if (isLockError) {
      errorLog('OpenSea', `Lock error for task ${taskId}: ${error instanceof Error ? error.message : String(error)}`);

      // Log lock error for monitoring (Direct execution - no BullMQ job cleanup needed)
      errorLog('Queue', `OpenSea scheduled bid lock error: taskId=${taskId}, lockAcquired=${lockAcquired}`, error instanceof Error ? error : new Error(String(error)));
    }

    // Log to file
    const logsDir = path.join(process.cwd(), 'debug-logs');

    const errorDetails = {
      timestamp: new Date().toISOString(),
      marketplace: 'opensea',
      taskId: task._id,
      error: error instanceof Error ? error.message : String(error),
      stackTrace: error instanceof Error ? error.stack : undefined,
      isTimeout,
      isLockError,
      lockAcquired,
      currentLoopCount,
      taskData: {
        running: task.running,
        selectedMarketplaces: task.selectedMarketplaces,
        contract: task.contract,
        selectedTraits: task.selectedTraits,
        bidPrice: task.bidPrice,
        loopInterval: task.loopInterval
      }
    };

    const logPath = path.join(logsDir, 'scheduled-job-failures.log');
    fs.appendFileSync(logPath, JSON.stringify(errorDetails, null, 2) + '\n---\n');

    // Check if this is a Redis failure
    if (isRedisError(error)) {

      // Add detailed error information
      errorLog('General', `Redis failure in OpenSea scheduled bid: taskId=${task._id}, marketplace=opensea, errorType=REDIS_DOWN`, error instanceof Error ? error : new Error(String(error)));
    }

    // Check if OpenSea marketplace is stuck in STARTED status
    const currentStatus = taskStatus[taskId]?.marketplaces?.opensea?.status;
    if (currentStatus === 'STARTED') {

      // Give other marketplaces a chance, then check
      setTimeout(() => {
        statusProgressionManager.checkAndFixStuckStartedTask(taskId, 'opensea');
      }, 5000);
    }

    // Re-throw error to trigger job retry
    throw error;
  } finally {
    // Always release lock if acquired
    if (lockAcquired) {
      timedLockManager.releaseLock(taskId, 'opensea');
    }
  }
}

/**
 * Transforms an array of attributes with multiple values into individual attribute objects,
 * each containing a single value.
 * 
 * @param attributes Array of attribute objects with multiple values
 * @returns Array of attribute objects, each with a single value
 */
function expandAttributes(attributes: Array<{ traitType: string; values: string[] }>): Array<{ traitType: string; values: string[] }> {
  const expandedAttributes: Array<{ traitType: string; values: string[] }> = [];

  for (const attribute of attributes) {
    for (const value of attribute.values) {
      expandedAttributes.push({
        traitType: attribute.traitType,
        values: [value]
      });
    }
  }

  return expandedAttributes;
}

function transformSelectedTraitsToAttributes(selectedTraits: Record<string, string[]>): Array<{ traitType: string, values: string[] }> {
  return Object.entries(selectedTraits).map(([traitType, values]) => ({
    traitType,
    values
  }));
}

async function processOpenseaCollectionBid(data: {
  taskId: string;
  collectionDetails: any;
  currency?: string;
  floor_price?: number;
  maxBidPriceEth?: number;
  minBidPriceEth?: number;
  offerPriceEth?: number;
  decimal?: number;
  currencySymbol?: string;
}) {
  const taskId = data.taskId;
  if (isTaskBlacklisted(taskId, 'opensea')) {
    return;
  }

  try {

    // Fetch task from activeTasks
    const currentTask = activeTasks.get(taskId);
    if (!currentTask || !currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("opensea") || !enabledMarketplaces
      .map((m) => m.toLowerCase())
      .includes("opensea")) {
      if (DEBUG) {
      }
      return;
    }


    const { collectionDetails } = data;

    // Early wallet validation - fetch once and reuse later
    const wallet = await walletCache.getWalletById(currentTask.walletId);
    if (!wallet || !wallet.encryptedPrivateKey) {
      if (DEBUG) {
      }
      return;
    }


    // Initialize stats early
    if (!floorPrices[currentTask._id]) {
      floorPrices[currentTask._id] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      };
    }

    if (!skipStats[currentTask._id]) {
      skipStats[currentTask._id] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      }
    }


    // Parallel fetch: floor price (orders now from cache)
    const floor_price = await getCachedFloorPrice(currentTask.contract.slug);
    const cachedOrder = await orderCache.getNewest(currentTask._id, OPENSEA.toLowerCase(), 'collection');
    const orders = cachedOrder ? [cachedOrder] : [];


    if (!floor_price) {
      errorLog('OpenSea', `Failed to fetch floor price | taskId=${taskId}`);
      throw new Error(`Failed to fetch floor price for ${currentTask.contract.slug}`);
    }

    // Fix 9: Defensive null check - floorPrices may be deleted by cleanupTaskResources during async operations
    if (!floorPrices[currentTask._id]) {
      return;
    }
    floorPrices[currentTask._id].opensea = floor_price as number;

    const stopBid = await stopOption(currentTask, 'opensea', Number(floor_price), false)
    if (stopBid) {
      if (DEBUG) {
      }
      return;
    }

    // Use pre-calculated values if available (scheduled bids), otherwise calculate (backwards compatibility)
    let offerPriceEth, maxBidPriceEth, minBidPriceEth, currencySymbol, currency, decimal;

    if (data.maxBidPriceEth !== undefined && data.offerPriceEth !== undefined) {
      // Use pre-calculated values from scheduled bid (saves 15-150ms)
      maxBidPriceEth = data.maxBidPriceEth;
      offerPriceEth = data.offerPriceEth;
      minBidPriceEth = data.minBidPriceEth!;
      currencySymbol = data.currencySymbol!;
      currency = data.currency!;
      decimal = data.decimal!;

    } else {
      // Fallback: Calculate (backwards compatibility)
      const result = await calculateBidPrice(currentTask, floor_price as number, "opensea");
      maxBidPriceEth = result.maxBidPriceEth;
      offerPriceEth = result.offerPriceEth;
      minBidPriceEth = result.minBidPriceEth;
      currencySymbol = result.currencySymbol;
      currency = result.currency;
      decimal = result.decimal;

    }

    const expiry = getExpiry(currentTask.bidDuration)
    const usdcBid = currency.toLowerCase() === USDC.toLowerCase()
    let offerPrice = BigInt(Math.ceil(roundToValidPrecision(offerPriceEth) * Math.pow(10, decimal)));

    let collectionOffer = BigInt(offerPrice)

    // Extract first active order (if any) with proper null guards
    const [order] = orders
    const hasActiveOrder = order && order.expiresAt && order.createdAt;
    // CachedOrder has expiresAt/createdAt as number (timestamp in ms)
    const orderExpiresAt = typeof order?.expiresAt === 'number' ? order.expiresAt : new Date(order?.expiresAt).getTime();
    const orderCreatedAt = typeof order?.createdAt === 'number' ? order.createdAt : new Date(order?.createdAt).getTime();
    const ttl = hasActiveOrder ? (orderExpiresAt - Date.now()) / 1000 : -1;
    const currentPrice = hasActiveOrder ? Number(order.offerPrice) : Number(collectionOffer);
    const orderExpiration = hasActiveOrder
      ? Math.round((orderExpiresAt - orderCreatedAt) / 1000)
      : expiry;

    // Initialize spread info variables with defaults (used in PLACING BID log)
    let highestBidAmount = 0;
    let secondTopBidAmount = 0;
    let outbidMargin = 0;
    let isOwnBid = false;

    if (!currentTask.outbidOptions.outbid) {
      // Skip if bid is still valid: TTL sufficient, same price, similar expiration
      if (hasActiveOrder && ttl >= MIN_BID_DURATION && currentPrice === Number(collectionOffer) && Math.abs(orderExpiration - expiry) < 30) {
        if (DEBUG) {
        }
        return;
      }
    }
    else {
      // Parallel fetch: wallets and offers (priority 0 for scheduled jobs)
      const [walletsArr, offers] = await Promise.all([
        walletCache.getAllWallets(),
        fetchOpenseaOffers('COLLECTION', currentTask.contract.slug, {}, 0)
      ]);
      const [topOffer, secondOffer] = (offers as unknown) as OpenseaOfferData[]
      highestBidAmount = usdcBid ? topOffer.usd * Math.pow(10, decimal) : topOffer.eth * Math.pow(10, decimal)
      secondTopBidAmount = usdcBid ? secondOffer.usd * Math.pow(10, decimal) : secondOffer.eth * Math.pow(10, decimal)

      outbidMargin = calculateOutbidMargin(highestBidAmount ? highestBidAmount / Math.pow(10, decimal) : offerPriceEth, currency);

      const owner = topOffer.owner.toLowerCase();
      isOwnBid = walletsArr
        .map((addr: any) => addr.address.toLowerCase())
        .includes(owner);

      // Top offer fetched from API

      // 2. Handle own bid case
      if (isOwnBid) {
        const spreadRaw = (Number(highestBidAmount) - Number(secondTopBidAmount)) / Math.pow(10, decimal);
        const spread = roundToValidPrecision(spreadRaw);
        const secondTopBidAmountEth = Number(secondTopBidAmount) / Math.pow(10, decimal);
        const potentialNewBidEth = secondTopBidAmountEth + outbidMargin;


        if (spread > outbidMargin && Number(secondTopBidAmount) > 0 && potentialNewBidEth >= minBidPriceEth) {
          collectionOffer = BigInt(Math.round(roundToValidPrecision(potentialNewBidEth) * Math.pow(10, decimal)));
          if (DEBUG) {
          }
        }
        else if (spread > outbidMargin && Number(secondTopBidAmount) > 0 && potentialNewBidEth < minBidPriceEth) {
          // Large spread but potential bid below min: keep current offer (don't lower below min)
          if (DEBUG) {
          }
        }
        else if (ttl > MIN_BID_DURATION) {
          if (DEBUG) {
          }
          return;
        }
        else {
          collectionOffer = BigInt(Math.round(roundToValidPrecision(highestBidAmount / Math.pow(10, decimal)) * Math.pow(10, decimal)));
          if (DEBUG) {
          }
        }
      }
      else {
        const newBidEth = (highestBidAmount / Math.pow(10, decimal)) + outbidMargin;
        collectionOffer = BigInt(Math.round(roundToValidPrecision(newBidEth) * Math.pow(10, decimal)));
      }
    }
    const bidCount = getIncrementedBidCount(OPENSEA, currentTask.contract.slug, currentTask._id)

    // Convert all creator fees to the IFee format (ensuring values are numbers)
    const creatorFees: IFee = Object.fromEntries(
      Object.entries(collectionDetails.creator_fees).map(([key, value]) => [key, Number(value)])
    );

    const offerEth = Number(collectionOffer) / Math.pow(10, decimal);

    // Check if offer exceeds floor price (only for WETH)
    if (currencySymbol.toLowerCase() === 'weth' && offerEth > floor_price) {
      if (DEBUG) {
      }
      skipStats[currentTask._id]['opensea']++;
      const message = `⚠️ Offer price ${offerEth} ${currencySymbol} for ${currentTask.contract.slug} collection bid exceeds floor price ${floor_price} ${currencySymbol} FOR OPENSEA.`.toUpperCase();


      logBidError(currentTask._id, "OFFER EXCEEDS FLOOR PRICE", message, "warning", "opensea"); // Non-blocking
      return;
    }

    // CRITICAL: Always check maxBidPrice regardless of outbid setting
    if (maxBidPriceEth > 0 && offerEth >= maxBidPriceEth) {
      if (DEBUG) {
      }
      // Orders already cancelled above - no additional cancellation needed
      skipStats[currentTask._id]['opensea']++;
      const message = `❌ Offer price ${offerEth} ${currencySymbol} for ${currentTask.contract.slug} collection bid exceeds max bid price ${maxBidPriceEth} ${currencySymbol} FOR OPENSEA.Skipping ...`.toUpperCase()

      logBidError(currentTask._id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "opensea"); // Non-blocking
      return;
    } else if (offerEth < minBidPriceEth) {
      collectionOffer = BigInt(Math.round(minBidPriceEth * Math.pow(10, decimal)));
    }

    const finalOfferEth = Number(collectionOffer) / Math.pow(10, decimal);
    const topBidEth = highestBidAmount / Math.pow(10, decimal);
    const secondBidEth = secondTopBidAmount / Math.pow(10, decimal);

    // Log bid API start

    try {
      // Decrypt private key from wallet (already validated at function start)
      const privateKey: string = decryptPrivateKey(wallet.encryptedPrivateKey);

      // Queue cancellation jobs asynchronously (non-blocking)
      if (orders.length > 0) {

        const cancelData = orders.map((order) => ({
          name: CANCEL_OPENSEA_OUTBID,
          data: {
            walletId: currentTask.wallet,
            orderId: order.orderHash,
            taskId: currentTask._id.toString(),
            identifier: 'collection',
            slug: order.slug,
            source: 'processOpenseaCollectionBid'
          },
          opts: getCancellationJobOptions(currentTask._id.toString(), order.orderHash, CANCEL_OPENSEA_OUTBID)
        }));

        await queue.addBulk(cancelData);

      }

      const bidMetadata = {
        bidCount,
        walletAddress: currentTask.wallet,
        slug: currentTask.contract.slug,
        offerPrice: Number(collectionOffer),
        offerEth: Number(collectionOffer) / Math.pow(10, decimal),
        expiry,
        currency,
        contractAddress: currentTask.contract.contractAddress,
        creatorFees,
      };

      await bidOnOpensea(
        currentTask._id,
        bidCount,
        currentTask.wallet,
        privateKey,
        currentTask.contract.slug,
        Number(collectionOffer),
        creatorFees,
        expiry,
        currency,
        currentTask.contract.contractAddress
      )

      // Log success
      // Log job completed

    } catch (bidError: any) {
      // Check if this is an insufficient balance error
      const handled = await handleInsufficientBalanceError(
        taskStatus,
        warningBids,
        warningMessage,
        bidError,
        currentTask._id,
        'opensea',
        currentTask.wallet,
        currency,
        minBidPriceEth,
        'collection'
      );

      if (handled) {
        // Balance error was handled (marketplace stopped or bid skipped)
        return;
      }

      // Not a balance error, log and re-throw
      const errMsg = bidError?.message || String(bidError);

      // Log bid error

      errorLog('OpenSea', `Failed to place collection bid | taskId=${taskId}`, bidError);
      throw bidError; // Re-throw to be caught by outer catch
    }

  } catch (error: any) {
    // Format OpenSea error message for better readability
    const errMsg = error?.message || String(error);
    const errorMessage = extractDetailedOpenSeaError(error);

    // Log job failed

    errorLog('OpenSea', `Fatal error processing OpenSea collection bid | taskId=${taskId}`, error);


    await logBidError(taskId, "OPENSEA COLLECTION BID ERROR", `Error processing OpenSea collection bid for task: ${taskId}: ${errorMessage}`, "error", "opensea");

    // Throw the error
    throw error;
  }
}

async function processBlurScheduledBid(task: ITask) {
  const taskId = task._id.toString();
  const currentLoopCount = loopCounterStats[taskId]?.blur || 1;


  // Check blacklist at the VERY START to prevent any processing if task is being stopped
  if (isTaskBlacklisted(taskId, 'blur')) {
    return;
  }

  let lockAcquired = false;

  try {
    const currentTask = activeTasks.get(task._id)
    if (!currentTask || !currentTask?.running || !currentTask.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("blur") || !enabledMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("blur")) {
      if (DEBUG) {
      }
      return;
    }

    // Transition from SCHEDULED to STARTED for this marketplace - AFTER validation
    setTaskStatus(taskId, 'STARTED', `blur scheduled bid starting - Loop #${currentLoopCount}`, 'blur');
    // Processing Blur scheduled bid

    // Get preprocessing data - first check the Map, then perform if needed
    let preprocessing = taskPreprocessing.get(taskId);

    if (!preprocessing || !preprocessing.blur.enabled) {


      // Determine available marketplaces for this task
      const validMarketplaces = ["opensea", "magiceden", "blur"];
      const availableMarketplaces = currentTask.selectedMarketplaces
        .map(m => m.toLowerCase())
        .filter(m => enabledMarketplaces.map(em => em.toLowerCase()).includes(m))
        .filter(m => validMarketplaces.includes(m));

      // Perform preprocessing for Blur (with priority 0 for scheduled jobs)
      preprocessing = await performMarketplacePreprocessing(currentTask, "blur", availableMarketplaces, 0);

      // Store in Map for other marketplaces to use
      taskPreprocessing.set(taskId, preprocessing);


      // If preprocessing didn't enable Blur, it means something went wrong
      if (!preprocessing.blur.enabled) {
        if (DEBUG) {
        }
        const error = new Error(`Preprocessing failed to enable Blur for task ${taskId}`);


        throw error;
      }
    }

    if (preprocessing.blur.skipReason) {
      if (DEBUG) {
      }

      if (!warningBids[taskId]) {
        warningBids[taskId] = { opensea: false, magiceden: false, blur: false };
      }
      if (!warningMessage[taskId]) {
        warningMessage[taskId] = { opensea: "", magiceden: "", blur: "" };
      }
      warningBids[taskId].blur = true;
      warningMessage[taskId].blur = preprocessing.blur.skipReason;

      return;
    }

    // Initialize task status if it doesn't exist
    if (!taskStatus[taskId]) {
      const activeTask = activeTasks.get(taskId);
      taskStatus[taskId] = {
        selectedMarketplaces: activeTask?.selectedMarketplaces.map(m => m.toLowerCase()) || [],
        lastUpdate: Date.now(),
        marketplaces: {}
      };
    } else {
      // Validate before setting PROCESSING status
      const activeTask = activeTasks.get(taskId);
      const isBlurEnabled = enabledMarketplaces.map(m => m.toLowerCase()).includes('blur');
      const isBlurSelected = activeTask?.selectedMarketplaces.map(m => m.toLowerCase()).includes('blur');
      const isTaskRunning = activeTask?.running === true;

      if (isBlurEnabled && isBlurSelected && isTaskRunning) {
        // Use setTaskStatus for comprehensive logging
        setTaskStatus(taskId, 'PROCESSING', 'blur scheduled bid processing', 'blur');
        const blurProcLoopNum = loopCounterStats[taskId]?.['blur'] || 1;
      } else {
        return;
      }
    }

    // Acquire lock for this task - will throw if stale or already locked
    // Use timed lock manager for better timeout handling
    lockAcquired = timedLockManager.acquireLock(taskId, 'blur');
    if (!lockAcquired) {
      if (DEBUG) {
      }
      return;
    }



    const { currencySymbol } = getCurrencyDetailsFromTask(currentTask, "blur");

    if (currencySymbol.toLowerCase() !== "weth") {
      if (DEBUG) {
      }

      if (!warningMessage[taskId]) {
        warningMessage[taskId] = {
          blur: "",
          magiceden: "",
          opensea: ""
        }
      }
      if (!warningBids[taskId]) {
        warningBids[taskId] = {
          blur: false,
          magiceden: false,
          opensea: false
        }
      }

      warningBids[taskId].blur = true;
      warningMessage[taskId].blur = `${currencySymbol} is not supported on Blur`;
      return
    }

    const expiry = getExpiry(currentTask.bidDuration)
    const WALLET_ADDRESS: string = currentTask.wallet;
    const wallets = await walletCache.getWalletById(task.walletId)
    const WALLET_PRIVATE_KEY: string = decryptPrivateKey(wallets?.encryptedPrivateKey as string)

    // Extend lock before fetching balances
    if (lockAcquired) {
      timedLockManager.extendLock(taskId, 'blur');
    }

    // Fetch fresh BETH balance with timeout protection
    let bethBalance = 0;
    try {

      const { fetchERC20Balance } = await import('./utils/balance');
      const { BETH } = await import('./utils/currencies');
      bethBalance = await fetchERC20Balance(WALLET_ADDRESS, BETH);


    } catch (error) {

      errorLog('Blur', 'Failed to fetch BETH balance', error);
    }

    // Calculate minimum bid price to validate sufficient balance
    const minBidPriceEth = getMinBidPriceFromTask(currentTask, "blur");

    // Check if balance is insufficient or zero
    if (bethBalance <= 0 || bethBalance < minBidPriceEth) {
      if (DEBUG) {
      }
      const errorMsg = bethBalance <= 0
        ? `Insufficient BETH balance: ${bethBalance} (zero or negative) for task ${taskId}`
        : `Insufficient BETH balance: ${bethBalance} < ${minBidPriceEth} for task ${taskId}`;



      // Increment skip stats
      if (!skipStats[taskId]) {
        skipStats[taskId] = { opensea: 0, magiceden: 0, blur: 0 };
      }
      skipStats[taskId].blur++;

      // Log the insufficient balance
      await logBidError(taskId, "INSUFFICIENT_BALANCE", errorMsg, "skipped", "blur");

      // Create empty parent job and complete the marketplace loop
      await ensureParentJobExists(taskId, 'blur');
      const parentJobId = getParentJobId(taskId, 'blur');
      const parentJob = parentJobs.get(parentJobId);
      if (parentJob) {
        parentJob.expectedJobs = 0;
        parentJob.totalJobs = 0;
        parentJob.actualJobs = 0;
        parentJob.completedJobs = 0;
        parentJob.allJobsQueued = true;


      }

      // Update task status to trigger completion flow
      // Insufficient balance - loop will complete without transitioning to RUNNING
      await updateTaskRunningPercentage(taskId);

      // Trigger marketplace-specific completion for Blur
      await checkTaskCompletionAndScheduleNext(taskId, 'blur');

      return; // Exit function without throwing error
    }

    const contractAddress = currentTask.contract.contractAddress
    // Check authentication block status before pre-authenticating (LRU cache)
    const block = blurAuthBlockLRU.get(WALLET_ADDRESS.toLowerCase());
    if (block && block.blockedUntil > Date.now()) {
      const waitTime = block.blockedUntil - Date.now();
      const waitSeconds = Math.round(waitTime / 1000);

      if (!skipStats[taskId]) {
        skipStats[taskId] = { opensea: 0, magiceden: 0, blur: 0 };
      }
      skipStats[taskId].blur++;

      throw new Error(`Authentication blocked - retry in ${waitSeconds}s`);
    } else if (block) {
      // Block has expired, clean it up
      blurAuthBlockLRU.delete(WALLET_ADDRESS.toLowerCase());
    }

    // Extend lock before pre-authentication
    if (lockAcquired) {
      timedLockManager.extendLock(taskId, 'blur');
    }

    // Pre-authenticate for Blur before creating jobs

    try {
      const authStartTime = Date.now();
      const accessToken = await getAccessToken(WALLET_PRIVATE_KEY);

      if (accessToken) {
        const authTime = Date.now() - authStartTime;
      } else {
      }
    } catch (authError: any) {

      errorLog('Blur', `Error pre-authenticating wallet ${WALLET_ADDRESS}`, authError);

      // Check if error is due to authentication block
      if (authError.message && authError.message.includes('Authentication blocked until')) {
        throw new Error(authError.message);
      }
      // Continue anyway - individual jobs will attempt auth if needed
    }

    // Check wallet pause status before creating jobs (LRU cache)
    const walletPause = blurWalletPauseLRU.get(WALLET_ADDRESS.toLowerCase());
    if (walletPause && walletPause.pauseUntil > Date.now()) {
      const waitTime = walletPause.pauseUntil - Date.now();
      throw new Error(`Wallet paused for ${Math.round(waitTime / 1000)}s`);
    } else if (walletPause) {
      // Pause has expired, clean it up
      blurWalletPauseLRU.delete(WALLET_ADDRESS.toLowerCase());
    }

    // Define Blur rate limits based on API documentation
    const BLUR_RATE_LIMIT = {
      maxBidsPerWindow: 40,  // Maximum 40 bids per 10 seconds
      windowDuration: 10000, // 10 second window
      batchSize: 35,         // Process 35 jobs at a time (87.5% of limit for safety)
      batchDelay: 10000      // 10 second delay between batches to respect the window
    };

    // Create jobs based on preprocessing results
    if (preprocessing.blur.traits.length > 0) {
      // Trait bid - process all traits respecting Blur's rate limit
      const totalTraits = preprocessing.blur.traits.length;
      const batchSize = BLUR_RATE_LIMIT.batchSize;
      const totalBatches = Math.ceil(totalTraits / batchSize);



      // Ensure parent job exists BEFORE processing jobs
      const parentJobId = `${taskId}-blur`;
      await ensureParentJobExists(taskId, 'blur');
      const parentJob = parentJobs.get(parentJobId);
      if (parentJob) {
        parentJob.expectedJobs = preprocessing.blur.traits.length;
        parentJob.totalJobs = preprocessing.blur.traits.length; // Use expected count initially
      }

      // Process traits in smaller batches that respect the rate limit
      for (let i = 0; i < totalTraits; i += batchSize) {
        const batchNumber = Math.floor(i / batchSize) + 1;
        const batchTraits = preprocessing.blur.traits.slice(i, i + batchSize);
        const batchDelay = (batchNumber - 1) * BLUR_RATE_LIMIT.batchDelay;



        // Create jobs for this batch with staggered delays within the batch
        const traitJobs = batchTraits.map((trait, index) => {
          // Add a small delay between each job within the batch (150ms)
          const inBatchDelay = index * 150;
          const totalDelay = batchDelay + inBatchDelay;

          return {
            name: BLUR_TRAIT_BID,
            data: {
              _id: taskId,
              address: WALLET_ADDRESS,
              walletId: task.walletId,
              contractAddress,
              slug: currentTask.contract.slug,
              trait: JSON.stringify(trait),
              expiry,
              outbidOptions: currentTask.outbidOptions,
              batchInfo: {
                batchNumber,
                totalBatches,
                traitIndex: i + index,
                totalTraits,
                batchSize
              }
            },
            opts: {
              priority: TRAIT_BID_PRIORITY.BLUR,
              delay: totalDelay
            } as JobsOptions
          };
        }) as Job[]

        // Queue jobs directly with queue.addBulk
        if (traitJobs.length > 0) {
          const preparedJobs = traitJobs.map(job => ({
            name: job.name,
            data: job.data,
            opts: {
              jobId: createJobKey(job),
              ...job.opts,
              removeOnComplete: true,
              removeOnFail: true
            }
          }));

          await queue.addBulk(preparedJobs);

          // Update parent job tracking
          if (parentJob) {
            parentJob.actualJobs = (parentJob.actualJobs || 0) + preparedJobs.length;
            // Set allJobsQueued only after last batch
            if (i + batchSize >= totalTraits) {
              parentJob.allJobsQueued = true;
            }
          }

          // Set status on first batch
          if (batchNumber === 1) {
            setTaskStatus(taskId, 'QUEUED', 'blur trait jobs queued', 'blur');
          }
        }

        // Log batch scheduling
      }

      // Trigger percentage update after all batches queued
      await updateTaskRunningPercentage(taskId);

      const totalTime = (totalBatches - 1) * BLUR_RATE_LIMIT.batchDelay / 1000;
      const effectiveBidsPerSecond = BLUR_RATE_LIMIT.batchSize / BLUR_RATE_LIMIT.windowDuration * 1000;
      const utilizationPercent = (BLUR_RATE_LIMIT.batchSize / BLUR_RATE_LIMIT.maxBidsPerWindow * 100).toFixed(1);



    } else if (preprocessing.blur.jobCount === 1) {
      // Collection bid
      const slug = removeChar(currentTask.contract.slug, "-")
      const jobId = `bid-${taskId}-blur-${slug}-collection-${WALLET_ADDRESS}`

      // Ensure parent job exists BEFORE adding job
      const parentJobId = `${taskId}-blur`;
      await ensureParentJobExists(taskId, 'blur');
      const parentJob = parentJobs.get(parentJobId);
      if (parentJob) {
        parentJob.expectedJobs = 1;
        parentJob.totalJobs = 1; // Use expected count initially
      }

      await queue.add(
        BLUR_COLLECTION_BID,
        { taskId: taskId, contractAddress, expiry },
        { jobId, priority: 2, attempts: 1 }
      );

      // Update parent job count after successful job addition - mark as all queued for single job
      if (parentJob) {
        parentJob.actualJobs = 1;
        parentJob.allJobsQueued = true;
      }

      if (currentTask && currentTask.running) {
        if (taskStatus[taskId]) {
          // Use setTaskStatus for comprehensive logging
          setTaskStatus(taskId, 'QUEUED', 'blur collection bid job queued', 'blur');
          const blurLoopNum = loopCounterStats[taskId]?.['blur'] || 1;
        }
      }

      // Trigger percentage update now that the job is queued
      await updateTaskRunningPercentage(taskId);
    } else {
      // No jobs to create - create empty parent job and complete the loop


      const parentJobId = `${taskId}-blur`;
      await ensureParentJobExists(taskId, 'blur');
      const parentJob = parentJobs.get(parentJobId);
      if (parentJob) {
        parentJob.expectedJobs = 0;
        parentJob.totalJobs = 0;
        parentJob.actualJobs = 0;
        parentJob.completedJobs = 0;
        parentJob.allJobsQueued = true;
      }

      // Update task status to trigger completion flow
      // No jobs to process - loop will complete without transitioning to RUNNING
      await updateTaskRunningPercentage(taskId);

      // Trigger marketplace-specific completion for Blur
      await checkTaskCompletionAndScheduleNext(taskId, 'blur');
    }
  } catch (error) {
    errorLog('Blur', `Error processing Blur scheduled bid for task: ${task._id}`, error);


    // Log to file
    const logsDir = path.join(process.cwd(), 'debug-logs');

    const errorDetails = {
      timestamp: new Date().toISOString(),
      marketplace: 'blur',
      taskId: task._id,
      error: error instanceof Error ? error.message : String(error),
      stackTrace: error instanceof Error ? error.stack : undefined,
      taskData: {
        running: task.running,
        selectedMarketplaces: task.selectedMarketplaces,
        contract: task.contract,
        selectedTraits: task.selectedTraits,
        bidPrice: task.bidPrice,
        loopInterval: task.loopInterval
      }
    };

    const logPath = path.join(logsDir, 'scheduled-job-failures.log');
    fs.appendFileSync(logPath, JSON.stringify(errorDetails, null, 2) + '\n---\n');

    // Re-throw error to trigger job retry
    throw error;
  } finally {
    // Always release lock if acquired
    if (lockAcquired) {
      timedLockManager.releaseLock(taskId, 'blur');


    }

  }
}


async function processBlurCollectionBid(data: { taskId: string }) {
  const taskId = data.taskId;
  const startTime = Date.now();

  // Get task early to access collectionSlug
  const currentTask = activeTasks.get(taskId);
  const collectionSlug = currentTask?.contract?.slug || 'unknown';

  // Create trace for scheduled bid
  // Log job started

  // Check if task is blacklisted before processing
  if (isTaskBlacklisted(taskId)) {
    return;
  }

  // Fetch task from activeTasks outside try block so it's accessible in catch
  if (!currentTask || !currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("blur") || !enabledMarketplaces.map((market) => market.toLowerCase()).includes("blur")) {
    if (DEBUG) {
    }
    return;
  }

  // Transition to RUNNING state when job starts processing
  // Job is starting to process - marketplace status will be updated when jobs are marked as processed

  try {
    if (!enabledMarketplaces.length) {
      return;
    }


    const wallets = await walletCache.getWalletById(currentTask.walletId)
    const privateKey: string = decryptPrivateKey(wallets?.encryptedPrivateKey as string)



    const floor_price: number = Number(await fetchBlurCollectionStats(currentTask.contract.slug, 0));
    if (!floor_price) {
      throw new Error(`Failed to fetch floor price for ${currentTask.contract.slug}`);
    }




    const cachedOrder = await orderCache.getNewest(currentTask._id, BLUR.toLowerCase(), "collection");
    const orders = cachedOrder ? [cachedOrder] : [];

    const stopBid = await stopOption(currentTask, 'blur', Number(floor_price), false)
    if (stopBid) {
      if (DEBUG) {
      }

      if (orders.length > 0) {

        const cancelData = orders.map((order) => {
          const payload = JSON.parse(order.orderHash)

          return {
            name: CANCEL_BLUR_BID,
            data: { walletId: currentTask.walletId, payload, taskId: currentTask._id, identifier: order.identifier }
          }
        })
        await queue.addBulk(cancelData);
      }

      return;
    }

    if (!floorPrices[currentTask._id]) {
      floorPrices[currentTask._id] = {
        opensea: 0,
        magiceden: 0,
        blur: floor_price
      };
    }
    // Fix 9: Defensive null check - floorPrices may be deleted by cleanupTaskResources during async operations
    if (!floorPrices[currentTask._id]) {
      return;
    }
    floorPrices[currentTask._id].blur = floor_price;


    const { offerPriceEth, maxBidPriceEth, minBidPriceEth } = await calculateBidPrice(currentTask, floor_price, "blur")


    let collectionOffer = BigInt(Math.round(offerPriceEth * 1e18))

    // Check if offer exceeds floor price (only for WETH)
    const offerEth = Number(collectionOffer) / 1e18;
    if (offerEth > floor_price) {
      skipStats[currentTask._id]['blur']++;

      await logBidError(currentTask._id, "OFFER EXCEEDS FLOOR PRICE", `Offer price ${offerEth} WETH for ${currentTask.contract.slug} collection bid exceeds floor price ${floor_price} WETH FOR BLUR.`, "warning", "blur");
      return;
    }

    const [order, ...remainingOrders] = orders
    const orderExpiresAt = typeof order?.expiresAt === 'number' ? order.expiresAt : new Date(order?.expiresAt).getTime();
    const ttl = (orderExpiresAt - Date.now()) / 1000
    const currentPrice = Number(order ? order.offerPrice : collectionOffer)
    const expiry = getExpiry(currentTask.bidDuration)

    const createdAt = typeof order?.createdAt === 'number' ? order.createdAt : new Date(order?.createdAt).getTime()
    const expiredAt = orderExpiresAt
    const orderExpiration = order ? Math.round((expiredAt - createdAt) / 1000) : expiry

    // Initialize spread info variables with defaults (used in PLACING BID log)
    let topOfferAmount = 0;
    let secondOfferAmount = 0;
    let blurOutbidMargin = 0;
    let isTopBid = false;

    if (!currentTask.outbidOptions.outbid) {
      if (ttl >= MIN_BID_DURATION && currentPrice === Number(collectionOffer) && Math.abs(orderExpiration - expiry) < 30) {
        return;
      }
    }
    else {

      const highestBid = await fetchBlurBid(currentTask.contract.slug, "COLLECTION", {}, 0);
      const [topOffer, secondOffer] = highestBid?.priceLevels?.sort((a, b) => +b.price - +a.price) || [{ price: "0" }, { price: "0" }];
      topOfferAmount = typeof topOffer === 'object' && topOffer ? Number(topOffer.price) : 0
      secondOfferAmount = typeof secondOffer === 'object' && secondOffer ? Number(secondOffer.price) : 0

      const bidderAddressesSample = topOffer?.bidderAddressesSample || []
      const numberBidders = topOffer?.numberBidders || 0
      isTopBid = numberBidders === 1 && bidderAddressesSample.map((address) => address.toLowerCase()).includes(currentTask.wallet.toLowerCase())
      blurOutbidMargin = currentTask.outbidOptions.blurOutbidMargin || 0.01

      // Top offer fetched from API

      if (isTopBid) {
        const spread = (Number(topOfferAmount) - Number(secondOfferAmount));
        const bidPrice = secondOfferAmount + blurOutbidMargin;
        if (+spread.toFixed(2) > blurOutbidMargin && Number(secondOfferAmount) > 0 && bidPrice >= minBidPriceEth) {
          collectionOffer = BigInt(Math.round(bidPrice * 1e18));
        } else if (+spread.toFixed(2) > blurOutbidMargin && Number(secondOfferAmount) > 0 && bidPrice < minBidPriceEth) {
          // Large spread but can't lower below min - maintain current bid
          collectionOffer = BigInt(Math.round(topOfferAmount * 1e18));
        } else if (ttl > MIN_BID_DURATION) {
          return;
        } else {
          collectionOffer = BigInt(Math.round(topOfferAmount * 1e18));
        }
      }
      else {
        collectionOffer = BigInt(Math.round((topOfferAmount + blurOutbidMargin) * 1e18));
      }
    }

    // Round to avoid floating-point precision issues
    const collectionOfferEth = Math.round(Number(collectionOffer) / 1e18 * 100) / 100;

    if (collectionOfferEth < minBidPriceEth) {
      collectionOffer = BigInt(Math.round(minBidPriceEth * 1e18))
    }

    const bidCount = getIncrementedBidCount(BLUR, currentTask.contract.slug, currentTask._id)

    if (orders.length > 0) {

      const cancelData = orders.map((order) => {
        const payload = JSON.parse(order.orderHash)

        return {
          name: CANCEL_BLUR_BID,
          data: { walletId: currentTask.walletId, payload, taskId: currentTask._id, identifier: order.identifier }
        }
      })
      await queue.addBulk(cancelData);
    }

    // Add tolerance for floating-point comparison (0.000001 = 1e-6)
    const PRICE_TOLERANCE = 0.000001;

    // CRITICAL: Always check maxBidPrice regardless of outbid setting
    if (maxBidPriceEth > 0 && collectionOfferEth >= maxBidPriceEth) {
      // Orders already cancelled above - no additional cancellation needed
      if (DEBUG) {
      }
      if (!skipStats[currentTask._id]) {
        skipStats[currentTask._id] = {
          opensea: 0,
          magiceden: 0,
          blur: 0
        }
      }
      skipStats[currentTask._id]['blur']++;

      await logBidError(currentTask._id, "MAXIMUM BID PRICE EXCEEDED", `Offer price ${collectionOfferEth} BETH for ${currentTask.contract.slug} collection bid exceeds max bid price ${maxBidPriceEth} BETH FOR BLUR BID.`, "skipped", "blur");

      return;
    } else if (collectionOfferEth < minBidPriceEth) {
      collectionOffer = BigInt(Math.round(minBidPriceEth * 1e18));
    }

    // Validate offer amount meets Blur's minimum before calling bidOnBlur
    const finalOfferEth = Number(collectionOffer) / 1e18;
    const BLUR_MIN_OFFER = 0.01; // Blur's minimum offer amount in ETH

    if (finalOfferEth < BLUR_MIN_OFFER) {
      skipStats[currentTask._id]['blur']++;

      await logBidError(currentTask._id, "BELOW_MINIMUM_OFFER", `Offer price ${finalOfferEth} BETH is below minimum blur offer price (${BLUR_MIN_OFFER} BETH) for collection ${currentTask.contract.slug}`, "skipped", "blur");
      return;
    }

    // Wrap bidOnBlur call to track completion
    try {
      const finalBidEth = Number(collectionOffer) / 1e18;
      // Log bid API start

      await bidOnBlur(currentTask._id, bidCount, currentTask.wallet, privateKey, currentTask.contract.contractAddress, collectionOffer, currentTask.contract.slug, expiry);

      // Log bid success
      // Bid successful

    } catch (bidError: any) {
      throw bidError; // Re-throw to be handled by outer catch
    }
  } catch (error: any) {
    const errMsg = error?.response?.data?.message || error?.response?.data || error?.message || String(error);

    // Log bid error

    // Handle rate limit errors
    if (error?.code === 'RATE_LIMITED') {
      throw error; // Re-throw to let worker handle retry with delay
    }
    // Initialize errorStats if needed
    if (!errorStats[currentTask._id]) {
      errorStats[currentTask._id] = {
        magiceden: 0,
        opensea: 0,
        blur: 0
      }
    }


    // Handle transfer restriction errors
    if (error.code === 'TRANSFER_RESTRICTED') {
      await logBidError(currentTask._id, "BLUR TRANSFER RESTRICTION", error.message, "warning", "blur");
      // Update tracking for transfer restriction
      return;
    }


    await logBidError(currentTask._id, "BLUR COLLECTION BID ERROR", `Error processing Blur collection bid for task: ${currentTask._id}: ${errMsg}`, "error", "blur");

    // Throw the error
    throw error;
  }
}


async function processOpenseaTraitBid(data: IProcessOpenseaTraitBidData) {
  // Parse trait early for logging
  let traitKey = 'unknown';
  try {
    const traitInfo = JSON.parse(data.trait);
    traitKey = `${traitInfo.type}:${traitInfo.value}`;
  } catch { /* ignore */ }

  // Check blacklist at the VERY START to prevent any processing if task is being stopped
  if (isTaskBlacklisted(data._id, 'opensea')) {
    return;
  }

  let errorMessage: string | undefined;

  // Structured start
  const traitInfo = JSON.parse(data.trait);

  // Check if task is blacklisted before processing
  if (isTaskBlacklisted(data._id)) {
    if (DEBUG) {
    }
    return;
  }

  try {
    if (!enabledMarketplaces.length) {
      if (DEBUG) {
      }
      return;
    }
    if (!enabledMarketplaces.map((m) => m.toLowerCase()).includes("opensea")) {
      if (DEBUG) {
      }
      return;
    }

    // Check if task is still active
    const task = activeTasks.get(data._id);
    if (!task || !task.running) {
      if (DEBUG) {
      }
      return;
    }

    const { address, walletId, slug, creatorFees, enforceCreatorFee, trait, contractAddress, _id } = data

    // Early wallet validation - fetch once and reuse later
    const wallet = await walletCache.getWalletById(walletId);
    if (!wallet || !wallet.encryptedPrivateKey) {
      if (DEBUG) {
      }
      return;
    }
    const privateKey: string = decryptPrivateKey(wallet.encryptedPrivateKey);

    // Skip trait validation - preprocessing already determined valid traits
    // The trait passed in the job data is already validated
    const parsedTrait = JSON.parse(trait);

    const currentTask = activeTasks.get(_id)
    if (!currentTask || !currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("opensea")) {
      if (DEBUG) {
      }
      return;
    }


    // Initialize stats early
    if (!floorPrices[_id]) {
      floorPrices[_id] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      };
    }

    if (!skipStats[_id]) {
      skipStats[_id] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      }
    }

    const expiry = getExpiry(currentTask.bidDuration)
    const { type, value } = JSON.parse(trait);
    const identifier = `${type}:${value}`

    // Parallel fetch: floor price and trait floor (if needed). Orders from cache.
    const traitPriceKeyExists = currentTask?.stopOptions?.traitPrices && currentTask?.stopOptions?.triggerStopOptions && `opensea:${identifier}` in currentTask?.stopOptions?.traitPrices;

    const [floor_price, traitFloor] = await Promise.all([
      getCachedFloorPrice(slug),
      traitPriceKeyExists
        ? openseaTraitFloor(slug, { traitType: type, values: [value] })
        : Promise.resolve(undefined)
    ]);
    const cachedOrder = await orderCache.getNewest(_id, OPENSEA.toLowerCase(), identifier);
    const orders = cachedOrder ? [cachedOrder] : [];


    if (!floor_price) { throw new Error(`Failed to fetch floor price for ${slug}`); }

    // Fix 9: Defensive null check - floorPrices may be deleted by cleanupTaskResources during async operations
    if (!floorPrices[_id]) {
      return;
    }
    floorPrices[_id].opensea = floor_price as number;

    // Standalone Floor log removed - combined with PriceCalc log below

    const stopBid = await stopOption(currentTask, 'opensea', Number(floor_price), false, traitFloor, identifier)
    if (stopBid) {
      if (DEBUG) {
      }
      if (orders.length > 0) {
        const cancelData = orders.map((order) => {
          return {
            name: CANCEL_OPENSEA_BID,
            data: { walletId, orderId: order.orderHash, taskId: _id, identifier: order.identifier },
          }
        })
        await queue.addBulk(cancelData)
      }
      return
    }

    // Use pre-calculated values if available (scheduled bids), otherwise calculate (counterbids/backwards compatibility)
    let maxBidPriceEth, offerPriceEth, minBidPriceEth, currencySymbol, currency, decimal;

    if (data.maxBidPriceEth !== undefined && data.offerPriceEth !== undefined) {
      // Use pre-calculated values from scheduled bid (saves 15-150ms)
      maxBidPriceEth = data.maxBidPriceEth;
      offerPriceEth = data.offerPriceEth;
      minBidPriceEth = data.minBidPriceEth!;
      currencySymbol = data.currencySymbol!;
      currency = data.currency!;
      decimal = data.decimal!;

    } else {
      // Fallback: Calculate (counterbids or backwards compatibility)
      const result = await calculateBidPrice(currentTask, floor_price as number, "opensea");
      maxBidPriceEth = result.maxBidPriceEth;
      offerPriceEth = result.offerPriceEth;
      minBidPriceEth = result.minBidPriceEth;
      currencySymbol = result.currencySymbol;
      currency = result.currency;
      decimal = result.decimal;

    }

    // === BALANCE PRE-CHECK ===
    const { checkBalanceBeforeBid } = await import('./utils/balance-helpers');
    const balanceCheck = await checkBalanceBeforeBid(
      _id, wallet.address, offerPriceEth, currency, currencySymbol, slug, 'opensea'
    );

    if (!balanceCheck.sufficient) {
      await logBidError(
        _id,
        "INSUFFICIENT BALANCE (PRE-CHECK)",
        `Insufficient ${currencySymbol} balance: required ${offerPriceEth} ${currencySymbol}, available ${balanceCheck.balance} ${currencySymbol} for wallet ${wallet.address} on collection ${slug} (trait: ${identifier})`,
        "skipped",
        "opensea"
      );

      if (!skipStats[_id]) skipStats[_id] = { opensea: 0, magiceden: 0, blur: 0 };
      skipStats[_id].opensea++;

      if (DEBUG) {
      }
      return;
    }

    const outbidOptions = currentTask.outbidOptions

    const usdcBid = currency.toLowerCase() === USDC.toLowerCase()
    let traitOffer = BigInt(Math.round(roundToValidPrecision(offerPriceEth) * Math.pow(10, decimal)))

    // Extract first active order (if any) with proper null guards
    const [order] = orders
    const hasActiveOrder = order && order.expiresAt && order.createdAt;
    // CachedOrder has expiresAt/createdAt as number (timestamp in ms)
    const orderExpiresAt = typeof order?.expiresAt === 'number' ? order.expiresAt : new Date(order?.expiresAt).getTime();
    const orderCreatedAt = typeof order?.createdAt === 'number' ? order.createdAt : new Date(order?.createdAt).getTime();
    const ttl = hasActiveOrder ? (orderExpiresAt - Date.now()) / 1000 : -1;
    const currentPrice = hasActiveOrder ? Number(order.offerPrice) : Number(traitOffer);
    const orderExpiration = hasActiveOrder
      ? Math.round((orderExpiresAt - orderCreatedAt) / 1000)
      : expiry;

    // Initialize spread info variables with defaults (used in PLACING BID log)
    let highestBidAmount = 0;
    let secondTopBidAmount = 0;
    let outbidMargin = 0;
    let isOwnBid = false;

    if (!outbidOptions.outbid) {
      // Skip if bid is still valid: TTL sufficient, same price, similar expiration
      if (hasActiveOrder && ttl >= MIN_BID_DURATION && currentPrice === Number(traitOffer) && Math.abs(orderExpiration - expiry) < 30) {
        if (DEBUG) {
        }
        return;
      }
    }

    else {      // Parallel fetch: offers and wallets
      const [offers, walletsArr] = await Promise.all([
        fetchOpenseaOffers('TRAIT', slug, JSON.parse(trait)),
        walletCache.getAllWallets()
      ]);

      const [topOffer, secondOffer] = offers as unknown as OpenseaOfferData[]
      highestBidAmount = usdcBid ? topOffer.usd * Math.pow(10, decimal) : topOffer.eth * Math.pow(10, decimal)
      secondTopBidAmount = usdcBid ? secondOffer.usd * Math.pow(10, decimal) : secondOffer.eth * Math.pow(10, decimal)
      outbidMargin = calculateOutbidMargin(highestBidAmount ? highestBidAmount / Math.pow(10, decimal) : offerPriceEth, currency);

      const owner = topOffer && typeof topOffer === 'object' ? topOffer.owner?.toLowerCase() : '';
      isOwnBid = walletsArr
        .map(addr => addr.address.toLowerCase())
        .includes(owner as string);

      // Top offer fetched from API

      if (isOwnBid) {
        const spreadRaw = (Number(highestBidAmount) - Number(secondTopBidAmount)) / Math.pow(10, decimal);
        const spread = roundToValidPrecision(spreadRaw);
        const secondTopBidAmountEth = Number(secondTopBidAmount) / Math.pow(10, decimal);
        const potentialNewBidEth = secondTopBidAmountEth + outbidMargin;


        if (spread > outbidMargin && Number(secondTopBidAmount) > 0 && potentialNewBidEth >= minBidPriceEth) {
          traitOffer = BigInt(Math.round(roundToValidPrecision(potentialNewBidEth) * Math.pow(10, decimal)));
          if (DEBUG) {
          }
        }
        else if (spread > outbidMargin && Number(secondTopBidAmount) > 0 && potentialNewBidEth < minBidPriceEth) {
          // Large spread but potential bid below min: keep current offer (don't lower below min)
          if (DEBUG) {
          }
        }
        else if (ttl > MIN_BID_DURATION) {
          if (DEBUG) {
          }
          return;
        } else {
          traitOffer = BigInt(Math.round(roundToValidPrecision(highestBidAmount / Math.pow(10, decimal)) * Math.pow(10, decimal)));
          if (DEBUG) {
          }
        }
      }
      else {
        const newBidEth = (highestBidAmount / Math.pow(10, decimal)) + outbidMargin;
        traitOffer = BigInt(Math.round(roundToValidPrecision(newBidEth) * Math.pow(10, decimal)));
      }
    }

    const bidCount = getIncrementedBidCount(OPENSEA, slug, _id)
    const offerEth = Number(traitOffer) / Math.pow(10, decimal);

    // CRITICAL: Always check maxBidPrice regardless of outbid setting
    if (maxBidPriceEth > 0 && offerEth >= maxBidPriceEth) {
      // Queue cancellation jobs asynchronously when exceeding max bid price
      // This prevents orphaned orders from accumulating when we can't compete
      if (orders.length > 0) {

        const cancelData = orders.map((order) => ({
          name: CANCEL_OPENSEA_OUTBID,
          data: {
            walletId,
            orderId: order.orderHash,
            taskId: _id,
            identifier,
            slug: order.slug,
            source: 'processOpenseaTraitBid_max_exceeded'
          },
          opts: getCancellationJobOptions(_id, order.orderHash, CANCEL_OPENSEA_OUTBID)
        }));

        await queue.addBulk(cancelData);

      }

      skipStats[_id]['opensea']++;
      const message = `❌ Offer price ${offerEth} ${currencySymbol} for ${slug} trait ${trait} bid exceeds max bid price ${maxBidPriceEth} ${currencySymbol} FOR OPENSEA.Skipping ...`.toUpperCase()
      logBidError(_id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "opensea"); // Non-blocking
      if (DEBUG) {
      }
      return;
    } else if (offerEth < minBidPriceEth) {
      traitOffer = BigInt(Math.round(roundToValidPrecision(minBidPriceEth) * Math.pow(10, decimal)));
    }

    if (orders.length > 0) {

      const cancelData = orders.map((order) => ({
        name: CANCEL_OPENSEA_OUTBID,
        data: {
          walletId,
          orderId: order.orderHash,
          taskId: _id,
          identifier,
          slug: order.slug,
          source: 'processOpenseaTraitBid'
        },
        opts: getCancellationJobOptions(_id, order.orderHash, CANCEL_OPENSEA_OUTBID)
      }));

      await queue.addBulk(cancelData);

    }

    const finalOfferEth = Number(traitOffer) / Math.pow(10, decimal);
    const topBidEth = highestBidAmount / Math.pow(10, decimal);
    const secondBidEth = secondTopBidAmount / Math.pow(10, decimal);

    try {
      // Log API start

      await bidOnOpensea(
        _id,
        bidCount,
        address,
        privateKey,
        slug,
        Number(traitOffer),
        creatorFees,
        expiry,
        currency,
        task.contract.contractAddress,
        trait
      );

      // Log API success
      // Log job completed

    } catch (bidError: any) {
      // Check if this is an insufficient balance error
      const handled = await handleInsufficientBalanceError(
        taskStatus,
        warningBids,
        warningMessage,
        bidError,
        _id,
        'opensea',
        address,
        currency,
        minBidPriceEth,
        identifier
      );

      if (handled) {
        // Balance error was handled (marketplace stopped or bid skipped)
        return;
      }

      // Not a balance error, format error message and re-throw
      errorMessage = extractDetailedOpenSeaError(bidError);

      errorLog('OpenSea', `OpenSea trait bid failed for ${_id} ${trait}: ${errorMessage}`);

      // Re-throw to be caught by outer catch block
      throw bidError;
    }

  } catch (error: any) {

    if (!errorMessage) {
      // Use the new detailed error extraction function
      errorMessage = extractDetailedOpenSeaError(error);
    }

    // Log bid result and job failed

    // Don't log bid errors for stopped/cancelled tasks
    if (errorMessage && !errorMessage.includes('Bid execution blocked - task') && !errorMessage.includes('is stopped')) {
      await logBidError(data._id, "OPENSEA TRAIT BID ERROR", `Error bidding on trait ${data.trait} in collection ${data.slug}: ${errorMessage}`, "error", "opensea");
    }

    // Throw the error
    throw error;
  }
}

async function processOpenseaTokenBid(data: IProcessOpenseaTokenBidData) {
  const taskId = data._id;
  // const tokenId = data.asset?.tokenId?.toString() || 'unknown';

  // Create trace for scheduled bid
  // Log job started

  // Check if task is blacklisted
  if (isTaskBlacklisted(taskId, 'opensea')) {
    return;
  }

  // Early exit if marketplace already stopped due to insufficient balance
  // This prevents thousands of redundant error logs when balance is exhausted
  // if (isMarketplaceStoppedDueToBalance(taskStatus, taskId, 'opensea')) {
  //   return;
  // }

  let errorMessage = ''
  try {

    if (!enabledMarketplaces.length) {
      return;
    }
    if (!enabledMarketplaces.map((m) => m.toLowerCase()).includes("opensea")) {
      return;
    }
    const { address, walletId, slug, creatorFees, asset, _id } = data

    // DEBUG: Log creatorFees received in token bid processor

    // Early wallet validation - fetch once and reuse later
    const wallet = await walletCache.getWalletById(walletId);
    if (!wallet || !wallet.encryptedPrivateKey) {
      return;
    }

    if (!currentTokenIds[_id]?.opensea?.includes(asset.tokenId.toString())) {
      if (DEBUG) {
      } return;
    }

    // Check if token is cached as trading disabled
    // if (tradingDisabledCache.isDisabled(asset.contractAddress, +asset.tokenId, 'opensea')) {
    //   const message = `Token ${asset.tokenId} is cached as trading disabled - skipping`;


    //   // Log the skip and update error stats
    //   logBidError(_id, "TRADING DISABLED (CACHED)", message, "skipped", "opensea"); // Non-blocking

    //   if (!errorStats[_id]) {
    //     errorStats[_id] = { opensea: 0, magiceden: 0, blur: 0 };
    //   }
    //   // Error tracking is now handled in queue event handlers

    //   if (DEBUG) {
    //   }
    //   return;
    // }

    const currentTask = activeTasks.get(_id)
    if (!currentTask || !currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("opensea")) {
      if (DEBUG) {
      } return;
    }

    // SIMPLIFIED: Removed pending counterbid check - BullMQ deterministic job IDs handle deduplication

    // Initialize stats early
    if (!floorPrices[_id]) {
      floorPrices[_id] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      };
    }

    if (!skipStats[_id]) {
      skipStats[_id] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      }
    }

    // Fetch orders from cache
    const cachedOrder = await orderCache.getNewest(_id, OPENSEA.toLowerCase(), asset.tokenId.toString());
    const orders = cachedOrder ? [cachedOrder] : [];

    // Use pre-calculated values if available (scheduled bids), otherwise calculate (counterbids/backwards compatibility)
    let maxBidPriceEth, offerPriceEth, minBidPriceEth, currencySymbol, currency, decimal;

    if (data.maxBidPriceEth !== undefined && data.offerPriceEth !== undefined) {
      // Use pre-calculated values from scheduled bid (saves 15-150ms)
      maxBidPriceEth = data.maxBidPriceEth;
      offerPriceEth = data.offerPriceEth;
      minBidPriceEth = data.minBidPriceEth!;
      currencySymbol = data.currencySymbol!;
      currency = data.currency!;
      decimal = data.decimal!;

    } else {
      // Fallback: Calculate (counterbids or backwards compatibility)
      const floor = data.floor_price || 0;
      const result = await calculateBidPrice(currentTask, floor, "opensea");
      maxBidPriceEth = result.maxBidPriceEth;
      offerPriceEth = result.offerPriceEth;
      minBidPriceEth = result.minBidPriceEth;
      currencySymbol = result.currencySymbol;
      currency = result.currency;
      decimal = result.decimal;

    }

    // === BALANCE PRE-CHECK ===
    const { checkBalanceBeforeBid } = await import('./utils/balance-helpers');
    const balanceCheck = await checkBalanceBeforeBid(
      _id, wallet.address, offerPriceEth, currency, currencySymbol, slug, 'opensea'
    );

    if (!balanceCheck.sufficient) {
      await logBidError(
        _id,
        "INSUFFICIENT BALANCE (PRE-CHECK)",
        `Insufficient ${currencySymbol} balance: required ${offerPriceEth} ${currencySymbol}, available ${balanceCheck.balance} ${currencySymbol} for wallet ${wallet.address} on collection ${slug}`,
        "skipped",
        "opensea"
      );

      if (!skipStats[_id]) skipStats[_id] = { opensea: 0, magiceden: 0, blur: 0 };
      skipStats[_id].opensea++; return;
    }

    const outbidOptions = currentTask.outbidOptions
    const expiry = getExpiry(currentTask.bidDuration)
    const usdcBid = currency.toLowerCase() === USDC.toLowerCase()

    let tokenOffer = BigInt(Math.round(roundToValidPrecision(offerPriceEth) * Math.pow(10, decimal)))

    // Extract first active order (if any) with proper null guards
    const [order] = orders
    const hasActiveOrder = order && order.expiresAt && order.createdAt;
    // CachedOrder has expiresAt/createdAt as number (timestamp in ms)
    const orderExpiresAt = typeof order?.expiresAt === 'number' ? order.expiresAt : new Date(order?.expiresAt).getTime();
    const orderCreatedAt = typeof order?.createdAt === 'number' ? order.createdAt : new Date(order?.createdAt).getTime();
    const ttl = hasActiveOrder ? (orderExpiresAt - Date.now()) / 1000 : -1;
    const currentPrice = hasActiveOrder ? Number(order.offerPrice) : Number(tokenOffer);
    const orderExpiration = hasActiveOrder
      ? Math.round((orderExpiresAt - orderCreatedAt) / 1000)
      : expiry;

    if (!outbidOptions.outbid) {
      // Skip if bid is still valid: TTL sufficient, same price, similar expiration
      if (hasActiveOrder && ttl >= MIN_BID_DURATION && currentPrice === Number(tokenOffer) && Math.abs(orderExpiration - expiry) < 30) {
        if (DEBUG) {
        } return;
      }
    } else {
      // === INTELLIGENT BID ADJUSTMENT FOR SCHEDULED BIDS ===
      // Note: This function ONLY processes scheduled bids (OPENSEA_TOKEN_BID).
      // Counterbids use a separate processor (openseaTokenCounterBid).

      // Resolve contract address for Max Pain collection override support
      const resolvedContractAddress = await resolveContractAddress(
        slug,
        asset.tokenId,
        asset.contractAddress
      );

      // BLOCKING fetch: wallets and item offers (needed for intelligent pricing)
      const [walletsArr, highestBid] = await Promise.all([
        walletCache.getAllWallets(),
        fetchItemOffers(resolvedContractAddress, asset.tokenId.toString())
      ]);

      // Validate fetchItemOffers result - throw error immediately if failed
      if (!highestBid || highestBid.length === 0) {
        // fetchItemOffers failed - THROW ERROR IMMEDIATELY (no cached order fallback for placing bids)
        errorLog('BidFlow',
          `[fetchItemOffers Failed] Cannot place bid without real-time market data | taskId=${_id} | collection=${slug} | token=${asset.tokenId} | Retrying job...`,
          undefined,
          { taskId: _id, marketplace: 'opensea', slug, tokenId: asset.tokenId }
        );

        // Throw error to trigger BullMQ retry - job will retry when API recovers
        throw new Error(`fetchItemOffers failed for ${slug} token ${asset.tokenId} - retrying until API recovers`);
      } else {
        const [topOffer, secondOffer] = highestBid;

        // Validate topOffer has valid data
        if (!topOffer || (topOffer.eth === 0 && topOffer.usd === 0)) {
          errorLog('BidFlow',
            `[Invalid TopOffer] API returned malformed data (zero values) | taskId=${_id} | collection=${slug} | token=${asset.tokenId} | Retrying job...`,
            undefined,
            { taskId: _id, marketplace: 'opensea', slug, tokenId: asset.tokenId, topOffer }
          );

          // Throw error - malformed API response should trigger retry, not blind minBid placement
          throw new Error(`fetchItemOffers returned invalid data (zero values) for ${slug} token ${asset.tokenId}`);
        } else {
          const highestBidAmount = usdcBid ? topOffer.usd * Math.pow(10, decimal) : topOffer.eth * Math.pow(10, decimal);

          // Safety check for zero bid amount
          if (highestBidAmount === 0) {
            errorLog('BidFlow',
              `[Zero HighestBid] Calculated bid amount is zero (malformed API data) | taskId=${_id} | collection=${slug} | token=${asset.tokenId} | topOffer.eth=${topOffer.eth} | topOffer.usd=${topOffer.usd} | Retrying job...`,
              undefined,
              { taskId: _id, marketplace: 'opensea', slug, tokenId: asset.tokenId, topOfferEth: topOffer.eth, topOfferUsd: topOffer.usd }
            );

            // Throw error - zero calculated bid indicates malformed data, should retry
            throw new Error(`Calculated bid amount is zero for ${slug} token ${asset.tokenId} (topOffer.eth=${topOffer.eth}, topOffer.usd=${topOffer.usd})`);
          } else {
            const secondTopBidAmount = usdcBid ? secondOffer.usd * Math.pow(10, decimal) : secondOffer.eth * Math.pow(10, decimal);
            const outbidMargin = calculateOutbidMargin(highestBidAmount ? highestBidAmount / Math.pow(10, decimal) : offerPriceEth, currency);

            const owner = topOffer && typeof topOffer === 'object' ? topOffer.owner?.toLowerCase() : '';
            const isOwnBid = walletsArr
              .map(addr => addr.address.toLowerCase())
              .includes(owner);

            if (isOwnBid) {
              // We own the top bid - apply spread optimization
              const spreadRaw = (Number(highestBidAmount) - Number(secondTopBidAmount)) / Math.pow(10, decimal);
              const spread = roundToValidPrecision(spreadRaw);
              const secondTopBidAmountEth = Number(secondTopBidAmount) / Math.pow(10, decimal);
              const potentialNewBidEth = secondTopBidAmountEth + outbidMargin;

              if (spread > outbidMargin && Number(secondTopBidAmount) > 0 && potentialNewBidEth >= minBidPriceEth) {
                // Large spread: lower our bid to just above second place
                tokenOffer = BigInt(Math.round(roundToValidPrecision(potentialNewBidEth) * Math.pow(10, decimal)));
              }
              else if (spread > outbidMargin && Number(secondTopBidAmount) > 0 && potentialNewBidEth < minBidPriceEth) {
                // Large spread but can't lower bid (would go below minimum) - keep current offer, skip rebid
                return;
              }
              else if (ttl > MIN_BID_DURATION) {
                // Our bid is still valid with sufficient TTL - skip rebid
                return;
              } else {
                // Renew at same price
                tokenOffer = BigInt(Math.round(roundToValidPrecision(highestBidAmount / Math.pow(10, decimal)) * Math.pow(10, decimal)));
              }
            }
            else {
              // Not our bid: outbid the current top bidder
              const newBidEth = (highestBidAmount / Math.pow(10, decimal)) + outbidMargin;
              tokenOffer = BigInt(Math.round(roundToValidPrecision(newBidEth) * Math.pow(10, decimal)));
            }
          }
        }
      }
    }

    const bidCount = getIncrementedBidCount(OPENSEA, slug, _id)
    const offerEth = Number(tokenOffer) / Math.pow(10, decimal);

    // CRITICAL: Always check maxBidPrice regardless of outbid setting
    // Previously only checked when outbidOptions.outbid was true, allowing bids above max
    if (maxBidPriceEth > 0 && offerEth >= maxBidPriceEth) {
      // Always cancel existing orders when exceeding max bid price
      // This prevents orphaned orders from accumulating when we can't compete
      if (orders.length > 0) {
        const cancelData = orders.map((order) => ({
          name: CANCEL_OPENSEA_BID,
          data: { walletId, orderId: order.orderHash, taskId: _id, identifier: order.identifier }
        }));
        queue.addBulk(cancelData);
      }

      const message = `Offer price ${offerEth} ${currencySymbol} for ${slug} ${asset.tokenId} bid exceeds max bid price ${maxBidPriceEth} ${currencySymbol} FOR OPENSEA. Skipping...`.toUpperCase()
      skipStats[_id].opensea++;
      logBidError(_id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "opensea"); return;
    } else if (offerEth < minBidPriceEth) {
      tokenOffer = BigInt(Math.round(roundToValidPrecision(minBidPriceEth) * Math.pow(10, decimal)));
    }

    // Decrypt private key from wallet (already validated at function start)
    const privateKey: string = decryptPrivateKey(wallet.encryptedPrivateKey);

    // Queue cancellation jobs asynchronously (non-blocking)
    if (orders.length > 0) {
      const tokenId = asset.tokenId.toString();


      const cancelData = orders.map((order) => ({
        name: CANCEL_OPENSEA_OUTBID,
        data: {
          walletId,
          orderId: order.orderHash,
          taskId: _id,
          identifier: tokenId,
          slug: order.slug,
          source: 'processOpenseaTokenBid'
        },
        opts: getCancellationJobOptions(_id, order.orderHash, CANCEL_OPENSEA_OUTBID)
      }));

      await queue.addBulk(cancelData);

    }

    const finalOfferEth = Number(tokenOffer) / Math.pow(10, decimal);

    // Log bid decision context for debugging
    if (Math.abs(finalOfferEth - minBidPriceEth) < 0.0001) {
      warnLog('BidFlow',
        `[MinBid Fallback] Placing bid at minimum price | taskId=${_id} | collection=${slug} | ` +
        `token=${asset.tokenId} | minBid=${minBidPriceEth} | finalBid=${finalOfferEth} | ` +
        `Calculated bid was below minimum threshold (likely due to: no existing competitive offers, ` +
        `low top bid, spread optimization, or user-configured price below minimum)`,
        { taskId: _id, marketplace: 'opensea', slug, tokenId: asset.tokenId, minBidPriceEth, finalOfferEth }
      );
    }

    try {
      await bidOnOpensea(
        _id,
        bidCount,
        address,
        privateKey,
        slug,
        Number(tokenOffer),
        creatorFees,
        expiry,
        currency,
        currentTask.contract.contractAddress,
        undefined,
        asset
      )

      // Bid successful

    } catch (bidError: any) {

      // Format OpenSea error message for better readability
      let primaryMessage = '';
      if (bidError?.response?.data?.errors && Array.isArray(bidError.response.data.errors)) {
        // Extract the first error message from OpenSea API errors
        const extracted = bidError.response.data.errors[0];
        primaryMessage = typeof extracted === 'string'
          ? extracted
          : extracted?.message || 'Unknown OpenSea error';

        // Special handling for "Trading is disabled" and "Trading is not enabled" errors
        if (primaryMessage.includes('Trading is disabled') || primaryMessage.includes('Trading is not enabled')) {
          // Cache the disabled token
          tradingDisabledCache.markAsDisabled(
            data.asset.contractAddress,
            +data.asset.tokenId,
            'opensea',
            primaryMessage
          );

          // Remove token from the list to avoid repeated attempts
          if (currentTokenIds[data._id]?.opensea) {
            const tokenIndex = currentTokenIds[data._id].opensea.indexOf(data.asset.tokenId.toString());
            if (tokenIndex > -1) {
              currentTokenIds[data._id].opensea.splice(tokenIndex, 1);
            }
          }

          // Increment skipStats for this task
          if (!skipStats[data._id]) skipStats[data._id] = { opensea: 0, magiceden: 0, blur: 0 };
          skipStats[data._id].opensea++;

          // Log as warning with updated message
          await logBidError(data._id, "OPENSEA TOKEN TRADING DISABLED",
            `Token ${data.asset.tokenId} in collection ${data.slug}: ${primaryMessage} - Cached for 24 hours`,
            "warning", "opensea");

          errorMessage = primaryMessage; return; // Exit early without re-throwing
        }

        // Special handling for "API key has expired" errors (HTTP 401)
        if (bidError?.response?.status === 401 || primaryMessage.includes('API key has expired')) {
          // Log critical error for API key expiration
          errorLog(
            'API',
            `OPENSEA_API_KEY_EXPIRED - OpenSea API key has expired - manual renewal required. Task: ${data._id}, Token: ${data.asset.tokenId}`,
            bidError
          );

          errorLog('OpenSea', 'CRITICAL: OpenSea API key has expired! All OpenSea bids will fail until key is renewed.');

          errorMessage = 'API key has expired - manual renewal required';

          // Re-throw to fail the job and trigger retry via adaptive rate limiting
          throw bidError;
        }
      }

      if (!primaryMessage) {
        primaryMessage = bidError?.message || String(bidError);
      }

      const detailedError = extractDetailedOpenSeaError(bidError);
      if (detailedError) {
        errorMessage = primaryMessage && !detailedError.includes(primaryMessage)
          ? `${primaryMessage} | ${detailedError}`
          : detailedError;
      } else {
        errorMessage = primaryMessage || 'Unknown OpenSea error';
      }

      // Check if this is an insufficient balance error
      const handled = await handleInsufficientBalanceError(
        taskStatus,
        warningBids,
        warningMessage,
        bidError,
        _id,
        'opensea',
        address,
        currency,
        minBidPriceEth,
        asset.tokenId.toString()
      );

      if (handled) {
        // Balance error was handled (marketplace stopped or bid skipped)        return;
      }

      // Not a balance error - Throttle logging to prevent log flooding
      const isBalanceError = errorMessage.includes('InsufficientBalanceError') ||
        errorMessage.includes('INSUFFICIENT_BALANCE') ||
        errorMessage.includes('Insufficient') && errorMessage.includes('balance');

      if (!isBalanceError || shouldLogBalanceError(_id, 'opensea')) {
        errorLog('OpenSea', `OpenSea token bid failed for ${_id} token ${data.asset.tokenId}: ${errorMessage}`);
      }

      // Re-throw to be caught by outer catch block
      throw bidError;
    }
    // SIMPLIFIED: Removed pending counterbid cleanup - no longer needed


  } catch (error: any) {
    if (!errorStats[data._id]) {
      errorStats[data._id] = {
        magiceden: 0,
        opensea: 0,
        blur: 0
      }
    }

    if (!errorMessage) {
      // Use the new detailed error extraction function
      errorMessage = extractDetailedOpenSeaError(error);
    }

    // Log bid result and job failed

    // Error stats already initialized above
    // Error tracking is now handled in queue event handlers

    await logBidError(data._id, "OPENSEA TOKEN BID ERROR", `Error bidding on token ${data.asset.tokenId} in collection ${data.slug}: ${errorMessage}`, "error", "opensea"); throw error;
  }
}

async function openseaCollectionCounterBid(job: Job<IOpenseaBidParams>) {
  const data = job.data;
  const { taskId, bidCount, walletAddress, walletId, slug, offerPrice, creatorFees, expiry, floor_price, enqueuedAt } = data;
  let success = false;

  // [18] WORKER ENTRY

  // [19] WORKER VALIDATION CHECKS
  if (!enabledMarketplaces.length) {
    return;
  }

  const currentTask = activeTasks.get(taskId);

  if (!currentTask) {
    return;
  }

  if (!currentTask.running) {
    return;
  }

  const selectedIncludesOpensea = currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("opensea");
  if (!selectedIncludesOpensea) {
    return;
  }

  const openseaGloballyEnabled = enabledMarketplaces.map((m) => m.toLowerCase()).includes("opensea");
  if (!openseaGloballyEnabled) {
    return;
  }

  const { currency } = getCurrencyDetailsFromTask(currentTask, "opensea");

  try {
    const cachedOrder = await orderCache.getNewest(taskId, "opensea", "collection");
    const existingOrders = cachedOrder ? [cachedOrder] : [];

    const wallet = await walletCache.getWalletById(walletId);
    const privateKey: string = decryptPrivateKey(wallet?.encryptedPrivateKey as string);

    // Queue cancellation jobs asynchronously (non-blocking)
    if (existingOrders.length > 0) {
      // Log queue entry

      const cancelData = existingOrders.map((order) => ({
        name: CANCEL_OPENSEA_COUNTERBID,
        data: {
          walletId,
          orderId: order.orderHash,
          taskId,
          identifier: 'collection',
          slug: order.slug,
          source: 'openseaCollectionCounterBid'
        },
        opts: getCancellationJobOptions(taskId, order.orderHash, CANCEL_OPENSEA_COUNTERBID)
      }));

      await queue.addBulk(cancelData);

      // Log queue success
    }

    // [20] WORKER BID PLACEMENT

    await bidOnOpensea(
      taskId,
      bidCount,
      walletAddress,
      privateKey,
      slug,
      Number(offerPrice),
      creatorFees,
      expiry,
      currency,
      currentTask.contract.contractAddress,
      undefined,
      undefined,
      'counterbid'
    );


    if (!counterbidCounts[taskId]) {
      counterbidCounts[taskId] = {
        opensea: 0,
        magiceden: 0,
        blur: 0,
      };
    }
    counterbidCounts[taskId]["opensea"]++;
    success = true;  // Mark as successful before return
  } catch (error: any) {
    success = false;
    const errorMessage = extractDetailedOpenSeaError(error);
    errorLog('OpenSea', `Error during OpenSea collection counterbid | taskId=${taskId}`, error);
    await logBidError(data.taskId, "OPENSEA COLLECTION COUNTER BID ERROR", `Error processing OpenSea collection counter bid for task: ${data.slug}: ${errorMessage}`, "error", "opensea");
  } finally {
    // Record execution latency (proxy → job completion)
    recordCounterbidExecution('opensea', job.id!, enqueuedAt || Date.now(), success);
  }
}

async function magicedenCollectionCounterBid(job: Job<IMagicedenCollectionBidData>) {
  const data = job.data;
  if (!enabledMarketplaces.length) {
    if (DEBUG) {
    }
    return;
  }
  const { taskId, bidCount, address, contractAddress, quantity, offerPrice, expiration, walletId, slug, enqueuedAt } = data;
  let success = false;
  const currentTask = activeTasks.get(taskId)
  if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("magiceden") || !enabledMarketplaces.map((m) => m.toLowerCase()).includes("magiceden")) {
    if (DEBUG) {
    }
    return;
  }

  try {
    // Query and cancel existing collection orders
    const cachedOrder = await orderCache.getNewest(taskId, MAGICEDEN.toLowerCase(), "collection");
    const orders = cachedOrder ? [cachedOrder] : [];

    const { currency, currencySymbol } = getCurrencyDetailsFromTask(currentTask, "magiceden");
    const offerEth = Number(offerPrice) / 1e18;

    // Fetch wallet and decrypt private key before cancellation (needed for cancelMagicEdenOrdersDirectly)
    const wallet = await walletCache.getWalletById(walletId);
    const privateKey: string = decryptPrivateKey(wallet?.encryptedPrivateKey as string);

    // Cancel existing orders synchronously BEFORE placing new bid
    if (orders.length > 0) {
      await cancelMagicEdenOrdersDirectly({
        taskId,
        walletId,
        privateKey,
        orders,
        source: 'magicedenCollectionCounterBid',
        identifier: 'collection',
        reason: 'counterbid_replace_existing'
      });
    }

    if (DEBUG) {
    }

    await bidOnMagiceden(taskId, bidCount, address, contractAddress, quantity, offerPrice.toString(), privateKey, slug, expiration, currency, undefined, undefined, 'counterbid');

    // Initialize and increment counterbidCounts after successful bid
    if (!counterbidCounts[taskId]) {
      counterbidCounts[taskId] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      };
    }
    counterbidCounts[taskId]['magiceden']++;

    success = true;  // Mark as successful before return
    // Log job completed
  } catch (error: any) {
    success = false;
    await logBidError(taskId, "MAGICEDEN COLLECTION COUNTER BID ERROR", `Error processing MagicEden collection counter bid for task: ${data.slug}: ${error?.response?.data?.message || error?.response?.data || error?.message || String(error)}`, "error", "magiceden");
  } finally {
    // Record execution latency (Stage 2: Job completion)
    recordCounterbidExecution('magiceden', job.id!, enqueuedAt || Date.now(), success);
  }
}

interface IMagicedenTraitBidData {
  taskId: string
  bidCount: string
  address: string
  contractAddress: string
  quantity: number
  offerPrice: string  // Changed from number to string to preserve Wei precision
  expiration: number
  walletId: string
  slug: string
  trait: string
  enqueuedAt?: number
}

async function magicedenTraitCounterBid(job: Job<IMagicedenTraitBidData>) {
  const data = job.data;
  // Extract or create trace context
  const { taskId, bidCount, address, contractAddress, quantity, offerPrice, expiration, walletId, slug, trait, enqueuedAt } = data;
  let success = false;
  const triatData = JSON.parse(trait)
  const bidTrait = {
    attributeKey: triatData?.attributeKey,
    attributeValue: triatData?.attributeValue,
  }
  const traitKey = `${bidTrait.attributeKey}:${bidTrait.attributeValue}`;

  // Log job started

  if (!enabledMarketplaces.length) {
    if (DEBUG) {
    }
    return;
  }

  const currentTask = activeTasks.get(taskId)
  if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("magiceden") || !enabledMarketplaces.map((m) => m.toLowerCase()).includes("magiceden")) {
    if (DEBUG) {
    }
    return;
  }

  try {
    // Query existing trait orders
    const identifier = traitKey;
    const cachedOrder = await orderCache.getNewest(taskId, MAGICEDEN.toLowerCase(), identifier);
    const orders = cachedOrder ? [cachedOrder] : [];

    const { currencyType, currency } = getCurrencyDetailsFromTask(currentTask, "magiceden");
    const offerEth = Number(offerPrice) / 1e18;

    // Fetch wallet and decrypt private key just before use
    const wallet = await walletCache.getWalletById(walletId);
    const privateKey: string = decryptPrivateKey(wallet?.encryptedPrivateKey as string);

    // Cancel existing orders synchronously BEFORE placing new bid
    if (orders.length > 0) {
      await cancelMagicEdenOrdersDirectly({
        taskId,
        walletId,
        privateKey,
        orders,
        source: 'magicedenTraitCounterBid',
        identifier,
        reason: 'counterbid_replace_existing'
      });
    }

    // Log API start

    if (DEBUG) {
    }

    await bidOnMagiceden(taskId, bidCount, address, contractAddress, quantity, offerPrice.toString(), privateKey, slug, expiration, currency, bidTrait, undefined, 'counterbid');

    // Log API success

    // Initialize and increment counterbidCounts after successful bid
    if (!counterbidCounts[taskId]) {
      counterbidCounts[taskId] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      };
    }
    counterbidCounts[taskId]['magiceden']++;

    success = true;  // Mark as successful before return
    // Log job completed
  } catch (error: any) {
    success = false;
    const errorMsg = error?.response?.data?.message || error?.response?.data || error?.message || String(error);
    await logBidError(data.taskId, "MAGICEDEN TRAIT COUNTER BID ERROR", `Error processing MagicEden trait counter bid for task: ${data.slug}: ${errorMsg}`, "error", "magiceden");
  } finally {
    // Record execution latency (Stage 2: Job completion)
    recordCounterbidExecution('magiceden', job.id!, enqueuedAt || Date.now(), success);
  }
}

async function blurCollectionCounterBid(job: Job<IBlurBidData>) {
  const data = job.data;
  // Extract or create trace context
  const { taskId, bidCount, address, walletId, contractAddress, offerPrice, slug, expiry, enqueuedAt } = data;
  let success = false;

  // Log job started

  if (!enabledMarketplaces.length) {
    if (DEBUG) {
    }
    return
  }

  const currentTask = activeTasks.get(taskId)

  if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("blur") || !enabledMarketplaces.map((m) => m.toLowerCase()).includes("blur")) {
    if (DEBUG) {
    }
    return
  }


  const offerEth = Number(offerPrice) / 1e18;

  try {
    // Query existing collection orders
    const identifier = "collection";

    const cachedOrder = await orderCache.getNewest(taskId, "blur", identifier);
    const existingOrders = cachedOrder ? [cachedOrder] : [];

    // Fetch wallet and decrypt private key just before use
    const wallet = await walletCache.getWalletById(walletId);
    const privateKey: string = decryptPrivateKey(wallet?.encryptedPrivateKey as string);

    // Cancel existing orders synchronously BEFORE placing new bid
    if (existingOrders.length > 0) {
      await cancelBlurOrdersDirectly({
        taskId,
        walletId,
        privateKey,
        orders: existingOrders,
        source: 'blurCollectionCounterBid',
        identifier,
        reason: 'counterbid_replace_existing'
      });
    }

    if (DEBUG) {
    }

    // Log API start

    await bidOnBlur(taskId, bidCount, address, privateKey, contractAddress, BigInt(offerPrice), slug, expiry, undefined, undefined, 'counterbid');

    // Log API success

    // Initialize and increment counterbidCounts after successful bid
    if (!counterbidCounts[taskId]) {
      counterbidCounts[taskId] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      };
    }
    counterbidCounts[taskId]['blur']++;

    success = true;  // Mark as successful before return
    // Log job completed

  } catch (error: any) {
    success = false;
    const errorMsg = error?.response?.data?.message || error?.response?.data || error?.message || String(error);

    // Handle rate limit errors
    if (error?.code === 'RATE_LIMITED') {
      throw error; // Re-throw to let worker handle retry with delay
    }

    // Handle transfer restriction errors
    if (error.code === 'TRANSFER_RESTRICTED') {
      await logBidError(taskId, "BLUR TRANSFER RESTRICTION", error.message, "warning", "blur");
      return;
    }

    await logBidError(taskId, "BLUR COLLECTION COUNTER BID ERROR", `Error processing Blur collection counter bid for task: ${slug}: ${errorMsg}`, "error", "blur");
  } finally {
    // Record execution latency (Stage 2: Job completion)
    recordCounterbidExecution('blur', job.id!, enqueuedAt || Date.now(), success);
  }
}

async function blurTraitCounterBid(job: Job<IBlurBidData>) {
  const data = job.data;
  // Extract or create trace context
  const { taskId, bidCount, address, walletId, contractAddress, offerPrice, slug, expiry, traits, enqueuedAt } = data;
  let success = false;

  // Parse traits early to get identifier for logging
  let traitKey = 'unknown';
  if (traits) {
    try {
      const traitsObj = JSON.parse(traits);
      const [traitType, traitValue] = Object.entries(traitsObj)[0] as string[];
      traitKey = `${traitType}:${traitValue}`;
    } catch { /* ignore parse errors */ }
  }

  // Log job started

  if (!enabledMarketplaces.length) {
    if (DEBUG) {
    }
    return
  }

  const currentTask = activeTasks.get(taskId)
  if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("blur") || !enabledMarketplaces.map((m) => m.toLowerCase()).includes("blur")) {
    if (DEBUG) {
    }
    return
  }

  try {
    if (!traits) {
      if (DEBUG) {
      }
      return
    }
    const traitsObj = JSON.parse(traits);
    const [traitType, traitValue] = Object.entries(traitsObj)[0] as string[];
    const identifier = `${traitType}:${traitValue}`;

    // Query existing orders for this trait
    const cachedOrder = await orderCache.getNewest(taskId, "blur", identifier);
    const existingOrders = cachedOrder ? [cachedOrder] : [];

    // Fetch wallet and decrypt private key just before use
    const wallet = await walletCache.getWalletById(walletId);
    const privateKey: string = decryptPrivateKey(wallet?.encryptedPrivateKey as string);

    // Cancel existing orders synchronously BEFORE placing new bid
    if (existingOrders.length > 0) {
      await cancelBlurOrdersDirectly({
        taskId,
        walletId,
        privateKey,
        orders: existingOrders,
        source: 'blurTraitCounterBid',
        identifier,
        reason: 'counterbid_replace_existing'
      });

    }

    const offerEth = Number(offerPrice) / 1e18;

    if (DEBUG) {
    }

    // Log API start

    await bidOnBlur(taskId, bidCount, address, privateKey, contractAddress, BigInt(offerPrice), slug, expiry, traits, undefined, 'counterbid');

    // Log API success

    // Initialize and increment counterbidCounts after successful bid
    if (!counterbidCounts[taskId]) {
      counterbidCounts[taskId] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      };
    }
    counterbidCounts[taskId]['blur']++;

    success = true;  // Mark as successful before return
    // Log job completed

  } catch (error: any) {
    success = false;
    const errorMsg = error?.response?.data?.message || error?.response?.data || error?.message || String(error);

    // Handle rate limit errors
    if (error?.code === 'RATE_LIMITED') {
      throw error; // Re-throw to let worker handle retry with delay
    }

    // Handle transfer restriction errors
    if (error.code === 'TRANSFER_RESTRICTED') {
      await logBidError(data.taskId, "BLUR TRANSFER RESTRICTION", error.message, "warning", "blur");
      return;
    }

    logBidError(data.taskId, "BLUR TRAIT COUNTER BID ERROR", `Error processing Blur trait counter bid for task: ${data.slug}: ${errorMsg}`, "error", "blur");
  } finally {
    // Record execution latency (Stage 2: Job completion)
    recordCounterbidExecution('blur', job.id!, enqueuedAt || Date.now(), success);
  }
}

interface BlurTraitCounterBid {
  taskId: string;
  bidCount: string;
  address: string;
  walletId: string;
  contractAddress: string;
  offerPrice: string;  // Changed from number to string to preserve Wei precision
  slug: string;
  expiry: number;
  traits: string;
}

async function openseaTokenCounterBid(job: Job<IProcessOpenseaTokenBidData>) {
  const data = job.data;
  const {
    _id,
    address,
    walletId,
    slug,
    offerPrice,
    creatorFees,
    asset,
    enqueuedAt } = data;
  const taskId = _id;
  let success = false;

  // Start timing for end-to-end execution measurement

  // Log job started

  if (!enabledMarketplaces.length) {
    if (DEBUG) {
    } return;
  }

  const currentTask = activeTasks.get(taskId);

  if (!currentTask) {
    if (DEBUG) {
    } return;
  }

  if (!currentTask.running) {
    if (DEBUG) {
    } return;
  }

  const selectsOpensea = currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("opensea");
  if (!selectsOpensea) {
    if (DEBUG) {
    } return;
  }

  const openseaEnabled = enabledMarketplaces.map((m) => m.toLowerCase()).includes("opensea");
  if (!openseaEnabled) {
    if (DEBUG) {
    } return;
  }

  const expiry = getExpiry(currentTask.bidDuration);

  const bidCount = getIncrementedBidCount(OPENSEA, slug, taskId);

  try {
    const cachedOrder = await orderCache.getNewest(taskId, OPENSEA.toLowerCase(), asset.tokenId.toString());
    const orders = cachedOrder ? [cachedOrder] : [];

    // Check if we need to cancel existing orders
    if (orders.length > 0) {
      const mostRecentOrder = orders[0]; // Cache already stores the most recent order

      // Validate offerPrice is a valid Wei integer string before BigInt conversion
      if (!/^\d+$/.test(offerPrice)) {
        errorLog('OpenSea', `[openseaTokenCounterBid] Invalid offerPrice format | taskId=${taskId} | tokenId=${asset.tokenId} | offerPrice=${offerPrice} | Expected Wei integer string`, new Error('Invalid offerPrice'));
        return;
      }

      const currentOfferWei = BigInt(mostRecentOrder.offerPrice);
      const incomingOfferWei = BigInt(offerPrice);

      if (currentOfferWei >= incomingOfferWei) {
        if (DEBUG) {
        } return;
      }
    }

    const { currency, decimal, currencySymbol } = getCurrencyDetailsFromTask(currentTask, "opensea");
    const offerPriceEth = Number(offerPrice) / Math.pow(10, decimal);

    // Fetch wallet for balance check
    const wallet = await walletCache.getWalletById(walletId);
    if (!wallet || !wallet.encryptedPrivateKey) {
      return;
    }

    // === BALANCE PRE-CHECK ===
    const { checkBalanceBeforeBid } = await import('./utils/balance-helpers');
    const balanceCheck = await checkBalanceBeforeBid(
      taskId, wallet.address, offerPriceEth, currency, currencySymbol, slug, 'opensea'
    );

    if (!balanceCheck.sufficient) {
      await logBidError(
        taskId,
        "INSUFFICIENT BALANCE (PRE-CHECK)",
        `Insufficient ${currencySymbol} balance: required ${offerPriceEth} ${currencySymbol}, available ${balanceCheck.balance} ${currencySymbol} for wallet ${wallet.address} on collection ${slug} (counterbid)`,
        "skipped",
        "opensea"
      );

      if (!skipStats[taskId]) skipStats[taskId] = { opensea: 0, magiceden: 0, blur: 0 };
      skipStats[taskId].opensea++; return;
    }

    // Continue with private key decryption
    const privateKey: string = decryptPrivateKey(wallet.encryptedPrivateKey);

    // Queue cancellation jobs asynchronously (non-blocking)
    if (orders.length > 0) {
      const tokenId = asset.tokenId.toString();


      const cancelData = orders.map((order) => ({
        name: CANCEL_OPENSEA_COUNTERBID,
        data: {
          walletId,
          orderId: order.orderHash,
          taskId,
          identifier: tokenId,
          slug: order.slug,
          source: 'openseaTokenCounterBid'
        },
        opts: getCancellationJobOptions(taskId, order.orderHash, CANCEL_OPENSEA_COUNTERBID)
      }));

      await queue.addBulk(cancelData);

    }

    const offerEth = Number(offerPrice) / Math.pow(10, decimal);
    if (DEBUG) {
    }


    await bidOnOpensea(
      taskId,
      bidCount,
      address,
      privateKey,
      slug,
      Number(offerPrice),
      creatorFees,
      expiry,
      currency,
      currentTask.contract.contractAddress,
      undefined,
      asset,
      'counterbid'
    );


    if (!counterbidCounts[taskId]) {
      counterbidCounts[taskId] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      };
    }
    counterbidCounts[taskId]['opensea']++;

    success = true;  // Mark as successful before return
    // Log job completed
  } catch (error: any) {
    success = false;
    const errorMessage = extractDetailedOpenSeaError(error);
    errorLog('OpenSea', `Error processing OpenSea token counterbid | taskId=${taskId}`, error);
    await logBidError(data._id, "OPENSEA TOKEN COUNTER BID ERROR", `Error bidding on token ${data.asset.tokenId} in collection ${data.slug}: ${errorMessage}`, "error", "opensea");
  } finally {
    // Record execution latency (Stage 2: Job completion)
    recordCounterbidExecution('opensea', job.id!, enqueuedAt || Date.now(), success);
  }
  // SIMPLIFIED: Removed pending counterbid cleanup - no longer needed
}

async function openseaTraitCounterBid(job: Job<IProcessOpenseaTraitBidData>) {
  const data = job.data;
  const { _id, bidCount, address, walletId, slug, offerPrice, creatorFees, expiry, trait, floor_price, enqueuedAt } = data;
  const taskId = _id;
  let success = false;

  if (!enabledMarketplaces.length) {
    if (DEBUG) {
    }
    return;
  }

  const currentTask = activeTasks.get(taskId);

  if (!currentTask) {
    if (DEBUG) {
    }
    return;
  }

  if (!currentTask.running) {
    if (DEBUG) {
    }
    return;
  }

  const selectsOpensea = currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("opensea");
  if (!selectsOpensea) {
    if (DEBUG) {
    }
    return;
  }

  const openseaEnabled = enabledMarketplaces.map((m) => m.toLowerCase()).includes("opensea");
  if (!openseaEnabled) {
    if (DEBUG) {
    }
    return;
  }

  if (!trait) {
    if (DEBUG) {
    }
    return;
  }

  const traitObj: { type: string; value: string } = JSON.parse(trait);
  const identifier = `${traitObj.type}:${traitObj.value}`;

  try {
    const cachedOrder = await orderCache.getNewest(taskId, OPENSEA.toLowerCase(), identifier);
    const orders = cachedOrder ? [cachedOrder] : [];

    const wallet = await walletCache.getWalletById(walletId);
    const privateKey: string = decryptPrivateKey(wallet?.encryptedPrivateKey as string);

    if (orders.length > 0) {

      const cancelData = orders.map((order) => ({
        name: CANCEL_OPENSEA_COUNTERBID,
        data: {
          walletId,
          orderId: order.orderHash,
          taskId,
          identifier,
          slug: order.slug,
          source: 'openseaTraitCounterBid'
        },
        opts: getCancellationJobOptions(taskId, order.orderHash, CANCEL_OPENSEA_COUNTERBID)
      }));

      await queue.addBulk(cancelData);

    }

    const { currency, decimal, currencySymbol } = getCurrencyDetailsFromTask(currentTask, "opensea");
    const offerEth = Number(offerPrice) / Math.pow(10, decimal);

    if (DEBUG) {
    }

    await bidOnOpensea(
      taskId,
      bidCount,
      address,
      privateKey,
      slug,
      Number(offerPrice),
      creatorFees,
      expiry,
      currency,
      currentTask.contract.contractAddress,
      trait,
      undefined,
      'counterbid'
    );

    if (!counterbidCounts[taskId]) {
      counterbidCounts[taskId] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      };
    }
    counterbidCounts[taskId]['opensea']++;

    success = true;  // Mark as successful before return
    // Log job completed
  } catch (error: any) {
    success = false;
    const errorMessage = extractDetailedOpenSeaError(error);
    errorLog('OpenSea', `Error processing OpenSea trait counterbid | taskId=${taskId}`, error);
    await logBidError(data._id, "OPENSEA TRAIT COUNTER BID ERROR", `Error bidding on trait ${data.trait} in collection ${data.slug}: ${errorMessage}`, "error", "opensea");
    throw error;
  } finally {
    // Record execution latency (Stage 2: Job completion)
    recordCounterbidExecution('opensea', job.id!, enqueuedAt || Date.now(), success);
  }
}

async function magicedenTokenCounterBid(job: Job<IMagicedenTokenBidData>) {
  const data = job.data;
  // Extract or create trace context
  const { enqueuedAt } = data;
  let success = false;
  // Log job started

  try {
    if (!enabledMarketplaces.length) {
      if (DEBUG) {
      }
      return;
    }
    const { taskId, address, contractAddress, quantity, offerPrice, walletId, slug, tokenId, expiration } = data
    const currentTask = activeTasks.get(taskId)
    if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("magiceden") || !enabledMarketplaces.map((m) => m.toLowerCase()).includes("magiceden")) {
      if (DEBUG) {
      }
      return;
    }

    // Check if current offer in database is greater than incoming offer price
    const cachedOrder = await orderCache.getNewest(taskId, MAGICEDEN.toLowerCase(), tokenId.toString());
    const orders = cachedOrder ? [cachedOrder] : [];

    // Check if we need to cancel existing orders
    if (orders.length > 0 && offerPrice !== undefined) {
      const mostRecentOrder = orders[0]; // Cache already stores the most recent order

      // Validate offerPrice is a valid Wei integer string before BigInt conversion
      if (!/^\d+$/.test(offerPrice)) {
        errorLog('MagicEden', `[magicedenTokenCounterBid] Invalid offerPrice format | taskId=${taskId} | tokenId=${tokenId} | offerPrice=${offerPrice} | Expected Wei integer string`, new Error('Invalid offerPrice'));
        return;
      }

      const currentOfferWei = BigInt(mostRecentOrder.offerPrice);
      const incomingOfferWei = BigInt(offerPrice);

      if (currentOfferWei >= incomingOfferWei) {
        if (DEBUG) {
        }
        return;
      }
    }

    const { currency } = getCurrencyDetailsFromTask(currentTask, "magiceden");
    const offerEth = Number(offerPrice) / 1e18;
    const bidCount = getIncrementedBidCount(MAGICEDEN, slug, taskId)
    // Fetch wallet and decrypt private key just before use
    const wallet = await walletCache.getWalletById(walletId);
    const privateKey: string = decryptPrivateKey(wallet?.encryptedPrivateKey as string);

    // Cancel existing orders synchronously BEFORE placing new bid
    if (orders.length > 0) {
      await cancelMagicEdenOrdersDirectly({
        taskId,
        walletId,
        privateKey,
        orders,
        source: 'magicedenTokenCounterBid',
        identifier: tokenId.toString(),
        reason: 'counterbid_replace_existing'
      });
    }

    // Log API start

    if (DEBUG) {
    }

    await bidOnMagiceden(taskId, bidCount, address, contractAddress, quantity, offerPrice!, privateKey, slug, expiration, currency, undefined, tokenId, 'counterbid')

    // Log API success

    // Initialize and increment counterbidCounts after successful bid
    if (!counterbidCounts[taskId]) {
      counterbidCounts[taskId] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      };
    }
    counterbidCounts[taskId]['magiceden']++;

    success = true;  // Mark as successful before return
    // Log job completed
  } catch (error: any) {
    success = false;
    const errorMsg = error?.response?.data?.message || error?.response?.data || error?.message || String(error);
    await logBidError(data.taskId, "MAGICEDEN TOKEN COUNTER BID ERROR", `Error bidding on token ${data.tokenId} in collection ${data.slug}: ${errorMsg}`, "error", "magiceden");
  } finally {
    // Record execution latency (Stage 2: Job completion)
    recordCounterbidExecution('magiceden', job.id!, enqueuedAt || Date.now(), success);
  }
}


async function processBlurTraitBid(data: {
  _id: string;
  address: string;
  walletId: string;
  contractAddress: string;
  slug: string;
  trait: string;
  expiry: number;
  outbidOptions: {
    outbid: boolean;
    blurOutbidMargin: number | null;
    openseaOutbidMargin: number | null;
    magicedenOutbidMargin: number | null;
    counterbid: boolean;
  };
  batchInfo?: {
    batchNumber: number;
    totalBatches: number;
    traitIndex: number;
    totalTraits: number;
    batchSize?: number;
  };
}) {
  // Parse trait early to get traitKey for logging
  let traitKey = 'unknown';
  try {
    const parsedTraitForKey = JSON.parse(data.trait);
    const [type, value] = Object.entries(parsedTraitForKey)[0];
    traitKey = `${type}:${value}`;
  } catch { /* ignore */ }

  // Create trace for scheduled bid
  // Log job started

  // Check blacklist at the VERY START to prevent any processing if task is being stopped
  if (isTaskBlacklisted(data._id, 'blur')) {
    return;
  }

  try {
    if (!enabledMarketplaces.length) {
      if (DEBUG) {
      }
      return;
    }
    const { address, walletId, contractAddress, slug, trait, _id } = data;


    // Fetch wallet and decrypt private key
    const wallet = await walletCache.getWalletById(walletId);
    const privateKey: string = decryptPrivateKey(wallet?.encryptedPrivateKey as string);

    // Skip trait validation - preprocessing already determined valid traits
    // The trait passed in the job data is already validated
    JSON.parse(trait); // Validate JSON

    const currentTask = activeTasks.get(_id)

    if (!currentTask || !currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("blur") || !enabledMarketplaces.map((m) => m.toLowerCase()).includes("blur")) {
      if (DEBUG) {
      }
      return;
    }
    const outbidOptions = currentTask.outbidOptions


    const floor_price: number = Number(await fetchBlurCollectionStats(currentTask.contract.slug));
    const traitsObj = JSON.parse(trait);
    const [traitType, traitValue] = Object.entries(traitsObj)[0];
    const identifier = `${traitType}:${traitValue}`
    const traitPriceKeyExists = currentTask?.stopOptions?.traitPrices && currentTask?.stopOptions?.triggerStopOptions && `blur:${identifier}` in currentTask?.stopOptions?.traitPrices;

    const traitFloor = traitPriceKeyExists ? await fetchBlurTraitFloor(slug, { type: traitType, value: traitValue as string }) : undefined

    const stopBid = await stopOption(currentTask, 'blur', Number(floor_price), false, traitFloor, identifier)
    const expiry = getExpiry(currentTask.bidDuration)


    const cachedOrder = await orderCache.getNewest(_id, BLUR.toLowerCase(), identifier);
    const orders = cachedOrder ? [cachedOrder] : [];


    if (stopBid) {
      if (DEBUG) {
      }

      if (orders.length > 0) {

        const cancelData = orders.map((order) => {
          const payload = JSON.parse(order.orderHash)
          return {
            name: CANCEL_BLUR_BID,
            data: { walletId, payload, taskId: _id, identifier: order.identifier }
          }
        })
        // Log cancellation enqueue
        queue.addBulk(cancelData);
      }

      return;
    }
    if (!floor_price) {
      throw new Error(`Failed to fetch floor price for ${slug}`);
    }



    if (!floorPrices[_id]) {
      floorPrices[_id] = {
        blur: floor_price,
        opensea: 0,
        magiceden: 0
      }
    }


    const { offerPriceEth, maxBidPriceEth, minBidPriceEth } = await calculateBidPrice(currentTask, floor_price, "blur")



    let traitOffer = BigInt(Math.round(Number(offerPriceEth * Math.pow(10, 18)) / 1e16) * 1e16);


    const [order, ...remainingOrders] = orders
    const orderExpiresAt = typeof order?.expiresAt === 'number' ? order.expiresAt : new Date(order?.expiresAt).getTime();
    const ttl = (orderExpiresAt - Date.now()) / 1000
    const currentPrice = Number(order ? order.offerPrice : traitOffer)

    const createdAt = typeof order?.createdAt === 'number' ? order.createdAt : new Date(order?.createdAt).getTime()
    const expiredAt = orderExpiresAt
    const orderExpiration = order ? Math.round((expiredAt - createdAt) / 1000) : expiry

    // Initialize spread info variables with defaults (used in PLACING BID log)
    let topOfferAmount = 0;
    let secondOfferAmount = 0;
    let blurOutbidMargin = 0;
    let isTopBid = false;

    if (!outbidOptions.outbid) {
      if (ttl >= MIN_BID_DURATION && currentPrice === Number(traitOffer) && Math.abs(orderExpiration - expiry) < 30) {
        return;
      }
    }
    else {

      const bids = await fetchBlurBid(slug, "TRAIT", JSON.parse(trait));
      const [topOffer, secondOffer] = bids?.priceLevels?.sort((a, b) => +b.price - +a.price) || [{ price: "0" }, { price: "0" }];

      topOfferAmount = typeof topOffer === 'object' && topOffer ? Number(topOffer.price) : 0
      secondOfferAmount = typeof secondOffer === 'object' && secondOffer ? Number(secondOffer.price) : 0
      if (!bestOffers[_id]) {
        bestOffers[_id] = {
          blur: topOfferAmount,
          opensea: 0,
          magiceden: 0
        }
      }
      const bidderAddressesSample = topOffer?.bidderAddressesSample || []
      const numberBidders = topOffer?.numberBidders || 0
      blurOutbidMargin = outbidOptions.blurOutbidMargin || 0.01

      isTopBid = numberBidders === 1 && bidderAddressesSample.map((address) => address.toLowerCase()).includes(currentTask.wallet.toLowerCase())

      // Top offer fetched from API

      if (isTopBid) {
        const spread = (Number(topOfferAmount) - Number(secondOfferAmount));
        const bidPrice = secondOfferAmount + blurOutbidMargin;
        if (+spread.toFixed(2) > blurOutbidMargin && Number(secondOfferAmount) > 0 && bidPrice >= minBidPriceEth) {
          traitOffer = BigInt(Math.round((bidPrice * 1e18) / 1e16) * 1e16);
        } else if (+spread.toFixed(2) > blurOutbidMargin && Number(secondOfferAmount) > 0 && bidPrice < minBidPriceEth) {
          // Large spread but can't lower below min - maintain current bid
          traitOffer = BigInt(Math.round(topOfferAmount * 1e18));
        } else if (ttl > MIN_BID_DURATION) {
          return;
        } else {
          // Maintain our current bid price
          traitOffer = BigInt(Math.round(topOfferAmount * 1e18));
        }
      }
      else {
        const bidPrice = topOfferAmount + blurOutbidMargin
        traitOffer = BigInt(Math.round(bidPrice * 1e18));
      }
    }

    // Round to avoid floating-point precision issues
    const offerEth = Math.round(Number(traitOffer) / 1e18 * 1000000) / 1000000;

    const bidCount = getIncrementedBidCount(BLUR, slug, _id)

    if (orders.length > 0) {
      const cancelData = orders.map((order) => {
        const payload = JSON.parse(order.orderHash)

        return {
          name: CANCEL_BLUR_BID,
          data: { walletId, payload, taskId: _id, identifier: order.identifier }
        }
      })
      // Log cancellation enqueue
      queue.addBulk(cancelData);
    }

    // CRITICAL: Always check maxBidPrice regardless of outbid setting
    if (maxBidPriceEth > 0 && offerEth >= maxBidPriceEth) {
      // Orders already cancelled above - no additional cancellation needed
      if (DEBUG) {
      }
      if (!skipStats[_id]) {
        skipStats[_id] = {
          opensea: 0,
          magiceden: 0,
          blur: 0
        }
      }
      skipStats[_id]['blur']++;
      const message = `❌ Offer price ${offerEth} BETH for ${slug} trait ${trait} bid exceeds max bid price ${maxBidPriceEth} BETH FOR BLUR BID.Skipping ...`.toUpperCase()

      await logBidError(_id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "blur");

      return;
    } else if (offerEth < minBidPriceEth) {
      traitOffer = BigInt(Math.round(minBidPriceEth * 1e18));
    }


    const finalOfferEth = Math.round(Number(traitOffer) / 1e18 * 1000000) / 1000000;

    // Log bid API start

    await bidOnBlur(_id, bidCount, address, privateKey, contractAddress, traitOffer, slug, expiry, trait, data.batchInfo);

    // Log success
    // Log job completed

  } catch (error: any) {
    const errMsg = error?.message || String(error);

    // Log bid error

    // Log job failed

    // Handle rate limit errors
    if (error?.code === 'RATE_LIMITED') {
      throw error; // Re-throw to let worker handle retry with delay
    }

    // Handle transfer restriction errors
    if (error.code === 'TRANSFER_RESTRICTED') {
      await logBidError(data._id, "BLUR TRANSFER RESTRICTION", error.message, "warning", "blur");
      return;
    }


    errorLog('Blur', 'Blur trait bid error', error instanceof Error ? error : new Error(String(error)));

    // Don't log bid errors for stopped/cancelled tasks
    if (!errMsg.includes('Bid execution blocked - task') && !errMsg.includes('is stopped')) {
      await logBidError(data._id, "BLUR TRAIT BID ERROR", `Error bidding on trait ${data.trait} in collection ${data.slug}: ${error?.response?.data?.message || error?.response?.data || errMsg}`, "error", "blur");
    }

    // Throw the error
    throw error;
  }
}

async function processMagicedenScheduledBid(task: ITask) {
  // Structured start
  const taskId = task._id.toString();
  const currentLoopCount = loopCounterStats[taskId]?.magiceden || 1;


  // Check blacklist at the VERY START to prevent any processing if task is being stopped
  if (isTaskBlacklisted(taskId, 'magiceden')) {
    return;
  }

  const checkpointStart = Date.now();
  let lockAcquired = false;

  // Timeout protection wrapper - 15 minutes max processing time (increased for event loop recovery)
  const TIMEOUT_DURATION = 900000; // 15 minutes
  const TIMEOUT_WARNING_THRESHOLD = TIMEOUT_DURATION * 0.8; // Warn at 80% (12 minutes)
  let timeoutWarningShown = false;

  const timeoutPromise = new Promise<never>((_, reject) => {
    // Set warning timeout at 80%
    setTimeout(() => {
      if (!timeoutWarningShown) {
        timeoutWarningShown = true;
        warnLog('ScheduledBid', `[ME-Sched] ${task.contract?.slug || taskId} | Processing taking longer than expected (${TIMEOUT_WARNING_THRESHOLD}ms)`, {
          taskId,
          marketplace: 'magiceden',
          slug: task.contract?.slug
        });
      }
    }, TIMEOUT_WARNING_THRESHOLD);

    // Set actual timeout
    setTimeout(() => {
      errorLog('ScheduledBid', `[ME-Sched] ${task.contract?.slug || taskId} | TIMEOUT after ${TIMEOUT_DURATION}ms`, {
        taskId,
        marketplace: 'magiceden',
        slug: task.contract?.slug
      });
      reject(new Error(`MagicEden scheduled bid timeout after ${TIMEOUT_DURATION}ms for task ${taskId}`));
    }, TIMEOUT_DURATION);
  });


  try {
    // Wrap the main processing logic in a timeout
    await Promise.race([
      (async () => {
        // Early return consolidation
        if (!enabledMarketplaces.map(m => m.toLowerCase()).includes("magiceden")) {
          if (DEBUG) {
          }
          return;
        }

        const currentTask = activeTasks.get(task._id);

        if (!currentTask?.running || !currentTask.selectedMarketplaces.map(m => m.toLowerCase()).includes("magiceden")) {
          if (DEBUG) {
          }
          return;
        }

        // Transition from SCHEDULED to STARTED for this marketplace - AFTER validation
        setTaskStatus(taskId, 'STARTED', `magiceden scheduled bid starting - Loop #${currentLoopCount}`, 'magiceden');

        // Get preprocessing data - first check the Map, then perform if needed
        let preprocessing = taskPreprocessing.get(taskId);

        if (!preprocessing || !preprocessing.magiceden.enabled) {

          // Determine available marketplaces for this task
          const validMarketplaces = ["opensea", "magiceden", "blur"];
          const availableMarketplaces = currentTask.selectedMarketplaces
            .map(m => m.toLowerCase())
            .filter(m => enabledMarketplaces.map(em => em.toLowerCase()).includes(m))
            .filter(m => validMarketplaces.includes(m));

          // Perform preprocessing for MagicEden (with priority 0 for scheduled jobs)
          preprocessing = await performMarketplacePreprocessing(currentTask, "magiceden", availableMarketplaces, 0);

          // Store in Map for other marketplaces to use
          taskPreprocessing.set(taskId, preprocessing);

          // If preprocessing didn't enable MagicEden, it means something went wrong
          if (!preprocessing.magiceden.enabled) {
            if (DEBUG) {
            }
            throw new Error(`Preprocessing failed to enable MagicEden for task ${taskId}`);
          }
        }

        if (preprocessing.magiceden.skipReason) {
          if (DEBUG) {
          }
          if (!warningBids[taskId]) {
            warningBids[taskId] = { opensea: false, magiceden: false, blur: false };
          }
          if (!warningMessage[taskId]) {
            warningMessage[taskId] = { opensea: "", magiceden: "", blur: "" };
          }
          warningBids[taskId].magiceden = true;
          warningMessage[taskId].magiceden = preprocessing.magiceden.skipReason;

          return;
        }

        // Initialize task status if it doesn't exist or check for completion state
        if (!taskStatus[taskId]) {
          const activeTask = activeTasks.get(taskId);
          taskStatus[taskId] = {
            selectedMarketplaces: activeTask?.selectedMarketplaces.map(m => m.toLowerCase()) || [],
            lastUpdate: Date.now(),
            marketplaces: {}
          };
        }

        // Acquire lock for this task
        try {
          lockAcquired = timedLockManager.acquireLock(taskId, 'magiceden');
          if (!lockAcquired) {
            if (DEBUG) {
            }
            return;
          }
        } catch (lockError) {
          errorLog('Lock', `Failed to acquire lock for task ${taskId}`, lockError);

          // If lock acquisition fails due to corruption, attempt cleanup
          if (lockError instanceof Error && lockError.message.includes('Missing lock')) {
            try {
              // Clean up any stale locks
              timedLockManager.releaseLock(taskId, 'magiceden');
              // Try once more
              lockAcquired = timedLockManager.acquireLock(taskId, 'magiceden');
              if (lockAcquired) {
              } else {
                throw new Error('Lock recovery failed');
              }
            } catch (recoveryError) {
              errorLog('Lock', `Failed to recover lock for task ${taskId}`, recoveryError);
              throw lockError; // Re-throw original error
            }
          } else {
            throw lockError;
          }
        }

        if (!currentTokenIds[taskId]) {
          currentTokenIds[taskId] = {
            opensea: [],
            magiceden: [],
          }
        }
        currentTokenIds[taskId].magiceden = preprocessing.magiceden.tokens;

        const WALLET_ADDRESS: string = currentTask.wallet;
        const wallets = await walletCache.getWalletById(currentTask.walletId);

        // Extend lock before fetching balances
        if (lockAcquired) {
          timedLockManager.extendLock(taskId, 'magiceden');
        }

        // Fetch fresh WETH balance with timeout protection
        let wethBalance = 0;
        try {
          const { fetchERC20Balance } = await import('./utils/balance');
          const { WETH } = await import('./utils/currencies');
          wethBalance = await fetchERC20Balance(WALLET_ADDRESS, WETH);
        } catch (error) {
          errorLog('MagicEden', 'Failed to fetch WETH balance', error);
        }

        const contractAddress = currentTask.contract.contractAddress
        const expiration = getExpiry(currentTask.bidDuration)
        const newTask = { ...currentTask, traits: {} } as ITask

        // Note: Status will be set to appropriate state after job creation is determined


        // Create jobs based on preprocessing results
        if (preprocessing.magiceden.tokens.length > 0) {
          // Token bid
          if (DEBUG) {
          }

          // Ensure parent job exists BEFORE processing jobs
          await ensureParentJobExists(taskId, 'magiceden');
          const parentJobId = getParentJobId(taskId, 'magiceden');
          const parentJob = parentJobs.get(parentJobId);
          if (parentJob) {
            parentJob.expectedJobs = preprocessing.magiceden.tokens.length;
            parentJob.totalJobs = preprocessing.magiceden.tokens.length; // Use expected count initially
          }

          const jobs = preprocessing.magiceden.tokens
            .map((token) => ({
              name: MAGICEDEN_TOKEN_BID,
              data: {
                _id: taskId,
                address: WALLET_ADDRESS,
                contractAddress,
                quantity: 1,
                expiration: expiration,
                walletId: task.walletId,
                slug: currentTask.contract.slug,
                tokenId: token,
                outbidOptions: currentTask.outbidOptions,
              },
              opts: {
                priority: TOKEN_BID_PRIORITY.MAGICEDEN,
                attempts: 1
              } as JobsOptions
            })) as Job[]

          try {
            // Extend lock before processing bulk jobs
            if (lockAcquired) {
              timedLockManager.extendLock(taskId, 'magiceden');
            }

            // Transition to QUEUEING state before creating jobs
            setTaskStatus(taskId, 'QUEUEING', 'queueing magiceden token bid jobs', 'magiceden');
            const meLoopNum = loopCounterStats[taskId]?.['magiceden'] || 1;

            // Queue jobs directly with queue.addBulk
            const preparedMEJobs = jobs.map(job => ({
              name: job.name,
              data: job.data,
              opts: {
                jobId: createJobKey(job),
                ...job.opts,
                removeOnComplete: true,
                removeOnFail: true
              }
            }));

            await queue.addBulk(preparedMEJobs);

            // Update parent job tracking
            if (parentJob) {
              parentJob.actualJobs = preparedMEJobs.length;
              parentJob.allJobsQueued = true;
            }

            // Transition to QUEUED after jobs are queued
            setTaskStatus(taskId, 'QUEUED', 'magiceden token bid jobs queued', 'magiceden');

            // Trigger percentage update
            await updateTaskRunningPercentage(taskId);

            // The task will transition to RUNNING when jobs start processing

          } catch (jobError) {
            errorLog('MagicEden', `Failed to queue token bid jobs: ${jobError}`);

            // Reset parent job on failure
            const parentJobId = getParentJobId(taskId, 'magiceden');
            const parentJob = parentJobs.get(parentJobId);
            if (parentJob) {
              parentJob.actualJobs = 0;
              parentJob.allJobsQueued = true; // Mark as queued even on failure
              parentJob.totalJobs = 0;
            }

            // Update task status to reflect failure - but only if marketplace is still valid
            const activeTask = activeTasks.get(taskId);
            const isMagicedenEnabled = enabledMarketplaces.map(m => m.toLowerCase()).includes('magiceden');
            const isMagicedenSelected = activeTask?.selectedMarketplaces.map(m => m.toLowerCase()).includes('magiceden');
            const isTaskRunning = activeTask?.running === true;

            if (isMagicedenEnabled && isMagicedenSelected && isTaskRunning) {
              setTaskStatus(taskId, 'PROCESSING', 'job queueing failed', 'magiceden');
            }

            // Re-throw to trigger retry
            throw jobError;
          }
        }
        else if (preprocessing.magiceden.traits.length > 0) {
          // Trait bid
          if (DEBUG) {
          }

          // Ensure parent job exists BEFORE processing jobs
          await ensureParentJobExists(taskId, 'magiceden');
          const parentJobId = getParentJobId(taskId, 'magiceden');
          const parentJob = parentJobs.get(parentJobId);
          if (parentJob) {
            parentJob.expectedJobs = preprocessing.magiceden.traits.length;
            parentJob.totalJobs = preprocessing.magiceden.traits.length; // Use expected count initially
          }

          const traitJobs = preprocessing.magiceden.traits.map((trait) => ({
            name: MAGICEDEN_TRAIT_BID,
            data: {
              _id: taskId,
              address: WALLET_ADDRESS,
              contractAddress,
              quantity: 1,
              expiration: expiration,
              walletId: task.walletId,
              slug: currentTask.contract.slug,
              trait,
              outbidOptions: currentTask.outbidOptions,
            },
            opts: { priority: TRAIT_BID_PRIORITY.MAGICEDEN, attempts: 1 } as JobsOptions
          })) as Job[]

          try {
            // Extend lock before processing bulk trait jobs
            if (lockAcquired) {
              timedLockManager.extendLock(taskId, 'magiceden');
            }

            // Transition to QUEUEING state before creating jobs
            setTaskStatus(taskId, 'QUEUEING', 'queueing magiceden trait bid jobs', 'magiceden');
            const meLoopNumTrait = loopCounterStats[taskId]?.['magiceden'] || 1;

            // Queue jobs directly with queue.addBulk
            const preparedMETraitJobs = traitJobs.map(job => ({
              name: job.name,
              data: job.data,
              opts: {
                jobId: createJobKey(job),
                ...job.opts,
                removeOnComplete: true,
                removeOnFail: true
              }
            }));

            await queue.addBulk(preparedMETraitJobs);

            // Update parent job tracking
            if (parentJob) {
              parentJob.actualJobs = preparedMETraitJobs.length;
              parentJob.allJobsQueued = true;
            }

            // Transition to QUEUED after jobs are queued
            setTaskStatus(taskId, 'QUEUED', 'magiceden trait bid jobs queued', 'magiceden');

            // Trigger percentage update
            await updateTaskRunningPercentage(taskId);

            // The task will transition to RUNNING when jobs start processing

          } catch (jobError) {
            errorLog('MagicEden', 'Failed to queue trait bid jobs', jobError);

            // Reset parent job on failure
            const parentJobId = getParentJobId(taskId, 'magiceden');
            const parentJob = parentJobs.get(parentJobId);
            if (parentJob) {
              parentJob.actualJobs = 0;
              parentJob.allJobsQueued = true; // Mark as queued even on failure
              parentJob.totalJobs = 0;
            }

            // Update task status to reflect failure - but only if marketplace is still valid
            const activeTask = activeTasks.get(taskId);
            const isMagicedenEnabled = enabledMarketplaces.map(m => m.toLowerCase()).includes('magiceden');
            const isMagicedenSelected = activeTask?.selectedMarketplaces.map(m => m.toLowerCase()).includes('magiceden');
            const isTaskRunning = activeTask?.running === true;

            if (isMagicedenEnabled && isMagicedenSelected && isTaskRunning) {
              setTaskStatus(taskId, 'PROCESSING', 'job queueing failed', 'magiceden');
            }

            // Re-throw to trigger retry
            throw jobError;
          }
        }
        else if (preprocessing.magiceden.jobCount === 1) {
          // Collection bid
          if (DEBUG) {
          }
          const collJobId = `bid-${taskId}-magiceden-${currentTask.contract.slug}-collection`

          // Ensure parent job exists BEFORE adding job
          await ensureParentJobExists(taskId, 'magiceden');
          const parentJobId = getParentJobId(taskId, 'magiceden');
          const parentJob = parentJobs.get(parentJobId);
          if (parentJob) {
            parentJob.expectedJobs = 1;
            parentJob.totalJobs = 1; // Use expected count initially
          }

          // Transition to QUEUEING state before creating job
          setTaskStatus(taskId, 'QUEUEING', 'queueing magiceden collection bid job', 'magiceden');
          const meLoopNumColl = loopCounterStats[taskId]?.['magiceden'] || 1;

          await queue.add(
            MAGICEDEN_COLLECTION_BID,
            { taskId: taskId },
            { jobId: collJobId, priority: 2, attempts: 1 }
          );

          // Update parent job count after successful job addition - mark as all queued for single job
          if (parentJob) {
            parentJob.actualJobs = 1;
            parentJob.allJobsQueued = true;
          }


          // Transition to QUEUED after job is queued (not RUNNING)
          setTaskStatus(taskId, 'QUEUED', 'magiceden collection bid job queued', 'magiceden');

          // Trigger percentage update now that the job is queued
          await updateTaskRunningPercentage(taskId);
        } else {
          // No jobs to create - create empty parent job and complete the loop

          await ensureParentJobExists(taskId, 'magiceden');
          const parentJobId = getParentJobId(taskId, 'magiceden');
          const parentJob = parentJobs.get(parentJobId);
          if (parentJob) {
            parentJob.expectedJobs = 0;
            parentJob.totalJobs = 0;
            parentJob.actualJobs = 0;
            parentJob.completedJobs = 0;
            parentJob.allJobsQueued = true;
          }

          // Update task status to trigger completion flow
          // No jobs to process - loop will complete without transitioning to RUNNING
          await updateTaskRunningPercentage(taskId);

          // Trigger marketplace-specific completion for MagicEden
          await checkTaskCompletionAndScheduleNext(taskId, 'magiceden');

          // Don't throw error - let the loop complete normally
        }
      })(), // End of main processing logic
      timeoutPromise
    ]);
  } catch (error) {
    errorLog('MagicEden', `Error processing MagicEden scheduled bid for task: ${task._id}`, error);

    // Enhanced error handling for timeout and lock issues
    const isTimeout = error instanceof Error && error.message.includes('timeout');
    const isLockError = error instanceof Error && (
      error.message.includes('Missing lock') ||
      error.message.includes('Lock acquisition failed') ||
      error.message.includes('LOCK ERROR')
    );

    if (isTimeout) {
      errorLog('MagicEdenTimeout', `Task ${taskId} timed out after ${TIMEOUT_DURATION}ms`, undefined);

      // Log timeout with additional context
      errorLog('Queue', `MagicEden scheduled bid timeout: taskId=${taskId}, timeoutDuration=${TIMEOUT_DURATION}, lockAcquired=${lockAcquired}, currentLoopCount=${currentLoopCount}, processingTime=${Date.now() - checkpointStart}`, error instanceof Error ? error : new Error(String(error)));

      // Clean up resources on timeout
      if (lockAcquired) {
        try {
          timedLockManager.releaseLock(taskId, 'magiceden');
        } catch (cleanupError) {
          errorLog('TimeoutCleanup', `Failed to release lock for task ${taskId}`, cleanupError);
        }
      }
    }

    if (isLockError) {
      errorLog('MagicEdenLockError', `Lock error for task ${taskId}`, error);

      // Log lock error for monitoring (Direct execution - no BullMQ job cleanup needed)
      errorLog('Queue', `MagicEden scheduled bid lock error: taskId=${taskId}, lockAcquired=${lockAcquired}`, error instanceof Error ? error : new Error(String(error)));
    }

    // Log to file
    const logsDir = path.join(process.cwd(), 'debug-logs');

    const errorDetails = {
      timestamp: new Date().toISOString(),
      marketplace: 'magiceden',
      taskId: task._id,
      processingTime: Date.now() - checkpointStart,
      error: error instanceof Error ? error.message : String(error),
      stackTrace: error instanceof Error ? error.stack : undefined,
      isTimeout,
      isLockError,
      lockAcquired,
      currentLoopCount,
      taskData: {
        running: task.running,
        selectedMarketplaces: task.selectedMarketplaces,
        contract: task.contract,
        selectedTraits: task.selectedTraits,
        bidPrice: task.bidPrice,
        loopInterval: task.loopInterval
      }
    };

    const logPath = path.join(logsDir, 'scheduled-job-failures.log');
    fs.appendFileSync(logPath, JSON.stringify(errorDetails, null, 2) + '\n---\n');

    // Log specific error details to help diagnose timeouts
    if (isTimeout) {
      errorLog('MagicEdenSchedule', 'Timeout details', error);
    }

    // Re-throw error to trigger job retry
    throw error;
  } finally {
    // Always release lock if acquired
    if (lockAcquired) {
      timedLockManager.releaseLock(taskId, 'magiceden');
    }
  }
}

async function processMagicedenCollectionBid(data: { taskId: string }) {
  const taskId = data.taskId;

  // Create trace for scheduled bid
  // Log job started

  // Check if task is blacklisted before processing
  if (isTaskBlacklisted(taskId)) {
    return;
  }

  try {
    if (!enabledMarketplaces.length) {
      if (DEBUG) {
      }
      return;
    }

    // Fetch task from activeTasks
    const currentTask = activeTasks.get(taskId);
    if (!currentTask || !currentTask?.running) {
      if (DEBUG) {
      }
      return;
    }

    if (!currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("magiceden")) {
      if (DEBUG) {
      }
      return;
    }

    if (!enabledMarketplaces.map((m) => m.toLowerCase()).includes("magiceden")) {
      if (DEBUG) {
      }
      return;
    }

    // Transition to RUNNING state when job starts processing
    // Job is starting to process - marketplace status will be updated when jobs are marked as processed

    const expiration = getExpiry(currentTask.bidDuration)


    // Fetch floor price. Orders from cache. (priority 0 for scheduled jobs)
    const floor_price = await fetchMagicEdenCollectionStats(currentTask.contract.contractAddress, 0).then(Number);
    const cachedOrder = await orderCache.getNewest(currentTask._id, MAGICEDEN.toLowerCase(), 'collection');
    const orders = cachedOrder ? [cachedOrder] : [];

    // Early return for floor price
    if (!floor_price) {
      errorLog('MagicEden', `Failed to fetch floor price | taskId=${taskId}`);
      throw new Error(`Failed to fetch floor price for ${currentTask.contract.slug}`);
    }


    // Fix 9: Defensive null check - floorPrices may be deleted by cleanupTaskResources during async operations
    if (!floorPrices[currentTask._id]) {
      return;
    }
    floorPrices[currentTask._id].magiceden = floor_price;

    const stopBid = await stopOption(currentTask, 'magiceden', floor_price, false)
    if (stopBid) {
      if (DEBUG) {
      }
      if (orders.length > 0) {
        const orderKeys = orders.map(order => order.orderHash)
        const extractedOrderIds = await extractMagicedenOrderHash(orders)
        // Log cancellation enqueue
        queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, walletId: currentTask.walletId, orderKeys: orderKeys, taskId: currentTask._id }); // Non-blocking
      }
      return
    }

    const { offerPriceEth, maxBidPriceEth, minBidPriceEth, currencySymbol, currency, decimal } = await calculateBidPrice(currentTask, floor_price as number, "magiceden")

    const usdcBid = currency.toLowerCase() === USDC.toLowerCase()

    let offerPrice = Math.ceil(offerPriceEth * Math.pow(10, decimal))

    let collectionOffer = (Math.round(offerPrice))


    const [order] = orders
    // CachedOrder has expiresAt/createdAt as number (timestamp in ms)
    const orderExpiresAt = typeof order?.expiresAt === 'number' ? order.expiresAt : new Date(order?.expiresAt).getTime();
    const ttl = (orderExpiresAt - Date.now()) / 1000
    const currentPrice = Number(order ? order.offerPrice : offerPrice)

    const createdAt = typeof order?.createdAt === 'number' ? order.createdAt : new Date(order?.createdAt).getTime()
    const expiredAt = orderExpiresAt
    const orderExpiration = order ? Math.round((expiredAt - createdAt) / 1000) : expiration

    // Initialize spread info variables with defaults (used in PLACING BID log)
    let highestBidAmount = 0;
    let secondTopBidAmount = 0;
    let outbidMargin = 0;
    let isOwnBid = false;

    if (!currentTask.outbidOptions.outbid) {
      if (ttl >= MIN_BID_DURATION && currentPrice === Number(offerPrice) && Math.abs(orderExpiration - expiration) < 30) {
        return;
      }
    }

    else {
      // Parallel offer fetching when outbidding (priority 0 for scheduled jobs)
      const [[topOffer, secondOffer], walletsArr] = await Promise.all([
        fetchMagicEdenOffer("COLLECTION", currentTask.contract.contractAddress, "{}", 0) as unknown as Promise<OpenseaOfferData[]>,
        walletCache.getAllWallets()
      ]);

      highestBidAmount = usdcBid ? topOffer.usd * Math.pow(10, decimal) : topOffer.eth * Math.pow(10, decimal)

      secondTopBidAmount = usdcBid ? secondOffer.usd * Math.pow(10, decimal) : secondOffer.eth * Math.pow(10, decimal)
      outbidMargin = currentTask.outbidOptions.magicedenOutbidMargin || usdcBid ? 0.01 : 0.0001

      const owner = topOffer.owner.toLowerCase();
      isOwnBid = walletsArr
        .map((addr: any) => addr.address.toLowerCase())
        .includes(owner);

      // Top offer fetched from API

      if (isOwnBid) {
        const spreadRaw = (Number(highestBidAmount) - Number(secondTopBidAmount)) / Math.pow(10, decimal);
        const spread = roundToValidPrecision(spreadRaw);
        const secondTopBidAmountEth = Number(secondTopBidAmount) / Math.pow(10, decimal);
        const potentialNewBidEth = secondTopBidAmountEth + outbidMargin;


        if (spread > outbidMargin && Number(secondTopBidAmount) > 0 && potentialNewBidEth >= minBidPriceEth) {
          collectionOffer = Number(secondTopBidAmount) + (outbidMargin * Math.pow(10, decimal))
        }
        else if (spread > outbidMargin && Number(secondTopBidAmount) > 0 && potentialNewBidEth < minBidPriceEth) {
          // Large spread but can't lower below min - maintain current bid
          collectionOffer = highestBidAmount;
        }
        else if (ttl > MIN_BID_DURATION) {
          return
        }
        else {
          collectionOffer = highestBidAmount
        }

      }
      else {
        collectionOffer = highestBidAmount + (outbidMargin * Math.pow(10, decimal))
      }
    }
    const offerEth = Number(collectionOffer) / Math.pow(10, decimal);

    if (currencySymbol.toLowerCase() === 'weth' && offerEth > floor_price) {
      skipStats[currentTask._id]['magiceden']++;
      const message = `⚠️ Offer price ${offerEth} ${currencySymbol} for ${currentTask.contract.slug} collection bid exceeds floor price ${floor_price} ${currencySymbol} FOR MAGICEDEN.`.toUpperCase();


      logBidError(currentTask._id, "OFFER EXCEEDS FLOOR PRICE", message, "warning", "magiceden"); // Non-blocking
      return;
    }

    const bidCount = getIncrementedBidCount(MAGICEDEN, currentTask.contract.slug, currentTask._id)
    const outbidOptions = currentTask.outbidOptions

    // CRITICAL: Always check maxBidPrice regardless of outbid setting
    if (maxBidPriceEth > 0 && offerEth >= maxBidPriceEth) {
      // Cancel orders and return early when max exceeded (fire-and-forget is OK since we return)
      if (orders.length > 0) {
        const orderKeys = orders.map(order => order.orderHash)
        const extractedOrderIds = await extractMagicedenOrderHash(orders)
        queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, walletId: currentTask.walletId, orderKeys: orderKeys, taskId: currentTask._id });
      }
      if (DEBUG) {
      }
      const message = `❌ Offer price ${offerEth} ${currencySymbol} for ${currentTask.contract.slug} COLLECTION bid exceeds max bid price ${maxBidPriceEth} ${currencySymbol} FOR MAGICEDEN.Skipping ...`.toUpperCase()

      logBidError(currentTask._id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "magiceden"); // Non-blocking

      if (!skipStats[currentTask._id]) {
        skipStats[currentTask._id] = {
          opensea: 0,
          magiceden: 0,
          blur: 0
        }
      }
      skipStats[currentTask._id].magiceden++;
      return;
    } else if (offerEth < minBidPriceEth) {
      collectionOffer = minBidPriceEth * Math.pow(10, decimal);
    }

    // Log bid API start

    try {
      // Fetch wallet and decrypt private key just before cancellation/bid
      const wallet = await walletCache.getWalletById(currentTask.walletId);
      const privateKey: string = decryptPrivateKey(wallet?.encryptedPrivateKey as string);

      // Cancel existing orders SYNCHRONOUSLY before placing new bid to prevent race conditions
      if (orders.length > 0) {
        await cancelMagicEdenOrdersDirectly({
          taskId: currentTask._id,
          walletId: currentTask.walletId,
          privateKey,
          orders,
          source: 'processMagicedenCollectionBid',
          identifier: 'collection',
          reason: 'placing_new_bid'
        });
      }

      const finalOfferEth = Number(collectionOffer) / Math.pow(10, decimal);
      const topBidEth = highestBidAmount / Math.pow(10, decimal);
      const secondBidEth = secondTopBidAmount / Math.pow(10, decimal);


      await bidOnMagiceden(currentTask._id, bidCount, currentTask.wallet, currentTask.contract.contractAddress, 1, collectionOffer.toString(), privateKey, currentTask.contract.slug, expiration, currency);

      // Log success
      // Log job completed

    } catch (bidError: any) {
      // Check if this is an insufficient balance error
      const handled = await handleInsufficientBalanceError(
        taskStatus,
        warningBids,
        warningMessage,
        bidError,
        currentTask._id,
        'magiceden',
        currentTask.wallet,
        currencySymbol,
        minBidPriceEth,
        'collection'
      );

      if (handled) {
        // Balance error was handled (marketplace stopped or bid skipped)
        return;
      }

      // Not a balance error, log and re-throw
      const errMsg = bidError?.message || String(bidError);
      errorLog('MagicEden', `Failed to place collection bid | taskId=${taskId}`, bidError);
      throw bidError; // Re-throw to be caught by outer catch
    }

  } catch (error: any) {
    const errMsg = error?.message || String(error);

    // Log job failed

    logBidError(taskId, "MAGICEDEN COLLECTION BID ERROR", `Error processing MagicEden collection bid for task: ${taskId}: ${error?.response?.data?.message || error?.message || String(error)}`, "error", "magiceden"); // Non-blocking
    // Throw the error
    throw error;
  }
}


async function processMagicedenTokenBid(data: IMagicedenTokenBidData) {
  const taskId = data.taskId;
  const tokenIdStr = data.tokenId?.toString() || 'unknown';

  // Create trace for scheduled bid
  // Log job started

  // Check blacklist at the VERY START to prevent any processing if task is being stopped
  if (isTaskBlacklisted(data.taskId, 'magiceden')) {
    return;
  }

  try {
    const { contractAddress, address, quantity, walletId, slug, tokenId } = data
    if (!enabledMarketplaces.length) {
      if (DEBUG) {
      }
      return;
    }

    // Early return for token validation
    if (!currentTokenIds[taskId]?.magiceden?.includes(+tokenId)) {
      if (DEBUG) {
      }
      return;
    }
    const currentTask = activeTasks.get(taskId)
    if (!currentTask || !currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("magiceden") || !enabledMarketplaces.map((m) => m.toLowerCase()).includes("magiceden")) {
      if (DEBUG) {
      }
      return;
    }

    const outbidOptions = currentTask.outbidOptions

    // Fetch orders from cache
    const cachedOrder = await orderCache.getNewest(taskId, MAGICEDEN.toLowerCase(), tokenId.toString());
    const orders = cachedOrder ? [cachedOrder] : [];

    if (!skipStats[taskId]) {
      skipStats[taskId] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      }
    }

    const { offerPriceEth, maxBidPriceEth, minBidPriceEth, currencySymbol, currency, decimal } = await calculateBidPrice(currentTask, 0, "magiceden")


    const expiration = getExpiry(currentTask.bidDuration)
    const usdcBid = currency.toLowerCase() === USDC.toLowerCase()


    let tokenOffer = Number(offerPriceEth * Math.pow(10, decimal))

    const [order, ...remainingOrders] = orders
    const hasActiveOrder = !!order
    const orderExpiresAt = typeof order?.expiresAt === 'number' ? order.expiresAt : new Date(order?.expiresAt).getTime()
    const orderCreatedAt = typeof order?.createdAt === 'number' ? order.createdAt : new Date(order?.createdAt).getTime()
    const ttl = hasActiveOrder ? (orderExpiresAt - Date.now()) / 1000 : -1
    const currentPrice = Number(order ? order.offerPrice : tokenOffer)

    const orderExpiration = hasActiveOrder
      ? Math.round((orderExpiresAt - orderCreatedAt) / 1000)
      : expiration

    // Initialize spread info variables with defaults (used in PLACING BID log)
    let highestBidAmount = 0;
    let secondTopBidAmount = 0;
    let outbidMargin = 0;
    let isOwnBid = false;

    if (!outbidOptions.outbid) {
      if (ttl >= MIN_BID_DURATION && currentPrice === tokenOffer && Math.abs(orderExpiration - expiration) < 30) {
        return;
      }

    } else {
      const walletsArr = await walletCache.getAllWallets()
      const [topOffer, secondOffer] = await fetchMagicEdenOffer("TOKEN", contractAddress, tokenId.toString()) as unknown as OpenseaOfferData[]
      // Add null checks and default values
      highestBidAmount = usdcBid ? topOffer.usd * Math.pow(10, decimal) : topOffer.eth * Math.pow(10, decimal)
      secondTopBidAmount = usdcBid ? secondOffer.usd * Math.pow(10, decimal) : secondOffer.eth * Math.pow(10, decimal)
      const owner = topOffer && typeof topOffer === 'object' ? topOffer.owner?.toLowerCase() || '' : '';
      const isTopBid = order ? +order.offerPrice > highestBidAmount : false
      isOwnBid = walletsArr
        .map(addr => addr.address.toLowerCase())
        .includes(owner);

      // Top offer fetched from API

      outbidMargin = currentTask.outbidOptions.magicedenOutbidMargin || usdcBid ? 0.01 : 0.0001

      if (isOwnBid) {
        const spreadRaw = (Number(highestBidAmount) - Number(secondTopBidAmount)) / Math.pow(10, decimal);
        const spread = roundToValidPrecision(spreadRaw);
        const secondTopBidAmountEth = Number(secondTopBidAmount) / Math.pow(10, decimal);
        const potentialNewBidEth = secondTopBidAmountEth + outbidMargin;


        if (spread > outbidMargin && Number(secondTopBidAmount) > 0 && potentialNewBidEth >= minBidPriceEth) {
          tokenOffer = Number(secondTopBidAmount) + (outbidMargin * Math.pow(10, decimal))
        }
        else if (spread > outbidMargin && Number(secondTopBidAmount) > 0 && potentialNewBidEth < minBidPriceEth) {
          // Large spread but can't lower below min - maintain current bid
          tokenOffer = highestBidAmount;
        }
        else if (ttl > MIN_BID_DURATION) {
          return;
        } else {
          tokenOffer = highestBidAmount
        }
      }

      else if (isTopBid) {
        const spreadRaw = (Number(order.offerPrice) - Number(highestBidAmount)) / Math.pow(10, decimal);
        const spread = roundToValidPrecision(spreadRaw);
        const highestBidAmountEth = Number(highestBidAmount) / Math.pow(10, decimal);
        const potentialNewBidEth = highestBidAmountEth + outbidMargin;


        if (spread > outbidMargin && Number(highestBidAmount) > 0 && potentialNewBidEth >= minBidPriceEth) {
          tokenOffer = Number(highestBidAmount) + (outbidMargin * Math.pow(10, decimal));
        }
        else if (spread > outbidMargin && Number(highestBidAmount) > 0 && potentialNewBidEth < minBidPriceEth) {
          // Large spread but can't lower below min - maintain current bid
          tokenOffer = +order.offerPrice;
        }
        else if (ttl > MIN_BID_DURATION) {
          return;
        } else {
          tokenOffer = +order.offerPrice
        }
      }

      else {
        tokenOffer = highestBidAmount + (outbidMargin * Math.pow(10, decimal))
      }
    }

    const offerEth = Number(tokenOffer) / Math.pow(10, decimal);
    const bidCount = getIncrementedBidCount(MAGICEDEN, slug, taskId)

    // CRITICAL: Always check maxBidPrice regardless of outbid setting
    if (maxBidPriceEth > 0 && offerEth >= maxBidPriceEth) {
      // Always cancel existing orders when exceeding max bid price
      // This prevents orphaned orders from accumulating when we can't compete
      if (orders.length > 0) {
        const orderKeys = orders.map((order) => order.orderHash)
        const extractedOrderIds = await extractMagicedenOrderHash(orders)
        queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, walletId, orderKeys, taskId: taskId });
      }
      const message = `❌ Offer price ${offerEth} ${currencySymbol} for ${slug} ${tokenId} bid exceeds max bid price ${maxBidPriceEth} ${currencySymbol} FOR MAGICEDEN.Skipping ...`.toUpperCase()
      logBidError(taskId, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "magiceden");

      skipStats[taskId].magiceden++;
      return;
    } else if (offerEth < minBidPriceEth) {
      tokenOffer = minBidPriceEth * Math.pow(10, decimal);
    }

    // Fetch wallet and decrypt private key just before cancellation/bid
    const wallet = await walletCache.getWalletById(walletId);
    const privateKey: string = decryptPrivateKey(wallet?.encryptedPrivateKey as string);

    // Cancel existing orders SYNCHRONOUSLY before placing new bid to prevent race conditions
    if (orders.length > 0) {
      await cancelMagicEdenOrdersDirectly({
        taskId: taskId,
        walletId,
        privateKey,
        orders,
        source: 'processMagicedenTokenBid',
        identifier: tokenId.toString(),
        reason: 'placing_new_bid'
      });
    }

    const finalOfferEth = Number(tokenOffer) / Math.pow(10, decimal);
    const topBidEth = highestBidAmount / Math.pow(10, decimal);
    const secondBidEth = secondTopBidAmount / Math.pow(10, decimal);

    try {
      // Log bid API start

      await bidOnMagiceden(taskId, bidCount, address, contractAddress, quantity, tokenOffer.toString(), privateKey, slug, expiration, currency, undefined, tokenId);

      // Log bid success
    } catch (bidError: any) {
      // Check if this is an insufficient balance error
      const handled = await handleInsufficientBalanceError(
        taskStatus,
        warningBids,
        warningMessage,
        bidError,
        taskId,
        'magiceden',
        address,
        currency,
        minBidPriceEth,
        tokenId.toString()
      );

      if (handled) {
        // Balance error was handled (marketplace stopped or bid skipped)
        return;
      }

      // Not a balance error, log and re-throw
      const errMsg = bidError?.response?.data?.message || bidError?.message || String(bidError);

      // Log bid error

      logBidError(data.taskId, "MAGICEDEN TOKEN BID ERROR", `Error bidding on token ${data.tokenId} in collection ${data.slug}: ${errMsg}`, "error", "magiceden"); // Non-blocking

      // Throw the error
      throw bidError;
    }

  } catch (error: any) {
    const errMsg = error?.response?.data?.message || error?.message || String(error);

    // Log bid error

    logBidError(data.taskId, "MAGICEDEN TOKEN BID ERROR", `Error bidding on token ${data.tokenId} in collection ${data.slug}: ${errMsg}`, "error", "magiceden"); // Non-blocking

    // Throw the error
    throw error;
  }
}

async function processMagicedenTraitBid(data: {
  _id: string;
  address: string;
  contractAddress: string;
  quantity: number;
  offerPrice?: string;
  expiration: number;
  walletId: string,
  slug: string;
  trait: {
    attributeKey: string;
    attributeValue: string;
  },
  outbidOptions: {
    outbid: boolean;
    blurOutbidMargin: number | null;
    openseaOutbidMargin: number | null;
    magicedenOutbidMargin: number | null;
    counterbid: boolean;
  }
  maxBidPriceEth?: number;
  minBidPriceEth?: number;
  task: ITask;
}) {
  const traitKey = `${data.trait?.attributeKey}:${data.trait?.attributeValue}`;

  // Create trace for scheduled bid
  // Log job started

  // Check blacklist at the VERY START to prevent any processing if task is being stopped
  if (isTaskBlacklisted(data._id, 'magiceden')) {
    return;
  }

  // Early validation
  if (!enabledMarketplaces.length || !enabledMarketplaces.map((m) => m.toLowerCase()).includes("magiceden")) {
    if (DEBUG) {
    }
    return;
  }

  const activeTask = activeTasks.get(data._id);
  if (!activeTask?.running || taskStatus[data._id]?.marketplaces?.magiceden?.status === 'COMPLETED' || taskStatus[data._id]?.marketplaces?.magiceden?.status === 'STOPPED') {
    if (DEBUG) {
    }
    return;
  }

  try {

    const
      { contractAddress, address, quantity, walletId, slug, trait, _id } = data

    // Skip trait validation - preprocessing already determined valid traits
    // The trait passed in the job data is already validated

    const currentTask = activeTasks.get(_id)
    if (!currentTask || !currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("magiceden")) {
      if (DEBUG) {
      }
      return;
    }

    const outbidOptions = currentTask.outbidOptions
    const identifier = `${trait.attributeKey}:${trait.attributeValue}`

    // Parallel floor price fetching. Orders from cache.
    const traitPriceKeyExists = currentTask?.stopOptions?.traitPrices && currentTask?.stopOptions?.triggerStopOptions && `magiceden:${identifier}` in currentTask?.stopOptions?.traitPrices;

    const [floor_price, traitFloor] = await Promise.all([
      fetchMagicEdenCollectionStats(currentTask.contract.contractAddress).then(Number),
      traitPriceKeyExists
        ? fetchMagicEdenTraitFloor(contractAddress, { attributeKey: trait.attributeKey, attributeValue: trait.attributeValue })
        : Promise.resolve(undefined)
    ]);
    const cachedOrder = await orderCache.getNewest(_id, MAGICEDEN.toLowerCase(), identifier);
    const orders = cachedOrder ? [cachedOrder] : [];

    if (!floor_price) { throw new Error(`Failed to fetch floor price for ${slug}`); }

    const stopBid = await stopOption(currentTask, 'magiceden', Number(floor_price), false, traitFloor, identifier)
    if (stopBid) {
      if (DEBUG) {
      }
      if (orders.length > 0) {
        const orderKeys = orders.map((order) => order.orderHash)
        const extractedOrderIds = await extractMagicedenOrderHash(orders)
        queue.add(CANCEL_MAGICEDEN_BID, {
          orderIds: extractedOrderIds,
          walletId,
          orderKeys,
          taskId: _id
        }); // Non-blocking
      }
      return;
    };

    const { offerPriceEth, maxBidPriceEth, minBidPriceEth, currencySymbol, currency, decimal } = await calculateBidPrice(currentTask, floor_price as number, "magiceden")

    const expiration = getExpiry(currentTask.bidDuration)
    // Fix 9: Defensive null check - floorPrices may be deleted by cleanupTaskResources during async operations
    if (!floorPrices[currentTask._id]) {
      return;
    }
    floorPrices[currentTask._id].magiceden = floor_price;

    const usdcBid = currency.toLowerCase() === USDC.toLowerCase()
    let traitOffer = Number(offerPriceEth * Math.pow(10, decimal))

    const [order, ...remainingOrders] = orders
    // CachedOrder has expiresAt/createdAt as number (timestamp in ms)
    const orderExpiresAt = typeof order?.expiresAt === 'number' ? order.expiresAt : new Date(order?.expiresAt).getTime();
    const ttl = (orderExpiresAt - Date.now()) / 1000
    const currrentPrice = Number(order ? order.offerPrice : traitOffer)

    const createdAt = typeof order?.createdAt === 'number' ? order.createdAt : new Date(order?.createdAt).getTime()
    const expiredAt = orderExpiresAt
    const orderExpiration = order ? Math.round((expiredAt - createdAt) / 1000) : expiration

    // Declare spread variables outside if/else so they're available for PLACING BID log
    let highestBidAmount = 0;
    let secondTopBidAmount = 0;
    let outbidMargin = usdcBid ? 0.01 : 0.0001;
    let isOwnBid = false;

    if (!outbidOptions.outbid) {
      if (ttl >= MIN_BID_DURATION && currrentPrice === traitOffer && Math.abs(orderExpiration - expiration) < 30) {
        return;
      }
    }

    else {
      const walletsArr = await walletCache.getAllWallets()
      const [topOffer, secondOffer] = await fetchMagicEdenOffer("TRAIT", contractAddress, trait) as unknown as OpenseaOfferData[]
      highestBidAmount = usdcBid ? topOffer.usd * Math.pow(10, decimal) : topOffer.eth * Math.pow(10, decimal)
      secondTopBidAmount = usdcBid ? secondOffer.usd * Math.pow(10, decimal) : secondOffer.eth * Math.pow(10, decimal)
      const owner = topOffer && typeof topOffer === 'object' ? topOffer.owner?.toLowerCase() : '';
      isOwnBid = walletsArr.map(addr => addr.address.toLowerCase()).includes(owner)
      const isTopBid = order ? +order.offerPrice > highestBidAmount : false
      outbidMargin = currentTask.outbidOptions.magicedenOutbidMargin || usdcBid ? 0.01 : 0.0001

      // Top offer fetched from API

      if (isOwnBid) {
        const spreadRaw = (Number(highestBidAmount) - Number(secondTopBidAmount)) / Math.pow(10, decimal);
        const spread = roundToValidPrecision(spreadRaw);
        const secondTopBidAmountEth = Number(secondTopBidAmount) / Math.pow(10, decimal);
        const potentialNewBidEth = secondTopBidAmountEth + outbidMargin;


        if (spread > outbidMargin && Number(secondTopBidAmount) > 0 && potentialNewBidEth >= minBidPriceEth) {
          traitOffer = Number(secondTopBidAmount) + (outbidMargin * Math.pow(10, decimal));
        }
        else if (spread > outbidMargin && Number(secondTopBidAmount) > 0 && potentialNewBidEth < minBidPriceEth) {
          // Large spread but can't lower below min - maintain current bid
          traitOffer = highestBidAmount;
        }
        else if (ttl > MIN_BID_DURATION) {
          return;
        } else {
          traitOffer = highestBidAmount
        }
      }
      else if (isTopBid) {
        const spreadRaw = (Number(order.offerPrice) - Number(highestBidAmount)) / Math.pow(10, decimal);
        const spread = roundToValidPrecision(spreadRaw);
        const highestBidAmountEth = Number(highestBidAmount) / Math.pow(10, decimal);
        const potentialNewBidEth = highestBidAmountEth + outbidMargin;


        if (spread > outbidMargin && Number(highestBidAmount) > 0 && potentialNewBidEth >= minBidPriceEth) {
          traitOffer = Number(highestBidAmount) + (outbidMargin * Math.pow(10, decimal));
        }
        else if (spread > outbidMargin && Number(highestBidAmount) > 0 && potentialNewBidEth < minBidPriceEth) {
          // Large spread but can't lower below min - maintain current bid
          traitOffer = +order.offerPrice;
        }
        else if (ttl > MIN_BID_DURATION) {
          return;
        } else {
          traitOffer = +order.offerPrice
        }
      }
      else {
        traitOffer = highestBidAmount + (outbidMargin * Math.pow(10, decimal))
      }
    }

    const offerEth = Number(traitOffer) / Math.pow(10, decimal);
    const bidCount = getIncrementedBidCount(MAGICEDEN, slug, _id)

    // CRITICAL: Always check maxBidPrice regardless of outbid setting
    if (maxBidPriceEth > 0 && offerEth >= maxBidPriceEth) {
      // Cancel orders and return early when max exceeded (fire-and-forget is OK since we return)
      if (orders.length > 0) {
        const orderKeys = orders.map((order) => order.orderHash)
        const extractedOrderIds = await extractMagicedenOrderHash(orders)
        queue.add(CANCEL_MAGICEDEN_BID, {
          orderIds: extractedOrderIds,
          walletId,
          orderKeys,
          taskId: _id
        });
      }
      if (DEBUG) {
      }
      logBidError(_id, "MAXIMUM BID PRICE EXCEEDED", `Offer price ${offerEth} ${currencySymbol} for ${slug} ${JSON.stringify(trait)} bid exceeds max bid price ${maxBidPriceEth} ${currencySymbol} FOR MAGICEDEN.Skipping ...`.toUpperCase(), "skipped", "magiceden");

      if (!skipStats[_id]) {
        skipStats[_id] = {
          opensea: 0,
          magiceden: 0,
          blur: 0
        }
      }
      skipStats[_id].magiceden++;
      return;
    } else if (offerEth < minBidPriceEth) {
      traitOffer = minBidPriceEth * Math.pow(10, decimal);
    }

    try {
      // Fetch wallet and decrypt private key just before cancellation/bid
      const wallet = await walletCache.getWalletById(walletId);
      const privateKey: string = decryptPrivateKey(wallet?.encryptedPrivateKey as string);

      // Cancel existing orders SYNCHRONOUSLY before placing new bid to prevent race conditions
      if (orders.length > 0) {
        await cancelMagicEdenOrdersDirectly({
          taskId: _id,
          walletId,
          privateKey,
          orders,
          source: 'processMagicedenTraitBid',
          identifier,
          reason: 'placing_new_bid'
        });
      }

      // Log bid API start

      const finalOfferEth = Number(traitOffer) / Math.pow(10, decimal);
      const topBidEth = highestBidAmount / Math.pow(10, decimal);
      const secondBidEth = secondTopBidAmount / Math.pow(10, decimal);
      if (DEBUG) {
      }

      await bidOnMagiceden(_id, bidCount, address, contractAddress, quantity, traitOffer.toString(), privateKey, slug, expiration, currency, trait)

      // Log bid success

    } catch (bidError: any) {
      // Check if this is an insufficient balance error
      const handled = await handleInsufficientBalanceError(
        taskStatus,
        warningBids,
        warningMessage,
        bidError,
        _id,
        'magiceden',
        address,
        currency,
        minBidPriceEth,
        identifier
      );

      if (handled) {
        // Balance error was handled (marketplace stopped or bid skipped)
        return;
      }

      // Not a balance error, log and re-throw
      errorLog('MagicEdenTraitBid', `MagicEden trait bid failed for task ${_id}`, bidError);
      throw bidError; // Re-throw to be caught by outer catch
    }
  } catch (error: any) {
    const errMsg = error?.response?.data?.message || error?.response?.data || error?.message || String(error);

    // Log bid error

    // Don't log bid errors for stopped/cancelled tasks
    if (!errMsg.includes('Bid execution blocked - task') && !errMsg.includes('is stopped')) {
      // Record error stats
      if (!errorStats[data._id]) {
        errorStats[data._id] = { opensea: 0, magiceden: 0, blur: 0 };
      }
      // Error tracking is now handled in queue event handlers

      logBidError(data._id, "MAGICEDEN TRAIT BID ERROR", `Error bidding on trait ${JSON.stringify(data.trait)} in collection ${data.slug}: ${errMsg}`, "error", "magiceden"); // Non-blocking

      // Throw the error
      throw error;
    }
  }
}

async function bulkCancelOpenseaBid(data: { walletId: string, orderId: string, taskId: string, identifier?: string }) {
  const { walletId, orderId, taskId, identifier } = data
  const safeWalletId = maskSensitiveIdentifier(walletId);

  // Debug log: Check if walletId is a valid MongoDB ObjectId
  const isValidObjectId = /^[a-f\d]{24}$/i.test(walletId);

  try {
    // Fetch wallet and decrypt private key
    const wallet = await walletCache.getWalletById(walletId);

    if (!wallet) {
      // Graceful handling: Log warning (not error), clean up order from cache, and return without throwing
      warnLog('BulkCancelOpensea', `Wallet ${safeWalletId} not found for task ${taskId} - cleaning up order from cache`);
      // Remove order from cache (source of truth)
      if (identifier) {
        await safeDeleteFromCache(taskId, 'opensea', identifier, orderId);
      }
      // Record completion since we've cleaned up our side
      cancellationTracker.recordCompletion(taskId, 1, 'opensea');
      await updateTaskCancellationPercentage(taskId, undefined, 'opensea');
      // Log to cancellation debug file
      cancellationDebugLogger.logJobResult(orderId, 'skipped', 'wallet_not_found');
      return; // Don't throw - gracefully skip this order
    }


    if (!wallet.encryptedPrivateKey) {
      // Graceful handling: Log warning, clean up order from cache, and return without throwing
      warnLog('BulkCancelOpensea', `No encryptedPrivateKey found for wallet ${safeWalletId} - cleaning up order from cache`);
      // Remove order from cache (source of truth)
      if (identifier) {
        await safeDeleteFromCache(taskId, 'opensea', identifier, orderId);
      }
      cancellationTracker.recordCompletion(taskId, 1, 'opensea');
      await updateTaskCancellationPercentage(taskId, undefined, 'opensea');
      // Log to cancellation debug file
      cancellationDebugLogger.logJobResult(orderId, 'skipped', 'no_private_key');
      return; // Don't throw - gracefully skip this order
    }

    const privateKey: string = decryptPrivateKey(wallet.encryptedPrivateKey);


    // First, cancel the order with OpenSea API (but don't update tracking yet)
    await cancelOrderWithoutTracking(orderId, OPENSEA_PROTOCOL_ADDRESS, privateKey, taskId);

    // Track successful cancellation for verification
    if (!successfullyCancelledOrders.has(taskId)) {
      successfullyCancelledOrders.set(taskId, new Set());
    }
    successfullyCancelledOrders.get(taskId)!.add(orderId);

    // Remove order from cache ONLY after confirmed API success
    if (identifier) {
      await safeDeleteFromCache(taskId, 'opensea', identifier, orderId);
    }

    // Record completion and update percentage - now that cache is clean
    cancellationTracker.recordCompletion(taskId, 1, 'opensea');
    await updateTaskCancellationPercentage(taskId, undefined, 'opensea');

    // Decrement ownBidCounts for the cancelled order

    // Decrement bidStats for UI display
    decrementBidCount('opensea', taskId);

    // Log success to cancellation debug file
    cancellationDebugLogger.logJobResult(orderId, 'success');

    // Add marketplace file logging for successful cancellation
  } catch (error) {
    // Log detailed error information
    const errorMessage = error instanceof Error ? error.message : String(error);
    errorLog('BulkCancelOpensea', `Failed to cancel OpenSea order ${orderId} for task ${taskId}`, error);

    // Log error to cancellation debug file
    cancellationDebugLogger.logJobResult(orderId, 'error', errorMessage);

    // Log additional error details if available
    if (error instanceof Error && 'response' in error) {
      const responseError = error as any;
      if (responseError.response?.data) {
        errorLog('BulkCancelOpensea', `Response data for failed cancellation`, undefined);
      }
    }

    throw error; // Re-throw to maintain existing behavior
  }
}

async function bulkCancelMagicedenBid(data: { orderIds: string[], walletId: string, orderKeys: string[], taskId: string, batchSize?: number, identifier?: string }) {
  const { orderIds, walletId, orderKeys, taskId, batchSize, identifier = 'collection' } = data
  const safeWalletId = maskSensitiveIdentifier(walletId);


  // Add marketplace file logging

  try {
    if (orderIds.length > 0) {
      // Fetch wallet and decrypt private key
      const wallet = await walletCache.getWalletById(walletId);

      if (!wallet) {
        // Graceful handling: Log warning, clean up orders from Redis cache, and return without throwing
        warnLog('BulkCancelMagiceden', `Wallet ${safeWalletId} not found for task ${taskId} - cleaning up orders from cache`);
        // Remove orders from Redis cache (source of truth)
        for (const orderKey of orderKeys) {
          await safeDeleteFromCache(taskId, 'magiceden', identifier, orderKey);
        }
        const completedCount = batchSize || orderIds.length;
        cancellationTracker.recordCompletion(taskId, completedCount, 'magiceden');
        await updateTaskCancellationPercentage(taskId, undefined, 'magiceden');
        orderKeys.forEach(orderKey => magicedenOrdersBeingCancelled.delete(orderKey));
        return; // Don't throw - gracefully skip these orders
      }


      if (!wallet.encryptedPrivateKey) {
        // Graceful handling: Log warning, clean up orders from Redis cache, and return without throwing
        warnLog('BulkCancelMagiceden', `No encryptedPrivateKey found for wallet ${safeWalletId} - cleaning up orders from cache`);
        // Remove orders from Redis cache (source of truth)
        for (const orderKey of orderKeys) {
          await safeDeleteFromCache(taskId, 'magiceden', identifier, orderKey);
        }
        const completedCount = batchSize || orderIds.length;
        cancellationTracker.recordCompletion(taskId, completedCount, 'magiceden');
        await updateTaskCancellationPercentage(taskId, undefined, 'magiceden');
        orderKeys.forEach(orderKey => magicedenOrdersBeingCancelled.delete(orderKey));
        return; // Don't throw - gracefully skip these orders
      }

      const privateKey: string = decryptPrivateKey(wallet.encryptedPrivateKey);

      // Cancel the orders with MagicEden API
      await cancelMagicEdenBid(orderIds, privateKey, taskId);

      // Track successful cancellation for verification
      if (!successfullyCancelledOrders.has(taskId)) {
        successfullyCancelledOrders.set(taskId, new Set());
      }
      orderIds.forEach(orderId => successfullyCancelledOrders.get(taskId)!.add(orderId));

      // Remove orders from cache ONLY after confirmed API success
      for (const orderKey of orderKeys) {
        await safeDeleteFromCache(taskId, 'magiceden', identifier, orderKey);
      }


      // Record completion for the batch
      const completedCount = batchSize || orderIds.length;
      cancellationTracker.recordCompletion(taskId, completedCount, 'magiceden');
      await updateTaskCancellationPercentage(taskId, undefined, 'magiceden');

      // Decrement ownBidCounts for the cancelled orders

      // Decrement bidStats for UI display (once per order in batch)
      for (let i = 0; i < completedCount; i++) {
        decrementBidCount('magiceden', taskId);
      }

      // Add marketplace file logging for successful cancellation
    } else {
    }

    // Clean up the tracking set after successful cancellation
    orderKeys.forEach(orderKey => {
      magicedenOrdersBeingCancelled.delete(orderKey);
    });

    // Cancellation tracking is now handled in cancelMagicEdenBid directly
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    errorLog('BulkCancelMagiceden', `Error processing Magiceden bid cancellation for task ${taskId}`, error);

    // Add marketplace file logging for error

    // Clean up the tracking set even on error to allow retry
    orderKeys.forEach(orderKey => {
      magicedenOrdersBeingCancelled.delete(orderKey);
    });

    throw error; // Re-throw to maintain existing behavior
  }
}

async function validateBidCount() {
  // Early exit if no tasks
  if (currentTasks.length === 0) {
    return;
  }

  // Retry configuration for database resilience (Fix #4)
  const MAX_RETRIES = 3;
  const RETRY_DELAYS = [1000, 2000, 4000]; // exponential backoff: 1s, 2s, 4s

  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    try {
      await validateBidCountInternal();
      return; // Success - exit the function
    } catch (error) {
      if (attempt < MAX_RETRIES - 1) {
        // Log retry attempt and wait before retrying
        warnLog('ValidateBidCount', `Database operation failed, retrying (attempt ${attempt + 1}/${MAX_RETRIES})`);
        await new Promise(resolve => setTimeout(resolve, RETRY_DELAYS[attempt]));
        continue;
      }
      // Final attempt failed - log error and set defaults
      for (const task of currentTasks) {
        bidStats[task._id] = { opensea: 0, magiceden: 0, blur: 0 };
      }
      errorLog('Database', 'Failed to validate bid counts after retries', error instanceof Error ? error : new Error(String(error)));
      return;
    }
  }
}

async function validateBidCountInternal() {
  // Note: Redis TTL handles expiration automatically - no manual cleanup needed
  // Orders are stored with TTL based on their expiresAt timestamp

  // Process order counts from cache
  const bidStatsMap: { [taskId: string]: { opensea: number; magiceden: number; blur: number } } = {};

  // Initialize all tasks with zero counts and populate from cache
  for (const task of currentTasks) {
    // Fix 8: Skip tasks being cancelled - bidStats managed by decrementBidCount during cancellation
    // When a task is blacklisted (stopTask called), cleanupTaskResources clears orderCache
    // which would cause bidStats to show 0 immediately instead of decrementing gradually
    const taskIdStr = task._id.toString();
    if (isTaskBlacklisted(taskIdStr)) {
      // Keep existing bidStats for this task - cancellation will decrement via decrementBidCount
      continue;
    }
    const counts = await orderCache.countGroupedByMarketplace(task._id);
    bidStatsMap[task._id] = {
      opensea: counts['opensea'] || 0,
      magiceden: counts['magiceden'] || 0,
      blur: counts['blur'] || 0
    };
  }

  // Update global bidStats
  Object.assign(bidStats, bidStatsMap);

  // Note: Expired order cleanup is now handled automatically by Redis TTL
  // Orders in Redis expire based on their expiresAt timestamp, so no manual cleanup needed
}

// NOTE: validateBidCountInterval is initialized in startServer() after MongoDB connection
// validateBidCountInterval = setInterval(validateBidCount, STANDARD_INTERVAL);

async function bulkCancelBlurBid(data: BlurCancelPayload) {
  const { orderKey, walletId, taskId, payload, identifier } = data
  const safeWalletId = maskSensitiveIdentifier(walletId);


  // Add marketplace file logging

  try {
    if (!data) return

    // Fetch wallet and decrypt private key
    const wallet = await walletCache.getWalletById(walletId);

    if (!wallet) {
      errorLog('BulkCancelBlur', `Wallet ${safeWalletId} not found for task ${taskId}`, undefined);
      throw new Error(`Wallet ${safeWalletId} not found`);
    }

    if (!wallet.encryptedPrivateKey) {
      errorLog('BulkCancelBlur', `No encryptedPrivateKey found for wallet ${safeWalletId}`, undefined);
      throw new Error(`No encryptedPrivateKey found for wallet ${safeWalletId}`);
    }

    const privateKey: string = decryptPrivateKey(wallet.encryptedPrivateKey);

    // Cancel the order with Blur API
    await cancelBlurBid({ payload, privateKey, taskId });

    // Track successful cancellation for verification
    const orderId = JSON.stringify(payload); // Blur uses payload as order identifier
    if (!successfullyCancelledOrders.has(taskId)) {
      successfullyCancelledOrders.set(taskId, new Set());
    }
    successfullyCancelledOrders.get(taskId)!.add(orderId);

    // Remove order from cache ONLY after confirmed API success
    await safeDeleteFromCache(taskId, 'blur', identifier, orderKey);


    // Record completion
    cancellationTracker.recordCompletion(taskId, 1, 'blur');
    await updateTaskCancellationPercentage(taskId, undefined, 'blur');

    // Decrement bidStats for UI display
    decrementBidCount('blur', taskId);

    // Add marketplace file logging for successful cancellation

    // Cancellation tracking is now handled in cancelBlurBid directly
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    errorLog('BulkCancelBlur', `Error processing Blur bid cancellation for task ${taskId}`, error);

    // Add marketplace file logging for error

    throw error; // Re-throw to maintain existing behavior
  }
}

app.get("/", (req, res) => {
  res.json({ message: "Welcome to the NFTTools bidding bot server! Let's make magic happen! 🚀🚀🚀" });
});




app.get("/health", async (req, res) => {
  try {
    // Silently renew password TTL on health checks (client calls this frequently)
    if (getPassword()) {
      renewPasswordTTL().catch(() => { }); // Fire and forget, don't block health check
    }

    // Use Promise.race to timeout slow operations after 5 seconds
    const timeoutPromise = new Promise((_, reject) => {
      setTimeout(() => reject(new Error('Health check timeout')), 5000);
    });

    let queueCounts = { active: 0, completed: 0, delayed: 0, failed: 0, paused: 0, prioritized: 0, waiting: 0, 'waiting-children': 0 };
    let blurRateLimits = { totalWallets: 0, rateLimitedWallets: 0, wallets: [] };

    try {
      // Get queue counts with timeout
      queueCounts = await Promise.race([
        queue.getJobCounts(),
        timeoutPromise
      ]) as any;
    } catch (error) {
      warnLog('Health', 'Failed to get queue counts in health check');
    }

    try {
      // Get blur rate limits with timeout
      blurRateLimits = await Promise.race([
        Promise.resolve(blurRateLimiter.getStatus()),
        timeoutPromise
      ]) as any;
    } catch (error) {
      warnLog('Health', 'Failed to get blur rate limits in health check');
    }

    // Check BullMQ connection health
    const bullmqHealthy = await checkRedisHealth();

    const metrics = {
      status: "ok",
      timestamp: new Date().toISOString(),
      uptime: process.uptime(),
      passwordSet: !!getPassword(),
      components: {
        bullmq: bullmqHealthy ? "ok" : "error",
        mongodb: mongoose.connection.readyState === 1 ? "ok" : "error",
      },
      metrics: {
        memory: {
          rss: `${Math.round(process.memoryUsage().rss / 1024 / 1024)}MB`,
          heapTotal: `${Math.round(process.memoryUsage().heapTotal / 1024 / 1024)}MB`,
          heapUsed: `${Math.round(process.memoryUsage().heapUsed / 1024 / 1024)}MB`,
          external: `${Math.round(process.memoryUsage().external / 1024 / 1024)}MB`
        },
        queue: queueCounts,
        blurRateLimits: blurRateLimits
      }
    };
    res.json(metrics);
  } catch (error: any) {
    errorLog('HealthCheck', 'Health check error', error);
    res.status(500).json({
      status: "error",
      timestamp: new Date().toISOString(),
      error: error.message
    });
  }
});

// Detailed metrics endpoint for monitoring
app.get("/metrics", async (req, res) => {
  try {
    // Get queue metrics (single queue architecture)
    const [mainQueueCounts] = await Promise.all([
      queue.getJobCounts()
    ]);

    // Get worker health status
    const workerStatuses: Array<{
      index: number;
      name: string;
      running: boolean;
      paused: boolean;
      concurrency: number;
      healthy: boolean;
      error?: string;
    }> = [];

    // Check all workers (single worker array)
    workers.forEach((worker, index) => {
      try {
        workerStatuses.push({
          index,
          name: `worker-${index}`,
          running: worker.isRunning(),
          paused: worker.isPaused(),
          concurrency: worker.concurrency,
          healthy: worker.isRunning() && !worker.isPaused()
        });
      } catch (error) {
        workerStatuses.push({
          index,
          name: `worker-${index}`,
          running: false,
          paused: false,
          concurrency: 0,
          healthy: false,
          error: error instanceof Error ? error.message : 'Unknown error'
        });
      }
    });


    // Get Redis connection pool stats
    const connectionPoolStats = getBullMQConnections();
    const connectionHealth = await checkBullMQConnectionsHealth();

    // Calculate processing rate: total completed jobs / server uptime
    const totalCompleted = (mainQueueCounts.completed || 0);
    const uptimeSeconds = process.uptime();
    const processingRate = uptimeSeconds > 0 ? totalCompleted / uptimeSeconds : 0;

    // Get active tasks and their status
    const taskStatuses = Array.from(activeTasks.entries()).map(([taskId, task]) => ({
      taskId,
      slug: task.contract?.slug || 'Unknown',
      running: task.running,
      marketplaceStatuses: taskStatus[taskId]?.marketplaces || {},
      bidCount: (bidStats[taskId]?.opensea || 0) + (bidStats[taskId]?.magiceden || 0) + (bidStats[taskId]?.blur || 0),
      errorCount: (errorStats[taskId]?.opensea || 0) + (errorStats[taskId]?.magiceden || 0) + (errorStats[taskId]?.blur || 0)
    }));

    // Calculate bid rates
    const totalBidsPerSecond = bidRates.opensea.bidsPerSecond + bidRates.blur.bidsPerSecond + bidRates.magiceden.bidsPerSecond;

    const metrics = {
      timestamp: new Date().toISOString(),
      uptime: process.uptime(),
      system: {
        memory: {
          rss: process.memoryUsage().rss,
          heapTotal: process.memoryUsage().heapTotal,
          heapUsed: process.memoryUsage().heapUsed,
          external: process.memoryUsage().external,
          heapUsedPercentage: (process.memoryUsage().heapUsed / process.memoryUsage().heapTotal) * 100
        },
        cpu: process.cpuUsage()
      },
      queues: {
        main: {
          ...mainQueueCounts,
          name: QUEUE_NAME,
          concurrency: workers.reduce((sum, w) => sum + w.concurrency, 0),
          workers: workers.length,
          description: 'All bids (scheduled + counterbids for OpenSea, MagicEden, Blur)'
        },
        totals: {
          waiting: (mainQueueCounts.waiting || 0),
          active: (mainQueueCounts.active || 0),
          completed: (mainQueueCounts.completed || 0),
          failed: (mainQueueCounts.failed || 0)
        },
        thresholds: {
          currentDepth: (mainQueueCounts.waiting || 0),
          maxThreshold: MAX_QUEUE_JOBS,
          drainThreshold: DRAIN_THRESHOLD,
          utilizationPct: parseFloat(((mainQueueCounts.waiting || 0) / MAX_QUEUE_JOBS * 100).toFixed(2))
        },
        dynamicThrottling: {
          currentDepth: (mainQueueCounts.waiting || 0),
          bufferMultiplier:
            ((mainQueueCounts.waiting || 0)) > 50000 ? 0.5 :
              ((mainQueueCounts.waiting || 0)) > 10000 ? 1 :
                ((mainQueueCounts.waiting || 0)) > 1000 ? 2 : 3,
          queueingRatePerSec:
            ((mainQueueCounts.waiting || 0)) > 50000 ? 32 :
              ((mainQueueCounts.waiting || 0)) > 10000 ? 64 :
                ((mainQueueCounts.waiting || 0)) > 1000 ? 128 : 192,
          status:
            ((mainQueueCounts.waiting || 0)) > 50000 ? 'CRITICAL' :
              ((mainQueueCounts.waiting || 0)) > 10000 ? 'ELEVATED' :
                ((mainQueueCounts.waiting || 0)) > 1000 ? 'MEDIUM' : 'NORMAL'
        },
        processingRate: processingRate.toFixed(2) + ' jobs/sec'
      },
      workers: {
        total: workers.length,
        healthy: workerStatuses.filter(w => w.healthy).length,
        unhealthy: workerStatuses.filter(w => !w.healthy).length,
        totalConcurrency: workers.reduce((sum, w) => sum + w.concurrency, 0),
        details: workerStatuses
      },
      redis: {
        connections: {
          total: connectionPoolStats.length,
          healthy: Object.values(connectionHealth).filter(c => c.healthy).length,
          unhealthy: Object.values(connectionHealth).filter(c => !c.healthy).length,
          oldest: Math.max(...connectionPoolStats.map(c => c.age)) / 1000 + ' seconds',
          details: connectionPoolStats.map(c => ({
            id: c.id,
            age: Math.floor(c.age / 1000) + ' seconds',
            healthy: connectionHealth[c.id]?.healthy || false
          }))
        }
      },
      tasks: {
        total: activeTasks.size,
        running: taskStatuses.filter(t => t.running).length,
        active: taskStatuses.filter(t => {
          // Check if any marketplace is in RUNNING or PROCESSING state
          const marketplaces = t.marketplaceStatuses;
          return Object.values(marketplaces).some(mp =>
            mp?.status === 'RUNNING' || mp?.status === 'PROCESSING'
          );
        }).length,
        details: taskStatuses
      },
      bidding: {
        ratePerSecond: totalBidsPerSecond.toFixed(2),
        byMarketplace: {
          opensea: bidRates.opensea.bidsPerSecond.toFixed(2),
          blur: bidRates.blur.bidsPerSecond.toFixed(2),
          magiceden: bidRates.magiceden.bidsPerSecond.toFixed(2)
        },
        totalBids: {
          opensea: bidRates.opensea.totalBids,
          blur: bidRates.blur.totalBids,
          magiceden: bidRates.magiceden.totalBids
        }
      },
      rateLimits: {
        current: rps.currentRPS,
        max: rps.maxRPS,
        utilization: rps.maxRPS > 0 ? (rps.currentRPS / rps.maxRPS * 100).toFixed(2) + '%' : '0%',
        blur: blurRateLimiter.getStatus()
      },
      memoryCleanup: memoryCleanupService.getStats(),
      lruCaches: {
        processedMessages: processedMessages.size // LRUCache with 1min TTL, max 1M
      },
      unboundedDataStructures: {
        traitsToRemove: Object.keys(traitsToRemove).length,
        removedTokens: Object.keys(removedTokens).length,
        tasksBeingRestarted: tasksBeingRestarted.size,
        tasksBeingCancelled: tasksBeingCancelled.size,
        tasksInCompletionProcess: tasksInCompletionProcess.size, // Monitor for hanging checks
        failedStartQueue: failedTaskStartQueue.size,
        failedStopQueue: failedTaskStopQueue.size,
        queueingProgress: queueingProgress.size,
        taskLocks: taskLocks.size,
        loopSchedulingLocks: loopSchedulingLocks.size
      }
    };

    res.json(metrics);
  } catch (error: any) {
    errorLog('MetricsEndpoint', 'Metrics endpoint error', error);
    res.status(500).json({
      status: "error",
      timestamp: new Date().toISOString(),
      error: error.message
    });
  }
});


app.post("/api/traits/count", async (req, res) => {
  try {
    const { selectedTraits: incomingTraits, slug }: { selectedTraits: SelectedTraits; slug: string } = req.body
    const filteredTasks = Object.fromEntries(Object.entries(incomingTraits?.opensea || {}).filter(([_, traits]) => traits.length > 0))
    const selectedTraits = transformNewTask(filteredTasks)

    const attributes = transformSelectedTraitsToAttributes(selectedTraits)
    const variable: CollectionItemsQueryVariables = {
      collectionSlug: slug,
      filter: {
        attributes,
      },
      limit: 100,
      sort: {
        by: 'PRICE',
        direction: 'ASC',
      },
    };
    const items = await fetchOpenSeaCollectionItems(variable);
    res.status(200).json(items.length)
  } catch (error: any) {
    const err = error?.response?.data || error?.message
    errorLog('API', `Failed to count traits | endpoint=/api/traits/count | slug=${req.body?.slug} | status=${error?.response?.status || 500}`, error instanceof Error ? error : new Error(String(error)))
    res.status(500).json(err)
  }
})

app.post("/api/rarity", async (req, res) => {
  try {
    const { collectionSlug, contractAddress, rarityCategory } = req.body;

    if (!collectionSlug || !rarityCategory) {
      return res.status(400).json({
        error: "Missing required fields: collectionSlug, rarityCategory"
      });
    }

    if (!["LEGENDARY", "EPIC", "RARE"].includes(rarityCategory)) {
      return res.status(400).json({
        error: "Invalid rarity category. Must be LEGENDARY, EPIC, or RARE"
      });
    }

    const result = await fetchRarityTokens(
      collectionSlug,
      rarityCategory as "LEGENDARY" | "EPIC" | "RARE"
    );

    res.status(200).json(result);
  } catch (error: any) {
    const err = error?.response?.data || error?.message;
    errorLog('API', `Failed to fetch rarity tokens | endpoint=/api/rarity | slug=${req.body?.collectionSlug} | rarity=${req.body?.rarityCategory} | status=${error?.response?.status || 500}`, error instanceof Error ? error : new Error(String(error)));
    res.status(500).json({
      error: "Failed to fetch rarity tokens",
      details: err
    });
  }
});

app.get("/api/task/:taskId", async (req, res) => {
  try {
    const { taskId } = req.params;
    const { page = "1", pageSize = "20", marketplace = "" } = req.query;

    const ABSOLUTE_MAX_RECORDS = 10000;

    // Get marketplace filter if provided
    let marketplaceFilter: string | string[] | undefined;
    if (marketplace) {
      // Handle marketplace as an array or single value
      if (Array.isArray(marketplace)) {
        marketplaceFilter = marketplace.map((m: any) => m.toLowerCase());
      } else if (typeof marketplace === 'string' && marketplace.includes(',')) {
        // Handle comma-separated string
        marketplaceFilter = marketplace.split(',').map(m => m.trim().toLowerCase());
      } else {
        marketplaceFilter = (marketplace as string).toLowerCase();
      }
    }

    // Check if "all" was requested
    const isAll = pageSize === "all";
    let limitedPageSize: number;
    let skip: number;

    if (isAll) {
      // Validate that total count doesn't exceed safety limit
      const totalCount = await orderCache.countByTask(taskId, {
        marketplace: marketplaceFilter
      });

      if (totalCount > ABSOLUTE_MAX_RECORDS) {
        return res.status(400).json({
          status: "error",
          message: `Cannot display all records. Dataset has ${totalCount} records, which exceeds the maximum limit of ${ABSOLUTE_MAX_RECORDS} records. Please use pagination instead.`,
          totalCount,
          maxRecords: ABSOLUTE_MAX_RECORDS
        });
      }

      // For "all", we fetch all records
      limitedPageSize = totalCount;
      skip = 0;
    } else {
      // Convert to numbers and validate
      const pageNum = parseInt(page as string, 10) || 1;
      const pageSizeNum = parseInt(pageSize as string, 10) || 20;

      // Limit page size to reasonable values
      limitedPageSize = Math.min(Math.max(pageSizeNum, 1), 100);

      // Calculate skip value for pagination
      skip = (pageNum - 1) * limitedPageSize;
    }

    // Get paginated orders from Redis cache (source of truth)
    const { orders, total: totalCount } = await orderCache.getByTaskPaginated(taskId, {
      marketplace: marketplaceFilter,
      skip,
      limit: isAll ? undefined : limitedPageSize
    });

    // Calculate pagination metadata
    const pageNum = isAll ? 1 : (parseInt(page as string, 10) || 1);
    const totalPages = isAll ? 1 : Math.ceil(totalCount / limitedPageSize);

    res.status(200).json({
      data: orders,
      pagination: {
        total: totalCount,
        page: pageNum,
        pageSize: isAll ? totalCount : limitedPageSize,
        totalPages,
        hasNextPage: isAll ? false : pageNum < totalPages,
        hasPrevPage: isAll ? false : pageNum > 1,
        isAll
      }
    });
  } catch (error: any) {
    errorLog('API', 'Error fetching task', error);
    res.status(500).json({
      status: "error",
      message: "Failed to fetch task details",
      error: error.message
    });
  }
});

// Get task status history
app.get("/api/task/:taskId/status-history", async (req, res) => {
  try {
    const { taskId } = req.params;

    // Get current status
    const currentStatus = taskStatus[taskId];

    // Get status history
    const history = taskStatusHistory[taskId] || [];

    // Get task state manager status if available
    let managerState = null;
    if (taskStateManager) {
      managerState = taskStateManager.getTaskState(taskId);
    }

    res.status(200).json({
      taskId,
      currentStatus: currentStatus || null,
      managerState: managerState || null,
      history: history.slice(-50), // Return last 50 status changes
      historyCount: history.length,
      timestamp: new Date().toISOString()
    });
  } catch (error: any) {
    errorLog('API', 'Error fetching task status history', error);
    res.status(500).json({ error: 'Failed to fetch task status history' });
  }
});

app.post('/api/task/cancel/:taskId', async (req, res) => {
  const startTime = Date.now();
  try {
    const { taskId } = req.params;

    // Task cancellation initiated - removed info log
    const task = activeTasks.get(taskId)
    if (!task) {
      return res.status(404).json({ error: 'Task not found' });
    }
    const wallet = await walletCache.getWalletById(task.walletId)
    const orders: any[] = req.body
    const openseaOrders = orders.filter((order: any) => order.marketplace === OPENSEA.toLowerCase())
    const magicedenOrders = orders.filter((order: any) => order.marketplace === 'magiceden')
    const blurOrders = orders.filter((order: any) => order.marketplace === 'blur')

    if (openseaOrders.length > 0) {
      const cancelData = openseaOrders.map((order) => {
        return {
          name: CANCEL_OPENSEA_BID,
          data: { walletId: task.walletId, orderId: order.orderHash, taskId: task._id, identifier: order.identifier }
        }
      })
      await queue.addBulk(cancelData);
    }

    if (magicedenOrders.length > 0) {
      const extractedOrderIds = await extractMagicedenOrderHash(magicedenOrders)
      const orderKeys = magicedenOrders.map((order) => order.orderHash)
      queue.add(CANCEL_MAGICEDEN_BID, {
        orderIds: extractedOrderIds,
        walletId: task.walletId,
        orderKeys,
        taskId: task._id
      });
    }

    if (blurOrders.length > 0) {
      const blurCancelData = blurOrders.map((order) => {
        return {
          name: CANCEL_BLUR_BID,
          data: { walletId: task.walletId, orderId: order.orderHash, taskId: task._id, identifier: order.identifier }
        }
      })
      await queue.addBulk(blurCancelData)
    }

    res.status(200).json({ message: 'Task cancelled' });

    // Task cancelled successfully - removed info log
  } catch (error: any) {
    errorLog('API', `Failed to cancel task | endpoint=/api/task/cancel/:taskId | taskId=${req.params.taskId} | responseTime=${Date.now() - startTime}ms`, error instanceof Error ? error : new Error(String(error)))
    res.status(500).json({ error: 'Failed to update task' });
  }
})

// Reset purchase count for a task
app.post('/api/task/:taskId/reset-purchases', async (req, res) => {
  try {
    const { taskId } = req.params;

    const result = await Task.findByIdAndUpdate(
      taskId,
      { $set: { 'stopOptions.actualPurchase': 0 } },
      { new: true }
    );

    if (!result) {
      return res.status(404).json({ error: 'Task not found' });
    }

    res.status(200).json({ message: 'Purchase count reset', actualPurchase: 0 });
  } catch (error: any) {
    errorLog('API', `Failed to reset purchases | taskId=${req.params.taskId}`, error instanceof Error ? error : new Error(String(error)));
    res.status(500).json({ error: 'Failed to reset purchase count' });
  }
});

// Background refresh function for floor prices - fire-and-forget, non-blocking
async function refreshCollectionFloorInBackground(
  key: string,
  slug: string,
  contractAddress: string
): Promise<void> {
  const state = collectionFloorCache.get(key);
  if (!state || state.isRefreshing) return;

  state.isRefreshing = true;

  try {
    const [openseaFloor, openseaBestOffer, blurFloor, blurBestOffer, meFloor, meBestOffer] =
      await Promise.allSettled([
        getCachedFloorPrice(slug),
        fetchOpenseaOffers('COLLECTION', slug, {}).then((r: any) => Number(r[0]?.eth) || 0),
        fetchBlurCollectionStats(slug),
        fetchBlurBid(contractAddress, "COLLECTION", {}).then((r: any) =>
          r?.priceLevels?.[0]?.price ? Number(r.priceLevels[0].price) : 0),
        fetchMagicEdenCollectionStats(contractAddress).then(Number),
        fetchMagicEdenOffer("COLLECTION", contractAddress, "{}").then((r: any) => Number(r[0]?.eth) || 0),
      ]);

    // Update only successful results (preserve existing values on failure)
    if (openseaFloor.status === 'fulfilled') state.floorPrices.opensea = openseaFloor.value;
    if (openseaBestOffer.status === 'fulfilled') state.bestOffers.opensea = openseaBestOffer.value;
    if (blurFloor.status === 'fulfilled') state.floorPrices.blur = blurFloor.value;
    if (blurBestOffer.status === 'fulfilled') state.bestOffers.blur = blurBestOffer.value;
    if (meFloor.status === 'fulfilled') state.floorPrices.magiceden = meFloor.value;
    if (meBestOffer.status === 'fulfilled') state.bestOffers.magiceden = meBestOffer.value;

    state.lastFetched = Date.now();
    // Background refresh success log removed - was logging every 4 minutes per collection (15MB/day)
    // Only failures are logged
  } catch (error) {
  } finally {
    state.isRefreshing = false;
  }
}

// Fetch floor prices for all tasks and all marketplaces
// NON-BLOCKING: Returns cached data immediately, triggers background refresh for stale collections
app.post('/api/floor', async (req, res) => {
  try {
    const { tasks }: { tasks: { _id: string, slug: string, contractAddress: string }[] } = req.body;

    const responseFloorPrices: BidCounts = {};
    const responseBestOffers: BidCounts = {};
    const collectionsToRefresh: Array<{ key: string; slug: string; contractAddress: string }> = [];

    // Group tasks by unique slug+contractAddress
    const collectionToTaskIds = new Map<string, string[]>();
    const collectionInfo = new Map<string, { slug: string, contractAddress: string }>();

    for (const task of tasks) {
      if (!task._id) continue;
      const key = `${task.slug}:${task.contractAddress}`;
      if (!collectionToTaskIds.has(key)) {
        collectionToTaskIds.set(key, []);
        collectionInfo.set(key, { slug: task.slug, contractAddress: task.contractAddress });
      }
      collectionToTaskIds.get(key)!.push(task._id);
    }

    // Return cached data immediately, mark stale for refresh
    const now = Date.now();

    for (const [key, { slug, contractAddress }] of collectionInfo.entries()) {
      const taskIds = collectionToTaskIds.get(key) || [];
      let state = collectionFloorCache.get(key);

      if (!state) {
        state = {
          floorPrices: { opensea: 0, blur: 0, magiceden: 0 },
          bestOffers: { opensea: 0, blur: 0, magiceden: 0 },
          lastFetched: 0,
          isRefreshing: false,
        };
        collectionFloorCache.set(key, state);
      }

      // Assign cached values to all tasks for this collection
      for (const taskId of taskIds) {
        responseFloorPrices[taskId] = { ...state.floorPrices };
        responseBestOffers[taskId] = { ...state.bestOffers };
        // Also update the global floorPrices/bestOffers for other parts of the system
        floorPrices[taskId] = { ...state.floorPrices };
        bestOffers[taskId] = { ...state.bestOffers };
      }

      // Mark stale collections for background refresh
      if ((now - state.lastFetched) > FLOOR_STALE_THRESHOLD && !state.isRefreshing) {
        collectionsToRefresh.push({ key, slug, contractAddress });
      }
    }

    // Return immediately (non-blocking)
    res.status(200).json({
      floorPrices: responseFloorPrices,
      bestOffers: responseBestOffers,
      _meta: { cached: true, refreshingCount: collectionsToRefresh.length }
    });

    // Trigger background refresh (fire-and-forget)
    if (collectionsToRefresh.length > 0) {
      setImmediate(() => {
        for (const { key, slug, contractAddress } of collectionsToRefresh) {
          refreshCollectionFloorInBackground(key, slug, contractAddress).catch(() => { });
        }
      });
    }
  } catch (error: any) {
    errorLog('API', 'Failed to fetch floor prices', error instanceof Error ? error : new Error(String(error)));
    res.status(500).json({ error: 'Failed to fetch floor prices' });
  }
});


app.post('/api/collection', async (req, res) => {
  try {
    const slug = req.body.slug as string;

    if (!slug) {
      console.error('[API] /api/collection: Missing slug in request');
      return res.status(400).json({
        status: "error",
        message: "Collection slug is required"
      });
    }

    // STEP 1: Check LRU cache first (<1ms access)
    const cacheKey = slug.toLowerCase();
    const cached = collectionDataLRU.get(cacheKey);

    if (cached) {
      const age = Date.now() - cached.cachedAt;
      return res.status(200).json(cached.data);
    }


    // STEP 2: Fetch from APIs (existing logic continues...)
    const { collectionResult, magicEdenResult } = await fetchCollectionData(slug);

    // Extract data safely based on success flags
    const collection: CollectionData | null = collectionResult.success ? collectionResult.data?.data ?? null : null;
    const magicEdenData: MagicEdenCollectionsResponse | null = magicEdenResult.success ? magicEdenResult.data?.data ?? null : null;

    // Handle case where both API calls fail
    if (!collection && !magicEdenData) {
      console.error(`[API] /api/collection: Both APIs failed for: ${slug}`, {
        openseaError: collectionResult.error instanceof Error ? collectionResult.error.message : String(collectionResult.error),
        magicEdenError: magicEdenResult.error instanceof Error ? magicEdenResult.error.message : String(magicEdenResult.error)
      });

      return res.status(404).json({
        error: "Unable to fetch collection data",
        details: {
          opensea: collectionResult.error instanceof Error ? collectionResult.error.message : "Unknown error",
          magiceden: magicEdenResult.error instanceof Error ? magicEdenResult.error.message : "Unknown error"
        }
      });
    }

    const collectionAddress =
      magicEdenData?.collections?.[0]?.chainData?.contract;

    const slugValid =
      (collection?.collection_offers_enabled ?? false) ||
      typeof collectionAddress === "string";

    const contractAddress =
      collection?.contracts?.[0]?.address || collectionAddress;

    const data = {
      ...collection,
      slugValid,
      contractAddress,
    };

    // STEP 3: Cache result if valid (non-blocking)
    if (slugValid) {
      try {
        collectionDataLRU.set(cacheKey, {
          data,
          cachedAt: Date.now()
        });
      } catch (cacheError: any) {
        // Fail-open: cache errors don't break endpoint
        errorLog('CollectionData', `Failed to cache result for ${slug}`, cacheError instanceof Error ? cacheError : new Error(String(cacheError)));
      }

      return res.status(200).json(data);
    } else {
      // Don't cache invalid slugs (400 errors)
      console.warn(`[API] /api/collection: Collection offers not enabled for: ${slug}`);
      return res.status(400).json({ error: "Collection offers not enabled" });
    }
  } catch (error: any) {
    console.error('[API] /api/collection: Unexpected error', {
      error: error instanceof Error ? error.message : String(error),
      stack: error instanceof Error ? error.stack : undefined,
      errorType: error?.constructor?.name,
      fullError: JSON.stringify(error, Object.getOwnPropertyNames(error))
    });

    errorLog('API', 'Failed to fetch collection details', error instanceof Error ? error : new Error(String(error)))

    return res.status(500).json({
      status: "error",
      message: "Failed to fetch collection details",
      error: error instanceof Error ? error.message : "Unknown error"
    });
  }
})

app.post('/api/collection/stats', async (req, res) => {
  try {
    const slug = req.body.slug as string;
    const contractAddress = req.body.address as string;
    const startTime = Date.now();

    // 1. Check LRU cache first (24-hour TTL, <1ms access)
    const cacheKey = `${slug}:${contractAddress}`;
    const cached = collectionStatsLRU.get(cacheKey);

    if (cached) {
      const age = Date.now() - cached.cachedAt;
      return res.status(200).json(cached.data);
    }


    // Helper to time individual function calls with cache hit/miss detection
    const timeFunction = async <T>(
      name: string,
      fn: Promise<T>
    ): Promise<{ result: T | null; elapsed: number; status: 'fulfilled' | 'rejected' }> => {
      const start = Date.now();
      try {
        const result = await fn;
        const elapsed = Date.now() - start;

        // Detect cache hit/miss based on response time (<10ms = cache hit)
        const cacheStatus = elapsed < 10 ? 'CACHE_HIT' : 'CACHE_MISS';
        return { result, elapsed, status: 'fulfilled' };
      } catch (error: any) {
        const elapsed = Date.now() - start;
        return { result: null, elapsed, status: 'rejected' };
      }
    };

    // 2. Fetch data using EXISTING cached functions (with timing instrumentation)
    const timedCalls = await Promise.all([
      timeFunction('getCachedFloorPrice', getCachedFloorPrice(slug)),
      timeFunction('getBlurFloor', getBlurFloor(contractAddress)),
      timeFunction('getMagicEdenFloor', getMagicEdenFloor(contractAddress)),
      timeFunction('fetchOpenseaTraits', fetchOpenseaTraits(slug)),
      timeFunction('getCachedOpenseaBestOffer', getCachedOpenseaBestOffer(slug)),
      timeFunction('getCachedBlurBestOffer', getCachedBlurBestOffer(contractAddress))
      // TODO: Re-enable after adding timeout to MagicEden API (currently times out at 30s)
      // TODO: Fix in magiceden/bid.ts:1312 - add timeout: 5000 to axiosInstance.get()
      // timeFunction('getCachedMagicEdenBestOffer', getCachedMagicEdenBestOffer(contractAddress))
    ]);

    // Calculate total API time (sum of all parallel calls)
    const totalApiTime = timedCalls.reduce((sum, call) => sum + call.elapsed, 0);
    const longestCall = timedCalls.reduce((max, call) => call.elapsed > max.elapsed ? call : max, timedCalls[0]);
    const cacheHits = timedCalls.filter(call => call.elapsed < 10).length;
    const cacheMisses = timedCalls.filter(call => call.elapsed >= 10).length;

    // Extract results (handle both fulfilled and rejected)
    const [
      openseaFloorResult,
      blurFloorResult,
      magicedenFloorResult,
      openseaTraitsResult,
      openseaBestOfferResult,
      blurBestOfferResult
      // magicedenBestOfferResult - temporarily removed, using OpenSea value instead (see line 23831)
    ] = timedCalls.map(call => ({
      status: call.status,
      value: call.result
    }));

    // 3. Extract values with fallbacks (graceful degradation if individual marketplaces fail)
    const openseaFloorPrice = openseaFloorResult.status === 'fulfilled' ? openseaFloorResult.value : 0;
    const blurFloorPrice = blurFloorResult.status === 'fulfilled' ? blurFloorResult.value : 0;
    const magicedenFloorPrice = magicedenFloorResult.status === 'fulfilled' ? magicedenFloorResult.value : 0;
    const openseaTraits = openseaTraitsResult.status === 'fulfilled' ? openseaTraitsResult.value : [];
    const openseaBestOffer = openseaBestOfferResult.status === 'fulfilled' ? Number(openseaBestOfferResult.value?.eth) || 0 : 0;
    const blurBestOffer = blurBestOfferResult.status === 'fulfilled' ? Number(blurBestOfferResult.value?.price) || 0 : 0;
    const magicedenBestOffer = openseaBestOfferResult.status === 'fulfilled' ? Number(openseaBestOfferResult.value?.eth) || 0 : 0; // Temporarily use OpenSea result to avoid MagicEden timeout


    // Use fallback data for disabled APIs (MagicEden + Blur traits temporarily disabled due to timeouts)
    const magicEdenTraits = { traits: { categories: {}, counts: {} }, magicedenRaw: { attributes: [] } };
    const blurTraits = { traits: { categories: {}, counts: {} }, blurRaw: { success: false, traits: {} } };

    const traitsProcessStart = Date.now();

    const combinedTraits: {
      categories: Record<string, string>;
      counts: Record<
        string,
        Record<
          string,
          {
            count: number;
            availableInMarketplaces: string[];
            magicedenFloor: number;
            blurFloor: number;
            openseaFloor: number;
          }
        >
      >;
    } = {
      categories: {},
      counts: {},
    };

    if (Array.isArray(openseaTraits)) {

      openseaTraits.forEach((trait: any) => {
        const category = trait.traitType;
        if (!combinedTraits.categories[category]) {
          combinedTraits.categories[category] = "string";
        }
        if (!combinedTraits.counts[category]) {
          combinedTraits.counts[category] = {};
        }

        if (trait.values && Array.isArray(trait.values)) {
          trait.values.forEach((valueObj: any) => {
            const traitValue = valueObj.value;
            const count = valueObj.count || 0;

            if (!combinedTraits.counts[category][traitValue]) {
              combinedTraits.counts[category][traitValue] = {
                count: 0,
                availableInMarketplaces: [],
                magicedenFloor: 0,
                blurFloor: 0,
                openseaFloor: 0,
              };
            }

            combinedTraits.counts[category][traitValue].count = count;
            combinedTraits.counts[category][traitValue].availableInMarketplaces.push("opensea");
          });
        }
      });
    }

    // Process MagicEden and Blur traits (object format)
    const marketplaces = ["magiceden", "blur"] as const;
    const traitsData = [
      (magicEdenTraits as MagicEdenData)?.traits || { categories: {}, counts: {} },
      (blurTraits as IBlurData)?.traits || { categories: {}, counts: {} },
    ];

    marketplaces.forEach((marketplace, index) => {
      const traits = traitsData[index];

      // Skip if traits is invalid
      if (!traits || !traits.categories || !traits.counts) {
        return;
      }

      Object.entries(traits.categories).forEach(([category]) => {
        if (!combinedTraits.categories[category]) {
          combinedTraits.categories[category] = "string";
        }
        if (!combinedTraits.counts[category]) {
          combinedTraits.counts[category] = {};
        }

        Object.entries(traits.counts[category]).forEach(([trait, count]) => {
          if (!combinedTraits.counts[category][trait]) {
            combinedTraits.counts[category][trait] = {
              count: 0,
              availableInMarketplaces: [],
              magicedenFloor: 0,
              blurFloor: 0,
              openseaFloor: 0,
            };
          }

          if (
            typeof count === "number" &&
            count > combinedTraits.counts[category][trait].count
          ) {
            combinedTraits.counts[category][trait].count = count;
          }

          if (
            !combinedTraits.counts[category][
              trait
            ].availableInMarketplaces.includes(marketplace)
          ) {
            combinedTraits.counts[category][trait].availableInMarketplaces.push(
              marketplace
            );
          }

          if (
            marketplace === "blur" &&
            (blurTraits as IBlurData)?.blurRaw?.traits &&
            (blurTraits as IBlurData)?.blurRaw?.traits[category]
          ) {
            const blurFloor =
              Number(
                (blurTraits as IBlurData)?.blurRaw?.traits[category][trait]
                  ?.floor?.amount
              ) || 0;
            combinedTraits.counts[category][trait].blurFloor = blurFloor;
          }

          if (
            marketplace === "magiceden" &&
            (magicEdenTraits as MagicEdenData)?.magicedenRaw?.attributes
          ) {
            const magicEdenAttribute = (
              magicEdenTraits as MagicEdenData
            )?.magicedenRaw?.attributes.find(
              (attr: any) => attr.key === category
            );
            if (magicEdenAttribute) {
              const magicEdenValue = magicEdenAttribute.values.find(
                (value: any) => value.value === trait
              );
              if (magicEdenValue && magicEdenValue.floorAskPrice) {
                const meFloor = magicEdenValue?.floorAskPrice?.amount?.decimal;
                combinedTraits.counts[category][trait].magicedenFloor = meFloor;
              }
            }
          }
        });
      });
    });

    Object.keys(combinedTraits.counts).forEach(category => {
      Object.keys(combinedTraits.counts[category]).forEach(trait => {
        const traitData = combinedTraits.counts[category][trait];
        traitData.openseaFloor = traitData.magicedenFloor;
      });
    });


    // 4. Build response data
    const data = {
      traits: combinedTraits,
      magicEdenValid: Number(magicedenFloorPrice) > 0,
      blurValid: Number(blurFloorPrice) > 0,
      openseaValid: Number(openseaFloorPrice) > 0,
      blurFloorPrice,
      magicedenFloorPrice,
      openseaFloorPrice,
      openseaBestOffer,
      blurBestOffer,
      magicedenBestOffer,
      contractAddress,
      slug
    };

    // 5. Populate collectionFloorCache so /api/floor has valid data immediately
    // This prevents the dashboard from seeing 0 values when a task is first created
    const floorCacheKey = `${slug}:${contractAddress}`;
    collectionFloorCache.set(floorCacheKey, {
      floorPrices: {
        opensea: Number(openseaFloorPrice) || 0,
        blur: Number(blurFloorPrice) || 0,
        magiceden: Number(magicedenFloorPrice) || 0,
      },
      bestOffers: {
        opensea: openseaBestOffer || 0,
        blur: blurBestOffer || 0,
        magiceden: magicedenBestOffer || 0,
      },
      lastFetched: Date.now(),
      isRefreshing: false,
    });

    // 6. Cache result in LRU (non-blocking, <1ms)
    collectionStatsLRU.set(cacheKey, {
      data,
      cachedAt: Date.now()
    });

    const breakdown = timedCalls.map(call => `${call.elapsed}ms`).join(', ');
    res.status(200).json(data);
  } catch (error: any) {
    errorLog('API', 'Unexpected error in collection stats endpoint', error);
    errorLog('API', 'Failed to process collection stats', error instanceof Error ? error : new Error(String(error)))
    res.status(500).json({ error: 'Failed to process collection stats' });
  }
})

app.post('/api/currency/conversion', async (req, res) => {
  try {
    const wallet = currentTasks && currentTasks.length > 0 ? currentTasks[0].wallet : "0x1b8AfD0DE8a9368ce635Be8B9572c65d8F590B21"
    const data = await fetchAddressBalances(wallet);

    const usdPrice = data.data.addressBalances.find((bal) => bal.currency.contractAddress.toLowerCase() === "0x0000000000000000000000000000000000000000".toLowerCase())?.currency.usdPrice ?? 0
    res.status(200).json(usdPrice)
  } catch (error: any) {
    errorLog('API', 'Failed to fetch currency conversion', error instanceof Error ? error : new Error(String(error)))
    res.status(500).json({ error: 'Failed to fetch currency conversion' });
  }

})

// Password endpoint for storing user password in server memory
app.post('/api/auth/password', async (req, res) => {
  try {
    const { encryptedPassword } = req.body;
    const password = decryptPassword(encryptedPassword)

    if (!password) {
      return res.status(400).json({ error: 'Password is required' });
    }
    const wasPasswordSet = !!getPassword();

    // Store password in memory (and optionally Redis)
    await setPassword(password);

    // If this is the first time setting password, resume queue and workers
    if (!wasPasswordSet) {
      await resumeQueueAndWorkers();
    }

    res.status(200).json({ success: true, message: 'Password stored successfully' });
  } catch (error: any) {
    errorLog('API', 'Failed to store password', error);
    warnLog('Authentication', 'Failed to store password')
    res.status(500).json({
      error: 'Failed to store password',
      message: error.message
    });
  }
});

// Check if password is set
app.get('/api/auth/password-status', async (req, res) => {
  res.status(200).json({ hasPassword: !!getPassword() });
});

// Renew password TTL (called by client on activity)
app.post('/api/auth/renew-password', async (req, res) => {
  try {
    if (!getPassword()) {
      return res.status(401).json({ error: 'No password set' });
    }

    await renewPasswordTTL();
    res.status(200).json({ success: true, message: 'Password TTL renewed' });
  } catch (error: any) {
    errorLog('API', 'Failed to renew password TTL', error);
    res.status(500).json({
      error: 'Failed to renew password TTL',
      message: error.message
    });
  }
});

// Change password and re-encrypt wallets
app.post('/api/auth/change-password', async (req, res) => {
  try {
    const { encryptedOldPassword, encryptedNewPassword } = req.body;

    if (!encryptedOldPassword || !encryptedNewPassword) {
      return res.status(400).json({ error: 'Both old and new passwords are required' });
    }

    // Decrypt the passwords
    const oldPassword = decryptPassword(encryptedOldPassword);
    const newPassword = decryptPassword(encryptedNewPassword);

    // Verify old password matches current
    const currentPassword = getPassword();
    if (!currentPassword) {
      return res.status(400).json({ error: 'No password is currently set' });
    }

    if (currentPassword !== oldPassword) {
      return res.status(401).json({ error: 'Old password is incorrect' });
    }


    // Get all wallets from database
    const wallets = await Wallet.find({ encryptedPrivateKey: { $exists: true, $ne: null } });

    let successCount = 0;
    let failedWallets: string[] = [];

    // Re-encrypt each wallet
    for (const wallet of wallets) {
      try {
        const newEncryptedKey = await reEncryptWallet(
          wallet.encryptedPrivateKey,
          oldPassword,
          newPassword
        );

        // Update wallet in database
        wallet.encryptedPrivateKey = newEncryptedKey;
        await wallet.save();
        successCount++;

      } catch (error) {
        errorLog('WalletEncryption', `Failed to re-encrypt wallet ${wallet.address}`, error);
        failedWallets.push(wallet.address);
      }
    }

    // Clear the decrypted key cache
    clearDecryptedKeyCache();

    // If all wallets were successfully re-encrypted, update the password
    if (failedWallets.length === 0) {
      await setPassword(newPassword);

      res.status(200).json({
        success: true,
        message: 'Password changed successfully',
        walletsReEncrypted: successCount
      });
    } else {
      errorLog('WalletEncryption', `Failed to re-encrypt ${failedWallets.length} wallets`, undefined);
      res.status(500).json({
        error: 'Failed to re-encrypt some wallets',
        failedWallets,
        successCount
      });
    }

  } catch (error: any) {
    errorLog('API', 'Failed to change password', error);
    warnLog('Authentication', 'Failed to change password');
    res.status(500).json({
      error: 'Failed to change password',
      message: error.message
    });
  }
});

// Manual password recovery endpoint for debugging
app.post('/api/auth/recover-password', async (req, res) => {
  try {

    // Check BullMQ connection health
    const bullmqHealthy = await checkRedisHealth();
    if (!bullmqHealthy) {
      return res.status(503).json({
        error: 'BullMQ not connected',
        message: 'BullMQ Redis connections are not ready'
      });
    }

    const recovered = await recoverPasswordFromRedis();

    if (recovered) {
      // Resume operations if password was recovered
      await resumeQueueAndWorkers();

      res.status(200).json({
        success: true,
        message: 'Password recovered from Redis and operations resumed',
        passwordSet: true
      });
    } else {
      res.status(404).json({
        success: false,
        message: 'No password found in Redis',
        passwordSet: false
      });
    }
  } catch (error: any) {
    errorLog('API', 'Failed to recover password', error);
    res.status(500).json({
      error: 'Failed to recover password',
      message: error.message,
      stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
    });
  }
});

// Blur rate limit monitoring endpoints
app.get('/api/blur-rate-limits', async (req, res) => {
  try {
    const status = blurRateLimiter.getStatus();
    res.status(200).json({
      success: true,
      data: status
    });
  } catch (error: any) {
    errorLog('API', 'Failed to get Blur rate limits', error);
    res.status(500).json({
      success: false,
      error: error.message || 'Failed to get rate limit status'
    });
  }
});

app.get('/api/blur-rate-limits/:wallet', async (req, res) => {
  try {
    const { wallet } = req.params;
    if (!wallet) {
      return res.status(400).json({
        success: false,
        error: 'Wallet address is required'
      });
    }

    const status = blurRateLimiter.getStatus(wallet);
    res.status(200).json({
      success: true,
      data: status
    });
  } catch (error: any) {
    errorLog('API', 'Failed to get Blur rate limit for wallet', error);
    res.status(500).json({
      success: false,
      error: error.message || 'Failed to get rate limit status'
    });
  }
});

app.post('/api/blur-rate-limits/:wallet/reset', async (req, res) => {
  try {
    const { wallet } = req.params;
    if (!wallet) {
      return res.status(400).json({
        success: false,
        error: 'Wallet address is required'
      });
    }

    // Only allow in development mode or with API key
    if (process.env.NODE_ENV !== 'development') {
      const apiKey = req.headers['x-api-key'];
      if (!apiKey || apiKey !== API_KEY) {
        return res.status(401).json({
          success: false,
          error: 'Unauthorized'
        });
      }
    }

    blurRateLimiter.reset(wallet);
    res.status(200).json({
      success: true,
      message: `Rate limit reset for wallet ${wallet}`
    });
  } catch (error: any) {
    errorLog('API', 'Failed to reset Blur rate limit', error);
    res.status(500).json({
      success: false,
      error: error.message || 'Failed to reset rate limit'
    });
  }
});

// Clear all Blur authentication tokens
app.post('/api/blur-auth/clear', async (req, res) => {
  try {
    // Only allow in development mode or with API key
    if (process.env.NODE_ENV !== 'development') {
      const apiKey = req.headers['x-api-key'];
      if (!apiKey || apiKey !== API_KEY) {
        return res.status(401).json({
          success: false,
          error: 'Unauthorized'
        });
      }
    }

    // Clear all Blur access tokens from LRU cache
    const clearedCount = blurAccessTokenLRU.size;
    blurAccessTokenLRU.clear();

    res.status(200).json({
      success: true,
      message: `Cleared ${clearedCount} Blur authentication tokens`,
      clearedTokens: clearedCount
    });
  } catch (error: any) {
    errorLog('API', 'Failed to clear Blur auth tokens', error);
    res.status(500).json({
      success: false,
      error: error.message || 'Failed to clear authentication tokens'
    });
  }
});

// Blur API health check endpoint
app.get('/api/blur-health', async (req, res) => {
  try {
    const startTime = Date.now();
    const healthStatus: {
      status: string;
      responseTime: number;
      rateLimitStatus: { totalWallets?: number; rateLimitedWallets?: number; wallets?: any[] };
      authStatus: { success?: boolean; error?: string; wallet?: string; tokenReceived?: boolean; statusCode?: number };
      authMetrics: any;
      timeoutMetrics: any;
      lastError: string | null;
      timestamp: string;
    } = {
      status: 'unknown',
      responseTime: 0,
      rateLimitStatus: {},
      authStatus: {},
      authMetrics: getAuthMetrics(),
      timeoutMetrics: getTimeoutMetrics(),
      lastError: null,
      timestamp: new Date().toISOString()
    };

    // Check rate limit status
    healthStatus.rateLimitStatus = blurRateLimiter.getStatus();

    // Test authentication for a sample wallet if tasks exist
    if (currentTasks.length > 0) {
      const sampleTask = currentTasks[0];
      const wallet = await walletCache.getWalletById(sampleTask.walletId);

      if (wallet) {
        try {
          const privateKey = decryptPrivateKey(wallet.encryptedPrivateKey as string);
          const accessToken = await getAccessToken(privateKey);

          healthStatus.authStatus = {
            success: !!accessToken,
            wallet: sampleTask.wallet,
            tokenReceived: !!accessToken
          };
        } catch (authError: any) {
          healthStatus.authStatus = {
            success: false,
            error: authError.message,
            statusCode: authError.response?.status
          };
          healthStatus.lastError = authError.message;
        }
      }
    }

    // Calculate overall status
    const hasAuthIssues = healthStatus.authStatus.success === false;
    const hasRateLimitIssues = (healthStatus.rateLimitStatus.rateLimitedWallets || 0) > 0;

    if (hasAuthIssues) {
      healthStatus.status = 'unhealthy';
    } else if (hasRateLimitIssues) {
      healthStatus.status = 'degraded';
    } else {
      healthStatus.status = 'healthy';
    }

    healthStatus.responseTime = Date.now() - startTime;

    // Return appropriate status code based on health
    const statusCode = healthStatus.status === 'healthy' ? 200 :
      healthStatus.status === 'degraded' ? 200 : 503;

    res.status(statusCode).json(healthStatus);
  } catch (error: any) {
    errorLog('API', 'Failed to check Blur health', error);
    res.status(500).json({
      status: 'error',
      error: error.message || 'Failed to check Blur health',
      timestamp: new Date().toISOString()
    });
  }
});

// Task metrics health check endpoint - diagnostic tool for debugging task metrics issues
app.get('/api/task-metrics-health', (req, res) => {
  try {
    const health = {
      timestamp: new Date().toISOString(),
      connectedClients: clients.size,
      publishingEnabled: clients.size > 0,
      tasks: {
        inBidStats: Object.keys(bidStats),
        inTaskStatus: Object.keys(taskStatus),
        inActiveTasks: Array.from(activeTasks.keys()).filter(taskId => {
          const task = activeTasks.get(taskId);
          return task?.running === true;
        }),
        orphaned: Object.keys(taskStatus).filter(taskId => !bidStats[taskId])
      },
      bidStatsSnapshot: bidStats,
      taskStatusSnapshot: Object.keys(taskStatus).reduce((acc, taskId) => {
        acc[taskId] = {
          selectedMarketplaces: taskStatus[taskId]?.selectedMarketplaces || [],
          lastUpdate: taskStatus[taskId]?.lastUpdate || 0,
          marketplaces: Object.keys(taskStatus[taskId]?.marketplaces || {})
        };
        return acc;
      }, {} as any)
    };

    res.json(health);
  } catch (error: any) {
    errorLog('API', 'Failed to get task metrics health', error);
    res.status(500).json({
      status: 'error',
      error: error.message || 'Failed to get task metrics health',
      timestamp: new Date().toISOString()
    });
  }
});


// Secure update endpoint with authentication and rate limiting
app.post('/api/update-app', async (req, res) => {

  try {
    // Authentication check - require API key
    const apiKey = req.headers['x-api-key']

    if (!apiKey || apiKey !== API_KEY) {
      warnLog('Update', `AUTH FAILED - apiKey provided: ${!!apiKey}, matches: ${apiKey === API_KEY}`);
      warnLog('Security', 'UNAUTHORIZED_UPDATE_ATTEMPT - Unauthorized update attempt');
      return res.status(401).json({ error: 'Unauthorized' });
    }

    // Additional security: Only allow updates from the server IP
    const SERVER_IP = process.env.SERVER_IP as string;
    if (!SERVER_IP) {
      errorLog('Update', 'SERVER_IP not configured in environment');
      return res.status(500).json({ error: 'Server IP not configured' });
    }

    // Rate limiting check - prevent abuse
    const rateLimitKey = req.ip || 'unknown';
    let entry = updateRateLimitLRU.get(rateLimitKey);
    if (!entry || entry.resetAt < Date.now()) {
      // Create new entry or reset expired entry
      entry = { count: 1, resetAt: Date.now() + (60 * 60 * 1000) };
    } else {
      // Increment existing entry
      entry.count++;
    }
    updateRateLimitLRU.set(rateLimitKey, entry);

    if (entry.count > 3) { // Max 3 update attempts per hour
      warnLog('Update', 'RATE_LIMIT_EXCEEDED');
      warnLog('Security', 'RATE_LIMIT_EXCEEDED - Update rate limit exceeded');
      return res.status(429).json({ error: 'Too many requests' });
    }

    // Check for updates

    const { data: update } = await axios.post(
      "https://updates.nfttools.io/api/v1/check", {},
      {
        headers: {
          "x-app-version": "0.0.0",
          username: process.env.USERNAME
        },
        timeout: 10000 // 10 second timeout
      }
    );


    if (!update?.releaseInfo?.downloadUrl) {
      warnLog('Update', 'No downloadUrl in update response');
      return res.status(404).json({ error: 'No update available' });
    }

    // Validate the download URL is from trusted source
    const trustedDomains = ['updates.nfttools.io', 'github.com', 'githubusercontent.com'];
    const urlObj = new URL(update.releaseInfo.downloadUrl);

    if (!trustedDomains.some(domain => urlObj.hostname.includes(domain))) {
      warnLog('Update', `UNTRUSTED URL - hostname ${urlObj.hostname} not in trusted domains`);
      warnLog('Security', 'UNTRUSTED_UPDATE_URL - Update from untrusted source blocked');
      return res.status(400).json({ error: 'Invalid update source' });
    }

    const scriptUrl = update.releaseInfo.downloadUrl;

    // Trigger update with timeout

    const { data } = await axios.post(`http://host.docker.internal:9999/update`,
      { scriptUrl },
      { timeout: 60000 } // 60 second timeout
    );

    warnLog('Security', 'UPDATE_TRIGGERED - Application update triggered successfully');

    res.status(200).json(data);
  } catch (error: any) {
    errorLog('Update', '=== UPDATE FAILED ===');
    errorLog('Update', `Error type: ${error.name || 'Unknown'}`);
    errorLog('Update', `Error message: ${error.message || 'No message'}`);
    if (error.response) {
      errorLog('Update', `Response status: ${error.response.status}`);
      errorLog('Update', `Response data: ${JSON.stringify(error.response.data)}`);
    }
    if (error.code) {
      errorLog('Update', `Error code: ${error.code}`);
    }
    errorLog('API', 'Failed to schedule update', error instanceof Error ? error : new Error(String(error)));
    res.status(500).json({
      scheduled: false,
      message: 'Failed to schedule update',
      error: process.env.NODE_ENV === 'production' ? 'Internal server error' : error.message
    });
  }
});

app.get('/api/wallet/nfts', async (req, res) => {
  try {

    // Get all wallet addresses from the database
    const wallets = await Wallet.find({}, 'address');
    const walletAddresses = wallets.map((item) => ({ walletId: item._id.toString(), address: item.address }))

    if (walletAddresses.length === 0) {
      return res.status(200).json({
        success: true,
        data: [],
        message: 'No wallets found'
      });
    }

    // Fetch NFTs for all wallets
    const nftData = await fetchWalletNFTs(walletAddresses);

    res.status(200).json({
      success: true,
      data: nftData,
      count: nftData.length
    });
  } catch (error: any) {
    errorLog('API', 'Failed to fetch wallet NFTs', error);
    errorLog('API', 'Failed to fetch wallet NFTs', error instanceof Error ? error : new Error(String(error)))
    res.status(500).json({
      success: false,
      message: 'Failed to fetch wallet NFTs',
      error: error.message
    });
  }
});

// Clear wallet cache endpoint (called by bidding-bot-client on wallet deletion)
app.post('/api/wallet/:id/clear-cache', async (req, res) => {
  try {
    const { id } = req.params;

    // Fetch wallet to get encryptedPrivateKey
    const wallet = await walletCache.getWalletById(id);
    if (!wallet) {
      return res.status(404).json({
        success: false,
        error: 'Wallet not found'
      });
    }

    // Clear from crypto.ts decrypted key cache
    clearDecryptedKey(wallet.encryptedPrivateKey);

    // Invalidate wallet-cache.ts metadata cache
    walletCache.invalidateWallet(wallet.address);

    // Remove from wallet address tracking
    removeOurWalletAddress(wallet.address);


    return res.json({
      success: true,
      message: 'Wallet cache cleared'
    });
  } catch (error) {
    errorLog('WalletCache', 'Failed to clear wallet cache via API', error);
    return res.status(500).json({
      success: false,
      error: 'Failed to clear cache'
    });
  }
});

// Add wallet address to tracking (called by bidding-bot-client on wallet creation)
app.post('/api/wallet/add-address', async (req, res) => {
  try {
    const { address } = req.body;

    if (!address) {
      return res.status(400).json({
        success: false,
        error: 'Address is required'
      });
    }

    // Add to wallet address tracking set
    addOurWalletAddress(address);


    return res.json({
      success: true,
      message: 'Wallet address added to tracking'
    });
  } catch (error) {
    errorLog('WalletTracking', 'Failed to add wallet address via API', error);
    return res.status(500).json({
      success: false,
      error: 'Failed to add address'
    });
  }
});

// NFT metadata endpoint
app.get('/api/collection/:contractAddress/:tokenId/:slug/:walletAddress', async (req, res) => {
  try {
    const { contractAddress, tokenId, slug, walletAddress } = req.params;

    // Validate parameters
    if (!contractAddress || !tokenId || !slug || !walletAddress) {
      return res.status(400).json({
        success: false,
        error: 'wallet address, slug, contract address and token ID are required'
      });
    }

    const collectionDetails = await getCachedCollectionDetails(slug, contractAddress)
    const enforceCreatorFee = collectionDetails?.enforceCreatorFee
    const fees = collectionDetails.fees

    // Extract creator fees
    const creatorFees: { [walletAddress: string]: number } = {};
    if (enforceCreatorFee && fees && Array.isArray(fees)) {
      // OpenSea marketplace fee address
      const OPENSEA_FEE_ADDRESS = '0x0000a26b00c1f0df003000390027140000faa719';

      // Find creator fee (any address that's not OpenSea's)
      const creatorFee = fees.find(fee =>
        fee.recipient.toLowerCase() !== OPENSEA_FEE_ADDRESS.toLowerCase()
      );

      if (creatorFee && creatorFee.recipient) {
        creatorFees[creatorFee.recipient.toLowerCase()] = creatorFee.fee;
      }
    }

    // Marketplace addresses
    const marketplaceAddresses = {
      opensea: '0x1E0049783F008A0085193E00003D00cd54003c71',
      magiceden: '0x9A1D00bEd7CD04BCDA516d721A596eb22Aac6834',
      blur: '0x2f18f339620a63e43f0839eeb18d7de1e1be4dfb'
    };

    // Create contract instance
    const approveAbi = [
      {
        inputs: [
          { internalType: 'address', name: 'account', type: 'address' },
          { internalType: 'address', name: 'operator', type: 'address' }
        ],
        name: 'isApprovedForAll',
        outputs: [{ internalType: 'bool', name: '', type: 'bool' }],
        stateMutability: 'view',
        type: 'function'
      },
      {
        inputs: [
          { internalType: 'address', name: 'operator', type: 'address' },
          { internalType: 'bool', name: 'approved', type: 'bool' }
        ],
        name: 'setApprovalForAll',
        outputs: [],
        stateMutability: 'nonpayable',
        type: 'function'
      }
    ];

    // Set up provider with retry
    const provider = await withRetry(async () => {
      const p = new ethers.AlchemyProvider('mainnet', ALCHEMY_API_KEY);
      // Test the connection
      await p.getNetwork();
      return p;
    });

    const nftContract = new Contract(contractAddress, approveAbi, provider);
    const contractInterface = new ethers.Interface(approveAbi);

    // Function to check approval for a marketplace
    const checkMarketplaceApproval = async (marketplaceName: string, marketplaceAddress: string) => {
      try {
        // Check if approved
        const isApproved = await nftContract.isApprovedForAll(walletAddress, marketplaceAddress);

        let gasEstimate = null;

        if (!isApproved) {
          // Estimate gas for approval
          try {
            const encodedData = contractInterface.encodeFunctionData('setApprovalForAll', [
              marketplaceAddress,
              true
            ]);

            // Estimate gas using provider.estimateGas
            const gasLimit = await provider.estimateGas({
              to: contractAddress,
              from: walletAddress,
              data: encodedData
            });

            // Get current gas price
            const feeData = await provider.getFeeData();
            const gasPrice = feeData.gasPrice || BigInt(20000000000); // 20 gwei fallback

            // Calculate total cost in wei
            const totalCostWei = gasLimit * gasPrice;

            // Convert to ether
            gasEstimate = ethers.formatEther(totalCostWei);
          } catch (gasError: any) {
            errorLog('GasEstimation', `Gas estimation error for ${marketplaceName}`, gasError);
            gasEstimate = null;
          }
        }

        return {
          approved: isApproved,
          approvalFeeEstimate: gasEstimate,
          contractAddress: marketplaceAddress
        };
      } catch (error: any) {
        errorLog('ApprovalCheck', `Approval check error for ${marketplaceName}`, error);
        return {
          approved: false,
          approvalFeeEstimate: null,
          error: error?.message || 'Failed to check approval'
        };
      }
    };

    // Check all marketplaces in parallel
    const [openseaApproval, magicedenApproval, blurApproval] = await Promise.all([
      checkMarketplaceApproval('opensea', marketplaceAddresses.opensea),
      checkMarketplaceApproval('magiceden', marketplaceAddresses.magiceden),
      checkMarketplaceApproval('blur', marketplaceAddresses.blur)
    ]);

    res.status(200).json({
      enforceCreatorFee,
      creatorFees,
      marketplaceApprovals: {
        opensea: openseaApproval,
        magiceden: magicedenApproval,
        blur: blurApproval
      }
    });
  } catch (error: any) {
    errorLog('API', 'Failed to fetch NFT metadata', error instanceof Error ? error : new Error(String(error)));

    // Handle specific error cases
    if (error.message === 'Invalid contract address') {
      return res.status(400).json({
        success: false,
        error: 'Invalid contract address format'
      });
    }

    if (error.response?.status === 404) {
      return res.status(404).json({
        success: false,
        error: 'NFT not found'
      });
    }

    res.status(500).json({
      success: false,
      error: 'Failed to fetch NFT metadata',
      message: error.message
    });
  }
});

// Health check endpoint
app.get('/health', (req, res) => {
  const blurStatus = blurRateLimiter.getStatus();
  const blurRateLimitSummary = {
    totalWallets: blurStatus.totalWallets || 0,
    rateLimitedWallets: blurStatus.rateLimitedWallets || 0,
    healthy: (blurStatus.rateLimitedWallets || 0) === 0
  };

  const healthStatus = {
    status: 'ok',
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    memory: process.memoryUsage(),
    security: {
      headers: {
        helmet: 'enabled',
        hsts: 'enabled (max-age: 31536000)',
        csp: 'enabled',
        xFrameOptions: 'DENY',
        xContentTypeOptions: 'nosniff',
        xssProtection: 'enabled'
      }
    },
    blurRateLimits: blurRateLimitSummary,
    environment: process.env.NODE_ENV || 'development'
  };

  res.status(200).json(healthStatus);
});

// Redis connections health endpoint
app.get('/api/health/redis', async (req, res) => {
  try {
    // Get BullMQ connections (Redis is BullMQ-only now)
    const bullMQConnections = getBullMQConnections();
    const connectionHealth = await checkBullMQConnectionsHealth();

    const allHealthy = Object.values(connectionHealth).every((h: any) => h.healthy);

    // BullMQ-only health status
    const redisHealth = {
      status: allHealthy ? 'healthy' : 'degraded',
      note: 'Redis is BullMQ-only - all non-BullMQ usage migrated to in-memory LRU caches',
      bullMQConnections: {
        total: bullMQConnections.length,
        healthy: Object.values(connectionHealth).filter((h: any) => h.healthy).length,
        connections: bullMQConnections.map(conn => ({
          ...conn,
          healthy: connectionHealth[conn.id]?.healthy || false,
          error: connectionHealth[conn.id]?.error
        }))
      }
    };

    // Determine overall health based on BullMQ connections
    const unhealthyCount = Object.values(connectionHealth).filter((status: any) => !status.healthy).length;
    const isHealthy = allHealthy && unhealthyCount === 0;

    res.status(isHealthy ? 200 : 503).json({
      timestamp: new Date().toISOString(),
      ...redisHealth
    });
  } catch (error) {
    res.status(500).json({
      status: 'error',
      timestamp: new Date().toISOString(),
      error: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});

// Memory snapshot endpoints
app.get('/api/memory-snapshots/latest', async (req, res) => {
  try {
    const latest = await mongoose.model('MemorySnapshot')
      .findOne()
      .sort({ timestamp: -1 })
      .lean();

    res.json({ success: true, data: latest });
  } catch (error) {
    errorLog('MemorySnapshotAPI', 'Failed to fetch latest snapshot', error as Error);
    res.status(500).json({
      success: false,
      error: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});

app.get('/api/memory-snapshots', async (req, res) => {
  try {
    const { limit = 10, offset = 0 } = req.query;

    const snapshots = await mongoose.model('MemorySnapshot')
      .find()
      .sort({ timestamp: -1 })
      .skip(Number(offset))
      .limit(Number(limit))
      .lean();

    const total = await mongoose.model('MemorySnapshot').countDocuments();

    res.json({
      success: true,
      data: snapshots,
      pagination: {
        total,
        limit: Number(limit),
        offset: Number(offset),
        hasMore: Number(offset) + Number(limit) < total
      }
    });
  } catch (error) {
    errorLog('MemorySnapshotAPI', 'Failed to fetch snapshots', error as Error);
    res.status(500).json({
      success: false,
      error: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});

app.post('/api/memory-snapshots/trigger', async (req, res) => {
  try {
    const snapshot = await memorySnapshotService.takeSnapshot();
    res.json({ success: true, data: snapshot });
  } catch (error) {
    errorLog('MemorySnapshotAPI', 'Manual snapshot failed', error as Error);
    res.status(500).json({
      success: false,
      error: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});

// Worker health endpoint
app.get('/api/health/workers', async (req, res) => {
  try {
    // Check worker status
    const workerStatuses: Array<{
      index: number;
      name: string;
      running: boolean;
      paused: boolean;
      concurrency: number;
      healthy: boolean;
      error?: string;
    }> = [];

    // Check all workers
    workers.forEach((worker, index) => {
      try {
        workerStatuses.push({
          index,
          name: `worker-${index}`,
          running: worker.isRunning(),
          paused: worker.isPaused(),
          concurrency: worker.concurrency,
          healthy: worker.isRunning() && !worker.isPaused()
        });
      } catch (error) {
        workerStatuses.push({
          index,
          name: `worker-${index}`,
          running: false,
          paused: false,
          concurrency: 0,
          healthy: false,
          error: error instanceof Error ? error.message : 'Unknown error'
        });
      }
    });

    // Get queue counts
    const [mainQueueCounts] = await Promise.all([queue.getJobCounts()]);

    const totalWorkers = workers.length;
    const healthyWorkers = workerStatuses.filter(w => w.healthy).length;
    const isHealthy = healthyWorkers === totalWorkers;

    res.status(isHealthy ? 200 : 503).json({
      status: isHealthy ? 'healthy' : 'unhealthy',
      message: isHealthy
        ? `All ${totalWorkers} workers are healthy and connected`
        : `${healthyWorkers} / ${totalWorkers} workers are healthy`,
      timestamp: new Date().toISOString(),
      workers: {
        total: totalWorkers,
        healthy: healthyWorkers,
        details: workerStatuses
      },
      queues: {
        main: mainQueueCounts,
      },
      configuration: {
        workerConcurrency: workers.reduce((sum, w) => sum + w.concurrency, 0),
        totalWorkers
      }
    });
  } catch (error) {
    res.status(500).json({
      status: 'error',
      timestamp: new Date().toISOString(),
      error: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});

// Combined system health endpoint
app.get('/api/health/system', async (req, res) => {
  try {
    // Get all health statuses
    const redisHealth = await checkBullMQConnectionsHealth();

    // Check workers
    const workerHealth: Array<{ running: boolean; paused: boolean }> = [];
    workers.forEach(worker => {
      workerHealth.push({ running: worker.isRunning(), paused: worker.isPaused() });
    });

    const unhealthyRedis = Object.values(redisHealth).filter((s: any) => !s.healthy).length;
    const unhealthyWorkers = workerHealth.filter(w => !w.running || w.paused).length;

    const systemHealth = {
      status: unhealthyRedis === 0 && unhealthyWorkers === 0 ? 'healthy' : 'degraded',
      timestamp: new Date().toISOString(),
      components: {
        redis: {
          healthy: unhealthyRedis === 0,
          unhealthyConnections: unhealthyRedis
        },
        workers: {
          healthy: unhealthyWorkers === 0,
          unhealthyCount: unhealthyWorkers
        },
        mongodb: {
          connected: mongoose.connection.readyState === 1
        }
      },
      caches: {
        decryptedKeyCache: getDecryptedKeyCacheStats(),
        tokenTraits: getTokenTraitCacheStats(),
        logs: getLogsCacheStats(),
        marketplaceApprovals: getMarketplaceApprovalCacheStats()
      },
      uptime: process.uptime(),
      memory: process.memoryUsage()
    };

    const isHealthy = systemHealth.status === 'healthy';
    res.status(isHealthy ? 200 : 503).json(systemHealth);
  } catch (error) {
    res.status(500).json({
      status: 'error',
      timestamp: new Date().toISOString(),
      error: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});

// Memory stats endpoint for real-time monitoring (crash debugging)
app.get('/api/memory-stats', (req, res) => {
  try {
    const memoryStats = getMemoryStats();
    res.status(200).json({
      ...memoryStats,
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    res.status(500).json({
      status: 'error',
      timestamp: new Date().toISOString(),
      error: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});

// Latency statistics API
app.get('/api/latency-stats', (req, res) => {
  try {
    // Get memory stats with V8 heap limit
    const memUsage = process.memoryUsage();
    const v8HeapStats = v8.getHeapStatistics();
    const heapUsedMB = Math.round(memUsage.heapUsed / 1024 / 1024);
    const heapTotalMB = Math.round(memUsage.heapTotal / 1024 / 1024);
    const heapLimitMB = Math.round(v8HeapStats.heap_size_limit / 1024 / 1024);
    const heapPercent = Math.round((memUsage.heapUsed / v8HeapStats.heap_size_limit) * 100);
    const externalMB = Math.round(memUsage.external / 1024 / 1024);
    const rssMB = Math.round(memUsage.rss / 1024 / 1024);

    res.json({
      timestamp: Date.now(),
      forwardingLatency: {
        opensea: getForwardingStats('opensea'),
        blur: getForwardingStats('blur'),
        magiceden: getForwardingStats('magiceden'),
        overall: getForwardingStats()
      },
      executionLatency: getAllExecutionStats(),
      proactiveGC: getProactiveGCStats(),
      gcStats: getGCStats(),
      memory: {
        heapUsedMB,
        heapTotalMB,
        heapLimitMB,
        heapPercent,
        externalMB,
        rssMB,
        gcEfficiency: {
          proactiveTriggersPerHour: getProactiveGCStats().lastHour,
          automaticGCsPerHour: getGCStats().automaticGCs,
          avgMemoryFreedMB: getProactiveGCStats().averageFreedMB
        }
      }
    });
  } catch (error) {
    errorLog('LatencyStats', 'Failed to get latency stats', error);
    res.status(500).json({ error: 'Failed to retrieve latency statistics' });
  }
});

// Memory breakdown API - detailed memory leak detection
app.get('/api/memory-breakdown', async (req, res) => {
  try {
    const breakdown = await memoryBreakdownService.getBreakdown();
    res.json(breakdown);
  } catch (error) {
    errorLog('API', 'Error getting memory breakdown', error);
    res.status(500).json({ error: 'Failed to get memory breakdown' });
  }
});

// Historical latency snapshots API - fetch with pagination and time-range filtering
app.get('/api/latency-snapshots', async (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit) || 100, 1000);
    const offset = Number(req.query.offset) || 0;
    const startDate = req.query.startDate ? new Date(req.query.startDate as string) : undefined;
    const endDate = req.query.endDate ? new Date(req.query.endDate as string) : undefined;

    // Build query with optional time-range filters
    const query: any = {};
    if (startDate || endDate) {
      query.timestamp = {};
      if (startDate) query.timestamp.$gte = startDate;
      if (endDate) query.timestamp.$lte = endDate;
    }

    // Get total count for pagination
    const total = await LatencySnapshot.countDocuments(query);

    // Fetch snapshots with pagination
    const snapshots = await LatencySnapshot.find(query)
      .sort({ timestamp: -1 })
      .skip(offset)
      .limit(limit)
      .lean();

    res.json({
      success: true,
      data: snapshots,
      pagination: {
        total,
        limit,
        offset,
        hasMore: offset + snapshots.length < total
      }
    });
  } catch (error) {
    errorLog('LatencySnapshotAPI', 'Failed to fetch latency snapshots', { error: String(error) });
    res.status(500).json({ success: false, error: 'Failed to retrieve latency snapshots' });
  }
});

// Get most recent latency snapshot
app.get('/api/latency-snapshots/latest', async (req, res) => {
  try {
    const latestSnapshot = await LatencySnapshot.findOne()
      .sort({ timestamp: -1 })
      .lean();

    if (!latestSnapshot) {
      return res.status(404).json({ success: false, error: 'No latency snapshots found' });
    }

    res.json({
      success: true,
      data: latestSnapshot
    });
  } catch (error) {
    errorLog('LatencySnapshotAPI', 'Failed to fetch latest snapshot', { error: String(error) });
    res.status(500).json({ success: false, error: 'Failed to retrieve latest snapshot' });
  }
});

// Get snapshots for specific time range (24h/7d/30d)
app.get('/api/latency-snapshots/range', async (req, res) => {
  try {
    const range = req.query.range as string;

    if (!['24h', '7d', '30d'].includes(range)) {
      return res.status(400).json({ success: false, error: 'Invalid range. Must be 24h, 7d, or 30d' });
    }

    // Calculate time window
    const now = new Date();
    const startDate = new Date(now);

    switch (range) {
      case '24h':
        startDate.setHours(now.getHours() - 24);
        break;
      case '7d':
        startDate.setDate(now.getDate() - 7);
        break;
      case '30d':
        startDate.setDate(now.getDate() - 30);
        break;
    }

    // Fetch snapshots in time range
    const snapshots = await LatencySnapshot.find({
      timestamp: { $gte: startDate }
    })
      .sort({ timestamp: -1 })
      .limit(1000) // Max 1000 data points for UI performance
      .lean();

    res.json({
      success: true,
      data: snapshots.reverse(), // Reverse to chronological order for charts
      range,
      startDate: startDate.toISOString(),
      endDate: now.toISOString()
    });
  } catch (error) {
    errorLog('LatencySnapshotAPI', 'Failed to fetch range snapshots', { error: String(error) });
    res.status(500).json({ success: false, error: 'Failed to retrieve range snapshots' });
  }
});

// Manually trigger snapshot creation (for debugging)
app.post('/api/latency-snapshots/trigger', async (req, res) => {
  try {
    const snapshot = await captureLatencySnapshot();

    if (!snapshot) {
      return res.status(500).json({ success: false, error: 'Failed to capture snapshot' });
    }

    res.json({
      success: true,
      data: snapshot
    });
  } catch (error) {
    errorLog('LatencySnapshotAPI', 'Failed to trigger snapshot', { error: String(error) });
    res.status(500).json({ success: false, error: 'Failed to trigger snapshot' });
  }
});

// Queue monitoring status endpoint
app.get('/api/queue-monitor/status', async (req, res) => {
  try {
    // Get current queue counts
    const mainCounts = await queue.getJobCounts();

    const monitorStatus = {
      mainQueue: {
        waiting: mainCounts.waiting || 0,
        active: mainCounts.active || 0,
        completed: mainCounts.completed || 0,
        failed: mainCounts.failed || 0,
        delayed: mainCounts.delayed || 0,
        prioritized: mainCounts.prioritized || 0
      }
    };

    res.json(monitorStatus);
  } catch (error) {
    errorLog('API', 'Error getting queue monitor status', error);
    res.status(500).json({ error: 'Failed to get queue monitor status' });
  }
});

// Comprehensive queue health endpoint for diagnosing QUEUEING stuck issues
app.get('/api/queue-health', async (req, res) => {
  try {
    const concurrency = Number(RATE_LIMIT);

    // Get current queue counts
    const mainCounts = await queue.getJobCounts();

    // Calculate total capacity
    const totalMainCapacity = concurrency; // Single main worker

    // Get active tasks count for context
    const activeTaskCount = activeTasks.size;

    // Calculate queue health indicators
    const mainQueueBacklog = (mainCounts.waiting || 0) + (mainCounts.delayed || 0);

    // Estimate processing time based on backlog and capacity
    const mainEstimatedProcessingTimeSec = totalMainCapacity > 0 ? Math.ceil(mainQueueBacklog / totalMainCapacity) : 0;

    const queueHealth = {
      timestamp: new Date().toISOString(),
      mainQueue: {
        waiting: mainCounts.waiting || 0,
        active: mainCounts.active || 0,
        delayed: mainCounts.delayed || 0,
        completed: mainCounts.completed || 0,
        failed: mainCounts.failed || 0,
        prioritized: mainCounts.prioritized || 0,
        backlog: mainQueueBacklog,
        estimatedProcessingTimeSec: mainEstimatedProcessingTimeSec
      },
      workers: {
        mainWorkerCount: 1,
        blurWorkerCount: 0,
        concurrency: concurrency,
        totalMainCapacity: totalMainCapacity,
        rateLimit: Number(RATE_LIMIT)
      },
      context: {
        activeTaskCount: activeTaskCount,
        cpuCount: os.cpus().length
      },
      health: {
        mainQueueHealthy: mainQueueBacklog < 5000,
        workersUtilized: (mainCounts.active || 0) > 0,
        recommendation: mainQueueBacklog > 10000
          ? 'HIGH_BACKLOG: Consider stopping some tasks or increasing worker count'
          : mainQueueBacklog > 5000
            ? 'ELEVATED_BACKLOG: Queue is building up, monitor closely'
            : 'HEALTHY: Queue backlog is within normal range'
      }
    };

    res.json(queueHealth);
  } catch (error) {
    errorLog('API', 'Error getting queue health', error);
    res.status(500).json({ error: 'Failed to get queue health' });
  }
});

// Manual queue clear endpoint
app.post('/api/queue-monitor/clear', async (req, res) => {
  try {

    // Clear the queues
    await clearQueueAndRestartJobs();

    res.json({
      success: true,
      message: 'Queue cleared and jobs restarted',
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    errorLog('API', 'Error during manual queue clear', error);
    res.status(500).json({ error: 'Failed to clear queue' });
  }
});

// 408 Error monitoring endpoint
app.get('/api/blur-timeout-metrics', async (req, res) => {
  try {
    const metrics = getTimeoutMetrics();

    // Add alert status based on error rate
    const alertStatus = {
      isAlerting: metrics.total408Errors > 10 && metrics.retrySuccessRate < 50,
      recommendation: metrics.total408Errors > 10 && metrics.retrySuccessRate < 50
        ? 'High 408 error rate detected. Consider reducing concurrent requests or increasing timeouts.'
        : 'System operating normally'
    };

    res.json({
      ...metrics,
      alertStatus,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    errorLog('API', 'Error fetching timeout metrics', error);
    res.status(500).json({
      error: 'Failed to fetch timeout metrics',
      message: error instanceof Error ? error.message : String(error)
    });
  }
});

// Blur rate limiter status endpoint
app.get('/api/blur-rate-limiter/status', async (req, res) => {
  try {
    const walletAddress = req.query.wallet as string | undefined;
    const status = blurRateLimiter.getStatus(walletAddress);

    res.json({
      ...status,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    errorLog('API', 'Error fetching blur rate limiter status', error);
    res.status(500).json({
      error: 'Failed to fetch blur rate limiter status',
      message: error instanceof Error ? error.message : String(error)
    });
  }
});

// Blur completion tracker status endpoint
app.get('/api/blur-completion/status', async (req, res) => {
  try {
    const walletAddress = req.query.wallet as string | undefined;
    const stats = blurCompletionTracker.getStats(walletAddress);

    res.json({
      ...stats,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    errorLog('API', 'Error fetching blur completion stats', error);
    res.status(500).json({
      error: 'Failed to fetch blur completion stats',
      message: error instanceof Error ? error.message : String(error)
    });
  }
});

// Debug endpoint for taskStatus
app.get('/api/debug/task-status/:taskId', (req, res) => {
  const { taskId } = req.params;
  const status = taskStatus[taskId];
  const countdownTimerExists = taskCountdownTimers.has(taskId);


  res.status(200).json({
    taskId,
    status: status || null,
    countdownTimerExists,
    timestamp: new Date().toISOString()
  });
});

// GET /api/task/:taskId/logs - Get logs for task
app.get('/api/task/:taskId/logs', async (req, res) => {
  try {
    const { taskId } = req.params;

    // Validate taskId
    if (!taskId) {
      return res.status(400).json({ error: 'taskId required' });
    }

    // Get logs from cache
    const logs = await logsCache.find(taskId);

    res.json(logs);

  } catch (error) {
    errorLog('API', `Failed to fetch logs for task ${req.params.taskId}`, error);
    res.status(500).json({ error: 'Failed to fetch logs' });
  }
});

// Circuit breaker status endpoint
app.get('/circuit-breaker/status', (req, res) => {
  const status = taskCircuitBreaker.getStatus();
  res.json({
    totalSuspended: status.filter(s => s.status === 'SUSPENDED').length,
    tasks: status
  });
});

// Task diagnostics endpoint - comprehensive task status
app.get('/api/debug/task-diagnostics/:taskId', async (req, res) => {
  try {
    const { taskId } = req.params;

    // Get task
    const task = activeTasks.get(taskId);
    if (!task) {
      return res.status(404).json({ error: 'Task not found' });
    }

    // Get task status
    const status = taskStatus[taskId];

    // Get parent jobs
    const parentJobsInfo: any = {};
    const selectedMarketplaces = task.selectedMarketplaces.map(m => m.toLowerCase());
    const enabledMarketplacesLower = enabledMarketplaces.map(m => m.toLowerCase());
    const marketplacesToCheck = selectedMarketplaces.filter(m => enabledMarketplacesLower.includes(m));

    for (const marketplace of marketplacesToCheck) {
      const parentJobId = `${taskId}-${marketplace}`;
      const parentJob = parentJobs.get(parentJobId);
      const processedJobs = processedJobsPerParent.get(parentJobId);

      parentJobsInfo[marketplace] = {
        parentJobId,
        exists: !!parentJob,
        totalJobs: parentJob?.totalJobs || 0,
        completedJobs: parentJob?.completedJobs || 0,
        percentage: parentJob && parentJob.totalJobs > 0
          ? ((parentJob.completedJobs / parentJob.totalJobs) * 100).toFixed(2)
          : '0.00',
        processedJobsCount: processedJobs?.size || 0,
        startTime: parentJob?.startTime ? new Date(parentJob.startTime).toISOString() : null
      };
    }

    // Check various states
    const allParentJobsComplete = checkAllParentJobsComplete(taskId);
    const isScheduled = tasksBeingScheduled.has(taskId);
    const loopCount = loopCounterStats[taskId] || 1;
    const hasCountdownTimer = taskCountdownTimers.has(taskId);
    const hasSchedulingLock = loopSchedulingLocks.has(taskId);
    const completionFirstSeen = taskCompletionTracking.get(taskId);

    // Get preprocessing data
    const preprocessing = taskPreprocessing.get(taskId);

    res.json({
      taskId,
      running: task.running,
      status: status || { status: 'NO_STATUS' },
      parentJobs: parentJobsInfo,
      diagnostics: {
        allParentJobsComplete,
        isBeingScheduled: isScheduled,
        loopCount,
        hasCountdownTimer,
        hasSchedulingLock,
        completionFirstSeenAt: completionFirstSeen ? new Date(completionFirstSeen).toISOString() : null,
        timeSinceCompletion: completionFirstSeen ? Math.round((Date.now() - completionFirstSeen) / 1000) + 's' : null,
        hasPreprocessingData: !!preprocessing,
        selectedMarketplaces: task.selectedMarketplaces,
        enabledMarketplaces: marketplacesToCheck
      },
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    errorLog('TaskDiagnostics', 'Error in task diagnostics', error);
    res.status(500).json({ error: 'Failed to get task diagnostics' });
  }
});

// Force task completion endpoint
app.post('/api/debug/force-complete/:taskId', async (req, res) => {
  try {
    const { taskId } = req.params;

    const task = activeTasks.get(taskId);
    if (!task) {
      return res.status(404).json({ error: 'Task not found' });
    }

    if (!task.running) {
      return res.status(400).json({ error: 'Task is not running' });
    }


    // Update status for all marketplaces
    if (taskStatus[taskId]) {
      const task = activeTasks.get(taskId);
      const selectedMarketplaces = task?.selectedMarketplaces || [];

      // Set each marketplace to COMPLETED with 100% running percentage
      for (const mp of selectedMarketplaces) {
        const mpLower = mp.toLowerCase();
        setTaskStatus(taskId, 'COMPLETED', 'Manually forced to complete', mpLower);

        const mpKey = mpLower as 'opensea' | 'magiceden' | 'blur';
        if (taskStatus[taskId].marketplaces[mpKey]) {
          taskStatus[taskId].marketplaces[mpKey]!.runningPercentage = 100;
        }
      }
    }

    // Trigger completion for each marketplace
    const selectedMarketplaces = task?.selectedMarketplaces || [];
    for (const mp of selectedMarketplaces) {
      await checkTaskCompletionAndScheduleNext(taskId, mp.toLowerCase());
    }

    res.json({
      success: true,
      message: `Task ${taskId} forced to complete`,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    errorLog('TaskCompletion', 'Error forcing task completion', error);
    res.status(500).json({ error: 'Failed to force task completion' });
  }
});

// Recalculate task percentage endpoint
app.post('/api/debug/recalculate-percentage/:taskId', async (req, res) => {
  try {
    const { taskId } = req.params;

    const task = activeTasks.get(taskId);
    if (!task) {
      return res.status(404).json({ error: 'Task not found' });
    }


    // Get before state
    const beforeStatus = { ...taskStatus[taskId] };

    // Recalculate
    await updateTaskRunningPercentage(taskId);

    // Get after state
    const afterStatus = { ...taskStatus[taskId] };

    res.json({
      success: true,
      taskId,
      before: {
        marketplaces: beforeStatus?.marketplaces || {}
      },
      after: {
        marketplaces: afterStatus?.marketplaces || {}
      },
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    errorLog('PercentageCalculation', 'Error recalculating percentage', error);
    res.status(500).json({ error: 'Failed to recalculate percentage' });
  }
});

// Get all task statuses endpoint
app.get('/api/debug/all-task-statuses', (req, res) => {
  try {
    const allStatuses: any[] = [];

    activeTasks.forEach((task, taskId) => {
      const status = taskStatus[taskId];
      const parentJobsInfo: any = {};

      // Get parent job info
      const marketplaces = ['opensea', 'magiceden', 'blur'];
      for (const marketplace of marketplaces) {
        const parentJobId = `${taskId}-${marketplace}`;
        const parentJob = parentJobs.get(parentJobId);

        if (parentJob) {
          parentJobsInfo[marketplace] = {
            total: parentJob.totalJobs,
            completed: parentJob.completedJobs,
            percentage: parentJob.totalJobs > 0
              ? ((parentJob.completedJobs / parentJob.totalJobs) * 100).toFixed(2)
              : '0.00'
          };
        }
      }

      // Calculate aggregate values from marketplace data
      const marketplaceStatuses = status?.marketplaces || {};
      const totalJobs = Object.values(marketplaceStatuses).reduce((sum, mp) => sum + (mp?.totalJobs || 0), 0);
      const completedJobs = Object.values(marketplaceStatuses).reduce((sum, mp) => sum + (mp?.completedJobs || 0), 0);
      const percentage = totalJobs > 0 ? (completedJobs / totalJobs * 100) : 0;

      allStatuses.push({
        taskId,
        slug: task.contract?.slug || 'unknown',
        running: task.running,
        marketplaceStatuses,
        percentage: percentage.toFixed(2),
        totalJobs,
        completedJobs,
        parentJobs: parentJobsInfo,
        hasIssues: totalJobs > 0 && completedJobs === totalJobs && percentage < 100
      });
    });

    // Sort by percentage (ascending) to show stuck tasks first
    allStatuses.sort((a, b) => {
      const aPerc = parseFloat(a.percentage);
      const bPerc = parseFloat(b.percentage);
      return aPerc - bPerc;
    });

    res.json({
      totalTasks: allStatuses.length,
      runningTasks: allStatuses.filter(s => s.running).length,
      stuckTasks: allStatuses.filter(s => s.hasIssues).length,
      tasks: allStatuses,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    errorLog('TaskStatus', 'Error getting all task statuses', error);
    res.status(500).json({ error: 'Failed to get task statuses' });
  }
});

// Message loss detection endpoint - compare WS received vs own bid counter
app.get('/api/message-loss-check', (req, res) => {
  const wsReceived = wsManager?.getOpenseaBidCreatedReceived?.() || 0;
  const ownBidsCounted = ownBidCounts['opensea'] || 0;
  const gap = wsReceived - ownBidsCounted;
  const lossRate = wsReceived > 0 ? ((gap / wsReceived) * 100).toFixed(2) + '%' : '0%';

  res.json({
    wsReceived,          // Messages received at WebSocket layer
    ownBidsCounted,      // Messages that reached incrementOwnBidCounter
    gap,                 // Difference (messages lost in processing)
    lossRate,            // Percentage of loss
    timestamp: new Date().toISOString()
  });
});

// Enhanced message flow tracking endpoint (tracks from raw WebSocket receipt)
app.get('/api/message-flow-stats', (req, res) => {
  const wsManagerCounter = wsManager?.getOpenseaBidCreatedReceived?.() || 0;
  const ownBidsCounted = ownBidCounts['opensea'] || 0;

  res.json({
    checkpoints: {
      rawWsMessages: rawWsMessagesReceived,           // ALL WebSocket messages received
      rawOurBids: rawOurBidsReceived,                 // Our bids at earliest checkpoint (before filtering)
      wsManagerCounter,                               // OpenSea BID_CREATED counter in wsManager
      ownBidsCounted,                                 // Final count in incrementOwnBidCounter
    },
    analysis: {
      lossBeforeHandler: rawOurBidsReceived - ownBidsCounted,
      lossPercentage: rawOurBidsReceived > 0
        ? ((1 - ownBidsCounted / rawOurBidsReceived) * 100).toFixed(2) + '%'
        : 'N/A'
    },
    timestamp: new Date().toISOString()
  });
});

// Bid flow checkpoints endpoint - tracks bid events through the pipeline
app.get('/api/bid-flow-checkpoints', (req, res) => {
  res.json({
    wsMessagesTotal: rawWsMessagesReceived,
    bidEventsReceived,
    bidEventsReachingHandler,
    noMarketplaceMatch: noMarketplaceMatchCounter,
    filteredReasons: filteredCounters,
    ownBidsCounted: {
      opensea: ownBidCounts['opensea'] || 0,
      magiceden: ownBidCounts['magiceden'] || 0,
      blur: ownBidCounts['blur'] || 0,
    },
    ownBidsDeduplicated: {
      opensea: ownBidsDeduplicated['opensea'] || 0,
      magiceden: ownBidsDeduplicated['magiceden'] || 0,
      blur: ownBidsDeduplicated['blur'] || 0,
    },
    gaps: {
      wsToHandler: bidEventsReceived - bidEventsReachingHandler,
    },
    timestamp: new Date().toISOString()
  });
});

// Deduplication cache stats endpoint
app.get('/api/dedup-cache-stats', (req, res) => {
  res.json({
    seenOrderHashes: {
      size: seenOrderHashes.size,
      capacity: 100_000,
      ttl: '60 seconds',
      type: 'LRUCache (task-specific: orderHash-taskId)',
      estimatedMemoryBytes: seenOrderHashes.size * 80
    },
    processedOwnBids: {
      size: processedOwnBids.size,
      capacity: 100_000,
      ttl: '60 seconds',
      type: 'LRUCache (final counter dedup: orderHash only)',
      estimatedMemoryBytes: processedOwnBids.size * 80
    }
  });
});

// Failed task retry queue visibility endpoint
app.get('/api/failed-task-retries', (req, res) => {
  const now = Date.now();

  const startEntries = Array.from(failedTaskStartQueue.entries()).map(([taskId, entry]) => ({
    taskId,
    slug: entry.task.contract?.slug,
    type: 'start',
    retryCount: entry.retryCount,
    nextRetryAt: new Date(entry.nextRetryAt).toISOString(),
    nextRetryInMs: Math.max(0, entry.nextRetryAt - now),
    lastError: entry.lastError
  }));

  const stopEntries = Array.from(failedTaskStopQueue.entries()).map(([taskId, entry]) => ({
    taskId,
    slug: entry.task.contract?.slug,
    type: 'stop',
    retryCount: entry.retryCount,
    nextRetryAt: new Date(entry.nextRetryAt).toISOString(),
    nextRetryInMs: Math.max(0, entry.nextRetryAt - now),
    lastError: entry.lastError
  }));

  res.json({
    pendingStartRetries: startEntries.length,
    pendingStopRetries: stopEntries.length,
    tasks: [...startEntries, ...stopEntries],
    timestamp: new Date().toISOString()
  });
});


// Debug endpoint for task scheduling state
app.get('/debug/scheduling', (req, res) => {
  const now = Date.now();

  // Get all lock states
  const lockStates: any[] = [];
  loopSchedulingLocks.forEach((lock, taskId) => {
    lockStates.push({
      taskId,
      locked: lock.locked,
      age: Math.round((now - lock.timestamp) / 1000),
      timestamp: new Date(lock.timestamp).toISOString()
    });
  });

  // Get loop counters
  const loopCounters: any[] = [];
  Object.entries(loopCounterStats).forEach(([taskId, count]) => {
    loopCounters.push({
      taskId,
      loopCount: count
    });
  });

  // Get scheduling flags
  const schedulingFlags: any[] = [];
  tasksBeingScheduled.forEach((value, taskId) => {
    schedulingFlags.push({
      taskId,
      beingScheduled: value
    });
  });

  // Get active tasks
  const activeTasksList: any[] = [];
  activeTasks.forEach((task, taskId) => {
    // Determine overall status from marketplace statuses
    const marketplaceStatuses = taskStatus[taskId]?.marketplaces || {};
    let overallStatus = 'unknown';

    // Prioritize status (RUNNING > PROCESSING > CANCELLING > SCHEDULED > COMPLETED > STOPPED)
    if (Object.values(marketplaceStatuses).some(mp => mp?.status === 'RUNNING')) {
      overallStatus = 'RUNNING';
    } else if (Object.values(marketplaceStatuses).some(mp => mp?.status === 'PROCESSING')) {
      overallStatus = 'PROCESSING';
    } else if (Object.values(marketplaceStatuses).some(mp => mp?.status === 'CANCELLING')) {
      overallStatus = 'CANCELLING';
    } else if (Object.values(marketplaceStatuses).some(mp => mp?.status === 'SCHEDULED')) {
      overallStatus = 'SCHEDULED';
    } else if (Object.values(marketplaceStatuses).every(mp => mp?.status === 'COMPLETED')) {
      overallStatus = 'COMPLETED';
    } else if (Object.values(marketplaceStatuses).some(mp => mp?.status === 'STOPPED')) {
      overallStatus = 'STOPPED';
    }

    activeTasksList.push({
      taskId,
      running: task.running,
      loopInterval: task.loopInterval,
      status: overallStatus,
      marketplaceStatuses
    });
  });

  res.json({
    timestamp: new Date().toISOString(),
    lockStates,
    loopCounters,
    schedulingFlags,
    activeTasks: activeTasksList,
    summary: {
      totalLocks: lockStates.length,
      activeLocks: lockStates.filter(l => l.locked).length,
      staleLocks: lockStates.filter(l => l.locked && l.age > 300).length,
      totalLoopCounters: loopCounters.length,
      totalSchedulingFlags: schedulingFlags.length,
      totalActiveTasks: activeTasksList.length,
      runningTasks: activeTasksList.filter(t => t.running).length
    }
  });
});

// Helper function to format bytes
function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

// ============================================
// LRU CACHE SAMPLER ENDPOINTS
// ============================================

// Sample all LRU caches
// GET /api/debug/sample-caches
app.get('/api/debug/sample-caches', async (req, res) => {
  try {
    const { sampleAllCaches, getCacheStatistics } = await import('./utils/lru-cache-sampler');

    // Trigger sampling (logs to debug-logs/system-YYYY-MM-DD.log)
    sampleAllCaches();

    // Get statistics for response
    const stats = getCacheStatistics();

    res.json({
      success: true,
      message: 'Cache sampling complete. Check debug-logs/system-YYYY-MM-DD.log for detailed output.',
      statistics: {
        totalCaches: stats.total,
        totalMaxCapacity: stats.totalMaxCapacity.toLocaleString(),
        totalCurrentSize: stats.totalCurrentSize.toLocaleString(),
        fillRate: `${stats.fillRate.toFixed(2)}%`,
        byCategory: stats.byCategory
      }
    });
  } catch (error) {
    errorLog('API', 'Failed to sample caches', error);
    res.status(500).json({ error: 'Failed to sample caches' });
  }
});

// Sample caches by category
// GET /api/debug/sample-caches/category/:category
app.get('/api/debug/sample-caches/category/:category', async (req, res) => {
  try {
    const { category } = req.params;
    const validCategories = ['dedup', 'marketplace', 'security', 'monitoring', 'error', 'trait', 'other'];

    if (!validCategories.includes(category)) {
      return res.status(400).json({
        error: `Invalid category. Must be one of: ${validCategories.join(', ')}`
      });
    }

    const { sampleCachesByCategory } = await import('./utils/lru-cache-sampler');

    // Trigger sampling (logs to debug-logs/system-YYYY-MM-DD.log)
    sampleCachesByCategory(category as any);

    res.json({
      success: true,
      message: `Sampled caches in category '${category}'. Check debug-logs/system-YYYY-MM-DD.log for detailed output.`,
      category
    });
  } catch (error) {
    errorLog('API', 'Failed to sample caches by category', error);
    res.status(500).json({ error: 'Failed to sample caches by category' });
  }
});

// Sample a specific cache
// GET /api/debug/sample-caches/cache/:cacheName
app.get('/api/debug/sample-caches/cache/:cacheName', async (req, res) => {
  try {
    const { cacheName } = req.params;
    const { sampleCache } = await import('./utils/lru-cache-sampler');

    // Trigger sampling (logs to debug-logs/system-YYYY-MM-DD.log)
    sampleCache(cacheName);

    res.json({
      success: true,
      message: `Sampled cache '${cacheName}'. Check debug-logs/system-YYYY-MM-DD.log for detailed output.`,
      cacheName
    });
  } catch (error) {
    errorLog('API', 'Failed to sample cache', error);
    res.status(500).json({ error: 'Failed to sample cache' });
  }
});

// Get cache statistics only (no sampling)
// GET /api/debug/cache-statistics
app.get('/api/debug/cache-statistics', async (req, res) => {
  try {
    const { getCacheStatistics } = await import('./utils/lru-cache-sampler');
    const stats = getCacheStatistics();

    res.json({
      success: true,
      statistics: {
        totalCaches: stats.total,
        totalMaxCapacity: stats.totalMaxCapacity.toLocaleString(),
        totalCurrentSize: stats.totalCurrentSize.toLocaleString(),
        fillRate: `${stats.fillRate.toFixed(2)}%`,
        byCategory: stats.byCategory
      }
    });
  } catch (error) {
    errorLog('API', 'Failed to get cache statistics', error);
    res.status(500).json({ error: 'Failed to get cache statistics' });
  }
});

// List all debug log files with metadata
app.get('/api/debug-logs/list', async (_req, res) => {
  try {
    const logDir = path.resolve(process.cwd(), 'debug-logs');

    if (!fs.existsSync(logDir)) {
      return res.json({ files: [], message: 'Debug logs directory not found' });
    }

    const files = fs.readdirSync(logDir)
      .filter(file => file.endsWith('.log'))
      .map(file => {
        const filePath = path.join(logDir, file);
        const stats = fs.statSync(filePath);
        return {
          name: file,
          size: stats.size,
          sizeFormatted: formatBytes(stats.size),
          modified: stats.mtime.toISOString(),
          category: file.split('-')[0] // e.g., 'bidflow', 'counterbid', etc.
        };
      })
      .sort((a, b) => new Date(b.modified).getTime() - new Date(a.modified).getTime());

    res.json({ files, totalSize: formatBytes(files.reduce((acc, f) => acc + f.size, 0)) });
  } catch (error) {
    errorLog('API', 'Failed to list debug logs', error);
    res.status(500).json({ error: 'Failed to list debug logs' });
  }
});

// Download a single debug log file
app.get('/api/debug-logs/download/:filename', async (req, res) => {
  try {
    const { filename } = req.params;
    const logDir = path.resolve(process.cwd(), 'debug-logs');

    // Security: Prevent path traversal
    const safeName = path.basename(filename);
    if (safeName !== filename || !filename.endsWith('.log')) {
      return res.status(400).json({ error: 'Invalid filename' });
    }

    const filePath = path.join(logDir, safeName);

    if (!fs.existsSync(filePath)) {
      return res.status(404).json({ error: 'File not found' });
    }

    res.setHeader('Content-Type', 'text/plain');
    res.setHeader('Content-Disposition', `attachment; filename="${safeName}"`);

    const fileStream = fs.createReadStream(filePath);
    fileStream.pipe(res);
  } catch (error) {
    errorLog('API', 'Failed to download debug log file', error);
    res.status(500).json({ error: 'Failed to download file' });
  }
});

// Download debug logs as ZIP (supports file selection via query param)
// GET /api/debug-logs/download?files=file1.log,file2.log - Download selected files
// GET /api/debug-logs/download - Download all files (backward compatible)
app.get('/api/debug-logs/download', async (req, res) => {
  try {
    const logDir = path.resolve(process.cwd(), 'debug-logs');
    const filesParam = req.query.files as string | undefined;

    if (!fs.existsSync(logDir)) {
      return res.status(404).json({ error: 'Debug logs directory not found' });
    }

    // Get all log files
    const allFiles = fs.readdirSync(logDir).filter(f => f.endsWith('.log'));

    if (allFiles.length === 0) {
      return res.status(404).json({ error: 'No log files found in directory' });
    }

    // Filter to selected files if specified
    let filesToInclude = allFiles;
    if (filesParam) {
      const selectedFiles = filesParam.split(',').map(f => path.basename(f.trim()));
      filesToInclude = allFiles.filter(f => selectedFiles.includes(f));

      if (filesToInclude.length === 0) {
        return res.status(404).json({ error: 'No matching files found' });
      }
    }

    const dateStamp = new Date().toISOString().split('T')[0];
    const filename = `debug-logs-${dateStamp}.zip`;

    res.setHeader('Content-Type', 'application/zip');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);

    const archive = archiver('zip', { zlib: { level: 5 } });

    archive.on('error', (err) => {
      errorLog('API', `Archive error: ${err.message}`, err);
      if (!res.headersSent) {
        res.status(500).json({ error: 'Failed to create archive' });
      }
    });

    archive.pipe(res);

    // Add selected files explicitly (fixes empty ZIP issue)
    for (const file of filesToInclude) {
      archive.file(path.join(logDir, file), { name: file });
    }

    await archive.finalize();

  } catch (error) {
    errorLog('API', `Failed to download debug logs`, error);
    if (!res.headersSent) {
      res.status(500).json({ error: 'Failed to download debug logs' });
    }
  }
});

// Legacy endpoint - redirect to new endpoint
app.get('/api/logs/export', async (_req, res) => {
  return res.status(301).json({
    error: 'Endpoint moved',
    message: 'This endpoint has been moved to /api/debug-logs/export',
    newEndpoint: '/api/debug-logs/export'
  });
});

// Generic marketplace logs with filtering + pagination + summary
app.get('/api/logs', async (req, res) => {
  try {
    const {
      marketplace = 'blur',
      taskId,
      timeRange,
      startTime,
      endTime,
      level = 'all',
      function: functionFilter,
      limit = 1000,
      offset = 0
    } = req.query as any;

    // Validate marketplace
    const validMarketplaces = ['blur', 'opensea', 'magiceden'];
    if (!validMarketplaces.includes(marketplace)) {
      return res.status(400).json({
        error: 'Invalid marketplace',
        message: `Marketplace must be one of: ${validMarketplaces.join(', ')}`
      });
    }

    // Validate level
    const validLevels = ['all', 'info', 'warn', 'error', 'debug'];
    if (!validLevels.includes(level)) {
      return res.status(400).json({
        error: 'Invalid level',
        message: `Level must be one of: ${validLevels.join(', ')}`
      });
    }

    // Validate pagination parameters
    const parsedLimit = Number(limit);
    const parsedOffset = Number(offset);

    if (isNaN(parsedLimit) || parsedLimit < 1 || parsedLimit > 10000) {
      return res.status(400).json({
        error: 'Invalid limit',
        message: 'Limit must be a number between 1 and 10000'
      });
    }

    if (isNaN(parsedOffset) || parsedOffset < 0) {
      return res.status(400).json({
        error: 'Invalid offset',
        message: 'Offset must be a non-negative number'
      });
    }

    // Validate time range
    const validTimeRanges = ['5m', '10m', '1h', '24h', 'custom'];
    if (timeRange && !validTimeRanges.includes(timeRange)) {
      return res.status(400).json({
        error: 'Invalid time range',
        message: `Time range must be one of: ${validTimeRanges.join(', ')}`
      });
    }

    // Validate dates
    let parsedStartTime: Date | undefined;
    let parsedEndTime: Date | undefined;

    if (startTime) {
      parsedStartTime = new Date(String(startTime));
      if (isNaN(parsedStartTime.getTime())) {
        return res.status(400).json({
          error: 'Invalid start time',
          message: 'Start time must be a valid ISO date string'
        });
      }
    }

    if (endTime) {
      parsedEndTime = new Date(String(endTime));
      if (isNaN(parsedEndTime.getTime())) {
        return res.status(400).json({
          error: 'Invalid end time',
          message: 'End time must be a valid ISO date string'
        });
      }
    }

    // Validate custom time range
    if (timeRange === 'custom' && !parsedStartTime) {
      return res.status(400).json({
        error: 'Missing start time',
        message: 'Start time is required when using custom time range'
      });
    }

    // Marketplace logger has been removed - this endpoint is no longer available
    return res.status(501).json({
      error: 'Endpoint no longer available',
      message: 'Marketplace log retrieval has been deprecated. Please use file-based logging instead.'
    });
  } catch (error) {
    errorLog('MarketplaceLogs', 'Error fetching marketplace logs', error);
    const errorMessage = error instanceof Error ? error.message : 'Unknown error';
    res.status(500).json({
      error: 'Failed to fetch logs',
      message: errorMessage,
      details: process.env.NODE_ENV === 'development' ? error : undefined
    });
  }
});

// ==========================================
// BID TRACE API ENDPOINTS
// ==========================================

// Query bid traces with filtering
app.get('/api/bid-traces', async (req, res) => {
  res.status(501).json({
    error: 'Endpoint no longer available',
    message: 'Bid trace logging has been removed. Use debug logs instead.'
  });
});

// Export bid traces to JSON or CSV
app.get('/api/bid-traces/export', async (req, res) => {
  res.status(501).json({
    error: 'Endpoint no longer available',
    message: 'Bid trace logging has been removed. Use debug logs instead.'
  });
});

// Find duplicate bids for a specific token
app.get('/api/bid-traces/duplicates', async (req, res) => {
  res.status(501).json({
    error: 'Endpoint no longer available',
    message: 'Bid trace logging has been removed. Use debug logs instead.'
  });
});


export async function fetchAddressBalances(walletAddress: string): Promise<AddressBalancesResponse> {
  // Check LRU cache first
  const normalizedAddress = walletAddress.toLowerCase();
  const cachedData = addressBalancesLRU.get(normalizedAddress);
  if (cachedData) {
    return cachedData;
  }

  // No distributed lock needed for in-memory cache - fetch fresh data
  const query = {
    operationName: "BalancesQuery",
    query: `query BalancesQuery($address: Address!) {
      addressBalances(address: $address) {
        ...ContextBalance
        __typename
      }
    }
    fragment ContextBalance on Balance {
      id
      currency {
        id
        contractAddress
        chain {
          identifier
          minOfferPriceUsd
          __typename
        }
        usdPrice
        symbol
        imageUrl
        name
        decimals
        ...ContextCurrency
        __typename
      }
      quantity
      usdValue
      __typename
    }
    fragment ContextCurrency on Currency {
      contractAddress
      usdPrice
      stats {
        marketCapUsd
        oneDay {
          priceChange
          __typename
        }
        __typename
      }
      symbol
      decimals
      name
      imageUrl
      chain {
        identifier
        __typename
      }
      isSwapDisabled
      __typename
    }`,
    variables: {
      address: walletAddress
    }
  };

  try {
    const { data } = await apiDedup(
      `opensea-balances-${normalizedAddress}`,
      () => limiter.schedule({ priority: 0 }, () => axiosInstance.post<AddressBalancesResponse>(
        `${BASE_URL}/opensea/graphql`,
        query,
        {
          headers: {
            'content-type': 'application/json',
            'x-api-key': API_KEY
          }
        }
      )),
      { category: 'AddressBalances' }
    );

    // Cache the result in LRU (30 second TTL set in cache definition)
    addressBalancesLRU.set(normalizedAddress, data);

    return data;
  } catch (error) {
    errorLog('OpenSea', `[fetchAddressBalances] API_ERROR | status=${(error as any)?.response?.status} | url=${BASE_URL}/opensea/graphql | wallet=${walletAddress.slice(0, 10)}...`, (error as any)?.response?.data);
    throw error;
  }
}


export async function fetchBlurTraits(
  contractAddress: string
): Promise<IBlurData> {
  const url = `${BASE_URL}/blur/v1/traits/${contractAddress}`;
  try {
    const { data } = await apiDedup(
      `blur-traits-${contractAddress.toLowerCase()}`,
      () => limiter.schedule({ priority: 0 }, () => axiosInstance.get(url, {
        headers: { "x-api-key": API_KEY },
      })),
      { category: 'BlurTraits' }
    );
    const traits = terraformBlurTraits(data.traits);
    return { traits, blurRaw: data };
  } catch (error) {
    return {
      traits: { categories: {}, counts: {} },
      blurRaw: { success: false, traits: {} },
    };
  }
}


export async function fetchMagicEdenAttributes(
  collectionId: string
): Promise<MagicEdenData> {
  const url = `${BASE_URL}/magiceden/v3/rtp/ethereum/collections/${collectionId}/attributes/all/v4`;
  try {
    const { data } = await apiDedup(
      `magiceden-attrs-${collectionId.toLowerCase()}`,
      () => limiter.schedule({ priority: 0 }, () => axiosInstance.get<MagicEdenTraits>(url, {
        headers: {
          accept: "application/json",
          "x-api-key": API_KEY,
        },
      })),
      { category: 'MagicEdenAttrs' }
    );

    const traits = transformMagicEdenTraits(data);
    return { traits, magicedenRaw: data };
  } catch (error) {
    return {
      traits: { categories: {}, counts: {} },
      magicedenRaw: { attributes: [] },
    };
  }
}

const fetchCollectionData = async (slug: string) => {
  const apiUrl: string = `${BASE_URL}/opensea/api/v2/collections/${slug?.toLowerCase()}`;

  const tasks = [
    async () => {
      try {
        const result = await apiDedup(
          `opensea-collection-${slug.toLowerCase()}`,
          () => limiter.schedule({ priority: 0 }, () => axiosInstance.get<CollectionData>(apiUrl, {
            headers: { "x-api-key": API_KEY },
          })),
          { category: 'Collection' }
        );
        return { success: true, data: result, error: null };
      } catch (error) {
        console.error(`[COLLECTION] OpenSea API failed for: ${slug}`, {
          error: error instanceof Error ? error.message : String(error),
          stack: error instanceof Error ? error.stack : undefined
        });
        return { success: false, data: null, error };
      }
    },

    async () => {
      try {
        const result = await apiDedup(
          `magiceden-collection-${slug.toLowerCase()}`,
          () => limiter.schedule({ priority: 0 }, () => axiosInstance.post(
            `${BASE_URL}/magiceden/v4/collections`,
            {
              chain: "ethereum",
              collectionSlugs: [slug],
            },
            {
              headers: { "x-api-key": API_KEY },
            }
          )),
          { category: 'Collection' }
        );
        return { success: true, data: result, error: null };
      } catch (error) {
        console.error(`[COLLECTION] MagicEden API failed for: ${slug}`, {
          error: error instanceof Error ? error.message : String(error),
          stack: error instanceof Error ? error.stack : undefined
        });
        return { success: false, data: null, error };
      }
    },
  ];

  const results = await Promise.allSettled(tasks.map(task => task()));
  const [collectionResult, magicEdenResult] = results.map(r =>
    r.status === 'fulfilled' ? r.value : { success: false, data: null, error: r.status === 'rejected' ? r.reason : 'Unknown error' }
  );

  return { collectionResult, magicEdenResult };
};

export function getExpiry(bidDuration: { value: number; unit: string }) {
  // Handle invalid or zero values
  if (!bidDuration || bidDuration.value < 0) {
    return 900; // Default to 15 minutes
  }

  const expiry = bidDuration.unit === 'minutes'
    ? bidDuration.value * 60
    : bidDuration.unit === 'hours'
      ? bidDuration.value * 3600
      : bidDuration.unit === 'days'
        ? bidDuration.value * 86400
        : 900;

  return expiry
}

/**
 * Extract currency details from task configuration without needing floor price.
 * This avoids calling calculateBidPrice(task, 0) just to get currency info.
 */
function getCurrencyDetailsFromTask(
  task: ITask,
  marketplaceName: "opensea" | "magiceden" | "blur"
): { currencyType: string; currency: string; decimal: number; currencySymbol: string } {
  const isGeneralBidPrice = task.bidPriceType === "GENERAL_BID_PRICE";

  const marketplaceBidPrice = marketplaceName.toLowerCase() === "blur"
    ? task.blurBidPrice
    : marketplaceName.toLowerCase() === "opensea"
      ? task.openseaBidPrice
      : marketplaceName.toLowerCase() === "magiceden"
        ? task.magicEdenBidPrice
        : task.bidPrice;

  let bidPrice = isGeneralBidPrice ? task.bidPrice : marketplaceBidPrice;

  if (task.bidPriceType === "MARKETPLACE_BID_PRICE" && !marketplaceBidPrice) {
    bidPrice = task.bidPrice;
  }

  const currencyType = bidPrice.minType === "percentage" ? "eth" : bidPrice.minType;
  const currency = currencyType === "usdc" ? USDC : WETH;
  const decimal = currency.toLowerCase() === USDC.toLowerCase() ? 6 : 18;
  const currencySymbol = currencyType === "usdc" ? "usdc" : "weth";

  return { currencyType, currency, decimal, currencySymbol: currencySymbol.toUpperCase() };
}

/**
 * Extract minimum bid price from task configuration for balance checking.
 * For percentage-based pricing, returns 0 (any balance above 0 is sufficient for a 0% floor scenario).
 * For absolute pricing (eth/usdc), returns the actual minimum from task config.
 * This avoids calling calculateBidPrice(task, 0) just to get min bid price.
 */
function getMinBidPriceFromTask(
  task: ITask,
  marketplaceName: "opensea" | "magiceden" | "blur"
): number {
  const isGeneralBidPrice = task.bidPriceType === "GENERAL_BID_PRICE";

  const marketplaceBidPrice = marketplaceName.toLowerCase() === "blur"
    ? task.blurBidPrice
    : marketplaceName.toLowerCase() === "opensea"
      ? task.openseaBidPrice
      : marketplaceName.toLowerCase() === "magiceden"
        ? task.magicEdenBidPrice
        : task.bidPrice;

  let bidPrice = isGeneralBidPrice ? task.bidPrice : marketplaceBidPrice;

  if (task.bidPriceType === "MARKETPLACE_BID_PRICE" && !marketplaceBidPrice) {
    bidPrice = task.bidPrice;
  }

  // For percentage-based pricing, return 0 (floor price would be needed for actual calculation)
  // For absolute pricing, return the actual minimum value
  if (bidPrice.maxType === "percentage") {
    return 0;
  }
  return bidPrice.min || task.bidPrice.min || 0;
}

async function calculateBidPrice(task: ITask, floorPrice: number, marketplaceName: "opensea" | "magiceden" | "blur"):
  Promise<{ offerPriceEth: number; maxBidPriceEth: number, minBidPriceEth: number, currencyType: string, currency: string, decimal: number, currencySymbol: string }> {
  const isGeneralBidPrice = task.bidPriceType === "GENERAL_BID_PRICE";

  const marketplaceBidPrice = marketplaceName.toLowerCase() === "blur" ? task.blurBidPrice
    : marketplaceName.toLowerCase() === "opensea" ? task.openseaBidPrice
      : marketplaceName.toLowerCase() === "magiceden" ? task.magicEdenBidPrice
        : task.bidPrice

  let bidPrice = isGeneralBidPrice ? task.bidPrice : marketplaceBidPrice;

  if (task.bidPriceType === "MARKETPLACE_BID_PRICE" && !marketplaceBidPrice) {
    if (!task.bidPrice) throw new Error("No bid price found");
    bidPrice = task.bidPrice;
  }

  let offerPriceEth: number;
  if (bidPrice.minType === "percentage") {
    offerPriceEth = Number((floorPrice * bidPrice.min / 100).toFixed(4));
  } else {
    offerPriceEth = bidPrice.min;
  }

  let maxBidPriceEth: number;
  let minBidPriceEth: number;
  if (bidPrice.maxType === "percentage") {
    maxBidPriceEth = Number((floorPrice * (bidPrice.max || task.bidPrice.max || 0) / 100).toFixed(4));
    minBidPriceEth = Number((floorPrice * (bidPrice.min || task.bidPrice.min || 0) / 100).toFixed(4));
  } else {
    maxBidPriceEth = bidPrice.max || task.bidPrice.max || 0;
    minBidPriceEth = bidPrice.min || task.bidPrice.min || 0;
  }

  // Apply hard cap if enabled
  if (bidPrice.hardCapEnabled && bidPrice.hardCap !== null && bidPrice.hardCap !== undefined && bidPrice.hardCap > 0) {
    // If hard cap is enabled, use the minimum of calculated max price and hard cap
    const hardCapValue = bidPrice.hardCap;
    maxBidPriceEth = Math.min(maxBidPriceEth, hardCapValue);

    // Log if hard cap is limiting the max bid
    const calculatedMax = bidPrice.maxType === "percentage"
      ? (floorPrice * (bidPrice.max || task.bidPrice.max || 0) / 100)
      : (bidPrice.max || task.bidPrice.max || 0);

    if (maxBidPriceEth === hardCapValue && hardCapValue < calculatedMax) {
    }
  }

  // Validate min/max bid prices
  if (maxBidPriceEth > 0 && minBidPriceEth > maxBidPriceEth) {
    await logBidError(task._id, "INVALID BID PRICE CONFIGURATION", `Minimum bid price (${minBidPriceEth} ETH) cannot be greater than maximum bid price (${maxBidPriceEth} ETH)`, "skipped", marketplaceName);
    warningBids[task._id][marketplaceName] = true;
    warningMessage[task._id][marketplaceName] = `Minimum bid price (${minBidPriceEth} ETH) cannot be greater than maximum bid price (${maxBidPriceEth} ETH)`;
  }


  if (marketplaceName.toLowerCase() === "blur") {
    // Use proper rounding to avoid floating-point precision issues
    // Round to 6 decimal places first, then to 2 decimal places
    offerPriceEth = Math.round(Math.round(offerPriceEth * 1000000) / 1000000 * 100) / 100;
    maxBidPriceEth = Math.round(Math.round(maxBidPriceEth * 1000000) / 1000000 * 100) / 100;
    minBidPriceEth = Math.round(Math.round(minBidPriceEth * 1000000) / 1000000 * 100) / 100;
  }
  const currencyType = bidPrice.minType === "percentage" ? "eth" : bidPrice.minType
  const currency = currencyType === "usdc" ? USDC : WETH

  const decimal = currency.toLowerCase() === USDC.toLowerCase() ? 6 : 18
  const currencySymbol = currencyType === "usdc" ? "usdc" : "weth"

  // PriceCalc log removed - duplicate of BidFlow PriceCalc logs

  return { offerPriceEth, maxBidPriceEth, minBidPriceEth, currencyType, currency, decimal, currencySymbol: currencySymbol.toUpperCase() };
}

async function stopOption(
  task: ITask,
  marketplace: 'opensea' | 'magiceden' | 'blur',
  floorPrice: number,
  counterBid: boolean,
  traitFloor?: number,
  trait?: string
): Promise<boolean> {
  // Initialize warning tracking if not exists
  if (!warningBids[task._id]) {
    warningBids[task._id] = {
      opensea: false,
      magiceden: false,
      blur: false
    };
  }

  if (!warningMessage[task._id]) {
    warningMessage[task._id] = {
      opensea: '',
      magiceden: '',
      blur: ''
    };
  }

  if (!skipStats[task._id]) {
    skipStats[task._id] = {
      opensea: 0,
      magiceden: 0,
      blur: 0
    };
  }

  const slug = task.contract?.slug || task._id;
  const taskId = task._id.toString();
  const metadata = { taskId, marketplace, slug };

  try {
    // Early return if stop options aren't enabled
    if (!task?.stopOptions?.triggerStopOptions) {
      return false;
    }

    // Return false if minFloorPrice or maxFloorPrice is 0 or undefined
    const minFloorPrice = Number(task?.stopOptions?.minFloorPrice);
    const maxFloorPrice = Number(task?.stopOptions?.maxFloorPrice);

    // Check for trait prices with marketplace prefix first, then fall back to old format
    const marketplaceTraitKey = `${marketplace}:${trait}`;
    const hasMarketplaceKey = task?.stopOptions?.traitPrices && marketplaceTraitKey in task?.stopOptions?.traitPrices;
    const hasOldKey = task?.stopOptions?.traitPrices && trait && trait in task?.stopOptions?.traitPrices;

    const traitPriceKey = hasMarketplaceKey ? marketplaceTraitKey : (hasOldKey ? trait : null);
    const traitFloorMin = traitPriceKey && task?.stopOptions?.traitPrices ? Number(task?.stopOptions?.traitPrices[traitPriceKey].min) : 0;
    const traitFloorMax = traitPriceKey && task?.stopOptions?.traitPrices ? Number(task?.stopOptions?.traitPrices[traitPriceKey].max) : 0;

    if (trait && traitFloor && traitFloor > 0 && (traitFloorMin > 0 || traitFloorMax > 0)) {
      const belowMin = traitFloorMin > 0 && traitFloor < traitFloorMin;
      const aboveMax = traitFloorMax > 0 && traitFloor > traitFloorMax;
      const outOfRange = belowMin || aboveMax;


      if (outOfRange) {
        const message = `❌ Trait Floor price ${traitFloor} ETH for ${task.contract.slug} [${trait}] is outside the allowed range (${traitFloorMin} ETH - ${traitFloorMax} ETH). Skipping...`.toUpperCase()

        await logBidError(task._id, "TRAIT FLOOR OUT OF RANGE", message, 'skipped', marketplace)

        if (!counterBid) {
          skipStats[task._id][marketplace]++
        }
        return true
      }
    } else {
      // Check collection floor range
      const belowMin = minFloorPrice > 0 && floorPrice < minFloorPrice;
      const aboveMax = maxFloorPrice > 0 && floorPrice > maxFloorPrice;
      const outOfRange = belowMin || aboveMax;

      if (outOfRange) {
        if (!counterBid) {
          skipStats[task._id][marketplace]++;
        }
        return true;
      }
    }

    // Check max purchase limit
    const maxPurchase = task?.stopOptions?.maxPurchase;
    const actualPurchase = task?.stopOptions?.actualPurchase ?? 0;

    const maxPurchaseTriggered = maxPurchase && maxPurchase > 0 && actualPurchase >= maxPurchase;

    if (maxPurchaseTriggered) {
      // Set warning flags and message
      warningBids[task._id][marketplace] = true;
      warningMessage[task._id][marketplace] =
        `Maximum purchase amount (${maxPurchase}) for ${task.contract.slug} has been reached. ` +
        `Current purchases: ${actualPurchase} items.`;

      return true;
    }

    // Clear warnings if all checks pass AND task is still running
    const taskStatus = activeTasks.get(task._id);
    if (taskStatus?.running) {
      warningBids[task._id][marketplace] = false;
      warningMessage[task._id][marketplace] = '';
    }

    return false;

  } catch (error) {
    errorLog('StopOption', `Error in stopOption for ${task?.contract?.slug || 'unknown'} on ${marketplace}`, error);
    // In case of error, don't stop the task to avoid blocking
    return false;
  }
}

function transformBlurTraits(selectedTraits: Record<string, string[]> | undefined): { [key: string]: string }[] {
  if (!selectedTraits) return []

  const result: { [key: string]: string }[] = [];
  for (const [traitType, values] of Object.entries(selectedTraits)) {
    for (const value of values) {
      if (value.includes(',')) {
        const subValues = value.split(',');
        for (const subValue of subValues) {
          result.push({ [traitType]: subValue.trim() });
        }
      } else {
        result.push({ [traitType]: value.trim() });
      }
    }
  }
  return result;
}

// Store bid counts in memory with a Map
const bidCounts = new Map<string, number>();

function getIncrementedBidCount(marketplace: string, slug: string, taskId: string): string {
  const countKey = `${marketplace}:${slug}`;
  const currentCount = bidCounts.get(countKey) || 0;
  const newCount = currentCount + 1;
  bidCounts.set(countKey, newCount);
  return `${taskId}:${newCount}`;
}

function transformOpenseaTraits(selectedTraits: Record<string, string[]>): { type: string; value: string }[] {
  const result: { type: string; value: string }[] = [];
  for (const [traitType, values] of Object.entries(selectedTraits)) {
    for (const value of values) {
      if (value.includes(',')) {
        const subValues = value.split(',');
        for (const subValue of subValues) {
          result.push({ type: traitType, value: subValue.trim() });
        }
      } else {
        result.push({ type: traitType, value: value.trim() });
      }
    }
  }
  return result;
}


let marketplaceIntervals: { [key: string]: NodeJS.Timeout } = {};
const subscribedTasks = new Map<string, Set<string>>(); // Map<taskId, Set<marketplaces>>
const activeSubscriptions = new Set<string>(); // Set of "slug:marketplace" combinations

// Reference count for collection subscriptions: slug -> Set<taskId>
// Only unsubscribe from WebSocket when no more tasks need a collection
const collectionSubscriptionRefCount = new Map<string, Set<string>>();

/**
 * Add a task to the collection subscription reference count
 * @returns true if this is the first task for this collection (should subscribe)
 */
function addTaskToCollectionSubscription(slug: string, taskId: string): boolean {
  if (!collectionSubscriptionRefCount.has(slug)) {
    collectionSubscriptionRefCount.set(slug, new Set());
  }
  const wasEmpty = collectionSubscriptionRefCount.get(slug)!.size === 0;
  collectionSubscriptionRefCount.get(slug)!.add(taskId);
  return wasEmpty;
}

/**
 * Remove a task from the collection subscription reference count
 * @returns true if no more tasks need this collection (should unsubscribe)
 */
function removeTaskFromCollectionSubscription(slug: string, taskId: string): boolean {
  const tasks = collectionSubscriptionRefCount.get(slug);
  if (!tasks) return false;

  tasks.delete(taskId);

  if (tasks.size === 0) {
    collectionSubscriptionRefCount.delete(slug);
    return true; // No more tasks subscribed, safe to unsubscribe
  }

  return false; // Other tasks still need this subscription
}

/**
 * Get count of tasks subscribed to a collection
 */
function getCollectionSubscriptionCount(slug: string): number {
  return collectionSubscriptionRefCount.get(slug)?.size || 0;
}

async function subscribeToCollections(tasks: ITask[]) {
  try {
    // DEBUG: Log subscription attempt
    const slugs = tasks.map(t => t.contract?.slug).filter(Boolean);
    if (DEBUG) {
    }

    // Always subscribe all tasks to WebSocket
    if (!wsManager?.isConnected()) {
      if (DEBUG) {
      }
      return;
    }

    // Clear ping interval if it exists (now handled by WebSocketManager)
    if (marketplaceIntervals['global']) {
      clearInterval(marketplaceIntervals['global']);
      delete marketplaceIntervals['global'];
    }

    // Track which collections we've already subscribed to
    const subscribedCollections = new Set<string>();

    // Handle subscriptions - subscribe all tasks by default
    for (const task of tasks) {
      try {
        const slug = task.contract.slug;
        const taskId = task._id.toString();

        // Skip if already processed this collection in this batch
        if (subscribedCollections.has(slug)) {
          // Still track this task's subscription in ref count even if we've already sent WebSocket message
          addTaskToCollectionSubscription(slug, taskId);
          continue;
        }

        // Track this task's subscription and check if it's the first for this collection
        const isFirstSubscriber = addTaskToCollectionSubscription(slug, taskId);

        // Only send WebSocket subscription if this is the first task for this collection
        if (isFirstSubscriber) {
          // Subscribe via wsManager.subscribe() to properly track in activeSubscriptions
          // This ensures isSubscribedToSlug() returns true for received events
          for (const marketplace of task.selectedMarketplaces) {
            await wsManager.subscribe(
              slug,
              task.contract.contractAddress,
              marketplace.toLowerCase(),
              'bidding'
            );
          }

          if (DEBUG) {
          }
        } else {
          if (DEBUG) {
          }
        }

        subscribedCollections.add(slug);

        // Note: activeSubscriptions tracking is now handled by wsManager.subscribe()
        // No need for manual activeSubscriptions.add() calls here
      } catch (error) {
        errorLog('WebSocketSubscription', `Error subscribing to collection ${task.contract.slug}`, error);
      }
    }

  } catch (error) {
    errorLog('WebSocketSubscription', 'Error subscribing to collections', error);
    // Don't rethrow the error to prevent server crash
  }
}

async function subscribeToListingEvents(contractAddress: string, slug?: string) {
  try {
    if (!wsManager?.isConnected()) {
      return;
    }

    // Get collection info from tasks if slug not provided
    if (!slug) {
      const task = currentTasks.find(t =>
        t.contract.contractAddress?.toLowerCase() === contractAddress.toLowerCase()
      );
      slug = task?.contract.slug || contractAddress; // Use contractAddress as fallback
    }

    // Subscribe to listing events
    wsManager.sendMessage({
      event: 'join_listings',
      clientId: clientId,
      slug: slug,
      contractAddress: contractAddress
    });

  } catch (error) {
    errorLog('ListingSubscription', `Error subscribing to listing events for ${contractAddress}`, error);
  }
}

async function resubscribeToListingCollections() {
  try {

    if (!wsManager?.isConnected()) {
      // Retry after a short delay
      setTimeout(() => {
        resubscribeToListingCollections();
      }, 2000);
      return;
    }

    // Find all active listings with instant price matching enabled
    const activeListings = await Listing.find({
      instantPriceMatching: true,
      running: true
    });


    // Subscribe using the WebSocket manager's subscribe method with proper type
    for (const listing of activeListings) {
      try {
        const contractAddress = listing.contract.contractAddress;
        const contractSlug = listing.contract.slug;

        // Subscribe to all marketplaces for this listing
        for (const marketplace of listing.selectedMarketplaces) {
          await wsManager.subscribe(
            contractSlug,
            contractAddress,
            marketplace.toLowerCase(),
            'listing' // Ensure subscription type is 'listing'
          );
        }

      } catch (error) {
        errorLog('ListingResubscription', `Error re-subscribing to listing ${listing._id}`, error);
      }
    }


    // Check if any listings have been outbid while disconnected
    await processAllActiveListings();
  } catch (error) {
    errorLog('ListingResubscription', 'Error in resubscribeToListingCollections', error);
  }
}

async function processAllActiveListings() {
  try {

    // Find all active listings
    const activeListings = await Listing.find({
      running: true
    });


    // Process each listing
    for (const listing of activeListings) {
      try {
        const listingId = listing._id.toString();

        // Add job to queue
        await queue.add(
          PROCESS_LISTING,
          { listingId },
          {
            jobId: `listing-manual-${listingId}`,
            removeOnComplete: true,
            removeOnFail: true
          }
        );

      } catch (error) {
        errorLog('ListingProcessing', `Error processing listing ${listing._id}`, error);
      }
    }

  } catch (error) {
    errorLog('ListingProcessing', 'Error in processAllActiveListings', error);
  }
}

/**
 * Unsubscribes from WebSocket collection monitoring for a specific task
 * Uses reference counting to only unsubscribe when no other tasks need the collection
 * @param task - The bidding task to unsubscribe from
 * @param marketplace - Optional marketplace filter (opensea, magiceden, blur)
 * @returns Promise with operation result
 */
async function unsubscribeFromCollection(
  task: ITask,
  marketplace?: string
): Promise<{ success: boolean; unsubscribed: boolean; error?: string }> {
  const taskId = task?._id?.toString();
  const contractSlug = task?.contract?.slug;
  const contractAddress = task?.contract?.contractAddress;

  try {
    // Input validation
    if (!task) {
      warnLog('WebSocket', 'Invalid task provided to unsubscribeFromCollection');
      return { success: false, unsubscribed: false, error: 'Invalid task provided' };
    }

    if (!taskId) {
      warnLog('WebSocket', 'Task missing _id property');
      return { success: false, unsubscribed: false, error: 'Task missing _id property' };
    }

    if (!contractSlug) {
      warnLog('WebSocket', 'Task missing contract.slug property');
      return { success: false, unsubscribed: false, error: 'Task missing contract.slug property' };
    }

    if (!contractAddress) {
      warnLog('WebSocket', 'Task missing contract.contractAddress property');
      return { success: false, unsubscribed: false, error: 'Task missing contract.contractAddress property' };
    }

    // Validate marketplace parameter if provided
    if (marketplace && !['opensea', 'magiceden', 'blur'].includes(marketplace.toLowerCase())) {
      warnLog('WebSocket', 'Invalid marketplace parameter');
      return { success: false, unsubscribed: false, error: 'Invalid marketplace parameter' };
    }

    // Check if other tasks still need this subscription using reference counting
    const shouldUnsubscribe = removeTaskFromCollectionSubscription(contractSlug, taskId);
    const remainingCount = getCollectionSubscriptionCount(contractSlug);


    if (!shouldUnsubscribe) {
      if (DEBUG) {
      }
      // Success but didn't actually unsubscribe - other tasks still need this collection
      return { success: true, unsubscribed: false };
    }

    if (DEBUG) {
    }

    // Check WebSocket manager availability
    if (!wsManager) {
      warnLog('WebSocket', 'WebSocket manager not available');
      return { success: false, unsubscribed: false, error: 'WebSocket manager not available' };
    }

    // Enhanced connection check with retry logic
    const maxRetries = 3;
    const retryDelay = 2000; // 2 seconds
    let retryCount = 0;

    while (retryCount < maxRetries) {
      if (wsManager.isConnected()) {
        break;
      }

      // WebSocket not connected, retrying... (removed debug log)

      await new Promise(resolve => setTimeout(resolve, retryDelay));
      retryCount++;
    }

    if (!wsManager.isConnected()) {
      warnLog('WebSocket', 'WebSocket connection failed after retries');
      return { success: false, unsubscribed: false, error: 'WebSocket connection failed' };
    }

    // Perform unsubscription with timeout protection
    const unsubscribeTimeout = 10000; // 10 seconds
    const unsubscribePromise = wsManager.unsubscribe(contractSlug, contractAddress, marketplace);

    const timeoutPromise = new Promise<never>((_, reject) => {
      setTimeout(() => reject(new Error('Unsubscribe operation timed out')), unsubscribeTimeout);
    });

    await Promise.race([unsubscribePromise, timeoutPromise]);

    // Enhanced logging with structured data
    const logContext = {
      taskId,
      contractSlug,
      contractAddress,
      marketplace: marketplace?.toLowerCase(),
      operation: 'websocket_unsubscribe',
      timestamp: new Date().toISOString()
    };

    // Unsubscribed from collection (removed info logs)

    // Clean up local state - Only remove if unsubscribing from all marketplaces
    // Note: subscribedTasks appears to be unused/incomplete in current implementation
    // Consider removing or properly implementing this tracking
    if (!marketplace && subscribedTasks.has(taskId)) {
      subscribedTasks.delete(taskId);
      // Removed task from subscribedTasks (removed debug log)
    }

    return { success: true, unsubscribed: true };
  } catch (error) {
    errorLog('WebSocket', 'Failed to unsubscribe from collection', error instanceof Error ? error : new Error(String(error)));

    return {
      success: false,
      unsubscribed: false,
      error: error instanceof Error ? error.message : String(error)
    };
  }
}


function checkOpenseaTrait(selectedTraits: any, trait_criteria: any) {
  // Null guard to prevent "Cannot read properties of null (reading 'trait_type')" errors
  if (!trait_criteria || !trait_criteria.trait_type) {
    return false;
  }

  if (selectedTraits.hasOwnProperty(trait_criteria.trait_type)) {
    return selectedTraits[trait_criteria.trait_type].includes(trait_criteria.trait_name);
  }
  return false;
}

function checkMagicEdenTrait(selectedTraits: Record<string, string[]>, trait: { attributeKey: string; attributeValue: string }) {
  if (selectedTraits.hasOwnProperty(trait.attributeKey)) {
    return selectedTraits[trait.attributeKey].includes(trait.attributeValue)
  }
  return false;
}

export function decrementBidCount(marketplace: 'opensea' | 'magiceden' | 'blur', taskId: string) {
  if (!bidStats[taskId]) {
    return;
  }
  if (bidStats[taskId][marketplace] > 0) {
    bidStats[taskId][marketplace]--;
  }
}

interface BlurCancelPayload {
  payload: {
    contractAddress: string;
    criteriaPrices: Array<{
      price: string;
      criteria?: {
        type: string;
        value: Record<string, string>;
      }
    }>;
  };
  walletId: string;
  orderKey: string;
  taskId: string;
  identifier: string;
}

function checkBlurTraits(incomingBids: any, traits: any) {
  const traitKeys = traits.flatMap((trait: any) => Object.entries(trait).map(([key, value]) => ({ key, value })));
  return incomingBids.filter((bid: any) => {
    return traitKeys.some((trait: any) => {
      return bid.criteriaValue[trait.key] === trait.value;
    });
  });
}

const approvalLocks = new Map<string, Promise<boolean>>();

// Retry wrapper function
const RETRY_DELAY = 1000; // 1 second
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
async function withRetry<T>(fn: () => Promise<T>, retries = MAX_RETRIES): Promise<T> {
  try {
    return await fn();
  } catch (error: any) {
    if (retries > 0) {
      await delay(RETRY_DELAY);
      return withRetry(fn, retries - 1);
    }
    throw error;
  }
}

/**
 * Approves a marketplace contract to spend WETH on behalf of a wallet
 * @param marketplace The marketplace contract address to approve (e.g. Seaport, MagicEden)
 * @param privateKey The private key of the wallet to approve from
 * @returns Promise<boolean> Whether the approval was successful
 */
async function approveMarketplace(marketplace: string, privateKey: string, currency = WETH): Promise<boolean> {

  // Helper function to delay execution


  try {
    // Set up provider and wallet with retry
    const provider = await withRetry(async () => {
      const p = new ethers.AlchemyProvider('mainnet', ALCHEMY_API_KEY);
      // Test the connection
      await p.getNetwork();
      return p;
    });

    const wallet = new Web3Wallet(privateKey, provider);
    const address = (await wallet.address).toLowerCase();

    // Create unique key for tracking approvals
    const approvalKey = `{${address}}:${marketplace.toLowerCase()}:${currency.toLowerCase()}`;

    // Check if approval already exists in cache
    const marketplaceName = marketplace.toLowerCase() === SEAPORT.toLowerCase() ? 'opensea' : 'magiceden';
    const hasApproval = await marketplaceApprovalCache.findOne(address, currency, marketplaceName);

    if (hasApproval) {
      return true;
    }

    // If approval is already in progress, return existing promise
    if (approvalLocks.has(approvalKey)) {
      return approvalLocks.get(approvalKey)!;
    }

    // Create new approval process and cache it
    const approvalPromise = (async () => {
      try {
        // Create WETH contract instance
        const erc20Contract = new Contract(currency, ERC20_MIN_ABI, wallet);

        // Check if sufficient allowance already exists on blockchain with retry
        const allowance = await withRetry(async () => {
          const result = await erc20Contract.allowance(address, marketplace);
          return result;
        });

        if (allowance >= (MaxUint256 / 2n)) {
          // Update database logic...
          // ... existing database update code ...
          return true;
        }

        // Submit approval transaction for maximum amount with retry
        const tx = await withRetry(async () => {
          const transaction = await erc20Contract.approve(marketplace, MaxUint256);
          return transaction;
        });

        await withRetry(async () => {
          await tx.wait();
        });

        // Update cache with new approval
        await marketplaceApprovalCache.upsert(address, currency, marketplaceName);

        return true;
      } catch (error: any) {
        errorLog('MarketplaceApproval', `Approval failed for ${marketplace}`, error);
        return false;
      } finally {
        // Clean up lock after completion
        approvalLocks.delete(approvalKey);
      }
    })();

    // Store promise in locks map and return it
    approvalLocks.set(approvalKey, approvalPromise);
    return approvalPromise;
  } catch (error: any) {
    errorLog('MarketplaceApproval', 'Fatal error in approveMarketplace', error);
    return false;
  }
}

export async function logBidError(taskId: string, title: string, message: string, type: "skipped" | "warning" | "error", marketplace: "opensea" | "magiceden" | "blur") {
  try {
    // Filter out circuit breaker errors - they're expected during high-failure periods
    if (message.includes('Circuit breaker is OPEN')) {
      return;
    }

    // Create a hash of the message for comparison (to handle long messages)
    const messageHash = crypto.createHash('md5').update(message).digest('hex');

    // Truncate message if it's too long for MongoDB
    const maxMessageLength = 10000; // Safe limit for MongoDB
    const truncatedMessage = message.length > maxMessageLength
      ? message.substring(0, maxMessageLength) + '... (truncated)'
      : message;

    // Try to find an existing log with the same taskId, messageHash, and marketplace
    const existingLog = await logsCache.findOne(taskId, messageHash, marketplace.toLowerCase());

    if (existingLog) {
      // Update timestamp only
      await logsCache.updateTimestamp(taskId, messageHash, marketplace.toLowerCase(), new Date());
      return; // Skip duplicate
    }

    // Create new log entry
    const logEntry: BidLogEntry = {
      taskId,
      title,
      message: truncatedMessage,
      messageHash, // Store hash for future comparisons
      type: type,
      marketplace: marketplace.toLowerCase() as 'opensea' | 'magiceden' | 'blur' | 'system',
      timestamp: new Date()
    };

    await logsCache.upsert(logEntry);
  } catch (error) {
    errorLog('BidLogging', 'Error logging bid error', error);
  }
}

export function updateTaskProcessingPercentage(taskId: string, percentage: number, marketplace?: string): void {
  if (!taskStatus[taskId]) {
    return;
  }

  // Update marketplace-specific processing percentage
  if (marketplace) {
    const mpKey = marketplace.toLowerCase() as 'opensea' | 'magiceden' | 'blur';
    if (taskStatus[taskId].marketplaces[mpKey]) {
      taskStatus[taskId].marketplaces[mpKey]!.processingPercentage = percentage;
    }
  } else {
    // Update all marketplaces if no specific marketplace provided
    const task = activeTasks.get(taskId);
    const selectedMarketplaces = task?.selectedMarketplaces || [];
    for (const mp of selectedMarketplaces) {
      const mpKey = mp.toLowerCase() as 'opensea' | 'magiceden' | 'blur';
      if (taskStatus[taskId].marketplaces[mpKey]) {
        taskStatus[taskId].marketplaces[mpKey]!.processingPercentage = percentage;
      }
    }
  }

  // Note: Removed global percentage assignment and WebSocket broadcast
  // The main publishTaskMetrics() function handles all WebSocket updates via Redis Streams
}

interface BidStat {
  criteriaType: string;
  criteriaValue: { [key: string]: string };
  bestPrice: string;
  totalValue: string;
}

interface BidStats {
  contractAddress: string;
  stats: BidStat[];
}

function transformNewTask(newTask: Record<string, Array<{ name: string }>>): Record<string, string[]> {
  const transformedTask: Record<string, string[]> = {};

  for (const key in newTask) {
    if (newTask.hasOwnProperty(key)) {
      transformedTask[key] = newTask[key].map(item => item.name);
    }
  }
  return transformedTask;
}

// Use Set for O(1) operations instead of array
export const abortedTasks = new Set<string>();

// Track timestamps for cleanup
const abortedTasksWithTimestamp = new Map<string, number>();
const ABORTED_TASKS_TTL = 30 * 60 * 1000; // 30 minutes

export function markTaskAsAborted(taskId: string): boolean {
  // Input validation
  if (!taskId || typeof taskId !== 'string') {
    warnLog('General', 'Invalid taskId provided to markTaskAsAborted');
    return false;
  }

  // Check if already aborted
  if (abortedTasks.has(taskId)) {
    // Task already marked as aborted (removed debug log)
    return false;
  }

  // Mark as aborted with timestamp
  abortedTasks.add(taskId);
  abortedTasksWithTimestamp.set(taskId, Date.now());

  // Task marked as aborted (removed info log)

  return true;
}

function removeTaskFromAborted(taskId: string): boolean {
  if (!taskId || typeof taskId !== 'string') {
    warnLog('General', 'Invalid taskId provided to removeTaskFromAborted');
    return false;
  }

  const wasRemoved = abortedTasks.delete(taskId);
  abortedTasksWithTimestamp.delete(taskId);

  // Task removed from aborted list (removed info log)

  return wasRemoved;
}

export function isTaskAborted(taskId: string): boolean {
  if (!taskId || typeof taskId !== 'string') {
    return false;
  }
  return abortedTasks.has(taskId);
}

// Add cleanup function for expired aborted tasks
export function cleanupAbortedTasks(): void {
  const now = Date.now();
  const cutoff = now - ABORTED_TASKS_TTL;
  let cleanedCount = 0;

  for (const [taskId, timestamp] of abortedTasksWithTimestamp.entries()) {
    if (timestamp < cutoff) {
      abortedTasks.delete(taskId);
      abortedTasksWithTimestamp.delete(taskId);
      cleanedCount++;
    }
  }

  // Cleaned up expired aborted tasks (removed info log)
}

interface IProcessOpenseaTokenBidData {
  _id: string;
  address: string;
  walletId: string;
  slug: string;
  creatorFees: IFee;
  offerPrice: string;  // Changed from number to string to preserve Wei precision
  enforceCreatorFee: boolean;
  floor_price?: number;           // Passed from handler to avoid redundant API call
  maxBidPriceEth?: number;        // Passed from handler to avoid redundant calculation
  minBidPriceEth?: number;        // Pre-calculated min bid price
  offerPriceEth?: number;         // Pre-calculated offer price
  currency?: string;              // Pre-calculated currency address
  decimal?: number;               // Pre-calculated decimal places
  currencySymbol?: string;        // Pre-calculated currency symbol (WETH/USDC)
  enqueuedAt?: number;            // Timestamp when job was enqueued
  asset: {
    contractAddress: string;
    tokenId: number;
  };
  outbidOptions: {
    outbid: boolean;
    blurOutbidMargin: number | null;
    openseaOutbidMargin: number | null;
    magicedenOutbidMargin: number | null;
    counterbid: boolean;
  };
  oldOrderHash?: string;  // For speed-first cancellation
}

interface IMagicedenTokenBidData {
  taskId: string;  // Fixed: was _id, causing memory leak in counterbidCounts
  address: string;
  contractAddress: string;
  quantity: number;
  offerPrice?: string;  // Changed from number to string to preserve Wei precision
  expiration: number;
  walletId: string;
  slug: string;
  tokenId: number;
  enqueuedAt?: number;
  outbidOptions: {
    outbid: boolean;
    blurOutbidMargin: number | null;
    openseaOutbidMargin: number | null;
    magicedenOutbidMargin: number | null;
    counterbid: boolean;
  };
  maxBidPriceEth?: number;
  minBidPriceEth?: number;
}

// Optimized bid rate tracker using circular buffer (O(1) operations)
// Replaces the previous O(6n) array-based implementation
const bidRateTracker = new MarketplaceBidRateTracker(BID_WINDOW_SECONDS);

// Maintained for backward compatibility - now populated from tracker
export const globalBidRates: MarketplaceBidRates = {
  opensea: { bidsPerSecond: 0, totalBids: 0, windowPeriod: BID_WINDOW_SECONDS },
  magiceden: { bidsPerSecond: 0, totalBids: 0, windowPeriod: BID_WINDOW_SECONDS },
  blur: { bidsPerSecond: 0, totalBids: 0, windowPeriod: BID_WINDOW_SECONDS }
};

export function getAllBidRates(): MarketplaceBidRates {
  const tasks = Array.from(activeTasks.values());
  const running = tasks.some(task => task.running);

  if (!running) {
    return {
      opensea: { bidsPerSecond: 0, totalBids: 0, windowPeriod: BID_WINDOW_SECONDS },
      blur: { bidsPerSecond: 0, totalBids: 0, windowPeriod: BID_WINDOW_SECONDS },
      magiceden: { bidsPerSecond: 0, totalBids: 0, windowPeriod: BID_WINDOW_SECONDS }
    };
  }

  // Get current rates from tracker and update global state
  const rates = bidRateTracker.getAllBidRates();
  globalBidRates.opensea = rates.opensea;
  globalBidRates.blur = rates.blur;
  globalBidRates.magiceden = rates.magiceden;

  return globalBidRates;
}

export function trackBidRate(marketplace: 'opensea' | 'magiceden' | 'blur', taskId: string) {
  // O(1) operation - no array manipulation or reconstruction
  bidRateTracker.trackBid(marketplace);

  // Update bid stats for the specific task
  if (!bidStats[taskId]) {
    bidStats[taskId] = {
      opensea: 0,
      magiceden: 0,
      blur: 0
    };
  }
  bidStats[taskId][marketplace]++;


  // Update globalBidRates for immediate access
  const rates = bidRateTracker.getAllBidRates();
  globalBidRates.opensea = rates.opensea;
  globalBidRates.blur = rates.blur;
  globalBidRates.magiceden = rates.magiceden;

  return { globalBidRates, bidStats };
}

/**
 * Increments memory-based counter for our own bids when we detect we're the maker
 * Uses unbounded Set for deduplication - cleared on task stop/restart
 */
export function incrementOwnBidCounter(marketplace: 'opensea' | 'magiceden' | 'blur', orderHash: string) {
  try {
    // Normalize orderHash to always have 0x prefix for consistent deduplication
    // This fixes bug where same orderHash with/without 0x prefix was counted twice
    const normalizedHash = orderHash?.startsWith('0x') ? orderHash : `0x${orderHash}`;

    // Check if already processed (unbounded Set deduplication)
    if (!addOwnBidIfNotExists(normalizedHash)) {
      // Already counted in this session - skip silently (duplicates are expected behavior)
      incrementOwnBidsDeduplicated(marketplace);
      return ownBidCounts[marketplace] || 0;
    }

    // New bid - increment counter
    ownBidCounts[marketplace] = (ownBidCounts[marketplace] || 0) + 1;


    return ownBidCounts[marketplace];
  } catch (error) {
    errorLog('BidCounter', 'Failed to increment own bid counter', error);
  }
  return 0;
}

/**
 * Decrements memory-based counter for our own bids when we cancel a bid
 */
// export function decrementOwnBidCounter(marketplace: 'opensea' | 'magiceden' | 'blur', count: number = 1, orderHash?: string) {
//   try {
//     // If order hash provided, remove it from processed set
//     if (orderHash) {
//       processedOwnBids.delete(orderHash);
//     }
//
//     // Update local ownBidCounts object for immediate broadcast
//     if (ownBidCounts[marketplace] && ownBidCounts[marketplace] > 0) {
//       ownBidCounts[marketplace] = Math.max(0, ownBidCounts[marketplace] - count);
//       return ownBidCounts[marketplace];
//     }
//     return 0;
//   } catch (error) {
//     errorLog('BidCounter', 'Failed to decrement own bid counter', error);
//   }
//   return 0;
// }


// Dead code removed: getOwnBidCounter() and resetOwnBidCounter()
// These functions were never called and referenced non-existent Redis patterns


// Type definitions already moved to the top of the file

// Legacy functions removed - bid rate tracking now handled by MarketplaceBidRateTracker
// No need for updateBidRate() or updateAllBidRates() - tracker handles all calculations internally

export function getBidStats(taskId?: string): BidCounts | { [key: string]: number } {
  if (taskId) {
    return bidStats[taskId] || {
      opensea: 0,
      magiceden: 0,
      blur: 0
    };
  }
  return bidStats;
}
interface MarketplaceBidRates {
  opensea: BidRateStats;
  blur: BidRateStats;
  magiceden: BidRateStats;
}

interface BidRateStats {
  bidsPerSecond: number;
  totalBids: number;
  windowPeriod: number;
}
interface IOpenseaBidParams {
  taskId: string;
  bidCount: string;
  walletAddress: string;
  walletId: string;
  slug: string;
  offerPrice: string;  // Changed from number to string to preserve Wei precision
  creatorFees: IFee;
  enforceCreatorFee: boolean;
  expiry: number;
  floor_price?: number;  // Cached floor price from handler to avoid redundant API call
  enqueuedAt?: number;
}

interface IMagicedenCollectionBidData {
  taskId: string;  // Fixed: was _id, causing memory leak in counterbidCounts
  bidCount: string;
  address: string;
  contractAddress: string;
  quantity: number;
  offerPrice: string;  // Changed from number to string to preserve Wei precision
  expiration: number;
  walletId: string;
  slug: string;
  currency?: string;
  enqueuedAt?: number;
}
interface IBlurBidData {
  taskId: string;
  bidCount: string;
  address: string;
  walletId: string;
  contractAddress: string;
  offerPrice: string;  // Changed from number to string to preserve Wei precision
  slug: string;
  expiry: number;
  traits?: string;
  enqueuedAt?: number;
}

interface IProcessOpenseaTraitBidData {
  _id: string;
  bidCount: string;
  address: string;
  walletId: string;
  slug: string;
  offerPrice: string;  // Changed from number to string to preserve Wei precision
  creatorFees: IFee;
  enforceCreatorFee: boolean;
  expiry: number;
  trait: string;
  oldOrderHash?: string;  // For speed-first cancellation
  floor_price?: number;   // Cached floor price from handler to avoid redundant API call
  maxBidPriceEth?: number;        // Pre-calculated max bid price
  minBidPriceEth?: number;        // Pre-calculated min bid price
  offerPriceEth?: number;         // Pre-calculated offer price
  currency?: string;              // Pre-calculated currency address
  decimal?: number;               // Pre-calculated decimal places
  currencySymbol?: string;        // Pre-calculated currency symbol (WETH/USDC)
  contractAddress?: string;       // Contract address (added for trait bids)
  enqueuedAt?: number;            // Timestamp when job was enqueued
  outbidOptions?: {               // Outbid options
    outbid: boolean;
    blurOutbidMargin: number | null;
    openseaOutbidMargin: number | null;
    magicedenOutbidMargin: number | null;
    counterbid: boolean;
  };
}

interface Currency {
  contract: string;
  name: string;
  symbol: string;
  decimals: number;
}

interface Amount {
  raw: string;
  decimal: number;
  usd: number;
  native: number;
}

interface NetAmount extends Amount { }

interface Price {
  currency: Currency;
  amount: Amount;
  netAmount: NetAmount;
}

interface Collection {
  id: string;
}

interface CriteriaData {
  collection: Collection;
}

interface Criteria {
  kind: string;
  data: CriteriaData;
}

interface Source {
  id: string;
  domain: string;
  name: string;
  icon: string;
  url: string;
}

interface FeeBreakdown {
  kind: string;
  recipient: string;
  bps: number;
}

export interface Orders {
  id: string;
  kind: string;
  side: string;
  status: string;
  tokenSetId: string;
  tokenSetSchemaHash: string;
  contract: string;
  contractKind: string;
  maker: string;
  taker: string;
  price: Price;
  validFrom: number;
  validUntil: number;
  quantityFilled: number;
  quantityRemaining: number;
  criteria: Criteria;
  source: Source;
  feeBps: number;
  feeBreakdown: FeeBreakdown[];
  expiration: number;
  isReservoir: boolean | null;
  createdAt: string;
  updatedAt: string;
  originatedAt: string | null;
}


// Legacy network monitoring system removed - now using unified HTTP-based monitoring in startConnectivityMonitoring()




export interface CollectionData {
  collection: string;
  name: string;
  description: string;
  image_url: string;
  banner_image_url: string;
  owner: string;
  safelist_status: string;
  category: string;
  is_disabled: boolean;
  is_nsfw: boolean;
  trait_offers_enabled: boolean;
  collection_offers_enabled: boolean;
  opensea_url: string;
  project_url: string;
  wiki_url: string;
  discord_url: string;
  telegram_url: string;
  twitter_username: string;
  instagram_username: string;
  contracts: {
    address: string;
    chain: string;
  }[];
  editors: string[];
  fees: {
    fee: number;
    recipient: string;
    required: boolean;
  }[];
  payment_tokens: {
    symbol: string;
    address: string;
    chain: string;
    image: string;
    name: string;
    decimals: number;
    eth_price: string;
    usd_price: string;
  }[];
  total_supply: number;
  created_date: string;
}

export interface MagicEdenCollectionMedia {
  url: string;
}

export interface MagicEdenCollectionSocial {
  twitterUsername: string;
  discordUrl: string;
  websiteUrl: string;
}

export interface MagicEdenCollectionRoyalty {
  recipient: string;
  bps: number;
  isOptional: boolean;
}

export interface MagicEdenCollectionChainData {
  contract: string;
  transferability: "TRANSFERABLE_TRADABLE" | string;
  collectionBidSupported: boolean;
  contractDeployedAt: string;
  isMinting: boolean;
}

export interface MagicEdenCollection {
  id: string;
  chain: string;
  name: string;
  symbol: string;
  description: string;
  media: MagicEdenCollectionMedia;
  social: MagicEdenCollectionSocial;
  verification: string;
  flags: string[];
  visibility: string;
  isTradeable: boolean;
  filterableAttributes: string[];
  royalty: MagicEdenCollectionRoyalty;
  collectionType: string;
  chainData: MagicEdenCollectionChainData;
}

export interface MagicEdenCollectionsResponse {
  collections: MagicEdenCollection[];
}


export interface BlurFloorPriceResponse {
  success: boolean;
  collection: {
    contractAddress: string;
    name: string;
    collectionSlug: string;
    imageUrl: string;
    totalSupply: number;
    numberOwners: number;
    floorPrice: {
      amount: string;
      unit: string;
    };
    floorPriceOneDay: {
      amount: string;
      unit: string;
    };
    floorPriceOneWeek: {
      amount: string;
      unit: string;
    };
    volumeFifteenMinutes: null | number;
    volumeOneDay: {
      amount: string;
      unit: string;
    };
    volumeOneWeek: {
      amount: string;
      unit: string;
    };
    bestCollectionBid: {
      amount: string;
      unit: string;
    };
    totalCollectionBidValue: {
      amount: string;
      unit: string;
    };
    traitFrequencies: Record<string, any>;
    bestCollectionLoanOffer: null;
  };
}

interface IBlurData {
  traits: {
    categories: IBlurTraitCategories;
    counts: IBlurTraitCounts;
  };
  blurRaw: {
    success: boolean;
    traits: IBlurTraits;
  };
}

interface IBlurTraitCategories {
  [key: string]: string;
}

interface IBlurTraitCounts {
  [key: string]: {
    [key: string]: number;
  };
}

interface IBlurTraits {
  [key: string]: {
    [key: string]: IBlurTraitData;
  };
}

interface IBlurTraitData {
  sale?: {
    amount: string;
    unit: string;
    listedAt: string;
  };
  floor?: {
    amount: string;
    unit: string;
    listedAt: string;
  };
  bestBidPrice?: {
    amount: string;
    unit: string;
  } | null;
  totalBidValue?: {
    amount: string;
    unit: string;
  } | null;
}


function terraformBlurTraits(
  blurTraits: Record<string, Record<string, any>>
): Traits {
  try {
    const traits: {
      categories: Record<string, string>;
      counts: Record<string, Record<string, number>>;
    } = {
      categories: {},
      counts: {},
    };
    for (const category in blurTraits) {
      traits.categories[category] = "string";
      traits.counts[category] = {};
      for (const item in blurTraits[category]) {
        traits.counts[category][item] = 0;
      }
    }
    return traits;
  } catch (error) {
    return { categories: {}, counts: {} };
  }
}

type MagicEdenAttribute = {
  key: string;
  attributeCount: number;
  kind: string;
  values: MagicEdenValue[];
};

type MagicEdenValue = {
  count: number;
  value: string;
  floorAskPrice?: {
    currency: {
      contract: string;
      name: string;
      symbol: string;
      decimals: number;
    };
    amount: {
      raw: string;
      decimal: number;
      usd: number;
      native: number;
    };
  };
};

type MagicEdenTraits = {
  attributes: MagicEdenAttribute[];
};
interface Traits {
  categories: {
    [key: string]: string;
  };
  counts: {
    [key: string]: Record<string, number>;
  };
}

interface MagicEdenData {
  traits: MagicEdenTraitsData;
  magicedenRaw: MagicEdenRaw;
}

interface MagicEdenTraitsData {
  categories: {
    [key: string]: string;
  };
  counts: {
    [key: string]: {
      [key: string]: number;
    };
  };
}

interface MagicEdenRaw {
  attributes: MagicEdenRawAttribute[];
}

interface MagicEdenRawAttribute {
  key: string;
  attributeCount: number;
  kind: string;
  values: MagicEdenRawAttributeValue[];
}

interface MagicEdenRawAttributeValue {
  count: number;
  value: string;
  floorAskPrice?: {
    currency: MagicEdenCurrency;
    amount: MagicEdenAmount;
  };
}

interface MagicEdenCurrency {
  contract: string;
  name: string;
  symbol: string;
  decimals: number;
}

interface MagicEdenAmount {
  raw: string;
  decimal: number;
  usd: number;
  native: number;
}


function transformMagicEdenTraits(magicEdenTraits: MagicEdenTraits): Traits {
  const traits: Traits = {
    categories: {},
    counts: {},
  };

  if (!magicEdenTraits || !magicEdenTraits.attributes) {
    return traits;
  }

  try {
    magicEdenTraits.attributes.forEach((attribute) => {
      if (!attribute || !attribute.key || !attribute.values) return;

      const { key, values } = attribute;
      traits.categories[key] = "string";
      traits.counts[key] = {};

      values.forEach((value) => {
        if (!value || !value.value) return;
        const { value: itemValue, count } = value;
        traits.counts[key][itemValue] = count;
      });
    });

    return traits;
  } catch (error) {
    errorLog('MagicEdenTraits', 'Error transforming Magic Eden traits', error);
    return traits;
  }
}
interface OpenseaTraits {
  data: OpenseaData;
}

interface OpenseaData {
  collection: OpenseaCollection;
}

interface OpenseaCollection {
  statsV2: OpenseaStatsV2;
  stringTraits: OpenseaStringTrait[];
  id: string;
}

interface OpenseaStatsV2 {
  totalSupply: number;
}

interface OpenseaStringTrait {
  key: string;
  counts: OpenseaTraitCount[];
}

interface OpenseaTraitCount {
  count: number;
  value: string;
  floor: OpenseaFloor | null;
}

interface OpenseaFloor {
  eth: string;
  unit: string;
  symbol: string;
  usd: string;
}

function transformOpenseaTrait(actualData: any) {
  const result: any = {
    categories: {},
    counts: {},
  };
  actualData?.data?.collection?.stringTraits?.forEach((trait: any) => {
    result.categories[trait.key] = "string";
    result.counts[trait.key] = {};
    trait.counts.forEach((item: any) => {
      result.counts[trait.key][item.value] = item.count;
    });
  });

  return result;
}

interface DebugInfo {
  additionalInformation: {
    'x-trace-id': string;
    'x-cache-status': string;
    age: number;
    'cache-control': string;
    'x-ratelimit-remaining': number;
  };
}

interface RollingStats {
  priceChange: number;
  __typename: 'CurrencyRollingStats';
}

interface CurrencyStats {
  marketCapUsd: number;
  oneDay: RollingStats;
  __typename: 'CurrencyStats';
}

interface Chain {
  identifier: string;
  minOfferPriceUsd: string;
  __typename: 'Chain';
}

interface Currency {
  id: string;
  contractAddress: string;
  chain: Chain;
  usdPrice: string;
  symbol: string;
  imageUrl: string;
  name: string;
  decimals: number;
  stats: CurrencyStats;
  isSwapDisabled: boolean;
  __typename: 'Currency';
}

interface Balance {
  id: string;
  currency: Currency;
  quantity: string;
  usdValue: string;
  __typename: 'Balance';
}

export interface AddressBalancesResponse {
  data: {
    addressBalances: Balance[];
  };
  extensions: {
    debugInfo: DebugInfo;
  };
}

// Listing WebSocket handlers
async function handleStartListing(data: { _id: string }, ws: WebSocket) {
  try {
    const listing = await Listing.findById(data._id);

    if (!listing) {
      ws.send(JSON.stringify({
        type: 'error',
        message: 'Listing not found'
      }));
      return;
    }

    // Update listing to running
    listing.running = true;
    await listing.save();

    const createListingStart = Date.now();
    // Schedule the listing
    await createListing(listing)

    const scheduleStart = Date.now();
    await scheduleListing(listing);

    // Subscribe to WebSocket events if instant price matching is enabled

    if (listing.instantPriceMatching && wsManager) {

      try {
        // Use proper subscription management with listing type
        await wsManager.subscribe(listing.contract.slug, listing.contract.contractAddress, 'ALL', 'listing');
      } catch (error) {
        errorLog('ListingWebSocket', 'WebSocket subscription failed', error);
      }
    } else {
      if (!listing.instantPriceMatching) {
      }
      if (!wsManager) {
      }
    }

    ws.send(JSON.stringify({
      type: 'listing-started',
      listingId: data._id,
      message: 'Listing started successfully'
    }));

  } catch (error) {
    errorLog('ListingStart', 'Error starting listing', error);
    ws.send(JSON.stringify({
      type: 'error',
      message: 'Failed to start listing',
      error: error instanceof Error ? error.message : String(error)
    }));
  }
}

async function handleStopListing(data: { listingId: string }, ws: WebSocket) {
  try {
    const listing = await Listing.findById(data.listingId);
    if (!listing) {
      ws.send(JSON.stringify({
        type: 'error',
        message: 'Listing not found'
      }));
      return;
    }

    // Update listing to not running
    listing.running = false;
    await listing.save();

    // Cancel the scheduled job
    await cancelListingJob(data.listingId);

    // Unsubscribe from WebSocket events
    if (wsManager && listing.contract.slug && listing.contract.contractAddress) {
      try {
        // Use proper subscription management for unsubscription
        await wsManager.unsubscribe(listing.contract.slug, listing.contract.contractAddress, 'ALL');
      } catch (error) {
        errorLog('ListingWebSocket', 'WebSocket unsubscription failed for cancelled listing', error);
      }
    }

    ws.send(JSON.stringify({
      type: 'listing-stopped',
      listingId: data.listingId,
      message: 'Listing stopped successfully'
    }));

  } catch (error) {
    errorLog('ListingStop', 'Error stopping listing', error);
    ws.send(JSON.stringify({
      type: 'error',
      message: 'Failed to stop listing',
      error: error instanceof Error ? error.message : String(error)
    }));
  }
}

async function handleUpdateListing(data: { _id: string, updates: any }, ws: WebSocket) {
  try {
    const listing = await Listing.findById(data._id);
    if (!listing) {
      ws.send(JSON.stringify({
        type: 'error',
        message: 'Listing not found'
      }));
      return;
    }

    // Update listing fields
    Object.assign(listing, data.updates);
    await listing.save();

    // If running, reschedule with new parameters
    if (listing.running) {
      await scheduleListing(listing);
    }

    // Handle instant price matching subscription changes
    if (data.updates.instantPriceMatching !== undefined && wsManager) {
      if (data.updates.instantPriceMatching && listing.running) {

        try {
          // Subscribe to listing events using proper subscription management
          await wsManager.subscribe(listing.contract.slug, listing.contract.contractAddress, 'ALL', 'listing');
        } catch (error) {
          errorLog('ListingWebSocket', 'WebSocket subscription failed', error);
        }
      } else {

        try {
          // Unsubscribe from listing events
          await wsManager.unsubscribe(listing.contract.slug, listing.contract.contractAddress, 'ALL');
        } catch (error) {
          errorLog('ListingWebSocket', 'WebSocket unsubscription failed', error);
        }
      }
    }

    ws.send(JSON.stringify({
      type: 'listing-updated',
      listingId: data._id,
      message: 'Listing updated successfully'
    }));

  } catch (error) {
    errorLog('ListingUpdate', 'Error updating listing', error);
    ws.send(JSON.stringify({
      type: 'error',
      message: 'Failed to update listing',
      error: error instanceof Error ? error.message : String(error)
    }));
  }
}

// ============================================
// Job Tracking Validation and Recovery System
// ============================================

/**
 * Periodically logs job tracking status to identify mismatches
 * between taskStatus and parent job tracking
 */
async function logJobTrackingStatus() {
  try {

    for (const [taskId, status] of Object.entries(taskStatus)) {
      // Check marketplace-specific statuses
      const marketplaces = ['opensea', 'magiceden', 'blur'] as const;
      const hasActiveMarketplace = marketplaces.some(mp => {
        const mpStatus = status.marketplaces?.[mp];
        return mpStatus && (mpStatus.status === 'RUNNING' || mpStatus.status === 'QUEUED' || mpStatus.status === 'QUEUEING');
      });

      if (hasActiveMarketplace) {
        let totalTracked = 0;
        let completedTracked = 0;
        const parentJobDetails: Record<string, { total: number; completed: number }> = {};

        // Calculate totals from marketplace data
        let taskTotalJobs = 0;
        let taskCompletedJobs = 0;
        let overallRunningPercentage = 0;

        for (const mp of marketplaces) {
          const mpStatus = status.marketplaces?.[mp];
          if (mpStatus) {
            taskTotalJobs += mpStatus.totalJobs || 0;
            taskCompletedJobs += mpStatus.completedJobs || 0;
          }

          const parentJobId = `${taskId}-${mp}`;
          const parentJob = parentJobs.get(parentJobId);

          if (parentJob) {
            totalTracked += parentJob.totalJobs;
            completedTracked += parentJob.completedJobs;
            parentJobDetails[mp] = {
              total: parentJob.totalJobs,
              completed: parentJob.completedJobs
            };
          }
        }

        // Calculate overall running percentage
        if (taskTotalJobs > 0) {
          overallRunningPercentage = (taskCompletedJobs / taskTotalJobs) * 100;
        }

        // Log discrepancies or status
        const hasDiscrepancy = taskTotalJobs !== totalTracked || taskCompletedJobs !== completedTracked;

        if (hasDiscrepancy || overallRunningPercentage < 100) {

          // Show breakdown by marketplace
          for (const [mp, details] of Object.entries(parentJobDetails)) {
            const percentage = details.total > 0 ? (details.completed / details.total * 100).toFixed(1) : '0.0';
          }

          if (hasDiscrepancy) {

            // Check for partial queueing mismatch (0 totalJobs but positive expected)
            if (taskTotalJobs === 0 && totalTracked === 0) {
              let hasPartialQueueing = false;
              for (const [mp, details] of Object.entries(parentJobDetails)) {
                const parentJobId = `${taskId}-${mp}`;
                const parentJob = parentJobs.get(parentJobId);
                if (parentJob &&
                  parentJob.expectedJobs !== undefined &&
                  parentJob.expectedJobs > 0 &&
                  !parentJob.allJobsQueued) {
                  hasPartialQueueing = true;
                  break;
                }
              }

              if (hasPartialQueueing) {
              }
            }
          }

          // Check if all jobs are complete but percentage is not 100
          if (totalTracked > 0 && completedTracked === totalTracked && overallRunningPercentage < 100) {
          }

          // Check for stuck task - low percentage with no queue activity
          const queueCounts = await queue.getJobCounts();
          const totalQueueJobs = (queueCounts.waiting || 0) + (queueCounts.active || 0)
          if (overallRunningPercentage < 10 && totalQueueJobs === 0 && totalTracked > 0) {

            // Force mark all parent jobs as complete to unstick the task
            for (const mp of marketplaces) {
              const parentJobId = `${taskId}-${mp}`;
              const parentJob = parentJobs.get(parentJobId);
              if (parentJob && parentJob.completedJobs < parentJob.totalJobs) {
                parentJob.completedJobs = parentJob.totalJobs;
              }
            }

            // Trigger task completion check for each selected marketplace
            const stuckTask = activeTasks.get(taskId);
            if (stuckTask && stuckTask.selectedMarketplaces) {
              for (const mp of stuckTask.selectedMarketplaces) {
                const mpLower = mp.toLowerCase();
                if (marketplaces.includes(mpLower as 'opensea' | 'magiceden' | 'blur')) {
                  await checkTaskCompletionAndScheduleNext(taskId, mpLower);
                }
              }
            }
          }
        }
      }
    }

  } catch (error) {
    errorLog('JobTracking', 'Error logging job tracking status', error);
  }
}

// NOTE: recoverPartialQueueingTasks() removed - recovery logic was causing issues
// See docs/RECOVERY-FUNCTIONS.md for historical documentation

/**
 * Checks if all parent jobs for a task are complete
 */
function checkAllParentJobsComplete(taskId: string): boolean {
  const task = activeTasks.get(taskId);
  if (!task) {
    return false;
  }

  const selectedMarketplaces = task.selectedMarketplaces.map(m => m.toLowerCase());
  const enabledMarketplacesLower = enabledMarketplaces.map(m => m.toLowerCase());
  const marketplacesToCheck = selectedMarketplaces.filter(m => enabledMarketplacesLower.includes(m));

  let allComplete = true;
  let hasAnyJobs = false;
  const mpResults: Array<{ mp: string; completed: number; total: number; isComplete: boolean; hasParentJob: boolean; allJobsQueued?: boolean; actualJobs?: number }> = [];

  for (const marketplace of marketplacesToCheck) {
    const parentJobId = getParentJobId(taskId, marketplace);
    const parentJob = parentJobs.get(parentJobId);

    if (parentJob) {
      // Use hybrid tracking: expectedJobs while queueing, actualJobs after all queued
      let totalJobs = parentJob.totalJobs || 0;

      // If we have actualJobs and allJobsQueued is true, use actualJobs as the accurate count
      if (parentJob.allJobsQueued && parentJob.actualJobs !== undefined) {
        totalJobs = parentJob.actualJobs;
      }

      const isComplete = totalJobs > 0 && parentJob.completedJobs >= totalJobs;

      // DEBUG: Track each marketplace result
      mpResults.push({
        mp: marketplace,
        completed: parentJob.completedJobs,
        total: totalJobs,
        isComplete,
        hasParentJob: true,
        allJobsQueued: parentJob.allJobsQueued,
        actualJobs: parentJob.actualJobs
      });

      if (totalJobs > 0) {
        hasAnyJobs = true;
        if (parentJob.completedJobs < totalJobs) {
          allComplete = false;
          break;
        }
      }
    } else {
      // DEBUG: Track missing parent job
      mpResults.push({
        mp: marketplace,
        completed: 0,
        total: 0,
        isComplete: false,
        hasParentJob: false
      });
    }
  }

  const result = hasAnyJobs && allComplete;
  return result;
}

/**
 * Track when tasks first appear to be complete
 */
const taskCompletionTracking = new Map<string, number>();

// NOTE: taskScheduleWatchdog() removed - recovery logic was causing issues
// See docs/RECOVERY-FUNCTIONS.md for historical documentation


// NOTE: queueingTaskWatchdog() and hasTaskJobsInQueue() removed - recovery logic was causing issues
// See docs/RECOVERY-FUNCTIONS.md for historical documentation


// NOTE: checkStuckTasks() removed - recovery logic was causing issues
// See docs/RECOVERY-FUNCTIONS.md for historical documentation

// NOTE: healthCheckStuckTasks() and recoverStuckTask() removed - recovery logic was causing issues
// See docs/RECOVERY-FUNCTIONS.md for historical documentation

// NOTE: healthCheckStuckTasks interval removed - recovery logic was causing issues
// const TASK_HEALTH_CHECK_INTERVAL = 30000;
// setInterval(healthCheckStuckTasks, TASK_HEALTH_CHECK_INTERVAL);

// NOTE: verifyJobCreation() and registerJobCreationVerification() removed - recovery logic was causing issues
// See docs/RECOVERY-FUNCTIONS.md for historical documentation

/**
 * Ensures parent job exists before job creation
 * Uses a queue to prevent race conditions
 */
const pendingJobCreation = new Map<string, Promise<void>>();

/**
 * Helper function to generate parent job ID with loop number
 */
function getParentJobId(taskId: string, marketplace: string): string {
  const currentLoopCount = loopCounterStats[taskId]?.[marketplace] || 1;
  return `${taskId}-${marketplace}-loop${currentLoopCount}`.toLowerCase();
}


async function ensureParentJobExists(taskId: string, marketplace: string): Promise<void> {
  const mpKeyForLog = marketplace.toLowerCase() as 'opensea' | 'magiceden' | 'blur';
  const parentJobId = getParentJobId(taskId, marketplace);
  const currentLoopCount = loopCounterStats[taskId]?.[marketplace] || 1;

  // TaskFlow logging: Entry

  // Validate inputs
  if (!taskId || !marketplace) {
    errorLog('ParentJob', `ENSURE_INVALID_INPUT | taskId=${taskId} | mp=${marketplace}`, undefined, { taskId, marketplace: mpKeyForLog });
    return;
  }

  // Check if already being created
  const existing = pendingJobCreation.get(parentJobId);
  if (existing) {
    await existing;
    return;
  }

  // Create promise for this parent job
  const promise = (async () => {
    try {
      if (!parentJobs.has(parentJobId)) {

        // Validate task exists
        const task = activeTasks.get(taskId);
        if (!task) {
          errorLog('ParentJob', `ENSURE_TASK_NOT_FOUND | taskId=${taskId} | parentJobId=${parentJobId}`, undefined, { taskId, marketplace: mpKeyForLog, parentJobId });
          return;
        }

        // Create parent job with validation
        const newParentJob: BulkJobParent = {
          parentId: parentJobId,
          totalJobs: 0,
          completedJobs: 0,
          failedJobs: 0,
          startTime: Date.now(),
          taskId: taskId,
          jobName: `${marketplace.toUpperCase()}_JOBS`,
          loopNumber: currentLoopCount
        };

        parentJobs.set(parentJobId, newParentJob);

        // TaskFlow logging: Parent job created

        // Initialize processed jobs tracking
        if (!processedJobsPerParent.has(parentJobId)) {
          processedJobsPerParent.set(parentJobId, new Set<string>());
        }
      } else {
        // Parent job already exists - validate its state
        const existingParentJob = parentJobs.get(parentJobId);
        if (existingParentJob) {
          // Check for inconsistencies
          if (existingParentJob.completedJobs > existingParentJob.totalJobs) {
            warnLog('ParentJob',
              `INCONSISTENCY_FIX | parentJobId=${parentJobId} | completedJobs=${existingParentJob.completedJobs} > totalJobs=${existingParentJob.totalJobs}`,
              { taskId, marketplace: mpKeyForLog, parentJobId, completedJobs: existingParentJob.completedJobs, totalJobs: existingParentJob.totalJobs }
            );
            existingParentJob.completedJobs = existingParentJob.totalJobs;
          }
        }
      }
    } catch (error) {
      errorLog('ParentJob', `ENSURE_ERROR | parentJobId=${parentJobId}`, error, { taskId, marketplace: mpKeyForLog, parentJobId });
      if (error instanceof Error) {
        errorLog('ParentJob', `Error details: ${error.message}`);
      }
    }
  })();

  pendingJobCreation.set(parentJobId, promise);

  try {
    await promise;
  } finally {
    pendingJobCreation.delete(parentJobId);
  }
}

/**
 * Wait for parent job to be ready before queueing child jobs
 * Optimized with shorter timeout and faster polling for quick in-memory operations
 */
async function waitForParentJobReady(taskId: string, marketplace: string, maxWaitTime: number = 2000): Promise<boolean> {
  const parentJobId = getParentJobId(taskId, marketplace);
  const startTime = Date.now();

  while (Date.now() - startTime < maxWaitTime) {
    const parentJob = parentJobs.get(parentJobId);
    if (parentJob) {
      return true;
    }

    // Check if creation is pending
    if (pendingJobCreation.has(parentJobId)) {
      await new Promise(resolve => setTimeout(resolve, 50)); // Reduced from 100ms to 50ms
      continue;
    }

    // Parent job doesn't exist and isn't being created
    break;
  }

  return false;
}

/**
 * Enhanced ensureParentJobExists with validation
 */
async function ensureParentJobExistsEnhanced(taskId: string, marketplace: string): Promise<void> {
  const parentJobId = getParentJobId(taskId, marketplace);
  const mpKeyForLog = marketplace.toLowerCase() as 'opensea' | 'magiceden' | 'blur';


  // Validate inputs
  if (!taskId || !marketplace) {
    throw new Error(`Invalid inputs - taskId: ${taskId}, marketplace: ${marketplace}`);
  }

  // Check if task is in valid state
  const taskState = taskStatus[taskId];
  if (!taskState) {
    throw new Error(`Task ${taskId} has no status - cannot create parent job`);
  }

  // Prevent parent job creation for completed/stopped tasks
  // Check if all marketplaces are completed or stopped
  const marketplaceStatuses = Object.entries(taskState.marketplaces || {}).map(([mp, state]) => `${mp}:${state?.status}`).join(', ');
  const allMarketplacesFinished = Object.values(taskState.marketplaces || {}).every(
    mp => mp && ['COMPLETED', 'STOPPED'].includes(mp.status)
  );

  if (allMarketplacesFinished) {
    return;
  }

  // Use the existing ensureParentJobExists
  await ensureParentJobExists(taskId, marketplace);
}

/**
 * Orphaned job recovery system
 */
class OrphanedJobRecovery {
  private recoveryInterval: NodeJS.Timeout | null = null;

  // Start periodic recovery checks
  startRecovery(intervalMs: number = 30000) { // Check every 30 seconds
    if (this.recoveryInterval) {
      clearInterval(this.recoveryInterval);
    }

    this.recoveryInterval = setInterval(() => {
      this.recoverOrphanedJobs().catch(error => {
        errorLog('OrphanedJobRecovery', 'Recovery check failed', error);
      });
    }, intervalMs);

  }

  stopRecovery() {
    if (this.recoveryInterval) {
      clearInterval(this.recoveryInterval);
      this.recoveryInterval = null;
    }
  }

  // Recover orphaned jobs
  async recoverOrphanedJobs() {
    try {
      const jobs = await queue.getJobs(['active', 'waiting', 'delayed']);
      const orphanedJobs: Job[] = [];
      const recoveryActions = new Map<string, Job[]>();

      for (const job of jobs) {
        if (!job || !job.id || !job.id.startsWith('bid-')) continue;

        // Extract task and marketplace from job ID
        const parts = job.id.split('-');
        if (parts.length < 3) continue;

        const taskId = parts[1];
        const marketplace = parts[2];

        // Check if parent job exists
        const currentLoop = loopCounterStats[taskId]?.[marketplace] || 1;
        const parentJobId = `${taskId}-${marketplace}-loop${currentLoop}`.toLowerCase();
        const parentJob = parentJobs.get(parentJobId);

        if (!parentJob) {
          // Check if it's from an old loop
          const jobLoopMatch = job.id.match(/loop(\d+)/);
          const jobLoop = jobLoopMatch ? parseInt(jobLoopMatch[1]) : 0;

          if (jobLoop > 0 && jobLoop < currentLoop) {
            // Old loop job - should be removed
            orphanedJobs.push(job);
          } else {
            // Current loop job without parent - needs recovery
            if (!recoveryActions.has(parentJobId)) {
              recoveryActions.set(parentJobId, []);
            }
            recoveryActions.get(parentJobId)!.push(job);
          }
        }
      }

      // Remove old loop jobs
      if (orphanedJobs.length > 0) {

        for (const job of orphanedJobs) {
          try {
            await job.remove();
          } catch (error) {
            errorLog('OrphanedJobRecovery', `Failed to remove job ${job.id}`, error);
          }
        }
      }

      // Recover current loop orphaned jobs by creating parent jobs
      for (const [parentJobId, jobs] of recoveryActions.entries()) {
        const [taskId, marketplace] = parentJobId.split('-');


        // Check if task is still active
        const task = activeTasks.get(taskId);
        if (!task || !task.running) {
          for (const job of jobs) {
            try {
              await job.remove();
            } catch (error) {
              errorLog('OrphanedJobRecovery', `Failed to remove job ${job.id}`, error);
            }
          }
          continue;
        }

        // Create parent job for recovery
        const currentLoop = loopCounterStats[taskId]?.[marketplace] || 1;
        const newParentJob: BulkJobParent = {
          parentId: parentJobId,
          totalJobs: jobs.length,
          completedJobs: 0,
          failedJobs: 0,
          actualJobs: jobs.length,
          expectedJobs: jobs.length,
          allJobsQueued: true,
          startTime: Date.now(),
          taskId: taskId,
          jobName: `${marketplace.toUpperCase()}_JOBS_RECOVERED`,
          loopNumber: currentLoop
        };

        parentJobs.set(parentJobId, newParentJob);

        // Update task status if needed
        const status = taskStatus[taskId];
        // Check if any marketplace is in STARTED state
        const hasStartedMarketplace = status && Object.values(status.marketplaces || {}).some(
          mp => mp?.status === 'STARTED'
        );

        if (hasStartedMarketplace) {
          // Orphaned jobs recovered - will transition to RUNNING when processed
        }
      }

    } catch (error) {
      errorLog('OrphanedJobRecovery', 'Error during recovery', error);
      throw error;
    }
  }

  // Manual recovery for specific task
  async recoverTaskJobs(taskId: string) {

    const task = activeTasks.get(taskId);
    if (!task) {
      return;
    }

    const marketplaces = task.selectedMarketplaces.map(m => m.toLowerCase());

    for (const marketplace of marketplaces) {
      const currentLoop = loopCounterStats[taskId]?.[marketplace] || 1;
      const parentJobId = `${taskId}-${marketplace}-loop${currentLoop}`.toLowerCase();
      const parentJob = parentJobs.get(parentJobId);

      if (!parentJob) {
        // Count jobs for this marketplace
        const pattern = `bid-${taskId}-${marketplace}-`;
        const jobs = await queue.getJobs(['active', 'waiting', 'delayed']);
        const matchingJobs = jobs.filter(job => job?.id?.includes(pattern));

        if (matchingJobs.length > 0) {

          // Create parent job
          const newParentJob: BulkJobParent = {
            parentId: parentJobId,
            totalJobs: matchingJobs.length,
            completedJobs: 0,
            failedJobs: 0,
            actualJobs: matchingJobs.length,
            expectedJobs: matchingJobs.length,
            allJobsQueued: true,
            startTime: Date.now(),
            taskId: taskId,
            jobName: `${marketplace.toUpperCase()}_JOBS_RECOVERED`,
            loopNumber: currentLoop
          };

          parentJobs.set(parentJobId, newParentJob);
        }
      }
    }

    // Update task percentage
    await updateTaskRunningPercentage(taskId);
  }
}

// Create global instance
const orphanedJobRecovery = new OrphanedJobRecovery();

// Helper function to fix stuck tasks
async function fixStuckTask(taskId: string) {

  // First, try orphaned job recovery
  await orphanedJobRecovery.recoverTaskJobs(taskId);

  // Check current status
  const status = taskStatus[taskId];
  const task = activeTasks.get(taskId);
  // Check if any marketplace is in STARTED state
  const hasStartedMarketplace = status && Object.values(status.marketplaces || {}).some(
    mp => mp?.status === 'STARTED'
  );

  if (hasStartedMarketplace && task) {
    // Force progression check for all marketplaces
    for (const mp of task.selectedMarketplaces) {
      const mpStatus = status.marketplaces?.[mp.toLowerCase() as 'opensea' | 'magiceden' | 'blur'];
      if (mpStatus?.status === 'STARTED') {
        statusProgressionManager.checkAndFixStuckStartedTask(taskId, mp.toLowerCase());
      }
    }
  }

  // Update percentage
  await updateTaskRunningPercentage(taskId);

}

/**
 * Diagnostic function to check cancellation job status in queue
 */
async function checkCancellationJobStatus(taskId: string): Promise<{
  totalJobs: number;
  completedJobs: number;
  failedJobs: number;
  activeJobs: number;
  waitingJobs: number;
  stalledJobs: number;
  cancellationProgress: number | null;
  details: any;
}> {
  try {

    // Find cancellation jobs for this task
    let totalCancellationJobs = 0;
    let completedCancellationJobs = 0;
    let failedCancellationJobs = 0;
    let activeCancellationJobs = 0;
    let waitingCancellationJobs = 0;
    let stalledCancellationJobs = 0;

    // Check each job state
    const states: JobType[] = ['completed', 'failed', 'active', 'waiting'];

    for (const state of states) {
      const jobs = await queue.getJobs([state], 0, -1);

      for (const job of jobs) {
        if (job.name.includes('CANCEL_') && job.data?.taskId === taskId) {
          totalCancellationJobs++;

          switch (state) {
            case 'completed':
              completedCancellationJobs++;
              break;
            case 'failed':
              failedCancellationJobs++;
              break;
            case 'active':
              activeCancellationJobs++;
              break;
            case 'waiting':
              waitingCancellationJobs++;
              break;
          }
        }
      }
    }

    // Get cancellation progress from tracker
    const cancellationProgress = cancellationTracker.getAggregatedPercentage(taskId) ||
      cancellationTracker.getPercentage(taskId);

    const result = {
      totalJobs: totalCancellationJobs,
      completedJobs: completedCancellationJobs,
      failedJobs: failedCancellationJobs,
      activeJobs: activeCancellationJobs,
      waitingJobs: waitingCancellationJobs,
      stalledJobs: stalledCancellationJobs,
      cancellationProgress,
      details: {
        taskStatus: taskStatus[taskId],
        trackerStatus: cancellationTracker.getStatus(taskId),
        queueCounts: await queue.getJobCounts()
      }
    };


    return result;
  } catch (error) {
    errorLog('CancellationJobStatus', `Error checking cancellation status for task ${taskId}`, error);
    throw error;
  }
}

// NOTE: recoverOrphanedJobs() removed - recovery logic was causing issues
// See docs/RECOVERY-FUNCTIONS.md for historical documentation

// NOTE: These intervals are initialized in startServer() after MongoDB connection
// setInterval(logJobTrackingStatus, STANDARD_INTERVAL);

// Export verification function for use in other modules
export { verifyCancellationComplete };

/**
 * Get cache instances for debugging/monitoring
 * @internal - For use by lru-cache-sampler.ts only
 */
export function getProcessedMessages(): LRUCache<string, number> {
  return processedMessages;
}

export function getSeenOrderHashes(): LRUCache<string, boolean> {
  return seenOrderHashes;
}

export function getErrorLogCache(): LRUCache<string, boolean> {
  return errorLogCache;
}

export function getCollectionStatsLRU(): LRUCache<string, any> {
  return collectionStatsLRU;
}

export function getCollectionDataLRU(): LRUCache<string, any> {
  return collectionDataLRU;
}

// force reload
