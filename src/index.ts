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

config()

import bodyParser from "body-parser";
import { clearTimeout } from 'timers';
import crypto from 'crypto';
import os from "os"
import cors from "cors";
import helmet from "helmet";
import stringify from 'json-stable-stringify';
import WebSocket, { Server as WebSocketServer } from 'ws';
import http from 'http';
import { axiosInstance, initialize, limiter, cancelTaskJobs, removeTaskFromBlacklist, isTaskBlacklisted } from "./init";
import { bidOnOpensea, cancelOpenseaRequest, cancelOrder, fetchItemOffers, fetchOpenseaListings, fetchOpenseaOffers, IFee, OpenseaOfferData, openseaTraitFloor } from "./marketplace/opensea";
import { bidOnBlur, cancelBlurBid, cancelBlurRequest, fetchBlurBid, fetchBlurCollectionStats, fetchBlurTraitFloor, getAccessToken, getAuthMetrics, getTimeoutMetrics } from "./marketplace/blur/bid";
import { bidOnMagiceden, cancelMagicEdenBid, cancelMagicedenRequest, fetchMagicEdenCollectionStats, fetchMagicEdenOffer, fetchMagicEdenTokens, fetchMagicEdenTraitFloor } from "./marketplace/magiceden";
import { CollectionItemsQueryVariables, fetchOpenSeaCollectionItems, fetchOpenseaTraits, getCollectionDetails, getOpenseaFloor } from "./functions";
import mongoose from 'mongoose';
import Task, { ITask, SelectedTraits } from "./models/task.model";
import { Queue, Worker, Job, JobType, JobsOptions, QueueEvents, QueueOptions } from "bullmq";
import Wallet, { IWallet } from "./models/wallet.model";
import redisClient, { createBullMQConnection, checkBullMQConnectionsHealth, closeAllBullMQConnections, getBullMQConnections } from "./utils/redis";
import { BASE_URL, ERC20_MIN_ABI } from "./constants";
import { Contract, ethers, Wallet as Web3Wallet, parseEther, MaxUint256 } from "ethers";
import { DistributedLockManager } from "./utils/lock";
import { safeGetJob, safeMarkJobAsProcessed, recoverStuckJobs } from "./utils/redis-job-fallback";
import { startRedisClusterMonitoring, stopRedisClusterMonitoring } from "./utils/redis-monitor";

import BidLogs from "./models/logs.model";
import PQueue from "p-queue";
import Order from "./models/order.model";
import Approval from "./models/approval.model";
import { USDC, WETH } from "./utils/currencies";
import axios from "axios";
import { decryptPrivateKey, setPassword, getPassword, decryptPassword, recoverPasswordFromRedis, renewPasswordTTL, reEncryptWallet, clearDecryptedKeyCache } from "./utils/crypto";
import { fetchWalletNFTs } from "./utils/nft";
import { logger, ErrorCategory } from "./utils/logger";
import { MarketplaceError, InsufficientBalanceError, TransferRestrictedError, MinimumPriceError, isSoftError, isRetryableError } from "./marketplace/errors";
import WebSocketManager from "./utils/websocket-manager";
import Listing from "./models/listing.model";
import { initializeListingScheduler, shutdownListingScheduler, scheduleListing, cancelListingJob, processListingJob } from "./functions/listingScheduler";
import { initializeExpirationMonitor, stopExpirationMonitor } from "./functions/relistExpired";
import { handleListingPriceUpdate } from "./functions/priceMonitor";
import { createListing } from "./functions/list";
import { BullMQErrorFormatter } from "./utils/bullmq-error-formatter";
import { tradingDisabledCache } from "./utils/trading-disabled-cache";
import { IListing } from "./types/listing.types";
import { BoundedCache } from "./utils/bounded-cache";
import { walletCache } from "./utils/wallet-cache";
import { collectionCache } from "./utils/collection-cache";
import { taskCircuitBreaker } from "./utils/task-circuit-breaker";
import { blurRateLimiter } from "./utils/blur-rate-limiter";
import { approvalCache } from "./utils/approval-cache";
import { blurCompletionTracker } from "./utils/blur-completion-tracker";
import { cancellationTracker } from "./utils/cancellation-tracker";
// import { blurJobCoordinator } from "./utils/blur-job-coordinator";

const MARKETPLACE_WS_URL = "wss://nfttools-ws-proxy.nfttools.io";

export const RATE_LIMIT = Number(process.env.RATE_LIMIT)
export const API_KEY = process.env.API_KEY;
const bidStats: BidCounts = {};
const ownBidCounts: { [marketplace: string]: number } = {};
const currentTokenIds: BottomListings = {};
const currentTraits: CurrentTraits = {}
export const taskStatus: TaskStatus = {};
const bestOffers: BidCounts = {};
const floorPrices: BidCounts = {};
const balances: WalletBalances = {}
export const warningBids: WarningBids = {};
export const warningMessage: WarningMessage = {};
export const errorStats: BidCounts = {};
export const skipStats: BidCounts = {};

// Blur rate limiting is now handled by BlurRateLimiter class
const QUEUE_NAME = 'BIDDING_BOT';
const BLUR_BIDDING_QUEUE = 'BLUR_BIDDING_QUEUE';
const CLEAR_COOLDOWN = 5 * 60 * 1000; // 5 minutes cooldown between queue clears


// Type definitions
interface BidCounts {
  [key: string]: {
    opensea: number;
    magiceden: number;
    blur: number;
  };
}

interface WalletBalances {
  [walletAddress: string]: {
    [contractAddress: string]: {
      opensea: number;
      magiceden: number;
      blur: number;
    };
  };
}

interface BottomListings {
  [key: string]: {
    opensea: string[];
    magiceden: number[];
  }
}

interface CurrentTraits {
  [key: string]: {
    opensea: Record<string, string[]>;
    magiceden: Record<string, string[]>;
    blur: Record<string, string[]>
  }
}

interface TaskStatus {
  [key: string]: {
    status: 'RUNNING' | 'WAITING' | 'CANCELLING' | 'SCHEDULED' | 'COMPLETED' | 'QUEUED' | 'QUEUEING' | 'STARTED' | "STOPPED" | "PROCESSING",
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
    previousState?: 'RUNNING' | 'WAITING' | 'CANCELLING' | 'SCHEDULED' | 'COMPLETED' | 'QUEUED' | 'QUEUEING' | 'STARTED' | "STOPPED" | "PROCESSING";
    cancellingMarketplace?: string;
    expectedCancellationJobs?: number;
    remainingOrders?: number;
    lastCancellationCheck?: number;
  };
}

interface LoopCount {
  [key: string]: number;
}

interface WarningBids {
  [key: string]: {
    opensea: boolean;
    magiceden: boolean;
    blur: boolean;
  };
}

interface WarningMessage {
  [key: string]: {
    opensea: string;
    magiceden: string;
    blur: string;
  };
}


// Dynamically allocate up to 75% of available CPU cores
export const WORKER_COUNT = 1
let traitsToRemove: { [key: string]: string[] } = {}
let removedTokens: { [key: string]: { opensea: string[]; magiceden: string[] } } = {}

// Use BoundedCache instead of Maps for memory-efficient tracking
const seenOrderHashes = new BoundedCache<string, number>(10000, 30 * 60 * 1000); // 10k items, 30 min TTL
const seenBids = new BoundedCache<string, number>(10000, 30 * 60 * 1000); // 10k items, 30 min TTL

// Helper functions for seen items management
function addSeenItem(cache: BoundedCache<string, number>, key: string): void {
  const now = Date.now();
  cache.set(key, now);
  // BoundedCache handles cleanup automatically
}

// Note: Manual .delete() calls after queuing counterbids are intentional
// They allow immediate re-processing of the same order/bid if needed
// The TTL system acts as a safety net for any items not manually deleted

function hasSeenItem(cache: BoundedCache<string, number>, key: string): boolean {
  // BoundedCache.get() handles expiry check automatically
  return cache.has(key);
}
// Set up periodic intervals
let expirationMonitorInterval: NodeJS.Timeout | null = null;
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
  MAGICEDEN: 3,
  BLUR: 2
};

const TRAIT_BID_PRIORITY = {
  OPENSEA: 2,
  MAGICEDEN: 3,
  BLUR: 2
};



const lockManager = new DistributedLockManager({
  lockPrefix: '{marketplace}:',
  defaultTTLSeconds: 60
});

// Create a getter for redis to ensure it's always current

// For internal use

// Export redis as a getter property
export { lockManager };
export { queue };
// Export redis getter to ensure it always returns current connection
export const getRedis = () => redis;
// Removed netEnabled - using isConnected from connectivity monitoring instead

// Helper function to extract taskId from various contexts for logging
function getTaskIdForLog(context: any): string {
  // Check various possible locations for taskId
  return context?.taskId ||
    context?.data?.taskId ||
    context?.data?._id ||
    context?._id ||
    context?.id ||
    'N/A';
}

// Helper function to extract detailed OpenSea error messages
function extractDetailedOpenSeaError(error: any): string {
  // If we have response data, stringify it completely for debugging
  if (error?.response?.data) {
    const status = error.response.status;
    const statusText = error.response.statusText;
    const dataStr = JSON.stringify(error.response.data);

    return `[HTTP ${status}${statusText ? ` ${statusText}` : ''}] ${dataStr}`;
  }

  // Fallback - stringify the entire error object
  return JSON.stringify(error);
}

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
        await new Promise(resolve => setTimeout(resolve, 100));
      }
    }
    this.tokens--;
  }
}


// Initialize worker later in startServer function
let workers: Worker[] = [];

// Track all BullMQ connections for proper cleanup
let bullMQConnections: any[] = [];

const taskTimeouts = new Map<string, NodeJS.Timeout>();
const taskCountdownTimers = new Map<string, NodeJS.Timeout>();
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
    console.log(YELLOW + `[extendScheduledBidLock] No lock found for ${lockKey}` + RESET);
    return false;
  }

  const now = Date.now();

  // Only extend if lock is about to expire (within 2 minutes)
  if (lock.lockUntil && (lock.lockUntil - now) < (2 * 60 * 1000)) {
    lock.lockUntil = now + extensionMs;
    console.log(GREEN + `[extendScheduledBidLock] Extended lock for ${lockKey} by ${extensionMs / 1000}s` + RESET);
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
        console.log(YELLOW + `[TimedLockManager] Stale lock detected for ${lockKey}, elapsed: ${elapsed}ms, force releasing` + RESET);
        this.releaseLock(taskId, marketplace);
      } else {
        // Lock is still valid
        console.log(YELLOW + `[TimedLockManager] Lock already held for ${lockKey}, elapsed: ${elapsed}ms` + RESET);
        return false;
      }
    }

    const now = Date.now();

    // Set up warning timeouts at 50% and 75% of lock duration
    const fiftyPercentWarning = setTimeout(() => {
      console.log(YELLOW + `[TimedLockManager] WARNING: Lock for ${lockKey} has been held for ${Math.round(this.MAX_LOCK_DURATION * 0.5 / 1000)}s (50% of timeout)` + RESET);
    }, this.MAX_LOCK_DURATION * 0.5);

    const seventyFivePercentWarning = setTimeout(() => {
      console.log(YELLOW + `[TimedLockManager] WARNING: Lock for ${lockKey} has been held for ${Math.round(this.MAX_LOCK_DURATION * 0.75 / 1000)}s (75% of timeout)` + RESET);
      console.log(YELLOW + `[TimedLockManager] Task ${taskId} may be stuck - consider investigating` + RESET);
    }, this.MAX_LOCK_DURATION * 0.75);

    // Acquire new lock with auto-release timeout
    const timeout = setTimeout(() => {
      // Wrap everything in a try-catch to ensure we see any errors
      try {
        console.log(RED + `[TimedLockManager] Auto-releasing expired lock for ${lockKey} after ${this.MAX_LOCK_DURATION}ms` + RESET);
        this.releaseLock(taskId, marketplace);

        // When a lock expires, it means the scheduled bid processor got stuck or failed
        // This causes tasks to get stuck at 0% running with parent jobs expecting children
        // Extract taskId from lockKey (format: taskId-marketplace-scheduled) and trigger restart
        const lockParts = lockKey.split('-');
        if (lockParts.length >= 1) {
          const extractedTaskId = lockParts[0];
          console.log(RED + `[TimedLockManager] Lock expired for task ${extractedTaskId}, triggering task restart` + RESET);

          // Use setImmediate to ensure async execution
          setImmediate(async () => {
            try {
              await triggerTaskRestart(extractedTaskId);
              console.log(GREEN + `[TimedLockManager] Successfully triggered restart for task ${extractedTaskId}` + RESET);
            } catch (error) {
              console.error(RED + `[ERROR - TimedLockManager - Task: ${extractedTaskId}] Failed to trigger task restart: ${error}` + RESET);
            }
          });
        }
      } catch (error) {
        console.error(RED + `[ERROR - TimedLockManager - Task: N/A] Error in timeout handler: ${error}` + RESET);
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

    console.log(GREEN + `[TimedLockManager] Acquired lock for ${lockKey}` + RESET);
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
      console.log(GREEN + `[TimedLockManager] Released lock for ${lockKey} after ${Math.round(heldDuration / 1000)}s` + RESET);
    }
  }

  // Extend an existing lock by resetting its timeouts
  extendLock(taskId: string, marketplace: string): boolean {
    const lockKey = `${taskId}-${marketplace}-scheduled`;
    const lock = this.locks.get(lockKey);

    if (!lock) {
      console.log(YELLOW + `[TimedLockManager] Cannot extend lock ${lockKey} - lock not found` + RESET);
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

    console.log(CYAN + `[TimedLockManager] Extending lock for ${lockKey} (was at ${Math.round(percentComplete)}% of timeout)` + RESET);

    // Set up new warning timeouts based on elapsed time
    const remainingTime = this.MAX_LOCK_DURATION - elapsed;

    // Only set up warning timeouts for future events
    if (elapsed < this.MAX_LOCK_DURATION * 0.5) {
      lock.warningTimeouts!.fifty = setTimeout(() => {
        console.log(YELLOW + `[TimedLockManager] WARNING: Extended lock for ${lockKey} has been held for ${Math.round(this.MAX_LOCK_DURATION * 0.5 / 1000)}s (50% of timeout)` + RESET);
      }, this.MAX_LOCK_DURATION * 0.5 - elapsed);
    }

    if (elapsed < this.MAX_LOCK_DURATION * 0.75) {
      lock.warningTimeouts!.seventyFive = setTimeout(() => {
        console.log(YELLOW + `[TimedLockManager] WARNING: Extended lock for ${lockKey} has been held for ${Math.round(this.MAX_LOCK_DURATION * 0.75 / 1000)}s (75% of timeout)` + RESET);
        console.log(YELLOW + `[TimedLockManager] Task ${taskId} may be stuck - consider investigating` + RESET);
      }, this.MAX_LOCK_DURATION * 0.75 - elapsed);
    }

    // Set up new timeout for full duration from original acquisition time
    lock.timeout = setTimeout(() => {
      try {
        console.log(RED + `[TimedLockManager] Auto-releasing extended lock for ${lockKey} after ${this.MAX_LOCK_DURATION}ms total` + RESET);
        this.releaseLock(taskId, marketplace);

        const lockParts = lockKey.split('-');
        if (lockParts.length >= 1) {
          const extractedTaskId = lockParts[0];
          console.log(RED + `[TimedLockManager] Extended lock expired for task ${extractedTaskId}, triggering task restart` + RESET);

          setImmediate(async () => {
            try {
              await triggerTaskRestart(extractedTaskId);
              console.log(GREEN + `[TimedLockManager] Successfully triggered restart for task ${extractedTaskId}` + RESET);
            } catch (error) {
              console.error(RED + `[ERROR - TimedLockManager - Task: ${extractedTaskId}] Failed to trigger task restart: ${error}` + RESET);
            }
          });
        }
      } catch (error) {
        console.error(RED + `[ERROR - TimedLockManager - Task: N/A] Error in extended timeout handler: ${error}` + RESET);
      }
    }, this.MAX_LOCK_DURATION);

    console.log(GREEN + `[TimedLockManager] Lock ${lockKey} extended for another ${Math.round(this.MAX_LOCK_DURATION / 1000)}s` + RESET);
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

  // Ensure status progresses from STARTED to RUNNING
  ensureStatusProgression(taskId: string, fromStatus: string, toStatus: string, timeoutMs: number = 10000) {
    const key = `${taskId}-${fromStatus}-${toStatus}`;

    // Clear existing timeout if any
    this.clearStatusTimeout(key);

    // Set new timeout for status progression
    const timeout = setTimeout(() => {
      const currentStatus = taskStatus[taskId]?.status;

      if (currentStatus === fromStatus) {
        console.log(YELLOW + `[StatusManager] Task ${taskId} stuck in ${fromStatus} for ${timeoutMs}ms, forcing transition to ${toStatus}` + RESET);

        // Force status transition
        const transitioned = setTaskStatus(taskId, toStatus, 'forced progression timeout');

        if (!transitioned) {
          console.error(RED + `[ERROR - StatusManager - Task: ${taskId}] Failed to transition from ${fromStatus} to ${toStatus}` + RESET);

          // If it's stuck in STARTED, we may need to check parent jobs
          if (fromStatus === 'STARTED' && toStatus === 'RUNNING') {
            this.checkAndFixStuckStartedTask(taskId);
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

  // Fix tasks stuck in STARTED status
  async checkAndFixStuckStartedTask(taskId: string) {
    console.log(CYAN + `[StatusManager] Checking stuck STARTED task ${taskId}` + RESET);

    const task = activeTasks.get(taskId);
    if (!task) {
      console.log(YELLOW + `[StatusManager] Task ${taskId} not found in activeTasks` + RESET);
      return;
    }

    // Check if any parent jobs exist
    const marketplaces = ['opensea', 'magiceden', 'blur'];
    let hasAnyParentJobs = false;
    let totalJobs = 0;

    for (const mp of marketplaces) {
      if (task.selectedMarketplaces.map(m => m.toLowerCase()).includes(mp)) {
        const parentJobId = getParentJobId(taskId, mp);
        const parentJob = parentJobs.get(parentJobId);

        if (parentJob) {
          hasAnyParentJobs = true;
          totalJobs += parentJob.totalJobs || 0;
        }
      }
    }

    if (hasAnyParentJobs && totalJobs > 0) {
      // Parent jobs exist, safe to transition to RUNNING
      console.log(GREEN + `[StatusManager] Found ${totalJobs} jobs, transitioning ${taskId} to RUNNING` + RESET);
      setTaskStatus(taskId, 'RUNNING', 'recovered from stuck STARTED');
    } else {
      // No parent jobs found, task might need to be restarted
      console.log(YELLOW + `[StatusManager] No parent jobs found for ${taskId}, may need restart` + RESET);

      // Set to SCHEDULED to trigger next loop
      setTaskStatus(taskId, 'SCHEDULED', 'no jobs found, scheduling next loop');
      await scheduleNextLoop(taskId);
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

// Create global instance
const taskStatusManager = new TaskStatusManager();

// Initialize queue and queueEvents with Redis connection
// These will be initialized in startServer()
let queue: Queue;
let queueEvents: QueueEvents;
let QUEUE_OPTIONS: QueueOptions;

// Blur queue variables
export let blurQueue: Queue;
let blurQueueEvents: QueueEvents;

// Redis connection for internal use (not BullMQ)
let redis = redisClient.getClient();

export const MAGENTA = '\x1b[35m';
export const BLUE = '\x1b[34m';
export const RESET = '\x1b[0m';
export const GREEN = '\x1b[32m';
export const YELLOW = '\x1b[33m';
export const RED = '\x1b[31m';
export const GOLD = '\x1b[33m';
export const CYAN = '\x1b[36m';

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

  // Removed console.log for task indexes
}

// Remove a single task from the indexes
function removeTaskFromIndexes(task: ITask): void {
  const contractKey = task.contract?.contractAddress?.toLowerCase();
  const slugKey = task.contract?.slug?.toLowerCase();

  if (contractKey && contractToTasksMap.has(contractKey)) {
    const contractTasks = contractToTasksMap.get(contractKey)!;
    contractTasks.delete(task);
    if (contractTasks.size === 0) {
      contractToTasksMap.delete(contractKey);
    }
  }

  if (slugKey && slugToTasksMap.has(slugKey)) {
    const slugTasks = slugToTasksMap.get(slugKey)!;
    slugTasks.delete(task);
    if (slugTasks.size === 0) {
      slugToTasksMap.delete(slugKey);
    }
  }

  // Removed console.log for task indexes
}

export const OPENSEA_SCHEDULE = "OPENSEA_SCHEDULE"
export const OPENSEA_COLLECTION_BID = "OPENSEA_COLLECTION_BID"
export const OPENSEA_TOKEN_BID = "OPENSEA_TOKEN_BID"
export const OPENSEA_TRAIT_BID = "OPENSEA_TRAIT_BID"
export const BLUR_SCHEDULE = "BLUR_SCHEDULE"
export const MAGICEDEN_SCHEDULE = "MAGICEDEN_SCHEDULE"
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
const CANCEL_OPENSEA_BID = "CANCEL_OPENSEA_BID"
const CANCEL_MAGICEDEN_BID = "CANCEL_MAGICEDEN_BID"
const CANCEL_BLUR_BID = "CANCEL_BLUR_BID"

// Retry configuration for cancellation jobs
const CANCELLATION_JOB_RETRY_ATTEMPTS = 3; // 3 attempts total (1 initial + 2 retries)
const CANCELLATION_JOB_RETRY_DELAY = 2000;
// Helper function to create cancellation job options with retry configuration
function getCancellationJobOptions(taskId: string, orderId: string, jobType: string) {
  return {
    jobId: `${jobType}-${taskId}-${orderId}`,
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
const STANDARD_INTERVAL = 60000; // 60 seconds - standard interval for most timers
const HEALTH_CHECK_INTERVAL = process.env.NODE_ENV === 'development' ? 5000 : 60000; // 5 seconds in dev, 60 seconds in prod
// Initialize with default marketplaces - will be updated by WebSocket
let enabledMarketplaces: string[] = ['opensea', 'magiceden', 'blur']
console.log(CYAN + `[Server] Initialized enabledMarketplaces with defaults: ${enabledMarketplaces.join(', ')}` + RESET)
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
      connectSrc: ["'self'", "wss:", "https:", "https://nfttools-ws-proxy.nfttools.io", "https://updates.nfttools.io"],
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
let wsManager: WebSocketManager;
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

// Track subscriptions by marketplace and collection slug
const clients = new Set<WebSocket>();

const walletsArr: string[] = []
const MIN_BID_DURATION = 60

// Redis cluster health monitoring
let redisClusterFailures = 0;
let lastRedisClusterRecovery = 0;
const REDIS_CLUSTER_FAILURE_THRESHOLD = 2; // Consecutive failures before recovery
const REDIS_RECOVERY_COOLDOWN = 60000; // 1 minute cooldown between recovery attempts

// Helper to detect Redis cluster errors
function isRedisClusterError(error: any): boolean {
  const errorMessage = error instanceof Error ? error.message : String(error);
  return errorMessage.includes('CLUSTERDOWN') ||
    errorMessage.includes('Too many Cluster redirections') ||
    errorMessage.includes('MOVED') ||
    errorMessage.includes('ASK');
}

function cleanupMemory() {
  if (global.gc) {
    global.gc();
  }
}

async function checkRedisClusterHealth(): Promise<boolean> {
  try {
    const redis = redisClient.getClient();
    if (!redis) {
      return false;
    }

    // Check cluster info using proper command
    const clusterInfo = await redis.cluster('INFO') as string;
    const clusterState = clusterInfo.split('\n')
      .find((line: string) => line.startsWith('cluster_state:'))
      ?.split(':')[1]
      ?.trim();

    return clusterState === 'ok';
  } catch (error) {
    // If we can't even check cluster info, it's definitely not healthy
    return false;
  }
}

async function handleRedisClusterFailure() {
  const now = Date.now();

  // Check cooldown
  if (now - lastRedisClusterRecovery < REDIS_RECOVERY_COOLDOWN) {
    const cooldownRemaining = Math.ceil((REDIS_RECOVERY_COOLDOWN - (now - lastRedisClusterRecovery)) / 1000);
    console.log(YELLOW + `Redis cluster recovery on cooldown (${cooldownRemaining}s remaining)` + RESET);
    return;
  }

  console.log(RED + '🚨 REDIS CLUSTER FAILURE DETECTED - Initiating recovery 🚨' + RESET);
  console.log(RED + `Consecutive failures: ${redisClusterFailures}` + RESET);

  try {
    lastRedisClusterRecovery = now;
    await clearQueueAndRestartJobs();

    // Reset failure counter after successful recovery
    redisClusterFailures = 0;
    console.log(GREEN + '✅ Redis cluster recovery completed' + RESET);
  } catch (error) {
    console.error(RED + '[ERROR - Redis Cluster Recovery - Task: N/A] Failed to recover from Redis cluster failure:' + RESET, error);
    // Don't reset failure counter if recovery fails
  }
}

async function monitorHealth() {
  try {
    const isRedisHealthy = await checkRedisClusterHealth();
    if (!isRedisHealthy) {
      redisClusterFailures++;
      console.log(RED + `⚠️  Redis cluster unhealthy - failure count: ${redisClusterFailures}/${REDIS_CLUSTER_FAILURE_THRESHOLD}` + RESET);

      if (redisClusterFailures >= REDIS_CLUSTER_FAILURE_THRESHOLD) {
        await handleRedisClusterFailure();
      }
      return; // Skip rest of health check if Redis is down
    } else {
      // Reset failure counter if Redis is healthy
      if (redisClusterFailures > 0) {
        console.log(GREEN + '✅ Redis cluster recovered - resetting failure counter' + RESET);
        redisClusterFailures = 0;
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

    // Log high memory usage but don't pause the queue
    if (heapPercentOfMax > 75) {
      console.log(RED + `⚠️  HIGH MEMORY USAGE: ${heapUsedMB.toFixed(0)}MB / ${MAX_HEAP_MB}MB (${heapPercentOfMax.toFixed(1)}% of max heap) ⚠️` + RESET);
      console.log(RED + `⚠️  Heap allocation: ${heapTotalMB.toFixed(0)}MB (${heapPercentOfTotal.toFixed(1)}% used) ⚠️` + RESET);
      cleanupMemory();
    }

    const totalJobs = Object.values(counts).reduce((a, b) => a + b, 0);

    console.log('\n📊 ═══════ Health Monitor Stats ═══════ 📊');

    console.log(BLUE + '\n💾 Memory Usage:' + RESET);
    Object.entries(memoryStats).forEach(([key, value]) => {
      console.log(`${key}:${value}`);
    });

    console.log(BLUE + '\n📋 Queue Status:' + RESET);

    // Show password status first
    const hasPassword = !!getPassword();
    if (!hasPassword) {
      console.log(RED + '🔒 PASSWORD REQUIRED - Operations are paused' + RESET);
      console.log(YELLOW + '   Set password via POST /api/auth/password' + RESET);
    } else {
      console.log(GREEN + '🔓 Password is set' + RESET);
    }

    // Show paused status prominently
    if (isPaused) {
      console.log(RED + '⏸️  QUEUE IS PAUSED' + RESET);
    } else {
      console.log(GREEN + '▶️  QUEUE IS RUNNING' + RESET);
    }

    // Show waiting job monitor status
    if (counts.waiting > 0) {
      console.log(YELLOW + '⚡ Waiting Job Monitor: Active (failing waiting jobs for retry)' + RESET);
    }

    Object.entries(counts).forEach(([key, value]) => {
      const color = key === 'waiting' && value > 0 ? RED :
        value > RATE_LIMIT * 2 ? RED :
          value > RATE_LIMIT ? YELLOW : GREEN;
      console.log(`${key}: ${color}${value}${RESET}`);
    });
    console.log(`Total Jobs: ${totalJobs}`);

    // Show trading disabled cache stats
    const disabledStats = tradingDisabledCache.getStats();
    if (disabledStats.total > 0) {
      console.log(YELLOW + '\n🚫 Trading Disabled Cache:' + RESET);
      console.log(`Total Cached: ${disabledStats.total}`);
      Object.entries(disabledStats.byMarketplace).forEach(([marketplace, count]) => {
        console.log(`${marketplace}: ${count}`);
      });

      // Show cache details
      const cacheDetails = tradingDisabledCache.getCacheStats();
      console.log(`Cache TTL: ${Math.floor(cacheDetails.ttl / 1000 / 60)} minutes`);
      if (cacheDetails.oldestEntry) {
        const ageMinutes = Math.floor((Date.now() - cacheDetails.oldestEntry) / 1000 / 60);
        console.log(`Oldest entry: ${ageMinutes} minutes ago`);
      }
    }

    updateAllBidRates();

  } catch (error) {
    // Log as debug level to avoid sending to Axiom
    logger.logDebug(ErrorCategory.GENERAL, "Health monitoring failed", {
      error: error as Error,
      uptime: process.uptime(),
      memoryUsage: process.memoryUsage()
    });
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
      console.log(GREEN + '✅ Password recovered from Redis - resuming operations' + RESET);
      await resumeQueueAndWorkers();
    }
  } catch (error) {
    console.error(RED + '[ERROR - Password Check - Task: N/A] Error checking password:' + RESET, error);
  } finally {
    isCheckingPassword = false;
  }
}

// Note: Password auto-recovery checker will be started in startServer() after Redis is ready

// Queue monitor will be initialized after queues are created in startServer()

// Add this near the top with other global variables
export const activeTasks: Map<string, ITask> = new Map();

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
    // Pause both queues before obliterating
    await queue.pause();
    await blurQueue.pause();

    // Wait for queues to fully pause and check status
    let retries = 10;
    while (retries > 0) {
      const isPaused = await queue.isPaused();
      const isBlurPaused = await blurQueue.isPaused();
      if (isPaused && isBlurPaused) break;
      await new Promise(resolve => setTimeout(resolve, 500));
      retries--;
    }

    // Additional safety wait
    await new Promise(resolve => setTimeout(resolve, 1000));

    // Obliterate both queues with force option
    await queue.obliterate({ force: true });
    await blurQueue.obliterate({ force: true });

    // Check if password is available before resuming queues
    const password = getPassword();
    if (password) {
      // Resume both queues only if password is available
      await queue.resume();
      await blurQueue.resume();
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
      console.log(YELLOW + 'Waiting 5 seconds before restarting scheduled loop...' + RESET);
      await new Promise(resolve => setTimeout(resolve, 5000));

      // Start the scheduled loop
      console.log(GREEN + 'Starting scheduled loop...' + RESET);
      await runScheduledLoop();
    } else {
      console.log(YELLOW + 'Scheduled loop not started - waiting for password' + RESET);
    }

  } catch (error) {
    console.error(RED + `[ERROR - Queue Clear - Task: N/A] Error during clearQueueAndRestartJobs: ${error}` + RESET);
    throw error;
  }
}

// Network connectivity monitoring
let isConnected = true;
let connectivityLostAt: number | null = null;

// Export network status for use in other modules if needed
export const getNetworkStatus = () => isConnected;

async function checkInternetConnectivity(): Promise<boolean> {
  try {
    // Use Google's connectivity check endpoint - designed for this purpose
    const response = await axios.get('http://connectivitycheck.gstatic.com/generate_204', {
      timeout: 5000
    });
    // This endpoint returns 204 No Content when internet is available
    return response.status === 204;
  } catch (error) {
    return false;
  }
}

async function handleConnectivityRestored() {
  console.log(GREEN + '\n🌐 Internet connectivity restored!' + RESET);

  // Calculate how long connectivity was lost
  const downtime = connectivityLostAt ? Date.now() - connectivityLostAt : 0;
  const downtimeMinutes = Math.floor(downtime / 60000);

  if (downtime > 0) {
    console.log(YELLOW + `Connectivity was lost for ${downtimeMinutes} minutes ${Math.floor((downtime % 60000) / 1000)} seconds` + RESET);
  }

  // Resume both queues first
  try {
    await queue.resume();
    await blurQueue.resume();
    console.log(GREEN + 'Queues resumed after network restoration' + RESET);
  } catch (err) {
    console.error(RED + '[ERROR - Queue Resume - Task: N/A] Failed to resume queues:' + RESET, err);
  }

  // Only clear and restart jobs if connectivity was lost for more than 1 minute
  if (downtime > 60000) {
    console.log(YELLOW + 'Connectivity lost for more than 1 minute, clearing queue and restarting jobs...' + RESET);
    await clearQueueAndRestartJobs();
  } else {
    console.log(GREEN + 'Brief connectivity loss detected, skipping queue restart' + RESET);
  }

  // Always reconnect WebSocket
  if (wsManager) {
    console.log('Reconnecting WebSocket...');
    await connectWebSocket();
  }

  // Always reinitialize listing scheduler
  if (wsManager) {
    await initializeListingScheduler(wsManager);
    console.log('📋 Listing scheduler reinitialized');
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
  console.log(CYAN + `Clearing ${recentItemSoldEvents.size} cached ITEM_SOLD events` + RESET);
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
      console.error(RED + '[ERROR - Connectivity Monitor - Task: N/A] \n⚠️  Internet connectivity lost!' + RESET);

      // Pause queue operations during network outage
      try {
        await queue.pause();
        await blurQueue.pause();
        console.log(YELLOW + 'Queues paused due to network loss' + RESET);
      } catch (err) {
        console.error(RED + '[ERROR - Queue Pause - Task: N/A] Failed to pause queues:' + RESET, err);
      }
    }
  }, STANDARD_INTERVAL);

  console.log('🔌 Internet connectivity monitoring started');
}

function stopConnectivityMonitoring() {
  if (connectivityCheckInterval) {
    clearInterval(connectivityCheckInterval);
    connectivityCheckInterval = null;
    console.log('Internet connectivity monitoring stopped');
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

    // Task initialization complete
  } catch (error) {
    logger.logDatabaseError("Failed to fetch current tasks", {
      error: error as Error,
      taskCount: currentTasks.length
    });
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
    console.log(YELLOW + 'No password set - pausing queues and workers...' + RESET);

    // Pause both queues
    await queue.pause();
    await blurQueue.pause();

    // Pause all workers
    if (workers.length > 0) {
      await Promise.all(workers.map(worker => worker.pause()));
    }

    console.log(YELLOW + 'All queues and workers paused. Waiting for password...' + RESET);
  } catch (error) {
    console.error(RED + '[ERROR - Queue Pause - Task: N/A] Error pausing queues and workers:' + RESET, error);
  }
}

async function resumeQueueAndWorkers() {
  try {
    console.log(GREEN + 'Password set - resuming queues and workers...' + RESET);

    // Resume both queues
    await queue.resume();
    await blurQueue.resume();

    // Resume all workers
    if (workers.length > 0) {
      await Promise.all(workers.map(worker => worker.resume()));
    }

    console.log(GREEN + 'All queues and workers resumed.' + RESET);

    // Retry any failed jobs that were due to missing password
    try {
      const failedJobs = await queue.getFailed();
      const blurFailedJobs = await blurQueue.getFailed();

      // Retry jobs that failed due to missing password
      const passwordFailedJobs = failedJobs.filter(job =>
        job.failedReason?.includes('No password available')
      );

      const blurPasswordFailedJobs = blurFailedJobs.filter(job =>
        job.failedReason?.includes('No password available')
      );

      if (passwordFailedJobs.length > 0) {
        console.log(GREEN + `Retrying ${passwordFailedJobs.length} main queue jobs that failed due to missing password` + RESET);
        await Promise.all(passwordFailedJobs.map(job => job.retry()));
      }

      if (blurPasswordFailedJobs.length > 0) {
        console.log(GREEN + `Retrying ${blurPasswordFailedJobs.length} blur queue jobs that failed due to missing password` + RESET);
        await Promise.all(blurPasswordFailedJobs.map(job => job.retry()));
      }
    } catch (retryError) {
      console.error(RED + '[ERROR - Job Retry - Task: N/A] Error retrying failed jobs:' + RESET, retryError);
    }
  } catch (error) {
    console.error(RED + '[ERROR - Queue Resume - Task: N/A] Error resuming queues and workers:' + RESET, error);
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

// Queue drain monitoring
const QUEUE_BACKLOG_THRESHOLD = (RATE_LIMIT || 100) * 2; // 2x rate limit
const QUEUE_STAGNATION_TIME = 3 * 60 * 1000; // 3 minutes
const DRAIN_CHECK_INTERVAL = 30 * 1000; // Check every 30 seconds
let queueDrainInterval: NodeJS.Timeout | null = null;
const queueProcessingHistory = new Map<number, { processed: number; waiting: number }>();
let lastQueueClearForBacklog = 0;

function startWorkerHealthMonitoring() {
  console.log(GREEN + '🏥 Starting worker health monitoring...' + RESET);

  // Initial health check
  checkWorkerHealth();

  // Set up periodic health checks
  workerHealthInterval = setInterval(() => {
    checkWorkerHealth();
  }, WORKER_HEALTH_CHECK_INTERVAL);

  // Start queue drain monitoring
  startQueueDrainMonitoring();
}


function startQueueDrainMonitoring() {
  console.log(GREEN + '💧 Starting queue drain monitoring...' + RESET);

  // Initial drain check
  monitorQueueDrain();

  // Set up periodic drain checks
  queueDrainInterval = setInterval(() => {
    monitorQueueDrain();
  }, DRAIN_CHECK_INTERVAL);
}

async function checkWorkerHealth() {
  try {
    // Check Redis connections health
    const connectionHealth = await checkBullMQConnectionsHealth();
    const unhealthyConnections = Object.entries(connectionHealth)
      .filter(([_, status]) => !status.healthy);

    if (unhealthyConnections.length > 0) {
      console.error(RED + `[ERROR - Health Check - Task: N/A] ❌ Unhealthy Redis connections detected: ${unhealthyConnections.length}` + RESET);
      unhealthyConnections.forEach(([id, status]) => {
        console.error(RED + `[ERROR - Health Check - Task: N/A]   - ${id}: ${status.error}` + RESET);
      });
    }

    // Check worker status
    const workerStatuses = await Promise.all(
      workers.map(async (worker, index) => {
        try {
          // Check if worker is running
          const isRunning = worker.isRunning();
          const isPaused = worker.isPaused();

          return {
            index,
            running: isRunning,
            paused: isPaused,
            healthy: isRunning && !isPaused
          };
        } catch (error) {
          return {
            index,
            running: false,
            paused: false,
            healthy: false,
            error: error instanceof Error ? error.message : 'Unknown error'
          };
        }
      })
    );

    const unhealthyWorkers = workerStatuses.filter(status => !status.healthy);

    if (unhealthyWorkers.length > 0) {
      console.error(RED + `[ERROR - Health Check - Task: N/A] ❌ Unhealthy workers detected: ${unhealthyWorkers.length}/${workers.length}` + RESET);

      let shouldRestartAll = false;

      // Attempt to restart unhealthy workers
      for (const workerStatus of unhealthyWorkers) {
        if ('error' in workerStatus && workerStatus.error) {
          console.error(RED + `[ERROR - Health Check - Task: N/A]   Worker ${workerStatus.index}: ${workerStatus.error}` + RESET);

          // Track the failure
          const thresholdExceeded = trackWorkerFailure(workerStatus.index, workerStatus.error);
          if (thresholdExceeded) {
            shouldRestartAll = true;
          }
        } else if (workerStatus.paused) {
          console.log(YELLOW + `  Worker ${workerStatus.index} is paused, attempting to resume...` + RESET);
          try {
            await workers[workerStatus.index].resume();
            console.log(GREEN + `  ✅ Worker ${workerStatus.index} resumed successfully` + RESET);
          } catch (error) {
            console.error(RED + `[ERROR - Health Check - Task: N/A]   Failed to resume worker ${workerStatus.index}: ${error}` + RESET);

            // Track resume failure
            const thresholdExceeded = trackWorkerFailure(workerStatus.index, `Resume failed: ${error}`);
            if (thresholdExceeded) {
              shouldRestartAll = true;
            }
          }
        } else if (!workerStatus.running) {
          // Worker is not running at all
          console.error(RED + `[ERROR - Health Check - Task: N/A]   Worker ${workerStatus.index} is not running` + RESET);
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
          console.log(RED + '🚨 WORKER FAILURE THRESHOLD EXCEEDED 🚨' + RESET);
          console.log(YELLOW + 'Multiple workers have failed. Initiating full system restart...' + RESET);

          // Log failure details
          workerFailures.forEach((failure, index) => {
            if (failure.count >= WORKER_FAILURE_THRESHOLD) {
              console.log(RED + `Worker ${index}: ${failure.count} failures, last error types:` + RESET);
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
          console.log(YELLOW + `Worker restart needed but cooldown active (${cooldownRemaining}s remaining)` + RESET);
        }
      }
    }

    // Check queue status
    const [mainQueueCounts, blurQueueCounts] = await Promise.all([
      queue.getJobCounts(),
      blurQueue.getJobCounts()
    ]);

    // Log health summary
    const healthSummary = {
      connections: {
        total: Object.keys(connectionHealth).length,
        healthy: Object.keys(connectionHealth).length - unhealthyConnections.length
      },
      workers: {
        total: workers.length,
        healthy: workers.length - unhealthyWorkers.length
      },
      queues: {
        main: {
          waiting: mainQueueCounts.waiting || 0,
          active: mainQueueCounts.active || 0,
          failed: mainQueueCounts.failed || 0
        },
        blur: {
          waiting: blurQueueCounts.waiting || 0,
          active: blurQueueCounts.active || 0,
          failed: blurQueueCounts.failed || 0
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
      console.log(CYAN + '\n📊 Worker Health Check Summary:' + RESET);
      console.log(CYAN + `  Connections: ${healthSummary.connections.healthy}/${healthSummary.connections.total} healthy` + RESET);
      console.log(CYAN + `  Workers: ${healthSummary.workers.healthy}/${healthSummary.workers.total} healthy` + RESET);
      console.log(CYAN + `  Main Queue: ${healthSummary.queues.main.waiting} waiting, ${healthSummary.queues.main.active} active` + RESET);
      console.log(CYAN + `  Blur Queue: ${healthSummary.queues.blur.waiting} waiting, ${healthSummary.queues.blur.active} active` + RESET);

      if (oldConnections.length > 0) {
        console.log(YELLOW + `  ⚠️  ${oldConnections.length} connections older than 1 hour:` + RESET);
        oldConnections.forEach(conn => {
          console.log(YELLOW + `    - ${conn.id}: ${Math.floor(conn.age / 60)} minutes old` + RESET);
        });
      }
    }

    // Log detailed connection pool stats to monitoring system
    await logger.logInfo(ErrorCategory.GENERAL, 'Connection pool statistics', {
      totalConnections: connectionPoolStats.length,
      healthyConnections: healthSummary.connections.healthy,
      unhealthyConnections: unhealthyConnections.length,
      oldestConnectionAge: Math.max(...connectionAges.map(c => c.age)),
      connectionDetails: connectionAges
    });

  } catch (error) {
    console.error(RED + '[ERROR - Worker Health Check - Task: N/A] Error during worker health check:' + RESET, error);
  }
}

function stopWorkerHealthMonitoring() {
  if (workerHealthInterval) {
    clearInterval(workerHealthInterval);
    workerHealthInterval = null;
    console.log('Worker health monitoring stopped');
  }
  if (queueDrainInterval) {
    clearInterval(queueDrainInterval);
    queueDrainInterval = null;
    console.log('Queue drain monitoring stopped');
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

  console.error(RED + `[ERROR - Worker ${workerIndex} - Task: N/A] Failure tracked: ${failure.count}/${WORKER_FAILURE_THRESHOLD} in window` + RESET);
  console.error(RED + `[ERROR - Worker ${workerIndex} - Task: N/A] Error: ${error}` + RESET);

  return failure.count >= WORKER_FAILURE_THRESHOLD;
}

// Monitor queue drain and detect backlogs
async function monitorQueueDrain() {
  try {
    if (!queue || !blurQueue) {
      return;
    }

    const now = Date.now();
    const [mainCounts, blurCounts] = await Promise.all([
      queue.getJobCounts(),
      blurQueue.getJobCounts()
    ]);

    const totalWaiting = (mainCounts.waiting || 0) + (blurCounts.waiting || 0);
    const totalActive = (mainCounts.active || 0) + (blurCounts.active || 0);
    const totalCompleted = (mainCounts.completed || 0) + (blurCounts.completed || 0);

    // Store current state for history
    queueProcessingHistory.set(now, { processed: totalCompleted, waiting: totalWaiting });

    // Clean old history (keep 5 minutes)
    const historyLimit = now - (5 * 60 * 1000);
    for (const [timestamp] of queueProcessingHistory) {
      if (timestamp < historyLimit) {
        queueProcessingHistory.delete(timestamp);
      }
    }

    // Check for backlog conditions
    let shouldClearQueue = false;
    let clearReason = '';

    // Condition 1: High waiting count with no active workers
    // if (totalWaiting > QUEUE_BACKLOG_THRESHOLD && totalActive === 0) {
    //   shouldClearQueue = true;
    //   clearReason = `High waiting jobs (${totalWaiting}) with no active workers`;
    // }

    // Condition 2: Check for stagnation (no progress in processing)
    const historyEntries = Array.from(queueProcessingHistory.entries()).sort((a, b) => a[0] - b[0]);
    if (historyEntries.length >= 3) {
      const recentHistory = historyEntries.slice(-3);
      const noProgress = recentHistory.every((entry, idx) => {
        if (idx === 0) return true;
        const prevEntry = recentHistory[idx - 1];
        return entry[1].processed === prevEntry[1].processed && entry[1].waiting > 0;
      });

      // if (noProgress && totalWaiting > 0) {
      //   const stagnationTime = now - recentHistory[0][0];
      //   if (stagnationTime > QUEUE_STAGNATION_TIME) {
      //     shouldClearQueue = true;
      //     clearReason = `Queue stagnation detected - no progress for ${Math.round(stagnationTime / 1000)}s`;
      //   }
      // }
    }

    // Condition 3: Extreme backlog
    // if (totalWaiting > QUEUE_BACKLOG_THRESHOLD * 3) {
    //   shouldClearQueue = true;
    //   clearReason = `Extreme backlog detected: ${totalWaiting} waiting jobs`;
    // }

    // Check cooldown before clearing
    if (shouldClearQueue && (now - lastQueueClearForBacklog) > CLEAR_COOLDOWN) {
      console.log(RED + '🚨 QUEUE BACKLOG DETECTED 🚨' + RESET);
      console.log(RED + `Reason: ${clearReason}` + RESET);
      console.log(YELLOW + `Queue stats - Waiting: ${totalWaiting}, Active: ${totalActive}` + RESET);
      console.log(YELLOW + 'Initiating queue clear and restart...' + RESET);

      lastQueueClearForBacklog = now;
      await clearQueueAndRestartJobs();
    } else if (shouldClearQueue) {
      const cooldownRemaining = Math.ceil((CLEAR_COOLDOWN - (now - lastQueueClearForBacklog)) / 1000);
      console.log(YELLOW + `Queue backlog detected but cooldown active (${cooldownRemaining}s remaining)` + RESET);
    }

    // Log drain status if high waiting count
    if (totalWaiting > RATE_LIMIT) {
      const processingRate = historyEntries.length >= 2
        ? (historyEntries[historyEntries.length - 1][1].processed - historyEntries[0][1].processed) / (historyEntries.length - 1)
        : 0;

      console.log(YELLOW + `[Queue Drain Monitor] High waiting: ${totalWaiting}, Active: ${totalActive}, Rate: ${processingRate.toFixed(1)} jobs/check` + RESET);
    }

  } catch (error) {
    console.error(RED + '[ERROR - Queue Drain Monitor - Task: N/A] Error in queue drain monitoring:' + RESET, error);
  }
}

async function startServer() {
  try {
    // Development mode setup
    if (process.env.NODE_ENV === 'development') {
      // Make processAllActiveListings available globally in dev mode
      (global as any).processAllActiveListings = processAllActiveListings;
    }

    // Wait for Redis to be ready
    while (!redisClient.isReady()) {
      await new Promise(resolve => setTimeout(resolve, 100));
    }


    // Create separate BullMQ connections for each component
    console.log('Creating separate Redis connections for BullMQ components...');

    let mainQueueConnection: any;
    let mainQueueEventsConnection: any;
    let blurQueueConnection: any;
    let blurQueueEventsConnection: any;

    try {
      // Main queue connection
      mainQueueConnection = await createBullMQConnection('main-queue');
      bullMQConnections.push(mainQueueConnection);
      console.log(GREEN + '✅ Created main queue connection' + RESET);

      // Main queue events connection
      mainQueueEventsConnection = await createBullMQConnection('main-queue-events');
      bullMQConnections.push(mainQueueEventsConnection);
      console.log(GREEN + '✅ Created main queue events connection' + RESET);

      // Blur queue connection
      blurQueueConnection = await createBullMQConnection('blur-queue');
      bullMQConnections.push(blurQueueConnection);
      console.log(GREEN + '✅ Created blur queue connection' + RESET);

      // Blur queue events connection
      blurQueueEventsConnection = await createBullMQConnection('blur-queue-events');
      bullMQConnections.push(blurQueueEventsConnection);
      console.log(GREEN + '✅ Created blur queue events connection' + RESET);

      // Verify all connections are healthy before proceeding
      const healthCheck = await checkBullMQConnectionsHealth();
      const unhealthyConnections = Object.entries(healthCheck).filter(([_, status]) => !status.healthy);

      if (unhealthyConnections.length > 0) {
        console.error(RED + '[ERROR - BullMQ Init - Task: N/A] Unhealthy Redis connections detected:' + RESET);
        unhealthyConnections.forEach(([id, status]) => {
          console.error(RED + `[ERROR - Health Check - Task: N/A]   - ${id}: ${status.error}` + RESET);
        });
        throw new Error('One or more Redis connections are unhealthy');
      }

      console.log(GREEN + '✅ All Redis connections verified healthy' + RESET);
    } catch (error) {
      console.error(RED + '[ERROR - BullMQ Init - Task: N/A] Failed to create BullMQ connections:' + RESET, error);
      // Close any connections that were created
      await closeAllBullMQConnections();
      throw new Error('Failed to initialize BullMQ connections');
    }

    // Initialize queue options with main queue connection
    QUEUE_OPTIONS = {
      prefix: '{bull}',
      connection: mainQueueConnection,
      defaultJobOptions: {
        removeOnComplete: true,
        removeOnFail: true,
        attempts: 1,  // Prevent BullMQ's default 3 retries to avoid duplicate job processing
      }
    };

    // Initialize queue and events with their own connections
    queue = new Queue(QUEUE_NAME, QUEUE_OPTIONS);
    queueEvents = new QueueEvents(QUEUE_NAME, { connection: mainQueueEventsConnection, prefix: '{bull}' });
    blurQueueEvents = new QueueEvents(BLUR_BIDDING_QUEUE, { connection: blurQueueEventsConnection, prefix: '{bull}' });

    // Initialize Blur queue with its own connection
    blurQueue = new Queue(BLUR_BIDDING_QUEUE, {
      prefix: '{bull}',
      connection: blurQueueConnection,
      defaultJobOptions: {
        removeOnComplete: true,
        removeOnFail: true,
        attempts: 1,
      }
    });

    // Initialize queue event handlers
    initializeQueueEventHandlers();
    const concurrencyPerWorker = Number(RATE_LIMIT) / WORKER_COUNT;

    for (let i = 0; i < WORKER_COUNT; i++) {
      // Create a separate connection for each worker
      const workerConnection = await createBullMQConnection(`main-worker-${i + 1}`);
      bullMQConnections.push(workerConnection);
      console.log(GREEN + `✅ Created connection for worker ${i + 1}/${WORKER_COUNT}` + RESET);

      const worker = new Worker(
        QUEUE_NAME,
        async (job) => {
          const jobTimeout = job.name.endsWith('_SCHEDULE') ? 900000 : 60000; // 15 minutes for scheduled jobs, 60 seconds for others
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
          prefix: '{bull}',
          concurrency: concurrencyPerWorker,
          lockDuration: 90000, // 90 seconds lock (1.5x job timeout)
          stalledInterval: 90000, // Check for stalled jobs every 90 seconds
          maxStalledCount: 1 // Move to failed after 1 stall
        }
      );

      // Add error handlers for main workers
      worker.on('error', async (error) => {
        console.error(RED + `[ERROR - Worker ${i + 1} - Task: N/A] Worker error:` + RESET, error);
        trackWorkerFailure(i, `Worker error: ${error.message || String(error)}`);

        // Check if this is a Redis cluster error
        if (isRedisClusterError(error)) {
          console.log(RED + '🚨 Redis cluster error detected in worker' + RESET);
          redisClusterFailures++;

          if (redisClusterFailures >= REDIS_CLUSTER_FAILURE_THRESHOLD) {
            await handleRedisClusterFailure();
          }
        }

        logger.logGeneralError(`Main worker ${i + 1} error`, {
          error: error,
          errorMessage: error.message || String(error),
          errorStack: error.stack,
          workerIndex: i
        });
      });

      worker.on('failed', (job, error) => {
        const taskId = getTaskIdForLog(job);
        console.error(RED + `[ERROR - Worker ${i + 1} - Task: ${taskId}] Job ${job?.id} failed:` + RESET, error.message || error);

        // Handle failed cancellation jobs - track them as completed
        if (job && job.name && (job.name === CANCEL_OPENSEA_BID || job.name === CANCEL_MAGICEDEN_BID || job.name === CANCEL_BLUR_BID)) {
          const { taskId } = job.data;
          const marketplace = job.name === CANCEL_OPENSEA_BID ? 'opensea' :
            job.name === CANCEL_MAGICEDEN_BID ? 'magiceden' : 'blur';

          console.log(YELLOW + `[Worker ${i + 1}] Cancellation job failed after all retries - marking as complete for tracking` + RESET);

          // Record the failed cancellation as a completion to ensure we reach 100%
          cancellationTracker.recordCompletion(taskId, 1, marketplace);
          updateTaskCancellationPercentage(taskId).catch(err => {
            const failedTaskId = getTaskIdForLog(job);
            console.error(RED + `[ERROR - Cancellation Tracking - Task: ${failedTaskId}] Failed to update cancellation percentage after job failure: ${err}` + RESET);
          });

          // Delete the order from database anyway to clean up
          if (job.data.orderKey) {
            Order.deleteOne({ _id: job.data.orderKey }).catch((err) => {
              const orderTaskId = getTaskIdForLog(job);
              console.error(RED + `[ERROR - Order Cleanup - Task: ${orderTaskId}] Failed to delete order after cancellation failure: ${err}` + RESET);
            });
          } else if (job.data.orderKeys) {
            Order.deleteMany({ _id: { $in: job.data.orderKeys } }).catch((err) => {
              const ordersTaskId = getTaskIdForLog(job);
              console.error(RED + `[ERROR - Order Cleanup - Task: ${ordersTaskId}] Failed to delete orders after cancellation failure: ${err}` + RESET);
            });
          }
        }

        // Only track as worker failure if it's a systemic issue
        if (error.message?.includes('could not renew lock') ||
          error.message?.includes('Missing lock') ||
          error.message?.includes('connection lost')) {
          trackWorkerFailure(i, `Job processing error: ${error.message}`);
        }
      });

      // Add worker to array
      workers.push(worker);
    }

    // Create Blur worker with its own connection and rate limiting
    const blurWorkerConnection = await createBullMQConnection('blur-worker');
    bullMQConnections.push(blurWorkerConnection);
    console.log(GREEN + '✅ Created connection for Blur worker' + RESET);

    const blurWorker = new Worker(
      BLUR_BIDDING_QUEUE,
      async (job) => {
        const jobTimeout = job.name === BLUR_SCHEDULE ? 900000 : 60000; // 15 minutes for scheduled jobs, 60 seconds for others
        const abortController = new AbortController();

        const timeoutPromise = new Promise((_, reject) => {
          setTimeout(() => {
            abortController.abort(); // Signal cancellation to any async operations
            const customJobId = (job.opts as any)?.jobId || job.id;
            reject(new Error(`Blur job ${customJobId} (ID: ${job.id}) timed out after ${jobTimeout}ms - aborted`));
          }, jobTimeout);
        });

        try {
          const result = await Promise.race([
            processBlurJob(job),
            timeoutPromise
          ]);

          return result || { success: true };
        } catch (error: any) {
          const blurJobTaskId = getTaskIdForLog(job);
          console.error(RED + `[ERROR - Blur Worker - Task: ${blurJobTaskId}] Job ${job.id} failed:` + RESET, error.message || error);
          // If it's a password error, throw it to fail the job properly
          if (error.code === 'NO_PASSWORD' || error.message?.includes('No password available')) {
            throw error;
          }
          // For other errors, also throw to ensure proper retry
          throw error;
        }
      },
      {
        prefix: '{bull}',
        connection: blurWorkerConnection,
        concurrency: concurrencyPerWorker, // Reduce concurrency for Blur to prevent rate limit issues
        lockDuration: 90000, // 90 seconds lock (1.5x job timeout)
        stalledInterval: 90000, // Check for stalled jobs every 90 seconds
        maxStalledCount: 1 // Move to failed after 1 stall
      }
    );



    workers.push(blurWorker);

    // Add error handlers for Blur queue components
    blurWorker.on('error', async (error) => {
      console.error(RED + '[ERROR - Blur Worker - Task: N/A] Worker error:' + RESET, error);
      const blurWorkerIndex = workers.length - 1; // Blur worker is the last one
      trackWorkerFailure(blurWorkerIndex, `Blur worker error: ${error.message || String(error)}`);

      // Check if this is a Redis cluster error
      if (isRedisClusterError(error)) {
        console.log(RED + '🚨 Redis cluster error detected in blur worker' + RESET);
        redisClusterFailures++;

        if (redisClusterFailures >= REDIS_CLUSTER_FAILURE_THRESHOLD) {
          await handleRedisClusterFailure();
        }
      }

      logger.logGeneralError('Blur worker error', {
        error: error,
        errorMessage: error.message || String(error),
        errorStack: error.stack
      });
    });

    blurWorker.on('failed', (job, error) => {
      const blurFailedTaskId = getTaskIdForLog(job);
      console.error(RED + `[ERROR - Blur Worker - Task: ${blurFailedTaskId}] Job ${job?.id} failed:` + RESET, error.message || error);

      // Handle failed Blur cancellation jobs - track them as completed
      if (job && job.name === CANCEL_BLUR_BID) {
        const { taskId } = job.data;

        console.log(YELLOW + `[Blur Worker] Cancellation job failed after all retries - marking as complete for tracking` + RESET);

        // Record the failed cancellation as a completion to ensure we reach 100%
        cancellationTracker.recordCompletion(taskId, 1, 'blur');
        updateTaskCancellationPercentage(taskId).catch((err) => {
          const blurCancelTaskId = getTaskIdForLog(job);
          console.error(RED + `[ERROR - Cancellation Tracking - Task: ${blurCancelTaskId}] Failed to update cancellation percentage after Blur job failure: ${err}` + RESET);
        });

        // Delete the order from database anyway to clean up
        if (job.data.orderKey) {
          Order.deleteOne({ _id: job.data.orderKey }).catch((err) => {
            const blurOrderTaskId = getTaskIdForLog(job);
            console.error(RED + `[ERROR - Order Cleanup - Task: ${blurOrderTaskId}] Failed to delete Blur order after cancellation failure: ${err}` + RESET);
          });
        } else if (job.data.orderKeys) {
          Order.deleteMany({ _id: { $in: job.data.orderKeys } }).catch((err) => {
            const blurOrdersTaskId = getTaskIdForLog(job);
            console.error(RED + `[ERROR - Order Cleanup - Task: ${blurOrdersTaskId}] Failed to delete Blur orders after cancellation failure: ${err}` + RESET);
          });
        }
      }

      // Track systemic failures
      if (error.message?.includes('could not renew lock') ||
        error.message?.includes('Missing lock') ||
        error.message?.includes('connection lost')) {
        const blurWorkerIndex = workers.length - 1;
        trackWorkerFailure(blurWorkerIndex, `Blur job processing error: ${error.message}`);
      }
    });

    blurQueue.on('error', (error) => {
      console.error(RED + '[ERROR - Blur Queue - Task: N/A] Queue error:' + RESET, error);
      logger.logGeneralError('Blur queue error', {
        error: error,
        errorMessage: error.message || String(error),
        errorStack: error.stack
      });
    });

    blurQueueEvents.on('error', (error) => {
      console.error(RED + '[ERROR - Blur Queue Events - Task: N/A] Events error:' + RESET, error);
      logger.logGeneralError('Blur queue events error', {
        error: error,
        errorMessage: error.message || String(error),
        errorStack: error.stack
      });
    });

    console.log(`Created ${bullMQConnections.length} separate Redis connections for BullMQ components`);

    // Initialize worker health monitoring
    startWorkerHealthMonitoring();


    // Add process-level error handler for uncaught BullMQ errors
    process.on('unhandledRejection', async (reason, promise) => {
      if (reason instanceof Error && reason.message.includes('could not renew lock')) {
        await BullMQErrorFormatter.formatError(reason);
      } else {
        console.error(RED + '[ERROR - Unhandled Rejection - Task: N/A] Unhandled Rejection at:' + RESET, promise, 'reason:', reason);
      }
    });

    const mongoUri = process.env.MONGODB_URI;
    if (!mongoUri) {
      throw new Error('MONGODB_URI environment variable is not set');
    }

    console.log('Connecting to MongoDB...');
    try {
      await mongoose.connect(mongoUri, {
        // Connection timeouts
        serverSelectionTimeoutMS: 30000,
        connectTimeoutMS: 30000,
        socketTimeoutMS: 45000,

        // Connection pool optimization for high throughput (100+ QPS)
        maxPoolSize: Number(process.env.MONGO_MAX_POOL_SIZE) || 50, // Increased from 10
        minPoolSize: Number(process.env.MONGO_MIN_POOL_SIZE) || 5,  // Maintain warm connections
        maxIdleTimeMS: 30000, // Close idle connections after 30s

        // Performance optimizations
        retryWrites: true,
        family: 4, // Use IPv4, skip trying IPv6
        heartbeatFrequencyMS: 10000, // Reduced from 5000 to default for less network traffic

        // Buffer settings for immediate error feedback
        bufferCommands: false,  // Disable command buffering for immediate errors

        // Write concern optimization for performance
        writeConcern: {
          w: 1,      // Wait for acknowledgment from primary only
          j: false   // Don't wait for journal sync (faster writes)
        }
      });
      console.log('Connected to MongoDB successfully');

      // Add connection pool monitoring for high-throughput operations
      mongoose.connection.on('disconnected', () => {
        console.warn('🔴 MongoDB disconnected');
      });

      mongoose.connection.on('reconnected', () => {
        console.log('🟢 MongoDB reconnected');
      });

      mongoose.connection.on('error', (err) => {
        console.error(RED + '[ERROR - MongoDB Connection - Task: N/A] 🔴 MongoDB connection error:' + RESET, err);
      });

      // Log connection pool stats in development
      if (process.env.NODE_ENV === 'development') {
        setInterval(() => {
          const readyState = mongoose.connection.readyState;
          const stateMap = { 0: 'disconnected', 1: 'connected', 2: 'connecting', 3: 'disconnecting' };
          console.log(`📊 MongoDB Status: ${stateMap[readyState as keyof typeof stateMap]} | Max Pool: ${Number(process.env.MONGO_MAX_POOL_SIZE) || 50}`);
        }, STANDARD_INTERVAL);
      }
    } catch (error) {
      await logger.logDatabaseConnectionError('Failed to connect to MongoDB', {
        error: error as Error,
        errorMessage: error instanceof Error ? error.message : String(error),
        mongoUri: mongoUri.replace(/\/\/[^:]+:[^@]+@/, '//***:***@')
      });
      throw error;
    }

    // Set all tasks to not running
    try {
      const result = await Task.updateMany({ running: true }, { running: false });
      await Listing.updateMany({ running: true }, { running: false });
      // Tasks updated silently
    } catch (error) {
      console.error(RED + '[ERROR - Task Update - Task: N/A] Failed to update tasks:' + RESET, error);
    }

    // Try to recover password from Redis BEFORE starting jobs
    console.log(CYAN + '🔍 Checking for stored password...' + RESET);
    let passwordRecovered = false;
    const maxRetries = 3;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      console.log(CYAN + `📡 Password recovery attempt ${attempt}/${maxRetries}...` + RESET);

      // Check Redis connection status
      if (!redisClient.isReady()) {
        console.log(YELLOW + 'Waiting for Redis connection...' + RESET);
        await new Promise(resolve => setTimeout(resolve, 1000));
        continue;
      }

      passwordRecovered = await recoverPasswordFromRedis();

      if (passwordRecovered) {
        break;
      } else if (attempt < maxRetries) {
        console.log(YELLOW + `Password not found in Redis, retrying in ${attempt} second(s)...` + RESET);
        await new Promise(resolve => setTimeout(resolve, attempt * 1000));
      }
    }

    // Check if password is set (either from Redis recovery or needs to be set)
    const currentPassword = getPassword();
    if (!currentPassword) {
      console.log(YELLOW + '⚠️  No password found after ' + maxRetries + ' attempts - all operations are paused' + RESET);
      console.log(YELLOW + 'Please set password via POST /api/auth/password to start operations' + RESET);
      // Don't start jobs if there's no password
    } else {
      if (passwordRecovered) {
        console.log(GREEN + '✅ Password recovered from Redis - operations will start' + RESET);
      } else {
        console.log(GREEN + '✅ Password is already set - operations will start' + RESET);
      }
    }

    // Clear queue and restart all jobs on server start - AFTER MongoDB is connected and password check
    await clearQueueAndRestartJobs();

    // If no password was found, ensure queues and workers are paused
    if (!currentPassword) {
      await pauseQueueAndWorkers();
      console.log(YELLOW + '⚠️  Queues and workers are paused - waiting for password' + RESET);
    } else {
      // Verify all workers are ready before processing jobs
      console.log(CYAN + '🔍 Verifying workers are ready...' + RESET);

      // Give workers time to fully initialize
      await new Promise(resolve => setTimeout(resolve, 2000));

      // Check worker health
      const workerStatuses = await Promise.all(
        workers.map(async (worker, index) => {
          try {
            const isRunning = worker.isRunning();
            const isPaused = worker.isPaused();
            return { index, running: isRunning, paused: isPaused, healthy: isRunning && !isPaused };
          } catch (error) {
            return { index, running: false, paused: false, healthy: false, error };
          }
        })
      );

      const unhealthyWorkers = workerStatuses.filter(s => !s.healthy);
      if (unhealthyWorkers.length > 0) {
        console.error(RED + `[ERROR - Worker Health - Task: N/A] ⚠️  ${unhealthyWorkers.length} workers are not ready:` + RESET);
        unhealthyWorkers.forEach(w => {
          console.error(RED + `[ERROR - Worker Health - Task: N/A]   - Worker ${w.index}: running=${w.running}, paused=${w.paused}` + RESET);
        });

        // Try to resume paused workers
        for (const workerStatus of unhealthyWorkers) {
          if (!workerStatus.running || workerStatus.paused) {
            try {
              await workers[workerStatus.index].resume();
              console.log(GREEN + `  ✅ Worker ${workerStatus.index} resumed` + RESET);
            } catch (error) {
              console.error(RED + `[ERROR - Worker Resume - Task: N/A]   Failed to resume worker ${workerStatus.index}:` + RESET, error);
            }
          }
        }
      } else {
        console.log(GREEN + '✅ All workers are ready and healthy' + RESET);
      }

      // Verify Redis connections one more time
      const finalHealthCheck = await checkBullMQConnectionsHealth();
      const finalUnhealthy = Object.entries(finalHealthCheck).filter(([_, status]) => !status.healthy);

      if (finalUnhealthy.length > 0) {
        console.error(RED + '[ERROR - Redis Health - Task: N/A] ⚠️  Some Redis connections are still unhealthy:' + RESET);
        finalUnhealthy.forEach(([id, status]) => {
          console.error(RED + `[ERROR - Health Check - Task: N/A]   - ${id}: ${status.error}` + RESET);
        });
      } else {
        console.log(GREEN + '✅ All Redis connections are healthy' + RESET);
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
      console.error(RED + '[ERROR - Redis Operation - Task: N/A] Redis operation failed:' + RESET, redisError);
    }

    server.listen(port, async () => {
      console.log(`Magic happening on http://localhost:${port}`);
      console.log(`WebSocket server is running on ws://localhost:${port}`);

      // Start Redis cluster monitoring with clearQueueAndRestartJobs callback
      try {
        startRedisClusterMonitoring(clearQueueAndRestartJobs);
        console.log('Redis cluster monitoring started with recovery callback');
      } catch (error) {
        console.error(RED + '[ERROR - Redis Monitor - Task: N/A] Failed to start Redis cluster monitoring:' + RESET, error);
      }
    });

    // Initialize periodic cleanup for aborted tasks (every 30 minutes)
    setInterval(() => {
      cleanupAbortedTasks();
    }, 30 * 60 * 1000); // 30 minutes

    // Initialize WebSocket connection monitoring
    await loadConnectionMetrics();
    startConnectionMonitoring();
    await logger.logInfo(ErrorCategory.WEBSOCKET, 'WebSocket connection monitoring initialized', {
      monitoringInterval: 60000
    });

    // Connect WebSocket before initializing listing scheduler
    try {
      await connectWebSocket();
      await logger.logInfo(ErrorCategory.WEBSOCKET, 'WebSocket connected to marketplace proxy', {
        url: MARKETPLACE_WS_URL
      });
    } catch (error) {
      await logger.logWebSocketError('Failed to connect to marketplace WebSocket', {
        error: error as Error,
        url: MARKETPLACE_WS_URL,
        errorMessage: error instanceof Error ? error.message : String(error)
      });
      console.error(RED + '[ERROR - WebSocket - Task: N/A] WebSocket connection failed:' + RESET, error);
    }

    // Initialize listing scheduler with WebSocket manager
    try {
      await initializeListingScheduler(wsManager);
      await logger.logInfo(ErrorCategory.GENERAL, 'Listing scheduler initialized', {
        schedulerType: 'listing_scheduler',
        hasWebSocketManager: !!wsManager
      });
    } catch (error) {
      await logger.logGeneralError('Failed to initialize listing scheduler', {
        error: error as Error,
        errorMessage: error instanceof Error ? error.message : String(error)
      });
    }

    // Initialize expiration monitor
    expirationMonitorInterval = initializeExpirationMonitor();
    console.log('⏰ Expiration monitor initialized');
    await logger.logInfo(ErrorCategory.GENERAL, 'Expiration monitor initialized', {
      schedulerType: 'expiration_monitor',
      checkInterval: 5 * 60 * 1000 // 5 minutes
    });

    // Initialize orphaned job recovery system
    orphanedJobRecovery.startRecovery(30000); // Check every 30 seconds
    console.log('🔧 Orphaned job recovery system initialized');
    await logger.logInfo(ErrorCategory.GENERAL, 'Orphaned job recovery initialized', {
      recoveryType: 'orphaned_jobs',
      checkInterval: 30000 // 30 seconds
    });

    // Start internet connectivity monitoring
    startConnectivityMonitoring();

    // Start Redis cluster monitoring
    startRedisClusterMonitoring();
    console.log('🔍 Redis cluster monitoring initialized');

    await fetchCurrentTasks()

    // Start all tasks that have running: true in database
    // This handles server restarts where tasks were previously running
    console.log(YELLOW + '🔄 Checking for tasks to restart...' + RESET);
    let tasksToRestart = 0;
    for (const [taskId, task] of activeTasks) {
      if (task.running) {
        tasksToRestart++;
        // Initialize task status if not exists
        if (!taskStatus[taskId]) {
          taskStatus[taskId] = {
            status: 'SCHEDULED',
            runningPercentage: 0,
            cancellationPercentage: 0,
            processingPercentage: 0,
            totalJobs: 0,
            completedJobs: 0,
            waitingTime: 0,
            lastUpdate: Date.now()
          };
        }

        // Start the task with a small delay to ensure everything is initialized
        setTimeout(() => {
          console.log(GREEN + `✅ Restarting task ${taskId} (${task.contract?.slug || 'Unknown collection'})` + RESET);
          startTask(task, true);
        }, tasksToRestart * 1000); // Stagger task starts by 1 second each
      }
    }

    if (tasksToRestart > 0) {
      console.log(GREEN + `✅ Found ${tasksToRestart} running tasks to restart` + RESET);
    } else {
      console.log(YELLOW + '⚠️  No running tasks found to restart' + RESET);
    }

    // Check for orphaned scheduled tasks (tasks stuck in SCHEDULED state without timers)
    console.log(YELLOW + '🔍 Checking for orphaned scheduled tasks...' + RESET);
    const orphanedScheduledTasks = Array.from(activeTasks.entries())
      .filter(([taskId, task]) => {
        const status = taskStatus[taskId];
        const hasTimer = taskTimeouts.has(taskId);
        return task.running &&
          status?.status === 'SCHEDULED' &&
          !hasTimer;
      });

    if (orphanedScheduledTasks.length > 0) {
      console.log(YELLOW + `⚠️  Found ${orphanedScheduledTasks.length} orphaned scheduled tasks` + RESET);

      for (const [taskId, task] of orphanedScheduledTasks) {
        const timeSinceLastUpdate = taskStatus[taskId]?.lastUpdate ?
          Date.now() - taskStatus[taskId].lastUpdate : 'unknown';

        console.log(YELLOW + `📌 Orphaned task ${taskId} (${task.contract?.slug || 'Unknown'})` + RESET);
        console.log(YELLOW + `   - Status: ${taskStatus[taskId]?.status}` + RESET);
        console.log(YELLOW + `   - Completed Jobs: ${taskStatus[taskId]?.completedJobs || 0}/${taskStatus[taskId]?.totalJobs || 0}` + RESET);
        console.log(YELLOW + `   - Time since last update: ${timeSinceLastUpdate}ms` + RESET);

        // Initialize task status if needed
        if (!taskStatus[taskId]) {
          taskStatus[taskId] = {
            status: 'SCHEDULED',
            runningPercentage: 0,
            cancellationPercentage: 0,
            processingPercentage: 0,
            totalJobs: 0,
            completedJobs: 0,
            waitingTime: 0,
            lastUpdate: Date.now()
          };
        }

        // Restart the orphaned task immediately
        console.log(GREEN + `✅ Restarting orphaned scheduled task ${taskId}` + RESET);
        setTimeout(() => {
          startTask(task, true);
        }, 500); // Small delay to ensure system is ready
      }
    } else {
      console.log(GREEN + '✅ No orphaned scheduled tasks found' + RESET);
    }

    // Start password auto-recovery checker now that Redis is connected
    setInterval(checkPasswordAndResume, PASSWORD_CHECK_INTERVAL);
    console.log(GREEN + '✅ Password auto-recovery checker started (checking every 30s)' + RESET);


    // Start stuck task detection
    const STUCK_TASK_CHECK_INTERVAL = 30000; // 30 seconds
    setInterval(checkStuckTasks, STUCK_TASK_CHECK_INTERVAL);
    console.log(GREEN + '✅ Stuck task detection started (checking every 30s)' + RESET);


  } catch (error) {
    console.error(RED + '[ERROR - Server Startup - Task: N/A] Failed to start server:' + RESET, error);
    process.exit(1);
  }
}

// Cleanup handler for graceful shutdown
process.on('SIGINT', async () => {
  console.log('\nReceived SIGINT, cleaning up...');

  // Update all running tasks to not running
  try {
    const result = await Task.updateMany(
      { running: true },
      { running: false }
    );
    console.log(`Updated ${result.modifiedCount} tasks to not running`);
  } catch (error) {
    console.error(RED + '[ERROR - Task State Update - Task: N/A] Error updating task states:' + RESET, error);
  }

  // Stop connectivity monitoring
  stopConnectivityMonitoring();

  // Shutdown listing scheduler
  await shutdownListingScheduler();
  console.log('Listing scheduler shut down');

  // Stop expiration monitor
  if (expirationMonitorInterval) {
    stopExpirationMonitor(expirationMonitorInterval);
    console.log('Expiration monitor stopped');
  }

  // Stop orphaned job recovery
  orphanedJobRecovery.stopRecovery();
  console.log('Orphaned job recovery stopped');

  // Stop Redis cluster monitoring
  stopRedisClusterMonitoring();
  console.log('Redis cluster monitoring stopped');

  // Stop validate bid count interval
  if (validateBidCountInterval) {
    clearInterval(validateBidCountInterval);
    console.log('Validate bid count interval stopped');
  }

  // Disconnect WebSocket
  if (wsManager) {
    wsManager.disconnect();
    console.log('WebSocket disconnected');
  }

  // Close all BullMQ connections
  console.log('Closing BullMQ connections...');
  for (const connection of bullMQConnections) {
    try {
      await connection.quit();
    } catch (error) {
      console.error(RED + '[ERROR - Connection Close - Task: N/A] Error closing connection:' + RESET, error);
    }
  }
  console.log('All BullMQ connections closed');

  // Final cleanup before exit
  seenOrderHashes.clear();
  seenBids.clear();

  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.log('\nReceived SIGTERM, cleaning up...');

  // Update all running tasks to not running
  try {
    const result = await Task.updateMany(
      { running: true },
      { running: false }
    );
    console.log(`Updated ${result.modifiedCount} tasks to not running`);
  } catch (error) {
    console.error(RED + '[ERROR - Task State Update - Task: N/A] Error updating task states:' + RESET, error);
  }

  // Stop connectivity monitoring
  stopConnectivityMonitoring();

  // Shutdown listing scheduler
  await shutdownListingScheduler();
  console.log('Listing scheduler shut down');

  // Stop expiration monitor
  if (expirationMonitorInterval) {
    stopExpirationMonitor(expirationMonitorInterval);
    console.log('Expiration monitor stopped');
  }

  // Stop orphaned job recovery
  orphanedJobRecovery.stopRecovery();
  console.log('Orphaned job recovery stopped');

  // Stop Redis cluster monitoring
  stopRedisClusterMonitoring();
  console.log('Redis cluster monitoring stopped');

  // Stop validate bid count interval
  if (validateBidCountInterval) {
    clearInterval(validateBidCountInterval);
    console.log('Validate bid count interval stopped');
  }

  // Disconnect WebSocket
  if (wsManager) {
    wsManager.disconnect();
    console.log('WebSocket disconnected');
  }

  // Close all BullMQ connections
  console.log('Closing BullMQ connections...');
  for (const connection of bullMQConnections) {
    try {
      await connection.quit();
    } catch (error) {
      console.error(RED + '[ERROR - Connection Close - Task: N/A] Error closing connection:' + RESET, error);
    }
  }
  console.log('All BullMQ connections closed');

  seenOrderHashes.clear();
  seenBids.clear();
  process.exit(0);
});

startServer()
  .catch(async error => {
    await logger.logGeneralError('Failed to start server', {
      error: error as Error,
      errorMessage: error instanceof Error ? error.message : String(error)
    });
    console.error(RED + '[ERROR - Server Startup - Task: N/A] Failed to start server:' + RESET, error);
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
  const isCancellationComplete = status.cancellationPercentage >= 100;

  // If cancellation is at 100%, verify no active orders exist
  if (isCancellationComplete) {
    try {
      const activeOrderCount = await Order.countDocuments({
        taskId: taskId,
        expiresAt: { $gt: new Date() }
      });

      if (activeOrderCount > 0) {
        console.log(YELLOW + `[shouldTaskBeStopped] Task ${taskId} at 100% but still has ${activeOrderCount} active orders, keeping CANCELLING status` + RESET);
        return false;
      }
    } catch (error) {
      console.error(RED + `[ERROR - Task Status Check - Task: ${taskId}] Error checking active orders:` + RESET, error);
      // If we can't verify, don't transition to STOPPED
      return false;
    }
  }

  console.log(CYAN + `[shouldTaskBeStopped] Task ${taskId} - Running: ${task?.running || false}, CancellationPercentage: ${status.cancellationPercentage}%, Should stop: ${isNotRunning && isCancellationComplete}` + RESET);

  return isNotRunning && isCancellationComplete;
}

// Helper function to ensure task transitions to STOPPED when cancellation is complete
async function setTaskCancellationComplete(taskId: string) {
  const task = activeTasks.get(taskId)

  // Only proceed if taskStatus exists (don't create new ones here)
  if (!taskStatus[taskId]) {
    console.log(YELLOW + `[setTaskCancellationComplete] taskStatus not found for ${taskId}, skipping` + RESET);
    return;
  }

  // Let the cancellation tracker handle percentage updates naturally

  // Check if task should be stopped based on both conditions
  if (await shouldTaskBeStopped(taskId)) {
    console.log(GREEN + `[setTaskCancellationComplete] Task ${taskId} meets both conditions (not running + 100% cancelled), transitioning to STOPPED` + RESET);
    taskStatus[taskId].status = 'STOPPED';
    delete taskStatus[taskId].nextLoopTime;
    taskStatus[taskId].waitingTime = 0;
  } else {
    // Task doesn't meet both conditions, keep current status
    const task = activeTasks.get(taskId);
    if (task && task.running) {
      console.log(YELLOW + `[setTaskCancellationComplete] Task ${taskId} is still running, keeping current status` + RESET);
    } else {
      console.log(YELLOW + `[setTaskCancellationComplete] Task ${taskId} cancellation not at 100%, keeping CANCELLING status` + RESET);
    }
  }

  // Clean up cancellation operation tracking
  const cancellationOp = activeCancellationOperations.get(taskId);
  if (cancellationOp) {
    activeCancellationOperations.delete(taskId);
  }
}

// Helper function to safely set task status with regression protection and validation
function setTaskStatus(taskId: string, newStatus: string, context?: string): boolean {
  if (!taskStatus[taskId]) {
    taskStatus[taskId] = {
      status: newStatus as any,
      runningPercentage: 0,
      cancellationPercentage: 0,
      processingPercentage: 0,
      totalJobs: 0,
      completedJobs: 0,
      waitingTime: 0,
      lastUpdate: Date.now()
    };
    return true;
  }

  const currentStatus = taskStatus[taskId].status;

  // Define valid state transitions
  const validTransitions: { [key: string]: string[] } = {
    'STARTED': ['PROCESSING', 'QUEUEING', 'STOPPED', 'CANCELLING'],
    'PROCESSING': ['QUEUEING', 'STARTED', 'STOPPED', 'CANCELLING'],
    'QUEUEING': ['RUNNING', 'STARTED', 'STOPPED', 'CANCELLING'],
    'QUEUED': ['RUNNING', 'QUEUEING', 'STOPPED', 'CANCELLING'],
    'RUNNING': ['COMPLETED', 'SCHEDULED', 'STOPPED', 'CANCELLING'],
    'COMPLETED': ['SCHEDULED'],
    'SCHEDULED': ['STARTED'],
    'CANCELLING': ['STOPPED'],
    'STOPPED': ['STARTED']
  };

  // Check if transition is valid
  const allowedTransitions = validTransitions[currentStatus] || [];
  if (!allowedTransitions.includes(newStatus)) {
    // Log detailed state info for debugging
    console.log(YELLOW + `  Current state: ${JSON.stringify({
      status: currentStatus,
      runningPercentage: taskStatus[taskId].runningPercentage,
      totalJobs: taskStatus[taskId].totalJobs,
      completedJobs: taskStatus[taskId].completedJobs
    })}` + RESET);

    return false;
  }

  // Additional validation for specific transitions
  if (newStatus === 'RUNNING') {
    // Verify jobs exist before transitioning to RUNNING
    const hasJobs = verifyTaskHasJobs(taskId);
    if (!hasJobs) {
      console.log(YELLOW + `[StateTransition] Cannot transition to RUNNING - no jobs queued for task ${taskId}` + RESET);

      // Check if we have parent jobs waiting to queue
      const task = activeTasks.get(taskId);
      if (task) {
        let hasParentJobs = false;
        for (const marketplace of task.selectedMarketplaces) {
          const parentJobId = getParentJobId(taskId, marketplace.toLowerCase());
          if (parentJobs.has(parentJobId)) {
            hasParentJobs = true;
            break;
          }
        }

        if (hasParentJobs) {
          console.log(CYAN + `[StateTransition] Parent jobs exist - keeping in QUEUEING state` + RESET);
          setTaskStatus(taskId, 'QUEUEING', 'jobs being queued');
        }
      }

      return false;
    }
  }

  // Special validation for COMPLETED transition
  if (newStatus === 'COMPLETED') {
    // Verify task is actually complete
    const runningPercentage = taskStatus[taskId].runningPercentage || 0;
    if (runningPercentage < 100) {
      console.log(YELLOW + `[StateTransition] Cannot transition to COMPLETED - task only at ${runningPercentage}%` + RESET);
      return false;
    }
  }

  // Update status
  const previousStatus = taskStatus[taskId].status;
  taskStatus[taskId].status = newStatus as any;
  taskStatus[taskId].lastUpdate = Date.now();

  // Update lastProgressUpdate when changing percentage-related states
  if (newStatus === 'RUNNING' || newStatus === 'COMPLETED') {
    taskStatus[taskId].lastProgressUpdate = Date.now();
  }

  // Set completion timestamp when task completes
  if (newStatus === 'COMPLETED') {
    taskStatus[taskId].completionTimestamp = Date.now();
  }

  // Clear completion timestamp when restarting
  if (newStatus === 'STARTED' && previousStatus === 'COMPLETED') {
    delete taskStatus[taskId].completionTimestamp;
  }

  console.log(CYAN + `[setTaskStatus] ${taskId}: ${currentStatus} -> ${newStatus}${context ? ' (' + context + ')' : ''}` + RESET);

  return true;
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
    console.log(YELLOW + `[setMarketplaceCancellationComplete] taskStatus not found for ${taskId}, skipping` + RESET);
    return;
  }

  // Clean up no-progress counter
  // Removed cancellationNoProgressCount.delete(taskId) - now using cancellationTracker

  // Check if this is a marketplace-specific cancellation (has previousState)
  if (taskStatus[taskId].previousState) {
    // This is a marketplace-specific cancellation
    console.log(GREEN + `[setMarketplaceCancellationComplete] Marketplace-specific cancellation complete for ${taskId}/${marketplace}` + RESET);

    // Verify no active orders remain for this marketplace
    if (marketplace) {
      try {
        const activeOrderCount = await Order.countDocuments({
          taskId: taskId,
          marketplace: marketplace.toLowerCase(),
          expiresAt: { $gt: new Date() }
        });

        if (activeOrderCount > 0) {
          console.log(YELLOW + `[setMarketplaceCancellationComplete] Task ${taskId}/${marketplace} still has ${activeOrderCount} active orders, not marking complete` + RESET);
          return;
        }
      } catch (error) {
        console.error(RED + `[ERROR - Marketplace Cancellation - Task: ${taskId}] Error checking active orders for marketplace ${marketplace}:` + RESET, error);
        // If we can't verify, don't mark as complete
        return;
      }
    }

    // Let the cancellation tracker handle percentage updates naturally for marketplace-specific cancellation

    // Check if the task is still running before scheduling next loop
    const task = activeTasks.get(taskId);
    if (task && task.running) {
      console.log(GREEN + `Setting task ${taskId} to SCHEDULED (was: ${taskStatus[taskId].previousState})` + RESET);
      setTaskStatus(taskId, 'SCHEDULED', 'marketplace-specific cancellation complete');
      delete taskStatus[taskId].previousState;
      delete taskStatus[taskId].cancellingMarketplace;
      // Reset cancellation percentage after setting to SCHEDULED
      await updateTaskCancellationPercentage(taskId, 0);
    } else {
      // Task is not running, set to STOPPED instead of SCHEDULED
      console.log(YELLOW + `Task ${taskId} is not running, setting to STOPPED instead of SCHEDULED` + RESET);
      setTaskStatus(taskId, 'STOPPED', 'marketplace-specific cancellation complete - task not running');
      delete taskStatus[taskId].previousState;
      delete taskStatus[taskId].cancellingMarketplace;
      // Reset cancellation percentage
      await updateTaskCancellationPercentage(taskId, 0);
    }

    // Remove marketplace from blacklist to allow jobs to execute again
    if (marketplace) {
      const wasRemoved = removeTaskFromBlacklist(taskId, marketplace);
      if (wasRemoved) {
        console.log(GREEN + `Task ${taskId} removed from blacklist for marketplace: ${marketplace}` + RESET);
      }
    }

    // Keep all other progress values intact
    console.log(GREEN + `Task ${taskId} restored. Current status: ${taskStatus[taskId].status}` + RESET);
  } else {
    // No previous state - this could be either:
    // 1. A global cancellation where one marketplace finished (don't mark as complete)
    // 2. An edge case where we lost state

    // Check if we have task-level tracking (indicates global cancellation)
    if (cancellationTracker.hasTaskLevelTracking(taskId)) {
      // This is part of a global cancellation - just log and let the tracker handle it
      console.log(CYAN + `[setMarketplaceCancellationComplete] Marketplace ${marketplace} complete during global cancellation for task ${taskId}` + RESET);
      console.log(CYAN + `[setMarketplaceCancellationComplete] Letting cancellation tracker handle percentage updates` + RESET);

      // Do NOT call setTaskCancellationComplete here!
      // The task will transition to STOPPED when the aggregated percentage reaches 100%
    } else {
      // Edge case: no previous state and no task-level tracking
      console.log(YELLOW + `[setMarketplaceCancellationComplete] WARNING: No previous state or task-level tracking for ${taskId}` + RESET);
      console.log(YELLOW + `[setMarketplaceCancellationComplete] This might be an edge case - not marking as complete` + RESET);
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
function cleanupTaskResources(taskId: string): void {
  const timestamp = new Date().toISOString();
  console.log(CYAN + `[cleanupTaskResources] ${timestamp} - Cleaning up resources for task ${taskId}` + RESET);

  // Clear loop counter
  if (loopCounterStats[taskId]) {
    delete loopCounterStats[taskId];
  }

  // Clear and delete timeout
  const timeout = taskTimeouts.get(taskId);
  if (timeout) {
    clearTimeout(timeout);
    taskTimeouts.delete(taskId);
  }

  // Clear and delete countdown timer
  const countdownTimer = taskCountdownTimers.get(taskId);
  if (countdownTimer) {
    clearInterval(countdownTimer);
    taskCountdownTimers.delete(taskId);
  }

  // Release loop scheduling lock
  releaseLoopSchedulingLock(taskId);

  // Clear scheduling flag
  if (tasksBeingScheduled.has(taskId)) {
    tasksBeingScheduled.delete(taskId);
  }

  // Clear marketplace adding jobs flag

  // Log final stats before clearing
  if (skipStats[taskId]) {
    console.log(CYAN + `[cleanupTaskResources] Final skipStats for task ${taskId}: opensea=${skipStats[taskId].opensea || 0}, magiceden=${skipStats[taskId].magiceden || 0}, blur=${skipStats[taskId].blur || 0}` + RESET);
  }

  // Clear task-specific stats objects
  delete skipStats[taskId];
  delete errorStats[taskId];
  delete floorPrices[taskId];
  delete warningBids[taskId];
  delete warningMessage[taskId];
  delete bestOffers[taskId];

  // Clear scheduling tracking
  lastScheduleTimes.delete(taskId);
  lastCompletionCheckTimes.delete(taskId);

  // Clear processed jobs tracking for all marketplaces and all loops
  const marketplaces = ['opensea', 'magiceden', 'blur'];

  // Clean up all parent jobs for this task (across all loops)
  const keysToDelete: string[] = [];
  for (const [key, parentJob] of parentJobs) {
    if (parentJob.taskId === taskId) {
      keysToDelete.push(key);
    }
  }
  keysToDelete.forEach(key => {
    parentJobs.delete(key);
    processedJobsPerParent.delete(key);
    console.log(CYAN + `[cleanupTaskResources] Deleted parent job: ${key}` + RESET);
  });

  // Clean up cancellation tracking
  marketplaces.forEach(marketplace => {
    cancellationTracker.clearTracking(taskId, marketplace);
  });

  // Clean up restart counter
  taskRestartCounts.delete(taskId);

  console.log(CYAN + `[cleanupTaskResources] ${timestamp} - Cleanup complete for task ${taskId}` + RESET);
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

  // Calculate totals across all marketplaces
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
          console.log(YELLOW + `[updateTaskRunningPercentage] ${parentJobId}: Adjusting total from ${originalTotal} to ${mpTotal} (actualJobs=${parentJob.actualJobs} > expectedJobs=${parentJob.expectedJobs})` + RESET);
        }
      }

      // Log the values being used for transparency
      if (parentJob.allJobsQueued) {
        console.log(CYAN + `[updateTaskRunningPercentage] ${parentJobId}: Using totalJobs=${mpTotal} (actualJobs=${parentJob.actualJobs}, expectedJobs=${parentJob.expectedJobs}, allJobsQueued=true)` + RESET);

        // Log partial queueing warning
        if (parentJob.actualJobs !== undefined && parentJob.expectedJobs !== undefined &&
          parentJob.actualJobs < parentJob.expectedJobs) {
          console.log(YELLOW + `    ⚠️  Partial queueing detected: ${parentJob.completedJobs}/${parentJob.actualJobs} completed (${parentJob.expectedJobs} expected)` + RESET);
        }

        // Detect stuck condition: allJobsQueued=true, totalJobs=0, but expectedJobs > 0
        if (mpTotal === 0 && parentJob.expectedJobs && parentJob.expectedJobs > 0) {
          console.log(RED + `[updateTaskRunningPercentage] STUCK CONDITION DETECTED: ${parentJobId} has 0 totalJobs but expectedJobs=${parentJob.expectedJobs}` + RESET);

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

            console.log(YELLOW + `[updateTaskRunningPercentage] Scheduled restart for stuck task ${taskId} in 2 seconds` + RESET);
          }
        }
      } else {
        console.log(CYAN + `[updateTaskRunningPercentage] ${parentJobId}: Using totalJobs=${mpTotal} (expectedJobs=${parentJob.expectedJobs}, allJobsQueued=false - still queueing)` + RESET);
      }

      const mpCompleted = parentJob.completedJobs || 0;
      const mpPercentage = mpTotal > 0 ? (mpCompleted / mpTotal) * 100 : 0;

      marketplaceBreakdown[marketplace] = {
        total: mpTotal,
        completed: mpCompleted,
        percentage: mpPercentage
      };

      totalJobs += mpTotal;
      completedJobs += mpCompleted;

      // Additional safety check for individual marketplace overflow
      if (mpCompleted > mpTotal && mpTotal > 0) {
        console.log(RED + `[updateTaskRunningPercentage] MARKETPLACE OVERFLOW: ${marketplace} completed ${mpCompleted} exceeds total ${mpTotal}` + RESET);
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
      console.log(RED + `[updateTaskRunningPercentage] TOTAL OVERFLOW: completed ${completedJobs} exceeds total ${totalJobs}, percentage would be ${runningPercentage.toFixed(2)}%` + RESET);
    }
  }

  taskStatus[taskId].runningPercentage = Math.min(runningPercentage, 100);
  taskStatus[taskId].totalJobs = totalJobs;
  taskStatus[taskId].completedJobs = completedJobs;
  taskStatus[taskId].lastUpdate = Date.now();
  taskStatus[taskId].lastProgressUpdate = Date.now(); // Track last progress update

  // Summary section - Keep only TOTAL SUMMARY logs
  console.log(`\n${BLUE}[updateTaskRunningPercentage] TOTAL SUMMARY:${RESET}`);
  console.log(BLUE + `  Total Jobs: ${totalJobs} (` +
    marketplacesToCheck.map(mp =>
      `${mp.charAt(0).toUpperCase() + mp.slice(1)}: ${marketplaceBreakdown[mp]?.total || 0}`
    ).join(', ') + `)` + RESET);
  console.log(BLUE + `  Completed: ${completedJobs}` + RESET);
  console.log(MAGENTA + `  Overall Progress: ${runningPercentage.toFixed(2)}%` + RESET);

  // Overall progress bar
  const overallBarLength = 30;
  const overallFilledLength = Math.min(overallBarLength, Math.round((runningPercentage / 100) * overallBarLength));
  const overallProgressBar = '█'.repeat(overallFilledLength) + '░'.repeat(Math.max(0, overallBarLength - overallFilledLength));
  console.log(MAGENTA + `  [${overallProgressBar}] ${runningPercentage.toFixed(2)}%` + RESET);

  // Trigger completion when:
  // 1. We reach 100% normally, OR
  // 2. We have partial queueing AND all actual jobs are complete
  const shouldTriggerCompletion =
    taskStatus[taskId].runningPercentage >= 100 ||
    (hasPartialQueueing && allActualJobsComplete && completedJobs === actualTotal && actualTotal > 0);

  if (shouldTriggerCompletion) {
    // Check if recently completed to prevent duplicate completion calls
    const isRecentlyCompleted = false;

    if (!isRecentlyCompleted) {
      if (hasPartialQueueing && taskStatus[taskId].runningPercentage < 100) {
        console.log(YELLOW + `[Partial Completion] Task ${taskId} completed all ${completedJobs}/${actualTotal} actual jobs (${expectedTotal} were expected)` + RESET);
        console.log(YELLOW + `  Triggering completion at ${runningPercentage.toFixed(2)}% due to partial queueing` + RESET);
      } else {
        console.log(GREEN + `[Completion] Task ${taskId} reached ${runningPercentage.toFixed(2)}% - triggering completion` + RESET);
      }

      // Force to 100% for UI consistency
      taskStatus[taskId].runningPercentage = 100;
      await checkTaskCompletionAndScheduleNext(taskId);
    }
  }

  // Detect and handle partial queueing mismatch
  // This occurs when expectedJobs > actualJobs and allJobsQueued is false
  if (!shouldTriggerCompletion && totalJobs === 0 && expectedTotal > 0) {
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
      console.log(RED + `\n[PARTIAL QUEUEING MISMATCH DETECTED] Task ${taskId}` + RESET);
      console.log(RED + `  TaskStatus shows: ${completedJobs}/${totalJobs} (${runningPercentage.toFixed(1)}%)` + RESET);
      console.log(RED + `  Expected total: ${expectedTotal} jobs` + RESET);
      console.log(RED + `  Actual total: ${actualTotal} jobs` + RESET);
      console.log(RED + `  Mismatch details: ${mismatchDetails.join(', ')}` + RESET);

      // Check if we should trigger a restart
      const restartKey = `partial-queue-restart:${taskId}`;
      if (!tasksBeingRestarted.has(restartKey)) {
        tasksBeingRestarted.add(restartKey);

        console.log(YELLOW + `\n[RECOVERY] Scheduling automatic task restart for ${taskId} due to partial queueing mismatch` + RESET);
        console.log(YELLOW + `  This will retry queueing the missing ${expectedTotal - actualTotal} jobs` + RESET);

        // Schedule restart after a short delay
        setTimeout(async () => {
          try {
            console.log(MAGENTA + `\n[RECOVERY] Triggering task restart for ${taskId}` + RESET);
            await triggerTaskRestart(taskId);
          } catch (error) {
            console.error(RED + `[ERROR - Task Recovery - Task: ${taskId}] Failed to restart task: ${error}` + RESET);
          } finally {
            tasksBeingRestarted.delete(restartKey);
          }
        }, 5000); // 5 second delay
      } else {
        console.log(YELLOW + `[RECOVERY] Task restart already scheduled for ${taskId}` + RESET);
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
async function checkTaskCompletionAndScheduleNext(taskId: string) {

  const task = activeTasks.get(taskId);
  if (!task) return;

  // Check if task is already in completion process
  if (tasksInCompletionProcess.has(taskId)) {
    console.log(YELLOW + `[Task Completion] Task ${taskId} is already being completed, skipping duplicate call` + RESET);
    return;
  }

  // Mark task as in completion process
  tasksInCompletionProcess.add(taskId);

  try {
    // Log completion transition
    const currentLoopCount = loopCounterStats[taskId] || 1;
    console.log(GREEN + `[Task Completion] Task ${taskId} - Loop #${currentLoopCount} completed successfully` + RESET);

    // Mark task as recently completed

    setTaskStatus(taskId, 'COMPLETED', 'task completion');
    // Add 0.5-second delay before transitioning to SCHEDULED
    await new Promise(resolve => setTimeout(resolve, 500));

    // Handle task cleanup
    await handleTaskCleanup(taskId);

    // Clear removal trackingCON
    delete traitsToRemove[taskId];
    delete removedTokens[taskId];

    // Schedule next loop FIRST
    await scheduleNextLoop(taskId);

    // Mark as scheduled BEFORE clearing parent jobs
    setTaskStatus(taskId, 'SCHEDULED', 'next loop scheduled');

    // Add delay to ensure scheduled processors see SCHEDULED status before parent jobs are cleared
    await new Promise(resolve => setTimeout(resolve, 100));

    // Clear parent jobs AFTER task status is finalized - this ensures clean state for next loop
    const marketplaces = ['opensea', 'magiceden', 'blur'];
    for (const mp of marketplaces) {
      const parentJobId = getParentJobId(taskId, mp);
      if (parentJobs.has(parentJobId)) {
        console.log(CYAN + `[Task Completion] Clearing parent job ${parentJobId} after completion` + RESET);
        parentJobs.delete(parentJobId);
        // Also clear processed jobs tracking
        processedJobsPerParent.delete(parentJobId);
      }
    }
  } catch (error) {
    console.error(RED + `[ERROR - Task Completion - Task: ${taskId}] Error during completion process: ${error}` + RESET);
    // Remove from completion process on error
    tasksInCompletionProcess.delete(taskId);
    throw error;
  } finally {
    // Always remove from completion process when done
    tasksInCompletionProcess.delete(taskId);
  }
}

// Helper function to handle task cleanup (trait/token removal)
async function handleTaskCleanup(taskId: string) {
  const task = activeTasks.get(taskId);
  if (!task) return;

  const wallet = await walletCache.getWalletById(task.walletId);
  const WALLET_PRIVATE_KEY = decryptPrivateKey(wallet?.encryptedPrivateKey as string) as string;
  const { currencySymbol } = await calculateBidPrice(task, 0, "opensea");

  let totalCancellationJobs = 0;

  // Cancel removed trait bids
  if (traitsToRemove[taskId]?.length > 0) {
    const traitJobs = await cancelRemovedTraitBids(traitsToRemove[taskId], task._id, WALLET_PRIVATE_KEY, currencySymbol);
    totalCancellationJobs += traitJobs;
  }

  // Cancel removed token bids
  if (removedTokens[taskId]?.opensea?.length > 0 || removedTokens[taskId]?.magiceden?.length > 0) {
    const tokenJobs = await cancelRemovedTokenBids(removedTokens[taskId], task._id, WALLET_PRIVATE_KEY, currencySymbol);
    totalCancellationJobs += tokenJobs;
  }

  return totalCancellationJobs;
}

// Helper function to mark a job as processed regardless of outcome
async function markJobAsProcessed(taskId: string, marketplace: string, jobId: string, success: boolean = true) {
  const timestamp = new Date().toISOString();
  const currentLoopCount = loopCounterStats[taskId] || 1;

  // Validate task exists and is active
  const task = activeTasks.get(taskId);
  if (!task) return

  // Get parent job with current loop number
  let parentJobId = getParentJobId(taskId, marketplace);
  let parentJob = parentJobs.get(parentJobId);

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
        console.log(YELLOW + `[markJobAsProcessed] Job ${jobId} is from loop ${jobLoop}, current loop is ${currentLoopCount}` + RESET);
        // Process it with the old parent job
        parentJob = oldParentJob;
        parentJobId = oldParentJobId;
      } else {
        console.log(YELLOW + `[markJobAsProcessed] Ignoring job ${jobId} - no parent job found for ${parentJobId} or ${oldParentJobId}` + RESET);
        console.log(YELLOW + `  Current loop: ${currentLoopCount}, Job loop: ${jobLoop}` + RESET);
        return;
      }
    } else {
      console.log(YELLOW + `[markJobAsProcessed] Ignoring job ${jobId} - no parent job found for ${parentJobId}` + RESET);
      console.log(YELLOW + `  Current loop: ${currentLoopCount}` + RESET);
      return;
    }
  }

  // If we found an old parent job, skip loop validation
  // Otherwise validate the job belongs to the current loop
  if (parentJob.loopNumber !== currentLoopCount && parentJobId.includes(`loop${currentLoopCount}`)) {
    console.log(YELLOW + `[markJobAsProcessed] Job ${jobId} from loop ${parentJob.loopNumber} doesn't match current loop ${currentLoopCount}, ignoring` + RESET);
    return;
  }

  console.log(CYAN + `  Total jobs: ${parentJob.totalJobs}` + RESET);
  console.log(CYAN + `  Processed jobs: ${parentJob.completedJobs}` + RESET);
  console.log(CYAN + `  Failed jobs: ${parentJob.failedJobs || 0}` + RESET);
  const successCount = parentJob.completedJobs - (parentJob.failedJobs || 0);
  console.log(CYAN + `  Successful jobs: ${successCount}` + RESET);
  console.log(CYAN + `  Progress: ${parentJob.totalJobs > 0 ? ((parentJob.completedJobs / parentJob.totalJobs) * 100).toFixed(2) : 0}% (${parentJob.completedJobs}/${parentJob.totalJobs})` + RESET);

  // Check if parent job is already complete - prevent overflow
  if (parentJob.completedJobs >= parentJob.totalJobs && parentJob.totalJobs > 0) {
    console.log(YELLOW + `[markJobAsProcessed] Parent job ${parentJobId} already complete (${parentJob.completedJobs}/${parentJob.totalJobs})` + RESET);
    console.log(YELLOW + `  Ignoring late job: ${jobId}` + RESET);

    // Safety check: if allJobsQueued is false but we're already complete, something went wrong
    if (!parentJob.allJobsQueued) {
      console.log(RED + `  WARNING: Parent job complete but allJobsQueued=false - this indicates a tracking issue` + RESET);
      parentJob.allJobsQueued = true; // Fix the state
    }
    return;
  }

  if (taskStatus[taskId]) {
    setTaskStatus(taskId, 'RUNNING', 'job marked as processed');
  }

  // CRITICAL: Use a mutex lock to prevent race conditions during counter increment
  const counterLockKey = `counter-lock:${parentJobId}`;
  const lockManager = new DistributedLockManager({
    lockPrefix: '{counter}:',
    defaultTTLSeconds: 5
  });

  let lockAcquired = false;
  try {
    // Acquire lock with short timeout to prevent deadlocks
    lockAcquired = await lockManager.acquireLock(counterLockKey, 5);

    if (!lockAcquired) {
      console.log(RED + `[markJobAsProcessed] Failed to acquire counter lock for ${parentJobId}, retrying...` + RESET);
      // Wait a bit and retry once
      await new Promise(resolve => setTimeout(resolve, 50));
      lockAcquired = await lockManager.acquireLock(counterLockKey, 5);

      if (!lockAcquired) {
        const lockTaskId = getTaskIdForLog({ data: { taskId, _id: taskId } });
        console.error(RED + `[ERROR - Job Processing - Task: ${lockTaskId}] CRITICAL: Could not acquire lock for counter increment! Job ${jobId} may not be counted.` + RESET);
        // Continue anyway to prevent job from being stuck
      }
    }

    // Re-fetch parent job inside the lock to get latest values
    parentJob = parentJobs.get(parentJobId);
    if (!parentJob) {
      const missingTaskId = getTaskIdForLog({ data: { taskId, _id: taskId } });
      console.error(RED + `[ERROR - Job Processing - Task: ${missingTaskId}] Parent job disappeared during lock acquisition!` + RESET);
      return;
    }

    // Log before increment for debugging
    const beforeCount = parentJob.completedJobs;
    const beforeFailedCount = parentJob.failedJobs || 0;

    // Increment appropriate counter based on success/failure
    if (success) {
      // Increment completed jobs with hard cap protection
      parentJob.completedJobs++;
      console.log(CYAN + `[markJobAsProcessed] Success counter incremented for ${parentJobId}: ${beforeCount} -> ${parentJob.completedJobs}` + RESET);
    } else {
      // Increment both completed (for progress) and failed (for tracking) counters
      parentJob.completedJobs++;
      parentJob.failedJobs = (parentJob.failedJobs || 0) + 1;
      console.log(YELLOW + `[markJobAsProcessed] Failed job counted - completed: ${beforeCount} -> ${parentJob.completedJobs}, failed: ${beforeFailedCount} -> ${parentJob.failedJobs}` + RESET);
    }

    // Additional safety check to prevent overflow (should never happen with above logic)
    if (parentJob.completedJobs > parentJob.totalJobs) {
      console.log(RED + `[markJobAsProcessed] OVERFLOW DETECTED: ${parentJobId} completed ${parentJob.completedJobs} exceeds total ${parentJob.totalJobs}, capping to total` + RESET);
      parentJob.completedJobs = parentJob.totalJobs;
    }
  } finally {
    // Always release the lock
    if (lockAcquired) {
      try {
        await lockManager.releaseLock(counterLockKey);
      } catch (error) {
        const releaseTaskId = getTaskIdForLog({ data: { taskId, _id: taskId } });
        console.error(RED + `[ERROR - Lock Release - Task: ${releaseTaskId}] Error releasing counter lock: ${error}` + RESET);
      }
    }
  }

  // If this was the first job and totalJobs is 0, set it to 1
  if (parentJob.totalJobs === 0) {
    console.log(YELLOW + `[markJobAsProcessed] Parent job ${parentJobId} had 0 total jobs, setting to 1` + RESET);
    parentJob.totalJobs = 1;
  }
  // Update task running percentage
  try {
    await updateTaskRunningPercentage(taskId);
  } catch (error) {
    const percentTaskId = getTaskIdForLog({ data: { taskId, _id: taskId } });
    console.error(RED + `[ERROR - Job Processing - Task: ${percentTaskId}] Error updating task percentage: ${error}` + RESET);
    console.error(RED + `[ERROR - Job Processing - Task: ${percentTaskId}]   This may cause stuck percentage!` + RESET);
  }

  // Check if all jobs are complete for this parent job
  if (parentJob.completedJobs >= parentJob.totalJobs) {
    const successCount = parentJob.completedJobs - (parentJob.failedJobs || 0);
    console.log(GREEN + `[Parent Job Complete] ${parentJobId}: All ${parentJob.totalJobs} jobs processed (${successCount} successful, ${parentJob.failedJobs || 0} failed)` + RESET);

    // Check if all parent jobs for this task are complete
    const allComplete = checkAllParentJobsComplete(taskId);
    console.log(GREEN + `[markJobAsProcessed] All parent jobs complete check: ${allComplete}` + RESET);

    if (allComplete) {
      const currentLoopCount = loopCounterStats[taskId] || 1;
      console.log(MAGENTA + `[All Jobs Complete] Task ${taskId} - Loop #${currentLoopCount}: All marketplace jobs complete, checking percentage...` + RESET);

      // Do NOT force completion here - let updateTaskRunningPercentage handle it naturally
      // This prevents duplicate completion calls
      console.log(CYAN + `[All Jobs Complete] Current percentage: ${taskStatus[taskId]?.runningPercentage || 0}%` + RESET);
      console.log(CYAN + `[All Jobs Complete] Completion will be triggered by updateTaskRunningPercentage when it reaches 100%` + RESET);
    }
  }
}

// Helper function to mark a cancellation job as processed (similar to markJobAsProcessed)
// Removed markCancellationJobAsProcessed and checkAllCancellationJobsComplete - now using cancellationTracker

// Helper function to clean up old parent jobs
function cleanupOldParentJobs(taskId: string, currentLoop: number, keepLoops: number = 2) {
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
    console.log(YELLOW + `[cleanupOldParentJobs] Removed old parent job: ${parentJobId}` + RESET);
  }

  if (parentJobsToRemove.length > 0) {
    console.log(GREEN + `[cleanupOldParentJobs] Cleaned up ${parentJobsToRemove.length} old parent jobs for task ${taskId}` + RESET);
  }
}

// Helper function to clear task timers
function clearTaskTimers(taskId: string) {
  // Clear existing timeout
  const existingTimeout = taskTimeouts.get(taskId);
  if (existingTimeout) {
    clearTimeout(existingTimeout);
    taskTimeouts.delete(taskId);
  }

  // Clear existing countdown timer
  const existingCountdownTimer = taskCountdownTimers.get(taskId);
  if (existingCountdownTimer) {
    clearInterval(existingCountdownTimer);
    taskCountdownTimers.delete(taskId);
  }
}


// Track last schedule time to prevent rapid re-scheduling
const lastScheduleTimes = new Map<string, number>();

async function scheduleNextLoop(taskId: string): Promise<void> {
  const currentTime = Date.now();
  const timestamp = new Date(currentTime).toISOString();


  // Get the task from activeTasks
  const task = activeTasks.get(taskId);
  if (!task) {
    lastScheduleTimes.delete(taskId);
    return;
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
    console.log(YELLOW + `[scheduleNextLoop] Loop interval ${loopInterval}s is below minimum, setting to ${MIN_LOOP_INTERVAL}s` + RESET);
    loopInterval = MIN_LOOP_INTERVAL;
  }

  // Loop counter is now incremented in startTask when the next loop actually begins

  // Set up next loop time and countdown timer
  const nextLoopTime = new Date(Date.now() + loopInterval * 1000);
  const secondsUntilNextLoop = Math.floor((nextLoopTime.getTime() - Date.now()) / 1000);


  // Update task status
  if (taskStatus[taskId]) {
    setTaskStatus(taskId, 'SCHEDULED', 'next loop scheduled');
    taskStatus[taskId].waitingTime = secondsUntilNextLoop;
    taskStatus[taskId].nextLoopTime = nextLoopTime;
  }

  // Clear any existing countdown timer before setting a new one
  const existingCountdownTimer = taskCountdownTimers.get(taskId);
  if (existingCountdownTimer) {
    clearInterval(existingCountdownTimer);
    taskCountdownTimers.delete(taskId);
  }

  // Set up countdown timer
  const countdownTimer = setInterval(() => {
    const remainingSeconds = taskStatus[taskId]?.nextLoopTime ?
      Math.max(0, Math.floor((taskStatus[taskId].nextLoopTime.getTime() - Date.now()) / 1000)) : 0;

    if (taskStatus[taskId]) {
      taskStatus[taskId].waitingTime = remainingSeconds;
    }

    if (remainingSeconds <= 0) {
      clearInterval(countdownTimer);
      taskCountdownTimers.delete(taskId);
    }
  }, 1000);

  taskCountdownTimers.set(taskId, countdownTimer);

  // Clear any existing timeout for this task
  const existingTimeout = taskTimeouts.get(taskId);
  if (existingTimeout) {
    clearTimeout(existingTimeout);
    taskTimeouts.delete(taskId);
  }

  // Validate task is still active before scheduling
  if (!activeTasks.has(taskId)) {
    console.log(YELLOW + `[scheduleNextLoop] ${timestamp} - Task ${taskId} no longer active, skipping loop scheduling` + RESET);
    cleanupTaskResources(taskId);
    lastScheduleTimes.delete(taskId);
    return;
  }

  try {
    // Schedule the next iteration with setTimeout
    const timeoutId = setTimeout(async () => {

      // Acquire scheduling lock when timer expires, not before
      if (!acquireLoopSchedulingLock(taskId)) {
        return;
      }

      try {
        // Double-check task is still active and running
        const currentTask = activeTasks.get(taskId);
        const currentStatus = taskStatus[taskId];


        // Only skip if task is STOPPED (not CANCELLING, as user might want to restart)
        if (currentStatus?.status === 'STOPPED') {
          console.log(YELLOW + `[scheduleNextLoop] Task ${taskId} is STOPPED, skipping startTask` + RESET);
          return;
        }

        if (currentTask?.running) {
          // Clear recently completed flag before starting next loop
          await startTask(currentTask, true);
        }
      } catch (error) {
        console.error(RED + `[ERROR - Task Scheduler - Task: ${taskId}] Error in loop execution: ` + RESET, error);
      } finally {
        // Always release the lock after execution
        releaseLoopSchedulingLock(taskId);
      }
    }, loopInterval * 1000);

    taskTimeouts.set(taskId, timeoutId);
  } catch (error) {
    // If scheduling fails, clean up resources
    console.error(RED + `[scheduleNextLoop] Error scheduling task ${taskId}: ` + RESET, error);
    lastScheduleTimes.delete(taskId);
  }
}

// Initialize queue event handlers - called after queue is created
function initializeQueueEventHandlers() {
  queueEvents.on('completed', async ({ jobId }) => {
    try {
      // Skip schedule jobs - they are orchestration jobs, not actual bid jobs
      if (jobId.includes('-schedule')) {
        return;
      }

      // Log all job completions for debugging
      console.log(BLUE + `[Queue Event] Job completed: ${jobId}` + RESET);

      // Skip cancellation jobs - they are handled in the cancel functions directly
      if (jobId.includes('CANCEL_OPENSEA_BID') ||
        jobId.includes('CANCEL_MAGICEDEN_BID') ||
        jobId.includes('CANCEL_BLUR_BID')) {
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
          console.log(CYAN + `[Queue Event] Numeric job ID ${jobId} completed - no task context available` + RESET);
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
        }
      } else {
        console.log(YELLOW + `[Queue Event] Could not extract task/marketplace from job ID: ${jobId}` + RESET);
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

      console.error(RED + `[ERROR - Queue Event - Task: ${extractedTaskId}] Error handling job completion for ${jobId}:` + RESET, error);
      console.error(RED + `[ERROR - Queue Event - Task: ${extractedTaskId}] Stack trace:` + RESET);
      console.error(error);

      // Log error details for debugging
      if (error instanceof Error) {
        console.error(RED + `[ERROR - Queue Event - Task: ${extractedTaskId}] Error name: ${error.name}` + RESET);
        console.error(RED + `[ERROR - Queue Event - Task: ${extractedTaskId}] Error message: ${error.message}` + RESET);
      }

      // Don't let errors in event handler crash the process
      // This ensures other jobs can still be processed
    }
  });

  queueEvents.on('failed', async ({ jobId, failedReason }) => {
    try {
      // Handle schedule job failures specially - log to file
      if (jobId.includes('-schedule')) {
        // Log scheduled job failures to file
        const logsDir = path.join(process.cwd(), 'logs');
        if (!fs.existsSync(logsDir)) {
          fs.mkdirSync(logsDir, { recursive: true });
        }

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

        console.log(RED + `[Queue Event] Scheduled job failed: ${jobId}` + RESET);
        console.log(RED + `[Queue Event] Reason: ${failedReason}` + RESET);

        // Write to scheduled job failures log
        const logPath = path.join(logsDir, 'scheduled-job-failures.log');
        fs.appendFileSync(logPath, JSON.stringify(errorDetails, null, 2) + '\n---\n');

        // Handle timeout failures by restarting the task
        if (failedReason?.includes('timed out after') && jobId.includes('-schedule')) {
          const taskId = jobId.split('-')[0];

          // Use a distributed lock to prevent multiple restarts
          const restartLockKey = `schedule-timeout-restart:${taskId}`;
          const lockManager = new DistributedLockManager({
            lockPrefix: '{task-restart}:',
            defaultTTLSeconds: 60
          });

          try {
            const lockAcquired = await lockManager.acquireLock(restartLockKey, 60);
            if (lockAcquired) {
              try {
                console.log(YELLOW + `[Queue Event] Schedule job timeout detected for task ${taskId}, triggering restart` + RESET);

                // Get the task
                const task = await Task.findById(taskId);
                if (task) {
                  // Clear stuck parent jobs for all marketplaces
                  const marketplaces = ['opensea', 'magiceden', 'blur'];
                  for (const mp of marketplaces) {
                    const parentJobId = getParentJobId(taskId, mp);
                    if (parentJobs.has(parentJobId)) {
                      console.log(YELLOW + `[Queue Event] Clearing stuck parent job ${parentJobId}` + RESET);
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
                      // Ignore errors - lock may not exist
                    }
                  }

                  // Reset task status and restart
                  console.log(GREEN + `[Queue Event] Restarting task ${taskId} after schedule job timeout` + RESET);
                  setTaskStatus(taskId, 'SCHEDULED', 'restarting after timeout');
                  await startTask(task, true);

                  // Log the restart
                  const restartLogPath = path.join(logsDir, 'task-restarts.log');
                  const restartDetails = {
                    timestamp: new Date().toISOString(),
                    taskId,
                    reason: 'schedule_job_timeout',
                    failedJobId: jobId,
                    failedReason
                  };
                  fs.appendFileSync(restartLogPath, JSON.stringify(restartDetails, null, 2) + '\n---\n');
                }
              } finally {
                await lockManager.releaseLock(restartLockKey);
              }
            } else {
              console.log(YELLOW + `[Queue Event] Another process is already restarting task ${taskId}` + RESET);
            }
          } catch (lockError) {
            console.error(RED + `[Queue Event] Error acquiring restart lock for task ${taskId}: ${lockError}` + RESET);
          }

          // Don't continue with the normal retry exhaustion logic
          return;
        }

        // Check if this is the final failure (after all retries)
        if (job && job.attemptsMade >= (job.opts?.attempts || 1)) {
          const taskId = job.data?._id;
          if (taskId && taskStatus[taskId]) {
            console.log(RED + `[Queue Event] Scheduled job ${jobId} exhausted all retries - recovering task ${taskId}` + RESET);

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
              console.log(YELLOW + `[Queue Event] Created empty parent job for failed scheduled job: ${parentJobId}` + RESET);
            }

            // Transition task out of stuck QUEUEING status
            if (taskStatus[taskId].status === 'QUEUEING') {
              setTaskStatus(taskId, 'RUNNING', 'scheduled job failed - recovering');

              // Check if all parent jobs are complete to trigger next loop
              const allComplete = checkAllParentJobsComplete(taskId);
              if (allComplete) {
                console.log(GREEN + `[Queue Event] All parent jobs complete for task ${taskId}, triggering completion after scheduled job failure` + RESET);
                await checkTaskCompletionAndScheduleNext(taskId);
              }
            }
          }
        }

        return;
      }

      // Check if this is a cancellation job
      const isCancellationJob = jobId.includes('CANCEL_OPENSEA_BID') ||
        jobId.includes('CANCEL_MAGICEDEN_BID') ||
        jobId.includes('CANCEL_BLUR_BID');

      // Try to extract task and marketplace info from job ID
      let taskId: string | undefined;
      let marketplace: string | undefined;

      if (isCancellationJob) {
        // Extract marketplace from job name
        if (jobId.includes('CANCEL_OPENSEA_BID')) {
          marketplace = 'opensea';
          // Extract taskId from pattern: CANCEL_OPENSEA_BID-{taskId}-{orderId}
          const match = jobId.match(/CANCEL_OPENSEA_BID-([^-]+)-/);
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
            console.log(YELLOW + `[Queue Event] Cancellation job failed but tracking is handled in cancel functions` + RESET);
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
            console.log(YELLOW + `[Queue Event - MAIN FAILED] Processing failure for task ${taskId}, marketplace ${marketplace}, jobId: ${jobId}` + RESET);
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

      console.error(RED + `[ERROR - Queue Event - Task: ${extractedTaskId}] Error handling job failure for ${jobId}:` + RESET, error);
      console.error(RED + `[ERROR - Queue Event - Task: ${extractedTaskId}] Stack trace:` + RESET);
      console.error(error);

      // Log error details for debugging
      if (error instanceof Error) {
        console.error(RED + `[ERROR - Queue Event - Task: ${extractedTaskId}] Error name: ${error.name}` + RESET);
        console.error(RED + `[ERROR - Queue Event - Task: ${extractedTaskId}] Error message: ${error.message}` + RESET);
      }

      // Don't let errors in event handler crash the process
      // This ensures other jobs can still be processed
    }
  });

  // Handle stalled jobs - critical for preventing stuck tasks
  queueEvents.on('stalled', async ({ jobId }) => {
    try {
      console.log(YELLOW + `[Queue Event] Job stalled: ${jobId}` + RESET);

      // Skip schedule jobs - they are orchestration jobs, not actual bid jobs
      if (jobId.includes('-schedule')) {
        console.log(CYAN + `[Queue Event] Skipping schedule job tracking (stalled): ${jobId}` + RESET);
        return;
      }

      // Check if this is a cancellation job
      const isCancellationJob = jobId.includes('CANCEL_OPENSEA_BID') ||
        jobId.includes('CANCEL_MAGICEDEN_BID') ||
        jobId.includes('CANCEL_BLUR_BID');

      // Try to extract task and marketplace info from job ID
      let taskId: string | undefined;
      let marketplace: string | undefined;

      if (isCancellationJob) {
        // Extract marketplace from job name
        if (jobId.includes('CANCEL_OPENSEA_BID')) {
          marketplace = 'opensea';
          // Extract taskId from pattern: CANCEL_OPENSEA_BID-{taskId}-{orderId}
          const match = jobId.match(/CANCEL_OPENSEA_BID-([^-]+)-/);
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
            console.log(YELLOW + `[Queue Event] Processing stalled cancellation for task ${taskId}, marketplace ${marketplace}` + RESET);
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
            console.log(YELLOW + `[Queue Event - STALLED] Processing stalled job as completed for task ${taskId}, marketplace ${marketplace}, jobId: ${jobId}` + RESET);
            await safeMarkJobAsProcessed(taskId, marketplace, jobId, true, async (tid, mp, increment) => {
              await markJobAsProcessed(tid, mp, jobId);
            });
          }
        } else {
          console.log(RED + `[Queue Event] Could not extract task/marketplace from stalled job ID: ${jobId}` + RESET);
        }
      }
    } catch (error) {
      // Extract taskId from jobId when job is not in scope
      const parts = jobId.split('-');
      const stalledTaskId = (parts.length >= 3 && parts[0] === 'BID' && parts[2]) ? parts[2] : 'N/A';
      console.error(RED + `[ERROR - Queue Event - Task: ${stalledTaskId}] Error handling stalled job ${jobId}:` + RESET, error);
      console.error(RED + `[Queue Event] Stack trace:` + RESET);
      console.error(error);

      // Log error details for debugging
      if (error instanceof Error) {
        console.error(RED + `[ERROR - Queue Event - Task: ${stalledTaskId}] Error name: ${error.name}` + RESET);
        console.error(RED + `[ERROR - Queue Event - Task: ${stalledTaskId}] Error message: ${error.message}` + RESET);
      }

      // Don't let errors in event handler crash the process
      // This ensures other jobs can still be processed
    }
  });

  blurQueueEvents.on('completed', async ({ jobId }) => {
    try {
      // Skip schedule jobs - they are orchestration jobs, not actual bid jobs
      if (jobId.includes('-schedule')) {
        return;
      }

      // Extract wallet address from jobId (last element after split)
      const parts = jobId.split('-');
      const walletAddress = parts[parts.length - 1];

      // Track completion if it's a valid wallet address
      if (walletAddress && walletAddress.toLowerCase().startsWith('0x')) {
        blurCompletionTracker.trackCompletion(walletAddress);
      }

      // Try to extract task and marketplace info from job ID
      let taskId: string | undefined;
      let marketplace = 'blur'; // Default to blur for blur queue

      // Handle bid jobs (e.g., bid-taskId-blur-...)
      if (jobId.startsWith('bid-')) {
        if (parts.length >= 2) {
          taskId = parts[1];
        }
      }
      // Handle other job formats
      else {
        // Try to find task ID in the job ID
        if (parts.length >= 1) {
          // Check if first part is a valid task ID
          if (activeTasks.has(parts[0])) {
            taskId = parts[0];
          } else if (parts.length >= 2 && activeTasks.has(parts[1])) {
            taskId = parts[1];
          }
        }
      }

      // Process if we found a task ID
      if (taskId) {
        const task = activeTasks.get(taskId);
        if (task && task.running) {
          console.log(GREEN + `[Blur Queue Event] Processing completion for task ${taskId}, marketplace ${marketplace}, jobId: ${jobId}` + RESET);
          await markJobAsProcessed(taskId, marketplace, jobId);
        }
      }
    } catch (error) {
      console.error(RED + `[Blur Queue Event] Error handling job completion for ${jobId}:` + RESET, error);
      console.error(RED + `[Blur Queue Event] Stack trace:` + RESET);
      console.error(error);

      // Log error details for debugging
      if (error instanceof Error) {
        console.error(RED + `[Blur Queue Event] Error name: ${error.name}` + RESET);
        console.error(RED + `[Blur Queue Event] Error message: ${error.message}` + RESET);
      }

      // Don't let errors in event handler crash the process
      // This ensures other jobs can still be processed
    }
  });

  blurQueueEvents.on('failed', async ({ jobId, failedReason }) => {
    try {
      // Skip schedule jobs - they are orchestration jobs, not actual bid jobs
      if (jobId.includes('-schedule')) {
        console.log(CYAN + `[Blur Queue Event] Skipping schedule job tracking (failed): ${jobId}` + RESET);
        return;
      }

      // Log all job failures for debugging
      console.log(RED + `[Blur Queue Event] Job failed: ${jobId} - ${failedReason}` + RESET);

      // Try to extract task and marketplace info from job ID
      let taskId: string | undefined;
      let marketplace = 'blur'; // Default to blur for blur queue

      // Handle bid jobs (e.g., bid-taskId-blur-...)
      if (jobId.startsWith('bid-')) {
        const parts = jobId.split('-');
        if (parts.length >= 2) {
          taskId = parts[1];
        }
      }
      // Handle other job formats
      else {
        // Try to find task ID in the job ID
        const parts = jobId.split('-');
        if (parts.length >= 1) {
          // Check if first part is a valid task ID
          if (activeTasks.has(parts[0])) {
            taskId = parts[0];
          } else if (parts.length >= 2 && activeTasks.has(parts[1])) {
            taskId = parts[1];
          }
        }
      }

      // Process if we found a task ID

      if (taskId) {

        if (!errorStats[taskId]) {
          errorStats[taskId] = {
            opensea: 0,
            blur: 0,
            magiceden: 0
          }
        }
        // Increment error stats for blur marketplace
        errorStats[taskId].blur++;
        const task = activeTasks.get(taskId);
        if (task && task.running) {
          console.log(YELLOW + `[Blur Queue Event - FAILED] Processing failure for task ${taskId}, marketplace ${marketplace}, jobId: ${jobId}` + RESET);
          await safeMarkJobAsProcessed(taskId, marketplace, jobId, false, async (tid, mp, increment) => {
            await markJobAsProcessed(tid, mp, jobId, false);
          });
        }
      } else {
        console.log(YELLOW + `[Blur Queue Event] Could not extract task ID from failed job ID: ${jobId}` + RESET);
      }
    } catch (error) {
      console.error(RED + `[Blur Queue Event] Error handling job failure for ${jobId}:` + RESET, error);
      console.error(RED + `[Blur Queue Event] Stack trace:` + RESET);
      console.error(error);

      // Log error details for debugging
      if (error instanceof Error) {
        console.error(RED + `[Blur Queue Event] Error name: ${error.name}` + RESET);
        console.error(RED + `[Blur Queue Event] Error message: ${error.message}` + RESET);
      }

      // Don't let errors in event handler crash the process
      // This ensures other jobs can still be processed
    }
  });
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
  loopCounterStats
};

const BROADCAST_INTERVAL = 1000;
setInterval(broadcastBidRates, BROADCAST_INTERVAL);

async function broadcastBidRates() {
  bidRates = getAllBidRates();


  const message = JSON.stringify({
    type: 'bidRatesUpdate',
    data: { bidRates, bidCounts: bidStats, skipCounts: skipStats, errorCounts: errorStats, floorPrices, bestOffers, warningBids, warningMessage, rps, taskStatus, ownBidCounts, loopCounterStats }
  });

  clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  });
}

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

  clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  });
}

// Update the WebSocket connection handler to send initial state
wss.on('connection', async (ws) => {
  clients.add(ws);
  // Session established

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
        loopCounterStats
      },
    });

    ws.send(initialMessage);
  }

  ws.onmessage = async (event: WebSocket.MessageEvent) => {
    try {
      const message = JSON.parse(event.data as string);
      switch (message.endpoint) {
        case 'new-task':
          await processNewTask(message.data);
          break;
        case "import-tasks":
          await processImportedTasks(message.data);
          break;
        case 'updated-task':
          await updateTaskData(message.data);
          break;
        case 'toggle-status':
          await updateStatus(message.data);
          break;
        case 'stop-task':
          await stopTask(message.data, false);
          break;
        case 'delete-task':
          await deleteTask(message.data)
          break;
        case 'update-multiple-tasks-status':
          await updateMultipleTasksStatus(message.data);
          break;
        case 'globalMarketplaces':
          processGlobalMarketplaces(message.data)
          break
        case 'update-marketplace':
          await updateMarketplace(message.data)
          break

        case 'create-listing':
          await handleStartListing(message.data, ws)
          break
        case 'stop-listing':
          await handleStopListing(message.data, ws)
          break

        case 'update-listing':
          await handleUpdateListing(message.data, ws)
          break

        default:
          console.warn(YELLOW + `Unknown endpoint: ${message.endpoint}` + RESET);
      }
    } catch (error) {
      logger.logApiError("WebSocket message handler failed", {
        error: error as Error,
        endpoint: 'unknown',
        messageType: typeof event?.data,
        rawData: event?.data?.toString()?.substring(0, 100) // First 100 chars for debugging
      });
    }
  };

  ws.onclose = () => {
    // Remove client when disconnected
    clients.delete(ws);
    console.log(YELLOW + 'Session disconnected' + RESET);
  };
});

// Blur rate limiting is now handled by Redis and the BlurRateLimiter
// No need for in-memory tracking as we trust Blur's API responses

// Process Blur jobs
async function processBlurJob(job: Job) {
  // Declare walletAddress outside try block so it's accessible in catch/finally
  let walletAddress = '';

  try {
    // Update progress to indicate job started
    // Progress reporting removed - await job.updateProgress(5);

    // Check if password is available before processing
    if (!getPassword()) {
      const error = new Error('No password available. Job will be retried when password is set.');
      (error as any).code = 'NO_PASSWORD';
      throw error;
    }

    // Update progress after password check
    // Progress reporting removed - await job.updateProgress(10);

    // Extract wallet address from job data first (more reliable than parsing jobId)
    walletAddress = job.data?.address || job.data?.wallet;
    if (!walletAddress) {
      // Fallback to parsing jobId if address not in data
      const jobId = (job.opts as any)?.jobId || job.id;
      const jobParts = jobId.toString().split('-');
      const parsedWallet = jobParts[jobParts.length - 1];

      if (!parsedWallet || !parsedWallet.startsWith('0x')) {
        console.error(`[Blur Job] Cannot determine wallet address for job ${job.id}`);
        throw new Error('Cannot determine wallet address for rate limiting');
      }
      walletAddress = parsedWallet; // Assign the parsed wallet
    }

    // CRITICAL: Check rate limits BEFORE processing the job
    if (walletAddress && walletAddress.startsWith('0x')) {
      // First check if we can make a request using the rate limiter
      const canProceed = blurRateLimiter.canMakeRequest(walletAddress);
      if (!canProceed) {
        const waitTime = blurRateLimiter.getWaitTime(walletAddress);
        console.log(RED + `[Blur Rate Limit] Wallet ${walletAddress} is rate limited. Wait time: ${waitTime}ms` + RESET);

        try {
          // Reschedule the job with a delay
          await job.moveToDelayed(Date.now() + waitTime + 1000); // Add 1s buffer
          console.log(YELLOW + `[Blur Rate Limit] Job ${job.id} rescheduled for ${new Date(Date.now() + waitTime + 1000).toISOString()}` + RESET);
        } catch (error) {
          // If we can't move to delayed (e.g., lock already released), job will be retried automatically
          console.log(YELLOW + `[Blur Rate Limit] Could not delay job ${job.id}, will be retried: ${error}` + RESET);
        }
        return; // Exit without processing
      }

      // Check completion-based rate limiting
      const requiredDelay = blurCompletionTracker.getRequiredDelay(walletAddress);
      if (requiredDelay > 0) {
        console.log(YELLOW + `[Blur Completion Rate] Wallet ${walletAddress} needs ${requiredDelay}ms delay based on completion rate` + RESET);

        try {
          // Reschedule the job with a delay
          await job.moveToDelayed(Date.now() + requiredDelay + 1000); // Add 1s buffer
          console.log(YELLOW + `[Blur Completion Rate] Job ${job.id} rescheduled for ${new Date(Date.now() + requiredDelay + 1000).toISOString()}` + RESET);
        } catch (error) {
          // If we can't move to delayed (e.g., lock already released), job will be retried automatically
          console.log(YELLOW + `[Blur Completion Rate] Could not delay job ${job.id}, will be retried: ${error}` + RESET);
        }
        return; // Exit without processing
      }

      // Update progress before Redis checks
      // Progress reporting removed - await job.updateProgress(15);

      // Check for authentication blocks first
      try {
        const redis = redisClient.getClient();
        if (redis) {
          // Check authentication block
          const authBlockKey = `{blurAuth}:block:${walletAddress}`;
          const authBlockTime = await redis.get(authBlockKey);
          if (authBlockTime) {
            const blockTime = parseInt(authBlockTime);
            if (blockTime > Date.now()) {
              const waitTime = blockTime - Date.now();
              const waitSeconds = Math.round(waitTime / 1000);

              console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
              console.log(RED + `[Blur Auth Block] Wallet: ${walletAddress}` + RESET);
              console.log(RED + `[Blur Auth Block] Status: BLOCKED` + RESET);
              console.log(RED + `[Blur Auth Block] Wait Time: ${waitSeconds} seconds` + RESET);
              console.log(RED + `[Blur Auth Block] Resume At: ${new Date(blockTime).toLocaleTimeString()}` + RESET);
              console.log(RED + `[Blur Auth Block] Job: ${job.id || 'unknown'}` + RESET);
              console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);

              // Update progress periodically during wait
              // Progress reporting removed - was: const updateInterval = setInterval(() => job.updateProgress(20), 10000);
              await new Promise(resolve => setTimeout(resolve, waitTime + 2500));
              // Progress reporting removed - was: clearInterval(updateInterval);

              console.log(GREEN + `[Blur Auth Block] Resumed processing for wallet ${walletAddress.substring(0, 8)}...` + RESET);
            } else {
              // Block has expired, clean it up
              await redis.del(authBlockKey);
              console.log(CYAN + `[Blur Auth Block] Cleaned up expired block for wallet ${walletAddress.substring(0, 8)}...` + RESET);
            }
          }

          // Update progress after auth check
          // Progress reporting removed - await job.updateProgress(25);

          // Then check rate limit pauses
          const pauseKey = `{blurWallet}:pause:${walletAddress}`;
          const redisPauseUntil = await redis.get(pauseKey);
          if (redisPauseUntil) {
            const pauseTime = parseInt(redisPauseUntil);
            if (pauseTime > Date.now()) {
              const waitTime = pauseTime - Date.now();
              const waitSeconds = Math.round(waitTime / 1000);

              // Enhanced logging with more details
              console.log(YELLOW + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
              console.log(YELLOW + `[Blur API Rate Limit] Wallet: ${walletAddress}` + RESET);
              console.log(YELLOW + `[Blur API Rate Limit] Status: PAUSED` + RESET);
              console.log(YELLOW + `[Blur API Rate Limit] Wait Time: ${waitSeconds} seconds (${waitTime}ms)` + RESET);
              console.log(YELLOW + `[Blur API Rate Limit] Resume At: ${new Date(pauseTime).toLocaleTimeString()}` + RESET);
              console.log(YELLOW + `[Blur API Rate Limit] Job: ${job.id || 'unknown'}` + RESET);
              console.log(YELLOW + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);

              // Update progress periodically during wait
              // Progress reporting removed - was: const updateInterval = setInterval(() => job.updateProgress(30), 10000);
              await new Promise(resolve => setTimeout(resolve, waitTime + 2500));
              // Progress reporting removed - was: clearInterval(updateInterval);

              console.log(GREEN + `[Blur API Rate Limit] Resumed processing for wallet ${walletAddress.substring(0, 8)}...` + RESET);
            } else {
              // Pause has expired, clean it up
              await redis.del(pauseKey);
              console.log(CYAN + `[Blur API Rate Limit] Cleaned up expired pause for wallet ${walletAddress.substring(0, 8)}...` + RESET);
            }
          }
        }
      } catch (error) {
        console.warn(`Failed to check Redis for wallet status: ${error}`);
      }
    }

    // Update progress before processing job
    // Progress reporting removed - await job.updateProgress(40);

    // Process job based on type
    switch (job.name) {
      case BLUR_SCHEDULE:
        // Progress reporting removed - await job.updateProgress(45);
        await processBlurScheduledBid(job.data);
        // Progress reporting removed - await job.updateProgress(95);
        return { completed: true };
      case BLUR_COLLECTION_BID:
        // Progress reporting removed - await job.updateProgress(45);
        await processBlurCollectionBid(job.data);
        // Progress reporting removed - await job.updateProgress(95);
        return { completed: true };
      case BLUR_TRAIT_BID:
        // Progress reporting removed - await job.updateProgress(45);
        await processBlurTraitBid(job.data);
        // Progress reporting removed - await job.updateProgress(95);
        return { completed: true };
      case BLUR_COLLECTION_BID_COUNTERBID:
        // Progress reporting removed - await job.updateProgress(45);
        await blurCollectionCounterBid(job.data);
        // Progress reporting removed - await job.updateProgress(95);
        return { completed: true };
      case BLUR_TRAIT_BID_COUNTERBID:
        // Progress reporting removed - await job.updateProgress(45);
        await blurTraitCounterBid(job.data);
        // Progress reporting removed - await job.updateProgress(95);
        return { completed: true };
      default:
        throw new Error(`Unknown Blur job type: ${job.name}`);
    }
  } catch (error) {
    console.error(RED + `[Blur Job Error] ${job.name}: ${error}` + RESET);

    // Track completion even on error (for rate limiting purposes)
    if (walletAddress && walletAddress.startsWith('0x')) {
      blurCompletionTracker.trackCompletion(walletAddress);
    }

    throw error;
  } finally {
    // Track successful completion
    if (walletAddress && walletAddress.startsWith('0x')) {
      blurCompletionTracker.trackCompletion(walletAddress);
    }

    // Always update to 100% if we haven't already
    // Progress reporting removed - await job.updateProgress(100);
  }
}

async function processJob(job: Job, signal?: AbortSignal) {

  try {
    // Update progress to indicate job started
    // Progress reporting removed - await job.updateProgress(5);

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

    // Update progress after initial checks
    // Progress reporting removed - await job.updateProgress(10);

    const result = await (async () => {
      switch (job.name) {
        case OPENSEA_COLLECTION_BID:
          // Progress reporting removed - await job.updateProgress(15);
          await processOpenseaCollectionBid(job.data);
          // Progress reporting removed - await job.updateProgress(95);
          return { completed: true };
        case MAGICEDEN_COLLECTION_BID:
          // Progress reporting removed - await job.updateProgress(15);
          await processMagicedenCollectionBid(job.data);
          // Progress reporting removed - await job.updateProgress(95);
          return { completed: true };
        case OPENSEA_COLLECTION_BID_COUNTERBID:
          // Progress reporting removed - await job.updateProgress(15);
          await openseaCollectionCounterBid(job.data);
          // Progress reporting removed - await job.updateProgress(95);
          return { completed: true };
        case MAGICEDEN_COLLECTION_BID_COUNTERBID:
          // Progress reporting removed - await job.updateProgress(15);
          await magicedenCollectionCounterBid(job.data);
          // Progress reporting removed - await job.updateProgress(95);
          return { completed: true };
        case OPENSEA_TOKEN_BID_COUNTERBID:
          // Progress reporting removed - await job.updateProgress(15);
          await openseaTokenCounterBid(job.data);
          // Progress reporting removed - await job.updateProgress(95);
          return { completed: true };
        case OPENSEA_TRAIT_BID_COUNTERBID:
          // Progress reporting removed - await job.updateProgress(15);
          await openseaTraitCounterBid(job.data);
          // Progress reporting removed - await job.updateProgress(95);
          return { completed: true };
        case MAGICEDEN_TOKEN_BID_COUNTERBID:
          // Progress reporting removed - await job.updateProgress(15);
          await magicedenTokenCounterBid(job.data);
          // Progress reporting removed - await job.updateProgress(95);
          return { completed: true };
        case MAGICEDEN_TRAIT_BID_COUNTERBID:
          // Progress reporting removed - await job.updateProgress(15);
          await magicedenTraitCounterBid(job.data);
          // Progress reporting removed - await job.updateProgress(95);
          return { completed: true };
        case OPENSEA_SCHEDULE:
          // Progress reporting removed - await job.updateProgress(15);
          await processOpenseaScheduledBid(job.data, job.id as string, job);
          // Progress reporting removed - await job.updateProgress(95);
          return { completed: true };
        case OPENSEA_TRAIT_BID:
          // Progress reporting removed - await job.updateProgress(15);
          await processOpenseaTraitBid(job.data);
          // Progress reporting removed - await job.updateProgress(95);
          return { completed: true };
        case OPENSEA_TOKEN_BID:
          // Progress reporting removed - await job.updateProgress(15);
          await processOpenseaTokenBid(job.data);
          // Progress reporting removed - await job.updateProgress(95);
          return { completed: true };
        case MAGICEDEN_SCHEDULE:
          // Progress reporting removed - await job.updateProgress(15);
          await processMagicedenScheduledBid(job.data, job.id as string, job);
          // Progress reporting removed - await job.updateProgress(95);
          return { completed: true };
        case MAGICEDEN_TRAIT_BID:
          // Progress reporting removed - await job.updateProgress(15);
          await processMagicedenTraitBid(job.data);
          // Progress reporting removed - await job.updateProgress(95);
          return { completed: true };
        case MAGICEDEN_TOKEN_BID:
          // Progress reporting removed - await job.updateProgress(15);
          await processMagicedenTokenBid(job.data);
          // Progress reporting removed - await job.updateProgress(95);
          return { completed: true };
        case CANCEL_OPENSEA_BID:
          // Progress reporting removed - await job.updateProgress(15);
          try {
            await bulkCancelOpenseaBid(job.data);
          } catch (error) {
            console.error(RED + `[CANCEL_OPENSEA_BID] Failed to cancel order: ${error}` + RESET);
            // Track the failure as a completion to ensure we reach 100%
            const { taskId } = job.data;
            cancellationTracker.recordCompletion(taskId, 1, 'opensea');
            await updateTaskCancellationPercentage(taskId);
            throw error; // Re-throw to let the job fail properly
          }
          // Progress reporting removed - await job.updateProgress(95);
          return { completed: true };
        case CANCEL_MAGICEDEN_BID:
          // Progress reporting removed - await job.updateProgress(15);
          try {
            await bulkCancelMagicedenBid(job.data);
          } catch (error) {
            console.error(RED + `[CANCEL_MAGICEDEN_BID] Failed to cancel order: ${error}` + RESET);
            // Track the failure as a completion to ensure we reach 100%
            const { taskId } = job.data;
            cancellationTracker.recordCompletion(taskId, 1, 'magiceden');
            await updateTaskCancellationPercentage(taskId);
            throw error; // Re-throw to let the job fail properly
          }
          // Progress reporting removed - await job.updateProgress(95);
          return { completed: true };
        case CANCEL_BLUR_BID:
          // Progress reporting removed - await job.updateProgress(15);
          try {
            await bulkCancelBlurBid(job.data);
          } catch (error) {
            console.error(RED + `[CANCEL_BLUR_BID] Failed to cancel order: ${error}` + RESET);
            // Track the failure as a completion to ensure we reach 100%
            const { taskId } = job.data;
            cancellationTracker.recordCompletion(taskId, 1, 'blur');
            await updateTaskCancellationPercentage(taskId);
            throw error; // Re-throw to let the job fail properly
          }
          // Progress reporting removed - await job.updateProgress(95);
          return { completed: true };
        case PROCESS_LISTING:
          return await processListingJob(job);
        case PROCESS_LISTING_UPDATE:
          // Progress reporting removed - await job.updateProgress(15);
          const updateResult = await processListingUpdateJob(job);
          // Progress reporting removed - await job.updateProgress(95);
          return updateResult;
        default:
          return { completed: false, error: 'Unknown job type' };
      }
    })();

    // Final progress update
    // Progress reporting removed - await job.updateProgress(100);
    return result;
  } catch (error) {
    throw error;
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
        console.log(`Ensured listing ${listingId} has scheduled job for continuous listing`);
      } catch (scheduleError) {
        console.error(`Failed to ensure scheduled job for listing ${listingId}:`, scheduleError);
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
    console.error(`Failed to process listing update for ${listingId}:`, error);
    throw error;
  }
}

async function runScheduledLoop() {
  if (!API_KEY) {
    console.log(RED + 'API_KEY is not set - queue operations disabled' + RESET);
    console.log(YELLOW + 'Please set API_KEY environment variable and restart' + RESET);
    // Don't pause queue, just skip task creation
    return
  }

  if (!RATE_LIMIT) {
    console.log(RED + 'RATE_LIMIT is not set - queue operations disabled' + RESET);
    console.log(YELLOW + 'Please set RATE_LIMIT environment variable and restart' + RESET);
    // Don't pause queue, just skip task creation
    return
  }

  await Promise.all(
    Array.from(activeTasks.entries())
      .filter(([_, task]) => task.running)
      .map(([_, task]) => startTask(task, true))
  );
}

async function processImportedTasks(tasks: ITask[]) {
  try {
    console.log(BLUE + `\n===  Importing ${tasks.length} Tasks === ` + RESET);

    tasks.forEach(task => {
      console.log(`Collection: ${task.contract.slug}`);
      activeTasks.set(task._id, task);
      currentTasks.push(task);
    });
    updateTaskIndexes(currentTasks);

    // Start all running tasks concurrently
    const runningTasks = tasks.filter(task => task.running);
    if (runningTasks.length > 0) {
      console.log(CYAN + `Starting ${runningTasks.length} running tasks...` + RESET);
      await Promise.all(
        runningTasks.map(task => startTask(task, true))
      );
      console.log(GREEN + `Started ${runningTasks.length} tasks successfully` + RESET);
    }

    console.log(GREEN + `=== Imported Tasks Processing Complete ===\n` + RESET);
  } catch (error) {
    console.error(RED + `Error processing imported tasks` + RESET, error);
  }
}


async function processNewTask(task: ITask) {
  try {
    activeTasks.set(task._id, task)
    currentTasks.push(task);
    updateTaskIndexes(currentTasks);
    await startTask(task, task.running)
    console.log(GREEN + `=== New Task Processing Complete ===\n` + RESET);
  } catch (error) {
    console.error(RED + `Error processing new task: ${task.contract.slug}` + RESET, error);
  }
}

async function processGlobalMarketplaces(incomingMarketplaces: string[]) {
  try {
    const tasks: ITask[] = Array.from(activeTasks.values());
    const marketplacesToProcess = incomingMarketplaces.map((m) => m.toLowerCase());

    // Calculate what's changing
    const newMarketplaces = marketplacesToProcess.filter(m => !enabledMarketplaces.map((em) => em.toLowerCase()).includes(m));
    const removedMarketplaces = enabledMarketplaces
      .map(m => m.toLowerCase())
      .filter(m => !marketplacesToProcess.includes(m));


    // Update enabled marketplaces first
    enabledMarketplaces = marketplacesToProcess;

    // Early return if no marketplaces to process
    if (marketplacesToProcess.length === 0) {
      console.log(YELLOW + `[processGlobalMarketplaces] No marketplaces to process` + RESET);
      return;
    }

    // Only process if there are NEW marketplaces being added
    if (newMarketplaces.length === 0) {
      console.log(YELLOW + `[processGlobalMarketplaces] No new marketplaces to add. Skipping processing.` + RESET);
      if (removedMarketplaces.length > 0) {
        console.log(YELLOW + `[processGlobalMarketplaces] Removed marketplaces: ${removedMarketplaces.join(', ')}` + RESET);
      }
      return;
    }

    // Extract the marketplace to process: use the first new marketplace
    const marketplaceToProcess: string = newMarketplaces[0];

    console.log(GREEN + `[processGlobalMarketplaces] Processing new marketplace: ${marketplaceToProcess}` + RESET);

    // Filter tasks that have this marketplace selected and are running
    const tasksToProcess = tasks.filter(task =>
      task.running && task.selectedMarketplaces.map((m) => m.toLowerCase()).includes(marketplaceToProcess)
    );

    // Process each task for this specific marketplace only
    await Promise.all(tasksToProcess.map(async (task) => {
      try {
        await startTask(task, task.running, marketplaceToProcess);
      } catch (error) {
        console.error(RED + `Error processing marketplace ${marketplaceToProcess} for task ${task._id}: ` + RESET, error);
      }
    }));

    // Enabled marketplaces already updated at the beginning
  } catch (error) {
    console.error(RED + `Error processing global marketplaces: ` + RESET, error);
  }
}

async function cancelRemovedTokenBids(removedTokens: { opensea: string[]; magiceden: string[] }, taskId: string, walletPrivateKey: string, currencySymbol: string): Promise<number> {
  let totalJobsCreated = 0;
  try {
    const openseaTokensToCancel = await Order.find({
      taskId: taskId,
      identifier: { $in: removedTokens.opensea },
      marketplace: "opensea"
    })

    const magicedenTokensToCancel = await Order.find({
      taskId: taskId,
      identifier: { $in: removedTokens.magiceden },
      marketplace: "magiceden"
    })

    if (openseaTokensToCancel.length > 0) {
      const cancelData = openseaTokensToCancel.map((order) => {
        return {
          name: CANCEL_OPENSEA_BID,
          data: { privateKey: walletPrivateKey, orderId: order.orderHash, taskId, orderKey: order._id }
        };
      });
      await queue.addBulk(cancelData);
      totalJobsCreated += cancelData.length;

      // const openseaJobId = `${taskId}-opensea`.toLowerCase()
      // const openseaJob = parentJobs.get(openseaJobId)

      // if (openseaJob) {
      //   openseaJob.totalJobs -= openseaTokensToCancel.length
      //   openseaJob.completedJobs -= openseaTokensToCancel.length
      // }
    }

    for (let i = 0; i < magicedenTokensToCancel.length; i += 500) {
      const batchOrders = magicedenTokensToCancel.slice(i, i + 500);
      const extractedOrderIds = await extractMagicedenOrderHash(batchOrders)
      const orderKeys = batchOrders.map((order: any) => order._id)
      await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey: walletPrivateKey, orderKeys, taskId: taskId }, getCancellationJobOptions(taskId, orderKeys[0], CANCEL_MAGICEDEN_BID));
      totalJobsCreated++;

      // const magicedenJobId = `${taskId}-magiceden`.toLowerCase()
      // const magicedenJob = parentJobs.get(magicedenJobId)

      // if (magicedenJob) {
      //   magicedenJob.totalJobs -= batchOrders.length
      //   magicedenJob.completedJobs -= batchOrders.length
      // }
    }
  } catch (error) {
    console.error(RED + `Error canceling removed token bids: ` + RESET, error);
  }
  return totalJobsCreated;
}

async function cancelRemovedTraitBids(traitsToRemove: string[], taskId: string, walletPrivateKey: string, currencySymbol: string): Promise<number> {
  let totalJobsCreated = 0;
  try {
    const ordersToCancel = await Order.find({
      taskId: taskId,
      identifier: { $in: traitsToRemove }
    })

    console.log(RED + '--------------------------------------------------------------' + RESET);
    console.log(RED + `OLD TRAIT BIDS TO CANCEL: ${ordersToCancel.length}`.toUpperCase() + RESET);
    console.log(RED + '--------------------------------------------------------------' + RESET);

    const blurOrders = ordersToCancel.filter(order => order.marketplace.toLowerCase() === BLUR.toLowerCase());
    const openseaOrders = ordersToCancel.filter(order => order.marketplace.toLowerCase() === OPENSEA.toLowerCase());
    const magicedenOrders = ordersToCancel.filter(order => order.marketplace.toLowerCase() === MAGICEDEN.toLowerCase());

    if (blurOrders.length > 0) {
      const cancelData = blurOrders.map((order) => {
        const payload = JSON.parse(order.orderHash);
        return {
          name: CANCEL_BLUR_BID,
          data: { privateKey: walletPrivateKey, payload, taskId, orderKey: order._id }
        };
      });
      await queue.addBulk(cancelData);
      totalJobsCreated += cancelData.length;
    }

    if (openseaOrders.length > 0) {
      const cancelData = openseaOrders.map((order) => {
        return {
          name: CANCEL_OPENSEA_BID,
          data: { privateKey: walletPrivateKey, orderId: order.orderHash, taskId, orderKey: order._id }
        };
      });
      await queue.addBulk(cancelData);
      totalJobsCreated += cancelData.length;

      // const openseaJobId = `${taskId}-opensea`.toLowerCase()
      // const openseaJob = parentJobs.get(openseaJobId)

      // if (openseaJob) {
      //   openseaJob.totalJobs -= openseaOrders.length
      //   openseaJob.completedJobs -= openseaOrders.length
      // }
    }

    for (let i = 0; i < magicedenOrders.length; i += 500) {
      const batchOrders = magicedenOrders.slice(i, i + 500);
      const extractedOrderIds = await extractMagicedenOrderHash(batchOrders)
      const orderKeys = batchOrders.map((order: any) => order._id)
      await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey: walletPrivateKey, orderKeys, taskId }, getCancellationJobOptions(taskId, orderKeys[0], CANCEL_MAGICEDEN_BID));
      totalJobsCreated++;

      // const magicedenJobId = `${taskId}-magiceden`.toLowerCase()
      // const magicedenJob = parentJobs.get(magicedenJobId)

      // if (magicedenJob) {
      //   magicedenJob.totalJobs -= batchOrders.length
      //   magicedenJob.completedJobs -= batchOrders.length
      // }
    }
  } catch (error) {
    console.error(RED + `Error canceling removed trait bids: ` + RESET, error);
  }
  return totalJobsCreated;
}


async function updateTaskData(task: ITask) {
  try {
    const existingTaskIndex = currentTasks.findIndex(t => t._id === task._id);
    const existingTask = getActiveTaskWithIdByIndex(existingTaskIndex)?.task
    warningBids[task._id] = {
      opensea: false,
      magiceden: false,
      blur: false
    };

    if (existingTaskIndex === -1 || !existingTask) return

    const wallet = await walletCache.getWalletById(task.walletId)
    const WALLET_PRIVATE_KEY = decryptPrivateKey(wallet?.encryptedPrivateKey as string) as string;
    currentTasks.splice(existingTaskIndex, 1, task);
    activeTasks.set(task._id, task);
    updateTaskIndexes(currentTasks);

    console.log(YELLOW + `Updated existing task: ${task.contract.slug}`.toUpperCase() + RESET);

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

    currentTraits[task._id].opensea = openseaSelectedTraits
    currentTraits[task._id].magiceden = magicedenSelectedTraits
    currentTraits[task._id].blur = blurSelectedTraits

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
    const tokenBid = task.bidType === "token"
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

    if (task.bidType === "token" && selectedTraits && Object.keys(selectedTraits).length > 0) {
      const attributes = transformSelectedTraitsToAttributes(selectedTraits)
      const expandedAttributes = expandAttributes(attributes)
      // Initialize tokens with bottomListings and taskTokenIds
      let openseaTokens: string[] = [...openseaBottonListings, ...taskTokenIds]
      let magicedenTokens: number[] = [...magicedenBottomListings, ...taskTokenIds.map((token) => +token)]

      if (currentTask.traitsOperator.value === "or") {
        await pQueue.addAll(
          expandedAttributes.map((attribute) => async () => {
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

            const items = await fetchOpenSeaCollectionItems(variable);
            openseaTokens.push(...items);
            magicedenTokens.push(...items.map(id => Number(id)))
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

    } else if (task.bidType === "token") {
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

    const { currencySymbol } = await calculateBidPrice(currentTask, 0, "magiceden")

    traitsToRemove[task._id] = removedTraits.map((trait) => `${trait.type}:${trait.value}`)
    await cancelRemovedTraitBids(traitsToRemove[task._id], task._id, WALLET_PRIVATE_KEY, currencySymbol)
    await cancelRemovedTokenBids(removedTokens[task._id], task._id, WALLET_PRIVATE_KEY, currencySymbol)

    console.log(GREEN + '--------------------------------------------------------------' + RESET);
    console.log(GREEN + `NEW TRAIT BIDS TO ADD: ${addedTraits.length}`.toUpperCase() + RESET);
    console.log(GREEN + '--------------------------------------------------------------' + RESET);

    const newTask = { ...task, traits: {} } as ITask
    // Check cache first
    let collectionDetails = await collectionCache.getCollectionDetails(task.contract.slug);

    if (!collectionDetails) {
      // Cache miss - fetch from API with deduplication
      collectionDetails = await collectionCache.fetchWithDedup(
        `collection-details-${task.contract.slug}`,
        async () => {
          const details = await getCollectionDetails(task.contract.slug, task.contract.contractAddress);
          await collectionCache.setCollectionDetails(task.contract.slug, details);
          return details;
        }
      );
    }

    const creatorFees: IFee = collectionDetails.creator_fees.null !== undefined
      ? { null: collectionDetails.creator_fees.null }
      : Object.fromEntries(Object.entries(collectionDetails.creator_fees).map(([key, value]) => [key, Number(value)]));

    const bidDuration = currentTask && currentTask.bidDuration ? currentTask.bidDuration : task.bidDuration
    const expiration = getExpiry(bidDuration)

    if (traitBid) {
      const openseaTraitJobs = (currentTask && currentTask.running && currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("opensea") && enabledMarketplaces.map((m) => m.toLowerCase()).includes('opensea')) ? addedTraits.map((trait) => ({
        name: OPENSEA_TRAIT_BID,
        data: {
          _id: task._id,
          address: task.wallet,
          privateKey: WALLET_PRIVATE_KEY,
          slug: task.contract.slug,
          contractAddress: task.contract.contractAddress,
          creatorFees,
          enforceCreatorFee: collectionDetails.enforceCreatorFee,
          trait: JSON.stringify(trait),
          outbidOptions: task.outbidOptions,
          task: newTask
        },
        opts: { priority: TRAIT_BID_PRIORITY.OPENSEA } as JobsOptions
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
            privateKey: WALLET_PRIVATE_KEY,
            slug: task.contract.slug,
            trait,
            outbidOptions: task.outbidOptions,
            task: newTask,
          },
          opts: { priority: TRAIT_BID_PRIORITY.MAGICEDEN } as JobsOptions
        })) as Job[] : [] as Job[]


      const blurTraits = addedTraits.map((trait) => ({
        [trait.type]: trait.value
      }));


      const blurTraitJobs = (currentTask && currentTask.running && currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("blur") && enabledMarketplaces.map((m) => m.toLowerCase()).includes('blur')) ? blurTraits.map((trait) => ({
        name: BLUR_TRAIT_BID,
        data: {
          _id: task._id,
          address: task.wallet,
          privateKey: WALLET_PRIVATE_KEY,
          contractAddress: task.contract.contractAddress,
          slug: task.contract.slug,
          trait: JSON.stringify(trait),
          expiry: expiration,
          outbidOptions: task.outbidOptions,
          task: newTask
        },
        opts: { priority: TRAIT_BID_PRIORITY.BLUR } as JobsOptions
      })) as Job[] : [] as Job[]


      await Promise.all([
        processBulkJobs(openseaTraitJobs, true),
        processBulkJobs(magicedenTraitJobs, true),
        processBulkJobs(blurTraitJobs, true)
      ])
    } else if (tokenBid) {
      const openseaTokenJob = (currentTask && currentTask.running && currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("opensea") && enabledMarketplaces.map((m) => m.toLowerCase()).includes('opensea')) ? openseaTokensToAdd
        .filter(token => !isNaN(Number(token)) && String(token).toLowerCase().indexOf('bot') === -1)
        .map((token) => ({
          name: OPENSEA_TOKEN_BID,
          data: {
            _id: task._id,
            address: task.wallet,
            privateKey: WALLET_PRIVATE_KEY,
            slug: task.contract.slug,
            creatorFees,
            enforceCreatorFee: collectionDetails.enforceCreatorFee,
            asset: { contractAddress: task.contract.contractAddress, tokenId: token },
            outbidOptions: task.outbidOptions,
            task: newTask
          },
          opts: { priority: TOKEN_BID_PRIORITY.OPENSEA } as JobsOptions,
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
              privateKey: WALLET_PRIVATE_KEY,
              slug: task.contract.slug,
              tokenId: token,
              outbidOptions: task.outbidOptions,
              task: newTask,
            },
            opts: { priority: TOKEN_BID_PRIORITY.MAGICEDEN } as JobsOptions
          })) as Job[] : [] as Job[]


      if (!currentTokenIds[task._id]) {
        currentTokenIds[task._id] = {
          opensea: [],
          magiceden: []
        }
      }

      currentTokenIds[task._id].opensea = openseaTokenIds
      currentTokenIds[task._id].magiceden = magicedenTokenIds

      await Promise.all([
        processBulkJobs(magicedenTokenJob, true),
        processBulkJobs(openseaTokenJob, true)
      ]);
    }

    // existingTask

    const start = existingTask.running === false && task.running === true
    const stop = existingTask.running === true && task.running === false

    if (start) {
      await startTask(task, true)
    }

    if (stop) {
      await stopTask(task, false)
    }
  } catch (error) {
    console.error(RED + `Error processing updated task: ${task.contract.slug}` + RESET, error);
  }
}

const DRAIN_THRESHOLD = 9000; // Queue drain threshold

const BLUR_JOBS = [
  'BLUR_COLLECTION_BID',
  'BLUR_COLLECTION_BID_COUNTERBID',
  'BLUR_TRAIT_BID_COUNTERBID',
  'BLUR_TRAIT_BID'
];

// Helper function to extract marketplace from job name
function getMarketplaceFromJobName(jobName: string): string | null {
  const name = jobName.toUpperCase();
  if (name.includes('OPENSEA')) return 'opensea';
  if (name.includes('MAGICEDEN')) return 'magiceden';
  if (name.includes('BLUR')) return 'blur';
  return null;
}

async function waitForQueueDrain(): Promise<void> {
  while (true) {
    const counts = await queue.getJobCounts();
    const prioritizedCount = counts.prioritized || 0;

    if (prioritizedCount <= DRAIN_THRESHOLD) {
      break;
    }

    const delay = prioritizedCount / DRAIN_THRESHOLD;
    await new Promise(resolve => setTimeout(resolve, delay * 1000));
  }
}

const MAX_QUEUE_JOBS = 10000; // Maximum jobs allowed in queue
// Helper function to get total jobs in all queues
async function getTotalJobsInQueues(): Promise<number> {
  const [counts, blurCounts] = await Promise.all([
    queue.getJobCounts(),
    blurQueue.getJobCounts()
  ]);

  return (counts.waiting || 0) + (counts.active || 0) + (counts.prioritized || 0) +
    (blurCounts.waiting || 0) + (blurCounts.active || 0) + (blurCounts.prioritized || 0);
}


async function processBulkJobs(jobs: Job[], createKey = false, scheduleJob?: Job): Promise<void> {
  if (!jobs?.length) {
    return;
  }

  const firstJob = jobs[0];
  const taskId = firstJob.data._id || firstJob.data.taskId;
  const task = activeTasks.get(taskId);
  let processedCount = 0;
  let actuallyQueuedCount = 0;

  // Detect marketplace from job name
  const jobName = firstJob.name.toLowerCase();
  let marketplace = '';
  if (jobName.includes('opensea')) marketplace = 'opensea';
  else if (jobName.includes('magiceden')) marketplace = 'magiceden';
  else if (jobName.includes('blur')) marketplace = 'blur';

  // Ensure parent job exists and wait for it to be ready (unless it's a cancellation job)
  const isCancellationJob = jobName.includes('cancel');
  if (marketplace && !isCancellationJob) {
    try {
      await ensureParentJobExistsEnhanced(taskId, marketplace);
      const isReady = await waitForParentJobReady(taskId, marketplace);

      if (!isReady) {
        throw new Error(`Parent job not ready for ${taskId}/${marketplace} after timeout`);
      }
    } catch (error) {
      console.error(RED + `[processBulkJobs] Parent job sync failed: ${error}` + RESET);
      throw error;
    }
  }

  // Progress tracking for schedule job
  let lastProgressUpdate = Date.now();
  const progressUpdateInterval = 10000; // Update every 10 seconds

  // Create a simple rate limiter for queueing jobs at RATE_LIMIT per second
  const rateLimiter = new RateLimiter(500);

  try {
    // Check Redis health before attempting bulk operations
    const isRedisHealthy = await checkRedisClusterHealth();
    if (!isRedisHealthy) {
      const error = new Error('Redis cluster is not healthy - cannot queue jobs');
      throw error;
    }

    // For cancellation jobs, we should process them even if the task is not running
    const isCancellationJob = firstJob.name.toLowerCase().includes('cancel');

    if (!task) {
      return;
    }

    if (!task.running && !isCancellationJob) {
      return;
    }

    // Set task status to QUEUEING at the start of bulk job processing
    // Don't override CANCELLING status for cancellation jobs
    if (taskStatus[taskId] && taskStatus[taskId].status !== "RUNNING" && !isCancellationJob) {
      setTaskStatus(taskId, "QUEUEING", "jobs being queued");
    }

    // Check current queue state but don't wait for initial drain
    const counts = await queue.getJobCounts();
    const blurCounts = await blurQueue.getJobCounts();
    const prioritizedCount = (counts.prioritized || 0) + (blurCounts.prioritized || 0);

    // Log current state but proceed immediately
    console.log(CYAN + `[processBulkJobs] Current prioritized count: ${prioritizedCount}, target range: [${2 * RATE_LIMIT}, ${DRAIN_THRESHOLD}]` + RESET);

    const CHUNK_SIZE = 500
    const chunks = chunk(jobs, CHUNK_SIZE);
    let retryChunkIndex = -1; // Track if we need to retry a chunk

    // Track total jobs actually added for each marketplace
    const jobsAddedByMarketplace = new Map<string, number>();

    // Setup scheduled bid lock extension if we have a large batch
    let scheduledBidLockInterval: NodeJS.Timeout | undefined;
    if (jobs.length > 1000 && firstJob.name) {
      const marketplace = getMarketplaceFromJobName(firstJob.name)?.toLowerCase() || '';
      if (marketplace) {
        try {
          // Acquire the scheduled bid lock first
          acquireScheduledBidLock(taskId, marketplace);
          console.log(GREEN + `[processBulkJobs] Acquired scheduled bid lock for ${taskId}-${marketplace}` + RESET);

          // Set up interval to extend the lock
          scheduledBidLockInterval = setInterval(() => {
            try {
              const extended = extendScheduledBidLock(taskId, marketplace);
              if (!extended) {
                console.warn(YELLOW + `[processBulkJobs] Failed to extend scheduled bid lock for ${taskId}-${marketplace}` + RESET);
              }
            } catch (error) {
              console.error(RED + `[processBulkJobs] Error extending scheduled bid lock: ${error}` + RESET);
            }
          }, 60000); // Check every minute
        } catch (lockError) {
          console.error(RED + `[processBulkJobs] Failed to acquire scheduled bid lock: ${lockError}` + RESET);
          // Don't set up the interval if we couldn't acquire the lock
        }
      }
    }

    try {
      for (let chunkIndex = 0; chunkIndex < chunks.length; chunkIndex++) {
        // Handle retry logic
        if (retryChunkIndex >= 0) {
          chunkIndex = retryChunkIndex;
          retryChunkIndex = -1;
        }

        const currentChunk = chunks[chunkIndex];

        try {
          // Process chunk

          // Check capacity and prioritized count
          const counts = await queue.getJobCounts();
          const blurCounts = await blurQueue.getJobCounts();

          let totalJobs = (counts.waiting || 0) + (counts.active || 0) + (counts.prioritized || 0) +
            (blurCounts.waiting || 0) + (blurCounts.active || 0) + (blurCounts.prioritized || 0);
          let prioritizedJobs = (counts.prioritized || 0) + (blurCounts.prioritized || 0);

          // Check if we need to wait for capacity BEFORE processing any jobs
          // Implement recursive waiting - never skip jobs due to capacity
          const WAIT_INTERVAL = (prioritizedJobs / RATE_LIMIT) * 1000; // 1 minute between checks
          const startWaitTime = Date.now();
          let waitIterations = 0;

          while (totalJobs + currentChunk.length > MAX_QUEUE_JOBS || prioritizedJobs >= DRAIN_THRESHOLD) {
            waitIterations++;
            const elapsedTime = Date.now() - startWaitTime;


            console.log(YELLOW + `[processBulkJobs] Task ${taskId}: Queue at capacity (${totalJobs}/${MAX_QUEUE_JOBS}, prioritized: ${prioritizedJobs}). ` +
              `Waiting ${WAIT_INTERVAL / 1000} seconds before retry (iteration ${waitIterations}, elapsed: ${Math.round(elapsedTime / 1000)}s)...` + RESET);

            // Wait for 1 minute
            await new Promise(resolve => setTimeout(resolve, WAIT_INTERVAL));

            // Re-check queue counts
            const newCounts = await queue.getJobCounts();
            const newBlurCounts = await blurQueue.getJobCounts();

            totalJobs = (newCounts.waiting || 0) + (newCounts.active || 0) + (newCounts.prioritized || 0) +
              (newBlurCounts.waiting || 0) + (newBlurCounts.active || 0) + (newBlurCounts.prioritized || 0);
            prioritizedJobs = (newCounts.prioritized || 0) + (newBlurCounts.prioritized || 0);

            // Log progress
            console.log(CYAN + `[processBulkJobs] Re-checked queue: totalJobs=${totalJobs}, prioritized=${prioritizedJobs}` + RESET);
          }

          if (waitIterations > 0) {
            console.log(GREEN + `[processBulkJobs] Queue capacity available after ${waitIterations} iterations (${Math.round((Date.now() - startWaitTime) / 1000)}s)` + RESET);
          }

          // If we reach here, we have capacity to process the full chunk

          // Process the full chunk since we verified we have capacity
          const jobsToProcess = currentChunk;

          const validJobs = jobsToProcess.filter(job => {
            if (!job?.name || !job?.data) return false;

            // For cancellation jobs, taskId is in data.taskId, for others it's in data._id
            const jobTaskId = job.data._id || job.data.taskId;
            if (!jobTaskId) return false;

            const jobTask = activeTasks.get(jobTaskId);
            const isCancelJob = job.name.toLowerCase().includes('cancel');

            // Allow cancellation jobs even if task is not running
            if (isCancelJob) {
              return !!jobTask; // Just check if task exists
            }

            return jobTask && jobTask.running;
          });

          if (!validJobs.length) {
            console.log(YELLOW + `[processBulkJobs] Task ${taskId}: No valid jobs in chunk` + RESET);
            // No valid jobs, but we successfully processed the chunk
          } else {
            // Separate Blur and non-Blur jobs
            const blurJobs = validJobs.filter(job => BLUR_JOBS.includes(job.name));
            const nonBlurJobs = validJobs.filter(job => !BLUR_JOBS.includes(job.name));

            // Add non-Blur jobs to queue
            if (nonBlurJobs.length > 0) {
              const jobsToAdd = nonBlurJobs.map(job => {
                const isCancelJob = job.name.toLowerCase().includes('cancel');
                const jobKey = isCancelJob ? undefined :
                  (createKey ? createJobKey(job) : generateJobKey(job));

                const priority = isCancelJob ? 0 : loopCounterStats[taskId] + 1

                return {
                  name: job.name,
                  data: job.data,
                  opts: {
                    ...(jobKey ? { jobId: jobKey } : {}),
                    ...job.opts,
                    removeOnComplete: true,
                    removeOnFail: isCancelJob ? false : true,  // Keep failed cancellation jobs for retry
                    priority
                  }
                };
              });

              // Skip duplicate check - use jobId to prevent duplicates
              const uniqueJobs = jobsToAdd;

              // Add jobs with rate limiting using chunked addBulk
              const rateLimitChunks = chunk(uniqueJobs, RATE_LIMIT);

              // Track cancellation jobs specifically
              const cancellationJobCount = uniqueJobs.filter(job => job.name.toLowerCase().includes('cancel')).length;
              if (cancellationJobCount > 0) {
                console.log(GOLD + `[processBulkJobs] Adding ${cancellationJobCount} cancellation jobs to queue for task ${taskId}` + RESET);
              }

              for (const jobChunk of rateLimitChunks) {
                await rateLimiter.acquire();
                const addedJobs = await queue.addBulk(jobChunk);

                // Log successful addition of cancellation jobs
                const addedCancellationCount = addedJobs.filter(job => job.name.toLowerCase().includes('cancel')).length;
                if (addedCancellationCount > 0) {
                  console.log(GREEN + `[processBulkJobs] Successfully queued ${addedCancellationCount} cancellation jobs` + RESET);
                }

                // Log current prioritized count to track target range
                if (Math.random() < 0.1) { // Log 10% of the time to avoid spam
                  const qCounts = await queue.getJobCounts();
                  const bCounts = await blurQueue.getJobCounts();
                  const currentPri = (qCounts.prioritized || 0) + (bCounts.prioritized || 0);
                  console.log(CYAN + `[processBulkJobs] Current prioritized: ${currentPri}, target: [${2 * RATE_LIMIT}, ${DRAIN_THRESHOLD}]` + RESET);
                }
              }

              // Track actual jobs added for each marketplace (use unique jobs count)
              for (const job of uniqueJobs) {
                const marketplace = getMarketplaceFromJobName(job.name);
                if (marketplace) {
                  jobsAddedByMarketplace.set(marketplace, (jobsAddedByMarketplace.get(marketplace) || 0) + 1);
                }
              }

              // Update progress (use unique jobs count)
              processedCount += uniqueJobs.length;
              actuallyQueuedCount += uniqueJobs.length;

              // Update actualJobs incrementally for each marketplace
              for (const [marketplace, count] of jobsAddedByMarketplace) {
                try {
                  const parentJobId = getParentJobId(taskId, marketplace);
                  const parentJob = parentJobs.get(parentJobId);
                  if (parentJob && count > 0) {
                    parentJob.actualJobs = (parentJob.actualJobs || 0) + count;
                    // Clear the count after updating
                    jobsAddedByMarketplace.set(marketplace, 0);
                  }
                } catch (error) {
                  console.error(RED + `[processBulkJobs] Error updating parent job for ${marketplace}: ${error}` + RESET);
                }
              }

              // Update schedule job progress if needed
              if (scheduleJob && Date.now() - lastProgressUpdate > progressUpdateInterval) {
                const percentComplete = Math.round((processedCount / jobs.length) * 100);
                try {
                  await scheduleJob.updateProgress(percentComplete);
                  lastProgressUpdate = Date.now();
                } catch (error) {
                  // Silent fail
                }
              }
            }

            // Add Blur jobs using the coordinator
            if (blurJobs.length > 0) {
              const blurJobsToAdd = blurJobs.map(job => {
                const isCancelJob = job.name.toLowerCase().includes('cancel');
                const jobKey = isCancelJob ? undefined :
                  (createKey ? createJobKey(job) : generateJobKey(job));
                const priority = isCancelJob ? 0 : loopCounterStats[taskId] + 1
                return {
                  name: job.name,
                  data: job.data,
                  opts: {
                    ...(jobKey ? { jobId: jobKey } : {}),
                    ...job.opts,
                    priority,
                    removeOnComplete: true,
                    removeOnFail: isCancelJob ? false : true  // Keep failed cancellation jobs for retry
                  }
                };
              });

              // Skip duplicate check for Blur jobs - use jobId to prevent duplicates
              const uniqueBlurJobs = blurJobsToAdd;

              // Add Blur jobs with rate limiting using chunked addBulk
              const blurRateLimitChunks = chunk(uniqueBlurJobs, RATE_LIMIT);
              for (const jobChunk of blurRateLimitChunks) {
                await rateLimiter.acquire();
                await blurQueue.addBulk(jobChunk);
              }

              // Track actual Blur jobs added (use unique jobs count)
              jobsAddedByMarketplace.set('blur', (jobsAddedByMarketplace.get('blur') || 0) + uniqueBlurJobs.length);

              // Update progress (use unique jobs count)
              processedCount += uniqueBlurJobs.length;
              actuallyQueuedCount += uniqueBlurJobs.length;

              // Update actualJobs incrementally for Blur
              try {
                const blurParentJobId = getParentJobId(taskId, 'blur');
                const blurParentJob = parentJobs.get(blurParentJobId);
                if (blurParentJob && uniqueBlurJobs.length > 0) {
                  blurParentJob.actualJobs = (blurParentJob.actualJobs || 0) + uniqueBlurJobs.length;
                  // Clear the count after updating
                  jobsAddedByMarketplace.set('blur', 0);
                }
              } catch (error) {
                console.error(RED + `[processBulkJobs] Error updating Blur parent job: ${error}` + RESET);
              }

              // Update schedule job progress if needed
              if (scheduleJob && Date.now() - lastProgressUpdate > progressUpdateInterval) {
                const percentComplete = Math.round((processedCount / jobs.length) * 100);
                try {
                  await scheduleJob.updateProgress(percentComplete);
                  lastProgressUpdate = Date.now();
                } catch (error) {
                  // Silent fail
                }
              }
            }

            // We always process the full chunk now
          }

          // No lock acquisition needed anymore
          // Since we wait before processing, added is always true now
        } catch (error) {
          // Check if it's a timeout error
          if (error instanceof Error && error.message?.includes('Command timed out')) {
            // For timeout errors, check queue state before retrying
            const counts = await queue.getJobCounts();
            const blurCounts = await blurQueue.getJobCounts();
            const currentPrioritized = (counts.prioritized || 0) + (blurCounts.prioritized || 0);

            if (currentPrioritized < DRAIN_THRESHOLD) {
              // Within target range, retry immediately
              console.log(YELLOW + `[processBulkJobs] Timeout error but within range (${currentPrioritized}), retrying chunk immediately` + RESET);
              retryChunkIndex = chunkIndex;
              chunkIndex--; // Decrement to retry the same chunk
            } else {
              console.log(RED + `[processBulkJobs] Timeout error and queue full, waiting before retry` + RESET);
              // Calculate wait time based on queue processing rate
              const waitTimeSeconds = Math.ceil(currentPrioritized / RATE_LIMIT);
              const waitTimeMs = waitTimeSeconds * 1000;
              await new Promise(resolve => setTimeout(resolve, waitTimeMs));
              retryChunkIndex = chunkIndex;
              chunkIndex--; // Decrement to retry the same chunk
            }
          }
          // Continue with next chunk on any error
        }
      }

      // After all chunks processed, finalize parent jobs that haven't been marked complete
      for (const [marketplace, actualCount] of jobsAddedByMarketplace) {
        const parentJobId = getParentJobId(taskId, marketplace);
        const parentJob = parentJobs.get(parentJobId);
        if (parentJob && !parentJob.allJobsQueued) {
          // Only update if not already marked as complete (by retry limit logic)
          // Update actual jobs count (should already be updated incrementally)
          parentJob.actualJobs = (parentJob.actualJobs || 0) + actualCount;

          // Always update totalJobs to match actualJobs now that queueing is complete
          const previousTotal = parentJob.totalJobs;
          parentJob.totalJobs = parentJob.actualJobs;

          // Only mark as allJobsQueued if we've met or exceeded expectedJobs
          if (parentJob.expectedJobs !== undefined && parentJob.actualJobs < parentJob.expectedJobs) {
            console.log(YELLOW + `[processBulkJobs] ${parentJobId}: Only queued ${parentJob.actualJobs}/${parentJob.expectedJobs} jobs - NOT marking as complete` + RESET);
            console.log(YELLOW + `  ⚠️  PARTIAL QUEUEING DETECTED - Missing ${parentJob.expectedJobs - parentJob.actualJobs} jobs` + RESET);
            console.log(YELLOW + `  This will trigger automatic task restart to queue missing jobs` + RESET);
            // Don't set allJobsQueued=true - this will allow the task to be restarted
          } else {
            parentJob.allJobsQueued = true;
            console.log(GREEN + `[processBulkJobs] Finalized ${parentJobId}: totalJobs=${parentJob.totalJobs} (was ${previousTotal})` + RESET);

            // Log if actualJobs exceeds expectedJobs (likely due to retries)
            if (parentJob.expectedJobs !== undefined && parentJob.actualJobs > parentJob.expectedJobs) {
              console.log(YELLOW + `  Note: actualJobs=${parentJob.actualJobs} exceeds expectedJobs=${parentJob.expectedJobs} (likely due to retries)` + RESET);
            }
          }
        }
      }

      // Ensure all marketplaces are marked as queued complete
      const allMarketplaces = new Set<string>();
      for (const job of jobs) {
        const marketplace = getMarketplaceFromJobName(job.name);
        if (marketplace) allMarketplaces.add(marketplace);
      }

      for (const marketplace of allMarketplaces) {
        const parentJobId = getParentJobId(taskId, marketplace);
        const parentJob = parentJobs.get(parentJobId);
        if (parentJob && !parentJob.allJobsQueued) {
          // This marketplace didn't get updated above, meaning no jobs were queued
          parentJob.actualJobs = parentJob.actualJobs || 0;
          parentJob.totalJobs = parentJob.actualJobs;

          // Only mark as queued if we've actually queued the expected number of jobs
          if (parentJob.expectedJobs !== undefined && parentJob.actualJobs < parentJob.expectedJobs) {
            console.log(YELLOW + `[processBulkJobs] WARNING: ${parentJobId} has only ${parentJob.actualJobs}/${parentJob.expectedJobs} jobs queued` + RESET);
            // Don't mark as allJobsQueued - this will trigger a restart
          } else {
            parentJob.allJobsQueued = true;
            console.log(YELLOW + `[processBulkJobs] Marked ${parentJobId} as complete with ${parentJob.actualJobs} jobs` + RESET);
          }
        }
      }

      // Verify all jobs were queued successfully
      if (actuallyQueuedCount < jobs.length) {
        const errorMsg = `Failed to queue all jobs: ${actuallyQueuedCount}/${jobs.length} for task ${taskId}`;
        console.error(RED + `[processBulkJobs] ${errorMsg}` + RESET);

        // Log the failure details
        await logger.logQueueError('Failed to queue all jobs', {
          taskId: taskId,
          marketplace: getMarketplaceFromJobName(firstJob.name) || 'unknown',
          requested: jobs.length,
          queued: actuallyQueuedCount,
          failed: jobs.length - actuallyQueuedCount
        });

        // Since we have recursive waiting, this should never happen unless there's a real error
        throw new Error(errorMsg);
      }

      // Log summary with special attention to cancellation jobs
      const totalCancellationJobs = jobs.filter(job => job.name.toLowerCase().includes('cancel')).length;
      if (totalCancellationJobs > 0) {
        console.log(GREEN + `[processBulkJobs] Successfully queued all ${actuallyQueuedCount} jobs for task ${taskId} (including ${totalCancellationJobs} cancellation jobs)` + RESET);
      } else {
        console.log(GREEN + `[processBulkJobs] Successfully queued all ${actuallyQueuedCount} jobs for task ${taskId}` + RESET);
      }

      // Trigger percentage update now that all jobs are queued
      await updateTaskRunningPercentage(taskId);

      // Final progress update to 100%
      if (scheduleJob) {
        try {
          await scheduleJob.updateProgress(100);
        } catch (error) {
          // Silent fail
        }
      }

    } catch (error) {
      // Log to file for debugging
      await logger.logGeneralError(
        `Failed to process bulk jobs for task ${taskId}`,
        {
          taskId,
          jobCount: jobs.length,
          firstJobName: jobs[0]?.name,
          isRedisError: error instanceof Error && error.message.includes('CLUSTERDOWN'),
          error: error instanceof Error ? error : new Error(String(error))
        }
      );

      // Reset task status to allow retry
      if (taskStatus[taskId]) {
        setTaskStatus(taskId, 'PROCESSING', 'job queueing failed - will retry');
      }

      // Re-throw error to trigger job retry in scheduled bid processor
      throw new Error(`Job queueing failed for task ${taskId}: ${error instanceof Error ? error.message : String(error)}`);
    } finally {
      // Clear scheduled bid lock extension interval if set
      if (scheduledBidLockInterval) {
        clearInterval(scheduledBidLockInterval);
      }

      // Release the scheduled bid lock if it was acquired
      if (jobs.length > 1000 && firstJob.name) {
        const marketplace = getMarketplaceFromJobName(firstJob.name)?.toLowerCase() || '';
        if (marketplace) {
          releaseScheduledBidLock(taskId, marketplace);
          console.log(CYAN + `[processBulkJobs] Released scheduled bid lock for ${taskId}-${marketplace}` + RESET);
        }
      }
    }
  } catch (error) {
    // Outer catch for the main try block
    console.error(RED + `[processBulkJobs] Fatal error: ${error}` + RESET);
    throw error;
  }
}



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
    if (duplicatesFound > 0 || process.env.DEBUG_JOB_KEYS === 'true') {
      console.log(GOLD + `[checkForDuplicateJobs] Checked ${jobsToAdd.length} jobs, found ${duplicatesFound} duplicates` + RESET);
    }

    return nonDuplicateJobs;
  } catch (error) {
    // If checking fails, log the error but don't block job addition
    console.error('[checkForDuplicateJobs] Error checking for duplicates:', error);
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
  if (process.env.DEBUG_JOB_KEYS === 'true') {
    console.log(BLUE + `[createJobKey] Creating key for job type: ${job.name}` + RESET);
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
      if (process.env.DEBUG_JOB_KEYS === 'true') {
        console.log(YELLOW + `[createJobKey] Warning: Using default case for job type: ${job.name}` + RESET);
      }
  }
  const baseKey = `${job?.data?._id}`

  const rawSlug = job?.data?.contract?.slug || job?.data?.slug || ''
  const slug = removeChar(rawSlug, '-')
  const walletAddress = job?.data?.address || ''
  const key = `bid-${baseKey}-${uniqueKey}-${slug}-${walletAddress}`

  // Enhanced debug logging for key generation
  if (process.env.DEBUG_JOB_KEYS === 'true') {
    console.log(BLUE + `[createJobKey] Generated key components:` + RESET);
    console.log(BLUE + `  - Job Type: ${job.name}` + RESET);
    console.log(BLUE + `  - Unique Key: ${uniqueKey}` + RESET);
    console.log(BLUE + `  - Slug: ${slug}` + RESET);
    console.log(BLUE + `  - Wallet: ${walletAddress.substring(0, 10)}...` + RESET);
    console.log(BLUE + `  - Final Key: ${key}` + RESET);
  }

  return key
}

function generateJobKey(job: any): string {
  const taskId = job?.data?._id || '';
  const contractSlug = job?.data?.contract?.slug || job?.data?.slug || '';
  const name = job?.name || '';

  const key = `${taskId}-${contractSlug}-${name}`;

  // Log generateJobKey usage
  if (process.env.DEBUG_JOB_KEYS === 'true') {
    console.log(CYAN + `[generateJobKey] Simple key generated: ${key}` + RESET);
  }

  return key;
}

function removeChar(str: string, char: string): string {
  return str.split(char).join('');
}

// Function to trigger task restart when stuck at 0% due to queue capacity issues
async function triggerTaskRestart(taskId: string) {
  const task = activeTasks.get(taskId);
  if (!task) {
    console.log(YELLOW + `[triggerTaskRestart] Task ${taskId} not found in activeTasks` + RESET);
    return;
  }

  // Check restart limit to prevent infinite loops
  const now = Date.now();
  const restartInfo = taskRestartCounts.get(taskId);

  if (restartInfo) {
    // Reset counter if it's been more than an hour since last restart
    if (now - restartInfo.lastRestart > 3600000) { // 1 hour
      taskRestartCounts.set(taskId, { count: 1, lastRestart: now });
    } else if (restartInfo.count >= MAX_RESTARTS_PER_HOUR) {
      console.error(RED + `[triggerTaskRestart] Task ${taskId} has reached maximum restarts (${MAX_RESTARTS_PER_HOUR}) in the last hour` + RESET);
      console.error(RED + `  Last restart was ${Math.round((now - restartInfo.lastRestart) / 60000)} minutes ago` + RESET);

      // Log this as an error
      await logger.logGeneralError('Task exceeded restart limit', {
        taskId,
        restartCount: restartInfo.count,
        maxRestarts: MAX_RESTARTS_PER_HOUR,
        timeSinceLastRestart: now - restartInfo.lastRestart
      });

      return; // Don't restart
    } else {
      // Increment counter
      taskRestartCounts.set(taskId, { count: restartInfo.count + 1, lastRestart: now });
    }
  } else {
    // First restart
    taskRestartCounts.set(taskId, { count: 1, lastRestart: now });
  }

  console.log(RED + `[triggerTaskRestart] Restarting stuck task ${taskId} due to queue capacity issue` + RESET);
  console.log(YELLOW + `  Restart count: ${taskRestartCounts.get(taskId)?.count || 1}/${MAX_RESTARTS_PER_HOUR}` + RESET);

  // Clear any stuck parent jobs for all marketplaces
  const marketplaces = ['opensea', 'magiceden', 'blur'];
  for (const marketplace of marketplaces) {
    const parentJobId = getParentJobId(taskId, marketplace);
    if (parentJobs.has(parentJobId)) {
      console.log(YELLOW + `[triggerTaskRestart] Clearing stuck parent job: ${parentJobId}` + RESET);
      parentJobs.delete(parentJobId);
    }
  }

  // Clear processed jobs tracking
  marketplaces.forEach(marketplace => {
    const parentJobId = getParentJobId(taskId, marketplace);
    processedJobsPerParent.delete(parentJobId);
  });

  // Reset task status to SCHEDULED to allow restart
  setTaskStatus(taskId, 'SCHEDULED', 'restarting stuck task');

  // Clear any preprocessing data
  taskPreprocessing.delete(taskId);

  console.log(GREEN + `[triggerTaskRestart] Triggering restart for task ${taskId}` + RESET);

  try {
    // Restart the task with isRestart=true to prevent loop counter increment
    await startTask(task, true, undefined, true);
    console.log(GREEN + `[triggerTaskRestart] Successfully restarted task ${taskId}` + RESET);
  } catch (error) {
    console.error(RED + `[triggerTaskRestart] Failed to restart task ${taskId}:` + RESET, error);
    // Log the error but don't throw - we want to attempt recovery
    await logger.logGeneralError('Failed to restart stuck task', {
      taskId,
      error: error instanceof Error ? error : new Error(String(error))
    });
  }
}

// Wrapper function with retry logic for Redis timeouts
async function startTask(task: ITask, start: boolean, marketplace?: string, isRestart?: boolean) {
  const MAX_RETRIES = 3;
  const RETRY_DELAYS = [2000, 4000, 8000]; // 2s, 4s, 8s

  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    try {
      const result = await startTaskInternal(task, start, marketplace, isRestart);
      return result; // Pass through the result
    } catch (error) {
      // Check if it's a timeout error and we have retries left
      if (error instanceof Error &&
        error.message?.includes('Command timed out') &&
        attempt < MAX_RETRIES - 1) {
        console.log(YELLOW + `[startTask] Redis timeout for task ${task._id}, retry ${attempt + 1}/${MAX_RETRIES} after ${RETRY_DELAYS[attempt]}ms` + RESET);
        await new Promise(resolve => setTimeout(resolve, RETRY_DELAYS[attempt]));
        continue; // Retry
      }

      // If not a timeout or no retries left, re-throw the error
      throw error;
    }
  }

  // If all retries failed with timeout
  return { success: false, reason: 'timeout_exhausted' };
}

// Original startTask function implementation
async function startTaskInternal(task: ITask, start: boolean, marketplace?: string, isRestart?: boolean) {
  const startTime = Date.now();
  const taskId = task._id.toString();

  console.log(GREEN + `[startTask] Called for task ${taskId}, start=${start}, marketplace=${marketplace}, isRestart=${isRestart}` + RESET);

  if (!start) {
    console.log(YELLOW + `[startTask] Not starting task ${taskId} - start=false` + RESET);
    return { success: false, reason: 'start_false' };
  }


  // If task is in CANCELLING state, we'll clean it up and allow the start
  const currentStatus = taskStatus[taskId]?.status;
  if (currentStatus === 'CANCELLING') {
    console.log(YELLOW + `[startTask] Task ${taskId} is in CANCELLING state, cleaning up and proceeding with start` + RESET);

    // Remove task from blacklist to allow new jobs
    const wasRemoved = removeTaskFromBlacklist(taskId);
    if (wasRemoved) {
      console.log(GREEN + `[startTask] Removed task ${taskId} from blacklist` + RESET);
    }

    // Reset cancellation percentage
    if (taskStatus[taskId]) {
      taskStatus[taskId].cancellationPercentage = 0;
      delete taskStatus[taskId].cancellationStartTime;
      delete taskStatus[taskId].previousState;
      delete taskStatus[taskId].cancellingMarketplace;
    }

    // Clear any cancellation tracking
    cancellationTracker.clearTracking(taskId);
  }

  clearTaskTimers(taskId)

  if (!marketplace) {
    skipStats[task._id] = {
      opensea: 0,
      magiceden: 0,
      blur: 0
    };
    // Reset error stats when starting task without marketplace parameter
    errorStats[taskId] = { opensea: 0, magiceden: 0, blur: 0 };

    // Check if we're transitioning from SCHEDULED status
    const previousStatus = taskStatus[taskId]?.status || 'NONE';
    taskStatus[taskId] = {
      status: 'STARTED',
      runningPercentage: 0,
      cancellationPercentage: 0,
      processingPercentage: 0,
      totalJobs: 0,
      completedJobs: 0,
      waitingTime: 0
    };
    console.log(MAGENTA + `[startTask] Task ${taskId} status: ${previousStatus} -> STARTED` + RESET);

    // Set up status progression monitoring
    taskStatusManager.ensureStatusProgression(taskId, 'STARTED', 'RUNNING', 15000); // 15 seconds timeout

    // Simple loop counter: initialize to 0, then increment each time startTask is called (except for restarts)
    const previousLoopCount = loopCounterStats[taskId] || 0;
    let newLoopCount = previousLoopCount;

    if (!isRestart) {
      // Only increment counter for normal task starts, not restarts
      if (!loopCounterStats[taskId]) {
        loopCounterStats[taskId] = 0;
      }
      loopCounterStats[taskId]++;
      newLoopCount = loopCounterStats[taskId];
      console.log(CYAN + `[startTask] Starting new loop #${newLoopCount} for task ${taskId} (previous: #${previousLoopCount})` + RESET);

      // Clean up old parent jobs when starting a new loop
      cleanupOldParentJobs(taskId, newLoopCount);
    } else {
      // For restarts, keep the same loop count
      newLoopCount = loopCounterStats[taskId] || 1;
      console.log(YELLOW + `[startTask] Restarting loop #${newLoopCount} for task ${taskId} (not incrementing counter)` + RESET);
    }

    // Clear all processed jobs for this task to ensure fresh tracking
    for (const [parentJobId, processedJobs] of processedJobsPerParent.entries()) {
      if (parentJobId.includes(taskId)) {
        console.log(YELLOW + `[startTask] Clearing processed jobs for parent: ${parentJobId} (${processedJobs.size} entries)` + RESET);
        processedJobsPerParent.delete(parentJobId);
      }
    }

    // Clear ALL parent jobs for this task from previous loops
    console.log(YELLOW + `[startTask] Clearing parent jobs from previous loops for task ${taskId}` + RESET);
    let clearedParentJobs = 0;
    for (const [parentJobId, parentJob] of parentJobs.entries()) {
      if (parentJobId.includes(taskId)) {
        // Delete all parent jobs for this task, regardless of loop number
        // They will be recreated fresh for the new loop
        console.log(YELLOW + `  - Clearing parent job ${parentJobId} (loop #${parentJob.loopNumber})` + RESET);
        parentJobs.delete(parentJobId);
        clearedParentJobs++;
      }
    }
    if (clearedParentJobs > 0) {
      console.log(GREEN + `[startTask] Cleared ${clearedParentJobs} parent jobs from previous loops` + RESET);
    }

    // Clear pending job creation promises for this task
    console.log(YELLOW + `[startTask] Clearing pending job creation promises for task ${taskId}` + RESET);
    let clearedPromises = 0;
    for (const [promiseKey] of pendingJobCreation.entries()) {
      if (promiseKey.includes(taskId)) {
        console.log(YELLOW + `  - Clearing pending promise: ${promiseKey}` + RESET);
        pendingJobCreation.delete(promiseKey);
        clearedPromises++;
      }
    }
    if (clearedPromises > 0) {
      console.log(GREEN + `[startTask] Cleared ${clearedPromises} pending job creation promises` + RESET);
    }

    // Clear any task-specific state in warningBids and warningMessage
    if (warningBids[taskId]) {
      console.log(YELLOW + `[startTask] Clearing warning bids for task ${taskId}` + RESET);
      delete warningBids[taskId];
    }
    if (warningMessage[taskId]) {
      console.log(YELLOW + `[startTask] Clearing warning messages for task ${taskId}` + RESET);
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
      console.log(YELLOW + `[startTask] Clearing processed jobs for marketplace-specific parent: ${parentJobId} (${processedJobsCount} entries)` + RESET);
      processedJobsPerParent.delete(parentJobId);
    }
  }

  await removeTaskFromBlacklist(taskId);

  if (!API_KEY) {
    console.log(RED + 'API_KEY is not set - cannot start task' + RESET);
    console.log(YELLOW + 'Please set API_KEY environment variable and restart' + RESET);
    return;
  }

  // Async bid log deletion - fire and forget
  BidLogs.deleteMany({ taskId: task._id }).catch(() => { });

  // Ensure wallet approvals for all selected marketplaces
  try {
    const wallet = await walletCache.getWalletById(task.walletId);
    if (!wallet?.encryptedPrivateKey) {
      console.log(RED + `Cannot start task - wallet not found or missing private key: ${task.walletId}` + RESET);
      return;
    }

    const privateKey = decryptPrivateKey(wallet.encryptedPrivateKey);
    const validMarketplaces = ["opensea", "magiceden", "blur"];
    const selectedMarketplaces = marketplace ? [marketplace.toLowerCase()] : task.selectedMarketplaces.map(m => m.toLowerCase());
    const marketplacesToApprove = selectedMarketplaces.filter(m => validMarketplaces.includes(m));

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
          // console.log(YELLOW + `Skipping approval for Blur - not required` + RESET);
          return { marketplace: mp, approved: true, cached: false };
        default:
          console.log(YELLOW + `Unknown marketplace: ${mp}, skipping approval` + RESET);
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
      }

      return { marketplace: mp, approved, cached: false };
    });

    // Wait for all approvals to complete
    const approvalResults = await Promise.all(approvalPromises);
    const failedApprovals = approvalResults.filter(r => !r.approved);

    if (failedApprovals.length > 0) {
      console.log(YELLOW + `Warning: ${failedApprovals.length} marketplace(s) failed approval` + RESET);
    }
  } catch (error) {
    console.error(RED + `Error during marketplace approval check: ${error}` + RESET);
    // Continue with task execution even if approval check fails
  }

  const RATE_LIMIT = process.env.RATE_LIMIT;
  if (!RATE_LIMIT) {
    console.log(RED + 'RATE_LIMIT is not set - cannot start task' + RESET);
    console.log(YELLOW + 'Please set RATE_LIMIT environment variable and restart' + RESET);
    return;
  }

  // taskId already declared at function start

  try {
    await removeTaskFromAborted(taskId);

    // Only update task running status if not already running
    if (!task.running) {
      const updatedTask = { ...task, running: true };
      activeTasks.set(taskId, updatedTask);
      const taskIndex = currentTasks.findIndex(t => t._id === task._id);
      if (taskIndex !== -1) {
        currentTasks[taskIndex].running = true;
      }

      // Add task to indexes for fast lookup
      addTaskToIndexes(updatedTask);

      // Subscribe to WebSocket collection (WebSocket should already be connected at startup)
      try {
        if (wsManager?.isConnected()) {
          // console.log("subscribing to collection: ", task.contract.slug);
          const newTask = { ...task, running: true };
          subscribeToCollections([newTask]);
        }
      } catch (error) {
        console.error(RED + `Error subscribing to collection ${task.contract.slug}: ` + RESET, error);
      }
    }

    const validMarketplaces = ["opensea", "magiceden", "blur"];
    const selectedMarketplaces = marketplace ? [marketplace.toLowerCase()] : task.selectedMarketplaces.map(m => m.toLowerCase());

    // Pre-filter marketplaces based on circuit breaker state
    const availableMarketplaces = selectedMarketplaces
      .filter((m) => enabledMarketplaces.map((m) => m.toLowerCase()).includes(m))
      .filter(m => validMarketplaces.includes(m))
      .filter(m => {
        // Check circuit breaker before creating jobs
        if (taskCircuitBreaker.isSuspended(taskId, m)) {
          return false;
        }
        return true;
      });

    // Clear parentJobs for this task at the start of each loop
    // This ensures we start fresh and don't have stale data from previous loops
    if (availableMarketplaces.length > 0) {
      const marketplacesToClear = marketplace ? [marketplace.toLowerCase()] : ['opensea', 'magiceden', 'blur'];
      for (const mp of marketplacesToClear) {
        // Clear parent job for current loop
        const parentId = getParentJobId(taskId, mp);
        if (parentJobs.has(parentId)) {
          console.log(CYAN + `[startTask] Clearing parent job ${parentId} at loop start` + RESET);
          parentJobs.delete(parentId);
        }
      }

      // Clear old jobs from queue to prevent state bleed
      console.log(YELLOW + `[startTask] Clearing old jobs for task ${taskId} from queue` + RESET);
    }

    // Calculate and store task-wide totals after parent job initialization
    if (marketplace && task.running) {
      // When adding a marketplace to running task, just increment totals
      const parentJobId = `${taskId}-${marketplace}`;
      const parentJob = parentJobs.get(parentJobId);
      if (parentJob && taskStatus[taskId]) {

        taskStatus[taskId].totalJobs += parentJob.totalJobs;
        const newPercentage = (taskStatus[taskId].completedJobs / taskStatus[taskId].totalJobs) * 100;
        taskStatus[taskId].runningPercentage = Math.min(100, Math.round(newPercentage * 100) / 100);
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

      // Store the totals in taskStatus
      if (taskStatus[taskId]) {
        taskStatus[taskId].totalJobs = taskTotalJobs;
        taskStatus[taskId].completedJobs = taskCompletedJobs;
      }
    }

    // Only create jobs for the specified marketplace if provided, otherwise create for all available
    const marketplacesForJobs = marketplace ? [marketplace.toLowerCase()].filter(m => availableMarketplaces.includes(m)) : availableMarketplaces;


    const jobs = marketplacesForJobs
      .map(m => {
        switch (m) {
          case "opensea":
            return {
              name: OPENSEA_SCHEDULE,
              data: { ...task, running: start },
              opts: {
                jobId: `${task._id}-opensea-schedule`,
                removeOnComplete: true,
                removeOnFail: true,
                attempts: 100,
                timeout: 900000, // 15 minutes instead of default 5
                backoff: {
                  type: 'exponential',
                  delay: 2000,
                },
              }
            };
          case "blur":
            return {
              name: BLUR_SCHEDULE,
              data: { ...task, running: start },
              opts: {
                jobId: `${task._id}-blur-schedule`,
                removeOnComplete: true,
                removeOnFail: true,
                attempts: 100,
                timeout: 900000, // 15 minutes instead of default 5
                backoff: {
                  type: 'exponential',
                  delay: 2000,
                },
              }
            };
          case "magiceden":
            return {
              name: MAGICEDEN_SCHEDULE,
              data: { ...task, running: start },
              opts: {
                jobId: `${task._id}-magiceden-schedule`,
                removeOnComplete: true,
                removeOnFail: true,
                attempts: 100,
                timeout: 900000, // 15 minutes instead of default 5
                priority: 1, // Higher priority than default jobs
                backoff: {
                  type: 'exponential',
                  delay: 2000,
                },
              }
            };
          default:
            return null;
        }
      })
      .filter(job => job !== null);

    // Add deduplication check before adding jobs (skip deduplication for loop restarts)
    if (jobs.length > 0) {

      // Separate Blur and non-Blur jobs
      const blurJobs = jobs.filter(job => job.name === BLUR_SCHEDULE);
      const nonBlurJobs = jobs.filter(job => job.name !== BLUR_SCHEDULE);


      // For loop restarts (start=true), always add jobs without deduplication
      if (start === true) {
        if (nonBlurJobs.length > 0) {
          const addedJobs = await queue.addBulk(nonBlurJobs);
        }
        if (blurJobs.length > 0) {
          const addedBlurJobs = await blurQueue.addBulk(blurJobs);
        }
      } else {
        // Optimized deduplication - only check for this specific task's jobs
        const existingJobNames = new Set<string>();

        // Check each job type individually to avoid loading all jobs
        for (const jobToAdd of jobs) {
          const jobType = jobToAdd.name;
          const targetJobId = jobToAdd.opts?.jobId || `${taskId}-${jobType}`;

          // Try to get the specific job by ID first
          try {
            const targetQueue = jobType === BLUR_SCHEDULE ? blurQueue : queue;
            const existingJob = await targetQueue.getJob(targetJobId);
            if (existingJob) {
              const jobState = await existingJob.getState();
              if (['waiting', 'active', 'delayed'].includes(jobState)) {
                existingJobNames.add(jobType);
              }
            }
          } catch (err) {
            console.log(RED + `[startTask] Error checking job ${targetJobId}: ${err}` + RESET);
          }
        }

        // Filter out duplicate jobs
        const newJobs = jobs.filter(job => !existingJobNames.has(job.name));
        const newBlurJobs = newJobs.filter(job => job.name === BLUR_SCHEDULE);
        const newNonBlurJobs = newJobs.filter(job => job.name !== BLUR_SCHEDULE);

        if (newJobs.length > 0) {

          if (newNonBlurJobs.length > 0) {
            const addedNonBlur = await queue.addBulk(newNonBlurJobs);
          }
          if (newBlurJobs.length > 0) {
            const addedBlur = await blurQueue.addBulk(newBlurJobs);
          }
          // Check queue status
          const jobCounts = await queue.getJobCounts();
        }
      }
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
    console.error(RED + `Error starting task ${taskId}: ` + RESET, error);
    return { success: false, reason: 'error', error };
  } finally {
    // Always release lock when done
    unlockTask(taskId);

    // Log performance metrics
    const duration = Date.now() - startTime;
    // startTask completed

    // Log warning if startup took too long
    if (duration > 5000) {
      console.log(YELLOW + `[Performance] Task startup took ${(duration / 1000).toFixed(1)}s - consider optimization` + RESET);
    }
  }

  return { success: true };
}

// Enhanced BullMQ job cancellation for active jobs
// Enhanced job removal from all queue states using correct job ID format
// Remove all jobs for a specific task from the queue
// NOTE: This function should ONLY be called from stopTask
// No other part of the codebase should remove jobs directly
async function removeAllTaskJobsFromQueue(taskId: string, marketplace?: string) {
  const marketplaces = marketplace ? [marketplace.toLowerCase()] : ['opensea', 'blur', 'magiceden'];
  let totalRemoved = 0;
  const removalResults = [];

  for (const mp of marketplaces) {
    const jobIdPrefix = `$bid-${taskId}-${mp}-`;
    const marketplaceResult = {
      marketplace: mp,
      waiting: 0,
      active: 0,
      delayed: 0,
      completed: 0,
      failed: 0,
      total: 0
    };

    try {
      // Remove from all job states
      const jobStates = ['waiting', 'active', 'delayed', 'completed', 'failed'] as const;

      for (const state of jobStates) {
        const jobs = await queue.getJobs([state], 0, -1);
        const taskJobs = jobs.filter(job => job && job.id && job.id.startsWith(jobIdPrefix));

        for (const job of taskJobs) {
          try {
            if (state === 'active') {
              // Use moveToFailed for active jobs to respect BullMQ patterns
              await job.moveToFailed(new Error(`Task ${taskId} stopped`), job.token, false);
            } else {
              // Remove non-active jobs
              await job.remove();
            }
            (marketplaceResult as any)[state]++;
            totalRemoved++;
            console.log(`Removed ${state} job: ${job.id}`);
          } catch (error) {
            console.error(`Failed to remove job ${job.id} from ${state}:`, error);
          }
        }
      }

      marketplaceResult.total = Object.keys(marketplaceResult)
        .filter(key => key !== 'marketplace' && key !== 'total')
        .reduce((sum, key) => sum + (marketplaceResult as any)[key], 0);

    } catch (error: any) {
      console.error(`Error removing ${mp} jobs for task ${taskId}:`, error);
      (marketplaceResult as any).error = error?.message || String(error);
    }

    removalResults.push(marketplaceResult);
  }

  console.log(`Total jobs removed for task ${taskId}: ${totalRemoved}`);

  // Job state verification - wait and re-check for any missed jobs
  if (totalRemoved > 0) {
    console.log(YELLOW + `[removeAllTaskJobsFromQueue] Verifying job removal for task ${taskId}...` + RESET);

    // Wait a short time for jobs that might be transitioning between states
    await new Promise(resolve => setTimeout(resolve, 500));

    // Re-check for any remaining jobs
    let remainingJobs = 0;
    for (const mp of marketplaces) {
      const jobIdPrefix = `$bid-${taskId}-${mp}-`;
      const jobStates = ['waiting', 'active', 'delayed'] as const;

      for (const state of jobStates) {
        const jobs = await queue.getJobs([state], 0, -1);
        const taskJobs = jobs.filter(job => job && job.id && job.id.startsWith(jobIdPrefix));

        if (taskJobs.length > 0) {
          console.log(YELLOW + `[removeAllTaskJobsFromQueue] Found ${taskJobs.length} remaining ${state} jobs for ${mp}` + RESET);
          remainingJobs += taskJobs.length;

          // Try to remove them again
          for (const job of taskJobs) {
            try {
              if (state === 'active') {
                await job.moveToFailed(new Error(`Task ${taskId} stopped - second pass`), job.token, false);
              } else {
                await job.remove();
              }
              totalRemoved++;
              console.log(GREEN + `[removeAllTaskJobsFromQueue] Removed missed job: ${job.id}` + RESET);
            } catch (error) {
              console.error(RED + `[removeAllTaskJobsFromQueue] Failed to remove missed job ${job.id}:` + RESET, error);
            }
          }
        }
      }
    }

    if (remainingJobs > 0) {
      console.log(YELLOW + `[removeAllTaskJobsFromQueue] Removed ${remainingJobs} additional jobs in verification pass` + RESET);
    } else {
      console.log(GREEN + `[removeAllTaskJobsFromQueue] Verification complete - no remaining jobs found` + RESET);
    }
  }

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
      console.log(`Would cancel Blur rate limiter requests for task ${taskId}`);
    }

    // Add other marketplace rate limiter cancellations as needed
    // OpenSea and MagicEden don't currently have specific rate limiters

  } catch (error) {
    console.error(`Error cancelling rate limiter jobs for task ${taskId}:`, error);
  }
}

// Centralized function to update task cancellation percentage - single source of truth
export async function updateTaskCancellationPercentage(taskId: string, forcePercentage?: number) {
  if (!taskStatus[taskId]) {
    console.log(YELLOW + `[updateTaskCancellationPercentage] Task status not found for ${taskId}` + RESET);
    return;
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
        console.log(CYAN + `[updateTaskCancellationPercentage] Task ${taskId} - Using aggregated percentage: ${percentage}%` + RESET);
      } else {
        console.log(YELLOW + `[updateTaskCancellationPercentage] No aggregated tracking data for ${taskId}` + RESET);
        return;
      }
    } else {
      // Use regular percentage for marketplace-specific cancellations
      const trackerPercentage = cancellationTracker.getPercentage(taskId);
      if (trackerPercentage !== null) {
        percentage = trackerPercentage;
        console.log(CYAN + `[updateTaskCancellationPercentage] Task ${taskId} - Using regular percentage: ${percentage}%` + RESET);
      } else {
        console.log(YELLOW + `[updateTaskCancellationPercentage] No tracking data for ${taskId}` + RESET);
        return;
      }
    }
  }

  // Update the task status - this is the ONLY place where cancellationPercentage should be set
  taskStatus[taskId].cancellationPercentage = percentage;

  // Log the update
  console.log(CYAN + `[updateTaskCancellationPercentage] Task ${taskId} cancellation: ${percentage}%` + RESET);

  // Verify the value was actually set
  console.log(CYAN + `[updateTaskCancellationPercentage] Verification - taskStatus[${taskId}].cancellationPercentage = ${taskStatus[taskId].cancellationPercentage}` + RESET);

  // Log entire taskStatus object for this task
  console.log(CYAN + `[updateTaskCancellationPercentage] Full taskStatus[${taskId}]:`, JSON.stringify(taskStatus[taskId], null, 2) + RESET);

  // If we've reached 100%, check for any remaining active orders
  if (percentage >= 100 && taskStatus[taskId].status === 'CANCELLING') {
    console.log(YELLOW + `[updateTaskCancellationPercentage] Task ${taskId} reached 100%, checking for remaining active orders...` + RESET);

    try {
      // Determine if this is a marketplace-specific cancellation
      const cancellingMarketplace = taskStatus[taskId].cancellingMarketplace;

      // Build query based on cancellation type
      const query: any = {
        taskId: taskId,
        expiresAt: { $gt: new Date() }
      };

      if (cancellingMarketplace) {
        query.marketplace = cancellingMarketplace.toLowerCase();
      }

      const activeOrderCount = await Order.countDocuments(query);

      if (activeOrderCount > 0) {
        console.log(RED + `[updateTaskCancellationPercentage] WARNING: Task ${taskId} at 100% but found ${activeOrderCount} active orders!` + RESET);
        console.log(RED + `[updateTaskCancellationPercentage] Query used: ${JSON.stringify(query)}` + RESET);
        console.log(RED + `[updateTaskCancellationPercentage] Manual intervention or retry may be required` + RESET);

        // Update remaining orders for tracking
        taskStatus[taskId].remainingOrders = activeOrderCount;
        taskStatus[taskId].lastCancellationCheck = Date.now();

        console.log(RED + `[updateTaskCancellationPercentage] Task ${taskId} has remaining orders` + RESET);
        console.log(RED + `[updateTaskCancellationPercentage] Details:` + RESET);
        console.log(RED + `  - Remaining orders: ${activeOrderCount}` + RESET);
        console.log(RED + `  - Cancelling marketplace: ${cancellingMarketplace || 'ALL'}` + RESET);
        console.log(RED + `  - Will retry after 10 seconds` + RESET);
      } else {
        console.log(GREEN + `[updateTaskCancellationPercentage] Task ${taskId} at 100% with no remaining active orders` + RESET);
        // Transition to STOPPED since cancellation is complete
        await setTaskCancellationComplete(taskId);
      }
    } catch (error) {
      console.error(`[updateTaskCancellationPercentage] Error checking for remaining orders:`, error);
    }
  }

  // Force an immediate broadcast to ensure UI gets the update
  broadcastBidRates();
}

// Enhanced progress tracking for cancellation (now uses centralized function)
function updateCancellationProgress(taskId: string, completed: number, total: number) {
  // This function is deprecated - cancellation progress is now tracked by the cancellationTracker
  // Log progress for debugging only, don't update the actual percentage
  const percentage = total > 0 ? Math.min(100, Math.round((completed / total) * 100)) : 100;
  console.log(`[${taskId}] Cancellation steps progress: ${completed}/${total} (${percentage}%)`);
}

// Helper function to fetch orders for cancellation
async function fetchOrdersForCancellation(taskId: string, marketplace?: string): Promise<{
  opensea: any[];
  magiceden: any[];
  blur: any[];
}> {
  try {
    const now = new Date();
    const baseConditions = {
      taskId: taskId,
      expiresAt: { $gt: now } // Only fetch non-expired orders
    };

    if (marketplace) {
      // Fetch orders only for the specified marketplace
      const marketplaceLower = marketplace.toLowerCase();
      const orders = await Order.find({
        ...baseConditions,
        marketplace: marketplaceLower
      }).lean().exec();

      console.log(`[fetchOrdersForCancellation] Fetched ${orders.length} ${marketplaceLower} orders for task ${taskId}`);

      return {
        opensea: marketplaceLower === 'opensea' ? orders : [],
        magiceden: marketplaceLower === 'magiceden' ? orders : [],
        blur: marketplaceLower === 'blur' ? orders : []
      };
    } else {
      // Fetch orders for all marketplaces - ensure we get ALL orders without cursor limits
      const [openseaOrders, magicedenOrders, blurOrders] = await Promise.all([
        Order.find({
          ...baseConditions,
          marketplace: 'opensea'
        }).lean().exec(),
        Order.find({
          ...baseConditions,
          marketplace: 'magiceden'
        }).lean().exec(),
        Order.find({
          ...baseConditions,
          marketplace: 'blur'
        }).lean().exec()
      ]);

      console.log(`[fetchOrdersForCancellation] Fetched orders for task ${taskId} - OpenSea: ${openseaOrders.length}, MagicEden: ${magicedenOrders.length}, Blur: ${blurOrders.length}`);

      return {
        opensea: openseaOrders,
        magiceden: magicedenOrders,
        blur: blurOrders
      };
    }
  } catch (error) {
    console.error(`[fetchOrdersForCancellation] Error fetching orders for task ${taskId}:`, error);
    return {
      opensea: [],
      magiceden: [],
      blur: []
    };
  }
}

async function stopTask(task: ITask, start: boolean, marketplace?: string) {
  const taskId = task._id.toString();


  try {

    let WALLET_PRIVATE_KEY: string | undefined;

    try {
      const wallet = await walletCache.getWalletById(task.walletId);
      if (wallet?.encryptedPrivateKey) {
        WALLET_PRIVATE_KEY = decryptPrivateKey(wallet.encryptedPrivateKey);
      }
    } catch (decryptError) {
      console.error(`[StopTask] Failed to decrypt wallet for task ${taskId}:`, decryptError);
      // Continue with stopTask even if wallet decryption fails
    }
    // === ENHANCED STOPTASK: IMMEDIATE JOB CANCELLATION ===
    let totalOperations = 0;
    let completedOperations = 0;
    let completionHandled = false; // Track if completion has been handled


    // Initialize or update task status to CANCELLING
    if (!taskStatus[taskId]) {
      taskStatus[taskId] = {
        status: 'CANCELLING',
        runningPercentage: 0,
        cancellationPercentage: 0,
        processingPercentage: 0,
        totalJobs: 0,
        completedJobs: 0,
        cancellationStartTime: Date.now()
      };
      console.log(CYAN + `[stopTask] Created new taskStatus for ${taskId} with CANCELLING status` + RESET);
    } else {
      const previousStatus = taskStatus[taskId].status;
      taskStatus[taskId].status = 'CANCELLING';
      taskStatus[taskId].cancellationStartTime = Date.now();
      console.log(CYAN + `[stopTask] Updated taskStatus for ${taskId} from ${previousStatus} to CANCELLING` + RESET);

      // Log warning if task was already STOPPED
      if (previousStatus === 'STOPPED') {
        console.log(RED + `[stopTask] WARNING: Task ${taskId} was already in STOPPED status but stopTask was called again!` + RESET);
        console.log(RED + `[stopTask] Current cancellationPercentage: ${taskStatus[taskId].cancellationPercentage}%` + RESET);
      }
    }

    if (marketplace) {
      const currentStatus = taskStatus[taskId];
      if (currentStatus) {
        currentStatus.previousState = currentStatus.status !== 'CANCELLING' ? currentStatus.status : currentStatus.previousState;
        currentStatus.cancellingMarketplace = marketplace.toLowerCase();
      }
    } else {
      // Full task stop: clear scheduling fields
      delete loopCounterStats[task._id];
      if (taskStatus[taskId]) {
        taskStatus[taskId].nextLoopTime = undefined;
        taskStatus[taskId].waitingTime = 0;
        taskStatus[taskId].cancellingMarketplace = undefined;
        taskStatus[taskId].previousState = undefined;
      }
    }
    // Step 1: Blacklist task to prevent new jobs (immediate)
    if (marketplace) {
      // Add task to marketplace-specific blacklist
      isTaskBlacklisted(taskId, marketplace);
    } else {
      // Add task to complete blacklist
      isTaskBlacklisted(taskId);
      console.log(`Task ${taskId} fully blacklisted`);
    }
    totalOperations += 1;
    completedOperations += 1;
    updateCancellationProgress(taskId, completedOperations, totalOperations + 3); // +3 for remaining major steps

    // Step 2: Cancel active jobs and remove from queue (most important)
    const queueResult = await removeAllTaskJobsFromQueue(taskId, marketplace);
    totalOperations += Math.max(1, queueResult.removed);
    completedOperations += Math.max(1, queueResult.removed);
    updateCancellationProgress(taskId, completedOperations, totalOperations + 2); // +2 for remaining steps

    // Step 3: Cancel marketplace rate limiter requests
    await cancelMarketplaceRateLimiterJobs(taskId, marketplace);
    completedOperations += 1;
    updateCancellationProgress(taskId, completedOperations, totalOperations + 1); // +1 for remaining step

    // === EXISTING STOPTASK LOGIC CONTINUES ===
    if (!marketplace || task.selectedMarketplaces.length === 0) {
      markTaskAsAborted(taskId);
      const updatedTask = { ...task, running: false };
      activeTasks.set(taskId, updatedTask);

      // Remove task from indexes
      removeTaskFromIndexes(task);

      // Clean up all task resources
      cleanupTaskResources(taskId);

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
    }

    if (!marketplace) {
      warningBids[taskId] = {
        opensea: false,
        magiceden: false,
        blur: false
      };
    }

    // Fetch orders for cancellation
    const orders = await fetchOrdersForCancellation(taskId, marketplace);
    console.log(YELLOW + `[stopTask] Fetched orders for cancellation:` + RESET);
    console.log(YELLOW + `  OpenSea: ${orders.opensea.length}` + RESET);
    console.log(YELLOW + `  MagicEden: ${orders.magiceden.length}` + RESET);
    console.log(YELLOW + `  Blur: ${orders.blur.length}` + RESET);

    // Log detailed order information for debugging
    if (orders.opensea.length > 0) {
      console.log(YELLOW + `  OpenSea order samples (first 3):` + RESET);
      orders.opensea.slice(0, 3).forEach(order => {
        console.log(YELLOW + `    - ${order.tokenId || 'collection'} | ${order.price} | expires: ${new Date(order.expiresAt).toISOString()}` + RESET);
      });
    }
    if (orders.magiceden.length > 0) {
      console.log(YELLOW + `  MagicEden order samples (first 3):` + RESET);
      orders.magiceden.slice(0, 3).forEach(order => {
        console.log(YELLOW + `    - ${order.tokenId || 'collection'} | ${order.price} | expires: ${new Date(order.expiresAt).toISOString()}` + RESET);
      });
    }
    if (orders.blur.length > 0) {
      console.log(YELLOW + `  Blur order samples (first 3):` + RESET);
      orders.blur.slice(0, 3).forEach(order => {
        console.log(YELLOW + `    - ${order.tokenId || 'collection'} | ${order.price} | expires: ${new Date(order.expiresAt).toISOString()}` + RESET);
      });
    }

    // Count total active orders from database
    let totalBids = 0;
    const marketplaceTotals = {
      opensea: 0,
      magiceden: 0,
      blur: 0
    };

    if (marketplace) {
      // Count for specific marketplace
      totalBids = await Order.countDocuments({
        taskId: taskId,
        marketplace: marketplace.toLowerCase(),
        expiresAt: { $gt: new Date() }
      });
    } else {
      // Count all active orders for the task and by marketplace
      totalBids = await Order.countDocuments({
        taskId: taskId,
        expiresAt: { $gt: new Date() }
      });

      // Get marketplace-specific counts for global cancellation
      const [openSeaCount, magicEdenCount, blurCount] = await Promise.all([
        Order.countDocuments({
          taskId: taskId,
          marketplace: 'opensea',
          expiresAt: { $gt: new Date() }
        }),
        Order.countDocuments({
          taskId: taskId,
          marketplace: 'magiceden',
          expiresAt: { $gt: new Date() }
        }),
        Order.countDocuments({
          taskId: taskId,
          marketplace: 'blur',
          expiresAt: { $gt: new Date() }
        })
      ]);

      marketplaceTotals.opensea = openSeaCount;
      marketplaceTotals.magiceden = magicEdenCount;
      marketplaceTotals.blur = blurCount;

      console.log(CYAN + `[stopTask] Marketplace-specific counts - OpenSea: ${openSeaCount}, MagicEden: ${magicEdenCount}, Blur: ${blurCount}` + RESET);
    }

    // Check if there are no orders and task is not running - immediately stop
    const totalOrderCount = orders.opensea.length + orders.magiceden.length + orders.blur.length;
    if (totalOrderCount === 0 && !task.running) {
      console.log(GREEN + `[stopTask] No orders to cancel and task not running - immediately setting to STOPPED` + RESET);

      // Update task status to STOPPED immediately
      if (taskStatus[taskId]) {
        taskStatus[taskId].status = 'STOPPED';
        taskStatus[taskId].cancellationPercentage = 100;
        taskStatus[taskId].runningPercentage = 0;
        taskStatus[taskId].processingPercentage = 0;
      }

      // Broadcast the update
      broadcastBidRates();

      // Return early
      return;
    }

    // Initialize cancellation tracking
    console.log(CYAN + `[stopTask] Initializing cancellation tracking - Task: ${taskId}, TotalBids: ${totalBids}, Marketplace: ${marketplace || 'ALL'}` + RESET);
    cancellationTracker.initializeTracking(taskId, totalBids, marketplace);

    // For global cancellations, set marketplace-specific totals
    if (!marketplace) {
      cancellationTracker.setMarketplaceTotals(taskId, marketplaceTotals);
      console.log(CYAN + `[stopTask] Marketplace totals set - OpenSea: ${marketplaceTotals.opensea}, MagicEden: ${marketplaceTotals.magiceden}, Blur: ${marketplaceTotals.blur}` + RESET);
    }

    // Log current cancellation tracking state
    const trackingStatus = cancellationTracker.getStatus(taskId, marketplace);
    if (trackingStatus) {
      console.log(CYAN + `[stopTask] Tracking initialized - Total: ${trackingStatus.totalBids}, Completed: ${trackingStatus.completedCancellations}` + RESET);
    }

    // Set initial cancellation percentage
    if (taskStatus[taskId]) {
      await updateTaskCancellationPercentage(taskId, 0);
    }

    console.log(YELLOW + `[stopTask] Total active bids to cancel: ${totalBids}` + RESET);

    // Get currency symbol for OpenSea cancellations
    let currencySymbol = 'WETH'; // Default to WETH
    try {
      const { currencySymbol: cs } = await calculateBidPrice(task, 0, "opensea");
      currencySymbol = cs;
    } catch (error) {
      console.warn(`[stopTask] Failed to get currency symbol, using default WETH:`, error);
    }

    switch (marketplace?.toLowerCase()) {
      case 'opensea': {
        // Cancel active HTTP requests first
        const httpResult = await cancelOpenseaRequest(taskId);
        console.log(GOLD + `  HTTP Cancellation Result: ${JSON.stringify(httpResult)}` + RESET);

        // Now cancel actual orders
        if (orders.opensea.length > 0 && WALLET_PRIVATE_KEY) {
          console.log(GOLD + `  Cancelling ${orders.opensea.length} OpenSea orders...` + RESET);


          // Track cancellation operation
          const cancellationOp: CancellationOperation = {
            taskId,
            marketplace: 'opensea',
            totalJobs: orders.opensea.length,
            completedJobs: 0,
            startTime: Date.now()
          };
          activeCancellationOperations.set(`${taskId}-opensea`, cancellationOp);

          await cancelOpenseaBids(orders.opensea, WALLET_PRIVATE_KEY, taskId, currencySymbol);
        } else if (orders.opensea.length === 0) {
          console.log(GREEN + `  ✅ No orders to cancel` + RESET);

          // Let the cancellation tracker handle percentage updates naturally

          await setMarketplaceCancellationComplete(taskId, marketplace);
          completionHandled = true;
          console.log(GOLD + `  🏁 Completion handled flag set to true` + RESET);
        } else if (!WALLET_PRIVATE_KEY) {
          console.log(RED + `  ❌ Cannot cancel orders - wallet private key not available` + RESET);
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
        const httpResult = await cancelMagicedenRequest(taskId);
        console.log(MAGENTA + `  HTTP Cancellation Result: ${JSON.stringify(httpResult)}` + RESET);

        // Now cancel actual orders
        if (orders.magiceden.length > 0 && WALLET_PRIVATE_KEY) {
          console.log(MAGENTA + `  Cancelling ${orders.magiceden.length} MagicEden orders...` + RESET);


          // Track cancellation operation
          const cancellationOp: CancellationOperation = {
            taskId,
            marketplace: 'magiceden',
            totalJobs: orders.magiceden.length,
            completedJobs: 0,
            startTime: Date.now()
          };
          activeCancellationOperations.set(`${taskId}-magiceden`, cancellationOp);

          await cancelMagicedenBids(orders.magiceden, WALLET_PRIVATE_KEY, taskId);
        } else if (orders.magiceden.length === 0) {
          console.log(GREEN + `  ✅ No orders to cancel` + RESET);

          // Let the cancellation tracker handle percentage updates naturally

          setMarketplaceCancellationComplete(taskId, marketplace);
          completionHandled = true;
          console.log(MAGENTA + `  🏁 Completion handled flag set to true` + RESET);
        } else if (!WALLET_PRIVATE_KEY) {
          console.log(RED + `  ❌ Cannot cancel orders - wallet private key not available` + RESET);
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
        const httpResult = await cancelBlurRequest(taskId);
        console.log(BLUE + `  HTTP Cancellation Result: ${JSON.stringify(httpResult)}` + RESET);

        // Now cancel actual orders
        if (orders.blur.length > 0 && WALLET_PRIVATE_KEY) {
          console.log(BLUE + `  Cancelling ${orders.blur.length} Blur orders...` + RESET);


          // Track cancellation operation
          const cancellationOp: CancellationOperation = {
            taskId,
            marketplace: 'blur',
            totalJobs: orders.blur.length,
            completedJobs: 0,
            startTime: Date.now()
          };
          activeCancellationOperations.set(`${taskId}-blur`, cancellationOp);

          await cancelBlurBids(orders.blur, WALLET_PRIVATE_KEY, taskId);
        } else if (orders.blur.length === 0) {
          console.log(GREEN + `  ✅ No orders to cancel` + RESET);

          // Let the cancellation tracker handle percentage updates naturally

          setMarketplaceCancellationComplete(taskId, marketplace);
          completionHandled = true;
          console.log(BLUE + `  🏁 Completion handled flag set to true` + RESET);
        } else if (!WALLET_PRIVATE_KEY) {
          console.log(RED + `  ❌ Cannot cancel orders - wallet private key not available` + RESET);
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

        // Log HTTP cancellation results
        const marketplaceNames = ['opensea', 'magiceden', 'blur'];
        httpResults.forEach((result, index) => {
          if (result.status === 'rejected' || (result.status === 'fulfilled' && result.value.error)) {
            logger.logWarning(ErrorCategory.MARKETPLACE, `Failed to cancel ${marketplaceNames[index]} HTTP requests`, {
              taskId,
              marketplace: marketplaceNames[index],
              errorMessage: result.status === 'rejected' ? result.reason : result.value.error
            });
          }
        });

        // Now cancel actual orders for all marketplaces
        const totalOrderCount = orders.opensea.length + orders.magiceden.length + orders.blur.length;

        if (totalOrderCount > 0 && WALLET_PRIVATE_KEY) {
          console.log(YELLOW + `[stopTask] Cancelling orders for all marketplaces...` + RESET);


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
            console.log(GOLD + `  Cancelling ${orders.opensea.length} OpenSea orders...` + RESET);
            cancellationPromises.push(cancelOpenseaBids(orders.opensea, WALLET_PRIVATE_KEY, taskId, currencySymbol));
          }

          if (orders.magiceden.length > 0) {
            console.log(MAGENTA + `  Cancelling ${orders.magiceden.length} MagicEden orders...` + RESET);
            cancellationPromises.push(cancelMagicedenBids(orders.magiceden, WALLET_PRIVATE_KEY, taskId));
          }

          if (orders.blur.length > 0) {
            console.log(BLUE + `  Cancelling ${orders.blur.length} Blur orders...` + RESET);
            cancellationPromises.push(cancelBlurBids(orders.blur, WALLET_PRIVATE_KEY, taskId));
          }

          await Promise.allSettled(cancellationPromises);
        } else if (totalOrderCount === 0) {
          console.log(GREEN + `  ✅ No orders to cancel` + RESET);

          // For global cancellation with no orders, let tracker handle it naturally

          await setTaskCancellationComplete(taskId);
        } else if (!WALLET_PRIVATE_KEY) {
          console.log(RED + `  ❌ Cannot cancel orders - wallet private key not available` + RESET);
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

    const statusResult = await updateTaskStatus(task, start, marketplace);
    if (!statusResult.success) {
      logger.logWarning(ErrorCategory.GENERAL, 'Failed to update task status', {
        taskId: task._id,
        contractSlug: task.contract.slug,
        start,
        marketplace,
        errorMessage: statusResult.error
      });
    }
    if (task.outbidOptions.counterbid && !marketplace) {
      const unsubscribeResult = await unsubscribeFromCollection(task);
      if (!unsubscribeResult.success) {
        logger.logWebSocketError('Failed to unsubscribe from collection during task stop', {
          taskId: task._id,
          contractSlug: task.contract.slug,
          errorMessage: unsubscribeResult.error,
          counterbidEnabled: task.outbidOptions.counterbid,
          operation: 'task_stop_unsubscribe'
        });
      } else if (unsubscribeResult.unsubscribed) {
        logger.logInfo(ErrorCategory.WEBSOCKET, 'Successfully unsubscribed from collection during task stop', {
          taskId: task._id,
          contractSlug: task.contract.slug,
          counterbidEnabled: task.outbidOptions.counterbid,
          operation: 'task_stop_unsubscribe'
        });
      }
    }
    const removalResult = await removePendingAndWaitingBids(task, marketplace);
    if (!removalResult.success) {
      logger.logQueueError('Failed to remove pending and waiting bids during task stop', {
        taskId: task._id,
        contractSlug: task.contract.slug,
        marketplace,
        jobsRemoved: removalResult.jobsRemoved,
        bidsCancelled: removalResult.bidsCancelled,
        errorsCount: removalResult.errors.length,
        duration: removalResult.duration,
        errors: removalResult.errors.slice(0, 3), // Log first 3 errors
        operation: 'task_stop_removal'
      });
    } else {
      logger.logInfo(ErrorCategory.QUEUE, 'Successfully removed pending and waiting bids during task stop', {
        taskId: task._id,
        contractSlug: task.contract.slug,
        marketplace,
        jobsRemoved: removalResult.jobsRemoved,
        bidsCancelled: removalResult.bidsCancelled,
        duration: removalResult.duration,
        operation: 'task_stop_removal'
      });
    }

    if (!task?.selectedMarketplaces.length || !task.running) {
      if (taskStatus[taskId]) {
        delete taskStatus[taskId].nextLoopTime
        taskStatus[taskId].waitingTime = 0
      }

      // Clean up all task resources
      cleanupTaskResources(taskId);
    }

    // Check if there are any orders and determine appropriate status
    const totalOrdersCount = orders.opensea.length + orders.magiceden.length + orders.blur.length;
    if (totalOrdersCount === 0) {
      if (!task?.running) {
        // Task is not running and no orders - check if should be STOPPED
        if (taskStatus[taskId]) {
          delete taskStatus[taskId].nextLoopTime;
          taskStatus[taskId].waitingTime = 0;

          // Only set STOPPED if both conditions are met
          if (await shouldTaskBeStopped(taskId)) {
            taskStatus[taskId].status = 'STOPPED';
            console.log(GREEN + `[stopTask] Task ${taskId} has no orders and meets STOPPED conditions` + RESET);
          } else {
            // Keep current status (likely CANCELLING if cancellation not complete)
            console.log(YELLOW + `[stopTask] Task ${taskId} has no orders but doesn't meet STOPPED conditions` + RESET);
          }
        }

        // Clear countdown timer
        const countdownTimer = taskCountdownTimers.get(taskId);
        if (countdownTimer) {
          clearInterval(countdownTimer);
          taskCountdownTimers.delete(taskId);
        }
      } else {
        // Task is still running but has no orders
        if (taskStatus[taskId]) {
          taskStatus[taskId].status = 'SCHEDULED';
        }
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
      cancellationResult = await cancelAllRelatedBids(task, WALLET_PRIVATE_KEY, marketplace);
    } else {
      console.log(YELLOW + `[StopTask] Skipping bid cancellation for task ${taskId} - no valid wallet key available` + RESET);
      logger.logWarning(ErrorCategory.MARKETPLACE, 'Skipping bid cancellation - no valid wallet key', {
        taskId: task._id,
        contractSlug: task.contract.slug,
        marketplace,
        operation: 'task_stop_bid_cancellation'
      });
    }

    if (!cancellationResult.success) {
      logger.logMarketplaceError('Failed to cancel related bids during task stop', {
        taskId: task._id,
        contractSlug: task.contract.slug,
        marketplace,
        totalOrdersFound: cancellationResult.totalOrdersFound,
        errorsCount: cancellationResult.errors.length,
        errors: cancellationResult.errors.slice(0, 3), // Log first 3 errors
        duration: cancellationResult.duration,
        results: cancellationResult.results,
        operation: 'task_stop_bid_cancellation'
      });
    } else if (cancellationResult.totalOrdersFound > 0) {
      logger.logInfo(ErrorCategory.MARKETPLACE, 'Successfully cancelled related bids during task stop', {
        taskId: task._id,
        contractSlug: task.contract.slug,
        marketplace,
        totalOrdersFound: cancellationResult.totalOrdersFound,
        duration: cancellationResult.duration,
        results: cancellationResult.results,
        operation: 'task_stop_bid_cancellation_complete'
      });
    }

    // === ENHANCED STOPTASK: FINAL COMPLETION ===
    completedOperations += 1;
    updateCancellationProgress(taskId, completedOperations, totalOperations);

    if (!completionHandled) {
      if (marketplace) {
        console.log(GREEN + `  🎯 Calling setMarketplaceCancellationComplete` + RESET);
        await setMarketplaceCancellationComplete(taskId, marketplace);
      } else {
        console.log(GREEN + `  🛑 Calling setTaskCancellationComplete` + RESET);
        await setTaskCancellationComplete(taskId);
      }
    } else {
      console.log(YELLOW + `  ⏭️  Completion already handled, skipping` + RESET);
    }

    console.log(GREEN + `[stopTask] Enhanced cancellation complete for task ${taskId}` + RESET);

  } catch (error: any) {
    console.error(`[StopTask] Error in enhanced stopTask for ${taskId}:`, {
      error: error.message,
      command: error.command,
      timestamp: new Date().toISOString()
    });

    // Force complete cancellation on error and leave task blacklisted
    console.log(RED + `  Error: ${error.message}` + RESET);
    console.log(RED + `  Marketplace: ${marketplace || 'ALL'}` + RESET);

    if (marketplace) {
      console.log(RED + `  🎯 Force calling setMarketplaceCancellationComplete` + RESET);
      await setMarketplaceCancellationComplete(taskId, marketplace);
    } else {
      console.log(RED + `  🛑 Force calling setTaskCancellationComplete` + RESET);
      await setTaskCancellationComplete(taskId);
    }

    // Log that task remains blacklisted due to error
    console.log(YELLOW + `[StopTask] Task ${taskId} remains blacklisted due to error - manual intervention may be required` + RESET);
  }
}

async function deleteTask(task: ITask) {
  try {
    await stopTask(task, false);
    const taskIndex = currentTasks.findIndex(t => t._id === task._id);
    if (taskIndex !== -1) {
      currentTasks.splice(taskIndex, 1);
      updateTaskIndexes(currentTasks);
    }
    const taskId = task._id.toString();
    activeTasks.delete(task._id);

    // Clean up taskStatus
    delete taskStatus[taskId];

    // Clear countdown timer
    const countdownTimer = taskCountdownTimers.get(taskId);
    if (countdownTimer) {
      clearInterval(countdownTimer);
      taskCountdownTimers.delete(taskId);
    }

    // Clean up all task resources
    cleanupTaskResources(taskId);
  } catch (error) {
    console.error(RED + `Error deleting task: ` + RESET, error);
  }
}


const BATCH_SIZE = 1000; // Process jobs in batches
const JOB_REMOVAL_TIMEOUT = 30000; // 30 seconds
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
      await logger.logWarning(ErrorCategory.QUEUE, error, { task, marketplace });
      return { success: false, jobsRemoved: 0, bidsCancelled: 0, errors: [error], duration: 0 };
    }

    if (!taskId) {
      const error = 'Task missing _id property';
      await logger.logWarning(ErrorCategory.QUEUE, error, { task, marketplace });
      return { success: false, jobsRemoved: 0, bidsCancelled: 0, errors: [error], duration: 0 };
    }

    if (!contractSlug) {
      const error = 'Task missing contract.slug property';
      await logger.logWarning(ErrorCategory.QUEUE, error, { taskId, marketplace });
      return { success: false, jobsRemoved: 0, bidsCancelled: 0, errors: [error], duration: 0 };
    }

    // Validate marketplace parameter if provided
    if (marketplace && !['opensea', 'magiceden', 'blur'].includes(marketplace.toLowerCase())) {
      const error = 'Invalid marketplace parameter';
      await logger.logWarning(ErrorCategory.QUEUE, error, {
        taskId,
        contractSlug,
        marketplace,
        validMarketplaces: ['opensea', 'magiceden', 'blur']
      });
      return { success: false, jobsRemoved: 0, bidsCancelled: 0, errors: [error], duration: 0 };
    }

    await logger.logInfo(ErrorCategory.QUEUE, 'Starting removal of pending and waiting bids', {
      taskId,
      contractSlug,
      marketplace,
      operation: 'remove_pending_bids'
    });

    // STEP 1: Immediately cancel active bids for fast response
    try {
      const wallet = await walletCache.getWalletById(task.walletId);
      if (wallet?.encryptedPrivateKey) {
        const privateKey = decryptPrivateKey(wallet.encryptedPrivateKey);

        // Cancel active bids in parallel for immediate response
        const bidCancellationPromises = [];

        if (!marketplace || marketplace.toLowerCase() === 'opensea') {
          bidCancellationPromises.push(
            fetchAndCancelOpenseaBids(task, privateKey)
              .then(() => { bidsCancelled++; })
              .catch(error => {
                errors.push(`OpenSea bid cancellation failed: ${error.message}`);
              })
          );
        }

        if (!marketplace || marketplace.toLowerCase() === 'magiceden') {
          bidCancellationPromises.push(
            fetchAndCancelMagicedenBids(task, privateKey)
              .then(() => { bidsCancelled++; })
              .catch(error => {
                errors.push(`MagicEden bid cancellation failed: ${error.message}`);
              })
          );
        }

        if (!marketplace || marketplace.toLowerCase() === 'blur') {
          bidCancellationPromises.push(
            fetchAndCancelBlurBids(task, privateKey)
              .then(() => { bidsCancelled++; })
              .catch(error => {
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

        await logger.logInfo(ErrorCategory.QUEUE, 'Completed active bid cancellation', {
          taskId,
          contractSlug,
          marketplace,
          bidsProcessed: bidCancellationPromises.length,
          bidsSuccessful: bidsCancelled,
          operation: 'active_bid_cancellation'
        });
      }
    } catch (error) {
      const errorMsg = `Active bid cancellation failed: ${error instanceof Error ? error.message : String(error)}`;
      errors.push(errorMsg);
      await logger.logWarning(ErrorCategory.QUEUE, errorMsg, {
        taskId,
        contractSlug,
        marketplace
      });
    }

    // STEP 2: Generate job IDs for removal
    let jobIds: string[] = [];
    try {
      switch (marketplace?.toLowerCase()) {
        case OPENSEA.toLowerCase():
          jobIds = getJobIds(OPENSEA.toLowerCase(), task);
          break;
        case MAGICEDEN.toLowerCase():
          jobIds = getJobIds(MAGICEDEN.toLowerCase(), task);
          break;
        case BLUR.toLowerCase():
          jobIds = getJobIds(BLUR.toLowerCase(), task);
          break;
        default:
          jobIds = [
            ...getJobIds(BLUR.toLowerCase(), task),
            ...getJobIds(MAGICEDEN.toLowerCase(), task),
            ...getJobIds(OPENSEA.toLowerCase(), task)
          ];
      }

      await logger.logDebug(ErrorCategory.QUEUE, 'Generated job IDs for removal', {
        taskId,
        contractSlug,
        marketplace,
        jobIdsCount: jobIds.length,
        sampleJobIds: jobIds.slice(0, 3)
      });
    } catch (error) {
      const errorMsg = `Job ID generation failed: ${error instanceof Error ? error.message : String(error)}`;
      errors.push(errorMsg);
      await logger.logWarning(ErrorCategory.QUEUE, errorMsg, {
        taskId,
        contractSlug,
        marketplace
      });
    }

    // STEP 3: Direct job removal for better performance
    if (jobIds.length > 0) {
      try {
        await logger.logInfo(ErrorCategory.QUEUE, 'Starting direct job removal', {
          taskId,
          contractSlug,
          marketplace,
          totalJobIds: jobIds.length,
          operation: 'direct_job_removal'
        });

        // Process jobs in parallel batches for better performance
        const chunkSize = 50; // Process 50 jobs at a time
        const jobIdChunks = [];
        for (let i = 0; i < jobIds.length; i += chunkSize) {
          jobIdChunks.push(jobIds.slice(i, i + chunkSize));
        }

        for (const chunk of jobIdChunks) {
          const chunkPromises = chunk.map(async (jobId) => {
            try {
              const job = await queue.getJob(jobId);
              if (job) {
                await Promise.race([
                  job.remove(),
                  new Promise((_, reject) => {
                    setTimeout(() => reject(new Error('Job removal timeout')), DIRECT_REMOVAL_TIMEOUT);
                  })
                ]);
                jobsRemoved++;
              }
            } catch (error) {
              // Job might not exist or already removed - this is not necessarily an error
              await logger.logDebug(ErrorCategory.QUEUE, 'Job removal failed or job not found', {
                taskId,
                contractSlug,
                jobId,
                errorMessage: error instanceof Error ? error.message : String(error)
              });
            }
          });

          // Process chunk with timeout
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
            await logger.logWarning(ErrorCategory.QUEUE, errorMsg, {
              taskId,
              contractSlug,
              chunkSize: chunk.length
            });
          }
        }

        await logger.logInfo(ErrorCategory.QUEUE, 'Direct job removal completed', {
          taskId,
          contractSlug,
          marketplace,
          jobsRemoved,
          totalJobIds: jobIds.length,
          operation: 'direct_job_removal_complete'
        });
      } catch (error) {
        const errorMsg = `Direct job removal failed: ${error instanceof Error ? error.message : String(error)}`;
        errors.push(errorMsg);
        await logger.logQueueError(errorMsg, {
          taskId,
          contractSlug,
          marketplace,
          jobIdsCount: jobIds.length
        });
      }
    }

    const duration = Date.now() - startTime;
    const success = errors.length === 0;

    await logger.logInfo(ErrorCategory.QUEUE, 'Completed removal of pending and waiting bids', {
      taskId,
      contractSlug,
      marketplace,
      success,
      jobsRemoved,
      bidsCancelled,
      errorsCount: errors.length,
      duration,
      operation: 'remove_pending_bids_complete'
    });

    return { success, jobsRemoved, bidsCancelled, errors, duration };
  } catch (error) {
    const duration = Date.now() - startTime;
    const errorMsg = error instanceof Error ? error.message : String(error);

    await logger.logQueueError('Failed to remove pending and waiting bids', {
      taskId,
      contractSlug,
      marketplace,
      errorMessage: errorMsg,
      stackTrace: error instanceof Error ? error.stack : undefined,
      duration,
      operation: 'remove_pending_bids_failed'
    });

    return {
      success: false,
      jobsRemoved,
      bidsCancelled,
      errors: [...errors, errorMsg],
      duration
    };
  }
}


function getJobIds(marketplace: string, task: ITask) {
  const jobIds: string[] = []
  const selectedTraits = transformNewTask(task.selectedTraits?.[marketplace.toLowerCase()])
  const isTraitBid = task.bidType === "collection" && selectedTraits && Object.keys(selectedTraits).length > 0

  const tokenBid = task.bidType === "token"
  const traits = transformOpenseaTraits(selectedTraits);

  if (isTraitBid) {
    traits.map((trait) => {
      const uniqueKey = `${trait.type}-${trait.value}`
      jobIds.push(`bid-${task._id}-${task.contract.slug}-${marketplace.toLowerCase()}-${uniqueKey}`)
    })
  } else if (tokenBid) {
    task.tokenIds.map((tokenId) => {
      jobIds.push(`bid-${task._id}-${task.contract.slug}-${marketplace.toLowerCase()}-${tokenId}`)
    })
  } else {
    jobIds.push(`bid-${task._id}-${task.contract.slug}-${marketplace.toLowerCase()}-collection`)
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
      await logger.logWarning(ErrorCategory.GENERAL, 'Invalid task provided to updateTaskStatus', {
        task: task,
        running,
        marketplace
      });
      return { success: false, updated: false, error: 'Invalid task provided' };
    }

    if (!task._id) {
      await logger.logWarning(ErrorCategory.GENERAL, 'Task missing _id property', {
        task: task,
        running,
        marketplace
      });
      return { success: false, updated: false, error: 'Task missing _id property' };
    }

    if (!task.contract?.slug) {
      await logger.logWarning(ErrorCategory.GENERAL, 'Task missing contract.slug property', {
        taskId: task._id,
        running,
        marketplace
      });
      return { success: false, updated: false, error: 'Task missing contract.slug property' };
    }

    if (typeof running !== 'boolean') {
      await logger.logWarning(ErrorCategory.GENERAL, 'Invalid running parameter type', {
        taskId: task._id,
        running,
        marketplace,
        runningType: typeof running
      });
      return { success: false, updated: false, error: 'Invalid running parameter type' };
    }

    // Validate marketplace parameter if provided
    if (marketplace && !['opensea', 'magiceden', 'blur'].includes(marketplace.toLowerCase())) {
      await logger.logWarning(ErrorCategory.GENERAL, 'Invalid marketplace parameter', {
        taskId: task._id,
        running,
        marketplace,
        validMarketplaces: ['opensea', 'magiceden', 'blur']
      });
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
      await logger.logDebug(ErrorCategory.GENERAL, 'Initialized warning bids for task', {
        taskId,
        contractSlug: task.contract.slug
      });
    }

    // Update task status in currentTasks array
    const taskIndex = currentTasks.findIndex(t => t._id === taskId);
    if (taskIndex !== -1) {
      // For marketplace-specific operations, we still want to update the overall task status
      // unless it's a partial update that shouldn't affect the global running state
      if (marketplace) {
        // Log marketplace-specific status change but don't update global running state
        await logger.logInfo(ErrorCategory.GENERAL, `Task ${running ? 'started' : 'stopped'} for marketplace`, {
          taskId,
          contractSlug: task.contract.slug,
          marketplace: marketplace.toLowerCase(),
          running,
          operation: 'marketplace_specific_update'
        });
      } else {
        // Update global task running state
        const previousRunning = currentTasks[taskIndex].running;
        currentTasks[taskIndex].running = running;
        taskUpdated = true;

        await logger.logInfo(ErrorCategory.GENERAL, `Task ${running ? 'started' : 'stopped'} globally`, {
          taskId,
          contractSlug: task.contract.slug,
          previousRunning,
          newRunning: running,
          operation: 'global_status_update'
        });
      }
    } else {
      await logger.logWarning(ErrorCategory.GENERAL, 'Task not found in currentTasks array', {
        taskId,
        contractSlug: task.contract.slug,
        running,
        marketplace,
        currentTasksCount: currentTasks.length
      });
    }

    // Enhanced logging with structured data instead of console.log
    const statusMessage = running ? 'Started' : 'Stopped';
    // Enhanced logging with structured data instead of console.log

    await logger.logInfo(ErrorCategory.GENERAL, `${statusMessage} processing task`, {
      taskId,
      contractSlug: task.contract.slug,
      running,
      marketplace: marketplace?.toLowerCase(),
      taskUpdated,
      operation: 'task_status_update',
      timestamp: new Date().toISOString()
    });

    return { success: true, updated: taskUpdated };
  } catch (error) {
    await logger.logGeneralError('Failed to update task status', {
      taskId: task?._id,
      contractSlug: task?.contract?.slug,
      running,
      marketplace,
      errorMessage: error instanceof Error ? error.message : String(error),
      stackTrace: error instanceof Error ? error.stack : undefined
    });
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
 * @param WALLET_PRIVATE_KEY - Private key for wallet authentication
 * @returns Promise with operation results
 */
async function fetchAndCancelOpenseaBids(
  task: ITask,
  WALLET_PRIVATE_KEY: string
): Promise<{ success: boolean; ordersFound: number; error?: string }> {
  const taskId = task?._id?.toString();
  const contractSlug = task?.contract?.slug;

  try {
    // Input validation
    if (!task || !taskId || !contractSlug) {
      const error = 'Invalid task provided to fetchAndCancelOpenseaBids';
      await logger.logWarning(ErrorCategory.MARKETPLACE, error, { task });
      return { success: false, ordersFound: 0, error };
    }

    if (!WALLET_PRIVATE_KEY) {
      const error = 'Invalid wallet private key provided';
      await logger.logWarning(ErrorCategory.MARKETPLACE, error, { taskId, contractSlug });
      return { success: false, ordersFound: 0, error };
    }

    await logger.logInfo(ErrorCategory.MARKETPLACE, 'Starting OpenSea bid cancellation', {
      taskId,
      contractSlug,
      marketplace: 'opensea',
      operation: 'fetch_and_cancel_opensea_bids'
    });

    // Calculate bid price for currency symbol
    const { currencySymbol } = await Promise.race([
      calculateBidPrice(task, 0, "magiceden"),
      new Promise<never>((_, reject) => {
        setTimeout(() => reject(new Error('Calculate bid price timeout')), 5000);
      })
    ]);

    // Implement pagination to handle large numbers of orders
    const BATCH_SIZE = 1000;
    let allOpenseaBids: any[] = [];
    let skip = 0;
    let hasMore = true;
    let totalOrdersProcessed = 0;

    await logger.logInfo(ErrorCategory.MARKETPLACE, 'Starting paginated OpenSea order fetch', {
      taskId,
      contractSlug,
      batchSize: BATCH_SIZE,
      marketplace: 'opensea'
    });

    while (hasMore) {
      try {
        // Fetch batch of orders with timeout
        const batch = await Promise.race([
          Order.find({
            slug: contractSlug,
            taskId: taskId,
            marketplace: OPENSEA.toLowerCase()
          })
            .sort({ createdAt: -1 })
            .skip(skip)
            .limit(BATCH_SIZE),
          new Promise<never>((_, reject) => {
            setTimeout(() => reject(new Error('Database query timeout')), 10000);
          })
        ]);

        if (batch.length === 0) {
          hasMore = false;
          break;
        }

        // Add to total orders
        allOpenseaBids = allOpenseaBids.concat(batch);
        totalOrdersProcessed += batch.length;

        await logger.logDebug(ErrorCategory.MARKETPLACE, 'Fetched OpenSea order batch', {
          taskId,
          contractSlug,
          batchNumber: Math.floor(skip / BATCH_SIZE) + 1,
          ordersInBatch: batch.length,
          totalOrdersSoFar: totalOrdersProcessed,
          marketplace: 'opensea'
        });

        // Process this batch immediately to avoid memory buildup
        if (batch.length > 0) {
          // Queue cancellations for this batch
          await cancelOpenseaBids(batch, WALLET_PRIVATE_KEY, taskId, currencySymbol);

          await logger.logInfo(ErrorCategory.MARKETPLACE, 'Queued OpenSea bid cancellation batch', {
            taskId,
            contractSlug,
            batchNumber: Math.floor(skip / BATCH_SIZE) + 1,
            ordersQueued: batch.length,
            totalOrdersQueued: totalOrdersProcessed,
            marketplace: 'opensea',
            operation: 'opensea_batch_cancellations_queued'
          });
        }

        // Check if we got a full batch
        if (batch.length < BATCH_SIZE) {
          hasMore = false;
        } else {
          skip += BATCH_SIZE;
        }

        // Clear batch from memory to prevent buildup
        allOpenseaBids = [];

      } catch (error) {
        await logger.logMarketplaceError('Failed to fetch OpenSea order batch', {
          taskId,
          contractSlug,
          batchNumber: Math.floor(skip / BATCH_SIZE) + 1,
          skip,
          errorMessage: error instanceof Error ? error.message : String(error),
          marketplace: 'opensea'
        });
        hasMore = false; // Stop pagination on error
      }
    }

    await logger.logInfo(ErrorCategory.MARKETPLACE, 'Completed OpenSea order processing', {
      taskId,
      contractSlug,
      totalOrdersProcessed,
      totalBatches: Math.ceil(totalOrdersProcessed / BATCH_SIZE),
      marketplace: 'opensea',
      operation: 'opensea_order_processing_complete'
    });

    if (totalOrdersProcessed === 0) {
      await logger.logInfo(ErrorCategory.MARKETPLACE, 'No OpenSea bids found to cancel', {
        taskId,
        contractSlug,
        marketplace: 'opensea'
      });
    }

    return { success: true, ordersFound: allOpenseaBids.length };
  } catch (error) {
    const errorMsg = error instanceof Error ? error.message : String(error);

    await logger.logMarketplaceError('Failed to fetch and cancel OpenSea bids', {
      taskId,
      contractSlug,
      marketplace: 'opensea',
      errorMessage: errorMsg,
      stackTrace: error instanceof Error ? error.stack : undefined,
      operation: 'fetch_and_cancel_opensea_bids_failed'
    });

    return { success: false, ordersFound: 0, error: errorMsg };
  }
}

/**
 * Fetches and cancels Blur bids for a specific task
 * @param task - The bidding task to cancel bids for
 * @param WALLET_PRIVATE_KEY - Private key for wallet authentication
 * @returns Promise with operation results
 */
async function fetchAndCancelBlurBids(
  task: ITask,
  WALLET_PRIVATE_KEY: string
): Promise<{ success: boolean; ordersFound: number; error?: string }> {
  const taskId = task?._id?.toString();
  const contractSlug = task?.contract?.slug;

  try {
    // Input validation
    if (!task || !taskId || !contractSlug) {
      const error = 'Invalid task provided to fetchAndCancelBlurBids';
      await logger.logWarning(ErrorCategory.MARKETPLACE, error, { task });
      return { success: false, ordersFound: 0, error };
    }

    if (!WALLET_PRIVATE_KEY) {
      const error = 'Invalid wallet private key provided';
      await logger.logWarning(ErrorCategory.MARKETPLACE, error, { taskId, contractSlug });
      return { success: false, ordersFound: 0, error };
    }

    await logger.logInfo(ErrorCategory.MARKETPLACE, 'Starting Blur bid cancellation', {
      taskId,
      contractSlug,
      marketplace: 'blur',
      operation: 'fetch_and_cancel_blur_bids'
    });

    // Implement pagination to handle large numbers of orders
    const BATCH_SIZE = 1000;
    let allBlurBids: any[] = [];
    let skip = 0;
    let hasMore = true;
    let totalOrdersProcessed = 0;

    await logger.logInfo(ErrorCategory.MARKETPLACE, 'Starting paginated Blur order fetch', {
      taskId,
      contractSlug,
      batchSize: BATCH_SIZE,
      marketplace: 'blur'
    });

    while (hasMore) {
      try {
        // Fetch batch of orders with timeout
        const batch = await Promise.race([
          Order.find({
            slug: contractSlug,
            taskId: taskId,
            marketplace: BLUR.toLowerCase()
          })
            .sort({ createdAt: -1 })
            .skip(skip)
            .limit(BATCH_SIZE),
          new Promise<never>((_, reject) => {
            setTimeout(() => reject(new Error('Database query timeout')), 10000);
          })
        ]);

        if (batch.length === 0) {
          hasMore = false;
          break;
        }

        // Add to total orders
        allBlurBids = allBlurBids.concat(batch);
        totalOrdersProcessed += batch.length;

        await logger.logDebug(ErrorCategory.MARKETPLACE, 'Fetched Blur order batch', {
          taskId,
          contractSlug,
          batchNumber: Math.floor(skip / BATCH_SIZE) + 1,
          ordersInBatch: batch.length,
          totalOrdersSoFar: totalOrdersProcessed,
          marketplace: 'blur'
        });

        // Process this batch immediately to avoid memory buildup
        if (batch.length > 0) {
          // Queue cancellations for this batch
          await cancelBlurBids(batch, WALLET_PRIVATE_KEY, taskId);

          await logger.logInfo(ErrorCategory.MARKETPLACE, 'Queued Blur bid cancellation batch', {
            taskId,
            contractSlug,
            batchNumber: Math.floor(skip / BATCH_SIZE) + 1,
            ordersQueued: batch.length,
            totalOrdersQueued: totalOrdersProcessed,
            marketplace: 'blur',
            operation: 'blur_batch_cancellations_queued'
          });
        }

        // Check if we got a full batch
        if (batch.length < BATCH_SIZE) {
          hasMore = false;
        } else {
          skip += BATCH_SIZE;
        }

        // Clear batch from memory to prevent buildup
        allBlurBids = [];

      } catch (error) {
        await logger.logMarketplaceError('Failed to fetch Blur order batch', {
          taskId,
          contractSlug,
          batchNumber: Math.floor(skip / BATCH_SIZE) + 1,
          skip,
          errorMessage: error instanceof Error ? error.message : String(error),
          marketplace: 'blur'
        });
        hasMore = false; // Stop pagination on error
      }
    }

    await logger.logInfo(ErrorCategory.MARKETPLACE, 'Completed Blur order processing', {
      taskId,
      contractSlug,
      totalOrdersProcessed,
      totalBatches: Math.ceil(totalOrdersProcessed / BATCH_SIZE),
      marketplace: 'blur',
      operation: 'blur_order_processing_complete'
    });

    if (totalOrdersProcessed === 0) {
      await logger.logInfo(ErrorCategory.MARKETPLACE, 'No Blur bids found to cancel', {
        taskId,
        contractSlug,
        marketplace: 'blur'
      });
    }

    return { success: true, ordersFound: totalOrdersProcessed };
  } catch (error) {
    const errorMsg = error instanceof Error ? error.message : String(error);

    await logger.logMarketplaceError('Failed to fetch and cancel Blur bids', {
      taskId,
      contractSlug,
      marketplace: 'blur',
      errorMessage: errorMsg,
      stackTrace: error instanceof Error ? error.stack : undefined,
      operation: 'fetch_and_cancel_blur_bids_failed'
    });

    return { success: false, ordersFound: 0, error: errorMsg };
  }
}

/**
 * Fetches and cancels MagicEden bids for a specific task
 * @param task - The bidding task to cancel bids for
 * @param WALLET_PRIVATE_KEY - Private key for wallet authentication
 * @returns Promise with operation results
 */
async function fetchAndCancelMagicedenBids(
  task: ITask,
  WALLET_PRIVATE_KEY: string
): Promise<{ success: boolean; ordersFound: number; error?: string }> {
  const taskId = task?._id?.toString();
  const contractSlug = task?.contract?.slug;

  try {
    // Input validation
    if (!task || !taskId || !contractSlug) {
      const error = 'Invalid task provided to fetchAndCancelMagicedenBids';
      await logger.logWarning(ErrorCategory.MARKETPLACE, error, { task });
      return { success: false, ordersFound: 0, error };
    }

    if (!WALLET_PRIVATE_KEY) {
      const error = 'Invalid wallet private key provided';
      await logger.logWarning(ErrorCategory.MARKETPLACE, error, { taskId, contractSlug });
      return { success: false, ordersFound: 0, error };
    }

    await logger.logInfo(ErrorCategory.MARKETPLACE, 'Starting MagicEden bid cancellation', {
      taskId,
      contractSlug,
      marketplace: 'magiceden',
      operation: 'fetch_and_cancel_magiceden_bids'
    });

    // Implement pagination to handle large numbers of orders
    const BATCH_SIZE = 1000;
    let allMagicedenBids: any[] = [];
    let skip = 0;
    let hasMore = true;
    let totalOrdersProcessed = 0;

    await logger.logInfo(ErrorCategory.MARKETPLACE, 'Starting paginated MagicEden order fetch', {
      taskId,
      contractSlug,
      batchSize: BATCH_SIZE,
      marketplace: 'magiceden'
    });

    while (hasMore) {
      try {
        // Fetch batch of orders with timeout
        const batch = await Promise.race([
          Order.find({
            slug: contractSlug,
            taskId: taskId,
            marketplace: MAGICEDEN.toLowerCase()
          })
            .sort({ createdAt: -1 })
            .skip(skip)
            .limit(BATCH_SIZE),
          new Promise<never>((_, reject) => {
            setTimeout(() => reject(new Error('Database query timeout')), 10000);
          })
        ]);

        if (batch.length === 0) {
          hasMore = false;
          break;
        }

        // Add to total orders
        allMagicedenBids = allMagicedenBids.concat(batch);
        totalOrdersProcessed += batch.length;

        await logger.logDebug(ErrorCategory.MARKETPLACE, 'Fetched MagicEden order batch', {
          taskId,
          contractSlug,
          batchNumber: Math.floor(skip / BATCH_SIZE) + 1,
          ordersInBatch: batch.length,
          totalOrdersSoFar: totalOrdersProcessed,
          marketplace: 'magiceden'
        });

        // Process this batch immediately to avoid memory buildup
        if (batch.length > 0) {
          // Set up or update parent job tracking (MagicEden batches in groups of 500)
          const parentJobId = `${taskId}-CANCEL_MAGICEDEN_BID`;
          const batchJobCount = Math.ceil(batch.length / 500);
          const existingParentJob = parentJobs.get(parentJobId);

          if (!existingParentJob) {
            parentJobs.set(parentJobId, {
              parentId: parentJobId,
              totalJobs: batchJobCount,
              completedJobs: 0,
              failedJobs: 0,
              startTime: Date.now(),
              taskId: taskId,
              jobName: CANCEL_MAGICEDEN_BID,
              loopNumber: 0  // Cancellation jobs don't have loop numbers
            });
          } else {
            // Update total jobs count
            existingParentJob.totalJobs += batchJobCount;
          }

          // Queue cancellations for this batch
          await cancelMagicedenBids(batch, WALLET_PRIVATE_KEY, taskId);

          await logger.logInfo(ErrorCategory.MARKETPLACE, 'Queued MagicEden bid cancellation batch', {
            taskId,
            contractSlug,
            batchNumber: Math.floor(skip / BATCH_SIZE) + 1,
            ordersQueued: batch.length,
            totalOrdersQueued: totalOrdersProcessed,
            batchJobCount: batchJobCount,
            marketplace: 'magiceden',
            operation: 'magiceden_batch_cancellations_queued'
          });
        }

        // Check if we got a full batch
        if (batch.length < BATCH_SIZE) {
          hasMore = false;
        } else {
          skip += BATCH_SIZE;
        }

        // Clear batch from memory to prevent buildup
        allMagicedenBids = [];

      } catch (error) {
        await logger.logMarketplaceError('Failed to fetch MagicEden order batch', {
          taskId,
          contractSlug,
          batchNumber: Math.floor(skip / BATCH_SIZE) + 1,
          skip,
          errorMessage: error instanceof Error ? error.message : String(error),
          marketplace: 'magiceden'
        });
        hasMore = false; // Stop pagination on error
      }
    }

    await logger.logInfo(ErrorCategory.MARKETPLACE, 'Completed MagicEden order processing', {
      taskId,
      contractSlug,
      totalOrdersProcessed,
      totalBatches: Math.ceil(totalOrdersProcessed / BATCH_SIZE),
      marketplace: 'magiceden',
      operation: 'magiceden_order_processing_complete'
    });

    if (totalOrdersProcessed === 0) {
      await logger.logInfo(ErrorCategory.MARKETPLACE, 'No MagicEden bids found to cancel', {
        taskId,
        contractSlug,
        marketplace: 'magiceden'
      });
    }

    return { success: true, ordersFound: totalOrdersProcessed };
  } catch (error) {
    const errorMsg = error instanceof Error ? error.message : String(error);

    await logger.logMarketplaceError('Failed to fetch and cancel MagicEden bids', {
      taskId,
      contractSlug,
      marketplace: 'magiceden',
      errorMessage: errorMsg,
      stackTrace: error instanceof Error ? error.stack : undefined,
      operation: 'fetch_and_cancel_magiceden_bids_failed'
    });

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
  WALLET_PRIVATE_KEY: string,
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
      await logger.logWarning(ErrorCategory.MARKETPLACE, error, { task, marketplace, attempt });
      return { success: false, results: {}, totalOrdersFound: 0, errors: [error], duration: 0 };
    }

    if (!taskId) {
      const error = 'Task missing _id property';
      await logger.logWarning(ErrorCategory.MARKETPLACE, error, { task, marketplace, attempt });
      return { success: false, results: {}, totalOrdersFound: 0, errors: [error], duration: 0 };
    }

    if (!contractSlug) {
      const error = 'Task missing contract.slug property';
      await logger.logWarning(ErrorCategory.MARKETPLACE, error, { taskId, marketplace, attempt });
      return { success: false, results: {}, totalOrdersFound: 0, errors: [error], duration: 0 };
    }

    if (!WALLET_PRIVATE_KEY) {
      const error = 'Invalid wallet private key provided';
      await logger.logWarning(ErrorCategory.MARKETPLACE, error, { taskId, contractSlug, marketplace, attempt });
      return { success: false, results: {}, totalOrdersFound: 0, errors: [error], duration: 0 };
    }

    // Validate marketplace parameter if provided
    if (marketplace && !['opensea', 'magiceden', 'blur'].includes(marketplace.toLowerCase())) {
      const error = 'Invalid marketplace parameter';
      await logger.logWarning(ErrorCategory.MARKETPLACE, error, {
        taskId,
        contractSlug,
        marketplace,
        attempt,
        validMarketplaces: ['opensea', 'magiceden', 'blur']
      });
      return { success: false, results: {}, totalOrdersFound: 0, errors: [error], duration: 0 };
    }

    await logger.logInfo(ErrorCategory.MARKETPLACE, 'Starting bid cancellation', {
      taskId,
      contractSlug,
      marketplace,
      attempt,
      operation: 'cancel_all_related_bids'
    });

    // Process marketplace-specific cancellation
    if (marketplace?.toLowerCase()) {
      const parentJobId = `${taskId}-CANCEL_${marketplace.toUpperCase()}_BID`;
      // Don't delete parent job here - it's needed for tracking
      // parentJobs.delete(parentJobId);

      switch (marketplace.toLowerCase()) {
        case OPENSEA.toLowerCase():
          results.opensea = await fetchAndCancelOpenseaBids(task, WALLET_PRIVATE_KEY);
          totalOrdersFound += results.opensea.ordersFound;
          if (!results.opensea.success) {
            errors.push(`OpenSea cancellation failed: ${results.opensea.error}`);
          }
          break;
        case MAGICEDEN.toLowerCase():
          results.magiceden = await fetchAndCancelMagicedenBids(task, WALLET_PRIVATE_KEY);
          totalOrdersFound += results.magiceden.ordersFound;
          if (!results.magiceden.success) {
            errors.push(`MagicEden cancellation failed: ${results.magiceden.error}`);
          }
          break;
        case BLUR.toLowerCase():
          results.blur = await fetchAndCancelBlurBids(task, WALLET_PRIVATE_KEY);
          totalOrdersFound += results.blur.ordersFound;
          if (!results.blur.success) {
            errors.push(`Blur cancellation failed: ${results.blur.error}`);
          }
          break;
      }
    } else {
      // Process all marketplaces in parallel
      const openSeaParentJobId = `${taskId}-CANCEL_OPENSEA_BID`;
      const magicEdenParentJobId = `${taskId}-CANCEL_MAGICEDEN_BID`;
      const blurParentJobId = `${taskId}-CANCEL_BLUR_BID`;

      // Don't delete parent jobs here - they're needed for tracking
      // parentJobs.delete(openSeaParentJobId);
      // parentJobs.delete(magicEdenParentJobId);
      // parentJobs.delete(blurParentJobId);

      // Execute all marketplace cancellations in parallel with timeout
      const cancellationPromises = [
        fetchAndCancelOpenseaBids(task, WALLET_PRIVATE_KEY),
        fetchAndCancelMagicedenBids(task, WALLET_PRIVATE_KEY),
        fetchAndCancelBlurBids(task, WALLET_PRIVATE_KEY)
      ];

      const cancellationTimeout = 30000; // 30 seconds
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
        await logger.logMarketplaceError(errorMsg, {
          taskId,
          contractSlug,
          marketplace,
          attempt
        });
      }
    }

    const duration = Date.now() - startTime;
    const success = errors.length === 0;

    await logger.logInfo(ErrorCategory.MARKETPLACE, 'Completed bid cancellation', {
      taskId,
      contractSlug,
      marketplace,
      attempt,
      success,
      totalOrdersFound,
      errorsCount: errors.length,
      duration,
      results,
      operation: 'cancel_all_related_bids_complete'
    });

    return { success, results, totalOrdersFound, errors, duration };
  } catch (error) {
    const duration = Date.now() - startTime;
    const errorMsg = error instanceof Error ? error.message : String(error);

    await logger.logMarketplaceError('Failed to cancel all related bids', {
      taskId,
      contractSlug,
      marketplace,
      attempt,
      errorMessage: errorMsg,
      stackTrace: error instanceof Error ? error.stack : undefined,
      duration,
      operation: 'cancel_all_related_bids_failed'
    });

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
      // Check specific marketplace
      const remainingOrders = await Order.countDocuments({
        taskId: taskId,
        marketplace: marketplace.toLowerCase()
      });

      return {
        complete: remainingOrders === 0,
        remainingOrders
      };
    } else {
      // Check all marketplaces
      const [openSeaCount, magicEdenCount, blurCount] = await Promise.all([
        Order.countDocuments({ taskId, marketplace: 'opensea' }),
        Order.countDocuments({ taskId, marketplace: 'magiceden' }),
        Order.countDocuments({ taskId, marketplace: 'blur' })
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
    console.error(`Error verifying cancellation for task ${taskId}:`, error);
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
        console.error('Error parsing order hash:', e);
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

async function cancelMagicedenBids(orders: any[], privateKey: string, taskId: string) {
  if (!orders.length) return;

  // Set task status to CANCELLING
  if (taskStatus[taskId]) {
    taskStatus[taskId].status = 'CANCELLING';
  }

  // Parent job tracking removed - now using cancellationTracker

  console.log(MAGENTA + `[cancelMagicedenBids] Queueing ${orders.length} MagicEden cancellation orders for task ${taskId}` + RESET);

  for (let i = 0; i < orders.length; i += 500) {
    const batchOrders = orders.slice(i, i + 500);
    const extractedOrderIds = await extractMagicedenOrderHash(batchOrders)
    const orderKeys = batchOrders.map((order: any) => order._id)
    const batchId = `batch-${i / 500}-${Date.now()}`; // Unique batch identifier

    console.log(MAGENTA + `[cancelMagicedenBids] Adding batch ${i / 500 + 1} with ${batchOrders.length} orders` + RESET);

    await queue.add(CANCEL_MAGICEDEN_BID, {
      orderIds: extractedOrderIds,
      privateKey,
      orderKeys,
      taskId,
      batchSize: batchOrders.length // Add batch size to job data
    }, getCancellationJobOptions(taskId, batchId, CANCEL_MAGICEDEN_BID));
  }

  // Decrement the own bid counter by the number of cancelled orders
  decrementOwnBidCounter('magiceden', orders.length);

  console.log(MAGENTA + `[cancelMagicedenBids] Successfully queued all MagicEden cancellation jobs` + RESET);
}

async function cancelBlurBids(orders: any[], privateKey: string, taskId: string) {
  if (!orders.length) return

  // Set task status to CANCELLING
  if (taskStatus[taskId]) {
    taskStatus[taskId].status = 'CANCELLING';
  }

  // Parent job tracking removed - now using cancellationTracker

  console.log(BLUE + `[cancelBlurBids] Queueing ${orders.length} Blur cancellation orders for task ${taskId}` + RESET);

  const cancelData = orders.map((order, index) => {
    const payload = JSON.parse(order.orderHash)
    return {
      name: CANCEL_BLUR_BID,
      data: { privateKey, payload, taskId, orderKey: order._id },
      opts: getCancellationJobOptions(taskId, order._id, CANCEL_BLUR_BID)
    }
  }) as Job[]

  await processBulkJobs(cancelData);

  console.log(BLUE + `[cancelBlurBids] Successfully queued all Blur cancellation jobs` + RESET);
}

async function cancelOpenseaBids(orders: any[], privateKey: string, taskId: string, currencySymbol: string) {
  if (!orders.length) return;

  // Set task status to CANCELLING
  if (taskStatus[taskId]) {
    taskStatus[taskId].status = 'CANCELLING';
  }

  // Parent job tracking removed - now using cancellationTracker

  const cancelData = orders.map((order, index) => {
    return {
      name: CANCEL_OPENSEA_BID,
      data: { privateKey, orderId: order.orderHash, taskId, orderKey: order._id },
      opts: getCancellationJobOptions(taskId, order._id, CANCEL_OPENSEA_BID)
    }
  }) as Job[]

  // Track the job IDs we're about to create for reconciliation
  const expectedJobIds = cancelData.map(job => job.opts.jobId);
  console.log(GOLD + `[cancelOpenseaBids] Creating ${expectedJobIds.length} cancellation jobs for task ${taskId}` + RESET);

  // Store expected job count for reconciliation
  if (taskStatus[taskId]) {
    taskStatus[taskId].expectedCancellationJobs = (taskStatus[taskId].expectedCancellationJobs || 0) + expectedJobIds.length;
  }

  await processBulkJobs(cancelData)

  console.log(GOLD + `[cancelOpenseaBids] Successfully queued all OpenSea cancellation jobs` + RESET);

  // Decrement the own bid counter by the number of cancelled orders
  decrementOwnBidCounter('opensea', orders.length);
}

async function updateStatus(task: ITask) {
  try {
    const { _id: taskId, running } = task;
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
    console.error(RED + `Error updating status for task: ${task._id}` + RESET, error);
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
      console.log(RED + `Removing marketplace: ${outgoing.toUpperCase()} for collection: ${task.contract.slug}`.toUpperCase() + RESET);
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

      console.log(color + `Adding marketplace: ${incoming.toUpperCase()} for collection: ${task.contract.slug}`.toUpperCase() + RESET);

      // Use startTask to handle marketplace addition - it will create the necessary jobs
      // This prevents duplicate job creation
      try {
        await startTask(updatedTask, updatedTask.running, incoming.toLowerCase());
      } catch (error) {
        console.error(RED + `Error adding marketplace ${incoming} to task ${task._id}: ` + RESET, error);
      }
    }

  } catch (error) {
    console.error(RED + `Error updating marketplace for task: ${task._id}` + RESET, error);
  }
}

async function handleOutgoingMarketplace(marketplace: string, task: ITask) {
  try {
    await stopTask(task, false, marketplace);
  } catch (error) {
    console.error(RED + `Failed to handle outgoing marketplace ${marketplace} for task ${task.contract.slug}: ` + RESET, error);
  }
}

async function updateMultipleTasksStatus(data: { tasks: ITask[], running: boolean }) {
  try {
    const { tasks, running } = data;

    if (running) {
      // Process all tasks in parallel
      const startPromises = tasks.map(task =>
        (async () => {
          try {
            const result = await startTask(task, true);
            return { task: task.contract.slug, result };
          } catch (error) {
            console.error(RED + `Error starting task ${task.contract.slug}: ` + RESET, error);
            // Return error info instead of throwing to allow other tasks to continue
            return { task: task.contract.slug, error };
          }
        })()
      );

      const results = await Promise.all(startPromises);

      // Log summary of results
      const errors = results.filter(r => r?.error);
      const aborted = results.filter(r => r?.result && !r.result.success);
      const succeeded = results.filter(r => r?.result?.success);

      if (errors.length > 0 || aborted.length > 0) {
        console.log(YELLOW + `Started ${succeeded.length} tasks successfully, ${errors.length} failed, ${aborted.length} aborted` + RESET);
        if (aborted.length > 0) {
          const reasons = aborted.map(r => r.result?.reason || 'unknown').filter((v, i, a) => a.indexOf(v) === i);
          console.log(YELLOW + `Abort reasons: ${reasons.join(', ')}` + RESET);
        }
      } else {
        console.log(GREEN + `Successfully started all ${tasks.length} tasks` + RESET);
      }
    } else {
      // Process all tasks with coordination to prevent race conditions
      // Add delay between each task to avoid overwhelming the system
      const STOP_TASK_DELAY = 100; // 100ms between each task
      const stopPromises = tasks.map((task, index) =>
        (async () => {
          try {
            // Add staggered delay to prevent all tasks from hitting the system at once
            await new Promise(resolve => setTimeout(resolve, index * STOP_TASK_DELAY));

            await stopTask(task, false);
            return { task: task.contract.slug, success: true };
          } catch (error) {
            console.error(RED + `Error stopping task ${task.contract.slug}: ` + RESET, error);
            // Return error info instead of throwing to allow other tasks to continue
            return { task: task.contract.slug, error };
          }
        })()
      );

      const results = await Promise.all(stopPromises);

      // Log summary of results
      const errors = results.filter(r => r?.error);
      if (errors.length > 0) {
        console.log(YELLOW + `Stopped ${tasks.length - errors.length} tasks successfully, ${errors.length} failed` + RESET);
      } else {
        console.log(GREEN + `Successfully stopped all ${tasks.length} tasks` + RESET);
      }
    }
  } catch (error) {
    console.error(RED + 'Error updating multiple tasks status:' + RESET, error);
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
  console.log(CYAN + "=== WebSocket Connection Statistics ===" + RESET);
  console.log(`Total Connections: ${connectionMetrics.totalConnections}`);
  console.log(`Total Disconnections: ${connectionMetrics.totalDisconnections}`);
  console.log(`Average Connection Duration: ${formatDuration(connectionMetrics.averageConnectionDuration)}`);

  if (connectionMetrics.connectionStartTime) {
    const currentDuration = Date.now() - connectionMetrics.connectionStartTime;
    console.log(`Current Connection Duration: ${formatDuration(currentDuration)}`);
  }

  // Recent disconnection reasons
  const recentDisconnections = connectionHistory.slice(-10);
  const reasonCounts = recentDisconnections.reduce((acc, conn) => {
    acc[conn.reason] = (acc[conn.reason] || 0) + 1;
    return acc;
  }, {} as Record<string, number>);

  console.log(`Recent Disconnection Reasons:`, reasonCounts);
  console.log(CYAN + "=========================================" + RESET);
}

async function saveConnectionMetrics() {
  try {
    if (redis) {
      await redis.set('websocket:metrics', JSON.stringify(connectionMetrics));
      await redis.set('websocket:history', JSON.stringify(connectionHistory));
    }
  } catch (error) {
    console.error('Error saving connection metrics:', error);
  }
}

async function loadConnectionMetrics() {
  try {
    const metricsData = redis ? await redis.get('websocket:metrics') : null;
    const historyData = redis ? await redis.get('websocket:history') : null;

    if (metricsData) {
      const loadedMetrics = JSON.parse(metricsData);
      // Don't restore connectionStartTime as we want fresh connections
      connectionMetrics.totalConnections = loadedMetrics.totalConnections || 0;
      connectionMetrics.totalDisconnections = loadedMetrics.totalDisconnections || 0;
      connectionMetrics.averageConnectionDuration = loadedMetrics.averageConnectionDuration || 0;
      connectionMetrics.longestConnection = loadedMetrics.longestConnection || 0;
      connectionMetrics.shortestConnection = loadedMetrics.shortestConnection === Infinity ? Infinity : loadedMetrics.shortestConnection || Infinity;
    }
    if (historyData) {
      connectionHistory.push(...JSON.parse(historyData));
    }
  } catch (error) {
    console.error('Error loading connection metrics:', error);
  }
}

function startConnectionMonitoring() {
  setInterval(() => {
    if (connectionMetrics.connectionStartTime && wsManager?.isConnected()) {
      const currentDuration = Date.now() - connectionMetrics.connectionStartTime;

      // Log every 5 minutes
      if (currentDuration % (5 * 60 * 1000) < 10000) { // Within 10 seconds of 5-minute mark
        console.log(BLUE + `🔗 WebSocket uptime: ${formatDuration(currentDuration)}` + RESET);
      }

      // Log connection stats every hour
      if (currentDuration % (60 * 60 * 1000) < 10000) { // Within 10 seconds of hour mark
        logConnectionStats();
        saveConnectionMetrics(); // Save metrics every hour
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
    console.log(YELLOW + 'WebSocket connection already in progress, waiting...' + RESET);
    return connectionPromise;
  }

  // If already connected, just return
  if (wsManager?.isConnected()) {
    console.log(BLUE + 'WebSocket already connected' + RESET);
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
    console.log(GREEN + `Using deterministic client ID: ${clientId}` + RESET);
    console.log(GREEN + `Based on: ${process.env.API_KEY ? 'API_KEY' : process.env.CLIENT_ID ? 'CLIENT_ID' : 'default'}` + RESET);

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
      console.log(GOLD + `CONNECTED TO MARKETPLACE EVENTS WEBSOCKET (ID: ${connectionId}) at ${new Date().toISOString()}` + RESET);

      await logger.logInfo(ErrorCategory.WEBSOCKET, 'WebSocket connected to marketplace', {
        connectionId,
        url: MARKETPLACE_WS_URL,
        clientId,
        reconnectAttempts: retryCount
      });

      // Update connection metrics
      connectionMetrics.connectionStartTime = Date.now();
      connectionMetrics.currentConnectionId = connectionId;
      connectionMetrics.totalConnections++;
      retryCount = 0;

      // Clear activeSubscriptions to sync with server
      activeSubscriptions.clear();
      console.log(YELLOW + "Cleared active subscriptions for fresh reconnect" + RESET);

      // Subscribe only running tasks after connection
      const runningTasks = currentTasks.filter(task => task.running);
      if (runningTasks.length > 0) {
        subscribeToCollections(runningTasks as unknown as ITask[]);

        // Pre-warm caches for active tasks to improve counterbid performance
        runningTasks.forEach(async (task) => {
          if (task.wallet) {
            // Pre-load wallet data into cache
            walletCache.getWallet(task.wallet).catch(() => { });
          }
          if (task.contract?.contractAddress) {
            // Pre-load collection data into cache for each marketplace
            ['opensea', 'magiceden', 'blur'].forEach(async marketplace => {
              await collectionCache.getFloorPrice(task.contract.contractAddress, marketplace);
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

        console.log(YELLOW + `WebSocket disconnected after ${formatDuration(connectionMetrics.connectionDuration)} (Code: ${event.code}) - cleared subscription cache` + RESET);

        await logger.logInfo(ErrorCategory.WEBSOCKET, 'WebSocket disconnected', {
          connectionId: connectionMetrics.currentConnectionId || undefined,
          connectionDuration: connectionMetrics.connectionDuration,
          disconnectCode: event.code,
          disconnectReason: event.reason || getCloseReason(event.code),
          totalDisconnections: connectionMetrics.totalDisconnections
        });
      } else {
        console.log(YELLOW + "WebSocket closed - cleared subscription cache" + RESET);

        await logger.logInfo(ErrorCategory.WEBSOCKET, 'WebSocket closed unexpectedly', {
          disconnectCode: event.code,
          disconnectReason: event.reason
        });
      }

      // Clear subscription cache on disconnect
      activeSubscriptions.clear();
    });

    wsManager.on('message', async (message: any) => {
      try {

        // Handle initial data request synchronously
        if (message.endpoint === "get-initial-data") {
          const initialDataMessage = {
            type: 'bidRatesUpdate',
            data: clientData
          };
          wsManager.sendMessage(initialDataMessage);
          return;
        }
        // All messages use the queue
        pQueue.add(() => handleMarketplaceMessage(message))

      } catch (error) {
        console.error('Error in message handler:', error);
      }
    });

    wsManager.on('error', async (error: Error) => {
      await logger.logWebSocketError("WebSocket connection error", {
        error: error,
        connectionId: connectionMetrics.currentConnectionId || undefined,
        retryCount: retryCount,
        connectionDuration: connectionMetrics.connectionStartTime ? Date.now() - connectionMetrics.connectionStartTime : 0
      });
    });

    wsManager.on('server_status', (status: any) => {
      console.log(`Server Status Update: `, status);
      // Update any UI or metrics as needed
    });

    wsManager.on('max_retries_exceeded', async () => {
      await logger.logWebSocketError("WebSocket max reconnection attempts exceeded", {
        connectionId: connectionMetrics.currentConnectionId || undefined,
        retryCount: MAX_RETRIES,
        maxRetries: MAX_RETRIES
      });
    });
  }

  // Connect with error handling
  try {
    await wsManager.connect();
  } catch (error) {
    logger.logWebSocketError("Failed to initiate WebSocket connection", error as Error);
    // Don't rethrow - let the WebSocketManager handle reconnection
  }
}


/**
 * Unified handler for all marketplace messages
 * Handles both counterbid messages and listing price updates
 */
async function handleMarketplaceMessage(message: any) {
  try {
    // Debug logging for message inspection
    if (message.eventType === 'BID_CREATED') {
      return handleCounterBid(message)
    }

    else if (message.eventType === 'LISTING_CREATED') {
      // Handle listing price update for instant price matching
      return handleListingPriceUpdate(message);
    }

    else if (message.eventType === 'ITEM_SOLD') {
      return updateBalancesForItemSold(message);
    }

  } catch (error) {
    console.error("Error in handleMarketplaceMessage:", error);
  }
}


// Helper function to get balance for a specific wallet, contract, and marketplace
function getWalletContractBalance(walletAddress: string, contractAddress: string, marketplace: 'opensea' | 'magiceden' | 'blur'): number {
  const walletKey = walletAddress.toLowerCase();
  const contractKey = contractAddress.toLowerCase();

  return balances[walletKey]?.[contractKey]?.[marketplace] || 0;
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
      console.log(YELLOW + `Missing required fields in ITEM_SOLD event` + RESET);
      return;
    }

    // Create a unique key for deduplication including transaction hash if available
    const txHash = data.transactionHash || '';
    const eventKey = txHash ?
      `${contractAddress}:${tokenId}:${txHash}` :
      `${contractAddress}:${tokenId}:${marketplace}:${toAddress}:${price}:${data.createdAt || Date.now()}`;

    console.log(GOLD + JSON.stringify(data) + RESET);

    // Add to the set to prevent future duplicates
    recentItemSoldEvents.add(eventKey);

    // Check if the toAddress belongs to one of our wallets (case-insensitive)
    const wallet: IWallet | null = await walletCache.getWallet(toAddress);

    if (!wallet) {
      // Not one of our wallets, ignore
      console.log(YELLOW + `Wallet ${toAddress} not found in our system, skipping auto-listing` + RESET);
      return;
    }

    // Trigger auto-listing for the purchased item
    await triggerAutoListingForPurchase(wallet, contractAddress, tokenId, price, marketplace);

    const walletAddressKey = wallet.address.toLowerCase();

    console.log(GREEN + `💰 Item purchased by our wallet: ${wallet.address}` + RESET);
    console.log(GREEN + `   Contract: ${contractAddress}, Token: ${tokenId}, Marketplace: ${marketplace}` + RESET);

    // Initialize wallet balance structure if not exists
    if (!balances[walletAddressKey]) {
      balances[walletAddressKey] = {};
    }

    // Initialize contract balance structure if not exists
    if (!balances[walletAddressKey][contractAddress]) {
      balances[walletAddressKey][contractAddress] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      };
    }

    // Increment the balance for the specific marketplace
    balances[walletAddressKey][contractAddress][marketplace] =
      (balances[walletAddressKey][contractAddress][marketplace] || 0) + 1;

    console.log(CYAN + `📊 Updated balance for wallet ${wallet.address}:` + RESET);
    console.log(CYAN + `   Contract: ${contractAddress}` + RESET);
    console.log(CYAN + `   ${marketplace}: ${balances[walletAddressKey][contractAddress][marketplace]} items` + RESET);

    // Check for auto-listing tasks

  } catch (error) {
    console.error("Error updating balances for ITEM_SOLD:", error);
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
      console.error(RED + `Invalid wallet provided to triggerAutoListingForPurchase` + RESET);
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
      console.log(`No auto-listing tasks found for contract ${contractAddress} and wallet ${wallet.address}`);
      return;
    }

    console.log(GREEN + `🚀 Found ${autoListingTasks.length} auto-listing task(s) for purchased item` + RESET);

    // Process each auto-listing task
    for (const task of autoListingTasks) {
      const listingConfig = task.listingConfig!;

      console.log(GREEN + `Creating auto-listing for token ${tokenId} based on task ${task._id}` + RESET);

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
        console.log(`Using dynamic pricing based on buy price: ${price} + ${minimumProfit} = ${calculatedPrice}`);
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
          console.log(YELLOW + `Listing already exists for token ${tokenId}, skipping auto-listing` + RESET);
          continue;
        }

        // Create new listing
        const newListing = new Listing(listingData);
        await newListing.save();

        console.log(GREEN + `✅ Created auto-listing ${newListing._id} for token ${tokenId}` + RESET);

        // Schedule the listing for immediate processing
        await scheduleListing(newListing);

        await logger.logInfo(ErrorCategory.GENERAL,
          `Auto-listing triggered for purchased item`,
          {
            taskId: task._id.toString(),
            listingId: newListing._id.toString(),
            contractAddress,
            tokenId,
            purchasePrice: price,
            purchaseMarketplace: marketplace,
            wallet: wallet.address
          }
        );

      } catch (listingError) {
        console.error(RED + `Failed to create auto-listing for token ${tokenId}:` + RESET, listingError);
        await logger.logGeneralError('Auto-listing creation failed', {
          taskId: task._id.toString(),
          contractAddress,
          tokenId,
          error: listingError instanceof Error ? listingError : new Error(String(listingError))
        });
      }
    }

  } catch (error) {
    console.error("Error in triggerAutoListingForPurchase:", error);
    await logger.logGeneralError('Auto-listing trigger failed', {
      contractAddress,
      tokenId,
      wallet: wallet?.address || 'undefined',
      error: error instanceof Error ? error : new Error(String(error))
    });
  }
}

async function handleCounterBid(message: any) {
  try {

    const { contractAddress, slug } = getMarketplaceDetails(message);
    if (!contractAddress && !slug) return;
    // Use fast O(1) lookup instead of O(n) filter
    const relevantTasks = getRelevantTasks(contractAddress, slug);
    if (!relevantTasks.length) return;
    relevantTasks.map(task => handleCounterBidForTask(task, message))

  } catch (error) {
    console.error("Error in handleCounterBid:", error);
  }

}

function getMarketplaceDetails(message: any): { contractAddress?: string, slug?: string } {
  switch (message.marketplace) {
    case BLUR:
      return { contractAddress: handleBlurMessages(message) };
    case OPENSEA:
      return handleOpenSeaMessages(message);
    case MAGICEDEN:
      return handleMagicEdenMessages(message);
    default:
      console.log(`Unknown marketplace: ${message.marketplace}`);
      return {};
  }
}

async function handleCounterBidForTask(task: any, message: any) {
  const selectedMarketplaces = task.selectedMarketplaces.map((m: string) => m.toLowerCase());
  // Process each marketplace handler with error handling and timeout protection
  try {
    if (selectedMarketplaces.includes('blur') && message.marketplace === BLUR) {
      await handleBlurCounterbid(message['1'], task)
    }
    else if (selectedMarketplaces.includes('opensea') && message.marketplace === OPENSEA) {
      handleOpenseaCounterbid(message, task)
    }
    else if (selectedMarketplaces.includes('magiceden') && message.marketplace === MAGICEDEN) {
      handleMagicEdenCounterbid(message, task)
    }
  } catch (error: any) {
    console.log(`Counterbid error for task ${task._id}: ${error.message}`);
  }
}


async function handleMagicEdenCounterbid(data: any, task: ITask) {
  try {
    const currentTask = activeTasks.get(task._id)

    if (!currentTask || !currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("magiceden") || !enabledMarketplaces
      .map((m) => m.toLowerCase())
      .includes("magiceden")) {
      return;
    };

    const expiration = getExpiry(currentTask.bidDuration)
    const orderHash = data?.data?.id
    // Include task ID in the hash to make it task-specific
    const taskSpecificHash = orderHash;
    if (hasSeenItem(seenOrderHashes, taskSpecificHash)) return;
    addSeenItem(seenOrderHashes, taskSpecificHash)
    const maker = data?.data?.maker?.toLowerCase()

    // Parallel fetch wallet data and all wallets
    const [taskWallet, allWallets] = await Promise.all([
      walletCache.getWallet(task.wallet),
      walletCache.getAllWallets()
    ]);

    if (!taskWallet) {
      await logger.logError(ErrorCategory.DATABASE, `Wallet not found for task ${task._id}: ${task.wallet}`, new Error('Wallet not found'));
      return;
    }

    const walletAddresses = allWallets.map((wallet) => wallet.address.toLowerCase())

    // Extract bid data before wallet check
    const bidType = data?.data?.criteria?.kind
    const tokenId = +data?.data?.criteria?.data?.token?.tokenId;
    const usdPrice = Number(data?.data?.price?.amount?.usd)
    const eth_price = Number(data?.data?.price?.amount?.native)

    // Check if the maker is one of our wallets before decrypting private key
    if (walletAddresses.includes(maker)) {
      incrementOwnBidCounter('magiceden', task._id);
      return
    }

    if (!currentTask.outbidOptions.counterbid) {
      return;
    }
    // Comprehensive logging for MagicEden counterbid
    const makerFormatted = maker ? `${maker.slice(0, 6)}...${maker.slice(-4)}` : 'unknown';
    const bidTypeDisplay = bidType === 'token' ? `Token #${tokenId}` : 'Collection';
    console.log(`\x1b[35m[MagicEden Counterbid] Task: ${task._id} | Collection: ${task.contract.slug} | Type: ${bidTypeDisplay} | Incoming: ${eth_price.toFixed(4)} ETH ($${usdPrice.toFixed(2)}) | Maker: ${makerFormatted}\x1b[0m`);

    // Only decrypt private key after checking if it's our own bid
    const WALLET_PRIVATE_KEY = await walletCache.getDecryptedPrivateKey(taskWallet);
    if (!WALLET_PRIVATE_KEY) {
      await logger.logError(ErrorCategory.SECURITY, `Failed to decrypt private key for wallet ${task.wallet}`, new Error('Decryption failed'));
      return;
    }

    const { currencyType, currency, decimal, currencySymbol } = await calculateBidPrice(currentTask, 0, "magiceden")
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

    const selectedTraits = transformNewTask(task?.selectedTraits?.magiceden)
    const bestOffer = (bestOffers[task._id]?.magiceden || 0) * Math.pow(10, decimal);
    const collectionOffer = bidType === "collection" ? incomingBidAmount / Math.pow(10, decimal) : 0;

    if (collectionOffer > bestOffer) {
      bestOffers[task._id].magiceden = collectionOffer;
    }

    if (bidType === "token") {
      const currentTokens = currentTokenIds[task._id].magiceden
      const tokenIds = [...task.tokenIds.filter(id => !isNaN(Number(id))), ...currentTokens];
      const tokenBid = task.bidType === "token"

      if (!tokenBid || !tokenIds.includes(tokenId)) return

      // Check cache first
      let floor_price = await collectionCache.getFloorPrice(task.contract.contractAddress, 'magiceden');

      if (floor_price === null) {
        // Cache miss - fetch from API with deduplication
        floor_price = await collectionCache.fetchWithDedup(
          `magiceden-floor-${task.contract.contractAddress}`,
          async () => {
            const price = Number(await fetchMagicEdenCollectionStats(task.contract.contractAddress));
            await collectionCache.setFloorPrice(task.contract.contractAddress, 'magiceden', price);
            return price;
          }
        );
      }
      if (!floor_price) return

      const { maxBidPriceEth, minBidPriceEth } = await calculateBidPrice(currentTask, floor_price as number, "magiceden")
      const incomingBidAmountEth = incomingBidAmount / Math.pow(10, decimal)
      if (incomingBidAmountEth < minBidPriceEth || incomingBidAmountEth > maxBidPriceEth) {
        const reason = incomingBidAmountEth < minBidPriceEth
          ? `is less than min bid price ${minBidPriceEth} ${currencySymbol}`
          : `is greater than max bid price ${maxBidPriceEth} ${currencySymbol}`;
        const message = `incoming bid ${incomingBidAmountEth} ${currencySymbol} for ${task.contract.slug}: ${tokenId} ${reason}. Skipping ...`.toUpperCase()
        await logBidError(task._id, "INVALID BID PRICE", message, "skipped", "magiceden");
        return
      }

      const orders = await Order.find({
        taskId: task._id,
        marketplace: MAGICEDEN.toLowerCase(),
        identifier: tokenId.toString()
      })

      const stopBid = await stopOption(currentTask, 'magiceden', Number(floor_price), true)
      if (stopBid) {
        if (orders.length > 0) {
          const orderKeys = orders.map((order: any) => order._id)
          const extractedOrderIds = await extractMagicedenOrderHash(orders)
          queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey: WALLET_PRIVATE_KEY, orderKeys: orderKeys, taskId: task._id });
        }
        return
      }


      const offers = orders.map((order: any) => order.offerPrice)
      const currentBidPrice = !offers.length ? 0 : Math.max(...offers.map((offer) => Number(offer)))

      if (incomingBidAmount < currentBidPrice) return

      const roundedEth = roundToValidPrecision(incomingBidAmountEth)

      offerPrice = Math.ceil((magicedenOutbidMargin * Math.pow(10, decimal)) + roundedEth * Math.pow(10, decimal))
      const offerPriceEth = offerPrice / Math.pow(10, decimal)


      if (maxBidPriceEth > 0 && offerPriceEth > maxBidPriceEth) {
        const message = `magiceden counter offer ${offerPriceEth} ${currencySymbol} for ${task.contract.slug} ${tokenId} exceeds max bid price ${maxBidPriceEth} ${currencySymbol} ON MAGICEDEN.Skipping ...`.toUpperCase()
        await logBidError(task._id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "magiceden");


        if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
          if (orders.length > 0) {
            const orderKeys = orders.map((order: any) => order._id)
            const extractedOrderIds = await extractMagicedenOrderHash(orders)
            queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey: WALLET_PRIVATE_KEY, orderKeys: orderKeys, taskId: task._id });
          }
        }

        return;
      }

      const bidData: IMagicedenTokenBidData = {
        _id: task._id,
        address: task.wallet,
        contractAddress: task.contract.contractAddress,
        quantity: 1,
        offerPrice: Number(offerPrice),
        tokenId: tokenId,
        expiration: expiration,
        privateKey: WALLET_PRIVATE_KEY,
        slug: task.contract.slug,
        outbidOptions: task.outbidOptions,
        maxBidPriceEth,
        minBidPriceEth,
        task: currentTask,
      }

      await queue.add(
        MAGICEDEN_TOKEN_BID_COUNTERBID,
        bidData,
        { jobId: orderHash }
      );
      // Log successful counterbid preparation
      console.log(GREEN + '----------------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(GREEN + `[MagicEden Counterbid SUCCESS] Task: ${task._id} | Placing bid: ${offerPriceEth.toFixed(4)} ${currencySymbol} | Token: #${tokenId} | Outbidding: ${incomingBidAmountEth.toFixed(4)} ${currencySymbol}` + RESET);
      console.log(GREEN + '----------------------------------------------------------------------------------------------------------------------------------------------' + RESET);

      seenOrderHashes.delete(orderHash);

      if (orders.length > 0) {
        const extractedOrderIds = await extractMagicedenOrderHash(orders)
        const orderKeys = orders.map((order: any) => order._id)
        queue.add(CANCEL_MAGICEDEN_BID, {
          orderIds: extractedOrderIds,
          privateKey: WALLET_PRIVATE_KEY,
          orderKeys,
          taskId: task._id
        });
      }
    }
    else if (bidType === "attribute") {

      const traitBid = task.bidType === "collection" && selectedTraits && Object.keys(selectedTraits).length > 0
      const trait = {
        attributeKey: data?.data?.criteria?.data?.attribute?.key,
        attributeValue: data?.data?.criteria?.data?.attribute?.value,
      }

      const hasTraits = checkMagicEdenTrait(selectedTraits, trait)
      if (!hasTraits || !traitBid || !trait.attributeKey || !trait.attributeValue) return


      // Check cache first
      let floor_price = await collectionCache.getFloorPrice(task.contract.contractAddress, 'magiceden');

      if (floor_price === null) {
        // Cache miss - fetch from API with deduplication
        floor_price = await collectionCache.fetchWithDedup(
          `magiceden-floor-${task.contract.contractAddress}`,
          async () => {
            const price = Number(await fetchMagicEdenCollectionStats(task.contract.contractAddress));
            await collectionCache.setFloorPrice(task.contract.contractAddress, 'magiceden', price);
            return price;
          }
        );
      }
      if (!floor_price) return

      const identifier = `${trait.attributeKey}:${trait.attributeValue}`

      const { maxBidPriceEth, minBidPriceEth } = await calculateBidPrice(currentTask, floor_price as number, "magiceden")
      const incomingBidAmountEth = incomingBidAmount / Math.pow(10, decimal)
      if (incomingBidAmountEth < minBidPriceEth || incomingBidAmountEth > maxBidPriceEth) {
        const reason = incomingBidAmountEth < minBidPriceEth
          ? `is less than min bid price ${minBidPriceEth} ${currencySymbol}`
          : `is greater than max bid price ${maxBidPriceEth} ${currencySymbol}`;
        const message = `incoming bid ${incomingBidAmountEth} ${currencySymbol} for ${task.contract.slug}: ${identifier} ${reason}. Skipping ...`.toUpperCase()
        await logBidError(task._id, "INVALID BID PRICE", message, "skipped", "magiceden");
        return
      }

      const traitFloor = currentTask?.stopOptions?.traitPrices && currentTask?.stopOptions?.triggerStopOptions && identifier in currentTask?.stopOptions?.traitPrices ? await fetchMagicEdenTraitFloor(currentTask.contract.contractAddress, { attributeKey: trait.attributeKey, attributeValue: trait.attributeValue }) : undefined
      const orders = await Order.find({
        taskId: task._id,
        marketplace: MAGICEDEN.toLowerCase(),
        identifier
      })

      const stopBid = await stopOption(currentTask, 'magiceden', Number(floor_price), true, traitFloor, identifier)

      if (stopBid) {
        if (orders.length > 0) {
          const orderKeys = orders.map((order: any) => order._id)
          const extractedOrderIds = await extractMagicedenOrderHash(orders)
          queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey: WALLET_PRIVATE_KEY, orderKeys: orderKeys, taskId: task._id });
        }
        return
      }


      const offers = orders.map((order: any) => order.offerPrice)
      const currentBidPrice = !offers.length ? 0 : Math.max(...offers.map((offer) => Number(offer)))

      if (incomingBidAmount < currentBidPrice) return

      offerPrice = Math.ceil((magicedenOutbidMargin * Math.pow(10, decimal)) + incomingBidAmount)
      const offerPriceEth = offerPrice / Math.pow(10, decimal)

      // Log successful trait counterbid preparation
      const traitDisplay = trait ? JSON.stringify(trait) : 'Unknown trait';

      if (maxBidPriceEth > 0 && offerPriceEth > maxBidPriceEth) {
        const message = `magiceden counter offer ${offerPriceEth} ${currencySymbol} for ${task.contract.slug} ${JSON.stringify(trait)} exceeds max bid price ${maxBidPriceEth} ${currencySymbol} ON MAGICEDEN.Skipping ...`.toUpperCase()
        await logBidError(task._id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "magiceden");


        if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
          if (orders.length > 0) {
            const orderKeys = orders.map((order: any) => order._id)
            const extractedOrderIds = await extractMagicedenOrderHash(orders)
            queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey: WALLET_PRIVATE_KEY, orderKeys: orderKeys, taskId: task._id });
          }
        }
        return;
      }

      const bidData: IMagicedenTraitBidData = {
        taskId: task._id,
        bidCount: getIncrementedBidCount(MAGICEDEN, task.contract.slug, task._id),
        address: task.wallet,
        contractAddress: task.contract.contractAddress,
        quantity: 1,
        offerPrice: offerPrice,
        expiration: expiration,
        privateKey: WALLET_PRIVATE_KEY,
        slug: task.contract.slug,
        trait: JSON.stringify(trait),
      }

      await queue.add(
        MAGICEDEN_TRAIT_BID_COUNTERBID,
        bidData,
        { jobId: orderHash }
      );

      console.log(GREEN + '----------------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(GREEN + `[MagicEden Counterbid SUCCESS] Task: ${task._id} | Placing bid: ${offerPriceEth.toFixed(4)} ${currencySymbol} | Trait: ${traitDisplay} | Outbidding: ${incomingBidAmountEth.toFixed(4)} ${currencySymbol}` + RESET);
      console.log(GREEN + '----------------------------------------------------------------------------------------------------------------------------------------------' + RESET);

      seenOrderHashes.delete(orderHash);

      if (orders.length > 0) {
        const extractedOrderIds = await extractMagicedenOrderHash(orders)
        const orderKeys = orders.map((order: any) => order._id)
        queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey: WALLET_PRIVATE_KEY, orderKeys: orderKeys, taskId: task._id });
      }
    }
    else if (bidType === "collection") {
      const isTraitBid = task.bidType === "collection" && selectedTraits && Object.keys(selectedTraits).length > 0
      const tokenBid = task.bidType === "token"
      const collectionBid = task.bidType.toLowerCase() === "collection" && Object.keys(task?.selectedTraits?.magiceden || {}).length === 0

      if (!collectionBid) return


      // Check cache first
      let floor_price = await collectionCache.getFloorPrice(task.contract.contractAddress, 'magiceden');

      if (floor_price === null) {
        // Cache miss - fetch from API with deduplication
        floor_price = await collectionCache.fetchWithDedup(
          `magiceden-floor-${task.contract.contractAddress}`,
          async () => {
            const price = Number(await fetchMagicEdenCollectionStats(task.contract.contractAddress));
            await collectionCache.setFloorPrice(task.contract.contractAddress, 'magiceden', price);
            return price;
          }
        );
      }
      if (!floor_price) return

      const { maxBidPriceEth, minBidPriceEth } = await calculateBidPrice(currentTask, floor_price as number, "magiceden")
      const incomingBidAmountEth = incomingBidAmount / Math.pow(10, decimal)
      if (incomingBidAmountEth < minBidPriceEth || incomingBidAmountEth > maxBidPriceEth) {
        const reason = incomingBidAmountEth < minBidPriceEth
          ? `is less than min bid price ${minBidPriceEth} ${currencySymbol}`
          : `is greater than max bid price ${maxBidPriceEth} ${currencySymbol}`;
        const message = `incoming bid ${incomingBidAmountEth} ${currencySymbol} for ${task.contract.slug}: collection ${reason}. Skipping ...`.toUpperCase()
        await logBidError(task._id, "INVALID BID PRICE", message, "skipped", "magiceden");
        return
      }

      const stopBid = await stopOption(currentTask, 'magiceden', Number(floor_price), true)
      const orders = await Order.find({
        taskId: task._id,
        marketplace: MAGICEDEN.toLowerCase(),
        identifier: 'collection'
      })
      if (stopBid) {
        if (orders.length > 0) {
          const orderKeys = orders.map((order: any) => order._id)
          const extractedOrderIds = await extractMagicedenOrderHash(orders)
          queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey: WALLET_PRIVATE_KEY, orderKeys: orderKeys, taskId: task._id });
        }
        return
      }


      const offers = orders.map((order: any) => order.offerPrice)
      const currentBidPrice = !offers.length ? 0 : Math.max(...offers.map((offer) => Number(offer)))

      if (incomingBidAmount < currentBidPrice) return

      const roundedEth = roundToValidPrecision(incomingBidAmountEth)
      offerPrice = Math.ceil((magicedenOutbidMargin * Math.pow(10, decimal)) + (roundedEth * Math.pow(10, decimal)))
      const offerPriceEth = offerPrice / Math.pow(10, decimal)

      // Log successful collection counterbid preparation
      const traitDisplay = selectedTraits && Object.keys(selectedTraits).length > 0 ? ' (Trait bid)' : '';


      if (maxBidPriceEth > 0 && offerPriceEth > maxBidPriceEth) {
        const message = `counter Offer ${offerPriceEth} ${currencySymbol} for ${task.contract.slug}  exceeds max bid price ${maxBidPriceEth} ${currencySymbol}.Skipping ...`.toUpperCase()
        await logBidError(task._id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "magiceden");

        if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
          if (orders.length > 0) {
            const orderKeys = orders.map((order: any) => order._id)
            const extractedOrderIds = await extractMagicedenOrderHash(orders)
            queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey: WALLET_PRIVATE_KEY, orderKeys: orderKeys, taskId: task._id });
          }
        }
        return;
      }

      const bidCount = getIncrementedBidCount(MAGICEDEN, task.contract.slug, task._id)

      const bidData: IMagicedenCollectionBidData = {
        _id: task._id,
        bidCount,
        address: task.wallet,
        contractAddress: task.contract.contractAddress,
        quantity: 1,
        offerPrice: offerPrice,
        expiration: expiration,
        privateKey: WALLET_PRIVATE_KEY,
        slug: task.contract.slug,
        currency
      }

      await queue.add(
        MAGICEDEN_COLLECTION_BID_COUNTERBID,
        bidData,
        { jobId: orderHash }
      );

      console.log(GREEN + '----------------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(GREEN + `[MagicEden Counterbid SUCCESS] Task: ${task._id} | Placing bid: ${offerPriceEth.toFixed(4)} ${currencySymbol} | Collection${traitDisplay} | Outbidding: ${incomingBidAmountEth.toFixed(4)} ${currencySymbol}` + RESET);
      console.log(GREEN + '----------------------------------------------------------------------------------------------------------------------------------------------' + RESET);

      seenOrderHashes.delete(orderHash);


      if (orders.length > 0) {
        const orderKeys = orders.map((order: any) => order._id)
        const extractedOrderIds = await extractMagicedenOrderHash(orders)
        queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey: WALLET_PRIVATE_KEY, orderKeys: orderKeys, taskId: task._id });
      }

    }
  } catch (error) {
  }
}

async function handleOpenseaCounterbid(data: any, task: ITask) {
  try {
    const currentTask = activeTasks.get(task._id)

    if (!currentTask || !currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("opensea") || !enabledMarketplaces
      .map((m) => m.toLowerCase())
      .includes("opensea")) {
      return;
    }

    const expiry = getExpiry(currentTask.bidDuration)
    const outbidOptions = currentTask.outbidOptions

    const maker = data?.payload?.payload?.maker?.address.toLowerCase()
    const quantity = Number(data?.payload?.payload?.quantity) || 1
    const base_price: number = Number(data?.payload?.payload?.base_price) / quantity;
    const eth_price: number = Number(data?.payload?.payload?.payment_token?.eth_price)
    const symbol = data?.payload?.payload?.payment_token?.symbol?.toLowerCase()
    const paymentTokenDecimal = Number(data?.payload?.payload?.payment_token?.decimals)
    const usd_price = Number(data?.payload?.payload?.payment_token?.usd_price)
    const etherPrice = usd_price / eth_price

    let incomingBidAmount = 0
    const { currencySymbol, decimal, currencyType } = await calculateBidPrice(currentTask, 0, "opensea")
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

    const formattedBidAmout = base_price / Math.pow(10, paymentTokenDecimal)
    const bidDetails = `${formattedBidAmout} ${symbol.toUpperCase()}`
    const extraDetails = `${incomingBidAmount / Math.pow(10, decimal)} ${currencySymbol.toUpperCase()}`

    const tokenId = data.event === "item_received_bid" ? data.payload.payload.protocol_data.parameters.consideration.find((item: any) => item.token.toLowerCase() === task.contract.contractAddress.toLowerCase()).identifierOrCriteria : null

    const trait = data.event === "trait_offer" ? {
      type: data.payload.payload.trait_criteria.trait_type,
      value: data.payload.payload.trait_criteria.trait_name
    } : null

    const orderHash = data.payload.payload.order_hash

    // Fetch all wallets first to check if it's our own bid
    const wallets = await walletCache.getAllWallets()
    const walletAddresses = wallets.map((wallet) => wallet.address.toLowerCase())

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

    // Include task ID in the hash to make it task-specific
    const taskSpecificHash = orderHash
    if (hasSeenItem(seenOrderHashes, taskSpecificHash)) return;
    addSeenItem(seenOrderHashes, taskSpecificHash)

    if (walletAddresses.includes(maker)) {
      incrementOwnBidCounter('opensea', task._id);
      return
    }

    // Comprehensive logging for OpenSea counterbid
    const bidType = tokenId ? `Token #${tokenId}` : trait ? `Trait: ${trait.type}:${trait.value}` : 'Collection';
    console.log(BLUE + '----------------------------------------------------------------------------------------------------------------------------------------------' + RESET);
    console.log(BLUE + `[OpenSea Counterbid] Task: ${task._id} | Collection: ${task.contract.slug} | Type: ${bidType} | Incoming: ${bidDetails} (${extraDetails}) | Quantity: ${quantity} | Maker: ${maker}` + RESET);
    console.log(BLUE + '----------------------------------------------------------------------------------------------------------------------------------------------' + RESET);


    if (!outbidOptions.counterbid) {
      return;
    }
    // Only decrypt private key after checking if it's our own bid
    const WALLET_PRIVATE_KEY = decryptPrivateKey(wallets.find((wallet) => wallet.address.toLowerCase() === task.wallet.toLowerCase())?.encryptedPrivateKey as string);

    const incomingBidAmountEth = incomingBidAmount / Math.pow(10, decimal);
    // Check cache first
    let collectionDetails = await collectionCache.getCollectionDetails(task.contract.slug);

    if (!collectionDetails) {
      // Cache miss - fetch from API with deduplication
      collectionDetails = await collectionCache.fetchWithDedup(
        `collection-details-${task.contract.slug}`,
        async () => {
          const details = await getCollectionDetails(task.contract.slug, task.contract.contractAddress);
          await collectionCache.setCollectionDetails(task.contract.slug, details);
          return details;
        }
      );
    }

    const creatorFees: IFee = collectionDetails.creator_fees.null !== undefined
      ? { null: collectionDetails.creator_fees.null }
      : Object.fromEntries(Object.entries(collectionDetails.creator_fees).map(([key, value]) => [key, Number(value)]));

    let offerPrice: number;
    let colletionOffer: bigint;

    const selectedTraits = transformNewTask(currentTask?.selectedTraits?.opensea)
    const bestOffer = (bestOffers[task._id]?.opensea || 0);

    const collectionOffer = data.event === "collection_offer" ? incomingBidAmount / Math.pow(10, decimal) : 0

    if (collectionOffer > bestOffer) {
      bestOffers[task._id].opensea = collectionOffer;
    }

    if (data.event === "item_received_bid") {
      let tokenOffer: bigint;
      let offerPrice: number;

      const currentTokens = currentTokenIds[task._id].opensea
      const tokenIds = [...task.tokenIds, ...currentTokens]
      const tokenBid = task.bidType === "token"

      if (!tokenBid || !tokenIds.includes(+tokenId)) {
        return;
      }


      // Check cache first
      let floor_price = await collectionCache.getFloorPrice(task.contract.slug, 'opensea');

      if (floor_price === null) {
        // Cache miss - fetch from API with deduplication
        floor_price = await collectionCache.fetchWithDedup(
          `opensea-floor-${task.contract.slug}`,
          async () => {
            const price = await getOpenseaFloor(task.contract.slug);
            await collectionCache.setFloorPrice(task.contract.slug, 'opensea', price);
            return price;
          }
        );
      }
      if (!floor_price) return

      const { maxBidPriceEth, minBidPriceEth, currency } = await calculateBidPrice(currentTask, floor_price as number, "opensea")
      const orders = await Order.find({
        slug: task.contract.slug,
        taskId: task._id,
        marketplace: OPENSEA.toLowerCase(),
        identifier: tokenId.toString()
      })
      if (incomingBidAmountEth < minBidPriceEth) {
        const reason = `is less than min bid price ${minBidPriceEth} ${currencySymbol}`
        const message = `incoming bid ${bidDetails} ${similarCurrency ? '' : `(${extraDetails})`} for ${task.contract.slug}: ${tokenId} ${reason}. Skipping ...`.toUpperCase()
        await logBidError(task._id, "INVALID BID PRICE", message, "skipped", "opensea");
        return
      } else if (incomingBidAmountEth > maxBidPriceEth) {
        const reason = `is greater than max bid price ${maxBidPriceEth} ${currencySymbol}`;
        const message = `incoming bid ${bidDetails} ${similarCurrency ? '' : `(${extraDetails})`} for ${task.contract.slug}: ${tokenId} ${reason}. Skipping ...`.toUpperCase()
        await logBidError(task._id, "INVALID BID PRICE", message, "skipped", "opensea");

        if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
          if (orders.length > 0) {
            const cancelData = orders.map((order, index) => {
              return {
                name: CANCEL_OPENSEA_BID,
                data: { privateKey: WALLET_PRIVATE_KEY, orderId: order.orderHash, taskId: task._id, orderKey: order._id },

              }
            })
            await queue.addBulk(cancelData)
          }
        }

        return
      }
      const stopBid = await stopOption(currentTask, 'opensea', Number(floor_price), true)
      if (stopBid) {
        if (orders.length > 0) {
          const cancelData = orders.map((order, index) => {
            return {
              name: CANCEL_OPENSEA_BID,
              data: { privateKey: WALLET_PRIVATE_KEY, orderId: order.orderHash, taskId: task._id, orderKey: order._id },
            }
          })
          queue.addBulk(cancelData)
        }
        return
      }

      const offers = orders.map((order: any) => order.offerPrice)
      const currentBidPrice = offers.length > 0 ? Math.max(...offers.map((offer) => Number(offer))) : 0

      if (incomingBidAmount < currentBidPrice) return

      const openseaOutbidMargin = calculateOutbidMargin(incomingBidAmountEth, currency)
      const roundedEth = roundToValidPrecision(incomingBidAmountEth)

      offerPrice = Math.ceil((openseaOutbidMargin * Math.pow(10, decimal)) + (roundedEth * Math.pow(10, decimal)))
      const offerPriceEth = offerPrice / Math.pow(10, decimal)



      if (offerPriceEth > maxBidPriceEth) {
        const message = `counter offer ${offerPriceEth} ${currencySymbol} for ${task.contract.slug}:${tokenId}  exceeds max bid price ${maxBidPriceEth} ${currencySymbol} ON OPENSEA.Skipping ...`.toUpperCase()
        await logBidError(task._id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "opensea");
        if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
          if (orders.length > 0) {
            const cancelData = orders.map((order, index) => {
              return {
                name: CANCEL_OPENSEA_BID,
                data: { privateKey: WALLET_PRIVATE_KEY, orderId: order.orderHash, taskId: task._id, orderKey: order._id },

              }
            })
            queue.addBulk(cancelData)
          }
        }
        return;
      }
      tokenOffer = BigInt(offerPrice)

      const asset = {
        contractAddress: task.contract.contractAddress,
        tokenId: tokenId
      }

      const bidData: IProcessOpenseaTokenBidData = {
        _id: task._id,
        address: task.wallet,
        privateKey: WALLET_PRIVATE_KEY,
        slug: task.contract.slug,
        offerPrice: Number(tokenOffer),
        creatorFees,
        enforceCreatorFee: collectionDetails.enforceCreatorFee,
        asset,
        outbidOptions: outbidOptions,
        task
      }

      await queue.add(
        OPENSEA_TOKEN_BID_COUNTERBID,
        bidData,
        { jobId: orderHash }
      );

      // Log successful counterbid preparation
      console.log(GREEN + '----------------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(GREEN + `[OpenSea Counterbid SUCCESS] Task: ${task._id} | Placing bid: ${offerPriceEth.toFixed(4)} ${currencySymbol} | Token: #${tokenId} | Outbidding: ${incomingBidAmountEth.toFixed(4)} ${currencySymbol}` + RESET);
      console.log(GREEN + '----------------------------------------------------------------------------------------------------------------------------------------------' + RESET);

      seenOrderHashes.delete(orderHash);
    }

    if (data.event === "trait_offer") {
      if (!trait) return
      let traitOffer = BigInt(incomingBidAmount)
      const traitBid = task.bidType === "collection" && selectedTraits && Object.keys(selectedTraits).length > 0
      const hasTraits = checkOpenseaTrait(selectedTraits, data.payload.payload.trait_criteria)

      if (!hasTraits || !traitBid) return
      const identifier = `${trait.type}:${trait.value}`

      const traitFloor = currentTask?.stopOptions?.traitPrices && currentTask?.stopOptions?.triggerStopOptions && identifier in currentTask?.stopOptions?.traitPrices ? await openseaTraitFloor(currentTask.contract.slug, { traitType: trait.type, values: [trait.value] }) : undefined

      // Check cache first
      let floor_price = await collectionCache.getFloorPrice(task.contract.slug, 'opensea');

      if (floor_price === null) {
        // Cache miss - fetch from API with deduplication
        floor_price = await collectionCache.fetchWithDedup(
          `opensea-floor-${task.contract.slug}`,
          async () => {
            const price = await getOpenseaFloor(task.contract.slug);
            await collectionCache.setFloorPrice(task.contract.slug, 'opensea', price);
            return price;
          }
        );
      }
      if (!floor_price) return

      const { maxBidPriceEth, minBidPriceEth, currency } = await calculateBidPrice(currentTask, floor_price as number, "opensea")
      const incomingBidAmountEth = incomingBidAmount / Math.pow(10, decimal)

      const orders = await Order.find({
        slug: task.contract.slug,
        taskId: task._id,
        marketplace: OPENSEA.toLowerCase(),
        identifier
      })

      if (incomingBidAmountEth < minBidPriceEth) {
        const reason = `is less than min bid price ${minBidPriceEth} ${currencySymbol}`
        const message = `incoming bid ${bidDetails} ${similarCurrency ? '' : `(${extraDetails})`} for ${task.contract.slug}: ${identifier} ${reason}.Skipping ...`.toUpperCase()
        await logBidError(task._id, "INVALID BID PRICE", message, "skipped", "opensea");
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
                data: { privateKey: WALLET_PRIVATE_KEY, orderId: order.orderHash, taskId: task._id, orderKey: order._id },
              }
            })
            queue.addBulk(cancelData)
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
              data: { privateKey: WALLET_PRIVATE_KEY, orderId: order.orderHash, taskId: task._id, orderKey: order._id },
            }
          })
          queue.addBulk(cancelData)
        }
        return
      }

      const offers = orders.map((order: any) => order.offerPrice)
      const currentBidPrice = offers.length > 0 ? Math.max(...offers.map((offer) => Number(offer))) : 0

      if (incomingBidAmount < currentBidPrice) return

      const openseaOutbidMargin = calculateOutbidMargin(incomingBidAmountEth, currency)
      const roundedEth = roundToValidPrecision(incomingBidAmountEth)

      offerPrice = Math.ceil((openseaOutbidMargin * Math.pow(10, decimal)) + (roundedEth * Math.pow(10, decimal)))
      const offerPriceEth = offerPrice / Math.pow(10, decimal)

      if (maxBidPriceEth > 0 && offerPriceEth > maxBidPriceEth) {

        if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
          if (orders.length > 0) {
            const cancelData = orders.map((order, index) => {
              return {
                name: CANCEL_OPENSEA_BID,
                data: { privateKey: WALLET_PRIVATE_KEY, orderId: order.orderHash, taskId: task._id, orderKey: order._id },
              }
            })
            queue.addBulk(cancelData)
          }
        }
        return;
      }

      traitOffer = BigInt(offerPrice)
      const bidCount = getIncrementedBidCount(OPENSEA, task.contract.slug, task._id)

      const counterbidData: IProcessOpenseaTraitBidData = {
        _id: task._id,
        bidCount,
        address: task.wallet,
        privateKey: WALLET_PRIVATE_KEY,
        slug: task.contract.slug,
        offerPrice: Number(traitOffer),
        creatorFees,
        enforceCreatorFee: collectionDetails.enforceCreatorFee,
        expiry,
        trait: JSON.stringify(trait)
      }

      await queue.add(
        OPENSEA_TRAIT_BID_COUNTERBID,
        counterbidData,
        { jobId: orderHash }
      );

      // Log successful trait counterbid preparation
      console.log(GREEN + '----------------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(GREEN + `[OpenSea Counterbid SUCCESS] Task: ${task._id} | Placing bid: ${offerPriceEth.toFixed(4)} ${currencySymbol} | Trait: ${identifier} | Outbidding: ${incomingBidAmountEth.toFixed(4)} ${currencySymbol}` + RESET);
      console.log(GREEN + '----------------------------------------------------------------------------------------------------------------------------------------------' + RESET);

      seenOrderHashes.delete(orderHash);
    }
    if (data.event === "collection_offer") {
      const collectionBid = task.bidType.toLowerCase() === "collection" && Object.keys(task?.selectedTraits?.opensea || {}).length === 0

      if (!collectionBid) return


      // Check cache first
      let floor_price = await collectionCache.getFloorPrice(task.contract.slug, 'opensea');

      if (floor_price === null) {
        // Cache miss - fetch from API with deduplication
        floor_price = await collectionCache.fetchWithDedup(
          `opensea-floor-${task.contract.slug}`,
          async () => {
            const price = await getOpenseaFloor(task.contract.slug);
            await collectionCache.setFloorPrice(task.contract.slug, 'opensea', price);
            return price;
          }
        );
      }
      if (!floor_price) return

      const { maxBidPriceEth, minBidPriceEth, currency } = await calculateBidPrice(currentTask, floor_price as number, "opensea")
      const incomingBidAmountEth = incomingBidAmount / Math.pow(10, decimal)

      const orders = await Order.find({
        slug: task.contract.slug,
        taskId: task._id,
        marketplace: OPENSEA.toLowerCase(),
        identifier: 'collection'
      })

      if (incomingBidAmountEth < minBidPriceEth) {
        const reason = `is less than min bid price ${minBidPriceEth} ${currencySymbol}`
        const message = `incoming bid  ${bidDetails} ${similarCurrency ? '' : `(${extraDetails})`} for ${task.contract.slug}: collection ${reason}. Skipping ...`.toUpperCase()
        await logBidError(task._id, "INVALID BID PRICE", message, "skipped", "opensea");
        return
      } else if (incomingBidAmountEth > maxBidPriceEth) {
        const reason = `is greater than max bid price ${maxBidPriceEth} ${currencySymbol}`;
        const message = `incoming bid  ${bidDetails} ${similarCurrency ? '' : `(${extraDetails})`} for ${task.contract.slug}: collection ${reason}. Skipping ...`.toUpperCase()
        await logBidError(task._id, "INVALID BID PRICE", message, "skipped", "opensea");

        if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
          if (orders.length > 0) {
            const cancelData = orders.map((order, index) => {
              return {
                name: CANCEL_OPENSEA_BID,
                data: { privateKey: WALLET_PRIVATE_KEY, orderId: order.orderHash, taskId: task._id, orderKey: order._id },
              }
            })
            queue.addBulk(cancelData)
          }
        }

        return
      }

      const stopBid = await stopOption(currentTask, 'opensea', Number(floor_price), true)
      if (stopBid) {
        if (orders.length > 0) {
          const cancelData = orders.map((order, index) => {
            return {
              name: CANCEL_OPENSEA_BID,
              data: { privateKey: WALLET_PRIVATE_KEY, orderId: order.orderHash, taskId: task._id, orderKey: order._id },
            }
          })
          queue.addBulk(cancelData)
        }
        return
      }

      const offers = orders.map((order: any) => order.offerPrice)
      const currentBidPrice = offers.length > 0 ? Math.max(...offers.map((offer) => Number(offer))) : 0

      if (incomingBidAmount < currentBidPrice) return

      const openseaOutbidMargin = calculateOutbidMargin(incomingBidAmountEth, currency)
      const roundedEth = roundToValidPrecision(incomingBidAmountEth)

      offerPrice = Math.ceil((openseaOutbidMargin * Math.pow(10, decimal)) + (roundedEth * Math.pow(10, decimal)))
      const offerPriceEth = offerPrice / Math.pow(10, decimal)

      // Log successful collection counterbid preparation
      const traitDisplay = selectedTraits && Object.keys(selectedTraits).length > 0 ? ' (Trait bid)' : '';

      if (maxBidPriceEth > 0 && offerPriceEth > maxBidPriceEth) {
        const message = `counter Offer price ${offerPriceEth} ${currencySymbol} for ${task.contract.slug} exceeds max bid price ${maxBidPriceEth} ${currencySymbol} ON OPENSEA.Skipping ...`.toUpperCase()
        await logBidError(task._id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "opensea");

        if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
          if (orders.length > 0) {
            const cancelData = orders.map((order, index) => {
              return {
                name: CANCEL_OPENSEA_BID,
                data: { privateKey: WALLET_PRIVATE_KEY, orderId: order.orderHash, taskId: task._id, orderKey: order._id },
              }
            })
            queue.addBulk(cancelData)
          }
        }

        return;
      }
      colletionOffer = BigInt(offerPrice)
      const bidCount = getIncrementedBidCount(OPENSEA, task.contract.slug, task._id)

      const bidData: IOpenseaBidParams = {
        taskId: task._id,
        bidCount,
        walletAddress: task.wallet,
        walletPrivateKey: WALLET_PRIVATE_KEY,
        slug: task.contract.slug,
        offerPrice: Number(colletionOffer),
        creatorFees,
        enforceCreatorFee: collectionDetails.enforceCreatorFee,
        expiry
      }

      await queue.add(
        OPENSEA_COLLECTION_BID_COUNTERBID,
        bidData,
        { jobId: orderHash }
      );

      console.log(GREEN + '----------------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(GREEN + `[OpenSea Counterbid SUCCESS] Task: ${task._id} | Placing bid: ${offerPriceEth.toFixed(4)} ${currencySymbol} | Collection${traitDisplay} | Outbidding: ${incomingBidAmountEth.toFixed(4)} ${currencySymbol}` + RESET);
      console.log(GREEN + '----------------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      seenOrderHashes.delete(orderHash);
    }
    seenOrderHashes.delete(orderHash);
  } catch (error) {
    console.log("Error handling opensea counterbid: ", error);
  }
}

async function handleBlurCounterbid(data: any, task: ITask) {
  const startTime = Date.now();
  const incomingBid = data

  try {
    const bidHash = hashBid({ ...data, taskId: task._id });
    const currentTask = activeTasks.get(task._id)

    if (!currentTask || !currentTask?.running || !currentTask?.selectedMarketplaces?.map((marketplace) => marketplace?.toLowerCase())?.includes("blur") || !enabledMarketplaces?.map((marketplace) => marketplace?.toLowerCase())?.includes("blur")) {
      return;
    }
    const expiry = getExpiry(currentTask?.bidDuration)
    // Use cached wallet for better performance
    const wallets = await walletCache.getWallet(task.wallet);
    if (!wallets) {
      await logger.logError(ErrorCategory.DATABASE, `Wallet not found for task ${task._id}: ${task.wallet}`, new Error('Wallet not found'));
      return;
    }
    const WALLET_PRIVATE_KEY = await walletCache.getDecryptedPrivateKey(wallets);
    const selectedTraits = transformNewTask(currentTask?.selectedTraits?.blur)
    const traitBid = currentTask?.bidType === "collection" && selectedTraits && Object.keys(selectedTraits)?.length > 0
    const blurOutbidMargin = currentTask?.outbidOptions?.blurOutbidMargin || 0.01

    // Comprehensive logging for Blur counterbid
    const bidInfo = incomingBid?.stats?.[0] || incomingBid?.updates?.[0] || incomingBid;
    const price = bidInfo?.bestPrice || bidInfo?.price || 0;
    const criteriaType = bidInfo?.criteriaType || 'unknown';
    const criteriaValue = bidInfo?.criteriaValue || {};
    console.log(GOLD + '------------------------------------------------------------------------------------------------------------------------------------------------------------------' + RESET);
    console.log(GOLD + `[Blur Counterbid] Task: ${task._id} | Collection: ${task.contract.slug} | Type: ${criteriaType} | Incoming: ${price} ETH | Criteria: ${JSON.stringify(criteriaValue)}` + RESET);
    console.log(GOLD + '------------------------------------------------------------------------------------------------------------------------------------------------------------------' + RESET);

    if (!currentTask?.outbidOptions?.counterbid) {
      return;
    }
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

      if (!hasMatchingTraits?.length) return
      const transformedTraits = selectedTraits && Object.keys(selectedTraits)?.length > 0 ? transformOpenseaTraits(selectedTraits) : []

      for (const traitBid of hasMatchingTraits) {
        const trait = JSON.stringify(traitBid?.criteriaValue)
        const incomingPrice = Number(traitBid?.bestPrice || traitBid?.price || 0)
        const traits = JSON.stringify(traitBid?.criteriaValue)

        // Check cache first
        let floor_price = await collectionCache.getFloorPrice(task?.contract?.slug, 'blur');

        if (floor_price === null) {
          // Cache miss - fetch from API with deduplication
          floor_price = await collectionCache.fetchWithDedup(
            `blur-floor-${task?.contract?.slug}`,
            async () => {
              const price = await fetchBlurCollectionStats(task?.contract?.slug);
              await collectionCache.setFloorPrice(task?.contract?.slug, 'blur', price);
              return price;
            }
          );
        }
        const { maxBidPriceEth, minBidPriceEth } = await calculateBidPrice(task, Number(floor_price), "blur")
        const traitsObj = JSON.parse(trait);
        const [traitType, traitValue] = Object.entries(traitsObj)[0] as string[]
        const identifier = `${traitType}:${traitValue}`

        const incomingTrait = { type: traitType, value: traitValue }
        const activeTrait = transformedTraits?.some(function (trait) {
          return trait?.type?.toLowerCase() === incomingTrait?.type?.toLowerCase() && trait?.value?.toLowerCase() === incomingTrait?.value?.toLowerCase();
        });

        if (!activeTrait) return

        const totalOffer = incomingPrice + blurOutbidMargin

        const orders = await Order.find({
          slug: task.contract.slug,
          taskId: task._id,
          marketplace: BLUR.toLowerCase(),
          identifier: identifier
        })

        if (+totalOffer < minBidPriceEth) {
          const reason = `is less than min bid price ${minBidPriceEth} BETH`
          const message = `incoming bid ${incomingPrice} + Blur Outbid Margin(${blurOutbidMargin}) BETH for ${task.contract.slug}: ${identifier} ${reason}. Skipping ...`.toUpperCase()
          await logBidError(task._id, "INVALID BID PRICE", message, "skipped", "blur");
          return
        } else if (+totalOffer > maxBidPriceEth) {
          const reason = `is greater than max bid price ${maxBidPriceEth} BETH`
          const message = `incoming bid ${incomingPrice} + Blur Outbid Margin(${blurOutbidMargin}) BETH for ${task.contract.slug}: ${identifier} ${reason}. Skipping ...`.toUpperCase()

          if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
            if (orders.length > 0) {
              const cancelData = orders.map((order, index) => {
                const payload = JSON.parse(order.orderHash)
                return {
                  name: CANCEL_BLUR_BID,
                  data: { privateKey: WALLET_PRIVATE_KEY, payload, taskId: task._id, orderKey: order._id }
                }
              })
              await queue.addBulk(cancelData)
            }
          }

          await logBidError(task._id, "INVALID BID PRICE", message, "skipped", "blur");
          return
        }

        const traitFloor = currentTask?.stopOptions?.traitPrices && currentTask?.stopOptions?.triggerStopOptions && identifier in currentTask?.stopOptions?.traitPrices ? await fetchBlurTraitFloor(currentTask?.contract?.slug, { type: traitType, value: traitValue as string }) : undefined

        const stopBid = await stopOption(currentTask, 'blur', Number(floor_price), true, traitFloor, identifier)

        if (stopBid) {
          if (orders.length > 0) {
            console.log(YELLOW + `[DEBUG][handleBlurCounterbid] Stop condition met, cancelling ${orders.length} bids` + RESET);
            const cancelData = orders.map((order, index) => {
              const payload = JSON.parse(order.orderHash)
              return {
                name: CANCEL_BLUR_BID,
                data: { privateKey: WALLET_PRIVATE_KEY, payload, taskId: task._id, orderKey: order._id }
              }
            })
            await queue.addBulk(cancelData)
          }
          return
        }

        const offers = orders?.map((order: any) => order?.offerPrice)
        const currentBidPrice = !offers?.length ? 0 : Math.max(...offers?.map((offer) => Number(offer)))
        const currentBidPriceEth = currentBidPrice / 1e18
        if (incomingPrice <= currentBidPriceEth) return

        let offerPrice: any = Number((blurOutbidMargin + Number(incomingPrice))?.toFixed(2))
        offerPrice = parseEther(offerPrice?.toString())

        const bidCount = getIncrementedBidCount(BLUR, task.contract.slug, task._id)

        const bidData: IBlurBidData = {
          taskId: task._id,
          bidCount: bidCount,
          address: task.wallet,
          privateKey: WALLET_PRIVATE_KEY || '',
          contractAddress: task.contract.contractAddress,
          offerPrice: Number(offerPrice),
          slug: task.contract.slug,
          traits,
          expiry
        }


        await blurQueue.add(
          BLUR_TRAIT_BID_COUNTERBID,
          bidData,
          { jobId: bidHash }
        );

        // Log successful trait counterbid preparation
        console.log(GREEN + '------------------------------------------------------------------------------------------------------------------------------------------------------------------' + RESET);
        console.log(GREEN + `[Blur Counterbid SUCCESS] Task: ${task._id} | Placing bid: ${Number(offerPrice) / 1e18} ETH | Trait: ${identifier} | Outbidding: ${incomingPrice} ETH` + RESET);
        console.log(GREEN + '------------------------------------------------------------------------------------------------------------------------------------------------------------------' + RESET);

        // Track completion time
        const completionTime = Date.now() - startTime;
        console.log(CYAN + `[DEBUG][handleBlurCounterbid] COMPLETED successfully in ${completionTime}ms` + RESET);
      }

    } else {
      console.log(CYAN + `[DEBUG][handleBlurCounterbid] Processing collection bid` + RESET);
      const collectionBid = task.bidType.toLowerCase() === "collection" && Object.keys(task?.selectedTraits?.blur || {}).length === 0
      if (!collectionBid) {
        console.log(YELLOW + `[DEBUG][handleBlurCounterbid] Not a collection bid, exiting` + RESET);
        return;
      }

      const incomingBidDetails = incomingBid?.stats?.filter((item: any) => item?.criteriaType?.toLowerCase() === "collection") || incomingBid?.updates?.filter((item: any) => item?.criteriaType?.toLowerCase() === "collection")

      if (!incomingBidDetails || !incomingBidDetails?.length) return

      const incomingBestPrice = incomingBidDetails?.sort((a: any, b: any) => +b?.bestPrice - +a?.bestPrice)[0].bestPrice
      const incomingBidAmount = (incomingBidDetails as any)?.sort((a: any, b: any) => +b.price - +a.price)[0].price

      const bestPrice = +incomingBestPrice > 0 ? incomingBestPrice : +incomingBidAmount > 0 ? incomingBidAmount : "0"


      // Check cache first
      let floor_price = await collectionCache.getFloorPrice(task.contract.slug, 'blur');

      if (floor_price === null) {
        // Cache miss - fetch from API with deduplication
        floor_price = await collectionCache.fetchWithDedup(
          `blur-floor-${task.contract.slug}`,
          async () => {
            const price = await fetchBlurCollectionStats(task.contract.slug);
            await collectionCache.setFloorPrice(task.contract.slug, 'blur', price);
            return price;
          }
        );
      }
      const { maxBidPriceEth, minBidPriceEth } = await calculateBidPrice(task, Number(floor_price), "blur")

      // Wallet already fetched above, no need to fetch again

      const orders = await Order.find({
        slug: task.contract.slug,
        taskId: task._id,
        marketplace: BLUR.toLowerCase(),
        identifier: 'collection'
      }).sort({ createdAt: -1 })

      if (+bestPrice < minBidPriceEth) {
        const reason = `is less than min bid price ${minBidPriceEth} BETH`
        const message = `incoming bid ${bestPrice} BETH for ${task.contract.slug}: collection ${reason}. Skipping ...`.toUpperCase()
        await logBidError(task._id, "INVALID BID PRICE", message, "skipped", "blur");
        return
      } else if (+bestPrice > maxBidPriceEth) {
        const reason = `is greater than max bid price ${maxBidPriceEth} BETH`;
        const message = `incoming bid ${bestPrice} BETH for ${task.contract.slug}: collection ${reason}. Skipping ...`.toUpperCase()
        await logBidError(task._id, "INVALID BID PRICE", message, "skipped", "blur");

        if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
          console.log(YELLOW + `[DEBUG][handleBlurCounterbid] Collection: Cancelling ${orders.length} existing bids due to max price exceeded` + RESET);
          const cancelData = orders.map((order) => {
            const payload = JSON.parse(order.orderHash)
            return {
              name: CANCEL_BLUR_BID,
              data: {
                privateKey: WALLET_PRIVATE_KEY,
                payload,
                taskId: task._id,
                orderKey: order._id
              }
            }
          })
          await queue.addBulk(cancelData);
          console.log(GREEN + `[DEBUG][handleBlurCounterbid] Collection cancel jobs added` + RESET);
        }
        return
      }

      const stopBid = await stopOption(currentTask, 'blur', Number(floor_price), true)
      if (stopBid) {
        if (orders.length > 0) {
          console.log(YELLOW + `[DEBUG][handleBlurCounterbid] Collection: Stop condition met, cancelling ${orders.length} bids` + RESET);
          const cancelData = orders.map((order) => {
            const payload = JSON.parse(order.orderHash)
            return {
              name: CANCEL_BLUR_BID,
              data: {
                privateKey: WALLET_PRIVATE_KEY,
                payload,
                taskId: task._id,
                orderKey: order._id
              }
            }
          })
          await queue.addBulk(cancelData);
          console.log(GREEN + `[DEBUG][handleBlurCounterbid] Collection stop condition cancel jobs added` + RESET);
        }
        return
      }
      const incomingPrice = Number(bestPrice) * 1e18
      const offers = orders?.map((order: any) => order?.offerPrice)
      const currentBidPrice = !offers?.length ? 0 : Math.max(...offers.map((offer) => Number(offer)))
      if (incomingPrice <= currentBidPrice) return

      const blurOutbidMargin = currentTask?.outbidOptions?.blurOutbidMargin || 0.01
      const expiry = getExpiry(currentTask?.bidDuration)
      const rawPrice = (+bestPrice + Number(blurOutbidMargin.toFixed(2))) * 1e18
      let offerPrice = BigInt(Math.round(rawPrice / 1e16) * 1e16)

      // Log successful collection counterbid preparation
      const traitDisplay = selectedTraits && Object.keys(selectedTraits).length > 0 ? ' (Trait bid)' : '';

      const bidCount = getIncrementedBidCount(BLUR, task.contract.slug, task._id)
      const bidData: IBlurBidData = {
        taskId: task._id,
        bidCount: bidCount,
        address: task.wallet,
        privateKey: WALLET_PRIVATE_KEY || '',
        contractAddress: task.contract.contractAddress,
        offerPrice: Number(offerPrice),
        slug: task.contract.slug,
        expiry
      }

      await blurQueue.add(
        BLUR_COLLECTION_BID_COUNTERBID,
        bidData,
        { jobId: bidHash }
      );

      console.log(GREEN + '------------------------------------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(GREEN + `[Blur Counterbid SUCCESS] Task: ${task._id} | Placing bid: ${Number(offerPrice) / 1e18} ETH | Collection${traitDisplay} | Outbidding: ${bestPrice} ETH` + RESET);
      console.log(GREEN + '------------------------------------------------------------------------------------------------------------------------------------------------------------------' + RESET);
    }

    // Log completion time
    const endTime = Date.now();
    console.log(GREEN + `[DEBUG][handleBlurCounterbid] COMPLETED successfully in ${endTime - startTime}ms` + RESET);

  } catch (error: any) {
    const endTime = Date.now();
    console.log(RED + `[DEBUG][handleBlurCounterbid] ERROR after ${endTime - startTime}ms: ${error?.message || error?.toString() || 'Unknown error'}` + RESET);
    if (error?.stack) {
      console.log(RED + `[DEBUG][handleBlurCounterbid] Stack trace: ${error.stack}` + RESET);
    }
    // Log specific error details
    if (error?.response?.data) {
      console.log(RED + `[DEBUG][handleBlurCounterbid] API Response:`, JSON.stringify(error.response.data) + RESET);
    }
  } finally {
    // Redis cache handles cleanup automatically with TTL
    const duration = Date.now() - startTime;
    console.log(CYAN + `[DEBUG][handleBlurCounterbid] COMPLETED in ${duration}ms` + RESET);
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
    }
  } catch (error) {
    console.error("Error parsing MagicEden message:", error);
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
    }
  } catch (err) {
    console.error("Error parsing OpenSea message:", err);
  }
  return { contractAddress, slug }
}



function handleBlurMessages(message: any) {
  let contractAddress: string | undefined;
  try {
    if (message && message['1'] && message['1'].contractAddress) {
      contractAddress = message['1'].contractAddress;
    }
  } catch (error) {
    console.error(`Failed to parse Blur message: `, error);
  }
  return contractAddress
}

function hashBid(data: any): string {
  const stableString = stringify(data) as string;
  return crypto.createHash('sha256')
    .update(stableString, 'utf8')
    .digest('hex');
}

function calculateOutbidMargin(currentPrice: number, currency = WETH): number {
  if (currency === USDC) return 0.1
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

export const pQueue = new PQueue({
  concurrency: RATE_LIMIT,
})

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
  availableMarketplaces: string[]
): Promise<TaskPreprocessing> {
  const taskId = task._id;


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

  // Track whether API was called and returned empty
  let openseaApiCalled = false;
  let openseaApiReturnedEmpty = false;
  let magicedenApiCalled = false;
  let magicedenApiReturnedEmpty = false;

  // Extract manual token IDs (needed for error checking)
  const manualTokenIds = task.tokenIds.filter(id => !isNaN(Number(id)) && !id.toString().toLowerCase().includes('bot')).map(id => id.toString());

  if (task.bidType === "token") {
    // First, fetch bottom listings if autoIds are present
    if (amount) {
      const [openseaListings, magicedenListings] = await Promise.all([
        marketplace === "opensea" && availableMarketplaces.includes("opensea") ?
          fetchOpenseaListings(taskId, task.contract.slug, amount)
            .then(result => {
              openseaApiCalled = true;
              openseaApiReturnedEmpty = result ? result.length === 0 : true;
              return result || [];
            })
            .catch(() => []) :
          Promise.resolve([]),
        marketplace === "magiceden" && availableMarketplaces.includes("magiceden") ?
          fetchMagicEdenTokens(taskId, task.contract.contractAddress, amount)
            .then(result => {
              magicedenApiCalled = true;
              magicedenApiReturnedEmpty = result ? result.length === 0 : true;
              return result || [];
            })
            .catch(() => []) :
          Promise.resolve([])
      ]);

      openseaTokens = [...(openseaListings || [])];
      magicedenTokens = [...(magicedenListings || [])];
    }

    // Add manually specified token IDs
    openseaTokens = [...openseaTokens, ...manualTokenIds];
    magicedenTokens = [...magicedenTokens, ...manualTokenIds.map(id => Number(id))];

    // If traits are selected for token bid type, fetch trait-based tokens
    // Fetch once and share between OpenSea and MagicEden
    console.log(CYAN + `[DEBUG] Task bidType: ${task.bidType}, marketplace: ${marketplace}` + RESET);
    if ((marketplace === "opensea" || marketplace === "magiceden") && availableMarketplaces.includes("opensea")) {
      const openseaFilteredTraits = Object.fromEntries(
        Object.entries(task?.selectedTraits?.opensea || {}).filter(([_, traits]) => traits.length > 0)
      );

      if (Object.keys(openseaFilteredTraits).length > 0) {
        const selectedTraits = transformNewTask(openseaFilteredTraits);
        const attributes = transformSelectedTraitsToAttributes(selectedTraits);
        const expandedAttributes = expandAttributes(attributes);

        // Set task status to PROCESSING when starting trait token fetching
        setTaskStatus(taskId, "PROCESSING", "fetching trait tokens");

        if (task.traitsOperator.value === "or") {
          await pQueue.addAll(
            expandedAttributes.map((attribute) => async () => {
              const variable: CollectionItemsQueryVariables = {
                collectionSlug: task.contract.slug,
                filter: { attributes: [attribute] },
                limit: 100,
                sort: { by: 'PRICE', direction: 'ASC' },
              };
              try {
                // Calculate expected count for this trait
                const traitCount = getTraitCount(task.traits, attribute);
                const expectedCount = typeof traitCount === 'number' ? traitCount : 0;

                const items = await fetchOpenSeaCollectionItems(variable, [], undefined, taskId, expectedCount);
                if (items.length === 0) {
                  console.log(YELLOW + `[WARN] No items returned for ${task.contract.slug} with traits. Collection may not support GraphQL.` + RESET);
                } else if (items.length >= 2000) {
                  console.log(YELLOW + `[INFO] Retrieved ${items.length} items for ${task.contract.slug} with traits (API limit may apply)` + RESET);
                }
                traitTokens.push(...items);
              } catch (error) {
                console.error(RED + `[ERROR] Failed to fetch trait items for ${task.contract.slug}: ${error}` + RESET);
                // Continue without trait tokens rather than failing completely
              }
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

            const items = await fetchOpenSeaCollectionItems(variable, [], undefined, taskId, expectedCount);
            if (items.length === 0) {
              console.log(YELLOW + `[WARN] No items returned for ${task.contract.slug} with traits. Collection may not support GraphQL.` + RESET);
            } else if (items.length >= 2000) {
              console.log(YELLOW + `[INFO] Retrieved ${items.length} items for ${task.contract.slug} with traits (API limit may apply)` + RESET);
            }
            traitTokens.push(...items);
          } catch (error) {
            console.error(RED + `[ERROR] Failed to fetch trait items for ${task.contract.slug}: ${error}` + RESET);
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
          skipStats[taskId].opensea = (skipStats[taskId].opensea || 0) + missingTraitTokenCount;
          skipStats[taskId].magiceden = (skipStats[taskId].magiceden || 0) + missingTraitTokenCount;

          console.log(YELLOW + `[INFO] Missing ${missingTraitTokenCount} trait tokens for ${task.contract.slug}` + RESET);
          console.log(YELLOW + `[DEBUG] Updated skipStats[${taskId}]: opensea=${skipStats[taskId].opensea}, magiceden=${skipStats[taskId].magiceden}` + RESET);
        }

        // Add trait tokens to both OpenSea and MagicEden lists
        openseaTokens = [...openseaTokens, ...traitTokens];
        magicedenTokens = [...magicedenTokens, ...traitTokens.map(id => Number(id))];

        if (traitTokens.length > 0) {
          console.log(GREEN + `[DEBUG] Added ${traitTokens.length} trait tokens for ${task.contract.slug}` + RESET);
        }

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

    if (task.bidType === "token") {
      console.log(CYAN + `[DEBUG] OpenSea tokens available: ${openseaTokens.length}` + RESET);
      if (openseaTokens.length > 0) {
        // Track duplicates before deduplication
        const originalCount = openseaTokens.length;
        preprocessing.opensea.tokens = [...new Set(openseaTokens)];
        preprocessing.opensea.jobCount = preprocessing.opensea.tokens.length;

        // Log duplicates if any were removed
        const duplicateCount = originalCount - preprocessing.opensea.tokens.length;
        if (duplicateCount > 0) {
          // Find which tokens were duplicates
          const tokenCounts = new Map<string, number>();
          openseaTokens.forEach(token => {
            tokenCounts.set(token, (tokenCounts.get(token) || 0) + 1);
          });

          const duplicateTokens: string[] = [];
          tokenCounts.forEach((count, token) => {
            if (count > 1) {
              duplicateTokens.push(`${token} (${count} times)`);
            }
          });

          // Log the duplicate tokens
          await logBidError(
            taskId,
            "Duplicate tokens removed",
            `Removed ${duplicateCount} duplicate tokens from OpenSea: ${duplicateTokens.join(", ")}`,
            "skipped",
            "opensea"
          );

          // Update skipStats
          if (!skipStats[taskId]) {
            skipStats[taskId] = { opensea: 0, magiceden: 0, blur: 0 };
          }
          skipStats[taskId].opensea = (skipStats[taskId].opensea || 0) + duplicateCount;

          console.log(YELLOW + `[INFO] Removed ${duplicateCount} duplicate tokens for OpenSea` + RESET);
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

            skipStats[taskId].opensea = (skipStats[taskId].opensea || 0) + additionalMissing;
            console.log(YELLOW + `[INFO] Total missing ${totalMissing} tokens to reach target amount of ${amount} for OpenSea` + RESET);
          }
        }
      } else {
        // Only throw error if API was called and returned empty (scenario 6)
        if (openseaApiCalled && openseaApiReturnedEmpty && !manualTokenIds.length && !traitTokens.length) {
          console.log(RED + `[ERROR] OpenSea API returned no listings for ${task.contract.slug}` + RESET);
          throw new Error(`No listings found on OpenSea for ${task.contract.slug}. Collection may have no active listings.`);
        }
        // Otherwise, just skip without error
        preprocessing.opensea.skipReason = `No tokens found for ${task.contract.slug}`;
        preprocessing.opensea.jobCount = 0;
        console.log(YELLOW + `[WARN] No tokens found for token bid on ${task.contract.slug}` + RESET);
      }
    } else if (traitBid) {
      // Trait bid - need to check if collection supports trait offers
      const collectionDetails = await getCollectionDetails(task.contract.slug, task.contract.contractAddress);
      if (collectionDetails.trait_offers_enabled) {
        preprocessing.opensea.traits = transformOpenseaTraits(selectedTraits);
        preprocessing.opensea.jobCount = preprocessing.opensea.traits.length;
      } else {
        preprocessing.opensea.skipReason = `Trait bidding not available for ${task.contract.slug}`;
        preprocessing.opensea.jobCount = 0;
      }
    } else if (task.bidType === "collection") {
      // Collection bid
      preprocessing.opensea.jobCount = 1;
    }
  } else if (marketplace === "blur") {
    preprocessing.blur.enabled = true;

    // Check if Blur supports this bid type
    if (task.bidType === "token") {
      preprocessing.blur.skipReason = "Token bids not supported on Blur";
      preprocessing.blur.jobCount = 0;
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

    if (task.bidType === "token") {
      if (magicedenTokens.length > 0) {
        // Token bid
        // Track duplicates before deduplication
        const originalCount = magicedenTokens.length;
        preprocessing.magiceden.tokens = [...new Set(magicedenTokens)];
        preprocessing.magiceden.jobCount = preprocessing.magiceden.tokens.length;

        // Log duplicates if any were removed
        const duplicateCount = originalCount - preprocessing.magiceden.tokens.length;
        if (duplicateCount > 0) {
          // Find which tokens were duplicates
          const tokenCounts = new Map<number, number>();
          magicedenTokens.forEach(token => {
            tokenCounts.set(token, (tokenCounts.get(token) || 0) + 1);
          });

          const duplicateTokens: string[] = [];
          tokenCounts.forEach((count, token) => {
            if (count > 1) {
              duplicateTokens.push(`${token} (${count} times)`);
            }
          });

          // Log the duplicate tokens
          await logBidError(
            taskId,
            "Duplicate tokens removed",
            `Removed ${duplicateCount} duplicate tokens from MagicEden: ${duplicateTokens.join(", ")}`,
            "skipped",
            "magiceden"
          );

          // Update skipStats
          if (!skipStats[taskId]) {
            skipStats[taskId] = { opensea: 0, magiceden: 0, blur: 0 };
          }
          skipStats[taskId].magiceden = (skipStats[taskId].magiceden || 0) + duplicateCount;

          console.log(YELLOW + `[INFO] Removed ${duplicateCount} duplicate tokens for MagicEden` + RESET);
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
            console.log(YELLOW + `[INFO] Total missing ${totalMissing} tokens to reach target amount of ${amount} for MagicEden` + RESET);
          }
        }
      } else {
        // Only throw error if API was called and returned empty (scenario 6)
        if (magicedenApiCalled && magicedenApiReturnedEmpty && !manualTokenIds.length) {
          console.log(RED + `[ERROR] MagicEden API returned no tokens for ${task.contract.slug}` + RESET);
          throw new Error(`No tokens found on MagicEden for ${task.contract.slug}. Collection may have no active listings.`);
        }
        // Otherwise, just skip without error
        preprocessing.magiceden.skipReason = `No tokens found for ${task.contract.slug}`;
        preprocessing.magiceden.jobCount = 0;
        console.log(YELLOW + `[WARN] No tokens found for token bid on ${task.contract.slug} for MagicEden` + RESET);
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

  return preprocessing;
}

async function processOpenseaScheduledBid(task: ITask, jobId?: string, scheduleJob?: Job) {
  const currentLoopCount = loopCounterStats[task._id.toString()] || 1;
  console.log(`${BLUE}[processOpenseaScheduledBid] CALLED for task ${task._id} - Loop #${currentLoopCount}${RESET}`);

  // Log if this appears to be a retry (job has an ID and attempt number)
  if (scheduleJob && scheduleJob.attemptsMade > 0) {
    console.log(`${YELLOW}[processOpenseaScheduledBid] This is retry attempt ${scheduleJob.attemptsMade + 1} for job ${scheduleJob.id}${RESET}`);
  }

  const taskId = task._id.toString();
  let lockAcquired = false;

  try {
    if (!enabledMarketplaces.map((m) => m.toLowerCase()).includes("opensea")) {
      return;
    }
    const currentTask = activeTasks.get(task._id);
    if (!currentTask || !currentTask.running || !currentTask.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("opensea")) {
      return;
    }

    const taskId = task._id;
    // Processing OpenSea scheduled bid

    // Get preprocessing data - first check the Map, then perform if needed
    let preprocessing = taskPreprocessing.get(taskId);

    if (!preprocessing || !preprocessing.opensea.enabled) {
      console.log(`${BLUE}[processOpenseaScheduledBid] No preprocessing data found, performing preprocessing now${RESET}`);

      // Determine available marketplaces for this task
      const validMarketplaces = ["opensea", "magiceden", "blur"];
      const availableMarketplaces = currentTask.selectedMarketplaces
        .map(m => m.toLowerCase())
        .filter(m => enabledMarketplaces.map(em => em.toLowerCase()).includes(m))
        .filter(m => validMarketplaces.includes(m));

      // Perform preprocessing for OpenSea
      preprocessing = await performMarketplacePreprocessing(currentTask, "opensea", availableMarketplaces);

      // Store in Map for other marketplaces to use
      taskPreprocessing.set(taskId, preprocessing);

      // If preprocessing didn't enable OpenSea, it means something went wrong
      if (!preprocessing.opensea.enabled) {
        throw new Error(`Preprocessing failed to enable OpenSea for task ${taskId}`);
      }
    }

    if (preprocessing.opensea.skipReason) {
      console.log(`${YELLOW}[OPENSEA] Skipped: ${preprocessing.opensea.skipReason}${RESET}`);
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
      taskStatus[taskId] = {
        status: 'PROCESSING',
        runningPercentage: 0,
        cancellationPercentage: 0,
        processingPercentage: 0,
        totalJobs: 0,
        completedJobs: 0,
        waitingTime: 0
      };
    }

    // Use timed lock manager for better timeout handling
    lockAcquired = timedLockManager.acquireLock(taskId, 'opensea');
    if (!lockAcquired) {
      console.log(`${YELLOW}[LOCK] Task ${taskId} is already being processed by OpenSea scheduled bid - skipping${RESET}`);
      return;
    }
    console.log(`${GREEN}[LOCK] Acquired lock for task ${taskId} in OpenSea scheduled bid${RESET}`);

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
    const WALLET_PRIVATE_KEY: string = decryptPrivateKey(wallets?.encryptedPrivateKey as string)

    // Extend lock before fetching balances
    if (lockAcquired) {
      timedLockManager.extendLock(taskId, 'opensea');
    }

    // Fetch fresh WETH balance with timeout protection
    console.log(`${BLUE}[OpenSea] Fetching WETH balance for ${WALLET_ADDRESS}...${RESET}`);
    let wethBalance = 0;
    try {
      const { fetchERC20BalanceFresh } = await import('./utils/balance');
      const { WETH } = await import('./utils/currencies');
      wethBalance = await fetchERC20BalanceFresh(WALLET_ADDRESS, WETH);
      console.log(`${BLUE}[OpenSea] Fresh WETH balance for ${WALLET_ADDRESS}: ${wethBalance}${RESET}`);
    } catch (error) {
      console.error(`${RED}[OpenSea] Failed to fetch WETH balance: ${error}${RESET}`);
      console.log(`${YELLOW}[OpenSea] Continuing with balance: 0.00${RESET}`);
    }

    console.log(`${BLUE}[OpenSea] Getting collection details for ${currentTask.contract.slug}...${RESET}`);
    // Get collection details for creator fees
    let currency, collectionDetails;
    try {
      const results = await Promise.all([
        calculateBidPrice(currentTask, 0, "opensea"),
        getCollectionDetails(currentTask.contract.slug, currentTask.contract.contractAddress)
      ]);
      currency = results[0].currency;
      collectionDetails = results[1];
      console.log(`${BLUE}[OpenSea] Successfully got collection details${RESET}`);
    } catch (error) {
      console.error(`${RED}[OpenSea] Failed to get collection details: ${error}${RESET}`);
      throw error;
    }

    const creatorFees: IFee = collectionDetails.creator_fees.null !== undefined
      ? { null: collectionDetails.creator_fees.null }
      : Object.fromEntries(Object.entries(collectionDetails.creator_fees).map(([key, value]) => [key, Number(value)]));

    // Note: Status will be set to appropriate state after job creation is determined
    const newTask = { ...currentTask, traits: {} } as ITask

    console.log(`${BLUE}[OpenSea] Checking preprocessing results:${RESET}`);
    console.log(`${BLUE}[OpenSea] - tokens.length: ${preprocessing.opensea.tokens.length}${RESET}`);
    console.log(`${BLUE}[OpenSea] - traits.length: ${preprocessing.opensea.traits.length}${RESET}`);
    console.log(`${BLUE}[OpenSea] - jobCount: ${preprocessing.opensea.jobCount}${RESET}`);

    // Create jobs based on preprocessing results
    if (preprocessing.opensea.tokens.length > 0) {
      // Token bid
      console.log(`${BLUE}[OPENSEA] Creating ${preprocessing.opensea.tokens.length} token bid jobs${RESET}`);
      console.log(`${BLUE}[OPENSEA] Current loop count: ${loopCounterStats[taskId] || 1}${RESET}`);

      const jobs = preprocessing.opensea.tokens
        .map((token) => ({
          name: OPENSEA_TOKEN_BID,
          data: {
            _id: taskId,
            address: WALLET_ADDRESS,
            privateKey: WALLET_PRIVATE_KEY,
            slug: currentTask.contract.slug,
            creatorFees,
            enforceCreatorFee: collectionDetails.enforceCreatorFee,
            asset: { contractAddress: currentTask.contract.contractAddress, tokenId: token },
            outbidOptions: currentTask.outbidOptions,
            task: newTask,
          },
          opts: { priority: TOKEN_BID_PRIORITY.OPENSEA } as JobsOptions,
        })) as Job[]

      // Ensure parent job exists BEFORE processing jobs
      await ensureParentJobExists(taskId, 'opensea');
      const parentJobId = getParentJobId(taskId, 'opensea');
      const parentJob = parentJobs.get(parentJobId);
      if (parentJob) {
        parentJob.expectedJobs = preprocessing.opensea.tokens.length;
        parentJob.totalJobs = preprocessing.opensea.tokens.length; // Use expected count initially
        console.log(`${BLUE}[OPENSEA] Set parent job ${parentJobId} expectedJobs: ${parentJob.expectedJobs}${RESET}`);
      }

      try {
        // Extend lock before processing bulk jobs
        if (lockAcquired) {
          timedLockManager.extendLock(taskId, 'opensea');
        }

        // Transition to QUEUEING state before creating jobs
        setTaskStatus(taskId, 'QUEUEING', 'queueing opensea token bid jobs');

        // Fire-and-forget: processBulkJobs runs asynchronously to prevent timeout
        processBulkJobs(jobs, true, scheduleJob);

        // Register for job creation verification
        registerJobCreationVerification(taskId, 'opensea', preprocessing.opensea.tokens.length);

        console.log(BLUE + `[OPENSEA] Started queueing ${preprocessing.opensea.tokens.length} token bid jobs` + RESET);

        // The task will transition to RUNNING when jobs start processing

      } catch (jobError) {
        console.error(RED + `[OPENSEA] Failed to queue token bid jobs: ${jobError}` + RESET);
        // Re-throw to trigger retry
        throw jobError;
      }
    } else if (preprocessing.opensea.traits.length > 0) {
      // Trait bid
      console.log(`${BLUE}[OPENSEA] Creating ${preprocessing.opensea.traits.length} trait bid jobs${RESET}`);

      const traitJobs = preprocessing.opensea.traits.map((trait) => {
        const jobData = {
          _id: taskId,
          address: WALLET_ADDRESS,
          privateKey: WALLET_PRIVATE_KEY,
          slug: currentTask.contract.slug,
          contractAddress: currentTask.contract.contractAddress,
          creatorFees,
          enforceCreatorFee: collectionDetails.enforceCreatorFee,
          trait: JSON.stringify(trait),
          outbidOptions: currentTask.outbidOptions,
          task: newTask,
        };
        return {
          name: OPENSEA_TRAIT_BID,
          data: jobData,
          opts: { priority: TRAIT_BID_PRIORITY.OPENSEA } as JobsOptions
        };
      }) as Job[]

      // Ensure parent job exists BEFORE processing jobs
      await ensureParentJobExists(taskId, 'opensea');
      const parentJobId = getParentJobId(taskId, 'opensea');
      const parentJob = parentJobs.get(parentJobId);
      if (parentJob) {
        parentJob.expectedJobs = preprocessing.opensea.traits.length;
        parentJob.totalJobs = preprocessing.opensea.traits.length; // Use expected count initially
        console.log(`${BLUE}[OPENSEA] Set parent job ${parentJobId} expectedJobs: ${parentJob.expectedJobs}${RESET}`);
      }

      try {
        // Extend lock before processing bulk trait jobs
        if (lockAcquired) {
          timedLockManager.extendLock(taskId, 'opensea');
        }

        // Transition to QUEUEING state before creating jobs
        setTaskStatus(taskId, 'QUEUEING', 'queueing opensea trait bid jobs');

        // Fire-and-forget: processBulkJobs runs asynchronously to prevent timeout
        processBulkJobs(traitJobs, true, scheduleJob);

        // Register for job creation verification
        registerJobCreationVerification(taskId, 'opensea', preprocessing.opensea.traits.length);

        console.log(BLUE + `[OPENSEA] Started queueing ${preprocessing.opensea.traits.length} trait bid jobs` + RESET);

        // The task will transition to RUNNING when jobs start processing

      } catch (jobError) {
        console.error(RED + `[OPENSEA] Failed to queue trait bid jobs: ${jobError}` + RESET);
        // Re-throw to trigger retry
        throw jobError;
      }
    } else if (preprocessing.opensea.jobCount === 1) {
      // Collection bid
      console.log(`${BLUE}[OPENSEA] Creating 1 collection bid job${RESET}`);
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
        console.log(`${BLUE}[OPENSEA] Set parent job ${parentJobId} for single collection bid${RESET}`);
      }

      // Transition to QUEUEING state before creating job
      setTaskStatus(taskId, 'QUEUEING', 'queueing opensea collection bid job');

      await queue.add(
        OPENSEA_COLLECTION_BID,
        { task: newTask, collectionDetails, currency },
        { jobId }
      );

      console.log(GREEN + `[OPENSEA] Successfully queued 1 collection bid job` + RESET);

      // Transition to RUNNING after job is queued
      setTaskStatus(taskId, 'RUNNING', 'opensea collection bid job queued');

      // Trigger percentage update for single job
      await updateTaskRunningPercentage(taskId);
    } else {
      // No jobs to create - create empty parent job and complete the loop
      console.log(YELLOW + `[OPENSEA] No jobs to create for task ${taskId} - completing loop with 0 jobs` + RESET);

      await ensureParentJobExists(taskId, 'opensea');
      const parentJobId = getParentJobId(taskId, 'opensea');
      const parentJob = parentJobs.get(parentJobId);
      if (parentJob) {
        parentJob.expectedJobs = 0;
        parentJob.totalJobs = 0;
        parentJob.actualJobs = 0;
        parentJob.completedJobs = 0;
        parentJob.allJobsQueued = true;
        console.log(GREEN + `[OPENSEA] Marked parent job ${parentJobId} as complete with 0 jobs` + RESET);
      }

      // Update task status to trigger completion flow
      setTaskStatus(taskId, 'RUNNING', 'opensea no jobs - completing loop');
      await updateTaskRunningPercentage(taskId);

      // Check if all parent jobs are complete to trigger next loop
      const allComplete = checkAllParentJobsComplete(taskId);
      if (allComplete) {
        console.log(GREEN + `[OPENSEA] All parent jobs complete for task ${taskId}, triggering completion` + RESET);
        await checkTaskCompletionAndScheduleNext(taskId);
      }

      // Don't throw error - let the loop complete normally
      console.log(YELLOW + `[OPENSEA] Loop will complete with 0 jobs and retry in next cycle` + RESET);
    }
  } catch (error) {
    console.error(RED + `Error processing OpenSea scheduled bid for task: ${task._id}` + RESET, error);

    // Log to file
    const logsDir = path.join(process.cwd(), 'logs');
    if (!fs.existsSync(logsDir)) {
      fs.mkdirSync(logsDir, { recursive: true });
    }

    const errorDetails = {
      timestamp: new Date().toISOString(),
      marketplace: 'opensea',
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

    // Check if this is a Redis cluster failure
    if (isRedisClusterError(error)) {
      console.log(RED + '🚨 Redis cluster error detected in OpenSea scheduled bid processor' + RESET);

      // Add detailed error information
      await logger.logGeneralError(
        'Redis cluster failure in OpenSea scheduled bid',
        {
          taskId: task._id.toString(),
          marketplace: 'opensea',
          errorType: 'REDIS_CLUSTER_DOWN',
          error: error instanceof Error ? error : new Error(String(error))
        }
      );
    }

    // Check if task is stuck in STARTED status
    const currentStatus = taskStatus[taskId]?.status;
    if (currentStatus === 'STARTED') {
      console.log(YELLOW + `[processOpenseaScheduledBid] Error while in STARTED status, checking state` + RESET);

      // Give other marketplaces a chance, then check
      setTimeout(() => {
        taskStatusManager.checkAndFixStuckStartedTask(taskId);
      }, 5000);
    }

    // Re-throw error to trigger job retry
    throw error;
  } finally {
    // Always release lock if acquired
    if (lockAcquired) {
      timedLockManager.releaseLock(taskId, 'opensea');
      console.log(`${GREEN}[LOCK] Released lock for task ${taskId} in OpenSea scheduled bid${RESET}`);
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

async function processOpenseaCollectionBid(data: { task: ITask, collectionDetails: any }) {
  const taskId = data.task._id.toString();
  const jobId = `opensea-collection-${data.task.contract.slug}`;

  // Check if task is blacklisted
  if (isTaskBlacklisted(taskId, 'opensea')) {
    console.log(`[processOpenseaCollectionBid] Task ${taskId} is blacklisted, skipping job execution`);
    return;
  }

  try {
    if (!enabledMarketplaces.length) {
      return;
    }
    const { task, collectionDetails } = data
    const currentTask = activeTasks.get(task._id)
    if (!currentTask || !currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("opensea") || !enabledMarketplaces
      .map((m) => m.toLowerCase())
      .includes("opensea")) {
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

    // Parallel fetch: wallet, floor price, and orders
    const [wallets, floor_price, orders] = await Promise.all([
      walletCache.getWalletById(task.walletId),
      getOpenseaFloor(currentTask.contract.slug),
      Order.find({
        slug: currentTask.contract.slug,
        taskId: currentTask._id,
        marketplace: OPENSEA.toLowerCase(),
        identifier: 'collection'
      }).sort({ createdAt: -1 })
    ]);

    const privateKey: string = decryptPrivateKey(wallets?.encryptedPrivateKey as string)

    if (!floor_price) {
      return;
    }

    floorPrices[currentTask._id].opensea = floor_price as number;

    const { offerPriceEth, maxBidPriceEth, minBidPriceEth, currencySymbol, currency, decimal } = await calculateBidPrice(currentTask, floor_price as number, "opensea")
    const stopBid = await stopOption(currentTask, 'opensea', Number(data), false)
    const expiry = getExpiry(currentTask.bidDuration)

    if (stopBid) {
      return;
    }

    const usdcBid = currency.toLowerCase() === USDC.toLowerCase()
    let offerPrice = BigInt(Math.ceil(roundToValidPrecision(offerPriceEth) * Math.pow(10, decimal)));

    let colletionOffer = BigInt(offerPrice)
    const [order, ...remainingOrders] = orders
    const ttl = (order?.expiresAt?.getTime() - Date.now()) / 1000

    const currentPrice = Number(order ? order.offerPrice : colletionOffer)

    if (remainingOrders.length > 0) {
      const cancelData = remainingOrders.map((order, index) => {
        return {
          name: CANCEL_OPENSEA_BID,
          data: { privateKey, orderId: order.orderHash, taskId: task._id, orderKey: order._id },
        }
      })
      queue.addBulk(cancelData); // Non-blocking
    }

    const createdAt = new Date(order?.createdAt).getTime()
    const expiredAt = new Date(order?.expiresAt).getTime()
    const orderExpiration = order ? Math.round((expiredAt - createdAt) / 1000) : expiry

    if (!currentTask.outbidOptions.outbid) {
      if (ttl >= MIN_BID_DURATION && currentPrice === Number(colletionOffer) && Math.abs(orderExpiration - expiry) < 30) {
        return;
      }
    }
    else {
      // Parallel fetch: wallets and offers
      const [walletsArr, offers] = await Promise.all([
        walletCache.getAllWallets(),
        fetchOpenseaOffers('COLLECTION', currentTask.contract.slug, currentTask.contract.contractAddress, {})
      ]);
      const [topOffer, secondOffer] = (offers as unknown) as OpenseaOfferData[]
      const highestBidAmount = usdcBid ? topOffer.usd * Math.pow(10, decimal) : topOffer.eth * Math.pow(10, decimal)
      const secondTopBidAmount = usdcBid ? secondOffer.usd * Math.pow(10, decimal) : secondOffer.eth * Math.pow(10, decimal)

      const outbidMargin = calculateOutbidMargin(highestBidAmount ? highestBidAmount / Math.pow(10, decimal) : offerPriceEth, currency);

      const owner = topOffer.owner.toLowerCase();
      const isOwnBid = walletsArr
        .map(addr => addr.address.toLowerCase())
        .includes(owner);

      // 2. Handle own bid case
      if (isOwnBid) {
        const spread = Math.round((Number(highestBidAmount) - Number(secondTopBidAmount)) / Math.pow(10, decimal));
        // Check for overbidding
        if (spread > outbidMargin && Number(secondTopBidAmount) > 0) {
          if (orders.length > 0) {
            const cancelData = orders.map((order) => {
              return {
                name: CANCEL_OPENSEA_BID,
                data: { privateKey, orderId: order.orderHash, taskId: task._id, orderKey: order._id },
              }
            })
            queue.addBulk(cancelData)
          }
          // Place new bid at optimal level
          colletionOffer = BigInt(Math.round(secondTopBidAmount + (outbidMargin * Math.pow(10, decimal))));
        }
        // Early return if bid isn't close to expiration
        else if (ttl > MIN_BID_DURATION) {
          console.log(YELLOW + `Skipping opensea collection bid for ${task.contract.slug} - we have top bid(${highestBidAmount / Math.pow(10, decimal)} ${currencySymbol}) with ${(ttl / 60).toFixed(2)} minutes remaining`.toUpperCase() + RESET);
          return;
        }
        else {
          colletionOffer = BigInt(Math.round(highestBidAmount));
        }
      }
      else {
        colletionOffer = BigInt(Math.round(highestBidAmount + (outbidMargin * Math.pow(10, decimal))));
      }
    }
    const bidCount = getIncrementedBidCount(OPENSEA, task.contract.slug, task._id)

    if (orders.length > 0) {
      const cancelData = orders.map((order, index) => {
        return {
          name: CANCEL_OPENSEA_BID,
          data: { privateKey, orderId: order.orderHash, taskId: task._id, orderKey: order._id },
        }
      })
      queue.addBulk(cancelData); // Non-blocking
    }

    const creatorFees: IFee = collectionDetails.creator_fees.null !== undefined
      ? { null: collectionDetails.creator_fees.null }
      : Object.fromEntries(Object.entries(collectionDetails.creator_fees).map(([key, value]) => [key, Number(value)]));


    const offerEth = Number(colletionOffer) / Math.pow(10, decimal);

    // Check if offer exceeds floor price (only for WETH)
    if (currencySymbol.toLowerCase() === 'weth' && offerEth > floor_price) {
      const message = `⚠️ Offer price ${offerEth} ${currencySymbol} for ${task.contract.slug} collection bid exceeds floor price ${floor_price} ${currencySymbol} FOR OPENSEA.Stopping bid...`.toUpperCase();

      warningBids[task._id] = { opensea: true, magiceden: true, blur: true };
      warningMessage[task._id] = { opensea: message, magiceden: message, blur: message }

      logBidError(task._id, "OFFER EXCEEDS FLOOR PRICE", message, "warning", "opensea"); // Non-blocking
      console.log(YELLOW + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(YELLOW + message + RESET);
      console.log(YELLOW + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      return;
    }

    if (currentTask.outbidOptions.outbid && offerEth > maxBidPriceEth) {
      skipStats[task._id]['opensea']++;
      const message = `❌ Offer price ${offerEth} ${currencySymbol} for ${task.contract.slug} collection bid exceeds max bid price ${maxBidPriceEth} ${currencySymbol} FOR OPENSEA.Skipping ...`.toUpperCase()
      logBidError(task._id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "opensea"); // Non-blocking
      console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(RED + message + RESET);
      console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
        if (orders.length > 0) {
          const cancelData = orders.map((order, index) => {
            return {
              name: CANCEL_OPENSEA_BID,
              data: { privateKey, orderId: order.orderHash, taskId: task._id, orderKey: order._id },
            }
          })
          queue.addBulk(cancelData)
        }
      }
      return;
    } else if (offerEth < minBidPriceEth) {
      colletionOffer = BigInt(Math.round(minBidPriceEth * Math.pow(10, decimal)));
    }

    try {
      await bidOnOpensea(
        task._id,
        bidCount,
        currentTask.wallet,
        privateKey,
        task.contract.slug,
        Number(colletionOffer),
        creatorFees,
        collectionDetails.enforceCreatorFee,
        expiry,
        currency,
        task.contract.contractAddress
      )
    } catch (bidError) {
      console.error(RED + `OpenSea collection bid failed:`, bidError instanceof Error ? bidError.message : String(bidError) + RESET);
      throw bidError; // Re-throw to be caught by outer catch
    }

  } catch (error: any) {
    // Format OpenSea error message for better readability
    const errorMessage = extractDetailedOpenSeaError(error);

    await logBidError(data.task._id, "OPENSEA COLLECTION BID ERROR", `Error processing OpenSea collection bid for task: ${data.task._id}: ${errorMessage}`, "error", "opensea");

    // Throw the error
    throw error;
  }
}

async function processBlurScheduledBid(task: ITask) {
  const currentLoopCount = loopCounterStats[task._id.toString()] || 1;
  console.log(`${BLUE}[processBlurScheduledBid] CALLED for task ${task._id} - Loop #${currentLoopCount}${RESET}`);

  const taskId = task._id.toString();
  let lockAcquired = false;

  try {
    const currentTask = activeTasks.get(task._id)
    if (!currentTask || !currentTask?.running || !currentTask.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("blur") || !enabledMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("blur")) {
      return;
    }
    // Processing Blur scheduled bid

    // Get preprocessing data - first check the Map, then perform if needed
    let preprocessing = taskPreprocessing.get(taskId);

    if (!preprocessing || !preprocessing.blur.enabled) {
      console.log(`${GOLD}[processBlurScheduledBid] No preprocessing data found, performing preprocessing now${RESET}`);

      // Determine available marketplaces for this task
      const validMarketplaces = ["opensea", "magiceden", "blur"];
      const availableMarketplaces = currentTask.selectedMarketplaces
        .map(m => m.toLowerCase())
        .filter(m => enabledMarketplaces.map(em => em.toLowerCase()).includes(m))
        .filter(m => validMarketplaces.includes(m));

      // Perform preprocessing for Blur
      preprocessing = await performMarketplacePreprocessing(currentTask, "blur", availableMarketplaces);

      // Store in Map for other marketplaces to use
      taskPreprocessing.set(taskId, preprocessing);

      // If preprocessing didn't enable Blur, it means something went wrong
      if (!preprocessing.blur.enabled) {
        throw new Error(`Preprocessing failed to enable Blur for task ${taskId}`);
      }
    }

    if (preprocessing.blur.skipReason) {
      console.log(`${YELLOW}[BLUR] Skipped: ${preprocessing.blur.skipReason}${RESET}`);
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
      taskStatus[taskId] = {
        status: 'PROCESSING',
        runningPercentage: 0,
        cancellationPercentage: 0,
        processingPercentage: 0,
        totalJobs: 0,
        completedJobs: 0,
        waitingTime: 0
      };
    } else {
      taskStatus[taskId].status = 'PROCESSING';
    }

    // Acquire lock for this task - will throw if stale or already locked
    // Use timed lock manager for better timeout handling
    lockAcquired = timedLockManager.acquireLock(taskId, 'blur');
    if (!lockAcquired) {
      console.log(`${YELLOW}[LOCK] Task ${taskId} is already being processed by Blur scheduled bid - skipping${RESET}`);
      return;
    }
    console.log(`${GREEN}[LOCK] Acquired lock for task ${taskId} in Blur scheduled bid${RESET}`);

    const { currencySymbol } = await calculateBidPrice(currentTask, 0, "blur")

    if (currencySymbol.toLowerCase() !== "weth") {
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
    console.log(`${CYAN}[Blur] Fetching BETH balance for ${WALLET_ADDRESS}...${RESET}`);
    let bethBalance = 0;
    try {
      const { fetchERC20BalanceFresh } = await import('./utils/balance');
      const { BETH } = await import('./utils/currencies');
      bethBalance = await fetchERC20BalanceFresh(WALLET_ADDRESS, BETH);
      console.log(`${CYAN}[Blur] Fresh BETH balance for ${WALLET_ADDRESS}: ${bethBalance}${RESET}`);
    } catch (error) {
      console.error(`${RED}[Blur] Failed to fetch BETH balance: ${error}${RESET}`);
      console.log(`${YELLOW}[Blur] Continuing with balance: 0.00${RESET}`);
    }

    console.log(`${CYAN}[Blur] Processing for collection ${currentTask.contract.slug}...${RESET}`);
    const contractAddress = currentTask.contract.contractAddress
    const newTask = { ...currentTask, traits: {} } as ITask

    // Check authentication block status before pre-authenticating
    const redis = redisClient.getClient();
    if (redis) {
      const authBlockKey = `blur:auth:block:${WALLET_ADDRESS}`;
      const authBlockTime = await redis.get(authBlockKey);

      if (authBlockTime) {
        const blockTime = parseInt(authBlockTime);
        if (blockTime > Date.now()) {
          const waitTime = blockTime - Date.now();
          const waitSeconds = Math.round(waitTime / 1000);

          console.log(RED + `[Blur Auth Block] Wallet ${WALLET_ADDRESS} is blocked from authentication` + RESET);
          console.log(RED + `[Blur Auth Block] Will resume at: ${new Date(blockTime).toLocaleTimeString()}` + RESET);
          console.log(RED + `[Blur Auth Block] Skipping task ${taskId} - wait ${waitSeconds} seconds` + RESET);

          // Update warning for UI
          if (!warningBids[taskId]) {
            warningBids[taskId] = { opensea: false, magiceden: false, blur: true };
          }
          if (!warningMessage[taskId]) {
            warningMessage[taskId] = { opensea: "", magiceden: "", blur: "" };
          }
          warningBids[taskId].blur = true;
          warningMessage[taskId].blur = `Authentication blocked - retry in ${waitSeconds}s`;

          throw new Error(`Authentication blocked - retry in ${waitSeconds}s`);
        }
      }
    }

    // Extend lock before pre-authentication
    if (lockAcquired) {
      timedLockManager.extendLock(taskId, 'blur');
    }

    // Pre-authenticate for Blur before creating jobs
    console.log(`${BLUE}[Blur Auth Warm-up] Pre-authenticating wallet ${WALLET_ADDRESS}...${RESET}`);
    try {
      const authStartTime = Date.now();
      const accessToken = await getAccessToken(WALLET_PRIVATE_KEY);

      if (accessToken) {
        const authTime = Date.now() - authStartTime;
        console.log(`${GREEN}[Blur Auth Warm-up] Successfully pre-authenticated wallet ${WALLET_ADDRESS} in ${authTime}ms${RESET}`);
      } else {
        console.log(`${YELLOW}[Blur Auth Warm-up] Failed to pre-authenticate wallet ${WALLET_ADDRESS}${RESET}`);
      }
    } catch (authError: any) {
      console.error(`${RED}[Blur Auth Warm-up] Error pre-authenticating wallet ${WALLET_ADDRESS}: ${authError}${RESET}`);

      // Check if error is due to authentication block
      if (authError.message && authError.message.includes('Authentication blocked until')) {
        console.log(RED + `[Blur Auth Block] Cannot create jobs for blocked wallet ${WALLET_ADDRESS}` + RESET);
        throw new Error(authError.message);
      }
      // Continue anyway - individual jobs will attempt auth if needed
    }

    // Check wallet pause status before creating jobs
    const walletPauseKey = `blur:wallet:pause:${WALLET_ADDRESS}`;
    const walletPauseUntil = redis ? await redis.get(walletPauseKey) : null;

    if (walletPauseUntil) {
      const pauseTime = parseInt(walletPauseUntil);
      if (pauseTime > Date.now()) {
        const waitTime = pauseTime - Date.now();
        console.log(YELLOW + `[Blur Job Creation] Wallet ${WALLET_ADDRESS.substring(0, 8)}... is paused for ${Math.round(waitTime / 1000)}s. Skipping job creation.` + RESET);
        throw new Error(`Wallet paused for ${Math.round(waitTime / 1000)}s`);
      }
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

      console.log(`${GOLD}[BLUR] Processing ${totalTraits} trait bids in ${totalBatches} batches (${batchSize} per batch)${RESET}`);
      console.log(`${GOLD}[BLUR] Rate limit: ${BLUR_RATE_LIMIT.maxBidsPerWindow} bids per ${BLUR_RATE_LIMIT.windowDuration / 1000} seconds${RESET}`);

      // Ensure parent job exists BEFORE processing jobs
      const parentJobId = `${taskId}-blur`;
      await ensureParentJobExists(taskId, 'blur');
      const parentJob = parentJobs.get(parentJobId);
      if (parentJob) {
        parentJob.expectedJobs = preprocessing.blur.traits.length;
        parentJob.totalJobs = preprocessing.blur.traits.length; // Use expected count initially
        console.log(`${GOLD}[BLUR] Set parent job ${parentJobId} expectedJobs: ${parentJob.expectedJobs}${RESET}`);
      }

      // Process traits in smaller batches that respect the rate limit
      for (let i = 0; i < totalTraits; i += batchSize) {
        const batchNumber = Math.floor(i / batchSize) + 1;
        const batchTraits = preprocessing.blur.traits.slice(i, i + batchSize);
        const batchDelay = (batchNumber - 1) * BLUR_RATE_LIMIT.batchDelay;

        console.log(`${BLUE}[BLUR] Scheduling batch ${batchNumber}/${totalBatches} (${batchTraits.length} traits) with ${batchDelay / 1000}s delay${RESET}`);

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
              privateKey: WALLET_PRIVATE_KEY,
              contractAddress,
              slug: currentTask.contract.slug,
              trait: JSON.stringify(trait),
              expiry,
              outbidOptions: currentTask.outbidOptions,
              task: newTask,
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

        processBulkJobs(traitJobs, true);

        // Register for job creation verification (for this batch)
        registerJobCreationVerification(taskId, 'blur', batchSize);

        // Log batch scheduling
        console.log(`${GREEN}[BLUR] Batch ${batchNumber}/${totalBatches} scheduled (will start in ${batchDelay / 1000}s)${RESET}`);
      }

      const totalTime = (totalBatches - 1) * BLUR_RATE_LIMIT.batchDelay / 1000;
      const effectiveBidsPerSecond = BLUR_RATE_LIMIT.batchSize / BLUR_RATE_LIMIT.windowDuration * 1000;
      const utilizationPercent = (BLUR_RATE_LIMIT.batchSize / BLUR_RATE_LIMIT.maxBidsPerWindow * 100).toFixed(1);

      console.log(`${GREEN}[BLUR] All ${totalTraits} trait bids scheduled across ${totalBatches} batches${RESET}`);
      console.log(`${GREEN}[BLUR] Rate: ${effectiveBidsPerSecond} bids/second, ${BLUR_RATE_LIMIT.batchSize} bids/10s (${utilizationPercent}% of limit)${RESET}`);
      console.log(`${GREEN}[BLUR] Estimated completion time: ~${totalTime}s${RESET}`);

    } else if (preprocessing.blur.jobCount === 1) {
      // Collection bid
      console.log(`${GOLD}[BLUR] Creating 1 collection bid job${RESET}`);

      const slug = removeChar(currentTask.contract.slug, "-")
      const jobId = `bid-${taskId}-blur-${slug}-collection-${WALLET_ADDRESS}`

      // Ensure parent job exists BEFORE adding job
      const parentJobId = `${taskId}-blur`;
      await ensureParentJobExists(taskId, 'blur');
      const parentJob = parentJobs.get(parentJobId);
      if (parentJob) {
        parentJob.expectedJobs = 1;
        parentJob.totalJobs = 1; // Use expected count initially
        console.log(`${GOLD}[BLUR] Set parent job ${parentJobId} expectedJobs: 1${RESET}`);
      }

      await blurQueue.add(
        BLUR_COLLECTION_BID,
        { task: newTask, contractAddress, expiry },
        { jobId }
      );

      // Update parent job count after successful job addition - mark as all queued for single job
      if (parentJob) {
        parentJob.actualJobs = 1;
        parentJob.allJobsQueued = true;
        console.log(GREEN + `[BLUR] Collection bid job added, marking ${parentJobId} as allJobsQueued` + RESET);
      }

      if (currentTask && currentTask.running) {
        if (taskStatus[taskId]) {
          taskStatus[taskId].status = 'QUEUED';
        }
      }

      // Trigger percentage update now that the job is queued
      await updateTaskRunningPercentage(taskId);
    } else {
      // No jobs to create - create empty parent job and complete the loop
      console.log(YELLOW + `[BLUR] No jobs to create for task ${taskId} - completing loop with 0 jobs` + RESET);

      const parentJobId = `${taskId}-blur`;
      await ensureParentJobExists(taskId, 'blur');
      const parentJob = parentJobs.get(parentJobId);
      if (parentJob) {
        parentJob.expectedJobs = 0;
        parentJob.totalJobs = 0;
        parentJob.actualJobs = 0;
        parentJob.completedJobs = 0;
        parentJob.allJobsQueued = true;
        console.log(GREEN + `[BLUR] Marked parent job ${parentJobId} as complete with 0 jobs` + RESET);
      }

      // Update task status to trigger completion flow
      setTaskStatus(taskId, 'RUNNING', 'blur no jobs - completing loop');
      await updateTaskRunningPercentage(taskId);

      // Check if all parent jobs are complete to trigger next loop
      const allComplete = checkAllParentJobsComplete(taskId);
      if (allComplete) {
        console.log(GREEN + `[BLUR] All parent jobs complete for task ${taskId}, triggering completion` + RESET);
        await checkTaskCompletionAndScheduleNext(taskId);
      }

      // Don't throw error - let the loop complete normally
      console.log(YELLOW + `[BLUR] Loop will complete with 0 jobs and retry in next cycle` + RESET);
    }
  } catch (error) {
    console.error(RED + `Error processing Blur scheduled bid for task: ${task._id}` + RESET, error);

    // Log to file
    const logsDir = path.join(process.cwd(), 'logs');
    if (!fs.existsSync(logsDir)) {
      fs.mkdirSync(logsDir, { recursive: true });
    }

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
      console.log(`${GREEN}[LOCK] Released lock for task ${taskId} in Blur scheduled bid${RESET}`);
    }
  }
}


async function processBlurCollectionBid(data: { task: ITask }) {
  const taskId = data.task._id;

  // Check if task is blacklisted before processing
  if (isTaskBlacklisted(taskId)) {
    return;
  }

  try {
    if (!enabledMarketplaces.length) {
      return;
    }
    const { task } = data
    const currentTask = activeTasks.get(task._id)
    if (!currentTask || !currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("blur") || !enabledMarketplaces.map((market) => market.toLowerCase()).includes("blur")) {
      return;
    }

    const wallets = await walletCache.getWalletById(currentTask.walletId)
    const privateKey: string = decryptPrivateKey(wallets?.encryptedPrivateKey as string)

    const floor_price: number = Number(await fetchBlurCollectionStats(currentTask.contract.slug));
    if (!floor_price) {
      return;
    }

    const orders = await Order.find({
      slug: task.contract.slug,
      taskId: task._id,
      marketplace: BLUR.toLowerCase(),
      identifier: "collection"
    }).sort({ createdAt: -1 })
    const stopBid = await stopOption(currentTask, 'blur', Number(floor_price), false)
    if (stopBid) {
      if (orders.length > 0) {
        const cancelData = orders.map((order) => {
          const payload = JSON.parse(order.orderHash)

          return {
            name: CANCEL_BLUR_BID,
            data: { privateKey, payload, taskId: task._id, orderKey: order._id }
          }
        })
        queue.addBulk(cancelData);
      }

      return
    }

    if (!floorPrices[task._id]) {
      floorPrices[task._id] = {
        opensea: 0,
        magiceden: 0,
        blur: floor_price
      };
    }
    floorPrices[task._id].blur = floor_price;

    const { offerPriceEth, maxBidPriceEth, minBidPriceEth } = await calculateBidPrice(currentTask, floor_price, "blur")
    let colletionOffer = BigInt(Math.round(offerPriceEth * 1e18))

    // Check if offer exceeds floor price (only for WETH)
    const offerEth = Number(colletionOffer) / 1e18;
    if (offerEth > floor_price) {

      const message = `⚠️ Offer price ${offerEth} WETH for ${task.contract.slug} collection bid exceeds floor price ${floor_price} WETH FOR BLUR.Stopping bid...`.toUpperCase();
      warningBids[task._id] = { opensea: true, magiceden: true, blur: true };
      warningMessage[task._id] = { opensea: message, magiceden: message, blur: message }

      await logBidError(task._id, "OFFER EXCEEDS FLOOR PRICE", message, "warning", "blur");
      console.log(YELLOW + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(YELLOW + message + RESET);
      console.log(YELLOW + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);

      return;
    }

    const [order, ...remainingOrders] = orders
    const ttl = (order?.expiresAt?.getTime() - Date.now()) / 1000
    const currentPrice = Number(order ? order.offerPrice : colletionOffer)

    if (remainingOrders.length > 0) {
      const cancelData = remainingOrders.map((order) => {
        const payload = JSON.parse(order.orderHash)
        return {
          name: CANCEL_BLUR_BID,
          data: { privateKey, payload, taskId: task._id, orderKey: order._id }
        }
      })
      queue.addBulk(cancelData);
    }
    const expiry = getExpiry(currentTask.bidDuration)

    const createdAt = new Date(order?.createdAt).getTime()
    const expiredAt = new Date(order?.expiresAt).getTime()
    const orderExpiration = order ? Math.round((expiredAt - createdAt) / 1000) : expiry

    if (!task.outbidOptions.outbid) {
      if (ttl >= MIN_BID_DURATION && currentPrice === Number(colletionOffer) && Math.abs(orderExpiration - expiry) < 30) {
        return;
      }
    }
    else {
      const highestBid = await fetchBlurBid(task.contract.slug, "COLLECTION", {});
      const [topOffer, secondOffer] = highestBid?.priceLevels?.sort((a, b) => +b.price - +a.price) || [{ price: "0" }, { price: "0" }];
      const topOfferAmount = typeof topOffer === 'object' && topOffer ? Number(topOffer.price) : 0
      const secondOfferAmount = typeof secondOffer === 'object' && secondOffer ? Number(secondOffer.price) : 0

      const bidderAddressesSample = topOffer?.bidderAddressesSample || []
      const numberBidders = topOffer?.numberBidders || 0
      const isTopBid = numberBidders === 1 && bidderAddressesSample.map((address) => address.toLowerCase()).includes(task.wallet.toLowerCase())
      const blurOutbidMargin = task.outbidOptions.blurOutbidMargin || 0.01

      console.log({ isTopBid, topOfferAmount, secondOfferAmount, bidderAddressesSample, numberBidders });


      if (isTopBid) {
        const spread = (Number(topOfferAmount) - Number(secondOfferAmount));
        if (+spread.toFixed(2) > blurOutbidMargin && Number(secondOfferAmount) > 0) {
          if (orders.length > 0) {
            const cancelData = orders.map((order) => {
              const payload = JSON.parse(order.orderHash)
              return {
                name: CANCEL_BLUR_BID,
                data: { privateKey, payload, taskId: task._id, orderKey: order._id }
              }
            })
            queue.addBulk(cancelData);
          }
          const bidPrice = secondOfferAmount + blurOutbidMargin
          colletionOffer = BigInt(Math.round(bidPrice * 1e18));
        } else if (ttl > MIN_BID_DURATION) {
          console.log(YELLOW + `Skipping blur collection bid for ${task.contract.slug} - we have top bid(${topOfferAmount} ETH) with ${(ttl / 60).toFixed(2)} minutes remaining`.toUpperCase() + RESET);

          return;
        } else {
          colletionOffer = BigInt(Math.round(topOfferAmount * 1e18));
        }
      }
      else {
        colletionOffer = BigInt(Math.round((topOfferAmount + blurOutbidMargin) * 1e18));
      }
    }

    // Round to avoid floating-point precision issues
    const collectionOfferEth = Math.round(Number(colletionOffer) / 1e18 * 100) / 100;

    if (collectionOfferEth < minBidPriceEth) {
      colletionOffer = BigInt(Math.round(minBidPriceEth * 1e18))
    }

    const bidCount = getIncrementedBidCount(BLUR, task.contract.slug, task._id)
    if (orders.length > 0) {
      const cancelData = orders.map((order) => {
        const payload = JSON.parse(order.orderHash)

        return {
          name: CANCEL_BLUR_BID,
          data: { privateKey, payload, taskId: task._id, orderKey: order._id }
        }
      })
      queue.addBulk(cancelData);
    }

    // Add tolerance for floating-point comparison (0.000001 = 1e-6)
    const PRICE_TOLERANCE = 0.000001;
    
    if (currentTask.outbidOptions.outbid && collectionOfferEth > maxBidPriceEth + PRICE_TOLERANCE) {
      if (!skipStats[task._id]) {
        skipStats[task._id] = {
          opensea: 0,
          magiceden: 0,
          blur: 0
        }
      }
      skipStats[task._id]['blur']++;
      const message = `❌ Offer price ${collectionOfferEth} BETH for ${task.contract.slug} collection bid exceeds max bid price ${maxBidPriceEth} BETH FOR BLUR BID.Skipping ...`.toUpperCase()
      await logBidError(task._id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "blur");
      console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(RED + message + RESET);
      console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);

      if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
        if (orders.length > 0) {
          const cancelData = orders.map((order) => {
            const payload = JSON.parse(order.orderHash)

            return {
              name: CANCEL_BLUR_BID,
              data: { privateKey, payload, taskId: task._id, orderKey: order._id }
            }
          })
          queue.addBulk(cancelData);
        }
      }

      return;
    } else if (collectionOfferEth < minBidPriceEth) {
      colletionOffer = BigInt(Math.round(minBidPriceEth * 1e18));
    }

    // Wrap bidOnBlur call to track completion
    try {
      await bidOnBlur(task._id, bidCount, task.wallet, privateKey, task.contract.contractAddress, colletionOffer, task.contract.slug, expiry);
    } catch (bidError: any) {
      throw bidError; // Re-throw to be handled by outer catch
    }
  } catch (error: any) {
    // Handle rate limit errors
    if (error?.code === 'RATE_LIMITED') {
      console.log(`[Blur] Rate limited for collection bid ${data.task.contract.slug} - will retry`);
      throw error; // Re-throw to let worker handle retry with delay
    }
    // Initialize errorStats if needed
    if (!errorStats[data?.task?._id]) {
      errorStats[data?.task?._id] = {
        magiceden: 0,
        opensea: 0,
        blur: 0
      }
    }


    // Handle transfer restriction errors
    if (error.code === 'TRANSFER_RESTRICTED') {
      console.log(`Skipping Blur bid for ${data.task.contract.slug} - transfer restricted collection`);
      await logBidError(data.task._id, "BLUR TRANSFER RESTRICTION", error.message, "warning", "blur");
      // Update tracking for transfer restriction
      return;
    }

    console.error('Blur collection bid error:', error instanceof Error ? error.message : String(error));
    await logBidError(data.task._id, "BLUR COLLECTION BID ERROR", `Error processing Blur collection bid for task: ${data.task._id}: ${error?.response?.data?.message || error?.response?.data || error?.message || String(error)}`, "error", "blur");

    // Throw the error
    throw error;
  }
}


async function processOpenseaTraitBid(data: {
  _id: string
  address: string;
  privateKey: string;
  slug: string;
  contractAddress: string;
  creatorFees: IFee;
  enforceCreatorFee: boolean;
  trait: string;
  outbidOptions: {
    outbid: boolean;
    blurOutbidMargin: number | null;
    openseaOutbidMargin: number | null;
    magicedenOutbidMargin: number | null;
    counterbid: boolean;
  }
  task: ITask;
}) {
  let errorMessage: string | undefined;

  // Check if task is blacklisted before processing
  if (isTaskBlacklisted(data._id)) {
    return;
  }

  try {
    if (!enabledMarketplaces.length) {
      return;
    }
    if (!enabledMarketplaces.map((m) => m.toLowerCase()).includes("opensea")) {
      return;
    }

    // Check if task is still active
    const task = activeTasks.get(data._id);
    if (!task || !task.running) {
      return;
    }

    // Check if task status is COMPLETED or STOPPED
    if (taskStatus[data._id]?.status === 'COMPLETED' || taskStatus[data._id]?.status === 'STOPPED') {
      return;
    }

    const { address, privateKey, slug, creatorFees, enforceCreatorFee, trait, contractAddress, _id } = data

    // Skip trait validation - preprocessing already determined valid traits
    // The trait passed in the job data is already validated
    const parsedTrait = JSON.parse(trait);

    const currentTask = activeTasks.get(_id)
    if (!currentTask || !currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("opensea")) {
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

    // Parallel fetch: floor price, trait floor (if needed), and orders
    const [floor_price, traitFloor, orders] = await Promise.all([
      getOpenseaFloor(slug),
      currentTask?.stopOptions?.traitPrices && currentTask?.stopOptions?.triggerStopOptions && identifier in currentTask?.stopOptions?.traitPrices
        ? openseaTraitFloor(slug, { traitType: type, values: [value] })
        : Promise.resolve(undefined),
      Order.find({
        slug: slug,
        taskId: _id,
        marketplace: OPENSEA.toLowerCase(),
        identifier
      }).sort({ createdAt: -1 })
    ]);


    if (!floor_price) {
      return;
    }

    floorPrices[_id].opensea = floor_price as number;
    const { maxBidPriceEth, offerPriceEth, minBidPriceEth, currencySymbol, currency, decimal } = await calculateBidPrice(currentTask, floor_price as number, "opensea")


    const stopBid = await stopOption(currentTask, 'opensea', Number(floor_price), false, traitFloor, identifier)
    const outbidOptions = currentTask.outbidOptions

    if (stopBid) {
      if (orders.length > 0) {
        const cancelData = orders.map((order, index) => {
          return {
            name: CANCEL_OPENSEA_BID,
            data: { privateKey, orderId: order.orderHash, taskId: _id, orderKey: order._id },
          }
        })
        queue.addBulk(cancelData)
      }
      return
    }

    const usdcBid = currency.toLowerCase() === USDC.toLowerCase()
    let traitOffer = BigInt(Math.round(roundToValidPrecision(offerPriceEth) * Math.pow(10, decimal)))
    const [order, ...remainingOrders] = orders
    const ttl = (order?.expiresAt?.getTime() - Date.now()) / 1000
    const currentPrice = Number(order ? order.offerPrice : traitOffer)

    if (remainingOrders.length > 0) {
      const cancelData = remainingOrders.map((order, index) => {
        return {
          name: CANCEL_OPENSEA_BID,
          data: { privateKey, orderId: order.orderHash, taskId: _id, orderKey: order._id },
        }
      })
      queue.addBulk(cancelData); // Non-blocking
    }

    const createdAt = new Date(order?.createdAt).getTime()
    const expiredAt = new Date(order?.expiresAt).getTime()
    const orderExpiration = order ? Math.round((expiredAt - createdAt) / 1000) : expiry
    if (!outbidOptions.outbid) {
      if (ttl >= MIN_BID_DURATION && currentPrice === Number(traitOffer) && Math.abs(orderExpiration - expiry) < 30) {
        return;
      }
    }

    else {      // Parallel fetch: offers and wallets
      const [offers, walletsArr] = await Promise.all([
        fetchOpenseaOffers('TRAIT', slug, contractAddress, JSON.parse(trait)),
        walletCache.getAllWallets()
      ]);

      const [topOffer, secondOffer] = offers as unknown as OpenseaOfferData[]
      const highestBidAmount = usdcBid ? topOffer.usd * Math.pow(10, decimal) : topOffer.eth * Math.pow(10, decimal)
      const secondTopBidAmount = usdcBid ? secondOffer.usd * Math.pow(10, decimal) : secondOffer.eth * Math.pow(10, decimal)
      const outbidMargin = calculateOutbidMargin(highestBidAmount ? highestBidAmount / Math.pow(10, decimal) : offerPriceEth, currency);

      const owner = topOffer && typeof topOffer === 'object' ? topOffer.owner?.toLowerCase() : '';
      const isOwnBid = walletsArr
        .map(addr => addr.address.toLowerCase())
        .includes(owner as string);


      if (isOwnBid) {
        const spread = Math.round((Number(highestBidAmount) - Number(secondTopBidAmount)) / Math.pow(10, decimal));
        if (spread > outbidMargin && Number(secondTopBidAmount) > 0) {
          if (orders.length > 0) {
            const cancelData = orders.map((order, index) => {
              return {
                name: CANCEL_OPENSEA_BID,
                data: { privateKey, orderId: order.orderHash, taskId: _id, orderKey: order._id },
              }
            })
            queue.addBulk(cancelData); // Non-blocking
          }
          traitOffer = BigInt(Math.round(+secondTopBidAmount + (outbidMargin * Math.pow(10, decimal))));
        }
        else if (ttl > MIN_BID_DURATION) {
          console.log(YELLOW + `Skipping opensea trait bid for ${slug} ${trait} - we have top bid(${highestBidAmount / Math.pow(10, decimal)} ${currencySymbol}) with ${(ttl / 60).toFixed(2)} minutes remaining`.toUpperCase() + RESET);
          return;
        } else {
          traitOffer = BigInt(Math.round(highestBidAmount));
        }
      }
      else {
        traitOffer = BigInt(Math.round(highestBidAmount + (outbidMargin * Math.pow(10, decimal))));
      }
    }

    const bidCount = getIncrementedBidCount(OPENSEA, slug, _id)
    if (orders.length > 0) {
      const cancelData = orders.map((order, index) => {
        return {
          name: CANCEL_OPENSEA_BID,
          data: { privateKey, orderId: order.orderHash, taskId: _id, orderKey: order._id },
        }
      })
      queue.addBulk(cancelData); // Non-blocking
    }

    const offerEth = Number(traitOffer) / Math.pow(10, decimal);

    if (outbidOptions.outbid && offerEth > maxBidPriceEth) {
      skipStats[_id]['opensea']++;
      const message = `❌ Offer price ${offerEth} ${currencySymbol} for ${slug} trait ${trait} bid exceeds max bid price ${maxBidPriceEth} ${currencySymbol} FOR OPENSEA.Skipping ...`.toUpperCase()
      logBidError(_id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "opensea"); // Non-blocking
      console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(RED + message + RESET);
      console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);

      if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
        if (orders.length > 0) {
          const cancelData = orders.map((order, index) => {
            return {
              name: CANCEL_OPENSEA_BID,
              data: { privateKey, orderId: order.orderHash, taskId: _id, orderKey: order._id },
            }
          })
          queue.addBulk(cancelData); // Non-blocking
        }
      }

      return;
    } else if (offerEth < minBidPriceEth) {
      traitOffer = BigInt(Math.round(minBidPriceEth * Math.pow(10, decimal)));
    }

    // Get parent job for tracking
    const parentId = `${_id}-opensea`.toLowerCase();
    const parentJob = parentJobs.get(parentId);

    try {
      await bidOnOpensea(
        _id,
        bidCount,
        address,
        privateKey,
        slug,
        Number(traitOffer),
        creatorFees,
        enforceCreatorFee,
        expiry,
        currency,
        task.contract.contractAddress,
        trait
      )

    } catch (bidError: any) {
      // Format OpenSea error message for better readability
      errorMessage = extractDetailedOpenSeaError(bidError);

      console.error(RED + `OpenSea trait bid failed for ${_id} ${trait}:`, errorMessage + RESET);

      // Re-throw to be caught by outer catch block
      throw bidError;
    }

  } catch (error: any) {

    if (!errorMessage) {
      // Use the new detailed error extraction function
      errorMessage = extractDetailedOpenSeaError(error);
    }


    // Don't log bid errors for stopped/cancelled tasks
    if (errorMessage && !errorMessage.includes('Bid execution blocked - task') && !errorMessage.includes('is stopped')) {
      await logBidError(data.task._id, "OPENSEA TRAIT BID ERROR", `Error bidding on trait ${data.trait} in collection ${data.slug}: ${errorMessage}`, "error", "opensea");
    }

    // Throw the error
    throw error;
  }
}

async function processOpenseaTokenBid(data: IProcessOpenseaTokenBidData) {
  const taskId = data._id;
  // Check if task is blacklisted
  if (isTaskBlacklisted(taskId, 'opensea')) {
    console.log(`[processOpenseaTokenBid] Task ${taskId} is blacklisted, skipping job execution`);
    return;
  }

  let errorMessage = ''
  try {

    if (!enabledMarketplaces.length) {
      return;
    }
    if (!enabledMarketplaces.map((m) => m.toLowerCase()).includes("opensea")) {
      return;
    }
    const { address, privateKey, slug, creatorFees, enforceCreatorFee, asset, _id } = data
    if (!currentTokenIds[_id]?.opensea?.includes(asset.tokenId.toString())) {
      return;
    }

    // Check if token is cached as trading disabled
    if (tradingDisabledCache.isDisabled(asset.contractAddress, +asset.tokenId, 'opensea')) {
      const message = `Token ${asset.tokenId} is cached as trading disabled - skipping`;
      console.log(YELLOW + message + RESET);

      // Log the skip and update error stats
      logBidError(_id, "TRADING DISABLED (CACHED)", message, "skipped", "opensea"); // Non-blocking

      if (!errorStats[_id]) {
        errorStats[_id] = { opensea: 0, magiceden: 0, blur: 0 };
      }
      // Error tracking is now handled in queue event handlers

      return;
    }

    const currentTask = activeTasks.get(_id)
    if (!currentTask || !currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("opensea")) {
      return;
    }

    const outbidEnabled = currentTask.outbidOptions?.outbid || false;

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

    // Parallel fetch: floor price and orders
    const [floor_price, orders] = await Promise.all([
      getOpenseaFloor(slug),
      Order.find({
        slug: slug,
        taskId: _id,
        marketplace: OPENSEA.toLowerCase(),
        identifier: asset.tokenId.toString()
      }).sort({ createdAt: -1 })
    ]);

    if (!floor_price) {
      return;
    }

    floorPrices[_id].opensea = floor_price as number;

    const { maxBidPriceEth, offerPriceEth, minBidPriceEth, currencySymbol, currency, decimal } = await calculateBidPrice(currentTask, floor_price as number, "opensea")
    const outbidOptions = currentTask.outbidOptions
    const expiry = getExpiry(currentTask.bidDuration)
    const usdcBid = currency.toLowerCase() === USDC.toLowerCase()

    let tokenOffer = BigInt(Math.round(offerPriceEth * Math.pow(10, decimal)))

    const stopBid = await stopOption(currentTask, 'opensea', Number(floor_price), false)
    if (stopBid) {
      if (orders.length > 0) {
        const cancelData = orders.map((order, index) => {
          return {
            name: CANCEL_OPENSEA_BID,
            data: { privateKey, orderId: order.orderHash, taskId: _id, orderKey: order._id }
          }
        })
        queue.addBulk(cancelData); // Non-blocking
      }
      return
    }

    const [order, ...remainingOrders] = orders
    const ttl = (order?.expiresAt?.getTime() - Date.now()) / 1000
    const currentPrice = Number(order ? order.offerPrice : tokenOffer)

    if (remainingOrders.length > 0) {
      const cancelData = remainingOrders.map((order, index) => {
        return {
          name: CANCEL_OPENSEA_BID,
          data: { privateKey, orderId: order.orderHash, taskId: _id, orderKey: order._id },
        }
      })
      queue.addBulk(cancelData); // Non-blocking
    }

    const createdAt = new Date(order?.createdAt).getTime()
    const expiredAt = new Date(order?.expiresAt).getTime()
    const orderExpiration = order ? Math.round((expiredAt - createdAt) / 1000) : expiry


    if (!outbidOptions.outbid) {

      if (ttl >= MIN_BID_DURATION && currentPrice === Number(tokenOffer) && Math.abs(orderExpiration - expiry) < 30) {
        return;
      }
    } else {
      // Parallel fetch: wallets and item offers
      const [walletsArr, highestBid] = await Promise.all([
        walletCache.getAllWallets(),
        fetchItemOffers(asset.contractAddress, asset.tokenId.toString())
      ]);
      const [topOffer, secondOffer] = highestBid

      const highestBidAmount = usdcBid ? topOffer.usd * Math.pow(10, decimal) : topOffer.eth * Math.pow(10, decimal)
      const secondTopBidAmount = usdcBid ? secondOffer.usd * Math.pow(10, decimal) : secondOffer.eth * Math.pow(10, decimal)
      const outbidMargin = calculateOutbidMargin(highestBidAmount ? highestBidAmount / Math.pow(10, decimal) : offerPriceEth, currency);

      const owner = topOffer && typeof topOffer === 'object' ? topOffer.owner?.toLowerCase() : '';
      const isOwnBid = walletsArr
        .map(addr => addr.address.toLowerCase())
        .includes(owner);

      if (isOwnBid) {
        const spread = Math.round((Number(highestBidAmount) - Number(secondTopBidAmount)) / Math.pow(10, decimal));
        if (spread > outbidMargin && Number(secondTopBidAmount) > 0) {
          if (orders.length > 0) {
            const cancelData = orders.map((order, index) => {
              return {
                name: CANCEL_OPENSEA_BID,
                data: { privateKey, orderId: order.orderHash, taskId: _id, orderKey: order._id },
              }
            })
            queue.addBulk(cancelData)
          }
          tokenOffer = BigInt(Math.round(+secondTopBidAmount + (outbidMargin * Math.pow(10, decimal))));
        }
        else if (ttl > MIN_BID_DURATION) {
          console.log(YELLOW + `Skipping opensea collection bid for ${slug} ${asset.tokenId} - we have top bid(${highestBidAmount / Math.pow(10, decimal)} ${currencySymbol}) with ${(ttl / 60).toFixed(2)} minutes remaining`.toUpperCase() + RESET);
          return;
        } else {
          tokenOffer = BigInt(Math.round(highestBidAmount));
        }
      }
      else {
        tokenOffer = BigInt(Math.round(highestBidAmount + (outbidMargin * Math.pow(10, decimal))));
      }

    }
    const bidCount = getIncrementedBidCount(OPENSEA, slug, _id)
    const offerEth = Number(tokenOffer) / Math.pow(10, decimal);

    if (outbidOptions.outbid && offerEth > maxBidPriceEth) {
      if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
        if (orders.length > 0) {
          const cancelData = orders.map((order, index) => {
            return {
              name: CANCEL_OPENSEA_BID,
              data: { privateKey, orderId: order.orderHash, taskId: _id, orderKey: order._id }
            }
          })
          queue.addBulk(cancelData)
        }
      }

      const message = `❌ Offer price ${offerEth} ${currencySymbol} for ${slug} ${asset.tokenId} bid exceeds max bid price ${maxBidPriceEth} ${currencySymbol} FOR OPENSEA.Skipping ...`.toUpperCase()
      console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(RED + message + RESET);
      console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      logBidError(_id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "opensea");
      skipStats[_id].opensea++;
      return;
    } else if (offerEth < minBidPriceEth) {
      tokenOffer = BigInt(Math.round(minBidPriceEth * Math.pow(10, decimal)));
    }

    // Cancel existing orders only when we're actually proceeding with the bid
    if (orders.length > 0) {
      const cancelData = orders.map((order, index) => {
        return {
          name: CANCEL_OPENSEA_BID,
          data: { privateKey, orderId: order.orderHash, taskId: _id, orderKey: order._id }
        }
      })
      queue.addBulk(cancelData); // Non-blocking
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
        enforceCreatorFee,
        expiry,
        currency,
        currentTask.contract.contractAddress,
        undefined,
        asset
      )
    } catch (bidError: any) {

      // Format OpenSea error message for better readability
      let errorMsg = '';
      if (bidError?.response?.data?.errors && Array.isArray(bidError.response.data.errors)) {
        // Extract the first error message from OpenSea API errors
        errorMsg = bidError.response.data.errors[0]?.message || 'Unknown OpenSea error';

        // Special handling for "Trading is disabled" errors
        if (errorMsg.includes('Trading is disabled')) {
          // Cache the disabled token
          tradingDisabledCache.markAsDisabled(
            data.asset.contractAddress,
            +data.asset.tokenId,
            'opensea',
            errorMsg
          );

          // Remove token from the list to avoid repeated attempts
          if (currentTokenIds[data._id]?.opensea) {
            const tokenIndex = currentTokenIds[data._id].opensea.indexOf(data.asset.tokenId.toString());
            if (tokenIndex > -1) {
              currentTokenIds[data._id].opensea.splice(tokenIndex, 1);
            }
          }

          // Log as warning with updated message
          console.log(YELLOW + `Token ${data.asset.tokenId} marked as trading disabled and cached for 1 hour` + RESET);
          await logBidError(data._id, "OPENSEA TOKEN TRADING DISABLED",
            `Token ${data.asset.tokenId} in collection ${data.slug}: ${errorMsg} - Cached for 1 hour`,
            "warning", "opensea");

          errorMessage = errorMsg;

          return; // Exit early without re-throwing
        }
      } else {
        errorMsg = bidError?.message || String(bidError);
      }

      errorMessage = errorMsg;
      console.error(RED + `OpenSea token bid failed for ${_id} token ${data.asset.tokenId}:`, errorMessage + RESET);

      // Re-throw to be caught by outer catch block
      throw bidError;
    }


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

    // Record error stats
    if (!errorStats[data._id]) {
      errorStats[data._id] = { opensea: 0, magiceden: 0, blur: 0 };
    }
    // Error tracking is now handled in queue event handlers

    await logBidError(data._id, "OPENSEA TOKEN BID ERROR", `Error bidding on token ${data.asset.tokenId} in collection ${data.slug}: ${errorMessage}`, "error", "opensea");
    throw error;
  }
}

async function openseaCollectionCounterBid(data: IOpenseaBidParams) {
  if (!enabledMarketplaces.length) return

  const { taskId, bidCount, walletAddress, walletPrivateKey, slug, offerPrice, creatorFees, enforceCreatorFee, expiry } = data
  const currentTask = activeTasks.get(taskId)
  if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("opensea") || !enabledMarketplaces.map((m) => m.toLowerCase()).includes("opensea")) return

  const floor_price = await getOpenseaFloor(currentTask.contract.slug);
  if (!floor_price) return

  const { currency, decimal, currencySymbol } = await calculateBidPrice(currentTask, floor_price as number, "opensea")
  const offerEth = Number(offerPrice) / Math.pow(10, decimal);

  // Check if offer exceeds floor price (only for WETH)
  if (currencySymbol.toLowerCase() === 'weth' && offerEth > floor_price) {
    const message = `⚠️ Counter bid offer price ${offerEth} ${currencySymbol} for ${slug} collection exceeds floor price ${floor_price} ${currencySymbol} FOR OPENSEA.Stopping bid...`.toUpperCase();

    warningBids[taskId] = { opensea: true, magiceden: true, blur: true };
    warningMessage[taskId] = { opensea: message, magiceden: message, blur: message }

    await logBidError(taskId, "COUNTER BID OFFER EXCEEDS FLOOR PRICE", message, "warning", "opensea");
    console.log(YELLOW + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
    console.log(YELLOW + message + RESET);
    console.log(YELLOW + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
    return;
  }

  try {
    // Fetch existing collection orders before placing new bid
    const existingOrders = await Order.find({
      slug: slug,
      taskId: taskId,
      marketplace: "opensea",
      identifier: "collection"
    });


    if (existingOrders.length > 0) {
      console.log(YELLOW + `[DEBUG][handleOpenseaCounterbid] Cancelling ${existingOrders.length} existing bids due to max price exceeded` + RESET);
      const cancelData = existingOrders.map((order, index) => {
        return {
          name: CANCEL_OPENSEA_BID,
          data: { privateKey: walletPrivateKey, orderId: order.orderHash, taskId, orderKey: order._id },

        }
      })
      await queue.addBulk(cancelData)
      console.log(GREEN + `[DEBUG][handleOpenseaCounterbid] Cancel jobs added successfully` + RESET);
    }
    await bidOnOpensea(
      taskId,
      bidCount,
      walletAddress,
      walletPrivateKey,
      slug,
      Number(offerPrice),
      creatorFees,
      enforceCreatorFee,
      expiry,
      currency,
      currentTask.contract.contractAddress
    )
  } catch (error: any) {
    const errorMessage = extractDetailedOpenSeaError(error);
    await logBidError(data.taskId, "OPENSEA COLLECTION COUNTER BID ERROR", `Error processing OpenSea collection counter bid for task: ${data.slug}: ${errorMessage}`, "error", "opensea");
  }
}

async function magicedenCollectionCounterBid(data: IMagicedenCollectionBidData) {
  if (!enabledMarketplaces.length) return
  const { _id, bidCount, address, contractAddress, quantity, offerPrice, expiration, privateKey, slug } = data
  const currentTask = activeTasks.get(_id)
  if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("magiceden") || !enabledMarketplaces.map((m) => m.toLowerCase()).includes("magiceden")) return

  const floor_price = Number(await fetchMagicEdenCollectionStats(contractAddress));
  if (!floor_price) return

  try {
    const { currencyType, currency, decimal, currencySymbol } = await calculateBidPrice(currentTask, floor_price, "magiceden")
    const offerEth = Number(offerPrice) / Math.pow(10, decimal);

    // Check if offer exceeds floor price (only for WETH)
    if (currencySymbol.toLowerCase() === 'weth' && offerEth > floor_price) {
      const message = `⚠️ Counter bid offer price ${offerEth} ${currencySymbol} for ${slug} collection exceeds floor price ${floor_price} ${currencySymbol} FOR MAGICEDEN.Stopping bid...`.toUpperCase();

      warningBids[_id] = { opensea: true, magiceden: true, blur: true };
      warningMessage[_id] = { opensea: message, magiceden: message, blur: message }

      await logBidError(_id, "COUNTER BID OFFER EXCEEDS FLOOR PRICE", message, "warning", "magiceden");
      console.log(YELLOW + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(YELLOW + message + RESET);
      console.log(YELLOW + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      return;
    }

    await bidOnMagiceden(_id, bidCount, address, contractAddress, quantity, offerPrice.toString(), privateKey, slug, expiration, currency);

  } catch (error: any) {
    await logBidError(data._id, "MAGICEDEN COLLECTION COUNTER BID ERROR", `Error processing MagicEden collection counter bid for task: ${data.slug}: ${error?.response?.data?.message || error?.response?.data || error?.message || String(error)}`, "error", "magiceden");
  }
}

interface IMagicedenTraitBidData {
  taskId: string
  bidCount: string
  address: string
  contractAddress: string
  quantity: number
  offerPrice: number
  expiration: number
  privateKey: string
  slug: string
  trait: string
}

async function magicedenTraitCounterBid(data: IMagicedenTraitBidData) {
  if (!enabledMarketplaces.length) return
  const { taskId, bidCount, address, contractAddress, quantity, offerPrice, expiration, privateKey, slug, trait } = data
  const currentTask = activeTasks.get(taskId)
  if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("magiceden") || !enabledMarketplaces.map((m) => m.toLowerCase()).includes("magiceden")) return
  const triatData = JSON.parse(trait)
  const bidTrait = {
    attributeKey: triatData?.attributeKey,
    attributeValue: triatData?.attributeValue,
  }
  try {
    const { currencyType, currency } = await calculateBidPrice(currentTask, 0, "magiceden")

    await bidOnMagiceden(taskId, bidCount, address, contractAddress, quantity, offerPrice.toString(), privateKey, slug, expiration, currency, bidTrait);
  } catch (error: any) {
    await logBidError(data.taskId, "MAGICEDEN TRAIT COUNTER BID ERROR", `Error processing MagicEden trait counter bid for task: ${data.slug}: ${error?.response?.data?.message || error?.response?.data || error?.message || String(error)}`, "error", "magiceden");
  }
}

async function blurCollectionCounterBid(data: IBlurBidData) {
  if (!enabledMarketplaces.length) return
  const { taskId, bidCount, address, privateKey, contractAddress, offerPrice, slug, expiry } = data
  const currentTask = activeTasks.get(taskId)

  if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("blur") || !enabledMarketplaces.map((m) => m.toLowerCase()).includes("blur")) return

  const floor_price: number = Number(await fetchBlurCollectionStats(currentTask.contract.slug));
  if (!floor_price) return

  const offerEth = Number(offerPrice) / 1e18;

  if (offerEth > floor_price) {
    const message = `⚠️ Counter bid offer price ${offerEth} WETH for ${slug} collection exceeds floor price ${floor_price} WETH FOR BLUR.Stopping bid...`.toUpperCase();

    warningBids[taskId] = { opensea: true, magiceden: true, blur: true };
    warningMessage[taskId] = { opensea: message, magiceden: message, blur: message }

    await logBidError(taskId, "COUNTER BID OFFER EXCEEDS FLOOR PRICE", message, "warning", "blur");
    console.log(YELLOW + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
    console.log(YELLOW + message + RESET);
    console.log(YELLOW + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
    return;
  }

  try {
    // Cancel existing orders for this collection before placing new counterbid
    const identifier = "collection";
    const existingOrders = await Order.find({
      slug: slug,
      taskId: taskId,
      marketplace: "blur",
      identifier: identifier
    });

    if (existingOrders.length > 0) {
      console.log(YELLOW + `[DEBUG][blurCollectionCounterBid] Found ${existingOrders.length} existing orders to cancel` + RESET);

      const cancelData = existingOrders.map((order) => {
        const payload = JSON.parse(order.orderHash);
        return {
          name: CANCEL_BLUR_BID,
          data: {
            privateKey: privateKey,
            payload: payload,
            taskId: taskId,
            orderKey: order._id
          }
        };
      });

      await queue.addBulk(cancelData);
      console.log(GREEN + `[DEBUG][blurCollectionCounterBid] Added ${cancelData.length} cancellation jobs to queue` + RESET);
    } else {
      console.log(CYAN + `[DEBUG][blurCollectionCounterBid] No existing orders found to cancel for ${identifier}` + RESET);
    }

    await bidOnBlur(taskId, bidCount, address, privateKey, contractAddress, BigInt(offerPrice), slug, expiry);
    console.log(YELLOW + `[DEBUG][blurCollectionCounterBid] Checking for existing orders to cancel for ${identifier}` + RESET);

  } catch (error: any) {
    // Handle rate limit errors
    if (error?.code === 'RATE_LIMITED') {
      console.log(`[Blur] Rate limited for collection counter bid ${slug} - will retry`);
      throw error; // Re-throw to let worker handle retry with delay
    }


    // Handle transfer restriction errors
    if (error.code === 'TRANSFER_RESTRICTED') {
      console.log(`Skipping Blur counter bid for ${slug} - transfer restricted collection`);
      await logBidError(taskId, "BLUR TRANSFER RESTRICTION", error.message, "warning", "blur");
      return;
    }
    await logBidError(taskId, "BLUR COLLECTION COUNTER BID ERROR", `Error processing Blur collection counter bid for task: ${slug}: ${error?.response?.data?.message || error?.response?.data || error?.message || String(error)}`, "error", "blur");
  }
}

async function blurTraitCounterBid(data: IBlurBidData) {
  if (!enabledMarketplaces.length) return
  const { taskId, bidCount, address, privateKey, contractAddress, offerPrice, slug, expiry, traits } = data
  const currentTask = activeTasks.get(taskId)
  if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("blur") || !enabledMarketplaces.map((m) => m.toLowerCase()).includes("blur")) return

  try {
    // Cancel existing orders for this trait before placing new counterbid

    if (!traits) return
    const traitsObj = JSON.parse(traits);
    const [traitType, traitValue] = Object.entries(traitsObj)[0] as string[];
    const identifier = `${traitType}:${traitValue}`;

    console.log(YELLOW + `[DEBUG][blurTraitCounterBid] Checking for existing orders to cancel for ${identifier}` + RESET);

    const existingOrders = await Order.find({
      slug: slug,
      taskId: taskId,
      marketplace: "blur",
      identifier: identifier
    });

    if (existingOrders.length > 0) {
      console.log(YELLOW + `[DEBUG][blurTraitCounterBid] Found ${existingOrders.length} existing orders to cancel` + RESET);
      const cancelData = existingOrders.map((order) => {
        const payload = JSON.parse(order.orderHash);
        return {
          name: CANCEL_BLUR_BID,
          data: {
            privateKey: privateKey,
            payload: payload,
            taskId: taskId,
            orderKey: order._id
          }
        };
      });

      await queue.addBulk(cancelData);
      console.log(GREEN + `[DEBUG][blurTraitCounterBid] Added ${cancelData.length} cancellation jobs to queue` + RESET);
    } else {
      console.log(CYAN + `[DEBUG][blurTraitCounterBid] No existing orders found to cancel for ${identifier}` + RESET);
    }
    await bidOnBlur(taskId, bidCount, address, privateKey, contractAddress, BigInt(offerPrice), slug, expiry, traits);
  } catch (error: any) {
    // Handle rate limit errors
    if (error?.code === 'RATE_LIMITED') {
      console.log(`[Blur] Rate limited for trait counter bid ${slug} ${traits} - will retry`);
      throw error; // Re-throw to let worker handle retry with delay
    }

    // Handle transfer restriction errors
    if (error.code === 'TRANSFER_RESTRICTED') {
      console.log(`Skipping Blur trait counter bid for ${slug} - transfer restricted collection`);
      await logBidError(data.taskId, "BLUR TRANSFER RESTRICTION", error.message, "warning", "blur");
      return;
    }
    logBidError(data.taskId, "BLUR TRAIT COUNTER BID ERROR", `Error processing Blur trait counter bid for task: ${data.slug}: ${error?.response?.data?.message || error?.response?.data || error?.message || String(error)}`, "error", "blur");
  }
}

interface BlurTraitCounterBid {
  taskId: string;
  bidCount: string;
  address: string;
  privateKey: string;
  contractAddress: string;
  offerPrice: number;
  slug: string;
  expiry: number;
  traits: string;
}

async function openseaTokenCounterBid(data: IProcessOpenseaTokenBidData) {
  const {
    _id,
    address,
    privateKey,
    slug,
    offerPrice,
    creatorFees,
    enforceCreatorFee,
    asset
  } = data
  const currentTask = activeTasks.get(_id)
  if (!enabledMarketplaces.length) return
  if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("opensea") || !enabledMarketplaces.map((m) => m.toLowerCase()).includes("opensea")) return
  const expiry = getExpiry(currentTask.bidDuration)
  const bidCount = getIncrementedBidCount(OPENSEA, slug, _id)
  try {

    const orders = await Order.find({
      slug: slug,
      taskId: _id,
      marketplace: OPENSEA.toLowerCase(),
      identifier: asset.tokenId.toString()
    })
    if (orders.length > 0) {
      console.log(YELLOW + `[DEBUG][handleOpenseaCounterbid] Cancelling ${orders.length} existing bids due to max price exceeded` + RESET);
      const cancelData = orders.map((order, index) => {
        return {
          name: CANCEL_OPENSEA_BID,
          data: { privateKey: privateKey, orderId: order.orderHash, taskId: _id, orderKey: order._id },

        }
      })
      await queue.addBulk(cancelData)
      console.log(GREEN + `[DEBUG][handleOpenseaCounterbid] Cancel jobs added successfully` + RESET);
    }
    const { currency } = await calculateBidPrice(currentTask, 0, "opensea")

    await bidOnOpensea(
      _id,
      bidCount,
      address,
      privateKey,
      slug,
      Number(offerPrice),
      creatorFees,
      enforceCreatorFee,
      expiry,
      currency,
      currentTask.contract.contractAddress,
      undefined,
      asset
    )

  } catch (error: any) {
    const errorMessage = extractDetailedOpenSeaError(error);
    await logBidError(data.task._id, "OPENSEA TOKEN COUNTER BID ERROR", `Error bidding on token ${data.asset.tokenId} in collection ${data.slug}: ${errorMessage}`, "error", "opensea");
  }
}

async function openseaTraitCounterBid(data: IProcessOpenseaTraitBidData) {
  try {
    if (!enabledMarketplaces.length) return
    const { _id, bidCount, address, privateKey, slug, offerPrice, creatorFees, enforceCreatorFee, expiry, trait } = data
    const currentTask = activeTasks.get(_id)
    if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("opensea") || !enabledMarketplaces.map((m) => m.toLowerCase()).includes("opensea")) return
    if (!trait) return

    const parsedTrait: { type: string; value: string } = JSON.parse(trait)
    const identifier = `${parsedTrait.type}:${parsedTrait.value}`
    const orders = await Order.find({
      slug: slug,
      taskId: _id,
      marketplace: OPENSEA.toLowerCase(),
      identifier: identifier
    })
    if (orders.length > 0) {
      console.log(YELLOW + `[DEBUG][handleOpenseaCounterbid] Cancelling ${orders.length} existing bids due to max price exceeded` + RESET);
      const cancelData = orders.map((order, index) => {
        return {
          name: CANCEL_OPENSEA_BID,
          data: { privateKey: privateKey, orderId: order.orderHash, taskId: _id, orderKey: order._id },

        }
      })
      await queue.addBulk(cancelData)
      console.log(GREEN + `[DEBUG][handleOpenseaCounterbid] Cancel jobs added successfully` + RESET);
    }

    const { currency } = await calculateBidPrice(currentTask, 0, "opensea")

    await bidOnOpensea(
      _id,
      bidCount,
      address,
      privateKey,
      slug,
      Number(offerPrice),
      creatorFees,
      enforceCreatorFee,
      expiry,
      currency,
      currentTask.contract.contractAddress,
      trait
    )
  } catch (error: any) {
    await logBidError(data._id, "OPENSEA TRAIT COUNTER BID ERROR", `Error bidding on trait ${data.trait} in collection ${data.slug}: ${error?.response?.data?.errors?.[0] || error?.response?.data || error?.message || String(error)}`, "error", "opensea");
  }
}

async function magicedenTokenCounterBid(data: IMagicedenTokenBidData) {
  try {
    if (!enabledMarketplaces.length) return
    const { _id, address, contractAddress, quantity, offerPrice, privateKey, slug, tokenId, expiration } = data
    const currentTask = activeTasks.get(_id)
    if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("magiceden") || !enabledMarketplaces.map((m) => m.toLowerCase()).includes("magiceden")) return

    const { currency } = await calculateBidPrice(currentTask, 0, "magiceden")

    const bidCount = getIncrementedBidCount(MAGICEDEN, slug, _id)
    await bidOnMagiceden(_id, bidCount, address, contractAddress, quantity, (offerPrice as number).toString(), privateKey, slug, expiration, currency, undefined, tokenId)
  } catch (error: any) {
    await logBidError(data._id, "MAGICEDEN TOKEN COUNTER BID ERROR", `Error bidding on token ${data.tokenId} in collection ${data.slug}: ${error?.response?.data?.message || error?.response?.data || error?.message || String(error)}`, "error", "magiceden");
  }
}


async function processBlurTraitBid(data: {
  _id: string;
  address: string;
  privateKey: string;
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
  task: ITask;
  batchInfo?: {
    batchNumber: number;
    totalBatches: number;
    traitIndex: number;
    totalTraits: number;
    batchSize?: number;
  };
}) {
  const taskId = data._id;

  try {
    if (!enabledMarketplaces.length) {
      return;
    }
    const { address, privateKey, contractAddress, slug, trait, _id, task } = data;

    // Skip trait validation - preprocessing already determined valid traits
    // The trait passed in the job data is already validated
    const parsedTrait = JSON.parse(trait);

    const currentTask = activeTasks.get(_id)

    if (!currentTask || !currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("blur") || !enabledMarketplaces.map((m) => m.toLowerCase()).includes("blur")) {
      return;
    }
    const outbidOptions = currentTask.outbidOptions
    const floor_price: number = Number(await fetchBlurCollectionStats(task.contract.slug));
    const traitsObj = JSON.parse(trait);
    const [traitType, traitValue] = Object.entries(traitsObj)[0];
    const identifier = `${traitType}:${traitValue}`
    const traitFloor = currentTask?.stopOptions?.traitPrices && currentTask?.stopOptions?.triggerStopOptions && identifier in currentTask?.stopOptions?.traitPrices ? await fetchBlurTraitFloor(slug, { type: traitType, value: traitValue as string }) : undefined

    const stopBid = await stopOption(currentTask, 'blur', Number(floor_price), false, traitFloor, identifier)
    const expiry = getExpiry(currentTask.bidDuration)
    const orders = await Order.find({
      slug: slug,
      taskId: _id,
      marketplace: BLUR.toLowerCase(),
      identifier
    }).sort({ createdAt: -1 })

    if (stopBid) {
      if (orders.length > 0) {
        const cancelData = orders.map((order) => {
          const payload = JSON.parse(order.orderHash)
          return {
            name: CANCEL_BLUR_BID,
            data: { privateKey, payload, taskId: _id, orderKey: order._id }
          }
        })
        queue.addBulk(cancelData);
      }

      return;
    }
    if (!floor_price) {
      return;
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
    const ttl = (order?.expiresAt?.getTime() - Date.now()) / 1000
    const currentPrice = Number(order ? order.offerPrice : traitOffer)

    if (remainingOrders.length > 0) {
      const cancelData = remainingOrders.map((order) => {
        const payload = JSON.parse(order.orderHash)
        return {
          name: CANCEL_BLUR_BID,
          data: { privateKey, payload, taskId: _id, orderKey: order._id }
        }
      })
      queue.addBulk(cancelData);
    }

    const createdAt = new Date(order?.createdAt).getTime()
    const expiredAt = new Date(order?.expiresAt).getTime()
    const orderExpiration = order ? Math.round((expiredAt - createdAt) / 1000) : expiry

    if (!outbidOptions.outbid) {
      if (ttl >= MIN_BID_DURATION && currentPrice === Number(traitOffer) && Math.abs(orderExpiration - expiry) < 30) {
        return;
      }
    }
    else {
      const bids = await fetchBlurBid(slug, "TRAIT", JSON.parse(trait));
      const [topOffer, secondOffer] = bids?.priceLevels?.sort((a, b) => +b.price - +a.price) || [{ price: "0" }, { price: "0" }];

      const topOfferAmount = typeof topOffer === 'object' && topOffer ? Number(topOffer.price) : 0
      const secondOfferAmount = typeof secondOffer === 'object' && secondOffer ? Number(secondOffer.price) : 0
      if (!bestOffers[_id]) {
        bestOffers[_id] = {
          blur: topOfferAmount,
          opensea: 0,
          magiceden: 0
        }
      }
      const bidderAddressesSample = topOffer?.bidderAddressesSample || []
      const numberBidders = topOffer?.numberBidders || 0
      const blurOutbidMargin = outbidOptions.blurOutbidMargin || 0.01

      const isTopBid = numberBidders === 1 && bidderAddressesSample.map((address) => address.toLowerCase()).includes(task.wallet.toLowerCase())

      if (isTopBid) {
        const spread = (Number(topOfferAmount) - Number(secondOfferAmount));
        if (+spread.toFixed(2) > blurOutbidMargin && Number(secondOfferAmount) > 0) {
          if (orders.length > 0) {
            const cancelData = orders.map((order) => {
              const payload = JSON.parse(order.orderHash)

              return {
                name: CANCEL_BLUR_BID,
                data: { privateKey, payload, taskId: _id, orderKey: order._id }
              }
            })
            queue.addBulk(cancelData);
          }
          const bidPrice = secondOfferAmount + blurOutbidMargin

          traitOffer = BigInt(Math.round((bidPrice * 1e18) / 1e16) * 1e16);
        } else if (ttl > MIN_BID_DURATION) {
          console.log(YELLOW + `Skipping blur trait bid for ${slug} ${trait} - we have top bid(${topOfferAmount} ETH) with ${(ttl / 60).toFixed(2)} minutes remaining`.toUpperCase() + RESET);

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
          data: { privateKey, payload, taskId: _id, orderKey: order._id }
        }
      })
      queue.addBulk(cancelData);
    }

    // Add tolerance for floating-point comparison (0.000001 = 1e-6)
    const PRICE_TOLERANCE = 0.000001;
    
    if (outbidOptions.outbid && offerEth > maxBidPriceEth + PRICE_TOLERANCE) {
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
      console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(RED + message + RESET);
      console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
        if (orders.length > 0) {
          const cancelData = orders.map((order) => {
            const payload = JSON.parse(order.orderHash)

            return {
              name: CANCEL_BLUR_BID,
              data: { privateKey, payload, taskId: _id, orderKey: order._id }
            }
          })
          queue.addBulk(cancelData);
        }
      }

      return;
    } else if (offerEth < minBidPriceEth) {
      traitOffer = BigInt(Math.round(minBidPriceEth * 1e18));
    }


    await bidOnBlur(_id, bidCount, address, privateKey, contractAddress, traitOffer, slug, expiry, trait, data.batchInfo);
  } catch (error: any) {
    // Handle rate limit errors
    if (error?.code === 'RATE_LIMITED') {
      console.log(`[Blur] Rate limited for trait bid ${data.slug} ${data.trait} - will retry`);
      throw error; // Re-throw to let worker handle retry with delay
    }

    // Handle transfer restriction errors
    if (error.code === 'TRANSFER_RESTRICTED') {
      console.log(`Skipping Blur trait bid for ${data.slug} - transfer restricted collection`);
      await logBidError(data.task._id, "BLUR TRANSFER RESTRICTION", error.message, "warning", "blur");
      return;
    }

    console.error('Blur trait bid error:', error instanceof Error ? error.message : String(error));

    // Don't log bid errors for stopped/cancelled tasks
    const errorMessage = error?.message || String(error);
    if (!errorMessage.includes('Bid execution blocked - task') && !errorMessage.includes('is stopped')) {
      await logBidError(data.task._id, "BLUR TRAIT BID ERROR", `Error bidding on trait ${data.trait} in collection ${data.slug}: ${error?.response?.data?.message || error?.response?.data || errorMessage}`, "error", "blur");
    }

    // Throw the error
    throw error;
  }
}

async function processMagicedenScheduledBid(task: ITask, jobId?: string, scheduleJob?: Job) {
  const currentLoopCount = loopCounterStats[task._id.toString()] || 1;
  console.log(`${BLUE}[processMagicedenScheduledBid] CALLED for task ${task._id} - Loop #${currentLoopCount}${RESET}`);

  // Log if this appears to be a retry (job has an ID and attempt number)
  if (scheduleJob && scheduleJob.attemptsMade > 0) {
    console.log(`${YELLOW}[processMagicedenScheduledBid] This is retry attempt ${scheduleJob.attemptsMade + 1} for job ${scheduleJob.id}${RESET}`);
  }

  const checkpointStart = Date.now();
  const taskId = task._id.toString();
  let lockAcquired = false;

  try {
    // Early return consolidation
    if (!enabledMarketplaces.map(m => m.toLowerCase()).includes("magiceden")) {
      console.log(`${YELLOW}[processMagicedenScheduledBid] MagicEden not in enabled marketplaces, returning${RESET}`);
      return;
    }

    const currentTask = activeTasks.get(task._id);

    if (!currentTask?.running || !currentTask.selectedMarketplaces.map(m => m.toLowerCase()).includes("magiceden")) {
      console.log(`${YELLOW}[processMagicedenScheduledBid] Task not running or MagicEden not selected, returning${RESET}`);
      return;
    }

    // Get preprocessing data - first check the Map, then perform if needed
    let preprocessing = taskPreprocessing.get(taskId);

    if (!preprocessing || !preprocessing.magiceden.enabled) {
      console.log(`${MAGENTA}[processMagicedenScheduledBid] No preprocessing data found, performing preprocessing now${RESET}`);

      // Determine available marketplaces for this task
      const validMarketplaces = ["opensea", "magiceden", "blur"];
      const availableMarketplaces = currentTask.selectedMarketplaces
        .map(m => m.toLowerCase())
        .filter(m => enabledMarketplaces.map(em => em.toLowerCase()).includes(m))
        .filter(m => validMarketplaces.includes(m));

      // Perform preprocessing for MagicEden
      preprocessing = await performMarketplacePreprocessing(currentTask, "magiceden", availableMarketplaces);

      // Store in Map for other marketplaces to use
      taskPreprocessing.set(taskId, preprocessing);

      // If preprocessing didn't enable MagicEden, it means something went wrong
      if (!preprocessing.magiceden.enabled) {
        throw new Error(`Preprocessing failed to enable MagicEden for task ${taskId}`);
      }
    }

    if (preprocessing.magiceden.skipReason) {
      console.log(`${YELLOW}[MAGICEDEN] Skipped: ${preprocessing.magiceden.skipReason}${RESET}`);
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
      taskStatus[taskId] = {
        status: 'PROCESSING',
        runningPercentage: 0,
        cancellationPercentage: 0,
        processingPercentage: 0,
        totalJobs: 0,
        completedJobs: 0,
        waitingTime: 0
      };
    }

    // Acquire lock for this task - will throw if stale or already locked
    // Use timed lock manager for better timeout handling
    lockAcquired = timedLockManager.acquireLock(taskId, 'magiceden');
    if (!lockAcquired) {
      console.log(`${YELLOW}[LOCK] Task ${taskId} is already being processed by MagicEden scheduled bid - skipping${RESET}`);
      return;
    }
    console.log(`${GREEN}[LOCK] Acquired lock for task ${taskId} in MagicEden scheduled bid${RESET}`);

    if (!currentTokenIds[taskId]) {
      currentTokenIds[taskId] = {
        opensea: [],
        magiceden: [],
      }
    }
    currentTokenIds[taskId].magiceden = preprocessing.magiceden.tokens;

    const WALLET_ADDRESS: string = currentTask.wallet;
    const wallets = await walletCache.getWalletById(currentTask.walletId);
    const WALLET_PRIVATE_KEY: string = decryptPrivateKey(wallets?.encryptedPrivateKey as string)

    // Extend lock before fetching balances
    if (lockAcquired) {
      timedLockManager.extendLock(taskId, 'magiceden');
    }

    // Fetch fresh WETH balance with timeout protection
    console.log(`${MAGENTA}[MagicEden] Fetching WETH balance for ${WALLET_ADDRESS}...${RESET}`);
    let wethBalance = 0;
    try {
      const { fetchERC20BalanceFresh } = await import('./utils/balance');
      const { WETH } = await import('./utils/currencies');
      wethBalance = await fetchERC20BalanceFresh(WALLET_ADDRESS, WETH);
      console.log(`${MAGENTA}[MagicEden] Fresh WETH balance for ${WALLET_ADDRESS}: ${wethBalance}${RESET}`);
    } catch (error) {
      console.error(`${RED}[MagicEden] Failed to fetch WETH balance: ${error}${RESET}`);
      console.log(`${YELLOW}[MagicEden] Continuing with balance: 0.00${RESET}`);
    }

    console.log(`${MAGENTA}[MagicEden] Processing for collection ${currentTask.contract.slug}...${RESET}`);
    const contractAddress = currentTask.contract.contractAddress
    const expiration = getExpiry(currentTask.bidDuration)
    const newTask = { ...currentTask, traits: {} } as ITask

    // Note: Status will be set to appropriate state after job creation is determined

    console.log(`${MAGENTA}[MagicEden] Checking preprocessing results:${RESET}`);
    console.log(`${MAGENTA}[MagicEden] - tokens.length: ${preprocessing.magiceden.tokens.length}${RESET}`);
    console.log(`${MAGENTA}[MagicEden] - traits.length: ${preprocessing.magiceden.traits.length}${RESET}`);
    console.log(`${MAGENTA}[MagicEden] - jobCount: ${preprocessing.magiceden.jobCount}${RESET}`);

    // Create jobs based on preprocessing results
    if (preprocessing.magiceden.tokens.length > 0) {
      // Token bid
      console.log(`${MAGENTA}[MAGICEDEN] Creating ${preprocessing.magiceden.tokens.length} token bid jobs${RESET}`);

      // Ensure parent job exists BEFORE processing jobs
      await ensureParentJobExists(taskId, 'magiceden');
      const parentJobId = getParentJobId(taskId, 'magiceden');
      const parentJob = parentJobs.get(parentJobId);
      if (parentJob) {
        parentJob.expectedJobs = preprocessing.magiceden.tokens.length;
        parentJob.totalJobs = preprocessing.magiceden.tokens.length; // Use expected count initially
        console.log(`${MAGENTA}[MAGICEDEN] Set parent job ${parentJobId} expectedJobs: ${parentJob.expectedJobs}${RESET}`);
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
            privateKey: WALLET_PRIVATE_KEY,
            slug: currentTask.contract.slug,
            tokenId: token,
            outbidOptions: currentTask.outbidOptions,
            task: newTask,
          },
          opts: {
            priority: TOKEN_BID_PRIORITY.MAGICEDEN,
          } as JobsOptions
        })) as Job[]

      try {
        // Extend lock before processing bulk jobs
        if (lockAcquired) {
          timedLockManager.extendLock(taskId, 'magiceden');
        }

        // Transition to QUEUEING state before creating jobs
        setTaskStatus(taskId, 'QUEUEING', 'queueing magiceden token bid jobs');

        // Fire-and-forget: processBulkJobs runs asynchronously to prevent timeout
        processBulkJobs(jobs, true, scheduleJob);

        // Register for job creation verification
        registerJobCreationVerification(taskId, 'magiceden', preprocessing.magiceden.tokens.length);

        console.log(MAGENTA + `[MAGICEDEN] Started queueing ${preprocessing.magiceden.tokens.length} token bid jobs` + RESET);

        // The task will transition to RUNNING when jobs start processing

      } catch (jobError) {
        console.error(RED + `[MAGICEDEN] Failed to queue token bid jobs: ${jobError}` + RESET);

        // Reset parent job on failure
        const parentJobId = getParentJobId(taskId, 'magiceden');
        const parentJob = parentJobs.get(parentJobId);
        if (parentJob) {
          parentJob.actualJobs = 0;
          parentJob.allJobsQueued = true; // Mark as queued even on failure
          parentJob.totalJobs = 0;
          console.log(RED + `[MAGICEDEN] Marked parent job ${parentJobId} as complete with 0 jobs due to queueing failure` + RESET);
        }

        // Update task status to reflect failure
        setTaskStatus(taskId, 'PROCESSING', 'job queueing failed');

        // Re-throw to trigger retry
        throw jobError;
      }
    }
    else if (preprocessing.magiceden.traits.length > 0) {
      // Trait bid
      console.log(`${MAGENTA}[MAGICEDEN] Creating ${preprocessing.magiceden.traits.length} trait bid jobs${RESET}`);

      // Ensure parent job exists BEFORE processing jobs
      await ensureParentJobExists(taskId, 'magiceden');
      const parentJobId = getParentJobId(taskId, 'magiceden');
      const parentJob = parentJobs.get(parentJobId);
      if (parentJob) {
        parentJob.expectedJobs = preprocessing.magiceden.traits.length;
        parentJob.totalJobs = preprocessing.magiceden.traits.length; // Use expected count initially
        console.log(`${MAGENTA}[MAGICEDEN] Set parent job ${parentJobId} expectedJobs: ${parentJob.expectedJobs}${RESET}`);
      }

      const traitJobs = preprocessing.magiceden.traits.map((trait) => ({
        name: MAGICEDEN_TRAIT_BID,
        data: {
          _id: taskId,
          address: WALLET_ADDRESS,
          contractAddress,
          quantity: 1,
          expiration: expiration,
          privateKey: WALLET_PRIVATE_KEY,
          slug: currentTask.contract.slug,
          trait,
          outbidOptions: currentTask.outbidOptions,
          task: newTask,
        },
        opts: { priority: TRAIT_BID_PRIORITY.MAGICEDEN } as JobsOptions
      })) as Job[]

      try {
        // Extend lock before processing bulk trait jobs
        if (lockAcquired) {
          timedLockManager.extendLock(taskId, 'magiceden');
        }

        // Transition to QUEUEING state before creating jobs
        setTaskStatus(taskId, 'QUEUEING', 'queueing magiceden trait bid jobs');

        // Fire-and-forget: processBulkJobs runs asynchronously to prevent timeout
        processBulkJobs(traitJobs, true, scheduleJob);

        // Register for job creation verification
        registerJobCreationVerification(taskId, 'magiceden', preprocessing.magiceden.traits.length);

        console.log(MAGENTA + `[MAGICEDEN] Started queueing ${preprocessing.magiceden.traits.length} trait bid jobs` + RESET);

        // The task will transition to RUNNING when jobs start processing

      } catch (jobError) {
        console.error(RED + `[MAGICEDEN] Failed to queue trait bid jobs: ${jobError}` + RESET);

        // Reset parent job on failure
        const parentJobId = getParentJobId(taskId, 'magiceden');
        const parentJob = parentJobs.get(parentJobId);
        if (parentJob) {
          parentJob.actualJobs = 0;
          parentJob.allJobsQueued = true; // Mark as queued even on failure
          parentJob.totalJobs = 0;
          console.log(RED + `[MAGICEDEN] Marked parent job ${parentJobId} as complete with 0 jobs due to queueing failure` + RESET);
        }

        // Update task status to reflect failure
        setTaskStatus(taskId, 'PROCESSING', 'job queueing failed');

        // Re-throw to trigger retry
        throw jobError;
      }
    }
    else if (preprocessing.magiceden.jobCount === 1) {
      // Collection bid
      console.log(`${MAGENTA}[MAGICEDEN] Creating 1 collection bid job${RESET}`);
      const jobId = `bid-${taskId}-magiceden-${currentTask.contract.slug}-collection`

      // Ensure parent job exists BEFORE adding job
      await ensureParentJobExists(taskId, 'magiceden');
      const parentJobId = getParentJobId(taskId, 'magiceden');
      const parentJob = parentJobs.get(parentJobId);
      if (parentJob) {
        parentJob.expectedJobs = 1;
        parentJob.totalJobs = 1; // Use expected count initially
        console.log(`${MAGENTA}[MAGICEDEN] Set parent job ${parentJobId} expectedJobs: 1${RESET}`);
      }

      // Transition to QUEUEING state before creating job
      setTaskStatus(taskId, 'QUEUEING', 'queueing magiceden collection bid job');

      await queue.add(
        MAGICEDEN_COLLECTION_BID,
        { task: newTask },
        { jobId }
      );

      // Update parent job count after successful job addition - mark as all queued for single job
      if (parentJob) {
        parentJob.actualJobs = 1;
        parentJob.allJobsQueued = true;
        console.log(GREEN + `[MAGICEDEN] Collection bid job added, marking ${parentJobId} as allJobsQueued` + RESET);
      }

      console.log(GREEN + `[MAGICEDEN] Successfully queued 1 collection bid job` + RESET);

      // Transition to RUNNING after job is queued
      setTaskStatus(taskId, 'RUNNING', 'magiceden collection bid job queued');

      // Trigger percentage update now that the job is queued
      await updateTaskRunningPercentage(taskId);
    } else {
      // No jobs to create - create empty parent job and complete the loop
      console.log(YELLOW + `[MAGICEDEN] No jobs to create for task ${taskId} - completing loop with 0 jobs` + RESET);

      await ensureParentJobExists(taskId, 'magiceden');
      const parentJobId = getParentJobId(taskId, 'magiceden');
      const parentJob = parentJobs.get(parentJobId);
      if (parentJob) {
        parentJob.expectedJobs = 0;
        parentJob.totalJobs = 0;
        parentJob.actualJobs = 0;
        parentJob.completedJobs = 0;
        parentJob.allJobsQueued = true;
        console.log(GREEN + `[MAGICEDEN] Marked parent job ${parentJobId} as complete with 0 jobs` + RESET);
      }

      // Update task status to trigger completion flow
      setTaskStatus(taskId, 'RUNNING', 'magiceden no jobs - completing loop');
      await updateTaskRunningPercentage(taskId);

      // Check if all parent jobs are complete to trigger next loop
      const allComplete = checkAllParentJobsComplete(taskId);
      if (allComplete) {
        console.log(GREEN + `[MAGICEDEN] All parent jobs complete for task ${taskId}, triggering completion` + RESET);
        await checkTaskCompletionAndScheduleNext(taskId);
      }

      // Don't throw error - let the loop complete normally
      console.log(YELLOW + `[MAGICEDEN] Loop will complete with 0 jobs and retry in next cycle` + RESET);
    }
  } catch (error) {
    console.error(RED + `Error processing MagicEden scheduled bid for task: ${task._id} (${Date.now() - checkpointStart}ms)` + RESET, error);

    // Log to file
    const logsDir = path.join(process.cwd(), 'logs');
    if (!fs.existsSync(logsDir)) {
      fs.mkdirSync(logsDir, { recursive: true });
    }

    const errorDetails = {
      timestamp: new Date().toISOString(),
      marketplace: 'magiceden',
      taskId: task._id,
      processingTime: Date.now() - checkpointStart,
      error: error instanceof Error ? error.message : String(error),
      stackTrace: error instanceof Error ? error.stack : undefined,
      isTimeout: error instanceof Error && error.message.includes('timeout'),
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
    if (error instanceof Error && error.message.includes('timeout')) {
      console.error(RED + `[MAGICEDEN_SCHEDULE] Timeout details: ${error.message}` + RESET);
    }

    // Re-throw error to trigger job retry
    throw error;
  } finally {
    // Always release lock if acquired
    if (lockAcquired) {
      timedLockManager.releaseLock(taskId, 'magiceden');
      console.log(`${GREEN}[LOCK] Released lock for task ${taskId} in MagicEden scheduled bid${RESET}`);
    }
  }
}

async function processMagicedenCollectionBid(data: { task: ITask }) {
  const taskId = data.task._id;

  // Check if task is blacklisted before processing
  if (isTaskBlacklisted(taskId)) {
    return;
  }

  try {
    if (!enabledMarketplaces.length) {
      return;
    }
    const { task } = data
    const currentTask = activeTasks.get(task._id)
    if (!currentTask || !currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("magiceden") || !enabledMarketplaces.map((m) => m.toLowerCase()).includes("magiceden")) {
      return;
    }

    const expiration = getExpiry(currentTask.bidDuration)

    // Parallel data fetching
    const [wallets, floor_price, orders] = await Promise.all([
      walletCache.getWalletById(task.walletId),
      fetchMagicEdenCollectionStats(task.contract.contractAddress).then(Number),
      Order.find({
        slug: task.contract.slug,
        taskId: task._id,
        marketplace: MAGICEDEN.toLowerCase(),
        identifier: 'collection'
      }).sort({ createdAt: -1 })
    ]);

    // Early return for floor price
    if (!floor_price) return;

    const privateKey: string = decryptPrivateKey(wallets?.encryptedPrivateKey as string)

    if (!floorPrices[task._id]) {
      floorPrices[task._id] = {
        opensea: 0,
        magiceden: floor_price,
        blur: 0
      };
    }

    const { offerPriceEth, maxBidPriceEth, minBidPriceEth, currencySymbol, currency, decimal } = await calculateBidPrice(currentTask, floor_price as number, "magiceden")
    floorPrices[task._id].magiceden = floor_price;
    const stopBid = await stopOption(currentTask, 'magiceden', floor_price, false)

    if (stopBid) {
      if (orders.length > 0) {
        const orderKeys = orders.map(order => order._id)
        const extractedOrderIds = await extractMagicedenOrderHash(orders)
        queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey, orderKeys: orderKeys, taskId: task._id }); // Non-blocking
      }
      return
    }
    const usdcBid = currency.toLowerCase() === USDC.toLowerCase()

    let offerPrice = Math.ceil(offerPriceEth * Math.pow(10, decimal))

    let colletionOffer = (Math.round(offerPrice))


    const [order, ...remainingOrders] = orders
    const ttl = (order?.expiresAt?.getTime() - Date.now()) / 1000
    const currentPrice = Number(order ? order.offerPrice : offerPrice)

    if (remainingOrders.length > 0) {
      const orderIds = remainingOrders.map(order => order._id)
      const extractedOrderIds = await extractMagicedenOrderHash(remainingOrders)
      queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey, orderKeys: orderIds, taskId: task._id }); // Non-blocking
    }

    const createdAt = new Date(order?.createdAt).getTime()
    const expiredAt = new Date(order?.expiresAt).getTime()
    const orderExpiration = order ? Math.round((expiredAt - createdAt) / 1000) : expiration

    if (!task.outbidOptions.outbid) {
      if (ttl >= MIN_BID_DURATION && currentPrice === Number(offerPrice) && Math.abs(orderExpiration - expiration) < 30) {
        return;
      }
    }

    else {
      // Parallel offer fetching when outbidding
      const [[topOffer, secondOffer], walletsArr] = await Promise.all([
        fetchMagicEdenOffer("COLLECTION", task.contract.contractAddress, "{}") as unknown as Promise<OpenseaOfferData[]>,
        walletCache.getAllWallets()
      ]);

      const highestBidAmount = usdcBid ? topOffer.usd * Math.pow(10, decimal) : topOffer.eth * Math.pow(10, decimal)

      const secondTopBidAmount = usdcBid ? secondOffer.usd * Math.pow(10, decimal) : secondOffer.eth * Math.pow(10, decimal)
      const outbidMargin = currentTask.outbidOptions.magicedenOutbidMargin || usdcBid ? 0.01 : 0.0001

      const owner = topOffer.owner.toLowerCase();
      const isOwnBid = walletsArr
        .map(addr => addr.address.toLowerCase())
        .includes(owner);

      if (isOwnBid) {
        const spread = Math.round((Number(highestBidAmount) - Number(secondTopBidAmount)) / Math.pow(10, decimal));
        if (spread > outbidMargin && Number(secondTopBidAmount) > 0) {
          if (orders.length > 0) {
            const orderKeys = orders.map((order: any) => order._id)
            const extractedOrderIds = await extractMagicedenOrderHash(orders)
            queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey, orderKeys, taskId: task._id });
          }
          colletionOffer = Number(secondTopBidAmount) + (outbidMargin * Math.pow(10, decimal))
        }
        else if (ttl > MIN_BID_DURATION) {
          console.log(YELLOW + `Skipping magiceden collection bid for ${task.contract.slug} - we have top bid(${highestBidAmount / Math.pow(10, decimal)} ${currencySymbol}) with ${(ttl / 60).toFixed(2)} minutes remaining`.toUpperCase() + RESET);

          return
        }
        else {
          colletionOffer = highestBidAmount
        }

      }
      else {
        colletionOffer = highestBidAmount + (outbidMargin * Math.pow(10, decimal))
      }
    }
    const offerEth = Number(colletionOffer) / Math.pow(10, decimal);

    if (currencySymbol.toLowerCase() === 'weth' && offerEth > floor_price) {
      const message = `⚠️ Offer price ${offerEth} ${currencySymbol} for ${task.contract.slug} collection bid exceeds floor price ${floor_price} ${currencySymbol} FOR MAGICEDEN.Stopping bid...`.toUpperCase();

      warningBids[task._id] = { opensea: true, magiceden: true, blur: true };
      warningMessage[task._id] = { opensea: message, magiceden: message, blur: message }

      logBidError(task._id, "OFFER EXCEEDS FLOOR PRICE", message, "warning", "magiceden"); // Non-blocking
      console.log(YELLOW + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(YELLOW + message + RESET);
      console.log(YELLOW + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      return;
    }

    const bidCount = getIncrementedBidCount(MAGICEDEN, task.contract.slug, task._id)

    if (orders.length > 0) {
      const orderKeys = orders.map(order => order._id)
      const extractedOrderIds = await extractMagicedenOrderHash(orders)
      queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey, orderKeys: orderKeys, taskId: task._id }); // Non-blocking
    }

    const outbidOptions = currentTask.outbidOptions

    if (outbidOptions.outbid && offerEth > maxBidPriceEth) {
      if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
        if (orders.length > 0) {
          const orderKeys = orders.map((order: any) => order._id)
          const extractedOrderIds = await extractMagicedenOrderHash(orders)
          queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, orderKeys, taskId: task._id }); // Non-blocking
        }
      }
      const message = `❌ Offer price ${offerEth} ${currencySymbol} for ${task.contract.slug} COLLECTION bid exceeds max bid price ${maxBidPriceEth} ${currencySymbol} FOR MAGICEDEN.Skipping ...`.toUpperCase()
      console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(RED + message + RESET);
      console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      logBidError(task._id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "magiceden"); // Non-blocking

      if (!skipStats[task._id]) {
        skipStats[task._id] = {
          opensea: 0,
          magiceden: 0,
          blur: 0
        }
      }
      skipStats[task._id].magiceden++;
      return;
    } else if (offerEth < minBidPriceEth) {
      colletionOffer = minBidPriceEth * Math.pow(10, decimal);
    }

    try {
      await bidOnMagiceden(task._id, bidCount, task.wallet, task.contract.contractAddress, 1, colletionOffer.toString(), privateKey, task.contract.slug, expiration, currency);
    } catch (bidError) {
      console.error(RED + `MagicEden bid failed for task ${task._id}: ${bidError}` + RESET);
      throw bidError; // Re-throw to be caught by outer catch
    }

  } catch (error: any) {

    logBidError(data.task._id, "MAGICEDEN COLLECTION BID ERROR", `Error processing MagicEden collection bid for task: ${data.task._id}: ${error?.response?.data?.message || error?.message || String(error)}`, "error", "magiceden"); // Non-blocking

    // Throw the error
    throw error;
  }
}


async function processMagicedenTokenBid(data: IMagicedenTokenBidData) {
  try {
    const { contractAddress, address, quantity, privateKey, slug, tokenId, task, _id } = data
    if (!enabledMarketplaces.length) return

    // Early return for token validation
    if (!currentTokenIds[_id]?.magiceden?.includes(+tokenId)) {
      return
    }
    const currentTask = activeTasks.get(_id)
    if (!currentTask || !currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("magiceden") || !enabledMarketplaces.map((m) => m.toLowerCase()).includes("magiceden")) {
      return;
    }
    const outbidOptions = currentTask.outbidOptions

    // Parallel operations for floor price and orders
    const [floor_price, orders] = await Promise.all([
      fetchMagicEdenCollectionStats(task.contract.contractAddress).then(Number),
      Order.find({
        slug,
        taskId: _id,
        identifier: tokenId.toString(),
        marketplace: MAGICEDEN.toLowerCase()
      }).sort({ createdAt: -1 })
    ]);

    if (!floor_price) {
      return
    }

    if (!floorPrices[task._id]) {
      floorPrices[task._id] = {
        opensea: 0,
        magiceden: floor_price,
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

    floorPrices[task._id].magiceden = floor_price;

    const { offerPriceEth, maxBidPriceEth, minBidPriceEth, currencySymbol, currency, decimal } = await calculateBidPrice(currentTask, floor_price as number, "magiceden")
    const stopBid = await stopOption(currentTask, 'magiceden', Number(floor_price), false)
    const expiration = getExpiry(currentTask.bidDuration)

    if (stopBid) {
      if (orders.length > 0) {
        const orderKeys = orders.map((order: any) => order._id)
        const extractedOrderIds = await extractMagicedenOrderHash(orders)
        queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey, orderKeys, taskId: _id });
      }
      return
    };
    const usdcBid = currency.toLowerCase() === USDC.toLowerCase()


    let tokenOffer = Number(offerPriceEth * Math.pow(10, decimal))

    const [order, ...remainingOrders] = orders
    const ttl = (order?.expiresAt?.getTime() - Date.now()) / 1000
    const currentPrice = Number(order ? order.offerPrice : tokenOffer)

    if (remainingOrders.length > 0) {
      const extractedOrderIds = await extractMagicedenOrderHash(remainingOrders)
      const orderKeys = remainingOrders.map((order: any) => order._id)
      queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey, orderKeys, taskId: task._id }); // Non-blocking
    }

    const createdAt = new Date(order?.createdAt).getTime()
    const expiredAt = new Date(order?.expiresAt).getTime()
    const orderExpiration = order ? Math.round((expiredAt - createdAt) / 1000) : expiration

    if (!outbidOptions.outbid) {
      if (ttl >= MIN_BID_DURATION && currentPrice === tokenOffer && Math.abs(orderExpiration - expiration) < 30) {
        return;
      }

    } else {
      const walletsArr = await walletCache.getAllWallets()
      const [topOffer, secondOffer] = await fetchMagicEdenOffer("TOKEN", contractAddress, tokenId.toString()) as unknown as OpenseaOfferData[]
      // Add null checks and default values 
      const highestBidAmount = usdcBid ? topOffer.usd * Math.pow(10, decimal) : topOffer.eth * Math.pow(10, decimal)
      const secondTopBidAmount = usdcBid ? secondOffer.usd * Math.pow(10, decimal) : secondOffer.eth * Math.pow(10, decimal)
      const owner = topOffer && typeof topOffer === 'object' ? topOffer.owner?.toLowerCase() || '' : '';
      const isTopBid = order ? +order.offerPrice > highestBidAmount : false
      const isOwnBid = walletsArr
        .map(addr => addr.address.toLowerCase())
        .includes(owner);

      const outbidMargin = currentTask.outbidOptions.magicedenOutbidMargin || usdcBid ? 0.01 : 0.0001

      if (isOwnBid) {
        const spread = Math.round((Number(highestBidAmount) - Number(secondTopBidAmount)) / Math.pow(10, decimal));
        if (spread > outbidMargin && Number(secondTopBidAmount) > 0) {
          if (orders.length > 0) {
            const orderKeys = orders.map((order: any) => order._id)
            const extractedOrderIds = await extractMagicedenOrderHash(orders)
            queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey, orderKeys, taskId: _id }); // Non-blocking
          }
          tokenOffer = Number(secondTopBidAmount) + (outbidMargin * Math.pow(10, decimal))
        } else if (ttl > MIN_BID_DURATION) {
          console.log(YELLOW + `Skipping magiceden token bid for ${slug} ${tokenId} - we have top bid(${highestBidAmount / Math.pow(10, decimal)} ETH) with ${(ttl / 60).toFixed(2)} minutes remaining`.toUpperCase() + RESET);
          return
        } else {
          tokenOffer = highestBidAmount
        }
      }

      else if (isTopBid) {
        const spread = Math.round((Number(order.offerPrice) - Number(highestBidAmount)) / Math.pow(10, decimal));
        if (spread > outbidMargin && Number(highestBidAmount) > 0) {
          if (orders.length > 0) {
            const orderKeys = orders.map((order: any) => order._id)
            const extractedOrderIds = await extractMagicedenOrderHash(orders)
            queue.add(CANCEL_MAGICEDEN_BID, {
              orderIds: extractedOrderIds,
              privateKey,
              orderKeys,
              taskId: _id
            }); // Non-blocking
          }
          tokenOffer = Number(highestBidAmount) + (outbidMargin * Math.pow(10, decimal));
        } else if (ttl > MIN_BID_DURATION) {
          console.log(YELLOW + `Skipping magiceden token bid for ${slug} ${tokenId} - we have top bid(${+ order.offerPrice / Math.pow(10, decimal)} ETH) with ${(ttl / 60).toFixed(2)} minutes remaining`.toUpperCase() + RESET);
          return
        } else {
          tokenOffer = +order.offerPrice
        }
      }

      else {
        tokenOffer = highestBidAmount + (outbidMargin * Math.pow(10, decimal))
      }
    }

    const offerEth = Number(tokenOffer) / Math.pow(10, decimal);
    const bidCount = getIncrementedBidCount(MAGICEDEN, slug, _id)

    if (outbidOptions.outbid && offerEth > maxBidPriceEth) {
      if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
        if (orders.length > 0) {
          const orderKeys = orders.map((order: any) => order._id)
          const extractedOrderIds = await extractMagicedenOrderHash(orders)
          queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey, orderKeys, taskId: _id });
        }
      }
      const message = `❌ Offer price ${offerEth} ${currencySymbol} for ${slug} ${tokenId} bid exceeds max bid price ${maxBidPriceEth} ${currencySymbol} FOR MAGICEDEN.Skipping ...`.toUpperCase()
      console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(RED + message + RESET);
      console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      logBidError(_id, "MAXIMUM BID PRICE EXCEEDED", `Offer price ${offerEth} ${currencySymbol} for ${slug} ${tokenId} bid exceeds max bid price ${maxBidPriceEth} ${currencySymbol} FOR MAGICEDEN.Skipping ...`.toUpperCase(), "skipped", "magiceden");

      skipStats[_id].magiceden++;
      return;
    } else if (offerEth < minBidPriceEth) {
      tokenOffer = minBidPriceEth * Math.pow(10, decimal);
    }

    // Cancel existing orders only when we're actually proceeding with the bid
    if (orders.length > 0) {
      const orderKeys = orders.map((order: any) => order._id)
      const extractedOrderIds = await extractMagicedenOrderHash(orders)
      queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey, orderKeys, taskId: _id });
    }

    await bidOnMagiceden(_id, bidCount, address, contractAddress, quantity, tokenOffer.toString(), privateKey, slug, expiration, currency, undefined, tokenId);
  } catch (error: any) {

    logBidError(data._id, "MAGICEDEN TOKEN BID ERROR", `Error bidding on token ${data.tokenId} in collection ${data.slug}: ${error?.response?.data?.message || error?.message || String(error)}`, "error", "magiceden"); // Non-blocking

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
  privateKey: string,
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
  // Early validation
  if (!enabledMarketplaces.length || !enabledMarketplaces.map((m) => m.toLowerCase()).includes("magiceden")) return

  const task = activeTasks.get(data._id);
  if (!task?.running || taskStatus[data._id]?.status === 'COMPLETED' || taskStatus[data._id]?.status === 'STOPPED') {
    return;
  }

  try {

    const
      { contractAddress, address, quantity, privateKey, slug, trait, _id } = data

    // Skip trait validation - preprocessing already determined valid traits
    // The trait passed in the job data is already validated

    const currentTask = activeTasks.get(_id)
    if (!currentTask || !currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("magiceden")) return;

    const outbidOptions = currentTask.outbidOptions
    const identifier = `${trait.attributeKey}:${trait.attributeValue}`

    // Parallel floor price fetching
    const [floor_price, traitFloor, orders] = await Promise.all([
      fetchMagicEdenCollectionStats(currentTask.contract.contractAddress).then(Number),
      currentTask?.stopOptions?.traitPrices && currentTask?.stopOptions?.triggerStopOptions && identifier in currentTask?.stopOptions?.traitPrices
        ? fetchMagicEdenTraitFloor(contractAddress, { attributeKey: trait.attributeKey, attributeValue: trait.attributeValue })
        : Promise.resolve(undefined),
      Order.find({
        slug,
        taskId: _id,
        identifier: identifier,
        marketplace: MAGICEDEN.toLowerCase()
      }).sort({ createdAt: -1 })
    ]);

    if (!floor_price) return
    const { offerPriceEth, maxBidPriceEth, minBidPriceEth, currencySymbol, currency, decimal } = await calculateBidPrice(currentTask, floor_price as number, "magiceden")

    const stopBid = await stopOption(currentTask, 'magiceden', Number(floor_price), false, traitFloor, identifier)

    if (stopBid) {
      if (orders.length > 0) {
        const orderKeys = orders.map((order: any) => order._id)
        const extractedOrderIds = await extractMagicedenOrderHash(orders)
        queue.add(CANCEL_MAGICEDEN_BID, {
          orderIds: extractedOrderIds,
          privateKey,
          orderKeys,
          taskId: _id
        }); // Non-blocking
      }
      return
    };
    const expiration = getExpiry(currentTask.bidDuration)
    if (!floorPrices[currentTask._id]) {
      floorPrices[currentTask._id] = {
        opensea: 0,
        magiceden: floor_price,
        blur: 0
      };
    }
    floorPrices[currentTask._id].magiceden = floor_price;

    const usdcBid = currency.toLowerCase() === USDC.toLowerCase()
    let traitOffer = Number(offerPriceEth * Math.pow(10, decimal))

    const [order, ...remainingOrders] = orders
    const ttl = (order?.expiresAt?.getTime() - Date.now()) / 1000
    const currrentPrice = Number(order ? order.offerPrice : traitOffer)

    if (remainingOrders.length > 0) {
      const orderKeys = remainingOrders.map((order: any) => order._id)
      const extractedOrderIds = await extractMagicedenOrderHash(remainingOrders)
      queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey, orderKeys, taskId: currentTask._id }); // Non-blocking
    }

    const createdAt = new Date(order?.createdAt).getTime()
    const expiredAt = new Date(order?.expiresAt).getTime()
    const orderExpiration = order ? Math.round((expiredAt - createdAt) / 1000) : expiration

    if (!outbidOptions.outbid) {
      if (ttl >= MIN_BID_DURATION && currrentPrice === traitOffer && Math.abs(orderExpiration - expiration) < 30) {
        return;
      }
    }

    else {
      const walletsArr = await walletCache.getAllWallets()
      const [topOffer, secondOffer] = await fetchMagicEdenOffer("TRAIT", contractAddress, trait) as unknown as OpenseaOfferData[]
      const highestBidAmount = usdcBid ? topOffer.usd * Math.pow(10, decimal) : topOffer.eth * Math.pow(10, decimal)
      const secondTopBidAmount = usdcBid ? secondOffer.usd * Math.pow(10, decimal) : secondOffer.eth * Math.pow(10, decimal)
      const owner = topOffer && typeof topOffer === 'object' ? topOffer.owner?.toLowerCase() : '';
      const isOwnBid = walletsArr.map(addr => addr.address.toLowerCase()).includes(owner)
      const isTopBid = order ? +order.offerPrice > highestBidAmount : false
      const outbidMargin = currentTask.outbidOptions.magicedenOutbidMargin || usdcBid ? 0.01 : 0.0001

      if (isOwnBid) {
        const spread = Math.round((Number(highestBidAmount) - Number(secondTopBidAmount)) / Math.pow(10, decimal));
        if (spread > outbidMargin && Number(secondTopBidAmount) > 0) {
          if (orders.length > 0) {
            const orderKeys = orders.map((order: any) => order._id)
            const extractedOrderIds = await extractMagicedenOrderHash(orders)
            queue.add(CANCEL_MAGICEDEN_BID, {
              orderIds: extractedOrderIds,
              privateKey,
              orderKeys,
              taskId: _id
            }); // Non-blocking
          }
          traitOffer = Number(secondTopBidAmount) + (outbidMargin * Math.pow(10, decimal));
        } else if (ttl > MIN_BID_DURATION) {
          console.log(YELLOW + `Skipping magiceden token bid for ${slug} ${JSON.stringify(trait)} - we have top bid(${highestBidAmount / Math.pow(10, decimal)} ETH) with ${(ttl / 60).toFixed(2)} minutes remaining`.toUpperCase() + RESET);
          return
        } else {
          traitOffer = highestBidAmount
        }
      }
      else if (isTopBid) {
        const spread = Math.round((Number(order.offerPrice) - Number(highestBidAmount)) / Math.pow(10, decimal));
        if (spread > outbidMargin && Number(highestBidAmount) > 0) {
          if (orders.length > 0) {
            const orderKeys = orders.map((order: any) => order._id)
            const extractedOrderIds = await extractMagicedenOrderHash(orders)
            queue.add(CANCEL_MAGICEDEN_BID, {
              orderIds: extractedOrderIds,
              privateKey,
              orderKeys,
              taskId: _id
            }); // Non-blocking
          }
          traitOffer = Number(highestBidAmount) + (outbidMargin * Math.pow(10, decimal));
        } else if (ttl > MIN_BID_DURATION) {
          console.log(YELLOW + `Skipping magiceden token bid for ${slug} ${JSON.stringify(trait)} - we have top bid(${+ order.offerPrice / Math.pow(10, decimal)} ETH) with ${(ttl / 60).toFixed(2)} minutes remaining`.toUpperCase() + RESET);
          return
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

    if (orders.length > 0) {
      const orderKeys = orders.map((order: any) => order._id)
      const extractedOrderIds = await extractMagicedenOrderHash(orders)
      queue.add(CANCEL_MAGICEDEN_BID, {
        orderIds: extractedOrderIds,
        privateKey,
        orderKeys,
        taskId: _id
      }); // Non-blocking
    }
    if (outbidOptions.outbid && offerEth > maxBidPriceEth) {
      if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
        if (orders.length > 0) {
          const orderKeys = orders.map((order: any) => order._id)
          const extractedOrderIds = await extractMagicedenOrderHash(orders)
          queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey, orderKeys, taskId: _id });
        }
      }
      const message = `❌ Offer price ${offerEth} ${currencySymbol} for ${slug} ${JSON.stringify(trait)} bid exceeds max bid price ${maxBidPriceEth} ${currencySymbol} FOR MAGICEDEN.Skipping ...`.toUpperCase()
      console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(RED + message + RESET);
      console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
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
      await bidOnMagiceden(_id, bidCount, address, contractAddress, quantity, traitOffer.toString(), privateKey, slug, expiration, currency, trait)
    } catch (bidError) {
      console.error(RED + `MagicEden trait bid failed for task ${_id}: ${bidError}` + RESET);
      throw bidError; // Re-throw to be caught by outer catch
    }
  } catch (error: any) {
    // Don't log bid errors for stopped/cancelled tasks
    const errorMessage = error?.message || String(error);
    if (!errorMessage.includes('Bid execution blocked - task') && !errorMessage.includes('is stopped')) {
      // Record error stats
      if (!errorStats[data._id]) {
        errorStats[data._id] = { opensea: 0, magiceden: 0, blur: 0 };
      }
      // Error tracking is now handled in queue event handlers

      logBidError(data._id, "MAGICEDEN TRAIT BID ERROR", `Error bidding on trait ${JSON.stringify(data.trait)} in collection ${data.slug}: ${error?.response?.data?.message || error?.response?.data || errorMessage}`, "error", "magiceden"); // Non-blocking

      // Throw the error
      throw error;
    }
  }
}

async function bulkCancelOpenseaBid(data: { privateKey: string, orderId: string, taskId: string, orderKey: string }) {
  const { privateKey, orderId, taskId, orderKey } = data

  try {
    await Promise.all([
      Order.deleteMany({
        _id: orderKey
      }),
      cancelOrder(orderId, OPENSEA_PROTOCOL_ADDRESS, privateKey, taskId)
    ])

    // Cancellation tracking is now handled in cancelOrder directly
  } catch (error) {
    // Log detailed error information
    const errorMessage = error instanceof Error ? error.message : String(error);
    console.error(RED + `[ERROR - bulkCancelOpenseaBid - Task: ${taskId}] Failed to cancel OpenSea order ${orderId}: ${errorMessage}` + RESET);

    // Log additional error details if available
    if (error instanceof Error && 'response' in error) {
      const responseError = error as any;
      if (responseError.response?.data) {
        console.error(RED + `[ERROR - bulkCancelOpenseaBid - Task: ${taskId}] Response data: ${JSON.stringify(responseError.response.data)}` + RESET);
      }
    }

    throw error; // Re-throw to maintain existing behavior
  }
}

async function bulkCancelMagicedenBid(data: { orderIds: string[], privateKey: string, orderKeys: string[], taskId: string, batchSize?: number }) {
  const { orderIds, privateKey, orderKeys, taskId } = data

  try {
    if (orderIds.length > 0) {
      await Promise.all([
        cancelMagicEdenBid(orderIds, privateKey, taskId),
        Order.deleteMany({
          _id: { $in: orderKeys }
        })
      ])
    }

    // Cancellation tracking is now handled in cancelMagicEdenBid directly
  } catch (error) {
    console.error(RED + `Error processing Magiceden bid cancellation for task: ${taskId}` + RESET, error);

    throw error; // Re-throw to maintain existing behavior
  }
}

async function validateBidCount() {
  // Early exit if no tasks
  if (currentTasks.length === 0) {
    return;
  }

  try {
    const taskIds = currentTasks.map(task => task._id);
    const now = new Date();

    // Batch process all tasks in a single query using Mongodb's aggregate
    const [orderCounts, expiredOrders] = await Promise.all([
      // Get counts for all tasks grouped by taskId and marketplace
      Order.aggregate([
        {
          $match: {
            taskId: { $in: taskIds },
            expiresAt: { $gt: now }  // Only count non-expired orders
          }
        },
        {
          $group: {
            _id: { taskId: '$taskId', marketplace: '$marketplace' },
            _count: { $sum: 1 }
          }
        }
      ]),

      // Get all expired orders
      Order.find({
        taskId: { $in: taskIds },
        expiresAt: { $lte: now }  // Already expired
      }, '_id')  // Only need _id for deletion
    ]);

    // Process order counts
    const bidStatsMap: { [taskId: string]: { opensea: number; magiceden: number; blur: number } } = {};

    // Initialize all tasks with zero counts
    for (const task of currentTasks) {
      bidStatsMap[task._id] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      };
    }

    // Populate counts from grouped results
    for (const group of orderCounts) {
      const marketplace = group._id.marketplace.toLowerCase();
      const taskId = group._id.taskId;
      if (bidStatsMap[taskId] && (marketplace === 'opensea' || marketplace === 'magiceden' || marketplace === 'blur')) {
        bidStatsMap[taskId][marketplace as 'opensea' | 'magiceden' | 'blur'] = group._count;
      }
    }

    // Update global bidStats
    Object.assign(bidStats, bidStatsMap);

    // Delete expired orders
    if (expiredOrders.length > 0) {
      await Order.deleteMany({
        _id: { $in: expiredOrders.map(o => o._id) }
      });
      console.log(YELLOW + `[validateBidCount] Deleted ${expiredOrders.length} expired orders` + RESET);
    }
  } catch (error: any) {
    // Handle database errors
    console.log(YELLOW + `Order collection doesn't exist in MongoDB.` + RESET);
    // Set all bid stats to 0 when table doesn't exist
    for (const task of currentTasks) {
      bidStats[task._id] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      };
    }
    await logger.logError(ErrorCategory.DATABASE, 'Failed to validate bid counts', error);
  }
}

validateBidCountInterval = setInterval(validateBidCount, STANDARD_INTERVAL);

async function bulkCancelBlurBid(data: BlurCancelPayload) {
  const { orderKey, privateKey, taskId, payload } = data

  try {
    if (!data) return
    await Promise.all([
      Order.deleteMany({
        _id: orderKey
      }),
      cancelBlurBid({ payload, privateKey, taskId })
    ])

    // Cancellation tracking is now handled in cancelBlurBid directly
  } catch (error) {
    console.error(RED + `Error processing Blur bid cancellation for task: ${taskId}` + RESET, error);

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

    const queueCounts = await queue.getJobCounts();
    const metrics = {
      status: "ok",
      timestamp: new Date().toISOString(),
      uptime: process.uptime(),
      passwordSet: !!getPassword(),
      components: {
        redis: redis && redis.status === "ready" ? "ok" : "error",
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
        blurRateLimits: blurRateLimiter.getStatus()
      }
    };
    res.json(metrics);
  } catch (error: any) {
    console.error("Health check error:", error);
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
    await logger.logApiError('Failed to count traits', {
      endpoint: '/api/traits/count',
      method: 'POST',
      collectionSlug: req.body?.slug,
      error: error,
      errorMessage: err,
      statusCode: error?.response?.status || 500
    })
    res.status(500).json(err)
  }
})

app.get("/api/task/:taskId", async (req, res) => {
  try {
    const { taskId } = req.params;
    const { page = "1", pageSize = "20", marketplace = "" } = req.query;



    // Convert to numbers and validate
    const pageNum = parseInt(page as string, 10) || 1;
    const pageSizeNum = parseInt(pageSize as string, 10) || 20;

    // Limit page size to reasonable values
    const limitedPageSize = Math.min(Math.max(pageSizeNum, 1), 100);

    // Calculate skip value for pagination
    const skip = (pageNum - 1) * limitedPageSize;

    // Build where clause
    const whereClause: any = { taskId };

    // Add marketplace filter if provided
    if (marketplace) {
      // Handle marketplace as an array or single value
      if (Array.isArray(marketplace)) {
        whereClause.marketplace = {
          $in: marketplace.map((m: any) => m.toLowerCase())
        };
      } else if (typeof marketplace === 'string' && marketplace.includes(',')) {
        // Handle comma-separated string
        whereClause.marketplace = {
          $in: marketplace.split(',').map(m => m.trim().toLowerCase())
        };
      } else {
        whereClause.marketplace = (marketplace as string).toLowerCase();
      }
    }

    // Get total count for pagination metadata
    const totalCount = await Order.countDocuments(whereClause);

    // Fetch paginated orders
    const orders = await Order.find(whereClause)
      .sort({ createdAt: -1 })
      .skip(skip)
      .limit(limitedPageSize);

    // Calculate pagination metadata
    const totalPages = Math.ceil(totalCount / limitedPageSize);

    res.status(200).json({
      data: orders,
      pagination: {
        total: totalCount,
        page: pageNum,
        pageSize: limitedPageSize,
        totalPages,
        hasNextPage: pageNum < totalPages,
        hasPrevPage: pageNum > 1
      }
    });
  } catch (error: any) {
    console.error("Error fetching task:", error);
    res.status(500).json({
      status: "error",
      message: "Failed to fetch task details",
      error: error.message
    });
  }
});

app.post('/api/task/cancel/:taskId', async (req, res) => {
  const startTime = Date.now();
  try {
    const { taskId } = req.params;

    await logger.logInfo(ErrorCategory.API, 'Task cancellation initiated', {
      endpoint: '/api/task/cancel/:taskId',
      method: 'POST',
      taskId
    });
    const task = activeTasks.get(taskId)
    if (!task) {
      return res.status(404).json({ error: 'Task not found' });
    }
    const wallet = await walletCache.getWalletById(task.walletId)
    const WALLET_PRIVATE_KEY: string = decryptPrivateKey(wallet?.encryptedPrivateKey as string)
    const orders: any[] = req.body
    const openseaOrders = orders.filter((order: any) => order.marketplace === OPENSEA.toLowerCase())
    const magicedenOrders = orders.filter((order: any) => order.marketplace === 'magiceden')
    const blurOrders = orders.filter((order: any) => order.marketplace === 'blur')

    if (openseaOrders.length > 0) {
      const cancelData = openseaOrders.map((order) => {
        return {
          name: CANCEL_OPENSEA_BID,
          data: { privateKey: WALLET_PRIVATE_KEY, orderId: order.orderHash, taskId: task._id, orderKey: order._id }
        }
      })
      queue.addBulk(cancelData); // Non-blocking
    }

    if (magicedenOrders.length > 0) {
      const extractedOrderIds = await extractMagicedenOrderHash(magicedenOrders)
      const orderKeys = magicedenOrders.map((order) => order._id)
      queue.add(CANCEL_MAGICEDEN_BID, {
        orderIds: extractedOrderIds,
        privateKey: WALLET_PRIVATE_KEY,
        orderKeys,
        taskId: task._id
      });
    }

    if (blurOrders.length > 0) {
      const blurCancelData = blurOrders.map((order) => {
        return {
          name: CANCEL_BLUR_BID,
          data: { privateKey: WALLET_PRIVATE_KEY, orderId: order.orderHash, taskId: task._id, orderKey: order._id }
        }
      })
      queue.addBulk(blurCancelData)
    }

    res.status(200).json({ message: 'Task cancelled' });

    await logger.logInfo(ErrorCategory.API, 'Task cancelled successfully', {
      endpoint: '/api/task/cancel/:taskId',
      method: 'POST',
      taskId,
      responseTime: Date.now() - startTime,
      cancelledOrders: {
        opensea: openseaOrders.length,
        magiceden: magicedenOrders.length,
        blur: blurOrders.length
      }
    });
  } catch (error: any) {
    await logger.logApiError('Failed to cancel task', {
      endpoint: '/api/task/cancel/:taskId',
      method: 'POST',
      taskId: req.params.taskId,
      responseTime: Date.now() - startTime,
      error: error,
      errorMessage: error?.message || 'Failed to update task',
      statusCode: 500
    })
    res.status(500).json({ error: 'Failed to update task' });
  }
})

// Fetch floor prices for all tasks and all marketplaces
app.post('/api/floor', async (req, res) => {
  try {
    const { tasks }: { tasks: { _id: string, slug: string, contractAddress: string }[] } = req.body;
    await pQueue.addAll(tasks.map(task => async () => {
      const taskId = task._id;
      const slug = task.slug;
      const contractAddress = task.contractAddress;

      // Validate taskId exists
      if (!taskId) {
        console.warn(`Skipping task with missing _id: ${JSON.stringify(task)}`);
        return;
      }

      if (!floorPrices[taskId]) {
        floorPrices[taskId] = {
          opensea: 0,
          blur: 0,
          magiceden: 0
        };
      }

      if (!bestOffers[taskId]) {
        bestOffers[taskId] = {
          opensea: 0,
          blur: 0,
          magiceden: 0
        };
      }

      // OpenSea
      try {
        const openseaFloorPrice = await getOpenseaFloor(slug);
        floorPrices[taskId]['opensea'] = openseaFloorPrice;
        const [topOffer] = (await fetchOpenseaOffers('COLLECTION', slug, contractAddress, {}) as unknown) as OpenseaOfferData[]
        const topOfferAmount = Number(topOffer?.eth) || 0
        bestOffers[taskId]['opensea'] = topOfferAmount
      } catch (error: any) {
        console.warn(`Failed to fetch OpenSea data for ${slug}: ${error?.message || error}`);
        floorPrices[taskId]['opensea'] = 0;
        bestOffers[taskId]['opensea'] = 0;
      }

      // Blur
      try {
        const blurFloorPrice = await fetchBlurCollectionStats(slug);
        floorPrices[taskId]['blur'] = blurFloorPrice;
        const bestOffer = await fetchBlurBid(contractAddress, "COLLECTION", {})
        const bestOfferAmount = bestOffer?.priceLevels?.[0]?.price ? Number(bestOffer.priceLevels[0].price) : 0
        bestOffers[taskId]['blur'] = bestOfferAmount
      } catch (error: any) {
        console.warn(`Failed to fetch Blur data for ${slug}: ${error?.message || error}`);
        floorPrices[taskId]['blur'] = 0;
        bestOffers[taskId]['blur'] = 0;
      }

      // Magic Eden
      try {
        const magicEdenFloorPrice = await fetchMagicEdenCollectionStats(contractAddress);
        floorPrices[taskId]['magiceden'] = magicEdenFloorPrice;

        const [bestOffer] = await fetchMagicEdenOffer("COLLECTION", contractAddress, "{}") as unknown as OpenseaOfferData[]
        const bestOfferAmount = Number(bestOffer?.eth) || 0
        bestOffers[taskId]['magiceden'] = bestOfferAmount;
      } catch (error: any) {
        console.warn(`Failed to fetch Magic Eden data for ${contractAddress}: ${error?.message || error}`);
        floorPrices[taskId]['magiceden'] = 0;
        bestOffers[taskId]['magiceden'] = 0;
      }
    }));
    res.status(200).json({ floorPrices, bestOffers });
  } catch (error: any) {
    await logger.logApiError('Failed to fetch floor prices', {
      endpoint: '/api/floor',
      method: 'POST',
      taskCount: req.body?.tasks?.length || 0,
      error: error,
      errorMessage: error?.message || 'Failed to fetch floor prices',
      statusCode: 500
    })
    res.status(500).json({ error: 'Failed to fetch floor prices' });
  }
});


app.post('/api/collection', async (req, res) => {
  try {
    const slug = req.body.slug as string;

    if (!slug) {
      return res.status(400).json({
        status: "error",
        message: "Collection slug is required"
      });
    }

    const { collectionResult, magicEdenResult } = await fetchCollectionData(slug);

    const collection: CollectionData = collectionResult.data;
    const magicEdenData: MagicEdenCollectionsResponse = magicEdenResult.data;

    const collectionAddress =
      magicEdenData?.collections[0]?.chainData?.contract;

    // Handle case where both API calls fail
    if (!collection && !magicEdenData) {
      return res.status(404).json({ error: "Unable to fetch collection data" });
    }

    const slugValid =
      (collection?.collection_offers_enabled ?? false) ||
      typeof collectionAddress === "string";
    const contractAddress =
      collection?.contracts[0]?.address || collectionAddress;

    const data = {
      ...collection,
      slugValid,
      contractAddress,
    };

    if (slugValid) {
      return res.status(200).json(data);
    } else {
      return res.status(400).json({ error: "Collection offers not enabled" });
    }
  } catch (error: any) {
    await logger.logApiError('Failed to fetch collection details', {
      endpoint: '/api/collection',
      method: 'POST',
      collectionSlug: req.body?.slug,
      error: error,
      errorMessage: error instanceof Error ? error.message : "Unknown error",
      statusCode: error?.response?.status || 500
    })

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


    // Wrap each task to handle failures gracefully
    const tasks: (() => Promise<unknown>)[] = [
      async () => {
        try {
          return await fetchBlurCollectionStats(slug);
        } catch (error) {
          console.error('Failed to fetch Blur collection stats:', error);
          return 0;
        }
      },
      async () => {
        try {
          return await fetchMagicEdenCollectionStats(contractAddress);
        } catch (error) {
          console.error('Failed to fetch MagicEden collection stats:', error);
          return 0;
        }
      },
      async () => {
        try {
          return await getOpenseaFloor(slug);
        } catch (error) {
          console.error('Failed to fetch OpenSea floor:', error);
          return 0;
        }
      },
      async () => {
        try {
          return await fetchBlurTraits(contractAddress);
        } catch (error) {
          console.error('Failed to fetch Blur traits:', error);
          return { traits: { categories: {}, counts: {} }, blurRaw: null };
        }
      },
      async () => {
        try {
          return await fetchMagicEdenAttributes(contractAddress);
        } catch (error) {
          console.error('Failed to fetch MagicEden attributes:', error);
          return { traits: { categories: {}, counts: {} }, magicedenRaw: null };
        }
      },
      async () => {
        try {
          return await fetchOpenseaTraits(slug);
        } catch (error) {
          console.error('Failed to fetch MagicEden attributes:', error);
          return { traits: { categories: {}, counts: {} }, magicedenRaw: null };
        }
      },
    ];

    const results = await pQueue.addAll(tasks);

    const [
      blurFloorPrice,
      magicedenFloorPrice,
      openseaFloorPrice,
      blurTraits,
      magicEdenTraits,
      openseaTraits
    ] = results

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

    const data = {
      traits: combinedTraits,
      magicEdenValid: Number(magicedenFloorPrice) > 0,
      blurValid: Number(blurFloorPrice) > 0,
      openseaValid: Number(openseaFloorPrice) > 0,
      blurFloorPrice,
      magicedenFloorPrice,
      openseaFloorPrice,
      contractAddress,
      slug
    };

    res.status(200).json(data);
  } catch (error: any) {
    console.error('Unexpected error in collection stats endpoint:', error);
    await logger.logApiError('Failed to process collection stats', {
      endpoint: '/api/collection/stats',
      method: 'POST',
      collectionSlug: req.body?.slug,
      contractAddress: req.body?.address,
      error: error,
      errorMessage: error?.message || 'Failed to process collection stats',
      statusCode: 500
    })
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
    await logger.logApiError('Failed to fetch currency conversion', {
      endpoint: '/api/currency/conversion',
      method: 'POST',
      error: error,
      errorMessage: error?.response?.data || error?.message || 'Failed to fetch currency conversion',
      statusCode: error?.response?.status || 500
    })
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
      console.log(GREEN + '✅ Password set - bidding operations have started' + RESET);
    }

    res.status(200).json({ success: true, message: 'Password stored successfully' });
  } catch (error: any) {
    console.error('Failed to store password:', error);
    await logger.logAuthenticationError('Failed to store password', {
      endpoint: '/api/auth/password',
      method: 'POST',
      error: error,
      errorMessage: error?.message || 'Failed to store password',
      statusCode: 500
    })
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
    console.error('Failed to renew password TTL:', error);
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

    console.log(YELLOW + '🔐 Starting password change and wallet re-encryption...' + RESET);

    // Get all wallets from database
    const wallets = await Wallet.find({ encryptedPrivateKey: { $exists: true, $ne: null } });
    console.log(CYAN + `Found ${wallets.length} wallets to re-encrypt` + RESET);

    let successCount = 0;
    let failedWallets: string[] = [];

    // Re-encrypt each wallet
    for (const wallet of wallets) {
      try {
        console.log(CYAN + `Re-encrypting wallet ${wallet.address}...` + RESET);
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
        console.error(RED + `Failed to re-encrypt wallet ${wallet.address}:` + RESET, error);
        failedWallets.push(wallet.address);
      }
    }

    // Clear the decrypted key cache
    clearDecryptedKeyCache();

    // If all wallets were successfully re-encrypted, update the password
    if (failedWallets.length === 0) {
      await setPassword(newPassword);
      console.log(GREEN + '✅ Password changed successfully and all wallets re-encrypted' + RESET);

      res.status(200).json({
        success: true,
        message: 'Password changed successfully',
        walletsReEncrypted: successCount
      });
    } else {
      console.error(RED + `Failed to re-encrypt ${failedWallets.length} wallets` + RESET);
      res.status(500).json({
        error: 'Failed to re-encrypt some wallets',
        failedWallets,
        successCount
      });
    }

  } catch (error: any) {
    console.error('Failed to change password:', error);
    await logger.logAuthenticationError('Failed to change password', {
      endpoint: '/api/auth/change-password',
      method: 'POST',
      error: error,
      errorMessage: error?.message || 'Failed to change password',
      statusCode: 500
    });
    res.status(500).json({
      error: 'Failed to change password',
      message: error.message
    });
  }
});

// Manual password recovery endpoint for debugging
app.post('/api/auth/recover-password', async (req, res) => {
  try {
    console.log('Manual password recovery requested...');

    // Check Redis connection
    if (!redisClient.isReady()) {
      return res.status(503).json({
        error: 'Redis not connected',
        message: 'Redis connection is not ready'
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
    console.error('Failed to recover password:', error);
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
    console.error('Failed to get Blur rate limits:', error);
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
    console.error('Failed to get Blur rate limit for wallet:', error);
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
    console.error('Failed to reset Blur rate limit:', error);
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

    const redis = redisClient.getClient();
    if (!redis) {
      return res.status(500).json({
        success: false,
        error: 'Redis not available'
      });
    }

    // Find all Blur access token keys
    const keys = await redis.keys('{blurAccessToken}:*');
    let clearedCount = 0;

    if (keys.length > 0) {
      // Delete all tokens
      await redis.del(...keys);
      clearedCount = keys.length;
    }

    console.log(`Cleared ${clearedCount} Blur authentication tokens`);

    res.status(200).json({
      success: true,
      message: `Cleared ${clearedCount} Blur authentication tokens`,
      clearedTokens: clearedCount
    });
  } catch (error: any) {
    console.error('Failed to clear Blur auth tokens:', error);
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
    console.error('Failed to check Blur health:', error);
    res.status(500).json({
      status: 'error',
      error: error.message || 'Failed to check Blur health',
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
      await logger.logSecurityEvent('UNAUTHORIZED_UPDATE_ATTEMPT', 'Unauthorized update attempt', {
        ip: req.ip,
        userAgent: req.headers['user-agent'],
        endpoint: '/api/update-app'
      });
      return res.status(401).json({ error: 'Unauthorized' });
    }

    // Additional security: Only allow updates from the server IP
    const SERVER_IP = process.env.SERVER_IP as string;
    if (!SERVER_IP) {
      return res.status(500).json({ error: 'Server IP not configured' });
    }

    // Rate limiting check - prevent abuse
    const rateLimitKey = `update_app_${req.ip}`;
    const redis = redisClient.getClient();
    if (redis) {
      const attempts = await redis.incr(rateLimitKey);
      if (attempts === 1) {
        await redis.expire(rateLimitKey, 3600); // 1 hour expiry
      }
      if (attempts > 3) { // Max 3 update attempts per hour
        await logger.logSecurityEvent('RATE_LIMIT_EXCEEDED', 'Update rate limit exceeded', {
          ip: req.ip,
          attempts,
          endpoint: '/api/update-app'
        });
        return res.status(429).json({ error: 'Too many requests' });
      }
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
      return res.status(404).json({ error: 'No update available' });
    }

    // Validate the download URL is from trusted source
    const trustedDomains = ['updates.nfttools.io', 'github.com', 'githubusercontent.com'];
    const urlObj = new URL(update.releaseInfo.downloadUrl);
    if (!trustedDomains.some(domain => urlObj.hostname.includes(domain))) {
      await logger.logSecurityEvent('UNTRUSTED_UPDATE_URL', 'Update from untrusted source blocked', {
        url: update.releaseInfo.downloadUrl,
        endpoint: '/api/update-app'
      });
      return res.status(400).json({ error: 'Invalid update source' });
    }

    const scriptUrl = update.releaseInfo.downloadUrl;

    // Trigger update with timeout
    const { data } = await axios.post(`http://${SERVER_IP}:9999/update`,
      { scriptUrl },
      { timeout: 30000 } // 30 second timeout
    );

    await logger.logSecurityEvent('UPDATE_TRIGGERED', 'Application update triggered successfully', {
      version: update.releaseInfo.version,
      ip: req.ip,
      endpoint: '/api/update-app'
    });

    res.status(200).json(data);
  } catch (error: any) {
    console.error('Failed to schedule update:', error);
    await logger.logApiError('Failed to schedule update', {
      endpoint: '/api/update-app',
      method: 'POST',
      error: error,
      errorMessage: error.message,
      statusCode: error?.response?.status || 500
    });
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
    console.error('Failed to fetch wallet NFTs:', error);
    await logger.logApiError('Failed to fetch wallet NFTs', {
      endpoint: '/api/wallet/nfts',
      method: 'POST',
      error: error,
      errorMessage: error.message,
      statusCode: 500
    })
    res.status(500).json({
      success: false,
      message: 'Failed to fetch wallet NFTs',
      error: error.message
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

    const collectionDetails = await getCollectionDetails(slug)
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
            console.error(`Gas estimation error for ${marketplaceName}:`, gasError);
            gasEstimate = null;
          }
        }

        return {
          approved: isApproved,
          approvalFeeEstimate: gasEstimate,
          contractAddress: marketplaceAddress
        };
      } catch (error: any) {
        console.error(`Approval check error for ${marketplaceName}:`, error);
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
    console.error('Failed to fetch NFT metadata:', error);
    await logger.logApiError('Failed to fetch NFT metadata', {
      endpoint: `/api/collection/${req.params.contractAddress}/${req.params.tokenId}/${req.params.slug}`,
      method: 'GET',
      error: error,
      errorMessage: error.message,
      statusCode: error.response?.status || 500
    });

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
    // Get BullMQ connections
    const bullMQConnections = getBullMQConnections();
    const connectionHealth = await checkBullMQConnectionsHealth();

    // Get main Redis client status
    const mainRedisStatus = {
      connected: redisClient.isReady(),
      client: redisClient.getClient() ? 'initialized' : 'not initialized'
    };

    // Combine results
    const redisHealth = {
      mainClient: mainRedisStatus,
      bullMQConnections: {
        total: bullMQConnections.length,
        connections: bullMQConnections.map(conn => ({
          ...conn,
          healthy: connectionHealth[conn.id]?.healthy || false,
          error: connectionHealth[conn.id]?.error
        }))
      },
      clusterInfo: {
        nodes: JSON.parse(process.env.REDIS_NODES || '[]').length,
        environment: process.env.NODE_ENV || 'development'
      }
    };

    // Determine overall health
    const unhealthyCount = Object.values(connectionHealth).filter(status => !status.healthy).length;
    const isHealthy = mainRedisStatus.connected && unhealthyCount === 0;

    res.status(isHealthy ? 200 : 503).json({
      status: isHealthy ? 'healthy' : 'unhealthy',
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

// Worker health endpoint
app.get('/api/health/workers', async (req, res) => {
  try {
    // Check worker status
    const workerStatuses = await Promise.all(
      workers.map(async (worker, index) => {
        try {
          const isRunning = worker.isRunning();
          const isPaused = worker.isPaused();

          return {
            index,
            name: index < WORKER_COUNT ? `main-worker-${index + 1}` : 'blur-worker',
            running: isRunning,
            paused: isPaused,
            healthy: isRunning && !isPaused
          };
        } catch (error) {
          return {
            index,
            name: index < WORKER_COUNT ? `main-worker-${index + 1}` : 'blur-worker',
            running: false,
            paused: false,
            healthy: false,
            error: error instanceof Error ? error.message : 'Unknown error'
          };
        }
      })
    );

    // Get queue counts
    const [mainQueueCounts, blurQueueCounts] = await Promise.all([
      queue.getJobCounts(),
      blurQueue.getJobCounts()
    ]);

    const healthyWorkers = workerStatuses.filter(w => w.healthy).length;
    const isHealthy = healthyWorkers === workers.length;

    res.status(isHealthy ? 200 : 503).json({
      status: isHealthy ? 'healthy' : 'unhealthy',
      timestamp: new Date().toISOString(),
      workers: {
        total: workers.length,
        healthy: healthyWorkers,
        details: workerStatuses
      },
      queues: {
        main: mainQueueCounts,
        blur: blurQueueCounts
      },
      configuration: {
        workerCount: WORKER_COUNT,
        concurrencyPerWorker: Number(RATE_LIMIT) / WORKER_COUNT
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
    const [redisHealth, workerHealth] = await Promise.all([
      checkBullMQConnectionsHealth(),
      Promise.all(workers.map(w => ({ running: w.isRunning(), paused: w.isPaused() })))
    ]);

    const unhealthyRedis = Object.values(redisHealth).filter(s => !s.healthy).length;
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

// Queue monitoring status endpoint
app.get('/api/queue-monitor/status', async (req, res) => {
  try {
    // Get current queue counts
    const [mainCounts, blurCounts] = await Promise.all([
      queue.getJobCounts(),
      blurQueue.getJobCounts()
    ]);


    const monitorStatus = {
      mainQueue: {
        waiting: mainCounts.waiting || 0,
        active: mainCounts.active || 0,
        completed: mainCounts.completed || 0,
        failed: mainCounts.failed || 0,
        delayed: mainCounts.delayed || 0,
        prioritized: mainCounts.prioritized || 0
      },
      blurQueue: {
        waiting: blurCounts.waiting || 0,
        active: blurCounts.active || 0,
        completed: blurCounts.completed || 0,
        failed: blurCounts.failed || 0,
        delayed: blurCounts.delayed || 0,
        prioritized: blurCounts.prioritized || 0
      }
    };

    res.json(monitorStatus);
  } catch (error) {
    console.error('Error getting queue monitor status:', error);
    res.status(500).json({ error: 'Failed to get queue monitor status' });
  }
});

// Manual queue clear endpoint
app.post('/api/queue-monitor/clear', async (req, res) => {
  try {
    console.log(YELLOW + '\n🔧 Manual queue clear requested via API' + RESET);

    // Clear the queues
    await clearQueueAndRestartJobs();

    res.json({
      success: true,
      message: 'Queue cleared and jobs restarted',
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    console.error('Error during manual queue clear:', error);
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
    console.error('Error fetching timeout metrics:', error);
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
    console.error('Error fetching blur rate limiter status:', error);
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
    console.error('Error fetching blur completion stats:', error);
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

  console.log(`[DEBUG waitingTime] API request for task ${taskId} status`);

  res.status(200).json({
    taskId,
    status: status || null,
    countdownTimerExists,
    timestamp: new Date().toISOString()
  });
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
    console.error('Error in task diagnostics:', error);
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

    console.log(YELLOW + `[Force Complete] Manually forcing completion for task ${taskId}` + RESET);

    // Update status
    if (taskStatus[taskId]) {
      taskStatus[taskId].runningPercentage = 100;
      taskStatus[taskId].status = 'COMPLETED';
    }

    // Trigger completion
    await checkTaskCompletionAndScheduleNext(taskId);

    res.json({
      success: true,
      message: `Task ${taskId} forced to complete`,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    console.error('Error forcing task completion:', error);
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

    console.log(CYAN + `[Manual Recalculation] Recalculating percentage for task ${taskId}` + RESET);

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
        percentage: beforeStatus?.runningPercentage || 0,
        totalJobs: beforeStatus?.totalJobs || 0,
        completedJobs: beforeStatus?.completedJobs || 0
      },
      after: {
        percentage: afterStatus?.runningPercentage || 0,
        totalJobs: afterStatus?.totalJobs || 0,
        completedJobs: afterStatus?.completedJobs || 0
      },
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    console.error('Error recalculating percentage:', error);
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

      allStatuses.push({
        taskId,
        slug: task.contract?.slug || 'unknown',
        running: task.running,
        status: status?.status || 'NO_STATUS',
        percentage: status?.runningPercentage?.toFixed(2) || '0.00',
        totalJobs: status?.totalJobs || 0,
        completedJobs: status?.completedJobs || 0,
        parentJobs: parentJobsInfo,
        hasIssues: status && status.totalJobs > 0 && status.completedJobs === status.totalJobs && status.runningPercentage < 100
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
    console.error('Error getting all task statuses:', error);
    res.status(500).json({ error: 'Failed to get task statuses' });
  }
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
    activeTasksList.push({
      taskId,
      running: task.running,
      loopInterval: task.loopInterval,
      status: taskStatus[taskId]?.status || 'unknown'
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

export async function fetchAddressBalances(walletAddress: string): Promise<AddressBalancesResponse> {
  const CACHE_EXPIRY_SECONDS = 30;
  // Use hash tag for deterministic slot assignment in Redis Cluster
  const cacheKey = `{address_balances:${walletAddress}}`;

  // Check cache first
  const cachedData = redis ? await redis.get(cacheKey) : null;
  if (cachedData) {
    try {
      return JSON.parse(cachedData);
    } catch (error) {
      console.error('Error parsing cached address balances:', error);
    }
  }

  // Use distributed lock to prevent duplicate requests
  const lockManager = new DistributedLockManager({
    lockPrefix: '{address_balances}:lock:',
    defaultTTLSeconds: CACHE_EXPIRY_SECONDS
  });

  const result = await lockManager.withLock(
    `{address_balances}_${walletAddress}`,
    async () => {
      // Check cache again after acquiring lock
      const cachedDataAfterLock = redis ? await redis.get(cacheKey) : null;
      if (cachedDataAfterLock) {
        try {
          return JSON.parse(cachedDataAfterLock);
        } catch (error) {
          console.error('Error parsing cached address balances after lock:', error);
        }
      }

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
        const { data } = await limiter.schedule(() => axiosInstance.post<AddressBalancesResponse>(
          `${BASE_URL}/opensea/graphql`,
          query,
          {
            headers: {
              'content-type': 'application/json',
              'x-api-key': API_KEY
            }
          }
        ));

        // Cache the result
        if (redis) {
          await redis.set(cacheKey, JSON.stringify(data), 'EX', CACHE_EXPIRY_SECONDS);
        }

        return data;
      } catch (error) {
        console.error(RED + "Error fetching address balances:", error + RESET);
        throw error;
      }
    }
  );

  return result;
}


export async function fetchBlurTraits(
  contractAddress: string
): Promise<IBlurData> {
  const url = `${BASE_URL}/blur/v1/traits/${contractAddress}`;
  try {
    const { data } = await limiter.schedule(() => axiosInstance.get(url, {
      headers: { "x-api-key": API_KEY },
    }));
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
    const { data } = await limiter.schedule(() => axiosInstance.get<MagicEdenTraits>(url, {
      headers: {
        accept: "application/json",
        "x-api-key": API_KEY,
      },
    }));

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
    () =>
      limiter.schedule(() => axiosInstance.get<CollectionData>(apiUrl, {
        headers: { "x-api-key": API_KEY },
      })),

    () =>
      limiter.schedule(() => axiosInstance.post(
        `${BASE_URL}/magiceden/v4/collections`,
        {
          chain: "ethereum",
          collectionSlugs: [slug],
        },
        {
          headers: { "x-api-key": API_KEY },
        }
      )),
  ];

  const [collectionResult, magicEdenResult] = await pQueue.addAll(tasks, {
    throwOnTimeout: true,
  });

  return { collectionResult, magicEdenResult };
};

export function getExpiry(bidDuration: { value: number; unit: string }) {
  // Handle invalid or zero values
  if (!bidDuration || bidDuration.value < 0) {
    console.log(YELLOW + `Invalid bid duration value: ${bidDuration?.value}, using default 900 seconds` + RESET);
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
      console.log(`Hard cap of ${hardCapValue} ETH is limiting max bid for ${task.contract.slug} on ${marketplaceName}`);
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

  try {
    // Early return if stop options aren't enabled
    if (!task?.stopOptions?.triggerStopOptions) return false;

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
      if ((traitFloorMin > 0 && traitFloor < traitFloorMin) || (traitFloorMax > 0 && traitFloor > traitFloorMax)) {
        const message = `❌ Trait Floor price ${traitFloor} ETH for ${task.contract.slug} [${trait}] is outside the allowed range (${traitFloorMin} ETH - ${traitFloorMax} ETH). Skipping...`.toUpperCase()
        console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
        console.log(RED + message + RESET);
        console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);

        await logBidError(task._id, "TRAIT FLOOR OUT OF RANGE", message, 'skipped', marketplace)

        if (!counterBid) {
          skipStats[task._id][marketplace]++
        }
        return true
      }
    } else {
      // Check if floor price is outside allowed range
      if ((minFloorPrice > 0 && floorPrice < minFloorPrice) || (maxFloorPrice > 0 && floorPrice > maxFloorPrice)) {
        console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
        console.log(RED + `❌ Floor price ${floorPrice} ETH for ${task.contract.slug} is outside the allowed range (${minFloorPrice} ETH - ${maxFloorPrice} ETH). Skipping...`.toUpperCase() + RESET);
        console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);

        warningBids[task._id][marketplace] = true;
        warningMessage[task._id][marketplace] = `Floor price ${floorPrice} ETH for ${task.contract.slug} is outside the allowed range (${minFloorPrice} ETH - ${maxFloorPrice} ETH).`;
        return true;
      }
    }

    // Check max purchase limit
    const maxPurchase = task?.stopOptions?.maxPurchase;

    // Get the current balance using the new structure
    const walletAddress = task.wallet.toLowerCase();
    const contractAddress = task.contract.contractAddress.toLowerCase();
    const currentBalance = getWalletContractBalance(walletAddress, contractAddress, marketplace);

    if (maxPurchase && maxPurchase > 0 && currentBalance !== undefined && currentBalance >= maxPurchase) {
      // Set warning flags and message
      warningBids[task._id][marketplace] = true;
      warningMessage[task._id][marketplace] =
        `Maximum purchase amount (${maxPurchase}) for ${task.contract.slug} has been reached. ` +
        `Current balance: ${currentBalance} items.`;

      // Log the warning
      console.log(YELLOW + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(YELLOW + `⚠️ Purchase limit reached for ${task.contract.slug} on ${marketplace}. Current balance: ${currentBalance}/${maxPurchase}` + RESET);
      console.log(YELLOW + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      return true;
    }

    // Clear warnings if all checks pass
    warningBids[task._id][marketplace] = false;
    warningMessage[task._id][marketplace] = '';
    return false;

  } catch (error) {
    console.error(`Error in stopOption for ${task?.contract?.slug || 'unknown'} on ${marketplace}:`, error);
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

function subscribeToCollections(tasks: ITask[]) {
  try {
    // console.log(YELLOW + `subscribeToCollections called with ${tasks.length} tasks` + RESET);

    // Always subscribe all tasks to WebSocket
    if (!wsManager?.isConnected()) {
      console.log(YELLOW + `WebSocket not connected, skipping subscription` + RESET);
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
    tasks.forEach((task) => {
      try {
        // Skip if already subscribed to this collection
        if (subscribedCollections.has(task.contract.slug)) {
          return;
        }

        // Always subscribe all tasks to WebSocket
        // Send subscription message directly (server subscribes to ALL marketplaces)
        wsManager.sendMessage({
          event: 'join_the_party',
          clientId: clientId,
          slug: task.contract.slug,
          contractAddress: task.contract.contractAddress
        });

        subscribedCollections.add(task.contract.slug);

        console.log('----------------------------------------------------------------------');
        console.log(`SUBSCRIBED TO COLLECTION: ${task.contract.slug} (ALL MARKETPLACES)`);
        console.log('----------------------------------------------------------------------');

        // Track subscriptions locally in activeSubscriptions for all marketplaces
        const hasOpenseaMarketplace = task.selectedMarketplaces.map(marketplace => marketplace.toLowerCase()).includes("opensea");
        const hasMagiceden = task.selectedMarketplaces.map(marketplace => marketplace.toLowerCase()).includes("magiceden");
        const hasBlur = task.selectedMarketplaces.map(marketplace => marketplace.toLowerCase()).includes("blur");

        if (hasOpenseaMarketplace) {
          activeSubscriptions.add(`${task.contract.slug}:${OPENSEA}`);
        }

        if (hasMagiceden) {
          activeSubscriptions.add(`${task.contract.slug}:${MAGICEDEN}`);
        }

        if (hasBlur) {
          activeSubscriptions.add(`${task.contract.slug}:${BLUR}`);
        }
      } catch (error) {
        console.error(RED + `Error subscribing to collection ${task.contract.slug}:` + RESET, error);
      }
    });

  } catch (error) {
    console.error(RED + 'Error subscribing to collections' + RESET, error);
    // Don't rethrow the error to prevent server crash
  }
}

async function subscribeToListingEvents(contractAddress: string, slug?: string) {
  try {
    if (!wsManager?.isConnected()) {
      console.log(YELLOW + `WebSocket not connected, skipping listing subscription` + RESET);
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

    console.log(CYAN + `SUBSCRIBED TO LISTING EVENTS: ${slug} (${contractAddress})` + RESET);
  } catch (error) {
    console.error(RED + `Error subscribing to listing events for ${contractAddress}:` + RESET, error);
  }
}

async function resubscribeToListingCollections() {
  try {
    console.log(CYAN + `Re-subscribing to listing collections after WebSocket reconnection...` + RESET);

    if (!wsManager?.isConnected()) {
      console.log(YELLOW + `WebSocket not connected, delaying listing re-subscription` + RESET);
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

    console.log(BLUE + `Found ${activeListings.length} active listings with instant price matching` + RESET);

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

        console.log(GREEN + `Re-subscribed to listing events for ${contractSlug} (${contractAddress})` + RESET);
      } catch (error) {
        console.error(RED + `Error re-subscribing to listing ${listing._id}:` + RESET, error);
      }
    }

    console.log(GREEN + `Listing re-subscription complete` + RESET);

    // Check if any listings have been outbid while disconnected
    console.log(YELLOW + `Checking if any listings were outbid during disconnection...` + RESET);
    await processAllActiveListings();
  } catch (error) {
    console.error(RED + 'Error in resubscribeToListingCollections:' + RESET, error);
  }
}

async function processAllActiveListings() {
  try {
    console.log(CYAN + `Processing all active listings...` + RESET);

    // Find all active listings
    const activeListings = await Listing.find({
      running: true
    });

    console.log(BLUE + `Found ${activeListings.length} active listings to process` + RESET);

    // Process each listing
    for (const listing of activeListings) {
      try {
        const listingId = listing._id.toString();
        console.log(YELLOW + `Processing listing: ${listing.contract?.slug || 'Unknown'} (${listingId})` + RESET);

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

        console.log(GREEN + `Added listing ${listing.contract?.slug || 'Unknown'} to processing queue` + RESET);
      } catch (error) {
        console.error(RED + `Error processing listing ${listing._id}:` + RESET, error);
      }
    }

    console.log(GREEN + `All active listings have been queued for processing` + RESET);
  } catch (error) {
    console.error(RED + 'Error in processAllActiveListings:' + RESET, error);
  }
}

/**
 * Unsubscribes from WebSocket collection monitoring for a specific task
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
      await logger.logWarning(ErrorCategory.WEBSOCKET, 'Invalid task provided to unsubscribeFromCollection', {
        task,
        marketplace
      });
      return { success: false, unsubscribed: false, error: 'Invalid task provided' };
    }

    if (!taskId) {
      await logger.logWarning(ErrorCategory.WEBSOCKET, 'Task missing _id property', {
        task,
        marketplace
      });
      return { success: false, unsubscribed: false, error: 'Task missing _id property' };
    }

    if (!contractSlug) {
      await logger.logWarning(ErrorCategory.WEBSOCKET, 'Task missing contract.slug property', {
        taskId,
        marketplace
      });
      return { success: false, unsubscribed: false, error: 'Task missing contract.slug property' };
    }

    if (!contractAddress) {
      await logger.logWarning(ErrorCategory.WEBSOCKET, 'Task missing contract.contractAddress property', {
        taskId,
        contractSlug,
        marketplace
      });
      return { success: false, unsubscribed: false, error: 'Task missing contract.contractAddress property' };
    }

    // Validate marketplace parameter if provided
    if (marketplace && !['opensea', 'magiceden', 'blur'].includes(marketplace.toLowerCase())) {
      await logger.logWarning(ErrorCategory.WEBSOCKET, 'Invalid marketplace parameter', {
        taskId,
        contractSlug,
        marketplace,
        validMarketplaces: ['opensea', 'magiceden', 'blur']
      });
      return { success: false, unsubscribed: false, error: 'Invalid marketplace parameter' };
    }

    // Check WebSocket manager availability
    if (!wsManager) {
      await logger.logWarning(ErrorCategory.WEBSOCKET, 'WebSocket manager not available', {
        taskId,
        contractSlug,
        marketplace
      });
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

      await logger.logDebug(ErrorCategory.WEBSOCKET, 'WebSocket not connected, retrying...', {
        taskId,
        contractSlug,
        marketplace,
        retryCount: retryCount + 1,
        maxRetries
      });

      await new Promise(resolve => setTimeout(resolve, retryDelay));
      retryCount++;
    }

    if (!wsManager.isConnected()) {
      await logger.logWarning(ErrorCategory.WEBSOCKET, 'WebSocket connection failed after retries', {
        taskId,
        contractSlug,
        marketplace,
        maxRetries,
        retryDelay
      });
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

    if (marketplace) {
      await logger.logInfo(ErrorCategory.WEBSOCKET, 'Unsubscribed from collection for specific marketplace', {
        ...logContext,
        scope: 'marketplace_specific'
      });
    } else {
      await logger.logInfo(ErrorCategory.WEBSOCKET, 'Unsubscribed from collection for all marketplaces', {
        ...logContext,
        scope: 'all_marketplaces'
      });
    }

    // Clean up local state - Only remove if unsubscribing from all marketplaces
    // Note: subscribedTasks appears to be unused/incomplete in current implementation
    // Consider removing or properly implementing this tracking
    if (!marketplace && subscribedTasks.has(taskId)) {
      subscribedTasks.delete(taskId);
      await logger.logDebug(ErrorCategory.WEBSOCKET, 'Removed task from subscribedTasks', {
        taskId,
        contractSlug,
        remainingSubscriptions: subscribedTasks.size
      });
    }

    return { success: true, unsubscribed: true };
  } catch (error) {
    await logger.logWebSocketError('Failed to unsubscribe from collection', {
      taskId,
      contractSlug,
      contractAddress,
      marketplace,
      errorMessage: error instanceof Error ? error.message : String(error),
      stackTrace: error instanceof Error ? error.stack : undefined,
      operation: 'websocket_unsubscribe'
    });

    return {
      success: false,
      unsubscribed: false,
      error: error instanceof Error ? error.message : String(error)
    };
  }
}


function checkOpenseaTrait(selectedTraits: any, trait_criteria: any) {
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
  privateKey: string;
  orderKey: string;
  taskId: string;
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
      console.log(`Retrying operation, ${retries} attempts remaining...`);
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

    // Check if approval already exists in database
    const existingApproval = await Approval.findOne({ address, currency });

    // Return early if marketplace is already approved
    if (marketplace.toLowerCase() === SEAPORT.toLowerCase() && existingApproval?.openseaApproval) {
      return true;
    }
    if (marketplace.toLowerCase() === MAGICEDEN.toLowerCase() && existingApproval?.magicedenApproval) {
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

        // Update database with new approval
        const updateData = marketplace.toLowerCase() === SEAPORT.toLowerCase()
          ? { openseaApproval: true }
          : { magicedenApproval: true };

        if (!existingApproval) {
          await Approval.create({
            address,
            currency: currency,
            ...updateData
          });
        } else {
          await Approval.updateOne({ address }, updateData);
        }

        return true;
      } catch (error: any) {
        console.error(`Approval failed for ${marketplace}:`, {
          error: error.message,
          code: error.code,
          reason: error.reason,
          details: error.error || 'No additional details'
        });
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
    console.error('Fatal error in approveMarketplace:', {
      error: error.message,
      code: error.code,
      reason: error.reason,
      details: error.error || 'No additional details'
    });
    return false;
  }
}

export async function logBidError(taskId: string, title: string, message: string, type: "skipped" | "warning" | "error", marketplace: "opensea" | "magiceden" | "blur") {
  try {
    // Create a hash of the message for comparison (to handle long messages)
    const messageHash = crypto.createHash('md5').update(message).digest('hex');

    // Truncate message if it's too long for MongoDB
    const maxMessageLength = 10000; // Safe limit for MongoDB
    const truncatedMessage = message.length > maxMessageLength
      ? message.substring(0, maxMessageLength) + '... (truncated)'
      : message;

    // Try to find an existing log with the same taskId, messageHash, and marketplace
    const existingLog = await BidLogs.findOne({
      taskId,
      messageHash,
      marketplace: marketplace.toLowerCase(),
    });

    if (existingLog) {
      // Update timestamp of existing log
      await BidLogs.updateOne(
        { _id: existingLog._id },
        {
          timestamp: new Date(),
          message: truncatedMessage // Update message in case it was truncated differently
        }
      );
    } else {
      // Create new log if no matching record exists
      const bidLogs = new BidLogs({
        taskId,
        title,
        message: truncatedMessage,
        messageHash, // Store hash for future comparisons
        timestamp: new Date(),
        type: type,
        marketplace: marketplace
      });
      await bidLogs.save();
    }
  } catch (error) {
    console.error(RED + `Error logging bid error: ${error}` + RESET);
  }
}

export function updateTaskProcessingPercentage(taskId: string, percentage: number): void {
  if (!taskStatus[taskId]) {
    return;
  }

  taskStatus[taskId].processingPercentage = percentage;

  // Broadcast the update via WebSocket to all connected clients
  const message = JSON.stringify({
    type: 'taskUpdate',
    payload: {
      taskId,
      status: taskStatus[taskId].status,
      processingPercentage: percentage,
      runningPercentage: taskStatus[taskId].runningPercentage,
      cancellationPercentage: taskStatus[taskId].cancellationPercentage
    }
  });

  clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  });
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
    logger.logWarning(ErrorCategory.GENERAL, 'Invalid taskId provided to markTaskAsAborted', { taskId });
    return false;
  }

  // Check if already aborted
  if (abortedTasks.has(taskId)) {
    logger.logDebug(ErrorCategory.GENERAL, 'Task already marked as aborted', { taskId });
    return false;
  }

  // Mark as aborted with timestamp
  abortedTasks.add(taskId);
  abortedTasksWithTimestamp.set(taskId, Date.now());

  // Log the action
  logger.logInfo(ErrorCategory.GENERAL, 'Task marked as aborted', {
    taskId,
    timestamp: new Date().toISOString(),
    totalAbortedTasks: abortedTasks.size
  });

  return true;
}

function removeTaskFromAborted(taskId: string): boolean {
  if (!taskId || typeof taskId !== 'string') {
    logger.logWarning(ErrorCategory.GENERAL, 'Invalid taskId provided to removeTaskFromAborted', { taskId });
    return false;
  }

  const wasRemoved = abortedTasks.delete(taskId);
  abortedTasksWithTimestamp.delete(taskId);

  if (wasRemoved) {
    logger.logInfo(ErrorCategory.GENERAL, 'Task removed from aborted list', {
      taskId,
      remainingAbortedTasks: abortedTasks.size
    });
  }

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

  if (cleanedCount > 0) {
    logger.logInfo(ErrorCategory.GENERAL, 'Cleaned up expired aborted tasks', {
      cleanedCount,
      remainingCount: abortedTasks.size
    });
  }
}

interface IProcessOpenseaTokenBidData {
  _id: string;
  address: string;
  privateKey: string;
  slug: string;
  creatorFees: IFee;
  offerPrice: number;
  enforceCreatorFee: boolean;
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
  task: ITask;
}

interface IMagicedenTokenBidData {
  _id: string;
  address: string;
  contractAddress: string;
  quantity: number;
  offerPrice?: number;
  expiration: number;
  privateKey: string;
  slug: string;
  tokenId: number;
  outbidOptions: {
    outbid: boolean;
    blurOutbidMargin: number | null;
    openseaOutbidMargin: number | null;
    magicedenOutbidMargin: number | null;
    counterbid: boolean;
  };
  maxBidPriceEth?: number;
  minBidPriceEth?: number;
  task: ITask;
}

// Create separate bid rate states for each marketplace
const bidRateState: {
  [marketplace: string]: {
    bids: Array<{ timestamp: number; count: number }>
  }
} = {
  opensea: { bids: [] },
  magiceden: { bids: [] },
  blur: { bids: [] }
};

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

  return globalBidRates;
}

export function trackBidRate(marketplace: 'opensea' | 'magiceden' | 'blur', taskId: string) {
  const now = Math.floor(Date.now() / 1000);
  const cutoff = now - BID_WINDOW_SECONDS;

  // Add the new bid to the tracking state
  bidRateState[marketplace].bids.push({
    timestamp: now,
    count: 1
  });

  // Consider batch processing - combine bids that happen in the same second
  // This would reduce array size and improve performance for high-volume scenarios
  const bidsGroupedByTimestamp = new Map<number, number>();
  bidRateState[marketplace].bids.forEach(bid => {
    const current = bidsGroupedByTimestamp.get(bid.timestamp) || 0;
    bidsGroupedByTimestamp.set(bid.timestamp, current + bid.count);
  });

  // Convert back to array format but with consolidated entries
  bidRateState[marketplace].bids = Array.from(bidsGroupedByTimestamp.entries())
    .map(([timestamp, count]) => ({ timestamp, count }))
    .filter(bid => bid.timestamp > cutoff);

  // Update the bid rate calculations
  updateBidRate(marketplace, now);

  // Update bid stats for the specific task
  if (!bidStats[taskId]) {
    bidStats[taskId] = {
      opensea: 0,
      magiceden: 0,
      blur: 0
    };
  }
  bidStats[taskId][marketplace]++;

  return { globalBidRates, bidStats };
}

/**
 * Increments memory-based counter for our own bids when we detect we're the maker
 */
export function incrementOwnBidCounter(marketplace: 'opensea' | 'magiceden' | 'blur', taskId: string) {
  try {
    if (!ownBidCounts[marketplace]) {
      ownBidCounts[marketplace] = 0;
    }
    ownBidCounts[marketplace]++;
    return ownBidCounts[marketplace];
  } catch (error) {
    console.error('Failed to increment own bid counter:', error);
  }
  return 0;
}

/**
 * Decrements memory-based counter for our own bids when we cancel a bid
 */
export function decrementOwnBidCounter(marketplace: 'opensea' | 'magiceden' | 'blur', count: number = 1) {
  try {
    // Update local ownBidCounts object for immediate broadcast
    if (ownBidCounts[marketplace] && ownBidCounts[marketplace] > 0) {
      ownBidCounts[marketplace] = Math.max(0, ownBidCounts[marketplace] - count);
      console.log(`🔻 Own bid cancelled - ${marketplace.toUpperCase()}: ${ownBidCounts[marketplace]} total bids remaining`);
      return ownBidCounts[marketplace];
    }
    return 0;
  } catch (error) {
    console.error('Failed to decrement own bid counter:', error);
  }
  return 0;
}

/**
 * Gets Redis-based counter for our own bids
 */
export async function getOwnBidCounter(marketplace: 'opensea' | 'magiceden' | 'blur', taskId: string) {
  try {
    if (redis) {
      const key = `{own_bid_count}:${marketplace}:${taskId}`;
      const count = await redis.get(key);
      return parseInt(count || '0');
    }
  } catch (error) {
    console.error('Failed to get own bid counter:', error);
  }
  return 0;
}

/**
 * Resets memory-based counter for our own bids
 */
export function resetOwnBidCounter(marketplace: 'opensea' | 'magiceden' | 'blur', taskId: string) {
  try {
    // Reset the marketplace counter to 0
    ownBidCounts[marketplace] = 0;

    console.log(`🔄 Reset own bid counter for ${marketplace.toUpperCase()} task ${taskId}`);
  } catch (error) {
    console.error('Failed to reset own bid counter:', error);
  }
}



// Type definitions already moved to the top of the file

// New function to update bid rates - can be called independently
function updateBidRate(marketplace: 'opensea' | 'magiceden' | 'blur', now = Math.floor(Date.now() / 1000)) {
  const bids = bidRateState[marketplace].bids;
  const cutoff = now - BID_WINDOW_SECONDS;

  // Filter out old bids again to ensure we're working with current data
  const recentBids = bids.filter(bid => bid.timestamp > cutoff);

  // Calculate total bids in the window
  const totalBids = recentBids.reduce((sum, bid) => sum + bid.count, 0);

  // Calculate actual time span (in case window isn't full yet)
  let timeSpan = BID_WINDOW_SECONDS;
  if (recentBids.length > 0) {
    const oldestTimestamp = Math.min(...recentBids.map(bid => bid.timestamp));
    timeSpan = Math.max(1, now - oldestTimestamp); // Ensure at least 1 second to avoid division by zero
  }

  // Calculate bids per second
  const bidsPerSecond = totalBids / timeSpan;

  // Update global bid rates
  globalBidRates[marketplace] = {
    bidsPerSecond: parseFloat(bidsPerSecond.toFixed(2)), // Round to 2 decimal places for readability
    totalBids,
    windowPeriod: BID_WINDOW_SECONDS
  };

  // Update bidRateState with filtered bids
  bidRateState[marketplace].bids = recentBids;
}

// New function to periodically update all bid rates
export function updateAllBidRates() {
  const now = Math.floor(Date.now() / 1000);
  ['opensea', 'magiceden', 'blur'].forEach(marketplace => {
    updateBidRate(marketplace as 'opensea' | 'magiceden' | 'blur', now);
  });
  return globalBidRates;
}

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
  walletPrivateKey: string;
  slug: string;
  offerPrice: number;
  creatorFees: IFee;
  enforceCreatorFee: boolean;
  expiry: number;
}

interface IMagicedenCollectionBidData {
  _id: string;
  bidCount: string;
  address: string;
  contractAddress: string;
  quantity: number;
  offerPrice: number;
  expiration: number;
  privateKey: string;
  slug: string;
  currency?: string;
}
interface IBlurBidData {
  taskId: string;
  bidCount: string;
  address: string;
  privateKey: string;
  contractAddress: string;
  offerPrice: number;
  slug: string;
  expiry: number;
  traits?: string
}

interface IProcessOpenseaTraitBidData {
  _id: string;
  bidCount: string;
  address: string;
  privateKey: string;
  slug: string;
  offerPrice: number;
  creatorFees: IFee;
  enforceCreatorFee: boolean;
  expiry: number;
  trait: string;
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
    console.error("Error transforming Magic Eden traits:", error);
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

    // Schedule the listing
    await scheduleListing(listing);

    // Subscribe to WebSocket events if instant price matching is enabled
    console.log(`\n🔍 Checking WebSocket subscription conditions:`);
    console.log(`📋 listing.instantPriceMatching: ${listing.instantPriceMatching}`);
    console.log(`🌐 wsManager exists: ${!!wsManager}`);
    console.log(`📡 wsManager connected: ${wsManager?.isConnected?.() || 'N/A'}`);
    console.log(`🆔 clientId: ${clientId}`);

    if (listing.instantPriceMatching && wsManager) {
      console.log(`\n🔗 Subscribing to listings WebSocket for instant price matching:`);
      console.log(`🏷️  Collection: ${listing.contract.slug}`);
      console.log(`📄 Contract: ${listing.contract.contractAddress}`);

      try {
        // Use proper subscription management with listing type
        await wsManager.subscribe(listing.contract.slug, listing.contract.contractAddress, 'ALL', 'listing');
        console.log(`✅ WebSocket subscription successful`);
      } catch (error) {
        console.error(`❌ WebSocket subscription failed:`, error);
      }
    } else {
      console.log(`❌ WebSocket subscription NOT sent - conditions not met`);
      if (!listing.instantPriceMatching) {
        console.log(`- Instant price matching is disabled`);
      }
      if (!wsManager) {
        console.log(`- WebSocket manager is not available`);
      }
    }

    ws.send(JSON.stringify({
      type: 'listing-started',
      listingId: data._id,
      message: 'Listing started successfully'
    }));

    console.log(GREEN + `Started listing ${data._id}` + RESET);
  } catch (error) {
    console.error('Error starting listing:', error);
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
        console.log(`✅ WebSocket unsubscription successful for cancelled listing`);
      } catch (error) {
        console.error(`❌ WebSocket unsubscription failed for cancelled listing:`, error);
      }
    }

    ws.send(JSON.stringify({
      type: 'listing-stopped',
      listingId: data.listingId,
      message: 'Listing stopped successfully'
    }));

    console.log(RED + `Stopped listing ${listing.contract.slug} #${listing.tokenId}` + RESET);
  } catch (error) {
    console.error('Error stopping listing:', error);
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
        console.log(`\n🔗 Enabling instant price matching - subscribing to listings WebSocket:`);
        console.log(`🏷️  Collection: ${listing.contract.slug}`);
        console.log(`📄 Contract: ${listing.contract.contractAddress}`);

        try {
          // Subscribe to listing events using proper subscription management
          await wsManager.subscribe(listing.contract.slug, listing.contract.contractAddress, 'ALL', 'listing');
          console.log(`✅ WebSocket subscription successful`);
        } catch (error) {
          console.error(`❌ WebSocket subscription failed:`, error);
        }
      } else {
        console.log(`\n🔗 Disabling instant price matching - unsubscribing from listings WebSocket:`);
        console.log(`🏷️  Collection: ${listing.contract.slug}`);

        try {
          // Unsubscribe from listing events
          await wsManager.unsubscribe(listing.contract.slug, listing.contract.contractAddress, 'ALL');
          console.log(`✅ WebSocket unsubscription successful`);
        } catch (error) {
          console.error(`❌ WebSocket unsubscription failed:`, error);
        }
      }
    }

    ws.send(JSON.stringify({
      type: 'listing-updated',
      listingId: data._id,
      message: 'Listing updated successfully'
    }));

    console.log(GREEN + `Updated listing ${data._id}` + RESET);
  } catch (error) {
    console.error('Error updating listing:', error);
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
    console.log(CYAN + '\n========== Job Tracking Status Report ==========' + RESET);

    for (const [taskId, status] of Object.entries(taskStatus)) {
      if (status.status === 'RUNNING' || status.status === 'QUEUED' || status.status === 'QUEUEING') {
        const marketplaces = ['opensea', 'magiceden', 'blur'];
        let totalTracked = 0;
        let completedTracked = 0;
        const parentJobDetails: Record<string, { total: number; completed: number }> = {};

        for (const mp of marketplaces) {
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

        // Log discrepancies or status
        const hasDiscrepancy = status.totalJobs !== totalTracked || status.completedJobs !== completedTracked;

        if (hasDiscrepancy || status.runningPercentage < 100) {
          console.log(`\n${YELLOW}[Task ${taskId}] Status: ${status.status}${RESET}`);
          console.log(`  TaskStatus: ${status.completedJobs}/${status.totalJobs} (${status.runningPercentage?.toFixed(1)}%)`);
          console.log(`  ParentJobs: ${completedTracked}/${totalTracked}`);

          // Show breakdown by marketplace
          for (const [mp, details] of Object.entries(parentJobDetails)) {
            const percentage = details.total > 0 ? (details.completed / details.total * 100).toFixed(1) : '0.0';
            console.log(`    ${mp}: ${details.completed}/${details.total} (${percentage}%)`);
          }

          if (hasDiscrepancy) {
            console.log(RED + '  ⚠️  MISMATCH DETECTED!' + RESET);

            // Check for partial queueing mismatch (0 totalJobs but positive expected)
            if (status.totalJobs === 0 && totalTracked === 0) {
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
                console.log(MAGENTA + '  🔄 Partial queueing detected - will trigger recovery' + RESET);
              }
            }
          }

          // Check if all jobs are complete but percentage is not 100
          if (totalTracked > 0 && completedTracked === totalTracked && status.runningPercentage < 100) {
            console.log(MAGENTA + '  🔍 All parent jobs complete but percentage < 100%' + RESET);
          }

          // Check for stuck task - low percentage with no queue activity
          const queueCounts = await queue.getJobCounts();
          const blurCounts = await blurQueue.getJobCounts();
          const totalQueueJobs = (queueCounts.waiting || 0) + (queueCounts.active || 0) +
            (blurCounts.waiting || 0) + (blurCounts.active || 0);

          if (status.runningPercentage < 10 && totalQueueJobs === 0 && totalTracked > 0) {
            console.log(RED + '  🚨 STUCK TASK DETECTED - Low percentage with empty queue!' + RESET);
            console.log(YELLOW + '  Marking task as complete to allow restart...' + RESET);

            // Force mark all parent jobs as complete to unstick the task
            for (const mp of marketplaces) {
              const parentJobId = `${taskId}-${mp}`;
              const parentJob = parentJobs.get(parentJobId);
              if (parentJob && parentJob.completedJobs < parentJob.totalJobs) {
                console.log(YELLOW + `  Force completing ${parentJobId}: ${parentJob.completedJobs}/${parentJob.totalJobs}` + RESET);
                parentJob.completedJobs = parentJob.totalJobs;
              }
            }

            // Trigger task completion check
            await checkTaskCompletionAndScheduleNext(taskId);
          }
        }
      }
    }

    console.log(CYAN + '================================================\n' + RESET);
  } catch (error) {
    console.error(RED + '[logJobTrackingStatus] Error:' + RESET, error);
  }
}

/**
 * Recovers tasks with partial queueing mismatches
 * This function detects when expectedJobs > actualJobs and triggers task restart
 */
async function recoverPartialQueueingTasks(): Promise<void> {
  try {
    console.log(CYAN + '\n========== Partial Queueing Recovery Check ==========' + RESET);

    for (const [taskId, status] of Object.entries(taskStatus)) {
      if (status.status === 'RUNNING' || status.status === 'QUEUED' || status.status === 'QUEUEING') {
        const task = activeTasks.get(taskId);
        if (!task || !task.running) continue;

        const marketplaces = ['opensea', 'magiceden', 'blur'];
        let hasPartialQueueing = false;
        let totalExpected = 0;
        let totalActual = 0;
        const mismatchDetails: string[] = [];

        for (const mp of marketplaces) {
          const parentJobId = `${taskId}-${mp}`;
          const parentJob = parentJobs.get(parentJobId);

          if (parentJob) {
            if (parentJob.expectedJobs !== undefined) {
              totalExpected += parentJob.expectedJobs;
              totalActual += parentJob.actualJobs || 0;

              // Check for partial queueing
              if (parentJob.expectedJobs > 0 &&
                parentJob.actualJobs !== undefined &&
                parentJob.actualJobs < parentJob.expectedJobs &&
                !parentJob.allJobsQueued) {
                hasPartialQueueing = true;
                mismatchDetails.push(`${mp}: ${parentJob.actualJobs}/${parentJob.expectedJobs}`);
              }
            }
          }
        }

        if (hasPartialQueueing && status.totalJobs === 0) {
          console.log(RED + `\n[Partial Queueing Recovery] Task ${taskId} detected with partial queueing` + RESET);
          console.log(RED + `  Expected: ${totalExpected} jobs` + RESET);
          console.log(RED + `  Actual: ${totalActual} jobs` + RESET);
          console.log(RED + `  Missing: ${totalExpected - totalActual} jobs` + RESET);
          console.log(RED + `  Details: ${mismatchDetails.join(', ')}` + RESET);

          // Check if restart is already scheduled
          const restartKey = `recovery-restart:${taskId}`;
          if (!tasksBeingRestarted.has(restartKey)) {
            tasksBeingRestarted.add(restartKey);

            console.log(MAGENTA + `[Recovery] Triggering automatic task restart for ${taskId}` + RESET);

            try {
              await triggerTaskRestart(taskId);
              console.log(GREEN + `[Recovery] Successfully triggered restart for task ${taskId}` + RESET);
            } catch (error) {
              console.error(RED + `[Recovery] Failed to restart task ${taskId}: ${error}` + RESET);
            } finally {
              // Clear the flag after 30 seconds to allow future restarts if needed
              setTimeout(() => {
                tasksBeingRestarted.delete(restartKey);
              }, 30000);
            }
          } else {
            console.log(YELLOW + `[Recovery] Restart already in progress for task ${taskId}` + RESET);
          }
        }
      }
    }

    console.log(CYAN + '==================================================\n' + RESET);
  } catch (error) {
    console.error(RED + '[recoverPartialQueueingTasks] Error:' + RESET, error);
  }
}

/**
 * Checks if all parent jobs for a task are complete
 */
function checkAllParentJobsComplete(taskId: string): boolean {
  const task = activeTasks.get(taskId);
  if (!task) return false;

  const selectedMarketplaces = task.selectedMarketplaces.map(m => m.toLowerCase());
  const enabledMarketplacesLower = enabledMarketplaces.map(m => m.toLowerCase());
  const marketplacesToCheck = selectedMarketplaces.filter(m => enabledMarketplacesLower.includes(m));

  let allComplete = true;
  let hasAnyJobs = false;

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

      if (totalJobs > 0) {
        hasAnyJobs = true;
        if (parentJob.completedJobs < totalJobs) {
          allComplete = false;
          break;
        }
      }
    }
  }

  return hasAnyJobs && allComplete;
}

/**
 * Track when tasks first appear to be complete
 */
const taskCompletionTracking = new Map<string, number>();


/**
 * Checks for tasks that have all jobs complete but haven't transitioned
 * Forces completion after 30 seconds of being stuck
 */
async function checkStuckTasks() {
  try {
    const now = Date.now();

    for (const [taskId, task] of activeTasks) {
      if (!task.running) continue;

      const status = taskStatus[taskId];
      if (!status || (status.status !== 'RUNNING' && status.status !== 'QUEUED' && status.status !== 'QUEUEING')) continue;

      // Check for stuck QUEUEING status
      if (status.status === 'QUEUEING') {
        const firstSeenQueueing = taskCompletionTracking.get(`${taskId}-queueing`);

        if (!firstSeenQueueing) {
          // First time seeing this task in QUEUEING
          taskCompletionTracking.set(`${taskId}-queueing`, now);
          console.log(YELLOW + `[Stuck Task Monitor] Task ${taskId} in QUEUEING status, monitoring...` + RESET);
        } else {
          // Check if stuck in QUEUEING for too long
          const stuckDuration = now - firstSeenQueueing;

          if (stuckDuration > 600000) { // 10 minutes in QUEUEING
            console.log(RED + `[Stuck Task] Task ${taskId} stuck in QUEUEING for ${(stuckDuration / 1000).toFixed(1)}s` + RESET);

            // Check parent jobs
            const marketplaces = ['opensea', 'magiceden', 'blur'];
            let hasEmptyParentJobs = false;

            for (const mp of marketplaces) {
              const parentJobId = getParentJobId(taskId, mp);
              const parentJob = parentJobs.get(parentJobId);
              if (parentJob && parentJob.totalJobs === 0) {
                console.log(RED + `  ${mp}: Parent job exists with 0 totalJobs (failed job creation)` + RESET);
                hasEmptyParentJobs = true;
              }
            }

            if (hasEmptyParentJobs) {
              console.log(YELLOW + `[Stuck Task] Task ${taskId} has empty parent jobs, likely due to timeout. Restart should be triggered by schedule job timeout handler.` + RESET);
            }

            // Clean up tracking after logging
            taskCompletionTracking.delete(`${taskId}-queueing`);
          }
        }
      } else {
        // Task is not in QUEUEING, remove from tracking
        if (taskCompletionTracking.has(`${taskId}-queueing`)) {
          taskCompletionTracking.delete(`${taskId}-queueing`);
        }
      }

      // Check for 99% stuck condition
      if (status.runningPercentage >= 99 && status.runningPercentage < 100) {
        const firstSeen99 = taskCompletionTracking.get(`${taskId}-99`);

        if (!firstSeen99) {
          // First time seeing this task at 99%
          taskCompletionTracking.set(`${taskId}-99`, now);
          console.log(YELLOW + `[Stuck Task Monitor] Task ${taskId} at ${status.runningPercentage.toFixed(2)}%, monitoring...` + RESET);

          // Log detailed parent job status
          const marketplaces = ['opensea', 'magiceden', 'blur'];
          for (const mp of marketplaces) {
            const parentJobId = getParentJobId(taskId, mp);
            const parentJob = parentJobs.get(parentJobId);
            if (parentJob) {
              console.log(YELLOW + `  ${mp}: ${parentJob.completedJobs}/${parentJob.totalJobs} (${((parentJob.completedJobs / parentJob.totalJobs) * 100).toFixed(2)}%)` + RESET);
            }
          }
        } else {
          // Check if stuck at 99% for too long
          const stuckDuration = now - firstSeen99;

          if (stuckDuration > 120000) { // 2 minutes at 99%
            console.log(RED + `[Stuck Task] Task ${taskId} stuck at ${status.runningPercentage.toFixed(2)}% for ${(stuckDuration / 1000).toFixed(1)}s` + RESET);

            // Reconcile actual job counts
            let totalExpected = 0;
            let totalCompleted = 0;
            const marketplaces = ['opensea', 'magiceden', 'blur'];

            for (const mp of marketplaces) {
              const parentJobId = getParentJobId(taskId, mp);
              const parentJob = parentJobs.get(parentJobId);
              if (parentJob) {
                totalExpected += parentJob.totalJobs;
                totalCompleted += parentJob.completedJobs;
                console.log(RED + `  ${mp}: ${parentJob.completedJobs}/${parentJob.totalJobs} jobs` + RESET);
              }
            }

            const actualPercentage = totalExpected > 0 ? (totalCompleted / totalExpected) * 100 : 0;
            console.log(RED + `  Actual completion: ${totalCompleted}/${totalExpected} = ${actualPercentage.toFixed(2)}%` + RESET);

            // Check for jobs in fallback store before forcing completion
            const expectedCounts: { [marketplace: string]: number } = {};
            const actualCounts: { [marketplace: string]: number } = {};

            for (const mp of marketplaces) {
              const parentJobId = getParentJobId(taskId, mp);
              const parentJob = parentJobs.get(parentJobId);
              if (parentJob) {
                expectedCounts[mp] = parentJob.totalJobs;
                actualCounts[mp] = parentJob.completedJobs;
              }
            }

            // Try to recover missing jobs from fallback store
            await recoverStuckJobs(taskId, expectedCounts, actualCounts, async (tid, mp, increment) => {
              const parentJobId = getParentJobId(tid, mp);
              const parentJob = parentJobs.get(parentJobId);
              if (parentJob) {
                parentJob.completedJobs = Math.min(parentJob.completedJobs + increment, parentJob.totalJobs);
                console.log(GREEN + `[Recovery] Recovered ${increment} ${mp} jobs from fallback store` + RESET);
              }
            });

            // Recalculate after recovery
            totalCompleted = 0;
            for (const mp of marketplaces) {
              const parentJobId = getParentJobId(taskId, mp);
              const parentJob = parentJobs.get(parentJobId);
              if (parentJob) {
                totalCompleted += parentJob.completedJobs;
              }
            }
            const actualPercentageAfterRecovery = totalExpected > 0 ? (totalCompleted / totalExpected) * 100 : 0;

            // If we're actually very close to 100%, force completion
            if (actualPercentageAfterRecovery >= 99.0) { // Lower threshold to 99% for tolerance
              console.log(GREEN + `[Stuck Task] Forcing completion for task ${taskId} (${actualPercentageAfterRecovery.toFixed(2)}% complete)` + RESET);

              // Force all parent jobs to be marked as complete
              for (const mp of marketplaces) {
                const parentJobId = getParentJobId(taskId, mp);
                const parentJob = parentJobs.get(parentJobId);
                if (parentJob && parentJob.completedJobs < parentJob.totalJobs) {
                  console.log(GREEN + `  Adjusting ${mp}: ${parentJob.completedJobs} -> ${parentJob.totalJobs}` + RESET);
                  parentJob.completedJobs = parentJob.totalJobs;
                }
              }

              // Force percentage to 100
              status.runningPercentage = 100;

              // Trigger completion
              await checkTaskCompletionAndScheduleNext(taskId);

              // Clean up tracking
              taskCompletionTracking.delete(`${taskId}-99`);
            }
          }
        }
      } else {
        // Task is not at 99%, remove from tracking
        if (taskCompletionTracking.has(`${taskId}-99`)) {
          taskCompletionTracking.delete(`${taskId}-99`);
        }
      }

      const allComplete = checkAllParentJobsComplete(taskId);

      if (allComplete) {
        const firstSeen = taskCompletionTracking.get(taskId);

        if (!firstSeen) {
          // First time seeing this task as complete
          taskCompletionTracking.set(taskId, now);
          console.log(YELLOW + `[Stuck Task Monitor] Task ${taskId} appears complete, monitoring...` + RESET);
        } else {
          // Check if stuck for too long
          const stuckDuration = now - firstSeen;

          if (stuckDuration > 30000) { // 30 seconds
            console.log(RED + `[Stuck Task] Task ${taskId} has all jobs complete for ${(stuckDuration / 1000).toFixed(1)}s` + RESET);
            console.log(GREEN + `[Stuck Task] Forcing completion for task ${taskId}` + RESET);

            // Force percentage to 100
            if (status) {
              status.runningPercentage = 100;

              // Log the force completion
              const marketplaces = ['opensea', 'magiceden', 'blur'];
              for (const mp of marketplaces) {
                const parentJobId = getParentJobId(taskId, mp);
                const parentJob = parentJobs.get(parentJobId);
                if (parentJob) {
                  console.log(`  ${mp}: ${parentJob.completedJobs}/${parentJob.totalJobs}`);
                }
              }
            }

            // Trigger completion
            await checkTaskCompletionAndScheduleNext(taskId);

            // Clean up tracking
            taskCompletionTracking.delete(taskId);
          }
        }
      } else {
        // Task is not complete, remove from tracking
        if (taskCompletionTracking.has(taskId)) {
          console.log(BLUE + `[Stuck Task Monitor] Task ${taskId} no longer complete, removing from monitoring` + RESET);
          taskCompletionTracking.delete(taskId);
        }
      }
    }

    // Clean up old entries
    for (const [taskId, firstSeen] of taskCompletionTracking) {
      if (!activeTasks.has(taskId) || (now - firstSeen) > 300000) { // 5 minutes
        taskCompletionTracking.delete(taskId);
      }
    }
  } catch (error) {
    console.error(RED + '[checkStuckTasks] Error:' + RESET, error);
  }
}

/**
 * Health check monitor for tasks stuck at 0% with parent jobs but no actual jobs
 * Detects and recovers from job creation failures
 */
async function healthCheckStuckTasks() {
  try {
    const now = Date.now();

    for (const [taskId, status] of Object.entries(taskStatus)) {
      // Only check RUNNING tasks
      if (status.status !== 'RUNNING') continue;

      const task = activeTasks.get(taskId);
      if (!task || !task.running) continue;

      // Check if task has been at 0% for too long
      const lastProgressUpdate = status.lastProgressUpdate || status.lastUpdate || now;
      const stuckDuration = now - lastProgressUpdate;

      if (status.runningPercentage === 0 && stuckDuration > 60000) { // 1 minute at 0%
        console.log(YELLOW + `[HealthCheck] Task ${taskId} stuck at 0% for ${(stuckDuration / 1000).toFixed(1)}s` + RESET);

        // Check parent jobs
        let hasParentJobs = false;
        let hasActualJobs = false;
        let needsRecovery = false;

        const marketplaces = task.selectedMarketplaces.map(m => m.toLowerCase());

        for (const marketplace of marketplaces) {
          const parentJobId = getParentJobId(taskId, marketplace);
          const parentJob = parentJobs.get(parentJobId);

          if (parentJob) {
            hasParentJobs = true;
            console.log(CYAN + `  ${marketplace}: Parent job exists - expectedJobs=${parentJob.expectedJobs}, actualJobs=${parentJob.actualJobs || 0}, totalJobs=${parentJob.totalJobs}` + RESET);

            // Check if parent job has expected jobs but no actual jobs
            if (parentJob.expectedJobs && parentJob.expectedJobs > 0 && (!parentJob.actualJobs || parentJob.actualJobs === 0)) {
              needsRecovery = true;
              console.log(RED + `  ${marketplace}: Parent job expects ${parentJob.expectedJobs} jobs but has 0 actual jobs!` + RESET);
            }

            if (parentJob.actualJobs && parentJob.actualJobs > 0) {
              hasActualJobs = true;
            }
          }
        }

        // If we have parent jobs but no actual jobs, trigger recovery
        if (hasParentJobs && !hasActualJobs && needsRecovery) {
          console.log(RED + `[HealthCheck] Task ${taskId} has parent jobs but no actual jobs - triggering recovery` + RESET);

          // Log to error tracking
          await logger.logError(ErrorCategory.QUEUE, 'Task stuck at 0% with parent jobs but no actual jobs', {
            taskId,
            stuckDuration,
            hasParentJobs,
            hasActualJobs,
            runningPercentage: status.runningPercentage
          });

          // Attempt recovery
          await recoverStuckTask(taskId);
        }
      }
    }
  } catch (error) {
    console.error(RED + '[healthCheckStuckTasks] Error:' + RESET, error);
  }
}

/**
 * Recover a stuck task by attempting to create missing jobs
 */
async function recoverStuckTask(taskId: string): Promise<void> {
  const task = activeTasks.get(taskId);
  if (!task) {
    console.log(YELLOW + `[RecoverTask] Task ${taskId} not found in activeTasks` + RESET);
    return;
  }

  console.log(YELLOW + `[RecoverTask] Attempting to recover stuck task ${taskId}` + RESET);

  let jobsCreated = false;

  // Try to create missing jobs for each marketplace
  for (const marketplaceName of task.selectedMarketplaces) {
    const marketplace = marketplaceName.toLowerCase();
    const parentJobId = getParentJobId(taskId, marketplace);
    const parentJob = parentJobs.get(parentJobId);

    if (parentJob && parentJob.expectedJobs && parentJob.expectedJobs > 0 && (!parentJob.actualJobs || parentJob.actualJobs === 0)) {
      console.log(YELLOW + `[RecoverTask] Creating missing ${marketplace} jobs for task ${taskId}` + RESET);

      try {
        // Set task to QUEUEING to reflect that we're creating jobs
        setTaskStatus(taskId, 'QUEUEING', 'recovering stuck task - creating missing jobs');

        // Determine which function to call based on marketplace
        switch (marketplace) {
          case 'opensea':
            // Create OpenSea jobs
            // Create token/trait bid jobs similar to processOpenseaScheduledBid
            const jobs: Job[] = [];

            // For simplicity, create collection bid for recovery
            const jobId = `bid-${taskId}-opensea-${task.contract.slug}-collection-recovery`;

            await queue.add(
              OPENSEA_COLLECTION_BID,
              {
                task: task,
                _id: taskId,
                address: task.wallet,
                slug: task.contract.slug
              },
              { jobId }
            );

            jobsCreated = true;
            console.log(GREEN + `[RecoverTask] Created OpenSea recovery job` + RESET);
            break;

          case 'magiceden':
            // Create MagicEden recovery job
            const meJobId = `bid-${taskId}-magiceden-${task.contract.slug}-collection-recovery`;

            await queue.add(
              MAGICEDEN_COLLECTION_BID,
              {
                task: task,
                _id: taskId,
                address: task.wallet,
                slug: task.contract.slug
              },
              { jobId: meJobId }
            );

            jobsCreated = true;
            console.log(GREEN + `[RecoverTask] Created MagicEden recovery job` + RESET);
            break;

          case 'blur':
            // Create Blur recovery job
            const blurJobId = `bid-${taskId}-blur-${task.contract.slug}-collection-recovery`;

            await queue.add(
              BLUR_COLLECTION_BID,
              {
                task: task,
                _id: taskId,
                address: task.wallet,
                slug: task.contract.slug
              },
              { jobId: blurJobId }
            );

            jobsCreated = true;
            console.log(GREEN + `[RecoverTask] Created Blur recovery job` + RESET);
            break;
        }

      } catch (error) {
        console.error(RED + `[RecoverTask] Failed to create jobs for ${marketplace}:` + RESET, error);

        // Log the error
        const errorToLog = error instanceof Error ? error : new Error(String(error));
        await logger.logError(ErrorCategory.QUEUE, `Failed to recover stuck task ${taskId} for ${marketplace}`, {
          taskId,
          marketplace,
          error: errorToLog
        });
      }
    }
  }

  // Update task status back to RUNNING if jobs were created
  if (jobsCreated) {
    setTaskStatus(taskId, 'RUNNING', 'recovery complete - jobs created');

    // Force update task percentage after recovery
    await updateTaskRunningPercentage(taskId);

    console.log(GREEN + `[RecoverTask] Recovery complete for task ${taskId}` + RESET);
  } else {
    console.log(YELLOW + `[RecoverTask] No jobs were created during recovery for task ${taskId}` + RESET);

    // If no jobs could be created, might need to restart the task
    console.log(YELLOW + `[RecoverTask] Scheduling task restart for ${taskId}` + RESET);
    setTimeout(async () => {
      await triggerTaskRestart(taskId);
    }, 5000);
  }
}

// Set up health check interval (runs every 30 seconds)
const TASK_HEALTH_CHECK_INTERVAL = 30000;
setInterval(healthCheckStuckTasks, TASK_HEALTH_CHECK_INTERVAL);

// Job creation verification - runs more frequently to catch issues early
const JOB_VERIFICATION_INTERVAL = 15000; // 15 seconds
const jobCreationVerificationMap = new Map<string, { timestamp: number; expectedJobs: number; marketplace: string }>();

/**
 * Verify job creation for tasks that recently called processBulkJobs
 * This runs asynchronously and doesn't block the main flow
 */
async function verifyJobCreation(): Promise<void> {
  const now = Date.now();
  const verificationTimeout = 30000; // 30 seconds to verify job creation

  for (const [key, verificationData] of jobCreationVerificationMap.entries()) {
    const [taskId, loopCount] = key.split('-loop');
    const { timestamp, expectedJobs, marketplace } = verificationData;

    // Skip if too old
    if (now - timestamp > verificationTimeout) {
      jobCreationVerificationMap.delete(key);
      continue;
    }

    // Check if parent job has actualJobs matching expectedJobs
    const parentJobId = `${taskId}-${marketplace}-loop${loopCount}`.toLowerCase();
    const parentJob = parentJobs.get(parentJobId);

    if (!parentJob) {
      console.warn(YELLOW + `[JobVerification] Parent job ${parentJobId} not found` + RESET);
      jobCreationVerificationMap.delete(key);
      continue;
    }

    // Check if jobs are actually being created
    if (parentJob.actualJobs === 0 && expectedJobs > 0 && now - timestamp > 10000) {
      console.error(RED + `[JobVerification] Task ${taskId} loop ${loopCount}: No jobs created after 10s (expected: ${expectedJobs})` + RESET);

      // Trigger recovery if task is still in QUEUEING state
      const status = taskStatus[taskId];
      if (status && status.status === 'QUEUEING') {
        console.log(YELLOW + `[JobVerification] Triggering recovery for stuck task ${taskId}` + RESET);
        await recoverStuckTask(taskId);
      }

      jobCreationVerificationMap.delete(key);
    } else if (parentJob.actualJobs && parentJob.actualJobs > 0) {
      // Jobs are being created, remove from verification
      console.log(GREEN + `[JobVerification] Task ${taskId} loop ${loopCount}: Jobs created (${parentJob.actualJobs}/${expectedJobs})` + RESET);
      jobCreationVerificationMap.delete(key);
    }
  }
}

setInterval(verifyJobCreation, JOB_VERIFICATION_INTERVAL);

/**
 * Register a task for job creation verification
 * Call this when processBulkJobs is called with fire-and-forget
 */
function registerJobCreationVerification(taskId: string, marketplace: string, expectedJobs: number): void {
  const loopCount = loopCounterStats[taskId] || 1;
  const key = `${taskId}-loop${loopCount}`;
  jobCreationVerificationMap.set(key, {
    timestamp: Date.now(),
    expectedJobs,
    marketplace
  });
}

/**
 * Ensures parent job exists before job creation
 * Uses a queue to prevent race conditions
 */
const pendingJobCreation = new Map<string, Promise<void>>();

/**
 * Helper function to generate parent job ID with loop number
 */
function getParentJobId(taskId: string, marketplace: string): string {
  const currentLoopCount = loopCounterStats[taskId] || 1;
  return `${taskId}-${marketplace}-loop${currentLoopCount}`.toLowerCase();
}


async function ensureParentJobExists(taskId: string, marketplace: string): Promise<void> {
  const parentJobId = getParentJobId(taskId, marketplace);
  const currentLoopCount = loopCounterStats[taskId] || 1;
  const timestamp = new Date().toISOString();

  // Validate inputs
  if (!taskId || !marketplace) {
    console.error(RED + `[ensureParentJobExists] Invalid inputs - taskId: ${taskId}, marketplace: ${marketplace}` + RESET);
    return;
  }

  // Check if already being created
  const existing = pendingJobCreation.get(parentJobId);
  if (existing) {
    console.log(CYAN + `[ensureParentJobExists] Waiting for existing creation of ${parentJobId}` + RESET);
    await existing;
    return;
  }

  // Create promise for this parent job
  const promise = (async () => {
    try {
      if (!parentJobs.has(parentJobId)) {
        console.log(BLUE + `[Parent Job] Creating parent job: ${parentJobId} at ${timestamp}` + RESET);

        // Validate task exists
        const task = activeTasks.get(taskId);
        if (!task) {
          console.error(RED + `[Parent Job] Task ${taskId} not found in activeTasks!` + RESET);
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
            existingParentJob.completedJobs = existingParentJob.totalJobs;
          }
        }
      }
    } catch (error) {
      console.error(RED + `[Parent Job] Error creating parent job ${parentJobId}:` + RESET, error);
      if (error instanceof Error) {
        console.error(RED + `  Error details: ${error.message}` + RESET);
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
 */
async function waitForParentJobReady(taskId: string, marketplace: string, maxWaitTime: number = 5000): Promise<boolean> {
  const parentJobId = getParentJobId(taskId, marketplace);
  const startTime = Date.now();

  while (Date.now() - startTime < maxWaitTime) {
    const parentJob = parentJobs.get(parentJobId);
    if (parentJob) {
      console.log(GREEN + `[waitForParentJobReady] Parent job ${parentJobId} is ready` + RESET);
      return true;
    }

    // Check if creation is pending
    if (pendingJobCreation.has(parentJobId)) {
      console.log(CYAN + `[waitForParentJobReady] Waiting for parent job ${parentJobId} creation...` + RESET);
      await new Promise(resolve => setTimeout(resolve, 100));
      continue;
    }

    // Parent job doesn't exist and isn't being created
    break;
  }

  console.log(YELLOW + `[waitForParentJobReady] Parent job ${parentJobId} not ready after ${maxWaitTime}ms` + RESET);
  return false;
}

/**
 * Enhanced ensureParentJobExists with validation
 */
async function ensureParentJobExistsEnhanced(taskId: string, marketplace: string): Promise<void> {
  const parentJobId = getParentJobId(taskId, marketplace);

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
  if (['COMPLETED', 'STOPPED'].includes(taskState.status)) {
    console.log(YELLOW + `[ensureParentJobExistsEnhanced] Task ${taskId} is ${taskState.status}, skipping parent job creation` + RESET);
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
        console.error(RED + `[OrphanedJobRecovery] Recovery check failed: ${error}` + RESET);
      });
    }, intervalMs);

    console.log(GREEN + `[OrphanedJobRecovery] Started recovery checks every ${intervalMs}ms` + RESET);
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
        const currentLoop = loopCounterStats[taskId] || 1;
        const parentJobId = `${taskId}-${marketplace}-loop${currentLoop}`.toLowerCase();
        const parentJob = parentJobs.get(parentJobId);

        if (!parentJob) {
          // Check if it's from an old loop
          const jobLoopMatch = job.id.match(/loop(\d+)/);
          const jobLoop = jobLoopMatch ? parseInt(jobLoopMatch[1]) : 0;

          if (jobLoop > 0 && jobLoop < currentLoop) {
            // Old loop job - should be removed
            orphanedJobs.push(job);
            console.log(YELLOW + `[OrphanedJobRecovery] Found old loop job: ${job.id} (loop ${jobLoop}, current ${currentLoop})` + RESET);
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
        console.log(YELLOW + `[OrphanedJobRecovery] Removing ${orphanedJobs.length} old loop jobs` + RESET);

        for (const job of orphanedJobs) {
          try {
            await job.remove();
            console.log(CYAN + `[OrphanedJobRecovery] Removed old job: ${job.id}` + RESET);
          } catch (error) {
            console.error(RED + `[OrphanedJobRecovery] Failed to remove job ${job.id}: ${error}` + RESET);
          }
        }
      }

      // Recover current loop orphaned jobs by creating parent jobs
      for (const [parentJobId, jobs] of recoveryActions.entries()) {
        const [taskId, marketplace] = parentJobId.split('-');

        console.log(CYAN + `[OrphanedJobRecovery] Recovering ${jobs.length} orphaned jobs for ${parentJobId}` + RESET);

        // Check if task is still active
        const task = activeTasks.get(taskId);
        if (!task || !task.running) {
          console.log(YELLOW + `[OrphanedJobRecovery] Task ${taskId} not active, removing orphaned jobs` + RESET);
          for (const job of jobs) {
            try {
              await job.remove();
            } catch (error) {
              console.error(RED + `[OrphanedJobRecovery] Failed to remove job ${job.id}: ${error}` + RESET);
            }
          }
          continue;
        }

        // Create parent job for recovery
        const currentLoop = loopCounterStats[taskId] || 1;
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
        console.log(GREEN + `[OrphanedJobRecovery] Created recovery parent job ${parentJobId} for ${jobs.length} jobs` + RESET);

        // Update task status if needed
        const status = taskStatus[taskId];
        if (status && status.status === 'STARTED') {
          setTaskStatus(taskId, 'RUNNING', 'orphaned jobs recovered');
        }
      }

    } catch (error) {
      console.error(RED + `[OrphanedJobRecovery] Error during recovery: ${error}` + RESET);
      throw error;
    }
  }

  // Manual recovery for specific task
  async recoverTaskJobs(taskId: string) {
    console.log(CYAN + `[OrphanedJobRecovery] Manual recovery for task ${taskId}` + RESET);

    const task = activeTasks.get(taskId);
    if (!task) {
      console.log(YELLOW + `[OrphanedJobRecovery] Task ${taskId} not found` + RESET);
      return;
    }

    const currentLoop = loopCounterStats[taskId] || 1;
    const marketplaces = task.selectedMarketplaces.map(m => m.toLowerCase());

    for (const marketplace of marketplaces) {
      const parentJobId = `${taskId}-${marketplace}-loop${currentLoop}`.toLowerCase();
      const parentJob = parentJobs.get(parentJobId);

      if (!parentJob) {
        // Count jobs for this marketplace
        const pattern = `bid-${taskId}-${marketplace}-`;
        const jobs = await queue.getJobs(['active', 'waiting', 'delayed']);
        const matchingJobs = jobs.filter(job => job?.id?.includes(pattern));

        if (matchingJobs.length > 0) {
          console.log(CYAN + `[OrphanedJobRecovery] Found ${matchingJobs.length} orphaned ${marketplace} jobs for task ${taskId}` + RESET);

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
          console.log(GREEN + `[OrphanedJobRecovery] Created parent job for ${matchingJobs.length} ${marketplace} jobs` + RESET);
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
  console.log(CYAN + `[fixStuckTask] Attempting to fix stuck task ${taskId}` + RESET);

  // First, try orphaned job recovery
  await orphanedJobRecovery.recoverTaskJobs(taskId);

  // Check current status
  const status = taskStatus[taskId];
  if (status?.status === 'STARTED') {
    // Force progression check
    taskStatusManager.checkAndFixStuckStartedTask(taskId);
  }

  // Update percentage
  await updateTaskRunningPercentage(taskId);

  console.log(GREEN + `[fixStuckTask] Recovery attempt completed for task ${taskId}` + RESET);
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
    console.log(CYAN + `[checkCancellationJobStatus] Checking cancellation status for task ${taskId}` + RESET);

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
              console.log(RED + `[checkCancellationJobStatus] Failed job: ${job.id}, attempts: ${job.attemptsMade}/${job.opts.attempts}, error: ${job.failedReason}` + RESET);
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

    console.log(GREEN + `[checkCancellationJobStatus] Summary:` + RESET);
    console.log(`  Total: ${totalCancellationJobs}`);
    console.log(`  Completed: ${completedCancellationJobs}`);
    console.log(`  Failed: ${failedCancellationJobs}`);
    console.log(`  Active: ${activeCancellationJobs}`);
    console.log(`  Waiting: ${waitingCancellationJobs}`);
    console.log(`  Stalled: ${stalledCancellationJobs}`);
    console.log(`  Progress: ${cancellationProgress}%`);

    return result;
  } catch (error) {
    console.error(RED + `[ERROR - checkCancellationJobStatus - Task: ${taskId}] Error checking cancellation status:` + RESET, error);
    throw error;
  }
}

/**
 * Recovers orphaned jobs that don't have parent job tracking
 * This helps fix jobs that were created before parent jobs
 */
async function recoverOrphanedJobs() {
  try {
    console.log(CYAN + '\n========== Orphaned Job Recovery ==========' + RESET);

    // Get jobs from different states
    const states = ['completed', 'failed', 'active', 'waiting'] as const;
    let orphanedCount = 0;
    let recoveredCount = 0;

    for (const state of states) {
      try {
        const jobs = await queue.getJobs([state], 0, 100); // Get first 100 jobs

        for (const job of jobs) {
          if (!job.id) continue;

          // Check if it's a bid job
          if (job.id.startsWith('bid-')) {
            const parts = job.id.split('-');
            if (parts.length >= 3) {
              const taskId = parts[1];
              const marketplace = parts[2];
              const parentJobId = `${taskId}-${marketplace}`;

              // Check if parent job exists
              if (!parentJobs.has(parentJobId)) {
                orphanedCount++;

                // Only recover if task still exists and is active
                const task = activeTasks.get(taskId);
                if (task) {
                  console.log(YELLOW + `[Orphaned Job] Found: ${job.id} (${state})` + RESET);

                  // Create parent job retroactively
                  await ensureParentJobExists(taskId, marketplace);

                  // Update counts based on job state
                  const parentJob = parentJobs.get(parentJobId);
                  if (parentJob) {
                    parentJob.totalJobs++;

                    if (state === 'completed' || state === 'failed') {
                      parentJob.completedJobs++;
                    }

                    recoveredCount++;
                    console.log(GREEN + `  ✓ Recovered and updated parent job tracking` + RESET);
                  }
                }
              }
            }
          }
        }
      } catch (error) {
        console.error(RED + `[recoverOrphanedJobs] Error processing ${state} jobs:` + RESET, error);
      }
    }

    if (orphanedCount > 0) {
      console.log(`\nFound ${orphanedCount} orphaned jobs, recovered ${recoveredCount}`);
    } else {
      console.log('No orphaned jobs found');
    }

    console.log(CYAN + '==========================================\n' + RESET);
  } catch (error) {
    console.error(RED + '[recoverOrphanedJobs] Error:' + RESET, error);
  }
}

// Schedule periodic job tracking status report
setInterval(logJobTrackingStatus, STANDARD_INTERVAL);

// Schedule stuck task checker
setInterval(checkStuckTasks, STANDARD_INTERVAL);

// Schedule partial queueing recovery check
setInterval(recoverPartialQueueingTasks, STANDARD_INTERVAL);

// Schedule orphaned job recovery - DISABLED to prevent state bleed
// setInterval(recoverOrphanedJobs, STANDARD_INTERVAL);

// Export verification function for use in other modules
export { verifyCancellationComplete };