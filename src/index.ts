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
import cors from "cors";
import helmet from "helmet";
import stringify from 'json-stable-stringify';
import WebSocket, { Server as WebSocketServer } from 'ws';
import http from 'http';
import { axiosInstance, initialize, limiter, removeTaskFromBlacklist, isTaskBlacklisted } from "./init";
import { bidOnOpensea, cancelOpenseaRequest, cancelOrderWithoutTracking, fetchItemOffers, fetchOpenseaListings, fetchOpenseaOffers, IFee, OpenseaOfferData, openseaTraitFloor, resolveContractAddress } from "./marketplace/opensea";
import { bidOnBlur, cancelBlurBid, cancelBlurRequest, fetchBlurBid, fetchBlurCollectionStats, fetchBlurTraitFloor, getAccessToken, getAuthMetrics, getTimeoutMetrics } from "./marketplace/blur/bid";
import { bidOnMagiceden, cancelMagicEdenBid, cancelMagicedenRequest, fetchMagicEdenCollectionStats, fetchMagicEdenOffer, fetchMagicEdenTokens, fetchMagicEdenTraitFloor } from "./marketplace/magiceden";
import { CollectionItemsQueryVariables, fetchOpenSeaCollectionItems, fetchOpenseaTraits, fetchRarityTokens, getCollectionDetails, getOpenseaFloor } from "./functions";
import mongoose from 'mongoose';
import Task, { ITask, SelectedTraits } from "./models/task.model";
import { Queue, Worker, Job, JobType, JobsOptions, QueueEvents, QueueOptions } from "bullmq";
import Wallet, { IWallet } from "./models/wallet.model";
import redisClient, { createBullMQConnection, checkBullMQConnectionsHealth, closeAllBullMQConnections, getBullMQConnections } from "./utils/redis";
import { BASE_URL, ERC20_MIN_ABI } from "./constants";
import { Contract, ethers, Wallet as Web3Wallet, parseEther, MaxUint256 } from "ethers";
import { DistributedLockManager } from "./utils/lock";
import { TaskStateManager } from "./utils/task-state-manager";
import { safeGetJob, safeMarkJobAsProcessed, recoverStuckJobs } from "./utils/redis-job-fallback";

import BidLogs from "./models/logs.model";
import PQueue from "p-queue";
import Order from "./models/order.model";
import Approval from "./models/approval.model";
import { USDC, WETH } from "./utils/currencies";
import axios from "axios";
import { decryptPrivateKey, setPassword, getPassword, decryptPassword, recoverPasswordFromRedis, renewPasswordTTL, reEncryptWallet, clearDecryptedKeyCache } from "./utils/crypto";
import { fetchWalletNFTs } from "./utils/nft";
import { logger, ErrorCategory, DatabaseOperation } from "./utils/logger";
import { getMarketplaceLogger, logError, logSystemError } from './utils/marketplace-file-logger';
import WebSocketManager from "./utils/websocket-manager";
import { initializeListingScheduler, shutdownListingScheduler, scheduleListing, cancelListingJob, processListingJob } from "./functions/listingScheduler";
import { initializeExpirationMonitor, stopExpirationMonitor } from "./functions/relistExpired";
import { initializeExpiredPriceCleanup, stopExpiredPriceCleanup } from "./functions/cleanupExpiredPrices";
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

import { listingPriceCache } from "./utils/listing-price-cache";
import Listing from "./models/listing.model";

const MARKETPLACE_WS_URL = "wss://nfttools-ws-proxy.nfttools.io";

// Backward-compatible alias used across Blur codepaths
// const MARKETPLACE_WS_URL = "ws://localhost:8080";

export const RATE_LIMIT = Number(process.env.RATE_LIMIT)
export const API_KEY = process.env.API_KEY;

// Validate API_KEY is loaded from environment
if (!API_KEY) {
  console.error('[FATAL] API_KEY environment variable is not set! Check .env file.');
  throw new Error('API_KEY is required but not found in environment variables');
}
if (process.env.NODE_ENV === 'development') {
  console.log(`[STARTUP] API_KEY loaded successfully (${API_KEY.substring(0, 8)}...)`);
}

const bidStats: BidCounts = {};
const ownBidCounts: { [marketplace: string]: number } = {};
const processedOwnBids: Set<string> = new Set(); // Track processed own bid hashes
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
const counterbidCounts: BidCounts = {};

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
const processedMessages = new Map<string, number>();
const MESSAGE_TTL = 5 * 60 * 1000; // 5 minutes

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

type MarketplaceStatus = 'RUNNING' | 'WAITING' | 'CANCELLING' | 'SCHEDULED' | 'COMPLETED' | 'QUEUED' | 'QUEUEING' | 'STARTED' | "STOPPED" | "PROCESSING";

interface MarketplaceTaskStatus {
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

interface TaskStatus {
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

interface LoopCount {
  [taskId: string]: {
    [marketplace: string]: number;
  };
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
  MAGICEDEN: 3,
  BLUR: 2
};

const TRAIT_BID_PRIORITY = {
  OPENSEA: 2,
  MAGICEDEN: 3,
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
export { queue };
export { listingPriceCache };
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

function parseOpenSeaResponseData(data: any): string | null {
  if (!data) {
    return null;
  }

  if (typeof data === 'string') {
    const trimmed = data.trim();

    const looksLikeJson = (trimmed.startsWith('{') && trimmed.endsWith('}')) ||
      (trimmed.startsWith('[') && trimmed.endsWith(']'));

    if (looksLikeJson) {
      try {
        return parseOpenSeaResponseData(JSON.parse(trimmed));
      } catch (_) {
        // fall through to string handling if JSON.parse fails
      }
    }

    const rayMatch = trimmed.match(/Ray ID:\s*<strong[^>]*>([^<]+)<\/strong>/i);
    const ipMatch = trimmed.match(/id=["']cf-footer-ip["'][^>]*>\s*([^<\s]+)\s*<\/span>/i);
    const headlineMatch = trimmed.match(/data-translate=["']block_headline["']>([^<]+)</i);
    const targetMatch = trimmed.match(/data-translate=["']unable_to_access["'][^>]*><span[^>]*>.*?<\/span>\s*([^<]+)/i);
    const reasonMatch = trimmed.match(/data-translate=["']blocked_why_detail["']>([^<]+)</i);

    if (rayMatch || ipMatch || headlineMatch || reasonMatch) {
      const parts: string[] = ['Cloudflare block detected'];
      if (headlineMatch) {
        parts.push(headlineMatch[1].trim());
      }
      if (targetMatch) {
        parts.push(`target=${targetMatch[1].trim()}`);
      }
      if (rayMatch) {
        parts.push(`ray=${rayMatch[1].trim()}`);
      }
      if (ipMatch) {
        parts.push(`ip=${ipMatch[1].trim()}`);
      }
      if (reasonMatch) {
        parts.push(reasonMatch[1].trim());
      }
      return parts.join(' | ');
    }

    const sanitized = trimmed.replace(/\s+/g, ' ');
    return sanitized.length > 200 ? `${sanitized.slice(0, 200)}…` : sanitized;
  }

  if (Array.isArray(data)) {
    const messages = data
      .map((entry) => parseOpenSeaResponseData(entry))
      .filter((value): value is string => Boolean(value));

    return messages.length ? messages.join('; ') : null;
  }

  if (typeof data === 'object') {
    const messages: string[] = [];

    if (Array.isArray((data as any).errors)) {
      const errorMessages = (data as any).errors
        .map((err: any) => {
          if (!err) {
            return null;
          }
          if (typeof err === 'string') {
            return err;
          }
          if (typeof err?.message === 'string') {
            return err.message;
          }
          if (typeof err?.code === 'string' && typeof err?.description === 'string') {
            return `${err.code}: ${err.description}`;
          }
          try {
            const stringified = JSON.stringify(err);
            return stringified.length > 200 ? `${stringified.slice(0, 200)}…` : stringified;
          } catch (_) {
            return String(err);
          }
        })
        .filter((value: any): value is string => Boolean(value));

      if (errorMessages.length) {
        messages.push(`errors: ${errorMessages.join('; ')}`);
      }
    }

    if (typeof (data as any).error === 'string') {
      messages.push((data as any).error);
    }

    if (typeof (data as any).message === 'string') {
      messages.push((data as any).message);
    }

    if (typeof (data as any).detail === 'string') {
      messages.push((data as any).detail);
    }

    if (messages.length) {
      return messages.join(' | ');
    }

    try {
      const stringified = JSON.stringify(data);
      if (stringified && stringified !== '{}') {
        return stringified.length > 200 ? `${stringified.slice(0, 200)}…` : stringified;
      }
    } catch (_) {
      return String(data);
    }

    return null;
  }

  return null;
}

function maskSensitiveIdentifier(value: string | undefined | null): string {
  if (!value) {
    return 'N/A';
  }

  const trimmed = value.trim();
  if (trimmed.length <= 10) {
    return trimmed;
  }

  return `${trimmed.slice(0, 6)}…${trimmed.slice(-4)}`;
}

// Helper function to extract detailed OpenSea error messages
function extractDetailedOpenSeaError(error: any): string {
  const parts: string[] = [];

  const status = error?.response?.status;
  const statusText = error?.response?.statusText;
  if (status) {
    parts.push(`HTTP ${status}${statusText ? ` ${statusText}` : ''}`);
  }

  const code = error?.code;
  if (code) {
    parts.push(`code=${code}`);
  }

  const url = error?.config?.url ?? error?.response?.config?.url;
  if (url) {
    parts.push(`url=${url}`);
  }

  const responseDetails = parseOpenSeaResponseData(error?.response?.data);
  if (responseDetails) {
    parts.push(responseDetails);
  }

  if (!parts.length && error?.message) {
    parts.push(error.message);
  }

  if (!parts.length) {
    return typeof error === 'string' ? error : 'Unknown OpenSea error';
  }

  return parts.join(' | ');
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
              logSystemError('TimedLockManager', `Failed to trigger task restart for task ${extractedTaskId}`, error);
            }
          });
        }
      } catch (error) {
        logSystemError('TimedLockManager', 'Error in timeout handler', error);
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
              logSystemError('TimedLockManager', `Failed to trigger task restart for task ${extractedTaskId}`, error);
            }
          });
        }
      } catch (error) {
        logSystemError('TimedLockManager', 'Error in extended timeout handler', error);
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
          logSystemError('StatusManager', `Failed to transition task ${taskId}/${marketplace} from ${fromStatus} to ${toStatus}`, undefined, { taskId, marketplace, fromStatus, toStatus });

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

// Track subscriptions by marketplace and collection slug
const clients = new Set<WebSocket>();

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
    const redis = redisClient.getClient();
    if (!redis) {
      return false;
    }

    // Simple ping check for single Redis instance
    const result = await redis.ping();
    return result === 'PONG';
  } catch (error) {
    // If we can't even ping, it's definitely not healthy
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
    logSystemError('RedisRecovery', 'Failed to recover from Redis failure', error);
    // Don't reset failure counter if recovery fails
  }
}

async function monitorHealth() {
  try {
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

    // Log high memory usage but don't pause the queue
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
      if (cacheDetails.oldestEntry) {
        const ageMinutes = Math.floor((Date.now() - cacheDetails.oldestEntry) / 1000 / 60);
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
      await resumeQueueAndWorkers();
    }
  } catch (error) {
    logSystemError('PasswordCheck', 'Error checking password', error);
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
      await new Promise(resolve => setTimeout(resolve, 5000));

      // Start the scheduled loop
      await runScheduledLoop();
    } else {
    }

  } catch (error) {
    logSystemError('QueueClear', 'Error during clearQueueAndRestartJobs', error);
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
      logSystemError('ConnectivityMonitor', 'Internet connectivity lost!');
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

    // Pause both queues
    await queue.pause();
    await blurQueue.pause();

    // Pause all workers
    if (workers.length > 0) {
      await Promise.all(workers.map(worker => worker.pause()));
    }

  } catch (error) {
    logSystemError('QueuePause', 'Error pausing queues and workers', error);
  }
}

async function resumeQueueAndWorkers() {
  try {

    // Resume both queues
    await queue.resume();
    await blurQueue.resume();

    // Resume all workers
    if (workers.length > 0) {
      await Promise.all(workers.map(worker => worker.resume()));
    }


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
        await Promise.all(passwordFailedJobs.map(job => job.retry()));
      }

      if (blurPasswordFailedJobs.length > 0) {
        await Promise.all(blurPasswordFailedJobs.map(job => job.retry()));
      }
    } catch (retryError) {
      logSystemError('JobRetry', 'Error retrying failed jobs', retryError);
    }
  } catch (error) {
    logSystemError('QueueResume', 'Error resuming queues and workers', error);
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
      logSystemError('HealthCheck', `Unhealthy Redis connections detected: ${unhealthyConnections.length}`);
      unhealthyConnections.forEach(([id, status]) => {
        logSystemError('HealthCheck', `Redis connection ${id}: ${status.error}`);
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
      logSystemError('HealthCheck', `Unhealthy workers detected: ${unhealthyWorkers.length}/${workers.length}`);

      let shouldRestartAll = false;

      // Attempt to restart unhealthy workers
      for (const workerStatus of unhealthyWorkers) {
        if ('error' in workerStatus && workerStatus.error) {
          logSystemError('HealthCheck', `Worker ${workerStatus.index}: ${workerStatus.error}`);

          // Track the failure
          const thresholdExceeded = trackWorkerFailure(workerStatus.index, workerStatus.error);
          if (thresholdExceeded) {
            shouldRestartAll = true;
          }
        } else if (workerStatus.paused) {
          try {
            await workers[workerStatus.index].resume();
          } catch (error) {
            logSystemError('HealthCheck', `Failed to resume worker ${workerStatus.index}`, error);

            // Track resume failure
            const thresholdExceeded = trackWorkerFailure(workerStatus.index, `Resume failed: ${error}`);
            if (thresholdExceeded) {
              shouldRestartAll = true;
            }
          }
        } else if (!workerStatus.running) {
          // Worker is not running at all
          logSystemError('HealthCheck', `Worker ${workerStatus.index} is not running`);
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

      if (oldConnections.length > 0) {
        oldConnections.forEach(conn => {
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

    // Enhanced Redis connection pool monitoring
    const maxConnections = connectionPoolStats.length;
    const connectionPoolWarningThreshold = 40; // Warn when 80% of assumed max 50 connections
    const connectionPoolCriticalThreshold = 45; // Critical when 90% of max connections

    if (maxConnections >= connectionPoolCriticalThreshold) {
      logSystemError('ConnectionPool', `Redis connection pool at critical level: ${maxConnections} connections`);
      await logger.logDatabaseError('Redis connection pool critical', {
        databaseOperation: DatabaseOperation.CONNECTION,
        collection_name: 'redis_pool',
        connectionCount: maxConnections,
        threshold: connectionPoolCriticalThreshold,
        connections: connectionAges
      });
    } else if (maxConnections >= connectionPoolWarningThreshold) {
      await logger.logWarning(ErrorCategory.DATABASE, 'Redis connection pool warning', {
        connectionCount: maxConnections,
        threshold: connectionPoolWarningThreshold,
        connections: connectionAges
      });
    }

    // Check for connection leaks (very old connections)
    const connectionLeakThreshold = 2 * 60 * 60; // 2 hours in seconds
    const possibleLeaks = connectionAges.filter(conn => conn.age > connectionLeakThreshold);

    if (possibleLeaks.length > 0) {
      possibleLeaks.forEach(conn => {
      });

      await logger.logWarning(ErrorCategory.DATABASE, 'Possible Redis connection leaks', {
        leakedConnections: possibleLeaks.length,
        connections: possibleLeaks.map(c => ({
          id: c.id,
          ageHours: Math.floor(c.age / 3600)
        }))
      });
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
        await logger.logInfo(ErrorCategory.GENERAL, 'MongoDB connection pool statistics', {
          totalConnections,
          availableConnections,
          activeConnections,
          maxPoolSize,
          minPoolSize,
          utilizationPercent: Math.round(poolUtilization * 100) / 100,
          poolState: mongoose.connection.readyState === 1 ? 'connected' : 'disconnected'
        });

        // Warning threshold: 80% of maxPoolSize
        const warningThreshold = maxPoolSize * 0.8;
        // Critical threshold: 90% of maxPoolSize
        const criticalThreshold = maxPoolSize * 0.9;

        if (activeConnections >= criticalThreshold) {
          logSystemError('MongoDBPool', `MongoDB connection pool at critical level: ${activeConnections}/${maxPoolSize} connections in use (${Math.round(poolUtilization)}%)`);
          await logger.logDatabaseError('MongoDB connection pool critical', {
            databaseOperation: DatabaseOperation.CONNECTION,
            collection_name: 'mongodb_pool',
            activeConnections,
            totalConnections,
            maxPoolSize,
            utilizationPercent: Math.round(poolUtilization),
            warningMessage: 'Connection pool near exhaustion - consider increasing MONGO_MAX_POOL_SIZE'
          });
        } else if (activeConnections >= warningThreshold) {
          await logger.logWarning(ErrorCategory.DATABASE, 'MongoDB connection pool warning', {
            activeConnections,
            totalConnections,
            maxPoolSize,
            utilizationPercent: Math.round(poolUtilization),
            threshold: Math.round(warningThreshold),
            recommendation: 'Monitor pool usage closely - may need to increase MONGO_MAX_POOL_SIZE'
          });
        }

        // Check if pool is below minimum (possible connection issues)
        if (totalConnections < minPoolSize && mongoose.connection.readyState === 1) {
          await logger.logWarning(ErrorCategory.DATABASE, 'MongoDB pool below minimum size', {
            totalConnections,
            minPoolSize,
            warningMessage: 'Connection pool not maintaining minimum connections'
          });
        }
      }
    } catch (mongoMonitorError) {
      // Don't let MongoDB monitoring errors crash the health check
      logSystemError('MongoDBPoolMonitoring', 'Error monitoring MongoDB pool', mongoMonitorError);
    }

  } catch (error) {
    logSystemError('WorkerHealthCheck', 'Error during worker health check', error);
  }
}

function stopWorkerHealthMonitoring() {
  if (workerHealthInterval) {
    clearInterval(workerHealthInterval);
    workerHealthInterval = null;
  }
  if (queueDrainInterval) {
    clearInterval(queueDrainInterval);
    queueDrainInterval = null;
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

  logSystemError('WorkerFailure', `Worker ${workerIndex} failure tracked: ${failure.count}/${WORKER_FAILURE_THRESHOLD} in window`);
  logSystemError('WorkerFailure', `Worker ${workerIndex} error`, error);

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

      lastQueueClearForBacklog = now;
      await clearQueueAndRestartJobs();
    } else if (shouldClearQueue) {
      const cooldownRemaining = Math.ceil((CLEAR_COOLDOWN - (now - lastQueueClearForBacklog)) / 1000);
    }

    // Log drain status if high waiting count
    if (totalWaiting > RATE_LIMIT) {
      const processingRate = historyEntries.length >= 2
        ? (historyEntries[historyEntries.length - 1][1].processed - historyEntries[0][1].processed) / (historyEntries.length - 1)
        : 0;

    }

  } catch (error) {
    logSystemError('QueueDrainMonitor', 'Error in queue drain monitoring', error);
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

    let mainQueueConnection: any;
    let mainQueueEventsConnection: any;
    let blurQueueConnection: any;
    let blurQueueEventsConnection: any;

    try {
      // Main queue connection
      mainQueueConnection = await createBullMQConnection('main-queue');
      bullMQConnections.push(mainQueueConnection);

      // Main queue events connection
      mainQueueEventsConnection = await createBullMQConnection('main-queue-events');
      bullMQConnections.push(mainQueueEventsConnection);

      // Blur queue connection
      blurQueueConnection = await createBullMQConnection('blur-queue');
      bullMQConnections.push(blurQueueConnection);

      // Blur queue events connection
      blurQueueEventsConnection = await createBullMQConnection('blur-queue-events');
      bullMQConnections.push(blurQueueEventsConnection);

      // Verify all connections are healthy before proceeding
      const healthCheck = await checkBullMQConnectionsHealth();
      const unhealthyConnections = Object.entries(healthCheck).filter(([_, status]) => !status.healthy);

      if (unhealthyConnections.length > 0) {
        logSystemError('BullMQInit', 'Unhealthy Redis connections detected');
        unhealthyConnections.forEach(([id, status]) => {
          logSystemError('HealthCheck', `Redis connection ${id}: ${status.error}`);
        });
        throw new Error('One or more Redis connections are unhealthy');
      }

    } catch (error) {
      logSystemError('BullMQInit', 'Failed to create BullMQ connections', error);
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
    blurQueueEvents = new QueueEvents(BLUR_BIDDING_QUEUE, { connection: blurQueueEventsConnection });

    // Initialize Blur queue with its own connection
    blurQueue = new Queue(BLUR_BIDDING_QUEUE, {
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

      const worker = new Worker(
        QUEUE_NAME,
        async (job) => {
          const jobTimeout = job.name.endsWith('_SCHEDULE') ? 900000 : 60000; // 15 minutes for scheduled jobs, 60 seconds for otherscla
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
          concurrency: concurrencyPerWorker,
          lockDuration: 960000, // 16 minutes lock (covers 15-minute scheduled job timeout + buffer)
          stalledInterval: 120000, // Check for stalled jobs every 2 minutes
          maxStalledCount: 1 // Move to failed after 1 stall
        }
      );

      // Add error handlers for main workers
      worker.on('error', async (error) => {
        logSystemError('Worker', `Worker ${i + 1} error`, error);
        trackWorkerFailure(i, `Worker error: ${error.message || String(error)}`);

        // Check if this is a Redis error
        if (isRedisError(error)) {
          redisFailures++;

          if (redisFailures >= REDIS_FAILURE_THRESHOLD) {
            await handleRedisFailure();
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
        logSystemError('Worker', `Worker ${i + 1} - Task ${taskId} - Job ${job?.id} failed`, error, { taskId, jobId: job?.id });

        // Handle failed bid jobs - immediately mark as processed
        if (job && job.id && typeof job.id === 'string' && job.id.startsWith('bid-')) {

          // Extract task and marketplace from job ID (format: bid-taskId-marketplace-...)
          const parts = job.id.split('-');
          if (parts.length >= 3) {
            const taskId = parts[1];
            const marketplace = parts[2].toLowerCase();

            // Mark job as processed to allow loop completion
            markJobAsProcessed(taskId, marketplace, job.id, false).catch(err => {
              logSystemError('Worker', `Worker ${i + 1} failed to mark failed bid job as processed`, err);
            });
          }
        }

        // Handle failed cancellation jobs - track them as completed
        if (job && job.name && (job.name === CANCEL_OPENSEA_BID || job.name === CANCEL_MAGICEDEN_BID || job.name === CANCEL_BLUR_BID)) {
          const { taskId } = job.data;
          const marketplace = job.name === CANCEL_OPENSEA_BID ? 'opensea' :
            job.name === CANCEL_MAGICEDEN_BID ? 'magiceden' : 'blur';


          // Record the failed cancellation as a completion to ensure we reach 100%
          cancellationTracker.recordCompletion(taskId, 1, marketplace);
          updateTaskCancellationPercentage(taskId).catch(err => {
            const failedTaskId = getTaskIdForLog(job);
            logSystemError('CancellationTracking', `Failed to update cancellation percentage for task ${failedTaskId} after job failure`, err, { taskId: failedTaskId });
          });

          // Delete the order from database anyway to clean up
          if (job.data.orderKey) {
            Order.deleteOne({ _id: job.data.orderKey }).catch((err) => {
              const orderTaskId = getTaskIdForLog(job);
              logSystemError('OrderCleanup', `Failed to delete order for task ${orderTaskId} after cancellation failure`, err, { taskId: orderTaskId });
            });
          } else if (job.data.orderKeys) {
            Order.deleteMany({ _id: { $in: job.data.orderKeys } }).catch((err) => {
              const ordersTaskId = getTaskIdForLog(job);
              logSystemError('OrderCleanup', `Failed to delete orders for task ${ordersTaskId} after cancellation failure`, err, { taskId: ordersTaskId });
            });

            // Clean up MagicEden tracking set on failure
            if (job.name === CANCEL_MAGICEDEN_BID && job.data.orderKeys) {
              job.data.orderKeys.forEach((orderKey: string) => {
                magicedenOrdersBeingCancelled.delete(orderKey);
              });
            }
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
          logSystemError('BlurWorker', `Task ${blurJobTaskId} - Job ${job.id} failed`, error, { taskId: blurJobTaskId, jobId: job.id });
          // If it's a password error, throw it to fail the job properly
          if (error.code === 'NO_PASSWORD' || error.message?.includes('No password available')) {
            throw error;
          }
          // For other errors, also throw to ensure proper retry
          throw error;
        }
      },
      {
        connection: blurWorkerConnection,
        concurrency: concurrencyPerWorker, // Reduce concurrency for Blur to prevent rate limit issues
        lockDuration: 960000, // 16 minutes lock (covers 15-minute scheduled job timeout + buffer)
        stalledInterval: 120000, // Check for stalled jobs every 2 minutes
        maxStalledCount: 1 // Move to failed after 1 stall
      }
    );



    workers.push(blurWorker);

    // Add error handlers for Blur queue components
    blurWorker.on('error', async (error) => {
      logSystemError('BlurWorker', 'Worker error', error);
      const blurWorkerIndex = workers.length - 1; // Blur worker is the last one
      trackWorkerFailure(blurWorkerIndex, `Blur worker error: ${error.message || String(error)}`);

      // Check if this is a Redis error
      if (isRedisError(error)) {
        // Single node Redis - no cluster failure tracking needed
      }

      logger.logGeneralError('Blur worker error', {
        error: error,
        errorMessage: error.message || String(error),
        errorStack: error.stack
      });
    });

    blurWorker.on('failed', (job, error) => {
      const blurFailedTaskId = getTaskIdForLog(job);
      logSystemError('BlurWorker', `Task ${blurFailedTaskId} - Job ${job?.id} failed`, error, { taskId: blurFailedTaskId, jobId: job?.id });

      // Handle failed bid jobs - immediately mark as processed
      if (job && job.id && typeof job.id === 'string' && job.id.startsWith('bid-')) {

        // Extract task and marketplace from job ID (format: bid-taskId-marketplace-...)
        const parts = job.id.split('-');
        if (parts.length >= 3) {
          const taskId = parts[1];
          const marketplace = parts[2].toLowerCase();

          // Mark job as processed to allow loop completion
          markJobAsProcessed(taskId, marketplace, job.id, false).catch(err => {
            logSystemError('BlurWorker', 'Failed to mark failed bid job as processed', err);
          });
        }
      }

      // Handle failed Blur cancellation jobs - track them as completed
      if (job && job.name === CANCEL_BLUR_BID) {
        const { taskId } = job.data;


        // Record the failed cancellation as a completion to ensure we reach 100%
        cancellationTracker.recordCompletion(taskId, 1, 'blur');
        updateTaskCancellationPercentage(taskId).catch((err) => {
          const blurCancelTaskId = getTaskIdForLog(job);
          logSystemError('CancellationTracking', `Failed to update cancellation percentage for task ${blurCancelTaskId} after Blur job failure`, err, { taskId: blurCancelTaskId });
        });

        // Delete the order from database anyway to clean up
        if (job.data.orderKey) {
          Order.deleteOne({ _id: job.data.orderKey }).catch((err) => {
            const blurOrderTaskId = getTaskIdForLog(job);
            logSystemError('OrderCleanup', `Failed to delete Blur order for task ${blurOrderTaskId} after cancellation failure`, err, { taskId: blurOrderTaskId });
          });
        } else if (job.data.orderKeys) {
          Order.deleteMany({ _id: { $in: job.data.orderKeys } }).catch((err) => {
            const blurOrdersTaskId = getTaskIdForLog(job);
            logSystemError('OrderCleanup', `Failed to delete Blur orders for task ${blurOrdersTaskId} after cancellation failure`, err, { taskId: blurOrdersTaskId });
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
      logSystemError('BlurQueue', 'Queue error', error);
      logger.logGeneralError('Blur queue error', {
        error: error,
        errorMessage: error.message || String(error),
        errorStack: error.stack
      });
    });

    blurQueueEvents.on('error', (error) => {
      logSystemError('BlurQueueEvents', 'Events error', error);
      logger.logGeneralError('Blur queue events error', {
        error: error,
        errorMessage: error.message || String(error),
        errorStack: error.stack
      });
    });


    // Initialize worker health monitoring
    startWorkerHealthMonitoring();


    // Add process-level error handler for uncaught BullMQ errors
    process.on('unhandledRejection', async (reason, promise) => {
      if (reason instanceof Error && reason.message.includes('could not renew lock')) {
        await BullMQErrorFormatter.formatError(reason);
      } else {
        logSystemError('UnhandledRejection', 'Unhandled Rejection', reason, { promise: String(promise) });
      }
    });

    // Add uncaught exception handler for production stability
    process.on('uncaughtException', async (error) => {
      logSystemError('UncaughtException', 'Uncaught exception', error);

      try {
        // Log to Better Stack if possible
        await logger.logGeneralError('Uncaught exception', {
          error: error,
          errorMessage: error.message || String(error),
          errorStack: error.stack,
          critical: true
        });
      } catch (logError) {
        logSystemError('Logger', 'Failed to log uncaught exception', logError);
      }

      // Give some time for error logging before exiting
      logSystemError('UncaughtException', 'Server shutting down due to uncaught exception');

      // Try graceful shutdown
      try {
        // Update all running tasks to not running
        await Task.updateMany({ running: true }, { running: false });

        // Stop workers
        await Promise.all(workers.map(worker => worker.close()));

        // Close BullMQ connections
        await closeAllBullMQConnections();

        // Close MongoDB connection
        await mongoose.connection.close();

      } catch (shutdownError) {
        logSystemError('Shutdown', 'Error during graceful shutdown', shutdownError);
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
        serverSelectionTimeoutMS: 30000,
        connectTimeoutMS: 30000,
        socketTimeoutMS: 45000,

        // Connection pool optimization for high throughput (100+ QPS)
        maxPoolSize: Number(process.env.MONGO_MAX_POOL_SIZE) || 100, // Increased to handle high concurrency
        minPoolSize: Number(process.env.MONGO_MIN_POOL_SIZE) || 30, // More warm connections for faster startup
        maxIdleTimeMS: 60000, // Keep connections open longer for frequent queries
        waitQueueTimeoutMS: 10000, // Max wait time for connection from pool

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

      // Add connection pool monitoring for high-throughput operations
      mongoose.connection.on('disconnected', () => {
      });

      mongoose.connection.on('reconnected', () => {
      });

      mongoose.connection.on('error', (err) => {
        logSystemError('MongoDBConnection', 'MongoDB connection error', err);
      });

      // Log connection pool stats in development
      if (process.env.NODE_ENV === 'development') {
        setInterval(() => {
          const readyState = mongoose.connection.readyState;
          const stateMap = { 0: 'disconnected', 1: 'connected', 2: 'connecting', 3: 'disconnecting' };
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
        console.log('💡 Tasks/listings must be manually restarted via the dashboard');
      }
    } catch (error) {
      logSystemError('ServerRestart', 'Error stopping tasks/listings on server restart', error);
    }

    // Try to recover password from Redis BEFORE starting jobs
    let passwordRecovered = false;
    const maxRetries = 3;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {

      // Check Redis connection status
      if (!redisClient.isReady()) {
        await new Promise(resolve => setTimeout(resolve, 1000));
        continue;
      }

      passwordRecovered = await recoverPasswordFromRedis();

      if (passwordRecovered) {
        break;
      } else if (attempt < maxRetries) {
        await new Promise(resolve => setTimeout(resolve, attempt * 1000));
      }
    }

    // Check if password is set (either from Redis recovery or needs to be set)
    const currentPassword = getPassword();
    if (!currentPassword) {
      // Don't start jobs if there's no password
      console.log('⏸️  No password available - server in paused state');
      console.log('💡 Server will remain in paused state until password is provided via API');
    } else {
      if (passwordRecovered) {
        console.log('✅ Password recovered - operations will resume');
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
        logSystemError('WorkerHealth', `${unhealthyWorkers.length} workers are not ready`);
        unhealthyWorkers.forEach(w => {
          logSystemError('WorkerHealth', `Worker ${w.index}: running=${w.running}, paused=${w.paused}`);
        });

        // Try to resume paused workers
        for (const workerStatus of unhealthyWorkers) {
          if (!workerStatus.running || workerStatus.paused) {
            try {
              await workers[workerStatus.index].resume();
            } catch (error) {
              logSystemError('WorkerResume', `Failed to resume worker ${workerStatus.index}`, error);
            }
          }
        }
      } else {
      }

      // Verify Redis connections one more time
      const finalHealthCheck = await checkBullMQConnectionsHealth();
      const finalUnhealthy = Object.entries(finalHealthCheck).filter(([_, status]) => !status.healthy);

      if (finalUnhealthy.length > 0) {
        logSystemError('RedisHealth', 'Some Redis connections are still unhealthy');
        finalUnhealthy.forEach(([id, status]) => {
          logSystemError('HealthCheck', `Redis connection ${id}: ${status.error}`);
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
      logSystemError('RedisOperation', 'Redis operation failed', redisError);
    }

    server.listen(port, async () => {
      console.log(`Server running on port ${port}`);
      console.log(`Environment: ${process.env.NODE_ENV || 'development'}`);
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
      logSystemError('WebSocket', 'WebSocket connection failed', error);
    }

    // Initialize listing scheduler with WebSocket manager (only if password available)
    if (currentPassword) {
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
    } else {
      console.log('⏸️  Listing scheduler skipped - password required');
      await logger.logInfo(ErrorCategory.GENERAL, 'Listing scheduler skipped - no password', {
        schedulerType: 'listing_scheduler',
        reason: 'no_password'
      });
    }

    // Initialize expiration monitor
    expirationMonitorInterval = initializeExpirationMonitor();
    await logger.logInfo(ErrorCategory.GENERAL, 'Expiration monitor initialized', {
      schedulerType: 'expiration_monitor',
      checkInterval: 5 * 60 * 1000 // 5 minutes
    });

    // Initialize expired price cleanup
    expiredPriceCleanupInterval = initializeExpiredPriceCleanup();
    await logger.logInfo(ErrorCategory.GENERAL, 'Expired price cleanup initialized', {
      schedulerType: 'expired_price_cleanup',
      checkInterval: 5 * 60 * 1000 // 5 minutes
    });

    // Initialize orphaned job recovery system
    orphanedJobRecovery.startRecovery(30000); // Check every 30 seconds
    await logger.logInfo(ErrorCategory.GENERAL, 'Orphaned job recovery initialized', {
      recoveryType: 'orphaned_jobs',
      checkInterval: 30000 // 30 seconds
    });

    // Start internet connectivity monitoring
    startConnectivityMonitoring();

    // Redis cluster monitoring disabled - using single node
    // startRedisClusterMonitoring();

    await fetchCurrentTasks()

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

    // Check for orphaned scheduled tasks (tasks stuck in SCHEDULED state without timers)
    const orphanedScheduledTasks = Array.from(activeTasks.entries())
      .filter(([taskId, task]) => {
        const status = taskStatus[taskId];
        const hasTimer = taskTimeouts.has(taskId);

        // Check if any marketplace is in SCHEDULED state
        const hasScheduledMarketplace = status?.marketplaces && Object.values(status.marketplaces).some(
          mpStatus => mpStatus?.status === 'SCHEDULED'
        );

        return task.running &&
          hasScheduledMarketplace &&
          !hasTimer;
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
              logSystemError('ListingRestart', `Failed to restart listing ${listing._id}`, error, { listingId: listing._id });
            }
          }, i * 500);
        }
      } else {
      }
    } catch (error) {
      logSystemError('ListingFetch', 'Failed to fetch running listings', error);
    }

    // Start password auto-recovery checker now that Redis is connected
    setInterval(checkPasswordAndResume, PASSWORD_CHECK_INTERVAL);


    // Start stuck task detection
    const STUCK_TASK_CHECK_INTERVAL = 30000; // 30 seconds
    setInterval(checkStuckTasks, STUCK_TASK_CHECK_INTERVAL);


  } catch (error) {
    logSystemError('ServerStartup', 'Failed to start server', error);
    process.exit(1);
  }
}

// Cleanup handler for graceful shutdown
process.on('SIGINT', async () => {

  // Update all running tasks to not running
  try {
    const result = await Task.updateMany(
      { running: true },
      { running: false }
    );
  } catch (error) {
    logSystemError('TaskStateUpdate', 'Error updating task states', error);
  }

  // Stop connectivity monitoring
  stopConnectivityMonitoring();

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

  // Disconnect WebSocket
  if (wsManager) {
    wsManager.disconnect();
  }

  // Close all BullMQ connections
  for (const connection of bullMQConnections) {
    try {
      await connection.quit();
    } catch (error) {
      logSystemError('ConnectionClose', 'Error closing connection', error);
    }
  }

  // Final cleanup before exit
  seenOrderHashes.clear();
  seenBids.clear();

  process.exit(0);
});

process.on('SIGTERM', async () => {

  // Update all running tasks to not running
  try {
    const result = await Task.updateMany(
      { running: true },
      { running: false }
    );
  } catch (error) {
    logSystemError('TaskStateUpdate', 'Error updating task states', error);
  }

  // Stop connectivity monitoring
  stopConnectivityMonitoring();

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

  // Disconnect WebSocket
  if (wsManager) {
    wsManager.disconnect();
  }

  // Close all BullMQ connections
  for (const connection of bullMQConnections) {
    try {
      await connection.quit();
    } catch (error) {
      logSystemError('ConnectionClose', 'Error closing connection', error);
    }
  }

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
    logSystemError('ServerStartup', 'Failed to start server', error);
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
      const activeOrderCount = await Order.countDocuments({ taskId, expiresAt: { $gt: new Date() } });

      if (activeOrderCount > 0) {
        return false;
      }
    } catch (error) {
      logSystemError('TaskStatusCheck', `Error checking active orders for task ${taskId}`, error, { taskId });
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
      logSystemError('TaskStatusTransition', `Invalid state transition: ${currentMarketplaceStatus} -> ${newStatus} for task ${taskId}/${marketplace}`, undefined, { taskId, marketplace, currentStatus: currentMarketplaceStatus, newStatus });
      logSystemError('TaskStatusTransition', `Context: ${context || 'No context provided'}`);
      logSystemError('TaskStatusTransition', `Called from: ${stackTrace}`);
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

    const loopCount = loopCounterStats[taskId]?.[marketplace] || 0;

    return true;
  }

  // Marketplace parameter is required
  logSystemError('TaskStatus', `Marketplace parameter is required for task ${taskId}`, undefined, { taskId });
  logSystemError('TaskStatus', `Called from: ${stackTrace}`);
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
        const activeOrderCount = await Order.countDocuments({ taskId, marketplace: marketplace.toLowerCase(), expiresAt: { $gt: new Date() } });

        if (activeOrderCount > 0) {
          return;
        }
      } catch (error) {
        logSystemError('MarketplaceCancellation', `Error checking active orders for marketplace ${marketplace} - task ${taskId}`, error, { taskId, marketplace });
        // If we can't verify, don't mark as complete
        return;
      }
    }

    // Let the cancellation tracker handle percentage updates naturally for marketplace-specific cancellation

    // After cancellation is complete, transition back to STOPPED
    const task = activeTasks.get(taskId);
    setTaskStatus(taskId, 'STOPPED', 'marketplace-specific cancellation complete', marketplace);
    // Reset cancellation percentage
    await updateTaskCancellationPercentage(taskId, 0);

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
function cleanupTaskResources(taskId: string, preserveWarnings = false): void {
  const timestamp = new Date().toISOString();

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
  }

  // Clear and delete countdown timers (both legacy and marketplace-specific)
  // First clear the legacy timer (if any)
  const countdownTimer = taskCountdownTimers.get(taskId);
  if (countdownTimer) {
    clearInterval(countdownTimer);
    taskCountdownTimers.delete(taskId);
  }

  // Then clear any marketplace-specific timers
  const marketplacesList = ['opensea', 'magiceden', 'blur'];
  for (const marketplace of marketplacesList) {
    const timerKey = `${taskId}-${marketplace}`;
    const marketplaceTimer = taskCountdownTimers.get(timerKey);
    if (marketplaceTimer) {
      clearInterval(marketplaceTimer);
      taskCountdownTimers.delete(timerKey);
    }
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
  });

  // Clean up cancellation tracking
  marketplaces.forEach(marketplace => {
    cancellationTracker.clearTracking(taskId, marketplace);
  });

  // Clean up restart counter
  taskRestartCounts.delete(taskId);

}

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

  // Log marketplace filtering for updateTaskRunningPercentage

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

        // Comprehensive logging for marketplace-specific status
      }

      totalJobs += mpTotal;
      completedJobs += mpCompleted;

      // Additional safety check for individual marketplace overflow
      if (mpCompleted > mpTotal && mpTotal > 0) {
      }

      // Check if this specific marketplace is complete
      if (mpTotal > 0 && mpCompleted >= mpTotal) {
        const marketplaceStatus = taskStatus[taskId]?.marketplaces?.[marketplace as 'opensea' | 'magiceden' | 'blur'];
        if (marketplaceStatus && marketplaceStatus.status === 'RUNNING') {
          // Trigger marketplace-specific completion
          checkTaskCompletionAndScheduleNext(taskId, marketplace);
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
            logSystemError('TaskRecovery', `Failed to restart task ${taskId}`, error, { taskId });
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

  const task = activeTasks.get(taskId);
  if (!task) return;

  // Create unique key for marketplace-specific or global completion
  const completionKey = marketplace ? `${taskId}-${marketplace}` : taskId;

  // Check if task/marketplace is already in completion process
  if (tasksInCompletionProcess.has(completionKey)) {
    return;
  }

  // Mark task/marketplace as in completion process
  tasksInCompletionProcess.add(completionKey);

  try {
    if (!marketplace) {
      logSystemError('TaskCompletion', `Called without marketplace parameter for task ${taskId}`, undefined, { taskId });
      tasksInCompletionProcess.delete(completionKey);
      return;
    }

    // Log completion transition
    const currentLoopCount = loopCounterStats[taskId]?.[marketplace] || 1;

    // Marketplace-specific completion only

    // Log current marketplace status before completion
    const mpStatus = taskStatus[taskId]?.marketplaces?.[marketplace as 'opensea' | 'magiceden' | 'blur'];

    setTaskStatus(taskId, 'COMPLETED', `marketplace completion - Loop #${currentLoopCount}`, marketplace);

    // Add 0.5-second delay before transitioning to SCHEDULED
    await new Promise(resolve => setTimeout(resolve, 500));

    // Schedule next loop for this marketplace
    await scheduleNextLoop(taskId, marketplace);

    // Mark marketplace as scheduled
    const nextLoopCount = (loopCounterStats[taskId]?.[marketplace] || 0) + 1;
    setTaskStatus(taskId, 'SCHEDULED', `next loop scheduled - Loop #${nextLoopCount}`, marketplace);

    // Add delay to ensure scheduled processors see SCHEDULED status before parent jobs are cleared
    await new Promise(resolve => setTimeout(resolve, 100));

    if (marketplace) {
      // Clear parent job for specific marketplace
      const parentJobId = getParentJobId(taskId, marketplace);
      if (parentJobs.has(parentJobId)) {
        parentJobs.delete(parentJobId);
        // Also clear processed jobs tracking
        processedJobsPerParent.delete(parentJobId);
      }
    } else {
      // Clear parent jobs for all marketplaces
      const marketplaces = ['opensea', 'magiceden', 'blur'];
      for (const mp of marketplaces) {
        const parentJobId = getParentJobId(taskId, mp);
        if (parentJobs.has(parentJobId)) {
          parentJobs.delete(parentJobId);
          // Also clear processed jobs tracking
          processedJobsPerParent.delete(parentJobId);
        }
      }
    }
  } catch (error) {
    logSystemError('TaskCompletion', `Error during completion process for ${completionKey}`, error, { completionKey });
    // Remove from completion process on error
    tasksInCompletionProcess.delete(completionKey);
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
  const { currencySymbol } = await calculateBidPrice(task, 0, "opensea");

  let totalCancellationJobs = 0;

  // Cancel removed trait bids
  if (traitsToRemove[taskId]?.length > 0) {
    const traitJobs = await cancelRemovedTraitBids(traitsToRemove[taskId], task._id, task.walletId, currencySymbol);
    totalCancellationJobs += traitJobs;
  }

  // Cancel removed token bids
  if (removedTokens[taskId]?.opensea?.length > 0 || removedTokens[taskId]?.magiceden?.length > 0) {
    const tokenJobs = await cancelRemovedTokenBids(removedTokens[taskId], task._id, task.walletId, currencySymbol);
    totalCancellationJobs += tokenJobs;
  }

  return totalCancellationJobs;
}

// Helper function to mark a job as processed regardless of outcome
async function markJobAsProcessed(taskId: string, marketplace: string, jobId: string, success: boolean = true) {
  const timestamp = new Date().toISOString();
  const currentLoopCount = loopCounterStats[taskId]?.[marketplace] || 1;

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
        // Process it with the old parent job
        parentJob = oldParentJob;
        parentJobId = oldParentJobId;
      } else {
        return;
      }
    } else {
      return;
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
      parentJob.allJobsQueued = true; // Fix the state
    }
    return;
  }

  if (taskStatus[taskId]) {
    // Transition marketplace to RUNNING when job is processed
    // This is the ONLY place where a marketplace should transition to RUNNING
    setTaskStatus(taskId, 'RUNNING', 'job marked as processed', marketplace);
  }

  // CRITICAL: Use a mutex lock to prevent race conditions during counter increment
  const counterLockKey = `counter-lock:${parentJobId}`;
  const lockManager = new DistributedLockManager({
    lockPrefix: 'counter:',
    defaultTTLSeconds: 5
  });

  const lockAcquired = await lockManager.acquireLock(counterLockKey, 5);

  if (!lockAcquired) {
    const lockTaskId = getTaskIdForLog({ data: { taskId, _id: taskId } });
    logSystemError('JobProcessing', `CRITICAL: Could not acquire lock for counter increment after retries! Job ${jobId} may not be counted`, undefined, { taskId: lockTaskId, jobId });
    // Continue anyway to prevent job from being stuck, but log this critical error
    await logger.logError(ErrorCategory.PERFORMANCE, 'Failed to acquire counter lock after retries', {
      parentJobId,
      jobId,
      taskId,
      operation: 'markJobAsProcessed'
    });
  }

  try {

    // Re-fetch parent job inside the lock to get latest values
    parentJob = parentJobs.get(parentJobId);
    if (!parentJob) {
      const missingTaskId = getTaskIdForLog({ data: { taskId, _id: taskId } });
      logSystemError('JobProcessing', `Parent job disappeared during lock acquisition! Task ${missingTaskId}`, undefined, { taskId: missingTaskId });
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
        logSystemError('LockRelease', `Error releasing counter lock for task ${releaseTaskId}`, error, { taskId: releaseTaskId });
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
    logSystemError('JobProcessing', `Error updating task percentage for task ${percentTaskId}`, error, { taskId: percentTaskId });
    logSystemError('JobProcessing', `Task ${percentTaskId}: This may cause stuck percentage!`, undefined, { taskId: percentTaskId });
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
      // Get overall percentage from marketplaces
      const marketplaceStatuses = taskStatus[taskId]?.marketplaces;
      const overallPercentage = marketplaceStatuses ?
        Math.max(...Object.values(marketplaceStatuses).map(mp => mp?.runningPercentage || 0)) : 0;
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

async function scheduleNextLoop(taskId: string, marketplace?: string): Promise<void> {
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
    loopInterval = MIN_LOOP_INTERVAL;
  }

  // Increment loop counter for the specific marketplace that is scheduling its next loop
  if (marketplace) {
    // Initialize task counters if they don't exist
    if (!loopCounterStats[taskId]) {
      loopCounterStats[taskId] = {};
    }

    // Increment the counter for this specific marketplace
    if (!loopCounterStats[taskId][marketplace]) {
      loopCounterStats[taskId][marketplace] = 0;
    }
    loopCounterStats[taskId][marketplace]++;

    const newLoopCount = loopCounterStats[taskId][marketplace];
  } else {
  }

  // Set up next loop time and countdown timer
  const nextLoopTime = new Date(Date.now() + loopInterval * 1000);
  const secondsUntilNextLoop = Math.floor((nextLoopTime.getTime() - Date.now()) / 1000);


  // Update task status
  if (taskStatus[taskId]) {
    if (marketplace) {
      // Schedule specific marketplace
      setTaskStatus(taskId, 'SCHEDULED', 'next loop scheduled', marketplace);
      const mpKey = marketplace as 'opensea' | 'magiceden' | 'blur';
      if (taskStatus[taskId].marketplaces && taskStatus[taskId].marketplaces[mpKey]) {
        taskStatus[taskId].marketplaces[mpKey].waitingTime = secondsUntilNextLoop;
        taskStatus[taskId].marketplaces[mpKey].nextLoopTime = nextLoopTime;

        // Log other marketplace statuses
        for (const [mp, status] of Object.entries(taskStatus[taskId].marketplaces)) {
          if (mp !== marketplace && status) {
            const waitingDisplay = status.waitingTime !== undefined ? `${status.waitingTime}s` : 'N/A';
          }
        }
      }
    } else {
      // Schedule all marketplaces
      for (const mp of task.selectedMarketplaces) {
        const mpLower = mp.toLowerCase() as 'opensea' | 'magiceden' | 'blur';
        setTaskStatus(taskId, 'SCHEDULED', 'next loop scheduled', mpLower);
        if (taskStatus[taskId].marketplaces && taskStatus[taskId].marketplaces[mpLower]) {
          taskStatus[taskId].marketplaces[mpLower].waitingTime = secondsUntilNextLoop;
          taskStatus[taskId].marketplaces[mpLower].nextLoopTime = nextLoopTime;
        }
      }
    }
  }

  // Create a unique key for marketplace-specific timers
  const timerKey = marketplace ? `${taskId}-${marketplace}` : taskId;

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

  // Validate task is still active before scheduling
  if (!activeTasks.has(taskId)) {
    cleanupTaskResources(taskId);
    lastScheduleTimes.delete(taskId);
    return;
  }

  try {
    // Schedule the next iteration with setTimeout
    const timeoutId = setTimeout(async () => {

      // Acquire scheduling lock when timer expires, not before
      // Use marketplace-specific lock key if marketplace is provided
      const lockKey = marketplace ? `${taskId}-${marketplace}` : taskId;
      if (!acquireLoopSchedulingLock(lockKey)) {
        return;
      }

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
          // Clear recently completed flag before starting next loop
          if (marketplace) {
            // Start only the specific marketplace that triggered this timer
            await startTask(currentTask, true, marketplace);
          } else {
            // Start all marketplaces (legacy behavior)
            await startTask(currentTask, true);
          }
        }
      } catch (error) {
        logSystemError('TaskScheduler', `Error in loop execution for task ${taskId}`, error, { taskId });
      } finally {
        // Always release the lock after execution
        releaseLoopSchedulingLock(lockKey);
      }
    }, loopInterval * 1000);

    taskTimeouts.set(timerKey, timeoutId);
  } catch (error) {
    // If scheduling fails, clean up resources
    logSystemError('TaskScheduler', `Error scheduling next loop for task ${taskId}`, error, { taskId });
    lastScheduleTimes.delete(taskId);
  }
}


// Initialize queue event handlers - called after queue is created
function initializeQueueEventHandlers() {
  queueEvents.on('completed', async ({ jobId }) => {
    try {
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

      // Log all job completions for debugging

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

      logSystemError('QueueEvent', `Error handling job completion for job ${jobId}, task ${extractedTaskId}`, error, { taskId: extractedTaskId, jobId });
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


        // Write to scheduled job failures log
        const logPath = path.join(logsDir, 'scheduled-job-failures.log');
        fs.appendFileSync(logPath, JSON.stringify(errorDetails, null, 2) + '\n---\n');

        // Handle timeout failures by restarting the task
        if (failedReason?.includes('timed out after') && jobId.includes('-schedule')) {
          const taskId = jobId.split('-')[0];

          // Use a distributed lock to prevent multiple restarts
          const restartLockKey = `schedule-timeout-restart:${taskId}`;
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
                      // Ignore errors - lock may not exist
                    }
                  }

                  // Reset task status for all marketplaces and restart
                  for (const mp of task.selectedMarketplaces) {
                    setTaskStatus(taskId, 'SCHEDULED', 'restarting after timeout', mp.toLowerCase());
                  }
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
            }
          } catch (lockError) {
            logSystemError('QueueEvent', `Error acquiring restart lock for task ${taskId}`, lockError, { taskId });
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

      logSystemError('QueueEvent', `Error handling job failure for job ${jobId}, task ${extractedTaskId}`, error, { taskId: extractedTaskId, jobId });
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
      logSystemError('QueueEvent', `Error handling stalled job ${jobId}, task ${stalledTaskId}`, error, { taskId: stalledTaskId, jobId });
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

  blurQueueEvents.on('completed', async ({ jobId }) => {
    try {
      // Handle Blur schedule job completions for sequential execution
      if (jobId.includes('-schedule')) {
        if (jobId.includes('-blur-schedule')) {
          const taskId = jobId.replace('-blur-schedule', '');
        }
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
          await markJobAsProcessed(taskId, marketplace, jobId);
        }
      }
    } catch (error) {
      logSystemError('BlurQueueEvent', `Error handling job completion for job ${jobId}`, error, { jobId });
      // Stack trace included in error object above
      // Error object already logged

      // Log error details for debugging
      if (error instanceof Error) {
        // Error details included in error object above
      }

      // Don't let errors in event handler crash the process
      // This ensures other jobs can still be processed
    }
  });

  blurQueueEvents.on('failed', async ({ jobId, failedReason }) => {
    try {
      // Skip schedule jobs - they are orchestration jobs, not actual bid jobs
      if (jobId.includes('-schedule')) {
        return;
      }

      // Log all job failures for debugging

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
          await safeMarkJobAsProcessed(taskId, marketplace, jobId, false, async (tid, mp, increment) => {
            await markJobAsProcessed(tid, mp, jobId, false);
          });
        }
      } else {
      }
    } catch (error) {
      logSystemError('BlurQueueEvent', `Error handling job failure for job ${jobId}`, error, { jobId });
      // Stack trace included in error object above
      // Error object already logged

      // Log error details for debugging
      if (error instanceof Error) {
        // Error details included in error object above
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
  loopCounterStats,
  counterbidCounts
};

// Debounced broadcasting system for efficient real-time updates
let lastBroadcastData: string | null = null;
const HEARTBEAT_INTERVAL_MS = 30000; // 30 second heartbeat for connection health


async function broadcastBidRates() {
  const broadcastStart = Date.now();

  bidRates = getAllBidRates();


  const currentData = {
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
    counterbidCounts,
    timestamp: Date.now() // Add timestamp for client to track data freshness
  };

  let message: string;
  try {
    message = JSON.stringify({
      type: 'bidRatesUpdate',
      data: currentData
    });
  } catch (error) {
    logSystemError('BroadcastBidRates', 'Error stringifying data', error);
    // Log which property might be causing the issue
    for (const [key, value] of Object.entries(currentData)) {
      try {
        JSON.stringify(value);
      } catch (e) {
        logSystemError('BroadcastBidRates', `Error stringifying ${key}`, e, { key });
      }
    }
    return;
  }

  // Change detection - only broadcast if data actually changed
  if (lastBroadcastData === message) {
    return;
  }

  // Change detection - only broadcast if data actually changed
  if (lastBroadcastData !== message) {
    lastBroadcastData = message;

    let activeSockets = 0;
    clients.forEach(client => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(message);
        activeSockets++;
      }
    });

    const broadcastDuration = Date.now() - broadcastStart;
  }
}

// Periodic heartbeat to maintain WebSocket connections and ensure clients stay connected
setInterval(() => {
  const heartbeat = JSON.stringify({
    type: 'heartbeat',
    timestamp: Date.now()
  });

  let activeConnections = 0;
  clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(heartbeat);
      activeConnections++;
    }
  });

  if (activeConnections > 0) {
  }
}, HEARTBEAT_INTERVAL_MS);

// Real-time bid statistics broadcast - sends updates every second
const REALTIME_BROADCAST_INTERVAL_MS = 1000; // 1 second
setInterval(() => {
  // Only broadcast if we have connected clients
  if (clients.size > 0) {
    // Trigger broadcast without debounce for real-time updates
    broadcastBidRates();
  }
}, REALTIME_BROADCAST_INTERVAL_MS);

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
        loopCounterStats,
        counterbidCounts
      },
    });

    ws.send(initialMessage);
  }

  // Helper functions for message idempotency
  function isDuplicateMessage(messageId: string): boolean {
    const timestamp = processedMessages.get(messageId);
    if (!timestamp) return false;

    // Check if message is still within TTL
    if (Date.now() - timestamp > MESSAGE_TTL) {
      processedMessages.delete(messageId);
      return false;
    }

    return true;
  }

  function markMessageProcessed(messageId: string): void {
    processedMessages.set(messageId, Date.now());

    // Cleanup old entries
    if (processedMessages.size > 10000) {
      const cutoff = Date.now() - MESSAGE_TTL;
      for (const [id, timestamp] of processedMessages.entries()) {
        if (timestamp < cutoff) {
          processedMessages.delete(id);
        }
      }
    }
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
      logger.logApiError("WebSocket message handler failed", {
        error: error as Error,
        endpoint,
        messageId,
        messageType: typeof event?.data,
        rawData: event?.data?.toString()?.substring(0, 100)
      });

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
        logger.logApiError("Failed to send error acknowledgment", {
          error: sendError as Error
        });
      }
    }
  };

  ws.onclose = () => {
    // Remove client when disconnected
    clients.delete(ws);
  };
});

// Blur rate limiting is now handled by Redis and the BlurRateLimiter
// No need for in-memory tracking as we trust Blur's API responses

// Process Blur jobs
async function processBlurJob(job: Job) {
  // Declare walletAddress outside try block so it's accessible in catch/finally
  let walletAddress = '';
  const startTime = Date.now();
  const taskId = job.data?.taskId || job.data?._id || 'unknown';

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
        logError('blur', taskId, 'processBlurJob', 'Cannot determine wallet address for job', undefined, { jobId: job.id, jobParts, parsedWallet });
        logSystemError('BlurJob', `Cannot determine wallet address for job ${job.id}`, undefined, { jobId: job.id });
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

        try {
          // Reschedule the job with a delay
          await job.moveToDelayed(Date.now() + waitTime + 1000); // Add 1s buffer

        } catch (error) {
          // If we can't move to delayed (e.g., lock already released), job will be retried automatically

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
          const authBlockKey = `blurAuth:block:${walletAddress}`;
          const authBlockTime = await redis.get(authBlockKey);
          if (authBlockTime) {
            const blockTime = parseInt(authBlockTime);
            if (blockTime > Date.now()) {
              const waitTime = blockTime - Date.now();
              const waitSeconds = Math.round(waitTime / 1000);



              // Update progress periodically during wait
              // Progress reporting removed - was: const updateInterval = setInterval(() => job.updateProgress(20), 10000);
              await new Promise(resolve => setTimeout(resolve, waitTime + 2500));
              // Progress reporting removed - was: clearInterval(updateInterval);


            } else {
              // Block has expired, clean it up
              await redis.del(authBlockKey);

            }
          }

          // Update progress after auth check
          // Progress reporting removed - await job.updateProgress(25);

          // Then check rate limit pauses
          const pauseKey = `blurWallet:pause:${walletAddress}`;
          const redisPauseUntil = await redis.get(pauseKey);
          if (redisPauseUntil) {
            const pauseTime = parseInt(redisPauseUntil);
            if (pauseTime > Date.now()) {
              const waitTime = pauseTime - Date.now();
              const waitSeconds = Math.round(waitTime / 1000);


              // Enhanced logging with more details

              // Update progress periodically during wait
              // Progress reporting removed - was: const updateInterval = setInterval(() => job.updateProgress(30), 10000);
              await new Promise(resolve => setTimeout(resolve, waitTime + 2500));
              // Progress reporting removed - was: clearInterval(updateInterval);


            } else {
              // Pause has expired, clean it up
              await redis.del(pauseKey);

            }
          }
        }
      } catch (error) {

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
        const unknownError = new Error(`Unknown Blur job type: ${job.name}`);

        throw unknownError;
    }
  } catch (error) {
    logSystemError('BlurJob', `Job error - ${job.name}`, error, { jobName: job.name });

    logError('blur', taskId, 'processBlurJob', 'Job processing failed', error, {
      jobName: job.name,
      jobId: job.id,
      walletAddress,
      errorCode: (error as any).code,
      duration: Date.now() - startTime
    });

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

    const result = await (async () => {
      switch (job.name) {
        case OPENSEA_COLLECTION_BID:
          await processOpenseaCollectionBid(job.data);
          return { completed: true };
        case MAGICEDEN_COLLECTION_BID:
          await processMagicedenCollectionBid(job.data);
          return { completed: true };
        case OPENSEA_COLLECTION_BID_COUNTERBID:
          await openseaCollectionCounterBid(job.data);
          return { completed: true };
        case MAGICEDEN_COLLECTION_BID_COUNTERBID:
          await magicedenCollectionCounterBid(job.data);
          return { completed: true };
        case OPENSEA_TOKEN_BID_COUNTERBID:
          await openseaTokenCounterBid(job.data);
          return { completed: true };
        case OPENSEA_TRAIT_BID_COUNTERBID:
          await openseaTraitCounterBid(job.data);
          return { completed: true };
        case MAGICEDEN_TOKEN_BID_COUNTERBID:
          await magicedenTokenCounterBid(job.data);
          return { completed: true };
        case MAGICEDEN_TRAIT_BID_COUNTERBID:
          await magicedenTraitCounterBid(job.data);
          return { completed: true };
        case OPENSEA_SCHEDULE:
          await processOpenseaScheduledBid(job.data, job);
          return { completed: true };
        case OPENSEA_TRAIT_BID:
          await processOpenseaTraitBid(job.data);
          return { completed: true };
        case OPENSEA_TOKEN_BID:
          await processOpenseaTokenBid(job.data);
          return { completed: true };
        case MAGICEDEN_SCHEDULE:
          await processMagicedenScheduledBid(job.data, job.id as string, job);
          return { completed: true };
        case MAGICEDEN_TRAIT_BID:
          await processMagicedenTraitBid(job.data);
          return { completed: true };
        case MAGICEDEN_TOKEN_BID:
          await processMagicedenTokenBid(job.data);
          return { completed: true };
        case CANCEL_OPENSEA_BID:
          try {
            await bulkCancelOpenseaBid(job.data);
          } catch (error) {
            logSystemError('CancelOpenseaBid', 'Failed to cancel order', error);
            // Track the failure as a completion to ensure we reach 100%
            const { taskId } = job.data;
            cancellationTracker.recordCompletion(taskId, 1, 'opensea');
            await updateTaskCancellationPercentage(taskId);
            throw error; // Re-throw to let the job fail properly
          }
          return { completed: true };
        case CANCEL_MAGICEDEN_BID:
          try {
            // Log when CANCEL_MAGICEDEN_BID job is being processed
            const { taskId, orderIds } = job.data;

            await bulkCancelMagicedenBid(job.data);
          } catch (error) {
            logSystemError('CancelMagicedenBid', 'Failed to cancel order', error);
            const { taskId } = job.data;
            cancellationTracker.recordCompletion(taskId, 1, 'magiceden');
            await updateTaskCancellationPercentage(taskId);
            throw error; // Re-throw to let the job fail properly
          }
          return { completed: true };
        case CANCEL_BLUR_BID:
          try {
            // Log when CANCEL_BLUR_BID job is being processed
            const { taskId, orderKey } = job.data;

            await bulkCancelBlurBid(job.data);
          } catch (error) {
            logSystemError('CancelBlurBid', 'Failed to cancel order', error);
            const { taskId } = job.data;
            cancellationTracker.recordCompletion(taskId, 1, 'blur');
            await updateTaskCancellationPercentage(taskId);
            throw error; // Re-throw to let the job fail properly
          }
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
        logSystemError('ListingSchedule', `Failed to ensure scheduled job for listing ${listingId}`, scheduleError, { listingId });
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
    logSystemError('ListingUpdate', `Failed to process listing update for ${listingId}`, error, { listingId });
    throw error;
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

  await Promise.all(
    Array.from(activeTasks.entries())
      .filter(([_, task]) => task.running)
      .map(([_, task]) => startTask(task, true))
  );
}

async function processImportedTasks(tasks: ITask[]) {
  try {

    tasks.forEach(task => {
      activeTasks.set(task._id, task);
      currentTasks.push(task);
    });
    updateTaskIndexes(currentTasks);

    // Start all running tasks concurrently
    const runningTasks = tasks.filter(task => task.running);
    if (runningTasks.length > 0) {
      await Promise.all(
        runningTasks.map(task => startTask(task, true))
      );
    }

  } catch (error) {
    logSystemError('TaskImport', 'Error processing imported tasks', error);
  }
}


async function processNewTask(task: ITask) {
  try {
    activeTasks.set(task._id, task)
    currentTasks.push(task);
    updateTaskIndexes(currentTasks);
    await startTask(task, task.running)
  } catch (error) {
    logSystemError('TaskProcessing', `Error processing new task: ${task.contract.slug}`, error, { contractSlug: task.contract.slug });
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
        logSystemError('MarketplaceProcessing', `Error processing marketplace ${marketplaceToProcess} for task ${task._id}`, error, { marketplace: marketplaceToProcess, taskId: task._id });
      }
    }));

    // Enabled marketplaces already updated at the beginning
  } catch (error) {
    logSystemError('GlobalMarketplaceProcessing', 'Error processing global marketplaces', error);
  }
}

async function cancelRemovedTokenBids(removedTokens: { opensea: string[]; magiceden: string[] }, taskId: string, walletId: string, currencySymbol: string): Promise<number> {
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
          data: { walletId: walletId, orderId: order.orderHash, taskId, orderKey: order._id }
        };
      });
      // Log cancellation enqueue
      await queue.addBulk(cancelData);
      totalJobsCreated += cancelData.length;
    }

    for (let i = 0; i < magicedenTokensToCancel.length; i += 500) {
      const batchOrders = magicedenTokensToCancel.slice(i, i + 500);
      const extractedOrderIds = await extractMagicedenOrderHash(batchOrders)
      const orderKeys = batchOrders.map((order: any) => order._id)
      await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, walletId: walletId, orderKeys, taskId: taskId });
      totalJobsCreated++;
    }
  } catch (error) {
    logSystemError('TokenBidCancellation', 'Error canceling removed token bids', error);
  }
  return totalJobsCreated;
}

async function cancelRemovedTraitBids(traitsToRemove: string[], taskId: string, walletId: string, currencySymbol: string): Promise<number> {
  let totalJobsCreated = 0;
  try {
    const ordersToCancel = await Order.find({
      taskId: taskId,
      identifier: { $in: traitsToRemove }
    })


    const blurOrders = ordersToCancel.filter(order => order.marketplace.toLowerCase() === BLUR.toLowerCase());
    const openseaOrders = ordersToCancel.filter(order => order.marketplace.toLowerCase() === OPENSEA.toLowerCase());
    const magicedenOrders = ordersToCancel.filter(order => order.marketplace.toLowerCase() === MAGICEDEN.toLowerCase());

    if (blurOrders.length > 0) {
      const cancelData = blurOrders.map((order) => {
        const payload = JSON.parse(order.orderHash);
        return {
          name: CANCEL_BLUR_BID,
          data: { walletId: walletId, payload, taskId, orderKey: order._id }
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
          data: { walletId: walletId, orderId: order.orderHash, taskId, orderKey: order._id }
        };
      });
      // Log cancellation enqueue
      await queue.addBulk(cancelData);
      totalJobsCreated += cancelData.length;
    }

    for (let i = 0; i < magicedenOrders.length; i += 500) {
      const batchOrders = magicedenOrders.slice(i, i + 500);
      const extractedOrderIds = await extractMagicedenOrderHash(batchOrders)
      const orderKeys = batchOrders.map((order: any) => order._id)
      await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, walletId: walletId, orderKeys, taskId });
      totalJobsCreated++;
    }
  } catch (error) {
    logSystemError('TraitBidCancellation', 'Error canceling removed trait bids', error);
  }
  return totalJobsCreated;
}


async function updateTaskData(task: ITask) {
  if (process.env.NODE_ENV === 'development') {
    console.log(`[TaskUpdate][${task._id}] Starting task update - Contract: ${task.contract?.slug || 'unknown'}`);
    console.log(`[TaskUpdate][${task._id}] Update payload:`, {
      bidPrice: task.bidPrice,
      openseaBidPrice: task.openseaBidPrice,
      blurBidPrice: task.blurBidPrice,
      magicEdenBidPrice: task.magicEdenBidPrice,
      running: task.running,
      walletId: task.walletId
    });

    console.log(`[TaskUpdate][${task._id}] Step 1: Creating MongoDB session...`);
  }
  const session = await mongoose.startSession();
  if (process.env.NODE_ENV === 'development') {
    console.log(`[TaskUpdate][${task._id}] Step 2: MongoDB session created successfully`);

    console.log(`[TaskUpdate][${task._id}] Step 3: Starting transaction...`);
  }
  session.startTransaction();
  if (process.env.NODE_ENV === 'development') {
    console.log(`[TaskUpdate][${task._id}] Step 4: Transaction started successfully`);
  }

  try {
    const existingTaskIndex = currentTasks.findIndex(t => t._id === task._id);
    const existingTask = getActiveTaskWithIdByIndex(existingTaskIndex)?.task

    if (process.env.NODE_ENV === 'development') {
      console.log(`[TaskUpdate][${task._id}] Step 5: Found existing task at index ${existingTaskIndex}`);
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
        console.log(`[TaskUpdate][${task._id}] ERROR: Task not found in memory at index ${existingTaskIndex}`);
      }
      await session.abortTransaction();
      session.endSession();
      return { success: false, error: "Task not found" };
    }

    // 1. UPDATE DATABASE FIRST with optimistic locking
    if (process.env.NODE_ENV === 'development') {
      console.log(`[TaskUpdate][${task._id}] Step 6: Updating database with transaction session...`);
      console.log(`[TaskUpdate][${task._id}] MongoDB connection state:`, {
      readyState: mongoose.connection.readyState,
      host: mongoose.connection.host,
      name: mongoose.connection.name
    });
    }

    const dbTask = await Task.findOneAndUpdate(
      { _id: task._id },
      { $set: task },
      {
        new: true,
        session,
        runValidators: true
      }
    );

    if (process.env.NODE_ENV === 'development') {
      console.log(`[TaskUpdate][${task._id}] Step 7: Database update completed successfully`);
    }

    if (!dbTask) {
      if (process.env.NODE_ENV === 'development') {
        console.log(`[TaskUpdate][${task._id}] ERROR: Database returned null after update`);
      }
      await session.abortTransaction();
      session.endSession();
      logSystemError('TaskUpdate', `Task not found in database: ${task._id}`, undefined, { taskId: task._id });
      return { success: false, error: "Task not found in database" };
    }

    // 2. Commit transaction to ensure database persistence
    if (process.env.NODE_ENV === 'development') {
      console.log(`[TaskUpdate][${task._id}] Step 8: Committing transaction...`);
    }
    await session.commitTransaction();
    if (process.env.NODE_ENV === 'development') {
      console.log(`[TaskUpdate][${task._id}] Step 9: Transaction committed successfully`);
    }

    session.endSession();
    if (process.env.NODE_ENV === 'development') {
      console.log(`[TaskUpdate][${task._id}] Step 10: Session ended successfully`);

      // 3. NOW update in-memory state after database confirms
      console.log(`[TaskUpdate][${task._id}] Step 11: Starting in-memory state synchronization...`);
    }

    // Check if loopInterval has changed
    const loopIntervalChanged = existingTask.loopInterval.value !== task.loopInterval.value ||
      existingTask.loopInterval.unit !== task.loopInterval.unit;

    const wallet = await walletCache.getWalletById(task.walletId)
    currentTasks.splice(existingTaskIndex, 1, dbTask.toObject());
    activeTasks.set(task._id, dbTask.toObject());
    updateTaskIndexes(currentTasks);

    if (process.env.NODE_ENV === 'development') {
      console.log(`[TaskUpdate][${task._id}] Step 12: In-memory state updated successfully`);
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
      logSystemError('TraitUpdate', `currentTraits[${task._id}] is undefined when trying to update marketplace traits`, undefined, { taskId: task._id });
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

    const { currencySymbol } = await calculateBidPrice(currentTask, 0, "magiceden")

    traitsToRemove[task._id] = removedTraits.map((trait) => `${trait.type}:${trait.value}`)
    await cancelRemovedTraitBids(traitsToRemove[task._id], task._id, task.walletId, currencySymbol)
    await cancelRemovedTokenBids(removedTokens[task._id], task._id, task.walletId, currencySymbol)


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
          task: newTask
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
            walletId: task.walletId,
            slug: task.contract.slug,
            creatorFees,
            enforceCreatorFee: collectionDetails.enforceCreatorFee,
            asset: { contractAddress: task.contract.contractAddress, tokenId: token },
            outbidOptions: task.outbidOptions,
            task: newTask
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
        logSystemError('TokenIdUpdate', `currentTokenIds[${task._id}] is undefined when trying to update marketplace token IDs`, undefined, { taskId: task._id });
      }

      await Promise.all([
        processBulkJobs(magicedenTokenJob, true),
        processBulkJobs(openseaTokenJob, true)
      ]);
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
  } catch (error) {
    const errorObj = error as any;
    console.error(`[TaskUpdate][${task._id}] CRITICAL ERROR in transaction:`, {
      errorName: errorObj.name,
      errorMessage: errorObj.message,
      errorCode: errorObj.code,
      errorCodeName: errorObj.codeName,
      isMongoError: errorObj.name?.includes('Mongo'),
      stack: errorObj.stack?.split('\n').slice(0, 5).join('\n')
    });

    if (process.env.NODE_ENV === 'development') {
      const connectionStateMap: Record<number, string> = {
        0: 'disconnected',
        1: 'connected',
        2: 'connecting',
        3: 'disconnecting'
      };
      console.log(`[TaskUpdate][${task._id}] MongoDB state at error:`, {
        connectionState: mongoose.connection.readyState,
        connectionStateDescription: connectionStateMap[mongoose.connection.readyState] || 'unknown',
        host: mongoose.connection.host,
        inTransaction: session.inTransaction()
      });
    }

    // Rollback transaction if it's still active
    if (session.inTransaction()) {
      if (process.env.NODE_ENV === 'development') {
        console.log(`[TaskUpdate][${task._id}] Rolling back transaction...`);
      }
      await session.abortTransaction();
      if (process.env.NODE_ENV === 'development') {
        console.log(`[TaskUpdate][${task._id}] Transaction rolled back successfully`);
      }
    }
    session.endSession();

    logSystemError('TaskUpdate', `Error processing updated task: ${task.contract?.slug || 'unknown'}`, error, {
      contractSlug: task.contract?.slug,
      taskId: task._id
    });

    return { success: false, error: (error as Error).message };
  }
}

const DRAIN_THRESHOLD = RATE_LIMIT * 9; // Queue drain threshold

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

const MAX_QUEUE_JOBS = RATE_LIMIT * 10; // Maximum jobs allowed in queue
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
  let totalRejectedCount = 0;
  const rejectionReasonsSummary: Record<string, number> = {};

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
      logSystemError('BulkJobs', 'Parent job sync failed', error);
      throw error;
    }
  }

  // Progress tracking for schedule job
  let lastProgressUpdate = Date.now();
  const progressUpdateInterval = 10000; // Update every 10 seconds

  // Create a simple rate limiter for queueing jobs at RATE_LIMIT per second
  const rateLimiter = new RateLimiter(RATE_LIMIT * 2);

  try {
    // Check Redis health before attempting bulk operations
    const isRedisHealthy = await checkRedisHealth();
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

    // Set marketplace status to QUEUEING at the start of bulk job processing
    if (marketplace && !isCancellationJob) {
      const mpKey = marketplace.toLowerCase() as 'opensea' | 'magiceden' | 'blur';
      const currentMpStatus = taskStatus[taskId]?.marketplaces[mpKey]?.status;
      if (currentMpStatus && currentMpStatus !== "RUNNING") {
        setTaskStatus(taskId, "QUEUEING", "jobs being queued", marketplace);
      }
    }

    // Set marketplace status to CANCELLING when cancellation jobs are being queued
    if (marketplace && isCancellationJob) {
      const mpKey = marketplace.toLowerCase() as 'opensea' | 'magiceden' | 'blur';
      const currentMpStatus = taskStatus[taskId]?.marketplaces[mpKey]?.status;
      if (currentMpStatus && currentMpStatus !== "CANCELLING") {
        setTaskStatus(taskId, "CANCELLING", "cancellation jobs being queued", marketplace);
      }
    }

    // Check current queue state but don't wait for initial drain
    const counts = await queue.getJobCounts();
    const blurCounts = await blurQueue.getJobCounts();
    const prioritizedCount = (counts.prioritized || 0) + (blurCounts.prioritized || 0);

    // Log current state but proceed immediately

    const CHUNK_SIZE = RATE_LIMIT * 2
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

          // Set up interval to extend the lock
          scheduledBidLockInterval = setInterval(() => {
            try {
              const extended = extendScheduledBidLock(taskId, marketplace);
              if (!extended) {
              }
            } catch (error) {
              logSystemError('BulkJobs', 'Error extending scheduled bid lock', error);
            }
          }, 60000); // Check every minute
        } catch (lockError) {
          logSystemError('BulkJobs', 'Failed to acquire scheduled bid lock', lockError);
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


            // `Waiting ${WAIT_INTERVAL / 1000} seconds before retry (iteration ${waitIterations}, elapsed: ${Math.round(elapsedTime / 1000)}s)...` + RESET);

            // Wait for 1 minute
            await new Promise(resolve => setTimeout(resolve, WAIT_INTERVAL));

            // Re-check queue counts
            const newCounts = await queue.getJobCounts();
            const newBlurCounts = await blurQueue.getJobCounts();

            totalJobs = (newCounts.waiting || 0) + (newCounts.active || 0) + (newCounts.prioritized || 0) +
              (newBlurCounts.waiting || 0) + (newBlurCounts.active || 0) + (newBlurCounts.prioritized || 0);
            prioritizedJobs = (newCounts.prioritized || 0) + (newBlurCounts.prioritized || 0);

            // Log progress
          }

          if (waitIterations > 0) {
          }

          // If we reach here, we have capacity to process the full chunk

          // Process the full chunk since we verified we have capacity
          const jobsToProcess = currentChunk;

          const rejectedJobs: { job: any; reason: string }[] = [];
          const validJobs = jobsToProcess.filter(job => {
            if (!job?.name || !job?.data) {
              rejectedJobs.push({ job, reason: 'Missing name or data' });
              return false;
            }

            // For cancellation jobs, taskId is in data.taskId, for others it's in data._id
            const jobTaskId = job.data._id || job.data.taskId;
            if (!jobTaskId) {
              rejectedJobs.push({ job, reason: 'Missing taskId' });
              return false;
            }

            const jobTask = activeTasks.get(jobTaskId);
            const isCancelJob = job.name.toLowerCase().includes('cancel');

            // Allow cancellation jobs even if task is not running
            if (isCancelJob) {
              if (!jobTask) {
                rejectedJobs.push({ job, reason: 'Task not found in activeTasks' });
                return false;
              }
              return true; // Just check if task exists
            }

            if (!jobTask) {
              rejectedJobs.push({ job, reason: 'Task not found in activeTasks' });
              return false;
            }
            if (!jobTask.running) {
              rejectedJobs.push({ job, reason: 'Task not running' });
              return false;
            }
            return true;
          });

          if (!validJobs.length) {

            // Log rejection reasons summary
            if (rejectedJobs.length > 0) {
              totalRejectedCount += rejectedJobs.length;

              // Update global rejection summary
              rejectedJobs.forEach(({ reason }) => {
                rejectionReasonsSummary[reason] = (rejectionReasonsSummary[reason] || 0) + 1;
              });

              const rejectionSummary = rejectedJobs.reduce((acc, { reason }) => {
                acc[reason] = (acc[reason] || 0) + 1;
                return acc;
              }, {} as Record<string, number>);


              // Log first few rejected jobs for debugging
              const samplesToLog = Math.min(3, rejectedJobs.length);
              for (let i = 0; i < samplesToLog; i++) {
                const { job, reason } = rejectedJobs[i];
              }
            }
            // No valid jobs, but we successfully processed the chunk
          } else {
            // Track rejected jobs even when we have some valid ones
            if (rejectedJobs.length > 0) {
              totalRejectedCount += rejectedJobs.length;

              // Update global rejection summary
              rejectedJobs.forEach(({ reason }) => {
                rejectionReasonsSummary[reason] = (rejectionReasonsSummary[reason] || 0) + 1;
              });

            }

            // Separate Blur and non-Blur jobs
            const blurJobs = validJobs.filter(job => BLUR_JOBS.includes(job.name));
            const nonBlurJobs = validJobs.filter(job => !BLUR_JOBS.includes(job.name));

            // Add non-Blur jobs to queue
            if (nonBlurJobs.length > 0) {
              const jobsToAdd = nonBlurJobs.map(job => {
                const isCancelJob = job.name.toLowerCase().includes('cancel');
                const jobKey = isCancelJob ? undefined :
                  (createKey ? createJobKey(job) : generateJobKey(job));

                const priority = isCancelJob ? 0 : (loopCounterStats[taskId]?.[marketplace] || 0) + 1

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
              }

              for (const jobChunk of rateLimitChunks) {
                await rateLimiter.acquire();

                const addedJobs = await queue.addBulk(jobChunk);

                // Log successful addition of cancellation jobs
                const addedCancellationCount = addedJobs.filter(job => job.name.toLowerCase().includes('cancel')).length;
                if (addedCancellationCount > 0) {
                }

                // Log current prioritized count to track target range
                if (Math.random() < 0.1) { // Log 10% of the time to avoid spam
                  const qCounts = await queue.getJobCounts();
                  const bCounts = await blurQueue.getJobCounts();
                  const currentPri = (qCounts.prioritized || 0) + (bCounts.prioritized || 0);
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
                  logSystemError('BulkJobs', `Error updating parent job for ${marketplace}`, error, { marketplace });
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
                const priority = isCancelJob ? 0 : (loopCounterStats[taskId]?.[marketplace] || 0) + 1
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

                const addedJobs = await blurQueue.addBulk(jobChunk);
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
                logSystemError('BulkJobs', 'Error updating Blur parent job', error);
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
              retryChunkIndex = chunkIndex;
              chunkIndex--; // Decrement to retry the same chunk
            } else {
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
            // Don't set allJobsQueued=true - this will allow the task to be restarted
          } else {
            parentJob.allJobsQueued = true;

            // Log if actualJobs exceeds expectedJobs (likely due to retries)
            if (parentJob.expectedJobs !== undefined && parentJob.actualJobs > parentJob.expectedJobs) {
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
            // Don't mark as allJobsQueued - this will trigger a restart
          } else {
            parentJob.allJobsQueued = true;
          }
        }
      }

      // Verify all jobs were queued successfully
      if (actuallyQueuedCount < jobs.length) {
        const errorMsg = `Failed to queue all jobs: ${actuallyQueuedCount}/${jobs.length} for task ${taskId} (${totalRejectedCount} rejected)`;
        logSystemError('BulkJobs', errorMsg);

        // Log rejection summary if we have rejected jobs
        if (totalRejectedCount > 0) {
          logSystemError('BulkJobs', 'Total rejection reasons', undefined, { rejectionReasons: rejectionReasonsSummary });

          // Check if task is still in activeTasks and running
          const currentTask = activeTasks.get(taskId);
          if (!currentTask) {
            logSystemError('BulkJobs', `Task ${taskId} is no longer in activeTasks`, undefined, { taskId });
          } else if (!currentTask.running) {
            logSystemError('BulkJobs', `Task ${taskId} is no longer running`, undefined, { taskId, running: currentTask.running });
          }
        }

        // Log the failure details
        await logger.logQueueError('Failed to queue all jobs', {
          taskId: taskId,
          marketplace: getMarketplaceFromJobName(firstJob.name) || 'unknown',
          requested: jobs.length,
          queued: actuallyQueuedCount,
          rejected: totalRejectedCount,
          failed: jobs.length - actuallyQueuedCount,
          rejectionReasons: rejectionReasonsSummary
        });

        // Since we have recursive waiting, this should never happen unless there's a real error
        throw new Error(errorMsg);
      }

      // Log summary with special attention to cancellation jobs
      const totalCancellationJobs = jobs.filter(job => job.name.toLowerCase().includes('cancel')).length;
      if (totalCancellationJobs > 0) {
      } else {
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

      // Reset marketplace status to allow retry - but only if marketplace is still valid
      if (taskStatus[taskId] && marketplace) {
        const activeTask = activeTasks.get(taskId);
        const marketplaceLower = marketplace.toLowerCase();
        const isMarketplaceEnabled = enabledMarketplaces.map(m => m.toLowerCase()).includes(marketplaceLower);
        const isMarketplaceSelected = activeTask?.selectedMarketplaces.map(m => m.toLowerCase()).includes(marketplaceLower);
        const isTaskRunning = activeTask?.running === true;

        if (isMarketplaceEnabled && isMarketplaceSelected && isTaskRunning) {
          setTaskStatus(taskId, 'PROCESSING', 'job queueing failed - will retry', marketplace);
        }
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
        }
      }
    }
  } catch (error) {
    // Outer catch for the main try block
    logSystemError('BulkJobs', 'Fatal error in processBulkJobs', error);
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
    }

    return nonDuplicateJobs;
  } catch (error) {
    // If checking fails, log the error but don't block job addition
    logSystemError('DuplicateJobCheck', 'Error checking for duplicates', error);
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
      }
  }
  const baseKey = `${job?.data?._id}`

  const rawSlug = job?.data?.contract?.slug || job?.data?.slug || ''
  const slug = removeChar(rawSlug, '-')
  const walletAddress = job?.data?.address || ''
  const key = `bid-${baseKey}-${uniqueKey}-${slug}-${walletAddress}`

  // Enhanced debug logging for key generation
  if (process.env.DEBUG_JOB_KEYS === 'true') {
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
      logSystemError('TaskRestart', `Task ${taskId} has reached maximum restarts (${MAX_RESTARTS_PER_HOUR}) in the last hour`, undefined, { taskId, maxRestarts: MAX_RESTARTS_PER_HOUR });
      logSystemError('TaskRestart', `Last restart was ${Math.round((now - restartInfo.lastRestart) / 60000)} minutes ago`);

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


  // Clear any stuck parent jobs for all marketplaces
  const marketplaces = ['opensea', 'magiceden', 'blur'];
  for (const marketplace of marketplaces) {
    const parentJobId = getParentJobId(taskId, marketplace);
    if (parentJobs.has(parentJobId)) {
      parentJobs.delete(parentJobId);
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
    logSystemError('TaskRestart', `Failed to restart task ${taskId}`, error, { taskId });
    // Log the error but don't throw - we want to attempt recovery
    await logger.logGeneralError('Failed to restart stuck task', {
      taskId,
      error: error instanceof Error ? error : new Error(String(error))
    });
  }
}

/**
 * Initialize completion tracking for a task
 */

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
  // Check start parameter immediately to avoid any unnecessary processing
  if (!start) {
    return { success: false, reason: 'start_false' };
  }

  // Check if password is available before starting task
  const password = getPassword();
  if (!password) {
    logSystemError('TaskStart', `Cannot start task ${task.contract?.slug || 'unknown'} - password required`, undefined, {
      taskId: task._id,
      reason: 'no_password'
    });
    return { success: false, reason: 'no_password' };
  }

  const startTime = Date.now();
  const taskId = task._id.toString();



  // If task is in CANCELLING state, we'll clean it up and allow the start
  // Check if any marketplace is in CANCELLING state
  const hasCancellingMarketplace = taskStatus[taskId]?.marketplaces &&
    Object.values(taskStatus[taskId].marketplaces).some(mpStatus => mpStatus?.status === 'CANCELLING');

  if (hasCancellingMarketplace) {

    // Remove task from blacklist to allow new jobs
    const wasRemoved = removeTaskFromBlacklist(taskId);
    if (wasRemoved) {
    }

    // Reset cancellation percentage
    if (taskStatus[taskId]) {
      // Global cancellation properties removed - using marketplace-specific tracking
    }

    // Clear any cancellation tracking
    cancellationTracker.clearTracking(taskId);
  }

  clearTaskTimers(taskId, marketplace)

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

    if (taskStatus[taskId]) {
    }

    // Initialize task status
    if (!taskStatus[taskId]) {
      taskStatus[taskId] = {
        selectedMarketplaces: task.selectedMarketplaces.map(m => m.toLowerCase()),
        lastUpdate: Date.now(),
        marketplaces: {}
      };
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
        statusProgressionManager.ensureStatusProgression(taskId, marketplace, 'STARTED', 'RUNNING', 15000); // 15 seconds timeout
      }
    } else {
      // Set up monitoring only for enabled marketplaces
      for (const mp of task.selectedMarketplaces) {
        const mpLower = mp.toLowerCase();
        const isEnabled = enabledMarketplaces.map(em => em.toLowerCase()).includes(mpLower);
        if (isEnabled) {
          statusProgressionManager.ensureStatusProgression(taskId, mpLower, 'STARTED', 'RUNNING', 15000); // 15 seconds timeout
        }
      }
    }

    // Per-marketplace loop counters (nested structure)
    if (!loopCounterStats[taskId]) {
      loopCounterStats[taskId] = {};
    }

    if (marketplace) {
      // Single marketplace start
      const previousLoopCount = loopCounterStats[taskId][marketplace] || 0;
      let newLoopCount = previousLoopCount;

      if (!isRestart) {
        // Only increment counter for normal task starts, not restarts
        if (!loopCounterStats[taskId][marketplace]) {
          loopCounterStats[taskId][marketplace] = 0;
        }
        loopCounterStats[taskId][marketplace]++;
        newLoopCount = loopCounterStats[taskId][marketplace];

        // Clean up old parent jobs when starting a new loop
        cleanupOldParentJobs(taskId, newLoopCount);
      } else {
        // For restarts, keep the same loop count
        newLoopCount = loopCounterStats[taskId][marketplace] || 1;
      }
    } else {
      // Starting all marketplaces - DO NOT increment counters here
      // Counters should only be incremented when individual marketplaces complete and schedule their next loop
      for (const mp of task.selectedMarketplaces) {
        const mpLower = mp.toLowerCase();
        const currentLoopCount = loopCounterStats[taskId][mpLower] || 0;

        if (!isRestart) {
          // Initialize counter if it doesn't exist, but don't increment
          if (!loopCounterStats[taskId][mpLower]) {
            loopCounterStats[taskId][mpLower] = 0;
          }
        } else {
          const restartLoopCount = loopCounterStats[taskId][mpLower] || 1;
        }
      }

      // Clean up old parent jobs when starting for all marketplaces
      if (!isRestart) {
        const maxLoopCount = Math.max(...task.selectedMarketplaces.map(mp =>
          loopCounterStats[taskId][mp.toLowerCase()] || 1
        ));
        cleanupOldParentJobs(taskId, maxLoopCount);
      }

      // Log complete loop counter state for all marketplaces
    }

    // Clear all processed jobs for this task to ensure fresh tracking
    for (const [parentJobId, processedJobs] of processedJobsPerParent.entries()) {
      if (parentJobId.includes(taskId)) {
        processedJobsPerParent.delete(parentJobId);
      }
    }

    // Clear ALL parent jobs for this task from previous loops
    let clearedParentJobs = 0;
    for (const [parentJobId, parentJob] of parentJobs.entries()) {
      if (parentJobId.includes(taskId)) {
        // Delete all parent jobs for this task, regardless of loop number
        // They will be recreated fresh for the new loop
        parentJobs.delete(parentJobId);
        clearedParentJobs++;
      }
    }
    if (clearedParentJobs > 0) {
    }

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
    return;
  }

  // Async bid log deletion - fire and forget
  BidLogs.deleteMany({ taskId: task._id }).catch(() => { });

  // Ensure wallet approvals for all selected marketplaces
  try {
    const wallet = await walletCache.getWalletById(task.walletId);
    if (!wallet?.encryptedPrivateKey) {
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
      }

      return { marketplace: mp, approved, cached: false };
    });

    // Wait for all approvals to complete
    const approvalResults = await Promise.all(approvalPromises);
    const failedApprovals = approvalResults.filter(r => !r.approved);

    if (failedApprovals.length > 0) {
    }
  } catch (error) {
    logSystemError('MarketplaceApproval', 'Error during marketplace approval check', error);
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
      const taskIndex = currentTasks.findIndex(t => t._id === task._id);
      if (taskIndex !== -1) {
        currentTasks[taskIndex].running = true;
      }

      // Add task to indexes for fast lookup
      addTaskToIndexes(updatedTask);

      // Subscribe to WebSocket collection (WebSocket should already be connected at startup)
      try {
        if (wsManager?.isConnected()) {
          const newTask = { ...task, running: true };
          subscribeToCollections([newTask]);
        }
      } catch (error) {
        logSystemError('CollectionSubscription', `Error subscribing to collection ${task.contract.slug}`, error, { contractSlug: task.contract.slug });
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
          parentJobs.delete(parentId);
        }
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
      // Separate jobs by type
      const blurJobs = jobs.filter(job => job.name === BLUR_SCHEDULE);
      const openseaJobs = jobs.filter(job => job.name === OPENSEA_SCHEDULE);
      const magicedenJobs = jobs.filter(job => job.name === MAGICEDEN_SCHEDULE);

      // Calculate delay for MagicEden based on total marketplace count
      const totalMarketplaceCount = jobs.length;
      let magicedenDelay = 0;
      if (totalMarketplaceCount === 3 && magicedenJobs.length > 0) {
        magicedenDelay = 2000; // 2 seconds if all 3 marketplaces
      } else if (totalMarketplaceCount === 2 && magicedenJobs.length > 0) {
        magicedenDelay = 1000; // 1 second if 2 marketplaces
      }

      // We'll add the delay when scheduling MagicEden jobs
      if (magicedenDelay > 0) {
      }

      // For loop restarts (start=true), always add jobs without deduplication
      if (start === true) {
        // Schedule all jobs immediately (MagicEden with delay if applicable)
        if (openseaJobs.length > 0) {
          await queue.addBulk(openseaJobs);
        }
        if (blurJobs.length > 0) {
          await blurQueue.addBulk(blurJobs);
        }
        if (magicedenJobs.length > 0) {
          if (magicedenDelay > 0) {
            // Schedule MagicEden job with delay
            setTimeout(async () => {
              try {
                await queue.addBulk(magicedenJobs);
              } catch (error) {
                logSystemError('JobScheduling', `Failed to schedule MagicEden jobs for task ${taskId}`, error, { taskId, marketplace: 'magiceden' });
              }
            }, magicedenDelay);
          } else {
            await queue.addBulk(magicedenJobs);
          }
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
          }
        }

        // Filter out duplicate jobs
        const newJobs = jobs.filter(job => !existingJobNames.has(job.name));
        const newBlurJobs = newJobs.filter(job => job.name === BLUR_SCHEDULE);
        const newOpenseaJobs = newJobs.filter(job => job.name === OPENSEA_SCHEDULE);
        const newMagicedenJobs = newJobs.filter(job => job.name === MAGICEDEN_SCHEDULE);

        if (newJobs.length > 0) {
          // Schedule all new jobs (MagicEden with delay if applicable)
          if (newOpenseaJobs.length > 0) {
            await queue.addBulk(newOpenseaJobs);
          }
          if (newBlurJobs.length > 0) {
            await blurQueue.addBulk(newBlurJobs);
          }
          if (newMagicedenJobs.length > 0) {
            if (magicedenDelay > 0) {
              // Schedule MagicEden job with delay
              setTimeout(async () => {
                try {
                  await queue.addBulk(newMagicedenJobs);
                } catch (error) {
                  console.error(RED + `[Scheduling] Failed to schedule new MagicEden jobs for task ${taskId}:` + RESET, error);
                }
              }, magicedenDelay);
            } else {
              await queue.addBulk(newMagicedenJobs);
            }
          }
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


  // Job state verification - wait and re-check for any missed jobs
  if (totalRemoved > 0) {

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
            } catch (error) {
              console.error(RED + `[removeAllTaskJobsFromQueue] Failed to remove missed job ${job.id}:` + RESET, error);
            }
          }
        }
      }
    }

    if (remainingJobs > 0) {
    } else {
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
    }

    // Add other marketplace rate limiter cancellations as needed
    // OpenSea and MagicEden don't currently have specific rate limiters

  } catch (error) {
    console.error(`Error cancelling rate limiter jobs for task ${taskId}:`, error);
  }
}

// Helper function to attempt cancellation of any remaining orders
async function attemptRemainingCancellations(taskId: string, marketplace: string): Promise<number> {

  try {
    // Fetch the remaining active orders
    const remainingOrders = await Order.find({
      taskId: taskId,
      marketplace: marketplace.toLowerCase(),
      expiresAt: { $gt: new Date() }
    }).limit(500); // Limit to prevent overwhelming the system

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
    const ordersArray = remainingOrders.map(o => o.toObject());

    switch (marketplaceLower) {
      case 'opensea':
        await cancelOpenseaBids(ordersArray, task.walletId, taskId, 'WETH');
        break;
      case 'magiceden':
        await cancelMagicedenBids(ordersArray, task.walletId, taskId);
        break;
      case 'blur':
        await cancelBlurBids(ordersArray, task.walletId, taskId);
        break;
      default:
        return remainingOrders.length;
    }


    // Wait a bit for jobs to be processed
    await new Promise(resolve => setTimeout(resolve, 5000));

    // Re-check how many orders remain
    const stillActiveCount = await Order.countDocuments({
      taskId: taskId,
      marketplace: marketplace.toLowerCase(),
      expiresAt: { $gt: new Date() }
    });

    return stillActiveCount;

  } catch (error) {
    console.error(RED + `[attemptRemainingCancellations] Error attempting remaining cancellations:` + RESET, error);
    return -1; // Return -1 to indicate error
  }
}

// Enhanced cancellation completion validation with race condition protection
async function validateAndTransitionToStopped(taskId: string, marketplace: string, marketplaceName: string, retryCount = 0): Promise<void> {
  const MAX_RETRIES = 3;
  const RETRY_DELAY = 500; // 500ms delay between retries

  try {
    // First check: Count active orders for this marketplace
    const query = {
      taskId: taskId,
      marketplace: marketplace.toLowerCase(),
      expiresAt: { $gt: new Date() }
    };

    const activeOrderCount = await Order.countDocuments(query);

    // Get current tracking status to validate completion
    const trackingStatus = cancellationTracker.getStatus(taskId, marketplace);
    const isTrackingComplete = trackingStatus ? cancellationTracker.isComplete(taskId, marketplace) : false;


    if (activeOrderCount > 0) {
      // Still have active orders - check if we should retry or if this is expected
      if (retryCount < MAX_RETRIES) {

        // Wait for potential database updates to complete
        await new Promise(resolve => setTimeout(resolve, RETRY_DELAY));

        // Retry validation
        return await validateAndTransitionToStopped(taskId, marketplace, marketplaceName, retryCount + 1);
      } else {
        // Max retries reached but still have orders - attempt to cancel them

        // Attempt to cancel remaining orders
        const remainingAfterAttempt = await attemptRemainingCancellations(taskId, marketplace);

        if (remainingAfterAttempt === 0) {
          // Successfully cancelled all remaining orders
          setTaskStatus(taskId, 'STOPPED', `${marketplaceName} cancellation complete after cleanup`, marketplace);
          return;
        } else if (remainingAfterAttempt > 0) {
          // Still have orders after cancellation attempt

          // Reset the cancellation percentage to reflect actual state
          if (trackingStatus && trackingStatus.totalBids > 0) {
            const actualCompleted = Math.max(0, trackingStatus.totalBids - remainingAfterAttempt);
            const actualPercentage = (actualCompleted / trackingStatus.totalBids) * 100;

            if (taskStatus[taskId]?.marketplaces[marketplaceName as keyof typeof taskStatus[string]['marketplaces']]) {
              taskStatus[taskId].marketplaces[marketplaceName as keyof typeof taskStatus[string]['marketplaces']]!.cancellationPercentage = Math.floor(actualPercentage);
              broadcastBidRates();
            }
          }
        }
        return;
      }
    }

    // No active orders found - safe to transition to STOPPED
    if (isTrackingComplete || activeOrderCount === 0) {

      // Safe to transition to STOPPED
      setTaskStatus(taskId, 'STOPPED', `${marketplaceName} cancellation complete - validated with enhanced checks`, marketplace);
    } else {
    }

  } catch (error) {
    console.error(RED + `[validateAndTransitionToStopped] Error validating ${marketplaceName} cancellation completion:` + RESET, error);

    // On error, keep CANCELLING status to be safe
  }
}

// Centralized function to update task cancellation percentage - single source of truth
export async function updateTaskCancellationPercentage(taskId: string, forcePercentage?: number, marketplace?: string) {

  // Log current tracker state
  const allTrackings = cancellationTracker.getAllTrackings();
  allTrackings.forEach((tracking, key) => {
  });

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
        return;
      }
    } else {
      // Use regular percentage for marketplace-specific cancellations
      const trackerPercentage = cancellationTracker.getPercentage(taskId, marketplace);
      if (trackerPercentage !== null) {
        percentage = trackerPercentage;
      } else {
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

        taskStatus[taskId].marketplaces[mpKey]!.cancellationPercentage = marketplacePercentage;
      }
    }

    // Log the global update (deprecated)
  }

  // Log marketplace-specific cancellation status
  Object.entries(taskStatus[taskId].marketplaces).forEach(([mp, status]) => {
    if (status) {
    }
  });

  // If we've reached 100%, check for remaining active orders with enhanced validation
  // IMPORTANT: Only transition to STOPPED when marketplace-specific cancellation is truly complete
  if (marketplace) {
    // Marketplace-specific cancellation - check if THIS marketplace reached 100%
    const marketplaceName = marketplace.toLowerCase() as 'opensea' | 'magiceden' | 'blur';
    const isCancellingThisMarketplace = taskStatus[taskId]?.marketplaces[marketplaceName]?.status === 'CANCELLING';
    const marketplacePercentage = taskStatus[taskId]?.marketplaces[marketplaceName]?.cancellationPercentage || 0;

    // CRITICAL BUG FIX: Fixed >= 100 to === 100 to prevent premature transitions
    if (isCancellingThisMarketplace && marketplacePercentage === 100) {

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
        const activeOrderCount = await Order.countDocuments({
          taskId: taskId,
          expiresAt: { $gt: new Date() }
        });

        if (activeOrderCount === 0) {
          // Transition all marketplaces back to STOPPED
          for (const mp of cancellingMarketplaces) {
            setTaskStatus(taskId, 'STOPPED', 'global cancellation complete - 100% and no active orders', mp);
          }
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
        console.error(`[updateTaskCancellationPercentage] Error checking for remaining orders:`, error);
      }
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

async function
  stopTask(task: ITask, start: boolean, marketplace?: string, stoppedByWarning = false) {
  const taskId = task._id.toString();
  try {
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
    // Step 1: Blacklist task to prevent new jobs (immediate)
    if (marketplace) {
      // Add task to marketplace-specific blacklist
      isTaskBlacklisted(taskId, marketplace);
    } else {
      // Add task to complete blacklist
      isTaskBlacklisted(taskId);
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

      // Clean up all task resources - preserve warnings if stopped by warning
      cleanupTaskResources(taskId, stoppedByWarning);

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
      counterbidCounts[taskId] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      };
    }

    if (!marketplace && !stoppedByWarning) {
      // Only clear warningBids if NOT stopped by warning
      warningBids[taskId] = {
        opensea: false,
        magiceden: false,
        blur: false
      };
    }

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

    const fetchResult = await lockManager.withLock(lockKey, async () => {
      const fetchedOrders = await fetchOrdersForCancellation(taskId, marketplace);
      return fetchedOrders;
    });

    if (!fetchResult) {
      return;
    }

    orders = fetchResult;



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

    // Count total active orders from database
    let totalBids = 0;
    const marketplaceTotals = {
      opensea: 0,
      magiceden: 0,
      blur: 0
    };

    if (marketplace) {
      // Count for specific marketplace
      totalBids = await Order.countDocuments({ taskId, marketplace: marketplace.toLowerCase(), expiresAt: { $gt: new Date() } });
    } else {
      // Count all active orders for the task and by marketplace
      totalBids = await Order.countDocuments({ taskId, expiresAt: { $gt: new Date() } });

      // Get marketplace-specific counts for global cancellation
      const [openSeaCount, magicEdenCount, blurCount] = await Promise.all([
        Order.countDocuments({ taskId, marketplace: 'opensea', expiresAt: { $gt: new Date() } }),
        Order.countDocuments({ taskId, marketplace: 'magiceden', expiresAt: { $gt: new Date() } }),
        Order.countDocuments({ taskId, marketplace: 'blur', expiresAt: { $gt: new Date() } })
      ]);

      marketplaceTotals.opensea = openSeaCount;
      marketplaceTotals.magiceden = magicEdenCount;
      marketplaceTotals.blur = blurCount;

    }

    // Check if there are no orders to cancel
    const totalOrderCount = orders.opensea.length + orders.magiceden.length + orders.blur.length;
    if (totalOrderCount === 0) {

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

      // Broadcast the update
      broadcastBidRates();

      // Return early - task stays in STOPPED state
      return;
    }

    // Initialize cancellation tracking
    cancellationTracker.initializeTracking(taskId, totalBids, marketplace);

    // For global cancellations, set marketplace-specific totals
    if (!marketplace) {
      cancellationTracker.setMarketplaceTotals(taskId, marketplaceTotals);
    }

    // Log current cancellation tracking state
    const trackingStatus = cancellationTracker.getStatus(taskId, marketplace);
    if (trackingStatus) {
    } else {
    }

    // Set initial cancellation percentage
    if (taskStatus[taskId]) {
      await updateTaskCancellationPercentage(taskId, 0);
    }


    // Get currency symbol for OpenSea cancellations
    let currencySymbol = 'WETH'; // Default to WETH
    try {
      const { currencySymbol: cs } = await calculateBidPrice(task, 0, "opensea");
      currencySymbol = cs;
    } catch (error) {
    }

    switch (marketplace?.toLowerCase()) {
      case 'opensea': {
        // Cancel active HTTP requests first
        const httpResult = await cancelOpenseaRequest(taskId);

        // Now cancel actual orders
        if (orders.opensea.length > 0 && WALLET_PRIVATE_KEY) {


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
        const httpResult = await cancelMagicedenRequest(taskId);

        // Now cancel actual orders
        if (orders.magiceden.length > 0 && WALLET_PRIVATE_KEY) {


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
        const httpResult = await cancelBlurRequest(taskId);

        // Now cancel actual orders
        if (orders.blur.length > 0 && WALLET_PRIVATE_KEY) {


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
        await setMarketplaceCancellationComplete(taskId, marketplace);
      } else {
        await setTaskCancellationComplete(taskId);
      }
    } else {
    }


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
    console.error(`[StopTask] Error in enhanced stopTask for ${taskId}:`, {
      error: error.message,
      command: error.command,
      timestamp: new Date().toISOString()
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
        console.error(RED + `[stopTask] Error during marketplace cleanup: ${cleanupError}` + RESET);
      }
    }
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

    // Unsubscribe from WebSocket for this task's collection
    if (wsManager && task.contract?.slug && task.contract?.contractAddress) {
      await wsManager.unsubscribe(task.contract.slug, task.contract.contractAddress);
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
            fetchAndCancelOpenseaBids(task)
              .then(() => { bidsCancelled++; })
              .catch(error => {
                errors.push(`OpenSea bid cancellation failed: ${error.message}`);
              })
          );
        }

        if (!marketplace || marketplace.toLowerCase() === 'magiceden') {
          bidCancellationPromises.push(
            fetchAndCancelMagicedenBids(task)
              .then(() => { bidsCancelled++; })
              .catch(error => {
                errors.push(`MagicEden bid cancellation failed: ${error.message}`);
              })
          );
        }

        if (!marketplace || marketplace.toLowerCase() === 'blur') {
          bidCancellationPromises.push(
            fetchAndCancelBlurBids(task)
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

  const tokenBid = task.bidType === "token" || task.bidType === "rarity"
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
 * @returns Promise with operation results
 */
async function fetchAndCancelOpenseaBids(
  task: ITask
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

    // Fetch wallet and decrypt private key
    const wallet = await walletCache.getWalletById(task.walletId);
    if (!wallet?.encryptedPrivateKey) {
      const error = 'Wallet not found or missing private key';
      await logger.logWarning(ErrorCategory.MARKETPLACE, error, { taskId, walletId: task.walletId });
      return { success: false, ordersFound: 0, error };
    }
    const WALLET_PRIVATE_KEY = decryptPrivateKey(wallet.encryptedPrivateKey);

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
 * @returns Promise with operation results
 */
async function fetchAndCancelBlurBids(
  task: ITask
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

    // Fetch wallet and decrypt private key
    const wallet = await walletCache.getWalletById(task.walletId);
    if (!wallet?.encryptedPrivateKey) {
      const error = 'Wallet not found or missing private key';
      await logger.logWarning(ErrorCategory.MARKETPLACE, error, { taskId, walletId: task.walletId });
      return { success: false, ordersFound: 0, error };
    }
    const WALLET_PRIVATE_KEY = decryptPrivateKey(wallet.encryptedPrivateKey);

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
          await cancelBlurBids(batch, task.walletId, taskId);

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
 * @returns Promise with operation results
 */
async function fetchAndCancelMagicedenBids(
  task: ITask
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

    // Fetch wallet and decrypt private key
    const wallet = await walletCache.getWalletById(task.walletId);
    if (!wallet?.encryptedPrivateKey) {
      const error = 'Wallet not found or missing private key';
      await logger.logWarning(ErrorCategory.MARKETPLACE, error, { taskId, walletId: task.walletId });
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
          await cancelMagicedenBids(batch, task.walletId, taskId);

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
    const orderId = order._id?.toString() || order.orderHash;
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
    const orderKeys = batchOrders.map((order: any) => order._id)
    const batchId = `batch-${i / 500}-${Date.now()}`; // Unique batch identifier


    const jobOptions = getCancellationJobOptions(taskId, batchId, CANCEL_MAGICEDEN_BID);

    await queue.add(CANCEL_MAGICEDEN_BID, {
      orderIds: extractedOrderIds,
      walletId,
      orderKeys,
      taskId,
      batchSize: batchOrders.length // Add batch size to job data
    }, jobOptions);
  }

  // Decrement the own bid counter by the number of cancelled orders
  decrementOwnBidCounter('magiceden', ordersToCancel.length);

}

async function cancelBlurBids(orders: any[], walletId: string, taskId: string) {
  if (!orders.length) return


  // Parent job tracking removed - now using cancellationTracker

  // Check cancellation tracker status
  const trackingStatus = cancellationTracker.getStatus(taskId, 'blur');


  const cancelData = orders.map((order, index) => {
    const payload = JSON.parse(order.orderHash)
    const jobOptions = getCancellationJobOptions(taskId, order._id, CANCEL_BLUR_BID);
    return {
      name: CANCEL_BLUR_BID,
      data: { walletId, payload, taskId, orderKey: order._id },
      opts: jobOptions
    }
  }) as Job[]


  await processBulkJobs(cancelData);

}

async function cancelOpenseaBids(orders: any[], walletId: string, taskId: string, currencySymbol: string) {
  if (!orders.length) return;


  // Parent job tracking removed - now using cancellationTracker

  const cancelData = orders.map((order, index) => {
    return {
      name: CANCEL_OPENSEA_BID,
      data: { walletId, orderId: order.orderHash, taskId, orderKey: order._id },
      opts: getCancellationJobOptions(taskId, order._id, CANCEL_OPENSEA_BID)
    }
  }) as Job[]

  // Track the job IDs we're about to create for reconciliation
  const expectedJobIds = cancelData.map(job => job.opts.jobId);

  // Expected job count tracking removed - using marketplace-specific counts

  await processBulkJobs(cancelData)


  // Log current tracker state
  const trackingStatus = cancellationTracker.getStatus(taskId, 'opensea');
  if (trackingStatus) {
  }

  // Decrement the own bid counter by the number of cancelled orders
  decrementOwnBidCounter('opensea', orders.length);
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
      await Order.deleteOne({ _id: order._id });
      completed += 1;
    }
  } catch (error) {
    throw error;
  }

  if (completed > 0) {
    cancellationTracker.recordCompletion(taskId, completed, 'opensea');
    await updateTaskCancellationPercentage(taskId, undefined, 'opensea');
    decrementOwnBidCounter('opensea', completed);
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
          console.error(RED + `Error adding marketplace ${incoming} to task ${task._id}: ` + RESET, error);
        }
      } else {
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
        if (aborted.length > 0) {
          const reasons = aborted.map(r => r.result?.reason || 'unknown').filter((v, i, a) => a.indexOf(v) === i);
        }
      } else {
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

      // Add defensive check to ensure results are valid
      if (!Array.isArray(results)) {
        console.error(RED + '[updateMultipleTasksStatus] Invalid results from stopPromises' + RESET);
        return;
      }
      if (errors.length > 0) {
      } else {
      }

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

        // Ensure bidStats is cleaned up
        if (bidStats[taskId]) {
          delete bidStats[taskId];
        }

        // Ensure errorStats is cleaned up
        if (errorStats[taskId]) {
          delete errorStats[taskId];
        }
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


        await logger.logInfo(ErrorCategory.WEBSOCKET, 'WebSocket disconnected', {
          connectionId: connectionMetrics.currentConnectionId || undefined,
          connectionDuration: connectionMetrics.connectionDuration,
          disconnectCode: event.code,
          disconnectReason: event.reason || getCloseReason(event.code),
          totalDisconnections: connectionMetrics.totalDisconnections
        });
      } else {

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
        console.error('[WebSocket][Reception] Error in message handler:', error);
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
  const processingStartTime = Date.now();
  const eventType = message.eventType || message.event || 'unknown';

  try {
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

    const walletAddressKey = wallet.address.toLowerCase();


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

    if (!contractAddress && !slug) {
      return;
    }

    // Use fast O(1) lookup instead of O(n) filter
    const relevantTasks = getRelevantTasks(contractAddress, slug);

    if (!relevantTasks.length) {
      return;
    }

    relevantTasks.map(task => handleCounterBidForTask(task, message))

  } catch (error) {
    console.error("Error in handleCounterBid:", error);
  }

}

function getMarketplaceDetails(message: any): { contractAddress?: string, slug?: string } {

  let result: { contractAddress?: string, slug?: string } = {};

  switch (message.marketplace) {
    case BLUR:
      result = { contractAddress: handleBlurMessages(message) };
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

async function handleCounterBidForTask(task: any, message: any) {
  const selectedMarketplaces = task.selectedMarketplaces.map((m: string) => m.toLowerCase());
  try {
    if (selectedMarketplaces.includes('blur') && message.marketplace === BLUR) {
      // Validate Blur message structure before accessing nested properties
      if (!message['1'] || typeof message['1'] !== 'object') {
        await logger.logWarning(
          ErrorCategory.WEBSOCKET,
          'BLUR_INCOMPLETE - Invalid Blur WebSocket message structure',
          {
            taskId: task._id?.toString(),
            marketplace: message.marketplace,
            hasNestedData: !!message['1'],
            messageKeys: Object.keys(message || {}),
            component: 'blur_counterbid_validation'
          }
        );
        return; // Skip processing invalid Blur messages
      }
      await handleBlurCounterbid(message['1'], task)
    }
    else if (selectedMarketplaces.includes('opensea') && message.marketplace === OPENSEA) {
      handleOpenseaCounterbid(message, task)
    }
    else if (selectedMarketplaces.includes('magiceden') && message.marketplace === MAGICEDEN) {
      handleMagicEdenCounterbid(message, task)
    } else {
    }
  } catch (error: any) {
  }
}


async function handleMagicEdenCounterbid(data: any, task: ITask) {
  // Structured log: start
  try {

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

    const expiration = getExpiry(currentTask.bidDuration)
    const orderHash = data?.data?.id

    // Include task ID in the hash to make it task-specific
    const taskSpecificHash = `${orderHash}-${task._id}`;
    if (hasSeenItem(seenOrderHashes, taskSpecificHash)) {
      return;
    }
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


    if (walletAddresses.includes(maker)) {
      incrementOwnBidCounter('magiceden', orderHash);
      return
    }

    // Comprehensive logging for MagicEden counterbid
    const makerFormatted = maker ? `${maker.slice(0, 6)}...${maker.slice(-4)}` : 'unknown';
    const bidTypeDisplay = bidType === 'token' ? `Token #${tokenId}` : 'Collection';

    if (!currentTask.outbidOptions.counterbid) {
      return;
    }


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
      if (incomingBidAmountEth < minBidPriceEth) {
        // Return silently when incoming bid is less than min bid price
        return
      }
      if (incomingBidAmountEth > maxBidPriceEth) {
        const message = `incoming bid ${incomingBidAmountEth} ${currencySymbol} for ${task.contract.slug}: ${tokenId} is greater than max bid price ${maxBidPriceEth} ${currencySymbol}. Skipping ...`.toUpperCase()
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
          queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, walletId: task.walletId, orderKeys: orderKeys, taskId: task._id });
        }
        return
      }


      const offers = orders.map((order: any) => order.offerPrice)
      const currentBidPrice = !offers.length ? 0 : Math.max(...offers.map((offer) => Number(offer)))

      if (incomingBidAmount < currentBidPrice) return

      const roundedEth = roundToValidPrecision(incomingBidAmountEth)

      offerPrice = Math.ceil((magicedenOutbidMargin * Math.pow(10, decimal)) + roundedEth * Math.pow(10, decimal))
      const offerPriceEth = offerPrice / Math.pow(10, decimal)


      if (maxBidPriceEth > 0 && offerPriceEth >= maxBidPriceEth) {
        const message = `magiceden counter offer ${offerPriceEth} ${currencySymbol} for ${task.contract.slug} ${tokenId} exceeds max bid price ${maxBidPriceEth} ${currencySymbol} ON MAGICEDEN.Skipping ...`.toUpperCase()
        await logBidError(task._id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "magiceden");


        if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
          if (orders.length > 0) {
            const orderKeys = orders.map((order: any) => order._id)
            const extractedOrderIds = await extractMagicedenOrderHash(orders)
            queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, walletId: task.walletId, orderKeys: orderKeys, taskId: task._id });
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
        walletId: task.walletId,
        slug: task.contract.slug,
        outbidOptions: task.outbidOptions,
        maxBidPriceEth,
        minBidPriceEth,
      }


      await queue.add(
        MAGICEDEN_TOKEN_BID_COUNTERBID,
        bidData
      );

      seenOrderHashes.delete(taskSpecificHash);

      if (orders.length > 0) {
        const extractedOrderIds = await extractMagicedenOrderHash(orders)
        const orderKeys = orders.map((order: any) => order._id)
        queue.add(CANCEL_MAGICEDEN_BID, {
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

      const hasTraits = checkMagicEdenTrait(selectedTraits, trait)
      if (!hasTraits || !trait.attributeKey || !trait.attributeValue) {
        return
      }


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
      if (incomingBidAmountEth < minBidPriceEth) {
        // Return silently when incoming bid is less than min bid price
        return
      }
      if (incomingBidAmountEth > maxBidPriceEth) {
        const message = `incoming bid ${incomingBidAmountEth} ${currencySymbol} for ${task.contract.slug}: ${identifier} is greater than max bid price ${maxBidPriceEth} ${currencySymbol}. Skipping ...`.toUpperCase()
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
          queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, walletId: task.walletId, orderKeys: orderKeys, taskId: task._id });
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

      if (maxBidPriceEth > 0 && offerPriceEth >= maxBidPriceEth) {
        const message = `magiceden counter offer ${offerPriceEth} ${currencySymbol} for ${task.contract.slug} ${JSON.stringify(trait)} exceeds max bid price ${maxBidPriceEth} ${currencySymbol} ON MAGICEDEN.Skipping ...`.toUpperCase()
        await logBidError(task._id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "magiceden");

        if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
          if (orders.length > 0) {
            const orderKeys = orders.map((order: any) => order._id)
            const extractedOrderIds = await extractMagicedenOrderHash(orders)
            queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, walletId: task.walletId, orderKeys: orderKeys, taskId: task._id });
          }
        }
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
          const tokenOrders = await Order.find({
            slug: task.contract.slug,
            taskId: task._id,
            marketplace: MAGICEDEN.toLowerCase(),
            identifier: tokenId
          });

          if (tokenOrders.length > 0) {
            // We have an existing offer on this token - queue instant counterbid
            // Calculate offer price with crossTypeMarginMultiplier
            const marginMultiplier = currentTask.outbidOptions.crossTypeMarginMultiplier || 1;
            const multipliedMargin = magicedenOutbidMargin * marginMultiplier;
            const tokenOfferPrice = Math.ceil((multipliedMargin * Math.pow(10, decimal)) + incomingBidAmount);

            // IMPORTANT: Check if the calculated offer price exceeds max bid price
            const tokenOfferPriceEth = tokenOfferPrice / Math.pow(10, decimal);
            if (maxBidPriceEth > 0 && tokenOfferPriceEth > maxBidPriceEth) {
              continue; // Skip this token as it exceeds max bid price
            }

            const tokenBidData: IMagicedenTokenBidData = {
              _id: task._id,
              address: task.wallet,
              contractAddress: task.contract.contractAddress,
              quantity: 1,
              offerPrice: tokenOfferPrice,
              expiration: expiration,
              walletId: task.walletId,
              slug: task.contract.slug,
              tokenId: parseInt(tokenId),
              outbidOptions: currentTask.outbidOptions,
              maxBidPriceEth,
              minBidPriceEth
            };

            await queue.add(
              MAGICEDEN_TOKEN_BID_COUNTERBID,
              tokenBidData
            );
            tokenCounterbidsQueued++;
          }
        }

        if (tokenCounterbidsQueued > 0) {
        }
      } catch (error) {
        console.error(`[MagicEden] Error fetching tokens with trait ${trait.attributeKey}:${trait.attributeValue}:`, error);
      }

      // Handle collection bids with traits
      if (task.bidType === "collection") {
        const bidData: IMagicedenTraitBidData = {
          taskId: task._id,
          bidCount: getIncrementedBidCount(MAGICEDEN, task.contract.slug, task._id),
          address: task.wallet,
          contractAddress: task.contract.contractAddress,
          quantity: 1,
          offerPrice: offerPrice,
          expiration: expiration,
          walletId: task.walletId,
          slug: task.contract.slug,
          trait: JSON.stringify(trait),
        }


        await queue.add(
          MAGICEDEN_TRAIT_BID_COUNTERBID,
          bidData
        );

      }

      seenOrderHashes.delete(taskSpecificHash);

      if (orders.length > 0) {
        const extractedOrderIds = await extractMagicedenOrderHash(orders)
        const orderKeys = orders.map((order: any) => order._id)
        queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, walletId: task.walletId, orderKeys: orderKeys, taskId: task._id });
      }
    }
    else if (bidType === "collection") {
      // Allow all task types to respond to collection offers
      // Process each existing offer individually

      const incomingBidAmountEth = incomingBidAmount / Math.pow(10, decimal);

      // Check cache first for floor price
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

      // Check if incoming bid is within valid range
      if (incomingBidAmountEth < minBidPriceEth) {
        // Return silently when incoming bid is less than min bid price
        return
      }
      if (incomingBidAmountEth > maxBidPriceEth) {
        const message = `incoming bid ${incomingBidAmountEth} ${currencySymbol} for ${task.contract.slug}: collection is greater than max bid price ${maxBidPriceEth} ${currencySymbol}. Skipping ...`.toUpperCase()
        await logBidError(task._id, "INVALID BID PRICE", message, "skipped", "magiceden");

        // Cancel all orders if configured and price exceeds max
        if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
          const allOrders = await Order.find({
            taskId: task._id,
            marketplace: MAGICEDEN.toLowerCase()
          })
          if (allOrders.length > 0) {
            const orderKeys = allOrders.map((order: any) => order._id)
            const extractedOrderIds = await extractMagicedenOrderHash(allOrders)
            queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, walletId: task.walletId, orderKeys: orderKeys, taskId: task._id });
          }
        }
        return
      }

      const stopBid = await stopOption(currentTask, 'magiceden', Number(floor_price), true)
      if (stopBid) {
        const allOrders = await Order.find({
          taskId: task._id,
          marketplace: MAGICEDEN.toLowerCase()
        })
        if (allOrders.length > 0) {
          const orderKeys = allOrders.map((order: any) => order._id)
          const extractedOrderIds = await extractMagicedenOrderHash(allOrders)
          queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, walletId: task.walletId, orderKeys: orderKeys, taskId: task._id });
        }
        return
      }

      // Fetch ALL orders for this task
      const allTaskOrders = await Order.find({
        slug: currentTask.contract.slug,
        marketplace: MAGICEDEN.toLowerCase()
      });

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
          // Filter orders to only include those belonging to the current task
          const taskOrders = allTaskOrders.filter((order: any) => order.taskId === currentTask._id);
          if (taskOrders.length > 0) {
            const orderKeys = taskOrders.map((order: any) => order._id)
            const extractedOrderIds = await extractMagicedenOrderHash(taskOrders)
            queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, walletId: task.walletId, orderKeys: orderKeys, taskId: task._id });
          }
        }
        return;
      }

      // Process each existing order
      let counterbidsQueued = 0;
      const ordersToCancel = [];

      for (const order of allTaskOrders) {
        // Only counterbid orders that are <= incoming collection offer
        if (Number(order.offerPrice) <= incomingBidAmount) {
          const bidCount = getIncrementedBidCount(MAGICEDEN, currentTask.contract.slug, currentTask._id)

          // Calculate offer price based on order type
          let orderOfferPrice: number;
          if (order.identifier === 'collection') {
            // Collection vs collection: use normal margin
            orderOfferPrice = baseOfferPrice;
          } else {
            // Collection vs trait/token: use configurable margin multiplier
            const marginMultiplier = currentTask.outbidOptions.crossTypeMarginMultiplier || 1;
            const multipliedMargin = magicedenOutbidMargin * marginMultiplier;
            orderOfferPrice = Math.ceil((multipliedMargin * Math.pow(10, decimal)) + (roundedEth * Math.pow(10, decimal)));
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
              offerPrice: orderOfferPrice,
              expiration: expiration,
              walletId: task.walletId,
              slug: task.contract.slug,
              trait: JSON.stringify(trait)
            }

            await queue.add(
              MAGICEDEN_TRAIT_BID_COUNTERBID,
              counterbidData
            );

            counterbidsQueued++;
            ordersToCancel.push(order);

          } else if (order.identifier === 'collection') {
            // This is a collection offer
            const bidData: IMagicedenCollectionBidData = {
              _id: task._id,
              bidCount,
              address: task.wallet,
              contractAddress: task.contract.contractAddress,
              quantity: 1,
              offerPrice: orderOfferPrice,
              expiration: expiration,
              walletId: task.walletId,
              slug: task.contract.slug,
              currency
            }

            await queue.add(
              MAGICEDEN_COLLECTION_BID_COUNTERBID,
              bidData
            );

            counterbidsQueued++;
            ordersToCancel.push(order);

          } else {
            // This is a token offer
            const tokenId = order.identifier;

            const bidData: IMagicedenTokenBidData = {
              _id: task._id,
              address: task.wallet,
              contractAddress: task.contract.contractAddress,
              quantity: 1,
              offerPrice: orderOfferPrice,
              expiration: expiration,
              walletId: task.walletId,
              slug: task.contract.slug,
              tokenId: parseInt(tokenId),
              outbidOptions: currentTask.outbidOptions,
              maxBidPriceEth,
              minBidPriceEth
            }


            await queue.add(
              MAGICEDEN_TOKEN_BID_COUNTERBID,
              bidData
            );

            counterbidsQueued++;
            ordersToCancel.push(order);
          }
        } else {
        }
      }

      if (counterbidsQueued > 0) {

        // Cancel the orders that we're counterbidding
        if (ordersToCancel.length > 0) {
          const orderKeys = ordersToCancel.map((order: any) => order._id)
          const extractedOrderIds = await extractMagicedenOrderHash(ordersToCancel)
          queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, walletId: task.walletId, orderKeys: orderKeys, taskId: task._id });
        }
      } else {
      }

      seenOrderHashes.delete(taskSpecificHash);
    }
  } catch (error) {
  }
}

async function handleOpenseaCounterbid(data: any, task: ITask) {
  try {

    const currentTask = activeTasks.get(task._id)
    if (!currentTask) {
      return;
    }

    if (!currentTask?.running) {
      return;
    }

    const taskMarketplaces = currentTask.selectedMarketplaces.map((market) => market.toLowerCase());
    if (!taskMarketplaces.includes("opensea")) {
      return;
    }

    if (!enabledMarketplaces.map((m) => m.toLowerCase()).includes("opensea")) {
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

    if (!Number.isFinite(incomingBidAmount)) {
      return;
    }

    const normalizedIncomingBidAmount = Math.ceil(incomingBidAmount);
    if (normalizedIncomingBidAmount !== incomingBidAmount) {
    }
    incomingBidAmount = normalizedIncomingBidAmount;

    const formattedBidAmout = base_price / Math.pow(10, paymentTokenDecimal)
    const bidDetails = `${formattedBidAmout} ${symbol.toUpperCase()}`
    const extraDetails = `${incomingBidAmount / Math.pow(10, decimal)} ${currencySymbol.toUpperCase()}`

    const tokenId = data.event === "item_received_bid" ? data.payload.payload.protocol_data.parameters.consideration.find((item: any) => item.token.toLowerCase() === task.contract.contractAddress.toLowerCase()).identifierOrCriteria : null

    const trait = data.event === "trait_offer" ? {
      type: data.payload.payload.trait_criteria.trait_type,
      value: data.payload.payload.trait_criteria.trait_name
    } : null

    const orderHash = data.payload.payload.order_hash

    const detectedBidType = tokenId ? 'token' : (trait ? 'trait' : 'collection');

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

    // Fetch all wallets first to check if it's our own bid
    const wallets = await walletCache.getAllWallets()
    const walletAddresses = wallets.map((wallet) => wallet.address.toLowerCase())

    // Include task ID in the hash to make it task-specific
    const taskSpecificHash = `${orderHash}-${task._id}`
    if (hasSeenItem(seenOrderHashes, taskSpecificHash)) {
      return;
    }
    addSeenItem(seenOrderHashes, taskSpecificHash)

    if (walletAddresses.includes(maker)) {
      incrementOwnBidCounter('opensea', orderHash);
      return
    }

    // Comprehensive logging for OpenSea counterbid
    const bidType = tokenId ? `Token #${tokenId}` : trait ? `Trait: ${trait.type}:${trait.value}` : 'Collection';


    if (!outbidOptions.counterbid) {
      return;
    }


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

    // Convert all creator fees to the IFee format (ensuring values are numbers)
    const creatorFees: IFee = Object.fromEntries(
      Object.entries(collectionDetails.creator_fees).map(([key, value]) => [key, Number(value)])
    );

    let offerPrice: number;
    let colletionOffer: bigint;

    const selectedTraits = transformNewTask(currentTask?.selectedTraits?.opensea)
    const bestOffer = (bestOffers[task._id]?.opensea || 0);

    const collectionOffer = data.event === "collection_offer" ? incomingBidAmount / Math.pow(10, decimal) : 0

    if (collectionOffer > bestOffer) {
      // Defensive check to ensure bestOffers[task._id] exists
      if (!bestOffers[task._id]) {
        bestOffers[task._id] = {
          opensea: 0,
          magiceden: 0,
          blur: 0
        };
      }
      bestOffers[task._id].opensea = collectionOffer;
    }

    if (data.event === "item_received_bid") {
      let tokenOffer: bigint;
      let offerPrice: number;

      const orders = await Order.find({
        slug: task.contract.slug,
        taskId: task._id,
        marketplace: OPENSEA.toLowerCase(),
        identifier: tokenId.toString()
      })

      if (!orders) {
        return;
      }

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
      if (!floor_price) {
        return
      }

      // VALIDATION: Ensure floor price is reasonable (not negative, not zero, not suspiciously low)
      if (floor_price <= 0 || floor_price < 0.001) {
        if (process.env.NODE_ENV === 'development') {
          console.warn(`[Counterbid] Suspicious floor price detected: ${floor_price} ETH for ${task.contract.slug} - Skipping counterbid`);
        }
        await logBidError(task._id, "INVALID FLOOR PRICE", `Floor price ${floor_price} ETH is invalid or suspiciously low for ${task.contract.slug}:${tokenId}. Skipping counterbid.`, "skipped", "opensea");
        return;
      }

      const { maxBidPriceEth, minBidPriceEth, currency } = await calculateBidPrice(currentTask, floor_price as number, "opensea")

      // VALIDATION: Log floor price and calculated limits for debugging
      if (process.env.NODE_ENV === 'development') {
        console.log(`[Counterbid] OpenSea Token Bid - Slug: ${task.contract.slug}, Token: ${tokenId}, Floor: ${floor_price} ETH, Min: ${minBidPriceEth} ETH, Max: ${maxBidPriceEth} ETH`);
      }

      if (incomingBidAmountEth < minBidPriceEth) {
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
                data: { walletId: task.walletId, orderId: order.orderHash, taskId: task._id, orderKey: order._id },

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
              data: { walletId: task.walletId, orderId: order.orderHash, taskId: task._id, orderKey: order._id },
            }
          })
          await queue.addBulk(cancelData)
        }
        return
      }

      const offers = orders.map((order: any) => order.offerPrice)
      const currentBidPrice = offers.length > 0 ? Math.max(...offers.map((offer) => Number(offer))) : 0

      if (incomingBidAmount < currentBidPrice) {
        return;
      }

      const openseaOutbidMargin = calculateOutbidMargin(incomingBidAmountEth, currency)
      const roundedEth = roundToValidPrecision(incomingBidAmountEth)

      offerPrice = Math.ceil((openseaOutbidMargin * Math.pow(10, decimal)) + (roundedEth * Math.pow(10, decimal)))
      const offerPriceEth = offerPrice / Math.pow(10, decimal)

      // VALIDATION: Log outbid calculation details
      if (process.env.NODE_ENV === 'development') {
        console.log(`[Counterbid] Outbid Calculation - Incoming: ${incomingBidAmountEth} ETH, Margin: ${openseaOutbidMargin} ETH, Rounded: ${roundedEth} ETH, Counter: ${offerPriceEth} ETH`);
      }

      // VALIDATION: Ensure calculated bid is not suspiciously different from incoming bid
      const bidDifferenceRatio = Math.abs(offerPriceEth - incomingBidAmountEth) / incomingBidAmountEth;
      if (bidDifferenceRatio > 0.5) {
        // If counter bid is >50% different from incoming bid, something is wrong
        const message = `Counter bid ${offerPriceEth} ETH differs significantly from incoming bid ${incomingBidAmountEth} ETH (${(bidDifferenceRatio * 100).toFixed(1)}% difference) for ${task.contract.slug}:${tokenId}. Possible calculation error.`.toUpperCase();
        await logBidError(task._id, "SUSPICIOUS BID CALCULATION", message, "skipped", "opensea");
        if (process.env.NODE_ENV === 'development') {
          console.error(`[Counterbid] ${message}`);
        }
        return;
      }



      if (offerPriceEth >= maxBidPriceEth) {
        const message = `counter offer ${offerPriceEth} ${currencySymbol} for ${task.contract.slug}:${tokenId}  exceeds max bid price ${maxBidPriceEth} ${currencySymbol} ON OPENSEA.Skipping ...`.toUpperCase()
        await logBidError(task._id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "opensea");
        if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
          if (orders.length > 0) {
            const cancelData = orders.map((order, index) => {
              return {
                name: CANCEL_OPENSEA_BID,
                data: { walletId: task.walletId, orderId: order.orderHash, taskId: task._id, orderKey: order._id },

              }
            })
            // Log cancellation enqueue
            queue.addBulk(cancelData)
          }
        }
        return;
      }
      const normalizedOfferPrice = Math.ceil(offerPrice)
      if (normalizedOfferPrice !== offerPrice) {
      }
      offerPrice = normalizedOfferPrice
      tokenOffer = BigInt(offerPrice)

      const asset = {
        contractAddress: task.contract.contractAddress,
        tokenId: tokenId
      }

      const tokenOfferPriceForQueue = Math.ceil(Number(tokenOffer))
      const bidData: IProcessOpenseaTokenBidData = {
        _id: task._id,
        address: task.wallet,
        walletId: task.walletId,
        slug: task.contract.slug,
        offerPrice: tokenOfferPriceForQueue,
        creatorFees,
        enforceCreatorFee: collectionDetails.enforceCreatorFee,
        asset,
        outbidOptions: outbidOptions
      }

      await queue.add(
        OPENSEA_TOKEN_BID_COUNTERBID,
        bidData
      );

      // Log successful counterbid preparation

      seenOrderHashes.delete(taskSpecificHash);
    }

    if (data.event === "trait_offer") {
      if (!trait) {
        return
      }

      let traitOffer = BigInt(incomingBidAmount)
      const hasTraits = checkOpenseaTrait(selectedTraits, data.payload.payload.trait_criteria)
      const identifier = `${trait.type}:${trait.value}`

      if (!hasTraits) {
        return
      }

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
      if (!floor_price) {
        return
      }

      // VALIDATION: Ensure floor price is reasonable (not negative, not zero, not suspiciously low)
      if (floor_price <= 0 || floor_price < 0.001) {
        if (process.env.NODE_ENV === 'development') {
          console.warn(`[Counterbid] Suspicious floor price detected: ${floor_price} ETH for ${task.contract.slug} trait ${identifier} - Skipping counterbid`);
        }
        await logBidError(task._id, "INVALID FLOOR PRICE", `Floor price ${floor_price} ETH is invalid or suspiciously low for ${task.contract.slug}:${identifier}. Skipping counterbid.`, "skipped", "opensea");
        return;
      }

      const { maxBidPriceEth, minBidPriceEth, currency } = await calculateBidPrice(currentTask, floor_price as number, "opensea")

      // VALIDATION: Log floor price and calculated limits for debugging
      if (process.env.NODE_ENV === 'development') {
        console.log(`[Counterbid] OpenSea Trait Bid - Slug: ${task.contract.slug}, Trait: ${identifier}, Floor: ${floor_price} ETH, Min: ${minBidPriceEth} ETH, Max: ${maxBidPriceEth} ETH`);
      }
      const incomingBidAmountEth = incomingBidAmount / Math.pow(10, decimal)

      const orders = await Order.find({
        slug: task.contract.slug,
        taskId: task._id,
        marketplace: OPENSEA.toLowerCase(),
        identifier
      })

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
                data: { walletId: task.walletId, orderId: order.orderHash, taskId: task._id, orderKey: order._id },
              }
            })
            // Log cancellation enqueue
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
              data: { walletId: task.walletId, orderId: order.orderHash, taskId: task._id, orderKey: order._id },
            }
          })
          // Log cancellation enqueue
          queue.addBulk(cancelData)
        }
        return
      }

      const offers = orders.map((order: any) => order.offerPrice)
      const currentBidPrice = offers.length > 0 ? Math.max(...offers.map((offer) => Number(offer))) : 0

      if (incomingBidAmount < currentBidPrice) {
        return
      }

      const openseaOutbidMargin = calculateOutbidMargin(incomingBidAmountEth, currency)
      const roundedEth = roundToValidPrecision(incomingBidAmountEth)
      offerPrice = Math.ceil((openseaOutbidMargin * Math.pow(10, decimal)) + (roundedEth * Math.pow(10, decimal)))
      const offerPriceEth = offerPrice / Math.pow(10, decimal)

      // VALIDATION: Log outbid calculation details
      if (process.env.NODE_ENV === 'development') {
        console.log(`[Counterbid] Trait Outbid Calculation - Incoming: ${incomingBidAmountEth} ETH, Margin: ${openseaOutbidMargin} ETH, Rounded: ${roundedEth} ETH, Counter: ${offerPriceEth} ETH`);
      }

      // VALIDATION: Ensure calculated bid is not suspiciously different from incoming bid
      const bidDifferenceRatio = Math.abs(offerPriceEth - incomingBidAmountEth) / incomingBidAmountEth;
      if (bidDifferenceRatio > 0.5) {
        // If counter bid is >50% different from incoming bid, something is wrong
        const message = `Counter bid ${offerPriceEth} ETH differs significantly from incoming bid ${incomingBidAmountEth} ETH (${(bidDifferenceRatio * 100).toFixed(1)}% difference) for ${task.contract.slug}:${identifier}. Possible calculation error.`.toUpperCase();
        await logBidError(task._id, "SUSPICIOUS BID CALCULATION", message, "skipped", "opensea");
        if (process.env.NODE_ENV === 'development') {
          console.error(`[Counterbid] ${message}`);
        }
        return;
      }

      if (maxBidPriceEth > 0 && offerPriceEth >= maxBidPriceEth) {

        if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
          if (orders.length > 0) {
            const cancelData = orders.map((order, index) => {
              return {
                name: CANCEL_OPENSEA_BID,
                data: { walletId: task.walletId, orderId: order.orderHash, taskId: task._id, orderKey: order._id },
              }
            })
            // Log cancellation enqueue
            queue.addBulk(cancelData)
          }
        }
        return;
      }

      traitOffer = BigInt(offerPrice)

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
          const tokenOrders = await Order.find({
            slug: task.contract.slug,
            taskId: task._id,
            marketplace: OPENSEA.toLowerCase(),
            identifier: tokenId
          });

          if (tokenOrders.length > 0) {
            // We have an existing offer on this token - queue instant counterbid
            const asset = {
              contractAddress: task.contract.contractAddress,
              tokenId: parseInt(tokenId)
            };

            // Calculate offer price with crossTypeMarginMultiplier
            const marginMultiplier = currentTask.outbidOptions.crossTypeMarginMultiplier || 1;
            const multipliedMargin = openseaOutbidMargin * marginMultiplier;
            const tokenOfferPrice = Math.ceil((multipliedMargin * Math.pow(10, decimal)) + (roundedEth * Math.pow(10, decimal)));

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
              offerPrice: tokenOfferPriceForQueue,
              creatorFees,
              enforceCreatorFee: collectionDetails.enforceCreatorFee,
              asset,
              outbidOptions: outbidOptions
            };

            await queue.add(
              OPENSEA_TOKEN_BID_COUNTERBID,
              tokenBidData
            );
            tokenCounterbidsQueued++;
          }
        }

        if (tokenCounterbidsQueued > 0) {
        }
      } catch (error) {
        console.error(`[OpenSea] Error fetching tokens with trait ${trait.type}:${trait.value}:`, error);
      }

      // Handle collection bids with traits
      if (task.bidType.toLowerCase() === "collection") {
        const bidCount = getIncrementedBidCount(OPENSEA, task.contract.slug, task._id)

        const traitOfferForQueue = Math.ceil(Number(traitOffer));
        const counterbidData: IProcessOpenseaTraitBidData = {
          _id: task._id,
          bidCount,
          address: task.wallet,
          walletId: task.walletId,
          slug: task.contract.slug,
          offerPrice: traitOfferForQueue,
          creatorFees,
          enforceCreatorFee: collectionDetails.enforceCreatorFee,
          expiry,
          trait: JSON.stringify(trait)
        }

        await queue.add(
          OPENSEA_TRAIT_BID_COUNTERBID,
          counterbidData
        );

        // Log successful trait counterbid preparation
      }

      seenOrderHashes.delete(taskSpecificHash);
    }

    if (data.event === "collection_offer") {
      const incomingBidAmountEth = incomingBidAmount / Math.pow(10, decimal);
      let floor_price = await collectionCache.getFloorPrice(task.contract.slug, 'opensea');

      if (floor_price === null) {
        floor_price = await collectionCache.fetchWithDedup(
          `opensea-floor-${task.contract.slug}`,
          async () => {
            const price = await getOpenseaFloor(task.contract.slug);
            await collectionCache.setFloorPrice(task.contract.slug, 'opensea', price);
            return price;
          }
        );
      }
      if (!floor_price) {
        return;
      }
      const { maxBidPriceEth, minBidPriceEth, currency } = await calculateBidPrice(currentTask, floor_price as number, "opensea")
      if (incomingBidAmountEth < minBidPriceEth) {
        return
      } else if (incomingBidAmountEth > maxBidPriceEth) {
        const reason = `is greater than max bid price ${maxBidPriceEth} ${currencySymbol}`;
        const message = `incoming bid  ${bidDetails} ${similarCurrency ? '' : `(${extraDetails})`} for ${task.contract.slug}: collection ${reason}. Skipping ...`.toUpperCase()
        await logBidError(task._id, "INVALID BID PRICE", message, "skipped", "opensea");

        // Cancel all orders if configured
        if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
          const allOrders = await Order.find({
            taskId: task._id,
            marketplace: OPENSEA.toLowerCase()
          })
          if (allOrders.length > 0) {
            const cancelData = allOrders.map((order) => {
              return {
                name: CANCEL_OPENSEA_BID,
                data: { walletId: task.walletId, orderId: order.orderHash, taskId: task._id, orderKey: order._id },
              }
            })
            await queue.addBulk(cancelData)
          }
        }
        return
      }

      const stopBid = await stopOption(currentTask, 'opensea', Number(floor_price), true)
      if (stopBid) {
        const allOrders = await Order.find({
          taskId: task._id,
          marketplace: OPENSEA.toLowerCase()
        })
        if (allOrders.length > 0) {
          const cancelData = allOrders.map((order) => {
            return {
              name: CANCEL_OPENSEA_BID,
              data: { walletId: task.walletId, orderId: order.orderHash, taskId: task._id, orderKey: order._id },
            }
          })
          await queue.addBulk(cancelData)
        }
        return
      }

      const allTaskOrders = await Order.find({
        slug: currentTask.contract.slug,
        marketplace: OPENSEA.toLowerCase()
      });

      const openseaOutbidMargin = calculateOutbidMargin(incomingBidAmountEth, currency)
      const roundedEth = roundToValidPrecision(incomingBidAmountEth)
      const baseOfferPrice = Math.ceil((openseaOutbidMargin * Math.pow(10, decimal)) + (roundedEth * Math.pow(10, decimal)))
      const baseOfferPriceEth = baseOfferPrice / Math.pow(10, decimal)

      if (maxBidPriceEth > 0 && baseOfferPriceEth > maxBidPriceEth) {
        const message = `counter Offer price ${baseOfferPriceEth} ${currencySymbol} for ${task.contract.slug} exceeds max bid price ${maxBidPriceEth} ${currencySymbol} ON OPENSEA.Skipping ...`.toUpperCase()
        await logBidError(task._id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "opensea");

        if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
          const taskOrders = allTaskOrders.filter((order: any) => order.taskId === currentTask._id);
          if (taskOrders.length > 0) {
            const cancelData = taskOrders.map((order) => {
              return {
                name: CANCEL_OPENSEA_BID,
                data: { walletId: task.walletId, orderId: order.orderHash, taskId: task._id, orderKey: order._id },
              }
            })
            await queue.addBulk(cancelData)
          }
        }
        return;
      }

      // Process each existing order
      let counterbidsQueued = 0;
      for (const order of allTaskOrders) {
        if (Number(order.offerPrice) <= incomingBidAmount) {
          const bidCount = getIncrementedBidCount(OPENSEA, task.contract.slug, task._id)
          let orderOfferPrice: number;
          if (order.identifier === 'collection') {
            orderOfferPrice = baseOfferPrice;
          } else {
            const marginMultiplier = currentTask.outbidOptions.crossTypeMarginMultiplier || 1;
            const multipliedMargin = openseaOutbidMargin * marginMultiplier;
            orderOfferPrice = Math.ceil((multipliedMargin * Math.pow(10, decimal)) + (roundedEth * Math.pow(10, decimal)));
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
              offerPrice: orderOfferPriceForQueue,
              creatorFees,
              enforceCreatorFee: collectionDetails.enforceCreatorFee,
              expiry,
              trait: JSON.stringify(trait)
            }


            await queue.add(
              OPENSEA_TRAIT_BID_COUNTERBID,
              counterbidData
            );
            counterbidsQueued++;
          } else if (order.identifier === 'collection') {
            colletionOffer = BigInt(orderOfferPrice)
            const collectionOfferForQueue = Math.ceil(Number(colletionOffer));
            const bidData: IOpenseaBidParams = {
              taskId: task._id,
              bidCount,
              walletAddress: task.wallet,
              walletId: task.walletId,
              slug: task.contract.slug,
              offerPrice: collectionOfferForQueue,
              creatorFees,
              enforceCreatorFee: collectionDetails.enforceCreatorFee,
              expiry
            }


            await queue.add(
              OPENSEA_COLLECTION_BID_COUNTERBID,
              bidData
            );
            counterbidsQueued++;
          } else {
            // This is a token offer
            const tokenId = order.identifier;
            const orderOfferPriceForQueue = Math.ceil(Number(orderOfferPrice));
            const bidData: IProcessOpenseaTokenBidData = {
              _id: task._id,
              address: task.wallet,
              walletId: task.walletId,
              slug: task.contract.slug,
              offerPrice: orderOfferPriceForQueue,
              creatorFees,
              enforceCreatorFee: collectionDetails.enforceCreatorFee,
              asset: {
                contractAddress: task.contract.contractAddress,
                tokenId: parseInt(tokenId)
              },
              outbidOptions: outbidOptions
            }


            await queue.add(
              OPENSEA_TOKEN_BID_COUNTERBID,
              bidData
            );
            counterbidsQueued++;
          }
        }
      }

      if (counterbidsQueued > 0) {
      }
      seenOrderHashes.delete(taskSpecificHash);
    }
    seenOrderHashes.delete(taskSpecificHash);
  } catch (error) {
    logError('opensea', task._id.toString(), 'handleOpenseaCounterbid', 'Error in OpenSea counterbid handler', error);
  }
}

async function
  handleBlurCounterbid(data: any, task: ITask) {
  const startTime = Date.now();
  const incomingBid = data

  // Log function start

  try {

    const bidHash = hashBid({ ...data, taskId: task._id });
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
    const expiry = getExpiry(currentTask?.bidDuration)
    // Use cached wallet for better performance
    const wallets = await walletCache.getWallet(task.wallet);
    if (!wallets) {
      await logger.logError(ErrorCategory.DATABASE, `Wallet not found for task ${task._id}: ${task.wallet}`, new Error('Wallet not found'));
      return;
    }
    const selectedTraits = transformNewTask(currentTask?.selectedTraits?.blur)
    const traitBid = currentTask?.bidType === "collection" && selectedTraits && Object.keys(selectedTraits)?.length > 0
    const blurOutbidMargin = currentTask?.outbidOptions?.blurOutbidMargin || 0.01

    // Comprehensive logging for Blur counterbid
    const bidInfo = incomingBid?.stats?.[0] || incomingBid?.updates?.[0] || incomingBid;
    const price = bidInfo?.bestPrice || bidInfo?.price || 0;
    const criteriaType = bidInfo?.criteriaType || 'unknown';
    const criteriaValue = bidInfo?.criteriaValue || {};
    const makerAddress = incomingBid?.maker || bidInfo?.maker || 'unknown';
    const makerFormatted = makerAddress !== 'unknown' ? `${makerAddress.slice(0, 6)}...${makerAddress.slice(-4)}` : 'unknown';
    const bidTypeDisplay = criteriaType === 'trait' ? `Trait: ${JSON.stringify(criteriaValue)}` : criteriaType === 'token' ? `Token #${criteriaValue?.identifier || 'unknown'}` : 'Collection';

    if (!currentTask?.outbidOptions?.counterbid) {
      return;
    }


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

        if (!activeTrait) {
          return;
        }

        const totalOffer = incomingPrice + blurOutbidMargin

        const orders = await Order.find({
          slug: task.contract.slug,
          taskId: task._id,
          marketplace: BLUR.toLowerCase(),
          identifier: identifier
        })

        if (+totalOffer < minBidPriceEth) {
          // Return silently when incoming bid is less than min bid price
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
                  data: { walletId: task.walletId, payload, taskId: task._id, orderKey: order._id }
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
            const cancelData = orders.map((order, index) => {
              const payload = JSON.parse(order.orderHash)
              return {
                name: CANCEL_BLUR_BID,
                data: { walletId: task.walletId, payload, taskId: task._id, orderKey: order._id }
              }
            })
            await queue.addBulk(cancelData)
          }
          return
        }

        const offers = orders?.map((order: any) => order?.offerPrice)
        const currentBidPrice = !offers?.length ? 0 : Math.max(...offers?.map((offer) => Number(offer)))
        const currentBidPriceEth = currentBidPrice / 1e18
        if (incomingPrice <= currentBidPriceEth) {
          return;
        }

        let offerPrice: any = Number((blurOutbidMargin + Number(incomingPrice))?.toFixed(2))
        offerPrice = parseEther(offerPrice?.toString())

        const bidCount = getIncrementedBidCount(BLUR, task.contract.slug, task._id)

        const bidData: IBlurBidData = {
          taskId: task._id,
          bidCount: bidCount,
          address: task.wallet,
          walletId: task.walletId,
          contractAddress: task.contract.contractAddress,
          offerPrice: Number(offerPrice),
          slug: task.contract.slug,
          traits,
          expiry
        }




        await blurQueue.add(
          BLUR_TRAIT_BID_COUNTERBID,
          bidData
        );
      }

    } else {
      const incomingBidDetails = incomingBid?.stats?.filter((item: any) => item?.criteriaType?.toLowerCase() === "collection") || incomingBid?.updates?.filter((item: any) => item?.criteriaType?.toLowerCase() === "collection")

      if (!incomingBidDetails || !incomingBidDetails?.length) {
        return
      }

      const incomingBestPrice = incomingBidDetails?.sort((a: any, b: any) => +b?.bestPrice - +a?.bestPrice)[0].bestPrice
      const incomingBidAmount = (incomingBidDetails as any)?.sort((a: any, b: any) => +b.price - +a.price)[0].price

      const bestPrice = +incomingBestPrice > 0 ? incomingBestPrice : +incomingBidAmount > 0 ? incomingBidAmount : "0"

      // Check cache first for floor price
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

      // Check if incoming bid is within valid range
      if (+bestPrice < minBidPriceEth) {
        // Return silently when incoming bid is less than min bid price
        return
      } else if (+bestPrice > maxBidPriceEth) {
        const reason = `is greater than max bid price ${maxBidPriceEth} BETH`;
        const message = `incoming bid ${bestPrice} BETH for ${task.contract.slug}: collection ${reason}. Skipping ...`.toUpperCase()
        await logBidError(task._id, "INVALID BID PRICE", message, "skipped", "blur");


        // Cancel all orders if configured
        if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
          const allOrders = await Order.find({
            taskId: task._id,
            marketplace: BLUR.toLowerCase()
          })
          const cancelData = allOrders.map((order) => {
            const payload = JSON.parse(order.orderHash)
            return {
              name: CANCEL_BLUR_BID,
              data: {
                walletId: task.walletId,
                payload,
                taskId: task._id,
                orderKey: order._id
              }
            }
          })
          await queue.addBulk(cancelData);
        }
        return
      }

      const stopBid = await stopOption(currentTask, 'blur', Number(floor_price), true)
      if (stopBid) {

        const allOrders = await Order.find({
          taskId: task._id,
          marketplace: BLUR.toLowerCase()
        })
        if (allOrders.length > 0) {
          const cancelData = allOrders.map((order) => {
            const payload = JSON.parse(order.orderHash)
            return {
              name: CANCEL_BLUR_BID,
              data: {
                walletId: task.walletId,
                payload,
                taskId: task._id,
                orderKey: order._id
              }
            }
          })
          await queue.addBulk(cancelData);
        }
        return
      }

      // Fetch ALL orders for this task
      const allTaskOrders = await Order.find({
        slug: currentTask.contract.slug,
        marketplace: BLUR.toLowerCase()
      });

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
          const taskOrders = allTaskOrders.filter((order: any) => order.taskId === currentTask._id);
          if (taskOrders.length > 0) {
            const cancelData = taskOrders.map((order) => {
              const payload = JSON.parse(order.orderHash)
              return {
                name: CANCEL_BLUR_BID,
                data: {
                  walletId: task.walletId,
                  payload,
                  taskId: task._id,
                  orderKey: order._id
                }
              }
            })
            await queue.addBulk(cancelData);
          }
        }
        return;
      }

      // Process each existing order
      let counterbidsQueued = 0;

      for (const order of allTaskOrders) {
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
              offerPrice: Number(orderOfferPrice),
              slug: task.contract.slug,
              traits,
              expiry
            }

            await blurQueue.add(
              BLUR_TRAIT_BID_COUNTERBID,
              counterbidData
            );

            counterbidsQueued++;

          } else if (order.identifier === 'collection') {
            // This is a collection offer
            const bidData: IBlurBidData = {
              taskId: task._id,
              bidCount,
              address: task.wallet,
              walletId: task.walletId,
              contractAddress: task.contract.contractAddress,
              offerPrice: Number(orderOfferPrice),
              slug: task.contract.slug,
              expiry
            }



            await blurQueue.add(
              BLUR_COLLECTION_BID_COUNTERBID,
              bidData
            );
            counterbidsQueued++;
          }
        }
      }

      if (counterbidsQueued > 0) {
      }
    }

    // Log completion

  } catch (error: any) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    const errorStack = error instanceof Error ? error.stack : undefined;


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
    } else {
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

  if (task.bidType === "token" || task.bidType === "rarity") {
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
          fetchMagicEdenTokens(taskId, task.contract.slug, amount)
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
                } else if (items.length >= 2000) {
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
            } else if (items.length >= 2000) {
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
          // Add defensive check before property access
          if (skipStats[taskId]) {
            skipStats[taskId].opensea = (skipStats[taskId].opensea || 0) + missingTraitTokenCount;
            skipStats[taskId].magiceden = (skipStats[taskId].magiceden || 0) + missingTraitTokenCount;
          } else {
            console.error(RED + `[ERROR] skipStats[${taskId}] is undefined when trying to update opensea/magiceden counts` + RESET);
          }

        }

        // Add trait tokens to both OpenSea and MagicEden lists
        openseaTokens = [...openseaTokens, ...traitTokens];
        magicedenTokens = [...magicedenTokens, ...traitTokens.map(id => Number(id))];

        if (traitTokens.length > 0) {
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

    if (task.bidType === "token" || task.bidType === "rarity") {
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
          // Add defensive check before property access
          if (skipStats[taskId]) {
            skipStats[taskId].opensea = (skipStats[taskId].opensea || 0) + duplicateCount;
          } else {
            console.error(RED + `[ERROR] skipStats[${taskId}] is undefined when trying to update opensea duplicate count` + RESET);
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
              console.error(RED + `[ERROR] skipStats[${taskId}] is undefined when trying to update opensea additional missing count` + RESET);
            }
          }
        }
      } else {
        // Only throw error if API was called and returned empty (scenario 6)
        if (openseaApiCalled && openseaApiReturnedEmpty && !manualTokenIds.length && !traitTokens.length) {
          throw new Error(`No listings found on OpenSea for ${task.contract.slug}. Collection may have no active listings.`);
        }
        // Otherwise, just skip without error
        preprocessing.opensea.skipReason = `No tokens found for ${task.contract.slug}`;
        preprocessing.opensea.jobCount = 0;
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
    if (task.bidType === "token" || task.bidType === "rarity") {
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

    if (task.bidType === "token" || task.bidType === "rarity") {
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
        // Otherwise, just skip without error
        preprocessing.magiceden.skipReason = `No tokens found for ${task.contract.slug}`;
        preprocessing.magiceden.jobCount = 0;
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

async function processOpenseaScheduledBid(task: ITask, scheduleJob?: Job) {
  const taskId = task._id.toString();
  const currentLoopCount = loopCounterStats[taskId]?.opensea || 1;

  let lockAcquired = false;

  // Timeout protection wrapper - 120 seconds max processing time
  const TIMEOUT_DURATION = 120000; // 2 minutes
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
          return;
        }
        const currentTask = activeTasks.get(task._id);
        if (!currentTask || !currentTask.running || !currentTask.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("opensea")) {
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

          // Perform preprocessing for OpenSea
          preprocessing = await performMarketplacePreprocessing(currentTask, "opensea", availableMarketplaces);

          // Store in Map for other marketplaces to use
          taskPreprocessing.set(taskId, preprocessing);

          // If preprocessing didn't enable OpenSea, it means something went wrong
          if (!preprocessing.opensea.enabled) {
            logError('opensea', taskId, 'processOpenseaScheduledBid', 'Preprocessing failed to enable OpenSea');
            throw new Error(`Preprocessing failed to enable OpenSea for task ${taskId}`);
          }
        }

        if (preprocessing.opensea.skipReason) {
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

        // Use timed lock manager for better timeout handling with BullMQ lock validation
        try {
          // Validate BullMQ lock before acquiring our custom lock
          if (scheduleJob && scheduleJob.id) {
            const lockKey = `bull:BIDDING_BOT:${scheduleJob.id}:lock`;
            const redis = redisClient.getClient();
            if (redis) {
              const lockExists = await redis.get(lockKey);
              if (!lockExists) {
                // Continue processing but add monitoring
                await logger.logError(ErrorCategory.QUEUE, 'Missing BullMQ lock detected in OpenSea scheduled bid', {
                  taskId,
                  jobId: scheduleJob.id,
                  lockKey
                });
              }
            }
          }

          lockAcquired = timedLockManager.acquireLock(taskId, 'opensea');
          if (!lockAcquired) {
            return;
          }
        } catch (lockError) {
          console.error(`${RED}[LOCK ERROR] Failed to acquire lock for task ${taskId}: ${lockError}${RESET}`);

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
              console.error(`${RED}[LOCK RECOVERY] Failed to recover lock for task ${taskId}: ${recoveryError}${RESET}`);
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
          const { fetchERC20BalanceFresh } = await import('./utils/balance');
          const { WETH } = await import('./utils/currencies');
          wethBalance = await fetchERC20BalanceFresh(WALLET_ADDRESS, WETH);
        } catch (error) {
          console.error(`${RED}[OpenSea] Failed to fetch WETH balance: ${error}${RESET}`);
        }

        // Get collection details for creator fees
        let currency, collectionDetails;
        try {
          const results = await Promise.all([
            calculateBidPrice(currentTask, 0, "opensea"),
            getCollectionDetails(currentTask.contract.slug, currentTask.contract.contractAddress)
          ]);
          currency = results[0].currency;
          collectionDetails = results[1];

          // Log full collection details object
        } catch (error) {
          console.error(`${RED}[OpenSea] Failed to get collection details: ${error}${RESET}`);
          throw error;
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


        // Note: Status will be set to appropriate state after job creation is determined
        const newTask = { ...currentTask, traits: {} } as ITask


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

            // Fire-and-forget: processBulkJobs runs asynchronously to prevent timeout
            processBulkJobs(jobs, true, scheduleJob);

            // Register for job creation verification
            registerJobCreationVerification(taskId, 'opensea', preprocessing.opensea.tokens.length);


            // The task will transition to RUNNING when jobs start processing

          } catch (jobError) {
            console.error(RED + `[OPENSEA] Failed to queue token bid jobs: ${jobError}` + RESET);
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

            // Fire-and-forget: processBulkJobs runs asynchronously to prevent timeout
            processBulkJobs(traitJobs, true, scheduleJob);

            // Register for job creation verification
            registerJobCreationVerification(taskId, 'opensea', preprocessing.opensea.traits.length);


            // The task will transition to RUNNING when jobs start processing

          } catch (jobError) {
            console.error(RED + `[OPENSEA] Failed to queue trait bid jobs: ${jobError}` + RESET);
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

          await queue.add(
            OPENSEA_COLLECTION_BID,
            { taskId: taskId, collectionDetails, currency },
            { jobId, attempts: 1 }
          );


          // Transition to QUEUED after job is queued (not RUNNING)
          setTaskStatus(taskId, 'QUEUED', 'opensea collection bid job queued', 'opensea');

          // Trigger percentage update for single job
          await updateTaskRunningPercentage(taskId);
        } else {
          // No jobs to create - create empty parent job and complete the loop

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
    console.error(RED + `Error processing OpenSea scheduled bid for task: ${task._id}` + RESET, error);

    // Enhanced error handling for timeout and lock issues
    const isTimeout = error instanceof Error && error.message.includes('timeout');
    const isLockError = error instanceof Error && (
      error.message.includes('Missing lock') ||
      error.message.includes('Lock acquisition failed') ||
      error.message.includes('LOCK ERROR')
    );

    if (isTimeout) {
      console.error(RED + `[OPENSEA_TIMEOUT] Task ${taskId} timed out after ${TIMEOUT_DURATION}ms${RESET}`);

      // Log timeout with additional context
      await logger.logError(ErrorCategory.QUEUE, 'OpenSea scheduled bid timeout', {
        taskId,
        timeoutDuration: TIMEOUT_DURATION,
        lockAcquired,
        currentLoopCount,
        error: error instanceof Error ? error : new Error(String(error))
      });

      // Clean up resources on timeout
      if (lockAcquired) {
        try {
          timedLockManager.releaseLock(taskId, 'opensea');
        } catch (cleanupError) {
          logSystemError('TimeoutCleanup', `Failed to release lock for task ${taskId}`, cleanupError, { taskId });
        }
      }
    }

    if (isLockError) {
      console.error(RED + `[OPENSEA_LOCK_ERROR] Lock error for task ${taskId}: ${error instanceof Error ? error.message : String(error)}${RESET}`);

      // Attempt queue cleanup on lock errors
      try {
        const redis = redisClient.getClient();
        if (redis && scheduleJob) {
          const lockKey = `bull:BIDDING_BOT:${scheduleJob.id}:lock`;
          await redis.del(lockKey);
        }
      } catch (cleanupError) {
        logSystemError('LockCleanup', 'Failed to clean up BullMQ lock', cleanupError);
      }

      // Log lock error for monitoring
      await logger.logError(ErrorCategory.QUEUE, 'OpenSea scheduled bid lock error', {
        taskId,
        lockAcquired,
        jobId: scheduleJob?.id,
        error: error instanceof Error ? error : new Error(String(error))
      });
    }

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
      isTimeout,
      isLockError,
      lockAcquired,
      currentLoopCount,
      processingTimeBeforeError: Date.now() - (scheduleJob?.processedOn || Date.now()),
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
      await logger.logGeneralError(
        'Redis failure in OpenSea scheduled bid',
        {
          taskId: task._id.toString(),
          marketplace: 'opensea',
          errorType: 'REDIS_DOWN',
          error: error instanceof Error ? error : new Error(String(error))
        }
      );
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

async function processOpenseaCollectionBid(data: { taskId: string, collectionDetails: any }) {
  const taskId = data.taskId;

  // Structured log: start

  if (isTaskBlacklisted(taskId, 'opensea')) {
    return;
  }

  try {

    // Fetch task from activeTasks
    const currentTask = activeTasks.get(taskId);
    if (!currentTask || !currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("opensea") || !enabledMarketplaces
      .map((m) => m.toLowerCase())
      .includes("opensea")) {
      return;
    }


    const { collectionDetails } = data;
    const jobId = `opensea-collection-${currentTask.contract.slug}`;


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


    // Parallel fetch: floor price and orders (wallet will be fetched when needed)
    const [floor_price, orders] = await Promise.all([
      getOpenseaFloor(currentTask.contract.slug),
      Order.find({
        slug: currentTask.contract.slug,
        taskId: currentTask._id,
        marketplace: OPENSEA.toLowerCase(),
        identifier: 'collection'
      }).sort({ createdAt: -1 })
    ])


    if (!floor_price) {
      logError('opensea', taskId, 'processOpenseaCollectionBid', 'Failed to fetch floor price', undefined, { slug: currentTask.contract.slug });
      throw new Error(`Failed to fetch floor price for ${currentTask.contract.slug}`);
    }

    floorPrices[currentTask._id].opensea = floor_price as number;

    const stopBid = await stopOption(currentTask, 'opensea', Number(floor_price), false)
    if (stopBid) {
      return;
    }

    const { offerPriceEth, maxBidPriceEth, minBidPriceEth, currencySymbol, currency, decimal } = await calculateBidPrice(currentTask, floor_price as number, "opensea")
    // Fetch fresh balance and check if sufficient for minimum bid
    const { fetchERC20BalanceFresh } = await import('./utils/balance');
    const balance = await fetchERC20BalanceFresh(currentTask.wallet, currency);
    if (balance < minBidPriceEth) {
      const errorMsg = balance <= 0
        ? `Insufficient ${currencySymbol} balance: ${balance} (zero or negative) for task ${taskId}`
        : `Insufficient ${currencySymbol} balance: ${balance} < ${minBidPriceEth} for task ${taskId}`;
      // Increment skip stats
      if (!skipStats[currentTask._id]) {
        skipStats[currentTask._id] = { opensea: 0, magiceden: 0, blur: 0 };
      }
      skipStats[currentTask._id].opensea++;

      // Log the insufficient balance
      await logBidError(taskId, "INSUFFICIENT_BALANCE", errorMsg, "skipped", "opensea");

      // Create empty parent job and complete the marketplace loop
      await ensureParentJobExists(taskId, 'opensea');
      const parentJobId = getParentJobId(taskId, 'opensea');
      const parentJob = parentJobs.get(parentJobId);
      if (parentJob) {
        parentJob.expectedJobs = 0;
        parentJob.totalJobs = 0;
        parentJob.allJobsQueued = true;
        parentJob.completedJobs = 0;
      }

      // Trigger marketplace completion
      await checkTaskCompletionAndScheduleNext(taskId, 'opensea');

      return;
    }

    const expiry = getExpiry(currentTask.bidDuration)
    const usdcBid = currency.toLowerCase() === USDC.toLowerCase()
    let offerPrice = BigInt(Math.ceil(roundToValidPrecision(offerPriceEth) * Math.pow(10, decimal)));

    let colletionOffer = BigInt(offerPrice)
    const [order, ...remainingOrders] = orders
    const ttl = (order?.expiresAt?.getTime() - Date.now()) / 1000

    const currentPrice = Number(order ? order.offerPrice : colletionOffer)

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
        fetchOpenseaOffers('COLLECTION', currentTask.contract.slug, {})
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
        const spread = (Number(highestBidAmount) - Number(secondTopBidAmount)) / Math.pow(10, decimal);
        const secondTopBidAmountEth = Number(secondTopBidAmount) / Math.pow(10, decimal);
        const potentialNewBidEth = secondTopBidAmountEth + outbidMargin;


        if (spread > outbidMargin && Number(secondTopBidAmount) > 0 && potentialNewBidEth >= minBidPriceEth) {
          colletionOffer = BigInt(Math.round(secondTopBidAmount + (outbidMargin * Math.pow(10, decimal))));
        }
        else if (spread > outbidMargin && Number(secondTopBidAmount) > 0 && potentialNewBidEth < minBidPriceEth) {
          // Skip price correction
        }
        else if (ttl > MIN_BID_DURATION) {
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
    const bidCount = getIncrementedBidCount(OPENSEA, currentTask.contract.slug, currentTask._id)

    if (orders.length > 0) {
      const cancelData = orders.map((order, index) => {
        return {
          name: CANCEL_OPENSEA_BID,
          data: { walletId: currentTask.walletId, orderId: order.orderHash, taskId: currentTask._id, orderKey: order._id },
        }
      })
      await queue.addBulk(cancelData);
    }

    // Convert all creator fees to the IFee format (ensuring values are numbers)
    const creatorFees: IFee = Object.fromEntries(
      Object.entries(collectionDetails.creator_fees).map(([key, value]) => [key, Number(value)])
    );

    const offerEth = Number(colletionOffer) / Math.pow(10, decimal);

    // Check if offer exceeds floor price (only for WETH)
    if (currencySymbol.toLowerCase() === 'weth' && offerEth > floor_price) {
      skipStats[currentTask._id]['opensea']++;
      const message = `⚠️ Offer price ${offerEth} ${currencySymbol} for ${currentTask.contract.slug} collection bid exceeds floor price ${floor_price} ${currencySymbol} FOR OPENSEA.`.toUpperCase();


      logBidError(currentTask._id, "OFFER EXCEEDS FLOOR PRICE", message, "warning", "opensea"); // Non-blocking
      return;
    }

    if (currentTask.outbidOptions.outbid && offerEth >= maxBidPriceEth) {
      // Enhanced logging with bid configuration details
      skipStats[currentTask._id]['opensea']++;
      const message = `❌ Offer price ${offerEth} ${currencySymbol} for ${currentTask.contract.slug} collection bid exceeds max bid price ${maxBidPriceEth} ${currencySymbol} FOR OPENSEA.Skipping ...`.toUpperCase()


      logBidError(currentTask._id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "opensea"); // Non-blocking

      if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
        if (orders.length > 0) {
          const cancelData = orders.map((order, index) => {
            return {
              name: CANCEL_OPENSEA_BID,
              data: { walletId: currentTask.walletId, orderId: order.orderHash, taskId: currentTask._id, orderKey: order._id },
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
      // Fetch wallet and decrypt private key just before use
      const wallet = await walletCache.getWalletById(currentTask.walletId);
      const privateKey: string = decryptPrivateKey(wallet?.encryptedPrivateKey as string);

      const bidMetadata = {
        bidCount,
        walletAddress: currentTask.wallet,
        slug: currentTask.contract.slug,
        offerPrice: Number(colletionOffer),
        offerEth: Number(colletionOffer) / Math.pow(10, decimal),
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
        Number(colletionOffer),
        creatorFees,
        expiry,
        currency,
        currentTask.contract.contractAddress
      )

    } catch (bidError) {
      logError('opensea', taskId, 'processOpenseaCollectionBid', 'Failed to place collection bid', bidError, {
        bidCount,
        slug: currentTask.contract.slug,
      });
      throw bidError; // Re-throw to be caught by outer catch
    }

  } catch (error: any) {
    // Format OpenSea error message for better readability
    const errorMessage = extractDetailedOpenSeaError(error);

    logError('opensea', taskId, 'processOpenseaCollectionBid', 'Fatal error processing OpenSea collection bid', error, {
      errorMessage,
      collection: data.collectionDetails?.slug,
    });


    await logBidError(taskId, "OPENSEA COLLECTION BID ERROR", `Error processing OpenSea collection bid for task: ${taskId}: ${errorMessage}`, "error", "opensea");

    // Throw the error
    throw error;
  }
}

async function processBlurScheduledBid(task: ITask) {
  const taskId = task._id.toString();
  const currentLoopCount = loopCounterStats[taskId]?.blur || 1;
  const startTime = Date.now();

  // Log function start


  let lockAcquired = false;

  try {
    const currentTask = activeTasks.get(task._id)
    if (!currentTask || !currentTask?.running || !currentTask.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("blur") || !enabledMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("blur")) {
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

      // Perform preprocessing for Blur
      preprocessing = await performMarketplacePreprocessing(currentTask, "blur", availableMarketplaces);

      // Store in Map for other marketplaces to use
      taskPreprocessing.set(taskId, preprocessing);


      // If preprocessing didn't enable Blur, it means something went wrong
      if (!preprocessing.blur.enabled) {
        const error = new Error(`Preprocessing failed to enable Blur for task ${taskId}`);


        throw error;
      }
    }

    if (preprocessing.blur.skipReason) {

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
      } else {
        return;
      }
    }

    // Acquire lock for this task - will throw if stale or already locked
    // Use timed lock manager for better timeout handling
    lockAcquired = timedLockManager.acquireLock(taskId, 'blur');
    if (!lockAcquired) {

      return;
    }



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
    let bethBalance = 0;
    try {

      const { fetchERC20BalanceFresh } = await import('./utils/balance');
      const { BETH } = await import('./utils/currencies');
      bethBalance = await fetchERC20BalanceFresh(WALLET_ADDRESS, BETH);


    } catch (error) {

      console.error(`${RED}[Blur] Failed to fetch BETH balance: ${error}${RESET}`);
    }

    // Calculate minimum bid price to validate sufficient balance
    const { minBidPriceEth } = await calculateBidPrice(currentTask, 0, "blur");

    // Check if balance is insufficient or zero
    if (bethBalance <= 0 || bethBalance < minBidPriceEth) {
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



          if (!skipStats[taskId]) {
            skipStats[taskId] = { opensea: 0, magiceden: 0, blur: 0 };
          }
          skipStats[taskId].blur++;

          throw new Error(`Authentication blocked - retry in ${waitSeconds}s`);
        }
      }
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

      console.error(`${RED}[Blur Auth Warm-up] Error pre-authenticating wallet ${WALLET_ADDRESS}: ${authError}${RESET}`);

      // Check if error is due to authentication block
      if (authError.message && authError.message.includes('Authentication blocked until')) {
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

        processBulkJobs(traitJobs, true);

        // Register for job creation verification (for this batch)
        registerJobCreationVerification(taskId, 'blur', batchSize);


        // Log batch scheduling
      }

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

      await blurQueue.add(
        BLUR_COLLECTION_BID,
        { taskId: taskId, contractAddress, expiry },
        { jobId, attempts: 1 }
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


    }

  }
}


async function processBlurCollectionBid(data: { taskId: string }) {
  const taskId = data.taskId;
  const startTime = Date.now();

  // Entry log


  // Check if task is blacklisted before processing
  if (isTaskBlacklisted(taskId)) {
    return;
  }

  // Fetch task from activeTasks outside try block so it's accessible in catch
  const currentTask = activeTasks.get(taskId);
  if (!currentTask || !currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("blur") || !enabledMarketplaces.map((market) => market.toLowerCase()).includes("blur")) {
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



    const floor_price: number = Number(await fetchBlurCollectionStats(currentTask.contract.slug));
    if (!floor_price) {
      throw new Error(`Failed to fetch floor price for ${currentTask.contract.slug}`);
    }




    const orders = await Order.find({
      slug: currentTask.contract.slug,
      taskId: currentTask._id,
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
            data: { walletId: currentTask.walletId, payload, taskId: currentTask._id, orderKey: order._id }
          }
        })
        await queue.addBulk(cancelData);
      }

      return
    }

    if (!floorPrices[currentTask._id]) {
      floorPrices[currentTask._id] = {
        opensea: 0,
        magiceden: 0,
        blur: floor_price
      };
    }
    floorPrices[currentTask._id].blur = floor_price;


    const { offerPriceEth, maxBidPriceEth, minBidPriceEth } = await calculateBidPrice(currentTask, floor_price, "blur")



    // Fetch fresh BETH balance and check if sufficient for minimum bid

    const { fetchERC20BalanceFresh } = await import('./utils/balance');
    const { BETH } = await import('./utils/currencies');
    const balance = await fetchERC20BalanceFresh(currentTask.wallet, BETH);



    if (balance < minBidPriceEth) {
      skipStats[currentTask._id]['blur']++;



      await logBidError(
        taskId,
        "INSUFFICIENT_BALANCE",
        `Insufficient BETH balance: ${balance} < ${minBidPriceEth} for task ${taskId} collection bid`,
        "skipped",
        "blur"
      );
      return;
    }

    let colletionOffer = BigInt(Math.round(offerPriceEth * 1e18))

    // Check if offer exceeds floor price (only for WETH)
    const offerEth = Number(colletionOffer) / 1e18;
    if (offerEth > floor_price) {
      skipStats[currentTask._id]['blur']++;
      const message = `⚠️ Offer price ${offerEth} WETH for ${currentTask.contract.slug} collection bid exceeds floor price ${floor_price} WETH FOR BLUR.`.toUpperCase();


      await logBidError(currentTask._id, "OFFER EXCEEDS FLOOR PRICE", message, "warning", "blur");
      return;
    }

    const [order, ...remainingOrders] = orders
    const ttl = (order?.expiresAt?.getTime() - Date.now()) / 1000
    const currentPrice = Number(order ? order.offerPrice : colletionOffer)
    const expiry = getExpiry(currentTask.bidDuration)

    const createdAt = new Date(order?.createdAt).getTime()
    const expiredAt = new Date(order?.expiresAt).getTime()
    const orderExpiration = order ? Math.round((expiredAt - createdAt) / 1000) : expiry

    if (!currentTask.outbidOptions.outbid) {
      if (ttl >= MIN_BID_DURATION && currentPrice === Number(colletionOffer) && Math.abs(orderExpiration - expiry) < 30) {
        return;
      }
    }
    else {

      const highestBid = await fetchBlurBid(currentTask.contract.slug, "COLLECTION", {});
      const [topOffer, secondOffer] = highestBid?.priceLevels?.sort((a, b) => +b.price - +a.price) || [{ price: "0" }, { price: "0" }];
      const topOfferAmount = typeof topOffer === 'object' && topOffer ? Number(topOffer.price) : 0
      const secondOfferAmount = typeof secondOffer === 'object' && secondOffer ? Number(secondOffer.price) : 0

      const bidderAddressesSample = topOffer?.bidderAddressesSample || []
      const numberBidders = topOffer?.numberBidders || 0
      const isTopBid = numberBidders === 1 && bidderAddressesSample.map((address) => address.toLowerCase()).includes(currentTask.wallet.toLowerCase())
      const blurOutbidMargin = currentTask.outbidOptions.blurOutbidMargin || 0.01


      if (isTopBid) {
        const spread = (Number(topOfferAmount) - Number(secondOfferAmount));
        if (+spread.toFixed(2) > blurOutbidMargin && Number(secondOfferAmount) > 0) {
          const bidPrice = secondOfferAmount + blurOutbidMargin
          colletionOffer = BigInt(Math.round(bidPrice * 1e18));
        } else if (ttl > MIN_BID_DURATION) {

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

    const bidCount = getIncrementedBidCount(BLUR, currentTask.contract.slug, currentTask._id)

    if (orders.length > 0) {

      const cancelData = orders.map((order) => {
        const payload = JSON.parse(order.orderHash)

        return {
          name: CANCEL_BLUR_BID,
          data: { walletId: currentTask.walletId, payload, taskId: currentTask._id, orderKey: order._id }
        }
      })
      await queue.addBulk(cancelData);
    }

    // Add tolerance for floating-point comparison (0.000001 = 1e-6)
    const PRICE_TOLERANCE = 0.000001;

    if (currentTask.outbidOptions.outbid && collectionOfferEth >= maxBidPriceEth) {
      if (!skipStats[currentTask._id]) {
        skipStats[currentTask._id] = {
          opensea: 0,
          magiceden: 0,
          blur: 0
        }
      }
      skipStats[currentTask._id]['blur']++;
      const message = `❌ Offer price ${collectionOfferEth} BETH for ${currentTask.contract.slug} collection bid exceeds max bid price ${maxBidPriceEth} BETH FOR BLUR BID.Skipping ...`.toUpperCase()



      await logBidError(currentTask._id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "blur");

      if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
        if (orders.length > 0) {
          const cancelData = orders.map((order) => {
            const payload = JSON.parse(order.orderHash)

            return {
              name: CANCEL_BLUR_BID,
              data: { walletId: currentTask.walletId, payload, taskId: currentTask._id, orderKey: order._id }
            }
          })
          await queue.addBulk(cancelData);
        }
      }

      return;
    } else if (collectionOfferEth < minBidPriceEth) {
      colletionOffer = BigInt(Math.round(minBidPriceEth * 1e18));
    }

    // Validate offer amount meets Blur's minimum before calling bidOnBlur
    const finalOfferEth = Number(colletionOffer) / 1e18;
    const BLUR_MIN_OFFER = 0.01; // Blur's minimum offer amount in ETH

    if (finalOfferEth < BLUR_MIN_OFFER) {
      skipStats[currentTask._id]['blur']++;
      const message = `Offer price ${finalOfferEth} BETH is below minimum blur offer price (${BLUR_MIN_OFFER} BETH) for collection ${currentTask.contract.slug}`;



      await logBidError(currentTask._id, "BELOW_MINIMUM_OFFER", message, "skipped", "blur");
      return;
    }

    // Wrap bidOnBlur call to track completion
    try {


      await bidOnBlur(currentTask._id, bidCount, currentTask.wallet, privateKey, currentTask.contract.contractAddress, colletionOffer, currentTask.contract.slug, expiry);


    } catch (bidError: any) {

      throw bidError; // Re-throw to be handled by outer catch
    }
  } catch (error: any) {
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


    await logBidError(currentTask._id, "BLUR COLLECTION BID ERROR", `Error processing Blur collection bid for task: ${currentTask._id}: ${error?.response?.data?.message || error?.response?.data || error?.message || String(error)}`, "error", "blur");

    // Throw the error
    throw error;
  }
}


async function processOpenseaTraitBid(data: {
  _id: string
  address: string;
  walletId: string;
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
}) {
  let errorMessage: string | undefined;

  // Structured start
  const traitInfo = JSON.parse(data.trait);

  // Check if task is blacklisted before processing
  if (isTaskBlacklisted(data._id)) {
    return;
  }

  try {
    if (!enabledMarketplaces.length) { return; }
    if (!enabledMarketplaces.map((m) => m.toLowerCase()).includes("opensea")) { return; }

    // Check if task is still active
    const task = activeTasks.get(data._id);
    if (!task || !task.running) { return; }

    // Check if OpenSea marketplace status is COMPLETED or STOPPED
    if (taskStatus[data._id]?.marketplaces?.opensea?.status === 'COMPLETED' || taskStatus[data._id]?.marketplaces?.opensea?.status === 'STOPPED') { return; }

    const { address, walletId, slug, creatorFees, enforceCreatorFee, trait, contractAddress, _id } = data

    // Fetch wallet and decrypt private key
    const wallet = await walletCache.getWalletById(walletId);
    const privateKey: string = decryptPrivateKey(wallet?.encryptedPrivateKey as string);

    // Skip trait validation - preprocessing already determined valid traits
    // The trait passed in the job data is already validated
    const parsedTrait = JSON.parse(trait);

    const currentTask = activeTasks.get(_id)
    if (!currentTask || !currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("opensea")) { return; }


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


    if (!floor_price) { throw new Error(`Failed to fetch floor price for ${slug}`); }

    floorPrices[_id].opensea = floor_price as number;

    const stopBid = await stopOption(currentTask, 'opensea', Number(floor_price), false, traitFloor, identifier)
    if (stopBid) {
      if (orders.length > 0) {
        const cancelData = orders.map((order, index) => {
          return {
            name: CANCEL_OPENSEA_BID,
            data: { walletId, orderId: order.orderHash, taskId: _id, orderKey: order._id },
          }
        })
        await queue.addBulk(cancelData)
      }
      return
    }

    const { maxBidPriceEth, offerPriceEth, minBidPriceEth, currencySymbol, currency, decimal } = await calculateBidPrice(currentTask, floor_price as number, "opensea")

    // Fetch fresh balance and check if sufficient for minimum bid
    const { fetchERC20BalanceFresh } = await import('./utils/balance');
    const balance = await fetchERC20BalanceFresh(address, currency);

    if (balance < minBidPriceEth) {
      skipStats[_id]['opensea']++;
      await logBidError(
        _id,
        "INSUFFICIENT_BALANCE",
        `Insufficient ${currencySymbol} balance: ${balance} < ${minBidPriceEth} for task ${_id} trait ${trait}`,
        "skipped",
        "opensea"
      );
      return;
    }


    const outbidOptions = currentTask.outbidOptions

    const usdcBid = currency.toLowerCase() === USDC.toLowerCase()
    let traitOffer = BigInt(Math.round(roundToValidPrecision(offerPriceEth) * Math.pow(10, decimal)))
    const [order, ...remainingOrders] = orders
    const ttl = (order?.expiresAt?.getTime() - Date.now()) / 1000
    const currentPrice = Number(order ? order.offerPrice : traitOffer)

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
        fetchOpenseaOffers('TRAIT', slug, JSON.parse(trait)),
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
        const spread = (Number(highestBidAmount) - Number(secondTopBidAmount)) / Math.pow(10, decimal);
        const secondTopBidAmountEth = Number(secondTopBidAmount) / Math.pow(10, decimal);
        const potentialNewBidEth = secondTopBidAmountEth + outbidMargin;


        if (spread > outbidMargin && Number(secondTopBidAmount) > 0 && potentialNewBidEth >= minBidPriceEth) {
          traitOffer = BigInt(Math.round(+secondTopBidAmount + (outbidMargin * Math.pow(10, decimal))));
        }
        else if (spread > outbidMargin && Number(secondTopBidAmount) > 0 && potentialNewBidEth < minBidPriceEth) {
        }
        else if (ttl > MIN_BID_DURATION) {
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
    const offerEth = Number(traitOffer) / Math.pow(10, decimal);

    if (outbidOptions.outbid && offerEth >= maxBidPriceEth) {
      // Log bid price configuration for debugging
      skipStats[_id]['opensea']++;
      const message = `❌ Offer price ${offerEth} ${currencySymbol} for ${slug} trait ${trait} bid exceeds max bid price ${maxBidPriceEth} ${currencySymbol} FOR OPENSEA.Skipping ...`.toUpperCase()
      logBidError(_id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "opensea"); // Non-blocking

      if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
        if (orders.length > 0) {
          const cancelData = orders.map((order, index) => {
            return {
              name: CANCEL_OPENSEA_BID,
              data: { walletId, orderId: order.orderHash, taskId: _id, orderKey: order._id },
            }
          })

          // Log cancellation enqueue
          queue.addBulk(cancelData)
        }
      }

      return;
    } else if (offerEth < minBidPriceEth) {
      traitOffer = BigInt(Math.round(minBidPriceEth * Math.pow(10, decimal)));
    }

    if (orders.length > 0) {
      await cancelOpenseaOrdersDirectly({
        taskId: _id,
        walletId,
        privateKey,
        orders,
        source: 'processOpenseaTraitBid',
        identifier,
        reason: 'placing_new_bid'
      });
    }

    try {
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

      // Log successful bid placement

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
      await logBidError(data._id, "OPENSEA TRAIT BID ERROR", `Error bidding on trait ${data.trait} in collection ${data.slug}: ${errorMessage}`, "error", "opensea");
    }

    // Throw the error
    throw error;
  }
}

async function processOpenseaTokenBid(data: IProcessOpenseaTokenBidData) {
  const taskId = data._id;

  // Structured start

  // Check if task is blacklisted
  if (isTaskBlacklisted(taskId, 'opensea')) { return; }

  let errorMessage = ''
  try {

    if (!enabledMarketplaces.length) { return; }
    if (!enabledMarketplaces.map((m) => m.toLowerCase()).includes("opensea")) { return; }
    const { address, walletId, slug, creatorFees, enforceCreatorFee, asset, _id } = data

    // Fetch wallet and decrypt private key
    const wallet = await walletCache.getWalletById(walletId);

    if (!currentTokenIds[_id]?.opensea?.includes(asset.tokenId.toString())) { return; }

    // Check if token is cached as trading disabled
    if (tradingDisabledCache.isDisabled(asset.contractAddress, +asset.tokenId, 'opensea')) {
      const message = `Token ${asset.tokenId} is cached as trading disabled - skipping`;

      // Log the skip and update error stats
      logBidError(_id, "TRADING DISABLED (CACHED)", message, "skipped", "opensea"); // Non-blocking

      if (!errorStats[_id]) {
        errorStats[_id] = { opensea: 0, magiceden: 0, blur: 0 };
      }
      // Error tracking is now handled in queue event handlers

      return;
    }

    const currentTask = activeTasks.get(_id)
    if (!currentTask || !currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("opensea")) { return; }


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

    if (!floor_price) { throw new Error(`Failed to fetch floor price for ${slug}`); }

    floorPrices[_id].opensea = floor_price as number;

    const stopBid = await stopOption(currentTask, 'opensea', Number(floor_price), false)
    if (stopBid) {
      if (orders.length > 0) {
        const cancelData = orders.map((order, index) => {
          return {
            name: CANCEL_OPENSEA_BID,
            data: { walletId, orderId: order.orderHash, taskId: _id, orderKey: order._id }
          }
        })

        // Log cancellation enqueue
        queue.addBulk(cancelData)
      }
      return
    }

    const { maxBidPriceEth, offerPriceEth, minBidPriceEth, currencySymbol, currency, decimal } = await calculateBidPrice(currentTask, floor_price as number, "opensea")
    const outbidOptions = currentTask.outbidOptions
    const expiry = getExpiry(currentTask.bidDuration)
    const usdcBid = currency.toLowerCase() === USDC.toLowerCase()

    // Fetch fresh balance and check if sufficient for minimum bid
    const { fetchERC20BalanceFresh } = await import('./utils/balance');
    const balance = await fetchERC20BalanceFresh(address, currency);

    if (balance < minBidPriceEth) {
      skipStats[_id]['opensea']++;
      await logBidError(
        _id,
        "INSUFFICIENT_BALANCE",
        `Insufficient ${currencySymbol} balance: ${balance} < ${minBidPriceEth} for task ${_id} token ${asset.tokenId}`,
        "skipped",
        "opensea"
      );
      return;
    }

    let tokenOffer = BigInt(Math.round(offerPriceEth * Math.pow(10, decimal)))

    const [order] = orders
    const ttl = (order?.expiresAt?.getTime() - Date.now()) / 1000
    const currentPrice = Number(order ? order.offerPrice : tokenOffer)

    const createdAt = new Date(order?.createdAt).getTime()
    const expiredAt = new Date(order?.expiresAt).getTime()
    const orderExpiration = order ? Math.round((expiredAt - createdAt) / 1000) : expiry


    if (!outbidOptions.outbid) {

      if (ttl >= MIN_BID_DURATION && currentPrice === Number(tokenOffer) && Math.abs(orderExpiration - expiry) < 30) {
        return;
      }
    } else {
      // Resolve contract address for Max Pain collection override support
      const resolvedContractAddress = await resolveContractAddress(
        slug,
        asset.tokenId,
        asset.contractAddress
      );

      // Parallel fetch: wallets and item offers
      const [walletsArr, highestBid] = await Promise.all([
        walletCache.getAllWallets(),
        fetchItemOffers(resolvedContractAddress, asset.tokenId.toString())
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
        const spread = (Number(highestBidAmount) - Number(secondTopBidAmount)) / Math.pow(10, decimal);
        const secondTopBidAmountEth = Number(secondTopBidAmount) / Math.pow(10, decimal);
        const potentialNewBidEth = secondTopBidAmountEth + outbidMargin;


        if (spread > outbidMargin && Number(secondTopBidAmount) > 0 && potentialNewBidEth >= minBidPriceEth) {
          tokenOffer = BigInt(Math.round(+secondTopBidAmount + (outbidMargin * Math.pow(10, decimal))));
        }
        else if (spread > outbidMargin && Number(secondTopBidAmount) > 0 && potentialNewBidEth < minBidPriceEth) {
        }
        else if (ttl > MIN_BID_DURATION) {
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

    if (outbidOptions.outbid && offerEth >= maxBidPriceEth) {
      if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
        if (orders.length > 0) {
          const cancelData = orders.map((order, index) => {
            return {
              name: CANCEL_OPENSEA_BID,
              data: { walletId, orderId: order.orderHash, taskId: _id, orderKey: order._id }
            }
          })

          // Log cancellation enqueue
          queue.addBulk(cancelData)
        }
      }

      const message = `❌ Offer price ${offerEth} ${currencySymbol} for ${slug} ${asset.tokenId} bid exceeds max bid price ${maxBidPriceEth} ${currencySymbol} FOR OPENSEA.Skipping ...`.toUpperCase()
      skipStats[_id].opensea++;
      logBidError(_id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "opensea");
      return;
    } else if (offerEth < minBidPriceEth) {
      tokenOffer = BigInt(Math.round(minBidPriceEth * Math.pow(10, decimal)));
    }

    // Cancel existing orders only when we're actually proceeding with the bid
    if (orders.length > 0) {
      const cancelData = orders.map((order, index) => {
        return {
          name: CANCEL_OPENSEA_BID,
          data: { walletId, orderId: order.orderHash, taskId: _id, orderKey: order._id }
        }
      })

      // Log cancellation enqueue
      queue.addBulk(cancelData)
    }

    try {
      // Fetch wallet and decrypt private key just before use
      const wallet = await walletCache.getWalletById(walletId);
      const privateKey: string = decryptPrivateKey(wallet?.encryptedPrivateKey as string);

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
    } catch (bidError: any) {

      // Format OpenSea error message for better readability
      let primaryMessage = '';
      if (bidError?.response?.data?.errors && Array.isArray(bidError.response.data.errors)) {
        // Extract the first error message from OpenSea API errors
        const extracted = bidError.response.data.errors[0];
        primaryMessage = typeof extracted === 'string'
          ? extracted
          : extracted?.message || 'Unknown OpenSea error';

        // Special handling for "Trading is disabled" errors
        if (primaryMessage.includes('Trading is disabled')) {
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

          // Log as warning with updated message
          await logBidError(data._id, "OPENSEA TOKEN TRADING DISABLED",
            `Token ${data.asset.tokenId} in collection ${data.slug}: ${primaryMessage} - Cached for 24 hours`,
            "warning", "opensea");

          errorMessage = primaryMessage;

          return; // Exit early without re-throwing
        }

        // Special handling for "API key has expired" errors (HTTP 401)
        if (bidError?.response?.status === 401 || primaryMessage.includes('API key has expired')) {
          // Log critical error for API key expiration
          await logger.logError(
            ErrorCategory.API,
            `OPENSEA_API_KEY_EXPIRED - OpenSea API key has expired - manual renewal required. Task: ${data._id}, Token: ${data.asset.tokenId}`,
            {
              taskId: data._id ? data._id.toString() : undefined,
              tokenId: data.asset.tokenId ? data.asset.tokenId.toString() : undefined,
              collection: data.slug,
              statusCode: bidError?.response?.status ? bidError.response.status.toString() : undefined,
              errorMessage: primaryMessage,
              component: 'opensea_token_bid'
            }
          );

          console.error(RED + `[CRITICAL] OpenSea API key has expired! All OpenSea bids will fail until key is renewed.` + RESET);

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
  const { taskId, bidCount, walletAddress, walletId, slug, offerPrice, creatorFees, enforceCreatorFee, expiry } = data;

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

  const floor_price = await getOpenseaFloor(currentTask.contract.slug);

  if (!floor_price) {
    return;
  }

  const { currency, decimal, currencySymbol } = await calculateBidPrice(currentTask, floor_price as number, "opensea");

  const offerEth = Number(offerPrice) / Math.pow(10, decimal);

  if (currencySymbol.toLowerCase() === "weth" && offerEth > floor_price) {
    const message = `⚠️ Counter bid offer price ${offerEth} ${currencySymbol} for ${slug} collection exceeds floor price ${floor_price} ${currencySymbol} FOR OPENSEA.`.toUpperCase();
    await logBidError(taskId, "COUNTER BID OFFER EXCEEDS FLOOR PRICE", message, "warning", "opensea");
    return;
  }

  try {
    const existingOrders = await Order.find({
      slug,
      taskId,
      marketplace: "opensea",
      identifier: "collection",
    });

    if (existingOrders.length > 0) {
      const cancelData = existingOrders.map((order) => ({
        name: CANCEL_OPENSEA_BID,
        data: { walletId: walletId, orderId: order.orderHash, taskId, orderKey: order._id },
      }));
      await queue.addBulk(cancelData);
    }

    const wallet = await walletCache.getWalletById(walletId);
    const privateKey: string = decryptPrivateKey(wallet?.encryptedPrivateKey as string);

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
      currentTask.contract.contractAddress
    );

    if (!counterbidCounts[taskId]) {
      counterbidCounts[taskId] = {
        opensea: 0,
        magiceden: 0,
        blur: 0,
      };
    }
    counterbidCounts[taskId]["opensea"]++;
  } catch (error: any) {
    const errorMessage = extractDetailedOpenSeaError(error);
    logError("opensea", taskId, "openseaCollectionCounterBid", "Error during OpenSea collection counterbid", error, {
      slug,
      offerPrice,
      bidCount,
      expiry,
      errorMessage,
    });
    await logBidError(data.taskId, "OPENSEA COLLECTION COUNTER BID ERROR", `Error processing OpenSea collection counter bid for task: ${data.slug}: ${errorMessage}`, "error", "opensea");
  }
}

async function magicedenCollectionCounterBid(data: IMagicedenCollectionBidData) {
  if (!enabledMarketplaces.length) return
  const { _id, bidCount, address, contractAddress, quantity, offerPrice, expiration, walletId, slug } = data
  const currentTask = activeTasks.get(_id)
  if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("magiceden") || !enabledMarketplaces.map((m) => m.toLowerCase()).includes("magiceden")) return

  const floor_price = Number(await fetchMagicEdenCollectionStats(contractAddress));
  if (!floor_price) return

  try {
    // Query and cancel existing collection orders
    const orders = await Order.find({
      slug: slug,
      taskId: _id,
      marketplace: MAGICEDEN.toLowerCase(),
      identifier: "collection"
    });

    if (orders.length > 0) {
      const cancelData = orders.map((order) => ({
        name: CANCEL_MAGICEDEN_BID,
        data: { walletId, orderId: order.orderHash, taskId: _id, orderKey: order._id }
      }));
      await queue.addBulk(cancelData);
    }

    const { currencyType, currency, decimal, currencySymbol } = await calculateBidPrice(currentTask, floor_price, "magiceden")
    const offerEth = Number(offerPrice) / Math.pow(10, decimal);

    // Check if offer exceeds floor price (only for WETH)
    if (currencySymbol.toLowerCase() === 'weth' && offerEth > floor_price) {
      const message = `⚠️ Counter bid offer price ${offerEth} ${currencySymbol} for ${slug} collection exceeds floor price ${floor_price} ${currencySymbol} FOR MAGICEDEN.Stopping bid...`.toUpperCase();


      await logBidError(_id, "COUNTER BID OFFER EXCEEDS FLOOR PRICE", message, "warning", "magiceden");
      return;
    }

    // Fetch wallet and decrypt private key just before use
    const wallet = await walletCache.getWalletById(walletId);
    const privateKey: string = decryptPrivateKey(wallet?.encryptedPrivateKey as string);

    await bidOnMagiceden(_id, bidCount, address, contractAddress, quantity, offerPrice.toString(), privateKey, slug, expiration, currency);

    // Initialize and increment counterbidCounts after successful bid
    if (!counterbidCounts[_id]) {
      counterbidCounts[_id] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      };
    }
    counterbidCounts[_id]['magiceden']++;

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
  walletId: string
  slug: string
  trait: string
}

async function magicedenTraitCounterBid(data: IMagicedenTraitBidData) {
  if (!enabledMarketplaces.length) return
  const { taskId, bidCount, address, contractAddress, quantity, offerPrice, expiration, walletId, slug, trait } = data
  const currentTask = activeTasks.get(taskId)
  if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("magiceden") || !enabledMarketplaces.map((m) => m.toLowerCase()).includes("magiceden")) return
  const triatData = JSON.parse(trait)
  const bidTrait = {
    attributeKey: triatData?.attributeKey,
    attributeValue: triatData?.attributeValue,
  }

  try {
    // Query and cancel existing trait orders
    const identifier = `${bidTrait.attributeKey}:${bidTrait.attributeValue}`;
    const orders = await Order.find({
      slug: slug,
      taskId: taskId,
      marketplace: MAGICEDEN.toLowerCase(),
      identifier: identifier
    });

    if (orders.length > 0) {
      const cancelData = orders.map((order) => ({
        name: CANCEL_MAGICEDEN_BID,
        data: { walletId, orderId: order.orderHash, taskId, orderKey: order._id }
      }));
      await queue.addBulk(cancelData);
    }

    const { currencyType, currency } = await calculateBidPrice(currentTask, 0, "magiceden")

    // Fetch wallet and decrypt private key just before use
    const wallet = await walletCache.getWalletById(walletId);
    const privateKey: string = decryptPrivateKey(wallet?.encryptedPrivateKey as string);

    await bidOnMagiceden(taskId, bidCount, address, contractAddress, quantity, offerPrice.toString(), privateKey, slug, expiration, currency, bidTrait);

    // Initialize and increment counterbidCounts after successful bid
    if (!counterbidCounts[taskId]) {
      counterbidCounts[taskId] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      };
    }
    counterbidCounts[taskId]['magiceden']++;
  } catch (error: any) {
    await logBidError(data.taskId, "MAGICEDEN TRAIT COUNTER BID ERROR", `Error processing MagicEden trait counter bid for task: ${data.slug}: ${error?.response?.data?.message || error?.response?.data || error?.message || String(error)}`, "error", "magiceden");
  }
}

async function blurCollectionCounterBid(data: IBlurBidData) {
  const startTime = Date.now();
  const { taskId, bidCount, address, walletId, contractAddress, offerPrice, slug, expiry } = data


  if (!enabledMarketplaces.length) {
    return
  }

  const currentTask = activeTasks.get(taskId)

  if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("blur") || !enabledMarketplaces.map((m) => m.toLowerCase()).includes("blur")) {
    return
  }


  const floor_price: number = Number(await fetchBlurCollectionStats(currentTask.contract.slug));
  if (!floor_price) {
    return
  }


  const offerEth = Number(offerPrice) / 1e18;

  if (offerEth > floor_price) {
    const message = `⚠️ Counter bid offer price ${offerEth} WETH for ${slug} collection exceeds floor price ${floor_price} WETH FOR BLUR.Stopping bid...`.toUpperCase();


    await logBidError(taskId, "COUNTER BID OFFER EXCEEDS FLOOR PRICE", message, "warning", "blur");
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


      const cancelData = existingOrders.map((order) => {
        const payload = JSON.parse(order.orderHash);
        return {
          name: CANCEL_BLUR_BID,
          data: {
            walletId: walletId,
            payload: payload,
            taskId: taskId,
            orderKey: order._id
          }
        };
      });

      await queue.addBulk(cancelData);
    } else {
    }

    // Fetch wallet and decrypt private key just before use

    const wallet = await walletCache.getWalletById(walletId);
    const privateKey: string = decryptPrivateKey(wallet?.encryptedPrivateKey as string);


    await bidOnBlur(taskId, bidCount, address, privateKey, contractAddress, BigInt(offerPrice), slug, expiry);

    // Initialize and increment counterbidCounts after successful bid
    if (!counterbidCounts[taskId]) {
      counterbidCounts[taskId] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      };
    }
    counterbidCounts[taskId]['blur']++;


  } catch (error: any) {
    // Handle rate limit errors
    if (error?.code === 'RATE_LIMITED') {
      throw error; // Re-throw to let worker handle retry with delay
    }


    // Handle transfer restriction errors
    if (error.code === 'TRANSFER_RESTRICTED') {
      await logBidError(taskId, "BLUR TRANSFER RESTRICTION", error.message, "warning", "blur");
      return;
    }


    await logBidError(taskId, "BLUR COLLECTION COUNTER BID ERROR", `Error processing Blur collection counter bid for task: ${slug}: ${error?.response?.data?.message || error?.response?.data || error?.message || String(error)}`, "error", "blur");
  }
}

async function blurTraitCounterBid(data: IBlurBidData) {
  const startTime = Date.now();
  const { taskId, bidCount, address, walletId, contractAddress, offerPrice, slug, expiry, traits } = data


  if (!enabledMarketplaces.length) {
    return
  }

  const currentTask = activeTasks.get(taskId)
  if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("blur") || !enabledMarketplaces.map((m) => m.toLowerCase()).includes("blur")) {
    return
  }

  try {
    // Cancel existing orders for this trait before placing new counterbid

    if (!traits) {
      return
    }
    const traitsObj = JSON.parse(traits);
    const [traitType, traitValue] = Object.entries(traitsObj)[0] as string[];
    const identifier = `${traitType}:${traitValue}`;




    const existingOrders = await Order.find({
      slug: slug,
      taskId: taskId,
      marketplace: "blur",
      identifier: identifier
    });


    if (existingOrders.length > 0) {

      const cancelData = existingOrders.map((order) => {
        const payload = JSON.parse(order.orderHash);
        return {
          name: CANCEL_BLUR_BID,
          data: {
            walletId: walletId,
            payload: payload,
            taskId: taskId,
            orderKey: order._id
          }
        };
      });

      await queue.addBulk(cancelData);
    } else {
    }
    // Fetch wallet and decrypt private key just before use

    const wallet = await walletCache.getWalletById(walletId);
    const privateKey: string = decryptPrivateKey(wallet?.encryptedPrivateKey as string);


    await bidOnBlur(taskId, bidCount, address, privateKey, contractAddress, BigInt(offerPrice), slug, expiry, traits);

    // Initialize and increment counterbidCounts after successful bid
    if (!counterbidCounts[taskId]) {
      counterbidCounts[taskId] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      };
    }
    counterbidCounts[taskId]['blur']++;

  } catch (error: any) {
    const errorIdentifier = traits ? (() => {
      try {
        const traitsObj = JSON.parse(traits);
        const [traitType, traitValue] = Object.entries(traitsObj)[0] as string[];
        return `${traitType}:${traitValue}`;
      } catch {
        return 'unknown';
      }
    })() : 'unknown';

    // Handle rate limit errors
    if (error?.code === 'RATE_LIMITED') {
      throw error; // Re-throw to let worker handle retry with delay
    }

    // Handle transfer restriction errors
    if (error.code === 'TRANSFER_RESTRICTED') {
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
  walletId: string;
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
    walletId,
    slug,
    offerPrice,
    creatorFees,
    enforceCreatorFee,
    asset
  } = data;
  const taskId = _id;

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

  const selectsOpensea = currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("opensea");
  if (!selectsOpensea) {
    return;
  }

  const openseaEnabled = enabledMarketplaces.map((m) => m.toLowerCase()).includes("opensea");
  if (!openseaEnabled) {
    return;
  }

  const expiry = getExpiry(currentTask.bidDuration);

  const bidCount = getIncrementedBidCount(OPENSEA, slug, taskId);

  try {
    const orders = await Order.find({
      slug,
      taskId,
      marketplace: OPENSEA.toLowerCase(),
      identifier: asset.tokenId.toString()
    });

    if (orders.length > 0) {
      const mostRecentOrder = orders.sort((a, b) => b.createdAt.getTime() - a.createdAt.getTime())[0];
      const currentOfferWei = BigInt(mostRecentOrder.offerPrice);
      const incomingOfferWei = BigInt(offerPrice);

      if (currentOfferWei >= incomingOfferWei) {
        return;
      }

      // Cancel existing orders since we're placing a higher bid
      const cancelData = orders.map((order) => ({
        name: CANCEL_OPENSEA_BID,
        data: { walletId, orderId: order.orderHash, taskId, orderKey: order._id }
      }));
      await queue.addBulk(cancelData);
    }

    const { currency } = await calculateBidPrice(currentTask, 0, "opensea");

    const wallet = await walletCache.getWalletById(walletId);
    const privateKey: string = decryptPrivateKey(wallet?.encryptedPrivateKey as string);

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
      asset
    );

    if (!counterbidCounts[taskId]) {
      counterbidCounts[taskId] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      };
    }
    counterbidCounts[taskId]['opensea']++;

  } catch (error: any) {
    const errorMessage = extractDetailedOpenSeaError(error);
    logError("opensea", taskId, "openseaTokenCounterBid", "Error processing OpenSea token counterbid", error, {
      slug,
      tokenId: asset.tokenId,
      offerPrice,
      bidCount,
      errorMessage
    });
    await logBidError(data._id, "OPENSEA TOKEN COUNTER BID ERROR", `Error bidding on token ${data.asset.tokenId} in collection ${data.slug}: ${errorMessage}`, "error", "opensea");
  }
}

async function openseaTraitCounterBid(data: IProcessOpenseaTraitBidData) {
  const { _id, bidCount, address, walletId, slug, offerPrice, creatorFees, enforceCreatorFee, expiry, trait } = data;
  const taskId = _id;
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

  const selectsOpensea = currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("opensea");
  if (!selectsOpensea) {
    return;
  }

  const openseaEnabled = enabledMarketplaces.map((m) => m.toLowerCase()).includes("opensea");
  if (!openseaEnabled) {
    return;
  }

  if (!trait) {
    return;
  }

  const parsedTrait: { type: string; value: string } = JSON.parse(trait);
  const identifier = `${parsedTrait.type}:${parsedTrait.value}`;

  try {
    const orders = await Order.find({
      slug,
      taskId,
      marketplace: OPENSEA.toLowerCase(),
      identifier
    });

    const wallet = await walletCache.getWalletById(walletId);
    const privateKey: string = decryptPrivateKey(wallet?.encryptedPrivateKey as string);

    if (orders.length > 0) {
      await cancelOpenseaOrdersDirectly({
        taskId,
        walletId,
        privateKey,
        orders,
        source: 'openseaTraitCounterBid',
        identifier,
        reason: 'counterbid_replace_existing'
      });
    }

    const { currency } = await calculateBidPrice(currentTask, 0, "opensea");

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
      trait
    );

    if (!counterbidCounts[taskId]) {
      counterbidCounts[taskId] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      };
    }
    counterbidCounts[taskId]['opensea']++;
  } catch (error: any) {
    const errorMessage = extractDetailedOpenSeaError(error);
    logError("opensea", taskId, "openseaTraitCounterBid", "Error processing OpenSea trait counterbid", error, {
      slug,
      traitIdentifier: identifier,
      offerPrice,
      bidCount,
      errorMessage
    });
    await logBidError(data._id, "OPENSEA TRAIT COUNTER BID ERROR", `Error bidding on trait ${data.trait} in collection ${data.slug}: ${errorMessage}`, "error", "opensea");
    throw error;
  }
}

async function magicedenTokenCounterBid(data: IMagicedenTokenBidData) {
  try {
    if (!enabledMarketplaces.length) return
    const { _id, address, contractAddress, quantity, offerPrice, walletId, slug, tokenId, expiration } = data
    const currentTask = activeTasks.get(_id)
    if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("magiceden") || !enabledMarketplaces.map((m) => m.toLowerCase()).includes("magiceden")) return

    // Check if current offer in database is greater than incoming offer price
    const orders = await Order.find({
      slug: slug,
      taskId: _id,
      marketplace: MAGICEDEN.toLowerCase(),
      identifier: tokenId.toString()
    });

    if (orders.length > 0 && offerPrice !== undefined) {
      const mostRecentOrder = orders.sort((a, b) => b.createdAt.getTime() - a.createdAt.getTime())[0];
      const currentOfferWei = BigInt(mostRecentOrder.offerPrice);
      const incomingOfferWei = BigInt(offerPrice);

      if (currentOfferWei >= incomingOfferWei) {
        return;
      }

      // Cancel existing orders since we're placing a higher bid
      const cancelData = orders.map((order) => ({
        name: CANCEL_MAGICEDEN_BID,
        data: { walletId, orderId: order.orderHash, taskId: _id, orderKey: order._id }
      }));
      await queue.addBulk(cancelData);
    }

    const { currency } = await calculateBidPrice(currentTask, 0, "magiceden")

    const bidCount = getIncrementedBidCount(MAGICEDEN, slug, _id)
    // Fetch wallet and decrypt private key just before use
    const wallet = await walletCache.getWalletById(walletId);
    const privateKey: string = decryptPrivateKey(wallet?.encryptedPrivateKey as string);

    await bidOnMagiceden(_id, bidCount, address, contractAddress, quantity, (offerPrice as number).toString(), privateKey, slug, expiration, currency, undefined, tokenId)

    // Initialize and increment counterbidCounts after successful bid
    if (!counterbidCounts[_id]) {
      counterbidCounts[_id] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      };
    }
    counterbidCounts[_id]['magiceden']++;
  } catch (error: any) {
    await logBidError(data._id, "MAGICEDEN TOKEN COUNTER BID ERROR", `Error bidding on token ${data.tokenId} in collection ${data.slug}: ${error?.response?.data?.message || error?.response?.data || error?.message || String(error)}`, "error", "magiceden");
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
  const taskId = data._id;
  const startTime = Date.now();

  // Log entry


  try {
    if (!enabledMarketplaces.length) {
      return;
    }
    const { address, walletId, contractAddress, slug, trait, _id } = data;


    // Fetch wallet and decrypt private key
    const wallet = await walletCache.getWalletById(walletId);
    const privateKey: string = decryptPrivateKey(wallet?.encryptedPrivateKey as string);

    // Skip trait validation - preprocessing already determined valid traits
    // The trait passed in the job data is already validated
    const parsedTrait = JSON.parse(trait);

    const currentTask = activeTasks.get(_id)

    if (!currentTask || !currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("blur") || !enabledMarketplaces.map((m) => m.toLowerCase()).includes("blur")) {
      return;
    }
    const outbidOptions = currentTask.outbidOptions


    const floor_price: number = Number(await fetchBlurCollectionStats(currentTask.contract.slug));
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
            data: { walletId, payload, taskId: _id, orderKey: order._id }
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


    const { fetchERC20BalanceFresh } = await import('./utils/balance');
    const { BETH } = await import('./utils/currencies');
    const balance = await fetchERC20BalanceFresh(address, BETH);


    if (balance < minBidPriceEth) {

      skipStats[_id]['blur']++;
      await logBidError(
        _id,
        "INSUFFICIENT_BALANCE",
        `Insufficient BETH balance: ${balance} < ${minBidPriceEth} for task ${_id} trait ${trait}`,
        "skipped",
        "blur"
      );
      return;
    }


    let traitOffer = BigInt(Math.round(Number(offerPriceEth * Math.pow(10, 18)) / 1e16) * 1e16);


    const [order, ...remainingOrders] = orders
    const ttl = (order?.expiresAt?.getTime() - Date.now()) / 1000
    const currentPrice = Number(order ? order.offerPrice : traitOffer)

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

      const isTopBid = numberBidders === 1 && bidderAddressesSample.map((address) => address.toLowerCase()).includes(currentTask.wallet.toLowerCase())


      if (isTopBid) {
        const spread = (Number(topOfferAmount) - Number(secondOfferAmount));
        if (+spread.toFixed(2) > blurOutbidMargin && Number(secondOfferAmount) > 0) {
          const bidPrice = secondOfferAmount + blurOutbidMargin

          traitOffer = BigInt(Math.round((bidPrice * 1e18) / 1e16) * 1e16);
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
          data: { walletId, payload, taskId: _id, orderKey: order._id }
        }
      })
      // Log cancellation enqueue
      queue.addBulk(cancelData);
    }

    // Add tolerance for floating-point comparison (0.000001 = 1e-6)
    const PRICE_TOLERANCE = 0.000001;

    if (outbidOptions.outbid && offerEth >= maxBidPriceEth) {
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
      if (currentTask.outbidOptions.cancelMaxWhenOutbid) {

        if (orders.length > 0) {
          const cancelData = orders.map((order) => {
            const payload = JSON.parse(order.orderHash)

            return {
              name: CANCEL_BLUR_BID,
              data: { walletId, payload, taskId: _id, orderKey: order._id }
            }
          })
          // Log cancellation enqueue
          queue.addBulk(cancelData);
        }
      }

      return;
    } else if (offerEth < minBidPriceEth) {
      traitOffer = BigInt(Math.round(minBidPriceEth * 1e18));
    }



    await bidOnBlur(_id, bidCount, address, privateKey, contractAddress, traitOffer, slug, expiry, trait, data.batchInfo);


  } catch (error: any) {
    const errorMessage = error?.message || String(error);
    const errorIdentifier = data.trait ? `${JSON.parse(data.trait).type || 'unknown'}:${JSON.parse(data.trait).value || 'unknown'}` : 'unknown';

    // Handle rate limit errors
    if (error?.code === 'RATE_LIMITED') {
      throw error; // Re-throw to let worker handle retry with delay
    }

    // Handle transfer restriction errors
    if (error.code === 'TRANSFER_RESTRICTED') {
      await logBidError(data._id, "BLUR TRANSFER RESTRICTION", error.message, "warning", "blur");
      return;
    }


    console.error(`${RED}[BLUR TRAIT BID] Error:${RESET}`, error instanceof Error ? error.message : String(error));

    // Don't log bid errors for stopped/cancelled tasks
    if (!errorMessage.includes('Bid execution blocked - task') && !errorMessage.includes('is stopped')) {
      await logBidError(data._id, "BLUR TRAIT BID ERROR", `Error bidding on trait ${data.trait} in collection ${data.slug}: ${error?.response?.data?.message || error?.response?.data || errorMessage}`, "error", "blur");
    }

    // Throw the error
    throw error;
  }
}

async function processMagicedenScheduledBid(task: ITask, jobId?: string, scheduleJob?: Job) {
  // Structured start
  const taskId = task._id.toString();
  const currentLoopCount = loopCounterStats[taskId]?.magiceden || 1;

  // Log if this appears to be a retry (job has an ID and attempt number)
  if (scheduleJob && scheduleJob.attemptsMade > 0) {
  }

  const checkpointStart = Date.now();
  let lockAcquired = false;

  // Timeout protection wrapper - 120 seconds max processing time
  const TIMEOUT_DURATION = 120000; // 2 minutes
  const timeoutPromise = new Promise<never>((_, reject) => {
    setTimeout(() => {
      reject(new Error(`MagicEden scheduled bid timeout after ${TIMEOUT_DURATION}ms for task ${taskId}`));
    }, TIMEOUT_DURATION);
  });

  try {
    // Wrap the main processing logic in a timeout
    await Promise.race([
      (async () => {
        // Early return consolidation
        if (!enabledMarketplaces.map(m => m.toLowerCase()).includes("magiceden")) { return; }

        const currentTask = activeTasks.get(task._id);

        if (!currentTask?.running || !currentTask.selectedMarketplaces.map(m => m.toLowerCase()).includes("magiceden")) { return; }

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

          // Perform preprocessing for MagicEden
          preprocessing = await performMarketplacePreprocessing(currentTask, "magiceden", availableMarketplaces);

          // Store in Map for other marketplaces to use
          taskPreprocessing.set(taskId, preprocessing);

          // If preprocessing didn't enable MagicEden, it means something went wrong
          if (!preprocessing.magiceden.enabled) { throw new Error(`Preprocessing failed to enable MagicEden for task ${taskId}`); }
        }

        if (preprocessing.magiceden.skipReason) {
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

        // Acquire lock for this task with BullMQ lock validation
        try {
          // Validate BullMQ lock before acquiring our custom lock
          if (scheduleJob && scheduleJob.id) {
            const lockKey = `bull:BIDDING_BOT:${scheduleJob.id}:lock`;
            const redis = redisClient.getClient();
            if (redis) {
              const lockExists = await redis.get(lockKey);
              if (!lockExists) {
                // Continue processing but add monitoring
                await logger.logError(ErrorCategory.QUEUE, 'Missing BullMQ lock detected in MagicEden scheduled bid', {
                  taskId,
                  jobId: scheduleJob.id,
                  lockKey
                });
              }
            }
          }

          lockAcquired = timedLockManager.acquireLock(taskId, 'magiceden');
          if (!lockAcquired) {
            return;
          }
        } catch (lockError) {
          console.error(`${RED}[LOCK ERROR] Failed to acquire lock for task ${taskId}: ${lockError}${RESET}`);

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
              console.error(`${RED}[LOCK RECOVERY] Failed to recover lock for task ${taskId}: ${recoveryError}${RESET}`);
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
          const { fetchERC20BalanceFresh } = await import('./utils/balance');
          const { WETH } = await import('./utils/currencies');
          wethBalance = await fetchERC20BalanceFresh(WALLET_ADDRESS, WETH);
        } catch (error) {
          console.error(`${RED}[MagicEden] Failed to fetch WETH balance: ${error}${RESET}`);
        }

        const contractAddress = currentTask.contract.contractAddress
        const expiration = getExpiry(currentTask.bidDuration)
        const newTask = { ...currentTask, traits: {} } as ITask

        // Note: Status will be set to appropriate state after job creation is determined


        // Create jobs based on preprocessing results
        if (preprocessing.magiceden.tokens.length > 0) {
          // Token bid

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

            // Fire-and-forget: processBulkJobs runs asynchronously to prevent timeout
            processBulkJobs(jobs, true, scheduleJob);

            // Register for job creation verification
            registerJobCreationVerification(taskId, 'magiceden', preprocessing.magiceden.tokens.length);


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

            // Fire-and-forget: processBulkJobs runs asynchronously to prevent timeout
            processBulkJobs(traitJobs, true, scheduleJob);

            // Register for job creation verification
            registerJobCreationVerification(taskId, 'magiceden', preprocessing.magiceden.traits.length);


            // The task will transition to RUNNING when jobs start processing

          } catch (jobError) {
            logSystemError('MagicEden', 'Failed to queue trait bid jobs', jobError);

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
          const jobId = `bid-${taskId}-magiceden-${currentTask.contract.slug}-collection`

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

          await queue.add(
            MAGICEDEN_COLLECTION_BID,
            { taskId: taskId },
            { jobId, attempts: 1 }
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
    logSystemError('MagicEden', `Error processing MagicEden scheduled bid for task: ${task._id}`, error, { taskId: task._id, duration: Date.now() - checkpointStart });

    // Enhanced error handling for timeout and lock issues
    const isTimeout = error instanceof Error && error.message.includes('timeout');
    const isLockError = error instanceof Error && (
      error.message.includes('Missing lock') ||
      error.message.includes('Lock acquisition failed') ||
      error.message.includes('LOCK ERROR')
    );

    if (isTimeout) {
      logSystemError('MagicEdenTimeout', `Task ${taskId} timed out after ${TIMEOUT_DURATION}ms`, undefined, { taskId, timeout: TIMEOUT_DURATION });

      // Log timeout with additional context
      await logger.logError(ErrorCategory.QUEUE, 'MagicEden scheduled bid timeout', {
        taskId,
        timeoutDuration: TIMEOUT_DURATION,
        lockAcquired,
        currentLoopCount,
        processingTime: Date.now() - checkpointStart,
        error: error instanceof Error ? error : new Error(String(error))
      });

      // Clean up resources on timeout
      if (lockAcquired) {
        try {
          timedLockManager.releaseLock(taskId, 'magiceden');
        } catch (cleanupError) {
          logSystemError('TimeoutCleanup', `Failed to release lock for task ${taskId}`, cleanupError, { taskId });
        }
      }
    }

    if (isLockError) {
      logSystemError('MagicEdenLockError', `Lock error for task ${taskId}`, error, { taskId });

      // Attempt queue cleanup on lock errors
      try {
        const redis = redisClient.getClient();
        if (redis && scheduleJob) {
          const lockKey = `bull:BIDDING_BOT:${scheduleJob.id}:lock`;
          await redis.del(lockKey);
        }
      } catch (cleanupError) {
        logSystemError('LockCleanup', 'Failed to clean up BullMQ lock', cleanupError);
      }

      // Log lock error for monitoring
      await logger.logError(ErrorCategory.QUEUE, 'MagicEden scheduled bid lock error', {
        taskId,
        lockAcquired,
        jobId: scheduleJob?.id,
        error: error instanceof Error ? error : new Error(String(error))
      });
    }

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
      logSystemError('MagicEdenSchedule', 'Timeout details', error);
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
  // Entry log

  // Check if task is blacklisted before processing
  if (isTaskBlacklisted(taskId)) {
    return;
  }

  try {
    if (!enabledMarketplaces.length) {
      return;
    }

    // Fetch task from activeTasks
    const currentTask = activeTasks.get(taskId);
    if (!currentTask || !currentTask?.running) {
      return;
    }

    if (!currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("magiceden")) {
      return;
    }

    if (!enabledMarketplaces.map((m) => m.toLowerCase()).includes("magiceden")) {
      return;
    }

    // Transition to RUNNING state when job starts processing
    // Job is starting to process - marketplace status will be updated when jobs are marked as processed

    const expiration = getExpiry(currentTask.bidDuration)


    // Parallel data fetching (wallet will be fetched when needed)
    const [floor_price, orders] = await Promise.all([
      fetchMagicEdenCollectionStats(currentTask.contract.contractAddress).then(Number),
      Order.find({
        slug: currentTask.contract.slug,
        taskId: currentTask._id,
        marketplace: MAGICEDEN.toLowerCase(),
        identifier: 'collection'
      }).sort({ createdAt: -1 })
    ]);

    // Early return for floor price
    if (!floor_price) {
      logError('magiceden', taskId, 'processMagicedenCollectionBid', 'Failed to fetch floor price', undefined, { slug: currentTask.contract.slug });
      throw new Error(`Failed to fetch floor price for ${currentTask.contract.slug}`);
    }


    if (!floorPrices[currentTask._id]) {
      floorPrices[currentTask._id] = {
        opensea: 0,
        magiceden: floor_price,
        blur: 0
      };
    }

    floorPrices[currentTask._id].magiceden = floor_price;

    const stopBid = await stopOption(currentTask, 'magiceden', floor_price, false)
    if (stopBid) {
      if (orders.length > 0) {
        const orderKeys = orders.map(order => order._id)
        const extractedOrderIds = await extractMagicedenOrderHash(orders)
        // Log cancellation enqueue
        queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, walletId: currentTask.walletId, orderKeys: orderKeys, taskId: currentTask._id }); // Non-blocking
      }
      return
    }

    const { offerPriceEth, maxBidPriceEth, minBidPriceEth, currencySymbol, currency, decimal } = await calculateBidPrice(currentTask, floor_price as number, "magiceden")


    // Fetch fresh balance and check if sufficient for minimum bid
    const { fetchERC20BalanceFresh } = await import('./utils/balance');
    const balance = await fetchERC20BalanceFresh(currentTask.wallet, currency);


    if (balance < minBidPriceEth) {
      const errorMsg = balance <= 0
        ? `Insufficient ${currencySymbol} balance: ${balance} (zero or negative) for task ${taskId}`
        : `Insufficient ${currencySymbol} balance: ${balance} < ${minBidPriceEth} for task ${taskId}`;


      // Increment skip stats
      if (!skipStats[currentTask._id]) {
        skipStats[currentTask._id] = { opensea: 0, magiceden: 0, blur: 0 };
      }
      skipStats[currentTask._id].magiceden++;

      // Log the insufficient balance
      await logBidError(taskId, "INSUFFICIENT_BALANCE", errorMsg, "skipped", "magiceden");

      // Create empty parent job and complete the marketplace loop
      await ensureParentJobExists(taskId, 'magiceden');
      const parentJobId = getParentJobId(taskId, 'magiceden');
      const parentJob = parentJobs.get(parentJobId);
      if (parentJob) {
        parentJob.expectedJobs = 0;
        parentJob.totalJobs = 0;
        parentJob.allJobsQueued = true;
        parentJob.completedJobs = 0;
      }

      // Trigger marketplace completion
      await checkTaskCompletionAndScheduleNext(taskId, 'magiceden');

      return;
    }
    const usdcBid = currency.toLowerCase() === USDC.toLowerCase()

    let offerPrice = Math.ceil(offerPriceEth * Math.pow(10, decimal))

    let colletionOffer = (Math.round(offerPrice))


    const [order] = orders
    const ttl = (order?.expiresAt?.getTime() - Date.now()) / 1000
    const currentPrice = Number(order ? order.offerPrice : offerPrice)

    const createdAt = new Date(order?.createdAt).getTime()
    const expiredAt = new Date(order?.expiresAt).getTime()
    const orderExpiration = order ? Math.round((expiredAt - createdAt) / 1000) : expiration

    if (!currentTask.outbidOptions.outbid) {
      if (ttl >= MIN_BID_DURATION && currentPrice === Number(offerPrice) && Math.abs(orderExpiration - expiration) < 30) {
        return;
      }
    }

    else {
      // Parallel offer fetching when outbidding
      const [[topOffer, secondOffer], walletsArr] = await Promise.all([
        fetchMagicEdenOffer("COLLECTION", currentTask.contract.contractAddress, "{}") as unknown as Promise<OpenseaOfferData[]>,
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
        const spread = (Number(highestBidAmount) - Number(secondTopBidAmount)) / Math.pow(10, decimal);
        const secondTopBidAmountEth = Number(secondTopBidAmount) / Math.pow(10, decimal);
        const potentialNewBidEth = secondTopBidAmountEth + outbidMargin;


        if (spread > outbidMargin && Number(secondTopBidAmount) > 0 && potentialNewBidEth >= minBidPriceEth) {
          colletionOffer = Number(secondTopBidAmount) + (outbidMargin * Math.pow(10, decimal))
        }
        else if (spread > outbidMargin && Number(secondTopBidAmount) > 0 && potentialNewBidEth < minBidPriceEth) {
        }
        else if (ttl > MIN_BID_DURATION) {
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
      skipStats[currentTask._id]['magiceden']++;
      const message = `⚠️ Offer price ${offerEth} ${currencySymbol} for ${currentTask.contract.slug} collection bid exceeds floor price ${floor_price} ${currencySymbol} FOR MAGICEDEN.`.toUpperCase();


      logBidError(currentTask._id, "OFFER EXCEEDS FLOOR PRICE", message, "warning", "magiceden"); // Non-blocking
      return;
    }

    const bidCount = getIncrementedBidCount(MAGICEDEN, currentTask.contract.slug, currentTask._id)

    if (orders.length > 0) {
      const orderKeys = orders.map(order => order._id)
      const extractedOrderIds = await extractMagicedenOrderHash(orders)
      queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, walletId: currentTask.walletId, orderKeys: orderKeys, taskId: currentTask._id }); // Non-blocking
    }

    const outbidOptions = currentTask.outbidOptions

    if (outbidOptions.outbid && offerEth >= maxBidPriceEth) {
      if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
        if (orders.length > 0) {
          const orderKeys = orders.map((order: any) => order._id)
          const extractedOrderIds = await extractMagicedenOrderHash(orders)
          // Log cancellation enqueue
          queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, walletId: currentTask.walletId, orderKeys, taskId: currentTask._id }); // Non-blocking
        }
      }
      const message = `❌ Offer price ${offerEth} ${currencySymbol} for ${currentTask.contract.slug} COLLECTION bid exceeds max bid price ${maxBidPriceEth} ${currencySymbol} FOR MAGICEDEN.Skipping ...`.toUpperCase()


      logBidError(currentTask._id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "magiceden"); // Non-blocking

      if (currentTask.outbidOptions.cancelMaxWhenOutbid && orders.length > 0) {
      }

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
      colletionOffer = minBidPriceEth * Math.pow(10, decimal);
    }

    try {
      // Fetch wallet and decrypt private key just before use
      const wallet = await walletCache.getWalletById(currentTask.walletId);
      const privateKey: string = decryptPrivateKey(wallet?.encryptedPrivateKey as string);


      await bidOnMagiceden(currentTask._id, bidCount, currentTask.wallet, currentTask.contract.contractAddress, 1, colletionOffer.toString(), privateKey, currentTask.contract.slug, expiration, currency);

    } catch (bidError) {
      throw bidError; // Re-throw to be caught by outer catch
    }

  } catch (error: any) {
    logBidError(taskId, "MAGICEDEN COLLECTION BID ERROR", `Error processing MagicEden collection bid for task: ${taskId}: ${error?.response?.data?.message || error?.message || String(error)}`, "error", "magiceden"); // Non-blocking
    // Throw the error
    throw error;
  }
}


async function processMagicedenTokenBid(data: IMagicedenTokenBidData) {
  try {
    const { contractAddress, address, quantity, walletId, slug, tokenId, _id } = data
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
      fetchMagicEdenCollectionStats(currentTask.contract.contractAddress).then(Number),
      Order.find({
        slug,
        taskId: _id,
        identifier: tokenId.toString(),
        marketplace: MAGICEDEN.toLowerCase()
      }).sort({ createdAt: -1 })
    ]);

    if (!floor_price) {
      throw new Error(`Failed to fetch floor price for ${slug}`);
    }

    if (!floorPrices[_id]) {
      floorPrices[_id] = {
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

    floorPrices[_id].magiceden = floor_price;

    const stopBid = await stopOption(currentTask, 'magiceden', Number(floor_price), false)
    if (stopBid) {
      if (orders.length > 0) {
        const orderKeys = orders.map((order: any) => order._id)
        const extractedOrderIds = await extractMagicedenOrderHash(orders)
        queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, walletId, orderKeys, taskId: _id });
      }
      return
    };

    const { offerPriceEth, maxBidPriceEth, minBidPriceEth, currencySymbol, currency, decimal } = await calculateBidPrice(currentTask, floor_price as number, "magiceden")

    // Fetch fresh balance and check if sufficient for minimum bid
    const { fetchERC20BalanceFresh } = await import('./utils/balance');
    const balance = await fetchERC20BalanceFresh(address, currency);

    if (balance < minBidPriceEth) {
      skipStats[_id]['magiceden']++;
      await logBidError(
        _id,
        "INSUFFICIENT_BALANCE",
        `Insufficient ${currencySymbol} balance: ${balance} < ${minBidPriceEth} for task ${_id} token ${tokenId}`,
        "skipped",
        "magiceden"
      );
      return;
    }

    const expiration = getExpiry(currentTask.bidDuration)
    const usdcBid = currency.toLowerCase() === USDC.toLowerCase()


    let tokenOffer = Number(offerPriceEth * Math.pow(10, decimal))

    const [order, ...remainingOrders] = orders
    const ttl = (order?.expiresAt?.getTime() - Date.now()) / 1000
    const currentPrice = Number(order ? order.offerPrice : tokenOffer)

    const createdAt = new Date(order?.createdAt).getTime()
    const expiredAt = new Date(order?.expiresAt).getTime()
    const orderExpiration = order ? Math.round((expiredAt - createdAt) / 1000) : expiration

    if (!outbidOptions.outbid) {
      if (ttl >= MIN_BID_DURATION && currentPrice === tokenOffer && Math.abs(orderExpiration - expiration) < 30) { return; }

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
        const spread = (Number(highestBidAmount) - Number(secondTopBidAmount)) / Math.pow(10, decimal);
        const secondTopBidAmountEth = Number(secondTopBidAmount) / Math.pow(10, decimal);
        const potentialNewBidEth = secondTopBidAmountEth + outbidMargin;


        if (spread > outbidMargin && Number(secondTopBidAmount) > 0 && potentialNewBidEth >= minBidPriceEth) {
          tokenOffer = Number(secondTopBidAmount) + (outbidMargin * Math.pow(10, decimal))
        }
        else if (spread > outbidMargin && Number(secondTopBidAmount) > 0 && potentialNewBidEth < minBidPriceEth) {
        }
        else if (ttl > MIN_BID_DURATION) {
          return
        } else {
          tokenOffer = highestBidAmount
        }
      }

      else if (isTopBid) {
        const spread = (Number(order.offerPrice) - Number(highestBidAmount)) / Math.pow(10, decimal);
        const highestBidAmountEth = Number(highestBidAmount) / Math.pow(10, decimal);
        const potentialNewBidEth = highestBidAmountEth + outbidMargin;


        if (spread > outbidMargin && Number(highestBidAmount) > 0 && potentialNewBidEth >= minBidPriceEth) {
          tokenOffer = Number(highestBidAmount) + (outbidMargin * Math.pow(10, decimal));
        }
        else if (spread > outbidMargin && Number(highestBidAmount) > 0 && potentialNewBidEth < minBidPriceEth) {
        }
        else if (ttl > MIN_BID_DURATION) {
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

    if (outbidOptions.outbid && offerEth >= maxBidPriceEth) {
      if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
        if (orders.length > 0) {
          const orderKeys = orders.map((order: any) => order._id)
          const extractedOrderIds = await extractMagicedenOrderHash(orders)
          queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, walletId, orderKeys, taskId: _id });
        }
      }
      const message = `❌ Offer price ${offerEth} ${currencySymbol} for ${slug} ${tokenId} bid exceeds max bid price ${maxBidPriceEth} ${currencySymbol} FOR MAGICEDEN.Skipping ...`.toUpperCase()
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
      queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, walletId, orderKeys, taskId: _id });
    }

    // Fetch wallet and decrypt private key just before use
    const wallet = await walletCache.getWalletById(walletId);
    const privateKey: string = decryptPrivateKey(wallet?.encryptedPrivateKey as string);

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
  const taskId = data._id;

  // Log entry

  // Early validation
  if (!enabledMarketplaces.length || !enabledMarketplaces.map((m) => m.toLowerCase()).includes("magiceden")) {
    return;
  }

  const activeTask = activeTasks.get(data._id);
  if (!activeTask?.running || taskStatus[data._id]?.marketplaces?.magiceden?.status === 'COMPLETED' || taskStatus[data._id]?.marketplaces?.magiceden?.status === 'STOPPED') {
    return;
  }

  try {

    const
      { contractAddress, address, quantity, walletId, slug, trait, _id } = data

    // Skip trait validation - preprocessing already determined valid traits
    // The trait passed in the job data is already validated

    const currentTask = activeTasks.get(_id)
    if (!currentTask || !currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("magiceden")) { return; }

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

    if (!floor_price) { throw new Error(`Failed to fetch floor price for ${slug}`); }

    const stopBid = await stopOption(currentTask, 'magiceden', Number(floor_price), false, traitFloor, identifier)
    if (stopBid) {
      if (orders.length > 0) {
        const orderKeys = orders.map((order: any) => order._id)
        const extractedOrderIds = await extractMagicedenOrderHash(orders)
        queue.add(CANCEL_MAGICEDEN_BID, {
          orderIds: extractedOrderIds,
          walletId,
          orderKeys,
          taskId: _id
        }); // Non-blocking
      }
      return
    };

    const { offerPriceEth, maxBidPriceEth, minBidPriceEth, currencySymbol, currency, decimal } = await calculateBidPrice(currentTask, floor_price as number, "magiceden")

    // Fetch fresh balance and check if sufficient for minimum bid
    const { fetchERC20BalanceFresh } = await import('./utils/balance');
    const balance = await fetchERC20BalanceFresh(address, currency);

    if (balance < minBidPriceEth) {
      skipStats[_id]['magiceden']++;
      await logBidError(
        _id,
        "INSUFFICIENT_BALANCE",
        `Insufficient ${currencySymbol} balance: ${balance} < ${minBidPriceEth} for task ${_id} trait ${identifier}`,
        "skipped",
        "magiceden"
      );
      return;
    }
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
        const spread = (Number(highestBidAmount) - Number(secondTopBidAmount)) / Math.pow(10, decimal);
        const secondTopBidAmountEth = Number(secondTopBidAmount) / Math.pow(10, decimal);
        const potentialNewBidEth = secondTopBidAmountEth + outbidMargin;


        if (spread > outbidMargin && Number(secondTopBidAmount) > 0 && potentialNewBidEth >= minBidPriceEth) {
          traitOffer = Number(secondTopBidAmount) + (outbidMargin * Math.pow(10, decimal));
        }
        else if (spread > outbidMargin && Number(secondTopBidAmount) > 0 && potentialNewBidEth < minBidPriceEth) {
        }
        else if (ttl > MIN_BID_DURATION) {
          return
        } else {
          traitOffer = highestBidAmount
        }
      }
      else if (isTopBid) {
        const spread = (Number(order.offerPrice) - Number(highestBidAmount)) / Math.pow(10, decimal);
        const highestBidAmountEth = Number(highestBidAmount) / Math.pow(10, decimal);
        const potentialNewBidEth = highestBidAmountEth + outbidMargin;


        if (spread > outbidMargin && Number(highestBidAmount) > 0 && potentialNewBidEth >= minBidPriceEth) {
          traitOffer = Number(highestBidAmount) + (outbidMargin * Math.pow(10, decimal));
        }
        else if (spread > outbidMargin && Number(highestBidAmount) > 0 && potentialNewBidEth < minBidPriceEth) {
        }
        else if (ttl > MIN_BID_DURATION) { return } else {
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
        walletId,
        orderKeys,
        taskId: _id
      }); // Non-blocking
    }
    if (outbidOptions.outbid && offerEth >= maxBidPriceEth) {
      if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
        if (orders.length > 0) {
          const orderKeys = orders.map((order: any) => order._id)
          const extractedOrderIds = await extractMagicedenOrderHash(orders)
          queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, walletId, orderKeys, taskId: _id });
        }
      }
      const message = `❌ Offer price ${offerEth} ${currencySymbol} for ${slug} ${JSON.stringify(trait)} bid exceeds max bid price ${maxBidPriceEth} ${currencySymbol} FOR MAGICEDEN.Skipping ...`.toUpperCase()
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
      // Fetch wallet and decrypt private key just before use
      const wallet = await walletCache.getWalletById(walletId);
      const privateKey: string = decryptPrivateKey(wallet?.encryptedPrivateKey as string);

      await bidOnMagiceden(_id, bidCount, address, contractAddress, quantity, traitOffer.toString(), privateKey, slug, expiration, currency, trait)
    } catch (bidError) {
      logSystemError('MagicEdenTraitBid', `MagicEden trait bid failed for task ${_id}`, bidError, { taskId: _id });
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

async function bulkCancelOpenseaBid(data: { walletId: string, orderId: string, taskId: string, orderKey: string }) {
  const { walletId, orderId, taskId, orderKey } = data
  const safeWalletId = maskSensitiveIdentifier(walletId);

  try {
    // Fetch wallet and decrypt private key
    const wallet = await walletCache.getWalletById(walletId);

    if (!wallet) {
      logSystemError('BulkCancelOpensea', `Wallet ${safeWalletId} not found for task ${taskId}`, undefined, { walletId: safeWalletId, taskId });
      throw new Error(`Wallet ${safeWalletId} not found`);
    }


    if (!wallet.encryptedPrivateKey) {
      logSystemError('BulkCancelOpensea', `No encryptedPrivateKey found for wallet ${safeWalletId}`, undefined, { walletId: safeWalletId, taskId });
      throw new Error(`No encryptedPrivateKey found for wallet ${safeWalletId}`);
    }

    const privateKey: string = decryptPrivateKey(wallet.encryptedPrivateKey);


    // First, cancel the order with OpenSea API (but don't update tracking yet)
    await cancelOrderWithoutTracking(orderId, OPENSEA_PROTOCOL_ADDRESS, privateKey, taskId);

    // Then, ensure database cleanup completes before tracking completion
    await Order.deleteMany({
      _id: orderKey
    });

    // Finally, record completion and update percentage - now that DB is clean
    cancellationTracker.recordCompletion(taskId, 1, 'opensea');
    await updateTaskCancellationPercentage(taskId, undefined, 'opensea');

    // Decrement ownBidCounts for the cancelled order
    decrementOwnBidCounter('opensea', 1);


    // Add marketplace file logging for successful cancellation
  } catch (error) {
    // Log detailed error information
    const errorMessage = error instanceof Error ? error.message : String(error);
    logSystemError('BulkCancelOpensea', `Failed to cancel OpenSea order ${orderId} for task ${taskId}`, error, { taskId, orderId, errorMessage });

    // Add marketplace file logging for error

    // Log additional error details if available
    if (error instanceof Error && 'response' in error) {
      const responseError = error as any;
      if (responseError.response?.data) {
        logSystemError('BulkCancelOpensea', `Response data for failed cancellation`, undefined, { taskId, responseData: responseError.response.data });
      }
    }

    throw error; // Re-throw to maintain existing behavior
  }
}

async function bulkCancelMagicedenBid(data: { orderIds: string[], walletId: string, orderKeys: string[], taskId: string, batchSize?: number }) {
  const { orderIds, walletId, orderKeys, taskId, batchSize } = data
  const safeWalletId = maskSensitiveIdentifier(walletId);


  // Add marketplace file logging

  try {
    if (orderIds.length > 0) {
      // Fetch wallet and decrypt private key
      const wallet = await walletCache.getWalletById(walletId);

      if (!wallet) {
        logSystemError('BulkCancelMagiceden', `Wallet ${safeWalletId} not found for task ${taskId}`, undefined, { walletId: safeWalletId, taskId });
        throw new Error(`Wallet ${safeWalletId} not found`);
      }


      if (!wallet.encryptedPrivateKey) {
        logSystemError('BulkCancelMagiceden', `No encryptedPrivateKey found for wallet ${safeWalletId}`, undefined, { walletId: safeWalletId, taskId });
        throw new Error(`No encryptedPrivateKey found for wallet ${safeWalletId}`);
      }

      const privateKey: string = decryptPrivateKey(wallet.encryptedPrivateKey);


      await Promise.all([
        cancelMagicEdenBid(orderIds, privateKey, taskId),
        Order.deleteMany({
          _id: { $in: orderKeys }
        })
      ])


      // Record completion for the batch
      const completedCount = batchSize || orderIds.length;
      cancellationTracker.recordCompletion(taskId, completedCount, 'magiceden');
      await updateTaskCancellationPercentage(taskId, undefined, 'magiceden');

      // Decrement ownBidCounts for the cancelled orders
      decrementOwnBidCounter('magiceden', completedCount);

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
    logSystemError('BulkCancelMagiceden', `Error processing Magiceden bid cancellation for task ${taskId}`, error, { taskId });

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

    // Delete expired orders and update ownBidCounts
    if (expiredOrders.length > 0) {
      // First, count expired orders by marketplace for ownBidCounts
      const expiredByMarketplace = await Order.aggregate([
        {
          $match: {
            _id: { $in: expiredOrders.map(o => o._id) },
            marketplace: { $in: ['opensea', 'magiceden'] } // Only track these two
          }
        },
        {
          $group: {
            _id: '$marketplace',
            count: { $sum: 1 }
          }
        }
      ]);

      // Decrement ownBidCounts for expired orders
      for (const group of expiredByMarketplace) {
        const marketplace = group._id.toLowerCase();
        if (marketplace === 'opensea' || marketplace === 'magiceden') {
          decrementOwnBidCounter(marketplace as 'opensea' | 'magiceden', group.count);
        }
      }

      // Now delete the expired orders
      await Order.deleteMany({
        _id: { $in: expiredOrders.map(o => o._id) }
      });
    }
  } catch (error: any) {
    // Handle database errors
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
  const { orderKey, walletId, taskId, payload } = data
  const safeWalletId = maskSensitiveIdentifier(walletId);


  // Add marketplace file logging

  try {
    if (!data) return

    // Fetch wallet and decrypt private key
    const wallet = await walletCache.getWalletById(walletId);

    if (!wallet) {
      logSystemError('BulkCancelBlur', `Wallet ${safeWalletId} not found for task ${taskId}`, undefined, { walletId: safeWalletId, taskId });
      throw new Error(`Wallet ${safeWalletId} not found`);
    }

    if (!wallet.encryptedPrivateKey) {
      logSystemError('BulkCancelBlur', `No encryptedPrivateKey found for wallet ${safeWalletId}`, undefined, { walletId: safeWalletId, taskId });
      throw new Error(`No encryptedPrivateKey found for wallet ${safeWalletId}`);
    }

    const privateKey: string = decryptPrivateKey(wallet.encryptedPrivateKey);


    await Promise.all([
      Order.deleteMany({
        _id: orderKey
      }),
      cancelBlurBid({ payload, privateKey, taskId })
    ])


    // Record completion
    cancellationTracker.recordCompletion(taskId, 1, 'blur');
    await updateTaskCancellationPercentage(taskId, undefined, 'blur');

    // Add marketplace file logging for successful cancellation

    // Cancellation tracking is now handled in cancelBlurBid directly
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    logSystemError('BulkCancelBlur', `Error processing Blur bid cancellation for task ${taskId}`, error, { taskId });

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
    }

    try {
      // Get blur rate limits with timeout
      blurRateLimits = await Promise.race([
        Promise.resolve(blurRateLimiter.getStatus()),
        timeoutPromise
      ]) as any;
    } catch (error) {
    }

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
        blurRateLimits: blurRateLimits
      }
    };
    res.json(metrics);
  } catch (error: any) {
    logSystemError('HealthCheck', 'Health check error', error);
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
    // Get queue metrics
    const [mainQueueCounts, blurQueueCounts] = await Promise.all([
      queue.getJobCounts(),
      blurQueue.getJobCounts()
    ]);

    // Get worker health status
    const workerStatuses = await Promise.all(
      workers.map(async (worker, index) => {
        try {
          return {
            index,
            running: worker.isRunning(),
            paused: worker.isPaused(),
            healthy: worker.isRunning() && !worker.isPaused()
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

    // Get Redis connection pool stats
    const connectionPoolStats = getBullMQConnections();
    const connectionHealth = await checkBullMQConnectionsHealth();

    // Calculate processing rates
    const currentTime = Date.now();
    const processingHistory = Array.from(queueProcessingHistory.entries())
      .sort((a, b) => b[0] - a[0])
      .slice(0, 10); // Last 10 entries

    let processingRate = 0;
    if (processingHistory.length >= 2) {
      const timeDiff = (processingHistory[0][0] - processingHistory[processingHistory.length - 1][0]) / 1000; // seconds
      const processedDiff = processingHistory[0][1].processed - processingHistory[processingHistory.length - 1][1].processed;
      processingRate = processedDiff / timeDiff;
    }

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
          name: QUEUE_NAME
        },
        blur: {
          ...blurQueueCounts,
          name: BLUR_BIDDING_QUEUE
        },
        totals: {
          waiting: (mainQueueCounts.waiting || 0) + (blurQueueCounts.waiting || 0),
          active: (mainQueueCounts.active || 0) + (blurQueueCounts.active || 0),
          completed: (mainQueueCounts.completed || 0) + (blurQueueCounts.completed || 0),
          failed: (mainQueueCounts.failed || 0) + (blurQueueCounts.failed || 0)
        },
        processingRate: processingRate.toFixed(2) + ' jobs/sec'
      },
      workers: {
        total: workers.length,
        healthy: workerStatuses.filter(w => w.healthy).length,
        unhealthy: workerStatuses.filter(w => !w.healthy).length,
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
      }
    };

    res.json(metrics);
  } catch (error: any) {
    logSystemError('MetricsEndpoint', 'Metrics endpoint error', error);
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
      contractAddress || "",
      rarityCategory as "LEGENDARY" | "EPIC" | "RARE"
    );

    res.status(200).json(result);
  } catch (error: any) {
    const err = error?.response?.data || error?.message;
    await logger.logApiError('Failed to fetch rarity tokens', {
      endpoint: '/api/rarity',
      method: 'POST',
      collectionSlug: req.body?.collectionSlug,
      rarityCategory: req.body?.rarityCategory,
      error: error,
      errorMessage: err,
      statusCode: error?.response?.status || 500
    });
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
    logSystemError('API', 'Error fetching task', error);
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
    logSystemError('API', 'Error fetching task status history', error);
    res.status(500).json({ error: 'Failed to fetch task status history' });
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
    const orders: any[] = req.body
    const openseaOrders = orders.filter((order: any) => order.marketplace === OPENSEA.toLowerCase())
    const magicedenOrders = orders.filter((order: any) => order.marketplace === 'magiceden')
    const blurOrders = orders.filter((order: any) => order.marketplace === 'blur')

    if (openseaOrders.length > 0) {
      const cancelData = openseaOrders.map((order) => {
        return {
          name: CANCEL_OPENSEA_BID,
          data: { walletId: task.walletId, orderId: order.orderHash, taskId: task._id, orderKey: order._id }
        }
      })
      await queue.addBulk(cancelData);
    }

    if (magicedenOrders.length > 0) {
      const extractedOrderIds = await extractMagicedenOrderHash(magicedenOrders)
      const orderKeys = magicedenOrders.map((order) => order._id)
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
          data: { walletId: task.walletId, orderId: order.orderHash, taskId: task._id, orderKey: order._id }
        }
      })
      await queue.addBulk(blurCancelData)
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
        const [topOffer] = (await fetchOpenseaOffers('COLLECTION', slug, {}) as unknown) as OpenseaOfferData[]
        const topOfferAmount = Number(topOffer?.eth) || 0
        bestOffers[taskId]['opensea'] = topOfferAmount
      } catch (error: any) {
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
        floorPrices[taskId]['blur'] = 0;
        bestOffers[taskId]['blur'] = 0;
      }

      // Magic Eden
      try {
        const magicEdenFloorPrice = await fetchMagicEdenCollectionStats(contractAddress);
        floorPrices[taskId]['magiceden'] = Number(magicEdenFloorPrice);

        const [bestOffer] = await fetchMagicEdenOffer("COLLECTION", contractAddress, "{}") as unknown as OpenseaOfferData[]
        const bestOfferAmount = Number(bestOffer?.eth) || 0
        bestOffers[taskId]['magiceden'] = bestOfferAmount;
      } catch (error: any) {
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
          logSystemError('CollectionStats', 'Failed to fetch Blur collection stats', error);
          return 0;
        }
      },
      async () => {
        try {
          return await fetchMagicEdenCollectionStats(contractAddress);
        } catch (error) {
          logSystemError('CollectionStats', 'Failed to fetch MagicEden collection stats', error);
          return 0;
        }
      },
      async () => {
        try {
          return await getOpenseaFloor(slug);
        } catch (error) {
          logSystemError('CollectionStats', 'Failed to fetch OpenSea floor', error);
          return 0;
        }
      },
      async () => {
        try {
          return await fetchBlurTraits(contractAddress);
        } catch (error) {
          logSystemError('CollectionStats', 'Failed to fetch Blur traits', error);
          return { traits: { categories: {}, counts: {} }, blurRaw: null };
        }
      },
      async () => {
        try {
          return await fetchMagicEdenAttributes(contractAddress);
        } catch (error) {
          logSystemError('CollectionStats', 'Failed to fetch MagicEden attributes', error);
          return { traits: { categories: {}, counts: {} }, magicedenRaw: null };
        }
      },
      async () => {
        try {
          return await fetchOpenseaTraits(slug);
        } catch (error) {
          logSystemError('CollectionStats', 'Failed to fetch MagicEden attributes', error);
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
    logSystemError('API', 'Unexpected error in collection stats endpoint', error);
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
    }

    res.status(200).json({ success: true, message: 'Password stored successfully' });
  } catch (error: any) {
    logSystemError('API', 'Failed to store password', error);
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
    logSystemError('API', 'Failed to renew password TTL', error);
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
        logSystemError('WalletEncryption', `Failed to re-encrypt wallet ${wallet.address}`, error, { walletAddress: wallet.address });
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
      logSystemError('WalletEncryption', `Failed to re-encrypt ${failedWallets.length} wallets`, undefined, { failedCount: failedWallets.length });
      res.status(500).json({
        error: 'Failed to re-encrypt some wallets',
        failedWallets,
        successCount
      });
    }

  } catch (error: any) {
    logSystemError('API', 'Failed to change password', error);
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
    logSystemError('API', 'Failed to recover password', error);
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
    logSystemError('API', 'Failed to get Blur rate limits', error);
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
    logSystemError('API', 'Failed to get Blur rate limit for wallet', error);
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
    logSystemError('API', 'Failed to reset Blur rate limit', error);
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
    const keys = await redis.keys('blurAccessToken:*');
    let clearedCount = 0;

    if (keys.length > 0) {
      // Delete all tokens
      await redis.del(...keys);
      clearedCount = keys.length;
    }


    res.status(200).json({
      success: true,
      message: `Cleared ${clearedCount} Blur authentication tokens`,
      clearedTokens: clearedCount
    });
  } catch (error: any) {
    logSystemError('API', 'Failed to clear Blur auth tokens', error);
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
    logSystemError('API', 'Failed to check Blur health', error);
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

    const { data } = await axios.post(`http://host.docker.internal:9999/update`,
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
    logSystemError('API', 'Failed to schedule update', error);
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
    logSystemError('API', 'Failed to fetch wallet NFTs', error);
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
            logSystemError('GasEstimation', `Gas estimation error for ${marketplaceName}`, gasError, { marketplace: marketplaceName });
            gasEstimate = null;
          }
        }

        return {
          approved: isApproved,
          approvalFeeEstimate: gasEstimate,
          contractAddress: marketplaceAddress
        };
      } catch (error: any) {
        logSystemError('ApprovalCheck', `Approval check error for ${marketplaceName}`, error, { marketplace: marketplaceName });
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
    logSystemError('API', 'Failed to fetch NFT metadata', error);
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
    logSystemError('API', 'Error getting queue monitor status', error);
    res.status(500).json({ error: 'Failed to get queue monitor status' });
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
    logSystemError('API', 'Error during manual queue clear', error);
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
    logSystemError('API', 'Error fetching timeout metrics', error);
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
    logSystemError('API', 'Error fetching blur rate limiter status', error);
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
    logSystemError('API', 'Error fetching blur completion stats', error);
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
    logSystemError('TaskDiagnostics', 'Error in task diagnostics', error);
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
    logSystemError('TaskCompletion', 'Error forcing task completion', error);
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
    logSystemError('PercentageCalculation', 'Error recalculating percentage', error);
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
    logSystemError('TaskStatus', 'Error getting all task statuses', error);
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

// Generic marketplace logs export (blur, opensea, magiceden)
app.get('/api/logs/export', async (req, res) => {
  try {
    const {
      marketplace = 'blur',
      taskId,
      timeRange,
      startTime,
      endTime,
      level = 'all',
      function: functionFilter,
      format = 'json'
    } = req.query as any;

    // Validate marketplace
    const validMarketplaces = ['blur', 'opensea', 'magiceden'];
    if (!validMarketplaces.includes(marketplace)) {
      return res.status(400).json({
        error: 'Invalid marketplace',
        message: `Marketplace must be one of: ${validMarketplaces.join(', ')}`
      });
    }

    // Validate format
    const validFormats = ['json', 'csv'];
    if (!validFormats.includes(format)) {
      return res.status(400).json({
        error: 'Invalid format',
        message: `Format must be one of: ${validFormats.join(', ')}`
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

    const logger = getMarketplaceLogger(marketplace);

    const options: any = {
      format: format as 'json' | 'csv',
      level: level as any
    };
    if (taskId) options.taskId = taskId;
    if (timeRange) options.timeRange = timeRange;
    if (functionFilter) options.function = functionFilter;
    if (parsedStartTime) options.startTime = parsedStartTime;
    if (parsedEndTime) options.endTime = parsedEndTime;

    const buffer = await logger.exportLogs(options);
    const filename = `${marketplace}-logs-${new Date().toISOString().split('T')[0]}.${format}`;
    res.setHeader('Content-Type', format === 'json' ? 'application/json' : 'text/csv');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.setHeader('Content-Length', String(buffer.length));
    res.send(buffer);
  } catch (error) {
    logSystemError('MarketplaceLogs', 'Error exporting marketplace logs', error);
    const errorMessage = error instanceof Error ? error.message : 'Unknown error';
    res.status(500).json({
      error: 'Failed to export logs',
      message: errorMessage,
      details: process.env.NODE_ENV === 'development' ? error : undefined
    });
  }
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

    const logger = getMarketplaceLogger(marketplace);

    const options: any = { level };
    if (taskId) options.taskId = taskId;
    if (timeRange) options.timeRange = timeRange;
    if (functionFilter) options.function = functionFilter;
    if (parsedStartTime) options.startTime = parsedStartTime;
    if (parsedEndTime) options.endTime = parsedEndTime;

    const allLogs = await logger.getLogs(options);
    const start = parsedOffset;
    const end = start + parsedLimit;
    const logs = allLogs.slice(start, end);

    const summary = {
      total: allLogs.length,
      byLevel: {
        info: allLogs.filter(l => l.level === 'info').length,
        warn: allLogs.filter(l => l.level === 'warn').length,
        error: allLogs.filter(l => l.level === 'error').length,
        debug: allLogs.filter(l => l.level === 'debug').length,
      },
      byFunction: {} as Record<string, number>,
      byStage: {} as Record<string, number>,
    };
    allLogs.forEach(l => {
      summary.byFunction[l.function] = (summary.byFunction[l.function] || 0) + 1;
      summary.byStage[l.stage] = (summary.byStage[l.stage] || 0) + 1;
    });

    res.json({
      logs,
      summary,
      pagination: {
        total: allLogs.length,
        limit: parsedLimit,
        offset: parsedOffset,
        hasMore: end < allLogs.length,
      },
    });
  } catch (error) {
    logSystemError('MarketplaceLogs', 'Error fetching marketplace logs', error);
    const errorMessage = error instanceof Error ? error.message : 'Unknown error';
    res.status(500).json({
      error: 'Failed to fetch logs',
      message: errorMessage,
      details: process.env.NODE_ENV === 'development' ? error : undefined
    });
  }
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
      logSystemError('AddressBalances', 'Error parsing cached address balances', error);
    }
  }

  // Use distributed lock to prevent duplicate requests
  const lockManager = new DistributedLockManager({
    lockPrefix: 'address_balances:lock:',
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
          logSystemError('AddressBalances', 'Error parsing cached address balances after lock', error);
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
        logSystemError('AddressBalances', 'Error fetching address balances', error);
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

        await logBidError(task._id, "TRAIT FLOOR OUT OF RANGE", message, 'skipped', marketplace)

        if (!counterBid) {
          skipStats[task._id][marketplace]++
        }
        return true
      }
    } else {
      // Check if floor price is outside allowed range
      if ((minFloorPrice > 0 && floorPrice < minFloorPrice) || (maxFloorPrice > 0 && floorPrice > maxFloorPrice)) {

        if (!counterBid) {
          skipStats[task._id][marketplace]++;
        }
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
      return true;
    }

    // Clear warnings if all checks pass AND task is still running
    const taskStatus = activeTasks.get(task._id);
    if (taskStatus?.running) {
      warningBids[task._id][marketplace] = false;
      warningMessage[task._id][marketplace] = '';
    } else {
      // Task is stopped, preserve warnings
    }
    return false;

  } catch (error) {
    logSystemError('StopOption', `Error in stopOption for ${task?.contract?.slug || 'unknown'} on ${marketplace}`, error, { marketplace, contractSlug: task?.contract?.slug });
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

    // Always subscribe all tasks to WebSocket
    if (!wsManager?.isConnected()) {
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
        logSystemError('WebSocketSubscription', `Error subscribing to collection ${task.contract.slug}`, error, { contractSlug: task.contract.slug });
      }
    });

  } catch (error) {
    logSystemError('WebSocketSubscription', 'Error subscribing to collections', error);
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
    logSystemError('ListingSubscription', `Error subscribing to listing events for ${contractAddress}`, error, { contractAddress });
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
        logSystemError('ListingResubscription', `Error re-subscribing to listing ${listing._id}`, error, { listingId: listing._id });
      }
    }


    // Check if any listings have been outbid while disconnected
    await processAllActiveListings();
  } catch (error) {
    logSystemError('ListingResubscription', 'Error in resubscribeToListingCollections', error);
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
        logSystemError('ListingProcessing', `Error processing listing ${listing._id}`, error, { listingId: listing._id });
      }
    }

  } catch (error) {
    logSystemError('ListingProcessing', 'Error in processAllActiveListings', error);
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
  walletId: string;
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
        logSystemError('MarketplaceApproval', `Approval failed for ${marketplace}`, error, {
          marketplace,
          errorMessage: error.message,
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
    logSystemError('MarketplaceApproval', 'Fatal error in approveMarketplace', error, {
      errorMessage: error.message,
      code: error.code,
      reason: error.reason,
      details: error.error || 'No additional details'
    });
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
    logSystemError('BidLogging', 'Error logging bid error', error);
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
  // The main broadcastBidRates() function handles all WebSocket updates
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
  walletId: string;
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
}

interface IMagicedenTokenBidData {
  _id: string;
  address: string;
  contractAddress: string;
  quantity: number;
  offerPrice?: number;
  expiration: number;
  walletId: string;
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
export function incrementOwnBidCounter(marketplace: 'opensea' | 'magiceden' | 'blur', orderHash: string) {
  try {
    // Check if we've already processed this bid
    if (processedOwnBids.has(orderHash)) {
      return ownBidCounts[marketplace] || 0;
    }

    // Mark this bid as processed
    processedOwnBids.add(orderHash);

    if (!ownBidCounts[marketplace]) {
      ownBidCounts[marketplace] = 0;
    }
    ownBidCounts[marketplace]++;

    // Clean up old entries periodically (keep last 10000 entries)
    if (processedOwnBids.size > 10000) {
      const entriesToKeep = Array.from(processedOwnBids).slice(-5000);
      processedOwnBids.clear();
      entriesToKeep.forEach(entry => processedOwnBids.add(entry));
    }

    return ownBidCounts[marketplace];
  } catch (error) {
    logSystemError('BidCounter', 'Failed to increment own bid counter', error);
  }
  return 0;
}

/**
 * Decrements memory-based counter for our own bids when we cancel a bid
 */
export function decrementOwnBidCounter(marketplace: 'opensea' | 'magiceden' | 'blur', count: number = 1, orderHash?: string) {
  try {
    // If order hash provided, remove it from processed set
    if (orderHash) {
      processedOwnBids.delete(orderHash);
    }

    // Update local ownBidCounts object for immediate broadcast
    if (ownBidCounts[marketplace] && ownBidCounts[marketplace] > 0) {
      ownBidCounts[marketplace] = Math.max(0, ownBidCounts[marketplace] - count);
      return ownBidCounts[marketplace];
    }
    return 0;
  } catch (error) {
    logSystemError('BidCounter', 'Failed to decrement own bid counter', error);
  }
  return 0;
}


/**
 * Gets Redis-based counter for our own bids
 */
export async function getOwnBidCounter(marketplace: 'opensea' | 'magiceden' | 'blur', taskId: string) {
  try {
    if (redis) {
      const key = `own_bid_count:${marketplace}:${taskId}`;
      const count = await redis.get(key);
      return parseInt(count || '0');
    }
  } catch (error) {
    logSystemError('BidCounter', 'Failed to get own bid counter', error);
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

  } catch (error) {
    logSystemError('BidCounter', 'Failed to reset own bid counter', error);
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
  walletId: string;
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
  walletId: string;
  slug: string;
  currency?: string;
}
interface IBlurBidData {
  taskId: string;
  bidCount: string;
  address: string;
  walletId: string;
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
  walletId: string;
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
    logSystemError('MagicEdenTraits', 'Error transforming Magic Eden traits', error);
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
        logSystemError('ListingWebSocket', 'WebSocket subscription failed', error);
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
    logSystemError('ListingStart', 'Error starting listing', error);
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
        logSystemError('ListingWebSocket', 'WebSocket unsubscription failed for cancelled listing', error);
      }
    }

    ws.send(JSON.stringify({
      type: 'listing-stopped',
      listingId: data.listingId,
      message: 'Listing stopped successfully'
    }));

  } catch (error) {
    logSystemError('ListingStop', 'Error stopping listing', error);
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
          logSystemError('ListingWebSocket', 'WebSocket subscription failed', error);
        }
      } else {

        try {
          // Unsubscribe from listing events
          await wsManager.unsubscribe(listing.contract.slug, listing.contract.contractAddress, 'ALL');
        } catch (error) {
          logSystemError('ListingWebSocket', 'WebSocket unsubscription failed', error);
        }
      }
    }

    ws.send(JSON.stringify({
      type: 'listing-updated',
      listingId: data._id,
      message: 'Listing updated successfully'
    }));

  } catch (error) {
    logSystemError('ListingUpdate', 'Error updating listing', error);
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
          const blurCounts = await blurQueue.getJobCounts();
          const totalQueueJobs = (queueCounts.waiting || 0) + (queueCounts.active || 0) +
            (blurCounts.waiting || 0) + (blurCounts.active || 0);

          if (overallRunningPercentage < 10 && totalQueueJobs === 0 && totalTracked > 0) {

            // Force mark all parent jobs as complete to unstick the task
            for (const mp of marketplaces) {
              const parentJobId = `${taskId}-${mp}`;
              const parentJob = parentJobs.get(parentJobId);
              if (parentJob && parentJob.completedJobs < parentJob.totalJobs) {
                parentJob.completedJobs = parentJob.totalJobs;
              }
            }

            // Trigger task completion check
            await checkTaskCompletionAndScheduleNext(taskId);
          }
        }
      }
    }

  } catch (error) {
    logSystemError('JobTracking', 'Error logging job tracking status', error);
  }
}

/**
 * Recovers tasks with partial queueing mismatches
 * This function detects when expectedJobs > actualJobs and triggers task restart
 */
async function recoverPartialQueueingTasks(): Promise<void> {
  try {

    for (const [taskId, status] of Object.entries(taskStatus)) {
      // Check if any marketplace is in RUNNING, QUEUED, or QUEUEING state
      const hasActiveMarketplace = status.marketplaces && Object.values(status.marketplaces).some(
        mpStatus => mpStatus && (mpStatus.status === 'RUNNING' || mpStatus.status === 'QUEUED' || mpStatus.status === 'QUEUEING')
      );

      if (hasActiveMarketplace) {
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

        // Calculate total jobs across marketplaces
        let totalJobs = 0;
        for (const mp of marketplaces) {
          const mpStatus = status.marketplaces?.[mp as 'opensea' | 'magiceden' | 'blur'];
          if (mpStatus) {
            totalJobs += mpStatus.totalJobs || 0;
          }
        }

        if (hasPartialQueueing && totalJobs === 0) {

          // Check if restart is already scheduled
          const restartKey = `recovery-restart:${taskId}`;
          if (!tasksBeingRestarted.has(restartKey)) {
            tasksBeingRestarted.add(restartKey);


            try {
              await triggerTaskRestart(taskId);
            } catch (error) {
              logSystemError('TaskRecovery', `Failed to restart task ${taskId}`, error, { taskId });
            } finally {
              // Clear the flag after 30 seconds to allow future restarts if needed
              setTimeout(() => {
                tasksBeingRestarted.delete(restartKey);
              }, 30000);
            }
          } else {
          }
        }
      }
    }

  } catch (error) {
    logSystemError('PartialQueueingRecovery', 'Error recovering partial queueing tasks', error);
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
      if (!status) continue;

      // Check if any marketplace is in RUNNING, QUEUED, or QUEUEING state
      const hasActiveMarketplace = Object.values(status.marketplaces || {}).some(
        mp => mp && (mp.status === 'RUNNING' || mp.status === 'QUEUED' || mp.status === 'QUEUEING')
      );
      if (!hasActiveMarketplace) continue;

      // Check for stuck QUEUEING status in any marketplace
      const queuingMarketplaces = Object.entries(status.marketplaces || {}).filter(
        ([_, mp]) => mp?.status === 'QUEUEING'
      );

      if (queuingMarketplaces.length > 0) {
        const firstSeenQueueing = taskCompletionTracking.get(`${taskId}-queueing`);

        if (!firstSeenQueueing) {
          // First time seeing this task in QUEUEING
          taskCompletionTracking.set(`${taskId}-queueing`, now);
        } else {
          // Check if stuck in QUEUEING for too long
          const stuckDuration = now - firstSeenQueueing;

          if (stuckDuration > 600000) { // 10 minutes in QUEUEING

            // Check parent jobs
            const marketplaces = ['opensea', 'magiceden', 'blur'];
            let hasEmptyParentJobs = false;

            for (const mp of marketplaces) {
              const parentJobId = getParentJobId(taskId, mp);
              const parentJob = parentJobs.get(parentJobId);
              if (parentJob && parentJob.totalJobs === 0) {
                hasEmptyParentJobs = true;
              }
            }

            if (hasEmptyParentJobs) {
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

      // Check for 99% stuck condition in any marketplace
      const marketplacesAt99 = Object.entries(status.marketplaces || {}).filter(
        ([_, mp]) => mp && mp.runningPercentage >= 99 && mp.runningPercentage < 100
      );

      if (marketplacesAt99.length > 0) {
        const firstSeen99 = taskCompletionTracking.get(`${taskId}-99`);

        if (!firstSeen99) {
          // First time seeing this task at 99%
          taskCompletionTracking.set(`${taskId}-99`, now);
          // Calculate overall percentage from marketplaces at 99%
          const overallPercentage = marketplacesAt99.length > 0 ? marketplacesAt99[0][1].runningPercentage : 99;

          // Log detailed parent job status
          const marketplaces = ['opensea', 'magiceden', 'blur'];
          for (const mp of marketplaces) {
            const parentJobId = getParentJobId(taskId, mp);
            const parentJob = parentJobs.get(parentJobId);
            if (parentJob) {
            }
          }
        } else {
          // Check if stuck at 99% for too long
          const stuckDuration = now - firstSeen99;

          if (stuckDuration > 120000) { // 2 minutes at 99%
            // Calculate overall percentage from marketplaces at 99%
            const stuckPercentage = marketplacesAt99.length > 0 ? marketplacesAt99[0][1].runningPercentage : 99;

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
              }
            }

            const actualPercentage = totalExpected > 0 ? (totalCompleted / totalExpected) * 100 : 0;

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

              // Force all parent jobs to be marked as complete
              for (const mp of marketplaces) {
                const parentJobId = getParentJobId(taskId, mp);
                const parentJob = parentJobs.get(parentJobId);
                if (parentJob && parentJob.completedJobs < parentJob.totalJobs) {
                  parentJob.completedJobs = parentJob.totalJobs;
                }
              }

              // Force percentage to 100 for all marketplaces
              for (const mp of marketplaces) {
                const mpKey = mp as 'opensea' | 'magiceden' | 'blur';
                if (status.marketplaces[mpKey]) {
                  status.marketplaces[mpKey]!.runningPercentage = 100;
                }
              }

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
        } else {
          // Check if stuck for too long
          const stuckDuration = now - firstSeen;

          if (stuckDuration > 30000) { // 30 seconds

            // Force percentage to 100 for all marketplaces
            if (status) {
              const marketplaces = ['opensea', 'magiceden', 'blur'] as const;
              for (const mp of marketplaces) {
                if (status.marketplaces[mp]) {
                  status.marketplaces[mp]!.runningPercentage = 100;
                }
              }

              // Log the force completion
              for (const mp of marketplaces) {
                const parentJobId = getParentJobId(taskId, mp);
                const parentJob = parentJobs.get(parentJobId);
                if (parentJob) {
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
    logSystemError('StuckTasksCheck', 'Error checking stuck tasks', error);
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
      // Only check tasks with RUNNING marketplaces
      const hasRunningMarketplace = Object.values(status.marketplaces || {}).some(
        mp => mp?.status === 'RUNNING'
      );
      if (!hasRunningMarketplace) continue;

      const task = activeTasks.get(taskId);
      if (!task || !task.running) continue;

      // Check if any marketplace has been at 0% for too long
      const stuckMarketplaces = Object.entries(status.marketplaces || {}).filter(([mp, mpStatus]) => {
        if (!mpStatus || mpStatus.status !== 'RUNNING') return false;
        const lastUpdate = mpStatus.lastUpdate || status.lastUpdate || now;
        const stuckDuration = now - lastUpdate;
        return mpStatus.runningPercentage === 0 && stuckDuration > 60000; // 1 minute at 0%
      });

      if (stuckMarketplaces.length > 0) {
        const firstStuckMarketplace = stuckMarketplaces[0];
        const lastUpdate = firstStuckMarketplace[1].lastUpdate || status.lastUpdate || now;
        const stuckDuration = now - lastUpdate;

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

            // Check if parent job has expected jobs but no actual jobs
            if (parentJob.expectedJobs && parentJob.expectedJobs > 0 && (!parentJob.actualJobs || parentJob.actualJobs === 0)) {
              needsRecovery = true;
            }

            if (parentJob.actualJobs && parentJob.actualJobs > 0) {
              hasActualJobs = true;
            }
          }
        }

        // If we have parent jobs but no actual jobs, trigger recovery
        if (hasParentJobs && !hasActualJobs && needsRecovery) {

          // Log to error tracking
          await logger.logError(ErrorCategory.QUEUE, 'Task stuck at 0% with parent jobs but no actual jobs', {
            taskId,
            stuckDuration,
            hasParentJobs,
            hasActualJobs,
            runningPercentage: stuckMarketplaces.map(([mp, mpStatus]) => ({ [mp]: mpStatus?.runningPercentage || 0 }))
          });

          // Attempt recovery
          await recoverStuckTask(taskId);
        }
      }
    }
  } catch (error) {
    logSystemError('StuckTasksHealthCheck', 'Error during health check of stuck tasks', error);
  }
}

/**
 * Recover a stuck task by attempting to create missing jobs
 */
async function recoverStuckTask(taskId: string): Promise<void> {
  const task = activeTasks.get(taskId);
  if (!task) {
    return;
  }


  let jobsCreated = false;

  // Try to create missing jobs for each marketplace
  for (const marketplaceName of task.selectedMarketplaces) {
    const marketplace = marketplaceName.toLowerCase();
    const parentJobId = getParentJobId(taskId, marketplace);
    const parentJob = parentJobs.get(parentJobId);

    if (parentJob && parentJob.expectedJobs && parentJob.expectedJobs > 0 && (!parentJob.actualJobs || parentJob.actualJobs === 0)) {

      try {
        // Set task to QUEUEING to reflect that we're creating jobs
        setTaskStatus(taskId, 'QUEUEING', 'recovering stuck task - creating missing jobs', marketplace);

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
              { jobId: meJobId, attempts: 1 }
            );

            jobsCreated = true;
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
              { jobId: blurJobId, attempts: 1 }
            );

            jobsCreated = true;
            break;
        }

      } catch (error) {
        logSystemError('TaskRecovery', `Failed to create jobs for marketplace ${marketplace}`, error, { marketplace });

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
    // Recovery complete - jobs created, will transition to RUNNING when processed

    // Force update task percentage after recovery
    await updateTaskRunningPercentage(taskId);

  } else {

    // If no jobs could be created, might need to restart the task
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
      jobCreationVerificationMap.delete(key);
      continue;
    }

    // Check if jobs are actually being created
    if (parentJob.actualJobs === 0 && expectedJobs > 0 && now - timestamp > 10000) {
      logSystemError('JobVerification', `Task ${taskId} loop ${loopCount}: No jobs created after 10s (expected: ${expectedJobs})`, undefined, { taskId, loopCount, expectedJobs });

      // Trigger recovery if any marketplace is still in QUEUEING state
      const status = taskStatus[taskId];
      const hasQueuingMarketplace = status && Object.values(status.marketplaces || {}).some(
        mp => mp?.status === 'QUEUEING'
      );

      if (hasQueuingMarketplace) {
        await recoverStuckTask(taskId);
      }

      jobCreationVerificationMap.delete(key);
    } else if (parentJob.actualJobs && parentJob.actualJobs > 0) {
      // Jobs are being created, remove from verification
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
  const currentLoopCount = loopCounterStats[taskId]?.[marketplace] || 1;
  return `${taskId}-${marketplace}-loop${currentLoopCount}`.toLowerCase();
}


async function ensureParentJobExists(taskId: string, marketplace: string): Promise<void> {
  const parentJobId = getParentJobId(taskId, marketplace);
  const currentLoopCount = loopCounterStats[taskId]?.[marketplace] || 1;
  const timestamp = new Date().toISOString();

  // Validate inputs
  if (!taskId || !marketplace) {
    logSystemError('ParentJob', `Invalid inputs - taskId: ${taskId}, marketplace: ${marketplace}`, undefined, { taskId, marketplace });
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
          logSystemError('ParentJob', `Task ${taskId} not found in activeTasks!`, undefined, { taskId });
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
      logSystemError('ParentJob', `Error creating parent job ${parentJobId}`, error, { parentJobId });
      if (error instanceof Error) {
        logSystemError('ParentJob', `Error details: ${error.message}`);
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
      return true;
    }

    // Check if creation is pending
    if (pendingJobCreation.has(parentJobId)) {
      await new Promise(resolve => setTimeout(resolve, 100));
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
        logSystemError('OrphanedJobRecovery', 'Recovery check failed', error);
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
            logSystemError('OrphanedJobRecovery', `Failed to remove job ${job.id}`, error, { jobId: job.id });
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
              logSystemError('OrphanedJobRecovery', `Failed to remove job ${job.id}`, error, { jobId: job.id });
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
      logSystemError('OrphanedJobRecovery', 'Error during recovery', error);
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
    logSystemError('CancellationJobStatus', `Error checking cancellation status for task ${taskId}`, error, { taskId });
    throw error;
  }
}

/**
 * Recovers orphaned jobs that don't have parent job tracking
 * This helps fix jobs that were created before parent jobs
 */
async function recoverOrphanedJobs() {
  try {

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
                  }
                }
              }
            }
          }
        }
      } catch (error) {
        logSystemError('OrphanedJobRecovery', `Error processing ${state} jobs`, error, { state });
      }
    }

    if (orphanedCount > 0) {
    } else {
    }

  } catch (error) {
    logSystemError('OrphanedJobRecovery', 'Error during orphaned job recovery', error);
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
