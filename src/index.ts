/* eslint-disable */
import express from "express";
import { config } from "dotenv";

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
import { bidOnBlur, cancelBlurBid, cancelBlurRequest, fetchBlurBid, fetchBlurCollectionStats, fetchBlurTraitFloor } from "./marketplace/blur/bid";
import { bidOnMagiceden, cancelMagicEdenBid, cancelMagicedenRequest, fetchMagicEdenCollectionStats, fetchMagicEdenOffer, fetchMagicEdenTokens, fetchMagicEdenTraitFloor } from "./marketplace/magiceden";
import { CollectionItemsQueryVariables, fetchOpenSeaCollectionItems, fetchOpenseaTraits, getCollectionDetails, getOpenseaFloor } from "./functions";
import mongoose from 'mongoose';
import Task, { ITask, SelectedTraits } from "./models/task.model";
import { Queue, Worker, Job, JobType, JobsOptions, QueueEvents, QueueOptions } from "bullmq";
import Wallet, { IWallet } from "./models/wallet.model";
import redisClient, { createBullMQConnection } from "./utils/redis";
import { BASE_URL, ERC20_MIN_ABI } from "./constants";
import { Contract, ethers, Wallet as Web3Wallet, parseEther, MaxUint256 } from "ethers";
import { DistributedLockManager } from "./utils/lock";
import net from 'net'

import BidLogs from "./models/logs.model";
import PQueue from "p-queue";
import { PrismaClient, Order as PrismaOrder } from '@prisma/client'
import Bottleneck from "bottleneck";
import { USDC, WETH } from "./utils/currencies";
import axios from "axios";
import { decryptPrivateKey, setPassword, getPassword, decryptPassword } from "./utils/crypto";
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
const prisma = new PrismaClient()

const MARKETPLACE_WS_URL = "wss://nfttools-ws-proxy.nfttools.io";

export const RATE_LIMIT = Number(process.env.RATE_LIMIT)
export const API_KEY = process.env.API_KEY;
const bidStats: BidCounts = {};
const ownBidCounts: { [marketplace: string]: number } = {};
const currentTokenIds: BottomListings = {};
const currentTraits: CurrentTraits = {}
const taskStatus: TaskStatus = {};
const bestOffers: BidCounts = {};
const floorPrices: BidCounts = {};
const balances: WalletBalances = {}
export const warningBids: WarningBids = {};
export const warningMessage: WarningMessage = {};
export const errorStats: BidCounts = {};
export const skipStats: BidCounts = {};
// Blur rate limiting is now handled by BlurRateLimiter class
const QUEUE_NAME = 'BIDDING_BOT';

// Debug counter for OpenSea trait bid completions
let openseaTraitBidCompletionCounter = 0;

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
    opensea: number[];
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
    nextLoopTime?: Date;
    waitingTime?: number;
    cancellationStartTime?: number;
    previousState?: 'RUNNING' | 'WAITING' | 'CANCELLING' | 'SCHEDULED' | 'COMPLETED' | 'QUEUED' | 'QUEUEING' | 'STARTED' | "STOPPED" | "PROCESSING";
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
export const WORKER_COUNT = Math.max(2, Math.floor(os.cpus().length * 0.75))
let traitsToRemove: { [key: string]: string[] } = {}
let removedTokens: { [key: string]: { opensea: string[]; magiceden: string[] } } = {}

// Use BoundedCache instead of Maps for memory-efficient tracking
const seenOrderHashes = new BoundedCache<string, number>(10000, 30 * 60 * 1000); // 10k items, 30 min TTL
const seenBids = new BoundedCache<string, number>(10000, 30 * 60 * 1000); // 10k items, 30 min TTL

// Configuration for cleanup (no longer needed with BoundedCache)
const CLEANUP_INTERVAL = 30 * 60 * 1000; // Run cleanup every 30 minutes for better deduplication

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
let queueCleanupInterval: NodeJS.Timeout | null = null;
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

const COLLECTION_BID_PRIORITY = {
  OPENSEA: 2,
  MAGICEDEN: 2,
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
let netEnabled = true;

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

const blurLimiter = new Bottleneck({
  minTime: 250, maxConcurrent: 4,
})

// Initialize worker later in startServer function
let workers: Worker[] = [];

const taskTimeouts = new Map<string, NodeJS.Timeout>();
const taskCountdownTimers = new Map<string, NodeJS.Timeout>();

// Add a Map to track task locks
const taskLocks = new Map<string, TaskLock>();
// Add a Map to track loop scheduling locks with timestamps
const loopSchedulingLocks = new Map<string, { locked: boolean; timestamp: number }>();
const LOCK_TIMEOUT_MS = 5 * 60 * 1000; // 5 minutes
// Add a Map to track tasks being scheduled to prevent duplicates
const tasksBeingScheduled = new Map<string, boolean>();
// Completion retry tracking to prevent infinite loops
const completionRetries = new Map<string, number>();
const MAX_COMPLETION_RETRIES = 12; // 60 seconds total (12 * 5s)

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
      console.log(YELLOW + `[acquireLoopSchedulingLock] Task ${taskId} has stale lock (age: ${Math.round((now - existingLock.timestamp) / 1000)}s), releasing it` + RESET);
      loopSchedulingLocks.delete(taskId);
    } else {
      console.log(YELLOW + `[acquireLoopSchedulingLock] Task ${taskId} already has a scheduling lock (age: ${Math.round((now - existingLock.timestamp) / 1000)}s)` + RESET);
      return false;
    }
  }

  console.log(GREEN + `[acquireLoopSchedulingLock] Acquiring lock for task ${taskId}` + RESET);
  loopSchedulingLocks.set(taskId, { locked: true, timestamp: now });
  return true;
}

// Helper function to release loop scheduling lock
function releaseLoopSchedulingLock(taskId: string): void {
  if (loopSchedulingLocks.has(taskId)) {
    console.log(GREEN + `[releaseLoopSchedulingLock] Releasing lock for task ${taskId}` + RESET);
    loopSchedulingLocks.delete(taskId);
  } else {
    console.log(YELLOW + `[releaseLoopSchedulingLock] No lock found for task ${taskId}` + RESET);
  }
}

// Helper function to clean up stale jobs from queue
async function cleanupStaleQueueJobs(): Promise<void> {
  try {
    const [waitingJobs, activeJobs, delayedJobs, isPaused] = await Promise.all([
      queue.getWaitingCount(),
      queue.getActiveCount(),
      queue.getDelayedCount(),
      queue.isPaused()
    ]);

    console.log(YELLOW + `[Queue Cleanup] Checking queue: ${waitingJobs} waiting, ${activeJobs} active, ${delayedJobs} delayed` + RESET);

    // Process waiting jobs if they're stuck
    if (waitingJobs > 0 && activeJobs === 0) {
      console.log(YELLOW + '[Queue Cleanup] Found waiting jobs with no active workers - attempting to restart queue' + RESET);

      // Resume queue if paused
      if (isPaused) {
        await queue.resume();
        console.log(GREEN + '[Queue Cleanup] Resumed paused queue' + RESET);
      }

      // Check first few waiting jobs for disabled tokens
      const stuckJobs = await queue.getJobs(['waiting'], 0, 10);
      for (const job of stuckJobs) {
        if (job && job.data) {
          // Check if job is for disabled token
          if (job.name === OPENSEA_TOKEN_BID && job.data.asset &&
            tradingDisabledCache.isDisabled(job.data.asset.contractAddress, +job.data.asset.tokenId, 'opensea')) {
            await job.remove();
            console.log(YELLOW + `[Queue Cleanup] Removed job for disabled token ${job.data.asset.tokenId}` + RESET);
          }
        }
      }
    }

    // Get all jobs that might be stale
    const allJobs = await queue.getJobs(['waiting', 'active', 'delayed']);
    const now = Date.now();
    let cleanedCount = 0;
    let disabledTokenCount = 0;

    for (const job of allJobs) {
      // Skip if job is null or undefined
      if (!job) continue;

      const taskId = job.data?._id;

      // Check if job is for a task that no longer exists
      if (taskId && !activeTasks.has(taskId)) {
        await job.remove();
        cleanedCount++;
        const customJobId = (job.opts as any)?.jobId || job.id;
        console.log(YELLOW + `[Queue Cleanup] Removed stale job ${job.name} (${customJobId}) for non-existent task ${taskId}` + RESET);
        continue;
      }

      // Check if job has been stuck in active state for too long (> 5 minutes)
      if (job.processedOn && (now - job.processedOn) > 5 * 60 * 1000) {
        await job.remove();
        cleanedCount++;
        const customJobId = (job.opts as any)?.jobId || job.id;
        console.log(YELLOW + `[Queue Cleanup] Removed stuck job ${job.name} (${customJobId}) (processing for > 5 minutes)` + RESET);
        continue;
      }

      // Remove jobs for disabled tokens
      if (job.name === OPENSEA_TOKEN_BID && job.data?.asset) {
        if (tradingDisabledCache.isDisabled(job.data.asset.contractAddress, +job.data.asset.tokenId, 'opensea')) {
          await job.remove();
          disabledTokenCount++;
          cleanedCount++;
          // Don't log each one to avoid spam
        }
      }
    }

    if (disabledTokenCount > 0) {
      console.log(YELLOW + `[Queue Cleanup] Removed ${disabledTokenCount} jobs for disabled tokens` + RESET);
    }

    if (cleanedCount > 0) {
      console.log(GREEN + `[Queue Cleanup] Cleaned up ${cleanedCount} total stale/disabled jobs` + RESET);
    }
  } catch (error) {
    console.error(RED + '[Queue Cleanup] Error during cleanup: ' + RESET, error);
  }
}

// Mutex/locking pattern for taskStatus updates
const taskStatusLocks = new Map<string, boolean>();

async function safeUpdateTaskStatus(taskId: string, updates: Partial<typeof taskStatus[string]>): Promise<void> {
  // Wait for any existing lock to be released
  while (taskStatusLocks.get(taskId)) {
    await new Promise(resolve => setTimeout(resolve, 10));
  }

  // Acquire lock
  taskStatusLocks.set(taskId, true);

  try {
    // Initialize taskStatus if it doesn't exist
    if (!taskStatus[taskId]) {
      taskStatus[taskId] = {
        status: 'RUNNING',
        runningPercentage: 0,
        waitingTime: 0,
        cancellationPercentage: 0,
        processingPercentage: 0,
        nextLoopTime: new Date()
      };
    }

    // Apply updates
    Object.assign(taskStatus[taskId], updates);
  } finally {
    // Always release lock
    taskStatusLocks.delete(taskId);
  }
}


// Initialize queue and queueEvents with Redis connection
// These will be initialized in startServer()
let queue: Queue;
let queueEvents: QueueEvents;
let QUEUE_OPTIONS: QueueOptions;

// Redis connection for internal use (not BullMQ)
let redis = redisClient.getClient();

export const MAGENTA = '\x1b[35m';
export const BLUE = '\x1b[34m';
export const RESET = '\x1b[0m';
const GREEN = '\x1b[32m';
const YELLOW = '\x1b[33m';
export const RED = '\x1b[31m';
export const GOLD = '\x1b[33m';
const CYAN = '\x1b[36m';

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
const CANCELLATION_JOB_RETRY_ATTEMPTS = 3;
const CANCELLATION_JOB_RETRY_DELAY = 2000;
const CANCELLATION_JOB_TYPES = [CANCEL_OPENSEA_BID, CANCEL_MAGICEDEN_BID, CANCEL_BLUR_BID];

// Helper function to create cancellation job options with retry configuration
function getCancellationJobOptions(taskId: string, orderId: string, jobType: string) {
  return {
    jobId: `${jobType}-${taskId}-${orderId}`,
    attempts: CANCELLATION_JOB_RETRY_ATTEMPTS,
    backoff: {
      type: 'exponential' as const,
      delay: CANCELLATION_JOB_RETRY_DELAY,
    },
    removeOnFail: false, // Keep failed jobs for inspection
    removeOnComplete: true
  };
}

const MAGICEDEN_MARKETPLACE = "0x9A1D00bEd7CD04BCDA516d721A596eb22Aac6834"
const MAX_RETRIES: number = 5;
const ALCHEMY_API_KEY = process.env.ALCHEMY_API_KEY as string;
const OPENSEA_PROTOCOL_ADDRESS = "0x0000000000000068F116a894984e2DB1123eB395"
const HEALTH_CHECK_INTERVAL = 5000;
let enabledMarketplaces: string[] = []
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
let reconnectTimeoutId: NodeJS.Timeout | null = null;
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

function cleanupMemory() {
  if (global.gc) {
    global.gc();
  }
}

async function monitorHealth() {
  try {
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

    // Also track system memory
    const totalMemory = os.totalmem();
    const systemMemoryPercent = (used.rss / totalMemory) * 100;

    // Check if queue is paused
    const isPaused = await queue.isPaused();

    // Auto-recovery: If queue is paused but we have waiting jobs and conditions are good
    if (isPaused && counts.waiting > 0 && heapPercentOfMax < 70 && counts.active < DRAIN_THRESHOLD) {
      console.log(YELLOW + '⚡ Queue is paused with waiting jobs - attempting auto-recovery...' + RESET);
      try {
        await queue.resume();
        console.log(GREEN + '✅ Queue auto-recovery successful' + RESET);
      } catch (err) {
        console.error(RED + '❌ Queue auto-recovery failed:' + RESET, err);
      }
    }

    // Log high memory usage but don't pause the queue
    if (heapPercentOfMax > 75) {
      console.log(RED + `⚠️  HIGH MEMORY USAGE: ${heapUsedMB.toFixed(0)}MB / ${MAX_HEAP_MB}MB (${heapPercentOfMax.toFixed(1)}% of max heap) ⚠️` + RESET);
      console.log(RED + `⚠️  Heap allocation: ${heapTotalMB.toFixed(0)}MB (${heapPercentOfTotal.toFixed(1)}% used) ⚠️` + RESET);
      cleanupMemory();
    }

    // Log high active jobs but let queue handle backpressure naturally
    if (counts.active > DRAIN_THRESHOLD) {
      console.log(YELLOW + `[Queue Debug] High active jobs: ${counts.active} > ${DRAIN_THRESHOLD} (queue will handle backpressure)` + RESET);
    }

    // Log rate limit status but let queue concurrency handle it
    if (counts.prioritized > RATE_LIMIT) {
      console.log(YELLOW + `[Queue Rate Limit] Prioritized jobs (${counts.prioritized}) exceed RATE_LIMIT (${RATE_LIMIT})` + RESET);
    }

    const totalJobs = Object.values(counts).reduce((a, b) => a + b, 0);

    console.log('\n📊 ═══════ Health Monitor Stats ═══════ 📊');

    console.log(BLUE + '\n💾 Memory Usage:' + RESET);
    Object.entries(memoryStats).forEach(([key, value]) => {
      console.log(`${key}:${value}`);
    });

    console.log(BLUE + '\n📋 Queue Status:' + RESET);
    // Show paused status prominently
    if (isPaused) {
      console.log(RED + '⏸️  QUEUE IS PAUSED' + RESET);
    } else {
      console.log(GREEN + '▶️  QUEUE IS RUNNING' + RESET);
    }

    // Show waiting job monitor status
    if (counts.waiting > 0) {
      console.log(YELLOW + '⚡ Waiting Job Monitor: Active (failing waiting jobs for retry)' + RESET);
      // await queue.drain()
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

    // Timeout stats logging removed

    // Monitor stuck tasks at 100% completion
    let stuckTaskCount = 0;
    for (const [taskId, status] of Object.entries(taskStatus)) {
      if (status.runningPercentage >= 100 && status.status === 'RUNNING') {
        const task = activeTasks.get(taskId);
        if (task && task.running) {
          stuckTaskCount++;
          // Check if task has been stuck for more than 2 minutes
          const stuckDuration = Date.now() - ((status as any).lastUpdate || Date.now());
          if (stuckDuration > 120000) { // 2 minutes
            console.warn(YELLOW + `[HEALTH] Task ${taskId} stuck at 100% for ${Math.floor(stuckDuration / 1000)}s` + RESET);
          }
        }
      }
    }

    if (stuckTaskCount > 0) {
      console.log(YELLOW + `\n⚠️  Stuck Tasks: ${stuckTaskCount} tasks at 100% completion` + RESET);
    }

    updateAllBidRates();

  } catch (error) {
    logger.logGeneralError("Health monitoring failed", {
      error: error as Error,
      uptime: process.uptime(),
      memoryUsage: process.memoryUsage()
    });
  }
}


setInterval(monitorHealth, HEALTH_CHECK_INTERVAL);

// Memory cleanup interval - runs every 5 minutes
const MEMORY_CLEANUP_INTERVAL = 5 * 60 * 1000; // 5 minutes

async function performMemoryCleanup() {
  try {
    const startTime = Date.now();
    let cleanedItems = 0;

    // Clean up completed tasks older than 1 hour
    const oneHourAgo = Date.now() - 60 * 60 * 1000;

    // Clean completedTasks
    const completedTasksBefore = Object.keys(completedTasks).length;
    Object.keys(completedTasks).forEach(taskId => {
      if (!activeTasks.has(taskId)) {
        delete completedTasks[taskId];
        cleanedItems++;
      }
    });

    // Clean taskStatus for inactive tasks
    const taskStatusBefore = Object.keys(taskStatus).length;
    Object.keys(taskStatus).forEach(taskId => {
      if (!activeTasks.has(taskId) && taskStatus[taskId].status === 'STOPPED') {
        delete taskStatus[taskId];
        cleanedItems++;
      }
    });

    // Clean errorStats for inactive tasks
    const errorStatsBefore = Object.keys(errorStats).length;
    Object.keys(errorStats).forEach(taskId => {
      if (!activeTasks.has(taskId)) {
        delete errorStats[taskId];
        cleanedItems++;
      }
    });

    // Clean bidStats for inactive tasks
    const bidStatsBefore = Object.keys(bidStats).length;
    Object.keys(bidStats).forEach(taskId => {
      if (!activeTasks.has(taskId)) {
        delete bidStats[taskId];
        cleanedItems++;
      }
    });

    // Clean skipStats for inactive tasks
    Object.keys(skipStats).forEach(taskId => {
      if (!activeTasks.has(taskId)) {
        delete skipStats[taskId];
        cleanedItems++;
      }
    });

    // Clean floorPrices for inactive tasks
    Object.keys(floorPrices).forEach(taskId => {
      if (!activeTasks.has(taskId)) {
        delete floorPrices[taskId];
        cleanedItems++;
      }
    });

    // Clean currentTokenIds for inactive tasks
    Object.keys(currentTokenIds).forEach(taskId => {
      if (!activeTasks.has(taskId)) {
        delete currentTokenIds[taskId];
        cleanedItems++;
      }
    });

    // Clean taskLoopCounters
    taskLoopCounters.forEach((count, taskId) => {
      if (!activeTasks.has(taskId)) {
        taskLoopCounters.delete(taskId);
        cleanedItems++;
      }
    });

    // Clean tasksBeingScheduled
    tasksBeingScheduled.forEach((value, taskId) => {
      if (!activeTasks.has(taskId)) {
        tasksBeingScheduled.delete(taskId);
        cleanedItems++;
      }
    });

    // Force garbage collection if available
    if (global.gc) {
      global.gc();
      console.log(GREEN + '[Memory Cleanup] Forced garbage collection' + RESET);
    }

    const duration = Date.now() - startTime;
    console.log(GREEN + `[Memory Cleanup] Completed in ${duration}ms - Cleaned ${cleanedItems} items` + RESET);
    console.log(GREEN + `[Memory Cleanup] Stats: completedTasks ${completedTasksBefore}→${Object.keys(completedTasks).length}, ` +
      `taskStatus ${taskStatusBefore}→${Object.keys(taskStatus).length}, ` +
      `errorStats ${errorStatsBefore}→${Object.keys(errorStats).length}, ` +
      `bidStats ${bidStatsBefore}→${Object.keys(bidStats).length}` + RESET);
  } catch (error) {
    console.error(RED + '[Memory Cleanup] Error during cleanup:' + RESET, error);
  }
}

// Start cleanup interval
setInterval(performMemoryCleanup, MEMORY_CLEANUP_INTERVAL);

// Clean up stale scheduling locks periodically
setInterval(() => {
  const now = Date.now();
  let cleanedCount = 0;

  console.log(CYAN + `[Lock Status] Current scheduling locks: ${loopSchedulingLocks.size}` + RESET);

  loopSchedulingLocks.forEach((lock, taskId) => {
    const age = Math.round((now - lock.timestamp) / 1000);
    console.log(CYAN + `[Lock Status] Task ${taskId}: locked=${lock.locked}, age=${age}s` + RESET);

    if (lock.locked && now - lock.timestamp > LOCK_TIMEOUT_MS) {
      console.log(YELLOW + `[Lock Cleanup] Releasing stale lock for task ${taskId} (age: ${age}s)` + RESET);
      loopSchedulingLocks.delete(taskId);
      cleanedCount++;
    }
  });

  if (cleanedCount > 0) {
    console.log(GREEN + `[Lock Cleanup] Cleaned ${cleanedCount} stale scheduling locks` + RESET);
  }
}, 60000); // Check every minute

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

// Clear queue and restart all jobs on server start
async function clearQueueAndRestartJobs() {
  try {
    // Pause the queue before obliterating
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

    // Clear all existing jobs from the queue
    await queue.obliterate({ force: true });

    // Resume the queue after clearing
    await queue.resume();

    // Clear any stored job states
    parentJobs.clear();
    // Reset completedTasks to empty object
    Object.keys(completedTasks).forEach(key => delete completedTasks[key]);
    // DO NOT clear taskStatus - it should persist across queue restarts
    // Clear all countdown timers
    taskCountdownTimers.forEach((timer) => clearInterval(timer));
    taskCountdownTimers.clear();

    // Clear all task timeouts
    taskTimeouts.forEach((timeout) => clearTimeout(timeout));
    taskTimeouts.clear();

    // Fetch all tasks from database
    await fetchCurrentTasks();

    // Start all running tasks - DISABLED to prevent auto-starting tasks on server startup
    // await runScheduledLoop();
    console.log(YELLOW + '⚠️  Automatic task startup disabled - tasks loaded but not started' + RESET);

    // Note: Listing jobs will be recreated by initializeListingScheduler
  } catch (error) {
    throw error;
  }
}

// Network connectivity monitoring
let isConnected = true;
let lastConnectivityCheck = Date.now();
let connectivityLostAt: number | null = null;

async function checkInternetConnectivity(): Promise<boolean> {
  try {
    // Try to ping a reliable endpoint
    const response = await axios.get('https://api.etherscan.io/api?module=proxy&action=eth_blockNumber', {
      timeout: 5000,
      validateStatus: () => true
    });
    return response.status === 200;
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
// Clean up old events every 30 minutes to prevent memory growth
// This is long enough to catch duplicates but not so long as to cause memory issues
setInterval(() => {
  console.log(CYAN + `Clearing ${recentItemSoldEvents.size} cached ITEM_SOLD events` + RESET);
  recentItemSoldEvents.clear();
}, 30 * 60 * 1000);
function startConnectivityMonitoring() {
  // Check connectivity every 30 seconds
  connectivityCheckInterval = setInterval(async () => {
    const currentlyConnected = await checkInternetConnectivity();

    if (!isConnected && currentlyConnected) {
      // Connection restored
      isConnected = true;
      lastConnectivityCheck = Date.now();
      await handleConnectivityRestored();
    } else if (isConnected && !currentlyConnected) {
      // Connection lost
      isConnected = false;
      connectivityLostAt = Date.now();
      console.error(RED + '\n⚠️  Internet connectivity lost!' + RESET);
    }

    lastConnectivityCheck = Date.now();
  }, 30000); // Check every 30 seconds

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

    // Clean up orphaned taskStatus entries (only if task no longer exists in database)
    const validTaskIds = new Set(formattedTasks.map(task => task._id));
    Object.keys(taskStatus).forEach(taskId => {
      if (!validTaskIds.has(taskId)) {
        // Cleaning up orphaned taskStatus
        delete taskStatus[taskId];

        // Also clean up any associated timers
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

    const jobs = formattedTasks
      .filter((task) => task.running)
      .flatMap(task =>
        task.running && task.selectedMarketplaces.map((m: string) => m.toLowerCase()).includes("opensea")
          ? [{ name: OPENSEA_SCHEDULE, data: task, opts: { jobId: `${task._id}-opensea-schedule` } }]
          : []
      ).concat(
        formattedTasks.flatMap(task =>
          task.running && task.selectedMarketplaces.map((m: string) => m.toLowerCase()).includes("blur")
            ? [{ name: BLUR_SCHEDULE, data: task, opts: { jobId: `${task._id}-blur-schedule` } }]
            : []
        )
      ).concat(
        formattedTasks.flatMap(task =>
          task.running && task.selectedMarketplaces.map((m: string) => m.toLowerCase()).includes("magiceden")
            ? [{ name: MAGICEDEN_SCHEDULE, data: task, opts: { jobId: `${task._id}-magiceden-schedule` } }]
            : []
        )
      );
    // Task initialization complete
  } catch (error) {
    logger.logDatabaseError("Failed to fetch current tasks", {
      error: error as Error,
      taskCount: currentTasks.length
    });
  }
}

const DOWNTIME_THRESHOLD = 5 * 60 * 1000;
const LAST_RUNTIME_KEY = 'server:last_runtime';


interface BulkJobTracker {
  taskId: string;
  jobName: string;
  marketplace: string; // Added marketplace field
  totalJobs: number;
  processedJobs: number;
  completedJobs: number;
  failedJobs: number;
  startTime: number;
  status: 'pending' | 'processing' | 'completed' | 'failed';
  percentComplete: number;
}

// Add tracking map
const bulkJobTrackers = new Map<string, BulkJobTracker>();
interface BulkJobParent {
  parentId: string;
  totalJobs: number;
  completedJobs: number;
  failedJobs: number;
  startTime: number;
  taskId: string;
  jobName: string;
  // Optional bid tracking fields (only used for cancellation jobs)
  totalBids?: number;        // Total number of bids to cancel
  completedBids?: number;    // Number of successfully cancelled bids
  failedBids?: number;       // Number of bids that failed to cancel
}

const parentJobs = new Map<string, BulkJobParent>();

// Cancellation parent jobs for tracking cancellation progress (similar to parentJobs)
const cancellationParentJobs = new Map<string, BulkJobParent>();

// Preprocessing results stored per task
interface TaskPreprocessing {
  bidType: 'token' | 'trait' | 'collection';
  opensea: {
    enabled: boolean;
    traits: { type: string; value: string }[];
    tokens: number[];
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

// Queue management for password-based security
async function pauseQueueAndWorkers() {
  try {
    console.log(YELLOW + 'No password set - pausing queue and workers...' + RESET);
    await queue.pause();
    if (workers.length > 0) {
      await Promise.all(workers.map(worker => worker.pause()));
    }
    console.log(YELLOW + 'Queue and workers paused. Waiting for password...' + RESET);
    console.log(YELLOW + 'Note: All jobs have been removed from the queue for security.' + RESET);
  } catch (error) {
    console.log(error);
  }
}

async function resumeQueueAndWorkers() {
  try {
    console.log(GREEN + 'Password set - resuming queue and workers...' + RESET);
    await queue.resume();
    if (workers.length > 0) {
      await Promise.all(workers.map(worker => worker.resume()));
    }
    console.log(GREEN + 'Queue and workers resumed.' + RESET);
  } catch (error) {
    console.log(error);
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

    // Create BullMQ connections
    const queueConnection = await createBullMQConnection();
    // Initialize queue options
    QUEUE_OPTIONS = {
      prefix: '{bull}',
      connection: queueConnection,
      defaultJobOptions: {
        removeOnComplete: true,
        removeOnFail: true
      }
    };

    // Initialize queue and events
    queue = new Queue(QUEUE_NAME, QUEUE_OPTIONS);
    queueEvents = new QueueEvents(QUEUE_NAME, { connection: queueConnection, prefix: '{bull}' });


    // Initialize queue event handlers
    initializeQueueEventHandlers();

    // Initialize multiple workers with Redis connection and proper lock settings
    const concurrencyPerWorker = Number(RATE_LIMIT) / WORKER_COUNT;

    for (let i = 0; i < WORKER_COUNT; i++) {
      // Create a separate connection for each worker
      const workerConnection = await createBullMQConnection();

      const worker = new Worker(
        QUEUE_NAME,
        async (job) => {
          const startTime = Date.now();

          // Add job timeout handling - longer timeout for SCHEDULE jobs
          const isScheduleJob = job.name && [OPENSEA_SCHEDULE, MAGICEDEN_SCHEDULE, BLUR_SCHEDULE].includes(job.name);
          const jobTimeout = 15000; // 3 minutes for schedule jobs, 90 seconds for others
          const abortController = new AbortController();

          const timeoutPromise = new Promise((_, reject) => {
            setTimeout(() => {
              abortController.abort(); // Signal cancellation to any async operations
              const customJobId = (job.opts as any)?.jobId || job.id;
              reject(new Error(`Job ${customJobId} (ID: ${job.id}) timed out after ${jobTimeout}ms - aborted`));
            }, jobTimeout);
          });

          // Check if job has exceeded its timeout
          if (job.data.timeoutAt && Date.now() > job.data.timeoutAt) {
            const customJobId = (job.opts as any)?.jobId || job.id;
            const timeoutError = new Error(`Job ${customJobId} (ID: ${job.id}) timed out after ${job.data.maxDuration}ms`);
            throw timeoutError;
          }

          // Memory check before processing
          const memUsage = process.memoryUsage();
          const heapUsedMB = memUsage.heapUsed / 1024 / 1024;
          const heapTotalMB = memUsage.heapTotal / 1024 / 1024;

          // Get max heap from NODE_OPTIONS or use default
          const maxHeapMatch = process.execArgv.join(' ').match(/--max-old-space-size=(\d+)/);
          const MAX_HEAP_MB = maxHeapMatch ? parseInt(maxHeapMatch[1]) : 8192; // Default to 8GB if not found

          // Calculate percentage based on max configured heap
          const heapPercentOfMax = (heapUsedMB / MAX_HEAP_MB) * 100;
          const heapPercentOfTotal = (memUsage.heapUsed / memUsage.heapTotal) * 100;

          // Adjust thresholds based on configured heap size
          const memoryThreshold = MAX_HEAP_MB * 0.8; // 80% of max heap
          const criticalThreshold = MAX_HEAP_MB * 0.9; // 90% of max heap

          // Only warn if we're actually approaching the memory limit
          if (heapUsedMB > memoryThreshold) {
            console.log(YELLOW + `[Memory Warning] Heap Used: ${heapUsedMB.toFixed(0)}MB / ${heapTotalMB.toFixed(0)}MB allocated (${heapPercentOfTotal.toFixed(1)}%)` + RESET);
            console.log(YELLOW + `[Memory Warning] ${heapUsedMB.toFixed(0)}MB / ${MAX_HEAP_MB}MB max (${heapPercentOfMax.toFixed(1)}% of limit)` + RESET);
            const customJobId = (job.opts as any)?.jobId || job.id;
            console.log(YELLOW + `[Memory Warning] Delaying job ${customJobId} (ID: ${job.id})` + RESET);

            // Log memory warning but don't pause - let the system handle it
            console.log(YELLOW + '[Memory Warning] High memory detected, allowing job to continue' + RESET);

            // Force GC if available and memory is critical
            if (global.gc && heapUsedMB > criticalThreshold) {
              global.gc();
              console.log(YELLOW + '[Memory Warning] Forced garbage collection' + RESET);
            }
          } else if (heapPercentOfTotal > 95 && heapTotalMB < MAX_HEAP_MB * 0.5) {
            // If allocated heap is nearly full but we have lots of room to grow, just log it
            if (global.gc) {
              global.gc();
              console.log(CYAN + `[Memory] Heap ${heapUsedMB.toFixed(0)}MB/${heapTotalMB.toFixed(0)}MB (${heapPercentOfTotal.toFixed(1)}%) - Triggering GC to expand heap` + RESET);
            }
          }

          try {
            // Track job execution for debugging


            const result = await Promise.race([
              processJob(job, abortController.signal),
              timeoutPromise
            ]);

            // Record success in circuit breaker for SCHEDULE jobs
            if ([OPENSEA_SCHEDULE, MAGICEDEN_SCHEDULE, BLUR_SCHEDULE].includes(job.name)) {
              const marketplace = job.name.replace('_SCHEDULE', '').toLowerCase();
              taskCircuitBreaker.recordSuccess(job.data._id || 'unknown', marketplace);
            }

            return result || { success: true };
          } catch (error: any) {
            // Check if this is a rate limit error
            if (error?.code === 'RATE_LIMITED') {
              const retryAfter = error.retryAfter || 8; // Default 8 seconds
              const customJobId = (job.opts as any)?.jobId || job.id;
              console.log(`[Blur] Rate limit hit for job ${customJobId} (ID: ${job.id}) - will retry in ${retryAfter}s`);

              // Update job to add delay
              await job.updateData({
                ...job.data,
                rateLimited: true,
                retryAfter: retryAfter
              });

              // Throw error with delay to let BullMQ handle retry with backoff
              const rateLimitError = new Error(`Rate limited - retry after ${retryAfter}s`);
              (rateLimitError as any).retryAfter = retryAfter * 1000; // Convert to ms
              throw rateLimitError;
            }

            // Check if it's a timeout error
            if (error.message && error.message.includes('timed out')) {
              const isScheduleJob = [OPENSEA_SCHEDULE, MAGICEDEN_SCHEDULE, BLUR_SCHEDULE].includes(job.name);
              const isCancellationJob = CANCELLATION_JOB_TYPES.includes(job.name);

              if (isScheduleJob) {
                // Get queue stats for timeout analysis
                const queueCounts = await queue.getJobCounts();
                const queueStats = {
                  waiting: queueCounts.waiting || 0,
                  active: queueCounts.active || 0,
                  completed: queueCounts.completed || 0,
                  failed: queueCounts.failed || 0,
                  delayed: queueCounts.delayed || 0
                };

                // Record timeout in circuit breaker
                const taskId = job.data._id || 'unknown';
                const collection = job.data.contract?.slug || 'unknown';
                const marketplace = job.name.replace('_SCHEDULE', '').toLowerCase();

                const shouldSuspend = taskCircuitBreaker.recordTimeout(taskId, collection, marketplace);

                if (shouldSuspend) {
                  // Remove this job and don't retry
                  await job.remove();
                  return { suspended: true };
                }
              } else {
                // Non-SCHEDULE jobs fail immediately without retry
              }

              // Track timeout stats
              timeoutStats.count++;
              timeoutStats.lastHour.push({ timestamp: Date.now(), jobId: job?.id as string });
              timeoutStats.byJobType[job.name] = (timeoutStats.byJobType[job.name] || 0) + 1;

              // Clean old entries from lastHour
              const oneHourAgo = Date.now() - 60 * 60 * 1000;
              timeoutStats.lastHour = timeoutStats.lastHour.filter(t => t.timestamp > oneHourAgo);

              await logger.logQueueJobFailed(job.name, {
                jobId: job.id,
                taskId: job.data.taskId,
                marketplace: job.data.marketplace,
                error: error as Error,
                errorMessage: error.message,
                jobAttempts: job.attemptsMade,
                duration: Date.now() - startTime,
                queueName: QUEUE_NAME,
                errorType: 'TIMEOUT',
                skipRetry: !isScheduleJob
              });

              // SCHEDULE jobs and cancellation jobs always retry, others fail immediately
              if (isScheduleJob || isCancellationJob) {
                throw error; // Retry with configured backoff
              } else {
                return { success: false, error: error.message, skipRetry: true };
              }
            }


            // Check if this is a SCHEDULE job or cancellation job - they must always retry
            const isScheduleJob = [OPENSEA_SCHEDULE, MAGICEDEN_SCHEDULE, BLUR_SCHEDULE].includes(job.name);
            const isCancellationJob = CANCELLATION_JOB_TYPES.includes(job.name);

            // Handle marketplace-specific errors
            if (error instanceof MarketplaceError) {
              const taskId = job.data.taskId;
              const marketplace = error.marketplace.toLowerCase();

              // Update error stats for hard errors
              if (!isSoftError(error)) {
                if (!errorStats[taskId]) {
                  errorStats[taskId] = { opensea: 0, magiceden: 0, blur: 0 };
                }
                errorStats[taskId][marketplace as keyof typeof errorStats[typeof taskId]]++;
              }

              // Log specific error types
              // Marketplace errors are logged via logger, no need for console logs

              // For non-SCHEDULE jobs, fail immediately without retries
              if (!isScheduleJob) {
                await logger.logQueueJobFailed(job.name, {
                  jobId: job.id,
                  taskId: job.data.taskId,
                  marketplace: job.data.marketplace,
                  error: error as Error,
                  errorMessage: error.message,
                  jobAttempts: job.attemptsMade,
                  duration: Date.now() - startTime,
                  queueName: QUEUE_NAME,
                  errorType: error.constructor.name,
                  isSoftError: isSoftError(error),
                  skipRetry: true
                });
                // Move job to failed without retrying
                await job.moveToFailed(error, '0', false);
                return { success: false, error: error.message };
              }
            }

            await logger.logQueueJobFailed(job.name, {
              jobId: job.id,
              taskId: job.data.taskId,
              marketplace: job.data.marketplace,
              error: error as Error,
              errorMessage: error instanceof Error ? error.message : String(error),
              jobAttempts: job.attemptsMade,
              duration: Date.now() - startTime,
              queueName: QUEUE_NAME,
              skipRetry: !isScheduleJob
            });

            // SCHEDULE jobs and cancellation jobs always retry, others fail immediately
            if (isScheduleJob || isCancellationJob) {
              throw error; // Let BullMQ handle retries
            } else {
              // Non-SCHEDULE/cancellation jobs fail without retry
              await job.moveToFailed(error, '0', false);
              return { success: false, error: error instanceof Error ? error.message : String(error) };
            }
          }
        },
        {
          connection: workerConnection,
          prefix: '{bull}',
          concurrency: concurrencyPerWorker,
          lockDuration: 30000, // 1 minute (reduced from 2 minutes)
          lockRenewTime: 10000, // Renew lock every 20 seconds
          stalledInterval: 15000, // 45 seconds before considering job stalled
          maxStalledCount: 1, // Max 1 stall attempt (reduced from 2)
        }
      );

      // Set up worker error handlers
      worker.on('error', async (error) => {
        // Format BullMQ lock renewal errors
        await BullMQErrorFormatter.formatError(error, {
          workerConcurrency: concurrencyPerWorker,
          workerId: i,
          queueName: QUEUE_NAME
        });
      });
      // Add worker to array
      workers.push(worker);
    }

    console.log(GREEN + `Initialized ${WORKER_COUNT} workers with ${concurrencyPerWorker} concurrency each (Total: ${Number(RATE_LIMIT)})` + RESET);

    // Add process-level error handler for uncaught BullMQ errors
    process.on('unhandledRejection', async (reason, promise) => {
      if (reason instanceof Error && reason.message.includes('could not renew lock')) {
        await BullMQErrorFormatter.formatError(reason);
      } else {
        console.error('Unhandled Rejection at:', promise, 'reason:', reason);
      }
    });


    const mongoUri = process.env.MONGODB_URI;
    if (!mongoUri) {
      throw new Error('MONGODB_URI environment variable is not set');
    }

    console.log('Connecting to MongoDB...');
    try {
      await mongoose.connect(mongoUri, {
        serverSelectionTimeoutMS: 30000, // Increased from 15000
        connectTimeoutMS: 30000, // Increased from 15000
        socketTimeoutMS: 45000, // Increased from 30000
        maxPoolSize: 10,
        retryWrites: true,
        family: 4, // Use IPv4, skip trying IPv6
        heartbeatFrequencyMS: 5000 // Add heartbeat to detect connection issues earlier
      });
      console.log('Connected to MongoDB successfully');
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
      console.error(RED + 'Failed to update tasks:' + RESET, error);
    }

    // Clear queue and restart all jobs on server start - AFTER MongoDB is connected
    await clearQueueAndRestartJobs();

    try {
      const lastRuntime = redis ? await redis.get(LAST_RUNTIME_KEY) : null;
      const currentTime = Date.now();

      if (lastRuntime) {
        const downtime = currentTime - parseInt(lastRuntime);
        if (downtime > DOWNTIME_THRESHOLD) {
          const states: JobType[] = ['active', 'prioritized', 'delayed', 'failed', 'completed', 'wait', 'paused'];
          for (const state of states) {
            const jobs = await queue.getJobs([state]);
            await Promise.allSettled(jobs.map(async (job) => {
              if (!job.token) {
                await job.remove()
              }
            }));
          }
        }
      }

      await initialize(RATE_LIMIT);

      // Initialize wallet cache with pre-decryption
      const password = await getPassword();
      if (password) {
        await walletCache.initialize(password);
      }

      if (redis) {
        await redis.set(LAST_RUNTIME_KEY, currentTime.toString());
      }
    } catch (redisError) {
      console.error('Redis operation failed:', redisError);
    }

    setInterval(async () => {
      if (redis) {
        await redis.set(LAST_RUNTIME_KEY, Date.now().toString());
      }

      // Monitor memory usage
      const memoryUsage = process.memoryUsage();
      const heapPercentage = (memoryUsage.heapUsed / memoryUsage.heapTotal) * 100;

      if (heapPercentage > 90) {
        await logger.logMemoryWarning('High memory usage detected', memoryUsage, {
          heapPercentage,
          threshold: '90%',
          availableMemory: os.freemem(),
          totalMemory: os.totalmem(),
          uptime: process.uptime()
        });
      }

      // Log memory metrics periodically for monitoring
      if (Date.now() % 300000 < 60000) { // Every 5 minutes
        await logger.logDebug(ErrorCategory.PERFORMANCE, 'Memory usage metrics', {
          memoryUsage,
          heapPercentage,
          availableMemory: os.freemem(),
          totalMemory: os.totalmem(),
          uptime: process.uptime(),
          activeTasksCount: activeTasks.size,
          queueMetrics: await queue.getJobCounts()
        });
      }
    }, 60000);

    server.listen(port, async () => {
      console.log(`Magic happening on http://localhost:${port}`);
      console.log(`WebSocket server is running on ws://localhost:${port}`);
      await logger.logInfo(ErrorCategory.GENERAL, 'Server started successfully', {
        port: port,
        environment: process.env.NODE_ENV,
        nodeVersion: process.version,
        platform: os.platform(),
        memory: process.memoryUsage(),
        cpuCount: os.cpus().length,
        rateLimit: RATE_LIMIT
      });
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
      console.error('WebSocket connection failed:', error);
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

    // Initialize queue cleanup interval - more aggressive
    queueCleanupInterval = setInterval(cleanupStaleQueueJobs, 30 * 1000); // Run every 30 seconds
    console.log('🧹 Queue cleanup interval initialized (30s interval)');
    await logger.logInfo(ErrorCategory.GENERAL, 'Queue cleanup interval initialized', {
      schedulerType: 'queue_cleanup',
      checkInterval: 30 * 1000 // 30 seconds
    });


    // Start periodic cleanup of ITEM_SOLD events only (BoundedCache handles its own cleanup)
    setInterval(() => {
      // Clear ITEM_SOLD events to prevent memory growth
      console.log(`Clearing ${recentItemSoldEvents.size} cached ITEM_SOLD events`);
      recentItemSoldEvents.clear();

      // Log cache statistics
      const orderStats = seenOrderHashes.getStats();
      const bidStats = seenBids.getStats();
      console.log(`[Cache Stats] seenOrderHashes: ${orderStats.size}/${orderStats.maxSize} items`);
      console.log(`[Cache Stats] seenBids: ${bidStats.size}/${bidStats.maxSize} items`);
    }, CLEANUP_INTERVAL);

    console.log(`Started periodic cleanup for ITEM_SOLD events (every ${CLEANUP_INTERVAL / 1000 / 60} minutes)`);

    // Check for stuck tasks periodically (every 30 seconds)
    setInterval(async () => {
      try {
        for (const [taskId, status] of Object.entries(taskStatus)) {
          // Check if task is stuck at 100% runningPercentage
          if (status.runningPercentage >= 100 && status.status === 'RUNNING') {
            const task = activeTasks.get(taskId);
            if (!task) continue;

            // Check if all marketplaces are actually complete
            const selectedMarketplaces = task.selectedMarketplaces.map(m => m.toLowerCase());
            const enabledAndSelectedMarketplaces = selectedMarketplaces.filter(m =>
              enabledMarketplaces.map(em => em.toLowerCase()).includes(m)
            );

            if (!completedTasks[taskId]) {
              console.log(YELLOW + `Task ${taskId} stuck at 100% - initializing completedTasks` + RESET);
              completedTasks[taskId] = {
                opensea: false,
                magiceden: false,
                blur: false
              };
            }

            // Check each marketplace completion
            let allComplete = true;
            for (const marketplace of enabledAndSelectedMarketplaces) {
              const parentId = `${taskId}-${marketplace}`.toLowerCase();
              const parentJob = parentJobs.get(parentId);

              if (parentJob) {
                const isComplete = parentJob.completedJobs + parentJob.failedJobs >= parentJob.totalJobs;
                completedTasks[taskId][marketplace as 'opensea' | 'magiceden' | 'blur'] = isComplete;
                if (!isComplete) {
                  allComplete = false;
                }
              }
            }

            // If all marketplaces are complete but task is stuck, transition it
            if (allComplete) {
              console.log(YELLOW + `Recovering stuck task ${taskId} at 100% completion` + RESET);
              taskStatus[taskId].status = 'COMPLETED';

              // Only schedule next loop if task is still running
              if (task.running) {
                const loopInterval = getExpiry(task.loopInterval);
                const nextLoopTime = new Date(Date.now() + loopInterval * 1000);
                const secondsUntilNextLoop = Math.floor((nextLoopTime.getTime() - Date.now()) / 1000);

                console.log(GREEN + `Scheduling recovered task ${taskId} to run again in ${loopInterval} seconds` + RESET);
                taskStatus[taskId].status = 'SCHEDULED';
                taskStatus[taskId].waitingTime = secondsUntilNextLoop;
                taskStatus[taskId].nextLoopTime = nextLoopTime;

                const timeoutId = setTimeout(async () => {
                  const currentTask = activeTasks.get(taskId);
                  if (currentTask?.running) {
                    console.log(GREEN + `[Timeout Handler] Auto-restarting recovered task ${taskId}` + RESET);
                    await startTask(currentTask, true);
                  }
                }, loopInterval * 1000);

                taskTimeouts.set(taskId, timeoutId);
              }
            }
          }
        }
      } catch (error) {
        console.error(RED + 'Error checking for stuck tasks:' + RESET, error);
      }
    }, 30000); // Check every 30 seconds

    console.log('Started periodic check for stuck tasks (every 30 seconds)');

    // Start internet connectivity monitoring
    startConnectivityMonitoring();

    // Note: Listing scheduler is already initialized above at line 749
    // The expiration monitor is also initialized at line 753

    await fetchCurrentTasks()


    // Check if password is set on startup
    const currentPassword = getPassword();
    if (!currentPassword) {
      console.log(YELLOW + '⚠️  No password set - all operations are paused' + RESET);
      console.log(YELLOW + 'Please set password via POST /api/auth/password to start operations' + RESET);
      console.log(YELLOW + 'Or set DEV_PASSWORD environment variable for development' + RESET);

      await pauseQueueAndWorkers();
    } else {
      console.log(GREEN + '✅ Password is already set - operations can proceed' + RESET);
      console.log(GREEN + '✅ Queue and workers are active' + RESET);
    }

  } catch (error) {
    console.error(RED + 'Failed to start server:' + RESET, error);
    process.exit(1);
  }
}

// Cleanup handler for graceful shutdown
process.on('SIGINT', async () => {
  console.log('\nReceived SIGINT, cleaning up...');

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

  // Stop queue cleanup interval
  if (queueCleanupInterval) {
    clearInterval(queueCleanupInterval);
    console.log('Queue cleanup interval stopped');
  }

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

  // Final cleanup before exit
  seenOrderHashes.clear();
  seenBids.clear();


  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.log('\nReceived SIGTERM, cleaning up...');

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

  // Stop queue cleanup interval
  if (queueCleanupInterval) {
    clearInterval(queueCleanupInterval);
    console.log('Queue cleanup interval stopped');
  }

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

  // Final cleanup before exit
  seenOrderHashes.clear();
  seenBids.clear();

  // Cleanup HTTP agents to close all connections
  // cleanupHttpAgents();

  process.exit(0);
});

startServer()
  .catch(async error => {
    await logger.logGeneralError('Failed to start server', {
      error: error as Error,
      errorMessage: error instanceof Error ? error.message : String(error)
    });
    console.error('Failed to start server:', error);
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

interface CompletedTasks {
  [key: string]: {
    opensea: boolean;
    magiceden: boolean;
    blur: boolean;
  };
}
const completedTasks: CompletedTasks = {};

// Create job options with appropriate retry logic based on job type
function createJobOptions(jobId?: string, jobName?: string): JobsOptions {
  // SCHEDULE jobs must always retry to prevent tasks from getting stuck
  const isScheduleJob = jobName && [OPENSEA_SCHEDULE, MAGICEDEN_SCHEDULE, BLUR_SCHEDULE].includes(jobName);

  // BID jobs should fail once with no retry for faster failure detection
  const isBidJob = jobName && jobName.includes('BID');

  return {
    ...(jobId ? { jobId } : {}),
    removeOnComplete: true,
    removeOnFail: true,
    lifo: false, // Change to FIFO (First In First Out) for fair processing
    attempts: isScheduleJob ? -1 : (isBidJob ? 1 : 3), // Unlimited retries for SCHEDULE jobs, 1 attempt for BID jobs, 3 retries for others
    backoff: {
      type: 'exponential',
      delay: 8000, // Start with 8 seconds for rate limits
    }
  };
}


// Helper function to ensure task transitions to STOPPED when cancellation is complete
function setTaskCancellationComplete(taskId: string) {
  const task = activeTasks.get(taskId)
  if (!task || !task.running) {
    if (!taskStatus[taskId]) {
      taskStatus[taskId] = {
        status: 'STOPPED',
        cancellationPercentage: 100,
        runningPercentage: 0,
        waitingTime: 0,
        processingPercentage: 0
      };
    } else {
      taskStatus[taskId].cancellationPercentage = 100;
      taskStatus[taskId].status = 'STOPPED';
      delete taskStatus[taskId].nextLoopTime;
      taskStatus[taskId].waitingTime = 0;
    }
  }
  
  // Ensure completedTasks is properly initialized
  if (!completedTasks[taskId]) {
    completedTasks[taskId] = {
      opensea: false,
      magiceden: false,
      blur: false
    };
  }
}

// Helper function for marketplace-specific cancellation completion
function setMarketplaceCancellationComplete(taskId: string, marketplace?: string) {
  // Initialize taskStatus if it doesn't exist
  if (!taskStatus[taskId]) {
    taskStatus[taskId] = {
      status: 'STOPPED',
      cancellationPercentage: 100,
      runningPercentage: 0,
      waitingTime: 0,
      processingPercentage: 0
    };
  }
  
  // Ensure completedTasks is properly initialized
  if (!completedTasks[taskId]) {
    completedTasks[taskId] = {
      opensea: false,
      magiceden: false,
      blur: false
    };
  }

  console.log(YELLOW + `[setMarketplaceCancellationComplete] Called for task ${taskId}` + RESET);
  console.log(YELLOW + `  Current status: ${taskStatus[taskId].status}` + RESET);
  console.log(YELLOW + `  Previous state: ${taskStatus[taskId].previousState || 'none'}` + RESET);
  console.log(YELLOW + `  Marketplace: ${marketplace || 'all'}` + RESET);

  taskStatus[taskId].cancellationPercentage = 100;

  // Restore previous state if this was a marketplace-specific cancellation
  if (taskStatus[taskId].previousState) {
    console.log(GREEN + `Restoring task ${taskId} to previous state: ${taskStatus[taskId].previousState}` + RESET);
    taskStatus[taskId].status = taskStatus[taskId].previousState;
    delete taskStatus[taskId].previousState;
    // Reset cancellation percentage after restoration
    taskStatus[taskId].cancellationPercentage = 0;

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
    // No previous state means full task cancellation
    console.log(YELLOW + `No previous state for task ${taskId}, proceeding with full cancellation` + RESET);
    setTaskCancellationComplete(taskId);
  }
}

// Track loop counters for debugging with cleanup
const taskLoopCounters: Map<string, number> = new Map();
const MAX_LOOP_COUNT = 1000; // Prevent unbounded growth

// Helper function to clean up task resources
function cleanupTaskResources(taskId: string): void {
  // Clear loop counter
  if (taskLoopCounters.has(taskId)) {
    taskLoopCounters.delete(taskId);
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

  // Clear task-specific stats objects
  delete skipStats[taskId];
  delete errorStats[taskId];
  delete floorPrices[taskId];
  delete warningBids[taskId];
  delete warningMessage[taskId];
  delete bestOffers[taskId];
}

// Helper function to update task running percentage
function updateTaskRunningPercentage(taskId: string) {
  const task = activeTasks.get(taskId);
  if (!task || !taskStatus[taskId]) return;

  // Get selected marketplaces that are also enabled
  const selectedMarketplaces = task.selectedMarketplaces.map(m => m.toLowerCase());
  const enabledAndSelectedMarketplaces = selectedMarketplaces.filter(m =>
    enabledMarketplaces.map(em => em.toLowerCase()).includes(m)
  );

  // Sum totals across all marketplaces
  let totalJobs = 0;
  let completedJobs = 0;
  let failedJobs = 0;
  const marketplaceBreakdown: Record<string, { total: number, completed: number, failed: number }> = {};

  // Initialize completedTasks tracking if needed
  if (!completedTasks[taskId]) {
    completedTasks[taskId] = {
      opensea: false,
      magiceden: false,
      blur: false
    };
  }

  for (const marketplace of enabledAndSelectedMarketplaces) {
    const marketplaceParentId = `${taskId}-${marketplace}`.toLowerCase();
    const marketplaceParentJob = parentJobs.get(marketplaceParentId);
    if (marketplaceParentJob) {
      totalJobs += marketplaceParentJob.totalJobs;
      completedJobs += marketplaceParentJob.completedJobs;
      failedJobs += marketplaceParentJob.failedJobs;

      marketplaceBreakdown[marketplace] = {
        total: marketplaceParentJob.totalJobs,
        completed: marketplaceParentJob.completedJobs,
        failed: marketplaceParentJob.failedJobs
      };

      // *** FIX: Update marketplace completion flags when parent job is complete ***
      const isMarketplaceComplete = (marketplaceParentJob.completedJobs + marketplaceParentJob.failedJobs) >= marketplaceParentJob.totalJobs;
      const wasComplete = completedTasks[taskId][marketplace as 'opensea' | 'magiceden' | 'blur'];

      if (isMarketplaceComplete && !wasComplete) {
        completedTasks[taskId][marketplace as 'opensea' | 'magiceden' | 'blur'] = true;
        console.log(GREEN + `[MARKETPLACE COMPLETE] ${marketplace.toUpperCase()} finished for task ${taskId} (${marketplaceParentJob.completedJobs + marketplaceParentJob.failedJobs}/${marketplaceParentJob.totalJobs} jobs)` + RESET);
      }

      // Update the flag regardless to ensure synchronization
      completedTasks[taskId][marketplace as 'opensea' | 'magiceden' | 'blur'] = isMarketplaceComplete;
    }
    // Parent job data collected
  }

  const runningPercentage = totalJobs > 0 ? ((completedJobs + failedJobs) / totalJobs) * 100 : 0;
  taskStatus[taskId].runningPercentage = Math.min(runningPercentage, 100);
  (taskStatus[taskId] as any).lastUpdate = Date.now(); // Track when task was last updated for stuck task monitoring

  // Check if task is complete and schedule next iteration
  if (runningPercentage >= 100) {
    checkTaskCompletionAndScheduleNext(taskId);
  }
}

// Helper function to update task cancellation percentage (similar to updateTaskRunningPercentage)
function updateTaskCancellationPercentage(taskId: string) {
  const task = activeTasks.get(taskId);
  if (!task || !taskStatus[taskId]) return;

  // Get selected marketplaces that are also enabled
  const selectedMarketplaces = task.selectedMarketplaces.map(m => m.toLowerCase());
  const enabledAndSelectedMarketplaces = selectedMarketplaces.filter(m =>
    enabledMarketplaces.map(em => em.toLowerCase()).includes(m)
  );

  // Sum totals across all cancellation parent jobs - now using bid counts
  let totalBids = 0;
  let completedBids = 0;
  let failedBids = 0;

  for (const marketplace of enabledAndSelectedMarketplaces) {
    const cancellationParentJobId = `${taskId}-CANCEL_${marketplace.toUpperCase()}_BID`;
    const cancellationParentJob = cancellationParentJobs.get(cancellationParentJobId);

    if (cancellationParentJob) {
      totalBids += cancellationParentJob.totalBids || 0;
      completedBids += cancellationParentJob.completedBids || 0;
      failedBids += cancellationParentJob.failedBids || 0;
    }
  }

  // Calculate percentage based on bids, not jobs
  const cancellationPercentage = totalBids > 0 ? ((completedBids + failedBids) / totalBids) * 100 : 0;
  taskStatus[taskId].cancellationPercentage = Math.min(cancellationPercentage, 100);
  taskStatus[taskId].status = 'CANCELLING'
}

// Helper function to cancel pending jobs for a task
async function cancelPendingJobsForTask(taskId: string): Promise<void> {
  try {
    console.log(YELLOW + `[cancelPendingJobsForTask] Cancelling pending jobs for task ${taskId}` + RESET);

    // Get all job types that might be associated with this task
    const jobNames = [
      'OPENSEA_TRAIT_BID', 'OPENSEA_TOKEN_BID', 'OPENSEA_COLLECTION_BID',
      'MAGICEDEN_TRAIT_BID', 'MAGICEDEN_TOKEN_BID', 'MAGICEDEN_COLLECTION_BID',
      'BLUR_TRAIT_BID', 'BLUR_COLLECTION_BID'
    ];

    let totalCancelled = 0;

    // Cancel waiting jobs for each job type
    for (const jobName of jobNames) {
      const waitingJobs = await queue.getJobs(['waiting']);
      const taskJobs = waitingJobs.filter(job =>
        job.name === jobName && job.data._id === taskId
      );

      for (const job of taskJobs) {
        try {
          await job.remove();
          totalCancelled++;
        } catch (error) {
          console.error(`[cancelPendingJobsForTask] Failed to cancel job ${job.id}:`, error);
        }
      }
    }

    if (totalCancelled > 0) {
      console.log(GREEN + `[cancelPendingJobsForTask] Cancelled ${totalCancelled} pending jobs for task ${taskId}` + RESET);
    }
  } catch (error) {
    console.error(`[cancelPendingJobsForTask] Error cancelling jobs for task ${taskId}:`, error);
  }
}

// Helper function to get active job count for a task
async function getActiveJobCount(taskId: string): Promise<number> {
  try {
    const task = activeTasks.get(taskId);
    if (!task) {
      return 0;
    }

    // Add small delay to allow job state transitions to complete
    await new Promise(resolve => setTimeout(resolve, 200));

    // Build job names based on selected marketplaces
    const jobNames: string[] = [];
    const selectedMarketplaces = task.selectedMarketplaces.map(m => m.toLowerCase());

    if (selectedMarketplaces.includes('opensea')) {
      jobNames.push('OPENSEA_TRAIT_BID', 'OPENSEA_TOKEN_BID', 'OPENSEA_COLLECTION_BID');
    }
    if (selectedMarketplaces.includes('magiceden')) {
      jobNames.push('MAGICEDEN_TRAIT_BID', 'MAGICEDEN_TOKEN_BID', 'MAGICEDEN_COLLECTION_BID');
    }
    if (selectedMarketplaces.includes('blur')) {
      jobNames.push('BLUR_TRAIT_BID', 'BLUR_COLLECTION_BID');
    }

    // If no marketplaces selected, return 0
    if (jobNames.length === 0) {
      return 0;
    }

    // Get counts once for all job types - include delayed jobs for more accurate count
    const counts = await queue.getJobCounts();
    const totalActiveInQueue = (counts.active || 0) + (counts.waiting || 0) + (counts.delayed || 0);

    // If no active jobs in queue, return early
    if (totalActiveInQueue === 0) {
      return 0;
    }

    // Get all active, waiting, and delayed jobs once
    const activeJobs = await queue.getJobs(['active', 'waiting', 'delayed']);

    // Count jobs that belong to this task
    let totalActive = 0;
    for (const jobName of jobNames) {
      const taskJobs = activeJobs.filter(job =>
        job && job.name === jobName && job.data && job.data._id === taskId
      );
      totalActive += taskJobs.length;
    }

    console.log(`[DEBUG] Active job count for task ${taskId}: ${totalActive} (total in queue: ${totalActiveInQueue})`);
    return totalActive;
  } catch (error) {
    console.error(`[getActiveJobCount] Error counting active jobs for task ${taskId}:`, error);
    return 0;
  }
}

// Helper function to check task completion and schedule next iteration
async function checkTaskCompletionAndScheduleNext(taskId: string) {
  const task = activeTasks.get(taskId);
  if (!task || !task.running) return;

  // Prevent duplicate scheduling
  if (tasksBeingScheduled.get(taskId)) {
    return;
  }

  // Get enabled marketplaces for this task
  const selectedMarketplaces = task.selectedMarketplaces.map(m => m.toLowerCase());
  const availableMarketplaces = selectedMarketplaces.filter(m =>
    enabledMarketplaces.map(em => em.toLowerCase()).includes(m)
  );

  // Check if all marketplaces are complete
  const allMarketsComplete = availableMarketplaces.every(m =>
    completedTasks[taskId]?.[m as 'opensea' | 'magiceden' | 'blur']
  );


  // Verify all marketplaces have parentJobs entries before considering task complete
  const allMarketsHaveParentJobs = availableMarketplaces.every(m => {
    const parentId = `${taskId}-${m}`.toLowerCase();
    return parentJobs.has(parentId);
  });

  if (!allMarketsHaveParentJobs) {
    return;
  }

  // *** FALLBACK FIX: Check parent job states directly if completion flags are out of sync ***
  let actualAllMarketsComplete = allMarketsComplete;
  if (!allMarketsComplete && taskStatus[taskId]?.runningPercentage >= 100) {
    console.log(YELLOW + `[FALLBACK] Task ${taskId} at 100% but allMarketsComplete is false, checking parent job states directly` + RESET);

    actualAllMarketsComplete = availableMarketplaces.every(marketplace => {
      const parentId = `${taskId}-${marketplace}`.toLowerCase();
      const parentJob = parentJobs.get(parentId);
      if (!parentJob) return true; // No parent job means no jobs for this marketplace

      const isComplete = (parentJob.completedJobs + parentJob.failedJobs) >= parentJob.totalJobs;
      // Ensure completedTasks[taskId] exists before accessing
      if (!completedTasks[taskId]) {
        completedTasks[taskId] = {
          opensea: false,
          magiceden: false,
          blur: false
        };
      }
      if (isComplete && !completedTasks[taskId][marketplace as 'opensea' | 'magiceden' | 'blur']) {
        console.log(GREEN + `[FALLBACK SYNC] Marking ${marketplace} as complete for task ${taskId} (${parentJob.completedJobs + parentJob.failedJobs}/${parentJob.totalJobs})` + RESET);
        completedTasks[taskId][marketplace as 'opensea' | 'magiceden' | 'blur'] = true;
      }
      return isComplete;
    });

    if (actualAllMarketsComplete) {
      console.log(GREEN + `[FALLBACK SUCCESS] Task ${taskId} completion flags synchronized, proceeding with completion` + RESET);
    }
  }

  console.log(`[DEBUG] Task ${taskId} completion check:`, {
    allMarketsComplete,
    actualAllMarketsComplete,
    runningPercentage: taskStatus[taskId]?.runningPercentage,
    allMarketsHaveParentJobs,
    availableMarketplaces,
    tasksBeingScheduled: tasksBeingScheduled.has(taskId),
    taskStatus: taskStatus[taskId]?.status,
    completedFlags: completedTasks[taskId]
  });

  if (actualAllMarketsComplete && taskStatus[taskId].runningPercentage >= 100) {
    console.log(GREEN + `[COMPLETION] Task ${taskId} reached 100% completion, starting completion process` + RESET);
    // Set flag to prevent duplicate scheduling
    tasksBeingScheduled.set(taskId, true);

    try {
      // Handle trait/token removal cancellations
      await handleTaskCleanup(taskId);

      // Update task status
      taskStatus[taskId].status = 'COMPLETED';

      await delay(1000)
      // Clear removal tracking
      delete traitsToRemove[taskId];
      delete removedTokens[taskId];

      // Cancel any pending jobs for this task
      await cancelPendingJobsForTask(taskId);

      // Wait for active jobs to complete before clearing parentJobs
      const activeJobCount = await getActiveJobCount(taskId);
      if (activeJobCount > 0) {
        // Check retry limits to prevent infinite loops
        const retryKey = `completion-retry-${taskId}`;
        const retryCount = completionRetries.get(retryKey) || 0;

        if (retryCount >= MAX_COMPLETION_RETRIES) {
          console.log(YELLOW + `[WARNING] Task ${taskId} exceeded completion retry limit (${MAX_COMPLETION_RETRIES}), forcing completion with ${activeJobCount} active jobs` + RESET);
          completionRetries.delete(retryKey);
          // Continue with completion instead of retrying
        } else {
          completionRetries.set(retryKey, retryCount + 1);
          console.log(`[DEBUG] Task ${taskId} has ${activeJobCount} active jobs, retry ${retryCount + 1}/${MAX_COMPLETION_RETRIES} in 5s`);
          // Set a timeout to check again later
          setTimeout(() => checkTaskCompletionAndScheduleNext(taskId), 5000);
          // Clear the scheduling flag and return early
          tasksBeingScheduled.delete(taskId);
          return;
        }
      } else {
        // Clear retry counter on successful completion
        const retryKey = `completion-retry-${taskId}`;
        completionRetries.delete(retryKey);
      }

      // Don't clear parentJobs here - let the next loop handle it
      // This prevents race conditions with jobs still executing

      // Clear completedTasks to ensure clean state for next loop
      delete completedTasks[taskId];

      // Wait before scheduling next loop (unless loopInterval is 0)
      const loopIntervalValue = typeof task.loopInterval === 'object' ? task.loopInterval.value : task.loopInterval;
      if (loopIntervalValue !== 0) {
        await new Promise(resolve => setTimeout(resolve, 1000));
      }

      // Clear the scheduling flag before calling scheduleNextLoop
      tasksBeingScheduled.delete(taskId);

      // Schedule next loop
      await scheduleNextLoop(taskId);
    } catch (error) {
      // Always clear the scheduling flag on error
      tasksBeingScheduled.delete(taskId);
    }
  } else {
    // Clear flag if we're not scheduling
    tasksBeingScheduled.delete(taskId);
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
function markJobAsProcessed(taskId: string, marketplace: string) {
  // Guard check: Skip if task is no longer active
  const task = activeTasks.get(taskId);
  if (!task || !task.running) {
    return;
  }

  const parentId = `${taskId}-${marketplace}`.toLowerCase();
  let parentJob = parentJobs.get(parentId);

  // Update OpenSea counter if needed
  if (marketplace === 'opensea') {
    openseaTraitBidCompletionCounter++;
  }

  // Create parent job if it doesn't exist (handles race conditions)
  if (!parentJob) {
    // Don't create parent job if task is already completed
    if (taskStatus[taskId]?.status === 'COMPLETED') {
      return;
    }

    // Only create missing parent job if task is still active and has this marketplace selected
    const selectedMarketplaces = task.selectedMarketplaces.map(m => m.toLowerCase());
    if (!selectedMarketplaces.includes(marketplace.toLowerCase())) {
      return;
    }

    parentJob = {
      parentId: parentId,
      totalJobs: 1,
      completedJobs: 0,
      failedJobs: 0,
      startTime: Date.now(),
      taskId: taskId,
      jobName: `${marketplace.toUpperCase()}_COLLECTION_BID`
    };
    parentJobs.set(parentId, parentJob);
  }

  // Always increment completedJobs - job has been processed regardless of outcome
  parentJob.completedJobs++;

  // Validate job counts don't exceed total
  const totalProcessed = parentJob.completedJobs + parentJob.failedJobs;
  if (totalProcessed > parentJob.totalJobs) {
    const overflow = totalProcessed - parentJob.totalJobs;
    parentJob.completedJobs -= overflow;
    // Overflow adjusted
  }

  // Ensure the parent job is saved back to the Map
  parentJobs.set(parentId, parentJob);

  // Update running percentage after marking job as processed
  updateTaskRunningPercentage(taskId);

  if (parentJob.completedJobs + parentJob.failedJobs === parentJob.totalJobs) {
    if (!completedTasks[taskId]) {
      completedTasks[taskId] = {
        opensea: false,
        magiceden: false,
        blur: false
      };
    }
    completedTasks[taskId][marketplace as 'opensea' | 'magiceden' | 'blur'] = true;
    // Task completed for marketplace
  }
}

// Helper function to schedule next loop iteration (simple version)
async function scheduleNextLoopSimple(taskId: string) {
  const task = activeTasks.get(taskId);
  if (!task || !task.running) return;

  let loopInterval = getExpiry(task.loopInterval);

  // Handle immediate rescheduling (loopInterval = 0)
  if (loopInterval === 0) {
    console.log(YELLOW + `Task ${taskId} has loopInterval of 0, rescheduling immediately` + RESET);
    loopInterval = 0.1; // 100ms delay to prevent blocking
  } else if (loopInterval < 0) {
    console.log(YELLOW + `Task ${taskId} has negative loopInterval (${loopInterval}), using minimum interval` + RESET);
    loopInterval = 1; // Minimum 1 second for negative values
  }

  // Check loop counter limits - counter is incremented during job creation, not scheduling
  const currentLoop = taskLoopCounters.get(taskId) || 0;
  if (currentLoop > MAX_LOOP_COUNT) {
    console.error(RED + `Task ${taskId} exceeded maximum loop count (${MAX_LOOP_COUNT}). Stopping task.` + RESET);
    await stopTask(task, false);
    return;
  }
  console.log(GREEN + `Scheduling task ${taskId} to run again in ${loopInterval} seconds (Loop #${currentLoop + 1})` + RESET);

  // Set up next loop time
  const nextLoopTime = new Date(Date.now() + loopInterval * 1000);
  const secondsUntilNextLoop = Math.floor((nextLoopTime.getTime() - Date.now()) / 1000);

  console.log(`[DEBUG waitingTime] scheduleNextLoopSimple - Before update:`, {
    taskId,
    currentStatus: taskStatus[taskId]?.status,
    currentWaitingTime: taskStatus[taskId]?.waitingTime,
    currentNextLoopTime: taskStatus[taskId]?.nextLoopTime,
    newNextLoopTime: nextLoopTime,
    newSecondsUntilNextLoop: secondsUntilNextLoop,
    loopInterval
  });

  taskStatus[taskId].status = 'SCHEDULED';
  taskStatus[taskId].waitingTime = secondsUntilNextLoop;
  taskStatus[taskId].nextLoopTime = nextLoopTime;

  console.log(`[DEBUG waitingTime] scheduleNextLoopSimple - After update:`, {
    taskId,
    status: taskStatus[taskId].status,
    waitingTime: taskStatus[taskId].waitingTime,
    nextLoopTime: taskStatus[taskId].nextLoopTime,
    nextLoopTimeType: typeof taskStatus[taskId].nextLoopTime,
    isDate: taskStatus[taskId].nextLoopTime instanceof Date
  });

  // Clear existing timers
  clearTaskTimers(taskId);

  // Set up countdown timer (skip for immediate rescheduling)
  if (loopInterval > 1) {
    const countdownTimer = setInterval(() => {
      try {
        // Ensure nextLoopTime is a Date object
        const nextLoop = taskStatus[taskId]?.nextLoopTime;

        console.log(`[DEBUG waitingTime] Countdown tick for task ${taskId}:`, {
          hasNextLoop: !!nextLoop,
          nextLoopValue: nextLoop,
          nextLoopType: typeof nextLoop,
          isDate: nextLoop instanceof Date,
          currentWaitingTime: taskStatus[taskId]?.waitingTime,
          taskExists: !!taskStatus[taskId]
        });

        if (!nextLoop) {
          console.log(`[DEBUG waitingTime] No nextLoop found for task ${taskId}, clearing timer`);
          clearInterval(countdownTimer);
          taskCountdownTimers.delete(taskId);
          return;
        }

        const nextLoopDate = nextLoop instanceof Date ? nextLoop : new Date(nextLoop);
        const now = Date.now();
        const remainingSeconds = Math.max(0, Math.floor((nextLoopDate.getTime() - now) / 1000));

        console.log(`[DEBUG waitingTime] Countdown calculation for task ${taskId}:`, {
          nextLoopDate: nextLoopDate.toISOString(),
          nextLoopDateMs: nextLoopDate.getTime(),
          nowMs: now,
          diffMs: nextLoopDate.getTime() - now,
          remainingSeconds,
          oldWaitingTime: taskStatus[taskId]?.waitingTime
        });

        if (taskStatus[taskId]) {
          taskStatus[taskId].waitingTime = remainingSeconds;

          console.log(`[DEBUG waitingTime] Updated waitingTime for task ${taskId}:`, {
            newWaitingTime: taskStatus[taskId].waitingTime,
            status: taskStatus[taskId].status
          });
        }

        if (remainingSeconds <= 0) {
          console.log(`[DEBUG waitingTime] Countdown reached 0 for task ${taskId}, clearing timer`);
          clearInterval(countdownTimer);
          taskCountdownTimers.delete(taskId);
        }
      } catch (error) {
        console.error(`[DEBUG waitingTime] Error updating countdown for task ${taskId}:`, error);
        clearInterval(countdownTimer);
        taskCountdownTimers.delete(taskId);
      }
    }, 1000);

    taskCountdownTimers.set(taskId, countdownTimer);
    console.log(`[DEBUG waitingTime] Countdown timer SET for task ${taskId}, loopInterval: ${loopInterval}`);
  } else {
    console.log(`[DEBUG waitingTime] Countdown timer SKIPPED for task ${taskId}, loopInterval: ${loopInterval} (<=1)`);
  }

  // Validate task is still active
  if (!activeTasks.has(taskId)) {
    console.log(YELLOW + `Task ${taskId} no longer active, skipping loop scheduling` + RESET);
    cleanupTaskResources(taskId);
    return;
  }

  // Acquire scheduling lock
  if (!acquireLoopSchedulingLock(taskId)) {
    console.log(YELLOW + `Task ${taskId} already has a pending loop schedule, skipping duplicate scheduling` + RESET);
    return;
  }

  try {
    // Schedule the next iteration
    const timeoutId = setTimeout(async () => {
      console.log(YELLOW + `[Loop Timer] Timeout fired for task ${taskId}, loop #${currentLoop}` + RESET);
      try {
        const currentTask = activeTasks.get(taskId);
        if (currentTask?.running) {
          console.log(YELLOW + `[Loop ${currentLoop}] Starting next iteration for task ${taskId}` + RESET);
          await startTask(currentTask, true);
        } else {
          console.log(YELLOW + `Task ${taskId} no longer running, cleaning up...` + RESET);
          cleanupTaskResources(taskId);
        }
      } catch (error) {
        console.error(RED + `Error in loop scheduling for task ${taskId}: ` + RESET, error);
        cleanupTaskResources(taskId);
      } finally {
        // Always release the lock after execution
        releaseLoopSchedulingLock(taskId);
      }
    }, loopInterval * 1000);

    taskTimeouts.set(taskId, timeoutId);

    // Release the lock immediately after scheduling - the timeout has been set
    releaseLoopSchedulingLock(taskId);
    console.log(GREEN + `[scheduleNextLoopSimple] Released scheduling lock for task ${taskId} after scheduling` + RESET);
  } catch (error) {
    // If scheduling fails, ensure we release the lock
    console.error(RED + `[scheduleNextLoopSimple] Error scheduling task ${taskId}: ` + RESET, error);
    releaseLoopSchedulingLock(taskId);
    cleanupTaskResources(taskId);
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


async function scheduleNextLoop(taskId: string): Promise<void> {
  console.log(GREEN + `[scheduleNextLoop] Starting loop scheduling for task ${taskId}` + RESET);

  // Get the task from activeTasks
  const task = activeTasks.get(taskId);
  if (!task) {
    console.log(YELLOW + `[scheduleNextLoop] Task ${taskId} not found in activeTasks` + RESET);
    return;
  }

  // Calculate loopInterval with getExpiry()
  let loopInterval = getExpiry(task.loopInterval);
  console.log(YELLOW + `[scheduleNextLoop] Task ${taskId} - Initial loopInterval: ${loopInterval} seconds` + RESET);
  console.log(YELLOW + `[scheduleNextLoop] Task ${taskId} - loopInterval config: ${JSON.stringify(task.loopInterval)}` + RESET);

  // Handle immediate rescheduling when loopInterval = 0
  if (loopInterval <= 0) {
    console.log(YELLOW + `[scheduleNextLoop] Task ${taskId} has loopInterval of ${loopInterval}, using minimum interval of 1 second` + RESET);
    loopInterval = 1; // Minimum 1 second to prevent tight loops
  }

  // Increment loop counter here in scheduleNextLoop (not in startTask)
  const currentLoop = taskLoopCounters.get(taskId) || 0;
  const newLoopCount = currentLoop + 1;
  taskLoopCounters.set(taskId, newLoopCount);
  console.log(GREEN + `[scheduleNextLoop] Task ${taskId} - Loop counter incremented: ${currentLoop} -> ${newLoopCount}` + RESET);

  if (newLoopCount > MAX_LOOP_COUNT) {
    console.error(RED + `[scheduleNextLoop] Task ${taskId} exceeded maximum loop count (${MAX_LOOP_COUNT}). Stopping task.` + RESET);
    await stopTask(task, false);
    return;
  }

  // Set up next loop time and countdown timer
  const nextLoopTime = new Date(Date.now() + loopInterval * 1000);
  const secondsUntilNextLoop = Math.floor((nextLoopTime.getTime() - Date.now()) / 1000);

  console.log(GREEN + `[scheduleNextLoop] Scheduling task ${taskId} to run in ${loopInterval} seconds (Loop #${newLoopCount})` + RESET);
  console.log(GREEN + `[scheduleNextLoop] Next loop time: ${nextLoopTime.toISOString()}` + RESET);

  // Update task status
  if (taskStatus[taskId]) {
    taskStatus[taskId].status = 'SCHEDULED';
    taskStatus[taskId].waitingTime = secondsUntilNextLoop;
    taskStatus[taskId].nextLoopTime = nextLoopTime;
  }

  // Clear any existing countdown timer before setting a new one
  const existingCountdownTimer = taskCountdownTimers.get(taskId);
  if (existingCountdownTimer) {
    console.log(YELLOW + `[scheduleNextLoop] Clearing existing countdown timer for task ${taskId}` + RESET);
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
  console.log(GREEN + `[scheduleNextLoop] Countdown timer set for task ${taskId}` + RESET);

  // Clear any existing timeout for this task
  const existingTimeout = taskTimeouts.get(taskId);
  if (existingTimeout) {
    console.log(YELLOW + `[scheduleNextLoop] Clearing existing timeout for task ${taskId}` + RESET);
    clearTimeout(existingTimeout);
    taskTimeouts.delete(taskId);
  }

  // Validate task is still active before scheduling
  if (!activeTasks.has(taskId)) {
    console.log(YELLOW + `[scheduleNextLoop] Task ${taskId} no longer active, skipping loop scheduling` + RESET);
    cleanupTaskResources(taskId);
    return;
  }

  // Acquire scheduling lock
  if (!acquireLoopSchedulingLock(taskId)) {
    console.log(YELLOW + `[scheduleNextLoop] Task ${taskId} already has a pending loop schedule, skipping duplicate scheduling` + RESET);
    return;
  }

  console.log(GREEN + `[scheduleNextLoop] Acquired scheduling lock for task ${taskId}` + RESET);

  try {
    // Schedule the next iteration with setTimeout
    const timeoutId = setTimeout(async () => {
      console.log(YELLOW + `[scheduleNextLoop] Timeout fired for task ${taskId}, loop #${newLoopCount}` + RESET);
      try {
        // Double-check task is still active and running
        const currentTask = activeTasks.get(taskId);
        if (currentTask?.running) {
          console.log(YELLOW + `[scheduleNextLoop] Starting next iteration for task ${taskId} (Loop #${newLoopCount})` + RESET);
          console.log(YELLOW + `[scheduleNextLoop] Task running state: ${currentTask.running}, Task ID: ${taskId}` + RESET);
          await startTask(currentTask, true);
        } else if (currentTask) {
          console.log(YELLOW + `[scheduleNextLoop] Task ${taskId} no longer running (running=${currentTask.running}), stopping...` + RESET);
          await stopTask(currentTask, false);
        } else {
          console.log(YELLOW + `[scheduleNextLoop] Task ${taskId} not found in activeTasks, cleaning up...` + RESET);
          cleanupTaskResources(taskId);
        }
      } catch (error) {
        console.error(RED + `[scheduleNextLoop] Error in loop execution for task ${taskId}: ` + RESET, error);
        cleanupTaskResources(taskId);
      } finally {
        // Always release the lock after execution
        releaseLoopSchedulingLock(taskId);
      }
    }, loopInterval * 1000);

    taskTimeouts.set(taskId, timeoutId);
    console.log(GREEN + `[scheduleNextLoop] Timeout set for task ${taskId}, will fire in ${loopInterval} seconds` + RESET);

    // Release the lock immediately after scheduling - the timeout has been set
    releaseLoopSchedulingLock(taskId);
    console.log(GREEN + `[scheduleNextLoop] Released scheduling lock for task ${taskId} after scheduling` + RESET);
    console.log(GREEN + `[scheduleNextLoop] Successfully scheduled next loop for task ${taskId}` + RESET);
  } catch (error) {
    // If scheduling fails, ensure we release the lock
    console.error(RED + `[scheduleNextLoop] Error scheduling task ${taskId}: ` + RESET, error);
    releaseLoopSchedulingLock(taskId);
    cleanupTaskResources(taskId);
  }
}

// Initialize queue event handlers - called after queue is created
function initializeQueueEventHandlers() {
  queueEvents.on('completed', async ({ jobId }) => {
    // Cancellation job completion is now handled in the bulk cancel functions

    if (jobId.split('-').length < 3) return;

    const taskId = jobId.split('-')[0];

    const task = activeTasks.get(taskId);

    if (!task) return;

    const marketplace = jobId.split('-')[1].toLowerCase();

    const parentId = `${taskId}-${marketplace}`.toLowerCase();

    const { currencySymbol } = await calculateBidPrice(task, 0, "magiceden")


    // Job completion is now handled in individual process functions
    // This event handler should only handle cancellation-related tasks

  });

  queueEvents.on('failed', async ({ jobId }) => {
    // Cancellation job failures are now handled in the bulk cancel functions

    if (jobId.split('-').length < 3) return;

    const taskId = jobId.split('-')[0];
    const task = activeTasks.get(taskId);

    if (!task || !task.running) return;

    const marketplace = jobId.split('-')[1].toLowerCase();
    const parentId = `${taskId}-${marketplace}`.toLowerCase();

    const { currencySymbol } = await calculateBidPrice(task, 0, "magiceden")

    // Job failure is now handled in individual process functions
    // This event handler should only handle cancellation-related tasks

    // Handle failed bid jobs
    if (jobId.startsWith('bid-')) {
      const parts = jobId.split('-');
      if (parts.length >= 3) {
        const taskId = parts[1];
        const marketplace = parts[2];

        // Validate task is still active
        const task = activeTasks.get(taskId);
        if (task && task.running) {
          console.log(`[Failed Bid Job] Marking as processed - Task: ${taskId} | Marketplace: ${marketplace.toUpperCase()} | JobID: ${jobId}`);
          markJobAsProcessed(taskId, marketplace);
        }
      }
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
  taskStatus
};

const BROADCAST_INTERVAL = 1000;
setInterval(broadcastBidRates, BROADCAST_INTERVAL);

function broadcastBidRates() {
  bidRates = getAllBidRates();

  // Enforce STOPPED status for any task with 100% cancellation before broadcasting
  // BUT skip this for marketplace-specific cancellations (those with previousState)
  Object.keys(taskStatus).forEach(taskId => {
    if (taskStatus[taskId].cancellationPercentage >= 100 &&
      taskStatus[taskId].status !== 'STOPPED' &&
      !taskStatus[taskId].previousState) {
      console.log(`Broadcast enforcement: Setting task ${taskId} to STOPPED (cancellation: ${taskStatus[taskId].cancellationPercentage}%)`);
      setTaskCancellationComplete(taskId);
    }
  });

  const message = JSON.stringify({
    type: 'bidRatesUpdate',
    data: { bidRates, bidCounts: bidStats, skipCounts: skipStats, errorCounts: errorStats, floorPrices, bestOffers, warningBids, warningMessage, rps, taskStatus, ownBidCounts }
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


async function processJob(job: Job, signal?: AbortSignal) {
  try {
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
          // Processing OPENSEA_SCHEDULE
          await processOpenseaScheduledBid(job.data, job.id as string);
          return { completed: true };
        case OPENSEA_TRAIT_BID:
          await processOpenseaTraitBid(job.data);
          return { completed: true };
        case OPENSEA_TOKEN_BID:
          await processOpenseaTokenBid(job.data);
          return { completed: true };
        case BLUR_SCHEDULE:
          // Processing BLUR_SCHEDULE
          await processBlurScheduledBid(job.data);
          return { completed: true };
        case MAGICEDEN_SCHEDULE:
          // Processing MAGICEDEN_SCHEDULE
          await processMagicedenScheduledBid(job.data, job.id as string);
          return { completed: true };
        case MAGICEDEN_TRAIT_BID:
          await processMagicedenTraitBid(job.data);
          return { completed: true };
        case MAGICEDEN_TOKEN_BID:
          await processMagicedenTokenBid(job.data);
          return { completed: true };
        case CANCEL_OPENSEA_BID:
          await bulkCancelOpenseaBid(job.data);
          return { completed: true };
        case CANCEL_MAGICEDEN_BID:
          await bulkCancelMagicedenBid(job.data);
          return { completed: true };
        case CANCEL_BLUR_BID:
          await bulkCancelBlurBid(job.data);
          return { completed: true };
        case BLUR_COLLECTION_BID:
          await processBlurCollectionBid(job.data);
          return { completed: true };
        case BLUR_TRAIT_BID:
          await processBlurTraitBid(job.data);
          return { completed: true };
        case BLUR_COLLECTION_BID_COUNTERBID:
          await blurCollectionCounterBid(job.data);
          return { completed: true };
        case BLUR_TRAIT_BID_COUNTERBID:
          await blurTraitCounterBid(job.data);
          return { completed: true };
        case PROCESS_LISTING:
          return await processListingJob(job);
        case PROCESS_LISTING_UPDATE:
          return await processListingUpdateJob(job);
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
  loopCount: number;
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


    const jobs = tasks.flatMap(task => [
      ...(task.selectedMarketplaces.map(m => m.toLowerCase()).includes("opensea") ? [{ name: OPENSEA_SCHEDULE, data: task, opts: { jobId: `${task._id}-opensea-schedule` } }] : []),
      ...(task.selectedMarketplaces.map(m => m.toLowerCase()).includes("blur") ? [{ name: BLUR_SCHEDULE, data: task, opts: { jobId: `${task._id}-blur-schedule` } }] : []),
      ...(task.selectedMarketplaces.map(m => m.toLowerCase()).includes("magiceden") ? [{ name: MAGICEDEN_SCHEDULE, data: task, opts: { jobId: `${task._id}-magiceden-schedule` } }] : []),
    ]);

    if (jobs.length > 0) {
      await queue.addBulk(jobs)
      console.log(`Successfully added ${jobs.length} jobs to the queue.`);

      // Log each job added
      for (const job of jobs) {
        await logger.logQueueJobAdded(job.name, {
          jobName: job.name,
          taskId: job.data._id,
          taskName: (job.data as any).name || job.data.contract?.slug,
          marketplace: job.name.replace('_SCHEDULE', '').toLowerCase(),
          queueName: QUEUE_NAME
        });
      }
    }

    // Subscribe all running tasks to WebSocket (WebSocket should already be connected at startup)
    const runningTasks = tasks.filter(task => task.running).map(task => ({ ...task, running: true }));

    if (runningTasks.length > 0 && wsManager?.isConnected()) {
      console.log(GREEN + `Subscribing to ${runningTasks.length} collections` + RESET);
      subscribeToCollections(runningTasks);
    } else if (runningTasks.length > 0) {
      console.log(YELLOW + "WebSocket not connected, skipping task subscription" + RESET);
    }

    console.log(GREEN + `=== Imported Tasks Processing Complete ===\n` + RESET);
  } catch (error) {
    console.error(RED + `Error processing imported tasks` + RESET, error);
  }
}


async function processNewTask(task: ITask) {
  try {
    // Add to activeTasks Map
    activeTasks.set(task._id, task)
    currentTasks.push(task);
    updateTaskIndexes(currentTasks);
    const jobs = [
      ...(task.selectedMarketplaces.map(m => m.toLowerCase()).includes("opensea") ? [{ name: OPENSEA_SCHEDULE, data: task, opts: { jobId: `${task._id}-opensea-schedule` } }] : []),
      ...(task.selectedMarketplaces.map(m => m.toLowerCase()).includes("blur") ? [{ name: BLUR_SCHEDULE, data: task, opts: { jobId: `${task._id}-blur-schedule` } }] : []),
      ...(task.selectedMarketplaces.map(m => m.toLowerCase()).includes("magiceden") ? [{ name: MAGICEDEN_SCHEDULE, data: task, opts: { jobId: `${task._id}-magiceden-schedule` } }] : []),
    ];

    if (jobs.length > 0) {
      await queue.addBulk(jobs)
      console.log(`Successfully added ${jobs.length} jobs to the queue.`);

      // Log each job added
      for (const job of jobs) {
        await logger.logQueueJobAdded(job.name, {
          jobName: job.name,
          taskId: job.data._id,
          taskName: (job.data as any).name || job.data.contract?.slug,
          marketplace: job.name.replace('_SCHEDULE', '').toLowerCase(),
          queueName: QUEUE_NAME
        });
      }
    }

    // Subscribe to WebSocket collection (WebSocket should already be connected at startup)
    try {
      if (wsManager?.isConnected()) {
        // console.log("subscribing to collection: ", task.contract.slug);
        const newTask = { ...task, running: true };
        subscribeToCollections([newTask]);
      } else {
        console.log(YELLOW + `WebSocket not connected, skipping subscription for ${task.contract.slug}` + RESET);
      }
    } catch (error) {
      console.error(RED + `Error subscribing to collection ${task.contract.slug}: ` + RESET, error);
    }

    console.log(GREEN + `=== New Task Processing Complete ===\n` + RESET);
  } catch (error) {
    console.error(RED + `Error processing new task: ${task.contract.slug}` + RESET, error);
  }
}

async function processGlobalMarketplaces(incomingMarketplaces: string[]) {
  try {
    const tasks: ITask[] = Array.from(activeTasks.values());
    const newMarketplaces = incomingMarketplaces.map((m) => m.toLowerCase()).filter(m => !enabledMarketplaces.map((m) => m.toLowerCase()).includes(m));
    
    // Process each new marketplace
    for (const marketplace of newMarketplaces) {
      const tasksToProcess = tasks.filter(task => 
        task.running && task.selectedMarketplaces.map((m) => m.toLowerCase()).includes(marketplace.toLowerCase())
      );
      
      console.log(GREEN + `[processGlobalMarketplaces] Adding marketplace ${marketplace} to ${tasksToProcess.length} tasks` + RESET);
      
      // Use start=false for marketplace-specific additions (not a loop restart)
      await Promise.all(tasksToProcess.map(async (task) => {
        try {
          await startTask(task, false, marketplace.toLowerCase());
        } catch (error) {
          console.error(RED + `Error adding marketplace ${marketplace} to task ${task._id}: ` + RESET, error);
        }
      }));
    }
    
    enabledMarketplaces = incomingMarketplaces.map((m) => m.toLowerCase())
  } catch (error) {
    console.error(RED + `Error processing global marketplaces: ` + RESET, error);
  }
}

async function cancelRemovedTokenBids(removedTokens: { opensea: string[]; magiceden: string[] }, taskId: string, walletPrivateKey: string, currencySymbol: string): Promise<number> {
  let totalJobsCreated = 0;
  try {
    const openseaTokensToCancel = await prisma.order.findMany({
      where: {
        taskId: taskId,
        identifier: { in: removedTokens.opensea },
        marketplace: "opensea"
      }
    })

    const magicedenTokensToCancel = await prisma.order.findMany({
      where: {
        taskId: taskId,
        identifier: { in: removedTokens.magiceden },
        marketplace: "magiceden"
      }
    })

    if (openseaTokensToCancel.length > 0) {
      const cancelData = openseaTokensToCancel.map((order) => {
        return {
          name: CANCEL_OPENSEA_BID,
          data: { privateKey: walletPrivateKey, orderId: order.orderHash, taskId, orderKey: order.id }
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
      const orderKeys = batchOrders.map((order: PrismaOrder) => order.id)
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
    const ordersToCancel = await prisma.order.findMany({
      where: {
        taskId: taskId,
        identifier: { in: traitsToRemove }
      }
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
          data: { privateKey: walletPrivateKey, payload, taskId, orderKey: order.id }
        };
      });
      await queue.addBulk(cancelData);
      totalJobsCreated += cancelData.length;
    }

    if (openseaOrders.length > 0) {
      const cancelData = openseaOrders.map((order) => {
        return {
          name: CANCEL_OPENSEA_BID,
          data: { privateKey: walletPrivateKey, orderId: order.orderHash, taskId, orderKey: order.id }
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
      const orderKeys = batchOrders.map((order: PrismaOrder) => order.id)
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
    const taskTokenIds = currentTask?.tokenIds ?? []

    const autoIds = existingTask.tokenIds
      .filter(id => id.toString().toLowerCase().startsWith('bot'))
      .map(id => {
        const matches = id.toString().match(/\d+/);
        return matches ? parseInt(matches[0]) : null;
      })
      .filter(id => id !== null);

    const amount = autoIds[0]
    let openseaTokenIds: number[] = []
    let magicedenTokenIds: number[] = []

    if (!currentTokenIds[task._id]) {
      currentTokenIds[task._id] = {
        opensea: [],
        magiceden: []
      }
    }

    const previousOpenseaTokens = [...currentTokenIds[task._id]?.opensea, ...existingTask.tokenIds]
    const previousMagicedenTokens = [...currentTokenIds[task._id]?.magiceden, ...existingTask.tokenIds]

    const openseaBottonListings = autoIds ? await fetchOpenseaListings(task._id, task.contract.slug, amount) ?? [] : []
    const magicedenBottomListings = amount ? await fetchMagicEdenTokens(task._id, task.contract.contractAddress, amount) ?? [] : []

    if (task.bidType === "token" && selectedTraits && Object.keys(selectedTraits).length > 0) {
      const attributes = transformSelectedTraitsToAttributes(selectedTraits)
      const expandedAttributes = expandAttributes(attributes)
      // Initialize tokens with bottomListings and taskTokenIds
      let openseaTokens: number[] = [...openseaBottonListings, ...taskTokenIds.map((token) => +token)]
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
            magicedenTokens.push(...items)
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
        magicedenTokens.push(...items)
      }

      openseaTokens = [...openseaBottonListings, ...taskTokenIds.map((token) => +token), ...openseaTokens];
      openseaTokenIds = [...new Set(openseaTokens)];

      magicedenTokens = [...magicedenBottomListings, ...taskTokenIds.map((token) => +token), ...magicedenTokens]
      magicedenTokenIds = [...new Set(magicedenTokens)]

    } else if (task.bidType === "token") {
      openseaTokenIds = [...new Set([...openseaBottonListings, ...taskTokenIds.map((token) => +token)])];
      magicedenTokenIds = [...new Set([...magicedenBottomListings, ...taskTokenIds.map((token) => +token)])];
    }

    const openseaTokensToRemove = previousOpenseaTokens.filter(token => !openseaTokenIds.includes(+token)).map((token) => token.toString())
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

const MAX_MEMORY_USAGE = 75;
const DRAIN_THRESHOLD = RATE_LIMIT * 2;

const BLUR_JOBS = [
  'BLUR_COLLECTION_BID',
  'BLUR_COLLECTION_BID_COUNTERBID',
  'BLUR_TRAIT_BID_COUNTERBID',
  'BLUR_TRAIT_BID'
];

async function processBulkCancellations(jobs: Job[]) {
  if (!jobs?.length) return;

  const rateLimiter = new RateLimiter(RATE_LIMIT * 2);
  const chunks = chunk(jobs, RATE_LIMIT * 2);

  try {
    for (const [chunkIndex, currentChunk] of chunks.entries()) {
      try {
        const memUsage = process.memoryUsage();
        const memoryUsagePercent = (memUsage.heapUsed / os.totalmem()) * 100;

        if (memoryUsagePercent > MAX_MEMORY_USAGE) {
          console.log(YELLOW + `Memory usage high (${memoryUsagePercent.toFixed(1)}%). Waiting for GC...` + RESET);
          await new Promise(resolve => setTimeout(resolve, 5000));
          global.gc?.();
          continue;
        }

        while (true) {
          const counts = await queue.getJobCounts();
          const prioritizedCount = counts.prioritized || 0;

          if (prioritizedCount <= DRAIN_THRESHOLD * 2) {
            break;
          }
          const delay = (prioritizedCount / DRAIN_THRESHOLD * 2);
          await new Promise(resolve => setTimeout(resolve, delay * 2000));
        }

        // Check queue health
        const counts = await queue.getJobCounts();
        const prioritizedprioritizedCount = counts.prioritized || 0;

        if (prioritizedprioritizedCount > DRAIN_THRESHOLD) {
          await waitForQueueDrain();
        }

        let retries = 3;
        while (retries > 0) {
          try {
            await rateLimiter.acquire();
            await queue.addBulk(
              currentChunk
                .filter((job): job is NonNullable<typeof job> => job !== null)
                .map(job => ({
                  name: job.name,
                  data: job.data,
                  opts: {
                    ...job?.opts,
                    removeOnComplete: true,
                    removeOnFail: true, // Keep failed jobs for debugging
                  },
                }))
            );

            break;
          } catch (error) {
            retries--;
            if (retries === 0) {
              throw error;
            }
            console.log(YELLOW + `Retrying addBulk for chunk ${chunkIndex + 1}. Attempts remaining: ${retries}` + RESET);
            await new Promise(resolve => setTimeout(resolve, 2000));
          }
        }
        await new Promise(resolve => setTimeout(resolve, 1000));
      } catch (error) {
        console.log(RED + `Error processing chunk ${chunkIndex + 1}:`, error, RESET);
      }
    }
  } catch (error) {
    console.log(RED + 'Error processing bulk jobs:', error, RESET);
  }
}

async function processBulkJobs(jobs: Job[], createKey = false) {
  if (!jobs?.length) return;

  const firstJob = jobs[0];
  const taskId = firstJob.data._id;
  const marketplace = firstJob.name.split('_')[0] || 'UNKNOWN';
  const bulkJobId = `${firstJob.data._id}-${marketplace}`.toLowerCase();
  const task = activeTasks.get(taskId);

  // Only set parentJobs if it doesn't already exist
  if (!parentJobs.has(bulkJobId)) {
    parentJobs.set(bulkJobId, {
      parentId: bulkJobId,
      totalJobs: jobs.length,
      completedJobs: 0,
      failedJobs: 0,
      startTime: Date.now(),
      taskId,
      jobName: firstJob.name
    })
  } else {
    const existingParent = parentJobs.get(bulkJobId);
  }


  const tracker: BulkJobTracker = {
    taskId: firstJob.data._id,
    jobName: firstJob.name,
    totalJobs: jobs.length,
    marketplace, // Added marketplace field
    processedJobs: 0,
    completedJobs: 0,
    failedJobs: 0,
    startTime: Date.now(),
    status: 'processing',
    percentComplete: 0
  };

  bulkJobTrackers.set(bulkJobId, tracker);

  if (!task || !task.running) return
  const rateLimiter = new RateLimiter(RATE_LIMIT);
  const chunks = chunk(jobs, RATE_LIMIT);

  let totalProcessed = 0;
  let totalAdded = 0;
  let totalFailed = 0;

  try {
    for (const [chunkIndex, currentChunk] of chunks.entries()) {
      try {
        const memUsage = process.memoryUsage();
        const memoryUsagePercent = (memUsage.heapUsed / os.totalmem()) * 100;

        if (memoryUsagePercent > MAX_MEMORY_USAGE) {
          console.log(YELLOW + `Memory usage high (${memoryUsagePercent.toFixed(1)}%). Waiting for GC...` + RESET);
          await new Promise(resolve => setTimeout(resolve, 5000));
          global.gc?.();
          continue;
        }

        // Add timeout to prevent infinite waiting
        const MAX_WAIT_TIME = 30000; // 30 seconds max wait
        const startWait = Date.now();

        while (Date.now() - startWait < MAX_WAIT_TIME) {
          const counts = await queue.getJobCounts();
          const prioritizedCount = counts.prioritized || 0;

          if (prioritizedCount <= DRAIN_THRESHOLD) {
            break;
          }

          // Fixed 1 second delay instead of variable delay
          await new Promise(resolve => setTimeout(resolve, 1000));
        }

        // Check queue health
        const counts = await queue.getJobCounts();
        const prioritizedCount = counts.prioritized || 0;

        if (prioritizedCount > DRAIN_THRESHOLD) {
          await waitForQueueDrain();
        }

        totalProcessed += currentChunk.length;
        tracker.processedJobs = totalProcessed;
        tracker.percentComplete = Math.round((totalProcessed / tracker.totalJobs) * 100);
        bulkJobTrackers.set(bulkJobId, tracker);

        const validJobs = await Promise.all(currentChunk.map(async (job) => {
          if (!job?.name || !job?.data) return null;

          const taskId = job?.data?._id;
          if (taskId && !activeTasks.get(taskId)?.running) return null;

          const isCancel = job.name.toLowerCase().includes('cancel');
          if (isCancel) {
            return {
              ...job,
              jobKey: undefined
            };
          }

          const jobKey = createKey ? createJobKey(job) : generateJobKey(job);

          return {
            ...job,
            jobKey
          };
        }));
        const filteredJobs = validJobs.filter(Boolean);
        if (!filteredJobs.length) {
          continue;
        }
        // Filter out jobs for stopped tasks
        const activeJobs = currentChunk.filter(job => {
          const taskId = job?.data?._id;
          if (!taskId) return true; // Keep jobs without taskId

          const task = activeTasks.get(taskId);
          if (task && !task?.running) {
            return false;
          }
          return true;
        });

        if (!activeJobs.length) {
          continue;
        }
        if (task && task.running) {
          await safeUpdateTaskStatus(taskId, {
            status: 'QUEUEING'
          });
        }
        let retries = 3;
        const blurJobs = filteredJobs.filter((job): job is NonNullable<typeof job> => job !== null).filter(job => BLUR_JOBS.includes(job.name));
        const nonBlurJobs = filteredJobs.filter((job): job is NonNullable<typeof job> => job !== null).filter(job => !BLUR_JOBS.includes(job.name));

        while (retries > 0) {
          try {
            await rateLimiter.acquire();
            // Add non-Blur jobs first
            const jobsToAdd = nonBlurJobs.map(job => {
              const jobOpts = {
                ...(job.jobKey ? { jobId: job.jobKey } : {}),
                ...job?.opts,
                removeOnComplete: true,
                removeOnFail: true
              };
              return {
                name: job.name,
                data: job.data,
                opts: jobOpts,
              };
            });


            const addedJobs = await queue.addBulk(jobsToAdd);

            // Group blur jobs by wallet address
            const blurJobsByWallet = new Map<string, typeof blurJobs>();
            const uniqueBlurJobs = new Set<string>();

            for (const job of blurJobs) {
              const wallet = job.data.address;
              // Create a unique identifier for this job
              const jobIdentifier = `${job.name}-${job.data._id}-${job.data.trait}`;

              if (uniqueBlurJobs.has(jobIdentifier)) {
                continue;
              }
              uniqueBlurJobs.add(jobIdentifier);

              if (!blurJobsByWallet.has(wallet)) {
                blurJobsByWallet.set(wallet, []);
              }
              blurJobsByWallet.get(wallet)?.push(job);
            }

            // Process Blur jobs with rate limiting
            let blurJobsProcessed = 0;
            const MAX_BLUR_JOBS_PER_BATCH = 10; // Limit jobs per batch to avoid overwhelming the API

            for (const [wallet, walletJobs] of blurJobsByWallet.entries()) {
              // Check rate limit before processing wallet's jobs
              if (!blurRateLimiter.canMakeRequest(wallet)) {
                const waitTime = Math.max(blurRateLimiter.getWaitTime(wallet), 2500); // Wait at least 2.5 seconds
                console.log(YELLOW + `Blur rate limit: Waiting ${waitTime}ms for wallet ${wallet}` + RESET);
                await new Promise(resolve => setTimeout(resolve, waitTime));
              }

              // Process jobs in smaller batches
              const jobBatches = [];
              for (let i = 0; i < walletJobs.length; i += MAX_BLUR_JOBS_PER_BATCH) {
                jobBatches.push(walletJobs.slice(i, i + MAX_BLUR_JOBS_PER_BATCH));
              }

              for (const [batchIndex, batch] of jobBatches.entries()) {
                // Add delay between batches
                if (batchIndex > 0) {
                  console.log(YELLOW + `Blur batch ${batchIndex + 1}/${jobBatches.length}: Waiting 5s before next batch` + RESET);
                  await new Promise(resolve => setTimeout(resolve, 5000)); // 5 second delay between batches
                }

                for (const [index, job] of batch.entries()) {
                  if (index > 0) {
                    // Longer delay for Blur jobs to respect rate limits
                    const delayBetweenJobs = 500; // 500ms between each job (max 2 per second)
                    await new Promise(resolve => setTimeout(resolve, delayBetweenJobs));
                  }

                  const jobOptions = createJobOptions(job.jobKey, job.name);

                  try {
                    await queue.add(job.name, job.data, jobOptions);
                    blurJobsProcessed++;

                    // Update progress for Blur jobs
                    const currentProgress = addedJobs.length + blurJobsProcessed;
                  } catch (error) {
                    if ((error as any)?.message?.includes('Missing lock for job')) {
                      console.log(YELLOW + `Duplicate job skipped for task ${taskId}` + RESET);
                    } else {
                      throw error;
                    }
                  }
                }
              }
            }
            // Count actual Blur jobs added
            let blurJobsAdded = 0;
            for (const [wallet, jobs] of blurJobsByWallet.entries()) {
              blurJobsAdded += jobs.length;
            }

            totalAdded += (addedJobs.length + blurJobsProcessed);
            break;
          } catch (error) {
            retries--;
            totalFailed++;
            tracker.failedJobs = totalFailed;
            if (retries === 0) {
              throw error;
            }
            console.log(YELLOW + `Retrying addBulk for chunk ${chunkIndex + 1}. Attempts remaining: ${retries}` + RESET);
            await new Promise(resolve => setTimeout(resolve, 2000));
          }
        }
        await new Promise(resolve => setTimeout(resolve, 1000));
      } catch (error) {
        tracker.status = 'failed';
        bulkJobTrackers.set(bulkJobId, tracker);
      }
    }
    // Final status update
    tracker.status = 'completed';
    tracker.processedJobs = tracker.totalJobs;
    bulkJobTrackers.set(bulkJobId, tracker);

  } catch (error) {
    tracker.status = 'failed';
    bulkJobTrackers.set(bulkJobId, tracker);
    console.error(RED + `Failed to process bulk jobs for task ${taskId}: ${error}` + RESET);
  }
}

// Helper function to wait for queue drain
async function waitForQueueDrain(): Promise<void> {
  while (true) {
    const counts = await queue.getJobCounts();
    if ((counts.prioritized || 0) <= DRAIN_THRESHOLD) {
      return;
    }

    const delay = counts.prioritized / RATE_LIMIT
    await new Promise(resolve => setTimeout(resolve, delay * 1000));
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
  switch (job.name) {
    case 'OPENSEA_TRAIT_BID':
      trait = JSON.parse(job.data.trait)
      type = trait.type
      value = trait.value
      uniqueKey = `opensea-${type}-${value}`
      break;
    case 'BLUR_TRAIT_BID':
      const traitObj = JSON.parse(job.data.trait);
      const traitKey = Object.keys(traitObj)[0];
      const traitValue = traitObj[traitKey];
      uniqueKey = `blur-${traitKey}-${traitValue}`;
      break;
    case 'MAGICEDEN_TRAIT_BID':
      type = job.data.trait.attributeKey
      value = job.data.trait.attributeValue
      uniqueKey = `magiceden-${type}-${value}`
      break;
    case 'OPENSEA_TOKEN_BID':
      uniqueKey = `opensea-${job.data.asset.tokenId}`
      break;
    case 'MAGICEDEN_TOKEN_BID':
      uniqueKey = `magiceden-${job.data.tokenId}`;
      break;
    default:
      uniqueKey = 'collection';
  }
  const baseKey = `${job?.data?._id}`
  const key = `bid-${baseKey}-${uniqueKey}-${job?.data?.contract?.slug || job?.data?.slug}`
  return key
}

function generateJobKey(job: any): string {
  const taskId = job?.data?._id || '';
  const contractSlug = job?.data?.contract?.slug || job?.data?.slug || '';
  const name = job?.name || '';

  return `${taskId}-${contractSlug}-${name}`;
}


async function startTask(task: ITask, start: boolean, marketplace?: string) {
  const startTime = Date.now();
  const taskId = task._id.toString();

  // === ENHANCED STARTTASK: REMOVE FROM BLACKLIST ===
  // Remove task from blacklist to allow job execution
  const wasBlacklisted = removeTaskFromBlacklist(taskId, marketplace);
  if (wasBlacklisted) {
    console.log(GREEN + `[startTask] Task ${taskId} removed from blacklist${marketplace ? ` for marketplace: ${marketplace}` : ''} - jobs can now execute` + RESET);
  }

  // console.log(YELLOW + `[startTask] Called with task ${task._id}, start=${start}, marketplace=${marketplace || 'all'}` + RESET);

  if (!API_KEY) {
    console.log(RED + 'API_KEY is not set - cannot start task' + RESET);
    console.log(YELLOW + 'Please set API_KEY environment variable and restart' + RESET);
    return;
  }

  // Initialize taskStatus immediately to prevent race conditions
  if (!taskStatus[task._id]) {
    taskStatus[task._id] = {
      status: 'STARTED',
      runningPercentage: 0,
      waitingTime: 0,
      cancellationPercentage: 0,
      processingPercentage: 0,
      nextLoopTime: new Date()
    };
    // Reset loop counter for new task
    taskLoopCounters.delete(task._id);
  } else if (!marketplace) {
    // Only update status to STARTED when starting the entire task (no marketplace specified)
    // This handles the case where a task is transitioning from SCHEDULED/STOPPED to STARTED
    taskStatus[task._id].status = 'STARTED';
    taskStatus[task._id].runningPercentage = 0;
    taskStatus[task._id].cancellationPercentage = 0;
    taskStatus[task._id].processingPercentage = 0;
    // Don't reset loop counter if we're in a scheduled loop (start=true from scheduleNextLoop)
    // Only reset when manually starting/restarting the task
    if (!start) {
      taskLoopCounters.delete(task._id);
    }
    // IMPORTANT: Loop counter should ONLY be incremented in scheduleNextLoop
    // NOT here in startTask, even when start=true
  } else {
    // Marketplace-specific start - don't modify global task status or loop counters
    console.log(CYAN + `[startTask] Marketplace-specific start for ${marketplace} - preserving global task state` + RESET);
  }

  // Always initialize stats if they don't exist, regardless of marketplace parameter
  if (!skipStats[task._id]) {
    skipStats[task._id] = {
      opensea: 0,
      magiceden: 0,
      blur: 0
    };
  }

  if (!errorStats[task._id]) {
    errorStats[task._id] = {
      opensea: 0,
      magiceden: 0,
      blur: 0
    };
  }

  // Always initialize completedTasks if it doesn't exist
  if (!completedTasks[task._id]) {
    completedTasks[task._id] = {
      opensea: false,
      magiceden: false,
      blur: false
    };
  }

  // Async bid log deletion - fire and forget
  BidLogs.deleteMany({ taskId: task._id }).catch(err =>
    console.error(YELLOW + `Failed to delete bid logs for task ${task._id}: ${err.message}` + RESET)
  );

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

    // console.log(CYAN + `Checking marketplace approvals for wallet ${wallet.address}...` + RESET);

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
        console.log(GREEN + `✓ ${mp} approval confirmed for wallet ${wallet.address} (cached)` + RESET);
        return { marketplace: mp, approved: true, cached: true };
      }

      console.log(CYAN + `Checking ${mp} approval on-chain...` + RESET);
      const approved = await approveMarketplace(marketplaceAddress, privateKey, WETH);

      if (approved) {
        console.log(GREEN + `✓ ${mp} approval confirmed for wallet ${wallet.address}` + RESET);
        // Update cache with successful approval
        await approvalCache.setApproval(wallet.address, marketplaceAddress, WETH, {
          approved: true,
          timestamp: Date.now(),
          allowance: MaxUint256.toString()
        });
      } else {
        console.log(RED + `✗ Failed to approve ${mp} for wallet ${wallet.address}` + RESET);
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
    const updatedTask = { ...task, running: true };
    activeTasks.set(taskId, updatedTask);
    const taskIndex = currentTasks.findIndex(t => t._id === task._id);
    if (taskIndex !== -1) {
      currentTasks[taskIndex].running = true;
    }

    // Add task to indexes for fast lookup
    addTaskToIndexes(updatedTask);

    console.log(GREEN + `Updated task ${task.contract.slug} running status to: ${start}`.toUpperCase() + RESET);
    // Subscribe to WebSocket collection (WebSocket should already be connected at startup)
    try {
      if (wsManager?.isConnected()) {
        // console.log("subscribing to collection: ", task.contract.slug);
        const newTask = { ...task, running: true };
        subscribeToCollections([newTask]);
      } else {
        console.log(YELLOW + `WebSocket not connected, skipping subscription for ${task.contract.slug}` + RESET);
      }
    } catch (error) {
      console.error(RED + `Error subscribing to collection ${task.contract.slug}: ` + RESET, error);
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
          console.log(YELLOW + `[Circuit Breaker] Skipping ${m} for task ${taskId} - currently suspended` + RESET);
          return false;
        }
        return true;
      });

    // Perform preprocessing to determine bid types and job counts
    if (availableMarketplaces.length > 0) {
      // console.log(CYAN + `[Preprocessing] Starting task preprocessing for ${taskId}${marketplace ? ` (marketplace: ${marketplace})` : ''}` + RESET);

      // Clear parentJobs for this task at the start of each loop
      // This ensures we start fresh and don't have stale data from previous loops
      const marketplacesToClear = marketplace ? [marketplace.toLowerCase()] : ['opensea', 'magiceden', 'blur'];
      for (const mp of marketplacesToClear) {
        const parentId = `${taskId}-${mp}`.toLowerCase();
        if (parentJobs.has(parentId)) {
          parentJobs.delete(parentId);
        }
      }

      try {
        // Initialize preprocessing result
        const preprocessing: TaskPreprocessing = {
          bidType: task.bidType as 'token' | 'trait' | 'collection',
          opensea: { enabled: false, traits: [], tokens: [], jobCount: 0 },
          blur: { enabled: false, traits: [], tokens: [], jobCount: 0 },
          magiceden: { enabled: false, traits: [], tokens: [], jobCount: 0 }
        };

        // Get wallet info for token fetching
        const wallet = await walletCache.getWalletById(task.walletId);
        const WALLET_PRIVATE_KEY = decryptPrivateKey(wallet?.encryptedPrivateKey as string);

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
        let openseaTokens: number[] = [];
        let magicedenTokens: number[] = [];

        if (task.bidType === "token") {
          // First, fetch bottom listings if autoIds are present
          if (amount) {
            const [openseaListings, magicedenListings] = await Promise.all([
              availableMarketplaces.includes("opensea") ?
                fetchOpenseaListings(taskId, task.contract.slug, amount).catch(() => []) :
                Promise.resolve([]),
              availableMarketplaces.includes("magiceden") ?
                fetchMagicEdenTokens(taskId, task.contract.contractAddress, amount).catch(() => []) :
                Promise.resolve([])
            ]);

            openseaTokens = [...(openseaListings || [])];
            magicedenTokens = [...(magicedenListings || [])];
          }

          // Add manually specified token IDs
          const manualTokenIds = task.tokenIds.filter(id => !isNaN(Number(id)) && !id.toString().toLowerCase().includes('bot')).map(id => Number(id));
          openseaTokens = [...openseaTokens, ...manualTokenIds];
          magicedenTokens = [...magicedenTokens, ...manualTokenIds];

          // If traits are selected for token bid type, fetch trait-based tokens
          const openseaFilteredTraits = Object.fromEntries(
            Object.entries(task?.selectedTraits?.opensea || {}).filter(([_, traits]) => traits.length > 0)
          );

          if (Object.keys(openseaFilteredTraits).length > 0) {
            const selectedTraits = transformNewTask(openseaFilteredTraits);
            const attributes = transformSelectedTraitsToAttributes(selectedTraits);
            const expandedAttributes = expandAttributes(attributes);
            let traitTokens: number[] = [];

            if (task.traitsOperator.value === "or") {
              await pQueue.addAll(
                expandedAttributes.map((attribute) => async () => {
                  const variable: CollectionItemsQueryVariables = {
                    collectionSlug: task.contract.slug,
                    filter: { attributes: [attribute] },
                    limit: 100,
                    sort: { by: 'PRICE', direction: 'ASC' },
                  };
                  const items = await fetchOpenSeaCollectionItems(variable);
                  traitTokens.push(...items);
                })
              );
            } else if (task.traitsOperator.value === "and") {
              const variable: CollectionItemsQueryVariables = {
                collectionSlug: task.contract.slug,
                filter: { attributes },
                limit: 100,
                sort: { by: 'PRICE', direction: 'ASC' },
              };
              const items = await fetchOpenSeaCollectionItems(variable);
              traitTokens.push(...items);
            }

            // Add trait tokens to both OpenSea and MagicEden lists
            openseaTokens = [...openseaTokens, ...traitTokens];
            magicedenTokens = [...magicedenTokens, ...traitTokens];
          }
        }

        // Process each marketplace - if marketplace parameter is specified, only process that one
        const marketplacesToProcess = marketplace ? [marketplace.toLowerCase()].filter(m => availableMarketplaces.includes(m)) : availableMarketplaces;
        
        for (const mp of marketplacesToProcess) {
          if (mp === "opensea") {
            preprocessing.opensea.enabled = true;

            // Transform OpenSea traits
            const filteredTraits = Object.fromEntries(
              Object.entries(task?.selectedTraits?.opensea || {}).filter(([_, traits]) => traits.length > 0)
            );
            const selectedTraits = transformNewTask(filteredTraits);
            const traitBid = selectedTraits && Object.keys(selectedTraits).length > 0;

            if (task.bidType === "token" && openseaTokens.length > 0) {
              // Token bid
              preprocessing.opensea.tokens = [...new Set(openseaTokens)];
              preprocessing.opensea.jobCount = preprocessing.opensea.tokens.length;
              console.log(`${BLUE}[Preprocessing] OpenSea will create ${preprocessing.opensea.jobCount} token bid jobs${RESET}`);
            } else if (traitBid) {
              // Trait bid - need to check if collection supports trait offers
              const collectionDetails = await getCollectionDetails(task.contract.slug, task.contract.contractAddress);
              if (collectionDetails.trait_offers_enabled) {
                preprocessing.opensea.traits = transformOpenseaTraits(selectedTraits);
                preprocessing.opensea.jobCount = preprocessing.opensea.traits.length;
                console.log(`${BLUE}[Preprocessing] OpenSea will create ${preprocessing.opensea.jobCount} trait bid jobs${RESET}`);
              } else {
                preprocessing.opensea.skipReason = `Trait bidding not available for ${task.contract.slug}`;
                preprocessing.opensea.jobCount = 0;
              }
            } else if (task.bidType === "collection") {
              // Collection bid
              preprocessing.opensea.jobCount = 1;
              console.log(`${BLUE}[Preprocessing] OpenSea will create 1 collection bid job${RESET}`);
            }

            // Set up parent job for OpenSea
            if (preprocessing.opensea.jobCount > 0) {
              const parentJobId = `${taskId}-opensea`.toLowerCase();
              parentJobs.set(parentJobId, {
                parentId: parentJobId,
                totalJobs: preprocessing.opensea.jobCount,
                completedJobs: 0,
                failedJobs: 0,
                startTime: Date.now(),
                taskId: taskId,
                jobName: task.bidType === "token" ? OPENSEA_TOKEN_BID :
                  traitBid ? OPENSEA_TRAIT_BID : OPENSEA_COLLECTION_BID,
              });
              console.log(`${BLUE}[Parent Job Created] OpenSea | Task: ${taskId} | Total Jobs: ${preprocessing.opensea.jobCount}${RESET}`);
            }
          } else if (mp === "blur") {
            preprocessing.blur.enabled = true;

            // Check if Blur supports this bid type
            if (task.bidType === "token") {
              preprocessing.blur.skipReason = "Token bids not supported on Blur";
              preprocessing.blur.jobCount = 0;
              console.log(`${GOLD}[Preprocessing] Blur skipped - token bids not supported${RESET}`);
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
                // console.log(`${GOLD}[Preprocessing] Blur will create ${preprocessing.blur.jobCount} trait bid jobs${RESET}`);
              } else if (task.bidType === "collection") {
                // Collection bid
                preprocessing.blur.jobCount = 1;
                console.log(`${GOLD}[Preprocessing] Blur will create 1 collection bid job${RESET}`);
              }

              // Set up parent job for Blur
              if (preprocessing.blur.jobCount > 0) {
                const parentJobId = `${taskId}-blur`.toLowerCase();
                parentJobs.set(parentJobId, {
                  parentId: parentJobId,
                  totalJobs: preprocessing.blur.jobCount,
                  completedJobs: 0,
                  failedJobs: 0,
                  startTime: Date.now(),
                  taskId: taskId,
                  jobName: traitBid ? BLUR_TRAIT_BID : BLUR_COLLECTION_BID,
                });
                // console.log(`${GOLD}[Parent Job Created] Blur | Task: ${taskId} | Total Jobs: ${preprocessing.blur.jobCount}${RESET}`);
              }
            }
          } else if (mp === "magiceden") {
            preprocessing.magiceden.enabled = true;

            // Transform MagicEden traits
            const filteredTraits = Object.fromEntries(
              Object.entries(task?.selectedTraits?.magiceden || {}).filter(([_, traits]) => traits.length > 0)
            );
            const selectedTraits = transformNewTask(filteredTraits);
            const traitBid = selectedTraits && Object.keys(selectedTraits).length > 0;

            if (task.bidType === "token" && magicedenTokens.length > 0) {
              // Token bid
              preprocessing.magiceden.tokens = [...new Set(magicedenTokens)];
              preprocessing.magiceden.jobCount = preprocessing.magiceden.tokens.length;
              console.log(`${MAGENTA}[Preprocessing] MagicEden will create ${preprocessing.magiceden.jobCount} token bid jobs${RESET}`);
            } else if (traitBid) {
              // Trait bid
              preprocessing.magiceden.traits = Object.entries(selectedTraits).flatMap(([key, values]) =>
                values.map(value => ({ attributeKey: key, attributeValue: value }))
              );
              preprocessing.magiceden.jobCount = preprocessing.magiceden.traits.length;
              console.log(`${MAGENTA}[Preprocessing] MagicEden will create ${preprocessing.magiceden.jobCount} trait bid jobs${RESET}`);
            } else if (task.bidType === "collection") {
              // Collection bid
              preprocessing.magiceden.jobCount = 1;
              console.log(`${MAGENTA}[Preprocessing] MagicEden will create 1 collection bid job${RESET}`);
            }

            // Set up parent job for MagicEden
            if (preprocessing.magiceden.jobCount > 0) {
              const parentJobId = `${taskId}-magiceden`.toLowerCase();
              parentJobs.set(parentJobId, {
                parentId: parentJobId,
                totalJobs: preprocessing.magiceden.jobCount,
                completedJobs: 0,
                failedJobs: 0,
                startTime: Date.now(),
                taskId: taskId,
                jobName: task.bidType === "token" ? MAGICEDEN_TOKEN_BID :
                  traitBid ? MAGICEDEN_TRAIT_BID : MAGICEDEN_COLLECTION_BID,
              });
              console.log(`${MAGENTA}[Parent Job Created] MagicEden | Task: ${taskId} | Total Jobs: ${preprocessing.magiceden.jobCount}${RESET}`);
            }
          }
        }

        // Store preprocessing results
        taskPreprocessing.set(taskId, preprocessing);

        // Also update currentTraits for backwards compatibility
        if (!currentTraits[taskId]) {
          currentTraits[taskId] = {
            opensea: {},
            magiceden: {},
            blur: {}
          };
        }

        // Convert preprocessed traits back to the format expected by currentTraits
        if (preprocessing.opensea.traits.length > 0) {
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

        if (preprocessing.blur.traits.length > 0) {
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

        if (preprocessing.magiceden.traits.length > 0) {
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

        // Log summary
        const totalJobs = preprocessing.opensea.jobCount + preprocessing.blur.jobCount + preprocessing.magiceden.jobCount;
        // console.log(GREEN + `[Preprocessing] Complete for task ${taskId} - Total jobs across all marketplaces: ${totalJobs}` + RESET);
        // console.log(GREEN + `  OpenSea: ${preprocessing.opensea.jobCount} jobs${preprocessing.opensea.skipReason ? ' (skipped: ' + preprocessing.opensea.skipReason + ')' : ''}` + RESET);
        // console.log(GREEN + `  Blur: ${preprocessing.blur.jobCount} jobs${preprocessing.blur.skipReason ? ' (skipped: ' + preprocessing.blur.skipReason + ')' : ''}` + RESET);
        // console.log(GREEN + `  MagicEden: ${preprocessing.magiceden.jobCount} jobs${preprocessing.magiceden.skipReason ? ' (skipped: ' + preprocessing.magiceden.skipReason + ')' : ''}` + RESET);

      } catch (error) {
        console.error(RED + `[Preprocessing] Error during task preprocessing: ${error}` + RESET);
        // Continue with normal flow even if preprocessing fails
      }
    }

    // Only create jobs for the specified marketplace if provided, otherwise create for all available
    const marketplacesForJobs = marketplace ? [marketplace.toLowerCase()].filter(m => availableMarketplaces.includes(m)) : availableMarketplaces;
    
    const jobs = marketplacesForJobs
      .map(m => {
        const jobId = `${task._id}-${m}-schedule`;
        switch (m) {
          case "opensea":
            return { name: OPENSEA_SCHEDULE, data: { ...task, running: start }, opts: { jobId } };
          case "blur":
            return { name: BLUR_SCHEDULE, data: { ...task, running: start }, opts: { jobId } };
          case "magiceden":
            return { name: MAGICEDEN_SCHEDULE, data: { ...task, running: start }, opts: { jobId } };
          default:
            return null;
        }
      })
      .filter(job => job !== null);

    // Add deduplication check before adding jobs (skip deduplication for loop restarts)
    if (jobs.length > 0) {
      // Add job options for SCHEDULE jobs to ensure unlimited retries
      const jobsWithOptions = jobs.map(job => ({
        ...job,
        opts: createJobOptions(undefined, job.name)
      }));

      // For loop restarts (start=true), always add jobs without deduplication
      if (start === true) {
        // console.log(GREEN + `Loop restart: Adding ${jobs.length} jobs without deduplication` + RESET);
        await queue.addBulk(jobsWithOptions as Job[]);
      } else {
        // Optimized deduplication - only check for this specific task's jobs
        const existingJobNames = new Set<string>();

        // Check each job type individually to avoid loading all jobs
        for (const jobToAdd of jobsWithOptions) {
          const jobType = jobToAdd.name;
          const targetJobId = jobToAdd.opts?.jobId || `${taskId}-${jobType}`;

          // Try to get the specific job by ID first
          try {
            const existingJob = await queue.getJob(targetJobId);
            if (existingJob && ['waiting', 'active', 'delayed'].includes(await existingJob.getState())) {
              existingJobNames.add(jobType);
            }
          } catch (err) {
            // Job doesn't exist, which is fine
          }
        }

        // Filter out duplicate jobs
        const newJobs = jobsWithOptions.filter(job => !existingJobNames.has(job.name));

        if (newJobs.length > 0) {
          console.log(GREEN + `Adding ${newJobs.length} new jobs (filtered ${jobs.length - newJobs.length} duplicates)` + RESET);
          await queue.addBulk(newJobs as Job[]);
        } else {
          console.log(YELLOW + `All ${jobs.length} jobs already exist in queue, skipping` + RESET);
        }
      }
    }

    // Task status will be updated to RUNNING when jobs are actually queued in scheduled bid functions
    if (marketplace) {
      // For marketplace-specific starts, recalculate progress percentages
      console.log(GREEN + `Recalculating progress for task ${taskId} after adding marketplace ${marketplace}` + RESET);

      // Reset the marketplace completion status
      if (completedTasks[taskId]) {
        completedTasks[taskId][marketplace.toLowerCase() as 'opensea' | 'magiceden' | 'blur'] = false;
      }

      // The progress will be recalculated automatically as jobs complete
    }

  } catch (error) {
    console.error(RED + `Error starting task ${taskId}: ` + RESET, error);
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
}

// Enhanced BullMQ job cancellation for active jobs
async function cancelActiveTaskJobs(taskId: string, marketplace?: string) {
  const marketplaces = marketplace ? [marketplace.toLowerCase()] : ['opensea', 'blur', 'magiceden'];
  let cancelledCount = 0;
  const results = [];

  for (const mp of marketplaces) {
    const jobIdPrefix = `$bid-${taskId}-${mp}-`;

    try {
      // Get active jobs and manually fail them using BullMQ pattern
      const activeJobs = await queue.getJobs(['active'], 0, -1);
      const taskActiveJobs = activeJobs.filter(job =>
        job.id && job.id.startsWith(jobIdPrefix)
      );

      for (const job of taskActiveJobs) {
        try {
          // Use BullMQ's moveToFailed to properly terminate active jobs
          await job.moveToFailed(new Error(`Task ${taskId} stopped - job cancelled`), job.token, false);
          cancelledCount++;
          console.log(`Cancelled active job: ${job.id}`);
        } catch (error) {
          console.error(`Failed to cancel active job ${job.id}:`, error);
        }
      }

      results.push({
        marketplace: mp,
        activeJobsCancelled: taskActiveJobs.length
      });

    } catch (error: any) {
      console.error(`Error cancelling active ${mp} jobs for task ${taskId}:`, error);
      results.push({
        marketplace: mp,
        activeJobsCancelled: 0,
        error: error?.message || String(error)
      });
    }
  }

  return { cancelled: cancelledCount, results };
}

// Enhanced job removal from all queue states using correct job ID format
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

// Enhanced progress tracking for cancellation
function updateCancellationProgress(taskId: string, completed: number, total: number) {
  if (!taskStatus[taskId]) return;

  const percentage = total > 0 ? Math.min(100, Math.round((completed / total) * 100)) : 100;
  taskStatus[taskId].cancellationPercentage = percentage;

  // Log progress for debugging
  console.log(`[${taskId}] Cancellation progress: ${completed}/${total} (${percentage}%)`);
}


async function stopTask(task: ITask, start: boolean, marketplace?: string) {
  const taskId = task._id.toString();
  
  // Initialize taskStatus if it doesn't exist
  if (!taskStatus[taskId]) {
    taskStatus[taskId] = {
      status: 'STOPPED',
      cancellationPercentage: 0,
      runningPercentage: 0,
      waitingTime: 0,
      processingPercentage: 0
    };
  }
  
  // Initialize completedTasks if it doesn't exist
  if (!completedTasks[taskId]) {
    completedTasks[taskId] = {
      opensea: false,
      magiceden: false,
      blur: false
    };
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

  try {
    // === ENHANCED STOPTASK: IMMEDIATE JOB CANCELLATION ===
    let totalOperations = 0;
    let completedOperations = 0;
    let completionHandled = false; // Track if completion has been handled


    if (marketplace) {
      // Marketplace-specific stop: store previous state and set status to CANCELLING
      // The status will be restored to previousState when cancellation completes
      const oldStatus = taskStatus[taskId].status;
      taskStatus[taskId].previousState = taskStatus[taskId].status;
      console.log(MAGENTA + `🎯 [DEBUG] MARKETPLACE-SPECIFIC STOP` + RESET);
      console.log(MAGENTA + `  Marketplace: ${marketplace}` + RESET);
      console.log(MAGENTA + `  Status: ${oldStatus} → CANCELLING` + RESET);
      console.log(MAGENTA + `  Previous State Stored: ${taskStatus[taskId].previousState}` + RESET);
    } else {
      // Full task stop: clear scheduling fields
      console.log(RED + `🛑 [DEBUG] FULL TASK STOP` + RESET);
      delete taskStatus[taskId].nextLoopTime;
      delete taskStatus[taskId].waitingTime;
    }
    // Set cancellation status and progress for both cases
    const oldStatus = taskStatus[taskId].status;
    taskStatus[taskId].status = 'CANCELLING';
    taskStatus[taskId].cancellationPercentage = 0;
    taskStatus[taskId].cancellationStartTime = Date.now();
    console.log(BLUE + `📊 [DEBUG] STATUS CHANGE: ${oldStatus} → CANCELLING` + RESET);

    // Step 1: Blacklist task to prevent new jobs (immediate)
    const limiterResult = cancelTaskJobs(taskId, marketplace);
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

    // Duplicate initialization removed - already handled above

    if (!marketplace) {
      warningBids[taskId] = {
        opensea: false,
        magiceden: false,
        blur: false
      };
    }

    switch (marketplace?.toLowerCase()) {
      case 'opensea': {
        console.log(GOLD + `🌊 [DEBUG] OPENSEA CANCELLATION` + RESET);
        const result = await cancelOpenseaRequest(taskId);
        console.log(GOLD + `  Result: ${JSON.stringify(result)}` + RESET);
        if (result.error) {
          console.log(RED + `  ❌ OpenSea Error: ${result.error}` + RESET);
          logger.logWarning(ErrorCategory.MARKETPLACE, 'Failed to cancel OpenSea requests', {
            taskId,
            marketplace: 'opensea',
            errorMessage: result.error
          });
        } else if (result.cancelledCount === 0) {
          console.log(GREEN + `  ✅ No jobs to cancel - calling immediate completion` + RESET);
          // If no cancellation jobs were created, immediately update status
          // Use marketplace-specific completion for marketplace cancellations
          setMarketplaceCancellationComplete(taskId, marketplace);
          completionHandled = true;
          console.log(GOLD + `  🏁 Completion handled flag set to true` + RESET);
        } else {
          console.log(YELLOW + `  ⏳ ${result.cancelledCount} cancellation jobs created` + RESET);
        }
        const openseaParentJobId = `${taskId}-opensea`.toLowerCase()
        const openseaParentJob = parentJobs.get(openseaParentJobId)
        if (openseaParentJob) {
          await parentJobs.delete(openseaParentJobId)
        }
        break;
      }
      case 'magiceden': {
        console.log(MAGENTA + `🪄 [DEBUG] MAGICEDEN CANCELLATION` + RESET);
        const result = await cancelMagicedenRequest(taskId);
        console.log(MAGENTA + `  Result: ${JSON.stringify(result)}` + RESET);
        if (result.error) {
          console.log(RED + `  ❌ MagicEden Error: ${result.error}` + RESET);
          logger.logWarning(ErrorCategory.MARKETPLACE, 'Failed to cancel MagicEden requests', {
            taskId,
            marketplace: 'magiceden',
            errorMessage: result.error
          });
        } else if (result.cancelledCount === 0) {
          console.log(GREEN + `  ✅ No jobs to cancel - calling immediate completion` + RESET);
          // If no cancellation jobs were created, immediately update status
          // Use marketplace-specific completion for marketplace cancellations
          setMarketplaceCancellationComplete(taskId, marketplace);
          completionHandled = true;
          console.log(MAGENTA + `  🏁 Completion handled flag set to true` + RESET);
        } else {
          console.log(YELLOW + `  ⏳ ${result.cancelledCount} cancellation jobs created` + RESET);
        }
        const magicedenParentJobId = `${taskId}-magiceden`.toLowerCase()
        const magicedenParentJob = parentJobs.get(magicedenParentJobId)
        if (magicedenParentJob) {
          await parentJobs.delete(magicedenParentJobId)
        }
        break;
      }
      case 'blur': {
        console.log(BLUE + `💙 [DEBUG] BLUR CANCELLATION` + RESET);
        const result = await cancelBlurRequest(taskId);
        console.log(BLUE + `  Result: ${JSON.stringify(result)}` + RESET);
        if (result.error) {
          console.log(RED + `  ❌ Blur Error: ${result.error}` + RESET);
          logger.logWarning(ErrorCategory.MARKETPLACE, 'Failed to cancel Blur requests', {
            taskId,
            marketplace: 'blur',
            errorMessage: result.error
          });
        } else if (result.cancelledCount === 0) {
          console.log(GREEN + `  ✅ No jobs to cancel - calling immediate completion` + RESET);
          // If no cancellation jobs were created, immediately update status
          // Use marketplace-specific completion for marketplace cancellations
          setMarketplaceCancellationComplete(taskId, marketplace);
          completionHandled = true;
          console.log(BLUE + `  🏁 Completion handled flag set to true` + RESET);
        } else {
          console.log(YELLOW + `  ⏳ ${result.cancelledCount} cancellation jobs created` + RESET);
        }
        const blurParentJobId = `${taskId}-blur`.toLowerCase()
        const blurParentJob = parentJobs.get(blurParentJobId)
        if (blurParentJob) {
          await parentJobs.delete(blurParentJobId)
        }
        break;
      }
      default: {
        // Cancel requests for all marketplaces and track results
        const results = await Promise.allSettled([
          cancelOpenseaRequest(taskId),
          cancelMagicedenRequest(taskId),
          cancelBlurRequest(taskId)
        ]);

        // Calculate total cancelled count
        let totalCancelledCount = 0;

        // Log any failures and count successful cancellations
        const marketplaceNames = ['opensea', 'magiceden', 'blur'];
        results.forEach((result, index) => {
          if (result.status === 'rejected' || (result.status === 'fulfilled' && result.value.error)) {
            logger.logWarning(ErrorCategory.MARKETPLACE, `Failed to cancel ${marketplaceNames[index]} requests`, {
              taskId,
              marketplace: marketplaceNames[index],
              errorMessage: result.status === 'rejected' ? result.reason : result.value.error
            });
          } else if (result.status === 'fulfilled' && result.value.success) {
            totalCancelledCount += result.value.cancelledCount || 0;
          }
        });

        // If no cancellation jobs were created, immediately set status to STOPPED
        if (totalCancelledCount === 0) {
          setTaskCancellationComplete(taskId);
        }

        // Delete parent jobs for all marketplaces
        const marketplaces = ['opensea', 'magiceden', 'blur']
        for (const marketplace of marketplaces) {
          const parentJobId = `${taskId}-${marketplace}`.toLowerCase()
          const parentJob = parentJobs.get(parentJobId)
          if (parentJob) {
            await parentJobs.delete(parentJobId)
          }
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

    // get all the task, if no task, change state to stopped

    const orders = await prisma.order.findMany({
      where: {
        taskId: taskId,
        // ...(marketplace && { marketplace: marketplace.toLowerCase() })
      }
    })

    if (!orders.length) {
      if (taskStatus[taskId]) {
        delete taskStatus[taskId].nextLoopTime
        taskStatus[taskId].waitingTime = 0
        taskStatus[taskId].status = 'STOPPED'
      }

      // Clear countdown timer
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

    // Complete cancellation status
    console.log(CYAN + `🏁 [DEBUG] FINAL COMPLETION CHECK` + RESET);
    console.log(CYAN + `  Completion Handled: ${completionHandled}` + RESET);
    console.log(CYAN + `  Marketplace: ${marketplace || 'ALL'}` + RESET);
    console.log(CYAN + `  Current Status: ${taskStatus[taskId]?.status}` + RESET);
    console.log(CYAN + `  Previous State: ${taskStatus[taskId]?.previousState || 'NONE'}` + RESET);

    if (!completionHandled) {
      if (marketplace) {
        console.log(GREEN + `  🎯 Calling setMarketplaceCancellationComplete` + RESET);
        setMarketplaceCancellationComplete(taskId, marketplace);
      } else {
        console.log(GREEN + `  🛑 Calling setTaskCancellationComplete` + RESET);
        setTaskCancellationComplete(taskId);
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
    console.log(RED + `💥 [DEBUG] ERROR COMPLETION` + RESET);
    console.log(RED + `  Error: ${error.message}` + RESET);
    console.log(RED + `  Marketplace: ${marketplace || 'ALL'}` + RESET);

    if (marketplace) {
      console.log(RED + `  🎯 Force calling setMarketplaceCancellationComplete` + RESET);
      setMarketplaceCancellationComplete(taskId, marketplace);
    } else {
      console.log(RED + `  🛑 Force calling setTaskCancellationComplete` + RESET);
      setTaskCancellationComplete(taskId);
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
      jobIds.push(`${task._id}-${task.contract.slug}-${marketplace.toLowerCase()}-${uniqueKey}`)
    })
  } else if (tokenBid) {
    task.tokenIds.map((tokenId) => {
      jobIds.push(`${task._id}-${task.contract.slug}-${marketplace.toLowerCase()}-${tokenId}`)
    })
  } else {
    jobIds.push(`${task._id}-${task.contract.slug}-${marketplace.toLowerCase()}-collection`)
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
    let allOpenseaBids: PrismaOrder[] = [];
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
          prisma.order.findMany({
            where: {
              slug: contractSlug,
              taskId: taskId,
              marketplace: OPENSEA.toLowerCase()
            },
            take: BATCH_SIZE,
            skip: skip,
            orderBy: {
              createdAt: 'desc'
            }
          }),
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
          // Set up or update parent job tracking
          const parentJobId = `${taskId}-CANCEL_OPENSEA_BID`;
          const existingParentJob = parentJobs.get(parentJobId);

          if (!existingParentJob) {
            parentJobs.set(parentJobId, {
              parentId: parentJobId,
              totalJobs: batch.length,
              completedJobs: 0,
              failedJobs: 0,
              startTime: Date.now(),
              taskId: taskId,
              jobName: CANCEL_OPENSEA_BID
            });
          } else {
            // Update total jobs count
            existingParentJob.totalJobs += batch.length;
          }

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
    let allBlurBids: PrismaOrder[] = [];
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
          prisma.order.findMany({
            where: {
              slug: contractSlug,
              taskId: taskId,
              marketplace: BLUR.toLowerCase()
            },
            take: BATCH_SIZE,
            skip: skip,
            orderBy: {
              createdAt: 'desc'
            }
          }),
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
          // Set up or update parent job tracking
          const parentJobId = `${taskId}-CANCEL_BLUR_BID`;
          const existingParentJob = parentJobs.get(parentJobId);

          if (!existingParentJob) {
            parentJobs.set(parentJobId, {
              parentId: parentJobId,
              totalJobs: batch.length,
              completedJobs: 0,
              failedJobs: 0,
              startTime: Date.now(),
              taskId: taskId,
              jobName: CANCEL_BLUR_BID
            });
          } else {
            // Update total jobs count
            existingParentJob.totalJobs += batch.length;
          }

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
    let allMagicedenBids: PrismaOrder[] = [];
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
          prisma.order.findMany({
            where: {
              slug: contractSlug,
              taskId: taskId,
              marketplace: MAGICEDEN.toLowerCase()
            },
            take: BATCH_SIZE,
            skip: skip,
            orderBy: {
              createdAt: 'desc'
            }
          }),
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
              jobName: CANCEL_MAGICEDEN_BID
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
      parentJobs.delete(parentJobId);

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

      parentJobs.delete(openSeaParentJobId);
      parentJobs.delete(magicEdenParentJobId);
      parentJobs.delete(blurParentJobId);

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

async function extractMagicedenOrderHash(orders: PrismaOrder[]): Promise<string[]> {

  const orderData = orders.map((order, index) => {
    if (!order) return
    const parsed = JSON.parse(order.orderHash);
    return parsed;
  })

  const extractedOrderIds = orderData
    .map(bid => {
      try {

        if (bid.results) {
          return bid.results[0].orderId;
        }
        if (bid.message && bid.orderId) {
          return bid.orderId;
        }
        return null;
      } catch (e) {
        console.error('Error parsing bid data:', e);
        return null;
      }
    })
  return extractedOrderIds as string[];
}

async function cancelMagicedenBids(orders: PrismaOrder[], privateKey: string, taskId: string) {
  if (!orders.length) return;

  // Set task status to CANCELLING
  if (taskStatus[taskId]) {
    taskStatus[taskId].status = 'CANCELLING';
  }

  // Create parent job for tracking cancellation progress (totalJobs = number of batches)
  const batchCount = Math.ceil(orders.length / 500);
  const parentJobId = `${taskId}-CANCEL_MAGICEDEN_BID`;
  // Always create a fresh parent job for each cancellation operation
  cancellationParentJobs.set(parentJobId, {
    parentId: parentJobId,
    totalJobs: batchCount,
    completedJobs: 0,
    failedJobs: 0,
    startTime: Date.now(),
    taskId: taskId,
    jobName: 'CANCEL_MAGICEDEN_BID',
    totalBids: orders.length,
    completedBids: 0,
    failedBids: 0
  });

  for (let i = 0; i < orders.length; i += 500) {
    const batchOrders = orders.slice(i, i + 500);
    const extractedOrderIds = await extractMagicedenOrderHash(batchOrders)
    const orderKeys = batchOrders.map((order: PrismaOrder) => order.id)
    await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey, orderKeys, taskId }, getCancellationJobOptions(taskId, orderKeys[0], CANCEL_MAGICEDEN_BID));
  }

  // Decrement the own bid counter by the number of cancelled orders
  decrementOwnBidCounter('magiceden', orders.length);
}

async function cancelBlurBids(orders: PrismaOrder[], privateKey: string, taskId: string) {
  if (!orders.length) return

  // Set task status to CANCELLING
  if (taskStatus[taskId]) {
    taskStatus[taskId].status = 'CANCELLING';
  }

  // Create parent job for tracking cancellation progress
  const parentJobId = `${taskId}-CANCEL_BLUR_BID`;
  // Always create a fresh parent job for each cancellation operation
  cancellationParentJobs.set(parentJobId, {
    parentId: parentJobId,
    totalJobs: orders.length,
    completedJobs: 0,
    failedJobs: 0,
    startTime: Date.now(),
    taskId: taskId,
    jobName: 'CANCEL_BLUR_BID',
    totalBids: orders.length,
    completedBids: 0,
    failedBids: 0
  });

  const cancelData = orders.map((order, index) => {
    const payload = JSON.parse(order.orderHash)
    return {
      name: CANCEL_BLUR_BID,
      data: { privateKey, payload, taskId, orderKey: order.id },
      opts: getCancellationJobOptions(taskId, order.id, CANCEL_BLUR_BID)
    }
  }) as Job[]
  await processBulkCancellations(cancelData);
}

async function cancelOpenseaBids(orders: PrismaOrder[], privateKey: string, taskId: string, currencySymbol: string) {
  if (!orders.length) return;

  // Set task status to CANCELLING
  if (taskStatus[taskId]) {
    taskStatus[taskId].status = 'CANCELLING';
  }

  // Create parent job for tracking cancellation progress
  const parentJobId = `${taskId}-CANCEL_OPENSEA_BID`;
  // Always create a fresh parent job for each cancellation operation
  cancellationParentJobs.set(parentJobId, {
    parentId: parentJobId,
    totalJobs: orders.length,
    completedJobs: 0,
    failedJobs: 0,
    startTime: Date.now(),
    taskId: taskId,
    jobName: 'CANCEL_OPENSEA_BID',
    totalBids: orders.length,
    completedBids: 0,
    failedBids: 0
  });

  const cancelData = orders.map((order, index) => {
    return {
      name: CANCEL_OPENSEA_BID,
      data: { privateKey, orderId: order.orderHash, taskId, orderKey: order.id },
      opts: getCancellationJobOptions(taskId, order.id, CANCEL_OPENSEA_BID)
    }
  }) as Job[]

  await processBulkCancellations(cancelData)

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

      // Initialize completedTasks for this task when creating from socket
      if (!completedTasks[taskId]) {
        completedTasks[taskId] = {
          opensea: false,
          magiceden: false,
          blur: false
        };
      }

      // startTask will properly initialize taskStatus
      await startTask(task, true);
    } else {
      // Update activeTasks Map
      const updatedTask = { ...task, running: false };
      activeTasks.set(taskId, updatedTask);
      if (taskStatus[taskId]) {
        taskStatus[taskId].status = 'CANCELLING';
        taskStatus[taskId].cancellationStartTime = Date.now();
      }
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
        await startTask(updatedTask, false, incoming.toLowerCase());
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
            await startTask(task, true);
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
      if (errors.length > 0) {
        console.log(YELLOW + `Started ${tasks.length - errors.length} tasks successfully, ${errors.length} failed` + RESET);
      } else {
        console.log(GREEN + `Successfully started all ${tasks.length} tasks` + RESET);
      }
    } else {
      // Process all tasks in parallel for faster shutdown
      const stopPromises = tasks.map(task =>
        (async () => {
          try {
            await stopTask(task, false);
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
  console.log(`Longest Connection: ${formatDuration(connectionMetrics.longestConnection)}`);
  console.log(`Shortest Connection: ${connectionMetrics.shortestConnection === Infinity ? '0s' : formatDuration(connectionMetrics.shortestConnection)}`);

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
  }, 10000); // Check every 10 seconds
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

    const allWallets = await walletCache.getAllWallets();
    const testWallet = allWallets[0] as IWallet;
    await triggerAutoListingForPurchase(testWallet, contractAddress, tokenId, price, marketplace);


    // Check if the toAddress belongs to one of our wallets (case-insensitive)
    const wallet: IWallet | null = await walletCache.getWallet(toAddress);

    if (!wallet) {
      // Not one of our wallets, ignore
      return;
    }

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
      wallet: wallet.address,
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
      handleBlurCounterbid(message['1'], task)
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
      .includes("magiceden")) return;

    const expiration = getExpiry(currentTask.bidDuration)
    const orderHash = data?.data?.id
    // Include task ID in the hash to make it task-specific
    const taskSpecificHash = orderHash;
    if (hasSeenItem(seenOrderHashes, taskSpecificHash)) return;
    addSeenItem(seenOrderHashes, taskSpecificHash)


    const domain: string = data?.data?.source?.domain
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

    if (!currentTask.outbidOptions.counterbid) return
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

      const orders = await prisma.order.findMany({
        where: {
          taskId: task._id,
          marketplace: MAGICEDEN.toLowerCase(),
          identifier: tokenId.toString()
        }
      })

      const stopBid = await stopOption(currentTask, 'magiceden', Number(floor_price), true)
      if (stopBid) {
        if (orders.length > 0) {
          const orderKeys = orders.map((order: PrismaOrder) => order.id)
          const extractedOrderIds = await extractMagicedenOrderHash(orders)
          queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey: WALLET_PRIVATE_KEY, orderKeys: orderKeys, taskId: task._id });
        }
        return
      }


      const offers = orders.map((order: PrismaOrder) => order.offerPrice)
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
            const orderKeys = orders.map((order: PrismaOrder) => order.id)
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

      const jobId = `counterbid-${task._id}-${task.contract.slug}-${MAGICEDEN.toLowerCase()}-${tokenId}`
      const job: Job = await queue.getJob(jobId)

      if (job) {
        if (incomingBidAmount <= Number(job.data.offerPrice)) {
          return
        } else {
          await job.updateData(bidData)
        }
      } else {
        await queue.add(
          MAGICEDEN_TOKEN_BID_COUNTERBID,
          bidData,
          createJobOptions(jobId, MAGICEDEN_TOKEN_BID_COUNTERBID)
        );
      }

      // Log successful counterbid preparation
      console.log(GREEN + '----------------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(GREEN + `[MagicEden Counterbid SUCCESS] Task: ${task._id} | Placing bid: ${offerPriceEth.toFixed(4)} ${currencySymbol} | Token: #${tokenId} | Outbidding: ${incomingBidAmountEth.toFixed(4)} ${currencySymbol}` + RESET);
      console.log(GREEN + '----------------------------------------------------------------------------------------------------------------------------------------------' + RESET);

      seenOrderHashes.delete(orderHash);

      if (orders.length > 0) {
        const extractedOrderIds = await extractMagicedenOrderHash(orders)
        const orderKeys = orders.map((order: PrismaOrder) => order.id)
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
      const orders = await prisma.order.findMany({
        where: {
          taskId: task._id,
          marketplace: MAGICEDEN.toLowerCase(),
          identifier
        }
      })

      const stopBid = await stopOption(currentTask, 'magiceden', Number(floor_price), true, traitFloor, identifier)

      if (stopBid) {
        if (orders.length > 0) {
          const orderKeys = orders.map((order: PrismaOrder) => order.id)
          const extractedOrderIds = await extractMagicedenOrderHash(orders)
          queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey: WALLET_PRIVATE_KEY, orderKeys: orderKeys, taskId: task._id });
        }
        return
      }


      const offers = orders.map((order: PrismaOrder) => order.offerPrice)
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
            const orderKeys = orders.map((order: PrismaOrder) => order.id)
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

      const jobId = `counterbid-${task._id}-${task.contract.slug}-${MAGICEDEN.toLowerCase()}-${JSON.stringify(trait)}`
      const job: Job = await queue.getJob(jobId)

      if (job) {
        if (incomingBidAmount <= job.data.offerPrice) {
          return
        } else {
          await job.updateData(data)
        }
      } else {
        await queue.add(
          MAGICEDEN_TRAIT_BID_COUNTERBID,
          bidData,
          createJobOptions(jobId, MAGICEDEN_TRAIT_BID_COUNTERBID)
        );
      }

      console.log(GREEN + '----------------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(GREEN + `[MagicEden Counterbid SUCCESS] Task: ${task._id} | Placing bid: ${offerPriceEth.toFixed(4)} ${currencySymbol} | Trait: ${traitDisplay} | Outbidding: ${incomingBidAmountEth.toFixed(4)} ${currencySymbol}` + RESET);
      console.log(GREEN + '----------------------------------------------------------------------------------------------------------------------------------------------' + RESET);

      seenOrderHashes.delete(orderHash);

      if (orders.length > 0) {
        const extractedOrderIds = await extractMagicedenOrderHash(orders)
        const orderKeys = orders.map((order: PrismaOrder) => order.id)
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
      const orders = await prisma.order.findMany({
        where: {
          taskId: task._id,
          marketplace: MAGICEDEN.toLowerCase(),
          identifier: 'collection'
        }
      })
      if (stopBid) {
        if (orders.length > 0) {
          const orderKeys = orders.map((order: PrismaOrder) => order.id)
          const extractedOrderIds = await extractMagicedenOrderHash(orders)
          queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey: WALLET_PRIVATE_KEY, orderKeys: orderKeys, taskId: task._id });
        }
        return
      }


      const offers = orders.map((order: PrismaOrder) => order.offerPrice)
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
            const orderKeys = orders.map((order: PrismaOrder) => order.id)
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

      const jobId = `counterbid-${task._id}-${task.contract.slug}-${MAGICEDEN.toLowerCase()}-collection`
      const job: Job = await queue.getJob(jobId)

      if (job) {
        if (incomingBidAmount <= job.data.offerPrice) {
          return
        } else {
          await job.updateData(bidData)
        }
      } else {
        await queue.add(
          MAGICEDEN_COLLECTION_BID_COUNTERBID,
          bidData,
          createJobOptions(jobId, MAGICEDEN_COLLECTION_BID_COUNTERBID)
        );
      }
      console.log(GREEN + '----------------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(GREEN + `[MagicEden Counterbid SUCCESS] Task: ${task._id} | Placing bid: ${offerPriceEth.toFixed(4)} ${currencySymbol} | Collection${traitDisplay} | Outbidding: ${incomingBidAmountEth.toFixed(4)} ${currencySymbol}` + RESET);
      console.log(GREEN + '----------------------------------------------------------------------------------------------------------------------------------------------' + RESET);

      seenOrderHashes.delete(orderHash);


      if (orders.length > 0) {
        const orderKeys = orders.map((order: PrismaOrder) => order.id)
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
      .includes("opensea")) return;

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


    if (!outbidOptions.counterbid) return
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

      if (!tokenBid || !tokenIds.includes(+tokenId)) return


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
      const orders = await prisma.order.findMany({
        where: {
          slug: task.contract.slug,
          taskId: task._id,
          marketplace: OPENSEA.toLowerCase(),
          identifier: tokenId.toString()
        }
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
                data: { privateKey: WALLET_PRIVATE_KEY, orderId: order.orderHash, taskId: task._id, orderKey: order.id },

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
              data: { privateKey: WALLET_PRIVATE_KEY, orderId: order.orderHash, taskId: task._id, orderKey: order.id },
            }
          })
          queue.addBulk(cancelData)
        }
        return
      }

      const offers = orders.map((order: PrismaOrder) => order.offerPrice)
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
                data: { privateKey: WALLET_PRIVATE_KEY, orderId: order.orderHash, taskId: task._id, orderKey: order.id },

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

      const jobId = `counterbid-${task._id}-${task.contract.slug}-${OPENSEA.toLowerCase()}-${asset.tokenId}`
      const job: Job = await queue.getJob(jobId)
      if (job) {
        if (incomingBidAmount <= job.data.offerPrice) {
          return
        } else {
          await job.updateData(bidData)
        }
      } else {
        await queue.add(
          OPENSEA_TOKEN_BID_COUNTERBID,
          bidData,
          createJobOptions(jobId, OPENSEA_TOKEN_BID_COUNTERBID)
        );
      }

      // Log successful counterbid preparation
      console.log(GREEN + '----------------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(GREEN + `[OpenSea Counterbid SUCCESS] Task: ${task._id} | Placing bid: ${offerPriceEth.toFixed(4)} ${currencySymbol} | Token: #${tokenId} | Outbidding: ${incomingBidAmountEth.toFixed(4)} ${currencySymbol}` + RESET);
      console.log(GREEN + '----------------------------------------------------------------------------------------------------------------------------------------------' + RESET);

      seenOrderHashes.delete(orderHash);

      if (orders.length > 0) {
        const cancelData = orders.map((order, index) => {
          return {
            name: CANCEL_OPENSEA_BID,
            data: { privateKey: WALLET_PRIVATE_KEY, orderId: order.orderHash, taskId: task._id, orderKey: order.id },
          }
        })
        queue.addBulk(cancelData)
      }
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

      const orders = await prisma.order.findMany({
        where: {
          slug: task.contract.slug,
          taskId: task._id,
          marketplace: OPENSEA.toLowerCase(),
          identifier
        }
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
                data: { privateKey: WALLET_PRIVATE_KEY, orderId: order.orderHash, taskId: task._id, orderKey: order.id },
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
              data: { privateKey: WALLET_PRIVATE_KEY, orderId: order.orderHash, taskId: task._id, orderKey: order.id },
            }
          })
          queue.addBulk(cancelData)
        }
        return
      }

      const offers = orders.map((order: PrismaOrder) => order.offerPrice)
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
                data: { privateKey: WALLET_PRIVATE_KEY, orderId: order.orderHash, taskId: task._id, orderKey: order.id },
              }
            })
            queue.addBulk(cancelData)
          }
        }
        return;
      }

      traitOffer = BigInt(offerPrice)
      const jobId = `counterbid-${task._id}-${task.contract.slug}-${OPENSEA.toLowerCase()}-${trait}`
      const job: Job = await queue.getJob(jobId)
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

      if (job) {
        if (incomingBidAmount <= job.data.offerPrice) {
          return
        } else {
          await job.updateData(counterbidData)
        }
      } else {
        await queue.add(
          OPENSEA_TRAIT_BID_COUNTERBID,
          counterbidData,
          createJobOptions(jobId, OPENSEA_TRAIT_BID_COUNTERBID)
        );
      }

      // Log successful trait counterbid preparation
      console.log(GREEN + '----------------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(GREEN + `[OpenSea Counterbid SUCCESS] Task: ${task._id} | Placing bid: ${offerPriceEth.toFixed(4)} ${currencySymbol} | Trait: ${identifier} | Outbidding: ${incomingBidAmountEth.toFixed(4)} ${currencySymbol}` + RESET);
      console.log(GREEN + '----------------------------------------------------------------------------------------------------------------------------------------------' + RESET);

      seenOrderHashes.delete(orderHash);


      if (orders.length > 0) {
        const cancelData = orders.map((order, index) => {
          return {
            name: CANCEL_OPENSEA_BID,
            data: { privateKey: WALLET_PRIVATE_KEY, orderId: order.orderHash, taskId: task._id, orderKey: order.id },
          }
        })
        queue.addBulk(cancelData)
      }
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

      const orders = await prisma.order.findMany({
        where: {
          slug: task.contract.slug,
          taskId: task._id,
          marketplace: OPENSEA.toLowerCase(),
          identifier: 'collection'
        }
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
                data: { privateKey: WALLET_PRIVATE_KEY, orderId: order.orderHash, taskId: task._id, orderKey: order.id },
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
              data: { privateKey: WALLET_PRIVATE_KEY, orderId: order.orderHash, taskId: task._id, orderKey: order.id },
            }
          })
          queue.addBulk(cancelData)
        }
        return
      }

      const offers = orders.map((order: PrismaOrder) => order.offerPrice)
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
                data: { privateKey: WALLET_PRIVATE_KEY, orderId: order.orderHash, taskId: task._id, orderKey: order.id },
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
      const jobId = `counterbid-${task._id}-${task.contract.slug}-${OPENSEA.toLowerCase()}-collection`
      const job: Job = await queue.getJob(jobId)

      if (job) {
        if (incomingBidAmount <= job.data.offerPrice) {
          return
        } else {
          await job.updateData(bidData)
        }
      } else {
        await queue.add(
          OPENSEA_COLLECTION_BID_COUNTERBID,
          bidData,
          createJobOptions(jobId, OPENSEA_COLLECTION_BID_COUNTERBID)
        );
      }
      console.log(GREEN + '----------------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(GREEN + `[OpenSea Counterbid SUCCESS] Task: ${task._id} | Placing bid: ${offerPriceEth.toFixed(4)} ${currencySymbol} | Collection${traitDisplay} | Outbidding: ${incomingBidAmountEth.toFixed(4)} ${currencySymbol}` + RESET);
      console.log(GREEN + '----------------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      seenOrderHashes.delete(orderHash);
      if (orders.length > 0) {
        const cancelData = orders.map((order, index) => {
          return {
            name: CANCEL_OPENSEA_BID,
            data: { privateKey: WALLET_PRIVATE_KEY, orderId: order.orderHash, taskId: task._id, orderKey: order.id },
          }
        })
        queue.addBulk(cancelData)
      }
    }

    seenOrderHashes.delete(orderHash);

  } catch (error) {
  }
}

async function handleBlurCounterbid(data: any, task: ITask) {
  const incomingBid = data
  try {
    // Include task ID in the hash to make it task-specific
    const bidHash = hashBid({ ...data, taskId: task._id });
    if (hasSeenItem(seenBids, bidHash)) return;
    addSeenItem(seenBids, bidHash);

    const currentTask = activeTasks.get(task._id)
    if (!currentTask || !currentTask?.running || !currentTask?.selectedMarketplaces?.map((marketplace) => marketplace?.toLowerCase())?.includes("blur") || !enabledMarketplaces?.map((marketplace) => marketplace?.toLowerCase())?.includes("blur")) return
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

    if (!currentTask?.outbidOptions?.counterbid) return

    if (traitBid) {
      const isTraitBid = currentTask?.bidType === "collection" && selectedTraits && Object.keys(selectedTraits)?.length > 0
      if (!isTraitBid) return
      const traits = transformBlurTraits(selectedTraits)
      const incomingTraitBids = incomingBid?.stats?.filter((item: any) => item?.criteriaType?.toLowerCase() === "trait") || incomingBid?.updates?.filter((item: any) => item?.criteriaType?.toLowerCase() === "trait")
      if (!incomingTraitBids) return
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

        const orders = await prisma.order.findMany({
          where: {
            slug: task.contract.slug,
            taskId: task._id,
            marketplace: BLUR.toLowerCase(),
            identifier: identifier
          }
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
                  data: { privateKey: WALLET_PRIVATE_KEY, payload, taskId: task._id, orderKey: order.id }
                }
              })
              queue.addBulk(cancelData)
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
                data: { privateKey: WALLET_PRIVATE_KEY, payload, taskId: task._id, orderKey: order.id }
              }
            })
            queue.addBulk(cancelData)
          }
          return
        }

        const offers = orders?.map((order: PrismaOrder) => order?.offerPrice)
        const currentBidPrice = !offers?.length ? 0 : Math.max(...offers?.map((offer) => Number(offer)))
        const currentBidPriceEth = currentBidPrice / 1e18
        if (incomingPrice <= currentBidPriceEth) return

        let offerPrice: any = Number((blurOutbidMargin + Number(incomingPrice))?.toFixed(2))
        offerPrice = parseEther(offerPrice?.toString())


        const jobId = `counterbid-${task._id}-${task.contract.slug}-${BLUR.toLowerCase()}-${trait}`
        const job: Job = await queue.getJob(jobId)
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


        if (job) {
          if (incomingPrice <= Number(job?.data?.offerPrice) / 1e18) {
            return
          } else {
            await job.updateData(data)
          }
        } else {
          await queue.add(
            BLUR_TRAIT_BID_COUNTERBID,
            bidData,
            {
              jobId,
            },
          );
        }

        // Log successful trait counterbid preparation
        console.log(GREEN + '------------------------------------------------------------------------------------------------------------------------------------------------------------------' + RESET);
        console.log(GREEN + `[Blur Counterbid SUCCESS] Task: ${task._id} | Placing bid: ${Number(offerPrice) / 1e18} ETH | Trait: ${identifier} | Outbidding: ${incomingPrice} ETH` + RESET);
        console.log(GREEN + '------------------------------------------------------------------------------------------------------------------------------------------------------------------' + RESET);
        seenBids.delete(bidHash)
      }

    } else {
      const collectionBid = task.bidType.toLowerCase() === "collection" && Object.keys(task?.selectedTraits?.blur || {}).length === 0
      if (!collectionBid) return

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

      const orders = await prisma.order.findMany({
        where: {
          slug: task.contract.slug,
          taskId: task._id,
          marketplace: BLUR.toLowerCase(),
          identifier: 'collection'
        },
        orderBy: {
          createdAt: 'desc'
        }
      })

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
          const cancelData = orders.map((order) => {
            const payload = JSON.parse(order.orderHash)
            return {
              name: CANCEL_BLUR_BID,
              data: {
                privateKey: WALLET_PRIVATE_KEY,
                payload,
                taskId: task._id,
                orderKey: order.id
              }
            }
          })
          queue.addBulk(cancelData);
        }
        return
      }

      const stopBid = await stopOption(currentTask, 'blur', Number(floor_price), true)
      if (stopBid) {
        if (orders.length > 0) {
          const cancelData = orders.map((order) => {
            const payload = JSON.parse(order.orderHash)
            return {
              name: CANCEL_BLUR_BID,
              data: {
                privateKey: WALLET_PRIVATE_KEY,
                payload,
                taskId: task._id,
                orderKey: order.id
              }
            }
          })
          queue.addBulk(cancelData);
        }
        return
      }
      const incomingPrice = Number(bestPrice) * 1e18
      const offers = orders?.map((order: PrismaOrder) => order?.offerPrice)
      const currentBidPrice = !offers?.length ? 0 : Math.max(...offers.map((offer) => Number(offer)))
      if (incomingPrice <= currentBidPrice) return

      const blurOutbidMargin = currentTask?.outbidOptions?.blurOutbidMargin || 0.01
      const expiry = getExpiry(currentTask?.bidDuration)
      const rawPrice = (+bestPrice + Number(blurOutbidMargin.toFixed(2))) * 1e18
      let offerPrice = BigInt(Math.round(rawPrice / 1e16) * 1e16)

      // Log successful collection counterbid preparation
      const traitDisplay = selectedTraits && Object.keys(selectedTraits).length > 0 ? ' (Trait bid)' : '';

      const jobId = `counterbid-${task._id}-${task.contract.slug}-${BLUR.toLowerCase()}-collection`
      const job: Job = await queue.getJob(jobId)
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


      if (job) {
        if (+incomingPrice <= +job?.data?.offerPrice) {
          return
        } else {
          await job.updateData(bidData)
        }
      } else {
        await queue.add(
          BLUR_COLLECTION_BID_COUNTERBID,
          bidData,
          {
            jobId,
          },

        );
      }
      console.log(GREEN + '------------------------------------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(GREEN + `[Blur Counterbid SUCCESS] Task: ${task._id} | Placing bid: ${Number(offerPrice) / 1e18} ETH | Collection${traitDisplay} | Outbidding: ${bestPrice} ETH` + RESET);
      console.log(GREEN + '------------------------------------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      seenBids.delete(bidHash)
    }
  } catch (error: any) {
    console.log(RED + `Error handling Blur counterbid: ${error?.message || error?.toString() || 'Unknown error'}` + RESET);
    if (error?.stack) {
      console.log(RED + `Stack trace: ${error.stack}` + RESET);
    }
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

async function processOpenseaScheduledBid(task: ITask, jobId?: string) {
  try {
    if (!enabledMarketplaces.map((m) => m.toLowerCase()).includes("opensea")) return
    const currentTask = activeTasks.get(task._id);
    if (!currentTask || !currentTask.running || !currentTask.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("opensea")) return;

    const taskId = task._id;
    // Processing OpenSea scheduled bid

    // Get preprocessing data
    const preprocessing = taskPreprocessing.get(taskId);
    if (!preprocessing || !preprocessing.opensea.enabled) {
      // No preprocessing data found for OpenSea
      return;
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
      if (!completedTasks[taskId]) {
        completedTasks[taskId] = { opensea: false, magiceden: false, blur: false };
      }
      completedTasks[taskId].opensea = true;
      return;
    }

    // Initialize task status and other tracking
    if (!completedTasks[taskId]) {
      completedTasks[taskId] = {
        opensea: false,
        magiceden: false,
        blur: false
      };
    }
    completedTasks[taskId].opensea = false;

    if (!taskStatus[taskId]) {
      taskStatus[taskId] = {
        status: 'STARTED',
        runningPercentage: 0,
        waitingTime: 0,
        cancellationPercentage: 0,
        processingPercentage: 0,
        nextLoopTime: new Date()
      }
    }
    taskStatus[taskId].status = 'PROCESSING'

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

    // Get collection details for creator fees
    const [{ currency }, collectionDetails] = await Promise.all([
      calculateBidPrice(currentTask, 0, "opensea"),
      getCollectionDetails(currentTask.contract.slug, currentTask.contract.contractAddress)
    ]);

    const creatorFees: IFee = collectionDetails.creator_fees.null !== undefined
      ? { null: collectionDetails.creator_fees.null }
      : Object.fromEntries(Object.entries(collectionDetails.creator_fees).map(([key, value]) => [key, Number(value)]));

    taskStatus[taskId].status = 'SCHEDULED'
    const newTask = { ...currentTask, traits: {} } as ITask

    // Create jobs based on preprocessing results
    if (preprocessing.opensea.tokens.length > 0) {
      // Token bid
      console.log(`${BLUE}[OPENSEA] Creating ${preprocessing.opensea.tokens.length} token bid jobs${RESET}`);

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

      await processBulkJobs(jobs, true);
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

      await processBulkJobs(traitJobs, true);
    } else if (preprocessing.opensea.jobCount === 1) {
      // Collection bid
      console.log(`${BLUE}[OPENSEA] Creating 1 collection bid job${RESET}`);
      const jobId = `bid-${taskId}-opensea-${currentTask.contract.slug}-collection`

      await queue.add(
        OPENSEA_COLLECTION_BID,
        { task: newTask, collectionDetails, currency },
        {
          ...createJobOptions(jobId, OPENSEA_COLLECTION_BID),
          priority: COLLECTION_BID_PRIORITY.OPENSEA
        } as JobsOptions
      );

      if (currentTask && currentTask.running) {
        taskStatus[taskId].status = 'QUEUED'
      }
    }
  } catch (error) {
    console.error(RED + `Error processing OpenSea scheduled bid for task: ${task._id}` + RESET, error);
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
  const taskId = data.task._id;

  // Check if task is blacklisted before processing
  if (isTaskBlacklisted(taskId)) {
    return;
  }

  taskStatus[taskId].status = 'RUNNING';
  const marketplace = 'opensea';

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
      prisma.order.findMany({
        where: {
          slug: currentTask.contract.slug,
          taskId: currentTask._id,
          marketplace: OPENSEA.toLowerCase(),
          identifier: 'collection',
        },
        orderBy: {
          createdAt: 'desc'
        }
      })
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
          data: { privateKey, orderId: order.orderHash, taskId: task._id, orderKey: order.id },
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
                data: { privateKey, orderId: order.orderHash, taskId: task._id, orderKey: order.id },
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
          data: { privateKey, orderId: order.orderHash, taskId: task._id, orderKey: order.id },
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
              data: { privateKey, orderId: order.orderHash, taskId: task._id, orderKey: order.id },
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
        currency
      )
    } catch (bidError) {
      console.error(RED + `OpenSea collection bid failed:`, bidError instanceof Error ? bidError.message : String(bidError) + RESET);
      throw bidError; // Re-throw to be caught by outer catch
    }

  } catch (error: any) {
    if (!errorStats[data.task._id]) {
      errorStats[data.task._id] = {
        magiceden: 0,
        opensea: 0,
        blur: 0
      }
    }
    // Format OpenSea error message for better readability
    let errorMessage = '';
    if (error?.response?.data?.errors && Array.isArray(error.response.data.errors)) {
      // Extract the first error message from OpenSea API errors
      errorMessage = error.response.data.errors[0]?.message || 'Unknown OpenSea error';
    } else {
      errorMessage = error?.message || String(error);
    }

    await logBidError(data.task._id, "OPENSEA COLLECTION BID ERROR", `Error processing OpenSea collection bid for task: ${data.task._id}: ${errorMessage}`, "error", "opensea");
    errorStats[data.task._id]['opensea']++
  } finally {
    // Always mark job as processed
    markJobAsProcessed(taskId, marketplace);
  }
}

async function processBlurScheduledBid(task: ITask) {
  try {
    const currentTask = activeTasks.get(task._id)
    if (!currentTask || !currentTask?.running || !currentTask.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("blur") || !enabledMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("blur")) return

    const taskId = task._id;
    // Processing Blur scheduled bid

    // Get preprocessing data
    const preprocessing = taskPreprocessing.get(taskId);
    if (!preprocessing || !preprocessing.blur.enabled) {
      // No preprocessing data found for Blur
      return;
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
      if (!completedTasks[taskId]) {
        completedTasks[taskId] = { opensea: false, magiceden: false, blur: false };
      }
      completedTasks[taskId].blur = true;
      return;
    }

    // Initialize task status and other tracking
    if (!completedTasks[taskId]) {
      completedTasks[taskId] = {
        opensea: false,
        magiceden: false,
        blur: false
      };
    }
    completedTasks[taskId].blur = false;

    if (!taskStatus[taskId]) {
      taskStatus[taskId] = {
        status: 'STARTED',
        runningPercentage: 0,
        waitingTime: 0,
        cancellationPercentage: 0,
        processingPercentage: 0,
        nextLoopTime: new Date()
      }
    }
    taskStatus[taskId].status = 'PROCESSING'

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

      if (!completedTasks[taskId]) {
        completedTasks[taskId] = {
          blur: false,
          magiceden: false,
          opensea: false
        }
      }
      completedTasks[taskId].blur = true;
      warningBids[taskId].blur = true;
      warningMessage[taskId].blur = `${currencySymbol} is not supported on Blur`;
      return
    }

    const expiry = getExpiry(currentTask.bidDuration)
    const WALLET_ADDRESS: string = currentTask.wallet;
    const wallets = await walletCache.getWalletById(task.walletId)
    const WALLET_PRIVATE_KEY: string = decryptPrivateKey(wallets?.encryptedPrivateKey as string)
    const contractAddress = currentTask.contract.contractAddress
    const newTask = { ...currentTask, traits: {} } as ITask

    // Create jobs based on preprocessing results
    if (preprocessing.blur.traits.length > 0) {
      // Trait bid
      // console.log(`${GOLD}[BLUR] Creating ${preprocessing.blur.traits.length} trait bid jobs${RESET}`);

      const traitJobs = preprocessing.blur.traits.map((trait) => ({
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
          task: newTask
        },
        opts: { priority: TRAIT_BID_PRIORITY.BLUR } as JobsOptions
      })) as Job[]

      await processBulkJobs(traitJobs, true);

    } else if (preprocessing.blur.jobCount === 1) {
      // Collection bid
      console.log(`${GOLD}[BLUR] Creating 1 collection bid job${RESET}`);
      const jobId = `bid-${taskId}-blur-${currentTask.contract.slug}-collection`

      await queue.add(
        BLUR_COLLECTION_BID,
        { task: newTask, contractAddress, expiry },
        {
          ...createJobOptions(jobId, BLUR_COLLECTION_BID),
          priority: COLLECTION_BID_PRIORITY.BLUR
        } as JobsOptions
      );

      if (currentTask && currentTask.running) {
        taskStatus[taskId].status = 'QUEUED'
      }
    }
  } catch (error) {
    console.error(RED + `Error processing Blur scheduled bid for task: ${task._id}` + RESET, error);
  }
}


async function processBlurCollectionBid(data: { task: ITask }) {
  const taskId = data.task._id;

  // Check if task is blacklisted before processing
  if (isTaskBlacklisted(taskId)) {
    return;
  }

  const marketplace = 'blur';
  taskStatus[taskId].status = 'RUNNING';

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

    const orders = await prisma.order.findMany({
      where: {
        slug: task.contract.slug,
        taskId: task._id,
        marketplace: BLUR.toLowerCase(),
        identifier: "collection"
      },
      orderBy: {
        createdAt: 'desc'
      }
    })
    const stopBid = await stopOption(currentTask, 'blur', Number(floor_price), false)
    if (stopBid) {
      if (orders.length > 0) {
        const cancelData = orders.map((order) => {
          const payload = JSON.parse(order.orderHash)

          return {
            name: CANCEL_BLUR_BID,
            data: { privateKey, payload, taskId: task._id, orderKey: order.id }
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
          data: { privateKey, payload, taskId: task._id, orderKey: order.id }
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
                data: { privateKey, payload, taskId: task._id, orderKey: order.id }
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

    const collectionOfferEth = Number((Number(colletionOffer) / 1e18).toFixed(2));

    if (collectionOfferEth < minBidPriceEth) {
      colletionOffer = BigInt(Math.round(minBidPriceEth * 1e18))
    }

    const bidCount = getIncrementedBidCount(BLUR, task.contract.slug, task._id)
    if (orders.length > 0) {
      const cancelData = orders.map((order) => {
        const payload = JSON.parse(order.orderHash)

        return {
          name: CANCEL_BLUR_BID,
          data: { privateKey, payload, taskId: task._id, orderKey: order.id }
        }
      })
      queue.addBulk(cancelData);
    }

    if (currentTask.outbidOptions.outbid && collectionOfferEth > maxBidPriceEth) {
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
              data: { privateKey, payload, taskId: task._id, orderKey: order.id }
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
      errorStats[data?.task?._id]['blur']++; // Count transfer restriction errors

      // Update tracking for transfer restriction
      return;
    }

    console.error('Blur collection bid error:', error instanceof Error ? error.message : String(error));
    await logBidError(data.task._id, "BLUR COLLECTION BID ERROR", `Error processing Blur collection bid for task: ${data.task._id}: ${error?.response?.data?.message || error?.response?.data || error?.message || String(error)}`, "error", "blur");
    errorStats[data?.task?._id]['blur']++
  } finally {
    // Always mark job as processed
    markJobAsProcessed(taskId, marketplace);
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

  taskStatus[data._id].status = 'RUNNING';

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

    const expiry = getExpiry(currentTask.bidDuration)
    const { type, value } = JSON.parse(trait);
    const identifier = `${type}:${value}`

    // Parallel fetch: floor price, trait floor (if needed), and orders
    const [floor_price, traitFloor, orders] = await Promise.all([
      getOpenseaFloor(slug),
      currentTask?.stopOptions?.traitPrices && currentTask?.stopOptions?.triggerStopOptions && identifier in currentTask?.stopOptions?.traitPrices
        ? openseaTraitFloor(slug, { traitType: type, values: [value] })
        : Promise.resolve(undefined),
      prisma.order.findMany({
        where: {
          slug: slug,
          taskId: _id,
          marketplace: OPENSEA.toLowerCase(),
          identifier
        },
        orderBy: {
          createdAt: 'desc'
        }
      })
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
            data: { privateKey, orderId: order.orderHash, taskId: _id, orderKey: order.id },
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
          data: { privateKey, orderId: order.orderHash, taskId: _id, orderKey: order.id },
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
                data: { privateKey, orderId: order.orderHash, taskId: _id, orderKey: order.id },
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
          data: { privateKey, orderId: order.orderHash, taskId: _id, orderKey: order.id },
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
              data: { privateKey, orderId: order.orderHash, taskId: _id, orderKey: order.id },
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
        trait
      )

    } catch (bidError: any) {
      // Format OpenSea error message for better readability
      if (bidError?.response?.data?.errors && Array.isArray(bidError.response.data.errors)) {
        // Extract the first error message from OpenSea API errors
        errorMessage = bidError.response.data.errors[0]?.message || 'Unknown OpenSea error';
      } else {
        errorMessage = bidError?.message || String(bidError);
      }

      console.error(RED + `OpenSea trait bid failed for ${_id} ${trait}:`, errorMessage + RESET);

      // Re-throw to be caught by outer catch block
      throw bidError;
    }

  } catch (error: any) {
    if (!errorStats[data.task._id]) {
      errorStats[data.task._id] = {
        magiceden: 0,
        opensea: 0,
        blur: 0
      }
    }

    if (!errorMessage) {
      // Format OpenSea error message for better readability
      if (error?.response?.data?.errors && Array.isArray(error.response.data.errors)) {
        // Extract the first error message from OpenSea API errors
        errorMessage = error.response.data.errors[0]?.message || 'Unknown OpenSea error';
      } else {
        errorMessage = error?.message || String(error);
      }
    }

    // Don't log bid errors for stopped/cancelled tasks
    if (errorMessage && !errorMessage.includes('Bid execution blocked - task') && !errorMessage.includes('is stopped')) {
      await logBidError(data.task._id, "OPENSEA TRAIT BID ERROR", `Error bidding on trait ${data.trait} in collection ${data.slug}: ${errorMessage}`, "error", "opensea");
      errorStats[data.task._id]['opensea']++
    }
  } finally {
    // Always mark job as processed
    markJobAsProcessed(data._id, 'opensea');
  }
}

async function processOpenseaTokenBid(data: IProcessOpenseaTokenBidData) {
  // Check if task is blacklisted before processing
  if (isTaskBlacklisted(data._id)) {
    return;
  }

  // Initialize timing tracker
  taskStatus[data._id].status = 'RUNNING'
  let errorMessage = ''
  try {

    if (!enabledMarketplaces.length) {
      return;
    }
    if (!enabledMarketplaces.map((m) => m.toLowerCase()).includes("opensea")) {
      return;
    }
    const { address, privateKey, slug, creatorFees, enforceCreatorFee, asset, _id } = data
    if (!currentTokenIds[_id]?.opensea?.includes(+asset.tokenId)) {
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
      errorStats[_id].opensea++;

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
      prisma.order.findMany({
        where: {
          slug: slug,
          taskId: _id,
          marketplace: OPENSEA.toLowerCase(),
          identifier: asset.tokenId.toString()
        },
        orderBy: {
          createdAt: 'desc'
        }
      })
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
            data: { privateKey, orderId: order.orderHash, taskId: _id, orderKey: order.id }
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
          data: { privateKey, orderId: order.orderHash, taskId: _id, orderKey: order.id },
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
                data: { privateKey, orderId: order.orderHash, taskId: _id, orderKey: order.id },
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
              data: { privateKey, orderId: order.orderHash, taskId: _id, orderKey: order.id }
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
          data: { privateKey, orderId: order.orderHash, taskId: _id, orderKey: order.id }
        }
      })
      queue.addBulk(cancelData); // Non-blocking
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
        Number(tokenOffer),
        creatorFees,
        enforceCreatorFee,
        expiry,
        currency,
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
            const tokenIndex = currentTokenIds[data._id].opensea.indexOf(+data.asset.tokenId);
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
      // Format OpenSea error message for better readability
      let errorMsg = '';
      if (error?.response?.data?.errors && Array.isArray(error.response.data.errors)) {
        // Extract the first error message from OpenSea API errors
        errorMsg = error.response.data.errors[0]?.message || 'Unknown OpenSea error';
      } else {
        errorMsg = error?.message || String(error);
      }
      errorMessage = errorMsg;
    }

    await logBidError(data._id, "OPENSEA TOKEN BID ERROR", `Error bidding on token ${data.asset.tokenId} in collection ${data.slug}: ${errorMessage}`, "error", "opensea");
    errorStats[data._id]['opensea']++
  } finally {
    // Always mark job as processed
    markJobAsProcessed(data._id, 'opensea');
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
      currency
    )
  } catch (error: any) {
    await logBidError(data.taskId, "OPENSEA COLLECTION COUNTER BID ERROR", `Error processing OpenSea collection counter bid for task: ${data.slug}: ${error?.response?.data?.errors?.[0] || error?.response?.data || error?.message || String(error)}`, "error", "opensea");
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
    await bidOnBlur(taskId, bidCount, address, privateKey, contractAddress, BigInt(offerPrice), slug, expiry);
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
      undefined,
      asset
    )

  } catch (error: any) {
    await logBidError(data.task._id, "OPENSEA TOKEN COUNTER BID ERROR", `Error bidding on token ${data.asset.tokenId} in collection ${data.slug}: ${error?.response?.data?.errors?.[0] || error?.response?.data || error?.message || String(error)}`, "error", "opensea");
  }
}

async function openseaTraitCounterBid(data: IProcessOpenseaTraitBidData) {
  try {
    if (!enabledMarketplaces.length) return
    const { _id, bidCount, address, privateKey, slug, offerPrice, creatorFees, enforceCreatorFee, expiry, trait } = data
    const currentTask = activeTasks.get(_id)
    if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("opensea") || !enabledMarketplaces.map((m) => m.toLowerCase()).includes("opensea")) return
    if (!trait) return

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
  task: ITask,
}) {
  const taskId = data._id;
  const marketplace = 'blur';
  taskStatus[taskId].status = 'RUNNING'
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
    const orders = await prisma.order.findMany({
      where: {
        slug: slug,
        taskId: _id,
        marketplace: BLUR.toLowerCase(),
        identifier
      },
      orderBy: {
        createdAt: 'desc'
      }
    })

    if (stopBid) {
      if (orders.length > 0) {
        const cancelData = orders.map((order) => {
          const payload = JSON.parse(order.orderHash)
          return {
            name: CANCEL_BLUR_BID,
            data: { privateKey, payload, taskId: _id, orderKey: order.id }
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
          data: { privateKey, payload, taskId: _id, orderKey: order.id }
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
                data: { privateKey, payload, taskId: _id, orderKey: order.id }
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

    const offerEth = Number(traitOffer) / 1e18;

    const bidCount = getIncrementedBidCount(BLUR, slug, _id)
    if (orders.length > 0) {
      const cancelData = orders.map((order) => {
        const payload = JSON.parse(order.orderHash)

        return {
          name: CANCEL_BLUR_BID,
          data: { privateKey, payload, taskId: _id, orderKey: order.id }
        }
      })
      queue.addBulk(cancelData);
    }

    if (outbidOptions.outbid && offerEth > maxBidPriceEth) {
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
              data: { privateKey, payload, taskId: _id, orderKey: order.id }
            }
          })
          queue.addBulk(cancelData);
        }
      }

      return;
    } else if (offerEth < minBidPriceEth) {
      traitOffer = BigInt(Math.round(minBidPriceEth * 1e18));
    }


    await bidOnBlur(_id, bidCount, address, privateKey, contractAddress, traitOffer, slug, expiry, trait);
  } catch (error: any) {
    // Handle rate limit errors
    if (error?.code === 'RATE_LIMITED') {
      console.log(`[Blur] Rate limited for trait bid ${data.slug} ${data.trait} - will retry`);
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
      console.log(`Skipping Blur trait bid for ${data.slug} - transfer restricted collection`);
      await logBidError(data.task._id, "BLUR TRANSFER RESTRICTION", error.message, "warning", "blur");
      errorStats[data?.task?._id]['blur']++; // Count transfer restriction errors

      return;
    }

    console.error('Blur trait bid error:', error instanceof Error ? error.message : String(error));
    
    // Don't log bid errors for stopped/cancelled tasks
    const errorMessage = error?.message || String(error);
    if (!errorMessage.includes('Bid execution blocked - task') && !errorMessage.includes('is stopped')) {
      errorStats[data?.task?._id]['blur']++
      await logBidError(data.task._id, "BLUR TRAIT BID ERROR", `Error bidding on trait ${data.trait} in collection ${data.slug}: ${error?.response?.data?.message || error?.response?.data || errorMessage}`, "error", "blur");
    }
  } finally {
    // Always mark job as processed
    markJobAsProcessed(taskId, marketplace);
  }
}

async function processMagicedenScheduledBid(task: ITask, jobId?: string) {
  const checkpointStart = Date.now();

  try {
    if (!enabledMarketplaces.map((m) => m.toLowerCase()).includes("magiceden")) return
    const currentTask = activeTasks.get(task._id);
    if (!currentTask || !currentTask?.running || !currentTask?.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("magiceden")) return

    const taskId = task._id;
    // Processing MagicEden scheduled bid

    // Get preprocessing data
    const preprocessing = taskPreprocessing.get(taskId);
    if (!preprocessing || !preprocessing.magiceden.enabled) {
      // No preprocessing data found for MagicEden
      return;
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
      if (!completedTasks[taskId]) {
        completedTasks[taskId] = { opensea: false, magiceden: false, blur: false };
      }
      completedTasks[taskId].magiceden = true;
      return;
    }

    // Initialize task status and other tracking
    if (!completedTasks[taskId]) {
      completedTasks[taskId] = {
        opensea: false,
        magiceden: false,
        blur: false
      };
    }
    completedTasks[taskId].magiceden = false;

    if (!taskStatus[taskId]) {
      taskStatus[taskId] = {
        status: 'STARTED',
        runningPercentage: 0,
        waitingTime: 0,
        cancellationPercentage: 0,
        processingPercentage: 0,
        nextLoopTime: new Date()
      }
    }
    taskStatus[taskId].status = 'PROCESSING'

    // Update currentTokenIds if we have token data
    if (!currentTokenIds[taskId]) {
      currentTokenIds[taskId] = {
        opensea: [],
        magiceden: [],
      }
    }
    currentTokenIds[taskId].magiceden = preprocessing.magiceden.tokens

    const WALLET_ADDRESS: string = currentTask.wallet;
    const wallets = await walletCache.getWalletById(currentTask.walletId);
    const WALLET_PRIVATE_KEY: string = decryptPrivateKey(wallets?.encryptedPrivateKey as string)
    const contractAddress = currentTask.contract.contractAddress
    const expiration = getExpiry(currentTask.bidDuration)
    const newTask = { ...currentTask, traits: {} } as ITask
    taskStatus[taskId].status = 'SCHEDULED'

    // Create jobs based on preprocessing results
    if (preprocessing.magiceden.tokens.length > 0) {
      // Token bid
      console.log(`${MAGENTA}[MAGICEDEN] Creating ${preprocessing.magiceden.tokens.length} token bid jobs${RESET}`);

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
          opts: { priority: TOKEN_BID_PRIORITY.MAGICEDEN } as JobsOptions
        })) as Job[]
      await processBulkJobs(jobs, true);
    }
    else if (preprocessing.magiceden.traits.length > 0) {
      // Trait bid
      console.log(`${MAGENTA}[MAGICEDEN] Creating ${preprocessing.magiceden.traits.length} trait bid jobs${RESET}`);

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
      await processBulkJobs(traitJobs, true);
    }
    else if (preprocessing.magiceden.jobCount === 1) {
      // Collection bid
      console.log(`${MAGENTA}[MAGICEDEN] Creating 1 collection bid job${RESET}`);
      const jobId = `bid-${taskId}-magiceden-${currentTask.contract.slug}-collection`

      await queue.add(
        MAGICEDEN_COLLECTION_BID,
        { task: newTask },
        {
          ...createJobOptions(jobId, MAGICEDEN_COLLECTION_BID),
          priority: COLLECTION_BID_PRIORITY.MAGICEDEN
        } as JobsOptions
      );
      if (currentTask && currentTask.running) {
        taskStatus[taskId].status = 'QUEUED'
      }
    }
  } catch (error) {
    console.error(RED + `Error processing MagicEden scheduled bid for task: ${task._id} (${Date.now() - checkpointStart}ms)` + RESET, error);
    // Log specific error details to help diagnose timeouts
    if (error instanceof Error && error.message.includes('timeout')) {
      console.error(RED + `[MAGICEDEN_SCHEDULE] Timeout details: ${error.message}` + RESET);
    }
  }
}

async function processMagicedenCollectionBid(data: { task: ITask }) {
  const taskId = data.task._id;

  // Check if task is blacklisted before processing
  if (isTaskBlacklisted(taskId)) {
    return;
  }

  taskStatus[taskId].status = 'RUNNING';
  const marketplace = 'magiceden';

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
    const wallets = await walletCache.getWalletById(task.walletId)
    const privateKey: string = decryptPrivateKey(wallets?.encryptedPrivateKey as string)
    const floor_price = Number(await fetchMagicEdenCollectionStats(task.contract.contractAddress))

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
    const orders = await prisma.order.findMany({
      where: {
        slug: task.contract.slug,
        taskId: task._id,
        marketplace: MAGICEDEN.toLowerCase(),
        identifier: 'collection',
      },
      orderBy: {
        createdAt: 'desc'
      }
    })

    if (stopBid) {
      if (orders.length > 0) {
        const orderKeys = orders.map(order => order.id)
        const extractedOrderIds = await extractMagicedenOrderHash(orders)
        queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey, orderKeys: orderKeys, taskId: task._id });
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
      const orderIds = remainingOrders.map(order => order.id)
      const extractedOrderIds = await extractMagicedenOrderHash(remainingOrders)
      queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey, orderKeys: orderIds, taskId: task._id });
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
      const [topOffer, secondOffer] = await fetchMagicEdenOffer("COLLECTION", task.contract.contractAddress, "{}") as unknown as OpenseaOfferData[]
      const walletsArr = await walletCache.getAllWallets()

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
            const orderKeys = orders.map((order: PrismaOrder) => order.id)
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

      await logBidError(task._id, "OFFER EXCEEDS FLOOR PRICE", message, "warning", "magiceden");
      console.log(YELLOW + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(YELLOW + message + RESET);
      console.log(YELLOW + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      return;
    }

    const bidCount = getIncrementedBidCount(MAGICEDEN, task.contract.slug, task._id)

    if (orders.length > 0) {
      const orderKeys = orders.map(order => order.id)
      const extractedOrderIds = await extractMagicedenOrderHash(orders)
      queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey, orderKeys: orderKeys, taskId: task._id });
    }

    const outbidOptions = currentTask.outbidOptions

    if (outbidOptions.outbid && offerEth > maxBidPriceEth) {
      if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
        if (orders.length > 0) {
          const orderKeys = orders.map((order: PrismaOrder) => order.id)
          const extractedOrderIds = await extractMagicedenOrderHash(orders)
          queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, orderKeys, taskId: task._id });
        }
      }
      const message = `❌ Offer price ${offerEth} ${currencySymbol} for ${task.contract.slug} COLLECTION bid exceeds max bid price ${maxBidPriceEth} ${currencySymbol} FOR MAGICEDEN.Skipping ...`.toUpperCase()
      console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(RED + message + RESET);
      console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      logBidError(task._id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "magiceden");

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
    await logBidError(data.task._id, "MAGICEDEN COLLECTION BID ERROR", `Error processing MagicEden collection bid for task: ${data.task._id}: ${error?.response?.data?.message || error?.message || String(error)}`, "error", "magiceden");
    if (!errorStats[data.task._id]) {
      errorStats[data.task._id] = {
        magiceden: 0,
        opensea: 0,
        blur: 0
      }
    }
    errorStats[data.task._id]['magiceden']++
  } finally {
    // Always mark job as processed
    markJobAsProcessed(taskId, marketplace);
  }
}


async function processMagicedenTokenBid(data: IMagicedenTokenBidData) {
  try {
    const { contractAddress, address, quantity, privateKey, slug, tokenId, task, _id } = data
    taskStatus[_id].status = 'RUNNING';

    if (!enabledMarketplaces.length) return

    if (!currentTokenIds[_id]?.magiceden?.includes(+tokenId)) {
      return
    }
    const currentTask = activeTasks.get(_id)
    if (!currentTask || !currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("magiceden") || !enabledMarketplaces.map((m) => m.toLowerCase()).includes("magiceden")) {
      return;
    }
    const outbidOptions = currentTask.outbidOptions
    const floor_price = Number(await fetchMagicEdenCollectionStats(task.contract.contractAddress))
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
    const orders = await prisma.order.findMany({
      where: {
        slug,
        taskId: _id,
        identifier: tokenId.toString(),
        marketplace: MAGICEDEN.toLowerCase()
      },
      orderBy: {
        createdAt: 'desc'
      }
    })

    if (stopBid) {
      if (orders.length > 0) {
        const orderKeys = orders.map((order: PrismaOrder) => order.id)
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
      const orderKeys = remainingOrders.map((order: PrismaOrder) => order.id)
      queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey, orderKeys, taskId: task._id });
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
            const orderKeys = orders.map((order: PrismaOrder) => order.id)
            const extractedOrderIds = await extractMagicedenOrderHash(orders)
            queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey, orderKeys, taskId: _id });
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
            const orderKeys = orders.map((order: PrismaOrder) => order.id)
            const extractedOrderIds = await extractMagicedenOrderHash(orders)
            queue.add(CANCEL_MAGICEDEN_BID, {
              orderIds: extractedOrderIds,
              privateKey,
              orderKeys,
              taskId: _id
            });
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
          const orderKeys = orders.map((order: PrismaOrder) => order.id)
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
      const orderKeys = orders.map((order: PrismaOrder) => order.id)
      const extractedOrderIds = await extractMagicedenOrderHash(orders)
      queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey, orderKeys, taskId: _id });
    }

    await bidOnMagiceden(_id, bidCount, address, contractAddress, quantity, tokenOffer.toString(), privateKey, slug, expiration, currency, undefined, tokenId);
  } catch (error: any) {
    await logBidError(data._id, "MAGICEDEN TOKEN BID ERROR", `Error bidding on token ${data.tokenId} in collection ${data.slug}: ${error?.response?.data?.message || error?.message || String(error)}`, "error", "magiceden");
    if (!errorStats[data.task._id]) {
      errorStats[data.task._id] = {
        magiceden: 0,
        opensea: 0,
        blur: 0
      }
    }
    errorStats[data.task._id]['magiceden']++
  } finally {
    // Always mark job as processed
    markJobAsProcessed(data._id, 'magiceden');
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
  taskStatus[data._id].status = 'RUNNING';
  if (!enabledMarketplaces.length) return
  if (!enabledMarketplaces.map((m) => m.toLowerCase()).includes("magiceden")) return

  try {
    // Check if task is still active
    const task = activeTasks.get(data._id);
    if (!task || !task.running) {
      console.log(`[MagicEden Trait] Skipping bid - task ${data._id} is no longer active or running`);
      return;
    }

    // Check if task status is COMPLETED or STOPPED
    if (taskStatus[data._id]?.status === 'COMPLETED' || taskStatus[data._id]?.status === 'STOPPED') {
      return;
    }

    const
      { contractAddress, address, quantity, privateKey, slug, trait, _id } = data

    // Skip trait validation - preprocessing already determined valid traits
    // The trait passed in the job data is already validated

    const currentTask = activeTasks.get(_id)
    if (!currentTask || !currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("magiceden")) return;

    const outbidOptions = currentTask.outbidOptions

    const floor_price = Number(await fetchMagicEdenCollectionStats(currentTask.contract.contractAddress))
    if (!floor_price) return
    const { offerPriceEth, maxBidPriceEth, minBidPriceEth, currencySymbol, currency, decimal } = await calculateBidPrice(currentTask, floor_price as number, "magiceden")

    const identifier = `${trait.attributeKey}:${trait.attributeValue}`

    const traitFloor = currentTask?.stopOptions?.traitPrices && currentTask?.stopOptions?.triggerStopOptions && identifier in currentTask?.stopOptions?.traitPrices ? await fetchMagicEdenTraitFloor(contractAddress, { attributeKey: trait.attributeKey, attributeValue: trait.attributeValue }) : undefined
    const orders = await prisma.order.findMany({
      where: {
        slug,
        taskId: _id,
        identifier: identifier,
        marketplace: MAGICEDEN.toLowerCase()
      },
      orderBy: {
        createdAt: 'desc'
      }
    })

    const stopBid = await stopOption(currentTask, 'magiceden', Number(floor_price), false, traitFloor, identifier)

    if (stopBid) {
      if (orders.length > 0) {
        const orderKeys = orders.map((order: PrismaOrder) => order.id)
        const extractedOrderIds = await extractMagicedenOrderHash(orders)
        queue.add(CANCEL_MAGICEDEN_BID, {
          orderIds: extractedOrderIds,
          privateKey,
          orderKeys,
          taskId: _id
        });
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
      const orderKeys = remainingOrders.map((order: PrismaOrder) => order.id)
      const extractedOrderIds = await extractMagicedenOrderHash(remainingOrders)
      queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey, orderKeys, taskId: currentTask._id });
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
            const orderKeys = orders.map((order: PrismaOrder) => order.id)
            const extractedOrderIds = await extractMagicedenOrderHash(orders)
            queue.add(CANCEL_MAGICEDEN_BID, {
              orderIds: extractedOrderIds,
              privateKey,
              orderKeys,
              taskId: _id
            });
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
            const orderKeys = orders.map((order: PrismaOrder) => order.id)
            const extractedOrderIds = await extractMagicedenOrderHash(orders)
            queue.add(CANCEL_MAGICEDEN_BID, {
              orderIds: extractedOrderIds,
              privateKey,
              orderKeys,
              taskId: _id
            });
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
      const orderKeys = orders.map((order: PrismaOrder) => order.id)
      const extractedOrderIds = await extractMagicedenOrderHash(orders)
      queue.add(CANCEL_MAGICEDEN_BID, {
        orderIds: extractedOrderIds,
        privateKey,
        orderKeys,
        taskId: _id
      });
    }
    if (outbidOptions.outbid && offerEth > maxBidPriceEth) {
      if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
        if (orders.length > 0) {
          const orderKeys = orders.map((order: PrismaOrder) => order.id)
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
      await logBidError(data._id, "MAGICEDEN TRAIT BID ERROR", `Error bidding on trait ${JSON.stringify(data.trait)} in collection ${data.slug}: ${error?.response?.data?.message || error?.response?.data || errorMessage}`, "error", "magiceden");
      if (!errorStats[data._id]) {
        errorStats[data._id] = {
          magiceden: 0,
          opensea: 0,
          blur: 0
        }
      }
      errorStats[data._id]['magiceden']++
    }
  } finally {
    // Always mark job as processed
    markJobAsProcessed(data._id, 'magiceden');
  }
}

async function bulkCancelOpenseaBid(data: { privateKey: string, orderId: string, taskId: string, orderKey: string }) {
  const { privateKey, orderId, taskId, orderKey } = data
  const parentJobId = `${taskId}-CANCEL_OPENSEA_BID`;
  const parentJob = cancellationParentJobs.get(parentJobId);

  try {
    await Promise.all([
      prisma.order.deleteMany({
        where: {
          id: orderKey
        }
      }),
      cancelOrder(orderId, OPENSEA_PROTOCOL_ADDRESS, privateKey, taskId)
    ])

    // Update success counter and percentage (1 bid per job for OpenSea)
    if (parentJob) {
      parentJob.completedJobs++;
      parentJob.completedBids = (parentJob.completedBids || 0) + 1;
      updateTaskCancellationPercentage(taskId);
    }
  } catch (error) {
    console.error(RED + `Error processing Opensea bid cancellation for task: ${taskId}` + RESET, error);

    // Update failure counter and percentage
    if (parentJob) {
      parentJob.failedJobs++;
      parentJob.failedBids = (parentJob.failedBids || 0) + 1;
      updateTaskCancellationPercentage(taskId);
    }

    throw error; // Re-throw to maintain existing behavior
  }
}

async function bulkCancelMagicedenBid(data: { orderIds: string[], privateKey: string, orderKeys: string[], taskId: string }) {
  const { orderIds, privateKey, orderKeys, taskId } = data
  const parentJobId = `${taskId}-CANCEL_MAGICEDEN_BID`;
  const parentJob = cancellationParentJobs.get(parentJobId);
  const bidCount = orderIds.length; // Number of bids in this batch

  try {
    if (orderIds.length > 0) {
      await Promise.all([
        cancelMagicEdenBid(orderIds, privateKey, taskId),
        prisma.order.deleteMany({
          where: {
            id: {
              in: orderKeys
            }
          }
        })
      ])
    }

    // Update success counter and percentage (multiple bids per job for MagicEden)
    if (parentJob) {
      parentJob.completedJobs++;
      parentJob.completedBids = (parentJob.completedBids || 0) + bidCount;
      updateTaskCancellationPercentage(taskId);
    }
  } catch (error) {
    console.error(RED + `Error processing Magiceden bid cancellation for task: ${taskId}` + RESET, error);

    // Update failure counter and percentage
    if (parentJob) {
      parentJob.failedJobs++;
      parentJob.failedBids = (parentJob.failedBids || 0) + bidCount;
      updateTaskCancellationPercentage(taskId);
    }

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
    const oneMinuteFromNow = new Date(Date.now() + 10 * 1000);

    // Batch process all tasks in a single query using Prisma's groupBy
    const [orderCounts, expiringOrders] = await Promise.all([
      // Get counts for all tasks grouped by taskId and marketplace
      prisma.order.groupBy({
        by: ['taskId', 'marketplace'],
        where: {
          taskId: { in: taskIds },
          expiresAt: { gt: now }
        },
        _count: {
          _all: true
        }
      }),

      // Get all expiring orders across all tasks
      prisma.order.findMany({
        where: {
          taskId: { in: taskIds },
          expiresAt: { lte: oneMinuteFromNow }
        },
        select: {
          taskId: true,
          id: true
        }
      })
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
      const marketplace = group.marketplace.toLowerCase();
      if (bidStatsMap[group.taskId] && (marketplace === 'opensea' || marketplace === 'magiceden' || marketplace === 'blur')) {
        bidStatsMap[group.taskId][marketplace as 'opensea' | 'magiceden' | 'blur'] = group._count._all;
      }
    }

    // Update global bidStats
    Object.assign(bidStats, bidStatsMap);

    // Process expiring orders by task
    if (expiringOrders.length > 0) {
      const tasksToRestart = new Set<string>();
      const orderIdsToDelete: string[] = [];

      for (const order of expiringOrders) {
        tasksToRestart.add(order.taskId);
        orderIdsToDelete.push(order.id);
      }

      // Batch delete expiring orders
      await prisma.order.deleteMany({
        where: {
          id: { in: orderIdsToDelete }
        }
      });

      // Restart affected tasks
      const restartPromises = Array.from(tasksToRestart).map(taskId => {
        const task = currentTasks.find(t => t._id === taskId);
        if (task) {
          return startTask(task, true);
        }
        return null;
      }).filter(Boolean);

      await Promise.allSettled(restartPromises);
    }

  } catch (error: any) {
    // Handle specific Prisma errors
    if (error?.code === 'P2021') {
      console.log(YELLOW + `Order table doesn't exist. Run 'npx prisma migrate dev' to create it.` + RESET);
      // Set all bid stats to 0 when table doesn't exist
      for (const task of currentTasks) {
        bidStats[task._id] = {
          opensea: 0,
          magiceden: 0,
          blur: 0
        };
      }
    } else {
      await logger.logError(ErrorCategory.DATABASE, 'Failed to validate bid counts', error);
      console.error(RED + `Error validating bid counts` + RESET, error);
    }
  }
}

validateBidCountInterval = setInterval(validateBidCount, 5000);

async function bulkCancelBlurBid(data: BlurCancelPayload) {
  const { orderKey, privateKey, taskId, payload } = data
  const parentJobId = `${taskId}-CANCEL_BLUR_BID`;
  const parentJob = cancellationParentJobs.get(parentJobId);

  try {
    if (!data) return
    await Promise.all([
      prisma.order.deleteMany({
        where: {
          id: orderKey
        }
      }),
      cancelBlurBid({ payload, privateKey, taskId })
    ])

    // Update success counter and percentage (1 bid per job for Blur)
    if (parentJob) {
      parentJob.completedJobs++;
      parentJob.completedBids = (parentJob.completedBids || 0) + 1;
      updateTaskCancellationPercentage(taskId);
    }
  } catch (error) {
    console.error(RED + `Error processing Blur bid cancellation for task: ${taskId}` + RESET, error);

    // Update failure counter and percentage
    if (parentJob) {
      parentJob.failedJobs++;
      parentJob.failedBids = (parentJob.failedBids || 0) + 1;
      updateTaskCancellationPercentage(taskId);
    }

    throw error; // Re-throw to maintain existing behavior
  }
}

app.get("/", (req, res) => {
  res.json({ message: "Welcome to the NFTTools bidding bot server! Let's make magic happen! 🚀🚀🚀" });
});




app.get("/health", async (req, res) => {
  try {
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
          in: marketplace.map((m: any) => m.toLowerCase())
        };
      } else if (typeof marketplace === 'string' && marketplace.includes(',')) {
        // Handle comma-separated string
        whereClause.marketplace = {
          in: marketplace.split(',').map(m => m.trim().toLowerCase())
        };
      } else {
        whereClause.marketplace = (marketplace as string).toLowerCase();
      }
    }

    // Get total count for pagination metadata
    const totalCount = await prisma.order.count({ where: whereClause });

    // Fetch paginated orders
    const orders = await prisma.order.findMany({
      where: whereClause,
      skip,
      take: limitedPageSize,
      orderBy: { createdAt: 'desc' }
    });

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
    const orders: PrismaOrder[] = req.body
    const openseaOrders = orders.filter((order: any) => order.marketplace === OPENSEA.toLowerCase())
    const magicedenOrders = orders.filter((order: any) => order.marketplace === 'magiceden')
    const blurOrders = orders.filter((order: any) => order.marketplace === 'blur')

    if (openseaOrders.length > 0) {
      const cancelData = openseaOrders.map((order) => {
        return {
          name: CANCEL_OPENSEA_BID,
          data: { privateKey: WALLET_PRIVATE_KEY, orderId: order.orderHash, taskId: task._id, orderKey: order.id }
        }
      })
      queue.addBulk(cancelData); // Non-blocking
    }

    if (magicedenOrders.length > 0) {
      const extractedOrderIds = await extractMagicedenOrderHash(magicedenOrders)
      const orderKeys = magicedenOrders.map((order) => order.id)
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
          data: { privateKey: WALLET_PRIVATE_KEY, orderId: order.orderHash, taskId: task._id, orderKey: order.id }
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

    // Store password in memory only
    setPassword(password);

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

    // Check if request is from the allowed server IP
    const clientIP = req.ip || req.socket.remoteAddress;
    if (clientIP !== SERVER_IP && clientIP !== `::ffff:${SERVER_IP}`) {
      await logger.logSecurityEvent('BLOCKED_UPDATE_IP', 'Update attempt from unauthorized IP', {
        ip: clientIP,
        allowedIP: SERVER_IP,
        endpoint: '/api/update-app'
      });
      return res.status(403).json({ error: 'Forbidden: Updates only allowed from server IP' });
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
  taskLoopCounters.forEach((count, taskId) => {
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
    offerPriceEth = Math.floor(offerPriceEth * 100) / 100;
    maxBidPriceEth = Math.floor(maxBidPriceEth * 100) / 100;
    minBidPriceEth = Math.floor(minBidPriceEth * 100) / 100;
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
    const existingApproval = await prisma.wallet.findUnique({ where: { address, currency } });

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
          await prisma.wallet.upsert({
            where: { address },
            update: updateData,
            create: {
              address,
              currency: currency,
              ...updateData
            }
          });
        } else {
          await prisma.wallet.update({ where: { address }, data: updateData });
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
    // Update local ownBidCounts object for immediate broadcast
    if (!ownBidCounts[marketplace]) {
      ownBidCounts[marketplace] = 0;
    }
    ownBidCounts[marketplace]++;

    console.log(`🔄 Own bid detected - ${marketplace.toUpperCase()}: ${ownBidCounts[marketplace]} total bids for task ${taskId}`);
    console.log(`[DEBUG] Updated ownBidCounts:`, ownBidCounts);
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


/**
 * Network Monitoring System
 * 
 * This code implements a network connectivity monitoring system that manages the application's
 * behavior during network outages. Here's what it does:
 * 
 * 1. Tracks network state with two variables:
 *    - previousNetState: Remembers if network was previously available
 *    - lastConnectedTime: Records timestamp of last successful connection
 * 
 * 2. watchNetworkState() function:
 *    - Compares current and previous network states
 *    - When network is restored:
 *      a. Pauses the queue temporarily
 *      b. Drains existing jobs
 *      c. Resumes queue and workers
 *      d. Restarts the scheduled task loop
 *    - When network is lost:
 *      a. Pauses queue and workers to prevent failed operations
 *    - Requires 5 minutes of continuous disconnection before taking action
 * 
 * 3. checkNetwork() function:
 *    - Creates a socket connection to google.com:80 to test connectivity
 *    - Updates netEnabled flag based on connection success/failure
 *    - Calls watchNetworkState() to take appropriate action
 * 
 * 4. Runs network checks:
 *    - Initial check at startup
 *    - Periodic checks every 30 seconds
 * 
 * This system ensures the application gracefully handles network interruptions
 * by pausing operations during outages and resuming them when connectivity returns.
 */

let previousNetState = true; // Track previous network state
let lastConnectedTime = Date.now(); // Track last successful connection time

async function watchNetworkState() {
  const now = Date.now();
  const timeSinceLastConnection = now - lastConnectedTime;

  if (!netEnabled && timeSinceLastConnection < 15000) {
    return; // Wait for the full minute confirmation
  }

  if (previousNetState !== netEnabled) {
    if (netEnabled) {
      // Network restored
      console.log('Network restored, resuming operations...');

      // Resume the queue first
      try {
        await queue.resume();
        console.log(GREEN + 'Queue resumed after network restoration' + RESET);
      } catch (err) {
        console.error('Failed to resume queue:', err);
      }

      // Then restart the scheduled loop
      runScheduledLoop().catch(err => {
        console.error('Failed to restart scheduled loop:', err);
      });
    } else {
      // Network lost after a full minute
      console.log('Network lost for a full minute, pausing operations...');
      try {
        await queue.pause();
        // Add a small delay to ensure queue is fully paused
        await new Promise(resolve => setTimeout(resolve, 1000));
        await queue.obliterate({ force: true });
      } catch (err) {
        console.error('Error while pausing/obliterating queue:', err);
      }
    }

    previousNetState = netEnabled;
  }
}

function checkNetwork() {
  const socket = new net.Socket();
  socket.setTimeout(5000);

  socket.on('connect', () => {
    // Network is connected
    netEnabled = true;
    lastConnectedTime = Date.now(); // Update last successful connection time
    socket.destroy();
    watchNetworkState();
  });

  socket.on('error', () => {
    console.log('Network is not connected');
    netEnabled = false;
    watchNetworkState();
  });

  socket.connect(80, 'google.com');
}

// Periodic network checks
setInterval(checkNetwork, 30000); // Check every 30 seconds
checkNetwork(); // Initial check




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


const fetchTraitData = async (collectionSlug: string) => {
  try {
    const data = {
      id: "TraitSelectorQuery",
      query:
        "query TraitSelectorQuery(\n  $collectionSlug: CollectionSlug\n  $withTraitFloor: Boolean\n) {\n  collection(collection: $collectionSlug) {\n    ...TraitSelector_data_4zPn1c\n    id\n  }\n}\n\nfragment TraitSelector_data_4zPn1c on CollectionType {\n  statsV2 {\n    totalSupply\n  }\n  stringTraits(withTraitFloor: $withTraitFloor) {\n    key\n    counts {\n      count\n      value\n      floor {\n        eth\n        unit\n        symbol\n        usd\n      }\n    }\n  }\n}\n",
      variables: {
        collectionSlug: collectionSlug,
        withTraitFloor: true,
      },
    };

    const { data: openseaTraits } = await limiter.schedule(() => axiosInstance.post<OpenseaTraits>(
      `${BASE_URL}/opensea/__api/graphql/`,
      data,
      {
        headers: {
          "x-api-key": API_KEY,
          "content-type": "application/json",
          "x-signed-query":
            "6ae240a98f748a2ecef0a71cb6424c3d47c9c62691ca07ebadf318bf7fcc9517",
        },
      }
    ));

    console.log({ openseaTraits: JSON.stringify(openseaTraits) });

    return openseaTraits;
  } catch (error: any) {
    console.error("Error fetching trait data:", error.response.data);
  }
};

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

