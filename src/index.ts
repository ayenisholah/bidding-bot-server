/* eslint-disable */
import express from "express";
import { config } from "dotenv";

config()

import bodyParser from "body-parser";
import { clearTimeout } from 'timers';
import crypto, { randomUUID } from 'crypto';
import os from "os"
import cors from "cors";
import helmet from "helmet";
import stringify from 'json-stable-stringify';
import WebSocket, { Server as WebSocketServer } from 'ws';
import http from 'http';
import { axiosInstance, initialize, limiter } from "./init";
import { bidOnOpensea, cancelOpenseaRequest, cancelOrder, fetchItemOffers, fetchOpenseaListings, fetchOpenseaOffers, IFee, OpenseaOfferData, openseaTraitFloor } from "./marketplace/opensea";
import { bidOnBlur, cancelBlurBid, cancelBlurRequest, fetchBlurBid, fetchBlurCollectionStats, fetchBlurTraitFloor } from "./marketplace/blur/bid";
import { bidOnMagiceden, cancelMagicEdenBid, cancelMagicedenRequest, fetchMagicEdenCollectionStats, fetchMagicEdenOffer, fetchMagicEdenTokens, fetchMagicEdenTraitFloor } from "./marketplace/magiceden";
import { CollectionItemsQueryVariables, createListing, fetchOpenSeaCollectionItems, fetchOpenseaTraits, getCollectionDetails, getOpenseaFloor } from "./functions";
import mongoose from 'mongoose';
import Task, { ITask, SelectedTraits } from "./models/task.model";
import { Queue, Worker, Job, JobType, UnrecoverableError, JobsOptions, QueueEvents, QueueOptions } from "bullmq";
import Wallet from "./models/wallet.model";
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
import User from "./models/user.model";
import axios from "axios";
import { decryptPrivateKey, setPassword, getPassword, decryptPassword } from "./utils/crypto";
import { fetchWalletNFTs } from "./utils/nft";
import { logger, DatabaseOperation, QueueOperation } from "./utils/logger";
import WebSocketManager from "./utils/websocket-manager";
import Listing from "./models/listing.model";
import { initializeListingScheduler, shutdownListingScheduler, scheduleListing, cancelListingJob } from "./functions/listingScheduler";
import { initializeExpirationMonitor, stopExpirationMonitor } from "./functions/relistExpired";
import { handleListingPriceUpdate } from "./functions/priceMonitor";
const prisma = new PrismaClient()

const MARKETPLACE_WS_URL = "wss://nfttools-ws-proxy.nfttools.io";

export const RATE_LIMIT = Number(process.env.RATE_LIMIT)
export const API_KEY = process.env.API_KEY;
const bidStats: BidCounts = {};
const currentTokenIds: BottomListings = {};
const currentTraits: CurrentTraits = {}
const taskStatus: TaskStatus = {};
const bestOffers: BidCounts = {};
const floorPrices: BidCounts = {};
const balances: BidCounts = {}
export const warningBids: WarningBids = {};
export const warningMessage: WarningMessage = {};
export const errorStats: BidCounts = {};
export const skipStats: BidCounts = {};
export const blurRateLimitInfo: { [walletAddress: string]: { remaining: number; reset: number; limit: number } } = {};
const QUEUE_NAME = 'BIDDING_BOT';


export const WORKER_COUNT = 1

// BullMQ configuration will use the connection from redis.ts

let traitsToRemove: { [key: string]: string[] } = {}
let removedTokens: { [key: string]: { opensea: string[]; magiceden: string[] } } = {}

// Use Maps instead of Sets to track timestamps for cleanup
const seenOrderHashes = new Map<string, number>(); // Map<orderHash, timestamp>
const seenBids = new Map<string, number>(); // Map<bidHash, timestamp>

// Configuration for cleanup
const SEEN_ITEMS_TTL = 30 * 60 * 1000; // 30 minutes TTL
const SEEN_ITEMS_MAX_SIZE = 10000; // Maximum items to keep
const CLEANUP_INTERVAL = 5 * 60 * 1000; // Run cleanup every 5 minutes

// Helper functions for seen items management
function addSeenItem(map: Map<string, number>, key: string): void {
  const now = Date.now();
  map.set(key, now);

  // If map exceeds max size, remove oldest entries
  if (map.size > SEEN_ITEMS_MAX_SIZE) {
    const entries = Array.from(map.entries());
    entries.sort((a, b) => a[1] - b[1]); // Sort by timestamp (oldest first)
    const toRemove = entries.slice(0, entries.length - SEEN_ITEMS_MAX_SIZE);
    toRemove.forEach(([key]) => map.delete(key));
  }
}

// Note: Manual .delete() calls after queuing counterbids are intentional
// They allow immediate re-processing of the same order/bid if needed
// The TTL system acts as a safety net for any items not manually deleted

function hasSeenItem(map: Map<string, number>, key: string): boolean {
  const timestamp = map.get(key);
  if (!timestamp) return false;

  // Check if item has expired
  if (Date.now() - timestamp > SEEN_ITEMS_TTL) {
    map.delete(key);
    return false;
  }

  return true;
}

function cleanupSeenItems(map: Map<string, number>): void {
  const now = Date.now();
  const expiredKeys: string[] = [];
  const beforeSize = map.size;

  map.forEach((timestamp, key) => {
    if (now - timestamp > SEEN_ITEMS_TTL) {
      expiredKeys.push(key);
    }
  });

  expiredKeys.forEach(key => map.delete(key));

  if (expiredKeys.length > 0) {
    const mapName = map === seenOrderHashes ? 'seenOrderHashes' : 'seenBids';
    console.log(`[Cleanup] ${mapName}: removed ${expiredKeys.length} expired items, ${beforeSize} -> ${map.size} items`);
  }
}

// Set up periodic cleanup
let cleanupIntervalId: NodeJS.Timeout | null = null;
let expirationMonitorInterval: NodeJS.Timer | null = null;

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
  OPENSEA: 5,
  MAGICEDEN: 5,
  BLUR: 5
};

const TRAIT_BID_PRIORITY = {
  OPENSEA: 5,
  MAGICEDEN: 5,
  BLUR: 5
};

const COLLECTION_BID_PRIORITY = {
  OPENSEA: 5,
  MAGICEDEN: 5,
  BLUR: 5
};

type JobState = 'completed' | 'failed' | 'delayed' | 'active' | 'waiting' | 'waiting-children' | 'unknown' | 'prioritized' | 'paused';

const lockManager = new DistributedLockManager({
  lockPrefix: '{marketplace}:',
  defaultTTLSeconds: 60
});

// Create a getter for redis to ensure it's always current

// For internal use
let redis: any = null;

// Export redis as a getter property
export { lockManager };
export { redis };
export { queue };

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
let workers: Worker;

const taskTimeouts = new Map<string, NodeJS.Timeout>();

// Add a Map to track task locks
const taskLocks = new Map<string, TaskLock>();


function unlockTask(taskId: string): void {
  const currentLock = taskLocks.get(taskId);
  if (currentLock) {
    currentLock.locked = false;
  }
}

// Queue and QueueEvents will be initialized in startServer
let queue: Queue;
let queueEvents: QueueEvents;

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
export const MAGICEDEN_COLLECTION_BID_COUNTERBID = "MAGICEDEN_COLLECTION_BID_COUNTERBID"
export const MAGICEDEN_TOKEN_BID_COUNTERBID = "MAGICEDEN_TOKEN_BID_COUNTERBID"
export const MAGICEDEN_TRAIT_BID_COUNTERBID = "MAGICEDEN_TRAIT_BID_COUNTERBID"
const CANCEL_OPENSEA_BID = "CANCEL_OPENSEA_BID"
const CANCEL_MAGICEDEN_BID = "CANCEL_MAGICEDEN_BID"
const CANCEL_BLUR_BID = "CANCEL_BLUR_BID"
const MAGICEDEN_MARKETPLACE = "0x9A1D00bEd7CD04BCDA516d721A596eb22Aac6834"
const MAX_RETRIES: number = 5;
const ALCHEMY_API_KEY = process.env.ALCHEMY_API_KEY as string;
const OPENSEA_PROTOCOL_ADDRESS = "0x0000000000000068F116a894984e2DB1123eB395"
const HEALTH_CHECK_INTERVAL = 5000;
let enabledMarketplaces: string[] = []
const clientId = randomUUID()

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

    const totalMemory = os.totalmem();
    const memoryUsagePercent = (used.heapUsed / totalMemory) * 100;

    if (memoryUsagePercent > 75) {
      console.log(RED + `⚠️  HIGH MEMORY USAGE (${memoryUsagePercent.toFixed(2)}%) - INITIATING CLEANUP... ⚠️` + RESET);
      await queue.pause();
      cleanupMemory();

      setTimeout(async () => {
        await queue.resume();
      }, 5000);
    }

    if (counts.active > DRAIN_THRESHOLD) {
      await queue.pause();
      setTimeout(async () => {
        await queue.resume();
      }, 2500);
    } else {
      await queue.resume();
    }

    const totalJobs = Object.values(counts).reduce((a, b) => a + b, 0);

    console.log('\n📊 ═══════ Health Monitor Stats ═══════ 📊');

    console.log(BLUE + '\n💾 Memory Usage:' + RESET);
    Object.entries(memoryStats).forEach(([key, value]) => {
      console.log(`${key}:${value}`);
    });

    console.log(BLUE + '\n📋 Queue Status:' + RESET);
    Object.entries(counts).forEach(([key, value]) => {
      const color = value > RATE_LIMIT * 2 ? RED : value > RATE_LIMIT ? YELLOW : GREEN;
      console.log(`${key}: ${color}${value}${RESET}`);
    });
    console.log(`Total Jobs: ${totalJobs}`);

    updateAllBidRates();

  } catch (error) {
    console.error(RED + '❌ Error monitoring health:' + RESET, error);
    logger.logGeneralError("Health monitoring failed", {
      error: error as Error,
      uptime: process.uptime(),
      memoryUsage: process.memoryUsage()
    });
  }
}


setInterval(monitorHealth, HEALTH_CHECK_INTERVAL);

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

// Modify fetchCurrentTasks to populate activeTasks
async function fetchCurrentTasks() {
  try {
    console.log(BLUE + '\n=== Fetching Current Tasks ===' + RESET);
    const tasks = await Task.find({})
    const wallets = (await Wallet.find({})).map((wallet) => wallet.address)

    walletsArr.push(...wallets);
    const formattedTasks = tasks.map((task) => ({
      ...task.toObject(),
      _id: task._id.toString(),
      user: task.user.toString()
    })) as ITask[];

    // Clear and populate activeTasks Map
    activeTasks.clear();

    formattedTasks.forEach(task => {
      activeTasks.set(task._id, task);
    });

    console.log(`Found ${formattedTasks.length} tasks`);
    currentTasks.push(...formattedTasks);

    console.log('Creating initial jobs...');
    const jobs = formattedTasks
      .filter((task) => task.running)
      .flatMap(task =>
        task.running && task.selectedMarketplaces.map((m: string) => m.toLowerCase()).includes("opensea")
          ? [{ name: OPENSEA_SCHEDULE, data: task }]
          : []
      ).concat(
        formattedTasks.flatMap(task =>
          task.running && task.selectedMarketplaces.map((m: string) => m.toLowerCase()).includes("blur")
            ? [{ name: BLUR_SCHEDULE, data: task }]
            : []
        )
      ).concat(
        formattedTasks.flatMap(task =>
          task.running && task.selectedMarketplaces.map((m: string) => m.toLowerCase()).includes("magiceden")
            ? [{ name: MAGICEDEN_SCHEDULE, data: task }]
            : []
        )
      );
    console.log(`Created ${jobs.length} initial jobs`);
    console.log(GREEN + '=== Task Initialization Complete ===\n' + RESET);
  } catch (error) {
    console.error(RED + 'Error fetching current tasks:', error, RESET);
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
}

const parentJobs = new Map<string, BulkJobParent>();

// Queue management for password-based security
async function pauseQueueAndWorkers() {
  try {
    console.log(YELLOW + 'No password set - pausing queue and workers...' + RESET);
    await queue.pause();
    if (workers) {
      await workers.pause();
    }
    console.log(YELLOW + 'Queue and workers paused. Waiting for password...' + RESET);
  } catch (error) {
    await logger.logQueueError('Error pausing queue and workers', {
      queueOperation: QueueOperation.PAUSE_QUEUE,
      error: error as Error,
      errorMessage: error instanceof Error ? error.message : String(error)
    });
    console.error(RED + 'Error pausing queue and workers:' + RESET, error);
  }
}

async function resumeQueueAndWorkers() {
  try {
    console.log(GREEN + 'Password set - resuming queue and workers...' + RESET);
    await queue.resume();
    if (workers) {
      await workers.resume();
    }
    console.log(GREEN + 'Queue and workers resumed.' + RESET);
  } catch (error) {
    await logger.logQueueError('Error resuming queue and workers', {
      queueOperation: QueueOperation.RESUME_QUEUE,
      error: error as Error,
      errorMessage: error instanceof Error ? error.message : String(error)
    });
    console.error(RED + 'Error resuming queue and workers:' + RESET, error);
  }
}

async function startServer() {
  try {
    // Development mode notification
    if (process.env.NODE_ENV === 'development') {
      console.log(GREEN + '🚀 Running in DEVELOPMENT mode' + RESET);
      if (process.env.PASSWORD) {
        console.log(GREEN + '✅ Development password will be auto-set from environment' + RESET);
      }
    }

    // Wait for Redis to be ready
    console.log('Waiting for Redis connection...');
    while (!redisClient.isReady()) {
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    console.log('Redis is ready');

    // Initialize redis for internal use
    redis = redisClient.getClient();

    // Initialize queue and queueEvents with Redis connection
    const queueConnection = await createBullMQConnection();

    const QUEUE_OPTIONS: QueueOptions = {
      prefix: '{bull}',
      connection: queueConnection,
      defaultJobOptions: {
        removeOnComplete: true,
        removeOnFail: true,
        attempts: 0
      }
    };

    queue = new Queue(QUEUE_NAME, QUEUE_OPTIONS);
    queueEvents = new QueueEvents(QUEUE_NAME, QUEUE_OPTIONS);

    // Set up queue event handlers now that queueEvents is initialized
    setupQueueEventHandlers();

    // Initialize the worker with Redis connection
    workers = new Worker(
      QUEUE_NAME,
      async (job) => {
        try {
          await processJob(job);
          return
        } catch (error) {
          if (job.token) {
            await job.moveToFailed(new Error("Job failed"), job.token);
          } else {
            await job.remove()
          }
          throw new UnrecoverableError('Unrecoverable');
        }
      },
      {
        connection: await createBullMQConnection(),
        prefix: '{bull}',
        concurrency: Number(RATE_LIMIT),
      }
    );

    const mongoUri = process.env.MONGODB_URI;
    if (!mongoUri) {
      throw new Error('MONGODB_URI environment variable is not set');
    }

    console.log('Connecting to MongoDB...');
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

    try {
      const lastRuntime = redis ? await redis.get(LAST_RUNTIME_KEY) : null;
      console.log('Last runtime:', lastRuntime ? new Date(parseInt(lastRuntime)).toLocaleString() : 'Never');

      const currentTime = Date.now();

      if (lastRuntime) {
        const downtime = currentTime - parseInt(lastRuntime);
        if (downtime > DOWNTIME_THRESHOLD) {
          console.log(YELLOW + `Server was down for ${Math.round(downtime / 60000)} minutes. Clearing queue...` + RESET);
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
      if (redis) {
        await redis.set(LAST_RUNTIME_KEY, currentTime.toString());
      }
    } catch (redisError) {
      await logger.logDatabaseError('Redis operation failed during startup', {
        databaseOperation: DatabaseOperation.CONNECTION,
        collection_name: 'redis',
        error: redisError as Error,
        errorMessage: redisError instanceof Error ? redisError.message : String(redisError)
      });
      console.error('Redis operation failed:', redisError);
    }

    setInterval(async () => {
      if (redis) {
        await redis.set(LAST_RUNTIME_KEY, Date.now().toString());
      }
    }, 60000);

    server.listen(port, () => {
      console.log(`Magic happening on http://localhost:${port}`);
      console.log(`WebSocket server is running on ws://localhost:${port}`);
    });

    // Prevent server crashes from unhandled WebSocket errors
    process.on('unhandledRejection', (reason, promise) => {
      console.error('Unhandled Rejection at:', promise, 'reason:', reason);
      logger.logGeneralError('Unhandled Promise Rejection', {
        operation: 'process_unhandled_rejection',
        error: reason instanceof Error ? reason : new Error(String(reason))
      });
      // Don't exit the process - let the server continue running
    });

    process.on('uncaughtException', (error) => {
      console.error('Uncaught Exception:', error);
      logger.logGeneralError('Uncaught Exception', {
        operation: 'process_uncaught_exception',
        error: error
      });
      // Don't exit the process - let the server continue running
    });

    // Initialize WebSocket connection monitoring
    await loadConnectionMetrics();
    startConnectionMonitoring();
    console.log('🔗 WebSocket connection monitoring initialized');

    // Initialize listing scheduler
    await initializeListingScheduler();
    console.log('📋 Listing scheduler initialized');

    // Initialize expiration monitor
    expirationMonitorInterval = initializeExpirationMonitor();
    console.log('⏰ Expiration monitor initialized');

    // Start periodic cleanup of seen items
    cleanupIntervalId = setInterval(() => {
      cleanupSeenItems(seenOrderHashes);
      cleanupSeenItems(seenBids);
    }, CLEANUP_INTERVAL);

    console.log(`Started periodic cleanup for seen items (every ${CLEANUP_INTERVAL / 1000 / 60} minutes)`);

    // Note: Listing scheduler is already initialized above at line 749
    // The expiration monitor is also initialized at line 753

    await fetchCurrentTasks()

    // Check if password is set on startup
    if (!getPassword()) {
      // In development, set password from env if available
      if (process.env.NODE_ENV === 'development' && process.env.PASSWORD) {
        console.log(GREEN + 'Development mode: Setting password from environment' + RESET);
        setPassword(process.env.PASSWORD);
      } else {
        await pauseQueueAndWorkers();
      }
    }

  } catch (error) {
    console.error(RED + 'Failed to start server:' + RESET, error);
    process.exit(1);
  }
}

// Cleanup handler for graceful shutdown
process.on('SIGINT', async () => {
  console.log('\nReceived SIGINT, cleaning up...');
  if (cleanupIntervalId) {
    clearInterval(cleanupIntervalId);
    console.log('Stopped periodic cleanup');
  }

  // Shutdown listing scheduler
  await shutdownListingScheduler();
  console.log('Listing scheduler shut down');

  // Stop expiration monitor
  if (expirationMonitorInterval) {
    stopExpirationMonitor(expirationMonitorInterval);
    console.log('Expiration monitor stopped');
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
  if (cleanupIntervalId) {
    clearInterval(cleanupIntervalId);
    console.log('Stopped periodic cleanup');
  }

  // Shutdown listing scheduler
  await shutdownListingScheduler();
  console.log('Listing scheduler shut down');

  // Stop expiration monitor
  if (expirationMonitorInterval) {
    stopExpirationMonitor(expirationMonitorInterval);
    console.log('Expiration monitor stopped');
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

startServer()
  .then(() => {
    runScheduledLoop().catch(async error => {
      await logger.logGeneralError('Failed to run scheduled loop', {
        error: error as Error,
        errorMessage: error instanceof Error ? error.message : String(error)
      });
      console.error('Failed to run scheduled loop:', error);
    })

    connectWebSocket().catch(async error => {
      await logger.logWebSocketError('Failed to connect to WebSocket', {
        error: error as Error,
        errorMessage: error instanceof Error ? error.message : String(error),
        operation: 'initial_connection'
      });
      console.error('Failed to connect to WebSocket:', error);
    })
  })
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

// Queue event handlers will be set up after queue initialization
function setupQueueEventHandlers() {
  queueEvents.on('completed', async ({ jobId }) => {
    if (jobId.includes('CANCEL_OPENSEA_BID') || jobId.includes('CANCEL_MAGICEDEN_BID') || jobId.includes('CANCEL_BLUR_BID')) {
      const taskId = jobId.split('-')[1];
      if (!taskStatus[taskId]) {
        taskStatus[taskId] = {
          status: 'CANCELLING',
          cancellationPercentage: 0,
          runningPercentage: 0,
          waitingTime: 0,
          nextLoopTime: new Date(),
          processingPercentage: 0
        }
      }
      taskStatus[taskId].status = 'CANCELLING'

      const task = activeTasks.get(taskId);
      if (!task) return;

      const marketplace = jobId.includes('CANCEL_OPENSEA_BID') ? 'OPENSEA' : jobId.includes('CANCEL_MAGICEDEN_BID') ? 'MAGICEDEN' : 'BLUR';
      const parentJobId = `${taskId}-CANCEL_${marketplace}_BID`;
      const parentJob = parentJobs.get(parentJobId);

      if (!parentJob) return;

      parentJob.completedJobs++;

      // Calculate marketplace-specific cancellation percentage

      // If task is not running, calculate combined cancellation percentage with weighting
      if (!task?.running) {
        // Get all parent jobs for this task (both OpenSea and MagicEden)
        const openSeaParentJobId = `${taskId}-CANCEL_OPENSEA_BID`;
        const magicEdenParentJobId = `${taskId}-CANCEL_MAGICEDEN_BID`;
        const blurParentJobId = `${taskId}-CANCEL_BLUR_BID`;

        const openSeaParentJob = parentJobs.get(openSeaParentJobId);
        const magicEdenParentJob = parentJobs.get(magicEdenParentJobId);
        const blurParentJob = parentJobs.get(blurParentJobId);

        // Apply a weight of 1000 to MagicEden jobs
        const magicEdenWeight = 500;

        // Calculate weighted totals across both marketplaces
        const openSeaTotalJobs = openSeaParentJob?.totalJobs || 0;
        const openSeaCompletedJobs = openSeaParentJob?.completedJobs || 0;
        const openSeaFailedJobs = openSeaParentJob?.failedJobs || 0;

        const blurTotalJobs = blurParentJob?.totalJobs || 0;
        const blurCompletedJobs = blurParentJob?.completedJobs || 0;
        const blurFailedJobs = blurParentJob?.failedJobs || 0;

        const magicEdenFailedJobs = (magicEdenParentJob?.failedJobs || 0) * magicEdenWeight;
        const magicEdenTotalJobs = (magicEdenParentJob?.totalJobs || 0) * magicEdenWeight;
        const magicEdenCompletedJobs = (magicEdenParentJob?.totalJobs || 0) < 1 ? (magicEdenParentJob?.totalJobs || 0) * magicEdenWeight : (magicEdenParentJob?.completedJobs || 0) * magicEdenWeight;

        const totalJobs = openSeaTotalJobs + magicEdenTotalJobs + blurTotalJobs;
        const completedJobs = openSeaCompletedJobs + magicEdenCompletedJobs + blurCompletedJobs;
        const failedJobs = openSeaFailedJobs + magicEdenFailedJobs + blurFailedJobs;

        const combinedCancellationPercentage = totalJobs > 0 ?
          ((completedJobs + failedJobs) / totalJobs) * 100 : 0;

        taskStatus[taskId].cancellationPercentage = Math.min(combinedCancellationPercentage, 100);

        if (totalJobs < completedJobs + failedJobs || combinedCancellationPercentage > 99) {
          const orders = await prisma.order.findMany({
            where: {
              taskId: taskId,
            }
          });

          if (orders.length > 0) {
            const wallet = await Wallet.findById(task.walletId);
            const private_key = await decryptPrivateKey(wallet?.encryptedPrivateKey as string) as string;
            await cancelAllRelatedBids(task, private_key);
            return
          }
          taskStatus[taskId].status = 'STOPPED'
          delete taskStatus[taskId].nextLoopTime
          taskStatus[taskId].waitingTime = 0
        }
      } else {
        let marketplaceCancellationPercentage;
        if (marketplace.toLowerCase() === 'magiceden') {
          if (parentJob.totalJobs < 1) {
            marketplaceCancellationPercentage = 100;
          } else {
            const weightedCompletedJobs = parentJob.completedJobs * 500;
            const weightedTotalJobs = parentJob.totalJobs * 500;
            const weightedFailedJobs = parentJob.failedJobs * 500
            marketplaceCancellationPercentage = weightedTotalJobs > 0 ?
              ((weightedCompletedJobs + weightedFailedJobs) / weightedTotalJobs) * 100 : 0;
          }
        } else {
          marketplaceCancellationPercentage = parentJob.totalJobs > 0 ?
            ((parentJob.completedJobs + parentJob.failedJobs) / parentJob.totalJobs) * 100 : 0;
        }

        // If task is running, just update with the marketplace-specific percentage
        taskStatus[taskId].cancellationPercentage = marketplaceCancellationPercentage;
        if (parentJob.totalJobs < parentJob.completedJobs + parentJob.failedJobs || marketplaceCancellationPercentage > 99) {
          const orders = await prisma.order.findMany({
            where: {
              taskId: taskId,
              marketplace: marketplace.toLowerCase()
            }
          });

          if (orders.length > 0) {
            const wallet = await Wallet.findById(task.walletId);
            const private_key = await decryptPrivateKey(wallet?.encryptedPrivateKey as string) as string;
            await cancelAllRelatedBids(task, private_key, marketplace.toLowerCase());
            return
          }
          taskStatus[taskId].cancellationPercentage = 100
          if (taskStatus[taskId].nextLoopTime && taskStatus[taskId].waitingTime > 0) {
            taskStatus[taskId].status = 'SCHEDULED'
          }
        }
      }
    }

    if (jobId.split('-').length < 3) return;

    const taskId = jobId.split('-')[0];
    const task = activeTasks.get(taskId);

    if (!task || !task.running) return;

    const marketplace = jobId.split('-')[1].toLowerCase();
    const parentId = `${taskId}-${marketplace}`.toLowerCase();

    const { currencySymbol } = await calculateBidPrice(task, 0, "magiceden")


    const parentJob = parentJobs.get(parentId);
    if (!parentJob) return;

    taskStatus[taskId].status = 'RUNNING'
    parentJob.completedJobs++;

    // Calculate total jobs across all selected and enabled marketplaces for this task
    let totalJobs = 0;
    let completedJobs = 0;
    let failedJobs = 0;
    const selectedMarketplaces = task.selectedMarketplaces.map(m => m.toLowerCase());
    const enabledAndSelectedMarketplaces = selectedMarketplaces.filter(m =>
      enabledMarketplaces.map(em => em.toLowerCase()).includes(m)
    );

    for (const marketplace of enabledAndSelectedMarketplaces) {
      const marketplaceParentId = `${taskId}-${marketplace}`.toLowerCase();
      const marketplaceParentJob = parentJobs.get(marketplaceParentId);
      if (marketplaceParentJob) {
        totalJobs += marketplaceParentJob.totalJobs;
        completedJobs += marketplaceParentJob.completedJobs;
        failedJobs += marketplaceParentJob.failedJobs;
      }
    }
    const runningPercentage = ((completedJobs + failedJobs) / totalJobs) * 100
    taskStatus[taskId].runningPercentage = Math.min(runningPercentage, 100) // 100 is the max value since it represents 100%


    if (parentJob.completedJobs + parentJob.failedJobs === parentJob.totalJobs) {
      if (!completedTasks[taskId]) {
        completedTasks[taskId] = {
          opensea: false,
          magiceden: false,
          blur: false
        }
      }
      completedTasks[taskId][marketplace as 'opensea' | 'magiceden' | 'blur'] = true
      console.log(GREEN + `Task ${taskId} completed for ${marketplace}` + RESET);
      const availableMarketplaces = enabledAndSelectedMarketplaces.map(m => m.toLowerCase())

      // Check if all available marketplaces have completed
      if (availableMarketplaces && availableMarketplaces.length > 0) {
        const allMarketsComplete = availableMarketplaces.every(m =>
          completedTasks[taskId]?.[m as 'opensea' | 'magiceden' | 'blur']
        );

        if (allMarketsComplete && runningPercentage > 99) {
          console.log(GREEN + `Task ${taskId} completed for all marketplaces` + RESET);
          const wallet = await Wallet.findById(task.walletId)
          const WALLET_PRIVATE_KEY = await decryptPrivateKey(wallet?.encryptedPrivateKey as string) as string;
          if (traitsToRemove[taskId]?.length > 0) {
            await cancelRemovedTraitBids(traitsToRemove[taskId], task._id, WALLET_PRIVATE_KEY, currencySymbol)
          }

          if (removedTokens[taskId]?.opensea?.length > 0 || removedTokens[taskId]?.magiceden?.length > 0) {
            await cancelRemovedTokenBids(removedTokens[taskId], task._id, WALLET_PRIVATE_KEY, currencySymbol)
          }

          taskStatus[taskId].status = 'COMPLETED';
          delete traitsToRemove[taskId];
          delete removedTokens[taskId];
          await new Promise(resolve => setTimeout(resolve, 1000));

          if (task && task.running) {
            const loopInterval = getExpiry(task.loopInterval) + 1;
            const nextLoopTime = new Date(Date.now() + loopInterval * 1000);
            const secondsUntilNextLoop = Math.floor((nextLoopTime.getTime() - Date.now()) / 1000);

            console.log(GREEN + `Scheduling task ${taskId} to run again in ${loopInterval} seconds` + RESET);
            taskStatus[taskId].status = 'SCHEDULED';
            taskStatus[taskId].waitingTime = secondsUntilNextLoop;
            taskStatus[taskId].nextLoopTime = nextLoopTime;

            const countdownTimer = setInterval(() => {
              const remainingSeconds = taskStatus[taskId].nextLoopTime ? Math.max(0, Math.floor((taskStatus[taskId].nextLoopTime.getTime() - Date.now()) / 1000)) : 0
              taskStatus[taskId].waitingTime = remainingSeconds;

              if (remainingSeconds <= 0) {
                clearInterval(countdownTimer);
              }
            }, 1000);

            // Clear any existing timeout for this task
            const existingTimeout = taskTimeouts.get(taskId);
            if (existingTimeout) {
              clearTimeout(existingTimeout);
            }

            const timeoutId = setTimeout(async () => {
              // When we restart, update the status
              const currentTask = activeTasks.get(taskId);
              if (currentTask?.running) {
                await startTask(currentTask, true);
              } else if (currentTask) {
                await stopTask(currentTask, false);
              }
            }, loopInterval * 1000);

            taskTimeouts.set(taskId, timeoutId);
          }
          return;
        }
      }
    }

  });

  queueEvents.on('failed', async ({ jobId }) => {
    if (jobId.includes('CANCEL_OPENSEA_BID') || jobId.includes('CANCEL_MAGICEDEN_BID') || jobId.includes('CANCEL_BLUR_BID')) {
      const taskId = jobId.split('-')[1];
      if (!taskStatus[taskId]) {
        taskStatus[taskId] = {
          status: 'CANCELLING',
          cancellationPercentage: 0,
          runningPercentage: 0,
          waitingTime: 0,
          nextLoopTime: new Date(),
          processingPercentage: 0
        }
      }
      taskStatus[taskId].status = 'CANCELLING'

      const task = activeTasks.get(taskId);
      if (!task) return;

      const marketplace = jobId.includes('CANCEL_OPENSEA_BID') ? 'OPENSEA' : jobId.includes('CANCEL_MAGICEDEN_BID') ? 'MAGICEDEN' : 'BLUR';
      const parentJobId = `${taskId}-CANCEL_${marketplace}_BID`;
      const parentJob = parentJobs.get(parentJobId);

      if (!parentJob) return;

      parentJob.failedJobs++;

      // Calculate marketplace-specific cancellation percentage

      // If task is not running, calculate combined cancellation percentage with weighting
      if (!task?.running) {
        // Get all parent jobs for this task (both OpenSea and MagicEden)
        const openSeaParentJobId = `${taskId}-CANCEL_OPENSEA_BID`;
        const magicEdenParentJobId = `${taskId}-CANCEL_MAGICEDEN_BID`;
        const blurParentJobId = `${taskId}-CANCEL_BLUR_BID`;

        const openSeaParentJob = parentJobs.get(openSeaParentJobId);
        const magicEdenParentJob = parentJobs.get(magicEdenParentJobId);
        const blurParentJob = parentJobs.get(blurParentJobId);

        // Apply a weight of 1000 to MagicEden jobs
        const magicEdenWeight = 500;

        // Calculate weighted totals across both marketplaces
        const openSeaTotalJobs = openSeaParentJob?.totalJobs || 0;
        const openSeaCompletedJobs = openSeaParentJob?.completedJobs || 0;
        const openSeaFailedJobs = openSeaParentJob?.failedJobs || 0;

        const blurTotalJobs = blurParentJob?.totalJobs || 0;
        const blurCompletedJobs = blurParentJob?.completedJobs || 0;
        const blurFailedJobs = blurParentJob?.failedJobs || 0;

        const magicEdenFailedJobs = (magicEdenParentJob?.failedJobs || 0) * magicEdenWeight;
        const magicEdenTotalJobs = (magicEdenParentJob?.totalJobs || 0) * magicEdenWeight;
        const magicEdenCompletedJobs = (magicEdenParentJob?.totalJobs || 0) < 1 ? (magicEdenParentJob?.totalJobs || 0) * magicEdenWeight : (magicEdenParentJob?.completedJobs || 0) * magicEdenWeight;

        const totalJobs = openSeaTotalJobs + magicEdenTotalJobs + blurTotalJobs;
        const completedJobs = openSeaCompletedJobs + magicEdenCompletedJobs + blurCompletedJobs;
        const failedJobs = openSeaFailedJobs + magicEdenFailedJobs + blurFailedJobs;

        const combinedCancellationPercentage = totalJobs > 0 ?
          ((completedJobs + failedJobs) / totalJobs) * 100 : 0;

        taskStatus[taskId].cancellationPercentage = Math.min(combinedCancellationPercentage, 100);

        if (totalJobs < completedJobs + failedJobs || combinedCancellationPercentage > 99) {
          const orders = await prisma.order.findMany({
            where: {
              taskId: taskId,
            }
          });

          if (orders.length > 0) {
            const wallet = await Wallet.findById(task.walletId);
            const private_key = await decryptPrivateKey(wallet?.encryptedPrivateKey as string) as string;
            await cancelAllRelatedBids(task, private_key);
            return
          }
          taskStatus[taskId].status = 'STOPPED'
          delete taskStatus[taskId].nextLoopTime
          taskStatus[taskId].waitingTime = 0
        }
      } else {
        let marketplaceCancellationPercentage;
        if (marketplace.toLowerCase() === 'magiceden') {
          if (parentJob.totalJobs < 1) {
            marketplaceCancellationPercentage = 100;
          } else {
            const weightedCompletedJobs = parentJob.completedJobs * 500;
            const weightedTotalJobs = parentJob.totalJobs * 500;
            const weightedFailedJobs = parentJob.failedJobs * 500
            marketplaceCancellationPercentage = weightedTotalJobs > 0 ?
              ((weightedCompletedJobs + weightedFailedJobs) / weightedTotalJobs) * 100 : 0;
          }
        } else {
          marketplaceCancellationPercentage = parentJob.totalJobs > 0 ?
            ((parentJob.completedJobs + parentJob.failedJobs) / parentJob.totalJobs) * 100 : 0;
        }

        // If task is running, just update with the marketplace-specific percentage
        taskStatus[taskId].cancellationPercentage = marketplaceCancellationPercentage;
        if (parentJob.totalJobs < parentJob.completedJobs + parentJob.failedJobs || marketplaceCancellationPercentage > 99) {
          const orders = await prisma.order.findMany({
            where: {
              taskId: taskId,
              marketplace: marketplace.toLowerCase()
            }
          });

          if (orders.length > 0) {
            const wallet = await Wallet.findById(task.walletId);
            const private_key = await decryptPrivateKey(wallet?.encryptedPrivateKey as string) as string;
            await cancelAllRelatedBids(task, private_key, marketplace.toLowerCase());
            return
          }
          taskStatus[taskId].cancellationPercentage = 100
          if (taskStatus[taskId].nextLoopTime && taskStatus[taskId].waitingTime > 0) {
            taskStatus[taskId].status = 'SCHEDULED'
          }
        }
      }
    }

    if (jobId.split('-').length < 3) return;

    const taskId = jobId.split('-')[0];
    const task = activeTasks.get(taskId);

    if (!task || !task.running) return;

    const marketplace = jobId.split('-')[1].toLowerCase();
    const parentId = `${taskId}-${marketplace}`.toLowerCase();

    const { currencySymbol } = await calculateBidPrice(task, 0, "magiceden")

    const parentJob = parentJobs.get(parentId);
    if (!parentJob) return;

    taskStatus[taskId].status = 'RUNNING'
    parentJob.failedJobs++;

    // Calculate total jobs across all selected and enabled marketplaces for this task
    let totalJobs = 0;
    let completedJobs = 0;
    let failedJobs = 0;
    const selectedMarketplaces = task.selectedMarketplaces.map(m => m.toLowerCase());
    const enabledAndSelectedMarketplaces = selectedMarketplaces.filter(m =>
      enabledMarketplaces.map(em => em.toLowerCase()).includes(m)
    );

    for (const marketplace of enabledAndSelectedMarketplaces) {
      const marketplaceParentId = `${taskId}-${marketplace}`.toLowerCase();
      const marketplaceParentJob = parentJobs.get(marketplaceParentId);
      if (marketplaceParentJob) {
        totalJobs += marketplaceParentJob.totalJobs;
        completedJobs += marketplaceParentJob.completedJobs;
        failedJobs += marketplaceParentJob.failedJobs;
      }
    }
    const runningPercentage = ((completedJobs + failedJobs) / totalJobs) * 100
    taskStatus[taskId].runningPercentage = Math.min(runningPercentage, 100) // 100 is the max value since it represents 100%

    if (parentJob.completedJobs + parentJob.failedJobs === parentJob.totalJobs) {
      if (!completedTasks[taskId]) {
        completedTasks[taskId] = {
          opensea: false,
          magiceden: false,
          blur: false
        }
      }
      completedTasks[taskId][marketplace as 'opensea' | 'magiceden' | 'blur'] = true
      console.log(GREEN + `Task ${taskId} completed for ${marketplace}` + RESET);
      const availableMarketplaces = enabledAndSelectedMarketplaces.map(m => m.toLowerCase())

      // Check if all available marketplaces have completed
      if (availableMarketplaces && availableMarketplaces.length > 0) {
        const allMarketsComplete = availableMarketplaces.every(m =>
          completedTasks[taskId]?.[m as 'opensea' | 'magiceden' | 'blur']
        );

        if (allMarketsComplete && runningPercentage > 99) {
          console.log(GREEN + `Task ${taskId} completed for all marketplaces` + RESET);

          const wallet = await Wallet.findById(task.walletId)
          const WALLET_PRIVATE_KEY = await decryptPrivateKey(wallet?.encryptedPrivateKey as string) as string;
          if (traitsToRemove[taskId]?.length > 0) {
            await cancelRemovedTraitBids(traitsToRemove[taskId], task._id, WALLET_PRIVATE_KEY, currencySymbol)
          }

          if (removedTokens[taskId]?.opensea?.length > 0 || removedTokens[taskId]?.magiceden?.length > 0) {
            await cancelRemovedTokenBids(removedTokens[taskId], task._id, WALLET_PRIVATE_KEY, currencySymbol)
          }

          taskStatus[taskId].status = 'COMPLETED';
          delete traitsToRemove[taskId];
          delete removedTokens[taskId];
          await new Promise(resolve => setTimeout(resolve, 1000));

          if (task && task.running) {
            const loopInterval = getExpiry(task.loopInterval) + 1;
            const nextLoopTime = new Date(Date.now() + loopInterval * 1000);
            const secondsUntilNextLoop = Math.floor((nextLoopTime.getTime() - Date.now()) / 1000);

            console.log(GREEN + `Scheduling task ${taskId} to run again in ${loopInterval} seconds` + RESET);
            taskStatus[taskId].status = 'SCHEDULED';
            taskStatus[taskId].waitingTime = secondsUntilNextLoop;
            taskStatus[taskId].nextLoopTime = nextLoopTime;

            const countdownTimer = setInterval(() => {
              const remainingSeconds = taskStatus[taskId].nextLoopTime ? Math.max(0, Math.floor((taskStatus[taskId].nextLoopTime.getTime() - Date.now()) / 1000)) : 0
              taskStatus[taskId].waitingTime = remainingSeconds;

              if (remainingSeconds <= 0) {
                clearInterval(countdownTimer);
              }
            }, 1000);

            // Clear any existing timeout for this task
            const existingTimeout = taskTimeouts.get(taskId);
            if (existingTimeout) {
              clearTimeout(existingTimeout);
            }

            const timeoutId = setTimeout(async () => {
              // When we restart, update the status
              const currentTask = activeTasks.get(taskId);
              if (currentTask?.running) {
                await startTask(currentTask, true);
              } else if (currentTask) {
                await stopTask(currentTask, false);
              }
            }, loopInterval * 1000);

            taskTimeouts.set(taskId, timeoutId);
          }
          return;
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
  const message = JSON.stringify({
    type: 'bidRatesUpdate',
    data: { bidRates, bidCounts: bidStats, skipCounts: skipStats, errorCounts: errorStats, floorPrices, bestOffers, warningBids, warningMessage, rps, taskStatus }
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
  console.log(GREEN + 'Session established' + RESET);

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
        // case 'start-listing':
        //   await handleStartListing(message.data, ws)
        //   break

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
      console.error(RED + 'Error handling WebSocket message:' + RESET, error);
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


async function processJob(job: Job) {
  const startTime = Date.now();
  try {
    // Original processing logic
    const result = await (async () => {
      switch (job.name) {
        case 'OPENSEA_COLLECTION_BID':
          return await processOpenseaCollectionBid(job.data);
        case 'MAGICEDEN_COLLECTION_BID':
          return await processMagicedenCollectionBid(job.data);
        case 'OPENSEA_COLLECTION_BID_COUNTERBID':
          return await openseaCollectionCounterBid(job.data);
        case 'MAGICEDEN_COLLECTION_BID_COUNTERBID':
          return await magicedenCollectionCounterBid(job.data);
        case 'OPENSEA_TOKEN_BID_COUNTERBID':
          return await openseaTokenCounterBid(job.data);
        case 'OPENSEA_TRAIT_BID_COUNTERBID':
          return await openseaTraitCounterBid(job.data);
        case 'MAGICEDEN_TOKEN_BID_COUNTERBID':
          return await magicedenTokenCounterBid(job.data);
        case 'MAGICEDEN_TRAIT_BID_COUNTERBID':
          return await magicedenTraitCounterBid(job.data);
        case 'OPENSEA_SCHEDULE':
          return await processOpenseaScheduledBid(job.data);
        case 'OPENSEA_TRAIT_BID':
          return await processOpenseaTraitBid(job.data);
        case 'OPENSEA_TOKEN_BID':
          return await processOpenseaTokenBid(job.data);
        case 'BLUR_SCHEDULE':
          return await processBlurScheduledBid(job.data);
        case 'MAGICEDEN_SCHEDULE':
          return await processMagicedenScheduledBid(job.data);
        case 'MAGICEDEN_TRAIT_BID':
          return await processMagicedenTraitBid(job.data);
        case 'MAGICEDEN_TOKEN_BID':
          return await processMagicedenTokenBid(job.data);
        case 'CANCEL_OPENSEA_BID':
          return await bulkCancelOpenseaBid(job.data);
        case 'CANCEL_MAGICEDEN_BID':
          return await bulkCancelMagicedenBid(job.data);
        case 'CANCEL_BLUR_BID':
          return await bulkCancelBlurBid(job.data);
        case BLUR_COLLECTION_BID:
          return await processBlurCollectionBid(job.data);
        case BLUR_TRAIT_BID:
          return await processBlurTraitBid(job.data);
        case BLUR_COLLECTION_BID_COUNTERBID:
          return await blurCollectionCounterBid(job.data);
        case BLUR_TRAIT_BID_COUNTERBID:
          return await blurTraitCounterBid(job.data);
        default:
          return null;
      }
    })();

    return result;
  } catch (error) {
    console.error(`Job ${job.name} failed after ${Date.now() - startTime}ms:`, error);
    throw error;
  }
}

interface TaskLock {
  locked: boolean;
  lockUntil: number;
  loopCount: number;
  lastReleaseTime?: number;
}

async function runScheduledLoop() {


  if (!API_KEY) {
    console.log(RED + 'API_KEY is not set' + RESET);
    await queue.pause()
    return
  }


  if (!RATE_LIMIT) {
    console.log(RED + 'RATE_LIMIT is not set' + RESET);
    await queue.pause()
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


    const jobs = tasks.flatMap(task => [
      ...(task.selectedMarketplaces.map(m => m.toLowerCase()).includes("opensea") ? [{ name: OPENSEA_SCHEDULE, data: task }] : []),
      ...(task.selectedMarketplaces.map(m => m.toLowerCase()).includes("blur") ? [{ name: BLUR_SCHEDULE, data: task }] : []),
      ...(task.selectedMarketplaces.map(m => m.toLowerCase()).includes("magiceden") ? [{ name: MAGICEDEN_SCHEDULE, data: task }] : []),
    ]);

    if (jobs.length > 0) {
      await queue.addBulk(jobs)
      console.log(`Successfully added ${jobs.length} jobs to the queue.`);
    }

    // Always ensure WebSocket connection for all tasks
    if (!wsManager?.isConnected()) {
      console.log(YELLOW + "Waiting for WebSocket connection..." + RESET);
      try {
        await connectWebSocket();
        // Give it a moment to establish connection
        await new Promise(resolve => setTimeout(resolve, 1000));
      } catch (error) {
        console.error('Failed to establish WebSocket connection for task subscription:', error);
      }
    }

    // Subscribe all running tasks to WebSocket
    const runningTasks = tasks.filter(task => task.running).map(task => ({ ...task, running: true }));

    if (runningTasks.length > 0) {
      console.log(GREEN + `Subscribing to ${runningTasks.length} collections` + RESET);
      subscribeToCollections(runningTasks);
    }

    console.log(GREEN + `=== Imported Tasks Processing Complete ===\n` + RESET);
  } catch (error) {
    console.error(RED + `Error processing imported tasks` + RESET, error);
  }
}


async function processNewTask(task: ITask) {
  try {
    console.log(BLUE + `\n === Processing New Task === ` + RESET);
    console.log(`Collection: ${task.contract.slug}`);
    console.log(`Task ID: ${task._id}`);

    // Add to activeTasks Map
    activeTasks.set(task._id, task);

    currentTasks.push(task);

    console.log(GREEN + `Added task to currentTasks(Total: ${currentTasks.length})` + RESET);
    const jobs = [
      ...(task.selectedMarketplaces.map(m => m.toLowerCase()).includes("opensea") ? [{ name: OPENSEA_SCHEDULE, data: task }] : []),
      ...(task.selectedMarketplaces.map(m => m.toLowerCase()).includes("blur") ? [{ name: BLUR_SCHEDULE, data: task }] : []),
      ...(task.selectedMarketplaces.map(m => m.toLowerCase()).includes("magiceden") ? [{ name: MAGICEDEN_SCHEDULE, data: task }] : []),
    ];

    if (jobs.length > 0) {
      await queue.addBulk(jobs)
      console.log(`Successfully added ${jobs.length} jobs to the queue.`);
    }

    // Always connect to WebSocket and subscribe all tasks
    try {
      if (!wsManager?.isConnected()) {
        console.log(YELLOW + "Waiting for WebSocket connection..." + RESET);
        try {
          await connectWebSocket();
          // Give it a moment to establish connection
          await new Promise(resolve => setTimeout(resolve, 1000));
        } catch (error) {
          console.error('Failed to establish WebSocket connection for task:', error);
        }
      }

      console.log("subscribing to collection: ", task.contract.slug);
      const newTask = { ...task, running: true };
      subscribeToCollections([newTask]);
    } catch (error) {
      console.error(RED + `Error subscribing to collection ${task.contract.slug}: ` + RESET, error);
    }

    console.log(GREEN + `=== New Task Processing Complete ===\n` + RESET);
  } catch (error) {
    console.error(RED + `Error processing new task: ${task.contract.slug} ` + RESET, error);
  }
}

async function processGlobalMarketplaces(incomingMarketplaces: string[]) {
  try {
    const tasks: ITask[] = Array.from(activeTasks.values());
    const newMarketplaces = incomingMarketplaces.map((m) => m.toLowerCase()).filter(m => !enabledMarketplaces.map((m) => m.toLowerCase()).includes(m));
    for (const marketplace of newMarketplaces) {
      await Promise.all((tasks.map((task) => {
        if (!task.running || !task.selectedMarketplaces.map((m) => m.toLowerCase()).includes(marketplace.toLowerCase())) return
        startTask(task, true, marketplace.toLowerCase())
      })))
    }
    enabledMarketplaces = incomingMarketplaces.map((m) => m.toLowerCase())
  } catch (error) {
    console.error(RED + `Error processing global marketplaces: ` + RESET, error);
  }
}

async function cancelRemovedTokenBids(removedTokens: { opensea: string[]; magiceden: string[] }, taskId: string, walletPrivateKey: string, currencySymbol: string) {
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
      await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey: walletPrivateKey, orderKeys, taskId: taskId });

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
}

async function cancelRemovedTraitBids(traitsToRemove: string[], taskId: string, walletPrivateKey: string, currencySymbol: string) {
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
    }

    if (openseaOrders.length > 0) {
      const cancelData = openseaOrders.map((order) => {
        return {
          name: CANCEL_OPENSEA_BID,
          data: { privateKey: walletPrivateKey, orderId: order.orderHash, taskId, orderKey: order.id }
        };
      });
      await queue.addBulk(cancelData);

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
      await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey: walletPrivateKey, orderKeys, taskId });

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

    const wallet = await Wallet.findById(task.walletId)
    const WALLET_PRIVATE_KEY = await decryptPrivateKey(wallet?.encryptedPrivateKey as string) as string;
    currentTasks.splice(existingTaskIndex, 1, task);
    activeTasks.set(task._id, task);

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
    const collectionDetails = await getCollectionDetails(task.contract.slug, task.contract.contractAddress);

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
    console.error(RED + `Error processing updated task: ${task.contract.slug} ` + RESET, error);
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
          await new Promise(resolve => setTimeout(resolve, delay * 1000));
        }

        // Check queue health
        const counts = await queue.getJobCounts();
        const prioritizedCount = counts.prioritized || 0;

        if (prioritizedCount > DRAIN_THRESHOLD) {
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
                    removeOnFail: true,
                    attempts: 0,
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
  parentJobs.set(bulkJobId, {
    parentId: bulkJobId,
    totalJobs: jobs.length,
    completedJobs: 0,
    failedJobs: 0,
    startTime: Date.now(),
    taskId,
    jobName: firstJob.name
  })

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

        while (true) {
          const counts = await queue.getJobCounts();
          const prioritizedCount = counts.prioritized || 0;

          if (prioritizedCount <= DRAIN_THRESHOLD) {
            break;
          }
          const delay = (prioritizedCount / DRAIN_THRESHOLD);
          await new Promise(resolve => setTimeout(resolve, delay * 1000));
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
        if (!filteredJobs.length) continue;

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
          console.log(YELLOW + `No active jobs in chunk ${chunkIndex + 1}, skipping...` + RESET);
          continue;
        }
        if (task && task.running) taskStatus[taskId].status = 'QUEUEING'
        let retries = 3;
        const blurJobs = filteredJobs.filter((job): job is NonNullable<typeof job> => job !== null).filter(job => BLUR_JOBS.includes(job.name));
        const nonBlurJobs = filteredJobs.filter((job): job is NonNullable<typeof job> => job !== null).filter(job => !BLUR_JOBS.includes(job.name));
        while (retries > 0) {
          try {
            await rateLimiter.acquire();
            const addedJobs = await queue.addBulk(
              [
                ...nonBlurJobs.map(job => ({
                  name: job.name,
                  data: job.data,
                  opts: {
                    ...(job.jobKey ? { jobId: job.jobKey } : {}),
                    ...job?.opts,
                    removeOnComplete: true,
                    removeOnFail: true,
                    attempts: 0,
                  },
                })),
              ]
            );
            // Group blur jobs by wallet address
            const blurJobsByWallet = new Map<string, typeof blurJobs>();
            for (const job of blurJobs) {
              const wallet = job.data.address;
              if (!blurJobsByWallet.has(wallet)) {
                blurJobsByWallet.set(wallet, []);
              }
              blurJobsByWallet.get(wallet)?.push(job);
            }
            for (const [wallet, walletJobs] of blurJobsByWallet.entries()) {
              const waitTime = 10 * 1000;
              const rateLimitInfo = blurRateLimitInfo[wallet];
              if (rateLimitInfo) {
                if (rateLimitInfo.remaining === 0) {
                  await new Promise(resolve => setTimeout(resolve, waitTime));
                  blurRateLimitInfo[wallet] = {
                    remaining: 40,
                    reset: 10,
                    limit: 40
                  };
                }
              } else {
                blurRateLimitInfo[wallet] = {
                  remaining: 40,
                  reset: 10,
                  limit: 40
                };
              }
              for (const [index, job] of walletJobs.entries()) {
                const currentRateLimit = blurRateLimitInfo[wallet];
                if (currentRateLimit.remaining === 0) {
                  await new Promise(resolve => setTimeout(resolve, waitTime));
                  blurRateLimitInfo[wallet] = {
                    remaining: 40,
                    reset: 10,
                    limit: 40
                  };
                }
                if (index > 0) {
                  const delayBetweenJobs = 250; // 250ms between each job
                  await new Promise(resolve => setTimeout(resolve, delayBetweenJobs));
                }
                blurRateLimitInfo[wallet].remaining = Math.max(0, blurRateLimitInfo[wallet].remaining - 1);
                queue.add(job.name, job.data, {
                  ...(job.jobKey ? { jobId: job.jobKey } : {}),
                  removeOnComplete: true,
                  removeOnFail: true,
                  attempts: 0,
                });
              }
            }
            totalAdded += (addedJobs.length + blurJobs.length);
            tracker.completedJobs = totalAdded;
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
    tracker.status = 'completed';
    tracker.processedJobs = tracker.totalJobs;
    bulkJobTrackers.set(bulkJobId, tracker);
  } catch (error) {
    tracker.status = 'failed';
    bulkJobTrackers.set(bulkJobId, tracker);
  }
}

// Helper function to wait for queue drain
async function waitForQueueDrain(): Promise<void> {
  while (true) {
    const counts = await queue.getJobCounts();
    if ((counts.prioritized || 0) <= DRAIN_THRESHOLD) {
      return;
    }
    await new Promise(resolve => setTimeout(resolve, 1000));
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
  const baseKey = `${job?.data?._id}` || Math.floor(Math.random() * Number.MAX_SAFE_INTEGER).toString()
  const key = `${baseKey}-${uniqueKey}-${job?.data?.contract?.slug || job?.data?.slug}`
  return key
}

function generateJobKey(job: any): string {
  const taskId = job?.data?._id || '';
  const contractSlug = job?.data?.contract?.slug || job?.data?.slug || '';
  const name = job?.name || '';

  return `${taskId}-${contractSlug}-${name}`;
}


async function startTask(task: ITask, start: boolean, marketplace?: string) {
  if (!API_KEY) {
    console.log(RED + 'API_KEY is not set' + RESET);
    await queue.pause();
    return;
  }

  if (!marketplace) {
    skipStats[task._id] = {
      opensea: 0,
      magiceden: 0,
      blur: 0
    };

    errorStats[task._id] = {
      opensea: 0,
      magiceden: 0,
      blur: 0
    };

    taskStatus[task._id] = {
      status: 'STARTED',
      runningPercentage: 0,
      waitingTime: 0,
      cancellationPercentage: 0,
      processingPercentage: 0,
      nextLoopTime: new Date()
    };
  }

  await BidLogs.deleteMany({ taskId: task._id });

  const RATE_LIMIT = process.env.RATE_LIMIT;
  if (!RATE_LIMIT) {
    console.log(RED + 'RATE_LIMIT is not set' + RESET);
    await queue.pause();
    return;
  }

  const taskId = task._id.toString();

  try {
    await removeTaskFromAborted(taskId);
    const updatedTask = { ...task, running: true };
    activeTasks.set(taskId, updatedTask);
    const taskIndex = currentTasks.findIndex(t => t._id === task._id);
    if (taskIndex !== -1) {
      currentTasks[taskIndex].running = true;
    }
    console.log(GREEN + `Updated task ${task.contract.slug} running status to: ${start} `.toUpperCase() + RESET);
    // Always connect to WebSocket and subscribe all tasks
    try {
      if (!wsManager?.isConnected()) {
        try {
          await connectWebSocket();
        } catch (error) {
          console.error('Failed to establish WebSocket connection for task:', error);
        }
      }

      console.log("subscribing to collection: ", task.contract.slug);
      const newTask = { ...task, running: true };
      await subscribeToCollections([newTask]);
    } catch (error) {
      console.error(RED + `Error subscribing to collection ${task.contract.slug}: ` + RESET, error);
    }

    const validMarketplaces = ["opensea", "magiceden", "blur"];
    const selectedMarketplaces = marketplace ? [marketplace.toLowerCase()] : task.selectedMarketplaces.map(m => m.toLowerCase());
    const jobs = selectedMarketplaces
      .filter((m) => enabledMarketplaces.map((m) => m.toLowerCase()).includes(m))
      .filter(m => validMarketplaces.includes(m))
      .map(m => {
        switch (m) {
          case "opensea":
            return { name: OPENSEA_SCHEDULE, data: { ...task, running: start } };
          case "blur":
            return { name: BLUR_SCHEDULE, data: { ...task, running: start } };
          case "magiceden":
            return { name: MAGICEDEN_SCHEDULE, data: { ...task, running: start } };
          default:
            return null;
        }
      })
      .filter(job => job !== null);

    await queue.addBulk(jobs as Job[]);
  } catch (error) {
    console.error(RED + `Error starting task ${taskId}: ` + RESET, error);
  } finally {
    // Always release lock when done
    unlockTask(taskId);
  }
}


async function stopTask(task: ITask, start: boolean, marketplace?: string) {
  const taskId = task._id.toString();
  const wallet = await Wallet.findById(task.walletId)
  const WALLET_PRIVATE_KEY = await decryptPrivateKey(wallet?.encryptedPrivateKey as string) as string;

  try {
    if (!marketplace || task.selectedMarketplaces.length === 0) {
      markTaskAsAborted(taskId);
      const updatedTask = { ...task, running: false };
      activeTasks.set(taskId, updatedTask);
    }

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



    if (!marketplace) {
      taskStatus[taskId] = {
        status: 'CANCELLING',
        cancellationPercentage: 0,
        runningPercentage: 0,
        waitingTime: 0,
        nextLoopTime: new Date(),
        processingPercentage: 0
      }

      warningBids[taskId] = {
        opensea: false,
        magiceden: false,
        blur: false
      };
    }

    switch (marketplace?.toLowerCase()) {
      case 'opensea':
        await cancelOpenseaRequest(taskId)
        const openseaParentJobId = `${taskId}-opensea`.toLowerCase()
        const openseaParentJob = parentJobs.get(openseaParentJobId)
        if (openseaParentJob) {
          await parentJobs.delete(openseaParentJobId)
        }
        break;
      case 'magiceden':
        await cancelMagicedenRequest(taskId)
        const magicedenParentJobId = `${taskId}-magiceden`.toLowerCase()
        const magicedenParentJob = parentJobs.get(magicedenParentJobId)
        if (magicedenParentJob) {
          await parentJobs.delete(magicedenParentJobId)
        }
        break;
      case 'blur':
        await cancelBlurRequest(taskId)
        const blurParentJobId = `${taskId}-blur`.toLowerCase()
        const blurParentJob = parentJobs.get(blurParentJobId)
        if (blurParentJob) {
          await parentJobs.delete(blurParentJobId)
        }
        break;
      default:
        await cancelOpenseaRequest(taskId)
        await cancelMagicedenRequest(taskId)
        await cancelBlurRequest(taskId)
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

    await updateTaskStatus(task, start, marketplace);
    if (task.outbidOptions.counterbid && !marketplace) {
      try {
        await unsubscribeFromCollection(task);
      } catch (error) {
        console.error(RED + `Error unsubscribing from collection for task ${task.contract.slug}: ` + RESET, error);
      }
    }
    await removePendingAndWaitingBids(task, marketplace)

    if (!task?.selectedMarketplaces.length || !task.running) {
      delete taskStatus[taskId].nextLoopTime
      taskStatus[taskId].waitingTime = 0
    }

    // get all the task, if no task, change state to stopped

    const orders = await prisma.order.findMany({
      where: {
        taskId: taskId,
        // ...(marketplace && { marketplace: marketplace.toLowerCase() })
      }
    })

    if (!orders.length) {
      delete taskStatus[taskId].nextLoopTime
      taskStatus[taskId].waitingTime = 0
      taskStatus[taskId].status = 'STOPPED'
    }

    await cancelAllRelatedBids(task, WALLET_PRIVATE_KEY, marketplace);

  } catch (error: any) {
    console.error(`[StopTask] Error in stopTask for ${taskId}:`, {
      error: error.message,
      command: error.command,
      timestamp: new Date().toISOString()
    });
  }
}

async function deleteTask(task: ITask) {
  try {
    await stopTask(task, false);
    const taskIndex = currentTasks.findIndex(t => t._id === task._id);
    if (taskIndex !== -1) {
      currentTasks.splice(taskIndex, 1);
    }
    activeTasks.delete(task._id);
  } catch (error) {
    console.error(RED + `Error deleting task: ` + RESET, error);
  }
}


const BATCH_SIZE = 1000// Process 100 jobs at a time

async function removePendingAndWaitingBids(task: ITask, marketplace?: string) {
  try {
    let jobIds: string[] = []
    switch (marketplace?.toLowerCase()) {
      case OPENSEA.toLowerCase():
        jobIds = getJobIds(OPENSEA.toLowerCase(), task)
        break;
      case MAGICEDEN.toLowerCase():
        jobIds = getJobIds(MAGICEDEN.toLowerCase(), task)
        break;
      case BLUR.toLowerCase():
        jobIds = getJobIds(BLUR.toLowerCase(), task)
        break;
      default:
        jobIds = [
          ...getJobIds(BLUR.toLowerCase(), task),
          ...getJobIds(MAGICEDEN.toLowerCase(), task),
          ...getJobIds(OPENSEA.toLowerCase(), task)
        ]
    }

    let totalJobsRemoved = 0;
    try {
      let start = 0;
      let hasMoreJobs = true;

      while (hasMoreJobs) {
        // Get jobs in batches
        const jobs: Job[] = await queue.getJobs([], start, start + BATCH_SIZE - 1);
        console.log(`Processing batch of ${jobs.length} jobs starting at index ${start}`);


        if (jobs.length === 0) {
          hasMoreJobs = false;
          break;
        }

        const relevantJobs = jobs.filter((job) => job?.id && jobIds.includes(job.id));
        // Process this batch of jobs
        await Promise.all(relevantJobs.map(async (job) => {
          try {
            await job.remove();
            totalJobsRemoved++;

          } catch (error) {
            totalJobsRemoved++;
          }
        }));

        // Move to the next batch
        start += BATCH_SIZE;

        // If we got fewer jobs than the batch size, we've reached the end
        if (jobs.length < BATCH_SIZE) {
          hasMoreJobs = false;
        }
      }

      console.log(`Removed ${totalJobsRemoved} jobs for task`);
    } catch (error) {
      console.error(RED + `Error removing jobs: ${error}` + RESET);
    }

    console.log('=== End Summary ====' + RESET);
    console.log(GREEN + 'Queue resumed after removing pending bids' + RESET);

  } catch (error) {
    console.error(RED + `Error removing pending bids: ${error} ` + RESET);
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

async function updateTaskStatus(task: ITask, running: boolean, marketplace?: string) {
  if (!warningBids[task._id]) {
    warningBids[task._id] = {
      opensea: false,
      magiceden: false,
      blur: false
    };
  }

  const taskIndex = currentTasks.findIndex(t => t._id === task._id);
  if (taskIndex !== -1) {
    if (marketplace) {
      return
    } else {
      currentTasks[taskIndex].running = running;
    }
  }
  console.log(running ? GREEN : RED + `${running ? 'Started' : 'Stopped'} processing task ${task.contract.slug} `.toUpperCase() + RESET);
}

async function fetchAndCancelOpenseaBids(task: ITask, WALLET_PRIVATE_KEY: string) {
  try {
    const { currencySymbol } = await calculateBidPrice(task, 0, "magiceden")

    const allOpenseaBids = await prisma.order.findMany({
      where: {
        slug: task.contract.slug,
        taskId: task._id,
        marketplace: OPENSEA.toLowerCase()
      }
    });

    const parentJobId = `${task._id}-CANCEL_OPENSEA_BID`

    parentJobs.set(parentJobId, {
      parentId: parentJobId,
      totalJobs: allOpenseaBids.length,
      completedJobs: 0,
      failedJobs: 0,
      startTime: Date.now(),
      taskId: task._id,
      jobName: CANCEL_OPENSEA_BID
    })

    if (allOpenseaBids.length > 0) {
      await cancelOpenseaBids(allOpenseaBids, WALLET_PRIVATE_KEY, task._id, currencySymbol);
    }
  } catch (error) {
    console.log(error);
  }
}

async function fetchAndCancelBlurBids(task: ITask, WALLET_PRIVATE_KEY: string) {
  try {
    const allBlurBids = await prisma.order.findMany({
      where: {
        slug: task.contract.slug,
        taskId: task._id,
        marketplace: BLUR.toLowerCase()
      }
    });

    const parentJobId = `${task._id}-CANCEL_BLUR_BID`

    parentJobs.set(parentJobId, {
      parentId: parentJobId,
      totalJobs: allBlurBids.length,
      completedJobs: 0,
      failedJobs: 0,
      startTime: Date.now(),
      taskId: task._id,
      jobName: CANCEL_BLUR_BID
    })

    if (allBlurBids.length > 0) {
      await cancelBlurBids(allBlurBids, WALLET_PRIVATE_KEY, task._id);
    }
  } catch (error) {
    console.log(error);
  }
}

async function fetchAndCancelMagicedenBids(task: ITask, WALLET_PRIVATE_KEY: string) {
  try {
    const allMagicedenBids = await prisma.order.findMany({
      where: {
        slug: task.contract.slug,
        taskId: task._id,
        marketplace: MAGICEDEN.toLowerCase()
      }
    });
    const parentJobId = `${task._id}-CANCEL_MAGICEDEN_BID`
    const totalJobs = allMagicedenBids.length / 500
    parentJobs.set(parentJobId, {
      parentId: parentJobId,
      totalJobs: totalJobs,
      completedJobs: 0,
      failedJobs: 0,
      startTime: Date.now(),
      taskId: task._id,
      jobName: CANCEL_MAGICEDEN_BID
    })
    await cancelMagicedenBids(allMagicedenBids, WALLET_PRIVATE_KEY, task._id);
  } catch (error) {
    console.log(error);
  }
}


async function cancelAllRelatedBids(task: ITask, WALLET_PRIVATE_KEY: string, marketplace?: string, attempt: number = 1,): Promise<void> {
  try {
    // Delete parent jobs for the appropriate marketplace(s)
    if (marketplace?.toLowerCase()) {
      const parentJobId = `${task._id}-CANCEL_${marketplace.toUpperCase()}_BID`;
      parentJobs.delete(parentJobId);

      switch (marketplace.toLowerCase()) {
        case OPENSEA.toLowerCase():
          await fetchAndCancelOpenseaBids(task, WALLET_PRIVATE_KEY);
          break;
        case MAGICEDEN.toLowerCase():
          await fetchAndCancelMagicedenBids(task, WALLET_PRIVATE_KEY);
          break;
        case BLUR.toLowerCase():
          await fetchAndCancelBlurBids(task, WALLET_PRIVATE_KEY);
          break;
      }
    } else {
      // Delete all marketplace parent jobs
      const openSeaParentJobId = `${task._id}-CANCEL_OPENSEA_BID`;
      const magicEdenParentJobId = `${task._id}-CANCEL_MAGICEDEN_BID`;
      const blurParentJobId = `${task._id}-CANCEL_BLUR_BID`;

      parentJobs.delete(openSeaParentJobId);
      parentJobs.delete(magicEdenParentJobId);
      parentJobs.delete(blurParentJobId);

      await Promise.all([
        fetchAndCancelOpenseaBids(task, WALLET_PRIVATE_KEY),
        fetchAndCancelMagicedenBids(task, WALLET_PRIVATE_KEY),
        fetchAndCancelBlurBids(task, WALLET_PRIVATE_KEY)
      ]);
    }

  } catch (error) {
    console.error(RED + `Unexpected error during bid cancellation(attempt ${attempt}): `, error, RESET);
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
  for (let i = 0; i < orders.length; i += 500) {
    const batchOrders = orders.slice(i, i + 500);
    const extractedOrderIds = await extractMagicedenOrderHash(batchOrders)
    const orderKeys = batchOrders.map((order: PrismaOrder) => order.id)
    await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey, orderKeys, taskId }, { jobId: `CANCEL_MAGICEDEN_BID-${taskId}-${orderKeys[0]}` });
  }
}

async function cancelBlurBids(orders: PrismaOrder[], privateKey: string, taskId: string) {
  if (!orders.length) return
  const cancelData = orders.map((order, index) => {
    const payload = JSON.parse(order.orderHash)
    return {
      name: CANCEL_BLUR_BID,
      data: { privateKey, payload, taskId, orderKey: order.id },
      opts: {
        jobId: `CANCEL_BLUR_BID-${taskId}-${order.id}`
      }
    }
  }) as Job[]
  await processBulkCancellations(cancelData);
}

async function cancelOpenseaBids(orders: PrismaOrder[], privateKey: string, taskId: string, currencySymbol: string) {
  if (!orders.length) return;
  const cancelData = orders.map((order, index) => {
    return {
      name: CANCEL_OPENSEA_BID,
      data: { privateKey, orderId: order.orderHash, taskId, orderKey: order.id },
      opts: {
        jobId: `CANCEL_OPENSEA_BID-${taskId}-${order.id}`
      }
    }
  }) as Job[]

  await processBulkCancellations(cancelData)
  // await queue.addBulk(cancelData)
}

async function updateStatus(task: ITask) {
  try {
    const { _id: taskId, running } = task;
    const start = !running;

    if (start) {
      // Update activeTasks Map
      const updatedTask = { ...task, running: true };
      activeTasks.set(taskId, updatedTask);
      if (!taskStatus[taskId]) {
        taskStatus[taskId] = {
          status: 'STARTED',
          cancellationPercentage: 0,
          runningPercentage: 0,
          waitingTime: 0,
          nextLoopTime: new Date(),
          processingPercentage: 0
        }
      }
      taskStatus[taskId].status = 'STARTED'
      await startTask(task, true);
    } else {
      // Update activeTasks Map
      const updatedTask = { ...task, running: false };
      activeTasks.set(taskId, updatedTask);
      if (!taskStatus[taskId]) {
        taskStatus[taskId] = {
          status: 'CANCELLING',
          cancellationPercentage: 0,
          runningPercentage: 0,
          waitingTime: 0,
          nextLoopTime: new Date(),
          processingPercentage: 0
        }
      }
      taskStatus[taskId].status = 'CANCELLING'
      await stopTask(task, false);
    }
  } catch (error) {
    console.error(RED + `Error updating status for task: ${task._id} ` + RESET, error);
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
      console.log(RED + `Removing marketplace: ${outgoing.toUpperCase()} for collection: ${task.contract.slug} `.toUpperCase() + RESET);
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

      console.log(color + `Adding marketplace: ${incoming.toUpperCase()} for collection: ${task.contract.slug} `.toUpperCase() + RESET);

      switch (incoming.toLowerCase()) {
        case "opensea":
          await queue.add(OPENSEA_SCHEDULE, task, { priority: COLLECTION_BID_PRIORITY.OPENSEA });
          break;
        case "magiceden":
          await queue.add(MAGICEDEN_SCHEDULE, task, { priority: COLLECTION_BID_PRIORITY.MAGICEDEN });
          break;
        case "blur":
          await queue.add(BLUR_SCHEDULE, task, { priority: COLLECTION_BID_PRIORITY.BLUR });
          break;
      }
    }

  } catch (error) {
    console.error(RED + `Error updating marketplace for task: ${task._id} ` + RESET, error);
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
      await Promise.all(tasks.map(async (task) => {
        try {
          await startTask(task, true);
        } catch (error) {
          console.error(RED + `Error starting task ${task.contract.slug}: ` + RESET, error);
        }
      }));
    } else {
      await Promise.all(tasks.map(async (task) => {
        try {
          await stopTask(task, false);
        } catch (error) {
          console.error(RED + `Error stopping task ${task.contract.slug}: ` + RESET, error);
        }
      }));
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


async function connectWebSocket(): Promise<void> {
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
    wsManager.on('connected', (connectionId) => {
      console.log(GOLD + `CONNECTED TO MARKETPLACE EVENTS WEBSOCKET (ID: ${connectionId}) at ${new Date().toISOString()}` + RESET);

      // Update connection metrics
      connectionMetrics.connectionStartTime = Date.now();
      connectionMetrics.currentConnectionId = connectionId;
      connectionMetrics.totalConnections++;
      retryCount = 0;

      // Clear activeSubscriptions to sync with server
      activeSubscriptions.clear();
      console.log(YELLOW + "Cleared active subscriptions for fresh reconnect" + RESET);

      // Subscribe to collections after connection
      subscribeToCollections(currentTasks as unknown as ITask[]);
    });

    wsManager.on('disconnected', (event: { code: number; reason: string }) => {
      // Calculate connection duration
      if (connectionMetrics.connectionStartTime) {
        connectionMetrics.connectionEndTime = Date.now();
        connectionMetrics.connectionDuration = connectionMetrics.connectionEndTime - connectionMetrics.connectionStartTime;
        connectionMetrics.totalDisconnections++;

        updateConnectionStats(connectionMetrics.connectionDuration, getCloseReason(event.code));

        console.log(YELLOW + `WebSocket disconnected after ${formatDuration(connectionMetrics.connectionDuration)} (Code: ${event.code}) - cleared subscription cache` + RESET);
      } else {
        console.log(YELLOW + "WebSocket closed - cleared subscription cache" + RESET);
      }

      // Clear subscription cache on disconnect
      activeSubscriptions.clear();
    });

    wsManager.on('message', async (message: any) => {
      try {
        if (message.type === 'pong') {
          console.log('Pong received from server.');
          return;
        }

        // Handle initial data request
        if (message.endpoint === "get-initial-data") {
          const initialDataMessage = {
            type: 'bidRatesUpdate',
            data: clientData
          };
          wsManager.sendMessage(initialDataMessage);
          return;
        }

        // Handle counter bid messages
        await handleCounterBid(message);
        // Handle listing price updates for instant price matching
        await handleListingPriceUpdate(wsManager, message);
      } catch (error) {
        console.error('Error handling message:', error);
      }
    });

    wsManager.on('error', (error: Error) => {
      logger.logWebSocketError("WebSocket connection error", {
        error: error,
        connectionId: connectionMetrics.currentConnectionId || 'unknown',
        retryCount: retryCount,
        connectionDuration: connectionMetrics.connectionStartTime ? Date.now() - connectionMetrics.connectionStartTime : 0
      });
    });

    wsManager.on('server_status', (status: any) => {
      console.log(`Server Status Update:`, status);
      // Update any UI or metrics as needed
    });

    wsManager.on('max_retries_exceeded', () => {
      logger.logWebSocketError("WebSocket max reconnection attempts exceeded", {
        connectionId: connectionMetrics.currentConnectionId || 'unknown',
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


async function handleCounterBid(message: any) {
  try {

    const { contractAddress, slug } = getMarketplaceDetails(message);
    if (!contractAddress && !slug) return

    const relevantTasks = currentTasks.filter(task => {
      // Check if task is valid and running
      if (!task?.running || !task?.contract) {
        return false;
      }

      const taskContractAddress = task.contract.contractAddress?.toLowerCase();
      const taskSlug = task.contract.slug?.toLowerCase();
      const messageContractAddress = contractAddress?.toLowerCase();
      const messageSlug = slug?.toLowerCase();

      // Match by counterbid option
      const hasCounterbid = task.outbidOptions?.counterbid;

      // Match by contract address or slug
      const contractMatches = taskContractAddress === messageContractAddress ||
        taskSlug === messageSlug;

      // Match by stop options
      const hasValidStopOptions = taskSlug === messageSlug &&
        task.stopOptions?.triggerStopOptions &&
        Number(task.stopOptions?.maxPurchase) > 0;

      return (hasCounterbid && contractMatches) || hasValidStopOptions;
    });

    for (const task of relevantTasks) {
      if (message && message?.event && message.event === "item_transferred") {
        const toAccount = message?.payload?.payload?.to_account.toLowerCase() === task.wallet.toLowerCase()
        if (toAccount) {
          if (!balances[task._id]) {
            balances[task._id] = {
              opensea: 0,
              magiceden: 0,
              blur: 0
            }
          }
          balances[task._id].opensea++
        }
      }
    }

    if (!relevantTasks.length) return;

    await Promise.all(relevantTasks.map(task => handleCounterBidForTask(task, message)));
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
      console.log(`Unknown marketplace: ${message.marketplace} `);
      return {};
  }
}

async function handleCounterBidForTask(task: any, message: any) {
  const selectedMarketplaces = task.selectedMarketplaces.map((m: string) => m.toLowerCase());

  if (selectedMarketplaces.includes('blur') && message.marketplace === BLUR) {
    await handleBlurCounterbid(message['1'], task);
  }

  if (selectedMarketplaces.includes('opensea') && message.marketplace === OPENSEA) {
    await handleOpenseaCounterbid(message, task);
  }

  if (selectedMarketplaces.includes('magiceden') && message.marketplace === MAGICEDEN) {
    await handleMagicEdenCounterbid(message, task)
  }
}


async function handleMagicEdenCounterbid(data: any, task: ITask) {
  try {
    const currentTask = activeTasks.get(task._id)
    if (!currentTask || !currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("magiceden") || !enabledMarketplaces
      .map((m) => m.toLowerCase())
      .includes("magiceden")) return;

    const expiration = getExpiry(currentTask.bidDuration)
    if (!currentTask.outbidOptions.counterbid) return

    const orderHash = data?.data?.id
    // Include task ID in the hash to make it task-specific
    const taskSpecificHash = `${orderHash}-${task._id}`;
    if (hasSeenItem(seenOrderHashes, taskSpecificHash)) return;
    addSeenItem(seenOrderHashes, taskSpecificHash)


    const domain: string = data?.data?.source?.domain
    const wallets = await Wallet.find({})
    const walletAddresses = wallets.map((wallet) => wallet.address.toLowerCase())
    const WALLET_PRIVATE_KEY = await decryptPrivateKey(wallets.find((wallet) => wallet.address.toLowerCase() === task.wallet.toLowerCase())?.encryptedPrivateKey as string) as string;
    const maker = data?.data?.maker?.toLowerCase()

    if (walletAddresses.includes(maker)) return

    const { currencyType, currency, decimal, currencySymbol } = await calculateBidPrice(currentTask, 0, "magiceden")
    const usdcBid = currencySymbol.toLowerCase() === "usdc"
    const magicedenOutbidMargin = currentTask?.outbidOptions?.magicedenOutbidMargin || usdcBid ? 0.01 : 0.0001

    const bidType = data?.data?.criteria?.kind
    const tokenId = +data?.data?.criteria?.data?.token?.tokenId;
    const usdPrice = Number(data?.data?.price?.amount?.usd)
    const eth_price = Number(data?.data?.price?.amount?.native)

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
      console.log(MAGENTA + '----------------------------------------------------------------------------------' + RESET);
      console.log(MAGENTA + `incoming bid for ${task.contract.slug}:${tokenId} for ${incomingBidAmount / Math.pow(10, decimal)} ${currencySymbol} on magiceden`.toUpperCase() + RESET);
      console.log(MAGENTA + '----------------------------------------------------------------------------------' + RESET);

      const floor_price = Number(await fetchMagicEdenCollectionStats(task.contract.contractAddress))
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
          await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey: WALLET_PRIVATE_KEY, orderKeys: orderKeys, taskId: task._id });
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

        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
        console.log(RED + message + RESET);
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);

        if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
          if (orders.length > 0) {
            const orderKeys = orders.map((order: PrismaOrder) => order.id)
            const extractedOrderIds = await extractMagicedenOrderHash(orders)
            await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey: WALLET_PRIVATE_KEY, orderKeys: orderKeys, taskId: task._id });
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
          console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
          console.log(RED + `Skipping counterbid since ${task.contract.slug}:${tokenId} incoming bid ${incomingBidAmount / Math.pow(10, decimal)} ${currencySymbol} is less than or equal to existing bid ${Number(job.data.offerPrice) / Math.pow(10, decimal)} ${currencySymbol} Domain: ${domain}` + RESET);
          console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
          return
        } else {
          await job.updateData(bidData)
        }
      } else {
        await queue.add(
          MAGICEDEN_TOKEN_BID_COUNTERBID,
          bidData,
          {
            jobId,
          },

        );
      }

      seenOrderHashes.delete(orderHash);

      console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(GREEN + `Counterbidding incoming magiceden offer of ${Number(incomingBidAmount) / Math.pow(10, decimal)} ${currencySymbol} for ${task.contract.slug} ${tokenId} for ${Number(offerPrice) / Math.pow(10, decimal)} ${currencySymbol} ON MAGICEDEN`.toUpperCase() + RESET);
      console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
      if (orders.length > 0) {
        const extractedOrderIds = await extractMagicedenOrderHash(orders)
        const orderKeys = orders.map((order: PrismaOrder) => order.id)
        await queue.add(CANCEL_MAGICEDEN_BID, {
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

      console.log(MAGENTA + '----------------------------------------------------------------------------------' + RESET);
      console.log(MAGENTA + `incoming bid for ${task.contract.slug} ${JSON.stringify(trait)} for ${incomingBidAmount / Math.pow(10, decimal)} ${currencySymbol} on magiceden`.toUpperCase() + RESET);
      console.log(MAGENTA + '----------------------------------------------------------------------------------' + RESET);

      const floor_price = Number(await fetchMagicEdenCollectionStats(task.contract.contractAddress))
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
          await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey: WALLET_PRIVATE_KEY, orderKeys: orderKeys, taskId: task._id });
        }
        return
      }


      const offers = orders.map((order: PrismaOrder) => order.offerPrice)
      const currentBidPrice = !offers.length ? 0 : Math.max(...offers.map((offer) => Number(offer)))

      if (incomingBidAmount < currentBidPrice) return

      offerPrice = Math.ceil((magicedenOutbidMargin * Math.pow(10, decimal)) + incomingBidAmount)
      const offerPriceEth = offerPrice / Math.pow(10, decimal)

      if (maxBidPriceEth > 0 && offerPriceEth > maxBidPriceEth) {
        const message = `magiceden counter offer ${offerPriceEth} ${currencySymbol} for ${task.contract.slug} ${JSON.stringify(trait)} exceeds max bid price ${maxBidPriceEth} ${currencySymbol} ON MAGICEDEN.Skipping ...`.toUpperCase()
        await logBidError(task._id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "magiceden");

        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
        console.log(RED + message + RESET);
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);

        if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
          if (orders.length > 0) {
            const orderKeys = orders.map((order: PrismaOrder) => order.id)
            const extractedOrderIds = await extractMagicedenOrderHash(orders)
            await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey: WALLET_PRIVATE_KEY, orderKeys: orderKeys, taskId: task._id });
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
          console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
          console.log(RED + `Skipping counterbid since incoming bid ${task.contract.slug}:${JSON.stringify(trait)} ${incomingBidAmount / Math.pow(10, decimal)} ${currencySymbol} is less than or equal to existing bid ${Number(job.data.offerPrice) / Math.pow(10, decimal)} ${currencySymbol} Domain: ${domain}` + RESET);
          console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
          return
        } else {
          await job.updateData(data)
        }
      } else {
        await queue.add(
          MAGICEDEN_TRAIT_BID_COUNTERBID,
          bidData,
          {
            jobId,
          },
        );
      }
      seenOrderHashes.delete(orderHash);

      console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(GREEN + `Counterbidding incoming offer of ${Number(incomingBidAmount) / Math.pow(10, decimal)} ${currencySymbol} for ${task.contract.slug} trait ${JSON.stringify(trait)} for ${Number(offerPrice) / Math.pow(10, decimal)} ${currencySymbol} ON MAGICEDEN `.toUpperCase() + RESET);
      console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
      if (orders.length > 0) {
        const extractedOrderIds = await extractMagicedenOrderHash(orders)
        const orderKeys = orders.map((order: PrismaOrder) => order.id)
        await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey: WALLET_PRIVATE_KEY, orderKeys: orderKeys, taskId: task._id });
      }


    }
    else if (bidType === "collection") {
      const isTraitBid = task.bidType === "collection" && selectedTraits && Object.keys(selectedTraits).length > 0
      const tokenBid = task.bidType === "token"
      const collectionBid = task.bidType.toLowerCase() === "collection" && Object.keys(task?.selectedTraits?.magiceden || {}).length === 0

      if (!collectionBid) return

      console.log(MAGENTA + '----------------------------------------------------------------------------------' + RESET);
      console.log(MAGENTA + `incoming collection offer for ${task.contract.slug} for ${incomingBidAmount / Math.pow(10, decimal)} ${currencySymbol} on magiceden`.toUpperCase() + RESET);
      console.log(MAGENTA + '----------------------------------------------------------------------------------' + RESET);

      const floor_price = Number(await fetchMagicEdenCollectionStats(task.contract.contractAddress))
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
          await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey: WALLET_PRIVATE_KEY, orderKeys: orderKeys, taskId: task._id });
        }
        return
      }


      const offers = orders.map((order: PrismaOrder) => order.offerPrice)
      const currentBidPrice = !offers.length ? 0 : Math.max(...offers.map((offer) => Number(offer)))

      if (incomingBidAmount < currentBidPrice) return

      const roundedEth = roundToValidPrecision(incomingBidAmountEth)
      offerPrice = Math.ceil((magicedenOutbidMargin * Math.pow(10, decimal)) + (roundedEth * Math.pow(10, decimal)))
      const offerPriceEth = offerPrice / Math.pow(10, decimal)

      if (maxBidPriceEth > 0 && offerPriceEth > maxBidPriceEth) {
        const message = `counter Offer ${offerPriceEth} ${currencySymbol} for ${task.contract.slug}  exceeds max bid price ${maxBidPriceEth} ${currencySymbol}.Skipping ...`.toUpperCase()
        await logBidError(task._id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "magiceden");
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
        console.log(RED + message + RESET);
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);

        if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
          if (orders.length > 0) {
            const orderKeys = orders.map((order: PrismaOrder) => order.id)
            const extractedOrderIds = await extractMagicedenOrderHash(orders)
            await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey: WALLET_PRIVATE_KEY, orderKeys: orderKeys, taskId: task._id });
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
          console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
          console.log(RED + `Skipping counterbid since incoming bid ${task.contract.slug}:collection ${incomingBidAmount / Math.pow(10, decimal)} ${currencySymbol} is less than or equal to existing bid ${Number(job.data.offerPrice) / Math.pow(10, decimal)} ${currencySymbol} Domain: ${domain}` + RESET);
          console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
          return
        } else {
          await job.updateData(bidData)
        }
      } else {
        await queue.add(
          MAGICEDEN_COLLECTION_BID_COUNTERBID,
          bidData,
          {
            jobId,
          },

        );
      }
      seenOrderHashes.delete(orderHash);

      console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(GREEN + `Counterbidding incoming magiceden offer of ${Number(incomingBidAmount) / Math.pow(10, decimal)} ${currencySymbol} for ${task.contract.slug} for ${Number(offerPrice) / Math.pow(10, decimal)} ${currencySymbol}`.toUpperCase() + RESET);
      console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);

      if (orders.length > 0) {
        const orderKeys = orders.map((order: PrismaOrder) => order.id)
        const extractedOrderIds = await extractMagicedenOrderHash(orders)
        await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey: WALLET_PRIVATE_KEY, orderKeys: orderKeys, taskId: task._id });
      }

    }
  } catch (error) {
    console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
    console.log(RED + `Error processing magiceden counterbid: ${error}` + RESET);
    console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
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
    if (!outbidOptions.counterbid) return

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

    const wallets = await Wallet.find({})
    const walletAddresses = wallets.map((wallet) => wallet.address.toLowerCase())
    const WALLET_PRIVATE_KEY = await decryptPrivateKey(wallets.find((wallet) => wallet.address.toLowerCase() === task.wallet.toLowerCase())?.encryptedPrivateKey as string);
    const orderHash = data.payload.payload.order_hash

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
    const taskSpecificHash = `${orderHash}-${task._id}`;
    if (hasSeenItem(seenOrderHashes, taskSpecificHash)) return;
    addSeenItem(seenOrderHashes, taskSpecificHash)

    if (walletAddresses.includes(maker)) return

    const incomingBidAmountEth = incomingBidAmount / Math.pow(10, decimal);
    const collectionDetails = await getCollectionDetails(task.contract.slug, task.contract.contractAddress);

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

      console.log(BLUE + '---------------------------------------------------------------------------------' + RESET);
      console.log(BLUE + `incoming offer for ${task.contract.slug}:${tokenId} for ${bidDetails} ${similarCurrency ? '' : `(${extraDetails})`} on opensea`.toUpperCase() + RESET);
      console.log(BLUE + '---------------------------------------------------------------------------------' + RESET);

      const floor_price = await getOpenseaFloor(task.contract.slug);
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
              data: { privateKey: WALLET_PRIVATE_KEY, orderId: order.orderHash, taskId: task._id, orderKey: order.id },
            }
          })
          await queue.addBulk(cancelData)
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
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
        console.log(RED + message + RESET);
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
        if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
          if (orders.length > 0) {
            const cancelData = orders.map((order, index) => {
              return {
                name: CANCEL_OPENSEA_BID,
                data: { privateKey: WALLET_PRIVATE_KEY, orderId: order.orderHash, taskId: task._id, orderKey: order.id },

              }
            })
            await queue.addBulk(cancelData)
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
          console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
          console.log(RED + `Skipping counterbid since incoming bid ${task.contract.slug}:${asset.tokenId} ${incomingBidAmount / Math.pow(10, decimal)} ${currencySymbol} is less than or equal to existing bid ${Number(job.data.offerPrice) / Math.pow(10, decimal)} ${currencySymbol} Domain: Opensea` + RESET);
          console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
          return
        } else {
          await job.updateData(bidData)
        }
      } else {
        await queue.add(
          OPENSEA_TOKEN_BID_COUNTERBID,
          bidData,
          {
            jobId,
          },
        );
      }
      seenOrderHashes.delete(orderHash);
      console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(GREEN + `Counterbidding incoming opensea offer of ${bidDetails} ${similarCurrency ? '' : `(${extraDetails})`} for ${task.contract.slug}:${asset.tokenId} for ${Number(tokenOffer) / Math.pow(10, decimal)} ${currencySymbol} ON OPENSEA`.toUpperCase() + RESET);
      console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);

      if (orders.length > 0) {
        const cancelData = orders.map((order, index) => {
          return {
            name: CANCEL_OPENSEA_BID,
            data: { privateKey: WALLET_PRIVATE_KEY, orderId: order.orderHash, taskId: task._id, orderKey: order.id },
          }
        })
        await queue.addBulk(cancelData)
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

      console.log(BLUE + '---------------------------------------------------------------------------------' + RESET);
      console.log(BLUE + `incoming offer for ${task.contract.slug}:${identifier} for ${bidDetails} ${similarCurrency ? '' : `(${extraDetails})`} on opensea`.toUpperCase() + RESET);
      console.log(BLUE + '---------------------------------------------------------------------------------' + RESET);


      const floor_price = await getOpenseaFloor(task.contract.slug);
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
              data: { privateKey: WALLET_PRIVATE_KEY, orderId: order.orderHash, taskId: task._id, orderKey: order.id },
            }
          })
          await queue.addBulk(cancelData)
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
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
        console.log(RED + `counter Offer ${offerPriceEth} ${currencySymbol} for ${task.contract.slug} ${JSON.stringify(trait)} exceeds max bid price ${maxBidPriceEth} ${currencySymbol} ON OPENSEA.Skipping ...`.toUpperCase() + RESET);
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);

        if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
          if (orders.length > 0) {
            const cancelData = orders.map((order, index) => {
              return {
                name: CANCEL_OPENSEA_BID,
                data: { privateKey: WALLET_PRIVATE_KEY, orderId: order.orderHash, taskId: task._id, orderKey: order.id },
              }
            })
            await queue.addBulk(cancelData)
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
          console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
          console.log(RED + `Skipping counterbid since incoming bid ${incomingBidAmount / Math.pow(10, decimal)} ${currencySymbol} is less than or equal to existing bid ${Number(job.data.offerPrice) / Math.pow(10, decimal)} ${currencySymbol} domain: opensea` + RESET);
          console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
          return
        } else {
          await job.updateData(counterbidData)
        }
      } else {
        await queue.add(
          OPENSEA_TRAIT_BID_COUNTERBID,
          counterbidData,
          {
            jobId,
          },
        );
      }
      seenOrderHashes.delete(orderHash);

      console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(GREEN + `Counterbidding incoming opensea offer of ${bidDetails} ${similarCurrency ? '' : `(${extraDetails})`} for ${task.contract.slug} ${JSON.stringify(trait)} for ${Number(traitOffer) / Math.pow(10, decimal)} ${currencySymbol} ON OPENSEA`.toUpperCase() + RESET);
      console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);

      if (orders.length > 0) {
        const cancelData = orders.map((order, index) => {
          return {
            name: CANCEL_OPENSEA_BID,
            data: { privateKey: WALLET_PRIVATE_KEY, orderId: order.orderHash, taskId: task._id, orderKey: order.id },
          }
        })
        await queue.addBulk(cancelData)
      }
    }
    if (data.event === "collection_offer") {
      const isTraitBid = task.bidType === "collection" && selectedTraits && Object.keys(selectedTraits).length > 0
      const tokenBid = task.bidType === "token"
      const collectionBid = task.bidType.toLowerCase() === "collection" && Object.keys(task?.selectedTraits?.opensea || {}).length === 0

      if (!collectionBid) return

      console.log(BLUE + '---------------------------------------------------------------------------------' + RESET);
      console.log(BLUE + `incoming collection offer for ${task.contract.slug} for  ${bidDetails} ${similarCurrency ? '' : `(${extraDetails})`} on opensea`.toUpperCase() + RESET);
      console.log(BLUE + '---------------------------------------------------------------------------------' + RESET);

      const floor_price = await getOpenseaFloor(task.contract.slug);
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
              data: { privateKey: WALLET_PRIVATE_KEY, orderId: order.orderHash, taskId: task._id, orderKey: order.id },
            }
          })
          await queue.addBulk(cancelData)
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
        const message = `counter Offer price ${offerPriceEth} ${currencySymbol} for ${task.contract.slug} exceeds max bid price ${maxBidPriceEth} ${currencySymbol} ON OPENSEA.Skipping ...`.toUpperCase()
        await logBidError(task._id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "opensea");
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
        console.log(RED + message + RESET);
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);

        if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
          if (orders.length > 0) {
            const cancelData = orders.map((order, index) => {
              return {
                name: CANCEL_OPENSEA_BID,
                data: { privateKey: WALLET_PRIVATE_KEY, orderId: order.orderHash, taskId: task._id, orderKey: order.id },
              }
            })
            await queue.addBulk(cancelData)
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
          console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
          console.log(RED + `Skipping counterbid since incoming bid ${task.contract.slug}:collection ${incomingBidAmount / Math.pow(10, decimal)} ${currencySymbol} is less than or equal to existing bid ${Number(job.data.offerPrice) / Math.pow(10, decimal)} ${currencySymbol} domain: opensea` + RESET);
          console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
          return
        } else {
          await job.updateData(bidData)
        }
      } else {
        await queue.add(
          OPENSEA_COLLECTION_BID_COUNTERBID,
          bidData,
          {
            jobId,
          },

        );
      }
      seenOrderHashes.delete(orderHash);

      console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(GREEN + `Counterbidding incoming offer of  ${bidDetails} ${similarCurrency ? '' : `(${extraDetails})`} for ${task.contract.slug} for ${Number(colletionOffer) / Math.pow(10, decimal)} ${currencySymbol} ON OPENSEA`.toUpperCase() + RESET);
      console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);

      if (orders.length > 0) {
        const cancelData = orders.map((order, index) => {
          return {
            name: CANCEL_OPENSEA_BID,
            data: { privateKey: WALLET_PRIVATE_KEY, orderId: order.orderHash, taskId: task._id, orderKey: order.id },
          }
        })
        await queue.addBulk(cancelData)
      }
    }

    seenOrderHashes.delete(orderHash);

  } catch (error) {
    console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
    console.log(RED + `Error processing opensea counterbid: ${error} ` + RESET);
    console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
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
    if (!currentTask?.outbidOptions?.counterbid) return
    const wallets = await Wallet.findById(task.walletId)
    const WALLET_PRIVATE_KEY = await decryptPrivateKey(wallets?.encryptedPrivateKey as string);
    const selectedTraits = transformNewTask(currentTask?.selectedTraits?.blur)
    const traitBid = currentTask?.bidType === "collection" && selectedTraits && Object.keys(selectedTraits)?.length > 0
    const blurOutbidMargin = currentTask?.outbidOptions?.blurOutbidMargin || 0.01

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
        console.log(GOLD + '---------------------------------------------------------------------------------' + RESET);
        console.log(GOLD + `incoming trait offer for ${task?.contract?.slug}: ${trait} for ${incomingPrice} BETH on blur`.toUpperCase() + RESET);
        console.log(GOLD + '---------------------------------------------------------------------------------' + RESET);

        const floor_price = await fetchBlurCollectionStats(task?.contract?.slug);
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
          const message = `incoming bid ${incomingPrice} + Blur Outbid Margin (${blurOutbidMargin}) BETH for ${task.contract.slug}: ${identifier} ${reason}. Skipping ...`.toUpperCase()
          await logBidError(task._id, "INVALID BID PRICE", message, "skipped", "blur");
          return
        } else if (+totalOffer > maxBidPriceEth) {
          const reason = `is greater than max bid price ${maxBidPriceEth} BETH`
          const message = `incoming bid ${incomingPrice} + Blur Outbid Margin (${blurOutbidMargin}) BETH for ${task.contract.slug}: ${identifier} ${reason}. Skipping ...`.toUpperCase()

          if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
            if (orders.length > 0) {
              const cancelData = orders.map((order, index) => {
                const payload = JSON.parse(order.orderHash)
                return {
                  name: CANCEL_BLUR_BID,
                  data: { privateKey: WALLET_PRIVATE_KEY, payload, taskId: task._id, orderKey: order.id }
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
                data: { privateKey: WALLET_PRIVATE_KEY, payload, taskId: task._id, orderKey: order.id }
              }
            })
            await queue.addBulk(cancelData)
          }
          return
        }

        const offers = orders?.map((order: PrismaOrder) => order?.offerPrice)
        const currentBidPrice = !offers?.length ? 0 : Math.max(...offers?.map((offer) => Number(offer)))
        const currentBidPriceEth = currentBidPrice / 1e18
        console.log(JSON.stringify(data));
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
          privateKey: WALLET_PRIVATE_KEY,
          contractAddress: task.contract.contractAddress,
          offerPrice: Number(offerPrice),
          slug: task.contract.slug,
          traits,
          expiry
        }

        if (orders.length > 0) {
          const cancelData = orders.map((order, index) => {
            const payload = JSON.parse(order.orderHash)
            return {
              name: CANCEL_BLUR_BID,
              data: { privateKey: WALLET_PRIVATE_KEY, payload, taskId: task._id, orderKey: order.id }
            }
          })
          await queue.addBulk(cancelData)
        }

        if (job) {
          if (incomingPrice <= Number(job?.data?.offerPrice) / 1e18) {
            console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
            console.log(RED + `Skipping counterbid since incoming bid ${task.contract.slug}:${JSON.stringify(incomingTrait)} ${incomingPrice} BETH is less than or equal to existing bid ${Number(job.data.offerPrice) / 1e18} BETH domain: blur` + RESET);
            console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
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

        seenBids.delete(bidHash)

        console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
        console.log(GREEN + `Counterbidding incoming blur offer of ${Number(incomingPrice)} BETH for ${task.contract.slug} ${JSON.stringify(incomingTrait)} for ${Number(offerPrice) / 1e18} BETH ON BLUR`.toUpperCase() + RESET);
        console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
      }

    } else {
      const collectionBid = task.bidType.toLowerCase() === "collection" && Object.keys(task?.selectedTraits?.blur || {}).length === 0
      if (!collectionBid) return

      const incomingBidDetails = incomingBid?.stats?.filter((item: any) => item?.criteriaType?.toLowerCase() === "collection") || incomingBid?.updates?.filter((item: any) => item?.criteriaType?.toLowerCase() === "collection")

      if (!incomingBidDetails || !incomingBidDetails?.length) return

      const incomingBestPrice = incomingBidDetails?.sort((a: any, b: any) => +b?.bestPrice - +a?.bestPrice)[0].bestPrice
      const incomingBidAmount = (incomingBidDetails as any)?.sort((a: any, b: any) => +b.price - +a.price)[0].price

      const bestPrice = +incomingBestPrice > 0 ? incomingBestPrice : +incomingBidAmount > 0 ? incomingBidAmount : "0"

      console.log(GOLD + '---------------------------------------------------------------------------------' + RESET);
      console.log(GOLD + `incoming collection offer for ${task?.contract?.slug} for ${bestPrice} BETH on blur`.toUpperCase() + RESET);
      console.log(GOLD + '---------------------------------------------------------------------------------' + RESET);

      const floor_price = await fetchBlurCollectionStats(task.contract.slug);
      const { maxBidPriceEth, minBidPriceEth } = await calculateBidPrice(task, Number(floor_price), "blur")

      const wallets = await Wallet.findById(task.walletId)
      if (!wallets) {
        console.log(RED + `Wallet not found for task ${task._id}` + RESET);
        return
      }
      const WALLET_PRIVATE_KEY = await decryptPrivateKey(wallets?.encryptedPrivateKey as string)

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
          await queue.addBulk(cancelData);
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
          await queue.addBulk(cancelData);
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

      const jobId = `counterbid-${task._id}-${task.contract.slug}-${BLUR.toLowerCase()}-collection`
      const job: Job = await queue.getJob(jobId)
      const bidCount = getIncrementedBidCount(BLUR, task.contract.slug, task._id)
      const bidData: IBlurBidData = {
        taskId: task._id,
        bidCount: bidCount,
        address: task.wallet,
        privateKey: WALLET_PRIVATE_KEY,
        contractAddress: task.contract.contractAddress,
        offerPrice: Number(offerPrice),
        slug: task.contract.slug,
        expiry
      }

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
        await queue.addBulk(cancelData);
      }

      if (job) {
        if (+incomingPrice <= +job?.data?.offerPrice) {
          console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
          console.log(RED + `Skipping counterbid since incoming bid ${task.contract.slug}:collection ${incomingPrice / 1e18} BETH is less than or equal to existing bid ${Number(job.data.offerPrice) / 1e18} BETH domain: blur` + RESET);
          console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
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

      console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(GREEN + `Counterbidding incoming blur collection offer of ${Number(incomingPrice) / 1e18} BETH for ${task.contract.slug} for ${(Number(offerPrice) / 1e18)} BETH ON BLUR`.toUpperCase() + RESET);
      console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
      seenBids.delete(bidHash)
    }
  } catch (error: any) {
    console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
    console.log(RED + `Error handling Blur counterbid: ${error?.message || error?.toString() || 'Unknown error'}` + RESET);
    if (error?.stack) {
      console.log(RED + `Stack trace: ${error.stack}` + RESET);
    }
    console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
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

async function processOpenseaScheduledBid(task: ITask) {
  try {
    if (!enabledMarketplaces.map((m) => m.toLowerCase()).includes("opensea")) return
    const currentTask = activeTasks.get(task._id);
    if (!currentTask || !currentTask.running || !currentTask.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("opensea")) return;

    if (!taskStatus[currentTask._id]) {
      taskStatus[currentTask._id] = {
        status: 'STARTED',
        runningPercentage: 0,
        waitingTime: 0,
        cancellationPercentage: 0,
        processingPercentage: 0,
        nextLoopTime: new Date()
      }
    }

    taskStatus[currentTask._id].status = 'STARTED'

    if (!currentTokenIds[currentTask._id]) {
      currentTokenIds[currentTask._id] = {
        opensea: [],
        magiceden: [],
      }
    }

    const filteredTasks = Object.fromEntries(
      Object.entries(currentTask?.selectedTraits?.opensea || {}).filter(([_, traits]) => traits.length > 0)
    );
    const selectedTraits = transformNewTask(filteredTasks)
    if (!currentTraits[currentTask._id]) {
      currentTraits[currentTask._id] = {
        opensea: selectedTraits,
        magiceden: {},
        blur: {}
      }
    }

    currentTraits[currentTask._id].opensea = selectedTraits

    const WALLET_ADDRESS: string = currentTask.wallet;
    taskStatus[currentTask._id].status = 'PROCESSING'
    const wallets = await Wallet.findById(currentTask.walletId)
    taskStatus[currentTask._id].processingPercentage = (1 / 4) * 100
    const WALLET_PRIVATE_KEY: string = await decryptPrivateKey(wallets?.encryptedPrivateKey as string)

    const collectionDetails = await getCollectionDetails(currentTask.contract.slug, currentTask.contract.contractAddress);
    taskStatus[currentTask._id].processingPercentage = (2 / 4) * 100
    const traitBid = selectedTraits && Object.keys(selectedTraits).length > 0

    if (!warningBids[currentTask._id]) {
      warningBids[currentTask._id] = {
        opensea: false,
        magiceden: false,
        blur: false
      }
    }

    const autoIds = currentTask.tokenIds
      .filter(id => id.toString().toLowerCase().startsWith('bot'))
      .map(id => {
        const matches = id.toString().match(/\d+/);
        return matches ? parseInt(matches[0]) : null;
      })
      .filter(id => id !== null);

    const amount = autoIds[0]

    // determine token traits
    const taskTokenIds = currentTask.tokenIds
    const bottomListings = autoIds ? await fetchOpenseaListings(currentTask._id, currentTask.contract.slug, amount) ?? [] : []
    const difference = amount !== undefined ? Math.max(0, amount - bottomListings.length) : 0

    let tokens: number[] = [...bottomListings, ...taskTokenIds.map((token) => +token)]
    let tokenIds: number[] = []

    if (currentTask.bidType === "token" && selectedTraits && Object.keys(selectedTraits).length > 0) {
      const attributes = transformSelectedTraitsToAttributes(selectedTraits)
      const expandedAttributes = expandAttributes(attributes)
      let traitTokens: number[] = []
      let skips = 0

      if (currentTask.traitsOperator.value === "or") {
        await pQueue.addAll(
          expandedAttributes.map((attribute) => async () => {
            const count = getTraitCount(currentTask.traits, attribute)
            const variable: CollectionItemsQueryVariables = {
              collectionSlug: currentTask.contract.slug,
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
            traitTokens.push(...items);
            const skipped = count - items.length
            skips += skipped
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
        traitTokens.push(...items);
      }

      tokens = [...tokens, ...traitTokens]
      tokenIds = [...new Set(tokens)];
      const duplicateCount = tokens.length - tokenIds.length

      if (!skipStats[currentTask._id]) {
        skipStats[currentTask._id] = {
          opensea: 0,
          magiceden: 0,
          blur: 0
        }
      }
      skipStats[currentTask._id].opensea += (skips + duplicateCount + difference)

      if (duplicateCount > 0) {
        await logBidError(currentTask._id, "DUPLICATE COUNT", `Duplicate count: ${duplicateCount} for ${currentTask.contract.slug} on OpenSea.`, "skipped", "opensea");
      }

    } else if (currentTask.bidType === "token") {
      tokenIds = [...new Set([...tokens])];
      const duplicateCount = tokens.length - tokenIds.length
      if (!skipStats[currentTask._id]) {
        skipStats[currentTask._id] = {
          opensea: 0,
          magiceden: 0,
          blur: 0
        }
      }
      skipStats[currentTask._id]['opensea'] += (difference + duplicateCount)
      await logBidError(currentTask._id, "DUPLICATE COUNT", `Duplicate count: ${duplicateCount} for ${currentTask.contract.slug} on OpenSea.`, "skipped", "opensea");
    }

    if (difference > 0) {
      await logBidError(
        currentTask._id,
        "MISSING TOKENS",
        `There are ${difference} fewer listed tokens than requested for ${currentTask.contract.slug} on OpenSea.`,
        "skipped",
        "opensea"
      );
    }

    const staleTokens = currentTokenIds[currentTask._id].opensea.filter(token => !tokenIds.includes(token)).map((item) => item.toString())
    const { currency } = await calculateBidPrice(currentTask, 0, "opensea")


    if (staleTokens.length > 0) {
      const openseaTokensToCancel = await prisma.order.findMany({
        where: {
          taskId: task._id,
          identifier: { in: staleTokens },
          marketplace: "opensea"
        }
      })

      if (openseaTokensToCancel.length > 0) {
        const cancelData = openseaTokensToCancel.map((order) => {
          return {
            name: CANCEL_OPENSEA_BID,
            data: { privateKey: WALLET_PRIVATE_KEY, orderId: order.orderHash, taskId: task._id, orderKey: order.id },
          };
        });
        await queue.addBulk(cancelData);
      }
    }

    currentTokenIds[currentTask._id].opensea = [...tokenIds]
    taskStatus[currentTask._id].processingPercentage = (3 / 4) * 100
    const tokenBid = currentTask.bidType === "token" && tokenIds.length > 0
    const traits = selectedTraits && Object.keys(selectedTraits).length > 0 ? transformOpenseaTraits(selectedTraits) : [];

    const approved = await approveMarketplace(SEAPORT, WALLET_PRIVATE_KEY, currency);
    taskStatus[currentTask._id].processingPercentage = (4 / 4) * 100
    // if (!approved) return

    if (traitBid && !collectionDetails.trait_offers_enabled && !tokenBid) {

      if (!warningMessage[currentTask._id]) {
        warningMessage[currentTask._id] = {
          opensea: `Trait bidding is not available for ${currentTask.contract.slug}`,
          magiceden: "",
          blur: ""
        }
      }
      console.log(RED + `Trait bidding is not available for ${currentTask.contract.slug} on OpenSea.`.toUpperCase() + RESET);
      warningBids[currentTask._id].opensea = true;
      warningMessage[currentTask._id].opensea = `Trait bidding is not available for ${currentTask.contract.slug}`;
      await logBidError(currentTask._id, "TRAIT BIDDING NOT AVAILABLE", `Trait bidding is not available for ${currentTask.contract.slug} on OpenSea.`, "warning", "opensea");
      return;
    }

    const creatorFees: IFee = collectionDetails.creator_fees.null !== undefined
      ? { null: collectionDetails.creator_fees.null }
      : Object.fromEntries(Object.entries(collectionDetails.creator_fees).map(([key, value]) => [key, Number(value)]));

    taskStatus[currentTask._id].status = 'SCHEDULED'
    const newTask = { ...currentTask, traits: {} } as ITask

    const collectionBid = currentTask.bidType.toLowerCase() === "collection" && Object.keys(currentTask.selectedTraits?.opensea || {}).length === 0

    if (tokenBid) {
      // Get the array of OpenSea token IDs for this task, defaulting to empty array if undefined
      const tokens = currentTokenIds[task._id].opensea
        .filter(token => !isNaN(Number(token)) && String(token).toLowerCase()
          .indexOf('bot') === -1)
        ?? []
      const jobs = tokens
        .map((token) => ({
          name: OPENSEA_TOKEN_BID,
          data: {
            _id: currentTask._id,
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
    } else if (traitBid && collectionDetails.trait_offers_enabled) {
      const traitJobs = traits.map((trait) => ({
        name: OPENSEA_TRAIT_BID,
        data: {
          _id: currentTask._id,
          address: WALLET_ADDRESS,
          privateKey: WALLET_PRIVATE_KEY,
          slug: currentTask.contract.slug,
          contractAddress: currentTask.contract.contractAddress,
          creatorFees,
          enforceCreatorFee: collectionDetails.enforceCreatorFee,
          trait: JSON.stringify(trait),
          outbidOptions: currentTask.outbidOptions,
          task: newTask,
        },
        opts: { priority: TRAIT_BID_PRIORITY.OPENSEA } as JobsOptions
      })) as Job[]

      await processBulkJobs(traitJobs, true);

      console.log(`ADDED ${traitJobs.length} ${currentTask.contract.slug} OPENSEA TRAIT BID JOBS TO QUEUE`);
    } else if (collectionBid) {
      const jobId = `${currentTask._id}-opensea-${currentTask.contract.slug}-collection`
      const parentJobId = `${currentTask._id}-opensea`.toLowerCase()

      parentJobs.set(parentJobId, {
        parentId: parentJobId,
        totalJobs: 1,
        completedJobs: 0,
        failedJobs: 0,
        startTime: Date.now(),
        taskId: currentTask._id,
        jobName: OPENSEA_COLLECTION_BID,
      })

      await queue.add(
        OPENSEA_COLLECTION_BID,
        { task: newTask, collectionDetails, currency },
        { priority: COLLECTION_BID_PRIORITY.OPENSEA, jobId } as JobsOptions
      );

      if (currentTask && currentTask.running) taskStatus[currentTask._id].status = 'QUEUED'
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
  try {
    if (!enabledMarketplaces.length) return
    const { task, collectionDetails } = data
    const currentTask = activeTasks.get(task._id)
    if (!currentTask || !currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("opensea") || !enabledMarketplaces
      .map((m) => m.toLowerCase())
      .includes("opensea")) return;

    const wallets = await Wallet.findById(task.walletId)
    const privateKey: string = await decryptPrivateKey(wallets?.encryptedPrivateKey as string)

    const floor_price = await getOpenseaFloor(currentTask.contract.slug);
    if (!floor_price) return

    if (!floorPrices[currentTask._id]) {
      floorPrices[currentTask._id] = {
        opensea: floor_price as number,
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

    floorPrices[currentTask._id].opensea = floor_price as number;

    const { offerPriceEth, maxBidPriceEth, minBidPriceEth, currencySymbol, currency, decimal } = await calculateBidPrice(currentTask, floor_price as number, "opensea")
    const stopBid = await stopOption(currentTask, 'opensea', Number(data), false)
    const expiry = getExpiry(currentTask.bidDuration)

    if (stopBid) return

    const usdcBid = currency.toLowerCase() === USDC.toLowerCase()
    let offerPrice = BigInt(Math.ceil(roundToValidPrecision(offerPriceEth) * Math.pow(10, decimal)));

    let colletionOffer = BigInt(offerPrice)
    const orders = await prisma.order.findMany({
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
      await queue.addBulk(cancelData)
    }

    const createdAt = new Date(order?.createdAt).getTime()
    const expiredAt = new Date(order?.expiresAt).getTime()
    const orderExpiration = order ? Math.round((expiredAt - createdAt) / 1000) : expiry

    if (!currentTask.outbidOptions.outbid) {
      if (ttl >= MIN_BID_DURATION && currentPrice === Number(colletionOffer) && Math.abs(orderExpiration - expiry) < 30) return;
    }
    else {
      const walletsArr = await Wallet.find({})
      const [topOffer, secondOffer] = (await fetchOpenseaOffers('COLLECTION', currentTask.contract.slug, currentTask.contract.contractAddress, {}) as unknown) as OpenseaOfferData[]
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
            await queue.addBulk(cancelData)
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
      await queue.addBulk(cancelData)
    }

    const creatorFees: IFee = collectionDetails.creator_fees.null !== undefined
      ? { null: collectionDetails.creator_fees.null }
      : Object.fromEntries(Object.entries(collectionDetails.creator_fees).map(([key, value]) => [key, Number(value)]));


    const offerEth = Number(colletionOffer) / Math.pow(10, decimal);

    // Check if offer exceeds floor price (only for WETH)
    if (currencySymbol.toLowerCase() === 'weth' && offerEth > floor_price) {

      warningBids[task._id] = { opensea: true, magiceden: true, blur: true };

      const message = `⚠️ Offer price ${offerEth} ${currencySymbol} for ${task.contract.slug} collection bid exceeds floor price ${floor_price} ${currencySymbol} FOR OPENSEA. Stopping bid...`.toUpperCase();
      await logBidError(task._id, "OFFER EXCEEDS FLOOR PRICE", message, "warning", "opensea");
      console.log(YELLOW + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(YELLOW + message + RESET);
      console.log(YELLOW + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      return;
    }

    if (currentTask.outbidOptions.outbid && offerEth > maxBidPriceEth) {
      skipStats[task._id]['opensea']++;
      const message = `❌ Offer price ${offerEth} ${currencySymbol} for ${task.contract.slug} collection bid exceeds max bid price ${maxBidPriceEth} ${currencySymbol} FOR OPENSEA.Skipping ...`.toUpperCase()
      await logBidError(task._id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "opensea");
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
          await queue.addBulk(cancelData)
        }
      }
      return;
    } else if (offerEth < minBidPriceEth) {
      colletionOffer = BigInt(Math.round(minBidPriceEth * Math.pow(10, decimal)));
    }

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
  } catch (error: any) {
    if (!errorStats[data.task._id]) {
      errorStats[data.task._id] = {
        magiceden: 0,
        opensea: 0,
        blur: 0
      }
    }
    await logBidError(data.task._id, "OPENSEA COLLECTION BID ERROR", `Error processing OpenSea collection bid for task: ${data.task._id}: ${error?.response?.data?.errors?.[0] || error?.response?.data || error?.message || String(error)} `, "error", "opensea");
    errorStats[data.task._id]['opensea']++
  }
}

async function processBlurScheduledBid(task: ITask) {
  try {
    const currentTask = activeTasks.get(task._id)
    if (!currentTask || !currentTask?.running || !currentTask.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("blur") || !enabledMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("blur")) return

    const { currencySymbol } = await calculateBidPrice(currentTask, 0, "blur")

    if (currencySymbol.toLowerCase() !== "weth") {
      if (!warningMessage[currentTask._id]) {
        warningMessage[currentTask._id] = {
          blur: "",
          magiceden: "",
          opensea: ""
        }
      }
      if (!warningBids[currentTask._id]) {
        warningBids[currentTask._id] = {
          blur: false,
          magiceden: false,
          opensea: false
        }
      }

      if (!completedTasks[currentTask._id]) {
        completedTasks[currentTask._id] = {
          blur: false,
          magiceden: false,
          opensea: false
        }
      }
      completedTasks[currentTask._id].blur = true;
      warningBids[currentTask._id].blur = true;
      warningMessage[currentTask._id].blur = `${currencySymbol} is not supported on Blur`;
      return
    }

    const filteredTasks = Object.fromEntries(
      Object.entries(currentTask?.selectedTraits?.blur || {}).filter(([_, traits]) => traits.length > 0)
    );

    const tokenBid = currentTask.bidType === "token"
    if (tokenBid) {
      if (!warningMessage[currentTask._id]) {
        warningMessage[currentTask._id] = {
          blur: "",
          magiceden: "",
          opensea: ""
        }
      }
      warningBids[currentTask._id].blur = true;
      warningMessage[currentTask._id].blur = `Token Bid is not supported on Blur`;
      return
    }

    const expiry = getExpiry(currentTask.bidDuration)

    const WALLET_ADDRESS: string = currentTask.wallet;
    const wallets = await Wallet.findById(task.walletId)
    const WALLET_PRIVATE_KEY: string = await decryptPrivateKey(wallets?.encryptedPrivateKey as string)
    const selectedTraits = transformNewTask(filteredTasks)

    if (!currentTraits[currentTask._id]) {
      currentTraits[currentTask._id] = {
        opensea: {},
        magiceden: {},
        blur: selectedTraits
      }
    }

    currentTraits[currentTask._id].blur = selectedTraits
    const traitBid = selectedTraits && Object.keys(selectedTraits).length > 0

    if (!warningBids[currentTask._id]) {
      warningBids[currentTask._id] = {
        opensea: false,
        magiceden: false,
        blur: false
      }
    }
    warningBids[currentTask._id].blur = false;
    const contractAddress = currentTask.contract.contractAddress
    if (!taskStatus[currentTask._id]) {
      taskStatus[currentTask._id] = {
        status: 'SCHEDULED',
        runningPercentage: 0,
        waitingTime: 0,
        cancellationPercentage: 0,
        processingPercentage: 0,
        nextLoopTime: new Date()
      }
    }
    taskStatus[currentTask._id].status = 'SCHEDULED'

    const newTask = { ...currentTask, traits: {} } as ITask

    if (traitBid) {
      const traits = transformBlurTraits(selectedTraits)
      const traitJobs = traits.map((trait) => ({
        name: BLUR_TRAIT_BID,
        data: {
          _id: currentTask._id,
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

    } else if (currentTask.bidType.toLowerCase() === "collection" && !traitBid) {
      const jobId = `${currentTask._id}-blur-${currentTask.contract.slug}-collection`
      const parentJobId = `${currentTask._id}-blur`.toLowerCase()

      parentJobs.set(parentJobId, {
        parentId: parentJobId,
        totalJobs: 1,
        completedJobs: 0,
        failedJobs: 0,
        startTime: Date.now(),
        taskId: currentTask._id,
        jobName: BLUR_COLLECTION_BID
      })

      await queue.add(
        BLUR_COLLECTION_BID,
        { task: newTask },
        { priority: COLLECTION_BID_PRIORITY.BLUR, jobId } as JobsOptions
      );

      if (currentTask && currentTask.running) taskStatus[currentTask._id].status = 'QUEUED'
    }
  } catch (error) {
    console.error(RED + `Error processing Blur scheduled bid for task: ${task._id}` + RESET, error);
  }
}


async function processBlurCollectionBid(data: { task: ITask }) {
  try {
    if (!enabledMarketplaces.length) return
    const { task } = data
    const currentTask = activeTasks.get(task._id)
    if (!currentTask || !currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("blur") || !enabledMarketplaces.map((market) => market.toLowerCase()).includes("blur")) return;

    const wallets = await Wallet.findById(currentTask.walletId)
    const privateKey: string = await decryptPrivateKey(wallets?.encryptedPrivateKey as string)

    const floor_price: number = Number(await fetchBlurCollectionStats(currentTask.contract.slug));
    if (!floor_price) return

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
        await queue.addBulk(cancelData);
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
      if (!warningBids[task._id]) {
        warningBids[task._id] = {
          opensea: false,
          magiceden: false,
          blur: false
        };
      }
      warningBids[task._id] = { opensea: true, magiceden: true, blur: true };

      const message = `⚠️ Offer price ${offerEth} WETH for ${task.contract.slug} collection bid exceeds floor price ${floor_price} WETH FOR BLUR. Stopping bid...`.toUpperCase();
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
      await queue.addBulk(cancelData);
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
            await queue.addBulk(cancelData);
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
      await queue.addBulk(cancelData);
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
          await queue.addBulk(cancelData);
        }
      }
      return;
    } else if (collectionOfferEth < minBidPriceEth) {
      colletionOffer = BigInt(Math.round(minBidPriceEth * 1e18));
    }
    await bidOnBlur(task._id, bidCount, task.wallet, privateKey, task.contract.contractAddress, colletionOffer, task.contract.slug, expiry);
  } catch (error: any) {
    console.log(error);
    if (!errorStats[data?.task?._id]) {
      errorStats[data?.task?._id] = {
        magiceden: 0,
        opensea: 0,
        blur: 0
      }
    }
    await logBidError(data.task._id, "BLUR COLLECTION BID ERROR", `Error processing Blur collection bid for task: ${data.task._id}: ${error?.response?.data?.message || error?.response?.data || error?.message || String(error)} `, "error", "blur");
    errorStats[data?.task?._id]['blur']++
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
  try {
    if (!enabledMarketplaces.length) return
    if (!enabledMarketplaces.map((m) => m.toLowerCase()).includes("opensea")) return

    const { address, privateKey, slug, creatorFees, enforceCreatorFee, trait, contractAddress, _id } = data

    const selectedTraits = currentTraits[_id]?.opensea
    const traits = selectedTraits && Object.keys(selectedTraits).length > 0 ? transformOpenseaTraits(selectedTraits) : [];

    const parsedTrait = JSON.parse(trait);
    const traitsArray: { type: string; value: string }[] = traits;

    const traitExists = traitsArray.some(
      (t) => t.type === parsedTrait.type && t.value === parsedTrait.value
    );

    if (!traitExists) return

    const currentTask = activeTasks.get(_id)
    if (!currentTask || !currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("opensea")) return;

    const floor_price = await getOpenseaFloor(slug);

    if (!floor_price) return

    if (!floorPrices[_id]) {
      floorPrices[_id] = {
        opensea: floor_price as number,
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
    floorPrices[_id].opensea = floor_price as number;
    const { maxBidPriceEth, offerPriceEth, minBidPriceEth, currencySymbol, currency, decimal } = await calculateBidPrice(currentTask, floor_price as number, "opensea")
    const expiry = getExpiry(currentTask.bidDuration)
    const { type, value } = JSON.parse(trait);
    const identifier = `${type}:${value}`
    const traitFloor = currentTask?.stopOptions?.traitPrices && currentTask?.stopOptions?.triggerStopOptions && identifier in currentTask?.stopOptions?.traitPrices ? await openseaTraitFloor(slug, { traitType: type, values: [value] }) : undefined
    const stopBid = await stopOption(currentTask, 'opensea', Number(floor_price), false, traitFloor, identifier)
    const outbidOptions = currentTask.outbidOptions
    const orders = await prisma.order.findMany({
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

    if (stopBid) {
      if (orders.length > 0) {
        const cancelData = orders.map((order, index) => {
          return {
            name: CANCEL_OPENSEA_BID,
            data: { privateKey, orderId: order.orderHash, taskId: _id, orderKey: order.id },
          }
        })
        await queue.addBulk(cancelData)
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
      await queue.addBulk(cancelData)
    }

    const createdAt = new Date(order?.createdAt).getTime()
    const expiredAt = new Date(order?.expiresAt).getTime()
    const orderExpiration = order ? Math.round((expiredAt - createdAt) / 1000) : expiry

    if (!outbidOptions.outbid) {
      if (ttl >= MIN_BID_DURATION && currentPrice === Number(traitOffer) && Math.abs(orderExpiration - expiry) < 30) return;
    }

    else {
      const [topOffer, secondOffer] = await fetchOpenseaOffers('TRAIT', slug, contractAddress, JSON.parse(trait)) as unknown as OpenseaOfferData[]
      const walletsArr = await Wallet.find({})
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
            await queue.addBulk(cancelData)
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
      await queue.addBulk(cancelData)
    }

    const offerEth = Number(traitOffer) / Math.pow(10, decimal);

    if (outbidOptions.outbid && offerEth > maxBidPriceEth) {
      skipStats[_id]['opensea']++;
      const message = `❌ Offer price ${offerEth} ${currencySymbol} for ${slug} trait ${trait} bid exceeds max bid price ${maxBidPriceEth} ${currencySymbol} FOR OPENSEA.Skipping ...`.toUpperCase()
      await logBidError(_id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "opensea");
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
          await queue.addBulk(cancelData)
        }
      }

      return;
    } else if (offerEth < minBidPriceEth) {
      traitOffer = BigInt(Math.round(minBidPriceEth * Math.pow(10, decimal)));
    }

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
  } catch (error: any) {
    console.log(error?.response?.data);
    if (!errorStats[data.task._id]) {
      errorStats[data.task._id] = {
        magiceden: 0,
        opensea: 0,
        blur: 0
      }
    }
    await logBidError(data.task._id, "OPENSEA TRAIT BID ERROR", `Error bidding on trait ${data.trait} in collection ${data.slug}: ${error?.response?.data?.errors?.[0] || error?.response?.data || error?.message || String(error)} `, "error", "opensea");
    errorStats[data.task._id]['opensea']++
  }
}

async function processOpenseaTokenBid(data: IProcessOpenseaTokenBidData) {
  try {
    if (!enabledMarketplaces.length) return
    if (!enabledMarketplaces.map((m) => m.toLowerCase()).includes("opensea")) return
    const { address, privateKey, slug, creatorFees, enforceCreatorFee, asset, _id } = data
    if (!currentTokenIds[_id]?.opensea?.includes(+asset.tokenId)) return

    const currentTask = activeTasks.get(_id)
    if (!currentTask || !currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("opensea")) return;
    const floor_price = await getOpenseaFloor(slug);
    const { maxBidPriceEth, offerPriceEth, minBidPriceEth, currencySymbol, currency, decimal } = await calculateBidPrice(currentTask, floor_price as number, "opensea")

    const outbidOptions = currentTask.outbidOptions
    if (!floor_price) return

    if (!floorPrices[_id]) {
      floorPrices[_id] = {
        opensea: floor_price as number,
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

    floorPrices[_id].opensea = floor_price as number;

    const expiry = getExpiry(currentTask.bidDuration)
    const usdcBid = currency.toLowerCase() === USDC.toLowerCase()


    let tokenOffer = BigInt(Math.round(offerPriceEth * Math.pow(10, decimal)))
    const orders = await prisma.order.findMany({
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

    const stopBid = await stopOption(currentTask, 'opensea', Number(floor_price), false)
    if (stopBid) {
      if (orders.length > 0) {
        const cancelData = orders.map((order, index) => {
          return {
            name: CANCEL_OPENSEA_BID,
            data: { privateKey, orderId: order.orderHash, taskId: _id, orderKey: order.id }
          }
        })
        await queue.addBulk(cancelData)
      }
      return
    };
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
      await queue.addBulk(cancelData)
    }


    const createdAt = new Date(order?.createdAt).getTime()
    const expiredAt = new Date(order?.expiresAt).getTime()
    const orderExpiration = order ? Math.round((expiredAt - createdAt) / 1000) : expiry


    if (!outbidOptions.outbid) {
      if (ttl >= MIN_BID_DURATION && currentPrice === Number(tokenOffer) && Math.abs(orderExpiration - expiry) < 30) return;
    } else {
      const walletsArr = await Wallet.find({})
      const highestBid = await fetchItemOffers(asset.contractAddress, asset.tokenId.toString());
      const [topOffer, secondOffer] = highestBid

      console.log({ COLLECTION: slug, MARKETPLACE: OPENSEA, TYPE: 'TOKEN', tokenId: asset.tokenId, topOffer, secondOffer });

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
            await queue.addBulk(cancelData)
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
    if (orders.length > 0) {
      const cancelData = orders.map((order, index) => {
        return {
          name: CANCEL_OPENSEA_BID,
          data: { privateKey, orderId: order.orderHash, taskId: _id, orderKey: order.id }
        }
      })
      await queue.addBulk(cancelData)
    }

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
          await queue.addBulk(cancelData)
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

  } catch (error: any) {
    if (!errorStats[data.task._id]) {
      errorStats[data.task._id] = {
        magiceden: 0,
        opensea: 0,
        blur: 0
      }
    }
    await logBidError(data.task._id, "OPENSEA TOKEN BID ERROR", `Error bidding on token ${data.asset.tokenId} in collection ${data.slug}: ${error?.response?.data?.errors?.[0] || error?.response?.data || error?.message || String(error)} `, "error", "opensea");
    errorStats[data.task._id]['opensea']++
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
    if (!warningBids[taskId]) {
      warningBids[taskId] = {
        opensea: false,
        magiceden: false,
        blur: false
      };
    }
    warningBids[taskId] = { opensea: true, magiceden: true, blur: true };

    const message = `⚠️ Counter bid offer price ${offerEth} ${currencySymbol} for ${slug} collection exceeds floor price ${floor_price} ${currencySymbol} FOR OPENSEA. Stopping bid...`.toUpperCase();
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
    await logBidError(data.taskId, "OPENSEA COLLECTION COUNTER BID ERROR", `Error processing OpenSea collection counter bid for task: ${data.slug}: ${error?.response?.data?.errors?.[0] || error?.response?.data || error?.message || String(error)} `, "error", "opensea");
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
      if (!warningBids[_id]) {
        warningBids[_id] = {
          opensea: false,
          magiceden: false,
          blur: false
        };
      }
      warningBids[_id] = { opensea: true, magiceden: true, blur: true };

      const message = `⚠️ Counter bid offer price ${offerEth} ${currencySymbol} for ${slug} collection exceeds floor price ${floor_price} ${currencySymbol} FOR MAGICEDEN. Stopping bid...`.toUpperCase();
      await logBidError(_id, "COUNTER BID OFFER EXCEEDS FLOOR PRICE", message, "warning", "magiceden");
      console.log(YELLOW + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(YELLOW + message + RESET);
      console.log(YELLOW + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      return;
    }

    await bidOnMagiceden(_id, bidCount, address, contractAddress, quantity, offerPrice.toString(), privateKey, slug, expiration, currency);

  } catch (error: any) {
    await logBidError(data._id, "MAGICEDEN COLLECTION COUNTER BID ERROR", `Error processing MagicEden collection counter bid for task: ${data.slug}: ${error?.response?.data?.message || error?.response?.data || error?.message || String(error)} `, "error", "magiceden");
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
    await logBidError(data.taskId, "MAGICEDEN TRAIT COUNTER BID ERROR", `Error processing MagicEden trait counter bid for task: ${data.slug}: ${error?.response?.data?.message || error?.response?.data || error?.message || String(error)} `, "error", "magiceden");
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
    if (!warningBids[taskId]) {
      warningBids[taskId] = {
        opensea: false,
        magiceden: false,
        blur: false
      };
    }
    warningBids[taskId] = { opensea: true, magiceden: true, blur: true };

    const message = `⚠️ Counter bid offer price ${offerEth} WETH for ${slug} collection exceeds floor price ${floor_price} WETH FOR BLUR. Stopping bid...`.toUpperCase();
    await logBidError(taskId, "COUNTER BID OFFER EXCEEDS FLOOR PRICE", message, "warning", "blur");
    console.log(YELLOW + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
    console.log(YELLOW + message + RESET);
    console.log(YELLOW + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
    return;
  }

  try {
    await bidOnBlur(taskId, bidCount, address, privateKey, contractAddress, BigInt(offerPrice), slug, expiry);
  } catch (error: any) {
    await logBidError(taskId, "BLUR COLLECTION COUNTER BID ERROR", `Error processing Blur collection counter bid for task: ${slug}: ${error?.response?.data?.message || error?.response?.data || error?.message || String(error)} `, "error", "blur");
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
    await logBidError(data.taskId, "BLUR TRAIT COUNTER BID ERROR", `Error processing Blur trait counter bid for task: ${data.slug}: ${error?.response?.data?.message || error?.response?.data || error?.message || String(error)} `, "error", "blur");
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
    await logBidError(data.task._id, "OPENSEA TOKEN COUNTER BID ERROR", `Error bidding on token ${data.asset.tokenId} in collection ${data.slug}: ${error?.response?.data?.errors?.[0] || error?.response?.data || error?.message || String(error)} `, "error", "opensea");
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
    await logBidError(data._id, "OPENSEA TRAIT COUNTER BID ERROR", `Error bidding on trait ${data.trait} in collection ${data.slug}: ${error?.response?.data?.errors?.[0] || error?.response?.data || error?.message || String(error)} `, "error", "opensea");
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
    await logBidError(data._id, "MAGICEDEN TOKEN COUNTER BID ERROR", `Error bidding on token ${data.tokenId} in collection ${data.slug}: ${error?.response?.data?.message || error?.response?.data || error?.message || String(error)} `, "error", "magiceden");
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
  if (!enabledMarketplaces.length) return
  const { address, privateKey, contractAddress, slug, trait, _id, task } = data;
  const selectedTraits = currentTraits[_id]?.blur
  const traits = transformBlurTraits(selectedTraits)

  const parsedTrait = JSON.parse(trait);
  const traitKey = Object.keys(parsedTrait)[0];
  const traitValue = parsedTrait[traitKey];
  const traitExists = traits.some(t => t[traitKey] === traitValue);

  if (!traitExists) return;

  const currentTask = activeTasks.get(_id)

  if (!currentTask || !currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("blur") || !enabledMarketplaces.map((m) => m.toLowerCase()).includes("blur")) return
  const outbidOptions = currentTask.outbidOptions
  try {
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
        await queue.addBulk(cancelData);
      }
      return

    }
    if (!floor_price) return

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
      await queue.addBulk(cancelData);
    }

    const createdAt = new Date(order?.createdAt).getTime()
    const expiredAt = new Date(order?.expiresAt).getTime()
    const orderExpiration = order ? Math.round((expiredAt - createdAt) / 1000) : expiry

    if (!outbidOptions.outbid) {
      if (ttl >= MIN_BID_DURATION && currentPrice === Number(traitOffer) && Math.abs(orderExpiration - expiry) < 30) return;
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
      const secondBidder = secondOffer.bidderAddressesSample || []
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
            await queue.addBulk(cancelData);
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
      await queue.addBulk(cancelData);
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
          await queue.addBulk(cancelData);
        }
      }
      return;
    } else if (offerEth < minBidPriceEth) {
      traitOffer = BigInt(Math.round(minBidPriceEth * 1e18));
    }
    await bidOnBlur(_id, bidCount, address, privateKey, contractAddress, traitOffer, slug, expiry, trait);
  } catch (error: any) {
    if (!errorStats[data?.task?._id]) {
      errorStats[data?.task?._id] = {
        magiceden: 0,
        opensea: 0,
        blur: 0
      }
    }
    errorStats[data?.task?._id]['blur']++
    await logBidError(data.task._id, "BLUR TRAIT BID ERROR", `Error bidding on trait ${data.trait} in collection ${data.slug}: ${error?.response?.data?.message || error?.response?.data || error?.message || String(error)} `, "error", "blur");
  }
}

async function processMagicedenScheduledBid(task: ITask) {
  try {
    if (!enabledMarketplaces.map((m) => m.toLowerCase()).includes("magiceden")) return
    const currentTask = activeTasks.get(task._id);
    if (!currentTask || !currentTask?.running || !currentTask?.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("magiceden")) return

    if (!taskStatus[currentTask._id]) {
      taskStatus[currentTask._id] = {
        status: 'STARTED',
        runningPercentage: 0,
        waitingTime: 0,
        cancellationPercentage: 0,
        processingPercentage: 0,
        nextLoopTime: new Date()
      }
    }

    taskStatus[currentTask._id].status = 'STARTED'

    if (!currentTokenIds[currentTask._id]) {
      currentTokenIds[currentTask._id] = {
        opensea: [],
        magiceden: [],
      }
    }

    const filteredTasks = Object.fromEntries(
      Object.entries(currentTask?.selectedTraits?.magiceden || {}).filter(([_, traits]) => traits.length > 0)
    );
    const selectedTraits = transformNewTask(filteredTasks)

    if (!currentTraits[currentTask._id]) {
      currentTraits[currentTask._id] = {
        opensea: {},
        magiceden: selectedTraits,
        blur: {}
      }
    }

    currentTraits[currentTask._id].magiceden = selectedTraits

    taskStatus[currentTask._id].status = 'PROCESSING'
    const WALLET_ADDRESS: string = currentTask.wallet
    const wallets = await Wallet.findById(currentTask.walletId)
    taskStatus[currentTask._id].processingPercentage = (1 / 4) * 100
    const WALLET_PRIVATE_KEY: string = await decryptPrivateKey(wallets?.encryptedPrivateKey as string)
    const traitBid = selectedTraits && Object.keys(selectedTraits).length > 0

    const contractAddress = currentTask.contract.contractAddress
    const expiration = getExpiry(currentTask.bidDuration)

    if (!warningBids[currentTask._id]) {
      warningBids[currentTask._id] = {
        opensea: false,
        magiceden: false,
        blur: false
      }
    }

    const autoIds = currentTask.tokenIds
      .map(id => id.toString().trim()) // Remove whitespace
      .filter(id => id.toString().toLowerCase().startsWith('bot'))
      .map(id => {
        const matches = id.toString().match(/\d+/);
        return matches ? parseInt(matches[0]) : null;
      })
      .filter(id => id !== null);

    const amount = autoIds[0]
    const approved = await approveMarketplace(MAGICEDEN_MARKETPLACE, WALLET_PRIVATE_KEY);
    taskStatus[currentTask._id].processingPercentage = (2 / 4) * 100
    // if (!approved) return

    const taskTokenIds = currentTask.tokenIds
    let tokenIds: number[] = []
    const bottomListings = amount ? await fetchMagicEdenTokens(currentTask._id, currentTask.contract.contractAddress, amount) ?? [] : []
    const difference = amount !== undefined ? Math.max(0, amount - bottomListings.length) : 0
    taskStatus[currentTask._id].processingPercentage = (3 / 4) * 100

    let tokens: number[] = [...bottomListings, ...taskTokenIds.map((token) => +token)]

    if (currentTask.bidType === "token" && selectedTraits && Object.keys(selectedTraits).length > 0) {

      const filteredTasks = Object.fromEntries(
        Object.entries(currentTask?.selectedTraits?.opensea || {}).filter(([_, traits]) => traits.length > 0)
      );
      const selectedTraits = transformNewTask(filteredTasks)
      const attributes = transformSelectedTraitsToAttributes(selectedTraits)
      const expandedAttributes = expandAttributes(attributes)
      let traitTokens: number[] = []
      let skips = 0

      if (currentTask.traitsOperator.value === "or") {

        await pQueue.addAll(
          expandedAttributes.map((attribute) => async () => {
            const count = getTraitCount(currentTask.traits, attribute)
            const variable: CollectionItemsQueryVariables = {
              collectionSlug: currentTask.contract.slug,
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
            tokens.push(...items);
            const skipped = count - items.length
            skips += skipped
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
        tokens.push(...items);
      }
      tokens = [...tokens, ...traitTokens];
      tokenIds = [...new Set(tokens)];
      const duplicateCount = tokens.length - tokenIds.length

      if (!skipStats[currentTask._id]) {
        skipStats[currentTask._id] = {
          opensea: 0,
          magiceden: 0,
          blur: 0
        }
      }

      skipStats[currentTask._id].magiceden += (skips + duplicateCount + difference)
      if (duplicateCount > 0) {
        await logBidError(currentTask._id, "DUPLICATE COUNT", `Duplicate count: ${duplicateCount} for ${currentTask.contract.slug} on MagicEden.`, "skipped", "magiceden");
      }

    } else if (currentTask.bidType === "token") {
      tokenIds = [...new Set([...tokens])];
      const duplicateCount = tokens.length - tokenIds.length

      if (!skipStats[currentTask._id]) {
        skipStats[currentTask._id] = {
          opensea: 0,
          magiceden: 0,
          blur: 0
        }
      }
      skipStats[currentTask._id]['magiceden'] += (difference + duplicateCount)
      await logBidError(currentTask._id, "DUPLICATE COUNT", `Duplicate count: ${duplicateCount} for ${currentTask.contract.slug} on MagicEden.`, "skipped", "magiceden");
    }

    if (difference > 0) {
      await logBidError(
        currentTask._id,
        "MISSING TOKENS",
        `There are ${difference} fewer listed tokens than requested for ${currentTask.contract.slug} on OpenSea.`,
        "skipped",
        "magiceden"
      );
    }
    const staleTokens = currentTokenIds[currentTask._id].magiceden.filter(token => !tokenIds.includes(token)).map((item) => item.toString())

    if (staleTokens.length > 0) {
      const magicedenTokensToCancel = await prisma.order.findMany({
        where: {
          taskId: task._id,
          identifier: { in: staleTokens },
          marketplace: "magiceden"
        }
      })

      for (let i = 0; i < magicedenTokensToCancel.length; i += 500) {
        const batchOrders = magicedenTokensToCancel.slice(i, i + 500);
        const extractedOrderIds = await extractMagicedenOrderHash(batchOrders)
        const orderKeys = batchOrders.map((order: PrismaOrder) => order.id)
        await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey: WALLET_PRIVATE_KEY, orderKeys, taskId: task._id });
      }
    }

    currentTokenIds[currentTask._id].magiceden = [...tokenIds]
    taskStatus[currentTask._id].processingPercentage = (4 / 4) * 100
    const tokenBid = currentTask.bidType === "token" && tokenIds.length > 0
    const newTask = { ...currentTask, traits: {} } as ITask
    taskStatus[currentTask._id].status = 'SCHEDULED'

    const collectionBid = currentTask.bidType.toLowerCase() === "collection" && Object.keys(currentTask?.selectedTraits?.magiceden || {}).length === 0

    if (tokenBid) {
      const tokens = currentTokenIds[task._id].magiceden.filter(token => !isNaN(Number(token)))
      const jobs = tokens
        .map((token) => ({
          name: MAGICEDEN_TOKEN_BID,
          data: {
            _id: currentTask._id,
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
    else if (traitBid) {
      const traits = Object.entries(selectedTraits).flatMap(([key, values]) =>
        values.map(value => ({ attributeKey: key, attributeValue: value })))
      const traitJobs = traits.map((trait) => ({
        name: MAGICEDEN_TRAIT_BID,
        data: {
          _id: currentTask._id,
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
    else if (collectionBid) {
      const jobId = `${currentTask._id}-magiceden-${currentTask.contract.slug}-collection`
      const parentJobId = `${currentTask._id}-magiceden`.toLowerCase()

      parentJobs.set(parentJobId, {
        parentId: parentJobId,
        totalJobs: 1,
        completedJobs: 0,
        failedJobs: 0,
        startTime: Date.now(),
        taskId: currentTask._id,
        jobName: MAGICEDEN_COLLECTION_BID
      })

      await queue.add(
        MAGICEDEN_COLLECTION_BID,
        { task: newTask },
        { priority: COLLECTION_BID_PRIORITY.MAGICEDEN, jobId } as JobsOptions
      );
      if (currentTask && currentTask.running) taskStatus[currentTask._id].status = 'QUEUED'
    }
  } catch (error) {
    console.error(RED + `Error processing MagicEden scheduled bid for task: ${task._id} ` + RESET, error);
  }
}

async function processMagicedenCollectionBid(data: { task: ITask }) {
  try {
    if (!enabledMarketplaces.length) return
    const { task } = data
    const currentTask = activeTasks.get(task._id)
    if (!currentTask || !currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("magiceden") || !enabledMarketplaces.map((m) => m.toLowerCase()).includes("magiceden")) return;
    const expiration = getExpiry(currentTask.bidDuration)
    const wallets = await Wallet.findById(task.walletId)
    const privateKey: string = await decryptPrivateKey(wallets?.encryptedPrivateKey as string)
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
        await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey, orderKeys: orderKeys, taskId: task._id });
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
      await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey, orderKeys: orderIds, taskId: task._id });
    }

    const createdAt = new Date(order?.createdAt).getTime()
    const expiredAt = new Date(order?.expiresAt).getTime()
    const orderExpiration = order ? Math.round((expiredAt - createdAt) / 1000) : expiration

    if (!task.outbidOptions.outbid) {
      if (ttl >= MIN_BID_DURATION && currentPrice === Number(offerPrice) && Math.abs(orderExpiration - expiration) < 30) return;
    }

    else {
      const [topOffer, secondOffer] = await fetchMagicEdenOffer("COLLECTION", task.contract.contractAddress, "{}") as unknown as OpenseaOfferData[]
      const walletsArr = await Wallet.find({})

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
            await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey, orderKeys, taskId: task._id });
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
      if (!warningBids[task._id]) {
        warningBids[task._id] = {
          opensea: false,
          magiceden: false,
          blur: false
        };
      }
      warningBids[task._id] = { opensea: true, magiceden: true, blur: true };

      const message = `⚠️ Offer price ${offerEth} ${currencySymbol} for ${task.contract.slug} collection bid exceeds floor price ${floor_price} ${currencySymbol} FOR MAGICEDEN. Stopping bid...`.toUpperCase();
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
      await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey, orderKeys: orderKeys, taskId: task._id });
    }

    const outbidOptions = currentTask.outbidOptions

    if (outbidOptions.outbid && offerEth > maxBidPriceEth) {
      if (currentTask.outbidOptions.cancelMaxWhenOutbid) {
        if (orders.length > 0) {
          const orderKeys = orders.map((order: PrismaOrder) => order.id)
          const extractedOrderIds = await extractMagicedenOrderHash(orders)
          await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, orderKeys, taskId: task._id });
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
    await bidOnMagiceden(task._id, bidCount, task.wallet, task.contract.contractAddress, 1, colletionOffer.toString(), privateKey, task.contract.slug, expiration, currency);
  } catch (error: any) {
    await logBidError(data.task._id, "MAGICEDEN COLLECTION BID ERROR", `Error processing MagicEden collection bid for task: ${data.task._id}: ${error?.response?.data?.message || error?.message || String(error)} `, "error", "magiceden");
    if (!errorStats[data.task._id]) {
      errorStats[data.task._id] = {
        magiceden: 0,
        opensea: 0,
        blur: 0
      }
    }
    errorStats[data.task._id]['magiceden']++
  }
}


async function processMagicedenTokenBid(data: IMagicedenTokenBidData) {
  try {

    if (!enabledMarketplaces.length) return
    const { contractAddress, address, quantity, privateKey, slug, tokenId, task, _id } = data
    if (!currentTokenIds[_id]?.magiceden?.includes(+tokenId)) return
    const currentTask = activeTasks.get(_id)
    if (!currentTask || !currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("magiceden") || !enabledMarketplaces.map((m) => m.toLowerCase()).includes("magiceden")) return;
    const outbidOptions = currentTask.outbidOptions
    const floor_price = Number(await fetchMagicEdenCollectionStats(task.contract.contractAddress))
    if (!floor_price) return

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
        await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey, orderKeys, taskId: _id });
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
      await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey, orderKeys, taskId: task._id });
    }

    const createdAt = new Date(order?.createdAt).getTime()
    const expiredAt = new Date(order?.expiresAt).getTime()
    const orderExpiration = order ? Math.round((expiredAt - createdAt) / 1000) : expiration

    if (!outbidOptions.outbid) {
      if (ttl >= MIN_BID_DURATION && currentPrice === tokenOffer && Math.abs(orderExpiration - expiration) < 30) return;

    } else {
      const walletsArr = await Wallet.find({})
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
            await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey, orderKeys, taskId: _id });
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
            await queue.add(CANCEL_MAGICEDEN_BID, {
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

    if (orders.length > 0) {
      const orderKeys = orders.map((order: PrismaOrder) => order.id)
      const extractedOrderIds = await extractMagicedenOrderHash(orders)
      await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey, orderKeys, taskId: _id });
    }

    if (outbidOptions.outbid && offerEth > maxBidPriceEth) {
      if (currentTask.outbidOptions.outbid) {
        if (orders.length > 0) {
          const orderKeys = orders.map((order: PrismaOrder) => order.id)
          const extractedOrderIds = await extractMagicedenOrderHash(orders)
          await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey, orderKeys, taskId: _id });
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

    await bidOnMagiceden(_id, bidCount, address, contractAddress, quantity, tokenOffer.toString(), privateKey, slug, expiration, currency, undefined, tokenId)
  } catch (error: any) {
    await logBidError(data._id, "MAGICEDEN TOKEN BID ERROR", `Error bidding on token ${data.tokenId} in collection ${data.slug}: ${error?.response?.data?.message || error?.message || String(error)} `, "error", "magiceden");
    if (!errorStats[data.task._id]) {
      errorStats[data.task._id] = {
        magiceden: 0,
        opensea: 0,
        blur: 0
      }
    }
    errorStats[data.task._id]['magiceden']++
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
  if (!enabledMarketplaces.length) return
  if (!enabledMarketplaces.map((m) => m.toLowerCase()).includes("magiceden")) return
  try {
    const
      { contractAddress, address, quantity, privateKey, slug, trait, _id } = data
    const selectedTraits = currentTraits[_id]?.magiceden
    const traits = Object.entries(selectedTraits).flatMap(([key, values]) =>
      values.map(value => ({ attributeKey: key, attributeValue: value })))

    const traitExists = traits.some(
      (t) => t.attributeKey === trait.attributeKey && t.attributeValue === trait.attributeValue
    );

    if (!traitExists) return

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
        await queue.add(CANCEL_MAGICEDEN_BID, {
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
      await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey, orderKeys, taskId: currentTask._id });
    }

    const createdAt = new Date(order?.createdAt).getTime()
    const expiredAt = new Date(order?.expiresAt).getTime()
    const orderExpiration = order ? Math.round((expiredAt - createdAt) / 1000) : expiration

    if (!outbidOptions.outbid) {
      if (ttl >= MIN_BID_DURATION && currrentPrice === traitOffer && Math.abs(orderExpiration - expiration) < 30) return;
    }

    else {
      const walletsArr = await Wallet.find({})
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
            await queue.add(CANCEL_MAGICEDEN_BID, {
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
            await queue.add(CANCEL_MAGICEDEN_BID, {
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
      await queue.add(CANCEL_MAGICEDEN_BID, {
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
          await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey, orderKeys, taskId: _id });
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

    await bidOnMagiceden(_id, bidCount, address, contractAddress, quantity, traitOffer.toString(), privateKey, slug, expiration, currency, trait)
  } catch (error: any) {
    await logBidError(data._id, "MAGICEDEN TRAIT BID ERROR", `Error bidding on trait ${JSON.stringify(data.trait)} in collection ${data.slug}: ${error?.response?.data?.message || error?.response?.data || error?.message || String(error)} `, "error", "magiceden");
    if (!errorStats[data._id]) {
      errorStats[data._id] = {
        magiceden: 0,
        opensea: 0,
        blur: 0
      }
    }
    errorStats[data._id]['magiceden']++
  }
}

async function bulkCancelOpenseaBid(data: { privateKey: string, orderId: string, taskId: string, orderKey: string }) {
  const { privateKey, orderId, taskId, orderKey } = data
  try {
    await Promise.all([
      prisma.order.deleteMany({
        where: {
          id: orderKey
        }
      }),
      cancelOrder(orderId, OPENSEA_PROTOCOL_ADDRESS, privateKey, taskId)
    ])
  } catch (error) {
    console.error(RED + `Error processing Opensea bid cancellation for task: ${taskId} ` + RESET, error);
  }
}

async function bulkCancelMagicedenBid(data: { orderIds: string[], privateKey: string, orderKeys: string[], taskId: string }) {
  const { orderIds, privateKey, orderKeys, taskId } = data
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
  } catch (error) {
    console.error(RED + `Error processing Magiceden bid cancellation for task: ${taskId} ` + RESET, error);
  }
}

async function validateBidCount() {
  for (const task of currentTasks) {
    try {
      // Check if the Order table exists before querying
      let openseaOrders = [];
      let magicedenOrders = [];
      let blurOrders = [];
      let hasOrderTable = true;

      try {
        openseaOrders = await prisma.order.findMany({
          where: {
            taskId: task._id,
            marketplace: OPENSEA.toLowerCase(),
            expiresAt: {
              gt: new Date()
            }
          }
        });

        magicedenOrders = await prisma.order.findMany({
          where: {
            taskId: task._id,
            marketplace: MAGICEDEN.toLowerCase(),
            expiresAt: {
              gt: new Date()
            }
          }
        });

        blurOrders = await prisma.order.findMany({
          where: {
            taskId: task._id,
            marketplace: BLUR.toLowerCase(),
            expiresAt: {
              gt: new Date()
            }
          }
        })

        const oneMinuteFromNow = new Date(Date.now() + 10 * 1000);
        const expiringOrders = await prisma.order.findMany({
          where: {
            taskId: task._id,
            expiresAt: {
              lte: oneMinuteFromNow
            }
          }
        })

        if (expiringOrders.length > 0) {
          await prisma.order.deleteMany({
            where: {
              taskId: task._id,
              expiresAt: {
                lte: oneMinuteFromNow
              }
            }
          });
          await startTask(task, true)
        }
      } catch (err: any) {
        if (err?.code === 'P2021') {
          // Table doesn't exist yet - likely needs migration
          console.log(YELLOW + `Order table doesn't exist. Run 'npx prisma migrate dev' to create it.` + RESET);
          hasOrderTable = false;
        } else {
          throw err; // Re-throw if it's a different error
        }
      }

      bidStats[task._id] = {
        opensea: hasOrderTable ? openseaOrders.length : 0,
        magiceden: hasOrderTable ? magicedenOrders.length : 0,
        blur: hasOrderTable ? blurOrders.length : 0
      };
    } catch (error) {
      console.error(RED + `Error validating bid count for task: ${task._id}` + RESET, error);
    }
  }
}

setInterval(validateBidCount, 5000);

async function bulkCancelBlurBid(data: BlurCancelPayload) {
  const { orderKey, privateKey, taskId, payload } = data
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
  } catch (error) {
    console.error(RED + `Error processing Blur bid cancellation for task: ${taskId} ` + RESET, error);
  }
}

app.get("/", (req, res) => {
  res.json({ message: "Welcome to the NFTTools bidding bot server! Let's make magic happen! 🚀🚀🚀" });
});

// Debug endpoint to check seen items status
app.get("/seen-items", (req, res) => {
  const now = Date.now();
  const orderHashesInfo = Array.from(seenOrderHashes.entries()).map(([hash, timestamp]) => ({
    hash: hash.substring(0, 8) + '...',
    age: Math.round((now - timestamp) / 1000) + 's',
    expired: now - timestamp > SEEN_ITEMS_TTL
  }));

  const bidsInfo = Array.from(seenBids.entries()).map(([hash, timestamp]) => ({
    hash: hash.substring(0, 8) + '...',
    age: Math.round((now - timestamp) / 1000) + 's',
    expired: now - timestamp > SEEN_ITEMS_TTL
  }));

  res.json({
    seenOrderHashes: {
      count: seenOrderHashes.size,
      items: orderHashesInfo.slice(0, 10), // Show first 10
      maxSize: SEEN_ITEMS_MAX_SIZE,
      ttl: SEEN_ITEMS_TTL / 1000 + 's'
    },
    seenBids: {
      count: seenBids.size,
      items: bidsInfo.slice(0, 10), // Show first 10
      maxSize: SEEN_ITEMS_MAX_SIZE,
      ttl: SEEN_ITEMS_TTL / 1000 + 's'
    }
  });
});

app.get("/health", async (req, res) => {
  try {
    const queueCounts = await queue.getJobCounts();
    const metrics = {
      status: "ok",
      timestamp: new Date().toISOString(),
      uptime: process.uptime(),
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

app.get("/api/websocket/stats", (req, res) => {
  const currentDuration = connectionMetrics.connectionStartTime
    ? Date.now() - connectionMetrics.connectionStartTime
    : 0;

  res.json({
    current: {
      connected: wsManager?.isConnected() || false,
      duration: currentDuration,
      durationFormatted: formatDuration(currentDuration),
      connectionId: connectionMetrics.currentConnectionId
    },
    stats: {
      totalConnections: connectionMetrics.totalConnections,
      totalDisconnections: connectionMetrics.totalDisconnections,
      averageConnectionDuration: connectionMetrics.averageConnectionDuration,
      averageConnectionDurationFormatted: formatDuration(connectionMetrics.averageConnectionDuration),
      longestConnection: connectionMetrics.longestConnection,
      longestConnectionFormatted: formatDuration(connectionMetrics.longestConnection),
      shortestConnection: connectionMetrics.shortestConnection === Infinity ? 0 : connectionMetrics.shortestConnection,
      shortestConnectionFormatted: connectionMetrics.shortestConnection === Infinity ? '0s' : formatDuration(connectionMetrics.shortestConnection)
    },
    history: connectionHistory.slice(-20) // Last 20 connections
  });
});

// Enhanced WebSocket metrics endpoint
app.get('/api/websocket/metrics', (req, res) => {
  if (!wsManager) {
    return res.json({ error: 'WebSocket manager not initialized' });
  }

  const metrics = wsManager.getMetrics();
  const subscriptions = Array.from(wsManager.getActiveSubscriptions().entries()).map(([id, info]) => ({
    id,
    ...info
  }));

  res.json({
    metrics,
    subscriptions,
    isConnected: wsManager.isConnected(),
    url: MARKETPLACE_WS_URL,
    clientId: clientId
  });
});

// Test endpoint to manually connect WebSocket
app.post('/api/websocket/connect', async (req, res) => {
  try {
    if (!wsManager || !wsManager.isConnected()) {
      console.log(YELLOW + `Attempting WebSocket connection to ${MARKETPLACE_WS_URL}` + RESET);
      try {
        await connectWebSocket();
      } catch (error) {
        console.error('Failed to establish WebSocket connection for collection subscription:', error);
      }

      // Wait up to 3 seconds for connection
      let attempts = 0;
      while (!wsManager?.isConnected() && attempts < 6) {
        await new Promise(resolve => setTimeout(resolve, 500));
        attempts++;
      }
    }

    const isConnected = wsManager?.isConnected() || false;
    const metrics = wsManager?.getMetrics();

    res.json({
      success: isConnected,
      isConnected,
      message: isConnected ? 'WebSocket connected' : 'WebSocket connection failed',
      url: MARKETPLACE_WS_URL,
      metrics: metrics ? {
        connectionQuality: metrics.connectionQuality,
        totalConnections: metrics.totalConnections,
        totalErrors: metrics.totalErrors,
        lastError: metrics.lastError
      } : undefined
    });
  } catch (error) {
    console.error(RED + 'WebSocket connection error:' + RESET, error);
    res.status(500).json({
      success: false,
      error: error instanceof Error ? error.message : 'Unknown error',
      url: MARKETPLACE_WS_URL
    });
  }
});

// Test endpoint to manually subscribe to a collection
app.post('/api/websocket/subscribe-test', async (req, res) => {
  try {
    const { slug, contractAddress, marketplace } = req.body;

    if (!slug || !contractAddress || !marketplace) {
      return res.status(400).json({
        error: 'Missing required fields: slug, contractAddress, marketplace'
      });
    }

    if (!wsManager?.isConnected()) {
      try {
        await connectWebSocket();
        await new Promise(resolve => setTimeout(resolve, 1000));
      } catch (error) {
        console.error('Failed to establish WebSocket connection for subscription:', error);
      }
    }

    await wsManager.subscribe(slug, contractAddress, marketplace.toUpperCase());

    res.json({
      success: true,
      message: `Subscribed to ${slug} on ${marketplace}`,
      subscriptions: Array.from(wsManager.getActiveSubscriptions().entries())
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});

// Test raw WebSocket connectivity
app.get('/api/websocket/test-raw', async (req, res) => {
  try {
    const testWs = new WebSocket(MARKETPLACE_WS_URL);
    let connected = false;

    const timeout = setTimeout(() => {
      testWs.close();
      if (!connected) {
        res.json({
          success: false,
          url: MARKETPLACE_WS_URL,
          error: 'Connection timeout'
        });
      }
    }, 5000);

    testWs.on('open', () => {
      connected = true;
      clearTimeout(timeout);
      testWs.close();
      res.json({
        success: true,
        url: MARKETPLACE_WS_URL,
        message: 'Raw WebSocket connection successful'
      });
    });

    testWs.on('error', (error) => {
      clearTimeout(timeout);
      if (!connected) {
        res.json({
          success: false,
          url: MARKETPLACE_WS_URL,
          error: error.message
        });
      }
    });
  } catch (error) {
    res.json({
      success: false,
      url: MARKETPLACE_WS_URL,
      error: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});

// Test WebSocket ping without response requirement
app.post('/api/websocket/test-ping', async (req, res) => {
  try {
    if (!wsManager?.isConnected()) {
      return res.status(400).json({
        success: false,
        error: 'WebSocket not connected'
      });
    }

    // Send a simple ping message that doesn't require response
    wsManager.sendMessage({
      event: 'ping',
      clientId: clientId,
      timestamp: Date.now()
    });

    res.json({
      success: true,
      message: 'Ping sent',
      isConnected: wsManager.isConnected()
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error instanceof Error ? error.message : 'Unknown error'
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
  try {
    const { taskId } = req.params;
    const task = activeTasks.get(taskId)
    if (!task) {
      return res.status(404).json({ error: 'Task not found' });
    }
    const wallet = await Wallet.findById(task.walletId)
    const WALLET_PRIVATE_KEY: string = await decryptPrivateKey(wallet?.encryptedPrivateKey as string)
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
      await queue.addBulk(cancelData)
    }

    if (magicedenOrders.length > 0) {
      const extractedOrderIds = await extractMagicedenOrderHash(magicedenOrders)
      const orderKeys = magicedenOrders.map((order) => order.id)
      await queue.add(CANCEL_MAGICEDEN_BID, {
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
      await queue.addBulk(blurCancelData)
    }

    res.status(200).json({ message: 'Task cancelled' });
  } catch (error: any) {
    console.error('Error updating task:', error);
    await logger.logApiError('Failed to cancel task', {
      endpoint: '/api/task/cancel/:taskId',
      method: 'POST',
      taskId: req.params?.taskId,
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

      try {
        try {
          const openseaFloorPrice = await getOpenseaFloor(slug);
          floorPrices[taskId]['opensea'] = openseaFloorPrice;
          const [topOffer] = (await fetchOpenseaOffers('COLLECTION', slug, contractAddress, {}) as unknown) as OpenseaOfferData[]
          const topOfferAmount = Number(topOffer?.eth)
          bestOffers[taskId]['opensea'] = topOfferAmount

        } catch (error) {
          throw error
        }

        try {
          const blurFloorPrice = await fetchBlurCollectionStats(slug);
          floorPrices[taskId]['blur'] = blurFloorPrice;
          const bestOffer = await fetchBlurBid(contractAddress, "COLLECTION", {})
          const bestOfferAmount = bestOffer?.priceLevels?.[0]?.price ? Number(bestOffer.priceLevels[0].price) : 0
          bestOffers[taskId]['blur'] = bestOfferAmount

        } catch (error) {
          throw error
        }

        try {
          const magicEdenFloorPrice = await fetchMagicEdenCollectionStats(contractAddress);
          floorPrices[taskId]['magiceden'] = magicEdenFloorPrice;

          const [bestOffer] = await fetchMagicEdenOffer("COLLECTION", contractAddress, "{}") as unknown as OpenseaOfferData[]
          const bestOfferAmount = Number(bestOffer.eth)
          bestOffers[taskId]['magiceden'] = bestOfferAmount;

        } catch (error) {
          throw error
        }
      } catch (error) {
        throw error
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
    console.log(error.response);
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
    console.log(error?.response?.data || error?.message);
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
    const password = await decryptPassword(encryptedPassword)

    if (!password) {
      return res.status(400).json({ error: 'Password is required' });
    }
    const wasPasswordSet = !!getPassword();

    // Store password in memory
    setPassword(password);
    // If this is the first time setting password, resume queue and workers
    if (!wasPasswordSet) {
      await resumeQueueAndWorkers();
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


// Secure update endpoint with authentication and rate limiting
app.post('/api/update-app', async (req, res) => {
  try {
    // Authentication check - require API key
    const apiKey = req.headers['x-api-key'] || req.headers['authorization']?.replace('Bearer ', '');

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
    environment: process.env.NODE_ENV || 'development'
  };

  res.status(200).json(healthStatus);
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

    const traitFloorMin = task?.stopOptions?.traitPrices && trait && trait in task?.stopOptions?.traitPrices ? Number(task?.stopOptions?.traitPrices[trait].min) : 0
    const traitFloorMax = task?.stopOptions?.traitPrices && trait && trait in task?.stopOptions?.traitPrices ? Number(task?.stopOptions?.traitPrices[trait].max) : 0


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
    const currentBalance = balances[task._id]?.[marketplace];

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
    console.log(YELLOW + `subscribeToCollections called with ${tasks.length} tasks` + RESET);

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


async function unsubscribeFromCollection(task: ITask, marketplace?: string) {
  try {
    if (!wsManager?.isConnected()) {
      await new Promise(resolve => setTimeout(resolve, 5000)); // Wait 5 seconds
      if (!wsManager?.isConnected()) return;
    }

    await wsManager.unsubscribe(task.contract.slug, task.contract.contractAddress, marketplace);

    if (marketplace) {
      console.log(`Unsubscribed from collection: ${task.contract.slug} on ${marketplace}`);
    } else {
      console.log(`Unsubscribed from collection: ${task.contract.slug} (all marketplaces)`);
    }

    // Remove task from subscribedTasks
    subscribedTasks.delete(task._id);
  } catch (error) {
    console.error(RED + `Error in unsubscribeFromCollection: ` + RESET, error);
    throw error;
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


interface BidLevel {
  contractAddress: string;
  updates: Update[];
}

interface Update {
  criteriaType: string;
  criteriaValue: Record<string, unknown>;
  price: string;
  executableSize: number;
  bidderCount: number;
  singleBidder: string | null;
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

export const abortedTasks: string[] = [];

export function markTaskAsAborted(taskId: string) {
  if (!abortedTasks.includes(taskId)) {
    abortedTasks.push(taskId);
  }
}

function removeTaskFromAborted(taskId: string) {
  const index = abortedTasks.indexOf(taskId);
  if (index > -1) {
    abortedTasks.splice(index, 1);
  }
}

export function isTaskAborted(taskId: string): boolean {
  return abortedTasks.includes(taskId);
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


// Add this interface near other interfaces
interface BidCounts {
  [key: string]: {
    opensea: number;
    magiceden: number;
    blur: number;
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
    waitingTime: number;
    cancellationPercentage: number;
    processingPercentage: number;
    nextLoopTime?: Date
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

  if (!netEnabled && timeSinceLastConnection < 60000) {
    return; // Wait for the full minute confirmation
  }

  if (previousNetState !== netEnabled) {
    if (netEnabled) {
      // Network restored
      console.log('Network restored, resuming operations...');
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
    console.log('Network is connected');
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

    console.log({ listing });

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
      const joinMessage = {
        event: 'join_listings',
        clientId: clientId,
        slug: listing.contract.slug,
        contractAddress: listing.contract.contractAddress
      };

      console.log(`\n🔗 Sending WebSocket subscription for instant price matching:`);
      console.log(`📡 Event: ${joinMessage.event}`);
      console.log(`🆔 Client ID: ${joinMessage.clientId}`);
      console.log(`🏷️  Collection: ${joinMessage.slug}`);
      console.log(`📄 Contract: ${joinMessage.contractAddress}`);
      console.log(`📤 Message:`, JSON.stringify(joinMessage, null, 2));

      // Send join_listings event directly for listing subscriptions
      wsManager.sendMessage(joinMessage);
      console.log(`✅ WebSocket message sent successfully`);
    } else {
      console.log(`❌ WebSocket subscription NOT sent - conditions not met`);
      if (!listing.instantPriceMatching) {
        console.log(`   - Instant price matching is disabled`);
      }
      if (!wsManager) {
        console.log(`   - WebSocket manager is not available`);
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
      // Send leave_listings event directly for listing unsubscriptions
      wsManager.sendMessage({
        event: 'leave_listings',
        clientId: clientId,
        slug: listing.contract.slug,
        contractAddress: listing.contract.contractAddress
      });
    }

    ws.send(JSON.stringify({
      type: 'listing-stopped',
      listingId: data.listingId,
      message: 'Listing stopped successfully'
    }));

    console.log(YELLOW + `Stopped listing ${data.listingId}` + RESET);
  } catch (error) {
    console.error('Error stopping listing:', error);
    ws.send(JSON.stringify({
      type: 'error',
      message: 'Failed to stop listing',
      error: error instanceof Error ? error.message : String(error)
    }));
  }
}

async function handleUpdateListing(data: { listingId: string, updates: any }, ws: WebSocket) {
  try {
    const listing = await Listing.findById(data.listingId);
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
        const joinMessage = {
          event: 'join_listings',
          clientId: clientId,
          slug: listing.contract.slug,
          contractAddress: listing.contract.contractAddress
        };

        console.log(`\n🔗 Enabling instant price matching - sending WebSocket subscription:`);
        console.log(`📡 Event: ${joinMessage.event}`);
        console.log(`🆔 Client ID: ${joinMessage.clientId}`);
        console.log(`🏷️  Collection: ${joinMessage.slug}`);
        console.log(`📄 Contract: ${joinMessage.contractAddress}`);
        console.log(`📤 Message:`, JSON.stringify(joinMessage, null, 2));

        // Subscribe to listing events
        wsManager.sendMessage(joinMessage);
      } else {
        // Unsubscribe from listing events
        wsManager.sendMessage({
          event: 'leave_listings',
          clientId: clientId,
          slug: listing.contract.slug,
          contractAddress: listing.contract.contractAddress
        });
      }
    }

    ws.send(JSON.stringify({
      type: 'listing-updated',
      listingId: data.listingId,
      message: 'Listing updated successfully'
    }));

    console.log(GREEN + `Updated listing ${data.listingId}` + RESET);
  } catch (error) {
    console.error('Error updating listing:', error);
    ws.send(JSON.stringify({
      type: 'error',
      message: 'Failed to update listing',
      error: error instanceof Error ? error.message : String(error)
    }));
  }
}

