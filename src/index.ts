import express from "express";
import { config } from "dotenv";
import bodyParser from "body-parser";
import { clearTimeout } from 'timers';
import os from "os"
import cors from "cors";
import WebSocket, { Server as WebSocketServer } from 'ws';
import http from 'http';
import fs from 'fs/promises';
import path from 'path';
import { axiosInstance, initialize, limiter } from "./init";
import { bidOnOpensea, cancelOrder, fetchItemOffers, fetchOpenseaListings, fetchOpenseaOffers, IFee } from "./marketplace/opensea";
import { bidOnBlur, cancelBlurBid, fetchBlurBid, fetchBlurCollectionStats } from "./marketplace/blur/bid";
import { bidOnMagiceden, cancelMagicEdenBid, fetchMagicEdenCollectionStats, fetchMagicEdenOffer, fetchMagicEdenTokens } from "./marketplace/magiceden";
import { getCollectionDetails, getCollectionStats } from "./functions";
import mongoose from 'mongoose';
import Task from "./models/task.model";
import { Queue, Worker, Job, QueueOptions, JobType, FlowProducer, UnrecoverableError } from "bullmq";
import Wallet from "./models/wallet.model";
import redisClient from "./utils/redis";
import { WETH_CONTRACT_ADDRESS, WETH_MIN_ABI } from "./constants";
import { BigNumber, constants, Contract, ethers, utils, Wallet as Web3Wallet } from "ethers";
import { DistributedLockManager } from "./utils/lock";
import { exec } from "child_process";
import { execSync } from "child_process";
import net from 'net'
import BidLogs from "./models/logs.model";

let RATE_LIMIT = 64;
const bidStats: BidCounts = {};
const bestOffers: BidCounts = {};
const floorPrices: BidCounts = {};
const balances: any = {};
export const warningBids: WarningBids = {};
export const warningMessage: WarningMessage = {};
export const errorStats: BidCounts = {};
const skipStats: BidCounts = {};
const redis = redisClient.getClient()
const QUEUE_NAME = 'BIDDING_BOT';
const CPU_COUNT = os.cpus().length;
const WORKER_COUNT = Math.max(2, Math.min(CPU_COUNT - 1, 4));
const JOB_TIMEOUT = 30000;


const SEAPORT = '0x1e0049783f008a0085193e00003d00cd54003c71';

const TOKEN_BID_PRIORITY = {
  OPENSEA: 3,
  MAGICEDEN: 3,
  BLUR: 3
};

const TRAIT_BID_PRIORITY = {
  OPENSEA: 4,
  MAGICEDEN: 4,
  BLUR: 4
};

const COLLECTION_BID_PRIORITY = {
  OPENSEA: 5,
  MAGICEDEN: 5,
  BLUR: 5
};
config()

type JobState = 'completed' | 'failed' | 'delayed' | 'active' | 'waiting' | 'waiting-children' | 'unknown' | 'prioritized' | 'paused';
const REMOVABLE_STATES: JobState[] = ['prioritized', 'delayed', 'waiting', 'paused'];

const lockManager = new DistributedLockManager({
  lockPrefix: '{marketplace}:',
  defaultTTLSeconds: 60
});

export { redis, lockManager, queue };

const QUEUE_OPTIONS: QueueOptions = {
  prefix: '{bull}',
  connection: redis,
  defaultJobOptions: {
    removeOnComplete: true,
    removeOnFail: true,
    attempts: 1,
    backoff: {
      type: 'fixed',
      delay: 1000
    },
  }
};

let netEnabled = true;

const WORKER_METRICS = new Map<string, {
  lastJobStart: number;
  lastJobEnd: number;
  jobCount: number;
  idleTime: number;
}>();

const flowProducer = new FlowProducer(QUEUE_OPTIONS);

// Update the worker configuration with more conservative settings
const workers = Array.from({ length: WORKER_COUNT }, (_, index) => new Worker(
  QUEUE_NAME,
  async (job) => {
    const startTime = Date.now();
    console.log(`[Worker ${index}] Starting job ${job.id}, attempt ${job.attemptsMade + 1}`);

    try {
      await processJob(job);
      console.log(`[Worker ${index}] Completed job ${job.id} in ${Date.now() - startTime}ms`);
      return;
    } catch (error) {
      console.error(`[Worker ${index}] Failed job ${job.id} after ${Date.now() - startTime}ms:`, error);
      await job.moveToFailed(error as Error, "failed", true).catch((moveError) => {
        console.error(`[Worker ${index}] Error moving job ${job.id} to failed:`, moveError);
      });
      throw error;
    }
  },
  {
    connection: redis,
    prefix: '{bull}',
    // Each worker gets a fair share of the rate limit
    concurrency: RATE_LIMIT / WORKER_COUNT,
    lockDuration: 30000,
    stalledInterval: 15000,
    limiter: {
      max: RATE_LIMIT / WORKER_COUNT,
      duration: 1000,
    },
    maxStalledCount: 1,
    name: `worker-${index + 1}`
  }
));

// Add error handling for stalled jobs
workers.forEach((worker, index) => {
  worker.on('completed', (job: Job) => handleJobCompletion(worker, index, job));
  worker.on('failed', async (job, err) => {
    if (!job) return
    await job.moveToFailed(err as Error, "failed", true).catch(() => { });
  });

  worker.on('stalled', async (jobId) => {
    const job = await queue.getJob(jobId);
    if (job) {
      await job.moveToFailed(new Error('Job stalled')).catch(() => { });
    }
  });

  worker.on('failed', (job, err) => {
    console.error(`Job ${job?.id} failed:`, err);
  });

  worker.on('error', err => {
    console.error('Worker error:', err);
  });
});

// Store timeouts for cleanup
const taskTimeouts: Map<string, NodeJS.Timeout> = new Map();

// Add a Map to track task locks
const taskLocks = new Map<string, TaskLock>();

// Add lock/unlock helper functions
async function lockTask(taskId: string): Promise<boolean> {
  const now = Date.now();
  const currentLock = taskLocks.get(taskId);

  // If task is locked peand lock hasn't expired, return false
  if (currentLock?.locked && now < currentLock.lockUntil) {
    return false;
  }

  const task = activeTasks.get(taskId)
  if (!task || !task.running) return false;
  const lockDuration = getExpiry(task.loopInterval)
  // Set or update lock
  taskLocks.set(taskId, {
    locked: true,
    lockUntil: now + (lockDuration * 1000),
    loopCount: (currentLock?.loopCount || 0) + 1
  });

  return true;
}

function unlockTask(taskId: string): void {
  const currentLock = taskLocks.get(taskId);
  if (currentLock) {
    currentLock.locked = false;
  }
}

// Update handleJobCompletion to use locks
const handleJobCompletion = async (worker: Worker, index: number, job: Job) => {
  try {
    const taskId = extractTaskId(job.name);
    console.log({ jobName: job.name, taskId });
    if (!taskId) return;
    const jobs = await queue.getJobs(['waiting-children']);
    const waitingChildren = jobs?.filter(job => job?.name.includes(taskId));
    if (waitingChildren) return;
    const task = activeTasks.get(taskId);
    if (!task) return;
    const loopInterval = getExpiry(task.loopInterval) + 1;
    const existingTimeout = taskTimeouts.get(taskId);
    if (existingTimeout) {
      clearTimeout(existingTimeout);
    }

    const timeoutId = setTimeout(async () => {
      try {
        const currentTask = activeTasks.get(taskId);


        if (currentTask?.running) {

          console.log('running');

          // Try to acquire lock before starting task
          const locked = await lockTask(taskId);
          if (!locked) {
            console.log(`Task ${taskId} is already running, skipping new loop`);
            return;
          }

          try {
            await startTask(currentTask, true);
          } finally {
            unlockTask(taskId);
          }
        }
      } catch (error) {
        console.error(RED + `Error restarting task ${taskId}:` + RESET, error);
        unlockTask(taskId); // Make sure to unlock on error
      } finally {
        taskTimeouts.delete(taskId);
      }
    }, loopInterval * 1000);

    taskTimeouts.set(taskId, timeoutId);

  } catch (error) {
    console.error(RED + `Error in handleJobCompletion:` + RESET, error);
  }
};

// Helper function for consistent task ID extraction
function extractTaskId(jobName: string): string | null {
  try {
    const [taskId] = jobName.split('-');
    return taskId || null;
  } catch {
    return null;
  }
}

const BROADCAST_INTERVAL = 1000;
setInterval(broadcastBidRates, BROADCAST_INTERVAL);
const ACCEPTABLE_MARGIN_DIFF = 0.0001;


const queue = new Queue(QUEUE_NAME, QUEUE_OPTIONS);

export const MAGENTA = '\x1b[35m';
export const BLUE = '\x1b[34m';
export const RESET = '\x1b[0m';
const GREEN = '\x1b[32m';
const YELLOW = '\x1b[33m';
const RED = '\x1b[31m';
const GOLD = '\x1b[33m';

const OPENSEA = "OPENSEA";
const MAGICEDEN = "MAGICEDEN";
const BLUR = "BLUR";

export const currentTasks: ITask[] = [];

export const OPENSEA_SCHEDULE = "OPENSEA_SCHEDULE"
export const OPENSEA_COLLECTION_BID = "OPENSEA_COLLECTION_BID"
export const OPENSEA_TOKEN_BID = "OPENSEA_TOKEN_BID"
export const OPENSEA_TRAIT_BID = "OPENSEA_TRAIT_BID"
export const BLUR_SCHEDULE = "BLUR_SCHEDULE"
export const BLUR_COLLECTION_BID = "BLUR_COLLECTION_BID"
export const BLUR_TRAIT_BID = "BLUR_TRAIT_BID"
export const MAGICEDEN_SCHEDULE = "MAGICEDEN_SCHEDULE"
export const MAGICEDEN_COLLECTION_BID = "MAGICEDEN_COLLECTION_BID"
export const MAGICEDEN_TOKEN_BID = "MAGICEDEN_TOKEN_BID"
export const MAGICEDEN_TRAIT_BID = "MAGICEDEN_TRAIT_BID"
export const OPENSEA_TOKEN_BID_COUNTERBID = "OPENSEA_TOKEN_BID_COUNTERBID"
export const OPENSEA_TRAIT_BID_COUNTERBID = "OPENSEA_TRAIT_BID_COUNTERBID"
export const OPENSEA_COLLECTION_BID_COUNTERBID = "OPENSEA_COLLECTION_BID_COUNTERBID"
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
const MARKETPLACE_WS_URL = "wss://wss-marketplace.nfttools.website";
const ALCHEMY_API_KEY = process.env.ALCHEMY_API_KEY as string;
const PRIORITIZED_THRESHOLD = RATE_LIMIT * WORKER_COUNT;
const OPENSEA_PROTOCOL_ADDRESS = "0x0000000000000068F116a894984e2DB1123eB395"
const HEALTH_CHECK_INTERVAL = 10000;

const app = express();
const port = process.env.PORT || 3003;

app.use(bodyParser.json());
app.use(cors());

const server = http.createServer(app);
const wss = new WebSocketServer({ server });
let ws: WebSocket;
let heartbeatIntervalId: NodeJS.Timeout | null = null;
let reconnectTimeoutId: NodeJS.Timeout | null = null;
let retryCount: number = 0;

const taskIntervals = new Map<string, NodeJS.Timeout>();
const activeSubscriptions: Set<string> = new Set();
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
    console.log({ bidStats });
    console.log({ bestOffers });
    console.log({ floorPrices });
    console.log({ warningBids });

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

    if (counts.active > PRIORITIZED_THRESHOLD) {
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
      console.log(`   ${key}: ${value}`);
    });

    console.log(BLUE + '\n📋 Queue Status:' + RESET);
    Object.entries(counts).forEach(([key, value]) => {
      const color = value > RATE_LIMIT * 2 ? RED : value > RATE_LIMIT ? YELLOW : GREEN;
      console.log(`   ${key}: ${color}${value}${RESET}`);
    });
    console.log(`   Total Jobs: ${totalJobs}`);

    const activeWorkers = workers.filter(w => w.isRunning()).length;
    console.log(BLUE + '\n👷 Worker Status:' + RESET, `${GREEN}✅ ${activeWorkers}/${WORKER_COUNT} Workers Active${RESET}`);
  } catch (error) {
    console.error(RED + '❌ Error monitoring health:' + RESET, error);
  }
}


setInterval(monitorHealth, HEALTH_CHECK_INTERVAL);

// Update cleanup function to handle multiple workers
async function cleanup() {
  try {
    console.log(YELLOW + '\n=== Starting Cleanup ===');

    console.log('Closing workers...');
    await Promise.all(workers.map(worker => worker.close()));

    console.log('Closing queue...');
    await queue.close();

    console.log(GREEN + '=== Cleanup Complete ===\n' + RESET);
    await redis.disconnect();

    // Clear all pending timeouts
    for (const [taskId, timeout] of taskTimeouts) {
      clearTimeout(timeout);
      taskTimeouts.delete(taskId);
    }

  } catch (error) {
    console.error(RED + 'Error during cleanup:' + RESET, error);
  }
}


process.on('SIGTERM', cleanup);
process.on('SIGINT', cleanup);

// Add this near the top with other global variables
export const activeTasks: Map<string, ITask> = new Map();

// Modify fetchCurrentTasks to populate activeTasks
async function fetchCurrentTasks() {
  try {
    console.log(BLUE + '\n=== Fetching Current Tasks ===' + RESET);
    const tasks = await Task.find({}).lean().exec() as unknown as ITask[];
    const wallets = (await Wallet.find({}).lean()).map((wallet: any) => wallet.address);

    walletsArr.push(...wallets);
    const formattedTasks = tasks.map((task) => ({
      ...task,
      _id: task._id.toString(),
      user: task.user.toString()
    }));

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
        task.running && task.selectedMarketplaces.map(m => m.toLowerCase()).includes("opensea")
          ? [{ name: OPENSEA_SCHEDULE, data: task }]
          : []
      ).concat(
        formattedTasks.flatMap(task =>
          task.running && task.selectedMarketplaces.map(m => m.toLowerCase()).includes("blur")
            ? [{ name: BLUR_SCHEDULE, data: task }]
            : []
        )
      ).concat(
        formattedTasks.flatMap(task =>
          task.running && task.selectedMarketplaces.map(m => m.toLowerCase()).includes("magiceden")
            ? [{ name: MAGICEDEN_SCHEDULE, data: task }]
            : []
        )
      );

    console.log(`Created ${jobs.length} initial jobs`);



    console.log(GREEN + '=== Task Initialization Complete ===\n' + RESET);
  } catch (error) {
    console.error(RED + 'Error fetching current tasks:', error, RESET);
  }
}

const DOWNTIME_THRESHOLD = 5 * 60 * 1000;
const LAST_RUNTIME_KEY = 'server:last_runtime';

async function startServer() {
  try {
    await mongoose.connect(process.env.MONGODB_URI as string, {
      serverSelectionTimeoutMS: 15000,
      connectTimeoutMS: 15000,
      socketTimeoutMS: 30000,
      maxPoolSize: 10,
      retryWrites: true
    });
    console.log('Connected to MongoDB');

    try {
      const lastRuntime = await redis.get(LAST_RUNTIME_KEY);
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
              const currentState = await job.getState();
              if (currentState !== 'active') {
                await job?.remove();
              }
            }));
          }
        }
      }

      await initialize(RATE_LIMIT);

      await redis.set(LAST_RUNTIME_KEY, currentTime.toString());
    } catch (redisError) {
      console.error('Redis operation failed:', redisError);
    }

    setInterval(async () => {
      await redis.set(LAST_RUNTIME_KEY, Date.now().toString());
    }, 60000);

    server.listen(port, () => {
      console.log(`Magic happening on http://localhost:${port}`);
      console.log(`WebSocket server is running on ws://localhost:${port}`);
    });
    await fetchCurrentTasks()

  } catch (error) {
    console.error(RED + 'Failed to start server:' + RESET, error);
    process.exit(1);
  }
}

startServer()
  .then(() => {
    runScheduledLoop().catch(error => {
      console.error('Failed to run scheduled loop:', error);
    })

    connectWebSocket().catch(error => {
      console.error('Failed to connect to WebSocket:', error);
    })
  })
  .catch(error => {
    console.error('Failed to start server:', error);
  });

export const rps = {
  currentRPS: 0,
  maxRPS: RATE_LIMIT ?? 0,
}

function broadcastBidRates() {
  const bidRates = activeTasks.size > 0 ? getAllBidRates() : 0;
  const message = JSON.stringify({
    type: 'bidRatesUpdate',
    data: { bidRates, bidCounts: bidStats, skipCounts: skipStats, errorCounts: errorStats, floorPrices, bestOffers, warningBids, warningMessage, rps }
  });

  clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  });
}


// Update the WebSocket connection handler to send initial state
wss.on('connection', async (ws) => {
  // Add the new client to the set
  clients.add(ws);

  console.log(GREEN + 'New WebSocket connection' + RESET);


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
          await processUpdatedTask(message.data);
          break;
        case 'toggle-status':
          await updateStatus(message.data);
          break;
        case 'stop-task':
          await stopTask(message.data, false);
          break;
        case 'update-multiple-tasks-status':
          await updateMultipleTasksStatus(message.data);
          break;
        case 'update-marketplace':
          await updateMarketplace(message.data)
          break
        case 'retry-connection':
          await connectWebSocket()
          break
        case 'update-config':
          await updateConfig(message.data)
          break
        case 'shutdown':
          await handleShutdown();
          break;
        case 'update':
          await handleUpdate();
          break;
        default:
          console.warn(YELLOW + `Unknown endpoint: ${message.endpoint}` + RESET);
      }
    } catch (error) {
      console.error(RED + 'Error handling WebSocket message:' + RESET, error);
    }
  };

  ws.onclose = () => {
    // Remove client when disconnected
    clients.delete(ws);
    console.log(YELLOW + 'WebSocket connection closed' + RESET);
  };
});

async function updateConfig(data: any) {
  try {
    const { apiKey, rateLimit } = data;
    const oldRateLimit = RATE_LIMIT;

    // Update global variables with new values
    let API_KEY = apiKey
    RATE_LIMIT = rateLimit

    // Get path to .env file in project root
    const envPath = path.resolve(process.cwd(), '.env');

    // Try to read existing .env file content
    let envContent = '';
    try {
      envContent = await fs.readFile(envPath, 'utf-8');
    } catch (error) {
      // If .env doesn't exist, create an empty one
      console.log(YELLOW + 'Creating new .env file' + RESET);
      await fs.writeFile(envPath, '', 'utf-8');
    }

    // Parse existing .env file into key-value pairs
    // Filter out empty lines and split each line into key and value
    const envLines = envContent.split('\n').filter(line => line.trim());
    const newEnv = new Map(
      envLines.map(line => {
        // Handle values that may contain = signs by joining remaining parts
        const [key, ...values] = line.split('=');
        return [key, values.join('=')];
      })
    );

    // Update environment variables in memory and in Map
    if (apiKey) {
      newEnv.set('API_KEY', apiKey);
      process.env.API_KEY = apiKey;
    }

    if (rateLimit) {
      newEnv.set('RATE_LIMIT', rateLimit.toString());
      process.env.RATE_LIMIT = rateLimit.toString();
    }

    // Convert Map back to .env file format
    const newContent = Array.from(newEnv.entries())
      .map(([key, value]) => `${key}=${value}`)
      .join('\n');

    // Write updated content back to .env file
    await fs.writeFile(envPath, newContent + '\n', 'utf-8');

    console.log(GREEN + 'Configuration updated successfully' + RESET);

    // If rate limit changed, we need to restart server components
    if (oldRateLimit !== rateLimit) {
      console.log(YELLOW + 'Rate limit changed. Restarting server components...' + RESET);

      // Pause queue and shutdown existing workers
      await queue.pause();
      await Promise.all(workers.map(worker => worker.close()));

      // Reinitialize with new rate limit
      await initialize(RATE_LIMIT);

      rps.maxRPS = RATE_LIMIT

      // Create new workers with updated settings
      workers.length = 0; // Clear existing workers array
      for (let i = 0; i < WORKER_COUNT; i++) {
        const worker = new Worker(
          QUEUE_NAME,
          async (job) => {
            try {
              // Process job with timeout protection
              const result = await Promise.race([
                processJob(job),
                new Promise((_, reject) =>
                  setTimeout(() => reject(new Error('Job timeout')), JOB_TIMEOUT)
                )
              ]);
              return result;
            } catch (error) {
              throw error;
            }
          },
          {
            connection: redis,
            prefix: '{bull}',
            concurrency: RATE_LIMIT * WORKER_COUNT,
            lockDuration: 30000,
            stalledInterval: 30000,
            limiter: {
              max: RATE_LIMIT * WORKER_COUNT,
              duration: 1000
            },
            maxStalledCount: 1,
            name: `worker-${i + 1}`
          }
        );

        worker.on('completed', (job: Job) => handleJobCompletion(worker, i, job));

        workers.push(worker);
      }

      await queue.resume();
    }

    console.log(GREEN + 'Server components restarted successfully' + RESET);

    // Return updated config values
    return {
      apiKey: process.env.API_KEY,
      rateLimit: process.env.RATE_LIMIT
    };

  } catch (error) {
    console.error(RED + 'Error updating configuration:' + RESET, error);
    throw error;
  }
}

async function processJob(job: Job) {
  const startTime = Date.now();

  try {
    const result = await (async () => {
      switch (job.name) {
        case OPENSEA_COLLECTION_BID:
          const counts = await queue.getJobCounts();
          console.log(`Starting ${job.name}, queue size:`, JSON.stringify(counts));
          const result = await processOpenseaCollectionBid(job.data);
          console.log(`Completed ${job.name} in ${Date.now() - startTime}ms`);
          return result;
        case BLUR_COLLECTION_BID:
          const blurCounts = await queue.getJobCounts();
          console.log(`Starting ${job.name}, queue size:`, JSON.stringify(blurCounts));
          const blurResult = await processBlurCollectionBid(job.data);
          console.log(`Completed ${job.name} in ${Date.now() - startTime}ms`);
          return blurResult;
        case MAGICEDEN_COLLECTION_BID:
          const meCounts = await queue.getJobCounts();
          console.log(`Starting ${job.name}, queue size:`, JSON.stringify(meCounts));
          const meResult = await processMagicedenCollectionBid(job.data);
          console.log(`Completed ${job.name} in ${Date.now() - startTime}ms`);
          return meResult;
        case OPENSEA_COLLECTION_BID_COUNTERBID:
          const counterCounts = await queue.getJobCounts();
          console.log(`Starting ${job.name}, queue size:`, JSON.stringify(counterCounts));
          const counterResult = await openseaCollectionCounterBid(job.data);
          console.log(`Completed ${job.name} in ${Date.now() - startTime}ms`);
          return counterResult;
        case MAGICEDEN_COLLECTION_BID_COUNTERBID:
          const meCounterCounts = await queue.getJobCounts();
          console.log(`Starting ${job.name}, queue size:`, JSON.stringify(meCounterCounts));
          const meCounterResult = await magicedenCollectionCounterBid(job.data);
          console.log(`Completed ${job.name} in ${Date.now() - startTime}ms`);
          return meCounterResult;
        case BLUR_COLLECTION_BID_COUNTERBID:
          const blurCounterCounts = await queue.getJobCounts();
          console.log(`Starting ${job.name}, queue size:`, JSON.stringify(blurCounterCounts));
          const blurCounterResult = await blurCollectionCounterBid(job.data);
          console.log(`Completed ${job.name} in ${Date.now() - startTime}ms`);
          return blurCounterResult;
        case OPENSEA_TOKEN_BID_COUNTERBID:
          const tokenCounterCounts = await queue.getJobCounts();
          console.log(`Starting ${job.name}, queue size:`, JSON.stringify(tokenCounterCounts));
          const tokenCounterResult = await openseaTokenCounterBid(job.data);
          console.log(`Completed ${job.name} in ${Date.now() - startTime}ms`);
          return tokenCounterResult;
        case BLUR_TRAIT_BID_COUNTERBID:
          const traitCounterCounts = await queue.getJobCounts();
          console.log(`Starting ${job.name}, queue size:`, JSON.stringify(traitCounterCounts));
          const traitCounterResult = await blurTraitCounterBid(job.data);
          console.log(`Completed ${job.name} in ${Date.now() - startTime}ms`);
          return traitCounterResult;
        case OPENSEA_TRAIT_BID_COUNTERBID:
          const osTraitCounterCounts = await queue.getJobCounts();
          console.log(`Starting ${job.name}, queue size:`, JSON.stringify(osTraitCounterCounts));
          const osTraitCounterResult = await openseaTraitCounterBid(job.data);
          console.log(`Completed ${job.name} in ${Date.now() - startTime}ms`);
          return osTraitCounterResult;
        case MAGICEDEN_TOKEN_BID_COUNTERBID:
          const meTokenCounterCounts = await queue.getJobCounts();
          console.log(`Starting ${job.name}, queue size:`, JSON.stringify(meTokenCounterCounts));
          const meTokenCounterResult = await magicedenTokenCounterBid(job.data);
          console.log(`Completed ${job.name} in ${Date.now() - startTime}ms`);
          return meTokenCounterResult;
        case MAGICEDEN_TRAIT_BID_COUNTERBID:
          const meTraitCounterCounts = await queue.getJobCounts();
          console.log(`Starting ${job.name}, queue size:`, JSON.stringify(meTraitCounterCounts));
          const meTraitCounterResult = await magicedenTraitCounterBid(job.data);
          console.log(`Completed ${job.name} in ${Date.now() - startTime}ms`);
          return meTraitCounterResult;
        case OPENSEA_SCHEDULE:
          const scheduleCounts = await queue.getJobCounts();
          console.log(`Starting ${job.name}, queue size:`, JSON.stringify(scheduleCounts));
          const scheduleResult = await processOpenseaScheduledBid(job.data);
          console.log(`Completed ${job.name} in ${Date.now() - startTime}ms`);
          return scheduleResult;
        case OPENSEA_TRAIT_BID:
          const osTraitCounts = await queue.getJobCounts();
          console.log(`Starting ${job.name}, queue size:`, JSON.stringify(osTraitCounts));
          const osTraitResult = await processOpenseaTraitBid(job.data);
          console.log(`Completed ${job.name} in ${Date.now() - startTime}ms`);
          return osTraitResult;
        case OPENSEA_TOKEN_BID:
          const osTokenCounts = await queue.getJobCounts();
          console.log(`Starting ${job.name}, queue size:`, JSON.stringify(osTokenCounts));
          const osTokenResult = await processOpenseaTokenBid(job.data);
          console.log(`Completed ${job.name} in ${Date.now() - startTime}ms`);
          return osTokenResult;
        case BLUR_SCHEDULE:
          const blurScheduleCounts = await queue.getJobCounts();
          console.log(`Starting ${job.name}, queue size:`, JSON.stringify(blurScheduleCounts));
          const blurScheduleResult = await processBlurScheduledBid(job.data);
          console.log(`Completed ${job.name} in ${Date.now() - startTime}ms`);
          return blurScheduleResult;
        case BLUR_TRAIT_BID:
          const blurTraitCounts = await queue.getJobCounts();
          console.log(`Starting ${job.name}, queue size:`, JSON.stringify(blurTraitCounts));
          const blurTraitResult = await processBlurTraitBid(job.data);
          console.log(`Completed ${job.name} in ${Date.now() - startTime}ms`);
          return blurTraitResult;
        case MAGICEDEN_SCHEDULE:
          const meScheduleCounts = await queue.getJobCounts();
          console.log(`Starting ${job.name}, queue size:`, JSON.stringify(meScheduleCounts));
          const meScheduleResult = await processMagicedenScheduledBid(job.data);
          console.log(`Completed ${job.name} in ${Date.now() - startTime}ms`);
          return meScheduleResult;
        case MAGICEDEN_TRAIT_BID:
          const meTraitCounts = await queue.getJobCounts();
          console.log(`Starting ${job.name}, queue size:`, JSON.stringify(meTraitCounts));
          const meTraitResult = await processMagicedenTraitBid(job.data);
          console.log(`Completed ${job.name} in ${Date.now() - startTime}ms`);
          return meTraitResult;
        case MAGICEDEN_TOKEN_BID:
          const meTokenCounts = await queue.getJobCounts();
          console.log(`Starting ${job.name}, queue size:`, JSON.stringify(meTokenCounts));
          const meTokenResult = await processMagicedenTokenBid(job.data);
          console.log(`Completed ${job.name} in ${Date.now() - startTime}ms`);
          return meTokenResult;
        case CANCEL_OPENSEA_BID:
          const cancelOsCounts = await queue.getJobCounts();
          console.log(`Starting ${job.name}, queue size:`, JSON.stringify(cancelOsCounts));
          const cancelOsResult = await bulkCancelOpenseaBid(job.data);
          console.log(`Completed ${job.name} in ${Date.now() - startTime}ms`);
          return cancelOsResult;
        case CANCEL_MAGICEDEN_BID:
          const cancelMeCounts = await queue.getJobCounts();
          console.log(`Starting ${job.name}, queue size:`, JSON.stringify(cancelMeCounts));
          const cancelMeResult = await bulkCancelMagicedenBid(job.data);
          console.log(`Completed ${job.name} in ${Date.now() - startTime}ms`);
          return cancelMeResult;
        case CANCEL_BLUR_BID:
          const cancelBlurCounts = await queue.getJobCounts();
          console.log(`Starting ${job.name}, queue size:`, JSON.stringify(cancelBlurCounts));
          const cancelBlurResult = await blukCancelBlurBid(job.data);
          console.log(`Completed ${job.name} in ${Date.now() - startTime}ms`);
          return cancelBlurResult;
        default:

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

  const RATE_LIMIT = process.env.RATE_LIMIT
  const API_KEY = process.env.API_KEY

  if (!API_KEY) {
    console.log(RED + 'API_KEY is not set' + RESET);
    return
  }


  if (!RATE_LIMIT) {
    console.log(RED + 'RATE_LIMIT is not set' + RESET);
    return
  }

  for (const [taskId, task] of activeTasks.entries()) {
    if (!task.running) continue;
    await startTask(task, true);
  }
}

async function processImportedTasks(tasks: ITask[]) {
  try {
    console.log(BLUE + `\n===  Importing ${tasks.length} Tasks === ` + RESET);

    tasks.forEach(task => {
      console.log(`Collection: ${task.contract.slug} `);
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
    subscribeToCollections(tasks);

    console.log(GREEN + `=== Imported Tasks Processing Complete ===\n` + RESET);
  } catch (error) {
    console.error(RED + `Error processing imported tasks` + RESET, error);
  }
}


async function processNewTask(task: ITask) {
  try {
    console.log(BLUE + `\n === Processing New Task === ` + RESET);
    console.log(`Collection: ${task.contract.slug} `);
    console.log(`Task ID: ${task._id} `);

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
    subscribeToCollections([task]);
    console.log(GREEN + `=== New Task Processing Complete ===\n` + RESET);
  } catch (error) {
    console.error(RED + `Error processing new task: ${task.contract.slug} ` + RESET, error);
  }
}

async function processUpdatedTask(task: ITask) {
  try {
    const existingTaskIndex = currentTasks.findIndex(t => t._id === task._id);
    const taskDoc = await Task.findById(task._id).select('balance').exec();
    const balance = taskDoc?.balance;
    balances[task._id] = balance
    warningBids[task._id] = {
      opensea: false,
      magiceden: false,
      blur: false
    }

    if (existingTaskIndex !== -1) {
      currentTasks.splice(existingTaskIndex, 1, task);
      activeTasks.set(task._id, task);
      console.log(YELLOW + `Updated existing task: ${task.contract.slug} `.toUpperCase() + RESET);

      const jobs = await queue.getJobs(['prioritized', 'waiting', 'paused', 'waiting-children'])

      const filteredTasks = Object.fromEntries(
        Object.entries(task?.selectedTraits || {}).map(([category, traits]) => [
          category,
          traits.filter(trait => trait.availableInMarketplaces
            .map((item) => item.toLowerCase())
            .includes("blur"))
        ]).filter(([_, traits]) => traits.length > 0)
      );
      const selectedTraits = transformNewTask(filteredTasks)
      const expiry = getExpiry(task.bidDuration)

      if (task.selectedMarketplaces.map(m => m.toLowerCase()).includes("blur")) {
        const blurJobNames = getJobNames(BLUR.toLowerCase(), task)
        jobs.forEach(async (job) => {
          if (!job?.id) return
          if (blurJobNames.includes(job.name)) {
            const state = await job.getState();
            if (state !== 'active') {
              await job?.remove();
            }
          }
        })

        const traits = transformBlurTraits(selectedTraits)
        const traitBid = selectedTraits && Object.keys(selectedTraits).length > 0
        let floor_price: number = 0

        if (task.bidPrice.minType == "eth" || task.openseaBidPrice.minType === "eth") {
          floor_price = 0
        } else {
          const stats = await fetchBlurCollectionStats(task._id, task.contract.slug);
          if (!stats || stats === 0) return
          floor_price = Number(stats)
        }

        const { offerPriceEth, maxBidPriceEth, minBidPriceEth } = await calculateBidPrice(task, floor_price, "blur")
        const offerPrice = BigInt(Math.round(offerPriceEth * 1e18 / 1e16) * 1e16);

        if (traitBid) {
          await processBlurTraitJobs(task, traits, offerPrice, expiry, maxBidPriceEth, minBidPriceEth)
        }
      } else if (task.selectedMarketplaces.map(m => m.toLowerCase()).includes("opensea")) {
        const openseaJobNames = getJobNames(OPENSEA.toLowerCase(), task)
        jobs.forEach(async (job) => {
          if (!job?.id) return
          if (openseaJobNames.includes(job.name)) {
            const state = await job.getState();
            if (state !== 'active') {
              await job?.remove();
            }
          }
        })

        const traits = transformOpenseaTraits(selectedTraits)
        const traitBid = selectedTraits && Object.keys(selectedTraits).length > 0

        const autoIds = task.tokenIds
          .filter(id => id.toString().toLowerCase().startsWith('bot'))
          .map(id => {
            const matches = id.toString().match(/\d+/);
            return matches ? parseInt(matches[0]) : null;
          })
          .filter(id => id !== null);

        const bottlomListing = await fetchOpenseaListings(task._id, task.contract.slug, autoIds[0]) ?? []
        const taskTokenIds = task.tokenIds
        const tokenIds = [...bottlomListing, ...taskTokenIds]
        const tokenBid = task.bidType === "token" && tokenIds.length > 0

        let floor_price: number = 0
        if (task.bidPrice.minType == "eth" || task.openseaBidPrice.minType === "eth") {
          floor_price = 0
        } else {
          const data = await getCollectionStats(task.contract.slug);
          if (!data || data === 0) return
          floor_price = data;
        }
        const { offerPriceEth, maxBidPriceEth, minBidPriceEth } = await calculateBidPrice(task, floor_price, "opensea")
        let offerPrice = BigInt(Math.ceil(offerPriceEth * 1e18));
        const collectionDetails = await getCollectionDetails(task.contract.slug, task.contract.contractAddress);

        const creatorFees: IFee = collectionDetails.creator_fees.null !== undefined
          ? { null: collectionDetails.creator_fees.null }
          : Object.fromEntries(Object.entries(collectionDetails.creator_fees).map(([key, value]) => [key, Number(value)]));

        if (traitBid) {
          await processOpenseaTraitJobs(task, traits, offerPrice, expiry, creatorFees, collectionDetails)
        } else if (tokenBid) {
          await updateOpenseaTokenJobs(task, tokenIds, Number(offerPrice), expiry, maxBidPriceEth, creatorFees, collectionDetails, minBidPriceEth)
        }
      } else if (task.selectedMarketplaces.map(m => m.toLowerCase()).includes("magiceden")) {
        const magicedenJobNames = getJobNames(MAGICEDEN.toLowerCase(), task)
        jobs.forEach(async (job) => {
          if (!job?.id) return
          if (magicedenJobNames.includes(job.name)) {
            const state = await job.getState();
            if (state !== 'active') {
              await job?.remove();
            }
          }
        })
        const traits = Object.entries(selectedTraits).flatMap(([key, values]) =>
          values.map(value => ({ attributeKey: key, attributeValue: value })))
        const traitBid = selectedTraits && Object.keys(selectedTraits).length > 0
        let floor_price: number = 0

        if (task.bidPrice.minType == "eth" || task.openseaBidPrice.minType === "eth") {
          floor_price = 0
        } else {
          floor_price = Number(await fetchMagicEdenCollectionStats(task._id, task.contract.contractAddress))
        }
        const { offerPriceEth, maxBidPriceEth, minBidPriceEth } = await calculateBidPrice(task, floor_price as number, "magiceden")
        const offerPrice = Math.ceil(offerPriceEth * 1e18)
        const autoIds = task.tokenIds
          .filter(id => id.toString().toLowerCase().startsWith('bot'))
          .map(id => {
            const matches = id.toString().match(/\d+/);
            return matches ? parseInt(matches[0]) : null;
          })
          .filter(id => id !== null);

        const amount = autoIds[0]
        const bottlomListing = amount ? await fetchMagicEdenTokens(task._id, task.contract.contractAddress, amount) : []
        const taskTokenIds = task.tokenIds
        const tokenIds = bottlomListing ? [...bottlomListing, ...taskTokenIds] : [...taskTokenIds]
        const tokenBid = task.bidType === "token" && tokenIds.length > 0

        if (traitBid) {

          await processMagicEdenTraitJobs(task, traits, offerPrice, expiry)
        } else if (tokenBid) {
          await updateMagicEdenTokenJobs(task, tokenIds, offerPrice, expiry, maxBidPriceEth, minBidPriceEth)
        }
      }

    } else {
      console.log(RED + `Attempted to update non - existent task: ${task.contract.slug} ` + RESET);
    }
  } catch (error) {
    console.error(RED + `Error processing updated task: ${task.contract.slug} ` + RESET, error);
  }
}

const BATCH_DELAY = 1000;


const MAX_TOTAL_JOBS = RATE_LIMIT * 10;



async function addBatchedFlow(name: string, children: any[], priority: number) {
  try {
    const taskId = children[0]?.data?._id;
    if (!taskId) return;

    // Get initial job counts once
    let counts = await queue.getJobCounts();
    let activeJobs = counts.active || 0;
    let totalJobs = Object.values(counts).reduce((sum, count) => sum + count, 0);

    // Add at top of addBatchedFlow()
    const startTime = Date.now();
    console.log(`Starting batch flow for ${children.length} jobs`);

    // Calculate optimal batch size to maintain steady RPS
    const BATCH_SIZE = Math.floor(RATE_LIMIT / 2); // Process half rate limit per batch
    const BATCH_INTERVAL = 500; // Process two batches per second


    for (let i = 0; i < children.length;) {
      const task = activeTasks.get(taskId);
      if (!task?.running) return;

      // Wait if we exceed either active or total job limits
      while (activeJobs >= RATE_LIMIT) {
        const currentTask = activeTasks.get(taskId);
        if (!currentTask?.running) return;
        await new Promise(resolve => setTimeout(resolve, 100));
        counts = await queue.getJobCounts();
        activeJobs = counts.active || 0;
      }

      // Calculate optimal batch size based on available capacity
      const availableCapacity = Math.min(
        RATE_LIMIT - activeJobs,
        MAX_TOTAL_JOBS - totalJobs,
        children.length - i
      );
      const batchSize = Math.min(BATCH_SIZE, children.length - i);
      const batch = children.slice(i, i + batchSize);
      if (batchSize <= 0) continue;

      const flowStartTime = Date.now();
      console.log(`Starting batch ${i / batchSize} of ${Math.ceil(children.length / batchSize)}`);
      const batchStartTime = Date.now();
      await flowProducer.add({
        name,
        queueName: QUEUE_NAME,
        opts: {
          priority,
          timestamp: Date.now() // Add timestamp for tracking
        },
        children: batch.map(job => ({
          name: job.name,
          data: job.data,
          queueName: QUEUE_NAME,
          opts: {
            ...job.opts,
            attempts: 3, // Add retry attempts
            backoff: {
              type: 'exponential',
              delay: 1000
            }
          }
        }))
      }, {
        queuesOptions: {
          [QUEUE_NAME]: {
            defaultJobOptions: {
              removeOnComplete: true,
              removeOnFail: true
            }
          }
        }
      });

      console.log(`Batch processing breakdown:
        Batch size: ${batchSize}
        Queue addition time: ${Date.now() - batchStartTime}ms
        Total flow time: ${Date.now() - flowStartTime}ms
        Jobs/second: ${batchSize / ((Date.now() - batchStartTime) / 1000)}
      `);

      // Update counters and index
      i += batchSize
      activeJobs += batchSize;

      // Wait until next second to maintain steady RPS
      const processingTime = Date.now() - batchStartTime;
      if (processingTime < BATCH_INTERVAL) {
        await new Promise(resolve => setTimeout(resolve, BATCH_INTERVAL - processingTime));
      }

      activeJobs += batchSize;
      totalJobs += batchSize;

      // Add inside the main loop
      console.log(`Batch metrics:
        Active jobs: ${activeJobs}
        Total jobs: ${totalJobs}
        Batch size: ${batchSize}
        Processing time: ${Date.now() - startTime}ms
      `);

      // Add Redis monitoring
      const redisInfo = await redis.info();
      const metrics = redisInfo.split('\n').reduce((acc: any, line) => {
        const [key, value] = line.split(':');
        if (key && value) acc[key.trim()] = value.trim();
        return acc;
      }, {});

      console.log(`Redis metrics:
        Used memory: ${metrics.used_memory_human}
        Peak memory: ${metrics.used_memory_peak_human}
        AOF size: ${metrics.aof_current_size}
        Ops/sec: ${metrics.instantaneous_ops_per_sec}
      `);


      // Ensure consistent timing between batches
      await new Promise(resolve => setTimeout(resolve, BATCH_INTERVAL));
    }
  } catch (error) {
    console.error(RED + `Error in addBatchedFlow: ${error}` + RESET);
    throw error;
  }
}

async function updateMagicEdenTokenJobs(task: ITask, tokenIds: (number | string)[], offerPrice: number, expiry: number, maxBidPriceEth: number, minBidPriceEth: number) {
  try {
    const jobs = tokenIds
      .filter(token => !isNaN(Number(token)))
      .map((token) => ({
        name: MAGICEDEN_TOKEN_BID,
        data: {
          _id: task._id,
          address: task.wallet.address,
          contractAddress: task.contract.contractAddress,
          quantity: 1,
          expiration: expiry,
          privateKey: task.wallet.privateKey,
          slug: task.contract.slug,
          tokenId: token,
          outbidOptions: task.outbidOptions,
          maxBidPriceEth,
          minBidPriceEth
        },
        opts: { priority: TOKEN_BID_PRIORITY.MAGICEDEN }
      }));


    const name = `${task._id}-${MAGICEDEN_TOKEN_BID}`

    await addBatchedFlow(name, jobs, TOKEN_BID_PRIORITY.MAGICEDEN);

  } catch (error) {
    console.error(RED + `Error updating magiceden token jobs: ${task.contract.slug} ` + RESET, error);
  }

}

async function updateOpenseaTokenJobs(task: ITask, tokenIds: (number | string)[], offerPrice: number, expiry: number, maxBidPriceEth: number, creatorFees: IFee, collectionDetails: any, minBidPriceEth: number) {

  try {
    const jobs = tokenIds
      .filter(token => !isNaN(Number(token)))
      .map((token) => ({
        name: OPENSEA_TOKEN_BID,
        data: {
          _id: task._id,
          address: task.wallet.address,
          privateKey: task.wallet.privateKey,
          slug: task.contract.slug,
          creatorFees,
          enforceCreatorFee: collectionDetails.enforceCreatorFee,
          asset: { contractAddress: task.contract.contractAddress, tokenId: token },
          expiry,
          outbidOptions: task.outbidOptions,
          task
        },
        opts: { priority: TOKEN_BID_PRIORITY.OPENSEA }
      }));

    const name = `${task._id}-${OPENSEA_TOKEN_BID}`

    await addBatchedFlow(name, jobs, TOKEN_BID_PRIORITY.OPENSEA);

  } catch (error) {
    console.error(RED + `Error updating opensea token jobs: ${task.contract.slug} ` + RESET, error);
  }

}




async function processMagicEdenTraitJobs(task: ITask, traits: {
  attributeKey: string;
  attributeValue: string;
}[], offerPrice: number, expiration: number) {

  try {
    const traitJobs = traits.map((trait) => ({
      name: MAGICEDEN_TRAIT_BID,
      data: {
        _id: task._id,
        address: task.wallet.address,
        contractAddress: task.contract.contractAddress,
        quantity: 1,
        offerPrice,
        expiration,
        privateKey: task.wallet.privateKey,
        slug: task.contract.slug,
        trait,
        outbidOptions: task.outbidOptions,
        task
      },
      opts: { priority: TRAIT_BID_PRIORITY.MAGICEDEN }
    }));

    const name = `${task._id}-${MAGICEDEN_TRAIT_BID}`

    await addBatchedFlow(name, traitJobs, TRAIT_BID_PRIORITY.MAGICEDEN);

  } catch (error) {
    console.error(RED + `Error updating magiceden trait jobs: ${task.contract.slug} ` + RESET, error);
  }
}

async function processOpenseaTraitJobs(task: ITask, traits: {
  [key: string]: string;
}[], offerPrice: bigint, expiry: number, creatorFees: IFee, collectionDetails: any) {

  try {
    const traitJobs = traits.map((trait) => ({
      name: OPENSEA_TRAIT_BID,
      data: {
        _id: task._id,
        address: task.wallet.address,
        privateKey: task.wallet.privateKey,
        slug: task.contract.slug,
        contractAddress: task.contract.contractAddress,
        creatorFees,
        enforceCreatorFee: collectionDetails.enforceCreatorFee,
        trait: JSON.stringify(trait),
        expiry,
        outbidOptions: task.outbidOptions,
        task
      },
      opts: { priority: TRAIT_BID_PRIORITY.OPENSEA }
    }));

    const name = `${task._id}-${OPENSEA_TRAIT_BID}`
    await addBatchedFlow(name, traitJobs, TRAIT_BID_PRIORITY.OPENSEA);

  } catch (error) {
    console.error(RED + `Error updating opensea trait jobs: ${task.contract.slug} ` + RESET, error);
  }
}

async function processBlurTraitJobs(task: ITask, traits: {
  [key: string]: string;
}[], offerPrice: bigint, expiry: number, maxBidPriceEth: number, minBidPriceEth: number) {
  try {
    const traitJobs = traits.map((trait) => ({
      name: BLUR_TRAIT_BID,
      data: {
        _id: task._id,
        address: task.wallet.address,
        privateKey: task.wallet.privateKey,
        contractAddress: task.contract.contractAddress,
        offerPrice: offerPrice.toString(),
        slug: task.contract.slug,
        trait: JSON.stringify(trait),
        expiry: expiry,
        outbidOptions: task.outbidOptions,
        maxBidPriceEth: maxBidPriceEth,
        minBidPriceEth: minBidPriceEth
      },
      opts: { priority: TRAIT_BID_PRIORITY.BLUR }
    }));

    const name = `${task._id}-${BLUR_TRAIT_BID}`

    await addBatchedFlow(name, traitJobs, TRAIT_BID_PRIORITY.BLUR);
  } catch (error) {
    console.error(RED + `Error updating blur trait jobs: ${task.contract.slug} ` + RESET, error);
  }
}

async function startTask(task: ITask, start: boolean) {
  const API_KEY = process.env.API_KEY
  if (!API_KEY) {
    console.log(RED + 'API_KEY is not set' + RESET);
    return
  }

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


  const taskDoc = await Task.findById(task._id).select('balance').exec();
  await BidLogs.deleteMany({ taskId: task._id });
  const balance = taskDoc?.balance;
  balances[task._id] = balance

  const RATE_LIMIT = process.env.RATE_LIMIT
  if (!RATE_LIMIT) {
    console.log(RED + 'RATE_LIMIT is not set' + RESET);
    return
  }

  const taskId = task._id.toString();

  // Try to acquire lock
  const locked = await lockTask(taskId);
  if (!locked) {
    console.log(`Task ${taskId} is already running, skipping start`);
    return;
  }

  try {
    await removeTaskFromAborted(taskId);
    const updatedTask = { ...task, running: true };
    activeTasks.set(taskId, updatedTask);
    const taskIndex = currentTasks.findIndex(t => t._id === task._id);
    if (taskIndex !== -1) {
      currentTasks[taskIndex].running = true;
    }
    console.log(GREEN + `Updated task ${task.contract.slug} running status to: ${start} `.toUpperCase() + RESET);
    if (task.outbidOptions.counterbid) {
      try {
        if (!ws || ws.readyState !== WebSocket.OPEN) {
          await connectWebSocket();
        }

        console.log("subscribing to collection: ", task.contract.slug);
        const newTask = { ...task, running: true };
        await subscribeToCollections([newTask]);
      } catch (error) {
        console.error(RED + `Error subscribing to collection ${task.contract.slug}: ` + RESET, error);
      }
    }

    const jobs = [
      ...(task.selectedMarketplaces.map(m => m.toLowerCase()).includes("opensea") ? [{ name: OPENSEA_SCHEDULE, data: { ...task, running: start } }] : []),
      ...(task.selectedMarketplaces.map(m => m.toLowerCase()).includes("blur") ? [{ name: BLUR_SCHEDULE, data: { ...task, running: start } }] : []),
      ...(task.selectedMarketplaces.map(m => m.toLowerCase()).includes("magiceden") ? [{ name: MAGICEDEN_SCHEDULE, data: { ...task, running: start } }] : []),
    ];
    await queue.addBulk(jobs);
  } catch (error) {
    console.error(RED + `Error starting task ${taskId}: ` + RESET, error);
  } finally {
    // Always release lock when done
    unlockTask(taskId);
  }
}


async function stopTask(task: ITask, start: boolean, marketplace?: string) {
  const taskId = task._id.toString();

  try {
    if (!marketplace || task.selectedMarketplaces.length === 0) {
      markTaskAsAborted(taskId);
      const updatedTask = { ...task, running: false };
      activeTasks.set(taskId, updatedTask);
    }
    if (!skipStats[taskId]) {
      skipStats[taskId] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      };
    }

    if (!errorStats[taskId]) {
      errorStats[taskId] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      };
    }

    if (marketplace) {
      skipStats[taskId][marketplace.toLowerCase() as 'opensea' | 'magiceden' | 'blur'] = 0;
      errorStats[taskId][marketplace.toLowerCase() as 'opensea' | 'magiceden' | 'blur'] = 0;
    } else {
      // Reset all stats when no specific marketplace is provided
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

    await updateTaskStatus(task, start, marketplace);

    if (task.outbidOptions.counterbid && !marketplace) {
      try {
        await unsubscribeFromCollection(task);
      } catch (error) {
        console.error(RED + `Error unsubscribing from collection for task ${task.contract.slug}: ` + RESET, error);
      }
    }

    await cancelAllRelatedBids(task, marketplace)
    await removePendingAndWaitingBids(task, marketplace)
    setTimeout(async () => {
      await cancelAllRelatedBids(task, marketplace)
    }, 5000)
    await waitForRunningJobsToComplete(task, marketplace)

    // Schedule final cancellation after 10 seconds
    setTimeout(async () => {
      await cancelAllRelatedBids(task, marketplace)
    }, 5000)

  } catch (error) {
    console.error(RED + `Error in stopTask for ${task.contract.slug}: ` + RESET, error);
    throw error;
  }
}


async function removePendingAndWaitingBids(task: ITask, marketplace?: string) {
  try {
    let jobNames: string[] = []
    switch (marketplace?.toLowerCase()) {
      case OPENSEA.toLowerCase():
        jobNames = getJobNames(OPENSEA.toLowerCase(), task)
        break;
      case MAGICEDEN.toLowerCase():
        jobNames = getJobNames(MAGICEDEN.toLowerCase(), task)
        break;
      case BLUR.toLowerCase():
        jobNames = getJobNames(BLUR.toLowerCase(), task)
        break;
      default:
        jobNames = [
          ...getJobNames(BLUR.toLowerCase(), task),
          ...getJobNames(MAGICEDEN.toLowerCase(), task),
          ...getJobNames(OPENSEA.toLowerCase(), task)
        ]
    }
    const jobs: Job[] = await queue.getJobs(['prioritized', 'waiting', 'paused', 'waiting-children']);
    const activeJobs = await queue.getJobs(['active']);

    if (activeJobs && activeJobs.length > 0) {
      const relatedJobs = activeJobs.filter(job => job && job.data && job.data._id === task._id);
      console.log({ relatedJobs });
      if (relatedJobs && relatedJobs.length > 0) {
        await Promise.all(relatedJobs.map(async job => {
          if (job) {
            await job.moveToFailed(new Error("Task was stopped"), "failed", true).catch(() => { });
          }
        }));
      }
    }

    // Filter jobs first, then remove them in parallel
    const jobsToRemove = jobs.filter(job => jobNames.includes(job.name));

    if (jobsToRemove.length > 0) {
      console.log(`Removing ${jobsToRemove.length} pending jobs for ${task.contract.slug}`);
      await Promise.all(jobsToRemove.map(async job => {
        await job.remove({ removeChildren: true });
        console.log(`Removed job: ${job.name}`);
      }));
    }

  } catch (error) {
    console.error(RED + `Error removing pending bids: ${error} ` + RESET);
  }
}

async function waitForRunningJobsToComplete(task: ITask, marketplace?: string) {
  try {
    const checkInterval = 1000;
    const jobNames = [
      ...getJobNames(BLUR.toLowerCase(), task),
      ...getJobNames(MAGICEDEN.toLowerCase(), task),
      ...getJobNames(OPENSEA.toLowerCase(), task)
    ]

    console.log({ jobNames });

    let jobnames: string[] = [OPENSEA_SCHEDULE, OPENSEA_TRAIT_BID, OPENSEA_TOKEN_BID, MAGICEDEN_SCHEDULE, MAGICEDEN_TRAIT_BID, MAGICEDEN_TOKEN_BID, BLUR_SCHEDULE]

    switch (marketplace?.toLowerCase()) {
      case OPENSEA.toLowerCase():
        jobnames = [OPENSEA_SCHEDULE, OPENSEA_TRAIT_BID, OPENSEA_TOKEN_BID]
        break;

      case MAGICEDEN.toLowerCase():
        jobnames = [MAGICEDEN_SCHEDULE, MAGICEDEN_TRAIT_BID, MAGICEDEN_TOKEN_BID]
        break;

      case BLUR.toLowerCase():
        jobnames = [BLUR_SCHEDULE, BLUR_TRAIT_BID]
        break;
      default:
        jobnames = [OPENSEA_SCHEDULE, OPENSEA_TRAIT_BID, OPENSEA_TOKEN_BID, MAGICEDEN_SCHEDULE, MAGICEDEN_TRAIT_BID, MAGICEDEN_TOKEN_BID, BLUR_SCHEDULE, BLUR_TRAIT_BID]
    }

    console.log(YELLOW + 'Waiting for running jobs to complete...'.toUpperCase() + RESET);

    while (true) {
      await queue.pause();

      const activeJobs = await queue.getJobs(['active', 'waiting-children']);
      const relatedJobs = activeJobs?.filter(job => jobNames.includes(job?.name));

      if (relatedJobs?.length === 0) {
        await queue.resume()
        break;
      }
      await new Promise(resolve => setTimeout(resolve, checkInterval));
    }

  } catch (error: any) {
    console.error(RED + `Error waiting for running jobs to complete for task ${task.contract.slug}: ${error.message}` + RESET);
  }
}

async function updateTaskStatus(task: ITask, running: boolean, marketplace?: string) {
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




const CANCEL_BATCH_SIZE = 1000; // Define batch size constant

async function cancelAllRelatedBids(task: ITask, marketplace?: string, attempt: number = 1): Promise<void> {
  try {
    const { openseaBids, magicedenBids, blurBids } = await getAllRelatedBids(task);

    const bidsToCancel = {
      opensea: (!marketplace || marketplace.toLowerCase() === OPENSEA.toLowerCase()) ? openseaBids : [],
      magiceden: (!marketplace || marketplace.toLowerCase() === MAGICEDEN.toLowerCase()) ? magicedenBids : [],
      blur: (!marketplace || marketplace.toLowerCase() === BLUR.toLowerCase()) ? blurBids : [],
    };

    for (const [marketplace, bids] of Object.entries(bidsToCancel)) {
      if (bids.length > 0) {
        try {
          for (let i = 0; i < bids.length; i += CANCEL_BATCH_SIZE) {
            const batchBids = bids.slice(i, i + CANCEL_BATCH_SIZE);

            switch (marketplace) {
              case "opensea":
                await cancelOpenseaBids(batchBids, task.wallet.privateKey, task._id);
                break;
              case "magiceden":
                await cancelMagicedenBids(batchBids, task.wallet.privateKey, task._id);
                break;
              case "blur":
                await cancelBlurBids(batchBids, task.wallet.privateKey, task._id);
                break;
            }
            await new Promise(resolve => setTimeout(resolve, 1000));
          }
        } catch (error) {
          console.error(RED + `Error cancelling bids for ${marketplace}: `, error, RESET);
        }
      }
    }

  } catch (error) {
    console.error(RED + `Unexpected error during bid cancellation(attempt ${attempt}): `, error, RESET);
  }
}


async function getAllRelatedBids(task: ITask) {
  try {
    const taskId = task._id

    const magicedenOrderTrackingKey = `{${taskId}}:magiceden:orders`;
    const magicedenOrderKeys = await redis.smembers(magicedenOrderTrackingKey);

    const openseaOrderTrackingKey = `{${taskId}}:opensea:orders`;
    const openseaOrderKeys = await redis.smembers(openseaOrderTrackingKey);

    const blurOrderTrackingKey = `{${taskId}}:blur:orders`;
    const blurOrderKeys = await redis.smembers(blurOrderTrackingKey);

    return { openseaBids: openseaOrderKeys, magicedenBids: magicedenOrderKeys, blurBids: blurOrderKeys };

  } catch (error) {
    console.log(RED + `Error getting all related bids for task ${task._id}: `, error, RESET);
    return { openseaBids: [], magicedenBids: [], blurBids: [] };
  }
}

async function cancelOpenseaBids(orderKeys: string[], privateKey: string, taskId: string) {
  if (!orderKeys.length) return
  const bidData = await redis.mget(orderKeys);
  const cancelData = bidData.map((order, index) => {
    if (!order) return
    const parsed = JSON.parse(order);
    return {
      name: CANCEL_OPENSEA_BID,
      data: { privateKey, orderId: parsed.orderId, taskId, orderKey: orderKeys[index] },
    }
  }).filter((item) => item !== undefined)
  await queue.addBulk(cancelData)
}

async function extractMagicedenOrderHash(orderKeys: string[]): Promise<string[]> {

  if (!orderKeys.length) return []
  const bidData = await redis.mget(orderKeys);
  const orderData = bidData.map((order, index) => {
    if (!order) return
    const parsed = JSON.parse(order);
    return parsed.payload
  })

  const extractedOrderIds = orderData
    .map(bid => {
      if (!bid) return null;
      try {
        const parsed = JSON.parse(bid);

        if (parsed.results) {
          return parsed.results[0].orderId;
        }
        if (parsed.message && parsed.orderId) {
          return parsed.orderId;
        }
        return null;
      } catch (e) {
        console.error('Error parsing bid data:', e);
        return null;
      }
    })
    .filter(id => id !== null);

  return extractedOrderIds as string[];
}

async function cancelMagicedenBids(orderKeys: string[], privateKey: string, taskId: string) {
  if (!orderKeys.length) return;
  for (let i = 0; i < orderKeys.length; i += 500) {
    const batchKeys = orderKeys.slice(i, i + 500);
    const extractedOrderIds = await extractMagicedenOrderHash(batchKeys)
    await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey, orderKeys: batchKeys, taskId });
  }
}

async function cancelBlurBids(orderKeys: string[], privateKey: string, taskId: string) {
  if (!orderKeys.length) return
  const orderData = await redis.mget(orderKeys);
  const cancelData = orderData.map((orderKey, index) => {
    if (!orderKey) return
    const order = JSON.parse(orderKey);
    const payload = order.payload
    return {
      name: CANCEL_BLUR_BID,
      data: { privateKey, payload, taskId, orderKey: orderKeys[index] }
    }
  }).filter((item) => item !== undefined)
  await queue.addBulk(cancelData);
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

let wsConnectionStatus: 'connected' | 'disconnected' | 'connecting' = 'disconnected';
let pingIntervalId: NodeJS.Timeout | null = null;



async function connectWebSocket(): Promise<void> {

  const clientIds = [...new Set(currentTasks.filter(task => task.running).map(task => task.user.toString()))];
  const clientId = clientIds[0] ?? "";

  // Check if already connected
  if (ws?.readyState === WebSocket.OPEN) {
    console.log(YELLOW + "WebSocket already connected, sending ping to verify..." + RESET);
    ws.send(JSON.stringify({ event: 'ping', clientId }));
    return;
  }

  wsConnectionStatus = 'connecting';
  ws = new WebSocket(MARKETPLACE_WS_URL);

  ws.addEventListener("open", async function open() {
    wsConnectionStatus = 'connected';
    retryCount = 0;
    // Send custom ping messages at regular intervals
    if (pingIntervalId !== null) {
      clearTimeout(pingIntervalId);
      pingIntervalId = null;
    }
    pingIntervalId = setInterval(() => {
      if (ws.readyState === WebSocket?.OPEN) {
        ws.send(JSON.stringify({ event: 'ping', clientId })); // Custom ping message
        console.log('-----PING------');
      }
    }, 30000); // 15 seconds interval
    clients.forEach(client => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(JSON.stringify({ type: 'wsConnectionStatus', data: wsConnectionStatus }));
      }
    });
    console.log(GOLD + "CONNECTED TO MARKETPLACE EVENTS WEBSOCKET" + RESET);

    // Clear existing timeouts/intervals
    if (reconnectTimeoutId !== null) {
      clearTimeout(reconnectTimeoutId);
      reconnectTimeoutId = null;
    }

    if (heartbeatIntervalId !== null) {
      clearInterval(heartbeatIntervalId);
    }

    await subscribeToCollections(currentTasks as unknown as ITask[]);

    // Set up reconnection check
    heartbeatIntervalId = setInterval(() => {
      if (ws.readyState !== WebSocket.OPEN) {
        wsConnectionStatus = 'disconnected';

        clients.forEach(client => {
          if (client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify({ type: 'wsConnectionStatus', data: wsConnectionStatus }));
          }
        });
        console.log(YELLOW + "WebSocket connection lost, attempting reconnect..." + RESET);
        ws.close();
        connectWebSocket();
      }
    }, 30000);

    ws.on("message", async function incoming(data: string) {
      try {
        const message = JSON.parse(data.toString())
        if (message.type === 'pong') {
          console.log('Pong received from server.');
        }
        await handleCounterBid(message);
      } catch (error) {
      }
    });
  });

  ws.addEventListener("close", function close() {
    wsConnectionStatus = 'disconnected';

    clients.forEach(client => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(JSON.stringify({ type: 'wsConnectionStatus', data: wsConnectionStatus }));
      }
    });

    console.log(RED + "DISCONNECTED FROM MARKETPLACE EVENTS WEBSCKET" + `_TIME_AT_${String(new Date().getDate()).padStart(2, '0')} / ${String(new Date().getHours()).padStart(2, '0')}:${String(new Date().getMinutes()).padStart(2, '0')}` + RESET);
    if (heartbeatIntervalId !== null) {
      clearInterval(heartbeatIntervalId);
      heartbeatIntervalId = null;
    }
    attemptReconnect();
  });

  ws.addEventListener("error", function error(err) {
    wsConnectionStatus = 'disconnected';
    clients.forEach(client => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(JSON.stringify({ type: 'wsConnectionStatus', data: wsConnectionStatus }));
      }
    });
    attemptReconnect();
  });

}

function attemptReconnect(): void {
  if (retryCount < MAX_RETRIES) {
    if (reconnectTimeoutId !== null) {
      clearTimeout(reconnectTimeoutId);
    }
    let delay: number = Math.pow(2, retryCount) * 1000;
    console.log(`Attempting to reconnect in ${delay / 1000} seconds...`);
    reconnectTimeoutId = setTimeout(connectWebSocket, delay);
    retryCount++;
  } else {
    console.log("Max retries reached. Giving up on reconnecting.");
  }
}


async function handleCounterBid(message: any) {
  try {
    const { contractAddress, slug } = getMarketplaceDetails(message);

    if (!contractAddress && !slug) {
      console.log("Missing contractAddress and slug in message");
      return;
    }

    const relevantTasks = currentTasks.filter(task =>
      task?.outbidOptions?.counterbid &&
      task?.running &&
      (task?.contract?.contractAddress?.toLowerCase() === contractAddress?.toLowerCase() ||
        task?.contract?.slug?.toLowerCase() === slug?.toLowerCase())
    );

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
    const domain: string = data?.data?.source?.domain
    const wallets = await (await Wallet.find({}, { address: 1, _id: 0 }).exec()).map((wallet) => wallet.address.toLowerCase())

    const maker = data?.data?.maker?.toLowerCase()

    if (wallets.includes(maker)) return

    const expiry = getExpiry(task.bidDuration)
    const duration = expiry / 60 || 15;
    const currentTime = new Date().getTime();
    const expiration = Math.floor((currentTime + (duration * 60 * 1000)) / 1000);
    let floor_price: number = 0

    if (task.bidPrice.minType == "eth" || task.openseaBidPrice.minType === "eth") {
      floor_price = 0
    } else {
      floor_price = Number(await fetchMagicEdenCollectionStats(task._id, task.contract.contractAddress))
    }


    if (!warningBids[task._id]) {
      warningBids[task._id] = {
        opensea: false,
        magiceden: false,
        blur: false
      }
    }

    const balance = balances[task._id]
    warningBids[task._id].magiceden = false;
    const stopBid = await stopOption(task, 'magiceden', floor_price, balance)

    if (stopBid) return


    const { maxBidPriceEth, minBidPriceEth } = await calculateBidPrice(task, floor_price as number, "magiceden")

    const magicedenOutbidMargin = task?.outbidOptions?.magicedenOutbidMargin || 0.0001


    const bidType = data?.data?.criteria?.kind
    const tokenId = +data?.data?.criteria?.data?.token?.tokenId;

    const incomingBidAmount: number = Number(data?.data?.price?.amount?.raw);
    let offerPrice: number;

    const incomingBidAmountEth = incomingBidAmount / 1e18;

    if (incomingBidAmountEth < minBidPriceEth || incomingBidAmountEth > maxBidPriceEth) {
      const reason = incomingBidAmountEth < minBidPriceEth
        ? `is less than min bid price ${minBidPriceEth} WETH`
        : `is greater than max bid price ${maxBidPriceEth} WETH`;
      const message = `incoming bid ${incomingBidAmountEth} WETH for ${task.contract.slug} ${reason}. Skipping ...`.toUpperCase()
      await logBidError(task._id, "INVALID BID PRICE", message, "skipped", "magiceden");
      return
    }

    const selectedTraits = transformNewTask(task.selectedTraits)

    const bestOffer = (bestOffers[task._id]?.magiceden || 0) * 1e18;

    if (incomingBidAmount < bestOffer) return;

    const collectionOffer = bidType === "collection" ? incomingBidAmount / 1e18 : 0;

    if (collectionOffer > bestOffer) {
      bestOffers[task._id].magiceden = collectionOffer;
    }

    if (bidType === "token") {
      const currentTask = activeTasks.get(task._id)
      if (!currentTask?.running || !currentTask.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes(MAGICEDEN.toLowerCase())) return

      const tokenIds = task.tokenIds.filter(id => !isNaN(Number(id)));
      const tokenBid = task.bidType === "token" && task.tokenIds.length > 0

      if (!tokenBid) return
      console.log(MAGENTA + '----------------------------------------------------------------------------------' + RESET);
      console.log(MAGENTA + `incoming bid for ${task.contract.slug}:${tokenId} for ${incomingBidAmount / 1e18} WETH on magiceden`.toUpperCase() + RESET);
      console.log(MAGENTA + '----------------------------------------------------------------------------------' + RESET);

      if (!tokenIds.includes(tokenId)) return

      const orderTrackingKey = `{${task._id}}:magiceden:orders`;
      const orderKeys = await getPatternKeys(orderTrackingKey, tokenId.toString()) || []

      const orderData = orderKeys.length ? await redis.mget(orderKeys) : []
      const offers = orderData.map((order: any) => {
        if (order.offer) return
        const parsed = JSON.parse(order)
        return parsed.offer
      })
      const currentBidPrice = !offers.length ? 0 : Math.max(...offers.map((offer) => Number(offer)))
      if (incomingBidAmount < currentBidPrice) return

      const absoluteHighestBidAmount = Math.max(bestOffer, incomingBidAmount, (minBidPriceEth * 1e18))
      const absoluteHighestBidAmountEth = absoluteHighestBidAmount / 1e18
      const magicedenOutbidMargin = calculateOutbidMargin(absoluteHighestBidAmountEth)
      const roundedEth = roundToValidPrecision(absoluteHighestBidAmountEth)

      if (absoluteHighestBidAmount < currentBidPrice || incomingBidAmount < absoluteHighestBidAmount) return

      offerPrice = Math.ceil((magicedenOutbidMargin * 1e18) + roundedEth)

      const offerPriceEth = offerPrice / 1e18


      if (offerPriceEth < absoluteHighestBidAmountEth) {
        offerPrice = (absoluteHighestBidAmountEth + magicedenOutbidMargin) * 1e18
      }

      if (maxBidPriceEth > 0 && offerPriceEth > maxBidPriceEth) {

        const message = `magiceden counter offer ${offerPriceEth} WETH for ${task.contract.slug} ${tokenId} exceeds max bid price ${maxBidPriceEth} WETH ON MAGICEDEN.Skipping ...`.toUpperCase()

        await logBidError(task._id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "magiceden");

        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
        console.log(RED + message + RESET);

        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);


        if (!skipStats[task._id]) {
          skipStats[task._id] = {
            opensea: 0,
            magiceden: 0,
            blur: 0
          };
        }
        skipStats[task._id]['magiceden']++;
        if (orderKeys.length > 0) {
          const extractedOrderIds = await extractMagicedenOrderHash(orderKeys)
          await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey: task.wallet.privateKey, orderKeys: orderKeys, taskId: task._id });
        }
        return;
      }

      const bidData: IMagicedenTokenBidData = {
        _id: task._id,
        address: task.wallet.address,
        contractAddress: task.contract.contractAddress,
        quantity: 1,
        offerPrice: Number(offerPrice),
        tokenId: tokenId,
        expiration: expiration.toString(),
        privateKey: task.wallet.privateKey,
        slug: task.contract.slug,
        outbidOptions: task.outbidOptions,
        maxBidPriceEth,
        minBidPriceEth,
        task
      }


      const jobId = `counterbid-${task._id}-${task.contract.slug}-${MAGICEDEN.toLowerCase()}-${tokenId}`
      const job: Job = await queue.getJob(jobId)

      if (job) {
        if (incomingBidAmount <= Number(job.data.offerPrice)) {
          console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
          console.log(RED + `Skipping counterbid since incoming bid ${incomingBidAmount / 1e18} WETH is less than or equal to existing bid ${Number(job.data.offerPrice) / 1e18} WETH Domain: ${domain} ` + RESET);
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
      console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(GREEN + `Counterbidding incoming magiceden offer of ${Number(incomingBidAmount) / 1e18} WETH for ${task.contract.slug} ${tokenId} for ${Number(offerPrice) / 1e18} WETH ON MAGICEDEN`.toUpperCase() + RESET);
      console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
      if (orderKeys.length > 0) {
        const extractedOrderIds = await extractMagicedenOrderHash(orderKeys)
        await queue.add(CANCEL_MAGICEDEN_BID, {
          orderIds: extractedOrderIds,
          privateKey: task.wallet.privateKey,
          orderKeys,
          taskId: task._id
        });
      }
    }
    if (bidType === "attribute") {

      const traitBid = task.bidType === "collection" && selectedTraits && Object.keys(selectedTraits).length > 0
      const trait = {
        attributeKey: data?.data?.criteria?.data?.attribute?.key,
        attributeValue: data?.data?.criteria?.data?.attribute?.value,
      }

      const hasTraits = checkMagicEdenTrait(selectedTraits, trait)
      if (!hasTraits || !traitBid) return

      console.log(MAGENTA + '----------------------------------------------------------------------------------' + RESET);
      console.log(MAGENTA + `incoming bid for ${task.contract.slug} ${JSON.stringify(trait)} for ${incomingBidAmount / 1e18} WETH on magiceden`.toUpperCase() + RESET);
      console.log(MAGENTA + '----------------------------------------------------------------------------------' + RESET);

      const orderTrackingKey = `{${task._id}}:magiceden:orders`;
      const orderKeys = await getPatternKeys(orderTrackingKey, `${trait.attributeKey}:${trait.attributeValue}`) || []
      const orderData = orderKeys.length ? await redis.mget(orderKeys) : []
      const offers = orderData.map((order: any) => {
        if (order.offer) return
        const parsed = JSON.parse(order)
        return parsed.offer
      })

      const currentBidPrice = !offers.length ? 0 : Math.max(...offers.map((offer) => Number(offer)))

      if (incomingBidAmount < currentBidPrice) return

      offerPrice = Math.ceil((magicedenOutbidMargin * 1e18) + incomingBidAmount)
      const offerPriceEth = offerPrice / 1e18

      if (offerPriceEth < minBidPriceEth) {
        offerPrice = minBidPriceEth * 1e18
      }
      if (maxBidPriceEth > 0 && offerPriceEth > maxBidPriceEth) {
        const message = `magiceden trait counter offer ${offerPriceEth} WETH for ${task.contract.slug} ${JSON.stringify(trait)} exceeds max bid price ${maxBidPriceEth} WETH ON MAGICEDEN.Skipping ...`.toUpperCase()
        await logBidError(task._id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "magiceden");
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
        console.log(RED + message + RESET);
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);

        if (!skipStats[task._id]) {
          skipStats[task._id] = {
            opensea: 0,
            magiceden: 0,
            blur: 0
          };
        }
        skipStats[task._id]['magiceden']++;
        if (orderKeys.length > 0) {
          const extractedOrderIds = await extractMagicedenOrderHash(orderKeys)
          await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey: task.wallet.privateKey, orderKeys: orderKeys, taskId: task._id });
        }
        return;
      }

      const bidData: IMagicedenTraitBidData = {
        taskId: task._id,
        bidCount: getIncrementedBidCount(MAGICEDEN, task.contract.slug, task._id),
        address: task.wallet.address,
        contractAddress: task.contract.contractAddress,
        quantity: 1,
        offerPrice: offerPrice,
        expiration: expiration.toString(),
        privateKey: task.wallet.privateKey,
        slug: task.contract.slug,
        trait: JSON.stringify(trait)
      }



      const jobId = `counterbid-${task._id}-${task.contract.slug}-${MAGICEDEN.toLowerCase()}-${JSON.stringify(trait)}`
      const job: Job = await queue.getJob(jobId)

      if (job) {
        if (incomingBidAmount <= job.data.offerPrice) {
          console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
          console.log(RED + `Skipping counterbid since incoming bid ${incomingBidAmount / 1e18} WETH is less than or equal to existing bid ${Number(job.data.offerPrice) / 1e18} WETH Domain: ${domain} ` + RESET);
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

      console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(GREEN + `Counterbidding incoming offer of ${Number(incomingBidAmount) / 1e18} WETH for ${task.contract.slug} trait ${JSON.stringify(trait)} for ${Number(offerPrice) / 1e18} WETH ON MAGICEDEN `.toUpperCase() + RESET);
      console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
      if (orderKeys.length > 0) {
        const extractedOrderIds = await extractMagicedenOrderHash(orderKeys)
        await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey: task.wallet.privateKey, orderKeys: orderKeys, taskId: task._id });
      }


    }
    if (bidType === "collection") {
      if (maker === task.wallet.address.toLowerCase()) return

      const isTraitBid = task.bidType === "collection" && selectedTraits && Object.keys(selectedTraits).length > 0
      const tokenBid = task.bidType === "token" && task.tokenIds.length > 0
      const collectionBid = !isTraitBid && !tokenBid

      if (!collectionBid) return

      console.log(MAGENTA + '----------------------------------------------------------------------------------' + RESET);
      console.log(MAGENTA + `incoming collection offer for ${task.contract.slug} for ${incomingBidAmount / 1e18} WETH on magiceden`.toUpperCase() + RESET);
      console.log(MAGENTA + '----------------------------------------------------------------------------------' + RESET);


      const orderTrackingKey = `{${task._id}}:magiceden:orders`;
      const orderKeys = await getPatternKeys(orderTrackingKey, 'collection') || []
      const orderData = orderKeys.length ? await redis.mget(orderKeys) : []
      const offers = orderData.map((order: any) => {
        if (order.offer) return
        const parsed = JSON.parse(order)
        return parsed.offer
      })
      const currentBidPrice = !offers.length ? 0 : Math.max(...offers.map((offer) => Number(offer)))
      const bestOffer = bestOffers[task._id].magiceden * 1e18;
      const absoluteHighestBidAmount = Math.max(bestOffer, incomingBidAmount)

      if (incomingBidAmount < currentBidPrice || incomingBidAmount < absoluteHighestBidAmount) return

      const absoluteHighestBidAmountEth = absoluteHighestBidAmount / 1e18
      const magicedenOutbidMargin = calculateOutbidMargin(absoluteHighestBidAmountEth)
      const roundedEth = roundToValidPrecision(absoluteHighestBidAmountEth)


      offerPrice = Math.ceil((magicedenOutbidMargin * 1e18) + (roundedEth * 1e18))
      const offerPriceEth = offerPrice / 1e18

      if (offerPriceEth < minBidPriceEth) {
        offerPrice = minBidPriceEth * 1e18
      }

      if (maxBidPriceEth > 0 && offerPriceEth > maxBidPriceEth) {
        if (!skipStats[task._id]) {
          skipStats[task._id] = {
            opensea: 0,
            magiceden: 0,
            blur: 0
          };
        }
        skipStats[task._id]['magiceden']++;
        const message = `counter Offer ${offerPriceEth} WETH for ${task.contract.slug}  exceeds max bid price ${maxBidPriceEth} WETH.Skipping ...`.toUpperCase()
        await logBidError(task._id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "magiceden");
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
        console.log(RED + message + RESET);
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);

        if (orderKeys.length > 0) {
          const extractedOrderIds = await extractMagicedenOrderHash(orderKeys)
          await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey: task.wallet.privateKey, orderKeys: orderKeys, taskId: task._id });
        }
        return;
      }

      const bidCount = getIncrementedBidCount(MAGICEDEN, task.contract.slug, task._id)

      const bidData: IMagicedenCollectionBidData = {
        _id: task._id,
        bidCount,
        address: task.wallet.address,
        contractAddress: task.contract.contractAddress,
        quantity: 1,
        offerPrice: offerPrice,
        expiration: expiration.toString(),
        privateKey: task.wallet.privateKey,
        slug: task.contract.slug,
      }



      const jobId = `counterbid-${task._id}-${task.contract.slug}-${MAGICEDEN.toLowerCase()}-collection`
      const job: Job = await queue.getJob(jobId)

      if (job) {
        if (incomingBidAmount <= job.data.offerPrice) {
          console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
          console.log(RED + `Skipping counterbid since incoming bid ${incomingBidAmount / 1e18} WETH is less than or equal to existing bid ${Number(job.data.offerPrice) / 1e18} WETH Domain: ${domain} ` + RESET);
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

      console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(GREEN + `Counterbidding incoming magiceden offer of ${Number(incomingBidAmount) / 1e18} WETH for ${task.contract.slug} for ${Number(offerPrice) / 1e18} WETH`.toUpperCase() + RESET);
      console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);

      if (orderKeys.length > 0) {
        const extractedOrderIds = await extractMagicedenOrderHash(orderKeys)
        await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey: task.wallet.privateKey, orderKeys: orderKeys, taskId: task._id });
      }

    }
  } catch (error) {
    // console.error(RED + `Error handling MAGICEDEN counterbid: ${JSON.stringify(error)} ` + RESET);
  }
}

async function handleOpenseaCounterbid(data: any, task: ITask) {
  try {
    const maker = data?.payload?.payload?.maker?.address.toLowerCase()
    const quantity = Number(data?.payload?.payload?.quantity) || 1
    const incomingBidAmount: number = Number(data?.payload?.payload?.base_price) / quantity;
    const wallets = await (await Wallet.find({}, { address: 1, _id: 0 }).exec()).map((wallet) => wallet.address.toLowerCase())

    if (wallets.includes(maker)) return

    const expiry = getExpiry(task.bidDuration)
    let floor_price: number = 0

    if (task.bidPrice.minType == "eth" || task.openseaBidPrice.minType === "eth") {
      floor_price = 0
    } else {
      const data = await getCollectionStats(task.contract.slug);
      if (!data || data === 0) return
      floor_price = data;
    }

    if (!warningBids[task._id]) {
      warningBids[task._id] = {
        opensea: false,
        magiceden: false,
        blur: false
      }
    }
    warningBids[task._id].opensea = false;
    const stopBid = await stopOption(task, 'opensea', floor_price, balances[task._id])

    if (stopBid) return

    const { maxBidPriceEth, minBidPriceEth } = await calculateBidPrice(task, Number(floor_price), "opensea")

    const incomingBidAmountEth = incomingBidAmount / 1e18

    if (incomingBidAmountEth < minBidPriceEth || incomingBidAmountEth > maxBidPriceEth) {
      const reason = incomingBidAmountEth < minBidPriceEth
        ? `is less than min bid price ${minBidPriceEth} WETH`
        : `is greater than max bid price ${maxBidPriceEth} WETH`;
      const message = `incoming bid ${incomingBidAmountEth} WETH for ${task.contract.slug} ${reason}. Skipping ...`.toUpperCase()
      await logBidError(task._id, "INVALID BID PRICE", message, "skipped", "magiceden");
      return
    }

    const collectionDetails = await getCollectionDetails(task.contract.slug, task.contract.contractAddress);

    const creatorFees: IFee = collectionDetails.creator_fees.null !== undefined
      ? { null: collectionDetails.creator_fees.null }
      : Object.fromEntries(Object.entries(collectionDetails.creator_fees).map(([key, value]) => [key, Number(value)]));


    let offerPrice: number;
    let colletionOffer: bigint;

    const selectedTraits = transformNewTask(task.selectedTraits)
    const bestOffer = bestOffers[task._id].opensea * 1e18;

    if (incomingBidAmount < bestOffer) return

    const collectionOffer = data.event === "collection_offer" ? incomingBidAmount / 1e18 : 0

    if (collectionOffer > bestOffer) {
      bestOffers[task._id].opensea = collectionOffer;
    }


    if (data.event === "item_received_bid") {
      const currentTask = activeTasks.get(task._id)
      if (!currentTask?.running || !currentTask.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes(OPENSEA.toLowerCase())) return
      const tokenId = +data.payload.payload.protocol_data.parameters.consideration.find((item: any) => item.token.toLowerCase() === task.contract.contractAddress.toLowerCase()).identifierOrCriteria
      const tokenIds = task.tokenIds.filter(id => !isNaN(Number(id)));
      const tokenBid = task.bidType === "token" && task.tokenIds.length > 0

      if (!tokenBid) return

      if (!tokenIds.includes(tokenId)) return

      console.log(BLUE + '---------------------------------------------------------------------------------' + RESET);
      console.log(BLUE + `incoming offer for ${task.contract.slug}:${tokenId} for ${incomingBidAmount / 1e18} WETH on opensea`.toUpperCase() + RESET);
      console.log(BLUE + '---------------------------------------------------------------------------------' + RESET);

      const orderTrackingKey = `{${task._id}}:opensea:orders`;
      const orderKeys = await getPatternKeys(orderTrackingKey, tokenId.toString()) || []
      const orderData = orderKeys.length ? await redis.mget(orderKeys) : []

      const offers = orderData.map((order: any) => {
        if (order.offer) return
        const parsed = JSON.parse(order)
        return parsed.offer
      })

      const currentBidPrice = Math.max(...offers.map((offer) => Number(offer)))
      const bestOffer = bestOffers[task._id].opensea * 1e18;

      if (incomingBidAmount < currentBidPrice) return

      const absoluteHighestBidAmount = Math.max(bestOffer, incomingBidAmount, (minBidPriceEth * 1e18))
      const absoluteHighestBidAmountEth = absoluteHighestBidAmount / 1e18
      const openseaOutbidMargin = calculateOutbidMargin(absoluteHighestBidAmountEth)
      const roundedEth = roundToValidPrecision(absoluteHighestBidAmountEth)

      if (absoluteHighestBidAmount < currentBidPrice || incomingBidAmount < absoluteHighestBidAmount) return

      offerPrice = Math.ceil((openseaOutbidMargin * 1e18) + (roundedEth * 1e18))
      const offerPriceEth = offerPrice / 1e18

      if (offerPriceEth < absoluteHighestBidAmountEth) {
        offerPrice = (absoluteHighestBidAmountEth + openseaOutbidMargin) * 1e18
      }

      if (maxBidPriceEth > 0 && offerPriceEth > maxBidPriceEth) {
        if (!skipStats[task._id]) {
          skipStats[task._id] = {
            opensea: 0,
            magiceden: 0,
            blur: 0
          };
        }
        skipStats[task._id]['opensea']++;
        const message = `counter offer ${offerPriceEth} WETH for ${task.contract.slug}:${tokenId}  exceeds max bid price ${maxBidPriceEth} WETH ON OPENSEA.Skipping ...`.toUpperCase()
        await logBidError(task._id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "opensea");
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
        console.log(RED + message + RESET);
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);

        if (orderKeys.length > 0) {
          const bidData = await redis.mget(orderKeys);
          const cancelData = bidData.map((order, index) => {
            if (!order) return
            const parsed = JSON.parse(order);
            return {
              name: CANCEL_OPENSEA_BID,
              data: { privateKey: task.wallet.privateKey, orderId: parsed.orderId, taskId: task._id, orderKey: orderKeys[index] },
            }
          }).filter((item) => item !== undefined)
          await queue.addBulk(cancelData)
        }
        return;
      }
      colletionOffer = BigInt(offerPrice)

      const asset = {
        contractAddress: task.contract.contractAddress,
        tokenId: tokenId
      }

      const bidData: IProcessOpenseaTokenBidData = {
        _id: task._id,
        address: task.wallet.address,
        privateKey: task.wallet.privateKey,
        slug: task.contract.slug,
        offerPrice: Number(colletionOffer),
        creatorFees,
        enforceCreatorFee: collectionDetails.enforceCreatorFee,
        asset,
        expiry,
        outbidOptions: task.outbidOptions,
        maxBidPriceEth,
        minBidPriceEth,
        task
      }

      const jobId = `counterbid-${task._id}-${task.contract.slug}-${OPENSEA.toLowerCase()}-${asset.tokenId}`
      const job: Job = await queue.getJob(jobId)
      if (job) {
        if (absoluteHighestBidAmount <= job.data.offerPrice) {
          console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
          console.log(RED + `Skipping counterbid since incoming bid ${absoluteHighestBidAmount / 1e18} WETH is less than or equal to existing bid ${Number(job.data.offerPrice) / 1e18} WETH` + RESET);
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



      console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(GREEN + `Counterbidding incoming opensea offer of ${Number(absoluteHighestBidAmount) / 1e18} WETH for ${task.contract.slug}:${asset.tokenId} for ${Number(colletionOffer) / 1e18} WETH ON OPENSEA`.toUpperCase() + RESET);
      console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);

      if (orderKeys.length > 0) {
        const bidData = await redis.mget(orderKeys);
        const cancelData = bidData.map((order, index) => {
          if (!order) return
          const parsed = JSON.parse(order);
          return {
            name: CANCEL_OPENSEA_BID,
            data: { privateKey: task.wallet.privateKey, orderId: parsed.orderId, taskId: task._id, orderKey: orderKeys[index] }
          }
        }).filter((item) => item !== undefined)
        await queue.addBulk(cancelData)
      }
    }

    if (data.event === "trait_offer") {
      const traitBid = task.bidType === "collection" && selectedTraits && Object.keys(selectedTraits).length > 0
      const hasTraits = checkOpenseaTrait(selectedTraits, data.payload.payload.trait_criteria)

      if (!hasTraits || !traitBid) return
      const trait = {
        type: data.payload.payload.trait_criteria.trait_type,
        value: data.payload.payload.trait_criteria.trait_name
      }

      console.log(BLUE + '---------------------------------------------------------------------------------' + RESET);
      console.log(BLUE + `incoming offer for ${task.contract.slug}:${JSON.stringify(trait)} for ${incomingBidAmount / 1e18} WETH on opensea`.toUpperCase() + RESET);
      console.log(BLUE + '---------------------------------------------------------------------------------' + RESET);

      const orderTrackingKey = `{${task._id}}:opensea:orders`;
      const orderKeys = await getPatternKeys(orderTrackingKey, `${trait.type}:${trait.value}`) || []
      const orderData = orderKeys.length ? await redis.mget(orderKeys) : []

      const offers = orderData.map((order: any) => {
        if (order.offer) return
        const parsed = JSON.parse(order)
        return parsed.offer
      })

      const currentBidPrice = Math.max(...offers.map((offer) => Number(offer)))
      const bestOffer = bestOffers[task._id].opensea * 1e18;

      if (incomingBidAmount < currentBidPrice) return

      const absoluteHighestBidAmount = Math.max(bestOffer, incomingBidAmount)
      const absoluteHighestBidAmountEth = absoluteHighestBidAmount / 1e18
      const openseaOutbidMargin = calculateOutbidMargin(absoluteHighestBidAmountEth)
      const roundedEth = roundToValidPrecision(absoluteHighestBidAmountEth)

      if (absoluteHighestBidAmount < currentBidPrice || incomingBidAmount < absoluteHighestBidAmount) return

      offerPrice = Math.ceil((openseaOutbidMargin * 1e18) + (roundedEth * 1e18))
      const offerPriceEth = offerPrice / 1e18


      if (offerPriceEth < absoluteHighestBidAmountEth) {
        offerPrice = (absoluteHighestBidAmountEth + openseaOutbidMargin) * 1e18
      }

      if (maxBidPriceEth > 0 && offerPriceEth > maxBidPriceEth) {
        if (!skipStats[task._id]) {
          skipStats[task._id] = {
            opensea: 0,
            magiceden: 0,
            blur: 0
          };
        }
        skipStats[task._id]['opensea']++;
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
        console.log(RED + `counter Offer ${offerPriceEth} WETH for ${task.contract.slug} ${JSON.stringify(trait)} exceeds max bid price ${maxBidPriceEth} WETH ON OPENSEA.Skipping ...`.toUpperCase() + RESET);
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);

        if (orderKeys.length > 0) {
          const bidData = await redis.mget(orderKeys);
          const cancelData = bidData.map((order, index) => {
            if (!order) return
            const parsed = JSON.parse(order);
            return {
              name: CANCEL_OPENSEA_BID,
              data: { privateKey: task.wallet.privateKey, orderId: parsed.orderId, taskId: task._id, orderKey: orderKeys[index] },
            }
          }).filter((item) => item !== undefined)
          await queue.addBulk(cancelData)
        }

        return;
      }

      colletionOffer = BigInt(offerPrice)
      const jobId = `counterbid-${task._id}-${task.contract.slug}-${OPENSEA.toLowerCase()}-${trait}`
      const job: Job = await queue.getJob(jobId)
      const bidCount = getIncrementedBidCount(OPENSEA, task.contract.slug, task._id)

      const counterbidData: IProcessOpenseaTraitBidData = {
        _id: task._id,
        bidCount,
        address: task.wallet.address,
        privateKey: task.wallet.privateKey,
        slug: task.contract.slug,
        offerPrice: Number(colletionOffer),
        creatorFees,
        enforceCreatorFee: collectionDetails.enforceCreatorFee,
        expiry,
        trait: JSON.stringify(trait)
      }

      if (job) {
        if (absoluteHighestBidAmount <= job.data.offerPrice) {
          console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
          console.log(RED + `Skipping counterbid since incoming bid ${absoluteHighestBidAmount / 1e18} WETH is less than or equal to existing bid ${Number(job.data.offerPrice) / 1e18} WETH` + RESET);
          console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
          return
        } else {
          await job.updateData(data)
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
      console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(GREEN + `Counterbidding incoming opensea offer of ${Number(absoluteHighestBidAmount) / 1e18} WETH for ${task.contract.slug} ${JSON.stringify(trait)} for ${Number(colletionOffer) / 1e18} WETH ON OPENSEA`.toUpperCase() + RESET);
      console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);

      if (orderKeys.length > 0) {
        const bidData = await redis.mget(orderKeys);
        const cancelData = bidData.map((order, index) => {
          if (!order) return
          const parsed = JSON.parse(order);
          return {
            name: CANCEL_OPENSEA_BID,
            data: { privateKey: task.wallet.privateKey, orderId: parsed.orderId, taskId: task._id, orderKey: orderKeys[index] },
          }
        }).filter((item) => item !== undefined)
        await queue.addBulk(cancelData)
      }

    }
    if (data.event === "collection_offer") {
      const isTraitBid = task.bidType === "collection" && selectedTraits && Object.keys(selectedTraits).length > 0
      const tokenBid = task.bidType === "token" && task.tokenIds.length > 0
      const collectionBid = !isTraitBid && !tokenBid

      if (!collectionBid) return

      console.log(BLUE + '---------------------------------------------------------------------------------' + RESET);
      console.log(BLUE + `incoming collection offer for ${task.contract.slug} for ${incomingBidAmount / 1e18} WETH on opensea`.toUpperCase() + RESET);
      console.log(BLUE + '---------------------------------------------------------------------------------' + RESET);

      const orderTrackingKey = `{${task._id}}:opensea:orders`;
      const orderKeys = await getPatternKeys(orderTrackingKey, 'collection') || []
      const orderData = orderKeys.length ? await redis.mget(orderKeys) : []
      const offers = orderData.map((order: any) => {
        if (order.offer) return
        const parsed = JSON.parse(order)
        return parsed.offer
      })
      const currentBidPrice = !offers.length ? 0 : Math.max(...offers.map((offer) => Number(offer)))
      const bestOffer = bestOffers[task._id].opensea * 1e18;
      const absoluteHighestBidAmount = Math.max(bestOffer, incomingBidAmount)


      if (incomingBidAmount < currentBidPrice || incomingBidAmount < absoluteHighestBidAmount) return

      const absoluteHighestBidAmountEth = absoluteHighestBidAmount / 1e18
      const openseaOutbidMargin = calculateOutbidMargin(absoluteHighestBidAmountEth)
      const roundedEth = roundToValidPrecision(absoluteHighestBidAmountEth)

      offerPrice = Math.ceil((openseaOutbidMargin * 1e18) + (roundedEth * 1e18))
      const offerPriceEth = offerPrice / 1e18

      const trueMinimumBidPrice = Math.max(minBidPriceEth, absoluteHighestBidAmountEth)

      if (offerPriceEth < minBidPriceEth) {
        offerPrice = trueMinimumBidPrice * 1e18
      }

      if (maxBidPriceEth > 0 && offerPriceEth > maxBidPriceEth) {
        if (!skipStats[task._id]) {
          skipStats[task._id] = {
            opensea: 0,
            magiceden: 0,
            blur: 0
          };
        }
        skipStats[task._id]['opensea']++;
        const message = `counter Offer price ${offerPriceEth} WETH for ${task.contract.slug} exceeds max bid price ${maxBidPriceEth} WETH ON OPENSEA.Skipping ...`.toUpperCase()
        await logBidError(task._id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "opensea");
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
        console.log(RED + message + RESET);
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);

        if (orderKeys.length > 0) {
          const bidData = await redis.mget(orderKeys);
          const cancelData = bidData.map((order, index) => {
            if (!order) return
            const parsed = JSON.parse(order);
            return {
              name: CANCEL_OPENSEA_BID,
              data: { privateKey: task.wallet.privateKey, orderId: parsed.orderId, taskId: task._id, orderKey: orderKeys[index] },
            }
          }).filter((item) => item !== undefined)
          await queue.addBulk(cancelData)
        }
        return;
      }
      colletionOffer = BigInt(offerPrice)
      const bidCount = getIncrementedBidCount(OPENSEA, task.contract.slug, task._id)

      const bidData: IOpenseaBidParams = {
        taskId: task._id,
        bidCount,
        walletAddress: task.wallet.address,
        walletPrivateKey: task.wallet.privateKey,
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
          console.log(RED + `Skipping counterbid since incoming bid ${incomingBidAmount / 1e18} WETH is less than or equal to existing bid ${Number(job.data.offerPrice) / 1e18} WETH` + RESET);
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
      console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(GREEN + `Counterbidding incoming offer of ${Number(incomingBidAmount) / 1e18} WETH for ${task.contract.slug} for ${Number(colletionOffer) / 1e18} WETH ON OPENSEA`.toUpperCase() + RESET);
      console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);

      if (orderKeys.length > 0) {
        const bidData = await redis.mget(orderKeys);
        const cancelData = bidData.map((order, index) => {
          if (!order) return
          const parsed = JSON.parse(order);
          return {
            name: CANCEL_OPENSEA_BID,
            data: { privateKey: task.wallet.privateKey, orderId: parsed.orderId, taskId: task._id, orderKey: orderKeys[index] },
          }
        }).filter((item) => item !== undefined)
        await queue.addBulk(cancelData)
      }
    }

  } catch (error) {
  }
}

async function handleBlurCounterbid(data: any, task: ITask) {
  const incomingBid: CombinedBid = data
  try {
    const expiry = getExpiry(task.bidDuration)
    let floor_price: number = 0

    if (task.bidPrice.minType == "eth" || task.openseaBidPrice.minType === "eth") {
      floor_price = 0
    } else {
      const stats = await fetchBlurCollectionStats(task._id, task.contract.slug);
      if (!stats || stats === 0) return
      floor_price = stats;
    }

    if (!warningBids[task._id]) {
      warningBids[task._id] = {
        opensea: false,
        magiceden: false,
        blur: false
      }
    }

    warningBids[task._id].blur = false;
    const stopBid = await stopOption(task, 'blur', floor_price, balances[task._id])

    if (stopBid) return

    const { maxBidPriceEth, minBidPriceEth } = await calculateBidPrice(task, Number(floor_price), "blur")
    const selectedTraits = transformNewTask(task.selectedTraits)
    const traitBid = task.bidType === "collection" && selectedTraits && Object.keys(selectedTraits).length > 0
    const blurOutbidMargin = task.outbidOptions.blurOutbidMargin || 0.01
    const bestOffer = bestOffers[task._id]?.blur || 0

    if (traitBid) {
      const traits = transformBlurTraits(selectedTraits)
      const incomingTraitBids = incomingBid?.stats?.filter(item => item.criteriaType.toLowerCase() === "trait") || incomingBid?.updates?.filter(item => item.criteriaType.toLowerCase() === "trait")
      const hasMatchingTraits = checkBlurTraits(incomingTraitBids, traits);

      if (!hasMatchingTraits.length) return
      for (const traitBid of hasMatchingTraits) {
        const trait = JSON.stringify(traitBid.criteriaValue)
        const incomingPrice = Number(traitBid.bestPrice)

        if (incomingPrice < bestOffer) return

        console.log(GOLD + '---------------------------------------------------------------------------------' + RESET);
        console.log(GOLD + `incoming trait offer for ${task.contract.slug}: ${trait} for ${incomingPrice} BETH on blur`.toUpperCase() + RESET);
        console.log(GOLD + '---------------------------------------------------------------------------------' + RESET);

        const traitsObj = JSON.parse(trait);
        const [traitType, traitValue] = Object.entries(traitsObj)[0];
        const identifier = `${traitType}:${traitValue}`
        const orderTrackingKey = `{${task._id}}:blur:orders`;
        const orderKeys = await getPatternKeys(orderTrackingKey, identifier) || []
        const orderData = orderKeys.length ? await redis.mget(orderKeys) : []
        const offers = orderData.map((order: any) => {
          if (order.offer) return
          const parsed = JSON.parse(order)
          return parsed.offer
        })
        const currentBidPrice = !offers.length ? 0 : Math.max(...offers.map((offer) => Number(offer)))
        const currentBidPriceEth = currentBidPrice / 1e18

        if (incomingPrice <= currentBidPriceEth) return

        let offerPrice: any = Number((blurOutbidMargin + Number(incomingPrice)).toFixed(2))
        offerPrice = BigNumber.from(utils.parseEther(offerPrice.toString()).toString())
        const offerPriceEth = offerPrice / 1e18
        if (offerPriceEth < minBidPriceEth) {
          offerPrice = minBidPriceEth * 1e18
        }
        if (maxBidPriceEth > 0 && offerPriceEth > maxBidPriceEth) {
          if (!skipStats[task._id]) {
            skipStats[task._id] = {
              opensea: 0,
              magiceden: 0,
              blur: 0
            };
          }
          skipStats[task._id]['blur']++;
          const message = `counter Offer price ${Number(offerPrice) / 1e18} BETH for ${task.contract.slug} ${trait} exceeds max bid price ${maxBidPriceEth} BETH ON BLUR.Skipping ...`.toUpperCase()
          await logBidError(task._id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "blur");
          console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
          console.log(RED + message + RESET);
          console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);

          if (orderKeys.length > 0) {
            const orderData = await redis.mget(orderKeys);
            const cancelData = orderData.map((orderKey, index) => {
              if (!orderKey) return
              const order = JSON.parse(orderKey);
              const payload = order.payload

              return {
                name: CANCEL_BLUR_BID,
                data: { privateKey: task.wallet.privateKey, payload, taskId: task._id, orderKey: orderKeys[index] }
              }
            }).filter((item) => item !== undefined)
            await queue.addBulk(cancelData);
          }
          return;
        }
        const jobId = `counterbid-${task._id}-${task.contract.slug}-${BLUR.toLowerCase()}-${trait}`
        const job: Job = await queue.getJob(jobId)
        const bidCount = getIncrementedBidCount(BLUR, task.contract.slug, task._id)
        const bidData: IBlurBidData = {
          taskId: task._id,
          bidCount: bidCount,
          address: task.wallet.address,
          privateKey: task.wallet.privateKey,
          contractAddress: task.contract.contractAddress,
          offerPrice: Number(offerPrice),
          slug: task.contract.slug,
          expiry
        }

        if (orderKeys.length > 0) {
          const orderData = await redis.mget(orderKeys);
          const cancelData = orderData.map((orderKey, index) => {
            if (!orderKey) return
            const order = JSON.parse(orderKey);
            const payload = order.payload
            return {
              name: CANCEL_BLUR_BID,
              data: { privateKey: task.wallet.privateKey, payload, taskId: task._id, orderKey: orderKeys[index] }
            }
          }).filter((item) => item !== undefined)
          await queue.addBulk(cancelData)
        }
        if (job) {
          if (incomingPrice <= job.data.offerPrice) {
            console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
            console.log(RED + `Skipping counterbid since incoming bid ${incomingPrice} WETH is less than or equal to existing bid ${Number(job.data.offerPrice)} WETH` + RESET);
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
        console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
        console.log(GREEN + `Counterbidding incoming blur offer of ${Number(incomingPrice)} BETH for ${task.contract.slug} ${trait} for ${Number(offerPrice) / 1e18} BETH ON BLUR`.toUpperCase() + RESET);
        console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
      }

    } else {
      const collectionBid = incomingBid?.stats?.filter(item => item.criteriaType.toLowerCase() === "collection") || incomingBid?.updates?.filter(item => item.criteriaType.toLowerCase() === "collection")

      if (!collectionBid.length) return
      const bestPrice = collectionBid?.sort((a, b) => +b.bestPrice - +a.bestPrice)[0].bestPrice || "0"
      const incomingPrice = Number(bestPrice) * 1e18

      const orderTrackingKey = `{${task._id}}:blur:orders`;
      const orderKeys = await getPatternKeys(orderTrackingKey, 'collection') || []
      const orderData = orderKeys.length ? await redis.mget(orderKeys) : []
      const offers = orderData.map((order: any) => {
        if (order.offer) return
        const parsed = JSON.parse(order)
        return parsed.offer
      })
      const currentBidPrice = !offers.length ? 0 : Math.max(...offers.map((offer) => Number(offer)))
      if (incomingPrice <= currentBidPrice) return

      console.log(GOLD + '---------------------------------------------------------------------------------' + RESET);
      console.log(GOLD + `incoming collection offer for ${task.contract.slug} for ${incomingPrice} BETH on blur`.toUpperCase() + RESET);
      console.log(GOLD + '---------------------------------------------------------------------------------' + RESET);

      const rawPrice = (blurOutbidMargin * 1e18) + Number(incomingPrice)
      let offerPrice = BigInt(Math.round(rawPrice / 1e16) * 1e16)
      const offerPriceEth = Number(offerPrice) / 1e18

      if (offerPriceEth < minBidPriceEth) {
        offerPrice = BigInt((minBidPriceEth * 1e18))
      }

      if (maxBidPriceEth > 0 && offerPriceEth > maxBidPriceEth) {
        if (!skipStats[task._id]) {
          skipStats[task._id] = {
            opensea: 0,
            magiceden: 0,
            blur: 0
          };
        }
        skipStats[task._id]['blur']++;
        const message = `counter Offer price ${Number(offerPrice) / 1e18} BETH for ${task.contract.slug} exceeds max bid price ${maxBidPriceEth} BETH ON BLUR.Skipping ...`.toUpperCase()
        await logBidError(task._id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "blur");
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
        console.log(RED + message + RESET);
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);


        if (orderKeys.length > 0) {
          const orderData = await redis.mget(orderKeys);
          const cancelData = orderData.map((orderKey, index) => {
            if (!orderKey) return
            const order = JSON.parse(orderKey);
            const payload = order.payload

            return {
              name: CANCEL_BLUR_BID,
              data: { privateKey: task.wallet.privateKey, payload, taskId: task._id, orderKey: orderKeys[index] }
            }
          }).filter((item) => item !== undefined)
          await queue.addBulk(cancelData);
        }
        return;
      }

      const jobId = `counterbid-${task._id}-${task.contract.slug}-${BLUR.toLowerCase()}-collection`
      const job: Job = await queue.getJob(jobId)
      const bidCount = getIncrementedBidCount(BLUR, task.contract.slug, task._id)
      const bidData: IBlurBidData = {
        taskId: task._id,
        bidCount,
        address: task.wallet.address,
        privateKey: task.wallet.privateKey,
        contractAddress: task.contract.contractAddress,
        offerPrice: Number(offerPrice),
        slug: task.contract.slug,
        expiry
      }

      if (job) {
        if (incomingPrice <= job.data.offerPrice) {
          console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
          console.log(RED + `Skipping counterbid since incoming bid ${incomingPrice / 1e18} WETH is less than or equal to existing bid ${Number(job.data.offerPrice) / 1e18} WETH` + RESET);
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
    }

  } catch (error) {
    console.error(RED + `Error handling Blur counterbid: ${JSON.stringify(error)} ` + RESET);
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
    contractAddress = message['1'].contractAddress;
  } catch (error) {
    console.error(`Failed to parse Blur message: `, error);
  }
  return contractAddress
}

function calculateOutbidMargin(currentPrice: number): number {
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



async function processOpenseaScheduledBid(task: ITask) {
  try {

    const currentTask = await activeTasks.get(task._id);
    if (!currentTask || !currentTask.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("opensea")) return;

    const filteredTasks = Object.fromEntries(
      Object.entries(task?.selectedTraits || {}).map(([category, traits]) => [
        category,

        traits.filter(trait => trait.availableInMarketplaces.includes("opensea"))
      ]).filter(([_, traits]) => traits.length > 0)
    );
    const selectedTraits = transformNewTask(filteredTasks)

    const WALLET_ADDRESS: string = task.wallet.address;
    const WALLET_PRIVATE_KEY: string = task.wallet.privateKey;

    const expiry = getExpiry(task.bidDuration)

    const collectionDetails = await getCollectionDetails(task.contract.slug, task.contract.contractAddress);
    const traitBid = selectedTraits && Object.keys(selectedTraits).length > 0
    if (!warningBids[task._id]) {
      warningBids[task._id] = {
        opensea: false,
        magiceden: false,
        blur: false
      }
    }
    if (!warningMessage[task._id]) {
      warningMessage[task._id] = {
        opensea: "",
        magiceden: "",
        blur: ""
      }
    }

    warningBids[task._id].opensea = false;

    const highestOffer = await fetchOpenseaOffers(task._id, 'COLLECTION', task.contract.slug, task.contract.contractAddress, {})

    const [topOffer] = highestOffer
    const bestOfferAmount = (topOffer?.amount ?? 0) / 1e18
    if (!bestOffers[task._id]) {
      bestOffers[task._id] = {
        opensea: bestOfferAmount,
        magiceden: 0,
        blur: 0
      };
    }
    bestOffers[task._id].opensea = bestOfferAmount;
    const autoIds = task.tokenIds
      .filter(id => id.toString().toLowerCase().startsWith('bot'))
      .map(id => {
        const matches = id.toString().match(/\d+/);
        return matches ? parseInt(matches[0]) : null;
      })
      .filter(id => id !== null);

    const amount = autoIds[0]
    const bottlomListing = autoIds ? await fetchOpenseaListings(task._id, task.contract.slug, amount) : []
    const taskTokenIds = task.tokenIds

    const tokenIds = bottlomListing ? [...bottlomListing, ...taskTokenIds] : [...taskTokenIds]
    const tokenBid = task.bidType === "token" && tokenIds.length > 0

    const approved = await approveMarketplace(WETH_CONTRACT_ADDRESS, SEAPORT, task);
    if (!approved) return

    if (traitBid && !collectionDetails.trait_offers_enabled && !tokenBid) {
      console.log(RED + `Trait bidding is not available for ${task.contract.slug} on OpenSea.`.toUpperCase() + RESET);
      warningBids[task._id].opensea = true;
      warningMessage[task._id].opensea = `Trait bidding is not available for ${task.contract.slug}`;
      await logBidError(task._id, "TRAIT BIDDING NOT AVAILABLE", `Trait bidding is not available for ${task.contract.slug} on OpenSea.`, "warning", "opensea");
      return;
    }

    const creatorFees: IFee = collectionDetails.creator_fees.null !== undefined
      ? { null: collectionDetails.creator_fees.null }
      : Object.fromEntries(Object.entries(collectionDetails.creator_fees).map(([key, value]) => [key, Number(value)]));

    if (tokenBid) {
      const jobs = tokenIds
        .filter(token => !isNaN(Number(token)))
        .map((token) => ({
          name: OPENSEA_TOKEN_BID,
          data: {
            _id: task._id,
            address: WALLET_ADDRESS,
            privateKey: WALLET_PRIVATE_KEY,
            slug: task.contract.slug,
            creatorFees,
            enforceCreatorFee: collectionDetails.enforceCreatorFee,
            asset: { contractAddress: task.contract.contractAddress, tokenId: token },
            expiry,
            outbidOptions: task.outbidOptions,
            task
          },
          opts: { priority: TOKEN_BID_PRIORITY.OPENSEA },
        }));

      const name = `${task._id}-${OPENSEA_TOKEN_BID}`
      if (!currentTask || !currentTask.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("opensea")) return;
      await addBatchedFlow(name, jobs, TOKEN_BID_PRIORITY.OPENSEA);
      console.log(`ADDED ${jobs.length} ${task.contract.slug} OPENSEA TOKEN BID JOBS TO QUEUE`);

    } else if (traitBid && collectionDetails.trait_offers_enabled) {
      const traits = transformOpenseaTraits(selectedTraits);
      const traitJobs = traits.map((trait) => ({
        name: OPENSEA_TRAIT_BID,
        data: {
          _id: task._id,
          address: WALLET_ADDRESS,
          privateKey: WALLET_PRIVATE_KEY,
          slug: task.contract.slug,
          contractAddress: task.contract.contractAddress,
          creatorFees,
          enforceCreatorFee: collectionDetails.enforceCreatorFee,
          trait: JSON.stringify(trait),
          expiry,
          outbidOptions: task.outbidOptions,
          task
        },
        opts: { priority: TRAIT_BID_PRIORITY.OPENSEA }
      }));

      const name = `${task._id}-${OPENSEA_TRAIT_BID}`
      await addBatchedFlow(name, traitJobs, TRAIT_BID_PRIORITY.OPENSEA);

      console.log(`ADDED ${traitJobs.length} ${task.contract.slug} OPENSEA TRAIT BID JOBS TO QUEUE`);
    } else if (task.bidType.toLowerCase() === "collection" && !traitBid) {

      const name = `${task._id}-${OPENSEA_COLLECTION_BID}`
      await flowProducer.add({
        name: name,
        queueName: QUEUE_NAME,
        opts: { priority: COLLECTION_BID_PRIORITY.OPENSEA },
        children: [{
          name: OPENSEA_COLLECTION_BID,
          data: { task, collectionDetails },
          queueName: QUEUE_NAME,
          opts: { priority: COLLECTION_BID_PRIORITY.OPENSEA }
        }]
      }, {
        queuesOptions: {
          [QUEUE_NAME]: {
            defaultJobOptions: {
              removeOnComplete: true,
              removeOnFail: true
            },
          },
        },
      });
    }
  } catch (error) {
    console.error(RED + `Error processing OpenSea scheduled bid for task: ${task._id} ` + RESET, error);
  }
}

async function processOpenseaCollectionBid(data: { task: ITask, collectionDetails: any }) {
  try {
    const { task, collectionDetails } = data
    const floor_price = await getCollectionStats(task.contract.slug);

    if (!floorPrices[task._id]) {
      floorPrices[task._id] = {
        opensea: floor_price as number,
        magiceden: 0,
        blur: 0
      };
    }

    floorPrices[task._id].opensea = floor_price as number;
    const stopBid = await stopOption(task, 'opensea', Number(data), balances[task._id])
    if (stopBid) return

    const { offerPriceEth, maxBidPriceEth, minBidPriceEth } = await calculateBidPrice(task, floor_price as number, "opensea")
    const outbidMargin = calculateOutbidMargin(maxBidPriceEth);
    const currentTask = activeTasks.get(task._id)

    if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("opensea")) return;

    let offerPrice = BigInt(Math.ceil(roundToValidPrecision(offerPriceEth) * 1e18));

    let colletionOffer = BigInt(offerPrice)
    const orderTrackingKey = `{${task._id}}:opensea:orders`;
    const orderKeys = await getPatternKeys(orderTrackingKey, 'collection') || []
    const [orderKey] = orderKeys
    const ttl = await redis.ttl(orderKey)

    if (!task.outbidOptions.outbid) {
      if (ttl >= MIN_BID_DURATION) {
        return;
      }
    }
    else {
      const highestOffer = await fetchOpenseaOffers(task._id, 'COLLECTION', task.contract.slug, task.contract.contractAddress, {})
      const [topOffer, secondOffer] = highestOffer

      console.log({ MARKETPLACE: OPENSEA, TYPE: 'COLLECTION', topOffer, secondOffer });

      const highestBidAmount = topOffer.amount
      const topOfferEth = Number(highestBidAmount) / 1e18;
      const owner = topOffer.owner.toLowerCase();
      const secondOwner = secondOffer?.owner?.toLowerCase();
      const isOwnBid = walletsArr
        .map(addr => addr.toLowerCase())
        .includes(owner);

      const isOwnSecondBid = walletsArr
        .map(addr => addr.toLowerCase())
        .includes(secondOwner);

      // Check if we're at max bid and have top positions
      if (topOfferEth === maxBidPriceEth && (isOwnBid || isOwnSecondBid) && ttl > MIN_BID_DURATION) {
        console.log(YELLOW + `Skipping opensea collection bid for ${task.contract.slug} - we have top bid at max price ${maxBidPriceEth} ETH with ${ttl / 60} minutes remaining`.toUpperCase() + RESET);
        return;
      }
      // 1. Handle initial zero offer case
      if (topOfferEth === 0) {
        offerPrice = BigInt(Math.ceil(offerPriceEth * 1e18));
      }
      // 2. Handle own bid case
      else if (isOwnBid) {
        const spread = (Number(topOffer.amount) - Number(secondOffer.amount)) / 1e18;

        // Check for overbidding
        if (spread > outbidMargin && Number(secondOffer.amount) > 0) {
          if (orderKeys.length > 0) {
            const bidData = await redis.mget(orderKeys);
            const cancelData = bidData.map((order, index) => {
              if (!order) return
              const parsed = JSON.parse(order);
              return {
                name: CANCEL_OPENSEA_BID,
                data: { privateKey: task.wallet.privateKey, orderId: parsed.orderId, taskId: task._id, orderKey: orderKeys[index] },
              }
            }).filter((item) => item !== undefined)
            await queue.addBulk(cancelData)
          }
          // Place new bid at optimal level
          colletionOffer = BigInt(secondOffer.amount + (outbidMargin * 1e18));
        }
        // Early return if bid isn't close to expiration
        else if (ttl > MIN_BID_DURATION) {
          return;
        } else {
          colletionOffer = BigInt(highestBidAmount);
        }
      }
      else {
        colletionOffer = BigInt(highestBidAmount + (outbidMargin * 1e18));
        const offerPriceEth = Number(colletionOffer) / 1e18;

        // Apply price constraints in order:
        if (topOfferEth === maxBidPriceEth) {
          colletionOffer = BigInt(highestBidAmount);
        }
        else if (offerPriceEth < minBidPriceEth) {
          colletionOffer = BigInt((minBidPriceEth * 1e18))
        }


        else if (maxBidPriceEth > 0 && offerPriceEth > maxBidPriceEth) {
          if (!skipStats[task._id]) {
            skipStats[task._id] = {
              opensea: 0,
              magiceden: 0,
              blur: 0
            };
          }
          skipStats[task._id]['opensea']++;
          const message = `❌ Offer price ${offerPriceEth} WETH for ${task.contract.slug} collection bid exceeds max bid price ${maxBidPriceEth} WETH FOR OPENSEA.Skipping ...`.toUpperCase()
          await logBidError(task._id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "opensea");
          console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
          console.log(RED + message + RESET);
          console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);

          if (orderKeys.length > 0) {
            const bidData = await redis.mget(orderKeys);
            const cancelData = bidData.map((order, index) => {
              if (!order) return
              const parsed = JSON.parse(order);
              return {
                name: CANCEL_OPENSEA_BID,
                data: { privateKey: task.wallet.privateKey, orderId: parsed.orderId, taskId: task._id, orderKey: orderKeys[index] },
              }
            }).filter((item) => item !== undefined)
            await queue.addBulk(cancelData)
          }
          return;
        }

      }
    }
    const bidCount = getIncrementedBidCount(OPENSEA, task.contract.slug, task._id)
    const orders = await getPatternKeys(orderTrackingKey, 'collection') || []
    console.log({ orders });

    if (orders.length > 0) {
      const bidData = await redis.mget(orders);
      const cancelData = bidData.map((order, index) => {
        if (!order) return
        const parsed = JSON.parse(order);
        return {
          name: CANCEL_OPENSEA_BID,
          data: { privateKey: task.wallet.privateKey, orderId: parsed.orderId, taskId: task._id, orderKey: orderKeys[index] },
        }
      }).filter((item) => item !== undefined)
      await queue.addBulk(cancelData)
    }

    const expiry = getExpiry(task.bidDuration)

    const creatorFees: IFee = collectionDetails.creator_fees.null !== undefined
      ? { null: collectionDetails.creator_fees.null }
      : Object.fromEntries(Object.entries(collectionDetails.creator_fees).map(([key, value]) => [key, Number(value)]));

    await bidOnOpensea(
      task._id,
      bidCount,
      task.wallet.address,
      task.wallet.privateKey,
      task.contract.slug,
      Number(colletionOffer),
      creatorFees,
      collectionDetails.enforceCreatorFee,
      expiry
    )
  } catch (error) {
    console.error(RED + `Error processing OpenSea collection bid for task: ${data.task._id} ` + RESET, error);
  }
}

async function processBlurScheduledBid(task: ITask) {
  try {
    if (!task.running || !task.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("blur")) return

    const filteredTasks = Object.fromEntries(
      Object.entries(task?.selectedTraits || {}).map(([category, traits]) => [
        category,
        traits.filter(trait => trait.availableInMarketplaces
          .map((item) => item.toLowerCase())
          .includes("blur"))
      ]).filter(([_, traits]) => traits.length > 0)
    );
    const tokenBid = task.bidType === "token"
    if (tokenBid) return
    const expiry = getExpiry(task.bidDuration)
    const WALLET_ADDRESS: string = task.wallet.address;
    const WALLET_PRIVATE_KEY: string = task.wallet.privateKey;
    const selectedTraits = transformNewTask(filteredTasks)
    const traitBid = selectedTraits && Object.keys(selectedTraits).length > 0

    if (!warningBids[task._id]) {
      warningBids[task._id] = {
        opensea: false,
        magiceden: false,
        blur: false
      }
    }

    warningBids[task._id].blur = false;
    const bestOffer = await fetchBlurBid(task._id, task.contract.contractAddress, "COLLECTION", {})
    const bestOfferAmount = bestOffer?.priceLevels?.[0]?.price ? Number(bestOffer.priceLevels[0].price) : 0

    if (!bestOffers[task._id]) {
      bestOffers[task._id] = {
        opensea: 0,
        magiceden: 0,
        blur: bestOfferAmount
      }
    }

    bestOffers[task._id].blur = bestOfferAmount
    const contractAddress = task.contract.contractAddress

    if (traitBid) {
      const traits = transformBlurTraits(selectedTraits)
      const traitJobs = traits.map((trait) => ({
        name: BLUR_TRAIT_BID,
        data: {
          _id: task._id,
          address: WALLET_ADDRESS,
          privateKey: WALLET_PRIVATE_KEY,
          contractAddress,
          slug: task.contract.slug,
          trait: JSON.stringify(trait),
          expiry,
          outbidOptions: task.outbidOptions,
          task
        },
        opts: { priority: TRAIT_BID_PRIORITY.BLUR }
      }));

      const name = `${task._id}-${BLUR_TRAIT_BID}`
      await addBatchedFlow(name, traitJobs, TRAIT_BID_PRIORITY.BLUR);

    } else if (task.bidType.toLowerCase() === "collection" && !traitBid) {
      const name = `${task._id}-${BLUR_COLLECTION_BID}`
      await flowProducer.add({
        name: name,
        queueName: QUEUE_NAME,
        opts: { priority: COLLECTION_BID_PRIORITY.BLUR },
        children: [{
          name: BLUR_COLLECTION_BID,
          data: task,
          queueName: QUEUE_NAME,
          opts: { priority: COLLECTION_BID_PRIORITY.BLUR }
        }]
      }, {
        queuesOptions: {
          [QUEUE_NAME]: {
            defaultJobOptions: {
              removeOnComplete: true,
              removeOnFail: true
            },
          },
        },
      });
    }
  } catch (error) {
    console.error(RED + `Error processing Blur scheduled bid for task: ${task._id} ` + RESET, error);
  }
}


async function processBlurCollectionBid(task: ITask) {
  try {
    const currentTask = activeTasks.get(task._id)

    if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("blur")) return;

    const floor_price: number = Number(await fetchBlurCollectionStats(task._id, task.contract.slug));

    if (!floorPrices[task._id]) {
      floorPrices[task._id] = {
        opensea: 0,
        magiceden: 0,
        blur: floor_price
      };
    }

    floorPrices[task._id].blur = floor_price;
    const { offerPriceEth, maxBidPriceEth, minBidPriceEth } = await calculateBidPrice(task, floor_price, "blur")

    const stopBid = await stopOption(task, 'blur', floor_price, balances[task._id])
    if (stopBid) return

    let colletionOffer = BigInt(offerPriceEth * 1e18)
    const orderTrackingKey = `{${task._id}}:blur:orders`;
    const orderKeys = await getPatternKeys(orderTrackingKey, 'collection') || []

    const [orderKey] = orderKeys
    const ttl = await redis.ttl(orderKey)

    const marketDataPromise = task.outbidOptions.outbid ?
      fetchBlurBid(task._id, task.contract.slug, "COLLECTION", {}) :
      null;

    if (!task.outbidOptions.outbid) {
      if (ttl >= MIN_BID_DURATION) {
        return;
      }
    }
    else {
      const highestBid = await marketDataPromise;
      const [topOffer, secondOffer] = highestBid?.priceLevels?.sort((a, b) => +b.price - +a.price) || [{ price: "0" }, { price: "0" }];
      const topOfferAmount = typeof topOffer === 'object' && topOffer ? Number(topOffer.price) : 0
      const secondOfferAmount = typeof secondOffer === 'object' && secondOffer ? Number(secondOffer.price) : 0
      const orderData = orderKeys.length > 0 ? await redis.mget(orderKeys) : []
      const offers = orderData.map((order) => {
        if (!order) return
        const parsed = JSON.parse(order);
        return Number(parsed.offer)
      })

      const currentBidPrice = !offers.length ? 0 : Math.max(...offers.map((offer) => Number(offer)))
      const currentBidPriceEth = Number((currentBidPrice / 1e18).toFixed(2));
      const bidderAddressesSample = topOffer?.bidderAddressesSample || []
      const executableSize = topOffer?.executableSize || 0
      const isTopBid = currentBidPriceEth === topOfferAmount && executableSize === 1 && bidderAddressesSample
        .map((address) => address.toLowerCase())
        .includes(task.wallet.address.toLowerCase())
      const blurOutbidMargin = task.outbidOptions.blurOutbidMargin || 0.01

      // First check if we already have the top bid
      if (isTopBid) {
        // Check if our current bid has enough spread from second offer
        const spread = Number((currentBidPriceEth - secondOfferAmount).toFixed(2));

        if (spread > blurOutbidMargin && Number(secondOfferAmount) > 0) {
          // We're overbidding by too much, reduce our bid
          console.log(RED + `Canceling overbid orders for ${task.contract.slug} - spread of ${spread} ETH exceeds margin`.toUpperCase() + RESET);
          if (orderKeys.length > 0) {
            // Cancel existing bids
            const orderData = await redis.mget(orderKeys);
            const cancelData = orderData.map((orderKey, index) => {
              if (!orderKey) return
              const order = JSON.parse(orderKey);
              const payload = order.payload;
              return {
                name: CANCEL_BLUR_BID,
                data: { privateKey: task.wallet.privateKey, payload, taskId: task._id, orderKey: orderKeys[index] }
              }
            }).filter((item) => item !== undefined)
            await queue.addBulk(cancelData);
          }
          // Place new bid just above second offer
          colletionOffer = BigInt((secondOfferAmount + blurOutbidMargin) * 1e18);
        } else if (ttl > MIN_BID_DURATION) {
          // Our bid is good and not close to expiry
          return;
        }
      } else {
        // We need to outbid the current top offer
        const newBidPrice = topOfferAmount + blurOutbidMargin;

        // Check if new bid would be within our limits
        if (newBidPrice <= maxBidPriceEth && newBidPrice >= minBidPriceEth) {
          colletionOffer = BigInt(newBidPrice * 1e18);
        }
        else if (newBidPrice > maxBidPriceEth) {
          if (maxBidPriceEth === topOfferAmount && topOfferAmount === currentBidPriceEth && ttl > MIN_BID_DURATION) {
            return;
          }
          else if (ttl <= MIN_BID_DURATION && currentBidPriceEth === topOfferAmount) {
            colletionOffer = BigInt(currentBidPriceEth * 1e18);
          }
          else if (maxBidPriceEth === topOfferAmount && currentBidPriceEth < topOfferAmount) {
            colletionOffer = BigInt(maxBidPriceEth * 1e18);
          }
          else {
            // Would exceed max bid price
            if (!skipStats[task._id]) {
              skipStats[task._id] = {
                opensea: 0,
                magiceden: 0,
                blur: 0
              };
            }
            skipStats[task._id]['blur']++;
            const message = `❌ Required offer price ${newBidPrice} BETH for ${task.contract.slug} collection bid exceeds max bid price ${maxBidPriceEth} BETH FOR BLUR.Skipping ...`.toUpperCase()
            await logBidError(task._id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "blur");
            console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
            console.log(RED + message + RESET);
            console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);

            // Cancel existing bids since we can't compete
            if (orderKeys.length > 0) {
              const orderData = await redis.mget(orderKeys);
              const cancelData = orderData.map((orderKey, index) => {
                if (!orderKey) return
                const order = JSON.parse(orderKey);
                const payload = order.payload;
                return {
                  name: CANCEL_BLUR_BID,
                  data: { privateKey: task.wallet.privateKey, payload, taskId: task._id, orderKey: orderKeys[index] }
                }
              }).filter((item) => item !== undefined)
              await queue.addBulk(cancelData);
            }
            return;

          }

        } else {
          // Would be below minimum bid price
          colletionOffer = BigInt(minBidPriceEth * 1e18);
        }
      }
    }

    const collectionOfferEth = Number((Number(colletionOffer) / 1e18).toFixed(2));

    if (collectionOfferEth < minBidPriceEth) {
      colletionOffer = BigInt((minBidPriceEth * 1e18))
    }

    const bidCount = getIncrementedBidCount(BLUR, task.contract.slug, task._id)
    if (orderKeys.length > 0) {
      const orderData = await redis.mget(orderKeys);
      const cancelData = orderData.map((orderKey, index) => {
        if (!orderKey) return
        const order = JSON.parse(orderKey);
        const payload = order.payload

        return {
          name: CANCEL_BLUR_BID,
          data: { privateKey: task.wallet.privateKey, payload, taskId: task._id, orderKey: orderKeys[index] }
        }
      }).filter((item) => item !== undefined)
      await queue.addBulk(cancelData);
    }

    const expiry = getExpiry(task.bidDuration)
    await bidOnBlur(task._id, bidCount, task.wallet.address, task.wallet.privateKey, task.contract.contractAddress, colletionOffer, task.contract.slug, expiry);
  } catch (error) {
    console.error(RED + `Error processing Blur collection bid for task: ${task._id} ` + RESET, error);
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
  expiry: number;
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
    const { address, privateKey, slug, creatorFees, enforceCreatorFee, trait, expiry, outbidOptions, contractAddress, _id, task } = data

    const floor_price = await getCollectionStats(slug);

    if (!floorPrices[_id]) {
      floorPrices[_id] = {
        opensea: floor_price as number,
        magiceden: 0,
        blur: 0
      };
    }
    floorPrices[_id].opensea = floor_price as number;
    const { maxBidPriceEth, offerPriceEth } = await calculateBidPrice(task, floor_price as number, "opensea")
    const outbidMargin = calculateOutbidMargin(maxBidPriceEth);

    const stopBid = await stopOption(task, 'opensea', Number(data), balances[_id])
    if (stopBid) return

    const currentTask = activeTasks.get(_id)
    if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("opensea")) return;
    let traitOffer = BigInt(offerPriceEth * 1e18)
    const { type, value } = JSON.parse(trait);
    const orderTrackingKey = `{${_id}}:opensea:orders`;
    const bestOffer = bestOffers[_id]?.opensea || 0;
    const orderKeys = await getPatternKeys(orderTrackingKey, `${type}:${value}`) || []
    const [orderKey] = orderKeys
    const ttl = await redis.ttl(orderKey)
    const marketDataPromise = outbidOptions.outbid ?
      fetchOpenseaOffers(_id, 'TRAIT', slug, contractAddress, JSON.parse(trait)) :
      null;

    if (!outbidOptions.outbid) {
      if (ttl >= MIN_BID_DURATION) return;
    }
    else {
      const highestBid = await marketDataPromise;
      const [topOffer, secondOffer] = highestBid || [{ amount: 0, owner: "" }, { amount: 0 }];
      console.log({ MARKETPLACE: OPENSEA, TYPE: 'TRAIT', trait, topOffer, secondOffer });

      const highestBidAmount = typeof topOffer === 'object' && topOffer ? Number(topOffer.amount) : Number(highestBid);
      const bestOfferWei = bestOffer * 1e18;

      const absoluteHighestBidAmount = Math.max(highestBidAmount, bestOfferWei);
      const absoluteHighestBidAmountEth = Number(absoluteHighestBidAmount) / 1e18;
      const owner = topOffer && typeof topOffer === 'object' ? topOffer.owner?.toLowerCase() : '';
      const isOwnBid = walletsArr
        .map(addr => addr.toLowerCase())
        .includes(owner as string);
      const secondOwner = secondOffer?.owner?.toLowerCase();

      const isOwnSecondBid = walletsArr
        .map(addr => addr.toLowerCase())
        .includes(secondOwner as string);

      const topOfferEth = Number(absoluteHighestBidAmount) / 1e18;

      // Check if we're at max bid and have top positions
      if (topOfferEth === maxBidPriceEth && (isOwnBid || isOwnSecondBid) && ttl > MIN_BID_DURATION) {
        console.log(YELLOW + `Skipping opensea collection bid for ${slug} ${trait} - we have top bid at max price ${maxBidPriceEth} ETH with ${ttl / 60} minutes remaining`.toUpperCase() + RESET);
        return;
      }

      // 1. Handle initial zero offer case
      if (topOfferEth === 0) {
        traitOffer = BigInt(Math.ceil(absoluteHighestBidAmount + (outbidMargin * 1e18)));
      }
      else if (isOwnBid) {
        const absoluteSecondBidAmount = Math.max(Number(secondOffer.amount), Number(bestOfferWei));
        const spread = (Number(highestBidAmount) - Number(absoluteSecondBidAmount)) / 1e18;
        if (spread > outbidMargin && Number(secondOffer.amount) > 0) {
          if (orderKeys.length > 0) {
            const bidData = await redis.mget(orderKeys);
            const cancelData = bidData.map((order, index) => {
              if (!order) return
              const parsed = JSON.parse(order);
              return {
                name: CANCEL_OPENSEA_BID,
                data: { privateKey: privateKey, orderId: parsed.orderId, taskId: _id, orderKey: orderKeys[index] },
              }
            }).filter((item) => item !== undefined)
            await queue.addBulk(cancelData)
          }
          traitOffer = BigInt(absoluteSecondBidAmount + (outbidMargin * 1e18));
        }
        else if (ttl > MIN_BID_DURATION) {
          return;
        } else {
          traitOffer = BigInt(absoluteHighestBidAmount);
        }
      }
      else {
        traitOffer = BigInt(absoluteHighestBidAmount + (outbidMargin * 1e18));
        const offerPriceEth = Number(traitOffer) / 1e18;
        // Apply price constraints in order:
        if (topOfferEth === maxBidPriceEth) {
          traitOffer = BigInt(highestBidAmount);
        }

        else if (offerPriceEth < absoluteHighestBidAmountEth) {
          traitOffer = BigInt((absoluteHighestBidAmountEth + outbidMargin) * 1e18)
        }

        else if (maxBidPriceEth > 0 && offerPriceEth > maxBidPriceEth) {
          if (!skipStats[_id]) {
            skipStats[_id] = {
              opensea: 0,
              magiceden: 0,
              blur: 0
            };
          }
          skipStats[_id]['opensea']++;
          const message = `❌ Offer price ${offerPriceEth} WETH for ${slug} trait ${trait} bid exceeds max bid price ${maxBidPriceEth} WETH FOR OPENSEA.Skipping ...`.toUpperCase()
          await logBidError(_id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "opensea");
          console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
          console.log(RED + message + RESET);
          console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);

          if (orderKeys.length > 0) {
            const bidData = await redis.mget(orderKeys);
            const cancelData = bidData.map((order, index) => {
              if (!order) return
              const parsed = JSON.parse(order);
              return {
                name: CANCEL_OPENSEA_BID,
                data: { privateKey: privateKey, orderId: parsed.orderId, taskId: _id, orderKey: orderKeys[index] },
              }
            }).filter((item) => item !== undefined)
            await queue.addBulk(cancelData)
          }
          return;
        }
      }
    }

    const bidCount = getIncrementedBidCount(OPENSEA, slug, _id)
    if (orderKeys.length > 0) {
      const bidData = await redis.mget(orderKeys);
      const cancelData = bidData.map((order, index) => {
        if (!order) return
        const parsed = JSON.parse(order);
        return {
          name: CANCEL_OPENSEA_BID,
          data: { privateKey, orderId: parsed.orderId, taskId: _id, orderKey: orderKeys[index] },
        }
      }).filter((item) => item !== undefined)
      await queue.addBulk(cancelData)
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
      trait
    )
  } catch (error) {
    console.error(RED + `❌ Error processing OpenSea trait bid for task: ${data?.slug} ` + RESET, error);
  }
}

async function processOpenseaTokenBid(data: IProcessOpenseaTokenBidData) {
  try {
    const { address, privateKey, slug, creatorFees, enforceCreatorFee, asset, expiry, outbidOptions, _id, task } = data
    const currentTask = activeTasks.get(_id)
    if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("opensea")) return;
    const floor_price = 0.026
    if (!floorPrices[_id]) {
      floorPrices[_id] = {
        opensea: floor_price as number,
        magiceden: 0,
        blur: 0
      };
    }
    floorPrices[_id].opensea = floor_price as number;
    const { maxBidPriceEth, offerPriceEth } = await calculateBidPrice(task, floor_price as number, "opensea")
    let tokenOffer = BigInt(offerPriceEth * 1e18)
    const bestOffer = bestOffers[_id]?.opensea || 0;
    const orderTrackingKey = `{${_id}}:opensea:orders`;
    const orderKeys = await getPatternKeys(orderTrackingKey, asset.tokenId.toString()) || []
    const [orderKey] = orderKeys

    const stopBid = await stopOption(task, 'opensea', Number(floor_price), balances[task._id])

    if (stopBid) return;

    const outbidMargin = calculateOutbidMargin(maxBidPriceEth);

    const ttl = await redis.ttl(orderKey)

    if (!outbidOptions.outbid) {
      if (ttl >= MIN_BID_DURATION) return;

    } else {
      const highestBid = await fetchItemOffers(asset.contractAddress, asset.tokenId.toString());
      const [topOffer, secondOffer] = highestBid || [{ amount: 0, owner: "" }, { amount: 0, owner: "" }];
      console.log({ MARKETPLACE: OPENSEA, TYPE: 'TOKEN', tokenId: asset.tokenId, topOffer, secondOffer });

      const highestBidAmount = typeof topOffer === 'object' && topOffer ? Number(topOffer.amount) : Number(highestBid);
      const bestOfferWei = bestOffer * 1e18;
      const owner = topOffer && typeof topOffer === 'object' ? topOffer.owner?.toLowerCase() : '';
      const isOwnBid = walletsArr
        .map(addr => addr.toLowerCase())
        .includes(owner);
      const secondOwner = secondOffer?.owner?.toLowerCase();
      const isOwnSecondBid = walletsArr
        .map(addr => addr.toLowerCase())
        .includes(secondOwner);
      const topOfferEth = Number(highestBidAmount) / 1e18;

      if (topOfferEth === maxBidPriceEth && (isOwnBid || isOwnSecondBid) && ttl > MIN_BID_DURATION) {
        console.log(YELLOW + `Skipping opensea collection bid for ${slug} ${asset.tokenId} - we have top bid at max price ${maxBidPriceEth} ETH with ${ttl / 60} minutes remaining`.toUpperCase() + RESET);
        return;
      }
      if (topOfferEth === 0) {
        tokenOffer = BigInt(Math.ceil(bestOfferWei + (outbidMargin * 1e18)));
      }

      else if (isOwnBid) {
        const spread = (Number(highestBidAmount) - Number(secondOffer.amount)) / 1e18;
        if (spread > outbidMargin && Number(secondOffer.amount) > 0) {
          if (orderKeys.length > 0) {
            const bidData = await redis.mget(orderKeys);
            const cancelData = bidData.map((order, index) => {
              if (!order) return
              const parsed = JSON.parse(order);
              return {
                name: CANCEL_OPENSEA_BID,
                data: { privateKey: privateKey, orderId: parsed.orderId, taskId: _id, orderKey: orderKeys[index] },
              }
            }).filter((item) => item !== undefined)
            await queue.addBulk(cancelData)
          }

          tokenOffer = BigInt(highestBidAmount + (outbidMargin * 1e18));
        }
        else if (ttl > MIN_BID_DURATION) {
          return;
        } else {
          tokenOffer = BigInt(highestBidAmount);
        }
      }

      else {
        tokenOffer = BigInt(highestBidAmount + (outbidMargin * 1e18));
        const offerPriceEth = Number(tokenOffer) / 1e18;

        if (topOfferEth === maxBidPriceEth) {
          tokenOffer = BigInt(highestBidAmount);
        }

        else if (offerPriceEth < topOfferEth) {
          tokenOffer = BigInt((topOfferEth + outbidMargin) * 1e18)
        }

        else if (maxBidPriceEth > 0 && offerPriceEth > maxBidPriceEth) {
          if (!skipStats[_id]) {
            skipStats[_id] = {
              opensea: 0,
              magiceden: 0,
              blur: 0
            };
          }
          skipStats[_id]['opensea']++;
          const message = `❌ Offer price ${offerPriceEth} WETH for ${slug} ${asset.tokenId} bid exceeds max bid price ${maxBidPriceEth} WETH FOR OPENSEA.Skipping ...`.toUpperCase()
          await logBidError(_id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "opensea");
          console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
          console.log(RED + message + RESET);
          console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);

          if (orderKeys.length > 0) {
            const bidData = await redis.mget(orderKeys);
            const cancelData = bidData.map((order, index) => {
              if (!order) return
              const parsed = JSON.parse(order);
              return {
                name: CANCEL_OPENSEA_BID,
                data: { privateKey: privateKey, orderId: parsed.orderId, taskId: _id, orderKey: orderKeys[index] },
              }
            }).filter((item) => item !== undefined)
            await queue.addBulk(cancelData)
          }
          return;
        }
      }
    }

    const bidCount = getIncrementedBidCount(OPENSEA, slug, _id)
    if (orderKeys.length > 0) {
      const bidData = await redis.mget(orderKeys);
      const cancelData = bidData.map((order, index) => {
        if (!order) return
        const parsed = JSON.parse(order);
        return {
          name: CANCEL_OPENSEA_BID,
          data: { privateKey, orderId: parsed.orderId, taskId: _id, orderKey: orderKeys[index] },
        }
      }).filter((item) => item !== undefined)
      await queue.addBulk(cancelData)
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
      undefined,
      asset
    )

  } catch (error: any) {
    if (error?.response?.data?.message?.errors?.[0]?.includes("Duplicate order") ||
      (Array.isArray(error) && error[0]?.includes("Duplicate order"))) {
      return;
    }
    console.log(JSON.stringify(error?.response?.data?.message, null, 2))
  }
}

async function openseaCollectionCounterBid(data: IOpenseaBidParams) {
  const { taskId, bidCount, walletAddress, walletPrivateKey, slug, offerPrice, creatorFees, enforceCreatorFee, expiry } = data
  const currentTask = activeTasks.get(taskId)
  if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("opensea")) {
    return
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
    )
  } catch (error: any) {
    if (error.length > 0 && error[0]?.includes("Duplicate order")) {
      return;
    }
    console.error(RED + `❌ Error processing OpenSea collection counter bid for task: ${slug} ` + RESET, error);
  }
}

async function magicedenCollectionCounterBid(data: IMagicedenCollectionBidData) {
  const { _id, bidCount, address, contractAddress, quantity, offerPrice, expiration, privateKey, slug } = data
  const currentTask = activeTasks.get(_id)
  if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("magiceden")) return

  try {
    await bidOnMagiceden(_id, bidCount, address, contractAddress, quantity, offerPrice.toString(), privateKey, slug);
  } catch (error) {
    console.error(RED + `❌ Error processing MagicEden collection counter bid for task: ${slug} ` + RESET, error);
  }
}

interface IMagicedenTraitBidData {
  taskId: string
  bidCount: string
  address: string
  contractAddress: string
  quantity: number
  offerPrice: number
  expiration: string
  privateKey: string
  slug: string
  trait: string
}

async function magicedenTraitCounterBid(data: IMagicedenTraitBidData) {
  const { taskId, bidCount, address, contractAddress, quantity, offerPrice, expiration, privateKey, slug, trait } = data
  const currentTask = activeTasks.get(taskId)
  if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("magiceden")) return
  const triatData = JSON.parse(trait)
  const bidTrait = {
    attributeKey: triatData?.attributeKey,
    attributeValue: triatData?.attributeValue,
  }
  try {
    await bidOnMagiceden(taskId, bidCount, address, contractAddress, quantity, offerPrice.toString(), privateKey, slug, bidTrait);
  } catch (error) {
    console.error(RED + `❌ Error processing MagicEden trait counter bid for task: ${slug} ` + RESET, error);
  }
}

async function blurCollectionCounterBid(data: IBlurBidData) {
  const { taskId, bidCount, address, privateKey, contractAddress, offerPrice, slug, expiry } = data
  const currentTask = activeTasks.get(taskId)

  if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("blur")) {
    return
  }

  try {
    const orderKeyPattern = `{${taskId}:*:blur:order:${slug}}:default`
    let remainingOffers = await redis.keys(orderKeyPattern)
    await bidOnBlur(taskId, bidCount, address, privateKey, contractAddress, BigInt(offerPrice), slug, expiry);
    const redisKey = `blur:${slug}`;
    const offerKey = `{${bidCount}:${redisKey}}:collection`
    remainingOffers = remainingOffers.filter((offer) => offer !== undefined)
    if (remainingOffers.length > 0) {
      const cancelData = remainingOffers.map((orderKey) => {
        return {
          name: CANCEL_BLUR_BID,
          data: { privateKey, orderKey: orderKey, taskId }
        }
      }).filter((item) => item !== undefined)
      await queue.addBulk(cancelData);
    }
  } catch (error) {
    console.error(RED + `❌ Error processing Blur collection counter bid for task: ${slug} ` + RESET, error);
  }
}

async function blurTraitCounterBid(data: BlurTraitCounterBid) {
  const { taskId, bidCount, address, privateKey, contractAddress, offerPrice, slug, expiry, trait } = data
  const currentTask = activeTasks.get(taskId)
  if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("blur")) {
    return
  }
  try {
    await bidOnBlur(taskId, bidCount, address, privateKey, contractAddress, BigInt(offerPrice), slug, expiry, trait);
  } catch (error) {
    console.error(RED + `❌ Error processing Blur trait counter bid for task: ${data?.slug} ` + RESET, error);
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
  trait: string;
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
    expiry,
    asset,

  } = data
  const currentTask = activeTasks.get(_id)
  if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("opensea")) return
  const bidCount = getIncrementedBidCount(OPENSEA, slug, _id)

  try {
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
      undefined,
      asset
    )
  } catch (error: any) {
    console.error(RED + `❌ Error processing OpenSea token counter bid for task: ${data?.slug} ` + RESET, error?.response?.data?.message || error?.response?.data);
  }
}

async function openseaTraitCounterBid(data: IProcessOpenseaTraitBidData) {
  try {
    const { _id, bidCount, address, privateKey, slug, offerPrice, creatorFees, enforceCreatorFee, expiry, trait } = data
    if (!trait) return
    bidOnOpensea(
      _id,
      bidCount,
      address,
      privateKey,
      slug,
      Number(offerPrice),
      creatorFees,
      enforceCreatorFee,
      expiry,
      trait
    )
  } catch (error) {
    console.error(RED + `❌ Error processing OpenSea trait counter bid for task: ${data?.slug} ` + RESET, error);
  }
}

async function magicedenTokenCounterBid(data: IMagicedenTokenBidData) {
  try {
    const { _id, address, contractAddress, quantity, offerPrice, privateKey, slug, tokenId } = data
    const currentTask = activeTasks.get(_id)
    if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("magiceden")) return
    const bidCount = getIncrementedBidCount(MAGICEDEN, slug, _id)
    await bidOnMagiceden(_id, bidCount, address, contractAddress, quantity, (offerPrice as number).toString(), privateKey, slug, undefined, tokenId)
  } catch (error) {
    console.error('Error in magicedenTokenCounterBid:', error);
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
  task: ITask
}) {
  const { address, privateKey, contractAddress, slug, trait, expiry, outbidOptions, _id, task } = data;
  const floor_price: number = Number(await fetchBlurCollectionStats(task._id, task.contract.slug));
  const { offerPriceEth, maxBidPriceEth, minBidPriceEth } = await calculateBidPrice(task, floor_price, "blur")

  let traitOffer = BigInt(Math.round(Number(offerPriceEth * 1e18) / 1e16) * 1e16);
  const traitsObj = JSON.parse(trait);
  const [traitType, traitValue] = Object.entries(traitsObj)[0];
  const identifier = `${traitType}:${traitValue}`
  const orderTrackingKey = `{${_id}}:blur:orders`;
  const currentTask = activeTasks.get(_id)
  if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("blur")) return

  const bestOffer = bestOffers[_id].blur

  try {
    const orderKeys = await getPatternKeys(orderTrackingKey, identifier) || []
    const [orderKey] = orderKeys
    const ttl = await redis.ttl(orderKey)
    if (!outbidOptions.outbid) {
      if (ttl >= MIN_BID_DURATION) return;
    }
    else {
      const bids = await fetchBlurBid(_id, slug, "TRAIT", JSON.parse(trait));
      const [topOffer, secondOffer] = bids?.priceLevels?.sort((a, b) => +b.price - +a.price) || [{ price: "0" }, { price: "0" }];
      const topOfferAmount = typeof topOffer === 'object' && topOffer ? Number(topOffer.price) : 0
      const secondOfferAmount = typeof secondOffer === 'object' && secondOffer ? Number(secondOffer.price) : 0
      const orderTrackingKey = `{${_id}}:blur:orders`;
      const orderKeys = await getPatternKeys(orderTrackingKey, identifier) || []
      const orderData = orderKeys.length ? await redis.mget(orderKeys) : []
      const offers = orderData.map((order: any) => {
        if (!order) return 0
        const parsed = JSON.parse(order)
        return Number(parsed.offer) || 0
      }).filter(Boolean)

      const currentBidPrice = !offers.length ? 0 : Math.max(...offers)
      const currentBidPriceEth = Number((currentBidPrice / 1e18).toFixed(2));
      const bidderAddressesSample = topOffer?.bidderAddressesSample || []
      const executableSize = topOffer?.executableSize || 0

      const absoluteBestOffer = Math.max(bestOffer, topOfferAmount)
      const blurOutbidMargin = outbidOptions.blurOutbidMargin || 0.01

      const isTopBid = currentBidPriceEth === absoluteBestOffer && executableSize === 1 && bidderAddressesSample
        .map((address) => address.toLowerCase())
        .includes(address.toLowerCase())

      if (absoluteBestOffer === 0) {
        // No existing offers - start at minimum bid price
        traitOffer = BigInt(Math.ceil(minBidPriceEth * 1e18));
      } else if (isTopBid) {
        // We have the top bid
        const spread = Number((currentBidPriceEth - secondOfferAmount).toFixed(2));
        if (spread > blurOutbidMargin && secondOfferAmount > 0) {
          // We're overbidding by too much - reduce to just above second offer
          console.log(RED + `Canceling overbid orders for ${slug} - spread of ${spread} ETH exceeds margin`.toUpperCase() + RESET);
          await cancelExistingOrders(orderKeys, privateKey, _id);
          traitOffer = BigInt(Math.ceil((secondOfferAmount + blurOutbidMargin) * 1e18));
        } else if (ttl > MIN_BID_DURATION) {
          // Our current bid is good and not close to expiry
          return;
        } else {
          // Maintain our current bid price
          traitOffer = BigInt(currentBidPrice);
        }
      } else {
        // We need to outbid the current top offer
        const newBidPrice = absoluteBestOffer + blurOutbidMargin;

        if (absoluteBestOffer === maxBidPriceEth) {
          // Match the max bid price if that's the current top offer
          traitOffer = BigInt(Math.ceil(maxBidPriceEth * 1e18));
        } else if (absoluteBestOffer < maxBidPriceEth) {
          // Outbid if the current offer is below our max
          if (newBidPrice <= maxBidPriceEth) {
            traitOffer = BigInt(Math.ceil(newBidPrice * 1e18));
          } else {
            // If outbidding would exceed max, bid at max
            traitOffer = BigInt(Math.ceil(maxBidPriceEth * 1e18));
          }
        } else {
          // Cancel existing bids since we can't compete at this price
          logSkipAndCancel(skipStats, _id, slug, trait, newBidPrice, maxBidPriceEth, orderKeys, privateKey);
          const message = `❌ Required offer price ${newBidPrice} BETH for ${slug} trait ${trait} bid exceeds max bid price ${maxBidPriceEth} BETH FOR BLUR.Skipping ...`.toUpperCase()
          await logBidError(_id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "blur");
          return;
        }

        // Ensure we meet minimum bid price
        if (Number(traitOffer) / 1e18 < minBidPriceEth) {
          traitOffer = BigInt(Math.ceil(minBidPriceEth * 1e18));
        }
      }
    }
    const offerPriceEth = Number((Number(traitOffer) / 1e18).toFixed(2));

    const absoluteBestOffer = Math.max(bestOffer, minBidPriceEth)

    if (offerPriceEth < absoluteBestOffer) {
      traitOffer = BigInt((absoluteBestOffer * 1e18))
    }


    const bidCount = getIncrementedBidCount(BLUR, slug, _id)
    if (orderKeys.length > 0) {
      const orderData = await redis.mget(orderKeys);
      const cancelData = orderData.map((orderKey, index) => {
        if (!orderKey) return
        const order = JSON.parse(orderKey);
        const payload = order.payload

        return {
          name: CANCEL_BLUR_BID,
          data: { privateKey, payload, taskId: _id, orderKey: orderKeys[index] }
        }
      }).filter((item) => item !== undefined)
      await queue.addBulk(cancelData);
    }
    await bidOnBlur(_id, bidCount, address, privateKey, contractAddress, traitOffer, slug, expiry, trait);
  } catch (error) {
    console.error(RED + `Error processing Blur trait bid for task: ${data?.slug} ` + RESET, error);
  }
}

async function processMagicedenScheduledBid(task: ITask) {
  try {
    if (!task.running || !task.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("magiceden")) return
    const filteredTasks = Object.fromEntries(
      Object.entries(task?.selectedTraits || {}).map(([category, traits]) => [
        category,
        traits.filter(trait => trait.availableInMarketplaces.includes("magiceden"))
      ]).filter(([_, traits]) => traits.length > 0)
    );

    const WALLET_ADDRESS: string = task.wallet.address
    const WALLET_PRIVATE_KEY: string = task.wallet.privateKey
    const selectedTraits = transformNewTask(filteredTasks)
    const traitBid = selectedTraits && Object.keys(selectedTraits).length > 0

    const contractAddress = task.contract.contractAddress
    const expiry = getExpiry(task.bidDuration)
    const duration = expiry / 60 || 15;
    const currentTime = new Date().getTime();
    const expiration = Math.floor((currentTime + (duration * 60 * 1000)) / 1000);

    if (!warningBids[task._id]) {
      warningBids[task._id] = {
        opensea: false,
        magiceden: false,
        blur: false
      }
    }
    warningBids[task._id].magiceden = false;

    const [bestOffer] = await fetchMagicEdenOffer(task._id,
      "COLLECTION",
      task.contract.contractAddress,
      {}
    )

    const bestOfferAmount = Number(bestOffer.amount) / 1e18

    if (!bestOffers[task._id]) {
      bestOffers[task._id] = {
        opensea: bestOfferAmount,
        magiceden: 0,
        blur: 0
      };
    }

    bestOffers[task._id].magiceden = bestOfferAmount;

    const autoIds = task.tokenIds
      .map(id => id.toString().trim()) // Remove whitespace
      .filter(id => id.toString().toLowerCase().startsWith('bot'))
      .map(id => {
        const matches = id.toString().match(/\d+/);
        return matches ? parseInt(matches[0]) : null;
      })
      .filter(id => id !== null);

    const amount = autoIds[0]
    const bottlomListing = amount ? await fetchMagicEdenTokens(task._id, task.contract.contractAddress, amount) : []
    const taskTokenIds = task.tokenIds
    const tokenIds = bottlomListing ? [...bottlomListing, ...taskTokenIds] : [...taskTokenIds]
    const tokenBid = task.bidType === "token" && tokenIds.length > 0
    const approved = await approveMarketplace(WETH_CONTRACT_ADDRESS, SEAPORT, task);

    if (!approved) return

    if (tokenBid) {
      const jobs = tokenIds
        .filter(token => !isNaN(Number(token)))
        .map((token) => ({
          name: MAGICEDEN_TOKEN_BID,
          data: {
            _id: task._id,
            address: WALLET_ADDRESS,
            contractAddress,
            quantity: 1,
            expiration,
            privateKey: WALLET_PRIVATE_KEY,
            slug: task.contract.slug,
            tokenId: token,
            outbidOptions: task.outbidOptions,
            task
          },
          opts: { priority: TOKEN_BID_PRIORITY.MAGICEDEN }
        }));

      const name = `${task._id}-${MAGICEDEN_TOKEN_BID}`

      await addBatchedFlow(name, jobs, TOKEN_BID_PRIORITY.MAGICEDEN);

      console.log(`ADDED ${jobs.length} ${task.contract.slug} MAGICEDEN TOKEN BID JOBS TO QUEUE`);
    }
    else if (traitBid) {
      const traits = Object.entries(selectedTraits).flatMap(([key, values]) =>
        values.map(value => ({ attributeKey: key, attributeValue: value })))
      const traitJobs = traits.map((trait) => ({
        name: MAGICEDEN_TRAIT_BID,
        data: {
          _id: task._id,
          address: WALLET_ADDRESS,
          contractAddress,
          quantity: 1,
          expiration,
          privateKey: WALLET_PRIVATE_KEY,
          slug: task.contract.slug,
          trait,
          outbidOptions: task.outbidOptions,
          task
        },
        opts: { priority: TRAIT_BID_PRIORITY.MAGICEDEN }
      }));

      const name = `${task._id}-${MAGICEDEN_TRAIT_BID}`

      await addBatchedFlow(name, traitJobs, TRAIT_BID_PRIORITY.MAGICEDEN);
      console.log(`ADDED ${traitJobs.length} ${task.contract.slug} MAGICEDEN TRAIT BID JOBS TO QUEUE`);
    }
    else if (task.bidType.toLowerCase() === "collection" && !traitBid) {

      const name = `${task._id}-${MAGICEDEN_COLLECTION_BID}`
      await flowProducer.add({
        name: name,
        queueName: QUEUE_NAME,
        opts: { priority: COLLECTION_BID_PRIORITY.MAGICEDEN },
        children: [{
          name: MAGICEDEN_COLLECTION_BID,
          data: task,
          queueName: QUEUE_NAME,
          opts: { priority: COLLECTION_BID_PRIORITY.MAGICEDEN }
        }]
      }, {
        queuesOptions: {
          [QUEUE_NAME]: {
            defaultJobOptions: {
              removeOnComplete: true,
              removeOnFail: true
            },
          },
        },
      })

      console.log(`ADDED ${MAGICEDEN_COLLECTION_BID} JOB TO QUEUE`);
    }
  } catch (error) {
    console.error(RED + `Error processing MagicEden scheduled bid for task: ${task._id} ` + RESET, error);
  }
}


async function processMagicedenCollectionBid(task: ITask) {
  try {
    const floor_price = Number(await fetchMagicEdenCollectionStats(task._id, task.contract.contractAddress))

    if (!floorPrices[task._id]) {
      floorPrices[task._id] = {
        opensea: 0,
        magiceden: floor_price,
        blur: 0
      };
    }

    const { offerPriceEth, maxBidPriceEth, minBidPriceEth } = await calculateBidPrice(task, floor_price as number, "magiceden")
    floorPrices[task._id].magiceden = floor_price;

    const stopBid = await stopOption(task, 'magiceden', floor_price, balances[task._id])
    if (stopBid) return
    let offerPrice = Math.ceil(offerPriceEth * 1e18)

    const currentTask = activeTasks.get(task._id)
    if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("magiceden")) return;

    let colletionOffer = BigInt(offerPrice)
    const orderTrackingKey = `{${task._id}}:magiceden:orders`;
    const orderKeys = await getPatternKeys(orderTrackingKey, 'collection') || []
    const [orderKey] = orderKeys
    const ttl = await redis.ttl(orderKey)
    const outbidMargin = (task.outbidOptions.magicedenOutbidMargin || 0.0001) * 1e18

    const marketDataPromise = task.outbidOptions.outbid ?
      fetchMagicEdenOffer(task._id, "COLLECTION", task.contract.contractAddress, {}) :
      null;

    if (!task.outbidOptions.outbid) {
      if (ttl >= MIN_BID_DURATION) {
        return;
      }
    }

    else {
      const [highestBid, secondBestOffer] = await marketDataPromise || [{ amount: "0", owner: "" }];

      console.log({ MARKETPLACE: MAGICEDEN, TYPE: 'COLLECTION', highestBid, secondBestOffer });

      const highestBidAmount = typeof highestBid === 'object' && highestBid ? Number(highestBid.amount) : Number(highestBid);

      const topOfferEth = Number(highestBidAmount) / 1e18;

      const owner = highestBid && typeof highestBid === 'object' ? highestBid.owner?.toLowerCase() : '';
      const isOwnBid = walletsArr
        .map(addr => addr.toLowerCase())
        .includes(owner);
      const secondOwner = secondBestOffer?.owner?.toLowerCase();

      const isOwnSecondBid = walletsArr
        .map(addr => addr.toLowerCase())
        .includes(secondOwner);

      if (topOfferEth === maxBidPriceEth && (isOwnBid || isOwnSecondBid) && ttl > MIN_BID_DURATION) {
        console.log(YELLOW + `Skipping magiceden collection bid for ${task.contract.slug} - we have top bid at max price ${maxBidPriceEth} ETH with ${ttl / 60} minutes remaining`.toUpperCase() + RESET);
        return;
      }

      if (topOfferEth === 0) {
        colletionOffer = BigInt(Math.ceil(offerPriceEth * 1e18));
      }

      else if (isOwnBid) {
        const spread = (Number(highestBid.amount) - Number(secondBestOffer.amount)) / 1e18;





        if (spread > outbidMargin && Number(secondBestOffer.amount) > 0) {
          if (orderKeys.length > 0) {
            const extractedOrderIds = await extractMagicedenOrderHash(orderKeys)
            await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey: task.wallet.privateKey, orderKeys: orderKeys, taskId: task._id });
          }
          // Convert to BigInt before arithmetic operations
          const secondBestOfferBigInt = BigInt(secondBestOffer.amount);
          const outbidMarginBigInt = BigInt(outbidMargin * 1e18);
          colletionOffer = secondBestOfferBigInt + outbidMarginBigInt;
        }

        else if (ttl > MIN_BID_DURATION) {
          return
        }

        else {
          colletionOffer = BigInt(highestBidAmount);
        }
      }
      else {
        colletionOffer = BigInt(highestBidAmount + outbidMargin)
        const offerPriceEth = Number(colletionOffer) / 1e18;
        // Apply price constraints in order:
        if (topOfferEth === maxBidPriceEth) {
          colletionOffer = BigInt(highestBidAmount);
        }

        else if (offerPriceEth < minBidPriceEth) {
          colletionOffer = BigInt((minBidPriceEth * 1e18))
        }

        else if (maxBidPriceEth > 0 && offerPriceEth > maxBidPriceEth) {
          if (!skipStats[task._id]) {
            skipStats[task._id] = {
              opensea: 0,
              magiceden: 0,
              blur: 0
            };
          }
          skipStats[task._id]['magiceden']++;
          const message = `❌ Offer price ${offerPriceEth} WETH for ${task.contract.slug} collection bid exceeds max bid price ${maxBidPriceEth} WETH FOR MAGICEDEN.Skipping ...`.toUpperCase()
          await logBidError(task._id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "magiceden");
          console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
          console.log(RED + message + RESET);
          console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);


          if (orderKeys.length > 0) {
            const extractedOrderIds = await extractMagicedenOrderHash(orderKeys)
            await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey: task.wallet.privateKey, orderKeys: orderKeys, taskId: task._id });
          }
          return;
        }
      }
    }

    const bidCount = getIncrementedBidCount(MAGICEDEN, task.contract.slug, task._id)
    const orders = await getPatternKeys(orderTrackingKey, 'collection') || []
    if (orders.length > 0) {
      const extractedOrderIds = await extractMagicedenOrderHash(orders)

      console.log({ extractedOrderIds, orders });

      await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey: task.wallet.privateKey, orderKeys: orders, taskId: task._id });
    }
    await bidOnMagiceden(task._id, bidCount, task.wallet.address, task.contract.contractAddress, 1, colletionOffer.toString(), task.wallet.privateKey, task.contract.slug);
  } catch (error) {
    console.error(RED + `Error processing MagicEden collection bid for task: ${task._id} ` + RESET, error);
  }
}


async function processMagicedenTokenBid(data: IMagicedenTokenBidData) {
  try {
    const { contractAddress, address, quantity, expiration, privateKey, slug, tokenId, outbidOptions, task, _id } = data

    const floor_price = 0.026

    if (!floorPrices[task._id]) {
      floorPrices[task._id] = {
        opensea: 0,
        magiceden: floor_price,
        blur: 0
      };
    }
    floorPrices[task._id].magiceden = floor_price;
    const { offerPriceEth, maxBidPriceEth } = await calculateBidPrice(task, floor_price as number, "magiceden")

    const currentTask = activeTasks.get(_id)
    if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("magiceden")) return;
    const orderTrackingKey = `{${_id}}:magiceden:orders`;
    const orderKeys = await getPatternKeys(orderTrackingKey, tokenId.toString()) || []
    const [orderKey] = orderKeys
    const outbidMargin = calculateOutbidMargin(maxBidPriceEth);
    const ttl = await redis.ttl(orderKey)

    const bestOffer = bestOffers[_id]?.magiceden || 0;

    let tokenOffer = Number(offerPriceEth * 1e18)

    if (!outbidOptions.outbid) {
      if (ttl >= MIN_BID_DURATION) {
        return;
      }
    }

    else {
      const [topOffer, secondOffer] = await fetchMagicEdenOffer(_id, "TOKEN", contractAddress, tokenId.toString()) || [{ amount: 0, owner: "" }, { amount: 0, owner: "" }];
      console.log({ MARKETPLACE: MAGICEDEN, TYPE: 'TOKEN', tokenId: tokenId, topOffer, secondOffer });

      // Add null checks and default values
      const highestBidAmount = typeof topOffer === 'object' && topOffer ? Number(topOffer.amount || 0) : 0;
      const bestOfferWei = bestOffer * 1e18;
      const absoluteHighestBidAmount = Math.max(highestBidAmount, bestOfferWei);
      const absoluteHighestBidAmountEth = Number(absoluteHighestBidAmount) / 1e18;
      const owner = topOffer && typeof topOffer === 'object' ? topOffer.owner?.toLowerCase() || '' : '';
      const isOwnBid = walletsArr
        .map(addr => addr.toLowerCase())
        .includes(owner);

      // Add null check for secondOffer
      const secondOwner = secondOffer && typeof secondOffer === 'object' ? secondOffer.owner?.toLowerCase() || '' : '';
      const isOwnSecondBid = walletsArr
        .map(addr => addr.toLowerCase())
        .includes(secondOwner);

      const topOfferEth = Number(absoluteHighestBidAmount) / 1e18;

      if (topOfferEth === maxBidPriceEth && (isOwnBid || isOwnSecondBid) && ttl > MIN_BID_DURATION) {
        console.log(YELLOW + `Skipping magiceden token bid for ${slug} ${tokenId} - we have top bid at max price ${maxBidPriceEth} ETH with ${ttl / 60} minutes remaining`.toUpperCase() + RESET);
        return;
      }

      if (topOfferEth === 0) {
        tokenOffer = Math.ceil(absoluteHighestBidAmount + (outbidMargin * 1e18))
      }

      else if (isOwnBid) {
        // Add null check for secondOffer.amount
        const secondOfferAmount = secondOffer && typeof secondOffer === 'object' ? Number(secondOffer.amount || 0) : 0;
        const absoluteSecondBidAmount = Math.max(secondOfferAmount, Number(bestOfferWei));
        const spread = (Number(highestBidAmount) - Number(absoluteSecondBidAmount)) / 1e18;

        if (spread > outbidMargin && secondOfferAmount > 0) {
          if (orderKeys.length > 0) {
            const extractedOrderIds = await extractMagicedenOrderHash(orderKeys)
            await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey: privateKey, orderKeys: orderKeys, taskId: _id });
          }
          tokenOffer = absoluteSecondBidAmount + (outbidMargin * 1e18)
        }

        else if (ttl > MIN_BID_DURATION) {
          return;
        } else {
          tokenOffer = absoluteHighestBidAmount
        }
      }

      else {
        tokenOffer = absoluteHighestBidAmount + (outbidMargin * 1e18)
        const offerPriceEth = Number(tokenOffer) / 1e18;

        if (topOfferEth === maxBidPriceEth) {
          tokenOffer = absoluteHighestBidAmount
        }

        else if (offerPriceEth < absoluteHighestBidAmountEth) {
          tokenOffer = (absoluteHighestBidAmountEth + outbidMargin) * 1e18
        }

        else if (maxBidPriceEth > 0 && offerPriceEth > maxBidPriceEth) {
          if (!skipStats[_id]) {
            skipStats[_id] = {
              opensea: 0,
              magiceden: 0,
              blur: 0
            };
          }

          skipStats[_id]['magiceden']++;
          const message = `❌ Offer price ${offerPriceEth} WETH for ${slug} ${tokenId} exceeds max bid price ${maxBidPriceEth} WETH FOR MAGICEDEN.Skipping ...`.toUpperCase()
          await logBidError(_id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "magiceden");
          console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
          console.log(RED + message + RESET);
          console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);

          if (orderKeys.length > 0) {
            const extractedOrderIds = await extractMagicedenOrderHash(orderKeys)
            await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey: privateKey, orderKeys: orderKeys, taskId: _id });
          }
          return;
        }
      }
    }
    const bidCount = getIncrementedBidCount(MAGICEDEN, slug, _id)
    if (orderKeys.length > 0) {
      const extractedOrderIds = await extractMagicedenOrderHash(orderKeys)
      await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey, orderKeys: orderKeys, taskId: _id });
    }


    await bidOnMagiceden(_id, bidCount, address, contractAddress, quantity, tokenOffer.toString(), privateKey, slug, undefined, tokenId)
  } catch (error) {
    console.error(RED + `Error processing MagicEden token bid for task: ${data?.slug} ` + RESET, error);
  }
}

async function getPatternKeys(orderTrackingKey: string, identifier: string) {
  try {
    const orderKeys = await redis.smembers(orderTrackingKey);
    if (!orderKeys.length) return [];

    return orderKeys.filter(key => {
      const keyParts = key.split(':');
      const lastParts = keyParts.slice(-2);

      if (identifier.includes(':')) {
        return lastParts.join(':') === identifier;
      } else {
        return keyParts[keyParts.length - 1] === identifier;
      }
    });

  } catch (error) {
    console.log(error);
    return [];
  }
}

async function processMagicedenTraitBid(data: {
  _id: string;
  address: string;
  contractAddress: string;
  quantity: number;
  offerPrice?: string;
  expiration: string;
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
  try {
    const
      { contractAddress, address, quantity, expiration, privateKey, slug, trait, _id, outbidOptions, task } = data

    const currentTask = activeTasks.get(_id)
    const floor_price = Number(await fetchMagicEdenCollectionStats(task._id, task.contract.contractAddress))

    const { offerPriceEth, maxBidPriceEth } = await calculateBidPrice(task, floor_price as number, "magiceden")


    const outbidMargin = calculateOutbidMargin(maxBidPriceEth);

    const bestOffer = bestOffers[_id]?.magiceden || 0;

    if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("magiceden")) return

    const orderTrackingKey = `{${_id}}:magiceden:orders`;
    const orderKeys = await getPatternKeys(orderTrackingKey, `${trait.attributeKey}:${trait.attributeValue}`) || []

    const [orderKey] = orderKeys
    const ttl = await redis.ttl(orderKey)
    const marketDataPromise = outbidOptions.outbid ?
      fetchMagicEdenOffer(_id, "TRAIT", contractAddress, trait) :
      null;

    let traitOffer = Number(offerPriceEth * 1e18)

    if (!outbidOptions.outbid) {
      if (ttl >= MIN_BID_DURATION) {
        return;
      }
    }

    else {
      const [highestBid, secondBestOffer] = await marketDataPromise || [{ amount: "0", owner: "" }];
      console.log({ MARKETPLACE: MAGICEDEN, TYPE: 'TRAIT', trait: JSON.stringify(trait), highestBid, secondBestOffer });
      const highestBidAmount = typeof highestBid === 'object' && highestBid ? Number(highestBid.amount) : Number(highestBid);
      const owner = highestBid && typeof highestBid === 'object' ? highestBid.owner?.toLowerCase() : '';
      const isOwnBid = walletsArr
        .map(addr => addr.toLowerCase())
        .includes(owner);

      const bestOfferWei = bestOffer * 1e18;
      const absoluteHighestBidAmount = Math.max(highestBidAmount, bestOfferWei);
      const absoluteHighestBidAmountEth = Number(absoluteHighestBidAmount) / 1e18;

      const secondOwner = secondBestOffer?.owner?.toLowerCase();

      const isOwnSecondBid = walletsArr
        .map(addr => addr.toLowerCase())
        .includes(secondOwner);

      const topOfferEth = Number(absoluteHighestBidAmount) / 1e18;

      if (topOfferEth === maxBidPriceEth && (isOwnBid || isOwnSecondBid) && ttl > MIN_BID_DURATION) {
        console.log(YELLOW + `Skipping magiceden trait bid for ${slug} ${JSON.stringify(trait)} - we have top bid at max price ${maxBidPriceEth} ETH with ${ttl / 60} minutes remaining`.toUpperCase() + RESET);
        return;
      }

      if (topOfferEth === 0) {
        traitOffer = Math.ceil(absoluteHighestBidAmount + (outbidMargin * 1e18))
      }

      else if (isOwnBid) {
        const absoluteSecondBidAmount = Math.max(Number(secondBestOffer.amount), Number(bestOfferWei));
        const spread = (Number(highestBidAmount) - Number(absoluteSecondBidAmount)) / 1e18;

        if (spread > outbidMargin && Number(secondBestOffer.amount) > 0) {
          if (orderKeys.length > 0) {
            const extractedOrderIds = await extractMagicedenOrderHash(orderKeys)
            await queue.add(CANCEL_MAGICEDEN_BID, {
              orderIds: extractedOrderIds,
              privateKey,
              orderKeys: orderKeys,
              taskId: _id
            });
          }
          traitOffer = absoluteSecondBidAmount + (outbidMargin * 1e18);
        }
        else if (ttl > MIN_BID_DURATION) {
          return;
        }
      }

      else {
        traitOffer = (highestBidAmount + (outbidMargin * 1e18));
        const offerPriceEth = Number(traitOffer) / 1e18;

        if (topOfferEth === maxBidPriceEth) {
          traitOffer = highestBidAmount
        }

        else if (offerPriceEth < absoluteHighestBidAmountEth) {
          traitOffer = (absoluteHighestBidAmountEth + outbidMargin) * 1e18
        }


        else if (maxBidPriceEth > 0 && offerPriceEth > maxBidPriceEth) {
          if (!skipStats[_id]) {
            skipStats[_id] = {
              opensea: 0,
              magiceden: 0,
              blur: 0
            };
          }
          skipStats[_id]['magiceden']++;
          const message = `❌ Offer price ${offerPriceEth} WETH for ${slug} trait ${JSON.stringify(trait)} bid exceeds max bid price ${maxBidPriceEth} WETH FOR MAGICEDEN.Skipping ...`.toUpperCase()
          await logBidError(_id, "MAXIMUM BID PRICE EXCEEDED", message, "skipped", "magiceden");
          console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
          console.log(RED + message + RESET);
          console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);

          if (orderKeys.length > 0) {
            const extractedOrderIds = await extractMagicedenOrderHash(orderKeys)
            await queue.add(CANCEL_MAGICEDEN_BID, {
              orderIds: extractedOrderIds,
              privateKey,
              orderKeys: orderKeys,
              taskId: _id
            });
          }
          return;
        }
      }

    }

    const bidCount = getIncrementedBidCount(MAGICEDEN, slug, _id)
    if (orderKeys.length > 0) {
      const extractedOrderIds = await extractMagicedenOrderHash(orderKeys)
      await queue.add(CANCEL_MAGICEDEN_BID, {
        orderIds: extractedOrderIds,
        privateKey,
        orderKeys: orderKeys,
        taskId: _id
      });
    }


    await bidOnMagiceden(_id, bidCount, address, contractAddress, quantity, traitOffer.toString(), privateKey, slug, trait)
  } catch (error) {
    console.error(RED + `Error processing MagicEden trait bid for task: ${data?.slug} ` + RESET, error);
  }
}

async function bulkCancelOpenseaBid(data: { privateKey: string, orderId: string, taskId: string, orderKey: string }) {
  const { privateKey, orderId, taskId, orderKey } = data
  try {
    await Promise.all([
      cancelOrder(orderId, OPENSEA_PROTOCOL_ADDRESS, privateKey, taskId),
      redis.srem(`{${taskId}}:opensea:orders`, orderKey),
      redis.del(orderKey)
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
        ...orderKeys.map((key) => redis.srem(`{${taskId}}:magiceden:orders`, key)),
        ...orderKeys.map((key) => redis.del(key))
      ]);
    }
  } catch (error) {
    console.error(RED + `Error processing Magiceden bid cancellation for task: ${taskId} ` + RESET, error);
  }
}

async function validateBidCount() {
  for (const task of currentTasks) {
    try {
      const [openseaOrders, magicedenOrders, blurOrders] = await Promise.all([
        redis.smembers(`{${task._id}}:opensea:orders`),
        redis.smembers(`{${task._id}}:magiceden:orders`),
        redis.smembers(`{${task._id}}:blur:orders`)
      ])

      await Promise.all([
        ...blurOrders.map(async (key) => {
          const ttl = await redis.ttl(key)
          if (ttl <= 0) {
            await redis.srem(`{${task._id}}:blur:orders`, key)
            blurOrders.filter((item) => item !== key)
          }
        }),
        ...openseaOrders.map(async (key) => {
          const ttl = await redis.ttl(key)
          if (ttl <= 0) {
            await redis.srem(`{${task._id}}:opensea:orders`, key)
            openseaOrders.filter((item) => item !== key)
          }
        }),
        ...magicedenOrders.map(async (key) => {
          const ttl = await redis.ttl(key)
          if (ttl <= 0) {
            await redis.srem(`{${task._id}}:magiceden:orders`, key)
            magicedenOrders.filter((item) => item !== key)
          }
        })
      ])

      bidStats[task._id] = {
        opensea: openseaOrders.length,
        magiceden: magicedenOrders.length,
        blur: blurOrders.length
      }
    } catch (error) {
      console.error(RED + `Error validating bid count for task: ${task._id}` + RESET, error);
    }
  }
}

setInterval(validateBidCount, 5000);

async function blukCancelBlurBid(data: BlurCancelPayload) {
  const { orderKey, privateKey, taskId, payload } = data
  try {
    if (!data) return
    await Promise.all([
      cancelBlurBid(taskId, { payload, privateKey, taskId }),
      redis.srem(`{${taskId}}:blur:orders`, orderKey),
      redis.del(orderKey),
    ])
  } catch (error) {
    console.error(RED + `Error processing Blur bid cancellation for task: ${taskId} ` + RESET, error);
  }
}

app.get("/", (req, res) => {
  res.json({ message: "Welcome to the NFTTools bidding bot server! Let's make magic happen! 🚀🚀🚀" });
});

app.get("/health", async (req, res) => {
  try {
    const queueCounts = await queue.getJobCounts();
    const workerCount = workers.length;
    const activeWorkers = workers.forEach((worker) => worker.isRunning());

    const metrics = {
      status: "ok",
      timestamp: new Date().toISOString(),
      uptime: process.uptime(),
      components: {
        redis: redis.status === "ready" ? "ok" : "error",
        mongodb: mongoose.connection.readyState === 1 ? "ok" : "error",
        workers: workerCount > 0 ? "ok" : "error"
      },
      metrics: {
        memory: {
          rss: `${Math.round(process.memoryUsage().rss / 1024 / 1024)}MB`,
          heapTotal: `${Math.round(process.memoryUsage().heapTotal / 1024 / 1024)}MB`,
          heapUsed: `${Math.round(process.memoryUsage().heapUsed / 1024 / 1024)}MB`,
          external: `${Math.round(process.memoryUsage().external / 1024 / 1024)}MB`
        },
        queue: queueCounts,
        workers: {
          total: workerCount,
          active: activeWorkers
        }
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

function getExpiry(bidDuration: { value: number; unit: string }) {
  const expiry = bidDuration.unit === 'minutes'
    ? bidDuration.value * 60
    : bidDuration.unit === 'hours'
      ? bidDuration.value * 3600
      : bidDuration.unit === 'days'
        ? bidDuration.value * 86400
        : 900;

  return expiry
}

async function calculateBidPrice(task: ITask, floorPrice: number, marketplaceName: "opensea" | "magiceden" | "blur"): Promise<{ offerPriceEth: number; maxBidPriceEth: number, minBidPriceEth: number }> {
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
  return { offerPriceEth, maxBidPriceEth, minBidPriceEth };
}

async function stopOption(
  task: ITask,
  marketplace: 'opensea' | 'magiceden' | 'blur',
  floorPrice: number,
  balance: number = 0, // Optional parameter for balance checking
): Promise<boolean> {
  // Early return if stop options aren't configured
  if (!task.stopOptions?.triggerStopOptions || !task.stopOptions?.minFloorPrice || !task.stopOptions?.maxFloorPrice) {
    return false;
  }


  // Check if floor price is outside allowed range
  if ((Number(floorPrice) && floorPrice < task.stopOptions.minFloorPrice) || floorPrice > task.stopOptions.maxFloorPrice) {
    console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
    console.log(RED + `❌ Floor price ${floorPrice} ETH for ${task.contract.slug} is outside allowed range (${task.stopOptions.minFloorPrice} ETH - ${task.stopOptions.maxFloorPrice} ETH). Skipping...`.toUpperCase() + RESET);
    console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);

    if (!warningBids[task._id]) {
      warningBids[task._id] = {
        opensea: false,
        magiceden: false,
        blur: false
      };
    }


    if (!warningMessage[task._id]) {
      warningMessage[task._id] = {
        opensea: "",
        magiceden: "",
        blur: ""
      };
    }
    warningBids[task._id][marketplace] = true;
    warningMessage[task._id][marketplace] = `Floor price ${floorPrice} ETH for ${task.contract.slug} is outside allowed range (${task.stopOptions.minFloorPrice} ETH - ${task.stopOptions.maxFloorPrice} ETH).`;
    return true;
  }

  // Optional balance check
  if (task.stopOptions?.maxPurchase && task.stopOptions.maxPurchase > 0) {
    const provider = new ethers.providers.AlchemyProvider('mainnet', ALCHEMY_API_KEY);
    const abi = [
      "function balanceOf(address owner) view returns (uint256 balance)",
    ]
    const contract = new Contract(task.contract.contractAddress, abi, provider);
    const newBalance = await contract.balanceOf(task.wallet.address)
    if (newBalance - balance >= task.stopOptions.maxPurchase) {
      console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(RED + `❌ Total purchase amount (${(newBalance - balance)} ${task.contract.slug}) has reached or exceeded max purchase limit (${task.stopOptions.maxPurchase}) for ${task.contract.slug}. Skipping...`.toUpperCase() + RESET);
      console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
      warningBids[task._id].opensea = true;
      warningBids[task._id].magiceden = true;
      warningBids[task._id].blur = true;
      warningMessage[task._id].opensea = `Total purchase amount (${(newBalance - balance)} ${task.contract.slug}) has reached or exceeded max purchase limit (${task.stopOptions.maxPurchase}) for ${task.contract.slug}.`;
      warningMessage[task._id].magiceden = `Total purchase amount (${(newBalance - balance)} ${task.contract.slug}) has reached or exceeded max purchase limit (${task.stopOptions.maxPurchase}) for ${task.contract.slug}.`;
      warningMessage[task._id].blur = `Total purchase amount (${(newBalance - balance)} ${task.contract.slug}) has reached or exceeded max purchase limit (${task.stopOptions.maxPurchase}) for ${task.contract.slug}.`;
      return true;
    }
  }

  warningBids[task._id][marketplace] = false;
  return false;
}

function transformBlurTraits(selectedTraits: Record<string, string[]>): { [key: string]: string }[] {
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
  const countKey = `${marketplace}:${slug} `;
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



function checkNetwork() {
  const socket = new net.Socket();

  socket.setTimeout(5000);
  socket.on('connect', () => {
    console.log('Network is connected');
    netEnabled = true;
    socket.destroy();
  });

  socket.on('error', () => {
    console.log('Network is not connected');
    netEnabled = false;
  });

  socket.connect(80, 'google.com');
}

checkNetwork();

let marketplaceIntervals: { [key: string]: NodeJS.Timeout } = {};
const subscribedTasks = new Map<string, Set<string>>(); // Map<taskId, Set<marketplaces>>

function subscribeToCollections(tasks: ITask[]) {
  try {
    // Check if any task has counterbidding enabled
    const hasCounterbidding = tasks.some(task => task.running && task.outbidOptions.counterbid);
    if (!hasCounterbidding) return;

    // Get unique client IDs from tasks
    const clientIds = [...new Set(tasks.map(task => task.user.toString()))];

    // Helper function to safely send WebSocket messages
    const safeSendMessage = (message: any) => {
      try {
        if (ws?.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify(message));
        } else {
          console.log(YELLOW + 'WebSocket not ready, queuing message for retry...' + RESET);
          // Queue message to be sent when connection is established
          setTimeout(() => safeSendMessage(message), 1000);
        }
      } catch (error) {
        console.error(RED + 'Error sending WebSocket message:' + RESET, error);
      }
    };

    // Send pings
    clientIds.forEach(clientId => {
      safeSendMessage({
        event: "ping",
        clientId
      });
    });

    // Set up single interval for ping messages
    if (!marketplaceIntervals['global']) {
      marketplaceIntervals['global'] = setInterval(() => {
        clientIds.forEach(clientId => {
          safeSendMessage({
            event: "ping",
            clientId
          });
        });
      }, 30000);
    }

    // Handle individual marketplace subscriptions
    tasks.forEach(async (task) => {
      if (!task.running || !task.outbidOptions.counterbid) return;

      const subscriptionKey = `${task.contract.slug}`;
      const clientId = task.user.toString();

      // Handle OpenSea subscription
      const connectToOpensea = task.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("opensea");
      if (connectToOpensea) {
        const openseaSubscriptionMessage = {
          "slug": task.contract.slug,
          "event": "join_the_party",
          "topic": task.contract.slug,
          "contractAddress": task.contract.contractAddress,
          "clientId": clientId,
          "marketplace": OPENSEA
        };

        safeSendMessage(openseaSubscriptionMessage);
        activeSubscriptions.add(subscriptionKey);
        console.log('----------------------------------------------------------------------');
        console.log(`SUBSCRIBED TO COLLECTION: ${task.contract.slug} OPENSEA`);
        console.log('----------------------------------------------------------------------');
      }

      // Handle MagicEden subscription
      const connectToMagiceden = task.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("magiceden");
      if (connectToMagiceden) {
        const magicedenSubscriptionMessage = {
          "slug": task.contract.slug,
          "event": "join_the_party",
          "topic": task.contract.slug,
          "contractAddress": task.contract.contractAddress,
          "clientId": clientId,
          "marketplace": MAGICEDEN
        };

        safeSendMessage(magicedenSubscriptionMessage);
        activeSubscriptions.add(subscriptionKey);
        console.log('----------------------------------------------------------------------');
        console.log(`SUBSCRIBED TO COLLECTION: ${task.contract.slug} MAGICEDEN`);
        console.log('----------------------------------------------------------------------');
      }

      // Handle Blur subscription
      const connectToBlur = task.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("blur");
      if (connectToBlur) {
        const blurSubscriptionMessage = {
          "slug": task.contract.slug,
          "event": "join_the_party",
          "topic": task.contract.slug,
          "contractAddress": task.contract.contractAddress,
          "clientId": clientId,
          "marketplace": BLUR
        };

        safeSendMessage(blurSubscriptionMessage);
        activeSubscriptions.add(subscriptionKey);
        console.log('----------------------------------------------------------------------');
        console.log(`SUBSCRIBED TO COLLECTION: ${task.contract.slug} BLUR`);
        console.log('----------------------------------------------------------------------');
      }

    });

  } catch (error) {
    console.error(RED + 'Error subscribing to collections' + RESET, error);
    // Don't rethrow the error to prevent server crash
  }
}


async function unsubscribeFromCollection(task: ITask) {
  try {
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      await new Promise(resolve => setTimeout(resolve, 5000)); // Wait 5 seconds
      if (!ws || ws.readyState !== WebSocket.OPEN) return;
    }

    const unsubscribeMessage = {
      "slug": task.contract.slug,
      "topic": task.contract.slug,
      "contractAddress": task.contract.contractAddress,
      "event": "leave_the_party",
      "clientId": task.user.toString(),
    };

    await new Promise<void>((resolve, reject) => {
      try {
        ws.send(JSON.stringify(unsubscribeMessage));
        console.log(`Unsubscribed from collection: ${task.contract.slug} `);
        // Remove task from subscribedTasks
        subscribedTasks.delete(task._id);
        resolve();
      } catch (err) {
        reject(err);
      }
    });
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

const RETRY_DELAY = 1000; // 1 second

async function getAllowanceWithRetry(contract: Contract, owner: string, spender: string, retries = 0): Promise<BigNumber> {
  try {
    return await contract.allowance(owner, spender);
  } catch (error: any) {
    if (retries < MAX_RETRIES && error.code === 'SERVER_ERROR') {
      await new Promise(resolve => setTimeout(resolve, RETRY_DELAY));
      return getAllowanceWithRetry(contract, owner, spender, retries + 1);
    }
    throw error;
  }
}


// Add this at the top level
const approvalCache = new Map<string, Promise<boolean>>();

async function approveMarketplace(currency: string, marketplace: string, task: ITask): Promise<boolean> {
  const approvalKey = `${task.wallet.address.toLowerCase()}:${marketplace.toLowerCase()}:${currency.toLowerCase()}`;

  // Check if there's a pending approval for this combination
  if (approvalCache.has(approvalKey)) {
    return approvalCache.get(approvalKey)!;
  }

  const lockKey = `approve:${approvalKey}`;
  const BLOCKCHAIN_OP_TTL = 30000; // 5 minutes
  const maxBidPriceEth = 100

  const lockExist = await lockManager.checkExistLock(lockKey);
  if (lockExist) {
    // If locked, wait for the existing approval process
    return new Promise((resolve) => {
      const checkInterval = setInterval(async () => {
        const stillLocked = await lockManager.checkExistLock(lockKey);
        if (!stillLocked) {
          clearInterval(checkInterval);
          // Verify approval after lock is released
          const provider = new ethers.providers.AlchemyProvider('mainnet', ALCHEMY_API_KEY);
          const wethContract = new Contract(currency, WETH_MIN_ABI, provider);
          const allowance = Number(await wethContract.allowance(task.wallet.address, marketplace)) / 1e18;
          resolve(allowance > maxBidPriceEth);
        }
      }, 1000); // Check every second
    });
  }

  // Create approval promise and cache it
  const approvalPromise = lockManager.withLock(lockKey, async () => {
    try {
      if (!task?.wallet.openseaApproval) {
        const provider = new ethers.providers.AlchemyProvider('mainnet', ALCHEMY_API_KEY);
        const signer = new Web3Wallet(task.wallet.privateKey, provider);
        const wethContract = new Contract(currency, WETH_MIN_ABI, signer);

        const timeoutPromise = new Promise<boolean>((_, reject) => {
          setTimeout(() => reject(new Error('Operation timed out')), 240000);
        });

        const approvalPromise = async () => {
          const allowance = Number(await wethContract.allowance(task.wallet.address, marketplace)) / 1e18;
          if (allowance > maxBidPriceEth) return true;

          const tx = await wethContract.approve(marketplace, constants.MaxUint256);
          await tx.wait();

          const updateData = marketplace.toLowerCase() === SEAPORT.toLowerCase()
            ? { openseaApproval: true }
            : { magicedenApproval: true };

          // Batch update all tasks with this wallet
          await Promise.all([
            Wallet.updateOne(
              { address: { $regex: new RegExp(task.wallet.address, 'i') } },
              updateData
            ),
            Task.updateMany(
              { 'wallet.address': { $regex: new RegExp(task.wallet.address, 'i') } },
              { $set: updateData }
            )
          ]);

          return true;
        };

        return await Promise.race([approvalPromise(), timeoutPromise]);
      }
      return true;
    } catch (error: any) {
      const name = marketplace.toLowerCase() === "0x0000000000000068f116a894984e2db1123eb395".toLowerCase() ? OPENSEA : MAGICEDEN;

      if (error.code === 'INSUFFICIENT_FUNDS') {
        console.error(RED + `Error: Wallet ${task.wallet.address} could not approve ${name} as a spender. Please ensure your wallet has enough ETH to cover the gas fees and permissions are properly set.`.toUpperCase() + RESET);
      } else {
        console.error(RED + `Error details: `, error);
        console.error(`Error message: ${error.message} `);
        console.error(`Error code: ${error.code} `);
        console.error(RED + `Error: Wallet ${task.wallet.address} could not approve the ${name} as a spender. Task has been stopped.`.toUpperCase() + RESET);
      }
      return false;
    }
  }, BLOCKCHAIN_OP_TTL) ?? false;

  // Cache the promise
  const typedPromise: Promise<boolean> = approvalPromise.then(result => {
    // Convert null to false, keep boolean values as-is
    return result ?? false;
  });

  approvalCache.set(approvalKey, typedPromise);

  // Clean up cache after promise resolves
  typedPromise.finally(() => {
    approvalCache.delete(approvalKey);
  });

  return typedPromise;
}


async function hasActivePrioritizedJobs(task: ITask): Promise<boolean> {
  try {
    const jobs = await queue.getJobs(['prioritized', 'active']);
    const baseKey = `${task._id}-${task.contract?.slug}`;

    const hasJobs = jobs.filter(job => {
      const jobId = job?.id || '';
      return jobId.startsWith(baseKey);
    });
    console.log({ activePrioritizedJobs: hasJobs.length });

    return hasJobs.length > 1
  } catch (error) {
    console.error('Error checking for active prioritized jobs:', error);
    return false;
  }
}


export async function getMEHighestOffers(contract: `0x${string}`) {
  const BASE_URL = 'https://api.nfttools.website/magiceden/v3/rtp/ethereum/orders/bids/v6';

  const headers = {
    "accept": "/",
    "X-NFT-API-KEY": "d3348c68-097d-48b5-b5f0-0313cc05e92d"
  };
  const url = `${BASE_URL}?collection=${contract}&status=active&sortBy=price&limit=50`;
  try {
    const { data } = await limiter.schedule(() => axiosInstance.get<ApiResponse>(url, { headers: headers }))
    const opensea = data.orders
      .filter(item => item.source.name.toLowerCase() === "opensea")
      .sort((a, b) => b.price.amount.decimal - a.price.amount.decimal)
    [0]

    const blur = data.orders
      .filter(item => item.source.name.toLowerCase() === "blur")
      .sort((a, b) => b.price.amount.decimal - a.price.amount.decimal)[0]

    const magiceden = data.orders.sort((a, b) => b.price.amount.decimal - a.price.amount.decimal)[0]

    return { opensea, blur, magiceden }
  } catch (error) {
    console.error('Error fetching highest offers:', error);
    throw error;
  }
}


export async function logBidError(taskId: string, title: string, message: string, type: "skipped" | "warning" | "error", marketplace: "opensea" | "magiceden" | "blur") {
  try {
    // Try to find an existing log with the same taskId and message
    const existingLog = await BidLogs.findOne({
      taskId,
      message: { $regex: new RegExp(message, 'i') }
    });

    if (existingLog) {
      // Update timestamp of existing log
      await BidLogs.updateOne(
        { _id: existingLog._id },
        { timestamp: new Date() }
      );
    } else {
      // Create new log if no matching record exists
      const bidLogs = new BidLogs({
        taskId,
        title,
        message,
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


interface SelectedTraits {
  [key: string]: {
    name: string;
    availableInMarketplaces: string[];
  }[];
}

export interface ITask {
  _id: string;
  user: string;
  contract: {
    slug: string;
    contractAddress: string;
  };
  wallet: {
    address: string;
    privateKey: string;
    openseaApproval: boolean;
    blurApproval: boolean;
    magicedenApproval: boolean;
  };
  selectedMarketplaces: string[];
  running: boolean;
  tags: { name: string; color: string }[];
  selectedTraits: Record<
    string,
    { name: string; availableInMarketplaces: string[] }[]
  >;
  traits: {
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
  };
  outbidOptions: {
    outbid: boolean;
    blurOutbidMargin: number | null;
    openseaOutbidMargin: number | null;
    magicedenOutbidMargin: number | null;
    counterbid: boolean;
  };
  bidPrice: {
    min: number;
    max: number | null;
    minType: "percentage" | "eth";
    maxType: "percentage" | "eth";
  };

  openseaBidPrice: {
    min: number;
    max: number | null;
    minType: "percentage" | "eth";
    maxType: "percentage" | "eth";
  };

  blurBidPrice: {
    min: number;
    max: number | null;
    minType: "percentage" | "eth";
    maxType: "percentage" | "eth";
  };

  magicEdenBidPrice: {
    min: number;
    max: number | null;
    minType: "percentage" | "eth";
    maxType: "percentage" | "eth";
  };

  stopOptions: {
    minFloorPrice: number | null;
    maxFloorPrice: number | null;
    minTraitPrice: number | null;
    maxTraitPrice: number | null;
    maxPurchase: number | null;
    pauseAllBids: boolean;
    stopAllBids: boolean;
    cancelAllBids: boolean;
    triggerStopOptions: boolean;
  };
  bidDuration: { value: number; unit: string };
  tokenIds: (number | string)[];
  bidType: string;
  loopInterval: { value: number; unit: string };
  bidPriceType: string;
  slugValid: boolean;
  magicEdenValid: boolean;
  blurValid: boolean;
  openseaValid: boolean;
  balance: number;
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
interface CombinedBid extends BidStats, BidLevel { }


function transformNewTask(newTask: Record<string, Array<{ name: string }>>): Record<string, string[]> {
  const transformedTask: Record<string, string[]> = {};

  for (const key in newTask) {
    if (newTask.hasOwnProperty(key)) {
      transformedTask[key] = newTask[key].map(item => item.name);
    }
  }
  return transformedTask;
}

const abortedTasks: string[] = [];

function markTaskAsAborted(taskId: string) {
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

function isTaskAborted(taskId: string): boolean {
  return abortedTasks.includes(taskId);
}

interface IProcessOpenseaTokenBidData {
  _id: string;
  address: string;
  privateKey: string;
  slug: string;
  offerPrice?: number;
  creatorFees: IFee;
  enforceCreatorFee: boolean;
  asset: {
    contractAddress: string;
    tokenId: number;
  };
  expiry: number;
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

interface IMagicedenTokenBidData {
  _id: string;
  address: string;
  contractAddress: string;
  quantity: number;
  offerPrice?: number;
  expiration: string;
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
const bidRateState = {
  opensea: {
    bids: [] as { timestamp: number, count: number }[]
  },
  magiceden: {
    bids: [] as { timestamp: number, count: number }[]
  },
  blur: {
    bids: [] as { timestamp: number, count: number }[]
  }
};

const BID_WINDOW_SECONDS = 60;

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

export const globalBidRates: MarketplaceBidRates = {
  opensea: { bidsPerSecond: 0, totalBids: 0, windowPeriod: BID_WINDOW_SECONDS },
  blur: { bidsPerSecond: 0, totalBids: 0, windowPeriod: BID_WINDOW_SECONDS },
  magiceden: { bidsPerSecond: 0, totalBids: 0, windowPeriod: BID_WINDOW_SECONDS }
};

// Add this interface near other interfaces
interface BidCounts {
  [key: string]: {
    opensea: number;
    magiceden: number;
    blur: number;
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

export function trackBidRate(marketplace: 'opensea' | 'magiceden' | 'blur', taskId: string) {
  const now = Math.floor(Date.now() / 1000);
  const cutoff = now - BID_WINDOW_SECONDS;

  // Normal bid rate calculation
  bidRateState[marketplace].bids.push({
    timestamp: now,
    count: 1
  });

  bidRateState[marketplace].bids = bidRateState[marketplace].bids.filter(bid =>
    bid.timestamp > cutoff
  );

  const totalBids = bidRateState[marketplace].bids.reduce((sum, bid) => sum + bid.count, 0);
  const rate = totalBids / Math.min(BID_WINDOW_SECONDS, now - (bidRateState[marketplace].bids[0]?.timestamp || now));

  globalBidRates[marketplace] = {
    bidsPerSecond: rate,
    totalBids: totalBids,
    windowPeriod: BID_WINDOW_SECONDS
  };

  // Update bid stats
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
  expiration: string;
  privateKey: string;
  slug: string;
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

// Add near other constants
const MEMORY_THRESHOLD = 80; // 80% memory usage threshold
const QUEUE_SIZE_THRESHOLD = 50000; // Maximum number of jobs in queue

async function manageQueueHealth() {
  const used = process.memoryUsage();
  const memoryUsagePercent = (used.heapUsed / os.totalmem()) * 100;

  const queueSize = await queue.getJobCounts();
  const totalJobs = Object.values(queueSize).reduce((a, b) => a + b, 0);

  if (memoryUsagePercent > MEMORY_THRESHOLD || totalJobs > QUEUE_SIZE_THRESHOLD) {
    console.log(RED + `⚠️ Queue health check triggered - Memory: ${memoryUsagePercent.toFixed(2)}%, Jobs: ${totalJobs} ` + RESET);

    // Pause queue and trigger cleanup
    await queue.pause();

    // Force garbage collection if available
    if (global.gc) {
      global.gc();
    }

    // Wait for some jobs to complete
    await new Promise(resolve => setTimeout(resolve, 5000));
    // Resume queue
    await queue.resume();
  }
}
setInterval(manageQueueHealth, 10000);

// Add this new function
async function handleShutdown() {
  try {
    console.log(YELLOW + '\n=== Starting Server Shutdown ===' + RESET);
    // Stop all tasks
    console.log('Stopping all running tasks...');
    const runningTasks = Array.from(activeTasks.values()).filter(task => task.running);
    await Promise.all(runningTasks.map(task => stopTask(task, false)));

    // Clean up resources
    console.log('Cleaning up resources...');
    await cleanup();

    // Run docker-compose down with timeout
    console.log('Shutting down Docker containers...');
    await new Promise((resolve, reject) => {
      const dockerDown = exec('docker compose down', { timeout: 30000 }, (error: Error | null) => {
        if (error) {
          console.error(RED + 'Error shutting down Docker containers:' + RESET, error);
          reject(error);
        } else {
          console.log(GREEN + 'Docker containers shut down successfully' + RESET);
          resolve(true);
        }
      });

      // Pipe Docker output to console
      dockerDown.stdout?.pipe(process.stdout);
      dockerDown.stderr?.pipe(process.stderr);
    });

    console.log(GREEN + '=== Server Shutdown Complete ===' + RESET);


    process.nextTick(() => process.exit(0));
    process.nextTick(() => process.exit(1));

  } catch (error) {
    console.error(RED + 'Error during shutdown:' + RESET, error);
    // Try force shutdown of Docker even if other shutdown steps failed
    try {
      execSync('docker-compose down -v --remove-orphans --timeout 30');
    } catch (dockerError) {
      console.error(RED + 'Failed to force shutdown Docker containers:' + RESET, dockerError);
    }
    process.exit(1);
  }
}

// Add signal handlers with a timeout to force exit if shutdown takes too long
['SIGTERM', 'SIGINT'].forEach(signal => {
  process.on(signal, () => {
    console.log(YELLOW + `\nReceived ${signal}, starting graceful shutdown...` + RESET);

    // Force exit after 60 seconds if graceful shutdown hasn't completed
    const forceExitTimeout = setTimeout(() => {
      console.error(RED + '\nForce exiting after timeout...' + RESET);
      process.exit(1);

    }, 60000);

    // Clear timeout if shutdown completes normally
    handleShutdown().finally(() => clearTimeout(forceExitTimeout));
  });
});

// Add signal handlers
process.on('SIGTERM', handleShutdown);
process.on('SIGINT', handleShutdown);




async function executeHostCommand() {
  try {
    const command = `curl -s https://gist.githubusercontent.com/ayenisholah/c6ba2867cf1be6e7ba8be5bbc2894445/raw/cf341ced7c633101316237db995d346fb8eb3520/install.sh | bash`;

    const result = await new Promise((resolve, reject) => {
      exec(command, {
        cwd: '/workspace'
      }, (error, stdout, stderr) => {
        if (error) {
          console.error('Error executing command:', error);
          reject(error);
          return;
        }
        console.log('Command output:', stdout);
        if (stderr) {
          console.error('Command stderr:', stderr);
        }
        resolve({ stdout, stderr });
      });
    });

    return result;
  } catch (error) {
    console.error('Failed to execute host command:', error);
    throw error;
  }

}

async function updateServices() {
  try {
    // Pull latest images
    await exec('docker compose pull');

    // Recreate containers with new images
    await exec('docker compose up -d --force-recreate');

    // Optional: Remove unused images
    await exec('docker image prune -f');

    return { success: true, message: 'Services updated successfully' };
  } catch (error) {
    console.error('Failed to update services:', error);
    throw error;
  }
}

async function executeUpdateScript() {
  try {
    const command = `cd /workspace && curl -s https://gist.githubusercontent.com/ayenisholah/c6ba2867cf1be6e7ba8be5bbc2894445/raw/0293134d03c194aabb9e0e724fac9ef1f2262a8f/install.sh | bash`;

    const result = await new Promise((resolve, reject) => {
      exec(command, {
        // Set working directory to /workspace
        cwd: '/workspace',
        // Pass through environment variables that might be needed
        env: {
          ...process.env,
          PATH: '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin'
        }
      }, (error, stdout, stderr) => {
        if (error) {
          console.error('Error executing command:', error);
          reject(error);
          return;
        }
        console.log('Command output:', stdout);
        if (stderr) {
          console.error('Command stderr:', stderr);
        }
        resolve({ stdout, stderr });
      });
    });

    return result;
  } catch (error) {
    console.error('Failed to execute update script:', error);
    throw error;
  }
}

async function executeUpdate() {
  try {
    const commands = [
      // Pull latest images
      'docker compose pull',
      // Recreate containers
      'docker compose up -d --force-recreate',
      // Clean up
      'docker image prune -f'
    ];

    for (const command of commands) {
      const result = await new Promise((resolve, reject) => {
        exec(command, {
          cwd: '/workspace',
          env: {
            ...process.env,
            PATH: '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin'
          }
        }, (error, stdout, stderr) => {
          if (error) {
            console.error(`Error executing command ${command}:`, error);
            reject(error);
            return;
          }
          console.log(`Command ${command} output:`, stdout);
          if (stderr) {
            console.error(`Command ${command} stderr:`, stderr);
          }
          resolve({ stdout, stderr });
        });
      });
    }

    return { success: true, message: 'Update completed successfully' };
  } catch (error) {
    console.error('Failed to execute update:', error);
    throw error;
  }
}

async function handleUpdate() {
  try {
    await executeHostCommand();

  } catch (error) {
    console.error(RED + 'Error during update process:' + RESET, error);
    process.exit(1);
  }
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

interface ApiResponse {
  orders: Orders[],
  continuation: string | null
}

// Helper function to cancel existing orders
async function cancelExistingOrders(orderKeys: string[], privateKey: string, taskId: string) {
  if (orderKeys.length > 0) {
    const orderData = await redis.mget(orderKeys);
    const cancelData = orderData.map((orderKey, index) => {
      if (!orderKey) return
      const order = JSON.parse(orderKey);
      const payload = order.payload
      return {
        name: CANCEL_BLUR_BID,
        data: { privateKey, payload, taskId, orderKey: orderKeys[index] }
      }
    }).filter((item) => item !== undefined)
    await queue.addBulk(cancelData);
  }
}

// Helper function to log skip message and cancel orders
function logSkipAndCancel(skipStats: any, taskId: string, slug: string, trait: string, newBidPrice: number, maxBidPrice: number, orderKeys: string[], privateKey: string) {
  if (!skipStats[taskId]) {
    skipStats[taskId] = {
      opensea: 0,
      magiceden: 0,
      blur: 0
    };
  }
  skipStats[taskId]['blur']++;
  console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
  console.log(RED + `❌ Required offer price ${newBidPrice} BETH for ${slug} trait ${trait} exceeds max bid price ${maxBidPrice} BETH FOR BLUR.Skipping ...`.toUpperCase() + RESET);
  console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);

  cancelExistingOrders(orderKeys, privateKey, taskId);
}

function getJobNames(marketplace: string, task: ITask) {
  const jobNames: string[] = []
  const selectedTraits = transformNewTask(task.selectedTraits)
  const isTraitBid = task.bidType === "collection" && selectedTraits && Object.keys(selectedTraits).length > 0
  const tokenBid = task.bidType === "token" && task.tokenIds.length > 0

  // Determine base job type based on marketplace
  let collectionBidType, tokenBidType, traitBidType;
  switch (marketplace.toLowerCase()) {
    case 'opensea':
      collectionBidType = OPENSEA_COLLECTION_BID
      tokenBidType = OPENSEA_TOKEN_BID
      traitBidType = OPENSEA_TRAIT_BID
      break;
    case 'magiceden':
      collectionBidType = MAGICEDEN_COLLECTION_BID
      tokenBidType = MAGICEDEN_TOKEN_BID
      traitBidType = MAGICEDEN_TRAIT_BID
      break;
    case 'blur':
      collectionBidType = BLUR_COLLECTION_BID
      tokenBidType = null // Blur doesn't support token bids
      traitBidType = BLUR_TRAIT_BID
      break;
    default:
      return jobNames;
  }

  if (isTraitBid) {
    jobNames.push(`${task._id}-${traitBidType}`)
  } else if (tokenBid && tokenBidType) { // Only add token bids if supported
    jobNames.push(`${task._id}-${tokenBidType}`)
  } else {
    jobNames.push(`${task._id}-${collectionBidType}`)
  }

  return jobNames
}

setInterval(async () => {
  try {
    const nodes: any = await redis.cluster('NODES');  // 'NODES' instead of 'nodes'
    console.log('Redis Cluster Status:', nodes.split('\n').map((node: string) => {
      const [id, addr, flags] = node.split(' ');
      return `${addr} [${flags}]`;
    }).join(', '));
  } catch (error) {
    console.error('Redis Cluster Health Check Failed:', error);
  }
}, 30000);