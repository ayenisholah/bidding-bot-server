import Bottleneck from "bottleneck";
import axios, { AxiosInstance } from "axios";
import { config } from "dotenv";
import { RATE_LIMIT, rps } from ".";
import { wrapAxiosInstance, jobContext } from "./utils/axios-wrapper";
import { warnLog, errorLog } from "./utils/debug-file-logger";

config()

// Debug logging controlled by single environment variable
const DEBUG = process.env.DEBUG === 'true';
export const DEBUG_VERBOSE = process.env.DEBUG_VERBOSE === 'true';

let limiter: Bottleneck;
let axiosInstance: AxiosInstance;
let countLoggerInterval: NodeJS.Timeout | null = null;
let previousDoneCount = 0;
let trueRPS = 0;
// Add an array to store recent RPS values for averaging
const rpsHistory: number[] = [];
const RPS_HISTORY_SECONDS = 10; // Keep a 10-second history for averaging

// ============================================
// RETRY HELPERS (used by Bottleneck FAILED handler)
// ============================================
const MAX_RETRIES = 3;

function isRetryableError(error: any): boolean {
  // Don't retry client-side cancellations (AbortController, task stop)
  if (axios.isCancel(error) || error.code === 'ERR_CANCELED') return false;
  // Network errors (no response received) - retry
  if (!error.response) return true;
  const status = error.response.status;
  // Retry: rate limits (429/430), timeout (408), Cloudflare (520), gateway errors (502/504)
  return status === 429 || status === 430 || status === 408 || status === 520 || status === 502 || status === 504;
}

function calculateRetryDelay(retryCount: number, error: any): number {
  // Special handling for 408 errors
  if (error.response?.status === 408) {
    const url = error.config?.url || '';

    // Blur endpoints need extra time due to rate limiting
    if (url.includes('/blur/')) {
      const baseDelay = 60000; // 60 seconds for Blur
      const progressiveDelay = baseDelay + (retryCount - 1) * 10000; // +10s each retry (60s, 70s, 80s)
      const jitter = 1000 + Math.random() * 2000; // 1-3s jitter
      return progressiveDelay + jitter;
    }

    // Authentication endpoints get exponential backoff
    if (url.includes('/auth/challenge') || url.includes('/auth/login')) {
      const baseDelay = 5000; // 5 seconds
      const exponentialDelay = Math.min(baseDelay * Math.pow(2, retryCount - 1), 40000); // Max 40 seconds
      const jitter = 500 + Math.random() * 1500; // 500-2000ms jitter
      return exponentialDelay + jitter;
    }

    // Progressive delay for other 408 errors
    const delays = [40000, 60000, 80000]; // 40s, 60s, 80s
    const delay = delays[Math.min(retryCount - 1, delays.length - 1)];
    const jitter = 200 + Math.random() * 800; // 200-1000ms jitter
    return delay + jitter;
  }

  // Rate limit errors - respect retry-after header or use exponential backoff
  if (error.response?.status === 429 || error.response?.status === 430) {
    const retryAfter = error.response.headers['retry-after'];
    if (retryAfter) {
      return parseInt(retryAfter) * 1000;
    }
    // Exponential backoff: 2s, 4s, 8s (max 10s) + jitter
    const baseDelay = Math.min(Math.pow(2, retryCount) * 1000, 10000);
    const jitter = 100 + Math.random() * 400; // 100-500ms jitter
    return baseDelay + jitter;
  }

  // Cloudflare origin errors - need longer delays
  if (error.response?.status === 520) {
    // Exponential backoff: 5s, 15s, 30s + jitter
    const baseDelay = Math.min(5000 * Math.pow(2, retryCount - 1), 30000);
    const jitter = 500 + Math.random() * 1000; // 500-1500ms jitter
    return baseDelay + jitter;
  }

  // Gateway errors (502 Bad Gateway, 504 Gateway Timeout) - exponential backoff
  if (error.response?.status === 502 || error.response?.status === 504) {
    // Exponential backoff: 3s, 6s, 12s (max 15s) + jitter
    const baseDelay = Math.min(3000 * Math.pow(2, retryCount - 1), 15000);
    const jitter = 200 + Math.random() * 600; // 200-800ms jitter
    return baseDelay + jitter;
  }

  // Default exponential backoff
  return Math.pow(2, retryCount) * 1000;
}

async function initialize(rateLimit: number) {
  if (!Number.isFinite(rateLimit) || rateLimit <= 0) {
    throw new Error(`Invalid RATE_LIMIT: ${rateLimit}. RATE_LIMIT must be a positive finite number.`);
  }

  axiosInstance = axios.create({ timeout: 30000 });

  // Constant-rate limiter: exactly 1 request per (1000/rateLimit) ms
  // e.g. RATE_LIMIT=100 → minTime=10ms → 100 RPS, no bursts
  //
  // maxConcurrent = max HTTP requests in-flight simultaneously.
  // Must be >= rateLimit * (avgApiLatency / 1000) to sustain full throughput.
  // Real marketplace latencies: ~1000ms avg, spikes to 2000ms, 1-5% timeout at 10s.
  // rateLimit * 2 = 200 at rateLimit=100: handles 2000ms avg with headroom.
  // Priority still works: when all 200 slots are occupied (heavy load/spike),
  // highest-priority QUEUED job gets the next free slot.
  limiter = new Bottleneck({
    minTime: 1000 / rateLimit,
    maxConcurrent: Math.ceil(rateLimit * 2), // 200 at rateLimit=100
    trackDoneStatus: true,
  });

  // Retry via Bottleneck's native FAILED handler - retries re-enter the queue
  // and properly consume a rate-limit slot (wait for the next minTime interval)
  limiter.on("failed", async (error, jobInfo) => {
    if (jobInfo.retryCount >= MAX_RETRIES) return;
    if (!isRetryableError(error)) return;

    const delay = calculateRetryDelay(jobInfo.retryCount + 1, error);

    if (DEBUG) {
      warnLog('LimiterRetry', `Retry ${jobInfo.retryCount + 1}/${MAX_RETRIES} for ${error.config?.url || 'unknown'} - Status: ${error.response?.status || 'Network'} - Delay: ${delay}ms`);
    }

    return delay;
  });

  // Wrap axios instance to track API calls for timeout analysis
  wrapAxiosInstance(axiosInstance, () => jobContext.getJobId());
}

// Add a function to get current counts
export function getLimiterCounts() {
  if (!limiter) {
    return {
      RECEIVED: 0,
      QUEUED: 0,
      RUNNING: 0,
      EXECUTING: 0,
      DONE: 0
    };
  }
  return limiter.counts();
}

/**
 * Get current queue depth (total jobs in limiter).
 * Quick helper to check queue size without calling getLimiterCounts().
 */
export function getQueueDepth(): number {
  if (!limiter) return 0;
  const counts = getLimiterCounts();
  return (counts.RECEIVED || 0) + (counts.QUEUED || 0) + (counts.RUNNING || 0);
}

// Get detailed limiter queue state for debugging stopTask flow
export function getLimiterQueueDetails(rateLimit: number): {
  counts: ReturnType<typeof getLimiterCounts>;
  jobsInQueue: number;
  estimatedDrainTimeMs: number;
} {
  const counts = getLimiterCounts();
  const jobsInQueue = (counts.RECEIVED || 0) + (counts.QUEUED || 0) + (counts.RUNNING || 0);
  const estimatedDrainTimeMs = jobsInQueue * (1000 / rateLimit);

  return {
    counts,
    jobsInQueue,
    estimatedDrainTimeMs
  };
}

// Queue depth thresholds for proactive refill
// NOTE: These thresholds measure QUEUED + RECEIVED + RUNNING in Bottleneck.
// With ~2×RATE_LIMIT BullMQ slots (steady-state) and in-flight HTTP requests
// consuming slots, realistic queue depth ≈ totalSlots - (RATE_LIMIT × avgLatency).
// Previous values (2-5× RATE_LIMIT) were unreachable under normal API latency,
// causing constant false CRITICAL alerts.
const QUEUE_MINIMUM = RATE_LIMIT * 0.5; // HARD MINIMUM: 500ms buffer at full rate
const QUEUE_LOW_WATER_MARK = RATE_LIMIT * 1; // Refill trigger: 1 second of buffer
const QUEUE_TARGET = RATE_LIMIT * 1.5; // Target queue depth for smooth operation
const QUEUE_HIGH_WATER_MARK = RATE_LIMIT * 3; // Stop adding when queue exceeds 3x rate limit

/**
 * Check if the limiter queue needs refilling to maintain steady RPS.
 * Returns true when queue depth drops below low water mark (1x rate limit).
 */
export function shouldRefillQueue(): boolean {
  if (!limiter) return false;

  const counts = getLimiterCounts();
  const queueDepth = (counts.RECEIVED || 0) + (counts.QUEUED || 0) + (counts.RUNNING || 0);

  return queueDepth < QUEUE_LOW_WATER_MARK;
}

/**
 * Check if queue is below HARD MINIMUM (0.5x rate limit).
 * This is CRITICAL - queue should NEVER drop below this threshold.
 */
export function isBelowMinimum(): boolean {
  if (!limiter) return false;

  const counts = getLimiterCounts();
  const queueDepth = (counts.RECEIVED || 0) + (counts.QUEUED || 0) + (counts.RUNNING || 0);

  return queueDepth < QUEUE_MINIMUM;
}

/**
 * Check if the queue is too full and should pause adding new jobs.
 * Returns true when queue depth exceeds high water mark.
 */
export function shouldPauseQueueing(): boolean {
  if (!limiter) return false;

  const counts = getLimiterCounts();
  const queueDepth = (counts.RECEIVED || 0) + (counts.QUEUED || 0) + (counts.RUNNING || 0);

  return queueDepth > QUEUE_HIGH_WATER_MARK;
}

/**
 * Get recommended number of jobs to add to reach target queue depth.
 * Returns 0 if queue is at or above target.
 *
 * If below MINIMUM: returns jobs needed to reach TARGET (aggressive refill)
 * If below LOW_WATER: returns jobs needed to reach TARGET
 * Otherwise: returns 0
 */
export function getRecommendedRefillCount(): number {
  if (!limiter) return 0;

  const counts = getLimiterCounts();
  const queueDepth = (counts.RECEIVED || 0) + (counts.QUEUED || 0) + (counts.RUNNING || 0);

  // If at or above target, no refill needed
  if (queueDepth >= QUEUE_TARGET) return 0;

  // Calculate jobs needed to reach target
  const jobsNeeded = Math.ceil(QUEUE_TARGET - queueDepth);

  // If below minimum, this is URGENT - return full amount needed
  if (queueDepth < QUEUE_MINIMUM) {
    return jobsNeeded;
  }

  // If below low water mark, refill to target
  if (queueDepth < QUEUE_LOW_WATER_MARK) {
    return jobsNeeded;
  }

  // Between low water and target - no urgent need
  return 0;
}

/**
 * Get current queue health metrics for monitoring.
 */
export function getQueueHealth(): {
  queueDepth: number;
  minimumDepth: number;
  targetDepth: number;
  lowWaterMark: number;
  highWaterMark: number;
  isBelowMinimum: boolean;
  needsRefill: boolean;
  shouldPause: boolean;
  recommendedRefillCount: number;
  utilizationPercent: number;
  status: 'CRITICAL' | 'LOW' | 'HEALTHY' | 'HIGH' | 'OVERLOADED';
} {
  const counts = getLimiterCounts();
  const queueDepth = (counts.RECEIVED || 0) + (counts.QUEUED || 0) + (counts.RUNNING || 0);
  const belowMinimum = isBelowMinimum();
  const needsRefill = shouldRefillQueue();
  const shouldPause = shouldPauseQueueing();
  const recommendedRefillCount = getRecommendedRefillCount();
  const utilizationPercent = Math.round((queueDepth / QUEUE_HIGH_WATER_MARK) * 100);

  // Determine queue status
  let status: 'CRITICAL' | 'LOW' | 'HEALTHY' | 'HIGH' | 'OVERLOADED';
  if (queueDepth < QUEUE_MINIMUM) {
    status = 'CRITICAL';
  } else if (queueDepth < QUEUE_LOW_WATER_MARK) {
    status = 'LOW';
  } else if (queueDepth > QUEUE_HIGH_WATER_MARK) {
    status = 'OVERLOADED';
  } else if (queueDepth > QUEUE_TARGET * 1.2) {
    status = 'HIGH';
  } else {
    status = 'HEALTHY';
  }

  return {
    queueDepth,
    minimumDepth: QUEUE_MINIMUM,
    targetDepth: QUEUE_TARGET,
    lowWaterMark: QUEUE_LOW_WATER_MARK,
    highWaterMark: QUEUE_HIGH_WATER_MARK,
    isBelowMinimum: belowMinimum,
    needsRefill,
    shouldPause,
    recommendedRefillCount,
    utilizationPercent,
    status
  };
}

// Function to start logging limiter counts every second
export function startCountsLogger(intervalMs = 1000) {
  if (countLoggerInterval) {
    clearInterval(countLoggerInterval);
  }

  countLoggerInterval = setInterval(() => {
    // console.log('Limiter Counts:', getLimiterCounts());
  }, intervalMs);

  // console.log(`Started logging limiter counts every ${intervalMs}ms`);
  return countLoggerInterval;
}

// Function to stop the counts logger
export function stopCountsLogger() {
  if (countLoggerInterval) {
    clearInterval(countLoggerInterval);
    countLoggerInterval = null;
    // console.log('Stopped logging limiter counts');
  }
}
// Track low queue warnings to avoid log spam
let lowQueueWarningCount = 0;
let criticalQueueWarningCount = 0;
let lastLowQueueWarning = 0;
let lastCriticalQueueWarning = 0;

setInterval(() => {
  const counts = getLimiterCounts();
  // Calculate instantaneous RPS based on completed requests
  const completedInLastSecond = counts.DONE ? counts.DONE - previousDoneCount : 0;
  previousDoneCount = counts.DONE || 0;

  // Add current RPS to history
  rpsHistory.push(completedInLastSecond);

  // Keep only the last N seconds of history
  if (rpsHistory.length > RPS_HISTORY_SECONDS) {
    rpsHistory.shift();
  }

  // Calculate average RPS over the history window
  const executing = counts.EXECUTING + counts.RUNNING + counts.RECEIVED + counts.QUEUED
  // Filter out 0 values before calculating the average
  const nonZeroRpsHistory = rpsHistory.filter(val => val > 0);

  const averageRPS = nonZeroRpsHistory.length > 0
    ? nonZeroRpsHistory.reduce((sum, val) => sum + val, 0) / nonZeroRpsHistory.length
    : 0;
  trueRPS = Math.min(RATE_LIMIT, Math.max(executing, averageRPS));

  rps.currentRPS = trueRPS

  // Monitor queue health and warn when refill needed
  const queueDepth = executing;
  const health = getQueueHealth();
  const now = Date.now();

  // CRITICAL: Queue below HARD MINIMUM (2x rate limit)
  if (health.isBelowMinimum) {
    // Log immediately - this is critical
    if (now - lastCriticalQueueWarning > 2000) { // Every 2 seconds for critical issues
      criticalQueueWarningCount++;
      lastCriticalQueueWarning = now;
      errorLog('LimiterQueue', `CRITICAL_QUEUE_BELOW_MINIMUM | depth=${queueDepth} | minimum=${QUEUE_MINIMUM} | target=${QUEUE_TARGET} | recommendedRefill=${health.recommendedRefillCount} | status=${health.status} | criticalCount=${criticalQueueWarningCount}`);
    }
  }
  // LOW: Queue below low water mark (2.5x rate limit)
  else if (health.needsRefill) {
    // Only log warning every 5 seconds to avoid spam
    if (now - lastLowQueueWarning > 5000) {
      lowQueueWarningCount++;
      lastLowQueueWarning = now;
      warnLog('LimiterQueue', `LOW_QUEUE_DEPTH | depth=${queueDepth} | lowWaterMark=${QUEUE_LOW_WATER_MARK} | target=${QUEUE_TARGET} | recommendedRefill=${health.recommendedRefillCount} | status=${health.status} | warningCount=${lowQueueWarningCount}`);
    }
  }
  // HEALTHY or HIGH: Reset warning counters
  else {
    // Reset warning count when queue is healthy
    if ((lowQueueWarningCount > 0 || criticalQueueWarningCount > 0) && queueDepth >= QUEUE_TARGET) {
      lowQueueWarningCount = 0;
      criticalQueueWarningCount = 0;
    }
  }

}, 1000);

// Task blacklist for preventing stopped tasks from executing
// Now supports marketplace-specific blacklisting
const taskBlacklist = new Map<string, Set<string>>();

// Fix 5: Cache for blacklist checks to reduce redundant lookups
// Cache TTL of 100ms - checks within the same millisecond window return cached result
interface BlacklistCacheEntry {
  result: boolean;
  timestamp: number;
}
const blacklistCheckCache = new Map<string, BlacklistCacheEntry>();
const BLACKLIST_CACHE_TTL = 100; // 100ms cache

export function cancelTaskJobs(taskId: string, marketplace?: string) {
  if (!limiter) {
    warnLog('StopBlacklist', `BLACKLIST_NO_LIMITER | taskId=${taskId} | limiterNotInitialized=true`);
    return { cancelled: 0, blacklisted: false };
  }

  // Add to blacklist to prevent new jobs
  if (!taskBlacklist.has(taskId)) {
    taskBlacklist.set(taskId, new Set());
  }

  if (marketplace) {
    // Marketplace-specific blacklist
    taskBlacklist.get(taskId)!.add(marketplace.toLowerCase());
  } else {
    // Full task blacklist - add special marker
    taskBlacklist.get(taskId)!.add('*');
  }

  // Fix 5: Invalidate cache entries for this task to ensure immediate effect
  // Delete all cache entries that start with this taskId
  for (const cacheKey of blacklistCheckCache.keys()) {
    if (cacheKey.startsWith(`${taskId}-`)) {
      blacklistCheckCache.delete(cacheKey);
    }
  }

  // Get current limiter queue size for reporting
  const counts = limiter.counts();
  const queuedJobs = counts.RECEIVED + counts.QUEUED;

  return { cancelled: 0, blacklisted: true, queuedJobs };
}

export function removeTaskFromBlacklist(taskId: string, marketplace?: string) {
  const taskEntry = taskBlacklist.get(taskId);
  if (!taskEntry) {
    return false;
  }

  let wasRemoved = false;

  if (marketplace) {
    // Remove specific marketplace from blacklist
    wasRemoved = taskEntry.delete(marketplace.toLowerCase());

    // Clean up empty entries
    if (taskEntry.size === 0) {
      taskBlacklist.delete(taskId);
    }
  } else {
    // Remove entire task from blacklist
    taskBlacklist.delete(taskId);
    wasRemoved = true;
  }

  // Fix 5: Invalidate cache entries for this task to ensure immediate effect
  for (const cacheKey of blacklistCheckCache.keys()) {
    if (cacheKey.startsWith(`${taskId}-`)) {
      blacklistCheckCache.delete(cacheKey);
    }
  }

  return wasRemoved;
}

export function isTaskBlacklisted(taskId: string, marketplace?: string): boolean {
  // Fix 5: Check cache first to avoid redundant lookups
  const cacheKey = `${taskId}-${marketplace || '*'}`;
  const cached = blacklistCheckCache.get(cacheKey);
  const now = Date.now();

  if (cached && (now - cached.timestamp) < BLACKLIST_CACHE_TTL) {
    // Cache hit - return cached result without logging (reduces log noise)
    return cached.result;
  }

  // Cache miss - perform actual check
  const taskEntry = taskBlacklist.get(taskId);
  let result: boolean;

  if (!taskEntry) {
    result = false;
  } else if (taskEntry.has('*')) {
    result = true;
  } else if (marketplace) {
    result = taskEntry.has(marketplace.toLowerCase());
  } else {
    result = taskEntry.size > 0;
  }

  // Cache the result
  blacklistCheckCache.set(cacheKey, { result, timestamp: now });

  // Only log cache misses (actual checks) to reduce log noise

  return result;
}

// Enhanced limiter scheduling with task validation
export function scheduleWithTaskValidation(fn: () => PromiseLike<any>, taskId: string) {
  if (isTaskBlacklisted(taskId)) {
    return Promise.reject(new Error(`Task ${taskId} is stopped`));
  }
  return limiter.schedule(fn);
}

// Priority constants for Bottleneck limiter scheduling
// Lower number = higher priority (Bottleneck default)
export const PRIORITY = {
  COUNTERBID: 0,       // Counterbid bids + their paired cancels
  COUNTERBID_AUX: 1,   // CANCEL_OPENSEA_COUNTERBID, CANCEL_OPENSEA_OUTBID
  ORDER_CACHE: 1,      // Order cache operations
  SCHEDULED_CANCEL: 3, // Cancel old orders during task stop
  SCHEDULED_BID: 5,    // Normal scheduled bids
  FETCH_QUERY: 7,      // Floor prices, best offers, traits
  BACKGROUND: 9,       // Collection data warming
} as const;

// Schedule with priority support - lower number = higher priority (0 = highest)
export function scheduleWithPriority<T>(fn: () => PromiseLike<T>, priority?: number): Promise<T> {
  if (priority !== undefined) {
    return limiter.schedule({ priority }, fn);
  }
  return limiter.schedule(fn);
}

export { limiter, initialize, axiosInstance };
