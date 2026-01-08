import Bottleneck from "bottleneck";
import axios, { AxiosInstance } from "axios";
import axiosRetry, { IAxiosRetryConfig } from "axios-retry";
import { config } from "dotenv";
import { RATE_LIMIT, rps } from ".";
import { wrapAxiosInstance, jobContext } from "./utils/axios-wrapper";
import { debugLog, infoLog, warnLog, errorLog } from "./utils/debug-file-logger";

config()

// Debug logging controlled by single environment variable
const DEBUG = process.env.DEBUG === 'true';

let limiter: Bottleneck;
let axiosInstance: AxiosInstance;
let countLoggerInterval: NodeJS.Timeout | null = null;
let previousDoneCount = 0;
let trueRPS = 0;
// Add an array to store recent RPS values for averaging
const rpsHistory: number[] = [];
const RPS_HISTORY_SECONDS = 10; // Keep a 10-second history for averaging

const retryConfig: IAxiosRetryConfig = {
  retries: 3,
  retryDelay: (retryCount, error) => {
    limiter.schedule(() => Promise.resolve());

    // Special handling for 408 errors
    if (error.response?.status === 408) {
      const url = error.config?.url || '';

      // Blur endpoints need extra time due to rate limiting
      if (url.includes('/blur/')) {
        const baseDelay = 60000; // 60 seconds for Blur
        const progressiveDelay = baseDelay + (retryCount - 1) * 10000; // +10s each retry (60s, 70s, 80s)
        const jitter = 1000 + Math.random() * 2000; // 1-3s jitter
        // console.log(`[Blur 408] Retrying Blur endpoint after 408 (attempt ${retryCount}), waiting ${progressiveDelay + jitter}ms`);
        return progressiveDelay + jitter;
      }

      // Authentication endpoints get exponential backoff
      if (url.includes('/auth/challenge') || url.includes('/auth/login')) {
        // Exponential backoff starting at 5 seconds for auth endpoints
        const baseDelay = 5000; // 5 seconds
        const exponentialDelay = Math.min(baseDelay * Math.pow(2, retryCount - 1), 40000); // Max 40 seconds
        const jitter = 500 + Math.random() * 1500; // 500-2000ms jitter
        // console.log(`[Auth Backoff] Retrying auth endpoint after 408 (attempt ${retryCount}), waiting ${exponentialDelay + jitter}ms`);
        return exponentialDelay + jitter;
      } else {
        // Progressive delay for other 408 errors
        const delays = [40000, 60000, 80000]; // 40s, 60s, 80s
        const delay = delays[Math.min(retryCount - 1, delays.length - 1)];
        const jitter = 200 + Math.random() * 800; // 200-1000ms jitter
        // console.log(`Retrying after 408 error (attempt ${retryCount}), waiting ${delay + jitter}ms`);
        return delay + jitter;
      }
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

    return axiosRetry.exponentialDelay(retryCount);
  },
  retryCondition: async (error: any) => {
    if (!error.response) {
      return true; // Network errors
    }
    const status = error.response.status;
    // Retry: rate limits (429/430), timeout (408), Cloudflare (520), gateway errors (502/504)
    return status === 429 || status === 430 || status === 408 || status === 520 || status === 502 || status === 504;
  },
  onRetry: (_retryCount, _error, _requestConfig) => {
    // Only log retries in development mode or for specific debugging
    if (DEBUG) {
      // console.log(`[Axios Retry] Attempt ${_retryCount} for ${_requestConfig.url} - Status: ${_error.response?.status || 'Network Error'}`);
    }
  }
};

async function initialize(rateLimit: number) {
  axiosInstance = axios.create({ timeout: 60000 });

  limiter = new Bottleneck({
    minTime: 1000 / rateLimit, // Minimum time between job starts
    trackDoneStatus: true,
  });

  axiosRetry(axiosInstance, retryConfig);

  // Wrap axios instance to track API calls for timeout analysis
  wrapAxiosInstance(axiosInstance, () => jobContext.getJobId());

  // Limiter initialized
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

}, 1000);

// Task blacklist for preventing stopped tasks from executing
// Now supports marketplace-specific blacklisting
const taskBlacklist = new Map<string, Set<string>>();

// ============================================
// PENDING LIMITER JOB TRACKING (for abort capability)
// ============================================

/**
 * Tracks pending limiter jobs by taskId for abort capability.
 * When a task is stopped, we can reject all its pending jobs immediately
 * instead of letting them waste rate limit slots.
 */
interface PendingLimiterJob {
  taskId: string;
  marketplace: string;
  reject: (reason: Error) => void;
  jobId: string;
  controller: AbortController;
  queuedAt: number;
}

// Map taskId -> Set of pending jobs
const pendingLimiterJobs = new Map<string, Set<PendingLimiterJob>>();

/**
 * Schedule a function through the rate limiter with abort capability.
 * Jobs can be aborted when a task is stopped via abortPendingLimiterJobs().
 *
 * @param taskId - Task ID for tracking/abort
 * @param marketplace - Marketplace for logging
 * @param fn - Function to execute (receives optional AbortSignal)
 * @param jobId - Optional job ID for logging (defaults to generated ID)
 * @returns Promise that resolves with function result or rejects if aborted
 */
export async function scheduleWithAbort<T>(
  taskId: string,
  marketplace: string,
  fn: (signal?: AbortSignal) => Promise<T>,
  jobId?: string
): Promise<T> {
  // Early blacklist check before even queueing
  if (isTaskBlacklisted(taskId, marketplace)) {
    debugLog('LimiterAbort', `BLACKLIST_REJECT | taskId=${taskId} | marketplace=${marketplace} | reason=task_blacklisted_before_queue`);
    throw new Error(`Task ${taskId} is blacklisted for ${marketplace}`);
  }

  const controller = new AbortController();
  const actualJobId = jobId || `${taskId}-${marketplace}-${Date.now()}`;

  return new Promise<T>((resolve, reject) => {
    const pendingJob: PendingLimiterJob = {
      taskId,
      marketplace,
      reject,
      jobId: actualJobId,
      controller,
      queuedAt: Date.now()
    };

    // Add to pending tracking
    if (!pendingLimiterJobs.has(taskId)) {
      pendingLimiterJobs.set(taskId, new Set());
    }
    pendingLimiterJobs.get(taskId)!.add(pendingJob);

    const pendingCount = pendingLimiterJobs.get(taskId)?.size || 0;
    debugLog('LimiterPending', `ADD | taskId=${taskId} | marketplace=${marketplace} | jobId=${actualJobId} | pendingCount=${pendingCount}`);

    limiter.schedule({ id: actualJobId }, async () => {
      // Remove from pending tracking since we're now executing
      pendingLimiterJobs.get(taskId)?.delete(pendingJob);
      const remainingPending = pendingLimiterJobs.get(taskId)?.size || 0;
      debugLog('LimiterPending', `EXECUTE | taskId=${taskId} | jobId=${actualJobId} | remainingPending=${remainingPending}`);

      // Check blacklist again after dequeue (task may have been stopped while waiting)
      if (isTaskBlacklisted(taskId, marketplace)) {
        debugLog('LimiterAbort', `BLACKLIST_ABORT | taskId=${taskId} | marketplace=${marketplace} | jobId=${actualJobId} | reason=task_blacklisted_while_in_queue`);
        throw new Error(`Task ${taskId} aborted - blacklisted while in limiter queue`);
      }

      // Check if we were aborted while waiting in queue
      if (controller.signal.aborted) {
        debugLog('LimiterAbort', `SIGNAL_ABORT | taskId=${taskId} | jobId=${actualJobId} | reason=abort_signal_triggered`);
        throw new Error(`Task ${taskId} aborted - signal triggered`);
      }

      // Execute the actual function, passing the signal for HTTP abort capability
      return fn(controller.signal);
    })
    .then((result) => {
      debugLog('LimiterPending', `COMPLETE | taskId=${taskId} | jobId=${actualJobId} | result=success`);
      resolve(result);
    })
    .catch((err) => {
      // Clean up in case of error (may already be deleted if aborted)
      pendingLimiterJobs.get(taskId)?.delete(pendingJob);
      debugLog('LimiterPending', `FAILED | taskId=${taskId} | jobId=${actualJobId} | error=${err.message}`);
      reject(err);
    });
  });
}

/**
 * Abort all pending limiter jobs for a task.
 * Called during stopTask() to immediately cancel jobs waiting in the limiter queue.
 *
 * @param taskId - Task ID to abort jobs for
 * @returns Number of jobs aborted
 */
export function abortPendingLimiterJobs(taskId: string): number {
  const pending = pendingLimiterJobs.get(taskId);
  if (!pending || pending.size === 0) {
    debugLog('LimiterAbort', `ABORT_START | taskId=${taskId} | pendingJobCount=0`);
    return 0;
  }

  infoLog('LimiterAbort', `ABORT_START | taskId=${taskId} | pendingJobCount=${pending.size}`);

  let abortedCount = 0;
  for (const job of pending) {
    try {
      // Abort the controller (will cancel HTTP request if in flight)
      job.controller.abort();

      // Reject the promise
      job.reject(new Error(`Task ${taskId} stopped - limiter job aborted`));

      debugLog('LimiterAbort', `ABORT_JOB | taskId=${taskId} | marketplace=${job.marketplace} | jobId=${job.jobId} | waitedMs=${Date.now() - job.queuedAt}`);
      abortedCount++;
    } catch (err) {
      errorLog('LimiterAbort', `ABORT_ERROR | taskId=${taskId} | jobId=${job.jobId}`, err);
    }
  }

  // Clear the set
  pendingLimiterJobs.delete(taskId);

  infoLog('LimiterAbort', `ABORT_COMPLETE | taskId=${taskId} | abortedCount=${abortedCount}`);
  return abortedCount;
}

/**
 * Get count of pending limiter jobs for a task.
 * Useful for monitoring and debugging.
 */
export function getPendingLimiterJobCount(taskId: string): number {
  return pendingLimiterJobs.get(taskId)?.size || 0;
}

/**
 * Get all pending limiter job counts by task.
 * Returns a summary object for monitoring.
 */
export function getAllPendingLimiterJobs(): { taskId: string; count: number; marketplaces: string[] }[] {
  const result: { taskId: string; count: number; marketplaces: string[] }[] = [];
  for (const [taskId, jobs] of pendingLimiterJobs) {
    const marketplaces = [...new Set([...jobs].map(j => j.marketplace))];
    result.push({ taskId, count: jobs.size, marketplaces });
  }
  return result;
}

export function cancelTaskJobs(taskId: string, marketplace?: string) {
  if (!limiter) return { cancelled: 0, blacklisted: false };

  // Add to blacklist to prevent new jobs
  if (!taskBlacklist.has(taskId)) {
    taskBlacklist.set(taskId, new Set());
  }

  if (marketplace) {
    // Marketplace-specific blacklist
    taskBlacklist.get(taskId)!.add(marketplace.toLowerCase());
    // console.log(`Task ${taskId} blacklisted for marketplace: ${marketplace}`);
  } else {
    // Full task blacklist - add special marker
    taskBlacklist.get(taskId)!.add('*');
    // console.log(`Task ${taskId} fully blacklisted`);
  }

  // Get current limiter queue size for reporting
  const counts = limiter.counts();
  const queuedJobs = counts.RECEIVED + counts.QUEUED;

  // console.log(`Current limiter queue: ${queuedJobs} jobs`);

  return { cancelled: 0, blacklisted: true, queuedJobs };
}

export function removeTaskFromBlacklist(taskId: string, marketplace?: string) {
  const taskEntry = taskBlacklist.get(taskId);
  if (!taskEntry) return false;

  let wasRemoved = false;

  if (marketplace) {
    // Remove specific marketplace from blacklist
    wasRemoved = taskEntry.delete(marketplace.toLowerCase());
    if (wasRemoved) {
      // console.log(`Task ${taskId} removed from blacklist for marketplace: ${marketplace}`);
    }
    // Clean up empty entries
    if (taskEntry.size === 0) {
      taskBlacklist.delete(taskId);
    }
  } else {
    // Remove entire task from blacklist
    taskBlacklist.delete(taskId);
    wasRemoved = true;
    // console.log(`Task ${taskId} fully removed from blacklist - all marketplaces can execute again`);
  }

  return wasRemoved;
}

export function isTaskBlacklisted(taskId: string, marketplace?: string): boolean {
  const taskEntry = taskBlacklist.get(taskId);
  if (!taskEntry) return false;

  // Check if fully blacklisted
  if (taskEntry.has('*')) return true;

  // Check marketplace-specific blacklist
  if (marketplace) {
    return taskEntry.has(marketplace.toLowerCase());
  }

  // If no marketplace specified, check if any marketplace is blacklisted
  return taskEntry.size > 0;
}

// Enhanced limiter scheduling with task validation
export function scheduleWithTaskValidation(fn: () => PromiseLike<any>, taskId: string) {
  if (isTaskBlacklisted(taskId)) {
    // console.log(`Blocked job execution for stopped task: ${taskId}`);
    return Promise.reject(new Error(`Task ${taskId} is stopped`));
  }
  return limiter.schedule(fn);
}

// Schedule with priority support - lower number = higher priority (0 = highest)
export function scheduleWithPriority<T>(fn: () => PromiseLike<T>, priority?: number): Promise<T> {
  if (priority !== undefined) {
    return limiter.schedule({ priority }, fn);
  }
  return limiter.schedule(fn);
}

export { limiter, initialize, axiosInstance };