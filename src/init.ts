import Bottleneck from "bottleneck";
import axios, { AxiosInstance } from "axios";
import axiosRetry, { IAxiosRetryConfig } from "axios-retry";
import { config } from "dotenv";
import { RATE_LIMIT, rps } from ".";
import { wrapAxiosInstance, jobContext } from "./utils/axios-wrapper";

config()

let limiter: Bottleneck;
let axiosInstance: AxiosInstance;
let countLoggerInterval: NodeJS.Timeout | null = null;
let previousDoneCount = 0;
let trueRPS = 0;
// Add an array to store recent RPS values for averaging
const rpsHistory: number[] = [];
const RPS_HISTORY_SECONDS = 10; // Keep a 10-second history for averaging

const retryConfig: IAxiosRetryConfig = {
  retries: 3, // Reduced to 1 retry for 408 errors
  retryDelay: (retryCount, error) => {
    limiter.schedule(() => Promise.resolve());

    // Special handling for 408 errors
    if (error.response?.status === 408) {
      const url = error.config?.url || '';

      // Blur endpoints need extra time due to rate limiting
      if (url.includes('/blur/')) {
        const baseDelay = 30000; // 30 seconds for Blur
        const progressiveDelay = baseDelay + (retryCount - 1) * 10000; // +10s each retry (30s, 40s, 50s)
        const jitter = 1000 + Math.random() * 2000; // 1-3s jitter
        console.log(`[Blur 408] Retrying Blur endpoint after 408 (attempt ${retryCount}), waiting ${progressiveDelay + jitter}ms`);
        return progressiveDelay + jitter;
      }

      // Authentication endpoints get exponential backoff
      if (url.includes('/auth/challenge') || url.includes('/auth/login')) {
        // Exponential backoff starting at 5 seconds for auth endpoints
        const baseDelay = 5000; // 5 seconds
        const exponentialDelay = Math.min(baseDelay * Math.pow(2, retryCount - 1), 40000); // Max 40 seconds
        const jitter = 500 + Math.random() * 1500; // 500-2000ms jitter
        console.log(`[Auth Backoff] Retrying auth endpoint after 408 (attempt ${retryCount}), waiting ${exponentialDelay + jitter}ms`);
        return exponentialDelay + jitter;
      } else {
        // Progressive delay for other 408 errors
        const delays = [20000, 30000, 40000]; // 20s, 30s, 40s
        const delay = delays[Math.min(retryCount - 1, delays.length - 1)];
        const jitter = 200 + Math.random() * 800; // 200-1000ms jitter
        console.log(`Retrying after 408 error (attempt ${retryCount}), waiting ${delay + jitter}ms`);
        return delay + jitter;
      }
    }

    // Rate limit errors - respect retry-after header or use exponential backoff
    if (error.response?.status === 429 || error.response?.status === 430) {
      const retryAfter = error.response.headers['retry-after'];
      if (retryAfter) {
        return parseInt(retryAfter) * 1000;
      }
      return Math.min(Math.pow(2, retryCount) * 500, 4000); // Faster backoff
    }

    // Other 4xx/5xx errors - quick retry
    if (error.response && (error.response.status >= 400 && error.response.status < 600)) {
      return Math.min(retryCount * 500, 2000); // Max 2s
    }

    return axiosRetry.exponentialDelay(retryCount);
  },
  retryCondition: async (error: any) => {
    // Retry on network errors and 5xx status codes
    if (!error.response) {
      // Network error (no response)
      return true;
    }

    // Retry on server errors (5xx) and specific client errors
    const status = error.response.status;
    return status >= 500 || status === 429 || status === 408;
  },
  onRetry: (retryCount, error, requestConfig) => {
    console.log(`Retry attempt ${retryCount} for ${requestConfig.url} - Status: ${error.response?.status || 'Network Error'}`);
  }
};

async function initialize(rateLimit: number) {
  axiosInstance = axios.create({ timeout: 60000 });

  limiter = new Bottleneck({
    minTime: 1000 / rateLimit,
    maxConcurrent: rateLimit,
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

// Function to start logging limiter counts every second
export function startCountsLogger(intervalMs = 1000) {
  if (countLoggerInterval) {
    clearInterval(countLoggerInterval);
  }

  countLoggerInterval = setInterval(() => {
    const counts = getLimiterCounts();
    console.log('Limiter Counts:', counts);
  }, intervalMs);

  console.log(`Started logging limiter counts every ${intervalMs}ms`);
  return countLoggerInterval;
}

// Function to stop the counts logger
export function stopCountsLogger() {
  if (countLoggerInterval) {
    clearInterval(countLoggerInterval);
    countLoggerInterval = null;
    console.log('Stopped logging limiter counts');
  }
}
setInterval(() => {
  const counts = getLimiterCounts();
  console.log('Current Limiter Stats:', counts);
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

  console.log('Average RPS:', averageRPS.toFixed(2), 'Executing:', executing.toFixed(2), 'True RPS:', trueRPS.toFixed(2));

  rps.currentRPS = trueRPS

}, 1000);

// Task blacklist for preventing stopped tasks from executing
// Now supports marketplace-specific blacklisting
const taskBlacklist = new Map<string, Set<string>>();

export function cancelTaskJobs(taskId: string, marketplace?: string) {
  if (!limiter) return { cancelled: 0, blacklisted: false };

  // Add to blacklist to prevent new jobs
  if (!taskBlacklist.has(taskId)) {
    taskBlacklist.set(taskId, new Set());
  }

  if (marketplace) {
    // Marketplace-specific blacklist
    taskBlacklist.get(taskId)!.add(marketplace.toLowerCase());
    console.log(`Task ${taskId} blacklisted for marketplace: ${marketplace}`);
  } else {
    // Full task blacklist - add special marker
    taskBlacklist.get(taskId)!.add('*');
    console.log(`Task ${taskId} fully blacklisted`);
  }

  // Get current limiter queue size for reporting
  const counts = limiter.counts();
  const queuedJobs = counts.RECEIVED + counts.QUEUED;

  console.log(`Current limiter queue: ${queuedJobs} jobs`);

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
      console.log(`Task ${taskId} removed from blacklist for marketplace: ${marketplace}`);
    }
    // Clean up empty entries
    if (taskEntry.size === 0) {
      taskBlacklist.delete(taskId);
    }
  } else {
    // Remove entire task from blacklist
    taskBlacklist.delete(taskId);
    wasRemoved = true;
    console.log(`Task ${taskId} fully removed from blacklist - all marketplaces can execute again`);
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
    console.log(`Blocked job execution for stopped task: ${taskId}`);
    return Promise.reject(new Error(`Task ${taskId} is stopped`));
  }
  return limiter.schedule(fn);
}

export { limiter, initialize, axiosInstance };