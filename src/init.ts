import Bottleneck from "bottleneck";
import axios, { AxiosInstance } from "axios";
import axiosRetry, { IAxiosRetryConfig } from "axios-retry";
import { config } from "dotenv";
import { rps } from ".";

config()

let limiter: Bottleneck;
let axiosInstance: AxiosInstance;
let countLoggerInterval: NodeJS.Timeout | null = null;
let firstRequestTimestamp: number | null = null;

async function fetchRateLimitFromDatabase() {
  return { rateLimit: Number(process.env.RATE_LIMIT), apiKey: process.env.API_KEY as string };
}

const retryConfig: IAxiosRetryConfig = {
  retries: 3,
  retryDelay: (retryCount, error) => {
    limiter.schedule(() => Promise.resolve());
    if (error.response && (error.response.status >= 400 && error.response.status < 600)) {
      return 1000;
    }
    return axiosRetry.exponentialDelay(retryCount);
  },
  retryCondition: async (error: any) => {
    if (error.response && (error.response.status >= 400 && error.response.status < 600)) {
      return true;
    }
    if (axiosRetry.isNetworkError(error)) {
      return true;
    }
    return false;
  },
};

async function initialize(rateLimit: number) {
  axiosInstance = axios.create({
    timeout: 300000,
  });

  limiter = new Bottleneck({
    minTime: 1000 / rateLimit,
    trackDoneStatus: true, // Enable tracking of DONE status
  });
  axiosRetry(axiosInstance, retryConfig);

  // Reset first request timestamp on initialization
  firstRequestTimestamp = null;

  // Define window size for RPS calculation
  const RPS_WINDOW_MS = 10000; // 60 seconds in milliseconds
  const IDLE_THRESHOLD_MS = 10000; // 10 seconds in milliseconds
  const requests: { timestamp: number }[] = [];

  limiter.on("received", () => {
    const now = Date.now();

    // Track first request timestamp
    if (firstRequestTimestamp === null) {
      firstRequestTimestamp = now;
    }

    requests.push({ timestamp: now });

    // Remove old requests outside the window
    const cutoff = now - RPS_WINDOW_MS;
    while (requests.length > 0 && requests[0].timestamp <= cutoff) {
      requests.shift();
    }

    // Check if the most recent request is older than the idle threshold
    if (requests.length === 0 || (now - requests[requests.length - 1].timestamp) >= IDLE_THRESHOLD_MS) {
      rps.currentRPS = 0;
      return;
    }

    const windowDuration = now - requests[0].timestamp;
    // Avoid division by zero and convert to seconds
    rps.currentRPS = windowDuration > 0 ? (requests.length / (windowDuration / 1000)) : 0;
  });
  console.log(`Limiter initialized with rate limit: ${rateLimit} requests per second`);
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
}, 1000);


export { limiter, initialize, axiosInstance };