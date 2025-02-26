import Bottleneck from "bottleneck";
import axios, { AxiosInstance } from "axios";
import axiosRetry, { IAxiosRetryConfig } from "axios-retry";
import { config } from "dotenv";
import { rps } from ".";

config()

let limiter: Bottleneck;
let axiosInstance: AxiosInstance;

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
    maxConcurrent: rateLimit * 1.5,
  });
  axiosRetry(axiosInstance, retryConfig);

  // Define window size for RPS calculation
  const RPS_WINDOW_MS = 10000; // 60 seconds in milliseconds
  const IDLE_THRESHOLD_MS = 10000; // 10 seconds in milliseconds
  const requests: { timestamp: number }[] = [];

  limiter.on("done", () => {
    const now = Date.now();
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
  return {
    queued: limiter.queued(),
    running: limiter.running(),
    done: limiter.done()
  };
}

export { limiter, initialize, axiosInstance };