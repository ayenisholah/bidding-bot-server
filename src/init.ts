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
  });
  axiosRetry(axiosInstance, retryConfig);

  // Define window size for RPS calculation
  const RPS_WINDOW_SECONDS = 60; // Adjust window size as needed
  const IDLE_THRESHOLD_SECONDS = 10; // Time without requests before considering RPS as 0
  const requests: { timestamp: number }[] = [];

  limiter.on("received", () => {
    const now = Math.floor(Date.now() / 1000);
    requests.push({ timestamp: now });

    // Remove old requests outside the window
    const cutoff = now - RPS_WINDOW_SECONDS;
    while (requests.length > 0 && requests[0].timestamp <= cutoff) {
      requests.shift();
    }

    // Check if the most recent request is older than the idle threshold
    const mostRecentRequest = requests[requests.length - 1]?.timestamp || 0;
    if (now - mostRecentRequest >= IDLE_THRESHOLD_SECONDS) {
      rps.currentRPS = 0;
      return;
    }

    // Calculate current RPS
    const windowSize = Math.min(RPS_WINDOW_SECONDS, now - (requests[0]?.timestamp || now));
    const currentRPS = requests.length / windowSize;

    rps.currentRPS = currentRPS;
  });

  // Log RPS periodically
  setInterval(() => {
    console.log(`Current RPS: ${rps.currentRPS.toFixed(2)}`);
  }, 1000);

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