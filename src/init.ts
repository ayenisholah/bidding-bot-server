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
  axiosInstance = axios.create({ timeout: 60000 });

  limiter = new Bottleneck({
    minTime: 1000 / rateLimit,
    trackDoneStatus: true,
  });

  axiosRetry(axiosInstance, retryConfig);
  
  // Wrap axios instance to track API calls for timeout analysis
  wrapAxiosInstance(axiosInstance, () => jobContext.getJobId());
  
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

export { limiter, initialize, axiosInstance };