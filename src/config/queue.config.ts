import { QueueOptions, WorkerOptions, JobsOptions } from 'bullmq';

// Optimized configuration for 2 vCPU / 4GB system
export const OPTIMIZED_SYSTEM_CONFIG = {
  // System constraints
  TOTAL_CPUS: 2,
  TOTAL_MEMORY_GB: 4,
  
  // Worker configuration
  WORKER_COUNT: 2,                      // 1 worker per CPU
  CONCURRENCY_PER_WORKER: 10,           // 10 jobs per worker = 20 total
  
  // Rate limiting
  API_RATE_LIMIT: 20,                   // Reduced from 100
  BROADCAST_INTERVAL: 5000,             // 5 seconds (was 1 second)
  VALIDATE_INTERVAL: 30000,             // 30 seconds (was 5 seconds)
  HEALTH_CHECK_INTERVAL: 60000,         // 1 minute (was 30 seconds)
  
  // Job processing
  JOB_BATCH_SIZE: 5,                    // Process in batches
  MIN_LOOP_DELAY: 100,                  // Minimum 100ms between loops
};

// Optimized Redis configuration for BullMQ
export const OPTIMIZED_REDIS_CONFIG = {
  enableOfflineQueue: true,
  enableReadyCheck: true,
  maxRetriesPerRequest: 3,              // Reduced from null (unlimited)
  retryStrategy: (times: number) => {
    const delay = Math.min(times * 2000, 20000);
    return delay;
  },
  keepAlive: 30000,
  connectionName: 'bullmq-optimized',
  // Memory optimizations
  dropBufferSupport: true,
  enableAutoPipelining: true,           // Batch Redis commands
  autoPipeliningIgnoredCommands: ['brpoplpush', 'blpop', 'brpop'],
};

// Optimized Queue Options
export const OPTIMIZED_QUEUE_OPTIONS: QueueOptions = {
  connection: OPTIMIZED_REDIS_CONFIG as any,
  prefix: '{bull}',
  defaultJobOptions: {
    attempts: 3,
    backoff: {
      type: 'exponential',
      delay: 5000,                      // Start with 5 seconds
    },
    removeOnComplete: {
      age: 3600,                        // Keep completed jobs for 1 hour
      count: 100,                       // Keep last 100 completed jobs
    },
    removeOnFail: {
      age: 86400,                       // Keep failed jobs for 24 hours
      count: 500,                       // Keep last 500 failed jobs
    },
  },
};

// Optimized Worker Options
export const OPTIMIZED_WORKER_OPTIONS: WorkerOptions = {
  connection: OPTIMIZED_REDIS_CONFIG as any,
  prefix: '{bull}',
  concurrency: OPTIMIZED_SYSTEM_CONFIG.CONCURRENCY_PER_WORKER,
  lockDuration: 120000,                 // 2 minutes (was 30 seconds)
  lockRenewTime: 40000,                 // Renew at 1/3 of duration
  stalledInterval: 60000,               // Check for stalled jobs every minute
  maxStalledCount: 2,                   // Allow 2 stalls before failing
  
  // Graceful shutdown
  shutdownTimeout: 30000,               // 30 seconds to finish jobs
  
  // Rate limiting per worker
  limiter: {
    max: OPTIMIZED_SYSTEM_CONFIG.CONCURRENCY_PER_WORKER,
    duration: 1000,                     // Per second
  },
};

// Job-specific configurations
export const JOB_CONFIGS: Record<string, Partial<JobsOptions>> = {
  // High priority, quick jobs
  'OPENSEA-CHECK': {
    priority: 1,
    attempts: 2,
    backoff: { type: 'fixed', delay: 2000 },
  },
  
  // Schedule jobs need unlimited retries
  'OPENSEA-SCHEDULE': {
    priority: 2,
    attempts: -1,                       // Unlimited
    backoff: { type: 'exponential', delay: 8000 },
  },
  
  // Bidding jobs
  'OPENSEA-BID': {
    priority: 3,
    attempts: 3,
    backoff: { type: 'exponential', delay: 5000 },
    timeout: 90000,                     // 90 seconds timeout
  },
  
  // Cancellation jobs
  'OPENSEA-CANCEL': {
    priority: 0,                        // Highest priority
    attempts: 2,
    backoff: { type: 'fixed', delay: 3000 },
  },
};

// Circuit breaker configuration
export const CIRCUIT_BREAKER_CONFIG = {
  failureThreshold: 5,                  // Open after 5 failures
  resetTimeout: 60000,                  // Try again after 1 minute
  monitoringPeriod: 120000,             // Monitor over 2 minutes
};

// Memory management thresholds
export const MEMORY_THRESHOLDS = {
  PAUSE_QUEUE: 85,                      // Pause queue at 85% memory
  RESUME_QUEUE: 70,                     // Resume at 70% memory
  FORCE_GC: 80,                         // Force garbage collection at 80%
  ALERT_THRESHOLD: 75,                  // Alert at 75% memory usage
};

// Monitoring intervals
export const MONITORING_INTERVALS = {
  HEALTH_CHECK: OPTIMIZED_SYSTEM_CONFIG.HEALTH_CHECK_INTERVAL,
  MEMORY_CHECK: 30000,                  // Every 30 seconds
  QUEUE_STATS: 10000,                   // Every 10 seconds
  PERFORMANCE_METRICS: 60000,           // Every minute
};

// Helper function to get job-specific options
export function getJobOptions(jobName: string, baseOptions?: Partial<JobsOptions>): JobsOptions {
  const config = JOB_CONFIGS[jobName] || {};
  return {
    ...OPTIMIZED_QUEUE_OPTIONS.defaultJobOptions,
    ...baseOptions,
    ...config,
  } as JobsOptions;
}

// Export a ready-to-use configuration
export const QUEUE_CONFIG = {
  system: OPTIMIZED_SYSTEM_CONFIG,
  redis: OPTIMIZED_REDIS_CONFIG,
  queue: OPTIMIZED_QUEUE_OPTIONS,
  worker: OPTIMIZED_WORKER_OPTIONS,
  jobs: JOB_CONFIGS,
  circuitBreaker: CIRCUIT_BREAKER_CONFIG,
  memory: MEMORY_THRESHOLDS,
  monitoring: MONITORING_INTERVALS,
  getJobOptions,
};