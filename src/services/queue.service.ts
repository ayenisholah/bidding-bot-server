import { Queue, Worker, Job, QueueEvents, QueueOptions } from 'bullmq';
import {
  createBullMQConnection,
  checkBullMQConnectionsHealth,
  closeAllBullMQConnections
} from '../utils/redis';
import { errorLog, warnLog, debugLog } from '../utils/debug-file-logger';
import { orderCache } from '../utils/order-cache';
import { cancellationTracker } from '../utils/cancellation-tracker';

/**
 * QueueService - Manages BullMQ queues and workers
 *
 * Responsibilities:
 * - BullMQ queue and QueueEvents initialization
 * - Redis connection management for queues and workers
 * - Worker pool creation (main workers + Blur worker)
 * - Worker event handlers (error, failed, completed)
 * - Queue health check integration
 * - Shutdown/cleanup logic
 */
class QueueService {
  // Queue instances
  private mainQueue: Queue | null = null;
  private blurQueue: Queue | null = null;
  private counterbidQueue: Queue | null = null;

  // Queue event listeners
  private queueEvents: QueueEvents | null = null;
  private blurQueueEvents: QueueEvents | null = null;
  private counterbidQueueEvents: QueueEvents | null = null;

  // Workers
  private workers: Worker[] = [];

  // Connections tracking
  private bullMQConnections: any[] = [];

  // Dependencies (injected via initialize)
  private processJob: ((job: Job, signal?: AbortSignal) => Promise<any>) | null = null;
  private processBlurJob: ((job: Job, signal?: AbortSignal) => Promise<any>) | null = null;
  private trackWorkerFailure: ((workerIndex: number, error: string) => void) | null = null;
  private getTaskIdForLog: ((job: Job | undefined) => string) | null = null;
  private markJobAsProcessed: ((taskId: string, marketplace: string, jobId: string, success?: boolean, loopNumber?: number) => Promise<void>) | null = null;
  private updateTaskCancellationPercentage: ((taskId: string, taskData?: any, marketplace?: string) => Promise<void>) | null = null;
  private safeDeleteFromCache: ((orderKey: string, taskId: string, marketplace: string) => Promise<boolean>) | null = null;
  private errorLogCache: any = null;
  private magicedenOrdersBeingCancelled: Set<string> | null = null;
  private startWorkerHealthMonitoring: (() => void) | null = null;

  // Job type constants
  private CANCEL_OPENSEA_BID = "CANCEL_OPENSEA_BID";
  private CANCEL_OPENSEA_COUNTERBID = "CANCEL_OPENSEA_COUNTERBID";
  private CANCEL_OPENSEA_OUTBID = "CANCEL_OPENSEA_OUTBID";
  private CANCEL_MAGICEDEN_BID = "CANCEL_MAGICEDEN_BID";
  private CANCEL_BLUR_BID = "CANCEL_BLUR_BID";

  /**
   * Initialize the queue service with dependencies
   */
  public async initialize(config: {
    queueName: string;
    blurQueueName: string;
    counterbidQueueName: string;
    workerCount: number;
    rateLimit: number;
    processJob: (job: Job, signal?: AbortSignal) => Promise<any>;
    processBlurJob: (job: Job, signal?: AbortSignal) => Promise<any>;
    trackWorkerFailure: (workerIndex: number, error: string) => void;
    getTaskIdForLog: (job: Job | undefined) => string;
    markJobAsProcessed: (taskId: string, marketplace: string, jobId: string, success?: boolean, loopNumber?: number) => Promise<void>;
    updateTaskCancellationPercentage: (taskId: string, taskData?: any, marketplace?: string) => Promise<void>;
    safeDeleteFromCache: (orderKey: string, taskId: string, marketplace: string) => Promise<boolean>;
    errorLogCache: any;
    magicedenOrdersBeingCancelled: Set<string>;
    startWorkerHealthMonitoring: () => void;
  }): Promise<void> {
    // Store dependencies
    this.processJob = config.processJob;
    this.processBlurJob = config.processBlurJob;
    this.trackWorkerFailure = config.trackWorkerFailure;
    this.getTaskIdForLog = config.getTaskIdForLog;
    this.markJobAsProcessed = config.markJobAsProcessed;
    this.updateTaskCancellationPercentage = config.updateTaskCancellationPercentage;
    this.safeDeleteFromCache = config.safeDeleteFromCache;
    this.errorLogCache = config.errorLogCache;
    this.magicedenOrdersBeingCancelled = config.magicedenOrdersBeingCancelled;
    this.startWorkerHealthMonitoring = config.startWorkerHealthMonitoring;

    // Create BullMQ connections
    await this.createConnections(config.queueName, config.blurQueueName, config.counterbidQueueName);

    // Initialize queues
    await this.initializeQueues(config.queueName, config.blurQueueName, config.counterbidQueueName);

    // Create workers
    await this.createWorkers(config.workerCount, config.rateLimit);

    // Start worker health monitoring
    if (this.startWorkerHealthMonitoring) {
      this.startWorkerHealthMonitoring();
    }

    console.log('[STARTUP] Queue service initialized');
  }

  /**
   * Create separate BullMQ connections for each component
   */
  private async createConnections(_queueName: string, _blurQueueName: string, _counterbidQueueName: string): Promise<void> {
    try {
      // Main queue connection
      const mainQueueConnection = await createBullMQConnection('main-queue');
      this.bullMQConnections.push(mainQueueConnection);

      // Main queue events connection
      const mainQueueEventsConnection = await createBullMQConnection('main-queue-events');
      this.bullMQConnections.push(mainQueueEventsConnection);

      // Blur queue connection
      const blurQueueConnection = await createBullMQConnection('blur-queue');
      this.bullMQConnections.push(blurQueueConnection);

      // Blur queue events connection
      const blurQueueEventsConnection = await createBullMQConnection('blur-queue-events');
      this.bullMQConnections.push(blurQueueEventsConnection);

      // Counterbid queue connection
      const counterbidQueueConnection = await createBullMQConnection('counterbid-queue');
      this.bullMQConnections.push(counterbidQueueConnection);

      // Counterbid queue events connection
      const counterbidQueueEventsConnection = await createBullMQConnection('counterbid-queue-events');
      this.bullMQConnections.push(counterbidQueueEventsConnection);

      // Verify all connections are healthy before proceeding
      const healthCheck = await checkBullMQConnectionsHealth();
      const unhealthyConnections = Object.entries(healthCheck).filter(([_, status]) => !status.healthy);

      if (unhealthyConnections.length > 0) {
        errorLog('BullMQInit', 'Unhealthy Redis connections detected');
        unhealthyConnections.forEach(([id, status]) => {
          errorLog('HealthCheck', `Redis connection ${id}: ${status.error}`);
        });
        throw new Error('One or more Redis connections are unhealthy');
      }

    } catch (error) {
      errorLog('BullMQInit', 'Failed to create BullMQ connections', error);
      // Close any connections that were created
      await closeAllBullMQConnections();
      throw new Error('Failed to initialize BullMQ connections');
    }
  }

  /**
   * Initialize queue and queue events with their own connections
   */
  private async initializeQueues(queueName: string, blurQueueName: string, counterbidQueueName: string): Promise<void> {
    const mainQueueConnection = this.bullMQConnections[0];
    const mainQueueEventsConnection = this.bullMQConnections[1];
    const blurQueueConnection = this.bullMQConnections[2];
    const blurQueueEventsConnection = this.bullMQConnections[3];
    const counterbidQueueConnection = this.bullMQConnections[4];
    const counterbidQueueEventsConnection = this.bullMQConnections[5];

    // Initialize queue options with main queue connection
    const QUEUE_OPTIONS: QueueOptions = {
      connection: mainQueueConnection,
      defaultJobOptions: {
        removeOnComplete: true,
        removeOnFail: true,
        attempts: 1,  // Prevent BullMQ's default 3 retries to avoid duplicate job processing
      }
    };

    // Initialize main queue and events
    this.mainQueue = new Queue(queueName, QUEUE_OPTIONS);
    this.queueEvents = new QueueEvents(queueName, { connection: mainQueueEventsConnection });
    this.blurQueueEvents = new QueueEvents(blurQueueName, { connection: blurQueueEventsConnection });
    this.counterbidQueueEvents = new QueueEvents(counterbidQueueName, { connection: counterbidQueueEventsConnection });

    // Initialize Blur queue with its own connection
    this.blurQueue = new Queue(blurQueueName, {
      connection: blurQueueConnection,
      defaultJobOptions: {
        removeOnComplete: true,
        removeOnFail: true,
        attempts: 1,
      }
    });

    // Initialize Counterbid queue with its own connection
    this.counterbidQueue = new Queue(counterbidQueueName, {
      connection: counterbidQueueConnection,
      defaultJobOptions: {
        removeOnComplete: true,
        removeOnFail: true,
        attempts: 1,
      }
    });

    // Set up Blur queue error handlers
    this.blurQueue.on('error', (error) => {
      errorLog('BlurQueue', 'Queue error', error);
      errorLog('Queue', 'Blur queue error', error);
    });

    this.blurQueueEvents.on('error', (error) => {
      errorLog('BlurQueueEvents', 'Events error', error);
      errorLog('Queue', 'Blur queue events error', error);
    });

    // Set up Counterbid queue error handlers
    this.counterbidQueue.on('error', (error) => {
      errorLog('CounterbidQueue', 'Queue error', error);
      errorLog('Queue', 'Counterbid queue error', error);
    });

    this.counterbidQueueEvents.on('error', (error) => {
      errorLog('CounterbidQueueEvents', 'Events error', error);
      errorLog('Queue', 'Counterbid queue events error', error);
    });

    console.log('[STARTUP] BullMQ queues initialized');
  }

  /**
   * Create worker pool (main workers + Blur worker)
   */
  private async createWorkers(workerCount: number, rateLimit: number): Promise<void> {
    if (!this.processJob || !this.processBlurJob) {
      throw new Error('processJob and processBlurJob must be provided');
    }

    const concurrencyPerWorker = Number(rateLimit) / workerCount;

    // Create main workers
    for (let i = 0; i < workerCount; i++) {
      await this.createMainWorker(i, concurrencyPerWorker);
    }

    // Create Blur worker
    await this.createBlurWorker(concurrencyPerWorker);

    debugLog('STARTUP', `Workers initialized | count=${this.workers.length} | concurrency=${concurrencyPerWorker}`);
  }

  /**
   * Create a single main worker
   */
  private async createMainWorker(index: number, concurrencyPerWorker: number): Promise<void> {
    if (!this.mainQueue || !this.processJob) {
      throw new Error('Main queue and processJob must be initialized');
    }

    // Create a separate connection for each worker
    const workerConnection = await createBullMQConnection(`main-worker-${index + 1}`);
    this.bullMQConnections.push(workerConnection);

    const worker = new Worker(
      this.mainQueue.name,
      async (job) => {
        // TIMEOUT CONFIGURATION (Optimized for aggressive fast failure)
        // Cascade: Axios (30s) → Lock (45s) → Stall (60s) → Job (60s)
        // - Lock expires BEFORE stall detection (prevents race conditions)
        // - Ultra-fast failure (45s lock, 60s job) maximizes retry opportunities
        // - Allows 1-2 sequential API calls per job (competitive bidding priority)
        const jobTimeout = 60000; // 1 minute (aggressive timeout for fast retries)
        const abortController = new AbortController();

        const timeoutPromise = new Promise((_, reject) => {
          setTimeout(() => {
            abortController.abort(); // Signal cancellation to any async operations
            const customJobId = (job.opts as any)?.jobId || job.id;
            reject(new Error(`Job ${customJobId} (ID: ${job.id}) timed out after ${jobTimeout}ms - aborted`));
          }, jobTimeout);
        });

        try {
          const result = await Promise.race([
            this.processJob!(job, abortController.signal),
            timeoutPromise
          ]);
          return result || { success: true };
        } catch (error: any) {
          // If it's a password error, throw it to fail the job properly
          if (error.code === 'NO_PASSWORD' || error.message?.includes('No password available')) {
            throw error;
          }
          // For other errors, also throw to ensure proper retry
          throw error;
        }
      },
      {
        connection: workerConnection,
        concurrency: concurrencyPerWorker,
        lockDuration: 45000,       // 45 seconds - allows 1-2 sequential 30s API calls
        stalledInterval: 15000,    // 15 seconds - high-frequency stall detection
        maxStalledCount: 4         // 4 checks = 60s (triggers AFTER lock expires at 45s)
      }
    );

    // Add error handlers for main workers
    worker.on('error', async (error) => {
      errorLog('Worker', `Worker ${index + 1} error`, error);
      if (this.trackWorkerFailure) {
        this.trackWorkerFailure(index, `Worker error: ${error.message || String(error)}`);
      }
      errorLog('Worker', `Main worker ${index + 1} error`, error);
    });

    worker.on('failed', async (job, error) => {
      if (!this.getTaskIdForLog || !this.markJobAsProcessed) return;

      const taskId = this.getTaskIdForLog(job);
      // Deduplicate worker failure logs to prevent log flooding
      const workerErrorKey = `worker-failed-${taskId}-${job?.id}`;
      if (this.errorLogCache && this.errorLogCache.addIfNotExists(workerErrorKey, Date.now())) {
        errorLog('Worker', `Worker ${index + 1} - Task ${taskId} - Job ${job?.id} failed`, error);
      }

      // Handle failed bid jobs - immediately mark as processed
      if (job && job.id && typeof job.id === 'string' && job.id.startsWith('bid-')) {
        // Extract task and marketplace from job ID (format: bid-taskId-marketplace-...)
        const parts = job.id.split('-');
        if (parts.length >= 3) {
          const taskId = parts[1];
          const marketplace = parts[2].toLowerCase();

          // Extract loopNumber from job data if available for precise parent lookup
          const jobLoopNumber = job.data?.loopNumber as number | undefined;

          // Mark job as processed to allow loop completion
          this.markJobAsProcessed(taskId, marketplace, job.id, false, jobLoopNumber).catch(err => {
            errorLog('Worker', `Worker ${index + 1} failed to mark failed bid job as processed`, err);
          });
        }
      }

      // Handle failed cancellation jobs - track them as completed
      if (job && job.name && (job.name === this.CANCEL_OPENSEA_BID || job.name === this.CANCEL_MAGICEDEN_BID || job.name === this.CANCEL_BLUR_BID)) {
        const { taskId } = job.data;
        const marketplace = job.name === this.CANCEL_OPENSEA_BID ? 'opensea' :
          job.name === this.CANCEL_MAGICEDEN_BID ? 'magiceden' : 'blur';

        // Record the failed cancellation as a completion to ensure we reach 100%
        cancellationTracker.recordCompletion(taskId, 1, marketplace);
        if (this.updateTaskCancellationPercentage) {
          this.updateTaskCancellationPercentage(taskId, undefined, marketplace).catch(err => {
            const failedTaskId = this.getTaskIdForLog!(job);
            errorLog('CancellationTracking', `Failed to update cancellation percentage for task ${failedTaskId} after job failure`, err);
          });
        }

        // Delete the order from Redis cache to clean up
        if (this.safeDeleteFromCache) {
          if (job.data.orderKey) {
            await this.safeDeleteFromCache(job.data.orderKey, taskId, marketplace);
          } else if (job.data.orderKeys) {
            for (const orderKey of job.data.orderKeys) {
              await this.safeDeleteFromCache(orderKey, taskId, marketplace);
            }

            // Clean up MagicEden tracking set on failure
            if (job.name === this.CANCEL_MAGICEDEN_BID && job.data.orderKeys && this.magicedenOrdersBeingCancelled) {
              job.data.orderKeys.forEach((orderKey: string) => {
                this.magicedenOrdersBeingCancelled!.delete(orderKey);
              });
            }
          }
        }
      }

      // Only track as worker failure if it's a systemic issue
      if (error.message?.includes('could not renew lock') ||
        error.message?.includes('Missing lock') ||
        error.message?.includes('connection lost')) {
        if (this.trackWorkerFailure) {
          this.trackWorkerFailure(index, `Job processing error: ${error.message}`);
        }
      }
    });

    // Add completion hook for guaranteed cleanup verification
    worker.on('completed', async (job) => {
      // Verify cache was cleaned (defensive check)
      if (job && (job.name === this.CANCEL_OPENSEA_BID ||
                  job.name === this.CANCEL_OPENSEA_COUNTERBID ||
                  job.name === this.CANCEL_OPENSEA_OUTBID ||
                  job.name === this.CANCEL_MAGICEDEN_BID ||
                  job.name === this.CANCEL_BLUR_BID)) {
        const taskId = job.data.taskId;
        const marketplace = job.name === this.CANCEL_OPENSEA_BID ||
                           job.name === this.CANCEL_OPENSEA_COUNTERBID ||
                           job.name === this.CANCEL_OPENSEA_OUTBID ? 'opensea' :
                           job.name === this.CANCEL_BLUR_BID ? 'blur' : 'magiceden';

        // For single-order jobs (OpenSea, Blur)
        if (job.data.orderKey) {
          const orderKey = job.data.orderKey;
          const identifier = job.data.identifier;

          if (identifier) {
            const stillExists = await orderCache.get(taskId, marketplace, identifier);
            if (stillExists && this.safeDeleteFromCache) {
              warnLog('WorkerCleanup', `ORDER_STILL_IN_CACHE_AFTER_COMPLETION | taskId=${taskId} | orderKey=${orderKey} | marketplace=${marketplace}`, { taskId });
              await this.safeDeleteFromCache(orderKey, taskId, marketplace);
            }
          }
        }

        // For batch jobs (MagicEden)
        if (job.data.orderKeys && job.data.identifiers) {
          const orderKeys = job.data.orderKeys;
          const identifiers = job.data.identifiers;

          for (let i = 0; i < orderKeys.length; i++) {
            const orderKey = orderKeys[i];
            const identifier = identifiers[i];

            if (identifier) {
              const stillExists = await orderCache.get(taskId, marketplace, identifier);
              if (stillExists && this.safeDeleteFromCache) {
                warnLog('WorkerCleanup', `ORDER_STILL_IN_CACHE_AFTER_COMPLETION | taskId=${taskId} | orderKey=${orderKey} | marketplace=${marketplace}`, { taskId });
                await this.safeDeleteFromCache(orderKey, taskId, marketplace);
              }
            }
          }
        }
      }
    });

    // Add failure hook for guaranteed cleanup (even if error handler failed to delete)
    worker.on('failed', async (job, error) => {
      if (job && (job.name === this.CANCEL_OPENSEA_BID ||
                  job.name === this.CANCEL_OPENSEA_COUNTERBID ||
                  job.name === this.CANCEL_OPENSEA_OUTBID ||
                  job.name === this.CANCEL_MAGICEDEN_BID ||
                  job.name === this.CANCEL_BLUR_BID) && this.safeDeleteFromCache) {
        const taskId = job.data.taskId;
        const marketplace = job.name === this.CANCEL_OPENSEA_BID ||
                           job.name === this.CANCEL_OPENSEA_COUNTERBID ||
                           job.name === this.CANCEL_OPENSEA_OUTBID ? 'opensea' :
                           job.name === this.CANCEL_BLUR_BID ? 'blur' : 'magiceden';

        // GUARANTEED CLEANUP: Even if error handler failed to delete
        if (job.data.orderKey) {
          const orderKey = job.data.orderKey;
          warnLog('WorkerCleanup', `GUARANTEED_CLEANUP_ON_FAILURE | taskId=${taskId} | marketplace=${marketplace} | orderKey=${orderKey} | error=${error.message}`, { taskId });
          await this.safeDeleteFromCache(orderKey, taskId, marketplace);
        } else if (job.data.orderKeys) {
          for (const orderKey of job.data.orderKeys) {
            warnLog('WorkerCleanup', `GUARANTEED_CLEANUP_ON_FAILURE | taskId=${taskId} | marketplace=${marketplace} | orderKey=${orderKey} | error=${error.message}`, { taskId });
            await this.safeDeleteFromCache(orderKey, taskId, marketplace);
          }
        }
      }
    });

    // Add worker to array
    this.workers.push(worker);
  }

  /**
   * Create Blur worker with its own connection and rate limiting
   */
  private async createBlurWorker(concurrencyPerWorker: number): Promise<void> {
    if (!this.blurQueue || !this.processBlurJob) {
      throw new Error('Blur queue and processBlurJob must be initialized');
    }

    const blurWorkerConnection = await createBullMQConnection('blur-worker');
    this.bullMQConnections.push(blurWorkerConnection);

    const blurWorker = new Worker(
      this.blurQueue.name,
      async (job) => {
        // TIMEOUT CONFIGURATION (Optimized for aggressive fast failure)
        // Cascade: Axios (30s) → Lock (45s) → Stall (60s) → Job (60s)
        // - Lock expires BEFORE stall detection (prevents race conditions)
        // - Ultra-fast failure (45s lock, 60s job) maximizes retry opportunities
        // - Allows 1-2 sequential API calls per job (competitive bidding priority)
        const jobTimeout = 60000; // 1 minute (aggressive timeout for fast retries)
        const abortController = new AbortController();

        const timeoutPromise = new Promise((_, reject) => {
          setTimeout(() => {
            abortController.abort(); // Signal cancellation to any async operations
            const customJobId = (job.opts as any)?.jobId || job.id;
            reject(new Error(`Blur job ${customJobId} (ID: ${job.id}) timed out after ${jobTimeout}ms - aborted`));
          }, jobTimeout);
        });

        try {
          const result = await Promise.race([
            this.processBlurJob!(job, abortController.signal),
            timeoutPromise
          ]);

          return result || { success: true };
        } catch (error: any) {
          const blurJobTaskId = this.getTaskIdForLog ? this.getTaskIdForLog(job) : 'unknown';
          errorLog('BlurWorker', `Task ${blurJobTaskId} - Job ${job.id} failed`, error);
          // If it's a password error, throw it to fail the job properly
          if (error.code === 'NO_PASSWORD' || error.message?.includes('No password available')) {
            throw error;
          }
          // For other errors, also throw to ensure proper retry
          throw error;
        }
      },
      {
        connection: blurWorkerConnection,
        concurrency: concurrencyPerWorker, // Reduce concurrency for Blur to prevent rate limit issues
        lockDuration: 45000,       // 45 seconds - allows 1-2 sequential 30s API calls
        stalledInterval: 15000,    // 15 seconds - high-frequency stall detection
        maxStalledCount: 4         // 4 checks = 60s (triggers AFTER lock expires at 45s)
      }
    );

    // Add error handlers for Blur queue components
    blurWorker.on('error', async (error) => {
      errorLog('BlurWorker', 'Worker error', error);
      const blurWorkerIndex = this.workers.length; // Blur worker is the last one (not yet added)
      if (this.trackWorkerFailure) {
        this.trackWorkerFailure(blurWorkerIndex, `Blur worker error: ${error.message || String(error)}`);
      }
      errorLog('Queue', 'Blur worker error', error instanceof Error ? error : new Error(String(error)));
    });

    blurWorker.on('failed', async (job, error) => {
      if (!this.getTaskIdForLog || !this.markJobAsProcessed) return;

      const blurFailedTaskId = this.getTaskIdForLog(job);
      // Deduplicate Blur worker failure logs to prevent log flooding
      const blurErrorKey = `blur-worker-failed-${blurFailedTaskId}-${job?.id}`;
      if (this.errorLogCache && this.errorLogCache.addIfNotExists(blurErrorKey, Date.now())) {
        errorLog('BlurWorker', `Task ${blurFailedTaskId} - Job ${job?.id} failed`, error);
      }

      // Handle failed bid jobs - immediately mark as processed
      if (job && job.id && typeof job.id === 'string' && job.id.startsWith('bid-')) {
        // Extract task and marketplace from job ID (format: bid-taskId-marketplace-...)
        const parts = job.id.split('-');
        if (parts.length >= 3) {
          const taskId = parts[1];
          const marketplace = parts[2].toLowerCase();

          // Extract loopNumber from job data if available for precise parent lookup
          const jobLoopNumber = job.data?.loopNumber as number | undefined;

          // Mark job as processed to allow loop completion
          this.markJobAsProcessed(taskId, marketplace, job.id, false, jobLoopNumber).catch(err => {
            errorLog('BlurWorker', 'Failed to mark failed bid job as processed', err);
          });
        }
      }

      // Handle failed Blur cancellation jobs - track them as completed
      if (job && job.name === this.CANCEL_BLUR_BID) {
        const { taskId } = job.data;

        // Record the failed cancellation as a completion to ensure we reach 100%
        cancellationTracker.recordCompletion(taskId, 1, 'blur');
        if (this.updateTaskCancellationPercentage) {
          this.updateTaskCancellationPercentage(taskId, undefined, 'blur').catch((err) => {
            const blurCancelTaskId = this.getTaskIdForLog!(job);
            errorLog('CancellationTracking', `Failed to update cancellation percentage for task ${blurCancelTaskId} after Blur job failure`, err);
          });
        }

        // Delete the order from Redis cache to clean up
        if (this.safeDeleteFromCache) {
          if (job.data.orderKey) {
            await this.safeDeleteFromCache(job.data.orderKey, taskId, 'blur');
          } else if (job.data.orderKeys) {
            for (const orderKey of job.data.orderKeys) {
              await this.safeDeleteFromCache(orderKey, taskId, 'blur');
            }
          }
        }
      }

      // Track systemic failures
      if (error.message?.includes('could not renew lock') ||
        error.message?.includes('Missing lock') ||
        error.message?.includes('connection lost')) {
        const blurWorkerIndex = this.workers.length - 1;
        if (this.trackWorkerFailure) {
          this.trackWorkerFailure(blurWorkerIndex, `Blur job processing error: ${error.message}`);
        }
      }
    });

    // Add completion hook for Blur worker
    blurWorker.on('completed', async (job) => {
      // Verify cache was cleaned (defensive check)
      if (job && job.name === this.CANCEL_BLUR_BID && job.data.orderKey) {
        const taskId = job.data.taskId;
        const orderKey = job.data.orderKey;

        // Check if order still exists in cache (shouldn't be there)
        const identifier = job.data.identifier;
        if (identifier) {
          const stillExists = await orderCache.get(taskId, 'blur', identifier);
          if (stillExists && this.safeDeleteFromCache) {
            warnLog('WorkerCleanup', `ORDER_STILL_IN_CACHE_AFTER_COMPLETION | taskId=${taskId} | orderKey=${orderKey}`, { taskId });
            await this.safeDeleteFromCache(orderKey, taskId, 'blur');
          }
        }
      }
    });

    this.workers.push(blurWorker);
  }

  /**
   * Get the main queue
   */
  public getQueue(): Queue {
    if (!this.mainQueue) {
      throw new Error('Main queue not initialized');
    }
    return this.mainQueue;
  }

  /**
   * Get the Blur queue
   */
  public getBlurQueue(): Queue {
    if (!this.blurQueue) {
      throw new Error('Blur queue not initialized');
    }
    return this.blurQueue;
  }

  /**
   * Get queue events
   */
  public getQueueEvents(): QueueEvents {
    if (!this.queueEvents) {
      throw new Error('Queue events not initialized');
    }
    return this.queueEvents;
  }

  /**
   * Get Blur queue events
   */
  public getBlurQueueEvents(): QueueEvents {
    if (!this.blurQueueEvents) {
      throw new Error('Blur queue events not initialized');
    }
    return this.blurQueueEvents;
  }

  /**
   * Get the Counterbid queue
   */
  public getCounterbidQueue(): Queue {
    if (!this.counterbidQueue) {
      throw new Error('Counterbid queue not initialized');
    }
    return this.counterbidQueue;
  }

  /**
   * Get Counterbid queue events
   */
  public getCounterbidQueueEvents(): QueueEvents {
    if (!this.counterbidQueueEvents) {
      throw new Error('Counterbid queue events not initialized');
    }
    return this.counterbidQueueEvents;
  }

  /**
   * Get all workers
   */
  public getWorkers(): Worker[] {
    return this.workers;
  }

  /**
   * Get all BullMQ connections
   */
  public getConnections(): any[] {
    return this.bullMQConnections;
  }

  /**
   * Shutdown queues and workers
   */
  public async shutdown(): Promise<void> {
    // Close workers
    for (const worker of this.workers) {
      await worker.close();
    }

    // Close queue events
    if (this.queueEvents) {
      await this.queueEvents.close();
    }
    if (this.blurQueueEvents) {
      await this.blurQueueEvents.close();
    }
    if (this.counterbidQueueEvents) {
      await this.counterbidQueueEvents.close();
    }

    // Close queues
    if (this.mainQueue) {
      await this.mainQueue.close();
    }
    if (this.blurQueue) {
      await this.blurQueue.close();
    }
    if (this.counterbidQueue) {
      await this.counterbidQueue.close();
    }

    // Close all BullMQ connections
    await closeAllBullMQConnections();

    console.log('[SHUTDOWN] Queue service shutdown complete');
  }
}

// Singleton export
export const queueService = new QueueService();
