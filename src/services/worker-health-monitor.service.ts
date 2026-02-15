import { Worker } from 'bullmq';
import mongoose from 'mongoose';
import { errorLog, warnLog } from '../utils/debug-file-logger';

/**
 * Worker Health Monitor Service
 *
 * Monitors BullMQ worker health and connection pools.
 *
 * Responsibilities:
 * - Worker status monitoring (running, paused, healthy)
 * - Worker failure tracking with threshold detection
 * - Redis connection pool monitoring
 * - MongoDB connection pool monitoring
 * - Automatic worker restart coordination
 *
 * Features:
 * - Periodic health checks (configurable interval)
 * - Failure tracking with windowed thresholds
 * - Connection leak detection
 * - Cooldown mechanisms to prevent restart loops
 */

// Constants
const STANDARD_INTERVAL = 60000; // 60 seconds
const WORKER_HEALTH_CHECK_INTERVAL = STANDARD_INTERVAL;
const WORKER_FAILURE_THRESHOLD = 3; // Failures before restart
const WORKER_FAILURE_WINDOW = 5 * 60 * 1000; // 5 minutes
const WORKER_RESTART_COOLDOWN = 5 * 60 * 1000; // 5 minutes between restarts

// Color codes for console output
const RED = '\x1b[31m';
const RESET = '\x1b[0m';

/**
 * Dependencies required by WorkerHealthMonitorService
 */
interface WorkerHealthMonitorDependencies {
  workers: Worker[];
  queue: any;
  blurQueue: any;
  checkBullMQConnectionsHealth: () => Promise<Record<string, { healthy: boolean; error?: string }>>;
  getBullMQConnections: () => Array<{ id: string; age: number }>;
  clearQueueAndRestartJobs: () => Promise<void>;
  RATE_LIMIT: number;
}

interface WorkerFailure {
  count: number;
  lastFailure: number;
  errorTypes: string[];
}


class WorkerHealthMonitorService {
  private deps: WorkerHealthMonitorDependencies | null = null;

  // State
  private workerHealthInterval: NodeJS.Timeout | null = null;
  private workerFailures = new Map<number, WorkerFailure>();
  private lastWorkerRestartTime = 0;

  /**
   * Initialize the service with dependencies
   */
  public initialize(deps: WorkerHealthMonitorDependencies): void {
    this.deps = deps;
  }

  /**
   * Start worker health monitoring
   */
  public startMonitoring(): void {
    if (this.workerHealthInterval) {
      warnLog('WorkerHealthMonitor', 'Monitoring already started');
      return;
    }

    // Initial health check
    this.checkWorkerHealth();

    // Set up periodic health checks
    this.workerHealthInterval = setInterval(() => {
      this.checkWorkerHealth();
    }, WORKER_HEALTH_CHECK_INTERVAL);

  }

  /**
   * Stop worker health monitoring
   */
  public stopMonitoring(): void {
    if (this.workerHealthInterval) {
      clearInterval(this.workerHealthInterval);
      this.workerHealthInterval = null;
    }
  }

  /**
   * Track a worker failure and return true if threshold exceeded
   */
  public trackWorkerFailure(workerIndex: number, error: string): boolean {
    const now = Date.now();
    const failure = this.workerFailures.get(workerIndex) || { count: 0, lastFailure: 0, errorTypes: [] };

    // Reset count if outside the failure window
    if (now - failure.lastFailure > WORKER_FAILURE_WINDOW) {
      failure.count = 0;
      failure.errorTypes = [];
    }

    failure.count++;
    failure.lastFailure = now;
    failure.errorTypes.push(error);

    // Keep only last 10 error types
    if (failure.errorTypes.length > 10) {
      failure.errorTypes = failure.errorTypes.slice(-10);
    }

    this.workerFailures.set(workerIndex, failure);

    errorLog('WorkerFailure', `Worker ${workerIndex} failure tracked: ${failure.count}/${WORKER_FAILURE_THRESHOLD} in window`);
    errorLog('WorkerFailure', `Worker ${workerIndex} error`, error);

    return failure.count >= WORKER_FAILURE_THRESHOLD;
  }

  // =============================================================================
  // PRIVATE METHODS
  // =============================================================================

  /**
   * Check worker health - monitors workers, connections, and queues
   */
  private async checkWorkerHealth(): Promise<void> {
    if (!this.deps) {
      errorLog('WorkerHealthMonitor', 'Cannot check health - service not initialized');
      return;
    }

    try {
      // Check Redis connections health
      const connectionHealth = await this.deps.checkBullMQConnectionsHealth();
      const unhealthyConnections = Object.entries(connectionHealth)
        .filter(([_, status]) => !status.healthy);

      if (unhealthyConnections.length > 0) {
        errorLog('HealthCheck', `Unhealthy Redis connections detected: ${unhealthyConnections.length}`);
        unhealthyConnections.forEach(([id, status]) => {
          errorLog('HealthCheck', `Redis connection ${id}: ${status.error}`);
        });
      }

      // Check worker status
      const workerStatuses = await Promise.all(
        this.deps.workers.map(async (worker, index) => {
          try {
            // Check if worker is running
            const isRunning = worker.isRunning();
            const isPaused = worker.isPaused();

            return {
              index,
              running: isRunning,
              paused: isPaused,
              healthy: isRunning && !isPaused
            };
          } catch (error) {
            return {
              index,
              running: false,
              paused: false,
              healthy: false,
              error: error instanceof Error ? error.message : 'Unknown error'
            };
          }
        })
      );

      const unhealthyWorkers = workerStatuses.filter(status => !status.healthy);

      if (unhealthyWorkers.length > 0) {
        errorLog('HealthCheck', `Unhealthy workers detected: ${unhealthyWorkers.length}/${this.deps.workers.length}`);

        let shouldRestartAll = false;

        // Attempt to restart unhealthy workers
        for (const workerStatus of unhealthyWorkers) {
          if ('error' in workerStatus && workerStatus.error) {
            errorLog('HealthCheck', `Worker ${workerStatus.index}: ${workerStatus.error}`);

            // Track the failure
            const thresholdExceeded = this.trackWorkerFailure(workerStatus.index, workerStatus.error);
            if (thresholdExceeded) {
              shouldRestartAll = true;
            }
          } else if (workerStatus.paused) {
            try {
              await this.deps.workers[workerStatus.index].resume();
            } catch (error) {
              errorLog('HealthCheck', `Failed to resume worker ${workerStatus.index}`, error);

              // Track resume failure
              const thresholdExceeded = this.trackWorkerFailure(workerStatus.index, `Resume failed: ${error}`);
              if (thresholdExceeded) {
                shouldRestartAll = true;
              }
            }
          } else if (!workerStatus.running) {
            // Worker is not running at all
            errorLog('HealthCheck', `Worker ${workerStatus.index} is not running`);
            const thresholdExceeded = this.trackWorkerFailure(workerStatus.index, 'Worker not running');
            if (thresholdExceeded) {
              shouldRestartAll = true;
            }
          }
        }

        // Check if we should restart all workers
        if (shouldRestartAll) {
          const now = Date.now();
          if (now - this.lastWorkerRestartTime > WORKER_RESTART_COOLDOWN) {

            // Log failure details
            this.workerFailures.forEach((failure, _index) => {
              if (failure.count >= WORKER_FAILURE_THRESHOLD) {
                failure.errorTypes.slice(-3).forEach(err => console.log(RED + `  - ${err}` + RESET));
              }
            });

            this.lastWorkerRestartTime = now;

            // Clear failure tracking
            this.workerFailures.clear();

            // Clear queues and restart
            await this.deps.clearQueueAndRestartJobs();
          }
        }
      }

      // Check queue status
      const [mainQueueCounts, blurQueueCounts] = await Promise.all([
        this.deps.queue.getJobCounts(),
        this.deps.blurQueue.getJobCounts()
      ]);

      // Log health summary
      const healthSummary = {
        connections: {
          total: Object.keys(connectionHealth).length,
          healthy: Object.keys(connectionHealth).length - unhealthyConnections.length
        },
        workers: {
          total: this.deps.workers.length,
          healthy: this.deps.workers.length - unhealthyWorkers.length
        },
        queues: {
          main: {
            waiting: mainQueueCounts.waiting || 0,
            active: mainQueueCounts.active || 0,
            failed: mainQueueCounts.failed || 0
          },
          blur: {
            waiting: blurQueueCounts.waiting || 0,
            active: blurQueueCounts.active || 0,
            failed: blurQueueCounts.failed || 0
          }
        }
      };

      // Connection pool monitoring
      const connectionPoolStats = this.deps.getBullMQConnections();
      const connectionAges = connectionPoolStats.map(conn => ({
        id: conn.id,
        age: Math.floor(conn.age / 1000), // Convert to seconds
        healthy: connectionHealth[conn.id]?.healthy || false
      }));

      // Log connection pool information if there are old connections or in development
      const _oldConnections = connectionAges.filter(conn => conn.age > 3600); // Connections older than 1 hour

      // Log detailed connection pool stats to monitoring system

      // Enhanced Redis connection pool monitoring
      const maxConnections = connectionPoolStats.length;
      const connectionPoolWarningThreshold = 40; // Warn when 80% of assumed max 50 connections
      const connectionPoolCriticalThreshold = 45; // Critical when 90% of max connections

      if (maxConnections >= connectionPoolCriticalThreshold) {
        errorLog('ConnectionPool', `Redis connection pool at critical level: ${maxConnections} connections`);
        errorLog('Database', `Redis connection pool critical: ${maxConnections}/${connectionPoolCriticalThreshold}`, new Error('Connection pool exhausted'));
      } else if (maxConnections >= connectionPoolWarningThreshold) {
        warnLog('Database', `Redis connection pool warning: ${maxConnections}/${connectionPoolWarningThreshold}`);
      }

      // Check for connection leaks (very old connections)
      const connectionLeakThreshold = 2 * 60 * 60; // 2 hours in seconds
      const expectedLongLivedConnections = 8; // BullMQ expected connections
      const possibleLeaks = connectionAges.filter(conn => conn.age > connectionLeakThreshold);

      if (possibleLeaks.length > expectedLongLivedConnections) {
        warnLog('Database', `Possible Redis connection leaks: ${possibleLeaks.length} connections older than 2 hours (expected ~${expectedLongLivedConnections} for BullMQ)`);
      }

      // MongoDB connection pool monitoring
      try {
        const mongoClient = mongoose.connection.getClient();
        // @ts-ignore - Access internal connection pool stats
        const poolStats = mongoClient?.topology?.s?.pool;

        if (poolStats) {
          const maxPoolSize = Number(process.env.MONGO_MAX_POOL_SIZE) || 100;
          const minPoolSize = Number(process.env.MONGO_MIN_POOL_SIZE) || 30;

          // Calculate current pool metrics
          const totalConnections = poolStats.totalConnectionCount || 0;
          const availableConnections = poolStats.availableConnectionCount || 0;
          const activeConnections = totalConnections - availableConnections;
          const poolUtilization = totalConnections > 0 ? (activeConnections / maxPoolSize) * 100 : 0;

          // Log MongoDB pool statistics

          // Warning threshold: 80% of maxPoolSize
          const warningThreshold = maxPoolSize * 0.8;
          // Critical threshold: 90% of maxPoolSize
          const criticalThreshold = maxPoolSize * 0.9;

          if (activeConnections >= criticalThreshold) {
            errorLog('MongoDBPool', `MongoDB connection pool at critical level: ${activeConnections}/${maxPoolSize} connections in use (${Math.round(poolUtilization)}%)`);
            errorLog('Database', `MongoDB connection pool critical: ${activeConnections}/${maxPoolSize} (${Math.round(poolUtilization)}%)`, new Error('Connection pool near exhaustion'));
          } else if (activeConnections >= warningThreshold) {
            warnLog('Database', `MongoDB connection pool warning: ${activeConnections}/${maxPoolSize} (${Math.round(poolUtilization)}%)`);
          }

          // Check if pool is below minimum (possible connection issues)
          if (totalConnections < minPoolSize && mongoose.connection.readyState === 1) {
            warnLog('Database', `MongoDB pool below minimum: ${totalConnections}/${minPoolSize}`);
          }
        }
      } catch (mongoMonitorError) {
        // Don't let MongoDB monitoring errors crash the health check
        errorLog('MongoDBPoolMonitoring', 'Error monitoring MongoDB pool', mongoMonitorError);
      }

    } catch (error) {
      errorLog('WorkerHealthCheck', 'Error during worker health check', error);
    }
  }


  // =============================================================================
  // DIAGNOSTICS & STATS
  // =============================================================================

  /**
   * Get service statistics
   */
  public getStats() {
    return {
      isMonitoring: this.workerHealthInterval !== null,
      workerFailures: this.workerFailures.size,
      lastWorkerRestartTime: this.lastWorkerRestartTime
    };
  }
}

export const workerHealthMonitorService = new WorkerHealthMonitorService();
