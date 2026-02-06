import Redis from 'ioredis';
import { Queue } from 'bullmq';
import { errorLog, debugLog, warnLog, infoLog } from '../utils/debug-file-logger';
import { tradingDisabledCache } from '../utils/trading-disabled-cache';
import { GREEN, YELLOW, RED } from '../index';

// Constants
const REDIS_FAILURE_THRESHOLD = 2; // Consecutive failures before recovery
const REDIS_RECOVERY_COOLDOWN = 60000; // 1 minute cooldown between recovery attempts
const STANDARD_INTERVAL = 60000; // 60 seconds
const HEALTH_CHECK_INTERVAL = process.env.NODE_ENV === 'development' ? 5000 : 60000;
const PASSWORD_CHECK_INTERVAL = STANDARD_INTERVAL;

interface HealthConfig {
  clearQueueAndRestartJobs: () => Promise<void>;
  resumeQueueAndWorkers: () => Promise<void>;
  recoverPasswordFromRedis: () => Promise<boolean>;
  getPassword: () => string | undefined;
}

class HealthMonitorService {
  private redis: Redis | null = null;
  private queue: Queue | null = null;
  private config: HealthConfig | null = null;

  // State
  private redisFailures = 0;
  private lastRedisRecovery = 0;
  private isCheckingPassword = false;

  // Intervals
  private monitorInterval: NodeJS.Timeout | null = null;
  private passwordCheckInterval: NodeJS.Timeout | null = null;

  /**
   * Initialize the health monitor service with dependencies
   */
  public initialize(redis: Redis, queue: Queue, config: HealthConfig): void {
    this.redis = redis;
    this.queue = queue;
    this.config = config;
    infoLog('HealthMonitorService', 'Initialized with Redis and Queue');
  }

  /**
   * Start health monitoring intervals
   */
  public startMonitoring(): void {
    if (this.monitorInterval || this.passwordCheckInterval) {
      warnLog('HealthMonitorService', 'Monitoring already started');
      return;
    }

    // Start health check monitoring
    this.monitorInterval = setInterval(() => {
      this.monitorHealth().catch(err => {
        debugLog('Health', 'Health monitoring interval failed', err);
      });
    }, HEALTH_CHECK_INTERVAL);

    // Start password check monitoring
    this.passwordCheckInterval = setInterval(() => {
      this.checkPasswordAndResume().catch(err => {
        errorLog('PasswordCheck', 'Password check interval failed', err);
      });
    }, PASSWORD_CHECK_INTERVAL);

    infoLog('HealthMonitorService', 'Health monitoring started');
  }

  /**
   * Stop health monitoring intervals
   */
  public stopMonitoring(): void {
    if (this.monitorInterval) {
      clearInterval(this.monitorInterval);
      this.monitorInterval = null;
    }

    if (this.passwordCheckInterval) {
      clearInterval(this.passwordCheckInterval);
      this.passwordCheckInterval = null;
    }

    infoLog('HealthMonitorService', 'Health monitoring stopped');
  }

  /**
   * Check Redis health with ping
   *
   * DEPRECATED: Replaced by BullMQ connection health checks.
   * This method now always returns true to avoid breaking existing code.
   * Use checkBullMQConnectionsHealth() from redis.ts for actual health monitoring.
   */
  public async checkRedisHealth(): Promise<boolean> {
    warnLog('HealthMonitor', 'checkRedisHealth() is deprecated - use BullMQ connection health checks');
    return true; // Always return true to avoid breaking existing code
  }

  /**
   * Handle Redis failure by attempting recovery
   */
  private async handleRedisFailure(): Promise<void> {
    if (!this.config) {
      errorLog('HealthMonitorService', 'Cannot handle Redis failure - config not initialized');
      return;
    }

    const now = Date.now();

    // Check cooldown
    if (now - this.lastRedisRecovery < REDIS_RECOVERY_COOLDOWN) {
      const _cooldownRemaining = Math.ceil((REDIS_RECOVERY_COOLDOWN - (now - this.lastRedisRecovery)) / 1000);
      return;
    }

    try {
      this.lastRedisRecovery = now;
      await this.config.clearQueueAndRestartJobs();

      // Reset failure counter after successful recovery
      this.redisFailures = 0;
    } catch (error) {
      errorLog('RedisRecovery', 'Failed to recover from Redis failure', error);
      // Don't reset failure counter if recovery fails
    }
  }

  /**
   * Main health monitoring function
   */
  private async monitorHealth(): Promise<void> {
    try {
      const isRedisHealthy = await this.checkRedisHealth();
      if (!isRedisHealthy) {
        this.redisFailures++;

        if (this.redisFailures >= REDIS_FAILURE_THRESHOLD) {
          await this.handleRedisFailure();
        }
        return; // Skip rest of health check if Redis is down
      } else {
        // Reset failure counter if Redis is healthy
        if (this.redisFailures > 0) {
          this.redisFailures = 0;
        }
      }

      if (!this.queue) {
        return;
      }

      const counts = await this.queue.getJobCounts();
      const used = process.memoryUsage();
      const memoryStats = {
        rss: `${Math.round(used.rss / 1024 / 1024)}MB`,
        heapTotal: `${Math.round(used.heapTotal / 1024 / 1024)}MB`,
        heapUsed: `${Math.round(used.heapUsed / 1024 / 1024)}MB`,
        external: `${Math.round(used.external / 1024 / 1024)}MB`,
      };

      // Get max heap from NODE_OPTIONS or use default
      const maxHeapMatch = process.execArgv.join(' ').match(/--max-old-space-size=(\d+)/);
      const MAX_HEAP_MB = maxHeapMatch ? parseInt(maxHeapMatch[1]) : 8192;

      const heapUsedMB = used.heapUsed / 1024 / 1024;
      const _heapTotalMB = used.heapTotal / 1024 / 1024;
      const heapPercentOfMax = (heapUsedMB / MAX_HEAP_MB) * 100;
      const _heapPercentOfTotal = (used.heapUsed / used.heapTotal) * 100;

      // Check if queue is paused
      const isPaused = await this.queue.isPaused();

      // Memory threshold warnings (proactive GC adjusts these downward)
      // With proactive GC at 50%, these thresholds serve as safety nets for GC failure scenarios
      const MEMORY_WARNING_MB = 3000;  // 69% of heap - GC may be struggling
      const MEMORY_CRITICAL_MB = 3500; // 80% of heap - GC failure, manual intervention needed

      if (heapUsedMB > MEMORY_CRITICAL_MB) {
        errorLog('Memory', `CRITICAL: Heap at ${Math.round(heapUsedMB)}MB - approaching ${MAX_HEAP_MB}MB limit`);
        this.cleanupMemory();
        // Force GC if available
        if (global.gc) {
          global.gc();
        }
      } else if (heapUsedMB > MEMORY_WARNING_MB) {
        warnLog('Memory', `WARNING: Heap at ${Math.round(heapUsedMB)}MB - approaching limit`);
      }

      // Also trigger cleanup at 75% of max (fallback)
      if (heapPercentOfMax > 75) {
        this.cleanupMemory();
      }

      const _totalJobs = Object.values(counts).reduce((a, b) => a + b, 0);

      Object.entries(memoryStats).forEach(([_key, _value]) => {
        // Memory stats logging (if needed)
      });

      // Show password status first
      if (this.config) {
        const hasPassword = !!this.config.getPassword();
        if (!hasPassword) {
          // Password not available
        } else {
          // Password available
        }
      }

      // Show paused status prominently
      if (isPaused) {
        // Queue is paused
      } else {
        // Queue is running
      }

      // Show waiting job monitor status
      if (counts.waiting > 0) {
        // Jobs waiting
      }

      Object.entries(counts).forEach(([key, value]) => {
        const RATE_LIMIT = 100; // Default rate limit for coloring
        const _color = key === 'waiting' && value > 0 ? RED :
          value > RATE_LIMIT * 2 ? RED :
            value > RATE_LIMIT ? YELLOW : GREEN;
        // Queue stats logging (if needed)
      });

      // Show trading disabled cache stats
      const disabledStats = tradingDisabledCache.getStats();
      if (disabledStats.total > 0) {
        Object.entries(disabledStats.byMarketplace).forEach(([_marketplace, _count]) => {
          // Trading disabled stats logging (if needed)
        });
      }

      // No need to call updateAllBidRates() - tracker handles updates automatically

    } catch (error) {
      // Log as debug level to avoid sending to Axiom
      debugLog('Health', `Health monitoring failed: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Clean up memory by triggering garbage collection if available
   */
  private cleanupMemory(): void {
    if (global.gc) {
      global.gc();
    }
  }

  /**
   * Check for password availability and resume if found
   */
  private async checkPasswordAndResume(): Promise<void> {
    // Prevent concurrent checks
    if (this.isCheckingPassword) return;

    if (!this.config) {
      return;
    }

    try {
      this.isCheckingPassword = true;

      // If password is already set, nothing to do
      if (this.config.getPassword()) return;

      // Try to recover password from Redis
      const recovered = await this.config.recoverPasswordFromRedis();
      if (recovered) {
        await this.config.resumeQueueAndWorkers();
      }
    } catch (error) {
      errorLog('PasswordCheck', 'Error checking password', error);
    } finally {
      this.isCheckingPassword = false;
    }
  }

  /**
   * Helper to detect Redis errors
   */
  public isRedisError(error: any): boolean {
    const errorMessage = error instanceof Error ? error.message : String(error);
    return errorMessage.includes('ECONNREFUSED') ||
      errorMessage.includes('ECONNRESET') ||
      errorMessage.includes('ETIMEDOUT') ||
      errorMessage.includes('Connection is closed') ||
      errorMessage.includes('Connection lost') ||
      errorMessage.includes('READONLY');
  }

  /**
   * Get health monitoring stats
   */
  public getStats() {
    return {
      redisFailures: this.redisFailures,
      lastRedisRecovery: this.lastRedisRecovery,
      isMonitoring: this.monitorInterval !== null,
      isCheckingPassword: this.isCheckingPassword
    };
  }
}

// Singleton export
export const healthMonitorService = new HealthMonitorService();
