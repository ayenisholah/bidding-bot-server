import { debugLog, warnLog, errorLog } from '../utils/debug-file-logger';

/**
 * Memory Cleanup Service
 *
 * Implements business-logic-aligned TTL cleanup for unbounded data structures.
 *
 * Note: processedMessages now uses lru-cache with automatic TTL, no cleanup needed here.
 *
 * Cleanup Strategies:
 * - 5min TTL: Short-term operations (queueing progress)
 * - 30min TTL: Retry queues and stale locks
 */

const RETRY_TTL = 30 * 60 * 1000; // 30 minutes
const QUEUEING_TTL = 5 * 60 * 1000; // 5 minutes
const LOCK_STALE_THRESHOLD = 30 * 60 * 1000; // 30 minutes

interface MemoryCleanupDependencies {
  queueingProgress: Map<string, any>;
  failedTaskStartQueue: Map<string, any>;
  failedTaskStopQueue: Map<string, any>;
  taskLocks: Map<string, any>;
  loopSchedulingLocks: Map<string, any>;
  // Blur-specific caches and metrics
  blurTransferRestrictedCollections?: Map<string, number>;
  blurAuthMetrics?: { failureReasons: Map<string, number> };
  blurTimeoutMetrics?: { errorsByEndpoint: Map<string, number> };
  // Note: processedMessages uses lru-cache now (automatic cleanup)
  // Note: tasksInCompletionProcess removed - it's a lock mechanism with self-cleanup
}

class MemoryCleanupService {
  private deps: MemoryCleanupDependencies | null = null;
  private cleanupIntervals: NodeJS.Timeout[] = [];

  /**
   * Initialize the service with dependencies
   */
  public initialize(deps: MemoryCleanupDependencies): void {
    this.deps = deps;
    this.startCleanupIntervals();
    debugLog('MemoryCleanupService', 'Initialized with periodic cleanup intervals');
  }

  /**
   * Start all periodic cleanup intervals
   */
  private startCleanupIntervals(): void {
    if (!this.deps) return;

    // Note: processedMessages cleanup removed - now using lru-cache with automatic TTL

    // Cleanup 1: queueingProgress (every 5 min, 5-min TTL)
    this.cleanupIntervals.push(setInterval(() => {
      this.cleanupQueueingProgress();
    }, 5 * 60 * 1000));

    // Cleanup 2: failedTaskQueues (every 5 min, 30-min TTL)
    this.cleanupIntervals.push(setInterval(() => {
      this.cleanupFailedQueues();
    }, 5 * 60 * 1000));

    // Cleanup 3: Stale locks (every 5 min, 30-min age threshold)
    this.cleanupIntervals.push(setInterval(() => {
      this.cleanupStaleLocks();
    }, 5 * 60 * 1000));

    // Cleanup 4: Blur-specific caches (every 5 min)
    this.cleanupIntervals.push(setInterval(() => {
      this.cleanupBlurCaches();
    }, 5 * 60 * 1000));

    debugLog('MemoryCleanup', `Started ${this.cleanupIntervals.length} cleanup intervals`);
  }

  /**
   * Cleanup 1: queueingProgress - Job queueing progress tracking
   * TTL: 5 minutes (queueing should complete within this window)
   */
  private cleanupQueueingProgress(): void {
    if (!this.deps) return;

    const cutoff = Date.now() - QUEUEING_TTL;
    let cleaned = 0;

    for (const [id, progress] of this.deps.queueingProgress.entries()) {
      if (progress.startTime && progress.startTime < cutoff) {
        this.deps.queueingProgress.delete(id);
        cleaned++;
      }
    }

    if (cleaned > 0) {
      debugLog('MemoryCleanup', `Cleaned ${cleaned} stale entries from queueingProgress (TTL: 5min)`);
    }
  }

  /**
   * Cleanup 2: failedTaskQueues - Task retry queues
   * TTL: 30 minutes (give ample time for retries)
   * Max Retries: 5 attempts
   */
  private cleanupFailedQueues(): void {
    if (!this.deps) return;

    const MAX_RETRIES = 5;
    const cutoff = Date.now() - RETRY_TTL;
    let cleaned = 0;

    // Clean start queue
    for (const [id, retry] of this.deps.failedTaskStartQueue.entries()) {
      const shouldRemove =
        retry.timestamp < cutoff || // Age-based
        retry.attemptCount >= MAX_RETRIES; // Retry limit

      if (shouldRemove) {
        this.deps.failedTaskStartQueue.delete(id);
        cleaned++;
        if (retry.attemptCount >= MAX_RETRIES) {
          errorLog('TaskRetry', `Max retries (${MAX_RETRIES}) exceeded for start queue: ${id}`);
        }
      }
    }

    // Clean stop queue
    for (const [id, retry] of this.deps.failedTaskStopQueue.entries()) {
      const shouldRemove =
        retry.timestamp < cutoff ||
        retry.attemptCount >= MAX_RETRIES;

      if (shouldRemove) {
        this.deps.failedTaskStopQueue.delete(id);
        cleaned++;
        if (retry.attemptCount >= MAX_RETRIES) {
          errorLog('TaskRetry', `Max retries (${MAX_RETRIES}) exceeded for stop queue: ${id}`);
        }
      }
    }

    if (cleaned > 0) {
      debugLog('MemoryCleanup', `Cleaned ${cleaned} entries from failed task queues (TTL: 30min, max retries: 5)`);
    }
  }

  /**
   * Cleanup 3: Stale locks - Orphaned locks from crashed tasks
   * Age Threshold: 30 minutes
   */
  private cleanupStaleLocks(): void {
    if (!this.deps) return;

    const cutoff = Date.now() - LOCK_STALE_THRESHOLD;
    let cleaned = 0;

    // Clean loopSchedulingLocks
    for (const [id, lock] of this.deps.loopSchedulingLocks.entries()) {
      if (lock.timestamp && lock.timestamp < cutoff) {
        this.deps.loopSchedulingLocks.delete(id);
        cleaned++;
        warnLog('LockCleanup', `Removed stale loop scheduling lock: ${id} (age > 30min)`);
      }
    }

    // Clean taskLocks
    for (const [id, lock] of this.deps.taskLocks.entries()) {
      if (lock.acquiredAt && lock.acquiredAt < cutoff) {
        this.deps.taskLocks.delete(id);
        cleaned++;
        warnLog('LockCleanup', `Removed stale task lock: ${id} (age > 30min)`);
      }
    }

    if (cleaned > 0) {
      debugLog('MemoryCleanup', `Cleaned ${cleaned} stale locks (age threshold: 30min)`);
    }
  }

  /**
   * Cleanup 4: Blur-specific caches - Transfer restrictions and metrics
   * Transfer restricted collections: Check and delete expired entries
   * Metrics maps: Reset if they exceed size threshold
   */
  private cleanupBlurCaches(): void {
    if (!this.deps) return;

    // Cleanup 1: Transfer restricted collections with expired TTL
    if (this.deps.blurTransferRestrictedCollections) {
      const now = Date.now();
      let cleaned = 0;
      for (const [slug, expiresAt] of this.deps.blurTransferRestrictedCollections.entries()) {
        if (expiresAt < now) {
          this.deps.blurTransferRestrictedCollections.delete(slug);
          cleaned++;
        }
      }
      if (cleaned > 0) {
        debugLog('MemoryCleanup', `Cleaned ${cleaned} expired transfer-restricted collections (24h TTL)`);
      }
    }

    // Cleanup 2: Reset metrics if too large (weekly reset pattern)
    const MAX_METRICS_SIZE = 1000;

    // Check auth metrics with proper type guard
    const authMetrics = this.deps.blurAuthMetrics;
    if (authMetrics && authMetrics.failureReasons.size > MAX_METRICS_SIZE) {
      warnLog('MemoryCleanup', `Resetting authMetrics.failureReasons (size: ${authMetrics.failureReasons.size})`);
      authMetrics.failureReasons.clear();
    }

    // Check timeout metrics with proper type guard
    const timeoutMetrics = this.deps.blurTimeoutMetrics;
    if (timeoutMetrics && timeoutMetrics.errorsByEndpoint.size > MAX_METRICS_SIZE) {
      warnLog('MemoryCleanup', `Resetting timeoutMetrics.errorsByEndpoint (size: ${timeoutMetrics.errorsByEndpoint.size})`);
      timeoutMetrics.errorsByEndpoint.clear();
    }
  }

  /**
   * Stop all cleanup intervals
   */
  public stopCleanup(): void {
    this.cleanupIntervals.forEach(interval => clearInterval(interval));
    this.cleanupIntervals = [];
    debugLog('MemoryCleanupService', 'Stopped all cleanup intervals');
  }

  /**
   * Get service statistics
   */
  public getStats() {
    if (!this.deps) {
      return { initialized: false };
    }

    return {
      initialized: true,
      activeCleanupIntervals: this.cleanupIntervals.length,
      currentSizes: {
        // processedMessages now uses lru-cache (track separately)
        queueingProgress: this.deps.queueingProgress.size,
        failedTaskStartQueue: this.deps.failedTaskStartQueue.size,
        failedTaskStopQueue: this.deps.failedTaskStopQueue.size,
        taskLocks: this.deps.taskLocks.size,
        loopSchedulingLocks: this.deps.loopSchedulingLocks.size
      }
    };
  }
}

export const memoryCleanupService = new MemoryCleanupService();
