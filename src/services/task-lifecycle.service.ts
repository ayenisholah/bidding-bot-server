import { ITask } from '../models/task.model';
import { warnLog } from '../utils/debug-file-logger';

/**
 * Task Lifecycle Service
 *
 * Manages batch operations for task lifecycle (bulk start/stop).
 *
 * Responsibilities:
 * - Batch task start operations (bulk start with throttling)
 * - Batch task stop operations (bulk stop with throttling)
 * - Retry queue management for failed operations
 *
 * Features:
 * - Fire-and-forget batch operations to prevent blocking
 * - Automatic retry with exponential backoff
 * - Staggered delays to avoid thundering herd
 * - Comprehensive logging and error handling
 *
 * Note: Core start/stop logic (startTask, stopTask) remains in index.ts
 * This service only handles batch orchestration.
 */

/**
 * Dependencies required by TaskLifecycleService
 */
interface TaskLifecycleDependencies {
  failedTaskStartQueue: Map<string, any>;
  failedTaskStopQueue: Map<string, any>;
}

class TaskLifecycleService {
  private failedTaskStartQueue: Map<string, any> | null = null;
  private failedTaskStopQueue: Map<string, any> | null = null;

  /**
   * Initialize the service with dependencies
   */
  public initialize(deps: TaskLifecycleDependencies): void {
    this.failedTaskStartQueue = deps.failedTaskStartQueue;
    this.failedTaskStopQueue = deps.failedTaskStopQueue;

  }

  // =============================================================================
  // BATCH OPERATIONS (Bulk start/stop with throttling)
  // =============================================================================

  /**
   * Start tasks in parallel batches using fire-and-forget pattern.
   * Tasks are dispatched immediately without waiting for individual completion.
   * This ensures all tasks start at roughly the same time regardless of individual task startup duration.
   * Failed starts are automatically queued for retry with exponential backoff.
   */
  public async startTasksInBatches(
    tasks: ITask[],
    startTaskFn: (task: ITask, start: boolean) => Promise<any>,
    intervalMs: number = 100
  ): Promise<void> {
    const PARALLEL_BATCH_SIZE = 10; // Dispatch tasks per batch
    const STAGGER_WITHIN_BATCH_MS = 20; // Small delay within batch to avoid thundering herd
    const dispatchStartTime = Date.now();

    if (!this.failedTaskStartQueue) {
      warnLog('TaskLifecycle', 'Cannot start tasks in batches - service not initialized');
      return;
    }


    // Split tasks into batches
    const batches: ITask[][] = [];
    for (let i = 0; i < tasks.length; i += PARALLEL_BATCH_SIZE) {
      batches.push(tasks.slice(i, i + PARALLEL_BATCH_SIZE));
    }

    for (let batchIndex = 0; batchIndex < batches.length; batchIndex++) {
      const batch = batches[batchIndex];

      // Fire-and-forget: dispatch all tasks in batch without waiting for completion
      for (const task of batch) {
        const taskId = task._id?.toString() || 'unknown';
        const taskSlug = task.contract?.slug || taskId;
        const capturedBatchIndex = batchIndex; // Capture for async closure

        // Fire off task start asynchronously - don't await
        startTaskFn(task, true)
          .then(() => {
            // Success - remove from retry queue if present
            this.failedTaskStartQueue!.delete(taskId);
          })
          .catch((err: any) => {
            warnLog('BulkStart', `TASK_START_FAILED | taskId=${taskId} | slug=${taskSlug} | batch=${capturedBatchIndex + 1} | error=${err?.message || String(err)}`);

            // Add to retry queue with exponential backoff
            const existing = this.failedTaskStartQueue!.get(taskId);
            const retryCount = existing ? existing.retryCount + 1 : 1;
            const backoffMs = Math.min(5000 * Math.pow(2, retryCount - 1), 60000);

            this.failedTaskStartQueue!.set(taskId, {
              task,
              retryCount,
              nextRetryAt: Date.now() + backoffMs,
              lastError: err?.message || String(err)
            });
          });

        // Small stagger within batch to avoid thundering herd
        await new Promise(resolve => setTimeout(resolve, STAGGER_WITHIN_BATCH_MS));
      }

      // Delay between batches (skip after last batch)
      if (batchIndex < batches.length - 1) {
        await new Promise(resolve => setTimeout(resolve, intervalMs));
      }
    }

    const dispatchDuration = Date.now() - dispatchStartTime;
  }

  /**
   * Stop tasks in batches using fire-and-forget pattern.
   * Tasks are dispatched immediately without waiting for individual completion.
   * Failed stops are automatically queued for retry with exponential backoff.
   */
  public async stopTasksInBatches(
    tasks: ITask[],
    stopTaskFn: (task: ITask, start: boolean) => Promise<any>,
    batchSize: number = 10
  ): Promise<void> {
    const STAGGER_WITHIN_BATCH_MS = 20; // Small delay within batch to avoid thundering herd
    const BATCH_DELAY_MS = 100; // Linear delay between batches
    const dispatchStartTime = Date.now();

    if (!this.failedTaskStopQueue) {
      warnLog('TaskLifecycle', 'Cannot stop tasks in batches - service not initialized');
      return;
    }

    // Split tasks into batches
    const batches: ITask[][] = [];
    for (let i = 0; i < tasks.length; i += batchSize) {
      batches.push(tasks.slice(i, i + batchSize));
    }

    for (let batchIndex = 0; batchIndex < batches.length; batchIndex++) {
      const batch = batches[batchIndex];

      // Fire-and-forget: dispatch all stops without waiting for completion
      for (const task of batch) {
        const taskId = task._id?.toString() || 'unknown';
        const taskSlug = task.contract?.slug || taskId;
        const capturedBatchIndex = batchIndex; // Capture for async closure

        // Fire off task stop asynchronously - don't await
        stopTaskFn(task, false)
          .then(() => {
            // Success - remove from retry queue if present
            this.failedTaskStopQueue!.delete(taskId);
          })
          .catch((err: any) => {
            warnLog('StopTaskFlow', `TASK_STOP_FAILED | taskId=${taskId} | slug=${taskSlug} | batch=${capturedBatchIndex + 1} | error=${err?.message || String(err)}`);

            // Add to retry queue with exponential backoff
            const existing = this.failedTaskStopQueue!.get(taskId);
            const retryCount = existing ? existing.retryCount + 1 : 1;
            const backoffMs = Math.min(5000 * Math.pow(2, retryCount - 1), 60000);

            this.failedTaskStopQueue!.set(taskId, {
              task,
              retryCount,
              nextRetryAt: Date.now() + backoffMs,
              lastError: err?.message || String(err)
            });
          });

        // Small stagger within batch to avoid thundering herd on ws-proxy unsubscriptions
        await new Promise(resolve => setTimeout(resolve, STAGGER_WITHIN_BATCH_MS));
      }

      // Linear delay between batches (skip delay after last batch)
      if (batchIndex < batches.length - 1) {
        await new Promise(resolve => setTimeout(resolve, BATCH_DELAY_MS));
      }
    }

    const duration = Date.now() - dispatchStartTime;
  }

  // =============================================================================
  // DIAGNOSTICS & STATS
  // =============================================================================

  /**
   * Get service statistics
   */
  public getStats() {
    return {
      failedStarts: this.failedTaskStartQueue?.size || 0,
      failedStops: this.failedTaskStopQueue?.size || 0,
      initialized: this.failedTaskStartQueue !== null && this.failedTaskStopQueue !== null
    };
  }
}

export const taskLifecycleService = new TaskLifecycleService();
