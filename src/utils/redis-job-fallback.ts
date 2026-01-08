import { Job, Queue } from 'bullmq';
import { withRedisRetry } from './redis';
import { errorLog, debugLog } from './debug-file-logger';

interface JobFallbackData {
  jobId: string;
  taskId: string;
  marketplace: string;
  timestamp: number;
  data: any;
}

// In-memory fallback storage for critical job data
class JobFallbackStore {
  private store: Map<string, JobFallbackData> = new Map();
  private readonly maxSize = 10000; // Limit memory usage
  private readonly ttl = 30 * 60 * 1000; // 30 minutes TTL

  public save(jobId: string, taskId: string, marketplace: string, data: any): void {
    // Clean old entries if at capacity
    if (this.store.size >= this.maxSize) {
      this.cleanOldEntries();
    }

    this.store.set(jobId, {
      jobId,
      taskId,
      marketplace,
      timestamp: Date.now(),
      data
    });
  }

  public get(jobId: string): JobFallbackData | null {
    const entry = this.store.get(jobId);
    if (!entry) return null;

    // Check if entry is expired
    if (Date.now() - entry.timestamp > this.ttl) {
      this.store.delete(jobId);
      return null;
    }

    return entry;
  }

  public delete(jobId: string): void {
    this.store.delete(jobId);
  }

  public getByTaskId(taskId: string): JobFallbackData[] {
    const results: JobFallbackData[] = [];
    const now = Date.now();

    for (const [jobId, entry] of this.store.entries()) {
      if (entry.taskId === taskId && (now - entry.timestamp <= this.ttl)) {
        // console.log(`[JobFallbackStore] Found job ${jobId} for task ${taskId} in store`);
        results.push(entry);
      }
    }

    return results;
  }

  private cleanOldEntries(): void {
    const now = Date.now();
    const entriesToDelete: string[] = [];

    for (const [jobId, entry] of this.store.entries()) {
      if (now - entry.timestamp > this.ttl) {
        entriesToDelete.push(jobId);
      }
    }

    // Delete oldest 10% if still at capacity
    if (entriesToDelete.length < this.maxSize * 0.1) {
      const sortedEntries = Array.from(this.store.entries())
        .sort((a, b) => a[1].timestamp - b[1].timestamp);
      
      const deleteCount = Math.floor(this.maxSize * 0.1);
      for (let i = 0; i < deleteCount && i < sortedEntries.length; i++) {
        entriesToDelete.push(sortedEntries[i][0]);
      }
    }

    const deletedCount = entriesToDelete.length;
    entriesToDelete.forEach(jobId => this.store.delete(jobId));
  }

  public size(): number {
    return this.store.size;
  }
}

export const jobFallbackStore = new JobFallbackStore();

/**
 * Safely get a job with fallback mechanism
 */
export async function safeGetJob(
  queue: Queue,
  jobId: string,
  taskId?: string,
  marketplace?: string
): Promise<Job | null> {
  try {
    // Try to get job from Redis with retry
    const job = await withRedisRetry(
      async () => queue.getJob(jobId),
      `getJob-${jobId}`,
      3
    );

    if (job) {
      // Save to fallback store if we have task info
      if (taskId && marketplace) {
        jobFallbackStore.save(jobId, taskId, marketplace, job.data);
      }
      return job;
    }

    // Try fallback store
    const fallback = jobFallbackStore.get(jobId);
    if (fallback) {
      // console.log(`[JobFallback] Retrieved job ${jobId} from fallback store`);
      // Create a minimal job object for processing
      return {
        id: jobId,
        data: fallback.data,
        name: 'bid',
        opts: {},
        timestamp: fallback.timestamp,
        attemptsMade: 0,
        processedOn: undefined,
        finishedOn: undefined,
        progress: 0
      } as any;
    }

    return null;
  } catch (error) {
    errorLog('Queue', `Failed to get job from Redis | jobId=${jobId}`, error);

    // Try fallback store on error
    const fallback = jobFallbackStore.get(jobId);
    if (fallback) {
      // console.log(`[JobFallback] Retrieved job ${jobId} from fallback store after error`);
      return {
        id: jobId,
        data: fallback.data,
        name: 'bid',
        opts: {},
        timestamp: fallback.timestamp,
        attemptsMade: 0,
        processedOn: undefined,
        finishedOn: undefined,
        progress: 0
      } as any;
    }

    return null;
  }
}

/**
 * Mark job as processed with fallback handling
 */
export async function safeMarkJobAsProcessed(
  taskId: string,
  marketplace: string,
  jobId: string,
  success: boolean,
  updateCallback: (taskId: string, marketplace: string, increment: number) => Promise<void>
): Promise<void> {
  try {
    // Always update the counter - both successful and failed jobs count towards completion
    await updateCallback(taskId, marketplace, 1);
    
    // Clean up fallback store
    jobFallbackStore.delete(jobId);

    // console.log(`[JobFallback] Marked job ${jobId} as processed (${success ? 'success' : 'failed'})`);
  } catch (error) {
    errorLog('Queue', `Error marking job as processed | jobId=${jobId} | success=${success}`, error);
  }
}

/**
 * Get queue health status
 */
export async function getQueueHealth(queue: Queue): Promise<{
  healthy: boolean;
  fallbackSize: number;
  lastError?: string;
}> {
  try {
    const counts = await withRedisRetry(
      async () => queue.getJobCounts(),
      'getJobCounts',
      1
    );
    
    return {
      healthy: counts !== null,
      fallbackSize: jobFallbackStore.size(),
      lastError: counts === null ? 'Failed to get job counts' : undefined
    };
  } catch (error) {
    return {
      healthy: false,
      fallbackSize: jobFallbackStore.size(),
      lastError: error instanceof Error ? error.message : 'Unknown error'
    };
  }
}

// NOTE: recoverStuckJobs() removed - recovery logic was causing issues
// See docs/RECOVERY-FUNCTIONS.md for historical documentation

// Periodic cleanup
setInterval(() => {
  const sizeBefore = jobFallbackStore.size();
  if (sizeBefore > 0) {
    // Trigger cleanup by trying to add a dummy entry
    jobFallbackStore.save('__cleanup__', '', '', {});
    jobFallbackStore.delete('__cleanup__');
    
    const sizeAfter = jobFallbackStore.size();
    if (sizeAfter < sizeBefore) {
      // console.log(`[JobFallback] Cleaned up ${sizeBefore - sizeAfter} expired entries`);
    }
  }
}, 5 * 60 * 1000); // Every 5 minutes