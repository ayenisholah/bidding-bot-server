import { Job, Queue } from 'bullmq';
import { withRedisRetry } from './redis';
import { errorLog } from './debug-file-logger';
import { LRUCache } from 'lru-cache';

interface JobFallbackData {
  jobId: string;
  taskId: string;
  marketplace: string;
  data: any;
}

// LRU cache for job fallback storage
const jobFallbackLRU = new LRUCache<string, JobFallbackData>({
  max: 10000,
  ttl: 30 * 60 * 1000, // 30 minutes
  allowStale: false,
  updateAgeOnGet: false
});

// Wrapper class for compatibility with existing API
class JobFallbackStore {

  public save(jobId: string, taskId: string, marketplace: string, data: any): void {
    // LRU handles capacity and eviction automatically
    jobFallbackLRU.set(jobId, {
      jobId,
      taskId,
      marketplace,
      data
    });
  }

  public get(jobId: string): JobFallbackData | null {
    // LRU handles expiration automatically
    return jobFallbackLRU.get(jobId) || null;
  }

  public delete(jobId: string): void {
    jobFallbackLRU.delete(jobId);
  }

  public getByTaskId(taskId: string): JobFallbackData[] {
    const results: JobFallbackData[] = [];

    // LRU only contains non-expired entries
    for (const [, entry] of jobFallbackLRU.entries()) {
      if (entry.taskId === taskId) {
        results.push(entry);
      }
    }

    return results;
  }

  public size(): number {
    return jobFallbackLRU.size;
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
        timestamp: Date.now(),
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
        timestamp: Date.now(),
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

// LRU automatically cleans up expired entries - no manual cleanup needed
// Optional: Keep interval for monitoring only
setInterval(() => {
  const currentSize = jobFallbackStore.size();
  if (currentSize > 0) {
    // console.log(`[JobFallback] Current cache size: ${currentSize}/${jobFallbackLRU.max}`);
  }
}, 5 * 60 * 1000); // Every 5 minutes