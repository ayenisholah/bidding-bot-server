import { Queue, JobsOptions } from 'bullmq';
import { logger, QueueOperation, ErrorCategory } from './logger';

interface BatchedJob {
  name: string;
  data: any;
  opts?: JobsOptions;
}

class QueueBatcher {
  private queue: Queue;
  private batchQueue: Map<string, BatchedJob[]> = new Map();
  private flushInterval: NodeJS.Timeout | null = null;
  private readonly BATCH_SIZE = 100;
  private readonly FLUSH_INTERVAL = 100; // milliseconds

  constructor(queue: Queue) {
    this.queue = queue;
    this.startBatchProcessor();
  }

  private startBatchProcessor() {
    this.flushInterval = setInterval(() => {
      this.flushAllBatches();
    }, this.FLUSH_INTERVAL);
  }

  async add(name: string, data: any, opts?: JobsOptions): Promise<void> {
    const batchKey = this.getBatchKey(name);
    
    if (!this.batchQueue.has(batchKey)) {
      this.batchQueue.set(batchKey, []);
    }
    
    const batch = this.batchQueue.get(batchKey)!;
    batch.push({ name, data, opts });
    
    // Flush if batch is full
    if (batch.length >= this.BATCH_SIZE) {
      await this.flushBatch(batchKey);
    }
  }

  private getBatchKey(name: string): string {
    // Group similar jobs together for batching
    if (name.includes('CANCEL')) {
      return 'cancellations';
    } else if (name.includes('COUNTERBID')) {
      return 'counterbids';
    } else if (name.includes('BID')) {
      return 'bids';
    }
    return 'other';
  }

  private async flushBatch(batchKey: string): Promise<void> {
    const batch = this.batchQueue.get(batchKey);
    if (!batch || batch.length === 0) return;
    
    // Clear the batch immediately to avoid duplicate processing
    this.batchQueue.set(batchKey, []);
    
    try {
      // Group by job name for more efficient processing
      const jobsByName = new Map<string, BatchedJob[]>();
      
      for (const job of batch) {
        if (!jobsByName.has(job.name)) {
          jobsByName.set(job.name, []);
        }
        jobsByName.get(job.name)!.push(job);
      }
      
      // Process each job type
      for (const [jobName, jobs] of jobsByName) {
        if (jobs.length === 1) {
          // Single job - add normally
          await this.queue.add(jobName, jobs[0].data, jobs[0].opts);
        } else {
          // Multiple jobs - use addBulk
          const bulkJobs = jobs.map(job => ({
            name: jobName,
            data: job.data,
            opts: job.opts || {}
          }));
          
          await this.queue.addBulk(bulkJobs);
          
          await logger.logQueueOperation(QueueOperation.BULK_ADD, {
            jobName,
            count: jobs.length,
            batchKey
          });
        }
      }
    } catch (error) {
      await logger.logError(ErrorCategory.QUEUE, `Failed to flush batch ${batchKey}`, error as Error);
      // Re-add failed jobs to the batch
      if (batch.length > 0) {
        const existingBatch = this.batchQueue.get(batchKey) || [];
        this.batchQueue.set(batchKey, [...batch, ...existingBatch]);
      }
    }
  }

  private async flushAllBatches(): Promise<void> {
    const promises: Promise<void>[] = [];
    
    for (const batchKey of this.batchQueue.keys()) {
      promises.push(this.flushBatch(batchKey));
    }
    
    if (promises.length > 0) {
      await Promise.allSettled(promises);
    }
  }

  async flush(): Promise<void> {
    await this.flushAllBatches();
  }

  destroy() {
    if (this.flushInterval) {
      clearInterval(this.flushInterval);
      this.flushInterval = null;
    }
    
    // Flush remaining jobs
    this.flushAllBatches().catch(error => {
      console.error('Error flushing remaining batches:', error);
    });
  }

  getStats() {
    const stats: Record<string, number> = {};
    for (const [key, batch] of this.batchQueue) {
      stats[key] = batch.length;
    }
    return stats;
  }
}

export default QueueBatcher;