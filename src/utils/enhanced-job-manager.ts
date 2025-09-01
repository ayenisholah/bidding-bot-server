import { Queue } from 'bullmq';
import { EnhancedTaskStateManager, EnhancedBulkJobParent } from './enhanced-task-state-manager';
import { JobIdParser } from './job-id-parser';
import { ITask } from '../models/task.model';
import { logger, ErrorCategory } from './logger';

interface JobCreationData {
  taskId: string;
  marketplace: string;
  jobType: 'BID' | 'CANCEL';
  data: any;
  options?: any;
}

interface MarketplaceJobBatch {
  marketplace: string;
  jobs: JobCreationData[];
  expectedOrderCount?: number;
}

/**
 * Enhanced Job Manager that handles proper job creation, queueing, and tracking
 * with marketplace-specific batching (especially MagicEden's 500 batch limit)
 */
export class EnhancedJobManager {
  private stateManager: EnhancedTaskStateManager;
  private queue: Queue;
  
  // Marketplace configurations
  private readonly MARKETPLACE_CONFIG = {
    magiceden: {
      batchSize: 500,
      name: 'MagicEden',
      queueName: 'MAGICEDEN_BID'
    },
    opensea: {
      batchSize: 1000, // OpenSea can handle larger batches
      name: 'OpenSea',
      queueName: 'OPENSEA_BID'
    },
    blur: {
      batchSize: 1000, // Blur can handle larger batches
      name: 'Blur',
      queueName: 'BLUR_BID'
    }
  };

  constructor(stateManager: EnhancedTaskStateManager, queue: Queue) {
    this.stateManager = stateManager;
    this.queue = queue;
  }

  /**
   * Create and queue jobs for a task with proper state management
   */
  async createAndQueueTaskJobs(taskId: string, jobBatches: MarketplaceJobBatch[]): Promise<void> {
    // console.log(`[JobManager] Creating jobs for task ${taskId}`);
    
    // Initialize parent jobs for tracking
    await this.initializeParentJobs(taskId, jobBatches);
    
    // Start queueing process
    await this.stateManager.startQueueing(taskId);
    
    let totalQueuedJobs = 0;
    
    // Process each marketplace batch
    for (const batch of jobBatches) {
      const config = this.MARKETPLACE_CONFIG[batch.marketplace as keyof typeof this.MARKETPLACE_CONFIG];
      if (!config) {
        console.warn(`[JobManager] Unknown marketplace: ${batch.marketplace}`);
        continue;
      }
      
      const queuedCount = await this.queueMarketplaceJobs(taskId, batch, config);
      totalQueuedJobs += queuedCount;
      
      // console.log(`[JobManager] Queued ${queuedCount} jobs for ${batch.marketplace}`);
    }
    
    // console.log(`[JobManager] Total queued jobs: ${totalQueuedJobs} for task ${taskId}`);
  }

  /**
   * Queue jobs for a specific marketplace with proper batching
   */
  private async queueMarketplaceJobs(
    taskId: string, 
    batch: MarketplaceJobBatch, 
    config: typeof this.MARKETPLACE_CONFIG.magiceden
  ): Promise<number> {
    let queuedCount = 0;
    
    // Split jobs into batches based on marketplace limits
    const jobChunks = this.chunkArray(batch.jobs, config.batchSize);
    
    for (let i = 0; i < jobChunks.length; i++) {
      const chunk = jobChunks[i];
      const batchIndex = i + 1;
      
      // Create jobs for this chunk
      const jobsToAdd = chunk.map((jobData, index) => {
        const jobId = this.createEnhancedJobId(taskId, batch.marketplace, jobData, batchIndex, index);
        
        return {
          name: config.queueName,
          data: {
            ...jobData.data,
            taskId,
            marketplace: batch.marketplace,
            batchIndex,
            jobIndex: index,
            totalBatches: jobChunks.length
          },
          opts: {
            jobId,
            removeOnComplete: 100,
            removeOnFail: 50,
            ...jobData.options
          }
        };
      });
      
      // Add jobs to queue
      try {
        await this.queue.addBulk(jobsToAdd);
        queuedCount += jobsToAdd.length;
        
        // Update state manager for each queued job
        for (const job of jobsToAdd) {
          await this.stateManager.handleJobQueued(job.opts.jobId);
        }
        
      } catch (error) {
        await logger.logError(ErrorCategory.QUEUE, 'Error adding jobs to queue', {
          taskId,
          marketplace: batch.marketplace,
          batchIndex,
          error: error as Error
        });
      }
    }
    
    return queuedCount;
  }

  /**
   * Create cancellation jobs with proper marketplace batching
   */
  async createCancellationJobs(
    taskId: string, 
    marketplace: string, 
    orders: any[], 
    privateKey: string
  ): Promise<void> {
    const config = this.MARKETPLACE_CONFIG[marketplace as keyof typeof this.MARKETPLACE_CONFIG];
    if (!config) {
      console.warn(`[JobManager] Unknown marketplace for cancellation: ${marketplace}`);
      return;
    }
    
    // console.log(`[JobManager] Creating cancellation jobs for ${orders.length} orders in ${marketplace}`);
    
    // Calculate number of batches needed
    const batchSize = config.batchSize;
    const batchCount = Math.ceil(orders.length / batchSize);
    
    // Initialize parent job for cancellation tracking
    const parentJobId = `${taskId}-CANCEL_${marketplace.toUpperCase()}_BID`;
    const parentJob: EnhancedBulkJobParent = {
      totalJobs: batchCount,
      queuedJobs: 0,
      completedJobs: 0,
      failedJobs: 0,
      taskId: taskId,
      marketplace: marketplace,
      jobType: 'CANCEL',
      batchSize: batchSize,
      expectedOrderCount: orders.length,
      actualOrderCount: orders.length
    };
    
    this.stateManager.setParentJob(parentJobId, parentJob);
    
    // Create batches of orders
    for (let i = 0; i < orders.length; i += batchSize) {
      const batchOrders = orders.slice(i, i + batchSize);
      const batchIndex = Math.floor(i / batchSize) + 1;
      
      // Create job data based on marketplace
      let jobData: any;
      let jobId: string;
      
      if (marketplace === 'magiceden') {
        const extractedOrderIds = await this.extractMagicedenOrderHash(batchOrders);
        const orderKeys = batchOrders.map((order: any) => order.id);
        
        jobData = {
          orderIds: extractedOrderIds,
          privateKey,
          orderKeys,
          taskId
        };
        
        jobId = `CANCEL_MAGICEDEN_BID-${taskId}-${orderKeys[0]}-batch${batchIndex}`;
      } else {
        // For OpenSea and Blur
        jobData = {
          orders: batchOrders,
          privateKey,
          taskId
        };
        
        jobId = `CANCEL_${marketplace.toUpperCase()}_BID-${taskId}-batch${batchIndex}`;
      }
      
      // Add job to queue
      try {
        await this.queue.add(`CANCEL_${marketplace.toUpperCase()}_BID`, jobData, { 
          jobId,
          removeOnComplete: 10,
          removeOnFail: 10
        });
        
        // Update parent job queued count
        parentJob.queuedJobs += 1;
        this.stateManager.setParentJob(parentJobId, parentJob);
        
        // console.log(`[JobManager] Queued cancellation batch ${batchIndex}/${batchCount} for ${marketplace}`);
        
      } catch (error) {
        await logger.logError(ErrorCategory.QUEUE, 'Error queuing cancellation job', {
          taskId,
          marketplace,
          batchIndex,
          error: error as Error
        });
      }
    }
    
    // Update decrement counter (this would need to be implemented based on existing logic)
    this.decrementOwnBidCounter(marketplace, orders.length);
  }

  /**
   * Initialize parent jobs for tracking
   */
  private async initializeParentJobs(taskId: string, jobBatches: MarketplaceJobBatch[]): Promise<void> {
    for (const batch of jobBatches) {
      const config = this.MARKETPLACE_CONFIG[batch.marketplace as keyof typeof this.MARKETPLACE_CONFIG];
      if (!config) continue;
      
      const parentJobId = `${taskId}-${batch.marketplace.toUpperCase()}`;
      const parentJob: EnhancedBulkJobParent = {
        totalJobs: batch.jobs.length,
        queuedJobs: 0,
        completedJobs: 0,
        failedJobs: 0,
        taskId: taskId,
        marketplace: batch.marketplace,
        jobType: 'BID',
        batchSize: config.batchSize,
        expectedOrderCount: batch.expectedOrderCount || batch.jobs.length
      };
      
      this.stateManager.setParentJob(parentJobId, parentJob);
    }
  }

  /**
   * Create enhanced job ID with better parsing support
   */
  private createEnhancedJobId(
    taskId: string, 
    marketplace: string, 
    jobData: JobCreationData, 
    batchIndex: number, 
    jobIndex: number
  ): string {
    // Use a more robust delimiter to avoid parsing issues
    // Format: taskId::marketplace::jobType::batchIndex::jobIndex::timestamp
    const timestamp = Date.now();
    return `${taskId}::${marketplace}::${jobData.jobType}::${batchIndex}::${jobIndex}::${timestamp}`;
  }

  /**
   * Parse enhanced job ID (for backward compatibility with existing parsing)
   */
  static parseEnhancedJobId(jobId: string): {
    taskId: string;
    marketplace: string;
    jobType: string;
    batchIndex: number;
    jobIndex: number;
    timestamp: number;
    isEnhanced: boolean;
  } | null {
    // Check if it's the enhanced format
    if (jobId.includes('::')) {
      const parts = jobId.split('::');
      if (parts.length >= 6) {
        return {
          taskId: parts[0],
          marketplace: parts[1],
          jobType: parts[2],
          batchIndex: parseInt(parts[3]),
          jobIndex: parseInt(parts[4]),
          timestamp: parseInt(parts[5]),
          isEnhanced: true
        };
      }
    }
    
    // Fall back to legacy parsing
    const legacyParsed = JobIdParser.parseJobId(jobId);
    if (legacyParsed.isValid) {
      return {
        taskId: legacyParsed.taskId,
        marketplace: legacyParsed.marketplace || '',
        jobType: legacyParsed.jobType || 'BID',
        batchIndex: 1,
        jobIndex: 1,
        timestamp: Date.now(),
        isEnhanced: false
      };
    }
    
    return null;
  }

  /**
   * Chunk array into smaller arrays
   */
  private chunkArray<T>(array: T[], chunkSize: number): T[][] {
    const chunks: T[][] = [];
    for (let i = 0; i < array.length; i += chunkSize) {
      chunks.push(array.slice(i, i + chunkSize));
    }
    return chunks;
  }

  /**
   * Extract MagicEden order hash (placeholder - implement based on existing logic)
   */
  private async extractMagicedenOrderHash(orders: any[]): Promise<string[]> {
    // This would need to be implemented based on the existing extractMagicedenOrderHash function
    // For now, return order IDs as placeholder
    return orders.map(order => order.id || order.orderId);
  }

  /**
   * Decrement own bid counter (placeholder - implement based on existing logic)
   */
  private decrementOwnBidCounter(marketplace: string, count: number): void {
    // This would need to be implemented based on the existing decrementOwnBidCounter function
    // console.log(`[JobManager] Decrementing ${marketplace} bid counter by ${count}`);
  }

  /**
   * Get job statistics for a task
   */
  getTaskJobStatistics(taskId: string): {
    totalJobs: number;
    queuedJobs: number;
    completedJobs: number;
    failedJobs: number;
    marketplaceBreakdown: { [marketplace: string]: any };
  } {
    let totalJobs = 0;
    let queuedJobs = 0;
    let completedJobs = 0;
    let failedJobs = 0;
    const marketplaceBreakdown: { [marketplace: string]: any } = {};
    
    // Iterate through parent jobs to calculate statistics
    this.stateManager.getParentJobs().forEach((job, key) => {
      if (job.taskId === taskId) {
        totalJobs += job.totalJobs;
        queuedJobs += job.queuedJobs;
        completedJobs += job.completedJobs;
        failedJobs += job.failedJobs;
        
        marketplaceBreakdown[job.marketplace] = {
          totalJobs: job.totalJobs,
          queuedJobs: job.queuedJobs,
          completedJobs: job.completedJobs,
          failedJobs: job.failedJobs,
          batchSize: job.batchSize,
          expectedOrderCount: job.expectedOrderCount
        };
      }
    });
    
    return {
      totalJobs,
      queuedJobs,
      completedJobs,
      failedJobs,
      marketplaceBreakdown
    };
  }
}