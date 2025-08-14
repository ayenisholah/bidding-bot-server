import { blurRateLimiter } from './blur-rate-limiter';
import { blurCompletionTracker } from './blur-completion-tracker';

interface QueuedBlurJob {
  job: any;
  taskId: string;
  addedAt: number;
}

/**
 * Global coordinator for Blur jobs to prevent overwhelming the API
 * Ensures we stay within the 40 bids per 10 seconds limit across all tasks
 */
export class BlurJobCoordinator {
  private pendingJobs: QueuedBlurJob[] = [];
  private processingInterval: NodeJS.Timeout | null = null;
  private isProcessing = false;
  
  // Conservative limits to avoid hitting rate limits
  private readonly MAX_JOBS_PER_BATCH = 10; // Process 10 jobs at a time
  private readonly BATCH_INTERVAL = 3000; // Every 3 seconds
  private readonly MAX_JOBS_PER_10_SECONDS = 35; // Leave buffer from 40 limit
  
  constructor() {
    // Start the processing interval
    this.startProcessing();
  }
  
  /**
   * Add jobs to the coordinator queue
   */
  async addJobs(jobs: any[], taskId: string): Promise<void> {
    const now = Date.now();
    const queuedJobs = jobs.map(job => ({
      job,
      taskId,
      addedAt: now
    }));
    
    this.pendingJobs.push(...queuedJobs);
    console.log(`[Blur Coordinator] Added ${jobs.length} jobs for task ${taskId}. Total pending: ${this.pendingJobs.length}`);
  }
  
  /**
   * Start the interval that processes pending jobs
   */
  private startProcessing(): void {
    if (this.processingInterval) return;
    
    this.processingInterval = setInterval(() => {
      this.processPendingJobs();
    }, this.BATCH_INTERVAL);
  }
  
  /**
   * Process pending jobs while respecting rate limits
   */
  private async processPendingJobs(): Promise<void> {
    if (this.isProcessing || this.pendingJobs.length === 0) return;
    
    this.isProcessing = true;
    
    try {
      // Group jobs by wallet to check rate limits
      const jobsByWallet = new Map<string, QueuedBlurJob[]>();
      
      for (const queuedJob of this.pendingJobs) {
        const walletAddress = queuedJob.job.data.address;
        if (!jobsByWallet.has(walletAddress)) {
          jobsByWallet.set(walletAddress, []);
        }
        jobsByWallet.get(walletAddress)!.push(queuedJob);
      }
      
      const jobsToProcess: QueuedBlurJob[] = [];
      const jobsToDelay: QueuedBlurJob[] = [];
      
      // Check each wallet's rate limit status
      for (const [walletAddress, walletJobs] of jobsByWallet) {
        // Check completion tracker
        const requiredDelay = blurCompletionTracker.getRequiredDelay(walletAddress);
        
        if (requiredDelay > 0) {
          console.log(`[Blur Coordinator] Wallet ${walletAddress} needs ${requiredDelay}ms delay`);
          jobsToDelay.push(...walletJobs);
          continue;
        }
        
        // Check how many jobs we can process for this wallet
        let availableSlots = this.MAX_JOBS_PER_BATCH - jobsToProcess.length;
        
        for (const job of walletJobs) {
          if (availableSlots <= 0) {
            jobsToDelay.push(job);
          } else if (blurRateLimiter.canMakeRequest(walletAddress)) {
            jobsToProcess.push(job);
            availableSlots--;
          } else {
            jobsToDelay.push(job);
          }
        }
      }
      
      // Remove processed jobs from pending
      this.pendingJobs = jobsToDelay;
      
      // Return the jobs to be added to the actual queue
      if (jobsToProcess.length > 0) {
        console.log(`[Blur Coordinator] Processing ${jobsToProcess.length} jobs, ${jobsToDelay.length} delayed`);
        
        // Get the blur queue instance and add jobs
        const { blurQueue } = await import('../index');
        
        const jobsToAdd = jobsToProcess.map(qj => ({
          name: qj.job.name,
          data: qj.job.data,
          opts: qj.job.opts
        }));
        
        await blurQueue.addBulk(jobsToAdd);
        
        // Track that we're processing these jobs
        for (const qj of jobsToProcess) {
          const walletAddress = qj.job.data.address;
          blurRateLimiter.recordRequest(walletAddress);
        }
      }
      
      // Clean up old pending jobs (older than 5 minutes)
      const cutoffTime = Date.now() - 5 * 60 * 1000;
      this.pendingJobs = this.pendingJobs.filter(job => job.addedAt > cutoffTime);
      
    } catch (error) {
      console.error('[Blur Coordinator] Error processing jobs:', error);
    } finally {
      this.isProcessing = false;
    }
  }
  
  /**
   * Get status of the coordinator
   */
  getStatus(): any {
    const jobsByTask = new Map<string, number>();
    
    for (const job of this.pendingJobs) {
      jobsByTask.set(job.taskId, (jobsByTask.get(job.taskId) || 0) + 1);
    }
    
    return {
      totalPending: this.pendingJobs.length,
      pendingByTask: Object.fromEntries(jobsByTask),
      isProcessing: this.isProcessing,
      maxJobsPerBatch: this.MAX_JOBS_PER_BATCH,
      batchInterval: this.BATCH_INTERVAL
    };
  }
  
  /**
   * Stop the coordinator
   */
  stop(): void {
    if (this.processingInterval) {
      clearInterval(this.processingInterval);
      this.processingInterval = null;
    }
  }
}

// Singleton instance
export const blurJobCoordinator = new BlurJobCoordinator();