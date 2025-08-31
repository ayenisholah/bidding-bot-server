import { QueueEvents } from 'bullmq';
import { TaskStateManager } from './task-state-manager';
import { ITask } from '../models/task.model';
import { Wallet } from '../models/wallet.model';
import { decryptPrivateKey } from './crypto';
import { calculateBidPrice } from '../functions/bidding';
import { cancelAllRelatedBids, cancelRemovedTraitBids, cancelRemovedTokenBids } from '../functions/cancellation';
import Order from '../models/order.model';
import { logger, ErrorCategory } from './logger';
import { getExpiry } from '../functions/task';

interface RemovalData {
  [taskId: string]: {
    opensea?: string[];
    magiceden?: string[];
    blur?: string[];
  };
}

// Colors for console output
const GREEN = '\x1b[32m';
const RESET = '\x1b[0m';

export class QueueEventHandlerManager {
  private stateManager: TaskStateManager;
  private queueEvents: QueueEvents;
  private enabledMarketplaces: string[];
  private removedTokens: RemovalData = {};
  private traitsToRemove: { [taskId: string]: any[] } = {};

  constructor(
    stateManager: TaskStateManager,
    queueEvents: QueueEvents,
    enabledMarketplaces: string[]
  ) {
    this.stateManager = stateManager;
    this.queueEvents = queueEvents;
    this.enabledMarketplaces = enabledMarketplaces;
    this.initializeHandlers();
  }

  private initializeHandlers(): void {
    this.queueEvents.on('completed', this.handleJobCompleted.bind(this));
    this.queueEvents.on('failed', this.handleJobFailed.bind(this));
  }

  private async handleJobCompleted({ jobId }: { jobId: string }): Promise<void> {
    try {
      // Handle cancellation jobs
      if (this.isCancellationJob(jobId)) {
        await this.handleCancellationJobCompleted(jobId);
        return;
      }

      // Handle regular bidding jobs
      if (this.isBiddingJob(jobId)) {
        await this.handleBiddingJobCompleted(jobId);
      }
    } catch (error) {
      await logger.logError(ErrorCategory.QUEUE, 'Error handling completed job', {
        jobId,
        error: error as Error,
        errorMessage: error instanceof Error ? error.message : String(error)
      });
    }
  }

  private async handleJobFailed({ jobId }: { jobId: string }): Promise<void> {
    try {
      // Handle cancellation job failures
      if (this.isCancellationJob(jobId)) {
        await this.handleCancellationJobFailed(jobId);
        return;
      }

      // Handle regular bidding job failures
      if (this.isBiddingJob(jobId)) {
        await this.handleBiddingJobFailed(jobId);
      }
    } catch (error) {
      await logger.logError(ErrorCategory.QUEUE, 'Error handling failed job', {
        jobId,
        error: error as Error,
        errorMessage: error instanceof Error ? error.message : String(error)
      });
    }
  }

  private isCancellationJob(jobId: string): boolean {
    return jobId.includes('CANCEL_OPENSEA_BID') ||
      jobId.includes('CANCEL_MAGICEDEN_BID') ||
      jobId.includes('CANCEL_BLUR_BID');
  }

  private isBiddingJob(jobId: string): boolean {
    return jobId.split('-').length >= 3;
  }

  private async handleCancellationJobCompleted(jobId: string): Promise<void> {
    const taskId = jobId.split('-')[1];
    const marketplace = this.extractMarketplaceFromCancellationJob(jobId);

    // Update parent job tracking
    const parentJobId = `${taskId}-CANCEL_${marketplace}_BID`;
    const parentJob = await this.stateManager.updateParentJob(parentJobId, { completed: 1 });

    if (!parentJob) return;

    // Calculate cancellation progress
    await this.updateCancellationProgress(taskId, marketplace);
  }

  private async handleCancellationJobFailed(jobId: string): Promise<void> {
    const taskId = jobId.split('-')[1];
    const marketplace = this.extractMarketplaceFromCancellationJob(jobId);

    // Update parent job tracking
    const parentJobId = `${taskId}-CANCEL_${marketplace}_BID`;
    const parentJob = await this.stateManager.updateParentJob(parentJobId, { failed: 1 });

    if (!parentJob) return;

    // Calculate cancellation progress
    await this.updateCancellationProgress(taskId, marketplace);
  }

  private async handleBiddingJobCompleted(jobId: string): Promise<void> {
    const taskId = jobId.split('-')[0];
    const task = this.stateManager.getActiveTasks().get(taskId);

    if (!task || !task.running) return;

    const marketplace = jobId.split('-')[1].toLowerCase();
    const parentId = `${taskId}-${marketplace}`.toLowerCase();

    // Update parent job
    const parentJob = await this.stateManager.updateParentJob(parentId, { completed: 1 });
    if (!parentJob) return;

    // Update running percentage
    await this.updateRunningPercentage(taskId, task);

    // Check if marketplace is complete
    if (parentJob.completedJobs + parentJob.failedJobs === parentJob.totalJobs) {
      const allComplete = await this.stateManager.markMarketplaceComplete(taskId, marketplace as any);

      if (allComplete) {
        await this.handleAllMarketplacesComplete(taskId, task);
      }
    }
  }

  private async handleBiddingJobFailed(jobId: string): Promise<void> {
    const taskId = jobId.split('-')[0];
    const task = this.stateManager.getActiveTasks().get(taskId);

    if (!task || !task.running) return;

    const marketplace = jobId.split('-')[1].toLowerCase();
    const parentId = `${taskId}-${marketplace}`.toLowerCase();

    // Update parent job
    const parentJob = await this.stateManager.updateParentJob(parentId, { failed: 1 });
    if (!parentJob) return;

    // Update running percentage
    await this.updateRunningPercentage(taskId, task);

    // Check if marketplace is complete
    if (parentJob.completedJobs + parentJob.failedJobs === parentJob.totalJobs) {
      const allComplete = await this.stateManager.markMarketplaceComplete(taskId, marketplace as any);

      if (allComplete) {
        await this.handleAllMarketplacesComplete(taskId, task);
      }
    }
  }

  private async updateCancellationProgress(taskId: string, marketplace: string): Promise<void> {
    const task = this.stateManager.getActiveTasks().get(taskId);
    if (!task) return;

    const state = this.stateManager.getTaskState(taskId);
    if (!state || state.status !== 'CANCELLING') return;

    if (!task.running) {
      // Calculate combined cancellation percentage
      const percentage = await this.calculateCombinedCancellationPercentage(taskId);

      await this.stateManager.updateTaskProgress(taskId, {
        cancellationPercentage: Math.min(percentage, 100)
      });

      if (percentage >= 100) {
        await this.completeCancellation(taskId, task);
      }
    } else {
      // Single marketplace cancellation
      const percentage = await this.calculateMarketplaceCancellationPercentage(taskId, marketplace);

      await this.stateManager.updateTaskProgress(taskId, {
        cancellationPercentage: percentage
      });

      if (percentage >= 100) {
        await this.completeCancellation(taskId, task, marketplace.toLowerCase());
      }
    }
  }

  private async updateRunningPercentage(taskId: string, task: ITask): Promise<void> {
    const selectedMarketplaces = task.selectedMarketplaces.map(m => m.toLowerCase());
    const enabledAndSelected = selectedMarketplaces.filter(m =>
      this.enabledMarketplaces.map(em => em.toLowerCase()).includes(m)
    );

    let totalJobs = 0;
    let completedJobs = 0;
    let failedJobs = 0;

    for (const marketplace of enabledAndSelected) {
      const parentId = `${taskId}-${marketplace}`.toLowerCase();
      const job = this.stateManager.getParentJobs().get(parentId);
      if (job) {
        totalJobs += job.totalJobs;
        completedJobs += job.completedJobs;
        failedJobs += job.failedJobs;
      }
    }

    const runningPercentage = totalJobs > 0 ? ((completedJobs + failedJobs) / totalJobs) * 100 : 0;

    await this.stateManager.transitionState(taskId, 'RUNNING', (state) => {
      state.runningPercentage = Math.min(runningPercentage, 100);
    });
  }

  private async handleAllMarketplacesComplete(taskId: string, task: ITask): Promise<void> {
    console.log(GREEN + `Task ${taskId} completed for all marketplaces` + RESET);

    const wallet = await Wallet.findById(task.walletId);
    const WALLET_PRIVATE_KEY = decryptPrivateKey(wallet?.encryptedPrivateKey as string) as string;
    const { currencySymbol } = await calculateBidPrice(task, 0, "magiceden");

    // Handle trait and token removals
    if (this.traitsToRemove[taskId]?.length > 0) {
      await cancelRemovedTraitBids(this.traitsToRemove[taskId], task._id, WALLET_PRIVATE_KEY, currencySymbol);
    }

    if ((this.removedTokens[taskId]?.opensea?.length || 0) > 0 || (this.removedTokens[taskId]?.magiceden?.length || 0) > 0) {
      const removedTokens = this.removedTokens[taskId];
      await cancelRemovedTokenBids({
        opensea: removedTokens.opensea || [],
        magiceden: removedTokens.magiceden || [],
        blur: removedTokens.blur || []
      }, task._id, WALLET_PRIVATE_KEY, currencySymbol);
    }

    // Clean up removal data
    delete this.traitsToRemove[taskId];
    delete this.removedTokens[taskId];

    // Transition to completed
    await this.stateManager.transitionState(taskId, 'COMPLETED');

    // Wait a bit before scheduling next loop
    await new Promise(resolve => setTimeout(resolve, 1000));

    // Schedule next loop if task is still running
    if (task.running) {
      await this.scheduleNextLoop(taskId, task);
    }
  }

  private async scheduleNextLoop(taskId: string, task: ITask): Promise<void> {
    const loopInterval = getExpiry(task.loopInterval);
    const nextLoopTime = new Date(Date.now() + loopInterval * 1000);
    const secondsUntilNextLoop = Math.floor((nextLoopTime.getTime() - Date.now()) / 1000);

    console.log(GREEN + `Scheduling task ${taskId} to run again in ${loopInterval} seconds` + RESET);

    // Transition to scheduled state
    await this.stateManager.transitionState(taskId, 'SCHEDULED', (state) => {
      state.waitingTime = secondsUntilNextLoop;
      state.nextLoopTime = nextLoopTime;
    });

    // Set up countdown timer
    const countdownTimer = setInterval(() => {
      const state = this.stateManager.getTaskState(taskId);
      if (state?.nextLoopTime) {
        const remainingSeconds = Math.max(0, Math.floor((state.nextLoopTime.getTime() - Date.now()) / 1000));
        this.stateManager.updateTaskProgress(taskId, { waitingTime: remainingSeconds });

        if (remainingSeconds <= 0) {
          clearInterval(countdownTimer);
        }
      }
    }, 1000);

    this.stateManager.setCountdownTimer(taskId, countdownTimer);

    // Clear existing timeout and set new one
    const timeoutId = setTimeout(async () => {
      const currentTask = this.stateManager.getActiveTasks().get(taskId);
      if (currentTask?.running) {
        // This should trigger startTask in the main code
        await this.stateManager.transitionState(taskId, 'STARTED');
      }
    }, loopInterval * 1000);

    this.stateManager.setTaskTimeout(taskId, timeoutId);
  }

  private async completeCancellation(taskId: string, task: ITask, marketplace?: string): Promise<void> {
    const whereQuery = marketplace ? {
      taskId: taskId,
      marketplace: marketplace
    } : {
      taskId: taskId
    };
    const orders = await Order.find(whereQuery);

    if (orders.length > 0) {
      await cancelAllRelatedBids(task, marketplace);
    }

    // Transition to stopped state
    await this.stateManager.transitionState(taskId, 'STOPPED', (state) => {
      state.cancellationPercentage = 100;
      delete state.nextLoopTime;
      state.waitingTime = 0;
    });
  }

  private async calculateCombinedCancellationPercentage(taskId: string): Promise<number> {
    const [openSeaCount, magicEdenCount, blurCount] = await Promise.all([
      Order.countDocuments({ taskId, marketplace: 'opensea' }),
      Order.countDocuments({ taskId, marketplace: 'magiceden' }),
      Order.countDocuments({ taskId, marketplace: 'blur' })
    ]);

    const totalOrderCount = openSeaCount + magicEdenCount + blurCount;
    if (totalOrderCount === 0) return 100;

    let weightedPercentage = 0;

    // Calculate percentage for each marketplace
    const marketplaces = [
      { name: 'OPENSEA', count: openSeaCount },
      { name: 'MAGICEDEN', count: magicEdenCount },
      { name: 'BLUR', count: blurCount }
    ];

    for (const { name, count } of marketplaces) {
      if (count > 0) {
        const parentJobId = `${taskId}-CANCEL_${name}_BID`;
        const parentJob = this.stateManager.getParentJobs().get(parentJobId);

        if (parentJob) {
          const progress = parentJob.completedJobs + parentJob.failedJobs;
          const percentage = parentJob.totalJobs > 0 ? (progress / parentJob.totalJobs) * 100 : 100;
          weightedPercentage += (count * percentage);
        } else {
          weightedPercentage += (count * 100);
        }
      }
    }

    return weightedPercentage / totalOrderCount;
  }

  private async calculateMarketplaceCancellationPercentage(taskId: string, marketplace: string): Promise<number> {
    const parentJobId = `${taskId}-CANCEL_${marketplace}_BID`;
    const parentJob = this.stateManager.getParentJobs().get(parentJobId);

    if (!parentJob || parentJob.totalJobs < 1) return 100;

    return ((parentJob.completedJobs + parentJob.failedJobs) / parentJob.totalJobs) * 100;
  }

  private extractMarketplaceFromCancellationJob(jobId: string): string {
    if (jobId.includes('CANCEL_OPENSEA_BID')) return 'OPENSEA';
    if (jobId.includes('CANCEL_MAGICEDEN_BID')) return 'MAGICEDEN';
    if (jobId.includes('CANCEL_BLUR_BID')) return 'BLUR';
    return '';
  }

  // Setters for removal data (for compatibility)
  setTraitsToRemove(taskId: string, traits: any[]): void {
    this.traitsToRemove[taskId] = traits;
  }

  setRemovedTokens(taskId: string, tokens: any): void {
    this.removedTokens[taskId] = tokens;
  }

  // Getters for removal data
  getTraitsToRemove(taskId: string): any[] {
    return this.traitsToRemove[taskId] || [];
  }

  getRemovedTokens(taskId: string): any {
    return this.removedTokens[taskId] || {};
  }
}